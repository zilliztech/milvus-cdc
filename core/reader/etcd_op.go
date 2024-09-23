/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * //
 *     http://www.apache.org/licenses/LICENSE-2.0
 * //
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reader

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"

	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/log"
	"github.com/zilliztech/milvus-cdc/core/model"
	"github.com/zilliztech/milvus-cdc/core/pb"
	"github.com/zilliztech/milvus-cdc/core/util"
)

var _ api.MetaOp = (*EtcdOp)(nil)

const (
	collectionPrefix = "root-coord/database/collection-info"
	partitionPrefix  = "root-coord/partitions"
	fieldPrefix      = "root-coord/fields"
	databasePrefix   = "root-coord/database/db-info"
	tsPrefix         = "kv/gid/timestamp" // TODO the kv root is configurable
	TomeObject       = "_tome"            // has marked deleted object

	SkipCollectionState = pb.CollectionState(-100)
	SkipPartitionState  = pb.PartitionState(-100)
)

type EtcdOp struct {
	endpoints            []string
	rootPath             string
	metaSubPath          string
	defaultPartitionName string
	etcdClient           *clientv3.Client

	// don't use the name to get id, because the same name may have different id when an object is deleted and recreated
	collectionID2Name util.Map[int64, string]
	collectionID2DBID util.Map[int64, int64]
	// don't use the name to get id
	dbID2Name util.Map[int64, string]

	watchCollectionOnce sync.Once
	watchPartitionOnce  sync.Once
	watchStartOnce      sync.Once
	retryOptions        []retry.Option

	// task id -> api.CollectionFilter
	subscribeCollectionEvent util.Map[string, api.CollectionEventConsumer]
	subscribePartitionEvent  util.Map[string, api.PartitionEventConsumer]

	handlerWatchEventPool *conc.Pool[struct{}]
	startWatch            chan struct{}

	targetMilvus api.TargetAPI
}

func NewEtcdOpWithAddress(endpoints []string,
	rootPath, metaPath, defaultPartitionName string,
	etcdConfig config.EtcdRetryConfig, target api.TargetAPI,
) (api.MetaOp, error) {
	return NewEtcdOp(config.EtcdServerConfig{
		Address:     endpoints,
		RootPath:    rootPath,
		MetaSubPath: metaPath,
	}, defaultPartitionName, etcdConfig, target)
}

func NewEtcdOp(etcdServerConfig config.EtcdServerConfig, defaultPartitionName string,
	etcdConfig config.EtcdRetryConfig, target api.TargetAPI,
) (api.MetaOp, error) {
	// set default value
	if len(etcdServerConfig.Address) == 0 {
		etcdServerConfig.Address = []string{"127.0.0.1:2379"}
	}
	if etcdServerConfig.RootPath == "" {
		etcdServerConfig.RootPath = "by-dev"
	}
	if etcdServerConfig.MetaSubPath == "" {
		etcdServerConfig.MetaSubPath = "meta"
	}
	if defaultPartitionName == "" {
		defaultPartitionName = "_default"
	}

	etcdOp := &EtcdOp{
		endpoints:             etcdServerConfig.Address,
		rootPath:              etcdServerConfig.RootPath,
		metaSubPath:           etcdServerConfig.MetaSubPath,
		defaultPartitionName:  defaultPartitionName,
		retryOptions:          util.GetRetryOptions(etcdConfig.Retry),
		handlerWatchEventPool: conc.NewPool[struct{}](16, conc.WithExpiryDuration(time.Minute)),
		startWatch:            make(chan struct{}),
		targetMilvus:          target,
	}

	var err error
	log := log.With(zap.Strings("endpoints", etcdOp.endpoints))
	etcdClientConfig, err := util.GetEtcdConfig(etcdServerConfig)
	if err != nil {
		log.Warn("fail to get etcd config", zap.Error(err))
		return nil, err
	}
	etcdOp.etcdClient, err = clientv3.New(etcdClientConfig)
	if err != nil {
		log.Warn("create etcd client failed", zap.Error(err))
		return nil, err
	}
	// check etcd status
	timeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = etcdOp.etcdClient.Status(timeCtx, etcdOp.endpoints[0])
	if err != nil {
		log.Warn("etcd status check failed", zap.Error(err))
		return nil, err
	}
	log.Debug("success to create etcd client")

	return etcdOp, nil
}

func (e *EtcdOp) StartWatch() {
	e.watchStartOnce.Do(func() {
		close(e.startWatch)
	})
}

func (e *EtcdOp) collectionPrefix() string {
	return fmt.Sprintf("%s/%s/%s", e.rootPath, e.metaSubPath, collectionPrefix)
}

func (e *EtcdOp) partitionPrefix() string {
	return fmt.Sprintf("%s/%s/%s", e.rootPath, e.metaSubPath, partitionPrefix)
}

func (e *EtcdOp) fieldPrefix() string {
	return fmt.Sprintf("%s/%s/%s", e.rootPath, e.metaSubPath, fieldPrefix)
}

func (e *EtcdOp) databasePrefix() string {
	return fmt.Sprintf("%s/%s/%s", e.rootPath, e.metaSubPath, databasePrefix)
}

func (e *EtcdOp) tsPrefix() string {
	return fmt.Sprintf("%s/%s", e.rootPath, tsPrefix)
}

func (e *EtcdOp) WatchCollection(ctx context.Context, filter api.CollectionFilter) {
	e.watchCollectionOnce.Do(func() {
		watchChan := e.etcdClient.Watch(ctx, e.collectionPrefix()+"/", clientv3.WithPrefix(), clientv3.WithPrevKV())
		go func() {
			select {
			case <-e.startWatch:
			case <-ctx.Done():
				log.Warn("watch collection context done")
				return
			}
			for {
				select {
				case watchResp, ok := <-watchChan:
					if !ok {
						log.Info("etcd watch collection channel closed")
						return
					}
					for _, event := range watchResp.Events {
						if event.Type != clientv3.EventTypePut {
							log.Info("collection watch event type is not put", zap.String("event type", event.Type.String()))
							continue
						}
						collectionKey := util.ToString(event.Kv.Key)
						if util.IsTombstone(event.Kv.Value) {
							log.Info("the collection is deleted", zap.String("key", collectionKey))

							if event.PrevKv != nil {
								beforeInfo := &pb.CollectionInfo{}
								err := proto.Unmarshal(event.PrevKv.Value, beforeInfo)
								if err != nil {
									log.Warn("fail to unmarshal the collection info",
										zap.String("key", util.ToString(event.PrevKv.Key)),
										zap.String("value", util.Base64Encode(event.PrevKv.Value)), zap.Error(err))
									continue
								}
								if beforeInfo.State != pb.CollectionState_CollectionCreating {
									continue
								}
								collectionName := beforeInfo.Schema.GetName()
								beforeInfo.State = SkipCollectionState
								hasConsume := e.subscribeCollectionEvent.Range(func(key string, value api.CollectionEventConsumer) bool {
									if value != nil && value(beforeInfo) {
										log.Info("the collection has been consumed",
											zap.String("collection_name", collectionName),
											zap.Int64("collection_id", beforeInfo.ID),
											zap.String("task_id", key))
										return false
									}
									return true
								})
								if !hasConsume {
									log.Info("the collection is not consumed",
										zap.String("collection_name", collectionName),
										zap.Int64("collection_id", beforeInfo.ID))
								}
							}

							continue
						}
						info := &pb.CollectionInfo{}
						err := proto.Unmarshal(event.Kv.Value, info)
						if err != nil {
							log.Warn("fail to unmarshal the collection info", zap.String("key", collectionKey), zap.String("value", util.Base64Encode(event.Kv.Value)), zap.Error(err))
							continue
						}

						collectionName := info.Schema.GetName()

						if info.State != pb.CollectionState_CollectionCreated {
							log.Info("the collection state is not created",
								zap.String("key", collectionKey), zap.String("collection_name", collectionName),
								zap.String("state", info.State.String()))
							continue
						}

						if filter != nil && filter(info) {
							log.Info("the collection is filtered in the watch process",
								zap.String("key", collectionKey), zap.String("collection_name", collectionName))
							continue
						}

						_ = e.handlerWatchEventPool.Submit(func() (struct{}, error) {
							err := retry.Do(ctx, func() error {
								return e.fillCollectionField(info)
							}, e.retryOptions...)
							if err != nil {
								log.Warn("fail to fill collection field in the watch process",
									zap.String("key", collectionKey), zap.String("collection_name", collectionName), zap.Error(err))
								return struct{}{}, err
							}
							e.collectionID2Name.Store(info.ID, info.Schema.Name)
							if databaseID := e.getDatabaseIDFromCollectionKey(collectionKey); databaseID != 0 {
								e.collectionID2DBID.Store(info.ID, databaseID)
							}
							notConsumed := e.subscribeCollectionEvent.Range(func(key string, value api.CollectionEventConsumer) bool {
								if value != nil && value(info) {
									log.Info("the collection has been consumed", zap.Int64("collection_id", info.ID), zap.String("task_id", key))
									return false
								}
								return true
							})
							if notConsumed {
								log.Info("the collection is not consumed",
									zap.Int64("collection_id", info.ID),
									zap.String("collection_name", collectionName))
							}
							return struct{}{}, nil
						})
					}
				case <-ctx.Done():
					log.Info("watch collection context done")
					return
				}
			}
		}()
	})
}

func (e *EtcdOp) SubscribeCollectionEvent(taskID string, consumer api.CollectionEventConsumer) {
	e.subscribeCollectionEvent.Store(taskID, consumer)
}

func (e *EtcdOp) SubscribePartitionEvent(taskID string, consumer api.PartitionEventConsumer) {
	e.subscribePartitionEvent.Store(taskID, consumer)
}

func (e *EtcdOp) UnsubscribeEvent(taskID string, eventType api.WatchEventType) {
	switch eventType {
	case api.CollectionEventType:
		e.subscribeCollectionEvent.Delete(taskID)
	case api.PartitionEventType:
		e.subscribePartitionEvent.Delete(taskID)
	default:
		log.Warn("unknown event type", zap.String("taskID", taskID), zap.Any("eventType", eventType))
	}
}

func (e *EtcdOp) WatchPartition(ctx context.Context, filter api.PartitionFilter) {
	e.watchPartitionOnce.Do(func() {
		watchChan := e.etcdClient.Watch(ctx, e.partitionPrefix()+"/", clientv3.WithPrefix(), clientv3.WithPrevKV())
		go func() {
			select {
			case <-e.startWatch:
			case <-ctx.Done():
				log.Warn("watch partition context done")
				return
			}
			for {
				select {
				case watchResp, ok := <-watchChan:
					if !ok {
						log.Info("etcd watch partition channel closed")
						return
					}
					for _, event := range watchResp.Events {
						if event.Type != clientv3.EventTypePut {
							log.Debug("partition watch event type is not put", zap.String("event type", event.Type.String()))
							continue
						}
						partitionKey := util.ToString(event.Kv.Key)
						if util.IsTombstone(event.Kv.Value) {
							log.Info("the partition is deleted", zap.String("key", partitionKey))
							continue
						}
						info := &pb.PartitionInfo{}
						err := proto.Unmarshal(event.Kv.Value, info)
						if err != nil {
							log.Warn("fail to unmarshal the partition info",
								zap.String("key", partitionKey), zap.String("value", util.Base64Encode(event.Kv.Value)), zap.Error(err))
							if !strings.Contains(info.PartitionName, e.defaultPartitionName) &&
								event.PrevKv != nil {
								beforeInfo := &pb.PartitionInfo{}
								err := proto.Unmarshal(event.PrevKv.Value, info)
								if err != nil {
									log.Warn("fail to unmarshal the partition info",
										zap.String("key", util.ToString(event.PrevKv.Key)),
										zap.String("value", util.Base64Encode(event.PrevKv.Value)), zap.Error(err))
									continue
								}
								if beforeInfo.State != pb.PartitionState_PartitionCreating {
									continue
								}

								beforeInfo.State = SkipPartitionState
								hasConsume := e.subscribePartitionEvent.Range(func(key string, value api.PartitionEventConsumer) bool {
									if value != nil && value(beforeInfo) {
										log.Info("the partition has been consumed",
											zap.Int64("collection_id", beforeInfo.CollectionId),
											zap.String("partition_name", beforeInfo.PartitionName),
											zap.String("key", partitionKey),
											zap.String("task_id", key))
										return false
									}
									return true
								})
								if !hasConsume {
									log.Info("the partition is not consumed",
										zap.Int64("collection_id", beforeInfo.CollectionId),
										zap.String("partition_name", beforeInfo.PartitionName),
										zap.String("key", partitionKey))
								}
								continue
							}
							continue
						}
						if info.State != pb.PartitionState_PartitionCreated ||
							strings.Contains(info.PartitionName, e.defaultPartitionName) {
							log.Info("partition state is not created or partition name is default",
								zap.Int64("collection_id", info.CollectionId),
								zap.String("partition name", info.PartitionName), zap.Any("state", info.State))
							continue
						}
						if filter != nil && filter(info) {
							log.Info("partition filter",
								zap.Int64("collection_id", info.CollectionId),
								zap.String("partition name", info.PartitionName))
							continue
						}

						log.Debug("get a new partition in the watch process", zap.String("key", partitionKey))
						_ = e.handlerWatchEventPool.Submit(func() (struct{}, error) {
							hasConsume := e.subscribePartitionEvent.Range(func(key string, value api.PartitionEventConsumer) bool {
								if value != nil && value(info) {
									log.Info("the partition has been consumed",
										zap.Int64("collection_id", info.CollectionId),
										zap.String("partition_name", info.PartitionName),
										zap.String("key", partitionKey),
										zap.String("task_id", key))
									return false
								}
								return true
							})
							if !hasConsume {
								log.Info("the partition is not consumed",
									zap.Int64("collection_id", info.CollectionId),
									zap.String("partition_name", info.PartitionName),
									zap.String("key", partitionKey))
							}
							return struct{}{}, nil
						})
					}
				case <-ctx.Done():
					log.Info("watch partition context done")
					return
				}
			}
		}()
	})
}

func (e *EtcdOp) getCollectionIDFromPartitionKey(key string) int64 {
	subString := strings.Split(key[len(e.partitionPrefix())+1:], "/")
	if len(subString) != 2 {
		log.Warn("the key is invalid", zap.String("key", key), zap.Strings("sub", subString))
		return 0
	}
	id, err := strconv.ParseInt(subString[0], 10, 64)
	if err != nil {
		log.Warn("fail to parse the collection id", zap.String("id", subString[0]), zap.Error(err))
		return 0
	}
	return id
}

func (e *EtcdOp) getDatabaseIDFromCollectionKey(key string) int64 {
	subString := strings.Split(key[len(e.collectionPrefix())+1:], "/")
	if len(subString) != 2 {
		log.Warn("the key is invalid", zap.String("key", key), zap.Strings("sub", subString))
		return 0
	}
	id, err := strconv.ParseInt(subString[0], 10, 64)
	if err != nil {
		log.Warn("fail to parse the database id", zap.String("id", subString[0]), zap.Error(err))
		return 0
	}
	return id
}

func (e *EtcdOp) getDatabases(ctx context.Context) ([]model.DatabaseInfo, error) {
	resp, err := util.EtcdGetWithContext(ctx, e.etcdClient, e.databasePrefix(), clientv3.WithPrefix())
	if err != nil {
		log.Warn("fail to get all databases", zap.Error(err))
		return nil, err
	}
	var databases []model.DatabaseInfo
	for _, kv := range resp.Kvs {
		if util.IsTombstone(kv.Value) {
			idStr := util.ToString(kv.Key)[len(e.databasePrefix())+1:]
			databaseID, err := strconv.ParseInt(idStr, 10, 64)
			if err != nil {
				log.Panic("fail to parse the database id", zap.String("id", idStr), zap.Error(err))
				continue
			}
			databases = append(databases, model.DatabaseInfo{
				ID:      databaseID,
				Dropped: true,
			})
			e.dbID2Name.Store(databaseID, TomeObject)
			continue
		}
		info := &pb.DatabaseInfo{}
		err = proto.Unmarshal(kv.Value, info)
		if err != nil {
			log.Warn("fail to unmarshal database info",
				zap.String("key", string(kv.Key)),
				zap.String("value", util.Base64Encode(kv.Value)), zap.Error(err))
			return nil, err
		}
		databases = append(databases, model.DatabaseInfo{
			ID:   info.Id,
			Name: info.Name,
		})
		e.dbID2Name.Store(info.Id, info.Name)
	}
	return databases, nil
}

func (e *EtcdOp) getCollectionNameByID(ctx context.Context, collectionID int64) string {
	var (
		resp     *clientv3.GetResponse
		database model.DatabaseInfo
		key      string
		err      error
	)

	databases, err := e.getDatabases(ctx)
	if err != nil {
		log.Warn("fail to get all databases", zap.Error(err))
		return ""
	}

	for _, database = range databases {
		key = path.Join(e.collectionPrefix(), strconv.FormatInt(database.ID, 10), strconv.FormatInt(collectionID, 10))
		resp, err = util.EtcdGetWithContext(ctx, e.etcdClient, key)
		if err != nil {
			log.Warn("fail to get the collection data", zap.Int64("collection_id", collectionID), zap.Error(err))
			return ""
		}
		if len(resp.Kvs) == 0 {
			continue
		}
	}
	if resp == nil {
		log.Warn("there is no database")
		return ""
	}
	if len(resp.Kvs) == 0 {
		log.Warn("the collection isn't existed",
			zap.Int64("collection_id", collectionID), zap.String("key", key),
			zap.Any("databases", databases))
		return ""
	}

	e.collectionID2DBID.Store(collectionID, database.ID)
	if util.IsTombstone(resp.Kvs[0].Value) {
		e.collectionID2Name.Store(collectionID, TomeObject)
		log.Warn("the collection is deleted", zap.Int64("collection_id", collectionID))
		return TomeObject
	}

	info := &pb.CollectionInfo{}
	err = proto.Unmarshal(resp.Kvs[0].Value, info)
	if err != nil {
		log.Warn("fail to unmarshal collection info, maybe it's a deleted collection",
			zap.Int64("collection_id", collectionID),
			zap.String("value", util.Base64Encode(resp.Kvs[0].Value)),
			zap.Error(err))
		return ""
	}
	collectionName := info.Schema.GetName()
	e.collectionID2Name.Store(collectionID, collectionName)

	return collectionName
}

func (e *EtcdOp) internalGetAllCollection(ctx context.Context, fillField bool, filters []api.CollectionFilter) ([]*pb.CollectionInfo, error) {
	_, _ = e.getDatabases(ctx)

	resp, err := util.EtcdGetWithContext(ctx, e.etcdClient, e.collectionPrefix()+"/", clientv3.WithPrefix())
	if err != nil {
		log.Warn("fail to get all collection data", zap.Error(err))
		return nil, err
	}
	var existedCollectionInfos []*pb.CollectionInfo
	log.Info("get all collection data", zap.Int("count", len(resp.Kvs)))
	for _, kv := range resp.Kvs {
		info := &pb.CollectionInfo{}
		err = proto.Unmarshal(kv.Value, info)
		if err != nil {
			if !util.IsTombstone(kv.Value) {
				log.Info("fail to unmarshal collection info", zap.String("key", util.ToString(kv.Key)), zap.String("value", util.Base64Encode(kv.Value)), zap.Error(err))
			}
			continue
		}
		hasFilter := false
		for _, filter := range filters {
			if filter != nil && filter(info) {
				hasFilter = true
				break
			}
		}
		if hasFilter {
			continue
		}
		if info.State == pb.CollectionState_CollectionCreated && fillField {
			err = e.fillCollectionField(info)
			if err != nil {
				log.Warn("fail to fill collection field", zap.String("key", util.ToString(kv.Key)), zap.Error(err))
				continue
			}
		}
		e.collectionID2Name.Store(info.ID, info.Schema.Name)
		if databaseID := e.getDatabaseIDFromCollectionKey(util.ToString(kv.Key)); databaseID != 0 {
			e.collectionID2DBID.Store(info.ID, databaseID)
		}
		existedCollectionInfos = append(existedCollectionInfos, info)
	}
	return existedCollectionInfos, nil
}

func (e *EtcdOp) GetAllCollection(ctx context.Context, filter api.CollectionFilter) ([]*pb.CollectionInfo, error) {
	return e.internalGetAllCollection(ctx, true, []api.CollectionFilter{
		func(info *pb.CollectionInfo) bool {
			if info.State != pb.CollectionState_CollectionCreated &&
				info.State != pb.CollectionState_CollectionDropped &&
				info.State != pb.CollectionState_CollectionDropping {
				log.Info("not created/dropped collection",
					zap.String("collection", info.Schema.GetName()),
					zap.String("state", info.State.String()))
				return true
			}
			return false
		},
		func(info *pb.CollectionInfo) bool {
			if filter(info) {
				log.Info("the collection info is filtered", zap.String("collection", info.Schema.GetName()))
				return true
			}
			return false
		},
	})
}

func (e *EtcdOp) fillCollectionField(info *pb.CollectionInfo) error {
	prefix := path.Join(e.fieldPrefix(), strconv.FormatInt(info.ID, 10)) + "/"
	resp, err := util.EtcdGetWithContext(context.Background(), e.etcdClient, prefix, clientv3.WithPrefix())
	log := log.With(zap.String("prefix", prefix))
	if err != nil {
		log.Warn("fail to get the collection field data", zap.Error(err))
		return err
	}
	if len(resp.Kvs) == 0 {
		msg := "not found the collection field data"
		log.Warn(msg)
		return errors.New(msg)
	}
	var fields []*schemapb.FieldSchema
	for _, kv := range resp.Kvs {
		field := &schemapb.FieldSchema{}
		err = proto.Unmarshal(kv.Value, field)
		if err != nil {
			log.Warn("fail to unmarshal filed schema info", zap.String("key", util.ToString(kv.Key)), zap.Error(err))
			return err
		}
		if field.Name == common.MetaFieldName {
			info.Schema.EnableDynamicField = true
			continue
		}
		// if the field id is less than 100, it is a system field, skip it.
		if field.FieldID < 100 {
			continue
		}
		fields = append(fields, field)
	}
	info.Schema.Fields = fields
	return nil
}

func (e *EtcdOp) GetCollectionNameByID(ctx context.Context, id int64) string {
	collectionName, ok := e.collectionID2Name.Load(id)
	if !ok || collectionName == "" {
		collectionName = e.getCollectionNameByID(ctx, id)
		if collectionName == "" {
			log.Warn("not found the collection", zap.Int64("collection_id", id))
		}
	}
	return collectionName
}

func (e *EtcdOp) GetDatabaseInfoForCollection(ctx context.Context, id int64) model.DatabaseInfo {
	dbID, _ := e.collectionID2DBID.Load(id)
	dbName, _ := e.dbID2Name.Load(dbID)
	if dbName != "" {
		return model.DatabaseInfo{
			ID:      dbID,
			Name:    dbName,
			Dropped: IsDroppedObject(dbName),
		}
	}

	// it will update all database info and this collection info
	_ = retry.Do(ctx, func() error {
		name := e.getCollectionNameByID(ctx, id)
		if name == "" {
			return errors.Newf("not found the collection %d", id)
		}
		return nil
	}, e.retryOptions...)

	dbID, _ = e.collectionID2DBID.Load(id)
	dbName, _ = e.dbID2Name.Load(dbID)
	return model.DatabaseInfo{
		ID:      dbID,
		Name:    dbName,
		Dropped: IsDroppedObject(dbName),
	}
}

func (e *EtcdOp) internalGetAllPartition(ctx context.Context, filters []api.PartitionFilter) ([]*pb.PartitionInfo, error) {
	resp, err := util.EtcdGetWithContext(ctx, e.etcdClient, e.partitionPrefix()+"/", clientv3.WithPrefix())
	if err != nil {
		log.Warn("fail to get all partition data", zap.Error(err))
		return nil, err
	}
	var existedPartitionInfos []*pb.PartitionInfo
	log.Info("get all partition data", zap.Int("partition_num", len(resp.Kvs)))
	for _, kv := range resp.Kvs {
		info := &pb.PartitionInfo{}
		err = proto.Unmarshal(kv.Value, info)
		if err != nil {
			if !util.IsTombstone(kv.Value) {
				log.Warn("fail to unmarshal partition info", zap.String("key", util.ToString(kv.Key)), zap.String("value", util.Base64Encode(kv.Value)), zap.Error(err))
			}
			continue
		}
		hasFilter := false
		for _, filter := range filters {
			if filter != nil && filter(info) {
				hasFilter = true
				break
			}
		}
		if hasFilter {
			continue
		}
		existedPartitionInfos = append(existedPartitionInfos, info)
	}
	return existedPartitionInfos, nil
}

func (e *EtcdOp) GetAllPartition(ctx context.Context, filter api.PartitionFilter) ([]*pb.PartitionInfo, error) {
	return e.internalGetAllPartition(ctx, []api.PartitionFilter{
		func(info *pb.PartitionInfo) bool {
			if (info.State != pb.PartitionState_PartitionCreated &&
				info.State != pb.PartitionState_PartitionDropping &&
				info.State != pb.PartitionState_PartitionDropped) ||
				strings.Contains(info.PartitionName, e.defaultPartitionName) {
				log.Info("partition state is not created/dropped or partition name is default",
					zap.String("partition_name", info.PartitionName),
					zap.String("state", info.State.String()))
				return true
			}
			return false
		},
		func(info *pb.PartitionInfo) bool {
			if filter(info) {
				log.Info("the partition info is filtered", zap.String("partition", info.PartitionName))
				return true
			}
			return false
		},
	})
}

func (e *EtcdOp) GetAllDroppedObj() map[string]map[string]uint64 {
	ctx := context.Background()
	res := make(map[string]map[string]uint64)

	getResp, err := e.etcdClient.Get(ctx, e.tsPrefix())
	if err != nil {
		log.Panic("fail to get the ts data", zap.String("prefix", e.tsPrefix()), zap.Error(err))
		return res
	}
	if len(getResp.Kvs) != 1 {
		log.Panic("fail to get the ts data", zap.String("prefix", e.tsPrefix()), zap.Int("len", len(getResp.Kvs)))
		return res
	}
	curTime, err := typeutil.ParseTimestamp(getResp.Kvs[0].Value)
	if err != nil {
		log.Panic("fail to parse the ts data", zap.String("prefix", e.tsPrefix()), zap.Error(err))
		return res
	}
	log.Info("current time", zap.Time("ts", curTime))
	tt := tsoutil.ComposeTSByTime(curTime, 0)

	_, err = e.getDatabases(ctx)
	if err != nil {
		log.Panic("fail to get all database", zap.Error(err))
		return res
	}
	collections, err := e.internalGetAllCollection(ctx, false, []api.CollectionFilter{})
	if err != nil {
		log.Panic("fail to get all collection", zap.Error(err))
		return res
	}
	partitions, err := e.internalGetAllPartition(ctx, []api.PartitionFilter{})
	if err != nil {
		log.Panic("fail to get all partition", zap.Error(err))
		return res
	}

	droppedDatabaseKey := util.DroppedDatabaseKey
	droppedCollectionKey := util.DroppedCollectionKey
	droppedPartitionKey := util.DroppedPartitionKey

	res[droppedDatabaseKey] = make(map[string]uint64)
	res[droppedCollectionKey] = make(map[string]uint64)
	res[droppedPartitionKey] = make(map[string]uint64)

	createdCollection := make(map[string]uint64)
	createdPartition := make(map[string]uint64)

	getDBNameForCollection := func(collectionID int64) string {
		dbID := e.collectionID2DBID.LoadWithDefault(collectionID, -1)
		if dbID == 0 {
			log.Warn("fail to get db id for collection", zap.Int64("collection_id", collectionID))
			return ""
		}
		dbName := e.dbID2Name.LoadWithDefault(dbID, "")
		if dbName == "" {
			log.Warn("fail to get db name for collection", zap.Int64("collection_id", collectionID))
		}
		return dbName
	}

	var dbName string
	for _, collection := range collections {
		collectionName := collection.Schema.Name
		originDBName := getDBNameForCollection(collection.ID)
		if originDBName == "" {
			log.Panic("fail to get db name for collection", zap.Int64("collection_id", collection.ID))
			continue
		}
		// maybe the database has been drop, so get the database name from the target
		// targetMilvus is nil when downstream is not milvus
		if e.targetMilvus != nil {
			dbName, err = e.targetMilvus.GetDatabaseName(context.Background(), collectionName, originDBName)
			if IsDatabaseNotFoundError(err) {
				log.Info("the collection info has been dropped in the source and target", zap.String("collection_name", collectionName))
				continue
			}
			if err != nil {
				log.Panic("fail to get database name", zap.String("collection_name", collectionName), zap.Error(err))
				continue
			}
			if originDBName != dbName {
				_, dropDBKey := util.GetDBInfoKeys(dbName)
				res[droppedDatabaseKey][dropDBKey] = tt - 1
			}
		} else {
			dbName = originDBName
		}

		_, dropKey := util.GetCollectionInfoKeys(collectionName, dbName)
		if collection.State == pb.CollectionState_CollectionCreated || collection.State == pb.CollectionState_CollectionCreating {
			createdCollection[dropKey] = collection.CreateTime
		} else if collection.State == pb.CollectionState_CollectionDropped || collection.State == pb.CollectionState_CollectionDropping {
			res[droppedCollectionKey][dropKey] = tt - 1
		}
	}

	for _, partition := range partitions {
		collectionName := e.collectionID2Name.LoadWithDefault(partition.CollectionId, "")
		if collectionName == "" {
			log.Panic("fail to get collection name for partition",
				zap.Int64("partition_id", partition.PartitionID), zap.Int64("collection_id", partition.CollectionId))
			continue
		}
		originDBName := getDBNameForCollection(partition.CollectionId)
		if originDBName == "" {
			log.Panic("fail to get db name for collection", zap.Int64("collection_id", partition.CollectionId))
			continue
		}
		// targetMilvus is nil when downstream is not milvus
		if e.targetMilvus != nil {
			dbName, err = e.targetMilvus.GetDatabaseName(context.Background(), collectionName, originDBName)
			if IsDatabaseNotFoundError(err) {
				log.Info("the collection info has been dropped in the source and target", zap.String("collection_name", collectionName))
				continue
			}
			if err != nil {
				log.Panic("fail to get database name", zap.String("collection_name", collectionName), zap.Error(err))
				continue
			}
		}
		partitionName := partition.PartitionName
		_, dropKey := util.GetPartitionInfoKeys(partitionName, collectionName, dbName)
		if partition.State == pb.PartitionState_PartitionCreated || partition.State == pb.PartitionState_PartitionCreating {
			createdPartition[dropKey] = partition.PartitionCreatedTimestamp
		} else if partition.State == pb.PartitionState_PartitionDropped || partition.State == pb.PartitionState_PartitionDropping {
			res[droppedPartitionKey][dropKey] = tt - 1
		}
	}

	for s := range res[droppedCollectionKey] {
		c, ok := createdCollection[s]
		if ok {
			res[droppedCollectionKey][s] = c - 1
		}
	}
	for s := range res[droppedPartitionKey] {
		p, ok := createdPartition[s]
		if ok {
			res[droppedPartitionKey][s] = p - 1
		}
	}
	return res
}

func IsDroppedObject(name string) bool {
	return name == TomeObject
}
