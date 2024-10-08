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

package writer

import (
	"context"
	"encoding/base64"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/requestutil"
	"github.com/milvus-io/milvus/pkg/util/retry"

	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/log"
	"github.com/zilliztech/milvus-cdc/core/util"
)

var _ api.Writer = (*ChannelWriter)(nil)

type (
	opMessageFunc func(ctx context.Context, msgBase *commonpb.MsgBase, msgPack msgstream.TsMsg) error
	apiEventFunc  func(ctx context.Context, apiEvent *api.ReplicateAPIEvent) error
)

type ChannelWriter struct {
	dataHandler    api.DataHandler
	messageManager api.MessageManager

	opMessageFuncs map[commonpb.MsgType]opMessageFunc
	apiEventFuncs  map[api.ReplicateAPIEventType]apiEventFunc

	// TODO how to gc the infos
	dbInfos         util.Map[string, uint64]
	collectionInfos util.Map[string, uint64]
	partitionInfos  util.Map[string, uint64]

	retryOptions []retry.Option
	downstream   string
}

func NewChannelWriter(dataHandler api.DataHandler,
	writerConfig config.WriterConfig,
	droppedObjs map[string]map[string]uint64,
	downstream string,
) api.Writer {
	w := &ChannelWriter{
		dataHandler:    dataHandler,
		messageManager: NewReplicateMessageManager(dataHandler, writerConfig.MessageBufferSize),
		retryOptions:   util.GetRetryOptions(writerConfig.Retry),
		downstream:     downstream,
	}
	w.initAPIEventFuncs()
	w.initOPMessageFuncs()
	log.Info("new channel writer", zap.Any("droppedObjs", droppedObjs))
	if droppedObjs[util.DroppedDatabaseKey] != nil {
		for s, u := range droppedObjs[util.DroppedDatabaseKey] {
			w.dbInfos.Store(s, u)
		}
	}
	if droppedObjs[util.DroppedCollectionKey] != nil {
		for s, u := range droppedObjs[util.DroppedCollectionKey] {
			w.collectionInfos.Store(s, u)
		}
	}
	if droppedObjs[util.DroppedPartitionKey] != nil {
		for s, u := range droppedObjs[util.DroppedPartitionKey] {
			w.partitionInfos.Store(s, u)
		}
	}

	return w
}

func (c *ChannelWriter) initAPIEventFuncs() {
	c.apiEventFuncs = map[api.ReplicateAPIEventType]apiEventFunc{
		api.ReplicateCreateCollection: c.createCollection,
		api.ReplicateDropCollection:   c.dropCollection,
		api.ReplicateCreatePartition:  c.createPartition,
		api.ReplicateDropPartition:    c.dropPartition,
	}
}

func (c *ChannelWriter) initOPMessageFuncs() {
	c.opMessageFuncs = map[commonpb.MsgType]opMessageFunc{
		commonpb.MsgType_CreateDatabase:    c.createDatabase,
		commonpb.MsgType_DropDatabase:      c.dropDatabase,
		commonpb.MsgType_AlterDatabase:     c.alterDatabase,
		commonpb.MsgType_Flush:             c.flush,
		commonpb.MsgType_CreateIndex:       c.createIndex,
		commonpb.MsgType_DropIndex:         c.dropIndex,
		commonpb.MsgType_AlterIndex:        c.alterIndex,
		commonpb.MsgType_LoadCollection:    c.loadCollection,
		commonpb.MsgType_ReleaseCollection: c.releaseCollection,
		commonpb.MsgType_LoadPartitions:    c.loadPartitions,
		commonpb.MsgType_ReleasePartitions: c.releasePartitions,
		commonpb.MsgType_CreateCredential:  c.createCredential,
		commonpb.MsgType_DeleteCredential:  c.deleteCredential,
		commonpb.MsgType_UpdateCredential:  c.updateCredential,
		commonpb.MsgType_CreateRole:        c.createRole,
		commonpb.MsgType_DropRole:          c.dropRole,
		commonpb.MsgType_OperateUserRole:   c.operateUserRole,
		commonpb.MsgType_OperatePrivilege:  c.operatePrivilege,
	}
}

func (c *ChannelWriter) HandleReplicateAPIEvent(ctx context.Context, apiEvent *api.ReplicateAPIEvent) error {
	fields := []zap.Field{
		zap.Any("event", apiEvent.EventType),
	}
	if apiEvent.CollectionInfo != nil && apiEvent.CollectionInfo.Schema != nil {
		fields = append(fields, zap.String("collection", apiEvent.CollectionInfo.Schema.GetName()))
	}
	if apiEvent.PartitionInfo != nil {
		fields = append(fields, zap.String("partition", apiEvent.PartitionInfo.PartitionName))
	}
	log.Info("receive replicate api event", fields...)
	defer func() {
		log.Info("finish to handle replicate api event", fields...)
	}()

	f, ok := c.apiEventFuncs[apiEvent.EventType]
	if !ok {
		log.Warn("unknown replicate api event", zap.Any("event", apiEvent))
		return errors.New("unknown replicate api event")
	}
	return f(ctx, apiEvent)
}

func (c *ChannelWriter) HandleReplicateMessage(ctx context.Context, channelName string, msgPack *msgstream.MsgPack) ([]byte, []byte, error) {
	if len(msgPack.Msgs) == 0 {
		log.Warn("receive empty message pack", zap.String("channel", channelName))
		return nil, nil, errors.New("receive empty message pack")
	}
	msgBytesArr := make([][]byte, 0)
	for _, msg := range msgPack.Msgs {
		if msg.Type() != commonpb.MsgType_TimeTick {
			logFields := []zap.Field{
				zap.String("channel", channelName),
				zap.String("type", msg.Type().String()),
			}
			if msg.Type() == commonpb.MsgType_Insert {
				insertMsg := msg.(*msgstream.InsertMsg)
				logFields = append(logFields,
					zap.String("collection", insertMsg.GetCollectionName()),
					zap.String("partition", insertMsg.GetPartitionName()),
					zap.Uint64("insert_data_len", insertMsg.GetNumRows()),
				)
			}
			if msg.Type() == commonpb.MsgType_Delete {
				deleteMsg := msg.(*msgstream.DeleteMsg)
				logFields = append(logFields,
					zap.String("collection", deleteMsg.GetCollectionName()),
					zap.String("partition", deleteMsg.GetPartitionName()),
					zap.Int64("delete_data_len", deleteMsg.GetNumRows()),
				)
			}

			log.Debug("replicate msg", logFields...)
		}
		msgBytes, err := msg.Marshal(msg)
		if err != nil {
			log.Warn("failed to marshal msg", zap.Error(err))
			return nil, nil, err
		}
		if _, ok := msgBytes.([]byte); !ok {
			log.Warn("failed to convert msg bytes to []byte")
			return nil, nil, err
		}
		msgBytesArr = append(msgBytesArr, msgBytes.([]byte))
	}
	replicateMessageParam := &api.ReplicateMessageParam{
		MsgBaseParam:   api.MsgBaseParam{Base: &commonpb.MsgBase{ReplicateInfo: &commonpb.ReplicateInfo{IsReplicate: true}}},
		ChannelName:    channelName,
		StartPositions: msgPack.StartPositions,
		EndPositions:   msgPack.EndPositions,
		BeginTs:        msgPack.BeginTs,
		EndTs:          msgPack.EndTs,
		MsgsBytes:      msgBytesArr,
	}
	errChan := make(chan error, 1)
	message := &api.ReplicateMessage{
		Ctx:   ctx,
		Param: replicateMessageParam,
		SuccessFunc: func(param *api.ReplicateMessageParam) {
			errChan <- nil
		},
		FailFunc: func(param *api.ReplicateMessageParam, err error) {
			errChan <- err
		},
	}
	c.messageManager.ReplicateMessage(message)
	err := <-errChan
	if err != nil {
		return nil, nil, err
	}
	endPosition := msgPack.EndPositions[len(msgPack.EndPositions)-1]
	targetMsgBytes, err := base64.StdEncoding.DecodeString(replicateMessageParam.TargetMsgPosition)
	if err != nil {
		return nil, nil, err
	}
	return endPosition.MsgID, targetMsgBytes, nil
}

func (c *ChannelWriter) HandleOpMessagePack(ctx context.Context, msgPack *msgstream.MsgPack) ([]byte, error) {
	if len(msgPack.Msgs) == 0 {
		log.Warn("receive empty message pack")
		return nil, errors.New("receive empty message pack")
	}
	endPosition := msgPack.EndPositions[len(msgPack.EndPositions)-1]
	endTs := endPosition.Timestamp
	if len(msgPack.Msgs) != 1 {
		log.Warn("only one message is allowed in a message pack", zap.Any("msgPack", msgPack))
		return nil, errors.New("only one message is allowed in a message pack")
	}
	msgBase := &commonpb.MsgBase{ReplicateInfo: &commonpb.ReplicateInfo{IsReplicate: true, MsgTimestamp: endTs}}
	for _, msg := range msgPack.Msgs {
		logFields := []zap.Field{
			zap.String("type", msg.Type().String()),
		}
		collectionName, _ := requestutil.GetCollectionNameFromRequest(msg)
		if collectionName != "" {
			logFields = append(logFields, zap.Any("collection", collectionName))
		}
		partitionName, _ := requestutil.GetPartitionNameFromRequest(msg)
		if partitionName != "" {
			logFields = append(logFields, zap.Any("partition", partitionName))
		}
		dbName, _ := requestutil.GetDbNameFromRequest(msg)
		if dbName != "" {
			logFields = append(logFields, zap.Any("database", dbName))
		}

		log.Info("receive msg", logFields...)
		f, ok := c.opMessageFuncs[msg.Type()]
		if !ok {
			log.Warn("unknown msg type", zap.Any("msg", msg))
			return nil, errors.New("unknown msg type")
		}
		err := f(ctx, msgBase, msg)
		if err != nil {
			return nil, err
		}
		log.Info("finish to handle msg", logFields...)
	}

	return endPosition.MsgID, nil
}

// WaitDatabaseReady wait for database ready, return value: skip the op or not, wait timeout or not
func (c *ChannelWriter) WaitDatabaseReady(ctx context.Context, databaseName string, msgTs uint64) InfoState {
	if databaseName == "" {
		return InfoStateCreated
	}
	createKey, dropKey := util.GetDBInfoKeys(databaseName)
	ctime, cok := c.dbInfos.Load(createKey)
	dtime, dok := c.dbInfos.Load(dropKey)

	s := getObjState(msgTs, ctime, dtime, cok, dok)
	if s != InfoStateUnknown {
		return s
	}

	err := retry.Do(ctx, func() error {
		return c.dataHandler.DescribeDatabase(ctx, &api.DescribeDatabaseParam{
			Name: databaseName,
		})
	}, c.retryOptions...)
	if err == nil {
		c.dbInfos.Store(createKey, c.dbInfos.LoadWithDefault(dropKey, 0)+1)
		return InfoStateCreated
	}
	log.Warn("database is not ready", zap.String("database", databaseName))
	return InfoStateUnknown
}

func (c *ChannelWriter) WaitCollectionReady(ctx context.Context, collectionName, databaseName string, msgTs uint64) InfoState {
	createKey, dropKey := util.GetCollectionInfoKeys(collectionName, databaseName)
	ctime, cok := c.collectionInfos.Load(createKey)
	dtime, dok := c.collectionInfos.Load(dropKey)

	s := getObjState(msgTs, ctime, dtime, cok, dok)
	if s != InfoStateUnknown {
		return s
	}

	err := retry.Do(ctx, func() error {
		return c.dataHandler.DescribeCollection(ctx, &api.DescribeCollectionParam{
			ReplicateParam: api.ReplicateParam{
				Database: databaseName,
			},
			Name: collectionName,
		})
	}, c.retryOptions...)
	if err == nil {
		c.collectionInfos.Store(createKey, c.collectionInfos.LoadWithDefault(dropKey, 0)+1)
		return InfoStateCreated
	}
	return InfoStateUnknown
}

func (c *ChannelWriter) WaitPartitionReady(ctx context.Context, collectionName, partitionName, databaseName string, msgTs uint64) InfoState {
	createKey, dropKey := util.GetPartitionInfoKeys(partitionName, collectionName, databaseName)
	ctime, cok := c.partitionInfos.Load(createKey)
	dtime, dok := c.partitionInfos.Load(dropKey)

	s := getObjState(msgTs, ctime, dtime, cok, dok)
	if s != InfoStateUnknown {
		return s
	}

	err := retry.Do(ctx, func() error {
		return c.dataHandler.DescribePartition(ctx, &api.DescribePartitionParam{
			ReplicateParam: api.ReplicateParam{
				Database: databaseName,
			},
			CollectionName: collectionName,
			PartitionName:  partitionName,
		})
	}, c.retryOptions...)

	if err == nil {
		c.partitionInfos.Store(createKey, c.partitionInfos.LoadWithDefault(dropKey, 0)+1)
		return InfoStateCreated
	}

	return InfoStateUnknown
}

// WaitObjReadyForAPIEvent wait database/collection/partition ready, return value: skip the op or not, and error
func (c *ChannelWriter) WaitObjReadyForAPIEvent(ctx context.Context, apiEvent *api.ReplicateAPIEvent, waitDatabase, waitCollection, waitPartition bool) (bool, error) {
	ts := apiEvent.ReplicateInfo.MsgTimestamp
	db := apiEvent.ReplicateParam.Database
	collection := ""
	partition := ""
	if waitCollection {
		collection = apiEvent.CollectionInfo.Schema.GetName()
	}
	if waitPartition {
		partition = apiEvent.PartitionInfo.PartitionName
	}

	return c.WaitObjReady(ctx, db, collection, partition, ts)
}

func (c *ChannelWriter) WaitObjReady(ctx context.Context, db, collection, partition string, ts uint64) (bool, error) {
	// if downstream is not milvus, skip WaitObjReady
	if c.downstream != "milvus" {
		return false, nil
	}
	if db != "" {
		state := c.WaitDatabaseReady(ctx, db, ts)
		if state == InfoStateUnknown {
			return false, errors.Newf("database[%s] is not ready", db)
		} else if state == InfoStateDropped {
			return true, nil
		}
	}
	if collection != "" {
		state := c.WaitCollectionReady(ctx, collection, db, ts)
		if state == InfoStateUnknown {
			return false, errors.Newf("collection[%s] is not ready, db: %s", collection, db)
		} else if state == InfoStateDropped {
			return true, nil
		}
	}
	if collection != "" && partition != "" {
		state := c.WaitPartitionReady(ctx, collection, partition, db, ts)
		if state == InfoStateUnknown {
			return false, errors.Newf("partition[%s] is not ready, collection: %s, db: %s", partition, collection, db)
		} else if state == InfoStateDropped {
			return true, nil
		}
	}
	return false, nil
}

func (c *ChannelWriter) createCollection(ctx context.Context, apiEvent *api.ReplicateAPIEvent) error {
	if skip, err := c.WaitObjReadyForAPIEvent(ctx, apiEvent, true, false, false); err != nil {
		return err
	} else if skip {
		log.Info("database has been dropped",
			zap.String("database", apiEvent.ReplicateParam.Database),
			zap.String("collection", util.Base64ProtoObj(apiEvent.CollectionInfo)))
		return nil
	}
	collectionInfo := apiEvent.CollectionInfo
	entitySchema := &entity.Schema{}
	entitySchema = entitySchema.ReadProto(collectionInfo.Schema)
	createParam := &api.CreateCollectionParam{
		MsgBaseParam:     api.MsgBaseParam{Base: &commonpb.MsgBase{ReplicateInfo: apiEvent.ReplicateInfo}},
		ReplicateParam:   apiEvent.ReplicateParam,
		Schema:           entitySchema,
		ShardsNum:        collectionInfo.ShardsNum,
		ConsistencyLevel: collectionInfo.ConsistencyLevel,
		Properties:       collectionInfo.Properties,
	}
	err := c.dataHandler.CreateCollection(ctx, createParam)
	if err != nil {
		log.Warn("fail to create collection", zap.Any("event", apiEvent), zap.Error(err))
		return err
	}
	return nil
}

func (c *ChannelWriter) dropCollection(ctx context.Context, apiEvent *api.ReplicateAPIEvent) error {
	if skip, err := c.WaitObjReadyForAPIEvent(ctx, apiEvent, true, false, false); err != nil {
		return err
	} else if skip {
		log.Info("database has been dropped",
			zap.String("database", apiEvent.ReplicateParam.Database),
			zap.String("collection", util.Base64ProtoObj(apiEvent.CollectionInfo)))
		return nil
	}
	collectionName := apiEvent.CollectionInfo.Schema.GetName()
	databaseName := apiEvent.ReplicateParam.Database
	dropParam := &api.DropCollectionParam{
		MsgBaseParam:   api.MsgBaseParam{Base: &commonpb.MsgBase{ReplicateInfo: apiEvent.ReplicateInfo}},
		ReplicateParam: apiEvent.ReplicateParam,
		CollectionName: collectionName,
	}
	err := c.dataHandler.DropCollection(ctx, dropParam)
	if err != nil {
		log.Warn("fail to drop collection", zap.Any("event", apiEvent), zap.Error(err))
		return err
	}
	_, dropKey := util.GetCollectionInfoKeys(collectionName, databaseName)
	c.collectionInfos.Store(dropKey, apiEvent.ReplicateInfo.MsgTimestamp)
	return nil
}

func (c *ChannelWriter) createPartition(ctx context.Context, apiEvent *api.ReplicateAPIEvent) error {
	if skip, err := c.WaitObjReadyForAPIEvent(ctx, apiEvent, true, true, false); err != nil {
		return err
	} else if skip {
		log.Info("collection has been dropped", zap.String("database", apiEvent.ReplicateParam.Database),
			zap.String("collection", apiEvent.CollectionInfo.Schema.GetName()), zap.String("partition", util.Base64ProtoObj(apiEvent.PartitionInfo)))
		return nil
	}
	createParam := &api.CreatePartitionParam{
		MsgBaseParam:   api.MsgBaseParam{Base: &commonpb.MsgBase{ReplicateInfo: apiEvent.ReplicateInfo}},
		ReplicateParam: apiEvent.ReplicateParam,
		CollectionName: apiEvent.CollectionInfo.Schema.GetName(),
		PartitionName:  apiEvent.PartitionInfo.PartitionName,
	}
	err := c.dataHandler.CreatePartition(ctx, createParam)
	if err != nil {
		log.Warn("fail to create partition", zap.Any("event", apiEvent), zap.Error(err))
		skip, _ := c.WaitObjReadyForAPIEvent(ctx, apiEvent, true, true, false)
		if !skip {
			return err
		}
		log.Info("collection has been dropped", zap.String("database", apiEvent.ReplicateParam.Database),
			zap.String("collection", apiEvent.CollectionInfo.Schema.GetName()), zap.String("partition", util.Base64ProtoObj(apiEvent.PartitionInfo)))
	}
	return nil
}

func (c *ChannelWriter) dropPartition(ctx context.Context, apiEvent *api.ReplicateAPIEvent) error {
	if skip, err := c.WaitObjReadyForAPIEvent(ctx, apiEvent, true, true, false); err != nil {
		return err
	} else if skip {
		log.Info("collection has been dropped", zap.String("database", apiEvent.ReplicateParam.Database),
			zap.String("collection", apiEvent.CollectionInfo.Schema.GetName()), zap.String("partition", util.Base64ProtoObj(apiEvent.PartitionInfo)))
		return nil
	}
	partitionName := apiEvent.PartitionInfo.PartitionName
	collectionName := apiEvent.CollectionInfo.Schema.GetName()
	databaseName := apiEvent.ReplicateParam.Database
	dropParam := &api.DropPartitionParam{
		MsgBaseParam:   api.MsgBaseParam{Base: &commonpb.MsgBase{ReplicateInfo: apiEvent.ReplicateInfo}},
		ReplicateParam: apiEvent.ReplicateParam,
		CollectionName: collectionName,
		PartitionName:  partitionName,
	}
	err := c.dataHandler.DropPartition(ctx, dropParam)
	if err != nil {
		log.Warn("fail to drop partition", zap.Any("event", apiEvent), zap.Error(err))
		skip, _ := c.WaitObjReadyForAPIEvent(ctx, apiEvent, true, true, false)
		if !skip {
			return err
		}
		log.Info("collection has been dropped", zap.String("database", apiEvent.ReplicateParam.Database),
			zap.String("collection", apiEvent.CollectionInfo.Schema.GetName()), zap.String("partition", util.Base64ProtoObj(apiEvent.PartitionInfo)))
	}
	_, dropKey := util.GetPartitionInfoKeys(partitionName, collectionName, databaseName)
	c.partitionInfos.Store(dropKey, apiEvent.ReplicateInfo.MsgTimestamp)
	return nil
}

func (c *ChannelWriter) createDatabase(ctx context.Context, msgBase *commonpb.MsgBase, msg msgstream.TsMsg) error {
	createDatabaseMsg := msg.(*msgstream.CreateDatabaseMsg)
	err := c.dataHandler.CreateDatabase(ctx, &api.CreateDatabaseParam{
		CreateDatabaseRequest: &milvuspb.CreateDatabaseRequest{
			Base:   msgBase,
			DbName: createDatabaseMsg.GetDbName(),
		},
	})
	if err != nil {
		log.Warn("failed to create database", zap.Any("msg", createDatabaseMsg), zap.Error(err))
		return err
	}
	return nil
}

func (c *ChannelWriter) dropDatabase(ctx context.Context, msgBase *commonpb.MsgBase, msg msgstream.TsMsg) error {
	dropDatabaseMsg := msg.(*msgstream.DropDatabaseMsg)
	databaseName := dropDatabaseMsg.GetDbName()
	err := c.dataHandler.DropDatabase(ctx, &api.DropDatabaseParam{
		DropDatabaseRequest: &milvuspb.DropDatabaseRequest{
			Base:   msgBase,
			DbName: databaseName,
		},
	})
	if err != nil {
		log.Warn("failed to drop database", zap.Any("msg", dropDatabaseMsg), zap.Error(err))
		return err
	}
	_, dropKey := util.GetDBInfoKeys(databaseName)
	c.dbInfos.Store(dropKey, dropDatabaseMsg.EndTs())
	return nil
}

func (c *ChannelWriter) alterDatabase(ctx context.Context, msgBase *commonpb.MsgBase, msg msgstream.TsMsg) error {
	alterDatabaseMsg := msg.(*msgstream.AlterDatabaseMsg)
	UpdateMsgBase(alterDatabaseMsg.Base, msgBase)
	err := c.dataHandler.AlterDatabase(ctx, &api.AlterDatabaseParam{
		AlterDatabaseRequest: alterDatabaseMsg.AlterDatabaseRequest,
	})
	if err != nil {
		log.Warn("failed to alter database", zap.Any("msg", alterDatabaseMsg), zap.Error(err))
		return err
	}
	return nil
}

func (c *ChannelWriter) flush(ctx context.Context, msgBase *commonpb.MsgBase, msg msgstream.TsMsg) error {
	flushMsg := msg.(*msgstream.FlushMsg)
	var collectionNames []string
	for _, s := range flushMsg.GetCollectionNames() {
		if skip, err := c.WaitObjReady(ctx, flushMsg.GetDbName(), s, "", flushMsg.EndTs()); err != nil {
			return err
		} else if skip {
			log.Info("collection has been dropped", zap.String("database", flushMsg.GetDbName()),
				zap.String("collection", s), zap.String("msg", util.Base64Msg(msg)))
			continue
		}
		collectionNames = append(collectionNames, s)
	}
	if len(collectionNames) == 0 {
		return nil
	}
	err := c.dataHandler.Flush(ctx, &api.FlushParam{
		ReplicateParam: api.ReplicateParam{
			Database: flushMsg.GetDbName(),
		},
		FlushRequest: &milvuspb.FlushRequest{
			Base:            msgBase,
			CollectionNames: collectionNames,
		},
	})
	if err != nil {
		log.Warn("failed to flush", zap.Any("msg", flushMsg), zap.Error(err))
		for _, name := range collectionNames {
			skip, _ := c.WaitObjReady(ctx, flushMsg.GetDbName(), name, "", flushMsg.EndTs())
			if !skip {
				return err
			}
		}
		log.Info("collection has been dropped", zap.String("database", flushMsg.GetDbName()),
			zap.Strings("collections", collectionNames), zap.String("msg", util.Base64Msg(msg)))
	}
	return nil
}

func (c *ChannelWriter) createIndex(ctx context.Context, msgBase *commonpb.MsgBase, msg msgstream.TsMsg) error {
	createIndexMsg := msg.(*msgstream.CreateIndexMsg)
	if skip, err := c.WaitObjReady(ctx, createIndexMsg.GetDbName(), createIndexMsg.GetCollectionName(), "", createIndexMsg.EndTs()); err != nil {
		return err
	} else if skip {
		log.Info("collection has been dropped", zap.String("database", createIndexMsg.GetDbName()),
			zap.String("collection", createIndexMsg.GetCollectionName()), zap.String("msg", util.Base64Msg(msg)))
		return nil
	}
	UpdateMsgBase(createIndexMsg.Base, msgBase)
	err := c.dataHandler.CreateIndex(ctx, &api.CreateIndexParam{
		ReplicateParam: api.ReplicateParam{
			Database: createIndexMsg.GetDbName(),
		},
		CreateIndexRequest: createIndexMsg.CreateIndexRequest,
	})
	if err != nil {
		log.Warn("fail to create index", zap.Any("msg", createIndexMsg), zap.Error(err))
		skip, _ := c.WaitObjReady(ctx, createIndexMsg.GetDbName(), createIndexMsg.GetCollectionName(), "", createIndexMsg.EndTs())
		if !skip {
			return err
		}
		log.Info("collection has been dropped", zap.String("database", createIndexMsg.GetDbName()),
			zap.String("collection", createIndexMsg.GetCollectionName()), zap.String("msg", util.Base64Msg(msg)))
	}
	return nil
}

func (c *ChannelWriter) dropIndex(ctx context.Context, msgBase *commonpb.MsgBase, msg msgstream.TsMsg) error {
	dropIndexMsg := msg.(*msgstream.DropIndexMsg)
	if skip, err := c.WaitObjReady(ctx, dropIndexMsg.GetDbName(), dropIndexMsg.GetCollectionName(), "", dropIndexMsg.EndTs()); err != nil {
		return err
	} else if skip {
		log.Info("collection has been dropped", zap.String("database", dropIndexMsg.GetDbName()),
			zap.String("collection", dropIndexMsg.GetCollectionName()), zap.String("msg", util.Base64Msg(msg)))
		return nil
	}
	err := c.dataHandler.DropIndex(ctx, &api.DropIndexParam{
		ReplicateParam: api.ReplicateParam{
			Database: dropIndexMsg.GetDbName(),
		},
		DropIndexRequest: &milvuspb.DropIndexRequest{
			Base:           msgBase,
			CollectionName: dropIndexMsg.GetCollectionName(),
			FieldName:      dropIndexMsg.GetFieldName(),
			IndexName:      dropIndexMsg.GetIndexName(),
		},
	})
	if err != nil {
		log.Warn("fail to drop index", zap.Any("msg", dropIndexMsg), zap.Error(err))
		skip, _ := c.WaitObjReady(ctx, dropIndexMsg.GetDbName(), dropIndexMsg.GetCollectionName(), "", dropIndexMsg.EndTs())
		if !skip {
			return err
		}
		log.Info("collection has been dropped", zap.String("database", dropIndexMsg.GetDbName()),
			zap.String("collection", dropIndexMsg.GetCollectionName()), zap.String("msg", util.Base64Msg(msg)))
	}
	return nil
}

func (c *ChannelWriter) alterIndex(ctx context.Context, msgBase *commonpb.MsgBase, msg msgstream.TsMsg) error {
	alterIndexMsg := msg.(*msgstream.AlterIndexMsg)
	if skip, err := c.WaitObjReady(ctx, alterIndexMsg.GetDbName(), alterIndexMsg.GetCollectionName(), "", alterIndexMsg.EndTs()); err != nil {
		return err
	} else if skip {
		log.Info("collection has been dropped", zap.String("database", alterIndexMsg.GetDbName()),
			zap.String("collection", alterIndexMsg.GetCollectionName()), zap.String("msg", util.Base64Msg(msg)))
		return nil
	}
	UpdateMsgBase(alterIndexMsg.Base, msgBase)
	err := c.dataHandler.AlterIndex(ctx, &api.AlterIndexParam{
		AlterIndexRequest: alterIndexMsg.AlterIndexRequest,
	})
	if err != nil {
		log.Warn("failed to alter index", zap.Any("msg", alterIndexMsg), zap.Error(err))
		return err
	}
	return nil
}

func (c *ChannelWriter) loadCollection(ctx context.Context, msgBase *commonpb.MsgBase, msg msgstream.TsMsg) error {
	loadCollectionMsg := msg.(*msgstream.LoadCollectionMsg)
	if skip, err := c.WaitObjReady(ctx, loadCollectionMsg.GetDbName(), loadCollectionMsg.GetCollectionName(), "", loadCollectionMsg.EndTs()); err != nil {
		return err
	} else if skip {
		log.Info("collection has been dropped", zap.String("database", loadCollectionMsg.GetDbName()),
			zap.String("collection", loadCollectionMsg.GetCollectionName()), zap.String("msg", util.Base64Msg(msg)))
		return nil
	}
	UpdateMsgBase(loadCollectionMsg.Base, msgBase)
	err := c.dataHandler.LoadCollection(ctx, &api.LoadCollectionParam{
		ReplicateParam: api.ReplicateParam{
			Database: loadCollectionMsg.GetDbName(),
		},
		LoadCollectionRequest: loadCollectionMsg.LoadCollectionRequest,
	})
	if err != nil {
		log.Warn("fail to load collection", zap.Any("msg", loadCollectionMsg), zap.Error(err))
		skip, _ := c.WaitObjReady(ctx, loadCollectionMsg.GetDbName(), loadCollectionMsg.GetCollectionName(), "", loadCollectionMsg.EndTs())
		if !skip {
			return err
		}
		log.Info("collection has been dropped", zap.String("database", loadCollectionMsg.GetDbName()),
			zap.String("collection", loadCollectionMsg.GetCollectionName()), zap.String("msg", util.Base64Msg(msg)))
	}
	return nil
}

func (c *ChannelWriter) releaseCollection(ctx context.Context, msgBase *commonpb.MsgBase, msg msgstream.TsMsg) error {
	releaseCollectionMsg := msg.(*msgstream.ReleaseCollectionMsg)
	if skip, err := c.WaitObjReady(ctx, releaseCollectionMsg.GetDbName(), releaseCollectionMsg.GetCollectionName(), "", releaseCollectionMsg.EndTs()); err != nil {
		return err
	} else if skip {
		log.Info("collection has been dropped", zap.String("database", releaseCollectionMsg.GetDbName()),
			zap.String("collection", releaseCollectionMsg.GetCollectionName()), zap.String("msg", util.Base64Msg(msg)))
		return nil
	}
	err := c.dataHandler.ReleaseCollection(ctx, &api.ReleaseCollectionParam{
		ReplicateParam: api.ReplicateParam{
			Database: releaseCollectionMsg.GetDbName(),
		},
		ReleaseCollectionRequest: &milvuspb.ReleaseCollectionRequest{
			Base:           msgBase,
			CollectionName: releaseCollectionMsg.GetCollectionName(),
		},
	})
	if err != nil {
		log.Warn("fail to release collection", zap.Any("msg", releaseCollectionMsg), zap.Error(err))
		skip, _ := c.WaitObjReady(ctx, releaseCollectionMsg.GetDbName(), releaseCollectionMsg.GetCollectionName(), "", releaseCollectionMsg.EndTs())
		if !skip {
			return err
		}
		log.Info("collection has been dropped", zap.String("database", releaseCollectionMsg.GetDbName()),
			zap.String("collection", releaseCollectionMsg.GetCollectionName()), zap.String("msg", util.Base64Msg(msg)))
	}
	return nil
}

func (c *ChannelWriter) loadPartitions(ctx context.Context, msgBase *commonpb.MsgBase, msg msgstream.TsMsg) error {
	loadPartitionsMsg := msg.(*msgstream.LoadPartitionsMsg)
	var partitions []string
	for _, s := range loadPartitionsMsg.GetPartitionNames() {
		if skip, err := c.WaitObjReady(ctx, loadPartitionsMsg.GetDbName(), loadPartitionsMsg.GetCollectionName(), s, loadPartitionsMsg.EndTs()); err != nil {
			return err
		} else if skip {
			log.Info("partition has been dropped", zap.String("database", loadPartitionsMsg.GetDbName()),
				zap.String("collection", loadPartitionsMsg.GetCollectionName()), zap.String("partition", s), zap.String("msg", util.Base64Msg(msg)))
			continue
		}
		partitions = append(partitions, s)
	}
	if len(partitions) == 0 {
		return nil
	}
	err := c.dataHandler.LoadPartitions(ctx, &api.LoadPartitionsParam{
		ReplicateParam: api.ReplicateParam{
			Database: loadPartitionsMsg.GetDbName(),
		},
		LoadPartitionsRequest: &milvuspb.LoadPartitionsRequest{
			Base:           msgBase,
			CollectionName: loadPartitionsMsg.GetCollectionName(),
			PartitionNames: partitions,
			ReplicaNumber:  loadPartitionsMsg.GetReplicaNumber(),
		},
	})
	if err != nil {
		log.Warn("fail to load partitions", zap.Any("msg", loadPartitionsMsg), zap.Error(err))
		for _, p := range partitions {
			skip, _ := c.WaitObjReady(ctx, loadPartitionsMsg.GetDbName(), loadPartitionsMsg.GetCollectionName(), p, loadPartitionsMsg.EndTs())
			if !skip {
				return err
			}
		}
		log.Info("partition has been dropped", zap.String("database", loadPartitionsMsg.GetDbName()),
			zap.String("collection", loadPartitionsMsg.GetCollectionName()), zap.Strings("partitions", partitions), zap.String("msg", util.Base64Msg(msg)))
	}
	return nil
}

func (c *ChannelWriter) releasePartitions(ctx context.Context, msgBase *commonpb.MsgBase, msg msgstream.TsMsg) error {
	releasePartitionsMsg := msg.(*msgstream.ReleasePartitionsMsg)
	var partitions []string
	for _, s := range releasePartitionsMsg.GetPartitionNames() {
		if skip, err := c.WaitObjReady(ctx, releasePartitionsMsg.GetDbName(), releasePartitionsMsg.GetCollectionName(), s, releasePartitionsMsg.EndTs()); err != nil {
			return err
		} else if skip {
			log.Info("partition has been dropped", zap.String("database", releasePartitionsMsg.GetDbName()),
				zap.String("collection", releasePartitionsMsg.GetCollectionName()), zap.String("partition", s), zap.String("msg", util.Base64Msg(msg)))
			continue
		}
		partitions = append(partitions, s)
	}
	if len(partitions) == 0 {
		return nil
	}
	err := c.dataHandler.ReleasePartitions(ctx, &api.ReleasePartitionsParam{
		ReplicateParam: api.ReplicateParam{
			Database: releasePartitionsMsg.GetDbName(),
		},
		ReleasePartitionsRequest: &milvuspb.ReleasePartitionsRequest{
			Base:           msgBase,
			CollectionName: releasePartitionsMsg.GetCollectionName(),
			PartitionNames: partitions,
		},
	})
	if err != nil {
		log.Warn("fail to release partitions", zap.Any("msg", releasePartitionsMsg), zap.Error(err))
		for _, p := range partitions {
			skip, _ := c.WaitObjReady(ctx, releasePartitionsMsg.GetDbName(), releasePartitionsMsg.GetCollectionName(), p, releasePartitionsMsg.EndTs())
			if !skip {
				return err
			}
		}
		log.Info("partition has been dropped", zap.String("database", releasePartitionsMsg.GetDbName()),
			zap.String("collection", releasePartitionsMsg.GetCollectionName()), zap.Strings("partitions", partitions), zap.String("msg", util.Base64Msg(msg)))
	}
	return nil
}

func (c *ChannelWriter) createCredential(ctx context.Context, msgBase *commonpb.MsgBase, msg msgstream.TsMsg) error {
	createUserMsg := msg.(*msgstream.CreateUserMsg)
	UpdateMsgBase(createUserMsg.Base, msgBase)
	err := c.dataHandler.CreateUser(ctx, &api.CreateUserParam{
		CreateCredentialRequest: createUserMsg.CreateCredentialRequest,
	})
	if err != nil {
		log.Warn("failed to create credential", zap.Any("msg", createUserMsg), zap.Error(err))
		return err
	}
	return nil
}

func (c *ChannelWriter) deleteCredential(ctx context.Context, msgBase *commonpb.MsgBase, msg msgstream.TsMsg) error {
	deleteUserMsg := msg.(*msgstream.DeleteUserMsg)
	UpdateMsgBase(deleteUserMsg.Base, msgBase)
	err := c.dataHandler.DeleteUser(ctx, &api.DeleteUserParam{
		DeleteCredentialRequest: deleteUserMsg.DeleteCredentialRequest,
	})
	if err != nil {
		log.Warn("failed to drop credential", zap.Any("msg", deleteUserMsg), zap.Error(err))
		return err
	}
	return nil
}

func (c *ChannelWriter) updateCredential(ctx context.Context, msgBase *commonpb.MsgBase, msg msgstream.TsMsg) error {
	updateUserMsg := msg.(*msgstream.UpdateUserMsg)
	UpdateMsgBase(updateUserMsg.Base, msgBase)
	err := c.dataHandler.UpdateUser(ctx, &api.UpdateUserParam{
		UpdateCredentialRequest: updateUserMsg.UpdateCredentialRequest,
	})
	if err != nil {
		log.Warn("failed to update user", zap.Any("msg", updateUserMsg), zap.Error(err))
		return err
	}
	return nil
}

func (c *ChannelWriter) createRole(ctx context.Context, msgBase *commonpb.MsgBase, msg msgstream.TsMsg) error {
	createRoleMsg := msg.(*msgstream.CreateRoleMsg)
	UpdateMsgBase(createRoleMsg.Base, msgBase)
	err := c.dataHandler.CreateRole(ctx, &api.CreateRoleParam{
		CreateRoleRequest: createRoleMsg.CreateRoleRequest,
	})
	if err != nil {
		log.Warn("failed to create role", zap.Any("msg", createRoleMsg), zap.Error(err))
		return err
	}
	return nil
}

func (c *ChannelWriter) dropRole(ctx context.Context, msgBase *commonpb.MsgBase, msg msgstream.TsMsg) error {
	dropRoleMsg := msg.(*msgstream.DropRoleMsg)
	UpdateMsgBase(dropRoleMsg.Base, msgBase)
	err := c.dataHandler.DropRole(ctx, &api.DropRoleParam{
		DropRoleRequest: dropRoleMsg.DropRoleRequest,
	})
	if err != nil {
		log.Warn("failed to drop role", zap.Any("msg", dropRoleMsg), zap.Error(err))
		return err
	}
	return nil
}

func (c *ChannelWriter) operateUserRole(ctx context.Context, msgBase *commonpb.MsgBase, msg msgstream.TsMsg) error {
	operateUserRoleMsg := msg.(*msgstream.OperateUserRoleMsg)
	UpdateMsgBase(operateUserRoleMsg.Base, msgBase)
	err := c.dataHandler.OperateUserRole(ctx, &api.OperateUserRoleParam{
		OperateUserRoleRequest: operateUserRoleMsg.OperateUserRoleRequest,
	})
	if err != nil {
		log.Warn("failed to operate user role", zap.Any("msg", operateUserRoleMsg), zap.Error(err))
		return err
	}
	return nil
}

func (c *ChannelWriter) operatePrivilege(ctx context.Context, msgBase *commonpb.MsgBase, msg msgstream.TsMsg) error {
	operatePrivilegeMsg := msg.(*msgstream.OperatePrivilegeMsg)
	UpdateMsgBase(operatePrivilegeMsg.Base, msgBase)
	err := c.dataHandler.OperatePrivilege(ctx, &api.OperatePrivilegeParam{
		OperatePrivilegeRequest: operatePrivilegeMsg.OperatePrivilegeRequest,
	})
	if err != nil {
		log.Warn("failed to operate privilege", zap.Any("msg", operatePrivilegeMsg), zap.Error(err))
		return err
	}
	return nil
}

func UpdateMsgBase(msgBase *commonpb.MsgBase, withReplicateInfo *commonpb.MsgBase) {
	msgBase.ReplicateInfo = withReplicateInfo.ReplicateInfo
}

// shouldSkipOp, mtime: msg time, ctime: create time, dtime: drop time
func getObjState(mtime, ctime, dtime uint64, cok, dok bool) InfoState {
	// no info, should check it from api
	if !cok && !dok {
		return InfoStateUnknown
	}
	if !cok && dok {
		// the object (like database/collection/partition) has been drop, skip the op
		if mtime <= dtime {
			log.Info("skip op because the object has been drop",
				zap.Uint64("mtime", mtime),
				zap.Uint64("dtime", dtime))
			return InfoStateDropped
		}
		return InfoStateUnknown
	}
	if cok && !dok {
		if ctime <= mtime {
			return InfoStateCreated
		}
		log.Info("skip op because the object has been drop",
			zap.Uint64("mtime", mtime),
			zap.Uint64("ctime", ctime))
		return InfoStateDropped
	}

	if ctime >= dtime {
		if mtime >= ctime {
			return InfoStateCreated
		}
		log.Info("skip op because the object has been drop",
			zap.Uint64("mtime", mtime),
			zap.Uint64("ctime", ctime))
		return InfoStateDropped
	}
	if mtime > dtime {
		return InfoStateUnknown
	}
	log.Info("skip op because the object has been drop",
		zap.Uint64("mtime", mtime),
		zap.Uint64("dtime", dtime))
	return InfoStateDropped
}

type InfoState int

const (
	InfoStateUnknown InfoState = iota + 1
	InfoStateCreated
	InfoStateDropped
)
