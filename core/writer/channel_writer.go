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
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	pb "github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/util/requestutil"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"

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
	replicateMeta  api.ReplicateMeta

	opMessageFuncs map[commonpb.MsgType]opMessageFunc
	apiEventFuncs  map[api.ReplicateAPIEventType]apiEventFunc

	// TODO how to gc the infos
	dbInfos         util.Map[string, uint64]
	collectionInfos util.Map[string, uint64]
	partitionInfos  util.Map[string, uint64]
	nameMappings    util.Map[string, string]

	retryOptions []retry.Option
	downstream   string
	replicateID  string
}

func NewChannelWriter(
	dataHandler api.DataHandler,
	replicateMeta api.ReplicateMeta,
	writerConfig config.WriterConfig,
	droppedObjs map[string]map[string]uint64,
	downstream string,
) api.Writer {
	w := &ChannelWriter{
		dataHandler:    dataHandler,
		replicateMeta:  replicateMeta,
		messageManager: NewReplicateMessageManager(dataHandler, writerConfig.MessageBufferSize),
		retryOptions:   util.GetRetryOptions(writerConfig.Retry),
		downstream:     downstream,
		replicateID:    writerConfig.ReplicateID,
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
		commonpb.MsgType_CreateDatabase:        c.createDatabase,
		commonpb.MsgType_DropDatabase:          c.dropDatabase,
		commonpb.MsgType_AlterDatabase:         c.alterDatabase,
		commonpb.MsgType_Flush:                 c.flush,
		commonpb.MsgType_CreateIndex:           c.createIndex,
		commonpb.MsgType_DropIndex:             c.dropIndex,
		commonpb.MsgType_AlterIndex:            c.alterIndex,
		commonpb.MsgType_LoadCollection:        c.loadCollection,
		commonpb.MsgType_ReleaseCollection:     c.releaseCollection,
		commonpb.MsgType_LoadPartitions:        c.loadPartitions,
		commonpb.MsgType_ReleasePartitions:     c.releasePartitions,
		commonpb.MsgType_CreateCredential:      c.createCredential,
		commonpb.MsgType_DeleteCredential:      c.deleteCredential,
		commonpb.MsgType_UpdateCredential:      c.updateCredential,
		commonpb.MsgType_CreateRole:            c.createRole,
		commonpb.MsgType_DropRole:              c.dropRole,
		commonpb.MsgType_OperateUserRole:       c.operateUserRole,
		commonpb.MsgType_OperatePrivilege:      c.operatePrivilege,
		commonpb.MsgType_OperatePrivilegeV2:    c.operatePrivilegeV2,
		commonpb.MsgType_CreatePrivilegeGroup:  c.createPrivilegeGroup,
		commonpb.MsgType_DropPrivilegeGroup:    c.dropPrivilegeGroup,
		commonpb.MsgType_OperatePrivilegeGroup: c.operatePrivilegeGroup,
	}
}

func (c *ChannelWriter) HandleReplicateAPIEvent(ctx context.Context, apiEvent *api.ReplicateAPIEvent) error {
	fields := []zap.Field{
		zap.Any("event", apiEvent.EventType),
		zap.String("db", apiEvent.ReplicateParam.Database),
	}
	if apiEvent.CollectionInfo != nil && apiEvent.CollectionInfo.Schema != nil {
		fields = append(fields,
			zap.String("collection", apiEvent.CollectionInfo.Schema.GetName()),
		)
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
		if c.replicateID != "" {
			msgBase, ok := msg.(interface{ GetBase() *commonpb.MsgBase })
			if ok {
				replicateInfo := msgBase.GetBase().ReplicateInfo
				if replicateInfo == nil {
					replicateInfo = &commonpb.ReplicateInfo{}
					msgBase.GetBase().ReplicateInfo = replicateInfo
				}
				replicateInfo.IsReplicate = true
				replicateInfo.ReplicateID = c.replicateID
			} else {
				log.Warn("failed to get replicate info", zap.Any("msg", msg.Type()))
			}
			if msg.Type() == commonpb.MsgType_TimeTick {
				msg = &msgstream.ReplicateMsg{
					BaseMsg: msgstream.BaseMsg{
						BeginTimestamp: msg.BeginTs(),
						EndTimestamp:   msg.EndTs(),
						HashValues:     []uint32{0},
					},
					ReplicateMsg: &msgpb.ReplicateMsg{
						Base: &commonpb.MsgBase{
							MsgType:   commonpb.MsgType_Replicate,
							Timestamp: msg.EndTs(),
							ReplicateInfo: &commonpb.ReplicateInfo{
								IsReplicate: true,
								ReplicateID: c.replicateID,
							},
						},
						IsEnd:      false,
						Database:   "",
						Collection: "",
					},
				}
			}
		}
		if msg.Type() != commonpb.MsgType_TimeTick {
			logFields := []zap.Field{
				zap.String("channel", channelName),
				zap.String("type", msg.Type().String()),
			}
			if msg.Type() == commonpb.MsgType_Insert {
				insertMsg := msg.(*msgstream.InsertMsg)
				logFields = append(logFields,
					zap.String("db", insertMsg.GetDbName()),
					zap.String("collection", insertMsg.GetCollectionName()),
					zap.String("partition", insertMsg.GetPartitionName()),
					zap.Uint64("insert_data_len", insertMsg.GetNumRows()),
				)
				dbName := insertMsg.GetDbName()
				colName := insertMsg.GetCollectionName()
				dbName, colName = c.mapDBAndCollectionName(dbName, colName)
				insertMsg.DbName = dbName
				insertMsg.CollectionName = colName
			}
			if msg.Type() == commonpb.MsgType_Delete {
				deleteMsg := msg.(*msgstream.DeleteMsg)
				logFields = append(logFields,
					zap.String("db", deleteMsg.GetDbName()),
					zap.String("collection", deleteMsg.GetCollectionName()),
					zap.String("partition", deleteMsg.GetPartitionName()),
					zap.Int64("delete_data_len", deleteMsg.GetNumRows()),
				)
				dbName := deleteMsg.GetDbName()
				colName := deleteMsg.GetCollectionName()
				dbName, colName = c.mapDBAndCollectionName(dbName, colName)
				deleteMsg.DbName = dbName
				deleteMsg.CollectionName = colName
			}
			if msg.Type() == commonpb.MsgType_DropPartition {
				dropPartitionMsg := msg.(*msgstream.DropPartitionMsg)
				logFields = append(logFields,
					zap.String("db", dropPartitionMsg.GetDbName()),
					zap.String("collection", dropPartitionMsg.GetCollectionName()),
				)
				dbName := dropPartitionMsg.GetDbName()
				colName := dropPartitionMsg.GetCollectionName()
				dbName, colName = c.mapDBAndCollectionName(dbName, colName)
				dropPartitionMsg.DbName = dbName
				dropPartitionMsg.CollectionName = colName
			}
			if msg.Type() == commonpb.MsgType_DropCollection {
				dropCollectionMsg := msg.(*msgstream.DropCollectionMsg)
				logFields = append(logFields,
					zap.String("db", dropCollectionMsg.GetDbName()),
					zap.String("collection", dropCollectionMsg.GetCollectionName()),
				)
				dbName := dropCollectionMsg.GetDbName()
				colName := dropCollectionMsg.GetCollectionName()
				dbName, colName = c.mapDBAndCollectionName(dbName, colName)
				dropCollectionMsg.DbName = dbName
				dropCollectionMsg.CollectionName = colName
			}
			if msg.Type() == commonpb.MsgType_Import {
				importMsg := msg.(*msgstream.ImportMsg)
				logFields = append(logFields,
					zap.String("db", importMsg.GetDbName()),
					zap.String("collection", importMsg.GetCollectionName()),
				)
				dbName := importMsg.GetDbName()
				colName := importMsg.GetCollectionName()
				dbName, colName = c.mapDBAndCollectionName(dbName, colName)
				importMsg.DbName = dbName
				importMsg.CollectionName = colName
			}
			if msg.Type() == commonpb.MsgType_Replicate {
				replicateMsg := msg.(*msgstream.ReplicateMsg)
				logFields = append(logFields,
					zap.String("replicate_id", replicateMsg.GetBase().GetReplicateInfo().GetReplicateID()),
					zap.Any("ts", replicateMsg.EndTs()),
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
		indexName, _ := util.GetIndexNameFromRequest(msg)
		if indexName != "" {
			logFields = append(logFields, zap.Any("index", indexName))
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

func (c *ChannelWriter) RecoveryMetaMsg(ctx context.Context, taskID string) error {
	dropCollectionMetaMsgs, err := c.replicateMeta.GetTaskDropCollectionMsg(ctx, taskID, "")
	if err != nil {
		log.Warn("fail to get task drop collection msg", zap.Error(err))
		return err
	}
	for _, metaMsg := range dropCollectionMetaMsgs {
		if !metaMsg.Base.IsReady() {
			continue
		}
		err = c.HandleReplicateAPIEvent(ctx, &api.ReplicateAPIEvent{
			EventType: api.ReplicateDropCollection,
			CollectionInfo: &pb.CollectionInfo{
				Schema: &schemapb.CollectionSchema{
					Name: metaMsg.CollectionName,
				},
			},
			ReplicateInfo: &commonpb.ReplicateInfo{
				IsReplicate:  true,
				MsgTimestamp: metaMsg.DropTS,
			},
			ReplicateParam: api.ReplicateParam{
				Database: metaMsg.DatabaseName,
			},
			TaskID: metaMsg.Base.TaskID,
			MsgID:  metaMsg.Base.MsgID,
		})
		if err != nil {
			log.Warn("fail to handle replicate api event", zap.Error(err))
			return err
		}
	}

	dropPartitionMetaMsgs, err := c.replicateMeta.GetTaskDropPartitionMsg(ctx, taskID, "")
	if err != nil {
		log.Warn("fail to get task drop partition msg", zap.Error(err))
		return err
	}
	for _, metaMsg := range dropPartitionMetaMsgs {
		if !metaMsg.Base.IsReady() {
			continue
		}
		err = c.HandleReplicateAPIEvent(ctx, &api.ReplicateAPIEvent{
			EventType: api.ReplicateDropPartition,
			CollectionInfo: &pb.CollectionInfo{
				Schema: &schemapb.CollectionSchema{
					Name: metaMsg.CollectionName,
				},
			},
			PartitionInfo: &pb.PartitionInfo{
				PartitionName: metaMsg.PartitionName,
			},
			ReplicateInfo: &commonpb.ReplicateInfo{
				IsReplicate:  true,
				MsgTimestamp: metaMsg.DropTS,
			},
			ReplicateParam: api.ReplicateParam{
				Database: metaMsg.DatabaseName,
			},
			TaskID: metaMsg.Base.TaskID,
			MsgID:  metaMsg.Base.MsgID,
		})
		if err != nil {
			log.Warn("fail to handle replicate api event", zap.Error(err))
			return err
		}
	}
	return nil
}

// WaitDatabaseReady wait for database ready, return value: skip the op or not, wait timeout or not
func (c *ChannelWriter) WaitDatabaseReady(ctx context.Context, databaseName string, msgTs uint64, collectionName string) InfoState {
	if databaseName == "" || databaseName == util.DefaultDbName {
		return InfoStateCreated
	}
	createKey, dropKey := util.GetDBInfoKeys(databaseName)
	ctime, cok := c.dbInfos.Load(createKey)
	dtime, dok := c.dbInfos.Load(dropKey)

	s := getObjState(msgTs, ctime, dtime, cok, dok)
	if s != InfoStateUnknown {
		return s
	}

	dbName, _ := c.mapDBAndCollectionName(databaseName, collectionName)
	err := retry.Do(ctx, func() error {
		return c.dataHandler.DescribeDatabase(ctx, &api.DescribeDatabaseParam{
			Name: dbName,
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

	dbName, colName := c.mapDBAndCollectionName(databaseName, collectionName)
	err := retry.Do(ctx, func() error {
		return c.dataHandler.DescribeCollection(ctx, &api.DescribeCollectionParam{
			ReplicateParam: api.ReplicateParam{
				Database: dbName,
			},
			Name: colName,
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

	dbName, colName := c.mapDBAndCollectionName(databaseName, collectionName)
	err := retry.Do(ctx, func() error {
		return c.dataHandler.DescribePartition(ctx, &api.DescribePartitionParam{
			ReplicateParam: api.ReplicateParam{
				Database: dbName,
			},
			CollectionName: colName,
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
		state := c.WaitDatabaseReady(ctx, db, ts, collection)
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
	dbName, colName := c.mapDBAndCollectionName(apiEvent.ReplicateParam.Database, entitySchema.CollectionName)
	apiEvent.ReplicateParam.Database = dbName
	entitySchema.CollectionName = colName
	if c.replicateID != "" {
		collectionInfo.Properties = append(collectionInfo.Properties, &commonpb.KeyValuePair{
			Key:   "replicate.id",
			Value: c.replicateID,
		})
	}
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
	dbName, colName := c.mapDBAndCollectionName(databaseName, collectionName)
	apiEvent.ReplicateParam.Database = dbName
	dropParam := &api.DropCollectionParam{
		MsgBaseParam:   api.MsgBaseParam{Base: &commonpb.MsgBase{ReplicateInfo: apiEvent.ReplicateInfo}},
		ReplicateParam: apiEvent.ReplicateParam,
		CollectionName: colName,
	}
	err := c.dataHandler.DropCollection(ctx, dropParam)
	if err != nil {
		log.Warn("fail to drop collection", zap.Any("event", apiEvent), zap.Error(err))
		return err
	}
	_, dropKey := util.GetCollectionInfoKeys(collectionName, databaseName)
	c.collectionInfos.Store(dropKey, apiEvent.ReplicateInfo.MsgTimestamp)
	err = c.replicateMeta.RemoveTaskMsg(ctx, apiEvent.TaskID, apiEvent.MsgID)
	if err != nil {
		log.Warn("fail to remove task msg", zap.Error(err))
		return err
	}
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
	dbName, colName := c.mapDBAndCollectionName(apiEvent.ReplicateParam.Database, apiEvent.CollectionInfo.Schema.GetName())
	apiEvent.ReplicateParam.Database = dbName
	createParam := &api.CreatePartitionParam{
		MsgBaseParam:   api.MsgBaseParam{Base: &commonpb.MsgBase{ReplicateInfo: apiEvent.ReplicateInfo}},
		ReplicateParam: apiEvent.ReplicateParam,
		CollectionName: colName,
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
	dbName, colName := c.mapDBAndCollectionName(databaseName, collectionName)
	apiEvent.ReplicateParam.Database = dbName
	dropParam := &api.DropPartitionParam{
		MsgBaseParam:   api.MsgBaseParam{Base: &commonpb.MsgBase{ReplicateInfo: apiEvent.ReplicateInfo}},
		ReplicateParam: apiEvent.ReplicateParam,
		CollectionName: colName,
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
	err = c.replicateMeta.RemoveTaskMsg(ctx, apiEvent.TaskID, apiEvent.MsgID)
	if err != nil {
		log.Warn("fail to remove task msg", zap.Error(err))
		return err
	}
	return nil
}

func (c *ChannelWriter) createDatabase(ctx context.Context, msgBase *commonpb.MsgBase, msg msgstream.TsMsg) error {
	createDatabaseMsg := msg.(*msgstream.CreateDatabaseMsg)
	dbName, _ := c.mapDBAndCollectionName(createDatabaseMsg.GetDbName(), "")
	err := c.dataHandler.CreateDatabase(ctx, &api.CreateDatabaseParam{
		CreateDatabaseRequest: &milvuspb.CreateDatabaseRequest{
			Base:   msgBase,
			DbName: dbName,
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
	dbName, _ := c.mapDBAndCollectionName(databaseName, "")
	err := c.dataHandler.DropDatabase(ctx, &api.DropDatabaseParam{
		DropDatabaseRequest: &milvuspb.DropDatabaseRequest{
			Base:   msgBase,
			DbName: dbName,
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
	dbName, _ := c.mapDBAndCollectionName(alterDatabaseMsg.GetDbName(), "")
	alterDatabaseMsg.AlterDatabaseRequest.DbName = dbName
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
	var mapCollectionNames []string
	var mapDBName string
	for _, s := range flushMsg.GetCollectionNames() {
		if skip, err := c.WaitObjReady(ctx, flushMsg.GetDbName(), s, "", flushMsg.EndTs()); err != nil {
			return err
		} else if skip {
			log.Info("collection has been dropped", zap.String("database", flushMsg.GetDbName()),
				zap.String("collection", s), zap.String("msg", util.Base64Msg(msg)))
			continue
		}
		collectionNames = append(collectionNames, s)
		dbName, colName := c.mapDBAndCollectionName(flushMsg.GetDbName(), s)
		if mapDBName == "" {
			mapDBName = dbName
		} else if mapDBName != dbName {
			log.Warn("flush msg has multiple databases", zap.String("db1", mapDBName), zap.String("db2", dbName))
			return errors.New("flush msg has multiple databases")
		}
		mapCollectionNames = append(mapCollectionNames, colName)
	}
	if len(collectionNames) == 0 {
		return nil
	}
	err := c.dataHandler.Flush(ctx, &api.FlushParam{
		ReplicateParam: api.ReplicateParam{
			Database: mapDBName,
		},
		FlushRequest: &milvuspb.FlushRequest{
			Base:            msgBase,
			CollectionNames: mapCollectionNames,
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
	databaseName := createIndexMsg.GetDbName()
	collectionName := createIndexMsg.GetCollectionName()
	dbName, colName := c.mapDBAndCollectionName(databaseName, collectionName)
	createIndexMsg.DbName = dbName
	createIndexMsg.CollectionName = colName
	err := c.dataHandler.CreateIndex(ctx, &api.CreateIndexParam{
		ReplicateParam: api.ReplicateParam{
			Database: dbName,
		},
		CreateIndexRequest: createIndexMsg.CreateIndexRequest,
	})
	if err != nil {
		log.Warn("fail to create index", zap.Any("msg", createIndexMsg), zap.Error(err))
		skip, _ := c.WaitObjReady(ctx, databaseName, collectionName, "", createIndexMsg.EndTs())
		if !skip {
			return err
		}
		log.Info("collection has been dropped", zap.String("database", databaseName),
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
	databaseName := dropIndexMsg.GetDbName()
	collectionName := dropIndexMsg.GetCollectionName()
	dbName, colName := c.mapDBAndCollectionName(databaseName, collectionName)
	err := c.dataHandler.DropIndex(ctx, &api.DropIndexParam{
		ReplicateParam: api.ReplicateParam{
			Database: dbName,
		},
		DropIndexRequest: &milvuspb.DropIndexRequest{
			Base:           msgBase,
			CollectionName: colName,
			FieldName:      dropIndexMsg.GetFieldName(),
			IndexName:      dropIndexMsg.GetIndexName(),
		},
	})
	if err != nil {
		log.Warn("fail to drop index", zap.Any("msg", dropIndexMsg), zap.Error(err))
		skip, _ := c.WaitObjReady(ctx, databaseName, collectionName, "", dropIndexMsg.EndTs())
		if !skip {
			return err
		}
		log.Info("collection has been dropped", zap.String("database", databaseName),
			zap.String("collection", collectionName), zap.String("msg", util.Base64Msg(msg)))
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
	alterIndexMsg.DbName, alterIndexMsg.CollectionName = c.mapDBAndCollectionName(
		alterIndexMsg.GetDbName(), alterIndexMsg.GetCollectionName())
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
	databaseName := loadCollectionMsg.GetDbName()
	collectionName := loadCollectionMsg.GetCollectionName()
	dbName, colName := c.mapDBAndCollectionName(databaseName, collectionName)
	loadCollectionMsg.DbName = dbName
	loadCollectionMsg.CollectionName = colName
	err := c.dataHandler.LoadCollection(ctx, &api.LoadCollectionParam{
		ReplicateParam: api.ReplicateParam{
			Database: dbName,
		},
		LoadCollectionRequest: loadCollectionMsg.LoadCollectionRequest,
	})
	if err != nil {
		log.Warn("fail to load collection", zap.Any("msg", loadCollectionMsg), zap.Error(err))
		skip, _ := c.WaitObjReady(ctx, databaseName, collectionName, "", loadCollectionMsg.EndTs())
		if !skip {
			return err
		}
		log.Info("collection has been dropped", zap.String("database", databaseName),
			zap.String("collection", collectionName), zap.String("msg", util.Base64Msg(msg)))
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
	databaseName := releaseCollectionMsg.GetDbName()
	collectionName := releaseCollectionMsg.GetCollectionName()
	dbName, colName := c.mapDBAndCollectionName(databaseName, collectionName)
	err := c.dataHandler.ReleaseCollection(ctx, &api.ReleaseCollectionParam{
		ReplicateParam: api.ReplicateParam{
			Database: dbName,
		},
		ReleaseCollectionRequest: &milvuspb.ReleaseCollectionRequest{
			Base:           msgBase,
			CollectionName: colName,
		},
	})
	if err != nil {
		log.Warn("fail to release collection", zap.Any("msg", releaseCollectionMsg), zap.Error(err))
		skip, _ := c.WaitObjReady(ctx, databaseName, collectionName, "", releaseCollectionMsg.EndTs())
		if !skip {
			return err
		}
		log.Info("collection has been dropped", zap.String("database", databaseName),
			zap.String("collection", collectionName), zap.String("msg", util.Base64Msg(msg)))
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
	databaseName := loadPartitionsMsg.GetDbName()
	collectionName := loadPartitionsMsg.GetCollectionName()
	dbName, colName := c.mapDBAndCollectionName(databaseName, collectionName)
	err := c.dataHandler.LoadPartitions(ctx, &api.LoadPartitionsParam{
		ReplicateParam: api.ReplicateParam{
			Database: dbName,
		},
		LoadPartitionsRequest: &milvuspb.LoadPartitionsRequest{
			Base:           msgBase,
			CollectionName: colName,
			PartitionNames: partitions,
			ReplicaNumber:  loadPartitionsMsg.GetReplicaNumber(),
		},
	})
	if err != nil {
		log.Warn("fail to load partitions", zap.Any("msg", loadPartitionsMsg), zap.Error(err))
		for _, p := range partitions {
			skip, _ := c.WaitObjReady(ctx, databaseName, collectionName, p, loadPartitionsMsg.EndTs())
			if !skip {
				return err
			}
		}
		log.Info("partition has been dropped", zap.String("database", databaseName),
			zap.String("collection", collectionName), zap.Strings("partitions", partitions), zap.String("msg", util.Base64Msg(msg)))
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
	databaseName := releasePartitionsMsg.GetDbName()
	collectionName := releasePartitionsMsg.GetCollectionName()
	dbName, colName := c.mapDBAndCollectionName(databaseName, collectionName)
	err := c.dataHandler.ReleasePartitions(ctx, &api.ReleasePartitionsParam{
		ReplicateParam: api.ReplicateParam{
			Database: databaseName,
		},
		ReleasePartitionsRequest: &milvuspb.ReleasePartitionsRequest{
			Base:           msgBase,
			CollectionName: colName,
			PartitionNames: partitions,
		},
	})
	if err != nil {
		log.Warn("fail to release partitions", zap.Any("msg", releasePartitionsMsg), zap.Error(err))
		for _, p := range partitions {
			skip, _ := c.WaitObjReady(ctx, dbName, collectionName, p, releasePartitionsMsg.EndTs())
			if !skip {
				return err
			}
		}
		log.Info("partition has been dropped", zap.String("database", dbName),
			zap.String("collection", collectionName), zap.Strings("partitions", partitions), zap.String("msg", util.Base64Msg(msg)))
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

func (c *ChannelWriter) operatePrivilegeV2(ctx context.Context, msgBase *commonpb.MsgBase, msg msgstream.TsMsg) error {
	operatePrivilegeMsg := msg.(*msgstream.OperatePrivilegeV2Msg)
	UpdateMsgBase(operatePrivilegeMsg.Base, msgBase)
	err := c.dataHandler.OperatePrivilegeV2(ctx, &api.OperatePrivilegeV2Param{
		OperatePrivilegeV2Request: operatePrivilegeMsg.OperatePrivilegeV2Request,
	})
	if err != nil {
		log.Warn("failed to operate privilege", zap.Any("msg", operatePrivilegeMsg), zap.Error(err))
		return err
	}
	return nil
}

func (c *ChannelWriter) createPrivilegeGroup(ctx context.Context, msgBase *commonpb.MsgBase, msg msgstream.TsMsg) error {
	createPrivilegeGroupMsg := msg.(*msgstream.CreatePrivilegeGroupMsg)
	UpdateMsgBase(createPrivilegeGroupMsg.Base, msgBase)
	err := c.dataHandler.CreatePrivilegeGroup(ctx, &api.CreatePrivilegeGroupParam{
		CreatePrivilegeGroupRequest: createPrivilegeGroupMsg.CreatePrivilegeGroupRequest,
	})
	if err != nil {
		log.Warn("failed to create privilege group", zap.Any("msg", createPrivilegeGroupMsg), zap.Error(err))
		return err
	}
	return nil
}

func (c *ChannelWriter) dropPrivilegeGroup(ctx context.Context, msgBase *commonpb.MsgBase, msg msgstream.TsMsg) error {
	dropPrivilegeGroupMsg := msg.(*msgstream.DropPrivilegeGroupMsg)
	UpdateMsgBase(dropPrivilegeGroupMsg.Base, msgBase)
	err := c.dataHandler.DropPrivilegeGroup(ctx, &api.DropPrivilegeGroupParam{
		DropPrivilegeGroupRequest: dropPrivilegeGroupMsg.DropPrivilegeGroupRequest,
	})
	if err != nil {
		log.Warn("failed to drop privilege group", zap.Any("msg", dropPrivilegeGroupMsg), zap.Error(err))
		return err
	}
	return nil
}

func (c *ChannelWriter) operatePrivilegeGroup(ctx context.Context, msgBase *commonpb.MsgBase, msg msgstream.TsMsg) error {
	operatePrivilegeGroupMsg := msg.(*msgstream.OperatePrivilegeGroupMsg)
	UpdateMsgBase(operatePrivilegeGroupMsg.Base, msgBase)
	err := c.dataHandler.OperatePrivilegeGroup(ctx, &api.OperatePrivilegeGroupParam{
		OperatePrivilegeGroupRequest: operatePrivilegeGroupMsg.OperatePrivilegeGroupRequest,
	})
	if err != nil {
		log.Warn("failed to operate privilege group", zap.Any("msg", operatePrivilegeGroupMsg), zap.Error(err))
		return err
	}
	return nil
}

func (c *ChannelWriter) mapDBAndCollectionName(db, collection string) (string, string) {
	if db == "" {
		db = util.DefaultDbName
	}
	returnDB, returnCollection := db, collection
	c.nameMappings.Range(func(source, target string) bool {
		sourceDB, sourceCollection := util.GetCollectionNameFromFull(source)
		if sourceDB == db && sourceCollection == collection {
			returnDB, returnCollection = util.GetCollectionNameFromFull(target)
			return false
		}
		if sourceDB == db && (sourceCollection == "*" || collection == "") {
			returnDB, _ = util.GetCollectionNameFromFull(target)
			return false
		}
		return true
	})
	return returnDB, returnCollection
}

func (c *ChannelWriter) UpdateNameMappings(nameMappings map[string]string) {
	for k, v := range nameMappings {
		c.nameMappings.Store(k, v)
	}
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
