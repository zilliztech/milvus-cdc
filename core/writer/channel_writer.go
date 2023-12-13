package writer

import (
	"context"
	"encoding/base64"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-cdc/core/api"
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
}

func NewChannelWriter(dataHandler api.DataHandler, messageBufferSize int) api.Writer {
	w := &ChannelWriter{
		dataHandler:    dataHandler,
		messageManager: NewReplicateMessageManager(dataHandler, messageBufferSize),
	}
	w.initAPIEventFuncs()
	w.initOPMessageFuncs()

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
		commonpb.MsgType_Flush:             c.flush,
		commonpb.MsgType_CreateIndex:       c.createIndex,
		commonpb.MsgType_DropIndex:         c.dropIndex,
		commonpb.MsgType_LoadCollection:    c.loadCollection,
		commonpb.MsgType_ReleaseCollection: c.releaseCollection,
		commonpb.MsgType_LoadPartitions:    c.loadPartitions,
		commonpb.MsgType_ReleasePartitions: c.releasePartitions,
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
			log.Info("replicate msg", zap.String("type", msg.Type().String()))
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
		log.Info("receive msg", zap.String("type", msg.Type().String()))
		f, ok := c.opMessageFuncs[msg.Type()]
		if !ok {
			log.Warn("unknown msg type", zap.Any("msg", msg))
			return nil, errors.New("unknown msg type")
		}
		err := f(ctx, msgBase, msg)
		if err != nil {
			return nil, err
		}
		log.Info("finish to handle msg", zap.String("type", msg.Type().String()))
	}

	return endPosition.MsgID, nil
}

func (c *ChannelWriter) WaitCollectionReady(ctx context.Context, collectionName, databaseName string) bool {
	err := retry.Do(ctx, func() error {
		return c.dataHandler.DescribeCollection(ctx, &api.DescribeCollectionParam{
			ReplicateParam: api.ReplicateParam{
				Database: databaseName,
			},
			Name: collectionName,
		})
	}, util.GetRetryOptionsFor25s()...)
	return err == nil
}

func (c *ChannelWriter) WaitDatabaseReady(ctx context.Context, databaseName string) bool {
	if databaseName == "" {
		return true
	}
	err := retry.Do(ctx, func() error {
		return c.dataHandler.DescribeDatabase(ctx, &api.DescribeDatabaseParam{
			Name: databaseName,
		})
	}, util.GetRetryOptionsFor25s()...)
	return err == nil
}

func (c *ChannelWriter) createCollection(ctx context.Context, apiEvent *api.ReplicateAPIEvent) error {
	if ready := c.WaitDatabaseReady(ctx, apiEvent.ReplicateParam.Database); !ready {
		log.Warn("database is not ready", zap.String("database", apiEvent.ReplicateParam.Database))
		return errors.New("database is not ready")
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
	if ready := c.WaitDatabaseReady(ctx, apiEvent.ReplicateParam.Database); !ready {
		log.Warn("database is not ready", zap.String("database", apiEvent.ReplicateParam.Database))
		return errors.New("database is not ready")
	}
	dropParam := &api.DropCollectionParam{
		MsgBaseParam:   api.MsgBaseParam{Base: &commonpb.MsgBase{ReplicateInfo: apiEvent.ReplicateInfo}},
		ReplicateParam: apiEvent.ReplicateParam,
		CollectionName: apiEvent.CollectionInfo.Schema.GetName(),
	}
	err := c.dataHandler.DropCollection(ctx, dropParam)
	if err != nil {
		log.Warn("fail to drop collection", zap.Any("event", apiEvent), zap.Error(err))
		return err
	}
	return nil
}

func (c *ChannelWriter) createPartition(ctx context.Context, apiEvent *api.ReplicateAPIEvent) error {
	if ready := c.WaitDatabaseReady(ctx, apiEvent.ReplicateParam.Database); !ready {
		log.Warn("database is not ready", zap.String("database", apiEvent.ReplicateParam.Database))
		return errors.New("database is not ready")
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
		return err
	}
	return nil
}

func (c *ChannelWriter) dropPartition(ctx context.Context, apiEvent *api.ReplicateAPIEvent) error {
	if ready := c.WaitDatabaseReady(ctx, apiEvent.ReplicateParam.Database); !ready {
		log.Warn("database is not ready", zap.String("database", apiEvent.ReplicateParam.Database))
		return errors.New("database is not ready")
	}
	dropParam := &api.DropPartitionParam{
		MsgBaseParam:   api.MsgBaseParam{Base: &commonpb.MsgBase{ReplicateInfo: apiEvent.ReplicateInfo}},
		ReplicateParam: apiEvent.ReplicateParam,
		CollectionName: apiEvent.CollectionInfo.Schema.GetName(),
		PartitionName:  apiEvent.PartitionInfo.PartitionName,
	}
	err := c.dataHandler.DropPartition(ctx, dropParam)
	if err != nil {
		log.Warn("fail to drop partition", zap.Any("event", apiEvent), zap.Error(err))
		return err
	}
	return nil
}

func (c *ChannelWriter) createDatabase(ctx context.Context, msgBase *commonpb.MsgBase, msg msgstream.TsMsg) error {
	createDatabaseMsg := msg.(*msgstream.CreateDatabaseMsg)
	err := c.dataHandler.CreateDatabase(ctx, &api.CreateDatabaseParam{
		CreateDatabaseRequest: milvuspb.CreateDatabaseRequest{
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
	err := c.dataHandler.DropDatabase(ctx, &api.DropDatabaseParam{
		DropDatabaseRequest: milvuspb.DropDatabaseRequest{
			Base:   msgBase,
			DbName: dropDatabaseMsg.GetDbName(),
		},
	})
	if err != nil {
		log.Warn("failed to drop database", zap.Any("msg", dropDatabaseMsg), zap.Error(err))
		return err
	}
	return nil
}

func (c *ChannelWriter) flush(ctx context.Context, msgBase *commonpb.MsgBase, msg msgstream.TsMsg) error {
	flushMsg := msg.(*msgstream.FlushMsg)
	if !c.WaitDatabaseReady(ctx, flushMsg.GetDbName()) {
		log.Warn("database is not ready", zap.Any("msg", flushMsg))
		return errors.New("database is not ready")
	}
	for _, s := range flushMsg.GetCollectionNames() {
		if !c.WaitCollectionReady(ctx, s, flushMsg.GetDbName()) {
			log.Warn("collection is not ready", zap.Any("msg", flushMsg))
			return errors.New("collection is not ready")
		}
	}
	err := c.dataHandler.Flush(ctx, &api.FlushParam{
		ReplicateParam: api.ReplicateParam{
			Database: flushMsg.GetDbName(),
		},
		FlushRequest: milvuspb.FlushRequest{
			Base:            msgBase,
			CollectionNames: flushMsg.GetCollectionNames(),
		},
	})
	if err != nil {
		log.Warn("failed to flush", zap.Any("msg", flushMsg), zap.Error(err))
		return err
	}
	return nil
}

func (c *ChannelWriter) createIndex(ctx context.Context, msgBase *commonpb.MsgBase, msg msgstream.TsMsg) error {
	createIndexMsg := msg.(*msgstream.CreateIndexMsg)
	if !c.WaitDatabaseReady(ctx, createIndexMsg.GetDbName()) {
		log.Warn("database is not ready", zap.Any("msg", createIndexMsg))
		return errors.New("database is not ready")
	}
	if !c.WaitCollectionReady(ctx, createIndexMsg.GetCollectionName(), createIndexMsg.GetDbName()) {
		log.Warn("collection is not ready", zap.Any("msg", createIndexMsg))
		return errors.New("collection is not ready")
	}
	err := c.dataHandler.CreateIndex(ctx, &api.CreateIndexParam{
		ReplicateParam: api.ReplicateParam{
			Database: createIndexMsg.GetDbName(),
		},
		CreateIndexRequest: milvuspb.CreateIndexRequest{
			Base:           msgBase,
			CollectionName: createIndexMsg.GetCollectionName(),
			FieldName:      createIndexMsg.GetFieldName(),
			IndexName:      createIndexMsg.GetIndexName(),
			ExtraParams:    createIndexMsg.GetExtraParams(),
		},
	})
	if err != nil {
		log.Warn("fail to create index", zap.Any("msg", createIndexMsg), zap.Error(err))
		return err
	}
	return nil
}

func (c *ChannelWriter) dropIndex(ctx context.Context, msgBase *commonpb.MsgBase, msg msgstream.TsMsg) error {
	dropIndexMsg := msg.(*msgstream.DropIndexMsg)
	err := c.dataHandler.DropIndex(ctx, &api.DropIndexParam{
		ReplicateParam: api.ReplicateParam{
			Database: dropIndexMsg.GetDbName(),
		},
		DropIndexRequest: milvuspb.DropIndexRequest{
			Base:           msgBase,
			CollectionName: dropIndexMsg.GetCollectionName(),
			FieldName:      dropIndexMsg.GetFieldName(),
			IndexName:      dropIndexMsg.GetIndexName(),
		},
	})
	if err != nil {
		log.Warn("fail to drop index", zap.Any("msg", dropIndexMsg), zap.Error(err))
		return err
	}
	return nil
}

func (c *ChannelWriter) loadCollection(ctx context.Context, msgBase *commonpb.MsgBase, msg msgstream.TsMsg) error {
	loadCollectionMsg := msg.(*msgstream.LoadCollectionMsg)
	if !c.WaitDatabaseReady(ctx, loadCollectionMsg.GetDbName()) {
		log.Warn("database is not ready", zap.Any("msg", loadCollectionMsg))
		return errors.New("database is not ready")
	}
	if !c.WaitCollectionReady(ctx, loadCollectionMsg.GetCollectionName(), loadCollectionMsg.GetDbName()) {
		log.Warn("collection is not ready", zap.Any("msg", loadCollectionMsg))
		return errors.New("collection is not ready")
	}
	err := c.dataHandler.LoadCollection(ctx, &api.LoadCollectionParam{
		ReplicateParam: api.ReplicateParam{
			Database: loadCollectionMsg.GetDbName(),
		},
		LoadCollectionRequest: milvuspb.LoadCollectionRequest{
			Base:           msgBase,
			CollectionName: loadCollectionMsg.GetCollectionName(),
		},
	})
	if err != nil {
		log.Warn("fail to load collection", zap.Any("msg", loadCollectionMsg), zap.Error(err))
		return err
	}
	return nil
}

func (c *ChannelWriter) releaseCollection(ctx context.Context, msgBase *commonpb.MsgBase, msg msgstream.TsMsg) error {
	releaseCollectionMsg := msg.(*msgstream.ReleaseCollectionMsg)
	err := c.dataHandler.ReleaseCollection(ctx, &api.ReleaseCollectionParam{
		ReplicateParam: api.ReplicateParam{
			Database: releaseCollectionMsg.GetDbName(),
		},
		ReleaseCollectionRequest: milvuspb.ReleaseCollectionRequest{
			Base:           msgBase,
			CollectionName: releaseCollectionMsg.GetCollectionName(),
		},
	})
	if err != nil {
		log.Warn("fail to release collection", zap.Any("msg", releaseCollectionMsg), zap.Error(err))
		return err
	}
	return nil
}

func (c *ChannelWriter) loadPartitions(ctx context.Context, msgBase *commonpb.MsgBase, msg msgstream.TsMsg) error {
	loadPartitionsMsg := msg.(*msgstream.LoadPartitionsMsg)
	if !c.WaitDatabaseReady(ctx, loadPartitionsMsg.GetDbName()) {
		log.Warn("database is not ready", zap.Any("msg", loadPartitionsMsg))
		return errors.New("database is not ready")
	}
	if !c.WaitCollectionReady(ctx, loadPartitionsMsg.GetCollectionName(), loadPartitionsMsg.GetDbName()) {
		log.Warn("collection is not ready", zap.Any("msg", loadPartitionsMsg))
		return errors.New("collection is not ready")
	}
	err := c.dataHandler.LoadPartitions(ctx, &api.LoadPartitionsParam{
		ReplicateParam: api.ReplicateParam{
			Database: loadPartitionsMsg.GetDbName(),
		},
		LoadPartitionsRequest: milvuspb.LoadPartitionsRequest{
			Base:           msgBase,
			CollectionName: loadPartitionsMsg.GetCollectionName(),
			PartitionNames: loadPartitionsMsg.GetPartitionNames(),
		},
	})
	if err != nil {
		log.Warn("fail to load partitions", zap.Any("msg", loadPartitionsMsg), zap.Error(err))
		return err
	}
	return nil
}

func (c *ChannelWriter) releasePartitions(ctx context.Context, msgBase *commonpb.MsgBase, msg msgstream.TsMsg) error {
	releasePartitionsMsg := msg.(*msgstream.ReleasePartitionsMsg)
	err := c.dataHandler.ReleasePartitions(ctx, &api.ReleasePartitionsParam{
		ReplicateParam: api.ReplicateParam{
			Database: releasePartitionsMsg.GetDbName(),
		},
		ReleasePartitionsRequest: milvuspb.ReleasePartitionsRequest{
			Base:           msgBase,
			CollectionName: releasePartitionsMsg.GetCollectionName(),
			PartitionNames: releasePartitionsMsg.GetPartitionNames(),
		},
	})
	if err != nil {
		log.Warn("fail to release partitions", zap.Any("msg", releasePartitionsMsg), zap.Error(err))
		return err
	}
	return nil
}
