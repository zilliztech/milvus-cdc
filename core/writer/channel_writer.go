package writer

import (
	"context"
	"encoding/base64"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/zilliztech/milvus-cdc/core/api"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/retry"
)

var _ api.Writer = (*ChannelWriter)(nil)

type ChannelWriter struct {
	dataHandler    api.DataHandler
	messageManager api.MessageManager
}

func NewChannelWriter(dataHandler api.DataHandler, messageBufferSize int) api.Writer {
	return &ChannelWriter{
		dataHandler:    dataHandler,
		messageManager: NewReplicateMessageManager(dataHandler, messageBufferSize),
	}
}

func (c *ChannelWriter) HandleReplicateAPIEvent(ctx context.Context, apiEvent *api.ReplicateAPIEvent) error {
	log.Info("receive replicate api event", zap.Any("event", apiEvent.EventType))
	defer func() {
		log.Info("finish to handle replicate api event", zap.Any("event", apiEvent.EventType))
	}()
	switch apiEvent.EventType {
	case api.ReplicateCreateCollection:
		collectionInfo := apiEvent.CollectionInfo
		entitySchema := &entity.Schema{}
		entitySchema = entitySchema.ReadProto(collectionInfo.Schema)
		createParam := &api.CreateCollectionParam{
			MsgBaseParam:     api.MsgBaseParam{Base: &commonpb.MsgBase{ReplicateInfo: apiEvent.ReplicateInfo}},
			Schema:           entitySchema,
			ShardsNum:        collectionInfo.ShardsNum,
			ConsistencyLevel: collectionInfo.ConsistencyLevel,
			Properties:       collectionInfo.Properties,
		}
		err := c.dataHandler.CreateCollection(ctx, createParam)
		if err != nil {
			log.Warn("fail to create collection", zap.Any("event", apiEvent), zap.Error(err))
		}
		return err
	case api.ReplicateDropCollection:
		dropParam := &api.DropCollectionParam{
			MsgBaseParam:   api.MsgBaseParam{Base: &commonpb.MsgBase{ReplicateInfo: apiEvent.ReplicateInfo}},
			CollectionName: apiEvent.CollectionInfo.Schema.GetName(),
		}
		err := c.dataHandler.DropCollection(ctx, dropParam)
		if err != nil {
			log.Warn("fail to drop collection", zap.Any("event", apiEvent), zap.Error(err))
		}
		return err
	case api.ReplicateCreatePartition:
		createParam := &api.CreatePartitionParam{
			MsgBaseParam:   api.MsgBaseParam{Base: &commonpb.MsgBase{ReplicateInfo: apiEvent.ReplicateInfo}},
			CollectionName: apiEvent.CollectionInfo.Schema.GetName(),
			PartitionName:  apiEvent.PartitionInfo.PartitionName,
		}
		err := c.dataHandler.CreatePartition(ctx, createParam)
		if err != nil {
			log.Warn("fail to create partition", zap.Any("event", apiEvent), zap.Error(err))
		}
		return err
	case api.ReplicateDropPartition:
		dropParam := &api.DropPartitionParam{
			MsgBaseParam:   api.MsgBaseParam{Base: &commonpb.MsgBase{ReplicateInfo: apiEvent.ReplicateInfo}},
			CollectionName: apiEvent.CollectionInfo.Schema.GetName(),
			PartitionName:  apiEvent.PartitionInfo.PartitionName,
		}
		err := c.dataHandler.DropPartition(ctx, dropParam)
		if err != nil {
			log.Warn("fail to drop partition", zap.Any("event", apiEvent), zap.Error(err))
		}
		return err
	default:
		log.Warn("unknown replicate api event", zap.Any("event", apiEvent))
		return errors.New("unknown replicate api event")
	}
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
	return endPosition.MsgID, targetMsgBytes, err
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
		switch msg.Type() {
		case commonpb.MsgType_CreateDatabase:
			createDatabaseMsg := msg.(*msgstream.CreateDatabaseMsg)
			err := c.dataHandler.CreateDatabase(ctx, &api.CreateDatabaseParam{
				CreateDatabaseRequest: milvuspb.CreateDatabaseRequest{
					Base:   msgBase,
					DbName: createDatabaseMsg.GetDbName(),
				},
			})
			if err != nil {
				log.Warn("failed to create database", zap.Any("msg", createDatabaseMsg), zap.Error(err))
				return nil, err
			}
		case commonpb.MsgType_DropDatabase:
			dropDatabaseMsg := msg.(*msgstream.DropDatabaseMsg)
			err := c.dataHandler.DropDatabase(ctx, &api.DropDatabaseParam{
				DropDatabaseRequest: milvuspb.DropDatabaseRequest{
					Base:   msgBase,
					DbName: dropDatabaseMsg.GetDbName(),
				},
			})
			if err != nil {
				log.Warn("failed to drop database", zap.Any("msg", dropDatabaseMsg), zap.Error(err))
				return nil, err
			}
		case commonpb.MsgType_Flush:
			flushMsg := msg.(*msgstream.FlushMsg)
			for _, s := range flushMsg.GetCollectionNames() {
				if !c.WaitCollectionReady(ctx, s) {
					log.Warn("collection is not ready", zap.Any("msg", flushMsg))
					return nil, errors.New("collection is not ready")
				}
			}
			err := c.dataHandler.Flush(ctx, &api.FlushParam{
				FlushRequest: milvuspb.FlushRequest{
					Base:            msgBase,
					CollectionNames: flushMsg.GetCollectionNames(),
				},
			})
			if err != nil {
				log.Warn("failed to flush", zap.Any("msg", flushMsg), zap.Error(err))
				return nil, err
			}
		case commonpb.MsgType_CreateIndex:
			createIndexMsg := msg.(*msgstream.CreateIndexMsg)
			if !c.WaitCollectionReady(ctx, createIndexMsg.GetCollectionName()) {
				log.Warn("collection is not ready", zap.Any("msg", createIndexMsg))
				return nil, errors.New("collection is not ready")
			}
			err := c.dataHandler.CreateIndex(ctx, &api.CreateIndexParam{
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
				return nil, err
			}
		case commonpb.MsgType_DropIndex:
			dropIndexMsg := msg.(*msgstream.DropIndexMsg)
			err := c.dataHandler.DropIndex(ctx, &api.DropIndexParam{
				DropIndexRequest: milvuspb.DropIndexRequest{
					Base:           msgBase,
					CollectionName: dropIndexMsg.GetCollectionName(),
					FieldName:      dropIndexMsg.GetFieldName(),
					IndexName:      dropIndexMsg.GetIndexName(),
				},
			})
			if err != nil {
				log.Warn("fail to drop index", zap.Any("msg", dropIndexMsg), zap.Error(err))
				return nil, err
			}
		case commonpb.MsgType_LoadCollection:
			loadCollectionMsg := msg.(*msgstream.LoadCollectionMsg)
			if !c.WaitCollectionReady(ctx, loadCollectionMsg.GetCollectionName()) {
				log.Warn("collection is not ready", zap.Any("msg", loadCollectionMsg))
				return nil, errors.New("collection is not ready")
			}
			err := c.dataHandler.LoadCollection(ctx, &api.LoadCollectionParam{
				LoadCollectionRequest: milvuspb.LoadCollectionRequest{
					Base:           msgBase,
					CollectionName: loadCollectionMsg.GetCollectionName(),
				},
			})
			if err != nil {
				log.Warn("fail to load collection", zap.Any("msg", loadCollectionMsg), zap.Error(err))
				return nil, err
			}
		case commonpb.MsgType_ReleaseCollection:
			releaseCollectionMsg := msg.(*msgstream.ReleaseCollectionMsg)
			err := c.dataHandler.ReleaseCollection(ctx, &api.ReleaseCollectionParam{
				ReleaseCollectionRequest: milvuspb.ReleaseCollectionRequest{
					Base:           msgBase,
					CollectionName: releaseCollectionMsg.GetCollectionName(),
				},
			})
			if err != nil {
				log.Warn("fail to release collection", zap.Any("msg", releaseCollectionMsg), zap.Error(err))
				return nil, err
			}
		}
		log.Info("finish to handle msg", zap.String("type", msg.Type().String()))
	}

	return endPosition.MsgID, nil
}

func (c *ChannelWriter) WaitCollectionReady(ctx context.Context, name string) bool {
	err := retry.Do(ctx, func() error {
		return c.dataHandler.DescribeCollection(ctx, &api.DescribeCollectionParam{
			Name: name,
		})
	}, retry.Attempts(5), retry.Sleep(time.Second))
	return err == nil
}
