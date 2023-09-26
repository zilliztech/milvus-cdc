package writer

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-cdc/core/api"
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
	switch apiEvent.EventType {
	case api.ReplicateCreateCollection:
		collectionInfo := apiEvent.CollectionInfo
		entitySchema := &entity.Schema{}
		entitySchema = entitySchema.ReadProto(collectionInfo.Schema)
		createParam := &api.CreateCollectionParam{
			Schema:           entitySchema,
			ShardsNum:        collectionInfo.ShardsNum,
			ConsistencyLevel: collectionInfo.ConsistencyLevel,
			Properties:       collectionInfo.Properties,
		}
		err := c.dataHandler.CreateCollection(ctx, createParam)
		if err != nil {
			log.Warn("fail to create collection", zap.String("name", createParam.Schema.CollectionName), zap.Error(err))
		}
		return err
	case api.ReplicateDropCollection:
		dropParam := &api.DropCollectionParam{
			CollectionName: apiEvent.CollectionInfo.Schema.GetName(),
		}
		err := c.dataHandler.DropCollection(ctx, dropParam)
		if err != nil {
			log.Warn("fail to drop collection", zap.String("name", dropParam.CollectionName), zap.Error(err))
		}
		return err
	default:
		log.Warn("unknown replicate api event", zap.Any("event", apiEvent))
	}
	return nil
}

func (c *ChannelWriter) HandleReplicateMessage(ctx context.Context, channelName string, msgPack *msgstream.MsgPack) (*commonpb.KeyDataPair, error) {
	if len(msgPack.Msgs) == 0 {
		log.Warn("receive empty message pack", zap.String("channel", channelName))
		return nil, errors.New("receive empty message pack")
	}
	msgBytesArr := make([][]byte, 0)
	for _, msg := range msgPack.Msgs {
		msgBytes, err := msg.Marshal(msg)
		if err != nil {
			log.Warn("failed to marshal msg", zap.Error(err))
			return nil, err
		}
		if _, ok := msgBytes.([]byte); !ok {
			log.Warn("failed to convert msg bytes to []byte")
			return nil, err
		}
		msgBytesArr = append(msgBytesArr, msgBytes.([]byte))
	}
	replicateMessageParam := &api.ReplicateMessageParam{
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
	endPosition := msgPack.EndPositions[len(msgPack.EndPositions)-1]
	position := &commonpb.KeyDataPair{
		Key:  endPosition.ChannelName,
		Data: endPosition.MsgID,
	}
	return position, <-errChan
}
