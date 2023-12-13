package api

import (
	"context"

	"github.com/milvus-io/milvus/pkg/mq/msgstream"

	"github.com/zilliztech/milvus-cdc/core/log"
)

type Writer interface {
	HandleReplicateAPIEvent(ctx context.Context, apiEvent *ReplicateAPIEvent) error
	HandleReplicateMessage(ctx context.Context, channelName string, msgPack *msgstream.MsgPack) ([]byte, []byte, error)
	HandleOpMessagePack(ctx context.Context, msgPack *msgstream.MsgPack) ([]byte, error)
}

type DefaultWriter struct{}

var _ Writer = (*DefaultWriter)(nil)

func (d *DefaultWriter) HandleReplicateAPIEvent(ctx context.Context, apiEvent *ReplicateAPIEvent) error {
	log.Warn("HandleReplicateAPIEvent is not implemented, please check it")
	return nil
}

func (d *DefaultWriter) HandleReplicateMessage(ctx context.Context, channelName string, msgPack *msgstream.MsgPack) ([]byte, []byte, error) {
	log.Warn("HandleReplicateMessage is not implemented, please check it")
	return nil, nil, nil
}

func (d *DefaultWriter) HandleOpMessagePack(ctx context.Context, msgPack *msgstream.MsgPack) ([]byte, error) {
	log.Warn("HandleOpMessagePack is not implemented, please check it")
	return nil, nil
}
