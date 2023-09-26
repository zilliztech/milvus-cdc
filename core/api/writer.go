package api

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
)

type Writer interface {
	HandleReplicateAPIEvent(ctx context.Context, apiEvent *ReplicateAPIEvent) error
	HandleReplicateMessage(ctx context.Context, channelName string, msgPack *msgstream.MsgPack) (*commonpb.KeyDataPair, error)
}
