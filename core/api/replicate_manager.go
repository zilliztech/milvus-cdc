package api

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"

	"github.com/zilliztech/milvus-cdc/core/model"
	"github.com/zilliztech/milvus-cdc/core/pb"
)

// ChannelManager a target must promise a manager
type ChannelManager interface {
	StartReadCollection(ctx context.Context, info *pb.CollectionInfo, seekPositions []*msgpb.MsgPosition) error

	StopReadCollection(ctx context.Context, info *pb.CollectionInfo) error

	GetChannelChan() <-chan string
	GetMsgChan(pChannel string) <-chan *msgstream.MsgPack
	GetEventChan() <-chan *ReplicateAPIEvent
}

type TargetAPI interface {
	GetCollectionInfo(ctx context.Context, collectionName string) (*model.CollectionInfo, error)
}

type ReplicateAPIEvent struct {
	EventType      ReplicateAPIEventType
	CollectionInfo *pb.CollectionInfo
	PartitionInfo  *pb.PartitionInfo
}

type ReplicateAPIEventType int

const (
	ReplicateCreateCollection = iota + 1
	ReplicateDropCollection
	ReplicateCreatePartition
	ReplicateDropPartition
)
