package api

import (
	"context"

	"github.com/zilliztech/milvus-cdc/core/model"
	"github.com/zilliztech/milvus-cdc/core/pb"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
)

// ChannelManager a target must promise a manager
type ChannelManager interface {
	StartReadCollection(ctx context.Context, info *pb.CollectionInfo, seekPositions []*msgpb.MsgPosition) error
	StopReadCollection(ctx context.Context, info *pb.CollectionInfo) error
	AddPartition(ctx context.Context, collectionInfo *pb.CollectionInfo, partitionInfo *pb.PartitionInfo) error

	GetChannelChan() <-chan string
	GetMsgChan(pChannel string) <-chan *msgstream.MsgPack
	GetEventChan() <-chan *ReplicateAPIEvent
}

type TargetAPI interface {
	GetCollectionInfo(ctx context.Context, collectionName string) (*model.CollectionInfo, error)
	GetPartitionInfo(ctx context.Context, collectionName string) (*model.CollectionInfo, error)
}

type ReplicateAPIEvent struct {
	EventType      ReplicateAPIEventType
	CollectionInfo *pb.CollectionInfo
	PartitionInfo  *pb.PartitionInfo
	ReplicateInfo  *commonpb.ReplicateInfo
}

type ReplicateAPIEventType int

const (
	ReplicateCreateCollection = iota + 1
	ReplicateDropCollection
	ReplicateCreatePartition
	ReplicateDropPartition
)

type DefaultChannelManager struct{}

var _ ChannelManager = (*DefaultChannelManager)(nil)

func (d *DefaultChannelManager) StartReadCollection(ctx context.Context, info *pb.CollectionInfo, seekPositions []*msgpb.MsgPosition) error {
	log.Warn("StartReadCollection is not implemented, please check it")
	return nil
}

func (d *DefaultChannelManager) StopReadCollection(ctx context.Context, info *pb.CollectionInfo) error {
	log.Warn("StopReadCollection is not implemented, please check it")
	return nil
}

func (d *DefaultChannelManager) AddPartition(ctx context.Context, collectionInfo *pb.CollectionInfo, partitionInfo *pb.PartitionInfo) error {
	log.Warn("AddPartition is not implemented, please check it")
	return nil
}

func (d *DefaultChannelManager) GetChannelChan() <-chan string {
	log.Warn("GetChannelChan is not implemented, please check it")
	return nil
}

func (d *DefaultChannelManager) GetMsgChan(pChannel string) <-chan *msgstream.MsgPack {
	log.Warn("GetMsgChan is not implemented, please check it")
	return nil
}

func (d *DefaultChannelManager) GetEventChan() <-chan *ReplicateAPIEvent {
	log.Warn("GetEventChan is not implemented, please check it")
	return nil
}

type DefaultTargetAPI struct{}

var _ TargetAPI = (*DefaultTargetAPI)(nil)

func (d *DefaultTargetAPI) GetCollectionInfo(ctx context.Context, collectionName string) (*model.CollectionInfo, error) {
	log.Warn("GetCollectionInfo is not implemented, please check it")
	return nil, nil
}

func (d *DefaultTargetAPI) GetPartitionInfo(ctx context.Context, collectionName string) (*model.CollectionInfo, error) {
	log.Warn("GetPartitionInfo is not implemented, please check it")
	return nil, nil
}
