package api

import (
	"context"

	"github.com/zilliztech/milvus-cdc/core/pb"
)

// MetaOp meta operation
type MetaOp interface {
	// WatchCollection its implementation should make sure it's only called once. The WatchPartition is same
	WatchCollection(ctx context.Context, filter CollectionFilter)
	WatchPartition(ctx context.Context, filter PartitionFilter)

	// SubscribeCollectionEvent an event only is consumed once. The SubscribePartitionEvent is same
	// TODO need to consider the many target, maybe try the method a meta op corresponds to a target
	SubscribeCollectionEvent(taskID string, consumer CollectionEventConsumer)
	SubscribePartitionEvent(taskID string, consumer PartitionEventConsumer)
	UnsubscribeEvent(taskID string, eventType WatchEventType)

	GetAllCollection(ctx context.Context, filter CollectionFilter) ([]*pb.CollectionInfo, error)
	GetCollectionNameByID(ctx context.Context, id int64) string
}

// CollectionFilter the filter will be used before the collection is filled the schema info
type CollectionFilter func(*pb.CollectionInfo) bool

type PartitionFilter func(info *pb.PartitionInfo) bool

type CollectionEventConsumer CollectionFilter

type PartitionEventConsumer PartitionFilter

type WatchEventType int

const (
	CollectionEventType WatchEventType = iota + 1
	PartitionEventType
)
