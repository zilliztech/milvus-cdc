package model

import (
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
)

type SourceCollectionInfo struct {
	PChannelName string
	CollectionID int64
	SeekPosition *msgstream.MsgPosition
	ShardNum     int
}

type TargetCollectionInfo struct {
	CollectionID         int64
	CollectionName       string
	PartitionInfo        map[string]int64
	PChannel             string
	VChannel             string
	BarrierChan          chan<- uint64
	PartitionBarrierChan map[int64]chan<- uint64
}

type HandlerOpts struct {
	MessageBufferSize int
	Factory           msgstream.Factory
}

type CollectionInfo struct {
	CollectionID   int64
	CollectionName string
	VChannels      []string
	PChannels      []string
	Partitions     map[string]int64
}

type DatabaseInfo struct {
	ID   int64
	Name string
}
