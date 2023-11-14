package api

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/milvus-io/milvus/pkg/log"
)

type DataHandler interface {
	CreateCollection(ctx context.Context, param *CreateCollectionParam) error
	DropCollection(ctx context.Context, param *DropCollectionParam) error
	CreatePartition(ctx context.Context, param *CreatePartitionParam) error
	DropPartition(ctx context.Context, param *DropPartitionParam) error

	Insert(ctx context.Context, param *InsertParam) error
	Delete(ctx context.Context, param *DeleteParam) error
	Flush(ctx context.Context, param *FlushParam) error

	LoadCollection(ctx context.Context, param *LoadCollectionParam) error
	ReleaseCollection(ctx context.Context, param *ReleaseCollectionParam) error
	LoadPartitions(ctx context.Context, param *LoadPartitionsParam) error
	ReleasePartitions(ctx context.Context, param *ReleasePartitionsParam) error

	CreateIndex(ctx context.Context, param *CreateIndexParam) error
	DropIndex(ctx context.Context, param *DropIndexParam) error

	CreateDatabase(ctx context.Context, param *CreateDatabaseParam) error
	DropDatabase(ctx context.Context, param *DropDatabaseParam) error

	ReplicateMessage(ctx context.Context, param *ReplicateMessageParam) error

	DescribeCollection(ctx context.Context, param *DescribeCollectionParam) error
	DescribeDatabase(ctx context.Context, param *DescribeDatabaseParam) error
}

type DefaultDataHandler struct{}

var _ DataHandler = (*DefaultDataHandler)(nil)

func (d *DefaultDataHandler) CreateCollection(ctx context.Context, param *CreateCollectionParam) error {
	log.Warn("CreateCollection is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) DropCollection(ctx context.Context, param *DropCollectionParam) error {
	log.Warn("DropCollection is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) Insert(ctx context.Context, param *InsertParam) error {
	log.Warn("Insert is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) Delete(ctx context.Context, param *DeleteParam) error {
	log.Warn("Delete is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) CreatePartition(ctx context.Context, param *CreatePartitionParam) error {
	log.Warn("CreatePartition is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) DropPartition(ctx context.Context, param *DropPartitionParam) error {
	log.Warn("DropPartition is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) CreateIndex(ctx context.Context, param *CreateIndexParam) error {
	log.Warn("CreateIndex is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) DropIndex(ctx context.Context, param *DropIndexParam) error {
	log.Warn("DropIndex is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) LoadCollection(ctx context.Context, param *LoadCollectionParam) error {
	log.Warn("LoadCollection is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) ReleaseCollection(ctx context.Context, param *ReleaseCollectionParam) error {
	log.Warn("ReleaseCollection is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) Flush(ctx context.Context, param *FlushParam) error {
	log.Warn("Flush is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) CreateDatabase(ctx context.Context, param *CreateDatabaseParam) error {
	log.Warn("CreateDatabase is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) DropDatabase(ctx context.Context, param *DropDatabaseParam) error {
	log.Warn("DropDatabase is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) ReplicateMessage(ctx context.Context, param *ReplicateMessageParam) error {
	log.Warn("Replicate is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) LoadPartitions(ctx context.Context, param *LoadPartitionsParam) error {
	log.Warn("LoadPartitions is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) ReleasePartitions(ctx context.Context, param *ReleasePartitionsParam) error {
	log.Warn("ReleasePartitions is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) DescribeCollection(ctx context.Context, param *DescribeCollectionParam) error {
	log.Warn("DescribeCollection is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) DescribeDatabase(ctx context.Context, param *DescribeDatabaseParam) error {
	log.Warn("DescribeDatabase is not implemented, please check it")
	return nil
}

type MsgBaseParam struct {
	Base *commonpb.MsgBase
}

type ReplicateParam struct {
	Database string
}

type CreateCollectionParam struct {
	MsgBaseParam
	ReplicateParam
	Schema           *entity.Schema
	ShardsNum        int32
	ConsistencyLevel commonpb.ConsistencyLevel
	Properties       []*commonpb.KeyValuePair
}

type DropCollectionParam struct {
	MsgBaseParam
	ReplicateParam
	CollectionName string
}

type InsertParam struct {
	MsgBaseParam
	ReplicateParam
	CollectionName string
	PartitionName  string
	Columns        []entity.Column
}

type DeleteParam struct {
	MsgBaseParam
	ReplicateParam
	CollectionName string
	PartitionName  string
	Column         entity.Column
}

type CreatePartitionParam struct {
	MsgBaseParam
	ReplicateParam
	CollectionName string
	PartitionName  string
}

type DropPartitionParam struct {
	MsgBaseParam
	ReplicateParam
	CollectionName string
	PartitionName  string
}

type CreateIndexParam struct {
	ReplicateParam
	milvuspb.CreateIndexRequest
}

type DropIndexParam struct {
	ReplicateParam
	milvuspb.DropIndexRequest
}

type LoadCollectionParam struct {
	ReplicateParam
	milvuspb.LoadCollectionRequest
}

type ReleaseCollectionParam struct {
	ReplicateParam
	milvuspb.ReleaseCollectionRequest
}

type LoadPartitionsParam struct {
	ReplicateParam
	milvuspb.LoadPartitionsRequest
}

type ReleasePartitionsParam struct {
	ReplicateParam
	milvuspb.ReleasePartitionsRequest
}

type CreateDatabaseParam struct {
	ReplicateParam
	milvuspb.CreateDatabaseRequest
}

type DropDatabaseParam struct {
	ReplicateParam
	milvuspb.DropDatabaseRequest
}

type FlushParam struct {
	ReplicateParam
	milvuspb.FlushRequest
}

type ReplicateMessageParam struct {
	MsgBaseParam
	ReplicateParam
	ChannelName                  string
	BeginTs, EndTs               uint64
	MsgsBytes                    [][]byte
	StartPositions, EndPositions []*msgpb.MsgPosition

	TargetMsgPosition string
}

type DescribeCollectionParam struct {
	ReplicateParam
	Name string
}

type DescribeDatabaseParam struct {
	ReplicateParam
	Name string
}
