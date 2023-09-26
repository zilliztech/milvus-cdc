// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package writer

// import (
// 	"context"
//
// 	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
// 	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
// 	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
// 	"github.com/milvus-io/milvus-sdk-go/v2/entity"
// )
//
// type CDCDataHandler interface {
// 	CreateCollection(ctx context.Context, param *CreateCollectionParam) error
// 	DropCollection(ctx context.Context, param *DropCollectionParam) error
// 	Insert(ctx context.Context, param *InsertParam) error
// 	Delete(ctx context.Context, param *DeleteParam) error
// 	CreatePartition(ctx context.Context, param *CreatePartitionParam) error
// 	DropPartition(ctx context.Context, param *DropPartitionParam) error
//
// 	CreateIndex(ctx context.Context, param *CreateIndexParam) error
// 	DropIndex(ctx context.Context, param *DropIndexParam) error
// 	LoadCollection(ctx context.Context, param *LoadCollectionParam) error
// 	ReleaseCollection(ctx context.Context, param *ReleaseCollectionParam) error
// 	CreateDatabase(ctx context.Context, param *CreateDataBaseParam) error
// 	DropDatabase(ctx context.Context, param *DropDataBaseParam) error
//
// 	ReplicateMessage(ctx context.Context, param *ReplicateMessageParam) error
// 	// NOTE: please add the implements for the DataHandlerWrapper class when adding new interfaces
// }
//
// type DefaultDataHandler struct{}
//
// func (d *DefaultDataHandler) CreateCollection(ctx context.Context, param *CreateCollectionParam) error {
// 	log.Warn("CreateCollection is not implemented, please check it")
// 	return nil
// }
//
// func (d *DefaultDataHandler) DropCollection(ctx context.Context, param *DropCollectionParam) error {
// 	log.Warn("DropCollection is not implemented, please check it")
// 	return nil
// }
//
// func (d *DefaultDataHandler) Insert(ctx context.Context, param *InsertParam) error {
// 	log.Warn("Insert is not implemented, please check it")
// 	return nil
// }
//
// func (d *DefaultDataHandler) Delete(ctx context.Context, param *DeleteParam) error {
// 	log.Warn("Delete is not implemented, please check it")
// 	return nil
// }
//
// func (d *DefaultDataHandler) CreatePartition(ctx context.Context, param *CreatePartitionParam) error {
// 	log.Warn("CreatePartition is not implemented, please check it")
// 	return nil
// }
//
// func (d *DefaultDataHandler) DropPartition(ctx context.Context, param *DropPartitionParam) error {
// 	log.Warn("DropPartition is not implemented, please check it")
// 	return nil
// }
//
// func (d *DefaultDataHandler) CreateIndex(ctx context.Context, param *CreateIndexParam) error {
// 	log.Warn("CreateIndex is not implemented, please check it")
// 	return nil
// }
//
// func (d *DefaultDataHandler) DropIndex(ctx context.Context, param *DropIndexParam) error {
// 	log.Warn("DropIndex is not implemented, please check it")
// 	return nil
// }
//
// func (d *DefaultDataHandler) LoadCollection(ctx context.Context, param *LoadCollectionParam) error {
// 	log.Warn("LoadCollection is not implemented, please check it")
// 	return nil
// }
//
// func (d *DefaultDataHandler) ReleaseCollection(ctx context.Context, param *ReleaseCollectionParam) error {
// 	log.Warn("ReleaseCollection is not implemented, please check it")
// 	return nil
// }
//
// func (d *DefaultDataHandler) CreateDatabase(ctx context.Context, param *CreateDataBaseParam) error {
// 	log.Warn("CreateDatabase is not implemented, please check it")
// 	return nil
// }
//
// func (d *DefaultDataHandler) DropDatabase(ctx context.Context, param *DropDataBaseParam) error {
// 	log.Warn("DropDatabase is not implemented, please check it")
// 	return nil
// }
//
// func (d *DefaultDataHandler) ReplicateMessage(ctx context.Context, param *ReplicateMessageParam) error {
// 	log.Warn("Replicate is not implemented, please check it")
// 	return nil
// }
//
// type CreateCollectionParam struct {
// 	Schema           *entity.Schema
// 	ShardsNum        int32
// 	ConsistencyLevel commonpb.ConsistencyLevel
// 	Properties       []*commonpb.KeyValuePair
// }
//
// type DropCollectionParam struct {
// 	CollectionName string
// }
//
// type InsertParam struct {
// 	CollectionName string
// 	PartitionName  string
// 	Columns        []entity.Column
// }
//
// type DeleteParam struct {
// 	CollectionName string
// 	PartitionName  string
// 	Column         entity.Column
// }
//
// type CreatePartitionParam struct {
// 	CollectionName string
// 	PartitionName  string
// }
//
// type DropPartitionParam struct {
// 	CollectionName string
// 	PartitionName  string
// }
//
// type CreateIndexParam struct {
// 	milvuspb.CreateIndexRequest
// }
//
// type DropIndexParam struct {
// 	milvuspb.DropIndexRequest
// }
//
// type LoadCollectionParam struct {
// 	milvuspb.LoadCollectionRequest
// }
//
// type ReleaseCollectionParam struct {
// 	milvuspb.ReleaseCollectionRequest
// }
//
// type CreateDataBaseParam struct {
// 	milvuspb.CreateDatabaseRequest
// }
//
// type DropDataBaseParam struct {
// 	milvuspb.DropDatabaseRequest
// }
//
// type ReplicateMessageParam struct {
// 	ChannelName                  string
// 	BeginTs, EndTs               uint64
// 	MsgsBytes                    [][]byte
// 	StartPositions, EndPositions []*msgpb.MsgPosition
// }
