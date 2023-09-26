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

//
// import (
// 	"context"
//
// 	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
// 	"github.com/milvus-io/milvus-sdk-go/v2/client"
// 	"github.com/milvus-io/milvus-sdk-go/v2/entity"
//
// 	"github.com/zilliztech/milvus-cdc/core/util"
// )
//
// type MilvusClientAPI interface {
// 	CreateCollection(ctx context.Context, schema *entity.Schema, shardsNum int32, opts ...client.CreateCollectionOption) error
// 	DropCollection(ctx context.Context, collName string) error
// 	Insert(ctx context.Context, collName string, partitionName string, columns ...entity.Column) (entity.Column, error)
// 	DeleteByPks(ctx context.Context, collName string, partitionName string, ids entity.Column) error
// 	CreatePartition(ctx context.Context, collName string, partitionName string) error
// 	DropPartition(ctx context.Context, collName string, partitionName string) error
//
// 	CreateIndex(ctx context.Context, collName string, fieldName string, idx entity.Index, async bool, opts ...client.IndexOption) error
// 	DropIndex(ctx context.Context, collName string, fieldName string, opts ...client.IndexOption) error
// 	LoadCollection(ctx context.Context, collName string, async bool, opts ...client.LoadCollectionOption) error
// 	ReleaseCollection(ctx context.Context, collName string) error
// 	CreateDatabase(ctx context.Context, dbName string) error
// 	DropDatabase(ctx context.Context, dbName string) error
// 	ReplicateMessage(ctx context.Context,
// 		channelName string, beginTs, endTs uint64,
// 		msgsBytes [][]byte, startPositions, endPositions []*msgpb.MsgPosition) error
// }
//
// type MilvusClientFactory interface {
// 	util.CDCMark
// 	NewGrpcClientWithTLSAuth(ctx context.Context, addr, username, password string) (MilvusClientAPI, error)
// 	NewGrpcClientWithAuth(ctx context.Context, addr, username, password string) (MilvusClientAPI, error)
// 	NewGrpcClient(ctx context.Context, addr string) (MilvusClientAPI, error)
// }
//
// type DefaultMilvusClientFactory struct {
// 	util.CDCMark
// }
//
// func NewDefaultMilvusClientFactory() MilvusClientFactory {
// 	return &DefaultMilvusClientFactory{}
// }
//
// func (d *DefaultMilvusClientFactory) NewGrpcClientWithTLSAuth(ctx context.Context, addr, username, password string) (MilvusClientAPI, error) {
// 	return client.NewDefaultGrpcClientWithTLSAuth(ctx, addr, username, password)
// }
//
// func (d *DefaultMilvusClientFactory) NewGrpcClientWithAuth(ctx context.Context, addr, username, password string) (MilvusClientAPI, error) {
// 	return client.NewDefaultGrpcClientWithAuth(ctx, addr, username, password)
// }
//
// func (d *DefaultMilvusClientFactory) NewGrpcClient(ctx context.Context, addr string) (MilvusClientAPI, error) {
// 	return client.NewDefaultGrpcClient(ctx, addr)
// }
