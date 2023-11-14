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

import (
	"context"
	"log"
	"net"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/milvus-io/milvus-sdk-go/v2/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"

	"github.com/zilliztech/milvus-cdc/core/api"
)

func TestDataHandler(t *testing.T) {
	{
		_, err := NewMilvusDataHandler()
		assert.Error(t, err)
	}

	listen, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	server := grpc.NewServer()
	milvusService := mocks.NewMilvusServiceServer(t)
	milvusService.EXPECT().Connect(mock.Anything, mock.Anything).Return(&milvuspb.ConnectResponse{
		Status: &commonpb.Status{},
	}, nil)
	milvuspb.RegisterMilvusServiceServer(server, milvusService)

	go func() {
		log.Println("Server started on port 50051")
		if err := server.Serve(listen); err != nil {
			log.Println("server error", err)
		}
	}()
	time.Sleep(time.Second)
	defer listen.Close()

	dataHandler, err := NewMilvusDataHandler(AddressOption("localhost:50051"))
	assert.NoError(t, err)
	dataHandler.ignorePartition = true
	ctx := context.Background()

	// create collection
	t.Run("create collection", func(t *testing.T) {
		milvusService.EXPECT().CreateCollection(mock.Anything, mock.Anything).Return(&commonpb.Status{}, nil).Once()
		err := dataHandler.CreateCollection(ctx, &api.CreateCollectionParam{
			Properties: []*commonpb.KeyValuePair{
				{
					Key:   "foo",
					Value: "hoo",
				},
			},
			ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
			MsgBaseParam: api.MsgBaseParam{
				Base: &commonpb.MsgBase{
					ReplicateInfo: &commonpb.ReplicateInfo{
						IsReplicate:  true,
						MsgTimestamp: 1000,
					},
				},
			},
			Schema: &entity.Schema{
				CollectionName: "foo",
				Fields: []*entity.Field{
					{
						Name:     "age",
						DataType: entity.FieldTypeInt8,
					},
					{
						Name:     "data",
						DataType: entity.FieldTypeBinaryVector,
					},
				},
			},
			ShardsNum: 1,
		})
		assert.NoError(t, err)
	})

	t.Run("drop collection", func(t *testing.T) {
		{
			milvusService.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(&milvuspb.BoolResponse{
				Status: &commonpb.Status{},
				Value:  true,
			}, nil).Once()
			milvusService.EXPECT().DropCollection(mock.Anything, mock.Anything).Return(&commonpb.Status{}, nil).Once()
			err := dataHandler.DropCollection(ctx, &api.DropCollectionParam{
				CollectionName: "foo",
			})
			assert.NoError(t, err)
		}

		{
			milvusService.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(&milvuspb.BoolResponse{
				Status: &commonpb.Status{},
				Value:  true,
			}, nil).Once()
			milvusService.EXPECT().DropCollection(mock.Anything, mock.Anything).Return(&commonpb.Status{}, nil).Once()
			err := dataHandler.DropCollection(ctx, &api.DropCollectionParam{
				ReplicateParam: api.ReplicateParam{
					Database: "foo",
				},
				CollectionName: "foo",
			})
			assert.NoError(t, err)
		}

		{
			dataHandler.address = ""
			err := dataHandler.DropCollection(ctx, &api.DropCollectionParam{
				ReplicateParam: api.ReplicateParam{
					Database: "foo",
				},
				CollectionName: "foo",
			})
			assert.Error(t, err)
			dataHandler.address = "localhost:50051"
		}
	})

	t.Run("insert", func(t *testing.T) {
		milvusService.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(&milvuspb.BoolResponse{
			Status: &commonpb.Status{},
			Value:  true,
		}, nil).Once()
		milvusService.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{},
			Schema: &schemapb.CollectionSchema{
				Name: "foo",
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:      100,
						Name:         "age",
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
				},
			},
		}, nil).Once()
		milvusService.EXPECT().Insert(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{
			Status: &commonpb.Status{},
			IDs: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{100}}},
			},
		}, nil).Once()
		err := dataHandler.Insert(ctx, &api.InsertParam{
			CollectionName: "foo",
			Columns: []entity.Column{
				entity.NewColumnInt64("age", []int64{10}),
			},
		})
		assert.NoError(t, err)
	})

	t.Run("delete", func(t *testing.T) {
		milvusService.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(&milvuspb.BoolResponse{
			Status: &commonpb.Status{},
			Value:  true,
		}, nil).Once()
		milvusService.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{},
			Schema: &schemapb.CollectionSchema{
				Name: "foo",
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:      100,
						Name:         "age",
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
				},
			},
		}, nil).Once()
		milvusService.EXPECT().Delete(mock.Anything, mock.Anything).Return(&milvuspb.MutationResult{Status: &commonpb.Status{}}, nil).Once()
		err := dataHandler.Delete(ctx, &api.DeleteParam{
			CollectionName: "foo",
			Column:         entity.NewColumnInt64("age", []int64{10}),
		})
		assert.NoError(t, err)
	})

	t.Run("create partition", func(t *testing.T) {
		dataHandler.ignorePartition = true
		milvusService.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(&milvuspb.BoolResponse{
			Status: &commonpb.Status{},
			Value:  true,
		}, nil).Once()
		milvusService.EXPECT().HasPartition(mock.Anything, mock.Anything).Return(&milvuspb.BoolResponse{
			Status: &commonpb.Status{},
			Value:  false,
		}, nil).Once()
		milvusService.EXPECT().CreatePartition(mock.Anything, mock.Anything).Return(&commonpb.Status{}, nil).Once()
		{
			err := dataHandler.CreatePartition(ctx, &api.CreatePartitionParam{
				CollectionName: "foo",
				PartitionName:  "bar",
			})
			assert.NoError(t, err)
		}
		dataHandler.ignorePartition = false
		{
			err := dataHandler.CreatePartition(ctx, &api.CreatePartitionParam{
				CollectionName: "foo",
				PartitionName:  "bar",
			})
			assert.NoError(t, err)
		}
		dataHandler.ignorePartition = true
	})

	t.Run("drop partition", func(t *testing.T) {
		dataHandler.ignorePartition = true
		milvusService.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(&milvuspb.BoolResponse{
			Status: &commonpb.Status{},
			Value:  true,
		}, nil).Once()
		milvusService.EXPECT().HasPartition(mock.Anything, mock.Anything).Return(&milvuspb.BoolResponse{
			Status: &commonpb.Status{},
			Value:  true,
		}, nil).Once()
		milvusService.EXPECT().DropPartition(mock.Anything, mock.Anything).Return(&commonpb.Status{}, nil).Once()
		{
			err := dataHandler.DropPartition(ctx, &api.DropPartitionParam{
				CollectionName: "foo",
				PartitionName:  "bar",
			})
			assert.NoError(t, err)
		}
		dataHandler.ignorePartition = false
		{
			err := dataHandler.DropPartition(ctx, &api.DropPartitionParam{
				CollectionName: "foo",
				PartitionName:  "bar",
			})
			assert.NoError(t, err)
		}
		dataHandler.ignorePartition = true
	})

	t.Run("create index", func(t *testing.T) {
		milvusService.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(&milvuspb.BoolResponse{
			Status: &commonpb.Status{},
			Value:  true,
		}, nil).Twice()
		milvusService.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{},
			Schema: &schemapb.CollectionSchema{
				Name: "foo",
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:      100,
						Name:         "age",
						IsPrimaryKey: true,
						DataType:     schemapb.DataType_Int64,
					},
					{
						FieldID:      101,
						Name:         "name",
						IsPrimaryKey: false,
						DataType:     schemapb.DataType_FloatVector,
					},
				},
			},
		}, nil).Once()
		milvusService.EXPECT().Flush(mock.Anything, mock.Anything).Return(&milvuspb.FlushResponse{Status: &commonpb.Status{}}, nil).Once()
		milvusService.EXPECT().CreateIndex(mock.Anything, mock.Anything).Return(&commonpb.Status{}, nil).Once()
		err := dataHandler.CreateIndex(ctx, &api.CreateIndexParam{
			CreateIndexRequest: milvuspb.CreateIndexRequest{
				CollectionName: "foo",
				FieldName:      "name",
				IndexName:      "baz",
			},
		})
		assert.NoError(t, err)
	})

	t.Run("drop index", func(t *testing.T) {
		milvusService.EXPECT().DropIndex(mock.Anything, mock.Anything).Return(&commonpb.Status{}, nil).Once()
		err := dataHandler.DropIndex(ctx, &api.DropIndexParam{
			DropIndexRequest: milvuspb.DropIndexRequest{
				CollectionName: "foo",
				FieldName:      "bar",
				IndexName:      "baz",
			},
		})
		assert.NoError(t, err)
	})

	t.Run("load collection", func(t *testing.T) {
		milvusService.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(&milvuspb.BoolResponse{
			Status: &commonpb.Status{},
			Value:  true,
		}, nil).Once()
		milvusService.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(&commonpb.Status{}, nil).Once()
		err := dataHandler.LoadCollection(ctx, &api.LoadCollectionParam{
			LoadCollectionRequest: milvuspb.LoadCollectionRequest{
				CollectionName: "foo",
				ReplicaNumber:  1,
			},
		})
		assert.NoError(t, err)
	})

	t.Run("release collection", func(t *testing.T) {
		milvusService.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(&milvuspb.BoolResponse{
			Status: &commonpb.Status{},
			Value:  true,
		}, nil).Once()
		milvusService.EXPECT().ReleaseCollection(mock.Anything, mock.Anything).Return(&commonpb.Status{}, nil).Once()
		err := dataHandler.ReleaseCollection(ctx, &api.ReleaseCollectionParam{
			ReleaseCollectionRequest: milvuspb.ReleaseCollectionRequest{
				CollectionName: "foo",
			},
		})
		assert.NoError(t, err)
	})

	t.Run("load partitions", func(t *testing.T) {
		milvusService.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(&milvuspb.BoolResponse{
			Status: &commonpb.Status{},
			Value:  true,
		}, nil).Once()
		milvusService.EXPECT().LoadPartitions(mock.Anything, mock.Anything).Return(&commonpb.Status{}, nil).Once()
		err := dataHandler.LoadPartitions(ctx, &api.LoadPartitionsParam{
			LoadPartitionsRequest: milvuspb.LoadPartitionsRequest{
				CollectionName: "foo",
				PartitionNames: []string{"bar"},
			},
		})
		assert.NoError(t, err)
	})

	t.Run("release partitions", func(t *testing.T) {
		milvusService.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(&milvuspb.BoolResponse{
			Status: &commonpb.Status{},
			Value:  true,
		}, nil).Once()
		milvusService.EXPECT().ReleasePartitions(mock.Anything, mock.Anything).Return(&commonpb.Status{}, nil).Once()
		err := dataHandler.ReleasePartitions(ctx, &api.ReleasePartitionsParam{
			ReleasePartitionsRequest: milvuspb.ReleasePartitionsRequest{
				CollectionName: "foo",
				PartitionNames: []string{"bar"},
			},
		})
		assert.NoError(t, err)
	})

	t.Run("flush", func(t *testing.T) {
		{
			milvusService.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(&milvuspb.BoolResponse{
				Status: &commonpb.Status{},
				Value:  true,
			}, nil).Once()
			milvusService.EXPECT().Flush(mock.Anything, mock.Anything).Return(&milvuspb.FlushResponse{Status: &commonpb.Status{}}, nil).Once()
			err := dataHandler.Flush(ctx, &api.FlushParam{
				FlushRequest: milvuspb.FlushRequest{
					CollectionNames: []string{"foo"},
				},
			})
			assert.NoError(t, err)
		}

		{
			milvusService.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(&milvuspb.BoolResponse{
				Status: &commonpb.Status{},
				Value:  true,
			}, nil).Once()
			milvusService.EXPECT().Flush(mock.Anything, mock.Anything).Return(&milvuspb.FlushResponse{Status: &commonpb.Status{Code: 500, ErrorCode: commonpb.ErrorCode_UnexpectedError}}, nil).Once()
			err := dataHandler.Flush(ctx, &api.FlushParam{
				FlushRequest: milvuspb.FlushRequest{
					CollectionNames: []string{"foo"},
				},
			})
			assert.Error(t, err)
		}
	})

	t.Run("create database", func(t *testing.T) {
		milvusService.EXPECT().CreateDatabase(mock.Anything, mock.Anything).Return(&commonpb.Status{}, nil).Once()
		err := dataHandler.CreateDatabase(ctx, &api.CreateDatabaseParam{
			CreateDatabaseRequest: milvuspb.CreateDatabaseRequest{
				DbName: "foo",
			},
		})
		assert.NoError(t, err)
	})

	t.Run("drop database", func(t *testing.T) {
		milvusService.EXPECT().DropDatabase(mock.Anything, mock.Anything).Return(&commonpb.Status{}, nil).Once()
		err := dataHandler.DropDatabase(ctx, &api.DropDatabaseParam{
			DropDatabaseRequest: milvuspb.DropDatabaseRequest{
				DbName: "foo",
			},
		})
		assert.NoError(t, err)
	})

	t.Run("replicate message", func(t *testing.T) {
		milvusService.EXPECT().ReplicateMessage(mock.Anything, mock.Anything).Return(&milvuspb.ReplicateMessageResponse{Status: &commonpb.Status{}, Position: "hello"}, nil).Once()
		err := dataHandler.ReplicateMessage(ctx, &api.ReplicateMessageParam{
			ChannelName: "foo",
			BeginTs:     1,
			EndTs:       2,
			MsgsBytes:   [][]byte{{1}, {2}},
			StartPositions: []*msgpb.MsgPosition{
				{
					ChannelName: "foo",
					MsgID:       []byte{1},
				},
			},
			EndPositions: []*msgpb.MsgPosition{
				{
					ChannelName: "foo",
					MsgID:       []byte{1},
				},
			},
		})
		assert.NoError(t, err)
	})

	t.Run("describe collection", func(t *testing.T) {
		milvusService.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{Status: &commonpb.Status{}}, nil).Once()
		err := dataHandler.DescribeCollection(ctx, &api.DescribeCollectionParam{
			Name: "foo",
		})
		assert.NoError(t, err)
	})

	t.Run("describe database", func(t *testing.T) {
		// server error
		{
			milvusService.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(&milvuspb.ListDatabasesResponse{Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Code:      500,
			}}, nil).Once()
			err := dataHandler.DescribeDatabase(ctx, &api.DescribeDatabaseParam{})
			assert.Error(t, err)
		}

		// not found database
		{
			milvusService.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(&milvuspb.ListDatabasesResponse{Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			}, DbNames: []string{
				"foo", "foo2",
			}}, nil).Once()
			err := dataHandler.DescribeDatabase(ctx, &api.DescribeDatabaseParam{
				Name: "foo3",
			})
			assert.Error(t, err)
		}

		// success
		{
			milvusService.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(&milvuspb.ListDatabasesResponse{Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			}, DbNames: []string{
				"foo", "foo2",
			}}, nil).Once()
			err := dataHandler.DescribeDatabase(ctx, &api.DescribeDatabaseParam{
				Name: "foo",
			})
			assert.NoError(t, err)
		}
	})
}
