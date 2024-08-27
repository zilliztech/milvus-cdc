/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * //
 *     http://www.apache.org/licenses/LICENSE-2.0
 * //
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package writer

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"

	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/mocks"
	"github.com/zilliztech/milvus-cdc/core/pb"
)

func GetMockObjs(t *testing.T) (*mocks.DataHandler, api.Writer) {
	dataHandler := mocks.NewDataHandler(t)
	messageManager := mocks.NewMessageManager(t)
	w := NewChannelWriter(dataHandler, config.WriterConfig{
		MessageBufferSize: 10,
		Retry: config.RetrySettings{
			RetryTimes:  2,
			InitBackOff: 1,
			MaxBackOff:  1,
		},
	}, map[string]map[string]uint64{})
	assert.NotNil(t, w)
	realWriter := w.(*ChannelWriter)
	realWriter.messageManager = messageManager
	return dataHandler, w
}

func TestChannelWriter(t *testing.T) {
	t.Run("wait collection ready", func(t *testing.T) {
		dataHandler, w := GetMockObjs(t)
		dataHandler.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(errors.New("mock")).Once()
		dataHandler.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(nil).Once()
		realWriter := w.(*ChannelWriter)
		assert.True(t, realWriter.WaitCollectionReady(context.Background(), "test", "", 0) == InfoStateCreated)
	})

	t.Run("wait database ready", func(t *testing.T) {
		dataHandler, w := GetMockObjs(t)
		dataHandler.EXPECT().DescribeDatabase(mock.Anything, mock.Anything).Return(errors.New("mock")).Once()
		dataHandler.EXPECT().DescribeDatabase(mock.Anything, mock.Anything).Return(nil).Once()
		realWriter := w.(*ChannelWriter)
		assert.True(t, realWriter.WaitDatabaseReady(context.Background(), "test", 0) == InfoStateCreated)
	})

	t.Run("handler api event", func(t *testing.T) {
		// unknow event type
		t.Run("unknow event type", func(t *testing.T) {
			_, w := GetMockObjs(t)
			err := w.HandleReplicateAPIEvent(context.Background(), &api.ReplicateAPIEvent{})
			assert.Error(t, err)
		})

		// create collection
		t.Run("create collection", func(t *testing.T) {
			dataHandler, w := GetMockObjs(t)
			dataHandler.EXPECT().CreateCollection(mock.Anything, mock.Anything).Return(errors.New("create collection mock")).Once()
			err := w.HandleReplicateAPIEvent(context.Background(), &api.ReplicateAPIEvent{
				EventType: api.ReplicateCreateCollection,
				CollectionInfo: &pb.CollectionInfo{
					Schema: &schemapb.CollectionSchema{
						Name: "test",
					},
					ShardsNum: 1,
				},
				ReplicateInfo: &commonpb.ReplicateInfo{
					MsgTimestamp: 1,
				},
			})
			assert.Error(t, err)
		})

		// create database with db
		t.Run("create database with db", func(t *testing.T) {
			dataHandler, w := GetMockObjs(t)
			ctx := context.Background()
			call := dataHandler.EXPECT().DescribeDatabase(mock.Anything, mock.Anything).Return(errors.New("describe database mock")).Maybe()
			err := w.HandleReplicateAPIEvent(ctx, &api.ReplicateAPIEvent{
				EventType: api.ReplicateCreateCollection,
				CollectionInfo: &pb.CollectionInfo{
					Schema: &schemapb.CollectionSchema{
						Name: "test",
					},
					ShardsNum: 1,
				},
				ReplicateParam: api.ReplicateParam{
					Database: "link",
				},
				ReplicateInfo: &commonpb.ReplicateInfo{
					MsgTimestamp: 2,
				},
			})
			assert.Error(t, err)
			call.Unset()
		})

		// success create collection with db
		t.Run("success create collection with db", func(t *testing.T) {
			dataHandler, w := GetMockObjs(t)
			dataHandler.EXPECT().DescribeDatabase(mock.Anything, mock.Anything).Return(nil).Once()
			dataHandler.EXPECT().CreateCollection(mock.Anything, mock.Anything).Return(nil).Once()
			err := w.HandleReplicateAPIEvent(context.Background(), &api.ReplicateAPIEvent{
				EventType: api.ReplicateCreateCollection,
				CollectionInfo: &pb.CollectionInfo{
					Schema: &schemapb.CollectionSchema{
						Name: "test",
					},
					ShardsNum: 1,
				},
				ReplicateParam: api.ReplicateParam{
					Database: "link",
				},
				ReplicateInfo: &commonpb.ReplicateInfo{
					MsgTimestamp: 3,
				},
			})
			assert.NoError(t, err)
		})

		// drop collectiom
		t.Run("drop collection", func(t *testing.T) {
			dataHandler, w := GetMockObjs(t)
			dataHandler.EXPECT().DropCollection(mock.Anything, mock.Anything).Return(errors.New("drop cpllection mock")).Once()
			err := w.HandleReplicateAPIEvent(context.Background(), &api.ReplicateAPIEvent{
				EventType: api.ReplicateDropCollection,
				CollectionInfo: &pb.CollectionInfo{
					Schema: &schemapb.CollectionSchema{
						Name: "test",
					},
				},
				ReplicateInfo: &commonpb.ReplicateInfo{
					MsgTimestamp: 4,
				},
			})
			assert.Error(t, err)
		})

		// drop collection with db
		t.Run("drop collection with db", func(t *testing.T) {
			dataHandler, w := GetMockObjs(t)
			ctx := context.Background()
			call := dataHandler.EXPECT().DescribeDatabase(mock.Anything, mock.Anything).Return(errors.New("describe database mock")).Maybe()
			err := w.HandleReplicateAPIEvent(ctx, &api.ReplicateAPIEvent{
				EventType: api.ReplicateDropCollection,
				CollectionInfo: &pb.CollectionInfo{
					Schema: &schemapb.CollectionSchema{
						Name: "test",
					},
				},
				ReplicateInfo: &commonpb.ReplicateInfo{
					MsgTimestamp: 5,
				},
				ReplicateParam: api.ReplicateParam{Database: "link2"},
			})
			assert.Error(t, err)
			call.Unset()
		})

		// create partition
		t.Run("create partition", func(t *testing.T) {
			dataHandler, w := GetMockObjs(t)
			dataHandler.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(nil).Once()
			dataHandler.EXPECT().CreatePartition(mock.Anything, mock.Anything).Return(errors.New("create partition mock")).Once()
			err := w.HandleReplicateAPIEvent(context.Background(), &api.ReplicateAPIEvent{
				EventType: api.ReplicateCreatePartition,
				CollectionInfo: &pb.CollectionInfo{
					Schema: &schemapb.CollectionSchema{
						Name: "test",
					},
				},
				PartitionInfo: &pb.PartitionInfo{
					PartitionName: "test",
				},
				ReplicateInfo: &commonpb.ReplicateInfo{
					MsgTimestamp: 6,
				},
			})
			assert.Error(t, err)
		})
		t.Run("create partition with db", func(t *testing.T) {
			dataHandler, w := GetMockObjs(t)
			ctx := context.Background()
			call := dataHandler.EXPECT().DescribeDatabase(mock.Anything, mock.Anything).Return(errors.New("describe database mock")).Maybe()
			err := w.HandleReplicateAPIEvent(ctx, &api.ReplicateAPIEvent{
				EventType: api.ReplicateCreatePartition,
				CollectionInfo: &pb.CollectionInfo{
					Schema: &schemapb.CollectionSchema{
						Name: "test",
					},
				},
				PartitionInfo: &pb.PartitionInfo{
					PartitionName: "test",
				},
				ReplicateInfo: &commonpb.ReplicateInfo{
					MsgTimestamp: 7,
				},
				ReplicateParam: api.ReplicateParam{Database: "link3"},
			})
			assert.Error(t, err)
			call.Unset()
		})

		// drop partition
		t.Run("drop partition", func(t *testing.T) {
			dataHandler, w := GetMockObjs(t)
			dataHandler.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(nil).Once()
			dataHandler.EXPECT().DropPartition(mock.Anything, mock.Anything).Return(errors.New("drop partition mock")).Once()
			err := w.HandleReplicateAPIEvent(context.Background(), &api.ReplicateAPIEvent{
				EventType: api.ReplicateDropPartition,
				CollectionInfo: &pb.CollectionInfo{
					Schema: &schemapb.CollectionSchema{
						Name: "test",
					},
				},
				PartitionInfo: &pb.PartitionInfo{
					PartitionName: "test",
				},
				ReplicateInfo: &commonpb.ReplicateInfo{
					MsgTimestamp: 8,
				},
			})
			assert.Error(t, err)
		})
		t.Run("drop partition with db", func(t *testing.T) {
			dataHandler, w := GetMockObjs(t)
			ctx := context.Background()
			call := dataHandler.EXPECT().DescribeDatabase(mock.Anything, mock.Anything).Return(errors.New("describe database mock")).Maybe()
			err := w.HandleReplicateAPIEvent(ctx, &api.ReplicateAPIEvent{
				EventType: api.ReplicateDropPartition,
				CollectionInfo: &pb.CollectionInfo{
					Schema: &schemapb.CollectionSchema{
						Name: "test",
					},
				},
				PartitionInfo: &pb.PartitionInfo{
					PartitionName: "test",
				},
				ReplicateInfo: &commonpb.ReplicateInfo{
					MsgTimestamp: 9,
				},
				ReplicateParam: api.ReplicateParam{Database: "link4"},
			})
			assert.Error(t, err)
			call.Unset()
		})
	})

	t.Run("handler replicate msg", func(t *testing.T) {
		// empty
		{
			_, w := GetMockObjs(t)
			_, _, err := w.HandleReplicateMessage(context.Background(), "test", &msgstream.MsgPack{})
			assert.Error(t, err)
		}

		// success
		{
			_, w := GetMockObjs(t)
			realWriter := w.(*ChannelWriter)
			messageManager := realWriter.messageManager.(*mocks.MessageManager)
			messageManager.EXPECT().ReplicateMessage(mock.Anything).Run(func(rm *api.ReplicateMessage) {
				rm.Param.TargetMsgPosition = base64.StdEncoding.EncodeToString([]byte("foo"))
				rm.SuccessFunc(rm.Param)
			}).Return().Once()

			endPosition, targetPosition, err := w.HandleReplicateMessage(context.Background(), "test", &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.CreateCollectionMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues: []uint32{1},
						},
						CreateCollectionRequest: &msgpb.CreateCollectionRequest{
							Base: &commonpb.MsgBase{
								MsgType:  commonpb.MsgType_CreateCollection,
								SourceID: 1,
							},
							CollectionName: "test",
						},
					},
				},
				EndPositions: []*msgstream.MsgPosition{
					{
						ChannelName: "test",
						MsgID:       []byte("foo"),
					},
				},
			})
			assert.NoError(t, err)
			assert.Equal(t, endPosition, targetPosition)
		}

		// fail
		{
			_, w := GetMockObjs(t)
			realWriter := w.(*ChannelWriter)
			messageManager := realWriter.messageManager.(*mocks.MessageManager)
			messageManager.EXPECT().ReplicateMessage(mock.Anything).Run(func(rm *api.ReplicateMessage) {
				rm.Param.TargetMsgPosition = base64.StdEncoding.EncodeToString([]byte("foo"))
				rm.FailFunc(rm.Param, errors.New("mock"))
			}).Return().Once()

			_, _, err := w.HandleReplicateMessage(context.Background(), "test", &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.CreateCollectionMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues: []uint32{1},
						},
						CreateCollectionRequest: &msgpb.CreateCollectionRequest{
							Base: &commonpb.MsgBase{
								MsgType:  commonpb.MsgType_CreateCollection,
								SourceID: 1,
							},
							CollectionName: "test",
						},
					},
				},
				EndPositions: []*msgstream.MsgPosition{
					{
						ChannelName: "test",
						MsgID:       []byte("foo"),
					},
				},
			})
			assert.Error(t, err)
		}
	})

	t.Run("handler op msg", func(t *testing.T) {
		// empty pack
		t.Run("empty pack", func(t *testing.T) {
			_, w := GetMockObjs(t)
			_, err := w.HandleOpMessagePack(context.Background(), &msgstream.MsgPack{})
			assert.Error(t, err)
		})

		// more than one message
		t.Run("more than one message", func(t *testing.T) {
			_, w := GetMockObjs(t)
			_, err := w.HandleOpMessagePack(context.Background(), &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.CreateCollectionMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues: []uint32{1},
						},
						CreateCollectionRequest: &msgpb.CreateCollectionRequest{
							Base: &commonpb.MsgBase{
								MsgType:  commonpb.MsgType_CreateCollection,
								SourceID: 1,
							},
							CollectionName: "test",
						},
					},
					&msgstream.CreateCollectionMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues: []uint32{1},
						},
						CreateCollectionRequest: &msgpb.CreateCollectionRequest{
							Base: &commonpb.MsgBase{
								MsgType:  commonpb.MsgType_CreateCollection,
								SourceID: 1,
							},
							CollectionName: "test2",
						},
					},
				},
				EndPositions: []*msgstream.MsgPosition{
					{
						ChannelName: "test",
						MsgID:       []byte("foo"),
					},
				},
			})
			assert.Error(t, err)
		})

		// unknown msg type
		t.Run("unknown msg type", func(t *testing.T) {
			_, w := GetMockObjs(t)
			_, err := w.HandleOpMessagePack(context.Background(), &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.CreateCollectionMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues: []uint32{1},
						},
						CreateCollectionRequest: &msgpb.CreateCollectionRequest{
							Base: &commonpb.MsgBase{
								MsgType:  commonpb.MsgType_DescribeCollection,
								SourceID: 1,
							},
							CollectionName: "test",
						},
					},
				},
				EndPositions: []*msgstream.MsgPosition{
					{
						ChannelName: "test",
						MsgID:       []byte("foo"),
					},
				},
			})
			assert.Error(t, err)
		})

		// create database
		t.Run("create database", func(t *testing.T) {
			dataHandler, w := GetMockObjs(t)
			dataHandler.EXPECT().CreateDatabase(mock.Anything, mock.Anything).Return(errors.New("mock")).Once()
			_, err := w.HandleOpMessagePack(context.Background(), &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.CreateDatabaseMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues: []uint32{1},
						},
						CreateDatabaseRequest: &milvuspb.CreateDatabaseRequest{
							Base: &commonpb.MsgBase{
								MsgType:  commonpb.MsgType_CreateDatabase,
								SourceID: 1,
							},
							DbName: "test",
						},
					},
				},
				EndPositions: []*msgstream.MsgPosition{
					{
						ChannelName: "test",
						MsgID:       []byte("foo"),
					},
				},
			})
			assert.Error(t, err)
		})

		// drop database
		t.Run("drop database", func(t *testing.T) {
			dataHandler, w := GetMockObjs(t)
			dataHandler.EXPECT().DropDatabase(mock.Anything, mock.Anything).Return(errors.New("mock")).Once()
			_, err := w.HandleOpMessagePack(context.Background(), &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.DropDatabaseMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues: []uint32{1},
						},
						DropDatabaseRequest: &milvuspb.DropDatabaseRequest{
							Base: &commonpb.MsgBase{
								MsgType:  commonpb.MsgType_DropDatabase,
								SourceID: 1,
							},
							DbName: "test",
						},
					},
				},
				EndPositions: []*msgstream.MsgPosition{
					{
						ChannelName: "test",
						MsgID:       []byte("foo"),
					},
				},
			})
			assert.Error(t, err)
		})

		// flush
		t.Run("flush", func(t *testing.T) {
			dataHandler, w := GetMockObjs(t)
			dataHandler.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(nil).Once()
			dataHandler.EXPECT().Flush(mock.Anything, mock.Anything).Return(errors.New("mock")).Once()
			_, err := w.HandleOpMessagePack(context.Background(), &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.FlushMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues:   []uint32{1},
							EndTimestamp: 100,
						},
						FlushRequest: &milvuspb.FlushRequest{
							Base: &commonpb.MsgBase{
								MsgType:  commonpb.MsgType_Flush,
								SourceID: 1,
							},
							CollectionNames: []string{"test2"},
						},
					},
				},
				EndPositions: []*msgstream.MsgPosition{
					{
						ChannelName: "test",
						MsgID:       []byte("foo"),
					},
				},
			})
			assert.Error(t, err)
		})

		// flush with not ready collection
		t.Run("flush with not ready collection", func(t *testing.T) {
			dataHandler, w := GetMockObjs(t)
			ctx := context.Background()
			call := dataHandler.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(errors.New("foo")).Maybe()
			_, err := w.HandleOpMessagePack(ctx, &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.FlushMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues:   []uint32{1},
							EndTimestamp: 100,
						},
						FlushRequest: &milvuspb.FlushRequest{
							Base: &commonpb.MsgBase{
								MsgType:  commonpb.MsgType_Flush,
								SourceID: 1,
							},
							CollectionNames: []string{"test3"},
						},
					},
				},
				EndPositions: []*msgstream.MsgPosition{
					{
						ChannelName: "test",
						MsgID:       []byte("foo"),
					},
				},
			})
			assert.Error(t, err)
			call.Unset()
		})

		// flush with db
		t.Run("flush with db", func(t *testing.T) {
			dataHandler, w := GetMockObjs(t)
			ctx := context.Background()
			call := dataHandler.EXPECT().DescribeDatabase(mock.Anything, mock.Anything).Return(errors.New("mock")).Maybe()

			_, err := w.HandleOpMessagePack(ctx, &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.FlushMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues:   []uint32{1},
							EndTimestamp: 100,
						},
						FlushRequest: &milvuspb.FlushRequest{
							Base: &commonpb.MsgBase{
								MsgType:  commonpb.MsgType_Flush,
								SourceID: 1,
							},
							CollectionNames: []string{"test"},
							DbName:          "tree",
						},
					},
				},
				EndPositions: []*msgstream.MsgPosition{
					{
						ChannelName: "test",
						MsgID:       []byte("foo"),
					},
				},
			})
			assert.Error(t, err)

			call.Unset()
		})

		// create index
		t.Run("create index", func(t *testing.T) {
			dataHandler, w := GetMockObjs(t)
			dataHandler.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(nil).Once()
			dataHandler.EXPECT().CreateIndex(mock.Anything, mock.Anything).Return(errors.New("mock")).Once()
			_, err := w.HandleOpMessagePack(context.Background(), &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.CreateIndexMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues:   []uint32{1},
							EndTimestamp: 100,
						},
						CreateIndexRequest: &milvuspb.CreateIndexRequest{
							Base: &commonpb.MsgBase{
								MsgType:  commonpb.MsgType_CreateIndex,
								SourceID: 1,
							},
							CollectionName: "test",
						},
					},
				},
				EndPositions: []*msgstream.MsgPosition{
					{
						ChannelName: "test",
						MsgID:       []byte("foo"),
					},
				},
			})
			assert.Error(t, err)
		})

		// create index with not ready collection
		t.Run("create index with not ready collection", func(t *testing.T) {
			dataHandler, w := GetMockObjs(t)
			ctx := context.Background()
			call := dataHandler.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(errors.New("foo")).Maybe()
			_, err := w.HandleOpMessagePack(ctx, &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.CreateIndexMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues:   []uint32{1},
							EndTimestamp: 100,
						},
						CreateIndexRequest: &milvuspb.CreateIndexRequest{
							Base: &commonpb.MsgBase{
								MsgType:  commonpb.MsgType_CreateIndex,
								SourceID: 1,
							},
							CollectionName: "test-create-index-1",
						},
					},
				},
				EndPositions: []*msgstream.MsgPosition{
					{
						ChannelName: "test",
						MsgID:       []byte("foo"),
					},
				},
			})
			assert.Error(t, err)
			call.Unset()
		})

		// create index with db
		t.Run("create index with db", func(t *testing.T) {
			dataHandler, w := GetMockObjs(t)
			ctx := context.Background()
			call := dataHandler.EXPECT().DescribeDatabase(mock.Anything, mock.Anything).Return(errors.New("mock")).Maybe()
			_, err := w.HandleOpMessagePack(ctx, &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.CreateIndexMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues:   []uint32{1},
							EndTimestamp: 100,
						},
						CreateIndexRequest: &milvuspb.CreateIndexRequest{
							Base: &commonpb.MsgBase{
								MsgType:  commonpb.MsgType_CreateIndex,
								SourceID: 1,
							},
							CollectionName: "test",
							DbName:         "tree-create-index-db-1",
						},
					},
				},
				EndPositions: []*msgstream.MsgPosition{
					{
						ChannelName: "test",
						MsgID:       []byte("foo"),
					},
				},
			})
			assert.Error(t, err)
			call.Unset()
		})

		// drop index
		t.Run("drop index", func(t *testing.T) {
			dataHandler, w := GetMockObjs(t)
			dataHandler.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(nil).Once()
			dataHandler.EXPECT().DropIndex(mock.Anything, mock.Anything).Return(errors.New("mock")).Once()
			_, err := w.HandleOpMessagePack(context.Background(), &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.DropIndexMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues:   []uint32{1},
							EndTimestamp: 100,
						},
						DropIndexRequest: &milvuspb.DropIndexRequest{
							Base: &commonpb.MsgBase{
								MsgType:  commonpb.MsgType_DropIndex,
								SourceID: 1,
							},
							CollectionName: "test",
						},
					},
				},
				EndPositions: []*msgstream.MsgPosition{
					{
						ChannelName: "test",
						MsgID:       []byte("foo"),
					},
				},
			})
			assert.Error(t, err)
		})

		// load
		t.Run("load", func(t *testing.T) {
			dataHandler, w := GetMockObjs(t)
			dataHandler.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(nil).Once()
			dataHandler.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(errors.New("mock")).Once()
			_, err := w.HandleOpMessagePack(context.Background(), &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.LoadCollectionMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues:   []uint32{1},
							EndTimestamp: 100,
						},
						LoadCollectionRequest: &milvuspb.LoadCollectionRequest{
							Base: &commonpb.MsgBase{
								MsgType:  commonpb.MsgType_LoadCollection,
								SourceID: 1,
							},
							CollectionName: "test",
						},
					},
				},
				EndPositions: []*msgstream.MsgPosition{
					{
						ChannelName: "test",
						MsgID:       []byte("foo"),
					},
				},
			})
			assert.Error(t, err)
		})

		// load with not ready collection
		t.Run("load with not ready collection", func(t *testing.T) {
			dataHandler, w := GetMockObjs(t)
			ctx := context.Background()
			call := dataHandler.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(errors.New("foo")).Maybe()
			_, err := w.HandleOpMessagePack(ctx, &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.LoadCollectionMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues:   []uint32{1},
							EndTimestamp: 100,
						},
						LoadCollectionRequest: &milvuspb.LoadCollectionRequest{
							Base: &commonpb.MsgBase{
								MsgType:  commonpb.MsgType_LoadCollection,
								SourceID: 1,
							},
							CollectionName: "test-load-collection-1",
						},
					},
				},
				EndPositions: []*msgstream.MsgPosition{
					{
						ChannelName: "test",
						MsgID:       []byte("foo"),
					},
				},
			})
			assert.Error(t, err)
			call.Unset()
		})

		// load with db
		t.Run("load with db", func(t *testing.T) {
			dataHandler, w := GetMockObjs(t)
			ctx := context.Background()
			call := dataHandler.EXPECT().DescribeDatabase(mock.Anything, mock.Anything).Return(errors.New("mock")).Maybe()
			_, err := w.HandleOpMessagePack(ctx, &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.LoadCollectionMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues:   []uint32{1},
							EndTimestamp: 100,
						},
						LoadCollectionRequest: &milvuspb.LoadCollectionRequest{
							Base: &commonpb.MsgBase{
								MsgType:  commonpb.MsgType_LoadCollection,
								SourceID: 1,
							},
							CollectionName: "test",
							DbName:         "tree-load-db-1",
						},
					},
				},
				EndPositions: []*msgstream.MsgPosition{
					{
						ChannelName: "test",
						MsgID:       []byte("foo"),
					},
				},
			})
			assert.Error(t, err)
			call.Unset()
		})

		// release fail
		t.Run("release fail", func(t *testing.T) {
			dataHandler, w := GetMockObjs(t)
			dataHandler.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(nil).Once()
			dataHandler.EXPECT().ReleaseCollection(mock.Anything, mock.Anything).Return(errors.New("mock")).Once()
			_, err := w.HandleOpMessagePack(context.Background(), &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.ReleaseCollectionMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues:   []uint32{1},
							EndTimestamp: 100,
						},
						ReleaseCollectionRequest: &milvuspb.ReleaseCollectionRequest{
							Base: &commonpb.MsgBase{
								MsgType:  commonpb.MsgType_ReleaseCollection,
								SourceID: 1,
							},
							CollectionName: "test",
						},
					},
				},
				EndPositions: []*msgstream.MsgPosition{
					{
						ChannelName: "test",
						MsgID:       []byte("foo"),
					},
				},
			})
			assert.Error(t, err)
		})

		// release success
		t.Run("release success", func(t *testing.T) {
			dataHandler, w := GetMockObjs(t)
			dataHandler.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(nil).Once()
			dataHandler.EXPECT().ReleaseCollection(mock.Anything, mock.Anything).Return(nil).Once()
			_, err := w.HandleOpMessagePack(context.Background(), &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.ReleaseCollectionMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues:   []uint32{1},
							EndTimestamp: 100,
						},
						ReleaseCollectionRequest: &milvuspb.ReleaseCollectionRequest{
							Base: &commonpb.MsgBase{
								MsgType:  commonpb.MsgType_ReleaseCollection,
								SourceID: 1,
							},
							CollectionName: "test",
						},
					},
				},
				EndPositions: []*msgstream.MsgPosition{
					{
						ChannelName: "test",
						MsgID:       []byte("foo"),
					},
				},
			})
			assert.NoError(t, err)
		})
	})

	t.Run("load partitions", func(t *testing.T) {
		// load partitions without db, fail
		t.Run("load partitions without db, fail", func(t *testing.T) {
			dataHandler, w := GetMockObjs(t)
			dataHandler.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(nil).Once()
			dataHandler.EXPECT().DescribePartition(mock.Anything, mock.Anything).Return(nil).Twice()
			dataHandler.EXPECT().LoadPartitions(mock.Anything, mock.Anything).Return(errors.New("mock")).Once()
			_, err := w.HandleOpMessagePack(context.Background(), &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.LoadPartitionsMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues:   []uint32{1},
							EndTimestamp: 100,
						},
						LoadPartitionsRequest: &milvuspb.LoadPartitionsRequest{
							Base: &commonpb.MsgBase{
								MsgType:  commonpb.MsgType_LoadPartitions,
								SourceID: 1,
							},
							CollectionName: "test",
							PartitionNames: []string{"p1", "p2"},
						},
					},
				},
				EndPositions: []*msgstream.MsgPosition{
					{
						ChannelName: "test",
						MsgID:       []byte("foo"),
					},
				},
			})
			assert.Error(t, err)
		})

		// load partition with not ready collection
		t.Run("load partition with not ready collection", func(t *testing.T) {
			dataHandler, w := GetMockObjs(t)
			ctx := context.Background()
			call := dataHandler.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(errors.New("foo")).Maybe()
			_, err := w.HandleOpMessagePack(ctx, &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.LoadPartitionsMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues:   []uint32{1},
							EndTimestamp: 100,
						},
						LoadPartitionsRequest: &milvuspb.LoadPartitionsRequest{
							Base: &commonpb.MsgBase{
								MsgType:  commonpb.MsgType_LoadPartitions,
								SourceID: 1,
							},
							CollectionName: "test-collection-load-partition-1",
							PartitionNames: []string{"p1", "p2"},
						},
					},
				},
				EndPositions: []*msgstream.MsgPosition{
					{
						ChannelName: "test",
						MsgID:       []byte("foo"),
					},
				},
			})
			assert.Error(t, err)
			call.Unset()
		})

		// load partition with not ready db
		t.Run("load partition with not ready db", func(t *testing.T) {
			dataHandler, w := GetMockObjs(t)
			ctx := context.Background()
			call := dataHandler.EXPECT().DescribeDatabase(mock.Anything, mock.Anything).Return(errors.New("foo")).Maybe()
			_, err := w.HandleOpMessagePack(ctx, &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.LoadPartitionsMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues:   []uint32{1},
							EndTimestamp: 100,
						},
						LoadPartitionsRequest: &milvuspb.LoadPartitionsRequest{
							Base: &commonpb.MsgBase{
								MsgType:  commonpb.MsgType_LoadPartitions,
								SourceID: 1,
							},
							CollectionName: "test",
							PartitionNames: []string{"p1", "p2"},
							DbName:         "tree-load-partition-db-1",
						},
					},
				},
				EndPositions: []*msgstream.MsgPosition{
					{
						ChannelName: "test",
						MsgID:       []byte("foo"),
					},
				},
			})
			assert.Error(t, err)
			call.Unset()
		})

		// load partition success
		t.Run("load partition success", func(t *testing.T) {
			dataHandler, w := GetMockObjs(t)
			dataHandler.EXPECT().DescribeDatabase(mock.Anything, mock.Anything).Return(nil).Once()
			dataHandler.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(nil).Once()
			dataHandler.EXPECT().DescribePartition(mock.Anything, mock.Anything).Return(nil).Twice()
			dataHandler.EXPECT().LoadPartitions(mock.Anything, mock.Anything).Return(nil).Once()
			_, err := w.HandleOpMessagePack(context.Background(), &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.LoadPartitionsMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues:   []uint32{1},
							EndTimestamp: 100,
						},
						LoadPartitionsRequest: &milvuspb.LoadPartitionsRequest{
							Base: &commonpb.MsgBase{
								MsgType:  commonpb.MsgType_LoadPartitions,
								SourceID: 1,
							},
							CollectionName: "test",
							PartitionNames: []string{"p1", "p2"},
							DbName:         "tree",
						},
					},
				},
				EndPositions: []*msgstream.MsgPosition{
					{
						ChannelName: "test",
						MsgID:       []byte("foo"),
					},
				},
			})
			assert.NoError(t, err)
		})
	})

	t.Run("release partitions", func(t *testing.T) {
		// release partitions without db, fail
		t.Run("release partitions without db, fail", func(t *testing.T) {
			dataHandler, w := GetMockObjs(t)
			dataHandler.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(nil).Once()
			dataHandler.EXPECT().DescribePartition(mock.Anything, mock.Anything).Return(nil).Twice()
			dataHandler.EXPECT().ReleasePartitions(mock.Anything, mock.Anything).Return(errors.New("mock")).Once()
			_, err := w.HandleOpMessagePack(context.Background(), &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.ReleasePartitionsMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues:   []uint32{1},
							EndTimestamp: 100,
						},
						ReleasePartitionsRequest: &milvuspb.ReleasePartitionsRequest{
							Base: &commonpb.MsgBase{
								MsgType:  commonpb.MsgType_ReleasePartitions,
								SourceID: 1,
							},
							CollectionName: "test",
							PartitionNames: []string{"p1", "p2"},
						},
					},
				},
				EndPositions: []*msgstream.MsgPosition{
					{
						ChannelName: "test",
						MsgID:       []byte("foo"),
					},
				},
			})
			assert.Error(t, err)
		})

		// release partitions with not ready collection
		t.Run("release partitions with not ready collection", func(t *testing.T) {
			dataHandler, w := GetMockObjs(t)
			ctx := context.Background()
			call := dataHandler.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(errors.New("foo")).Maybe()
			_, err := w.HandleOpMessagePack(ctx, &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.ReleasePartitionsMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues:   []uint32{1},
							EndTimestamp: 100,
						},
						ReleasePartitionsRequest: &milvuspb.ReleasePartitionsRequest{
							Base: &commonpb.MsgBase{
								MsgType:  commonpb.MsgType_ReleasePartitions,
								SourceID: 1,
							},
							CollectionName: "test-collection-release-collection-1",
							PartitionNames: []string{"p1", "p2"},
						},
					},
				},
				EndPositions: []*msgstream.MsgPosition{
					{
						ChannelName: "test",
						MsgID:       []byte("foo"),
					},
				},
			})
			assert.Error(t, err)
			call.Unset()
		})

		// release partitions with not ready db
		t.Run("release partitions with not ready db", func(t *testing.T) {
			dataHandler, w := GetMockObjs(t)
			ctx := context.Background()
			call := dataHandler.EXPECT().DescribeDatabase(mock.Anything, mock.Anything).Return(errors.New("foo")).Maybe()
			_, err := w.HandleOpMessagePack(ctx, &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.ReleasePartitionsMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues:   []uint32{1},
							EndTimestamp: 100,
						},
						ReleasePartitionsRequest: &milvuspb.ReleasePartitionsRequest{
							Base: &commonpb.MsgBase{
								MsgType:  commonpb.MsgType_ReleasePartitions,
								SourceID: 1,
							},
							CollectionName: "test",
							PartitionNames: []string{"p1", "p2"},
							DbName:         "tree-release-partition-db-1",
						},
					},
				},
				EndPositions: []*msgstream.MsgPosition{
					{
						ChannelName: "test",
						MsgID:       []byte("foo"),
					},
				},
			})
			assert.Error(t, err)
			call.Unset()
		})

		// release partitions success
		t.Run("release partitions success", func(t *testing.T) {
			dataHandler, w := GetMockObjs(t)
			dataHandler.EXPECT().DescribeDatabase(mock.Anything, mock.Anything).Return(nil).Once()
			dataHandler.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(nil).Once()
			dataHandler.EXPECT().DescribePartition(mock.Anything, mock.Anything).Return(nil).Twice()
			dataHandler.EXPECT().ReleasePartitions(mock.Anything, mock.Anything).Return(nil).Once()
			_, err := w.HandleOpMessagePack(context.Background(), &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.ReleasePartitionsMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues:   []uint32{1},
							EndTimestamp: 100,
						},
						ReleasePartitionsRequest: &milvuspb.ReleasePartitionsRequest{
							Base: &commonpb.MsgBase{
								MsgType:  commonpb.MsgType_ReleasePartitions,
								SourceID: 1,
							},
							CollectionName: "test",
							PartitionNames: []string{"p1", "p2"},
							DbName:         "tree",
						},
					},
				},
				EndPositions: []*msgstream.MsgPosition{
					{
						ChannelName: "test",
						MsgID:       []byte("foo"),
					},
				},
			})
			assert.NoError(t, err)
		})
	})
}
