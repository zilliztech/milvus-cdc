package writer

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/mocks"
	"github.com/zilliztech/milvus-cdc/core/pb"
)

func TestChannelWriter(t *testing.T) {
	dataHandler := mocks.NewDataHandler(t)
	messageManager := mocks.NewMessageManager(t)
	w := NewChannelWriter(dataHandler, 10)
	assert.NotNil(t, w)

	realWriter := w.(*ChannelWriter)
	realWriter.messageManager = messageManager

	t.Run("wait collection ready", func(t *testing.T) {
		dataHandler.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(errors.New("mock")).Once()
		dataHandler.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(nil).Once()
		assert.True(t, realWriter.WaitCollectionReady(context.Background(), "test"))
	})

	t.Run("handler api event", func(t *testing.T) {
		// unknow event type
		{
			err := w.HandleReplicateAPIEvent(context.Background(), &api.ReplicateAPIEvent{})
			assert.Error(t, err)
		}

		// create collection
		{
			dataHandler.EXPECT().CreateCollection(mock.Anything, mock.Anything).Return(errors.New("mock")).Once()
			err := w.HandleReplicateAPIEvent(context.Background(), &api.ReplicateAPIEvent{
				EventType: api.ReplicateCreateCollection,
				CollectionInfo: &pb.CollectionInfo{
					Schema: &schemapb.CollectionSchema{
						Name: "test",
					},
					ShardsNum: 1,
				},
			})
			assert.Error(t, err)
		}

		// drop collectiom
		{
			dataHandler.EXPECT().DropCollection(mock.Anything, mock.Anything).Return(errors.New("mock")).Once()
			err := w.HandleReplicateAPIEvent(context.Background(), &api.ReplicateAPIEvent{
				EventType: api.ReplicateDropCollection,
				CollectionInfo: &pb.CollectionInfo{
					Schema: &schemapb.CollectionSchema{
						Name: "test",
					},
				},
			})
			assert.Error(t, err)
		}

		// create partition
		{
			dataHandler.EXPECT().CreatePartition(mock.Anything, mock.Anything).Return(errors.New("mock")).Once()
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
			})
			assert.Error(t, err)
		}

		// drop partition
		{
			dataHandler.EXPECT().DropPartition(mock.Anything, mock.Anything).Return(errors.New("mock")).Once()
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
			})
			assert.Error(t, err)
		}
	})

	t.Run("handler replicate msg", func(t *testing.T) {
		// empty
		{
			_, _, err := w.HandleReplicateMessage(context.Background(), "test", &msgstream.MsgPack{})
			assert.Error(t, err)
		}

		// success
		{
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
						CreateCollectionRequest: msgpb.CreateCollectionRequest{
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
						CreateCollectionRequest: msgpb.CreateCollectionRequest{
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
		{
			_, err := w.HandleOpMessagePack(context.Background(), &msgstream.MsgPack{})
			assert.Error(t, err)
		}

		// more than one message
		{
			_, err := w.HandleOpMessagePack(context.Background(), &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.CreateCollectionMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues: []uint32{1},
						},
						CreateCollectionRequest: msgpb.CreateCollectionRequest{
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
						CreateCollectionRequest: msgpb.CreateCollectionRequest{
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
		}

		// unknown msg type
		{
			_, err := w.HandleOpMessagePack(context.Background(), &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.CreateCollectionMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues: []uint32{1},
						},
						CreateCollectionRequest: msgpb.CreateCollectionRequest{
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
		}

		// create database
		{
			dataHandler.EXPECT().CreateDatabase(mock.Anything, mock.Anything).Return(errors.New("mock")).Once()
			_, err := w.HandleOpMessagePack(context.Background(), &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.CreateDatabaseMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues: []uint32{1},
						},
						CreateDatabaseRequest: milvuspb.CreateDatabaseRequest{
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
		}

		// drop database
		{
			dataHandler.EXPECT().DropDatabase(mock.Anything, mock.Anything).Return(errors.New("mock")).Once()
			_, err := w.HandleOpMessagePack(context.Background(), &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.DropDatabaseMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues: []uint32{1},
						},
						DropDatabaseRequest: milvuspb.DropDatabaseRequest{
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
		}

		// flush
		{
			dataHandler.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(nil).Once()
			dataHandler.EXPECT().Flush(mock.Anything, mock.Anything).Return(errors.New("mock")).Once()
			_, err := w.HandleOpMessagePack(context.Background(), &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.FlushMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues: []uint32{1},
						},
						FlushRequest: milvuspb.FlushRequest{
							Base: &commonpb.MsgBase{
								MsgType:  commonpb.MsgType_Flush,
								SourceID: 1,
							},
							CollectionNames: []string{"test"},
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

		// create index
		{
			dataHandler.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(nil).Once()
			dataHandler.EXPECT().CreateIndex(mock.Anything, mock.Anything).Return(errors.New("mock")).Once()
			_, err := w.HandleOpMessagePack(context.Background(), &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.CreateIndexMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues: []uint32{1},
						},
						CreateIndexRequest: milvuspb.CreateIndexRequest{
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
		}

		// drop index
		{
			dataHandler.EXPECT().DropIndex(mock.Anything, mock.Anything).Return(errors.New("mock")).Once()
			_, err := w.HandleOpMessagePack(context.Background(), &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.DropIndexMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues: []uint32{1},
						},
						DropIndexRequest: milvuspb.DropIndexRequest{
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
		}

		// load
		{
			dataHandler.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(nil).Once()
			dataHandler.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(errors.New("mock")).Once()
			_, err := w.HandleOpMessagePack(context.Background(), &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.LoadCollectionMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues: []uint32{1},
						},
						LoadCollectionRequest: milvuspb.LoadCollectionRequest{
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
		}

		// release
		{
			dataHandler.EXPECT().ReleaseCollection(mock.Anything, mock.Anything).Return(errors.New("mock")).Once()
			_, err := w.HandleOpMessagePack(context.Background(), &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.ReleaseCollectionMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues: []uint32{1},
						},
						ReleaseCollectionRequest: milvuspb.ReleaseCollectionRequest{
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
		}

		// success
		{
			dataHandler.EXPECT().ReleaseCollection(mock.Anything, mock.Anything).Return(nil).Once()
			_, err := w.HandleOpMessagePack(context.Background(), &msgstream.MsgPack{
				Msgs: []msgstream.TsMsg{
					&msgstream.ReleaseCollectionMsg{
						BaseMsg: msgstream.BaseMsg{
							HashValues: []uint32{1},
						},
						ReleaseCollectionRequest: milvuspb.ReleaseCollectionRequest{
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
		}
	})
}
