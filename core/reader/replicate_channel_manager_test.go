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

package reader

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/mocks"
	"github.com/zilliztech/milvus-cdc/core/model"
	"github.com/zilliztech/milvus-cdc/core/pb"
)

func TestNewReplicateChannelManager(t *testing.T) {
	t.Run("empty config", func(t *testing.T) {
		_, err := NewReplicateChannelManager(config.MQConfig{}, NewDefaultFactoryCreator(), nil, config.ReaderConfig{
			MessageBufferSize: 10,
		}, &api.DefaultMetaOp{}, func(s string, pack *msgstream.MsgPack) {
		})
		assert.Error(t, err)
	})

	t.Run("success", func(t *testing.T) {
		factoryCreator := mocks.NewFactoryCreator(t)
		factory := msgstream.NewMockMqFactory()
		factoryCreator.EXPECT().NewPmsFactory(mock.Anything).Return(factory)
		_, err := NewReplicateChannelManager(config.MQConfig{
			Pulsar: config.PulsarConfig{
				Address: "pulsar://localhost:6650",
			},
		}, factoryCreator, nil, config.ReaderConfig{
			MessageBufferSize: 10,
		}, &api.DefaultMetaOp{}, func(s string, pack *msgstream.MsgPack) {
		})
		assert.NoError(t, err)
	})
}

func TestChannelUtils(t *testing.T) {
	t.Run("GetVChannelByPChannel", func(t *testing.T) {
		assert.Equal(t, "p1_1", GetVChannelByPChannel("p1", []string{"p1_1", "p2_1", "p3_1"}))
		assert.Equal(t, "", GetVChannelByPChannel("p1", []string{"p2_1", "p3_1"}))
	})

	t.Run("ForeachChannel", func(t *testing.T) {
		{
			err := ForeachChannel([]string{"p1"}, []string{}, nil)
			assert.Error(t, err)
		}
		f := func(sourcePChannel, targetPChannel string) error {
			switch sourcePChannel {
			case "p1":
				assert.Equal(t, "p1_1", targetPChannel)
			case "p2":
				assert.Equal(t, "p2_1", targetPChannel)
			case "p3":
				assert.Equal(t, "p3_1", targetPChannel)
			default:
				return errors.New("unexpected pchannel: " + sourcePChannel)
			}
			return nil
		}
		{
			err := ForeachChannel([]string{"p1", "p2", "p3"}, []string{"p1_1", "p2_1", "p3_1"}, f)
			assert.NoError(t, err)
		}
		{
			err := ForeachChannel([]string{"p2", "p1", "p3"}, []string{"p1_1", "p2_1", "p3_1"}, f)
			assert.NoError(t, err)
		}
		{
			err := ForeachChannel([]string{"p3", "p1", "p2"}, []string{"p1_1", "p2_1", "p3_1"}, func(sourcePChannel, targetPChannel string) error {
				if sourcePChannel == "p3" {
					return errors.New("error")
				}
				return nil
			})
			assert.Error(t, err)
		}
	})
}

func TestStartReadCollection(t *testing.T) {
	factoryCreator := mocks.NewFactoryCreator(t)
	factory := msgstream.NewMockFactory(t)
	targetClient := mocks.NewTargetAPI(t)

	factoryCreator.EXPECT().NewPmsFactory(mock.Anything).Return(factory)

	manager, err := NewReplicateChannelManager(config.MQConfig{
		Pulsar: config.PulsarConfig{
			Address: "pulsar://localhost:6650",
		},
	}, factoryCreator, targetClient, config.ReaderConfig{
		MessageBufferSize: 10,
	}, &api.DefaultMetaOp{}, func(s string, pack *msgstream.MsgPack) {
	})
	assert.NoError(t, err)
	manager.SetCtx(context.Background())

	t.Run("context cancel", func(t *testing.T) {
		targetClient.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("error")).Once()
		ctx, cancelFunc := context.WithCancel(context.Background())
		cancelFunc()
		err = manager.StartReadCollection(ctx, &pb.CollectionInfo{}, nil)
		assert.Error(t, err)
	})

	realManager := manager.(*replicateChannelManager)

	t.Run("fail to get target info", func(t *testing.T) {
		go func() {
			event := <-realManager.GetEventChan()
			assert.Equal(t, api.ReplicateCreateCollection, event.EventType)
			assert.Equal(t, "test", event.CollectionInfo.Schema.Name)
			assert.True(t, event.ReplicateInfo.IsReplicate)
		}()
		targetClient.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("mock error")).Twice()
		realManager.retryOptions = []retry.Option{
			retry.Attempts(1),
		}
		err = manager.StartReadCollection(context.Background(), &pb.CollectionInfo{
			Schema: &schemapb.CollectionSchema{
				Name: "test",
			},
		}, nil)
		assert.Error(t, err)
	})

	stream := msgstream.NewMockMsgStream(t)
	streamChan := make(chan *msgstream.MsgPack)

	factory.EXPECT().NewTtMsgStream(mock.Anything).Return(stream, nil).Maybe()
	stream.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	stream.EXPECT().Chan().Return(streamChan).Maybe()
	stream.EXPECT().Close().Return().Maybe()

	t.Run("read channel", func(t *testing.T) {
		{
			// start read
			err := realManager.startReadChannel(&model.SourceCollectionInfo{
				PChannelName: "test_read_channel",
				CollectionID: 11001,
				ShardNum:     1,
			}, &model.TargetCollectionInfo{
				CollectionID:   21001,
				CollectionName: "read_channel",
				PartitionInfo: map[string]int64{
					"_default": 1101,
				},
				PChannel:    "ttest_read_channel",
				VChannel:    "ttest_read_channel_p",
				BarrierChan: make(chan<- uint64),
				PartitionBarrierChan: map[int64]chan<- uint64{
					1101: make(chan<- uint64),
				},
			})
			assert.NoError(t, err)
			assert.Equal(t, "test_read_channel", <-realManager.channelChan)

			err = realManager.startReadChannel(&model.SourceCollectionInfo{
				PChannelName: "test_read_channel",
				CollectionID: 11002,
			}, &model.TargetCollectionInfo{
				CollectionName: "read_channel_2",
			})
			assert.NoError(t, err)
		}
		{
			assert.NotNil(t, realManager.GetMsgChan("test_read_channel"))
			assert.Nil(t, realManager.GetMsgChan("no_exist_channel"))
		}
		{
			// stop read
			realManager.stopReadChannel("no_exist_channel", 11001)
			realManager.stopReadChannel("test_read_channel", 11001)
			realManager.stopReadChannel("test_read_channel", 11002)
		}
	})

	t.Run("collection and partition", func(t *testing.T) {
		// start collection
		{
			targetClient.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything).Return(&model.CollectionInfo{
				CollectionID:   3101,
				CollectionName: "read_collection",
				PChannels:      []string{"collection_partition_p2"},
				VChannels:      []string{"collection_partition_p2_v"},
				Partitions: map[string]int64{
					"_default": 31010,
				},
			}, nil).Twice()
			err := realManager.StartReadCollection(context.Background(), &pb.CollectionInfo{
				ID: 31001,
				Schema: &schemapb.CollectionSchema{
					Name: "test",
				},
				StartPositions: []*commonpb.KeyDataPair{
					{
						Key: "collection_partition_p1",
					},
				},
				PhysicalChannelNames: []string{"collection_partition_p1"},
			}, nil)
			assert.NoError(t, err)
		}

		// partition not found
		{
			realManager.retryOptions = []retry.Option{
				retry.Attempts(1),
			}
			err := realManager.AddPartition(context.Background(), &pb.CollectionInfo{
				ID: 41,
			}, &pb.PartitionInfo{})
			assert.Error(t, err)
		}

		// add partition
		{
			targetClient.EXPECT().GetPartitionInfo(mock.Anything, mock.Anything, mock.Anything).Return(&model.CollectionInfo{
				Partitions: map[string]int64{
					"_default":  31010,
					"_default2": 31020,
				},
			}, nil).Once()
			err := realManager.AddPartition(context.Background(), &pb.CollectionInfo{
				ID: 31001,
				Schema: &schemapb.CollectionSchema{
					Name: "test",
				},
			}, &pb.PartitionInfo{
				PartitionName: "_default2",
			})
			assert.NoError(t, err)
			time.Sleep(100 * time.Millisecond)

			event := <-realManager.GetEventChan()
			assert.Equal(t, api.ReplicateCreatePartition, event.EventType)
		}

		// stop read collection
		{
			err := realManager.StopReadCollection(context.Background(), &pb.CollectionInfo{
				ID: 31001,
				StartPositions: []*commonpb.KeyDataPair{
					{
						Key: "collection_partition_p1",
					},
				},
			})
			assert.NoError(t, err)
		}
	})
}

func TestReplicateChannelHandler(t *testing.T) {
	t.Run("fail to new msg stream", func(t *testing.T) {
		factory := msgstream.NewMockFactory(t)
		factory.EXPECT().NewTtMsgStream(mock.Anything).Return(nil, errors.New("mock error"))

		_, err := newReplicateChannelHandler(context.Background(), &model.SourceCollectionInfo{PChannelName: "test_p"}, (*model.TargetCollectionInfo)(nil), api.TargetAPI(nil), &api.DefaultMetaOp{}, nil, &model.HandlerOpts{Factory: factory})
		assert.Error(t, err)
	})

	t.Run("fail to as consume and seek", func(t *testing.T) {
		factory := msgstream.NewMockFactory(t)
		stream := msgstream.NewMockMsgStream(t)
		factory.EXPECT().NewTtMsgStream(mock.Anything).Return(stream, nil)

		{
			stream.EXPECT().Close().Return().Once()
			stream.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("mock error")).Once()
			_, err := newReplicateChannelHandler(context.Background(), &model.SourceCollectionInfo{PChannelName: "test_p"}, (*model.TargetCollectionInfo)(nil), api.TargetAPI(nil), &api.DefaultMetaOp{}, nil, &model.HandlerOpts{Factory: factory})
			assert.Error(t, err)
		}

		{
			stream.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
			stream.EXPECT().Close().Return().Once()
			stream.EXPECT().Seek(mock.Anything, mock.Anything).Return(errors.New("mock error")).Once()
			_, err := newReplicateChannelHandler(context.Background(), &model.SourceCollectionInfo{PChannelName: "test_p", SeekPosition: &msgstream.MsgPosition{ChannelName: "test_p", MsgID: []byte("test")}}, (*model.TargetCollectionInfo)(nil), api.TargetAPI(nil), &api.DefaultMetaOp{}, nil, &model.HandlerOpts{Factory: factory})
			assert.Error(t, err)
		}
	})

	t.Run("success", func(t *testing.T) {
		factory := msgstream.NewMockMqFactory()
		stream := msgstream.NewMockMsgStream(t)
		factory.NewMsgStreamFunc = func(ctx context.Context) (msgstream.MsgStream, error) {
			return stream, nil
		}

		streamChan := make(chan *msgstream.MsgPack)
		close(streamChan)
		stream.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		stream.EXPECT().Close().Return().Once()
		stream.EXPECT().Seek(mock.Anything, mock.Anything).Return(nil).Once()
		stream.EXPECT().Chan().Return(streamChan).Once()
		handler, err := newReplicateChannelHandler(context.Background(), &model.SourceCollectionInfo{PChannelName: "test_p", SeekPosition: &msgstream.MsgPosition{ChannelName: "test_p", MsgID: []byte("test")}}, &model.TargetCollectionInfo{PChannel: "test_p"}, api.TargetAPI(nil), &api.DefaultMetaOp{}, nil, &model.HandlerOpts{Factory: factory})
		assert.NoError(t, err)
		time.Sleep(100 * time.Microsecond)
		handler.Close()
	})

	t.Run("add and remove collection", func(t *testing.T) {
		factory := msgstream.NewMockFactory(t)
		stream := msgstream.NewMockMsgStream(t)
		targetClient := mocks.NewTargetAPI(t)
		streamChan := make(chan *msgstream.MsgPack)
		close(streamChan)

		factory.EXPECT().NewTtMsgStream(mock.Anything).Return(stream, nil)
		stream.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		stream.EXPECT().Close().Return().Once()
		stream.EXPECT().Seek(mock.Anything, mock.Anything).Return(nil).Once()
		stream.EXPECT().Chan().Return(streamChan).Once().Maybe()

		handler, err := newReplicateChannelHandler(context.Background(), &model.SourceCollectionInfo{
			PChannelName: "test_p",
			SeekPosition: &msgstream.MsgPosition{
				ChannelName: "test_p", MsgID: []byte("test"),
			},
			CollectionID: 15,
		}, &model.TargetCollectionInfo{
			PChannel:       "test_p",
			CollectionName: "foo",
		}, targetClient, &api.DefaultMetaOp{}, nil, &model.HandlerOpts{Factory: factory})
		assert.NoError(t, err)
		time.Sleep(100 * time.Millisecond)
		assert.True(t, handler.containCollection("foo"))
		handler.Close()

		handler.AddCollection(1, &model.TargetCollectionInfo{
			CollectionName: "test",
		})
		handler.RemoveCollection(1)
	})

	t.Run("add and remove partition", func(t *testing.T) {
		factory := msgstream.NewMockFactory(t)
		stream := msgstream.NewMockMsgStream(t)
		targetClient := mocks.NewTargetAPI(t)
		streamChan := make(chan *msgstream.MsgPack)
		close(streamChan)

		factory.EXPECT().NewTtMsgStream(mock.Anything).Return(stream, nil)
		stream.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		stream.EXPECT().Close().Return().Once()
		stream.EXPECT().Seek(mock.Anything, mock.Anything).Return(nil).Once()
		stream.EXPECT().Chan().Return(streamChan).Once()
		targetClient.EXPECT().GetPartitionInfo(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("mock error")).Once()
		targetClient.EXPECT().GetPartitionInfo(mock.Anything, mock.Anything, mock.Anything).Return(&model.CollectionInfo{
			Partitions: map[string]int64{
				"p1": 10001,
				"p2": 10002,
			},
		}, nil).Once()

		apiEventChan := make(chan *api.ReplicateAPIEvent)
		handler, err := func() (*replicateChannelHandler, error) {
			var _ chan<- *api.ReplicateAPIEvent = apiEventChan
			return newReplicateChannelHandler(context.Background(), &model.SourceCollectionInfo{CollectionID: 1, PChannelName: "test_p", SeekPosition: &msgstream.MsgPosition{ChannelName: "test_p", MsgID: []byte("test")}}, &model.TargetCollectionInfo{CollectionID: 100, CollectionName: "test", PChannel: "test_p"}, api.TargetAPI(targetClient), &api.DefaultMetaOp{}, nil, &model.HandlerOpts{Factory: factory})
		}()
		assert.NoError(t, err)
		time.Sleep(100 * time.Millisecond)
		handler.Close()

		go func() {
			time.Sleep(600 * time.Millisecond)
			handler.AddCollection(2, &model.TargetCollectionInfo{
				CollectionName: "test2",
				PartitionBarrierChan: map[int64]chan<- uint64{
					1001: make(chan<- uint64),
				},
			})
		}()
		_ = handler.AddPartitionInfo(&pb.CollectionInfo{
			ID: 2,
			Schema: &schemapb.CollectionSchema{
				Name: "test2",
			},
		}, &pb.PartitionInfo{
			PartitionID:   2001,
			PartitionName: "p2",
		}, make(chan<- uint64))
		time.Sleep(1500 * time.Millisecond)
		handler.RemovePartitionInfo(2, "p2", 10002)

		assert.False(t, handler.IsEmpty())
		assert.NotNil(t, handler.msgPackChan)

		// test updateTargetPartitionInfo
		targetClient.EXPECT().GetPartitionInfo(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("mock error")).Once()
		targetClient.EXPECT().GetPartitionInfo(mock.Anything, mock.Anything, mock.Anything).Return(&model.CollectionInfo{
			Partitions: map[string]int64{
				"p1": 30001,
				"p2": 30002,
			},
		}, nil).Once()
		targetClient.EXPECT().GetPartitionInfo(mock.Anything, mock.Anything, mock.Anything).Return(&model.CollectionInfo{
			Partitions: map[string]int64{
				"p1": 30001,
				"p2": 30002,
			},
		}, nil).Once()
		assert.EqualValues(t, 0, handler.updateTargetPartitionInfo(3, "col3", "p2"))
		assert.EqualValues(t, 0, handler.updateTargetPartitionInfo(3, "col3", "p2"))
		handler.AddCollection(3, &model.TargetCollectionInfo{
			CollectionName: "col3",
		})
		assert.EqualValues(t, 30002, handler.updateTargetPartitionInfo(3, "col3", "p2"))
	})

	t.Run("handle msg pack", func(t *testing.T) {
		factory := msgstream.NewMockFactory(t)
		stream := msgstream.NewMockMsgStream(t)
		targetClient := mocks.NewTargetAPI(t)
		streamChan := make(chan *msgstream.MsgPack)

		factory.EXPECT().NewTtMsgStream(mock.Anything).Return(stream, nil)
		stream.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		stream.EXPECT().Close().Return().Once()
		stream.EXPECT().Seek(mock.Anything, mock.Anything).Return(nil).Once()
		stream.EXPECT().Chan().Return(streamChan)

		barrierChan := make(chan uint64, 1)
		partitionBarrierChan := make(chan uint64, 1)
		handler, err := newReplicateChannelHandler(context.Background(), &model.SourceCollectionInfo{
			CollectionID: 1,
			PChannelName: "test_p_s",
			SeekPosition: &msgstream.MsgPosition{ChannelName: "test_p", MsgID: []byte("test")},
		}, &model.TargetCollectionInfo{
			CollectionID:   100,
			CollectionName: "test",
			PartitionInfo: map[string]int64{
				"p1": 100021,
			},
			PChannel:    "test_p",
			VChannel:    "test_p_1",
			BarrierChan: barrierChan,
			PartitionBarrierChan: map[int64]chan<- uint64{
				1021: partitionBarrierChan,
			},
		}, targetClient, &api.DefaultMetaOp{}, nil, &model.HandlerOpts{Factory: factory})
		assert.NoError(t, err)
		done := make(chan struct{})

		go func() {
			defer close(done)
			{
				// timetick pack
				pack := <-handler.msgPackChan
				// assert pack
				assert.NotNil(t, pack)
				assert.EqualValues(t, 1, pack.BeginTs)
				assert.EqualValues(t, 2, pack.EndTs)
				assert.Len(t, pack.StartPositions, 1)
				assert.Len(t, pack.EndPositions, 1)
				assert.Len(t, pack.Msgs, 1)
			}
			{
				// insert msg
				pack := <-handler.msgPackChan
				assert.Len(t, pack.Msgs, 1)
				insertMsg := pack.Msgs[0].(*msgstream.InsertMsg)
				assert.EqualValues(t, 100, insertMsg.CollectionID)
				assert.EqualValues(t, 100021, insertMsg.PartitionID)
				assert.Equal(t, "test_p_1", insertMsg.ShardName)
			}

			{
				// delete msg
				pack := <-handler.msgPackChan
				assert.Len(t, pack.Msgs, 2)
				{
					deleteMsg := pack.Msgs[0].(*msgstream.DeleteMsg)
					assert.EqualValues(t, 100, deleteMsg.CollectionID)
					assert.EqualValues(t, 0, deleteMsg.PartitionID)
					assert.Equal(t, "test_p_1", deleteMsg.ShardName)
				}
				{
					deleteMsg := pack.Msgs[1].(*msgstream.DeleteMsg)
					assert.EqualValues(t, 100, deleteMsg.CollectionID)
					assert.EqualValues(t, 100021, deleteMsg.PartitionID)
					assert.Equal(t, "test_p_1", deleteMsg.ShardName)
				}
			}

			{
				// drop collection msg
				pack := <-handler.msgPackChan
				assert.Len(t, pack.Msgs, 2)
				dropMsg := pack.Msgs[0].(*msgstream.DropCollectionMsg)
				assert.EqualValues(t, 100, dropMsg.CollectionID)
				assert.EqualValues(t, 2, <-barrierChan)
			}

			{
				// drop partition msg
				pack := <-handler.msgPackChan
				assert.Len(t, pack.Msgs, 1)
				dropMsg := pack.Msgs[0].(*msgstream.DropPartitionMsg)
				assert.EqualValues(t, 100, dropMsg.CollectionID)
				assert.EqualValues(t, 100021, dropMsg.PartitionID)
				assert.EqualValues(t, 2, <-partitionBarrierChan)
			}
		}()

		// create collection msg / create partition msg / timetick msg
		streamChan <- &msgstream.MsgPack{
			BeginTs: 1,
			EndTs:   2,
			StartPositions: []*msgstream.MsgPosition{
				{
					ChannelName: "test_p",
				},
			},
			EndPositions: []*msgstream.MsgPosition{
				{
					ChannelName: "test_p",
				},
			},
			Msgs: []msgstream.TsMsg{
				&msgstream.CreateCollectionMsg{
					BaseMsg: msgstream.BaseMsg{
						BeginTimestamp: 1,
						EndTimestamp:   2,
						HashValues:     []uint32{0},
					},
					CreateCollectionRequest: msgpb.CreateCollectionRequest{
						Base: &commonpb.MsgBase{
							MsgType: commonpb.MsgType_CreateCollection,
						},
					},
				},
				&msgstream.CreatePartitionMsg{
					BaseMsg: msgstream.BaseMsg{
						BeginTimestamp: 1,
						EndTimestamp:   2,
						HashValues:     []uint32{0},
					},
					CreatePartitionRequest: msgpb.CreatePartitionRequest{
						Base: &commonpb.MsgBase{
							MsgType: commonpb.MsgType_CreatePartition,
						},
					},
				},
				&msgstream.TimeTickMsg{
					BaseMsg: msgstream.BaseMsg{
						BeginTimestamp: 1,
						EndTimestamp:   2,
						HashValues:     []uint32{0},
					},
					TimeTickMsg: msgpb.TimeTickMsg{
						Base: &commonpb.MsgBase{
							MsgType: commonpb.MsgType_TimeTick,
						},
					},
				},
				&msgstream.TimeTickMsg{
					BaseMsg: msgstream.BaseMsg{
						BeginTimestamp: 1,
						EndTimestamp:   2,
						HashValues:     []uint32{0},
					},
					TimeTickMsg: msgpb.TimeTickMsg{
						Base: &commonpb.MsgBase{
							MsgType: commonpb.MsgType_ShowCollections,
						},
					},
				},
			},
		}

		// insert msg
		streamChan <- &msgstream.MsgPack{
			BeginTs: 1,
			EndTs:   2,
			StartPositions: []*msgstream.MsgPosition{
				{
					ChannelName: "test_p",
				},
			},
			EndPositions: []*msgstream.MsgPosition{
				{
					ChannelName: "test_p",
				},
			},
			Msgs: []msgstream.TsMsg{
				&msgstream.InsertMsg{
					BaseMsg: msgstream.BaseMsg{
						BeginTimestamp: 1,
						EndTimestamp:   2,
						HashValues:     []uint32{0},
					},
					InsertRequest: msgpb.InsertRequest{
						Base: &commonpb.MsgBase{
							MsgType: commonpb.MsgType_Insert,
						},
						CollectionID:   1,
						CollectionName: "test",
						PartitionID:    1021,
						PartitionName:  "p1",
					},
				},
			},
		}

		// delete msg
		streamChan <- &msgstream.MsgPack{
			BeginTs: 1,
			EndTs:   2,
			StartPositions: []*msgstream.MsgPosition{
				{
					ChannelName: "test_p",
				},
			},
			EndPositions: []*msgstream.MsgPosition{
				{
					ChannelName: "test_p",
				},
			},
			Msgs: []msgstream.TsMsg{
				&msgstream.DeleteMsg{
					BaseMsg: msgstream.BaseMsg{
						BeginTimestamp: 1,
						EndTimestamp:   2,
						HashValues:     []uint32{0},
					},
					DeleteRequest: msgpb.DeleteRequest{
						Base: &commonpb.MsgBase{
							MsgType: commonpb.MsgType_Delete,
						},
						CollectionID:   1,
						CollectionName: "test",
					},
				},
				&msgstream.DeleteMsg{
					BaseMsg: msgstream.BaseMsg{
						BeginTimestamp: 1,
						EndTimestamp:   2,
						HashValues:     []uint32{0},
					},
					DeleteRequest: msgpb.DeleteRequest{
						Base: &commonpb.MsgBase{
							MsgType: commonpb.MsgType_Delete,
						},
						CollectionID:   1,
						CollectionName: "test",
						PartitionID:    1021,
						PartitionName:  "p1",
					},
				},
			},
		}

		// drop collection msg
		streamChan <- &msgstream.MsgPack{
			BeginTs: 1,
			EndTs:   2,
			StartPositions: []*msgstream.MsgPosition{
				{
					ChannelName: "test_p",
				},
			},
			EndPositions: []*msgstream.MsgPosition{
				{
					ChannelName: "test_p",
				},
			},
			Msgs: []msgstream.TsMsg{
				&msgstream.DropCollectionMsg{
					BaseMsg: msgstream.BaseMsg{
						BeginTimestamp: 1,
						EndTimestamp:   2,
						HashValues:     []uint32{0},
					},
					DropCollectionRequest: msgpb.DropCollectionRequest{
						Base: &commonpb.MsgBase{
							MsgType: commonpb.MsgType_DropCollection,
						},
						CollectionID: 1,
					},
				},
			},
		}

		// drop partition msg
		streamChan <- &msgstream.MsgPack{
			BeginTs: 1,
			EndTs:   2,
			StartPositions: []*msgstream.MsgPosition{
				{
					ChannelName: "test_p",
				},
			},
			EndPositions: []*msgstream.MsgPosition{
				{
					ChannelName: "test_p",
				},
			},
			Msgs: []msgstream.TsMsg{
				&msgstream.DropPartitionMsg{
					BaseMsg: msgstream.BaseMsg{
						BeginTimestamp: 1,
						EndTimestamp:   2,
						HashValues:     []uint32{0},
					},
					DropPartitionRequest: msgpb.DropPartitionRequest{
						Base: &commonpb.MsgBase{
							MsgType: commonpb.MsgType_DropPartition,
						},
						CollectionID:  1,
						PartitionID:   1021,
						PartitionName: "p1",
					},
				},
			},
		}

		handler.Close()
		close(streamChan)
		<-done
	})
}
