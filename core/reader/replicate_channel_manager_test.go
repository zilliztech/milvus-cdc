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
	"math"
	"sort"
	"testing"
	"time"

	"github.com/sasha-s/go-deadlock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"

	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/log"
	"github.com/zilliztech/milvus-cdc/core/mocks"
	"github.com/zilliztech/milvus-cdc/core/model"
	"github.com/zilliztech/milvus-cdc/core/pb"
	"github.com/zilliztech/milvus-cdc/core/util"
)

func NewReplicateChannelManagerWithFactory(mqConfig config.MQConfig,
	factoryCreator FactoryCreator,
	client api.TargetAPI,
	readConfig config.ReaderConfig,
	metaOp api.MetaOp,
	msgPackCallback func(string, *msgstream.MsgPack),
	downstream string,
) (api.ChannelManager, error) {
	streamDispatchClient, err := GetMsgDispatcherClient(factoryCreator, mqConfig, false)
	if err != nil {
		log.Warn("fail to get the msg dispatcher client", zap.Error(err))
		return nil, err
	}
	streamFactory, _ := GetStreamFactory(factoryCreator, mqConfig, false)

	return &replicateChannelManager{
		streamDispatchClient: streamDispatchClient,
		streamCreator:        NewDisptachClientStreamCreator(streamFactory, streamDispatchClient),
		targetClient:         client,
		metaOp:               metaOp,
		retryOptions:         util.GetRetryOptions(readConfig.Retry),
		startReadRetryOptions: util.GetRetryOptions(config.RetrySettings{
			RetryTimes:  readConfig.Retry.RetryTimes,
			InitBackOff: readConfig.Retry.InitBackOff,
			MaxBackOff:  readConfig.Retry.InitBackOff,
		}),
		messageBufferSize:       readConfig.MessageBufferSize,
		ttInterval:              readConfig.TTInterval,
		channelMapping:          util.NewChannelMapping(0, 0),
		channelHandlerMap:       make(map[string]*replicateChannelHandler),
		channelForwardMap:       make(map[string]int),
		sourcePChannelKeyMap:    make(map[int64]map[string]string),
		replicateCollections:    make(map[int64]chan struct{}),
		replicatePartitions:     make(map[int64]map[int64]chan struct{}),
		apiEventChan:            make(chan *api.ReplicateAPIEvent, 10),
		forwardReplicateChannel: make(chan string),
		msgPackCallback:         msgPackCallback,
		addCollectionLock:       &deadlock.RWMutex{},
		addCollectionCnt:        new(int),
		downstream:              downstream,
	}, nil
}

func TestNewReplicateChannelManagerForMilvus(t *testing.T) {
	t.Run("empty config", func(t *testing.T) {
		_, err := NewReplicateChannelManagerWithFactory(config.MQConfig{}, NewDefaultFactoryCreator(), nil, config.ReaderConfig{
			MessageBufferSize: 10,
			Retry: config.RetrySettings{
				RetryTimes:  1,
				InitBackOff: 1,
				MaxBackOff:  1,
			},
		}, &api.DefaultMetaOp{}, func(s string, pack *msgstream.MsgPack) {
		},
			"milvus")
		assert.Error(t, err)
	})

	t.Run("success", func(t *testing.T) {
		factoryCreator := mocks.NewFactoryCreator(t)
		factory := msgstream.NewMockMqFactory()
		factoryCreator.EXPECT().NewPmsFactory(mock.Anything).Return(factory)
		_, err := NewReplicateChannelManagerWithFactory(config.MQConfig{
			Pulsar: config.PulsarConfig{
				Address: "pulsar://localhost:6650",
			},
		}, factoryCreator, nil, config.ReaderConfig{
			MessageBufferSize: 10,
			Retry: config.RetrySettings{
				RetryTimes:  1,
				InitBackOff: 1,
				MaxBackOff:  1,
			},
		}, &api.DefaultMetaOp{}, func(s string, pack *msgstream.MsgPack) {
		}, "milvus")
		assert.NoError(t, err)
	})
}

func TestNewReplicateChannelManagerForKafka(t *testing.T) {
	t.Run("empty config", func(t *testing.T) {
		_, err := NewReplicateChannelManagerWithFactory(config.MQConfig{}, NewDefaultFactoryCreator(), nil, config.ReaderConfig{
			MessageBufferSize: 10,
			Retry: config.RetrySettings{
				RetryTimes:  1,
				InitBackOff: 1,
				MaxBackOff:  1,
			},
		}, &api.DefaultMetaOp{}, func(s string, pack *msgstream.MsgPack) {
		}, "kafka")
		assert.Error(t, err)
	})

	t.Run("success", func(t *testing.T) {
		factoryCreator := mocks.NewFactoryCreator(t)
		factory := msgstream.NewMockMqFactory()
		factoryCreator.EXPECT().NewPmsFactory(mock.Anything).Return(factory)
		_, err := NewReplicateChannelManagerWithFactory(config.MQConfig{
			Pulsar: config.PulsarConfig{
				Address: "pulsar://localhost:6650",
			},
		}, factoryCreator, nil, config.ReaderConfig{
			MessageBufferSize: 10,
			Retry: config.RetrySettings{
				RetryTimes:  1,
				InitBackOff: 1,
				MaxBackOff:  1,
			},
		}, &api.DefaultMetaOp{}, func(s string, pack *msgstream.MsgPack) {
		}, "kafka")
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

func TestStartReadCollectionForMilvus(t *testing.T) {
	util.InitMilvusPkgParam(false)

	factoryCreator := mocks.NewFactoryCreator(t)
	factory := msgstream.NewMockFactory(t)
	targetClient := mocks.NewTargetAPI(t)

	factoryCreator.EXPECT().NewPmsFactory(mock.Anything).Return(factory)

	manager, err := NewReplicateChannelManagerWithFactory(config.MQConfig{
		Pulsar: config.PulsarConfig{
			Address: "pulsar://localhost:6650",
		},
	}, factoryCreator, targetClient, config.ReaderConfig{
		MessageBufferSize: 10,
		Retry: config.RetrySettings{
			RetryTimes:  1,
			InitBackOff: 1,
			MaxBackOff:  1,
		},
		ReplicateID: "127.0.0.1:19530",
	}, &api.DefaultMetaOp{}, func(s string, pack *msgstream.MsgPack) {
	}, "milvus")
	assert.NoError(t, err)
	manager.SetCtx(context.Background())

	t.Run("context cancel", func(t *testing.T) {
		ctx, cancelFunc := context.WithCancel(context.Background())
		cancelFunc()
		err = manager.StartReadCollection(ctx, &model.DatabaseInfo{}, &pb.CollectionInfo{}, nil, nil)
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
		targetClient.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("collection not found")).Twice()
		realManager.retryOptions = []retry.Option{
			retry.Attempts(1),
		}
		realManager.startReadRetryOptions = []retry.Option{
			retry.Attempts(1),
		}
		err = manager.StartReadCollection(context.Background(), &model.DatabaseInfo{}, &pb.CollectionInfo{
			Schema: &schemapb.CollectionSchema{
				Name: "test",
			},
		}, nil, nil)
		assert.Error(t, err)
	})

	stream := msgstream.NewMockMsgStream(t)
	streamChan := make(chan *msgstream.ConsumeMsgPack)

	factory.EXPECT().NewMsgStream(mock.Anything).Return(stream, nil).Maybe()
	stream.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	stream.EXPECT().Chan().Return(streamChan).Maybe()
	stream.EXPECT().Close().Return().Maybe()

	t.Run("read channel", func(t *testing.T) {
		{
			// start read
			handler, err := realManager.startReadChannel(context.Background(), &model.SourceCollectionInfo{
				PChannel:     "test_read_channel",
				VChannel:     "test_read_channel_v0",
				CollectionID: 11001,
				ShardNum:     1,
			}, &model.TargetCollectionInfo{
				CollectionID:   21001,
				CollectionName: "read_channel",
				PartitionInfo: map[string]int64{
					"_default": 1101,
				},
				PChannel:    "ttest_read_channel",
				VChannel:    "ttest_read_channel_v0",
				BarrierChan: model.NewOnceWriteChan(make(chan<- *model.BarrierSignal)),
				PartitionBarrierChan: map[int64]*model.OnceWriteChan[*model.BarrierSignal]{
					1101: model.NewOnceWriteChan(make(chan<- *model.BarrierSignal)),
				},
			})
			assert.NoError(t, err)
			handler.startReadChannel()
			assert.Equal(t, "ttest_read_channel", <-realManager.GetChannelChan())

			_, err = realManager.startReadChannel(context.Background(), &model.SourceCollectionInfo{
				PChannel:     "test_read_channel_2",
				VChannel:     "test_read_channel_2_v0",
				CollectionID: 11002,
			}, &model.TargetCollectionInfo{
				CollectionName: "read_channel_2",
				PChannel:       "ttest_read_channel_2",
				VChannel:       "ttest_read_channel_2_v0",
			})
			assert.NoError(t, err)
		}
		{
			assert.NotNil(t, realManager.GetMsgChan("ttest_read_channel"))
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
				PChannels:      []string{"collection-partition-p2"},
				VChannels:      []string{"collection-partition-p2_v"},
				Partitions: map[string]int64{
					"_default": 31010,
				},
			}, nil).Twice()
			err := realManager.StartReadCollection(context.Background(), &model.DatabaseInfo{}, &pb.CollectionInfo{
				ID: 31001,
				Schema: &schemapb.CollectionSchema{
					Name: "test",
				},
				StartPositions: []*commonpb.KeyDataPair{
					{
						Key: "collection_partition_p1",
					},
				},
				PhysicalChannelNames: []string{"collection-partition-p1"},
				VirtualChannelNames:  []string{"collection-partition-p1_v0"},
			}, nil, nil)
			assert.NoError(t, err)
			channel := <-realManager.GetChannelChan()
			assert.Equal(t, "collection-partition-p2", channel)
		}

		// partition not found
		{
			realManager.retryOptions = []retry.Option{
				retry.Attempts(1),
			}
			err := realManager.AddPartition(context.Background(), &model.DatabaseInfo{}, &pb.CollectionInfo{
				ID: 41,
				Schema: &schemapb.CollectionSchema{
					Name: "test",
				},
			}, &pb.PartitionInfo{})
			assert.Error(t, err)
		}

		// add partition
		{
			err := realManager.AddPartition(context.Background(), &model.DatabaseInfo{}, &pb.CollectionInfo{
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

func TestStartReadCollectionForKafka(t *testing.T) {
	util.InitMilvusPkgParam(false)

	factoryCreator := mocks.NewFactoryCreator(t)
	factory := msgstream.NewMockFactory(t)
	factoryCreator.EXPECT().NewPmsFactory(mock.Anything).Return(factory)

	manager, err := NewReplicateChannelManagerWithFactory(config.MQConfig{
		Pulsar: config.PulsarConfig{
			Address: "pulsar://localhost:6650",
		},
	}, factoryCreator, nil, config.ReaderConfig{
		MessageBufferSize: 10,
		Retry: config.RetrySettings{
			RetryTimes:  1,
			InitBackOff: 1,
			MaxBackOff:  1,
		},
		ReplicateID: "127.0.0.1:19530",
	}, &api.DefaultMetaOp{}, func(s string, pack *msgstream.MsgPack) {
	}, "kafka")
	assert.NoError(t, err)
	manager.SetCtx(context.Background())

	t.Run("context cancel", func(t *testing.T) {
		ctx, cancelFunc := context.WithCancel(context.Background())
		cancelFunc()
		err = manager.StartReadCollection(ctx, &model.DatabaseInfo{}, &pb.CollectionInfo{}, nil, nil)
		assert.Error(t, err)
	})

	realManager := manager.(*replicateChannelManager)
	stream := msgstream.NewMockMsgStream(t)
	streamChan := make(chan *msgstream.ConsumeMsgPack)

	factory.EXPECT().NewMsgStream(mock.Anything).Return(stream, nil).Maybe()
	stream.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	stream.EXPECT().Chan().Return(streamChan).Maybe()
	stream.EXPECT().Close().Return().Maybe()

	t.Run("read channel", func(t *testing.T) {
		{
			// start read
			handler, err := realManager.startReadChannel(context.Background(), &model.SourceCollectionInfo{
				PChannel:     "kafka_test_read_channel",
				VChannel:     "kafka_test_read_channel_v0",
				CollectionID: 11001,
				ShardNum:     1,
			}, &model.TargetCollectionInfo{
				CollectionID:   21001,
				CollectionName: "read_channel",
				PartitionInfo: map[string]int64{
					"_default": 1101,
				},
				PChannel:    "kafka_ttest_read_channel",
				VChannel:    "kafka_ttest_read_channel_v0",
				BarrierChan: model.NewOnceWriteChan(make(chan<- *model.BarrierSignal)),
				PartitionBarrierChan: map[int64]*model.OnceWriteChan[*model.BarrierSignal]{
					1101: model.NewOnceWriteChan(make(chan<- *model.BarrierSignal)),
				},
			})
			assert.NoError(t, err)
			handler.startReadChannel()
			assert.Equal(t, "kafka_ttest_read_channel", <-realManager.GetChannelChan())

			_, err = realManager.startReadChannel(context.Background(), &model.SourceCollectionInfo{
				PChannel:     "kafka_test_read_channel_2",
				VChannel:     "kafka_test_read_channel_2_v0",
				CollectionID: 11002,
			}, &model.TargetCollectionInfo{
				CollectionName: "kafka_read_channel_2",
				PChannel:       "kafka_ttest_read_channel_2",
				VChannel:       "kafka_ttest_read_channel_2_v0",
			})
			assert.NoError(t, err)
		}
		{
			assert.NotNil(t, realManager.GetMsgChan("kafka_ttest_read_channel"))
			assert.Nil(t, realManager.GetMsgChan("no_exist_channel"))
		}
		{
			// stop read
			realManager.stopReadChannel("no_exist_channel", 11001)
			realManager.stopReadChannel("kafka_test_read_channel", 11001)
			realManager.stopReadChannel("kafka_test_read_channel", 11002)
		}
	})

	t.Run("collection and partition", func(t *testing.T) {
		// start read collection
		{
			err := realManager.StartReadCollection(context.Background(), &model.DatabaseInfo{}, &pb.CollectionInfo{
				ID: 31001,
				Schema: &schemapb.CollectionSchema{
					Name: "test",
				},
				StartPositions: []*commonpb.KeyDataPair{
					{
						Key: "collection_partition_p1",
					},
				},
				PhysicalChannelNames: []string{"collection-partition-p1"},
				VirtualChannelNames:  []string{"collection-partition-p1_v0"},
			}, nil, nil)
			assert.NoError(t, err)
			event := <-realManager.GetEventChan()
			assert.Equal(t, api.ReplicateCreateCollection, event.EventType)
		}

		// partition not found
		{
			realManager.retryOptions = []retry.Option{
				retry.Attempts(1),
			}
			err := realManager.AddPartition(context.Background(), &model.DatabaseInfo{}, &pb.CollectionInfo{
				ID: 41,
				Schema: &schemapb.CollectionSchema{
					Name: "test",
				},
			}, &pb.PartitionInfo{})
			assert.Error(t, err)
		}

		// add partition
		{
			err := realManager.AddPartition(context.Background(), &model.DatabaseInfo{}, &pb.CollectionInfo{
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

func noRetry(handler *replicateChannelHandler) {
	handler.handlerOpts.RetryOptions = util.GetRetryOptions(config.RetrySettings{
		RetryTimes:  1,
		MaxBackOff:  1,
		InitBackOff: 1,
	})
}

func newReplicateChannelHandler(ctx context.Context,
	sourceInfo *model.SourceCollectionInfo, targetInfo *model.TargetCollectionInfo,
	targetClient api.TargetAPI, metaOp api.MetaOp,
	apiEventChan chan *api.ReplicateAPIEvent, opts *model.HandlerOpts,
) (*replicateChannelHandler, error) {
	creator := &FactoryStreamCreator{
		factory: opts.Factory,
	}

	channelHandler, err := initReplicateChannelHandler(ctx, sourceInfo, targetInfo, targetClient, metaOp, apiEventChan, opts, creator, "milvus", true, "")
	if err == nil {
		channelHandler.addCollectionCnt = new(int)
		channelHandler.addCollectionLock = &deadlock.RWMutex{}
	}
	return channelHandler, err
}

func TestReplicateChannelHandler(t *testing.T) {
	util.InitMilvusPkgParam(false)

	t.Run("fail to new msg stream", func(t *testing.T) {
		factory := msgstream.NewMockFactory(t)
		factory.EXPECT().NewMsgStream(mock.Anything).Return(nil, errors.New("mock error"))

		_, err := newReplicateChannelHandler(context.Background(), &model.SourceCollectionInfo{PChannel: "test_p"}, (*model.TargetCollectionInfo)(nil), api.TargetAPI(nil), &api.DefaultMetaOp{}, nil, &model.HandlerOpts{Factory: factory})
		assert.Error(t, err)
	})

	t.Run("fail to as consume and seek", func(t *testing.T) {
		factory := msgstream.NewMockFactory(t)
		stream := msgstream.NewMockMsgStream(t)
		factory.EXPECT().NewMsgStream(mock.Anything).Return(stream, nil)

		{
			stream.EXPECT().Close().Return().Once()
			stream.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("mock error")).Once()
			_, err := newReplicateChannelHandler(context.Background(), &model.SourceCollectionInfo{PChannel: "test_p"}, (*model.TargetCollectionInfo)(nil), api.TargetAPI(nil), &api.DefaultMetaOp{}, nil, &model.HandlerOpts{Factory: factory})
			assert.Error(t, err)
		}

		{
			stream.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
			stream.EXPECT().Close().Return().Once()
			stream.EXPECT().Seek(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("mock error")).Once()
			_, err := newReplicateChannelHandler(context.Background(), &model.SourceCollectionInfo{PChannel: "test_p", SeekPosition: &msgstream.MsgPosition{ChannelName: "test_p", MsgID: []byte("test")}}, (*model.TargetCollectionInfo)(nil), api.TargetAPI(nil), &api.DefaultMetaOp{}, nil, &model.HandlerOpts{Factory: factory})
			assert.Error(t, err)
		}
	})

	t.Run("success", func(t *testing.T) {
		factory := msgstream.NewMockMqFactory()
		stream := msgstream.NewMockMsgStream(t)
		factory.NewMsgStreamFunc = func(ctx context.Context) (msgstream.MsgStream, error) {
			return stream, nil
		}

		stream.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Twice()
		stream.EXPECT().Close().Return().Maybe()
		stream.EXPECT().Seek(mock.Anything, mock.Anything, mock.Anything).Return(nil).Twice()
		stream.EXPECT().Chan().Return(make(chan *msgstream.ConsumeMsgPack)).Maybe()
		handler, err := newReplicateChannelHandler(context.Background(),
			&model.SourceCollectionInfo{PChannel: "test_p", SeekPosition: &msgstream.MsgPosition{ChannelName: "test_p", MsgID: []byte("test")}},
			&model.TargetCollectionInfo{PChannel: "test_p"}, api.TargetAPI(nil), &api.DefaultMetaOp{}, nil, &model.HandlerOpts{Factory: factory})
		assert.NoError(t, err)
		handler.startReadChannel()
		noRetry(handler)
		time.Sleep(100 * time.Millisecond)
		handler.Close()
	})

	t.Run("add and remove collection", func(t *testing.T) {
		factory := msgstream.NewMockFactory(t)
		stream := msgstream.NewMockMsgStream(t)
		targetClient := mocks.NewTargetAPI(t)

		factory.EXPECT().NewMsgStream(mock.Anything).Return(stream, nil)
		stream.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(3)
		stream.EXPECT().Close().Return().Maybe()
		stream.EXPECT().Seek(mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(2)
		stream.EXPECT().Chan().Return(make(chan *msgstream.ConsumeMsgPack)).Maybe()

		handler, err := newReplicateChannelHandler(context.Background(), &model.SourceCollectionInfo{
			PChannel: "test_p",
			SeekPosition: &msgstream.MsgPosition{
				ChannelName: "test_p", MsgID: []byte("test"),
			},
			CollectionID: 15,
		}, &model.TargetCollectionInfo{
			PChannel:       "test_p",
			CollectionName: "foo",
		}, targetClient, &api.DefaultMetaOp{}, nil, &model.HandlerOpts{Factory: factory})
		assert.NoError(t, err)
		noRetry(handler)
		handler.startReadChannel()
		time.Sleep(100 * time.Millisecond)
		assert.True(t, handler.containCollection("foo"))
		handler.Close()

		handler.AddCollection("", &model.SourceCollectionInfo{
			CollectionID: 1,
		}, &model.TargetCollectionInfo{
			CollectionName: "test",
			PChannel:       "p1",
		})
		handler.RemoveCollection(1)
	})

	t.Run("add and remove partition", func(t *testing.T) {
		factory := msgstream.NewMockFactory(t)
		stream := msgstream.NewMockMsgStream(t)
		targetClient := mocks.NewTargetAPI(t)
		replicateID := "127.0.0.1:19530"

		factory.EXPECT().NewMsgStream(mock.Anything).Return(stream, nil)
		stream.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(4)
		stream.EXPECT().Close().Return().Maybe()
		stream.EXPECT().Seek(mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(2)
		stream.EXPECT().Chan().Return(make(chan *msgstream.ConsumeMsgPack)).Maybe()

		apiEventChan := make(chan *api.ReplicateAPIEvent)
		handler, err := func() (*replicateChannelHandler, error) {
			var _ chan<- *api.ReplicateAPIEvent = apiEventChan
			return newReplicateChannelHandler(context.Background(), &model.SourceCollectionInfo{CollectionID: 1, PChannel: "test_p", SeekPosition: &msgstream.MsgPosition{ChannelName: "test_p", MsgID: []byte("test")}}, &model.TargetCollectionInfo{CollectionID: 100, CollectionName: "test", PChannel: "test_p"}, api.TargetAPI(targetClient), &api.DefaultMetaOp{}, nil, &model.HandlerOpts{Factory: factory})
		}()
		assert.NoError(t, err)
		handler.handlerOpts.RetryOptions = util.GetRetryOptions(config.RetrySettings{
			RetryTimes:  3,
			MaxBackOff:  1,
			InitBackOff: 1,
		})
		handler.isDroppedCollection = func(i int64) bool {
			return false
		}
		handler.isDroppedPartition = func(i int64) bool {
			return false
		}
		handler.replicateID = replicateID
		time.Sleep(100 * time.Millisecond)
		handler.startReadChannel()

		go func() {
			time.Sleep(600 * time.Millisecond)
			handler.AddCollection("", &model.SourceCollectionInfo{
				CollectionID: 2,
			}, &model.TargetCollectionInfo{
				CollectionName: "test2",
				PartitionBarrierChan: map[int64]*model.OnceWriteChan[*model.BarrierSignal]{
					1001: model.NewOnceWriteChan(make(chan<- *model.BarrierSignal)),
				},
				DroppedPartition: make(map[int64]struct{}),
			})
		}()
		err = handler.AddPartitionInfo("", &pb.CollectionInfo{
			ID: 2,
			Schema: &schemapb.CollectionSchema{
				Name: "test2",
			},
		}, &pb.PartitionInfo{
			PartitionID:   2001,
			PartitionName: "p2",
		}, make(chan<- *model.BarrierSignal))
		assert.NoError(t, err)
		time.Sleep(1500 * time.Millisecond)
		handler.RemovePartitionInfo(2, "p2", 10002)

		assert.False(t, handler.IsEmpty())
		assert.NotNil(t, GetTSManager().GetTargetMsgChan(replicateID, handler.targetPChannel))

		// test updateTargetPartitionInfo
		targetClient.EXPECT().GetPartitionInfo(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("mock error 2")).Once()
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
		handler.AddCollection("", &model.SourceCollectionInfo{
			CollectionID: 3,
		}, &model.TargetCollectionInfo{
			CollectionName: "col3",
		})
		assert.EqualValues(t, 30002, handler.updateTargetPartitionInfo(3, "col3", "p2"))
	})

	t.Run("handle msg pack", func(t *testing.T) {
		GetTSManager().EmptyTS()

		factory := msgstream.NewMockFactory(t)
		stream := msgstream.NewMockMsgStream(t)
		targetClient := mocks.NewTargetAPI(t)
		streamChan := make(chan *msgstream.MsgPack)
		consumeStreamChan := make(chan *msgstream.ConsumeMsgPack)
		go func() {
			for {
				msgpack := <-streamChan
				consumeStreamChan <- msgstream.BuildConsumeMsgPack(msgpack)
			}
		}()
		replicateID := "127.0.0.1:19530"

		factory.EXPECT().NewMsgStream(mock.Anything).Return(stream, nil)
		stream.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Twice()
		stream.EXPECT().Close().Return().Maybe()
		stream.EXPECT().Seek(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once().Twice()
		stream.EXPECT().Chan().Return(consumeStreamChan)
		f := &msgstream.ProtoUDFactory{}
		dispatcher := f.NewUnmarshalDispatcher()
		stream.EXPECT().GetUnmarshalDispatcher().Return(dispatcher).Maybe()

		barrierChan := make(chan *model.BarrierSignal, 1)
		partitionBarrierChan := make(chan *model.BarrierSignal, 1)
		apiEventChan := make(chan *api.ReplicateAPIEvent, 10)
		handler, err := newReplicateChannelHandler(context.Background(), &model.SourceCollectionInfo{
			CollectionID: 1,
			PChannel:     "test_p",
			VChannel:     "test_p_v1",
			SeekPosition: &msgstream.MsgPosition{ChannelName: "test_p", MsgID: []byte("test")},
		}, &model.TargetCollectionInfo{
			CollectionID:   100,
			CollectionName: "test",
			PartitionInfo: map[string]int64{
				"p1": 100021,
			},
			PChannel:             "test_q",
			VChannel:             "test_q_v1",
			BarrierChan:          model.NewOnceWriteChan(barrierChan),
			PartitionBarrierChan: map[int64]*model.OnceWriteChan[*model.BarrierSignal]{},
			DroppedPartition:     make(map[int64]struct{}),
		}, targetClient, &api.DefaultMetaOp{}, apiEventChan, &model.HandlerOpts{
			Factory:    factory,
			TTInterval: 10000,
		})
		assert.NoError(t, err)
		handler.replicateID = replicateID
		handler.startReadChannel()

		handler.isDroppedCollection = func(i int64) bool {
			return false
		}
		handler.isDroppedPartition = func(i int64) bool {
			return false
		}
		GetTSManager().InitTSInfo(replicateID, handler.targetPChannel, 100*time.Millisecond, math.MaxUint64, 10)

		err = handler.AddPartitionInfo("", &pb.CollectionInfo{
			ID: 1,
			Schema: &schemapb.CollectionSchema{
				Name: "test",
			},
		}, &pb.PartitionInfo{
			PartitionID:   1021,
			PartitionName: "p1",
			CollectionId:  1,
		}, partitionBarrierChan)
		assert.NoError(t, err)

		noRetry(handler)

		done := make(chan struct{})
		targetMsgChan := GetTSManager().GetTargetMsgChan(replicateID, handler.targetPChannel)

		go func() {
			defer close(done)
			{
				log.Info("receive timetick msg")
				// timetick pack
				replicateMsg := <-targetMsgChan
				pack := replicateMsg.MsgPack
				// assert pack
				assert.NotNil(t, pack)
				assert.EqualValues(t, 1, pack.BeginTs)
				assert.EqualValues(t, 3, pack.EndTs)
				assert.Len(t, pack.StartPositions, 1)
				assert.Len(t, pack.EndPositions, 1)
				assert.Len(t, pack.Msgs, 2)
				_, ok := pack.Msgs[0].(*msgstream.TimeTickMsg)
				assert.True(t, ok, pack.Msgs[0])
				_, ok2 := pack.Msgs[1].(*msgstream.TimeTickMsg)
				assert.True(t, ok2, pack.Msgs[1])
			}
			{
				log.Info("receive insert msg")
				// insert msg
				replicateMsg := <-targetMsgChan
				pack := replicateMsg.MsgPack
				assert.Len(t, pack.Msgs, 2)
				insertMsg := pack.Msgs[0].(*msgstream.InsertMsg)
				assert.EqualValues(t, 100, insertMsg.CollectionID)
				assert.EqualValues(t, 100021, insertMsg.PartitionID)
				assert.Equal(t, "test_q_v1", insertMsg.ShardName)
			}

			{
				log.Info("receive delete msg")
				// delete msg
				replicateMsg := <-targetMsgChan
				pack := replicateMsg.MsgPack
				assert.Len(t, pack.Msgs, 3)
				{
					deleteMsg := pack.Msgs[0].(*msgstream.DeleteMsg)
					assert.EqualValues(t, 100, deleteMsg.CollectionID)
					assert.EqualValues(t, 0, deleteMsg.PartitionID)
					assert.Equal(t, "test_q_v1", deleteMsg.ShardName)
				}
				{
					deleteMsg := pack.Msgs[1].(*msgstream.DeleteMsg)
					assert.EqualValues(t, 100, deleteMsg.CollectionID)
					assert.EqualValues(t, 100021, deleteMsg.PartitionID)
					assert.Equal(t, "test_q_v1", deleteMsg.ShardName)
				}
			}

			{
				log.Info("receive drop partition msg")
				// drop partition msg
				replicateMsg := <-targetMsgChan
				pack := replicateMsg.MsgPack
				assert.Len(t, pack.Msgs, 2)
				dropMsg := pack.Msgs[0].(*msgstream.DropPartitionMsg)
				assert.EqualValues(t, 100, dropMsg.CollectionID)
				assert.EqualValues(t, 100021, dropMsg.PartitionID)
				signal := <-partitionBarrierChan
				assert.EqualValues(t, 12, signal.Msg.EndTs())
			}

			{
				log.Info("receive drop collection msg")
				// drop collection msg
				replicateMsg := <-targetMsgChan
				pack := replicateMsg.MsgPack
				assert.Len(t, pack.Msgs, 2)
				dropMsg := pack.Msgs[0].(*msgstream.DropCollectionMsg)
				assert.EqualValues(t, 100, dropMsg.CollectionID)
				signal := <-barrierChan
				assert.EqualValues(t, 14, signal.Msg.EndTs())
			}
		}()

		// create collection msg / create partition msg / timetick msg
		log.Info("create collection msg / create partition msg / timetick msg")
		streamChan <- &msgstream.MsgPack{
			BeginTs: 1,
			EndTs:   3,
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
						BeginTimestamp: 2,
						EndTimestamp:   2,
						HashValues:     []uint32{0},
					},
					CreateCollectionRequest: &msgpb.CreateCollectionRequest{
						Base: &commonpb.MsgBase{
							MsgType: commonpb.MsgType_CreateCollection,
						},
					},
				},
				&msgstream.CreatePartitionMsg{
					BaseMsg: msgstream.BaseMsg{
						BeginTimestamp: 2,
						EndTimestamp:   2,
						HashValues:     []uint32{0},
					},
					CreatePartitionRequest: &msgpb.CreatePartitionRequest{
						Base: &commonpb.MsgBase{
							MsgType: commonpb.MsgType_CreatePartition,
						},
					},
				},
				&msgstream.TimeTickMsg{
					BaseMsg: msgstream.BaseMsg{
						BeginTimestamp: 2,
						EndTimestamp:   2,
						HashValues:     []uint32{0},
					},
					TimeTickMsg: &msgpb.TimeTickMsg{
						Base: &commonpb.MsgBase{
							MsgType: commonpb.MsgType_TimeTick,
						},
					},
				},
				&msgstream.TimeTickMsg{
					BaseMsg: msgstream.BaseMsg{
						BeginTimestamp: 2,
						EndTimestamp:   2,
						HashValues:     []uint32{0},
					},
					TimeTickMsg: &msgpb.TimeTickMsg{
						Base: &commonpb.MsgBase{
							MsgType: commonpb.MsgType_ShowCollections,
						},
					},
				},
			},
		}

		streamChan <- &msgstream.MsgPack{
			BeginTs: 3,
			EndTs:   5,
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
				&msgstream.TimeTickMsg{
					BaseMsg: msgstream.BaseMsg{
						BeginTimestamp: 4,
						EndTimestamp:   4,
						HashValues:     []uint32{0},
					},
					TimeTickMsg: &msgpb.TimeTickMsg{
						Base: &commonpb.MsgBase{
							MsgType: commonpb.MsgType_TimeTick,
						},
					},
				},
			},
		}

		streamChan <- &msgstream.MsgPack{
			BeginTs: 5,
			EndTs:   7,
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
				&msgstream.TimeTickMsg{
					BaseMsg: msgstream.BaseMsg{
						BeginTimestamp: 6,
						EndTimestamp:   6,
						HashValues:     []uint32{0},
					},
					TimeTickMsg: &msgpb.TimeTickMsg{
						Base: &commonpb.MsgBase{
							MsgType: commonpb.MsgType_ShowCollections,
						},
					},
				},
			},
		}

		// insert msg
		log.Info("insert msg")
		streamChan <- &msgstream.MsgPack{
			BeginTs: 7,
			EndTs:   9,
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
						BeginTimestamp: 8,
						EndTimestamp:   8,
						HashValues:     []uint32{0},
						MsgPosition:    &msgstream.MsgPosition{ChannelName: "test_p"},
					},
					InsertRequest: &msgpb.InsertRequest{
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
		log.Info("delete msg")
		streamChan <- &msgstream.MsgPack{
			BeginTs: 9,
			EndTs:   11,
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
						BeginTimestamp: 10,
						EndTimestamp:   10,
						HashValues:     []uint32{0},
						MsgPosition:    &msgstream.MsgPosition{ChannelName: "test_p"},
					},
					DeleteRequest: &msgpb.DeleteRequest{
						Base: &commonpb.MsgBase{
							MsgType: commonpb.MsgType_Delete,
						},
						CollectionID:   1,
						CollectionName: "test",
					},
				},
				&msgstream.DeleteMsg{
					BaseMsg: msgstream.BaseMsg{
						BeginTimestamp: 11,
						EndTimestamp:   11,
						HashValues:     []uint32{0},
						MsgPosition:    &msgstream.MsgPosition{ChannelName: "test_p"},
					},
					DeleteRequest: &msgpb.DeleteRequest{
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

		// drop partition msg
		log.Info("drop partition msg")
		streamChan <- &msgstream.MsgPack{
			BeginTs: 11,
			EndTs:   13,
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
						BeginTimestamp: 12,
						EndTimestamp:   12,
						HashValues:     []uint32{0},
						MsgPosition:    &msgstream.MsgPosition{ChannelName: "test_p"},
					},
					DropPartitionRequest: &msgpb.DropPartitionRequest{
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

		// drop collection msg
		log.Info("drop collection msg")
		streamChan <- &msgstream.MsgPack{
			BeginTs: 13,
			EndTs:   15,
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
						BeginTimestamp: 14,
						EndTimestamp:   14,
						HashValues:     []uint32{0},
						MsgPosition:    &msgstream.MsgPosition{ChannelName: "test_p"},
					},
					DropCollectionRequest: &msgpb.DropCollectionRequest{
						Base: &commonpb.MsgBase{
							MsgType: commonpb.MsgType_DropCollection,
						},
						CollectionID: 1,
					},
				},
			},
		}

		// close
		log.Info("close")
		handler.Close()
		doneTimer := time.NewTimer(10 * time.Second)
		defer doneTimer.Stop()
		select {
		case <-done:
		case <-doneTimer.C:
			t.FailNow()
		}
	})
}

func TestResetPack(t *testing.T) {
	pack := &msgstream.MsgPack{
		BeginTs: 1,
		EndTs:   20,
		StartPositions: []*msgstream.MsgPosition{
			{
				ChannelName: "test_p",
				Timestamp:   1,
			},
		},
		EndPositions: []*msgstream.MsgPosition{
			{
				ChannelName: "test_p",
				Timestamp:   20,
			},
		},
		Msgs: []msgstream.TsMsg{
			&msgstream.InsertMsg{
				BaseMsg: msgstream.BaseMsg{
					BeginTimestamp: 10,
				},
				InsertRequest: &msgpb.InsertRequest{
					Timestamps: []uint64{10},
				},
			},
			&msgstream.DeleteMsg{
				BaseMsg: msgstream.BaseMsg{
					BeginTimestamp: 11,
				},
				DeleteRequest: &msgpb.DeleteRequest{
					Timestamps: []uint64{11},
				},
			},
			&msgstream.InsertMsg{
				BaseMsg: msgstream.BaseMsg{
					BeginTimestamp: 11,
				},
				InsertRequest: &msgpb.InsertRequest{
					Timestamps: []uint64{11},
				},
			},
			&msgstream.DeleteMsg{
				BaseMsg: msgstream.BaseMsg{
					BeginTimestamp: 20,
				},
				DeleteRequest: &msgpb.DeleteRequest{
					Timestamps: []uint64{20},
				},
			},
		},
	}
	resetMsgPackTimestamp(pack, 100)
	assert.EqualValues(t, 101, pack.BeginTs)
	assert.EqualValues(t, 104, pack.EndTs)
	assert.EqualValues(t, 102, pack.Msgs[1].BeginTs())
	assert.EqualValues(t, 102, pack.Msgs[2].BeginTs())
}

func TestResetPack2(t *testing.T) {
	pack := &msgstream.MsgPack{
		BeginTs: 1,
		EndTs:   20,
		StartPositions: []*msgstream.MsgPosition{
			{
				ChannelName: "test_p",
				Timestamp:   1,
			},
		},
		EndPositions: []*msgstream.MsgPosition{
			{
				ChannelName: "test_p",
				Timestamp:   20,
			},
		},
		Msgs: []msgstream.TsMsg{
			&msgstream.InsertMsg{
				BaseMsg: msgstream.BaseMsg{
					BeginTimestamp: 11,
				},
				InsertRequest: &msgpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Insert,
					},
					Timestamps: []uint64{11},
				},
			},
			&msgstream.DeleteMsg{
				BaseMsg: msgstream.BaseMsg{
					BeginTimestamp: 11,
				},
				DeleteRequest: &msgpb.DeleteRequest{
					Base: &commonpb.MsgBase{
						MsgType: commonpb.MsgType_Delete,
					},
					Timestamps: []uint64{11},
				},
			},
		},
	}
	resetMsgPackTimestamp(pack, 100)
	assert.EqualValues(t, 101, pack.BeginTs)
	assert.EqualValues(t, 101, pack.EndTs)
	assert.EqualValues(t, 101, pack.Msgs[0].BeginTs())
	assert.Equal(t, commonpb.MsgType_Insert, pack.Msgs[0].Type())
	assert.EqualValues(t, 101, pack.Msgs[1].BeginTs())

	sort.Slice(pack.Msgs, func(i, j int) bool {
		return pack.Msgs[i].BeginTs() < pack.Msgs[j].BeginTs() ||
			(pack.Msgs[i].BeginTs() == pack.Msgs[j].BeginTs() && pack.Msgs[i].Type() == commonpb.MsgType_Delete)
	})
	assert.Equal(t, commonpb.MsgType_Delete, pack.Msgs[0].Type())
}
