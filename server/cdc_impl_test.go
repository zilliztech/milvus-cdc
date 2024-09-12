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

package server

import (
	"context"
	"log"
	"net"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	sdkmocks "github.com/milvus-io/milvus-sdk-go/v2/mocks"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/typeutil"

	"github.com/zilliztech/milvus-cdc/core/config"
	coremocks "github.com/zilliztech/milvus-cdc/core/mocks"
	"github.com/zilliztech/milvus-cdc/core/pb"
	cdcreader "github.com/zilliztech/milvus-cdc/core/reader"
	"github.com/zilliztech/milvus-cdc/core/util"
	"github.com/zilliztech/milvus-cdc/server/mocks"
	"github.com/zilliztech/milvus-cdc/server/model"
	"github.com/zilliztech/milvus-cdc/server/model/meta"
	"github.com/zilliztech/milvus-cdc/server/model/request"
)

var (
	endpoints      = []string{"localhost:2379"}
	mysqlURL       = "root:123456@tcp(127.0.0.1:3306)/milvuscdc?charset=utf8"
	rootPath       = "cdc"
	collectionName = "coll"
	pulsarConfig   = &config.PulsarConfig{
		Address:    "pulsar://localhost:6650",
		WebAddress: "localhost:8080",
		Tenant:     "public",
		Namespace:  "default",
	}
	kafkaAddress = "localhost:9092"
)

func createTopic(topic string) error {
	util.InitMilvusPkgParam()
	c := cdcreader.NewDefaultFactoryCreator()
	f := c.NewPmsFactory(pulsarConfig)
	timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	s, err := f.NewMsgStream(timeoutCtx)
	if err != nil {
		return err
	}
	s.AsProducer([]string{topic})
	s.Close()
	return nil
}

func TestNewMetaCDC(t *testing.T) {
	createErr := createTopic("by-dev-replicate-msg")
	assert.NoError(t, createErr)
	t.Run("invalid meta store type", func(t *testing.T) {
		assert.Panics(t, func() {
			NewMetaCDC(&CDCServerConfig{
				MetaStoreConfig: CDCMetaStoreConfig{StoreType: "unknown"},
			})
		})
	})

	t.Run("etcd meta store", func(t *testing.T) {
		// invalid address
		assert.Panics(t, func() {
			NewMetaCDC(&CDCServerConfig{
				MetaStoreConfig: CDCMetaStoreConfig{
					StoreType:     "etcd",
					EtcdEndpoints: []string{"unknown"},
					RootPath:      "cdc-test",
				},
			})
		})

		// empty replicate chan
		assert.Panics(t, func() {
			NewMetaCDC(&CDCServerConfig{
				MetaStoreConfig: CDCMetaStoreConfig{
					StoreType:     "etcd",
					EtcdEndpoints: endpoints,
					RootPath:      "cdc-test",
				},
				SourceConfig: MilvusSourceConfig{},
			})
		})

		// invalid source etcd
		assert.Panics(t, func() {
			NewMetaCDC(&CDCServerConfig{
				MetaStoreConfig: CDCMetaStoreConfig{
					StoreType:     "etcd",
					EtcdEndpoints: endpoints,
					RootPath:      "cdc-test",
				},
				SourceConfig: MilvusSourceConfig{
					ReplicateChan: "by-dev-replicate-msg",
					EtcdAddress:   []string{"unknown"},
				},
			})
		})

		// invalid pulsar address
		{
			NewMetaCDC(&CDCServerConfig{
				MetaStoreConfig: CDCMetaStoreConfig{
					StoreType:     "etcd",
					EtcdEndpoints: endpoints,
					RootPath:      "cdc-test",
				},
				SourceConfig: MilvusSourceConfig{
					ReplicateChan: "by-dev-replicate-msg",
					EtcdAddress:   endpoints,
					Pulsar: config.PulsarConfig{
						Address: "invalid",
					},
				},
			})
		}

		// success
		{
			NewMetaCDC(&CDCServerConfig{
				MetaStoreConfig: CDCMetaStoreConfig{
					StoreType:     "etcd",
					EtcdEndpoints: endpoints,
					RootPath:      "cdc-test",
				},
				SourceConfig: MilvusSourceConfig{
					ReplicateChan: "by-dev-replicate-msg",
					EtcdAddress:   endpoints,
					Pulsar:        *pulsarConfig,
				},
			})
		}
	})

	t.Run("mysql meta store", func(t *testing.T) {
		// invalid address
		{
			assert.Panics(t, func() {
				NewMetaCDC(&CDCServerConfig{
					MetaStoreConfig: CDCMetaStoreConfig{
						StoreType:      "mysql",
						MysqlSourceURL: "unknown",
						RootPath:       "cdc-test",
					},
				})
			})
		}

		// success
		{
			NewMetaCDC(&CDCServerConfig{
				MetaStoreConfig: CDCMetaStoreConfig{
					StoreType:      "mysql",
					MysqlSourceURL: mysqlURL,
					RootPath:       "cdc-test",
				},
				SourceConfig: MilvusSourceConfig{
					EtcdAddress:   endpoints,
					ReplicateChan: "by-dev-replicate-msg",
					Pulsar:        *pulsarConfig,
				},
			})
		}
	})
}

func TestReload(t *testing.T) {
	t.Run("reverser invalid config", func(t *testing.T) {
		metaCDC := &MetaCDC{}
		metaCDC.config = &CDCServerConfig{
			EnableReverse: true,
		}
		assert.Panics(t, func() {
			metaCDC.ReloadTask()
		})
	})

	t.Run("fail to get task meta", func(t *testing.T) {
		metaCDC := &MetaCDC{}
		initMetaCDCMap(metaCDC)
		factory := mocks.NewMetaStoreFactory(t)
		store := mocks.NewMetaStore[*meta.TaskInfo](t)

		metaCDC.config = &CDCServerConfig{
			EnableReverse: false,
		}
		metaCDC.metaStoreFactory = factory
		assert.Panics(t, func() {
			factory.EXPECT().GetTaskInfoMetaStore(mock.Anything).Return(store).Once()
			store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("test")).Once()
			metaCDC.ReloadTask()
		})
	})

	t.Run("success", func(t *testing.T) {
		metaCDC := &MetaCDC{}
		initMetaCDCMap(metaCDC)
		factory := mocks.NewMetaStoreFactory(t)
		store := mocks.NewMetaStore[*meta.TaskInfo](t)
		positionStore := mocks.NewMetaStore[*meta.TaskCollectionPosition](t)

		metaCDC.config = &CDCServerConfig{
			EnableReverse: false,
		}
		metaCDC.metaStoreFactory = factory
		factory.EXPECT().GetTaskInfoMetaStore(mock.Anything).Return(store)
		factory.EXPECT().GetTaskCollectionPositionMetaStore(mock.Anything).Return(positionStore).Once()
		store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskInfo{
			{
				TaskID: "1234",
				State:  meta.TaskStateRunning,
				MilvusConnectParam: model.MilvusConnectParam{
					Host: "127.0.0.1",
					Port: 19530,
				},
				CollectionInfos: []model.CollectionInfo{
					{
						Name: "foo",
					},
				},
			},
		}, nil).Once()
		positionStore.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("test")).Once()

		metaCDC.replicateEntityMap.Lock()
		metaCDC.replicateEntityMap.data = map[string]*ReplicateEntity{
			"http://127.0.0.1:19530": {
				entityQuitFunc: func() {},
				taskQuitFuncs:  typeutil.NewConcurrentMap[string, func()](),
			},
		}
		metaCDC.replicateEntityMap.Unlock()

		// get error when pause task
		store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("test")).Once()

		metaCDC.ReloadTask()
	})
}

func TestValidCreateRequest(t *testing.T) {
	metaCDC := &MetaCDC{
		config: &CDCServerConfig{
			MaxNameLength: 6,
			SourceConfig: MilvusSourceConfig{
				ReplicateChan: "foo",
			},
		},
	}
	t.Run("empty downstream", func(t *testing.T) {
		_, err := metaCDC.Create(&request.CreateRequest{
			MilvusConnectParam: model.MilvusConnectParam{},
			KafkaConnectParam:  model.KafkaConnectParam{},
		})
		assert.Error(t, err)
	})
	t.Run("empty host", func(t *testing.T) {
		_, err := metaCDC.Create(&request.CreateRequest{
			MilvusConnectParam: model.MilvusConnectParam{
				Port: 19530,
			},
		})
		assert.Error(t, err)
	})
	t.Run("empty port", func(t *testing.T) {
		_, err := metaCDC.Create(&request.CreateRequest{
			MilvusConnectParam: model.MilvusConnectParam{
				Host: "localhost",
			},
		})
		assert.Error(t, err)
	})
	t.Run("empty username and default password", func(t *testing.T) {
		_, err := metaCDC.Create(&request.CreateRequest{
			MilvusConnectParam: model.MilvusConnectParam{
				Host:     "localhost",
				Port:     19530,
				Password: "xxx",
			},
		})
		assert.Error(t, err)
	})
	t.Run("invalid connect time", func(t *testing.T) {
		_, err := metaCDC.Create(&request.CreateRequest{
			MilvusConnectParam: model.MilvusConnectParam{
				Host:           "localhost",
				Port:           19530,
				ConnectTimeout: -1,
			},
		})
		assert.Error(t, err)
	})
	t.Run("empty kafka topic", func(t *testing.T) {
		_, err := metaCDC.Create(&request.CreateRequest{
			KafkaConnectParam: model.KafkaConnectParam{
				Address: kafkaAddress,
			},
		})
		assert.Error(t, err)
	})
	t.Run("invalid buffer period", func(t *testing.T) {
		_, err := metaCDC.Create(&request.CreateRequest{
			MilvusConnectParam: model.MilvusConnectParam{
				Host:           "localhost",
				Port:           19530,
				ConnectTimeout: 10,
			},
			BufferConfig: model.BufferConfig{
				Period: -1,
			},
		})
		assert.Error(t, err)
	})
	t.Run("invalid buffer size", func(t *testing.T) {
		_, err := metaCDC.Create(&request.CreateRequest{
			MilvusConnectParam: model.MilvusConnectParam{
				Host:           "localhost",
				Port:           19530,
				ConnectTimeout: 10,
			},
			BufferConfig: model.BufferConfig{
				Period: 10,
				Size:   -1,
			},
		})
		assert.Error(t, err)
	})
	t.Run("empty collection info", func(t *testing.T) {
		_, err := metaCDC.Create(&request.CreateRequest{
			MilvusConnectParam: model.MilvusConnectParam{
				Host:           "localhost",
				Port:           19530,
				ConnectTimeout: 10,
			},
			BufferConfig: model.BufferConfig{
				Period: 10,
				Size:   10,
			},
			CollectionInfos: []model.CollectionInfo{},
		})
		assert.Error(t, err)
	})
	t.Run("not star collection", func(t *testing.T) {
		_, err := metaCDC.Create(&request.CreateRequest{
			MilvusConnectParam: model.MilvusConnectParam{
				Host:           "localhost",
				Port:           19530,
				ConnectTimeout: 10,
			},
			BufferConfig: model.BufferConfig{
				Period: 10,
				Size:   10,
			},
			CollectionInfos: []model.CollectionInfo{
				{
					Name: "foofoofoo",
				},
			},
		})
		assert.Error(t, err)
	})
	t.Run("invalid rpc channel", func(t *testing.T) {
		_, err := metaCDC.Create(&request.CreateRequest{
			MilvusConnectParam: model.MilvusConnectParam{
				Host:           "localhost",
				Port:           19530,
				ConnectTimeout: 10,
			},
			BufferConfig: model.BufferConfig{
				Period: 10,
				Size:   10,
			},
			CollectionInfos: []model.CollectionInfo{
				{
					Name: "*",
				},
			},
			RPCChannelInfo: model.ChannelInfo{
				Name: "noo",
			},
		})
		assert.Error(t, err)
	})
	t.Run("fail to connect target", func(t *testing.T) {
		_, err := metaCDC.Create(&request.CreateRequest{
			MilvusConnectParam: model.MilvusConnectParam{
				Host:           "localhost",
				Port:           19530,
				ConnectTimeout: 3,
			},
			BufferConfig: model.BufferConfig{
				Period: 10,
				Size:   10,
			},
			CollectionInfos: []model.CollectionInfo{
				{
					Name: "*",
				},
			},
			RPCChannelInfo: model.ChannelInfo{
				Name: "foo",
			},
		})
		assert.Error(t, err)
	})
	t.Run("success when milvus", func(t *testing.T) {
		_, closeFunc := NewMockMilvus(t)
		defer closeFunc()

		err := metaCDC.validCreateRequest(&request.CreateRequest{
			MilvusConnectParam: model.MilvusConnectParam{
				Host:           "localhost",
				Port:           50051,
				ConnectTimeout: 5,
			},
			BufferConfig: model.BufferConfig{
				Period: 10,
				Size:   10,
			},
			CollectionInfos: []model.CollectionInfo{
				{
					Name: "*",
				},
			},
			RPCChannelInfo: model.ChannelInfo{
				Name: "foo",
			},
		})
		assert.NoError(t, err)
	})

	t.Run("success when kafka", func(t *testing.T) {
		err := metaCDC.validCreateRequest(&request.CreateRequest{
			KafkaConnectParam: model.KafkaConnectParam{
				Address: kafkaAddress,
				Topic:   "test",
			},
			BufferConfig: model.BufferConfig{
				Period: 10,
				Size:   10,
			},
			CollectionInfos: []model.CollectionInfo{
				{
					Name: "*",
				},
			},
			RPCChannelInfo: model.ChannelInfo{
				Name: "foo",
			},
		})
		assert.NoError(t, err)
	})
}

func TestCreateRequest(t *testing.T) {
	util.InitMilvusPkgParam()
	t.Run("success when milvus", func(t *testing.T) {
		metaCDC := &MetaCDC{
			config: &CDCServerConfig{
				MaxTaskNum: 10,
				SourceConfig: MilvusSourceConfig{
					ReadChanLen: 10,
					Pulsar: config.PulsarConfig{
						Address: "localhost:6650",
					},
					EtcdAddress:          []string{"localhost:2379"},
					EtcdRootPath:         "source-cdc-test",
					EtcdMetaSubPath:      "meta",
					DefaultPartitionName: "_default",
					ReplicateChan:        "foo",
				},
				MaxNameLength: 256,
			},
		}
		initMetaCDCMap(metaCDC)
		defer ClearEtcdData("source-cdc-test")
		PutTS("source-cdc-test")

		factory := mocks.NewMetaStoreFactory(t)
		store := mocks.NewMetaStore[*meta.TaskInfo](t)
		positionStore := mocks.NewMetaStore[*meta.TaskCollectionPosition](t)
		mqFactoryCreator := coremocks.NewFactoryCreator(t)
		mqFactory := msgstream.NewMockFactory(t)
		mq := msgstream.NewMockMsgStream(t)

		metaCDC.mqFactoryCreator = mqFactoryCreator

		factory.EXPECT().GetTaskInfoMetaStore(mock.Anything).Return(store)
		factory.EXPECT().GetTaskCollectionPositionMetaStore(mock.Anything).Return(positionStore)
		// check the task num
		store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskInfo{}, nil).Once()
		// save position
		positionStore.EXPECT().Put(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		// save task meta
		store.EXPECT().Put(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		// new channel manager
		mqFactoryCreator.EXPECT().NewPmsFactory(mock.Anything).Return(mqFactory)
		// get task position
		positionStore.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskCollectionPosition{
			{
				Positions: map[string]*meta.PositionInfo{
					"ch1_v0": {
						Time: 1,
						DataPair: &commonpb.KeyDataPair{
							Key:  "rootcoord-dml-channel_1_123v0",
							Data: []byte("ch1-position"),
						},
					},
				},
			},
		}, nil).Once()
		// new channel reader
		mqFactory.EXPECT().NewMsgStream(mock.Anything).Return(mq, nil).Once()
		mq.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		// start read channel
		streamChan := make(chan *msgstream.MsgPack)
		mq.EXPECT().Chan().Return(streamChan)
		// update state
		store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskInfo{
			{
				TaskID: "1",
				State:  meta.TaskStateInitial,
			},
		}, nil).Once()
		store.EXPECT().Put(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		metaCDC.metaStoreFactory = factory

		_, closeFunc := NewMockMilvus(t)
		defer closeFunc()
		{
			_, err := metaCDC.Create(&request.CreateRequest{
				MilvusConnectParam: model.MilvusConnectParam{
					Host:           "localhost",
					Port:           50051,
					ConnectTimeout: 5,
				},
				BufferConfig: model.BufferConfig{
					Period: 10,
					Size:   10,
				},
				CollectionInfos: []model.CollectionInfo{
					{
						Name: "hello_milvus",
						Positions: map[string]string{
							"rootcoord-dml-channel_1_123v0": util.Base64MsgPosition(&msgstream.MsgPosition{
								ChannelName: "rootcoord-dml-channel_1_123v0",
								MsgID:       []byte("123v0"),
							}),
						},
					},
				},
				RPCChannelInfo: model.ChannelInfo{
					Name: "foo",
				},
			})
			assert.NoError(t, err)
		}
	})

	t.Run("success when kafka", func(t *testing.T) {
		metaCDC := &MetaCDC{
			config: &CDCServerConfig{
				MaxTaskNum: 10,
				SourceConfig: MilvusSourceConfig{
					ReadChanLen: 10,
					Pulsar: config.PulsarConfig{
						Address: "localhost:6650",
					},
					EtcdAddress:          []string{"localhost:2379"},
					EtcdRootPath:         "source-cdc-test",
					EtcdMetaSubPath:      "meta",
					DefaultPartitionName: "_default",
					ReplicateChan:        "foo",
				},
				MaxNameLength: 256,
			},
		}
		initMetaCDCMap(metaCDC)
		defer ClearEtcdData("source-cdc-test")
		PutTS("source-cdc-test")

		factory := mocks.NewMetaStoreFactory(t)
		store := mocks.NewMetaStore[*meta.TaskInfo](t)
		positionStore := mocks.NewMetaStore[*meta.TaskCollectionPosition](t)
		mqFactoryCreator := coremocks.NewFactoryCreator(t)
		mqFactory := msgstream.NewMockFactory(t)
		mq := msgstream.NewMockMsgStream(t)

		metaCDC.mqFactoryCreator = mqFactoryCreator

		factory.EXPECT().GetTaskInfoMetaStore(mock.Anything).Return(store)
		factory.EXPECT().GetTaskCollectionPositionMetaStore(mock.Anything).Return(positionStore)
		// check the task num
		store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskInfo{}, nil).Once()
		// save position
		positionStore.EXPECT().Put(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		// save task meta
		store.EXPECT().Put(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		// new channel manager
		mqFactoryCreator.EXPECT().NewPmsFactory(mock.Anything).Return(mqFactory)
		// get task position
		positionStore.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskCollectionPosition{
			{
				Positions: map[string]*meta.PositionInfo{
					"ch1_v0": {
						Time: 1,
						DataPair: &commonpb.KeyDataPair{
							Key:  "rootcoord-dml-channel_1_123v0",
							Data: []byte("ch1-position"),
						},
					},
				},
			},
		}, nil).Once()
		// new channel reader
		mqFactory.EXPECT().NewMsgStream(mock.Anything).Return(mq, nil).Once()
		mq.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		// start read channel
		streamChan := make(chan *msgstream.MsgPack)
		mq.EXPECT().Chan().Return(streamChan)
		// update state
		store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskInfo{
			{
				TaskID: "1",
				State:  meta.TaskStateInitial,
			},
		}, nil).Once()
		store.EXPECT().Put(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		metaCDC.metaStoreFactory = factory
		{
			_, err := metaCDC.Create(&request.CreateRequest{
				KafkaConnectParam: model.KafkaConnectParam{
					Address: kafkaAddress,
					Topic:   "test",
				},
				BufferConfig: model.BufferConfig{
					Period: 10,
					Size:   10,
				},
				CollectionInfos: []model.CollectionInfo{
					{
						Name: "hello_milvus",
						Positions: map[string]string{
							"rootcoord-dml-channel_1_123v0": util.Base64MsgPosition(&msgstream.MsgPosition{
								ChannelName: "rootcoord-dml-channel_1_123v0",
								MsgID:       []byte("123v0"),
							}),
						},
					},
				},
				RPCChannelInfo: model.ChannelInfo{
					Name: "foo",
				},
			})
			assert.NoError(t, err)
		}
	})
}

func initMetaCDCMap(cdc *MetaCDC) {
	cdc.replicateEntityMap.Lock()
	cdc.replicateEntityMap.data = map[string]*ReplicateEntity{}
	cdc.replicateEntityMap.Unlock()

	cdc.collectionNames.Lock()
	cdc.collectionNames.data = map[string][]string{}
	cdc.collectionNames.excludeData = map[string][]string{}
	cdc.collectionNames.Unlock()

	cdc.cdcTasks.Lock()
	cdc.cdcTasks.data = map[string]*meta.TaskInfo{}
	cdc.cdcTasks.Unlock()
}

func NewMockMilvus(t *testing.T) (*sdkmocks.MilvusServiceServer, func() error) {
	listen, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	server := grpc.NewServer()
	milvusService := sdkmocks.NewMilvusServiceServer(t)
	milvusService.EXPECT().Connect(mock.Anything, mock.Anything).Return(&milvuspb.ConnectResponse{
		Status: &commonpb.Status{},
	}, nil).Maybe()
	milvuspb.RegisterMilvusServiceServer(server, milvusService)

	go func() {
		log.Println("Server started on port 50051")
		if err := server.Serve(listen); err != nil {
			log.Println("server error", err)
		}
	}()
	time.Sleep(time.Second)
	return milvusService, func() error {
		return listen.Close()
	}
}

func ClearEtcdData(rootPath string) {
	etcdCli, _ := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	_, _ = etcdCli.Delete(context.Background(), rootPath, clientv3.WithPrefix())
	_ = etcdCli.Close()
}

func PutTS(rootPath string) {
	etcdCli, _ := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	data := typeutil.Uint64ToBytesBigEndian(uint64(time.Now().UnixNano()))
	_, _ = etcdCli.Put(context.Background(), rootPath+"/kv/gid/timestamp", string(data))
	_ = etcdCli.Close()
}

func TestShouldReadCollection(t *testing.T) {
	t.Run("all collection", func(t *testing.T) {
		f := GetShouldReadFunc(&meta.TaskInfo{
			CollectionInfos: []model.CollectionInfo{
				{
					Name: "*",
				},
			},
			ExcludeCollections: []string{"foo"},
		})
		assert.True(t, f(&pb.CollectionInfo{
			Schema: &schemapb.CollectionSchema{
				Name: "hoo",
			},
		}))
		assert.False(t, f(&pb.CollectionInfo{
			Schema: &schemapb.CollectionSchema{
				Name: "foo",
			},
		}))
	})

	t.Run("some collection", func(t *testing.T) {
		f := GetShouldReadFunc(&meta.TaskInfo{
			CollectionInfos: []model.CollectionInfo{
				{
					Name: "a",
				},
				{
					Name: "b",
				},
			},
			ExcludeCollections: []string{"foo"},
		})
		assert.True(t, f(&pb.CollectionInfo{
			Schema: &schemapb.CollectionSchema{
				Name: "a",
			},
		}))
		assert.False(t, f(&pb.CollectionInfo{
			Schema: &schemapb.CollectionSchema{
				Name: "c",
			},
		}))
		assert.False(t, f(&pb.CollectionInfo{
			Schema: &schemapb.CollectionSchema{
				Name: "foo",
			},
		}))
	})
}

func TestList(t *testing.T) {
	t.Run("err", func(t *testing.T) {
		metaCDC := &MetaCDC{}
		factory := mocks.NewMetaStoreFactory(t)
		store := mocks.NewMetaStore[*meta.TaskInfo](t)
		metaCDC.metaStoreFactory = factory

		factory.EXPECT().GetTaskInfoMetaStore(mock.Anything).Return(store).Once()
		store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("test")).Once()

		_, err := metaCDC.List(&request.ListRequest{})
		assert.Error(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		metaCDC := &MetaCDC{}
		factory := mocks.NewMetaStoreFactory(t)
		store := mocks.NewMetaStore[*meta.TaskInfo](t)
		metaCDC.metaStoreFactory = factory

		factory.EXPECT().GetTaskInfoMetaStore(mock.Anything).Return(store).Once()
		store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskInfo{
			{
				TaskID: "1",
				State:  meta.TaskStateInitial,
			},
		}, nil)

		resp, err := metaCDC.List(&request.ListRequest{})
		assert.NoError(t, err)
		assert.Len(t, resp.Tasks, 1)
		assert.Equal(t, "1", resp.Tasks[0].TaskID)
	})
}

func TestTaskPosition(t *testing.T) {
	t.Run("err", func(t *testing.T) {
		metaCDC := &MetaCDC{}
		factory := mocks.NewMetaStoreFactory(t)
		store := mocks.NewMetaStore[*meta.TaskCollectionPosition](t)
		metaCDC.metaStoreFactory = factory

		factory.EXPECT().GetTaskCollectionPositionMetaStore(mock.Anything).Return(store).Once()
		store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("test")).Once()

		_, err := metaCDC.GetPosition(&request.GetPositionRequest{})
		assert.Error(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		metaCDC := &MetaCDC{}
		factory := mocks.NewMetaStoreFactory(t)
		store := mocks.NewMetaStore[*meta.TaskCollectionPosition](t)
		metaCDC.metaStoreFactory = factory

		factory.EXPECT().GetTaskCollectionPositionMetaStore(mock.Anything).Return(store).Once()
		store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskCollectionPosition{
			{
				TaskID: "1",
				Positions: map[string]*meta.PositionInfo{
					"ch1": {
						Time: 1,
						DataPair: &commonpb.KeyDataPair{
							Key:  "ch1-position",
							Data: []byte("ch1-position"),
						},
					},
				},
				OpPositions: map[string]*meta.PositionInfo{
					"ch1": {
						Time: 1,
						DataPair: &commonpb.KeyDataPair{
							Key:  "ch1",
							Data: []byte("ch2-position"),
						},
					},
				},
				TargetPositions: map[string]*meta.PositionInfo{
					"ch1-tar": {
						Time: 1,
						DataPair: &commonpb.KeyDataPair{
							Key:  "ch1-tar",
							Data: []byte("ch3-position"),
						},
					},
				},
			},
		}, nil)

		resp, err := metaCDC.GetPosition(&request.GetPositionRequest{})
		assert.NoError(t, err)
		assert.Len(t, resp.Positions, 1)
		assert.Equal(t, "ch1", resp.Positions[0].ChannelName)
		assert.EqualValues(t, 1, resp.Positions[0].Time)
		{
			p, err := util.Base64DecodeMsgPosition(resp.Positions[0].MsgID)
			assert.NoError(t, err)
			assert.Equal(t, []byte("ch1-position"), p.MsgID)
		}

		assert.Len(t, resp.OpPositions, 1)
		assert.Equal(t, "ch1", resp.OpPositions[0].ChannelName)
		assert.EqualValues(t, 1, resp.OpPositions[0].Time)
		{
			p, err := util.Base64DecodeMsgPosition(resp.OpPositions[0].MsgID)
			assert.NoError(t, err)
			assert.Equal(t, []byte("ch2-position"), p.MsgID)
		}

		assert.Len(t, resp.TargetPositions, 1)
		assert.Equal(t, "ch1-tar", resp.TargetPositions[0].ChannelName)
		assert.EqualValues(t, 1, resp.TargetPositions[0].Time)
		{
			p, err := util.Base64DecodeMsgPosition(resp.TargetPositions[0].MsgID)
			assert.NoError(t, err)
			assert.Equal(t, []byte("ch3-position"), p.MsgID)
		}
	})
}

func TestGet(t *testing.T) {
	t.Run("empty task", func(t *testing.T) {
		metaCDC := &MetaCDC{}
		_, err := metaCDC.Get(&request.GetRequest{TaskID: ""})
		assert.Error(t, err)
	})

	t.Run("err", func(t *testing.T) {
		metaCDC := &MetaCDC{}
		factory := mocks.NewMetaStoreFactory(t)
		store := mocks.NewMetaStore[*meta.TaskInfo](t)
		metaCDC.metaStoreFactory = factory

		factory.EXPECT().GetTaskInfoMetaStore(mock.Anything).Return(store).Once()
		store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("test")).Once()

		_, err := metaCDC.Get(&request.GetRequest{TaskID: "test"})
		assert.Error(t, err)
	})

	t.Run("not found", func(t *testing.T) {
		metaCDC := &MetaCDC{}
		factory := mocks.NewMetaStoreFactory(t)
		store := mocks.NewMetaStore[*meta.TaskInfo](t)
		metaCDC.metaStoreFactory = factory

		factory.EXPECT().GetTaskInfoMetaStore(mock.Anything).Return(store).Once()
		store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskInfo{}, nil).Once()

		_, err := metaCDC.Get(&request.GetRequest{TaskID: "test"})
		assert.Error(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		metaCDC := &MetaCDC{}
		factory := mocks.NewMetaStoreFactory(t)
		store := mocks.NewMetaStore[*meta.TaskInfo](t)
		metaCDC.metaStoreFactory = factory

		factory.EXPECT().GetTaskInfoMetaStore(mock.Anything).Return(store).Once()
		store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskInfo{
			{
				TaskID: "1",
				State:  meta.TaskStateInitial,
			},
		}, nil)

		resp, err := metaCDC.Get(&request.GetRequest{TaskID: "test"})
		assert.NoError(t, err)
		assert.Equal(t, "1", resp.Task.TaskID)
	})
}

func TestResume(t *testing.T) {
	t.Run("not found task", func(t *testing.T) {
		metaCDC := &MetaCDC{}
		initMetaCDCMap(metaCDC)

		_, err := metaCDC.Resume(&request.ResumeRequest{TaskID: "1"})
		assert.Error(t, err)
	})

	t.Run("fail no mq config", func(t *testing.T) {
		metaCDC := &MetaCDC{
			config: &CDCServerConfig{
				MaxTaskNum: 10,
				SourceConfig: MilvusSourceConfig{
					ReadChanLen:          10,
					EtcdAddress:          []string{"localhost:2379"},
					EtcdRootPath:         "source-cdc-test",
					EtcdMetaSubPath:      "meta",
					DefaultPartitionName: "_default",
				},
			},
		}
		initMetaCDCMap(metaCDC)
		defer ClearEtcdData("source-cdc-test")
		PutTS("source-cdc-test")

		factory := mocks.NewMetaStoreFactory(t)
		store := mocks.NewMetaStore[*meta.TaskInfo](t)
		positionStore := mocks.NewMetaStore[*meta.TaskCollectionPosition](t)
		mqFactoryCreator := coremocks.NewFactoryCreator(t)

		metaCDC.mqFactoryCreator = mqFactoryCreator
		metaCDC.metaStoreFactory = factory
		_, closeFunc := NewMockMilvus(t)
		defer closeFunc()

		factory.EXPECT().GetTaskInfoMetaStore(mock.Anything).Return(store).Maybe()
		factory.EXPECT().GetTaskCollectionPositionMetaStore(mock.Anything).Return(positionStore).Maybe()
		{
			metaCDC.cdcTasks.Lock()
			metaCDC.cdcTasks.data["1"] = &meta.TaskInfo{
				TaskID: "1",
				State:  meta.TaskStatePaused,
				MilvusConnectParam: model.MilvusConnectParam{
					Host:           "localhost",
					Port:           50051,
					ConnectTimeout: 5,
				},
				CollectionInfos: []model.CollectionInfo{
					{
						Name: "*",
					},
				},
				RPCRequestChannelInfo: model.ChannelInfo{
					Name: "foo",
				},
				WriterCacheConfig: model.BufferConfig{
					Period: 10,
					Size:   10,
				},
			}
			metaCDC.cdcTasks.Unlock()
			_, err := metaCDC.Resume(&request.ResumeRequest{TaskID: "1"})
			assert.Error(t, err)
		}
	})

	t.Run("success", func(t *testing.T) {
		metaCDC := &MetaCDC{
			config: &CDCServerConfig{
				MaxTaskNum: 10,
				SourceConfig: MilvusSourceConfig{
					ReadChanLen: 10,
					Pulsar: config.PulsarConfig{
						Address: "localhost:6650",
					},
					EtcdAddress:          []string{"localhost:2379"},
					EtcdRootPath:         "source-cdc-test",
					EtcdMetaSubPath:      "meta",
					DefaultPartitionName: "_default",
				},
			},
		}
		initMetaCDCMap(metaCDC)
		defer ClearEtcdData("source-cdc-test")
		PutTS("source-cdc-test")

		factory := mocks.NewMetaStoreFactory(t)
		store := mocks.NewMetaStore[*meta.TaskInfo](t)
		positionStore := mocks.NewMetaStore[*meta.TaskCollectionPosition](t)
		mqFactoryCreator := coremocks.NewFactoryCreator(t)
		mqFactory := msgstream.NewMockFactory(t)
		mq := msgstream.NewMockMsgStream(t)

		metaCDC.mqFactoryCreator = mqFactoryCreator
		metaCDC.metaStoreFactory = factory
		_, closeFunc := NewMockMilvus(t)
		defer closeFunc()

		factory.EXPECT().GetTaskInfoMetaStore(mock.Anything).Return(store).Maybe()
		factory.EXPECT().GetTaskCollectionPositionMetaStore(mock.Anything).Return(positionStore).Maybe()
		// // check the task num
		// store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskInfo{}, nil).Once()
		// // save position
		// positionStore.EXPECT().Put(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		// // save task meta
		// store.EXPECT().Put(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		// new channel manager
		mqFactoryCreator.EXPECT().NewPmsFactory(mock.Anything).Return(mqFactory)
		// get task position
		positionStore.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskCollectionPosition{
			{
				Positions: map[string]*meta.PositionInfo{
					"ch1": {
						Time: 1,
						DataPair: &commonpb.KeyDataPair{
							Key:  "ch1",
							Data: []byte("ch1-position"),
						},
					},
				},
			},
		}, nil).Once()
		// new channel reader
		mqFactory.EXPECT().NewMsgStream(mock.Anything).Return(mq, nil).Once()
		mq.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		// start read channel
		streamChan := make(chan *msgstream.MsgPack)
		mq.EXPECT().Chan().Return(streamChan)
		// update state
		store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskInfo{
			{
				TaskID: "1",
				State:  meta.TaskStateInitial,
			},
		}, nil).Once()
		store.EXPECT().Put(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		{
			metaCDC.cdcTasks.Lock()
			metaCDC.cdcTasks.data["1"] = &meta.TaskInfo{
				TaskID: "1",
				State:  meta.TaskStatePaused,
				MilvusConnectParam: model.MilvusConnectParam{
					Host:           "localhost",
					Port:           50051,
					ConnectTimeout: 5,
				},
				CollectionInfos: []model.CollectionInfo{
					{
						Name: "*",
					},
				},
				RPCRequestChannelInfo: model.ChannelInfo{
					Name: "foo",
				},
				WriterCacheConfig: model.BufferConfig{
					Period: 10,
					Size:   10,
				},
			}
			metaCDC.cdcTasks.Unlock()
			_, err := metaCDC.Resume(&request.ResumeRequest{TaskID: "1"})
			assert.NoError(t, err)
		}
	})
}

func TestPause(t *testing.T) {
	t.Run("not found task", func(t *testing.T) {
		metaCDC := &MetaCDC{}
		initMetaCDCMap(metaCDC)

		_, err := metaCDC.Pause(&request.PauseRequest{TaskID: "1"})
		assert.Error(t, err)
	})

	t.Run("fail to update state", func(t *testing.T) {
		metaCDC := &MetaCDC{}
		factory := mocks.NewMetaStoreFactory(t)
		store := mocks.NewMetaStore[*meta.TaskInfo](t)
		metaCDC.metaStoreFactory = factory

		initMetaCDCMap(metaCDC)
		metaCDC.cdcTasks.Lock()
		metaCDC.cdcTasks.data["1"] = &meta.TaskInfo{
			MilvusConnectParam: model.MilvusConnectParam{
				Host: "127.0.0.1",
				Port: 6666,
			},
		}
		metaCDC.cdcTasks.Unlock()

		factory.EXPECT().GetTaskInfoMetaStore(mock.Anything).Return(store).Once()
		store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskInfo{}, nil)

		_, err := metaCDC.Pause(&request.PauseRequest{TaskID: "1"})
		assert.Error(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		metaCDC := &MetaCDC{}
		factory := mocks.NewMetaStoreFactory(t)
		store := mocks.NewMetaStore[*meta.TaskInfo](t)
		metaCDC.metaStoreFactory = factory

		initMetaCDCMap(metaCDC)
		metaCDC.cdcTasks.Lock()
		metaCDC.cdcTasks.data["1"] = &meta.TaskInfo{
			MilvusConnectParam: model.MilvusConnectParam{
				Host: "127.0.0.1",
				Port: 6666,
			},
		}
		metaCDC.cdcTasks.Unlock()

		factory.EXPECT().GetTaskInfoMetaStore(mock.Anything).Return(store).Once()
		store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskInfo{
			{
				TaskID: "1",
				State:  meta.TaskStateRunning,
			},
		}, nil)
		store.EXPECT().Put(mock.Anything, mock.Anything, mock.Anything).Return(nil)

		_, err := metaCDC.Pause(&request.PauseRequest{TaskID: "1"})
		assert.NoError(t, err)
	})
}

func TestDelete(t *testing.T) {
	t.Run("not found task", func(t *testing.T) {
		metaCDC := &MetaCDC{}
		initMetaCDCMap(metaCDC)

		_, err := metaCDC.Delete(&request.DeleteRequest{TaskID: "1"})
		assert.Error(t, err)
	})

	t.Run("fail to delete task", func(t *testing.T) {
		metaCDC := &MetaCDC{}
		factory := mocks.NewMetaStoreFactory(t)
		metaStore := mocks.NewMetaStore[*meta.TaskInfo](t)
		positionStore := mocks.NewMetaStore[*meta.TaskCollectionPosition](t)
		factory.EXPECT().GetTaskInfoMetaStore(mock.Anything).Return(metaStore).Maybe()
		factory.EXPECT().GetTaskCollectionPositionMetaStore(mock.Anything).Return(positionStore).Maybe()
		metaCDC.metaStoreFactory = factory

		initMetaCDCMap(metaCDC)
		metaCDC.cdcTasks.Lock()
		metaCDC.cdcTasks.data["1"] = &meta.TaskInfo{
			MilvusConnectParam: model.MilvusConnectParam{
				Host: "127.0.0.1",
				Port: 6666,
			},
		}
		metaCDC.cdcTasks.Unlock()

		metaStore.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("fail")).Once()
		_, err := metaCDC.Delete(&request.DeleteRequest{TaskID: "1"})
		assert.Error(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		metaCDC := &MetaCDC{}
		factory := mocks.NewMetaStoreFactory(t)
		metaStore := mocks.NewMetaStore[*meta.TaskInfo](t)
		positionStore := mocks.NewMetaStore[*meta.TaskCollectionPosition](t)
		factory.EXPECT().GetTaskInfoMetaStore(mock.Anything).Return(metaStore).Maybe()
		factory.EXPECT().GetTaskCollectionPositionMetaStore(mock.Anything).Return(positionStore).Maybe()
		metaCDC.metaStoreFactory = factory

		initMetaCDCMap(metaCDC)
		metaCDC.cdcTasks.Lock()
		metaCDC.cdcTasks.data["1"] = &meta.TaskInfo{
			MilvusConnectParam: model.MilvusConnectParam{
				Host: "127.0.0.1",
				Port: 6666,
			},
		}
		metaCDC.cdcTasks.Unlock()
		metaCDC.replicateEntityMap.Lock()
		metaCDC.replicateEntityMap.data["http://127.0.0.1:6666"] = &ReplicateEntity{
			entityQuitFunc: func() {},
			taskQuitFuncs:  typeutil.NewConcurrentMap[string, func()](),
		}
		metaCDC.replicateEntityMap.Unlock()

		metaStore.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskInfo{
			{
				TaskID: "1",
				MilvusConnectParam: model.MilvusConnectParam{
					Host: "127.0.0.1",
					Port: 6666,
				},
				CollectionInfos: []model.CollectionInfo{
					{Name: "*"},
				},
			},
		}, nil).Once()
		factory.EXPECT().Txn(mock.Anything).Return(nil, func(err error) error {
			return nil
		}, nil).Once()
		metaStore.EXPECT().Delete(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		positionStore.EXPECT().Delete(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		_, err := metaCDC.Delete(&request.DeleteRequest{TaskID: "1"})
		assert.NoError(t, err)
	})
}

func TestIsRunningTask(t *testing.T) {
	m := &MetaCDC{}
	initMetaCDCMap(m)

	assert.False(t, m.isRunningTask("task1"))

	m.cdcTasks.Lock()
	m.cdcTasks.data["task2"] = &meta.TaskInfo{
		State: meta.TaskStateRunning,
	}
	m.cdcTasks.data["task3"] = &meta.TaskInfo{
		State: meta.TaskStatePaused,
	}
	m.cdcTasks.Unlock()
	assert.True(t, m.isRunningTask("task2"))
	assert.False(t, m.isRunningTask("task3"))
}

func TestPauseTask(t *testing.T) {
	m := &MetaCDC{}
	factory := mocks.NewMetaStoreFactory(t)
	store := mocks.NewMetaStore[*meta.TaskInfo](t)

	initMetaCDCMap(m)
	m.metaStoreFactory = factory
	factory.EXPECT().GetTaskInfoMetaStore(mock.Anything).Return(store)

	t.Run("fail to update state", func(t *testing.T) {
		store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("fail")).Once()
		err := m.pauseTaskWithReason("task1", "foo", []meta.TaskState{})
		assert.Error(t, err)
	})

	t.Run("not found task", func(t *testing.T) {
		store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskInfo{
			{
				TaskID: "task1",
				State:  meta.TaskStateRunning,
			},
		}, nil).Once()
		store.EXPECT().Put(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		err := m.pauseTaskWithReason("task1", "foo", []meta.TaskState{})
		assert.NoError(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskInfo{
			{
				TaskID: "task1",
				State:  meta.TaskStateRunning,
			},
		}, nil).Once()
		store.EXPECT().Put(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		m.cdcTasks.Lock()
		m.cdcTasks.data["task1"] = &meta.TaskInfo{
			MilvusConnectParam: model.MilvusConnectParam{
				Host: "127.0.0.1",
				Port: 19530,
			},
		}
		m.cdcTasks.Unlock()
		var isQuit util.Value[bool]
		isQuit.Store(false)
		m.replicateEntityMap.Lock()
		cm := typeutil.NewConcurrentMap[string, func()]()
		cm.Insert("task1", func() {
			isQuit.Store(true)
		})
		m.replicateEntityMap.data["http://127.0.0.1:19530"] = &ReplicateEntity{
			entityQuitFunc: func() {},
			taskQuitFuncs:  cm,
		}
		m.replicateEntityMap.Unlock()
		err := m.pauseTaskWithReason("task1", "foo", []meta.TaskState{})
		assert.NoError(t, err)
		assert.True(t, isQuit.Load())
	})
}
