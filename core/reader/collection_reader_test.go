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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"

	api2 "github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/mocks"
	"github.com/zilliztech/milvus-cdc/core/model"
	"github.com/zilliztech/milvus-cdc/core/pb"
)

// Before running this case, should start the etcd server
func TestCollectionReader(t *testing.T) {
	etcdOp, err := NewEtcdOpWithAddress(nil, "", "", "", config.EtcdRetryConfig{
		Retry: config.RetrySettings{
			RetryTimes:  1,
			InitBackOff: 1,
			MaxBackOff:  1,
		},
	}, &api2.DefaultTargetAPI{})
	assert.NoError(t, err)
	realOp := etcdOp.(*EtcdOp)

	realOp.rootPath = TestCasePrefix
	defer func() {
		_, _ = realOp.etcdClient.Delete(context.Background(), realOp.rootPath, clientv3.WithPrefix())
	}()

	// put collection and partition info
	{
		// collection info
		field1 := &schemapb.FieldSchema{
			FieldID: 1,
			Name:    "ID",
		}
		field2 := &schemapb.FieldSchema{
			FieldID: 2,
			Name:    common.MetaFieldName,
		}
		field3 := &schemapb.FieldSchema{
			FieldID: 100,
			Name:    "age",
		}
		_, _ = realOp.etcdClient.Put(context.Background(), realOp.fieldPrefix()+"/100002/1", getStringForMessage(field1))
		_, _ = realOp.etcdClient.Put(context.Background(), realOp.fieldPrefix()+"/100002/2", getStringForMessage(field2))
		_, _ = realOp.etcdClient.Put(context.Background(), realOp.fieldPrefix()+"/100002/100", getStringForMessage(field3))

		collectionInfo := &pb.CollectionInfo{
			ID:    100002,
			State: pb.CollectionState_CollectionCreated,
			Schema: &schemapb.CollectionSchema{
				Name: "foo",
			},
		}
		collectionBytes, _ := proto.Marshal(collectionInfo)
		_, _ = realOp.etcdClient.Put(context.Background(), realOp.collectionPrefix()+"/1/100002", string(collectionBytes))

		// partition info
		info := &pb.PartitionInfo{
			State:         pb.PartitionState_PartitionCreated,
			PartitionName: "foo",
			PartitionID:   200005,
			CollectionId:  100002,
		}
		_, _ = realOp.etcdClient.Put(context.Background(), realOp.partitionPrefix()+"/100002/200005", getStringForMessage(info))

		{
			info := &pb.PartitionInfo{
				State:         pb.PartitionState_PartitionCreated,
				PartitionName: "foo",
				PartitionID:   300046,
				CollectionId:  100003,
			}
			_, _ = realOp.etcdClient.Put(context.Background(), realOp.partitionPrefix()+"/100003/300046", getStringForMessage(info))
		}
	}

	// put database
	{
		databaseInfo := &pb.DatabaseInfo{
			Id:   1,
			Name: "default",
		}
		databaseBytes, _ := proto.Marshal(databaseInfo)
		_, _ = realOp.etcdClient.Put(context.Background(), realOp.databasePrefix()+"/1", string(databaseBytes))
	}

	channelManager := mocks.NewChannelManager(t)
	// existed collection and partition
	channelManager.EXPECT().StartReadCollection(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("mock err")).Once()
	channelManager.EXPECT().AddPartition(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	channelManager.EXPECT().AddDroppedCollection(mock.Anything).Return().Once()

	reader, err := NewCollectionReader("reader-1", channelManager, etcdOp, nil, nil, func(_ *model.DatabaseInfo, ci *pb.CollectionInfo) (bool, bool) {
		return false, !strings.Contains(ci.Schema.Name, "test")
	}, config.ReaderConfig{
		Retry: config.RetrySettings{
			RetryTimes:  1,
			InitBackOff: 1,
			MaxBackOff:  1,
		},
	})
	assert.NoError(t, err)
	go func() {
		select {
		case <-time.After(time.Second):
			t.Fail()
		case err := <-reader.ErrorChan():
			assert.Error(t, err)
		}
	}()
	reader.StartRead(context.Background())
	// put collection and partition
	channelManager.EXPECT().StartReadCollection(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	channelManager.EXPECT().AddPartition(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	{
		// filter collection
		field3 := &schemapb.FieldSchema{
			FieldID: 100,
			Name:    "age",
		}
		_, _ = realOp.etcdClient.Put(context.Background(), realOp.fieldPrefix()+"/100003/100", getStringForMessage(field3))
		collectionInfo := &pb.CollectionInfo{
			ID:    100003,
			State: pb.CollectionState_CollectionCreated,
			Schema: &schemapb.CollectionSchema{
				Name: "test123",
			},
		}
		collectionBytes, _ := proto.Marshal(collectionInfo)
		_, _ = realOp.etcdClient.Put(context.Background(), realOp.collectionPrefix()+"/1/100003", string(collectionBytes))

		// filter partition
		{
			info := &pb.PartitionInfo{
				State:         pb.PartitionState_PartitionCreated,
				PartitionName: "foo",
				PartitionID:   300047,
				CollectionId:  100003,
			}
			_, _ = realOp.etcdClient.Put(context.Background(), realOp.partitionPrefix()+"/100003/300047", getStringForMessage(info))
		}
	}

	{
		// put collection
		field3 := &schemapb.FieldSchema{
			FieldID: 100,
			Name:    "age",
		}
		_, _ = realOp.etcdClient.Put(context.Background(), realOp.fieldPrefix()+"/100004/100", getStringForMessage(field3))
		collectionInfo := &pb.CollectionInfo{
			ID:    100004,
			State: pb.CollectionState_CollectionCreated,
			Schema: &schemapb.CollectionSchema{
				Name: "hoo",
			},
			StartPositions: []*commonpb.KeyDataPair{
				{
					Key:  "hoo",
					Data: []byte{1, 2, 3},
				},
			},
		}
		collectionBytes, _ := proto.Marshal(collectionInfo)
		_, _ = realOp.etcdClient.Put(context.Background(), realOp.collectionPrefix()+"/1/100004", string(collectionBytes))

		// put partition
		info := &pb.PartitionInfo{
			State:         pb.PartitionState_PartitionCreated,
			PartitionName: "foo",
			PartitionID:   200045,
			CollectionId:  100004,
		}
		_, _ = realOp.etcdClient.Put(context.Background(), realOp.partitionPrefix()+"/100004/200045", getStringForMessage(info))
	}
	// add invalid partition
	{
		// not found collection
		{
			info := &pb.PartitionInfo{
				State:         pb.PartitionState_PartitionCreated,
				PartitionName: "foo",
				PartitionID:   900045,
				CollectionId:  900004,
			}
			_, _ = realOp.etcdClient.Put(context.Background(), realOp.partitionPrefix()+"/900004/900045", getStringForMessage(info))
		}

		// filter collection
		{
			info := &pb.PartitionInfo{
				State:         pb.PartitionState_PartitionCreated,
				PartitionName: "foo",
				PartitionID:   300045,
				CollectionId:  100003,
			}
			_, _ = realOp.etcdClient.Put(context.Background(), realOp.partitionPrefix()+"/100003/300045", getStringForMessage(info))
		}
	}
	time.Sleep(500 * time.Millisecond)
	channelManager.EXPECT().StopReadCollection(mock.Anything, mock.Anything).Return(errors.New("mock err")).Once()
	channelManager.EXPECT().StopReadCollection(mock.Anything, mock.Anything).Return(nil).Once()
	reader.QuitRead(context.Background())
}
