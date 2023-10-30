package reader

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/zilliztech/milvus-cdc/core/mocks"
	"github.com/zilliztech/milvus-cdc/core/pb"
)

func TestCollectionReader(t *testing.T) {
	etcdOp, err := NewEtcdOp(nil, "", "", "")
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
			CollectionID:  100002,
		}
		_, _ = realOp.etcdClient.Put(context.Background(), realOp.partitionPrefix()+"/100002/200005", getStringForMessage(info))

		{
			info := &pb.PartitionInfo{
				State:         pb.PartitionState_PartitionCreated,
				PartitionName: "foo",
				PartitionID:   300046,
				CollectionID:  100003,
			}
			_, _ = realOp.etcdClient.Put(context.Background(), realOp.partitionPrefix()+"/100003/300046", getStringForMessage(info))
		}
	}

	channelManager := mocks.NewChannelManager(t)
	channelManager.EXPECT().StartReadCollection(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("mock err")).Once()
	channelManager.EXPECT().AddPartition(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	reader, err := NewCollectionReader("reader-1", channelManager, etcdOp, nil, func(ci *pb.CollectionInfo) bool {
		return !strings.Contains(ci.Schema.Name, "test")
	})
	assert.NoError(t, err)
	reader.StartRead(context.Background())
	channelManager.EXPECT().StartReadCollection(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	channelManager.EXPECT().AddPartition(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	// add collection
	{
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

		{
			info := &pb.PartitionInfo{
				State:         pb.PartitionState_PartitionCreated,
				PartitionName: "foo",
				PartitionID:   300047,
				CollectionID:  100003,
			}
			_, _ = realOp.etcdClient.Put(context.Background(), realOp.partitionPrefix()+"/100003/300047", getStringForMessage(info))
		}
	}
	// add partition
	{
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
		info := &pb.PartitionInfo{
			State:         pb.PartitionState_PartitionCreated,
			PartitionName: "foo",
			PartitionID:   200045,
			CollectionID:  100004,
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
				CollectionID:  900004,
			}
			_, _ = realOp.etcdClient.Put(context.Background(), realOp.partitionPrefix()+"/900004/900045", getStringForMessage(info))
		}

		// filter collection
		{
			info := &pb.PartitionInfo{
				State:         pb.PartitionState_PartitionCreated,
				PartitionName: "foo",
				PartitionID:   300045,
				CollectionID:  100003,
			}
			_, _ = realOp.etcdClient.Put(context.Background(), realOp.partitionPrefix()+"/100003/300045", getStringForMessage(info))
		}
	}
	time.Sleep(500 * time.Millisecond)
	channelManager.EXPECT().StopReadCollection(mock.Anything, mock.Anything).Return(errors.New("mock err")).Once()
	channelManager.EXPECT().StopReadCollection(mock.Anything, mock.Anything).Return(nil).Once()
	reader.QuitRead(context.Background())
}
