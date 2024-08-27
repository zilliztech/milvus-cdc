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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/retry"

	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/pb"
	"github.com/zilliztech/milvus-cdc/core/util"
)

// HINT: Before running these cases, you should start the etcd server
const TestCasePrefix = "test_case/cdc"

func TestEtcdOp(t *testing.T) {
	etcdOp, err := NewEtcdOpWithAddress(nil, "", "", "", config.EtcdRetryConfig{
		Retry: config.RetrySettings{
			RetryTimes:  1,
			InitBackOff: 1,
			MaxBackOff:  1,
		},
	}, &api.DefaultTargetAPI{})
	assert.NoError(t, err)
	realOp := etcdOp.(*EtcdOp)

	realOp.rootPath = TestCasePrefix
	realOp.retryOptions = []retry.Option{
		retry.Attempts(1),
	}
	defer func() {
		_, _ = realOp.etcdClient.Delete(context.Background(), realOp.rootPath, clientv3.WithPrefix())
	}()

	assert.Equal(t, fmt.Sprintf("%s/%s/%s", TestCasePrefix, realOp.metaSubPath, collectionPrefix), realOp.collectionPrefix())
	assert.Equal(t, fmt.Sprintf("%s/%s/%s", TestCasePrefix, realOp.metaSubPath, partitionPrefix), realOp.partitionPrefix())
	assert.Equal(t, fmt.Sprintf("%s/%s/%s", TestCasePrefix, realOp.metaSubPath, fieldPrefix), realOp.fieldPrefix())
	assert.Equal(t, fmt.Sprintf("%s/%s/%s", TestCasePrefix, realOp.metaSubPath, databasePrefix), realOp.databasePrefix())

	{
		collectionName := etcdOp.GetCollectionNameByID(context.Background(), 900000)
		assert.Equal(t, "", collectionName)
	}

	etcdOp.StartWatch()
	// watch collection
	{
		var success util.Value[bool]
		success.Store(false)
		etcdOp.WatchCollection(context.Background(), func(ci *pb.CollectionInfo) bool {
			return strings.Contains(ci.Schema.Name, "test")
		})
		etcdOp.SubscribeCollectionEvent("empty_event", func(ci *pb.CollectionInfo) bool {
			return false
		})
		etcdOp.SubscribeCollectionEvent("collection_event_123", func(ci *pb.CollectionInfo) bool {
			success.Store(true)
			return true
		})

		// put database
		{
			databaseInfo := &pb.DatabaseInfo{
				Id:   1,
				Name: "default",
			}
			databaseBytes, _ := proto.Marshal(databaseInfo)
			_, _ = realOp.etcdClient.Put(context.Background(), realOp.databasePrefix()+"/1", string(databaseBytes))
		}

		// "not created collection"
		{
			collectionInfo := &pb.CollectionInfo{
				ID:    100008,
				State: pb.CollectionState_CollectionCreating,
			}
			collectionBytes, _ := proto.Marshal(collectionInfo)
			_, _ = realOp.etcdClient.Put(context.Background(), realOp.collectionPrefix()+"/1/100008", string(collectionBytes))
		}

		// invalid data
		{
			_, _ = realOp.etcdClient.Put(context.Background(), realOp.collectionPrefix()+"/1/100009", "hoo")
		}

		// filted collection
		{
			collectionInfo := &pb.CollectionInfo{
				State: pb.CollectionState_CollectionCreated,
				Schema: &schemapb.CollectionSchema{
					Name: "test123",
				},
			}
			collectionBytes, _ := proto.Marshal(collectionInfo)
			_, _ = realOp.etcdClient.Put(context.Background(), realOp.collectionPrefix()+"/1/100000", string(collectionBytes))
		}

		// "no filed info"
		{
			collectionInfo := &pb.CollectionInfo{
				ID:    100003,
				State: pb.CollectionState_CollectionCreated,
				Schema: &schemapb.CollectionSchema{
					Name: "hoo",
				},
			}
			collectionBytes, _ := proto.Marshal(collectionInfo)
			_, _ = realOp.etcdClient.Put(context.Background(), realOp.collectionPrefix()+"/1/100003", string(collectionBytes))
		}

		// success
		{
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
			time.Sleep(500 * time.Millisecond)
			assert.Eventually(t, success.Load, time.Second, time.Millisecond*100)
		}

		{
			etcdOp.UnsubscribeEvent("empty_event", api.CollectionEventType)
			etcdOp.UnsubscribeEvent("collection_event_123", api.CollectionEventType)
			etcdOp.UnsubscribeEvent("collection_event_123", api.WatchEventType(0))
		}
	}

	// get all collection
	{
		collections, err := etcdOp.GetAllCollection(context.Background(), func(ci *pb.CollectionInfo) bool {
			return strings.Contains(ci.Schema.Name, "test")
		})
		assert.NoError(t, err)
		assert.Len(t, collections, 1)
		assert.EqualValues(t, 100002, collections[0].ID)

		{
			dbName := etcdOp.GetDatabaseInfoForCollection(context.Background(), 100002)
			assert.NoError(t, err)
			assert.Equal(t, "default", dbName.Name)
		}
	}

	// get collection name by id
	{
		collectionName := etcdOp.GetCollectionNameByID(context.Background(), 100000)
		assert.NoError(t, err)
		assert.Equal(t, "test123", collectionName)
	}

	// get database name by collection id
	{
		dbName := etcdOp.GetDatabaseInfoForCollection(context.Background(), 100000)
		assert.NoError(t, err)
		assert.Equal(t, "default", dbName.Name)
	}

	// watch partition
	{
		var success util.Value[bool]
		success.Store(false)

		etcdOp.WatchPartition(context.Background(), func(pi *pb.PartitionInfo) bool {
			return strings.Contains(pi.PartitionName, "test")
		})
		etcdOp.SubscribePartitionEvent("empty_event", func(pi *pb.PartitionInfo) bool {
			return false
		})
		etcdOp.SubscribePartitionEvent("partition_event_123", func(pi *pb.PartitionInfo) bool {
			success.Store(true)
			return true
		})
		// no created state
		{
			info := &pb.PartitionInfo{
				State: pb.PartitionState_PartitionCreating,
			}
			_, _ = realOp.etcdClient.Put(context.Background(), realOp.partitionPrefix()+"/9/200005", getStringForMessage(info))
		}
		// "invalid data"
		{
			_, _ = realOp.etcdClient.Put(context.Background(), realOp.partitionPrefix()+"/9/200009", "foo")
		}
		// default partition
		{
			info := &pb.PartitionInfo{
				State:         pb.PartitionState_PartitionCreated,
				PartitionName: realOp.defaultPartitionName,
			}
			_, _ = realOp.etcdClient.Put(context.Background(), realOp.partitionPrefix()+"/9/200003", getStringForMessage(info))
		}
		// filled partition
		{
			info := &pb.PartitionInfo{
				State:         pb.PartitionState_PartitionCreated,
				PartitionName: "test345",
			}
			_, _ = realOp.etcdClient.Put(context.Background(), realOp.partitionPrefix()+"/9/200004", getStringForMessage(info))
		}
		// success
		{
			info := &pb.PartitionInfo{
				State:         pb.PartitionState_PartitionCreated,
				PartitionName: "foo",
			}
			_, _ = realOp.etcdClient.Put(context.Background(), realOp.partitionPrefix()+"/9/200005", getStringForMessage(info))

			time.Sleep(500 * time.Millisecond)
			assert.Eventually(t, success.Load, time.Second, time.Millisecond*100)
		}
		{
			etcdOp.UnsubscribeEvent("empty_event", api.PartitionEventType)
			etcdOp.UnsubscribeEvent("partition_event_123", api.PartitionEventType)
			etcdOp.UnsubscribeEvent("partition_event_123", api.WatchEventType(0))
		}
	}

	// get all partition
	{
		partitions, err := etcdOp.GetAllPartition(context.Background(), func(pi *pb.PartitionInfo) bool {
			return strings.Contains(pi.PartitionName, "test")
		})
		assert.NoError(t, err)
		assert.Len(t, partitions, 1)
		assert.EqualValues(t, "foo", partitions[0].PartitionName)
	}

	// get collection id from partition key
	{
		{
			id := realOp.getCollectionIDFromPartitionKey(realOp.partitionPrefix() + "/123456/9001")
			assert.EqualValues(t, 123456, id)
		}
		{
			id := realOp.getCollectionIDFromPartitionKey(realOp.partitionPrefix() + "/123456")
			assert.EqualValues(t, 0, id)
		}
		{
			id := realOp.getCollectionIDFromPartitionKey(realOp.partitionPrefix() + "/nonumber/9001")
			assert.EqualValues(t, 0, id)
		}
	}

	// get database id from collection key
	{
		{
			id := realOp.getDatabaseIDFromCollectionKey(realOp.collectionPrefix() + "/123456/9001")
			assert.EqualValues(t, 123456, id)
		}
		{
			id := realOp.getDatabaseIDFromCollectionKey(realOp.collectionPrefix() + "/123456")
			assert.EqualValues(t, 0, id)
		}
		{
			id := realOp.getDatabaseIDFromCollectionKey(realOp.collectionPrefix() + "/nonumber/9001")
			assert.EqualValues(t, 0, id)
		}
	}

	// get collection name by id
	{
		{
			collectionName := etcdOp.GetCollectionNameByID(context.Background(), 900000)
			assert.Equal(t, "", collectionName)
		}
		{
			_, _ = realOp.etcdClient.Put(context.Background(), realOp.collectionPrefix()+"/1/800002", "foo")
			collectionName := etcdOp.GetCollectionNameByID(context.Background(), 800002)
			assert.Equal(t, "", collectionName)
		}
	}

	// get db name by collection id
	{
		{
			databaseInfo := &pb.DatabaseInfo{
				Id:   2,
				Name: "foo",
			}
			databaseBytes, _ := proto.Marshal(databaseInfo)
			_, _ = realOp.etcdClient.Put(context.Background(), realOp.databasePrefix()+"/2", string(databaseBytes))
		}

		collectionInfo := &pb.CollectionInfo{
			ID:    99999,
			State: pb.CollectionState_CollectionCreated,
			Schema: &schemapb.CollectionSchema{
				Name: "foodb",
			},
		}
		collectionBytes, _ := proto.Marshal(collectionInfo)
		_, _ = realOp.etcdClient.Put(context.Background(), realOp.collectionPrefix()+"/2/99999", string(collectionBytes))

		dbInfo := realOp.GetDatabaseInfoForCollection(context.Background(), 99999)
		assert.Equal(t, "foo", dbInfo.Name)
		assert.EqualValues(t, 2, dbInfo.ID)
	}
}

func getStringForMessage(m proto.Message) string {
	b, _ := proto.Marshal(m)
	return string(b)
}
