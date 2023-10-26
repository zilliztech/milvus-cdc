package store

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-cdc/server/api"
	"github.com/zilliztech/milvus-cdc/server/model"
	"github.com/zilliztech/milvus-cdc/server/model/meta"
)

func TestTxnMap(t *testing.T) {
	{
		a := map[clientv3.Txn]string{}
		etcdClient, err := clientv3.New(clientv3.Config{
			Endpoints: []string{"localhost:2379"},
		})
		assert.NoError(t, err)
		t1 := etcdClient.Txn(context.Background())
		t2 := etcdClient.Txn(context.Background())
		a[t1] = "t1"
		a[t2] = "t2"
		assert.Equal(t, 2, len(a))
		assert.Equal(t, "t1", a[t1])
		assert.Equal(t, "t2", a[t2])
	}
	{
		a := map[*sql.Tx]string{}
		db, err := sql.Open("mysql", "root:123456@tcp(127.0.0.1:3306)/milvuscdc?charset=utf8")
		assert.NoError(t, err)
		t1, _ := db.BeginTx(context.Background(), nil)
		t2, _ := db.BeginTx(context.Background(), nil)
		a[t1] = "t1"
		a[t2] = "t2"
		assert.Equal(t, 2, len(a))
		assert.Equal(t, "t1", a[t1])
		assert.Equal(t, "t2", a[t2])
	}
}

func TestMysql(t *testing.T) {
	{
		ctx, cancelFunc := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancelFunc()
		_, err := NewMySQLMetaStore(ctx, "root:123456@tcp(127.0.0.1:33060)/milvuscdc?charset=utf8", "/cdc")
		assert.Error(t, err)
	}

	ctx := context.Background()
	mysqlStore, err := NewMySQLMetaStore(ctx, "root:123456@tcp(127.0.0.1:3306)/milvuscdc?charset=utf8", "/cdc")
	assert.NoError(t, err)

	testTaskInfoMetaStore(ctx, t, mysqlStore)
	testTaskCollectionPositionMetaStore(ctx, t, mysqlStore)
}

func TestEtcd(t *testing.T) {
	// nil endpoints
	{
		_, err := NewEtcdMetaStore(context.Background(), nil, "/cdc")
		assert.Error(t, err)
	}
	// error endpoints
	{
		ctx, cancelFunc := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancelFunc()
		_, err := NewEtcdMetaStore(ctx, []string{"localhost:23790"}, "/cdc")
		assert.Error(t, err)
	}

	ctx := context.Background()
	etcdStore, err := NewEtcdMetaStore(ctx, []string{"localhost:2379"}, "/cdc-meta-test")
	defer etcdStore.etcdClient.Delete(ctx, "/cdc-meta-test", clientv3.WithPrefix())
	assert.NoError(t, err)

	testTaskInfoMetaStore(ctx, t, etcdStore)
	testTaskCollectionPositionMetaStore(ctx, t, etcdStore)
}

func testTaskInfoMetaStore(ctx context.Context, t *testing.T, storeFactory api.MetaStoreFactory) {
	var nilTxn any = nil
	taskInfoStore := storeFactory.GetTaskInfoMetaStore(ctx)
	{
		err := taskInfoStore.Put(ctx, &meta.TaskInfo{
			TaskID: "bdef5f5c-08e2-4261-a2a3-ff28380230e4",
			MilvusConnectParam: model.MilvusConnectParam{
				Host: "localhost",
				Port: 19530,
			},
			State: meta.TaskStateInitial,
		}, nilTxn)
		assert.NoError(t, err)
	}
	{
		err := taskInfoStore.Put(ctx, &meta.TaskInfo{
			TaskID: "45692f04-0103-48c9-87f7-c61c9a62f176",
			MilvusConnectParam: model.MilvusConnectParam{
				Host: "127.0.0.1",
				Port: 19530,
			},
			State: meta.TaskStateRunning,
		}, nilTxn)
		assert.NoError(t, err)
	}
	{
		taskInfos, err := taskInfoStore.Get(ctx, &meta.TaskInfo{}, nilTxn)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(taskInfos))
		log.Info("get all task info", zap.Any("tasks", taskInfos))

		taskInfos, err = taskInfoStore.Get(ctx, &meta.TaskInfo{TaskID: "no-invalid"}, nilTxn)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(taskInfos))

		taskInfos, err = taskInfoStore.Get(ctx, &meta.TaskInfo{TaskID: "bdef5f5c-08e2-4261-a2a3-ff28380230e4"}, nilTxn)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(taskInfos))
		log.Info("get task info by task id", zap.Any("tasks", taskInfos))
	}
	{
		// delete taskinfo test
		err := taskInfoStore.Delete(ctx, &meta.TaskInfo{TaskID: "bdef5f5c-08e2-4261-a2a3-ff28380230e4"}, nilTxn)
		assert.NoError(t, err)

		taskInfos, err := taskInfoStore.Get(ctx, &meta.TaskInfo{}, nilTxn)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(taskInfos))
		log.Info("get all task info after delete", zap.Any("tasks", taskInfos))

		err = taskInfoStore.Delete(ctx, &meta.TaskInfo{TaskID: "45692f04-0103-48c9-87f7-c61c9a62f176"}, nilTxn)
		assert.NoError(t, err)
	}

	{
		txn, commitFunc, err := storeFactory.Txn(ctx)
		assert.NoError(t, err)

		{
			err := taskInfoStore.Put(ctx, &meta.TaskInfo{
				TaskID: "bdef5f5c-08e2-4261-a2a3-ff28380230e4",
				MilvusConnectParam: model.MilvusConnectParam{
					Host: "localhost",
					Port: 19530,
				},
				State: meta.TaskStateInitial,
			}, txn)
			assert.NoError(t, err)
		}
		{
			err := taskInfoStore.Put(ctx, &meta.TaskInfo{
				TaskID: "45692f04-0103-48c9-87f7-c61c9a62f176",
				MilvusConnectParam: model.MilvusConnectParam{
					Host: "127.0.0.1",
					Port: 19530,
				},
				State: meta.TaskStateRunning,
			}, txn)
			assert.NoError(t, err)
		}
		{
			_, err := taskInfoStore.Get(ctx, &meta.TaskInfo{TaskID: "11111"}, txn)
			assert.NoError(t, err)
		}
		err = commitFunc(err)
		assert.NoError(t, err)
	}

	{
		taskInfos, err := taskInfoStore.Get(ctx, &meta.TaskInfo{}, nilTxn)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(taskInfos))

		txn, commitFunc, err := storeFactory.Txn(ctx)
		assert.NoError(t, err)

		{
			err := taskInfoStore.Delete(ctx, &meta.TaskInfo{TaskID: "bdef5f5c-08e2-4261-a2a3-ff28380230e4"}, txn)
			assert.NoError(t, err)
		}
		{
			err = taskInfoStore.Delete(ctx, &meta.TaskInfo{TaskID: "45692f04-0103-48c9-87f7-c61c9a62f176"}, txn)
			assert.NoError(t, err)
		}
		err = commitFunc(err)
		assert.NoError(t, err)
	}
	{
		// invalid txn
		{
			err := taskInfoStore.Put(ctx, &meta.TaskInfo{
				TaskID: "45692f04-0103-48c9-87f7-c61c9a62f176",
				MilvusConnectParam: model.MilvusConnectParam{
					Host: "127.0.0.1",
					Port: 19530,
				},
				State: meta.TaskStateRunning,
			}, 1)
			assert.Error(t, err)
		}
		{
			_, err := taskInfoStore.Get(ctx, &meta.TaskInfo{TaskID: "45692f04-0103-48c9-87f7-c61c9a62f176"}, 1)
			assert.Error(t, err)
		}
		{
			err := taskInfoStore.Delete(ctx, &meta.TaskInfo{TaskID: "45692f04-0103-48c9-87f7-c61c9a62f176"}, 1)
			assert.Error(t, err)
		}
	}
	// delete empty task id
	{
		err := taskInfoStore.Delete(ctx, &meta.TaskInfo{TaskID: ""}, nilTxn)
		assert.Error(t, err)
	}
}

func testTaskCollectionPositionMetaStore(ctx context.Context, t *testing.T, storeFactory api.MetaStoreFactory) {
	var nilTxn any = nil
	taskCollectionPositionStore := storeFactory.GetTaskCollectionPositionMetaStore(ctx)
	{
		err := taskCollectionPositionStore.Put(ctx, &meta.TaskCollectionPosition{
			TaskID:         "bdef5f5c-08e2-4261-a2a3-ff28380230e4",
			CollectionID:   1,
			CollectionName: "foo",
			Positions: map[string]*meta.PositionInfo{
				"pchan1": {
					Time: 1,
					DataPair: &commonpb.KeyDataPair{
						Key:  "key1",
						Data: []byte("data1"),
					},
				},
				"pchan2": {
					Time: 2,
					DataPair: &commonpb.KeyDataPair{
						Key:  "key2",
						Data: []byte("data2"),
					},
				},
			},
		}, nilTxn)
		assert.NoError(t, err)

		err = taskCollectionPositionStore.Put(ctx, &meta.TaskCollectionPosition{
			TaskID:         "bdef5f5c-08e2-4261-a2a3-ff28380230e4",
			CollectionID:   1,
			CollectionName: "foo",
			Positions: map[string]*meta.PositionInfo{
				"pchan3": {
					Time: 1,
					DataPair: &commonpb.KeyDataPair{
						Key:  "key3",
						Data: []byte("data3"),
					},
				},
				"pchan4": {
					Time: 2,
					DataPair: &commonpb.KeyDataPair{
						Key:  "key4",
						Data: []byte("data4"),
					},
				},
			},
		}, nilTxn)
		assert.NoError(t, err)

		err = taskCollectionPositionStore.Put(ctx, &meta.TaskCollectionPosition{
			TaskID:         "45692f04-0103-48c9-87f7-c61c9a62f176",
			CollectionID:   2,
			CollectionName: "bar",
			Positions: map[string]*meta.PositionInfo{
				"pchan3": {
					Time: 1,
					DataPair: &commonpb.KeyDataPair{
						Key:  "key3",
						Data: []byte("data3"),
					},
				},
				"pchan4": {
					Time: 2,
					DataPair: &commonpb.KeyDataPair{
						Key:  "key4",
						Data: []byte("data4"),
					},
				},
			},
		}, nilTxn)
		assert.NoError(t, err)
	}
	{
		taskCollectionPositions, err := taskCollectionPositionStore.Get(ctx, &meta.TaskCollectionPosition{}, nilTxn)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(taskCollectionPositions))
		log.Info("get all task collection position", zap.Any("tasks", taskCollectionPositions))

		taskCollectionPositions, err = taskCollectionPositionStore.Get(ctx, &meta.TaskCollectionPosition{TaskID: "no-invalid"}, nilTxn)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(taskCollectionPositions))

		taskCollectionPositions, err = taskCollectionPositionStore.Get(ctx, &meta.TaskCollectionPosition{TaskID: "bdef5f5c-08e2-4261-a2a3-ff28380230e4"}, nilTxn)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(taskCollectionPositions))
		log.Info("get task collection position by task id", zap.Any("tasks", taskCollectionPositions))

		taskCollectionPositions, err = taskCollectionPositionStore.Get(ctx, &meta.TaskCollectionPosition{TaskID: "bdef5f5c-08e2-4261-a2a3-ff28380230e4", CollectionID: 1}, nilTxn)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(taskCollectionPositions))
	}

	{
		err := taskCollectionPositionStore.Delete(ctx, &meta.TaskCollectionPosition{TaskID: "bdef5f5c-08e2-4261-a2a3-ff28380230e4"}, nilTxn)
		assert.NoError(t, err)

		taskCollectionPositions, err := taskCollectionPositionStore.Get(ctx, &meta.TaskCollectionPosition{}, nilTxn)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(taskCollectionPositions))

		err = taskCollectionPositionStore.Delete(ctx, &meta.TaskCollectionPosition{TaskID: "bdef5f5c-08e2-4261-a2a3-ff28380230e4"}, nilTxn)
		assert.NoError(t, err)

		err = taskCollectionPositionStore.Delete(ctx, &meta.TaskCollectionPosition{TaskID: "45692f04-0103-48c9-87f7-c61c9a62f176"}, nilTxn)
		assert.NoError(t, err)
	}

	{
		txn, commitFunc, err := storeFactory.Txn(ctx)
		assert.NoError(t, err)

		{
			err := taskCollectionPositionStore.Put(ctx, &meta.TaskCollectionPosition{
				TaskID:         "bdef5f5c-08e2-4261-a2a3-ff28380230e4",
				CollectionID:   1,
				CollectionName: "foo",
				Positions: map[string]*meta.PositionInfo{
					"pchan1": {
						Time: 1,
						DataPair: &commonpb.KeyDataPair{
							Key:  "key1",
							Data: []byte("data1"),
						},
					},
					"pchan2": {
						Time: 2,
						DataPair: &commonpb.KeyDataPair{
							Key:  "key2",
							Data: []byte("data2"),
						},
					},
				},
			}, txn)
			assert.NoError(t, err)

			err = taskCollectionPositionStore.Put(ctx, &meta.TaskCollectionPosition{
				TaskID:         "45692f04-0103-48c9-87f7-c61c9a62f176",
				CollectionID:   2,
				CollectionName: "bar",
				Positions: map[string]*meta.PositionInfo{
					"pchan3": {
						Time: 1,
						DataPair: &commonpb.KeyDataPair{
							Key:  "key3",
							Data: []byte("data3"),
						},
					},
					"pchan4": {
						Time: 2,
						DataPair: &commonpb.KeyDataPair{
							Key:  "key4",
							Data: []byte("data4"),
						},
					},
				},
			}, txn)
			assert.NoError(t, err)

			_, err = taskCollectionPositionStore.Get(ctx, &meta.TaskCollectionPosition{TaskID: "bdef5f5c-08e2-4261-a2a3-ff28380230e4"}, txn)
			assert.NoError(t, err)
		}

		err = commitFunc(err)
		assert.NoError(t, err)
	}

	{
		taskCollectionPositions, err := taskCollectionPositionStore.Get(ctx, &meta.TaskCollectionPosition{}, nilTxn)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(taskCollectionPositions))

		txn, commitFunc, err := storeFactory.Txn(ctx)
		assert.NoError(t, err)

		{
			err := taskCollectionPositionStore.Delete(ctx, &meta.TaskCollectionPosition{TaskID: "bdef5f5c-08e2-4261-a2a3-ff28380230e4"}, txn)
			assert.NoError(t, err)
		}

		{
			err := taskCollectionPositionStore.Delete(ctx, &meta.TaskCollectionPosition{TaskID: "45692f04-0103-48c9-87f7-c61c9a62f176"}, txn)
			assert.NoError(t, err)
		}

		err = commitFunc(err)
		assert.NoError(t, err)
	}

	{
		// invalid txn
		{
			err := taskCollectionPositionStore.Put(ctx, &meta.TaskCollectionPosition{
				TaskID:         "bdef5f5c-08e2-4261-a2a3-ff28380230e4",
				CollectionID:   1,
				CollectionName: "foo",
				Positions: map[string]*meta.PositionInfo{
					"pchan1": {
						Time: 1,
						DataPair: &commonpb.KeyDataPair{
							Key:  "key1",
							Data: []byte("data1"),
						},
					},
					"pchan2": {
						Time: 2,
						DataPair: &commonpb.KeyDataPair{
							Key:  "key2",
							Data: []byte("data2"),
						},
					},
				},
			}, 1)
			assert.Error(t, err)
		}
		{
			err := taskCollectionPositionStore.Delete(ctx, &meta.TaskCollectionPosition{TaskID: "bdef5f5c-08e2-4261-a2a3-ff28380230e4"}, 1)
			assert.Error(t, err)
		}
		{
			_, err := taskCollectionPositionStore.Get(ctx, &meta.TaskCollectionPosition{TaskID: "bdef5f5c-08e2-4261-a2a3-ff28380230e4"}, 1)
			assert.Error(t, err)
		}
	}
	// delete empty task id
	{
		err := taskCollectionPositionStore.Delete(ctx, &meta.TaskCollectionPosition{TaskID: ""}, 1)
		assert.Error(t, err)
	}
}
