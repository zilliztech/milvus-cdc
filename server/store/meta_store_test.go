package store

import (
	"context"
	"database/sql"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-cdc/core/util"
	"github.com/zilliztech/milvus-cdc/server/model"
	"github.com/zilliztech/milvus-cdc/server/model/meta"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
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
	ctx := context.Background()
	mysqlStore, err := NewMySQLMetaStore(ctx, "root:123456@tcp(127.0.0.1:3306)/milvuscdc?charset=utf8", "/cdc")
	assert.NoError(t, err)

	testTaskInfoMetaStore(ctx, t, mysqlStore)
	testTaskCollectionPositionMetaStore(ctx, t, mysqlStore)
}

func TestEtcd(t *testing.T) {
	ctx := context.Background()
	etcdStore, err := NewEtcdMetaStore(ctx, []string{"localhost:2379"}, "/cdc")
	assert.NoError(t, err)

	testTaskInfoMetaStore(ctx, t, etcdStore)
	testTaskCollectionPositionMetaStore(ctx, t, etcdStore)
}

func testTaskInfoMetaStore(ctx context.Context, t *testing.T, storeFactory MetaStoreFactory) {
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
		util.Log.Info("get all task info", zap.Any("tasks", taskInfos))

		taskInfos, err = taskInfoStore.Get(ctx, &meta.TaskInfo{TaskID: "no-invalid"}, nilTxn)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(taskInfos))

		taskInfos, err = taskInfoStore.Get(ctx, &meta.TaskInfo{TaskID: "bdef5f5c-08e2-4261-a2a3-ff28380230e4"}, nilTxn)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(taskInfos))
		util.Log.Info("get task info by task id", zap.Any("tasks", taskInfos))
	}
	{
		// delete taskinfo test
		err := taskInfoStore.Delete(ctx, &meta.TaskInfo{TaskID: "bdef5f5c-08e2-4261-a2a3-ff28380230e4"}, nilTxn)
		assert.NoError(t, err)

		taskInfos, err := taskInfoStore.Get(ctx, &meta.TaskInfo{}, nilTxn)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(taskInfos))
		util.Log.Info("get all task info after delete", zap.Any("tasks", taskInfos))

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
}

func testTaskCollectionPositionMetaStore(ctx context.Context, t *testing.T, storeFactory MetaStoreFactory) {
	var nilTxn any = nil
	taskCollectionPositionStore := storeFactory.GetTaskCollectionPositionMetaStore(ctx)
	{
		err := taskCollectionPositionStore.Put(ctx, &meta.TaskCollectionPosition{
			TaskID:         "bdef5f5c-08e2-4261-a2a3-ff28380230e4",
			CollectionID:   1,
			CollectionName: "foo",
			Positions: map[string]*commonpb.KeyDataPair{
				"pchan1": {
					Key:  "key1",
					Data: []byte("data1"),
				},
				"pchan2": {
					Key:  "key2",
					Data: []byte("data2"),
				},
			},
		}, nilTxn)
		assert.NoError(t, err)

		err = taskCollectionPositionStore.Put(ctx, &meta.TaskCollectionPosition{
			TaskID:         "bdef5f5c-08e2-4261-a2a3-ff28380230e4",
			CollectionID:   1,
			CollectionName: "foo",
			Positions: map[string]*commonpb.KeyDataPair{
				"pchan3": {
					Key:  "key3",
					Data: []byte("data3"),
				},
				"pchan4": {
					Key:  "key4",
					Data: []byte("data4"),
				},
			},
		}, nilTxn)
		assert.NoError(t, err)

		err = taskCollectionPositionStore.Put(ctx, &meta.TaskCollectionPosition{
			TaskID:         "45692f04-0103-48c9-87f7-c61c9a62f176",
			CollectionID:   2,
			CollectionName: "bar",
			Positions: map[string]*commonpb.KeyDataPair{
				"pchan3": {
					Key:  "key3",
					Data: []byte("data3"),
				},
				"pchan4": {
					Key:  "key4",
					Data: []byte("data4"),
				},
			},
		}, nilTxn)
		assert.NoError(t, err)
	}
	{
		taskCollectionPositions, err := taskCollectionPositionStore.Get(ctx, &meta.TaskCollectionPosition{}, nilTxn)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(taskCollectionPositions))
		util.Log.Info("get all task collection position", zap.Any("tasks", taskCollectionPositions))

		taskCollectionPositions, err = taskCollectionPositionStore.Get(ctx, &meta.TaskCollectionPosition{TaskID: "no-invalid"}, nilTxn)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(taskCollectionPositions))

		taskCollectionPositions, err = taskCollectionPositionStore.Get(ctx, &meta.TaskCollectionPosition{TaskID: "bdef5f5c-08e2-4261-a2a3-ff28380230e4"}, nilTxn)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(taskCollectionPositions))
		util.Log.Info("get task collection position by task id", zap.Any("tasks", taskCollectionPositions))
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
				Positions: map[string]*commonpb.KeyDataPair{
					"pchan1": {
						Key:  "key1",
						Data: []byte("data1"),
					},
					"pchan2": {
						Key:  "key2",
						Data: []byte("data2"),
					},
				},
			}, txn)
			assert.NoError(t, err)

			err = taskCollectionPositionStore.Put(ctx, &meta.TaskCollectionPosition{
				TaskID:         "45692f04-0103-48c9-87f7-c61c9a62f176",
				CollectionID:   2,
				CollectionName: "bar",
				Positions: map[string]*commonpb.KeyDataPair{
					"pchan3": {
						Key:  "key3",
						Data: []byte("data3"),
					},
					"pchan4": {
						Key:  "key4",
						Data: []byte("data4"),
					},
				},
			}, txn)
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
}
