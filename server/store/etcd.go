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

package store

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/goccy/go-json"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-cdc/core/log"
	"github.com/zilliztech/milvus-cdc/core/util"
	"github.com/zilliztech/milvus-cdc/server/api"
	"github.com/zilliztech/milvus-cdc/server/model/meta"
)

var (
	EtcdOpTimeout        = 5 * time.Second
	EtcdOpRetryTime uint = 5
)

type EtcdMetaStore struct {
	log                         *zap.Logger
	etcdClient                  *clientv3.Client
	taskInfoStore               *TaskInfoEtcdStore
	taskCollectionPositionStore *TaskCollectionPositionEtcdStore
	txnMap                      map[any][]clientv3.Op
}

var _ api.MetaStoreFactory = &EtcdMetaStore{}

func NewEtcdMetaStore(ctx context.Context, endpoints []string, rootPath string) (*EtcdMetaStore, error) {
	log := log.With(zap.Strings("endpoints", endpoints)).Logger
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
		Logger:      log,
	})
	if err != nil {
		log.Warn("fail to get etcd client")
		return nil, err
	}
	txnMap := make(map[any][]clientv3.Op)
	taskInfoStore, err := NewTaskInfoEtcdStore(ctx, etcdClient, rootPath, txnMap)
	if err != nil {
		log.Warn("fail to get task info store")
		return nil, err
	}
	taskCollectionPositionStore, err := NewTaskCollectionPositionEtcdStore(ctx, etcdClient, rootPath, txnMap)
	if err != nil {
		log.Warn("fail to get task collection position store")
		return nil, err
	}

	return &EtcdMetaStore{
		log:                         log,
		etcdClient:                  etcdClient,
		taskInfoStore:               taskInfoStore,
		taskCollectionPositionStore: taskCollectionPositionStore,
		txnMap:                      txnMap,
	}, nil
}

func (e *EtcdMetaStore) GetTaskInfoMetaStore(ctx context.Context) api.MetaStore[*meta.TaskInfo] {
	return e.taskInfoStore
}

func (e *EtcdMetaStore) GetTaskCollectionPositionMetaStore(ctx context.Context) api.MetaStore[*meta.TaskCollectionPosition] {
	return e.taskCollectionPositionStore
}

func (e *EtcdMetaStore) Txn(ctx context.Context) (any, func(err error) error, error) {
	txn := e.etcdClient.Txn(ctx)
	commitFunc := func(err error) error {
		defer delete(e.txnMap, txn)
		if err == nil {
			txn.Then(e.txnMap[txn]...)
			_, err = txn.Commit()
			if err != nil {
				return err
			}
			return nil
		}
		return err
	}
	e.txnMap[txn] = []clientv3.Op{}
	return txn, commitFunc, nil
}

type TaskInfoEtcdStore struct {
	log        *zap.Logger
	rootPath   string
	etcdClient *clientv3.Client
	txnMap     map[any][]clientv3.Op
}

var _ api.MetaStore[*meta.TaskInfo] = &TaskInfoEtcdStore{}

func NewTaskInfoEtcdStore(ctx context.Context, etcdClient *clientv3.Client, rootPath string, txnMap map[any][]clientv3.Op) (*TaskInfoEtcdStore, error) {
	t := &TaskInfoEtcdStore{
		rootPath:   rootPath,
		etcdClient: etcdClient,
		txnMap:     txnMap,
	}
	t.log = log.With(zap.String("meta_store", "etcd"), zap.String("table", "task_info"), zap.String("root_path", rootPath)).Logger
	err := EtcdStatus(ctx, etcdClient)
	if err != nil {
		t.log.Warn("unavailable etcd server, please check it", zap.Error(err))
		return nil, err
	}
	return t, nil
}

func (t *TaskInfoEtcdStore) Put(ctx context.Context, metaObj *meta.TaskInfo, txn any) error {
	timeCtx, cancel := context.WithTimeout(ctx, EtcdOpTimeout)
	defer cancel()
	objBytes, err := json.Marshal(metaObj)
	if err != nil {
		t.log.Warn("fail to marshal task info", zap.Error(err))
		return err
	}
	taskInfoKey := getTaskInfoKey(t.rootPath, metaObj.TaskID)

	if txn != nil {
		if _, ok := t.txnMap[txn]; !ok {
			t.log.Warn("txn not exist")
			return errors.New("txn not exist")
		}
		t.txnMap[txn] = append(t.txnMap[txn], clientv3.OpPut(taskInfoKey, util.ToString(objBytes)))
		return nil
	}

	_, err = t.etcdClient.Put(timeCtx, taskInfoKey, util.ToString(objBytes))
	if err != nil {
		t.log.Warn("fail to put task info", zap.Error(err))
		return err
	}
	return nil
}

func (t *TaskInfoEtcdStore) Get(ctx context.Context, metaObj *meta.TaskInfo, txn any) ([]*meta.TaskInfo, error) {
	timeCtx, cancel := context.WithTimeout(ctx, EtcdOpTimeout)
	defer cancel()
	key := getTaskInfoPrefix(t.rootPath)
	ops := []clientv3.OpOption{clientv3.WithPrefix()}
	if metaObj.TaskID != "" {
		key = getTaskInfoKey(t.rootPath, metaObj.TaskID)
		ops = []clientv3.OpOption{}
	}
	if txn != nil {
		if _, ok := t.txnMap[txn]; !ok {
			return nil, errors.New("txn not exist")
		}
		t.txnMap[txn] = append(t.txnMap[txn], clientv3.OpGet(key, ops...))
		return nil, nil
	}

	getResp, err := t.etcdClient.Get(timeCtx, key, ops...)
	if err != nil {
		t.log.Warn("fail to get task info", zap.Error(err))
		return nil, err
	}
	var taskInfos []*meta.TaskInfo

	for _, kv := range getResp.Kvs {
		taskInfoValue := kv.Value
		var taskInfo meta.TaskInfo
		err = json.Unmarshal(taskInfoValue, &taskInfo)
		if err != nil {
			t.log.Warn("fail to unmarshal task info", zap.Error(err))
			return nil, err
		}
		taskInfos = append(taskInfos, &taskInfo)
	}
	return taskInfos, nil
}

func (t *TaskInfoEtcdStore) Delete(ctx context.Context, metaObj *meta.TaskInfo, txn any) error {
	if metaObj.TaskID == "" {
		return errors.New("task id is empty")
	}
	timeCtx, cancel := context.WithTimeout(ctx, EtcdOpTimeout)
	defer cancel()
	taskInfoKey := getTaskInfoKey(t.rootPath, metaObj.TaskID)
	if txn != nil {
		if _, ok := t.txnMap[txn]; !ok {
			return errors.New("txn not exist")
		}
		t.txnMap[txn] = append(t.txnMap[txn], clientv3.OpDelete(taskInfoKey))
		return nil
	}
	_, err := t.etcdClient.Delete(timeCtx, taskInfoKey)
	if err != nil {
		t.log.Warn("fail to delete task info", zap.Error(err))
		return err
	}
	return err
}

type TaskCollectionPositionEtcdStore struct {
	log        *zap.Logger
	rootPath   string
	etcdClient *clientv3.Client
	txnMap     map[any][]clientv3.Op
}

var _ api.MetaStore[*meta.TaskCollectionPosition] = &TaskCollectionPositionEtcdStore{}

func NewTaskCollectionPositionEtcdStore(ctx context.Context, etcdClient *clientv3.Client, rootPath string, txnMap map[any][]clientv3.Op) (*TaskCollectionPositionEtcdStore, error) {
	t := &TaskCollectionPositionEtcdStore{
		rootPath:   rootPath,
		etcdClient: etcdClient,
		txnMap:     txnMap,
	}
	t.log = log.With(zap.String("meta_store", "etcd"), zap.String("table", "task_collection_position"), zap.String("root_path", rootPath)).Logger
	err := EtcdStatus(ctx, etcdClient)
	if err != nil {
		t.log.Warn("unavailable etcd server, please check it", zap.Error(err))
		return nil, err
	}
	return t, nil
}

func (t *TaskCollectionPositionEtcdStore) Put(ctx context.Context, metaObj *meta.TaskCollectionPosition, txn any) error {
	timeCtx, cancel := context.WithTimeout(ctx, EtcdOpTimeout)
	defer cancel()
	positionBytes, err := json.Marshal(metaObj)
	taskPositionKey := getTaskCollectionPositionKey(t.rootPath, metaObj.TaskID, metaObj.CollectionID)
	if err != nil {
		t.log.Warn("fail to marshal task position", zap.Error(err))
		return err
	}

	if txn != nil {
		if _, ok := t.txnMap[txn]; !ok {
			t.log.Warn("txn not exist")
			return errors.New("txn not exist")
		}
		t.txnMap[txn] = append(t.txnMap[txn], clientv3.OpPut(taskPositionKey, util.ToString(positionBytes)))
		return nil
	}
	_, err = t.etcdClient.Put(timeCtx, taskPositionKey, util.ToString(positionBytes))
	if err != nil {
		t.log.Warn("fail to put task position", zap.Error(err))
		return err
	}
	return nil
}

func (t *TaskCollectionPositionEtcdStore) Get(ctx context.Context, metaObj *meta.TaskCollectionPosition, txn any) ([]*meta.TaskCollectionPosition, error) {
	timeCtx, cancel := context.WithTimeout(ctx, EtcdOpTimeout)
	defer cancel()
	key := getTaskCollectionPositionPrefix(t.rootPath)
	ops := []clientv3.OpOption{clientv3.WithPrefix()}
	if metaObj.TaskID != "" && metaObj.CollectionID != 0 {
		key = getTaskCollectionPositionKey(t.rootPath, metaObj.TaskID, metaObj.CollectionID)
		ops = []clientv3.OpOption{}
	} else if metaObj.TaskID != "" {
		key = getTaskCollectionPositionPrefixWithTaskID(t.rootPath, metaObj.TaskID)
		ops = []clientv3.OpOption{clientv3.WithPrefix()}
	}
	if txn != nil {
		if _, ok := t.txnMap[txn]; !ok {
			t.log.Warn("txn not exist")
			return nil, errors.New("txn not exist")
		}
		t.txnMap[txn] = append(t.txnMap[txn], clientv3.OpGet(key, ops...))
		return nil, nil
	}

	getResp, err := t.etcdClient.Get(timeCtx, key, ops...)
	if err != nil {
		t.log.Warn("fail to get task position", zap.Error(err))
		return nil, err
	}
	var taskPositions []*meta.TaskCollectionPosition

	for _, kv := range getResp.Kvs {
		taskPositionValue := kv.Value
		var taskPosition meta.TaskCollectionPosition
		err = json.Unmarshal(taskPositionValue, &taskPosition)
		if err != nil {
			t.log.Warn("fail to unmarshal task position", zap.Error(err))
			return nil, err
		}
		taskPositions = append(taskPositions, &taskPosition)
	}
	return taskPositions, nil
}

func (t *TaskCollectionPositionEtcdStore) Delete(ctx context.Context, metaObj *meta.TaskCollectionPosition, txn any) error {
	if metaObj.TaskID == "" {
		t.log.Warn("task id is empty")
		return errors.New("task id is empty")
	}
	timeCtx, cancel := context.WithTimeout(ctx, EtcdOpTimeout)
	defer cancel()
	key := getTaskCollectionPositionPrefixWithTaskID(t.rootPath, metaObj.TaskID)
	ops := []clientv3.OpOption{clientv3.WithPrefix()}
	if metaObj.CollectionID != 0 {
		key = getTaskCollectionPositionKey(t.rootPath, metaObj.TaskID, metaObj.CollectionID)
		ops = []clientv3.OpOption{}
	}
	if txn != nil {
		if _, ok := t.txnMap[txn]; !ok {
			t.log.Warn("txn not exist")
			return errors.New("txn not exist")
		}
		t.txnMap[txn] = append(t.txnMap[txn], clientv3.OpDelete(key, ops...))
		return nil
	}

	_, err := t.etcdClient.Delete(timeCtx, key, ops...)
	if err != nil {
		t.log.Warn("fail to delete task position", zap.Error(err))
		return err
	}
	return nil
}

func EtcdStatus(ctx context.Context, etcdClient *clientv3.Client) error {
	ctx, cancel := context.WithTimeout(ctx, EtcdOpTimeout)
	defer cancel()
	for _, endpoint := range etcdClient.Endpoints() {
		_, err := etcdClient.Status(ctx, endpoint)
		if err != nil {
			return err
		}
	}
	return nil
}
