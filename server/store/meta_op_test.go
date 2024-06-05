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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"

	"github.com/zilliztech/milvus-cdc/server/mocks"
	"github.com/zilliztech/milvus-cdc/server/model/meta"
)

func TestGetTaskInfo(t *testing.T) {
	store := mocks.NewMetaStore[*meta.TaskInfo](t)

	t.Run("fail", func(t *testing.T) {
		{
			store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("fail")).Once()
			_, err := GetTaskInfo(store, "1234")
			assert.Error(t, err)
		}
		{
			store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("fail")).Once()
			_, err := GetAllTaskInfo(store)
			assert.Error(t, err)
		}
	})

	t.Run("empty", func(t *testing.T) {
		{
			store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskInfo{}, nil).Once()
			_, err := GetTaskInfo(store, "1234")
			assert.Error(t, err)
		}
		{
			store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskInfo{}, nil).Once()
			_, err := GetAllTaskInfo(store)
			assert.Error(t, err)
		}
	})

	t.Run("success", func(t *testing.T) {
		{
			store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskInfo{{TaskID: "1234"}}, nil).Once()
			_, err := GetTaskInfo(store, "1234")
			assert.NoError(t, err)
		}
		{
			store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskInfo{{TaskID: "1234"}}, nil).Once()
			_, err := GetAllTaskInfo(store)
			assert.NoError(t, err)
		}
	})
}

func TestUpdateState(t *testing.T) {
	store := mocks.NewMetaStore[*meta.TaskInfo](t)

	t.Run("fail to get task info", func(t *testing.T) {
		store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("fail")).Once()
		err := UpdateTaskState(store, "1234", meta.TaskStateInitial, []meta.TaskState{}, "")
		assert.Error(t, err)
	})

	t.Run("empty task info", func(t *testing.T) {
		store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskInfo{}, nil).Once()
		err := UpdateTaskState(store, "1234", meta.TaskStateInitial, []meta.TaskState{}, "")
		assert.Error(t, err)
	})

	t.Run("unexpect state", func(t *testing.T) {
		store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskInfo{{TaskID: "1234", State: meta.TaskStateRunning}}, nil).Once()
		err := UpdateTaskState(store, "1234", meta.TaskStateRunning, []meta.TaskState{meta.TaskStateInitial}, "")
		assert.Error(t, err)
	})

	t.Run("fail to put task info", func(t *testing.T) {
		store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskInfo{{TaskID: "1234", State: meta.TaskStateInitial}}, nil).Once()
		store.EXPECT().Put(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("fail")).Once()
		err := UpdateTaskState(store, "1234", meta.TaskStateRunning, []meta.TaskState{meta.TaskStateInitial}, "")
		assert.Error(t, err)
	})

	t.Run("success", func(t *testing.T) {
		store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskInfo{{TaskID: "1234", State: meta.TaskStateInitial}}, nil).Once()
		store.EXPECT().Put(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		err := UpdateTaskState(store, "1234", meta.TaskStateRunning, []meta.TaskState{meta.TaskStateInitial}, "")
		assert.NoError(t, err)
	})

	t.Run("success to pause", func(t *testing.T) {
		store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskInfo{{TaskID: "1234", State: meta.TaskStateInitial}}, nil).Once()
		store.EXPECT().Put(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		err := UpdateTaskState(store, "1234", meta.TaskStatePaused, []meta.TaskState{}, "pause test")
		assert.NoError(t, err)
	})
}

func TestUpdateCollectionPosition(t *testing.T) {
	store := mocks.NewMetaStore[*meta.TaskCollectionPosition](t)

	t.Run("fail to get position info", func(t *testing.T) {
		store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("fail")).Once()
		err := UpdateTaskCollectionPosition(store, "1234", -1, "col1", "ch1", nil, nil, nil)
		assert.Error(t, err)
	})

	t.Run("first position", func(t *testing.T) {
		store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskCollectionPosition{}, nil).Once()
		store.EXPECT().Put(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		err := UpdateTaskCollectionPosition(store, "1234", -1, "col1", "ch1", &meta.PositionInfo{
			Time: 1,
			DataPair: &commonpb.KeyDataPair{
				Key:  "key1",
				Data: []byte("data1"),
			},
		}, &meta.PositionInfo{
			Time: 1,
			DataPair: &commonpb.KeyDataPair{
				Key:  "key1",
				Data: []byte("data1"),
			},
		}, &meta.PositionInfo{
			Time: 1,
			DataPair: &commonpb.KeyDataPair{
				Key:  "target-key1",
				Data: []byte("data1"),
			},
		})
		assert.NoError(t, err)
	})

	t.Run("update position", func(t *testing.T) {
		store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskCollectionPosition{
			{
				TaskID:          "1234",
				CollectionID:    -1,
				Positions:       nil,
				OpPositions:     nil,
				TargetPositions: nil,
			},
		}, nil).Once()
		store.EXPECT().Put(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		err := UpdateTaskCollectionPosition(store, "1234", -1, "col1", "ch1", &meta.PositionInfo{
			Time: 1,
			DataPair: &commonpb.KeyDataPair{
				Key:  "key1",
				Data: []byte("data1"),
			},
		}, &meta.PositionInfo{
			Time: 1,
			DataPair: &commonpb.KeyDataPair{
				Key:  "key1",
				Data: []byte("data1"),
			},
		}, &meta.PositionInfo{
			Time: 1,
			DataPair: &commonpb.KeyDataPair{
				Key:  "target-key1",
				Data: []byte("data1"),
			},
		})
		assert.NoError(t, err)
	})
}

func TestDeleteTaskPosition(t *testing.T) {
	store := mocks.NewMetaStore[*meta.TaskCollectionPosition](t)
	store.EXPECT().Delete(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("fail")).Once()
	err := DeleteTaskCollectionPosition(store, "1234", -1)
	assert.Error(t, err)
}

func TestDeleteTask(t *testing.T) {
	factory := mocks.NewMetaStoreFactory(t)
	metaStore := mocks.NewMetaStore[*meta.TaskInfo](t)
	positionStore := mocks.NewMetaStore[*meta.TaskCollectionPosition](t)
	factory.EXPECT().GetTaskInfoMetaStore(mock.Anything).Return(metaStore).Maybe()
	factory.EXPECT().GetTaskCollectionPositionMetaStore(mock.Anything).Return(positionStore).Maybe()

	t.Run("fail to get task info", func(t *testing.T) {
		metaStore.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("fail")).Once()
		_, err := DeleteTask(factory, "1234")
		assert.Error(t, err)
	})

	t.Run("empty task info", func(t *testing.T) {
		metaStore.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskInfo{}, nil).Once()
		_, err := DeleteTask(factory, "1234")
		assert.Error(t, err)
	})

	t.Run("fail to txn", func(t *testing.T) {
		metaStore.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskInfo{{TaskID: "1234"}}, nil).Once()
		factory.EXPECT().Txn(mock.Anything).Return(nil, nil, errors.New("fail")).Once()
		_, err := DeleteTask(factory, "1234")
		assert.Error(t, err)
	})

	t.Run("fail to delete meta", func(t *testing.T) {
		metaStore.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskInfo{{TaskID: "1234"}}, nil).Once()
		factory.EXPECT().Txn(mock.Anything).Return(nil, func(err error) error {
			return err
		}, nil).Once()
		metaStore.EXPECT().Delete(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("fail")).Once()
		_, err := DeleteTask(factory, "1234")
		assert.Error(t, err)
	})

	t.Run("fail to delete position", func(t *testing.T) {
		metaStore.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskInfo{{TaskID: "1234"}}, nil).Once()
		factory.EXPECT().Txn(mock.Anything).Return(nil, func(err error) error {
			return err
		}, nil).Once()
		metaStore.EXPECT().Delete(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		positionStore.EXPECT().Delete(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("fail")).Once()
		_, err := DeleteTask(factory, "1234")
		assert.Error(t, err)
	})

	t.Run("commit error", func(t *testing.T) {
		metaStore.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskInfo{{TaskID: "1234"}}, nil).Once()
		factory.EXPECT().Txn(mock.Anything).Return(nil, func(err error) error {
			return errors.New("mock")
		}, nil).Once()
		metaStore.EXPECT().Delete(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		positionStore.EXPECT().Delete(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("fail")).Once()
		_, err := DeleteTask(factory, "1234")
		assert.Error(t, err)
	})

	t.Run("success", func(t *testing.T) {
		metaStore.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskInfo{{TaskID: "1234"}}, nil).Once()
		factory.EXPECT().Txn(mock.Anything).Return(nil, func(err error) error {
			return nil
		}, nil).Once()
		metaStore.EXPECT().Delete(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		positionStore.EXPECT().Delete(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		_, err := DeleteTask(factory, "1234")
		assert.NoError(t, err)
	})
}
