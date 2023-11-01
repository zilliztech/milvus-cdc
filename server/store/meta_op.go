// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package store

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-cdc/server/api"
	servererror "github.com/zilliztech/milvus-cdc/server/error"
	"github.com/zilliztech/milvus-cdc/server/metrics"
	"github.com/zilliztech/milvus-cdc/server/model/meta"
)

func GetTaskInfo(taskInfoStore api.MetaStore[*meta.TaskInfo], taskID string) (*meta.TaskInfo, error) {
	ctx := context.Background()
	taskInfos, err := taskInfoStore.Get(ctx, &meta.TaskInfo{TaskID: taskID}, nil)
	if err != nil {
		log.Warn("fail to get the task info", zap.String("task_id", taskID), zap.Error(err))
		return nil, err
	}
	if len(taskInfos) == 0 {
		log.Warn("not found the task info", zap.String("task_id", taskID))
		return nil, servererror.NewNotFoundError(taskID)
	}
	return taskInfos[0], nil
}

func GetAllTaskInfo(taskInfoStore api.MetaStore[*meta.TaskInfo]) ([]*meta.TaskInfo, error) {
	ctx := context.Background()
	taskInfos, err := taskInfoStore.Get(ctx, &meta.TaskInfo{}, nil)
	if err != nil {
		log.Warn("fail to get the task info", zap.Error(err))
		return nil, err
	}
	if len(taskInfos) == 0 {
		log.Warn("not found the task info")
		return nil, servererror.NewNotFoundError("task info")
	}
	return taskInfos, nil
}

func UpdateTaskState(taskInfoStore api.MetaStore[*meta.TaskInfo], taskID string,
	newState meta.TaskState, oldStates []meta.TaskState, reason string,
) error {
	ctx := context.Background()
	infos, err := taskInfoStore.Get(ctx, &meta.TaskInfo{TaskID: taskID}, nil)
	if err != nil {
		log.Warn("fail to get the task info", zap.String("task_id", taskID), zap.Error(err))
		return err
	}
	if len(infos) == 0 {
		log.Warn("not found the task info", zap.String("task_id", taskID))
		return errors.Errorf("not found the task info with task id %s", taskID)
	}
	info := infos[0]
	if len(oldStates) != 0 && !lo.Contains(oldStates, info.State) {
		oldStateStrs := lo.Map[meta.TaskState, string](oldStates, func(taskState meta.TaskState, i int) string {
			return taskState.String()
		})
		return errors.Errorf("the task state can be only set to [%s] when current state is %v, but current state is %s. You can retry it.",
			newState.String(), oldStateStrs, info.State.String())
	}
	oldState := info.State
	info.State = newState
	if newState == meta.TaskStatePaused {
		info.Reason = reason
	}
	err = taskInfoStore.Put(ctx, info, nil)
	if err != nil {
		log.Warn("fail to put the task info to etcd", zap.String("task_id", taskID), zap.Error(err))
		return err
	}
	metrics.TaskNumVec.UpdateState(newState, oldState)
	return nil
}

func UpdateTaskCollectionPosition(taskPositionStore api.MetaStore[*meta.TaskCollectionPosition], taskID string, collectionID int64, collectionName string, pChannelName string, position, opPosition, targetPosition *meta.PositionInfo) error {
	ctx := context.Background()
	positions, err := taskPositionStore.Get(ctx, &meta.TaskCollectionPosition{TaskID: taskID, CollectionID: collectionID}, nil)
	if err != nil {
		log.Warn("fail to get the task position", zap.String("task_id", taskID), zap.Int64("collection_id", collectionID), zap.Error(err))
		return err
	}

	if len(positions) == 0 {
		metaPosition := &meta.TaskCollectionPosition{
			TaskID:         taskID,
			CollectionID:   collectionID,
			CollectionName: collectionName,
			Positions: map[string]*meta.PositionInfo{
				pChannelName: position,
			},
			OpPositions:     make(map[string]*meta.PositionInfo),
			TargetPositions: make(map[string]*meta.PositionInfo),
		}
		if opPosition != nil {
			metaPosition.OpPositions = map[string]*meta.PositionInfo{
				pChannelName: opPosition,
			}
		}
		if targetPosition != nil {
			metaPosition.TargetPositions = map[string]*meta.PositionInfo{
				targetPosition.DataPair.GetKey(): targetPosition,
			}
		}
		return taskPositionStore.Put(ctx, metaPosition, nil)
	}

	metaPosition := positions[0]
	if metaPosition.Positions == nil {
		metaPosition.Positions = make(map[string]*meta.PositionInfo)
	}
	if metaPosition.OpPositions == nil {
		metaPosition.OpPositions = make(map[string]*meta.PositionInfo)
	}
	if metaPosition.TargetPositions == nil {
		metaPosition.TargetPositions = make(map[string]*meta.PositionInfo)
	}
	if position != nil {
		metaPosition.Positions[pChannelName] = position
	}
	if opPosition != nil {
		metaPosition.OpPositions[pChannelName] = opPosition
	}
	if targetPosition != nil {
		metaPosition.TargetPositions[targetPosition.DataPair.GetKey()] = targetPosition
	}
	return taskPositionStore.Put(ctx, metaPosition, nil)
}

func DeleteTaskCollectionPosition(taskPositionStore api.MetaStore[*meta.TaskCollectionPosition], taskID string, collectionID int64) error {
	err := taskPositionStore.Delete(context.Background(), &meta.TaskCollectionPosition{TaskID: taskID, CollectionID: collectionID}, nil)
	if err != nil {
		log.Warn("fail to delete the task position", zap.String("task_id", taskID), zap.Int64("collection_id", collectionID), zap.Error(err))
	}
	return err
}

func DeleteTask(factory api.MetaStoreFactory, taskID string) (*meta.TaskInfo, error) {
	ctx := context.Background()
	infos, err := factory.GetTaskInfoMetaStore(ctx).Get(ctx, &meta.TaskInfo{TaskID: taskID}, nil)
	if err != nil {
		log.Warn("fail to get the task info", zap.String("task_id", taskID), zap.Error(err))
		return nil, err
	}
	if len(infos) == 0 {
		log.Warn("not found the task info", zap.String("task_id", taskID))
		return nil, servererror.NewNotFoundError(taskID)
	}
	info := infos[0]

	txnObj, commitFunc, err := factory.Txn(ctx)
	if err != nil {
		log.Warn("fail to create the store txn", zap.String("task_id", taskID), zap.Error(err))
		return nil, err
	}
	defer func() {
		if err == nil {
			return
		}
		err = commitFunc(err)
		if err != nil {
			log.Warn("fail to commit the txn", zap.String("task_id", taskID), zap.Error(err))
		}
	}()
	err = factory.GetTaskInfoMetaStore(ctx).Delete(ctx, &meta.TaskInfo{TaskID: taskID}, txnObj)
	if err != nil {
		log.Warn("fail to delete the task info", zap.String("task_id", taskID), zap.Error(err))
		return nil, err
	}
	err = factory.GetTaskCollectionPositionMetaStore(ctx).Delete(ctx, &meta.TaskCollectionPosition{TaskID: taskID}, txnObj)
	if err != nil {
		log.Warn("fail to delete the task position", zap.String("task_id", taskID), zap.Error(err))
		return nil, err
	}
	if err == nil {
		commitErr := commitFunc(err)
		if commitErr == nil {
			metrics.TaskNumVec.Delete(info.State)
			return info, nil
		}
		return nil, commitErr
	}
	return nil, err
}
