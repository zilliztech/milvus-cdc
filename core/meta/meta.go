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

package meta

import (
	"context"
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/zilliztech/milvus-cdc/core/api"
)

const KeyPrefix = "task_msg"

type ReplicateMeteImpl struct {
	store              api.ReplicateStore
	metaLock           sync.RWMutex
	dropCollectionMsgs map[string]map[string]api.TaskDropCollectionMsg
	dropPartitionMsgs  map[string]map[string]api.TaskDropPartitionMsg
}

var _ api.ReplicateMeta = (*ReplicateMeteImpl)(nil)

func NewReplicateMetaImpl(store api.ReplicateStore) (*ReplicateMeteImpl, error) {
	impl := &ReplicateMeteImpl{
		store:              store,
		dropCollectionMsgs: make(map[string]map[string]api.TaskDropCollectionMsg),
		dropPartitionMsgs:  make(map[string]map[string]api.TaskDropPartitionMsg),
	}
	err := impl.Reload()
	if err != nil {
		return nil, err
	}
	return impl, nil
}

func (r *ReplicateMeteImpl) Reload() error {
	metaMsgs, err := r.store.Get(context.Background(), "", true)
	if err != nil {
		return err
	}
	for _, msg := range metaMsgs {
		switch msg.Type {
		case api.DropCollectionMetaMsgType:
			taskDropCollectionMsg, err := api.GetTaskDropCollectionMsg(msg)
			if err != nil {
				return err
			}
			if _, ok := r.dropCollectionMsgs[taskDropCollectionMsg.Base.TaskID]; !ok {
				r.dropCollectionMsgs[taskDropCollectionMsg.Base.TaskID] = make(map[string]api.TaskDropCollectionMsg)
			}
			r.dropCollectionMsgs[taskDropCollectionMsg.Base.TaskID][taskDropCollectionMsg.Base.MsgID] = taskDropCollectionMsg
		case api.DropPartitionMetaMsgType:
			taskDropPartitionMsg, err := api.GetTaskDropPartitionMsg(msg)
			if err != nil {
				return err
			}
			if _, ok := r.dropPartitionMsgs[taskDropPartitionMsg.Base.TaskID]; !ok {
				r.dropPartitionMsgs[taskDropPartitionMsg.Base.TaskID] = make(map[string]api.TaskDropPartitionMsg)
			}
			r.dropPartitionMsgs[taskDropPartitionMsg.Base.TaskID][taskDropPartitionMsg.Base.MsgID] = taskDropPartitionMsg
		}
	}

	return nil
}

// UpdateTaskDropCollectionMsg bool: true if the msg is ready to be consumed
func (r *ReplicateMeteImpl) UpdateTaskDropCollectionMsg(ctx context.Context, msg api.TaskDropCollectionMsg) (bool, error) {
	r.metaLock.Lock()
	defer r.metaLock.Unlock()
	taskMsgs, ok := r.dropCollectionMsgs[msg.Base.TaskID]
	if !ok {
		taskMsgs = map[string]api.TaskDropCollectionMsg{
			msg.Base.MsgID: msg,
		}
		r.dropCollectionMsgs[msg.Base.TaskID] = taskMsgs
		metaMsg, err := msg.ConvertToMetaMsg()
		if err != nil {
			return false, err
		}
		err = r.store.Put(ctx, GetMetaKey(msg.Base.TaskID, msg.Base.MsgID), metaMsg)
		if err != nil {
			return false, err
		}
		return msg.Base.IsReady(), nil
	}
	var taskMsg api.TaskDropCollectionMsg
	if taskMsg, ok = taskMsgs[msg.Base.MsgID]; !ok {
		taskMsgs[msg.Base.MsgID] = msg
		metaMsg, err := msg.ConvertToMetaMsg()
		if err != nil {
			return false, err
		}
		err = r.store.Put(ctx, GetMetaKey(msg.Base.TaskID, msg.Base.MsgID), metaMsg)
		if err != nil {
			return false, err
		}
		return msg.Base.IsReady(), nil
	}
	taskMsg.Base.ReadyChannels = lo.Union[string](taskMsg.Base.ReadyChannels, msg.Base.ReadyChannels)
	metaMsg, err := taskMsg.ConvertToMetaMsg()
	if err != nil {
		return false, err
	}
	err = r.store.Put(ctx, GetMetaKey(msg.Base.TaskID, msg.Base.MsgID), metaMsg)
	if err != nil {
		return false, err
	}
	return taskMsg.Base.IsReady(), nil
}

func (r *ReplicateMeteImpl) GetTaskDropCollectionMsg(ctx context.Context, taskID string, msgID string) ([]api.TaskDropCollectionMsg, error) {
	if taskID == "" {
		return nil, errors.New("taskID is empty")
	}
	r.metaLock.RLock()
	defer r.metaLock.RUnlock()
	if msgID == "" {
		taskMsgs, ok := r.dropCollectionMsgs[taskID]
		if !ok {
			return nil, errors.Errorf("taskID %s not found", taskID)
		}
		result := make([]api.TaskDropCollectionMsg, 0, len(taskMsgs))
		for _, msg := range taskMsgs {
			result = append(result, msg)
		}
		return result, nil
	}

	if taskMsgs, ok := r.dropCollectionMsgs[taskID]; ok {
		if msg, ok := taskMsgs[msgID]; ok {
			return []api.TaskDropCollectionMsg{msg}, nil
		}
	}
	return nil, errors.Errorf("taskID %s or msgID %s not found", taskID, msgID)
}

func (r *ReplicateMeteImpl) UpdateTaskDropPartitionMsg(ctx context.Context, msg api.TaskDropPartitionMsg) (bool, error) {
	r.metaLock.Lock()
	defer r.metaLock.Unlock()
	taskMsgs, ok := r.dropPartitionMsgs[msg.Base.TaskID]
	if !ok {
		taskMsgs = map[string]api.TaskDropPartitionMsg{
			msg.Base.MsgID: msg,
		}
		r.dropPartitionMsgs[msg.Base.TaskID] = taskMsgs
		metaMsg, err := msg.ConvertToMetaMsg()
		if err != nil {
			return false, err
		}
		err = r.store.Put(ctx, GetMetaKey(msg.Base.TaskID, msg.Base.MsgID), metaMsg)
		if err != nil {
			return false, err
		}
		return msg.Base.IsReady(), nil
	}
	var taskMsg api.TaskDropPartitionMsg
	if taskMsg, ok = taskMsgs[msg.Base.MsgID]; !ok {
		taskMsgs[msg.Base.MsgID] = msg
		metaMsg, err := msg.ConvertToMetaMsg()
		if err != nil {
			return false, err
		}
		err = r.store.Put(ctx, GetMetaKey(msg.Base.TaskID, msg.Base.MsgID), metaMsg)
		if err != nil {
			return false, err
		}
		return msg.Base.IsReady(), nil
	}
	taskMsg.Base.ReadyChannels = lo.Union[string](taskMsg.Base.ReadyChannels, msg.Base.ReadyChannels)
	metaMsg, err := taskMsg.ConvertToMetaMsg()
	if err != nil {
		return false, err
	}
	err = r.store.Put(ctx, GetMetaKey(msg.Base.TaskID, msg.Base.MsgID), metaMsg)
	if err != nil {
		return false, err
	}
	return taskMsg.Base.IsReady(), nil
}

func (r *ReplicateMeteImpl) GetTaskDropPartitionMsg(ctx context.Context, taskID string, msgID string) ([]api.TaskDropPartitionMsg, error) {
	if taskID == "" {
		return nil, errors.New("taskID is empty")
	}
	r.metaLock.RLock()
	defer r.metaLock.RUnlock()
	if msgID == "" {
		taskMsgs, ok := r.dropPartitionMsgs[taskID]
		if !ok {
			return nil, errors.Errorf("taskID %s not found", taskID)
		}
		result := make([]api.TaskDropPartitionMsg, 0, len(taskMsgs))
		for _, msg := range taskMsgs {
			result = append(result, msg)
		}
		return result, nil
	}
	if taskMsgs, ok := r.dropPartitionMsgs[taskID]; ok {
		if msg, ok := taskMsgs[msgID]; ok {
			return []api.TaskDropPartitionMsg{msg}, nil
		}
	}
	return nil, errors.Errorf("taskID %s or msgID %s not found", taskID, msgID)
}

func (r *ReplicateMeteImpl) RemoveTaskMsg(ctx context.Context, taskID string, msgID string) error {
	key := GetMetaKey(taskID, msgID)
	err := r.store.Remove(ctx, key)
	if err != nil {
		return err
	}
	r.metaLock.Lock()
	defer r.metaLock.Unlock()
	if taskMsgs, ok := r.dropCollectionMsgs[taskID]; ok {
		delete(taskMsgs, msgID)
	}
	return nil
}

func GetMetaKey(taskID, msgID string) string {
	return KeyPrefix + "/" + taskID + "/" + msgID
}

func GetKeyDetail(key string) (taskID, msgID string) {
	details := strings.Split(key, "/")
	if len(details) < 3 {
		return "", ""
	}
	l := len(details)
	return details[l-2], details[l-1]
}
