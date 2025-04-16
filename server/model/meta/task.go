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
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"

	"github.com/zilliztech/milvus-cdc/server/model"
)

type TaskState int

const (
	TaskStateInitial TaskState = iota
	TaskStateRunning
	TaskStatePaused
)

const (
	// MinTaskState Must ATTENTION the of `add` and `reduce` method of server.TaskNumMetric
	// if you add new task state !!!!
	MinTaskState = TaskStateInitial
	MaxTaskState = TaskStatePaused
)

func (t TaskState) IsValidTaskState() bool {
	return t >= MinTaskState && t <= MaxTaskState
}

func (t TaskState) String() string {
	switch t {
	case TaskStateInitial:
		return "Initial"
	case TaskStateRunning:
		return "Running"
	case TaskStatePaused:
		return "Paused"
	default:
		return fmt.Sprintf("Unknown value[%d]", t)
	}
}

type TaskInfo struct {
	TaskID                string
	MilvusConnectParam    model.MilvusConnectParam
	KafkaConnectParam     model.KafkaConnectParam
	WriterCacheConfig     model.BufferConfig
	CollectionInfos       []model.CollectionInfo
	DBCollections         map[string][]model.CollectionInfo
	NameMapping           []model.NameMapping
	RPCRequestChannelInfo model.ChannelInfo
	ExtraInfo             model.ExtraInfo
	ExcludeCollections    []string // it's used for the `*` collection name
	DisableAutoStart      bool
	State                 TaskState
	Reason                string
}

func (t *TaskInfo) CollectionNames() []string {
	names := make([]string, len(t.CollectionInfos))
	for i, info := range t.CollectionInfos {
		names[i] = info.Name
	}
	return names
}

func (t *TaskInfo) MapNames(db, collection string) (string, string) {
	for _, mapping := range t.NameMapping {
		if mapping.SourceDB == db {
			if len(mapping.CollectionMapping) == 0 {
				return mapping.TargetDB, collection
			}
			if newCollection, ok := mapping.CollectionMapping[collection]; ok {
				return mapping.TargetDB, newCollection
			}
		}
	}
	return db, collection
}

type PositionInfo struct {
	StartTime int64
	Time      int64
	DataPair  *commonpb.KeyDataPair
	Dropped   bool
}

type TaskCollectionPosition struct {
	TaskID         string
	CollectionID   int64
	CollectionName string
	// Positions key -> channel name, value -> check point
	Positions map[string]*PositionInfo
	// OpPositions latest op positions
	OpPositions map[string]*PositionInfo
	// TargetPositions target instance positions
	TargetPositions map[string]*PositionInfo
}
