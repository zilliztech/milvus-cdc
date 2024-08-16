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

package request

import (
	"github.com/zilliztech/milvus-cdc/server/model"
	"github.com/zilliztech/milvus-cdc/server/model/meta"
)

const (
	Create      = "create"
	Delete      = "delete"
	Pause       = "pause"
	Resume      = "resume"
	Get         = "get"
	GetPosition = "position"
	List        = "list"
	Maintenance = "maintenance"
)

type CDCRequest struct {
	RequestType string         `json:"request_type" mapstructure:"request_type"`
	RequestData map[string]any `json:"request_data" mapstructure:"request_data"`
}

type CDCResponse struct {
	Code    int            `json:"code" mapstructure:"code"`
	Message string         `json:"message,omitempty" mapstructure:"message,omitempty"`
	Data    map[string]any `json:"data,omitempty" mapstructure:"data,omitempty"`
}

// Task some info can be showed about the task
type Task struct {
	TaskID          string                 `json:"task_id" mapstructure:"task_id"`
	ConnectParam    model.ConnectParam     `json:"connect_param" mapstructure:"connect_param"`
	CollectionInfos []model.CollectionInfo `json:"collection_infos" mapstructure:"collection_infos"`
	State           string                 `json:"state" mapstructure:"state"`
	LastPauseReason string                 `json:"reason,omitempty" mapstructure:"reason,omitempty"`
}

func GetTask(taskInfo *meta.TaskInfo) Task {
	taskInfo.ConnectParam.Milvus.Username = ""
	taskInfo.ConnectParam.Milvus.Password = ""
	return Task{
		TaskID:          taskInfo.TaskID,
		ConnectParam:    taskInfo.ConnectParam,
		CollectionInfos: taskInfo.CollectionInfos,
		State:           taskInfo.State.String(),
		LastPauseReason: taskInfo.Reason,
	}
}
