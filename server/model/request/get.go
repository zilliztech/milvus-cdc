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

//go:generate easytags $GOFILE json,mapstructure
type GetRequest struct {
	TaskID string `json:"task_id" mapstructure:"task_id"`
}

type GetResponse struct {
	Task Task `json:"task" mapstructure:"task"`
}

type GetPositionRequest struct {
	TaskID string `json:"task_id" mapstructure:"task_id"`
}

type Position struct {
	CollectionID int64  `json:"collection_id" mapstructure:"collection_id"`
	ChannelName  string `json:"channel_name" mapstructure:"channel_name"`
	Time         int64  `json:"time" mapstructure:"time"` // time.UnixMilli()
	TT           uint64 `json:"tt" mapstructure:"tt"`
	MsgID        string `json:"msg_id" mapstructure:"msg_id"`
}

type GetPositionResponse struct {
	Positions       []Position `json:"positions" mapstructure:"positions"`
	OpPositions     []Position `json:"op_positions" mapstructure:"op_positions"`
	TargetPositions []Position `json:"target_positions" mapstructure:"target_positions"`
}
