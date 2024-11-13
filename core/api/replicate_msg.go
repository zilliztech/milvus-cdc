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

package api

import "github.com/milvus-io/milvus/pkg/mq/msgstream"

var EmptyMsgPack = &ReplicateMsg{}

type ReplicateMsg struct {
	// source collection and channel info
	CollectionName string
	CollectionID   int64
	PChannelName   string
	TaskID         string
	MsgPack        *msgstream.MsgPack
}

func GetReplicateMsg(pchannelName string, collectionName string, collectionID int64, msgPack *msgstream.MsgPack, taskID string) *ReplicateMsg {
	return &ReplicateMsg{
		CollectionName: collectionName,
		CollectionID:   collectionID,
		PChannelName:   pchannelName,
		TaskID:         taskID,
		MsgPack:        msgPack,
	}
}
