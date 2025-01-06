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

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/cockroachdb/errors"
	"github.com/mitchellh/mapstructure"
)

type MetaMsgType int

const (
	DropCollectionMetaMsgType MetaMsgType = iota + 1
	DropPartitionMetaMsgType
)

type BaseTaskMsg struct {
	TaskID         string   `json:"task_id"`
	MsgID          string   `json:"msg_id"`
	TargetChannels []string `json:"target_channels"`
	ReadyChannels  []string `json:"ready_channels"`
}

func (msg BaseTaskMsg) IsReady() bool {
	if len(msg.TargetChannels) != len(msg.ReadyChannels) {
		return false
	}
	sort.Strings(msg.TargetChannels)
	sort.Strings(msg.ReadyChannels)
	for i := range msg.TargetChannels {
		if msg.TargetChannels[i] != msg.ReadyChannels[i] {
			return false
		}
	}
	return true
}

type MetaMsg struct {
	Base BaseTaskMsg            `json:"base"`
	Type MetaMsgType            `json:"type"`
	Data map[string]interface{} `json:"data"`
}

func (msg MetaMsg) ToJSON() (string, error) {
	bs, err := json.Marshal(msg)
	if err != nil {
		return "", err
	}
	return string(bs), nil
}

type TaskDropCollectionMsg struct {
	Base           BaseTaskMsg `mapstructure:"-"`
	DatabaseName   string      `mapstructure:"database_name"`
	CollectionName string      `mapstructure:"collection_name"`
	DropTS         uint64      `mapstructure:"drop_ts"`
}

func (msg TaskDropCollectionMsg) ConvertToMetaMsg() (MetaMsg, error) {
	var m map[string]interface{}
	err := mapstructure.Decode(msg, &m)
	if err != nil {
		return MetaMsg{}, err
	}
	return MetaMsg{
		Base: msg.Base,
		Type: DropCollectionMetaMsgType,
		Data: m,
	}, nil
}

func GetTaskDropCollectionMsg(msg MetaMsg) (TaskDropCollectionMsg, error) {
	if msg.Type != DropCollectionMetaMsgType {
		return TaskDropCollectionMsg{}, errors.Newf("type %d is not DropCollectionMetaMsg", msg.Type)
	}
	var m TaskDropCollectionMsg
	err := mapstructure.Decode(msg.Data, &m)
	if err != nil {
		return TaskDropCollectionMsg{}, err
	}
	m.Base = msg.Base
	return m, nil
}

func GetDropCollectionMsgID(collectionID int64) string {
	return fmt.Sprintf("drop-collection-%d", collectionID)
}

type TaskDropPartitionMsg struct {
	Base           BaseTaskMsg `mapstructure:"-"`
	DatabaseName   string      `mapstructure:"database_name"`
	CollectionName string      `mapstructure:"collection_name"`
	PartitionName  string      `mapstructure:"partition_name"`
	DropTS         uint64      `mapstructure:"drop_ts"`
}

func (msg TaskDropPartitionMsg) ConvertToMetaMsg() (MetaMsg, error) {
	var m map[string]interface{}
	err := mapstructure.Decode(msg, &m)
	if err != nil {
		return MetaMsg{}, err
	}
	return MetaMsg{
		Base: msg.Base,
		Type: DropPartitionMetaMsgType,
		Data: m,
	}, nil
}

func GetTaskDropPartitionMsg(msg MetaMsg) (TaskDropPartitionMsg, error) {
	if msg.Type != DropPartitionMetaMsgType {
		return TaskDropPartitionMsg{}, errors.Newf("type %d is not DropPartitionMetaMsg", msg.Type)
	}
	var m TaskDropPartitionMsg
	err := mapstructure.Decode(msg.Data, &m)
	if err != nil {
		return TaskDropPartitionMsg{}, err
	}
	m.Base = msg.Base
	return m, nil
}

func GetDropPartitionMsgID(collectionID int64, partitionID int64) string {
	return fmt.Sprintf("drop-partition-%d-%d", collectionID, partitionID)
}
