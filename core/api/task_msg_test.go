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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTaskDropCollectionMsg(t *testing.T) {
	dropCollectionMsg := TaskDropCollectionMsg{
		Base: BaseTaskMsg{
			TaskID: "1001",
			MsgID:  "1002",
			TargetChannels: []string{
				"c1", "c2",
			},
			ReadyChannels: []string{
				"c1",
			},
		},
		CollectionName: "test_collection",
		DatabaseName:   "test_db",
	}

	metaMsg, err := dropCollectionMsg.ConvertToMetaMsg()
	if err != nil {
		t.Errorf("TaskDropCollectionMsg.ConvertToMetaMsg() failed: %v", err)
	}
	assert.Equal(t, DropCollectionMetaMsgType, metaMsg.Type)
	assert.Equal(t, "1001", metaMsg.Base.TaskID)
	assert.Equal(t, "1002", metaMsg.Base.MsgID)
	assert.Equal(t, []string{"c1", "c2"}, metaMsg.Base.TargetChannels)
	assert.Equal(t, []string{"c1"}, metaMsg.Base.ReadyChannels)
	assert.Equal(t, "test_collection", metaMsg.Data["collection_name"])
	assert.Equal(t, "test_db", metaMsg.Data["database_name"])
	assert.Equal(t, 3, len(metaMsg.Data))

	// test convert to TaskDropCollectionMsg
	taskMsg, err := GetTaskDropCollectionMsg(metaMsg)
	if err != nil {
		t.Errorf("GetTaskDropCollectionMsg() failed: %v", err)
	}
	assert.Equal(t, "1001", taskMsg.Base.TaskID)
	assert.Equal(t, "1002", taskMsg.Base.MsgID)
	assert.Equal(t, []string{"c1", "c2"}, taskMsg.Base.TargetChannels)
	assert.Equal(t, []string{"c1"}, taskMsg.Base.ReadyChannels)
	assert.Equal(t, "test_collection", taskMsg.CollectionName)
	assert.Equal(t, "test_db", taskMsg.DatabaseName)
}

func TestMetaMsgToJson(t *testing.T) {
	metaMsg := MetaMsg{
		Base: BaseTaskMsg{
			TaskID: "1001",
			MsgID:  "1002",
			TargetChannels: []string{
				"c1", "c2",
			},
			ReadyChannels: []string{
				"c1",
			},
		},
		Type: DropCollectionMetaMsgType,
		Data: map[string]interface{}{
			"collection_name": "test_collection",
			"database_name":   "test_db",
		},
	}

	jsonStr, err := metaMsg.ToJSON()
	if err != nil {
		t.Errorf("MetaMsg.ToJSON() failed: %v", err)
	}
	assert.NotEmpty(t, jsonStr)
	t.Logf("jsonStr = %s", jsonStr)
}
