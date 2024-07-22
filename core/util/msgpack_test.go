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

package util

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
)

func TestOnceChan(t *testing.T) {
	c := make(chan int, 1)
	onceChan := NewOnceWriteChan(c)
	onceChan.Write(1)
	onceChan.Write(2)
	data := <-c
	assert.Equal(t, 1, data)
	select {
	case <-c:
		assert.Fail(t, "channel should be empty")
	default:
	}
}

func TestGetCollectionNameFromMsgPack(t *testing.T) {
	t.Run("empty pack", func(t *testing.T) {
		assert.Equal(t, "", GetCollectionNameFromMsgPack(&msgstream.MsgPack{}))
	})

	t.Run("success", func(t *testing.T) {
		msgPack := &msgstream.MsgPack{
			Msgs: []msgstream.TsMsg{
				&msgstream.CreateCollectionMsg{
					CreateCollectionRequest: msgpb.CreateCollectionRequest{
						CollectionName: "test",
					},
				},
			},
		}
		assert.Equal(t, "test", GetCollectionNameFromMsgPack(msgPack))
	})
}
