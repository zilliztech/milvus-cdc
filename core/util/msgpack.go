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
	"bytes"
	"sync"

	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/requestutil"
)

var SuffixSnapshotTombstone = []byte{0xE2, 0x9B, 0xBC} // base64 value: "4pu8"

type OnceWriteChan[T any] struct {
	once sync.Once
	ch   chan<- T
}

func NewOnceWriteChan[T any](c chan<- T) *OnceWriteChan[T] {
	return &OnceWriteChan[T]{
		ch: c,
	}
}

func (o *OnceWriteChan[T]) Write(data T) {
	o.once.Do(func() {
		o.ch <- data
	})
}

func IsTombstone(data []byte) bool {
	return bytes.Equal(data, SuffixSnapshotTombstone)
}

func GetCollectionNameFromMsgPack(msgPack *msgstream.MsgPack) string {
	if len(msgPack.Msgs) == 0 {
		return ""
	}
	firstMsg := msgPack.Msgs[0]
	collectionName, _ := requestutil.GetCollectionNameFromRequest(firstMsg)
	return collectionName.(string)
}

func GetDatabaseNameFromMsgPack(msgPack *msgstream.MsgPack) string {
	if len(msgPack.Msgs) == 0 {
		return ""
	}
	firstMsg := msgPack.Msgs[0]
	dbName, _ := requestutil.GetDbNameFromRequest(firstMsg)
	return dbName.(string)
}

func GetCollectionIDFromMsgPack(msgPack *msgstream.MsgPack) int64 {
	if len(msgPack.Msgs) == 0 {
		return 0
	}
	firstMsg := msgPack.Msgs[0]
	collectionID, _ := GetCollectionIDFromRequest(firstMsg)
	return collectionID
}
