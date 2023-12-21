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

package server

import (
	"encoding/json"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-cdc/core/log"
	"github.com/zilliztech/milvus-cdc/server/mocks"
	"github.com/zilliztech/milvus-cdc/server/model/meta"
)

func TestJson(t *testing.T) {
	b, err := json.Marshal(make(map[string]string))
	log.Info(string(b), zap.Error(err))
}

func TestWriterCallback(t *testing.T) {
	factory := mocks.NewMetaStoreFactory(t)
	store := mocks.NewMetaStore[*meta.TaskCollectionPosition](t)
	callback := NewWriteCallback(factory, "test", "12345")

	t.Run("empty position", func(t *testing.T) {
		err := callback.UpdateTaskCollectionPosition(1, "test", "test", nil, nil, nil)
		assert.Error(t, err)
	})

	t.Run("fail", func(t *testing.T) {
		factory.EXPECT().GetTaskCollectionPositionMetaStore(mock.Anything).Return(store).Once()
		store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("test")).Once()
		err := callback.UpdateTaskCollectionPosition(1, "test", "test", &meta.PositionInfo{
			Time: 1,
			DataPair: &commonpb.KeyDataPair{
				Key:  "test",
				Data: []byte("test"),
			},
		}, nil, nil)
		assert.Error(t, err)
	})
	t.Run("success", func(t *testing.T) {
		factory.EXPECT().GetTaskCollectionPositionMetaStore(mock.Anything).Return(store).Once()
		store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskCollectionPosition{}, nil).Once()
		store.EXPECT().Put(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		err := callback.UpdateTaskCollectionPosition(1, "test", "test", &meta.PositionInfo{
			Time: 1,
			DataPair: &commonpb.KeyDataPair{
				Key:  "test",
				Data: []byte("test"),
			},
		}, nil, nil)
		assert.NoError(t, err)
	})
}
