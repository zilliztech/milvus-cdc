// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"errors"

	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-cdc/core/util"
	"github.com/zilliztech/milvus-cdc/server/api"
	"github.com/zilliztech/milvus-cdc/server/metrics"
	"github.com/zilliztech/milvus-cdc/server/model/meta"
	"github.com/zilliztech/milvus-cdc/server/store"
)

type WriteCallback struct {
	metaStoreFactory api.MetaStoreFactory
	rootPath         string
	taskID           string
	log              *zap.Logger
}

func NewWriteCallback(factory api.MetaStoreFactory, rootPath string, taskID string) *WriteCallback {
	return &WriteCallback{
		metaStoreFactory: factory,
		rootPath:         rootPath,
		taskID:           taskID,
		log:              log.With(zap.String("task_id", taskID)).Logger,
	}
}

func (w *WriteCallback) UpdateTaskCollectionPosition(collectionID int64, collectionName string, pChannelName string, position, opPosition, targetPosition *meta.PositionInfo) error {
	if position == nil {
		return errors.New("position is nil")
	}
	err := store.UpdateTaskCollectionPosition(
		w.metaStoreFactory.GetTaskCollectionPositionMetaStore(context.Background()),
		w.taskID,
		collectionID,
		collectionName,
		pChannelName,
		position, opPosition, targetPosition)
	if err != nil {
		w.log.Warn("fail to update the collection position",
			zap.Int64("collection_id", collectionID),
			zap.String("vchannel_name", pChannelName),
			zap.String("position", util.Base64Encode(position)),
			zap.Error(err))
		metrics.WriterFailCountVec.WithLabelValues(w.taskID, metrics.WriteFailOnUpdatePosition).Inc()
	}
	return err
}
