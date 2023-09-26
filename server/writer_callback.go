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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-cdc/core/util"
	"github.com/zilliztech/milvus-cdc/server/metrics"
	"github.com/zilliztech/milvus-cdc/server/store"
)

type WriteCallback struct {
	// writer.DefaultWriteCallBack

	metaStoreFactory store.MetaStoreFactory
	rootPath         string
	taskID           string
	log              *zap.Logger
}

func NewWriteCallback(factory store.MetaStoreFactory, rootPath string, taskID string) *WriteCallback {
	return &WriteCallback{
		metaStoreFactory: factory,
		rootPath:         rootPath,
		taskID:           taskID,
		log:              log.With(zap.String("task_id", taskID)).Logger,
	}
}

// func (w *WriteCallback) OnFail(data *model.CDCData, err error) {
// 	w.log.Warn("fail to write the msg", zap.String("data", util.Base64Encode(data)), zap.Error(err))
// 	metrics.WriterFailCountVec.WithLabelValues(w.taskID, metrics.WriteFailOnFail).Inc()
// 	_ = store.UpdateTaskFailedReason(w.metaStoreFactory.GetTaskInfoMetaStore(context.Background()), w.taskID, err.Error())
// }
//
// func (w *WriteCallback) OnSuccess(collectionID int64, channelInfos map[string]writer.CallbackChannelInfo) {
// 	var msgType string
// 	var count int
// 	for channelName, info := range channelInfos {
// 		if info.MsgType == commonpb.MsgType_Insert {
// 			msgType = commonpb.MsgType_Insert.String()
// 		} else if info.MsgType == commonpb.MsgType_Delete {
// 			msgType = commonpb.MsgType_Delete.String()
// 		}
// 		count += info.MsgRowCount
// 		sub := util.SubByNow(info.Ts)
// 		metrics.WriterTimeDifferenceVec.WithLabelValues(w.taskID, strconv.FormatInt(collectionID, 10), channelName).Set(float64(sub))
// 	}
// 	if msgType != "" {
// 		metrics.WriteMsgRowCountVec.WithLabelValues(w.taskID, strconv.FormatInt(collectionID, 10), msgType).Add(float64(count))
// 	}
// 	// means it's drop collection message
// 	if len(channelInfos) > 1 {
// 		metrics.StreamingCollectionCountVec.WithLabelValues(w.taskID, metrics.FinishStatusLabel).Inc()
// 	}
// }

func (w *WriteCallback) UpdateTaskCollectionPosition(collectionID int64, collectionName string, pChannelName string, position *commonpb.KeyDataPair) {
	if position == nil {
		return
	}
	err := store.UpdateTaskCollectionPosition(
		w.metaStoreFactory.GetTaskCollectionPositionMetaStore(context.Background()),
		w.taskID,
		collectionID,
		collectionName,
		pChannelName,
		position)
	if err != nil {
		w.log.Warn("fail to update the collection position",
			zap.Int64("collection_id", collectionID),
			zap.String("vchannel_name", pChannelName),
			zap.String("position", util.Base64Encode(position)),
			zap.Error(err))
		metrics.WriterFailCountVec.WithLabelValues(w.taskID, metrics.WriteFailOnUpdatePosition).Inc()
	}
}
