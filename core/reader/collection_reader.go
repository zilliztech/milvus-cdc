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

package reader

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/retry"

	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/log"
	"github.com/zilliztech/milvus-cdc/core/model"
	"github.com/zilliztech/milvus-cdc/core/pb"
	"github.com/zilliztech/milvus-cdc/core/util"
)

const (
	AllCollection   = "*"
	AllDatabase     = "*"
	DefaultDatabase = "default"
)

type CollectionInfo struct {
	collectionName string
	positions      map[string]*commonpb.KeyDataPair
}

// ShouldReadFunc is a function to determine whether the collection should be read
// the first bool value is whether the collection should be replicated by the start position
// the second bool value is whether the collection should be read
type ShouldReadFunc func(*model.DatabaseInfo, *pb.CollectionInfo) (bool, bool)

var _ api.Reader = (*CollectionReader)(nil)

type CollectionReader struct {
	api.DefaultReader

	id                     string // which is task id
	channelManager         api.ChannelManager
	metaOp                 api.MetaOp
	channelSeekPositions   map[int64]map[string]*msgpb.MsgPosition
	replicateCollectionMap util.Map[int64, *pb.CollectionInfo]
	replicateChannelMap    util.Map[string, struct{}]
	errChan                chan error
	shouldReadFunc         ShouldReadFunc
	startOnce              sync.Once
	quitOnce               sync.Once

	retryOptions []retry.Option
}

func NewCollectionReader(id string,
	channelManager api.ChannelManager, metaOp api.MetaOp,
	seekPosition map[int64]map[string]*msgpb.MsgPosition,
	shouldReadFunc ShouldReadFunc,
	readerConfig config.ReaderConfig,
) (api.Reader, error) {
	reader := &CollectionReader{
		id:                   id,
		channelManager:       channelManager,
		metaOp:               metaOp,
		channelSeekPositions: seekPosition,
		shouldReadFunc:       shouldReadFunc,
		errChan:              make(chan error),
		retryOptions:         util.GetRetryOptions(readerConfig.Retry),
	}
	return reader, nil
}

func (reader *CollectionReader) StartRead(ctx context.Context) {
	reader.startOnce.Do(func() {
		ctx = util.GetCtxWithTaskID(ctx, reader.id)
		reader.metaOp.SubscribeCollectionEvent(reader.id, func(info *pb.CollectionInfo) bool {
			collectionLog := log.With(
				zap.String("task_id", reader.id),
				zap.String("collection_name", info.Schema.Name),
				zap.Int64("collection_id", info.ID))
			if info.State == SkipCollectionState {
				// handle the collection state from the creating directly to dropped
				reader.channelManager.AddDroppedCollection([]int64{info.ID})
				collectionLog.Info("has dropped collection")
				return true
			}

			collectionLog.Info("has watched to read collection")
			dbInfo := reader.metaOp.GetDatabaseInfoForCollection(ctx, info.ID)
			_, shouldRead := reader.shouldReadFunc(&dbInfo, info)
			if !shouldRead {
				collectionLog.Info("the collection should not be read")
				return false
			}
			startPositions := make([]*msgpb.MsgPosition, 0)
			for _, v := range info.StartPositions {
				startPositions = append(startPositions, &msgstream.MsgPosition{
					ChannelName: v.GetKey(),
					MsgID:       v.GetData(),
					Timestamp:   info.CreateTime,
				})
			}
			if err := reader.channelManager.StartReadCollection(ctx, &dbInfo, info, startPositions); err != nil {
				collectionLog.Warn("fail to start to replicate the collection data in the watch process", zap.Any("info", info), zap.Error(err))
				reader.sendError(err)
			}
			reader.replicateCollectionMap.Store(info.ID, info)
			collectionLog.Info("has started to read collection")
			return true
		})
		reader.metaOp.SubscribePartitionEvent(reader.id, func(info *pb.PartitionInfo) bool {
			partitionLog := log.With(
				zap.Int64("collection_id", info.CollectionId),
				zap.Int64("partition_id", info.PartitionID),
				zap.String("partition_name", info.PartitionName),
				zap.String("task_id", reader.id),
			)
			if info.State == SkipPartitionState {
				partitionLog.Info("has dropped partition")
				reader.channelManager.AddDroppedPartition([]int64{info.PartitionID})
				return true
			}

			partitionLog.Info("has watched to read partition")
			var collectionName string
			retryErr := retry.Do(ctx, func() error {
				collectionName = reader.metaOp.GetCollectionNameByID(ctx, info.CollectionId)
				if collectionName != "" {
					return nil
				}
				return errors.Newf("fail to get collection name by id %d", info.CollectionId)
			}, reader.retryOptions...)
			if retryErr != nil || collectionName == "" {
				partitionLog.Warn("empty collection name", zap.Int64("collection_id", info.CollectionId), zap.Error(retryErr))
				return true
			}
			if IsDroppedObject(collectionName) {
				partitionLog.Info("the collection has been dropped", zap.Int64("collection_id", info.CollectionId))
				return true
			}
			tmpCollectionInfo := &pb.CollectionInfo{
				ID: info.CollectionId,
				Schema: &schemapb.CollectionSchema{
					Name: collectionName,
				},
			}
			dbInfo := reader.metaOp.GetDatabaseInfoForCollection(ctx, tmpCollectionInfo.ID)
			_, shouldRead := reader.shouldReadFunc(&dbInfo, tmpCollectionInfo)
			if !shouldRead {
				partitionLog.Info("the partition should not be read", zap.String("name", collectionName))
				return true
			}

			err := reader.channelManager.AddPartition(ctx, &dbInfo, tmpCollectionInfo, info)
			if err != nil {
				partitionLog.Warn("fail to add partition", zap.String("collection_name", collectionName), zap.Any("partition", info), zap.Error(err))
				reader.sendError(err)
			}
			partitionLog.Info("has started to add partition")
			return false
		})
		reader.metaOp.WatchCollection(ctx, nil)
		reader.metaOp.WatchPartition(ctx, nil)

		readerLog := log.With(zap.String("task_id", reader.id))

		existedCollectionInfos, err := reader.metaOp.GetAllCollection(ctx, func(info *pb.CollectionInfo) bool {
			// return !reader.shouldReadFunc(info)
			return false
		})
		if err != nil {
			readerLog.Warn("get all collection failed", zap.Error(err))
			reader.sendError(err)
			return
		}

		recordCreateCollectionTime := make(map[int64]map[string]*pb.CollectionInfo)
		repeatedCollectionID := make(map[int64]struct{})
		repeatedCollectionName := make(map[int64][]string)
		for _, info := range existedCollectionInfos {
			collectionName := info.Schema.GetName()
			createTime := info.CreateTime
			dbCollections := recordCreateCollectionTime[info.GetDbId()]
			if dbCollections == nil {
				dbCollections = map[string]*pb.CollectionInfo{
					collectionName: info,
				}
				recordCreateCollectionTime[info.GetDbId()] = dbCollections
				continue
			}
			lastCollectionInfo, recordOK := dbCollections[collectionName]
			if recordOK {
				if createTime > lastCollectionInfo.CreateTime {
					repeatedCollectionID[lastCollectionInfo.ID] = struct{}{}
					dbCollections[collectionName] = info
				} else {
					repeatedCollectionID[info.ID] = struct{}{}
				}
				repeatedCollectionName[info.GetDbId()] = append(repeatedCollectionName[info.GetDbId()], collectionName)
			} else {
				dbCollections[collectionName] = info
			}
		}

		// for the dropped collection when cdc is down, like:
		// 1. create collection and cdc server is healthy
		// 2. cdc server is down
		// 3. drop collection, or drop and create the same collection
		// 4. cdc server restart
		// 5. create the same collection again
		// TODO: it may be different the `metaOp.GetAllDroppedObj()`
		reader.channelManager.AddDroppedCollection(lo.Keys(repeatedCollectionID))

		for _, info := range existedCollectionInfos {
			if _, ok := repeatedCollectionID[info.ID]; ok {
				readerLog.Info("skip to start to read collection", zap.String("name", info.Schema.Name), zap.Int64("collection_id", info.ID))
				continue
			}
			dbInfo := reader.metaOp.GetDatabaseInfoForCollection(ctx, info.ID)
			forceStartPosition, shouldRead := reader.shouldReadFunc(&dbInfo, info)
			if !shouldRead {
				readerLog.Info("the collection is not in the watch list", zap.String("name", info.Schema.Name), zap.Int64("collection_id", info.ID))
				continue
			}
			collectionSeekPositionMap := reader.channelSeekPositions[info.ID]
			seekPositions := make([]*msgpb.MsgPosition, 0)
			appendSeekPositionFromStartPosition := func() {
				for _, v := range info.StartPositions {
					seekPositions = append(seekPositions, &msgstream.MsgPosition{
						ChannelName: v.GetKey(),
						MsgID:       v.GetData(),
						Timestamp:   info.CreateTime,
					})
				}
			}
			if collectionSeekPositionMap != nil {
				seekPositions = lo.Values(collectionSeekPositionMap)
			} else if dbCollections, ok := repeatedCollectionName[info.DbId]; ok && lo.Contains(dbCollections, info.Schema.Name) {
				log.Warn("server warn: find the repeated collection, the latest collection will use the collection start position.", zap.String("name", info.Schema.Name), zap.Int64("collection_id", info.ID))
				appendSeekPositionFromStartPosition()
			} else if forceStartPosition {
				log.Info("server info: the collection will use the collection start position.", zap.String("name", info.Schema.Name), zap.Int64("collection_id", info.ID))
				appendSeekPositionFromStartPosition()
			}
			readerLog.Info("exist collection",
				zap.String("name", info.Schema.Name),
				zap.Int64("collection_id", info.ID),
				zap.String("state", info.State.String()),
				zap.Strings("seek_channels", lo.Map(seekPositions, func(v *msgpb.MsgPosition, _ int) string {
					return v.GetChannelName()
				})),
			)
			if err := reader.channelManager.StartReadCollection(ctx, &dbInfo, info, seekPositions); err != nil {
				readerLog.Warn("fail to start to replicate the collection data", zap.Any("collection", info), zap.Error(err))
				reader.sendError(err)
			}
			reader.replicateCollectionMap.Store(info.ID, info)
		}
		_, err = reader.metaOp.GetAllPartition(ctx, func(info *pb.PartitionInfo) bool {
			if _, ok := repeatedCollectionID[info.CollectionId]; ok {
				readerLog.Info("skip to start to add partition",
					zap.String("name", info.PartitionName),
					zap.Int64("partition_id", info.PartitionID),
					zap.Int64("collection_id", info.CollectionId))
				return true
			}
			var collectionName string
			retryErr := retry.Do(ctx, func() error {
				collectionName = reader.metaOp.GetCollectionNameByID(ctx, info.CollectionId)
				if collectionName != "" {
					return nil
				}
				return errors.Newf("fail to get collection name by id %d", info.CollectionId)
			}, reader.retryOptions...)
			if retryErr != nil || collectionName == "" {
				readerLog.Warn("empty collection name", zap.Int64("collection_id", info.CollectionId), zap.Error(retryErr))
				return true
			}
			if IsDroppedObject(collectionName) {
				readerLog.Info("the collection has been dropped", zap.Int64("collection_id", info.CollectionId))
				return true
			}
			tmpCollectionInfo := &pb.CollectionInfo{
				ID: info.CollectionId,
				Schema: &schemapb.CollectionSchema{
					Name: collectionName,
				},
			}
			dbInfo := reader.metaOp.GetDatabaseInfoForCollection(ctx, tmpCollectionInfo.ID)
			_, shouldRead := reader.shouldReadFunc(&dbInfo, tmpCollectionInfo)
			if !shouldRead {
				readerLog.Info("the collection is not in the watch list", zap.String("collection_name", collectionName), zap.String("partition_name", info.PartitionName))
				return true
			}
			readerLog.Info("exist partition",
				zap.String("name", info.PartitionName),
				zap.Int64("partition_id", info.PartitionID),
				zap.String("collection_name", collectionName),
				zap.Int64("collection_id", info.CollectionId))
			err := reader.channelManager.AddPartition(ctx, &dbInfo, tmpCollectionInfo, info)
			if err != nil {
				readerLog.Warn("fail to add partition", zap.String("collection_name", collectionName), zap.String("partition_name", info.PartitionName), zap.Error(err))
				reader.sendError(err)
			}
			return false
		})
		if err != nil {
			readerLog.Warn("get all partition failed", zap.Error(err))
			reader.sendError(err)
		}
		readerLog.Info("has started to read collection and partition")
		reader.metaOp.StartWatch()
	})
}

func (reader *CollectionReader) sendError(err error) {
	select {
	case reader.errChan <- err:
		log.Info("send the error", zap.String("id", reader.id), zap.Error(err))
	default:
		log.Info("skip the error, because it will quit soon", zap.String("id", reader.id), zap.Error(err))
	}
}

func (reader *CollectionReader) QuitRead(ctx context.Context) {
	reader.quitOnce.Do(func() {
		reader.replicateCollectionMap.Range(func(_ int64, value *pb.CollectionInfo) bool {
			err := reader.channelManager.StopReadCollection(ctx, value)
			if err != nil {
				log.Warn("fail to stop read collection", zap.String("id", reader.id), zap.Error(err))
			}
			return true
		})
		reader.metaOp.UnsubscribeEvent(reader.id, api.CollectionEventType)
		reader.metaOp.UnsubscribeEvent(reader.id, api.PartitionEventType)
		reader.sendError(nil)
	})
}

func (reader *CollectionReader) ErrorChan() <-chan error {
	return reader.errChan
}
