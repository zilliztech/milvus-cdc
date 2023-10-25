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

package reader

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/pb"
	"github.com/zilliztech/milvus-cdc/core/util"
)

const (
	AllCollection = "*"
)

type CollectionInfo struct {
	collectionName string
	positions      map[string]*commonpb.KeyDataPair
}

type ShouldReadFunc func(*pb.CollectionInfo) bool

var _ api.Reader = (*CollectionReader)(nil)

type CollectionReader struct {
	api.DefaultReader

	id                     string
	channelManager         api.ChannelManager
	metaOp                 api.MetaOp
	channelSeekPositions   map[string]*msgpb.MsgPosition
	replicateCollectionMap util.Map[int64, *pb.CollectionInfo]
	replicateChannelMap    util.Map[string, struct{}]
	replicateChannelChan   chan string
	shouldReadFunc         ShouldReadFunc
	startOnce              sync.Once
	quitOnce               sync.Once
}

func NewCollectionReader(id string, channelManager api.ChannelManager, metaOp api.MetaOp, seekPosition map[string]*msgpb.MsgPosition, shouldReadFunc ShouldReadFunc) (api.Reader, error) {
	reader := &CollectionReader{
		id:                   id,
		channelManager:       channelManager,
		metaOp:               metaOp,
		channelSeekPositions: seekPosition,
		shouldReadFunc:       shouldReadFunc,
		replicateChannelChan: make(chan string, 10),
	}
	return reader, nil
}

func (reader *CollectionReader) StartRead(ctx context.Context) {
	reader.startOnce.Do(func() {
		reader.metaOp.SubscribeCollectionEvent(reader.id, func(info *pb.CollectionInfo) bool {
			log.Info("has watched to read collection", zap.String("name", info.Schema.Name))
			if !reader.shouldReadFunc(info) {
				return false
			}
			startPositions := make([]*msgpb.MsgPosition, 0)
			for _, v := range info.StartPositions {
				startPositions = append(startPositions, &msgstream.MsgPosition{
					ChannelName: v.GetKey(),
					MsgID:       v.GetData(),
				})
			}
			if err := reader.channelManager.StartReadCollection(ctx, info, startPositions); err != nil {
				log.Warn("fail to start to replicate the collection data in the watch process", zap.Int64("id", info.ID), zap.Error(err))
			}
			reader.replicateCollectionMap.Store(info.ID, info)
			log.Info("has started to read collection", zap.String("name", info.Schema.Name))
			return true
		})
		reader.metaOp.SubscribePartitionEvent(reader.id, func(info *pb.PartitionInfo) bool {
			collectionName := reader.metaOp.GetCollectionNameByID(ctx, info.CollectionID)
			if collectionName == "" {
				log.Info("the collection name is empty", zap.Int64("collection_id", info.CollectionID), zap.String("partition_name", info.PartitionName))
				return true
			}
			tmpCollectionInfo := &pb.CollectionInfo{
				ID: info.CollectionID,
				Schema: &schemapb.CollectionSchema{
					Name: collectionName,
				},
			}
			if !reader.shouldReadFunc(tmpCollectionInfo) {
				return true
			}

			var err error
			err = retry.Do(ctx, func() error {
				err = reader.channelManager.AddPartition(ctx, tmpCollectionInfo, info)
				return err
			}, retry.Sleep(time.Second))
			if err != nil {
				log.Panic("fail to add partition", zap.String("collection_name", collectionName), zap.String("partition_name", info.PartitionName), zap.Error(err))
			}
			return false
		})
		reader.metaOp.WatchCollection(ctx, nil)
		reader.metaOp.WatchPartition(ctx, nil)

		existedCollectionInfos, err := reader.metaOp.GetAllCollection(ctx, func(info *pb.CollectionInfo) bool {
			return !reader.shouldReadFunc(info)
		})
		if err != nil {
			log.Warn("get all collection failed", zap.Error(err))
		}
		seekPositions := lo.Values(reader.channelSeekPositions)
		for _, info := range existedCollectionInfos {
			log.Info("exist collection", zap.String("name", info.Schema.Name))
			if err := reader.channelManager.StartReadCollection(ctx, info, seekPositions); err != nil {
				log.Warn("fail to start to replicate the collection data", zap.Int64("id", info.ID), zap.Error(err))
			}
			reader.replicateCollectionMap.Store(info.ID, info)
		}
		_, err = reader.metaOp.GetAllPartition(ctx, func(info *pb.PartitionInfo) bool {
			collectionName := reader.metaOp.GetCollectionNameByID(ctx, info.CollectionID)
			if collectionName == "" {
				log.Info("the collection name is empty", zap.Int64("collection_id", info.CollectionID), zap.String("partition_name", info.PartitionName))
				return true
			}
			tmpCollectionInfo := &pb.CollectionInfo{
				ID: info.CollectionID,
				Schema: &schemapb.CollectionSchema{
					Name: collectionName,
				},
			}
			if !reader.shouldReadFunc(tmpCollectionInfo) {
				log.Info("the collection is not in the watch list", zap.String("collection_name", collectionName), zap.String("partition_name", info.PartitionName))
				return true
			}
			var err error
			err = retry.Do(ctx, func() error {
				err = reader.channelManager.AddPartition(ctx, tmpCollectionInfo, info)
				return err
			}, retry.Sleep(time.Second))
			if err != nil {
				log.Panic("fail to add partition", zap.String("collection_name", collectionName), zap.String("partition_name", info.PartitionName), zap.Error(err))
			}
			return false
		})
		if err != nil {
			log.Warn("get all partition failed", zap.Error(err))
		}
	})
}

func (reader *CollectionReader) QuitRead(ctx context.Context) {
	reader.quitOnce.Do(func() {
		reader.replicateCollectionMap.Range(func(_ int64, value *pb.CollectionInfo) bool {
			err := reader.channelManager.StopReadCollection(ctx, value)
			if err != nil {
				log.Warn("fail to stop read collection", zap.Error(err))
			}
			return true
		})
		reader.metaOp.UnsubscribeEvent(reader.id, api.CollectionEventType)
		reader.metaOp.UnsubscribeEvent(reader.id, api.PartitionEventType)
	})
}
