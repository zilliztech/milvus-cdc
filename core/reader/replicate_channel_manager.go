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
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/sasha-s/go-deadlock"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"

	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/log"
	"github.com/zilliztech/milvus-cdc/core/model"
	"github.com/zilliztech/milvus-cdc/core/msgdispatcher"
	"github.com/zilliztech/milvus-cdc/core/pb"
	"github.com/zilliztech/milvus-cdc/core/util"
)

var _ api.ChannelManager = (*replicateChannelManager)(nil)

var replicatePool = conc.NewPool[struct{}](10, conc.WithExpiryDuration(time.Minute))

type replicateChannelManager struct {
	replicateCtx         context.Context
	replicateID          string
	streamDispatchClient msgdispatcher.Client
	streamCreator        StreamCreator
	targetClient         api.TargetAPI
	metaOp               api.MetaOp
	replicateMeta        api.ReplicateMeta

	retryOptions          []retry.Option
	startReadRetryOptions []retry.Option
	messageBufferSize     int
	ttInterval            int

	channelLock    deadlock.RWMutex
	channelMapping *util.ChannelMapping
	// the key is source/target milvus channel,
	// when the source is equal or more than the target, the key is source channel, otherwise is target channel
	channelHandlerMap map[string]*replicateChannelHandler
	// the key is the handler map value channel
	// when the source is equal or more than the target, the key is target channel, otherwise is source channel
	channelForwardMap map[string]int
	// the key is collection id, and the value is the map, which key is pchannel name and the value is the handler map key
	sourcePChannelKeyMap map[int64]map[string]string

	collectionLock       deadlock.RWMutex
	replicateCollections map[int64]chan struct{}

	partitionLock       deadlock.Mutex
	replicatePartitions map[int64]map[int64]chan struct{}

	apiEventChan            chan *api.ReplicateAPIEvent
	forwardReplicateChannel chan string

	msgPackCallback func(string, *msgstream.MsgPack)

	droppedCollections util.Map[int64, struct{}]
	droppedPartitions  util.Map[int64, struct{}]

	addCollectionLock *deadlock.RWMutex
	addCollectionCnt  *int

	downstream string
}

func NewReplicateChannelManager(
	dispatchClient msgdispatcher.Client,
	factory msgstream.Factory,
	client api.TargetAPI,
	readConfig config.ReaderConfig,
	metaOp api.MetaOp,
	replicateMeta api.ReplicateMeta,
	msgPackCallback func(string, *msgstream.MsgPack),
	downstream string,
) (api.ChannelManager, error) {
	return &replicateChannelManager{
		replicateID:          readConfig.ReplicateID,
		streamDispatchClient: dispatchClient,
		streamCreator:        NewDisptachClientStreamCreator(factory, dispatchClient),
		targetClient:         client,
		metaOp:               metaOp,
		replicateMeta:        replicateMeta,
		retryOptions:         util.GetRetryOptions(readConfig.Retry),
		startReadRetryOptions: util.GetRetryOptions(config.RetrySettings{
			RetryTimes:  readConfig.Retry.RetryTimes,
			InitBackOff: readConfig.Retry.InitBackOff,
			MaxBackOff:  readConfig.Retry.InitBackOff,
		}),
		messageBufferSize:       readConfig.MessageBufferSize,
		ttInterval:              readConfig.TTInterval,
		channelMapping:          util.NewChannelMapping(readConfig.SourceChannelNum, readConfig.TargetChannelNum),
		channelHandlerMap:       make(map[string]*replicateChannelHandler),
		channelForwardMap:       make(map[string]int),
		sourcePChannelKeyMap:    make(map[int64]map[string]string),
		replicateCollections:    make(map[int64]chan struct{}),
		replicatePartitions:     make(map[int64]map[int64]chan struct{}),
		apiEventChan:            make(chan *api.ReplicateAPIEvent, 10),
		forwardReplicateChannel: make(chan string),
		msgPackCallback:         msgPackCallback,

		addCollectionLock: &deadlock.RWMutex{},
		addCollectionCnt:  new(int),
		downstream:        downstream,
	}, nil
}

func (r *replicateChannelManager) SetCtx(ctx context.Context) {
	r.replicateCtx = ctx
}

func (r *replicateChannelManager) getCtx() context.Context {
	if r.replicateCtx == nil {
		return context.Background()
	}
	return r.replicateCtx
}

func IsCollectionNotFoundError(err error) bool {
	return strings.Contains(err.Error(), "collection not found") || strings.Contains(err.Error(), "can't find collection")
}

func IsDatabaseNotFoundError(err error) bool {
	return err == util.NotFoundDatabase
}

func (r *replicateChannelManager) AddDroppedCollection(ids []int64) {
	for _, id := range ids {
		r.droppedCollections.Store(id, struct{}{})
	}
	log.Info("has added dropped collection", zap.Int64s("ids", ids))
}

func (r *replicateChannelManager) AddDroppedPartition(ids []int64) {
	for _, id := range ids {
		r.droppedPartitions.Delete(id)
	}
	log.Info("has removed dropped partitions", zap.Int64s("ids", ids))
}

func (r *replicateChannelManager) startReadCollectionForKafka(ctx context.Context, info *pb.CollectionInfo, sourceDBInfo *model.DatabaseInfo) (*model.CollectionInfo, error) {
	r.collectionLock.RLock()
	_, ok := r.replicateCollections[info.ID]
	r.collectionLock.RUnlock()
	if ok {
		return nil, errors.Newf("the collection has been replicated, wait it [collection name: %s] to drop...", info.Schema.Name)
	}

	// send api event when the collection is not replicated and ctx is not done
	if err := r.sendCreateCollectionEvent(ctx, info, sourceDBInfo); err != nil {
		return nil, err
	}

	// get targetCollectionInfo from source
	var targetInfo *model.CollectionInfo
	partitions := make(map[string]int64)
	partitionInfos, err := r.metaOp.GetAllPartition(ctx, func(partitionInfo *pb.PartitionInfo) bool {
		return partitionInfo.CollectionId != info.ID
	})
	if err != nil {
		log.Warn("failed to get partition info", zap.Error(err))
		return nil, err
	}

	for _, partitionInfo := range partitionInfos {
		partitions[partitionInfo.PartitionName] = partitionInfo.PartitionID
	}
	targetInfo = &model.CollectionInfo{
		CollectionID:   info.ID,
		CollectionName: info.Schema.Name,
		DatabaseName:   sourceDBInfo.Name,
		VChannels:      info.VirtualChannelNames,
		Partitions:     partitions,
	}
	// source collection has dropped
	if info.State == pb.CollectionState_CollectionDropped || info.State == pb.CollectionState_CollectionDropping {
		targetInfo.Dropped = true
		log.Info("the collection is dropped in the source instance and it's existed in the target instance", zap.String("collection_name", info.Schema.Name))
	}

	return targetInfo, nil
}

func (r *replicateChannelManager) startReadCollectionForMilvus(ctx context.Context, info *pb.CollectionInfo, sourceDBInfo *model.DatabaseInfo) (*model.CollectionInfo, error) {
	var err error
	retryErr := retry.Do(ctx, func() error {
		_, err = r.targetClient.GetCollectionInfo(ctx, info.Schema.GetName(), sourceDBInfo.Name)
		if err != nil && !IsCollectionNotFoundError(err) && !IsDatabaseNotFoundError(err) {
			return err
		}
		r.collectionLock.RLock()
		_, ok := r.replicateCollections[info.ID]
		r.collectionLock.RUnlock()
		if ok {
			return errors.Newf("the collection has been replicated, wait it [collection name: %s] to drop...", info.Schema.Name)
		}
		// collection not found will exit the retry
		return nil
	}, r.startReadRetryOptions...)

	if retryErr != nil {
		return nil, retryErr
	}

	if err != nil {
		// the collection is not existed in the target and source collection has dropped, skip it
		if info.State == pb.CollectionState_CollectionDropped || info.State == pb.CollectionState_CollectionDropping {
			r.droppedCollections.Store(info.ID, struct{}{})
			log.Info("the collection is dropped in the target instance",
				zap.Int64("collection_id", info.ID), zap.String("collection_name", info.Schema.Name))
			return nil, nil
		}
		if IsDatabaseNotFoundError(err) {
			log.Panic("the database has been dropped but the collection is existed in the source instance",
				zap.String("collection_name", info.Schema.Name))
			return nil, nil
		}
		err = r.sendCreateCollectionEvent(ctx, info, sourceDBInfo)
		if err != nil {
			return nil, err
		}
	}

	var targetInfo *model.CollectionInfo
	err = retry.Do(ctx, func() error {
		targetInfo, err = r.targetClient.GetCollectionInfo(ctx, info.Schema.Name, sourceDBInfo.Name)
		return err
	}, r.startReadRetryOptions...)
	if err != nil {
		log.Warn("failed to get target collection info", zap.Error(err))
		return nil, err
	}
	log.Info("success to get the collection info in the target instance", zap.String("collection_name", targetInfo.CollectionName))
	if info.State == pb.CollectionState_CollectionDropped || info.State == pb.CollectionState_CollectionDropping {
		targetInfo.Dropped = true
		log.Info("the collection is dropped in the source instance and it's existed in the target instance", zap.String("collection_name", info.Schema.Name))
	}

	return targetInfo, nil
}

func (r *replicateChannelManager) sendCreateCollectionEvent(ctx context.Context, info *pb.CollectionInfo, sourceDBInfo *model.DatabaseInfo) error {
	select {
	case <-ctx.Done():
		log.Warn("context is done in the start read collection")
		return ctx.Err()
	default:
		// TODO fubang should give a error when the collection shard num is more than the target dml channel num
		r.apiEventChan <- &api.ReplicateAPIEvent{
			EventType:      api.ReplicateCreateCollection,
			TaskID:         util.GetTaskIDFromCtx(ctx),
			CollectionInfo: info,
			ReplicateInfo: &commonpb.ReplicateInfo{
				IsReplicate:  true,
				MsgTimestamp: info.CreateTime,
			},
			ReplicateParam: api.ReplicateParam{Database: sourceDBInfo.Name},
		}
	}
	return nil
}

func (r *replicateChannelManager) StartReadCollection(ctx context.Context, db *model.DatabaseInfo, info *pb.CollectionInfo, seekPositions []*msgpb.MsgPosition, channelStartTsMap map[string]uint64) error {
	r.addCollectionLock.Lock()
	*r.addCollectionCnt++
	r.addCollectionLock.Unlock()

	defer func() {
		r.addCollectionLock.Lock()
		*r.addCollectionCnt--
		r.addCollectionLock.Unlock()
	}()

	if r.isDroppedCollection(info.ID) {
		log.Info("the collection is dropped", zap.Int64("collection_id", info.ID))
		return nil
	}

	var targetInfo *model.CollectionInfo
	var err error
	if r.downstream == "milvus" {
		targetInfo, err = r.startReadCollectionForMilvus(ctx, info, db)
	} else if r.downstream == "kafka" {
		targetInfo, err = r.startReadCollectionForKafka(ctx, info, db)
	}

	if err != nil {
		log.Warn("failed to start read collection", zap.Error(err), zap.String("downstream", r.downstream))
		return err
	}
	if targetInfo == nil {
		return nil
	}

	getSeekPosition := func(channelName string) *msgpb.MsgPosition {
		for _, seekPosition := range seekPositions {
			if seekPosition.ChannelName == channelName {
				return seekPosition
			}
		}
		return nil
	}
	getStartTs := func(channelName string) uint64 {
		if ts, ok := channelStartTsMap[channelName]; ok {
			return ts
		}
		return 0
	}
	taskID := util.GetTaskIDFromCtx(ctx)
	r.collectionLock.Lock()
	if _, ok := r.replicateCollections[info.ID]; ok {
		r.collectionLock.Unlock()
		log.Info("the collection is already replicated", zap.String("collection_name", info.Schema.Name))
		return nil
	}

	targetMsgCount := len(info.StartPositions)
	barrier := NewBarrier(targetMsgCount, func(msgTs uint64, b *Barrier) {
		select {
		case <-b.CloseChan:
		case r.apiEventChan <- &api.ReplicateAPIEvent{
			EventType:      api.ReplicateDropCollection,
			CollectionInfo: info,
			ReplicateInfo: &commonpb.ReplicateInfo{
				IsReplicate:  true,
				MsgTimestamp: msgTs,
			},
			ReplicateParam: api.ReplicateParam{Database: targetInfo.DatabaseName},
			TaskID:         taskID,
			MsgID:          api.GetDropCollectionMsgID(info.ID),
		}:
			r.droppedCollections.Store(info.ID, struct{}{})
			for _, name := range info.PhysicalChannelNames {
				r.stopReadChannel(name, info.ID)
			}
		}
		r.collectionLock.Lock()
		delete(r.replicateCollections, info.ID)
		r.collectionLock.Unlock()
	}, func(vchannel string, m msgstream.TsMsg) {
		dropCollectionMsg, ok := m.(*msgstream.DropCollectionMsg)
		if !ok {
			log.Panic("the message is not drop collection message", zap.Any("msg", m))
		}
		msgID := api.GetDropCollectionMsgID(dropCollectionMsg.CollectionID)
		_, err := r.replicateMeta.UpdateTaskDropCollectionMsg(ctx, api.TaskDropCollectionMsg{
			Base: api.BaseTaskMsg{
				TaskID:         taskID,
				MsgID:          msgID,
				TargetChannels: info.VirtualChannelNames,
				ReadyChannels:  []string{vchannel},
			},
			DatabaseName:   dropCollectionMsg.DbName,
			CollectionName: dropCollectionMsg.CollectionName,
			DropTS:         dropCollectionMsg.EndTs(),
		})
		if err != nil {
			log.Panic("failed to update task drop collection msg", zap.Error(err))
		}
	})
	r.replicateCollections[info.ID] = barrier.CloseChan
	r.collectionLock.Unlock()

	var successChannels []string
	var channelHandlers []*replicateChannelHandler
	err = ForeachChannel(info.VirtualChannelNames, targetInfo.VChannels, func(sourceVChannel, targetVChannel string) error {
		sourcePChannel := funcutil.ToPhysicalChannel(sourceVChannel)
		targetPChannel := funcutil.ToPhysicalChannel(targetVChannel)
		channelHandler, err := r.startReadChannel(ctx, &model.SourceCollectionInfo{
			PChannel:     sourcePChannel,
			VChannel:     sourceVChannel,
			CollectionID: info.ID,
			SeekPosition: getSeekPosition(sourcePChannel),
			StartTs:      getStartTs(sourcePChannel),
		}, &model.TargetCollectionInfo{
			DatabaseName:         targetInfo.DatabaseName,
			CollectionID:         targetInfo.CollectionID,
			CollectionName:       info.Schema.Name,
			PartitionInfo:        targetInfo.Partitions,
			PChannel:             targetPChannel,
			VChannel:             targetVChannel,
			BarrierChan:          model.NewOnceWriteChan(barrier.BarrierSignalChan),
			PartitionBarrierChan: make(map[int64]*model.OnceWriteChan[*model.BarrierSignal]),
			Dropped:              targetInfo.Dropped,
			DroppedPartition:     make(map[int64]struct{}),
		})
		if err != nil {
			log.Warn("start read channel failed", zap.String("channel", sourcePChannel), zap.Int64("collection_id", info.ID), zap.Error(err))
			for _, channel := range successChannels {
				r.stopReadChannel(channel, info.ID)
			}
			return err
		}
		if channelHandler != nil {
			channelHandlers = append(channelHandlers, channelHandler)
		}
		successChannels = append(successChannels, sourcePChannel)
		log.Info("start read channel in the manager",
			zap.Bool("nil_handler", channelHandler == nil),
			zap.String("channel", sourcePChannel),
			zap.String("target_channel", targetPChannel),
			zap.Int64("collection_id", info.ID))
		return nil
	})

	if err == nil {
		for _, channelHandler := range channelHandlers {
			channelHandler.startReadChannel()
			log.Info("start read the source channel", zap.String("channel_name", channelHandler.sourcePChannel))
		}
	}
	return err
}

func GetVChannelByPChannel(pChannel string, vChannels []string) string {
	for _, vChannel := range vChannels {
		if strings.Contains(vChannel, pChannel) {
			return vChannel
		}
	}
	return ""
}

func ForeachChannel(sourcePChannels, targetPChannels []string, f func(sourcePChannel, targetPChannel string) error) error {
	if len(sourcePChannels) != len(targetPChannels) {
		return errors.New("the lengths of source and target channels are not equal")
	}
	sources := make([]string, len(sourcePChannels))
	targets := make([]string, len(targetPChannels))
	copy(sources, sourcePChannels)
	copy(targets, targetPChannels)
	sort.Strings(sources)
	sort.Strings(targets)

	for i, source := range sources {
		if err := f(source, targets[i]); err != nil {
			return err
		}
	}
	return nil
}

func (r *replicateChannelManager) AddPartition(ctx context.Context, dbInfo *model.DatabaseInfo, collectionInfo *pb.CollectionInfo, partitionInfo *pb.PartitionInfo) error {
	var handlers []*replicateChannelHandler
	collectionID := collectionInfo.ID
	taskID := util.GetTaskIDFromCtx(ctx)
	partitionLog := log.With(zap.Int64("partition_id", partitionInfo.PartitionID), zap.Int64("collection_id", collectionID),
		zap.String("collection_name", collectionInfo.Schema.Name), zap.String("partition_name", partitionInfo.PartitionName),
		zap.String("task_id", taskID),
	)
	if dbInfo.Dropped {
		partitionLog.Info("the database has been dropped when add partition")
		return nil
	}
	if r.isDroppedPartition(partitionInfo.PartitionID) {
		partitionLog.Info("the partition has been dropped when add partition")
		return nil
	}

	_ = retry.Do(ctx, func() error {
		if _, dropped := r.droppedCollections.Load(collectionID); dropped {
			partitionLog.Info("the collection is dropped when add partition")
			return nil
		}
		r.channelLock.RLock()
		for _, handler := range r.channelHandlerMap {
			handler.recordLock.RLock()
			if _, ok := handler.collectionRecords[collectionID]; ok {
				handlers = append(handlers, handler)
			}
			handler.recordLock.RUnlock()
		}
		r.channelLock.RUnlock()
		if len(handlers) == 0 {
			partitionLog.Info("waiting handler")
			return errors.New("no handler found")
		}
		return nil
	}, r.retryOptions...)
	if _, dropped := r.droppedCollections.Load(collectionID); dropped {
		return nil
	}

	if len(handlers) == 0 {
		partitionLog.Warn("no handler found")
		return errors.New("no handler found")
	}

	firstHandler := handlers[0]
	targetInfo, err := firstHandler.getCollectionTargetInfo(collectionID)
	if err != nil {
		return err
	}
	if targetInfo == nil || targetInfo.Dropped {
		partitionLog.Info("the collection is dropping or dropped, skip the partition")
		return nil
	}
	firstHandler.recordLock.RLock()
	partitionRecord := targetInfo.PartitionInfo
	_, ok := partitionRecord[partitionInfo.PartitionName]
	firstHandler.recordLock.RUnlock()
	if !ok {
		if partitionInfo.State == pb.PartitionState_PartitionDropping ||
			partitionInfo.State == pb.PartitionState_PartitionDropped {
			r.droppedPartitions.Store(partitionInfo.PartitionID, struct{}{})
			partitionLog.Info("the partition is dropped in the source and target")
			return nil
		}
		select {
		case r.apiEventChan <- &api.ReplicateAPIEvent{
			EventType:      api.ReplicateCreatePartition,
			CollectionInfo: collectionInfo,
			PartitionInfo:  partitionInfo,
			ReplicateInfo: &commonpb.ReplicateInfo{
				IsReplicate:  true,
				MsgTimestamp: partitionInfo.PartitionCreatedTimestamp,
			},
			ReplicateParam: api.ReplicateParam{Database: dbInfo.Name},
			TaskID:         taskID,
		}:
		case <-ctx.Done():
			partitionLog.Warn("context is done when adding partition")
			return ctx.Err()
		}
	}
	partitionLog.Info("start to add partition", zap.Int("num", len(handlers)))
	barrier := NewBarrier(len(handlers), func(msgTs uint64, b *Barrier) {
		select {
		case <-b.CloseChan:
			log.Info("close barrier")
		case r.apiEventChan <- &api.ReplicateAPIEvent{
			EventType:      api.ReplicateDropPartition,
			CollectionInfo: collectionInfo,
			PartitionInfo:  partitionInfo,
			ReplicateInfo: &commonpb.ReplicateInfo{
				IsReplicate:  true,
				MsgTimestamp: msgTs,
			},
			ReplicateParam: api.ReplicateParam{Database: dbInfo.Name},
			TaskID:         taskID,
			MsgID:          api.GetDropPartitionMsgID(collectionID, partitionInfo.PartitionID),
		}:
			r.droppedPartitions.Store(partitionInfo.PartitionID, struct{}{})
			for _, handler := range handlers {
				handler.RemovePartitionInfo(collectionID, partitionInfo.PartitionName, partitionInfo.PartitionID)
			}
		}
	}, func(vchannel string, m msgstream.TsMsg) {
		dropPartitionMsg, ok := m.(*msgstream.DropPartitionMsg)
		if !ok {
			log.Panic("the message is not drop partition message", zap.Any("msg", m))
		}
		msgID := api.GetDropPartitionMsgID(collectionID, partitionInfo.PartitionID)
		_, err := r.replicateMeta.UpdateTaskDropPartitionMsg(ctx, api.TaskDropPartitionMsg{
			Base: api.BaseTaskMsg{
				TaskID:         taskID,
				MsgID:          msgID,
				TargetChannels: collectionInfo.VirtualChannelNames,
				ReadyChannels:  []string{vchannel},
			},
			DatabaseName:   dropPartitionMsg.DbName,
			CollectionName: dropPartitionMsg.CollectionName,
			PartitionName:  dropPartitionMsg.PartitionName,
			DropTS:         dropPartitionMsg.EndTs(),
		})
		if err != nil {
			log.Panic("failed to update task drop partition msg", zap.Error(err))
		}
	})
	r.partitionLock.Lock()
	if _, ok := r.replicatePartitions[collectionID]; !ok {
		r.replicatePartitions[collectionID] = make(map[int64]chan struct{})
	}
	if _, ok := r.replicatePartitions[collectionID][partitionInfo.PartitionID]; ok {
		partitionLog.Info("the partition is already replicated", zap.Int64("partition_id", partitionInfo.PartitionID))
		r.partitionLock.Unlock()
		return nil
	}
	r.replicatePartitions[collectionID][partitionInfo.PartitionID] = barrier.CloseChan
	r.partitionLock.Unlock()
	for _, handler := range handlers {
		err = handler.AddPartitionInfo(taskID, collectionInfo, partitionInfo, barrier.BarrierSignalChan)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *replicateChannelManager) StopReadCollection(ctx context.Context, info *pb.CollectionInfo) error {
	for _, channel := range info.GetPhysicalChannelNames() {
		handler := r.stopReadChannel(channel, info.ID)
		if handler == nil {
			continue
		}
		handler.Close()
	}
	r.collectionLock.Lock()
	closeChan, ok := r.replicateCollections[info.ID]
	if ok {
		select {
		case <-closeChan:
		default:
			close(closeChan)
		}
		delete(r.replicateCollections, info.ID)
	}
	r.collectionLock.Unlock()
	r.partitionLock.Lock()
	partitions, ok := r.replicatePartitions[info.ID]
	if ok {
		for _, closeChan := range partitions {
			select {
			case <-closeChan:
			default:
				close(closeChan)
			}
		}
		delete(r.replicatePartitions, info.ID)
	}
	r.partitionLock.Unlock()
	return nil
}

func (r *replicateChannelManager) GetChannelChan() <-chan string {
	for {
		c := GetTSManager().GetTargetChannelChan(r.replicateID)
		if c != nil {
			return c
		}
		time.Sleep(time.Second)
	}
}

func (r *replicateChannelManager) GetMsgChan(pChannel string) <-chan *api.ReplicateMsg {
	return GetTSManager().GetTargetMsgChan(r.replicateID, pChannel)
}

func (r *replicateChannelManager) GetEventChan() <-chan *api.ReplicateAPIEvent {
	return r.apiEventChan
}

func (r *replicateChannelManager) GetChannelLatestMsgID(ctx context.Context, channelName string) ([]byte, error) {
	return r.streamCreator.GetChannelLatestMsgID(ctx, channelName)
}

// startReadChannel start read channel
// sourcePChannel: source milvus channel name, collectionID: source milvus collection id, startPosition: start position of the source milvus collection
// targetInfo: target collection info, it will be used to replace the message info in the source milvus channel
func (r *replicateChannelManager) startReadChannel(ctx context.Context, sourceInfo *model.SourceCollectionInfo, targetInfo *model.TargetCollectionInfo) (*replicateChannelHandler, error) {
	r.channelLock.Lock()
	defer r.channelLock.Unlock()

	channelLog := log.With(
		zap.Int64("collection_id", sourceInfo.CollectionID),
		zap.String("collection_name", targetInfo.CollectionName),
		zap.String("source_channel", sourceInfo.PChannel),
		zap.String("target_channel", targetInfo.PChannel),
	)
	channelMappingKey := r.channelMapping.GetMapKey(sourceInfo.PChannel, targetInfo.PChannel)
	channelMappingValue := r.channelMapping.GetMapValue(sourceInfo.PChannel, targetInfo.PChannel)
	taskID := util.GetTaskIDFromCtx(ctx)

	channelHandler, ok := r.channelHandlerMap[channelMappingKey]
	if !ok {
		var err error
		channelHandler, err = initReplicateChannelHandler(r.getCtx(), sourceInfo, targetInfo, r.targetClient, r.metaOp, r.apiEventChan, &model.HandlerOpts{
			MessageBufferSize: r.messageBufferSize,
			TTInterval:        r.ttInterval,
			RetryOptions:      r.retryOptions,
		}, r.streamCreator,
			r.downstream,
			channelMappingKey == sourceInfo.PChannel,
			taskID,
		)
		if err != nil {
			channelLog.Warn("init replicate channel handler failed", zap.Error(err))
			return nil, err
		}
		channelHandler.addCollectionCnt = r.addCollectionCnt
		channelHandler.addCollectionLock = r.addCollectionLock
		channelHandler.msgPackCallback = r.msgPackCallback
		channelHandler.forwardMsgFunc = r.forwardMsg
		channelHandler.isDroppedCollection = r.isDroppedCollection
		channelHandler.isDroppedPartition = r.isDroppedPartition
		channelHandler.replicateID = r.replicateID
		diffValueForKey := r.channelMapping.CheckKeyNotExist(sourceInfo.PChannel, targetInfo.PChannel)

		if !diffValueForKey {
			channelLog.Info("channel already has replicate for target channel")
			r.waitChannel(sourceInfo, targetInfo, channelHandler)
		} else {
			r.channelForwardMap[channelMappingValue] += 1
			r.channelMapping.AddKeyValue(sourceInfo.PChannel, targetInfo.PChannel)
		}
		r.channelHandlerMap[channelMappingKey] = channelHandler
		r.updateSourcePChannelMap(sourceInfo.CollectionID, sourceInfo.PChannel, channelMappingKey)
		if diffValueForKey {
			log.Info("create a replicate handler", zap.String("source_channel", sourceInfo.PChannel), zap.String("target_channel", targetInfo.PChannel))
			return channelHandler, nil
		}
		return nil, nil
	}
	if !r.channelMapping.CheckKeyExist(sourceInfo.PChannel, targetInfo.PChannel) {
		log.Info("diff target pchannel",
			zap.String("source_channel", sourceInfo.PChannel),
			zap.String("target_channel", targetInfo.PChannel),
			zap.String("mapping_value", channelMappingValue))
		r.forwardChannel(channelMappingValue)
	}
	// the msg dispatch client maybe blocked, and has get the target channel,
	// so we can use the goroutine and release the channelLock
	go channelHandler.AddCollection(taskID, sourceInfo, targetInfo)
	return nil, nil
}

// need the channel lock when call this function
func (r *replicateChannelManager) updateSourcePChannelMap(collectionID int64, pchannel string, channelMappingKey string) {
	keyMap, ok := r.sourcePChannelKeyMap[collectionID]
	if !ok {
		r.sourcePChannelKeyMap[collectionID] = map[string]string{
			pchannel: channelMappingKey,
		}
		return
	}
	keyMap[pchannel] = channelMappingKey
}

// need the channel lock when call this function
func (r *replicateChannelManager) getChannelMapKey(collectionID int64, pchannel string) string {
	keyMap, ok := r.sourcePChannelKeyMap[collectionID]
	if !ok {
		return ""
	}
	return keyMap[pchannel]
}

func (r *replicateChannelManager) waitChannel(sourceInfo *model.SourceCollectionInfo, targetInfo *model.TargetCollectionInfo, channelHandler *replicateChannelHandler) {
	go func() {
		tick := time.NewTicker(5 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				log.Info("wait the new replicate channel", zap.String("target_pchannel", targetInfo.PChannel))
			case targetChannel := <-r.forwardReplicateChannel:
				r.channelLock.Lock()
				var isRepeatedChannel bool
				if channelHandler.sourceKey {
					isRepeatedChannel = r.channelMapping.CheckKeyExist(sourceInfo.PChannel, targetChannel)
				} else {
					isRepeatedChannel = r.channelMapping.CheckKeyExist(targetChannel, targetInfo.PChannel)
				}
				if isRepeatedChannel {
					r.channelLock.Unlock()
					continue
				}
				log.Info("success to get the new replicate channel",
					zap.Bool("source_key", channelHandler.sourceKey),
					zap.String("target_pchannel", targetChannel),
					zap.String("source_pchannel", sourceInfo.PChannel),
					zap.String("target_pchannel", targetInfo.PChannel))
				if channelHandler.sourceKey {
					channelHandler.targetPChannel = targetChannel
				} else {
					channelHandler.sourcePChannel = targetChannel
				}
				r.channelForwardMap[targetChannel] += 1
				r.channelMapping.AddKeyValue(channelHandler.sourcePChannel, channelHandler.targetPChannel)
				channelHandler.startReadChannel()
				r.channelLock.Unlock()
				return
			}
		}
	}()
}

func (r *replicateChannelManager) forwardChannel(channelName string) {
	go func() {
		r.channelLock.Lock()
		forwardCnt := r.channelForwardMap[channelName]
		shouldForward := false
		if forwardCnt < r.channelMapping.AverageCnt() {
			r.channelForwardMap[channelName] += 1
			shouldForward = true
		}
		r.channelLock.Unlock()
		if shouldForward {
			tick := time.NewTicker(5 * time.Second)
			defer tick.Stop()
			for {
				select {
				case <-tick.C:
					log.Info("forward the diff replicate channel", zap.String("target_channel", channelName))
				case r.forwardReplicateChannel <- channelName:
					log.Info("success to forward the diff replicate channel", zap.String("target_channel", channelName))
					return
				}
			}
		}
	}()
}

func (r *replicateChannelManager) forwardMsg(targetPChannel string, msg *api.ReplicateMsg) {
	var handler *replicateChannelHandler

	_ = retry.Do(r.replicateCtx, func() error {
		r.channelLock.RLock()
		defer r.channelLock.RUnlock()

		sourceKey := r.channelMapping.UsingSourceKey()
		for _, channelHandler := range r.channelHandlerMap {
			if (sourceKey && channelHandler.targetPChannel == targetPChannel) ||
				(!sourceKey && channelHandler.sourcePChannel == targetPChannel) {
				handler = channelHandler
				break
			}
		}
		if handler == nil {
			select {
			case r.forwardReplicateChannel <- targetPChannel:
				log.Info("success to forward replicate channel", zap.String("target_pchannel", targetPChannel))
			default:
			}
			return errors.Newf("channel %s not found when forward the msg", targetPChannel)
		}
		return nil
	}, r.retryOptions...)

	if handler == nil {
		r.apiEventChan <- &api.ReplicateAPIEvent{
			EventType: api.ReplicateError,
			Error:     errors.Newf("channel %s not found when forward the msg", targetPChannel),
		}
		log.Warn("channel not found when forward the msg",
			zap.String("target_pchannel", targetPChannel), zap.Strings("channels", lo.Keys(r.channelHandlerMap)))
		return
	}
	handler.forwardPackChan <- msg
}

func (r *replicateChannelManager) isDroppedCollection(collection int64) bool {
	_, ok := r.droppedCollections.Load(collection)
	return ok
}

func (r *replicateChannelManager) isDroppedPartition(partition int64) bool {
	_, ok := r.droppedPartitions.Load(partition)
	return ok
}

func (r *replicateChannelManager) stopReadChannel(pChannelName string, collectionID int64) *replicateChannelHandler {
	r.channelLock.RLock()
	mapKey := r.getChannelMapKey(collectionID, pChannelName)
	if mapKey == "" {
		r.channelLock.RUnlock()
		return nil
	}
	channelHandler, ok := r.channelHandlerMap[mapKey]
	if !ok {
		r.channelLock.RUnlock()
		return nil
	}
	r.channelLock.RUnlock()
	channelHandler.RemoveCollection(collectionID)
	// because the channel maybe be repeated to use for the forward message, NOT CLOSE
	// if channelHandler.IsEmpty() {
	//	channelHandler.Close()
	// }
	return channelHandler
}

func (r *replicateChannelManager) Close(handler *replicateChannelHandler) {
	handler.Close()
}

type replicateChannelHandler struct {
	replicateCtx   context.Context
	replicateID    string
	sourcePChannel string
	targetPChannel string
	targetClient   api.TargetAPI
	metaOp         api.MetaOp
	streamCreator  StreamCreator

	// key: source milvus collectionID value: *model.TargetCollectionInfo
	recordLock        deadlock.RWMutex
	collectionRecords map[int64]*model.TargetCollectionInfo   // key is suorce collection id
	collectionNames   map[string]*model.HandlerCollectionInfo // key is collection name, value is the source brief collection info
	closeStreamFuncs  map[int64]io.Closer

	forwardPackChan  chan *api.ReplicateMsg
	generatePackChan chan *api.ReplicateMsg
	apiEventChan     chan *api.ReplicateAPIEvent

	msgPackCallback     func(string, *msgstream.MsgPack)
	forwardMsgFunc      func(string, *api.ReplicateMsg)
	isDroppedCollection func(int64) bool
	isDroppedPartition  func(int64) bool

	handlerOpts *model.HandlerOpts
	ttRateLog   *log.RateLog

	addCollectionLock *deadlock.RWMutex
	addCollectionCnt  *int

	sourceSeekPosition *msgstream.MsgPosition

	downstream    string
	sourceKey     bool // whether the pchannel of source milvus is key
	startReadChan chan struct{}
}

func (r *replicateChannelHandler) AddCollection(taskID string, sourceInfo *model.SourceCollectionInfo, targetInfo *model.TargetCollectionInfo) {
	select {
	case <-r.replicateCtx.Done():
		log.Warn("replicate channel handler closed")
		return
	case <-r.startReadChan:
	}
	r.collectionSourceSeekPosition(sourceInfo.SeekPosition, sourceInfo.StartTs)
	collectionID := sourceInfo.CollectionID
	streamChan, closeStreamFunc, err := r.streamCreator.GetStreamChan(r.replicateCtx, sourceInfo.VChannel, sourceInfo.SeekPosition)
	if err != nil {
		log.Warn("fail to get the msg pack channel",
			zap.String("channel_name", sourceInfo.VChannel),
			zap.Int64("collection_id", sourceInfo.CollectionID),
			zap.Error(err))
		return
	}
	r.recordLock.Lock()
	r.collectionRecords[collectionID] = targetInfo
	r.collectionNames[targetInfo.CollectionName] = &model.HandlerCollectionInfo{
		CollectionID: collectionID,
		PChannel:     sourceInfo.PChannel,
	}
	r.closeStreamFuncs[collectionID] = closeStreamFunc
	go func() {
		log.Info("start to handle the msg pack", zap.String("channel_name", sourceInfo.VChannel))
		for {
			select {
			case <-r.replicateCtx.Done():
				log.Warn("replicate channel handler closed")
				return
			case msgPack, ok := <-streamChan:
				if !ok {
					log.Warn("replicate channel closed", zap.String("channel_name", sourceInfo.VChannel))
					return
				}

				r.innerHandleReplicateMsg(false, api.GetReplicateMsg(sourceInfo.PChannel, targetInfo.CollectionName, collectionID, msgPack, taskID))
			}
		}
	}()
	r.recordLock.Unlock()
	log.Info("add collection to channel handler",
		zap.String("channel_name", sourceInfo.VChannel),
		zap.Int64("collection_id", collectionID),
		zap.String("collection_name", targetInfo.CollectionName),
		zap.String("seek_channel", sourceInfo.SeekPosition.GetChannelName()),
	)

	if targetInfo.Dropped {
		replicatePool.Submit(func() (struct{}, error) {
			dropCollectionLog := log.With(zap.Int64("collection_id", collectionID), zap.String("collection_name", targetInfo.CollectionName))
			dropCollectionLog.Info("generate msg for dropped collection")
			generatePosition := r.sourceSeekPosition
			if generatePosition == nil || generatePosition.Timestamp == 0 {
				// TODO how to do it???
				dropCollectionLog.Warn("drop collection, but seek timestamp is 0")
				return struct{}{}, nil
			}
			generateMsgPack := &msgstream.MsgPack{
				BeginTs: generatePosition.Timestamp,
				EndTs:   generatePosition.Timestamp,
				StartPositions: []*msgstream.MsgPosition{
					generatePosition,
				},
				EndPositions: []*msgstream.MsgPosition{
					generatePosition,
				},
				Msgs: []msgstream.TsMsg{
					&msgstream.DropCollectionMsg{
						BaseMsg: msgstream.BaseMsg{
							BeginTimestamp: generatePosition.Timestamp,
							EndTimestamp:   generatePosition.Timestamp,
							HashValues:     []uint32{uint32(0)},
							MsgPosition:    generatePosition,
						},
						DropCollectionRequest: &msgpb.DropCollectionRequest{
							Base: &commonpb.MsgBase{
								MsgType:   commonpb.MsgType_DropCollection,
								Timestamp: generatePosition.Timestamp,
							},
							DbName:         targetInfo.DatabaseName,
							CollectionID:   collectionID,
							CollectionName: targetInfo.CollectionName,
						},
					},
				},
			}
			r.generatePackChan <- api.GetReplicateMsg(sourceInfo.PChannel, targetInfo.CollectionName, collectionID, generateMsgPack, taskID)
			dropCollectionLog.Info("has generate msg for dropped collection")
			return struct{}{}, nil
		})
	}
}

func (r *replicateChannelHandler) RemoveCollection(collectionID int64) {
	// HINT: please care the lock when you return, because the closeStreamFunc func maybe block
	r.recordLock.Lock()
	defer r.recordLock.Unlock()
	collectionRecord, ok := r.collectionRecords[collectionID]
	if !ok {
		return
	}
	delete(r.collectionRecords, collectionID)
	if collectionRecord != nil {
		delete(r.collectionNames, collectionRecord.CollectionName)
	}
	var closeStreamFunc io.Closer
	closeStreamFunc, ok = r.closeStreamFuncs[collectionID]
	if ok {
		delete(r.closeStreamFuncs, collectionID)
	}
	if closeStreamFunc != nil {
		go func() {
			_ = closeStreamFunc.Close()
		}()
	}
	log.Info("remove collection from handler", zap.Int64("collection_id", collectionID))
}

func (r *replicateChannelHandler) AddPartitionInfo(taskID string, collectionInfo *pb.CollectionInfo, partitionInfo *pb.PartitionInfo, barrierChan chan<- *model.BarrierSignal) error {
	collectionID := collectionInfo.ID
	partitionID := partitionInfo.PartitionID
	collectionName := collectionInfo.Schema.Name
	partitionName := partitionInfo.PartitionName

	partitionLog := log.With(
		zap.String("collection_name", collectionName),
		zap.Int64("collection_id", collectionID),
		zap.Int64("partition_id", partitionID),
		zap.String("partition_name", partitionName),
	)

	targetInfo, err := r.getCollectionTargetInfo(collectionID)
	if err != nil {
		partitionLog.Warn("fail to get collection target info", zap.Error(err))
		return err
	}
	if targetInfo == nil || targetInfo.Dropped {
		partitionLog.Info("the collection is dropping or dropped, skip the partition")
		return nil
	}
	r.recordLock.Lock()
	if targetInfo.PartitionBarrierChan[partitionID] != nil {
		partitionLog.Info("the partition barrier chan is not nil")
		r.recordLock.Unlock()
		return nil
	}
	targetInfo.PartitionBarrierChan[partitionID] = model.NewOnceWriteChan(barrierChan)
	sourcePChannel := r.collectionNames[collectionName].PChannel
	partitionLog.Info("add partition info done")
	r.recordLock.Unlock()

	if partitionInfo.State == pb.PartitionState_PartitionDropping ||
		partitionInfo.State == pb.PartitionState_PartitionDropped {
		targetInfo.DroppedPartition[partitionID] = struct{}{}
		partitionLog.Info("the partition is dropped")
		replicatePool.Submit(func() (struct{}, error) {
			partitionLog.Info("generate msg for dropped partition")
			generatePosition := r.sourceSeekPosition
			if generatePosition == nil || generatePosition.Timestamp == 0 {
				partitionLog.Warn("drop partition, but seek timestamp is 0")
				return struct{}{}, nil
			}
			generateMsgPack := &msgstream.MsgPack{
				BeginTs: generatePosition.Timestamp,
				EndTs:   generatePosition.Timestamp,
				StartPositions: []*msgstream.MsgPosition{
					generatePosition,
				},
				EndPositions: []*msgstream.MsgPosition{
					generatePosition,
				},
				Msgs: []msgstream.TsMsg{
					&msgstream.DropPartitionMsg{
						BaseMsg: msgstream.BaseMsg{
							BeginTimestamp: generatePosition.Timestamp,
							EndTimestamp:   generatePosition.Timestamp,
							HashValues:     []uint32{uint32(0)},
							MsgPosition:    generatePosition,
						},
						DropPartitionRequest: &msgpb.DropPartitionRequest{
							Base: &commonpb.MsgBase{
								MsgType:   commonpb.MsgType_DropPartition,
								Timestamp: generatePosition.Timestamp,
							},
							DbName:         targetInfo.DatabaseName,
							CollectionID:   collectionID,
							CollectionName: targetInfo.CollectionName,
							PartitionName:  partitionName,
							PartitionID:    partitionID,
						},
					},
				},
			}
			r.generatePackChan <- api.GetReplicateMsg(sourcePChannel, collectionName, collectionID, generateMsgPack, taskID)
			partitionLog.Info("has generate msg for dropped partition")
			return struct{}{}, nil
		})
	}
	return nil
}

func (r *replicateChannelHandler) updateTargetPartitionInfo(collectionID int64, collectionName string, partitionName string) int64 {
	ctx, cancelFunc := context.WithTimeout(r.replicateCtx, 10*time.Second)
	defer cancelFunc()
	var collectionInfo *model.CollectionInfo
	var err error

	if r.downstream == "milvus" {
		collectionInfo, err = r.getPartitionInfoForMilvus(ctx, collectionName, collectionID)
	} else if r.downstream == "kafka" {
		collectionInfo, err = r.getPartitionInfoForKafka(ctx, collectionName)
	}
	if err != nil || collectionInfo == nil {
		log.Warn("fail to get partition info", zap.String("collection_name", collectionName), zap.Error(err))
		return 0
	}
	r.recordLock.Lock()
	defer r.recordLock.Unlock()
	targetInfo, ok := r.collectionRecords[collectionID]
	if !ok {
		log.Warn("not found the collection id", zap.Int64("collection_id", collectionID), zap.Any("collections", r.collectionNames))
		return 0
	}
	targetInfo.PartitionInfo = collectionInfo.Partitions
	log.Info("update partition info",
		zap.String("collection_name", collectionName),
		zap.Int64("collection_id", collectionID),
		zap.Any("partition_info", collectionInfo.Partitions),
	)
	if partitionName == "" {
		return 0
	}
	return targetInfo.PartitionInfo[partitionName]
}

func (r *replicateChannelHandler) getPartitionInfoForMilvus(ctx context.Context, collectionName string, collectionID int64) (*model.CollectionInfo, error) {
	sourceDBInfo := r.metaOp.GetDatabaseInfoForCollection(ctx, collectionID)
	if sourceDBInfo.Dropped {
		return nil, errors.New("the database has been dropped")
	}
	partitionInfo, err := r.targetClient.GetPartitionInfo(ctx, collectionName, sourceDBInfo.Name)
	if err != nil {
		return nil, err
	}
	return partitionInfo, nil
}

func (r *replicateChannelHandler) getPartitionInfoForKafka(ctx context.Context, collectionName string) (*model.CollectionInfo, error) {
	partitions := make(map[string]int64)
	partitionInfos, err := r.metaOp.GetAllPartition(ctx, func(info *pb.PartitionInfo) bool {
		targetCollectionName := r.metaOp.GetCollectionNameByID(ctx, info.CollectionId)
		return collectionName != targetCollectionName
	})
	if err != nil {
		return nil, err
	}
	collectionInfo := &model.CollectionInfo{}

	for _, partitionInfo := range partitionInfos {
		partitions[partitionInfo.PartitionName] = partitionInfo.PartitionID
	}
	collectionInfo.Partitions = partitions
	return collectionInfo, nil
}

func (r *replicateChannelHandler) RemovePartitionInfo(collectionID int64, name string, id int64) {
	targetInfo, err := r.getCollectionTargetInfo(collectionID)
	if err != nil {
		log.Warn("fail to get collection target info", zap.Int64("collection_id", collectionID), zap.Error(err))
		return
	}
	if targetInfo == nil {
		log.Info("the collection is dropping or dropped, skip the partition",
			zap.Int64("collection_id", collectionID), zap.String("partition_name", name))
		return
	}
	r.recordLock.Lock()
	defer r.recordLock.Unlock()
	if targetInfo.PartitionInfo[name] == id {
		delete(targetInfo.PartitionInfo, name)
	}
	delete(targetInfo.PartitionBarrierChan, id)
	targetInfo.DroppedPartition[id] = struct{}{}

	log.Info("remove partition info", zap.String("collection_name", targetInfo.CollectionName), zap.Int64("collection_id", collectionID),
		zap.String("partition_name", name), zap.Int64("partition_id", id))
}

func (r *replicateChannelHandler) IsEmpty() bool {
	r.recordLock.RLock()
	defer r.recordLock.RUnlock()
	return len(r.collectionRecords) == 0
}

func (r *replicateChannelHandler) Close() {
	// r.stream.Close()
	r.recordLock.Lock()
	defer r.recordLock.Unlock()
	for _, closeStreamFunc := range r.closeStreamFuncs {
		if closeStreamFunc != nil {
			_ = closeStreamFunc.Close()
		}
	}
}

func (r *replicateChannelHandler) getTSManagerChannelKey(channelName string) string {
	return FormatChanKey(r.replicateID, channelName)
}

func (r *replicateChannelHandler) innerHandleReplicateMsg(forward bool, msg *api.ReplicateMsg) {
	msgPack := msg.MsgPack
	p := r.handlePack(forward, msgPack, msg.TaskID)
	if p == api.EmptyMsgPack {
		return
	}
	p.CollectionID = msg.CollectionID
	p.CollectionName = msg.CollectionName
	p.PChannelName = msg.PChannelName
	p.TaskID = msg.TaskID
	GetTSManager().SendTargetMsg(r.getTSManagerChannelKey(r.targetPChannel), p)
}

func (r *replicateChannelHandler) collectionSourceSeekPosition(
	sourceSeekPosition *msgstream.MsgPosition,
	startTs uint64,
) {
	if sourceSeekPosition == nil {
		return
	}
	GetTSManager().CollectTS(r.getTSManagerChannelKey(r.targetPChannel), sourceSeekPosition.GetTimestamp())
	GetTSManager().CollectTS(r.getTSManagerChannelKey(r.targetPChannel), startTs)
}

func (r *replicateChannelHandler) startReadChannel() {
	var cts uint64 = math.MaxUint64
	if r.sourceSeekPosition != nil {
		cts = r.sourceSeekPosition.GetTimestamp()
	}
	GetTSManager().InitTSInfo(r.replicateID, r.targetPChannel, time.Duration(r.handlerOpts.TTInterval)*time.Millisecond, cts, r.handlerOpts.MessageBufferSize)
	log.Info("start read channel in the handler",
		zap.String("channel_name", r.sourcePChannel),
		zap.String("target_channel", r.targetPChannel),
	)
	close(r.startReadChan)
	r.collectionSourceSeekPosition(r.sourceSeekPosition, 0)
	go func() {
		for {
			select {
			case <-r.replicateCtx.Done():
				log.Warn("replicate channel handler closed",
					zap.String("replicate_id", r.replicateID),
					zap.String("target_channel", r.targetPChannel),
				)
				GetTSManager().ClearTSInfo(r.replicateID, r.targetPChannel)
				return
			case replicateMsg := <-r.forwardPackChan:
				r.innerHandleReplicateMsg(true, replicateMsg)
				GreedyConsumeChan(r.generatePackChan, true, r.innerHandleReplicateMsg)
			case replicateMsg := <-r.generatePackChan:
				r.innerHandleReplicateMsg(false, replicateMsg)
				GreedyConsumeChan(r.generatePackChan, false, r.innerHandleReplicateMsg)
			}
		}
	}()
}

func GreedyConsumeChan(packChan chan *api.ReplicateMsg, forward bool, f func(bool, *api.ReplicateMsg)) {
	for i := 0; i < 10; i++ {
		select {
		case pack := <-packChan:
			f(forward, pack)
		default:
			return
		}
	}
}

func (r *replicateChannelHandler) getCollectionTargetInfo(collectionID int64) (*model.TargetCollectionInfo, error) {
	if r.isDroppedCollection(collectionID) {
		return nil, nil
	}
	r.recordLock.RLock()
	targetInfo, ok := r.collectionRecords[collectionID]
	r.recordLock.RUnlock()
	if ok {
		return targetInfo, nil
	}

	// Sleep for a short time to avoid invalid requests
	time.Sleep(500 * time.Millisecond)
	err := retry.Do(r.replicateCtx, func() error {
		r.recordLock.RLock()
		var xs []int64
		for x := range r.collectionRecords {
			xs = append(xs, x)
		}
		log.Info("wait collection info", zap.Int64("msg_collection_id", collectionID), zap.Any("records", xs))
		// TODO it needs to be considered when supporting the specific collection in a task
		targetInfo, ok = r.collectionRecords[collectionID]
		r.recordLock.RUnlock()
		if ok {
			return nil
		}
		if r.isDroppedCollection(collectionID) {
			return nil
		}
		return errors.Newf("not found the collection [%d]", collectionID)
	}, r.handlerOpts.RetryOptions...)
	if err != nil {
		log.Warn("fail to find the collection info", zap.Error(err))
		return nil, err
	}
	return targetInfo, nil
}

func (r *replicateChannelHandler) containCollection(collectionName string) bool {
	r.recordLock.RLock()
	defer r.recordLock.RUnlock()
	return r.collectionNames[collectionName] != nil
}

func (r *replicateChannelHandler) getPartitionID(sourceCollectionID, sourcePartitionID int64, info *model.TargetCollectionInfo, name string) (int64, error) {
	r.recordLock.RLock()
	id, ok := info.PartitionInfo[name]
	r.recordLock.RUnlock()
	if ok {
		return id, nil
	}

	// Sleep for a short time to avoid invalid requests
	time.Sleep(500 * time.Millisecond)
	err := retry.Do(r.replicateCtx, func() error {
		log.Warn("wait partition info", zap.Int64("collection_id", info.CollectionID), zap.String("partition_name", name))
		id = r.updateTargetPartitionInfo(sourceCollectionID, info.CollectionName, name)
		if id != 0 {
			return nil
		}
		if r.isDroppedCollection(sourceCollectionID) || r.isDroppedPartition(sourcePartitionID) {
			id = -1
			return nil
		}
		return errors.Newf("not found the partition [%s]", name)
	}, r.handlerOpts.RetryOptions...)
	if err != nil {
		log.Warn("fail to find the partition id", zap.Int64("source_collection", sourceCollectionID), zap.Any("target_collection", info.CollectionID), zap.String("partition_name", name))
		return 0, err
	}

	return id, nil
}

func (r *replicateChannelHandler) getPartitionIDs(sourceCollectionID int64, sourcePartitionIDs []int64, info *model.TargetCollectionInfo) ([]int64, error) {
	var ids []int64
	r.recordLock.RLock()
	ids = lo.Values(info.PartitionInfo)
	r.recordLock.RUnlock()
	if len(ids) == len(sourcePartitionIDs) {
		return ids, nil
	}

	time.Sleep(500 * time.Millisecond)
	err := retry.Do(r.replicateCtx, func() error {
		log.Warn("wait partition info", zap.Int64("collection_id", info.CollectionID), zap.Int64s("source_partition_ids", sourcePartitionIDs))
		_ = r.updateTargetPartitionInfo(sourceCollectionID, info.CollectionName, "")

		r.recordLock.RLock()
		ids = lo.Values(info.PartitionInfo)
		r.recordLock.RUnlock()
		if len(ids) == len(sourcePartitionIDs) {
			return nil
		}

		return errors.Newf("not found the partition ids")
	}, r.handlerOpts.RetryOptions...)
	if err != nil {
		log.Warn("fail to find the partition ids",
			zap.Int64("source_collection", sourceCollectionID),
			zap.Any("target_collection", info.CollectionID),
			zap.Int64s("source_partition_ids", sourcePartitionIDs))
		return nil, err
	}

	return ids, nil
}

func (r *replicateChannelHandler) isDroppingPartition(partition int64, info *model.TargetCollectionInfo) bool {
	r.recordLock.RLock()
	defer r.recordLock.RUnlock()
	_, ok := info.DroppedPartition[partition]
	return ok
}

func isSupportedMsgType(msgType commonpb.MsgType) bool {
	return msgType == commonpb.MsgType_Insert ||
		msgType == commonpb.MsgType_Delete ||
		msgType == commonpb.MsgType_DropCollection ||
		msgType == commonpb.MsgType_DropPartition ||
		msgType == commonpb.MsgType_Import
}

func (r *replicateChannelHandler) handlePack(forward bool, pack *msgstream.MsgPack, taskID string) *api.ReplicateMsg {
	sort.Slice(pack.Msgs, func(i, j int) bool {
		return pack.Msgs[i].BeginTs() < pack.Msgs[j].BeginTs() ||
			(pack.Msgs[i].BeginTs() == pack.Msgs[j].BeginTs() && pack.Msgs[i].Type() == commonpb.MsgType_Delete)
	})
	tsManagerChannelKey := r.getTSManagerChannelKey(r.targetPChannel)

	r.addCollectionLock.RLock()
	if *r.addCollectionCnt != 0 {
		r.addCollectionLock.RUnlock()
		for {
			r.addCollectionLock.RLock()
			if *r.addCollectionCnt == 0 {
				break
			}
			r.addCollectionLock.RUnlock()
			time.Sleep(100 * time.Millisecond)
		}
	}
	beginTS := pack.BeginTs
	if beginTS == 0 {
		hasValidMsg := false
		miniTS := pack.EndTs
		for _, msg := range pack.Msgs {
			if isSupportedMsgType(msg.Type()) {
				hasValidMsg = true
				if miniTS > msg.BeginTs() {
					miniTS = msg.BeginTs()
				}
			}
		}

		switch {
		case !hasValidMsg && pack.EndTs != 0:
			beginTS = pack.EndTs
			pack.BeginTs = beginTS
			log.Info("begin timestamp is 0, use end timestamp",
				zap.String("target_channel", r.targetPChannel),
				zap.Uint64("end_ts", pack.EndTs), zap.Any("hasValidMsg", hasValidMsg))
		case miniTS != pack.EndTs:
			beginTS = miniTS - 1
			pack.BeginTs = beginTS
			log.Info("begin timestamp is 0, use mini timestamp",
				zap.String("target_channel", r.targetPChannel),
				zap.Uint64("mini_ts", miniTS), zap.Uint64("pack_end_ts", pack.EndTs))
		case len(pack.StartPositions) > 1:
			beginTS = pack.StartPositions[0].Timestamp
			pack.BeginTs = beginTS
			log.Info("begin timestamp is 0, use start position",
				zap.String("target_channel", r.targetPChannel),
				zap.Uint64("begin_ts", beginTS))
		default:
			log.Warn("begin timestamp is 0",
				zap.String("target_channel", r.targetPChannel),
				zap.Uint64("end_ts", pack.EndTs),
				zap.Any("hasValidMsg", hasValidMsg))
		}
	}
	GetTSManager().CollectTS(tsManagerChannelKey, beginTS)
	r.addCollectionLock.RUnlock()

	if r.msgPackCallback != nil && !forward {
		r.msgPackCallback(r.sourcePChannel, pack)
	}
	newPack := &msgstream.MsgPack{
		BeginTs:        pack.BeginTs,
		EndTs:          pack.EndTs,
		StartPositions: copyMsgPositions(pack.StartPositions),
		EndPositions:   copyMsgPositions(pack.EndPositions),
		Msgs:           make([]msgstream.TsMsg, 0),
	}

	needTsMsg := false
	pChannel := r.targetPChannel
	sourceCollectionID := int64(-1)
	sourceCollectionName := ""
	forwardChannel := ""
	streamPChannel := ""

	for _, msg := range pack.Msgs {
		if forward {
			newPack.Msgs = append(newPack.Msgs, msg)
			if msg.Type() == commonpb.MsgType_DropCollection {
				needTsMsg = true
			}
			continue
		}
		if msg.Type() == commonpb.MsgType_CreateCollection ||
			msg.Type() == commonpb.MsgType_CreatePartition ||
			msg.Type() == commonpb.MsgType_TimeTick {
			continue
		}
		if !isSupportedMsgType(msg.Type()) {
			log.Warn("not support msg type", zap.Any("msg", msg))
			continue
		}

		y, ok := msg.(interface{ GetCollectionID() int64 })
		if !ok {
			log.Warn("not support msg type", zap.Any("msg", msg))
			continue
		}

		msgCollectionID := y.GetCollectionID()
		if sourceCollectionID == -1 {
			sourceCollectionID = msgCollectionID
		}
		// the condition can't be satisfied because using the msg dispatch client
		if sourceCollectionID != msgCollectionID {
			// server warn
			log.Warn("server warn: not support multiple collection in one pack",
				zap.String("msg_type", msg.Type().String()),
				zap.Int64("source_collection_id", sourceCollectionID), zap.Int64("msg_collection_id", msgCollectionID))
			continue
		}
		info, err := r.getCollectionTargetInfo(sourceCollectionID)
		if err != nil {
			r.sendErrEvent(err)
			log.Warn("fail to get collection info", zap.Int64("collection_id", sourceCollectionID), zap.Error(err))
			return nil
		}
		if info == nil {
			log.Info("collection has been dropped in the source and target", zap.Int64("collection_id", sourceCollectionID))
			continue
		}
		sourceCollectionName = info.CollectionName
		logFields := []zap.Field{
			zap.String("msg", msg.Type().String()),
			zap.String("collection_name", info.CollectionName),
		}
		var dataLen int
		switch realMsg := msg.(type) {
		case *msgstream.InsertMsg:
			if info.Dropped {
				log.Info("skip insert msg because collection has been dropped", zap.Int64("collection_id", sourceCollectionID))
				continue
			}
			realMsg.CollectionID = info.CollectionID
			partitionID := realMsg.PartitionID
			realMsg.PartitionID, err = r.getPartitionID(sourceCollectionID, partitionID, info, realMsg.PartitionName)
			if realMsg.PartitionID == -1 {
				log.Info("skip insert msg because partition has been dropped in the source and target",
					zap.Int64("partition_id", partitionID), zap.String("partition_name", realMsg.PartitionName))
				continue
			}
			logFields = append(logFields, zap.String("shard_name", realMsg.ShardName))
			realMsg.ShardName = info.VChannel
			// nolint
			dataLen = int(realMsg.GetNumRows())
		case *msgstream.DeleteMsg:
			if info.Dropped {
				log.Info("skip delete msg because collection has been dropped", zap.Int64("collection_id", sourceCollectionID))
				continue
			}
			if r.isDroppedPartition(realMsg.PartitionID) || r.isDroppingPartition(realMsg.PartitionID, info) {
				log.Info("skip delete msg because partition has been dropped in the source and target",
					zap.Int64("partition_id", realMsg.PartitionID), zap.String("partition_name", realMsg.PartitionName))
				continue
			}
			realMsg.CollectionID = info.CollectionID
			if realMsg.PartitionName != "" {
				partitionID := realMsg.PartitionID
				realMsg.PartitionID, err = r.getPartitionID(sourceCollectionID, realMsg.PartitionID, info, realMsg.PartitionName)
				if realMsg.PartitionID == -1 {
					log.Info("skip delete msg because partition has been dropped in the source and target",
						zap.Int64("partition_id", partitionID), zap.String("partition_name", realMsg.PartitionName))
					continue
				}
			}
			logFields = append(logFields, zap.String("shard_name", realMsg.ShardName))
			realMsg.ShardName = info.VChannel
			dataLen = int(realMsg.GetNumRows())
		case *msgstream.DropCollectionMsg:
			collectionID := realMsg.CollectionID

			// copy msg, avoid the msg be modified by other goroutines when the msg dispather is spliting
			msg = copyDropTypeMsg(realMsg)
			realMsg = msg.(*msgstream.DropCollectionMsg)

			realMsg.CollectionID = info.CollectionID
			info.BarrierChan.Write(&model.BarrierSignal{
				Msg:      msg,
				VChannel: info.VChannel,
			})
			needTsMsg = true
			r.RemoveCollection(collectionID)
		case *msgstream.DropPartitionMsg:
			if info.Dropped {
				log.Info("skip drop partition msg because collection has been dropped",
					zap.Int64("collection_id", sourceCollectionID),
					zap.String("collection_name", info.CollectionName),
					zap.String("partition_name", realMsg.PartitionName))
				continue
			}
			if r.isDroppedPartition != nil && r.isDroppedPartition(realMsg.PartitionID) {
				log.Info("skip drop partition msg because partition has been dropped",
					zap.Int64("partition_id", realMsg.PartitionID),
					zap.String("partition_name", realMsg.PartitionName))
				continue
			}

			// copy msg
			msg = copyDropTypeMsg(realMsg)
			realMsg = msg.(*msgstream.DropPartitionMsg)

			realMsg.CollectionID = info.CollectionID
			if realMsg.PartitionName == "" {
				log.Warn("invalid drop partition message, empty partition name", zap.Any("msg", msg))
				continue
			}
			var partitionBarrierChan *model.OnceWriteChan[*model.BarrierSignal]
			retryErr := retry.Do(r.replicateCtx, func() error {
				err = nil
				r.recordLock.RLock()
				partitionBarrierChan = info.PartitionBarrierChan[realMsg.PartitionID]
				if partitionBarrierChan == nil {
					err = errors.Newf("not found the partition info [%d]", realMsg.PartitionID)
					log.Warn("invalid drop partition message", zap.Any("msg", msg))
				}
				r.recordLock.RUnlock()
				if r.isDroppedCollection(realMsg.CollectionID) ||
					r.isDroppedPartition(realMsg.PartitionID) {
					return nil
				}
				return err
			}, r.handlerOpts.RetryOptions...)
			if retryErr != nil && err == nil {
				err = retryErr
			}
			if r.isDroppedCollection(realMsg.CollectionID) ||
				r.isDroppedPartition(realMsg.PartitionID) {
				log.Info("skip drop partition msg because partition or collection is dropping",
					zap.Int64("partition_id", realMsg.PartitionID), zap.String("partition_name", realMsg.PartitionName))
				continue
			}
			if err == nil {
				partitionID := realMsg.PartitionID
				realMsg.PartitionID, err = r.getPartitionID(sourceCollectionID, partitionID, info, realMsg.PartitionName)
				if realMsg.PartitionID == -1 {
					log.Warn("skip drop partition msg because partition or collection is dropping",
						zap.Int64("partition_id", partitionID), zap.String("partition_name", realMsg.PartitionName))
					continue
				}
				partitionBarrierChan.Write(&model.BarrierSignal{
					Msg:      msg,
					VChannel: info.VChannel,
				})
				r.RemovePartitionInfo(sourceCollectionID, realMsg.PartitionName, partitionID)
			}
		case *msgstream.ImportMsg:
			if info.Dropped {
				log.Info("skip import msg because collection has been dropped", zap.Int64("collection_id", sourceCollectionID))
				continue
			}
			realMsg.CollectionID = info.CollectionID
			var partitionIDs []int64
			partitionIDs, err = r.getPartitionIDs(sourceCollectionID, realMsg.PartitionIDs, info)
			if err == nil {
				realMsg.PartitionIDs = partitionIDs
			}
		}
		if err != nil {
			r.sendErrEvent(err)
			log.Warn("fail to process the msg info", zap.Any("msg", msg.Type()), zap.Error(err))
			return nil
		}
		originPosition := msg.Position()
		originPositionPChannel := funcutil.ToPhysicalChannel(originPosition.GetChannelName())
		streamPChannel = originPositionPChannel
		positionChannel := info.PChannel
		if IsVirtualChannel(originPosition.GetChannelName()) {
			positionChannel = info.VChannel
		}
		msg.SetPosition(&msgpb.MsgPosition{
			ChannelName: positionChannel,
			MsgID:       originPosition.GetMsgID(),
			MsgGroup:    originPosition.GetMsgGroup(),
			Timestamp:   msg.EndTs(),
		})
		if dataLen != 0 {
			logFields = append(logFields, zap.Int("data_len", dataLen))
		}
		if r.targetPChannel != info.PChannel || r.sourcePChannel != originPositionPChannel {
			logFields = append(logFields,
				zap.Bool("source_key", r.sourceKey), zap.String("origin_position_channel", originPositionPChannel),
				zap.String("pChannel", pChannel), zap.String("info_pChannel", info.PChannel))
			log.Debug("forward the msg", logFields...)
			forwardChannel = info.PChannel
			if !r.sourceKey {
				forwardChannel = originPositionPChannel
			}
		} else {
			log.Debug("receive msg", logFields...)
			if forwardChannel != "" {
				log.Warn("server warn: the pack exist forward and not forward msg", zap.String("forward_channel", forwardChannel), zap.String("info_pChannel", info.PChannel))
				continue
			}
		}
		newPack.Msgs = append(newPack.Msgs, msg)
	}

	if forwardChannel != "" {
		r.forwardMsgFunc(forwardChannel, api.GetReplicateMsg(streamPChannel, sourceCollectionName, sourceCollectionID, newPack, taskID))
		return api.EmptyMsgPack
	}

	for _, position := range newPack.StartPositions {
		position.ChannelName = pChannel
	}
	for _, position := range newPack.EndPositions {
		position.ChannelName = pChannel
	}

	maxTS, _ := GetTSManager().GetMaxTS(tsManagerChannelKey)
	resetTS := resetMsgPackTimestamp(newPack, maxTS)
	if resetTS {
		GetTSManager().CollectTS(tsManagerChannelKey, newPack.EndTs)
	}

	GetTSManager().LockTargetChannel(tsManagerChannelKey)
	defer GetTSManager().UnLockTargetChannel(tsManagerChannelKey)

	resetLastTs := needTsMsg || len(newPack.Msgs) != 0
	needTsMsg = needTsMsg || len(newPack.Msgs) != 0 || GetTSManager().UnsafeShouldSendTSMsg(tsManagerChannelKey)

	if !needTsMsg {
		return api.EmptyMsgPack
	}

	generateTS, ok := GetTSManager().UnsafeGetMaxTS(tsManagerChannelKey)
	if !ok {
		log.Warn("not found the max ts", zap.String("channel", r.targetPChannel))
		r.sendErrEvent(fmt.Errorf("not found the max ts"))
		return nil
	}
	GetTSManager().UnsafeUpdatePackTS(tsManagerChannelKey, newPack.BeginTs, func(newTS uint64) (uint64, bool) {
		reset := resetMsgPackTimestamp(newPack, newTS)
		generateTS = newPack.EndTs
		return newPack.EndTs, reset
	})

	timeTickResult := &msgpb.TimeTickMsg{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_TimeTick),
			commonpbutil.WithMsgID(0),
			commonpbutil.WithTimeStamp(generateTS),
			commonpbutil.WithSourceID(-1),
		),
	}
	timeTickMsg := &msgstream.TimeTickMsg{
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: generateTS,
			EndTimestamp:   generateTS,
			HashValues:     []uint32{0},
			MsgPosition:    newPack.EndPositions[0],
		},
		TimeTickMsg: timeTickResult,
	}
	newPack.Msgs = append(newPack.Msgs, timeTickMsg)
	lastSendTs, ok := GetTSManager().UnsafeGetLastSendTS(tsManagerChannelKey)
	if ok && lastSendTs == 0 {
		beginTs := newPack.BeginTs
		beginMsgPosition := &msgpb.MsgPosition{
			ChannelName: newPack.StartPositions[0].ChannelName,
			MsgID:       newPack.StartPositions[0].MsgID,
			MsgGroup:    newPack.StartPositions[0].MsgGroup,
			Timestamp:   beginTs,
		}
		beginTsMsgPb := &msgpb.TimeTickMsg{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_TimeTick),
				commonpbutil.WithMsgID(0),
				commonpbutil.WithTimeStamp(beginTs),
				commonpbutil.WithSourceID(-1),
			),
		}
		beginTimeTickMsg := &msgstream.TimeTickMsg{
			BaseMsg: msgstream.BaseMsg{
				BeginTimestamp: beginTs,
				EndTimestamp:   beginTs,
				HashValues:     []uint32{0},
				MsgPosition:    beginMsgPosition,
			},
			TimeTickMsg: beginTsMsgPb,
		}
		newPack.Msgs = append([]msgstream.TsMsg{beginTimeTickMsg}, newPack.Msgs...)
	}

	GetTSManager().UnsafeUpdateTSInfo(tsManagerChannelKey, generateTS, resetLastTs)
	msgTime, _ := tsoutil.ParseHybridTs(generateTS)
	TSMetricVec.WithLabelValues(r.targetPChannel).Set(float64(msgTime))
	r.ttRateLog.Debug("time tick msg", zap.String("channel", r.targetPChannel), zap.Uint64("max_ts", generateTS))
	return api.GetReplicateMsg("", sourceCollectionName, sourceCollectionID, newPack, "")
}

func resetMsgPackTimestamp(pack *msgstream.MsgPack, newTimestamp uint64) bool {
	beginTs := pack.BeginTs
	if beginTs > newTimestamp || len(pack.Msgs) == 0 {
		return false
	}
	deltas := make([]uint64, len(pack.Msgs))
	lastTS := uint64(0)
	for i, msg := range pack.Msgs {
		if i != 0 && lastTS == msg.BeginTs() {
			deltas[i] = deltas[i-1]
		} else {
			// nolint
			deltas[i] = uint64(i) + 1
			lastTS = msg.BeginTs()
		}
	}

	for i, msg := range pack.Msgs {
		resetMsgTimestamp(msg, newTimestamp+deltas[i])
	}
	pack.BeginTs = newTimestamp + deltas[0]
	pack.EndTs = newTimestamp + deltas[len(deltas)-1]
	for _, pos := range pack.StartPositions {
		pos.Timestamp = pack.BeginTs
	}
	for _, pos := range pack.EndPositions {
		pos.Timestamp = pack.EndTs
	}
	return true
}

func resetMsgTimestamp(msg msgstream.TsMsg, newTimestamp uint64) {
	switch realMsg := msg.(type) {
	case *msgstream.InsertMsg:
		realMsg.BeginTimestamp = newTimestamp
		realMsg.EndTimestamp = newTimestamp
		realMsg.Timestamps = make([]uint64, len(realMsg.Timestamps))
		for i := range realMsg.Timestamps {
			realMsg.Timestamps[i] = newTimestamp
		}
	case *msgstream.DeleteMsg:
		realMsg.BeginTimestamp = newTimestamp
		realMsg.EndTimestamp = newTimestamp
		realMsg.Timestamps = make([]uint64, len(realMsg.Timestamps))
		for i := range realMsg.Timestamps {
			realMsg.Timestamps[i] = newTimestamp
		}
	case *msgstream.DropCollectionMsg:
		realMsg.BeginTimestamp = newTimestamp
		realMsg.EndTimestamp = newTimestamp
	case *msgstream.DropPartitionMsg:
		realMsg.BeginTimestamp = newTimestamp
		realMsg.EndTimestamp = newTimestamp
	case *msgstream.ImportMsg:
		realMsg.BeginTimestamp = newTimestamp
		realMsg.EndTimestamp = newTimestamp
	default:
		log.Panic("reset msg timestamp: not support msg type", zap.Any("msg", msg))
		return
	}
	pos := msg.Position()
	msg.SetPosition(&msgpb.MsgPosition{
		ChannelName: pos.GetChannelName(),
		MsgID:       pos.GetMsgID(),
		MsgGroup:    pos.GetMsgGroup(),
		Timestamp:   newTimestamp,
	})
}

func copyDropTypeMsg(msg msgstream.TsMsg) msgstream.TsMsg {
	switch realMsg := msg.(type) {
	case *msgstream.DropCollectionMsg:
		hashValues := make([]uint32, len(realMsg.HashValues))
		copy(hashValues, realMsg.HashValues)
		copyDropCollectionMsg := &msgstream.DropCollectionMsg{
			BaseMsg: msgstream.BaseMsg{
				Ctx:            realMsg.Ctx,
				BeginTimestamp: realMsg.BeginTimestamp,
				EndTimestamp:   realMsg.EndTimestamp,
				HashValues:     hashValues,
				MsgPosition:    typeutil.Clone(realMsg.MsgPosition),
			},
			DropCollectionRequest: typeutil.Clone(realMsg.DropCollectionRequest),
		}
		return copyDropCollectionMsg
	case *msgstream.DropPartitionMsg:
		hashValues := make([]uint32, len(realMsg.HashValues))
		copy(hashValues, realMsg.HashValues)
		copyDropPartitionMsg := &msgstream.DropPartitionMsg{
			BaseMsg: msgstream.BaseMsg{
				Ctx:            realMsg.Ctx,
				BeginTimestamp: realMsg.BeginTimestamp,
				EndTimestamp:   realMsg.EndTimestamp,
				HashValues:     hashValues,
				MsgPosition:    typeutil.Clone(realMsg.MsgPosition),
			},
			DropPartitionRequest: typeutil.Clone(realMsg.DropPartitionRequest),
		}
		return copyDropPartitionMsg
	default:
		log.Warn("copy drop type msg: not support msg type", zap.Any("msg", msg.Type()))
		return realMsg
	}
}

func copyMsgPositions(positions []*msgpb.MsgPosition) []*msgpb.MsgPosition {
	newPositions := make([]*msgpb.MsgPosition, len(positions))
	for i, pos := range positions {
		newPositions[i] = typeutil.Clone(pos)
	}
	return newPositions
}

func (r *replicateChannelHandler) sendErrEvent(err error) {
	r.apiEventChan <- &api.ReplicateAPIEvent{
		EventType: api.ReplicateError,
		Error:     err,
	}
}

func initReplicateChannelHandler(ctx context.Context,
	sourceInfo *model.SourceCollectionInfo,
	targetInfo *model.TargetCollectionInfo,
	targetClient api.TargetAPI, metaOp api.MetaOp,
	apiEventChan chan *api.ReplicateAPIEvent,
	opts *model.HandlerOpts,
	streamCreator StreamCreator,
	downstream string, sourceKey bool,
	taskID string,
) (*replicateChannelHandler, error) {
	err := streamCreator.CheckConnection(ctx, sourceInfo.VChannel, sourceInfo.SeekPosition)
	if err != nil {
		log.Warn("fail to connect the mq stream",
			zap.String("channel_name", sourceInfo.VChannel),
			zap.Int64("collection_id", sourceInfo.CollectionID),
			zap.Error(err))
		return nil, err
	}

	if opts.TTInterval <= 0 {
		opts.TTInterval = util.DefaultReplicateTTInterval
	}
	channelHandler := &replicateChannelHandler{
		replicateCtx:       ctx,
		sourcePChannel:     sourceInfo.PChannel,
		targetPChannel:     targetInfo.PChannel,
		targetClient:       targetClient,
		metaOp:             metaOp,
		streamCreator:      streamCreator,
		collectionRecords:  make(map[int64]*model.TargetCollectionInfo),
		collectionNames:    make(map[string]*model.HandlerCollectionInfo),
		closeStreamFuncs:   make(map[int64]io.Closer),
		apiEventChan:       apiEventChan,
		forwardPackChan:    make(chan *api.ReplicateMsg, opts.MessageBufferSize),
		generatePackChan:   make(chan *api.ReplicateMsg, 30),
		handlerOpts:        opts,
		sourceSeekPosition: sourceInfo.SeekPosition,
		ttRateLog:          log.NewRateLog(0.01, log.L()),
		downstream:         downstream,
		sourceKey:          sourceKey,
		startReadChan:      make(chan struct{}),
	}
	go channelHandler.AddCollection(taskID, sourceInfo, targetInfo)
	return channelHandler, nil
}
