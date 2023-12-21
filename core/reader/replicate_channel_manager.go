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
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/log"
	"github.com/zilliztech/milvus-cdc/core/model"
	"github.com/zilliztech/milvus-cdc/core/pb"
	"github.com/zilliztech/milvus-cdc/core/util"
)

var _ api.ChannelManager = (*replicateChannelManager)(nil)

type replicateChannelManager struct {
	replicateCtx      context.Context
	factory           msgstream.Factory
	targetClient      api.TargetAPI
	metaOp            api.MetaOp
	retryOptions      []retry.Option
	messageBufferSize int

	channelLock       sync.RWMutex
	channelHandlerMap map[string]*replicateChannelHandler

	collectionLock       sync.Mutex
	replicateCollections map[int64]chan struct{}

	partitionLock       sync.Mutex
	replicatePartitions map[int64]map[int64]chan struct{}

	channelChan  chan string
	apiEventChan chan *api.ReplicateAPIEvent

	msgPackCallback func(string, *msgstream.MsgPack)
}

func NewReplicateChannelManager(mqConfig config.MQConfig,
	factoryCreator FactoryCreator,
	client api.TargetAPI,
	messageBufferSize int,
	metaOp api.MetaOp,
	msgPackCallback func(string, *msgstream.MsgPack),
) (api.ChannelManager, error) {
	var factory msgstream.Factory
	switch {
	case mqConfig.Pulsar.Address != "":
		factory = factoryCreator.NewPmsFactory(&mqConfig.Pulsar)
	case mqConfig.Kafka.Address != "":
		factory = factoryCreator.NewKmsFactory(&mqConfig.Kafka)
	default:
		log.Warn("mqConfig is empty")
		return nil, errors.New("fail to get the msg stream, check the mqConfig param")
	}

	return &replicateChannelManager{
		factory:              factory,
		targetClient:         client,
		metaOp:               metaOp,
		retryOptions:         util.GetRetryOptionsFor25s(),
		messageBufferSize:    messageBufferSize,
		channelHandlerMap:    make(map[string]*replicateChannelHandler),
		replicateCollections: make(map[int64]chan struct{}),
		replicatePartitions:  make(map[int64]map[int64]chan struct{}),
		channelChan:          make(chan string, 10),
		apiEventChan:         make(chan *api.ReplicateAPIEvent, 10),
		msgPackCallback:      msgPackCallback,
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

func (r *replicateChannelManager) StartReadCollection(ctx context.Context, info *pb.CollectionInfo, seekPositions []*msgpb.MsgPosition) error {
	sourceDBInfo := r.metaOp.GetDatabaseInfoForCollection(ctx, info.ID)

	if _, err := r.targetClient.GetCollectionInfo(ctx, info.Schema.GetName(), sourceDBInfo.Name); err != nil {
		select {
		case <-ctx.Done():
			log.Warn("context is done in the start read collection")
			return ctx.Err()
		default:
			r.apiEventChan <- &api.ReplicateAPIEvent{
				EventType:      api.ReplicateCreateCollection,
				CollectionInfo: info,
				ReplicateInfo: &commonpb.ReplicateInfo{
					IsReplicate:  true,
					MsgTimestamp: info.CreateTime,
				},
				ReplicateParam: api.ReplicateParam{Database: sourceDBInfo.Name},
			}
		}
	}

	var targetInfo *model.CollectionInfo
	var err error

	err = retry.Do(ctx, func() error {
		targetInfo, err = r.targetClient.GetCollectionInfo(ctx, info.Schema.Name, sourceDBInfo.Name)
		return err
	}, r.retryOptions...)
	if err != nil {
		log.Warn("failed to get target collection info", zap.Error(err))
		return err
	}
	log.Info("success to get the collection info in the target instance", zap.String("collection_name", targetInfo.CollectionName))

	getSeekPosition := func(channelName string) *msgpb.MsgPosition {
		for _, seekPosition := range seekPositions {
			if seekPosition.ChannelName == channelName {
				return seekPosition
			}
		}
		return nil
	}

	r.collectionLock.Lock()
	if _, ok := r.replicateCollections[info.ID]; ok {
		r.collectionLock.Unlock()
		return nil
	}
	barrier := NewBarrier(len(info.StartPositions), func(msgTs uint64, b *Barrier) {
		select {
		case <-b.CloseChan:
		case r.apiEventChan <- &api.ReplicateAPIEvent{
			EventType:      api.ReplicateDropCollection,
			CollectionInfo: info,
			ReplicateInfo: &commonpb.ReplicateInfo{
				IsReplicate:  true,
				MsgTimestamp: msgTs,
			},
			ReplicateParam: api.ReplicateParam{Database: sourceDBInfo.Name},
		}:
		}
	})
	r.replicateCollections[info.ID] = barrier.CloseChan
	r.collectionLock.Unlock()

	var successChannels []string
	err = ForeachChannel(info.PhysicalChannelNames, targetInfo.PChannels, func(sourcePChannel, targetPChannel string) error {
		err := r.startReadChannel(&model.SourceCollectionInfo{
			PChannelName: sourcePChannel,
			CollectionID: info.ID,
			SeekPosition: getSeekPosition(sourcePChannel),
		}, &model.TargetCollectionInfo{
			CollectionID:         targetInfo.CollectionID,
			CollectionName:       info.Schema.Name,
			PartitionInfo:        targetInfo.Partitions,
			PChannel:             targetPChannel,
			VChannel:             GetVChannelByPChannel(targetPChannel, targetInfo.VChannels),
			BarrierChan:          barrier.BarrierChan,
			PartitionBarrierChan: make(map[int64]chan<- uint64),
		})
		if err != nil {
			log.Warn("start read channel failed", zap.String("channel", sourcePChannel), zap.Int64("collection_id", info.ID), zap.Error(err))
			for _, channel := range successChannels {
				r.stopReadChannel(channel, info.ID)
			}
			return err
		}
		successChannels = append(successChannels, sourcePChannel)
		log.Info("start read channel", zap.String("channel", sourcePChannel))
		return nil
	})
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

func (r *replicateChannelManager) AddPartition(ctx context.Context, collectionInfo *pb.CollectionInfo, partitionInfo *pb.PartitionInfo) error {
	var handlers []*replicateChannelHandler
	collectionID := collectionInfo.ID
	sourceDBInfo := r.metaOp.GetDatabaseInfoForCollection(ctx, collectionID)

	_ = retry.Do(ctx, func() error {
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
			log.Info("waiting handler", zap.Int64("collection_id", collectionID))
			return errors.New("no handler found")
		}
		return nil
	}, r.retryOptions...)
	if len(handlers) == 0 {
		log.Warn("no handler found", zap.Int64("collection_id", collectionID))
		return errors.New("no handler found")
	}

	firstHandler := handlers[0]
	targetInfo, err := firstHandler.getCollectionTargetInfo(collectionID)
	if err != nil {
		return err
	}
	partitionRecord := targetInfo.PartitionInfo
	if _, ok := partitionRecord[partitionInfo.PartitionName]; !ok {
		select {
		case r.apiEventChan <- &api.ReplicateAPIEvent{
			EventType:      api.ReplicateCreatePartition,
			CollectionInfo: collectionInfo,
			PartitionInfo:  partitionInfo,
			ReplicateInfo: &commonpb.ReplicateInfo{
				IsReplicate:  true,
				MsgTimestamp: partitionInfo.PartitionCreatedTimestamp,
			},
			ReplicateParam: api.ReplicateParam{Database: sourceDBInfo.Name},
		}:
		case <-ctx.Done():
			log.Warn("context is done when adding partition")
			return ctx.Err()
		}
	}
	log.Info("start to add partition", zap.String("collection_name", collectionInfo.Schema.Name), zap.String("partition_name", partitionInfo.PartitionName), zap.Int("num", len(handlers)))
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
			ReplicateParam: api.ReplicateParam{Database: sourceDBInfo.Name},
		}:
			for _, handler := range handlers {
				handler.RemovePartitionInfo(collectionID, partitionInfo.PartitionName, partitionInfo.PartitionID)
			}
		}
	})
	r.partitionLock.Lock()
	if _, ok := r.replicatePartitions[collectionID]; !ok {
		r.replicatePartitions[collectionID] = make(map[int64]chan struct{})
	}
	r.replicatePartitions[collectionID][partitionInfo.PartitionID] = barrier.CloseChan
	r.partitionLock.Unlock()
	for _, handler := range handlers {
		err = handler.AddPartitionInfo(collectionInfo, partitionInfo, barrier.BarrierChan)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *replicateChannelManager) StopReadCollection(ctx context.Context, info *pb.CollectionInfo) error {
	for _, position := range info.StartPositions {
		r.stopReadChannel(position.GetKey(), info.ID)
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
	return r.channelChan
}

func (r *replicateChannelManager) GetMsgChan(pChannel string) <-chan *msgstream.MsgPack {
	r.channelLock.RLock()
	defer r.channelLock.RUnlock()

	handler := r.channelHandlerMap[pChannel]
	if handler != nil {
		return handler.msgPackChan
	}
	return nil
}

func (r *replicateChannelManager) GetEventChan() <-chan *api.ReplicateAPIEvent {
	return r.apiEventChan
}

// startReadChannel start read channel
// pChannelName: source milvus channel name, collectionID: source milvus collection id, startPosition: start position of the source milvus collection
// targetInfo: target collection info, it will be used to replace the message info in the source milvus channel
func (r *replicateChannelManager) startReadChannel(sourceInfo *model.SourceCollectionInfo, targetInfo *model.TargetCollectionInfo) error {
	r.channelLock.Lock()
	defer r.channelLock.Unlock()
	var err error

	channelHandler, ok := r.channelHandlerMap[sourceInfo.PChannelName]
	if !ok {
		channelHandler, err = newReplicateChannelHandler(r.getCtx(),
			sourceInfo, targetInfo,
			r.targetClient, r.metaOp, r.apiEventChan,
			&model.HandlerOpts{MessageBufferSize: r.messageBufferSize, Factory: r.factory})
		if err != nil {
			log.Warn("fail to new replicate channel handler",
				zap.String("channel_name", sourceInfo.PChannelName), zap.Int64("collection_id", sourceInfo.CollectionID), zap.Error(err))
			return err
		}
		channelHandler.msgPackCallback = r.msgPackCallback
		r.channelHandlerMap[sourceInfo.PChannelName] = channelHandler
		r.channelChan <- sourceInfo.PChannelName
		return nil
	}
	channelHandler.AddCollection(sourceInfo.CollectionID, targetInfo)
	return nil
}

func (r *replicateChannelManager) stopReadChannel(pChannelName string, collectionID int64) {
	r.channelLock.Lock()
	defer r.channelLock.Unlock()
	channelHandler, ok := r.channelHandlerMap[pChannelName]
	if !ok {
		return
	}
	channelHandler.RemoveCollection(collectionID)
	if channelHandler.IsEmpty() {
		channelHandler.Close()
	}
}

type replicateChannelHandler struct {
	replicateCtx   context.Context
	pChannelName   string
	targetPChannel string
	stream         msgstream.MsgStream
	targetClient   api.TargetAPI
	metaOp         api.MetaOp
	// key: source milvus collectionID value: *model.TargetCollectionInfo
	recordLock        sync.RWMutex
	collectionRecords map[int64]*model.TargetCollectionInfo
	collectionNames   map[string]int64
	msgPackChan       chan *msgstream.MsgPack
	apiEventChan      chan *api.ReplicateAPIEvent
	msgPackCallback   func(string, *msgstream.MsgPack)
}

func (r *replicateChannelHandler) AddCollection(collectionID int64, targetInfo *model.TargetCollectionInfo) {
	r.recordLock.Lock()
	defer r.recordLock.Unlock()
	r.collectionRecords[collectionID] = targetInfo
	r.collectionNames[targetInfo.CollectionName] = collectionID
}

func (r *replicateChannelHandler) RemoveCollection(collectionID int64) {
	r.recordLock.Lock()
	defer r.recordLock.Unlock()
	collectionRecord := r.collectionRecords[collectionID]
	delete(r.collectionRecords, collectionID)
	if collectionRecord != nil {
		delete(r.collectionNames, collectionRecord.CollectionName)
	}
}

func (r *replicateChannelHandler) AddPartitionInfo(collectionInfo *pb.CollectionInfo, partitionInfo *pb.PartitionInfo, barrierChan chan<- uint64) error {
	collectionID := collectionInfo.ID
	partitionID := partitionInfo.PartitionID
	collectionName := collectionInfo.Schema.Name
	partitionName := partitionInfo.PartitionName

	targetInfo, err := r.getCollectionTargetInfo(collectionID)
	if err != nil {
		return err
	}
	r.recordLock.Lock()
	defer r.recordLock.Unlock()
	if targetInfo.PartitionBarrierChan[partitionID] != nil {
		log.Info("the partition barrier chan is not nil", zap.Int64("collection_id", collectionID), zap.String("partition_name", partitionName), zap.Int64("partition_id", partitionID))
		return nil
	}
	targetInfo.PartitionBarrierChan[partitionID] = barrierChan
	// TODO use goroutine pool
	go func() {
		_ = retry.Do(r.replicateCtx, func() error {
			id := r.updateTargetPartitionInfo(collectionID, collectionName, partitionName)
			if id == 0 {
				return errors.Newf("not found the partition [%s]", partitionName)
			}
			return nil
		}, util.GetRetryOptionsFor25s()...)
	}()
	return nil
}

func (r *replicateChannelHandler) updateTargetPartitionInfo(collectionID int64, collectionName string, partitionName string) int64 {
	ctx, cancelFunc := context.WithTimeout(r.replicateCtx, 10*time.Second)
	defer cancelFunc()
	sourceDBInfo := r.metaOp.GetDatabaseInfoForCollection(ctx, collectionID)

	collectionInfo, err := r.targetClient.GetPartitionInfo(ctx, collectionName, sourceDBInfo.Name)
	if err != nil {
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
	return targetInfo.PartitionInfo[partitionName]
}

func (r *replicateChannelHandler) RemovePartitionInfo(collectionID int64, name string, id int64) {
	targetInfo, err := r.getCollectionTargetInfo(collectionID)
	if err != nil {
		log.Warn("fail to get collection target info", zap.Int64("collection_id", collectionID), zap.Error(err))
		return
	}
	r.recordLock.Lock()
	defer r.recordLock.Unlock()
	if targetInfo.PartitionInfo[name] == id {
		delete(targetInfo.PartitionInfo, name)
	}
	delete(targetInfo.PartitionBarrierChan, id)
}

func (r *replicateChannelHandler) IsEmpty() bool {
	r.recordLock.RLock()
	defer r.recordLock.RUnlock()
	return len(r.collectionRecords) == 0
}

func (r *replicateChannelHandler) Close() {
	r.stream.Close()
}

func (r *replicateChannelHandler) startReadChannel() {
	go func() {
		for {
			msgPack, ok := <-r.stream.Chan()
			if !ok {
				close(r.msgPackChan)
				return
			}
			r.msgPackChan <- r.handlePack(msgPack)
		}
	}()
}

func (r *replicateChannelHandler) getCollectionTargetInfo(collectionID int64) (*model.TargetCollectionInfo, error) {
	r.recordLock.RLock()
	targetInfo, ok := r.collectionRecords[collectionID]
	r.recordLock.RUnlock()
	if ok {
		return targetInfo, nil
	}

	// Sleep for a short time to avoid invalid requests
	time.Sleep(500 * time.Millisecond)
	err := retry.Do(r.replicateCtx, func() error {
		log.Warn("wait collection info",
			zap.Int64("msg_collection_id", collectionID))
		r.recordLock.RLock()
		var xs []int64
		for x := range r.collectionRecords {
			xs = append(xs, x)
		}
		log.Info("collection records", zap.Any("records", xs))
		// TODO it needs to be considered when supporting the specific collection in a task
		targetInfo, ok = r.collectionRecords[collectionID]
		r.recordLock.RUnlock()
		if ok {
			return nil
		}
		return errors.Newf("not found the collection [%d]", collectionID)
	}, util.GetRetryOptionsFor9s()...)
	if err != nil {
		log.Warn("fail to find the collection info", zap.Error(err))
		return nil, err
	}
	return targetInfo, nil
}

func (r *replicateChannelHandler) containCollection(collectionName string) bool {
	r.recordLock.RLock()
	defer r.recordLock.RUnlock()
	return r.collectionNames[collectionName] != 0
}

func (r *replicateChannelHandler) getPartitionID(sourceCollectionID int64, info *model.TargetCollectionInfo, name string) (int64, error) {
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
		return errors.Newf("not found the partition [%s]", name)
	})
	if err != nil {
		log.Warn("fail to find the partition id", zap.Int64("source_collection", sourceCollectionID), zap.Any("target_collection", info.CollectionID), zap.String("partition_name", name))
		return 0, err
	}
	return id, nil
}

func (r *replicateChannelHandler) handlePack(pack *msgstream.MsgPack) *msgstream.MsgPack {
	newPack := &msgstream.MsgPack{
		BeginTs:        pack.BeginTs,
		EndTs:          pack.EndTs,
		StartPositions: pack.StartPositions,
		EndPositions:   pack.EndPositions,
		Msgs:           make([]msgstream.TsMsg, 0),
	}
	needTsMsg := false
	pChannel := r.targetPChannel
	for _, msg := range pack.Msgs {
		if msg.Type() == commonpb.MsgType_CreateCollection ||
			msg.Type() == commonpb.MsgType_CreatePartition {
			continue
		}
		if msg.Type() == commonpb.MsgType_TimeTick {
			newPack.Msgs = append(newPack.Msgs, msg)
			continue
		}
		if msg.Type() != commonpb.MsgType_Insert && msg.Type() != commonpb.MsgType_Delete &&
			msg.Type() != commonpb.MsgType_DropCollection && msg.Type() != commonpb.MsgType_DropPartition {
			log.Warn("not support msg type", zap.Any("msg", msg))
			continue
		}

		if y, ok := msg.(interface{ GetCollectionID() int64 }); ok {
			sourceCollectionID := y.GetCollectionID()
			info, err := r.getCollectionTargetInfo(sourceCollectionID)
			if err != nil {
				r.sendErrEvent(err)
				log.Warn("fail to get collection info", zap.Int64("collection_id", sourceCollectionID), zap.Error(err))
				return nil
			}
			var dataLen int
			switch realMsg := msg.(type) {
			case *msgstream.InsertMsg:
				realMsg.CollectionID = info.CollectionID
				realMsg.PartitionID, err = r.getPartitionID(sourceCollectionID, info, realMsg.PartitionName)
				realMsg.ShardName = info.VChannel
				dataLen = int(realMsg.GetNumRows())
			case *msgstream.DeleteMsg:
				realMsg.CollectionID = info.CollectionID
				if realMsg.PartitionName != "" {
					realMsg.PartitionID, err = r.getPartitionID(sourceCollectionID, info, realMsg.PartitionName)
				}
				realMsg.ShardName = info.VChannel
				dataLen = int(realMsg.GetNumRows())
			case *msgstream.DropCollectionMsg:
				realMsg.CollectionID = info.CollectionID
				info.BarrierChan <- msg.EndTs()
				needTsMsg = true
			case *msgstream.DropPartitionMsg:
				realMsg.CollectionID = info.CollectionID
				if realMsg.PartitionName == "" || info.PartitionBarrierChan[realMsg.PartitionID] == nil {
					err = errors.Newf("not found the partition info [%d]", realMsg.PartitionID)
					log.Warn("invalid drop partition message", zap.Any("msg", msg))
				} else {
					info.PartitionBarrierChan[realMsg.PartitionID] <- msg.EndTs()
					if realMsg.PartitionName != "" {
						realMsg.PartitionID, err = r.getPartitionID(sourceCollectionID, info, realMsg.PartitionName)
					}
				}
			}
			if err != nil {
				r.sendErrEvent(err)
				log.Warn("fail to get partition info", zap.Any("msg", msg.Type()), zap.Error(err))
				return nil
			}
			if pChannel != info.PChannel {
				r.sendErrEvent(errors.New("there is a error about the replicate channel"))
				log.Warn("pChannel not equal", zap.Any("msg", msg), zap.String("pChannel", pChannel), zap.String("info_pChannel", info.PChannel))
				return nil
			}
			originPosition := msg.Position()
			msg.SetPosition(&msgpb.MsgPosition{
				ChannelName: info.PChannel,
				MsgID:       originPosition.GetMsgID(),
				MsgGroup:    originPosition.GetMsgGroup(),
				Timestamp:   originPosition.GetTimestamp(),
			})
			logFields := []zap.Field{
				zap.String("msg", msg.Type().String()),
			}
			if dataLen != 0 {
				logFields = append(logFields, zap.Int("data_len", dataLen))
			}
			log.Info("receive msg", logFields...)
			newPack.Msgs = append(newPack.Msgs, msg)
		} else {
			log.Warn("not support msg type", zap.Any("msg", msg))
		}
	}
	for _, position := range newPack.StartPositions {
		position.ChannelName = pChannel
	}
	for _, position := range newPack.EndPositions {
		position.ChannelName = pChannel
	}
	needTsMsg = needTsMsg || len(newPack.Msgs) == 0
	if needTsMsg {
		timeTickResult := msgpb.TimeTickMsg{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_TimeTick),
				commonpbutil.WithMsgID(0),
				commonpbutil.WithTimeStamp(pack.EndTs),
				commonpbutil.WithSourceID(-1),
			),
		}
		timeTickMsg := &msgstream.TimeTickMsg{
			BaseMsg: msgstream.BaseMsg{
				BeginTimestamp: pack.EndTs,
				EndTimestamp:   pack.EndTs,
				HashValues:     []uint32{0},
				MsgPosition:    newPack.EndPositions[0],
			},
			TimeTickMsg: timeTickResult,
		}
		newPack.Msgs = append(newPack.Msgs, timeTickMsg)
	}
	if r.msgPackCallback != nil {
		r.msgPackCallback(r.pChannelName, newPack)
	}
	return newPack
}

func (r *replicateChannelHandler) sendErrEvent(err error) {
	r.apiEventChan <- &api.ReplicateAPIEvent{
		EventType: api.ReplicateError,
		Error:     err,
	}
}

func newReplicateChannelHandler(ctx context.Context,
	sourceInfo *model.SourceCollectionInfo, targetInfo *model.TargetCollectionInfo,
	targetClient api.TargetAPI, metaOp api.MetaOp,
	apiEventChan chan *api.ReplicateAPIEvent, opts *model.HandlerOpts,
) (*replicateChannelHandler, error) {
	stream, err := opts.Factory.NewTtMsgStream(ctx)
	log := log.With(zap.String("channel_name", sourceInfo.PChannelName), zap.Int64("collection_id", sourceInfo.CollectionID))
	if err != nil {
		log.Warn("fail to new the msg stream", zap.Error(err))
		return nil, err
	}
	subPositionType := mqwrapper.SubscriptionPositionUnknown
	if sourceInfo.SeekPosition == nil {
		subPositionType = mqwrapper.SubscriptionPositionLatest
	}
	err = stream.AsConsumer(ctx, []string{sourceInfo.PChannelName}, sourceInfo.PChannelName+strconv.Itoa(rand.Int()), subPositionType)
	if err != nil {
		log.Warn("fail to consume the channel", zap.Error(err))
		stream.Close()
		return nil, err
	}
	if sourceInfo.SeekPosition != nil {
		err = stream.Seek(ctx, []*msgstream.MsgPosition{sourceInfo.SeekPosition})
		if err != nil {
			log.Warn("fail to seek the msg stream", zap.Error(err))
			stream.Close()
			return nil, err
		}
	}
	channelHandler := &replicateChannelHandler{
		replicateCtx:      ctx,
		pChannelName:      sourceInfo.PChannelName,
		targetPChannel:    targetInfo.PChannel,
		stream:            stream,
		targetClient:      targetClient,
		metaOp:            metaOp,
		collectionRecords: make(map[int64]*model.TargetCollectionInfo),
		collectionNames:   make(map[string]int64),
		msgPackChan:       make(chan *msgstream.MsgPack, opts.MessageBufferSize),
		apiEventChan:      apiEventChan,
	}
	channelHandler.AddCollection(sourceInfo.CollectionID, targetInfo)
	channelHandler.startReadChannel()
	return channelHandler, nil
}
