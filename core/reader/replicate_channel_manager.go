package reader

import (
	"context"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/model"
	"github.com/zilliztech/milvus-cdc/core/pb"
	"github.com/zilliztech/milvus-cdc/core/util"
)

var _ api.ChannelManager = (*replicateChannelManager)(nil)

type replicateChannelManager struct {
	factory           msgstream.Factory
	apiEventChan      chan *api.ReplicateAPIEvent
	targetClient      api.TargetAPI
	retryOptions      []retry.Option
	messageBufferSize int

	channelLock       sync.RWMutex
	channelHandlerMap map[string]*replicateChannelHandler

	collectionLock       sync.Mutex
	replicateCollections map[int64]chan struct{}

	partitionLock       sync.Mutex
	replicatePartitions map[int64]map[int64]chan struct{}

	channelChan chan string
}

func NewReplicateChannelManager(mqConfig config.MQConfig, factoryCreator FactoryCreator, client api.TargetAPI, messageBufferSize int) (api.ChannelManager, error) {
	var factory msgstream.Factory
	if mqConfig.Pulsar.Address != "" {
		factory = factoryCreator.NewPmsFactory(&mqConfig.Pulsar)
	} else if mqConfig.Kafka.Address != "" {
		factory = factoryCreator.NewKmsFactory(&mqConfig.Kafka)
	} else {
		log.Warn("mqConfig is empty")
		return nil, errors.New("fail to get the msg stream, check the mqConfig param")
	}

	return &replicateChannelManager{
		factory:              factory,
		apiEventChan:         make(chan *api.ReplicateAPIEvent, 10),
		targetClient:         client,
		retryOptions:         util.GetRetryOptionsFor25s(),
		messageBufferSize:    messageBufferSize,
		channelHandlerMap:    make(map[string]*replicateChannelHandler),
		replicateCollections: make(map[int64]chan struct{}),
		replicatePartitions:  make(map[int64]map[int64]chan struct{}),
		channelChan:          make(chan string, 10),
	}, nil
}

func (r *replicateChannelManager) StartReadCollection(ctx context.Context, info *pb.CollectionInfo, seekPositions []*msgpb.MsgPosition) error {
	if _, err := r.targetClient.GetCollectionInfo(ctx, info.Schema.GetName()); err != nil {
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
			}
		}
	}

	var targetInfo *model.CollectionInfo
	var err error

	err = retry.Do(ctx, func() error {
		targetInfo, err = r.targetClient.GetCollectionInfo(ctx, info.Schema.Name)
		return err
	}, r.retryOptions...)
	if err != nil {
		log.Warn("failed to get target collection info", zap.Error(err))
		return err
	}
	log.Info("success to get the collection info in the target instance", zap.String("collection_name", targetInfo.CollectionName))

	for i, channel := range targetInfo.PChannels {
		if !strings.Contains(targetInfo.VChannels[i], channel) {
			log.Warn("physical channel not equal", zap.Strings("p", targetInfo.PChannels), zap.Strings("v", targetInfo.VChannels))
			return errors.New("the physical channels are not matched to the virtual channels")
		}
	}

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
		}:
		}
	})
	r.replicateCollections[info.ID] = barrier.CloseChan
	r.collectionLock.Unlock()

	var successChannels []string
	for i, position := range info.StartPositions {
		channelName := position.GetKey()
		err := r.startReadChannel(&model.SourceCollectionInfo{
			PChannelName: channelName,
			CollectionID: info.ID,
			SeekPosition: getSeekPosition(channelName),
		}, &model.TargetCollectionInfo{
			CollectionID:         targetInfo.CollectionID,
			CollectionName:       info.Schema.Name,
			PartitionInfo:        targetInfo.Partitions,
			PChannel:             targetInfo.PChannels[i],
			VChannel:             targetInfo.VChannels[i],
			BarrierChan:          barrier.BarrierChan,
			PartitionBarrierChan: make(map[int64]chan<- uint64),
		})
		if err != nil {
			log.Warn("start read channel failed", zap.String("channel", channelName), zap.Int64("collection_id", info.ID), zap.Error(err))
			for _, channel := range successChannels {
				r.stopReadChannel(channel, info.ID)
			}
			return err
		}
		successChannels = append(successChannels, channelName)
		log.Info("start read channel", zap.String("channel", channelName))
	}
	return err
}

func (r *replicateChannelManager) AddPartition(ctx context.Context, collectionInfo *pb.CollectionInfo, partitionInfo *pb.PartitionInfo) error {
	var handlers []*replicateChannelHandler
	collectionID := collectionInfo.ID
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
		log.Warn("no handler found", zap.Int64("collection_id", collectionID))
		return errors.New("no handler found")
	}

	firstHandler := handlers[0]
	partitionRecord := firstHandler.getCollectionTargetInfo(collectionID).PartitionInfo
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
		}:
		case <-ctx.Done():
			log.Warn("context is done when adding partition")
			return ctx.Err()
		}
	}
	log.Warn("start to add partition", zap.String("collection_name", collectionInfo.Schema.Name), zap.String("partition_name", partitionInfo.PartitionName), zap.Int("num", len(handlers)))
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
		handler.AddPartitionInfo(collectionInfo, partitionInfo, barrier.BarrierChan)
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
		channelHandler, err = newReplicateChannelHandler(sourceInfo, targetInfo, r.targetClient, &model.HandlerOpts{MessageBufferSize: r.messageBufferSize, Factory: r.factory})
		if err != nil {
			log.Warn("fail to new replicate channel handler",
				zap.String("channel_name", sourceInfo.PChannelName), zap.Int64("collection_id", sourceInfo.CollectionID), zap.Error(err))
			return err
		}
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
	pChannelName   string
	targetPChannel string
	stream         msgstream.MsgStream
	targetClient   api.TargetAPI
	// key: source milvus collectionID value: *model.TargetCollectionInfo
	recordLock        sync.RWMutex
	collectionRecords map[int64]*model.TargetCollectionInfo
	collectionNames   map[string]int64
	msgPackChan       chan *msgstream.MsgPack
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

func (r *replicateChannelHandler) AddPartitionInfo(collectionInfo *pb.CollectionInfo, partitionInfo *pb.PartitionInfo, barrierChan chan<- uint64) {
	collectionID := collectionInfo.ID
	partitionID := partitionInfo.PartitionID
	collectionName := collectionInfo.Schema.Name
	partitionName := partitionInfo.PartitionName

	targetInfo := r.getCollectionTargetInfo(collectionID)
	r.recordLock.Lock()
	defer r.recordLock.Unlock()
	if targetInfo.PartitionBarrierChan[partitionID] != nil {
		log.Info("the partition barrier chan is not nil", zap.Int64("collection_id", collectionID), zap.String("partition_name", partitionName), zap.Int64("partition_id", partitionID))
		return
	}
	targetInfo.PartitionBarrierChan[partitionID] = barrierChan
	go func() {
		_ = retry.Do(context.Background(), func() error {
			id := r.updateTargetPartitionInfo(collectionID, collectionName, partitionName)
			if id == 0 {
				return errors.Newf("not found the partition [%s]", partitionName)
			}
			return nil
		}, util.GetRetryOptionsFor25s()...)
	}()
}

func (r *replicateChannelHandler) updateTargetPartitionInfo(collectionID int64, collectionName string, partitionName string) int64 {
	collectionInfo, err := r.targetClient.GetPartitionInfo(context.Background(), collectionName)
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
	targetInfo := r.getCollectionTargetInfo(collectionID)
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

func (r *replicateChannelHandler) Chan() chan<- *msgstream.MsgPack {
	return r.msgPackChan
}

func (r *replicateChannelHandler) Close() {
	r.stream.Close()
}

func (r *replicateChannelHandler) startReadChannel() {
	go func() {
		// startTs := true
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

func (r *replicateChannelHandler) getCollectionTargetInfo(collectionID int64) *model.TargetCollectionInfo {
	r.recordLock.RLock()
	targetInfo, ok := r.collectionRecords[collectionID]
	r.recordLock.RUnlock()
	i := 0
	for !ok && i < 10 {
		log.Warn("wait collection info",
			zap.Int64("msg_collection_id", collectionID))
		time.Sleep(500 * time.Millisecond)
		r.recordLock.RLock()
		var xs []int64
		for x := range r.collectionRecords {
			xs = append(xs, x)
		}
		log.Info("collection records", zap.Any("xs", xs))
		// TODO it needs to be considered when supporting the specific collection in a task
		targetInfo, ok = r.collectionRecords[collectionID]
		r.recordLock.RUnlock()
		i++
	}
	if !ok && i == 10 {
		log.Panic("fail to find the collection info", zap.Int64("msg_collection_id", collectionID))
	}
	return targetInfo
}

func (r *replicateChannelHandler) containCollection(collectionName string) bool {
	r.recordLock.RLock()
	defer r.recordLock.RUnlock()
	return r.collectionNames[collectionName] != 0
}

func (r *replicateChannelHandler) getPartitionID(sourceCollectionID int64, info *model.TargetCollectionInfo, name string) int64 {
	r.recordLock.RLock()
	id, ok := info.PartitionInfo[name]
	r.recordLock.RUnlock()
	i := 0
	for !ok && i < 10 {
		log.Warn("wait partition info", zap.Int64("collection_id", info.CollectionID), zap.String("partition_name", name))
		time.Sleep(500 * time.Millisecond)
		id = r.updateTargetPartitionInfo(sourceCollectionID, info.CollectionName, name)
		ok = id != 0
		i++
	}
	if !ok && i == 10 {
		log.Panic("fail to find the partition id", zap.Int64("collection_id", info.CollectionID), zap.String("partition_name", name))
	}
	return id
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
			info := r.getCollectionTargetInfo(sourceCollectionID)
			switch realMsg := msg.(type) {
			case *msgstream.InsertMsg:
				realMsg.CollectionID = info.CollectionID
				realMsg.PartitionID = r.getPartitionID(sourceCollectionID, info, realMsg.PartitionName)
				realMsg.ShardName = info.VChannel
			case *msgstream.DeleteMsg:
				realMsg.CollectionID = info.CollectionID
				if realMsg.PartitionName != "" {
					realMsg.PartitionID = r.getPartitionID(sourceCollectionID, info, realMsg.PartitionName)
				}
				realMsg.ShardName = info.VChannel
			case *msgstream.DropCollectionMsg:
				realMsg.CollectionID = info.CollectionID
				info.BarrierChan <- msg.EndTs()
				needTsMsg = true
			case *msgstream.DropPartitionMsg:
				realMsg.CollectionID = info.CollectionID
				if realMsg.PartitionName == "" || info.PartitionBarrierChan[realMsg.PartitionID] == nil {
					log.Panic("drop partition msg", zap.Any("msg", msg))
				}
				info.PartitionBarrierChan[realMsg.PartitionID] <- msg.EndTs()
				if realMsg.PartitionName != "" {
					realMsg.PartitionID = r.getPartitionID(sourceCollectionID, info, realMsg.PartitionName)
				}
			}
			originPosition := msg.Position()
			msg.SetPosition(&msgpb.MsgPosition{
				ChannelName: info.PChannel,
				MsgID:       originPosition.GetMsgID(),
				MsgGroup:    originPosition.GetMsgGroup(),
				Timestamp:   originPosition.GetTimestamp(),
			})
			if pChannel != info.PChannel {
				log.Panic("pChannel not equal", zap.String("pChannel", pChannel), zap.String("info_pChannel", info.PChannel))
			}
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
	if len(newPack.Msgs) != 0 {
		log.Info("receive msg pack", zap.Any("msg_pack", newPack))
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
			},
			TimeTickMsg: timeTickResult,
		}
		newPack.Msgs = append(newPack.Msgs, timeTickMsg)
	}
	return newPack
}

func newReplicateChannelHandler(
	sourceInfo *model.SourceCollectionInfo,
	targetInfo *model.TargetCollectionInfo,
	targetClient api.TargetAPI,
	opts *model.HandlerOpts,
) (*replicateChannelHandler, error) {
	ctx := context.Background()
	stream, err := opts.Factory.NewTtMsgStream(ctx)
	log := log.With(zap.String("channel_name", sourceInfo.PChannelName), zap.Int64("collection_id", sourceInfo.CollectionID))
	if err != nil {
		log.Warn("fail to new the msg stream", zap.Error(err))
		return nil, err
	}
	err = stream.AsConsumer(ctx, []string{sourceInfo.PChannelName}, sourceInfo.PChannelName+strconv.Itoa(rand.Int()), mqwrapper.SubscriptionPositionLatest)
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
		pChannelName:      sourceInfo.PChannelName,
		targetPChannel:    targetInfo.PChannel,
		stream:            stream,
		targetClient:      targetClient,
		collectionRecords: make(map[int64]*model.TargetCollectionInfo),
		collectionNames:   make(map[string]int64),
		msgPackChan:       make(chan *msgstream.MsgPack, opts.MessageBufferSize),
	}
	channelHandler.AddCollection(sourceInfo.CollectionID, targetInfo)
	channelHandler.startReadChannel()
	return channelHandler, nil
}
