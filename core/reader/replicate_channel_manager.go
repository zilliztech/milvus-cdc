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
)

var _ api.ChannelManager = (*replicateChannelManager)(nil)

type replicateChannelManager struct {
	factory           msgstream.Factory
	apiEventChan      chan *api.ReplicateAPIEvent
	targetClient      api.TargetAPI
	messageBufferSize int

	channelLock       sync.RWMutex
	channelHandlerMap map[string]*replicateChannelHandler

	collectionLock       sync.Mutex
	replicateCollections map[int64]chan struct{}
	channelChan          chan string
}

func NewReplicateChannelManager(mqConfig config.MQConfig, client api.TargetAPI, messageBufferSize int) (api.ChannelManager, error) {
	factoryCreator := NewDefaultFactoryCreator()
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
		messageBufferSize:    messageBufferSize,
		channelHandlerMap:    make(map[string]*replicateChannelHandler),
		replicateCollections: make(map[int64]chan struct{}),
		channelChan:          make(chan string, 10),
	}, nil
}

func (r *replicateChannelManager) StartReadCollection(ctx context.Context, info *pb.CollectionInfo, seekPositions []*msgpb.MsgPosition) error {
	if _, err := r.targetClient.GetCollectionInfo(ctx, info.Schema.GetName()); err != nil {
		select {
		case r.apiEventChan <- &api.ReplicateAPIEvent{
			EventType:      api.ReplicateCreateCollection,
			CollectionInfo: info,
		}:
		case <-ctx.Done():
			log.Warn("context is done in the start read collection")
			return ctx.Err()
		}
	}

	var targetInfo *model.CollectionInfo
	var err error

	err = retry.Do(ctx, func() error {
		targetInfo, err = r.targetClient.GetCollectionInfo(ctx, info.Schema.Name)
		return err
	}, retry.Sleep(time.Second), retry.MaxSleepTime(10*time.Second), retry.Attempts(5))
	if err != nil {
		log.Warn("failed to get target collection info", zap.Error(err))
		return err
	}

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

	var barrierChan chan struct{}
	r.collectionLock.Lock()
	if _, ok := r.replicateCollections[info.ID]; ok {
		r.collectionLock.Unlock()
		return nil
	}
	barrierChan = make(chan struct{}, len(info.StartPositions))
	closeChan := make(chan struct{})
	r.replicateCollections[info.ID] = closeChan
	go func(dest int) {
		current := 0
		for current < dest {
			select {
			case <-closeChan:
				return
			case <-barrierChan:
				current++
			}
		}
		select {
		case <-closeChan:
		case r.apiEventChan <- &api.ReplicateAPIEvent{
			EventType:      api.ReplicateDropCollection,
			CollectionInfo: info,
		}:
		}
	}(len(info.StartPositions))
	r.collectionLock.Unlock()

	var successChannels []string
	for i, position := range info.StartPositions {
		channelName := position.GetKey()
		err := r.startReadChannel(&model.SourceCollectionInfo{
			PChannelName: channelName,
			CollectionID: info.ID,
			SeekPosition: getSeekPosition(channelName),
		}, &model.TargetCollectionInfo{
			CollectionID:  targetInfo.CollectionID,
			PartitionInfo: targetInfo.Partitions,
			PChannel:      targetInfo.PChannels[i],
			VChannel:      targetInfo.VChannels[i],
			BarrierChan:   barrierChan,
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

func (r *replicateChannelManager) StopReadCollection(ctx context.Context, info *pb.CollectionInfo) error {
	for _, position := range info.StartPositions {
		r.stopReadChannel(position.GetKey(), info.ID)
	}
	r.collectionLock.Lock()
	defer r.collectionLock.Unlock()
	closeChan, ok := r.replicateCollections[info.ID]
	if ok {
		close(closeChan)
		delete(r.replicateCollections, info.ID)
	}
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
		channelHandler, err = newReplicateChannelHandler(sourceInfo, targetInfo, &model.HandlerOpts{
			MessageBufferSize: r.messageBufferSize,
			Factory:           r.factory,
		})
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
	// key: source milvus collectionID value: *model.TargetCollectionInfo
	collectionRecords *sync.Map
	msgPackChan       chan *msgstream.MsgPack
}

func (r *replicateChannelHandler) AddCollection(collectionID int64, targetInfo *model.TargetCollectionInfo) {
	r.collectionRecords.Store(collectionID, targetInfo)
}

func (r *replicateChannelHandler) RemoveCollection(collectionID int64) {
	r.collectionRecords.Delete(collectionID)
}

func (r *replicateChannelHandler) IsEmpty() bool {
	isEmpty := true
	r.collectionRecords.Range(func(key, value interface{}) bool {
		isEmpty = false
		return false
	})
	return isEmpty
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
			// if startTs {
			// 	log.Info("first pack", zap.Any("msg_pack", msgPack))
			// 	beginTs := msgPack.EndTs
			// 	msgPosition := msgPack.EndPositions[0]
			// 	for _, msg := range msgPack.Msgs {
			// 		if msg.EndTs() < beginTs {
			// 			beginTs = msg.EndTs()
			// 			msgPosition = msg.Position()
			// 		}
			// 	}
			// 	if beginTs != msgPack.EndTs {
			// 		endPosition := &msgpb.MsgPosition{
			// 			ChannelName: r.targetPChannel,
			// 			MsgID:       msgPosition.MsgID,
			// 			MsgGroup:    msgPack.EndPositions[0].MsgGroup,
			// 			Timestamp:   beginTs,
			// 		}
			// 		newPack := &msgstream.MsgPack{
			// 			BeginTs:        beginTs,
			// 			EndTs:          beginTs,
			// 			StartPositions: []*msgpb.MsgPosition{endPosition},
			// 			EndPositions:   []*msgpb.MsgPosition{endPosition},
			// 			Msgs:           make([]msgstream.TsMsg, 0),
			// 		}
			// 		timeTickResult := msgpb.TimeTickMsg{
			// 			Base: commonpbutil.NewMsgBase(
			// 				commonpbutil.WithMsgType(commonpb.MsgType_TimeTick),
			// 				commonpbutil.WithMsgID(0),
			// 				commonpbutil.WithTimeStamp(beginTs),
			// 				commonpbutil.WithSourceID(-1),
			// 			),
			// 		}
			// 		timeTickMsg := &msgstream.TimeTickMsg{
			// 			BaseMsg: msgstream.BaseMsg{
			// 				BeginTimestamp: beginTs,
			// 				EndTimestamp:   beginTs,
			// 				HashValues:     []uint32{0},
			// 			},
			// 			TimeTickMsg: timeTickResult,
			// 		}
			// 		newPack.Msgs = append(newPack.Msgs, timeTickMsg)
			// 		r.msgPackChan <- newPack
			// 		log.Info("send extra ts pack", zap.Any("msg_pack", newPack))
			// 	}
			// 	startTs = false
			// }
			r.msgPackChan <- r.handlePack(msgPack)
		}
	}()
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
		if y, ok := msg.(interface{ GetCollectionID() int64 }); ok {
			targetInfo, ok := r.collectionRecords.Load(y.GetCollectionID())
			i := 0
			for !ok && i < 10 {
				log.Warn("filter msg in replicate channel handler",
					zap.Any("current_collections", r.collectionRecords),
					zap.Int64("msg_collection_id", y.GetCollectionID()),
					zap.Any("msg_type", msg.Type()))
				time.Sleep(500 * time.Millisecond)
				// TODO it needs to be considered when supporting the specific collection in a task
				// TODO maybe wait too long time?
				targetInfo, ok = r.collectionRecords.Load(y.GetCollectionID())
				i++
			}
			if !ok && i == 10 {
				log.Warn("filter msg in replicate channel handler", zap.Int64("msg_collection_id", y.GetCollectionID()), zap.Any("msg_type", msg.Type()))
				continue
			}
			info := targetInfo.(*model.TargetCollectionInfo)
			switch realMsg := msg.(type) {
			case *msgstream.InsertMsg:
				realMsg.CollectionID = info.CollectionID
				realMsg.PartitionID = info.PartitionInfo[realMsg.PartitionName]
				realMsg.ShardName = info.VChannel
			case *msgstream.DeleteMsg:
				realMsg.CollectionID = info.CollectionID
				if realMsg.PartitionName != "" {
					realMsg.PartitionID = info.PartitionInfo[realMsg.PartitionName]
				}
				realMsg.ShardName = info.VChannel
			case *msgstream.DropCollectionMsg:
				realMsg.CollectionID = info.CollectionID
				info.BarrierChan <- struct{}{}
				needTsMsg = true
			case *msgstream.DropPartitionMsg:
				realMsg.CollectionID = info.CollectionID
				if realMsg.PartitionName != "" {
					realMsg.PartitionID = info.PartitionInfo[realMsg.PartitionName]
				}
				// TODO barrier partition
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
		}
		newPack.Msgs = append(newPack.Msgs, msg)
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

func newReplicateChannelHandler(sourceInfo *model.SourceCollectionInfo, targetInfo *model.TargetCollectionInfo, opts *model.HandlerOpts) (*replicateChannelHandler, error) {
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
		collectionRecords: &sync.Map{},
		stream:            stream,
		msgPackChan:       make(chan *msgstream.MsgPack, opts.MessageBufferSize),
	}
	channelHandler.AddCollection(sourceInfo.CollectionID, targetInfo)
	channelHandler.startReadChannel()
	return channelHandler, nil
}
