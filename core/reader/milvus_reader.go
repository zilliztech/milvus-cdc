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
	"encoding/json"
	"errors"
	"math/rand"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/milvus-io/milvus/pkg/common"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/samber/lo"
	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/model"
	"github.com/zilliztech/milvus-cdc/core/pb"
	"github.com/zilliztech/milvus-cdc/core/util"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

var log = util.Log

const (
	AllCollection = "*"
)

type CollectionInfo struct {
	collectionName string
	positions      map[string]*commonpb.KeyDataPair
}

type ShouldReadFunc func(*pb.CollectionInfo) bool

type MilvusCollectionReader struct {
	DefaultReader

	etcdConfig  config.MilvusEtcdConfig
	mqConfig    config.MilvusMQConfig
	collections []CollectionInfo
	monitor     Monitor
	dataChanLen int

	etcdCli           util.KVApi
	factoryCreator    FactoryCreator
	shouldReadFunc    ShouldReadFunc
	dataChan          chan *model.CDCData
	cancelWatch       context.CancelFunc
	collectionID2Name util.Map[int64, string]
	closeStreamFuncs  util.SafeArray[func()]

	startOnce sync.Once
	quitOnce  sync.Once
	isQuit    util.Value[bool]

	// Please no read or write it excluding in the beginning of readStreamData method
	readingSteamCollection []int64
	readingLock            sync.Mutex

	dbID int64
}

func NewMilvusCollectionReader(options ...config.Option[*MilvusCollectionReader]) (*MilvusCollectionReader, error) {
	reader := &MilvusCollectionReader{
		monitor:        NewDefaultMonitor(),
		factoryCreator: NewDefaultFactoryCreator(),
		dataChanLen:    10,
		dbID:           1,
	}
	reader.shouldReadFunc = reader.getDefaultShouldReadFunc()
	for _, option := range options {
		option.Apply(reader)
	}
	var err error
	reader.etcdCli, err = util.GetEtcdClient(reader.etcdConfig.Endpoints)
	if err != nil {
		log.Warn("fail to get etcd client", zap.Error(err))
		return nil, err
	}
	reader.dataChan = make(chan *model.CDCData, reader.dataChanLen)
	reader.isQuit.Store(false)
	return reader, nil
}

func (reader *MilvusCollectionReader) StartRead(ctx context.Context) <-chan *model.CDCData {
	reader.startOnce.Do(func() {
		watchCtx, cancel := context.WithCancel(context.Background())
		reader.cancelWatch = cancel

		log.Info("start read collection")

		go reader.watchCollection(watchCtx)
		go reader.watchPartition(watchCtx)
		go reader.getAllCollections()
	})

	return reader.dataChan
}

func (reader *MilvusCollectionReader) getDefaultShouldReadFunc() ShouldReadFunc {
	return func(i *pb.CollectionInfo) bool {
		return lo.ContainsBy(reader.collections, func(info CollectionInfo) bool {
			return reader.collectionName(i) == info.collectionName
		})
	}
}

func (reader *MilvusCollectionReader) watchCollection(watchCtx context.Context) {
	// watch collection prefix to avoid new collection while getting the all collection
	// TODO improvement watch single instance
	watchChan := reader.etcdCli.Watch(watchCtx, reader.collectionPrefix()+"/", clientv3.WithPrefix())
	for {
		select {
		case watchResp, ok := <-watchChan:
			if !ok {
				reader.monitor.WatchChanClosed()
				return
			}
			lo.ForEach(watchResp.Events, func(event *clientv3.Event, _ int) {
				if event.Type != clientv3.EventTypePut {
					return
				}
				collectionKey := util.ToString(event.Kv.Key)
				log.Info("collection key", zap.String("key", collectionKey))
				if !strings.HasPrefix(collectionKey, reader.collectionPrefix()) {
					return
				}
				info := &pb.CollectionInfo{}
				err := proto.Unmarshal(event.Kv.Value, info)
				if err != nil {
					log.Warn("fail to unmarshal the collection info", zap.String("key", collectionKey), zap.String("value", util.Base64Encode(event.Kv.Value)), zap.Error(err))
					reader.monitor.OnFailUnKnowCollection(collectionKey, err)
					return
				}
				if info.State == pb.CollectionState_CollectionCreated {
					go func() {
						log.Info("collection key created", zap.String("key", collectionKey))
						if reader.shouldReadFunc(info) {
							log.Info("collection key should created", zap.String("key", collectionKey))
							reader.collectionID2Name.Store(info.ID, reader.collectionName(info))
							err := util.Do(context.Background(), func() error {
								err := reader.fillCollectionField(info)
								if err != nil {
									log.Info("fail to get collection fields, retry...", zap.String("key", collectionKey), zap.Error(err))
								}
								return err
							})
							if err != nil {
								log.Warn("fail to get collection fields", zap.String("key", collectionKey), zap.Error(err))
								reader.monitor.OnFailGetCollectionInfo(info.ID, reader.collectionName(info), err)
								return
							}
							reader.readStreamData(info, true)
						}
					}()
				}
			})
		case <-watchCtx.Done():
			log.Info("watch collection context done")
			return
		}
	}
}

func (reader *MilvusCollectionReader) watchPartition(watchCtx context.Context) {
	watchChan := reader.etcdCli.Watch(watchCtx, reader.partitionPrefix()+"/", clientv3.WithPrefix())
	for {
		select {
		case watchResp, ok := <-watchChan:
			if !ok {
				return
			}
			lo.ForEach(watchResp.Events, func(event *clientv3.Event, _ int) {
				if event.Type != clientv3.EventTypePut {
					return
				}
				partitionKey := util.ToString(event.Kv.Key)
				if !strings.HasPrefix(partitionKey, reader.partitionPrefix()) {
					return
				}
				id := reader.getCollectionIDFromPartitionKey(partitionKey)
				if id == 0 {
					log.Warn("fail to get the collection id", zap.String("key", partitionKey))
					return
				}
				info := &pb.PartitionInfo{}
				err := proto.Unmarshal(event.Kv.Value, info)
				if err != nil {
					log.Warn("fail to unmarshal the partition info", zap.String("key", partitionKey), zap.String("value", util.Base64Encode(event.Kv.Value)), zap.Error(err))
					// TODO monitor
					//reader.monitor.OnFailUnKnowCollection(collectionKey, err)
					return
				}
				if info.State == pb.PartitionState_PartitionCreated &&
					info.PartitionName != reader.etcdConfig.DefaultPartitionName {
					collectionName, ok := reader.collectionID2Name.Load(id)
					if !ok {
						collectionName = reader.getCollectionNameByID(id)
						if collectionName == "" {
							log.Warn("not found the collection", zap.Int64("collection_id", id),
								zap.Int64("partition_id", info.PartitionID),
								zap.String("partition_name", info.PartitionName))
							return
						}
					}
					data := &model.CDCData{
						Msg: &msgstream.CreatePartitionMsg{
							BaseMsg: msgstream.BaseMsg{},
							CreatePartitionRequest: msgpb.CreatePartitionRequest{
								Base: &commonpb.MsgBase{
									MsgType: commonpb.MsgType_CreatePartition,
								},
								CollectionName: collectionName,
								PartitionName:  info.PartitionName,
								CollectionID:   info.CollectionId,
								PartitionID:    info.PartitionID,
							},
						},
					}
					reader.sendData(data)
				}
			})
		case <-watchCtx.Done():
			log.Info("watch partition context done")
			return
		}
	}
}

func (reader *MilvusCollectionReader) getCollectionNameByID(collectionID int64) string {
	var (
		resp *clientv3.GetResponse
		err  error
	)

	if reader.dbID == 0 {
		resp, err = util.EtcdGet(reader.etcdCli, path.Join(reader.collectionPrefix(), strconv.FormatInt(collectionID, 10)))
	} else {
		resp, err = util.EtcdGet(reader.etcdCli, path.Join(reader.collectionPrefix(), strconv.FormatInt(reader.dbID, 10), strconv.FormatInt(collectionID, 10)))
	}
	if err != nil {
		log.Warn("fail to get all collection data", zap.Int64("collection_id", collectionID), zap.Error(err))
		return ""
	}
	if len(resp.Kvs) == 0 {
		log.Warn("the collection isn't existed", zap.Int64("collection_id", collectionID))
		return ""
	}
	info := &pb.CollectionInfo{}
	err = proto.Unmarshal(resp.Kvs[0].Value, info)
	if err != nil {
		log.Warn("fail to unmarshal collection info, maybe it's a deleted collection",
			zap.Int64("collection_id", collectionID),
			zap.String("value", util.Base64Encode(resp.Kvs[0].Value)),
			zap.Error(err))
		return ""
	}
	collectionName := reader.collectionName(info)
	if reader.shouldReadFunc(info) {
		reader.collectionID2Name.Store(collectionID, collectionName)
		return collectionName
	}
	log.Warn("the collection can't be read", zap.Int64("id", collectionID), zap.String("name", collectionName))
	return ""
}

func (reader *MilvusCollectionReader) getAllCollections() {
	var (
		existedCollectionInfos []*pb.CollectionInfo
		err                    error
	)

	existedCollectionInfos, err = reader.getCollectionInfo()
	if err != nil {
		log.Warn("fail to get collection", zap.Error(err))
		reader.monitor.OnFailUnKnowCollection(reader.collectionPrefix(), err)
	}
	for _, info := range existedCollectionInfos {
		if info.State == pb.CollectionState_CollectionCreated {
			go reader.readStreamData(info, false)
		}
	}
}

func (reader *MilvusCollectionReader) collectionPrefix() string {
	c := reader.etcdConfig
	collectionKey := c.CollectionKey
	if reader.dbID != 0 {
		collectionKey = c.CollectionWithDBKey
	}
	return util.GetCollectionPrefix(c.RootPath, c.MetaSubPath, collectionKey)
}

func (reader *MilvusCollectionReader) partitionPrefix() string {
	c := reader.etcdConfig
	return util.GetPartitionPrefix(c.RootPath, c.MetaSubPath, c.PartitionKey)
}

func (reader *MilvusCollectionReader) fieldPrefix() string {
	c := reader.etcdConfig
	return util.GetFieldPrefix(c.RootPath, c.MetaSubPath, c.FiledKey)
}

func (reader *MilvusCollectionReader) collectionName(info *pb.CollectionInfo) string {
	return info.Schema.Name
}

func (reader *MilvusCollectionReader) getCollectionIDFromPartitionKey(key string) int64 {
	subStrs := strings.Split(key[len(reader.partitionPrefix())+1:], "/")
	if len(subStrs) != 2 {
		log.Warn("the key is invalid", zap.String("key", key), zap.Strings("sub", subStrs))
		return 0
	}
	id, err := strconv.ParseInt(subStrs[0], 10, 64)
	if err != nil {
		log.Warn("fail to parse the collection id", zap.String("id", subStrs[0]), zap.Error(err))
		return 0
	}
	return id
}

// getCollectionInfo The return value meanings are respectively:
// 1. collection infos that the collection have existed
// 2. error message
func (reader *MilvusCollectionReader) getCollectionInfo() ([]*pb.CollectionInfo, error) {
	resp, err := util.EtcdGet(reader.etcdCli, reader.collectionPrefix()+"/", clientv3.WithPrefix())
	if err != nil {
		log.Warn("fail to get all collection data", zap.Error(err))
		return nil, err
	}
	var existedCollectionInfos []*pb.CollectionInfo

	for _, kv := range resp.Kvs {
		info := &pb.CollectionInfo{}
		err = proto.Unmarshal(kv.Value, info)
		if err != nil {
			log.Warn("fail to unmarshal collection info, maybe it's a deleted collection", zap.String("key", util.ToString(kv.Key)), zap.String("value", util.Base64Encode(kv.Value)), zap.Error(err))
			continue
		}
		if reader.shouldReadFunc(info) {
			reader.collectionID2Name.Store(info.ID, reader.collectionName(info))
			log.Info("get the collection that it need to be replicated", zap.String("name", reader.collectionName(info)), zap.String("key", util.ToString(kv.Key)))
			err = reader.fillCollectionField(info)
			if err != nil {
				return existedCollectionInfos, err
			}
			existedCollectionInfos = append(existedCollectionInfos, info)
		}
	}
	return existedCollectionInfos, nil
}

func (reader *MilvusCollectionReader) fillCollectionField(info *pb.CollectionInfo) error {
	filedPrefix := reader.fieldPrefix()
	prefix := path.Join(filedPrefix, strconv.FormatInt(info.ID, 10)) + "/"
	resp, err := util.EtcdGet(reader.etcdCli, prefix, clientv3.WithPrefix())
	if err != nil {
		log.Warn("fail to get the collection field data", zap.String("prefix", prefix), zap.Error(err))
		return err
	}
	if len(resp.Kvs) == 0 {
		err = errors.New("not found the collection field data")
		log.Warn(err.Error(), zap.String("prefix", filedPrefix))
		return err
	}
	var fields []*schemapb.FieldSchema
	for _, kv := range resp.Kvs {
		field := &schemapb.FieldSchema{}
		err = proto.Unmarshal(kv.Value, field)
		if err != nil {
			log.Warn("fail to unmarshal filed schema info",
				zap.String("key", util.ToString(kv.Key)), zap.String("value", util.Base64Encode(kv.Value)), zap.Error(err))
			return err
		}
		if field.Name == common.MetaFieldName {
			info.Schema.EnableDynamicField = true
			continue
		}
		if field.FieldID >= 100 {
			fields = append(fields, field)
		}
	}
	info.Schema.Fields = fields
	return nil
}

func (reader *MilvusCollectionReader) readStreamData(info *pb.CollectionInfo, sendCreateMsg bool) {
	isRepeatCollection := func(id int64) bool {
		reader.readingLock.Lock()
		defer reader.readingLock.Unlock()

		if lo.Contains(reader.readingSteamCollection, id) {
			return true
		}
		reader.readingSteamCollection = append(reader.readingSteamCollection, id)
		return false
	}
	if isRepeatCollection(info.ID) {
		return
	}
	reader.monitor.OnSuccessGetACollectionInfo(info.ID, reader.collectionName(info))

	if sendCreateMsg {
		baseMsg := msgstream.BaseMsg{
			HashValues: []uint32{0},
		}
		schemaByte, err := json.Marshal(info.Schema)
		if err != nil {
			log.Warn("fail to marshal the collection schema", zap.Error(err))
			reader.monitor.OnFailReadStream(info.ID, reader.collectionName(info), "unknown", err)
			return
		}
		createCollectionMsg := &msgstream.CreateCollectionMsg{
			BaseMsg: baseMsg,
			CreateCollectionRequest: msgpb.CreateCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_CreateCollection,
				},
				CollectionName: reader.collectionName(info),
				CollectionID:   info.ID,
				Schema:         schemaByte,
			},
		}
		reader.sendData(&model.CDCData{
			Msg: createCollectionMsg,
			Extra: map[string]any{
				model.ShardNumKey:             info.ShardsNum,
				model.ConsistencyLevelKey:     info.ConsistencyLevel,
				model.CollectionPropertiesKey: info.Properties,
			},
		})
	}

	vchannels := info.VirtualChannelNames
	barrierManager := NewDataBarrierManager(len(vchannels), reader.sendData)
	log.Info("read vchannels", zap.Strings("channels", vchannels))
	for _, vchannel := range vchannels {
		position, err := reader.collectionPosition(info, vchannel)
		handleError := func() {
			reader.monitor.OnFailReadStream(info.ID, reader.collectionName(info), vchannel, err)
			reader.isQuit.Store(true)
		}
		if err != nil {
			log.Warn("fail to find the collection position", zap.String("vchannel", vchannel), zap.Error(err))
			handleError()
			return
		}
		stream, err := reader.msgStream()
		if err != nil {
			log.Warn("fail to new message stream", zap.String("vchannel", vchannel), zap.Error(err))
			handleError()
			return
		}
		msgChan, err := reader.msgStreamChan(vchannel, position, stream)
		if err != nil {
			stream.Close()
			log.Warn("fail to get message stream chan", zap.String("vchannel", vchannel), zap.Error(err))
			handleError()
			return
		}
		reader.closeStreamFuncs.Append(stream.Close)
		go reader.readMsg(reader.collectionName(info), info.ID, vchannel, msgChan, barrierManager)
	}
}

func (reader *MilvusCollectionReader) collectionPosition(info *pb.CollectionInfo, vchannelName string) (*msgstream.MsgPosition, error) {
	pchannel := util.ToPhysicalChannel(vchannelName)
	for _, collection := range reader.collections {
		if collection.collectionName == reader.collectionName(info) &&
			collection.positions != nil {
			if pair, ok := collection.positions[pchannel]; ok {
				return &msgstream.MsgPosition{
					ChannelName: vchannelName,
					MsgID:       pair.GetData(),
				}, nil
			}
		}
	}
	//return util.GetChannelStartPosition(vchannelName, info.StartPositions)
	return nil, nil
}

func (reader *MilvusCollectionReader) msgStream() (msgstream.MsgStream, error) {
	var factory msgstream.Factory
	if reader.mqConfig.Pulsar.Address != "" {
		factory = reader.factoryCreator.NewPmsFactory(&reader.mqConfig.Pulsar)
	} else if reader.mqConfig.Kafka.Address != "" {
		factory = reader.factoryCreator.NewKmsFactory(&reader.mqConfig.Kafka)
	} else {
		return nil, errors.New("fail to get the msg stream, check the mqConfig param")
	}
	stream, err := factory.NewMsgStream(context.Background())
	if err != nil {
		log.Warn("fail to new the msg stream", zap.Error(err))
	}
	return stream, err
}

func (reader *MilvusCollectionReader) msgStreamChan(vchannel string, position *msgstream.MsgPosition, stream msgstream.MsgStream) (<-chan *msgstream.MsgPack, error) {
	consumeSubName := vchannel + string(rand.Int31())
	pchannelName := util.ToPhysicalChannel(vchannel)
	stream.AsConsumer([]string{pchannelName}, consumeSubName, mqwrapper.SubscriptionPositionLatest)
	if position == nil {
		return stream.Chan(), nil
	}
	position.ChannelName = pchannelName
	err := stream.Seek([]*msgstream.MsgPosition{position})
	if err != nil {
		log.Warn("fail to seek the msg position", zap.String("vchannel", vchannel), zap.Error(err))
		return nil, err
	}

	return stream.Chan(), nil
}

func (reader *MilvusCollectionReader) readMsg(collectionName string, collectionID int64, vchannelName string,
	c <-chan *msgstream.MsgPack,
	barrierManager *DataBarrierManager) {
	for {
		if reader.isQuit.Load() && barrierManager.IsEmpty() {
			return
		}
		msgPack := <-c
		if msgPack == nil {
			return
		}
		for _, msg := range msgPack.Msgs {
			msgType := msg.Type()
			if reader.filterMsgType(msgType) {
				continue
			}
			log.Info("msgType", zap.Any("msg_type", msgType))
			if reader.filterMsg(collectionName, collectionID, msg) {
				continue
			}
			data := &model.CDCData{
				Msg: msg,
			}
			if barrierManager.IsBarrierData(data) {
				if dropPartitionMsg, ok := msg.(*msgstream.DropPartitionMsg); ok {
					dropPartitionMsg.CollectionName = collectionName
				}
				barrierManager.AddData(vchannelName, data)
				if _, ok := msg.(*msgstream.DropCollectionMsg); ok {
					return
				}
				continue
			}
			reader.sendData(&model.CDCData{
				Msg: msg,
				Extra: map[string]any{
					model.CollectionIDKey:   collectionID,
					model.CollectionNameKey: collectionName,
				},
			})
		}
	}
}

func (reader *MilvusCollectionReader) filterMsgType(msgType commonpb.MsgType) bool {
	return msgType == commonpb.MsgType_TimeTick
}

func (reader *MilvusCollectionReader) filterMsg(collectionName string, collectionID int64, msg msgstream.TsMsg) bool {
	if x, ok := msg.(interface{ GetCollectionName() string }); ok {
		notEqual := x.GetCollectionName() != collectionName
		if y, ok := msg.(interface{ GetCollectionID() int64 }); ok {
			notEqual = y.GetCollectionID() != collectionID
		}
		if notEqual {
			log.Warn("filter msg",
				zap.String("current_collection_name", collectionName),
				zap.String("msg_collection_name", x.GetCollectionName()),
				zap.Any("msg_type", msg.Type()))
			reader.monitor.OnFilterReadMsg(msg.Type().String())
		}
		return notEqual
	}
	return true
}

func (reader *MilvusCollectionReader) CancelWatchCollection() {
	if reader.cancelWatch != nil {
		reader.cancelWatch()
	}
}

func (reader *MilvusCollectionReader) QuitRead(ctx context.Context) {
	reader.quitOnce.Do(func() {
		reader.isQuit.Store(true)
		reader.CancelWatchCollection()
		reader.closeStreamFuncs.Range(func(_ int, value func()) bool {
			value()
			return true
		})
	})
}

func (reader *MilvusCollectionReader) sendData(data *model.CDCData) {
	reader.dataChan <- data
}
