package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"sigs.k8s.io/yaml"

	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/pb"
	"github.com/zilliztech/milvus-cdc/core/util"
)

// cdc 在同步前需要确定之前是否已经同步了该channel，处理channel复用出现的case，
// 例如channel上存在两个collection的数据，最初只有同步一个collection，然后又开始同步另外一个，然后一会又取消同步其中一个collection
// 如果一个正在watch的流出现了createCollectionMsg，这之后需要查询下这个collection是否被watch了

var (
	// log       = util.Log
	milvusCli client.Client
	etcdCli   *clientv3.Client

	// source milvus config
	rootPath    = "by-dev"
	metaSubPath = "meta"
	// collectionID          = "444294399798489626"
	// collectionName        = "hello_milvus4"
	collectionPrefix      = fmt.Sprintf("%s/%s/root-coord/database/collection-info/1", rootPath, metaSubPath)
	collectionFieldPrefix = fmt.Sprintf("%s/%s/root-coord/fields", rootPath, metaSubPath)
	// collectionMetaKey     = collectionPrefix + collectionID
	etcdEndpoint = "localhost:2379"
	cfg          = &config.PulsarConfig{
		Address:        "pulsar://localhost:6650",
		WebAddress:     "localhost:80",
		MaxMessageSize: "5242880",
		Tenant:         "public",
		Namespace:      "default",
	}
	factory msgstream.Factory

	// target milvus config
	addr     = "in01-944f15585f248dd.aws-us-west-2.vectordb-uat3.zillizcloud.com:19540"
	username = "root"
	password = "O6-[6t|qR]<E~Y1k~TCFU4sk8d:zcmRK"

	replicateLock sync.RWMutex
	// pchannelName -> source collectionID -> targetCollectionInfo
	replicateChannels = make(map[string]map[int64]*targetCollectionInfo)
	stepWg            = sync.WaitGroup{}
)

type targetCollectionInfo struct {
	collectionID  int64
	partitionInfo map[string]int64
	pchannel      string
	vchannels     []string
	w             *sync.WaitGroup
}

type DemoConfig struct {
	// source config
	RootPath         string
	MetaPath         string
	EtcdEndpoint     string
	PulsarAddress    string
	PulsarWebAddress string
	PulsarTenant     string
	PulsarNamespace  string

	// target config
	Addr     string
	Username string
	Password string
}

func main() {
	fileContent, _ := os.ReadFile("demo.yaml")

	demoConfig := &DemoConfig{}
	err := yaml.Unmarshal(fileContent, demoConfig)
	if err != nil {
		log.Panic("Failed to parse config file", zap.Error(err))
	}
	rootPath = demoConfig.RootPath
	metaSubPath = demoConfig.MetaPath
	etcdEndpoint = demoConfig.EtcdEndpoint
	cfg.Address = demoConfig.PulsarAddress
	cfg.WebAddress = demoConfig.PulsarWebAddress
	cfg.Tenant = demoConfig.PulsarTenant
	cfg.Namespace = demoConfig.PulsarNamespace
	addr = demoConfig.Addr
	username = demoConfig.Username
	password = demoConfig.Password

	factory = NewPmsFactory()
	collectionPrefix = fmt.Sprintf("%s/%s/root-coord/database/collection-info/1", rootPath, metaSubPath)
	collectionFieldPrefix = fmt.Sprintf("%s/%s/root-coord/fields", rootPath, metaSubPath)
	milvusCli = GetMilvusConnection()
	etcdCli = GetEtcdConnection()

	// demo test
	// // target
	// pchannels, vchannels, targetCollectionID := GetCollectionChannel(collectionName)
	// log.Info("pchannels", zap.Strings("channels", pchannels))
	// partitionInfo := GetPartitionInfo(collectionName)
	//
	// // source
	// startPositions := GetCollectionStartPositions(collectionMetaKey)
	// if len(pchannels) != len(startPositions) {
	// 	log.Panic("channel number not equal to start position number")
	// }
	// log.Info("start positions", zap.Any("position", startPositions))
	// wg := sync.WaitGroup{}
	// for i, position := range startPositions {
	// 	wg.Add(1)
	// 	packChan := GetMsgStreamChan(position.GetChannelName(), startPositions, true)
	// 	go func(index int, c <-chan *msgstream.MsgPack) {
	// 		defer wg.Done()
	// 		log.Info("start to receive the msg")
	// 		if c == nil {
	// 			log.Panic("msg stream chan is nil")
	// 		}
	// 		for {
	// 			pack, ok := <-c
	// 			if !ok {
	// 				log.Info("packChan closed")
	// 				return
	// 			}
	// 			SendMsgPack(pchannels[index], vchannels, targetCollectionID, partitionInfo, pack)
	// 		}
	// 	}(i, packChan)
	// }
	// wg.Wait()

	stepWg.Add(2)
	go WatchCollectionForSource()
	go GetAllCollectionInfoAndReplicate()
	stepWg.Wait()
}

func WatchCollectionForSource() {
	defer stepWg.Done()
	watchChan := etcdCli.Watch(context.Background(), collectionPrefix+"/", clientv3.WithPrefix())
	for {
		watchResp, ok := <-watchChan
		if !ok {
			log.Info("close the watch channel")
			return
		}
		for _, event := range watchResp.Events {
			if event.Type != clientv3.EventTypePut {
				return
			}
			collectionKey := util.ToString(event.Kv.Key)
			log.Info("watch put collection event", zap.String("key", collectionKey))
			if !strings.HasPrefix(collectionKey, collectionPrefix) {
				return
			}
			info := &pb.CollectionInfo{}
			err := proto.Unmarshal(event.Kv.Value, info)
			if err != nil {
				log.Warn("fail to unmarshal the collection info", zap.String("key", collectionKey), zap.Error(err))
				continue
			}
			if info.State != pb.CollectionState_CollectionCreated {
				continue
			}
			err = fillCollectionField(info)
			for err != nil {
				log.Warn("fail to fill the collection field", zap.String("key", collectionKey), zap.Error(err))
				time.Sleep(500 * time.Millisecond)
				err = fillCollectionField(info)
			}
			ReplicateChannel(info, false)
		}
	}
}

func fillCollectionField(info *pb.CollectionInfo) error {
	prefix := path.Join(collectionFieldPrefix, strconv.FormatInt(info.ID, 10)) + "/"
	resp, err := util.EtcdGet(etcdCli, prefix, clientv3.WithPrefix())
	if err != nil {
		log.Warn("fail to get the collection field data", zap.String("prefix", prefix), zap.Error(err))
		return err
	}
	if len(resp.Kvs) == 0 {
		err = errors.New("not found the collection field data")
		log.Warn(err.Error(), zap.String("prefix", prefix))
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

func GetAllCollectionInfoAndReplicate() {
	defer stepWg.Done()
	resp, err := util.EtcdGet(etcdCli, collectionPrefix+"/", clientv3.WithPrefix())
	if err != nil {
		log.Warn("fail to get all collection data", zap.Error(err))
		return
	}
	var existedCollectionInfos []*pb.CollectionInfo

	for _, kv := range resp.Kvs {
		info := &pb.CollectionInfo{}
		err = proto.Unmarshal(kv.Value, info)
		if err != nil {
			log.Warn("fail to unmarshal collection info, maybe it's a deleted collection", zap.String("key", util.ToString(kv.Key)), zap.String("value", util.Base64Encode(kv.Value)), zap.Error(err))
			continue
		}
		if info.State != pb.CollectionState_CollectionCreated {
			continue
		}
		err = fillCollectionField(info)
		if err != nil {
			log.Warn("fail to fill the collection field", zap.String("key", util.ToString(kv.Key)), zap.Error(err))
			return
		}
		existedCollectionInfos = append(existedCollectionInfos, info)
	}
	for _, info := range existedCollectionInfos {
		ReplicateChannel(info, true)
	}
}

func ReplicateChannel(info *pb.CollectionInfo, isLatestMsg bool) {
	log.Info("start replicate collection", zap.Int64("id", info.ID), zap.String("name", info.Schema.Name))
	startPositions := make([]*msgpb.MsgPosition, 0)
	for _, v := range info.StartPositions {
		startPositions = append(startPositions, &msgstream.MsgPosition{
			ChannelName: v.GetKey(),
			MsgID:       v.GetData(),
		})
	}

	err := CreateCollectionForTarget(info)
	if err != nil {
		log.Warn("fail to create collection for target", zap.Int64("collection", info.ID), zap.Error(err))
		return
	}
	// target
	targetCollectionName := info.Schema.Name
	pchannels, vchannels, targetCollectionID := GetCollectionChannel(targetCollectionName)
	log.Info("pchannels", zap.Strings("channels", pchannels))
	partitionInfo := GetPartitionInfo(targetCollectionName)
	if len(pchannels) != len(startPositions) {
		log.Panic("channel number not equal to start position number")
	}

	replicateLock.Lock()
	w := &sync.WaitGroup{}
	w.Add(len(startPositions))
	for i, position := range startPositions {
		sourceCollectionID := info.ID
		targetInfo := &targetCollectionInfo{
			collectionID:  targetCollectionID,
			partitionInfo: partitionInfo,
			pchannel:      pchannels[i],
			vchannels:     vchannels,
			w:             w,
		}
		pchannel := position.GetChannelName()
		replicateChannelInfo, ok := replicateChannels[pchannel]
		if !ok {
			replicateChannels[pchannel] = map[int64]*targetCollectionInfo{sourceCollectionID: targetInfo}
			packChan := GetMsgStreamChan(position.GetChannelName(), startPositions, isLatestMsg)
			go func(channelName string, c <-chan *msgstream.MsgPack) {
				log.Info("start to receive the msg", zap.String("channel", channelName))
				if c == nil {
					log.Panic("msg stream chan is nil", zap.String("channel", channelName))
				}
				for {
					pack, ok := <-c
					if !ok {
						log.Info("packChan closed", zap.String("channel", channelName))
						return
					}

					SendMsgPack2(channelName, targetInfo.pchannel, pack)
				}
			}(pchannel, packChan)
			continue
		}
		replicateChannelInfo[sourceCollectionID] = targetInfo
	}
	replicateLock.Unlock()
	go func(dropCollectionName string, dropWait *sync.WaitGroup) {
		dropWait.Wait()
		_ = DropCollectionForTarget(dropCollectionName)
	}(targetCollectionName, w)
}

func CreateCollectionForTarget(info *pb.CollectionInfo) error {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelFunc()
	if _, err := milvusCli.DescribeCollection(ctx, info.Schema.Name); err == nil {
		log.Info("collection already exists", zap.String("name", info.Schema.Name))
		return nil
	}

	entitySchema := &entity.Schema{}
	entitySchema = entitySchema.ReadProto(info.Schema)
	var createCollectionOptions []client.CreateCollectionOption
	createCollectionOptions = append(createCollectionOptions, client.WithConsistencyLevel(entity.ConsistencyLevel(info.ConsistencyLevel)))
	for _, property := range info.Properties {
		createCollectionOptions = append(createCollectionOptions, client.WithCollectionProperty(property.GetKey(), property.GetValue()))
	}
	err := milvusCli.CreateCollection(ctx, entitySchema, info.ShardsNum, createCollectionOptions...)
	if err != nil {
		log.Warn("failed to create collection", zap.Any("info", info), zap.Error(err))
		return err
	}
	return nil
}

func DropCollectionForTarget(collectionName string) error {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelFunc()

	err := milvusCli.DropCollection(ctx, collectionName)
	if err != nil {
		log.Warn("failed to drop collection", zap.String("name", collectionName), zap.Error(err))
		return err
	}
	return nil
}

func GetMilvusConnection() client.Client {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFunc()
	cli, err := client.NewDefaultGrpcClientWithTLSAuth(ctx, addr, username, password)
	// cli, err := client.NewDefaultGrpcClientWithAuth(ctx, Addr, Username, password)
	if err != nil {
		log.Panic("failed to create milvus client", zap.Error(err))
	}
	return cli
}

func GetEtcdConnection() *clientv3.Client {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdEndpoint},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Panic("failed to create etcd client", zap.Error(err))
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()
	if status, err := cli.Status(ctx, etcdEndpoint); err != nil {
		log.Panic("failed to get etcd status", zap.Error(err))
	} else {
		log.Info("etcd status", zap.Any("status", status))
	}
	return cli
}

func GetCollectionChannel(collectionName string) ([]string, []string, int64) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFunc()
	collection, err := milvusCli.DescribeCollection(ctx, collectionName)
	if err != nil {
		log.Panic("failed to describe collection", zap.Error(err))
	}
	return collection.PhysicalChannels, collection.VirtualChannels, collection.ID
}

func GetPartitionInfo(collectionName string) map[string]int64 {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFunc()
	partition, err := milvusCli.ShowPartitions(ctx, collectionName)
	if err != nil || len(partition) == 0 {
		log.Panic("failed to show partitions", zap.Error(err))
	}
	partitionInfo := make(map[string]int64, len(partition))
	for _, e := range partition {
		partitionInfo[e.Name] = e.ID
	}
	return partitionInfo
}

func GetCollectionStartPositions(collectionMetaKey string) []*msgpb.MsgPosition {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFunc()
	getResp, err := etcdCli.Get(ctx, collectionMetaKey)
	if err != nil {
		log.Panic("failed to get collection meta", zap.Error(err))
	}
	if len(getResp.Kvs) == 0 {
		log.Panic("empty collection meta")
	}
	info := &pb.CollectionInfo{}
	err = proto.Unmarshal(getResp.Kvs[0].Value, info)
	if err != nil {
		log.Panic("failed to unmarshal collection meta", zap.Error(err))
	}
	positions := make([]*msgpb.MsgPosition, 0)
	for _, v := range info.StartPositions {
		positions = append(positions, &msgstream.MsgPosition{
			ChannelName: v.GetKey(),
			MsgID:       v.GetData(),
		})
	}
	return positions
}

func NewPmsFactory() msgstream.Factory {
	paramtable.Init()

	return msgstream.NewPmsFactory(
		&paramtable.ServiceParam{
			PulsarCfg: paramtable.PulsarConfig{
				Address:             config.NewParamItem(cfg.Address),
				WebAddress:          config.NewParamItem(cfg.WebAddress),
				WebPort:             config.NewParamItem(strconv.Itoa(cfg.WebPort)),
				MaxMessageSize:      config.NewParamItem(cfg.MaxMessageSize),
				AuthPlugin:          config.NewParamItem(""),
				AuthParams:          config.NewParamItem("{}"),
				Tenant:              config.NewParamItem(cfg.Tenant),
				Namespace:           config.NewParamItem(cfg.Namespace),
				RequestTimeout:      config.NewParamItem("60"),
				EnableClientMetrics: config.NewParamItem("false"),
			},
			MQCfg: paramtable.MQConfig{
				ReceiveBufSize: config.NewParamItem("16"),
				MQBufSize:      config.NewParamItem("16"),
			},
		},
	)
}

func GetMsgStreamChan(pChannel string, position []*msgpb.MsgPosition, isLatestMsg bool) <-chan *msgstream.MsgPack {
	ttMsgStream, err := factory.NewTtMsgStream(context.Background())
	if err != nil {
		log.Panic("failed to create tt msg stream", zap.Error(err))
	}
	consumeSubName := "cdc-" + pChannel + string(rand.Int31())
	ttMsgStream.AsConsumer(context.Background(), []string{pChannel}, consumeSubName, mqwrapper.SubscriptionPositionLatest)
	if isLatestMsg {
		return ttMsgStream.Chan()
	}
	for _, msgPosition := range position {
		if msgPosition.GetChannelName() == pChannel {
			err = ttMsgStream.Seek(context.Background(), []*msgstream.MsgPosition{msgPosition})
			if err != nil {
				log.Panic("failed to seek msg position", zap.Error(err))
			}
			return ttMsgStream.Chan()
		}
	}
	log.Panic("failed to find msg position")
	return nil
}

func GetLatestPosition(pChannel string) {
	ttMsgStream, err := factory.NewTtMsgStream(context.Background())
	if err != nil {
		log.Panic("failed to create tt msg stream", zap.Error(err))
	}
	consumeSubName := "get-latest-position-" + pChannel + string(rand.Int31())
	ttMsgStream.AsConsumer(context.Background(), []string{pChannel}, consumeSubName, mqwrapper.SubscriptionPositionEarliest)
	msgID, err := ttMsgStream.GetLatestMsgID(pChannel)
	if err != nil {
		log.Panic("failed to get latest msg id", zap.Error(err))
	}
	log.Info("latest msg id", zap.String("channel", pChannel), zap.Any("msgID", msgID.Serialize()))
}

func getVChannel(pChannel string, vchannnels []string) string {
	for _, vchannnel := range vchannnels {
		if strings.Contains(vchannnel, pChannel) {
			return vchannnel
		}
	}
	log.Panic("failed to find vchannel", zap.String("pchannel", pChannel), zap.Strings("vchannels", vchannnels))
	return ""
}

func SendMsgPack(pChannel string, vchannnels []string, collectionID int64, partitionInfo map[string]int64, pack *msgstream.MsgPack) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelFunc()
	msgBytesArr := make([][]byte, 0)
	needTsMsg := false
	for _, msg := range pack.Msgs {
		if msg.Type() == commonpb.MsgType_CreateCollection ||
			msg.Type() == commonpb.MsgType_CreatePartition {
			continue
		}
		originPosition := msg.Position()
		msg.SetPosition(&msgpb.MsgPosition{
			ChannelName: pChannel,
			MsgID:       originPosition.GetMsgID(),
			MsgGroup:    originPosition.GetMsgGroup(),
			Timestamp:   originPosition.GetTimestamp(),
		})
		switch realMsg := msg.(type) {
		case *msgstream.InsertMsg:
			realMsg.CollectionID = collectionID
			realMsg.PartitionID = partitionInfo[realMsg.PartitionName]
			realMsg.ShardName = getVChannel(pChannel, vchannnels)
		case *msgstream.DeleteMsg:
			realMsg.CollectionID = collectionID
			if realMsg.PartitionName != "" {
				realMsg.PartitionID = partitionInfo[realMsg.PartitionName]
			}
			realMsg.ShardName = getVChannel(pChannel, vchannnels)
		case *msgstream.DropCollectionMsg:
			realMsg.CollectionID = collectionID
			needTsMsg = true
		case *msgstream.DropPartitionMsg:
			realMsg.CollectionID = collectionID
			realMsg.PartitionID = partitionInfo[realMsg.PartitionName]
		}
		msgBytes, err := msg.Marshal(msg)
		if err != nil {
			log.Panic("failed to marshal msg", zap.Error(err))
		}
		if _, ok := msgBytes.([]byte); !ok {
			log.Panic("failed to convert msg bytes to []byte")
		}
		msgBytesArr = append(msgBytesArr, msgBytes.([]byte))
	}
	if len(msgBytesArr) != 0 {
		log.Info("pack msg", zap.Any("pack", pack))
	}
	needTsMsg = needTsMsg || len(msgBytesArr) == 0
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
		msgBytes, _ := timeTickMsg.Marshal(timeTickMsg)
		msgBytesArr = append(msgBytesArr, msgBytes.([]byte))
	}
	for _, position := range pack.StartPositions {
		position.ChannelName = pChannel
	}
	for _, position := range pack.EndPositions {
		position.ChannelName = pChannel
	}
	_, err := milvusCli.ReplicateMessage(ctx, pChannel,
		pack.BeginTs, pack.EndTs,
		msgBytesArr,
		pack.StartPositions, pack.EndPositions,
	)
	if err != nil {
		log.Panic("failed to replicate message", zap.Error(err))
	}
}

func getCollectionTargetInfo(pChannel string, collectionID int64) *targetCollectionInfo {
	replicateLock.RLock()
	defer replicateLock.RUnlock()
	targetInfo, ok := replicateChannels[pChannel][collectionID]
	if !ok {
		return nil
	}
	return targetInfo
}

func SendMsgPack2(sourceChannel, pChannel string, pack *msgstream.MsgPack) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelFunc()
	msgBytesArr := make([][]byte, 0)
	needTsMsg := false
	for _, msg := range pack.Msgs {
		if msg.Type() == commonpb.MsgType_CreateCollection ||
			msg.Type() == commonpb.MsgType_CreatePartition {
			continue
		}
		y, ok := msg.(interface{ GetCollectionID() int64 })
		if !ok {
			log.Warn("failed to get collection id", zap.String("channel", sourceChannel), zap.Any("msg", msg))
		} else {
			targetInfo := getCollectionTargetInfo(sourceChannel, y.GetCollectionID())
			for targetInfo == nil {
				log.Warn("failed to get target info", zap.String("channel", sourceChannel), zap.Any("msg", msg))
				time.Sleep(500 * time.Millisecond)
				targetInfo = getCollectionTargetInfo(sourceChannel, y.GetCollectionID())
			}
			switch realMsg := msg.(type) {
			case *msgstream.InsertMsg:
				realMsg.CollectionID = targetInfo.collectionID
				realMsg.PartitionID = targetInfo.partitionInfo[realMsg.PartitionName]
				realMsg.ShardName = getVChannel(pChannel, targetInfo.vchannels)
			case *msgstream.DeleteMsg:
				realMsg.CollectionID = targetInfo.collectionID
				if realMsg.PartitionName != "" {
					realMsg.PartitionID = targetInfo.partitionInfo[realMsg.PartitionName]
				}
				realMsg.ShardName = getVChannel(pChannel, targetInfo.vchannels)
			case *msgstream.DropCollectionMsg:
				realMsg.CollectionID = targetInfo.collectionID
				needTsMsg = true
				targetInfo.w.Done()
			case *msgstream.DropPartitionMsg:
				realMsg.CollectionID = targetInfo.collectionID
				realMsg.PartitionID = targetInfo.partitionInfo[realMsg.PartitionName]
			}
		}
		originPosition := msg.Position()
		msg.SetPosition(&msgpb.MsgPosition{
			ChannelName: pChannel,
			MsgID:       originPosition.GetMsgID(),
			MsgGroup:    originPosition.GetMsgGroup(),
			Timestamp:   originPosition.GetTimestamp(),
		})

		msgBytes, err := msg.Marshal(msg)
		if err != nil {
			log.Panic("failed to marshal msg", zap.Error(err))
		}
		if _, ok := msgBytes.([]byte); !ok {
			log.Panic("failed to convert msg bytes to []byte")
		}
		msgBytesArr = append(msgBytesArr, msgBytes.([]byte))
	}
	if len(msgBytesArr) != 0 {
		log.Info("pack msg", zap.String("channel", sourceChannel), zap.Any("pack", pack))
	}
	needTsMsg = needTsMsg || len(msgBytesArr) == 0
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
		msgBytes, _ := timeTickMsg.Marshal(timeTickMsg)
		msgBytesArr = append(msgBytesArr, msgBytes.([]byte))
	}
	for _, position := range pack.StartPositions {
		position.ChannelName = pChannel
	}
	for _, position := range pack.EndPositions {
		position.ChannelName = pChannel
	}
	_, err := milvusCli.ReplicateMessage(ctx, pChannel,
		pack.BeginTs, pack.EndTs,
		msgBytesArr,
		pack.StartPositions, pack.EndPositions,
	)
	if err != nil {
		log.Panic("failed to replicate message", zap.Error(err))
	}
}
