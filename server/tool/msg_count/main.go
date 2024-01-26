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

package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/samber/lo"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"sigs.k8s.io/yaml"

	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/reader"
	"github.com/zilliztech/milvus-cdc/core/util"
	"github.com/zilliztech/milvus-cdc/server/model"
	"github.com/zilliztech/milvus-cdc/server/model/meta"
)

var GlobalConfig PositionConfig

type PositionConfig struct {
	EtcdAddress        []string
	TaskPositionPrefix string
	TaskPositionKey    string
	PkFieldName        string
	Timeout            int
	CountMode          bool
	TaskPositionMode   bool
	MessageDetail      int
	Pulsar             config.PulsarConfig
	Kafka              config.KafkaConfig
	TaskPositions      []model.ChannelInfo
	DecodePositionType int

	CollectionID int64
	Data         string
}

func main() {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().ServiceParam.MQCfg.EnablePursuitMode.Key, "false")
	log.ReplaceGlobals(zap.NewNop(), &log.ZapProperties{
		Core:   zapcore.NewNopCore(),
		Syncer: zapcore.AddSync(ioutil.Discard),
		Level:  zap.NewAtomicLevel(),
	})

	fileContent, err := os.ReadFile("./configs/msg_count.yaml")
	if err != nil {
		panic(err)
	}
	var positionConfig PositionConfig
	err = yaml.Unmarshal(fileContent, &positionConfig)
	if err != nil {
		panic(err)
	}
	GlobalConfig = positionConfig

	if positionConfig.TaskPositionMode {
		markPrintln("task position mode")
		timeoutCtx, cancelFunc := context.WithTimeout(context.Background(), time.Duration(positionConfig.Timeout)*time.Second)
		defer cancelFunc()

		for _, position := range positionConfig.TaskPositions {
			kd, err := decodePosition(position.Name, position.Position)
			if err != nil {
				panic(err)
			}

			GetMQMessageDetail(timeoutCtx, positionConfig, position.Name, kd)
		}
		return
	}

	client, err := clientv3.New(clientv3.Config{Endpoints: positionConfig.EtcdAddress})
	if err != nil {
		panic(err)
	}

	timeoutCtx, cancelFunc := context.WithTimeout(context.Background(), time.Duration(positionConfig.Timeout)*time.Second)
	defer cancelFunc()
	var getResp *clientv3.GetResponse
	if positionConfig.TaskPositionKey != "" {
		getResp, err = client.Get(timeoutCtx, fmt.Sprintf("%s/%s", positionConfig.TaskPositionPrefix, positionConfig.TaskPositionKey))
	} else {
		getResp, err = client.Get(timeoutCtx, positionConfig.TaskPositionPrefix, clientv3.WithPrefix())
	}
	if err != nil {
		panic(err)
	}
	if len(getResp.Kvs) == 0 {
		panic("task position not exist")
	}
	for _, kv := range getResp.Kvs {
		GetCollectionPositionDetail(timeoutCtx, positionConfig, kv.Value)
		markPrintln("++++++++++++++++++++++++++")
	}
}

func decodePosition(pchannel, position string) (*commonpb.KeyDataPair, error) {
	positionBytes, err := base64.StdEncoding.DecodeString(position)
	if err != nil {
		return nil, err
	}
	if GlobalConfig.DecodePositionType == 1 {
		return &commonpb.KeyDataPair{
			Key:  pchannel,
			Data: positionBytes,
		}, nil
	}
	msgPosition := &msgpb.MsgPosition{}
	err = proto.Unmarshal(positionBytes, msgPosition)
	if err != nil {
		return nil, err
	}
	return &commonpb.KeyDataPair{
		Key:  pchannel,
		Data: msgPosition.MsgID,
	}, nil
}

func GetCollectionPositionDetail(ctx context.Context, config PositionConfig, v []byte) {
	taskPosition := &meta.TaskCollectionPosition{}
	err := json.Unmarshal(v, taskPosition)
	if err != nil {
		panic(err)
	}
	markPrintln("task id:", taskPosition.TaskID)
	markPrintln("collection id:", taskPosition.CollectionID)
	markPrintln("collection name:", taskPosition.CollectionName)
	markPrintln("====================")
	for s, pair := range taskPosition.Positions {
		GetMQMessageDetail(ctx, config, s, pair.DataPair)
	}
}

func GetMQMessageDetail(ctx context.Context, config PositionConfig, pchannel string, kd *commonpb.KeyDataPair) {
	//if config.IncludeCurrent {
	//	markPrintln("include current position")
	//	GetCurrentMsgInfo(ctx, config, pchannel, &msgstream.MsgPosition{
	//		ChannelName: pchannel,
	//		MsgID:       kd.GetData(),
	//	})
	//}

	msgStream := MsgStream(config, false)
	//msgStream := MsgStream(config, true)
	defer msgStream.Close()

	consumeSubName := pchannel + strconv.Itoa(rand.Int())
	initialPosition := mqwrapper.SubscriptionPositionUnknown
	// initialPosition := mqwrapper.SubscriptionPositionEarliest
	err := msgStream.AsConsumer(ctx, []string{pchannel}, consumeSubName, initialPosition)
	if err != nil {
		msgStream.Close()
		panic(err)
	}

	// not including the current msg in this position
	err = msgStream.Seek(ctx, []*msgstream.MsgPosition{
		{
			ChannelName: pchannel,
			MsgID:       kd.GetData(),
		},
	}, false)
	if err != nil {
		msgStream.Close()
		panic(err)
	}

	select {
	case <-ctx.Done():
		markPrintln(ctx.Err())
	case msgpack := <-msgStream.Chan():
		endTs := msgpack.EndTs
		end := msgpack.EndPositions[0]
		msgTime := tsoutil.PhysicalTime(endTs)
		markPrintln("channel name:", pchannel)
		markPrintln("msg time:", msgTime)
		markPrintln("end position:", util.Base64MsgPosition(end))
		currentMsgCount := make(map[string]int)
		MsgCount(msgpack, currentMsgCount, config.MessageDetail, config.PkFieldName)
		markPrintln("msg info, count:", currentMsgCount)
		if config.CountMode {
			msgCount := make(map[string]int)
			MsgCount(msgpack, msgCount, config.MessageDetail, config.PkFieldName)
			MsgCountForStream(ctx, msgStream, config, pchannel, msgCount)
		}

		markPrintln("====================")
	}
}

func MsgCountForStream(ctx context.Context, msgStream msgstream.MsgStream, config PositionConfig, pchannel string, msgCount map[string]int) {
	GetLatestMsgInfo(ctx, config, pchannel)

	latestMsgID, err := msgStream.GetLatestMsgID(pchannel)
	if err != nil {
		msgStream.Close()
		markPrintln("current count:", msgCount)
		panic(err)
	}
	for {
		select {
		case <-ctx.Done():
			markPrintln("count timeout, err: ", ctx.Err())
			markPrintln("current count:", msgCount)
			return
		case msgpack := <-msgStream.Chan():
			end := msgpack.EndPositions[0]
			ok, err := latestMsgID.LessOrEqualThan(end.GetMsgID())
			if err != nil {
				msgStream.Close()
				markPrintln("less or equal err, current count:", msgCount)
				panic(err)
			}
			MsgCount(msgpack, msgCount, config.MessageDetail, config.PkFieldName)
			if ok {
				markPrintln("has count the latest msg, current count:", msgCount)
				return
			}
		}
	}
}

func GetLatestMsgInfo(ctx context.Context, config PositionConfig, pchannel string) {
	msgStream := MsgStream(config, true)
	defer msgStream.Close()

	consumeSubName := pchannel + strconv.Itoa(rand.Int())
	initialPosition := mqwrapper.SubscriptionPositionLatest
	err := msgStream.AsConsumer(ctx, []string{pchannel}, consumeSubName, initialPosition)
	if err != nil {
		msgStream.Close()
		panic(err)
	}

	timeoutCtx, cancelFunc := context.WithTimeout(ctx, 3*time.Second)
	defer cancelFunc()

	select {
	case <-timeoutCtx.Done():
		markPrintln("get latest msg info timeout, err: ", timeoutCtx.Err())
	case msgpack := <-msgStream.Chan():
		endTs := msgpack.EndTs
		end := msgpack.EndPositions[0]
		msgTime := tsoutil.PhysicalTime(endTs)
		markPrintln("latest channel name:", pchannel)
		markPrintln("latest msg time:", msgTime)
		markPrintln("latest end position:", util.Base64MsgPosition(end))
	}
}

func MsgCount(msgpack *msgstream.MsgPack, msgCount map[string]int, detail int, pk string) {
	for _, msg := range msgpack.Msgs {
		msgCount[msg.Type().String()] += 1
		markPrintln("msg type:", msg.Type().String())
		if msg.Type() == commonpb.MsgType_Insert {
			insertMsg := msg.(*msgstream.InsertMsg)
			if GlobalConfig.CollectionID != 0 {
				if insertMsg.CollectionID != GlobalConfig.CollectionID {
					continue
				}
			}
			if detail > 0 {
				pkString := ""
				for _, data := range insertMsg.GetFieldsData() {
					if data.GetFieldName() == pk {
						if detail == 3 {
							var dataStrs []string
							if data.GetScalars().GetLongData() != nil {
								dataStrs = lo.Map(data.GetScalars().GetLongData().GetData(), func(t int64, i int) string {
									return strconv.FormatInt(t, 10)
								})
							} else if data.GetScalars().GetStringData() != nil {
								dataStrs = data.GetScalars().GetStringData().GetData()
							}
							for i, str := range dataStrs {
								if str == GlobalConfig.Data {
									pkString = fmt.Sprintf("insert pk: %s, timestamp: %d, endTS: %s", str, insertMsg.Timestamps[i], tsoutil.PhysicalTime(msgpack.EndTs))
									break
								}
							}
							break
						}

						if data.GetScalars().GetLongData() != nil {
							pkString = fmt.Sprintf("[\"insert pks\"] [pks=\"[%s]\"]", GetArrayString(data.GetScalars().GetLongData().GetData()))
						} else if data.GetScalars().GetStringData() != nil {
							pkString = fmt.Sprintf("[\"insert pks\"] [pks=\"[%s]\"]", strings.Join(data.GetScalars().GetStringData().GetData(), ","))
						} else {
							pkString = "[\"insert pks\"] [pks=\"[]\"], not found"
						}
						break
					}
				}
				if detail != 3 {
					var times []time.Time
					for _, timestamp := range insertMsg.Timestamps {
						times = append(times, tsoutil.PhysicalTime(timestamp))
					}
					markPrintln(pkString, ", timestamps:", times)
				} else if pkString != "" {
					markPrintln(pkString)
				}
			}
			markPrintln(fmt.Sprintf("channel_name=%s ,insert_data_len=%d", msgpack.EndPositions[0].GetChannelName(), insertMsg.GetNumRows()))
			msgCount["insert_count"] += int(insertMsg.GetNumRows())
		} else if msg.Type() == commonpb.MsgType_Delete {
			deleteMsg := msg.(*msgstream.DeleteMsg)
			if detail > 0 {
				if detail == 3 {
					var dataStrs []string
					if deleteMsg.GetPrimaryKeys().GetIntId() != nil {
						dataStrs = lo.Map(deleteMsg.GetPrimaryKeys().GetIntId().GetData(), func(t int64, i int) string {
							return strconv.FormatInt(t, 10)
						})
					} else if deleteMsg.GetPrimaryKeys().GetStrId() != nil {
						dataStrs = deleteMsg.GetPrimaryKeys().GetStrId().GetData()
					}
					for i, str := range dataStrs {
						if str == GlobalConfig.Data {
							markPrintln(fmt.Sprintf("delete pk: %s, timestamp: %d, endTS: %s", str, deleteMsg.Timestamps[i], tsoutil.PhysicalTime(msgpack.EndTs)))
							break
						}
					}
				} else {
					var times []time.Time
					for _, timestamp := range deleteMsg.Timestamps {
						times = append(times, tsoutil.PhysicalTime(timestamp))
					}
					if deleteMsg.GetPrimaryKeys().GetIntId() != nil {
						markPrintln(fmt.Sprintf("[\"delete pks\"] [pks=\"[%s]\"]", GetArrayString(deleteMsg.GetPrimaryKeys().GetIntId().GetData())), ", timestamps:", times)
					} else if deleteMsg.GetPrimaryKeys().GetStrId() != nil {
						markPrintln(fmt.Sprintf("[\"delete pks\"] [pks=\"[%s]\"]", strings.Join(deleteMsg.GetPrimaryKeys().GetStrId().GetData(), ",")), ", timestamps:", times)
					}
				}
			}
			markPrintln(fmt.Sprintf("channel_name=%s ,delete_data_len=%d", msgpack.EndPositions[0].GetChannelName(), deleteMsg.GetNumRows()))
			msgCount["delete_count"] += int(deleteMsg.GetNumRows())
		} else if msg.Type() == commonpb.MsgType_TimeTick {
			if detail > 1 && detail != 3 {
				timeTickMsg := msg.(*msgstream.TimeTickMsg)
				markPrintln("time tick msg info, ts:", tsoutil.PhysicalTime(timeTickMsg.EndTimestamp))
			}
		}
	}
	if detail > 1 && detail != 3 {
		markPrintln("msg count, end position:", util.Base64MsgPosition(msgpack.EndPositions[0]), ", endts:", tsoutil.PhysicalTime(msgpack.EndTs))
	}
}

func GetArrayString(n []int64) string {
	s := make([]string, len(n))
	for i, v := range n {
		s[i] = strconv.FormatInt(v, 10)
	}
	return strings.Join(s, ",")
}

func MsgStream(config PositionConfig, isTTStream bool) msgstream.MsgStream {
	var factory msgstream.Factory
	factoryCreator := reader.NewDefaultFactoryCreator()

	if config.Pulsar.Address != "" {
		factory = factoryCreator.NewPmsFactory(&config.Pulsar)
	} else if config.Kafka.Address != "" {
		factory = factoryCreator.NewKmsFactory(&config.Kafka)
	} else {
		panic(errors.New("fail to get the msg stream, check the mqConfig param"))
	}
	if isTTStream {
		stream, err := factory.NewTtMsgStream(context.Background())
		if err != nil {
			panic(err)
		}
		return stream
	}
	stream, err := factory.NewMsgStream(context.Background())
	if err != nil {
		panic(err)
	}
	return stream
}

func markPrintln(a ...any) {
	// a = append(a, "cdc-position-mark")
	fmt.Println(a...)
}
