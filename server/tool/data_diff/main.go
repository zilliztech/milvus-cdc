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
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/funcutil"

	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/log"
	"github.com/zilliztech/milvus-cdc/server/tool"
)

type DataReadConfig struct {
	Etcd       config.EtcdServerConfig
	Pulsar     config.PulsarConfig
	Kafka      config.KafkaConfig
	ChannelMap map[string]string `yaml:"channelMap"`

	Database   string // root config
	Collection string

	ID         int64 // not config
	DecodeType int   // not config
}

type PKData struct {
	PK     string
	Insert []uint64
	Delete []uint64
}

func (p *PKData) String() string {
	return fmt.Sprintf("PK: %s, Insert: %v, Delete: %v", p.PK, p.Insert, p.Delete)
}

type DiffData struct {
	Current *PKData // exist data
	Deleted *PKData // deleted data
}

type Config struct {
	A          DataReadConfig
	B          DataReadConfig
	Timeout    int
	Database   string
	Collection string
	PrintDiff  bool `yaml:"printDiff"`
	DecodeType int  `yaml:"decodeType"` // 0: same the cdc task, 1: msg position raw data
}

func main() {
	yamlConfig := tool.GetConfig[Config]("data_diff.yaml")
	w := &sync.WaitGroup{}
	w.Add(2)
	var dataA, dataB map[string]*PKData
	yamlConfig.A.Collection = yamlConfig.Collection
	yamlConfig.B.Collection = yamlConfig.Collection
	yamlConfig.A.DecodeType = yamlConfig.DecodeType
	yamlConfig.A.Database = yamlConfig.Database
	yamlConfig.B.Database = yamlConfig.Database
	yamlConfig.B.DecodeType = yamlConfig.DecodeType
	log.Info("Start to read data", zap.Any("config", yamlConfig))
	if yamlConfig.Timeout == 0 {
		yamlConfig.Timeout = 600
	}
	go func() {
		defer w.Done()
		dataA = ReadData(yamlConfig.A, yamlConfig.Timeout)
		log.Info("Read data A done")
	}()
	go func() {
		defer w.Done()
		dataB = ReadData(yamlConfig.B, yamlConfig.Timeout)
		log.Info("Read data B done")
	}()
	w.Wait()
	// diff dataA and dataB
	log.Info("Start to diff data")
	diffA := make(map[string]*DiffData) // A contain B not contain
	diffB := make(map[string]*DiffData) // B contain A not contain
	dataCntA := 0
	dataCntB := 0
	for k, v := range dataA {
		if ValidData(v) {
			dataCntA++
		}
		b := dataB[k]
		if b == nil {
			diffA[k] = &DiffData{
				Current: v,
			}
			continue
		}

		if ValidData(v) && !ValidData(b) {
			diffA[k] = &DiffData{
				Current: v,
				Deleted: b,
			}
		}
		if !ValidData(v) && ValidData(b) {
			diffB[k] = &DiffData{
				Current: b,
				Deleted: v,
			}
		}
	}

	for k, v := range dataB {
		if ValidData(v) {
			dataCntB++
		}
		if _, ok := dataA[k]; !ok {
			diffB[k] = &DiffData{
				Current: v,
			}
		}
	}

	log.Info("Diff data done")
	if yamlConfig.PrintDiff {
		for k, v := range diffA {
			log.Info("Diff A", zap.String("pk", k), zap.Any("current", v.Current), zap.Any("deleted", v.Deleted))
		}
		for k, v := range diffB {
			log.Info("Diff B", zap.String("pk", k), zap.Any("current", v.Current), zap.Any("deleted", v.Deleted))
		}
	}

	log.Info("data result", zap.Int("dataA", dataCntA), zap.Int("dataB", dataCntB))
	log.Info("Diff result", zap.Int("diffA", len(diffA)), zap.Int("diffB", len(diffB)))
}

func ValidData(data *PKData) bool {
	maxInsert := uint64(0)
	maxDelete := uint64(0)
	for _, v := range data.Insert {
		if v > maxInsert {
			maxInsert = v
		}
	}
	for _, v := range data.Delete {
		if v > maxDelete {
			maxDelete = v
		}
	}
	return maxInsert >= maxDelete
}

func ReadData(readConfig DataReadConfig, timeout int) map[string]*PKData {
	if len(readConfig.Etcd.Address) == 0 {
		return map[string]*PKData{}
	}
	collectionInfo, err := tool.GetCollectionInfo(readConfig.Etcd, tool.CollectionNameInfo{
		DBName:         readConfig.Database,
		CollectionName: readConfig.Collection,
	})
	if err != nil {
		log.Panic("Failed to get collection info", zap.Error(err))
	}
	readConfig.ID = collectionInfo.GetID()
	pkField := tool.GetPkField(collectionInfo.GetSchema())
	if pkField == nil {
		log.Panic("Failed to get pk field")
	}
	w := &sync.WaitGroup{}
	var dataLock sync.Mutex
	allData := make(map[string]*PKData)
	channelNum := len(collectionInfo.GetPhysicalChannelNames())
	w.Add(channelNum)
	seekPositions := make([]*commonpb.KeyDataPair, 0)
	if len(readConfig.ChannelMap) > 0 {
		decodeFunc := tool.DecodeType0
		if readConfig.DecodeType == 1 {
			decodeFunc = tool.DecodeType1
		}
		for k, v := range readConfig.ChannelMap {
			seekPosition, err := decodeFunc(k, v)
			if err != nil {
				log.Panic("Failed to decode seek position", zap.Error(err))
			}
			seekPositions = append(seekPositions, seekPosition)
		}
	} else {
		seekPositions = collectionInfo.GetStartPositions()
	}
	if len(seekPositions) != channelNum {
		log.Panic("Seek position not match channel num")
	}
	for _, startPosition := range seekPositions {
		go func(p *commonpb.KeyDataPair) {
			defer w.Done()
			streamData := GetStreamData(readConfig, p, pkField, timeout)
			dataLock.Lock()
			defer dataLock.Unlock()
			for k, v := range streamData {
				data, ok := allData[k]
				if !ok {
					allData[k] = v
					continue
				}
				v.Insert = append(v.Insert, data.Insert...)
				v.Delete = append(v.Delete, data.Delete...)
			}
		}(startPosition)
	}
	w.Wait()
	return allData
}

func GetStreamData(
	readConfig DataReadConfig,
	p *commonpb.KeyDataPair,
	pkField *schemapb.FieldSchema,
	timeout int,
) map[string]*PKData {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()
	pchannel := funcutil.ToPhysicalChannel(p.GetKey())
	msgStream, err := tool.MsgStream(ctx, &tool.MsgStreamConfig{
		Pulsar:   readConfig.Pulsar,
		Kafka:    readConfig.Kafka,
		PChannel: pchannel,
		TTStream: true,
		SeekPosition: &msgstream.MsgPosition{
			ChannelName: p.GetKey(),
			MsgID:       p.GetData(),
		},
	})
	if err != nil {
		log.Panic("Failed to create msg stream",
			zap.String("pchannel", pchannel),
			zap.Any("pulsar", readConfig.Pulsar),
			zap.Any("kafka", readConfig.Kafka),
			zap.Error(err),
		)
	}
	defer msgStream.Close()
	latestID, err := msgStream.GetLatestMsgID(pchannel)
	if err != nil {
		log.Panic("Failed to get latest msg id", zap.Error(err))
	}
	dataMap := make(map[string]*PKData)
	rateLog := log.NewRateLog(0.1, log.L())
	handler := GetMsgPackHandler(dataMap, pkField, readConfig.ID)
	for {
		select {
		case <-ctx.Done():
			log.Warn("Timeout", zap.Int("timeout", timeout))
			return dataMap
		case consumePack := <-msgStream.Chan():
			msgPack := &msgstream.MsgPack{
				BeginTs:        consumePack.BeginTs,
				EndTs:          consumePack.EndTs,
				Msgs:           make([]msgstream.TsMsg, 0),
				StartPositions: consumePack.StartPositions,
				EndPositions:   consumePack.EndPositions,
			}
			for _, msg := range consumePack.Msgs {
				unMsg, err := msg.Unmarshal(msgStream.GetUnmarshalDispatcher())
				if err != nil {
					log.Panic("fail to unmarshal the message", zap.Error(err))
				}
				msgPack.Msgs = append(msgPack.Msgs, unMsg)
			}
			end := msgPack.EndPositions[0]
			ok, err := latestID.LessOrEqualThan(end.GetMsgID())
			if err != nil {
				msgStream.Close()
				log.Panic("less or equal err", zap.Error(err))
			}
			tool.HandlePack(msgPack, handler)
			rateLog.Info("receive data...",
				zap.String("pchannel", pchannel),
				zap.Any("begin_ts", msgPack.BeginTs),
				zap.Uint64("end_ts", msgPack.EndTs),
				zap.Any("star_position", msgPack.StartPositions[0].GetTimestamp()),
				zap.Any("end_position", msgPack.EndPositions[0].GetTimestamp()),
			)
			if ok {
				log.Info("Get all data", zap.String("pchannel", pchannel))
				return dataMap
			}
		}
	}
}

func GetMsgPackHandler(
	dataMap map[string]*PKData,
	pkField *schemapb.FieldSchema,
	collectionID int64,
) *tool.MsgPackHandler {
	return &tool.MsgPackHandler{
		MsgFilter: tool.CollectionIDMsgFilter(collectionID),
		InsertMsgHandler: func(msg *msgstream.InsertMsg) {
			pks, tss := tool.GetInsertPKs(msg, pkField.GetName())
			for i, pk := range pks {
				if _, ok := dataMap[pk]; !ok {
					dataMap[pk] = &PKData{
						PK: pk,
					}
				}
				dataMap[pk].Insert = append(dataMap[pk].Insert, tss[i])
			}
			log.Info("Insert msg",
				zap.Uint64("ts", msg.BeginTimestamp),
				zap.Any("pks", pks),
				zap.Any("tss", tss),
			)
		},
		DeleteMsgHandler: func(msg *msgstream.DeleteMsg) {
			pks, tss := tool.GetDeletePKs(msg)
			for i, pk := range pks {
				if _, ok := dataMap[pk]; !ok {
					dataMap[pk] = &PKData{
						PK: pk,
					}
				}
				dataMap[pk].Delete = append(dataMap[pk].Delete, tss[i])
			}
		},
	}
}
