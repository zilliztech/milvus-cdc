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
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/zilliztech/milvus-cdc/core/model"
	"github.com/zilliztech/milvus-cdc/core/util"
	"go.uber.org/zap"
)

type MeetFunc func(m map[string]*model.CDCData)

type DataBarrier struct {
	total    int
	cur      util.Value[int]
	m        util.Map[string, *model.CDCData]
	meetFunc MeetFunc
}

func NewDataBarrier(count int, meetFunc MeetFunc) *DataBarrier {
	m := &DataBarrier{
		total:    count,
		meetFunc: meetFunc,
	}
	m.cur.Store(0)
	return m
}

// AddData it will return true if the barrier is meet
func (m *DataBarrier) AddData(channelName string, data *model.CDCData) bool {
	m.m.Store(channelName, data)
	m.cur.CompareAndSwapWithFunc(func(old int) int {
		return old + 1
	})
	if m.cur.Load() == m.total {
		m.meetFunc(m.m.GetUnsafeMap())
		return true
	}
	return false
}

type SendDataFunc func(data *model.CDCData)

type DataBarrierManager struct {
	total    int
	m        util.Map[string, *DataBarrier]
	sendFunc SendDataFunc
}

func NewDataBarrierManager(count int, dataFunc SendDataFunc) *DataBarrierManager {
	return &DataBarrierManager{
		total:    count,
		sendFunc: dataFunc,
	}
}

func (d *DataBarrierManager) IsBarrierData(data *model.CDCData) bool {
	msgType := data.Msg.Type()
	return msgType == commonpb.MsgType_DropCollection || msgType == commonpb.MsgType_DropPartition
}

func (d *DataBarrierManager) IsEmpty() bool {
	empty := true
	d.m.Range(func(_ string, _ *DataBarrier) bool {
		empty = false
		return false
	})
	return empty
}

func (d *DataBarrierManager) AddData(channelName string, data *model.CDCData) bool {
	switch data.Msg.(type) {
	case *msgstream.DropCollectionMsg:
		return d.addDropCollectionData(channelName, data)
	case *msgstream.DropPartitionMsg:
		return d.addDropPartitionData(channelName, data)
	default:
		log.Warn("the msg type not support", zap.String("type", data.Msg.Type().String()),
			zap.Any("channel", channelName), zap.Any("data", data))
	}
	return false
}

func (d *DataBarrierManager) addDropCollectionData(channelName string, data *model.CDCData) bool {
	msg := data.Msg.(*msgstream.DropCollectionMsg)
	barrier, _ := d.m.LoadOrStore(fmt.Sprintf("drop_collection_%d", msg.CollectionID), NewDataBarrier(d.total, func(m map[string]*model.CDCData) {
		dropCollectionCdcData := &model.CDCData{
			Extra: make(map[string]any),
		}
		var otherDropData []*msgstream.DropCollectionMsg
		for _, cdcData := range m {
			if dropCollectionCdcData.Msg == nil {
				dropCollectionCdcData.Msg = cdcData.Msg
			} else {
				otherDropData = append(otherDropData, cdcData.Msg.(*msgstream.DropCollectionMsg))
			}
		}
		dropCollectionCdcData.Extra[model.DropCollectionMsgsKey] = otherDropData
		d.sendFunc(dropCollectionCdcData)
	}))
	//log.Info("drop collection debug", zap.Int64("collection_id", msg.CollectionID), zap.String("channel_name", channelName))
	return barrier.AddData(channelName, data)
}

func (d *DataBarrierManager) addDropPartitionData(channelName string, data *model.CDCData) bool {
	msg := data.Msg.(*msgstream.DropPartitionMsg)
	barrier, _ := d.m.LoadOrStore(fmt.Sprintf("drop_partition_%d_%d", msg.CollectionID, msg.PartitionID), NewDataBarrier(d.total, func(m map[string]*model.CDCData) {
		dropPartitionCdcData := &model.CDCData{
			Extra: make(map[string]any),
		}
		var otherDropData []*msgstream.DropPartitionMsg
		for _, cdcData := range m {
			if dropPartitionCdcData.Msg == nil {
				dropPartitionCdcData.Msg = cdcData.Msg
			} else {
				otherDropData = append(otherDropData, cdcData.Msg.(*msgstream.DropPartitionMsg))
			}
		}
		dropPartitionCdcData.Extra[model.DropPartitionMsgsKey] = otherDropData
		d.sendFunc(dropPartitionCdcData)
	}))
	//log.Info("drop partition debug", zap.Int64("collection_id", msg.CollectionID),
	//	zap.Int64("partition_id", msg.PartitionID), zap.String("partition_name", msg.PartitionName),
	//	zap.String("channel_name", channelName))
	return barrier.AddData(channelName, data)
}
