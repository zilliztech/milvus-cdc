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

package tool

import (
	"context"
	"errors"
	"math/rand"
	"strconv"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus/pkg/mq/common"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/requestutil"

	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/reader"
)

type MsgStreamConfig struct {
	Pulsar       config.PulsarConfig
	Kafka        config.KafkaConfig
	SeekPosition *msgstream.MsgPosition
	PChannel     string
	TTStream     bool
}

type MsgPackHandler struct {
	MsgFilter        func(msg msgstream.TsMsg) bool
	InsertMsgHandler func(msg *msgstream.InsertMsg)
	DeleteMsgHandler func(msg *msgstream.DeleteMsg)
	AnyMsgHandler    func(msg msgstream.TsMsg)
}

func NameMsgFilter(databaseName, collectionName string) func(msg msgstream.TsMsg) bool {
	return func(msg msgstream.TsMsg) bool {
		msgDatabaseName, ok := requestutil.GetDbNameFromRequest(msg)
		if !ok {
			return true
		}
		msgCollectionName, ok := requestutil.GetCollectionNameFromRequest(msg)
		if !ok {
			return true
		}
		return msgDatabaseName != databaseName || msgCollectionName != collectionName
	}
}

type CollectionIDGetter interface {
	GetCollectionID() int64
}

func GetCollectionIDFromRequest(req interface{}) (any, bool) {
	getter, ok := req.(CollectionIDGetter)
	if !ok {
		return "", false
	}
	return getter.GetCollectionID(), true
}

func CollectionIDMsgFilter(collectionID int64) func(msg msgstream.TsMsg) bool {
	return func(msg msgstream.TsMsg) bool {
		msgCollectionID, ok := GetCollectionIDFromRequest(msg)
		if !ok {
			return true
		}
		return msgCollectionID != collectionID
	}
}

func MsgStream(ctx context.Context, streamConfig *MsgStreamConfig) (msgstream.MsgStream, error) {
	var factory msgstream.Factory
	factoryCreator := reader.NewDefaultFactoryCreator()

	if streamConfig.Pulsar.Address != "" {
		factory = factoryCreator.NewPmsFactory(&streamConfig.Pulsar)
	} else if streamConfig.Kafka.Address != "" {
		factory = factoryCreator.NewKmsFactory(&streamConfig.Kafka)
	} else {
		return nil, errors.New("fail to get the msg stream, check the mqConfig param")
	}
	if streamConfig.TTStream {
		stream, err := factory.NewTtMsgStream(ctx)
		if err != nil {
			return nil, err
		}
		return stream, nil
	}
	stream, err := factory.NewMsgStream(ctx)
	if err != nil {
		return nil, err
	}

	consumeSubName := streamConfig.PChannel + strconv.Itoa(rand.Int())
	initialPosition := common.SubscriptionPositionUnknown
	if streamConfig.SeekPosition == nil {
		initialPosition = common.SubscriptionPositionEarliest
	}
	err = stream.AsConsumer(ctx, []string{streamConfig.PChannel}, consumeSubName, initialPosition)
	if err != nil {
		stream.Close()
		return nil, err
	}
	if streamConfig.SeekPosition != nil {
		err = stream.Seek(ctx, []*msgstream.MsgPosition{streamConfig.SeekPosition}, true)
		if err != nil {
			stream.Close()
			return nil, err
		}
	}
	return stream, nil
}

func HandlePack(msgpack *msgstream.MsgPack, handler *MsgPackHandler) {
	for _, msg := range msgpack.Msgs {
		if handler.MsgFilter != nil && handler.MsgFilter(msg) {
			continue
		}
		switch m := msg.(type) {
		case *msgstream.InsertMsg:
			if handler.InsertMsgHandler != nil {
				handler.InsertMsgHandler(m)
			}
		case *msgstream.DeleteMsg:
			if handler.DeleteMsgHandler != nil {
				handler.DeleteMsgHandler(m)
			}
		}
		if handler.AnyMsgHandler != nil {
			handler.AnyMsgHandler(msg)
		}
	}
}

func GetInsertPKs(msg *msgstream.InsertMsg, fieldName string) ([]string, []uint64) {
	var pks []string
	for _, data := range msg.GetFieldsData() {
		if data.GetFieldName() != fieldName {
			continue
		}
		if data.GetScalars().GetLongData() != nil {
			pks = lo.Map(data.GetScalars().GetLongData().GetData(), func(t int64, i int) string {
				return strconv.FormatInt(t, 10)
			})
		} else if data.GetScalars().GetStringData() != nil {
			pks = data.GetScalars().GetStringData().GetData()
		}
		break
	}
	tss := msg.GetTimestamps()
	return pks, tss
}

func GetDeletePKs(msg *msgstream.DeleteMsg) ([]string, []uint64) {
	var pks []string
	if msg.GetPrimaryKeys().GetIntId() != nil {
		pks = lo.Map(msg.GetPrimaryKeys().GetIntId().GetData(), func(t int64, i int) string {
			return strconv.FormatInt(t, 10)
		})
	} else if msg.GetPrimaryKeys().GetStrId() != nil {
		pks = msg.GetPrimaryKeys().GetStrId().GetData()
	}
	tss := msg.GetTimestamps()
	return pks, tss
}
