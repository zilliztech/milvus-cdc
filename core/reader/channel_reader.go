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
	"encoding/base64"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/log"
	"github.com/zilliztech/milvus-cdc/core/util"
)

type ChannelReader struct {
	api.DefaultReader

	mqConfig             config.MQConfig
	factoryCreator       FactoryCreator
	channelName          string
	subscriptionPosition mqwrapper.SubscriptionInitialPosition
	seekPosition         string

	msgStream   msgstream.MsgStream
	dataHandler func(context.Context, *msgstream.MsgPack) bool // the return value is false means quit
	isQuit      util.Value[bool]
	startOnce   sync.Once
	quitOnce    sync.Once
}

var _ api.Reader = (*ChannelReader)(nil)

func NewChannelReader(channelName, seekPosition string, mqConfig config.MQConfig, dataHandler func(context.Context, *msgstream.MsgPack) bool, creator FactoryCreator) (api.Reader, error) {
	channelReader := &ChannelReader{
		factoryCreator:       creator,
		channelName:          channelName,
		seekPosition:         seekPosition,
		mqConfig:             mqConfig,
		dataHandler:          dataHandler,
		subscriptionPosition: mqwrapper.SubscriptionPositionUnknown,
	}
	if seekPosition == "" {
		channelReader.subscriptionPosition = mqwrapper.SubscriptionPositionLatest
	}
	channelReader.isQuit.Store(false)
	err := channelReader.initMsgStream()
	if err != nil {
		log.Warn("fail to init the msg stream", zap.Error(err))
		return nil, err
	}

	return channelReader, nil
}

func (c *ChannelReader) initMsgStream() error {
	var factory msgstream.Factory
	switch {
	case c.mqConfig.Pulsar.Address != "":
		factory = c.factoryCreator.NewPmsFactory(&c.mqConfig.Pulsar)
	case c.mqConfig.Kafka.Address != "":
		factory = c.factoryCreator.NewKmsFactory(&c.mqConfig.Kafka)
	default:
		return errors.New("fail to get the msg stream, check the mqConfig param")
	}
	stream, err := factory.NewMsgStream(context.Background())
	if err != nil {
		log.Warn("fail to new the msg stream", zap.Error(err))
		return err
	}

	rand.Seed(time.Now().UnixNano())
	consumeSubName := fmt.Sprintf("%s-%d", c.channelName, rand.Int31())
	err = stream.AsConsumer(context.Background(), []string{c.channelName}, consumeSubName, c.subscriptionPosition)
	if err != nil {
		log.Warn("fail to create the consumer", zap.Error(err))
		return err
	}
	log.Info("consume channel", zap.String("channel", c.channelName))

	if c.seekPosition != "" {
		decodeBytes, err := base64.StdEncoding.DecodeString(c.seekPosition)
		if err != nil {
			log.Warn("fail to decode the seek position", zap.Error(err))
			stream.Close()
			return err
		}
		msgPosition := &msgpb.MsgPosition{
			ChannelName: c.channelName,
			MsgID:       decodeBytes,
		}
		err = stream.Seek(context.Background(), []*msgstream.MsgPosition{msgPosition})
		if err != nil {
			log.Warn("fail to seek the msg position", zap.Any("position", msgPosition), zap.Error(err))
			stream.Close()
			return err
		}
	}
	c.msgStream = stream
	return nil
}

func (c *ChannelReader) StartRead(ctx context.Context) {
	c.startOnce.Do(func() {
		msgChan := c.msgStream.Chan()
		go func() {
			for {
				if c.isQuit.Load() {
					log.Info("the channel reader is quit")
					return
				}
				select {
				case <-ctx.Done():
					log.Warn("channel reader context is done")
					return
				case msgPack, ok := <-msgChan:
					if !ok || msgPack == nil {
						log.Info("the msg pack is nil, the channel reader is quit")
						return
					}
					if c.dataHandler == nil {
						log.Warn("the data handler is nil")
						return
					}
					if !c.dataHandler(ctx, msgPack) {
						log.Warn("the data handler return false, the channel reader is quit")
						return
					}
				}
			}
		}()
	})
}

func (c *ChannelReader) QuitRead(ctx context.Context) {
	c.quitOnce.Do(func() {
		c.isQuit.Store(true)
		c.msgStream.Close()
	})
}
