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
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/mq/common"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"

	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/log"
	"github.com/zilliztech/milvus-cdc/core/msgdispatcher"
	"github.com/zilliztech/milvus-cdc/core/util"
)

type ChannelReader struct {
	api.DefaultReader

	channelName          string
	subscriptionPosition common.SubscriptionInitialPosition
	seekPosition         *msgpb.MsgPosition

	msgPackChan   <-chan *msgstream.MsgPack
	dataHandler   func(context.Context, *msgstream.MsgPack) bool // the return value is false means quit
	isQuit        util.Value[bool]
	startOnceFunc func(ctx context.Context)
	quitOnceFunc  func()
}

var _ api.Reader = (*ChannelReader)(nil)

func NewChannelReader(channelName, seekPosition string,
	dispatchClient msgdispatcher.Client,
	taskID string,
	dataHandler func(context.Context, *msgstream.MsgPack) bool,
) (api.Reader, error) {
	channelReader := &ChannelReader{
		channelName:          channelName,
		dataHandler:          dataHandler,
		subscriptionPosition: common.SubscriptionPositionUnknown,
	}
	if seekPosition == "" {
		channelReader.subscriptionPosition = common.SubscriptionPositionLatest
	}
	channelReader.isQuit.Store(false)
	err := channelReader.decodeSeekPosition(seekPosition)
	if err != nil {
		log.Warn("fail to seek the position", zap.Error(err))
		return nil, err
	}
	vchannel := util.GetVChannel(channelName, taskID)
	if channelReader.seekPosition != nil && !IsVirtualChannel(channelReader.seekPosition.ChannelName) {
		channelReader.seekPosition.ChannelName = vchannel
	}
	msgPackChan, err := dispatchClient.Register(context.Background(),
		msgdispatcher.NewStreamConfig(
			vchannel,
			channelReader.seekPosition,
			channelReader.subscriptionPosition,
		),
	)
	if err != nil {
		log.Warn("fail to init the msg stream", zap.Error(err))
		return nil, err
	}
	channelReader.msgPackChan = msgPackChan

	channelReader.startOnceFunc = util.OnceFuncWithContext(func(ctx context.Context) {
		go channelReader.readPack(ctx)
	})
	channelReader.quitOnceFunc = sync.OnceFunc(func() {
		channelReader.isQuit.Store(true)
		dispatchClient.Deregister(util.GetVChannel(channelName, taskID))
	})
	return channelReader, nil
}

// ONLY FOR TEST
func (c *ChannelReader) initMsgStream(mqConfig config.MQConfig, creator FactoryCreator) (func(), error) {
	var factory msgstream.Factory
	switch {
	case mqConfig.Pulsar.Address != "":
		factory = creator.NewPmsFactory(&mqConfig.Pulsar)
	case mqConfig.Kafka.Address != "":
		factory = creator.NewKmsFactory(&mqConfig.Kafka)
	default:
		return nil, errors.New("fail to get the msg stream, check the mqConfig param")
	}
	stream, err := factory.NewMsgStream(context.Background())
	if err != nil {
		log.Warn("fail to new the msg stream", zap.Error(err))
		return nil, err
	}

	rand.Seed(time.Now().UnixNano())
	consumeSubName := fmt.Sprintf("%s-%d", c.channelName, rand.Int31())
	err = stream.AsConsumer(context.Background(), []string{c.channelName}, consumeSubName, c.subscriptionPosition)
	if err != nil {
		log.Warn("fail to create the consumer", zap.Error(err))
		return nil, err
	}
	log.Info("consume channel", zap.String("channel", c.channelName))

	if c.seekPosition != nil {
		err = stream.Seek(context.Background(), []*msgstream.MsgPosition{c.seekPosition}, false)
		if err != nil {
			log.Warn("fail to seek the msg position",
				zap.Any("position", util.Base64MsgPosition(c.seekPosition)), zap.Error(err))
			stream.Close()
			return nil, err
		}
	}
	msgPackChan := make(chan *msgstream.MsgPack)
	go func() {
		consumePackChan := stream.Chan()
		for {
			consumePack, ok := <-consumePackChan
			if !ok {
				log.Info("the consume pack channel is closed")
				return
			}
			msgPack := &msgstream.MsgPack{
				BeginTs:        consumePack.BeginTs,
				EndTs:          consumePack.EndTs,
				Msgs:           make([]msgstream.TsMsg, 0),
				StartPositions: consumePack.StartPositions,
				EndPositions:   consumePack.EndPositions,
			}
			for _, msg := range consumePack.Msgs {
				unMsg, err := msg.Unmarshal(stream.GetUnmarshalDispatcher())
				if err != nil {
					log.Panic("fail to unmarshal the message", zap.Error(err))
				}
				msgPack.Msgs = append(msgPack.Msgs, unMsg)
			}
			msgPackChan <- msgPack
		}
	}()
	c.msgPackChan = msgPackChan
	return func() {
		stream.Close()
	}, nil
}

func (c *ChannelReader) decodeSeekPosition(seekPosition string) error {
	if seekPosition == "" {
		return nil
	}
	decodePosition, err := util.Base64DecodeMsgPosition(seekPosition)
	if err != nil {
		log.Warn("fail to decode the seek position", zap.Error(err))
		return err
	}
	c.seekPosition = decodePosition
	return nil
}

func (c *ChannelReader) StartRead(ctx context.Context) {
	c.startOnceFunc(ctx)
}

func (c *ChannelReader) readPack(ctx context.Context) {
	for {
		if c.isQuit.Load() {
			log.Info("the channel reader is quit")
			return
		}
		select {
		case <-ctx.Done():
			log.Warn("channel reader context is done")
			return
		case msgPack, ok := <-c.msgPackChan:
			if !ok {
				log.Info("the channel reader is quit")
				return
			}
			if c.dataHandler == nil {
				log.Warn("the data handler is nil")
				return
			}
			if msgPack == nil {
				log.Warn("the msg pack is nil")
				continue
			}
			// TODO when cdc chaos kill, maybe the collection has drop,
			// and the op message has not been handled, which will block the task
			if !c.dataHandler(ctx, msgPack) {
				log.Warn("the data handler return false, the channel reader is quit")
				return
			}
		}
	}
}

func (c *ChannelReader) QuitRead(ctx context.Context) {
	c.quitOnceFunc()
}
