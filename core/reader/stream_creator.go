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
	"io"
	"math/rand"
	"strconv"
	"strings"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/mq/common"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgdispatcher"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"

	"github.com/zilliztech/milvus-cdc/core/log"
)

type StreamCreator interface {
	GetStreamChan(ctx context.Context, vchannel string, seekPosition *msgstream.MsgPosition) (<-chan *msgstream.MsgPack, io.Closer, error)
	CheckConnection(ctx context.Context, vchannel string, seekPosition *msgstream.MsgPosition) error
	GetChannelLatestMsgID(ctx context.Context, channelName string) ([]byte, error)
}

type FactoryStreamCreator struct {
	factory msgstream.Factory
}

func (fsc *FactoryStreamCreator) GetStreamChan(ctx context.Context,
	vchannel string,
	seekPosition *msgstream.MsgPosition,
) (<-chan *msgstream.MsgPack, io.Closer, error) {
	stream, err := getStream(ctx, fsc.factory, vchannel, seekPosition)
	if err != nil {
		return nil, nil, err
	}
	msgPackChan := make(chan *msgstream.MsgPack)
	go func() {
		for {
			consumePack, ok := <-stream.Chan()
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
	return msgPackChan, StreamCloser(func() {
		stream.Close()
	}), nil
}

func (fsc *FactoryStreamCreator) CheckConnection(ctx context.Context, vchannel string, seekPosition *msgstream.MsgPosition) error {
	stream, err := getStream(ctx, fsc.factory, vchannel, seekPosition)
	if err != nil {
		return err
	}
	stream.Close()
	return nil
}

func (fsc *FactoryStreamCreator) GetChannelLatestMsgID(ctx context.Context, channelName string) ([]byte, error) {
	return msgstream.GetChannelLatestMsgID(ctx, fsc.factory, channelName)
}

func getStream(ctx context.Context,
	factory msgstream.Factory,
	vchannel string,
	seekPosition *msgstream.MsgPosition,
) (msgstream.MsgStream, error) {
	stream, err := factory.NewMsgStream(ctx)
	log := log.With(zap.String("channel_name", vchannel))
	if err != nil {
		log.Warn("fail to new the msg stream", zap.Error(err))
		return nil, err
	}

	subPositionType := common.SubscriptionPositionUnknown
	if seekPosition == nil {
		subPositionType = common.SubscriptionPositionLatest
	}
	pchannel := funcutil.ToPhysicalChannel(vchannel)

	err = stream.AsConsumer(ctx, []string{pchannel}, pchannel+strconv.Itoa(rand.Int()), subPositionType)
	if err != nil {
		log.Warn("fail to consume the channel", zap.Error(err))
		stream.Close()
		return nil, err
	}

	if seekPosition != nil {
		err = stream.Seek(ctx, []*msgstream.MsgPosition{seekPosition}, false)
		if err != nil {
			log.Warn("fail to seek the msg stream", zap.Error(err))
			stream.Close()
			return nil, err
		}
		if seekPosition.Timestamp == 0 {
			log.Info("the seek timestamp is zero")
		}
		log.Info("success to seek the msg stream")
	}
	return stream, nil
}

type DisptachClientStreamCreator struct {
	// only be used for checking the connection
	factory        msgstream.Factory
	dispatchClient msgdispatcher.Client
}

func NewDisptachClientStreamCreator(factory msgstream.Factory, dispatchClient msgdispatcher.Client) *DisptachClientStreamCreator {
	return &DisptachClientStreamCreator{
		factory:        factory,
		dispatchClient: dispatchClient,
	}
}

func (dcsc *DisptachClientStreamCreator) GetStreamChan(ctx context.Context,
	vchannel string,
	seekPosition *msgstream.MsgPosition,
) (<-chan *msgstream.MsgPack, io.Closer, error) {
	log := log.With(zap.String("channel_name", vchannel))
	subPositionType := common.SubscriptionPositionUnknown
	if seekPosition == nil {
		subPositionType = common.SubscriptionPositionLatest
	}
	seekPosition = typeutil.Clone(seekPosition)
	// make the position channel is v channel
	if seekPosition != nil {
		seekPosition.ChannelName = CheckAndFixVirtualChannel(seekPosition.ChannelName)
	}
	if !IsVirtualChannel(vchannel) {
		log.Panic("the channel name is not virtual channel", zap.String("channel_name", vchannel))
	}
	msgpackChan, err := dcsc.dispatchClient.Register(ctx,
		msgdispatcher.NewStreamConfig(
			vchannel, seekPosition, subPositionType,
		),
	)
	if err != nil {
		log.Warn("fail to register the channel", zap.Error(err))
		return nil, nil, err
	}
	return msgpackChan, StreamCloser(func() {
		dcsc.dispatchClient.Deregister(vchannel)
	}), nil
}

func (dcsc *DisptachClientStreamCreator) CheckConnection(ctx context.Context, vchannel string, seekPosition *msgstream.MsgPosition) error {
	stream, err := getStream(ctx, dcsc.factory, vchannel, seekPosition)
	if err != nil {
		return err
	}
	stream.Close()
	return nil
}

func (dcsc *DisptachClientStreamCreator) GetChannelLatestMsgID(ctx context.Context, channelName string) ([]byte, error) {
	return msgstream.GetChannelLatestMsgID(ctx, dcsc.factory, channelName)
}

type StreamCloser func()

func (sc StreamCloser) Close() error {
	sc()
	return nil
}

func IsVirtualChannel(vchannel string) bool {
	i := strings.LastIndex(vchannel, "_")
	if i == -1 {
		return false
	}
	return strings.Contains(vchannel[i+1:], "v")
}

func CheckAndFixVirtualChannel(vchannel string) string {
	fakeVirtualName := "_fakev0"
	if IsVirtualChannel(vchannel) {
		return vchannel
	}
	return vchannel + fakeVirtualName
}
