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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/pkg/mq/msgstream"

	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/mocks"
)

func TestNewChannelReader(t *testing.T) {
	t.Run("channel mq", func(t *testing.T) {
		creator := mocks.NewFactoryCreator(t)
		factory := msgstream.NewMockMqFactory()
		stream := msgstream.NewMockMsgStream(t)

		creator.EXPECT().NewPmsFactory(mock.Anything).Return(factory)
		{
			// wrong format seek position
			factory.NewMsgStreamFunc = func(ctx context.Context) (msgstream.MsgStream, error) {
				return nil, errors.New("error")
			}
			_, err := NewChannelReader("test", "test", config.MQConfig{
				Pulsar: config.PulsarConfig{
					Address: "localhost",
				},
			}, nil, creator)
			assert.Error(t, err)
		}

		{
			// msg stream error
			factory.NewMsgStreamFunc = func(ctx context.Context) (msgstream.MsgStream, error) {
				return nil, errors.New("error")
			}
			_, err := NewChannelReader("test", "", config.MQConfig{
				Pulsar: config.PulsarConfig{
					Address: "localhost",
				},
			}, nil, creator)
			assert.Error(t, err)
		}

		factory.NewMsgStreamFunc = func(ctx context.Context) (msgstream.MsgStream, error) {
			return stream, nil
		}

		{
			// as consumer error
			stream.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("error")).Once()
			_, err := NewChannelReader("test", "", config.MQConfig{
				Pulsar: config.PulsarConfig{
					Address: "localhost",
				},
			}, nil, creator)
			assert.Error(t, err)
		}

		{
			// seek error
			stream.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
			stream.EXPECT().Close().Return().Once()
			stream.EXPECT().Seek(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("error")).Once()
			_, err := NewChannelReader("test", base64.StdEncoding.EncodeToString([]byte("foo")), config.MQConfig{
				Pulsar: config.PulsarConfig{
					Address: "localhost",
				},
			}, nil, creator)
			assert.Error(t, err)
		}

		{
			// success
			stream.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
			stream.EXPECT().Seek(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
			packChan := make(chan *msgstream.MsgPack, 1)
			stream.EXPECT().Chan().Return(packChan).Once()
			_, err := NewChannelReader("test", base64.StdEncoding.EncodeToString([]byte("foo")), config.MQConfig{
				Pulsar: config.PulsarConfig{
					Address: "localhost",
				},
				Kafka: config.KafkaConfig{},
			}, nil, creator)
			assert.NoError(t, err)
		}
	})

	t.Run("mq config", func(t *testing.T) {
		creator := mocks.NewFactoryCreator(t)
		{
			_, err := NewChannelReader("test", "", config.MQConfig{}, nil, creator)
			assert.Error(t, err)
		}

		factory := msgstream.NewMockMqFactory()
		stream := msgstream.NewMockMsgStream(t)
		stream.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		packChan := make(chan *msgstream.MsgPack, 1)
		stream.EXPECT().Chan().Return(packChan)
		factory.NewMsgStreamFunc = func(ctx context.Context) (msgstream.MsgStream, error) {
			return stream, nil
		}
		{
			creator.EXPECT().NewPmsFactory(mock.Anything).Return(factory).Once()
			_, err := NewChannelReader("test", "", config.MQConfig{
				Pulsar: config.PulsarConfig{
					Address: "localhost",
				},
			}, nil, creator)
			assert.NoError(t, err)
		}
		{
			creator.EXPECT().NewKmsFactory(mock.Anything).Return(factory).Once()
			_, err := NewChannelReader("test", "", config.MQConfig{
				Kafka: config.KafkaConfig{
					Address: "localhost",
				},
			}, nil, creator)
			assert.NoError(t, err)
		}
	})
}

func TestChannelReader(t *testing.T) {
	ctx := context.Background()
	creator := mocks.NewFactoryCreator(t)
	factory := msgstream.NewMockMqFactory()
	stream := msgstream.NewMockMsgStream(t)

	creator.EXPECT().NewPmsFactory(mock.Anything).Return(factory)
	factory.NewMsgStreamFunc = func(ctx context.Context) (msgstream.MsgStream, error) {
		return stream, nil
	}
	stream.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	stream.EXPECT().Close().Return()

	t.Run("quit", func(t *testing.T) {
		dataChan := make(chan *msgstream.MsgPack, 1)
		stream.EXPECT().Chan().Return(dataChan).Once()
		reader, err := NewChannelReader("test", "", config.MQConfig{
			Pulsar: config.PulsarConfig{
				Address: "localhost",
			},
		}, nil, creator)
		assert.NoError(t, err)
		reader.QuitRead(ctx)
		reader.StartRead(ctx)
	})

	t.Run("close", func(t *testing.T) {
		dataChan := make(chan *msgstream.MsgPack, 1)
		stream.EXPECT().Chan().Return(dataChan).Once()
		reader, err := NewChannelReader("test", "", config.MQConfig{
			Pulsar: config.PulsarConfig{
				Address: "localhost",
			},
		}, nil, creator)
		assert.NoError(t, err)
		reader.StartRead(ctx)
		close(dataChan)
	})

	t.Run("empty handler", func(t *testing.T) {
		dataChan := make(chan *msgstream.MsgPack, 1)
		stream.EXPECT().Chan().Return(dataChan).Once()
		reader, err := NewChannelReader("test", "", config.MQConfig{
			Pulsar: config.PulsarConfig{
				Address: "localhost",
			},
		}, nil, creator)
		assert.NoError(t, err)
		dataChan <- &msgstream.MsgPack{}
		reader.StartRead(ctx)
		time.Sleep(time.Second)
	})

	t.Run("success", func(t *testing.T) {
		dataChan := make(chan *msgstream.MsgPack, 1)
		stream.EXPECT().Chan().Return(dataChan).Once()
		reader, err := NewChannelReader("test", "", config.MQConfig{
			Pulsar: config.PulsarConfig{
				Address: "localhost",
			},
		}, func(ctx context.Context, pack *msgstream.MsgPack) bool {
			return pack.BeginTs == pack.EndTs
		}, creator)
		assert.NoError(t, err)
		reader.StartRead(ctx)
		dataChan <- &msgstream.MsgPack{
			BeginTs: 1,
			EndTs:   1,
		}
		dataChan <- &msgstream.MsgPack{
			BeginTs: 2,
			EndTs:   3,
		}
		time.Sleep(time.Second)
	})
}
