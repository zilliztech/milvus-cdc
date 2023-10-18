package reader

import (
	"context"
	"encoding/base64"
	"errors"
	"math/rand"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"

	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/config"
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
	dataHandler func(*msgstream.MsgPack) bool // the return value is false means quit
	isQuit      util.Value[bool]
	startOnce   sync.Once
	quitOnce    sync.Once
}

var _ api.Reader = (*ChannelReader)(nil)

func NewChannelReader(channelName, seekPosition string, mqConfig config.MQConfig, dataHandler func(*msgstream.MsgPack) bool) (api.Reader, error) {
	channelReader := &ChannelReader{
		factoryCreator: NewDefaultFactoryCreator(),
		channelName:    channelName,
		seekPosition:   seekPosition,
		mqConfig:       mqConfig,
		dataHandler:    dataHandler,
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
	if c.mqConfig.Pulsar.Address != "" {
		factory = c.factoryCreator.NewPmsFactory(&c.mqConfig.Pulsar)
	} else if c.mqConfig.Kafka.Address != "" {
		factory = c.factoryCreator.NewKmsFactory(&c.mqConfig.Kafka)
	} else {
		return errors.New("fail to get the msg stream, check the mqConfig param")
	}
	stream, err := factory.NewMsgStream(context.Background())
	if err != nil {
		log.Warn("fail to new the msg stream", zap.Error(err))
		return err
	}

	consumeSubName := c.channelName + string(rand.Int31())
	stream.AsConsumer(context.Background(), []string{c.channelName}, consumeSubName, c.subscriptionPosition)
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
				msgPack, ok := <-msgChan
				if !ok || msgPack == nil {
					log.Info("the msg pack is nil, the channel reader is quit")
					return
				}
				if c.dataHandler == nil {
					log.Panic("the data handler is nil")
				}
				if !c.dataHandler(msgPack) {
					log.Warn("the data handler return false, the channel reader is quit")
					return
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
