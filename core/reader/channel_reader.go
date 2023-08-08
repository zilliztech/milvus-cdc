package reader

import (
	"context"
	"encoding/base64"
	"errors"
	"math/rand"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/model"
	"github.com/zilliztech/milvus-cdc/core/util"
	"go.uber.org/zap"
)

type ChannelReader struct {
	DefaultReader

	mqConfig             config.MilvusMQConfig
	factoryCreator       FactoryCreator
	channelName          string
	subscriptionPosition mqwrapper.SubscriptionInitialPosition
	seekPosition         string

	msgStream   msgstream.MsgStream
	dataChan    chan *model.CDCData
	dataChanLen int
	isQuit      util.Value[bool]
	startOnce   sync.Once
	quitOnce    sync.Once
}

func NewChannelReader(options ...config.Option[*ChannelReader]) (*ChannelReader, error) {
	channelReader := &ChannelReader{
		factoryCreator: NewDefaultFactoryCreator(),
		dataChanLen:    100,
	}
	for _, option := range options {
		option.Apply(channelReader)
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
	stream.AsConsumer([]string{c.channelName}, consumeSubName, c.subscriptionPosition)
	log.Info("consume channel", zap.String("channel", c.channelName))

	if c.seekPosition != "" {
		decodeBytes, err := base64.StdEncoding.DecodeString(c.seekPosition)
		if err != nil {
			log.Warn("fail to decode the seek position", zap.Error(err))
			stream.Close()
			return err
		}
		msgPosition := &msgpb.MsgPosition{}
		err = proto.Unmarshal(decodeBytes, msgPosition)
		if err != nil {
			log.Warn("fail to unmarshal the seek position", zap.Error(err))
			stream.Close()
			return err
		}
		err = stream.Seek([]*msgstream.MsgPosition{msgPosition})
		if err != nil {
			log.Warn("fail to seek the msg position", zap.Any("position", msgPosition), zap.Error(err))
			return err
		}
	}
	c.msgStream = stream
	return nil
}

func (c *ChannelReader) StartRead(ctx context.Context) <-chan *model.CDCData {
	c.startOnce.Do(func() {
		c.dataChan = make(chan *model.CDCData, c.dataChanLen)
		msgChan := c.msgStream.Chan()
		go func() {
			for {
				if c.isQuit.Load() {
					close(c.dataChan)
					return
				}
				msgPack, ok := <-msgChan
				if !ok || msgPack == nil {
					return
				}
				for _, msg := range msgPack.Msgs {
					log.Info("msgType", zap.Any("msg_type", msg.Type()))
					c.dataChan <- &model.CDCData{
						Msg: msg,
					}
				}
			}
		}()
	})

	return c.dataChan
}

func (c *ChannelReader) QuitRead(ctx context.Context) {
	c.quitOnce.Do(func() {
		c.isQuit.Store(true)
		c.msgStream.Close()
	})
}
