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
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/zilliztech/milvus-cdc/core/config"
)

func CollectionInfoOption(collectionName string, positions map[string]*commonpb.KeyDataPair) config.Option[*MilvusCollectionReader] {
	return config.OptionFunc[*MilvusCollectionReader](func(object *MilvusCollectionReader) {
		object.collections = append(object.collections, CollectionInfo{
			collectionName: collectionName,
			positions:      positions,
		})
	})
}

func KafKaOption(options ...config.Option[*config.KafkaConfig]) config.Option[*MilvusCollectionReader] {
	return config.OptionFunc[*MilvusCollectionReader](func(object *MilvusCollectionReader) {
		object.mqConfig = config.MilvusMQConfig{Kafka: config.NewKafkaConfig(options...)}
	})
}

func PulsarOption(options ...config.Option[*config.PulsarConfig]) config.Option[*MilvusCollectionReader] {
	return config.OptionFunc[*MilvusCollectionReader](func(object *MilvusCollectionReader) {
		object.mqConfig = config.MilvusMQConfig{Pulsar: config.NewPulsarConfig(options...)}
	})
}

func MqOption(p config.PulsarConfig, k config.KafkaConfig) config.Option[*MilvusCollectionReader] {
	return config.OptionFunc[*MilvusCollectionReader](func(object *MilvusCollectionReader) {
		object.mqConfig = config.MilvusMQConfig{Pulsar: p, Kafka: k}
	})
}

func EtcdOption(c config.MilvusEtcdConfig) config.Option[*MilvusCollectionReader] {
	return config.OptionFunc[*MilvusCollectionReader](func(object *MilvusCollectionReader) {
		object.etcdConfig = c
	})
}

// MonitorOption the implement object of Monitor should include DefaultMonitor for the better compatibility
func MonitorOption(m Monitor) config.Option[*MilvusCollectionReader] {
	return config.OptionFunc[*MilvusCollectionReader](func(object *MilvusCollectionReader) {
		object.monitor = m
	})
}

func ChanLenOption(l int) config.Option[*MilvusCollectionReader] {
	return config.OptionFunc[*MilvusCollectionReader](func(object *MilvusCollectionReader) {
		if l > 0 {
			object.dataChanLen = l
		}
	})
}

func FactoryCreatorOption(f FactoryCreator) config.Option[*MilvusCollectionReader] {
	return config.OptionFunc[*MilvusCollectionReader](func(object *MilvusCollectionReader) {
		if f != nil {
			object.factoryCreator = f
		}
	})
}

func ShouldReadFuncOption(f ShouldReadFunc) config.Option[*MilvusCollectionReader] {
	return config.OptionFunc[*MilvusCollectionReader](func(object *MilvusCollectionReader) {
		if f != nil {
			object.shouldReadFunc = f
		}
	})
}

func MqChannelOption(p config.PulsarConfig, k config.KafkaConfig) config.Option[*ChannelReader] {
	return config.OptionFunc[*ChannelReader](func(object *ChannelReader) {
		object.mqConfig = config.MilvusMQConfig{Pulsar: p, Kafka: k}
	})
}

func FactoryChannelOption(f FactoryCreator) config.Option[*ChannelReader] {
	return config.OptionFunc[*ChannelReader](func(object *ChannelReader) {
		if f != nil {
			object.factoryCreator = f
		}
	})
}

func ChannelNameOption(c string) config.Option[*ChannelReader] {
	return config.OptionFunc[*ChannelReader](func(object *ChannelReader) {
		if c != "" {
			object.channelName = c
		}
	})
}

func SubscriptionPositionChannelOption(p mqwrapper.SubscriptionInitialPosition) config.Option[*ChannelReader] {
	return config.OptionFunc[*ChannelReader](func(object *ChannelReader) {
		object.subscriptionPosition = p
	})
}

func SeekPositionChannelOption(p string) config.Option[*ChannelReader] {
	return config.OptionFunc[*ChannelReader](func(object *ChannelReader) {
		object.seekPosition = p
	})
}

func DataChanChannelOption(l int) config.Option[*ChannelReader] {
	return config.OptionFunc[*ChannelReader](func(object *ChannelReader) {
		if l > 0 {
			object.dataChanLen = l
		}
	})
}
