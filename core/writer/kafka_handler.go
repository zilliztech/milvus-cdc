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

package writer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/log"
)

type KafkaDataHandler struct {
	api.DataHandler

	address           string
	topic             string
	saslUsername      string
	saslPassword      string
	saslMechanisms    string
	securityProtocol  string
	enableSASL        bool
	connectionTimeout int
	producer          *kafka.Producer
	deliveryChan      chan kafka.Event
}

// NewKafkaDataHandler returns a singleton KafkaDataHandler
func NewKafkaDataHandler(options ...config.Option[*KafkaDataHandler]) (*KafkaDataHandler, error) {
	handler := &KafkaDataHandler{
		connectionTimeout: 5,
		enableSASL:        false,
	}

	for _, option := range options {
		option.Apply(handler)
	}
	if handler.address == "" {
		return nil, errors.New("empty kafka address")
	}

	if handler.topic == "" {
		return nil, errors.New("empty kafka topic")
	}

	conf := kafka.ConfigMap{
		"bootstrap.servers": handler.address,
		"acks":              "all",
	}
	if handler.enableSASL {
		conf["sasl.username"] = handler.saslUsername
		conf["sasl.password"] = handler.saslPassword
		conf["sasl.mechanisms"] = handler.saslMechanisms
		conf["security.protocol"] = handler.securityProtocol
	}

	producer, err := kafka.NewProducer(&conf)
	if err != nil {
		log.Warn("fail to create kafka producer", zap.Error(err))
		return nil, err
	}
	handler.producer = producer
	handler.deliveryChan = make(chan kafka.Event)

	return handler, err
}

func (k *KafkaDataHandler) GetKafkaProducer() *kafka.Producer {
	return k.producer
}

func (k *KafkaDataHandler) KafkaOp(ctx context.Context, database string, f func(p *kafka.Producer, d chan kafka.Event) error) error {
	kafkaFunc := func() error {
		p := k.producer
		d := k.deliveryChan
		err := f(p, d)
		if err != nil {
			return err
		}
		return nil
	}
	return kafkaFunc()
}

func (k *KafkaDataHandler) CreateCollection(ctx context.Context, param *api.CreateCollectionParam) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, deliverChan chan kafka.Event) error {
		val := fmt.Sprintf("create collection %v", param.Schema.CollectionName)
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &k.topic,
				Partition: kafka.PartitionAny,
			},
			Value: []byte(val),
		}
		err := p.Produce(msg, nil)
		if err != nil {
			log.Warn("fail to produce msg", zap.Error(err))
			return err
		}

		select {
		case e := <-deliverChan:
			ev := e.(*kafka.Message)
			if ev.TopicPartition.Error != nil {
				log.Warn("fail to deliver msg", zap.Error(ev.TopicPartition.Error))
				return ev.TopicPartition.Error
			}
		case <-time.After(time.Second):
			log.Warn("deliver msg timeout", zap.Error(errors.New("timeout")))
		}
		return nil
	})
}

func (k *KafkaDataHandler) DropCollection(ctx context.Context, param *api.DropCollectionParam) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		val := fmt.Sprintf("drop collection %v", param.CollectionName)
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &k.topic,
				Partition: kafka.PartitionAny,
			},
			Value: []byte(val),
		}
		err := p.Produce(msg, dc)
		if err != nil {
			log.Warn("fail to produce msg", zap.Error(err))
			return err
		}

		select {
		case e := <-dc:
			ev := e.(*kafka.Message)
			if ev.TopicPartition.Error != nil {
				log.Warn("fail to deliver msg", zap.Error(ev.TopicPartition.Error))
				return ev.TopicPartition.Error
			}
		case <-time.After(time.Second):
			log.Warn("deliver msg timeout", zap.Error(errors.New("timeout")))
		}
		return nil
	})
}

func (k *KafkaDataHandler) CreatePartition(ctx context.Context, param *api.CreatePartitionParam) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		val := fmt.Sprintf("create partition %v in collection %v", param.PartitionName, param.CollectionName)
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &k.topic,
				Partition: kafka.PartitionAny,
			},
			Value: []byte(val),
		}
		err := p.Produce(msg, dc)
		if err != nil {
			log.Warn("fail to produce msg", zap.Error(err))
			return err
		}

		select {
		case e := <-dc:
			ev := e.(*kafka.Message)
			if ev.TopicPartition.Error != nil {
				log.Warn("fail to deliver msg", zap.Error(ev.TopicPartition.Error))
				return ev.TopicPartition.Error
			}
		case <-time.After(time.Second):
			log.Warn("deliver msg timeout", zap.Error(errors.New("timeout")))
		}
		return nil
	})
}

func (k *KafkaDataHandler) DropPartition(ctx context.Context, param *api.DropPartitionParam) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		val := fmt.Sprintf("drop partition %v in collection %v", param.PartitionName, param.CollectionName)
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &k.topic,
				Partition: kafka.PartitionAny,
			},
			Value: []byte(val),
		}
		err := p.Produce(msg, dc)
		if err != nil {
			log.Warn("fail to produce msg", zap.Error(err))
			return err
		}

		select {
		case e := <-dc:
			ev := e.(*kafka.Message)
			if ev.TopicPartition.Error != nil {
				log.Warn("fail to deliver msg", zap.Error(ev.TopicPartition.Error))
				return ev.TopicPartition.Error
			}
		case <-time.After(time.Second):
			log.Warn("deliver msg timeout", zap.Error(errors.New("timeout")))
		}
		return nil
	})
}

func (k *KafkaDataHandler) CreateDatabase(ctx context.Context, param *api.CreateDatabaseParam) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		val := fmt.Sprintf("create database %v", param.DbName)
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &k.topic,
				Partition: kafka.PartitionAny,
			},
			Value: []byte(val),
		}
		err := p.Produce(msg, dc)
		if err != nil {
			log.Warn("fail to produce msg", zap.Error(err))
			return err
		}

		select {
		case e := <-dc:
			ev := e.(*kafka.Message)
			if ev.TopicPartition.Error != nil {
				log.Warn("fail to deliver msg", zap.Error(ev.TopicPartition.Error))
				return ev.TopicPartition.Error
			}
		case <-time.After(time.Second):
			log.Warn("deliver msg timeout", zap.Error(errors.New("timeout")))
		}
		return nil
	})
}

func (k *KafkaDataHandler) DropDatabase(ctx context.Context, param *api.DropDatabaseParam) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		val := fmt.Sprintf("drop database %v", param.DbName)
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &k.topic,
				Partition: kafka.PartitionAny,
			},
			Value: []byte(val),
		}
		err := p.Produce(msg, dc)
		if err != nil {
			log.Warn("fail to produce msg", zap.Error(err))
			return err
		}

		select {
		case e := <-dc:
			ev := e.(*kafka.Message)
			if ev.TopicPartition.Error != nil {
				log.Warn("fail to deliver msg", zap.Error(ev.TopicPartition.Error))
				return ev.TopicPartition.Error
			}
		case <-time.After(time.Second):
			log.Warn("deliver msg timeout", zap.Error(errors.New("timeout")))
		}
		return nil
	})
}

func (k *KafkaDataHandler) Insert(ctx context.Context, param *api.InsertParam, f api.DataFormatter) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		val, err := f.Format(param)
		if err != nil {
			log.Warn("fail to format data", zap.Error(err))
		}
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &k.topic,
				Partition: kafka.PartitionAny,
			},
			Value: val,
		}
		err = p.Produce(msg, dc)
		if err != nil {
			log.Warn("fail to produce msg", zap.Error(err))
			return err
		}

		select {
		case e := <-dc:
			ev := e.(*kafka.Message)
			if ev.TopicPartition.Error != nil {
				log.Warn("fail to deliver msg", zap.Error(ev.TopicPartition.Error))
				return ev.TopicPartition.Error
			}
		case <-time.After(time.Second):
			log.Warn("deliver msg timeout", zap.Error(errors.New("timeout")))
		}
		return nil
	})
}

func (k *KafkaDataHandler) Delete(ctx context.Context, param *api.DeleteParam, f api.DataFormatter) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		val, err := f.Format(param)
		if err != nil {
			log.Warn("fail to format data", zap.Error(err))
		}
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &k.topic,
				Partition: kafka.PartitionAny,
			},
			Value: val,
		}
		err = p.Produce(msg, dc)
		if err != nil {
			log.Warn("fail to produce msg", zap.Error(err))
			return err
		}

		select {
		case e := <-dc:
			ev := e.(*kafka.Message)
			if ev.TopicPartition.Error != nil {
				log.Warn("fail to deliver msg", zap.Error(ev.TopicPartition.Error))
				return ev.TopicPartition.Error
			}
		case <-time.After(time.Second):
			log.Warn("deliver msg timeout", zap.Error(errors.New("timeout")))
		}
		return nil
	})
}

func (k *KafkaDataHandler) CreateIndex(ctx context.Context, param *api.CreateIndexParam) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		val := fmt.Sprintf("create index %v in collection %v field %v", param.IndexName, param.CollectionName, param.FieldName)
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &k.topic,
				Partition: kafka.PartitionAny,
			},
			Value: []byte(val),
		}
		err := p.Produce(msg, dc)
		if err != nil {
			log.Warn("fail to produce msg", zap.Error(err))
			return err
		}

		select {
		case e := <-dc:
			ev := e.(*kafka.Message)
			if ev.TopicPartition.Error != nil {
				log.Warn("fail to deliver msg", zap.Error(ev.TopicPartition.Error))
				return ev.TopicPartition.Error
			}
		case <-time.After(time.Second):
			log.Warn("deliver msg timeout", zap.Error(errors.New("timeout")))
		}
		return nil
	})
}

func (k *KafkaDataHandler) DropIndex(ctx context.Context, param *api.DropIndexParam) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		val := fmt.Sprintf("drop index %v in collection %v field %v", param.IndexName, param.CollectionName, param.FieldName)
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &k.topic,
				Partition: kafka.PartitionAny,
			},
			Value: []byte(val),
		}
		err := p.Produce(msg, dc)
		if err != nil {
			log.Warn("fail to produce msg", zap.Error(err))
			return err
		}

		select {
		case e := <-dc:
			ev := e.(*kafka.Message)
			if ev.TopicPartition.Error != nil {
				log.Warn("fail to deliver msg", zap.Error(ev.TopicPartition.Error))
				return ev.TopicPartition.Error
			}
		case <-time.After(time.Second):
			log.Warn("deliver msg timeout", zap.Error(errors.New("timeout")))
		}
		return nil
	})
}

func (k *KafkaDataHandler) LoadCollection(ctx context.Context, param *api.LoadCollectionParam) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		val := fmt.Sprintf("load collection %v", param.CollectionName)
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &k.topic,
				Partition: kafka.PartitionAny,
			},
			Value: []byte(val),
		}
		err := p.Produce(msg, dc)
		if err != nil {
			log.Warn("fail to produce msg", zap.Error(err))
			return err
		}

		select {
		case e := <-dc:
			ev := e.(*kafka.Message)
			if ev.TopicPartition.Error != nil {
				log.Warn("fail to deliver msg", zap.Error(ev.TopicPartition.Error))
				return ev.TopicPartition.Error
			}
		case <-time.After(time.Second):
			log.Warn("deliver msg timeout", zap.Error(errors.New("timeout")))
		}
		return nil
	})
}

func (k *KafkaDataHandler) ReleaseCollection(ctx context.Context, param *api.ReleaseCollectionParam) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		val := fmt.Sprintf("release collection %v", param.CollectionName)
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &k.topic,
				Partition: kafka.PartitionAny,
			},
			Value: []byte(val),
		}
		err := p.Produce(msg, dc)
		if err != nil {
			log.Warn("fail to produce msg", zap.Error(err))
			return err
		}

		select {
		case e := <-dc:
			ev := e.(*kafka.Message)
			if ev.TopicPartition.Error != nil {
				log.Warn("fail to deliver msg", zap.Error(ev.TopicPartition.Error))
				return ev.TopicPartition.Error
			}
		case <-time.After(time.Second):
			log.Warn("deliver msg timeout", zap.Error(errors.New("timeout")))
		}
		return nil
	})
}

func (k *KafkaDataHandler) LoadPartitions(ctx context.Context, param *api.LoadPartitionsParam) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		val := fmt.Sprintf("load partitions %v in collection %v", param.PartitionNames, param.CollectionName)
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &k.topic,
				Partition: kafka.PartitionAny,
			},
			Value: []byte(val),
		}
		err := p.Produce(msg, dc)
		if err != nil {
			log.Warn("fail to produce msg", zap.Error(err))
			return err
		}

		select {
		case e := <-dc:
			ev := e.(*kafka.Message)
			if ev.TopicPartition.Error != nil {
				log.Warn("fail to deliver msg", zap.Error(ev.TopicPartition.Error))
				return ev.TopicPartition.Error
			}
		case <-time.After(time.Second):
			log.Warn("deliver msg timeout", zap.Error(errors.New("timeout")))
		}
		return nil
	})
}

func (k *KafkaDataHandler) ReleasePartitions(ctx context.Context, param *api.ReleasePartitionsParam) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		val := fmt.Sprintf("release partitions %v in collection %v", param.PartitionNames, param.CollectionName)
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &k.topic,
				Partition: kafka.PartitionAny,
			},
			Value: []byte(val),
		}
		err := p.Produce(msg, dc)
		if err != nil {
			log.Warn("fail to produce msg", zap.Error(err))
			return err
		}

		select {
		case e := <-dc:
			ev := e.(*kafka.Message)
			if ev.TopicPartition.Error != nil {
				log.Warn("fail to deliver msg", zap.Error(ev.TopicPartition.Error))
				return ev.TopicPartition.Error
			}
		case <-time.After(time.Second):
			log.Warn("deliver msg timeout", zap.Error(errors.New("timeout")))
		}
		return nil
	})
}

func (k *KafkaDataHandler) Flush(ctx context.Context, param *api.FlushParam) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		val := fmt.Sprintf("flush collections %v", param.CollectionNames)
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &k.topic,
				Partition: kafka.PartitionAny,
			},
			Value: []byte(val),
		}
		err := p.Produce(msg, dc)
		if err != nil {
			log.Warn("fail to produce msg", zap.Error(err))
			return err
		}

		select {
		case e := <-dc:
			ev := e.(*kafka.Message)
			if ev.TopicPartition.Error != nil {
				log.Warn("fail to deliver msg", zap.Error(ev.TopicPartition.Error))
				return ev.TopicPartition.Error
			}
		case <-time.After(time.Second):
			log.Warn("deliver msg timeout", zap.Error(errors.New("timeout")))
		}
		return nil
	})
}
