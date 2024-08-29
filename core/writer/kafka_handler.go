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
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"

	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/log"
)

type KafkaDataHandler struct {
	api.DataHandler

	address           string
	topic             string
	enableSASL        bool
	connectionTimeout int
	producer          *kafka.Producer
	deliveryChan      chan kafka.Event
	sasl              KafkaSASLParam
}

type KafkaSASLParam struct {
	username         string
	password         string
	mechanisms       string
	securityProtocol string
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
		conf["sasl.username"] = handler.sasl.username
		conf["sasl.password"] = handler.sasl.password
		conf["sasl.mechanisms"] = handler.sasl.mechanisms
		conf["security.protocol"] = handler.sasl.securityProtocol
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
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		val := fmt.Sprintf("create collection %v", param.Schema.CollectionName)
		return k.sendMessage(p, []byte(val), dc)
	})
}

func (k *KafkaDataHandler) DropCollection(ctx context.Context, param *api.DropCollectionParam) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		val := fmt.Sprintf("drop collection %v", param.CollectionName)
		return k.sendMessage(p, []byte(val), dc)
	})
}

func (k *KafkaDataHandler) CreatePartition(ctx context.Context, param *api.CreatePartitionParam) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		val := fmt.Sprintf("create partition %v on collection %v", param.PartitionName, param.CollectionName)
		return k.sendMessage(p, []byte(val), dc)
	})
}

func (k *KafkaDataHandler) DropPartition(ctx context.Context, param *api.DropPartitionParam) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		val := fmt.Sprintf("drop partition %v on collection %v", param.PartitionName, param.CollectionName)
		return k.sendMessage(p, []byte(val), dc)
	})
}

func (k *KafkaDataHandler) CreateDatabase(ctx context.Context, param *api.CreateDatabaseParam) error {
	return k.KafkaOp(ctx, "", func(p *kafka.Producer, dc chan kafka.Event) error {
		val := fmt.Sprintf("create database %v", param.DbName)
		return k.sendMessage(p, []byte(val), dc)
	})
}

func (k *KafkaDataHandler) DropDatabase(ctx context.Context, param *api.DropDatabaseParam) error {
	return k.KafkaOp(ctx, "", func(p *kafka.Producer, dc chan kafka.Event) error {
		val := fmt.Sprintf("drop database %v", param.DbName)
		return k.sendMessage(p, []byte(val), dc)
	})
}

func (k *KafkaDataHandler) AlterDatabase(ctx context.Context, param *api.AlterDatabaseParam, f api.DataFormatter) error {
	return k.KafkaOp(ctx, "", func(p *kafka.Producer, dc chan kafka.Event) error {
		val, err := f.Format(param)
		if err != nil {
			log.Warn("fail to format data", zap.Error(err))
		}
		return k.sendMessage(p, val, dc)
	})
}

func (k *KafkaDataHandler) Insert(ctx context.Context, param *api.InsertParam, f api.DataFormatter) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		val, err := f.Format(param)
		if err != nil {
			log.Warn("fail to format data", zap.Error(err))
		}
		return k.sendMessage(p, val, dc)
	})
}

func (k *KafkaDataHandler) Delete(ctx context.Context, param *api.DeleteParam, f api.DataFormatter) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		val, err := f.Format(param)
		if err != nil {
			log.Warn("fail to format data", zap.Error(err))
		}
		return k.sendMessage(p, val, dc)
	})
}

func (k *KafkaDataHandler) CreateIndex(ctx context.Context, param *api.CreateIndexParam) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		val := fmt.Sprintf("create index %v on collection %v field %v", param.IndexName, param.CollectionName, param.FieldName)
		return k.sendMessage(p, []byte(val), dc)
	})
}

func (k *KafkaDataHandler) DropIndex(ctx context.Context, param *api.DropIndexParam) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		val := fmt.Sprintf("drop index %v on collection %v field %v", param.IndexName, param.CollectionName, param.FieldName)
		return k.sendMessage(p, []byte(val), dc)
	})
}

func (k *KafkaDataHandler) AlterIndex(ctx context.Context, param *api.AlterIndexParam, f api.DataFormatter) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		val, err := f.Format(param)
		if err != nil {
			log.Warn("fail to format data", zap.Error(err))
		}
		return k.sendMessage(p, val, dc)
	})
}

func (k *KafkaDataHandler) LoadCollection(ctx context.Context, param *api.LoadCollectionParam) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		val := fmt.Sprintf("load collection %v", param.CollectionName)
		return k.sendMessage(p, []byte(val), dc)
	})
}

func (k *KafkaDataHandler) ReleaseCollection(ctx context.Context, param *api.ReleaseCollectionParam) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		val := fmt.Sprintf("release collection %v", param.CollectionName)
		return k.sendMessage(p, []byte(val), dc)
	})
}

func (k *KafkaDataHandler) LoadPartitions(ctx context.Context, param *api.LoadPartitionsParam) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		val := fmt.Sprintf("load partitions %v on collection %v", param.PartitionNames, param.CollectionName)
		return k.sendMessage(p, []byte(val), dc)
	})
}

func (k *KafkaDataHandler) ReleasePartitions(ctx context.Context, param *api.ReleasePartitionsParam) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		val := fmt.Sprintf("release partitions %v on collection %v", param.PartitionNames, param.CollectionName)
		return k.sendMessage(p, []byte(val), dc)
	})
}

func (k *KafkaDataHandler) Flush(ctx context.Context, param *api.FlushParam) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		val := fmt.Sprintf("flush collections %v", param.CollectionNames)
		return k.sendMessage(p, []byte(val), dc)
	})
}

// rbac messages
func (k *KafkaDataHandler) CreateUser(ctx context.Context, param *api.CreateUserParam) error {
	return k.KafkaOp(ctx, "", func(p *kafka.Producer, dc chan kafka.Event) error {
		val := fmt.Sprintf("create user %v", param.Username)
		return k.sendMessage(p, []byte(val), dc)
	})
}

func (k *KafkaDataHandler) DeleteUser(ctx context.Context, param *api.DeleteUserParam) error {
	return k.KafkaOp(ctx, "", func(p *kafka.Producer, dc chan kafka.Event) error {
		val := fmt.Sprintf("delete user %v", param.Username)
		return k.sendMessage(p, []byte(val), dc)
	})
}

func (k *KafkaDataHandler) UpdateUser(ctx context.Context, param *api.UpdateUserParam) error {
	return k.KafkaOp(ctx, "", func(p *kafka.Producer, dc chan kafka.Event) error {
		val := fmt.Sprintf("update user %v with password", param.Username)
		return k.sendMessage(p, []byte(val), dc)
	})
}

func (k *KafkaDataHandler) CreateRole(ctx context.Context, param *api.CreateRoleParam) error {
	return k.KafkaOp(ctx, "", func(p *kafka.Producer, dc chan kafka.Event) error {
		roleName := param.GetEntity().GetName()
		val := fmt.Sprintf("create role %v", roleName)
		return k.sendMessage(p, []byte(val), dc)
	})
}

func (k *KafkaDataHandler) DropRole(ctx context.Context, param *api.DropRoleParam) error {
	return k.KafkaOp(ctx, "", func(p *kafka.Producer, dc chan kafka.Event) error {
		val := fmt.Sprintf("drop role %v", param.GetRoleName())
		return k.sendMessage(p, []byte(val), dc)
	})
}

func (k *KafkaDataHandler) OperateUserRole(ctx context.Context, param *api.OperateUserRoleParam) error {
	return k.KafkaOp(ctx, "", func(p *kafka.Producer, dc chan kafka.Event) error {
		var val string
		switch param.Type {
		case milvuspb.OperateUserRoleType_AddUserToRole:
			val = fmt.Sprintf("add user %v to role %v", param.Username, param.RoleName)
		case milvuspb.OperateUserRoleType_RemoveUserFromRole:
			val = fmt.Sprintf("remove user %v from role %v", param.Username, param.RoleName)
		default:
			log.Warn("unknown operate user role type", zap.String("type", param.Type.String()))
			return nil
		}
		return k.sendMessage(p, []byte(val), dc)
	})
}

func (k *KafkaDataHandler) OperatePrivilege(ctx context.Context, param *api.OperatePrivilegeParam) error {
	return k.KafkaOp(ctx, "", func(p *kafka.Producer, d chan kafka.Event) error {
		var val string
		on := param.GetEntity().GetObjectName()
		o := param.GetEntity().GetObject().GetName()
		role := param.GetEntity().GetRole().GetName()
		priv := param.GetEntity().GetGrantor().GetPrivilege().GetName()
		ot, err := k.GetObjectType(o)
		if err != nil {
			return err
		}

		switch param.Type {
		case milvuspb.OperatePrivilegeType_Grant:
			val = fmt.Sprintf("grant %v to role %v with object %v typed %v", priv, role, on, ot)
		case milvuspb.OperatePrivilegeType_Revoke:
			val = fmt.Sprintf("revoke %v from role %v with object %v typed %v", priv, role, on, ot)
		default:
			log.Warn("unknown operate privilege type", zap.String("type", param.Type.String()))
			return nil
		}
		return k.sendMessage(p, []byte(val), d)
	})
}

func (k *KafkaDataHandler) GetObjectType(o string) (string, error) {
	objectType, ok := commonpb.ObjectType_value[o]
	if !ok {
		return "", errors.Newf("invalid object type, %s", o)
	}
	return commonpb.ObjectType_name[objectType], nil
}

func (k *KafkaDataHandler) sendMessage(p *kafka.Producer, val []byte, dc chan kafka.Event) error {
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &k.topic,
			Partition: kafka.PartitionAny,
		},
		Value: val,
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
}
