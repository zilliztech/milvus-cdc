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
	"encoding/json"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.uber.org/zap"

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

func (k *KafkaDataHandler) CreateCollection(ctx context.Context, param *api.CreateCollectionParam, f api.DataFormatter) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		data, err := f.Format(param)
		if err != nil {
			log.Warn("fail to format data", zap.Error(err))
		}
		return k.sendMessage(p, data, createCollectionInfo, dc)
	})
}

func (k *KafkaDataHandler) DropCollection(ctx context.Context, param *api.DropCollectionParam, f api.DataFormatter) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		data, err := f.Format(param)
		if err != nil {
			log.Warn("fail to format data", zap.Error(err))
		}
		return k.sendMessage(p, data, dropCollectionInfo, dc)
	})
}

func (k *KafkaDataHandler) CreatePartition(ctx context.Context, param *api.CreatePartitionParam, f api.DataFormatter) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		data, err := f.Format(param)
		if err != nil {
			log.Warn("fail to format data", zap.Error(err))
		}
		return k.sendMessage(p, data, createPartitionInfo, dc)
	})
}

func (k *KafkaDataHandler) DropPartition(ctx context.Context, param *api.DropPartitionParam, f api.DataFormatter) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		data, err := f.Format(param)
		if err != nil {
			log.Warn("fail to format data", zap.Error(err))
		}
		return k.sendMessage(p, data, dropPartitionInfo, dc)
	})
}

func (k *KafkaDataHandler) CreateDatabase(ctx context.Context, param *api.CreateDatabaseParam, f api.DataFormatter) error {
	return k.KafkaOp(ctx, "", func(p *kafka.Producer, dc chan kafka.Event) error {
		data, err := f.Format(param)
		if err != nil {
			log.Warn("fail to format data", zap.Error(err))
		}
		return k.sendMessage(p, data, createDatabaseInfo, dc)
	})
}

func (k *KafkaDataHandler) DropDatabase(ctx context.Context, param *api.DropDatabaseParam, f api.DataFormatter) error {
	return k.KafkaOp(ctx, "", func(p *kafka.Producer, dc chan kafka.Event) error {
		data, err := f.Format(param)
		if err != nil {
			log.Warn("fail to format data", zap.Error(err))
		}
		return k.sendMessage(p, data, dropDatabaseInfo, dc)
	})
}

func (k *KafkaDataHandler) AlterDatabase(ctx context.Context, param *api.AlterDatabaseParam, f api.DataFormatter) error {
	return k.KafkaOp(ctx, "", func(p *kafka.Producer, dc chan kafka.Event) error {
		data, err := f.Format(param)
		if err != nil {
			log.Warn("fail to format data", zap.Error(err))
		}
		return k.sendMessage(p, data, alterDatabaseInfo, dc)
	})
}

func (k *KafkaDataHandler) Insert(ctx context.Context, param *api.InsertParam, f api.DataFormatter) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		data, err := f.Format(param)
		if err != nil {
			log.Warn("fail to format data", zap.Error(err))
		}
		return k.sendMessage(p, data, insertDataInfo, dc)
	})
}

func (k *KafkaDataHandler) Delete(ctx context.Context, param *api.DeleteParam, f api.DataFormatter) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		data, err := f.Format(param)
		if err != nil {
			log.Warn("fail to format data", zap.Error(err))
		}
		return k.sendMessage(p, data, deleteDataInfo, dc)
	})
}

func (k *KafkaDataHandler) CreateIndex(ctx context.Context, param *api.CreateIndexParam, f api.DataFormatter) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		data, err := f.Format(param)
		if err != nil {
			log.Warn("fail to format data", zap.Error(err))
		}
		return k.sendMessage(p, data, createIndexInfo, dc)
	})
}

func (k *KafkaDataHandler) DropIndex(ctx context.Context, param *api.DropIndexParam, f api.DataFormatter) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		data, err := f.Format(param)
		if err != nil {
			log.Warn("fail to format data", zap.Error(err))
		}
		return k.sendMessage(p, data, dropIndexInfo, dc)
	})
}

func (k *KafkaDataHandler) AlterIndex(ctx context.Context, param *api.AlterIndexParam, f api.DataFormatter) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		data, err := f.Format(param)
		if err != nil {
			log.Warn("fail to format data", zap.Error(err))
		}
		return k.sendMessage(p, data, alterIndexInfo, dc)
	})
}

func (k *KafkaDataHandler) LoadCollection(ctx context.Context, param *api.LoadCollectionParam, f api.DataFormatter) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		data, err := f.Format(param)
		if err != nil {
			log.Warn("fail to format data", zap.Error(err))
		}
		return k.sendMessage(p, data, loadCollectionInfo, dc)
	})
}

func (k *KafkaDataHandler) ReleaseCollection(ctx context.Context, param *api.ReleaseCollectionParam, f api.DataFormatter) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		data, err := f.Format(param)
		if err != nil {
			log.Warn("fail to format data", zap.Error(err))
		}
		return k.sendMessage(p, data, releaseCollectionInfo, dc)
	})
}

func (k *KafkaDataHandler) LoadPartitions(ctx context.Context, param *api.LoadPartitionsParam, f api.DataFormatter) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		data, err := f.Format(param)
		if err != nil {
			log.Warn("fail to format data", zap.Error(err))
		}
		return k.sendMessage(p, data, loadPartitionsInfo, dc)
	})
}

func (k *KafkaDataHandler) ReleasePartitions(ctx context.Context, param *api.ReleasePartitionsParam, f api.DataFormatter) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		data, err := f.Format(param)
		if err != nil {
			log.Warn("fail to format data", zap.Error(err))
		}
		return k.sendMessage(p, data, releasePartitionsInfo, dc)
	})
}

func (k *KafkaDataHandler) Flush(ctx context.Context, param *api.FlushParam, f api.DataFormatter) error {
	return k.KafkaOp(ctx, param.Database, func(p *kafka.Producer, dc chan kafka.Event) error {
		data, err := f.Format(param)
		if err != nil {
			log.Warn("fail to format data", zap.Error(err))
		}
		return k.sendMessage(p, data, flushInfo, dc)
	})
}

func (k *KafkaDataHandler) ReplicateMessage(ctx context.Context, param *api.ReplicateMessageParam, f api.DataFormatter) error {
	return k.KafkaOp(ctx, "", func(p *kafka.Producer, dc chan kafka.Event) error {
		data, err := f.Format(param)
		if err != nil {
			return err
		}
		return k.sendMessage(p, data, replicateMessageInfo, dc)
	})
}

// rbac messages
func (k *KafkaDataHandler) CreateUser(ctx context.Context, param *api.CreateUserParam, f api.DataFormatter) error {
	return k.KafkaOp(ctx, "", func(p *kafka.Producer, dc chan kafka.Event) error {
		data, err := f.Format(param)
		if err != nil {
			log.Warn("fail to format data", zap.Error(err))
		}
		return k.sendMessage(p, data, createUserInfo, dc)
	})
}

func (k *KafkaDataHandler) DeleteUser(ctx context.Context, param *api.DeleteUserParam, f api.DataFormatter) error {
	return k.KafkaOp(ctx, "", func(p *kafka.Producer, dc chan kafka.Event) error {
		data, err := f.Format(param)
		if err != nil {
			log.Warn("fail to format data", zap.Error(err))
		}
		return k.sendMessage(p, data, deleteUserInfo, dc)
	})
}

func (k *KafkaDataHandler) UpdateUser(ctx context.Context, param *api.UpdateUserParam, f api.DataFormatter) error {
	return k.KafkaOp(ctx, "", func(p *kafka.Producer, dc chan kafka.Event) error {
		data, err := f.Format(param)
		if err != nil {
			log.Warn("fail to format data", zap.Error(err))
		}
		return k.sendMessage(p, data, updateUserInfo, dc)
	})
}

func (k *KafkaDataHandler) CreateRole(ctx context.Context, param *api.CreateRoleParam, f api.DataFormatter) error {
	return k.KafkaOp(ctx, "", func(p *kafka.Producer, dc chan kafka.Event) error {
		data, err := f.Format(param)
		if err != nil {
			log.Warn("fail to format data", zap.Error(err))
		}
		return k.sendMessage(p, data, createRoleInfo, dc)
	})
}

func (k *KafkaDataHandler) DropRole(ctx context.Context, param *api.DropRoleParam, f api.DataFormatter) error {
	return k.KafkaOp(ctx, "", func(p *kafka.Producer, dc chan kafka.Event) error {
		data, err := f.Format(param)
		if err != nil {
			log.Warn("fail to format data", zap.Error(err))
		}
		return k.sendMessage(p, data, dropRoleInfo, dc)
	})
}

func (k *KafkaDataHandler) OperateUserRole(ctx context.Context, param *api.OperateUserRoleParam, f api.DataFormatter) error {
	return k.KafkaOp(ctx, "", func(p *kafka.Producer, dc chan kafka.Event) error {
		var info msgType
		switch param.Type {
		case milvuspb.OperateUserRoleType_AddUserToRole:
			info = addUserToRoleInfo
		case milvuspb.OperateUserRoleType_RemoveUserFromRole:
			info = removeUserFromRoleInfo
		default:
			log.Warn("unknown operate user role type", zap.String("type", param.Type.String()))
			return nil
		}
		data, err := f.Format(param)
		if err != nil {
			log.Warn("fail to format data", zap.Error(err))
		}
		return k.sendMessage(p, data, info, dc)
	})
}

func (k *KafkaDataHandler) OperatePrivilege(ctx context.Context, param *api.OperatePrivilegeParam, f api.DataFormatter) error {
	return k.KafkaOp(ctx, "", func(p *kafka.Producer, dc chan kafka.Event) error {
		var info msgType
		switch param.Type {
		case milvuspb.OperatePrivilegeType_Grant:
			info = grantPrivilegeInfo
		case milvuspb.OperatePrivilegeType_Revoke:
			info = revokePrivilegeInfo
		default:
			log.Warn("unknown operate privilege type", zap.String("type", param.Type.String()))
			return nil
		}
		data, err := f.Format(param)
		if err != nil {
			log.Warn("fail to format data", zap.Error(err))
		}
		return k.sendMessage(p, data, info, dc)
	})
}

func (k *KafkaDataHandler) sendMessage(p *kafka.Producer, data []byte, info msgType, dc chan kafka.Event) error {
	kafkaMsg := KafkaMsg{
		Data: string(data),
		Info: info,
	}
	val, err := json.Marshal(kafkaMsg)
	if err != nil {
		log.Warn("fail to marshal msg", zap.Error(err))
		return err
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
}
