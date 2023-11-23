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
	"encoding/json"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/paramtable"

	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/util"
)

type FactoryCreator interface {
	NewPmsFactory(cfg *config.PulsarConfig) msgstream.Factory
	NewKmsFactory(cfg *config.KafkaConfig) msgstream.Factory
}

type DefaultFactoryCreator struct{}

func NewDefaultFactoryCreator() FactoryCreator {
	return &DefaultFactoryCreator{}
}

func (d *DefaultFactoryCreator) NewPmsFactory(cfg *config.PulsarConfig) msgstream.Factory {
	authParams := "{}"
	if cfg.AuthParams != "" {
		jsonMap := make(map[string]string)
		params := strings.Split(cfg.AuthParams, ",")
		for _, param := range params {
			kv := strings.Split(param, ":")
			if len(kv) == 2 {
				jsonMap[kv[0]] = kv[1]
			}
		}

		jsonData, _ := json.Marshal(&jsonMap)
		authParams = util.ToString(jsonData)
	}
	return msgstream.NewPmsFactory(
		&paramtable.ServiceParam{
			PulsarCfg: paramtable.PulsarConfig{
				Address:             config.NewParamItem(cfg.Address),
				WebAddress:          config.NewParamItem(cfg.WebAddress),
				WebPort:             config.NewParamItem(strconv.Itoa(cfg.WebPort)),
				MaxMessageSize:      config.NewParamItem(cfg.MaxMessageSize),
				AuthPlugin:          config.NewParamItem(cfg.AuthPlugin),
				AuthParams:          config.NewParamItem(authParams),
				Tenant:              config.NewParamItem(cfg.Tenant),
				Namespace:           config.NewParamItem(cfg.Namespace),
				RequestTimeout:      config.NewParamItem("60"),
				EnableClientMetrics: config.NewParamItem("false"),
			},
			MQCfg: paramtable.MQConfig{
				ReceiveBufSize: config.NewParamItem("16"),
				MQBufSize:      config.NewParamItem("16"),
			},
		},
	)
}

func (d *DefaultFactoryCreator) NewKmsFactory(cfg *config.KafkaConfig) msgstream.Factory {
	return msgstream.NewKmsFactory(
		&paramtable.ServiceParam{
			KafkaCfg: paramtable.KafkaConfig{
				Address:             config.NewParamItem(cfg.Address),
				SaslUsername:        config.NewParamItem(cfg.SaslUsername),
				SaslPassword:        config.NewParamItem(cfg.SaslPassword),
				SaslMechanisms:      config.NewParamItem(cfg.SaslMechanisms),
				SecurityProtocol:    config.NewParamItem(cfg.SaslMechanisms),
				ConsumerExtraConfig: config.NewParamGroup(cfg.Consumer),
				ProducerExtraConfig: config.NewParamGroup(cfg.Producer),
				ReadTimeout:         config.NewParamItem("10"),
			},
			MQCfg: paramtable.MQConfig{
				ReceiveBufSize: config.NewParamItem("16"),
				MQBufSize:      config.NewParamItem("16"),
			},
		},
	)
}
