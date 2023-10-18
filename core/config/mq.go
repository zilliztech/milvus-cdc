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

package config

import (
	"github.com/milvus-io/milvus/pkg/config"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

var configManager = config.NewManager()

func NewParamItem(value string) paramtable.ParamItem {
	item := paramtable.ParamItem{
		Formatter: func(_ string) string {
			return value
		},
	}
	item.Init(configManager)
	return item
}

func NewParamGroup() paramtable.ParamGroup {
	group := paramtable.ParamGroup{
		GetFunc: func() map[string]string {
			return map[string]string{}
		},
	}
	return group
}

type MQConfig struct {
	Pulsar PulsarConfig
	Kafka  KafkaConfig
}

type KafkaConfig struct {
	Address          string
	SaslUsername     string
	SaslPassword     string
	SaslMechanisms   string
	SecurityProtocol string
}

type PulsarConfig struct {
	Address        string
	Port           string
	WebAddress     string
	WebPort        int
	MaxMessageSize string

	// support tenant
	Tenant    string
	Namespace string

	AuthPlugin string
	AuthParams string
}
