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

package model

import "github.com/zilliztech/milvus-cdc/core/util"

type ConnectParam struct {
	Milvus MilvusConnectParam `json:"milvus,omitempty" mapstructure:"milvus,omitempty"`
	Kafka  KafkaConnectParam  `json:"kafka,omitempty" mapstructure:"kafka,omitempty"`
}

type KafkaConnectParam struct {
	Address          string `json:"address" mapstructure:"address"`
	Topic            string `json:"topic" mapstructure:"topic"`
	EnableSASL       bool   `json:"enable_sasl" mapstructure:"enable_sasl"`
	SASLUsername     string `json:"sasl_username,omitempty" mapstructure:"sasl_username,omitempty"`
	SASLPassword     string `json:"sasl_password,omitempty" mapstructure:"sasl_password,omitempty"`
	SASLMechanisms   string `json:"sasl_mechanisms,omitempty" mapstructure:"sasl_mechanisms,omitempty"`
	SecurityProtocol string `json:"security_protocol,omitempty" mapstructure:"security_protocol,omitempty"`
}

//go:generate easytags $GOFILE json,mapstructure
type MilvusConnectParam struct {
	Host            string          `json:"host" mapstructure:"host"`
	Port            int             `json:"port" mapstructure:"port"`
	Username        string          `json:"username,omitempty" mapstructure:"username,omitempty"`
	Password        string          `json:"password,omitempty" mapstructure:"password,omitempty"`
	EnableTLS       bool            `json:"enable_tls" mapstructure:"enable_tls"`
	DialConfig      util.DialConfig `json:"dial_config" mapstructure:"dial_config"`
	IgnorePartition bool            `json:"ignore_partition" mapstructure:"ignore_partition"`
	// ConnectTimeout unit: s
	ConnectTimeout int `json:"connect_timeout" mapstructure:"connect_timeout"`
}

type CollectionInfo struct {
	Name      string            `json:"name" mapstructure:"name"`
	Positions map[string]string `json:"positions" mapstructure:"positions"`
}

type ChannelInfo struct {
	Name     string `json:"name" mapstructure:"name"`
	Position string `json:"position" mapstructure:"position"`
}

type BufferConfig struct {
	Period int `json:"period" mapstructure:"period"`
	Size   int `json:"size" mapstructure:"size"`
}

const (
	// TmpCollectionID which means it's the user custom collection position
	TmpCollectionID int64 = -1
	// TmpCollectionName TODO if replicate the rbac info, the collection id will be set it
	TmpCollectionName = "-1"

	ReplicateCollectionID   int64 = -10
	ReplicateCollectionName       = "-10"
)
