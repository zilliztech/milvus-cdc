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

//go:generate easytags $GOFILE json,mapstructure
type KafkaConnectParam struct {
	Address    string    `json:"address" mapstructure:"address"`
	Topic      string    `json:"topic" mapstructure:"topic"`
	EnableSASL bool      `json:"enable_sasl" mapstructure:"enable_sasl"`
	SASL       KafkaSASL `json:"sasl,omitempty" mapstructure:"sasl,omitempty"`
}

type KafkaSASL struct {
	Username         string `json:"username,omitempty" mapstructure:"username,omitempty"`
	Password         string `json:"password,omitempty" mapstructure:"password,omitempty"`
	Mechanisms       string `json:"mechanisms,omitempty" mapstructure:"mechanisms,omitempty"`
	SecurityProtocol string `json:"security_protocol,omitempty" mapstructure:"security_protocol,omitempty"`
}

//go:generate easytags $GOFILE json,mapstructure
type MilvusConnectParam struct {
	// Deprecated: use uri instead
	Host string `json:"host" mapstructure:"host"`
	// Deprecated: use uri instead
	Port int `json:"port" mapstructure:"port"`
	// Deprecated: use uri instead
	EnableTLS bool `json:"enable_tls" mapstructure:"enable_tls"`
	// Deprecated: use token instead
	Username string `json:"username,omitempty" mapstructure:"username,omitempty"`
	// Deprecated: use token instead
	Password        string          `json:"password,omitempty" mapstructure:"password,omitempty"`
	URI             string          `json:"uri" mapstructure:"uri"`
	Token           string          `json:"token,omitempty" mapstructure:"token,omitempty"`
	DialConfig      util.DialConfig `json:"dial_config,omitempty" mapstructure:"dial_config,omitempty"`
	ChannelNum      int             `json:"channel_num" mapstructure:"channel_num"`
	IgnorePartition bool            `json:"ignore_partition" mapstructure:"ignore_partition"`
	// ConnectTimeout unit: s
	ConnectTimeout int `json:"connect_timeout" mapstructure:"connect_timeout"`
}

type DatabaseInfo struct {
	Name string `json:"name" mapstructure:"name"`
}

type CollectionInfo struct {
	Name      string            `json:"name" mapstructure:"name"`
	Positions map[string]string `json:"positions" mapstructure:"positions"` // the key is the vchannel
}

type ChannelInfo struct {
	Name     string `json:"name" mapstructure:"name"`
	Position string `json:"position" mapstructure:"position"`
}

type ExtraInfo struct {
	EnableUserRole bool `json:"enable_user_role" mapstructure:"enable_user_role"`
}

type BufferConfig struct {
	Period int `json:"period" mapstructure:"period"`
	Size   int `json:"size" mapstructure:"size"`
}

type NameMapping struct {
	SourceDB          string            `json:"source_db" mapstructure:"source_db"`
	TargetDB          string            `json:"target_db" mapstructure:"target_db"`
	CollectionMapping map[string]string `json:"collection_mapping" mapstructure:"collection_mapping"`
}

const (
	// TmpCollectionID which means it's the user custom collection position
	TmpCollectionID int64 = -1
	// TmpCollectionName TODO if replicate the rbac info, the collection id will be set it
	TmpCollectionName = "-1"

	ReplicateCollectionID   int64 = -10
	ReplicateCollectionName       = "-10"
)
