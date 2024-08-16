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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-cdc/core/config"
)

func TestConfigOption(t *testing.T) {
	handler := &MilvusDataHandler{}
	opts := []config.Option[*MilvusDataHandler]{
		AddressOption("localhost:50051"),
		UserOption("root", "123456"),
		TLSOption(true),
		ConnectTimeoutOption(5),
		IgnorePartitionOption(true),
	}

	for _, opt := range opts {
		opt.Apply(handler)
	}

	assert.Equal(t, "localhost:50051", handler.address)
	assert.Equal(t, "root", handler.username)
	assert.Equal(t, "123456", handler.password)
	assert.True(t, handler.enableTLS)
	assert.Equal(t, 5, handler.connectTimeout)
	assert.True(t, handler.ignorePartition)
}

func TestKafkaConfigOption(t *testing.T) {
	handler := &KafkaDataHandler{}
	opts := []config.Option[*KafkaDataHandler]{
		KafkaAddressOption("localhost:9092"),
		KafkaTopicOption("test"),
		KafkaSASLOption("root", "123456"),
		KafkaSASLMechanismsOption("PLAIN"),
		KafkaSecurityProtocolOption("SASL_SSL"),
		KafkaEnableSASLOption(true),
	}

	for _, opt := range opts {
		opt.Apply(handler)
	}

	assert.Equal(t, "localhost:9092", handler.address)
	assert.Equal(t, "test", handler.topic)
	assert.True(t, handler.enableSASL)
	assert.Equal(t, "root", handler.saslUsername)
	assert.Equal(t, "123456", handler.saslPassword)
	assert.Equal(t, "PLAIN", handler.saslMechanisms)
	assert.Equal(t, "SASL_SSL", handler.securityProtocol)
}
