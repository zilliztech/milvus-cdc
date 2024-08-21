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
		URIOption("localhost:50051"),
		TokenOption("root:123456"),
		ConnectTimeoutOption(5),
		IgnorePartitionOption(true),
	}

	for _, opt := range opts {
		opt.Apply(handler)
	}

	assert.Equal(t, "localhost:50051", handler.uri)
	assert.Equal(t, "root:123456", handler.token)
	assert.Equal(t, 5, handler.connectTimeout)
	assert.True(t, handler.ignorePartition)
}
