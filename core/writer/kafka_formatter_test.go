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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"

	"github.com/zilliztech/milvus-cdc/core/api"
)

func TestFormat(t *testing.T) {
	kafkaFormatter := &KafkaDataFormatter{}
	t.Run("format common param", func(t *testing.T) {
		data := &api.CreateCollectionParam{
			Properties: []*commonpb.KeyValuePair{
				{
					Key:   "foo",
					Value: "hoo",
				},
			},
			ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
			MsgBaseParam: api.MsgBaseParam{
				Base: &commonpb.MsgBase{
					ReplicateInfo: &commonpb.ReplicateInfo{
						IsReplicate:  true,
						MsgTimestamp: 1000,
					},
				},
			},
			Schema: &entity.Schema{
				CollectionName: "foo",
				Fields: []*entity.Field{
					{
						Name:     "age",
						DataType: entity.FieldTypeInt8,
					},
					{
						Name:     "data",
						DataType: entity.FieldTypeBinaryVector,
					},
				},
			},
			ShardsNum: 1,
		}
		_, err := kafkaFormatter.Format(data)
		assert.NoError(t, err)
	})

	t.Run("format insert param", func(t *testing.T) {
		data := &api.InsertParam{
			CollectionName: "foo",
			Columns: []column.Column{
				column.NewColumnInt64("age", []int64{10}),
			},
		}
		_, err := kafkaFormatter.Format(data)
		assert.NoError(t, err)
	})

	t.Run("format delete param", func(t *testing.T) {
		data := &api.DeleteParam{
			CollectionName: "foo",
			Column:         column.NewColumnInt64("age", []int64{10}),
		}
		_, err := kafkaFormatter.Format(data)
		assert.NoError(t, err)
	})
}
