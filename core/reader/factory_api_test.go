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

package reader

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"

	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/util"
)

func TestDefaultFactoryCreator(t *testing.T) {
	c := NewDefaultFactoryCreator()
	t.Run("pulsar", func(t *testing.T) {
		{
			f := c.NewPmsFactory(&config.PulsarConfig{})
			pulsarFactory := f.(*msgstream.PmsFactory)
			assert.Equal(t, "{}", pulsarFactory.PulsarAuthParams)
		}

		{
			f := c.NewPmsFactory(&config.PulsarConfig{
				AuthParams: "a:b,c:d",
			})
			pulsarFactory := f.(*msgstream.PmsFactory)

			jsonMap := map[string]string{
				"a": "b",
				"c": "d",
			}
			jsonData, _ := json.Marshal(&jsonMap)
			authParams := util.ToString(jsonData)
			assert.Equal(t, authParams, pulsarFactory.PulsarAuthParams)
		}
	})

	t.Run("kafka", func(t *testing.T) {
		c.NewKmsFactory(&config.KafkaConfig{})
	})
}
