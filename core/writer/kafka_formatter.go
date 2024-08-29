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
	"encoding/json"
	"errors"
	"fmt"

	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/util"
)

type KafkaDataFormatter struct {
	api.DataFormatter
}

type KafkaMsg struct {
	Data []KafkaFormat `json:"data"`
	Info string        `json:"info"`
}

type KafkaFormat struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	Value any    `json:"value"`
}

func NewKafkaFormatter() *KafkaDataFormatter {
	return &KafkaDataFormatter{}
}

func (k *KafkaDataFormatter) Format(data any) ([]byte, error) {
	var msg KafkaMsg
	var result []KafkaFormat
	var info string
	var err error
	switch data := data.(type) {
	case *api.InsertParam:
		for _, column := range data.Columns {
			id := column.FieldData().FieldId
			val, err := column.Get(int(id))
			if err != nil {
				return nil, err
			}
			kafkaFormat := KafkaFormat{
				Name:  column.Name(),
				Type:  column.Type().String(),
				Value: val,
			}
			result = append(result, kafkaFormat)
		}
		info = fmt.Sprintf("insert entity in collection %v", data.CollectionName)
	case *api.DeleteParam:
		column := data.Column
		id := column.FieldData().FieldId
		val, err := column.Get(int(id))
		if err != nil {
			return nil, err
		}
		info = fmt.Sprintf("delete entity in collection %v", data.CollectionName)
		kafkaFormat := KafkaFormat{
			Name:  column.Name(),
			Type:  column.Type().String(),
			Value: val,
		}
		result = append(result, kafkaFormat)

	case *api.AlterDatabaseParam:
		properties := util.ConvertKVPairToMap(data.Properties)
		for k, v := range properties {
			kafkaFormat := KafkaFormat{
				Name:  k,
				Type:  "string",
				Value: v,
			}
			result = append(result, kafkaFormat)
		}
		info = fmt.Sprintf("alter database %v with properties", data.DbName)

	case *api.AlterIndexParam:
		indexParams := util.ConvertKVPairToMap(data.GetExtraParams())
		for k, v := range indexParams {
			kafkaFormat := KafkaFormat{
				Name:  k,
				Type:  "string",
				Value: v,
			}
			result = append(result, kafkaFormat)
		}
		info = fmt.Sprintf("alter index %v with indexParams", data.IndexName)

	default:
		return nil, errors.New("the data format is not supported")
	}

	msg.Data = result
	msg.Info = info
	kafkaData, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return kafkaData, err
}
