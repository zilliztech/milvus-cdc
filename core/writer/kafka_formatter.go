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
)

type KafkaDataFormatter struct {
	api.DataFormatter
}

type KafkaFormat struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	Value any    `json:"value"`
	Msg   string `json:"msg"`
}

func NewKafkaFormatter() *KafkaDataFormatter {
	return &KafkaDataFormatter{}
}

// TODO format data when insert or delete
func (k *KafkaDataFormatter) Format(data any) ([]byte, error) {
	var kafkaData []byte
	var err error
	switch data := data.(type) {
	case *api.InsertParam:
		var result []KafkaFormat
		for _, column := range data.Columns {
			id := column.FieldData().FieldId
			val, err := column.Get(int(id))
			if err != nil {
				return nil, err
			}
			msg := fmt.Sprintf("insert entity in collection %v", data.CollectionName)
			kafkaFormat := KafkaFormat{
				Name:  column.Name(),
				Type:  column.Type().String(),
				Value: val,
				Msg:   msg,
			}
			result = append(result, kafkaFormat)
		}
		kafkaData, err = json.Marshal(result)
		if err != nil {
			return nil, err
		}
	case *api.DeleteParam:
		column := data.Column
		id := column.FieldData().FieldId
		val, err := column.Get(int(id))
		if err != nil {
			return nil, err
		}
		msg := fmt.Sprintf("delete entity in collection %v", data.CollectionName)
		kafkaFormat := KafkaFormat{
			Name:  column.Name(),
			Type:  column.Type().String(),
			Value: val,
			Msg:   msg,
		}
		kafkaData, err = json.Marshal(kafkaFormat)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("the data format is not supported")
	}
	return kafkaData, err
}
