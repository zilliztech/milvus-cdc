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

	"github.com/zilliztech/milvus-cdc/core/api"
)

type KafkaDataFormatter struct {
	api.DataFormatter
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
	var res []byte
	var err error
	switch data := data.(type) {
	case *api.InsertParam:
		var kafkaFormatList []KafkaFormat
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
			kafkaFormatList = append(kafkaFormatList, kafkaFormat)
		}
		res, err = json.Marshal(kafkaFormatList)
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
		kafkaFormat := KafkaFormat{
			Name:  column.Name(),
			Type:  column.Type().String(),
			Value: val,
		}
		res, err = json.Marshal(kafkaFormat)
		if err != nil {
			return nil, err
		}
	default:
		res, err = json.Marshal(data)
		if err != nil {
			return nil, err
		}
	}

	return res, nil
}
