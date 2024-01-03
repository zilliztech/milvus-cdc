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

package util

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/stretchr/testify/assert"
)

func TestStringAndByte(t *testing.T) {
	str := "hello"
	assert.Equal(t, []byte(str), ToBytes(str))
	assert.Equal(t, str, ToString(ToBytes(str)))
}

func TestToPhysicalChannel(t *testing.T) {
	assert.Equal(t, "abc", ToPhysicalChannel("abc_"))
	assert.Equal(t, "abc", ToPhysicalChannel("abc_123"))
	assert.Equal(t, "abc", ToPhysicalChannel("abc_defgsg"))
	assert.Equal(t, "abc__", ToPhysicalChannel("abc___defgsg"))
	assert.Equal(t, "abcdef", ToPhysicalChannel("abcdef"))
}

func TestBase64Encode(t *testing.T) {
	str := "foo"
	encodeStr := Base64Encode([]byte(str))
	assert.NotEmpty(t, encodeStr)
	decodeByte, err := base64.StdEncoding.DecodeString(encodeStr)
	assert.NoError(t, err)
	assert.Equal(t, str, string(decodeByte))
}

func TestBase64JSON(t *testing.T) {
	str := "foo"
	encodeStr := Base64JSON(str)
	assert.NotEmpty(t, encodeStr)
	decodeByte, err := base64.StdEncoding.DecodeString(encodeStr)
	assert.NoError(t, err)
	var s string
	err = json.Unmarshal(decodeByte, &s)
	assert.NoError(t, err)
	assert.Equal(t, str, s)
}

func TestBase64TSMsg(t *testing.T) {
	tsMsg := &msgstream.InsertMsg{
		InsertRequest: msgpb.InsertRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_Insert,
				MsgID:   200,
				Properties: map[string]string{
					"hello": "world",
				},
				ReplicateInfo: &commonpb.ReplicateInfo{
					MsgTimestamp: 300,
					IsReplicate:  true,
				},
			},
			ShardName:  "sha",
			DbID:       400,
			Timestamps: []uint64{500},
			RowData: []*commonpb.Blob{
				{
					Value: []byte("blob"),
				},
			},
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Bool,
					FieldName: "pk",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_BoolData{
								BoolData: &schemapb.BoolArray{
									Data: []bool{true},
								},
							},
						},
					},
					FieldId:   600,
					IsDynamic: true,
				},
			},
		},
	}
	str := Base64Msg(tsMsg)

	decodeByte, err := base64.StdEncoding.DecodeString(str)
	assert.NoError(t, err)
	decodeMsg := &msgstream.InsertMsg{}
	tmpMsg, err := decodeMsg.Unmarshal(decodeByte)
	decodeMsg = tmpMsg.(*msgstream.InsertMsg)
	assert.NoError(t, err)

	assert.Equal(t, tsMsg.InsertRequest.Base.MsgType, decodeMsg.InsertRequest.Base.MsgType)
	assert.Equal(t, tsMsg.InsertRequest.Base.MsgID, decodeMsg.InsertRequest.Base.MsgID)
	assert.Equal(t, "world", decodeMsg.InsertRequest.Base.Properties["hello"])
	assert.Equal(t, tsMsg.InsertRequest.Base.ReplicateInfo.MsgTimestamp, decodeMsg.InsertRequest.Base.ReplicateInfo.MsgTimestamp)
	assert.Equal(t, tsMsg.InsertRequest.Base.ReplicateInfo.IsReplicate, decodeMsg.InsertRequest.Base.ReplicateInfo.IsReplicate)
	assert.Equal(t, tsMsg.InsertRequest.ShardName, decodeMsg.InsertRequest.ShardName)
	assert.Equal(t, tsMsg.InsertRequest.DbID, decodeMsg.InsertRequest.DbID)
	assert.Equal(t, tsMsg.InsertRequest.Timestamps[0], decodeMsg.InsertRequest.Timestamps[0])
	assert.Equal(t, tsMsg.InsertRequest.RowData[0].Value, decodeMsg.InsertRequest.RowData[0].Value)
	assert.Equal(t, tsMsg.InsertRequest.FieldsData[0].Type, decodeMsg.InsertRequest.FieldsData[0].Type)
	assert.Equal(t, tsMsg.InsertRequest.FieldsData[0].FieldName, decodeMsg.InsertRequest.FieldsData[0].FieldName)
	assert.Equal(t, tsMsg.InsertRequest.FieldsData[0].FieldId, decodeMsg.InsertRequest.FieldsData[0].FieldId)
	assert.Equal(t, tsMsg.InsertRequest.FieldsData[0].IsDynamic, decodeMsg.InsertRequest.FieldsData[0].IsDynamic)
	assert.True(t, tsMsg.InsertRequest.FieldsData[0].GetScalars().GetBoolData().GetData()[0])
}

func TestBase64ProtoObj(t *testing.T) {
	createCollectionReq := &milvuspb.CreateCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_CreateCollection,
			MsgID:   200,
			Properties: map[string]string{
				"hello": "world",
			},
			ReplicateInfo: &commonpb.ReplicateInfo{
				MsgTimestamp: 300,
				IsReplicate:  true,
			},
		},
		DbName:           "db",
		CollectionName:   "collection",
		Schema:           []byte("schema"),
		ShardsNum:        3,
		ConsistencyLevel: commonpb.ConsistencyLevel_Session,
		Properties: []*commonpb.KeyValuePair{
			{
				Key:   "key",
				Value: "value",
			},
		},
	}
	str := Base64ProtoObj(createCollectionReq)

	decodeByte, err := base64.StdEncoding.DecodeString(str)
	assert.NoError(t, err)
	decodeReq := &milvuspb.CreateCollectionRequest{}
	err = proto.Unmarshal(decodeByte, decodeReq)
	assert.NoError(t, err)

	assert.Equal(t, createCollectionReq.Base.MsgType, decodeReq.Base.MsgType)
	assert.Equal(t, createCollectionReq.Base.MsgID, decodeReq.Base.MsgID)
	assert.Equal(t, "world", decodeReq.Base.Properties["hello"])
	assert.Equal(t, createCollectionReq.Base.ReplicateInfo.MsgTimestamp, decodeReq.Base.ReplicateInfo.MsgTimestamp)
	assert.Equal(t, createCollectionReq.Base.ReplicateInfo.IsReplicate, decodeReq.Base.ReplicateInfo.IsReplicate)
	assert.Equal(t, createCollectionReq.DbName, decodeReq.DbName)
	assert.Equal(t, createCollectionReq.CollectionName, decodeReq.CollectionName)
	assert.Equal(t, createCollectionReq.Schema, decodeReq.Schema)
	assert.Equal(t, createCollectionReq.ShardsNum, decodeReq.ShardsNum)
	assert.Equal(t, createCollectionReq.ConsistencyLevel, decodeReq.ConsistencyLevel)
	assert.Equal(t, createCollectionReq.Properties[0].Key, decodeReq.Properties[0].Key)
}

func TestBase64MsgPosition(t *testing.T) {
	position := &msgstream.MsgPosition{
		ChannelName: "channel",
		MsgID:       []byte("100"),
		Timestamp:   200,
		MsgGroup:    "hello",
	}

	str := Base64MsgPosition(position)
	decodeByte, err := base64.StdEncoding.DecodeString(str)
	assert.NoError(t, err)
	decodePos := &msgstream.MsgPosition{}
	err = proto.Unmarshal(decodeByte, decodePos)
	assert.NoError(t, err)

	assert.Equal(t, position.ChannelName, decodePos.ChannelName)
	assert.Equal(t, position.MsgID, decodePos.MsgID)
	assert.Equal(t, position.Timestamp, decodePos.Timestamp)
	assert.Equal(t, position.MsgGroup, decodePos.MsgGroup)
}

func TestChan(t *testing.T) {
	s := []string{"a", "b", "c"}
	sByte, err := json.Marshal(s)
	assert.NoError(t, err)
	var a []string
	err = json.Unmarshal(sByte, &a)
	assert.NoError(t, err)
}
