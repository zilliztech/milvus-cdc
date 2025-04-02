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
	"bytes"
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/util/requestutil"
)

var SuffixSnapshotTombstone = []byte{0xE2, 0x9B, 0xBC} // base64 value: "4pu8"

func IsTombstone(data []byte) bool {
	return bytes.Equal(data, SuffixSnapshotTombstone)
}

func GetCollectionNameFromMsgPack(msgPack *msgstream.MsgPack) string {
	if len(msgPack.Msgs) == 0 {
		return ""
	}
	firstMsg := msgPack.Msgs[0]
	collectionName, _ := requestutil.GetCollectionNameFromRequest(firstMsg)
	return collectionName.(string)
}

func GetDatabaseNameFromMsgPack(msgPack *msgstream.MsgPack) string {
	if len(msgPack.Msgs) == 0 {
		return ""
	}
	firstMsg := msgPack.Msgs[0]
	dbName, _ := requestutil.GetDbNameFromRequest(firstMsg)
	return dbName.(string)
}

func GetCollectionIDFromMsgPack(msgPack *msgstream.MsgPack) int64 {
	if len(msgPack.Msgs) == 0 {
		return 0
	}
	firstMsg := msgPack.Msgs[0]
	collectionID, _ := GetCollectionIDFromRequest(firstMsg)
	return collectionID
}

func IsUserRoleMessage(msgPack *msgstream.MsgPack) bool {
	if len(msgPack.Msgs) == 0 {
		return false
	}
	msgType := msgPack.Msgs[0].Type()
	return msgType == commonpb.MsgType_CreateCredential ||
		msgType == commonpb.MsgType_DeleteCredential ||
		msgType == commonpb.MsgType_UpdateCredential ||
		msgType == commonpb.MsgType_CreateRole ||
		msgType == commonpb.MsgType_DropRole ||
		msgType == commonpb.MsgType_OperateUserRole ||
		msgType == commonpb.MsgType_OperatePrivilege
}

func base64MsgPositions(positions []*msgstream.MsgPosition) []string {
	base64Positions := make([]string, len(positions))
	for i, position := range positions {
		base64Positions[i] = Base64MsgPosition(position)
	}
	return base64Positions
}

func msgDetails(msg *msgstream.MsgPack) []string {
	if len(msg.Msgs) == 0 {
		return []string{}
	}
	msgDetails := make([]string, len(msg.Msgs))
	for i, m := range msg.Msgs {
		msgDetail := make(map[string]any)
		msgDetail["msg_type"] = m.Type().String()
		msgDetail["msg_id"] = m.ID()
		switch m.Type() {
		case commonpb.MsgType_Insert:
			insertMsg := m.(*msgstream.InsertMsg)
			msgDetail["row_num"] = insertMsg.GetNumRows()
		case commonpb.MsgType_Delete:
			deleteMsg := m.(*msgstream.DeleteMsg)
			msgDetail["row_num"] = deleteMsg.GetNumRows()
		default:
		}
		msgDetails[i] = fmt.Sprintf("%v", msgDetail)
	}
	return msgDetails
}

func MsgPackInfoForLog(msgPack *msgstream.MsgPack) map[string]any {
	packInfo := make(map[string]any)
	packInfo["start_ts"] = msgPack.BeginTs
	packInfo["end_ts"] = msgPack.EndTs
	packInfo["msg_count"] = len(msgPack.Msgs)
	packInfo["start_positions"] = base64MsgPositions(msgPack.StartPositions)
	packInfo["end_positions"] = base64MsgPositions(msgPack.EndPositions)
	packInfo["msg_details"] = msgDetails(msgPack)
	return packInfo
}
