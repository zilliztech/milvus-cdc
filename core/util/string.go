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
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unsafe"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/merr"

	"github.com/zilliztech/milvus-cdc/core/log"
)

var (
	DroppedDatabaseKey   = "database"
	DroppedCollectionKey = "collection"
	DroppedPartitionKey  = "partition"
)

type ctxTaskType struct{}

var CtxTaskKey = ctxTaskType{}

// ToBytes performs unholy acts to avoid allocations
func ToBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(&s))
}

// ToString like ToBytes
func ToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// ToPhysicalChannel get physical channel name from virtual channel name
func ToPhysicalChannel(vchannel string) string {
	index := strings.LastIndex(vchannel, "_")
	if index < 0 {
		return vchannel
	}
	return vchannel[:index]
}

func GetVChannel(pchannel, mark string) string {
	return fmt.Sprintf("%s_%sv0", pchannel, mark)
}

func Base64Encode(obj []byte) string {
	return base64.StdEncoding.EncodeToString(obj)
}

func Base64JSON(obj any) string {
	objByte, err := json.Marshal(obj)
	if err != nil {
		log.Warn("fail to marshal obj", zap.Any("obj", obj))
		return ""
	}
	return base64.StdEncoding.EncodeToString(objByte)
}

func Base64Msg(msg msgstream.TsMsg) string {
	msgByte, err := msg.Marshal(msg)
	if err != nil {
		log.Warn("fail to marshal msg", zap.Any("msg", msg))
		return ""
	}
	return base64.StdEncoding.EncodeToString(msgByte.([]byte))
}

func Base64ProtoObj(obj proto.Message) string {
	objByte, err := proto.Marshal(obj)
	if err != nil {
		log.Warn("fail to marshal obj", zap.Any("obj", obj))
		return ""
	}
	return base64.StdEncoding.EncodeToString(objByte)
}

func Base64MsgPosition(position *msgstream.MsgPosition) string {
	positionByte, err := proto.Marshal(position)
	if err != nil {
		log.Warn("fail to marshal position", zap.Any("position", position))
		return ""
	}
	return base64.StdEncoding.EncodeToString(positionByte)
}

func Base64DecodeMsgPosition(position string) (*msgstream.MsgPosition, error) {
	decodeBytes, err := base64.StdEncoding.DecodeString(position)
	if err != nil {
		log.Warn("fail to decode the position", zap.Error(err))
		return nil, err
	}
	msgPosition := &msgstream.MsgPosition{}
	err = proto.Unmarshal(decodeBytes, msgPosition)
	if err != nil {
		log.Warn("fail to unmarshal the position", zap.Error(err))
		return nil, err
	}
	return msgPosition, nil
}

func GetCreateInfoKey(key string) string {
	return fmt.Sprintf("%s_c", key)
}

func GetDropInfoKey(key string) string {
	return fmt.Sprintf("%s_d", key)
}

func GetCollectionInfoKeys(collectionName, dbName string) (string, string) {
	if dbName == "" {
		dbName = DefaultDbName
	}
	key := fmt.Sprintf("%s_%s", dbName, collectionName)
	return GetCreateInfoKey(key), GetDropInfoKey(key)
}

func GetPartitionInfoKeys(partitionName, collectionName, dbName string) (string, string) {
	if dbName == "" {
		dbName = DefaultDbName
	}
	key := fmt.Sprintf("%s_%s_%s", dbName, collectionName, partitionName)
	return GetCreateInfoKey(key), GetDropInfoKey(key)
}

func GetDBInfoKeys(dbName string) (string, string) {
	if dbName == "" {
		dbName = DefaultDbName
	}
	return GetCreateInfoKey(dbName), GetDropInfoKey(dbName)
}

type ChannelInfo struct {
	PChannelName string
	CollectionID int64
	ShardIndex   int
}

const (
	rgnPhysicalName = `PhysicalName`
	rgnCollectionID = `CollectionID`
	rgnShardIdx     = `ShardIdx`
)

var channelNameFormat = regexp.MustCompile(fmt.Sprintf(`^(?P<%s>.*)_(?P<%s>\d+)v(?P<%s>\d+)$`, rgnPhysicalName, rgnCollectionID, rgnShardIdx))

func ParseVChannel(virtualName string) (ChannelInfo, error) {
	if !channelNameFormat.MatchString(virtualName) {
		return ChannelInfo{}, merr.WrapErrParameterInvalidMsg("virtual channel name(%s) is not valid", virtualName)
	}
	matches := channelNameFormat.FindStringSubmatch(virtualName)

	physicalName := matches[channelNameFormat.SubexpIndex(rgnPhysicalName)]
	collectionIDRaw := matches[channelNameFormat.SubexpIndex(rgnCollectionID)]
	shardIdxRaw := matches[channelNameFormat.SubexpIndex(rgnShardIdx)]
	collectionID, err := strconv.ParseInt(collectionIDRaw, 10, 64)
	if err != nil {
		return ChannelInfo{}, err
	}
	shardIdx, err := strconv.ParseInt(shardIdxRaw, 10, 64)
	if err != nil {
		return ChannelInfo{}, err
	}
	return ChannelInfo{
		PChannelName: physicalName,
		CollectionID: collectionID,
		ShardIndex:   int(shardIdx),
	}, nil
}

func GetFullCollectionName(collectionName string, databaseName string) string {
	return fmt.Sprintf("%s.%s", databaseName, collectionName)
}

func GetCollectionNameFromFull(fullName string) (string, string) {
	names := strings.Split(fullName, ".")
	if len(names) != 2 {
		panic("invalid full collection name")
	}
	return names[0], names[1]
}

func GetCtxWithTaskID(ctx context.Context, taskID string) context.Context {
	return context.WithValue(ctx, CtxTaskKey, taskID)
}

func GetTaskIDFromCtx(ctx context.Context) string {
	taskID, ok := ctx.Value(CtxTaskKey).(string)
	if !ok {
		return ""
	}
	return taskID
}
