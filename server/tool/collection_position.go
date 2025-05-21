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

package tool

import (
	"context"
	"encoding/base64"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"

	pb "github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/log"
	"github.com/zilliztech/milvus-cdc/core/util"
)

type PositionInfo struct {
	// the MsgID bytes encode in the msgpb.MsgPosition
	MQPosition string
	// the proto.Marshal bytes encode of msgpb.MsgPosition
	MilvusPosition string
}

type CollectionIDInfo struct {
	DBID         string
	CollectionID string
}

func GetCollectionStartPosition(config config.EtcdServerConfig, info CollectionIDInfo) (map[string]*PositionInfo, error) {
	etcdConfig, err := util.GetEtcdConfig(config)
	if err != nil {
		log.Warn("Get etcd config failed", zap.Error(err))
		return nil, nil
	}
	etcdClient, _ := clientv3.New(etcdConfig)
	getResp, err := etcdClient.Get(context.Background(),
		fmt.Sprintf("%s/meta/root-coord/database/collection-info/%s/%s", config.RootPath, info.DBID, info.CollectionID))
	if err != nil {
		panic(err)
	}
	bytesData := getResp.Kvs[0].Value

	collectionInfo := &pb.CollectionInfo{}
	err = proto.Unmarshal(bytesData, collectionInfo)
	if err != nil {
		panic(err)
	}
	positions := make(map[string]*PositionInfo)
	for _, position := range collectionInfo.StartPositions {
		msgPosition := &msgpb.MsgPosition{
			ChannelName: position.Key,
			MsgID:       position.Data,
		}
		positionData, err := proto.Marshal(msgPosition)
		if err != nil {
			panic(err)
		}
		base64Position := base64.StdEncoding.EncodeToString(positionData)
		requestPosition := base64.StdEncoding.EncodeToString(position.Data)
		positions[position.Key] = &PositionInfo{
			MQPosition:     requestPosition,
			MilvusPosition: base64Position,
		}
	}
	return positions, nil
}

func DecodeMilvusPosition(position string) (*msgpb.MsgPosition, error) {
	data, err := base64.StdEncoding.DecodeString(position)
	if err != nil {
		return nil, err
	}
	msgPosition := &msgpb.MsgPosition{}
	err = proto.Unmarshal(data, msgPosition)
	if err != nil {
		return nil, err
	}
	return msgPosition, nil
}

func GetMilvusPosition(pchannel, mqPosition string) (*msgpb.MsgPosition, error) {
	data, err := base64.StdEncoding.DecodeString(mqPosition)
	if err != nil {
		return nil, err
	}
	return &msgpb.MsgPosition{
		ChannelName: pchannel,
		MsgID:       data,
	}, nil
}
