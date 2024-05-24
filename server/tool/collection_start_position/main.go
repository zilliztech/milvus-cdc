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

package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"

	"github.com/golang/protobuf/proto"
	clientv3 "go.etcd.io/etcd/client/v3"
	"sigs.k8s.io/yaml"

	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/pb"
	"github.com/zilliztech/milvus-cdc/core/util"
)

var GlobalConfig StartCollectionConfig

type StartCollectionConfig struct {
	// deprecated
	EtcdAddress      string
	EtcdServerConfig config.EtcdServerConfig
	RootPath         string
	CollectionID     string
}

func main() {
	fileContent, err := os.ReadFile("./configs/collection_start_position.yaml")
	if err != nil {
		panic(err)
	}

	var positionConfig StartCollectionConfig
	err = yaml.Unmarshal(fileContent, &positionConfig)
	if err != nil {
		panic(err)
	}
	GlobalConfig = positionConfig

	var etcdConfig clientv3.Config
	if len(GlobalConfig.EtcdAddress) > 0 {
		etcdConfig = clientv3.Config{
			Endpoints: []string{GlobalConfig.EtcdAddress},
		}
	} else {
		etcdConfig, err = util.GetEtcdConfig(GlobalConfig.EtcdServerConfig)
		if err != nil {
			panic(err)
		}
	}
	etcdClient, _ := clientv3.New(etcdConfig)
	getResp, err := etcdClient.Get(context.Background(),
		fmt.Sprintf("%s/meta/root-coord/database/collection-info/1/%s", GlobalConfig.RootPath, GlobalConfig.CollectionID))
	if err != nil {
		panic(err)
	}
	bytesData := getResp.Kvs[0].Value

	collectionInfo := &pb.CollectionInfo{}
	err = proto.Unmarshal(bytesData, collectionInfo)
	if err != nil {
		panic(err)
	}
	for _, position := range collectionInfo.StartPositions {
		msgPosition := &pb.MsgPosition{
			ChannelName: position.Key,
			MsgID:       position.Data,
		}
		positionData, err := proto.Marshal(msgPosition)
		if err != nil {
			panic(err)
		}
		base64Position := base64.StdEncoding.EncodeToString(positionData)
		fmt.Println("channelName: ", position.Key, " position: ", base64Position)
	}
}
