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
	"fmt"
	"os"

	"go.uber.org/zap"
	"sigs.k8s.io/yaml"

	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/log"
	"github.com/zilliztech/milvus-cdc/server/tool"
)

var GlobalConfig StartCollectionConfig

type StartCollectionConfig struct {
	// deprecated
	EtcdAddress      string
	EtcdServerConfig config.EtcdServerConfig
	RootPath         string
	CollectionID     string
	DBID             string
}

func main() {
	fileContent, err := os.ReadFile("./configs/collection_start_position.yaml")
	if err != nil {
		log.Panic("Read config file failed", zap.Error(err))
	}

	var positionConfig StartCollectionConfig
	err = yaml.Unmarshal(fileContent, &positionConfig)
	if err != nil {
		log.Panic("Unmarshal config failed", zap.Error(err))
	}
	GlobalConfig = positionConfig

	etcdConfig := GlobalConfig.EtcdServerConfig
	if len(GlobalConfig.EtcdAddress) > 0 {
		etcdConfig = config.EtcdServerConfig{
			Address:  []string{GlobalConfig.EtcdAddress},
			RootPath: GlobalConfig.RootPath,
		}
	}
	positions, err := tool.GetCollectionStartPosition(etcdConfig, tool.CollectionIDInfo{
		DBID:         GlobalConfig.DBID,
		CollectionID: GlobalConfig.CollectionID,
	})
	if err != nil {
		log.Panic("Get collection start position failed", zap.Error(err))
	}
	fmt.Println(positions)
}
