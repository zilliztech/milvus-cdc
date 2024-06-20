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
	"os"

	"go.uber.org/zap"
	"sigs.k8s.io/yaml"

	pkglog "github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"

	"github.com/zilliztech/milvus-cdc/core/log"
	"github.com/zilliztech/milvus-cdc/server"
	"github.com/zilliztech/milvus-cdc/server/tag"
)

func main() {
	pkglog.ReplaceGlobals(log.L(), log.Prop())
	baseTable := paramtable.NewBaseTable(
		paramtable.SkipRemote(true),
		paramtable.Interval(0),
	)
	paramtable.InitWithBaseTable(baseTable)
	tag.LogInfo()

	s := &server.CDCServer{}

	// parse config file
	fileContent, _ := os.ReadFile("./configs/cdc.yaml")
	var serverConfig server.CDCServerConfig
	err := yaml.Unmarshal(fileContent, &serverConfig)
	if err != nil {
		log.Panic("Failed to parse config file", zap.Error(err))
	}
	s.Run(&serverConfig)
}
