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

	"github.com/sasha-s/go-deadlock"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"sigs.k8s.io/yaml"

	pkglog "github.com/milvus-io/milvus/pkg/v2/log"

	"github.com/zilliztech/milvus-cdc/core/log"
	"github.com/zilliztech/milvus-cdc/core/util"
	"github.com/zilliztech/milvus-cdc/server"
	"github.com/zilliztech/milvus-cdc/server/tag"
)

func main() {
	pkglog.ReplaceGlobals(log.L(), log.Prop())
	util.InitMilvusPkgParam()
	tag.LogInfo()

	s := &server.CDCServer{}

	// parse config file
	fileContent, _ := os.ReadFile("./configs/cdc.yaml")
	var serverConfig server.CDCServerConfig
	err := yaml.Unmarshal(fileContent, &serverConfig)
	if err != nil {
		log.Panic("Failed to parse config file", zap.Error(err))
	}
	logLevel, err := zapcore.ParseLevel(serverConfig.LogLevel)
	if err != nil {
		log.Warn("Failed to parse log level, use the default log level [info]", zap.Error(err))
		logLevel = zap.InfoLevel
	}
	log.SetLevel(logLevel)
	deadlock.Opts.Disable = !serverConfig.DetectDeadLock
	s.Run(&serverConfig)
}
