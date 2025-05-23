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
	"io/ioutil"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v2"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func GetConfig[T any](fileName string) T {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().ServiceParam.MQCfg.EnablePursuitMode.Key, "false")
	log.ReplaceGlobals(zap.NewNop(), &log.ZapProperties{
		Core:   zapcore.NewNopCore(),
		Syncer: zapcore.AddSync(ioutil.Discard),
		Level:  zap.NewAtomicLevel(),
	})

	fileContent, err := os.ReadFile("./configs/" + fileName)
	if err != nil {
		panic(err)
	}
	var positionConfig T
	err = yaml.Unmarshal(fileContent, &positionConfig)
	if err != nil {
		panic(err)
	}
	return positionConfig
}
