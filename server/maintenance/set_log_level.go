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

package maintenance

import (
	"fmt"

	"go.uber.org/zap/zapcore"

	"github.com/zilliztech/milvus-cdc/core/log"
	"github.com/zilliztech/milvus-cdc/server/model/request"
)

func SetLogLevel(req *request.MaintenanceRequest) (*request.MaintenanceResponse, error) {
	level, ok := req.Params["log_level"]
	if !ok {
		return &request.MaintenanceResponse{
			State: "log_level is required",
		}, nil
	}
	levelStr := fmt.Sprintf("%v", level)
	levelObj, err := zapcore.ParseLevel(levelStr)
	if err != nil {
		return &request.MaintenanceResponse{
			State: fmt.Sprintf("parse log level failed: %s", err.Error()),
		}, nil
	}
	log.SetLevel(levelObj)
	return &request.MaintenanceResponse{
		State: fmt.Sprintf("set log level to %s", levelStr),
	}, nil
}
