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

	"github.com/zilliztech/milvus-cdc/server/model/request"
)

func Handle(req *request.MaintenanceRequest) (*request.MaintenanceResponse, error) {
	switch req.Operation {
	case "set_log_level":
		return SetLogLevel(req)
	case "set_force_log_msg":
		return ForceLogMsg(req)
	case "reset_log_msg":
		return ResetLogMsg(req)
	default:
		return &request.MaintenanceResponse{
			State: fmt.Sprintf("unsupported operation: %s", req.Operation),
		}, nil
	}
}
