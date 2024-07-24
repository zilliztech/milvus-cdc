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

import "github.com/milvus-io/milvus/pkg/util/paramtable"

func InitMilvusPkgParam() {
	baseTable := paramtable.NewBaseTable(
		paramtable.SkipRemote(true),
		paramtable.Interval(0),
	)
	paramtable.InitWithBaseTable(baseTable)
	innerParam := paramtable.Get()
	_ = innerParam.Save(innerParam.MQCfg.MaxTolerantLag.Key, "5")
	_ = innerParam.Save(innerParam.MQCfg.MergeCheckInterval.Key, "2")
	_ = innerParam.Save(innerParam.MQCfg.TargetBufSize.Key, "4")
}
