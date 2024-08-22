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

package writer

import (
	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/util"
)

func TokenOption(token string) config.Option[*MilvusDataHandler] {
	return config.OptionFunc[*MilvusDataHandler](func(object *MilvusDataHandler) {
		object.token = token
	})
}

func URIOption(uri string) config.Option[*MilvusDataHandler] {
	return config.OptionFunc[*MilvusDataHandler](func(object *MilvusDataHandler) {
		object.uri = uri
	})
}

func ConnectTimeoutOption(timeout int) config.Option[*MilvusDataHandler] {
	return config.OptionFunc[*MilvusDataHandler](func(object *MilvusDataHandler) {
		if timeout > 0 {
			object.connectTimeout = timeout
		}
	})
}

func IgnorePartitionOption(ignore bool) config.Option[*MilvusDataHandler] {
	return config.OptionFunc[*MilvusDataHandler](func(object *MilvusDataHandler) {
		object.ignorePartition = ignore
	})
}

func DialConfigOption(dialConfig util.DialConfig) config.Option[*MilvusDataHandler] {
	return config.OptionFunc[*MilvusDataHandler](func(object *MilvusDataHandler) {
		object.dialConfig = dialConfig
	})
}
