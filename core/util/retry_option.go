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

import (
	"time"

	"github.com/milvus-io/milvus/pkg/util/retry"

	"github.com/zilliztech/milvus-cdc/core/config"
)

// GetRetryDefaultOptions 2 days retry
func GetRetryDefaultOptions() []retry.Option {
	return []retry.Option{
		// about 2 days
		retry.Attempts(17280),
		retry.Sleep(time.Second),
		retry.MaxSleepTime(10 * time.Second),
	}
}

func GetRetryOptions(c config.RetrySettings) []retry.Option {
	if c.RetryTimes == 0 || c.InitBackOff == 0 || c.MaxBackOff == 0 {
		return GetRetryDefaultOptions()
	}
	return []retry.Option{
		// nolint
		retry.Attempts(uint(c.RetryTimes)),
		retry.Sleep(time.Duration(c.InitBackOff) * time.Second),
		retry.MaxSleepTime(time.Duration(c.MaxBackOff) * time.Second),
	}
}

func NoRetryOption() []retry.Option {
	return []retry.Option{
		retry.Attempts(1),
		retry.Sleep(time.Second),
		retry.MaxSleepTime(time.Second),
	}
}
