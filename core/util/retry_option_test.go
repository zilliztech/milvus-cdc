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
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/util/retry"

	"github.com/zilliztech/milvus-cdc/core/config"
)

func TestRetry(t *testing.T) {
	assert.Len(t, GetRetryDefaultOptions(), 3)

	t.Run("default options", func(t *testing.T) {
		options := GetRetryDefaultOptions()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		i := 0
		err := retry.Do(ctx, func() error {
			i++
			return errors.New("retry error")
		}, options...)
		assert.Error(t, err)
		assert.Equal(t, 1, i)
	})

	t.Run("custome options", func(t *testing.T) {
		options := GetRetryOptions(config.RetrySettings{
			RetryTimes:  2,
			InitBackOff: 1,
			MaxBackOff:  1,
		})
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		i := 0
		err := retry.Do(ctx, func() error {
			i++
			return errors.New("retry error")
		}, options...)
		assert.Error(t, err)
		assert.Equal(t, 2, i)
	})

	t.Run("no retry", func(t *testing.T) {
		options := NoRetryOption()

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		i := 0
		err := retry.Do(ctx, func() error {
			i++
			return errors.New("retry error")
		}, options...)
		assert.Error(t, err)
		assert.Equal(t, 1, i)
	})
}
