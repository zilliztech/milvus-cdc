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

package reader

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-cdc/core/util"
)

func TestNewBarrier(t *testing.T) {
	t.Run("close", func(t *testing.T) {
		b := NewBarrier(2, func(msgTs uint64, b *Barrier) {})
		close(b.CloseChan)
	})

	t.Run("success", func(t *testing.T) {
		var isExecuted util.Value[bool]
		isExecuted.Store(false)
		b := NewBarrier(2, func(msgTs uint64, b *Barrier) {
			assert.EqualValues(t, 2, msgTs)
			isExecuted.Store(true)
		})
		b.BarrierChan <- 2
		b.BarrierChan <- 2
		assert.Eventually(t, isExecuted.Load, time.Second, time.Millisecond*100)
	})
}
