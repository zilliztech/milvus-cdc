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
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/log"
	"github.com/zilliztech/milvus-cdc/core/util"
)

func TestTSRef(t *testing.T) {
	m := GetTSManager()
	w := sync.WaitGroup{}
	for i := 0; i < 40; i++ {
		for x := 0; x < 10000; x++ {
			m.AddRef("test")
		}
		w.Add(1)
		go func(start int) {
			defer w.Done()
			if start%2 == 0 {
				for x := 0; x < 10000; x++ {
					m.AddRef("test")
				}
				for x := 0; x < 10000; x++ {
					m.RemoveRef("test")
				}
				return
			}
			for x := 0; x < 10000; x++ {
				m.RemoveRef("test")
			}
			for x := 0; x < 10000; x++ {
				m.AddRef("test")
			}
		}(i)
	}
	w.Wait()
	for x := 0; x < 40*10000; x++ {
		m.RemoveRef("test")
	}
	v, ok := m.channelRef.Load("test")
	assert.True(t, ok)
	assert.Equal(t, 0, v.Load())
}

func TestTS(t *testing.T) {
	m := &tsManager{
		retryOptions: util.GetRetryOptions(config.RetrySettings{
			RetryTimes:  5,
			InitBackOff: 1,
			MaxBackOff:  1,
		}),
		lastTS:  util.NewValue[uint64](0),
		rateLog: log.NewRateLog(1, log.L()),

		channelTS2: make(map[string]uint64),
	}

	m.AddRef("a")
	m.AddRef("b")
	m.CollectTS("a", math.MaxUint64)
	m.CollectTS("b", 1)

	go func() {
		time.Sleep(500 * time.Millisecond)
		m.CollectTS("a", 2)
	}()
	minTS, _ := m.GetMinTS("a")
	assert.EqualValues(t, 1, minTS)
	m.EmptyTS()
}
