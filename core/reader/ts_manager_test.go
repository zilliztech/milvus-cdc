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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTSRef(t *testing.T) {
	m := GetTSManager()
	w := sync.WaitGroup{}
	for i := 0; i < 40; i++ {
		w.Add(1)
		go func(start int) {
			defer w.Done()
			if start%2 == 0 {
				for x := 0; x < 10000; x++ {
					m.AddRef("test")
				}
				return
			}
			for x := 0; x < 10000; x++ {
				m.RemoveRef("test")
			}
		}(i)
	}
	w.Wait()
	v, ok := m.channelRef.Load("test")
	assert.True(t, ok)
	assert.Equal(t, 0, v.Load())
}
