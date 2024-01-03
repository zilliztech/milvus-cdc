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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestValue(t *testing.T) {
	v := Value[int]{}
	v.Store(10)
	assert.Equal(t, 10, v.Load())
	inc := func() {
		go func() {
			for {
				old := v.Load()
				ok := v.CompareAndSwap(old, old+1)
				if ok {
					break
				}
			}
		}()
	}
	for i := 0; i < 10; i++ {
		inc()
	}
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 20, v.Load())
	v.CompareAndSwapWithFunc(func(old int) int {
		return old + 1
	})
}

func TestMap(t *testing.T) {
	m := Map[int, string]{}
	m.Store(1, "foo")
	v, ok := m.LoadOrStore(1, "hoo")
	assert.Equal(t, "foo", v)
	assert.True(t, ok)
	v, ok = m.LoadOrStore(2, "hoo")
	assert.Equal(t, "hoo", v)
	assert.False(t, ok)
	m.Store(3, "koo")
	v, ok = m.LoadAndDelete(3)
	assert.Equal(t, "koo", v)
	assert.True(t, ok)
	v, ok = m.LoadAndDelete(4)
	assert.Equal(t, "", v)
	assert.False(t, ok)
	m.Store(3, "koo")
	m.Delete(3)
	v, ok = m.Load(3)
	assert.Equal(t, "", v)
	assert.False(t, ok)
	v, ok = m.Load(1)
	assert.Equal(t, "foo", v)
	assert.True(t, ok)
	size := 0
	m.Range(func(key int, value string) bool {
		size++
		if key == 1 {
			assert.Equal(t, "foo", value)
		}
		if key == 2 {
			assert.Equal(t, "hoo", value)
		}
		return true
	})
	assert.Equal(t, 2, size)

	unsafeMap := m.GetUnsafeMap()
	assert.Len(t, unsafeMap, 2)
	assert.Equal(t, "foo", unsafeMap[1])
	assert.Equal(t, "hoo", unsafeMap[2])

	{
		v := m.LoadWithDefault(1, "bar")
		assert.Equal(t, "foo", v)
	}
	{
		v := m.LoadWithDefault(20, "bar")
		assert.Equal(t, "bar", v)
	}
}

func TestArray(t *testing.T) {
	a := SafeArray[int]{}
	a.Append(1)
	a.Append(2)
	assert.Equal(t, 1, a.Get(0))
	a.Range(func(index int, value int) bool {
		assert.Equal(t, index+1, value)
		return true
	})
}
