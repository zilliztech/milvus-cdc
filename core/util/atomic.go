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
	"sync"
	"sync/atomic"
)

type Value[T any] struct {
	v atomic.Value
}

func NewValue[T any](initValue T) *Value[T] {
	v := &Value[T]{v: atomic.Value{}}
	v.Store(initValue)
	return v
}

func (value *Value[T]) Load() T {
	return value.v.Load().(T)
}

func (value *Value[T]) Store(t T) {
	value.v.Store(t)
}

func (value *Value[T]) CompareAndSwap(old T, new T) bool {
	return value.v.CompareAndSwap(old, new)
}

func (value *Value[T]) CompareAndSwapWithFunc(f func(old T) T) {
	for {
		old := value.Load()
		ok := value.CompareAndSwap(old, f(old))
		if ok {
			break
		}
	}
}

type Map[K comparable, V any] struct {
	m sync.Map
}

func (m *Map[K, V]) Load(key K) (value V, ok bool) {
	v, o := m.m.Load(key)
	if !o {
		return
	}
	return v.(V), o
}

func (m *Map[K, V]) Store(key K, value V) {
	m.m.Store(key, value)
}

func (m *Map[K, V]) LoadWithDefault(key K, value V) V {
	v, o := m.Load(key)
	if !o {
		return value
	}
	return v
}

func (m *Map[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	v, o := m.m.LoadOrStore(key, value)
	return v.(V), o
}

func (m *Map[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
	v, o := m.m.LoadAndDelete(key)
	if !o {
		return
	}
	return v.(V), o
}

func (m *Map[K, V]) Delete(key K) {
	m.m.Delete(key)
}

func (m *Map[K, V]) Range(f func(key K, value V) bool) {
	m.m.Range(func(key, value any) bool {
		return f(key.(K), value.(V))
	})
}

func (m *Map[K, V]) GetUnsafeMap() map[K]V {
	res := make(map[K]V)
	m.Range(func(key K, value V) bool {
		res[key] = value
		return true
	})
	return res
}

type SafeArray[T any] struct {
	sync.RWMutex
	arr []T
}

func (sa *SafeArray[T]) Append(n T) {
	sa.Lock()
	defer sa.Unlock()

	sa.arr = append(sa.arr, n)
}

func (sa *SafeArray[T]) Get(index int) T {
	sa.RLock()
	defer sa.RUnlock()

	return sa.arr[index]
}

func (sa *SafeArray[T]) Range(f func(index int, value T) bool) {
	sa.RLock()
	defer sa.RUnlock()

	for i, t := range sa.arr {
		if !f(i, t) {
			break
		}
	}
}
