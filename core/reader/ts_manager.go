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
	"context"
	"math"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/log"
	"github.com/zilliztech/milvus-cdc/core/util"
)

var (
	tsOnce      sync.Once
	tsInstance  *tsManager
	TSMetricVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "milvus",
			Subsystem: "cdc",
			Name:      "center_tt",
			Help:      "the center ts, unit: ms",
		}, []string{"channel_name"})
)

type tsManager struct {
	channelTS    util.Map[string, uint64]
	channelRef   util.Map[string, *util.Value[int]]
	retryOptions []retry.Option
	lastTS       *util.Value[uint64]
}

func GetTSManager() *tsManager {
	tsOnce.Do(func() {
		tsInstance = &tsManager{
			retryOptions: util.GetRetryOptions(config.GetCommonConfig().Retry),
			lastTS:       util.NewValue[uint64](0),
		}
	})
	return tsInstance
}

func (m *tsManager) AddRef(channelName string) {
	v, _ := m.channelRef.LoadOrStore(channelName, util.NewValue[int](0))
	v.CompareAndSwapWithFunc(func(old int) int {
		return old + 1
	})
}

func (m *tsManager) RemoveRef(channelName string) {
	v, ok := m.channelRef.Load(channelName)
	if !ok {
		log.Warn("remove ref failed, channel not exist", zap.String("channelName", channelName))
		return
	}
	v.CompareAndSwapWithFunc(func(old int) int {
		return old - 1
	})
}

func (m *tsManager) CollectTS(channelName string, currentTS uint64) {
	m.channelTS.Store(channelName, currentTS)
}

func (m *tsManager) getUnsafeTSInfo() (map[string]uint64, map[string]int) {
	a := m.channelTS.GetUnsafeMap()
	b := m.channelRef.GetUnsafeMap()
	c := make(map[string]int, len(b))
	for k, v := range b {
		c[k] = v.Load()
	}
	return a, c
}

func (m *tsManager) GetMinTS(channelName string) uint64 {
	minTS := m.channelTS.LoadWithDefault(channelName, math.MaxUint64)

	err := retry.Do(context.Background(), func() error {
		m.channelTS.Range(func(k string, v uint64) bool {
			if v == math.MaxUint64 {
				minTS = math.MaxUint64
				return false
			}
			if v < minTS && m.channelRef.LoadWithDefault(k, util.NewValue[int](0)).Load() > 0 {
				minTS = v
			}
			return true
		})
		if minTS == math.MaxUint64 {
			a, b := m.getUnsafeTSInfo()
			return errors.Newf("there is no collection ts, channelName: %s, channelTS: %v, channelRef: %v",
				channelName, a, b)
		}
		return nil
	}, m.retryOptions...)
	if err != nil {
		return 0
	}

	if m.lastTS.Load() > minTS {
		a, b := m.getUnsafeTSInfo()
		log.Info("last ts is larger than min ts", zap.Uint64("lastTS", m.lastTS.Load()), zap.Uint64("minTS", minTS),
			zap.String("channelName", channelName), zap.Any("channelTS", a), zap.Any("channelRef", b))
		minTS = m.lastTS.Load()
	}
	m.lastTS.CompareAndSwapWithFunc(func(old uint64) uint64 {
		if old <= minTS {
			return minTS
		}
		return old
	})
	msgTime, _ := tsoutil.ParseHybridTs(minTS)
	TSMetricVec.WithLabelValues(channelName).Set(float64(msgTime))
	return minTS
}

// EmptyTS Only for test
func (m *tsManager) EmptyTS() {
	for k := range m.channelTS.GetUnsafeMap() {
		m.channelTS.Delete(k)
	}
	for k := range m.channelRef.GetUnsafeMap() {
		m.channelRef.Delete(k)
	}
	m.retryOptions = util.NoRetryOption()
}
