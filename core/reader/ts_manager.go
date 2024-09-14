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
	"time"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/util/lock"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"

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

type tsInfo struct {
	cts    uint64        // current ts
	lts    uint64        // last send ts
	sts    time.Time     // the time of last send ts
	period time.Duration // send ts period
}

type tsManager struct {
	// deprecated
	channelTS util.Map[string, uint64]
	// deprecated
	channelRef util.Map[string, *util.Value[int]]
	// deprecated
	refLock sync.Mutex
	// deprecated
	lastTS *util.Value[uint64]
	// deprecated
	retryOptions []retry.Option
	// deprecated
	lastMsgTS util.Map[string, uint64]

	channelTS2     *typeutil.ConcurrentMap[string, *tsInfo]
	channelTSLocks *lock.KeyLock[string]
	rateLog        *log.RateLog
}

func GetTSManager() *tsManager {
	tsOnce.Do(func() {
		tsInstance = &tsManager{
			retryOptions:   util.GetRetryOptions(config.GetCommonConfig().Retry),
			lastTS:         util.NewValue[uint64](0),
			rateLog:        log.NewRateLog(1, log.L()),
			channelTS2:     typeutil.NewConcurrentMap[string, *tsInfo](),
			channelTSLocks: lock.NewKeyLock[string](),
		}
	})
	return tsInstance
}

// AddRef: not been used
func (m *tsManager) AddRef(channelName string) {
	v, ok := m.channelRef.Load(channelName)
	if !ok {
		m.refLock.Lock()
		_, ok = m.channelRef.Load(channelName)
		if !ok {
			log.Info("add ref, channel not exist", zap.String("channelName", channelName))
			m.channelRef.Store(channelName, util.NewValue[int](0))
		}
		m.refLock.Unlock()
		v, _ = m.channelRef.Load(channelName)
	}
	v.CompareAndSwapWithFunc(func(old int) int {
		return old + 1
	})
}

// RemoveRef not been used
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

// CollectTS channel name is the target physical channel name
func (m *tsManager) CollectTS(channelName string, currentTS uint64) {
	m.channelTS.Store(channelName, currentTS)

	m.channelTSLocks.Lock(channelName)
	defer m.channelTSLocks.Unlock(channelName)
	if currentTS == math.MaxUint64 {
		return
	}
	ts2, ok := m.channelTS2.Get(channelName)
	if !ok {
		m.channelTS2.Insert(channelName, &tsInfo{
			cts: currentTS,
		})
		return
	}
	if ts2.cts == 0 || ts2.cts < currentTS {
		ts2.cts = currentTS
	}
}

// getUnsafeTSInfo not been used
func (m *tsManager) getUnsafeTSInfo() (map[string]uint64, map[string]int) {
	a := m.channelTS.GetUnsafeMap()
	b := m.channelRef.GetUnsafeMap()
	c := make(map[string]int, len(b))
	for k, v := range b {
		c[k] = v.Load()
	}
	return a, c
}

// GetMinTS not been used
func (m *tsManager) GetMinTS(channelName string) (uint64, bool) {
	curChannelTS := m.channelTS.LoadWithDefault(channelName, math.MaxUint64)
	minTS := curChannelTS

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
		return 0, false
	}

	resetTS := false
	lastTS := m.lastMsgTS.LoadWithDefault(channelName, 0)
	if lastTS > minTS {
		a, b := m.getUnsafeTSInfo()
		m.rateLog.Info("last ts is larger than min ts", zap.Uint64("lastTS", lastTS), zap.Uint64("minTS", minTS),
			zap.String("channelName", channelName), zap.Any("channelTS", a), zap.Any("channelRef", b))
		// TODO how to handle the upsert case or insert / delete same ts
		minTS = lastTS
		resetTS = true
	}
	// m.lastTS.CompareAndSwapWithFunc(func(old uint64) uint64 {
	// 	if old <= minTS {
	// 		return minTS
	// 	}
	// 	return old
	// })
	msgTime, _ := tsoutil.ParseHybridTs(minTS)
	TSMetricVec.WithLabelValues(channelName).Set(float64(msgTime))

	channelTime, _ := tsoutil.ParseHybridTs(curChannelTS)
	diffTimeValue := channelTime - msgTime
	if diffTimeValue > 3*1000 { // diff 3 second
		a, b := m.getUnsafeTSInfo()
		m.rateLog.Info("there is a big diff between channel ts and min ts", zap.Uint64("minTS", minTS),
			zap.String("channelName", channelName), zap.Any("channelTS", a), zap.Any("channelRef", b))
	}
	return minTS, resetTS
}

func (m *tsManager) GetMaxTS(channelName string) (uint64, bool) {
	m.channelTSLocks.RLock(channelName)
	defer m.channelTSLocks.RUnlock(channelName)
	ts, ok := m.channelTS2.Get(channelName)
	if !ok {
		return 0, false
	}
	return ts.cts, true
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

func (m *tsManager) LockTargetChannel(channelName string) {
	m.channelTSLocks.Lock(channelName)
}

func (m *tsManager) UnLockTargetChannel(channelName string) {
	m.channelTSLocks.Unlock(channelName)
}

func (m *tsManager) InitTSInfo(channelName string, p time.Duration, c uint64) {
	m.channelTSLocks.Lock(channelName)
	defer m.channelTSLocks.Unlock(channelName)
	if c == math.MaxUint64 {
		c = 0
	}
	t := time.Now().Add(-p)
	ts, ok := m.channelTS2.Get(channelName)
	if !ok {
		m.channelTS2.Insert(channelName, &tsInfo{
			cts:    c,
			sts:    t,
			period: p,
		})
		return
	}
	if ts.sts.After(t) {
		ts.sts = t
	}
	if (ts.cts == 0 || ts.cts < c) && c != 0 {
		ts.cts = c
	}
	ts.period = p
}

// UnsafeShouldSendTSMsg should call the LockTargetChannel and UnLockTargetChannel before call this function
func (m *tsManager) UnsafeShouldSendTSMsg(channelName string) bool {
	ts, ok := m.channelTS2.Get(channelName)
	if !ok {
		return false
	}
	if ts.cts == 0 {
		return false
	}
	if time.Since(ts.sts) >= ts.period {
		return true
	}
	return false
}

func (m *tsManager) UnsafeUpdatePackTS(channelName string, beginTS uint64, updatePackTSFunc func(newTS uint64) (uint64, bool)) {
	ts, ok := m.channelTS2.Get(channelName)
	if !ok {
		return
	}
	if ts.lts < beginTS {
		return
	}

	maxTS := ts.lts
	if updateTS, ok := updatePackTSFunc(maxTS); ok {
		ts.cts = updateTS
	}
}

func (m *tsManager) UnsafeUpdateTSInfo(channelName string, sendTS uint64, resetLastTime bool) {
	ts, ok := m.channelTS2.Get(channelName)
	if !ok {
		return
	}
	ts.lts = sendTS
	ts.sts = time.Now()
	if resetLastTime {
		ts.sts = ts.sts.Add(-ts.period)
	}
}

func (m *tsManager) UnsafeGetMaxTS(channelName string) (uint64, bool) {
	ts, ok := m.channelTS2.Get(channelName)
	if !ok {
		return 0, false
	}
	return ts.cts, true
}
