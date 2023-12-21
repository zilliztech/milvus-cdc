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

package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-cdc/core/log"
	"github.com/zilliztech/milvus-cdc/server/model/meta"
)

type TaskNumMetric struct {
	metricDesc     *prometheus.Desc
	numLock        sync.RWMutex
	initialTaskMap map[string]struct{}
	runningTaskMap map[string]struct{}
	pauseTaskMap   map[string]struct{}
}

func (t *TaskNumMetric) getStateMap(state meta.TaskState) map[string]struct{} {
	switch state {
	case meta.TaskStateInitial:
		return t.initialTaskMap
	case meta.TaskStateRunning:
		return t.runningTaskMap
	case meta.TaskStatePaused:
		return t.pauseTaskMap
	default:
		return nil
	}
}

func (t *TaskNumMetric) UpdateState(taskID string, newState meta.TaskState, oldStates meta.TaskState) {
	t.numLock.Lock()
	defer t.numLock.Unlock()

	oldStatesMap := t.getStateMap(oldStates)
	newStatesMap := t.getStateMap(newState)
	if oldStatesMap == nil || newStatesMap == nil {
		log.Warn("update state, not handle the state", zap.String("task", taskID),
			zap.Any("old_state", oldStates), zap.Any("new_state", newState))
		return
	}

	if _, ok := oldStatesMap[taskID]; !ok {
		return
	}

	delete(oldStatesMap, taskID)
	newStatesMap[taskID] = struct{}{}
}

func (t *TaskNumMetric) Delete(taskID string, state meta.TaskState) {
	t.numLock.Lock()
	defer t.numLock.Unlock()

	stateMap := t.getStateMap(state)
	if stateMap == nil {
		return
	}
	delete(stateMap, taskID)
}

func (t *TaskNumMetric) Add(taskID string, state meta.TaskState) {
	t.numLock.Lock()
	defer t.numLock.Unlock()

	stateMap := t.getStateMap(state)
	if stateMap == nil {
		return
	}
	stateMap[taskID] = struct{}{}
}

// Describe it should be adapted if you modify the meta.MinTaskState or the meta.MaxTaskState
func (t *TaskNumMetric) Describe(descs chan<- *prometheus.Desc) {
	descs <- t.metricDesc
	descs <- t.metricDesc
	descs <- t.metricDesc
}

func (t *TaskNumMetric) getStateNum() (float64, float64, float64) {
	t.numLock.RLock()
	defer t.numLock.RUnlock()

	return float64(len(t.initialTaskMap)), float64(len(t.runningTaskMap)), float64(len(t.pauseTaskMap))
}

// Collect it should be adapted if you modify the meta.MinTaskState or the meta.MaxTaskState
func (t *TaskNumMetric) Collect(metrics chan<- prometheus.Metric) {
	initialNum, runningNum, pauseNum := t.getStateNum()
	metrics <- prometheus.MustNewConstMetric(t.metricDesc, prometheus.GaugeValue,
		initialNum, meta.TaskStateInitial.String())
	metrics <- prometheus.MustNewConstMetric(t.metricDesc, prometheus.GaugeValue,
		runningNum, meta.TaskStateRunning.String())
	metrics <- prometheus.MustNewConstMetric(t.metricDesc, prometheus.GaugeValue,
		pauseNum, meta.TaskStatePaused.String())
}
