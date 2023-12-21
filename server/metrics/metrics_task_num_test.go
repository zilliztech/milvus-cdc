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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-cdc/server/model/meta"
)

func TestMetricTaskNum(t *testing.T) {
	m := &TaskNumMetric{
		initialTaskMap: make(map[string]struct{}),
		runningTaskMap: make(map[string]struct{}),
		pauseTaskMap:   make(map[string]struct{}),
	}
	m.Add("0", meta.TaskStateInitial)
	assertNum(t, m, 1, 0, 0)

	m.Add("1", meta.TaskStateRunning)
	assertNum(t, m, 1, 1, 0)

	m.UpdateState("1", meta.TaskStatePaused, meta.TaskStateRunning)
	assertNum(t, m, 1, 0, 1)

	m.UpdateState("0", meta.TaskStateRunning, meta.TaskStateInitial)
	assertNum(t, m, 0, 1, 1)

	m.Delete("1", meta.TaskStatePaused)
	assertNum(t, m, 0, 1, 0)

	m.UpdateState("2", meta.MinTaskState-1, meta.MaxTaskState+1)
	assertNum(t, m, 0, 1, 0)
}

func assertNum(t *testing.T, metric *TaskNumMetric, initialNum, runningNum, pauseNum int) {
	a, b, c := metric.getStateNum()
	assert.Equal(t, initialNum, int(a))
	assert.Equal(t, runningNum, int(b))
	assert.Equal(t, pauseNum, int(c))
}
