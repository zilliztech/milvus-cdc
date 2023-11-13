package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-cdc/server/model/meta"
)

func TestMetricTaskNum(t *testing.T) {
	m := &TaskNumMetric{}
	m.Add(meta.TaskStateInitial)
	assertNum(t, m, 1, 0, 0)

	m.Add(meta.TaskStateRunning)
	assertNum(t, m, 1, 1, 0)

	m.UpdateState(meta.TaskStatePaused, meta.TaskStateRunning)
	assertNum(t, m, 1, 0, 1)

	m.UpdateState(meta.TaskStateRunning, meta.TaskStateInitial)
	assertNum(t, m, 0, 1, 1)

	m.Delete(meta.TaskStatePaused)
	assertNum(t, m, 0, 1, 0)

	m.UpdateState(meta.MinTaskState-1, meta.MaxTaskState+1)
	assertNum(t, m, 0, 1, 0)
}

func assertNum(t *testing.T, metric *TaskNumMetric, initialNum, runningNum, pauseNum int) {
	assert.Equal(t, initialNum, metric.initialNum)
	assert.Equal(t, runningNum, metric.runningNum)
	assert.Equal(t, pauseNum, metric.pauseNum)
}
