package msgpacker

import (
	"sync"
	"time"

	"github.com/zilliztech/milvus-cdc/core/api"
)

type PackerChecker interface {
	Check(msg *api.ReplicateMsg) bool
	Reset()
}

type TimerChecker struct {
	interval time.Duration
	lastTime time.Time
}

func NewTimerChecker(interval time.Duration) *TimerChecker {
	return &TimerChecker{
		interval: interval,
		lastTime: time.Now(),
	}
}

func (t *TimerChecker) Check(msg *api.ReplicateMsg) bool {
	if time.Since(t.lastTime) > t.interval {
		t.lastTime = time.Now()
		return true
	}
	return false
}

func (t *TimerChecker) Reset() {
	t.lastTime = time.Now()
}

type MsgCountChecker struct {
	count    int
	maxCount int
}

func NewMsgCountChecker(maxCount int) *MsgCountChecker {
	return &MsgCountChecker{
		maxCount: maxCount,
	}
}

func (m *MsgCountChecker) Check(msg *api.ReplicateMsg) bool {
	m.count++
	if m.count >= m.maxCount {
		m.count = 0
		return true
	}
	return false
}

func (m *MsgCountChecker) Reset() {
	m.count = 0
}

// global object
type MemoryProtector struct {
	lock    sync.RWMutex
	max     int
	current int
}

var memoryCheck = &MemoryProtector{}

func GetMemoryChecker(max int) *MemoryProtector {
	memoryCheck.lock.RLock()
	if memoryCheck.max != 0 {
		memoryCheck.lock.RUnlock()
		return memoryCheck
	}
	memoryCheck.lock.RUnlock()
	memoryCheck.lock.Lock()
	defer memoryCheck.lock.Unlock()
	if memoryCheck.max != 0 {
		return memoryCheck
	}
	memoryCheck.max = max
	return memoryCheck
}

func (m *MemoryProtector) Add(msgSize int) bool {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.current += msgSize
	return m.current > m.max
}

func (m *MemoryProtector) Remove(msgSize int) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.current -= msgSize
}
