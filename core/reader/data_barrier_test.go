package reader

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-cdc/core/util"
)

func TestNewBarrier(t *testing.T) {
	t.Run("close", func(t *testing.T) {
		b := NewBarrier(2, func(msgTs uint64, b *Barrier) {})
		close(b.CloseChan)
	})

	t.Run("success", func(t *testing.T) {
		var isExecuted util.Value[bool]
		isExecuted.Store(false)
		b := NewBarrier(2, func(msgTs uint64, b *Barrier) {
			assert.EqualValues(t, 2, msgTs)
			isExecuted.Store(true)
		})
		b.BarrierChan <- 2
		b.BarrierChan <- 2
		assert.Eventually(t, isExecuted.Load, time.Second, time.Millisecond*100)
	})
}
