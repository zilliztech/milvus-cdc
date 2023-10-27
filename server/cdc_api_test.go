package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefaultCDCServer(t *testing.T) {
	baseCDC := NewBaseCDC()
	baseCDC.ReloadTask()
	{
		_, err := baseCDC.Create(nil)
		assert.NoError(t, err)
	}
	{
		_, err := baseCDC.Delete(nil)
		assert.NoError(t, err)
	}
	{
		_, err := baseCDC.Pause(nil)
		assert.NoError(t, err)
	}
	{
		_, err := baseCDC.Resume(nil)
		assert.NoError(t, err)
	}
	{
		_, err := baseCDC.Get(nil)
		assert.NoError(t, err)
	}
	{
		_, err := baseCDC.GetPosition(nil)
		assert.NoError(t, err)
	}
	{
		_, err := baseCDC.List(nil)
		assert.NoError(t, err)
	}
}
