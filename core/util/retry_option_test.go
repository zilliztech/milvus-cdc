package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRetry(t *testing.T) {
	assert.Len(t, GetRetryOptionsFor25s(), 3)
	assert.Len(t, GetRetryOptionsFor9s(), 1)
}
