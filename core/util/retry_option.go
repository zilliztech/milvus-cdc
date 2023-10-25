package util

import (
	"time"

	"github.com/milvus-io/milvus/pkg/util/retry"
)

// GetRetryOptionsFor30s 1/2/4/8/10
func GetRetryOptionsFor25s() []retry.Option {
	return []retry.Option{
		retry.Attempts(5),
		retry.Sleep(time.Second),
		retry.MaxSleepTime(10 * time.Second),
	}
}

// GetRetryOptionsFor9s 0.2/0.4/0.8/1.6/3/3
func GetRetryOptionsFor9s() []retry.Option {
	return []retry.Option{
		retry.Attempts(6),
	}
}
