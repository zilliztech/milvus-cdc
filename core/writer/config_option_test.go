package writer

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-cdc/core/config"
)

func TestConfigOption(t *testing.T) {
	handler := &MilvusDataHandler{}
	opts := []config.Option[*MilvusDataHandler]{
		AddressOption("localhost:50051"),
		UserOption("root", "123456"),
		TLSOption(true),
		ConnectTimeoutOption(5),
		IgnorePartitionOption(true),
	}

	for _, opt := range opts {
		opt.Apply(handler)
	}

	assert.Equal(t, "localhost:50051", handler.address)
	assert.Equal(t, "root", handler.username)
	assert.Equal(t, "123456", handler.password)
	assert.True(t, handler.enableTLS)
	assert.Equal(t, 5, handler.connectTimeout)
	assert.True(t, handler.ignorePartition)
}
