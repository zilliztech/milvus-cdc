package server

import (
	"encoding/json"
	"testing"

	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
)

func TestJson(t *testing.T) {
	b, err := json.Marshal(make(map[string]string))
	log.Info(string(b), zap.Error(err))
}
