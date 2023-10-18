package api

import (
	"context"

	"github.com/milvus-io/milvus/pkg/log"
)

type Reader interface {
	StartRead(ctx context.Context)
	QuitRead(ctx context.Context)
}

// DefaultReader All CDCReader implements should combine it
type DefaultReader struct{}

var _ Reader = (*DefaultReader)(nil)

// StartRead the return value is nil,
// and if you receive the data from the nil chan, will block forever, not panic
func (d *DefaultReader) StartRead(ctx context.Context) {
	log.Warn("StartRead is not implemented, please check it")
}

func (d *DefaultReader) QuitRead(ctx context.Context) {
	log.Warn("QuitRead is not implemented, please check it")
}
