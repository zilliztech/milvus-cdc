package api

import (
	"context"

	"github.com/zilliztech/milvus-cdc/core/log"
)

type Reader interface {
	StartRead(ctx context.Context)
	QuitRead(ctx context.Context)
	ErrorChan() <-chan error
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

func (d *DefaultReader) ErrorChan() <-chan error {
	log.Warn("ErrorChan is not implemented, please check it")
	return nil
}
