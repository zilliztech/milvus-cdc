package writer

import (
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/mocks"
)

func TestMessageManager(t *testing.T) {
	handler := mocks.NewDataHandler(t)
	manager := NewReplicateMessageManager(handler, 10)

	// success
	{
		handler.EXPECT().ReplicateMessage(mock.Anything, mock.Anything).Return(nil).Once()
		done := make(chan error, 1)
		manager.ReplicateMessage(&api.ReplicateMessage{
			Param: &api.ReplicateMessageParam{
				ChannelName: "test",
			},
			SuccessFunc: func(param *api.ReplicateMessageParam) {
				done <- nil
			},
			FailFunc: func(param *api.ReplicateMessageParam, err error) {
				done <- err
			},
		})

		select {
		case err := <-done:
			assert.NoError(t, err)
		case <-time.Tick(time.Second):
			t.Fail()
		}
	}

	// fail
	{
		handler.EXPECT().ReplicateMessage(mock.Anything, mock.Anything).Return(errors.New("fail")).Once()
		done := make(chan error, 1)
		manager.ReplicateMessage(&api.ReplicateMessage{
			Param: &api.ReplicateMessageParam{
				ChannelName: "test",
			},
			SuccessFunc: func(param *api.ReplicateMessageParam) {
				done <- nil
			},
			FailFunc: func(param *api.ReplicateMessageParam, err error) {
				done <- err
			},
		})
		select {
		case err := <-done:
			assert.Error(t, err)
		case <-time.Tick(time.Second):
			t.Fail()
		}
	}

	// close
	{
		manager.Close("test")
		done := make(chan error, 1)
		manager.ReplicateMessage(&api.ReplicateMessage{
			Param: &api.ReplicateMessageParam{
				ChannelName: "test",
			},
			SuccessFunc: func(param *api.ReplicateMessageParam) {
				done <- nil
			},
			FailFunc: func(param *api.ReplicateMessageParam, err error) {
				done <- err
			},
		})
		select {
		case err := <-done:
			assert.Error(t, err)
		case <-time.Tick(time.Second):
			t.Fail()
		}
	}
}
