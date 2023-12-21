/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * //
 *     http://www.apache.org/licenses/LICENSE-2.0
 * //
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
