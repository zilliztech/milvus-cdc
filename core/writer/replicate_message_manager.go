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
	"context"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/util"
)

// replicateMessageManager For the same channel, it is unsafe in concurrent situations
type replicateMessageManager struct {
	handler           api.DataHandler
	messageHandlerMap util.Map[string, *replicateMessageHandler]
	messageBufferSize int
}

func NewReplicateMessageManager(handler api.DataHandler, messageBufferSize int) api.MessageManager {
	manager := &replicateMessageManager{
		handler:           handler,
		messageBufferSize: messageBufferSize,
	}
	return manager
}

func (r *replicateMessageManager) ReplicateMessage(message *api.ReplicateMessage) {
	channelName := message.Param.ChannelName
	handler, _ := r.messageHandlerMap.LoadOrStore(channelName, newReplicateMessageHandler(channelName, r.messageBufferSize, r.handler))
	handler.handleMessage(message)
}

func (r *replicateMessageManager) Close(channelName string) {
	if handler, ok := r.messageHandlerMap.Load(channelName); ok {
		handler.close()
	}
}

type replicateMessageHandler struct {
	channelName string
	handler     api.DataHandler
	messageChan chan *api.ReplicateMessage
	stopOnce    sync.Once
	stopChan    chan struct{}
}

func (r *replicateMessageHandler) startHandleMessageLoop() {
	go func() {
		for {
			message := <-r.messageChan
			messageParam := message.Param
			if message.Ctx == nil {
				message.Ctx = context.Background()
			}
			err := r.handler.ReplicateMessage(message.Ctx, messageParam)
			if err != nil {
				message.FailFunc(message.Param, err)
			} else {
				message.SuccessFunc(message.Param)
			}
		}
	}()
}

func (r *replicateMessageHandler) handleMessage(message *api.ReplicateMessage) {
	select {
	case <-r.stopChan:
		message.FailFunc(message.Param, errors.New("replicate message handler is closed"))
	default:
		r.messageChan <- message
	}
}

func (r *replicateMessageHandler) close() {
	r.stopOnce.Do(func() {
		close(r.stopChan)
	})
}

func newReplicateMessageHandler(channelName string, messageBufferSize int, handler api.DataHandler) *replicateMessageHandler {
	paramChan := make(chan *api.ReplicateMessage, messageBufferSize)
	replicateHandler := &replicateMessageHandler{
		channelName: channelName,
		handler:     handler,
		messageChan: paramChan,
		stopChan:    make(chan struct{}),
	}
	replicateHandler.startHandleMessageLoop()
	return replicateHandler
}
