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

package util

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"

	"github.com/zilliztech/milvus-cdc/core/log"
)

type MsgStreamFactory struct {
	innerFactory msgstream.Factory
	ttMsgStream  bool
}

func NewMsgStreamFactory(factory msgstream.Factory, ttMsgStream bool) msgstream.Factory {
	if factory == nil {
		log.Panic("the factory can't be nil")
	}
	return &MsgStreamFactory{
		innerFactory: factory,
		ttMsgStream:  ttMsgStream,
	}
}

func (m *MsgStreamFactory) NewMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	return m.innerFactory.NewMsgStream(ctx)
}

func (m *MsgStreamFactory) NewTtMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	if m.ttMsgStream {
		return m.innerFactory.NewTtMsgStream(ctx)
	}
	return m.innerFactory.NewMsgStream(ctx)
}

func (m *MsgStreamFactory) NewMsgStreamDisposer(ctx context.Context) func([]string, string) error {
	return m.innerFactory.NewMsgStreamDisposer(ctx)
}
