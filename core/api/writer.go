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

package api

import (
	"context"

	"github.com/milvus-io/milvus/pkg/mq/msgstream"

	"github.com/zilliztech/milvus-cdc/core/log"
)

type Writer interface {
	HandleReplicateAPIEvent(ctx context.Context, apiEvent *ReplicateAPIEvent) error
	HandleReplicateMessage(ctx context.Context, channelName string, msgPack *msgstream.MsgPack) ([]byte, []byte, error)
	HandleOpMessagePack(ctx context.Context, msgPack *msgstream.MsgPack) ([]byte, error)
}

type DefaultWriter struct{}

var _ Writer = (*DefaultWriter)(nil)

func (d *DefaultWriter) HandleReplicateAPIEvent(ctx context.Context, apiEvent *ReplicateAPIEvent) error {
	log.Warn("HandleReplicateAPIEvent is not implemented, please check it")
	return nil
}

func (d *DefaultWriter) HandleReplicateMessage(ctx context.Context, channelName string, msgPack *msgstream.MsgPack) ([]byte, []byte, error) {
	log.Warn("HandleReplicateMessage is not implemented, please check it")
	return nil, nil, nil
}

func (d *DefaultWriter) HandleOpMessagePack(ctx context.Context, msgPack *msgstream.MsgPack) ([]byte, error) {
	log.Warn("HandleOpMessagePack is not implemented, please check it")
	return nil, nil
}
