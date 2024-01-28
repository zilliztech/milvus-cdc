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

package model

import (
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/retry"

	"github.com/zilliztech/milvus-cdc/core/util"
)

type SourceCollectionInfo struct {
	PChannelName string
	CollectionID int64
	SeekPosition *msgstream.MsgPosition
	ShardNum     int
}

type TargetCollectionInfo struct {
	DatabaseName         string
	CollectionID         int64
	CollectionName       string
	PartitionInfo        map[string]int64
	PChannel             string
	VChannel             string
	BarrierChan          *util.OnceWriteChan[uint64]
	PartitionBarrierChan map[int64]*util.OnceWriteChan[uint64] // id is the source partition id
	Dropped              bool
	DroppedPartition     map[int64]struct{} // id is the source partition id
}

type HandlerOpts struct {
	MessageBufferSize int
	Factory           msgstream.Factory
	RetryOptions      []retry.Option
}

type CollectionInfo struct {
	DatabaseName   string
	CollectionID   int64
	CollectionName string
	VChannels      []string
	PChannels      []string
	Partitions     map[string]int64
	Dropped        bool
}

type DatabaseInfo struct {
	ID   int64
	Name string
}
