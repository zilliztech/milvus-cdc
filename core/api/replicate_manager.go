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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"

	"github.com/zilliztech/milvus-cdc/core/log"
	"github.com/zilliztech/milvus-cdc/core/model"
	"github.com/zilliztech/milvus-cdc/core/pb"
)

// ChannelManager a target must promise a manager
type ChannelManager interface {
	SetCtx(ctx context.Context)
	AddDroppedCollection(ids []int64)
	AddDroppedPartition(ids []int64)

	StartReadCollection(ctx context.Context, info *pb.CollectionInfo, seekPositions []*msgpb.MsgPosition) error
	StopReadCollection(ctx context.Context, info *pb.CollectionInfo) error
	AddPartition(ctx context.Context, collectionInfo *pb.CollectionInfo, partitionInfo *pb.PartitionInfo) error

	GetChannelChan() <-chan string
	GetMsgChan(pChannel string) <-chan *ReplicateMsg
	GetEventChan() <-chan *ReplicateAPIEvent

	GetChannelLatestMsgID(ctx context.Context, channelName string) ([]byte, error)
}

type TargetAPI interface {
	GetCollectionInfo(ctx context.Context, collectionName, databaseName string) (*model.CollectionInfo, error)
	GetPartitionInfo(ctx context.Context, collectionName, databaseName string) (*model.CollectionInfo, error)
	GetDatabaseName(ctx context.Context, collectionName, databaseName string) (string, error)
}

type ReplicateAPIEvent struct {
	EventType      ReplicateAPIEventType
	CollectionInfo *pb.CollectionInfo
	PartitionInfo  *pb.PartitionInfo
	ReplicateInfo  *commonpb.ReplicateInfo
	ReplicateParam ReplicateParam
	Error          error
}

type ReplicateAPIEventType int

const (
	ReplicateCreateCollection ReplicateAPIEventType = iota + 1
	ReplicateDropCollection
	ReplicateCreatePartition
	ReplicateDropPartition

	ReplicateError = 100
)

func (r ReplicateAPIEventType) String() string {
	switch r {
	case ReplicateCreateCollection:
		return "CreateCollection"
	case ReplicateDropCollection:
		return "DropCollection"
	case ReplicateCreatePartition:
		return "CreatePartition"
	case ReplicateDropPartition:
		return "DropPartition"
	default:
		return "Unknown"
	}
}

type DefaultChannelManager struct{}

var _ ChannelManager = (*DefaultChannelManager)(nil)

func (d *DefaultChannelManager) SetCtx(ctx context.Context) {
	log.Warn("SetCtx is not implemented, please check it")
}

func (d *DefaultChannelManager) AddDroppedCollection(ids []int64) {
	log.Warn("AddDroppedCollection is not implemented, please check it")
}

func (d *DefaultChannelManager) AddDroppedPartition(ids []int64) {
	log.Warn("AddDroppedPartition is not implemented, please check it")
}

func (d *DefaultChannelManager) StartReadCollection(ctx context.Context, info *pb.CollectionInfo, seekPositions []*msgpb.MsgPosition) error {
	log.Warn("StartReadCollection is not implemented, please check it")
	return nil
}

func (d *DefaultChannelManager) StopReadCollection(ctx context.Context, info *pb.CollectionInfo) error {
	log.Warn("StopReadCollection is not implemented, please check it")
	return nil
}

func (d *DefaultChannelManager) AddPartition(ctx context.Context, collectionInfo *pb.CollectionInfo, partitionInfo *pb.PartitionInfo) error {
	log.Warn("AddPartition is not implemented, please check it")
	return nil
}

func (d *DefaultChannelManager) GetChannelChan() <-chan string {
	log.Warn("GetChannelChan is not implemented, please check it")
	return nil
}

func (d *DefaultChannelManager) GetMsgChan(pChannel string) <-chan *ReplicateMsg {
	log.Warn("GetMsgChan is not implemented, please check it")
	return nil
}

func (d *DefaultChannelManager) GetEventChan() <-chan *ReplicateAPIEvent {
	log.Warn("GetEventChan is not implemented, please check it")
	return nil
}

func (d *DefaultChannelManager) GetChannelLatestMsgID(ctx context.Context, channelName string) ([]byte, error) {
	log.Warn("GetChannelLatestMsgID is not implemented, please check it")
	return nil, nil
}

type DefaultTargetAPI struct{}

var _ TargetAPI = (*DefaultTargetAPI)(nil)

func (d *DefaultTargetAPI) GetCollectionInfo(ctx context.Context, collectionName, databaseName string) (*model.CollectionInfo, error) {
	log.Warn("GetCollectionInfo is not implemented, please check it")
	return nil, nil
}

func (d *DefaultTargetAPI) GetPartitionInfo(ctx context.Context, collectionName string, databaseName string) (*model.CollectionInfo, error) {
	log.Warn("GetPartitionInfo is not implemented, please check it")
	return nil, nil
}

func (d *DefaultTargetAPI) GetDatabaseName(ctx context.Context, collectionName, databaseName string) (string, error) {
	log.Warn("GetDatabaseName is not implemented, please check it")
	return "", nil
}
