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

	"github.com/zilliztech/milvus-cdc/core/log"
	"github.com/zilliztech/milvus-cdc/core/model"
	"github.com/zilliztech/milvus-cdc/core/pb"
)

// MetaOp meta operation
type MetaOp interface {
	// WatchCollection its implementation should make sure it's only called once. The WatchPartition is same
	WatchCollection(ctx context.Context, filter CollectionFilter)
	WatchPartition(ctx context.Context, filter PartitionFilter)

	// SubscribeCollectionEvent an event only is consumed once. The SubscribePartitionEvent is same
	// TODO need to consider the many target, maybe try the method a meta op corresponds to a target
	SubscribeCollectionEvent(taskID string, consumer CollectionEventConsumer)
	SubscribePartitionEvent(taskID string, consumer PartitionEventConsumer)
	UnsubscribeEvent(taskID string, eventType WatchEventType)

	GetAllCollection(ctx context.Context, filter CollectionFilter) ([]*pb.CollectionInfo, error)
	GetAllPartition(ctx context.Context, filter PartitionFilter) ([]*pb.PartitionInfo, error)
	GetAllDroppedObj() map[string]map[string]uint64
	GetCollectionNameByID(ctx context.Context, id int64) string
	GetDatabaseInfoForCollection(ctx context.Context, id int64) model.DatabaseInfo
}

// CollectionFilter the filter will be used before the collection is filled the schema info
type CollectionFilter func(*pb.CollectionInfo) bool

type PartitionFilter func(info *pb.PartitionInfo) bool

type CollectionEventConsumer CollectionFilter

type PartitionEventConsumer PartitionFilter

type WatchEventType int

const (
	CollectionEventType WatchEventType = iota + 1
	PartitionEventType
)

type DefaultMetaOp struct{}

var _ MetaOp = (*DefaultMetaOp)(nil)

func (d *DefaultMetaOp) WatchCollection(ctx context.Context, filter CollectionFilter) {
	log.Warn("WatchCollection is not implemented, please check it")
}

func (d *DefaultMetaOp) WatchPartition(ctx context.Context, filter PartitionFilter) {
	log.Warn("WatchPartition is not implemented, please check it")
}

func (d *DefaultMetaOp) SubscribeCollectionEvent(taskID string, consumer CollectionEventConsumer) {
	log.Warn("SubscribeCollectionEvent is not implemented, please check it")
}

func (d *DefaultMetaOp) SubscribePartitionEvent(taskID string, consumer PartitionEventConsumer) {
	log.Warn("SubscribePartitionEvent is not implemented, please check it")
}

func (d *DefaultMetaOp) UnsubscribeEvent(taskID string, eventType WatchEventType) {
	log.Warn("UnsubscribeEvent is not implemented, please check it")
}

func (d *DefaultMetaOp) GetAllCollection(ctx context.Context, filter CollectionFilter) ([]*pb.CollectionInfo, error) {
	log.Warn("GetAllCollection is not implemented, please check it")
	return nil, nil
}

func (d *DefaultMetaOp) GetAllPartition(ctx context.Context, filter PartitionFilter) ([]*pb.PartitionInfo, error) {
	log.Warn("GetAllPartition is not implemented, please check it")
	return nil, nil
}

func (d *DefaultMetaOp) GetAllDroppedObj() map[string]map[string]uint64 {
	log.Warn("GetAllDroppedObj is not implemented, please check it")
	return nil
}

func (d *DefaultMetaOp) GetCollectionNameByID(ctx context.Context, id int64) string {
	log.Warn("GetCollectionNameByID is not implemented, please check it")
	return ""
}

func (d *DefaultMetaOp) GetDatabaseInfoForCollection(ctx context.Context, id int64) model.DatabaseInfo {
	return model.DatabaseInfo{}
}
