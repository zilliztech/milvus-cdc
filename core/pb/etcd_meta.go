// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pb

import (
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

type CollectionState int32

const (
	CollectionState_CollectionCreated  CollectionState = 0
	CollectionState_CollectionCreating CollectionState = 1
	CollectionState_CollectionDropping CollectionState = 2
	CollectionState_CollectionDropped  CollectionState = 3
)

var CollectionState_name = map[int32]string{
	0: "CollectionCreated",
	1: "CollectionCreating",
	2: "CollectionDropping",
	3: "CollectionDropped",
}

var CollectionState_value = map[string]int32{
	"CollectionCreated":  0,
	"CollectionCreating": 1,
	"CollectionDropping": 2,
	"CollectionDropped":  3,
}

func (x CollectionState) String() string {
	return proto.EnumName(CollectionState_name, int32(x))
}

type PartitionState int32

const (
	PartitionState_PartitionCreated  PartitionState = 0
	PartitionState_PartitionCreating PartitionState = 1
	PartitionState_PartitionDropping PartitionState = 2
	PartitionState_PartitionDropped  PartitionState = 3
)

var PartitionState_name = map[int32]string{
	0: "PartitionCreated",
	1: "PartitionCreating",
	2: "PartitionDropping",
	3: "PartitionDropped",
}

var PartitionState_value = map[string]int32{
	"PartitionCreated":  0,
	"PartitionCreating": 1,
	"PartitionDropping": 2,
	"PartitionDropped":  3,
}

type CollectionInfo struct {
	ID         int64                      `protobuf:"varint,1,opt,name=ID,proto3" json:"ID,omitempty"`
	Schema     *schemapb.CollectionSchema `protobuf:"bytes,2,opt,name=schema,proto3" json:"schema,omitempty"`
	CreateTime uint64                     `protobuf:"varint,3,opt,name=create_time,json=createTime,proto3" json:"create_time,omitempty"`
	// deprecate
	PartitionIDs []int64 `protobuf:"varint,4,rep,packed,name=partitionIDs,proto3" json:"partitionIDs,omitempty"`
	// deprecate
	PartitionNames []string `protobuf:"bytes,5,rep,name=partitionNames,proto3" json:"partitionNames,omitempty"`
	// deprecate
	FieldIndexes         []*FieldIndexInfo `protobuf:"bytes,6,rep,name=field_indexes,json=fieldIndexes,proto3" json:"field_indexes,omitempty"`
	VirtualChannelNames  []string          `protobuf:"bytes,7,rep,name=virtual_channel_names,json=virtualChannelNames,proto3" json:"virtual_channel_names,omitempty"`
	PhysicalChannelNames []string          `protobuf:"bytes,8,rep,name=physical_channel_names,json=physicalChannelNames,proto3" json:"physical_channel_names,omitempty"`
	// deprecate
	PartitionCreatedTimestamps []uint64                  `protobuf:"varint,9,rep,packed,name=partition_created_timestamps,json=partitionCreatedTimestamps,proto3" json:"partition_created_timestamps,omitempty"`
	ShardsNum                  int32                     `protobuf:"varint,10,opt,name=shards_num,json=shardsNum,proto3" json:"shards_num,omitempty"`
	StartPositions             []*commonpb.KeyDataPair   `protobuf:"bytes,11,rep,name=start_positions,json=startPositions,proto3" json:"start_positions,omitempty"`
	ConsistencyLevel           commonpb.ConsistencyLevel `protobuf:"varint,12,opt,name=consistency_level,json=consistencyLevel,proto3,enum=milvus.proto.common.ConsistencyLevel" json:"consistency_level,omitempty"`
	State                      CollectionState           `protobuf:"varint,13,opt,name=state,proto3,enum=milvus.proto.etcd.CollectionState" json:"state,omitempty"`
	Properties                 []*commonpb.KeyValuePair  `protobuf:"bytes,14,rep,name=properties,proto3" json:"properties,omitempty"`
	XXX_NoUnkeyedLiteral       struct{}                  `json:"-"`
	XXX_unrecognized           []byte                    `json:"-"`
	XXX_sizecache              int32                     `json:"-"`
}

func (m *CollectionInfo) Reset()         { *m = CollectionInfo{} }
func (m *CollectionInfo) String() string { return proto.CompactTextString(m) }
func (*CollectionInfo) ProtoMessage()    {}

type FieldIndexInfo struct {
	FiledID              int64    `protobuf:"varint,1,opt,name=filedID,proto3" json:"filedID,omitempty"`
	IndexID              int64    `protobuf:"varint,2,opt,name=indexID,proto3" json:"indexID,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *FieldIndexInfo) Reset()         { *m = FieldIndexInfo{} }
func (m *FieldIndexInfo) String() string { return proto.CompactTextString(m) }
func (*FieldIndexInfo) ProtoMessage()    {}

type PartitionInfo struct {
	PartitionID               int64          `protobuf:"varint,1,opt,name=partitionID,proto3" json:"partitionID,omitempty"`
	PartitionName             string         `protobuf:"bytes,2,opt,name=partitionName,proto3" json:"partitionName,omitempty"`
	PartitionCreatedTimestamp uint64         `protobuf:"varint,3,opt,name=partition_created_timestamp,json=partitionCreatedTimestamp,proto3" json:"partition_created_timestamp,omitempty"`
	CollectionID              int64          `protobuf:"varint,4,opt,name=collection_id,json=collectionId,proto3" json:"collection_id,omitempty"`
	State                     PartitionState `protobuf:"varint,5,opt,name=state,proto3,enum=milvus.proto.etcd.PartitionState" json:"state,omitempty"`
	XXX_NoUnkeyedLiteral      struct{}       `json:"-"`
	XXX_unrecognized          []byte         `json:"-"`
	XXX_sizecache             int32          `json:"-"`
}

func (m *PartitionInfo) Reset()         { *m = PartitionInfo{} }
func (m *PartitionInfo) String() string { return proto.CompactTextString(m) }
func (*PartitionInfo) ProtoMessage()    {}
