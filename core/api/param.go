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
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
)

type MsgBaseParam struct {
	Base *commonpb.MsgBase
}

type ReplicateParam struct {
	Database string
}

type CreateCollectionParam struct {
	MsgBaseParam
	ReplicateParam
	Schema           *entity.Schema
	ShardsNum        int32
	ConsistencyLevel commonpb.ConsistencyLevel
	Properties       []*commonpb.KeyValuePair
}

type DropCollectionParam struct {
	MsgBaseParam
	ReplicateParam
	CollectionName string
}

type InsertParam struct {
	MsgBaseParam
	ReplicateParam
	CollectionName string
	PartitionName  string
	Columns        []column.Column
}

type DeleteParam struct {
	MsgBaseParam
	ReplicateParam
	CollectionName string
	PartitionName  string
	Column         column.Column
}

type CreatePartitionParam struct {
	MsgBaseParam
	ReplicateParam
	CollectionName string
	PartitionName  string
}

type DropPartitionParam struct {
	MsgBaseParam
	ReplicateParam
	CollectionName string
	PartitionName  string
}

type CreateIndexParam struct {
	ReplicateParam
	*milvuspb.CreateIndexRequest
}

type DropIndexParam struct {
	ReplicateParam
	*milvuspb.DropIndexRequest
}

const IndexKeyMmap = "mmap.enabled"

type AlterIndexParam struct {
	ReplicateParam
	*milvuspb.AlterIndexRequest
}

type LoadCollectionParam struct {
	ReplicateParam
	*milvuspb.LoadCollectionRequest
}

type ReleaseCollectionParam struct {
	ReplicateParam
	*milvuspb.ReleaseCollectionRequest
}

type LoadPartitionsParam struct {
	ReplicateParam
	*milvuspb.LoadPartitionsRequest
}

type ReleasePartitionsParam struct {
	ReplicateParam
	*milvuspb.ReleasePartitionsRequest
}

type CreateDatabaseParam struct {
	ReplicateParam
	*milvuspb.CreateDatabaseRequest
}

type DropDatabaseParam struct {
	ReplicateParam
	*milvuspb.DropDatabaseRequest
}

type AlterDatabaseParam struct {
	ReplicateParam
	*milvuspb.AlterDatabaseRequest
}

type FlushParam struct {
	ReplicateParam
	*milvuspb.FlushRequest
}

type CreateUserParam struct {
	ReplicateParam
	*milvuspb.CreateCredentialRequest
}

type UpdateUserParam struct {
	ReplicateParam
	*milvuspb.UpdateCredentialRequest
}

type DeleteUserParam struct {
	ReplicateParam
	*milvuspb.DeleteCredentialRequest
}

type CreateRoleParam struct {
	ReplicateParam
	*milvuspb.CreateRoleRequest
}

type DropRoleParam struct {
	ReplicateParam
	*milvuspb.DropRoleRequest
}

type OperateUserRoleParam struct {
	ReplicateParam
	*milvuspb.OperateUserRoleRequest
}

type OperatePrivilegeParam struct {
	ReplicateParam
	*milvuspb.OperatePrivilegeRequest
}

type OperatePrivilegeV2Param struct {
	ReplicateParam
	*milvuspb.OperatePrivilegeV2Request
}

type CreatePrivilegeGroupParam struct {
	ReplicateParam
	*milvuspb.CreatePrivilegeGroupRequest
}

type DropPrivilegeGroupParam struct {
	ReplicateParam
	*milvuspb.DropPrivilegeGroupRequest
}

type OperatePrivilegeGroupParam struct {
	ReplicateParam
	*milvuspb.OperatePrivilegeGroupRequest
}

type ReplicateMessageParam struct {
	MsgBaseParam
	ReplicateParam
	ChannelName                  string
	BeginTs, EndTs               uint64
	MsgsBytes                    [][]byte
	StartPositions, EndPositions []*msgpb.MsgPosition

	TargetMsgPosition string
}

type DescribeCollectionParam struct {
	ReplicateParam
	Name string
}

type DescribeDatabaseParam struct {
	ReplicateParam
	Name string
}

type DescribePartitionParam struct {
	ReplicateParam
	CollectionName string
	PartitionName  string
}

type SimpleAttribution struct {
	Key   string
	Value string
}

func (s SimpleAttribution) KeyValue() (string, string) {
	return s.Key, s.Value
}

func (s SimpleAttribution) Valid() error {
	return nil
}

func GetSimpleAttributions(pairs []*commonpb.KeyValuePair) []entity.CollectionAttribute {
	res := make([]entity.CollectionAttribute, 0, len(pairs))
	for _, pair := range pairs {
		res = append(res, SimpleAttribution{
			Key:   pair.Key,
			Value: pair.Value,
		})
	}
	return res
}
