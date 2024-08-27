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
)

type DataHandler interface {
	CreateCollection(ctx context.Context, param *CreateCollectionParam) error
	DropCollection(ctx context.Context, param *DropCollectionParam) error
	CreatePartition(ctx context.Context, param *CreatePartitionParam) error
	DropPartition(ctx context.Context, param *DropPartitionParam) error

	// Deprecated
	Insert(ctx context.Context, param *InsertParam) error
	// Deprecated
	Delete(ctx context.Context, param *DeleteParam) error
	Flush(ctx context.Context, param *FlushParam) error

	LoadCollection(ctx context.Context, param *LoadCollectionParam) error
	ReleaseCollection(ctx context.Context, param *ReleaseCollectionParam) error
	LoadPartitions(ctx context.Context, param *LoadPartitionsParam) error
	ReleasePartitions(ctx context.Context, param *ReleasePartitionsParam) error

	CreateIndex(ctx context.Context, param *CreateIndexParam) error
	DropIndex(ctx context.Context, param *DropIndexParam) error
	AlterIndex(ctx context.Context, param *AlterIndexParam) error

	CreateDatabase(ctx context.Context, param *CreateDatabaseParam) error
	DropDatabase(ctx context.Context, param *DropDatabaseParam) error
	AlterDatabase(ctx context.Context, param *AlterDatabaseParam) error

	ReplicateMessage(ctx context.Context, param *ReplicateMessageParam) error

	DescribeCollection(ctx context.Context, param *DescribeCollectionParam) error
	DescribeDatabase(ctx context.Context, param *DescribeDatabaseParam) error
	DescribePartition(ctx context.Context, param *DescribePartitionParam) error

	CreateUser(ctx context.Context, param *CreateUserParam) error
	DeleteUser(ctx context.Context, param *DeleteUserParam) error
	UpdateUser(ctx context.Context, param *UpdateUserParam) error
	CreateRole(ctx context.Context, param *CreateRoleParam) error
	DropRole(ctx context.Context, param *DropRoleParam) error
	OperateUserRole(ctx context.Context, param *OperateUserRoleParam) error
	OperatePrivilege(ctx context.Context, param *OperatePrivilegeParam) error
}

type DefaultDataHandler struct{}

var _ DataHandler = (*DefaultDataHandler)(nil)

func (d *DefaultDataHandler) CreateCollection(ctx context.Context, param *CreateCollectionParam) error {
	log.Warn("CreateCollection is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) DropCollection(ctx context.Context, param *DropCollectionParam) error {
	log.Warn("DropCollection is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) Insert(ctx context.Context, param *InsertParam) error {
	log.Warn("Insert is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) Delete(ctx context.Context, param *DeleteParam) error {
	log.Warn("Delete is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) CreatePartition(ctx context.Context, param *CreatePartitionParam) error {
	log.Warn("CreatePartition is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) DropPartition(ctx context.Context, param *DropPartitionParam) error {
	log.Warn("DropPartition is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) CreateIndex(ctx context.Context, param *CreateIndexParam) error {
	log.Warn("CreateIndex is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) DropIndex(ctx context.Context, param *DropIndexParam) error {
	log.Warn("DropIndex is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) AlterIndex(ctx context.Context, param *AlterIndexParam) error {
	log.Warn("AlterIndex is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) LoadCollection(ctx context.Context, param *LoadCollectionParam) error {
	log.Warn("LoadCollection is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) ReleaseCollection(ctx context.Context, param *ReleaseCollectionParam) error {
	log.Warn("ReleaseCollection is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) Flush(ctx context.Context, param *FlushParam) error {
	log.Warn("Flush is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) CreateDatabase(ctx context.Context, param *CreateDatabaseParam) error {
	log.Warn("CreateDatabase is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) DropDatabase(ctx context.Context, param *DropDatabaseParam) error {
	log.Warn("DropDatabase is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) AlterDatabase(ctx context.Context, param *AlterDatabaseParam) error {
	log.Warn("AlterDatabase is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) ReplicateMessage(ctx context.Context, param *ReplicateMessageParam) error {
	log.Warn("Replicate is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) LoadPartitions(ctx context.Context, param *LoadPartitionsParam) error {
	log.Warn("LoadPartitions is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) ReleasePartitions(ctx context.Context, param *ReleasePartitionsParam) error {
	log.Warn("ReleasePartitions is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) DescribeCollection(ctx context.Context, param *DescribeCollectionParam) error {
	log.Warn("DescribeCollection is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) DescribeDatabase(ctx context.Context, param *DescribeDatabaseParam) error {
	log.Warn("DescribeDatabase is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) DescribePartition(ctx context.Context, param *DescribePartitionParam) error {
	log.Warn("DescribePartition is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) CreateUser(ctx context.Context, param *CreateUserParam) error {
	log.Warn("CreateUser is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) DeleteUser(ctx context.Context, param *DeleteUserParam) error {
	log.Warn("DeleteUser is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) UpdateUser(ctx context.Context, param *UpdateUserParam) error {
	log.Warn("UpdateUser is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) CreateRole(ctx context.Context, param *CreateRoleParam) error {
	log.Warn("CreateRole is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) DropRole(ctx context.Context, param *DropRoleParam) error {
	log.Warn("DropRole is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) OperateUserRole(ctx context.Context, param *OperateUserRoleParam) error {
	log.Warn("OperateUserRole is not implemented, please check it")
	return nil
}

func (d *DefaultDataHandler) OperatePrivilege(ctx context.Context, param *OperatePrivilegeParam) error {
	log.Warn("OperatePrivilege is not implemented, please check it")
	return nil
}
