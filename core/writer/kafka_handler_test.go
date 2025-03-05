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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"

	"github.com/zilliztech/milvus-cdc/core/api"
)

func TestKafkaDataHandler(t *testing.T) {
	{
		_, err := NewKafkaDataHandler()
		assert.Error(t, err)
	}

	// no topic
	{
		_, err := NewKafkaDataHandler(KafkaAddressOption("localhost:9092"))
		assert.Error(t, err)
	}

	handler, err := NewKafkaDataHandler(
		KafkaTopicOption("test"),
		KafkaAddressOption("localhost:9092"),
	)
	assert.NoError(t, err)

	// wait for kafka connection
	time.Sleep(time.Second)
	ctx, cancelFunc := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancelFunc()

	t.Run("create collection", func(t *testing.T) {
		createCollectionParam := &api.CreateCollectionParam{
			Properties: []*commonpb.KeyValuePair{
				{
					Key:   "foo",
					Value: "hoo",
				},
			},
			ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
			MsgBaseParam: api.MsgBaseParam{
				Base: &commonpb.MsgBase{
					ReplicateInfo: &commonpb.ReplicateInfo{
						IsReplicate:  true,
						MsgTimestamp: 1000,
					},
				},
			},
			Schema: &entity.Schema{
				CollectionName: "foo",
				Fields: []*entity.Field{
					{
						Name:     "age",
						DataType: entity.FieldTypeInt8,
					},
					{
						Name:     "data",
						DataType: entity.FieldTypeBinaryVector,
					},
				},
			},
			ShardsNum: 1,
		}

		err := handler.CreateCollection(ctx, createCollectionParam)
		assert.NoError(t, err)
	})

	t.Run("drop collection", func(t *testing.T) {
		dropCollectionParam := &api.DropCollectionParam{
			CollectionName: "foo",
			ReplicateParam: api.ReplicateParam{
				Database: "foo",
			},
		}

		err := handler.DropCollection(context.Background(), dropCollectionParam)
		assert.NoError(t, err)
	})

	t.Run("insert", func(t *testing.T) {
		insertParam := &api.InsertParam{
			CollectionName: "foo",
			Columns: []column.Column{
				column.NewColumnInt64("age", []int64{10}),
			},
		}

		err := handler.Insert(ctx, insertParam)
		assert.NoError(t, err)
	})

	t.Run("delete", func(t *testing.T) {
		deleteParam := &api.DeleteParam{
			CollectionName: "foo",
			Column:         column.NewColumnInt64("age", []int64{10}),
		}

		err := handler.Delete(ctx, deleteParam)
		assert.NoError(t, err)
	})

	t.Run("create partition", func(t *testing.T) {
		createPartitionParam := &api.CreatePartitionParam{
			CollectionName: "foo",
			PartitionName:  "bar",
		}

		err := handler.CreatePartition(ctx, createPartitionParam)
		assert.NoError(t, err)
	})

	t.Run("delete partition", func(t *testing.T) {
		dropPartitionParam := &api.DropPartitionParam{
			CollectionName: "foo",
			PartitionName:  "bar",
		}

		err := handler.DropPartition(ctx, dropPartitionParam)
		assert.NoError(t, err)
	})

	t.Run("create database", func(t *testing.T) {
		createDatabaseParam := &api.CreateDatabaseParam{
			CreateDatabaseRequest: &milvuspb.CreateDatabaseRequest{
				DbName: "foo",
			},
		}

		err := handler.CreateDatabase(ctx, createDatabaseParam)
		assert.NoError(t, err)
	})

	t.Run("drop database", func(t *testing.T) {
		dropDatabaseParam := &api.DropDatabaseParam{
			DropDatabaseRequest: &milvuspb.DropDatabaseRequest{
				DbName: "foo",
			},
		}

		err := handler.DropDatabase(ctx, dropDatabaseParam)
		assert.NoError(t, err)
	})

	t.Run("alter database", func(t *testing.T) {
		alterDatabaseParam := &api.AlterDatabaseParam{
			AlterDatabaseRequest: &milvuspb.AlterDatabaseRequest{
				DbName: "foo",
				Properties: []*commonpb.KeyValuePair{
					{
						Key:   "foo",
						Value: "hoo",
					},
				},
			},
		}

		err := handler.AlterDatabase(ctx, alterDatabaseParam)
		assert.NoError(t, err)
	})

	t.Run("create index", func(t *testing.T) {
		createIndexParam := &api.CreateIndexParam{
			CreateIndexRequest: &milvuspb.CreateIndexRequest{
				CollectionName: "foo",
				FieldName:      "name",
				IndexName:      "baz",
			},
		}

		err := handler.CreateIndex(ctx, createIndexParam)
		assert.NoError(t, err)
	})

	t.Run("drop index", func(t *testing.T) {
		dropIndexParam := &api.DropIndexParam{
			DropIndexRequest: &milvuspb.DropIndexRequest{
				CollectionName: "foo",
				FieldName:      "bar",
				IndexName:      "baz",
			},
		}
		err := handler.DropIndex(ctx, dropIndexParam)
		assert.NoError(t, err)
	})

	t.Run("alter index", func(t *testing.T) {
		alterIndexParam := &api.AlterIndexParam{
			AlterIndexRequest: &milvuspb.AlterIndexRequest{
				CollectionName: "foo",
				IndexName:      "baz",
				ExtraParams: []*commonpb.KeyValuePair{
					{
						Key:   "foo",
						Value: "hoo",
					},
				},
			},
		}

		err := handler.AlterIndex(ctx, alterIndexParam)
		assert.NoError(t, err)
	})

	t.Run("load collection", func(t *testing.T) {
		loadCollectionParam := &api.LoadCollectionParam{
			LoadCollectionRequest: &milvuspb.LoadCollectionRequest{
				CollectionName: "foo",
				ReplicaNumber:  1,
			},
		}

		err := handler.LoadCollection(ctx, loadCollectionParam)
		assert.NoError(t, err)
	})

	t.Run("release collection", func(t *testing.T) {
		releaseCollectionParam := &api.ReleaseCollectionParam{
			ReleaseCollectionRequest: &milvuspb.ReleaseCollectionRequest{
				CollectionName: "foo",
			},
		}

		err := handler.ReleaseCollection(ctx, releaseCollectionParam)
		assert.NoError(t, err)
	})

	t.Run("load partitions", func(t *testing.T) {
		loadPartitionsParam := &api.LoadPartitionsParam{
			LoadPartitionsRequest: &milvuspb.LoadPartitionsRequest{
				CollectionName: "foo",
				PartitionNames: []string{"bar"},
			},
		}

		err := handler.LoadPartitions(ctx, loadPartitionsParam)
		assert.NoError(t, err)
	})

	t.Run("release partitions", func(t *testing.T) {
		releasePartitionsParam := &api.ReleasePartitionsParam{
			ReleasePartitionsRequest: &milvuspb.ReleasePartitionsRequest{
				CollectionName: "foo",
				PartitionNames: []string{"bar"},
			},
		}

		err := handler.ReleasePartitions(ctx, releasePartitionsParam)
		assert.NoError(t, err)
	})

	t.Run("flush", func(t *testing.T) {
		flushParam := &api.FlushParam{
			FlushRequest: &milvuspb.FlushRequest{
				CollectionNames: []string{"foo"},
			},
		}

		err := handler.Flush(ctx, flushParam)
		assert.NoError(t, err)
	})

	t.Run("replicate message", func(t *testing.T) {
		replicateMessageParam := &api.ReplicateMessageParam{
			ChannelName: "foo",
			BeginTs:     1,
			EndTs:       2,
			MsgsBytes:   [][]byte{{1}, {2}},
			StartPositions: []*msgpb.MsgPosition{
				{
					ChannelName: "foo",
					MsgID:       []byte{1},
				},
			},
			EndPositions: []*msgpb.MsgPosition{
				{
					ChannelName: "foo",
					MsgID:       []byte{1},
				},
			},
		}

		err := handler.ReplicateMessage(ctx, replicateMessageParam)
		assert.NoError(t, err)
	})

	t.Run("create user", func(t *testing.T) {
		createUserParam := &api.CreateUserParam{
			CreateCredentialRequest: &milvuspb.CreateCredentialRequest{
				Username: "user_test",
				Password: "password",
			},
		}

		err := handler.CreateUser(ctx, createUserParam)
		assert.NoError(t, err)
	})

	t.Run("delete user", func(t *testing.T) {
		deleteUserParam := &api.DeleteUserParam{
			DeleteCredentialRequest: &milvuspb.DeleteCredentialRequest{
				Username: "user_test",
			},
		}

		err := handler.DeleteUser(ctx, deleteUserParam)
		assert.NoError(t, err)
	})

	t.Run("update user", func(t *testing.T) {
		updateUserParam := &api.UpdateUserParam{
			UpdateCredentialRequest: &milvuspb.UpdateCredentialRequest{
				Username:    "user_test",
				OldPassword: "password",
				NewPassword: "new_password",
			},
		}

		err := handler.UpdateUser(ctx, updateUserParam)
		assert.NoError(t, err)
	})

	t.Run("create role", func(t *testing.T) {
		createRoleParam := &api.CreateRoleParam{
			CreateRoleRequest: &milvuspb.CreateRoleRequest{
				Entity: &milvuspb.RoleEntity{
					Name: "role_test",
				},
			},
		}

		err := handler.CreateRole(ctx, createRoleParam)
		assert.NoError(t, err)
	})

	t.Run("drop role", func(t *testing.T) {
		dropRoleParam := &api.DropRoleParam{
			DropRoleRequest: &milvuspb.DropRoleRequest{
				RoleName: "role_test",
			},
		}

		err := handler.DropRole(ctx, dropRoleParam)
		assert.NoError(t, err)
	})

	t.Run("add user to role", func(t *testing.T) {
		addUser2RoleParam := &api.OperateUserRoleParam{
			OperateUserRoleRequest: &milvuspb.OperateUserRoleRequest{
				Type:     milvuspb.OperateUserRoleType_AddUserToRole,
				Username: "user_test",
				RoleName: "role_test",
			},
		}

		err := handler.OperateUserRole(ctx, addUser2RoleParam)
		assert.NoError(t, err)
	})

	t.Run("remove user from role", func(t *testing.T) {
		removeUserFromRoleParam := &api.OperateUserRoleParam{
			OperateUserRoleRequest: &milvuspb.OperateUserRoleRequest{
				Type:     milvuspb.OperateUserRoleType_RemoveUserFromRole,
				Username: "user_test",
				RoleName: "role_test",
			},
		}

		err := handler.OperateUserRole(ctx, removeUserFromRoleParam)
		assert.NoError(t, err)
	})

	t.Run("grant privilege to role", func(t *testing.T) {
		grantPrivilege2UserParam := &api.OperatePrivilegeParam{
			OperatePrivilegeRequest: &milvuspb.OperatePrivilegeRequest{
				Type: milvuspb.OperatePrivilegeType_Grant,
				Entity: &milvuspb.GrantEntity{
					Role: &milvuspb.RoleEntity{
						Name: "role_test",
					},
					Object: &milvuspb.ObjectEntity{
						Name: "User",
					},
					ObjectName: "user_test",
					Grantor: &milvuspb.GrantorEntity{
						Privilege: &milvuspb.PrivilegeEntity{
							Name: "SelectUser",
						},
					},
				},
			},
		}

		err := handler.OperatePrivilege(ctx, grantPrivilege2UserParam)
		assert.NoError(t, err)
	})

	t.Run("revoke privilege from role", func(t *testing.T) {
		revokePrivilegeFromUserParam := &api.OperatePrivilegeParam{
			OperatePrivilegeRequest: &milvuspb.OperatePrivilegeRequest{
				Type: milvuspb.OperatePrivilegeType_Revoke,
				Entity: &milvuspb.GrantEntity{
					Role: &milvuspb.RoleEntity{
						Name: "role_test",
					},
					Object: &milvuspb.ObjectEntity{
						Name: "User",
					},
					ObjectName: "user_test",
					Grantor: &milvuspb.GrantorEntity{
						Privilege: &milvuspb.PrivilegeEntity{
							Name: "SelectUser",
						},
					},
				},
			},
		}

		err := handler.OperatePrivilege(ctx, revokePrivilegeFromUserParam)
		assert.NoError(t, err)
	})
}
