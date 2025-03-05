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
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/v2/util/crypto"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/resource"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"

	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/log"
	"github.com/zilliztech/milvus-cdc/core/util"
)

type MilvusDataHandler struct {
	api.DataHandler

	uri             string
	token           string
	enableTLS       bool
	ignorePartition bool // sometimes the has partition api is a deny api
	connectTimeout  int
	retryOptions    []retry.Option
	dialConfig      util.DialConfig
}

func NewMilvusDataHandler(options ...config.Option[*MilvusDataHandler]) (*MilvusDataHandler, error) {
	handler := &MilvusDataHandler{
		connectTimeout: 5,
	}
	for _, option := range options {
		option.Apply(handler)
	}
	if handler.uri == "" {
		return nil, errors.New("empty milvus connect uri")
	}

	var err error
	timeoutContext, cancel := context.WithTimeout(context.Background(), time.Duration(handler.connectTimeout)*time.Second)
	defer cancel()

	err = handler.milvusOp(timeoutContext, "", func(milvus *milvusclient.Client) error {
		return nil
	})
	if err != nil {
		log.Warn("fail to new the milvus client", zap.Error(err))
		return nil, err
	}
	handler.retryOptions = util.GetRetryOptions(config.GetCommonConfig().Retry)
	return handler, nil
}

func (m *MilvusDataHandler) milvusOp(ctx context.Context, database string, f func(milvus *milvusclient.Client) error) error {
	retryMilvusFunc := func() error {
		// TODO Retryable and non-retryable errors should be distinguished
		var err error
		var c *milvusclient.Client
		retryErr := retry.Do(ctx, func() error {
			c, err = util.GetMilvusClientManager().GetMilvusClient(ctx, m.uri, m.token, database, m.dialConfig)
			if err != nil {
				log.Warn("fail to get milvus client", zap.Error(err))
				return err
			}
			err = f(c)
			if status.Code(err) == codes.Canceled {
				util.GetMilvusClientManager().DeleteMilvusClient(m.uri, database)
				log.Warn("grpc: the client connection is closing, waiting...", zap.Error(err))
				time.Sleep(resource.DefaultExpiration)
			}
			return err
		}, m.retryOptions...)
		if retryErr != nil && err != nil {
			return err
		}
		if retryErr != nil {
			return retryErr
		}
		return nil
	}

	return retryMilvusFunc()
}

func (m *MilvusDataHandler) CreateCollection(ctx context.Context, param *api.CreateCollectionParam) error {
	createCollectionOption := milvusclient.NewCreateCollectionOption(param.Schema.CollectionName, param.Schema)
	describeCollectionOption := milvusclient.NewDescribeCollectionOption(param.Schema.CollectionName)

	for _, property := range param.Properties {
		createCollectionOption.WithProperty(property.GetKey(), property.GetValue())
	}
	createCollectionOption.WithShardNum(param.ShardsNum)
	createCollectionOption.WithConsistencyLevel(entity.ConsistencyLevel(param.ConsistencyLevel))
	createCollectionRequest := createCollectionOption.Request()
	createCollectionRequest.DbName = ""
	createCollectionRequest.Base = param.Base
	return m.milvusOp(ctx, param.Database, func(milvus *milvusclient.Client) error {
		if _, err := milvus.DescribeCollection(ctx, describeCollectionOption); err == nil {
			log.Info("skip to create collection, because it's has existed", zap.String("collection", param.Schema.CollectionName))
			return nil
		}
		ms := milvus.GetService()
		resp, err := ms.CreateCollection(ctx, createCollectionRequest)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		return nil
	})
}

func (m *MilvusDataHandler) DropCollection(ctx context.Context, param *api.DropCollectionParam) error {
	dropCollectionOption := milvusclient.NewDropCollectionOption(param.CollectionName)
	dropCollectionRequest := dropCollectionOption.Request()
	dropCollectionRequest.DbName = ""
	dropCollectionRequest.Base = param.Base
	return m.milvusOp(ctx, param.Database, func(milvus *milvusclient.Client) error {
		ms := milvus.GetService()
		resp, err := ms.DropCollection(ctx, dropCollectionRequest)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		return nil
	})
}

// TODO no used
func (m *MilvusDataHandler) Insert(ctx context.Context, param *api.InsertParam) error {
	partitionName := param.PartitionName
	if m.ignorePartition {
		log.Info("ignore partition name in insert request", zap.String("partition", partitionName))
		partitionName = ""
	}
	insertOption := milvusclient.NewColumnBasedInsertOption(param.CollectionName, param.Columns...)
	insertOption.WithPartition(partitionName)
	// insertOption.WithInsertMsgBase(param.Base)
	return m.milvusOp(ctx, param.Database, func(milvus *milvusclient.Client) error {
		_, err := milvus.Insert(ctx, insertOption)
		return err
	})
}

// TODO no used
func (m *MilvusDataHandler) Delete(ctx context.Context, param *api.DeleteParam) error {
	partitionName := param.PartitionName
	if m.ignorePartition {
		log.Info("ignore partition name in delete request", zap.String("partition", partitionName))
		partitionName = ""
	}
	deleteOption := milvusclient.NewDeleteOption(param.CollectionName)
	deleteOption.WithPartition(partitionName)
	pkName := param.Column.Name()
	if data := param.Column.FieldData().GetScalars().GetLongData().GetData(); data != nil {
		deleteOption.WithInt64IDs(pkName, data)
	} else if data := param.Column.FieldData().GetScalars().GetStringData().GetData(); data != nil {
		deleteOption.WithStringIDs(pkName, data)
	} else {
		return errors.New("unsupported data type")
	}

	return m.milvusOp(ctx, param.Database, func(milvus *milvusclient.Client) error {
		_, err := milvus.Delete(ctx, deleteOption)
		return err
	})
}

func (m *MilvusDataHandler) CreatePartition(ctx context.Context, param *api.CreatePartitionParam) error {
	if m.ignorePartition {
		log.Warn("ignore create partition", zap.String("collection", param.CollectionName), zap.String("partition", param.PartitionName))
		return nil
	}
	createPartitionParam := milvusclient.NewCreatePartitionOption(param.CollectionName, param.PartitionName)
	createPartitionRequest := createPartitionParam.Request()
	createPartitionRequest.DbName = ""
	createPartitionRequest.Base = param.Base
	listPartitionParam := milvusclient.NewListPartitionOption(param.CollectionName)
	return m.milvusOp(ctx, param.Database, func(milvus *milvusclient.Client) error {
		partitions, err := milvus.ListPartitions(ctx, listPartitionParam)
		if err != nil {
			log.Warn("fail to show partitions", zap.String("collection", param.CollectionName), zap.Error(err))
			return err
		}
		for _, partitionName := range partitions {
			if partitionName == param.PartitionName {
				log.Info("skip to create partition, because it's has existed",
					zap.String("collection", param.CollectionName),
					zap.String("partition", param.PartitionName))
				return nil
			}
		}
		ms := milvus.GetService()
		resp, err := ms.CreatePartition(ctx, createPartitionRequest)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		return nil
	})
}

func (m *MilvusDataHandler) DropPartition(ctx context.Context, param *api.DropPartitionParam) error {
	if m.ignorePartition {
		log.Warn("ignore drop partition", zap.String("collection", param.CollectionName), zap.String("partition", param.PartitionName))
		return nil
	}
	dropPartitionParam := milvusclient.NewDropPartitionOption(param.CollectionName, param.PartitionName)
	dropPartitionRequest := dropPartitionParam.Request()
	dropPartitionRequest.DbName = ""
	dropPartitionRequest.Base = param.Base
	return m.milvusOp(ctx, param.Database, func(milvus *milvusclient.Client) error {
		ms := milvus.GetService()
		resp, err := ms.DropPartition(ctx, dropPartitionRequest)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		return nil
	})
}

func (m *MilvusDataHandler) CreateIndex(ctx context.Context, param *api.CreateIndexParam) error {
	indexParam := index.NewGenericIndex(param.GetIndexName(), util.ConvertKVPairToMap(param.GetExtraParams()))
	createIndexParam := milvusclient.NewCreateIndexOption(param.CollectionName, param.FieldName, indexParam)
	createIndexParam = createIndexParam.WithIndexName(param.IndexName)
	createIndexRequest := createIndexParam.Request()
	createIndexRequest.DbName = ""
	createIndexRequest.Base = param.Base
	return m.milvusOp(ctx, param.Database, func(milvus *milvusclient.Client) error {
		ms := milvus.GetService()
		resp, err := ms.CreateIndex(ctx, createIndexRequest)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		return nil
	})
}

func (m *MilvusDataHandler) DropIndex(ctx context.Context, param *api.DropIndexParam) error {
	dropIndexParam := milvusclient.NewDropIndexOption(param.CollectionName, param.IndexName)
	dropIndexRequest := dropIndexParam.Request()
	dropIndexRequest.DbName = ""
	dropIndexRequest.Base = param.Base
	return m.milvusOp(ctx, param.Database, func(milvus *milvusclient.Client) error {
		ms := milvus.GetService()
		resp, err := ms.DropIndex(ctx, dropIndexRequest)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		return nil
	})
}

func (m *MilvusDataHandler) AlterIndex(ctx context.Context, param *api.AlterIndexParam) error {
	alterIndexRequest := param.AlterIndexRequest
	alterIndexRequest.DbName = ""
	alterIndexRequest.Base = param.Base
	return m.milvusOp(ctx, param.Database, func(milvus *milvusclient.Client) error {
		ms := milvus.GetService()
		resp, err := ms.AlterIndex(ctx, alterIndexRequest)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		return nil
	})
}

func (m *MilvusDataHandler) LoadCollection(ctx context.Context, param *api.LoadCollectionParam) error {
	loadCollectionRequest := param.LoadCollectionRequest
	loadCollectionRequest.DbName = ""
	loadCollectionRequest.Base = param.Base
	return m.milvusOp(ctx, param.Database, func(milvus *milvusclient.Client) error {
		ms := milvus.GetService()
		resp, err := ms.LoadCollection(ctx, loadCollectionRequest)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		return nil
	})
}

func (m *MilvusDataHandler) ReleaseCollection(ctx context.Context, param *api.ReleaseCollectionParam) error {
	releaseCollectionParam := milvusclient.NewReleaseCollectionOption(param.CollectionName)
	releaseCollectionRequest := releaseCollectionParam.Request()
	releaseCollectionRequest.DbName = ""
	releaseCollectionRequest.Base = param.Base
	return m.milvusOp(ctx, param.Database, func(milvus *milvusclient.Client) error {
		ms := milvus.GetService()
		resp, err := ms.ReleaseCollection(ctx, releaseCollectionRequest)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		return nil
	})
}

func (m *MilvusDataHandler) LoadPartitions(ctx context.Context, param *api.LoadPartitionsParam) error {
	loadPartitionsParam := milvusclient.NewLoadPartitionsOption(param.CollectionName, param.PartitionNames...)
	loadPartitionsRequest := loadPartitionsParam.Request()
	loadPartitionsRequest.DbName = ""
	loadPartitionsRequest.Base = param.Base
	return m.milvusOp(ctx, param.Database, func(milvus *milvusclient.Client) error {
		ms := milvus.GetService()
		resp, err := ms.LoadPartitions(ctx, loadPartitionsRequest)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		return nil
	})
}

func (m *MilvusDataHandler) ReleasePartitions(ctx context.Context, param *api.ReleasePartitionsParam) error {
	releasePartitonsParam := milvusclient.NewReleasePartitionsOptions(param.CollectionName, param.PartitionNames...)
	releasePartitionsRequest := releasePartitonsParam.Request()
	releasePartitionsRequest.DbName = ""
	releasePartitionsRequest.Base = param.Base
	return m.milvusOp(ctx, param.Database, func(milvus *milvusclient.Client) error {
		ms := milvus.GetService()
		resp, err := ms.ReleasePartitions(ctx, releasePartitionsRequest)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		return nil
	})
}

func (m *MilvusDataHandler) Flush(ctx context.Context, param *api.FlushParam) error {
	for _, s := range param.GetCollectionNames() {
		flushParam := milvusclient.NewFlushOption(s)
		flushRequest := flushParam.Request()
		flushRequest.DbName = ""
		flushRequest.Base = param.Base
		if err := m.milvusOp(ctx, param.Database, func(milvus *milvusclient.Client) error {
			ms := milvus.GetService()
			resp, err := ms.Flush(ctx, flushRequest)
			if err = merr.CheckRPCCall(resp, err); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func (m *MilvusDataHandler) CreateDatabase(ctx context.Context, param *api.CreateDatabaseParam) error {
	listDatabaseParam := milvusclient.NewListDatabaseOption()
	createDatabaseParam := milvusclient.NewCreateDatabaseOption(param.DbName)
	createDatabaseRequest := createDatabaseParam.Request()
	createDatabaseRequest.Base = param.Base
	return m.milvusOp(ctx, "", func(milvus *milvusclient.Client) error {
		databases, err := milvus.ListDatabase(ctx, listDatabaseParam)
		if err != nil {
			return err
		}
		for _, databaseName := range databases {
			if databaseName == param.DbName {
				log.Info("skip to create database, because it's has existed", zap.String("database", param.DbName))
				return nil
			}
		}

		ms := milvus.GetService()
		resp, err := ms.CreateDatabase(ctx, createDatabaseRequest)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		return nil
	})
}

func (m *MilvusDataHandler) DropDatabase(ctx context.Context, param *api.DropDatabaseParam) error {
	dropDatabaseParam := milvusclient.NewDropDatabaseOption(param.DbName)
	dropDatabaseRequest := dropDatabaseParam.Request()
	dropDatabaseRequest.Base = param.Base
	return m.milvusOp(ctx, "", func(milvus *milvusclient.Client) error {
		ms := milvus.GetService()
		resp, err := ms.DropDatabase(ctx, dropDatabaseRequest)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		return nil
	})
}

func (m *MilvusDataHandler) AlterDatabase(ctx context.Context, param *api.AlterDatabaseParam) error {
	alterDatabaseRequest := param.AlterDatabaseRequest
	alterDatabaseRequest.Base = param.Base
	return m.milvusOp(ctx, "", func(milvus *milvusclient.Client) error {
		ms := milvus.GetService()
		resp, err := ms.AlterDatabase(ctx, alterDatabaseRequest)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		return nil
	})
}

func (m *MilvusDataHandler) CreateUser(ctx context.Context, param *api.CreateUserParam) error {
	pwd, err := DecodePwd(param.Password)
	if err != nil {
		return err
	}
	createUserParam := milvusclient.NewCreateUserOption(param.Username, pwd)
	createUserRequest := createUserParam.Request()
	createUserRequest.Base = param.Base
	return m.milvusOp(ctx, "", func(milvus *milvusclient.Client) error {
		ms := milvus.GetService()
		resp, err := ms.CreateCredential(ctx, createUserRequest)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		return nil
	})
}

func (m *MilvusDataHandler) DeleteUser(ctx context.Context, param *api.DeleteUserParam) error {
	dropUserParam := milvusclient.NewDropUserOption(param.Username)
	dropUserRequest := dropUserParam.Request()
	dropUserRequest.Base = param.Base
	return m.milvusOp(ctx, "", func(milvus *milvusclient.Client) error {
		ms := milvus.GetService()
		resp, err := ms.DeleteCredential(ctx, dropUserRequest)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		return nil
	})
}

func (m *MilvusDataHandler) UpdateUser(ctx context.Context, param *api.UpdateUserParam) error {
	oldPwd, err := DecodePwd(param.OldPassword)
	if err != nil {
		return err
	}
	newPwd, err := DecodePwd(param.NewPassword)
	if err != nil {
		return err
	}
	updatePasswordParam := milvusclient.NewUpdatePasswordOption(param.Username, oldPwd, newPwd)
	updatePasswordRequest := updatePasswordParam.Request()
	updatePasswordRequest.Base = param.Base
	return m.milvusOp(ctx, "", func(milvus *milvusclient.Client) error {
		ms := milvus.GetService()
		resp, err := ms.UpdateCredential(ctx, updatePasswordRequest)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		return nil
	})
}

func DecodePwd(pwd string) (string, error) {
	return crypto.Base64Decode(pwd)
}

func (m *MilvusDataHandler) CreateRole(ctx context.Context, param *api.CreateRoleParam) error {
	createRoleParam := milvusclient.NewCreateRoleOption(param.GetEntity().GetName())
	createRoleRequest := createRoleParam.Request()
	createRoleRequest.Base = param.GetBase()
	return m.milvusOp(ctx, "", func(milvus *milvusclient.Client) error {
		ms := milvus.GetService()
		resp, err := ms.CreateRole(ctx, createRoleRequest)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		return nil
	})
}

func (m *MilvusDataHandler) DropRole(ctx context.Context, param *api.DropRoleParam) error {
	dropRoleParam := milvusclient.NewDropRoleOption(param.GetRoleName())
	dropRoleRequest := dropRoleParam.Request()
	dropRoleRequest.Base = param.GetBase()
	return m.milvusOp(ctx, "", func(milvus *milvusclient.Client) error {
		ms := milvus.GetService()
		resp, err := ms.DropRole(ctx, dropRoleRequest)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		return nil
	})
}

func (m *MilvusDataHandler) OperateUserRole(ctx context.Context, param *api.OperateUserRoleParam) error {
	operateUserRoleRequest := param.OperateUserRoleRequest
	operateUserRoleRequest.Base = param.Base

	return m.milvusOp(ctx, "", func(milvus *milvusclient.Client) error {
		ms := milvus.GetService()
		resp, err := ms.OperateUserRole(ctx, operateUserRoleRequest)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		return nil
	})
}

func (m *MilvusDataHandler) OperatePrivilege(ctx context.Context, param *api.OperatePrivilegeParam) error {
	operatePrivilegeRequest := param.OperatePrivilegeRequest
	operatePrivilegeRequest.Base = param.Base

	return m.milvusOp(ctx, "", func(milvus *milvusclient.Client) error {
		ms := milvus.GetService()
		resp, err := ms.OperatePrivilege(ctx, operatePrivilegeRequest)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		return nil
	})
}

func (m *MilvusDataHandler) ReplicateMessage(ctx context.Context, param *api.ReplicateMessageParam) error {
	var (
		req = &milvuspb.ReplicateMessageRequest{
			ChannelName:    param.ChannelName,
			BeginTs:        param.BeginTs,
			EndTs:          param.EndTs,
			Msgs:           param.MsgsBytes,
			StartPositions: param.StartPositions,
			EndPositions:   param.EndPositions,
			Base:           param.Base,
		}
		resp *milvuspb.ReplicateMessageResponse
		err  error
	)
	err = m.milvusOp(ctx, "", func(milvus *milvusclient.Client) error {
		ms := milvus.GetService()
		resp, err = ms.ReplicateMessage(ctx, req)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	param.TargetMsgPosition = resp.Position
	return nil
}

func (m *MilvusDataHandler) DescribeCollection(ctx context.Context, param *api.DescribeCollectionParam) error {
	describeCollectionParam := milvusclient.NewDescribeCollectionOption(param.Name)
	return m.milvusOp(ctx, param.Database, func(milvus *milvusclient.Client) error {
		_, err := milvus.DescribeCollection(ctx, describeCollectionParam)
		return err
	})
}

func (m *MilvusDataHandler) DescribeDatabase(ctx context.Context, param *api.DescribeDatabaseParam) error {
	listDatabaseParam := milvusclient.NewListDatabaseOption()
	return m.milvusOp(ctx, "", func(milvus *milvusclient.Client) error {
		databases, err := milvus.ListDatabase(ctx, listDatabaseParam)
		if err != nil {
			return err
		}
		for _, databaseName := range databases {
			if databaseName == param.Name {
				return nil
			}
		}
		return errors.Newf("database [%s] not found", param.Name)
	})
}

func (m *MilvusDataHandler) DescribePartition(ctx context.Context, param *api.DescribePartitionParam) error {
	listPartitionsParam := milvusclient.NewListPartitionOption(param.CollectionName)
	return m.milvusOp(ctx, param.Database, func(milvus *milvusclient.Client) error {
		partitions, err := milvus.ListPartitions(ctx, listPartitionsParam)
		if err != nil {
			return err
		}
		for _, partitionName := range partitions {
			if partitionName == param.PartitionName {
				return nil
			}
		}
		return errors.Newf("partition [%s] not found", param.PartitionName)
	})
}
