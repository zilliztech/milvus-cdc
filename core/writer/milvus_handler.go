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

package writer

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/log"
	"github.com/zilliztech/milvus-cdc/core/util"
)

type MilvusDataHandler struct {
	api.DataHandler

	address         string
	username        string
	password        string
	enableTLS       bool
	ignorePartition bool // sometimes the has partition api is a deny api
	connectTimeout  int

	// default db milvus client
	milvus client.Client
}

// NewMilvusDataHandler options must include AddressOption
func NewMilvusDataHandler(options ...config.Option[*MilvusDataHandler]) (*MilvusDataHandler, error) {
	handler := &MilvusDataHandler{
		connectTimeout: 5,
	}
	for _, option := range options {
		option.Apply(handler)
	}
	if handler.address == "" {
		return nil, errors.New("empty milvus address")
	}

	var err error
	timeoutContext, cancel := context.WithTimeout(context.Background(), time.Duration(handler.connectTimeout)*time.Second)
	defer cancel()

	handler.milvus, err = client.NewClient(timeoutContext, client.Config{
		Address:       handler.address,
		Username:      handler.username,
		Password:      handler.password,
		EnableTLSAuth: handler.enableTLS,
	})
	if err != nil {
		log.Warn("fail to new the milvus client", zap.Error(err))
		return nil, err
	}
	return handler, nil
}

func (m *MilvusDataHandler) milvusOp(ctx context.Context, database string, f func(milvus client.Client) error) error {
	if database == "" || database == util.DefaultDbName {
		return f(m.milvus)
	}
	milvusClient, err := util.GetMilvusClientManager().GetMilvusClient(ctx,
		m.address,
		util.GetAPIKey(m.username, m.password),
		database,
		m.enableTLS)
	if err != nil {
		log.Warn("fail to get milvus client", zap.Error(err))
		return err
	}
	return f(milvusClient)
}

func (m *MilvusDataHandler) CreateCollection(ctx context.Context, param *api.CreateCollectionParam) error {
	var options []client.CreateCollectionOption
	for _, property := range param.Properties {
		options = append(options, client.WithCollectionProperty(property.GetKey(), property.GetValue()))
	}
	options = append(options,
		client.WithConsistencyLevel(entity.ConsistencyLevel(param.ConsistencyLevel)),
		client.WithCreateCollectionMsgBase(param.Base),
	)
	return m.milvusOp(ctx, param.Database, func(milvus client.Client) error {
		return milvus.CreateCollection(ctx, param.Schema, param.ShardsNum, options...)
	})
}

func (m *MilvusDataHandler) DropCollection(ctx context.Context, param *api.DropCollectionParam) error {
	return m.milvusOp(ctx, param.Database, func(milvus client.Client) error {
		return milvus.DropCollection(ctx, param.CollectionName, client.WithDropCollectionMsgBase(param.Base))
	})
}

func (m *MilvusDataHandler) Insert(ctx context.Context, param *api.InsertParam) error {
	partitionName := param.PartitionName
	if m.ignorePartition {
		log.Info("ignore partition name in insert request", zap.String("partition", partitionName))
		partitionName = ""
	}
	return m.milvusOp(ctx, param.Database, func(milvus client.Client) error {
		_, err := milvus.Insert(ctx, param.CollectionName, partitionName, param.Columns...)
		return err
	})
}

func (m *MilvusDataHandler) Delete(ctx context.Context, param *api.DeleteParam) error {
	partitionName := param.PartitionName
	if m.ignorePartition {
		log.Info("ignore partition name in delete request", zap.String("partition", partitionName))
		partitionName = ""
	}

	return m.milvusOp(ctx, param.Database, func(milvus client.Client) error {
		return milvus.DeleteByPks(ctx, param.CollectionName, partitionName, param.Column)
	})
}

func (m *MilvusDataHandler) CreatePartition(ctx context.Context, param *api.CreatePartitionParam) error {
	if m.ignorePartition {
		log.Warn("ignore create partition", zap.String("collection", param.CollectionName), zap.String("partition", param.PartitionName))
		return nil
	}
	return m.milvusOp(ctx, param.Database, func(milvus client.Client) error {
		return milvus.CreatePartition(ctx, param.CollectionName, param.PartitionName)
	})
}

func (m *MilvusDataHandler) DropPartition(ctx context.Context, param *api.DropPartitionParam) error {
	if m.ignorePartition {
		log.Warn("ignore drop partition", zap.String("collection", param.CollectionName), zap.String("partition", param.PartitionName))
		return nil
	}
	return m.milvusOp(ctx, param.Database, func(milvus client.Client) error {
		return milvus.DropPartition(ctx, param.CollectionName, param.PartitionName)
	})
}

func (m *MilvusDataHandler) CreateIndex(ctx context.Context, param *api.CreateIndexParam) error {
	indexEntity := entity.NewGenericIndex(param.GetIndexName(), "", util.ConvertKVPairToMap(param.GetExtraParams()))
	return m.milvusOp(ctx, param.Database, func(milvus client.Client) error {
		return milvus.CreateIndex(ctx, param.GetCollectionName(), param.GetFieldName(), indexEntity, true,
			client.WithIndexName(param.GetIndexName()),
			client.WithIndexMsgBase(param.GetBase()),
		)
	})
}

func (m *MilvusDataHandler) DropIndex(ctx context.Context, param *api.DropIndexParam) error {
	return m.milvusOp(ctx, param.Database, func(milvus client.Client) error {
		return milvus.DropIndex(ctx, param.CollectionName, param.FieldName,
			client.WithIndexName(param.IndexName),
			client.WithIndexMsgBase(param.GetBase()),
		)
	})
}

func (m *MilvusDataHandler) LoadCollection(ctx context.Context, param *api.LoadCollectionParam) error {
	// TODO resource group
	return m.milvusOp(ctx, param.Database, func(milvus client.Client) error {
		return milvus.LoadCollection(ctx, param.CollectionName, true,
			client.WithReplicaNumber(param.ReplicaNumber),
			client.WithLoadCollectionMsgBase(param.GetBase()),
		)
	})
}

func (m *MilvusDataHandler) ReleaseCollection(ctx context.Context, param *api.ReleaseCollectionParam) error {
	return m.milvusOp(ctx, param.Database, func(milvus client.Client) error {
		return milvus.ReleaseCollection(ctx, param.CollectionName,
			client.WithReleaseCollectionMsgBase(param.GetBase()),
		)
	})
}

func (m *MilvusDataHandler) LoadPartitions(ctx context.Context, param *api.LoadPartitionsParam) error {
	return m.milvusOp(ctx, param.Database, func(milvus client.Client) error {
		return milvus.LoadPartitions(ctx, param.CollectionName, param.PartitionNames, true,
			client.WithLoadPartitionsMsgBase(param.GetBase()))
	})
}

func (m *MilvusDataHandler) ReleasePartitions(ctx context.Context, param *api.ReleasePartitionsParam) error {
	return m.milvusOp(ctx, param.Database, func(milvus client.Client) error {
		return milvus.ReleasePartitions(ctx, param.CollectionName, param.PartitionNames,
			client.WithReleasePartitionMsgBase(param.GetBase()))
	})
}

func (m *MilvusDataHandler) Flush(ctx context.Context, param *api.FlushParam) error {
	for _, s := range param.GetCollectionNames() {
		if err := m.milvusOp(ctx, param.Database, func(milvus client.Client) error {
			return milvus.Flush(ctx, s, true, client.WithFlushMsgBase(param.GetBase()))
		}); err != nil {
			return err
		}
	}
	return nil
}

func (m *MilvusDataHandler) CreateDatabase(ctx context.Context, param *api.CreateDatabaseParam) error {
	return m.milvus.CreateDatabase(ctx, param.DbName,
		client.WithCreateDatabaseMsgBase(param.GetBase()),
	)
}

func (m *MilvusDataHandler) DropDatabase(ctx context.Context, param *api.DropDatabaseParam) error {
	return m.milvus.DropDatabase(ctx, param.DbName,
		client.WithDropDatabaseMsgBase(param.GetBase()),
	)
}

func (m *MilvusDataHandler) ReplicateMessage(ctx context.Context, param *api.ReplicateMessageParam) error {
	resp, err := m.milvus.ReplicateMessage(ctx, param.ChannelName,
		param.BeginTs, param.EndTs,
		param.MsgsBytes,
		param.StartPositions, param.EndPositions,
		client.WithReplicateMessageMsgBase(param.Base))
	if err == nil {
		param.TargetMsgPosition = resp.Position
	}
	return err
}

func (m *MilvusDataHandler) DescribeCollection(ctx context.Context, param *api.DescribeCollectionParam) error {
	return m.milvusOp(ctx, param.Database, func(milvus client.Client) error {
		_, err := milvus.DescribeCollection(ctx, param.Name)
		return err
	})
}

func (m *MilvusDataHandler) DescribeDatabase(ctx context.Context, param *api.DescribeDatabaseParam) error {
	databases, err := m.milvus.ListDatabases(ctx)
	if err != nil {
		return err
	}
	for _, database := range databases {
		if database.Name == param.Name {
			return nil
		}
	}
	return errors.Newf("database [%s] not found", param.Name)
}
