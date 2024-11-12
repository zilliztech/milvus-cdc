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

package reader

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/milvus-io/milvus/pkg/util/resource"

	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/log"
	"github.com/zilliztech/milvus-cdc/core/model"
	"github.com/zilliztech/milvus-cdc/core/util"
)

var _ api.TargetAPI = (*TargetClient)(nil)

type TargetClient struct {
	client       client.Client
	config       TargetConfig
	nameMappings util.Map[string, string]
}

type TargetConfig struct {
	URI        string
	Token      string
	APIKey     string
	DialConfig util.DialConfig
}

func NewTarget(ctx context.Context, config TargetConfig) (api.TargetAPI, error) {
	targetClient := &TargetClient{
		config: config,
	}
	return targetClient, nil
}

func (t *TargetClient) milvusOp(ctx context.Context, database string, f func(milvus client.Client) error) error {
	c, err := util.GetMilvusClientManager().GetMilvusClient(ctx, t.config.URI, t.config.Token, database, t.config.DialConfig)
	if err != nil {
		log.Warn("fail to get milvus client", zap.Error(err))
		return err
	}
	err = f(c)
	if status.Code(err) == codes.Canceled {
		util.GetMilvusClientManager().DeleteMilvusClient(t.config.URI, database)
		log.Warn("grpc: the client connection is closing, waiting...", zap.Error(err))
		time.Sleep(resource.DefaultExpiration)
	}
	return err
}

func (t *TargetClient) GetCollectionInfo(ctx context.Context, collectionName, databaseName string) (*model.CollectionInfo, error) {
	databaseName, err := t.GetDatabaseName(ctx, collectionName, databaseName)
	if err != nil {
		log.Warn("fail to get database name", zap.Error(err))
		return nil, err
	}

	collectionInfo := &model.CollectionInfo{}
	dbName, colName := t.mapDBAndCollectionName(databaseName, collectionName)
	err = t.milvusOp(ctx, dbName, func(milvus client.Client) error {
		collection, err := milvus.DescribeCollection(ctx, colName)
		if err != nil {
			return err
		}
		collectionInfo.DatabaseName = databaseName
		collectionInfo.CollectionID = collection.ID
		collectionInfo.CollectionName = collectionName
		collectionInfo.PChannels = collection.PhysicalChannels
		collectionInfo.VChannels = collection.VirtualChannels
		return nil
	})
	if err != nil {
		log.Warn("fail to get collection info", zap.Error(err))
		return nil, err
	}

	tmpCollectionInfo, err := t.GetPartitionInfo(ctx, collectionName, databaseName)
	if err != nil {
		log.Warn("fail to get partition info", zap.Error(err))
		return nil, err
	}
	collectionInfo.Partitions = tmpCollectionInfo.Partitions
	return collectionInfo, nil
}

func (t *TargetClient) GetPartitionInfo(ctx context.Context, collectionName, databaseName string) (*model.CollectionInfo, error) {
	var err error
	databaseName, err = t.GetDatabaseName(ctx, collectionName, databaseName)
	if err != nil {
		log.Warn("fail to get database name", zap.Error(err))
		return nil, err
	}
	collectionInfo := &model.CollectionInfo{}
	var partition []*entity.Partition
	dbName, colName := t.mapDBAndCollectionName(databaseName, collectionName)
	err = t.milvusOp(ctx, dbName, func(milvus client.Client) error {
		partition, err = milvus.ShowPartitions(ctx, colName)
		if err != nil {
			return err
		}
		if len(partition) == 0 {
			log.Warn("failed to show partitions", zap.Error(err))
			return errors.New("fail to show the partitions")
		}
		return nil
	})
	if err != nil {
		log.Warn("fail to show partitions", zap.Error(err))
		return nil, err
	}

	partitionInfo := make(map[string]int64, len(partition))
	for _, e := range partition {
		partitionInfo[e.Name] = e.ID
	}
	collectionInfo.Partitions = partitionInfo
	return collectionInfo, nil
}

func (t *TargetClient) GetDatabaseName(ctx context.Context, collectionName, databaseName string) (string, error) {
	if !IsDroppedObject(databaseName) {
		return databaseName, nil
	}
	dbLog := log.With(zap.String("collection", collectionName), zap.String("database", databaseName))

	var databaseNames []entity.Database
	var err error

	err = t.milvusOp(ctx, "", func(milvus client.Client) error {
		databaseNames, err = milvus.ListDatabases(ctx)
		return err
	})
	if err != nil {
		dbLog.Warn("fail to list databases", zap.Error(err))
		return "", err
	}

	for _, dbName := range databaseNames {
		var collections []*entity.Collection
		err = t.milvusOp(ctx, dbName.Name, func(dbMilvus client.Client) error {
			collections, err = dbMilvus.ListCollections(ctx)
			return err
		})
		if err != nil {
			dbLog.Warn("fail to list collections", zap.Error(err))
			return "", err
		}

		for _, collection := range collections {
			if collection.Name == collectionName {
				return dbName.Name, nil
			}
		}
	}
	dbLog.Warn("not found the database", zap.Any("databases", databaseNames))
	return "", util.NotFoundDatabase
}

func (t *TargetClient) UpdateNameMappings(nameMappings map[string]string) {
	for k, v := range nameMappings {
		t.nameMappings.Store(k, v)
	}
}

func (t *TargetClient) mapDBAndCollectionName(db, collection string) (string, string) {
	if db == "" {
		db = util.DefaultDbName
	}
	returnDB, returnCollection := db, collection
	t.nameMappings.Range(func(source, target string) bool {
		sourceDB, sourceCollection := util.GetCollectionNameFromFull(source)
		if sourceDB == db && sourceCollection == collection {
			returnDB, returnCollection = util.GetCollectionNameFromFull(target)
			return false
		}
		if sourceDB == db && (sourceCollection == "*" || collection == "") {
			returnDB, _ = util.GetCollectionNameFromFull(target)
			return false
		}
		return true
	})
	return returnDB, returnCollection
}
