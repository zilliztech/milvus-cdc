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

package tool

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	pb "github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"

	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/reader"
)

type CollectionNameInfo struct {
	DBName         string
	CollectionName string
}

func GetCollectionInfo(c config.EtcdServerConfig, nameInfo CollectionNameInfo) (*pb.CollectionInfo, error) {
	etcdOP, err := reader.NewEtcdOp(c, "", config.EtcdRetryConfig{
		Retry: config.RetrySettings{
			RetryTimes: 1,
		},
	}, nil)
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	if nameInfo.DBName == "" {
		nameInfo.DBName = "default"
	}
	collectionInfos, err := etcdOP.GetAllCollection(ctx, func(info *pb.CollectionInfo) bool {
		if nameInfo.CollectionName != info.GetSchema().GetName() {
			return true
		}
		return false
	})
	if err != nil {
		return nil, err
	}
	if len(collectionInfos) == 0 {
		return nil, errors.New("collection not found")
	}
	for _, info := range collectionInfos {
		databaseInfo := etcdOP.GetDatabaseInfoForCollection(ctx, info.GetID())
		if databaseInfo.Name == nameInfo.DBName {
			return info, nil
		}
	}
	return nil, errors.New("collection not found, because of database not match")
}

func GetPkField(schema *schemapb.CollectionSchema) *schemapb.FieldSchema {
	for _, field := range schema.GetFields() {
		if field.GetIsPrimaryKey() {
			return field
		}
	}
	return nil
}
