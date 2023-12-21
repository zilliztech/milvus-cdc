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

	"github.com/zilliztech/milvus-cdc/server/model/meta"
)

// MetaStore M -> MetaObject, T -> txn Object
//
//go:generate mockery --name=MetaStore --filename=meta_store_mock.go --output=../mocks
type MetaStore[M any] interface {
	Put(ctx context.Context, metaObj M, txn any) error
	Get(ctx context.Context, metaObj M, txn any) ([]M, error)
	Delete(ctx context.Context, metaObj M, txn any) error
}

//go:generate mockery --name=MetaStoreFactory --filename=meta_store_factory_mock.go --output=../mocks
type MetaStoreFactory interface {
	GetTaskInfoMetaStore(ctx context.Context) MetaStore[*meta.TaskInfo]
	GetTaskCollectionPositionMetaStore(ctx context.Context) MetaStore[*meta.TaskCollectionPosition]
	// Txn return commit function and error
	Txn(ctx context.Context) (any, func(err error) error, error)
}
