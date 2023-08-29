package store

import (
	"context"

	"github.com/zilliztech/milvus-cdc/core/util"
	"github.com/zilliztech/milvus-cdc/server/model/meta"
)

// MetaStore M -> MetaObject, T -> txn Object
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

var log = util.Log
