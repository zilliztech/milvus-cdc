package server

import (
	"encoding/json"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/stretchr/testify/mock"
	"github.com/zilliztech/milvus-cdc/server/mocks"
	"github.com/zilliztech/milvus-cdc/server/model/meta"
	"go.uber.org/zap"
)

func TestJson(t *testing.T) {
	b, err := json.Marshal(make(map[string]string))
	log.Info(string(b), zap.Error(err))
}

func TestWriterCallback(t *testing.T) {
	factory := mocks.NewMetaStoreFactory(t)
	store := mocks.NewMetaStore[*meta.TaskCollectionPosition](t)
	callback := NewWriteCallback(factory, "test", "12345")

	t.Run("fail", func(t *testing.T) {
		factory.EXPECT().GetTaskCollectionPositionMetaStore(mock.Anything).Return(store).Once()
		store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("test")).Once()
		callback.UpdateTaskCollectionPosition(1, "test", "test", &meta.PositionInfo{
			Time: 1,
			DataPair: &commonpb.KeyDataPair{
				Key:  "test",
				Data: []byte("test"),
			},
		}, nil, nil)
	})
	t.Run("success", func(t *testing.T) {
		factory.EXPECT().GetTaskCollectionPositionMetaStore(mock.Anything).Return(store).Once()
		store.EXPECT().Get(mock.Anything, mock.Anything, mock.Anything).Return([]*meta.TaskCollectionPosition{}, nil).Once()
		store.EXPECT().Put(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		callback.UpdateTaskCollectionPosition(1, "test", "test", &meta.PositionInfo{
			Time: 1,
			DataPair: &commonpb.KeyDataPair{
				Key:  "test",
				Data: []byte("test"),
			},
		}, nil, nil)
	})
}
