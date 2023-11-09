package util

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/resource"
	"go.uber.org/zap"
)

const (
	MilvusClientResourceTyp = "milvus_client"
	MilvusClientExpireTime  = 30 * time.Second
	DefaultDbName           = "default"
)

var (
	clientManager     *MilvusClientResourceManager
	clientManagerOnce sync.Once
)

type MilvusClientResourceManager struct {
	manager resource.Manager
}

func GetMilvusClientManager() *MilvusClientResourceManager {
	clientManagerOnce.Do(func() {
		manager := resource.NewManager(0, 0, nil)
		clientManager = &MilvusClientResourceManager{
			manager: manager,
		}
	})
	return clientManager
}

func (m *MilvusClientResourceManager) newMilvusClient(ctx context.Context, address, apiKey, database string, enableTLS bool) resource.NewResourceFunc {
	return func() (resource.Resource, error) {
		c, err := client.NewClient(ctx, client.Config{
			Address:       address,
			APIKey:        apiKey,
			EnableTLSAuth: enableTLS,
			DBName:        database,
		})
		if err != nil {
			log.Warn("fail to new the milvus client", zap.String("database", database), zap.String("address", address), zap.Error(err))
			return nil, err
		}

		res := resource.NewSimpleResource(c, MilvusClientResourceTyp, fmt.Sprintf("%s:%s", address, database), MilvusClientExpireTime, func() {
			_ = c.Close()
		})

		return res, nil
	}
}

func (m *MilvusClientResourceManager) GetMilvusClient(ctx context.Context, address, apiKey, database string, enableTLS bool) (client.Client, error) {
	if database == "" {
		database = DefaultDbName
	}
	ctxLog := log.Ctx(ctx).With(zap.String("database", database), zap.String("address", address))
	res, err := m.manager.Get(MilvusClientResourceTyp,
		getMilvusClientResourceName(address, database),
		m.newMilvusClient(ctx, address, apiKey, database, enableTLS))
	if err != nil {
		ctxLog.Error("fail to get milvus client", zap.Error(err))
		return nil, err
	}
	if obj, ok := res.Get().(client.Client); ok && obj != nil {
		return obj, nil
	}
	ctxLog.Warn("invalid resource object", zap.Any("obj", reflect.TypeOf(res.Get())))
	return nil, errors.New("invalid resource object")
}

func getMilvusClientResourceName(address, database string) string {
	return fmt.Sprintf("%s:%s", address, database)
}

func GetAPIKey(username, password string) string {
	return fmt.Sprintf("%s:%s", username, password)
}
