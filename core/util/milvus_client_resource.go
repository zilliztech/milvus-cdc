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

package util

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus/pkg/util/resource"

	"github.com/zilliztech/milvus-cdc/core/log"
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

func (m *MilvusClientResourceManager) newMilvusClient(ctx context.Context, address, token, database string, dialConfig DialConfig) resource.NewResourceFunc {
	return func() (resource.Resource, error) {
		dialOptions, err := GetDialOptions(dialConfig)
		if err != nil {
			log.Warn("fail to get dial options", zap.Error(err))
			return nil, err
		}
		enableTLS := strings.Contains(address, "https://")
		c, err := client.NewClient(ctx, client.Config{
			Address:       address,
			APIKey:        token,
			EnableTLSAuth: enableTLS,
			DBName:        database,
			DialOptions:   dialOptions,
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

func (m *MilvusClientResourceManager) GetMilvusClient(ctx context.Context, address, token, database string, dialConfig DialConfig) (client.Client, error) {
	if database == "" {
		database = DefaultDbName
	}
	ctxLog := log.Ctx(ctx).With(zap.String("database", database), zap.String("address", address))
	res, err := m.manager.Get(MilvusClientResourceTyp,
		getMilvusClientResourceName(address, database),
		m.newMilvusClient(ctx, address, token, database, dialConfig))
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

func (m *MilvusClientResourceManager) DeleteMilvusClient(address, database string) {
	_ = m.manager.Delete(MilvusClientResourceTyp, getMilvusClientResourceName(address, database))
}

func getMilvusClientResourceName(address, database string) string {
	return fmt.Sprintf("%s:%s", address, database)
}

func GetToken(username, password string) string {
	return fmt.Sprintf("%s:%s", username, password)
}

func GetURI(address string, port int, enableTLS bool) string {
	if strings.Contains(address, "http://") || strings.Contains(address, "https://") {
		return address
	}
	if enableTLS {
		return fmt.Sprintf("https://%s:%d", address, port)
	}
	return fmt.Sprintf("http://%s:%d", address, port)
}
