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
	"log"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/util/resource"

	mocks "github.com/zilliztech/milvus-cdc/core/servermocks"
)

func TestNewMilvusClient(t *testing.T) {
	t.Run("GetMilvusClientResourceName", func(t *testing.T) {
		address := "localhost:19530"
		database := "default"
		assert.Equal(t, fmt.Sprintf("%s:%s", address, database), getMilvusClientResourceName(address, database))
	})

	t.Run("GetToken", func(t *testing.T) {
		username := "foo"
		password := "hoo"
		assert.Equal(t, fmt.Sprintf("%s:%s", username, password), GetToken(username, password))
	})

	t.Run("GetMilvusClient", func(t *testing.T) {
		resourceManager := GetMilvusClientManager()
		// error client address
		{
			timeCtx, cancelFunc := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancelFunc()
			_, err := resourceManager.GetMilvusClient(timeCtx, "localhost:19530", "foo", "", DialConfig{})
			assert.Error(t, err)
		}

		// invalid resource
		{
			address := "localhost:19530"
			database := "default"
			_, _ = resourceManager.manager.Get(MilvusClientResourceTyp, getMilvusClientResourceName(address, database), func() (resource.Resource, error) {
				return resource.NewSimpleResource("hello", MilvusClientResourceTyp, fmt.Sprintf("%s:%s", address, database), MilvusClientExpireTime, func() {}), nil
			})

			_, err := resourceManager.GetMilvusClient(context.Background(), "localhost:19530", "foo", "", DialConfig{})
			assert.Error(t, err)

			assert.Eventually(t, func() bool {
				return resourceManager.manager.Delete(MilvusClientResourceTyp, getMilvusClientResourceName(address, database)) == nil
			}, resource.DefaultExpiration+resource.DefaultCheckInterval, time.Second)
		}

		// success
		{
			listen, err := net.Listen("tcp", ":50051")
			if err != nil {
				log.Fatalf("failed to listen: %v", err)
			}

			server := grpc.NewServer()
			milvusService := mocks.NewMilvusServiceServer(t)
			milvusService.EXPECT().Connect(mock.Anything, mock.Anything).Return(&milvuspb.ConnectResponse{
				Status: &commonpb.Status{},
			}, nil)
			milvuspb.RegisterMilvusServiceServer(server, milvusService)

			go func() {
				log.Println("Server started on port 50051")
				if err := server.Serve(listen); err != nil {
					log.Println("server error", err)
				}
			}()
			time.Sleep(time.Second)
			defer listen.Close()

			{
				address := "localhost:50051"
				database := "foo"
				_, err := resourceManager.GetMilvusClient(context.Background(), address, "foo", database, DialConfig{})
				assert.NoError(t, err)

				assert.Eventually(t, func() bool {
					return resourceManager.manager.Delete(MilvusClientResourceTyp, getMilvusClientResourceName(address, database)) == nil
				}, resource.DefaultExpiration, time.Second)
			}
		}
	})
}
