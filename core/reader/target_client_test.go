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
	"log"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"

	mocks "github.com/zilliztech/milvus-cdc/core/servermocks"
)

func TestTargetClient(t *testing.T) {
	listen, err := net.Listen("tcp", ":50052")
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
		log.Println("Server started on port 50052")
		if err := server.Serve(listen); err != nil {
			log.Println("server error", err)
		}
	}()
	time.Sleep(time.Second)
	defer listen.Close()
	targetClient, err := NewTarget(context.Background(), TargetConfig{
		URI: "localhost:50052",
	})
	assert.NoError(t, err)
	realTarget := targetClient.(*TargetClient)

	{
		realTarget.config.URI = ""
		// error with db
		{
			_, err := targetClient.GetCollectionInfo(context.Background(), "test", "foo")
			assert.Error(t, err)
		}
		{
			_, err := targetClient.GetPartitionInfo(context.Background(), "test", "foo")
			assert.Error(t, err)
		}

		realTarget.config.URI = "localhost:50052"
	}

	{
		milvusService.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: 500,
			},
		}, nil).Once()
		_, err := targetClient.GetCollectionInfo(context.Background(), "test", "")
		assert.Error(t, err)
	}

	{
		milvusService.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
			Status:               &commonpb.Status{},
			CollectionID:         1001,
			VirtualChannelNames:  []string{"t1-v1", "t2-v2"},
			PhysicalChannelNames: []string{"t1", "t2"},
		}, nil).Once()
		milvusService.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(&milvuspb.ShowPartitionsResponse{
			Status: &commonpb.Status{
				ErrorCode: 500,
			},
		}, nil).Once()
		_, err := targetClient.GetCollectionInfo(context.Background(), "test", "")
		assert.Error(t, err)
	}

	{
		milvusService.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
			Status:               &commonpb.Status{},
			CollectionID:         1001,
			VirtualChannelNames:  []string{"t1-v1", "t2-v2"},
			PhysicalChannelNames: []string{"t1", "t2"},
		}, nil).Once()
		milvusService.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(&milvuspb.ShowPartitionsResponse{
			Status:         &commonpb.Status{},
			PartitionIDs:   []int64{1, 2},
			PartitionNames: []string{"p1", "p2"},
		}, nil).Once()
		_, err := targetClient.GetCollectionInfo(context.Background(), "test", "")
		assert.NoError(t, err)
	}

	{
		milvusService.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
			Status:               &commonpb.Status{},
			CollectionID:         1001,
			VirtualChannelNames:  []string{"t1-v1", "t2-v2"},
			PhysicalChannelNames: []string{"t1", "t2"},
		}, nil).Once()
		milvusService.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(&milvuspb.ShowPartitionsResponse{
			Status:         &commonpb.Status{},
			PartitionIDs:   []int64{1, 2},
			PartitionNames: []string{"p1", "p2"},
		}, nil).Once()
		_, err := targetClient.GetCollectionInfo(context.Background(), "test", "foo")
		assert.NoError(t, err)
	}
}
