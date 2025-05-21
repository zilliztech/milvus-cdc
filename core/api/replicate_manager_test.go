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
	"reflect"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"

	pb "github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/zilliztech/milvus-cdc/core/model"
)

func TestDefaultChannelManager_AddPartition(t *testing.T) {
	type args struct {
		ctx            context.Context
		collectionInfo *pb.CollectionInfo
		partitionInfo  *pb.PartitionInfo
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "TestDefaultChannelManager_AddPartition",
			args:    args{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultChannelManager{}
			if err := d.AddPartition(tt.args.ctx, nil, tt.args.collectionInfo, tt.args.partitionInfo); (err != nil) != tt.wantErr {
				t.Errorf("AddPartition() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDefaultChannelManager_GetChannelChan(t *testing.T) {
	tests := []struct {
		name string
		want <-chan string
	}{
		{
			name: "TestDefaultChannelManager_GetChannelChan",
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultChannelManager{}
			if got := d.GetChannelChan(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetChannelChan() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDefaultChannelManager_GetEventChan(t *testing.T) {
	tests := []struct {
		name string
		want <-chan *ReplicateAPIEvent
	}{
		{
			name: "TestDefaultChannelManager_GetEventChan",
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultChannelManager{}
			if got := d.GetEventChan(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetEventChan() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDefaultChannelManager_GetMsgChan(t *testing.T) {
	type args struct {
		pChannel string
	}
	tests := []struct {
		name string
		args args
		want <-chan *ReplicateMsg
	}{
		{
			name: "TestDefaultChannelManager_GetMsgChan",
			args: args{},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultChannelManager{}
			if got := d.GetMsgChan(tt.args.pChannel); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetMsgChan() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDefaultChannelManager_StartReadCollection(t *testing.T) {
	type args struct {
		ctx           context.Context
		info          *pb.CollectionInfo
		seekPositions []*msgpb.MsgPosition
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "TestDefaultChannelManager_StartReadCollection",
			args:    args{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultChannelManager{}
			if err := d.StartReadCollection(tt.args.ctx, nil, tt.args.info, tt.args.seekPositions, nil); (err != nil) != tt.wantErr {
				t.Errorf("StartReadCollection() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDefaultChannelManager_StopReadCollection(t *testing.T) {
	type args struct {
		ctx  context.Context
		info *pb.CollectionInfo
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "TestDefaultChannelManager_StopReadCollection",
			args:    args{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultChannelManager{}
			if err := d.StopReadCollection(tt.args.ctx, tt.args.info); (err != nil) != tt.wantErr {
				t.Errorf("StopReadCollection() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDefaultTargetAPI_GetCollectionInfo(t *testing.T) {
	type args struct {
		ctx            context.Context
		collectionName string
	}
	tests := []struct {
		name    string
		args    args
		want    *model.CollectionInfo
		wantErr bool
	}{
		{
			name:    "TestDefaultTargetAPI_GetCollectionInfo",
			args:    args{},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultTargetAPI{}
			got, err := d.GetCollectionInfo(tt.args.ctx, tt.args.collectionName, "")
			if (err != nil) != tt.wantErr {
				t.Errorf("GetCollectionInfo() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetCollectionInfo() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDefaultTargetAPI_GetPartitionInfo(t *testing.T) {
	type args struct {
		ctx            context.Context
		collectionName string
	}
	tests := []struct {
		name    string
		args    args
		want    *model.CollectionInfo
		wantErr bool
	}{
		{
			name:    "TestDefaultTargetAPI_GetPartitionInfo",
			args:    args{},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultTargetAPI{}
			got, err := d.GetPartitionInfo(tt.args.ctx, tt.args.collectionName, "")
			if (err != nil) != tt.wantErr {
				t.Errorf("GetPartitionInfo() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetPartitionInfo() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDefaultChannelManager_SetCtx(t *testing.T) {
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "TestDefaultChannelManager_SetCtx",
			args: args{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultChannelManager{}
			d.SetCtx(tt.args.ctx)
		})
	}
}

func TestReplicateAPIEventType_String(t *testing.T) {
	tests := []struct {
		name string
		r    ReplicateAPIEventType
		want string
	}{
		{
			name: "TestReplicateAPIEventType_create_collection",
			r:    ReplicateCreateCollection,
			want: "CreateCollection",
		},
		{
			name: "TestReplicateAPIEventType_drop_collection",
			r:    ReplicateDropCollection,
			want: "DropCollection",
		},
		{
			name: "TestReplicateAPIEventType_create_partition",
			r:    ReplicateCreatePartition,
			want: "CreatePartition",
		},
		{
			name: "TestReplicateAPIEventType_drop_partition",
			r:    ReplicateDropPartition,
			want: "DropPartition",
		},
		{
			name: "TestReplicateAPIEventType_add_replica",
			r:    ReplicateAPIEventType(-1),
			want: "Unknown",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.r.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}
