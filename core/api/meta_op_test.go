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

	pb "github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/zilliztech/milvus-cdc/core/model"
)

func TestDefaultMetaOp_GetAllCollection(t *testing.T) {
	type args struct {
		ctx    context.Context
		filter CollectionFilter
	}
	tests := []struct {
		name    string
		args    args
		want    []*pb.CollectionInfo
		wantErr bool
	}{
		{
			name:    "TestDefaultMetaOp_GetAllCollection",
			args:    args{},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultMetaOp{}
			got, err := d.GetAllCollection(tt.args.ctx, tt.args.filter)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetAllCollection() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetAllCollection() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDefaultMetaOp_GetAllPartition(t *testing.T) {
	type args struct {
		ctx    context.Context
		filter PartitionFilter
	}
	tests := []struct {
		name    string
		args    args
		want    []*pb.PartitionInfo
		wantErr bool
	}{
		{
			name:    "TestDefaultMetaOp_GetAllPartition",
			args:    args{},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultMetaOp{}
			got, err := d.GetAllPartition(tt.args.ctx, tt.args.filter)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetAllPartition() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetAllPartition() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDefaultMetaOp_GetCollectionNameByID(t *testing.T) {
	type args struct {
		ctx context.Context
		id  int64
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "TestDefaultMetaOp_GetCollectionNameByID",
			args: args{},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultMetaOp{}
			if got := d.GetCollectionNameByID(tt.args.ctx, tt.args.id); got != tt.want {
				t.Errorf("GetCollectionNameByID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDefaultMetaOp_SubscribeCollectionEvent(t *testing.T) {
	type args struct {
		taskID   string
		consumer CollectionEventConsumer
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "TestDefaultMetaOp_SubscribeCollectionEvent",
			args: args{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultMetaOp{}
			d.SubscribeCollectionEvent(tt.args.taskID, tt.args.consumer)
		})
	}
}

func TestDefaultMetaOp_SubscribePartitionEvent(t *testing.T) {
	type args struct {
		taskID   string
		consumer PartitionEventConsumer
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "TestDefaultMetaOp_SubscribePartitionEvent",
			args: args{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultMetaOp{}
			d.SubscribePartitionEvent(tt.args.taskID, tt.args.consumer)
		})
	}
}

func TestDefaultMetaOp_UnsubscribeEvent(t *testing.T) {
	type args struct {
		taskID    string
		eventType WatchEventType
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "TestDefaultMetaOp_UnsubscribeEvent",
			args: args{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultMetaOp{}
			d.UnsubscribeEvent(tt.args.taskID, tt.args.eventType)
		})
	}
}

func TestDefaultMetaOp_WatchCollection(t *testing.T) {
	type args struct {
		ctx    context.Context
		filter CollectionFilter
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "TestDefaultMetaOp_WatchCollection",
			args: args{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultMetaOp{}
			d.WatchCollection(tt.args.ctx, tt.args.filter)
		})
	}
}

func TestDefaultMetaOp_WatchPartition(t *testing.T) {
	type args struct {
		ctx    context.Context
		filter PartitionFilter
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "TestDefaultMetaOp_WatchPartition",
			args: args{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultMetaOp{}
			d.WatchPartition(tt.args.ctx, tt.args.filter)
		})
	}
}

func TestDefaultMetaOp_GetDatabaseInfoForCollection(t *testing.T) {
	type args struct {
		ctx context.Context
		id  int64
	}
	tests := []struct {
		name string
		args args
		want model.DatabaseInfo
	}{
		{
			name: "TestDefaultMetaOp_GetDatabaseInfoForCollection",
			args: args{},
			want: model.DatabaseInfo{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultMetaOp{}
			if got := d.GetDatabaseInfoForCollection(tt.args.ctx, tt.args.id); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetDatabaseInfoForCollection() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDefaultMetaOp_GetAllDroppedObj(t *testing.T) {
	tests := []struct {
		name string
		want map[string]map[string]uint64
	}{
		{
			name: "TestDefaultMetaOp_GetAllDroppedObj",
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultMetaOp{}
			if got := d.GetAllDroppedObj(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetAllDroppedObj() = %v, want %v", got, tt.want)
			}
		})
	}
}
