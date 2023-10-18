package api

import (
	"context"
	"reflect"
	"testing"

	"github.com/zilliztech/milvus-cdc/core/pb"
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
