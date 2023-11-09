package api

import (
	"context"
	"testing"
)

func TestDefaultDataHandler_CreateCollection(t *testing.T) {
	type args struct {
		ctx   context.Context
		param *CreateCollectionParam
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "TestDefaultDataHandler_CreateCollection",
			args:    args{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultDataHandler{}
			if err := d.CreateCollection(tt.args.ctx, tt.args.param); (err != nil) != tt.wantErr {
				t.Errorf("CreateCollection() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDefaultDataHandler_CreateDatabase(t *testing.T) {
	type args struct {
		ctx   context.Context
		param *CreateDatabaseParam
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "TestDefaultDataHandler_CreateDatabase",
			args:    args{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultDataHandler{}
			if err := d.CreateDatabase(tt.args.ctx, tt.args.param); (err != nil) != tt.wantErr {
				t.Errorf("CreateDatabase() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDefaultDataHandler_CreateIndex(t *testing.T) {
	type args struct {
		ctx   context.Context
		param *CreateIndexParam
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "TestDefaultDataHandler_CreateIndex",
			args:    args{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultDataHandler{}
			if err := d.CreateIndex(tt.args.ctx, tt.args.param); (err != nil) != tt.wantErr {
				t.Errorf("CreateIndex() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDefaultDataHandler_CreatePartition(t *testing.T) {
	type args struct {
		ctx   context.Context
		param *CreatePartitionParam
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "TestDefaultDataHandler_CreatePartition",
			args:    args{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultDataHandler{}
			if err := d.CreatePartition(tt.args.ctx, tt.args.param); (err != nil) != tt.wantErr {
				t.Errorf("CreatePartition() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDefaultDataHandler_Delete(t *testing.T) {
	type args struct {
		ctx   context.Context
		param *DeleteParam
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "TestDefaultDataHandler_Delete",
			args:    args{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultDataHandler{}
			if err := d.Delete(tt.args.ctx, tt.args.param); (err != nil) != tt.wantErr {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDefaultDataHandler_DescribeCollection(t *testing.T) {
	type args struct {
		ctx   context.Context
		param *DescribeCollectionParam
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "TestDefaultDataHandler_DescribeCollection",
			args:    args{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultDataHandler{}
			if err := d.DescribeCollection(tt.args.ctx, tt.args.param); (err != nil) != tt.wantErr {
				t.Errorf("DescribeCollection() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDefaultDataHandler_DropCollection(t *testing.T) {
	type args struct {
		ctx   context.Context
		param *DropCollectionParam
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "TestDefaultDataHandler_DropCollection",
			args:    args{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultDataHandler{}
			if err := d.DropCollection(tt.args.ctx, tt.args.param); (err != nil) != tt.wantErr {
				t.Errorf("DropCollection() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDefaultDataHandler_DropDatabase(t *testing.T) {
	type args struct {
		ctx   context.Context
		param *DropDatabaseParam
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "TestDefaultDataHandler_DropDatabase",
			args:    args{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultDataHandler{}
			if err := d.DropDatabase(tt.args.ctx, tt.args.param); (err != nil) != tt.wantErr {
				t.Errorf("DropDatabase() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDefaultDataHandler_DropIndex(t *testing.T) {
	type args struct {
		ctx   context.Context
		param *DropIndexParam
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "TestDefaultDataHandler_DropIndex",
			args:    args{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultDataHandler{}
			if err := d.DropIndex(tt.args.ctx, tt.args.param); (err != nil) != tt.wantErr {
				t.Errorf("DropIndex() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDefaultDataHandler_DropPartition(t *testing.T) {
	type args struct {
		ctx   context.Context
		param *DropPartitionParam
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "TestDefaultDataHandler_DropPartition",
			args:    args{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultDataHandler{}
			if err := d.DropPartition(tt.args.ctx, tt.args.param); (err != nil) != tt.wantErr {
				t.Errorf("DropPartition() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDefaultDataHandler_Flush(t *testing.T) {
	type args struct {
		ctx   context.Context
		param *FlushParam
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "TestDefaultDataHandler_Flush",
			args:    args{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultDataHandler{}
			if err := d.Flush(tt.args.ctx, tt.args.param); (err != nil) != tt.wantErr {
				t.Errorf("Flush() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDefaultDataHandler_Insert(t *testing.T) {
	type args struct {
		ctx   context.Context
		param *InsertParam
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "TestDefaultDataHandler_Insert",
			args: args{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultDataHandler{}
			if err := d.Insert(tt.args.ctx, tt.args.param); (err != nil) != tt.wantErr {
				t.Errorf("Insert() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDefaultDataHandler_LoadCollection(t *testing.T) {
	type args struct {
		ctx   context.Context
		param *LoadCollectionParam
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "TestDefaultDataHandler_LoadCollection",
			args:    args{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultDataHandler{}
			if err := d.LoadCollection(tt.args.ctx, tt.args.param); (err != nil) != tt.wantErr {
				t.Errorf("LoadCollection() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDefaultDataHandler_ReleaseCollection(t *testing.T) {
	type args struct {
		ctx   context.Context
		param *ReleaseCollectionParam
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "TestDefaultDataHandler_ReleaseCollection",
			args:    args{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultDataHandler{}
			if err := d.ReleaseCollection(tt.args.ctx, tt.args.param); (err != nil) != tt.wantErr {
				t.Errorf("ReleaseCollection() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDefaultDataHandler_ReplicateMessage(t *testing.T) {
	type args struct {
		ctx   context.Context
		param *ReplicateMessageParam
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "TestDefaultDataHandler_ReplicateMessage",
			args:    args{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultDataHandler{}
			if err := d.ReplicateMessage(tt.args.ctx, tt.args.param); (err != nil) != tt.wantErr {
				t.Errorf("ReplicateMessage() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDefaultDataHandler_DescribeDatabase(t *testing.T) {
	type args struct {
		ctx   context.Context
		param *DescribeDatabaseParam
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "TestDefaultDataHandler_DescribeDatabase",
			args:    args{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultDataHandler{}
			if err := d.DescribeDatabase(tt.args.ctx, tt.args.param); (err != nil) != tt.wantErr {
				t.Errorf("DescribeDatabase() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
