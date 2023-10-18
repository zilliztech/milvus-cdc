package api

import (
	"context"
	"reflect"
	"testing"

	"github.com/milvus-io/milvus/pkg/mq/msgstream"
)

func TestDefaultWriter_HandleOpMessagePack(t *testing.T) {
	type args struct {
		ctx     context.Context
		msgPack *msgstream.MsgPack
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name:    "TestDefaultWriter_HandleOpMessagePack",
			args:    args{},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultWriter{}
			got, err := d.HandleOpMessagePack(tt.args.ctx, tt.args.msgPack)
			if (err != nil) != tt.wantErr {
				t.Errorf("HandleOpMessagePack() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HandleOpMessagePack() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDefaultWriter_HandleReplicateAPIEvent(t *testing.T) {
	type args struct {
		ctx      context.Context
		apiEvent *ReplicateAPIEvent
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "TestDefaultWriter_HandleReplicateAPIEvent",
			args:    args{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultWriter{}
			if err := d.HandleReplicateAPIEvent(tt.args.ctx, tt.args.apiEvent); (err != nil) != tt.wantErr {
				t.Errorf("HandleReplicateAPIEvent() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDefaultWriter_HandleReplicateMessage(t *testing.T) {
	type args struct {
		ctx         context.Context
		channelName string
		msgPack     *msgstream.MsgPack
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		want1   []byte
		wantErr bool
	}{
		{
			name:    "TestDefaultWriter_HandleReplicateMessage",
			args:    args{},
			want:    nil,
			want1:   nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultWriter{}
			got, got1, err := d.HandleReplicateMessage(tt.args.ctx, tt.args.channelName, tt.args.msgPack)
			if (err != nil) != tt.wantErr {
				t.Errorf("HandleReplicateMessage() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HandleReplicateMessage() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("HandleReplicateMessage() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
