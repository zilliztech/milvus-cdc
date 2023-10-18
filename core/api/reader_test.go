package api

import (
	"context"
	"testing"
)

func TestDefaultReader_QuitRead(t *testing.T) {
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "TestDefaultReader_QuitRead",
			args: args{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultReader{}
			d.QuitRead(tt.args.ctx)
		})
	}
}

func TestDefaultReader_StartRead(t *testing.T) {
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "TestDefaultReader_StartRead",
			args: args{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultReader{}
			d.StartRead(tt.args.ctx)
		})
	}
}
