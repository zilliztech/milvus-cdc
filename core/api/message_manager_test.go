package api

import "testing"

func TestDefaultMessageManager_Close(t *testing.T) {
	type args struct {
		channelName string
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "TestDefaultMessageManager_Close",
			args: args{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultMessageManager{}
			d.Close(tt.args.channelName)
		})
	}
}

func TestDefaultMessageManager_ReplicateMessage(t *testing.T) {
	type args struct {
		message *ReplicateMessage
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "TestDefaultMessageManager_ReplicateMessage",
			args: args{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultMessageManager{}
			d.ReplicateMessage(tt.args.message)
		})
	}
}
