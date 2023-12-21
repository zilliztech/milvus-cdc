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

func TestDefaultReader_ErrorChan(t *testing.T) {
	tests := []struct {
		name string
		want <-chan error
	}{
		{
			name: "TestDefaultReader_ErrorChan",
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DefaultReader{}
			if got := d.ErrorChan(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ErrorChan() = %v, want %v", got, tt.want)
			}
		})
	}
}
