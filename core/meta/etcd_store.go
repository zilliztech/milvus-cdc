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

package meta

import (
	"context"
	"encoding/json"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/zilliztech/milvus-cdc/core/api"
)

type EtcdReplicateStore struct {
	client   *clientv3.Client
	rootPath string
}

func NewEtcdReplicateStore(endpoints []string, rootPath string) (*EtcdReplicateStore, error) {
	cfg := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	}
	client, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}
	return &EtcdReplicateStore{
		client:   client,
		rootPath: rootPath,
	}, nil
}

func (s *EtcdReplicateStore) Get(ctx context.Context, key string, withPrefix bool) ([]api.MetaMsg, error) {
	var result []api.MetaMsg

	var resp *clientv3.GetResponse
	var err error

	if withPrefix {
		resp, err = s.client.Get(ctx, s.rootPath+"/"+key, clientv3.WithPrefix())
	} else {
		resp, err = s.client.Get(ctx, s.rootPath+"/"+key)
	}

	if err != nil {
		return nil, err
	}

	for _, kv := range resp.Kvs {
		var msg api.MetaMsg
		if err := json.Unmarshal(kv.Value, &msg); err != nil {
			return nil, err
		}
		result = append(result, msg)
	}

	return result, nil
}

func (s *EtcdReplicateStore) Put(ctx context.Context, key string, value api.MetaMsg) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	_, err = s.client.Put(ctx, s.rootPath+"/"+key, string(data))
	return err
}

func (s *EtcdReplicateStore) Remove(ctx context.Context, key string) error {
	_, err := s.client.Delete(ctx, s.rootPath+"/"+key)
	return err
}
