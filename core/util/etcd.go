// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/retry"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

var (
	// TODO config

	EtcdOpTimeout        = 10 * time.Second
	EtcdOpRetryTime uint = 5
)

//go:generate mockery --name=KVApi --filename=kv_api_mock.go --output=../mocks --with-expecter
type KVApi interface {
	clientv3.KV
	clientv3.Watcher
	// Status From clientv3.Maintenance interface
	Status(ctx context.Context, endpoint string) (*clientv3.StatusResponse, error)
	Endpoints() []string
}

var newEtcdClient = func(cfg clientv3.Config) (KVApi, error) {
	return clientv3.New(cfg)
}

func MockEtcdClient(new func(cfg clientv3.Config) (KVApi, error), f func()) {
	origin := newEtcdClient
	newEtcdClient = new
	defer func() {
		newEtcdClient = origin
	}()
	f()
}

func GetEtcdClient(endpoints []string) (KVApi, error) {
	etcdCli, err := newEtcdClient(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
		Logger:      log.L(),
	})
	errLog := log.With(zap.Strings("endpoints", endpoints), zap.Error(err))
	if err != nil {
		errLog.Warn("fail to etcd client")
		return nil, err
	}
	err = EtcdStatus(etcdCli)
	if err != nil {
		errLog.Warn("unavailable etcd server, please check it")
		return nil, err
	}
	return etcdCli, err
}

func EtcdPut(etcdCli KVApi, key, val string, opts ...clientv3.OpOption) error {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdOpTimeout)
	defer cancel()
	return retry.Do(ctx, func() error {
		_, err := etcdCli.Put(ctx, key, val, opts...)
		return err
	}, retry.Attempts(EtcdOpRetryTime))
}

func EtcdGetWithContext(ctx context.Context, etcdCli KVApi, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	var err error
	var resp *clientv3.GetResponse

	ctx, cancel := context.WithTimeout(ctx, EtcdOpTimeout)
	defer cancel()
	err = retry.Do(ctx, func() error {
		resp, err = etcdCli.Get(ctx, key, opts...)
		return err
	}, retry.Attempts(EtcdOpRetryTime))
	return resp, err
}

// Deprecated: use EtcdGetWithContext instead
func EtcdGet(etcdCli KVApi, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return EtcdGetWithContext(context.TODO(), etcdCli, key, opts...)
}

func EtcdDelete(etcdCli KVApi, key string, opts ...clientv3.OpOption) error {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdOpTimeout)
	defer cancel()

	return retry.Do(ctx, func() error {
		_, err := etcdCli.Delete(ctx, key, opts...)
		return err
	}, retry.Attempts(EtcdOpRetryTime))
}

func EtcdTxn(etcdCli KVApi, fun func(txn clientv3.Txn) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdOpTimeout)
	defer cancel()

	return retry.Do(ctx, func() error {
		etcdTxn := etcdCli.Txn(ctx)
		err := fun(etcdTxn)
		return err
	}, retry.Attempts(EtcdOpRetryTime))
}

func EtcdStatus(etcdCli KVApi) error {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdOpTimeout)
	defer cancel()
	for _, endpoint := range etcdCli.Endpoints() {
		_, err := etcdCli.Status(ctx, endpoint)
		if err != nil {
			return err
		}
	}
	return nil
}
