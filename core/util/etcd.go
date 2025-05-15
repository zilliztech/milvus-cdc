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

package util

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/pkg/v2/util/retry"

	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/log"
)

var (
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

// deprecated
func MockEtcdClient(new func(cfg clientv3.Config) (KVApi, error), f func()) {
	origin := newEtcdClient
	newEtcdClient = new
	defer func() {
		newEtcdClient = origin
	}()
	f()
}

func GetEtcdClientWithAddress(endpoints []string) (KVApi, error) {
	return GetEtcdClient(config.EtcdServerConfig{
		Address: endpoints,
	})
}

func GetEtcdClient(etcdServerConfig config.EtcdServerConfig) (KVApi, error) {
	errLog := log.With(zap.Strings("endpoints", etcdServerConfig.Address))
	etcdConfig, err := GetEtcdConfig(etcdServerConfig)
	if err != nil {
		errLog.Warn("fail to get etcd config", zap.Error(err))
		return nil, err
	}
	etcdCli, err := newEtcdClient(etcdConfig)
	if err != nil {
		errLog.Warn("fail to etcd client", zap.Error(err))
		return nil, err
	}
	err = EtcdStatus(etcdCli)
	if err != nil {
		errLog.Warn("unavailable etcd server, please check it", zap.Error(err))
		return nil, err
	}
	return etcdCli, err
}

func GetEtcdConfig(etcdServerConfig config.EtcdServerConfig) (clientv3.Config, error) {
	dialTimeout := 5 * time.Second
	if !etcdServerConfig.EnableTLS && !etcdServerConfig.EnableAuth {
		return clientv3.Config{
			Endpoints:   etcdServerConfig.Address,
			DialTimeout: dialTimeout,
			Logger:      log.L(),
		}, nil
	}
	if !etcdServerConfig.EnableTLS && etcdServerConfig.EnableAuth {
		return clientv3.Config{
			Endpoints:   etcdServerConfig.Address,
			DialTimeout: dialTimeout,
			Username:    etcdServerConfig.Username,
			Password:    etcdServerConfig.Password,
			Logger:      log.L(),
		}, nil
	}
	c := &clientv3.Config{
		Logger: log.L(),
	}
	if etcdServerConfig.EnableAuth {
		c.Username = etcdServerConfig.Username
		c.Password = etcdServerConfig.Password
	}

	c, err := GetEtcdSSLCfg(etcdServerConfig.Address,
		etcdServerConfig.TLSCertPath,
		etcdServerConfig.TLSKeyPath,
		etcdServerConfig.TLSCACertPath,
		etcdServerConfig.TLSMinVersion, c)
	return *c, err
}

func GetEtcdSSLCfg(endpoints []string, certFile string, keyFile string, caCertFile string, minVersion string, cfg *clientv3.Config) (*clientv3.Config, error) {
	cfg.Endpoints = endpoints
	cfg.DialTimeout = 5 * time.Second
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, errors.Wrap(err, "load etcd cert key pair error")
	}
	caCert, err := os.ReadFile(filepath.Clean(caCertFile))
	if err != nil {
		return nil, errors.Wrapf(err, "load etcd CACert file error, filename = %s", caCertFile)
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	cfg.TLS = &tls.Config{
		MinVersion: tls.VersionTLS13,
		Certificates: []tls.Certificate{
			cert,
		},
		RootCAs: caCertPool,
	}
	switch minVersion {
	case "1.0":
		cfg.TLS.MinVersion = tls.VersionTLS10
	case "1.1":
		cfg.TLS.MinVersion = tls.VersionTLS11
	case "1.2":
		cfg.TLS.MinVersion = tls.VersionTLS12
	case "1.3":
		cfg.TLS.MinVersion = tls.VersionTLS13
	default:
		cfg.TLS.MinVersion = 0
	}

	if cfg.TLS.MinVersion == 0 {
		return nil, errors.Errorf("unknown TLS version,%s", minVersion)
	}

	cfg.DialOptions = append(cfg.DialOptions, grpc.WithBlock())

	return cfg, nil
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
