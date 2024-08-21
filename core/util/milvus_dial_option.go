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
	"crypto/tls"
	"crypto/x509"
	"os"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/zilliztech/milvus-cdc/core/log"
)

type DialConfig struct {
	ServerName    string `json:"server_name,omitempty" mapstructure:"server_name,omitempty"`
	ServerPemPath string `json:"server_pem_path,omitempty" mapstructure:"server_pem_path,omitempty"`
	CaPemPath     string `json:"ca_pem_path,omitempty" mapstructure:"ca_pem_path,omitempty"`
	ClientPemPath string `json:"client_pem_path,omitempty" mapstructure:"client_pem_path,omitempty"`
	ClientKeyPath string `json:"client_key_path,omitempty" mapstructure:"client_key_path,omitempty"`
}

func GetDialOptions(config DialConfig) ([]grpc.DialOption, error) {
	dialOptions := make([]grpc.DialOption, 0)

	// tls one
	if config.ServerPemPath != "" {
		certPool := x509.NewCertPool()
		ca, err := os.ReadFile(config.ServerPemPath)
		if err != nil {
			log.Warn("tls error: fail to read server pem", zap.String("path", config.ServerPemPath), zap.Error(err))
			return nil, err
		}

		if ok := certPool.AppendCertsFromPEM(ca); !ok {
			log.Warn("tls error: certPool.AppendCertsFromPEM err")
			return nil, errors.New("certPool.AppendCertsFromPEM err")
		}

		dialOptions = append(dialOptions, grpc.WithTransportCredentials(
			credentials.NewTLS(&tls.Config{
				ServerName: config.ServerName,
				RootCAs:    certPool,
				MinVersion: tls.VersionTLS13,
			}),
		))
		log.Info("tls one success")
	} else if config.CaPemPath != "" && config.ClientPemPath != "" && config.ClientKeyPath != "" {
		// tls2
		cert, err := tls.LoadX509KeyPair(config.ClientPemPath, config.ClientKeyPath) // 注意这里是 client 的证书
		if err != nil {
			log.Warn("tls error: fail to load client key pair",
				zap.String("clientPemPath", config.ClientPemPath),
				zap.String("clientKeyPath", config.ClientKeyPath),
				zap.Error(err))
			return nil, err
		}

		certPool := x509.NewCertPool()
		ca, err := os.ReadFile(config.CaPemPath)
		if err != nil {
			log.Warn("tls error: fail to read ca pem", zap.String("path", config.CaPemPath), zap.Error(err))
			return nil, err
		}

		if ok := certPool.AppendCertsFromPEM(ca); !ok {
			log.Warn("tls error: certPool.AppendCertsFromPEM err")
			return nil, errors.New("certPool.AppendCertsFromPEM err")
		}

		dialOptions = append(dialOptions, grpc.WithTransportCredentials(
			credentials.NewTLS(&tls.Config{
				Certificates: []tls.Certificate{cert},
				ServerName:   config.ServerName,
				RootCAs:      certPool,
				MinVersion:   tls.VersionTLS13,
			}),
		))
		log.Info("tls two success")
	}

	return dialOptions, nil
}
