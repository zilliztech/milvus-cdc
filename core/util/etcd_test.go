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
	"testing"

	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestEtcdClient(t *testing.T) {
	t.Run("empty endpoints", func(t *testing.T) {
		_, err := GetEtcdClient(nil)
		assert.Error(t, err)
	})

	t.Run("normal endpoints", func(t *testing.T) {
		_, err := GetEtcdClient([]string{"127.0.0.1:2379"})
		assert.NoError(t, err)
	})

	t.Run("op", func(t *testing.T) {
		prefix := "test-cdc/client"

		c, err := GetEtcdClient([]string{"127.0.0.1:2379"})
		assert.NoError(t, err)
		defer c.Delete(context.Background(), prefix, clientv3.WithPrefix())

		// put
		{
			err := EtcdPut(c, prefix+"/foo", "bar")
			assert.NoError(t, err)
		}

		// get
		{
			resp, err := EtcdGetWithContext(context.Background(), c, prefix+"/foo")
			assert.NoError(t, err)
			assert.Equal(t, "bar", string(resp.Kvs[0].Value))
		}
		{
			resp, err := EtcdGet(c, prefix+"/foo")
			assert.NoError(t, err)
			assert.Equal(t, "bar", string(resp.Kvs[0].Value))
		}

		// delete
		{
			err := EtcdDelete(c, prefix+"/foo")
			assert.NoError(t, err)
		}

		// txn
		{
			_ = EtcdPut(c, prefix+"/foo", "bar")
			err := EtcdTxn(c, func(txn clientv3.Txn) error {
				resp, err := txn.If(clientv3.Compare(clientv3.Version(prefix+"/foo"), ">", 0)).Then(clientv3.OpDelete(prefix + "/foo")).Commit()
				assert.NoError(t, err)
				assert.True(t, resp.Succeeded)
				return nil
			})
			assert.NoError(t, err)
		}

		// status
		{
			err := EtcdStatus(c)
			assert.NoError(t, err)
		}
	})
}
