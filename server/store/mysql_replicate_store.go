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

package store

import (
	"context"
	"database/sql"
	"fmt"
	"path"
	"time"

	"github.com/goccy/go-json"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/log"
	"github.com/zilliztech/milvus-cdc/core/util"
)

type MySQLReplicateStore struct {
	log      *zap.Logger
	db       *sql.DB
	rootPath string
}

func NewMySQLReplicateStore(ctx context.Context, dataSourceName string, rootPath string) (*MySQLReplicateStore, error) {
	s := &MySQLReplicateStore{}
	s.rootPath = rootPath
	s.log = log.With(zap.String("meta_store", "mysql")).Logger
	db, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		s.log.Warn("fail to open mysql", zap.Error(err))
		return nil, err
	}
	s.db = db
	timeoutCtx, cancelFunc := context.WithTimeout(ctx, 10*time.Second)
	defer cancelFunc()
	err = db.PingContext(timeoutCtx)
	if err != nil {
		s.log.Warn("fail to ping mysql", zap.Error(err))
		return nil, err
	}

	_, err = db.ExecContext(timeoutCtx, `
		CREATE TABLE IF NOT EXISTS task_msg (
			task_msg_key VARCHAR(255) NOT NULL,
			task_msg_value JSON NOT NULL,
			PRIMARY KEY (task_msg_key),
			INDEX idx_key (task_msg_key)
		)
	`)
	if err != nil {
		s.log.Warn("fail to create table", zap.Error(err))
		return nil, err
	}

	return s, nil
}

func (s *MySQLReplicateStore) Get(ctx context.Context, key string, withPrefix bool) ([]api.MetaMsg, error) {
	// key format is {task id}/{msg id}
	taskMsgKey := path.Join(s.rootPath, key)
	var sqlStr string
	var sqlArgs []any
	if withPrefix {
		sqlStr = fmt.Sprintf("SELECT task_msg_value FROM task_msg WHERE task_msg_key LIKE '%s%%'", taskMsgKey)
	} else {
		sqlStr = "SELECT task_msg_value FROM task_msg WHERE task_msg_key = ?"
	}
	sqlArgs = append(sqlArgs, taskMsgKey)

	rows, err := s.db.QueryContext(ctx, sqlStr, sqlArgs...)
	if err != nil {
		s.log.Warn("fail to get task info", zap.Error(err))
		return nil, err
	}
	defer rows.Close()

	var result []api.MetaMsg
	for rows.Next() {
		var taskMsgValue string
		err = rows.Scan(&taskMsgValue)
		if err != nil {
			s.log.Warn("fail to scan task info", zap.Error(err))
			return nil, err
		}
		var msg api.MetaMsg
		if err := json.Unmarshal(util.ToBytes(taskMsgValue), &msg); err != nil {
			return nil, err
		}
		result = append(result, msg)
	}

	return result, nil
}

func (s *MySQLReplicateStore) Put(ctx context.Context, key string, value api.MetaMsg) error {
	// key format is {task id}/{msg id}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	sqlStr := "INSERT INTO task_msg (task_msg_key, task_msg_value) VALUES (?, ?) ON DUPLICATE KEY UPDATE task_msg_value = ?"
	taskMsgKey := path.Join(s.rootPath, key)
	defer func() {
		if err != nil {
			s.log.Warn("fail to put task msg", zap.Error(err))
		}
	}()
	_, err = s.db.ExecContext(ctx, sqlStr, taskMsgKey, util.ToString(data), util.ToString(data))
	if err != nil {
		return err
	}
	return nil
}

func (s *MySQLReplicateStore) Remove(ctx context.Context, key string) error {
	// key format is {task id}/{msg id}
	sqlStr := "DELETE FROM task_msg WHERE task_msg_key = ?"
	taskMsgKey := path.Join(s.rootPath, key)
	var err error
	defer func() {
		if err != nil {
			s.log.Warn("fail to delete task info", zap.Error(err))
		}
	}()
	_, err = s.db.ExecContext(ctx, sqlStr, taskMsgKey)
	if err != nil {
		return err
	}
	return nil
}
