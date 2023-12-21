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
	"time"

	"github.com/cockroachdb/errors"
	_ "github.com/go-sql-driver/mysql"
	"github.com/goccy/go-json"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-cdc/core/log"
	"github.com/zilliztech/milvus-cdc/core/util"
	"github.com/zilliztech/milvus-cdc/server/api"
	"github.com/zilliztech/milvus-cdc/server/model/meta"
)

type MySQLMetaStore struct {
	log                         *zap.Logger
	db                          *sql.DB
	taskInfoStore               *TaskInfoMysqlStore
	taskCollectionPositionStore *TaskCollectionPositionMysqlStore
	txnMap                      map[any]func() *sql.Tx
}

func NewMySQLMetaStore(ctx context.Context, dataSourceName string, rootPath string) (*MySQLMetaStore, error) {
	s := &MySQLMetaStore{}
	// data source name: "root:root@tcp(127.0.0.1:3306)/milvuscdc?charset=utf8"
	err := s.init(ctx, dataSourceName, rootPath)
	if err != nil {
		s.log.Warn("fail to init db object", zap.Error(err))
		return nil, err
	}
	return s, nil
}

func (s *MySQLMetaStore) init(ctx context.Context, dataSourceName string, rootPath string) error {
	s.log = log.With(zap.String("meta_store", "mysql")).Logger
	db, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		s.log.Warn("fail to open mysql", zap.Error(err))
		return err
	}
	s.db = db
	timeoutCtx, cancelFunc := context.WithTimeout(ctx, 10*time.Second)
	defer cancelFunc()
	err = db.PingContext(timeoutCtx)
	if err != nil {
		s.log.Warn("fail to ping mysql", zap.Error(err))
		return err
	}
	txnMap := make(map[any]func() *sql.Tx)
	s.taskInfoStore, err = NewTaskInfoMysqlStore(ctx, db, rootPath, txnMap)
	if err != nil {
		s.log.Warn("fail to create task info store", zap.Error(err))
		return err
	}
	s.taskCollectionPositionStore, err = NewTaskCollectionPositionMysqlStore(ctx, db, rootPath, txnMap)
	if err != nil {
		s.log.Warn("fail to create task collection position store", zap.Error(err))
		return err
	}
	s.txnMap = txnMap

	return nil
}

var _ api.MetaStoreFactory = &MySQLMetaStore{}

func (s *MySQLMetaStore) GetTaskInfoMetaStore(ctx context.Context) api.MetaStore[*meta.TaskInfo] {
	return s.taskInfoStore
}

func (s *MySQLMetaStore) GetTaskCollectionPositionMetaStore(ctx context.Context) api.MetaStore[*meta.TaskCollectionPosition] {
	return s.taskCollectionPositionStore
}

func (s *MySQLMetaStore) Txn(ctx context.Context) (any, func(err error) error, error) {
	txObj, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.log.Warn("fail to begin transaction", zap.Error(err))
		return nil, nil, err
	}
	commitFunc := func(err error) error {
		defer delete(s.txnMap, txObj)
		if err != nil {
			s.log.Warn("fail to prepare transaction", zap.Error(err))
			return txObj.Rollback()
		}
		if err = txObj.Commit(); err != nil {
			s.log.Warn("fail to commit transaction", zap.Error(err))
			return txObj.Rollback()
		}
		return nil
	}
	s.txnMap[txObj] = func() *sql.Tx {
		return txObj
	}
	return txObj, commitFunc, nil
}

type TaskInfoMysqlStore struct {
	log      *zap.Logger
	rootPath string
	db       *sql.DB
	txnMap   map[any]func() *sql.Tx
}

var _ api.MetaStore[*meta.TaskInfo] = &TaskInfoMysqlStore{}

func NewTaskInfoMysqlStore(ctx context.Context, db *sql.DB, rootPath string, txnMap map[any]func() *sql.Tx) (*TaskInfoMysqlStore, error) {
	m := &TaskInfoMysqlStore{
		txnMap: txnMap,
	}
	err := m.init(ctx, db, rootPath)
	if err != nil {
		m.log.Warn("fail to init task info store", zap.Error(err))
		return nil, err
	}
	return m, nil
}

func (m *TaskInfoMysqlStore) init(ctx context.Context, db *sql.DB, rootPath string) error {
	m.log = log.With(zap.String("meta_store", "mysql"), zap.String("table", "task_info"), zap.String("root_path", rootPath)).Logger
	_, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS task_info (
			task_info_key VARCHAR(255) NOT NULL,
			task_id VARCHAR(255) NOT NULL,
			task_info_value JSON NOT NULL,
			PRIMARY KEY (task_info_key),
			INDEX idx_key (task_info_key),
			INDEX idx_task_id (task_id)
		)
	`)
	if err != nil {
		m.log.Warn("fail to create table", zap.Error(err))
		return err
	}
	m.db = db
	m.rootPath = rootPath
	return nil
}

func (m *TaskInfoMysqlStore) Put(ctx context.Context, metaObj *meta.TaskInfo, txn any) error {
	sqlStr := "INSERT INTO task_info (task_info_key, task_id, task_info_value) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE task_info_value = ?"
	objBytes, err := json.Marshal(metaObj)
	taskInfoKey := getTaskInfoKey(m.rootPath, metaObj.TaskID)
	if err != nil {
		m.log.Warn("fail to marshal task info", zap.Error(err))
		return err
	}
	defer func() {
		if err != nil {
			m.log.Warn("fail to put task info", zap.Error(err))
		}
	}()
	if txn != nil {
		if _, ok := m.txnMap[txn]; !ok {
			return errors.New("txn not exist")
		}
		stmt, err := m.txnMap[txn]().PrepareContext(ctx, sqlStr)
		if err != nil {
			m.log.Warn("fail to prepare put statement", zap.Error(err))
			return err
		}
		defer stmt.Close()
		_, err = stmt.ExecContext(ctx, taskInfoKey, metaObj.TaskID, util.ToString(objBytes), util.ToString(objBytes))
		if err != nil {
			return err
		}
		return nil
	}
	_, err = m.db.ExecContext(ctx, sqlStr, taskInfoKey, metaObj.TaskID, util.ToString(objBytes), util.ToString(objBytes))
	if err != nil {
		return err
	}
	return nil
}

func (m *TaskInfoMysqlStore) Get(ctx context.Context, metaObj *meta.TaskInfo, txn any) ([]*meta.TaskInfo, error) {
	sqlStr := fmt.Sprintf("SELECT task_info_value FROM task_info WHERE task_info_key LIKE '%s%%'", getTaskInfoPrefix(m.rootPath))
	var sqlArgs []any
	if metaObj.TaskID != "" {
		sqlStr += " AND task_id = ?"
		sqlArgs = append(sqlArgs, metaObj.TaskID)
	}

	var taskInfos []*meta.TaskInfo
	var err error
	var rows *sql.Rows

	if txn != nil {
		if _, ok := m.txnMap[txn]; !ok {
			return nil, errors.New("txn not exist")
		}
		stmt, err := m.txnMap[txn]().PrepareContext(ctx, sqlStr)
		if err != nil {
			m.log.Warn("fail to prepare get statement", zap.Error(err))
			return nil, err
		}
		defer stmt.Close()

		rows, err = stmt.QueryContext(ctx, sqlArgs...)
		if err != nil {
			m.log.Warn("fail to get task info", zap.Error(err))
			return nil, err
		}
	} else {
		rows, err = m.db.QueryContext(ctx, sqlStr, sqlArgs...)
		if err != nil {
			m.log.Warn("fail to get task info", zap.Error(err))
			return nil, err
		}
	}

	defer rows.Close()
	for rows.Next() {
		var taskInfoValue string
		err = rows.Scan(&taskInfoValue)
		if err != nil {
			m.log.Warn("fail to scan task info", zap.Error(err))
			return nil, err
		}
		var taskInfo meta.TaskInfo
		err = json.Unmarshal(util.ToBytes(taskInfoValue), &taskInfo)
		if err != nil {
			m.log.Warn("fail to unmarshal task info", zap.Error(err))
			return nil, err
		}
		taskInfos = append(taskInfos, &taskInfo)
	}

	return taskInfos, nil
}

func (m *TaskInfoMysqlStore) Delete(ctx context.Context, metaObj *meta.TaskInfo, txn any) error {
	taskID := metaObj.TaskID
	if taskID == "" {
		return errors.New("task id is empty")
	}
	sqlStr := "DELETE FROM task_info WHERE task_id = ?"
	var err error
	defer func() {
		if err != nil {
			m.log.Warn("fail to delete task info", zap.Error(err))
		}
	}()
	if txn != nil {
		if _, ok := m.txnMap[txn]; !ok {
			return errors.New("txn not exist")
		}
		stmt, err := m.txnMap[txn]().PrepareContext(ctx, sqlStr)
		if err != nil {
			return err
		}
		defer stmt.Close()
		_, err = stmt.ExecContext(ctx, taskID)
		if err != nil {
			return err
		}
		return nil
	}
	_, err = m.db.ExecContext(ctx, sqlStr, taskID)
	if err != nil {
		return err
	}
	return nil
}

type TaskCollectionPositionMysqlStore struct {
	log      *zap.Logger
	rootPath string
	db       *sql.DB
	txnMap   map[any]func() *sql.Tx
}

var _ api.MetaStore[*meta.TaskCollectionPosition] = &TaskCollectionPositionMysqlStore{}

func NewTaskCollectionPositionMysqlStore(ctx context.Context, db *sql.DB, rootPath string, txnMap map[any]func() *sql.Tx) (*TaskCollectionPositionMysqlStore, error) {
	s := &TaskCollectionPositionMysqlStore{
		txnMap: txnMap,
	}
	err := s.init(ctx, db, rootPath)
	if err != nil {
		s.log.Warn("fail to init task collection position store", zap.Error(err))
		return nil, err
	}
	return s, nil
}

func (m *TaskCollectionPositionMysqlStore) init(ctx context.Context, db *sql.DB, rootPath string) error {
	m.log = log.With(zap.String("meta_store", "mysql"), zap.String("table", "task_position"), zap.String("root_path", rootPath)).Logger
	_, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS task_position (
			task_position_key VARCHAR(255) NOT NULL,
			task_id VARCHAR(255) NOT NULL,
		    collection_id BIGINT NOT NULL,
		    collection_name VARCHAR(255) NOT NULL,
			task_position_value JSON NOT NULL,
			op_position_value JSON,
			target_position_value JSON,
			PRIMARY KEY (task_position_key),
			INDEX idx_key (task_position_key),
			INDEX idx_task_id (task_id),
		    INDEX idx_collection_id (collection_id)
		)
	`)
	if err != nil {
		m.log.Warn("fail to create table", zap.Error(err))
		return err
	}
	m.db = db
	m.rootPath = rootPath
	return nil
}

func (m *TaskCollectionPositionMysqlStore) Put(ctx context.Context, metaObj *meta.TaskCollectionPosition, txn any) error {
	sqlStr := "INSERT INTO task_position (task_position_key, task_id, collection_id, collection_name, task_position_value, op_position_value, target_position_value) VALUES (?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE task_position_value = ?, op_position_value = ?, target_position_value = ?"
	positionBytes, _ := json.Marshal(metaObj.Positions)
	opPositionBytes, _ := json.Marshal(metaObj.OpPositions)
	targetPositionBytes, _ := json.Marshal(metaObj.TargetPositions)
	var err error
	taskPositionKey := getTaskCollectionPositionKey(m.rootPath, metaObj.TaskID, metaObj.CollectionID)
	defer func() {
		if err != nil {
			m.log.Warn("fail to put task position", zap.Error(err))
		}
	}()
	if txn != nil {
		if _, ok := m.txnMap[txn]; !ok {
			return errors.New("txn not exist")
		}
		stmt, err := m.txnMap[txn]().PrepareContext(ctx, sqlStr)
		if err != nil {
			return err
		}
		defer stmt.Close()
		_, err = stmt.ExecContext(ctx, taskPositionKey, metaObj.TaskID, metaObj.CollectionID, metaObj.CollectionName, util.ToString(positionBytes), util.ToString(opPositionBytes), util.ToString(targetPositionBytes), util.ToString(positionBytes), util.ToString(opPositionBytes), util.ToString(targetPositionBytes))
		if err != nil {
			return err
		}
		return nil
	}
	_, err = m.db.ExecContext(ctx, sqlStr, taskPositionKey, metaObj.TaskID, metaObj.CollectionID, metaObj.CollectionName, util.ToString(positionBytes), util.ToString(opPositionBytes), util.ToString(targetPositionBytes), util.ToString(positionBytes), util.ToString(opPositionBytes), util.ToString(targetPositionBytes))
	if err != nil {
		return err
	}
	return nil
}

func (m *TaskCollectionPositionMysqlStore) Get(ctx context.Context, metaObj *meta.TaskCollectionPosition, txn any) ([]*meta.TaskCollectionPosition, error) {
	sqlStr := fmt.Sprintf("SELECT task_id, collection_id, collection_name, task_position_value, op_position_value, target_position_value FROM task_position WHERE task_position_key LIKE '%s%%'", getTaskCollectionPositionPrefix(m.rootPath))
	var sqlArgs []any
	if metaObj.TaskID != "" || metaObj.CollectionID != 0 {
		if metaObj.TaskID != "" {
			sqlStr += " AND task_id = ?"
			sqlArgs = append(sqlArgs, metaObj.TaskID)
		}
		if metaObj.CollectionID != 0 {
			sqlStr += " AND collection_id = ?"
			sqlArgs = append(sqlArgs, metaObj.CollectionID)
		}
	}

	var taskPositions []*meta.TaskCollectionPosition
	// var taskPositionValue string
	var rows *sql.Rows
	var err error

	if txn != nil {
		if _, ok := m.txnMap[txn]; !ok {
			return nil, errors.New("txn not exist")
		}
		stmt, err := m.txnMap[txn]().PrepareContext(ctx, sqlStr)
		if err != nil {
			m.log.Warn("fail to prepare get statement", zap.Error(err))
			return nil, err
		}
		defer stmt.Close()

		rows, err = stmt.QueryContext(ctx, sqlArgs...)
		if err != nil {
			m.log.Warn("fail to get task info", zap.Error(err))
			return nil, err
		}
	} else {
		rows, err = m.db.QueryContext(ctx, sqlStr, sqlArgs...)
		if err != nil {
			m.log.Warn("fail to get task info", zap.Error(err))
			return nil, err
		}
	}

	for rows.Next() {
		var taskPosition meta.TaskCollectionPosition
		var taskPositionValue string
		var opPositionValue string
		var targetPositionValue string
		err = rows.Scan(&taskPosition.TaskID, &taskPosition.CollectionID, &taskPosition.CollectionName, &taskPositionValue, &opPositionValue, &targetPositionValue)
		if err != nil {
			m.log.Warn("fail to scan task position", zap.Error(err))
			return nil, err
		}
		taskPosition.Positions = make(map[string]*meta.PositionInfo)
		err = json.Unmarshal(util.ToBytes(taskPositionValue), &taskPosition.Positions)
		if err != nil {
			m.log.Warn("fail to unmarshal task position", zap.Error(err))
			return nil, err
		}
		taskPosition.OpPositions = make(map[string]*meta.PositionInfo)
		err = json.Unmarshal(util.ToBytes(opPositionValue), &taskPosition.OpPositions)
		if err != nil {
			m.log.Warn("fail to unmarshal op position", zap.Error(err))
			return nil, err
		}
		taskPosition.TargetPositions = make(map[string]*meta.PositionInfo)
		err = json.Unmarshal(util.ToBytes(targetPositionValue), &taskPosition.TargetPositions)
		if err != nil {
			m.log.Warn("fail to unmarshal target position", zap.Error(err))
			return nil, err
		}
		taskPositions = append(taskPositions, &taskPosition)
	}

	return taskPositions, nil
}

func (m *TaskCollectionPositionMysqlStore) Delete(ctx context.Context, metaObj *meta.TaskCollectionPosition, txn any) error {
	taskID := metaObj.TaskID
	if taskID == "" {
		return errors.New("task id is empty")
	}
	sqlStr := "DELETE FROM task_position WHERE task_id = ?"
	var sqlArgs []any = []any{taskID}
	if metaObj.CollectionID != 0 {
		sqlStr += " AND collection_id = ?"
		sqlArgs = append(sqlArgs, metaObj.CollectionID)
	}
	var err error
	defer func() {
		if err != nil {
			m.log.Warn("fail to delete task position", zap.Error(err))
		}
	}()
	if txn != nil {
		if _, ok := m.txnMap[txn]; !ok {
			return errors.New("txn not exist")
		}
		stmt, err := m.txnMap[txn]().PrepareContext(ctx, sqlStr)
		if err != nil {
			m.log.Warn("fail to prepare delete statement", zap.Error(err))
			return err
		}
		defer stmt.Close()
		_, err = stmt.ExecContext(ctx, sqlArgs...)
		if err != nil {
			return err
		}
		return nil
	}
	_, err = m.db.ExecContext(ctx, sqlStr, sqlArgs...)
	if err != nil {
		return err
	}
	return nil
}
