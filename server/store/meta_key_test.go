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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetTaskInfoPrefix(t *testing.T) {
	rootPath := "/root"
	expected := "/root/task_info/"
	actual := getTaskInfoPrefix(rootPath)
	assert.Equal(t, expected, actual)
}

func TestGetTaskInfoKey(t *testing.T) {
	rootPath := "/root"
	taskID := "1234"
	expected := "/root/task_info/1234"
	actual := getTaskInfoKey(rootPath, taskID)
	assert.Equal(t, expected, actual)
}

func TestGetTaskCollectionPositionPrefix(t *testing.T) {
	rootPath := "/root"
	expected := "/root/task_position/"
	actual := getTaskCollectionPositionPrefix(rootPath)
	assert.Equal(t, expected, actual)
}

func TestGetTaskCollectionPositionPrefixWithTaskID(t *testing.T) {
	rootPath := "/root"
	taskID := "1234"
	expected := "/root/task_position/1234/"
	actual := getTaskCollectionPositionPrefixWithTaskID(rootPath, taskID)
	assert.Equal(t, expected, actual)
}

func TestGetTaskCollectionPositionKey(t *testing.T) {
	rootPath := "/root"
	taskID := "1234"
	collectionID := int64(5678)
	expected := "/root/task_position/1234/5678"
	actual := getTaskCollectionPositionKey(rootPath, taskID, collectionID)
	assert.Equal(t, expected, actual)
}
