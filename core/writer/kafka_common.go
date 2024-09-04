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

package writer

type msgType string

type KafkaMsg struct {
	Data string  `json:"data"`
	Info msgType `json:"info"`
}

const (
	createCollectionInfo   msgType = "create collection"
	dropCollectionInfo     msgType = "drop collection"
	createPartitionInfo    msgType = "create partition"
	dropPartitionInfo      msgType = "drop partition"
	createDatabaseInfo     msgType = "create database"
	dropDatabaseInfo       msgType = "drop database"
	alterDatabaseInfo      msgType = "alter database"
	insertDataInfo         msgType = "insert data"
	deleteDataInfo         msgType = "delete data"
	createIndexInfo        msgType = "create index"
	dropIndexInfo          msgType = "drop index"
	alterIndexInfo         msgType = "alter index"
	loadCollectionInfo     msgType = "load collection"
	releaseCollectionInfo  msgType = "release collection"
	loadPartitionsInfo     msgType = "load partitions"
	releasePartitionsInfo  msgType = "release partitions"
	flushInfo              msgType = "flush"
	replicateMessageInfo   msgType = "replicate message"
	createUserInfo         msgType = "create user"
	deleteUserInfo         msgType = "delete user"
	updateUserInfo         msgType = "update user"
	createRoleInfo         msgType = "create role"
	dropRoleInfo           msgType = "drop role"
	addUserToRoleInfo      msgType = "add user to role"
	removeUserFromRoleInfo msgType = "remove user from role"
	grantPrivilegeInfo     msgType = "grant privilege"
	revokePrivilegeInfo    msgType = "revoke privilege"
	describeDatabase       msgType = "describe database"
	describeCollection     msgType = "describe collection"
	describePartition      msgType = "describe partition"
)
