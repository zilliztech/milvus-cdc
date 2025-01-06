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

import "context"

type ReplicateStore interface {
	Get(ctx context.Context, key string, withPrefix bool) ([]MetaMsg, error)
	Put(ctx context.Context, key string, value MetaMsg) error
	Remove(ctx context.Context, key string) error
}

type ReplicateMeta interface {
	UpdateTaskDropCollectionMsg(ctx context.Context, msg TaskDropCollectionMsg) (bool, error)
	GetTaskDropCollectionMsg(ctx context.Context, taskID string, msgID string) ([]TaskDropCollectionMsg, error)
	UpdateTaskDropPartitionMsg(ctx context.Context, msg TaskDropPartitionMsg) (bool, error)
	GetTaskDropPartitionMsg(ctx context.Context, taskID string, msgID string) ([]TaskDropPartitionMsg, error)
	RemoveTaskMsg(ctx context.Context, taskID string, msgID string) error
}
