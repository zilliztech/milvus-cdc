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
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-cdc/core/log"
)

type ChannelMapping struct {
	sourceCnt  int
	targetCnt  int
	averageCnt int

	// when source and target are the same, use sameMapping, key is source channel
	sameMapping map[string]string
	// when source is more than target, use sourceMapping, key is source channel, A1/A2/A3 is source, A is target, like A1 -> A, A2 -> A, A3 -> A
	sourceMapping map[string]string
	// when source is less than target, use targetMapping, key is target channel, A is source, A1/A2/A3 is target, like A -> A1, A -> A2, A -> A3
	targetMapping map[string]string
}

// nolint
func NewChannelMapping(sourceCnt, targetCnt int) *ChannelMapping {
	if sourceCnt == 0 || targetCnt == 0 {
		sourceCnt = 0
		targetCnt = 0
	}
	c := &ChannelMapping{
		sourceCnt: sourceCnt,
		targetCnt: targetCnt,
	}
	if sourceCnt == targetCnt {
		c.sameMapping = make(map[string]string)
	} else if sourceCnt > targetCnt {
		c.sourceMapping = make(map[string]string)
	} else {
		c.targetMapping = make(map[string]string)
	}
	c.averageCnt = average(sourceCnt, targetCnt)
	log.Info("ChannelMapping", zap.Int("sourceCnt", sourceCnt), zap.Int("targetCnt", targetCnt), zap.Int("averageCnt", c.averageCnt))

	return c
}

func average(sourceCnt, targetCnt int) int {
	if sourceCnt == targetCnt {
		return 1
	}
	if sourceCnt > targetCnt {
		m := sourceCnt % targetCnt
		if m != 0 {
			return sourceCnt/targetCnt + 1
		}
		return sourceCnt / targetCnt
	}
	m := targetCnt % sourceCnt
	if m != 0 {
		return targetCnt/sourceCnt + 1
	}
	return targetCnt / sourceCnt
}

func (c *ChannelMapping) GetMapKey(source, target string) string {
	if c.sameMapping != nil || c.sourceMapping != nil {
		return source
	}
	return target
}

func (c *ChannelMapping) GetMapValue(source, target string) string {
	if c.sameMapping != nil || c.sourceMapping != nil {
		return target
	}
	return source
}

func (c *ChannelMapping) UsingSourceKey() bool {
	return c.sameMapping != nil || c.sourceMapping != nil
}

// CheckKeyNotExist which means the key isn't existed, it will return true if the value is no mapping key, else return false
func (c *ChannelMapping) CheckKeyNotExist(source, target string) bool {
	if c.sameMapping != nil {
		for _, v := range c.sameMapping {
			if v == target {
				return false
			}
		}
		return true
	}
	if c.sourceMapping != nil { // source is more
		targetCnt := 0
		for _, v := range c.sourceMapping {
			if v == target {
				targetCnt++
			}
		}
		return targetCnt < c.averageCnt
	}
	if c.targetMapping != nil { // target is more
		sourceCnt := 0
		for _, v := range c.targetMapping {
			if v == source {
				sourceCnt++
			}
		}
		return sourceCnt < c.averageCnt
	}
	panic("channel mapping is not initialized")
}

func (c *ChannelMapping) AddKeyValue(source, target string) {
	if c.sameMapping != nil {
		c.sameMapping[source] = target
		return
	}
	if c.sourceMapping != nil {
		c.sourceMapping[source] = target
		return
	}
	if c.targetMapping != nil {
		c.targetMapping[target] = source
		return
	}
	panic("channel mapping is not initialized")
}

func (c *ChannelMapping) CheckKeyExist(source, target string) bool {
	var ok bool
	var mappingValue, checkValue string
	if c.sameMapping != nil {
		mappingValue, ok = c.sameMapping[source]
		checkValue = target
	}
	if c.sourceMapping != nil {
		mappingValue, ok = c.sourceMapping[source]
		checkValue = target
	}
	if c.targetMapping != nil {
		mappingValue, ok = c.targetMapping[target]
		checkValue = source
	}
	if !ok || mappingValue == "" {
		return false
	}
	return mappingValue == checkValue
}
