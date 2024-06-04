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

package log

import (
	"time"

	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

type RateLog struct {
	l        rate.Limiter
	log      *zap.Logger
	lastTime time.Time
}

func NewRateLog(ratePerSecond float64, log *zap.Logger) *RateLog {
	bucketCnt := int(2 * ratePerSecond)
	if bucketCnt < 2 {
		bucketCnt = 2
	}
	return &RateLog{
		l:        *rate.NewLimiter(rate.Limit(ratePerSecond), bucketCnt),
		log:      log,
		lastTime: time.Now(),
	}
}

func (r *RateLog) Info(msg string, fields ...zap.Field) {
	if r.l.Allow() {
		r.log.Info(msg, fields...)
	}
}
