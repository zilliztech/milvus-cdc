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

package reader

type Barrier struct {
	Dest        int
	BarrierChan chan uint64
	CloseChan   chan struct{}
}

func NewBarrier(count int, f func(msgTs uint64, b *Barrier)) *Barrier {
	barrier := &Barrier{
		Dest:        count,
		BarrierChan: make(chan uint64, count),
		CloseChan:   make(chan struct{}),
	}

	go func() {
		current := 0
		var msgTs uint64
		for current < barrier.Dest {
			select {
			case <-barrier.CloseChan:
			case msgTs = <-barrier.BarrierChan:
				current++
			}
		}
		f(msgTs, barrier)
	}()

	return barrier
}
