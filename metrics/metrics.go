/*
 * Copyright (c) 2024 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package metrics

import "sync/atomic"

const (
	metricsSamples = 16
)

type Metrics struct {
	lastSample *atomic.Int64
	curSample  *atomic.Int64
	intervals  [metricsSamples]int64
	index      uint64
}

func NewMetrics() *Metrics {
	return &Metrics{lastSample: new(atomic.Int64), curSample: new(atomic.Int64), index: 0}
}

func (m *Metrics) GetAvg() float64 {
	var sum int64 = 0
	for i := 0; i < metricsSamples; i++ {
		sum += atomic.LoadInt64(&m.intervals[i])
	}
	avg := float64(sum) / float64(metricsSamples)
	return avg
}

func (m *Metrics) Inc(delta int64) {
	m.curSample.Add(delta)
}

func (m *Metrics) Observe() {
	cur := m.curSample.Load()
	last := m.lastSample.Load()
	m.lastSample.Store(cur)
	atomic.StoreInt64(&m.intervals[m.index%metricsSamples], cur-last)
	atomic.AddUint64(&m.index, 1)
}

func (m *Metrics) GetSample() int64 {
	return m.curSample.Load()
}
