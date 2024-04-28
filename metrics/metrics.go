package metrics

import "sync/atomic"

const (
	metricsSamples = 16
)

type Metrics struct {
	lastSample int64
	curSample  int64
	intervals  [metricsSamples]int64
	index      uint64
}

func NewMetrics() *Metrics {
	return &Metrics{lastSample: 0, curSample: 0, index: 0}
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
	atomic.AddInt64(&m.curSample, delta)
}

func (m *Metrics) Observe() {
	cur := atomic.LoadInt64(&m.curSample)
	last := atomic.LoadInt64(&m.lastSample)
	atomic.StoreInt64(&m.lastSample, cur)
	atomic.StoreInt64(&m.intervals[m.index%metricsSamples], cur-last)
	atomic.AddUint64(&m.index, 1)
}

func (m *Metrics) GetSample() int64 {
	return atomic.LoadInt64(&m.curSample)
}
