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
