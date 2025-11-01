package server

// Some statistics to see how goo the task queue process is

import (
	"fmt"
	"sync/atomic"
	"time"
)

type Metrics struct {
	jobsEnqueued int64
	jobsDequeued int64
	jobsAcked    int64
	jobsTimedOut int64
	startTime    time.Time
}

func NewMetrics() *Metrics {
	return &Metrics{
		startTime: time.Now(),
	}
}

func (m *Metrics) IncrementEnqueued() {
	atomic.AddInt64(&m.jobsEnqueued, 1)
}

func (m *Metrics) IncrementDequeued() {
	atomic.AddInt64(&m.jobsDequeued, 1)
}

func (m *Metrics) IncrementAcked() {
	atomic.AddInt64(&m.jobsAcked, 1)
}

func (m *Metrics) IncrementTimedOut(count int) {
	atomic.AddInt64(&m.jobsTimedOut, int64(count))
}

func (m *Metrics) GetStats() string {
	uptime := time.Since(m.startTime)
	return fmt.Sprintf(
		"Uptime: %v | Enqueued: %d | Dequeued: %d | Acked: %d | Timed Out: %d",
		uptime.Round(time.Second),
		atomic.LoadInt64(&m.jobsEnqueued),
		atomic.LoadInt64(&m.jobsDequeued),
		atomic.LoadInt64(&m.jobsAcked),
		atomic.LoadInt64(&m.jobsTimedOut),
	)
}
