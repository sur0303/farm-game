package dirtywriteback

import "github.com/prometheus/client_golang/prometheus"

// RegisterMetrics exposes one Registry's process-local dirty writeback state.
// Both data modes register the same series; durable_cas reports zero values so
// dashboards and alerts do not depend on an optional metric disappearing.
func RegisterMetrics(registerer prometheus.Registerer, actorKind string, snapshot func() MetricsSnapshot) {
	if registerer == nil || snapshot == nil || actorKind == "" {
		return
	}
	labels := prometheus.Labels{"actor_kind": actorKind}
	registerer.MustRegister(
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "actor_dirty_writeback", Name: "marks_total",
			Help: "Actor dirty marks observed, including marks coalesced into an existing actor entry.", ConstLabels: labels,
		}, func() float64 { return float64(snapshot().Marks) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "actor_dirty_writeback", Name: "coalesced_marks_total",
			Help: "Actor dirty marks coalesced into an already tracked actor entry.", ConstLabels: labels,
		}, func() float64 { return float64(snapshot().CoalescedMarks) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "actor_dirty_writeback", Name: "flush_attempts_total",
			Help: "Dirty actor flush attempts started by the process-local scheduler.", ConstLabels: labels,
		}, func() float64 { return float64(snapshot().FlushAttempts) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "actor_dirty_writeback", Name: "flush_succeeded_total",
			Help: "Dirty actor flush attempts completed successfully.", ConstLabels: labels,
		}, func() float64 { return float64(snapshot().FlushSucceeded) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "actor_dirty_writeback", Name: "flush_failed_total",
			Help: "Dirty actor flush attempts that failed and retained writeback responsibility.", ConstLabels: labels,
		}, func() float64 { return float64(snapshot().FlushFailed) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "farm", Subsystem: "actor_dirty_writeback", Name: "queue_depth",
			Help: "Unique dirty actors currently tracked, including in-flight entries.", ConstLabels: labels,
		}, func() float64 { return float64(snapshot().QueueDepth) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "farm", Subsystem: "actor_dirty_writeback", Name: "inflight",
			Help: "Dirty actor flushes currently performing or completing external storage I/O.", ConstLabels: labels,
		}, func() float64 { return float64(snapshot().Inflight) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "farm", Subsystem: "actor_dirty_writeback", Name: "oldest_dirty_age_seconds",
			Help: "Age in seconds of the oldest dirty actor still owned by this process.", ConstLabels: labels,
		}, func() float64 { return snapshot().OldestDirtyAge.Seconds() }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "farm", Subsystem: "actor_dirty_writeback", Name: "max_queue_depth",
			Help: "Process-lifetime high-water mark of unique dirty actors tracked at once.", ConstLabels: labels,
		}, func() float64 { return float64(snapshot().MaxQueueDepth) }),
	)
}
