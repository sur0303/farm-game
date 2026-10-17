package readiness

import (
	"math"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type ScanStats struct {
	OwnedBuckets       uint64
	AssignmentRevision uint64
	BucketsScanned     uint64
	Claimed            uint64
	Completed          uint64
	Retried            uint64
	StepUnknown        uint64
}

type Metrics struct {
	scanDuration       *prometheus.HistogramVec
	claimed            *prometheus.CounterVec
	completed          *prometheus.CounterVec
	retried            *prometheus.CounterVec
	stepUnknown        *prometheus.CounterVec
	terminalLatency    *prometheus.HistogramVec
	ownedBuckets       *prometheus.GaugeVec
	assignmentRevision *prometheus.GaugeVec
	bucketsScanned     *prometheus.CounterVec
}

func NewMetrics(registerer prometheus.Registerer, registry *Registry) (*Metrics, error) {
	metrics := &Metrics{
		scanDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "farm", Subsystem: "worker", Name: "scan_duration_ms",
			Help:    "Duration in milliseconds of one bounded recovery scan by Family.",
			Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 30_000},
		}, []string{"family"}),
		claimed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "worker", Name: "claimed_total",
			Help: "Durable work leases claimed by recovery Family.",
		}, []string{"family"}),
		completed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "worker", Name: "completed_total",
			Help: "Durable recovery work completed by Family.",
		}, []string{"family"}),
		retried: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "worker", Name: "retried_total",
			Help: "Durable recovery retries scheduled by Family.",
		}, []string{"family"}),
		stepUnknown: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "worker", Name: "step_unknown_total",
			Help: "Participant steps with an unknown response boundary by Family.",
		}, []string{"family"}),
		terminalLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "farm", Subsystem: "worker", Name: "terminal_latency_ms",
			Help:    "Creation-to-terminal latency in milliseconds when the durable repository exposes both timestamps.",
			Buckets: []float64{10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 30_000, 60_000, 300_000},
		}, []string{"family"}),
		ownedBuckets: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "farm", Subsystem: "worker", Name: "owned_bucket_count",
			Help: "Authority buckets currently owned by this worker for a recovery Family.",
		}, []string{"family"}),
		assignmentRevision: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "farm", Subsystem: "worker", Name: "assignment_revision",
			Help: "Route assignment revision used by the most recent recovery scan.",
		}, []string{"family"}),
		bucketsScanned: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "worker", Name: "bucket_scanned_total",
			Help: "Authority buckets scanned by recovery Family.",
		}, []string{"family"}),
	}
	if registerer != nil {
		collectors := []prometheus.Collector{
			&snapshotCollector{registry: registry}, metrics.scanDuration, metrics.claimed,
			metrics.completed, metrics.retried, metrics.stepUnknown, metrics.terminalLatency,
			metrics.ownedBuckets, metrics.assignmentRevision, metrics.bucketsScanned,
		}
		for _, collector := range collectors {
			if err := registerer.Register(collector); err != nil {
				return nil, err
			}
		}
	}
	for _, family := range allFamilies {
		label := string(family)
		metrics.scanDuration.WithLabelValues(label)
		metrics.claimed.WithLabelValues(label)
		metrics.completed.WithLabelValues(label)
		metrics.retried.WithLabelValues(label)
		metrics.stepUnknown.WithLabelValues(label)
		metrics.terminalLatency.WithLabelValues(label)
		metrics.ownedBuckets.WithLabelValues(label)
		metrics.assignmentRevision.WithLabelValues(label)
		metrics.bucketsScanned.WithLabelValues(label)
	}
	return metrics, nil
}

func (m *Metrics) ObserveScan(family Family, duration time.Duration, stats ScanStats) {
	if m == nil || !knownFamily(family) {
		return
	}
	label := string(family)
	m.scanDuration.WithLabelValues(label).Observe(float64(duration) / float64(time.Millisecond))
	m.ownedBuckets.WithLabelValues(label).Set(float64(stats.OwnedBuckets))
	m.assignmentRevision.WithLabelValues(label).Set(float64(stats.AssignmentRevision))
	m.bucketsScanned.WithLabelValues(label).Add(float64(stats.BucketsScanned))
	m.ObserveCounts(family, stats)
}

// ObserveCounts records work processed by a shared scanner without inventing
// a second Family scan duration. CARE uses this for Pair CARE orders routed by
// the THEFT-owned shared Pair DueIndex loop.
func (m *Metrics) ObserveCounts(family Family, stats ScanStats) {
	if m == nil || !knownFamily(family) {
		return
	}
	label := string(family)
	m.claimed.WithLabelValues(label).Add(float64(stats.Claimed))
	m.completed.WithLabelValues(label).Add(float64(stats.Completed))
	m.retried.WithLabelValues(label).Add(float64(stats.Retried))
	m.stepUnknown.WithLabelValues(label).Add(float64(stats.StepUnknown))
}

func (m *Metrics) ObserveTerminalLatency(family Family, latency time.Duration) {
	if m == nil || !knownFamily(family) || latency < 0 {
		return
	}
	m.terminalLatency.WithLabelValues(string(family)).Observe(float64(latency) / float64(time.Millisecond))
}

func knownFamily(family Family) bool {
	for _, candidate := range allFamilies {
		if family == candidate {
			return true
		}
	}
	return false
}

type snapshotCollector struct {
	registry *Registry
}

var (
	familyReadyDesc         = prometheus.NewDesc("farm_worker_family_ready", "Whether the Family is READY (1) or not (0).", []string{"family"}, nil)
	dueDepthDesc            = prometheus.NewDesc("farm_worker_due_depth", "Proven due work depth, or NaN when unavailable.", []string{"family"}, nil)
	oldestDueAgeDesc        = prometheus.NewDesc("farm_worker_oldest_due_age_ms", "Proven oldest due age in milliseconds, or NaN when unavailable.", []string{"family"}, nil)
	directDispatchRatioDesc = prometheus.NewDesc("farm_worker_direct_dispatch_hit_ratio", "Direct dispatch hits divided by direct hits plus worker takeovers, or NaN when unavailable.", []string{"family"}, nil)
	workerTakeoverRatioDesc = prometheus.NewDesc("farm_worker_worker_takeover_ratio", "Worker takeovers divided by direct hits plus worker takeovers, or NaN when unavailable.", []string{"family"}, nil)
)

func (c *snapshotCollector) Describe(output chan<- *prometheus.Desc) {
	output <- familyReadyDesc
	output <- dueDepthDesc
	output <- oldestDueAgeDesc
	output <- directDispatchRatioDesc
	output <- workerTakeoverRatioDesc
}

func (c *snapshotCollector) Collect(output chan<- prometheus.Metric) {
	if c == nil || c.registry == nil {
		return
	}
	for _, item := range c.registry.Snapshot().Families {
		ready := 0.0
		if item.State == StateReady {
			ready = 1
		}
		depth := math.NaN()
		if item.DueDepthAvailable {
			depth = float64(item.DueDepth)
		}
		oldest := math.NaN()
		if item.OldestDueAgeAvailable {
			oldest = float64(item.OldestDueAgeMs)
		}
		direct, takeover := math.NaN(), math.NaN()
		if item.DispatchRatioAvailable {
			direct, takeover = item.DirectDispatchHitRatio, item.WorkerTakeoverRatio
		}
		label := string(item.Family)
		output <- prometheus.MustNewConstMetric(familyReadyDesc, prometheus.GaugeValue, ready, label)
		output <- prometheus.MustNewConstMetric(dueDepthDesc, prometheus.GaugeValue, depth, label)
		output <- prometheus.MustNewConstMetric(oldestDueAgeDesc, prometheus.GaugeValue, oldest, label)
		output <- prometheus.MustNewConstMetric(directDispatchRatioDesc, prometheus.GaugeValue, direct, label)
		output <- prometheus.MustNewConstMetric(workerTakeoverRatioDesc, prometheus.GaugeValue, takeover, label)
	}
}
