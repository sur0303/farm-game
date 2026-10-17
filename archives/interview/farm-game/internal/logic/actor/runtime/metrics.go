package runtime

import (
	"github.com/prometheus/client_golang/prometheus"
)

type StatsSource func() Stats

type statsSample struct {
	desc      *prometheus.Desc
	valueType prometheus.ValueType
	read      func(Stats) float64
}

// statsCollector emits the farm_actor_* series from one Stats snapshot per scrape.
type statsCollector struct {
	source  StatsSource
	samples []statsSample
}

var _ prometheus.Collector = (*statsCollector)(nil)

func newStatsCollector(kind string, source StatsSource) *statsCollector {
	labels := prometheus.Labels{"actor_kind": kind}
	desc := func(name, help string) *prometheus.Desc {
		return prometheus.NewDesc(
			prometheus.BuildFQName("farm", "actor", name),
			help,
			nil,
			labels,
		)
	}
	return &statsCollector{
		source: source,
		samples: []statsSample{
			{
				desc:      desc("active_keys", "Active keyed actor mailboxes."),
				valueType: prometheus.GaugeValue,
				read:      func(stats Stats) float64 { return float64(stats.ActiveKeys) },
			},
			{
				desc:      desc("mailbox_queued", "Queued actor turns across this process (user + system)."),
				valueType: prometheus.GaugeValue,
				read:      func(stats Stats) float64 { return float64(stats.Queued) },
			},
			{
				desc:      desc("user_mailbox_queued", "Queued user turns across this process."),
				valueType: prometheus.GaugeValue,
				read:      func(stats Stats) float64 { return float64(stats.UserQueued) },
			},
			{
				desc:      desc("user_mailbox_bytes", "Queued user mailbox payload bytes across this process."),
				valueType: prometheus.GaugeValue,
				read:      func(stats Stats) float64 { return float64(stats.UserBytes) },
			},
			{
				desc: desc(
					"user_mailbox_oldest_wait_seconds",
					"Oldest user mailbox head wait across keys (enqueue timestamp FIFO).",
				),
				valueType: prometheus.GaugeValue,
				read:      func(stats Stats) float64 { return stats.OldestUserWait.Seconds() },
			},
			{
				desc:      desc("system_queue_queued", "Queued system turns across this process."),
				valueType: prometheus.GaugeValue,
				read:      func(stats Stats) float64 { return float64(stats.SystemQueued) },
			},
			{
				desc:      desc("mailbox_max_depth", "Maximum current user mailbox depth in this process."),
				valueType: prometheus.GaugeValue,
				read:      func(stats Stats) float64 { return float64(stats.MaxDepth) },
			},
			{
				desc:      desc("submit_total", "Accepted actor turns."),
				valueType: prometheus.CounterValue,
				read:      func(stats Stats) float64 { return float64(stats.Submitted) },
			},
			{
				desc:      desc("mailbox_rejected_total", "Actor turns rejected because a queue was full (user + system)."),
				valueType: prometheus.CounterValue,
				read:      func(stats Stats) float64 { return float64(stats.Rejected) },
			},
			{
				desc:      desc("user_mailbox_rejected_total", "User turns rejected because the user mailbox was full."),
				valueType: prometheus.CounterValue,
				read:      func(stats Stats) float64 { return float64(stats.UserRejected) },
			},
			{
				desc:      desc("system_queue_rejected_total", "System turns rejected because the system queue was full."),
				valueType: prometheus.CounterValue,
				read:      func(stats Stats) float64 { return float64(stats.SystemRejected) },
			},
			{
				desc: desc(
					"mailbox_retired_total",
					"Mailboxes retired per key after going idle (goroutine and queues released).",
				),
				valueType: prometheus.CounterValue,
				read:      func(stats Stats) float64 { return float64(stats.MailboxRetiredTotal) },
			},
			{
				desc: desc(
					"mailbox_retire_busy_total",
					"Per-key retire attempts kept because queued or in-flight work remained.",
				),
				valueType: prometheus.CounterValue,
				read:      func(stats Stats) float64 { return float64(stats.MailboxRetireBusyTotal) },
			},
			{
				desc: desc(
					"mailbox_retire_stale_total",
					"Per-key retire attempts abandoned on generation mismatch (newer mailbox reused the key).",
				),
				valueType: prometheus.CounterValue,
				read:      func(stats Stats) float64 { return float64(stats.MailboxRetireStaleTotal) },
			},
		},
	}
}

func (c *statsCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, sample := range c.samples {
		ch <- sample.desc
	}
}

func (c *statsCollector) Collect(ch chan<- prometheus.Metric) {
	stats := c.source()
	for _, sample := range c.samples {
		ch <- prometheus.MustNewConstMetric(sample.desc, sample.valueType, sample.read(stats))
	}
}

// RegisterMetrics registers farm_actor_* runtime gauges/counters from a StatsSource.
// Signature is stable for cmd/*-svr callers; Batch 2.6 adds mailbox bytes / oldest_wait.
// Each scrape snapshots source() once so Prometheus does not walk every mailbox per series.
func RegisterMetrics(reg prometheus.Registerer, kind string, source StatsSource) {
	reg.MustRegister(newStatsCollector(kind, source))
}
