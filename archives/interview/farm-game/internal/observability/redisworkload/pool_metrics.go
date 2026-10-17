package redisworkload

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

// poolMetrics exports the cumulative go-redis pool counters and current pool
// depth. Redis server latency does not include time spent waiting for a client
// connection; these metrics make that queue visible instead of attributing it
// to Redis execution.
type poolMetrics struct {
	targets map[string]redis.UniversalClient

	hits        *prometheus.Desc
	misses      *prometheus.Desc
	timeouts    *prometheus.Desc
	waits       *prometheus.Desc
	waitSeconds *prometheus.Desc
	unusable    *prometheus.Desc
	stale       *prometheus.Desc
	connections *prometheus.Desc
	pending     *prometheus.Desc
}

var _ prometheus.Collector = (*poolMetrics)(nil)

func newPoolMetrics(registerer prometheus.Registerer, targets DependencyTargets) (*poolMetrics, error) {
	if registerer == nil {
		registerer = prometheus.DefaultRegisterer
	}
	collector := &poolMetrics{
		targets: map[string]redis.UniversalClient{
			DomainAuthority:  targets.Authority,
			DomainRuntime:    targets.Runtime,
			DomainProjection: targets.Projection,
		},
		hits:        prometheus.NewDesc("farm_redis_pool_hits_total", "Free Redis connections obtained without creating or waiting.", []string{"domain"}, nil),
		misses:      prometheus.NewDesc("farm_redis_pool_misses_total", "Redis pool gets that could not use an immediately free connection.", []string{"domain"}, nil),
		timeouts:    prometheus.NewDesc("farm_redis_pool_timeouts_total", "Redis pool waits that timed out.", []string{"domain"}, nil),
		waits:       prometheus.NewDesc("farm_redis_pool_waits_total", "Redis commands that waited for a connection.", []string{"domain"}, nil),
		waitSeconds: prometheus.NewDesc("farm_redis_pool_wait_seconds_total", "Cumulative time spent waiting for a Redis connection.", []string{"domain"}, nil),
		unusable:    prometheus.NewDesc("farm_redis_pool_unusable_total", "Redis connections rejected as unusable.", []string{"domain"}, nil),
		stale:       prometheus.NewDesc("farm_redis_pool_stale_connections_total", "Stale Redis connections removed from the pool.", []string{"domain"}, nil),
		connections: prometheus.NewDesc("farm_redis_pool_connections", "Current Redis pool connections by state.", []string{"domain", "state"}, nil),
		pending:     prometheus.NewDesc("farm_redis_pool_pending_requests", "Current requests waiting for a Redis connection.", []string{"domain"}, nil),
	}
	if err := registerer.Register(collector); err != nil {
		return nil, err
	}
	return collector, nil
}

func (m *poolMetrics) Describe(ch chan<- *prometheus.Desc) {
	ch <- m.hits
	ch <- m.misses
	ch <- m.timeouts
	ch <- m.waits
	ch <- m.waitSeconds
	ch <- m.unusable
	ch <- m.stale
	ch <- m.connections
	ch <- m.pending
}

func (m *poolMetrics) Collect(ch chan<- prometheus.Metric) {
	if m == nil {
		return
	}
	for _, domain := range dependencyDomains {
		client := m.targets[domain]
		if client == nil {
			continue
		}
		stats := client.PoolStats()
		if stats == nil {
			continue
		}
		ch <- prometheus.MustNewConstMetric(m.hits, prometheus.CounterValue, float64(stats.Hits), domain)
		ch <- prometheus.MustNewConstMetric(m.misses, prometheus.CounterValue, float64(stats.Misses), domain)
		ch <- prometheus.MustNewConstMetric(m.timeouts, prometheus.CounterValue, float64(stats.Timeouts), domain)
		ch <- prometheus.MustNewConstMetric(m.waits, prometheus.CounterValue, float64(stats.WaitCount), domain)
		ch <- prometheus.MustNewConstMetric(m.waitSeconds, prometheus.CounterValue, float64(stats.WaitDurationNs)/1e9, domain)
		ch <- prometheus.MustNewConstMetric(m.unusable, prometheus.CounterValue, float64(stats.Unusable), domain)
		ch <- prometheus.MustNewConstMetric(m.stale, prometheus.CounterValue, float64(stats.StaleConns), domain)
		ch <- prometheus.MustNewConstMetric(m.connections, prometheus.GaugeValue, float64(stats.TotalConns), domain, "total")
		ch <- prometheus.MustNewConstMetric(m.connections, prometheus.GaugeValue, float64(stats.IdleConns), domain, "idle")
		ch <- prometheus.MustNewConstMetric(m.pending, prometheus.GaugeValue, float64(stats.PendingRequests), domain)
	}
}
