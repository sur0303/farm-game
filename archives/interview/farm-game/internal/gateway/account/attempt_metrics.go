package account

import "github.com/prometheus/client_golang/prometheus"

// AttemptMetrics intentionally uses only bounded labels. Raw usernames, IPs
// and device identifiers must never become metric labels.
type AttemptMetrics struct {
	attempts   *prometheus.CounterVec
	limited    *prometheus.CounterVec
	retryAfter *prometheus.HistogramVec
}

func NewAttemptMetrics(reg prometheus.Registerer) *AttemptMetrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	metrics := &AttemptMetrics{
		attempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm_gate",
			Subsystem: "account_auth",
			Name:      "attempts_total",
			Help:      "Web account login/register attempts by bounded outcome.",
		}, []string{"operation", "result"}),
		limited: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm_gate",
			Subsystem: "account_auth",
			Name:      "limited_total",
			Help:      "Rejected Web account attempts by risk dimension.",
		}, []string{"operation", "dimension"}),
		retryAfter: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "farm_gate",
			Subsystem: "account_auth",
			Name:      "retry_after_seconds",
			Help:      "Retry delay returned for rate-limited account requests.",
			Buckets:   []float64{1, 5, 15, 30, 60, 120, 300},
		}, []string{"operation"}),
	}
	reg.MustRegister(metrics.attempts, metrics.limited, metrics.retryAfter)
	return metrics
}

func (m *AttemptMetrics) Observe(operation string, decision AttemptDecision, err error) {
	if m == nil {
		return
	}
	result := "allowed"
	switch {
	case err != nil:
		result = "error"
	case !decision.Allowed:
		result = "limited"
	}
	m.attempts.WithLabelValues(operation, result).Inc()
	if decision.Allowed || err != nil {
		return
	}
	for _, dimension := range decision.LimitedBy {
		m.limited.WithLabelValues(operation, dimension).Inc()
	}
	retryAfter := decision.RetryAfter
	if retryAfter < 0 {
		retryAfter = 0
	}
	m.retryAfter.WithLabelValues(operation).Observe(retryAfter.Seconds())
}
