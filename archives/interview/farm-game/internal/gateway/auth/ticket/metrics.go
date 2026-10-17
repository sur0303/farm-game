package ticket

import (
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics 记录票据存储(Ticket Redis)的操作 QPS 与延迟,用于接入层容量观测。
// operation: save / redeem / activate;result: ok / redeem_failed / error。
type Metrics struct {
	Operations *prometheus.CounterVec
	Latency    *prometheus.HistogramVec
}

// NewMetrics 注册并返回票据指标。reg 为 nil 时使用 DefaultRegisterer。
func NewMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	m := &Metrics{
		Operations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm_gate", Subsystem: "ticket", Name: "operations_total",
			Help: "Ticket store operations by operation and result.",
		}, []string{"operation", "result"}),
		Latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "farm_gate", Subsystem: "ticket", Name: "operation_seconds",
			Help:    "Ticket store operation latency.",
			Buckets: []float64{.0001, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1},
		}, []string{"operation"}),
	}
	reg.MustRegister(m.Operations, m.Latency)
	return m
}

func (m *Metrics) observe(operation string, start time.Time, err error) {
	if m == nil {
		return
	}
	result := "ok"
	switch {
	case errors.Is(err, ErrRedeemFailed):
		result = "redeem_failed"
	case err != nil:
		result = "error"
	}
	m.Operations.WithLabelValues(operation, result).Inc()
	m.Latency.WithLabelValues(operation).Observe(time.Since(start).Seconds())
}
