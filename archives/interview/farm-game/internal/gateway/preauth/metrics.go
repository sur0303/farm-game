package preauth

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	Connections prometheus.Gauge
	Bytes       prometheus.Gauge
	Rejected    *prometheus.CounterVec
	Transitions *prometheus.CounterVec
	Duration    *prometheus.HistogramVec
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	metrics := &Metrics{
		Connections: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "farm_gate", Subsystem: "preauth", Name: "connections",
			Help: "Current transport connections waiting for ClientHello activation.",
		}),
		Bytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "farm_gate", Subsystem: "preauth", Name: "bytes",
			Help: "Current admitted FarmFrame bytes held by PRE_AUTH connections.",
		}),
		Rejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm_gate", Subsystem: "preauth", Name: "rejected_total",
			Help: "PRE_AUTH admission rejections by bounded reason.",
		}, []string{"reason"}),
		Transitions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm_gate", Subsystem: "preauth", Name: "transitions_total",
			Help: "PRE_AUTH connection exits by result.",
		}, []string{"result"}),
		Duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "farm_gate", Subsystem: "preauth", Name: "duration_seconds",
			Help:    "Time spent waiting for ClientHello activation.",
			Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
		}, []string{"result"}),
	}
	reg.MustRegister(metrics.Connections, metrics.Bytes, metrics.Rejected, metrics.Transitions, metrics.Duration)
	return metrics
}
