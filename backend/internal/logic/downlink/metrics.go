package downlink

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

// RPC 观测维度:method 区分两条出站链路,result 区分终态,attempt 记录重试
// 落在第几次,exhausted 标记有界重试耗尽。
const (
	metricMethodDeliver  = "deliver_batch"
	metricMethodActivate = "activate_subscription"

	metricResultOK          = "ok"
	metricResultUnavailable = "unavailable"
	metricResultError       = "error"
)

// DispatcherMetrics 承载 ActivateSubscription 与 DeliverBatch 的共享 RPC 观测。
// CounterVec 在构造时创建、可在未注册时自增;RegisterMetrics 再挂到注册表。
type DispatcherMetrics struct {
	attemptsVec  *prometheus.CounterVec // labels: method, attempt
	resultsVec   *prometheus.CounterVec // labels: method, result
	exhaustedVec *prometheus.CounterVec // labels: method
}

func newDispatcherMetrics() *DispatcherMetrics {
	return &DispatcherMetrics{
		attemptsVec: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "downlink", Name: "rpc_attempts_total",
			Help: "Downlink RPC attempts by method and attempt number.",
		}, []string{"method", "attempt"}),
		resultsVec: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "downlink", Name: "rpc_results_total",
			Help: "Terminal downlink RPC results by method and result.",
		}, []string{"method", "result"}),
		exhaustedVec: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "downlink", Name: "rpc_exhausted_total",
			Help: "Downlink transient retry loops exhausted by method.",
		}, []string{"method"}),
	}
}

func (m *DispatcherMetrics) attempt(method string, attempt int) {
	m.attemptsVec.WithLabelValues(method, strconv.Itoa(attempt)).Inc()
}

func (m *DispatcherMetrics) result(method, result string) {
	m.resultsVec.WithLabelValues(method, result).Inc()
}

func (m *DispatcherMetrics) exhausted(method string) {
	m.exhaustedVec.WithLabelValues(method).Inc()
}

// RegisterMetrics exposes lane lifecycle and failure evidence without putting
// Prometheus calls on Actor turns or the hot queueing path.
func RegisterMetrics(reg prometheus.Registerer, dispatcher *Dispatcher) {
	if reg == nil || dispatcher == nil {
		return
	}
	stats := func() DispatcherStats { return dispatcher.Stats() }
	reg.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "farm", Subsystem: "downlink", Name: "lanes_current",
			Help: "Current gateway-isolated downlink lanes.",
		}, func() float64 { return float64(stats().LanesCurrent) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "farm", Subsystem: "downlink", Name: "queue_items",
			Help: "Items currently queued across ordinary per-Gate downlink lanes.",
		}, func() float64 { return float64(stats().QueueItems) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "farm", Subsystem: "downlink", Name: "queue_capacity",
			Help: "Configured capacity of ordinary per-Gate downlink lanes.",
		}, func() float64 { return float64(stats().QueueCapacity) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "farm", Subsystem: "downlink", Name: "control_queue_items",
			Help: "Items currently queued across reserved control lanes.",
		}, func() float64 { return float64(stats().ControlQueueItems) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "farm", Subsystem: "downlink", Name: "control_queue_capacity",
			Help: "Configured capacity of reserved control lanes.",
		}, func() float64 { return float64(stats().ControlQueueCapacity) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "downlink", Name: "lanes_created_total",
			Help: "Gateway downlink lanes created.",
		}, func() float64 { return float64(stats().LanesCreated) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "downlink", Name: "lanes_retired_total",
			Help: "Idle gateway downlink lanes retired.",
		}, func() float64 { return float64(stats().LanesRetired) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "downlink", Name: "queue_full_total",
			Help: "Items rejected by a full per-Gate downlink lane.",
		}, func() float64 { return float64(stats().QueueFull) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "downlink", Name: "rpc_failure_total",
			Help: "Per-Gate downlink batches failed or timed out.",
		}, func() float64 { return float64(stats().RPCFailures) }),
	)
	if dispatcher.metrics != nil {
		reg.MustRegister(dispatcher.metrics.attemptsVec, dispatcher.metrics.resultsVec, dispatcher.metrics.exhaustedVec)
	}
}
