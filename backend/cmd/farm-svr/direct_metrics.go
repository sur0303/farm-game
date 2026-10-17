package main

import (
	"farmgame/internal/worker/carerecovery"

	"github.com/prometheus/client_golang/prometheus"
)

func registerSourceOrderDirectMetrics(registry *prometheus.Registry, dispatcher *carerecovery.DirectDispatcher) {
	if registry == nil || dispatcher == nil {
		return
	}
	registry.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "farm", Subsystem: "source_order_direct", Name: "queue_depth",
			Help: "Current durable SourceOrders waiting in the in-process direct dispatcher.",
		}, func() float64 { return float64(dispatcher.Stats().Depth) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "farm", Subsystem: "source_order_direct", Name: "queue_bytes",
			Help: "Current payload-byte budget held by the direct dispatcher queue.",
		}, func() float64 { return float64(dispatcher.Stats().Bytes) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "source_order_direct", Name: "enqueued_total",
			Help: "Durable SourceOrders accepted by the direct dispatcher queue.",
		}, func() float64 { return float64(dispatcher.Stats().Enqueued) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "source_order_direct", Name: "dropped_total",
			Help: "Direct dispatch attempts rejected by queue item/byte bounds.",
		}, func() float64 { return float64(dispatcher.Stats().Dropped) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "source_order_direct", Name: "dispatched_total",
			Help: "SourceOrders whose direct execution started.",
		}, func() float64 { return float64(dispatcher.Stats().Dispatched) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "source_order_direct", Name: "completed_total",
			Help: "SourceOrders completed by direct dispatch without Worker takeover.",
		}, func() float64 { return float64(dispatcher.Stats().Completed) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "source_order_direct", Name: "rescheduled_total",
			Help: "Direct attempts durably rescheduled for retry/recovery.",
		}, func() float64 { return float64(dispatcher.Stats().Rescheduled) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "source_order_direct", Name: "failed_total",
			Help: "Direct queue items whose dispatch returned an error.",
		}, func() float64 { return float64(dispatcher.Stats().Failed) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "source_order_direct", Name: "commit_proof_attached_total",
			Help: "Direct CARE/HARVEST envelopes carrying a proof minted from an exact first-COMMITTED stamp.",
		}, func() float64 { return float64(dispatcher.Stats().ProofAttached) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "source_order_direct", Name: "commit_proof_unavailable_total",
			Help: "Direct CARE/HARVEST proof attempts withheld because exact evidence, route, freshness, or encoding was invalid.",
		}, func() float64 { return float64(dispatcher.Stats().ProofUnavailable) }),
	)
}
