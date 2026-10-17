// Package sourceordermetrics records the byte and write-amplification signals
// used to compare WorkRecord saga cost against SourceOrder commits.
package sourceordermetrics

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	workflowAdvanceWrites atomic.Uint64
	workflowAdvanceBytes  atomic.Uint64
	actorCommitBytes      atomic.Uint64
	sourceOrderBytes      atomic.Uint64
	releaseSeedTotal      atomic.Uint64
	plantInvariantReject  atomic.Uint64
	mailOverflowTotal     atomic.Uint64
)

var (
	WorkflowAdvanceWrites = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "farm_workflow_advance_writes_total",
		Help: "Redis luaAdvanceWork evaluations. The {work-v1} saga cost.",
	})
	WorkflowAdvanceBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "farm_workflow_advance_bytes_total",
		Help: "JSON bytes written by luaAdvanceWork (full WorkRecord rewrite).",
	})
	ActorCommitBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "farm_actor_commit_bytes_total",
		Help: "Logical AuthorityCommitSerializedBytes observed at actor commit.",
	})
	SourceOrderBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "farm_source_order_payload_bytes_total",
		Help: "Attached SourceOrder payload bytes committed with source facts.",
	})
	ReleaseSeedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "farm_plant_release_seed_total",
		Help: "Plant compensation ReleaseSeedReservation outcomes.",
	}, []string{"reason"})
	PlantInvariantReject = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "farm_plant_farm_invariant_reject_total",
		Help: "COMMIT_PLANT rejected because occupancy and farm plot disagreed.",
	})
	MailOverflowTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "farm_source_order_mail_overflow_total",
		Help: "Destination grants that overflowed inventory into MAIL_DELIVERY_ORDER.",
	})
)

func ObserveWorkflowAdvance(recordBytes int) {
	workflowAdvanceWrites.Add(1)
	if recordBytes > 0 {
		workflowAdvanceBytes.Add(uint64(recordBytes))
		WorkflowAdvanceBytes.Add(float64(recordBytes))
	}
	WorkflowAdvanceWrites.Inc()
}

func ObserveActorCommit(serializedBytes, orderPayloadBytes int) {
	if serializedBytes > 0 {
		actorCommitBytes.Add(uint64(serializedBytes))
		ActorCommitBytes.Add(float64(serializedBytes))
	}
	if orderPayloadBytes > 0 {
		sourceOrderBytes.Add(uint64(orderPayloadBytes))
		SourceOrderBytes.Add(float64(orderPayloadBytes))
	}
}

func ObserveReleaseSeed(reason string) {
	releaseSeedTotal.Add(1)
	ReleaseSeedTotal.WithLabelValues(reason).Inc()
}

func ObservePlantInvariantReject() {
	plantInvariantReject.Add(1)
	PlantInvariantReject.Inc()
}

func ObserveMailOverflow() {
	mailOverflowTotal.Add(1)
	MailOverflowTotal.Inc()
}

func Snapshot() (advances, advanceBytes, commitBytes, orderBytes, releases uint64) {
	return workflowAdvanceWrites.Load(), workflowAdvanceBytes.Load(),
		actorCommitBytes.Load(), sourceOrderBytes.Load(), releaseSeedTotal.Load()
}

func Register(reg prometheus.Registerer) {
	if reg == nil {
		return
	}
	reg.MustRegister(WorkflowAdvanceWrites, WorkflowAdvanceBytes, ActorCommitBytes,
		SourceOrderBytes, ReleaseSeedTotal, PlantInvariantReject, MailOverflowTotal)
}
