package contracts

import (
	"fmt"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

// The compatibility counter has exactly two fixed caller series. Request,
// work, actor, command, and holder identities are deliberately not labels, so
// legacy rollout telemetry cannot grow with user-controlled cardinality.
var legacyParticipantRouteLeaseProof [2]atomic.Uint64

func participantCallerMetricIndex(caller ParticipantCallerKind) (int, bool) {
	switch caller {
	case ParticipantCallerCoordinator:
		return 0, true
	case ParticipantCallerWorkerRecovery:
		return 1, true
	default:
		return 0, false
	}
}

func observeLegacyParticipantRouteLeaseProof(caller ParticipantCallerKind) {
	if index, ok := participantCallerMetricIndex(caller); ok {
		legacyParticipantRouteLeaseProof[index].Add(1)
	}
}

// LegacyParticipantRouteLeaseProofCount returns the process-local accepted
// legacy-proof count for deterministic rollout tests and health diagnostics.
func LegacyParticipantRouteLeaseProofCount(caller ParticipantCallerKind) uint64 {
	if index, ok := participantCallerMetricIndex(caller); ok {
		return legacyParticipantRouteLeaseProof[index].Load()
	}
	return 0
}

// RegisterParticipantRouteLeaseProofMetrics exposes the bounded compatibility
// counter. Strict mode is controlled independently by RequireRouteLeaseProof.
func RegisterParticipantRouteLeaseProofMetrics(registerer prometheus.Registerer) error {
	if registerer == nil {
		return fmt.Errorf("contracts: Participant route lease metric registerer is required")
	}
	for index, caller := range []string{"coordinator", "worker_recovery"} {
		index := index
		collector := prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name:        "farm_participant_legacy_route_lease_proof_accepted_total",
			Help:        "Participant requests accepted during rollout with both route lease epoch and deadline absent.",
			ConstLabels: prometheus.Labels{"caller": caller},
		}, func() float64 {
			return float64(legacyParticipantRouteLeaseProof[index].Load())
		})
		if err := registerer.Register(collector); err != nil {
			return err
		}
	}
	return nil
}
