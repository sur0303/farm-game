package loadbot

import (
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const MetricsSchemaVersion = "mixed-farm-bot/metrics/v1"

const (
	MetricRunEventsTotal       = "run_events_total"
	MetricBotActionsTotal      = "bot_actions_total"
	MetricLeaseEventsTotal     = "lease_events_total"
	MetricPhaseDurationSeconds = "phase_duration_seconds"
	MetricAssignments          = "assignments"
)

const (
	MetricPhasePrepare = "prepare"
	MetricPhaseFixture = "fixture"
	MetricPhaseAssign  = "assign"
	MetricPhaseExecute = "execute"
	MetricPhaseCleanup = "cleanup"
	MetricPhaseOther   = "other"
)

const (
	MetricOutcomeOK       = "ok"
	MetricOutcomeError    = "error"
	MetricOutcomeRejected = "rejected"
	MetricOutcomeExpired  = "expired"
	MetricOutcomeFenced   = "fenced"
	MetricOutcomeSkipped  = "skipped"
	MetricOutcomeOther    = "other"
)

const (
	MetricLeaseAssigned = "assigned"
	MetricLeaseRenewed  = "renewed"
	MetricLeaseReleased = "released"
	MetricLeaseExpired  = "expired"
	MetricLeaseRejected = "rejected"
	MetricLeaseOther    = "other"
)

const (
	MetricAssignmentActive   = "active"
	MetricAssignmentExpired  = "expired"
	MetricAssignmentReleased = "released"
	MetricAssignmentOther    = "other"
)

var metricPhases = map[string]struct{}{
	MetricPhasePrepare: {}, MetricPhaseFixture: {}, MetricPhaseAssign: {},
	MetricPhaseExecute: {}, MetricPhaseCleanup: {}, MetricPhaseOther: {},
}

var metricOutcomes = map[string]struct{}{
	MetricOutcomeOK: {}, MetricOutcomeError: {}, MetricOutcomeRejected: {},
	MetricOutcomeExpired: {}, MetricOutcomeFenced: {}, MetricOutcomeSkipped: {},
	MetricOutcomeOther: {},
}

var metricLeaseEvents = map[string]struct{}{
	MetricLeaseAssigned: {}, MetricLeaseRenewed: {}, MetricLeaseReleased: {},
	MetricLeaseExpired: {}, MetricLeaseRejected: {}, MetricLeaseOther: {},
}

var metricAssignmentStates = map[string]struct{}{
	MetricAssignmentActive: {}, MetricAssignmentExpired: {}, MetricAssignmentReleased: {},
	MetricAssignmentOther: {},
}

// Metrics contains only bounded labels. In particular, it deliberately has no
// player_id, farm_id, operation_id, run_id, shard, worker_id, assignment_id,
// or other identity-bearing label.
type Metrics struct {
	runEvents     *prometheus.CounterVec
	botActions    *prometheus.CounterVec
	leaseEvents   *prometheus.CounterVec
	phaseDuration *prometheus.HistogramVec
	assignments   *prometheus.GaugeVec
}

func NewMetrics(registerer prometheus.Registerer) *Metrics {
	if registerer == nil {
		registerer = prometheus.DefaultRegisterer
	}
	metrics := &Metrics{
		runEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "mixed_farm_bot", Name: MetricRunEventsTotal,
			Help: "Mixed farm bot run lifecycle events with bounded phase and outcome labels.",
		}, []string{"phase", "outcome"}),
		botActions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "mixed_farm_bot", Name: MetricBotActionsTotal,
			Help: "Mixed farm bot action observations with bounded action and outcome labels.",
		}, []string{"action", "outcome"}),
		leaseEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "mixed_farm_bot", Name: MetricLeaseEventsTotal,
			Help: "Mixed farm bot worker lease events with bounded event and outcome labels.",
		}, []string{"event", "outcome"}),
		phaseDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "farm", Subsystem: "mixed_farm_bot", Name: MetricPhaseDurationSeconds,
			Help:    "Mixed farm bot phase duration observations with bounded phase and outcome labels.",
			Buckets: []float64{.0001, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, []string{"phase", "outcome"}),
		assignments: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "farm", Subsystem: "mixed_farm_bot", Name: MetricAssignments,
			Help: "Current worker assignment counts by bounded assignment state; identities are never labels.",
		}, []string{"state"}),
	}
	registerer.MustRegister(metrics.runEvents, metrics.botActions, metrics.leaseEvents, metrics.phaseDuration, metrics.assignments)
	return metrics
}

func (metrics *Metrics) ObserveRun(phase, outcome string) {
	if metrics == nil {
		return
	}
	metrics.runEvents.WithLabelValues(normalizeMetricValue(phase, metricPhases), normalizeMetricValue(outcome, metricOutcomes)).Inc()
}

func (metrics *Metrics) ObserveRunEvent(phase, outcome string) {
	metrics.ObserveRun(phase, outcome)
}

func (metrics *Metrics) ObserveAction(action Action, outcome string) {
	if metrics == nil {
		return
	}
	metrics.botActions.WithLabelValues(normalizeAction(action), normalizeMetricValue(outcome, metricOutcomes)).Inc()
}

func (metrics *Metrics) ObserveBotAction(action Action, outcome string) {
	metrics.ObserveAction(action, outcome)
}

func (metrics *Metrics) ObserveLease(event, outcome string) {
	if metrics == nil {
		return
	}
	metrics.leaseEvents.WithLabelValues(normalizeMetricValue(event, metricLeaseEvents), normalizeMetricValue(outcome, metricOutcomes)).Inc()
}

func (metrics *Metrics) ObserveLeaseEvent(event, outcome string) {
	metrics.ObserveLease(event, outcome)
}

func (metrics *Metrics) ObserveDuration(phase, outcome string, elapsed time.Duration) {
	if metrics == nil || elapsed < 0 {
		return
	}
	metrics.phaseDuration.WithLabelValues(normalizeMetricValue(phase, metricPhases), normalizeMetricValue(outcome, metricOutcomes)).Observe(elapsed.Seconds())
}

func (metrics *Metrics) ObservePhaseDuration(phase, outcome string, elapsed time.Duration) {
	metrics.ObserveDuration(phase, outcome, elapsed)
}

func (metrics *Metrics) SetAssignments(state string, count int) {
	if metrics == nil {
		return
	}
	if count < 0 {
		count = 0
	}
	metrics.assignments.WithLabelValues(normalizeMetricValue(state, metricAssignmentStates)).Set(float64(count))
}

func normalizeAction(action Action) string {
	if err := action.Validate(); err == nil {
		return string(action)
	}
	return MetricOutcomeOther
}

func normalizeMetricValue(value string, allowed map[string]struct{}) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if _, ok := allowed[value]; ok {
		return value
	}
	return MetricOutcomeOther
}
