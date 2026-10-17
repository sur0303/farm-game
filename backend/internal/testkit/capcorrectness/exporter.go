// Package capcorrectness exports the hard correctness gates a capacity run
// reads through its oracle. Every gate counts something that must never happen,
// so any nonzero value voids the run.
//
// The one failure this package exists to prevent is a gate that reads zero
// because nobody measured it. An earlier harness declared the same gate names
// in a struct and left most of them unassigned, so they serialized as zero and
// runs recorded a passing correctness claim over invariants that were never
// observed. A gate here is therefore either measured or absent, never zero by
// default: an absent gate makes the oracle report an incomplete observation,
// which costs a run, while a fabricated zero costs the truth of every run that
// reads it.
package capcorrectness

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Kind selects how the oracle reads a gate: a counter is compared against the
// baseline armed at the start of the run, a gauge must be zero as it stands.
type Kind int

const (
	KindCounter Kind = iota + 1
	KindGauge
)

type Gate struct {
	Name string
	Kind Kind
	Help string
}

// Gates is the canonical contract, matching the oracle adapter configuration.
// The oracle requires the served set to equal the required set exactly, so this
// list is the single place the membership is stated.
func Gates() []Gate {
	return []Gate{
		{"duplicate_grant_total", KindCounter, "Grants applied more than once for the same logical award."},
		{"duplicate_debit_total", KindCounter, "Debits applied more than once for the same logical charge."},
		{"stale_holder_commit_success_total", KindCounter, "Commits that succeeded from a holder generation already taken over."},
		{"farm_version_gap_total", KindCounter, "Gaps observed in a farm record version sequence."},
		{"workflow_step_unknown_residual", KindGauge, "Workflow steps left in an unknown state."},
		{"due_residual_unexpected", KindGauge, "Due items left behind after their scheduled work drained."},
		{"h5_pending_journal_residual", KindGauge, "H5 journal entries left pending."},
		{"authority_client_mismatch_total", KindCounter, "Authority clients that reached an authority they do not belong to."},
		{"silent_drop_total", KindCounter, "Messages dropped without surfacing an error to anyone."},
		{"unexpected_lease_expiry_total", KindCounter, "Leases that expired outside a planned expiry."},
		{"friend_pair_index_unrepaired", KindGauge, "Friend pair index entries found inconsistent and left unrepaired."},
		{"invalid_order_total", KindCounter, "Operations applied out of a required order."},
		{"wrong_scope_dispatch_total", KindCounter, "Requests dispatched to a scope that does not own them."},
		{"record_version_regression_total", KindCounter, "Record versions that moved backwards."},
		{"receipt_conflict_leak_total", KindCounter, "Receipt conflicts that leaked instead of being resolved."},
	}
}

// Producer observes one gate. An error means the invariant could not be
// observed right now, which the exporter reports as absence rather than as a
// clean result.
type Producer interface {
	Observe(context.Context) (float64, error)
}

// ProducerFunc adapts a function to Producer.
type ProducerFunc func(context.Context) (float64, error)

func (f ProducerFunc) Observe(ctx context.Context) (float64, error) { return f(ctx) }

// Unmeasured binds a gate that has no instrumentation yet. It is deliberately
// not the same as omitting the gate from the map: New rejects an incomplete
// map, so every gap has to be written down here with a reason, and the reason
// is what /status shows the operator who asks why the run was incomplete.
func Unmeasured(reason string) Producer {
	return ProducerFunc(func(context.Context) (float64, error) {
		return 0, fmt.Errorf("not instrumented: %s", reason)
	})
}

// Observation is one gate as of one moment: either a value the oracle can read
// or the reason it could not be read.
type Observation struct {
	Gate    string  `json:"gate"`
	Kind    string  `json:"kind"`
	Value   float64 `json:"value,omitempty"`
	Problem string  `json:"problem,omitempty"`
}

// Snapshot reports every gate with its value or its problem. Collect stays
// silent about absent gates by design, so this is how the gap becomes legible
// without weakening what the metrics endpoint claims.
func (e *Exporter) Snapshot(ctx context.Context) []Observation {
	ctx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()
	out := make([]Observation, 0, len(e.gates))
	for _, bound := range e.gates {
		observation := Observation{Gate: bound.gate.Name, Kind: kindName(bound.gate.Kind)}
		value, err := bound.producer.Observe(ctx)
		switch {
		case err != nil:
			observation.Problem = err.Error()
		case !observable(value):
			observation.Problem = fmt.Sprintf("value %v is not a count", value)
		default:
			observation.Value = value
		}
		out = append(out, observation)
	}
	return out
}

func kindName(kind Kind) string {
	if kind == KindGauge {
		return "gauge"
	}
	return "counter"
}

type boundGate struct {
	gate      Gate
	desc      *prometheus.Desc
	valueType prometheus.ValueType
	producer  Producer
}

type Exporter struct {
	gates   []boundGate
	timeout time.Duration
}

// New refuses to start unless every canonical gate has a producer. A partial
// exporter would serve a set the oracle rejects, but worse, it invites binding
// the gates that are easy and leaving the rest to look clean.
func New(producers map[string]Producer, timeout time.Duration) (*Exporter, error) {
	if timeout <= 0 {
		return nil, errors.New("capcorrectness: observation timeout is required")
	}
	exporter := &Exporter{timeout: timeout}
	var unbound []string
	for _, gate := range Gates() {
		producer, found := producers[gate.Name]
		if !found || producer == nil {
			unbound = append(unbound, gate.Name)
			continue
		}
		exporter.gates = append(exporter.gates, boundGate{
			gate:      gate,
			desc:      prometheus.NewDesc(gate.Name, gate.Help, nil, nil),
			valueType: valueType(gate.Kind),
			producer:  producer,
		})
	}
	if len(unbound) > 0 {
		sort.Strings(unbound)
		return nil, fmt.Errorf("capcorrectness: no producer for %v", unbound)
	}
	if extra := extraNames(producers); len(extra) > 0 {
		return nil, fmt.Errorf("capcorrectness: unknown gate %v", extra)
	}
	return exporter, nil
}

func valueType(kind Kind) prometheus.ValueType {
	if kind == KindGauge {
		return prometheus.GaugeValue
	}
	return prometheus.CounterValue
}

func extraNames(producers map[string]Producer) []string {
	known := map[string]bool{}
	for _, gate := range Gates() {
		known[gate.Name] = true
	}
	var extra []string
	for name := range producers {
		if !known[name] {
			extra = append(extra, name)
		}
	}
	sort.Strings(extra)
	return extra
}

func (e *Exporter) Describe(into chan<- *prometheus.Desc) {
	for _, bound := range e.gates {
		into <- bound.desc
	}
}

// Collect omits a gate whose producer failed or returned a value the oracle
// cannot accept. Omission is the honest answer: the oracle then reports the
// observation as incomplete instead of reading an invented zero.
func (e *Exporter) Collect(into chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), e.timeout)
	defer cancel()
	for _, bound := range e.gates {
		value, err := bound.producer.Observe(ctx)
		if err != nil || !observable(value) {
			continue
		}
		into <- prometheus.MustNewConstMetric(bound.desc, bound.valueType, value)
	}
}

// observable rejects values the oracle would refuse anyway, so a producer bug
// surfaces as an incomplete observation rather than as a gate that silently
// reads as a different number than it measured.
func observable(value float64) bool {
	return value >= 0 && value == float64(int64(value))
}

func (e *Exporter) Registry() (*prometheus.Registry, error) {
	registry := prometheus.NewRegistry()
	if err := registry.Register(e); err != nil {
		return nil, err
	}
	return registry, nil
}
