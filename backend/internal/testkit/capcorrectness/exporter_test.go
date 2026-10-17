package capcorrectness

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func constant(value float64) Producer {
	return ProducerFunc(func(context.Context) (float64, error) { return value, nil })
}

func failing() Producer {
	return ProducerFunc(func(context.Context) (float64, error) { return 0, errors.New("source down") })
}

func allZero() map[string]Producer {
	producers := map[string]Producer{}
	for _, gate := range Gates() {
		producers[gate.Name] = constant(0)
	}
	return producers
}

func exposition(t *testing.T, exporter *Exporter) string {
	t.Helper()
	registry, err := exporter.Registry()
	if err != nil {
		t.Fatal(err)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var builder strings.Builder
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			value := metric.GetCounter().GetValue()
			if metric.GetGauge() != nil {
				value = metric.GetGauge().GetValue()
			}
			builder.WriteString(family.GetName())
			builder.WriteString(" ")
			builder.WriteString(strconv.FormatFloat(value, 'g', -1, 64))
			builder.WriteString("\n")
		}
	}
	return builder.String()
}

func TestEveryGateNeedsAProducer(t *testing.T) {
	producers := allZero()
	delete(producers, "silent_drop_total")
	delete(producers, "duplicate_grant_total")
	_, err := New(producers, time.Second)
	if err == nil {
		t.Fatal("a partial exporter must not start")
	}
	for _, name := range []string{"duplicate_grant_total", "silent_drop_total"} {
		if !strings.Contains(err.Error(), name) {
			t.Fatalf("error must name the unbound gate %s: %v", name, err)
		}
	}
}

func TestUnknownGateIsRejected(t *testing.T) {
	producers := allZero()
	producers["invented_total"] = constant(0)
	if _, err := New(producers, time.Second); err == nil {
		t.Fatal("an unknown gate must not be served: the oracle requires an exact set")
	}
}

// A gate whose source is unreachable must disappear from the exposition. If it
// were served as zero instead, a run would record a passing correctness claim
// over an invariant nobody observed.
func TestUnobservableGateIsOmittedRatherThanZero(t *testing.T) {
	producers := allZero()
	producers["silent_drop_total"] = failing()
	exporter, err := New(producers, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	exposed := exposition(t, exporter)
	if strings.Contains(exposed, "silent_drop_total") {
		t.Fatalf("unobservable gate was served anyway:\n%s", exposed)
	}
	if !strings.Contains(exposed, "duplicate_grant_total 0") {
		t.Fatalf("observable gates must still be served:\n%s", exposed)
	}
}

func TestNonsenseValueIsOmitted(t *testing.T) {
	for name, value := range map[string]float64{"negative": -1, "fractional": 1.5} {
		t.Run(name, func(t *testing.T) {
			producers := allZero()
			producers["invalid_order_total"] = constant(value)
			exporter, err := New(producers, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(exposition(t, exporter), "invalid_order_total") {
				t.Fatalf("value %v is not a gate count and must not be served", value)
			}
		})
	}
}

func TestServedSetMatchesTheContract(t *testing.T) {
	exporter, err := New(allZero(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	exposed := exposition(t, exporter)
	for _, gate := range Gates() {
		if !strings.Contains(exposed, gate.Name+" ") {
			t.Fatalf("gate %s is missing from the exposition:\n%s", gate.Name, exposed)
		}
	}
	if lines := strings.Count(strings.TrimSpace(exposed), "\n") + 1; lines != len(Gates()) {
		t.Fatalf("served %d metrics for %d gates:\n%s", lines, len(Gates()), exposed)
	}
}

func TestGateKindsMatchTheOracleModes(t *testing.T) {
	gauges := map[string]bool{
		"workflow_step_unknown_residual": true, "due_residual_unexpected": true,
		"h5_pending_journal_residual": true, "friend_pair_index_unrepaired": true,
	}
	for _, gate := range Gates() {
		want := KindCounter
		if gauges[gate.Name] {
			want = KindGauge
		}
		if gate.Kind != want {
			t.Fatalf("%s: kind %v does not match the oracle mode", gate.Name, gate.Kind)
		}
	}
}

func TestObservationRunsUnderTheConfiguredTimeout(t *testing.T) {
	producers := allZero()
	observed := make(chan time.Duration, 1)
	producers["silent_drop_total"] = ProducerFunc(func(ctx context.Context) (float64, error) {
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Error("producer must receive a deadline")
			return 0, errors.New("no deadline")
		}
		observed <- time.Until(deadline)
		return 0, nil
	})
	exporter, err := New(producers, 250*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	_ = exposition(t, exporter)
	if budget := <-observed; budget <= 0 || budget > 250*time.Millisecond {
		t.Fatalf("producer budget %v does not follow the configured timeout", budget)
	}
}

var _ prometheus.Collector = (*Exporter)(nil)
