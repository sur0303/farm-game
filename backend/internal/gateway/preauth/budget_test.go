package preauth

import (
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func testController() *Controller {
	return New(Config{
		MaxConnections: 2, MaxConnectionsPerIP: 1,
		MaxFramesPerConnection: 1, MaxBytesPerConnection: 8,
		MaxBytesPerIP: 8, MaxBytesPod: 12,
	})
}

func TestControllerBoundsConnectionsFramesAndBytes(t *testing.T) {
	controller := testController()
	if err := controller.Open("c1", 1, "192.0.2.1"); err != nil {
		t.Fatal(err)
	}
	if err := controller.Open("c2", 1, "192.0.2.1"); ReasonOf(err) != ReasonIPConnections {
		t.Fatalf("same-IP connection err=%v", err)
	}
	if err := controller.Open("c2", 1, "192.0.2.2"); err != nil {
		t.Fatal(err)
	}
	if err := controller.Open("c3", 1, "192.0.2.3"); ReasonOf(err) != ReasonPodConnections {
		t.Fatalf("pod connection err=%v", err)
	}
	if err := controller.Admit("c1", 1, 8); err != nil {
		t.Fatal(err)
	}
	if err := controller.Admit("c1", 1, 1); ReasonOf(err) != ReasonFrames {
		t.Fatalf("second frame err=%v", err)
	}
	if err := controller.Admit("c2", 1, 5); ReasonOf(err) != ReasonPodBytes {
		t.Fatalf("pod bytes err=%v", err)
	}
	snapshot := controller.Snapshot("192.0.2.1")
	if snapshot.Connections != 2 || snapshot.PodBytes != 8 ||
		snapshot.IPConnections != 1 || snapshot.IPBytes != 8 {
		t.Fatalf("snapshot=%s", snapshot)
	}
}

func TestControllerReleasesEveryDimensionOnAuthCloseAndTimeout(t *testing.T) {
	controller := testController()
	for index, finish := range []func(string, uint64) bool{
		controller.Authenticate, controller.Close, controller.Timeout,
	} {
		connectionID := "c" + string(rune('1'+index))
		if err := controller.Open(connectionID, 1, "2001:db8::1"); err != nil {
			t.Fatal(err)
		}
		if err := controller.Admit(connectionID, 1, 4); err != nil {
			t.Fatal(err)
		}
		if !finish(connectionID, 1) {
			t.Fatalf("finish %d did not remove state", index)
		}
		if snapshot := controller.Snapshot("2001:db8::1"); snapshot != (Snapshot{}) {
			t.Fatalf("finish %d leaked budget: %s", index, snapshot)
		}
	}
}

func TestControllerRequiresExactConnectionFence(t *testing.T) {
	controller := testController()
	if err := controller.Open("reused", 1, "192.0.2.1"); err != nil {
		t.Fatal(err)
	}
	if err := controller.Admit("reused", 2, 1); ReasonOf(err) != ReasonUnknownFence {
		t.Fatalf("wrong generation err=%v", err)
	}
	if controller.Close("reused", 2) {
		t.Fatal("wrong generation removed current budget")
	}
	if !controller.IsPending("reused", 1) {
		t.Fatal("current generation disappeared")
	}
}

func TestControllerDoesNotTrustMalformedIPAsBucketIdentity(t *testing.T) {
	controller := New(Config{
		MaxConnections: 2, MaxConnectionsPerIP: 1,
		MaxFramesPerConnection: 1, MaxBytesPerConnection: 8,
		MaxBytesPerIP: 8, MaxBytesPod: 16,
	})
	if err := controller.Open("c1", 1, "attacker-bucket-a"); err != nil {
		t.Fatal(err)
	}
	if err := controller.Open("c2", 1, "attacker-bucket-b"); err != nil {
		t.Fatalf("malformed IPs should consume only the pod budget: %v", err)
	}
	if snapshot := controller.Snapshot("attacker-bucket-a"); snapshot.IPConnections != 0 || snapshot.Connections != 2 {
		t.Fatalf("malformed IP acquired a per-IP bucket: %s", snapshot)
	}
}

func TestControllerMetricsUseOnlyBoundedLabels(t *testing.T) {
	registry := prometheus.NewRegistry()
	now := time.Unix(100, 0)
	controller := New(Config{
		MaxConnections: 1, MaxConnectionsPerIP: 1,
		MaxFramesPerConnection: 1, MaxBytesPerConnection: 8,
		MaxBytesPerIP: 8, MaxBytesPod: 8,
		Metrics: NewMetrics(registry), Now: func() time.Time { return now },
	})
	if err := controller.Open("secret-connection", 1, "192.0.2.99"); err != nil {
		t.Fatal(err)
	}
	if err := controller.Open("other", 1, "192.0.2.100"); err == nil {
		t.Fatal("expected connection rejection")
	}
	now = now.Add(time.Second)
	controller.Authenticate("secret-connection", 1)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if label.GetValue() == "secret-connection" || label.GetValue() == "192.0.2.99" {
					t.Fatalf("high-cardinality identity leaked into metric: %s", label.GetValue())
				}
			}
		}
	}
}

func TestReasonOfIgnoresUnrelatedErrors(t *testing.T) {
	if ReasonOf(errors.New("other")) != "" {
		t.Fatal("unrelated error acquired a rejection reason")
	}
}
