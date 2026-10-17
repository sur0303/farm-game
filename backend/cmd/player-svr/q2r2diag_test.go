package main

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
)

// TestQ2R2DiagnosticsDisabledByDefault: with no env at all the endpoint must
// not start.
func TestQ2R2DiagnosticsDisabledByDefault(t *testing.T) {
	t.Setenv("Q2R2_LIVE", "")
	t.Setenv("Q2R2_DIAGNOSTICS", "")
	t.Setenv("Q2R2_DIAGNOSTICS_PORT", "0")
	if q2r2DiagnosticsEnabled() {
		t.Fatal("diagnostics must be disabled by default")
	}
}

// TestQ2R2DiagnosticsRequiresBothGates: a single variable must never open it.
func TestQ2R2DiagnosticsRequiresBothGates(t *testing.T) {
	for _, c := range []struct{ live, diag string }{
		{"1", ""}, {"", "1"}, {"0", "1"}, {"1", "0"}, {"yes", "1"}, {"1", "on"},
	} {
		t.Setenv("Q2R2_LIVE", c.live)
		t.Setenv("Q2R2_DIAGNOSTICS", c.diag)
		if q2r2DiagnosticsEnabled() {
			t.Fatalf("must require BOTH Q2R2_LIVE=1 and Q2R2_DIAGNOSTICS=1, got live=%q diag=%q", c.live, c.diag)
		}
	}
	t.Setenv("Q2R2_LIVE", "1")
	t.Setenv("Q2R2_DIAGNOSTICS", "1")
	if !q2r2DiagnosticsEnabled() {
		t.Fatal("both gates set must enable diagnostics")
	}
}

// TestQ2R2DiagnosticsRejectsNonLoopback: a non-loopback bind must fail closed.
func TestQ2R2DiagnosticsRejectsNonLoopback(t *testing.T) {
	t.Setenv("Q2R2_LIVE", "1")
	t.Setenv("Q2R2_DIAGNOSTICS", "1")
	for _, bind := range []string{":0", "[::]:0", "0.0.0.0:0", "198.18.0.3:0", "198.18.0.4:1234", "myhost:0"} {
		_, shutdown, err := startQ2R2Diagnostics(nil, actorrouting.DomainPlayer, bind)
		if err == nil {
			if shutdown != nil {
				shutdown()
			}
			t.Fatalf("non-loopback bind %q must be rejected", bind)
		}
	}
	_, shutdown, err := startQ2R2Diagnostics(nil, actorrouting.DomainPlayer, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("loopback bind must be accepted: %v", err)
	}
	if shutdown != nil {
		shutdown()
	}
}

// TestQ2R2DiagnosticsShutsDownWithRun: the shutdown func must stop the
// listener so repeated smokes never leak ports/goroutines.
func TestQ2R2DiagnosticsShutsDownWithRun(t *testing.T) {
	t.Setenv("Q2R2_LIVE", "1")
	t.Setenv("Q2R2_DIAGNOSTICS", "1")
	addr, shutdown, err := startQ2R2Diagnostics(nil, actorrouting.DomainPlayer, "127.0.0.1:0")
	if err != nil || shutdown == nil {
		t.Fatalf("start: %v", err)
	}
	resp, err := http.Get("http://" + addr + "/q2r2z/diag")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		t.Fatalf("status=%d", resp.StatusCode)
	}
	var d struct {
		ServingShardCount int  `json:"serving_shard_count"`
		CanServeShard     bool `json:"can_serve_shard"`
		LeaseFenceReason  int  `json:"lease_fence_reason"`
		RuntimeFatal      bool `json:"runtime_fatal"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
		resp.Body.Close()
		t.Fatalf("decode: %v", err)
	}
	resp.Body.Close()
	if d.ServingShardCount != 0 || d.CanServeShard || d.LeaseFenceReason != 0 || d.RuntimeFatal {
		t.Fatalf("nil runtime diagnostics must be fail-closed zero values: %+v", d)
	}
	shutdown()
	// Wait for the listener to close, then use a fresh keep-alive-free client
	// so the check is not served over an already established connection.
	time.Sleep(100 * time.Millisecond)
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	resp2, err := client.Get("http://" + addr + "/q2r2z/diag")
	if err == nil {
		resp2.Body.Close()
		t.Fatal("endpoint must stop responding after shutdown")
	}
}
