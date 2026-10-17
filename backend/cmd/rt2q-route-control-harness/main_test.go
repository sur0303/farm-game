//go:build linux

package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"farmgame/internal/testkit/routecontrol"
)

func TestRunRequiresStableSubcommand(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := run(nil, &out, &errOut); code != exitUsage {
		t.Fatalf("exit=%d stdout=%s stderr=%s", code, out.String(), errOut.String())
	}
	if !strings.Contains(errOut.String(), "run --level E3|E4") || !strings.Contains(errOut.String(), "verify --expected-head") {
		t.Fatalf("usage=%q", errOut.String())
	}
}

func TestParseRunAcceptsExactE3ProcessShape(t *testing.T) {
	head := strings.Repeat("a", 40)
	cfg, err := parseRun([]string{
		"--level", "E3", "--expected-head", head, "--runtime", "process",
		"--redis-mode", "isolated", "--scenario", "all", "--artifact-root", "run-a",
		"--overall-timeout", "10m", "--worktree", ".",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.level != routecontrol.EvidenceE3 || cfg.expectedHead != head || cfg.runtime != "process" || cfg.overallTimeout != 10*time.Minute {
		t.Fatalf("cfg=%+v", cfg)
	}
	if deadlineCfg, deadlineErr := parseRun([]string{
		"--level", "E3", "--expected-head", head, "--runtime", "process",
		"--redis-mode", "isolated", "--scenario", "deadline-refresh", "--artifact-root", "run-deadline",
		"--overall-timeout", "5m", "--worktree", ".",
	}, &bytes.Buffer{}); deadlineErr != nil || deadlineCfg.scenario != "deadline-refresh" {
		t.Fatalf("deadline refresh cfg=%+v err=%v", deadlineCfg, deadlineErr)
	}
}

func TestParseRunEnforcesEvidenceRuntimeAndRedisScenario(t *testing.T) {
	head := strings.Repeat("b", 40)
	tests := [][]string{
		{"--level", "E3", "--expected-head", head, "--runtime", "container", "--redis-mode", "isolated", "--scenario", "all", "--artifact-root", "x", "--overall-timeout", "1m"},
		{"--level", "E4", "--expected-head", head, "--runtime", "process", "--redis-mode", "isolated", "--scenario", "all", "--artifact-root", "x", "--overall-timeout", "1m"},
		{"--level", "E3", "--expected-head", head, "--runtime", "process", "--redis-mode", "company", "--scenario", "S0", "--artifact-root", "x", "--overall-timeout", "1m"},
		{"--level", "E3", "--expected-head", head, "--runtime", "process", "--redis-mode", "isolated", "--scenario", "S7", "--artifact-root", "x", "--overall-timeout", "1m"},
	}
	for _, args := range tests {
		if _, err := parseRun(args, &bytes.Buffer{}); err == nil {
			t.Fatalf("accepted invalid args: %v", args)
		}
	}
}

func TestParseVerifyAndStatusExits(t *testing.T) {
	head := strings.Repeat("c", 40)
	gotHead, root, err := parseVerify([]string{"--expected-head", head, "--run", "run-a"}, &bytes.Buffer{})
	if err != nil || gotHead != head || root != "run-a" {
		t.Fatalf("head=%q root=%q err=%v", gotHead, root, err)
	}
	if statusExit(routecontrol.StatusPass) != exitPass || statusExit(routecontrol.StatusFail) != exitFail || statusExit(routecontrol.StatusInconclusive) != exitInconclusive {
		t.Fatal("stable verdict exits changed")
	}
}

func TestEnvironmentNameIsStrict(t *testing.T) {
	for _, value := range []string{"RT2Q_COMPANY_REDIS_URL", "A1", "Z_"} {
		if !validEnvironmentName(value) {
			t.Fatalf("rejected %q", value)
		}
	}
	for _, value := range []string{"", "lower", "A-B", "1A"} {
		if validEnvironmentName(value) {
			t.Fatalf("accepted %q", value)
		}
	}
}

func TestHarnessAuthorityAndPlanTTLRemainDistinctAndTruthful(t *testing.T) {
	if harnessAuthorityTTL != "60s" {
		t.Fatalf("AuthorityTTL=%q want 60s (production ShardWriteAuthority MaxTTL)", harnessAuthorityTTL)
	}
	if harnessPlanTTL != "120s" {
		t.Fatalf("PlanTTL=%q want 120s", harnessPlanTTL)
	}
	if harnessAuthorityTTL == harnessPlanTTL {
		t.Fatalf("AuthorityTTL and PlanTTL must remain distinct; both=%q", harnessAuthorityTTL)
	}
	if harnessOperationTimeout != "30s" || harnessLeaseSafetyMargin != "10s" || harnessLeaseRenewInterval != "40s" {
		t.Fatalf("retained knobs drifted: operation=%q safety=%q renew=%q",
			harnessOperationTimeout, harnessLeaseSafetyMargin, harnessLeaseRenewInterval)
	}
	renew, err := time.ParseDuration(harnessLeaseRenewInterval)
	if err != nil || renew/2 <= 10*time.Second {
		t.Fatalf("first healthy renew must follow bounded final Verify window: renew=%v err=%v", renew, err)
	}

	values := harnessConfigValues("realm", "env", "zone", "process", "isolated")
	if !containsExact(values, harnessAuthorityTTL) || !containsExact(values, harnessPlanTTL) || !containsExact(values, harnessOperationTimeout) {
		t.Fatalf("config hash inputs missing distinct TTL evidence: %v", values)
	}
	authorityOnly := []string{"realm", "env", "zone", "redis-physical-v1", "v1", "v1", "1", "256", "60s", "30s", "process", "isolated"}
	if routecontrol.SHA256CanonicalSet("rt2q-config/v1", values) == routecontrol.SHA256CanonicalSet("rt2q-config/v1", authorityOnly) {
		t.Fatal("config hash must bind PlanTTL=120s distinctly from AuthorityTTL=60s")
	}

	fact := runtimeConfigFact{AuthorityTTL: harnessAuthorityTTL, PlanTTL: harnessPlanTTL, OperationTimeout: harnessOperationTimeout}
	if fact.AuthorityTTL != "60s" || fact.PlanTTL != "120s" || fact.OperationTimeout != "30s" {
		t.Fatalf("configs.json evidence drifted: %+v", fact)
	}

	leaseEnv := harnessAuthorityLeaseEnv()
	wantEnv := []string{
		"ACTOR_AUTHORITY_LEASE_TTL=60s",
		"ACTOR_AUTHORITY_LEASE_SAFETY_MARGIN=10s",
		"ACTOR_AUTHORITY_LEASE_RENEW_INTERVAL=40s",
	}
	if len(leaseEnv) != len(wantEnv) {
		t.Fatalf("lease env=%v want=%v", leaseEnv, wantEnv)
	}
	for i := range wantEnv {
		if leaseEnv[i] != wantEnv[i] {
			t.Fatalf("lease env[%d]=%q want %q", i, leaseEnv[i], wantEnv[i])
		}
	}
}

func containsExact(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func TestBusinessDriverStderrClassStaysRedacted(t *testing.T) {
	if got := businessDriverStderrClass("rt2q-business-driver failed: login_failed"); got != "login_failed" {
		t.Fatalf("class=%q", got)
	}
	if got := businessDriverStderrClass("rt2q-business-driver failed: other http://127.0.0.1:9/secret"); got != "stderr_present" {
		t.Fatalf("leaky class accepted: %q", got)
	}
	if got := businessDriverStderrClass(""); got != "" {
		t.Fatalf("empty=%q", got)
	}
}

func TestClassifyGateLoginFailureLineRecognizesWriterEpochMismatch(t *testing.T) {
	line := `2026/08/11 11:42:56 login failed path=/api/v1/auth/login err=rpc error: code = Unavailable desc = blobstore: writer epoch mismatch`
	if got := classifyGateLoginFailureLine(line); got != "writer_epoch_mismatch" {
		t.Fatalf("class=%q want writer_epoch_mismatch", got)
	}
	if got := classifyGateLoginFailureLine("login failed path=/api/v1/auth/login err=auth: unauthorized platform token"); got != "token" {
		t.Fatalf("token class=%q", got)
	}
}

func TestS0PlayerStartIsChallengeScoped(t *testing.T) {
	a := s0PlayerStart("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	b := s0PlayerStart("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	if a == 910000000 && b == 910000000 {
		t.Fatal("challenge-scoped player starts collapsed to the fixed legacy band")
	}
	if a == b {
		t.Fatalf("distinct challenges produced identical player starts: %d", a)
	}
	if a < 910000000 || b < 910000000 {
		t.Fatalf("player starts below base band: a=%d b=%d", a, b)
	}
}

func TestProbeGateLoginWaitsForOKWithoutSecretLeak(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if bytes.Contains(bytes.ToLower(body), []byte("rt2q-")) && bytes.Contains(body, []byte("private-material")) {
			t.Fatalf("probe body leaked harness secret material")
		}
		if attempts < 3 {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"player_id":"1","connection_ticket":"ticket"}`))
	}))
	defer server.Close()

	secret := []byte("0123456789abcdef0123456789abcdef")
	deadline := time.Now().Add(2 * time.Second)
	var lastStatus int
	var lastClass string
	for time.Now().Before(deadline) {
		status, class, err := probeGateLogin(context.Background(), server.URL, 42, "boot-1", secret)
		lastStatus, lastClass = status, class
		if err != nil {
			t.Fatal(err)
		}
		if status == http.StatusOK {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if lastStatus != http.StatusOK || lastClass != "" || attempts < 3 {
		t.Fatalf("status=%d class=%q attempts=%d", lastStatus, lastClass, attempts)
	}
}

func TestRoutectlStatusPollKeepsShortRPCBudget(t *testing.T) {
	// One hung status RPC must not consume the entire controller-ready wait.
	// Status polls use a fixed short budget; plan ops may still use up to 30s.
	if routectlStatusPollRPCTimeout != 2*time.Second {
		t.Fatalf("status poll budget=%s want 2s", routectlStatusPollRPCTimeout)
	}
	if got := boundedRoutectlRPCTimeout(0); got != routectlStatusPollRPCTimeout {
		t.Fatalf("non-positive budget=%s want %s", got, routectlStatusPollRPCTimeout)
	}
	if got := boundedRoutectlRPCTimeout(500 * time.Millisecond); got != 500*time.Millisecond {
		t.Fatalf("explicit short budget=%s", got)
	}
	if got := boundedRoutectlRPCTimeout(30 * time.Second); got != 30*time.Second {
		t.Fatalf("plan-capable budget=%s want 30s", got)
	}
	if got := boundedRoutectlRPCTimeout(31 * time.Second); got != 30*time.Second {
		t.Fatalf("overlong budget=%s must clamp to 30s", got)
	}
}

func TestConsumerReadinessSnapshotIncludesWorker(t *testing.T) {
	runtime := &runRuntime{
		holders:       []*holderSpec{{name: "player-a", ports: processPorts{health: "127.0.0.1:1"}}, {name: "farm-a", ports: processPorts{health: "127.0.0.1:2"}}},
		gates:         []*gateSpec{{name: "gate-1", ports: processPorts{http: "127.0.0.1:3"}}},
		workerProcess: &managedProcess{name: "worker"},
		workerHealth:  "127.0.0.1:4",
	}
	snapshot := runtime.consumerReadinessSnapshot()
	for _, name := range []string{"player-a", "farm-a", "gate-1", "worker"} {
		if _, ok := snapshot[name]; !ok {
			t.Fatalf("missing readiness key %q in %v", name, snapshot)
		}
	}
	if snapshot["worker"] {
		t.Fatal("closed loopback ports must report not ready")
	}
}
