package loadtest

import (
	"reflect"
	"testing"
	"time"
)

func TestLeaseStressArgsMapping(t *testing.T) {
	p := LeaseStressParams{
		GateHTTP:        "http://127.0.0.1:8080",
		GateWS:          "ws://127.0.0.1:8080/ws",
		MetricsURL:      "http://127.0.0.1:9090/metrics",
		LoadTestSecret:  "s3",
		Viewers:         199,
		JoinConcurrency: 32,
		Duration:        60 * time.Second,
		StepTimeout:     5 * time.Second,
		ActionInterval:  3 * time.Second,
	}
	want := []string{
		"-gate-http", "http://127.0.0.1:8080",
		"-gate-ws", "ws://127.0.0.1:8080/ws",
		"-metrics-url", "http://127.0.0.1:9090/metrics",
		"-load-test-secret", "s3",
		"-viewers", "199",
		"-join-concurrency", "32",
		"-duration", "1m0s",
		"-step-timeout", "5s",
		"-action-interval", "3s",
	}
	if got := p.Args(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Args=%v want %v", got, want)
	}
}

func TestLeaseStressZeroValuesOmitFlags(t *testing.T) {
	if got := (LeaseStressParams{}).Args(); len(got) != 0 {
		t.Fatalf("zero params must produce no flags, got %v", got)
	}
}

func TestLeaseStressViewerCap(t *testing.T) {
	if err := (LeaseStressParams{Viewers: 199}).Validate(); err != nil {
		t.Fatalf("199 viewers is the contract cap and must validate: %v", err)
	}
	if err := (LeaseStressParams{Viewers: 200}).Validate(); err == nil {
		t.Fatal("200 viewers exceeds the 26 §HC-ACCESS-36 cap and must be rejected")
	}
	if err := (LeaseStressParams{Viewers: -1}).Validate(); err == nil {
		t.Fatal("negative viewers must be rejected")
	}
}

func TestWSStressArgsMapping(t *testing.T) {
	p := WSStressParams{
		HTTPURL:        "http://127.0.0.1:8080",
		WSURL:          "ws://127.0.0.1:8080/ws",
		Conns:          200,
		Rate:           50,
		Duration:       30 * time.Second,
		PlayerStart:    900000000,
		Scenario:       "hot",
		SharedFarm:     42,
		HotLoops:       10,
		HotMixed:       true,
		ReconnectLoops: 3,
		JSONOut:        "/tmp/out.json",
	}
	want := []string{
		"-http-url", "http://127.0.0.1:8080",
		"-ws-url", "ws://127.0.0.1:8080/ws",
		"-conns", "200",
		"-rate", "50",
		"-duration", "30s",
		"-player-start", "900000000",
		"-scenario", "hot",
		"-shared-farm", "42",
		"-hot-loops", "10",
		"-hot-mixed",
		"-reconnect-loops", "3",
		"-json-out", "/tmp/out.json",
	}
	if got := p.Args(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Args=%v want %v", got, want)
	}
}

func TestWSStressValidateScenario(t *testing.T) {
	if err := (WSStressParams{Scenario: "bogus"}).Validate(); err == nil {
		t.Fatal("unknown scenario must be rejected")
	}
	for _, s := range []string{"", "login", "join", "plant", "hot", "reconnect"} {
		if err := (WSStressParams{Scenario: s}).Validate(); err != nil {
			t.Fatalf("scenario %q must validate: %v", s, err)
		}
	}
}

func TestDrainScriptEnv(t *testing.T) {
	if got := (DrainScriptEnv{}).Env(); len(got) != 0 {
		t.Fatalf("zero env must be empty, got %v", got)
	}
	env := DrainScriptEnv{RedisConfig: "/tmp/redis.conf", KeepTmp: true}.Env()
	want := []string{"FARM_REDIS_CONFIG=/tmp/redis.conf", "MULTIGATE_KEEP_TMP=1"}
	if !reflect.DeepEqual(env, want) {
		t.Fatalf("Env=%v want %v", env, want)
	}
}

func TestPresetsAreValidAndCarryEvidenceRules(t *testing.T) {
	broadcast, rule := BroadcastSlowConn()
	if err := broadcast.Validate(); err != nil {
		t.Fatalf("BroadcastSlowConn preset invalid: %v", err)
	}
	if rule.Tool == "" || len(rule.RequiredEvidence) == 0 || rule.PassCriteria == "" {
		t.Fatal("BroadcastSlowConn must carry a complete EvidenceRule")
	}

	full, rule := FullGameplay()
	if err := full.Validate(); err != nil {
		t.Fatalf("FullGameplay preset invalid: %v", err)
	}
	if len(rule.RequiredEvidence) == 0 {
		t.Fatal("FullGameplay must declare required evidence")
	}

	soak, rule := Soak(2 * time.Hour)
	if err := soak.Validate(); err != nil {
		t.Fatalf("Soak preset invalid: %v", err)
	}
	if soak.Viewers != 199 {
		t.Fatal("Soak must pin the 199-viewer contract scenario")
	}

	_, drainRule := Drain()
	if drainRule.Tool != "scripts/test-multigate-drain" {
		t.Fatal("Drain must reuse the W5B script, not a new implementation")
	}
}
