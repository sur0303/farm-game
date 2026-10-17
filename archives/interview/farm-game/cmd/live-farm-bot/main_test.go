package main

import (
	"bytes"
	"encoding/json"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"farmgame/internal/livebot"
	"farmgame/internal/selfprobe"
)

func TestParseConfigDefaultsAndSecretName(t *testing.T) {
	cfg, err := parseConfig(nil, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.httpURL != defaultHTTPURL || cfg.wsURL != defaultWSURL {
		t.Fatalf("endpoints = %q %q", cfg.httpURL, cfg.wsURL)
	}
	if cfg.loadSecretEnv != defaultSecretEnv {
		t.Fatalf("secret env = %q", cfg.loadSecretEnv)
	}
	if err := validateConfig(cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := parseConfig([]string{"--load-secret-env", "not an env name"}, io.Discard); err == nil {
		t.Fatal("plaintext-like value with spaces accepted as environment name")
	}
}

func TestLoopbackPolicyCoversLiteralIPv4IPv6LocalhostAndDNSBypass(t *testing.T) {
	for _, raw := range []string{
		"http://127.0.0.1:8080",
		"http://localhost:8080",
		"http://[::1]:8080",
	} {
		if err := validateEndpoint(raw, "http-url", false, "http"); err != nil {
			t.Fatalf("loopback endpoint %q rejected: %v", raw, err)
		}
	}
	for _, raw := range []string{
		"http://0.0.0.0:8080",
		"http://127.0.0.1.nip.io:8080",
		"http://example.com:8080",
	} {
		if err := validateEndpoint(raw, "http-url", false, "http"); err == nil {
			t.Fatalf("non-loopback endpoint %q accepted", raw)
		}
	}
	if err := validateEndpoint("http://example.com:8080", "http-url", true, "http"); err != nil {
		t.Fatalf("explicit non-loopback allowance rejected: %v", err)
	}
	for _, raw := range []string{"127.0.0.1:18602", "localhost:18602", "[::1]:18602"} {
		if err := validateListen(raw, false); err != nil {
			t.Fatalf("loopback listener %q rejected: %v", raw, err)
		}
	}
	if err := validateListen("127.0.0.1.nip.io:18602", false); err == nil {
		t.Fatal("DNS listener bypass accepted")
	}
}

func TestValidateConfigRejectsUnsafeEnvironmentAndServeAfter(t *testing.T) {
	cfg, err := parseConfig(nil, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	cfg.environment = "production"
	if err := validateConfig(cfg); err == nil {
		t.Fatal("production environment accepted")
	}
	cfg, err = parseConfig(nil, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	cfg.serveAfter = MaxServeAfter + time.Nanosecond
	if err := validateConfig(cfg); err == nil {
		t.Fatal("unbounded serve-after accepted")
	}
}

func TestSummaryDoesNotExposeBotErrors(t *testing.T) {
	var output bytes.Buffer
	result := livebot.RunResult{
		RunID:      "safe-run",
		Status:     livebot.RunFailed,
		Execution:  livebot.ExecutionRealSandbox,
		StartedAt:  time.Unix(1, 0).UTC(),
		FinishedAt: time.Unix(2, 0).UTC(),
		Bots: []livebot.BotResult{{
			Error: "secret-ticket-cookie",
		}},
		Summary: livebot.Summary{Latency: livebot.NewLatencyHistogram()},
	}
	if err := writeSummary(&output, result, selfprobe.Report{}); err != nil {
		t.Fatal(err)
	}
	text := output.String()
	if strings.Contains(text, "secret-ticket-cookie") || strings.Contains(text, "\"bots\"") || strings.Contains(text, "\"error\"") {
		t.Fatalf("summary leaked bot details: %s", text)
	}
}

// The sustained knobs only mean something together, so the CLI accepts them as
// a set and refuses the combinations that would quietly do nothing.
func TestSustainedFlagsRequireASession(t *testing.T) {
	cfg, err := parseConfig([]string{
		"--duration", "5m", "--session-duration", "2m", "--think-time", "3s", "--session-actions", "20",
		"--action-warmup", "45s", "--business-warmup", "2m", "--unlock-plots", "--unlock-plot-limit", "4", "--unlock-concurrency", "16",
		"--scenario", "mixed", "--action-mix", "care=5,harvest=3,plant=2",
	}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := runSpec(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !spec.Sustained() || spec.ThinkTime != 3*time.Second || spec.SessionActions != 20 {
		t.Fatalf("sustained spec = %+v", spec)
	}
	if cfg.actionWarmup != 45*time.Second || cfg.businessWarmup != 2*time.Minute || !cfg.unlockPlots || cfg.unlockPlotLimit != 4 || cfg.unlockConcurrency != 16 {
		t.Fatalf("warmup config = %+v", cfg)
	}
	if got := spec.ActionMix; len(got) != 3 || got["care"] != 5 || got["harvest"] != 3 || got["plant"] != 2 {
		t.Fatalf("action mix = %v", got)
	}
	if err := spec.Validate(); err != nil {
		t.Fatalf("validate sustained spec: %v", err)
	}
	for _, arguments := range [][]string{
		{"--think-time", "3s"},
		{"--session-actions", "5"},
		{"--action-mix", "care=1"},
		{"--action-warmup", "1s"},
		{"--business-warmup", "1s"},
		{"--session-duration", "1m", "--action-warmup", "6m"},
		{"--session-duration", "1m", "--action-warmup", "2m", "--business-warmup", "1m"},
		{"--session-duration", "1m", "--business-warmup", "11m"},
		{"--session-duration", "1m", "--unlock-plot-limit", "20"},
		{"--session-duration", "1m", "--unlock-concurrency", "33"},
		{"--session-duration", "1m", "--scenario", "reconnect"},
		{"--session-duration", "1m", "--action-mix", "care=1,care=2"},
		{"--session-duration", "1m", "--action-mix", "buy=1"},
		{"--session-duration", "1m", "--action-mix", "care"},
	} {
		if _, err := parseConfig(arguments, io.Discard); err == nil {
			t.Fatalf("accepted %v", arguments)
		}
	}
}

func TestJourneyScenarioAcceptsASustainedSession(t *testing.T) {
	cfg, err := parseConfig([]string{
		"--scenario", "journey",
		"--duration", "10m",
		"--session-duration", "5m",
		"--think-time", "2s",
	}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.scenario != "journey" {
		t.Fatalf("scenario=%q, want journey", cfg.scenario)
	}
	if cfg.sessionDuration != 5*time.Minute || cfg.thinkTime != 2*time.Second {
		t.Fatalf("journey session = duration=%s think=%s", cfg.sessionDuration, cfg.thinkTime)
	}
	spec, err := runSpec(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Scenario != "journey" || !spec.Sustained() {
		t.Fatalf("journey spec = %+v", spec)
	}
}

// A load run is read from the percentile view, so it has to survive the trip
// through the summary document.
func TestSummaryReportsPercentilesAndThroughput(t *testing.T) {
	latency := livebot.NewLatencyHistogram()
	for sample := 0; sample < 100; sample++ {
		latency.Observe(time.Duration(sample+1) * time.Millisecond)
	}
	phase := livebot.NewLatencyHistogram()
	phase.Observe(7 * time.Millisecond)
	var output bytes.Buffer
	err := writeSummary(&output, livebot.RunResult{
		RunID:      "percentile-run",
		Status:     livebot.RunSucceeded,
		StartedAt:  time.Unix(10, 0).UTC(),
		FinishedAt: time.Unix(20, 0).UTC(),
		Summary: livebot.Summary{
			Actions:    500,
			Planned:    600,
			Turns:      504,
			Timeouts:   4,
			Skipped:    96,
			OnlinePeak: 25,
			Latency:    latency,
			Phases:     map[string]livebot.LatencyHistogram{livebot.PhaseAdmission: phase},
		},
	}, selfprobe.Report{})
	if err != nil {
		t.Fatal(err)
	}
	var document summaryDocument
	if err := json.Unmarshal(output.Bytes(), &document); err != nil {
		t.Fatal(err)
	}
	if document.Latency.Count != 100 || document.Latency.P95Ms < 90 || document.Latency.P95Ms > 100 {
		t.Fatalf("latency report = %+v", document.Latency)
	}
	if document.Latency.P99Ms < document.Latency.P95Ms || document.Latency.P95Ms < document.Latency.P50Ms {
		t.Fatalf("percentiles are not ordered: %+v", document.Latency)
	}
	if report, ok := document.Phases[livebot.PhaseAdmission]; !ok || report.Count != 1 {
		t.Fatalf("phase reports = %+v", document.Phases)
	}
	if document.Throughput.Seconds != 10 || document.Throughput.ActionsPerSecond != 50 || document.Throughput.OnlinePeak != 25 {
		t.Fatalf("throughput = %+v", document.Throughput)
	}
	if document.Throughput.PlannedPerSecond != 60 {
		t.Fatalf("planned rate = %v, want 60", document.Throughput.PlannedPerSecond)
	}
	// 504 of 600 delivered: the step is under the floor and must say so next
	// to the percentiles rather than leaving them to be read as healthy.
	if document.Load.Sent != 504 || document.Load.Unsent != 96 || document.Load.Qualified {
		t.Fatalf("load account = %+v", document.Load)
	}
}

// A ramp step is only evidence about a server while the driver that produced
// it had headroom, so the summary has to fail the step on the driver's own
// numbers and name what ran out.
func TestSummaryDisqualifiesASaturatedDriver(t *testing.T) {
	arrival := livebot.NewLatencyHistogram()
	arrival.Observe(400 * time.Millisecond)
	run := livebot.RunResult{
		RunID:      "saturated-driver",
		Status:     livebot.RunSucceeded,
		StartedAt:  time.Unix(10, 0).UTC(),
		FinishedAt: time.Unix(20, 0).UTC(),
		Summary: livebot.Summary{
			Actions: 100,
			Planned: 100,
			Turns:   100,
			Latency: livebot.NewLatencyHistogram(),
			Phases:  map[string]livebot.LatencyHistogram{livebot.PhaseArrival: arrival},
		},
	}
	saturated := selfprobe.Report{
		Samples:              60,
		CPUPercentMax:        91,
		SchedLatencyP99MsMax: 42,
	}

	var output bytes.Buffer
	if err := writeSummary(&output, run, saturated); err != nil {
		t.Fatal(err)
	}
	var document summaryDocument
	if err := json.Unmarshal(output.Bytes(), &document); err != nil {
		t.Fatal(err)
	}
	if document.Qualified || document.Driver.Qualified {
		t.Fatalf("saturated driver qualified: %+v", document.Driver)
	}
	if got, want := document.Driver.DisqualifiedBy, []string{"cpu_percent_max", "arrival_lag_p99_ms", "sched_latency_p99_ms_max"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("disqualified by %v, want %v", got, want)
	}
	// The delivery account is intact, so the step is failing on the driver
	// alone. That distinction is the whole point of reporting both.
	if !document.Load.Qualified {
		t.Fatalf("load account = %+v", document.Load)
	}

	output.Reset()
	healthy := selfprobe.Report{Samples: 60, CPUPercentMax: 31, SchedLatencyP99MsMax: 2}
	run.Summary.Phases = nil
	if err := writeSummary(&output, run, healthy); err != nil {
		t.Fatal(err)
	}
	document = summaryDocument{}
	if err := json.Unmarshal(output.Bytes(), &document); err != nil {
		t.Fatal(err)
	}
	if !document.Qualified || !document.Driver.Qualified || len(document.Driver.DisqualifiedBy) != 0 {
		t.Fatalf("healthy driver rejected: %+v", document.Driver)
	}
}
