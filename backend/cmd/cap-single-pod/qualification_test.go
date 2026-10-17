package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type qualificationFixture struct {
	Dir              string
	IndexSHA         string
	ProfilePath      string
	CurrentManifest  string
	BindingPath      string
	DriverSHA        string
	ProfileSHA       string
	QualifiedTargets []target
	RequestedTargets []target
	Result           capacityResultDocument
	QualManifest     map[string]any
	CurrentManifestM map[string]any
}

func TestQualificationEvidenceHashRequired(t *testing.T) {
	fixture := newQualificationFixture(t)
	fixture.CurrentManifestM["load_generator"].(map[string]any)["qualification_evidence_hash"] = strings.Repeat("a", 64)
	writeJSON(t, fixture.CurrentManifest, fixture.CurrentManifestM)
	plan := fixture.plan("SMOKE")
	if _, err := verifyLoadGeneratorQualification(plan); err == nil || !strings.Contains(err.Error(), "qualification evidence hash mismatch") {
		t.Fatalf("err=%v", err)
	}
}

func TestQualificationMustPrecedeSmoke(t *testing.T) {
	plan := runPlan{Phase: "SMOKE", LoadGeneratorQualificationPath: ""}
	if err := validateQualificationPath(plan); err == nil || !strings.Contains(err.Error(), "qualification evidence path required") {
		t.Fatalf("err=%v", err)
	}
	if _, err := verifyLoadGeneratorQualification(plan); err == nil || !strings.Contains(err.Error(), "qualification evidence required") {
		t.Fatalf("verify err=%v", err)
	}
	for _, phase := range []string{"PRECHECK", "OBSERVABILITY_QUALIFICATION", "LOAD_GENERATOR_QUALIFICATION"} {
		if err := validateQualificationPath(runPlan{Phase: phase}); err != nil {
			t.Fatalf("phase=%s err=%v", phase, err)
		}
		if _, err := verifyLoadGeneratorQualification(runPlan{Phase: phase}); err != nil {
			t.Fatalf("phase=%s verify err=%v", phase, err)
		}
	}
}

func TestHeadroomFactorComputedNotSelfReported(t *testing.T) {
	fixture := newQualificationFixture(t)
	fixture.CurrentManifestM["load_generator"].(map[string]any)["headroom_factor_proven"] = 9.0
	writeJSON(t, fixture.CurrentManifest, fixture.CurrentManifestM)
	fixture.Result.Targets = []target{{Kind: "arrivals_per_second", Value: 120}}
	rewriteQualificationResult(t, &fixture)
	plan := fixture.plan("SMOKE")
	plan.Targets = []target{{Kind: "arrivals_per_second", Value: 100}}
	if _, err := verifyLoadGeneratorQualification(plan); err == nil || !strings.Contains(err.Error(), "qualification headroom insufficient") {
		t.Fatalf("insufficient err=%v", err)
	}

	fixture = newQualificationFixture(t)
	fixture.CurrentManifestM["load_generator"].(map[string]any)["headroom_factor_proven"] = 9.0
	writeJSON(t, fixture.CurrentManifest, fixture.CurrentManifestM)
	plan = fixture.plan("SMOKE")
	summary, err := verifyLoadGeneratorQualification(plan)
	if err != nil {
		t.Fatal(err)
	}
	if summary.HeadroomFactorActual != 1.5 || summary.QualificationEvidenceSHA256 != fixture.IndexSHA {
		t.Fatalf("summary=%+v", summary)
	}
}

func TestQualificationRejectsIdentityDriverProfileMismatch(t *testing.T) {
	t.Run("service", func(t *testing.T) {
		fixture := newQualificationFixture(t)
		fixture.Result.Service = "player"
		rewriteQualificationResult(t, &fixture)
		if _, err := verifyLoadGeneratorQualification(fixture.plan("SMOKE")); err == nil || !strings.Contains(err.Error(), "qualification identity mismatch") {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("binding", func(t *testing.T) {
		fixture := newQualificationFixture(t)
		fixture.Result.BindingID = "R2"
		rewriteQualificationResult(t, &fixture)
		if _, err := verifyLoadGeneratorQualification(fixture.plan("SMOKE")); err == nil || !strings.Contains(err.Error(), "qualification identity mismatch") {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("driver", func(t *testing.T) {
		fixture := newQualificationFixture(t)
		fixture.QualManifest["load_generator"].(map[string]any)["binary_sha256"] = strings.Repeat("b", 64)
		rewriteQualificationManifest(t, &fixture)
		if _, err := verifyLoadGeneratorQualification(fixture.plan("SMOKE")); err == nil || !strings.Contains(err.Error(), "qualification driver mismatch") {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("profile", func(t *testing.T) {
		fixture := newQualificationFixture(t)
		fixture.QualManifest["profile_hash"] = strings.Repeat("c", 64)
		rewriteQualificationManifest(t, &fixture)
		if _, err := verifyLoadGeneratorQualification(fixture.plan("SMOKE")); err == nil || !strings.Contains(err.Error(), "qualification profile mismatch") {
			t.Fatalf("err=%v", err)
		}
	})
}

func TestQualificationRequiresFullSlopeObservation(t *testing.T) {
	t.Run("plan duration", func(t *testing.T) {
		plan := runPlan{Phase: "LOAD_GENERATOR_QUALIFICATION", DurationSeconds: minimumLoadGeneratorQualificationSamples - 1, SampleIntervalMS: 1000}
		if err := validatePhaseDuration(plan); err == nil || !strings.Contains(err.Error(), "full slope window") {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("evidence duration", func(t *testing.T) {
		fixture := newQualificationFixture(t)
		fixture.Result.Duration = minimumLoadGeneratorQualificationSamples - 1
		rewriteQualificationResult(t, &fixture)
		if _, err := verifyLoadGeneratorQualification(fixture.plan("SMOKE")); err == nil || !strings.Contains(err.Error(), "qualification load metrics invalid") {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("evidence samples", func(t *testing.T) {
		fixture := newQualificationFixture(t)
		fixture.Result.QueueSlope.Samples = minimumLoadGeneratorQualificationSamples - 1
		rewriteQualificationResult(t, &fixture)
		if _, err := verifyLoadGeneratorQualification(fixture.plan("SMOKE")); err == nil || !strings.Contains(err.Error(), "qualification load metrics invalid") {
			t.Fatalf("err=%v", err)
		}
	})
}

func TestQualificationRejectsSlopeWindowsAndIncompleteIndex(t *testing.T) {
	t.Run("slope", func(t *testing.T) {
		fixture := newQualificationFixture(t)
		fixture.Result.Metrics["arrival_lag_positive_slope_windows_max"] = 1
		rewriteQualificationResult(t, &fixture)
		if _, err := verifyLoadGeneratorQualification(fixture.plan("WARMUP")); err == nil || !strings.Contains(err.Error(), "qualification slope windows nonzero") {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("incomplete", func(t *testing.T) {
		fixture := newQualificationFixture(t)
		raw, err := os.ReadFile(filepath.Join(fixture.Dir, "evidence-hash-index.json"))
		if err != nil {
			t.Fatal(err)
		}
		var index map[string]any
		if json.Unmarshal(raw, &index) != nil {
			t.Fatal("index")
		}
		index["complete"] = false
		writeJSON(t, filepath.Join(fixture.Dir, "evidence-hash-index.json"), index)
		fixture.IndexSHA = fileSHA(t, filepath.Join(fixture.Dir, "evidence-hash-index.json"))
		fixture.CurrentManifestM["load_generator"].(map[string]any)["qualification_evidence_hash"] = fixture.IndexSHA
		writeJSON(t, fixture.CurrentManifest, fixture.CurrentManifestM)
		if _, err := verifyLoadGeneratorQualification(fixture.plan("STAIRCASE")); err == nil || !strings.Contains(err.Error(), "qualification evidence incomplete") {
			t.Fatalf("err=%v", err)
		}
	})
}

func (fixture qualificationFixture) plan(phase string) runPlan {
	return runPlan{
		Phase:                          phase,
		Service:                        "gate",
		Profile:                        "login_ticket_wss_hello",
		Tier:                           2,
		Targets:                        fixture.RequestedTargets,
		DriverBinarySHA256:             fixture.DriverSHA,
		ManifestPath:                   fixture.CurrentManifest,
		ProfilePath:                    fixture.ProfilePath,
		BindingPath:                    fixture.BindingPath,
		LoadGeneratorQualificationPath: fixture.Dir,
	}
}

func newQualificationFixture(t *testing.T) qualificationFixture {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, "qualification")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	bindingPath, err := filepath.Abs(filepath.Join("..", "..", "deploy", "load", "cap-single-pod-r1-binding.json"))
	if err != nil {
		t.Fatal(err)
	}
	driverSHA := strings.Repeat("d", 64)
	bindingSHA := fileSHA(t, bindingPath)
	profilePath := filepath.Join(root, "profile.json")
	writeJSON(t, profilePath, map[string]any{"profile": "login_ticket_wss_hello"})
	profileSHA := fileSHA(t, profilePath)
	qualifiedTargets := []target{{Kind: "arrivals_per_second", Value: 150}, {Kind: "connections", Value: 300}}
	requestedTargets := []target{{Kind: "arrivals_per_second", Value: 100}, {Kind: "connections", Value: 200}}
	finishedAt := time.Now().UTC().Add(-2 * time.Hour).Format(time.RFC3339Nano)
	startedAt := time.Now().UTC().Add(-1 * time.Hour).Format(time.RFC3339Nano)
	result := capacityResultDocument{
		BindingID: "R1", RunID: "capsingle-l2-r1-20260810T010000Z-1", Service: "gate", Profile: "login_ticket_wss_hello",
		Phase: "LOAD_GENERATOR_QUALIFICATION", Tier: 4, Duration: 120, Targets: qualifiedTargets,
		PhaseStatus: PhaseStatusPass, PhaseDecision: "PASS_LOAD_GENERATOR_QUALIFICATION",
		Actual:          plannedActual{Planned: 1000, Launched: 1000, Completed: 1000, ActualToPlannedRatio: .99, ArrivalLagMS: map[string]float64{"p99": 12}},
		AdapterIdentity: map[string]any{"binding_sha256": bindingSHA},
		QueueSlope:      slopeResult{Samples: minimumLoadGeneratorQualificationSamples},
		Metrics: map[string]float64{
			"driver_cpu_percent_max":                          40,
			"arrival_lag_positive_slope_windows_max":          0,
			"driver_event_backlog_positive_slope_windows_max": 0,
			"driver_socket_error_positive_slope_windows_max":  0,
		},
		SafeClaimed: false,
	}
	frozenIdentity := map[string]any{"backend_evidence_head": "e", "backend_business_commit": "b", "backend_image_digest": "i", "h5_commit": "h", "h5_image_digest": "hi", "model_layout": "r1"}
	qualManifest := map[string]any{
		"profile_hash":    profileSHA,
		"frozen_identity": frozenIdentity,
		"finished_at":     finishedAt,
		"load_generator": map[string]any{
			"binary_sha256": driverSHA,
		},
	}
	files := map[string]any{
		"run-manifest.json":            qualManifest,
		"planned-vs-actual.json":       map[string]any{"ok": true},
		"resource-samples.ndjson":      "{\"observed_at\":\"x\"}\n",
		"latency-histograms.json":      map[string]any{"ok": true},
		"queue-slope.json":             map[string]any{"ok": true},
		"correctness-verdict.json":     map[string]any{"ok": true},
		"environment-verdict.json":     map[string]any{"ok": true},
		"service-capacity-result.json": result,
		"run-report.md":                "# ok\n",
	}
	for name, value := range files {
		switch typed := value.(type) {
		case string:
			if err := os.WriteFile(filepath.Join(dir, name), []byte(typed), 0o600); err != nil {
				t.Fatal(err)
			}
		default:
			writeJSON(t, filepath.Join(dir, name), typed)
		}
	}
	indexSHA := writeCompleteEvidenceIndex(t, dir)
	currentManifestPath := filepath.Join(root, "current-manifest.json")
	currentManifest := map[string]any{
		"started_at":      startedAt,
		"frozen_identity": frozenIdentity,
		"load_generator": map[string]any{
			"binary_sha256":               driverSHA,
			"qualification_evidence_hash": indexSHA,
			"headroom_factor_proven":      1.5,
			"arrival_model":               "open_loop_monotonic_scheduler",
		},
	}
	writeJSON(t, currentManifestPath, currentManifest)
	return qualificationFixture{
		Dir: dir, IndexSHA: indexSHA, ProfilePath: profilePath, CurrentManifest: currentManifestPath,
		BindingPath: bindingPath, DriverSHA: driverSHA, ProfileSHA: profileSHA,
		QualifiedTargets: qualifiedTargets, RequestedTargets: requestedTargets,
		Result: result, QualManifest: qualManifest, CurrentManifestM: currentManifest,
	}
}

func rewriteQualificationResult(t *testing.T, fixture *qualificationFixture) {
	t.Helper()
	writeJSON(t, filepath.Join(fixture.Dir, "service-capacity-result.json"), fixture.Result)
	fixture.IndexSHA = writeCompleteEvidenceIndex(t, fixture.Dir)
	fixture.CurrentManifestM["load_generator"].(map[string]any)["qualification_evidence_hash"] = fixture.IndexSHA
	writeJSON(t, fixture.CurrentManifest, fixture.CurrentManifestM)
}

func rewriteQualificationManifest(t *testing.T, fixture *qualificationFixture) {
	t.Helper()
	writeJSON(t, filepath.Join(fixture.Dir, "run-manifest.json"), fixture.QualManifest)
	fixture.IndexSHA = writeCompleteEvidenceIndex(t, fixture.Dir)
	fixture.CurrentManifestM["load_generator"].(map[string]any)["qualification_evidence_hash"] = fixture.IndexSHA
	writeJSON(t, fixture.CurrentManifest, fixture.CurrentManifestM)
}

func writeCompleteEvidenceIndex(t *testing.T, dir string) string {
	t.Helper()
	entries := make([]map[string]any, 0, len(requiredEvidenceFiles))
	for _, name := range requiredEvidenceFiles {
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		entries = append(entries, map[string]any{"path": name, "bytes": len(raw), "sha256": hex.EncodeToString(sum[:])})
	}
	indexPath := filepath.Join(dir, "evidence-hash-index.json")
	_ = os.Remove(indexPath)
	writeJSON(t, indexPath, map[string]any{
		"schema_version": "cap-single-pod-l2.evidence-index.v0.1",
		"complete":       true,
		"files":          entries,
	})
	return fileSHA(t, indexPath)
}

func writeJSON(t *testing.T, path string, value any) {
	t.Helper()
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(raw, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}

func fileSHA(t *testing.T, path string) string {
	t.Helper()
	digest, err := exactFileSHA256(path)
	if err != nil {
		t.Fatal(err)
	}
	return digest
}
