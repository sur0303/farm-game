package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"
)

func TestEvidenceWriterProducesExactRequiredSetAndHashes(t *testing.T) {
	out := filepath.Join(t.TempDir(), "evidence")
	if err := prepareOutput(out); err != nil {
		t.Fatal(err)
	}
	manifest := filepath.Join(t.TempDir(), "manifest.json")
	if err := os.WriteFile(manifest, []byte("{\"safe\":true}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := copyManifest(manifest, out); err != nil {
		t.Fatal(err)
	}
	resource := filepath.Join(out, "resource-samples.ndjson")
	if err := os.WriteFile(resource, []byte("{\"observed_at\":\"x\"}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	acc := newRunAccumulator()
	acc.planned = 10
	acc.launched = 10
	acc.completed = 10
	for i := 0; i < 10; i++ {
		acc.arrivalLag.observe(1)
		acc.latency.observe(2)
	}
	state := newCollectionState()
	for _, sample := range healthySamples() {
		state.addSample(sample, 60, 5)
	}
	eval := evaluateRun(healthyPlan(), acc.snapshot(), nil, healthySamples(), nil, true, true, "")
	binding := filepath.Clean(filepath.Join("..", "..", "deploy", "load", "cap-single-pod-r1-binding.json"))
	plan := runPlan{RunID: "capsingle-l2-r1-20260810T120000Z-1", Service: "gate", Profile: "smoke", Phase: "SAFE_TIER_CANDIDATE", BindingPath: binding, Targets: []target{{Kind: "arrivals_per_second", Value: 10}},
		Metadata: map[string]string{"tier_baseline": tierBaselineFirstTier}}
	baseline, err := resolveTierBaseline(plan)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeRunEvidence(out, plan, acc, state, eval, qualificationSummary{}, baseline); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 10 {
		t.Fatalf("files=%d", len(entries))
	}
	var index struct {
		Complete bool `json:"complete"`
		Files    []struct {
			Path  string `json:"path"`
			Bytes int    `json:"bytes"`
			SHA   string `json:"sha256"`
		} `json:"files"`
	}
	raw, err := os.ReadFile(filepath.Join(out, "evidence-hash-index.json"))
	if err != nil {
		t.Fatal(err)
	}
	if json.Unmarshal(raw, &index) != nil {
		t.Fatal("index")
	}
	if !index.Complete {
		t.Fatal("complete evidence marked incomplete")
	}
	if len(index.Files) != len(requiredEvidenceFiles) {
		t.Fatalf("indexed=%d", len(index.Files))
	}
	for _, item := range index.Files {
		data, err := os.ReadFile(filepath.Join(out, item.Path))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(data)
		if len(data) != item.Bytes || hex.EncodeToString(sum[:]) != item.SHA {
			t.Fatalf("hash %s", item.Path)
		}
	}
}

type failingEvidenceSink struct{ err error }

func (sink failingEvidenceSink) WriteRecord([]byte) error { return sink.err }

func TestEvidenceMarshalFailurePreventsPass(t *testing.T) {
	state := newCollectionState()
	collector := &collector{state: state, sink: failingEvidenceSink{}}
	if err := collector.writeSample(resourceSnapshot{ServiceCPUPercent: math.NaN(), MetricValues: map[string]float64{}}); err == nil {
		t.Fatal("marshal failure accepted")
	}
	state.addEvidenceWriteError()
	result := evaluateRun(healthyPlan(), healthyActual(), nil, healthySamples(), nil, true, false, VerdictInconclusiveObservability)
	if result.Verdict != VerdictInconclusiveObservability {
		t.Fatalf("result=%+v", result)
	}
}
func TestEvidenceShortWritePreventsPass(t *testing.T) {
	state := newCollectionState()
	collector := &collector{state: state, sink: failingEvidenceSink{err: errors.New("short write")}}
	if err := collector.writeSample(resourceSnapshot{MetricValues: map[string]float64{}}); err == nil {
		t.Fatal("short write accepted")
	}
	state.addEvidenceWriteError()
	if state.evidenceWriteErrorCount() != 1 {
		t.Fatal("error not latched")
	}
}
func TestEvidenceDiskWriteFailureStopsRun(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "sink")
	if err != nil {
		t.Fatal(err)
	}
	sink := &fileEvidenceSink{file: file}
	if file.Close() != nil {
		t.Fatal("close")
	}
	state := newCollectionState()
	collector := &collector{state: state, sink: sink}
	if err := collector.writeSample(resourceSnapshot{MetricValues: map[string]float64{}}); err == nil {
		t.Fatal("closed file write accepted")
	}
	state.addEvidenceWriteError()
	_, _, _, complete, stop := state.snapshot()
	if complete || stop != VerdictInconclusiveObservability {
		t.Fatalf("complete=%v stop=%s", complete, stop)
	}
}
func TestIncompleteEvidenceIndexCannotPass(t *testing.T) {
	out := t.TempDir()
	for _, name := range requiredEvidenceFiles {
		if os.WriteFile(filepath.Join(out, name), []byte("{}\n"), 0o600) != nil {
			t.Fatal("write")
		}
	}
	if err := writeEvidenceIndex(out, false); err != nil {
		t.Fatal(err)
	}
	if err := verifyRunEvidenceIndex(out); err == nil {
		t.Fatal("incomplete index accepted")
	}
}

func TestEvidenceRejectsSecretsAndRawEndpoints(t *testing.T) {
	for _, value := range [][]byte{[]byte(`{"password":"x"}`), []byte(`redis://127.0.0.1:6379`), []byte(`rediss://127.0.0.1:6379`), []byte(`{"operation_id":"raw"}`), []byte(`http://127.0.0.1:1`)} {
		if !containsForbiddenEvidence(value) {
			t.Fatalf("accepted %q", value)
		}
	}
	if containsForbiddenEvidence([]byte(`{"endpoint_hash":"abc","error_code":"EXPECTED"}`)) {
		t.Fatal("safe evidence rejected")
	}
}

func TestLoopbackEndpointValidation(t *testing.T) {
	for _, value := range []string{"http://127.0.0.1:1/op", "http://localhost:1/op", "http://[::1]:1/op"} {
		if err := validateLoopbackHTTP(value); err != nil {
			t.Fatalf("%s: %v", value, err)
		}
	}
	for _, value := range []string{"https://127.0.0.1/op", "http://example.com/op", "http://user:pass@127.0.0.1/op", "http://127.0.0.1:1/metrics?token=x", "http://127.0.0.1:1/metrics#fragment"} {
		if validateLoopbackHTTP(value) == nil {
			t.Fatalf("accepted %s", value)
		}
	}
}

func TestDesignIndexExact(t *testing.T) {
	path := filepath.Clean(filepath.Join("..", "..", "docs", "verification", "perf", "cap-single-pod", "CAP-SINGLE-POD-L2-R1"))
	if err := verifyDesignIndex(path); err != nil {
		t.Fatal(err)
	}
}
