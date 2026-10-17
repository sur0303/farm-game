package demokit

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPlanAndVerifyCreatesPlanOnlyEvidence(t *testing.T) {
	output := filepath.Join(t.TempDir(), "F2")
	result, err := Plan(PlanRequest{
		ScenarioID: "F2",
		OutputDir:  output,
		SourceRef:  "0c9cae3",
		CreatedAt:  time.Date(2026, 8, 12, 8, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if result.Scenario.ID != "F2" {
		t.Fatalf("Plan() scenario = %q, want F2", result.Scenario.ID)
	}
	if len(result.Hashes) != 5 {
		t.Fatalf("Plan() hashes = %d, want 5", len(result.Hashes))
	}
	if err := Verify(output); err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	manifest, err := os.ReadFile(filepath.Join(output, "run-manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if string(manifest) == "" {
		t.Fatal("manifest must not be empty")
	}
}

func TestPlanRefusesExistingAndUnsafeOutput(t *testing.T) {
	output := filepath.Join(t.TempDir(), "existing")
	if err := os.MkdirAll(output, 0o755); err != nil {
		t.Fatal(err)
	}
	_, err := Plan(PlanRequest{ScenarioID: "F1", OutputDir: output, SourceRef: "0c9cae3"})
	if err == nil {
		t.Fatal("Plan() error = nil, want overwrite refusal")
	}
	if _, err := Plan(PlanRequest{ScenarioID: "F1", OutputDir: "", SourceRef: "0c9cae3"}); err == nil {
		t.Fatal("Plan() error = nil, want output-dir refusal")
	}
}

func TestVerifyRejectsTamperingAndUnexpectedFiles(t *testing.T) {
	output := filepath.Join(t.TempDir(), "F6")
	if _, err := Plan(PlanRequest{ScenarioID: "F6", OutputDir: output, SourceRef: "0c9cae3"}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(output, "report.md"), []byte("tampered\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := Verify(output); err == nil {
		t.Fatal("Verify() error = nil, want hash mismatch")
	}

	output = filepath.Join(t.TempDir(), "extra")
	if _, err := Plan(PlanRequest{ScenarioID: "F6", OutputDir: output, SourceRef: "0c9cae3"}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(output, "extra.txt"), []byte("extra\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := Verify(output); err == nil {
		t.Fatal("Verify() error = nil, want unexpected file failure")
	}
}

func TestPlanRejectsUnsafeSourceAndExecutionIsDisabled(t *testing.T) {
	_, err := Plan(PlanRequest{
		ScenarioID: "F1",
		OutputDir:  filepath.Join(t.TempDir(), "F1"),
		SourceRef:  "https://not-a-commit",
	})
	if err == nil {
		t.Fatal("Plan() error = nil, want source-ref refusal")
	}
	if !errors.Is(Execute(), ErrExecutionDisabled) {
		t.Fatalf("Execute() = %v, want ErrExecutionDisabled", Execute())
	}
}

func TestCatalogIsDefensive(t *testing.T) {
	scenarios := ListScenarios()
	if len(scenarios) == 0 {
		t.Fatal("ListScenarios() returned no scenarios")
	}
	scenarios[0].Title = "mutated"
	if got, err := ScenarioByID("F1"); err != nil || got.Title == "mutated" {
		t.Fatalf("catalog was mutated: got %#v, err=%v", got, err)
	}
}
