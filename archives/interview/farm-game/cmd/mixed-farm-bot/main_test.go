package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"farmgame/internal/loadbot"
)

func TestRunDryRun(t *testing.T) {
	var output bytes.Buffer
	if err := run([]string{"dry-run", "--players", "4", "--actions", "3", "--seed", "7", "--run-id", "test"}, &output); err != nil {
		t.Fatalf("run() error = %v", err)
	}
	text := output.String()
	for _, expected := range []string{"\"execution\":\"DRY_RUN_ONLY\"", "\"capacity_claim\":false", "\"run_id\":\"test\""} {
		if !strings.Contains(text, expected) {
			t.Fatalf("output does not contain %q: %s", expected, text)
		}
	}
}

func TestRunRejectsAnythingButDryRun(t *testing.T) {
	if err := run([]string{"run"}, &bytes.Buffer{}); err == nil {
		t.Fatal("run accepted an execution subcommand")
	}
}
func TestRunDryRunEmitsVersionedRunAndFixtureContracts(t *testing.T) {
	var output bytes.Buffer
	if err := run([]string{
		"dry-run", "--players", "4", "--actions", "2", "--run-id", "cli-contract",
		"--fixture-id", "cli-fixture", "--environment", "sandbox",
	}, &output); err != nil {
		t.Fatalf("run() error = %v", err)
	}
	var document struct {
		RunSpec         loadbot.RunSpec         `json:"run_spec"`
		Run             loadbot.Run             `json:"run"`
		FixtureManifest loadbot.FixtureManifest `json:"fixture_manifest"`
	}
	if err := json.Unmarshal(output.Bytes(), &document); err != nil {
		t.Fatalf("decode dry-run output: %v", err)
	}
	if err := document.RunSpec.Validate(); err != nil {
		t.Fatalf("run spec invalid: %v", err)
	}
	if err := document.Run.Validate(); err != nil {
		t.Fatalf("run invalid: %v", err)
	}
	if document.Run.State != loadbot.RunStatePlanned {
		t.Fatalf("run state = %s, want PLANNED", document.Run.State)
	}
	if err := document.FixtureManifest.Validate(); err != nil {
		t.Fatalf("fixture manifest invalid: %v", err)
	}
	if document.FixtureManifest.Cleanup.Scope != "run/cli-contract/fixture/cli-fixture" {
		t.Fatalf("cleanup scope = %q", document.FixtureManifest.Cleanup.Scope)
	}
}
