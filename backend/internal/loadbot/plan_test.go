package loadbot

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestDefaultConfigValidates(t *testing.T) {
	if err := DefaultConfig().Validate(); err != nil {
		t.Fatalf("DefaultConfig().Validate() error = %v", err)
	}
}

func TestBuildDryRunIsDeterministicAndStateSafe(t *testing.T) {
	config := DefaultConfig()
	config.RunID = "mixed-farm-test"
	first, err := BuildDryRun(config, 100, 12)
	if err != nil {
		t.Fatalf("first BuildDryRun() error = %v", err)
	}
	second, err := BuildDryRun(config, 100, 12)
	if err != nil {
		t.Fatalf("second BuildDryRun() error = %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		firstJSON, _ := json.Marshal(first)
		secondJSON, _ := json.Marshal(second)
		t.Fatalf("same seed produced different plans\nfirst=%s\nsecond=%s", firstJSON, secondJSON)
	}
	if err := ValidatePlan(first); err != nil {
		t.Fatalf("ValidatePlan() error = %v", err)
	}
	if first.Execution != "DRY_RUN_ONLY" || first.CapacityClaim {
		t.Fatalf("unexpected plan safety flags: %#v", first)
	}
	for _, player := range first.Players {
		for _, step := range player.Steps {
			if (step.Action == ActionCare || step.Action == ActionTheft) && step.StateBefore != StateFriendFarm {
				t.Fatalf("social action outside friend farm: %#v", step)
			}
		}
	}
}

func TestBuildDryRunRejectsUnsafeOrInvalidInputs(t *testing.T) {
	config := DefaultConfig()
	config.StrictTransportEnabled = true
	if _, err := BuildDryRun(config, 1, 1); err == nil {
		t.Fatal("BuildDryRun accepted a reserved execution adapter")
	}
	config = DefaultConfig()
	if _, err := BuildDryRun(config, 0, 1); err == nil {
		t.Fatal("BuildDryRun accepted zero players")
	}
	if _, err := BuildDryRun(config, 1, 0); err == nil {
		t.Fatal("BuildDryRun accepted zero action count")
	}
	if _, err := BuildDryRun(config, 1_000, 1_000); err == nil {
		t.Fatal("BuildDryRun accepted an excessive aggregate plan")
	}
}

func TestValidatePlanRejectsTamper(t *testing.T) {
	plan, err := BuildDryRun(DefaultConfig(), 3, 4)
	if err != nil {
		t.Fatal(err)
	}
	plan.Players[0].Steps[0].StateAfter = StateEnded
	if err := ValidatePlan(plan); err == nil {
		t.Fatal("ValidatePlan accepted a tampered transition")
	}
}
