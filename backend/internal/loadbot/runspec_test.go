package loadbot

import (
	"testing"
	"time"
)

func TestRunSpecRejectsUnsafeExecutionAndEnvironment(t *testing.T) {
	config := DefaultConfig()
	config.RunID = "contract-test"
	spec, err := NewRunSpec(config, "", string(EnvironmentSandbox), "")
	if err != nil {
		t.Fatalf("NewRunSpec() error = %v", err)
	}
	if err := spec.Validate(); err != nil {
		t.Fatalf("RunSpec.Validate() error = %v", err)
	}
	for _, environment := range []string{"production", "staging", "live", "prod"} {
		if err := ValidateSafeEnvironment(environment); err == nil {
			t.Fatalf("environment %q was accepted", environment)
		}
	}
	spec.TransportEnabled = true
	if err := spec.Validate(); err == nil {
		t.Fatal("transport-enabled RunSpec was accepted")
	}
	spec, err = NewRunSpec(config, "", string(EnvironmentSandbox), "")
	if err != nil {
		t.Fatal(err)
	}
	spec.Namespace = "shared"
	if _, err := spec.WithFingerprint(); err == nil {
		t.Fatal("unscoped namespace was accepted")
	}
}

func TestRunSpecRejectsDangerousNamespaceTokens(t *testing.T) {
	config := DefaultConfig()
	config.RunID = "namespace-token-test"
	for _, token := range []string{
		"prod", "production", "preprod", "staging", "stage", "stg", "live", "prd",
	} {
		t.Run(token, func(t *testing.T) {
			namespace := "loadbot-namespace-token-test-" + token
			if _, err := NewRunSpec(
				config,
				"namespace-token-fixture",
				string(EnvironmentSandbox),
				namespace,
			); err == nil {
				t.Fatalf("namespace %q was accepted", namespace)
			}
		})
	}
}

func TestRunLifecycleIsVersionedAndStateSafe(t *testing.T) {
	config := DefaultConfig()
	config.RunID = "lifecycle-test"
	spec, err := NewRunSpec(config, "lifecycle-fixture", string(EnvironmentDryRun), "")
	if err != nil {
		t.Fatal(err)
	}
	at := time.Unix(100, 0).UTC()
	run, err := NewRun(spec, at)
	if err != nil {
		t.Fatal(err)
	}
	events := []struct {
		event RunEvent
		state RunState
	}{
		{RunEventPrepare, RunStatePreparing},
		{RunEventReady, RunStateReady},
		{RunEventStart, RunStateRunning},
		{RunEventStop, RunStateStopping},
		{RunEventSucceed, RunStateSucceeded},
		{RunEventCleanup, RunStateCleaned},
	}
	for index, step := range events {
		if err := run.Apply(step.event, at.Add(time.Duration(index+1)*time.Second), ""); err != nil {
			t.Fatalf("event %s: %v", step.event, err)
		}
		if run.State != step.state {
			t.Fatalf("state after %s = %s, want %s", step.event, run.State, step.state)
		}
	}
	if err := run.Validate(); err != nil {
		t.Fatalf("final run invalid: %v", err)
	}
	if err := run.Apply(RunEventStart, at.Add(time.Minute), ""); err == nil {
		t.Fatal("cleaned run accepted a new start")
	}

	failed, err := NewRun(spec, at)
	if err != nil {
		t.Fatal(err)
	}
	if err := failed.Apply(RunEventFail, at.Add(time.Second), "fixture preparation failed"); err != nil {
		t.Fatal(err)
	}
	if failed.State != RunStateFailed || failed.FailureReason != "fixture preparation failed" {
		t.Fatalf("failed run = %+v", failed)
	}
	if err := failed.Apply(RunEventCleanup, at.Add(2*time.Second), ""); err != nil {
		t.Fatal(err)
	}
	if err := failed.Apply(RunEventStart, at.Add(3*time.Second), ""); err == nil {
		t.Fatal("cleaned failed run accepted start")
	}
}

func TestRunLifecycleRequiresFailureReason(t *testing.T) {
	config := DefaultConfig()
	config.RunID = "reason-test"
	spec, err := NewRunSpec(config, "", string(EnvironmentCI), "")
	if err != nil {
		t.Fatal(err)
	}
	run, err := NewRun(spec, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if err := run.Apply(RunEventAbort, time.Unix(2, 0), ""); err == nil {
		t.Fatal("abort without reason was accepted")
	}
}

func TestNewRunRejectsZeroTime(t *testing.T) {
	config := DefaultConfig()
	config.RunID = "zero-time-test"
	spec, err := NewRunSpec(config, "zero-time-fixture", string(EnvironmentCI), "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewRun(spec, time.Time{}); err == nil {
		t.Fatal("NewRun accepted a zero creation time")
	}
}
