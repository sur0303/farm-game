package loadbot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const RunSpecSchemaVersion = "mixed-farm-bot/runspec/v1"
const RunSchemaVersion = "mixed-farm-bot/run/v1"

type RunExecution string

const ExecutionDryRunOnly RunExecution = "DRY_RUN_ONLY"

type SafeEnvironment string

const (
	EnvironmentLocal   SafeEnvironment = "local"
	EnvironmentCI      SafeEnvironment = "ci"
	EnvironmentSandbox SafeEnvironment = "sandbox"
	EnvironmentDryRun  SafeEnvironment = "dry-run"
)

var safeEnvironmentSet = map[SafeEnvironment]struct{}{
	EnvironmentCI: {}, EnvironmentDryRun: {}, EnvironmentLocal: {}, EnvironmentSandbox: {},
}

var dangerousNamespaceTokenSet = map[string]struct{}{
	"live": {}, "prd": {}, "preprod": {}, "prod": {}, "production": {},
	"stage": {}, "staging": {}, "stg": {},
}

func SafeEnvironments() []SafeEnvironment {
	result := make([]SafeEnvironment, 0, len(safeEnvironmentSet))
	for environment := range safeEnvironmentSet {
		result = append(result, environment)
	}
	sort.Slice(result, func(left, right int) bool { return result[left] < result[right] })
	return result
}

func ValidateSafeEnvironment(environment string) error {
	normalized := SafeEnvironment(strings.ToLower(strings.TrimSpace(environment)))
	if _, ok := safeEnvironmentSet[normalized]; !ok {
		return fmt.Errorf("environment %q is not in the loadbot safe allowlist", environment)
	}
	return nil
}

// RunSpec describes a transport-neutral dry run. It is not permission to
// connect to Redis, HTTP, WSS, or any other service.
type RunSpec struct {
	SchemaVersion    string       `json:"schema_version"`
	RunID            string       `json:"run_id"`
	FixtureID        string       `json:"fixture_id"`
	Environment      string       `json:"environment"`
	Namespace        string       `json:"namespace"`
	Execution        RunExecution `json:"execution"`
	CapacityClaim    bool         `json:"capacity_claim"`
	TransportEnabled bool         `json:"transport_enabled"`
	Workload         Config       `json:"workload"`
	Fingerprint      string       `json:"fingerprint"`
}

func NewRunSpec(config Config, fixtureID, environment, namespace string) (RunSpec, error) {
	if strings.TrimSpace(config.RunID) == "" {
		return RunSpec{}, errors.New("run spec: workload run_id is required")
	}
	if strings.TrimSpace(environment) == "" {
		environment = string(EnvironmentLocal)
	}
	if strings.TrimSpace(fixtureID) == "" {
		fixtureID = slugIdentifier(config.RunID) + "-fixture"
	}
	if strings.TrimSpace(namespace) == "" {
		namespace = "loadbot-" + slugIdentifier(config.RunID)
	}
	spec := RunSpec{
		SchemaVersion: RunSpecSchemaVersion, RunID: config.RunID, FixtureID: fixtureID,
		Environment: strings.ToLower(strings.TrimSpace(environment)), Namespace: namespace,
		Execution: ExecutionDryRunOnly, Workload: config,
	}
	return spec.WithFingerprint()
}

func BuildRunSpec(config Config, fixtureID, environment, namespace string) (RunSpec, error) {
	return NewRunSpec(config, fixtureID, environment, namespace)
}

func (spec RunSpec) Canonical() RunSpec {
	result := spec
	result.Environment = strings.ToLower(strings.TrimSpace(result.Environment))
	result.FixtureID = strings.TrimSpace(result.FixtureID)
	result.Namespace = strings.TrimSpace(result.Namespace)
	result.Workload.Profiles = result.Workload.CanonicalProfiles()
	result.Workload.Actions = result.Workload.CanonicalActions()
	return result
}

func (spec RunSpec) validateStructure() error {
	if spec.SchemaVersion != RunSpecSchemaVersion {
		return fmt.Errorf("run spec schema_version must be %q", RunSpecSchemaVersion)
	}
	if err := validateScopedIdentifier("run_id", spec.RunID); err != nil {
		return err
	}
	if err := validateScopedIdentifier("fixture_id", spec.FixtureID); err != nil {
		return err
	}
	if err := ValidateSafeEnvironment(spec.Environment); err != nil {
		return err
	}
	if err := validateNamespace(spec.Namespace, spec.RunID); err != nil {
		return err
	}
	if spec.Execution != ExecutionDryRunOnly {
		return fmt.Errorf("run spec execution must remain %q", ExecutionDryRunOnly)
	}
	if spec.CapacityClaim {
		return errors.New("run spec cannot make a capacity claim")
	}
	if spec.TransportEnabled {
		return errors.New("run spec cannot enable transport")
	}
	if spec.Workload.RunID != spec.RunID {
		return errors.New("run spec run_id must match workload run_id")
	}
	return spec.Workload.Validate()
}

func (spec RunSpec) Validate() error {
	if err := spec.validateStructure(); err != nil {
		return err
	}
	if strings.TrimSpace(spec.Fingerprint) == "" {
		return errors.New("run spec fingerprint is required")
	}
	expected, err := spec.computeFingerprint()
	if err != nil {
		return err
	}
	if spec.Fingerprint != expected {
		return errors.New("run spec fingerprint mismatch")
	}
	return nil
}

func (spec RunSpec) WithFingerprint() (RunSpec, error) {
	spec = spec.Canonical()
	if err := spec.validateStructure(); err != nil {
		return RunSpec{}, err
	}
	fingerprint, err := spec.computeFingerprint()
	if err != nil {
		return RunSpec{}, err
	}
	spec.Fingerprint = fingerprint
	return spec, nil
}

func (spec RunSpec) computeFingerprint() (string, error) {
	copySpec := spec.Canonical()
	copySpec.Fingerprint = ""
	encoded, err := json.Marshal(copySpec)
	if err != nil {
		return "", fmt.Errorf("encode run spec: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func validateScopedIdentifier(field, value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return fmt.Errorf("%s is required", field)
	}
	if len(value) > 96 {
		return fmt.Errorf("%s is too long", field)
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || character == '-' || character == '_' || character == '.' {
			continue
		}
		return fmt.Errorf("%s contains unsafe character %q", field, character)
	}
	return nil
}

func slugIdentifier(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var builder strings.Builder
	for _, character := range value {
		switch {
		case character >= 'a' && character <= 'z', character >= '0' && character <= '9':
			builder.WriteRune(character)
		case character == '-' || character == '_':
			builder.WriteRune('-')
		}
	}
	result := strings.Trim(builder.String(), "-")
	if result == "" {
		return "run"
	}
	return result
}

func validateNamespace(namespace, runID string) error {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return errors.New("namespace is required")
	}
	if len(namespace) > 63 || strings.ToLower(namespace) != namespace {
		return errors.New("namespace must be lowercase and at most 63 characters")
	}
	if !strings.Contains(namespace, slugIdentifier(runID)) {
		return fmt.Errorf("namespace %q is not scoped to run_id %q", namespace, runID)
	}
	for _, character := range namespace {
		if (character >= 'a' && character <= 'z') || (character >= '0' && character <= '9') || character == '-' {
			continue
		}
		return fmt.Errorf("namespace contains unsafe character %q", character)
	}
	for _, token := range strings.Split(namespace, "-") {
		if _, dangerous := dangerousNamespaceTokenSet[token]; dangerous {
			return fmt.Errorf("namespace %q contains dangerous environment token %q", namespace, token)
		}
	}
	return nil
}

type RunState string

const (
	RunStatePlanned   RunState = "PLANNED"
	RunStatePreparing RunState = "PREPARING"
	RunStateReady     RunState = "READY"
	RunStateRunning   RunState = "RUNNING"
	RunStateStopping  RunState = "STOPPING"
	RunStateSucceeded RunState = "SUCCEEDED"
	RunStateFailed    RunState = "FAILED"
	RunStateAborted   RunState = "ABORTED"
	RunStateCleaned   RunState = "CLEANED"
)

const (
	RunPlanned   = RunStatePlanned
	RunPreparing = RunStatePreparing
	RunReady     = RunStateReady
	RunRunning   = RunStateRunning
	RunStopping  = RunStateStopping
	RunSucceeded = RunStateSucceeded
	RunFailed    = RunStateFailed
	RunAborted   = RunStateAborted
	RunCleaned   = RunStateCleaned
)

func (state RunState) Validate() error {
	switch state {
	case RunStatePlanned, RunStatePreparing, RunStateReady, RunStateRunning, RunStateStopping,
		RunStateSucceeded, RunStateFailed, RunStateAborted, RunStateCleaned:
		return nil
	default:
		return fmt.Errorf("unknown run state %q", state)
	}
}

type RunEvent string

const (
	RunEventPrepare RunEvent = "prepare"
	RunEventReady   RunEvent = "ready"
	RunEventStart   RunEvent = "start"
	RunEventStop    RunEvent = "stop"
	RunEventSucceed RunEvent = "succeed"
	RunEventFail    RunEvent = "fail"
	RunEventAbort   RunEvent = "abort"
	RunEventCleanup RunEvent = "cleanup"
)

type Run struct {
	SchemaVersion string    `json:"schema_version"`
	Spec          RunSpec   `json:"spec"`
	State         RunState  `json:"state"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
	FailureReason string    `json:"failure_reason,omitempty"`
}

func NewRun(spec RunSpec, at time.Time) (Run, error) {
	if err := spec.Validate(); err != nil {
		return Run{}, err
	}
	if at.IsZero() {
		return Run{}, errors.New("run creation time is required")
	}
	at = lifecycleTime(at)
	return Run{SchemaVersion: RunSchemaVersion, Spec: spec.Canonical(), State: RunStatePlanned, CreatedAt: at, UpdatedAt: at}, nil
}

func (run Run) Validate() error {
	if run.SchemaVersion != RunSchemaVersion {
		return fmt.Errorf("run schema_version must be %q", RunSchemaVersion)
	}
	if err := run.Spec.Validate(); err != nil {
		return err
	}
	if err := run.State.Validate(); err != nil {
		return err
	}
	if run.CreatedAt.IsZero() || run.UpdatedAt.IsZero() || run.UpdatedAt.Before(run.CreatedAt) {
		return errors.New("run timestamps are invalid")
	}
	if run.State != RunStateFailed && run.State != RunStateAborted && run.State != RunStateCleaned && run.FailureReason != "" {
		return errors.New("failure_reason is only valid for failed or aborted runs")
	}
	return nil
}

func (run *Run) Transition(next RunState, at time.Time) error {
	return run.transition(next, at, "")
}

func (run *Run) Apply(event RunEvent, at time.Time, reason string) error {
	if run == nil {
		return errors.New("run is nil")
	}
	var next RunState
	switch event {
	case RunEventPrepare:
		next = RunStatePreparing
	case RunEventReady:
		next = RunStateReady
	case RunEventStart:
		next = RunStateRunning
	case RunEventStop:
		next = RunStateStopping
	case RunEventSucceed:
		next = RunStateSucceeded
	case RunEventFail:
		next = RunStateFailed
	case RunEventAbort:
		next = RunStateAborted
	case RunEventCleanup:
		next = RunStateCleaned
	default:
		return fmt.Errorf("unknown run event %q", event)
	}
	if (next == RunStateFailed || next == RunStateAborted) && strings.TrimSpace(reason) == "" {
		return errors.New("failed or aborted run requires a reason")
	}
	return run.transition(next, at, strings.TrimSpace(reason))
}

func (run *Run) transition(next RunState, at time.Time, reason string) error {
	if run == nil {
		return errors.New("run is nil")
	}
	if err := run.Validate(); err != nil {
		return fmt.Errorf("current run is invalid: %w", err)
	}
	if err := next.Validate(); err != nil {
		return err
	}
	if !legalRunTransition(run.State, next) {
		return fmt.Errorf("illegal run transition %s -> %s", run.State, next)
	}
	if (next == RunStateFailed || next == RunStateAborted) && strings.TrimSpace(reason) == "" {
		return errors.New("failed or aborted run requires a reason")
	}
	if at.IsZero() {
		return errors.New("run transition time is required")
	}
	at = lifecycleTime(at)
	if at.Before(run.UpdatedAt) {
		return errors.New("run transition time moves backwards")
	}
	run.State, run.UpdatedAt = next, at
	if next == RunStateFailed || next == RunStateAborted {
		run.FailureReason = reason
	} else if next != RunStateCleaned {
		run.FailureReason = ""
	}
	return nil
}

func lifecycleTime(at time.Time) time.Time {
	return at.UTC()
}

func legalRunTransition(from, to RunState) bool {
	switch from {
	case RunStatePlanned:
		return to == RunStatePreparing || to == RunStateFailed || to == RunStateAborted
	case RunStatePreparing:
		return to == RunStateReady || to == RunStateFailed || to == RunStateAborted
	case RunStateReady:
		return to == RunStateRunning || to == RunStateStopping || to == RunStateFailed || to == RunStateAborted
	case RunStateRunning:
		return to == RunStateStopping || to == RunStateFailed || to == RunStateAborted
	case RunStateStopping:
		return to == RunStateSucceeded || to == RunStateFailed || to == RunStateAborted
	case RunStateSucceeded, RunStateFailed, RunStateAborted:
		return to == RunStateCleaned
	default:
		return false
	}
}
