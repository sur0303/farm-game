package demokit

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

const evidenceFormatVersion = 1

var (
	// ErrExecutionDisabled makes the safety boundary programmatically testable.
	ErrExecutionDisabled = errors.New("fault execution is disabled: democtl only creates and verifies plans")

	sourceRefPattern = regexp.MustCompile("^[0-9a-f]{7,64}$")

	evidenceFiles = []string{
		"action-plan.ndjson",
		"evidence-hash-index.json",
		"report.md",
		"run-manifest.json",
		"safety-verdict.json",
		"scenario-contract.json",
	}
)

// PlanRequest contains only information needed to create an inert evidence
// bundle. SourceRef is deliberately a commit identity instead of a URL, so a
// plan cannot accidentally become a command transport.
type PlanRequest struct {
	ScenarioID string
	OutputDir  string
	SourceRef  string
	CreatedAt  time.Time
}

// PlanResult identifies the atomically-created evidence bundle.
type PlanResult struct {
	RunID     string
	OutputDir string
	Scenario  Scenario
	SourceRef string
	Hashes    map[string]string
}

type runManifest struct {
	FormatVersion string    `json:"format_version"`
	RunID         string    `json:"run_id"`
	ScenarioID    string    `json:"scenario_id"`
	SourceRef     string    `json:"source_ref"`
	CreatedAt     time.Time `json:"created_at"`
	Execution     string    `json:"execution"`
}

type safetyVerdict struct {
	SafeForPlanGeneration bool     `json:"safe_for_plan_generation"`
	ExternalActions       bool     `json:"external_actions"`
	CapacityClaim         bool     `json:"capacity_claim"`
	RequiredOperatorGates []string `json:"required_operator_gates"`
}

type hashEntry struct {
	File   string `json:"file"`
	SHA256 string `json:"sha256"`
	Bytes  int    `json:"bytes"`
}

type actionPlanLine struct {
	ScenarioID string `json:"scenario_id"`
	StepID     string `json:"step_id"`
	Action     string `json:"action"`
	Expected   string `json:"expected"`
	Executable bool   `json:"executable"`
}

// Plan creates an evidence directory through a temporary sibling and atomic
// rename. It refuses to overwrite an existing directory and never starts a
// process, opens a socket, or contacts a data store.
func Plan(request PlanRequest) (PlanResult, error) {
	scenario, err := ScenarioByID(strings.TrimSpace(request.ScenarioID))
	if err != nil {
		return PlanResult{}, err
	}
	sourceRef := strings.ToLower(strings.TrimSpace(request.SourceRef))
	if !sourceRefPattern.MatchString(sourceRef) {
		return PlanResult{}, fmt.Errorf("source_ref must be a lower-case Git commit prefix, got %q", request.SourceRef)
	}
	outputDir := strings.TrimSpace(request.OutputDir)
	if outputDir == "" {
		return PlanResult{}, errors.New("output_dir is required")
	}
	absoluteOutput, err := filepath.Abs(outputDir)
	if err != nil {
		return PlanResult{}, fmt.Errorf("resolve output_dir: %w", err)
	}
	if absoluteOutput == string(filepath.Separator) || absoluteOutput == "." {
		return PlanResult{}, fmt.Errorf("refuse unsafe output_dir %q", outputDir)
	}
	if _, err := os.Lstat(absoluteOutput); err == nil {
		return PlanResult{}, fmt.Errorf("refuse to overwrite existing evidence directory %q", absoluteOutput)
	} else if !errors.Is(err, os.ErrNotExist) {
		return PlanResult{}, fmt.Errorf("inspect output_dir: %w", err)
	}

	createdAt := request.CreatedAt.UTC()
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	runID, err := NewRunID(createdAt)
	if err != nil {
		return PlanResult{}, err
	}
	parent := filepath.Dir(absoluteOutput)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return PlanResult{}, fmt.Errorf("create evidence parent: %w", err)
	}
	tempDir, err := os.MkdirTemp(parent, "."+filepath.Base(absoluteOutput)+".plan-")
	if err != nil {
		return PlanResult{}, fmt.Errorf("create evidence staging dir: %w", err)
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.RemoveAll(tempDir)
		}
	}()

	manifest := runManifest{
		FormatVersion: "resilience-demo/v1",
		RunID:         runID,
		ScenarioID:    scenario.ID,
		SourceRef:     sourceRef,
		CreatedAt:     createdAt,
		Execution:     "PLAN_ONLY",
	}
	verdict := safetyVerdict{
		SafeForPlanGeneration: true,
		ExternalActions:       false,
		CapacityClaim:         false,
		RequiredOperatorGates: []string{
			"approved non-production namespace",
			"explicit operator fault-injection command",
			"pre-flight backup and rollback check",
		},
	}
	if err := writeJSONExclusive(filepath.Join(tempDir, "run-manifest.json"), manifest); err != nil {
		return PlanResult{}, err
	}
	if err := writeJSONExclusive(filepath.Join(tempDir, "scenario-contract.json"), scenario); err != nil {
		return PlanResult{}, err
	}
	if err := writeJSONExclusive(filepath.Join(tempDir, "safety-verdict.json"), verdict); err != nil {
		return PlanResult{}, err
	}
	if err := writeActionPlan(filepath.Join(tempDir, "action-plan.ndjson"), scenario); err != nil {
		return PlanResult{}, err
	}
	if err := writeReport(filepath.Join(tempDir, "report.md"), manifest, scenario); err != nil {
		return PlanResult{}, err
	}
	hashes, err := hashEvidence(tempDir, false)
	if err != nil {
		return PlanResult{}, err
	}
	if err := writeJSONExclusive(filepath.Join(tempDir, "evidence-hash-index.json"), hashes); err != nil {
		return PlanResult{}, err
	}
	if err := os.Rename(tempDir, absoluteOutput); err != nil {
		return PlanResult{}, fmt.Errorf("publish evidence atomically: %w", err)
	}
	cleanup = false

	resultHashes := make(map[string]string, len(hashes))
	for _, entry := range hashes {
		resultHashes[entry.File] = entry.SHA256
	}
	return PlanResult{
		RunID:     runID,
		OutputDir: absoluteOutput,
		Scenario:  scenario,
		SourceRef: sourceRef,
		Hashes:    resultHashes,
	}, nil
}

// Verify checks the exact evidence shape, content hash index, and absence of
// externally actionable transport text. It is safe to run against untrusted
// evidence directories because it only reads regular files below outputDir.
func Verify(outputDir string) error {
	absoluteOutput, err := filepath.Abs(strings.TrimSpace(outputDir))
	if err != nil {
		return fmt.Errorf("resolve evidence directory: %w", err)
	}
	entries, err := os.ReadDir(absoluteOutput)
	if err != nil {
		return fmt.Errorf("read evidence directory: %w", err)
	}
	actual := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("evidence contains non-regular entry %q", entry.Name())
		}
		actual = append(actual, entry.Name())
	}
	sort.Strings(actual)
	expected := append([]string(nil), evidenceFiles...)
	sort.Strings(expected)
	if strings.Join(actual, "\n") != strings.Join(expected, "\n") {
		return fmt.Errorf("unexpected evidence file set: got %v, want %v", actual, expected)
	}

	manifestBytes, err := os.ReadFile(filepath.Join(absoluteOutput, "run-manifest.json"))
	if err != nil {
		return err
	}
	var manifest runManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return fmt.Errorf("decode manifest: %w", err)
	}
	if manifest.Execution != "PLAN_ONLY" || !sourceRefPattern.MatchString(manifest.SourceRef) {
		return errors.New("manifest violates plan-only source identity contract")
	}

	indexBytes, err := os.ReadFile(filepath.Join(absoluteOutput, "evidence-hash-index.json"))
	if err != nil {
		return err
	}
	var indexed []hashEntry
	if err := json.Unmarshal(indexBytes, &indexed); err != nil {
		return fmt.Errorf("decode evidence hash index: %w", err)
	}
	actualHashes, err := hashEvidence(absoluteOutput, false)
	if err != nil {
		return err
	}
	if !sameHashes(indexed, actualHashes) {
		return errors.New("evidence hash index does not match evidence content")
	}
	for _, name := range actual {
		body, err := os.ReadFile(filepath.Join(absoluteOutput, name))
		if err != nil {
			return err
		}
		if containsForbiddenEvidence(body) {
			return fmt.Errorf("evidence file %q contains forbidden executable transport text", name)
		}
	}
	return nil
}

// Execute is deliberately unavailable. Fault injection belongs in a separately
// reviewed tool, never in a helper that can be run while load tests are active.
func Execute() error {
	return ErrExecutionDisabled
}

// NewRunID returns a human-sortable run identity with enough randomness to
// avoid collisions between separate agents writing evidence in the same second.
func NewRunID(now time.Time) (string, error) {
	var entropy [6]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", fmt.Errorf("generate run id entropy: %w", err)
	}
	return now.UTC().Format("20060102T150405.000000000Z") + "-" + hex.EncodeToString(entropy[:]), nil
}

func writeJSONExclusive(path string, value any) error {
	bytes, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal %s: %w", filepath.Base(path), err)
	}
	bytes = append(bytes, '\n')
	return writeFileExclusive(path, bytes)
}

func writeActionPlan(path string, scenario Scenario) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("create %s: %w", filepath.Base(path), err)
	}
	encoder := json.NewEncoder(file)
	for _, step := range scenario.Steps {
		if err := encoder.Encode(actionPlanLine{
			ScenarioID: scenario.ID,
			StepID:     step.ID,
			Action:     "OPERATOR_CONFIRM_REQUIRED",
			Expected:   step.Expected,
			Executable: false,
		}); err != nil {
			_ = file.Close()
			return fmt.Errorf("write %s: %w", filepath.Base(path), err)
		}
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close %s: %w", filepath.Base(path), err)
	}
	return nil
}

func writeReport(path string, manifest runManifest, scenario Scenario) error {
	var builder strings.Builder
	fmt.Fprintf(&builder, "# Resilience Demo Plan: %s ? %s\n\n", scenario.ID, scenario.Title)
	fmt.Fprintf(&builder, "- Run ID: %s\n- Source commit: %s\n- Mode: PLAN_ONLY\n- External actions: none\n- Capacity conclusion: none\n\n", manifest.RunID, manifest.SourceRef)
	builder.WriteString("## Goal\n\n" + scenario.Goal + "\n\n## Operator steps\n\n")
	for index, step := range scenario.Steps {
		fmt.Fprintf(&builder, "%d. %s ? expected: %s\n", index+1, step.Description, step.Expected)
	}
	builder.WriteString("\n## Assertions\n\n")
	for _, assertion := range scenario.Assertions {
		builder.WriteString("- " + assertion + "\n")
	}
	builder.WriteString("\nThis package is a reviewable scenario contract only. It does not run a fault, deploy a workload, or establish a capacity result.\n")
	return writeFileExclusive(path, []byte(builder.String()))
}

func writeFileExclusive(path string, body []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("create %s: %w", filepath.Base(path), err)
	}
	if _, err := file.Write(body); err != nil {
		_ = file.Close()
		return fmt.Errorf("write %s: %w", filepath.Base(path), err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close %s: %w", filepath.Base(path), err)
	}
	return nil
}

func hashEvidence(directory string, includeIndex bool) ([]hashEntry, error) {
	names := []string{"action-plan.ndjson", "report.md", "run-manifest.json", "safety-verdict.json", "scenario-contract.json"}
	if includeIndex {
		names = append(names, "evidence-hash-index.json")
	}
	entries := make([]hashEntry, 0, len(names))
	for _, name := range names {
		body, err := os.ReadFile(filepath.Join(directory, name))
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", name, err)
		}
		sum := sha256.Sum256(body)
		entries = append(entries, hashEntry{File: name, SHA256: hex.EncodeToString(sum[:]), Bytes: len(body)})
	}
	return entries, nil
}

func sameHashes(left, right []hashEntry) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func containsForbiddenEvidence(body []byte) bool {
	lower := strings.ToLower(string(body))
	for _, forbidden := range []string{"kubectl ", "redis://", "http://", "https://", "ssh ", "curl "} {
		if strings.Contains(lower, forbidden) {
			return true
		}
	}
	return false
}
