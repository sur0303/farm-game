package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

var requiredEvidenceFiles = []string{
	"run-manifest.json", "planned-vs-actual.json", "resource-samples.ndjson", "latency-histograms.json",
	"queue-slope.json", "correctness-verdict.json", "environment-verdict.json", "service-capacity-result.json", "run-report.md",
}

func prepareOutput(path string) error {
	if entries, err := os.ReadDir(path); err == nil && len(entries) != 0 {
		return fmt.Errorf("output directory is not empty")
	}
	return os.MkdirAll(path, 0o700)
}

func copyManifest(source, output string) error {
	raw, err := os.ReadFile(source)
	if err != nil {
		return err
	}
	if containsForbiddenEvidence(raw) {
		return fmt.Errorf("manifest contains forbidden raw endpoint or secret field")
	}
	return writeExclusive(filepath.Join(output, "run-manifest.json"), raw)
}

func writeFinalManifest(source, output string, startedAt, finishedAt time.Time) error {
	raw, err := os.ReadFile(source)
	if err != nil {
		return err
	}
	var manifest map[string]any
	if json.Unmarshal(raw, &manifest) != nil {
		return fmt.Errorf("manifest JSON")
	}
	manifest["started_at"] = startedAt.UTC().Format(time.RFC3339Nano)
	manifest["finished_at"] = finishedAt.UTC().Format(time.RFC3339Nano)
	encoded, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	if containsForbiddenEvidence(encoded) {
		return fmt.Errorf("manifest contains forbidden raw endpoint or secret field")
	}
	return writeExclusive(filepath.Join(output, "run-manifest.json"), encoded)
}

func writeJSONExclusive(path string, value any) error {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return writeExclusive(path, append(raw, '\n'))
}

func writeExclusive(path string, raw []byte) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err = file.Write(raw); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func writeRunEvidence(output string, plan runPlan, accumulator *runAccumulator, state *collectionState, evaluation evaluation, qualification qualificationSummary, baseline tierBaseline) error {
	actual := accumulator.snapshot()
	samples, _, _, _, _ := state.snapshot()
	binding, err := loadBinding(plan.BindingPath)
	if err != nil {
		return err
	}
	prefix := "cap-single-pod-l2-" + strings.ToLower(binding.BindingID)
	if err := writeJSONExclusive(filepath.Join(output, "planned-vs-actual.json"), actual); err != nil {
		return err
	}
	latencies := map[string]any{
		"schema_version": prefix + ".latency-histograms.v0.1",
		"arrival_lag_ms": accumulator.arrivalLag.snapshot(), "operation_latency_ms": accumulator.latency.snapshot(),
	}
	if err := writeJSONExclusive(filepath.Join(output, "latency-histograms.json"), latencies); err != nil {
		return err
	}
	slope := queueSlope(samples)
	if err := writeJSONExclusive(filepath.Join(output, "queue-slope.json"), map[string]any{
		"schema_version": prefix + ".queue-slope.v0.1", "method": "OLS_LAST_60_SAMPLES",
		"minimum_samples": 55, "positive_requires_lower_95ci_above_zero": true,
		"positive_requires_net_growth_at_least": 5, "result": slope,
	}); err != nil {
		return err
	}
	correctnessStatus := "PASS"
	if evaluation.Verdict == VerdictFailCorrectness {
		correctnessStatus = "FAIL"
	} else if !evaluation.CorrectnessComplete {
		correctnessStatus = "INCONCLUSIVE"
	}
	if err := writeJSONExclusive(filepath.Join(output, "correctness-verdict.json"), map[string]any{
		"schema_version": prefix + ".correctness.v0.1", "status": correctnessStatus,
		"observation_complete": evaluation.CorrectnessComplete, "gates": evaluation.CorrectnessGates,
	}); err != nil {
		return err
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	binarySHA, err := exactFileSHA256(executable)
	if err != nil {
		return err
	}
	expectedBindingSHA := trustedActiveBindingSHA256[binding.BindingID]
	bindingIdentity := map[string]any{"binding_path_basename": binding.sourceBasename, "expected_binding_sha256": expectedBindingSHA, "actual_binding_sha256": binding.sourceSHA256, "binding_id": binding.BindingID, "binding_status": binding.Status}
	adapterIdentity := map[string]any{"binary_sha256": binarySHA, "binding_sha256": binding.sourceSHA256, "collector_config_sha256": plan.CollectorConfigSHA256, "oracle_config_sha256": plan.OracleConfigSHA256}
	environment := map[string]any{
		"schema_version": prefix + ".environment-verdict.v0.1", "valid": evaluation.EnvironmentValid,
		"observability_complete": evaluation.ObservabilityComplete, "load_generator_qualified": evaluation.LoadGeneratorQualified,
		"dependency_first_bottleneck": evaluation.DependencyFirst, "adapter_identity": adapterIdentity, "binding_identity": bindingIdentity,
	}
	capacity := map[string]any{
		"schema_version": prefix + ".service-capacity-result.v0.1", "binding_id": binding.BindingID, "run_id": plan.RunID,
		"service": plan.Service, "profile": plan.Profile, "phase": plan.Phase, "tier": plan.Tier, "duration_seconds": plan.DurationSeconds, "targets": plan.Targets,
		"phase_status": evaluation.PhaseStatus, "phase_decision": evaluation.PhaseDecision, "reasons": evaluation.Reasons, "actual": actual,
		"queue_slope": slope, "metrics": evaluation.Metrics, "adapter_identity": adapterIdentity, "binding_identity": bindingIdentity, "safe_constant_claimed": false,
		"tier_baseline": baseline.evidence(),
	}
	if qualification.QualificationEvidenceSHA256 != "" {
		summary := map[string]any{
			"qualification_run_id":          qualification.QualificationRunID,
			"qualification_evidence_sha256": qualification.QualificationEvidenceSHA256,
			"qualified_tier":                qualification.QualifiedTier,
			"requested_tier":                qualification.RequestedTier,
			"headroom_factor_actual":        qualification.HeadroomFactorActual,
		}
		environment["load_generator_qualification"] = summary
		capacity["load_generator_qualification"] = summary
	}
	if err := writeJSONExclusive(filepath.Join(output, "environment-verdict.json"), environment); err != nil {
		return err
	}
	if evaluation.Verdict != "" {
		capacity["verdict"] = evaluation.Verdict
	}
	if err := writeJSONExclusive(filepath.Join(output, "service-capacity-result.json"), capacity); err != nil {
		return err
	}
	report := renderReport(plan, actual, evaluation, slope)
	if err := writeExclusive(filepath.Join(output, "run-report.md"), []byte(report)); err != nil {
		return err
	}
	return writeEvidenceIndex(output, state.evidenceWriteErrorCount() == 0)
}

func renderReport(plan runPlan, actual plannedActual, evaluation evaluation, slope slopeResult) string {
	result := evaluation.Verdict
	if result == "" {
		result = evaluation.PhaseDecision
	}
	lines := []string{
		"# CAP-SINGLE-POD-L2 run report", "", "- Run: `" + plan.RunID + "`", "- Service/Profile: `" + plan.Service + " / " + plan.Profile + "`",
		"- Phase: `" + plan.Phase + "`", "- Phase status: `" + evaluation.PhaseStatus + "`", "- Result: **" + result + "**", "- `baseline_eligible=false`", "- `capacity_claim=NONE`", "- `supports_30m_dau=false`", "",
		"## Planned versus actual", "", fmt.Sprintf("- Planned/launched/unsent: `%d/%d/%d`", actual.Planned, actual.Launched, actual.ScheduledUnsent),
		fmt.Sprintf("- Completed/rejected/timeout/failed: `%d/%d/%d/%d`", actual.Completed, actual.Rejected, actual.Timeout, actual.Failed),
		fmt.Sprintf("- Actual/planned: `%.6f`", actual.ActualToPlannedRatio), fmt.Sprintf("- Queue slope/lower95/net: `%.6f / %.6f / %.3f`", slope.SlopePerSecond, slope.Lower95, slope.NetGrowth), "",
		"## Verdict reasons", "",
	}
	for _, reason := range evaluation.Reasons {
		lines = append(lines, "- `"+reason+"`")
	}
	lines = append(lines, "", "No safe constant is published by a single phase. Formal×3 plus independent review is required.", "")
	return strings.Join(lines, "\n")
}

func writeEvidenceIndex(output string, complete bool) error {
	entries := make([]map[string]any, 0, len(requiredEvidenceFiles))
	for _, name := range requiredEvidenceFiles {
		path := filepath.Join(output, name)
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if containsForbiddenEvidence(raw) {
			return fmt.Errorf("forbidden evidence content in %s", name)
		}
		digest := sha256.Sum256(raw)
		entries = append(entries, map[string]any{"path": name, "bytes": len(raw), "sha256": hex.EncodeToString(digest[:])})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i]["path"].(string) < entries[j]["path"].(string) })
	index := map[string]any{"schema_version": "cap-single-pod-l2.evidence-index.v0.1", "created_at": time.Now().UTC().Format(time.RFC3339Nano), "complete": complete, "files": entries}
	if !complete {
		index["incomplete_reason_code"] = "EVIDENCE_WRITE_ERROR"
	}
	return writeJSONExclusive(filepath.Join(output, "evidence-hash-index.json"), index)
}

func containsForbiddenEvidence(raw []byte) bool {
	lower := strings.ToLower(string(raw))
	for _, value := range []string{
		"redis://", "rediss://", "http://", "https://", "\"redis_url\"", "\"redis-url\"", "\"password\"", "\"secret\"",
		"\"operation_id\"", "\"operation_ids\"", "\"session_id\"", "\"session_token\"", "\"connection_ticket\"",
		"\"receipt_id\"", "\"workflow_id\"", "\"work_id\"", "\"authorization\"", "\"cookie\"", "\"raw_resp\"",
	} {
		if strings.Contains(lower, value) {
			return true
		}
	}
	return false
}
