package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fatal("usage: cap-single-pod <verify-contract|run|serve-collector|serve-oracle|finalize-formal>")
	}
	switch os.Args[1] {
	case "verify-contract":
		verifyContractCommand(os.Args[2:])
	case "run":
		runCommand(os.Args[2:])
	case "serve-collector":
		serveCollectorCommand(os.Args[2:])
	case "serve-oracle":
		serveOracleCommand(os.Args[2:])
	case "finalize-formal":
		finalizeFormalCommand(os.Args[2:])
	default:
		fatal("unknown command")
	}
}

func verifyContractCommand(arguments []string) {
	flags := flag.NewFlagSet("verify-contract", flag.ExitOnError)
	directory := flags.String("contract-dir", "", "contract directory")
	_ = flags.Parse(arguments)
	if *directory == "" {
		fatal("contract-dir required")
	}
	if err := verifyDesignIndex(*directory); err != nil {
		fatal(err.Error())
	}
	fmt.Println("L2_CONTRACT_VERIFY=PASS")
}

func runCommand(arguments []string) {
	flags := flag.NewFlagSet("run", flag.ExitOnError)
	planPath := flags.String("plan", "", "absolute run plan")
	output := flags.String("out", "", "new evidence directory")
	_ = flags.Parse(arguments)
	if *planPath == "" || *output == "" || !filepath.IsAbs(*planPath) || !filepath.IsAbs(*output) {
		fatal("absolute plan and out required")
	}
	plan, err := loadPlan(*planPath)
	if err != nil {
		fatal(err.Error())
	}
	baseline, err := resolveTierBaseline(plan)
	if err != nil {
		fatal("tier baseline: " + err.Error())
	}
	qualification, err := preflight(plan, baseline)
	if err != nil {
		fatal("preflight: " + err.Error())
	}
	if err = prepareOutput(*output); err != nil {
		fatal(err.Error())
	}
	binding, err := loadBinding(plan.BindingPath)
	if err != nil {
		fatal(err.Error())
	}
	required, err := loadRequiredSpec(plan.RequiredMetricsPath, plan.Service, binding.RequiredMetricsSchemaVersion)
	if err != nil {
		fatal(err.Error())
	}
	schedule := newScheduler(plan, required.Gates)
	judge := newTierJudge(plan, baseline)
	collectorValue, err := newCollector(plan, filepath.Join(*output, "resource-samples.ndjson"), schedule.accumulator, judge)
	if err != nil {
		fatal(err.Error())
	}
	startedAt := time.Now().UTC()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	collectorDone := make(chan struct{})
	go func() { defer close(collectorDone); collectorValue.run(runCtx, cancelRun) }()
	accumulator, runErr := schedule.run(runCtx)
	if runErr == nil && runCtx.Err() == nil && !schedule.correctness.Load() && collectorValue.state.runtimeStopCode() == "" {
		judge.arrivalsCompleted()
		awaitControlledDrain(runCtx, judge, time.Duration(plan.SampleIntervalMS)*time.Millisecond)
	}
	cancelRun()
	<-collectorDone
	_ = collectorValue.state.addOracle(collectorValue.fetchOracle(context.Background()))
	if closeErr := collectorValue.close(); closeErr != nil {
		collectorValue.state.addEvidenceWriteError()
	}
	finishedAt := time.Now().UTC()
	if accumulator == nil {
		fatal("scheduler produced no accumulator")
	}
	if err := writeFinalManifest(plan.ManifestPath, *output, startedAt, finishedAt); err != nil {
		fatal(err.Error())
	}
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		fatal(runErr.Error())
	}
	samples, oracleGates, oracleComplete, observabilityComplete, runtimeStop := collectorValue.state.snapshot()
	if len(samples) == 0 {
		observabilityComplete = false
	}
	evaluation := evaluateRun(plan, accumulator.snapshot(), accumulator.correctnessSnapshot(), samples, oracleGates, oracleComplete, observabilityComplete, runtimeStop)
	for name, value := range collectorValue.state.diagnosticMetrics() {
		evaluation.Metrics[name] = value
	}
	for name, value := range judge.summaryMetrics() {
		evaluation.Metrics[name] = value
	}
	if err = writeRunEvidence(*output, plan, accumulator, collectorValue.state, evaluation, qualification, baseline); err != nil {
		fatal(err.Error())
	}
	result := evaluation.Verdict
	if result == "" {
		result = evaluation.PhaseDecision
	}
	fmt.Printf("CAP_SINGLE_POD_RESULT=%s OUT=%s\n", result, *output)
	if evaluation.Verdict != "" {
		os.Exit(1)
	}
}

// awaitControlledDrain keeps the collector sampling after the planned load ends,
// for as long as the profile allows the drain to take, so that the drain is a
// measured outcome rather than an assumption about what would have happened.
func awaitControlledDrain(ctx context.Context, judge *tierJudge, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	backstop := time.After(controlledDrainMaxSeconds * time.Second)
	for !judge.drainDecided() {
		select {
		case <-ctx.Done():
			return
		case <-backstop:
			return
		case <-ticker.C:
		}
	}
}

func preflight(plan runPlan, baseline tierBaseline) (qualificationSummary, error) {
	binding, err := loadBinding(plan.BindingPath)
	if err != nil {
		return qualificationSummary{}, err
	}
	required, err := loadRequiredSpec(plan.RequiredMetricsPath, plan.Service, binding.RequiredMetricsSchemaVersion)
	if err != nil {
		return qualificationSummary{}, err
	}
	if err := verifyManifest(plan); err != nil {
		return qualificationSummary{}, err
	}
	qualification, err := verifyLoadGeneratorQualification(plan)
	if err != nil {
		return qualificationSummary{}, err
	}
	if err := verifyDriverHealth(plan); err != nil {
		return qualificationSummary{}, err
	}
	if err := verifyAdapterHealth(plan); err != nil {
		return qualificationSummary{}, err
	}
	var sample resourceSnapshot
	client := &http.Client{Timeout: 2 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if !fetchStrictJSON(ctx, client, plan.SnapshotURL, &sample) {
		return qualificationSummary{}, errors.New("required metrics unavailable")
	}
	if sample.MetricValues == nil {
		sample.MetricValues = map[string]float64{}
	}
	normalizeSnapshotMetrics(&sample, newRunAccumulator(), newTierJudge(plan, baseline))
	requiredNames := effectiveRequiredMetricNames(required)
	if unexpectedMetricSet(sample.MetricValues, requiredNames) {
		sample.MissingMetrics = append(sample.MissingMetrics, "UNEXPECTED_METRIC_SET")
	}
	sample.MissingMetrics = uniqueSorted(sample.MissingMetrics, missingMetricNames(sample.MetricValues, requiredNames))
	sample.MissingMetrics = sanitizeMissingMetrics(sample.MissingMetrics, requiredNames)
	if sample.ScrapeErrorCode != "" && sample.ScrapeErrorCode != "COLLECTOR_SOURCE_FAILED" {
		return qualificationSummary{}, errors.New("required metrics unavailable")
	}
	if len(sample.MissingMetrics) != 0 || sample.ScrapeErrorCode != "" {
		return qualificationSummary{}, errors.New("required metrics unavailable")
	}
	var oracle oracleSnapshot
	if !fetchStrictJSON(ctx, client, plan.OracleURL, &oracle) || !oracle.ObservationComplete || oracle.ErrorCode != "" || !exactGateSet(oracle.Gates, required.Gates) {
		return qualificationSummary{}, errors.New("oracle unavailable or gate set incomplete")
	}
	for _, value := range oracle.Gates {
		if value < 0 {
			return qualificationSummary{}, errors.New("oracle value invalid")
		}
	}
	if anyNonzero(oracle.Gates) {
		return qualificationSummary{}, errors.New("correctness gate nonzero before run")
	}
	return qualification, nil
}

type driverHealth struct {
	BinarySHA256    string `json:"binary_sha256"`
	Service         string `json:"service"`
	Profile         string `json:"profile"`
	MaximumInflight int    `json:"maximum_inflight"`
}

type adapterHealth struct {
	SchemaVersion string `json:"schema_version"`
	Kind          string `json:"kind"`
	BinarySHA256  string `json:"binary_sha256"`
	ConfigSHA256  string `json:"config_sha256"`
}

func verifyAdapterHealth(plan runPlan) error {
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	binarySHA, err := exactFileSHA256(executable)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 2 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for _, expected := range []struct{ url, kind, configSHA string }{{plan.CollectorHealthURL, "collector", plan.CollectorConfigSHA256}, {plan.OracleHealthURL, "oracle", plan.OracleConfigSHA256}} {
		var health adapterHealth
		if !fetchStrictJSON(ctx, client, expected.url, &health) || health.SchemaVersion != "cap-single-pod-l2.adapter-health.v0.1" || health.Kind != expected.kind || health.BinarySHA256 != binarySHA || health.ConfigSHA256 != expected.configSHA {
			return fmt.Errorf("%s adapter identity mismatch", expected.kind)
		}
	}
	return nil
}

func verifyDriverHealth(plan runPlan) error {
	client := &http.Client{Timeout: 2 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var health driverHealth
	if !fetchStrictJSON(ctx, client, plan.DriverHealthURL, &health) {
		return errors.New("driver health unavailable")
	}
	if health.BinarySHA256 != plan.DriverBinarySHA256 || health.Service != plan.Service || health.Profile != plan.Profile || health.MaximumInflight < plan.MaximumInflight {
		return errors.New("driver identity or capacity mismatch")
	}
	return nil
}

func verifyManifest(plan runPlan) error {
	binding, err := loadBinding(plan.BindingPath)
	if err != nil {
		return err
	}
	if err := binding.validateForRun(plan); err != nil {
		return err
	}
	raw, err := os.ReadFile(plan.ManifestPath)
	if err != nil {
		return err
	}
	if containsForbiddenEvidence(raw) {
		return errors.New("manifest forbidden content")
	}
	var manifest map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err = decoder.Decode(&manifest); err != nil {
		return err
	}
	if manifest["schema_version"] != binding.ManifestSchemaVersion || manifest["task_id"] != binding.TaskID || manifest["run_id"] != plan.RunID || manifest["service"] != plan.Service || manifest["profile_name"] != plan.Profile || manifest["phase"] != plan.Phase || manifest["profile_version"] != binding.ProfileVersion {
		return errors.New("manifest run binding")
	}
	frozen, ok := manifest["frozen_identity"].(map[string]any)
	if !ok {
		return errors.New("manifest frozen identity")
	}
	for key, value := range binding.FrozenIdentity {
		if frozen[key] != value {
			return fmt.Errorf("manifest identity mismatch: %s", key)
		}
	}
	images, ok := manifest["images"].(map[string]any)
	if !ok || nestedString(images, "measured_service", "digest") != binding.FrozenIdentity["backend_image_digest"] || nestedString(images, "h5", "digest") != binding.FrozenIdentity["h5_image_digest"] {
		return errors.New("manifest runtime image binding")
	}
	harness, ok := manifest["harness_identity"].(map[string]any)
	if !ok || harness["design_review_head"] != designReviewHead {
		return errors.New("manifest design review binding")
	}
	for _, key := range []string{"harness_commit", "harness_review_head"} {
		value, valid := harness[key].(string)
		if !valid || len(value) != 40 || strings.Trim(value, "0123456789abcdef") != "" {
			return fmt.Errorf("manifest harness identity: %s", key)
		}
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	binarySHA, err := exactFileSHA256(executable)
	if err != nil {
		return err
	}
	if harness["harness_binary_sha256"] != binarySHA {
		return errors.New("manifest harness binary mismatch")
	}
	profileSHA, err := exactFileSHA256(plan.ProfilePath)
	if err != nil {
		return err
	}
	if manifest["profile_hash"] != profileSHA {
		return errors.New("manifest profile hash")
	}
	planned, ok := manifest["planned_load"].(map[string]any)
	if !ok || intFromJSON(planned["tier"]) != plan.Tier || intFromJSON(planned["ramp_seconds"]) != plan.RampSeconds || intFromJSON(planned["hold_seconds"]) != plan.DurationSeconds-plan.RampSeconds || !manifestTargetsEqual(planned["targets"], plan.Targets) {
		return errors.New("manifest planned load")
	}
	loadGenerator, ok := manifest["load_generator"].(map[string]any)
	if !ok || loadGenerator["binary_sha256"] != plan.DriverBinarySHA256 || loadGenerator["arrival_model"] != "open_loop_monotonic_scheduler" || floatFromJSON(loadGenerator["headroom_factor_proven"]) < 1.5 {
		return errors.New("manifest load generator")
	}
	contractHashes, ok := manifest["contract_hashes"].(map[string]any)
	if !ok {
		return errors.New("manifest contract hashes")
	}
	paths := map[string]string{"contract": plan.ContractPath, "profile": plan.ProfilePath, "stop_conditions": plan.StopConditionsPath, "required_metrics": plan.RequiredMetricsPath, "verdict_rules": plan.VerdictRulesPath, "environment_validity": plan.EnvironmentPath}
	for name, path := range paths {
		digest, e := exactFileSHA256(path)
		if e != nil {
			return e
		}
		if contractHashes[name] != digest {
			return fmt.Errorf("manifest contract mismatch: %s", name)
		}
	}
	return nil
}

func nestedString(parent map[string]any, object, field string) string {
	child, _ := parent[object].(map[string]any)
	value, _ := child[field].(string)
	return value
}

func intFromJSON(value any) int {
	number, ok := value.(float64)
	if !ok || number != float64(int(number)) {
		return -1
	}
	return int(number)
}
func floatFromJSON(value any) float64 { number, _ := value.(float64); return number }
func manifestTargetsEqual(value any, expected []target) bool {
	raw, err := json.Marshal(value)
	if err != nil {
		return false
	}
	var got []target
	if json.Unmarshal(raw, &got) != nil || len(got) != len(expected) {
		return false
	}
	for index := range got {
		if got[index] != expected[index] {
			return false
		}
	}
	return true
}

func verifyDesignIndex(directory string) error {
	indexPath := filepath.Join(directory, "design-index.json")
	raw, err := os.ReadFile(indexPath)
	if err != nil {
		return err
	}
	var index struct {
		Files map[string]struct {
			Bytes  int64  `json:"bytes"`
			SHA256 string `json:"sha256"`
		} `json:"files"`
	}
	if err = json.Unmarshal(raw, &index); err != nil {
		return err
	}
	if len(index.Files) != 8 {
		return errors.New("design index file count")
	}
	for name, item := range index.Files {
		if filepath.Base(name) != name {
			return errors.New("design index path")
		}
		path := filepath.Join(directory, name)
		info, e := os.Stat(path)
		if e != nil {
			return e
		}
		digest, e := exactFileSHA256(path)
		if e != nil {
			return e
		}
		if info.Size() != item.Bytes || digest != item.SHA256 {
			return fmt.Errorf("design index mismatch: %s", name)
		}
	}
	return nil
}

func fatal(message string) {
	message = strings.ReplaceAll(message, "\n", " ")
	fmt.Fprintln(os.Stderr, message)
	os.Exit(2)
}
