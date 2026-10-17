package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func r1ContractPath(name string) string {
	return filepath.Clean(filepath.Join("..", "..", "docs", "verification", "perf", "cap-single-pod", "CAP-SINGLE-POD-L2-R1", name))
}

func TestRequiredMetricsLoadCommonServiceAndGates(t *testing.T) {
	spec, err := loadRequiredSpec(r1ContractPath("required-metrics.json"), "worker", "cap-single-pod-l2-r1.required-metrics.v0.1")
	if err != nil {
		t.Fatal(err)
	}
	if len(spec.Metrics) < 40 || len(spec.Gates) != 15 || spec.Types["planned_arrivals_total"] != "counter" || spec.Types["worker_due_depth"] != "gauge" {
		t.Fatalf("spec=%+v", spec)
	}
	values := map[string]float64{}
	for _, definition := range spec.Metrics {
		values[definition.Name] = 0
	}
	if missing := missingMetricNames(values, requiredMetricNames(spec)); len(missing) != 0 {
		t.Fatalf("missing=%v", missing)
	}
	delete(values, "worker_due_depth")
	if missing := missingMetricNames(values, requiredMetricNames(spec)); len(missing) != 1 || missing[0] != "worker_due_depth" {
		t.Fatalf("missing=%v", missing)
	}
}

func TestRunPlanLoadsFrozenPoliciesAndRejectsDurationDrift(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	contractRoot := filepath.Join(root, "docs", "verification", "perf", "cap-single-pod", "CAP-SINGLE-POD-L2-R1")
	qualificationDir := filepath.Join(t.TempDir(), "qualification")
	if err := os.MkdirAll(qualificationDir, 0o700); err != nil {
		t.Fatal(err)
	}
	plan := runPlan{SchemaVersion: "cap-single-pod-l2.run-plan.v0.1", RunID: "capsingle-l2-r1-20260810T000000Z-1", Service: "gate", Profile: "login_ticket_wss_hello", Phase: "SMOKE", Tier: 0, DurationSeconds: 300, Targets: []target{{Kind: "arrivals_per_second", Value: 1}}, MaximumLaunchLagMS: 50, MaximumInflight: 1,
		DriverURL: "http://127.0.0.1:1/operation", DriverHealthURL: "http://127.0.0.1:1/health", SnapshotURL: "http://127.0.0.1:2/snapshot", CollectorHealthURL: "http://127.0.0.1:2/health", OracleURL: "http://127.0.0.1:3/oracle", OracleHealthURL: "http://127.0.0.1:3/health", CollectorConfigSHA256: "0000000000000000000000000000000000000000000000000000000000000000", OracleConfigSHA256: "0000000000000000000000000000000000000000000000000000000000000000", DriverBinarySHA256: string(make([]byte, 64)),
		ManifestPath: filepath.Join(contractRoot, "status.json"), ManifestSchemaPath: filepath.Join(contractRoot, "manifest.schema.json"), BindingPath: filepath.Join(root, "deploy", "load", "cap-single-pod-r1-binding.json"), ContractPath: filepath.Join(contractRoot, "contract.md"), RequiredMetricsPath: filepath.Join(contractRoot, "required-metrics.json"), StopConditionsPath: filepath.Join(contractRoot, "stop-conditions.json"), VerdictRulesPath: filepath.Join(contractRoot, "verdict-rules.json"), EnvironmentPath: filepath.Join(contractRoot, "environment-validity.json"), ProfilePath: filepath.Join(contractRoot, "profile.json"), LoadGeneratorQualificationPath: qualificationDir, SampleIntervalMS: 1000, RequestTimeoutMS: 1000}
	plan.DriverBinarySHA256 = "0000000000000000000000000000000000000000000000000000000000000000"
	path := filepath.Join(t.TempDir(), "plan.json")
	raw, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	if os.WriteFile(path, raw, 0o600) != nil {
		t.Fatal("write")
	}
	if _, err := loadPlan(path); err != nil {
		t.Fatal(err)
	}
	plan.DurationSeconds = 299
	raw, _ = json.Marshal(plan)
	_ = os.WriteFile(path, raw, 0o600)
	if _, err := loadPlan(path); err == nil {
		t.Fatal("duration drift accepted")
	}
}

func TestCheckedInR1BindingDigestAccepted(t *testing.T) {
	path := filepath.Clean(filepath.Join("..", "..", "deploy", "load", "cap-single-pod-r1-binding.json"))
	binding, err := loadBinding(path)
	if err != nil {
		t.Fatal(err)
	}
	if binding.sourceSHA256 != trustedActiveBindingSHA256["R1"] {
		t.Fatalf("sha=%s", binding.sourceSHA256)
	}
	if err := binding.validateForRun(runPlan{RunID: "capsingle-l2-r1-20260810T000000Z-1"}); err != nil {
		t.Fatal(err)
	}
}

func TestForgedActiveR2BindingRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "r2.json")
	raw, err := os.ReadFile(filepath.Clean(filepath.Join("..", "..", "deploy", "load", "cap-single-pod-r2-binding.blocked.json")))
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if json.Unmarshal(raw, &document) != nil {
		t.Fatal("json")
	}
	document["status"] = "ACTIVE"
	document["manifest_schema_version"] = "r2"
	document["profile_schema_version"] = "r2"
	document["profile_version"] = "r2"
	document["required_metrics_schema_version"] = "r2"
	document["stop_conditions_schema_version"] = "r2"
	document["verdict_rules_schema_version"] = "r2"
	document["environment_schema_version"] = "r2"
	frozen := document["frozen_identity"].(map[string]any)
	for key := range frozen {
		frozen[key] = "r2"
	}
	encoded, _ := json.Marshal(document)
	if os.WriteFile(path, encoded, 0o600) != nil {
		t.Fatal("write")
	}
	binding, err := loadBinding(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := binding.validateForRun(runPlan{RunID: "capsingle-l2-r2-20260810T000000Z-1"}); err == nil || !strings.Contains(err.Error(), "no reviewed trust root") {
		t.Fatalf("err=%v", err)
	}
}

func TestBlockedR2BindingCannotBeReplacedByExternalActiveBinding(t *testing.T) {
	path := filepath.Clean(filepath.Join("..", "..", "deploy", "load", "cap-single-pod-r1-binding.json"))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	copyPath := filepath.Join(t.TempDir(), "r1.json")
	raw = append(raw, ' ')
	if os.WriteFile(copyPath, raw, 0o600) != nil {
		t.Fatal("write")
	}
	binding, err := loadBinding(copyPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := binding.validateForRun(runPlan{RunID: "capsingle-l2-r1-20260810T000000Z-1"}); err == nil {
		t.Fatal("modified binding accepted")
	}
}

func TestRunPlanCannotOverrideExpectedBindingSHA(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plan.json")
	if os.WriteFile(path, []byte(`{"expected_binding_sha256":"`+strings.Repeat("a", 64)+`"}`), 0o600) != nil {
		t.Fatal("write")
	}
	var plan runPlan
	if err := loadJSONStrict(path, &plan); err == nil {
		t.Fatal("untrusted expected binding field accepted")
	}
}

func TestRejectsUnknownMetricExtra(t *testing.T) {
	required := []string{"service_cpu_percent"}
	values := map[string]float64{"service_cpu_percent": 1, "raw_extra": 2}
	if !unexpectedMetricSet(values, required) {
		t.Fatal("extra metric accepted")
	}
}
func TestRejectsUnknownCorrectnessGate(t *testing.T) {
	required := []string{"duplicate_grant_total"}
	if exactGateSet(map[string]int64{"duplicate_grant_total": 0, "unknown": 0}, required) {
		t.Fatal("extra gate accepted")
	}
}

func TestR2BindingIsBlockedAndR1BindingIsExact(t *testing.T) {
	r1, err := loadBinding(filepath.Clean(filepath.Join("..", "..", "deploy", "load", "cap-single-pod-r1-binding.json")))
	if err != nil {
		t.Fatal(err)
	}
	if r1.Status != "ACTIVE" || r1.FrozenIdentity["backend_evidence_head"] != "d1899ecab8d71248062ffd70eb4e42048e394a38" {
		t.Fatalf("r1=%+v", r1)
	}
	r2, err := loadBinding(filepath.Clean(filepath.Join("..", "..", "deploy", "load", "cap-single-pod-r2-binding.blocked.json")))
	if err != nil {
		t.Fatal(err)
	}
	if err := r2.validateForRun(runPlan{RunID: "capsingle-l2-r2-20260810T000000Z-1"}); err == nil {
		t.Fatal("blocked R2 binding accepted")
	}
}

func TestCheckedInR2BindingPinsTheCandidateThatCarriesTheGateVerdict(t *testing.T) {
	binding, err := loadBinding(filepath.Clean(filepath.Join("..", "..", "deploy", "load", "cap-single-pod-r2-binding.json")))
	if err != nil {
		t.Fatal(err)
	}
	if binding.sourceSHA256 != trustedActiveBindingSHA256["R2"] {
		t.Fatalf("sha=%s", binding.sourceSHA256)
	}
	if binding.Status != "ACTIVE" {
		t.Fatalf("status=%s", binding.Status)
	}
	// r75, not the r72 head R2 first bound. The r72 binaries never renewed a
	// subscription lease, so a capacity number taken on them is void by a hard
	// correctness gate no matter how the load behaves.
	if binding.FrozenIdentity["backend_evidence_head"] != "6f239b136e0943ec0dc30e3c92644974fc2b3454" {
		t.Fatalf("frozen=%+v", binding.FrozenIdentity)
	}
	if err := binding.validateForRun(runPlan{RunID: "capsingle-l2-r2-20260813T000000Z-1"}); err != nil {
		t.Fatal(err)
	}
	if err := binding.validateForRun(runPlan{RunID: "capsingle-l2-r1-20260813T000000Z-1"}); err == nil {
		t.Fatal("R2 binding accepted an R1 run ID")
	}
}

func TestR2ProfileMatchesItsBinding(t *testing.T) {
	binding, err := loadBinding(filepath.Clean(filepath.Join("..", "..", "deploy", "load", "cap-single-pod-r2-binding.json")))
	if err != nil {
		t.Fatal(err)
	}
	profile := filepath.Clean(filepath.Join("..", "..", "docs", "verification", "perf", "cap-single-pod", "CAP-SINGLE-POD-L2-R2", "profile.json"))
	if err := verifyProfileBinding(binding, profile); err != nil {
		t.Fatal(err)
	}
}

// support_service_primary_queue_items is compiled into mandatoryRuntimeMetricNames,
// so a sample without it is incomplete no matter what the contract says. The
// collector resolves a logical name through exactly one mapping, so the R2 binding
// has to name one producer for it and only one.
func TestR2CollectorTemplateGivesTheSupportQueueMetricOneProducer(t *testing.T) {
	raw, err := os.ReadFile(filepath.Clean(filepath.Join("..", "..", "deploy", "load", "cap-single-pod-r2-collector-adapter.template.json")))
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Sources  []prometheusSource `json:"sources"`
		Mappings []metricMapping    `json:"mappings"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	if err := validateMappings(document.Sources, document.Mappings); err != nil {
		t.Fatal(err)
	}
	owners := []metricMapping{}
	for _, mapping := range document.Mappings {
		if mapping.LogicalName == "support_service_primary_queue_items" {
			owners = append(owners, mapping)
		}
	}
	if len(owners) != 1 {
		t.Fatalf("owners=%d", len(owners))
	}
	if owners[0].Metric != "farm_workflow_backlog" || owners[0].Value != "value" || owners[0].Aggregate != "SINGLE" {
		t.Fatalf("mapping=%+v", owners[0])
	}
}

// The narrowed R2 metric set is only honest if every gate it asks the oracle for
// is one the oracle actually measures. A gate listed here but never exported
// would stall the run; a gate exported but not listed would go unread.
func TestR2RequiresExactlyTheGatesTheOracleMeasures(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", "..", "docs", "verification", "perf", "cap-single-pod", "CAP-SINGLE-POD-L2-R2"))
	spec, err := loadRequiredSpec(filepath.Join(root, "required-metrics.json"), "gate", "cap-single-pod-l2-r2.required-metrics.v0.1")
	if err != nil {
		t.Fatal(err)
	}
	unimplemented := map[string]bool{
		"h5_pending_journal_residual": true, "silent_drop_total": true,
		"invalid_order_total": true, "wrong_scope_dispatch_total": true,
	}
	if len(spec.Gates) != 11 {
		t.Fatalf("gates=%d", len(spec.Gates))
	}
	for _, gate := range spec.Gates {
		if unimplemented[gate] {
			t.Fatalf("required an uninstrumented gate: %s", gate)
		}
	}
}
