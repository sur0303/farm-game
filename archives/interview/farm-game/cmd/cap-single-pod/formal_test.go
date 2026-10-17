package main

import "testing"

func healthyFormalInputs() []formalInput {
	phases := []string{"SAFE_TIER_CANDIDATE", "FORMAL_1", "FORMAL_2", "FORMAL_3"}
	inputs := make([]formalInput, 0, len(phases))
	hash := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	commit := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	image := "sha256:" + hash
	cgroup := func(name string) map[string]any {
		return map[string]any{"name": name, "cpu_quota": 4.0, "memory_limit_bytes": float64(4096), "pids_limit": float64(128), "swap_disabled": true, "cgroup_path_hash": hash, "container_id_hash": hash}
	}
	for index, phase := range phases {
		adapter := map[string]any{"binary_sha256": hash, "binding_sha256": hash, "collector_config_sha256": hash, "oracle_config_sha256": hash}
		qualification := map[string]any{"qualification_run_id": "qual", "qualification_evidence_sha256": hash, "qualified_tier": float64(4), "requested_tier": float64(3), "headroom_factor_actual": 1.5}
		runID := "run-" + phase
		targets := []target{{Kind: "frames_per_second", Value: 100}}
		result := capacityResultDocument{BindingID: "R1", RunID: runID, Service: "gate", Profile: "steady_ping_pong", Phase: phase, Tier: 3, Duration: 100, Targets: targets, PhaseStatus: PhaseStatusPass, PhaseDecision: phasePassDecision(phase), Actual: plannedActual{Planned: 1000, Launched: 1000, Completed: int64(990 + index), ActualToPlannedRatio: .99, LatencyMS: map[string]float64{"p99": 10 + float64(index)/10}}, Metrics: map[string]float64{"service_rss_bytes_max": 1000 + float64(index)}, QueueSlope: slopeResult{Samples: 60, SlopePerSecond: 0}, AdapterIdentity: adapter, LoadGeneratorQualification: qualification}
		manifest := map[string]any{"task_id": "CAP-SINGLE-POD-L2-R1", "run_id": runID, "service": "gate", "profile_name": "steady_ping_pong", "phase": phase, "profile_version": "v", "profile_hash": hash, "contract_hashes": map[string]any{"contract": hash}, "fixture": map[string]any{"fixture_content_hash": hash}, "random_seed": float64(1), "planned_load": map[string]any{"tier": float64(3), "targets": []any{map[string]any{"kind": "frames_per_second", "value": float64(100)}}},
			"frozen_identity": map[string]any{"backend_evidence_head": commit, "backend_business_commit": commit, "backend_image_digest": image, "h5_commit": commit, "h5_image_digest": image, "model_layout": "r1"}, "harness_identity": map[string]any{"harness_commit": commit, "harness_binary_sha256": hash, "design_review_head": commit, "harness_review_head": commit}, "machine": map[string]any{"spec_hash": hash}, "load_generator": map[string]any{"binary_sha256": hash, "qualification_evidence_hash": hash, "arrival_model": "open_loop_monotonic_scheduler"}, "topology": map[string]any{"route_snapshot_hash": hash, "lease_profile_hash": hash, "server_config_hash": hash, "measured_service_replicas": float64(1), "support_service_replicas": map[string]any{"player": float64(1)}}, "redis": map[string]any{"profile_version": "redis-physical-v1", "config_hash": hash, "loopback": true, "ephemeral": true}, "images": map[string]any{"measured_service": map[string]any{"digest": image}, "h5": map[string]any{"digest": image}, "redis": map[string]any{"digest": image}, "support_services": []any{}}, "cgroups": map[string]any{"measured_service": cgroup("service"), "load_generator": cgroup("driver"), "redis": cgroup("redis"), "support_services": []any{}}}
		correctness := map[string]any{"status": "PASS", "observation_complete": true, "gates": map[string]any{"duplicate_grant_total": float64(0)}}
		environment := map[string]any{"valid": true, "observability_complete": true, "load_generator_qualified": true, "adapter_identity": adapter}
		inputs = append(inputs, formalInput{Result: result, Manifest: manifest, Correctness: correctness, Environment: environment})
	}
	return inputs
}

func TestOnlyFormalAggregateCanPassSafeTier(t *testing.T) {
	verdict, err := aggregateFormal(healthyFormalInputs())
	if err != nil {
		t.Fatal(err)
	}
	if verdict["verdict"] != VerdictPassSafeTier || verdict["publish_authorized"] != false || verdict["baseline_eligible"] != false {
		t.Fatalf("verdict=%v", verdict)
	}
}

func TestFormalAggregateRejectsIdentityAndCVDrift(t *testing.T) {
	inputs := healthyFormalInputs()
	inputs[2].Result.Tier++
	if _, err := aggregateFormal(inputs); err == nil {
		t.Fatal("tier drift accepted")
	}
	inputs = healthyFormalInputs()
	inputs[3].Result.Actual.LatencyMS["p99"] = 100
	if _, err := aggregateFormal(inputs); err == nil {
		t.Fatal("p99 CV drift accepted")
	}
}

func TestFormalRejectsWrongCandidateVerdict(t *testing.T) {
	inputs := healthyFormalInputs()
	inputs[0].Result.PhaseDecision = "PASS_FORMAL_ROUND"
	if _, err := aggregateFormal(inputs); err == nil {
		t.Fatal("wrong candidate decision accepted")
	}
}
func TestFormalRejectsWrongRoundVerdict(t *testing.T) {
	inputs := healthyFormalInputs()
	inputs[2].Result.PhaseDecision = "PASS_SAFE_TIER_CANDIDATE"
	if _, err := aggregateFormal(inputs); err == nil {
		t.Fatal("wrong formal decision accepted")
	}
}
func TestFormalRejectsResultManifestMismatch(t *testing.T) {
	inputs := healthyFormalInputs()
	inputs[1].Manifest["service"] = "player"
	if _, err := aggregateFormal(inputs); err == nil {
		t.Fatal("result/manifest mismatch accepted")
	}
}
func TestFormalRejectsServerConfigDrift(t *testing.T) {
	assertFormalNestedDriftRejected(t, "topology", "server_config_hash")
}
func TestFormalRejectsRouteSnapshotDrift(t *testing.T) {
	assertFormalNestedDriftRejected(t, "topology", "route_snapshot_hash")
}
func TestFormalRejectsLeaseProfileDrift(t *testing.T) {
	assertFormalNestedDriftRejected(t, "topology", "lease_profile_hash")
}
func TestFormalRejectsRedisConfigDrift(t *testing.T) {
	assertFormalNestedDriftRejected(t, "redis", "config_hash")
}
func TestFormalRejectsImageDigestDrift(t *testing.T) {
	inputs := healthyFormalInputs()
	inputs[3].Manifest["images"].(map[string]any)["measured_service"].(map[string]any)["digest"] = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if _, err := aggregateFormal(inputs); err == nil {
		t.Fatal("image drift accepted")
	}
}
func TestFormalRejectsBusinessCommitDrift(t *testing.T) {
	inputs := healthyFormalInputs()
	inputs[1].Manifest["frozen_identity"].(map[string]any)["backend_business_commit"] = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if _, err := aggregateFormal(inputs); err == nil {
		t.Fatal("business commit drift accepted")
	}
}
func TestFormalRejectsAdapterDigestDrift(t *testing.T) {
	inputs := healthyFormalInputs()
	replacement := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	inputs[2].Result.AdapterIdentity["collector_config_sha256"] = replacement
	inputs[2].Environment["adapter_identity"].(map[string]any)["collector_config_sha256"] = replacement
	if _, err := aggregateFormal(inputs); err == nil {
		t.Fatal("adapter drift accepted")
	}
}

func assertFormalNestedDriftRejected(t *testing.T, section, key string) {
	t.Helper()
	inputs := healthyFormalInputs()
	inputs[2].Manifest[section].(map[string]any)[key] = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if _, err := aggregateFormal(inputs); err == nil {
		t.Fatalf("%s.%s drift accepted", section, key)
	}
}

func TestFormalRoundCannotSelfClaimSafeTier(t *testing.T) {
	plan := healthyPlan()
	plan.Phase = "FORMAL_3"
	result := evaluateRun(plan, healthyActual(), nil, healthySamples(), nil, true, true, "")
	if result.Verdict == VerdictPassSafeTier || result.PhaseDecision != "PASS_FORMAL_ROUND" {
		t.Fatalf("result=%+v", result)
	}
}
