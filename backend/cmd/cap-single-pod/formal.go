package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
)

type capacityResultDocument struct {
	BindingID                  string             `json:"binding_id"`
	RunID                      string             `json:"run_id"`
	Service                    string             `json:"service"`
	Profile                    string             `json:"profile"`
	Phase                      string             `json:"phase"`
	Tier                       int                `json:"tier"`
	Duration                   int                `json:"duration_seconds"`
	Targets                    []target           `json:"targets"`
	PhaseStatus                string             `json:"phase_status"`
	PhaseDecision              string             `json:"phase_decision"`
	Verdict                    string             `json:"verdict,omitempty"`
	Actual                     plannedActual      `json:"actual"`
	Metrics                    map[string]float64 `json:"metrics"`
	QueueSlope                 slopeResult        `json:"queue_slope"`
	AdapterIdentity            map[string]any     `json:"adapter_identity"`
	LoadGeneratorQualification map[string]any     `json:"load_generator_qualification"`
	SafeClaimed                bool               `json:"safe_constant_claimed"`
}

type formalInput struct {
	Directory   string
	Result      capacityResultDocument
	Manifest    map[string]any
	Correctness map[string]any
	Environment map[string]any
}

func finalizeFormalCommand(arguments []string) {
	flags := flag.NewFlagSet("finalize-formal", flag.ExitOnError)
	candidatePath := flags.String("candidate", "", "SAFE_TIER_CANDIDATE evidence directory")
	formal1Path := flags.String("formal-1", "", "FORMAL_1 evidence directory")
	formal2Path := flags.String("formal-2", "", "FORMAL_2 evidence directory")
	formal3Path := flags.String("formal-3", "", "FORMAL_3 evidence directory")
	output := flags.String("out", "", "new aggregate evidence directory")
	_ = flags.Parse(arguments)
	for _, path := range []string{*candidatePath, *formal1Path, *formal2Path, *formal3Path, *output} {
		if path == "" || !filepath.IsAbs(path) {
			fatal("finalize-formal requires absolute candidate, formal and out paths")
		}
	}
	inputs := make([]formalInput, 0, 4)
	for _, path := range []string{*candidatePath, *formal1Path, *formal2Path, *formal3Path} {
		value, err := loadFormalInput(path)
		if err != nil {
			fatal(err.Error())
		}
		inputs = append(inputs, value)
	}
	verdict, err := aggregateFormal(inputs)
	if err != nil {
		fatal(err.Error())
	}
	if err := prepareOutput(*output); err != nil {
		fatal(err.Error())
	}
	if err := writeJSONExclusive(filepath.Join(*output, "formal-safe-tier-verdict.json"), verdict); err != nil {
		fatal(err.Error())
	}
	if err := writeExclusive(filepath.Join(*output, "formal-safe-tier-report.md"), []byte(renderFormalReport(verdict))); err != nil {
		fatal(err.Error())
	}
	if err := writeAggregateIndex(*output); err != nil {
		fatal(err.Error())
	}
	fmt.Printf("CAP_SINGLE_POD_FORMAL_RESULT=%s OUT=%s\n", VerdictPassSafeTier, *output)
}

func loadFormalInput(directory string) (formalInput, error) {
	if err := verifyRunEvidenceIndex(directory); err != nil {
		return formalInput{}, err
	}
	var result capacityResultDocument
	resultRaw, err := os.ReadFile(filepath.Join(directory, "service-capacity-result.json"))
	if err != nil {
		return formalInput{}, err
	}
	if json.Unmarshal(resultRaw, &result) != nil {
		return formalInput{}, errors.New("formal result JSON")
	}
	raw, err := os.ReadFile(filepath.Join(directory, "run-manifest.json"))
	if err != nil {
		return formalInput{}, err
	}
	var manifest map[string]any
	if json.Unmarshal(raw, &manifest) != nil {
		return formalInput{}, errors.New("formal manifest JSON")
	}
	correctness, err := readManifestObject(filepath.Join(directory, "correctness-verdict.json"))
	if err != nil {
		return formalInput{}, errors.New("formal correctness JSON")
	}
	environment, err := readManifestObject(filepath.Join(directory, "environment-verdict.json"))
	if err != nil {
		return formalInput{}, errors.New("formal environment JSON")
	}
	return formalInput{Directory: directory, Result: result, Manifest: manifest, Correctness: correctness, Environment: environment}, nil
}

func aggregateFormal(inputs []formalInput) (map[string]any, error) {
	if len(inputs) != 4 {
		return nil, errors.New("formal aggregate requires candidate plus three rounds")
	}
	expectedPhases := []string{"SAFE_TIER_CANDIDATE", "FORMAL_1", "FORMAL_2", "FORMAL_3"}
	expectedDecisions := []string{"PASS_SAFE_TIER_CANDIDATE", "PASS_FORMAL_ROUND", "PASS_FORMAL_ROUND", "PASS_FORMAL_ROUND"}
	base := inputs[0]
	baseIdentity, err := formalManifestIdentity(base)
	if err != nil {
		return nil, err
	}
	for index, input := range inputs {
		result := input.Result
		if result.Phase != expectedPhases[index] || result.PhaseStatus != PhaseStatusPass || result.PhaseDecision != expectedDecisions[index] || result.Verdict != "" || result.SafeClaimed {
			return nil, fmt.Errorf("formal input phase result invalid: %s", expectedPhases[index])
		}
		if err := validateFormalResultManifest(input); err != nil {
			return nil, err
		}
		if result.BindingID != base.Result.BindingID || result.Service != base.Result.Service || result.Profile != base.Result.Profile || result.Tier != base.Result.Tier || !reflect.DeepEqual(result.Targets, base.Result.Targets) {
			return nil, errors.New("formal identity/profile/tier mismatch")
		}
		identity, identityErr := formalManifestIdentity(input)
		if identityErr != nil || !reflect.DeepEqual(identity, baseIdentity) {
			return nil, errors.New("formal manifest identity mismatch")
		}
	}
	if base.Result.Actual.ActualToPlannedRatio < .95 {
		return nil, errors.New("formal candidate delivered load invalid")
	}
	throughputs, p99s, rssValues := []float64{}, []float64{}, []float64{}
	for _, input := range inputs[1:] {
		if input.Result.Duration <= 0 || input.Result.Actual.ActualToPlannedRatio < .98 {
			return nil, errors.New("formal delivered load invalid")
		}
		throughputs = append(throughputs, float64(input.Result.Actual.Completed)/float64(input.Result.Duration))
		p99s = append(p99s, input.Result.Actual.LatencyMS["p99"])
		rssValues = append(rssValues, input.Result.Metrics["service_rss_bytes_max"])
	}
	throughputCV, p99CV, rssCV := coefficientOfVariation(throughputs), coefficientOfVariation(p99s), coefficientOfVariation(rssValues)
	if throughputCV > .1 || p99CV > .1 || rssCV > .1 {
		return nil, errors.New("formal cross-round CV exceeds 0.10")
	}
	runIDs := []string{inputs[1].Result.RunID, inputs[2].Result.RunID, inputs[3].Result.RunID}
	return map[string]any{
		"schema_version": "cap-single-pod-l2.formal-safe-tier-verdict.v0.1",
		"binding_id":     base.Result.BindingID, "service": base.Result.Service, "profile": base.Result.Profile,
		"tier": base.Result.Tier, "targets": base.Result.Targets, "candidate_run_id": base.Result.RunID, "formal_run_ids": runIDs,
		"verdict": VerdictPassSafeTier, "phase_status": PhaseStatusPass, "safe_constant_claimed": true,
		"publish_authorized": false, "independent_review_required": true,
		"cross_round":       map[string]float64{"actual_throughput_cv": throughputCV, "p99_latency_cv": p99CV, "peak_rss_cv": rssCV},
		"baseline_eligible": false, "capacity_claim": "NONE", "supports_30m_dau": false,
	}, nil
}

func validateFormalResultManifest(input formalInput) error {
	result, manifest := input.Result, input.Manifest
	expectedTask := "CAP-SINGLE-POD-L2-" + result.BindingID
	if result.RunID == "" || manifest["task_id"] != expectedTask || manifest["run_id"] != result.RunID || manifest["service"] != result.Service || manifest["profile_name"] != result.Profile || manifest["phase"] != result.Phase {
		return errors.New("formal result manifest mismatch")
	}
	planned, ok := manifest["planned_load"].(map[string]any)
	if !ok || intFromJSON(planned["tier"]) != result.Tier || !manifestTargetsEqual(planned["targets"], result.Targets) {
		return errors.New("formal result manifest mismatch")
	}
	if input.Correctness["status"] != "PASS" || input.Correctness["observation_complete"] != true {
		return errors.New("formal correctness attachment invalid")
	}
	gates, ok := input.Correctness["gates"].(map[string]any)
	if !ok || len(gates) == 0 {
		return errors.New("formal correctness attachment invalid")
	}
	for _, value := range gates {
		number, valid := value.(float64)
		if !valid || number != 0 {
			return errors.New("formal correctness attachment invalid")
		}
	}
	if input.Environment["valid"] != true || input.Environment["observability_complete"] != true || input.Environment["load_generator_qualified"] != true {
		return errors.New("formal environment attachment invalid")
	}
	environmentAdapter, ok := input.Environment["adapter_identity"].(map[string]any)
	if !ok || !reflect.DeepEqual(environmentAdapter, result.AdapterIdentity) {
		return errors.New("formal adapter identity mismatch")
	}
	for _, key := range []string{"binary_sha256", "binding_sha256", "collector_config_sha256", "oracle_config_sha256"} {
		value, valid := result.AdapterIdentity[key].(string)
		if !valid || !sha256Pattern.MatchString(value) {
			return errors.New("formal adapter identity mismatch")
		}
	}
	if result.QueueSlope.Samples < 55 || result.QueueSlope.Positive || result.QueueSlope.SlopePerSecond > 0 {
		return errors.New("formal queue slope invalid")
	}
	return nil
}

func formalManifestIdentity(input formalInput) (map[string]any, error) {
	manifest := input.Manifest
	identity := map[string]any{}
	for _, key := range []string{"task_id", "service", "profile_name", "profile_version", "profile_hash", "contract_hashes", "fixture", "random_seed"} {
		value, found := manifest[key]
		if !found {
			return nil, errors.New("formal manifest identity incomplete")
		}
		identity[key] = value
	}
	if profileHash, ok := manifest["profile_hash"].(string); !ok || !sha256Pattern.MatchString(profileHash) {
		return nil, errors.New("formal manifest identity incomplete")
	}
	frozen, ok := manifest["frozen_identity"].(map[string]any)
	if !ok {
		return nil, errors.New("formal manifest identity incomplete")
	}
	for _, key := range []string{"backend_evidence_head", "backend_business_commit", "h5_commit"} {
		value, valid := frozen[key].(string)
		if !valid || len(value) != 40 {
			return nil, errors.New("formal manifest identity incomplete")
		}
	}
	for _, key := range []string{"backend_image_digest", "h5_image_digest"} {
		value, valid := frozen[key].(string)
		if !valid || !validImageDigest(value) {
			return nil, errors.New("formal manifest identity incomplete")
		}
	}
	if value, valid := frozen["model_layout"].(string); !valid || value == "" {
		return nil, errors.New("formal manifest identity incomplete")
	}
	identity["frozen_identity"] = frozen
	harness, ok := manifest["harness_identity"].(map[string]any)
	if !ok {
		return nil, errors.New("formal manifest identity incomplete")
	}
	for _, key := range []string{"harness_commit", "design_review_head", "harness_review_head"} {
		value, valid := harness[key].(string)
		if !valid || len(value) != 40 {
			return nil, errors.New("formal manifest identity incomplete")
		}
	}
	if value, valid := harness["harness_binary_sha256"].(string); !valid || !sha256Pattern.MatchString(value) {
		return nil, errors.New("formal manifest identity incomplete")
	}
	identity["harness_identity"] = harness
	machine, ok := manifest["machine"].(map[string]any)
	if !ok {
		return nil, errors.New("formal manifest identity incomplete")
	}
	specHash, ok := machine["spec_hash"].(string)
	if !ok || !sha256Pattern.MatchString(specHash) {
		return nil, errors.New("formal manifest identity incomplete")
	}
	identity["machine_spec_hash"] = specHash
	load, ok := manifest["load_generator"].(map[string]any)
	if !ok {
		return nil, errors.New("formal manifest identity incomplete")
	}
	identity["load_generator"] = selectIdentityFields(load, []string{"binary_sha256", "qualification_evidence_hash", "arrival_model"})
	if identity["load_generator"].(map[string]any)["__incomplete__"] == true || !selectedSHAFieldsValid(load, []string{"binary_sha256", "qualification_evidence_hash"}) {
		return nil, errors.New("formal manifest identity incomplete")
	}
	topology, ok := manifest["topology"].(map[string]any)
	if !ok {
		return nil, errors.New("formal manifest identity incomplete")
	}
	identity["topology"] = selectIdentityFields(topology, []string{"route_snapshot_hash", "lease_profile_hash", "server_config_hash", "measured_service_replicas", "support_service_replicas"})
	if identity["topology"].(map[string]any)["__incomplete__"] == true || !selectedSHAFieldsValid(topology, []string{"route_snapshot_hash", "lease_profile_hash", "server_config_hash"}) {
		return nil, errors.New("formal manifest identity incomplete")
	}
	redis, ok := manifest["redis"].(map[string]any)
	if !ok {
		return nil, errors.New("formal manifest identity incomplete")
	}
	identity["redis"] = selectIdentityFields(redis, []string{"profile_version", "config_hash", "loopback", "ephemeral"})
	if identity["redis"].(map[string]any)["__incomplete__"] == true || !selectedSHAFieldsValid(redis, []string{"config_hash"}) {
		return nil, errors.New("formal manifest identity incomplete")
	}
	images, err := formalImageDigests(manifest["images"])
	if err != nil {
		return nil, err
	}
	identity["image_digests"] = images
	cgroups, err := formalCgroupLimits(manifest["cgroups"])
	if err != nil {
		return nil, err
	}
	identity["cgroup_limits"] = cgroups
	identity["adapter_identity"] = input.Result.AdapterIdentity
	identity["load_generator_qualification"] = input.Result.LoadGeneratorQualification
	return identity, nil
}

func selectedSHAFieldsValid(source map[string]any, keys []string) bool {
	for _, key := range keys {
		value, ok := source[key].(string)
		if !ok || !sha256Pattern.MatchString(value) {
			return false
		}
	}
	return true
}
func validImageDigest(value string) bool {
	return len(value) == 71 && value[:7] == "sha256:" && sha256Pattern.MatchString(value[7:])
}

func selectIdentityFields(source map[string]any, keys []string) map[string]any {
	result := map[string]any{}
	for _, key := range keys {
		value, found := source[key]
		if !found {
			return map[string]any{"__incomplete__": true}
		}
		result[key] = value
	}
	return result
}

func formalImageDigests(value any) (map[string]any, error) {
	images, ok := value.(map[string]any)
	if !ok {
		return nil, errors.New("formal image identity incomplete")
	}
	result := map[string]any{}
	for _, name := range []string{"measured_service", "h5", "redis"} {
		item, valid := images[name].(map[string]any)
		if !valid {
			return nil, errors.New("formal image identity incomplete")
		}
		digest, valid := item["digest"].(string)
		if !valid || !validImageDigest(digest) {
			return nil, errors.New("formal image identity incomplete")
		}
		result[name] = digest
	}
	support, ok := images["support_services"].([]any)
	if ok {
		digests := []string{}
		for _, raw := range support {
			item, valid := raw.(map[string]any)
			if !valid {
				return nil, errors.New("formal image identity incomplete")
			}
			digest, valid := item["digest"].(string)
			if !valid || !validImageDigest(digest) {
				return nil, errors.New("formal image identity incomplete")
			}
			digests = append(digests, digest)
		}
		result["support_services"] = digests
	}
	return result, nil
}

func formalCgroupLimits(value any) (map[string]any, error) {
	groups, ok := value.(map[string]any)
	if !ok {
		return nil, errors.New("formal cgroup identity incomplete")
	}
	result := map[string]any{}
	extract := func(raw any) (map[string]any, error) {
		item, valid := raw.(map[string]any)
		if !valid {
			return nil, errors.New("formal cgroup identity incomplete")
		}
		selected := selectIdentityFields(item, []string{"name", "cpu_quota", "memory_limit_bytes", "pids_limit", "swap_disabled"})
		if selected["__incomplete__"] == true {
			return nil, errors.New("formal cgroup identity incomplete")
		}
		return selected, nil
	}
	for _, name := range []string{"measured_service", "load_generator", "redis"} {
		item, err := extract(groups[name])
		if err != nil {
			return nil, err
		}
		result[name] = item
	}
	support, ok := groups["support_services"].([]any)
	if !ok {
		return nil, errors.New("formal cgroup identity incomplete")
	}
	items := []map[string]any{}
	for _, raw := range support {
		item, err := extract(raw)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	result["support_services"] = items
	return result, nil
}

func coefficientOfVariation(values []float64) float64 {
	if len(values) == 0 {
		return math.Inf(1)
	}
	mean := 0.0
	for _, value := range values {
		mean += value
	}
	mean /= float64(len(values))
	if mean <= 0 {
		return math.Inf(1)
	}
	variance := 0.0
	for _, value := range values {
		difference := value - mean
		variance += difference * difference
	}
	variance /= float64(len(values))
	return math.Sqrt(variance) / mean
}

func renderFormalReport(verdict map[string]any) string {
	return fmt.Sprintf("# CAP-SINGLE-POD-L2 formal safe-tier result\n\n- Binding: `%s`\n- Service/Profile: `%s / %s`\n- Verdict: **%s**\n- Independent review required: `true`\n- `baseline_eligible=false`\n- `capacity_claim=NONE`\n- `supports_30m_dau=false`\n\nThis aggregate does not authorize publication until independent review passes.\n",
		verdict["binding_id"], verdict["service"], verdict["profile"], verdict["verdict"])
}

func writeAggregateIndex(output string) error {
	files := []string{"formal-safe-tier-report.md", "formal-safe-tier-verdict.json"}
	entries := []map[string]any{}
	for _, name := range files {
		raw, err := os.ReadFile(filepath.Join(output, name))
		if err != nil {
			return err
		}
		if containsForbiddenEvidence(raw) {
			return fmt.Errorf("forbidden aggregate evidence: %s", name)
		}
		digest := sha256Bytes(raw)
		entries = append(entries, map[string]any{"path": name, "bytes": len(raw), "sha256": digest})
	}
	return writeJSONExclusive(filepath.Join(output, "evidence-hash-index.json"), map[string]any{"schema_version": "cap-single-pod-l2.aggregate-index.v0.1", "files": entries})
}

func verifyRunEvidenceIndex(directory string) error {
	raw, err := os.ReadFile(filepath.Join(directory, "evidence-hash-index.json"))
	if err != nil {
		return err
	}
	var index struct {
		Complete bool `json:"complete"`
		Files    []struct {
			Path   string `json:"path"`
			Bytes  int    `json:"bytes"`
			SHA256 string `json:"sha256"`
		} `json:"files"`
	}
	if json.Unmarshal(raw, &index) != nil {
		return errors.New("evidence index JSON")
	}
	if !index.Complete {
		return errors.New("evidence index incomplete")
	}
	if len(index.Files) != len(requiredEvidenceFiles) {
		return errors.New("evidence index count")
	}
	seen := map[string]bool{}
	required := map[string]bool{}
	for _, name := range requiredEvidenceFiles {
		required[name] = true
	}
	for _, item := range index.Files {
		if !required[item.Path] || seen[item.Path] || filepath.Base(item.Path) != item.Path {
			return errors.New("evidence index path")
		}
		data, readErr := os.ReadFile(filepath.Join(directory, item.Path))
		if readErr != nil {
			return readErr
		}
		if len(data) != item.Bytes || sha256Bytes(data) != item.SHA256 {
			return fmt.Errorf("evidence index mismatch: %s", item.Path)
		}
		seen[item.Path] = true
	}
	return nil
}

func sha256Bytes(raw []byte) string {
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}
