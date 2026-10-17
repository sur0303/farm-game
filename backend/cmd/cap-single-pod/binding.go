package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const designReviewHead = "25ba9675fbab1a1ea796e39a52fefcc16de351c3"

var trustedActiveBindingSHA256 = map[string]string{
	"R1": "e2c9b487f1077fbacd6ed08af8b315a11a37d3bed13ad1fb6c5d738c9bf8bfe5",
	// R2 pins dyn-friend-r72-capacity-20260813. Its service binaries are the ones
	// that carried G1/G2/G3 on dyn-friend-r70-formal-20260813: byte-identical, and
	// the two images differ only by the deployment scripts in the build context.
	// The capacity posture signs its own driver in, so no run under this binding
	// may be read as a product acceptance verdict.
	"R2": "f4d6910e509cecbba76de45d5b928b0b731f57cfecf07c8d8ad8ec2602a77db3",
}

type profileBinding struct {
	SchemaVersion                string            `json:"schema_version"`
	BindingID                    string            `json:"binding_id"`
	Status                       string            `json:"status"`
	TaskID                       string            `json:"task_id"`
	RunIDPattern                 string            `json:"run_id_pattern"`
	ManifestSchemaVersion        string            `json:"manifest_schema_version"`
	ManifestSchemaSHA256         string            `json:"manifest_schema_sha256"`
	ProfileSchemaVersion         string            `json:"profile_schema_version"`
	ProfileVersion               string            `json:"profile_version"`
	RequiredMetricsSchemaVersion string            `json:"required_metrics_schema_version"`
	StopConditionsSchemaVersion  string            `json:"stop_conditions_schema_version"`
	VerdictRulesSchemaVersion    string            `json:"verdict_rules_schema_version"`
	EnvironmentSchemaVersion     string            `json:"environment_schema_version"`
	FrozenIdentity               map[string]string `json:"frozen_identity"`
	sourceSHA256                 string
	sourceBasename               string
}

func loadBinding(path string) (profileBinding, error) {
	var binding profileBinding
	if err := loadJSONStrict(path, &binding); err != nil {
		return profileBinding{}, fmt.Errorf("binding: %w", err)
	}
	if binding.SchemaVersion != "cap-single-pod-l2.profile-binding.v0.1" {
		return profileBinding{}, errors.New("binding: schema version")
	}
	if binding.BindingID != "R1" && binding.BindingID != "R2" {
		return profileBinding{}, errors.New("binding: id")
	}
	if binding.Status != "ACTIVE" && binding.Status != "BLOCKED_WAITING_R2_FINAL_CANDIDATE" {
		return profileBinding{}, errors.New("binding: status")
	}
	if binding.TaskID == "" || binding.RunIDPattern == "" || binding.ManifestSchemaVersion == "" || !sha256Pattern.MatchString(binding.ManifestSchemaSHA256) ||
		binding.ProfileSchemaVersion == "" || binding.ProfileVersion == "" || binding.RequiredMetricsSchemaVersion == "" ||
		binding.StopConditionsSchemaVersion == "" || binding.VerdictRulesSchemaVersion == "" || binding.EnvironmentSchemaVersion == "" {
		return profileBinding{}, errors.New("binding: required field")
	}
	pattern, err := regexp.Compile(binding.RunIDPattern)
	if err != nil || pattern.String() != binding.RunIDPattern {
		return profileBinding{}, errors.New("binding: run ID pattern")
	}
	digest, err := exactFileSHA256(path)
	if err != nil {
		return profileBinding{}, fmt.Errorf("binding: identity: %w", err)
	}
	binding.sourceSHA256 = digest
	binding.sourceBasename = filepath.Base(path)
	if binding.Status == "ACTIVE" {
		for _, name := range []string{"backend_evidence_head", "backend_business_commit", "backend_image_digest", "h5_commit", "h5_image_digest", "model_layout"} {
			if binding.FrozenIdentity[name] == "" || binding.FrozenIdentity[name] == "UNBOUND" {
				return profileBinding{}, fmt.Errorf("binding: frozen identity %s", name)
			}
		}
	}
	return binding, nil
}

func (binding profileBinding) validateForRun(plan runPlan) error {
	if binding.Status != "ACTIVE" {
		return fmt.Errorf("binding %s is not active: %s", binding.BindingID, binding.Status)
	}
	trustedSHA, trusted := trustedActiveBindingSHA256[binding.BindingID]
	expectedBasename := "cap-single-pod-" + strings.ToLower(binding.BindingID) + "-binding.json"
	if !trusted || binding.sourceSHA256 != trustedSHA || binding.sourceBasename != expectedBasename {
		return fmt.Errorf("binding %s has no reviewed trust root", binding.BindingID)
	}
	matched, err := regexp.MatchString(binding.RunIDPattern, plan.RunID)
	if err != nil || !matched {
		return errors.New("binding: run ID does not match")
	}
	return nil
}

func verifyProfileBinding(binding profileBinding, profilePath string) error {
	raw, err := os.ReadFile(profilePath)
	if err != nil {
		return err
	}
	var profile map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := decoder.Decode(&profile); err != nil {
		return err
	}
	if profile["schema_version"] != binding.ProfileSchemaVersion || profile["profile_version"] != binding.ProfileVersion || profile["task_id"] != binding.TaskID {
		return errors.New("binding: profile identity")
	}
	frozen, ok := profile["frozen_identity"].(map[string]any)
	if !ok {
		return errors.New("binding: profile frozen identity")
	}
	for name, expected := range binding.FrozenIdentity {
		if frozen[name] != expected {
			return fmt.Errorf("binding: profile frozen identity mismatch: %s", name)
		}
	}
	return nil
}
