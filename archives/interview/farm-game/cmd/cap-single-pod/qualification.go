package main

import (
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const minimumLoadGeneratorQualificationSamples = 60

type qualificationSummary struct {
	QualificationRunID          string  `json:"qualification_run_id"`
	QualificationEvidenceSHA256 string  `json:"qualification_evidence_sha256"`
	QualifiedTier               int     `json:"qualified_tier"`
	RequestedTier               int     `json:"requested_tier"`
	HeadroomFactorActual        float64 `json:"headroom_factor_actual"`
}

func verifyLoadGeneratorQualification(plan runPlan) (qualificationSummary, error) {
	if !phaseRequiresQualificationEvidence(plan.Phase) {
		return qualificationSummary{}, nil
	}
	directory := strings.TrimSpace(plan.LoadGeneratorQualificationPath)
	if directory == "" || !filepath.IsAbs(directory) {
		return qualificationSummary{}, errors.New("qualification evidence required")
	}
	if err := verifyRunEvidenceIndex(directory); err != nil {
		return qualificationSummary{}, errors.New("qualification evidence incomplete")
	}
	indexPath := filepath.Join(directory, "evidence-hash-index.json")
	indexSHA, err := exactFileSHA256(indexPath)
	if err != nil {
		return qualificationSummary{}, errors.New("qualification evidence incomplete")
	}
	currentManifest, err := readManifestObject(plan.ManifestPath)
	if err != nil {
		return qualificationSummary{}, errors.New("qualification evidence hash mismatch")
	}
	loadGenerator, _ := currentManifest["load_generator"].(map[string]any)
	claimedHash, _ := loadGenerator["qualification_evidence_hash"].(string)
	if !sha256Pattern.MatchString(claimedHash) || claimedHash != indexSHA {
		return qualificationSummary{}, errors.New("qualification evidence hash mismatch")
	}
	resultRaw, err := os.ReadFile(filepath.Join(directory, "service-capacity-result.json"))
	if err != nil {
		return qualificationSummary{}, errors.New("qualification phase result invalid")
	}
	var result capacityResultDocument
	if json.Unmarshal(resultRaw, &result) != nil {
		return qualificationSummary{}, errors.New("qualification phase result invalid")
	}
	if result.Phase != "LOAD_GENERATOR_QUALIFICATION" || result.PhaseStatus != PhaseStatusPass ||
		result.PhaseDecision != "PASS_LOAD_GENERATOR_QUALIFICATION" || result.Verdict != "" || result.SafeClaimed {
		return qualificationSummary{}, errors.New("qualification phase result invalid")
	}
	binding, err := loadBinding(plan.BindingPath)
	if err != nil {
		return qualificationSummary{}, errors.New("qualification identity mismatch")
	}
	if result.BindingID != binding.BindingID || result.Service != plan.Service || result.Profile != plan.Profile || result.AdapterIdentity["binding_sha256"] != binding.sourceSHA256 {
		return qualificationSummary{}, errors.New("qualification identity mismatch")
	}
	qualificationManifest, err := readManifestObject(filepath.Join(directory, "run-manifest.json"))
	if err != nil {
		return qualificationSummary{}, errors.New("qualification identity mismatch")
	}
	if !equalJSONValue(qualificationManifest["frozen_identity"], currentManifest["frozen_identity"]) {
		return qualificationSummary{}, errors.New("qualification identity mismatch")
	}
	qualLoad, _ := qualificationManifest["load_generator"].(map[string]any)
	qualDriver, _ := qualLoad["binary_sha256"].(string)
	if qualDriver != plan.DriverBinarySHA256 {
		return qualificationSummary{}, errors.New("qualification driver mismatch")
	}
	profileSHA, err := exactFileSHA256(plan.ProfilePath)
	if err != nil {
		return qualificationSummary{}, errors.New("qualification profile mismatch")
	}
	qualProfileHash, _ := qualificationManifest["profile_hash"].(string)
	if qualProfileHash != profileSHA {
		return qualificationSummary{}, errors.New("qualification profile mismatch")
	}
	finishedAt, err := parseStrictRFC3339(qualificationManifest["finished_at"])
	if err != nil {
		return qualificationSummary{}, errors.New("qualification timestamp invalid")
	}
	startedAt, err := parseStrictRFC3339(currentManifest["started_at"])
	if err != nil {
		return qualificationSummary{}, errors.New("qualification timestamp invalid")
	}
	now := time.Now().UTC()
	if !finishedAt.Before(startedAt) || finishedAt.After(now) {
		return qualificationSummary{}, errors.New("qualification timestamp invalid")
	}
	driverCPU, hasDriverCPU := result.Metrics["driver_cpu_percent_max"]
	arrivalLagP99, hasArrivalLag := result.Actual.ArrivalLagMS["p99"]
	if result.Duration < minimumLoadGeneratorQualificationSamples || result.QueueSlope.Samples < minimumLoadGeneratorQualificationSamples ||
		result.Actual.ActualToPlannedRatio < .98 || !hasArrivalLag || arrivalLagP99 > 50 || !hasDriverCPU || driverCPU > 70 {
		return qualificationSummary{}, errors.New("qualification load metrics invalid")
	}
	for _, name := range []string{
		"arrival_lag_positive_slope_windows_max",
		"driver_event_backlog_positive_slope_windows_max",
		"driver_socket_error_positive_slope_windows_max",
	} {
		value, ok := result.Metrics[name]
		if !ok || value != 0 {
			return qualificationSummary{}, errors.New("qualification slope windows nonzero")
		}
	}
	headroom, err := computeQualificationHeadroom(plan.Targets, result.Targets)
	if err != nil || headroom < 1.5 {
		return qualificationSummary{}, errors.New("qualification headroom insufficient")
	}
	if result.RunID == "" {
		return qualificationSummary{}, errors.New("qualification phase result invalid")
	}
	return qualificationSummary{
		QualificationRunID:          result.RunID,
		QualificationEvidenceSHA256: indexSHA,
		QualifiedTier:               result.Tier,
		RequestedTier:               plan.Tier,
		HeadroomFactorActual:        headroom,
	}, nil
}

func computeQualificationHeadroom(requested, qualified []target) (float64, error) {
	if len(requested) == 0 {
		return 0, errors.New("qualification headroom insufficient")
	}
	qualifiedByKind := make(map[string]float64, len(qualified))
	for _, item := range qualified {
		if item.Kind == "" || item.Value <= 0 {
			return 0, errors.New("qualification headroom insufficient")
		}
		if _, exists := qualifiedByKind[item.Kind]; exists {
			return 0, errors.New("qualification headroom insufficient")
		}
		qualifiedByKind[item.Kind] = item.Value
	}
	minimum := math.Inf(1)
	for _, item := range requested {
		if item.Kind == "" || item.Value <= 0 {
			return 0, errors.New("qualification headroom insufficient")
		}
		qualifiedValue, ok := qualifiedByKind[item.Kind]
		if !ok {
			return 0, errors.New("qualification headroom insufficient")
		}
		ratio := qualifiedValue / item.Value
		if ratio < minimum {
			minimum = ratio
		}
	}
	if math.IsInf(minimum, 1) {
		return 0, errors.New("qualification headroom insufficient")
	}
	return minimum, nil
}

func readManifestObject(path string) (map[string]any, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var manifest map[string]any
	if json.Unmarshal(raw, &manifest) != nil {
		return nil, errors.New("manifest JSON")
	}
	return manifest, nil
}

func equalJSONValue(left, right any) bool {
	leftRaw, leftErr := json.Marshal(left)
	rightRaw, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && string(leftRaw) == string(rightRaw)
}

func parseStrictRFC3339(value any) (time.Time, error) {
	raw, ok := value.(string)
	if !ok || raw == "" {
		return time.Time{}, errors.New("timestamp")
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		parsed, err = time.Parse(time.RFC3339, raw)
		if err != nil {
			return time.Time{}, err
		}
	}
	return parsed.UTC(), nil
}
