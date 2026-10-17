package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"farmgame/internal/testkit/capdriver"
)

var sha256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// The driver validates the service and phase it receives on every operation,
// so both sets are derived from the driver contract rather than restated here.
var allowedServices = driverServiceSet()
var allowedPhases = driverPhaseSet()

func driverServiceSet() map[string]bool {
	set := map[string]bool{}
	for _, service := range capdriver.Services() {
		set[string(service)] = true
	}
	return set
}

func driverPhaseSet() map[string]bool {
	set := map[string]bool{}
	for _, phase := range capdriver.Phases() {
		set[phase] = true
	}
	return set
}

func loadJSONStrict(path string, value any) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON value")
	}
	return nil
}

func loadPlan(path string) (runPlan, error) {
	var plan runPlan
	if err := loadJSONStrict(path, &plan); err != nil {
		return runPlan{}, fmt.Errorf("plan: %w", err)
	}
	if plan.SchemaVersion != "cap-single-pod-l2.run-plan.v0.1" {
		return runPlan{}, errors.New("plan: schema version")
	}
	if !allowedServices[plan.Service] || !allowedPhases[plan.Phase] {
		return runPlan{}, errors.New("plan: service or phase")
	}
	if plan.Profile == "" || plan.Tier < 0 || plan.DurationSeconds <= 0 || plan.RampSeconds < 0 || plan.RampSeconds >= plan.DurationSeconds || plan.MaximumLaunchLagMS != 50 ||
		plan.MaximumInflight <= 0 || plan.SampleIntervalMS < 100 || plan.RequestTimeoutMS <= 0 {
		return runPlan{}, errors.New("plan: duration, lag, inflight, or interval")
	}
	if !sha256Pattern.MatchString(plan.DriverBinarySHA256) || !sha256Pattern.MatchString(plan.CollectorConfigSHA256) || !sha256Pattern.MatchString(plan.OracleConfigSHA256) {
		return runPlan{}, errors.New("plan: binary or adapter config SHA-256")
	}
	for _, endpoint := range []string{plan.DriverURL, plan.DriverHealthURL, plan.SnapshotURL, plan.CollectorHealthURL, plan.OracleURL, plan.OracleHealthURL} {
		if err := validateLoopbackHTTP(endpoint); err != nil {
			return runPlan{}, err
		}
	}
	if _, err := scheduleRate(plan.Targets); err != nil {
		return runPlan{}, err
	}
	for _, target := range plan.Targets {
		if target.Kind == "" || target.Value <= 0 {
			return runPlan{}, errors.New("plan: invalid target")
		}
	}
	for _, path := range []string{plan.ManifestPath, plan.ManifestSchemaPath, plan.BindingPath, plan.ContractPath, plan.RequiredMetricsPath, plan.StopConditionsPath,
		plan.VerdictRulesPath, plan.EnvironmentPath, plan.ProfilePath} {
		if !filepath.IsAbs(path) {
			return runPlan{}, fmt.Errorf("plan: contract path must be absolute: %s", path)
		}
		if _, err := os.Stat(path); err != nil {
			return runPlan{}, fmt.Errorf("plan: contract path: %w", err)
		}
	}
	if err := validateQualificationPath(plan); err != nil {
		return runPlan{}, err
	}
	binding, err := loadBinding(plan.BindingPath)
	if err != nil {
		return runPlan{}, err
	}
	if err := binding.validateForRun(plan); err != nil {
		return runPlan{}, err
	}
	manifestSchemaSHA, err := exactFileSHA256(plan.ManifestSchemaPath)
	if err != nil || manifestSchemaSHA != binding.ManifestSchemaSHA256 {
		return runPlan{}, errors.New("binding: manifest schema hash")
	}
	if err := verifyProfileBinding(binding, plan.ProfilePath); err != nil {
		return runPlan{}, err
	}
	if err := validateExecutionContract(plan, binding); err != nil {
		return runPlan{}, err
	}
	codes := append([]string(nil), plan.AllowedErrorCodes...)
	sort.Strings(codes)
	for index, code := range codes {
		if code == "" || strings.ContainsAny(code, "\r\n") || (index > 0 && code == codes[index-1]) {
			return runPlan{}, errors.New("plan: error code allowlist")
		}
	}
	return plan, nil
}

func phaseAllowsNullQualificationPath(phase string) bool {
	switch phase {
	case "PRECHECK", "OBSERVABILITY_QUALIFICATION", "LOAD_GENERATOR_QUALIFICATION":
		return true
	default:
		return false
	}
}

func phaseRequiresQualificationEvidence(phase string) bool {
	switch phase {
	case "SMOKE", "WARMUP", "STAIRCASE", "SAFE_TIER_CANDIDATE",
		"FORMAL_1", "FORMAL_2", "FORMAL_3", "COOLDOWN_1", "COOLDOWN_2", "INDEPENDENT_REVIEW":
		return true
	default:
		return false
	}
}

func validateQualificationPath(plan runPlan) error {
	path := strings.TrimSpace(plan.LoadGeneratorQualificationPath)
	if path == "" {
		if phaseAllowsNullQualificationPath(plan.Phase) {
			return nil
		}
		return errors.New("qualification evidence path required")
	}
	if !filepath.IsAbs(path) {
		return errors.New("qualification evidence path invalid")
	}
	info, err := os.Stat(path)
	if err != nil || !info.IsDir() {
		return errors.New("qualification evidence path invalid")
	}
	return nil
}

func validateLoopbackHTTP(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "http" || parsed.User != nil || parsed.Host == "" || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Opaque != "" {
		return errors.New("plan: endpoint must be credential-free loopback HTTP")
	}
	host := parsed.Hostname()
	if host != "localhost" {
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return errors.New("plan: endpoint is not loopback")
		}
	}
	return nil
}

func scheduleRate(targets []target) (float64, error) {
	priority := []string{"arrivals_per_second", "frames_per_second", "handshakes_per_second", "work_steps_per_second", "due_claims_per_second", "fanout_per_second"}
	seen := make(map[string]float64)
	for _, target := range targets {
		if _, duplicate := seen[target.Kind]; duplicate {
			return 0, errors.New("plan: duplicate target kind")
		}
		seen[target.Kind] = target.Value
	}
	for _, kind := range priority {
		if value := seen[kind]; value > 0 {
			return value, nil
		}
	}
	return 0, errors.New("plan: no schedulable rate target")
}

func exactFileSHA256(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

func allowedCode(plan runPlan, code string) bool {
	if code == "" {
		return true
	}
	for _, allowed := range plan.AllowedErrorCodes {
		if code == allowed {
			return true
		}
	}
	return false
}
