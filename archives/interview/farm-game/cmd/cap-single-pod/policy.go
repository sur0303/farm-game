package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
)

type conditionDocument struct {
	Metric         string          `json:"metric"`
	Operator       string          `json:"operator"`
	Value          json.RawMessage `json:"value"`
	SustainSeconds int             `json:"sustain_seconds"`
}

type stopConditionDocument struct {
	SchemaVersion      string `json:"schema_version"`
	EvaluationInterval int    `json:"evaluation_interval_seconds"`
	Target             struct {
		Conditions []conditionDocument `json:"conditions"`
	} `json:"target_saturation_stop"`
	Driver struct {
		Conditions []conditionDocument `json:"conditions"`
	} `json:"load_generator_stop"`
	Dependency struct {
		Conditions []conditionDocument `json:"conditions"`
	} `json:"dependency_stop"`
	Observability struct {
		Conditions []conditionDocument `json:"conditions"`
	} `json:"observability_stop"`
	Environment struct {
		Conditions []conditionDocument `json:"conditions"`
	} `json:"environment_stop"`
	Precedence []string `json:"precedence"`
}

func validateExecutionContract(plan runPlan, binding profileBinding) error {
	if err := validateRequiredPolicy(plan.RequiredMetricsPath); err != nil {
		return err
	}
	if err := validateStopPolicy(plan.StopConditionsPath, binding); err != nil {
		return err
	}
	if err := validateVerdictPolicy(plan.VerdictRulesPath, binding); err != nil {
		return err
	}
	if err := validateEnvironmentPolicy(plan.EnvironmentPath, binding); err != nil {
		return err
	}
	if err := validateDrainPolicy(plan.ProfilePath); err != nil {
		return err
	}
	return validatePhaseDuration(plan)
}

// validateDrainPolicy binds the drain the core waits for to the drain the frozen
// profile allows, so the controlled drain the tier is judged on is the profile's
// and not the harness's own idea of one.
func validateDrainPolicy(path string) error {
	var document struct {
		Durations struct {
			ControlledDrainMax int `json:"controlled_drain_max"`
		} `json:"durations_seconds"`
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if json.Unmarshal(raw, &document) != nil {
		return errors.New("profile JSON")
	}
	if document.Durations.ControlledDrainMax != controlledDrainMaxSeconds {
		return errors.New("profile controlled drain window unsupported by core")
	}
	return nil
}

func validateRequiredPolicy(path string) error {
	var document struct {
		Policy struct {
			SampleInterval int  `json:"sample_interval_seconds"`
			MaximumGap     int  `json:"maximum_scrape_gap_seconds"`
			CounterReset   bool `json:"counter_reset_must_be_detected"`
		} `json:"policy"`
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if json.Unmarshal(raw, &document) != nil {
		return errors.New("required policy JSON")
	}
	if document.Policy.SampleInterval != 1 || document.Policy.MaximumGap != 5 || !document.Policy.CounterReset {
		return errors.New("required policy unsupported by core")
	}
	return nil
}

func validateStopPolicy(path string, binding profileBinding) error {
	var document stopConditionDocument
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if json.Unmarshal(raw, &document) != nil {
		return errors.New("stop policy JSON")
	}
	if document.SchemaVersion != binding.StopConditionsSchemaVersion || document.EvaluationInterval != 1 {
		return errors.New("stop policy identity")
	}
	expectedPrecedence := []string{VerdictFailCorrectness, VerdictInconclusiveEnvironment, VerdictInconclusiveObservability, VerdictInconclusiveDriver, VerdictInconclusiveDependency, VerdictFailTargetSaturation}
	if !reflect.DeepEqual(document.Precedence, expectedPrecedence) {
		return errors.New("stop policy precedence")
	}
	checks := []struct {
		group            []conditionDocument
		metric, operator string
		value            float64
		sustain          int
	}{
		{document.Target.Conditions, "measured_service_cpu_percent", ">=", 90, 60}, {document.Target.Conditions, "measured_service_memory_percent_of_limit", ">=", 90, 60},
		{document.Target.Conditions, "measured_service_oom_kill_total", ">", 0, 0}, {document.Target.Conditions, "measured_service_fd_percent_of_limit", ">=", 85, 60},
		{document.Target.Conditions, "primary_queue_positive_slope_windows", ">=", 3, 0}, {document.Target.Conditions, "p99_non_linear_degradation_vs_previous_tier", ">=", 2, 120}, {document.Target.Conditions, "controlled_rejection_or_timeout_percent", ">=", 5, 60},
		{document.Driver.Conditions, "actual_to_planned_ratio", "<", .95, 30}, {document.Driver.Conditions, "driver_cpu_percent", ">=", 80, 60}, {document.Driver.Conditions, "arrival_lag_p99_ms", ">", 50, 60},
		{document.Driver.Conditions, "arrival_lag_positive_slope_windows", ">=", 3, 0}, {document.Driver.Conditions, "driver_event_backlog_positive_slope_windows", ">=", 3, 0}, {document.Driver.Conditions, "driver_socket_error_positive_slope_windows", ">=", 3, 0},
		{document.Dependency.Conditions, "dependency_cpu_percent", ">=", 85, 60}, {document.Dependency.Conditions, "dependency_memory_percent_of_limit", ">=", 90, 60}, {document.Dependency.Conditions, "dependency_error_percent", ">=", 1, 30},
		{document.Dependency.Conditions, "redis_pool_wait_positive_slope_windows", ">=", 3, 0}, {document.Dependency.Conditions, "support_service_primary_queue_positive_slope_windows", ">=", 3, 0},
		{document.Observability.Conditions, "metric_scrape_gap_seconds", ">", 5, 0},
		{document.Environment.Conditions, "host_clock_step_ms", ">", 50, 0}, {document.Environment.Conditions, "host_cpu_steal_percent", ">", 2, 30}, {document.Environment.Conditions, "host_background_cpu_percent", ">", 10, 60},
	}
	for _, check := range checks {
		if !conditionMatches(check.group, check.metric, check.operator, check.value, check.sustain) {
			return fmt.Errorf("stop policy condition unsupported: %s", check.metric)
		}
	}
	return nil
}

func conditionMatches(conditions []conditionDocument, metric, operator string, value float64, sustain int) bool {
	for _, condition := range conditions {
		if condition.Metric != metric || condition.Operator != operator || condition.SustainSeconds != sustain {
			continue
		}
		var number float64
		if json.Unmarshal(condition.Value, &number) == nil && number == value {
			return true
		}
	}
	return false
}

func validateVerdictPolicy(path string, binding profileBinding) error {
	var document struct {
		SchemaVersion string `json:"schema_version"`
		Safe          struct {
			MinimumRatio float64 `json:"minimum_actual_to_planned_ratio"`
		} `json:"safe_tier_candidate"`
		FormalRaw struct {
			MinimumRatio float64 `json:"minimum_actual_to_planned_ratio_each_round"`
			ThroughputCV float64 `json:"cross_round_actual_throughput_cv_max"`
			P99CV        float64 `json:"cross_round_p99_latency_cv_max"`
			RSSCV        float64 `json:"cross_round_peak_rss_cv_max"`
		} `json:"formal_acceptance"`
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if json.Unmarshal(raw, &document) != nil {
		return errors.New("verdict policy JSON")
	}
	if document.SchemaVersion != binding.VerdictRulesSchemaVersion || document.Safe.MinimumRatio != .95 || document.FormalRaw.MinimumRatio != .98 || document.FormalRaw.ThroughputCV != .1 || document.FormalRaw.P99CV != .1 || document.FormalRaw.RSSCV != .1 {
		return errors.New("verdict policy unsupported by core")
	}
	return nil
}

func validateEnvironmentPolicy(path string, binding profileBinding) error {
	var document struct {
		SchemaVersion string `json:"schema_version"`
		Runtime       struct {
			SampleInterval   int `json:"sample_interval_seconds"`
			MaximumSampleGap int `json:"maximum_sample_gap_seconds"`
		} `json:"runtime_validity"`
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if json.Unmarshal(raw, &document) != nil {
		return errors.New("environment policy JSON")
	}
	if document.SchemaVersion != binding.EnvironmentSchemaVersion || document.Runtime.SampleInterval != 1 || document.Runtime.MaximumSampleGap != 5 {
		return errors.New("environment policy unsupported by core")
	}
	return nil
}

func validatePhaseDuration(plan runPlan) error {
	if plan.SampleIntervalMS != 1000 {
		return errors.New("plan sample interval differs from frozen profile")
	}
	if plan.Phase == "LOAD_GENERATOR_QUALIFICATION" && plan.DurationSeconds < minimumLoadGeneratorQualificationSamples {
		return errors.New("load generator qualification duration cannot form a full slope window")
	}
	type duration struct{ total, ramp int }
	expected := map[string]duration{"SMOKE": {300, 0}, "WARMUP": {600, 0}, "STAIRCASE": {720, 120}, "FORMAL_1": {1200, 0}, "COOLDOWN_1": {300, 0}, "FORMAL_2": {1200, 0}, "COOLDOWN_2": {300, 0}, "FORMAL_3": {1200, 0}}
	if value, found := expected[plan.Phase]; found && (plan.DurationSeconds != value.total || plan.RampSeconds != value.ramp) {
		return fmt.Errorf("plan duration differs from frozen profile: %s", plan.Phase)
	}
	return nil
}
