package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
)

type metricDefinition struct {
	Name   string `json:"name"`
	Type   string `json:"type"`
	Source string `json:"source,omitempty"`
}

type requiredSpec struct {
	Metrics []metricDefinition
	Types   map[string]string
	Gates   []string
}

func loadRequiredSpec(path, service, expectedSchema string) (requiredSpec, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return requiredSpec{}, err
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal(raw, &document); err != nil {
		return requiredSpec{}, errors.New("required metrics JSON")
	}
	var schema string
	if json.Unmarshal(document["schema_version"], &schema) != nil || schema != expectedSchema {
		return requiredSpec{}, errors.New("required metrics schema")
	}
	result := requiredSpec{Types: make(map[string]string)}
	seen := make(map[string]bool)
	for _, section := range []string{"common", service} {
		var definitions []metricDefinition
		if json.Unmarshal(document[section], &definitions) != nil || len(definitions) == 0 {
			return requiredSpec{}, fmt.Errorf("required metrics section: %s", section)
		}
		for _, definition := range definitions {
			if definition.Name == "" || seen[definition.Name] {
				return requiredSpec{}, fmt.Errorf("required metric duplicate: %s", definition.Name)
			}
			if definition.Type != "counter" && definition.Type != "gauge" && definition.Type != "histogram" {
				return requiredSpec{}, fmt.Errorf("required metric type: %s", definition.Name)
			}
			seen[definition.Name] = true
			result.Metrics = append(result.Metrics, definition)
			result.Types[definition.Name] = definition.Type
		}
	}
	if json.Unmarshal(document["correctness"], &result.Gates) != nil || len(result.Gates) == 0 {
		return requiredSpec{}, errors.New("required correctness gates")
	}
	gateSeen := make(map[string]bool)
	for _, gate := range result.Gates {
		if gate == "" || gateSeen[gate] {
			return requiredSpec{}, fmt.Errorf("required correctness duplicate: %s", gate)
		}
		gateSeen[gate] = true
	}
	sort.Slice(result.Metrics, func(i, j int) bool { return result.Metrics[i].Name < result.Metrics[j].Name })
	sort.Strings(result.Gates)
	return result, nil
}

func effectiveRequiredMetricNames(spec requiredSpec) []string {
	return uniqueSorted(requiredMetricNames(spec), mandatoryRuntimeMetricNames())
}

func mandatoryRuntimeMetricNames() []string {
	return []string{
		"service_memory_percent_of_limit", "measured_service_memory_percent_of_limit", "observer_cpu_percent", "cgroup_limit_drift", "identity_drift",
		"host_clock_step_ms", "host_cpu_steal_percent", "host_background_cpu_percent", "unexpected_container_restart_total", "namespace_residual_before_run",
		"actual_to_planned_ratio", "arrival_lag_p99_ms", "controlled_rejection_or_timeout_percent", "dependency_error_percent",
		"measured_service_fd_percent_of_limit", "measured_service_oom_kill_total", "p99_non_linear_degradation_vs_previous_tier",
		"driver_socket_error_total", "support_service_primary_queue_items", "controlled_drain_pass", "p99_sustained_degradation",
		"p99_stability_observed_seconds",
	}
}

func augmentMetricTypes(types map[string]string) map[string]string {
	result := map[string]string{}
	for name, kind := range types {
		result[name] = kind
	}
	for _, name := range mandatoryRuntimeMetricNames() {
		if _, found := result[name]; !found {
			result[name] = "gauge"
		}
	}
	for _, name := range []string{"driver_socket_error_total", "unexpected_container_restart_total", "measured_service_oom_kill_total"} {
		result[name] = "counter"
	}
	return result
}

func requiredMetricNames(spec requiredSpec) []string {
	names := make([]string, 0, len(spec.Metrics))
	for _, definition := range spec.Metrics {
		names = append(names, definition.Name)
	}
	return names
}

func missingMetricNames(values map[string]float64, required []string) []string {
	missing := []string{}
	for _, name := range required {
		if _, found := values[name]; !found {
			missing = append(missing, name)
		}
	}
	return missing
}

func sanitizeMissingMetrics(values []string, required []string) []string {
	allowed := map[string]bool{"SNAPSHOT_UNAVAILABLE": true, "UNEXPECTED_METRIC_SET": true}
	for _, name := range required {
		allowed[name] = true
	}
	for _, name := range runtimeControlMetricNames() {
		allowed[name] = true
	}
	for _, name := range values {
		if !allowed[name] {
			return []string{"UNEXPECTED_MISSING_METRIC_SET"}
		}
	}
	return uniqueSorted(values)
}

func unexpectedMetricSet(values map[string]float64, required []string) bool {
	allowed := make(map[string]bool)
	for _, name := range required {
		allowed[name] = true
	}
	for _, name := range runtimeControlMetricNames() {
		allowed[name] = true
	}
	for name := range values {
		if !allowed[name] {
			return true
		}
	}
	return false
}

func exactGateSet(values map[string]int64, required []string) bool {
	if len(values) != len(required) {
		return false
	}
	return len(missingGateNames(values, required)) == 0
}

func runtimeControlMetricNames() []string {
	return []string{
		"service_memory_percent_of_limit", "measured_service_memory_percent_of_limit", "observer_cpu_percent", "observer_rss_bytes",
		"cgroup_limit_drift", "identity_drift", "host_clock_step_ms", "host_cpu_steal_percent", "host_background_cpu_percent",
		"unexpected_container_restart_total", "namespace_residual_before_run", "actual_to_planned_ratio", "arrival_lag_p99_ms",
		"controlled_rejection_or_timeout_percent", "dependency_error_percent", "measured_service_fd_percent_of_limit",
		"measured_service_oom_kill_total", "p99_non_linear_degradation_vs_previous_tier", "driver_socket_error_total",
		"support_service_primary_queue_items", "controlled_drain_pass", "p99_sustained_degradation",
		"p99_stability_observed_seconds",
	}
}

func missingGateNames(values map[string]int64, required []string) []string {
	missing := []string{}
	for _, name := range required {
		if _, found := values[name]; !found {
			missing = append(missing, name)
		}
	}
	return missing
}

func uniqueSorted(values ...[]string) []string {
	seen := make(map[string]bool)
	for _, group := range values {
		for _, value := range group {
			if value != "" {
				seen[value] = true
			}
		}
	}
	result := make([]string, 0, len(seen))
	for value := range seen {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}
