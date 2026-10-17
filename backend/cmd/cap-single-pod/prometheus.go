package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

type prometheusSource struct {
	Name string `json:"name"`
	URL  string `json:"url"`
}

type metricMapping struct {
	LogicalName string            `json:"logical_name"`
	Source      string            `json:"source"`
	Metric      string            `json:"metric"`
	Labels      map[string]string `json:"labels,omitempty"`
	Value       string            `json:"value"`
	Aggregate   string            `json:"aggregate"`
}

type prometheusSnapshot map[string]map[string]*dto.MetricFamily

func scrapePrometheusSources(ctx context.Context, client *http.Client, sources []prometheusSource) (prometheusSnapshot, error) {
	result := make(prometheusSnapshot, len(sources))
	for _, source := range sources {
		if source.Name == "" || validateLoopbackHTTP(source.URL) != nil {
			return nil, fmt.Errorf("prometheus source invalid: %s", source.Name)
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, source.URL, nil)
		if err != nil {
			return nil, err
		}
		response, err := client.Do(request)
		if err != nil {
			return nil, fmt.Errorf("prometheus scrape %s: %w", source.Name, err)
		}
		if response.StatusCode != http.StatusOK {
			response.Body.Close()
			return nil, fmt.Errorf("prometheus scrape %s status", source.Name)
		}
		parser := expfmt.NewTextParser(model.UTF8Validation)
		families, parseErr := parser.TextToMetricFamilies(io.LimitReader(response.Body, 16<<20))
		closeErr := response.Body.Close()
		if parseErr != nil {
			return nil, fmt.Errorf("prometheus parse %s: %w", source.Name, parseErr)
		}
		if closeErr != nil {
			return nil, closeErr
		}
		result[source.Name] = families
	}
	return result, nil
}

func mappedMetricValue(snapshot prometheusSnapshot, mapping metricMapping) (float64, error) {
	families, found := snapshot[mapping.Source]
	if !found {
		return 0, errors.New("mapping source absent")
	}
	family, found := families[mapping.Metric]
	if !found {
		return 0, errors.New("mapping metric absent")
	}
	values := []float64{}
	for _, metric := range family.Metric {
		if !labelsMatch(metric, mapping.Labels) {
			continue
		}
		value, err := prometheusMetricValue(metric, mapping.Value)
		if err != nil {
			return 0, err
		}
		values = append(values, value)
	}
	if len(values) == 0 {
		return 0, errors.New("mapping series absent")
	}
	switch mapping.Aggregate {
	case "SINGLE":
		if len(values) != 1 {
			return 0, errors.New("mapping expected single series")
		}
		return values[0], nil
	case "SUM":
		value := 0.0
		for _, item := range values {
			value += item
		}
		return value, nil
	case "MAX":
		value := values[0]
		for _, item := range values[1:] {
			if item > value {
				value = item
			}
		}
		return value, nil
	case "MIN":
		value := values[0]
		for _, item := range values[1:] {
			if item < value {
				value = item
			}
		}
		return value, nil
	default:
		return 0, errors.New("mapping aggregate")
	}
}

func labelsMatch(metric *dto.Metric, expected map[string]string) bool {
	if len(expected) == 0 {
		return true
	}
	actual := make(map[string]string, len(metric.Label))
	for _, pair := range metric.Label {
		actual[pair.GetName()] = pair.GetValue()
	}
	for name, value := range expected {
		if actual[name] != value {
			return false
		}
	}
	return true
}

func prometheusMetricValue(metric *dto.Metric, selector string) (float64, error) {
	switch selector {
	case "value":
		if metric.Gauge != nil {
			return metric.Gauge.GetValue(), nil
		}
		if metric.Counter != nil {
			return metric.Counter.GetValue(), nil
		}
		if metric.Untyped != nil {
			return metric.Untyped.GetValue(), nil
		}
	case "sum":
		if metric.Histogram != nil {
			return metric.Histogram.GetSampleSum(), nil
		}
		if metric.Summary != nil {
			return metric.Summary.GetSampleSum(), nil
		}
	case "count":
		if metric.Histogram != nil {
			return float64(metric.Histogram.GetSampleCount()), nil
		}
		if metric.Summary != nil {
			return float64(metric.Summary.GetSampleCount()), nil
		}
	case "p50":
		return prometheusQuantile(metric, .50)
	case "p95":
		return prometheusQuantile(metric, .95)
	case "p99":
		return prometheusQuantile(metric, .99)
	}
	return 0, errors.New("mapping value selector")
}

func prometheusQuantile(metric *dto.Metric, quantile float64) (float64, error) {
	if metric.Summary != nil {
		for _, item := range metric.Summary.Quantile {
			if math.Abs(item.GetQuantile()-quantile) < 1e-9 {
				return item.GetValue(), nil
			}
		}
		return 0, errors.New("summary quantile absent")
	}
	if metric.Histogram == nil || metric.Histogram.GetSampleCount() == 0 {
		return 0, errors.New("histogram empty")
	}
	buckets := append([]*dto.Bucket(nil), metric.Histogram.Bucket...)
	sort.Slice(buckets, func(i, j int) bool { return buckets[i].GetUpperBound() < buckets[j].GetUpperBound() })
	wanted := uint64(math.Ceil(quantile * float64(metric.Histogram.GetSampleCount())))
	for _, bucket := range buckets {
		if bucket.GetCumulativeCount() >= wanted {
			return bucket.GetUpperBound(), nil
		}
	}
	return math.Inf(1), nil
}

func validateMappings(sources []prometheusSource, mappings []metricMapping) error {
	sourceNames := map[string]bool{}
	for _, source := range sources {
		if source.Name == "" || sourceNames[source.Name] || validateLoopbackHTTP(source.URL) != nil {
			return errors.New("duplicate or invalid prometheus source")
		}
		sourceNames[source.Name] = true
	}
	logicalNames := map[string]bool{}
	for _, mapping := range mappings {
		if mapping.LogicalName == "" || logicalNames[mapping.LogicalName] || !sourceNames[mapping.Source] || mapping.Metric == "" {
			return fmt.Errorf("invalid metric mapping: %s", mapping.LogicalName)
		}
		if !strings.Contains(" value sum count p50 p95 p99 ", " "+mapping.Value+" ") || !strings.Contains(" SINGLE SUM MAX MIN ", " "+mapping.Aggregate+" ") {
			return fmt.Errorf("invalid metric mapping selector: %s", mapping.LogicalName)
		}
		logicalNames[mapping.LogicalName] = true
	}
	return nil
}

func adapterHTTPClient() *http.Client {
	return &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{IdleConnTimeout: 30 * time.Second, MaxIdleConnsPerHost: 8}}
}
