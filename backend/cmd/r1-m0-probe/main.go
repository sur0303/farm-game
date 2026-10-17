// Command r1-m0-probe performs a GET-only metrics/inspect observation for the
// R1-Q Wave1 bundle. It never POSTs, writes to the target, or treats a
// loopback endpoint as staging. The response byte count and digest are part of
// the evidence so a later model review can distinguish an empty/partial scrape
// from a real observation.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	maxResponseBytes = 64 << 20
	probeSchema      = "farm.r1-q.metrics-summary/v1"
)

var metricPrefixes = []string{
	"farm_gate_subscription_lease_",
	"farmgate_ws_",
	"farm_subscription_",
	"farm_actor_",
	"go_",
}

type metricSummary struct {
	Type        string  `json:"type,omitempty"`
	SampleCount int     `json:"sample_count"`
	ValuesSum   float64 `json:"values_sum"`
	ValueMax    float64 `json:"value_max"`
}

type report struct {
	Schema                string                   `json:"schema"`
	RunID                 string                   `json:"run_id,omitempty"`
	CapturedAt            string                   `json:"captured_at"`
	Endpoint              string                   `json:"endpoint"`
	EndpointClass         string                   `json:"endpoint_class"`
	Official              bool                     `json:"official"`
	ReadOnly              bool                     `json:"read_only"`
	HTTPMethod            string                   `json:"http_method"`
	HTTPStatus            int                      `json:"http_status,omitempty"`
	ResponseBytes         int                      `json:"response_bytes"`
	ResponseSHA256        string                   `json:"response_sha256,omitempty"`
	Metrics               map[string]metricSummary `json:"metrics,omitempty"`
	Status                string                   `json:"status"`
	Error                 string                   `json:"error,omitempty"`
	FinalPassClaimAllowed bool                     `json:"final_pass_claim_allowed"`
}

func main() {
	endpoint := flag.String("url", "", "metrics HTTP(S) URL (GET only)")
	jsonOut := flag.String("json-out", "", "JSON output path; stdout when empty")
	runID := flag.String("run-id", "", "R1-Q run identifier")
	timeout := flag.Duration("timeout", 10*time.Second, "HTTP timeout")
	flag.Parse()
	if strings.TrimSpace(*endpoint) == "" {
		log.Fatal("-url is required")
	}
	if *timeout <= 0 {
		log.Fatal("-timeout must be positive")
	}

	result := probe(context.Background(), *endpoint, *runID, *timeout)
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		log.Fatalf("encode report: %v", err)
	}
	encoded = append(encoded, '\n')
	if *jsonOut == "" {
		fmt.Print(string(encoded))
	} else if err := os.WriteFile(*jsonOut, encoded, 0o644); err != nil {
		log.Fatalf("write report: %v", err)
	}
	if result.Status == "FAIL" {
		os.Exit(1)
	}
	if result.Status == "OBSERVED_UNAVAILABLE" {
		os.Exit(2)
	}
}

func probe(parent context.Context, rawEndpoint, runID string, timeout time.Duration) report {
	return probeWithClient(parent, rawEndpoint, runID, timeout, nil)
}

func probeWithClient(parent context.Context, rawEndpoint, runID string, timeout time.Duration, transport http.RoundTripper) report {
	endpoint, endpointClass, official, err := classifyEndpoint(rawEndpoint)
	result := report{
		Schema:                probeSchema,
		RunID:                 runID,
		CapturedAt:            time.Now().UTC().Format(time.RFC3339Nano),
		Endpoint:              endpoint,
		EndpointClass:         endpointClass,
		Official:              official,
		ReadOnly:              true,
		HTTPMethod:            http.MethodGet,
		Status:                "OBSERVED_UNAVAILABLE",
		FinalPassClaimAllowed: false,
	}
	if err != nil {
		result.Status = "FAIL"
		result.Error = err.Error()
		return result
	}

	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		result.Status = "FAIL"
		result.Error = err.Error()
		return result
	}
	request.Header.Set("Accept", "text/plain; version=0.0.4")
	request.Header.Set("User-Agent", "r1-q-m0-probe/1")
	client := &http.Client{
		Timeout:   timeout,
		Transport: transport,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	response, err := client.Do(request)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	defer response.Body.Close()
	result.HTTPStatus = response.StatusCode
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		result.Status = "FAIL"
		result.Error = err.Error()
		return result
	}
	if len(body) > maxResponseBytes {
		result.Status = "FAIL"
		result.Error = fmt.Sprintf("response exceeds %d bytes", maxResponseBytes)
		return result
	}
	result.ResponseBytes = len(body)
	digest := sha256.Sum256(body)
	result.ResponseSHA256 = hex.EncodeToString(digest[:])
	result.Metrics = parseMetrics(body)
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		result.Error = fmt.Sprintf("metrics endpoint returned HTTP %d", response.StatusCode)
		return result
	}
	if endpointClass == "localhost" {
		result.Status = "OBSERVED_LOCAL_ONLY"
	} else {
		result.Status = "OBSERVED_NOT_FINAL"
	}
	return result
}

func classifyEndpoint(raw string) (string, string, bool, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", "invalid", false, errors.New("metrics URL must be an absolute HTTP(S) URL")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", "invalid", false, errors.New("metrics URL scheme must be http or https")
	}
	if parsed.User != nil {
		return "", "invalid", false, errors.New("metrics URL must not contain userinfo")
	}
	redacted := *parsed
	redacted.RawQuery = ""
	redacted.Fragment = ""
	host := strings.ToLower(parsed.Hostname())
	local := host == "localhost" || host == "127.0.0.1" || host == "::1" || host == "0.0.0.0" || strings.HasSuffix(host, ".localhost")
	if local {
		return redacted.String(), "localhost", false, nil
	}
	return redacted.String(), "non_local_candidate", true, nil
}

func parseMetrics(body []byte) map[string]metricSummary {
	types := make(map[string]string)
	values := make(map[string]metricSummary)
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "# TYPE ") {
			fields := strings.Fields(line)
			if len(fields) >= 4 {
				types[fields[2]] = fields[3]
			}
			continue
		}
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		name := fields[0]
		if brace := strings.IndexByte(name, '{'); brace >= 0 {
			name = name[:brace]
		}
		if !wantedMetric(name) {
			continue
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			continue
		}
		current := values[name]
		current.Type = types[name]
		current.SampleCount++
		current.ValuesSum += value
		if current.SampleCount == 1 || value > current.ValueMax {
			current.ValueMax = value
		}
		values[name] = current
	}
	for name, value := range values {
		if value.Type == "" {
			value.Type = "untyped"
			values[name] = value
		}
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	ordered := make(map[string]metricSummary, len(keys))
	for _, key := range keys {
		ordered[key] = values[key]
	}
	return ordered
}

func wantedMetric(name string) bool {
	for _, prefix := range metricPrefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}
