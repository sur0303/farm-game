package friendopportunity

import (
	"bufio"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

type runConfigArtifact struct {
	Synthetic           bool        `json:"synthetic"`
	SyntheticDisclaimer string      `json:"synthetic_disclaimer"`
	CostModelVersion    string      `json:"cost_model_version"`
	ConfigFingerprint   string      `json:"config_fingerprint"`
	TraceHashes         TraceHashes `json:"trace_hashes"`
	Config              RunConfig   `json:"config"`
}

type traceRecord struct {
	RecordType string             `json:"record_type"`
	Stream     string             `json:"stream"`
	Lifecycle  string             `json:"lifecycle"`
	Metadata   *traceMetadata     `json:"metadata,omitempty"`
	Session    *SessionRequest    `json:"session_request,omitempty"`
	Boundary   *FarmBoundaryEvent `json:"farm_boundary_event,omitempty"`
}

type traceMetadata struct {
	SchemaVersion string      `json:"schema_version"`
	Synthetic     bool        `json:"synthetic"`
	TraceHashes   TraceHashes `json:"trace_hashes"`
	SessionSeed   int64       `json:"session_seed"`
	BoundarySeed  int64       `json:"boundary_seed"`
}

type manifestArtifact struct {
	Synthetic           bool              `json:"synthetic"`
	SyntheticDisclaimer string            `json:"synthetic_disclaimer"`
	Files               map[string]string `json:"file_sha256"`
}

// WriteArtifacts writes the reproducibility contract plus raw JSONL and CSV.
func WriteArtifacts(outputDir string, c RunConfig, trace Trace, results []RunResult) error {
	if outputDir == "" {
		return fmt.Errorf("friendopportunity: output directory is empty")
	}
	if err := trace.Validate(c); err != nil {
		return err
	}
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return fmt.Errorf("friendopportunity: create output directory: %w", err)
	}
	configArtifact := runConfigArtifact{
		Synthetic:           true,
		SyntheticDisclaimer: syntheticDisclaimer,
		CostModelVersion:    CostModelVersion,
		ConfigFingerprint:   c.Fingerprint(),
		TraceHashes:         trace.Hashes(),
		Config:              c,
	}
	if err := writeIndentedJSON(filepath.Join(outputDir, "run-config.json"), configArtifact); err != nil {
		return err
	}
	if err := writeTraceJSONL(filepath.Join(outputDir, "trace.jsonl"), trace); err != nil {
		return err
	}
	if err := writeResultsJSONL(filepath.Join(outputDir, "results.jsonl"), results); err != nil {
		return err
	}
	if err := writeResultsCSV(filepath.Join(outputDir, "results.csv"), results); err != nil {
		return err
	}
	summary := struct {
		Synthetic           bool        `json:"synthetic"`
		SyntheticDisclaimer string      `json:"synthetic_disclaimer"`
		TraceHashes         TraceHashes `json:"trace_hashes"`
		Results             []RunResult `json:"results"`
	}{true, syntheticDisclaimer, trace.Hashes(), results}
	if err := writeIndentedJSON(filepath.Join(outputDir, "summary.json"), summary); err != nil {
		return err
	}
	files := []string{"run-config.json", "trace.jsonl", "results.jsonl", "results.csv", "summary.json"}
	manifest := manifestArtifact{
		Synthetic:           true,
		SyntheticDisclaimer: syntheticDisclaimer,
		Files:               make(map[string]string, len(files)),
	}
	for _, name := range files {
		digest, err := fileSHA256(filepath.Join(outputDir, name))
		if err != nil {
			return err
		}
		manifest.Files[name] = digest
	}
	return writeIndentedJSON(filepath.Join(outputDir, "manifest.json"), manifest)
}

func writeTraceJSONL(path string, trace Trace) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("friendopportunity: create trace JSONL: %w", err)
	}
	writer := bufio.NewWriter(file)
	encoder := json.NewEncoder(writer)
	metadata := traceMetadata{
		SchemaVersion: TraceSchemaVersion,
		Synthetic:     true,
		TraceHashes:   trace.Hashes(),
		SessionSeed:   trace.SessionSeed,
		BoundarySeed:  trace.BoundarySeed,
	}
	if err := encoder.Encode(traceRecord{
		RecordType: "metadata",
		Stream:     "trace",
		Lifecycle:  "planned",
		Metadata:   &metadata,
	}); err != nil {
		_ = file.Close()
		return fmt.Errorf("friendopportunity: encode trace metadata: %w", err)
	}
	for i := range trace.SessionRequests {
		if err := encoder.Encode(traceRecord{
			RecordType: "session_request",
			Stream:     "session_get_next",
			Lifecycle:  "planned",
			Session:    &trace.SessionRequests[i],
		}); err != nil {
			_ = file.Close()
			return fmt.Errorf("friendopportunity: encode session trace: %w", err)
		}
	}
	for i := range trace.BoundaryEvents {
		if err := encoder.Encode(traceRecord{
			RecordType: "farm_boundary",
			Stream:     "farm_boundary",
			Lifecycle:  "planned",
			Boundary:   &trace.BoundaryEvents[i],
		}); err != nil {
			_ = file.Close()
			return fmt.Errorf("friendopportunity: encode boundary trace: %w", err)
		}
	}
	if err := writer.Flush(); err != nil {
		_ = file.Close()
		return fmt.Errorf("friendopportunity: flush trace JSONL: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("friendopportunity: close trace JSONL: %w", err)
	}
	return nil
}

func writeResultsJSONL(path string, results []RunResult) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("friendopportunity: create results JSONL: %w", err)
	}
	encoder := json.NewEncoder(file)
	for _, result := range results {
		if err := encoder.Encode(result); err != nil {
			_ = file.Close()
			return fmt.Errorf("friendopportunity: encode results JSONL: %w", err)
		}
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("friendopportunity: close results JSONL: %w", err)
	}
	return nil
}

func writeResultsCSV(path string, results []RunResult) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("friendopportunity: create results CSV: %w", err)
	}
	writer := csv.NewWriter(file)
	header := []string{
		"synthetic", "variant", "trace_sha256", "session_trace_sha256", "boundary_trace_sha256",
		"fallback_interval_ms", "sessions_planned", "sessions_sent", "sessions_completed", "sessions_rejected",
		"boundaries_planned", "boundaries_sent", "boundaries_completed", "boundaries_rejected",
		"fallback_pulls", "exact_pulls_after_hint", "pull_count", "evaluated_friends",
		"schedule_upserts", "schedule_cancels", "wakeup_claims", "authority_header_reads",
		"authority_facts_reads", "friend_index_reads", "session_route_reads", "online_hint_targets",
		"gate_batches", "logical_redis_ops", "logical_redis_bytes", "eligible_boundaries",
		"detected_boundaries", "undetected_boundaries", "realtime_detections", "realtime_detection_rate",
		"detection_latency_count", "detection_latency_mean_ms", "detection_latency_p50_ms",
		"detection_latency_p95_ms", "detection_latency_p99_ms", "saved_fallback_pulls",
		"poll_savings_cover_scheduler_ops", "poll_savings_cover_scheduler_bytes",
	}
	if err := writer.Write(header); err != nil {
		_ = file.Close()
		return fmt.Errorf("friendopportunity: write CSV header: %w", err)
	}
	for _, result := range results {
		m := result.Metrics
		savedPulls, coversOps, coversBytes := "", "", ""
		if result.PollSavings != nil {
			savedPulls = strconv.FormatInt(result.PollSavings.SavedFallbackPulls, 10)
			coversOps = strconv.FormatBool(result.PollSavings.CoversSchedulerOps)
			coversBytes = strconv.FormatBool(result.PollSavings.CoversSchedulerBytes)
		}
		row := []string{
			strconv.FormatBool(result.Synthetic), string(result.Variant), result.TraceHashes.TraceSHA256,
			result.TraceHashes.SessionSHA256, result.TraceHashes.BoundarySHA256,
			strconv.FormatInt(result.FallbackIntervalMS, 10),
			i64(result.SessionLifecycle.Planned), i64(result.SessionLifecycle.Sent),
			i64(result.SessionLifecycle.Completed), i64(result.SessionLifecycle.Rejected),
			i64(result.BoundaryLifecycle.Planned), i64(result.BoundaryLifecycle.Sent),
			i64(result.BoundaryLifecycle.Completed), i64(result.BoundaryLifecycle.Rejected),
			i64(m.FallbackPulls), i64(m.ExactPullsAfterHint), i64(m.PullCount), i64(m.EvaluatedFriends),
			i64(m.ScheduleUpserts), i64(m.ScheduleCancels), i64(m.WakeupClaims),
			i64(m.AuthorityHeaderReads), i64(m.AuthorityFactsReads), i64(m.FriendIndexReads),
			i64(m.SessionRouteReads), i64(m.OnlineHintTargets), i64(m.GateBatches),
			i64(m.LogicalRedisOps), i64(m.LogicalRedisBytes), i64(m.EligibleBoundaries),
			i64(m.DetectedBoundaries), i64(m.UndetectedBoundaries), i64(m.RealtimeDetections),
			strconv.FormatFloat(m.RealtimeDetectionRate, 'f', 6, 64),
			i64(m.DetectionLatency.Count), strconv.FormatFloat(m.DetectionLatency.MeanMS, 'f', 3, 64),
			i64(m.DetectionLatency.P50MS), i64(m.DetectionLatency.P95MS), i64(m.DetectionLatency.P99MS),
			savedPulls, coversOps, coversBytes,
		}
		if err := writer.Write(row); err != nil {
			_ = file.Close()
			return fmt.Errorf("friendopportunity: write CSV row: %w", err)
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		_ = file.Close()
		return fmt.Errorf("friendopportunity: flush results CSV: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("friendopportunity: close results CSV: %w", err)
	}
	return nil
}

func writeIndentedJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("friendopportunity: marshal %s: %w", filepath.Base(path), err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("friendopportunity: write %s: %w", filepath.Base(path), err)
	}
	return nil
}

func fileSHA256(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("friendopportunity: read %s: %w", filepath.Base(path), err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func i64(value int64) string {
	return strconv.FormatInt(value, 10)
}
