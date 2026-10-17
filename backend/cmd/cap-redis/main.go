package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"farmgame/internal/friend"

	"github.com/redis/go-redis/v9"
)

var errFriendBatchBucketMismatch = errors.New("friend CompleteSnapshotV2 batch bucket mismatch")

type manifestFile struct {
	SchemaVersion      string            `json:"schema_version"`
	Frozen             frozenIdentity    `json:"frozen_identity"`
	RedisConfigHash    string            `json:"redis_config_hash"`
	MachineSpecHash    string            `json:"machine_spec_hash"`
	MachineSpec        map[string]string `json:"machine_spec"`
	CPUGovernor        string            `json:"cpu_governor"`
	ContainerCPULimit  string            `json:"container_cpu_limit"`
	ContainerMemory    string            `json:"container_memory_limit"`
	ModelLayoutVersion string            `json:"model_layout_version"`
	RouteSnapshotHash  string            `json:"route_snapshot_hash"`
	LeaseProfileHash   string            `json:"lease_profile_hash"`
	ProfileHash        string            `json:"profile_hash"`
	RandomSeed         int64             `json:"random_seed"`
	RedisPoolSize      int               `json:"redis_pool_size"`
	RedisMinIdleConns  int               `json:"redis_min_idle_conns"`
}

type runConfig struct {
	redisURL       string
	manifestPath   string
	outputPath     string
	runID          string
	roundKind      string
	roundNumber    int
	environment    string
	zone           string
	operations     int
	concurrencies  []int
	distributions  []string
	scenarioFilter map[string]struct{}
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "CAP_REDIS_ERROR:", err)
		os.Exit(1)
	}
}

func run() error {
	var cfg runConfig
	var concurrencies, distributions, scenarios string
	flag.StringVar(&cfg.redisURL, "redis-url", os.Getenv("CAP_REDIS_URL"), "isolated Redis URL (credentials are never emitted)")
	flag.StringVar(&cfg.manifestPath, "manifest", "", "frozen CAP-REDIS-L1-R1 manifest")
	flag.StringVar(&cfg.outputPath, "output", "", "round JSON output")
	flag.StringVar(&cfg.runID, "run-id", "", "unique run id")
	flag.StringVar(&cfg.roundKind, "round-kind", "formal", "warmup or formal")
	flag.IntVar(&cfg.roundNumber, "round-number", 1, "round number")
	flag.StringVar(&cfg.environment, "environment", "capr1", "lowercase isolated environment")
	flag.StringVar(&cfg.zone, "zone", "z1", "lowercase unique zone")
	flag.IntVar(&cfg.operations, "operations", 600, "operations per cell")
	flag.StringVar(&concurrencies, "concurrency", "1,10,100,300", "comma-separated concurrency levels")
	flag.StringVar(&distributions, "key-distributions", "uniform,hot", "uniform,hot")
	flag.StringVar(&scenarios, "scenarios", "", "optional comma-separated operation names")
	flag.Parse()
	if cfg.redisURL == "" || cfg.manifestPath == "" || cfg.outputPath == "" || cfg.runID == "" {
		return errors.New("redis-url, manifest, output, and run-id are required")
	}
	if cfg.roundKind != "smoke" && cfg.roundKind != "warmup" && cfg.roundKind != "formal" {
		return errors.New("round-kind must be smoke, warmup, or formal")
	}
	var err error
	if cfg.concurrencies, err = parsePositiveInts(concurrencies); err != nil {
		return err
	}
	cfg.distributions = splitNonempty(distributions)
	if len(cfg.distributions) == 0 {
		return errors.New("key distributions required")
	}
	cfg.scenarioFilter = make(map[string]struct{})
	for _, name := range splitNonempty(scenarios) {
		cfg.scenarioFilter[name] = struct{}{}
	}
	if cfg.operations <= 0 {
		return errors.New("operations must be positive")
	}

	manifestData, err := os.ReadFile(cfg.manifestPath)
	if err != nil {
		return err
	}
	var manifest manifestFile
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		return fmt.Errorf("manifest: %w", err)
	}
	if err := validateManifest(manifest); err != nil {
		return err
	}

	options, err := redis.ParseURL(cfg.redisURL)
	if err != nil {
		return fmt.Errorf("redis URL: %w", err)
	}
	options.PoolSize = manifest.RedisPoolSize
	options.MinIdleConns = manifest.RedisMinIdleConns
	options.Protocol = 2
	hook := newRedisMetricHook()
	options.Dialer = hook.countingDialer
	client := redis.NewClient(options)
	client.AddHook(hook)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Minute)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}
	if err := client.ConfigSet(ctx, "slowlog-log-slower-than", "0").Err(); err != nil {
		return fmt.Errorf("slowlog threshold: %w", err)
	}
	if err := client.ConfigSet(ctx, "slowlog-max-len", "1000000").Err(); err != nil {
		return fmt.Errorf("slowlog len: %w", err)
	}
	env, err := newBenchmarkEnvironmentForTask(ctx, client, hook, cfg.environment, cfg.zone, manifest.Frozen.TaskID)
	if err != nil {
		return err
	}

	purpose := "redis_serialization_baseline_r1"
	if manifest.Frozen.TaskID == "CAP-REDIS-L1-R2" {
		purpose = "r2_redis_serialization_qualification"
	}
	report := roundReport{SchemaVersion: manifest.SchemaVersion, Purpose: purpose,
		BaselineEligible: false, Supports30MDAU: false, Frozen: manifest.Frozen,
		Runtime: runtimeIdentity{RunID: cfg.runID, RoundKind: cfg.roundKind, RoundNumber: cfg.roundNumber,
			StartedAtUTC: time.Now().UTC(), RedisEndpointHash: endpointHash(options), RedisConfigHash: manifest.RedisConfigHash,
			Namespace: cfg.environment + ":" + cfg.zone, MachineSpecHash: manifest.MachineSpecHash, MachineSpec: manifest.MachineSpec,
			CPUGovernor: manifest.CPUGovernor, ContainerCPULimit: manifest.ContainerCPULimit,
			ContainerMemory: manifest.ContainerMemory, GoGC: envOr("GOGC", "default"), GoMemLimit: envOr("GOMEMLIMIT", "off"),
			RedisPoolSize: options.PoolSize, RedisMinIdleConns: options.MinIdleConns,
			ModelLayoutVersion: manifest.ModelLayoutVersion, RouteSnapshotHash: manifest.RouteSnapshotHash,
			LeaseProfileHash: manifest.LeaseProfileHash, ProfileHash: manifest.ProfileHash, RandomSeed: manifest.RandomSeed}}

	definitions := allScenarios()
	for _, definition := range definitions {
		if len(cfg.scenarioFilter) != 0 {
			if _, ok := cfg.scenarioFilter[definition.name]; !ok {
				continue
			}
		}
		for _, distribution := range cfg.distributions {
			for _, concurrency := range cfg.concurrencies {
				planned := cfg.operations
				if planned < concurrency {
					planned = concurrency
				}
				if err := env.resetNamespace(ctx); err != nil {
					return fmt.Errorf("reset namespace: %w", err)
				}
				prepared, err := definition.prepare(ctx, env, distribution, concurrency, planned)
				if err != nil {
					return fmt.Errorf("prepare %s/%s/c%d: %w", definition.name, distribution, concurrency, err)
				}
				cell, err := measureCell(ctx, client, hook, definition, prepared, concurrency)
				report.Cells = append(report.Cells, cell)
				if cell.CorrectnessViolations != 0 {
					switch {
					case errors.Is(err, friend.ErrFriendReconcile):
						report.HardGates.FriendAuthorityReconcile += cell.CorrectnessViolations
					case errors.Is(err, errFriendBatchBucketMismatch):
						report.HardGates.FriendBatchBucketMismatch += cell.CorrectnessViolations
					default:
						report.HardGates.DueResidual += cell.CorrectnessViolations
					}
				}
				if err != nil {
					if cell.CorrectnessViolations != 0 {
						report.Verdict = "FAIL_CORRECTNESS"
					} else {
						report.Verdict = "FAIL_OPERATION"
					}
					report.VerdictReasons = append(report.VerdictReasons, fmt.Sprintf("%s/%s/c%d: %v", definition.name, distribution, concurrency, err))
					report.Runtime.CompletedAtUTC = time.Now().UTC()
					_ = env.deleteNamespace(ctx)
					_ = writeJSON(cfg.outputPath, report)
					return fmt.Errorf("measured cell failed: %w", err)
				}
			}
		}
	}
	if len(report.Cells) == 0 {
		return errors.New("no scenarios selected")
	}
	// Exact namespace deletion is both the Due-residual hard gate and the
	// cleanup boundary. It never touches keys outside this run namespace.
	if err := env.deleteNamespace(ctx); err != nil {
		return err
	}
	residual, err := env.namespaceResidual(ctx)
	if err != nil {
		return err
	}
	if residual != 0 {
		// Namespace residue after exact teardown is separate from business Due.
		if manifest.Frozen.TaskID == "CAP-REDIS-L1-R2" {
			report.HardGates.NamespaceResidual += int(residual)
		} else {
			report.HardGates.DueResidual += int(residual)
		}
	}
	if manifest.Frozen.TaskID == "CAP-REDIS-L1-R2" {
		assertions := r2RoundAssertions{FriendBatchCallsMatchUniqueBuckets: true}
		for _, cell := range report.Cells {
			if cell.FriendSnapshot != nil {
				assertions.FriendCompleteSnapshotV2Observed = true
				assertions.FriendBatchExpectedUniqueBuckets += cell.FriendSnapshot.ExpectedUniqueBucketCalls
				assertions.FriendBatchObservedScriptCalls += cell.FriendSnapshot.ObservedScriptCalls
				assertions.FriendBatchCallsMatchUniqueBuckets = assertions.FriendBatchCallsMatchUniqueBuckets && cell.FriendSnapshot.CallsMatchUniqueBuckets
			}
		}
		if !assertions.FriendCompleteSnapshotV2Observed {
			report.HardGates.FriendSnapshotUnobserved++
		}
		if !assertions.FriendBatchCallsMatchUniqueBuckets {
			report.HardGates.FriendBatchBucketMismatch++
		}
		report.R2Assertions = &assertions
	}
	report.CorrectnessObservationComplete = true
	if residual != 0 {
		report.VerdictReasons = append(report.VerdictReasons, fmt.Sprintf("namespace residual=%d", residual))
	}
	report.Runtime.CompletedAtUTC = time.Now().UTC()
	if report.HardGates.total() != 0 {
		report.Verdict = "FAIL_CORRECTNESS"
	} else if len(report.VerdictReasons) != 0 {
		report.Verdict = "INCONCLUSIVE"
	} else {
		report.Verdict = "PASS"
	}
	if err := writeJSON(cfg.outputPath, report); err != nil {
		return err
	}
	fmt.Printf("CAP_REDIS_ROUND=%s VERDICT=%s CELLS=%d OUTPUT=%s\n", cfg.runID, report.Verdict, len(report.Cells), cfg.outputPath)
	if report.Verdict != "PASS" {
		return fmt.Errorf("round verdict=%s", report.Verdict)
	}
	return nil
}

func allScenarios() []scenarioDefinition {
	out := append(actorScenarios(), workflowScenarios()...)
	out = append(out, friendScenarios()...)
	out = append(out, runtimeScenarios()...)
	return out
}

func measureCell(ctx context.Context, client *redis.Client, hook *redisMetricHook, definition scenarioDefinition, prepared *preparedScenario, concurrency int) (cellResult, error) {
	cell := cellResult{Operation: definition.name, Group: definition.group, KeyDistribution: prepared.keyDistribution,
		Concurrency: concurrency, PlannedOperations: len(prepared.operations), OutcomeCounts: make(map[string]int), Notes: append([]string(nil), prepared.notes...)}
	if err := client.SlowLogReset(ctx).Err(); err != nil {
		return cell, err
	}
	rtts, err := measureRTT(ctx, client, 11)
	if err != nil {
		return cell, err
	}
	if err := client.SlowLogReset(ctx).Err(); err != nil {
		return cell, err
	}
	cpuBeforeText, err := client.Info(ctx, "cpu").Result()
	if err != nil {
		return cell, err
	}
	poolBefore := client.PoolStats()
	metricBefore := hook.snapshot()
	var friendSnapshotBefore uint64
	if prepared.friendSnapshot != nil {
		friendSnapshotBefore = prepared.friendSnapshot.observedCalls()
	}
	var memBefore runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)
	usageBefore := readUsage()

	type observed struct {
		sample  sampleResult
		latency time.Duration
		err     error
	}
	observations := make([]observed, len(prepared.operations))
	jobs := make(chan int)
	var workers sync.WaitGroup
	for worker := 0; worker < concurrency; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				started := time.Now()
				sample, opErr := prepared.operations[index](ctx, index)
				observations[index] = observed{sample: sample, latency: time.Since(started), err: opErr}
			}
		}()
	}
	for index := range prepared.operations {
		jobs <- index
	}
	close(jobs)
	workers.Wait()

	metricAfter := hook.snapshot()
	poolAfter := client.PoolStats()
	usageAfter := readUsage()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)
	cpuAfterText, cpuErr := client.Info(ctx, "cpu").Result()
	if cpuErr != nil {
		return cell, cpuErr
	}
	logs, slowErr := client.SlowLogGet(ctx, 1000000).Result()
	if slowErr != nil {
		return cell, slowErr
	}
	var postMeasureErr error
	if prepared.observeDue != nil {
		expected, observed, observeErr := prepared.observeDue(ctx)
		cell.ExpectedFixtureDue, cell.ObservedFixtureDue = expected, observed
		if observeErr != nil {
			postMeasureErr = observeErr
		} else if expected != observed {
			cell.CorrectnessViolations++
			postMeasureErr = fmt.Errorf("fixture Due mismatch expected=%d observed=%d", expected, observed)
		}
	}
	if prepared.validate != nil {
		if err := prepared.validate(ctx); err != nil && postMeasureErr == nil {
			postMeasureErr = err
		}
	}
	if prepared.friendSnapshot != nil {
		observed := prepared.friendSnapshot.observedCalls() - friendSnapshotBefore
		match := observed == prepared.friendSnapshot.expectedCalls
		cell.FriendSnapshot = &friendSnapshotObservation{ScriptSHA: prepared.friendSnapshot.scriptSHA,
			ExpectedUniqueBucketCalls: prepared.friendSnapshot.expectedCalls, ObservedScriptCalls: observed, CallsMatchUniqueBuckets: match}
		if !match && postMeasureErr == nil {
			cell.CorrectnessViolations++
			postMeasureErr = fmt.Errorf("%w: calls=%d unique buckets=%d", errFriendBatchBucketMismatch, observed, prepared.friendSnapshot.expectedCalls)
		}
	}

	var latencies, serializations, deserializations []time.Duration
	var logicalRequests, logicalResponses, payloads []int
	var failures []failedOperation
	var firstErr error
	for index, observation := range observations {
		if observation.err != nil {
			cell.FailedOperations++
			failures = append(failures, failedOperation{index: index, sample: observation.sample, err: observation.err})
			if isCorrectnessError(observation.err) {
				cell.CorrectnessViolations++
			}
			if firstErr == nil {
				firstErr = observation.err
			}
			continue
		}
		cell.CompletedOperations++
		cell.OutcomeCounts[observation.sample.Outcome]++
		latencies = append(latencies, observation.latency)
		serializations = append(serializations, observation.sample.SerializationTime)
		deserializations = append(deserializations, observation.sample.DeserializationTime)
		logicalRequests = append(logicalRequests, observation.sample.LogicalRequestBytes)
		logicalResponses = append(logicalResponses, observation.sample.LogicalResponseBytes)
		payloads = append(payloads, observation.sample.PayloadBytes)
	}
	measured := diffMetrics(metricAfter, metricBefore)
	denominator := float64(max(1, cell.CompletedOperations+cell.FailedOperations))
	cell.RedisCommandsPerOp = float64(measured.Commands) / denominator
	cell.PipelineItemsPerOp = float64(measured.Items) / denominator
	cell.RequestBytesPerOp = float64(measured.WriteBytes) / denominator
	cell.ResponseBytesPerOp = float64(measured.ReadBytes) / denominator
	cell.LogicalRequestBytes, cell.LogicalResponseBytes, cell.PayloadBytes = ints(logicalRequests), ints(logicalResponses), ints(payloads)
	cell.ClientLatencyUS, cell.SerializationUS, cell.DeserializationUS = durationsUS(latencies), durationsUS(serializations), durationsUS(deserializations)
	cell.NetworkRTTUS = durationsUS(rtts)
	cell.RedisClientQueueUSPerOp = float64(poolAfter.WaitDurationNs-poolBefore.WaitDurationNs) / 1000 / denominator
	cell.AllocsPerOp = float64(memAfter.Mallocs-memBefore.Mallocs) / denominator
	cell.ClientCPUUSPerOp = (usageAfter - usageBefore).Seconds() * 1e6 / denominator
	cell.RedisCPUUSPerOp = (parseRedisCPU(cpuAfterText) - parseRedisCPU(cpuBeforeText)) * 1e6 / denominator
	cell.ConnectionsRebuilt = measured.Dials
	cell.Commands = sortedCommands(measured.ByCommand)
	luaDurations, luaCommands := slowLogDurations(logs)
	cell.LuaExecutionUS, cell.LuaCommands = durationsUS(luaDurations), luaCommands
	cell.FailureDiagnosticsExpected = len(failures)
	cell.FailureDiagnosticsComplete = len(failures) == 0
	if len(failures) != 0 {
		if prepared.diagnoseFailures == nil {
			cell.FailureDiagnosticCollectionError = "no bounded diagnostic collector for failed operation family"
		} else {
			diagnostics, diagnosticErr := prepared.diagnoseFailures(ctx, failures)
			cell.FailureDiagnostics = diagnostics
			cell.FailureDiagnosticsCaptured = len(diagnostics)
			cell.FailureEvidenceHashIndex, cell.FailureEvidenceAggregateHash = aggregateFailureEvidence(diagnostics)
			cell.FailureDiagnosticsComplete = diagnosticErr == nil && len(diagnostics) == len(failures)
			if diagnosticErr != nil {
				cell.FailureDiagnosticCollectionError = boundedError(diagnosticErr)
			}
		}
	}
	if firstErr != nil {
		return cell, firstErr
	}
	if postMeasureErr != nil {
		return cell, postMeasureErr
	}
	if cell.CompletedOperations != cell.PlannedOperations {
		return cell, fmt.Errorf("completed=%d planned=%d", cell.CompletedOperations, cell.PlannedOperations)
	}
	return cell, nil
}

func isCorrectnessError(err error) bool {
	return errors.Is(err, friend.ErrFriendReconcile) || errors.Is(err, errFriendBatchBucketMismatch)
}

func readUsage() time.Duration {
	var usage syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
	return time.Duration(usage.Utime.Sec+usage.Stime.Sec)*time.Second + time.Duration(usage.Utime.Usec+usage.Stime.Usec)*time.Microsecond
}

func measureRTT(ctx context.Context, client *redis.Client, count int) ([]time.Duration, error) {
	out := make([]time.Duration, 0, count)
	for i := 0; i < count; i++ {
		started := time.Now()
		if err := client.Ping(ctx).Err(); err != nil {
			return nil, err
		}
		out = append(out, time.Since(started))
	}
	return out, nil
}

func endpointHash(options *redis.Options) string {
	value := options.Network + "\x00" + options.Addr + "\x00" + strconv.Itoa(options.DB)
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func validateManifest(manifest manifestFile) error {
	validIdentity := (manifest.SchemaVersion == schemaVersion && manifest.Frozen.TaskID == "CAP-REDIS-L1-R1") ||
		(manifest.SchemaVersion == schemaVersionR2 && manifest.Frozen.TaskID == "CAP-REDIS-L1-R2")
	if !validIdentity {
		return errors.New("manifest schema/task mismatch")
	}
	values := []string{manifest.Frozen.BackendBusinessCommit, manifest.Frozen.BackendEvidenceHead, manifest.Frozen.H5Commit,
		manifest.Frozen.BackendImageDigest, manifest.Frozen.H5ImageDigest, manifest.RedisConfigHash, manifest.MachineSpecHash,
		manifest.ModelLayoutVersion, manifest.RouteSnapshotHash, manifest.LeaseProfileHash, manifest.ProfileHash}
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			return errors.New("manifest has empty required identity")
		}
	}
	if manifest.RedisPoolSize < 300 {
		return errors.New("redis pool size must cover concurrency 300")
	}
	return nil
}

func parsePositiveInts(value string) ([]int, error) {
	var out []int
	for _, raw := range splitNonempty(value) {
		number, err := strconv.Atoi(raw)
		if err != nil || number <= 0 {
			return nil, fmt.Errorf("invalid positive integer %q", raw)
		}
		out = append(out, number)
	}
	if len(out) == 0 {
		return nil, errors.New("at least one concurrency required")
	}
	sort.Ints(out)
	return out, nil
}
func splitNonempty(value string) []string {
	var out []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}
func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
func writeJSON(path string, value any) error {
	if err := os.MkdirAll(filepathDir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}
func filepathDir(path string) string {
	index := strings.LastIndex(path, "/")
	if index < 0 {
		return "."
	}
	return path[:index]
}
