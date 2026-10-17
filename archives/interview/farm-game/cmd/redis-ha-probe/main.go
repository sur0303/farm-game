// Command redis-ha-probe records the client-visible Redis semantics during an
// operator-triggered failover or backup restore. It never triggers failover.
//
// The run mode continuously creates idempotent markers under one isolated hash
// tag and fsyncs every observation to a local JSONL manifest. The verify mode
// can later inspect either the original endpoint or an isolated restored
// endpoint and distinguish:
//
//   - an acknowledged write that is missing;
//   - a write that timed out but was actually committed;
//   - a write that timed out and was not committed;
//   - a write acknowledged by the requested number of replicas that is missing.
//
// Credentials are read only from FARM_REDIS_URL and are never written to the
// manifest or stdout.
package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const manifestSchema = "farm.redis-ha-probe/v1"

var writeMarkerScript = redis.NewScript(`
local existing = redis.call("HGET", KEYS[1], ARGV[1])
if existing then
  local separator = string.find(existing, "|", 1, true)
  if not separator or string.sub(existing, separator + 1) ~= ARGV[2] then
    return redis.error_reply("IDEMPOTENCY_CONFLICT")
  end
  return existing
end
local sequence = redis.call("INCR", KEYS[2])
local value = tostring(sequence) .. "|" .. ARGV[2]
redis.call("HSET", KEYS[1], ARGV[1], value)
return value
`)

type event struct {
	Type      string         `json:"type"`
	Schema    string         `json:"schema,omitempty"`
	Namespace string         `json:"namespace,omitempty"`
	At        time.Time      `json:"at,omitempty"`
	Header    *header        `json:"header,omitempty"`
	Attempt   *attempt       `json:"attempt,omitempty"`
	Role      *roleSample    `json:"role,omitempty"`
	Summary   *verifySummary `json:"summary,omitempty"`
}

type header struct {
	Duration        string `json:"duration"`
	Interval        string `json:"interval"`
	CommandTimeout  string `json:"command_timeout"`
	WaitReplicas    int    `json:"wait_replicas"`
	WaitTimeout     string `json:"wait_timeout"`
	AttemptHashKey  string `json:"attempt_hash_key"`
	SequenceKey     string `json:"sequence_key"`
	Credentials     string `json:"credentials"`
	OperatorAction  string `json:"operator_action"`
	EndpointPurpose string `json:"endpoint_purpose"`
}

type attempt struct {
	ID           string    `json:"id"`
	PayloadHash  string    `json:"payload_hash"`
	StartedAt    time.Time `json:"started_at"`
	ReturnedAt   time.Time `json:"returned_at"`
	State        string    `json:"state"`
	RedisSeq     uint64    `json:"redis_sequence,omitempty"`
	ErrorClass   string    `json:"error_class,omitempty"`
	WaitReplicas int       `json:"wait_replicas"`
	WaitAcks     int       `json:"wait_acks"`
	WaitError    string    `json:"wait_error,omitempty"`
}

type roleSample struct {
	Role              string `json:"role,omitempty"`
	MasterReplID      string `json:"master_replid,omitempty"`
	MasterReplOffset  int64  `json:"master_repl_offset,omitempty"`
	ConnectedReplicas int    `json:"connected_replicas"`
	FailoverState     string `json:"failover_state,omitempty"`
	Error             string `json:"error,omitempty"`
}

type verifySummary struct {
	EndpointPurpose          string `json:"endpoint_purpose"`
	Attempts                 int    `json:"attempts"`
	Acknowledged             int    `json:"acknowledged"`
	Unknown                  int    `json:"unknown"`
	ReplicaAcknowledged      int    `json:"replica_acknowledged"`
	AcknowledgedMissing      int    `json:"acknowledged_missing"`
	ReplicaAcknowledgedLost  int    `json:"replica_acknowledged_lost"`
	UnknownCommitted         int    `json:"unknown_committed"`
	UnknownNotCommitted      int    `json:"unknown_not_committed"`
	PayloadMismatch          int    `json:"payload_mismatch"`
	ReadErrors               int    `json:"read_errors"`
	MaxObservedUnavailableMS int64  `json:"max_observed_unavailable_ms"`
	RoleChanges              int    `json:"role_changes"`
	Conclusion               string `json:"conclusion"`
}

type manifest struct {
	Namespace string
	Header    header
	Attempts  []attempt
	Roles     []roleSample
}

type config struct {
	mode            string
	manifestPath    string
	duration        time.Duration
	interval        time.Duration
	commandTimeout  time.Duration
	waitReplicas    int
	waitTimeout     time.Duration
	roleInterval    time.Duration
	endpointPurpose string
}

func main() {
	cfg := parseFlags()
	rawURL := strings.TrimSpace(os.Getenv("FARM_REDIS_URL"))
	if rawURL == "" {
		log.Fatal("FARM_REDIS_URL is required")
	}
	options, err := redis.ParseURL(rawURL)
	if err != nil {
		log.Fatal("invalid FARM_REDIS_URL")
	}
	options.MaxRetries = -1
	options.DialTimeout = cfg.commandTimeout
	options.ReadTimeout = cfg.commandTimeout + cfg.waitTimeout
	options.WriteTimeout = cfg.commandTimeout

	switch cfg.mode {
	case "run":
		if err := run(options, cfg); err != nil {
			log.Fatal(err)
		}
	case "verify":
		if err := verifyCommand(options, cfg); err != nil {
			log.Fatal(err)
		}
	case "cleanup":
		if err := cleanup(options, cfg.manifestPath); err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatalf("unsupported -mode %q (want run, verify or cleanup)", cfg.mode)
	}
}

func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.mode, "mode", "run", "run, verify or cleanup")
	flag.StringVar(&cfg.manifestPath, "manifest", "", "JSONL manifest path (required)")
	flag.DurationVar(&cfg.duration, "duration", 10*time.Minute, "run observation duration")
	flag.DurationVar(&cfg.interval, "interval", 100*time.Millisecond, "delay between writes")
	flag.DurationVar(&cfg.commandTimeout, "command-timeout", 750*time.Millisecond, "single Redis command timeout")
	flag.IntVar(&cfg.waitReplicas, "wait-replicas", 1, "replica acknowledgements requested after each successful write")
	flag.DurationVar(&cfg.waitTimeout, "wait-timeout", time.Second, "Redis WAIT timeout")
	flag.DurationVar(&cfg.roleInterval, "role-interval", time.Second, "INFO replication sample interval")
	flag.StringVar(&cfg.endpointPurpose, "endpoint-purpose", "primary", "label for primary or restored endpoint")
	flag.Parse()
	if cfg.manifestPath == "" {
		log.Fatal("-manifest is required")
	}
	if cfg.duration <= 0 || cfg.interval <= 0 || cfg.commandTimeout <= 0 ||
		cfg.waitReplicas < 0 || cfg.waitTimeout <= 0 || cfg.roleInterval <= 0 {
		log.Fatal("durations and intervals must be positive; wait-replicas cannot be negative")
	}
	return cfg
}

func run(options *redis.Options, cfg config) error {
	namespace, err := randomNamespace()
	if err != nil {
		return err
	}
	hashKey, sequenceKey := probeKeys(namespace)
	file, err := os.OpenFile(cfg.manifestPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create manifest: %w", err)
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	appendEvent := func(value event) error {
		encoded, marshalErr := json.Marshal(value)
		if marshalErr != nil {
			return marshalErr
		}
		if _, writeErr := writer.Write(append(encoded, '\n')); writeErr != nil {
			return writeErr
		}
		if flushErr := writer.Flush(); flushErr != nil {
			return flushErr
		}
		return file.Sync()
	}
	started := time.Now().UTC()
	meta := header{
		Duration: cfg.duration.String(), Interval: cfg.interval.String(),
		CommandTimeout: cfg.commandTimeout.String(), WaitReplicas: cfg.waitReplicas,
		WaitTimeout: cfg.waitTimeout.String(), AttemptHashKey: hashKey,
		SequenceKey: sequenceKey, Credentials: "not recorded",
		OperatorAction:  "none; this process only observes an independently triggered platform action",
		EndpointPurpose: cfg.endpointPurpose,
	}
	if err := appendEvent(event{
		Type: "header", Schema: manifestSchema, Namespace: namespace, At: started, Header: &meta,
	}); err != nil {
		return err
	}

	client := redis.NewClient(options)
	defer client.Close()
	if err := ping(client, cfg.commandTimeout); err != nil {
		return fmt.Errorf("initial Redis ping: %w", err)
	}

	deadline := started.Add(cfg.duration)
	nextRole := started
	index := 0
	for time.Now().Before(deadline) {
		now := time.Now().UTC()
		if !now.Before(nextRole) {
			sample := sampleRole(client, cfg.commandTimeout)
			if err := appendEvent(event{
				Type: "role", Schema: manifestSchema, Namespace: namespace, At: now, Role: &sample,
			}); err != nil {
				return err
			}
			nextRole = now.Add(cfg.roleInterval)
		}
		index++
		observation := executeAttempt(client, namespace, hashKey, sequenceKey, index, cfg)
		if err := appendEvent(event{
			Type: "attempt", Schema: manifestSchema, Namespace: namespace,
			At: observation.ReturnedAt, Attempt: &observation,
		}); err != nil {
			return err
		}
		time.Sleep(cfg.interval)
	}

	loaded, err := loadManifest(cfg.manifestPath)
	if err != nil {
		return err
	}
	summary := verify(client, loaded, cfg.endpointPurpose, cfg.commandTimeout)
	if err := appendEvent(event{
		Type: "summary", Schema: manifestSchema, Namespace: namespace,
		At: time.Now().UTC(), Summary: &summary,
	}); err != nil {
		return err
	}
	return printJSON(summary)
}

func verifyCommand(options *redis.Options, cfg config) error {
	loaded, err := loadManifest(cfg.manifestPath)
	if err != nil {
		return err
	}
	client := redis.NewClient(options)
	defer client.Close()
	if err := ping(client, cfg.commandTimeout); err != nil {
		return fmt.Errorf("verification endpoint ping: %w", err)
	}
	summary := verify(client, loaded, cfg.endpointPurpose, cfg.commandTimeout)
	return printJSON(summary)
}

func cleanup(options *redis.Options, manifestPath string) error {
	loaded, err := loadManifest(manifestPath)
	if err != nil {
		return err
	}
	client := redis.NewClient(options)
	defer client.Close()
	hashKey, sequenceKey := probeKeys(loaded.Namespace)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Del(ctx, hashKey, sequenceKey).Err(); err != nil {
		return err
	}
	fmt.Printf("{\"cleaned_namespace\":%q}\n", loaded.Namespace)
	return nil
}

func executeAttempt(client *redis.Client, namespace, hashKey, sequenceKey string, index int, cfg config) attempt {
	started := time.Now().UTC()
	id := fmt.Sprintf("attempt-%08d", index)
	payload := sha256.Sum256([]byte(namespace + "\x00" + id))
	payloadHash := hex.EncodeToString(payload[:])
	result := attempt{
		ID: id, PayloadHash: payloadHash, StartedAt: started,
		WaitReplicas: cfg.waitReplicas,
	}
	// WAIT only covers writes previously issued by the same Redis connection.
	// A dedicated Conn is therefore part of the measurement contract; calling
	// Client.Wait after Client.Eval could borrow a different pooled connection
	// and produce a misleading replica acknowledgement.
	conn := client.Conn()
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), cfg.commandTimeout)
	value, err := writeMarkerScript.Run(ctx, conn, []string{hashKey, sequenceKey}, id, payloadHash).Text()
	cancel()
	result.ReturnedAt = time.Now().UTC()
	if err != nil {
		result.State = "UNKNOWN"
		result.ErrorClass = classifyError(err)
		return result
	}
	sequence, returnedHash, parseErr := parseMarker(value)
	if parseErr != nil || returnedHash != payloadHash {
		result.State = "UNKNOWN"
		result.ErrorClass = "INVALID_RESPONSE"
		return result
	}
	result.State = "ACKNOWLEDGED"
	result.RedisSeq = sequence
	if cfg.waitReplicas == 0 {
		return result
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), cfg.waitTimeout+cfg.commandTimeout)
	acks, waitErr := conn.Wait(waitCtx, cfg.waitReplicas, cfg.waitTimeout).Result()
	waitCancel()
	result.WaitAcks = int(acks)
	if waitErr != nil {
		result.WaitError = classifyError(waitErr)
	}
	result.ReturnedAt = time.Now().UTC()
	return result
}

func verify(client *redis.Client, loaded manifest, endpointPurpose string, timeout time.Duration) verifySummary {
	summary := summarizeAvailability(loaded, endpointPurpose)
	hashKey, _ := probeKeys(loaded.Namespace)
	for _, item := range loaded.Attempts {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		value, err := client.HGet(ctx, hashKey, item.ID).Result()
		cancel()
		if errors.Is(err, redis.Nil) {
			if item.State == "ACKNOWLEDGED" {
				summary.AcknowledgedMissing++
				if item.WaitReplicas > 0 && item.WaitAcks >= item.WaitReplicas {
					summary.ReplicaAcknowledgedLost++
				}
			} else {
				summary.UnknownNotCommitted++
			}
			continue
		}
		if err != nil {
			summary.ReadErrors++
			continue
		}
		_, payloadHash, parseErr := parseMarker(value)
		if parseErr != nil || payloadHash != item.PayloadHash {
			summary.PayloadMismatch++
			continue
		}
		if item.State == "UNKNOWN" {
			summary.UnknownCommitted++
		}
	}
	switch {
	case summary.ReadErrors > 0:
		summary.Conclusion = "INCONCLUSIVE: verification reads failed"
	case summary.PayloadMismatch > 0:
		summary.Conclusion = "FAILED: committed marker payload mismatch"
	case summary.ReplicaAcknowledgedLost > 0:
		summary.Conclusion = "FAILED: a replica-acknowledged write is missing"
	case summary.AcknowledgedMissing > 0:
		summary.Conclusion = "FAILED: a client-acknowledged write is missing"
	default:
		summary.Conclusion = "PASS: all client-acknowledged markers are present"
	}
	return summary
}

func summarizeAvailability(loaded manifest, endpointPurpose string) verifySummary {
	summary := verifySummary{EndpointPurpose: endpointPurpose, Attempts: len(loaded.Attempts)}
	var unavailableStart time.Time
	var maxUnavailable time.Duration
	for _, item := range loaded.Attempts {
		if item.State == "ACKNOWLEDGED" {
			summary.Acknowledged++
			if item.WaitReplicas > 0 && item.WaitAcks >= item.WaitReplicas {
				summary.ReplicaAcknowledged++
			}
			if !unavailableStart.IsZero() {
				duration := item.ReturnedAt.Sub(unavailableStart)
				if duration > maxUnavailable {
					maxUnavailable = duration
				}
				unavailableStart = time.Time{}
			}
		} else {
			summary.Unknown++
			if unavailableStart.IsZero() {
				unavailableStart = item.StartedAt
			}
		}
	}
	if !unavailableStart.IsZero() && len(loaded.Attempts) > 0 {
		duration := loaded.Attempts[len(loaded.Attempts)-1].ReturnedAt.Sub(unavailableStart)
		if duration > maxUnavailable {
			maxUnavailable = duration
		}
	}
	summary.MaxObservedUnavailableMS = maxUnavailable.Milliseconds()
	lastReplID := ""
	for _, sample := range loaded.Roles {
		if sample.MasterReplID == "" {
			continue
		}
		if lastReplID != "" && sample.MasterReplID != lastReplID {
			summary.RoleChanges++
		}
		lastReplID = sample.MasterReplID
	}
	return summary
}

func loadManifest(path string) (manifest, error) {
	file, err := os.Open(path)
	if err != nil {
		return manifest{}, err
	}
	defer file.Close()
	var loaded manifest
	decoder := json.NewDecoder(bufio.NewReader(file))
	for {
		var value event
		if err := decoder.Decode(&value); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return manifest{}, fmt.Errorf("decode manifest: %w", err)
		}
		if value.Schema != manifestSchema {
			return manifest{}, fmt.Errorf("unsupported manifest schema %q", value.Schema)
		}
		if loaded.Namespace == "" {
			loaded.Namespace = value.Namespace
		} else if value.Namespace != loaded.Namespace {
			return manifest{}, errors.New("manifest contains multiple namespaces")
		}
		if value.Header != nil {
			loaded.Header = *value.Header
		}
		if value.Attempt != nil {
			loaded.Attempts = append(loaded.Attempts, *value.Attempt)
		}
		if value.Role != nil {
			loaded.Roles = append(loaded.Roles, *value.Role)
		}
	}
	if loaded.Namespace == "" || loaded.Header.AttemptHashKey == "" {
		return manifest{}, errors.New("manifest header is missing")
	}
	expectedHash, expectedSequence := probeKeys(loaded.Namespace)
	if loaded.Header.AttemptHashKey != expectedHash || loaded.Header.SequenceKey != expectedSequence {
		return manifest{}, errors.New("manifest keys do not match its namespace")
	}
	return loaded, nil
}

func sampleRole(client *redis.Client, timeout time.Duration) roleSample {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	raw, err := client.Info(ctx, "replication").Result()
	if err != nil {
		return roleSample{Error: classifyError(err)}
	}
	values := parseInfo(raw)
	return roleSample{
		Role: values["role"], MasterReplID: values["master_replid"],
		MasterReplOffset:  integer64(values["master_repl_offset"]),
		ConnectedReplicas: integer(values["connected_slaves"]),
		FailoverState:     values["master_failover_state"],
	}
}

func probeKeys(namespace string) (string, string) {
	tag := "{" + namespace + "}"
	return "farm:test:ha:" + tag + ":attempts", "farm:test:ha:" + tag + ":sequence"
}

func randomNamespace() (string, error) {
	var value [12]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func parseMarker(value string) (uint64, string, error) {
	parts := strings.SplitN(value, "|", 2)
	if len(parts) != 2 {
		return 0, "", errors.New("invalid marker")
	}
	sequence, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil || sequence == 0 || len(parts[1]) != sha256.Size*2 {
		return 0, "", errors.New("invalid marker")
	}
	return sequence, parts[1], nil
}

func parseInfo(raw string) map[string]string {
	values := make(map[string]string)
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 {
			values[parts[0]] = parts[1]
		}
	}
	return values
}

func classifyError(err error) string {
	if err == nil {
		return ""
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "TIMEOUT"
	case errors.Is(err, context.Canceled):
		return "CANCELED"
	case strings.Contains(strings.ToLower(err.Error()), "connection reset"):
		return "CONNECTION_RESET"
	case strings.Contains(strings.ToLower(err.Error()), "connection refused"):
		return "CONNECTION_REFUSED"
	case strings.Contains(strings.ToLower(err.Error()), "i/o timeout"):
		return "TIMEOUT"
	default:
		return "OTHER"
	}
}

func ping(client *redis.Client, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return client.Ping(ctx).Err()
}

func integer(value string) int {
	parsed, _ := strconv.Atoi(value)
	return parsed
}

func integer64(value string) int64 {
	parsed, _ := strconv.ParseInt(value, 10, 64)
	return parsed
}

func printJSON(value any) error {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(encoded))
	return nil
}
