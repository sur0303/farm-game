// Command redis-production-probe performs non-destructive capability
// inspection plus isolated write/Lua/WAIT round trips. It never prints the
// Redis URL or credentials and never triggers failover, SCRIPT FLUSH, slot
// migration, or changes managed-service configuration.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"farmgame/internal/redisdomains"
	"github.com/redis/go-redis/v9"
)

type report struct {
	ProfileVersion        string            `json:"profile_version"`
	RedisVersion          string            `json:"redis_version,omitempty"`
	Role                  string            `json:"role,omitempty"`
	ConnectedReplicas     int               `json:"connected_replicas"`
	MasterFailoverState   string            `json:"master_failover_state,omitempty"`
	AOFEnabled            bool              `json:"aof_enabled"`
	AOFLastWriteStatus    string            `json:"aof_last_write_status,omitempty"`
	RDBLastSaveStatus     string            `json:"rdb_last_bgsave_status,omitempty"`
	Loading               bool              `json:"loading"`
	MaxMemoryBytes        int64             `json:"maxmemory_bytes"`
	MaxMemoryPolicy       string            `json:"maxmemory_policy,omitempty"`
	NoEvictionAssessment  string            `json:"noeviction_assessment"`
	ConfigReadable        bool              `json:"config_readable"`
	RedisTimeUnixMS       int64             `json:"redis_time_unix_ms"`
	RedisTimeRoundTripMS  int64             `json:"redis_time_round_trip_ms"`
	RedisTimeClockSkewMS  int64             `json:"redis_time_clock_skew_ms"`
	ACLWhoAmI             string            `json:"acl_whoami"`
	ACLCategories         string            `json:"acl_categories"`
	ClusterMode           string            `json:"cluster_mode"`
	MovedAskProbe         string            `json:"moved_ask_probe"`
	IsolatedProbeKeyTTLMS int64             `json:"isolated_probe_key_ttl_ms"`
	WaitReplicaAcks       int               `json:"wait_replica_acks"`
	EvalAssessment        string            `json:"eval_assessment"`
	EvalSHAProfile        string            `json:"evalsha_assessment"`
	NoScriptAssessment    string            `json:"noscript_assessment"`
	LuaMaxLoadedBytes     int               `json:"lua_max_loaded_bytes"`
	LuaSizeResults        map[string]string `json:"lua_size_results"`
	Checks                map[string]string `json:"checks"`
}

func main() {
	rawURL := strings.TrimSpace(os.Getenv("FARM_REDIS_URL"))
	clusterURLs := strings.TrimSpace(os.Getenv("FARM_REDIS_CLUSTER_URLS"))
	profilePath := strings.TrimSpace(os.Getenv("FARM_REDIS_PHYSICAL_PROFILE"))
	if profilePath == "" {
		profilePath = "deploy/profiles/redis-physical.w31a.v1.json"
	}
	profile, err := loadProbeProfile(profilePath)
	if err != nil {
		log.Fatalf("invalid probe profile: %v", err)
	}
	if rawURL == "" && clusterURLs == "" {
		log.Fatal("FARM_REDIS_URL or FARM_REDIS_CLUSTER_URLS is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(profile.OverallTimeoutMS)*time.Millisecond)
	defer cancel()
	client, err := redisdomains.OpenEndpoint(ctx, rawURL, clusterURLs)
	if err != nil {
		log.Fatalf("invalid Redis endpoint: %v", err)
	}
	defer client.Close()
	result, err := runProbe(ctx, client, profile)
	if err != nil {
		log.Fatal(err)
	}
	encoded, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(encoded))
}

func runProbe(ctx context.Context, client redis.UniversalClient, profile probeProfile) (report, error) {
	if err := client.Ping(ctx).Err(); err != nil {
		return report{}, fmt.Errorf("redis ping failed: %w", err)
	}
	values, err := readInfo(ctx, client, "server", "persistence", "replication", "memory")
	if err != nil {
		return report{}, fmt.Errorf("INFO failed: %w", err)
	}
	result := report{
		ProfileVersion:      profile.ProfileVersion,
		RedisVersion:        values["redis_version"],
		Role:                values["role"],
		ConnectedReplicas:   integer(values["connected_slaves"]),
		MasterFailoverState: values["master_failover_state"],
		AOFEnabled:          values["aof_enabled"] == "1",
		AOFLastWriteStatus:  values["aof_last_write_status"],
		RDBLastSaveStatus:   values["rdb_last_bgsave_status"],
		Loading:             values["loading"] == "1",
		MaxMemoryBytes:      integer64(values["maxmemory"]),
		MaxMemoryPolicy:     values["maxmemory_policy"],
		LuaSizeResults:      make(map[string]string),
		Checks:              make(map[string]string),
	}
	readConfig(ctx, client, &result)
	if strings.EqualFold(result.MaxMemoryPolicy, "noeviction") {
		result.NoEvictionAssessment = "PASS"
	} else if result.MaxMemoryPolicy == "" {
		result.NoEvictionAssessment = "UNKNOWN: policy not readable"
	} else {
		result.NoEvictionAssessment = "FAIL: " + result.MaxMemoryPolicy
	}
	probeRedisTime(ctx, client, &result)
	probeACL(ctx, client, &result)
	probeCluster(ctx, client, &result)
	if err := probeIsolatedPersistence(ctx, client, profile, &result); err != nil {
		return report{}, err
	}
	probeLua(ctx, client, profile, &result)
	result.Checks["ping"] = "ok"
	result.Checks["failover_injection"] = "NOT_RUN: use redis-ha-probe during an operator-approved failover window"
	return result, nil
}

func readConfig(ctx context.Context, client redis.UniversalClient, result *report) {
	config := make(map[string]string)
	for _, key := range []string{"maxmemory-policy", "appendonly", "appendfsync", "save"} {
		values, err := client.ConfigGet(ctx, key).Result()
		if err != nil {
			result.Checks["config"] = classifyError(err)
			return
		}
		config[key] = values[key]
	}
	result.ConfigReadable = true
	if result.MaxMemoryPolicy == "" {
		result.MaxMemoryPolicy = config["maxmemory-policy"]
	}
	result.Checks["appendonly"] = config["appendonly"]
	result.Checks["appendfsync"] = config["appendfsync"]
	result.Checks["save"] = config["save"]
}

func probeRedisTime(ctx context.Context, client redis.UniversalClient, result *report) {
	started := time.Now()
	serverTime, err := client.Time(ctx).Result()
	finished := time.Now()
	if err != nil {
		result.Checks["redis_time"] = classifyError(err)
		return
	}
	midpoint := started.Add(finished.Sub(started) / 2)
	result.RedisTimeUnixMS = serverTime.UnixMilli()
	result.RedisTimeRoundTripMS = finished.Sub(started).Milliseconds()
	result.RedisTimeClockSkewMS = serverTime.Sub(midpoint).Milliseconds()
	result.Checks["redis_time"] = "ok"
}

func probeACL(ctx context.Context, client redis.UniversalClient, result *report) {
	if _, err := client.Do(ctx, "ACL", "WHOAMI").Text(); err != nil {
		result.ACLWhoAmI = classifyError(err)
	} else {
		// Do not emit the credential identity itself.
		result.ACLWhoAmI = "SUPPORTED"
	}
	if _, err := client.Do(ctx, "ACL", "CAT").Result(); err != nil {
		result.ACLCategories = classifyError(err)
	} else {
		result.ACLCategories = "SUPPORTED"
	}
}

func probeCluster(ctx context.Context, client redis.UniversalClient, result *report) {
	values, err := readInfo(ctx, client, "cluster")
	if err != nil {
		result.ClusterMode = "UNKNOWN: " + classifyError(err)
		result.MovedAskProbe = "NOT_RUN: cluster capability unknown"
		return
	}
	if values["cluster_enabled"] != "1" {
		result.ClusterMode = "STANDALONE_OR_PROXY"
		result.MovedAskProbe = "NOT_APPLICABLE_ON_CURRENT_ENDPOINT"
		return
	}
	result.ClusterMode = "CLUSTER"
	if _, err := client.Do(ctx, "CLUSTER", "INFO").Result(); err != nil {
		result.Checks["cluster_info"] = classifyError(err)
	} else {
		result.Checks["cluster_info"] = "ok"
	}
	// A real MOVED/ASK requires direct access to a non-owner node or an active
	// slot migration. This probe deliberately does neither.
	result.MovedAskProbe = "NOT_RUN: requires direct-node topology and operator-approved slot migration"
}

func probeIsolatedPersistence(ctx context.Context, client redis.UniversalClient, profile probeProfile, result *report) error {
	key := fmt.Sprintf("{classic-farm:w31a:probe:%d}:authority", time.Now().UnixNano())
	probeTTL := time.Duration(profile.ProbeKeyTTLMS) * time.Millisecond
	created, err := client.SetNX(ctx, key, "durability-probe", probeTTL).Result()
	if err != nil {
		return fmt.Errorf("isolated write failed: %w", err)
	}
	if !created {
		return errors.New("isolated probe key unexpectedly existed")
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = client.Del(cleanup, key).Err()
	}()
	ttl, err := client.TTL(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("isolated TTL failed: %w", err)
	}
	if ttl <= 0 {
		return fmt.Errorf("isolated probe key missing safety TTL: %s", ttl)
	}
	result.IsolatedProbeKeyTTLMS = ttl.Milliseconds()
	if result.ClusterMode == "CLUSTER" {
		// WAIT is connection-local. Through a generic ClusterClient it may be
		// sent to a different primary than the preceding key write, so it
		// cannot be reported as a meaningful replica durability guarantee.
		result.Checks["wait"] = "NOT_APPLICABLE: WAIT is not a cluster-wide durability proof"
	} else {
		acks, waitErr := client.Do(ctx, "WAIT", profile.WaitReplicas, time.Duration(profile.WaitTimeoutMS).Milliseconds()).Int()
		if waitErr != nil {
			result.Checks["wait"] = classifyError(waitErr)
		} else {
			result.Checks["wait"] = "ok"
		}
		result.WaitReplicaAcks = int(acks)
	}
	if value, err := client.Get(ctx, key).Result(); err != nil || value != "durability-probe" {
		return errors.New("isolated readback failed")
	}
	result.Checks["write_read_delete"] = "ok"
	return nil
}

func probeLua(ctx context.Context, client redis.UniversalClient, profile probeProfile, result *report) {
	probeKey := fmt.Sprintf("{classic-farm:w31a:lua:%d}:value", time.Now().UnixNano())
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = client.Del(cleanup, probeKey).Err()
	}()
	const script = `redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2]); return redis.call('GET', KEYS[1])`
	probeTTL := strconv.Itoa(profile.ProbeKeyTTLMS)
	scriptRef := redis.NewScript(script)
	if value, err := scriptRef.Run(ctx, client, []string{probeKey}, "eval-ok", probeTTL).Text(); err != nil || value != "eval-ok" {
		result.EvalAssessment = classifyError(err)
	} else {
		result.EvalAssessment = "PASS"
	}
	if value, evalErr := client.EvalSha(ctx, scriptRef.Hash(), []string{probeKey}, "evalsha-ok", probeTTL).Text(); evalErr != nil || value != "evalsha-ok" {
		result.EvalSHAProfile = classifyError(evalErr)
	} else {
		result.EvalSHAProfile = "PASS"
	}
	// Trigger NOSCRIPT with an impossible digest. Never use SCRIPT FLUSH because
	// that would disrupt other applications sharing the service.
	if _, err := client.EvalSha(ctx, strings.Repeat("0", 40), []string{probeKey}).Result(); err == nil {
		result.NoScriptAssessment = "FAIL: fake SHA unexpectedly executed"
	} else if strings.Contains(strings.ToUpper(err.Error()), "NOSCRIPT") {
		result.NoScriptAssessment = "PASS: NOSCRIPT classified without SCRIPT FLUSH"
	} else {
		result.NoScriptAssessment = classifyError(err)
	}
	for _, size := range profile.LuaScriptSizeCandidates {
		candidate := luaScriptOfSize(size)
		scriptRef := redis.NewScript(candidate)
		key := strconv.Itoa(size)
		value, runErr := scriptRef.Run(ctx, client, []string{probeKey}).Int()
		if runErr != nil || value != size {
			result.LuaSizeResults[key] = classifyError(runErr)
			continue
		}
		value, evalErr := client.EvalSha(ctx, scriptRef.Hash(), []string{probeKey}).Int()
		if evalErr != nil || value != size {
			result.LuaSizeResults[key] = classifyError(evalErr)
			continue
		}
		result.LuaSizeResults[key] = "PASS"
		if size > result.LuaMaxLoadedBytes {
			result.LuaMaxLoadedBytes = size
		}
	}
}

func luaScriptOfSize(size int) string {
	prefix := "return " + strconv.Itoa(size) + " --"
	if len(prefix) >= size {
		return prefix
	}
	return prefix + strings.Repeat("x", size-len(prefix))
}

func readInfo(ctx context.Context, client redis.UniversalClient, sections ...string) (map[string]string, error) {
	raw, err := client.Info(ctx, sections...).Result()
	if err != nil {
		return nil, err
	}
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
	return values, nil
}

func classifyError(err error) string {
	if err == nil {
		return "FAILED_WITHOUT_REDIS_ERROR"
	}
	upper := strings.ToUpper(err.Error())
	switch {
	case strings.Contains(upper, "NOPERM"):
		return "DENIED:NOPERM"
	case strings.Contains(upper, "UNKNOWN COMMAND"):
		return "UNSUPPORTED:UNKNOWN_COMMAND"
	case strings.Contains(upper, "NOSCRIPT"):
		return "NOSCRIPT"
	case errors.Is(err, context.DeadlineExceeded):
		return "TIMEOUT"
	default:
		return "ERROR:" + sanitizeError(err.Error())
	}
}

func sanitizeError(value string) string {
	value = strings.ReplaceAll(value, "\n", " ")
	if len(value) > 160 {
		value = value[:160]
	}
	return value
}

func integer(value string) int {
	parsed, _ := strconv.Atoi(value)
	return parsed
}

func integer64(value string) int64 {
	parsed, _ := strconv.ParseInt(value, 10, 64)
	return parsed
}
