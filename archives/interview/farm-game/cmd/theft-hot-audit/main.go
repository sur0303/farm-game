// Command theft-hot-audit owns only the external fixture and read-only audit
// for the 200-client WSS Theft capacity stage. Business Theft commands are
// always sent by cmd/ws-stress through FarmGate.
package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	commonpb "farmgame/api/gen/common"
	contractspb "farmgame/api/gen/contracts"
	farmpb "farmgame/api/gen/farm"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/actorrouting"
	"farmgame/internal/contracts"
	farmactor "farmgame/internal/farm/actor"
	"farmgame/internal/friend"
	"farmgame/internal/identity"
	"farmgame/internal/plantonline"
	playeringress "farmgame/internal/player/ingress"
	theftscenario "farmgame/internal/scenario/theft"
	"farmgame/internal/storage/authoritystore"
	redisauthority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workflow"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

const (
	hashSeed = uint64(202608050200)
	zone     = "local"
)

var (
	mode               = flag.String("mode", "audit", "authority, prepare, or audit")
	redisURL           = flag.String("redis-url", "redis://127.0.0.1:6379/0", "real Redis URL")
	namespaceEnv       = flag.String("namespace", "thefthot", "Redis namespace environment")
	realm              = flag.String("realm", "theft-hot", "actor routing realm")
	profileVersion     = flag.String("profile", "theft-hot-v1", "physical profile version")
	workflowPrefix     = flag.String("workflow-prefix", "theft-hot-work", "workflow Redis prefix")
	runID              = flag.String("run-id", "", "stable fixture/run identifier")
	owner              = flag.Uint64("owner", 920000000, "farm owner/player start")
	visitors           = flag.Int("visitors", 199, "distinct visitor count")
	prepareConcurrency = flag.Int("prepare-concurrency", 32, "bounded concurrent HTTPS share accepts during fixture preparation")
	playerEndpoint     = flag.String("player-endpoint", "127.0.0.1:19100", "player holder endpoint")
	farmEndpoint       = flag.String("farm-endpoint", "127.0.0.1:19000", "farm holder endpoint")
	playerHolder       = flag.String("player-holder", "theft-hot-player", "player holder id")
	farmHolder         = flag.String("farm-holder", "theft-hot-farm", "farm holder id")
	playerBoot         = flag.String("player-boot", "theft-hot-player-boot", "player holder boot id")
	farmBoot           = flag.String("farm-boot", "theft-hot-farm-boot", "farm holder boot id")
	routeOut           = flag.String("route-out", "", "authority mode route snapshot path")
	httpURL            = flag.String("http-url", "http://127.0.0.1:8080", "FarmGate HTTP URL")
	loadSecret         = flag.String("load-secret", "", "test-only Login HMAC secret")
	driverJSON         = flag.String("driver-json", "", "audit mode ws-stress JSON")
	jsonOut            = flag.String("json-out", "", "machine-readable result path")
)

type commonConfig struct {
	client     *redis.Client
	namespace  rediskey.Namespace
	profile    rediskey.PhysicalProfile
	calculator *actorrouting.Calculator
	actorRead  *auditActorReader
}

type authorityReport struct {
	Mode             string                     `json:"mode"`
	RunID            string                     `json:"run_id"`
	RedisVersion     string                     `json:"redis_version"`
	RedisURL         string                     `json:"redis_url"`
	RealRedisProcess bool                       `json:"real_redis_process"`
	Snapshot         actorrouting.RouteSnapshot `json:"route_snapshot"`
}

type prepareReport struct {
	Mode                  string  `json:"mode"`
	RunID                 string  `json:"run_id"`
	OwnerPlayerID         uint64  `json:"owner_player_id"`
	Visitors              int     `json:"visitors"`
	PublicShareCreated    bool    `json:"public_share_created"`
	PublicShareAccepted   int     `json:"public_share_accepted"`
	AuthorityPairsActive  int     `json:"authority_pairs_active"`
	PairFixtureWritesOnly bool    `json:"pair_fixture_writes_only"`
	TheftRecordsWritten   int     `json:"theft_records_written_by_helper"`
	AcceptP50Ms           float64 `json:"share_accept_p50_ms"`
	AcceptP95Ms           float64 `json:"share_accept_p95_ms"`
	AcceptP99Ms           float64 `json:"share_accept_p99_ms"`
}

type driverReport struct {
	RunID           string `json:"run_id"`
	ConfiguredConns int    `json:"configured_connections"`
	PlayerStart     uint64 `json:"player_start"`
	Ready           int    `json:"ready"`
	PeakActive      int    `json:"peak_active"`
	JoinErrors      int    `json:"join_errors"`
	UnexpectedEnd   int    `json:"unexpected_disconnects"`
	Theft           struct {
		ReadyBarrier       int  `json:"subscription_ready_barrier"`
		HealthyCompleted   int  `json:"healthy_connections_completed"`
		TerminalSucceeded  int  `json:"terminal_succeeded"`
		TerminalNoChange   int  `json:"terminal_no_change"`
		TerminalRejected   int  `json:"terminal_rejected"`
		TerminalErrors     int  `json:"terminal_errors"`
		OperationQueries   int  `json:"operation_result_queries"`
		ProcessingQueries  int  `json:"operation_result_processing"`
		UnavailableQueries int  `json:"operation_result_unavailable_retried"`
		RetryErrors        int  `json:"same_operation_retry_errors"`
		Rejoin             int  `json:"rejoin_required"`
		VersionGaps        int  `json:"farm_version_gaps"`
		VersionRegressions int  `json:"farm_version_regressions"`
		SlowReaderEnabled  bool `json:"slow_reader_enabled"`
		Fixture            struct {
			PlotID      uint32 `json:"plot_id"`
			CropCycleID string `json:"crop_cycle_id"`
		} `json:"fixture"`
		HealthyVersions map[string]struct {
			Baseline uint64   `json:"baseline_farm_version"`
			Last     uint64   `json:"last_farm_version"`
			Versions []uint64 `json:"farm_change_versions"`
			Gap      bool     `json:"gap"`
			Rollback bool     `json:"rollback_or_duplicate"`
		} `json:"healthy_connection_versions"`
	} `json:"theft_hot"`
}

type auditReport struct {
	Mode                        string          `json:"mode"`
	RunID                       string          `json:"run_id"`
	FarmID                      uint64          `json:"farm_id"`
	CropCycleID                 string          `json:"crop_cycle_id"`
	BaseYield                   int32           `json:"base_yield"`
	StealableLimit              int32           `json:"stealable_limit"`
	FarmStolenTotal             int32           `json:"farm_stolen_total"`
	FarmVersion                 uint64          `json:"farm_version"`
	FarmAuthorityRecordVersion  uint64          `json:"farm_authority_record_version"`
	WorkflowsRead               int             `json:"workflows_read"`
	WorkflowSucceeded           int             `json:"workflow_succeeded"`
	WorkflowNoChange            int             `json:"workflow_no_change"`
	WorkflowRejected            int             `json:"workflow_rejected"`
	WorkflowOther               int             `json:"workflow_other"`
	SuccessShares               int             `json:"success_shares"`
	ExpectedGrossPerSuccess     uint64          `json:"expected_gross_per_success"`
	GrossShareMismatches        int             `json:"gross_share_mismatches"`
	AttemptReceiptCount         int             `json:"attempt_receipt_count"`
	UniqueAttemptTuples         int             `json:"unique_attempt_tuples"`
	GrossCommitted              uint64          `json:"gross_committed"`
	NetCommitted                uint64          `json:"net_committed"`
	VisitorInventoryNet         int64           `json:"visitor_inventory_net"`
	GuardLost                   uint64          `json:"guard_lost"`
	GuardCompensation           uint64          `json:"guard_compensation"`
	SucceededFarmReceipts       int             `json:"succeeded_farm_receipts"`
	SucceededVisitorReceipts    int             `json:"succeeded_visitor_receipts"`
	ParticipantReceiptMismatch  int             `json:"participant_receipt_mismatches"`
	ActiveFarmSettlements       int             `json:"active_farm_settlements"`
	MaxWorkerConvergenceMs      int64           `json:"max_worker_convergence_ms"`
	OverdueNonterminalWorkflows int             `json:"overdue_nonterminal_workflows"`
	HealthyConnectionsAudited   int             `json:"healthy_connections_audited"`
	UnexpectedHealthyDisconnect int             `json:"unexpected_healthy_disconnects"`
	Checks                      map[string]bool `json:"checks"`
	Violations                  []string        `json:"violations,omitempty"`
}

func main() {
	flag.Parse()
	if *runID == "" || *owner == 0 || *visitors != 199 || *prepareConcurrency < 1 || *prepareConcurrency > 32 {
		fatal(errors.New("run-id, nonzero owner, visitors=199, and prepare-concurrency in [1,32] are required"))
	}
	cfg, err := buildConfig()
	if err != nil {
		fatal(err)
	}
	defer cfg.client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	var value any
	switch *mode {
	case "authority":
		value, err = runAuthority(ctx, cfg)
	case "prepare":
		value, err = runPrepare(ctx, cfg)
	case "audit":
		value, err = runAudit(ctx, cfg)
	default:
		err = fmt.Errorf("unknown mode %q", *mode)
	}
	if err != nil {
		fatal(err)
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		fatal(err)
	}
	data = append(data, '\n')
	if *jsonOut != "" {
		if err := os.WriteFile(*jsonOut, data, 0o644); err != nil {
			fatal(err)
		}
	}
	_, _ = os.Stdout.Write(data)
	if report, ok := value.(auditReport); ok && len(report.Violations) != 0 {
		os.Exit(2)
	}
}

func buildConfig() (commonConfig, error) {
	options, err := redis.ParseURL(*redisURL)
	if err != nil {
		return commonConfig{}, err
	}
	client := redis.NewClient(options)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return commonConfig{}, fmt.Errorf("real Redis unavailable: %w", err)
	}
	namespace, err := rediskey.NewNamespace(*namespaceEnv, zone)
	if err != nil {
		return commonConfig{}, err
	}
	profile := rediskey.PhysicalProfile{ProfileVersion: *profileVersion, Namespace: namespace,
		AuthorityLayoutVersion: "v1", WorkflowLayoutVersion: "v1", WorkflowHashVersion: 1, WorkflowBucketCount: 256}
	calculator, err := actorrouting.NewCalculator(*realm, actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: hashSeed}, 1, 1)
	if err != nil {
		client.Close()
		return commonConfig{}, err
	}
	store, err := redisauthority.NewStore(client, nil, redisauthority.Options{
		Profile: profile, Resolver: calculator, LeaseLimits: authoritystore.DefaultLeaseLimits(),
	})
	if err != nil {
		client.Close()
		return commonConfig{}, err
	}
	actorRead, err := newAuditActorReader(store)
	if err != nil {
		client.Close()
		return commonConfig{}, err
	}
	return commonConfig{client: client, namespace: namespace, profile: profile, calculator: calculator, actorRead: actorRead}, nil
}

func runAuthority(ctx context.Context, cfg commonConfig) (authorityReport, error) {
	if *routeOut == "" {
		return authorityReport{}, errors.New("route-out is required in authority mode")
	}
	scripts, err := redisauthority.NewScriptRegistry(cfg.client)
	if err != nil {
		return authorityReport{}, err
	}
	if err := scripts.EnsureLoaded(ctx); err != nil {
		return authorityReport{}, err
	}
	store, err := redisauthority.NewStore(cfg.client, scripts, redisauthority.Options{Profile: cfg.profile, Resolver: cfg.calculator, LeaseLimits: authoritystore.DefaultLeaseLimits()})
	if err != nil {
		return authorityReport{}, err
	}
	claim := func(domain actorrouting.RouteDomain, holder, boot string) (authoritystore.ShardAuthorityTransitionResult, error) {
		return store.ClaimShardAuthority(ctx, authoritystore.ClaimShardAuthorityRequest{
			Caller:                 authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerRouteManager, InstanceID: "theft-hot-route-manager", BootID: "theft-hot-route-manager-" + *runID},
			TransitionID:           "claim-" + strings.ToLower(string(domain)) + "-" + *runID,
			AuthorityLayoutVersion: "v1", Domain: domain, VirtualShard: 0,
			NewHolderID: holder, NewHolderBootID: boot, RequestedTTLms: (55 * time.Second).Milliseconds(),
		})
	}
	playerClaim, err := claim(actorrouting.DomainPlayer, *playerHolder, *playerBoot)
	if err != nil || playerClaim.Status != authoritystore.ShardTransitionApplied {
		return authorityReport{}, fmt.Errorf("claim player shard: status=%s err=%v", playerClaim.Status, err)
	}
	farmClaim, err := claim(actorrouting.DomainFarm, *farmHolder, *farmBoot)
	if err != nil || farmClaim.Status != authoritystore.ShardTransitionApplied {
		return authorityReport{}, fmt.Errorf("claim farm shard: status=%s err=%v", farmClaim.Status, err)
	}
	leaseUntil := playerClaim.Authority.LeaseUntilMs
	if farmClaim.Authority.LeaseUntilMs < leaseUntil {
		leaseUntil = farmClaim.Authority.LeaseUntilMs
	}
	snapshot := actorrouting.RouteSnapshot{Realm: *realm, GlobalRevision: 1, HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: hashSeed,
		Holders: []actorrouting.HolderRef{{InstanceID: *playerHolder, BootID: *playerBoot, Endpoint: *playerEndpoint, State: actorrouting.HolderStateActive, Weight: 1}, {InstanceID: *farmHolder, BootID: *farmBoot, Endpoint: *farmEndpoint, State: actorrouting.HolderStateActive, Weight: 1}},
		Player:  actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{HolderIndex: 0, State: actorrouting.ShardStateActive, ShardRevision: playerClaim.Authority.AssignmentRevision, ExpectedWriterEpoch: playerClaim.Authority.WriterEpoch, ExpectedLeaseEpoch: playerClaim.Authority.LeaseEpoch, LeaseUntilMs: leaseUntil}}},
		Farm:    actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{HolderIndex: 1, State: actorrouting.ShardStateActive, ShardRevision: farmClaim.Authority.AssignmentRevision, ExpectedWriterEpoch: farmClaim.Authority.WriterEpoch, ExpectedLeaseEpoch: farmClaim.Authority.LeaseEpoch, LeaseUntilMs: leaseUntil}}},
	}
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return authorityReport{}, err
	}
	if err := os.WriteFile(*routeOut, append(data, '\n'), 0o600); err != nil {
		return authorityReport{}, err
	}
	info, err := cfg.client.Info(ctx, "server").Result()
	if err != nil {
		return authorityReport{}, err
	}
	version := "unknown"
	for _, line := range strings.Split(info, "\n") {
		if strings.HasPrefix(line, "redis_version:") {
			version = strings.TrimSpace(strings.TrimPrefix(line, "redis_version:"))
		}
	}
	major, _ := strconv.Atoi(strings.Split(version, ".")[0])
	if major != 7 {
		return authorityReport{}, fmt.Errorf("Redis 7.x required, got %q", version)
	}
	return authorityReport{Mode: "authority", RunID: *runID, RedisVersion: version, RedisURL: *redisURL, RealRedisProcess: true, Snapshot: snapshot}, nil
}

type loginResult struct {
	PlayerID string
	Cookie   string
}

func login(ctx context.Context, playerID uint64) (loginResult, error) {
	id := strconv.FormatUint(playerID, 10)
	mac := hmac.New(sha256.New, []byte(*loadSecret))
	_, _ = mac.Write([]byte(id))
	payload, _ := json.Marshal(map[string]string{"platform_token": "load:" + id + ":" + hex.EncodeToString(mac.Sum(nil)), "bootstrap_id": "ws-stress-" + id, "client_build": "ws-stress/1"})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(*httpURL, "/")+"/api/v1/auth/login", bytes.NewReader(payload))
	if err != nil {
		return loginResult{}, err
	}
	req.Header.Set("content-type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return loginResult{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return loginResult{}, fmt.Errorf("login player=%d status=%d body=%s", playerID, resp.StatusCode, body)
	}
	var decoded struct {
		PlayerID string `json:"player_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return loginResult{}, err
	}
	result := loginResult{PlayerID: decoded.PlayerID}
	for _, cookie := range resp.Cookies() {
		if cookie.Name == "farm_session" {
			result.Cookie = cookie.Value
		}
	}
	if result.PlayerID != id || result.Cookie == "" {
		return loginResult{}, fmt.Errorf("login identity/cookie mismatch player=%d", playerID)
	}
	return result, nil
}

func postShare(ctx context.Context, path, cookie string, payload []byte, response any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(*httpURL, "/")+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("content-type", "application/json")
	req.AddCookie(&http.Cookie{Name: "farm_session", Value: cookie})
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("%s status=%d body=%s", path, resp.StatusCode, body)
	}
	if response != nil {
		return json.NewDecoder(resp.Body).Decode(response)
	}
	return nil
}

func runPrepare(ctx context.Context, cfg commonConfig) (prepareReport, error) {
	if *loadSecret == "" {
		return prepareReport{}, errors.New("load-secret is required in prepare mode")
	}
	ownerLogin, err := login(ctx, *owner)
	if err != nil {
		return prepareReport{}, err
	}
	var share struct {
		ShareToken string `json:"share_token"`
	}
	if err := postShare(ctx, "/api/v1/share/create", ownerLogin.Cookie, nil, &share); err != nil || share.ShareToken == "" {
		return prepareReport{}, fmt.Errorf("public share create: token=%t err=%v", share.ShareToken != "", err)
	}
	repositories, err := friend.NewRedisFriendshipRepositoriesForNamespace(cfg.client, cfg.namespace)
	if err != nil {
		return prepareReport{}, err
	}
	if err := repositories.EnsureScriptsLoaded(ctx); err != nil {
		return prepareReport{}, err
	}
	type item struct {
		duration time.Duration
		err      error
	}
	results := make(chan item, *visitors)
	// CP-B's durable multi-use ShareIssuer is one Authority record. Keep fixture
	// admission concurrency explicit so a capacity test for Theft does not turn
	// into an unrelated optimistic-CAS contention test for that single issuer.
	sem := make(chan struct{}, *prepareConcurrency)
	var wg sync.WaitGroup
	for index := 1; index <= *visitors; index++ {
		visitorID := *owner + uint64(index)
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			started := time.Now()
			visitorLogin, callErr := login(ctx, visitorID)
			if callErr == nil {
				payload, _ := json.Marshal(map[string]string{"share_token": share.ShareToken})
				callErr = postShare(ctx, "/api/v1/share/accept", visitorLogin.Cookie, payload, nil)
			}
			if callErr == nil {
				pairID, deriveErr := friend.RelationIDFor(visitorID, *owner)
				if deriveErr != nil {
					callErr = deriveErr
				} else {
					hash := sha256.Sum256([]byte("theft-hot-pair:" + *runID + ":" + strconv.FormatUint(visitorID, 10)))
					op := "pair-" + *runID + "-" + strconv.FormatUint(visitorID, 10)
					outcome, activateErr := repositories.Pair.ActivateFriendRelation(ctx, friend.ActivateCommand{InviterPlayerID: *owner, RedeemerPlayerID: visitorID, TargetRelationEpoch: 1, TokenID: "theft-hot-token-" + *runID, TokenGeneration: 1, OperationID: op, NowMs: time.Now().UnixMilli()}, friend.CommitIdentity{WorkAddressID: pairID, ParticipantInvocationID: op, ParticipantStep: "ACTIVATE"}, hash[:])
					if activateErr != nil || (outcome.Status != friend.StatusSucceeded && outcome.Status != friend.StatusNoChange) {
						callErr = fmt.Errorf("pair activate player=%d outcome=%+v err=%v", visitorID, outcome, activateErr)
					}
				}
			}
			results <- item{duration: time.Since(started), err: callErr}
		}()
	}
	wg.Wait()
	close(results)
	latencies := make([]time.Duration, 0, *visitors)
	accepted := 0
	for result := range results {
		if result.err != nil {
			return prepareReport{}, result.err
		}
		accepted++
		latencies = append(latencies, result.duration)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	percentile := func(q float64) float64 {
		return float64(latencies[int(float64(len(latencies)-1)*q)]) / float64(time.Millisecond)
	}
	return prepareReport{Mode: "prepare", RunID: *runID, OwnerPlayerID: *owner, Visitors: *visitors, PublicShareCreated: true, PublicShareAccepted: accepted, AuthorityPairsActive: accepted, PairFixtureWritesOnly: true, TheftRecordsWritten: 0, AcceptP50Ms: percentile(.50), AcceptP95Ms: percentile(.95), AcceptP99Ms: percentile(.99)}, nil
}

func runAudit(ctx context.Context, cfg commonConfig) (auditReport, error) {
	if *driverJSON == "" {
		return auditReport{}, errors.New("driver-json is required in audit mode")
	}
	data, err := os.ReadFile(*driverJSON)
	if err != nil {
		return auditReport{}, err
	}
	var driver driverReport
	if err := json.Unmarshal(data, &driver); err != nil {
		return auditReport{}, err
	}
	report := auditReport{Mode: "audit", RunID: *runID, FarmID: *owner, CropCycleID: driver.Theft.Fixture.CropCycleID, Checks: make(map[string]bool)}
	violate := func(check string, ok bool, detail string) {
		report.Checks[check] = ok
		if !ok {
			report.Violations = append(report.Violations, check+": "+detail)
		}
	}
	loadedFarm, err := cfg.actorRead.Load(ctx, actorrouting.DomainFarm, *owner)
	if err != nil {
		return report, err
	}
	farm := loadedFarm.Farm
	report.FarmAuthorityRecordVersion = loadedFarm.RecordVersion
	var crop *farmpb.CropCycle
	for _, candidate := range farm.GetCore().GetCropCycles() {
		if candidate.GetCropCycleId() == report.CropCycleID {
			crop = candidate
			break
		}
	}
	if crop == nil {
		plantOperation := "theft-fixture-plant-" + driver.RunID + "-" + strconv.FormatUint(*owner, 10)
		plantWorkflowID, deriveErr := identity.WorkflowID(identity.EffectiveIdempotencyKey{
			SubjectPlayerID: *owner, StableCommandTypeName: string(identity.WorkTypePlant), OperationID: plantOperation,
			Root: identity.OperationRootRef{Kind: identity.RootKindFarmActor, CanonicalID: strconv.FormatUint(*owner, 10)},
		})
		if deriveErr != nil {
			return report, deriveErr
		}
		plantRepository, repositoryErr := workflow.NewRedisWorkRepository(cfg.client, *workflowPrefix)
		if repositoryErr != nil {
			return report, repositoryErr
		}
		plantWork, getErr := plantRepository.Get(ctx, string(identity.WorkTypePlant), plantWorkflowID)
		if getErr != nil {
			return report, fmt.Errorf("crop cycle %q not found; Plant WorkRecord read: %w", report.CropCycleID, getErr)
		}
		snapshot := actorrouting.RouteSnapshot{Realm: *realm, GlobalRevision: 1, HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: hashSeed,
			Player: actorrouting.DomainSnapshot{ShardCount: 1}, Farm: actorrouting.DomainSnapshot{ShardCount: 1}}
		placement, placementErr := plantonline.NewPlacementResolver(snapshot, plantonline.PlacementConfig{
			ProfileVersion: *profileVersion, NamespaceEnvironment: *namespaceEnv, NamespaceZone: zone,
			AuthorityLayoutVersion: "v1", WorkflowLayoutVersion: "v1", WorkflowHashVersion: 1, WorkflowBucketCount: 256,
		})
		if placementErr != nil {
			return report, placementErr
		}
		reader, readerErr := playeringress.NewOperationResultReader(playeringress.OperationResultReaderConfig{Repository: plantRepository, BucketResolver: placement, BucketVersion: 1})
		if readerErr != nil {
			return report, readerErr
		}
		lookup, lookupErr := reader.Lookup(ctx, *owner, &publicpb.GetOperationResultRequest{OriginalCommandType: commonpb.CommandType_PLANT, OriginalOperationId: plantOperation,
			OriginalTarget: &publicpb.OperationTargetLocator{Target: &publicpb.OperationTargetLocator_TargetFarmId{TargetFarmId: *owner}}})
		return report, fmt.Errorf("crop cycle %q not found; Plant WorkRecord state=%s current_step=%d last_error=%q direct_lookup=%v direct_lookup_error=%v", report.CropCycleID, plantWork.State, plantWork.CurrentStep, plantWork.LastErrorClass, lookup, lookupErr)
	}
	report.BaseYield, report.StealableLimit, report.FarmStolenTotal = crop.GetBaseYield(), crop.GetStealableLimit(), crop.GetStolenTotal()
	if report.BaseYield > 0 {
		report.ExpectedGrossPerSuccess = uint64(report.BaseYield) * 2 / 100
	}
	report.FarmVersion = farm.GetCore().GetFarmVersion()
	report.ActiveFarmSettlements = len(farm.GetActiveSettlements())
	repository, err := workflow.NewRedisWorkRepository(cfg.client, *workflowPrefix)
	if err != nil {
		return report, err
	}
	farmReceipts := make(map[string]*farmpb.FarmOperationReceipt)
	for _, receipt := range farm.GetRecentOperationReceipts() {
		if receipt != nil {
			farmReceipts[receipt.GetOperationId()] = receipt
		}
	}
	attemptTuples := make(map[string]struct{})
	for _, receipt := range farm.GetRecentTheftAttempts() {
		if receipt.GetCropCycleId() != report.CropCycleID {
			continue
		}
		report.AttemptReceiptCount++
		tuple := fmt.Sprintf("%d/%s/%d", *owner, receipt.GetCropCycleId(), receipt.GetVisitorPlayerId())
		attemptTuples[tuple] = struct{}{}
	}
	report.UniqueAttemptTuples = len(attemptTuples)
	nowMs := time.Now().UnixMilli()
	for index := 1; index <= *visitors; index++ {
		visitorID := *owner + uint64(index)
		pairID, deriveErr := identity.CanonicalFriendRelationID(visitorID, *owner)
		if deriveErr != nil {
			return report, deriveErr
		}
		operationID := "theft-hot-" + driver.RunID + "-" + strconv.FormatUint(visitorID, 10)
		workflowID, deriveErr := identity.WorkflowID(identity.EffectiveIdempotencyKey{SubjectPlayerID: visitorID, StableCommandTypeName: string(identity.WorkTypeTheft), OperationID: operationID, Root: identity.OperationRootRef{Kind: identity.RootKindFriendPair, CanonicalID: pairID}})
		if deriveErr != nil {
			return report, deriveErr
		}
		work, getErr := repository.Get(ctx, string(identity.WorkTypeTheft), workflowID)
		if getErr != nil {
			report.WorkflowOther++
			report.Violations = append(report.Violations, fmt.Sprintf("workflow missing visitor=%d: %v", visitorID, getErr))
			continue
		}
		report.WorkflowsRead++
		if !workflow.WorkTerminal(work.State) {
			report.WorkflowOther++
			if work.DeadlineAtMs > 0 && nowMs > work.DeadlineAtMs {
				report.OverdueNonterminalWorkflows++
			}
			continue
		}
		convergence := work.CompletedAtMs - work.CreatedAtMs
		if convergence > report.MaxWorkerConvergenceMs {
			report.MaxWorkerConvergenceMs = convergence
		}
		switch theftscenario.OutcomeFor(work) {
		case theftscenario.OutcomeSucceeded:
			report.WorkflowSucceeded++
		case theftscenario.OutcomeNoChange:
			report.WorkflowNoChange++
		case theftscenario.OutcomeRejected:
			report.WorkflowRejected++
		default:
			report.WorkflowOther++
		}
		if theftscenario.OutcomeFor(work) != theftscenario.OutcomeSucceeded {
			continue
		}
		report.SuccessShares++
		decisionResult, _, decodeErr := theftscenario.DecodeDecideTheftResult(work)
		if decodeErr != nil {
			report.Violations = append(report.Violations, fmt.Sprintf("decode success visitor=%d: %v", visitorID, decodeErr))
			continue
		}
		decision := decisionResult.GetTheftTargetDecision()
		if decision.GetGrossStolenQuantity() != report.ExpectedGrossPerSuccess {
			report.GrossShareMismatches++
		}
		report.GrossCommitted += decision.GetGrossStolenQuantity()
		report.NetCommitted += decision.GetNetReceivedQuantity()
		report.GuardLost += decision.GetGuardLostQuantity()
		if len(work.Steps) > 0 {
			step := work.Steps[0]
			stepHash, hashErr := participantPayloadHash(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT, step.Payload)
			receipt := farmReceipts[farmactor.ParticipantReceiptOperationKey(step.InvocationID)]
			if hashErr == nil && receipt != nil && receipt.GetStatus() == commonpb.CommandStatus_SUCCEEDED &&
				receipt.GetErrorCode() == commonpb.ErrorCode_OK && len(receipt.GetResultPayload()) != 0 &&
				bytes.Equal(receipt.GetPayloadHash(), stepHash[:]) {
				report.SucceededFarmReceipts++
			} else {
				report.ParticipantReceiptMismatch++
			}
		}
		loadedPlayer, loadErr := cfg.actorRead.Load(ctx, actorrouting.DomainPlayer, visitorID)
		if loadErr != nil {
			report.Violations = append(report.Violations, fmt.Sprintf("visitor record=%d: %v", visitorID, loadErr))
			continue
		}
		player := loadedPlayer.Player
		for _, stack := range player.GetInventory().GetStacks() {
			if stack.GetItemId() == int32(decision.GetRewardItemId()) {
				report.VisitorInventoryNet += stack.GetCount()
			}
		}
		if len(work.Steps) > 1 {
			step := work.Steps[1]
			stepHash, hashErr := participantPayloadHash(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_SETTLE_THEFT_VISITOR, step.Payload)
			matched := false
			for _, receipt := range player.GetRecentOperationReceipts() {
				if hashErr == nil && receipt.GetOperationId() == playeringress.ParticipantReceiptOperationKey(step.InvocationID) &&
					receipt.GetStatus() == commonpb.CommandStatus_SUCCEEDED &&
					receipt.GetErrorCode() == commonpb.ErrorCode_OK && len(receipt.GetResultPayload()) != 0 &&
					bytes.Equal(receipt.GetPayloadHash(), stepHash[:]) {
					report.SucceededVisitorReceipts++
					matched = true
					break
				}
			}
			if !matched {
				report.ParticipantReceiptMismatch++
			}
		}
	}
	expectedHealthy := 200
	if driver.Theft.SlowReaderEnabled {
		expectedHealthy = 199
	}
	report.HealthyConnectionsAudited = len(driver.Theft.HealthyVersions)
	report.UnexpectedHealthyDisconnect = driver.UnexpectedEnd
	contiguousClients := true
	for _, audit := range driver.Theft.HealthyVersions {
		if audit.Gap || audit.Rollback || len(audit.Versions) != 10 || audit.Last != audit.Baseline+10 {
			contiguousClients = false
		}
		for i, version := range audit.Versions {
			if version != audit.Baseline+uint64(i)+1 {
				contiguousClients = false
			}
		}
	}
	violate("200_subscription_ready", driver.ConfiguredConns == 200 && driver.Ready == 200 && driver.Theft.ReadyBarrier == 200 && driver.PeakActive == 200 && driver.JoinErrors == 0, fmt.Sprintf("ready=%d barrier=%d peak=%d join_errors=%d", driver.Ready, driver.Theft.ReadyBarrier, driver.PeakActive, driver.JoinErrors))
	violate("exactly_10_success_shares", report.SuccessShares == 10 && report.WorkflowSucceeded == 10, fmt.Sprintf("shares=%d succeeded=%d", report.SuccessShares, report.WorkflowSucceeded))
	violate("each_success_is_2_percent_gross", report.ExpectedGrossPerSuccess > 0 && report.GrossShareMismatches == 0 && report.GrossCommitted == report.ExpectedGrossPerSuccess*uint64(report.SuccessShares), fmt.Sprintf("expected_each=%d mismatches=%d gross=%d", report.ExpectedGrossPerSuccess, report.GrossShareMismatches, report.GrossCommitted))
	violate("remaining_attempts_terminal", report.WorkflowsRead == 199 && report.WorkflowOther == 0 && report.WorkflowSucceeded+report.WorkflowNoChange+report.WorkflowRejected == 199, fmt.Sprintf("read=%d succeeded=%d no_change=%d rejected=%d other=%d", report.WorkflowsRead, report.WorkflowSucceeded, report.WorkflowNoChange, report.WorkflowRejected, report.WorkflowOther))
	violate("operation_results_terminal_queryable", driver.Theft.OperationQueries >= 199 && driver.Theft.TerminalSucceeded+driver.Theft.TerminalNoChange+driver.Theft.TerminalRejected == 199 && driver.Theft.TerminalErrors == 0, fmt.Sprintf("queries=%d processing=%d unavailable=%d succeeded=%d no_change=%d rejected=%d errors=%d", driver.Theft.OperationQueries, driver.Theft.ProcessingQueries, driver.Theft.UnavailableQueries, driver.Theft.TerminalSucceeded, driver.Theft.TerminalNoChange, driver.Theft.TerminalRejected, driver.Theft.TerminalErrors))
	violate("gross_is_exactly_20_percent", report.BaseYield > 0 && report.GrossCommitted == uint64(report.BaseYield)/5, fmt.Sprintf("base=%d gross=%d", report.BaseYield, report.GrossCommitted))
	violate("farm_gross_deduction_matches", report.FarmStolenTotal == int32(report.GrossCommitted), fmt.Sprintf("farm_stolen=%d committed=%d", report.FarmStolenTotal, report.GrossCommitted))
	violate("attempt_receipt_unique", report.AttemptReceiptCount == 10 && report.UniqueAttemptTuples == 10, fmt.Sprintf("receipts=%d unique=%d", report.AttemptReceiptCount, report.UniqueAttemptTuples))
	violate("visitor_net_matches_receipts", report.VisitorInventoryNet == int64(report.NetCommitted), fmt.Sprintf("inventory=%d net=%d", report.VisitorInventoryNet, report.NetCommitted))
	violate("pet_compensation_zero", report.GuardLost == 0 && report.GuardCompensation == 0, fmt.Sprintf("guard_lost=%d compensation=%d", report.GuardLost, report.GuardCompensation))
	violate("succeeded_participant_receipts_present", report.SucceededFarmReceipts == 10 && report.SucceededVisitorReceipts == 10 && report.ParticipantReceiptMismatch == 0, fmt.Sprintf("farm=%d visitor=%d mismatches=%d", report.SucceededFarmReceipts, report.SucceededVisitorReceipts, report.ParticipantReceiptMismatch))
	violate("no_duplicate_or_pending_settlement", report.ActiveFarmSettlements == 0 && report.AttemptReceiptCount == report.SuccessShares, fmt.Sprintf("active_settlements=%d attempt_receipts=%d success=%d", report.ActiveFarmSettlements, report.AttemptReceiptCount, report.SuccessShares))
	violate("workflow_recovery_window_clear", report.OverdueNonterminalWorkflows == 0 && report.WorkflowOther == 0, fmt.Sprintf("overdue=%d other=%d", report.OverdueNonterminalWorkflows, report.WorkflowOther))
	violate("farm_change_monotonic_no_gaps", contiguousClients && driver.Theft.VersionGaps == 0 && driver.Theft.VersionRegressions == 0, fmt.Sprintf("clients=%d gaps=%d regressions=%d", len(driver.Theft.HealthyVersions), driver.Theft.VersionGaps, driver.Theft.VersionRegressions))
	violate("healthy_connections_intact", report.HealthyConnectionsAudited == expectedHealthy && driver.Theft.HealthyCompleted == expectedHealthy && driver.UnexpectedEnd == 0 && driver.Theft.Rejoin == 0, fmt.Sprintf("audited=%d completed=%d disconnect=%d rejoin=%d", report.HealthyConnectionsAudited, driver.Theft.HealthyCompleted, driver.UnexpectedEnd, driver.Theft.Rejoin))
	violate("operation_id_retry_idempotent", driver.Theft.RetryErrors == 0 && report.SuccessShares == 10 && report.AttemptReceiptCount == 10, fmt.Sprintf("retry_errors=%d", driver.Theft.RetryErrors))
	return report, nil
}

func participantPayloadHash(command contractspb.ParticipantCommandType, payload []byte) ([32]byte, error) {
	spec, err := contracts.LookupParticipantCommand(command)
	if err != nil {
		return [32]byte{}, err
	}
	message := spec.NewPayload()
	if err := proto.Unmarshal(payload, message); err != nil {
		return [32]byte{}, err
	}
	return contracts.CanonicalParticipantHash(spec, message)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "theft-hot-audit:", err)
	os.Exit(1)
}
