// Command authority-smoke-fixture provides init and audit subcommands for
// Authority + Static Assignment vertical smoke tests.
//
// init: Claim ShardWriteAuthority for player/farm domains and generate
// the matching route-snapshot.json.
//
// audit: Read authority state from Redis and produce a verdict summary.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	farmpb "farmgame/api/gen/farm"
	playerpb "farmgame/api/gen/player"
	"farmgame/internal/actormodel/fieldv2"
	"farmgame/internal/actorrouting"
	"farmgame/internal/cropcatalog"
	farmactormodelv2 "farmgame/internal/farm/actormodelv2"
	farmdomain "farmgame/internal/farm/domain"
	"farmgame/internal/friend"
	logicauthority "farmgame/internal/logic/actor/authority"
	playeractormodelv2 "farmgame/internal/player/actormodelv2"
	playertask "farmgame/internal/player/task"
	"farmgame/internal/storage/authoritystore"
	redisauthority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: authority-smoke-fixture <init|audit> [flags]\n")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "init":
		runInit(os.Args[2:])
	case "audit":
		runAudit(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q; expected init or audit\n", os.Args[1])
		os.Exit(2)
	}
}

// ---------------------------------------------------------------------------
// init
// ---------------------------------------------------------------------------

func runInit(args []string) {
	fs := flag.NewFlagSet("init", flag.ExitOnError)
	redisURL := fs.String("redis-url", "", "Redis connection URL")
	namespaceEnv := fs.String("namespace", "smoke-a", "Redis namespace environment")
	namespaceZone := fs.String("zone", "local", "Redis namespace zone")
	profile := fs.String("profile", "redis-physical-v1", "Redis physical profile version")
	realm := fs.String("realm", "smoke-premerge-a", "Route realm")
	hashSeed := fs.Uint64("hash-seed", 202608060001, "Route hash seed")
	runID := fs.String("run-id", "", "Unique run identifier")
	playerHolder := fs.String("player-holder", "smoke-player", "Player holder instance ID")
	playerBoot := fs.String("player-boot", "", "Player holder boot ID")
	playerEndpoint := fs.String("player-endpoint", "127.0.0.1:19600", "Player holder endpoint")
	farmHolder := fs.String("farm-holder", "smoke-farm", "Farm holder instance ID")
	farmBoot := fs.String("farm-boot", "", "Farm holder boot ID")
	farmEndpoint := fs.String("farm-endpoint", "127.0.0.1:19500", "Farm holder endpoint")
	routeOut := fs.String("route-out", "", "Output path for route-snapshot.json")
	jsonOut := fs.String("json-out", "", "Output path for fixture.json")
	_ = fs.Parse(args)

	if *redisURL == "" {
		*redisURL = strings.TrimSpace(os.Getenv("FARM_REDIS_URL"))
	}
	if *redisURL == "" || *routeOut == "" {
		log.Fatal("FARM_REDIS_URL/--redis-url and --route-out are required")
	}
	if *runID == "" {
		*runID = fmt.Sprintf("%d", time.Now().UnixNano())
	}
	if *playerBoot == "" {
		*playerBoot = *playerHolder + "-boot-" + *runID
	}
	if *farmBoot == "" {
		*farmBoot = *farmHolder + "-boot-" + *runID
	}

	client, err := connectRedis(*redisURL)
	if err != nil {
		log.Fatalf("connect redis: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	physicalProfile := buildProfile(*namespaceEnv, *namespaceZone, *profile)
	calculator, err := actorrouting.NewCalculator(*realm, actorrouting.HashProfile{
		Version: actorrouting.HashVersionXXH64V2, Seed: *hashSeed,
	}, 1, 1)
	if err != nil {
		log.Fatalf("build calculator: %v", err)
	}

	store := buildStore(ctx, client, physicalProfile, calculator)

	playerClaim := claimShard(ctx, store, actorrouting.DomainPlayer, *playerHolder, *playerBoot, *runID)
	farmClaim := claimShard(ctx, store, actorrouting.DomainFarm, *farmHolder, *farmBoot, *runID)

	leaseUntil := playerClaim.Authority.LeaseUntilMs
	if farmClaim.Authority.LeaseUntilMs < leaseUntil {
		leaseUntil = farmClaim.Authority.LeaseUntilMs
	}
	// The generated file is a local routing cache, not the write grant. Keep its
	// endpoint assignment usable for the whole E2E run while the holder still
	// renews and proves the real short lease in Redis on every authority write.
	leaseUntil = time.Now().Add(30 * time.Minute).UnixMilli()

	snapshot := buildRouteSnapshot(*realm, *hashSeed,
		*playerHolder, *playerBoot, *playerEndpoint, playerClaim,
		*farmHolder, *farmBoot, *farmEndpoint, farmClaim, leaseUntil)

	writeJSON(*routeOut, snapshot)
	log.Printf("route snapshot written: %s", *routeOut)

	if *jsonOut != "" {
		fixture := map[string]any{
			"run_id": *runID, "realm": *realm,
			"namespace": *namespaceEnv, "zone": *namespaceZone, "profile": *profile,
			"player_holder": *playerHolder, "player_boot": *playerBoot,
			"farm_holder": *farmHolder, "farm_boot": *farmBoot,
			"player_writer_epoch":        playerClaim.Authority.WriterEpoch,
			"farm_writer_epoch":          farmClaim.Authority.WriterEpoch,
			"player_assignment_revision": playerClaim.Authority.AssignmentRevision,
			"farm_assignment_revision":   farmClaim.Authority.AssignmentRevision,
			"lease_until_ms":             leaseUntil,
		}
		writeJSON(*jsonOut, fixture)
		log.Printf("fixture written: %s", *jsonOut)
	}
}

// ---------------------------------------------------------------------------
// audit
// ---------------------------------------------------------------------------

func runAudit(args []string) {
	fs := flag.NewFlagSet("audit", flag.ExitOnError)
	redisURL := fs.String("redis-url", "", "Redis connection URL")
	namespaceEnv := fs.String("namespace", "smoke-a", "Redis namespace environment")
	namespaceZone := fs.String("zone", "local", "Redis namespace zone")
	profile := fs.String("profile", "redis-physical-v1", "Redis physical profile version")
	routePath := fs.String("route", "", "Path to route-snapshot.json")
	protocolVerdict := fs.String("protocol-verdict", "", "Path to protocol-verdict.json")
	browserVerdict := fs.String("browser-verdict", "", "Path to browser-verdict.json for business authority oracle")
	jsonOut := fs.String("json-out", "", "Output path for authority-after.json")
	_ = fs.Parse(args)

	if *redisURL == "" || *routePath == "" {
		log.Fatal("--redis-url and --route are required")
	}

	client, err := connectRedis(*redisURL)
	if err != nil {
		log.Fatalf("connect redis: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	physicalProfile := buildProfile(*namespaceEnv, *namespaceZone, *profile)

	// Read route snapshot to get holder/boot info.
	var snapshot actorrouting.RouteSnapshot
	routeData, err := os.ReadFile(*routePath)
	if err != nil {
		log.Fatalf("read route snapshot: %v", err)
	}
	if err := json.Unmarshal(routeData, &snapshot); err != nil {
		log.Fatalf("parse route snapshot: %v", err)
	}

	// Read ShardWriteAuthority for player and farm domains.
	playerAuth := readShardAuthority(ctx, client, physicalProfile, &snapshot, actorrouting.DomainPlayer)
	farmAuth := readShardAuthority(ctx, client, physicalProfile, &snapshot, actorrouting.DomainFarm)

	// Read protocol verdict if provided.
	var protocolResult map[string]any
	if *protocolVerdict != "" {
		if data, err := os.ReadFile(*protocolVerdict); err == nil {
			_ = json.Unmarshal(data, &protocolResult)
		}
	}

	businessOracle := auditSocialAuthority(ctx, client, physicalProfile, &snapshot, *browserVerdict)
	audit := map[string]any{
		"authority": map[string]any{
			"player": playerAuth,
			"farm":   farmAuth,
		},
		"route_snapshot_sha256": fmt.Sprintf("%x", len(routeData)),
		"protocol_verdict":      protocolResult,
		"business_oracle":       businessOracle,
		"audited_at":            time.Now().UTC().Format(time.RFC3339),
	}

	if *jsonOut != "" {
		writeJSON(*jsonOut, audit)
		log.Printf("audit written: %s", *jsonOut)
	} else {
		data, _ := json.MarshalIndent(audit, "", "  ")
		fmt.Println(string(data))
	}
}

type socialBrowserVerdict struct {
	Verdict string         `json:"verdict"`
	Details map[string]any `json:"details"`
}

func auditSocialAuthority(ctx context.Context, client *redis.Client, profile rediskey.PhysicalProfile,
	snapshot *actorrouting.RouteSnapshot, browserPath string) map[string]any {
	report := map[string]any{"verdict": "INCONCLUSIVE", "checks": map[string]bool{}, "mismatches": []string{}}
	if browserPath == "" || snapshot == nil {
		report["reason"] = "browser verdict not provided"
		return report
	}
	data, err := os.ReadFile(browserPath)
	if err != nil {
		report["reason"] = err.Error()
		return report
	}
	var browser socialBrowserVerdict
	if err := json.Unmarshal(data, &browser); err != nil || browser.Verdict != "PASS" {
		report["reason"] = "browser verdict missing, malformed, or not PASS"
		return report
	}
	readUint := func(name string) (uint64, bool) {
		value := browser.Details[name]
		switch typed := value.(type) {
		case string:
			parsed, parseErr := strconv.ParseUint(typed, 10, 64)
			return parsed, parseErr == nil
		case float64:
			if typed >= 0 && typed == float64(uint64(typed)) {
				return uint64(typed), true
			}
		}
		return 0, false
	}
	ownerA, okA := readUint("ownerA")
	ownerB, okB := readUint("ownerB")
	relationEpoch, okEpoch := readUint("relationEpoch")
	browserFarmVersion, okFarmVersion := readUint("bFarmVersionAfterTheft")
	browserPlotIndex, okPlot := readUint("bTheftPlotIndex")
	browserStolen, okStolen := readUint("bStolenTotal")
	browserProduce, okProduce := readUint("aFinalProduce")
	browserPlayerRevision, okPlayerRevision := readUint("aPlayerStateRevision")
	if !okA || !okB || !okEpoch || !okFarmVersion || !okPlot || !okStolen || !okProduce || !okPlayerRevision {
		report["reason"] = "browser authority details incomplete"
		return report
	}
	calculator, err := actorrouting.NewCalculator(snapshot.Realm,
		actorrouting.HashProfile{Version: snapshot.HashVersion, Seed: snapshot.HashSeed},
		snapshot.Player.ShardCount, snapshot.Farm.ShardCount)
	if err != nil {
		report["reason"] = err.Error()
		return report
	}
	store := buildStore(ctx, client, profile, calculator)
	farmFields, err := auditReadFieldNames(actorrouting.DomainFarm)
	if err != nil {
		report["reason"] = err.Error()
		return report
	}
	farmRows, err := store.BatchReadModelFields(ctx, actorrouting.DomainFarm, []uint64{ownerB}, farmFields)
	if err != nil || len(farmRows) != 1 || farmRows[0].Err != nil || !farmRows[0].Found {
		report["reason"] = modelReadFailure("farm", err, farmRows)
		return report
	}
	farmState, err := decodeAuditFarmRow(farmRows[0], ownerB)
	if err != nil {
		report["reason"] = fmt.Sprintf("farm authority row invalid: %v", err)
		return report
	}
	playerFields, err := auditReadFieldNames(actorrouting.DomainPlayer)
	if err != nil {
		report["reason"] = err.Error()
		return report
	}
	playerRows, err := store.BatchReadModelFields(ctx, actorrouting.DomainPlayer, []uint64{ownerA}, playerFields)
	if err != nil || len(playerRows) != 1 || playerRows[0].Err != nil || !playerRows[0].Found {
		report["reason"] = modelReadFailure("player", err, playerRows)
		return report
	}
	playerRecord, err := decodeAuditPlayerRow(playerRows[0], ownerA)
	if err != nil {
		report["reason"] = fmt.Sprintf("player authority row invalid: %v", err)
		return report
	}
	namespace := profile.Namespace
	repositories, err := friend.NewRedisFriendshipRepositoriesForNamespace(client, namespace)
	if err != nil {
		report["reason"] = err.Error()
		return report
	}
	pair, err := repositories.Pair.GetPair(ctx, ownerA, ownerB)
	if err != nil {
		report["reason"] = err.Error()
		return report
	}
	indexA, errA := repositories.Index.Scan(ctx, ownerA, "", friend.MaxPageSize*3)
	indexB, errB := repositories.Index.Scan(ctx, ownerB, "", friend.MaxPageSize*3)
	visitedA, visitedErrA := repositories.Navigation.ListVisited(ctx, ownerA)
	visitedB, visitedErrB := repositories.Navigation.ListVisited(ctx, ownerB)
	values := deriveAuditBusinessValues(playerRecord, farmState.Record, browserPlotIndex)
	taskState := playerRecord.GetTaskDomainState()
	containsIndex := func(scan friend.FriendIndexScan, peer uint64) bool {
		for _, entry := range scan.Entries {
			if entry.FriendPlayerID == peer && entry.RelationID == pair.Relation.RelationID && entry.RelationEpoch == pair.Relation.RelationEpoch {
				return true
			}
		}
		return false
	}
	checks := map[string]bool{
		"pair_active_epoch":  pair.Relation.Status == friend.RelationStatusActive && pair.Relation.RelationEpoch == relationEpoch && !pair.HasActiveBlock(),
		"index_a":            errA == nil && containsIndex(indexA, ownerB),
		"index_b":            errB == nil && containsIndex(indexB, ownerA),
		"ready_visit_a_to_b": visitedErrA == nil && hasVisited(visitedA, ownerB),
		"ready_visit_b_to_a": visitedErrB == nil && hasVisited(visitedB, ownerA),
		"farm_version":       values.FarmVersion == browserFarmVersion,
		"theft_total":        values.StolenTotal >= 0 && uint64(values.StolenTotal) == browserStolen,
		"player_revision":    playerRows[0].RecordVersion == browserPlayerRevision,
		"player_produce":     values.PlayerProduce >= 0 && uint64(values.PlayerProduce) == browserProduce,
		"opportunity_models_present": farmState.Header.SchemaVersion == farmdomain.FarmOpportunityHeaderSchemaV2 &&
			farmState.Facts.SchemaVersion == farmdomain.FarmOpportunityFactsSchemaV1 &&
			farmState.Header.SourceFarmVersion == values.FarmVersion && farmState.Facts.SourceFarmVersion == values.FarmVersion,
		"task_daily_growth_claimed": values.AllDailyClaimed && taskState.GetCurrentGrowth() == nil && values.TaskGrowthCursor == 1 &&
			values.TaskClaimableCount == 0 && values.TaskTotalHarvest >= 1,
	}
	mismatches := make([]string, 0)
	for name, passed := range checks {
		if !passed {
			mismatches = append(mismatches, name)
		}
	}
	report["checks"] = checks
	report["mismatches"] = mismatches
	report["authority"] = map[string]any{
		"relation_id": pair.Relation.RelationID, "relation_epoch": pair.Relation.RelationEpoch,
		"farm_model_layout": farmRows[0].ModelLayout.String(), "player_model_layout": playerRows[0].ModelLayout.String(),
		"farm_version": values.FarmVersion, "stolen_total": values.StolenTotal,
		"opportunity_header_source_farm_version": farmState.Header.SourceFarmVersion,
		"opportunity_facts_source_farm_version":  farmState.Facts.SourceFarmVersion,
		"player_revision":                        playerRows[0].RecordVersion, "player_economy_version": playerRecord.GetEconomyVersion(), "player_produce": values.PlayerProduce,
		"task_state_version": values.TaskStateVersion, "task_claimable_count": values.TaskClaimableCount,
		"task_growth_cursor": values.TaskGrowthCursor, "task_total_harvest": values.TaskTotalHarvest,
	}
	report["browser"] = browser.Details
	if len(mismatches) == 0 {
		report["verdict"] = "PASS"
	} else {
		report["verdict"] = "FAIL"
	}
	return report
}

type auditFarmState struct {
	Record *farmpb.FarmRecord
	Header farmdomain.FarmOpportunityHeaderV2
	Facts  farmdomain.FarmOpportunityFactsV1
}

type auditBusinessValues struct {
	FarmVersion        uint64
	StolenTotal        int32
	PlayerProduce      int64
	TaskStateVersion   uint64
	TaskClaimableCount int32
	TaskGrowthCursor   uint32
	TaskTotalHarvest   int64
	AllDailyClaimed    bool
}

func auditReadFieldNames(domain actorrouting.RouteDomain) ([]string, error) {
	names, err := fieldv2.ExactNames(domain)
	if err != nil {
		return nil, fmt.Errorf("audit model schema: %w", err)
	}
	return append([]string{logicauthority.RecordModelField}, names...), nil
}

func modelReadFailure(domain string, batchErr error, rows []authoritystore.ModelFieldReadResult) string {
	if batchErr != nil {
		return fmt.Sprintf("%s authority row unavailable: %v", domain, batchErr)
	}
	if len(rows) == 1 && rows[0].Err != nil {
		return fmt.Sprintf("%s authority row invalid: %v", domain, rows[0].Err)
	}
	return domain + " authority row unavailable"
}

func decodeAuditPlayerRow(row authoritystore.ModelFieldReadResult, playerID uint64) (*playerpb.PlayerRecord, error) {
	if !row.Found || row.ActorID != playerID || playerID == 0 {
		return nil, fmt.Errorf("player identity/read state")
	}
	var record *playerpb.PlayerRecord
	switch row.ModelLayout {
	case authoritystore.ModelLayoutV1:
		names, _ := fieldv2.ExactNames(actorrouting.DomainPlayer)
		for _, name := range names {
			if _, mixed := row.ModelFields[name]; mixed {
				return nil, fmt.Errorf("mixed-invalid v1/v2 player fields")
			}
		}
		data, ok := row.ModelFields[logicauthority.RecordModelField]
		if !ok || len(data) == 0 {
			return nil, fmt.Errorf("v1 player record missing")
		}
		decoded := &playerpb.PlayerRecord{}
		if err := proto.Unmarshal(data, decoded); err != nil {
			return nil, fmt.Errorf("decode v1 player record: %w", err)
		}
		record = decoded
	case authoritystore.ModelLayoutV2:
		decoded, err := playeractormodelv2.Decode(row.ModelFields)
		if err != nil {
			return nil, fmt.Errorf("decode v2 player fields: %w", err)
		}
		record = decoded
	default:
		return nil, fmt.Errorf("unknown player model layout %s", row.ModelLayout)
	}
	if record.GetPlayerId() != playerID {
		return nil, fmt.Errorf("player id mismatch")
	}
	return record, nil
}

func decodeAuditFarmRow(row authoritystore.ModelFieldReadResult, farmID uint64) (auditFarmState, error) {
	if !row.Found || row.ActorID != farmID || farmID == 0 {
		return auditFarmState{}, fmt.Errorf("farm identity/read state")
	}
	var record *farmpb.FarmRecord
	var headerBytes, factsBytes []byte
	switch row.ModelLayout {
	case authoritystore.ModelLayoutV1:
		// These two compact projections already existed beside the v1 record.
		// Any other v2 core field makes the layout mixed-invalid.
		names, _ := fieldv2.ExactNames(actorrouting.DomainFarm)
		for _, name := range names {
			if name == farmdomain.FarmOpportunityHeaderModelField || name == farmdomain.FarmOpportunityFactsModelField {
				continue
			}
			if _, mixed := row.ModelFields[name]; mixed {
				return auditFarmState{}, fmt.Errorf("mixed-invalid v1/v2 farm fields")
			}
		}
		data, ok := row.ModelFields[logicauthority.RecordModelField]
		if !ok || len(data) == 0 {
			return auditFarmState{}, fmt.Errorf("v1 farm record missing")
		}
		decoded := &farmpb.FarmRecord{}
		if err := proto.Unmarshal(data, decoded); err != nil {
			return auditFarmState{}, fmt.Errorf("decode v1 farm record: %w", err)
		}
		record = decoded
		headerBytes = row.ModelFields[farmdomain.FarmOpportunityHeaderModelField]
		factsBytes = row.ModelFields[farmdomain.FarmOpportunityFactsModelField]
	case authoritystore.ModelLayoutV2:
		decoded, header, facts, err := farmactormodelv2.Decode(row.ModelFields)
		if err != nil {
			return auditFarmState{}, fmt.Errorf("decode v2 farm fields: %w", err)
		}
		record, headerBytes, factsBytes = decoded, header, facts
	default:
		return auditFarmState{}, fmt.Errorf("unknown farm model layout %s", row.ModelLayout)
	}
	if record.GetFarmId() != farmID || record.GetCore() == nil || record.GetCore().GetFarmId() != farmID {
		return auditFarmState{}, fmt.Errorf("farm id mismatch")
	}
	header, err := farmdomain.UnmarshalFarmOpportunityHeaderV2(headerBytes)
	if err != nil {
		return auditFarmState{}, fmt.Errorf("decode opportunity header: %w", err)
	}
	facts, err := farmdomain.UnmarshalFarmOpportunityFactsV1(factsBytes)
	if err != nil {
		return auditFarmState{}, fmt.Errorf("decode opportunity facts: %w", err)
	}
	version := record.GetCore().GetFarmVersion()
	if header.SourceFarmVersion != version || facts.SourceFarmVersion != version {
		return auditFarmState{}, fmt.Errorf("opportunity models do not match farm version")
	}
	return auditFarmState{Record: record, Header: header, Facts: facts}, nil
}

func deriveAuditBusinessValues(player *playerpb.PlayerRecord, farm *farmpb.FarmRecord, plotIndex uint64) auditBusinessValues {
	values := auditBusinessValues{StolenTotal: -1}
	if player != nil {
		for _, stack := range player.GetInventory().GetStacks() {
			if stack.GetItemId() == cropcatalog.CarrotItemID {
				values.PlayerProduce = stack.GetCount()
			}
		}
		taskState := player.GetTaskDomainState()
		values.TaskStateVersion = taskState.GetTaskStateVersion()
		values.TaskClaimableCount = taskState.GetClaimableCount()
		values.TaskGrowthCursor = taskState.GetGrowthCursor()
		values.TaskTotalHarvest = taskState.GetTotalHarvestCount()
		values.AllDailyClaimed = taskState != nil && len(taskState.GetDailyTasks()) == playertask.DailySlotCount
		if values.AllDailyClaimed {
			for _, instance := range taskState.GetDailyTasks() {
				if instance == nil || playertask.State(instance.GetState()) != playertask.StateClaimed {
					values.AllDailyClaimed = false
					break
				}
			}
		}
	}
	if farm != nil {
		values.FarmVersion = farm.GetCore().GetFarmVersion()
		for _, cycle := range farm.GetCore().GetCropCycles() {
			if uint64(cycle.GetPlotIndex()) == plotIndex {
				values.StolenTotal = cycle.GetStolenTotal()
			}
		}
	}
	return values
}

func hasVisited(visited map[uint64]struct{}, owner uint64) bool {
	_, ok := visited[owner]
	return ok
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func connectRedis(url string) (*redis.Client, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	return redis.NewClient(opt), nil
}

func buildProfile(nsEnv, nsZone, profileVersion string) rediskey.PhysicalProfile {
	namespace, err := rediskey.NewNamespace(nsEnv, nsZone)
	if err != nil {
		log.Fatalf("build namespace: %v", err)
	}
	return rediskey.PhysicalProfile{
		ProfileVersion:         profileVersion,
		Namespace:              namespace,
		AuthorityLayoutVersion: "v1",
		WorkflowLayoutVersion:  "v1",
		WorkflowHashVersion:    1,
		WorkflowBucketCount:    256,
	}
}

func buildStore(ctx context.Context, client *redis.Client, profile rediskey.PhysicalProfile, calculator *actorrouting.Calculator) *redisauthority.Store {
	scripts, err := redisauthority.NewScriptRegistry(client)
	if err != nil {
		log.Fatalf("build script registry: %v", err)
	}
	if err := scripts.EnsureLoaded(ctx); err != nil {
		log.Fatalf("load scripts: %v", err)
	}
	store, err := redisauthority.NewStore(client, scripts, redisauthority.Options{
		Profile: profile, Resolver: calculator, LeaseLimits: authoritystore.DefaultLeaseLimits(),
	})
	if err != nil {
		log.Fatalf("build authority store: %v", err)
	}
	return store
}

func claimShard(ctx context.Context, store *redisauthority.Store, domain actorrouting.RouteDomain, holder, boot, runID string) authoritystore.ShardAuthorityTransitionResult {
	result, err := store.ClaimShardAuthority(ctx, authoritystore.ClaimShardAuthorityRequest{
		Caller: authoritystore.TrustedShardAuthorityCaller{
			Kind:       authoritystore.ShardAuthorityCallerRouteManager,
			InstanceID: "smoke-route-manager",
			BootID:     "smoke-route-manager-" + runID,
		},
		TransitionID:           fmt.Sprintf("claim-%s-%s", domain, runID),
		AuthorityLayoutVersion: "v1",
		Domain:                 domain,
		VirtualShard:           0,
		NewHolderID:            holder,
		NewHolderBootID:        boot,
		RequestedTTLms:         (55 * time.Second).Milliseconds(),
	})
	if err != nil {
		log.Fatalf("claim %s shard: %v", domain, err)
	}
	if result.Status != authoritystore.ShardTransitionApplied {
		log.Fatalf("claim %s shard: unexpected status %s", domain, result.Status)
	}
	log.Printf("claimed %s shard 0: holder=%s epoch=%d assignment_rev=%d",
		domain, holder, result.Authority.WriterEpoch, result.Authority.AssignmentRevision)
	return result
}

func buildRouteSnapshot(realm string, hashSeed uint64,
	pHolder, pBoot, pEndpoint string, pClaim authoritystore.ShardAuthorityTransitionResult,
	fHolder, fBoot, fEndpoint string, fClaim authoritystore.ShardAuthorityTransitionResult,
	leaseUntil int64,
) actorrouting.RouteSnapshot {
	return actorrouting.RouteSnapshot{
		Realm: realm, GlobalRevision: 1,
		HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: hashSeed,
		Holders: []actorrouting.HolderRef{
			{InstanceID: pHolder, BootID: pBoot, Endpoint: pEndpoint, State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: fHolder, BootID: fBoot, Endpoint: fEndpoint, State: actorrouting.HolderStateActive, Weight: 1},
		},
		Player: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{
			HolderIndex: 0, State: actorrouting.ShardStateActive,
			ShardRevision: pClaim.Authority.AssignmentRevision, ExpectedWriterEpoch: pClaim.Authority.WriterEpoch,
			ExpectedLeaseEpoch: pClaim.Authority.LeaseEpoch, LeaseUntilMs: leaseUntil,
		}}},
		Farm: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{
			HolderIndex: 1, State: actorrouting.ShardStateActive,
			ShardRevision: fClaim.Authority.AssignmentRevision, ExpectedWriterEpoch: fClaim.Authority.WriterEpoch,
			ExpectedLeaseEpoch: fClaim.Authority.LeaseEpoch, LeaseUntilMs: leaseUntil,
		}}},
	}
}

func readShardAuthority(ctx context.Context, client *redis.Client, profile rediskey.PhysicalProfile, snapshot *actorrouting.RouteSnapshot, domain actorrouting.RouteDomain) map[string]any {
	calculator, err := actorrouting.NewCalculator(snapshot.Realm, actorrouting.HashProfile{
		Version: snapshot.HashVersion, Seed: snapshot.HashSeed,
	}, 1, 1)
	if err != nil {
		return map[string]any{"error": err.Error()}
	}
	keys, err := rediskey.NewAuthorityKeys(profile, calculator)
	if err != nil {
		return map[string]any{"error": err.Error()}
	}
	bucket, err := profile.AuthorityBucketForShard(calculator, domain, 0)
	if err != nil {
		return map[string]any{"error": err.Error()}
	}
	key, err := keys.ShardAuthority(bucket)
	if err != nil {
		return map[string]any{"error": err.Error()}
	}
	fields, err := client.HGetAll(ctx, key.String()).Result()
	if err != nil {
		return map[string]any{"error": err.Error()}
	}
	return map[string]any{
		"redis_key": key.String(),
		"fields":    fields,
	}
}

func writeJSON(path string, v any) {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		log.Fatalf("marshal %s: %v", path, err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o600); err != nil {
		log.Fatalf("write %s: %v", path, err)
	}
}
