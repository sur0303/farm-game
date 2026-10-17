// Command lease-real-fixture composes the Redis Authority route used by the
// R1-Q1 real Gate lease evidence. It owns no business behavior: it invokes the
// production Redis ShardWriteAuthority store, then emits a static route with
// one Player shard and two Farm shards assigned to two real Farm holders.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
	redisauthority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"

	"github.com/redis/go-redis/v9"
)

const fixtureHashSeed = uint64(0x6c65617365523134)

type config struct {
	redisURL       string
	namespace      string
	profile        string
	realm          string
	runID          string
	routeOut       string
	reportOut      string
	playerHolder   string
	playerBoot     string
	playerEndpoint string
	farmAHolder    string
	farmABoot      string
	farmAEndpoint  string
	farmBHolder    string
	farmBBoot      string
	farmBEndpoint  string
	leaseTTL       time.Duration
	playerStart    uint64
}

type claimEvidence struct {
	Domain             string `json:"domain"`
	Shard              uint32 `json:"shard"`
	Holder             string `json:"holder"`
	Boot               string `json:"boot"`
	Status             string `json:"status"`
	AssignmentRevision uint64 `json:"assignment_revision"`
	WriterEpoch        uint64 `json:"writer_epoch"`
	LeaseEpoch         uint64 `json:"lease_epoch"`
	LeaseUntilMS       int64  `json:"lease_until_ms"`
}

type report struct {
	SchemaVersion  string            `json:"schema_version"`
	Status         string            `json:"status"`
	AuthorityMode  string            `json:"authority_mode"`
	MembershipMode string            `json:"membership_mode"`
	PlayerShards   uint32            `json:"player_shards"`
	FarmShards     uint32            `json:"farm_shards"`
	Claims         []claimEvidence   `json:"claims"`
	FarmIDs        map[string]uint64 `json:"farm_ids"`
	Checks         map[string]bool   `json:"checks"`
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	cfg := parseFlags()
	if err := run(cfg); err != nil {
		log.Printf("lease-real-fixture FAIL: %v", err)
		os.Exit(1)
	}
}

func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.redisURL, "redis-url", os.Getenv("FARM_REDIS_URL"), "real Redis URL (or FARM_REDIS_URL; never written to evidence)")
	flag.StringVar(&cfg.namespace, "namespace", "", "unique Redis namespace environment")
	flag.StringVar(&cfg.profile, "profile", "", "unique Redis physical profile")
	flag.StringVar(&cfg.realm, "realm", "", "route realm")
	flag.StringVar(&cfg.runID, "run-id", "", "unique run id")
	flag.StringVar(&cfg.routeOut, "route-out", "", "route snapshot output")
	flag.StringVar(&cfg.reportOut, "report-out", "", "fixture report output")
	flag.StringVar(&cfg.playerHolder, "player-holder", "", "Player holder instance")
	flag.StringVar(&cfg.playerBoot, "player-boot", "", "Player holder boot")
	flag.StringVar(&cfg.playerEndpoint, "player-endpoint", "", "Player holder endpoint")
	flag.StringVar(&cfg.farmAHolder, "farm-a-holder", "", "Farm shard 0 holder instance")
	flag.StringVar(&cfg.farmABoot, "farm-a-boot", "", "Farm shard 0 holder boot")
	flag.StringVar(&cfg.farmAEndpoint, "farm-a-endpoint", "", "Farm shard 0 routed endpoint")
	flag.StringVar(&cfg.farmBHolder, "farm-b-holder", "", "Farm shard 1 holder instance")
	flag.StringVar(&cfg.farmBBoot, "farm-b-boot", "", "Farm shard 1 holder boot")
	flag.StringVar(&cfg.farmBEndpoint, "farm-b-endpoint", "", "Farm shard 1 routed endpoint")
	flag.DurationVar(&cfg.leaseTTL, "authority-lease-ttl", 60*time.Second, "authority claim TTL")
	flag.Uint64Var(&cfg.playerStart, "player-start", 910_000_000, "first candidate load-test player id")
	flag.Parse()
	return cfg
}

func (c config) validate() error {
	values := map[string]string{
		"redis-url": c.redisURL, "namespace": c.namespace, "profile": c.profile,
		"realm": c.realm, "run-id": c.runID, "route-out": c.routeOut, "report-out": c.reportOut,
		"player-holder": c.playerHolder, "player-boot": c.playerBoot, "player-endpoint": c.playerEndpoint,
		"farm-a-holder": c.farmAHolder, "farm-a-boot": c.farmABoot, "farm-a-endpoint": c.farmAEndpoint,
		"farm-b-holder": c.farmBHolder, "farm-b-boot": c.farmBBoot, "farm-b-endpoint": c.farmBEndpoint,
	}
	for name, value := range values {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("-%s is required", name)
		}
	}
	if c.leaseTTL <= 0 || c.playerStart == 0 {
		return errors.New("positive authority lease TTL and player start are required")
	}
	if c.playerHolder == c.farmAHolder || c.playerHolder == c.farmBHolder || c.farmAHolder == c.farmBHolder {
		return errors.New("Player/Farm holder instance ids must be distinct")
	}
	return nil
}

func run(cfg config) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	options, err := redis.ParseURL(cfg.redisURL)
	if err != nil {
		return fmt.Errorf("parse Redis configuration: %w", err)
	}
	client := redis.NewClient(options)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("real Redis unavailable: %w", err)
	}

	namespace, err := rediskey.NewNamespace(cfg.namespace, "local")
	if err != nil {
		return fmt.Errorf("namespace: %w", err)
	}
	profile := rediskey.PhysicalProfile{
		ProfileVersion: cfg.profile, Namespace: namespace,
		AuthorityLayoutVersion: "v1", WorkflowLayoutVersion: "v1",
		WorkflowHashVersion: 1, WorkflowBucketCount: 256,
	}
	calculator, err := actorrouting.NewCalculator(cfg.realm,
		actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: fixtureHashSeed}, 1, 2)
	if err != nil {
		return err
	}
	scripts, err := redisauthority.NewScriptRegistry(client)
	if err != nil {
		return err
	}
	if err := scripts.EnsureLoaded(ctx); err != nil {
		return fmt.Errorf("load Authority Lua: %w", err)
	}
	store, err := redisauthority.NewStore(client, scripts, redisauthority.Options{
		Profile: profile, Resolver: calculator, LeaseLimits: authoritystore.DefaultLeaseLimits(),
	})
	if err != nil {
		return err
	}

	claims := make([]authoritystore.ShardAuthorityTransitionResult, 0, 3)
	claimEvidenceRows := make([]claimEvidence, 0, 3)
	claim := func(domain actorrouting.RouteDomain, shard uint32, holder, boot string) error {
		result, claimErr := store.ClaimShardAuthority(ctx, authoritystore.ClaimShardAuthorityRequest{
			Caller: authoritystore.TrustedShardAuthorityCaller{
				Kind:       authoritystore.ShardAuthorityCallerRouteManager,
				InstanceID: "lease-real-route-manager", BootID: "lease-real-route-manager-" + cfg.runID,
			},
			TransitionID:           "claim-" + strings.ToLower(string(domain)) + fmt.Sprintf("-%d-", shard) + cfg.runID,
			AuthorityLayoutVersion: "v1", Domain: domain, VirtualShard: shard,
			NewHolderID: holder, NewHolderBootID: boot, RequestedTTLms: cfg.leaseTTL.Milliseconds(),
		})
		if claimErr != nil {
			return claimErr
		}
		if result.Status != authoritystore.ShardTransitionApplied {
			return fmt.Errorf("claim %s shard %d status=%s", domain, shard, result.Status)
		}
		claims = append(claims, result)
		claimEvidenceRows = append(claimEvidenceRows, claimEvidence{
			Domain: string(domain), Shard: shard, Holder: holder, Boot: boot, Status: result.Status.String(),
			AssignmentRevision: result.Authority.AssignmentRevision, WriterEpoch: result.Authority.WriterEpoch,
			LeaseEpoch: result.Authority.LeaseEpoch, LeaseUntilMS: result.Authority.LeaseUntilMs,
		})
		return nil
	}
	if err := claim(actorrouting.DomainPlayer, 0, cfg.playerHolder, cfg.playerBoot); err != nil {
		return fmt.Errorf("claim Player: %w", err)
	}
	if err := claim(actorrouting.DomainFarm, 0, cfg.farmAHolder, cfg.farmABoot); err != nil {
		return fmt.Errorf("claim Farm A: %w", err)
	}
	if err := claim(actorrouting.DomainFarm, 1, cfg.farmBHolder, cfg.farmBBoot); err != nil {
		return fmt.Errorf("claim Farm B: %w", err)
	}

	snapshot := actorrouting.RouteSnapshot{
		Realm: cfg.realm, GlobalRevision: 1,
		HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: fixtureHashSeed,
		Holders: []actorrouting.HolderRef{
			{InstanceID: cfg.playerHolder, BootID: cfg.playerBoot, Endpoint: cfg.playerEndpoint, State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: cfg.farmAHolder, BootID: cfg.farmABoot, Endpoint: cfg.farmAEndpoint, State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: cfg.farmBHolder, BootID: cfg.farmBBoot, Endpoint: cfg.farmBEndpoint, State: actorrouting.HolderStateActive, Weight: 1},
		},
		Player: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{routeFor(0, claims[0])}},
		Farm: actorrouting.DomainSnapshot{ShardCount: 2, Routes: []actorrouting.ShardRoute{
			routeFor(1, claims[1]), routeFor(2, claims[2]),
		}},
	}
	if _, err := actorrouting.NewStaticResolver(snapshot); err != nil {
		return fmt.Errorf("validate generated route: %w", err)
	}
	farmIDs, err := findFarmIDs(calculator, cfg.playerStart)
	if err != nil {
		return err
	}
	result := report{
		SchemaVersion: "r1-q1-real-lease-fixture/v1", Status: "PASS",
		AuthorityMode: "authority", MembershipMode: "composed", PlayerShards: 1, FarmShards: 2,
		Claims: claimEvidenceRows, FarmIDs: farmIDs,
		Checks: map[string]bool{
			"redis_authority_store": true, "authority_lua_loaded": true,
			"two_distinct_farm_holders": true, "route_snapshot_valid": true,
			"farm_ids_cover_both_shards": true,
		},
	}
	if err := writeJSON(cfg.routeOut, snapshot, 0o600); err != nil {
		return err
	}
	if err := writeJSON(cfg.reportOut, result, 0o600); err != nil {
		return err
	}
	log.Printf("fixture PASS: Player shards=1 Farm shards=2 real Redis Authority claims=3")
	return nil
}

func routeFor(holderIndex int, claim authoritystore.ShardAuthorityTransitionResult) actorrouting.ShardRoute {
	return actorrouting.ShardRoute{
		HolderIndex: holderIndex, State: actorrouting.ShardStateActive,
		ShardRevision:       claim.Authority.AssignmentRevision,
		ExpectedWriterEpoch: claim.Authority.WriterEpoch,
		ExpectedLeaseEpoch:  claim.Authority.LeaseEpoch,
		LeaseUntilMs:        claim.Authority.LeaseUntilMs,
	}
}

func findFarmIDs(calculator *actorrouting.Calculator, start uint64) (map[string]uint64, error) {
	result := map[string]uint64{}
	counts := [2]int{}
	for id := start; id < start+1_000_000 && (counts[0] < 2 || counts[1] < 2); id++ {
		shard, err := calculator.ShardFor(actorrouting.DomainFarm, id)
		if err != nil {
			return nil, err
		}
		if shard > 1 || counts[shard] >= 2 {
			continue
		}
		counts[shard]++
		key := fmt.Sprintf("shard_%d_owner_%d", shard, counts[shard])
		result[key] = id
	}
	if counts[0] < 2 || counts[1] < 2 {
		return nil, errors.New("could not find two farm ids on each shard")
	}
	return result, nil
}

func writeJSON(path string, value any, mode os.FileMode) error {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), mode)
}
