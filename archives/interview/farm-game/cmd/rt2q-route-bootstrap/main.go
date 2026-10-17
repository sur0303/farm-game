// Command rt2q-route-bootstrap creates the initial RT2Q route state solely
// through production bootstrap and RouteAssignmentManager capabilities.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/actorrouting/authority"
	"farmgame/internal/holderidentity"
	"farmgame/internal/redisdomains"
	core "farmgame/internal/routecontrol"
	redisauthority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
	redisroutecontrol "farmgame/internal/storage/redis/routecontrol"
)

const (
	redisURLEnv         = "RT2Q_BOOTSTRAP_REDIS_URL"
	redisClusterURLsEnv = "RT2Q_BOOTSTRAP_REDIS_CLUSTER_URLS"
	claimTTL            = 60 * time.Second
	maxBootstrapShards  = 1024
)

type config struct {
	challenge              string
	realm                  string
	controllerID           string
	controllerBoot         string
	playerA                string
	playerABoot            string
	playerAClaimBoot       string
	playerAEndpoint        string
	playerAPodUID          string
	playerAWorkloadUID     string
	playerAOrdinal         int
	playerAControlEndpoint string
	playerATLSIdentity     string
	playerB                string
	playerBBoot            string
	playerBClaimBoot       string
	playerBEndpoint        string
	playerBPodUID          string
	playerBWorkloadUID     string
	playerBOrdinal         int
	playerBControlEndpoint string
	playerBTLSIdentity     string
	playerC                string
	playerCBoot            string
	playerCClaimBoot       string
	playerCEndpoint        string
	playerCPodUID          string
	playerCWorkloadUID     string
	playerCOrdinal         int
	playerCControlEndpoint string
	playerCTLSIdentity     string
	playerD                string
	playerDBoot            string
	playerDClaimBoot       string
	playerDEndpoint        string
	playerDPodUID          string
	playerDWorkloadUID     string
	playerDOrdinal         int
	playerDControlEndpoint string
	playerDTLSIdentity     string
	farmA                  string
	farmABoot              string
	farmAClaimBoot         string
	farmAEndpoint          string
	farmAPodUID            string
	farmAWorkloadUID       string
	farmAOrdinal           int
	farmAControlEndpoint   string
	farmATLSIdentity       string
	farmB                  string
	farmBBoot              string
	farmBClaimBoot         string
	farmBEndpoint          string
	farmBPodUID            string
	farmBWorkloadUID       string
	farmBOrdinal           int
	farmBControlEndpoint   string
	farmBTLSIdentity       string
	farmC                  string
	farmCBoot              string
	farmCClaimBoot         string
	farmCEndpoint          string
	farmCPodUID            string
	farmCWorkloadUID       string
	farmCOrdinal           int
	farmCControlEndpoint   string
	farmCTLSIdentity       string
	farmD                  string
	farmDBoot              string
	farmDClaimBoot         string
	farmDEndpoint          string
	farmDPodUID            string
	farmDWorkloadUID       string
	farmDOrdinal           int
	farmDControlEndpoint   string
	farmDTLSIdentity       string
	workerA                string
	workerABoot            string
	workerAEndpoint        string
	workerB                string
	workerBBoot            string
	workerBEndpoint        string
	environment            string
	zone                   string
	profile                string
	authorityLayout        string
	workflowLayout         string
	workflowHash           string
	workflowBuckets        string
	shardCount             uint
	bootstrapOnly          bool
	skipWorkerClaims       bool
	resumeExisting         bool
}

type claimFact struct {
	TransitionID       string `json:"transition_id"`
	Domain             string `json:"domain"`
	Shard              uint32 `json:"shard"`
	HolderID           string `json:"holder_id"`
	HolderBootID       string `json:"holder_boot_id"`
	Status             string `json:"status"`
	AssignmentRevision uint64 `json:"assignment_revision"`
	WriterEpoch        uint64 `json:"writer_epoch"`
	LeaseEpoch         uint64 `json:"lease_epoch"`
	LeaseUntilMs       int64  `json:"lease_until_ms"`
}

type output struct {
	Schema              string      `json:"schema"`
	Challenge           string      `json:"challenge"`
	KeyspaceFingerprint string      `json:"keyspace_fingerprint"`
	BootstrapStatus     string      `json:"bootstrap_status"`
	InitialRevision     uint64      `json:"initial_revision"`
	FinalRevision       uint64      `json:"final_revision"`
	Claims              []claimFact `json:"claims"`
	SnapshotChecksum    string      `json:"snapshot_checksum_sha256"`
	TopologyFingerprint string      `json:"topology_fingerprint"`
}

type bootstrapTarget struct {
	domain             actorrouting.RouteDomain
	shard              uint32
	holder             string
	boot               string
	initialBoot        string
	endpoint           string
	controlEndpoint    string
	tlsIdentity        string
	deploymentIdentity holderidentity.DeploymentIdentity
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "rt2q-route-bootstrap failed: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string, destination io.Writer) error {
	cfg, err := parseConfig(args)
	if err != nil {
		return err
	}
	rawRedisURL := strings.TrimSpace(os.Getenv(redisURLEnv))
	clusterURLs := strings.TrimSpace(os.Getenv(redisClusterURLsEnv))
	if rawRedisURL == "" && clusterURLs == "" {
		return errors.New("private bootstrap Redis URL or cluster seeds are required")
	}

	tuple, err := rediskey.ParseKeyspaceTuple(rediskey.KeyspaceTupleInput{
		NamespaceEnvironment: cfg.environment, NamespaceZone: cfg.zone, PhysicalProfile: cfg.profile,
		AuthorityLayout: cfg.authorityLayout, WorkflowLayout: cfg.workflowLayout,
		WorkflowHashVersion: cfg.workflowHash, WorkflowBucketCount: cfg.workflowBuckets,
	})
	if err != nil {
		return err
	}
	fingerprint, err := tuple.Fingerprint()
	if err != nil {
		return err
	}
	snapshot := initialSnapshot(cfg)
	// A 1024-shard three-domain acceptance fixture performs 3072 fenced
	// authority initializations. Keep one bounded setup deadline, but do not
	// confuse the old 30-second utility limit with a runtime migration timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	client, err := redisdomains.OpenEndpoint(ctx, rawRedisURL, clusterURLs)
	if err != nil {
		return errors.New("invalid private bootstrap Redis environment")
	}
	defer client.Close()
	storeOptions := redisroutecontrol.DefaultOptions(tuple.Namespace())
	storeOptions.OperationTimeout = 2 * time.Second
	snapshotStore, err := redisroutecontrol.NewSnapshotStore(client, storeOptions)
	if err != nil {
		return err
	}
	bootstrap, err := snapshotStore.Bootstrap(ctx, core.NestedControlOperationToken{}, snapshot)
	if err != nil && cfg.resumeExisting && errors.Is(err, core.ErrSnapshotCASConflict) {
		stored, found, loadErr := snapshotStore.LoadCurrent(ctx)
		if loadErr != nil || !found {
			return errors.New("production snapshot resume requested but current snapshot is unavailable")
		}
		current := stored.Snapshot
		if current.Realm != snapshot.Realm || current.HashVersion != snapshot.HashVersion || current.HashSeed != snapshot.HashSeed ||
			current.Player.ShardCount != snapshot.Player.ShardCount || current.Farm.ShardCount != snapshot.Farm.ShardCount ||
			current.FarmWorker.ShardCount != snapshot.FarmWorker.ShardCount {
			return errors.New("production snapshot resume topology mismatch")
		}
		bootstrap = core.SnapshotCASResult{Status: core.SnapshotCASAlreadyCommitted, Stored: stored}
		snapshot = current
		err = nil
	}
	if err != nil {
		return fmt.Errorf("production snapshot bootstrap: %w", err)
	}
	if bootstrap.Status != core.SnapshotCASCommitted && bootstrap.Status != core.SnapshotCASAlreadyCommitted {
		return fmt.Errorf("production snapshot bootstrap status %s", bootstrap.Status)
	}
	result := output{Schema: "rt2q-route-bootstrap/v1", Challenge: cfg.challenge, KeyspaceFingerprint: fingerprint, BootstrapStatus: string(bootstrap.Status), InitialRevision: bootstrap.Stored.Revision}
	if cfg.bootstrapOnly {
		result.FinalRevision = bootstrap.Stored.Revision
		result.SnapshotChecksum = bootstrap.Stored.ChecksumSHA256
		result.TopologyFingerprint = bootstrap.Stored.TopologyFingerprint
		encoder := json.NewEncoder(destination)
		encoder.SetEscapeHTML(false)
		return encoder.Encode(result)
	}

	calculator, err := actorrouting.NewCalculator(snapshot.Realm, actorrouting.HashProfile{Version: snapshot.HashVersion, Seed: snapshot.HashSeed}, snapshot.Player.ShardCount, snapshot.Farm.ShardCount)
	if err != nil {
		return err
	}
	profile := tuple.Profile()
	authorityStore, err := redisauthority.NewStore(client, nil, redisauthority.Options{Profile: profile, Resolver: calculator})
	if err != nil {
		return err
	}
	if err := authorityStore.Registry().EnsureLoaded(ctx); err != nil {
		return err
	}
	readerOptions := redisauthority.DefaultAuthorityReaderOptions(profile, calculator)
	readerOptions.OperationTimeout = 2 * time.Second
	reader, err := redisauthority.NewShardAuthorityReader(client, readerOptions)
	if err != nil {
		return err
	}
	transitionStore, err := redisroutecontrol.NewTransitionStore(client, storeOptions)
	if err != nil {
		return err
	}
	if err := transitionStore.EnsureLoaded(); err != nil {
		return err
	}

	holderOptions := redisroutecontrol.DefaultHolderStoreOptions(tuple.Namespace())
	holderOptions.OperationTimeout = 2 * time.Second
	holderStore, err := redisroutecontrol.NewHolderStore(client, holderOptions)
	if err != nil {
		return err
	}
	if err := holderStore.EnsureLoaded(ctx); err != nil {
		return err
	}
	holderRegistry, err := core.NewHolderRegistry(holderStore, core.DefaultHolderRegistryOptions())
	if err != nil {
		return err
	}
	defer holderRegistry.Close()

	registeredHolders := make(map[string]struct{})
	for _, target := range bootstrapTargets(cfg) {
		key := string(target.domain) + "\x00" + target.holder + "\x00" + target.boot
		if _, found := registeredHolders[key]; found {
			continue
		}
		controlEndpoint := target.controlEndpoint
		if controlEndpoint == "" {
			controlEndpoint = target.endpoint
		}
		tlsIdentity := target.tlsIdentity
		if tlsIdentity == "" {
			tlsIdentity = target.holder
		}
		existing, found, err := holderRegistry.Get(ctx, target.domain, target.holder)
		if err != nil {
			return fmt.Errorf("read bootstrap holder %s/%s: %w", target.domain, target.holder, err)
		}
		if found {
			if err := validateExistingBootstrapHolder(existing, target, controlEndpoint, tlsIdentity, time.Now()); err != nil {
				return fmt.Errorf("validate bootstrap holder %s/%s: %w", target.domain, target.holder, err)
			}
			registeredHolders[key] = struct{}{}
			continue
		}
		if _, err := holderRegistry.Register(ctx, core.HolderRegistration{
			Domain: target.domain, HolderID: target.holder, BootID: target.boot,
			DeploymentIdentity: target.deploymentIdentity,
			LogicEndpoint:      target.endpoint, HolderControlEndpoint: controlEndpoint, TLSDNSIdentity: tlsIdentity,
		}); err != nil {
			return fmt.Errorf("register bootstrap holder %s/%s: %w", target.domain, target.holder, err)
		}
		registeredHolders[key] = struct{}{}
	}

	publisher, err := core.NewDurableSnapshotPublisher(snapshotStore, nil, core.AuthoritySnapshotNotifierFunc(nil))
	if err != nil {
		return err
	}
	manager, err := authority.NewRouteAssignmentManager(authority.ManagerOptions{
		Store: authorityStore, Publisher: publisher, AuthorityReader: reader, InitialSnapshot: snapshot,
		AuthorityLayoutVersion: profile.AuthorityLayoutVersion, RouteManagerID: cfg.controllerID,
		PlacementExclusions: holderRegistry,
		RouteManagerBootID:  cfg.controllerBoot, TombstoneStore: transitionStore, InitializationContext: ctx,
	})
	if err != nil {
		return err
	}

	for _, target := range bootstrapTargets(cfg) {
		transitionID := fmt.Sprintf("rt2q-%s-bootstrap-%s-%d", cfg.challenge, strings.ToLower(string(target.domain)), target.shard)
		claim, claimErr := manager.Claim(ctx, authority.ClaimRequest{
			TransitionID: transitionID, Domain: target.domain, VirtualShard: target.shard,
			NewHolderID: target.holder, NewHolderBootID: target.boot, Endpoint: target.endpoint, RequestedTTL: claimTTL,
		})
		if claimErr != nil {
			return fmt.Errorf("production manager claim %s/%d: %w", target.domain, target.shard, claimErr)
		}
		result.Claims = append(result.Claims, claimFact{
			TransitionID: transitionID, Domain: string(target.domain), Shard: target.shard,
			HolderID: target.holder, HolderBootID: target.boot, Status: string(claim.Status),
			AssignmentRevision: claim.Authority.AssignmentRevision, WriterEpoch: claim.Authority.WriterEpoch,
			LeaseEpoch: claim.Authority.LeaseEpoch, LeaseUntilMs: claim.Authority.LeaseUntilMs,
		})
	}
	stored, found, err := snapshotStore.LoadCurrent(ctx)
	if err != nil || !found {
		return errors.New("production snapshot missing after manager claims")
	}
	result.FinalRevision = stored.Revision
	result.SnapshotChecksum = stored.ChecksumSHA256
	result.TopologyFingerprint = stored.TopologyFingerprint
	encoder := json.NewEncoder(destination)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(result)
}

// validateExistingBootstrapHolder adopts the live HolderAgent record during
// the assignment phase instead of re-registering the same process generation.
// Re-registering would replace a newer heartbeat/lease with bootstrap defaults
// and is correctly rejected as a lease regression. Exact Kubernetes holders
// must already prove READY_ZERO_SHARD before any initial Authority is claimed.
func validateExistingBootstrapHolder(record core.HolderRecord, target bootstrapTarget, controlEndpoint, tlsIdentity string, now time.Time) error {
	if record.Domain != target.domain || record.HolderID != target.holder || record.BootID != target.boot {
		return errors.New("exact holder generation mismatch")
	}
	if record.DeploymentIdentity != target.deploymentIdentity {
		return errors.New("holder deployment identity mismatch")
	}
	// FARM_WORKER registers its control endpoint and TLS DNS identity from the
	// holder-control certificate.  The bootstrap CLI only carries the worker's
	// logic endpoint, so use the already verified live record for those two
	// transport fields instead of comparing it with the actor-holder default.
	if record.LogicEndpoint != target.endpoint ||
		(target.domain != actorrouting.DomainFarmWorker &&
			(record.HolderControlEndpoint != controlEndpoint || record.TLSDNSIdentity != tlsIdentity)) {
		return errors.New("holder endpoint identity mismatch")
	}
	if target.deploymentIdentity.Exact() {
		if record.State != core.HolderStateReadyZeroShard || !record.ProcessReady || record.Draining || record.ServingShardCount != 0 {
			return errors.New("exact Kubernetes holder is not READY_ZERO_SHARD")
		}
		if record.SnapshotRevision != 1 {
			return errors.New("exact Kubernetes holder has not loaded bootstrap snapshot revision 1")
		}
		if !record.LeaseUntil.After(now.Add(core.DefaultHolderMinLeaseTTL)) {
			return errors.New("exact Kubernetes holder requires fresh synchronous verification")
		}
	}
	return nil
}

func bootstrapTargets(cfg config) []bootstrapTarget {
	players := playerHolderSpecs(cfg)
	farms := farmHolderSpecs(cfg)
	targets := make([]bootstrapTarget, 0, 3*cfg.shardCount)
	for shard := uint32(0); shard < uint32(cfg.shardCount); shard++ {
		player := players[int(shard)%len(players)]
		player.shard = shard
		farm := farms[int(shard)%len(farms)]
		farm.shard = shard
		targets = append(targets, player, farm)
		if !cfg.skipWorkerClaims {
			workerHolder := cfg.workerA
			workerBoot := cfg.workerABoot
			workerEndpoint := cfg.workerAEndpoint
			if shard%2 == 1 {
				workerHolder = cfg.workerB
				workerBoot = cfg.workerBBoot
				workerEndpoint = cfg.workerBEndpoint
			}
			targets = append(targets, bootstrapTarget{
				domain:   actorrouting.DomainFarmWorker,
				shard:    shard,
				holder:   workerHolder,
				boot:     workerBoot,
				endpoint: workerEndpoint,
			})
		}
	}
	return targets
}

func playerHolderSpecs(cfg config) []bootstrapTarget {
	specs := []bootstrapTarget{
		{domain: actorrouting.DomainPlayer, holder: cfg.playerA, boot: preferredBoot(cfg.playerAClaimBoot, cfg.playerABoot), initialBoot: cfg.playerABoot, endpoint: cfg.playerAEndpoint, controlEndpoint: cfg.playerAControlEndpoint, tlsIdentity: cfg.playerATLSIdentity, deploymentIdentity: holderidentity.DeploymentIdentity{PodUID: cfg.playerAPodUID, WorkloadUID: cfg.playerAWorkloadUID, Ordinal: int32(cfg.playerAOrdinal)}},
	}
	if cfg.playerB != "" {
		specs = append(specs, bootstrapTarget{domain: actorrouting.DomainPlayer, holder: cfg.playerB, boot: preferredBoot(cfg.playerBClaimBoot, cfg.playerBBoot), initialBoot: cfg.playerBBoot, endpoint: cfg.playerBEndpoint, controlEndpoint: cfg.playerBControlEndpoint, tlsIdentity: cfg.playerBTLSIdentity, deploymentIdentity: holderidentity.DeploymentIdentity{PodUID: cfg.playerBPodUID, WorkloadUID: cfg.playerBWorkloadUID, Ordinal: int32(cfg.playerBOrdinal)}})
	}
	if cfg.playerC != "" {
		specs = append(specs, bootstrapTarget{domain: actorrouting.DomainPlayer, holder: cfg.playerC, boot: preferredBoot(cfg.playerCClaimBoot, cfg.playerCBoot), initialBoot: cfg.playerCBoot, endpoint: cfg.playerCEndpoint, controlEndpoint: cfg.playerCControlEndpoint, tlsIdentity: cfg.playerCTLSIdentity, deploymentIdentity: holderidentity.DeploymentIdentity{PodUID: cfg.playerCPodUID, WorkloadUID: cfg.playerCWorkloadUID, Ordinal: int32(cfg.playerCOrdinal)}})
	}
	if cfg.playerD != "" {
		specs = append(specs, bootstrapTarget{domain: actorrouting.DomainPlayer, holder: cfg.playerD, boot: preferredBoot(cfg.playerDClaimBoot, cfg.playerDBoot), initialBoot: cfg.playerDBoot, endpoint: cfg.playerDEndpoint, controlEndpoint: cfg.playerDControlEndpoint, tlsIdentity: cfg.playerDTLSIdentity, deploymentIdentity: holderidentity.DeploymentIdentity{PodUID: cfg.playerDPodUID, WorkloadUID: cfg.playerDWorkloadUID, Ordinal: int32(cfg.playerDOrdinal)}})
	}
	return specs
}

func farmHolderSpecs(cfg config) []bootstrapTarget {
	specs := []bootstrapTarget{
		{domain: actorrouting.DomainFarm, holder: cfg.farmA, boot: preferredBoot(cfg.farmAClaimBoot, cfg.farmABoot), initialBoot: cfg.farmABoot, endpoint: cfg.farmAEndpoint, controlEndpoint: cfg.farmAControlEndpoint, tlsIdentity: cfg.farmATLSIdentity, deploymentIdentity: holderidentity.DeploymentIdentity{PodUID: cfg.farmAPodUID, WorkloadUID: cfg.farmAWorkloadUID, Ordinal: int32(cfg.farmAOrdinal)}},
	}
	if cfg.farmB != "" {
		specs = append(specs, bootstrapTarget{domain: actorrouting.DomainFarm, holder: cfg.farmB, boot: preferredBoot(cfg.farmBClaimBoot, cfg.farmBBoot), initialBoot: cfg.farmBBoot, endpoint: cfg.farmBEndpoint, controlEndpoint: cfg.farmBControlEndpoint, tlsIdentity: cfg.farmBTLSIdentity, deploymentIdentity: holderidentity.DeploymentIdentity{PodUID: cfg.farmBPodUID, WorkloadUID: cfg.farmBWorkloadUID, Ordinal: int32(cfg.farmBOrdinal)}})
	}
	if cfg.farmC != "" {
		specs = append(specs, bootstrapTarget{domain: actorrouting.DomainFarm, holder: cfg.farmC, boot: preferredBoot(cfg.farmCClaimBoot, cfg.farmCBoot), initialBoot: cfg.farmCBoot, endpoint: cfg.farmCEndpoint, controlEndpoint: cfg.farmCControlEndpoint, tlsIdentity: cfg.farmCTLSIdentity, deploymentIdentity: holderidentity.DeploymentIdentity{PodUID: cfg.farmCPodUID, WorkloadUID: cfg.farmCWorkloadUID, Ordinal: int32(cfg.farmCOrdinal)}})
	}
	if cfg.farmD != "" {
		specs = append(specs, bootstrapTarget{domain: actorrouting.DomainFarm, holder: cfg.farmD, boot: preferredBoot(cfg.farmDClaimBoot, cfg.farmDBoot), initialBoot: cfg.farmDBoot, endpoint: cfg.farmDEndpoint, controlEndpoint: cfg.farmDControlEndpoint, tlsIdentity: cfg.farmDTLSIdentity, deploymentIdentity: holderidentity.DeploymentIdentity{PodUID: cfg.farmDPodUID, WorkloadUID: cfg.farmDWorkloadUID, Ordinal: int32(cfg.farmDOrdinal)}})
	}
	return specs
}

func preferredBoot(current, initial string) string {
	if current != "" {
		return current
	}
	return initial
}

func initialSnapshot(cfg config) actorrouting.RouteSnapshot {
	playerSpecs := playerHolderSpecs(cfg)
	farmSpecs := farmHolderSpecs(cfg)
	holders := make([]actorrouting.HolderRef, 0, len(playerSpecs)+len(farmSpecs)+2)
	playerIndices := make([]int, 0, len(playerSpecs))
	for _, spec := range playerSpecs {
		playerIndices = append(playerIndices, len(holders))
		holders = append(holders, actorrouting.HolderRef{InstanceID: spec.holder, BootID: spec.initialBoot, Endpoint: spec.endpoint, State: actorrouting.HolderStateActive, Weight: 1})
	}
	farmIndices := make([]int, 0, len(farmSpecs))
	for _, spec := range farmSpecs {
		farmIndices = append(farmIndices, len(holders))
		holders = append(holders, actorrouting.HolderRef{InstanceID: spec.holder, BootID: spec.initialBoot, Endpoint: spec.endpoint, State: actorrouting.HolderStateActive, Weight: 1})
	}
	workerAIndex := len(holders)
	holders = append(holders,
		actorrouting.HolderRef{InstanceID: cfg.workerA, BootID: cfg.workerABoot, Endpoint: cfg.workerAEndpoint, State: actorrouting.HolderStateActive, Weight: 1},
		actorrouting.HolderRef{InstanceID: cfg.workerB, BootID: cfg.workerBBoot, Endpoint: cfg.workerBEndpoint, State: actorrouting.HolderStateActive, Weight: 1},
	)
	placeholders := func(holder func(uint32) int) []actorrouting.ShardRoute {
		// No Authority exists at bootstrap revision 1. ASSIGNING placeholders
		// therefore carry no lease deadline; production Claim installs each first
		// positive generation and deadline without a fabricated predecessor.
		routes := make([]actorrouting.ShardRoute, cfg.shardCount)
		for shard := range routes {
			routes[shard] = actorrouting.ShardRoute{HolderIndex: holder(uint32(shard)), State: actorrouting.ShardStateAssigning, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1}
		}
		return routes
	}
	playerPlaceholders := placeholders(func(shard uint32) int { return playerIndices[int(shard)%len(playerIndices)] })
	farmPlaceholders := placeholders(func(shard uint32) int { return farmIndices[int(shard)%len(farmIndices)] })
	workerPlaceholders := placeholders(func(shard uint32) int {
		if shard%2 == 1 {
			return workerAIndex + 1
		}
		return workerAIndex
	})
	return actorrouting.RouteSnapshot{
		Realm: cfg.realm, GlobalRevision: 1, HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 202608100001,
		Player: actorrouting.DomainSnapshot{ShardCount: uint32(cfg.shardCount), Routes: playerPlaceholders},
		Farm:   actorrouting.DomainSnapshot{ShardCount: uint32(cfg.shardCount), Routes: farmPlaceholders},
		FarmWorker: actorrouting.DomainSnapshot{
			ShardCount: uint32(cfg.shardCount),
			Routes:     workerPlaceholders,
		},
		Holders: holders,
	}
}

func parseConfig(args []string) (config, error) {
	fs := flag.NewFlagSet("rt2q-route-bootstrap", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var cfg config
	fs.StringVar(&cfg.challenge, "challenge", "", "fresh run challenge")
	fs.StringVar(&cfg.realm, "realm", "", "route realm")
	fs.StringVar(&cfg.controllerID, "controller-id", "", "controller identity")
	fs.StringVar(&cfg.controllerBoot, "controller-boot", "", "controller boot")
	fs.StringVar(&cfg.playerA, "player-a", "", "PLAYER A holder")
	fs.StringVar(&cfg.playerABoot, "player-a-boot", "", "PLAYER A boot")
	fs.StringVar(&cfg.playerAClaimBoot, "player-a-claim-boot", "", "optional current PLAYER A generation used by initial claims")
	fs.StringVar(&cfg.playerAEndpoint, "player-a-endpoint", "", "PLAYER A logic endpoint")
	fs.StringVar(&cfg.playerAPodUID, "player-a-pod-uid", "", "PLAYER A Kubernetes Pod UID")
	fs.StringVar(&cfg.playerAWorkloadUID, "player-a-workload-uid", "", "PLAYER StatefulSet UID")
	fs.IntVar(&cfg.playerAOrdinal, "player-a-ordinal", -1, "PLAYER A StatefulSet ordinal")
	fs.StringVar(&cfg.playerAControlEndpoint, "player-a-control-endpoint", "", "PLAYER A HolderControl endpoint")
	fs.StringVar(&cfg.playerATLSIdentity, "player-a-tls-identity", "", "PLAYER A HolderControl TLS identity")
	fs.StringVar(&cfg.playerB, "player-b", "", "PLAYER B holder")
	fs.StringVar(&cfg.playerBBoot, "player-b-boot", "", "PLAYER B boot")
	fs.StringVar(&cfg.playerBClaimBoot, "player-b-claim-boot", "", "optional current PLAYER B generation used by initial claims")
	fs.StringVar(&cfg.playerBEndpoint, "player-b-endpoint", "", "PLAYER B logic endpoint")
	fs.StringVar(&cfg.playerBPodUID, "player-b-pod-uid", "", "PLAYER B Kubernetes Pod UID")
	fs.StringVar(&cfg.playerBWorkloadUID, "player-b-workload-uid", "", "PLAYER StatefulSet UID")
	fs.IntVar(&cfg.playerBOrdinal, "player-b-ordinal", -1, "PLAYER B StatefulSet ordinal")
	fs.StringVar(&cfg.playerBControlEndpoint, "player-b-control-endpoint", "", "PLAYER B HolderControl endpoint")
	fs.StringVar(&cfg.playerBTLSIdentity, "player-b-tls-identity", "", "PLAYER B HolderControl TLS identity")
	fs.StringVar(&cfg.playerC, "player-c", "", "optional PLAYER C holder")
	fs.StringVar(&cfg.playerCBoot, "player-c-boot", "", "optional PLAYER C boot")
	fs.StringVar(&cfg.playerCClaimBoot, "player-c-claim-boot", "", "optional current PLAYER C generation used by initial claims")
	fs.StringVar(&cfg.playerCEndpoint, "player-c-endpoint", "", "optional PLAYER C logic endpoint")
	fs.StringVar(&cfg.playerCPodUID, "player-c-pod-uid", "", "optional PLAYER C Kubernetes Pod UID")
	fs.StringVar(&cfg.playerCWorkloadUID, "player-c-workload-uid", "", "optional PLAYER C StatefulSet UID")
	fs.IntVar(&cfg.playerCOrdinal, "player-c-ordinal", -1, "optional PLAYER C StatefulSet ordinal")
	fs.StringVar(&cfg.playerCControlEndpoint, "player-c-control-endpoint", "", "optional PLAYER C HolderControl endpoint")
	fs.StringVar(&cfg.playerCTLSIdentity, "player-c-tls-identity", "", "optional PLAYER C HolderControl TLS identity")
	fs.StringVar(&cfg.playerD, "player-d", "", "optional PLAYER D holder")
	fs.StringVar(&cfg.playerDBoot, "player-d-boot", "", "optional PLAYER D boot")
	fs.StringVar(&cfg.playerDClaimBoot, "player-d-claim-boot", "", "optional current PLAYER D generation used by initial claims")
	fs.StringVar(&cfg.playerDEndpoint, "player-d-endpoint", "", "optional PLAYER D logic endpoint")
	fs.StringVar(&cfg.playerDPodUID, "player-d-pod-uid", "", "optional PLAYER D Kubernetes Pod UID")
	fs.StringVar(&cfg.playerDWorkloadUID, "player-d-workload-uid", "", "optional PLAYER D StatefulSet UID")
	fs.IntVar(&cfg.playerDOrdinal, "player-d-ordinal", -1, "optional PLAYER D StatefulSet ordinal")
	fs.StringVar(&cfg.playerDControlEndpoint, "player-d-control-endpoint", "", "optional PLAYER D HolderControl endpoint")
	fs.StringVar(&cfg.playerDTLSIdentity, "player-d-tls-identity", "", "optional PLAYER D HolderControl TLS identity")
	fs.StringVar(&cfg.farmA, "farm-a", "", "FARM A holder")
	fs.StringVar(&cfg.farmABoot, "farm-a-boot", "", "FARM A boot")
	fs.StringVar(&cfg.farmAClaimBoot, "farm-a-claim-boot", "", "optional current FARM A generation used by initial claims")
	fs.StringVar(&cfg.farmAEndpoint, "farm-a-endpoint", "", "FARM A logic endpoint")
	fs.StringVar(&cfg.farmAPodUID, "farm-a-pod-uid", "", "FARM A Kubernetes Pod UID")
	fs.StringVar(&cfg.farmAWorkloadUID, "farm-a-workload-uid", "", "FARM StatefulSet UID")
	fs.IntVar(&cfg.farmAOrdinal, "farm-a-ordinal", -1, "FARM A StatefulSet ordinal")
	fs.StringVar(&cfg.farmAControlEndpoint, "farm-a-control-endpoint", "", "FARM A HolderControl endpoint")
	fs.StringVar(&cfg.farmATLSIdentity, "farm-a-tls-identity", "", "FARM A HolderControl TLS identity")
	fs.StringVar(&cfg.farmB, "farm-b", "", "FARM B holder")
	fs.StringVar(&cfg.farmBBoot, "farm-b-boot", "", "FARM B boot")
	fs.StringVar(&cfg.farmBClaimBoot, "farm-b-claim-boot", "", "optional current FARM B generation used by initial claims")
	fs.StringVar(&cfg.farmBEndpoint, "farm-b-endpoint", "", "FARM B logic endpoint")
	fs.StringVar(&cfg.farmBPodUID, "farm-b-pod-uid", "", "FARM B Kubernetes Pod UID")
	fs.StringVar(&cfg.farmBWorkloadUID, "farm-b-workload-uid", "", "FARM StatefulSet UID")
	fs.IntVar(&cfg.farmBOrdinal, "farm-b-ordinal", -1, "FARM B StatefulSet ordinal")
	fs.StringVar(&cfg.farmBControlEndpoint, "farm-b-control-endpoint", "", "FARM B HolderControl endpoint")
	fs.StringVar(&cfg.farmBTLSIdentity, "farm-b-tls-identity", "", "FARM B HolderControl TLS identity")
	fs.StringVar(&cfg.farmC, "farm-c", "", "optional FARM C holder")
	fs.StringVar(&cfg.farmCBoot, "farm-c-boot", "", "optional FARM C boot")
	fs.StringVar(&cfg.farmCClaimBoot, "farm-c-claim-boot", "", "optional current FARM C generation used by initial claims")
	fs.StringVar(&cfg.farmCEndpoint, "farm-c-endpoint", "", "optional FARM C logic endpoint")
	fs.StringVar(&cfg.farmCPodUID, "farm-c-pod-uid", "", "optional FARM C Kubernetes Pod UID")
	fs.StringVar(&cfg.farmCWorkloadUID, "farm-c-workload-uid", "", "optional FARM StatefulSet UID")
	fs.IntVar(&cfg.farmCOrdinal, "farm-c-ordinal", -1, "optional FARM C StatefulSet ordinal")
	fs.StringVar(&cfg.farmCControlEndpoint, "farm-c-control-endpoint", "", "optional FARM C HolderControl endpoint")
	fs.StringVar(&cfg.farmCTLSIdentity, "farm-c-tls-identity", "", "optional FARM C HolderControl TLS identity")
	fs.StringVar(&cfg.farmD, "farm-d", "", "optional FARM D holder")
	fs.StringVar(&cfg.farmDBoot, "farm-d-boot", "", "optional FARM D boot")
	fs.StringVar(&cfg.farmDClaimBoot, "farm-d-claim-boot", "", "optional current FARM D generation used by initial claims")
	fs.StringVar(&cfg.farmDEndpoint, "farm-d-endpoint", "", "optional FARM D logic endpoint")
	fs.StringVar(&cfg.farmDPodUID, "farm-d-pod-uid", "", "optional FARM D Kubernetes Pod UID")
	fs.StringVar(&cfg.farmDWorkloadUID, "farm-d-workload-uid", "", "optional FARM D StatefulSet UID")
	fs.IntVar(&cfg.farmDOrdinal, "farm-d-ordinal", -1, "optional FARM D StatefulSet ordinal")
	fs.StringVar(&cfg.farmDControlEndpoint, "farm-d-control-endpoint", "", "optional FARM D HolderControl endpoint")
	fs.StringVar(&cfg.farmDTLSIdentity, "farm-d-tls-identity", "", "optional FARM D HolderControl TLS identity")
	fs.StringVar(&cfg.workerA, "farm-worker-a", "", "FARM_WORKER A holder")
	fs.StringVar(&cfg.workerABoot, "farm-worker-a-boot", "", "FARM_WORKER A boot")
	fs.StringVar(&cfg.workerAEndpoint, "farm-worker-a-endpoint", "", "FARM_WORKER A endpoint")
	fs.StringVar(&cfg.workerB, "farm-worker-b", "", "FARM_WORKER B holder")
	fs.StringVar(&cfg.workerBBoot, "farm-worker-b-boot", "", "FARM_WORKER B boot")
	fs.StringVar(&cfg.workerBEndpoint, "farm-worker-b-endpoint", "", "FARM_WORKER B endpoint")
	fs.StringVar(&cfg.environment, "redis-environment", "", "Redis namespace environment")
	fs.StringVar(&cfg.zone, "redis-zone", "", "Redis namespace zone")
	fs.StringVar(&cfg.profile, "redis-physical-profile", "redis-physical-v1", "Redis physical profile")
	fs.StringVar(&cfg.authorityLayout, "authority-layout", "v1", "authority layout")
	fs.StringVar(&cfg.workflowLayout, "workflow-layout", "v1", "workflow layout")
	fs.StringVar(&cfg.workflowHash, "workflow-hash", "1", "workflow hash version")
	fs.StringVar(&cfg.workflowBuckets, "workflow-buckets", "256", "workflow bucket count")
	fs.UintVar(&cfg.shardCount, "shard-count", 2, "equal PLAYER, FARM, and FARM_WORKER virtual shard count (1..1024)")
	fs.BoolVar(&cfg.bootstrapOnly, "bootstrap-only", false, "bootstrap the initial zero-shard snapshot without Manager claims")
	fs.BoolVar(&cfg.skipWorkerClaims, "skip-worker-claims", false, "leave FARM_WORKER routes assigning when no worker Holders are running")
	fs.BoolVar(&cfg.resumeExisting, "resume-existing", false, "resume idempotent claims from a compatible partially initialized snapshot")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 {
		return config{}, errors.New("invalid bootstrap arguments")
	}
	values := []string{
		cfg.challenge,
		cfg.realm,
		cfg.controllerID,
		cfg.controllerBoot,
		cfg.playerA,
		cfg.playerABoot,
		cfg.playerAEndpoint,
		cfg.farmA,
		cfg.farmABoot,
		cfg.farmAEndpoint,
		cfg.workerA,
		cfg.workerABoot,
		cfg.workerAEndpoint,
		cfg.workerB,
		cfg.workerBBoot,
		cfg.workerBEndpoint,
		cfg.environment,
		cfg.zone,
	}
	for _, value := range values {
		if strings.TrimSpace(value) == "" || strings.TrimSpace(value) != value {
			return config{}, errors.New("all bootstrap identities, endpoints, and namespace fields are required")
		}
	}
	for name, value := range map[string]string{
		"player-a-claim-boot": cfg.playerAClaimBoot,
		"player-b-claim-boot": cfg.playerBClaimBoot,
		"player-c-claim-boot": cfg.playerCClaimBoot,
		"player-d-claim-boot": cfg.playerDClaimBoot,
		"farm-a-claim-boot":   cfg.farmAClaimBoot,
		"farm-b-claim-boot":   cfg.farmBClaimBoot,
		"farm-c-claim-boot":   cfg.farmCClaimBoot,
		"farm-d-claim-boot":   cfg.farmDClaimBoot,
	} {
		if value != "" && strings.TrimSpace(value) != value {
			return config{}, fmt.Errorf("%s must be trimmed", name)
		}
	}
	optionalHolders := []struct {
		name, holder, boot, endpoint string
		deployment                   holderDeploymentConfig
	}{
		{"player-a", cfg.playerA, cfg.playerABoot, cfg.playerAEndpoint, holderDeploymentConfig{cfg.playerAPodUID, cfg.playerAWorkloadUID, cfg.playerAOrdinal, cfg.playerAControlEndpoint, cfg.playerATLSIdentity}},
		{"player-b", cfg.playerB, cfg.playerBBoot, cfg.playerBEndpoint, holderDeploymentConfig{cfg.playerBPodUID, cfg.playerBWorkloadUID, cfg.playerBOrdinal, cfg.playerBControlEndpoint, cfg.playerBTLSIdentity}},
		{"player-c", cfg.playerC, cfg.playerCBoot, cfg.playerCEndpoint, holderDeploymentConfig{cfg.playerCPodUID, cfg.playerCWorkloadUID, cfg.playerCOrdinal, cfg.playerCControlEndpoint, cfg.playerCTLSIdentity}},
		{"player-d", cfg.playerD, cfg.playerDBoot, cfg.playerDEndpoint, holderDeploymentConfig{cfg.playerDPodUID, cfg.playerDWorkloadUID, cfg.playerDOrdinal, cfg.playerDControlEndpoint, cfg.playerDTLSIdentity}},
		{"farm-a", cfg.farmA, cfg.farmABoot, cfg.farmAEndpoint, holderDeploymentConfig{cfg.farmAPodUID, cfg.farmAWorkloadUID, cfg.farmAOrdinal, cfg.farmAControlEndpoint, cfg.farmATLSIdentity}},
		{"farm-b", cfg.farmB, cfg.farmBBoot, cfg.farmBEndpoint, holderDeploymentConfig{cfg.farmBPodUID, cfg.farmBWorkloadUID, cfg.farmBOrdinal, cfg.farmBControlEndpoint, cfg.farmBTLSIdentity}},
		{"farm-c", cfg.farmC, cfg.farmCBoot, cfg.farmCEndpoint, holderDeploymentConfig{cfg.farmCPodUID, cfg.farmCWorkloadUID, cfg.farmCOrdinal, cfg.farmCControlEndpoint, cfg.farmCTLSIdentity}},
		{"farm-d", cfg.farmD, cfg.farmDBoot, cfg.farmDEndpoint, holderDeploymentConfig{cfg.farmDPodUID, cfg.farmDWorkloadUID, cfg.farmDOrdinal, cfg.farmDControlEndpoint, cfg.farmDTLSIdentity}},
	}
	for _, holder := range optionalHolders {
		if err := validateOptionalHolder(holder.name, holder.holder, holder.boot, holder.endpoint, holder.deployment); err != nil {
			return config{}, err
		}
	}
	if cfg.playerC != "" && cfg.playerB == "" {
		return config{}, errors.New("player-c requires player-b")
	}
	if cfg.playerD != "" && cfg.playerC == "" {
		return config{}, errors.New("player-d requires player-c")
	}
	if cfg.farmC != "" && cfg.farmB == "" {
		return config{}, errors.New("farm-c requires farm-b")
	}
	if cfg.farmD != "" && cfg.farmC == "" {
		return config{}, errors.New("farm-d requires farm-c")
	}
	if len(cfg.challenge) != 32 {
		return config{}, errors.New("challenge must be a 128-bit lowercase hex token")
	}
	if cfg.shardCount == 0 || cfg.shardCount > maxBootstrapShards {
		return config{}, fmt.Errorf("shard-count must be between 1 and %d", maxBootstrapShards)
	}
	return cfg, nil
}

func validateOptionalHolder(name, holder, boot, endpoint string, deployment holderDeploymentConfig) error {
	present := holder != "" || boot != "" || endpoint != "" || deployment.podUID != "" || deployment.workloadUID != "" || deployment.ordinal != -1 || deployment.controlEndpoint != "" || deployment.tlsIdentity != ""
	if !present {
		return nil
	}
	if holder == "" || boot == "" || endpoint == "" {
		return fmt.Errorf("%s identity must include holder, boot, and endpoint", name)
	}
	if strings.TrimSpace(holder) != holder || strings.TrimSpace(boot) != boot || strings.TrimSpace(endpoint) != endpoint {
		return fmt.Errorf("%s identity fields must be trimmed", name)
	}
	if err := deployment.validate(); err != nil {
		return fmt.Errorf("%s deployment identity: %w", name, err)
	}
	return nil
}

type holderDeploymentConfig struct {
	podUID          string
	workloadUID     string
	ordinal         int
	controlEndpoint string
	tlsIdentity     string
}

func (c holderDeploymentConfig) validate() error {
	absent := c.podUID == "" && c.workloadUID == "" && c.ordinal == -1 && c.controlEndpoint == "" && c.tlsIdentity == ""
	if absent {
		return nil
	}
	if c.ordinal > 2147483647 {
		return errors.New("ordinal exceeds int32")
	}
	identity := holderidentity.DeploymentIdentity{PodUID: c.podUID, WorkloadUID: c.workloadUID, Ordinal: int32(c.ordinal)}
	if err := identity.ValidateExact(); err != nil {
		return err
	}
	if strings.TrimSpace(c.controlEndpoint) == "" || strings.TrimSpace(c.tlsIdentity) == "" {
		return errors.New("control endpoint and TLS identity are required with exact Kubernetes identity")
	}
	return nil
}
