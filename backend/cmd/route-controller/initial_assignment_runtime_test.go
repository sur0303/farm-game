package main

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/actorrouting/authority"
	"farmgame/internal/holderidentity"
	routecore "farmgame/internal/routecontrol"
	"farmgame/internal/routecontrol/verifiedholder"
	"farmgame/internal/routeplan"
	scaleplancore "farmgame/internal/scaleplan"
	scaleplancontrol "farmgame/internal/scaleplan/adapters/control"
	"farmgame/internal/storage/authoritystore"
	redisauthority "farmgame/internal/storage/redis/authority"
	redisroutecontrol "farmgame/internal/storage/redis/routecontrol"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestInitialAssignmentModeValidationAndMutationDependencies(t *testing.T) {
	if mode, err := initialAssignmentModeFromConfig(config{}); err != nil || mode != initialAssignmentModeOff {
		t.Fatalf("default mode=%q err=%v, want off", mode, err)
	}
	if err := validateInitialAssignmentMode("unexpected"); err == nil {
		t.Fatal("unknown initial assignment mode was accepted")
	}
	parsed, err := parseConfig([]string{
		"-initial-assignment-mode=off", "-initial-assignment-timeout=1s",
		"-initial-assignment-expected-player-holders=1", "-initial-assignment-expected-farm-holders=3",
	})
	if err != nil || parsed.initialAssignmentMode != initialAssignmentModeOff || parsed.initialAssignmentTimeout != time.Second ||
		parsed.initialAssignmentExpectedPlayerHolders != 1 || parsed.initialAssignmentExpectedFarmHolders != 3 {
		t.Fatalf("parsed initial assignment config=%+v err=%v", parsed, err)
	}
	for _, args := range [][]string{
		{"-initial-assignment-timeout=0"},
		{"-initial-assignment-expected-player-holders=0"},
		{"-initial-assignment-expected-farm-holders=0"},
	} {
		if _, err := parseConfig(args); err == nil {
			t.Fatalf("invalid initial assignment args %v were accepted", args)
		}
	}
	base := config{
		initialAssignmentMode:                  initialAssignmentModeAuto,
		initialAssignmentTimeout:               time.Second,
		initialAssignmentExpectedPlayerHolders: 2,
		initialAssignmentExpectedFarmHolders:   2,
		holderDiscoveryMode:                    holderDiscoveryModeK8s,
		scalePlanMode:                          string(scaleplancore.RuntimeModeManual),
	}
	if err := validateInitialAssignmentConfig(base); err != nil {
		t.Fatalf("valid auto initial assignment config rejected: %v", err)
	}
	base.holderDiscoveryMode = holderDiscoveryModeLegacy
	if err := validateInitialAssignmentConfig(base); err == nil {
		t.Fatal("auto initial assignment without Kubernetes discovery was accepted")
	}
	base.holderDiscoveryMode = holderDiscoveryModeK8s
	base.scalePlanMode = scalePlanModeOff
	if err := validateInitialAssignmentConfig(base); err == nil {
		t.Fatal("auto initial assignment without ScalePlan mutation mode was accepted")
	}
}

func TestRouteControllerTransitionCapacityCoversFresh2048ClaimTopology(t *testing.T) {
	items, err := routeControllerTransitionStoreItems(actorrouting.RouteSnapshot{
		Player: actorrouting.DomainSnapshot{ShardCount: 1024},
		Farm:   actorrouting.DomainSnapshot{ShardCount: 1024},
	})
	if err != nil || items < 2048 {
		t.Fatalf("transition items=%d err=%v, want capacity for 2048 claims", items, err)
	}
}

func TestInitialAssignmentDeterministicBalance1024(t *testing.T) {
	holders := []routecore.HolderRecord{
		initialAssignmentTestHolder(actorrouting.DomainPlayer, "player-1", "boot-1", 1, routecore.HolderStateReadyZeroShard, 0),
		initialAssignmentTestHolder(actorrouting.DomainPlayer, "player-0", "boot-0", 0, routecore.HolderStateReadyZeroShard, 0),
	}
	counts := map[string]int{}
	for shard := uint32(0); shard < 1024; shard++ {
		target, err := deterministicInitialAssignmentTarget(holders, shard)
		if err != nil {
			t.Fatal(err)
		}
		counts[target.HolderID]++
	}
	if counts["player-0"] != 512 || counts["player-1"] != 512 {
		t.Fatalf("balance=%v, want 512/512", counts)
	}
	first, _ := deterministicInitialAssignmentTarget(holders, 0)
	second, _ := deterministicInitialAssignmentTarget(holders, 1)
	if first.HolderID != "player-0" || second.HolderID != "player-1" {
		t.Fatalf("sorted modulo targets=%s,%s", first.HolderID, second.HolderID)
	}
}

func TestInitialAssignmentMiniredisAcceptance1024Plus1024(t *testing.T) {
	const shardCount = 1024
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	server := miniredis.RunT(t)
	server.SetTime(time.Now())
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()

	cfg := validCommandConfig()
	cfg.realm = "initial-batch-acceptance"
	cfg.redisOperation = 10 * time.Second
	cfg.initialAssignmentMode = initialAssignmentModeAuto
	cfg.initialAssignmentExpectedPlayerHolders = 2
	cfg.initialAssignmentExpectedFarmHolders = 2
	tuple, err := cfg.keyspaceTuple()
	if err != nil {
		t.Fatal(err)
	}
	profile := tuple.Profile()
	calculator, err := actorrouting.NewCalculator(cfg.realm, actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 1}, shardCount, shardCount)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := initialAssignmentBatchAcceptanceSnapshot(cfg.realm, shardCount)
	snapshotOptions := redisroutecontrol.DefaultOptions(tuple.Namespace())
	snapshotOptions.OperationTimeout = 10 * time.Second
	snapshotStore, err := redisroutecontrol.NewSnapshotStore(client, snapshotOptions)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := snapshotStore.Bootstrap(ctx, routecore.NestedControlOperationToken{}, snapshot); err != nil {
		t.Fatal(err)
	}
	hub, err := routecore.NewSnapshotWatchHubWithContext(ctx, snapshotStore, routecore.SnapshotWatchHubOptions{ReconcileInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Close()
	sub, err := hub.Subscribe(ctx, snapshot.GlobalRevision)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	authorityStore, err := redisauthority.NewStore(client, nil, redisauthority.Options{Profile: profile, Resolver: calculator})
	if err != nil {
		t.Fatal(err)
	}
	if err := authorityStore.Registry().EnsureLoaded(ctx); err != nil {
		t.Fatal(err)
	}
	readerOptions := redisauthority.DefaultAuthorityReaderOptions(profile, calculator)
	readerOptions.OperationTimeout = 10 * time.Second
	reader, err := redisauthority.NewShardAuthorityReader(client, readerOptions)
	if err != nil {
		t.Fatal(err)
	}
	candidateValues := initialAssignmentBatchAcceptanceCandidates(cfg.realm, shardCount)
	// Simulate an interrupted batch: exact durable Authority records exist for
	// PLAYER shards 0..136 and FARM shard 0 (the r13 production crash point),
	// but no durable transition tombstones/recoveries or final snapshot exist.
	// The runtime must replay them by exact target identity and continue.
	for _, domain := range []actorrouting.RouteDomain{actorrouting.DomainPlayer, actorrouting.DomainFarm} {
		preclaimed := 1
		if domain == actorrouting.DomainPlayer {
			preclaimed = 137
		}
		for shard := uint32(0); shard < uint32(preclaimed); shard++ {
			target, _ := deterministicInitialAssignmentTarget(candidateValues[domain].Holders, shard)
			result, claimErr := authorityStore.ClaimShardAuthority(ctx, authoritystore.ClaimShardAuthorityRequest{
				Caller:       authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerRouteManager, InstanceID: cfg.controllerID, BootID: cfg.boot},
				TransitionID: initialAssignmentTransitionIDForTarget(cfg.realm, domain, shard, target), AuthorityLayoutVersion: cfg.authorityLayout,
				Domain: domain, VirtualShard: shard, NewHolderID: target.HolderID, NewHolderBootID: target.BootID, RequestedTTLms: initialAssignmentClaimTTL.Milliseconds(),
			})
			if claimErr != nil || result.Status != authoritystore.ShardTransitionApplied {
				t.Fatalf("partial preclaim %s/%d result=%+v err=%v", domain, shard, result, claimErr)
			}
		}
	}
	transitionOptions := redisroutecontrol.DefaultOptions(tuple.Namespace())
	transitionOptions.OperationTimeout = 10 * time.Second
	transitionOptions.MaxTransitionItems = 4096
	transitionStore, err := redisroutecontrol.NewTransitionStore(client, transitionOptions)
	if err != nil {
		t.Fatal(err)
	}
	if err := transitionStore.EnsureLoaded(); err != nil {
		t.Fatal(err)
	}
	switchableTransitions, err := authority.NewSwitchableTransitionStore(transitionStore, 4096)
	if err != nil {
		t.Fatal(err)
	}

	controlScope := scaleplancore.MutationScope{Namespace: "initial-batch", Realm: cfg.realm}
	controlOwner := scaleplancore.MutationLeaseOwner{ControllerID: "controller-a", ControllerBootID: "boot-a"}
	coordinator, err := scaleplancontrol.NewControlMutationCoordinator(scaleplancontrol.ControlMutationCoordinatorOptions{Scope: controlScope, LeaseTTL: 5 * time.Minute, Epoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	control := routeplan.ControlMutationOptions{
		Coordinator: coordinator,
		Scope:       controlScope,
		Owner:       controlOwner,
		NonceSource: func() (string, error) { return "initial-batch-nonce", nil },
	}
	publisher, err := routecore.NewDurableSnapshotPublisher(snapshotStore, hub.Notifier(), hub.AuthorityNotifier())
	if err != nil {
		t.Fatal(err)
	}
	publisher.RequireControlToken()
	initialPublisher, err := routecore.NewInitialAuthorityBatchPublisher(publisher)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := authority.NewRouteAssignmentManager(authority.ManagerOptions{
		Store: authorityStore, Publisher: initialPublisher, AuthorityReader: reader, InitialSnapshot: snapshot,
		AuthorityLayoutVersion: cfg.authorityLayout, RouteManagerID: cfg.controllerID, RouteManagerBootID: cfg.boot,
		TombstoneStore: switchableTransitions, PlacementExclusions: acceptancePlacementExclusions{},
		InitializationContext: ctx, MemoLimit: 4096, ControlCoordinator: coordinator, ControlScope: controlScope,
	})
	if err != nil {
		t.Fatal(err)
	}
	candidateProvider := &initialAssignmentTestProvider{values: candidateValues}
	runtime, err := newInitialAssignmentRuntimeWithOptions(initialAssignmentRuntimeOptions{
		Config: cfg, Manager: manager, AuthorityReader: reader, Candidates: candidateProvider,
		Control: control, BatchPublisher: initialPublisher, TransitionStore: switchableTransitions, SnapshotStore: snapshotStore,
		PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := runtime.Run(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("initial 1024+1024 batch duration=%s", time.Since(started))
	if !initialPublisher.InitialAuthorityBatchSealed() || !switchableTransitions.Durable() {
		t.Fatal("initial batch did not irreversibly switch to durable adapters")
	}
	final := manager.CurrentSnapshot()
	if final.GlobalRevision != uint64(1+2*shardCount) {
		t.Fatalf("final revision=%d, want %d", final.GlobalRevision, 1+2*shardCount)
	}
	stored, found, err := snapshotStore.LoadCurrent(ctx)
	if err != nil || !found || !reflect.DeepEqual(stored.Snapshot, final) {
		t.Fatalf("durable final found=%t err=%v equal=%t", found, err, found && reflect.DeepEqual(stored.Snapshot, final))
	}
	update := <-sub.Updates
	if update.Revision != final.GlobalRevision {
		t.Fatalf("initial watch revision=%d, want %d", update.Revision, final.GlobalRevision)
	}
	select {
	case extra := <-sub.Updates:
		t.Fatalf("initial batch emitted extra watch update revision=%d", extra.Revision)
	case <-time.After(25 * time.Millisecond):
	}
	for _, domain := range initialAssignmentDomains {
		counts := map[string]int{}
		var routes actorrouting.DomainSnapshot
		if domain == actorrouting.DomainPlayer {
			routes = final.Player
		} else {
			routes = final.Farm
		}
		for shard, route := range routes.Routes {
			holder := final.Holders[route.HolderIndex]
			counts[holder.InstanceID]++
			target, _ := deterministicInitialAssignmentTarget(candidateProvider.values[domain].Holders, uint32(shard))
			if holder.InstanceID != target.HolderID || holder.BootID != target.BootID || route.State != actorrouting.ShardStateActive {
				t.Fatalf("%s/%d target=%s/%s route=%+v want=%s/%s", domain, shard, holder.InstanceID, holder.BootID, route, target.HolderID, target.BootID)
			}
			authorityValue, found, readErr := reader.ReadShardWriteAuthority(ctx, domain, uint32(shard))
			if readErr != nil || !found || authorityValue.HolderID != target.HolderID || authorityValue.HolderBootID != target.BootID ||
				authorityValue.AssignmentRevision != route.ShardRevision || authorityValue.WriterEpoch != route.ExpectedWriterEpoch ||
				authorityValue.LeaseEpoch != route.ExpectedLeaseEpoch || authorityValue.LastTransitionID != initialAssignmentTransitionIDForTarget(cfg.realm, domain, uint32(shard), target) {
				t.Fatalf("%s/%d authority=%+v found=%t err=%v", domain, shard, authorityValue, found, readErr)
			}
		}
		if counts["player-0"] != 512 && domain == actorrouting.DomainPlayer || counts["player-1"] != 512 && domain == actorrouting.DomainPlayer ||
			counts["farm-0"] != 512 && domain == actorrouting.DomainFarm || counts["farm-1"] != 512 && domain == actorrouting.DomainFarm {
			t.Fatalf("%s balance=%v, want 512/512", domain, counts)
		}
	}
	if !reflect.DeepEqual(snapshot.FarmWorker, final.FarmWorker) {
		t.Fatal("FARM_WORKER changed during initial batch")
	}

	beforePost := final.GlobalRevision
	server.SetTime(time.Now().Add(time.Second))
	postCtx, resolvePost, err := control.Begin(ctx, "initial-batch-post-switch", scaleplancore.ControlOperationRoutePlan)
	if err != nil {
		t.Fatal(err)
	}
	authorityValue, found, err := reader.ReadShardWriteAuthority(postCtx, actorrouting.DomainPlayer, 0)
	if err != nil || !found {
		t.Fatalf("post-switch authority found=%t err=%v", found, err)
	}
	postResult, err := manager.Renew(postCtx, authority.RenewRequest{
		Caller: authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerHolder, InstanceID: authorityValue.HolderID, BootID: authorityValue.HolderBootID},
		Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		ExpectedFence: authoritystore.ShardWriteFence{ExpectedHolderID: authorityValue.HolderID, ExpectedHolderBootID: authorityValue.HolderBootID, ExpectedAssignmentRevision: authorityValue.AssignmentRevision, ExpectedWriterEpoch: authorityValue.WriterEpoch, ExpectedLeaseEpoch: authorityValue.LeaseEpoch},
		RequestedTTL:  time.Minute,
	})
	if err != nil || postResult.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("post-switch renew=%+v err=%v", postResult, err)
	}
	if err := resolvePost(); err != nil {
		t.Fatal(err)
	}
	post := manager.CurrentSnapshot()
	if post.GlobalRevision != beforePost+1 {
		t.Fatalf("post-switch revision=%d, want %d", post.GlobalRevision, beforePost+1)
	}
	if extra := <-sub.Updates; extra.Revision != post.GlobalRevision {
		t.Fatalf("post-switch watch revision=%d, want %d", extra.Revision, post.GlobalRevision)
	}
}

func TestInitialAssignmentRejectsForeignAuthorityOnPlaceholderReplay(t *testing.T) {
	values := testCandidateSnapshots(routecore.HolderStateReadyZeroShard)
	reader := &initialAssignmentTestAuthorityReader{values: make(map[initialAssignmentAuthorityKey]authoritystore.ShardWriteAuthority)}
	manager := &initialAssignmentTestManager{snapshot: initialAssignmentTestSnapshot(false, false, values), reader: reader}
	foreign := authoritystore.ShardWriteAuthority{
		AuthorityLayoutVersion: "v1", HashVersion: manager.snapshot.HashVersion, HashSeed: manager.snapshot.HashSeed,
		Domain: actorrouting.DomainPlayer, VirtualShard: 0, HolderID: "foreign", HolderBootID: "foreign-boot",
		AssignmentRevision: 1, WriterEpoch: 1, LeaseEpoch: 1, State: authoritystore.ShardAuthorityActive,
		LeaseUntilMs: time.Now().Add(time.Minute).UnixMilli(), LastTransitionID: "foreign-transition",
	}
	reader.values[initialAssignmentAuthorityKey{domain: actorrouting.DomainPlayer, shard: 0}] = foreign
	runtime, err := newInitialAssignmentRuntimeWithOptions(initialAssignmentRuntimeOptions{
		Config:  config{realm: "realm-a", initialAssignmentMode: initialAssignmentModeAuto, initialAssignmentExpectedPlayerHolders: 1, initialAssignmentExpectedFarmHolders: 1, authorityLayout: "v1"},
		Manager: manager, AuthorityReader: reader, Candidates: &initialAssignmentTestProvider{values: values},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Run(context.Background()); !errors.Is(err, ErrInitialAssignmentForeignActive) {
		t.Fatalf("err=%v, want foreign placeholder authority rejection", err)
	}
	if len(manager.claims) != 0 {
		t.Fatalf("foreign placeholder authority caused %d claims", len(manager.claims))
	}
}

func TestInitialAssignmentResumeSkipsExactActiveRoutesAndRejectsForeignActive(t *testing.T) {
	t.Run("resume exact target", func(t *testing.T) {
		runtime, manager, _ := newInitialAssignmentTestRuntime(t, true, false)
		if err := runtime.Run(context.Background()); err != nil {
			t.Fatal(err)
		}
		if len(manager.claims) != 0 {
			t.Fatalf("resume issued %d claims", len(manager.claims))
		}
	})
	t.Run("foreign active", func(t *testing.T) {
		runtime, manager, _ := newInitialAssignmentTestRuntime(t, true, true)
		err := runtime.Run(context.Background())
		if !errors.Is(err, ErrInitialAssignmentForeignActive) {
			t.Fatalf("err=%v, want foreign active", err)
		}
		if len(manager.claims) != 0 {
			t.Fatalf("foreign active caused %d claims", len(manager.claims))
		}
	})
}

func TestInitialAssignmentRestartAcceptsExactPostMigrationPlacementWithoutModuloCandidates(t *testing.T) {
	runtime, manager, provider := newInitialAssignmentTestRuntime(t, true, false)
	reader, ok := runtime.authorityReader.(*initialAssignmentTestAuthorityReader)
	if !ok {
		t.Fatal("unexpected authority reader")
	}
	leaseUntil := time.Now().Add(time.Minute).UnixMilli()
	targetIndex := len(manager.snapshot.Holders)
	manager.snapshot.Holders = append(manager.snapshot.Holders, actorrouting.HolderRef{
		InstanceID: "player-1", BootID: "player-1-migrated-boot", Endpoint: "player-1.example:9000",
		State: actorrouting.HolderStateActive, Weight: 1,
	})
	manager.snapshot.Player.Routes[0] = actorrouting.ShardRoute{
		HolderIndex: targetIndex, State: actorrouting.ShardStateActive, ShardRevision: 2,
		ExpectedWriterEpoch: 2, ExpectedLeaseEpoch: 2, LeaseUntilMs: leaseUntil,
	}
	manager.snapshot.GlobalRevision = 4 // sealed initial revision is 3 for this 1+1 fixture
	reader.values[initialAssignmentAuthorityKey{domain: actorrouting.DomainPlayer, shard: 0}] = authoritystore.ShardWriteAuthority{
		AuthorityLayoutVersion: "v1", HashVersion: manager.snapshot.HashVersion, HashSeed: manager.snapshot.HashSeed,
		Domain: actorrouting.DomainPlayer, VirtualShard: 0, HolderID: "player-1", HolderBootID: "player-1-migrated-boot",
		AssignmentRevision: 2, WriterEpoch: 2, LeaseEpoch: 2, State: authoritystore.ShardAuthorityActive,
		LeaseUntilMs: leaseUntil, LastTransitionID: "sp-tr-v2-migrated",
	}
	provider.mu.Lock()
	provider.err = errors.New("candidate discovery must not run for durable post-migration restart")
	provider.mu.Unlock()
	if err := runtime.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(manager.claims) != 0 {
		t.Fatalf("restart issued %d initial claims", len(manager.claims))
	}
}

func TestInitialAssignmentTimeoutAndCancellation(t *testing.T) {
	runtime, _, provider := newInitialAssignmentTestRuntime(t, false, false)
	provider.mu.Lock()
	provider.err = ErrInitialAssignmentNotReady
	provider.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := runtime.Run(ctx); !errors.Is(err, ErrInitialAssignmentTimeout) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout err=%v", err)
	}

	runtime, _, provider = newInitialAssignmentTestRuntime(t, false, false)
	provider.mu.Lock()
	provider.err = ErrInitialAssignmentNotReady
	provider.mu.Unlock()
	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	if err := runtime.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel err=%v", err)
	}
}

func TestInitialAssignmentVerifyReadyRemainsFailClosedForStaleLease(t *testing.T) {
	runtime, manager, _ := newInitialAssignmentTestRuntime(t, false, false)
	if err := runtime.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.reader.mu.Lock()
	for key, value := range manager.reader.values {
		value.LeaseUntilMs = time.Unix(100, 0).UnixMilli() - 1
		manager.reader.values[key] = value
	}
	manager.reader.mu.Unlock()
	if err := runtime.VerifyReady(context.Background()); !errors.Is(err, ErrInitialAssignmentStaleLease) {
		t.Fatalf("VerifyReady err=%v, want stale lease", err)
	}
}

func TestInitialAssignmentClaimUsesControlBootstrapContextAndReadinessStaysClosedUntilComplete(t *testing.T) {
	manager := &initialAssignmentTestManager{}
	reader := &initialAssignmentTestAuthorityReader{values: make(map[initialAssignmentAuthorityKey]authoritystore.ShardWriteAuthority)}
	provider := &initialAssignmentTestProvider{values: testCandidateSnapshots(routecore.HolderStateReadyZeroShard)}
	cfg := config{realm: "realm-a", initialAssignmentMode: initialAssignmentModeAuto, initialAssignmentExpectedPlayerHolders: 1, initialAssignmentExpectedFarmHolders: 1, authorityLayout: "v1"}
	manager.snapshot = initialAssignmentTestSnapshot(false, false, provider.values)
	manager.reader = reader
	scope := scaleplancore.MutationScope{Namespace: "namespace-a", Realm: "realm-a"}
	owner := scaleplancore.MutationLeaseOwner{ControllerID: "controller-a", ControllerBootID: "boot-a"}
	inner, err := scaleplancontrol.NewControlMutationCoordinator(scaleplancontrol.ControlMutationCoordinatorOptions{Scope: scope, Now: time.Now, Epoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	control := &initialAssignmentTestCoordinator{inner: inner, scope: scope, owner: owner}
	runtime, err := newInitialAssignmentRuntimeWithOptions(initialAssignmentRuntimeOptions{
		Config: cfg, Manager: manager, AuthorityReader: reader, Candidates: provider,
		Control: routeplan.ControlMutationOptions{
			Coordinator: control,
			Scope:       scope,
			Owner:       owner,
			NonceSource: func() (string, error) { return "nonce-a", nil },
		},
		Now: func() time.Time { return time.Unix(100, 0) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(manager.claims) != 2 || control.beginCount != 2 || control.resolveCount != 2 {
		t.Fatalf("claims=%d begin=%d resolve=%d, want two gated claims", len(manager.claims), control.beginCount, control.resolveCount)
	}
	for _, claim := range manager.claims {
		target := provider.values[claim.Domain].Holders[0]
		if claim.TransitionID != initialAssignmentTransitionIDForTarget("realm-a", claim.Domain, claim.VirtualShard, target) {
			t.Fatalf("transition id=%q is not deterministic", claim.TransitionID)
		}
	}
}

type initialAssignmentAuthorityKey struct {
	domain actorrouting.RouteDomain
	shard  uint32
}

type initialAssignmentTestAuthorityReader struct {
	mu     sync.Mutex
	values map[initialAssignmentAuthorityKey]authoritystore.ShardWriteAuthority
	err    error
}

func (r *initialAssignmentTestAuthorityReader) ReadShardWriteAuthority(ctx context.Context, domain actorrouting.RouteDomain, shard uint32) (authoritystore.ShardWriteAuthority, bool, error) {
	if err := ctx.Err(); err != nil {
		return authoritystore.ShardWriteAuthority{}, false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return authoritystore.ShardWriteAuthority{}, false, r.err
	}
	value, found := r.values[initialAssignmentAuthorityKey{domain: domain, shard: shard}]
	return value, found, nil
}

type initialAssignmentTestManager struct {
	mu       sync.Mutex
	snapshot actorrouting.RouteSnapshot
	reader   *initialAssignmentTestAuthorityReader
	claims   []authority.ClaimRequest
	err      error
}

func (m *initialAssignmentTestManager) CurrentSnapshot() actorrouting.RouteSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return actorrouting.CloneRouteSnapshot(m.snapshot)
}

func (m *initialAssignmentTestManager) Claim(ctx context.Context, request authority.ClaimRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	if err := ctx.Err(); err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, m.err
	}
	m.claims = append(m.claims, request)
	index := -1
	for i, holder := range m.snapshot.Holders {
		if holder.InstanceID == request.NewHolderID && holder.BootID == request.NewHolderBootID {
			index = i
			break
		}
	}
	if index < 0 {
		m.snapshot.Holders = append(m.snapshot.Holders, actorrouting.HolderRef{InstanceID: request.NewHolderID, BootID: request.NewHolderBootID, Endpoint: request.Endpoint, State: actorrouting.HolderStateActive, Weight: 1})
		index = len(m.snapshot.Holders) - 1
	}
	routes := initialAssignmentTestDomain(&m.snapshot, request.Domain)
	route := routes.Routes[request.VirtualShard]
	route.HolderIndex = index
	route.State = actorrouting.ShardStateActive
	route.ShardRevision, route.ExpectedWriterEpoch, route.ExpectedLeaseEpoch = 1, 1, 1
	route.LeaseUntilMs = time.Now().Add(time.Minute).UnixMilli()
	routes.Routes[request.VirtualShard] = route
	m.snapshot.GlobalRevision++
	value := authoritystore.ShardWriteAuthority{
		AuthorityLayoutVersion: "v1", HashVersion: m.snapshot.HashVersion, HashSeed: m.snapshot.HashSeed,
		Domain: request.Domain, VirtualShard: request.VirtualShard, HolderID: request.NewHolderID,
		HolderBootID: request.NewHolderBootID, AssignmentRevision: 1, WriterEpoch: 1, LeaseEpoch: 1,
		State: authoritystore.ShardAuthorityActive, LeaseUntilMs: route.LeaseUntilMs, LastTransitionID: request.TransitionID,
	}
	m.reader.mu.Lock()
	m.reader.values[initialAssignmentAuthorityKey{domain: request.Domain, shard: request.VirtualShard}] = value
	m.reader.mu.Unlock()
	return authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionApplied, Authority: value, AcceptedTTLms: initialAssignmentClaimTTL.Milliseconds()}, nil
}

type initialAssignmentTestProvider struct {
	mu     sync.Mutex
	values map[actorrouting.RouteDomain]initialAssignmentCandidateSnapshot
	err    error
}

func (p *initialAssignmentTestProvider) Candidates(ctx context.Context, domain actorrouting.RouteDomain) (initialAssignmentCandidateSnapshot, error) {
	return p.observe(ctx, domain)
}

func (p *initialAssignmentTestProvider) ObserveGeneration(ctx context.Context, domain actorrouting.RouteDomain) (initialAssignmentCandidateSnapshot, error) {
	return p.observe(ctx, domain)
}

func (p *initialAssignmentTestProvider) observe(ctx context.Context, domain actorrouting.RouteDomain) (initialAssignmentCandidateSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return initialAssignmentCandidateSnapshot{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return initialAssignmentCandidateSnapshot{}, p.err
	}
	value, found := p.values[domain]
	if !found {
		return initialAssignmentCandidateSnapshot{}, ErrInitialAssignmentNotReady
	}
	value.Holders = append([]routecore.HolderRecord(nil), value.Holders...)
	return value, nil
}

func newInitialAssignmentTestRuntime(t *testing.T, active, foreign bool) (*initialAssignmentRuntime, *initialAssignmentTestManager, *initialAssignmentTestProvider) {
	t.Helper()
	values := testCandidateSnapshots(routecore.HolderStateReadyZeroShard)
	if active {
		values = testCandidateSnapshots(routecore.HolderStateServing)
	}
	provider := &initialAssignmentTestProvider{values: values}
	reader := &initialAssignmentTestAuthorityReader{values: make(map[initialAssignmentAuthorityKey]authoritystore.ShardWriteAuthority)}
	manager := &initialAssignmentTestManager{snapshot: initialAssignmentTestSnapshot(active, foreign, values), reader: reader}
	if active {
		for _, domain := range initialAssignmentDomains {
			target := values[domain].Holders[0]
			if foreign {
				target.HolderID = "foreign"
				target.BootID = "foreign-boot"
			}
			route, _, _ := initialAssignmentRoute(manager.snapshot, domain, 0)
			authorityValue := authoritystore.ShardWriteAuthority{
				AuthorityLayoutVersion: "v1", HashVersion: manager.snapshot.HashVersion, HashSeed: manager.snapshot.HashSeed,
				Domain: domain, VirtualShard: 0, HolderID: target.HolderID, HolderBootID: target.BootID,
				AssignmentRevision: route.ShardRevision, WriterEpoch: route.ExpectedWriterEpoch, LeaseEpoch: route.ExpectedLeaseEpoch,
				State: authoritystore.ShardAuthorityActive, LeaseUntilMs: time.Now().Add(time.Minute).UnixMilli(), LastTransitionID: initialAssignmentTransitionIDForTarget("realm-a", domain, 0, target),
			}
			reader.values[initialAssignmentAuthorityKey{domain: domain, shard: 0}] = authorityValue
		}
	}
	cfg := config{realm: "realm-a", initialAssignmentMode: initialAssignmentModeAuto, initialAssignmentExpectedPlayerHolders: 1, initialAssignmentExpectedFarmHolders: 1, authorityLayout: "v1"}
	runtime, err := newInitialAssignmentRuntimeWithOptions(initialAssignmentRuntimeOptions{Config: cfg, Manager: manager, AuthorityReader: reader, Candidates: provider})
	if err != nil {
		t.Fatal(err)
	}
	return runtime, manager, provider
}

func TestInitialAssignmentHolderIdentityIgnoresMutableLifecycleSkew(t *testing.T) {
	holder := verifiedholder.Holder{
		Domain: actorrouting.DomainFarm, HolderID: "farm-0", BootID: "boot-0",
		PodUID: "pod-0", WorkloadUID: "workload-0", Ordinal: 0,
		Lifecycle: verifiedholder.LifecycleReadyZeroShard, ProcessReady: true,
	}
	record := routecore.HolderRecord{
		Domain: actorrouting.DomainFarm, HolderID: "farm-0", BootID: "boot-0",
		DeploymentIdentity: holderidentity.DeploymentIdentity{PodUID: "pod-0", WorkloadUID: "workload-0", Ordinal: 0},
		State:              routecore.HolderStateServing, ProcessReady: true, ServingShardCount: 317,
	}
	if !sameInitialAssignmentHolderIdentity(holder, record) {
		t.Fatal("mutable lifecycle skew changed immutable holder generation")
	}
	for name, mutate := range map[string]func(*routecore.HolderRecord){
		"boot":     func(value *routecore.HolderRecord) { value.BootID = "other" },
		"pod":      func(value *routecore.HolderRecord) { value.DeploymentIdentity.PodUID = "other" },
		"workload": func(value *routecore.HolderRecord) { value.DeploymentIdentity.WorkloadUID = "other" },
		"ordinal":  func(value *routecore.HolderRecord) { value.DeploymentIdentity.Ordinal = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := record
			mutate(&candidate)
			if sameInitialAssignmentHolderIdentity(holder, candidate) {
				t.Fatal("foreign immutable generation accepted")
			}
		})
	}
}

func TestInitialAssignmentPinnedVerifyIgnoresExpiredVerifiedLease(t *testing.T) {
	runtime, _, provider := newInitialAssignmentTestRuntime(t, false, false)
	runtime.resume = true
	pinned := testCandidateSnapshots(routecore.HolderStateReadyZeroShard)
	expired := time.Now().Add(-time.Minute)
	provider.mu.Lock()
	for domain, snapshot := range provider.values {
		snapshot.Holders[0].LeaseUntil = expired
		snapshot.Holders[0].State = routecore.HolderStateServing
		provider.values[domain] = snapshot
	}
	provider.mu.Unlock()
	if err := runtime.verifyPinnedCandidates(context.Background(), pinned); err != nil {
		t.Fatalf("expired verified TTL treated as identity drift: %v", err)
	}
}

func TestInitialAssignmentRefreshTransientFailureRetriesThenCompletes(t *testing.T) {
	runtime, _, _ := newInitialAssignmentTestRuntime(t, false, false)
	runtime.resume = true
	pinned := testCandidateSnapshots(routecore.HolderStateReadyZeroShard)
	var mu sync.Mutex
	attempts := 0
	runtime.refreshDomain = func(ctx context.Context, domain actorrouting.RouteDomain) error {
		mu.Lock()
		attempts++
		n := attempts
		mu.Unlock()
		if n <= 2 {
			return fmt.Errorf("kubernetes bracket temporarily unavailable")
		}
		return nil
	}
	runtime.refreshRetry = func(int) time.Duration { return time.Millisecond }
	if err := runtime.refreshAndVerifyPinnedCandidates(context.Background(), pinned); err != nil {
		t.Fatalf("transient refresh failure was not retried to completion: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	// Two domains × two rounds (fail, fail) + one successful round.
	if attempts != 4 {
		t.Fatalf("refresh attempts=%d, want 4", attempts)
	}
}

func TestInitialAssignmentRefreshPersistentFailureTimesOutAsNotReady(t *testing.T) {
	runtime, _, _ := newInitialAssignmentTestRuntime(t, false, false)
	runtime.resume = true
	pinned := testCandidateSnapshots(routecore.HolderStateReadyZeroShard)
	runtime.refreshDomain = func(context.Context, actorrouting.RouteDomain) error {
		return errors.New("holder control identity RPC unavailable")
	}
	runtime.refreshRetry = func(int) time.Duration { return time.Millisecond }
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := runtime.refreshAndVerifyPinnedCandidates(ctx, pinned)
	if err == nil {
		t.Fatal("persistent refresh failure must not succeed")
	}
	if errors.Is(err, ErrInitialAssignmentIdentityDrift) {
		t.Fatalf("persistent transient refresh failure misreported as identity drift: %v", err)
	}
	if !errors.Is(err, ErrInitialAssignmentTimeout) {
		t.Fatalf("persistent refresh failure should surface as initial assignment timeout, got: %v", err)
	}
}

func testCandidateSnapshots(state routecore.HolderState) map[actorrouting.RouteDomain]initialAssignmentCandidateSnapshot {
	values := make(map[actorrouting.RouteDomain]initialAssignmentCandidateSnapshot, 2)
	for _, domain := range initialAssignmentDomains {
		prefix := "player"
		if domain == actorrouting.DomainFarm {
			prefix = "farm"
		}
		id := prefix + "-0"
		holder := initialAssignmentTestHolder(domain, id, id+"-boot", 0, state, 1)
		key := verifiedholder.WorkloadKey{Realm: "realm-a", Domain: domain, Namespace: string(domain), Kind: "StatefulSet", Name: string(domain) + "-svr"}
		values[domain] = initialAssignmentCandidateSnapshot{
			Domain: domain, WorkloadKey: key, WorkloadUID: string(domain) + "-workload", KubeGeneration: 1,
			KubeResourceVersion: "rv-1", KubeListResourceVersion: "list-rv-1", Holders: []routecore.HolderRecord{holder},
		}
	}
	return values
}

func initialAssignmentTestHolder(domain actorrouting.RouteDomain, id, boot string, ordinal int32, state routecore.HolderState, serving uint32) routecore.HolderRecord {
	return routecore.HolderRecord{
		Domain: domain, HolderID: id, BootID: boot,
		DeploymentIdentity: holderidentity.DeploymentIdentity{PodUID: id + "-pod", WorkloadUID: id + "-workload", Ordinal: ordinal},
		LogicEndpoint:      id + ".example:9000", HolderControlEndpoint: id + ".example:19100", TLSDNSIdentity: id + ".example",
		State: state, ProcessReady: true, ServingShardCount: serving, LeaseUntil: time.Now().Add(time.Minute), CapacityWeight: 1,
	}
}

func initialAssignmentTestSnapshot(active, foreign bool, values map[actorrouting.RouteDomain]initialAssignmentCandidateSnapshot) actorrouting.RouteSnapshot {
	snapshot := actorrouting.RouteSnapshot{Realm: "realm-a", GlobalRevision: 1, HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 1}
	snapshot.Holders = []actorrouting.HolderRef{
		{InstanceID: "old-player", BootID: "old-player-boot", Endpoint: "old-player.example:9000", State: actorrouting.HolderStateActive, Weight: 1},
		{InstanceID: "old-farm", BootID: "old-farm-boot", Endpoint: "old-farm.example:9000", State: actorrouting.HolderStateActive, Weight: 1},
	}
	for _, domain := range initialAssignmentDomains {
		var table *actorrouting.DomainSnapshot
		if domain == actorrouting.DomainPlayer {
			table = &snapshot.Player
		} else {
			table = &snapshot.Farm
		}
		table.ShardCount = 1
		table.Routes = []actorrouting.ShardRoute{{HolderIndex: 0, State: actorrouting.ShardStateAssigning, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1}}
		if domain == actorrouting.DomainFarm {
			table.Routes[0].HolderIndex = 1
		}
		if active {
			target := values[domain].Holders[0]
			if foreign {
				target.HolderID = "foreign"
				target.BootID = "foreign-boot"
			}
			index := len(snapshot.Holders)
			snapshot.Holders = append(snapshot.Holders, actorrouting.HolderRef{InstanceID: target.HolderID, BootID: target.BootID, Endpoint: target.LogicEndpoint, State: actorrouting.HolderStateActive, Weight: 1})
			table.Routes[0] = actorrouting.ShardRoute{HolderIndex: index, State: actorrouting.ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: time.Now().Add(time.Minute).UnixMilli()}
		}
	}
	return snapshot
}

func initialAssignmentTestDomain(snapshot *actorrouting.RouteSnapshot, domain actorrouting.RouteDomain) *actorrouting.DomainSnapshot {
	if domain == actorrouting.DomainPlayer {
		return &snapshot.Player
	}
	return &snapshot.Farm
}

func initialAssignmentBatchAcceptanceSnapshot(realm string, shards uint32) actorrouting.RouteSnapshot {
	snapshot := actorrouting.RouteSnapshot{Realm: realm, GlobalRevision: 1, HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 1,
		Holders: []actorrouting.HolderRef{
			{InstanceID: "placeholder-player", BootID: "placeholder-player-boot", Endpoint: "placeholder-player.example:9000", State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: "placeholder-farm", BootID: "placeholder-farm-boot", Endpoint: "placeholder-farm.example:9000", State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: "placeholder-worker", BootID: "placeholder-worker-boot", Endpoint: "placeholder-worker.example:9000", State: actorrouting.HolderStateActive, Weight: 1},
		}}
	snapshot.Player = actorrouting.DomainSnapshot{ShardCount: shards, Routes: make([]actorrouting.ShardRoute, shards)}
	snapshot.Farm = actorrouting.DomainSnapshot{ShardCount: shards, Routes: make([]actorrouting.ShardRoute, shards)}
	snapshot.FarmWorker = actorrouting.DomainSnapshot{ShardCount: shards, Routes: make([]actorrouting.ShardRoute, shards)}
	for shard := uint32(0); shard < shards; shard++ {
		snapshot.Player.Routes[shard] = actorrouting.ShardRoute{HolderIndex: 0, State: actorrouting.ShardStateAssigning, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1}
		snapshot.Farm.Routes[shard] = actorrouting.ShardRoute{HolderIndex: 1, State: actorrouting.ShardStateAssigning, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1}
		snapshot.FarmWorker.Routes[shard] = actorrouting.ShardRoute{HolderIndex: 2, State: actorrouting.ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: time.Now().Add(10 * time.Minute).UnixMilli()}
	}
	return snapshot
}

func initialAssignmentBatchAcceptanceCandidates(realm string, shards uint32) map[actorrouting.RouteDomain]initialAssignmentCandidateSnapshot {
	values := make(map[actorrouting.RouteDomain]initialAssignmentCandidateSnapshot, len(initialAssignmentDomains))
	for _, domain := range initialAssignmentDomains {
		prefix := "player"
		if domain == actorrouting.DomainFarm {
			prefix = "farm"
		}
		key := verifiedholder.WorkloadKey{Realm: realm, Domain: domain, Namespace: "batch", Kind: "StatefulSet", Name: prefix + "-svr"}
		holders := []routecore.HolderRecord{
			initialAssignmentTestHolder(domain, prefix+"-0", prefix+"-0-boot", 0, routecore.HolderStateReadyZeroShard, 0),
			initialAssignmentTestHolder(domain, prefix+"-1", prefix+"-1-boot", 1, routecore.HolderStateReadyZeroShard, 0),
		}
		for i := range holders {
			holders[i].LeaseUntil = time.Now().Add(10 * time.Minute)
		}
		values[domain] = initialAssignmentCandidateSnapshot{Domain: domain, WorkloadKey: key, WorkloadUID: prefix + "-workload", KubeGeneration: 1, KubeResourceVersion: "rv-1", KubeListResourceVersion: "list-rv-1", Holders: holders}
	}
	_ = shards
	return values
}

type acceptancePlacementExclusions struct{}

func (acceptancePlacementExclusions) GetPlacementExclusion(context.Context, actorrouting.RouteDomain, string, string) (routecore.PlacementExclusion, bool, error) {
	return routecore.PlacementExclusion{}, false, nil
}
func (acceptancePlacementExclusions) ExcludePlacement(context.Context, actorrouting.RouteDomain, string, string, routecore.PlacementExclusionPrecondition) (routecore.PlacementExclusionResult, error) {
	return routecore.PlacementExclusionResult{Committed: true}, nil
}
func (acceptancePlacementExclusions) ReleasePlacementExclusionForOwner(context.Context, routecore.NestedControlOperationToken, actorrouting.RouteDomain, string, string, routecore.PlacementExclusionRelease) (routecore.PlacementExclusionResult, error) {
	return routecore.PlacementExclusionResult{Committed: true}, nil
}
func (acceptancePlacementExclusions) ValidatePlacementTarget(context.Context, actorrouting.RouteDomain, string, string) (uint64, error) {
	return 1, nil
}

type initialAssignmentTestCoordinator struct {
	inner        scaleplancontrol.ExclusiveControlCoordinator
	beginCount   int
	resolveCount int
	scope        scaleplancore.MutationScope
	owner        scaleplancore.MutationLeaseOwner
}

func (c *initialAssignmentTestCoordinator) Acquire(ctx context.Context, request scaleplancore.ControlOperationRequest) (scaleplancore.ControlOperationToken, error) {
	c.beginCount++
	return c.inner.Acquire(ctx, request)
}
func (c *initialAssignmentTestCoordinator) Verify(ctx context.Context, token scaleplancore.ControlOperationToken, scope scaleplancore.MutationScope) error {
	return c.inner.Verify(ctx, token, scope)
}
func (c *initialAssignmentTestCoordinator) Resolve(ctx context.Context, token scaleplancore.ControlOperationToken) error {
	c.resolveCount++
	return c.inner.Resolve(ctx, token)
}
func (c *initialAssignmentTestCoordinator) Mode(ctx context.Context, scope scaleplancore.MutationScope) (scaleplancore.ControlGateMode, error) {
	return c.inner.Mode(ctx, scope)
}

var _ scaleplancore.ControlMutationCoordinator = (*initialAssignmentTestCoordinator)(nil)
