package authoritycomposition

import (
	"context"
	"encoding/json"
	"errors"
	"farmgame/internal/controlfence"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	playerpb "farmgame/api/gen/player"
	"farmgame/internal/actorrouting"
	"farmgame/internal/contracts"
	farmactor "farmgame/internal/farm/actor"
	farmingress "farmgame/internal/farm/ingress"
	routeauthority "farmgame/internal/logic/actor/authority"
	playeractor "farmgame/internal/player/actor"
	playerparticipant "farmgame/internal/player/participant"
	playertask "farmgame/internal/player/task"
	"farmgame/internal/storage/authoritystore"
	redisauthority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workflow"
	"farmgame/internal/workflow/participantproof"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

const compositionHashSeed uint64 = 202608050001

type compositionFixture struct {
	server  *miniredis.Miniredis
	client  *redis.Client
	profile rediskey.PhysicalProfile
	calc    *actorrouting.Calculator
	store   *redisauthority.Store
	path    string
	snap    actorrouting.RouteSnapshot
}

func newCompositionFixture(t *testing.T) *compositionFixture {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	namespace, err := rediskey.NewNamespace("test", "z1")
	if err != nil {
		t.Fatal(err)
	}
	profile := rediskey.PhysicalProfile{
		ProfileVersion: "p1", Namespace: namespace,
		AuthorityLayoutVersion: "v1", WorkflowLayoutVersion: "v1",
		WorkflowHashVersion: 1, WorkflowBucketCount: 64,
	}
	calc, err := actorrouting.NewCalculator("test-realm", actorrouting.HashProfile{
		Version: actorrouting.HashVersionXXH64V2, Seed: compositionHashSeed,
	}, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	scripts, err := redisauthority.NewScriptRegistry(client)
	if err != nil {
		t.Fatal(err)
	}
	if err := scripts.EnsureLoaded(context.Background()); err != nil {
		t.Fatal(err)
	}
	store, err := redisauthority.NewStore(client, scripts, redisauthority.Options{
		Profile: profile, Resolver: calc, LeaseLimits: authoritystore.DefaultLeaseLimits(),
	})
	if err != nil {
		t.Fatal(err)
	}
	playerClaim := claimCompositionShard(t, store, actorrouting.DomainPlayer, 0, "player-pod-1", "player-boot-1", "claim-player-1")
	farmClaim := claimCompositionShard(t, store, actorrouting.DomainFarm, 0, "farm-pod-1", "farm-boot-1", "claim-farm-1")
	snapshot := actorrouting.RouteSnapshot{
		Realm: "test-realm", GlobalRevision: 1,
		HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: compositionHashSeed,
		Holders: []actorrouting.HolderRef{
			{InstanceID: "player-pod-1", BootID: "player-boot-1", Endpoint: "player.local:9100", State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: "farm-pod-1", BootID: "farm-boot-1", Endpoint: "farm.local:9090", State: actorrouting.HolderStateActive, Weight: 1},
		},
		Player: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{
			HolderIndex: 0, State: actorrouting.ShardStateActive,
			ShardRevision:       playerClaim.Authority.AssignmentRevision,
			ExpectedWriterEpoch: playerClaim.Authority.WriterEpoch,
			ExpectedLeaseEpoch:  playerClaim.Authority.LeaseEpoch,
			LeaseUntilMs:        playerClaim.Authority.LeaseUntilMs,
		}}},
		Farm: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{
			HolderIndex: 1, State: actorrouting.ShardStateActive,
			ShardRevision:       farmClaim.Authority.AssignmentRevision,
			ExpectedWriterEpoch: farmClaim.Authority.WriterEpoch,
			ExpectedLeaseEpoch:  farmClaim.Authority.LeaseEpoch,
			LeaseUntilMs:        farmClaim.Authority.LeaseUntilMs,
		}}},
	}
	path := filepath.Join(t.TempDir(), "route.json")
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return &compositionFixture{server: server, client: client, profile: profile, calc: calc, store: store, path: path, snap: snapshot}
}

func claimCompositionShard(t *testing.T, store *redisauthority.Store, domain actorrouting.RouteDomain, shard uint32, holder, boot, transition string) authoritystore.ShardAuthorityTransitionResult {
	t.Helper()
	result, err := store.ClaimShardAuthority(context.Background(), authoritystore.ClaimShardAuthorityRequest{
		Caller:       authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerRouteManager, InstanceID: "route-manager-1", BootID: "route-manager-boot-1"},
		TransitionID: transition, AuthorityLayoutVersion: "v1", Domain: domain, VirtualShard: shard,
		NewHolderID: holder, NewHolderBootID: boot, RequestedTTLms: 5000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("claim %s status=%s", domain, result.Status)
	}
	return result
}

func compositionOptions(fixture *compositionFixture, domain actorrouting.RouteDomain, holder, boot string) Options {
	return Options{
		Client: fixture.client, Domain: domain, RouteSnapshotPath: fixture.path,
		HolderID: holder, HolderBootID: boot, Profile: fixture.profile,
		LeaseTTL: 3 * time.Second, SafetyMargin: 100 * time.Millisecond,
		RenewInterval: 2 * time.Second, LeaseLimits: authoritystore.DefaultLeaseLimits(), Now: time.Now,
	}
}

func TestRedisProcessCompositionActivatesAuthorityLoadsCommitsAndReloads(t *testing.T) {
	fixture := newCompositionFixture(t)
	runtime, err := New(context.Background(), compositionOptions(fixture, actorrouting.DomainPlayer, "player-pod-1", "player-boot-1"))
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	if !runtime.Ready() {
		t.Fatal("authority runtime is not ready after exact lease activation")
	}
	if !runtime.ProcessReady() {
		t.Fatal("authority runtime process readiness is not reported after activation")
	}
	if !runtime.CanServeShard(actorrouting.DomainPlayer, 0) {
		t.Fatal("installed player shard 0 is not reported as servable")
	}
	if runtime.ServingShardCount() != 1 {
		t.Fatalf("ServingShardCount=%d, want 1", runtime.ServingShardCount())
	}

	registry := playeractor.NewAuthorityRegistry(runtime.Provider, playeractor.RegistryOptions{MailboxSize: 16})
	defer registry.Stop()
	ctx := context.Background()
	if _, version, err := registry.Ensure(ctx, 7001, "bootstrap-7001"); err != nil || version != 1 {
		t.Fatalf("ensure version=%d err=%v", version, err)
	}
	if _, err := registry.Submit(ctx, 7001, func(record *playerpb.PlayerRecord) (any, bool, error) {
		record.Wallet.Coin--
		return record.Wallet.Coin, true, nil
	}); err != nil {
		t.Fatal(err)
	}
	registry.Stop()
	reloaded := playeractor.NewAuthorityRegistry(runtime.Provider, playeractor.RegistryOptions{MailboxSize: 16})
	defer reloaded.Stop()
	record, version, err := reloaded.Read(ctx, 7001)
	if err != nil {
		t.Fatal(err)
	}
	if version != 2 || record.Wallet.Coin != playeractor.DefaultInitialCoin-1 {
		t.Fatalf("reloaded record coin=%d version=%d", record.Wallet.Coin, version)
	}

	workRepository, err := workflow.NewRedisWorkRepository(fixture.client, "farm:test:composition:workflow")
	if err != nil {
		t.Fatal(err)
	}
	routeFence, err := NewParticipantRouteFence(runtime)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := participantproof.New(workRepository,
		participantproof.WithBucketResolver(runtime.Placement),
		participantproof.WithRouteFenceVerifier(routeFence),
		participantproof.WithClock(time.Now),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := playerparticipant.NewServerChecked(playerparticipant.Options{
		Registry: reloaded, WorkVersionVerifier: proof,
		WorkLeaseVerifier: proof, RouteFenceVerifier: proof, WorkAddressVerifier: proof,
		IdentityBindingVerifier: proof, PlantDependencyVerifier: proof,
		CareDependencyVerifier: playerparticipant.CareDependencyVerifierFunc(func(context.Context, *contracts.ParticipantCall) (playerparticipant.CareDependencyProof, error) {
			return playerparticipant.CareDependencyProof{}, errors.New("composition test does not admit Care")
		}),
		TaskConfigRepository: playertask.MustNewP0Repository(),
		NowMs:                func() int64 { return time.Now().UnixMilli() }, Now: time.Now,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRedisProcessCompositionRejectsStaleLeaseAndBootAndAcceptsNewWriterGeneration(t *testing.T) {
	fixture := newCompositionFixture(t)
	base := time.Now()
	var mu sync.RWMutex
	now := base
	clock := func() time.Time { mu.RLock(); defer mu.RUnlock(); return now }
	options := compositionOptions(fixture, actorrouting.DomainPlayer, "player-pod-1", "player-boot-1")
	options.Now = clock
	runtime, err := New(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	if !runtime.Ready() {
		t.Fatal("runtime should be ready before lease expiry")
	}
	actorAddress := authoritystore.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: 1}
	if _, err := runtime.FenceSource.WriteFence(context.Background(), actorAddress); err != nil {
		t.Fatal(err)
	}

	old := runtime.GuardsSnapshotForTest(actorrouting.DomainPlayer, 0)
	transfer, err := runtime.Store.TransferShardAuthority(context.Background(), authoritystore.TransferShardAuthorityRequest{
		Caller:       authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerRouteManager, InstanceID: "route-manager-2", BootID: "route-manager-boot-2"},
		TransitionID: "transfer-player-2", AuthorityLayoutVersion: "v1", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		ExpectedFence: old, NewHolderID: "player-pod-2", NewHolderBootID: "player-boot-2", RequestedTTLms: 5000,
	})
	if err != nil || transfer.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("transfer=%+v err=%v", transfer, err)
	}
	next := runtime.AssignmentProvider.Snapshot()
	next.GlobalRevision = 2
	next.Holders = append(next.Holders, actorrouting.HolderRef{InstanceID: "player-pod-2", BootID: "player-boot-2", Endpoint: "player-2.local:9100", State: actorrouting.HolderStateActive, Weight: 1})
	next.Player.Routes[0] = actorrouting.ShardRoute{
		HolderIndex: 2, State: actorrouting.ShardStateActive,
		ShardRevision: transfer.Authority.AssignmentRevision, ExpectedWriterEpoch: transfer.Authority.WriterEpoch,
		ExpectedLeaseEpoch: transfer.Authority.LeaseEpoch, LeaseUntilMs: transfer.Authority.LeaseUntilMs,
	}
	if err := runtime.publisher.Publish(context.Background(), controlfence.NestedControlOperationToken{}, next); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for runtime.Applier.LastGlobalRevision() != 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if _, err := runtime.FenceSource.WriteFence(context.Background(), actorAddress); err == nil {
		t.Fatal("old holder fence admitted after route generation moved")
	}

	newPath := filepath.Join(t.TempDir(), "route-new.json")
	data, err := json.Marshal(next)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(newPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	newOptions := options
	newOptions.RouteSnapshotPath = newPath
	newOptions.HolderID = "player-pod-2"
	newOptions.HolderBootID = "player-boot-2"
	newOptions.Now = time.Now
	newRuntime, err := New(context.Background(), newOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer newRuntime.Close()
	if !newRuntime.Ready() {
		t.Fatal("new writer generation did not become ready")
	}

	staleOptions := compositionOptions(fixture, actorrouting.DomainPlayer, "player-pod-1", "player-boot-2")
	if _, err := New(context.Background(), staleOptions); err == nil {
		t.Fatal("stale holder boot unexpectedly activated")
	}

	mu.Lock()
	now = base.Add(10 * time.Second)
	mu.Unlock()
	fixture.server.FastForward(10 * time.Second)
	if runtime.CanServeShard(actorrouting.DomainPlayer, 0) {
		t.Fatal("expired local lease remained shard-servable")
	}
	if runtime.ServingShardCount() != 0 {
		t.Fatalf("expired local lease still counted as serving: %d", runtime.ServingShardCount())
	}
	// Process readiness is common-dependency health only; it must stay true
	// even after a local shard lease expires, so a holder can continue
	// observing assignments while serving zero shards.
	if !runtime.ProcessReady() {
		t.Fatal("process readiness dropped after a single shard lease expired")
	}
}

func (r *Runtime) GuardsSnapshotForTest(domain actorrouting.RouteDomain, shard uint32) authoritystore.ShardWriteFence {
	snapshot, ok := r.Guards.Snapshot(domain, shard)
	if !ok {
		return authoritystore.ShardWriteFence{}
	}
	return authoritystore.ShardWriteFence{
		ExpectedHolderID: snapshot.Authority.HolderID, ExpectedHolderBootID: snapshot.Authority.HolderBootID,
		ExpectedAssignmentRevision: snapshot.Authority.AssignmentRevision, ExpectedWriterEpoch: snapshot.Authority.WriterEpoch,
		ExpectedLeaseEpoch: snapshot.Authority.LeaseEpoch,
	}
}

func TestRedisProcessCompositionKeepsPlayerAndFarmAuthorityNamespacesSeparate(t *testing.T) {
	fixture := newCompositionFixture(t)
	playerRuntime, err := New(context.Background(), compositionOptions(fixture, actorrouting.DomainPlayer, "player-pod-1", "player-boot-1"))
	if err != nil {
		t.Fatal(err)
	}
	defer playerRuntime.Close()
	farmRuntime, err := New(context.Background(), compositionOptions(fixture, actorrouting.DomainFarm, "farm-pod-1", "farm-boot-1"))
	if err != nil {
		t.Fatal(err)
	}
	defer farmRuntime.Close()
	if _, err := playerRuntime.FenceSource.WriteFence(context.Background(), authoritystore.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: 1}); err == nil {
		t.Fatal("PLAYER runtime admitted FARM authority")
	}
	if _, err := farmRuntime.FenceSource.WriteFence(context.Background(), authoritystore.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: 1}); err == nil {
		t.Fatal("FARM runtime admitted PLAYER authority")
	}
	farmRegistry := farmactor.NewAuthorityRegistry(farmRuntime.Provider, farmactor.RegistryOptions{MailboxSize: 16})
	defer farmRegistry.Stop()
	workRepository, err := workflow.NewRedisWorkRepository(fixture.client, "farm:test:composition:farm-workflow")
	if err != nil {
		t.Fatal(err)
	}
	farmFence, err := NewParticipantRouteFence(farmRuntime)
	if err != nil {
		t.Fatal(err)
	}
	farmProof, err := participantproof.New(workRepository,
		participantproof.WithBucketResolver(farmRuntime.Placement),
		participantproof.WithRouteFenceVerifier(farmFence),
		participantproof.WithClock(time.Now),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := farmingress.NewServerChecked(farmingress.Options{
		Registry: farmRegistry, WorkVersionVerifier: farmProof, WorkLeaseVerifier: farmProof,
		RouteFenceVerifier: farmProof, WorkAddressVerifier: farmProof,
		IdentityBindingVerifier: farmProof, CommitPlantDependency: farmProof, Now: time.Now,
	}); err != nil {
		t.Fatal(err)
	}
	playerRef, err := fixture.profile.AuthorityBucketForActor(fixture.calc, authoritystore.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: 1})
	if err != nil {
		t.Fatal(err)
	}
	farmRef, err := fixture.profile.AuthorityBucketForActor(fixture.calc, authoritystore.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: 1})
	if err != nil {
		t.Fatal(err)
	}
	playerKey, err := playerRuntime.Store.Keys().ShardAuthority(playerRef)
	if err != nil {
		t.Fatal(err)
	}
	farmKey, err := farmRuntime.Store.Keys().ShardAuthority(farmRef)
	if err != nil {
		t.Fatal(err)
	}
	if playerKey.String() == farmKey.String() || !strings.Contains(playerKey.String(), ":auth:player:") || !strings.Contains(farmKey.String(), ":auth:farm:") {
		t.Fatalf("domain authority keys are not separated: player=%s farm=%s", playerKey, farmKey)
	}
}

func TestAuthorityModeConfigurationFailsClosedAndLegacyRollbackIsExplicit(t *testing.T) {
	t.Setenv("ACTOR_AUTHORITY_MODE", "")
	mode, err := ModeFromEnvironment()
	if err != nil || mode != ModeAuthority {
		t.Fatalf("empty mode must default to authority: mode=%q err=%v", mode, err)
	}
	t.Setenv("ACTOR_AUTHORITY_MODE", "legacy")
	mode, err = ModeFromEnvironment()
	if err != nil || mode != ModeLegacy {
		t.Fatalf("legacy mode=%q err=%v", mode, err)
	}
	t.Setenv("ACTOR_AUTHORITY_MODE", "rollback")
	mode, err = ModeFromEnvironment()
	if err != nil || mode != ModeLegacy {
		t.Fatalf("rollback mode=%q err=%v", mode, err)
	}
	t.Setenv("ACTOR_AUTHORITY_MODE", "authority")
	if _, err := OptionsFromEnvironment(nil, actorrouting.DomainPlayer); err == nil {
		t.Fatal("incomplete authority configuration did not fail closed")
	}
	t.Setenv("ACTOR_AUTHORITY_MODE", "unexpected")
	if _, err := ModeFromEnvironment(); err == nil {
		t.Fatal("unknown authority mode was accepted")
	}
}

func TestRemotePlayerAndFarmOptionsUseImmutableTupleWithoutSecondEnvironmentRead(t *testing.T) {
	namespace, err := rediskey.NewNamespace("prod", "z1")
	if err != nil {
		t.Fatal(err)
	}
	profile := rediskey.PhysicalProfile{
		ProfileVersion: "redis-physical-v1", Namespace: namespace,
		AuthorityLayoutVersion: "v1", WorkflowLayoutVersion: "v1",
		WorkflowHashVersion: 1, WorkflowBucketCount: 256,
	}
	t.Setenv("ACTOR_ASSIGNMENT_MODE", "remote-watch")
	t.Setenv("AUTHORITY_MEMBERSHIP_MODE", "true")
	values := map[string]string{
		"ACTOR_AUTHORITY_LEASE_TTL":            "15s",
		"ACTOR_AUTHORITY_LEASE_SAFETY_MARGIN":  "2s",
		"ACTOR_AUTHORITY_LEASE_RENEW_INTERVAL": "5s",
		"ACTOR_ASSIGNMENT_MODE":                "remote-watch",
		"AUTHORITY_MEMBERSHIP_MODE":            "true",
		"ACTOR_HOLDER_POD":                     "holder-a",
		"ACTOR_HOLDER_IDENTITY_MODE":           "kubernetes-process-v1",
		"ACTOR_HOLDER_POD_UID":                 "pod-uid-a",
		"ACTOR_HOLDER_WORKLOAD_UID":            "workload-uid",
		"ACTOR_HOLDER_ORDINAL":                 "0",
	}
	reads := make(map[string]int)
	getenv := func(key string) string {
		reads[key]++
		return values[key]
	}
	for _, domain := range []actorrouting.RouteDomain{actorrouting.DomainPlayer, actorrouting.DomainFarm} {
		options, err := optionsFromEnvironmentWithProfile(nil, domain, profile, getenv)
		if err != nil || options.Profile != profile || options.Domain != domain || options.AssignmentMode != AssignmentModeRemoteWatch {
			t.Fatalf("domain=%s options=%+v err=%v", domain, options, err)
		}
	}
	for _, key := range []string{
		rediskey.RedisNamespaceEnvironmentEnv, rediskey.RedisNamespaceZoneEnv,
		rediskey.RedisPhysicalProfileEnv, rediskey.RedisAuthorityLayoutEnv,
		rediskey.RedisWorkflowLayoutEnv, rediskey.RedisWorkflowHashEnv,
		rediskey.RedisWorkflowBucketCountEnv,
	} {
		if reads[key] != 0 {
			t.Fatalf("immutable tuple field %s was re-read %d times", key, reads[key])
		}
	}
}

func TestExactKubernetesIdentityGeneratesOneProcessBootAndRejectsStaticBoot(t *testing.T) {
	t.Setenv("ACTOR_ASSIGNMENT_MODE", "remote-watch")
	t.Setenv("AUTHORITY_MEMBERSHIP_MODE", "true")
	namespace, err := rediskey.NewNamespace("prod", "z1")
	if err != nil {
		t.Fatal(err)
	}
	profile := rediskey.PhysicalProfile{
		ProfileVersion: "redis-physical-v1", Namespace: namespace,
		AuthorityLayoutVersion: "v1", WorkflowLayoutVersion: "v1",
		WorkflowHashVersion: 1, WorkflowBucketCount: 256,
	}
	values := map[string]string{
		"ACTOR_AUTHORITY_LEASE_TTL":            "15s",
		"ACTOR_AUTHORITY_LEASE_SAFETY_MARGIN":  "2s",
		"ACTOR_AUTHORITY_LEASE_RENEW_INTERVAL": "5s",
		"ACTOR_ASSIGNMENT_MODE":                "remote-watch",
		"AUTHORITY_MEMBERSHIP_MODE":            "true",
		"ACTOR_HOLDER_POD":                     "player-svr-2",
		"ACTOR_HOLDER_IDENTITY_MODE":           "kubernetes-process-v1",
		"ACTOR_HOLDER_POD_UID":                 "pod-uid-2",
		"ACTOR_HOLDER_WORKLOAD_UID":            "statefulset-uid",
		"ACTOR_HOLDER_ORDINAL":                 "2",
	}
	getenv := func(key string) string { return values[key] }
	options, err := optionsFromEnvironmentWithProfile(nil, actorrouting.DomainPlayer, profile, getenv)
	if err != nil {
		t.Fatal(err)
	}
	if !options.DeploymentIdentity.Exact() || options.DeploymentIdentity.Ordinal != 2 || len(options.HolderBootID) != 32 {
		t.Fatalf("options=%+v", options)
	}
	values["ACTOR_HOLDER_BOOT_ID"] = "deterministic-boot"
	if _, err := optionsFromEnvironmentWithProfile(nil, actorrouting.DomainPlayer, profile, getenv); err == nil {
		t.Fatal("exact Kubernetes identity accepted configured boot")
	}
	values["HOLDER_PROCESS_BOOT_MODE"] = "pod-uid-then-random"
	values["ACTOR_HOLDER_BOOT_ID"] = "player-svr-2-pod-uid-2"
	entrypointOptions, err := optionsFromEnvironmentWithProfile(nil, actorrouting.DomainPlayer, profile, getenv)
	if err != nil {
		t.Fatal(err)
	}
	if entrypointOptions.HolderBootID != "player-svr-2-pod-uid-2" {
		t.Fatalf("trusted entrypoint boot=%q", entrypointOptions.HolderBootID)
	}
	values["ACTOR_HOLDER_BOOT_ID"] = "other-holder-pod-uid-2"
	if _, err := optionsFromEnvironmentWithProfile(nil, actorrouting.DomainPlayer, profile, getenv); err == nil {
		t.Fatal("trusted entrypoint accepted boot for another holder")
	}
	delete(values, "ACTOR_HOLDER_BOOT_ID")
	delete(values, "HOLDER_PROCESS_BOOT_MODE")
	t.Setenv("ACTOR_ASSIGNMENT_MODE", "static")
	values["ACTOR_ASSIGNMENT_MODE"] = "static"
	values["ACTOR_ROUTE_SNAPSHOT"] = "snapshot.json"
	if _, err := optionsFromEnvironmentWithProfile(nil, actorrouting.DomainPlayer, profile, getenv); err == nil {
		t.Fatal("static assignment accepted exact Kubernetes identity")
	}
}

func TestRemoteWatchRejectsLegacyZeroIdentityAndConfiguredBoot(t *testing.T) {
	t.Setenv("ACTOR_ASSIGNMENT_MODE", "remote-watch")
	t.Setenv("AUTHORITY_MEMBERSHIP_MODE", "true")
	namespace, err := rediskey.NewNamespace("prod", "z1")
	if err != nil {
		t.Fatal(err)
	}
	profile := rediskey.PhysicalProfile{ProfileVersion: "redis-physical-v1", Namespace: namespace,
		AuthorityLayoutVersion: "v1", WorkflowLayoutVersion: "v1", WorkflowHashVersion: 1, WorkflowBucketCount: 256}
	values := map[string]string{
		"ACTOR_AUTHORITY_LEASE_TTL": "15s", "ACTOR_AUTHORITY_LEASE_SAFETY_MARGIN": "2s",
		"ACTOR_AUTHORITY_LEASE_RENEW_INTERVAL": "5s", "ACTOR_HOLDER_POD": "player-svr-2",
		"ACTOR_ASSIGNMENT_MODE": "remote-watch", "AUTHORITY_MEMBERSHIP_MODE": "true",
		"ACTOR_HOLDER_BOOT_ID": "reusable-boot",
	}
	if _, err := optionsFromEnvironmentWithProfile(nil, actorrouting.DomainPlayer, profile,
		func(key string) string { return values[key] }); err == nil || !strings.Contains(err.Error(), "remote-watch assignment requires exact Kubernetes holder identity") {
		t.Fatalf("remote-watch zero identity err=%v", err)
	}
}

func TestPartialKubernetesIdentityFailsClosed(t *testing.T) {
	t.Setenv("ACTOR_ASSIGNMENT_MODE", "remote-watch")
	t.Setenv("AUTHORITY_MEMBERSHIP_MODE", "true")
	namespace, err := rediskey.NewNamespace("prod", "z1")
	if err != nil {
		t.Fatal(err)
	}
	profile := rediskey.PhysicalProfile{ProfileVersion: "redis-physical-v1", Namespace: namespace,
		AuthorityLayoutVersion: "v1", WorkflowLayoutVersion: "v1", WorkflowHashVersion: 1, WorkflowBucketCount: 256}
	values := map[string]string{
		"ACTOR_AUTHORITY_LEASE_TTL": "15s", "ACTOR_AUTHORITY_LEASE_SAFETY_MARGIN": "2s",
		"ACTOR_AUTHORITY_LEASE_RENEW_INTERVAL": "5s", "ACTOR_ASSIGNMENT_MODE": "remote-watch",
		"AUTHORITY_MEMBERSHIP_MODE": "true", "ACTOR_HOLDER_POD": "player-svr-2",
		"ACTOR_HOLDER_POD_UID": "pod-uid-2",
	}
	if _, err := optionsFromEnvironmentWithProfile(nil, actorrouting.DomainPlayer, profile,
		func(key string) string { return values[key] }); err == nil {
		t.Fatal("partial deployment identity accepted")
	}
}

func TestOptionsParserUsesInjectedGetterForWatchBuffer(t *testing.T) {
	namespace, err := rediskey.NewNamespace("prod", "z1")
	if err != nil {
		t.Fatal(err)
	}
	profile := rediskey.PhysicalProfile{ProfileVersion: "redis-physical-v1", Namespace: namespace,
		AuthorityLayoutVersion: "v1", WorkflowLayoutVersion: "v1", WorkflowHashVersion: 1, WorkflowBucketCount: 256}
	t.Setenv("ACTOR_ROUTE_WATCH_BUFFER", "999")
	values := map[string]string{
		"ACTOR_AUTHORITY_LEASE_TTL": "15s", "ACTOR_AUTHORITY_LEASE_SAFETY_MARGIN": "2s",
		"ACTOR_AUTHORITY_LEASE_RENEW_INTERVAL": "5s", "ACTOR_ASSIGNMENT_MODE": "remote-watch",
		"AUTHORITY_MEMBERSHIP_MODE": "true", "ACTOR_HOLDER_POD": "player-svr-2",
		"ACTOR_HOLDER_IDENTITY_MODE": "kubernetes-process-v1", "ACTOR_HOLDER_POD_UID": "pod-uid-2",
		"ACTOR_HOLDER_WORKLOAD_UID": "workload-uid", "ACTOR_HOLDER_ORDINAL": "2",
		"ACTOR_ROUTE_WATCH_BUFFER": "7",
	}
	options, err := optionsFromEnvironmentWithProfile(nil, actorrouting.DomainPlayer, profile,
		func(key string) string { return values[key] })
	if err != nil || options.AssignmentWatchBuffer != 7 {
		t.Fatalf("buffer=%d err=%v", options.AssignmentWatchBuffer, err)
	}
}

func TestStaticEnvironmentTupleRejectsCanonicalNumericDrift(t *testing.T) {
	for key, value := range map[string]string{
		rediskey.RedisNamespaceEnvironmentEnv: "prod", rediskey.RedisNamespaceZoneEnv: "z1",
		rediskey.RedisPhysicalProfileEnv: "redis-physical-v1", rediskey.RedisAuthorityLayoutEnv: "v1",
		rediskey.RedisWorkflowLayoutEnv: "v1", rediskey.RedisWorkflowHashEnv: "01",
		rediskey.RedisWorkflowBucketCountEnv: "256",
	} {
		t.Setenv(key, value)
	}
	if _, err := OptionsFromEnvironment(nil, actorrouting.DomainPlayer); err == nil {
		t.Fatal("static environment parser accepted workflow hash 01")
	}
}

// TestZeroShardHolderStartsProcessReadyAndFailsClosedForEveryShard proves the
// RT1B readiness split: a holder with zero local shards in the static route
// snapshot starts successfully, reports process readiness from
// common-dependency health alone, reports zero serving shards, refuses every
// shard-level admission, and fails closed for any actor request it has not
// been granted.
func TestZeroShardHolderStartsProcessReadyAndFailsClosedForEveryShard(t *testing.T) {
	fixture := newCompositionFixture(t)
	snapshot := fixture.snap
	snapshot.GlobalRevision = 2
	// player shard 0 now belongs to a different holder; player-pod-1 keeps
	// its holder row but owns zero local shards.
	snapshot.Holders = append(snapshot.Holders, actorrouting.HolderRef{
		InstanceID: "other-pod-1", BootID: "other-boot-1",
		Endpoint: "other.local:9101", State: actorrouting.HolderStateActive, Weight: 1,
	})
	snapshot.Player.Routes[0] = actorrouting.ShardRoute{
		HolderIndex: 2, State: actorrouting.ShardStateActive,
		ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1,
		LeaseUntilMs: time.Now().Add(time.Hour).UnixMilli(),
	}
	path := filepath.Join(t.TempDir(), "route-zero.json")
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	options := compositionOptions(fixture, actorrouting.DomainPlayer, "player-pod-1", "player-boot-1")
	options.RouteSnapshotPath = path

	runtime, err := New(context.Background(), options)
	if err != nil {
		t.Fatalf("zero-shard holder must start without failure: %v", err)
	}
	defer runtime.Close()

	if !runtime.ProcessReady() {
		t.Fatal("zero-shard holder is not process ready")
	}
	if !runtime.Ready() {
		t.Fatal("zero-shard holder is not ready")
	}
	if runtime.ServingShardCount() != 0 {
		t.Fatalf("ServingShardCount=%d, want 0 for a zero-shard holder", runtime.ServingShardCount())
	}
	if runtime.CanServeShard(actorrouting.DomainPlayer, 0) {
		t.Fatal("zero-shard holder reported player shard 0 as servable")
	}
	if runtime.CanServeShard(actorrouting.DomainFarm, 0) {
		t.Fatal("zero-shard holder reported farm shard 0 as servable")
	}

	// The static assignment remains fully installed and observable even with
	// zero local shards (static-assignment compatibility).
	installed := runtime.AssignmentProvider.Snapshot()
	if installed.GlobalRevision != 2 || len(installed.Holders) != 3 {
		t.Fatalf("static assignment not installed: revision=%d holders=%d", installed.GlobalRevision, len(installed.Holders))
	}
	if runtime.Applier.LastGlobalRevision() != 2 {
		t.Fatalf("applier revision=%d, want 2", runtime.Applier.LastGlobalRevision())
	}
	if _, err := NewParticipantRouteFence(runtime); err != nil {
		t.Fatalf("zero-shard participant route fence construction failed: %v", err)
	}

	// Fail closed: no locally installed shard means every actor request is
	// rejected at the fence, epoch, and claim seams. Nothing is bulk-loaded.
	ctx := context.Background()
	if _, err := runtime.FenceSource.WriteFence(ctx, authoritystore.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: 1}); err == nil {
		t.Fatal("zero-shard holder admitted a player shard write fence")
	}
	if _, err := runtime.FenceSource.WriteFence(ctx, authoritystore.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: 1}); err == nil {
		t.Fatal("zero-shard holder admitted a farm shard write fence")
	}
	if _, err := runtime.Provider.WriterEpoch(ctx, actorrouting.DomainPlayer, 1); err == nil {
		t.Fatal("zero-shard holder resolved a writer epoch without a local shard")
	}
	if _, status, err := runtime.Provider.Claim(ctx, actorrouting.DomainPlayer, 1,
		"claim-zero-1", "wc-zero-1", nil); err == nil || status == routeauthority.ClaimAcquired {
		t.Fatalf("zero-shard holder acquired an actor claim without a local shard: status=%v err=%v", status, err)
	}
}

// TestZeroShardDynamicAssignmentStillFailsClosedBeforeGrant proves that a
// holder which is process-ready with zero shards continues to fail closed
// after the control-plane publishes a newer snapshot that keeps every shard
// elsewhere.
func TestZeroShardDynamicAssignmentStillFailsClosedBeforeGrant(t *testing.T) {
	fixture := newCompositionFixture(t)
	snapshot := fixture.snap
	snapshot.GlobalRevision = 2
	snapshot.Holders = append(snapshot.Holders, actorrouting.HolderRef{
		InstanceID: "other-pod-1", BootID: "other-boot-1",
		Endpoint: "other.local:9101", State: actorrouting.HolderStateActive, Weight: 1,
	})
	snapshot.Player.Routes[0] = actorrouting.ShardRoute{
		HolderIndex: 2, State: actorrouting.ShardStateActive,
		ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1,
		LeaseUntilMs: time.Now().Add(time.Hour).UnixMilli(),
	}
	path := filepath.Join(t.TempDir(), "route-zero-dynamic.json")
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	options := compositionOptions(fixture, actorrouting.DomainPlayer, "player-pod-1", "player-boot-1")
	options.RouteSnapshotPath = path
	runtime, err := New(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()

	next := runtime.AssignmentProvider.Snapshot()
	next.GlobalRevision = 3
	if err := runtime.publisher.Publish(context.Background(), controlfence.NestedControlOperationToken{}, next); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for runtime.Applier.LastGlobalRevision() != 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !runtime.ProcessReady() {
		t.Fatal("process readiness dropped while still observing assignments")
	}
	if runtime.ServingShardCount() != 0 {
		t.Fatalf("ServingShardCount=%d after dynamic publish, want 0", runtime.ServingShardCount())
	}
	if runtime.CanServeShard(actorrouting.DomainPlayer, 0) {
		t.Fatal("un-granted shard became servable after a control-plane publish")
	}
	if _, err := runtime.FenceSource.WriteFence(context.Background(), authoritystore.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: 1}); err == nil {
		t.Fatal("un-granted shard admitted a write fence after a control-plane publish")
	}
}

// TestLocalHolderMustBeRegisteredInSnapshot covers the RT1B-1 F1 gate: a
// holder ID absent from Snapshot.Holders fails static startup with
// ErrLocalHolderNotRegistered, whether the local ID is simply missing, a typo,
// or unlisted while other holders are registered.
func TestLocalHolderMustBeRegisteredInSnapshot(t *testing.T) {
	fixture := newCompositionFixture(t)

	// Registration gate: presence in Snapshot.Holders decides admission;
	// route ownership is intentionally not part of the gate.
	if !holderRegisteredInSnapshot(fixture.snap, "player-pod-1") {
		t.Fatal("registered holder must pass the registration gate")
	}
	if holderRegisteredInSnapshot(fixture.snap, "ghost-pod-1") {
		t.Fatal("unregistered holder must fail the registration gate")
	}

	cases := []struct {
		name   string
		holder string
		boot   string
	}{
		{name: "missing local holder", holder: "ghost-pod-1", boot: "ghost-boot-1"},
		{name: "typo holder id", holder: "player-pod-1-typo", boot: "player-boot-1"},
		{name: "other holders registered but local holder missing", holder: "unlisted-pod-1", boot: "unlisted-boot-1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			options := compositionOptions(fixture, actorrouting.DomainPlayer, tc.holder, tc.boot)
			_, err := New(context.Background(), options)
			if err == nil {
				t.Fatalf("holder %q absent from Snapshot.Holders unexpectedly started", tc.holder)
			}
			if !errors.Is(err, ErrLocalHolderNotRegistered) {
				t.Fatalf("expected ErrLocalHolderNotRegistered, got %v", err)
			}
		})
	}
}

// TestLocalHolderRegistrationFailureLeavesNoRunningResidue proves that the
// registration gate fires before any renewal/applier goroutine is created, so
// repeated failed startups never leak goroutines or running residue.
func TestLocalHolderRegistrationFailureLeavesNoRunningResidue(t *testing.T) {
	fixture := newCompositionFixture(t)
	before := runtime.NumGoroutine()
	for i := range 5 {
		options := compositionOptions(fixture, actorrouting.DomainPlayer, "ghost-pod-1", "ghost-boot-1")
		_, err := New(context.Background(), options)
		if err == nil || !errors.Is(err, ErrLocalHolderNotRegistered) {
			t.Fatalf("iteration %d: unexpected startup result err=%v", i, err)
		}
	}
	time.Sleep(50 * time.Millisecond)
	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Fatalf("failed startup leaked goroutines: before=%d after=%d", before, after)
	}
}

// TestLocalHolderBootAndNonActiveProtectionRemains proves that the existing
// boot-mismatch and non-ACTIVE-route startup protections stay intact beside
// the new registration gate.
func TestLocalHolderBootAndNonActiveProtectionRemains(t *testing.T) {
	fixture := newCompositionFixture(t)

	// Boot mismatch still fails even though the holder is registered.
	bootOptions := compositionOptions(fixture, actorrouting.DomainPlayer, "player-pod-1", "wrong-boot-1")
	if _, err := New(context.Background(), bootOptions); err == nil {
		t.Fatal("boot mismatch unexpectedly started")
	}

	// A registered holder whose local route is not ACTIVE still fails.
	snapshot := fixture.snap
	snapshot.GlobalRevision = 2
	snapshot.Player.Routes[0] = actorrouting.ShardRoute{
		HolderIndex: 0, State: actorrouting.ShardStateDraining,
		ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1,
		LeaseUntilMs: time.Now().Add(time.Hour).UnixMilli(),
	}
	path := filepath.Join(t.TempDir(), "route-draining.json")
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	drainingOptions := compositionOptions(fixture, actorrouting.DomainPlayer, "player-pod-1", "player-boot-1")
	drainingOptions.RouteSnapshotPath = path
	if _, err := New(context.Background(), drainingOptions); err == nil {
		t.Fatal("non-ACTIVE local route unexpectedly started")
	}
}

// TestRegisteredHolderWithZeroRoutesSucceeds explicitly proves the frozen
// semantic that a holder registered in Snapshot.Holders but owning no route
// starts successfully with zero serving shards.
func TestRegisteredHolderWithZeroRoutesSucceeds(t *testing.T) {
	fixture := newCompositionFixture(t)
	if !holderRegisteredInSnapshot(fixture.snap, "player-pod-1") {
		t.Fatal("fixture holder must be registered")
	}
	// player shard 0 is reassigned to a third holder, leaving player-pod-1
	// registered with zero routes.
	snapshot := fixture.snap
	snapshot.GlobalRevision = 2
	snapshot.Holders = append(snapshot.Holders, actorrouting.HolderRef{
		InstanceID: "other-pod-1", BootID: "other-boot-1",
		Endpoint: "other.local:9101", State: actorrouting.HolderStateActive, Weight: 1,
	})
	snapshot.Player.Routes[0] = actorrouting.ShardRoute{
		HolderIndex: 2, State: actorrouting.ShardStateActive,
		ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1,
		LeaseUntilMs: time.Now().Add(time.Hour).UnixMilli(),
	}
	path := filepath.Join(t.TempDir(), "route-zero-registered.json")
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	options := compositionOptions(fixture, actorrouting.DomainPlayer, "player-pod-1", "player-boot-1")
	options.RouteSnapshotPath = path
	runtime, err := New(context.Background(), options)
	if err != nil {
		t.Fatalf("registered zero-route holder must start: %v", err)
	}
	defer runtime.Close()
	if !runtime.ProcessReady() {
		t.Fatal("registered zero-route holder must be process ready")
	}
	if runtime.ServingShardCount() != 0 {
		t.Fatalf("ServingShardCount=%d, want 0", runtime.ServingShardCount())
	}
	if runtime.CanServeShard(actorrouting.DomainPlayer, 0) {
		t.Fatal("zero-route holder must not serve any shard")
	}
}
