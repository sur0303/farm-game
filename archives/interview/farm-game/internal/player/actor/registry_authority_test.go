package actor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	playerpb "farmgame/api/gen/player"
	"farmgame/internal/actorrouting"
	"farmgame/internal/logic/actor/authority"
	"farmgame/internal/logic/actor/persistence"
	"farmgame/internal/publicprofile"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/blobstore"
	redisauthority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

var ar1pNow = time.UnixMilli(1_770_300_000_000)

// ar1pRig 是 authority 模式 Player Registry 的测试装配;同一套测试体在
// MemoryStore(合同 Fake)与真实 Redis Adapter(miniredis)上各跑一遍。
type ar1pRig struct {
	fault        *authoritystore.FaultStore
	provider     *authority.Provider
	registry     *Registry
	modelReader  authoritystore.ModelFieldBatchReader
	sourceReader authoritystore.SourceWorkReader
	reassign     func(t *testing.T, holderID string, writerEpoch, leaseEpoch uint64) authoritystore.ShardWriteFence
}

type recordConflictOnceStore struct {
	authoritystore.ActorAuthorityStore
	conflict atomic.Bool
}

func (s *recordConflictOnceStore) PhysicalCapabilities() (authoritystore.PhysicalCapabilities, error) {
	return s.ActorAuthorityStore.(authoritystore.PhysicalCapabilitiesProvider).PhysicalCapabilities()
}

func (s *recordConflictOnceStore) ProjectPhysicalCommit(req authoritystore.AuthorityCommitRequest) (authoritystore.PhysicalCommitUsage, error) {
	return s.ActorAuthorityStore.(authoritystore.PhysicalCommitUsageProjector).ProjectPhysicalCommit(req)
}

func (s *recordConflictOnceStore) Commit(ctx context.Context, req authoritystore.AuthorityCommitRequest) (authoritystore.CommitResult, error) {
	if s.conflict.Swap(false) {
		return authoritystore.CommitResult{Status: authoritystore.CommitRecordVersionConflict}, nil
	}
	return s.ActorAuthorityStore.Commit(ctx, req)
}

func ar1pCalculator(t *testing.T) *actorrouting.Calculator {
	t.Helper()
	calculator, err := actorrouting.NewCalculator(
		"test-realm",
		actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608040021},
		64, 64,
	)
	if err != nil {
		t.Fatal(err)
	}
	return calculator
}

func ar1pMemoryRig(t *testing.T, playerID uint64) *ar1pRig {
	t.Helper()
	calculator := ar1pCalculator(t)
	store, err := authoritystore.NewMemoryStore(authoritystore.MemoryOptions{
		Resolver:               calculator,
		AuthorityLayoutVersion: "v1",
		Now:                    func() time.Time { return ar1pNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	setAuthority := func(t *testing.T, holderID string, writerEpoch, leaseEpoch uint64) authoritystore.ShardWriteFence {
		t.Helper()
		shard, err := calculator.ShardFor(actorrouting.DomainPlayer, playerID)
		if err != nil {
			t.Fatal(err)
		}
		authority := authoritystore.ShardWriteAuthority{
			AuthorityLayoutVersion: "v1",
			Domain:                 actorrouting.DomainPlayer,
			VirtualShard:           shard,
			HolderID:               holderID,
			HolderBootID:           holderID + "-boot",
			AssignmentRevision:     writerEpoch,
			WriterEpoch:            writerEpoch,
			LeaseEpoch:             leaseEpoch,
			State:                  authoritystore.ShardAuthorityActive,
			LeaseUntilMs:           ar1pNow.Add(time.Minute).UnixMilli(),
		}
		if err := store.SetShardWriteAuthority(authority); err != nil {
			t.Fatal(err)
		}
		return authoritystore.ShardWriteFence{
			ExpectedHolderID:           authority.HolderID,
			ExpectedHolderBootID:       authority.HolderBootID,
			ExpectedAssignmentRevision: authority.AssignmentRevision,
			ExpectedWriterEpoch:        authority.WriterEpoch,
			ExpectedLeaseEpoch:         authority.LeaseEpoch,
		}
	}
	fence := setAuthority(t, "player-holder-a", 1, 1)
	fault := authoritystore.NewFaultStore(store)
	provider, err := authority.NewProvider(authority.ProviderOptions{
		Store:                  fault,
		Fence:                  authority.StaticFenceSource{Fence: fence},
		AuthorityLayoutVersion: "v1",
		BootID:                 "player-holder-a-boot",
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := NewAuthorityRegistry(provider, RegistryOptions{MailboxSize: 64})
	t.Cleanup(registry.Stop)
	return &ar1pRig{fault: fault, provider: provider, registry: registry, modelReader: store, reassign: setAuthority}
}

func ar1pRedisRig(t *testing.T, playerID uint64) *ar1pRig {
	t.Helper()
	server := miniredis.RunT(t)
	server.SetTime(ar1pNow)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	namespace, err := rediskey.NewNamespace("test", "z1")
	if err != nil {
		t.Fatal(err)
	}
	profile := rediskey.PhysicalProfile{
		ProfileVersion:         "p1",
		Namespace:              namespace,
		AuthorityLayoutVersion: "v1",
		WorkflowLayoutVersion:  "v1",
		WorkflowHashVersion:    1,
		WorkflowBucketCount:    64,
	}
	if err := profile.Validate(); err != nil {
		t.Fatal(err)
	}
	calculator := ar1pCalculator(t)
	store, err := redisauthority.NewStore(client, nil, redisauthority.Options{Profile: profile, Resolver: calculator})
	if err != nil {
		t.Fatal(err)
	}
	shard, err := calculator.ShardFor(actorrouting.DomainPlayer, playerID)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	transitionSeq := 0
	var lastFence authoritystore.ShardWriteFence
	setAuthority := func(t *testing.T, holderID string, writerEpoch, leaseEpoch uint64) authoritystore.ShardWriteFence {
		t.Helper()
		transitionSeq++
		caller := authoritystore.TrustedShardAuthorityCaller{
			Kind:       authoritystore.ShardAuthorityCallerRouteManager,
			InstanceID: "route-manager-1",
			BootID:     "rm-boot-1",
		}
		result, err := store.ClaimShardAuthority(ctx, authoritystore.ClaimShardAuthorityRequest{
			Caller: caller, TransitionID: fmt.Sprintf("ar1p-transition-%d", transitionSeq),
			AuthorityLayoutVersion: "v1", Domain: actorrouting.DomainPlayer, VirtualShard: shard,
			NewHolderID: holderID, NewHolderBootID: holderID + "-boot", RequestedTTLms: 60_000,
		})
		if err != nil {
			t.Fatal(err)
		}
		if result.Status != authoritystore.ShardTransitionApplied {
			t.Fatalf("shard claim status=%s", result.Status)
		}
		lastFence = authoritystore.ShardWriteFence{
			ExpectedHolderID:           result.Authority.HolderID,
			ExpectedHolderBootID:       result.Authority.HolderBootID,
			ExpectedAssignmentRevision: result.Authority.AssignmentRevision,
			ExpectedWriterEpoch:        result.Authority.WriterEpoch,
			ExpectedLeaseEpoch:         result.Authority.LeaseEpoch,
		}
		return lastFence
	}
	fence := setAuthority(t, "player-holder-a", 1, 1)
	fault := authoritystore.NewFaultStore(store)
	provider, err := authority.NewProvider(authority.ProviderOptions{
		Store:                  fault,
		Fence:                  authority.StaticFenceSource{Fence: fence},
		AuthorityLayoutVersion: "v1",
		BootID:                 "player-holder-a-boot",
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := NewAuthorityRegistry(provider, RegistryOptions{MailboxSize: 64})
	t.Cleanup(registry.Stop)
	return &ar1pRig{fault: fault, provider: provider, registry: registry, modelReader: store, sourceReader: store, reassign: setAuthority}
}

func ar1pRigs(t *testing.T, playerID uint64) map[string]*ar1pRig {
	return map[string]*ar1pRig{
		"memory": ar1pMemoryRig(t, playerID),
		"redis":  ar1pRedisRig(t, playerID),
	}
}

func TestAuthorityRegistryCommitsPublicProfileBesideRecord(t *testing.T) {
	const playerID = uint64(9101)
	for name, rig := range ar1pRigs(t, playerID) {
		t.Run(name, func(t *testing.T) {
			if _, _, err := rig.registry.EnsureWithProfile(context.Background(), playerID, "bootstrap-profile", "profile_farmer", 3); err != nil {
				t.Fatal(err)
			}
			assertProfile := func(level uint32) {
				rows, err := rig.modelReader.BatchReadModelFields(context.Background(), actorrouting.DomainPlayer, []uint64{playerID}, []string{publicprofile.ModelField})
				if err != nil || len(rows) != 1 || !rows[0].Found {
					t.Fatalf("rows=%v err=%v", rows, err)
				}
				profile, err := publicprofile.UnmarshalProjection(rows[0].ModelFields[publicprofile.ModelField], playerID)
				if err != nil || profile.GetDisplayName() != "profile_farmer" || profile.GetAvatarId() != 3 || profile.GetLevel() != level {
					t.Fatalf("profile=%v err=%v", profile, err)
				}
			}
			assertProfile(1)
			if _, err := rig.registry.Submit(context.Background(), playerID, func(record *playerpb.PlayerRecord) (any, bool, error) {
				record.Progression.Level = 9
				return nil, true, nil
			}); err != nil {
				t.Fatal(err)
			}
			assertProfile(9)
		})
	}
}

func TestEnsureWithProfileSkipsRedundantVersionedTurn(t *testing.T) {
	const playerID = uint64(9102)
	for name, rig := range ar1pRigs(t, playerID) {
		t.Run(name, func(t *testing.T) {
			before := rig.registry.RuntimeStats().Submitted
			record, _, err := rig.registry.EnsureWithProfile(context.Background(), playerID, "bootstrap-once", "once_farmer", 3)
			if err != nil {
				t.Fatal(err)
			}
			afterCreate := rig.registry.RuntimeStats().Submitted
			if afterCreate-before != 1 {
				t.Fatalf("create submits=%d, want 1", afterCreate-before)
			}
			if record.GetPublicProfile().GetDisplayName() != "once_farmer" || record.GetPublicProfile().GetAvatarId() != 3 {
				t.Fatalf("create profile=%v", record.GetPublicProfile())
			}
			again, _, err := rig.registry.EnsureWithProfile(context.Background(), playerID, "bootstrap-once", "", 0)
			if err != nil {
				t.Fatal(err)
			}
			afterRepeat := rig.registry.RuntimeStats().Submitted
			if afterRepeat-afterCreate != 1 {
				t.Fatalf("repeat submits=%d, want 1", afterRepeat-afterCreate)
			}
			if again.GetPublicProfile().GetDisplayName() != "once_farmer" {
				t.Fatalf("repeat profile=%v", again.GetPublicProfile())
			}
			if _, err := rig.registry.Submit(context.Background(), playerID, func(rec *playerpb.PlayerRecord) (any, bool, error) {
				rec.PublicProfile = nil
				return nil, true, nil
			}); err != nil {
				t.Fatal(err)
			}
			beforeBackfill := rig.registry.RuntimeStats().Submitted
			restored, _, err := rig.registry.EnsureWithProfile(context.Background(), playerID, "bootstrap-once", "once_farmer", 3)
			if err != nil {
				t.Fatal(err)
			}
			afterBackfill := rig.registry.RuntimeStats().Submitted
			if afterBackfill-beforeBackfill != 2 {
				t.Fatalf("backfill submits=%d, want 2", afterBackfill-beforeBackfill)
			}
			if restored.GetPublicProfile().GetDisplayName() != "once_farmer" || restored.GetPublicProfile().GetAvatarId() != 3 {
				t.Fatalf("backfill profile=%v", restored.GetPublicProfile())
			}
		})
	}
}

// ar1pBuyTurn 是 Registry 级的最小买操作(资产副作用与 ingress Buy 相同:
// 扣 10 币 + 1 种子);真正的 ExecuteCommand Buy/Sell 接缝由
// registry_authority_ingress_test.go 覆盖。
func ar1pBuyTurn(rec *playerpb.PlayerRecord) (any, bool, error) {
	if rec.Wallet.Coin < 10 {
		return nil, false, errors.New("insufficient coin")
	}
	rec.Wallet.Coin -= 10
	if !AddItem(rec, ItemCarrotSeed, 1) {
		return nil, false, errors.New("add item failed")
	}
	rec.EconomyVersion++
	return rec.Wallet.Coin, true, nil
}

func TestAuthorityPlayerEnsureBuySellAndColdLoad(t *testing.T) {
	for name, rig := range ar1pRigs(t, 911) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			rec, version, err := rig.registry.Ensure(ctx, 911, "bootstrap-911")
			if err != nil {
				t.Fatal(err)
			}
			if rec.Wallet.Coin != DefaultInitialCoin || version != 1 {
				t.Fatalf("ensure coin=%d version=%d", rec.Wallet.Coin, version)
			}
			// 重复 Ensure 命中已安装记录(幂等)。
			if _, version, err = rig.registry.Ensure(ctx, 911, "bootstrap-911"); err != nil || version != 1 {
				t.Fatalf("re-ensure version=%d err=%v", version, err)
			}
			if _, err := rig.registry.Submit(ctx, 911, ar1pBuyTurn); err != nil {
				t.Fatal(err)
			}
			// 模拟进程崩溃:不钝化,直接在同一 Store 上重建 Registry。
			restarted := NewAuthorityRegistry(rig.provider, RegistryOptions{MailboxSize: 8})
			t.Cleanup(restarted.Stop)
			rec, version, err = restarted.Read(ctx, 911)
			if err != nil {
				t.Fatal(err)
			}
			if rec.Wallet.Coin != DefaultInitialCoin-10 || ItemCount(rec, ItemCarrotSeed) != 1 || version != 2 {
				t.Fatalf("cold-load coin=%d seeds=%d version=%d", rec.Wallet.Coin, ItemCount(rec, ItemCarrotSeed), version)
			}
		})
	}
}

func TestAuthorityPlayerMissingRecordStaysNotFound(t *testing.T) {
	rig := ar1pMemoryRig(t, 912)
	ctx := context.Background()
	if _, _, err := rig.registry.Read(ctx, 912); !errors.Is(err, blobstore.ErrNotFound) {
		t.Fatalf("read err=%v", err)
	}
	if _, err := rig.registry.Submit(ctx, 912, ar1pBuyTurn); !errors.Is(err, blobstore.ErrNotFound) {
		t.Fatalf("submit err=%v", err)
	}
}

func TestAuthorityPlayerCommitUnknownResolvedCommitted(t *testing.T) {
	rig := ar1pMemoryRig(t, 913)
	ctx := context.Background()
	if _, _, err := rig.registry.Ensure(ctx, 913, "bootstrap-913"); err != nil {
		t.Fatal(err)
	}
	// 提交已落库但响应丢失:FenceAndClaim 证明 COMMITTED,命令按成功返回,
	// 资产效果恰好一次。
	rig.fault.Inject(authoritystore.MethodCommit, authoritystore.FaultTimeoutUnknownAfterApply)
	value, err := rig.registry.Submit(ctx, 913, ar1pBuyTurn)
	if err != nil {
		t.Fatal(err)
	}
	if coin := value.(int64); coin != DefaultInitialCoin-10 {
		t.Fatalf("coin=%d", coin)
	}
	if got := rig.provider.Metrics().Snapshot().ResolveCommitted; got != 1 {
		t.Fatalf("resolve committed=%d", got)
	}
	rec, version, err := rig.registry.Read(ctx, 913)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Wallet.Coin != DefaultInitialCoin-10 || version != 2 {
		t.Fatalf("coin=%d version=%d (effect must land exactly once)", rec.Wallet.Coin, version)
	}
}

func TestAuthorityPlayerCommitUnknownResolvedNotCommitted(t *testing.T) {
	rig := ar1pMemoryRig(t, 914)
	ctx := context.Background()
	if _, _, err := rig.registry.Ensure(ctx, 914, "bootstrap-914"); err != nil {
		t.Fatal(err)
	}
	// 提交确定未落库:Resolver 证明 NOT_COMMITTED 后按原幂等身份有界重试。
	rig.fault.Inject(authoritystore.MethodCommit, authoritystore.FaultTimeoutUnknownBeforeApply)
	if _, err := rig.registry.Submit(ctx, 914, ar1pBuyTurn); err != nil {
		t.Fatal(err)
	}
	snap := rig.provider.Metrics().Snapshot()
	if snap.ResolveNotCommitted != 1 {
		t.Fatalf("resolve not-committed=%d", snap.ResolveNotCommitted)
	}
	if got := rig.fault.Calls(authoritystore.MethodCommit); got != 2 {
		t.Fatalf("commit calls=%d", got)
	}
	rec, _, err := rig.registry.Read(ctx, 914)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Wallet.Coin != DefaultInitialCoin-10 {
		t.Fatalf("coin=%d (effect must land exactly once)", rec.Wallet.Coin)
	}
}

func TestAuthorityPlayerUnknownBlocksThenNextTurnResolves(t *testing.T) {
	rig := ar1pMemoryRig(t, 915)
	ctx := context.Background()
	if _, _, err := rig.registry.Ensure(ctx, 915, "bootstrap-915"); err != nil {
		t.Fatal(err)
	}
	// 提交落库但响应丢失,且第一次 Fence 自身 UNKNOWN:本次命令上交
	// ErrTimeoutUnknown(PROCESSING);下一笔命令先解析悬挂的 Unknown 再执行。
	rig.fault.Inject(authoritystore.MethodCommit, authoritystore.FaultTimeoutUnknownAfterApply)
	rig.fault.Inject(authoritystore.MethodFenceAndClaim, authoritystore.FaultTimeoutUnknownBeforeApply)
	if _, err := rig.registry.Submit(ctx, 915, ar1pBuyTurn); !errors.Is(err, blobstore.ErrTimeoutUnknown) {
		t.Fatalf("submit err=%v", err)
	}
	if _, err := rig.registry.Submit(ctx, 915, ar1pBuyTurn); err != nil {
		t.Fatal(err)
	}
	rec, _, err := rig.registry.Read(ctx, 915)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Wallet.Coin != DefaultInitialCoin-20 {
		t.Fatalf("coin=%d (first op once + second op once)", rec.Wallet.Coin)
	}
}

func TestAuthorityPlayerStaleWriterLateWriteRejected(t *testing.T) {
	rig := ar1pMemoryRig(t, 916)
	ctx := context.Background()
	if _, _, err := rig.registry.Ensure(ctx, 916, "bootstrap-916"); err != nil {
		t.Fatal(err)
	}
	rig.reassign(t, "player-holder-b", 2, 2)
	if _, err := rig.registry.Submit(ctx, 916, ar1pBuyTurn); !errors.Is(err, blobstore.ErrWriterEpochMismatch) {
		t.Fatalf("stale write err=%v", err)
	}
	// 失权后绑定被丢弃,后续命令重新 Claim(仍会被新 Fence 拒绝)。
	if bindings, _ := rig.registry.AuthorityStats(); bindings != 0 {
		t.Fatalf("bindings=%d after fence loss", bindings)
	}
	// 热写复用本地快照，但最终 Authority Commit 仍原子校验 Fence。
	// 因此失权在 CAS 边界命中，而不是依赖一次前置 LoadAndClaim。
	if got := rig.provider.Metrics().Snapshot().CommitEpochMismatch; got == 0 {
		t.Fatal("writer epoch mismatch not observed at commit boundary")
	}
}

func TestAuthorityPlayerCASConflictQuarantinesCapacityUntilFreshLoad(t *testing.T) {
	const playerID = 920
	rig := ar1pMemoryRig(t, playerID)
	rig.registry.Stop()
	fence := rig.reassign(t, "player-holder-conflict", 2, 2)
	store := &recordConflictOnceStore{ActorAuthorityStore: rig.fault}
	provider, err := authority.NewProvider(authority.ProviderOptions{
		Store: store, Fence: authority.StaticFenceSource{Fence: fence},
		AuthorityLayoutVersion: "v1", BootID: "player-holder-conflict-boot",
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := NewAuthorityRegistry(provider, RegistryOptions{MailboxSize: 8})
	t.Cleanup(registry.Stop)
	if _, _, err := registry.Ensure(context.Background(), playerID, "conflict-capacity"); err != nil {
		t.Fatal(err)
	}
	registry.bindingsMu.Lock()
	binding := registry.bindings[playerID]
	registry.bindingsMu.Unlock()
	if binding == nil {
		t.Fatal("binding missing")
	}
	data, version, err := binding.Get(context.Background(), playerID)
	if err != nil {
		t.Fatal(err)
	}
	store.conflict.Store(true)
	if err := binding.CASEpoch(context.Background(), playerID, version, 0, data); !errors.Is(err, blobstore.ErrCASConflict) {
		t.Fatalf("CAS conflict err=%v", err)
	}
	modelBytes := len(authority.RecordModelField) + len(data)
	if err := registry.CheckParticipantAuthorityCapacity(context.Background(), playerID, version, modelBytes); !errors.Is(err, ErrParticipantPhysicalCapacity) {
		t.Fatalf("stale cached capacity did not fail closed: %v", err)
	}
	if _, freshVersion, err := registry.Read(context.Background(), playerID); err != nil || freshVersion != version {
		t.Fatalf("fresh load version=%d err=%v", freshVersion, err)
	}
	if err := registry.CheckParticipantAuthorityCapacity(context.Background(), playerID, version, modelBytes); err != nil {
		t.Fatalf("fresh capacity remained quarantined: %v", err)
	}

	businessErr := errors.New("deterministic business rejection")
	if _, err := registry.Submit(context.Background(), playerID, func(*playerpb.PlayerRecord) (any, bool, error) {
		return nil, false, businessErr
	}); !errors.Is(err, businessErr) {
		t.Fatalf("business rejection err=%v", err)
	}
	if bindings, _ := registry.AuthorityStats(); bindings != 1 {
		t.Fatalf("ordinary business rejection dropped binding: %d", bindings)
	}
	if err := registry.CheckParticipantAuthorityCapacity(context.Background(), playerID, version, modelBytes); err != nil {
		t.Fatalf("ordinary business rejection quarantined usage: %v", err)
	}
}

func TestAuthorityPlayerResolveWriterClaimMismatchDropsBinding(t *testing.T) {
	const playerID = 921
	rig := ar1pMemoryRig(t, playerID)
	if _, _, err := rig.registry.Ensure(context.Background(), playerID, "resolve-reconcile"); err != nil {
		t.Fatal(err)
	}
	rig.registry.bindingsMu.Lock()
	binding := rig.registry.bindings[playerID]
	rig.registry.bindingsMu.Unlock()
	if binding == nil {
		t.Fatal("binding missing")
	}
	data, version, err := binding.Get(context.Background(), playerID)
	if err != nil {
		t.Fatal(err)
	}
	rig.fault.Inject(authoritystore.MethodCommit, authoritystore.FaultTimeoutUnknownBeforeApply)
	if err := binding.CASEpoch(context.Background(), playerID, version, 0, data); !errors.Is(err, blobstore.ErrTimeoutUnknown) {
		t.Fatalf("unknown commit err=%v", err)
	}
	opID, claimID := rig.provider.NewClaimIdentity(playerID)
	if _, status, err := rig.provider.Claim(context.Background(), actorrouting.DomainPlayer, playerID, opID, claimID, nil); err != nil || status != authority.ClaimAcquired {
		t.Fatalf("third-party claim status=%v err=%v", status, err)
	}
	if err := rig.registry.resolvePending(context.Background(), binding); !errors.Is(err, blobstore.ErrWriterEpochMismatch) {
		t.Fatalf("resolve reconcile err=%v", err)
	}
	if bindings, _ := rig.registry.AuthorityStats(); bindings != 0 {
		t.Fatalf("reconciled binding retained: %d", bindings)
	}
}

func TestAuthorityPlayerConcurrentTurnsSerialized(t *testing.T) {
	rig := ar1pMemoryRig(t, 917)
	ctx := context.Background()
	if _, _, err := rig.registry.Ensure(ctx, 917, "bootstrap-917"); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := rig.registry.Submit(ctx, 917, func(rec *playerpb.PlayerRecord) (any, bool, error) {
				rec.Wallet.Coin--
				return nil, true, nil
			})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	rec, _, err := rig.registry.Read(ctx, 917)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Wallet.Coin != DefaultInitialCoin-32 {
		t.Fatalf("coin=%d (32 serialized turns must each commit once)", rec.Wallet.Coin)
	}
}

func TestAuthorityPlayerActorSoftStaysDisabled(t *testing.T) {
	rig := ar1pMemoryRig(t, 918)
	ctx := context.Background()
	if _, _, err := rig.registry.Ensure(ctx, 918, "bootstrap-918"); err != nil {
		t.Fatal(err)
	}
	if _, err := rig.registry.Submit(ctx, 918, ar1pBuyTurn); err != nil {
		t.Fatal(err)
	}
	rig.registry.bindingsMu.Lock()
	binding := rig.registry.bindings[918]
	rig.registry.bindingsMu.Unlock()
	if binding == nil {
		t.Fatal("binding missing")
	}
	if _, err := binding.Lane().BeginSoft(3); !errors.Is(err, persistence.ErrSoftSaveDisabled) {
		t.Fatalf("soft save err=%v", err)
	}
	// 成功命令返回前权威提交已推进:Lane 的 Durable 水位等于 RecordVersion。
	if snap := binding.Lane().Snapshot(); snap.DurableThroughVersion != binding.Version() || snap.Dirty {
		t.Fatalf("lane=%+v version=%d", snap, binding.Version())
	}
}
