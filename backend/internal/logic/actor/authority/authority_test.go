package authority

import (
	"context"
	"errors"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/logic/actor/persistence"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/blobstore"
)

var testNow = time.UnixMilli(1_770_100_000_000)

func newTestStore(t *testing.T) *authoritystore.MemoryStore {
	t.Helper()
	calculator, err := actorrouting.NewCalculator(
		"test-realm",
		actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608040001},
		64, 64,
	)
	if err != nil {
		t.Fatal(err)
	}
	store, err := authoritystore.NewMemoryStore(authoritystore.MemoryOptions{
		Resolver:               calculator,
		AuthorityLayoutVersion: "v1",
		Now:                    func() time.Time { return testNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func setShardAuthority(t *testing.T, store *authoritystore.MemoryStore, domain actorrouting.RouteDomain, actorID uint64, holder string, writerEpoch, leaseEpoch uint64) authoritystore.ShardWriteFence {
	t.Helper()
	calculator, err := actorrouting.NewCalculator(
		"test-realm",
		actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608040001},
		64, 64,
	)
	if err != nil {
		t.Fatal(err)
	}
	shard, err := calculator.ShardFor(domain, actorID)
	if err != nil {
		t.Fatal(err)
	}
	authority := authoritystore.ShardWriteAuthority{
		AuthorityLayoutVersion: "v1",
		Domain:                 domain,
		VirtualShard:           shard,
		HolderID:               holder,
		HolderBootID:           holder + "-boot-1",
		AssignmentRevision:     writerEpoch,
		WriterEpoch:            writerEpoch,
		LeaseEpoch:             leaseEpoch,
		State:                  authoritystore.ShardAuthorityActive,
		LeaseUntilMs:           testNow.Add(time.Minute).UnixMilli(),
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

func newProvider(t *testing.T, store authoritystore.ActorAuthorityStore, fence authoritystore.ShardWriteFence) *Provider {
	t.Helper()
	provider, err := NewProvider(ProviderOptions{
		Store:                  store,
		Fence:                  StaticFenceSource{Fence: fence},
		AuthorityLayoutVersion: "v1",
		BootID:                 "holder-a-boot-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	return provider
}

func claimBinding(t *testing.T, provider *Provider, domain actorrouting.RouteDomain, actorID uint64, initial authoritystore.ModelState) *Binding {
	t.Helper()
	opID, claimID := provider.NewClaimIdentity(actorID)
	binding, status, err := provider.Claim(context.Background(), domain, actorID, opID, claimID, initial)
	if err != nil {
		t.Fatal(err)
	}
	if status != ClaimAcquired {
		t.Fatalf("claim status=%v", status)
	}
	return binding
}

func TestClaimGetInsertCommitRoundTrip(t *testing.T) {
	store := newTestStore(t)
	fence := setShardAuthority(t, store, actorrouting.DomainPlayer, 7, "holder-a", 1, 1)
	provider := newProvider(t, store, fence)
	ctx := context.Background()

	// 空 initial 的 Claim:记录被创建但无 record 字段 → Get 返回 NotFound。
	binding := claimBinding(t, provider, actorrouting.DomainPlayer, 7, nil)
	if _, _, err := binding.Get(ctx, 7); !errors.Is(err, blobstore.ErrNotFound) {
		t.Fatalf("get err=%v", err)
	}
	// InsertEpoch 经权威 Commit 安装初始记录(stub v1 → v2),重放报 ErrExists。
	if err := binding.InsertEpoch(ctx, 7, 1, []byte("v1-bytes")); err != nil {
		t.Fatal(err)
	}
	if err := binding.InsertEpoch(ctx, 7, 1, []byte("other")); !errors.Is(err, blobstore.ErrExists) {
		t.Fatalf("insert err=%v", err)
	}
	data, version, err := binding.Get(ctx, 7)
	if err != nil || string(data) != "v1-bytes" || version != 2 {
		t.Fatalf("get data=%q version=%d err=%v", data, version, err)
	}
	// CAS 提交推进版本;过期版本报 CAS 冲突。
	if err := binding.CASEpoch(ctx, 7, 2, 1, []byte("v2-bytes")); err != nil {
		t.Fatal(err)
	}
	if err := binding.CASEpoch(ctx, 7, 2, 1, []byte("stale")); !errors.Is(err, blobstore.ErrCASConflict) {
		t.Fatalf("cas err=%v", err)
	}
	data, version, err = binding.Get(ctx, 7)
	if err != nil || string(data) != "v2-bytes" || version != 3 {
		t.Fatalf("get data=%q version=%d err=%v", data, version, err)
	}
	// ACTOR_SOFT 保持关闭:Lane 拒绝 SoftSave。
	if _, err := binding.Lane().BeginSoft(4); !errors.Is(err, persistence.ErrSoftSaveDisabled) {
		t.Fatalf("soft save err=%v", err)
	}
	if snap := binding.Lane().Snapshot(); snap.DurableThroughVersion != 3 {
		t.Fatalf("lane durable through=%d", snap.DurableThroughVersion)
	}
}

func TestClaimUnknownIdempotentRecovery(t *testing.T) {
	store := newTestStore(t)
	fence := setShardAuthority(t, store, actorrouting.DomainFarm, 8, "holder-a", 1, 1)
	fault := authoritystore.NewFaultStore(store)
	provider := newProvider(t, fault, fence)
	ctx := context.Background()

	fault.Inject(authoritystore.MethodLoadAndClaim, authoritystore.FaultTimeoutUnknownAfterApply)
	opID, claimID := provider.NewClaimIdentity(8)
	binding, status, err := provider.Claim(ctx, actorrouting.DomainFarm, 8, opID, claimID, authoritystore.ModelState{RecordModelField: []byte("init")})
	if err != nil || status != ClaimUnknown || binding != nil {
		t.Fatalf("claim status=%v err=%v", status, err)
	}
	// 原身份幂等重入:不生成新 Claim,直接取得已完成的声明。
	binding, status, err = provider.Claim(ctx, actorrouting.DomainFarm, 8, opID, claimID, authoritystore.ModelState{RecordModelField: []byte("init")})
	if err != nil || status != ClaimAcquired {
		t.Fatalf("reclaim status=%v err=%v", status, err)
	}
	data, _, err := binding.Get(ctx, 8)
	if err != nil || string(data) != "init" {
		t.Fatalf("get data=%q err=%v", data, err)
	}
	if got := fault.Calls(authoritystore.MethodLoadAndClaim); got != 3 {
		t.Fatalf("load calls=%d", got)
	}
}

func TestCommitUnknownResolvedCommittedAndNotCommitted(t *testing.T) {
	ctx := context.Background()

	t.Run("committed", func(t *testing.T) {
		store := newTestStore(t)
		fence := setShardAuthority(t, store, actorrouting.DomainPlayer, 9, "holder-a", 1, 1)
		fault := authoritystore.NewFaultStore(store)
		provider := newProvider(t, fault, fence)
		binding := claimBinding(t, provider, actorrouting.DomainPlayer, 9, authoritystore.ModelState{RecordModelField: []byte("init")})
		fault.Inject(authoritystore.MethodCommit, authoritystore.FaultTimeoutUnknownAfterApply)
		if err := binding.CASEpoch(ctx, 9, 1, 1, []byte("v2")); !errors.Is(err, blobstore.ErrTimeoutUnknown) {
			t.Fatalf("cas err=%v", err)
		}
		if !binding.HasPendingUnknown() {
			t.Fatal("pending unknown not recorded")
		}
		// Unknown 未解析前保守阻塞业务写(合同 30 §7.6)。
		if err := binding.CASEpoch(ctx, 9, 1, 1, []byte("blocked")); !errors.Is(err, blobstore.ErrTimeoutUnknown) {
			t.Fatalf("blocked cas err=%v", err)
		}
		resolution, err := binding.ResolveUnknown(ctx)
		if err != nil || resolution != ResolutionCommitted {
			t.Fatalf("resolution=%v err=%v", resolution, err)
		}
		data, version, err := binding.Get(ctx, 9)
		if err != nil || string(data) != "v2" || version != 2 {
			t.Fatalf("get data=%q version=%d err=%v", data, version, err)
		}
	})

	t.Run("not_committed_then_retry", func(t *testing.T) {
		store := newTestStore(t)
		fence := setShardAuthority(t, store, actorrouting.DomainPlayer, 10, "holder-a", 1, 1)
		fault := authoritystore.NewFaultStore(store)
		provider := newProvider(t, fault, fence)
		binding := claimBinding(t, provider, actorrouting.DomainPlayer, 10, authoritystore.ModelState{RecordModelField: []byte("init")})
		fault.Inject(authoritystore.MethodCommit, authoritystore.FaultTimeoutUnknownBeforeApply)
		if err := binding.CASEpoch(ctx, 10, 1, 1, []byte("v2")); !errors.Is(err, blobstore.ErrTimeoutUnknown) {
			t.Fatalf("cas err=%v", err)
		}
		resolution, err := binding.ResolveUnknown(ctx)
		if err != nil || resolution != ResolutionNotCommitted {
			t.Fatalf("resolution=%v err=%v", resolution, err)
		}
		// 确定未提交:同一幂等身份(同版本键)重试必须成功且只生效一次。
		if err := binding.CASEpoch(ctx, 10, 1, 1, []byte("v2")); err != nil {
			t.Fatal(err)
		}
		data, version, err := binding.Get(ctx, 10)
		if err != nil || string(data) != "v2" || version != 2 {
			t.Fatalf("get data=%q version=%d err=%v", data, version, err)
		}
	})

	t.Run("fence_unknown_keeps_stable_resolver_identity", func(t *testing.T) {
		store := newTestStore(t)
		fence := setShardAuthority(t, store, actorrouting.DomainPlayer, 11, "holder-a", 1, 1)
		fault := authoritystore.NewFaultStore(store)
		provider := newProvider(t, fault, fence)
		binding := claimBinding(t, provider, actorrouting.DomainPlayer, 11, authoritystore.ModelState{RecordModelField: []byte("init")})
		fault.Inject(authoritystore.MethodCommit, authoritystore.FaultTimeoutUnknownAfterApply)
		if err := binding.CASEpoch(ctx, 11, 1, 1, []byte("v2")); !errors.Is(err, blobstore.ErrTimeoutUnknown) {
			t.Fatalf("cas err=%v", err)
		}
		fault.Inject(authoritystore.MethodFenceAndClaim, authoritystore.FaultTimeoutUnknownBeforeApply)
		resolution, err := binding.ResolveUnknown(ctx)
		if err != nil || resolution != ResolutionRetry {
			t.Fatalf("resolution=%v err=%v", resolution, err)
		}
		// Fence 自身未知:保持隔离并以同一 Resolver 身份重试,取得持久结论。
		resolution, err = binding.ResolveUnknown(ctx)
		if err != nil || resolution != ResolutionCommitted {
			t.Fatalf("resolution=%v err=%v", resolution, err)
		}
	})
}

func TestStaleWriterLateWriteRejected(t *testing.T) {
	store := newTestStore(t)
	fence := setShardAuthority(t, store, actorrouting.DomainPlayer, 12, "holder-a", 1, 1)
	provider := newProvider(t, store, fence)
	ctx := context.Background()
	binding := claimBinding(t, provider, actorrouting.DomainPlayer, 12, authoritystore.ModelState{RecordModelField: []byte("init")})
	if err := binding.CASEpoch(ctx, 12, 1, 1, []byte("v2")); err != nil {
		t.Fatal(err)
	}
	// RouteManager 换代:holder-b 取得更高 WriterEpoch。
	setShardAuthority(t, store, actorrouting.DomainPlayer, 12, "holder-b", 2, 2)
	// 旧 writer(旧 epoch/旧 fence)迟到写必须被拒。
	if err := binding.CASEpoch(ctx, 12, 2, 1, []byte("stale-write")); !errors.Is(err, blobstore.ErrWriterEpochMismatch) {
		t.Fatalf("stale cas err=%v", err)
	}
	if _, _, err := binding.Get(ctx, 12); !errors.Is(err, blobstore.ErrWriterEpochMismatch) {
		t.Fatalf("stale get err=%v", err)
	}
	// 新 Holder 以新 fence 接管:新 claim 看到旧提交(v2)。
	fenceB := authoritystore.ShardWriteFence{
		ExpectedHolderID: "holder-b", ExpectedHolderBootID: "holder-b-boot-1",
		ExpectedAssignmentRevision: 2, ExpectedWriterEpoch: 2, ExpectedLeaseEpoch: 2,
	}
	providerB, err := NewProvider(ProviderOptions{
		Store: store, Fence: StaticFenceSource{Fence: fenceB},
		AuthorityLayoutVersion: "v1", BootID: "holder-b-boot-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	bindingB := claimBinding(t, providerB, actorrouting.DomainPlayer, 12, nil)
	data, version, err := bindingB.Get(ctx, 12)
	if err != nil || string(data) != "v2" || version != 2 {
		t.Fatalf("get data=%q version=%d err=%v", data, version, err)
	}
	if binding.WriterClaimID() == bindingB.WriterClaimID() {
		t.Fatal("writer claim must rotate across activations")
	}
}

func requireUntrustworthyUsage(t *testing.T, binding *Binding) {
	t.Helper()
	usage := binding.PhysicalUsage()
	if usage.Trustworthy || usage.Validate(binding.Version()) == nil {
		t.Fatalf("usage remained trustworthy at version %d: %+v", binding.Version(), usage)
	}
}

func TestBindingInvalidatesPhysicalUsageOnCASConflictUntilFreshLoad(t *testing.T) {
	store := newTestStore(t)
	fence := setShardAuthority(t, store, actorrouting.DomainPlayer, 14, "holder-a", 1, 1)
	provider := newProvider(t, store, fence)
	binding := claimBinding(t, provider, actorrouting.DomainPlayer, 14,
		authoritystore.ModelState{RecordModelField: []byte("v1")})
	if err := binding.PhysicalUsage().Validate(binding.Version()); err != nil {
		t.Fatalf("initial usage=%+v err=%v", binding.PhysicalUsage(), err)
	}

	external := binding.casepochRequest(binding.Version(), []byte("external-v2"))
	external.CommitRequestID = "commit-external-v2"
	external.IdempotencyKey.OperationID = "external-v2"
	external.Receipt.ReceiptID = "receipt-external-v2"
	external.Receipt.IdempotencyKey = external.IdempotencyKey
	committed, err := store.Commit(context.Background(), external)
	if err != nil || committed.Status != authoritystore.CommitCommitted {
		t.Fatalf("external commit=%+v err=%v", committed, err)
	}
	if err := binding.CASEpoch(context.Background(), 14, binding.Version(), 1, []byte("stale-v2")); !errors.Is(err, blobstore.ErrCASConflict) {
		t.Fatalf("CAS conflict err=%v", err)
	}
	requireUntrustworthyUsage(t, binding)

	data, version, err := binding.Get(context.Background(), 14)
	if err != nil || string(data) != "external-v2" || version != committed.Snapshot.RecordVersion {
		t.Fatalf("fresh load data=%q version=%d err=%v", data, version, err)
	}
	if err := binding.PhysicalUsage().Validate(version); err != nil {
		t.Fatalf("fresh usage=%+v err=%v", binding.PhysicalUsage(), err)
	}
}

func TestBindingGetWriterMismatchInvalidatesUsageUntilFreshActivation(t *testing.T) {
	store := newTestStore(t)
	fenceA := setShardAuthority(t, store, actorrouting.DomainPlayer, 15, "holder-a", 1, 1)
	providerA := newProvider(t, store, fenceA)
	bindingA := claimBinding(t, providerA, actorrouting.DomainPlayer, 15,
		authoritystore.ModelState{RecordModelField: []byte("v1")})

	setShardAuthority(t, store, actorrouting.DomainPlayer, 15, "holder-b", 2, 2)
	if _, _, err := bindingA.Get(context.Background(), 15); !errors.Is(err, blobstore.ErrWriterEpochMismatch) {
		t.Fatalf("stale get err=%v", err)
	}
	requireUntrustworthyUsage(t, bindingA)

	fenceB := authoritystore.ShardWriteFence{
		ExpectedHolderID: "holder-b", ExpectedHolderBootID: "holder-b-boot-1",
		ExpectedAssignmentRevision: 2, ExpectedWriterEpoch: 2, ExpectedLeaseEpoch: 2,
	}
	providerB := newProvider(t, store, fenceB)
	bindingB := claimBinding(t, providerB, actorrouting.DomainPlayer, 15, nil)
	if _, version, err := bindingB.Get(context.Background(), 15); err != nil {
		t.Fatal(err)
	} else if err := bindingB.PhysicalUsage().Validate(version); err != nil {
		t.Fatalf("reactivated usage=%+v err=%v", bindingB.PhysicalUsage(), err)
	}
}

func TestRetiredBindingUnknownHasLifecycleOnlyTerminalSettlement(t *testing.T) {
	store := newTestStore(t)
	fence := setShardAuthority(t, store, actorrouting.DomainPlayer, 18, "holder-a", 1, 1)
	fault := authoritystore.NewFaultStore(store)
	provider := newProvider(t, fault, fence)
	binding := claimBinding(t, provider, actorrouting.DomainPlayer, 18,
		authoritystore.ModelState{RecordModelField: []byte("v1")})
	fault.Inject(authoritystore.MethodCommit, authoritystore.FaultTimeoutUnknownBeforeApply)
	if err := binding.CASEpoch(context.Background(), 18, 1, 1, []byte("unknown-v2")); !errors.Is(err, blobstore.ErrTimeoutUnknown) {
		t.Fatalf("unknown commit err=%v", err)
	}
	binding.Retire()
	if resolution, err := binding.ResolveUnknown(context.Background()); resolution != ResolutionReconcile || !errors.Is(err, ErrBindingRetired) {
		t.Fatalf("business resolver on retired binding resolution=%v err=%v", resolution, err)
	}
	resolution, err := binding.ResolveUnknownForRetirement(context.Background())
	if err != nil || resolution != ResolutionNotCommitted {
		t.Fatalf("lifecycle resolution=%v err=%v", resolution, err)
	}
	if binding.HasPendingUnknown() {
		t.Fatal("lifecycle settlement retained UNKNOWN evidence")
	}
	if snapshot := binding.Lane().Snapshot(); snapshot.State != persistence.Draining || snapshot.Dirty {
		t.Fatalf("retired lifecycle lane=%+v", snapshot)
	}
}

func TestBindingResolveUnknownWriterClaimMismatchAndReconcileInvalidateUsage(t *testing.T) {
	t.Run("writer_claim_mismatch", func(t *testing.T) {
		store := newTestStore(t)
		fence := setShardAuthority(t, store, actorrouting.DomainPlayer, 16, "holder-a", 1, 1)
		fault := authoritystore.NewFaultStore(store)
		provider := newProvider(t, fault, fence)
		binding := claimBinding(t, provider, actorrouting.DomainPlayer, 16,
			authoritystore.ModelState{RecordModelField: []byte("v1")})
		fault.Inject(authoritystore.MethodCommit, authoritystore.FaultTimeoutUnknownBeforeApply)
		if err := binding.CASEpoch(context.Background(), 16, 1, 1, []byte("unknown-v2")); !errors.Is(err, blobstore.ErrTimeoutUnknown) {
			t.Fatalf("unknown commit err=%v", err)
		}
		claimBinding(t, provider, actorrouting.DomainPlayer, 16, nil)
		resolution, err := binding.ResolveUnknown(context.Background())
		if err != nil || resolution != ResolutionReconcile {
			t.Fatalf("resolution=%v err=%v", resolution, err)
		}
		requireUntrustworthyUsage(t, binding)
	})

	t.Run("resolution_receipt_reconcile", func(t *testing.T) {
		store := newTestStore(t)
		fence := setShardAuthority(t, store, actorrouting.DomainPlayer, 17, "holder-a", 1, 1)
		fault := authoritystore.NewFaultStore(store)
		provider := newProvider(t, fault, fence)
		binding := claimBinding(t, provider, actorrouting.DomainPlayer, 17,
			authoritystore.ModelState{RecordModelField: []byte("v1")})
		fault.Inject(authoritystore.MethodCommit, authoritystore.FaultTimeoutUnknownAfterApply)
		if err := binding.CASEpoch(context.Background(), 17, 1, 1, []byte("unknown-v2")); !errors.Is(err, blobstore.ErrTimeoutUnknown) {
			t.Fatalf("unknown commit err=%v", err)
		}
		fault.Inject(authoritystore.MethodFenceAndClaim, authoritystore.FaultTimeoutUnknownAfterApply)
		resolution, err := binding.ResolveUnknown(context.Background())
		if err != nil || resolution != ResolutionRetry {
			t.Fatalf("first resolution=%v err=%v", resolution, err)
		}
		binding.fenceID.newWriterClaimID += "_different"
		resolution, err = binding.ResolveUnknown(context.Background())
		if err != nil || resolution != ResolutionReconcile {
			t.Fatalf("reconciled resolution=%v err=%v", resolution, err)
		}
		requireUntrustworthyUsage(t, binding)
	})
}

func TestMetricsCounters(t *testing.T) {
	store := newTestStore(t)
	fence := setShardAuthority(t, store, actorrouting.DomainPlayer, 13, "holder-a", 1, 1)
	provider := newProvider(t, store, fence)
	ctx := context.Background()
	binding := claimBinding(t, provider, actorrouting.DomainPlayer, 13, authoritystore.ModelState{RecordModelField: []byte("init")})
	if err := binding.CASEpoch(ctx, 13, 1, 1, []byte("v2")); err != nil {
		t.Fatal(err)
	}
	snap := provider.Metrics().Snapshot()
	if snap.ClaimAcquiredTotal != 1 || snap.CommitOKTotal != 1 {
		t.Fatalf("snapshot=%+v", snap)
	}
}
