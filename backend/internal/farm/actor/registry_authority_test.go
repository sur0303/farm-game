package actor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"
	"farmgame/internal/actorrouting"
	"farmgame/internal/logic/actor/activation"
	"farmgame/internal/logic/actor/authority"
	"farmgame/internal/logic/actor/persistence"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/blobstore"
	redisauthority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

var ar1Now = time.UnixMilli(1_770_200_000_000)

// ar1Rig 是 authority 模式 Registry 的测试装配:同一套测试体在
// MemoryStore(合同 Fake)与真实 Redis Adapter(miniredis)上各跑一遍。
type ar1Rig struct {
	fault       *authoritystore.FaultStore
	provider    *authority.Provider
	registry    *Registry
	modelReader authoritystore.ModelFieldBatchReader
	// reassign 把该 farm 所在 Shard 的写权换代给另一个 Holder(旧 Holder
	// 的 fence 随之失效),返回新 Holder 的 fence。
	reassign func(t *testing.T, holderID string, writerEpoch, leaseEpoch uint64) authoritystore.ShardWriteFence
}

func ar1Calculator(t *testing.T) *actorrouting.Calculator {
	t.Helper()
	calculator, err := actorrouting.NewCalculator(
		"test-realm",
		actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608040011},
		64, 64,
	)
	if err != nil {
		t.Fatal(err)
	}
	return calculator
}

func ar1MemoryRig(t *testing.T, farmID uint64) *ar1Rig {
	return ar1MemoryRigWithLaneSize(t, farmID, 0)
}

func ar1MemoryRigWithLaneSize(t *testing.T, farmID uint64, farmEventLaneSize int) *ar1Rig {
	t.Helper()
	calculator := ar1Calculator(t)
	store, err := authoritystore.NewMemoryStore(authoritystore.MemoryOptions{
		Resolver:               calculator,
		AuthorityLayoutVersion: "v1",
		Now:                    func() time.Time { return ar1Now },
	})
	if err != nil {
		t.Fatal(err)
	}
	setAuthority := func(t *testing.T, holderID string, writerEpoch, leaseEpoch uint64) authoritystore.ShardWriteFence {
		t.Helper()
		shard, err := calculator.ShardFor(actorrouting.DomainFarm, farmID)
		if err != nil {
			t.Fatal(err)
		}
		authority := authoritystore.ShardWriteAuthority{
			AuthorityLayoutVersion: "v1",
			Domain:                 actorrouting.DomainFarm,
			VirtualShard:           shard,
			HolderID:               holderID,
			HolderBootID:           holderID + "-boot",
			AssignmentRevision:     writerEpoch,
			WriterEpoch:            writerEpoch,
			LeaseEpoch:             leaseEpoch,
			State:                  authoritystore.ShardAuthorityActive,
			LeaseUntilMs:           ar1Now.Add(time.Minute).UnixMilli(),
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
	fence := setAuthority(t, "farm-holder-a", 1, 1)
	fault := authoritystore.NewFaultStore(store)
	provider, err := authority.NewProvider(authority.ProviderOptions{
		Store:                  fault,
		Fence:                  authority.StaticFenceSource{Fence: fence},
		AuthorityLayoutVersion: "v1",
		BootID:                 "farm-holder-a-boot",
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := NewAuthorityRegistry(provider, RegistryOptions{MailboxSize: 8, FarmEventLaneSize: farmEventLaneSize})
	t.Cleanup(registry.Stop)
	return &ar1Rig{fault: fault, provider: provider, registry: registry, modelReader: store, reassign: setAuthority}
}

func ar1RedisRig(t *testing.T, farmID uint64) *ar1Rig {
	t.Helper()
	server := miniredis.RunT(t)
	server.SetTime(ar1Now)
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
	calculator := ar1Calculator(t)
	store, err := redisauthority.NewStore(client, nil, redisauthority.Options{Profile: profile, Resolver: calculator})
	if err != nil {
		t.Fatal(err)
	}
	shard, err := calculator.ShardFor(actorrouting.DomainFarm, farmID)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	caller := authoritystore.TrustedShardAuthorityCaller{
		Kind:       authoritystore.ShardAuthorityCallerRouteManager,
		InstanceID: "route-manager-1",
		BootID:     "rm-boot-1",
	}
	transitionSeq := 0
	var lastFence authoritystore.ShardWriteFence
	setAuthority := func(t *testing.T, holderID string, writerEpoch, leaseEpoch uint64) authoritystore.ShardWriteFence {
		t.Helper()
		transitionSeq++
		transitionID := fmt.Sprintf("ar1-transition-%d", transitionSeq)
		var result authoritystore.ShardAuthorityTransitionResult
		var err error
		if transitionSeq == 1 {
			result, err = store.ClaimShardAuthority(ctx, authoritystore.ClaimShardAuthorityRequest{
				Caller: caller, TransitionID: transitionID, AuthorityLayoutVersion: "v1",
				Domain: actorrouting.DomainFarm, VirtualShard: shard,
				NewHolderID: holderID, NewHolderBootID: holderID + "-boot", RequestedTTLms: 60_000,
			})
		} else {
			result, err = store.TransferShardAuthority(ctx, authoritystore.TransferShardAuthorityRequest{
				Caller: caller, TransitionID: transitionID, AuthorityLayoutVersion: "v1",
				Domain: actorrouting.DomainFarm, VirtualShard: shard, ExpectedFence: lastFence,
				NewHolderID: holderID, NewHolderBootID: holderID + "-boot", RequestedTTLms: 60_000,
			})
		}
		if err != nil {
			t.Fatal(err)
		}
		if result.Status != authoritystore.ShardTransitionApplied {
			t.Fatalf("shard transition status=%s", result.Status)
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
	fence := setAuthority(t, "farm-holder-a", 1, 1)
	fault := authoritystore.NewFaultStore(store)
	provider, err := authority.NewProvider(authority.ProviderOptions{
		Store:                  fault,
		Fence:                  authority.StaticFenceSource{Fence: fence},
		AuthorityLayoutVersion: "v1",
		BootID:                 "farm-holder-a-boot",
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := NewAuthorityRegistry(provider, RegistryOptions{MailboxSize: 8})
	t.Cleanup(registry.Stop)
	return &ar1Rig{fault: fault, provider: provider, registry: registry, modelReader: store, reassign: setAuthority}
}

func ar1Rigs(t *testing.T, farmID uint64) map[string]*ar1Rig {
	return map[string]*ar1Rig{
		"memory": ar1MemoryRig(t, farmID),
		"redis":  ar1RedisRig(t, farmID),
	}
}

func ar1Plant(ctx context.Context, registry *Registry, farmID uint64, operationID string, hash []byte) (*ActionResult, error) {
	return ar1PlantAt(ctx, registry, farmID, 0, operationID, hash)
}

func ar1PlantAt(ctx context.Context, registry *Registry, farmID uint64, plotIndex uint32, operationID string, hash []byte) (*ActionResult, error) {
	now := time.UnixMilli(1_800_000_000_000)
	value, err := registry.Submit(ctx, farmID, func(ctx context.Context, actor *FarmActor) (any, error) {
		return actor.Plant(ctx, operationID, testPlantCropCycleID(farmID, operationID), hash, &farmpb.PlantCropRequest{
			PlotIndex: plotIndex, CropDefinitionId: 1,
		}, 0, 1, now, time.Second)
	})
	if err != nil {
		return nil, err
	}
	return value.(*ActionResult), nil
}

func TestAuthorityFarmEmptyActivation(t *testing.T) {
	for name, rig := range ar1Rigs(t, 901) {
		t.Run(name, func(t *testing.T) {
			now := time.UnixMilli(1_800_000_000_000)
			value, err := rig.registry.Submit(context.Background(), 901, func(_ context.Context, actor *FarmActor) (any, error) {
				return actor.Snapshot(now), nil
			})
			if err != nil {
				t.Fatal(err)
			}
			snapshot := value.(*farmpb.FarmSnapshot)
			if len(snapshot.Plots) != DefaultPlotCount {
				t.Fatalf("empty farm plots=%d", len(snapshot.Plots))
			}
			if got := rig.registry.ActivationStats().StateActive; got != 1 {
				t.Fatalf("active=%d", got)
			}
			// 激活安装直接消费 LoadAndClaim 的有界全模型快照。
			if got := rig.fault.Calls(authoritystore.MethodLoadAndClaim); got != 1 {
				t.Fatalf("load-and-claim calls=%d", got)
			}
		})
	}
}

func TestAuthorityFarmConcurrentActivationSingleLoad(t *testing.T) {
	rig := ar1MemoryRig(t, 902)
	ctx := context.Background()
	now := time.UnixMilli(1_800_000_000_000)
	// 大 Mailbox,排除有界邮箱背压对激活合并证明的干扰。
	registry := NewAuthorityRegistry(rig.provider, RegistryOptions{MailboxSize: 64})
	defer registry.Stop()
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := registry.Submit(ctx, 902, func(_ context.Context, actor *FarmActor) (any, error) {
				return actor.Snapshot(now), nil
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
	// 同 Actor 并发首次请求合并为一次激活(30 §5.1 singleflight)。
	if stats := registry.LoadFutureStats(); stats.StartedTotal != 1 {
		t.Fatalf("load flights started=%d", stats.StartedTotal)
	}
	if got := rig.fault.Calls(authoritystore.MethodLoadAndClaim); got != 1 {
		t.Fatalf("load-and-claim calls=%d", got)
	}
	if got := registry.RuntimeStats().ActiveKeys; got != 1 {
		t.Fatalf("active keys=%d", got)
	}
}

func TestAuthorityFarmPlantCommitAndReplay(t *testing.T) {
	for name, rig := range ar1Rigs(t, 903) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			result, err := ar1Plant(ctx, rig.registry, 903, "plant-1", []byte("plant-1"))
			if err != nil || result.Status != commonpb.CommandStatus_PROCESSING {
				t.Fatalf("plant=%v err=%v", result, err)
			}
			replayed, err := ar1Plant(ctx, rig.registry, 903, "plant-1", []byte("plant-1"))
			if err != nil || !replayed.Replayed {
				t.Fatalf("replay=%v err=%v", replayed, err)
			}
		})
	}
}

func TestAuthorityFarmCommitUnknownResolvedCommitted(t *testing.T) {
	rig := ar1MemoryRig(t, 904)
	ctx := context.Background()
	// 提交已落库但响应丢失:命令上交 UNKNOWN,后台 Resolver 经
	// FenceAndClaim 证明已提交;重放同一 operation 命中记录内 Receipt。
	rig.fault.Inject(authoritystore.MethodCommit, authoritystore.FaultTimeoutUnknownAfterApply)
	if _, err := ar1Plant(ctx, rig.registry, 904, "plant-u1", []byte("plant-u1")); !errors.Is(err, blobstore.ErrTimeoutUnknown) {
		t.Fatalf("plant err=%v", err)
	}
	waitForState(t, rig.registry, 904, activation.Active)
	if got := rig.provider.Metrics().Snapshot().ResolveCommitted; got != 1 {
		t.Fatalf("resolve committed=%d", got)
	}
	replayed, err := ar1Plant(ctx, rig.registry, 904, "plant-u1", []byte("plant-u1"))
	if err != nil || !replayed.Replayed {
		t.Fatalf("replay=%v err=%v", replayed, err)
	}
	now := time.UnixMilli(1_800_000_000_000)
	value, err := rig.registry.Submit(ctx, 904, func(_ context.Context, actor *FarmActor) (any, error) {
		return actor.Snapshot(now), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := ar1PlantedPlots(value.(*farmpb.FarmSnapshot)); got != 1 {
		t.Fatalf("planted plots=%d (effect must land exactly once)", got)
	}
}

func TestAuthorityFarmCommitUnknownResolvedNotCommitted(t *testing.T) {
	rig := ar1MemoryRig(t, 905)
	ctx := context.Background()
	// 提交确定未落库:Resolver 证明 NOT_COMMITTED;同一 operation 重新执行
	// 不是重放,且只生效一次。
	rig.fault.Inject(authoritystore.MethodCommit, authoritystore.FaultTimeoutUnknownBeforeApply)
	if _, err := ar1Plant(ctx, rig.registry, 905, "plant-u2", []byte("plant-u2")); !errors.Is(err, blobstore.ErrTimeoutUnknown) {
		t.Fatalf("plant err=%v", err)
	}
	waitForState(t, rig.registry, 905, activation.Active)
	if got := rig.provider.Metrics().Snapshot().ResolveNotCommitted; got != 1 {
		t.Fatalf("resolve not-committed=%d", got)
	}
	result, err := ar1Plant(ctx, rig.registry, 905, "plant-u2", []byte("plant-u2"))
	if err != nil {
		t.Fatal(err)
	}
	if result.Replayed {
		t.Fatal("not-committed operation must re-execute, not replay")
	}
	now := time.UnixMilli(1_800_000_000_000)
	value, err := rig.registry.Submit(ctx, 905, func(_ context.Context, actor *FarmActor) (any, error) {
		return actor.Snapshot(now), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := ar1PlantedPlots(value.(*farmpb.FarmSnapshot)); got != 1 {
		t.Fatalf("planted plots=%d", got)
	}
}

func TestAuthorityFarmStaleWriterLateWriteRejected(t *testing.T) {
	rig := ar1MemoryRig(t, 906)
	ctx := context.Background()
	if _, err := ar1Plant(ctx, rig.registry, 906, "plant-a", []byte("plant-a")); err != nil {
		t.Fatal(err)
	}
	// RouteManager 换代给 holder-b;旧 Holder 的迟到写必须被存储 Fence 拒绝,
	// 旧 Activation 被驱逐(27 §16-28/30)。
	rig.reassign(t, "farm-holder-b", 2, 2)
	// 另一块空地:写必须到达 save 才会撞上存储 Fence(而不是领域拒绝)。
	_, err := ar1PlantAt(ctx, rig.registry, 906, 1, "plant-b", []byte("plant-b"))
	if !errors.Is(err, blobstore.ErrWriterEpochMismatch) {
		t.Fatalf("stale write err=%v", err)
	}
	if got := rig.registry.Activation().StateOf(906); got != activation.Absent {
		t.Fatalf("fenced actor state=%s", got)
	}
	if got := rig.provider.Metrics().Snapshot().CommitEpochMismatch; got == 0 {
		t.Fatal("writer epoch mismatch not observed")
	}
}

func TestAuthorityFarmColdLoadRecovery(t *testing.T) {
	for name, rig := range ar1Rigs(t, 907) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			if _, err := ar1Plant(ctx, rig.registry, 907, "plant-cold", []byte("plant-cold")); err != nil {
				t.Fatal(err)
			}
			// 模拟进程崩溃:不做 passivation,直接在同一 Store 上重建 Registry
			// (同一 Holder 新进程代次);持久 Claim 由新 Activation 原子换代。
			restarted := NewAuthorityRegistry(rig.provider, RegistryOptions{MailboxSize: 8})
			t.Cleanup(restarted.Stop)
			replayed, err := ar1Plant(ctx, restarted, 907, "plant-cold", []byte("plant-cold"))
			if err != nil || !replayed.Replayed {
				t.Fatalf("cold-load replay=%v err=%v", replayed, err)
			}
			now := time.UnixMilli(1_800_000_000_000)
			value, err := restarted.Submit(ctx, 907, func(_ context.Context, actor *FarmActor) (any, error) {
				return actor.Snapshot(now), nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if got := ar1PlantedPlots(value.(*farmpb.FarmSnapshot)); got != 1 {
				t.Fatalf("cold-load planted plots=%d", got)
			}
		})
	}
}

func TestAuthorityFarmPassivationReloadsReceipt(t *testing.T) {
	rig := ar1MemoryRig(t, 908)
	ctx := context.Background()
	if _, err := ar1Plant(ctx, rig.registry, 908, "plant-p", []byte("plant-p")); err != nil {
		t.Fatal(err)
	}
	if err := rig.registry.BeginPassivate(908); err != nil {
		t.Fatal(err)
	}
	if err := rig.registry.FinishPassivate(908); err != nil {
		t.Fatal(err)
	}
	if got := rig.registry.Activation().StateOf(908); got != activation.Absent {
		t.Fatalf("state=%s", got)
	}
	// 钝化后重新激活:从 AuthorityRecord 恢复,Receipt/Outcome 可重放。
	replayed, err := ar1Plant(ctx, rig.registry, 908, "plant-p", []byte("plant-p"))
	if err != nil || !replayed.Replayed {
		t.Fatalf("post-passivation replay=%v err=%v", replayed, err)
	}
	if got := rig.registry.ActivationStats().ReactivatedTotal; got != 1 {
		t.Fatalf("reactivated=%d", got)
	}
}

func TestAuthorityFarmActorSoftStaysDisabled(t *testing.T) {
	rig := ar1MemoryRig(t, 909)
	ctx := context.Background()
	if _, err := ar1Plant(ctx, rig.registry, 909, "plant-soft", []byte("plant-soft")); err != nil {
		t.Fatal(err)
	}
	// ACTOR_SOFT 关闭:每 Actor 的 Persistence Lane 拒绝 SoftSave;
	// 且成功命令的 Durable 水位已推进到当前 RecordVersion(不存在
	// "内存先成功、异步落库"的资产成功边界)。
	_, err := rig.registry.Submit(ctx, 909, func(_ context.Context, actor *FarmActor) (any, error) {
		binding, ok := actor.store.(*authority.Binding)
		if !ok {
			return nil, errors.New("authority binding missing")
		}
		if _, err := binding.Lane().BeginSoft(actor.recordVersion + 1); !errors.Is(err, persistence.ErrSoftSaveDisabled) {
			return nil, err
		}
		snap := binding.Lane().Snapshot()
		if snap.DurableThroughVersion != actor.recordVersion || snap.Dirty {
			return nil, errors.New("durable watermark behind memory state")
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func waitForState(t *testing.T, registry *Registry, farmID uint64, want activation.State) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if registry.Activation().StateOf(farmID) == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("state did not reach %s (now %s)", want, registry.Activation().StateOf(farmID))
}

// ar1FastBackoffRegistry 用 1ms 失败退避构造 authority Registry,使
// "失败后立即重试被拒 → 退避结束后重新激活成功"可确定性断言。
func ar1FastBackoffRegistry(t *testing.T, rig *ar1Rig) *Registry {
	t.Helper()
	registry := NewAuthorityRegistry(rig.provider, RegistryOptions{
		MailboxSize: 8,
		Activation:  activation.Options{LoadFailureBackoff: time.Millisecond},
	})
	t.Cleanup(registry.Stop)
	return registry
}

func ar1NoActiveWithoutActor(t *testing.T, registry *Registry, farmID uint64) {
	t.Helper()
	state := registry.Activation().StateOf(farmID)
	if state == activation.Active {
		registry.mu.Lock()
		_, installed := registry.actors[farmID]
		registry.mu.Unlock()
		if !installed {
			t.Fatalf("ACTIVE-without-actor wedge (review P1-1)")
		}
	}
}

// ClaimUnknown recovery installs only the ALREADY_CLAIMED all-core snapshot;
// M1-B deliberately removed the independently timed third activation read.
func TestAuthorityFarmClaimUnknownRecoveryUsesClaimSnapshot(t *testing.T) {
	rig := ar1MemoryRig(t, 921)
	registry := ar1FastBackoffRegistry(t, rig)
	rig.fault.Inject(authoritystore.MethodLoadAndClaim,
		authoritystore.FaultTimeoutUnknownAfterApply,
		authoritystore.FaultNone,
	)
	value, err := registry.Submit(context.Background(), 921, func(_ context.Context, actor *FarmActor) (any, error) {
		return actor.Snapshot(time.UnixMilli(1_800_000_000_000)), nil
	})
	if err != nil || len(value.(*farmpb.FarmSnapshot).Plots) != DefaultPlotCount {
		t.Fatalf("snapshot=%v err=%v", value, err)
	}
	if got := rig.fault.Calls(authoritystore.MethodLoadAndClaim); got != 2 {
		t.Fatalf("load-and-claim calls=%d", got)
	}
	if got := registry.Activation().StateOf(921); got != activation.Active {
		t.Fatalf("state=%s want ACTIVE", got)
	}
}

// 恢复循环最终以 ClaimUnavailable 结束时保留 Unavailable 分类,不误报 TimeoutUnknown。
func TestAuthorityFarmClaimRecoveryUnavailableKeepsClassification(t *testing.T) {
	rig := ar1MemoryRig(t, 922)
	registry := ar1FastBackoffRegistry(t, rig)
	rig.fault.Inject(authoritystore.MethodLoadAndClaim,
		authoritystore.FaultTimeoutUnknownAfterApply,
		authoritystore.FaultUnavailableNotSent,
	)
	now := time.UnixMilli(1_800_000_000_000)
	_, err := registry.Submit(context.Background(), 922, func(_ context.Context, actor *FarmActor) (any, error) {
		return actor.Snapshot(now), nil
	})
	if !errors.Is(err, blobstore.ErrUnavailable) || errors.Is(err, blobstore.ErrTimeoutUnknown) {
		t.Fatalf("submit err=%v want ErrUnavailable (not TimeoutUnknown)", err)
	}
	if got := registry.Activation().StateOf(922); got != activation.Absent {
		t.Fatalf("state=%s want ABSENT", got)
	}
}

// 记录损坏(解码失败)与 farm_id 完整性检查共用 fail 清理路径:
// 恢复成功后解码失败同样回到 ABSENT,不留 ACTIVE-without-actor。
func TestAuthorityFarmCorruptRecordFailsClosedWithoutWedge(t *testing.T) {
	rig := ar1MemoryRig(t, 923)
	ctx := context.Background()
	// 预置损坏记录:同一 Store 上以独立 claim 安装非 Protobuf 字节。
	corrupt, status, err := rig.provider.Claim(ctx, actorrouting.DomainFarm, 923, "claim-corrupt-1", "wc-corrupt-1",
		authoritystore.ModelState{authority.RecordModelField: []byte("!!not-a-farm-record!!")})
	if err != nil || status != authority.ClaimAcquired {
		t.Fatalf("seed claim status=%v err=%v", status, err)
	}
	if _, _, err := corrupt.Get(ctx, 923); err != nil {
		t.Fatal(err)
	}
	registry := ar1FastBackoffRegistry(t, rig)
	// claim#1 应用后丢响应 → 重入成功 → 激活读解码失败(recovery 路径的 fail 分支)。
	rig.fault.Inject(authoritystore.MethodLoadAndClaim,
		authoritystore.FaultTimeoutUnknownAfterApply,
		authoritystore.FaultNone,
	)
	now := time.UnixMilli(1_800_000_000_000)
	_, err = registry.Submit(ctx, 923, func(_ context.Context, actor *FarmActor) (any, error) {
		return actor.Snapshot(now), nil
	})
	if err == nil {
		t.Fatal("corrupt record must fail activation")
	}
	if got := registry.Activation().StateOf(923); got != activation.Absent {
		t.Fatalf("state=%s want ABSENT", got)
	}
	ar1NoActiveWithoutActor(t, registry, 923)
	// 数据仍损坏:后续激活继续干净地失败(FailLoad/退避),而不是楔死或非法转移。
	time.Sleep(5 * time.Millisecond)
	if _, err := registry.Submit(ctx, 923, func(_ context.Context, actor *FarmActor) (any, error) {
		return actor.Snapshot(now), nil
	}); err == nil {
		t.Fatal("corrupt record must keep failing closed")
	}
	ar1NoActiveWithoutActor(t, registry, 923)
}

func ar1PlantedPlots(snapshot *farmpb.FarmSnapshot) int {
	count := 0
	for _, plot := range snapshot.Plots {
		if plot != nil && plot.CropCycleId != "" {
			count++
		}
	}
	return count
}
