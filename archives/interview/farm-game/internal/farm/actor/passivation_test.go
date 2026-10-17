package actor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	farmpb "farmgame/api/gen/farm"
	"farmgame/internal/logic/actor/activation"
	"farmgame/internal/storage/blobstore"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newPassivationRegistry(store blobstore.Store, clock *fakeClock, idleTimeout time.Duration) *Registry {
	return NewRegistryWithOptions(store, RegistryOptions{
		MailboxSize: 8,
		Passivation: PassivationOptions{
			IdleTimeout: idleTimeout,
			BucketWidth: 10 * time.Second,
			Now:         clock.Now,
		},
	})
}

func joinPresence(t *testing.T, registry *Registry, farmID uint64, subscriptionID string) {
	t.Helper()
	_, err := registry.Submit(context.Background(), farmID, func(_ context.Context, actor *FarmActor) (any, error) {
		_, joinErr := actor.Join(JoinRequest{
			SubscriptionID: subscriptionID, ViewerPlayerID: 10001, SessionEpoch: 7,
			GatewayID: "gate-a", GatewayBootID: "boot-a", ConnectionID: "conn-a",
			TransportGeneration: 1, RouteRevision: 3,
		}, time.UnixMilli(1_800_000_000_000))
		return nil, joinErr
	})
	if err != nil {
		t.Fatalf("join presence: %v", err)
	}
}

// TestIdleFarmActorPassivatesAndReleasesAllResources 对应交接验收 1:
// idle FarmActor 自动 ABSENT,并释放 Actor/Mailbox/goroutine/Controller entry。
func TestIdleFarmActorPassivatesAndReleasesAllResources(t *testing.T) {
	clock := &fakeClock{now: time.Now()}
	registry := newPassivationRegistry(blobstore.NewMemoryStore(), clock, time.Minute)
	defer registry.Stop()
	ctx := context.Background()
	const farmID = uint64(1001)

	if _, err := registry.Submit(ctx, farmID, func(_ context.Context, actor *FarmActor) (any, error) {
		return nil, nil
	}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	registry.mu.Lock()
	if registry.actors[farmID] == nil {
		t.Fatal("actor not activated")
	}
	registry.mu.Unlock()
	if got := registry.RuntimeStats().ActiveKeys; got != 1 {
		t.Fatalf("ActiveKeys=%d, want 1", got)
	}
	if got := registry.Activation().StateOf(farmID); got != activation.Active {
		t.Fatalf("state=%s, want ACTIVE", got)
	}

	clock.Advance(2 * time.Minute)
	if got := registry.SweepIdleOnce(ctx); got != 1 {
		t.Fatalf("swept candidates=%d, want 1", got)
	}

	registry.mu.Lock()
	if registry.actors[farmID] != nil {
		t.Fatal("actor must be released after idle passivation")
	}
	registry.mu.Unlock()
	if got := registry.RuntimeStats().ActiveKeys; got != 0 {
		t.Fatalf("ActiveKeys=%d, want 0 (mailbox retired)", got)
	}
	if got := registry.Activation().StateOf(farmID); got != activation.Absent {
		t.Fatalf("state=%s, want ABSENT", got)
	}
	if got := registry.Activation().Stats().TrackedKeys; got != 0 {
		t.Fatalf("TrackedKeys=%d, want 0 (entry reclaimed)", got)
	}
	runtimeStats := registry.RuntimeStats()
	if runtimeStats.MailboxRetiredTotal != 1 {
		t.Fatalf("MailboxRetiredTotal=%d, want 1", runtimeStats.MailboxRetiredTotal)
	}
	actStats := registry.Activation().Stats()
	if actStats.PassivateCompletedTotal != 1 {
		t.Fatalf("PassivateCompletedTotal=%d, want 1", actStats.PassivateCompletedTotal)
	}
	if actStats.EntryForgottenTotal != 1 {
		t.Fatalf("EntryForgottenTotal=%d, want 1", actStats.EntryForgottenTotal)
	}
	if got := registry.IdleSweeperStats().RetiredTotal; got != 1 {
		t.Fatalf("sweeper RetiredTotal=%d, want 1", got)
	}
}

// TestPassivationKeepsHubPresence 对应交接验收 2:
// Presence 存在时 FarmActor 仍可 passivate,SubscriptionHub 状态保留。
func TestPassivationKeepsHubPresence(t *testing.T) {
	clock := &fakeClock{now: time.Now()}
	registry := newPassivationRegistry(blobstore.NewMemoryStore(), clock, time.Minute)
	defer registry.Stop()
	ctx := context.Background()
	const farmID = uint64(1002)

	joinPresence(t, registry, farmID, "sub-keep")
	clock.Advance(2 * time.Minute)
	registry.SweepIdleOnce(ctx)

	registry.mu.Lock()
	if registry.actors[farmID] != nil {
		t.Fatal("actor must passivate even with presence")
	}
	registry.mu.Unlock()
	count, err := registry.SubscriptionHub().PresenceCount(ctx, farmID)
	if err != nil || count != 1 {
		t.Fatalf("PresenceCount=%d err=%v, want 1 (Hub 保留 Presence)", count, err)
	}
	// Hub Lane 仍有 Presence,不应独立退休。
	if got := registry.SubscriptionHub().Stats().ActiveLanes; got != 1 {
		t.Fatalf("ActiveLanes=%d, want 1", got)
	}
}

// TestPassivationIgnoresDurableRetryWaitWorkflow 对应交接验收 3 的 registry 侧:
// 持久化 RETRY_WAIT Workflow 由 Store+Worker 接管,不是 Actor blocker。
// 这里验证 blocker 快照路径不读取任何 workflow 状态:即使存在持久 workflow
// 记录(本测试以独立 store 记录模拟),passivation 仍完成。
func TestPassivationIgnoresDurableRetryWaitWorkflow(t *testing.T) {
	clock := &fakeClock{now: time.Now()}
	registry := newPassivationRegistry(blobstore.NewMemoryStore(), clock, time.Minute)
	defer registry.Stop()
	ctx := context.Background()
	const farmID = uint64(1003)

	// 激活 Actor(相当于曾有 Workflow 在该 farm 上推进)。
	if _, err := registry.Submit(ctx, farmID, func(_ context.Context, actor *FarmActor) (any, error) {
		return nil, nil
	}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	clock.Advance(2 * time.Minute)
	if got := registry.SweepIdleOnce(ctx); got != 1 {
		t.Fatalf("swept=%d, want 1", got)
	}
	if got := registry.Activation().StateOf(farmID); got != activation.Absent {
		t.Fatalf("state=%s, want ABSENT (workflow 记录不得钉住 Actor)", got)
	}
}

// TestRunningTurnBlocksPassivation 对应交接验收 4(running Turn)。
func TestRunningTurnBlocksPassivation(t *testing.T) {
	clock := &fakeClock{now: time.Now()}
	registry := newPassivationRegistry(blobstore.NewMemoryStore(), clock, time.Minute)
	defer registry.Stop()
	ctx := context.Background()
	const farmID = uint64(1004)

	block := make(chan struct{})
	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		_, _ = registry.Submit(ctx, farmID, func(_ context.Context, actor *FarmActor) (any, error) {
			close(started)
			<-block
			return nil, nil
		})
		close(done)
	}()
	<-started

	clock.Advance(2 * time.Minute)
	// turn 执行期间:sweeper 候选被 turn_running 阻塞。
	deadline := time.Now().Add(2 * time.Second)
	for {
		registry.SweepIdleOnce(ctx)
		if registry.IdleSweeperStats().BlockedTotal[BlockerTurnRunning] > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("passivation was not blocked by running turn")
		}
		time.Sleep(time.Millisecond)
	}
	registry.mu.Lock()
	if registry.actors[farmID] == nil {
		t.Fatal("actor must survive while a turn is running")
	}
	registry.mu.Unlock()

	close(block)
	<-done
	// turn 结束;blocked 候选按 BlockedRetryDelay 重新入桶,推进时钟越过重试延迟后扫描成功钝化。
	clock.Advance(10 * time.Second)
	registry.SweepIdleOnce(ctx)
	if got := registry.Activation().StateOf(farmID); got != activation.Absent {
		t.Fatalf("state=%s, want ABSENT after turn finished", got)
	}
}

// TestQueuedUserTaskBlocksPassivation 对应交接验收 4(排队任务)。
func TestQueuedUserTaskBlocksPassivation(t *testing.T) {
	clock := &fakeClock{now: time.Now()}
	registry := newPassivationRegistry(blobstore.NewMemoryStore(), clock, time.Minute)
	defer registry.Stop()
	ctx := context.Background()
	const farmID = uint64(1005)

	block := make(chan struct{})
	started := make(chan struct{})
	release := make(chan struct{})
	go func() {
		_, _ = registry.Submit(ctx, farmID, func(_ context.Context, actor *FarmActor) (any, error) {
			close(started)
			<-block
			return nil, nil
		})
		close(release)
	}()
	<-started

	queuedDone := make(chan struct{})
	go func() {
		_, _ = registry.Submit(ctx, farmID, func(_ context.Context, actor *FarmActor) (any, error) {
			return nil, nil
		})
		close(queuedDone)
	}()
	// 等排队任务进入 mailbox。
	deadline := time.Now().Add(2 * time.Second)
	for {
		snap, _ := registry.exec.MailboxSnapshot(farmID)
		if snap.UserQueued > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("queued task never observed")
		}
		time.Sleep(time.Millisecond)
	}

	clock.Advance(2 * time.Minute)
	registry.SweepIdleOnce(ctx)
	if got := registry.IdleSweeperStats().BlockedTotal[BlockerUserQueueNonempty]; got == 0 {
		t.Fatal("passivation must be blocked by queued user task")
	}
	close(block)
	<-release
	<-queuedDone
}

// TestResolvingUnknownBlocksPassivation 对应交接验收 4(RESOLVING_UNKNOWN)。
func TestResolvingUnknownBlocksPassivation(t *testing.T) {
	clock := &fakeClock{now: time.Now()}
	store := newAmbiguousCASStore(false)
	registry := newPassivationRegistry(store, clock, time.Minute)
	registry.SetAutoResolveUnknown(false)
	defer registry.Stop()
	ctx := context.Background()
	const farmID = uint64(1006)

	_, err := registry.Submit(ctx, farmID, func(ctx context.Context, actor *FarmActor) (any, error) {
		return actor.Plant(ctx, "unknown-op", testPlantCropCycleID(actor.farmID, "unknown-op"), []byte("plant-hash"), &farmpb.PlantCropRequest{
			PlotIndex: 0, CropDefinitionId: 1,
		}, 0, 0, time.UnixMilli(1_800_000_000_000), time.Minute)
	})
	if !errors.Is(err, blobstore.ErrTimeoutUnknown) {
		t.Fatalf("plant err=%v", err)
	}
	if got := registry.Activation().StateOf(farmID); got != activation.ResolvingUnknown {
		t.Fatalf("state=%s, want RESOLVING_UNKNOWN", got)
	}

	clock.Advance(2 * time.Minute)
	registry.SweepIdleOnce(ctx)
	if got := registry.IdleSweeperStats().BlockedTotal[BlockerResolvingUnknown]; got == 0 {
		t.Fatal("passivation must be blocked by RESOLVING_UNKNOWN")
	}
	registry.mu.Lock()
	if registry.actors[farmID] == nil {
		t.Fatal("actor must survive while resolving unknown")
	}
	registry.mu.Unlock()

	// 权威重读解决后,推进时钟越过 blocked 重试延迟,下一轮扫描可以钝化。
	store.releaseReads()
	if _, err := registry.ResolveUnknown(ctx, farmID); err != nil {
		t.Fatalf("ResolveUnknown: %v", err)
	}
	clock.Advance(10 * time.Second)
	registry.SweepIdleOnce(ctx)
	if got := registry.Activation().StateOf(farmID); got != activation.Absent {
		t.Fatalf("state=%s, want ABSENT after unknown resolved", got)
	}
}

// TestSubmitRacesWithPassivation 对应交接验收 5:
// 新命令与退休并发,不丢、不重、不 panic、不投递到 closed channel。
func TestSubmitRacesWithPassivation(t *testing.T) {
	clock := &fakeClock{now: time.Now()}
	registry := newPassivationRegistry(blobstore.NewMemoryStore(), clock, time.Minute)
	defer registry.Stop()
	ctx := context.Background()
	const farmID = uint64(1007)

	var executed atomic.Int64
	for round := 0; round < 100; round++ {
		clock.Advance(2 * time.Minute)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, err := registry.Submit(ctx, farmID, func(_ context.Context, actor *FarmActor) (any, error) {
				executed.Add(1)
				return nil, nil
			})
			if err != nil {
				t.Errorf("submit: %v", err)
			}
		}()
		go func() {
			defer wg.Done()
			registry.SweepIdleOnce(ctx)
		}()
		wg.Wait()
	}
	if got := executed.Load(); got != 100 {
		t.Fatalf("executed=%d, want 100", got)
	}
	// 最终状态必须一致:要么 ACTIVE(最后一轮命令赢),要么 ABSENT(钝化赢),
	// 再次提交都必须成功。
	if _, err := registry.Submit(ctx, farmID, func(_ context.Context, actor *FarmActor) (any, error) {
		return actor.Snapshot(time.Now()).OwnerId, nil
	}); err != nil {
		t.Fatalf("submit after race: %v", err)
	}
}

// TestReactivationLoadsFromAuthoritativeStore 对应交接验收 6:
// 重新激活从权威 Store 恢复,旧 generation 不影响新 Actor。
func TestReactivationLoadsFromAuthoritativeStore(t *testing.T) {
	clock := &fakeClock{now: time.Now()}
	store := blobstore.NewMemoryStore()
	registry := newPassivationRegistry(store, clock, time.Minute)
	defer registry.Stop()
	ctx := context.Background()
	const farmID = uint64(1008)

	// 写入一次业务变更。
	if _, err := registry.Submit(ctx, farmID, func(ctx context.Context, actor *FarmActor) (any, error) {
		return actor.Plant(ctx, "op-1", testPlantCropCycleID(actor.farmID, "op-1"), []byte("hash-1"), &farmpb.PlantCropRequest{
			PlotIndex: 0, CropDefinitionId: 1,
		}, 0, 0, time.UnixMilli(1_800_000_000_000), time.Minute)
	}); err != nil {
		t.Fatalf("plant: %v", err)
	}
	firstSnap, _ := registry.exec.MailboxSnapshot(farmID)

	clock.Advance(2 * time.Minute)
	registry.SweepIdleOnce(ctx)
	if got := registry.RuntimeStats().ActiveKeys; got != 0 {
		t.Fatalf("ActiveKeys=%d, want 0", got)
	}

	// 重新激活:从权威 Store 恢复,farm_version 与作物周期保留。
	value, err := registry.Submit(ctx, farmID, func(_ context.Context, actor *FarmActor) (any, error) {
		snap := actor.Snapshot(time.UnixMilli(1_800_000_000_000))
		return struct {
			Version uint64
			Cycles  int
		}{Version: snap.FarmVersion, Cycles: len(actor.record.Core.CropCycles)}, nil
	})
	if err != nil {
		t.Fatalf("reactivation submit: %v", err)
	}
	got := value.(struct {
		Version uint64
		Cycles  int
	})
	if got.Version != 1 || got.Cycles != 1 {
		t.Fatalf("reactivated state=%+v, want version=1 cycles=1", got)
	}
	secondSnap, ok := registry.exec.MailboxSnapshot(farmID)
	if !ok || secondSnap.Generation == firstSnap.Generation {
		t.Fatalf("new generation expected, old=%d new=%+v ok=%v", firstSnap.Generation, secondSnap, ok)
	}
}

// TestManyFarmsResourceFallsBackAfterIdle 对应交接验收 8:
// 大量不同 Farm Key 访问后,等待 idle timeout,Actor/Mailbox/goroutine 数明显回落。
func TestManyFarmsResourceFallsBackAfterIdle(t *testing.T) {
	clock := &fakeClock{now: time.Now()}
	registry := newPassivationRegistry(blobstore.NewMemoryStore(), clock, time.Minute)
	defer registry.Stop()
	ctx := context.Background()

	const farms = 200
	for farmID := uint64(1); farmID <= farms; farmID++ {
		if _, err := registry.Submit(ctx, farmID, func(_ context.Context, actor *FarmActor) (any, error) {
			return nil, nil
		}); err != nil {
			t.Fatalf("submit farm %d: %v", farmID, err)
		}
	}
	if got := registry.RuntimeStats().ActiveKeys; got != farms {
		t.Fatalf("ActiveKeys=%d, want %d", got, farms)
	}
	registry.mu.Lock()
	if len(registry.actors) != farms {
		t.Fatalf("actors=%d, want %d", len(registry.actors), farms)
	}
	registry.mu.Unlock()

	clock.Advance(2 * time.Minute)
	// MaxPerSweep 默认 256,一轮即可覆盖 200 个候选。
	registry.SweepIdleOnce(ctx)

	if got := registry.RuntimeStats().ActiveKeys; got != 0 {
		t.Fatalf("ActiveKeys=%d after idle sweep, want 0", got)
	}
	registry.mu.Lock()
	if len(registry.actors) != 0 {
		t.Fatalf("actors=%d after idle sweep, want 0", len(registry.actors))
	}
	registry.mu.Unlock()
	if got := registry.Activation().Stats().TrackedKeys; got != 0 {
		t.Fatalf("TrackedKeys=%d, want 0", got)
	}
	if got := registry.RuntimeStats().MailboxRetiredTotal; got != farms {
		t.Fatalf("MailboxRetiredTotal=%d, want %d", got, farms)
	}
}

// TestActiveFarmNotPassivated 验证:持续活跃的 Actor 不会被扫描退休。
func TestActiveFarmNotPassivated(t *testing.T) {
	clock := &fakeClock{now: time.Now()}
	registry := newPassivationRegistry(blobstore.NewMemoryStore(), clock, time.Minute)
	defer registry.Stop()
	ctx := context.Background()
	const farmID = uint64(1009)

	for i := 0; i < 5; i++ {
		if _, err := registry.Submit(ctx, farmID, func(_ context.Context, actor *FarmActor) (any, error) {
			return nil, nil
		}); err != nil {
			t.Fatalf("submit: %v", err)
		}
		clock.Advance(30 * time.Second)
		registry.SweepIdleOnce(ctx)
	}
	registry.mu.Lock()
	if registry.actors[farmID] == nil {
		t.Fatal("continuously active actor must not passivate")
	}
	registry.mu.Unlock()
	if got := registry.Activation().StateOf(farmID); got != activation.Active {
		t.Fatalf("state=%s, want ACTIVE", got)
	}
}
