package actor

import (
	"context"
	"sync"
	"testing"
	"time"

	playerpb "farmgame/api/gen/player"
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

// TestPlayerMailboxRetiredAfterIdleAndNextCommandWorks 对应交接验收 7:
// Player Mailbox idle 回收后再次执行正确(新 generation、重新加载权威记录、不丢任务)。
func TestPlayerMailboxRetiredAfterIdleAndNextCommandWorks(t *testing.T) {
	clock := &fakeClock{now: time.Now()}
	registry := NewRegistryWithOptions(blobstore.NewMemoryStore(), RegistryOptions{
		MailboxSize: 8,
		Idle: IdleOptions{
			IdleTimeout: time.Minute,
			BucketWidth: 10 * time.Second,
			Now:         clock.Now,
		},
	})
	defer registry.Stop()
	ctx := context.Background()
	const playerID = uint64(20001)

	// 建立账号并买入种子(产生权威记录)。
	if _, _, err := registry.Ensure(ctx, playerID, "bootstrap-1"); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if _, err := registry.Submit(ctx, playerID, func(rec *playerpb.PlayerRecord) (any, bool, error) {
		return nil, false, nil
	}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if got := registry.RuntimeStats().ActiveKeys; got != 1 {
		t.Fatalf("ActiveKeys=%d, want 1", got)
	}
	firstSnap, _ := registry.exec.MailboxSnapshot(playerID)

	clock.Advance(2 * time.Minute)
	if got := registry.SweepIdleOnce(ctx); got != 1 {
		t.Fatalf("swept=%d, want 1", got)
	}
	if got := registry.RuntimeStats().ActiveKeys; got != 0 {
		t.Fatalf("ActiveKeys=%d, want 0 (mailbox retired)", got)
	}
	if got := registry.RuntimeStats().MailboxRetiredTotal; got != 1 {
		t.Fatalf("MailboxRetiredTotal=%d, want 1", got)
	}

	// 回收后下一条命令:新 generation,权威记录完整。
	if _, err := registry.Submit(ctx, playerID, func(rec *playerpb.PlayerRecord) (any, bool, error) {
		if rec.Wallet.Coin != DefaultInitialCoin {
			t.Fatalf("coin=%d, want %d (权威记录未正确重载)", rec.Wallet.Coin, DefaultInitialCoin)
		}
		return nil, false, nil
	}); err != nil {
		t.Fatalf("submit after retire: %v", err)
	}
	secondSnap, ok := registry.exec.MailboxSnapshot(playerID)
	if !ok || secondSnap.Generation == firstSnap.Generation {
		t.Fatalf("expected new generation, old=%d new=%+v ok=%v", firstSnap.Generation, secondSnap, ok)
	}
}

// TestManyPlayersMailboxFallsBackAfterIdle 验证:大量 Player Key 后 mailbox 数回落。
func TestManyPlayersMailboxFallsBackAfterIdle(t *testing.T) {
	clock := &fakeClock{now: time.Now()}
	registry := NewRegistryWithOptions(blobstore.NewMemoryStore(), RegistryOptions{
		MailboxSize: 8,
		Idle: IdleOptions{
			IdleTimeout: time.Minute,
			BucketWidth: 10 * time.Second,
			MaxPerSweep: 512,
			Now:         clock.Now,
		},
	})
	defer registry.Stop()
	ctx := context.Background()

	const players = 300
	for playerID := uint64(1); playerID <= players; playerID++ {
		if _, _, err := registry.Ensure(ctx, playerID, "boot"); err != nil {
			t.Fatalf("Ensure %d: %v", playerID, err)
		}
	}
	if got := registry.RuntimeStats().ActiveKeys; got != players {
		t.Fatalf("ActiveKeys=%d, want %d", got, players)
	}
	clock.Advance(2 * time.Minute)
	registry.SweepIdleOnce(ctx)
	if got := registry.RuntimeStats().ActiveKeys; got != 0 {
		t.Fatalf("ActiveKeys=%d after sweep, want 0", got)
	}
	if got := registry.RuntimeStats().MailboxRetiredTotal; got != players {
		t.Fatalf("MailboxRetiredTotal=%d, want %d", got, players)
	}
}

// TestBusyPlayerMailboxNotRetired 验证:在途任务阻止 mailbox 回收。
func TestBusyPlayerMailboxNotRetired(t *testing.T) {
	clock := &fakeClock{now: time.Now()}
	registry := NewRegistryWithOptions(blobstore.NewMemoryStore(), RegistryOptions{
		MailboxSize: 8,
		Idle: IdleOptions{
			IdleTimeout: time.Minute,
			BucketWidth: 10 * time.Second,
			Now:         clock.Now,
		},
	})
	defer registry.Stop()
	ctx := context.Background()
	const playerID = uint64(20002)

	// Player Submit 每 Turn 加载权威记录;记录不存在时 load 失败,turn 不会执行。
	if _, _, err := registry.Ensure(ctx, playerID, "boot"); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	block := make(chan struct{})
	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		_, _ = registry.Submit(ctx, playerID, func(rec *playerpb.PlayerRecord) (any, bool, error) {
			close(started)
			<-block
			return nil, false, nil
		})
		close(done)
	}()
	<-started

	clock.Advance(2 * time.Minute)
	deadline := time.Now().Add(2 * time.Second)
	for {
		registry.SweepIdleOnce(ctx)
		if registry.IdleSweeperStats().BlockedTotal["turn_running"] > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("retire was not blocked by running turn")
		}
		time.Sleep(time.Millisecond)
	}
	if got := registry.RuntimeStats().ActiveKeys; got != 1 {
		t.Fatalf("ActiveKeys=%d, want 1 while turn running", got)
	}
	close(block)
	<-done
}
