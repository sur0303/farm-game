package actor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	playerpb "farmgame/api/gen/player"
	actorruntime "farmgame/internal/logic/actor/runtime"
	"farmgame/internal/storage/blobstore"
)

// TestPlayerSystemPathProgressesWhenUserMailboxFull 验收 Player 已接入 SystemQueue:
// UserMailbox 满时 NotifyLeaseLost 仍可入队并执行。
func TestPlayerSystemPathProgressesWhenUserMailboxFull(t *testing.T) {
	registry := NewRegistryWithOptions(blobstore.NewMemoryStore(), RegistryOptions{MailboxSize: 1})
	defer registry.Stop()
	ctx := context.Background()
	const playerID = uint64(901)

	if _, _, err := registry.Ensure(ctx, playerID, "boot"); err != nil {
		t.Fatal(err)
	}

	release := make(chan struct{})
	started := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = registry.Submit(ctx, playerID, func(_ *playerpb.PlayerRecord) (any, bool, error) {
			close(started)
			<-release
			return nil, false, nil
		})
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("blocking user turn did not start")
	}

	queuedDone := make(chan error, 1)
	go func() {
		_, err := registry.Submit(ctx, playerID, func(_ *playerpb.PlayerRecord) (any, bool, error) {
			return nil, false, nil
		})
		queuedDone <- err
	}()
	time.Sleep(30 * time.Millisecond)

	_, err := registry.Submit(ctx, playerID, func(_ *playerpb.PlayerRecord) (any, bool, error) {
		return "nope", false, nil
	})
	if !errors.Is(err, actorruntime.ErrMailboxFull) && !errors.Is(err, actorruntime.ErrActorBusy) {
		t.Fatalf("want user mailbox full, got %v", err)
	}

	leaseDone := make(chan error, 1)
	go func() {
		leaseDone <- registry.NotifyLeaseLost(ctx, playerID)
	}()
	time.Sleep(30 * time.Millisecond)
	select {
	case err := <-leaseDone:
		t.Fatalf("NotifyLeaseLost returned before release: %v", err)
	default:
	}

	_, err = registry.Submit(ctx, playerID, func(_ *playerpb.PlayerRecord) (any, bool, error) {
		return nil, false, nil
	})
	if !errors.Is(err, actorruntime.ErrMailboxFull) && !errors.Is(err, actorruntime.ErrActorBusy) {
		t.Fatalf("want user still full, got %v", err)
	}

	close(release)
	wg.Wait()

	select {
	case err := <-leaseDone:
		if err != nil {
			t.Fatalf("NotifyLeaseLost: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("NotifyLeaseLost did not complete — Player SystemQueue not wired?")
	}
	if registry.LeaseLostTotal() < 1 {
		t.Fatal("LeaseLost system turn did not run")
	}
	if err := <-queuedDone; err != nil {
		t.Fatalf("queued user: %v", err)
	}
}
