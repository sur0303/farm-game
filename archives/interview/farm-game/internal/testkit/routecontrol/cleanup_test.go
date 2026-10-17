package routecontrol

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeExactKeyCleanup struct {
	mu          sync.Mutex
	present     bool
	retain      bool
	deleteCalls int
	existsCalls int
	deleteErr   error
}

type blockingExactKeyCleanup struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
	mu      sync.Mutex
	present bool
}

func (f *blockingExactKeyCleanup) DeleteExact(context.Context) error {
	f.once.Do(func() { close(f.started) })
	<-f.release
	f.mu.Lock()
	f.present = false
	f.mu.Unlock()
	return nil
}

func (f *blockingExactKeyCleanup) ExistsExact(context.Context) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.present, nil
}

func (f *fakeExactKeyCleanup) DeleteExact(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCalls++
	if f.deleteErr == nil {
		if !f.retain {
			f.present = false
		}
	}
	return f.deleteErr
}

func (f *fakeExactKeyCleanup) ExistsExact(context.Context) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.existsCalls++
	return f.present, nil
}

func TestExactKeyRegistryDeletesAndVerifiesEachRegisteredKey(t *testing.T) {
	first := &fakeExactKeyCleanup{present: true}
	second := &fakeExactKeyCleanup{present: true}
	firstHandle, err := NewKeyHandle(KeyDigest("raw-first-key"), first)
	if err != nil {
		t.Fatal(err)
	}
	secondHandle, err := NewKeyHandle(KeyDigest("raw-second-key"), second)
	if err != nil {
		t.Fatal(err)
	}
	registry := NewExactKeyRegistry()
	if err := registry.Register(secondHandle); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(firstHandle); err != nil {
		t.Fatal(err)
	}
	digests := registry.Digests()
	if len(digests) != 2 || digests[0] >= digests[1] {
		t.Fatalf("digests are not sorted: %v", digests)
	}
	for _, digest := range digests {
		if strings.Contains(strings.Join(digests, ","), "raw-") {
			t.Fatal("raw key leaked into registry identifiers")
		}
		if len(digest) != 64 {
			t.Fatalf("digest=%q is not SHA-256", digest)
		}
	}
	if err := registry.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	for name, cleanup := range map[string]*fakeExactKeyCleanup{"first": first, "second": second} {
		cleanup.mu.Lock()
		deleteCalls, existsCalls, present := cleanup.deleteCalls, cleanup.existsCalls, cleanup.present
		cleanup.mu.Unlock()
		if deleteCalls != 1 || existsCalls != 1 || present {
			t.Fatalf("%s cleanup delete=%d exists=%d present=%v", name, deleteCalls, existsCalls, present)
		}
	}
}

func TestExactKeyRegistryRejectsResidueAndStillChecksExistsAfterDeleteError(t *testing.T) {
	residue := &fakeExactKeyCleanup{present: true, retain: true}
	handle, err := NewKeyHandle(strings.Repeat("f", 64), residue)
	if err != nil {
		t.Fatal(err)
	}
	registry := NewExactKeyRegistry()
	if err := registry.Register(handle); err != nil {
		t.Fatal(err)
	}
	if err := registry.Cleanup(context.Background()); !errors.Is(err, ErrKeyResidue) {
		t.Fatalf("cleanup error=%v, want residue", err)
	}
	residue.mu.Lock()
	deleteCalls, existsCalls := residue.deleteCalls, residue.existsCalls
	residue.mu.Unlock()
	if deleteCalls != 1 || existsCalls != 1 {
		t.Fatalf("delete=%d exists=%d, want one each", deleteCalls, existsCalls)
	}

	failedDelete := &fakeExactKeyCleanup{present: true, deleteErr: errors.New("delete failed")}
	handle, err = NewKeyHandle(strings.Repeat("e", 64), failedDelete)
	if err != nil {
		t.Fatal(err)
	}
	registry = NewExactKeyRegistry()
	if err := registry.Register(handle); err != nil {
		t.Fatal(err)
	}
	err = registry.Cleanup(context.Background())
	if err == nil || !strings.Contains(err.Error(), "DEL") {
		t.Fatalf("delete failure was not reported: %v", err)
	}
	failedDelete.mu.Lock()
	existsCalls = failedDelete.existsCalls
	failedDelete.mu.Unlock()
	if existsCalls != 1 {
		t.Fatal("EXISTS was skipped after DEL failure")
	}
}

func TestExactKeyRegistryRejectsDuplicateDigest(t *testing.T) {
	first, err := NewKeyHandle(strings.Repeat("1", 64), &fakeExactKeyCleanup{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewKeyHandle(strings.Repeat("1", 64), &fakeExactKeyCleanup{})
	if err != nil {
		t.Fatal(err)
	}
	registry := NewExactKeyRegistry()
	if err := registry.Register(first); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(second); err == nil {
		t.Fatal("duplicate digest accepted")
	}
}

func TestExactKeyRegistryRejectsRegistrationDuringCleanupAndClosesAfterSuccess(t *testing.T) {
	blocking := &blockingExactKeyCleanup{
		started: make(chan struct{}),
		release: make(chan struct{}),
		present: true,
	}
	handle, err := NewKeyHandle(strings.Repeat("2", 64), blocking)
	if err != nil {
		t.Fatal(err)
	}
	registry := NewExactKeyRegistry()
	if err := registry.Register(handle); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- registry.Cleanup(context.Background()) }()
	select {
	case <-blocking.started:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not enter adapter")
	}

	other, err := NewKeyHandle(strings.Repeat("3", 64), &fakeExactKeyCleanup{})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(other); !errors.Is(err, ErrCleanupInProgress) {
		t.Fatalf("concurrent Register error=%v, want ErrCleanupInProgress", err)
	}
	if err := registry.Cleanup(context.Background()); !errors.Is(err, ErrCleanupInProgress) {
		t.Fatalf("concurrent Cleanup error=%v, want ErrCleanupInProgress", err)
	}
	close(blocking.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(other); !errors.Is(err, ErrExactKeyRegistryClosed) {
		t.Fatalf("post-cleanup Register error=%v, want ErrExactKeyRegistryClosed", err)
	}
	if err := registry.Cleanup(context.Background()); err != nil {
		t.Fatalf("idempotent closed Cleanup: %v", err)
	}
}

func TestExactKeyRegistryFailedCleanupCanBeRetried(t *testing.T) {
	residue := &fakeExactKeyCleanup{present: true, retain: true}
	handle, err := NewKeyHandle(strings.Repeat("4", 64), residue)
	if err != nil {
		t.Fatal(err)
	}
	registry := NewExactKeyRegistry()
	if err := registry.Register(handle); err != nil {
		t.Fatal(err)
	}
	if err := registry.Cleanup(context.Background()); !errors.Is(err, ErrKeyResidue) {
		t.Fatalf("first cleanup error=%v, want residue", err)
	}
	residue.mu.Lock()
	residue.retain = false
	residue.mu.Unlock()
	if err := registry.Cleanup(context.Background()); err != nil {
		t.Fatalf("retry cleanup failed: %v", err)
	}
}
