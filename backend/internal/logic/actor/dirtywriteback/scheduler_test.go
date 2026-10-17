package dirtywriteback

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testOptions() Options {
	return Options{
		FlushInterval: time.Hour,
		MaxDirtyAge:   2 * time.Hour,
		RetryDelay:    time.Millisecond,
		FlushTimeout:  time.Second,
		Workers:       1,
		QueueSize:     8,
		Now:           time.Now,
	}
}

func TestMarkCoalescesOneActor(t *testing.T) {
	s, err := New(testOptions(), func(context.Context, uint64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(context.Background()) }()
	for i := 0; i < 10; i++ {
		if err := s.Mark(7); err != nil {
			t.Fatal(err)
		}
	}
	got := s.Snapshot()
	if got.Marks != 10 || got.CoalescedMarks != 9 || got.QueueDepth != 1 || got.MaxQueueDepth != 1 {
		t.Fatalf("snapshot=%+v", got)
	}
}

func TestFlushNowRemovesQueuedEntry(t *testing.T) {
	var mu sync.Mutex
	var ids []uint64
	s, err := New(testOptions(), func(_ context.Context, id uint64) error {
		mu.Lock()
		ids = append(ids, id)
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Mark(9); err != nil {
		t.Fatal(err)
	}
	if err := s.FlushNow(context.Background(), 9); err != nil {
		t.Fatal(err)
	}
	if got := s.Snapshot(); got.QueueDepth != 0 || got.FlushSucceeded != 1 {
		t.Fatalf("snapshot=%+v", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(ids) != 1 || ids[0] != 9 {
		t.Fatalf("ids=%v", ids)
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestFlushNowWaitsForInflightThenFlushesRemarkedGeneration(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var attempts atomic.Int32
	s, err := New(testOptions(), func(context.Context, uint64) error {
		if attempts.Add(1) == 1 {
			close(started)
			<-release
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(context.Background()) }()
	if err := s.Mark(10); err != nil {
		t.Fatal(err)
	}
	firstDone := make(chan error, 1)
	go func() { firstDone <- s.FlushNow(context.Background(), 10) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first immediate flush did not start")
	}
	if err := s.Mark(10); err != nil {
		t.Fatal(err)
	}
	secondDone := make(chan error, 1)
	go func() { secondDone <- s.FlushNow(context.Background(), 10) }()
	select {
	case err := <-secondDone:
		t.Fatalf("second FlushNow returned before in-flight completion: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("flush attempts=%d want=2", got)
	}
}

func TestFailedBackgroundFlushIsRequeued(t *testing.T) {
	opts := testOptions()
	opts.FlushInterval = time.Millisecond
	opts.MaxDirtyAge = 10 * time.Millisecond
	called := make(chan struct{}, 2)
	var attempts int
	var mu sync.Mutex
	s, err := New(opts, func(context.Context, uint64) error {
		mu.Lock()
		defer mu.Unlock()
		attempts++
		called <- struct{}{}
		if attempts == 1 {
			return errors.New("temporary")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(context.Background()) }()
	if err := s.Mark(11); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		select {
		case <-called:
		case <-time.After(time.Second):
			t.Fatal("flush retry timeout")
		}
	}
	deadline := time.Now().Add(time.Second)
	for s.Snapshot().FlushSucceeded != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	got := s.Snapshot()
	if got.FlushAttempts != 2 || got.FlushFailed != 1 || got.FlushSucceeded != 1 {
		t.Fatalf("snapshot=%+v", got)
	}
}

func TestCloseFlushesPendingAndRejectsNewMarks(t *testing.T) {
	var mu sync.Mutex
	seen := map[uint64]bool{}
	s, err := New(testOptions(), func(_ context.Context, id uint64) error {
		mu.Lock()
		seen[id] = true
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Mark(21); err != nil {
		t.Fatal(err)
	}
	if err := s.Mark(22); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Close(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	if !seen[21] || !seen[22] {
		t.Fatalf("seen=%v", seen)
	}
	mu.Unlock()
	if err := s.Mark(23); !errors.Is(err, ErrClosed) {
		t.Fatalf("err=%v", err)
	}
}

func TestInvalidActorID(t *testing.T) {
	s, err := New(testOptions(), func(context.Context, uint64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(context.Background()) }()
	if err := s.Mark(0); !errors.Is(err, ErrInvalidActorID) {
		t.Fatalf("err=%v", err)
	}
}

func TestMarkDuringInflightKeepsActorTrackedForSecondFlush(t *testing.T) {
	opts := testOptions()
	opts.FlushInterval = time.Millisecond
	opts.MaxDirtyAge = 10 * time.Millisecond
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{}, 2)
	var attempts int
	var mu sync.Mutex
	s, err := New(opts, func(context.Context, uint64) error {
		mu.Lock()
		attempts++
		current := attempts
		mu.Unlock()
		if current == 1 {
			close(started)
			<-release
		}
		done <- struct{}{}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(context.Background()) }()
	if err := s.Mark(31); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first flush did not start")
	}
	if err := s.Mark(31); err != nil {
		t.Fatal(err)
	}
	close(release)
	for i := 0; i < 2; i++ {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("second flush was lost")
		}
	}
	deadline := time.Now().Add(time.Second)
	for {
		snapshot := s.Snapshot()
		if snapshot.QueueDepth == 0 && snapshot.Inflight == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("in-flight completion was not installed: %+v", snapshot)
		}
		time.Sleep(time.Millisecond)
	}
	if got := s.Snapshot(); got.FlushSucceeded != 2 || got.QueueDepth != 0 {
		t.Fatalf("snapshot=%+v", got)
	}
}

func TestQueueSizeBoundsUniqueActors(t *testing.T) {
	opts := testOptions()
	opts.QueueSize = 1
	s, err := New(opts, func(context.Context, uint64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(context.Background()) }()
	if err := s.Mark(41); err != nil {
		t.Fatal(err)
	}
	if err := s.Mark(42); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("err=%v", err)
	}
	if got := s.Snapshot(); got.QueueDepth != 1 || got.MaxQueueDepth != 1 {
		t.Fatalf("snapshot=%+v", got)
	}
}

func TestClosePreservesFailedFinalEntry(t *testing.T) {
	s, err := New(testOptions(), func(context.Context, uint64) error {
		return errors.New("redis unavailable")
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Mark(51); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Close(ctx); err == nil {
		t.Fatal("expected final flush error")
	}
	if got := s.Snapshot(); got.QueueDepth != 1 || got.FlushFailed != 1 {
		t.Fatalf("failed entry was discarded: %+v", got)
	}
}
