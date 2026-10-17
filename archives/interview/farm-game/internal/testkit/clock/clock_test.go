package clock

import (
	"sync"
	"testing"
	"time"
)

func TestClockAdvanceAndSet(t *testing.T) {
	start := time.UnixMilli(1_900_000_000_000)
	c := New(start)
	if !c.Now().Equal(start) {
		t.Fatalf("Now=%v want %v", c.Now(), start)
	}
	if got := c.Advance(3 * time.Second); !got.Equal(start.Add(3 * time.Second)) {
		t.Fatalf("Advance=%v", got)
	}
	if got := c.Advance(0); !got.Equal(start.Add(3 * time.Second)) {
		t.Fatalf("Advance(0) changed clock to %v", got)
	}
	later := start.Add(time.Hour)
	c.Set(later)
	if !c.Now().Equal(later) {
		t.Fatalf("Set failed, Now=%v", c.Now())
	}
}

func TestClockDeadlineAndExpired(t *testing.T) {
	start := time.UnixMilli(1_900_000_000_000)
	c := New(start)
	deadline := c.Deadline(10 * time.Second)
	if c.Expired(deadline) {
		t.Fatal("deadline should not be expired at start")
	}
	c.Advance(10 * time.Second)
	if !c.Expired(deadline) {
		t.Fatal("deadline at exactly now must count as expired (fail-closed boundary)")
	}
}

func TestClockNowFuncAdaptsSetClockSeam(t *testing.T) {
	c := New(time.UnixMilli(1_900_000_000_000))
	nowFn := c.NowFunc()
	before := nowFn()
	c.Advance(time.Second)
	if !nowFn().After(before) {
		t.Fatal("NowFunc closure did not observe Advance")
	}
}

func TestClockConcurrentAccess(t *testing.T) {
	c := New(time.UnixMilli(1_900_000_000_000))
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				c.Advance(time.Millisecond)
				_ = c.Now()
			}
		}()
	}
	wg.Wait()
	if got, want := c.Now(), time.UnixMilli(1_900_000_000_000).Add(800*time.Millisecond); !got.Equal(want) {
		t.Fatalf("Now=%v want %v", got, want)
	}
}
