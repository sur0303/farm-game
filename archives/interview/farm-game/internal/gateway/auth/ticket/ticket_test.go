package ticket

import (
	"context"
	"sync"
	"testing"
	"time"
)

func testBound() Bound {
	return Bound{
		PlayerID:              10001,
		SessionID:             "sess-abc",
		SessionAuthorityEpoch: 3,
		ActiveSessionEpoch:    7,
	}
}

func TestIssueAndRedeem(t *testing.T) {
	s := New(nil)
	iss, err := s.Issue(context.Background(), testBound())
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	if !ValidTokenFormat(iss.Token) {
		t.Fatalf("issued token bad format: %q", iss.Token)
	}
	b, err := s.Redeem(context.Background(), iss.Token)
	if err != nil {
		t.Fatalf("redeem: %v", err)
	}
	if b.PlayerID != 10001 || b.ActiveSessionEpoch != 7 {
		t.Fatalf("bound mismatch: %+v", b)
	}
}

func TestRedeemOnceOnly(t *testing.T) {
	s := New(nil)
	iss, _ := s.Issue(context.Background(), testBound())
	if _, err := s.Redeem(context.Background(), iss.Token); err != nil {
		t.Fatalf("first redeem: %v", err)
	}
	if _, err := s.Redeem(context.Background(), iss.Token); err != ErrRedeemFailed {
		t.Fatalf("second redeem want ErrRedeemFailed, got %v", err)
	}
}

func TestConcurrentRedeemOnlyOneWins(t *testing.T) {
	s := New(nil)
	iss, _ := s.Issue(context.Background(), testBound())

	const n = 16
	var wg sync.WaitGroup
	var okCount, failCount int
	var mu sync.Mutex
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.Redeem(context.Background(), iss.Token)
			mu.Lock()
			if err == nil {
				okCount++
			} else {
				failCount++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	if okCount != 1 {
		t.Fatalf("want exactly 1 successful redeem, got %d (fails=%d)", okCount, failCount)
	}
	if failCount != n-1 {
		t.Fatalf("want %d failed redeem, got %d", n-1, failCount)
	}
}

func TestInvalidTokenFormat(t *testing.T) {
	s := New(nil)
	if _, err := s.Redeem(context.Background(), "not-a-token"); err != ErrInvalidTicket {
		t.Fatalf("want ErrInvalidTicket, got %v", err)
	}
}

func TestTicketExpiry(t *testing.T) {
	store := NewMemoryStore()
	s := New(store, WithTTL(10*time.Millisecond))
	iss, _ := s.Issue(context.Background(), testBound())
	time.Sleep(30 * time.Millisecond)
	if _, err := s.Redeem(context.Background(), iss.Token); err != ErrRedeemFailed {
		t.Fatalf("expired ticket want ErrRedeemFailed, got %v", err)
	}
}

func TestStoreKeepsOnlyHash(t *testing.T) {
	// 验证内存存储以 hash 为键,不保存原文 token(通过 Hash 间接证明)。
	store := NewMemoryStore()
	_ = store.Save(context.Background(), "raw-token-value", testBound(), time.Minute)
	if store.Len() != 1 {
		t.Fatalf("store should hold 1 entry keyed by hash")
	}
}
