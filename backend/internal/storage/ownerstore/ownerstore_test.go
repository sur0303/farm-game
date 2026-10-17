package ownerstore

import (
	"context"
	"errors"
	"sync"
	"testing"

	ownercorepb "farmgame/api/gen/ownercore"
)

func TestEnsureOwnerInitializedCreatesOnce(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	created, err := EnsureOwnerInitialized(ctx, s, 1001, "boot-1", 1)
	if err != nil || !created {
		t.Fatalf("first ensure: created=%v err=%v", created, err)
	}
	// 重复调用不应重复创建。
	created2, err := EnsureOwnerInitialized(ctx, s, 1001, "boot-1", 1)
	if err != nil || created2 {
		t.Fatalf("second ensure should be reused: created=%v err=%v", created2, err)
	}
	rec, ver, err := s.Get(ctx, 1001)
	if err != nil || rec == nil || ver != 1 {
		t.Fatalf("get after ensure: ver=%d err=%v", ver, err)
	}
	if rec.OwnerMeta.BootstrapId != "boot-1" {
		t.Fatalf("bootstrap_id not persisted: %q", rec.OwnerMeta.BootstrapId)
	}
}

func TestEnsureOwnerBootstrapConflict(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	if _, err := EnsureOwnerInitialized(ctx, s, 1001, "boot-1", 1); err != nil {
		t.Fatalf("first: %v", err)
	}
	if _, err := EnsureOwnerInitialized(ctx, s, 1001, "boot-2", 1); !errors.Is(err, ErrBootstrapConflict) {
		t.Fatalf("conflicting bootstrap want ErrBootstrapConflict, got %v", err)
	}
}

func TestConcurrentEnsureOnlyOneCreates(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	const n = 100
	var wg sync.WaitGroup
	createdFlags := make([]bool, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, err := EnsureOwnerInitialized(ctx, s, 7777, "boot-stable", 1)
			if err != nil {
				t.Errorf("ensure err: %v", err)
				return
			}
			createdFlags[i] = c
		}(i)
	}
	wg.Wait()
	total := 0
	for _, c := range createdFlags {
		if c {
			total++
		}
	}
	if total != 1 {
		t.Fatalf("exactly one concurrent ensure should create; got %d", total)
	}
	if s.Len() != 1 {
		t.Fatalf("store should hold exactly 1 owner, got %d", s.Len())
	}
}

type bootstrapFollowupErrorStore struct {
	firstGet bool
}

func (s *bootstrapFollowupErrorStore) Get(context.Context, uint64) (*ownercorepb.OwnerCoreRecord, uint64, error) {
	if !s.firstGet {
		s.firstGet = true
		return nil, 0, ErrOwnerNotFound
	}
	return nil, 0, ErrTimeoutUnknown
}

func (*bootstrapFollowupErrorStore) Insert(context.Context, *ownercorepb.OwnerCoreRecord) error {
	return ErrOwnerExists
}

func (*bootstrapFollowupErrorStore) CAS(context.Context, *ownercorepb.OwnerCoreRecord, uint64) error {
	return ErrUnavailable
}

func TestEnsureOwnerInitializedDoesNotHideFollowupReadFailure(t *testing.T) {
	s := &bootstrapFollowupErrorStore{}
	created, err := EnsureOwnerInitialized(context.Background(), s, 55, "boot", 1)
	if created || !errors.Is(err, ErrTimeoutUnknown) {
		t.Fatalf("follow-up read failure must be returned, created=%v err=%v", created, err)
	}
}

func TestCASConflict(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	if _, err := EnsureOwnerInitialized(ctx, s, 1001, "boot-1", 1); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	rec, ver, _ := s.Get(ctx, 1001)
	// 修改后 CAS 成功。
	rec.Wallet.Coin += 10
	if err := s.CAS(ctx, rec, ver); err != nil {
		t.Fatalf("cas success expected: %v", err)
	}
	// 用旧版本再 CAS 应冲突。
	stale := Clone(rec)
	stale.Wallet.Coin += 10
	if err := s.CAS(ctx, stale, ver); !errors.Is(err, ErrCASConflict) {
		t.Fatalf("stale cas want ErrCASConflict, got %v", err)
	}
}

func TestCompareAndSwapRejectsStaleOwnerEpoch(t *testing.T) {
	s := NewMemoryStore()
	if _, err := EnsureOwnerInitialized(context.Background(), s, 1002, "boot", 1); err != nil {
		t.Fatal(err)
	}
	rec, version, err := s.Get(context.Background(), 1002)
	if err != nil {
		t.Fatal(err)
	}
	rec.Wallet.Coin++
	if err := s.CompareAndSwap(context.Background(), 1002, version, rec.OwnerMeta.OwnerEpoch+1, rec); !errors.Is(err, ErrOwnerEpochStale) {
		t.Fatalf("stale owner epoch want ErrOwnerEpochStale, got %v", err)
	}
}

type retryStore struct {
	base       *MemoryStore
	conflicts  int
	unknownCAS bool
}

func (s *retryStore) Get(ctx context.Context, ownerID uint64) (*ownercorepb.OwnerCoreRecord, uint64, error) {
	return s.base.Get(ctx, ownerID)
}

func (s *retryStore) Insert(ctx context.Context, rec *ownercorepb.OwnerCoreRecord) error {
	return s.base.Insert(ctx, rec)
}

func (s *retryStore) CAS(ctx context.Context, rec *ownercorepb.OwnerCoreRecord, version uint64) error {
	if s.conflicts > 0 {
		s.conflicts--
		return ErrCASConflict
	}
	if s.unknownCAS {
		s.unknownCAS = false
		if err := s.base.CAS(ctx, rec, version); err != nil {
			return err
		}
		return ErrTimeoutUnknown
	}
	return s.base.CAS(ctx, rec, version)
}

func TestCompareAndSwapWithRetryConflictThenSuccess(t *testing.T) {
	s := &retryStore{base: NewMemoryStore(), conflicts: 1}
	if _, err := EnsureOwnerInitialized(context.Background(), s, 901, "boot", 1); err != nil {
		t.Fatal(err)
	}
	rec, version, err := CompareAndSwapWithRetry(context.Background(), s, 901, "op-1", 3,
		func(current *ownercorepb.OwnerCoreRecord, _ uint64) (*ownercorepb.OwnerCoreRecord, error) {
			current.Wallet.Coin++
			return current, nil
		}, nil)
	if err != nil || rec.Wallet.Coin != DefaultInitialCoin+1 || version != 2 {
		t.Fatalf("retry result rec=%v version=%d err=%v", rec, version, err)
	}
}

func TestCompareAndSwapWithRetryStopsAfterThreeConflicts(t *testing.T) {
	s := &retryStore{base: NewMemoryStore(), conflicts: 3}
	if _, err := EnsureOwnerInitialized(context.Background(), s, 902, "boot", 1); err != nil {
		t.Fatal(err)
	}
	_, _, err := CompareAndSwapWithRetry(context.Background(), s, 902, "op-2", 3,
		func(current *ownercorepb.OwnerCoreRecord, _ uint64) (*ownercorepb.OwnerCoreRecord, error) {
			return current, nil
		}, nil)
	if !errors.Is(err, ErrUnavailable) || !errors.Is(err, ErrCASConflict) {
		t.Fatalf("three conflicts should become unavailable, got %v", err)
	}
}

func TestCompareAndSwapWithRetryResolvesUnknownByAuthoritativeRead(t *testing.T) {
	s := &retryStore{base: NewMemoryStore(), unknownCAS: true}
	if _, err := EnsureOwnerInitialized(context.Background(), s, 903, "boot", 1); err != nil {
		t.Fatal(err)
	}
	called := false
	rec, version, err := CompareAndSwapWithRetry(context.Background(), s, 903, "op-3", 3,
		func(current *ownercorepb.OwnerCoreRecord, _ uint64) (*ownercorepb.OwnerCoreRecord, error) {
			current.Wallet.Coin += 7
			return current, nil
		}, func(ctx context.Context, operationID string) (bool, error) {
			called = operationID == "op-3"
			got, _, getErr := s.Get(ctx, 903)
			return getErr == nil && got.Wallet.Coin == DefaultInitialCoin+7, getErr
		})
	if err != nil || !called || rec.Wallet.Coin != DefaultInitialCoin+7 || version != 2 {
		t.Fatalf("unknown resolution rec=%v version=%d called=%v err=%v", rec, version, called, err)
	}
}

func TestGetMissing(t *testing.T) {
	s := NewMemoryStore()
	if _, _, err := s.Get(context.Background(), 999); !errors.Is(err, ErrOwnerNotFound) {
		t.Fatalf("want ErrOwnerNotFound, got %v", err)
	}
}

func TestClassifyTcaplusErrors(t *testing.T) {
	tests := []struct {
		name string
		msg  string
		want error
	}{
		{"missing", "record not found", ErrOwnerNotFound},
		{"sdk missing", "errCode: 261, errMsg: txhdb_record_not_exist", ErrOwnerNotFound},
		{"exists", "already exists", ErrOwnerExists},
		{"version", "old record version", ErrCASConflict},
		{"timeout", "request timeout", ErrTimeoutUnknown},
		{"other", "connection reset", ErrUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := classifyTcaplus(errors.New(tt.msg)); !errors.Is(err, tt.want) {
				t.Fatalf("classify(%q)=%v, want %v", tt.msg, err, tt.want)
			}
		})
	}
}

func TestNewTcaplusStoreRequiresClientAndZone(t *testing.T) {
	if _, err := NewTcaplusStore(nil, 1); err == nil {
		t.Fatal("nil Tcaplus client should be rejected")
	}
	if _, err := NewTcaplusStore(nil, 0); err == nil {
		t.Fatal("zero Tcaplus zone should be rejected")
	}
}

func TestInitialRecordShape(t *testing.T) {
	rec := BuildInitialRecord(123, "boot", 2)
	if rec.OwnerMeta.AppliedBootstrapRuleVersion != 2 {
		t.Fatalf("rule version not set")
	}
	if len(rec.Farm.Plots) != DefaultPlotCount {
		t.Fatalf("plots=%d want %d", len(rec.Farm.Plots), DefaultPlotCount)
	}
	if rec.Wallet.Coin != DefaultInitialCoin {
		t.Fatalf("coin=%d", rec.Wallet.Coin)
	}
	_ = ownercorepb.OwnerCoreRecord{}
}
