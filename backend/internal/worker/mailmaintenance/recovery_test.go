package mailmaintenance_test

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"farmgame/internal/player/mail"
	redisplayermail "farmgame/internal/storage/redis/playermail"
	"farmgame/internal/worker/mailmaintenance"
)

type fakeRepository struct {
	mu sync.Mutex

	waitLease    bool
	held         map[uint32]bool
	leases       map[uint32]int
	leaseCalls   []uint32
	activeLeases int
	peakLeases   int
	expire       map[uint32][]mail.DueCandidate
	gc           map[uint32][]mail.DueCandidate
	receipts     map[uint32][]redisplayermail.ReceiptDueCandidate
	expireStatus mail.ExpireStatus
	registered   []mail.MailDueRefV1
}

func (f *fakeRepository) ClaimScanLease(ctx context.Context, bucketID uint32, _ string, _ int64) (mail.ScanLeaseResult, error) {
	f.mu.Lock()
	f.leases[bucketID]++
	f.leaseCalls = append(f.leaseCalls, bucketID)
	f.activeLeases++
	if f.activeLeases > f.peakLeases {
		f.peakLeases = f.activeLeases
	}
	wait := f.waitLease
	held := f.held[bucketID]
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.activeLeases--
		f.mu.Unlock()
	}()
	if wait {
		<-ctx.Done()
		return mail.ScanLeaseResult{}, ctx.Err()
	}
	if held {
		return mail.ScanLeaseResult{Status: mail.ScanLeaseHeldByOther}, nil
	}
	return mail.ScanLeaseResult{Status: mail.ScanLeaseAcquired, AcceptedTTLMs: 2000, LeaseEpoch: 1}, nil
}

func (f *fakeRepository) ListExpireDue(_ context.Context, bucketID uint32, _ int64, _ int) ([]mail.DueCandidate, error) {
	return append([]mail.DueCandidate(nil), f.expire[bucketID]...), nil
}

func (f *fakeRepository) ListGcDue(_ context.Context, bucketID uint32, _ int64, _ int) ([]mail.DueCandidate, error) {
	return append([]mail.DueCandidate(nil), f.gc[bucketID]...), nil
}

func (f *fakeRepository) ExpireVisibility(context.Context, mail.MailDueRefV1) (mail.ExpireResult, error) {
	status := f.expireStatus
	if status == "" {
		status = mail.ExpireApplied
	}
	return mail.ExpireResult{Status: status}, nil
}

func (f *fakeRepository) GcMail(context.Context, mail.MailDueRefV1) (mail.GcResult, error) {
	return mail.GcResult{Status: mail.GcPurged}, nil
}

func (f *fakeRepository) RegisterDueRef(_ context.Context, ref mail.MailDueRefV1, _ int64) error {
	f.mu.Lock()
	f.registered = append(f.registered, ref)
	f.mu.Unlock()
	return nil
}

func (f *fakeRepository) ListReceiptDue(_ context.Context, bucketID uint32, _ int64, _ int) ([]redisplayermail.ReceiptDueCandidate, error) {
	return append([]redisplayermail.ReceiptDueCandidate(nil), f.receipts[bucketID]...), nil
}

func (f *fakeRepository) GcReceipt(context.Context, redisplayermail.ReceiptDueRef) (redisplayermail.ReceiptGcResult, error) {
	return redisplayermail.ReceiptGcResult{Status: redisplayermail.ReceiptGcPurged}, nil
}

func TestRunOnceProcessesOwnedBucketsAndAllMailIndexes(t *testing.T) {
	player := uint64(10001)
	expireRef := testDueRef(t, player, "D:expire", mail.DueKindExpireVisibility)
	gcRef := testDueRef(t, player, "D:gc", mail.DueKindPhysicalGC)
	receiptRef := redisplayermail.ReceiptDueRef{
		Kind: redisplayermail.ReceiptDueProjection, PlayerID: player, MailID: gcRef.MailID,
		ReceiptField: "projection:" + strings.Repeat("a", 64),
	}
	repository := &fakeRepository{
		leases: make(map[uint32]int),
		expire: map[uint32][]mail.DueCandidate{3: {{Ref: expireRef, DueAtMs: 100}}},
		gc:     map[uint32][]mail.DueCandidate{7: {{Ref: gcRef, DueAtMs: 200}}},
		receipts: map[uint32][]redisplayermail.ReceiptDueCandidate{
			3: {{Ref: receiptRef, DueAtMs: 300}},
		},
	}
	worker, err := mailmaintenance.NewWorker(mailmaintenance.Config{
		Repository: repository, OwnerToken: "worker-a",
		BucketCount: 8, WorkerIndex: 3, WorkerCount: 4, BucketsPerRound: 2,
		BatchLimit: 4, MaxConcurrent: 2, LeaseTTL: 2 * time.Second,
		SafetyMargin: 100 * time.Millisecond, OperationTimeout: time.Second,
		RoundTimeout: time.Second, Now: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}

	stats, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.OwnedBuckets != 2 || stats.Buckets != 2 || stats.AdvisoryLeasedBuckets != 2 || stats.Candidates() != 3 ||
		stats.Expired != 1 || stats.GcPurged != 1 || stats.ReceiptPurged != 1 ||
		stats.Due.Depth != 3 || !stats.Due.Available || !stats.SweepCompleted {
		t.Fatalf("round stats=%+v", stats)
	}
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if repository.leases[3] != 1 || repository.leases[7] != 1 || len(repository.leases) != 2 {
		t.Fatalf("leases=%v", repository.leases)
	}
}

func TestRunOnceBoundsDefaultSingleWorkerCapacity(t *testing.T) {
	repository := &fakeRepository{leases: make(map[uint32]int)}
	worker := mustWorker(t, repository, mailmaintenance.Config{
		BucketCount: 4096, WorkerIndex: 0, WorkerCount: 1,
		BucketsPerRound: 7, MaxConcurrent: 3,
	})
	stats, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if stats.OwnedBuckets != 4096 || stats.Buckets != 7 || len(repository.leaseCalls) != 7 {
		t.Fatalf("capacity stats=%+v calls=%v", stats, repository.leaseCalls)
	}
	if repository.peakLeases > 3 {
		t.Fatalf("peak bucket concurrency=%d want <=3", repository.peakLeases)
	}
	if stats.Due.Available {
		t.Fatal("owned readiness must stay unavailable before the first complete rotation")
	}
}

func TestRunOnceStaticShardRotatesAndNeverTouchesForeignBuckets(t *testing.T) {
	repository := &fakeRepository{leases: make(map[uint32]int), held: map[uint32]bool{2: true}}
	worker := mustWorker(t, repository, mailmaintenance.Config{
		BucketCount: 10, WorkerIndex: 1, WorkerCount: 3,
		BucketsPerRound: 2, MaxConcurrent: 2,
	})
	first, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first.Due.Available || first.SweepCompleted {
		t.Fatalf("first partial rotation=%+v", first)
	}
	assertLeaseCalls(t, repository, []uint32{1, 4})

	repository.mu.Lock()
	repository.leaseCalls = nil
	repository.mu.Unlock()
	second, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assertLeaseCalls(t, repository, []uint32{1, 7})
	if !second.Due.Available || !second.SweepCompleted || second.HeldByOther != 0 {
		t.Fatalf("completed owned rotation=%+v", second)
	}
	if worker.Ownership().Owns(2) {
		t.Fatal("foreign held bucket unexpectedly belongs to worker")
	}
}

func TestOwnedReadinessAggregateUpdatesIncrementallyAcrossRotation(t *testing.T) {
	ref0 := testDueRef(t, 10001, "heap-0", mail.DueKindExpireVisibility)
	ref1 := testDueRef(t, 10002, "heap-1", mail.DueKindExpireVisibility)
	repository := &fakeRepository{
		leases: make(map[uint32]int),
		expire: map[uint32][]mail.DueCandidate{
			0: {{Ref: ref0, DueAtMs: 100}},
			1: {{Ref: ref1, DueAtMs: 200}},
		},
	}
	worker := mustWorker(t, repository, mailmaintenance.Config{
		BucketCount: 2, WorkerIndex: 0, WorkerCount: 1,
		BucketsPerRound: 1, MaxConcurrent: 1,
	})
	if first, err := worker.RunOnce(context.Background()); err != nil || first.Due.Available {
		t.Fatalf("first rotation stats=%+v err=%v", first, err)
	}
	second, err := worker.RunOnce(context.Background())
	if err != nil || !second.Due.Available || second.Due.Depth != 2 || second.Due.OldestDueAtMs != 100 {
		t.Fatalf("complete aggregate stats=%+v err=%v", second, err)
	}
	repository.expire[0] = []mail.DueCandidate{{Ref: ref0, DueAtMs: 300}}
	third, err := worker.RunOnce(context.Background())
	if err != nil || !third.Due.Available || third.Due.Depth != 2 || third.Due.OldestDueAtMs != 200 {
		t.Fatalf("incremental aggregate stats=%+v err=%v", third, err)
	}
}

func TestRunOnceRoundDeadlineCancelsInflightAndStopsScheduling(t *testing.T) {
	repository := &fakeRepository{waitLease: true, leases: make(map[uint32]int)}
	worker := mustWorker(t, repository, mailmaintenance.Config{
		BucketCount: 4096, WorkerIndex: 0, WorkerCount: 1,
		BucketsPerRound: 32, MaxConcurrent: 4,
		OperationTimeout: time.Second, RoundTimeout: 40 * time.Millisecond,
	})
	started := time.Now()
	stats, err := worker.RunOnce(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) || !stats.DeadlineExceeded {
		t.Fatalf("round error=%v stats=%+v", err, stats)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("round deadline took %s", elapsed)
	}
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if len(repository.leaseCalls) > 4 || repository.peakLeases > 4 {
		t.Fatalf("deadline scheduled calls=%d peak=%d", len(repository.leaseCalls), repository.peakLeases)
	}
}

func TestRunOnceClassifiesTypedTimeoutUnknown(t *testing.T) {
	ref := testDueRef(t, 10001, "timeout", mail.DueKindExpireVisibility)
	repository := &fakeRepository{
		leases: make(map[uint32]int), expireStatus: mail.ExpireTimeoutUnknown,
		expire: map[uint32][]mail.DueCandidate{0: {{Ref: ref, DueAtMs: 1}}},
	}
	worker := mustWorker(t, repository, mailmaintenance.Config{
		BucketCount: 1, WorkerIndex: 0, WorkerCount: 1, BucketsPerRound: 1,
	})
	stats, err := worker.RunOnce(context.Background())
	if err == nil || stats.Unknown != 1 || stats.Retried != 1 || stats.Failed != 0 {
		t.Fatalf("timeout unknown err=%v stats=%+v", err, stats)
	}
}

func TestRunOnceDistinguishesUnavailableNotSentFromTimeoutUnknown(t *testing.T) {
	ref := testDueRef(t, 10001, "not-sent", mail.DueKindExpireVisibility)
	repository := &fakeRepository{
		leases: make(map[uint32]int), expireStatus: mail.ExpireUnavailableNotSent,
		expire: map[uint32][]mail.DueCandidate{0: {{Ref: ref, DueAtMs: 1}}},
	}
	worker := mustWorker(t, repository, mailmaintenance.Config{
		BucketCount: 1, WorkerIndex: 0, WorkerCount: 1, BucketsPerRound: 1,
	})
	stats, err := worker.RunOnce(context.Background())
	if err == nil || stats.Unknown != 0 || stats.Retried != 1 || stats.Failed != 1 {
		t.Fatalf("unavailable-not-sent err=%v stats=%+v", err, stats)
	}
}

func TestRunOnceStopsOnParentCancellationBeforeScheduling(t *testing.T) {
	repository := &fakeRepository{waitLease: true, leases: make(map[uint32]int)}
	worker := mustWorker(t, repository, mailmaintenance.Config{
		BucketCount: 2, WorkerIndex: 0, WorkerCount: 1, BucketsPerRound: 2,
		OperationTimeout: 10 * time.Second,
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stats, err := worker.RunOnce(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("run error=%v, want context.Canceled", err)
	}
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if len(repository.leaseCalls) != 0 || stats.Buckets != 0 {
		t.Fatalf("canceled round performed work: stats=%+v calls=%v", stats, repository.leaseCalls)
	}
}

func TestNewWorkerRejectsUnboundedConfiguration(t *testing.T) {
	repository := &fakeRepository{}
	for _, config := range []mailmaintenance.Config{
		{BatchLimit: mailmaintenance.MaxBatchLimit + 1},
		{BucketsPerRound: mailmaintenance.MaxBucketsPerRound + 1},
		{WorkerIndex: 2, WorkerCount: 2},
	} {
		config.Repository = repository
		config.OwnerToken = "worker-a"
		config.BucketCount = 8
		if config.WorkerCount == 0 {
			config.WorkerCount = 1
		}
		_, err := mailmaintenance.NewWorker(config)
		if !errors.Is(err, mailmaintenance.ErrInvalidConfig) {
			t.Fatalf("config=%+v error=%v, want ErrInvalidConfig", config, err)
		}
	}
}

func mustWorker(t *testing.T, repository *fakeRepository, config mailmaintenance.Config) *mailmaintenance.Worker {
	t.Helper()
	config.Repository = repository
	config.OwnerToken = "worker-a"
	if config.BatchLimit == 0 {
		config.BatchLimit = 4
	}
	if config.MaxConcurrent == 0 {
		config.MaxConcurrent = 2
	}
	if config.LeaseTTL == 0 {
		config.LeaseTTL = 2 * time.Second
	}
	if config.SafetyMargin == 0 {
		config.SafetyMargin = 100 * time.Millisecond
	}
	if config.OperationTimeout == 0 {
		config.OperationTimeout = time.Second
	}
	if config.RoundTimeout == 0 {
		config.RoundTimeout = time.Second
	}
	worker, err := mailmaintenance.NewWorker(config)
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

func assertLeaseCalls(t *testing.T, repository *fakeRepository, want []uint32) {
	t.Helper()
	repository.mu.Lock()
	got := append([]uint32(nil), repository.leaseCalls...)
	repository.mu.Unlock()
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
	if len(got) != len(want) {
		t.Fatalf("lease calls=%v want=%v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("lease calls=%v want=%v", got, want)
		}
	}
}

func testDueRef(t *testing.T, player uint64, deliveryID string, kind mail.DueKind) mail.MailDueRefV1 {
	t.Helper()
	mailID, err := mail.MailID(player, deliveryID)
	if err != nil {
		t.Fatal(err)
	}
	return mail.MailDueRefV1{
		Kind: kind, PlayerID: player, MailID: mailID,
		MailVersion: 1, ExpectedStateVersion: 1,
	}
}
