package maildeliveryrecovery

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/player/mail"
	authority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workidentity"
)

const testRecipient = uint64(10001)

type fakeLease struct {
	mu sync.Mutex

	payloads     map[string][]byte
	operations   map[string]string
	claims       map[string]authority.WorkClaim
	retries      map[string]uint64
	completed    map[string]bool
	claimTTL     time.Duration
	hashOverride string

	renewStatus      authority.WorkLeaseStatus
	rescheduleStatus authority.WorkLeaseStatus
	completeStatus   authority.WorkLeaseStatus

	claimCalls      int
	renewCalls      int
	rescheduleCalls int
	completeCalls   int
	lastProof       authority.WorkCompletionProof
}

func newFakeLease() *fakeLease {
	return &fakeLease{
		payloads:   make(map[string][]byte),
		operations: make(map[string]string),
		claims:     make(map[string]authority.WorkClaim),
		retries:    make(map[string]uint64),
		completed:  make(map[string]bool),
	}
}

func (f *fakeLease) ClaimWorkWithBoot(_ context.Context, bucket rediskey.AuthorityBucketRef, workType, workID, holderID, bootID, token string, ttlMs int64) (authority.WorkClaim, error) {
	if err := bucket.Validate(); err != nil {
		return authority.WorkClaim{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claimCalls++
	if f.completed[workID] {
		return authority.WorkClaim{Status: authority.WorkClaimNotClaimable}, nil
	}
	if _, held := f.claims[workID]; held {
		return authority.WorkClaim{Status: authority.WorkClaimLeaseHeld}, nil
	}
	raw := append([]byte(nil), f.payloads[workID]...)
	if len(raw) == 0 {
		return authority.WorkClaim{Status: authority.WorkClaimNotFound}, nil
	}
	entry, err := mail.DecodeMailEntry(raw)
	if err != nil {
		return authority.WorkClaim{}, err
	}
	if f.claimTTL <= 0 {
		f.claimTTL = 2 * time.Second
	}
	epoch := uint64(f.claimCalls)
	digest := sha256.Sum256(raw)
	payloadHash := fmt.Sprintf("%x", digest[:])
	if f.hashOverride != "" {
		payloadHash = f.hashOverride
	}
	claim := authority.WorkClaim{
		Status: authority.WorkClaimClaimed, WorkID: workID, WorkType: workType,
		SubjectPlayerID: entry.RecipientPlayerID, OperationID: f.operations[workID],
		Payload: raw, PayloadHash: payloadHash, RetryCount: f.retries[workID],
		WorkVersion: 1, LeaseEpoch: epoch, AcceptedTTLms: f.claimTTL.Milliseconds(),
		LeaseHolderBootID: bootID,
	}
	if holderID == "" || bootID == "" || token == "" || ttlMs <= 0 {
		return authority.WorkClaim{}, errors.New("fake worker identity missing")
	}
	f.claims[workID] = claim
	return claim, nil
}

func (f *fakeLease) RenewWorkLeaseWithBoot(_ context.Context, bucket rediskey.AuthorityBucketRef, workType, workID, holderID, bootID string, epoch uint64, token string, ttlMs int64) (authority.WorkLeaseRenewResult, error) {
	if err := bucket.Validate(); err != nil {
		return authority.WorkLeaseRenewResult{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.renewCalls++
	if f.renewStatus != authority.WorkLeaseUnknown {
		return authority.WorkLeaseRenewResult{Status: f.renewStatus}, nil
	}
	claim, ok := f.claims[workID]
	if !ok || claim.LeaseEpoch != epoch || workType != FamilyMailDeliveryOrder ||
		holderID == "" || bootID == "" || token == "" {
		return authority.WorkLeaseRenewResult{Status: authority.WorkLeaseFenceMismatch}, nil
	}
	return authority.WorkLeaseRenewResult{Status: authority.WorkLeaseRenewed, AcceptedTTLms: ttlMs}, nil
}

func (f *fakeLease) CompleteWorkWithProof(_ context.Context, bucket rediskey.AuthorityBucketRef, workType, workID, holderID, bootID string, epoch, version uint64, token string, proof authority.WorkCompletionProof) (authority.WorkCompleteResult, error) {
	if err := bucket.Validate(); err != nil {
		return authority.WorkCompleteResult{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.completeCalls++
	f.lastProof = proof
	if f.completeStatus != authority.WorkLeaseUnknown {
		return authority.WorkCompleteResult{Status: f.completeStatus}, nil
	}
	claim, ok := f.claims[workID]
	if !ok || claim.LeaseEpoch != epoch || version != claim.WorkVersion ||
		workType != FamilyMailDeliveryOrder || holderID == "" || bootID == "" || token == "" {
		return authority.WorkCompleteResult{Status: authority.WorkLeaseFenceMismatch}, nil
	}
	if f.completed[workID] {
		return authority.WorkCompleteResult{Status: authority.WorkLeaseAlreadyCompleted}, nil
	}
	f.completed[workID] = true
	delete(f.claims, workID)
	return authority.WorkCompleteResult{Status: authority.WorkLeaseCompleted}, nil
}

func (f *fakeLease) RescheduleWork(_ context.Context, bucket rediskey.AuthorityBucketRef, workType, workID, holderID, bootID string, epoch, version uint64, token string, retry uint64, _ int64, _ string) (authority.WorkRescheduleResult, error) {
	if err := bucket.Validate(); err != nil {
		return authority.WorkRescheduleResult{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rescheduleCalls++
	if f.rescheduleStatus != authority.WorkLeaseUnknown {
		return authority.WorkRescheduleResult{Status: f.rescheduleStatus}, nil
	}
	claim, ok := f.claims[workID]
	if !ok || claim.LeaseEpoch != epoch || claim.WorkVersion != version ||
		workType != FamilyMailDeliveryOrder || holderID == "" || bootID == "" || token == "" {
		return authority.WorkRescheduleResult{Status: authority.WorkLeaseFenceMismatch}, nil
	}
	f.retries[workID] = retry
	delete(f.claims, workID)
	return authority.WorkRescheduleResult{Status: authority.WorkLeaseRenewed, RetryCount: retry}, nil
}

type fakeDue struct {
	mu    sync.Mutex
	refs  []authority.DueWorkRef
	limit int
	calls int
}

func (f *fakeDue) ListDueWork(_ context.Context, bucket rediskey.AuthorityBucketRef, limit int) ([]authority.DueWorkRef, error) {
	if err := bucket.Validate(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.limit = limit
	f.calls++
	if limit > len(f.refs) {
		limit = len(f.refs)
	}
	return append([]authority.DueWorkRef(nil), f.refs[:limit]...), nil
}

type fakeSourceOrders struct {
	mu      sync.Mutex
	records map[string]authority.SourceOrderSnapshot
	err     error
}

func (f *fakeSourceOrders) GetSourceOrder(_ context.Context, bucket rediskey.AuthorityBucketRef, workType, workID string) (authority.SourceOrderSnapshot, error) {
	if err := bucket.Validate(); err != nil {
		return authority.SourceOrderSnapshot{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return authority.SourceOrderSnapshot{}, f.err
	}
	record, ok := f.records[workID]
	if !ok || record.WorkType != workType {
		return authority.SourceOrderSnapshot{}, errors.New("source order not found")
	}
	return record, nil
}

type fixture struct {
	worker  *Worker
	lease   *fakeLease
	due     *fakeDue
	source  *fakeSourceOrders
	mailbox *mail.MemoryStore
	entry   mail.MailEntry
	raw     []byte
	bucket  rediskey.AuthorityBucketRef
	workID  string
}

func testBucket(t *testing.T) rediskey.AuthorityBucketRef {
	t.Helper()
	namespace, err := rediskey.NewNamespace("mail-delivery-test", "z1")
	if err != nil {
		t.Fatal(err)
	}
	profile := rediskey.PhysicalProfile{
		ProfileVersion: "redis-physical-v1", Namespace: namespace,
		AuthorityLayoutVersion: "v1", WorkflowLayoutVersion: "v1",
		WorkflowHashVersion: 1, WorkflowBucketCount: 64,
	}
	calculator, err := actorrouting.NewCalculator("classic-player",
		actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608130001}, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := profile.AuthorityBucketForActor(calculator,
		actorrouting.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: testRecipient})
	if err != nil {
		t.Fatal(err)
	}
	return bucket
}

func newFixture(t *testing.T, suffix string) *fixture {
	t.Helper()
	now := time.Now().UnixMilli()
	entry, err := mail.BuildMailEntry(mail.DeliveryParams{
		RecipientPlayerID:   testRecipient,
		DeliveryID:          "SYSTEM:10001:" + suffix,
		SourceType:          mail.SourceTypeSystem,
		SourceID:            "welcome-v1",
		TitleSnapshot:       "Welcome",
		BodySnapshot:        "Frozen body " + suffix,
		Reward:              mail.RewardBundle{Coin: 42, Exp: 7, Items: []mail.ItemGrant{{ItemID: 11, Count: 2}}},
		TemplateVersion:     "welcome-v1",
		RewardConfigVersion: "mail-reward-v1",
		CreatedAtMs:         now, VisibleAtMs: now, RetentionMs: 2 * time.Hour.Milliseconds(),
	}, mail.LimitsV1())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := mail.EncodeMailEntry(entry, mail.LimitsV1())
	if err != nil {
		t.Fatal(err)
	}
	workID, err := workidentity.StableOpaqueID("mail-delivery-order-test", suffix)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	operationID := "mail-delivery-operation-" + suffix
	bucket := testBucket(t)
	source := &fakeSourceOrders{records: map[string]authority.SourceOrderSnapshot{
		workID: {
			WorkID: workID, WorkType: FamilyMailDeliveryOrder, PlacementKind: mailPlacementKind,
			BucketVersion: 1, BucketID: bucket.VirtualShard(), SourceAuthorityKind: mailSourceKind,
			CanonicalSourceAuthorityID: fmt.Sprint(testRecipient), AuthorityLayoutVersion: bucket.LayoutVersion(),
			ActorRouteDomain: mailRouteDomain, SubjectPlayerID: testRecipient, OperationID: operationID,
			PayloadHash: fmt.Sprintf("%x", digest[:]), State: "PENDING", WorkVersion: 1,
			TypedPayload: append([]byte(nil), raw...),
		},
	}}
	lease := newFakeLease()
	lease.payloads[workID] = raw
	lease.operations[workID] = operationID
	member, err := workidentity.EncodeWorkRefV1(FamilyMailDeliveryOrder, workID)
	if err != nil {
		t.Fatal(err)
	}
	due := &fakeDue{refs: []authority.DueWorkRef{{
		Ref:     workidentity.WorkRef{WorkType: FamilyMailDeliveryOrder, WorkID: workID},
		DueAtMs: now - 1, Member: member,
	}}}
	mailbox, err := mail.NewMemoryStore(func() int64 { return now }, mail.LimitsV1())
	if err != nil {
		t.Fatal(err)
	}
	worker, err := NewWorker(Config{
		WorkLease: lease, DueIndex: due, SourceOrders: source, MailEntries: mailbox,
		Limits: mail.LimitsV1(), Buckets: []rediskey.AuthorityBucketRef{bucket}, OwnedBuckets: []rediskey.AuthorityBucketRef{bucket},
		Owner: "mail-delivery-worker", Token: "mail-delivery-token", BootID: "mail-delivery-boot",
		ClaimLimit: 8, MaxConcurrent: 1, LeaseTTL: 2 * time.Second,
		SafetyMargin: 10 * time.Millisecond, OperationTimeout: 100 * time.Millisecond,
		RetryDelay: time.Millisecond, Now: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{worker: worker, lease: lease, due: due, source: source,
		mailbox: mailbox, entry: entry, raw: raw, bucket: bucket, workID: workID}
}

func TestDecodeMailDeliveryPayloadRejectsNonCanonicalBytes(t *testing.T) {
	f := newFixture(t, "decode")
	tampered := append(append([]byte(nil), f.raw...), 0)
	if _, err := DecodeMailDeliveryPayload(tampered); !errors.Is(err, ErrPayloadInvalid) {
		t.Fatalf("error=%v, want payload invalid", err)
	}
}

func TestWorkerInsertsFrozenEntryAndCompletes(t *testing.T) {
	f := newFixture(t, "insert")
	stats, err := f.worker.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Claimed != 1 || stats.Inserted != 1 || stats.Completed != 1 ||
		stats.Rescheduled != 0 || f.lease.completeCalls != 1 {
		t.Fatalf("stats=%+v completes=%d", stats, f.lease.completeCalls)
	}
	got, _, err := f.mailbox.GetEntry(context.Background(), testRecipient, f.entry.MailID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, f.entry) {
		t.Fatalf("entry=%+v want=%+v", got, f.entry)
	}
	if f.lease.lastProof.ParticipantStep != mailDeliveryStep ||
		f.lease.lastProof.ParticipantOutcome != string(mail.InsertInserted) {
		t.Fatalf("proof=%+v", f.lease.lastProof)
	}
}

func TestWorkerDuplicateCompletesIdempotently(t *testing.T) {
	f := newFixture(t, "duplicate")
	if result, err := f.mailbox.InsertMailIfAbsent(context.Background(), f.entry); err != nil || result.Status != mail.InsertInserted {
		t.Fatalf("preinsert=%+v err=%v", result, err)
	}
	stats, err := f.worker.RunOnce(context.Background())
	if err != nil || stats.Duplicates != 1 || stats.Completed != 1 || stats.Rescheduled != 0 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	summary, err := f.mailbox.GetSummary(context.Background(), testRecipient)
	if err != nil {
		t.Fatal(err)
	}
	if summary.MailboxRevision != 1 || summary.DeliveryRecords != 1 {
		t.Fatalf("duplicate changed mailbox: %+v", summary)
	}
}

func TestWorkerConflictReschedules(t *testing.T) {
	f := newFixture(t, "conflict")
	conflict := f.entry
	conflict.BodySnapshot = "different frozen body"
	conflict.DeliveryPayloadHash, _ = mail.DeliveryPayloadHash(conflict)
	if result, err := f.mailbox.InsertMailIfAbsent(context.Background(), conflict); err != nil || result.Status != mail.InsertInserted {
		t.Fatalf("preinsert conflict=%+v err=%v", result, err)
	}
	stats, err := f.worker.RunOnce(context.Background())
	if err == nil || stats.Conflicts != 1 || stats.Rescheduled != 1 ||
		stats.Completed != 0 || f.lease.completeCalls != 0 || f.lease.rescheduleCalls != 1 {
		t.Fatalf("stats=%+v err=%v completes=%d reschedules=%d", stats, err, f.lease.completeCalls, f.lease.rescheduleCalls)
	}
}

func TestWorkerTimeoutAfterApplyRetriesSameFrozenEntry(t *testing.T) {
	f := newFixture(t, "timeout")
	f.mailbox.InjectFault(mail.FaultAfterApply)
	first, firstErr := f.worker.RunOnce(context.Background())
	if firstErr == nil || first.Unknown != 1 || first.Rescheduled != 1 || first.Completed != 0 {
		t.Fatalf("first stats=%+v err=%v", first, firstErr)
	}
	second, secondErr := f.worker.RunOnce(context.Background())
	if secondErr != nil || second.Duplicates != 1 || second.Completed != 1 ||
		f.lease.completeCalls != 1 {
		t.Fatalf("second stats=%+v err=%v completes=%d", second, secondErr, f.lease.completeCalls)
	}
}

func TestWorkerTamperedClaimHashDoesNotInsert(t *testing.T) {
	f := newFixture(t, "hash-tamper")
	f.lease.hashOverride = fmt.Sprintf("%064x", 7)
	stats, err := f.worker.RunOnce(context.Background())
	if err == nil || stats.BadPayload != 1 || stats.Rescheduled != 1 ||
		f.lease.completeCalls != 0 {
		t.Fatalf("stats=%+v err=%v completes=%d", stats, err, f.lease.completeCalls)
	}
	summary, err := f.mailbox.GetSummary(context.Background(), testRecipient)
	if err != nil {
		t.Fatal(err)
	}
	if summary.RetainedCount != 0 {
		t.Fatalf("tampered payload inserted: %+v", summary)
	}
}

func TestWorkerSourceOrderBindingRejectsTamper(t *testing.T) {
	f := newFixture(t, "order-binding")
	f.source.records[f.workID] = authority.SourceOrderSnapshot{
		WorkID: f.workID, WorkType: FamilyMailDeliveryOrder, PlacementKind: mailPlacementKind,
		BucketVersion: 1, BucketID: f.bucket.VirtualShard(), SourceAuthorityKind: mailSourceKind,
		CanonicalSourceAuthorityID: "99999", AuthorityLayoutVersion: f.bucket.LayoutVersion(),
		ActorRouteDomain: mailRouteDomain, SubjectPlayerID: testRecipient, OperationID: "other-operation",
		PayloadHash: fmt.Sprintf("%064x", 3), State: "PENDING", WorkVersion: 1,
		TypedPayload: append([]byte(nil), f.raw...),
	}
	stats, err := f.worker.RunOnce(context.Background())
	if err == nil || stats.BadPayload != 1 || stats.Rescheduled != 1 ||
		f.lease.completeCalls != 0 {
		t.Fatalf("stats=%+v err=%v completes=%d", stats, err, f.lease.completeCalls)
	}
}

func TestWorkerLeaseLossDoesNotInsert(t *testing.T) {
	f := newFixture(t, "lease-loss")
	f.lease.claimTTL = 20 * time.Millisecond
	f.lease.renewStatus = authority.WorkLeaseFenceMismatch
	f.worker.config.LeaseTTL = 20 * time.Millisecond
	f.worker.config.SafetyMargin = 10 * time.Millisecond
	f.worker.config.OperationTimeout = 10 * time.Millisecond
	stats, runErr := f.worker.RunOnce(context.Background())
	if runErr == nil || !errors.Is(runErr, ErrLeaseLost) || stats.LeaseLost != 1 ||
		stats.Rescheduled != 1 || f.lease.renewCalls != 1 {
		t.Fatalf("stats=%+v err=%v renews=%d", stats, runErr, f.lease.renewCalls)
	}
	summary, err := f.mailbox.GetSummary(context.Background(), testRecipient)
	if err != nil {
		t.Fatal(err)
	}
	if summary.RetainedCount != 0 {
		t.Fatalf("lease-lost worker inserted: %+v", summary)
	}
}

func TestWorkerFiltersOtherDueWorkTypesBeforeClaim(t *testing.T) {
	f := newFixture(t, "filter")
	otherID, err := workidentity.StableOpaqueID("other-work", "filter")
	if err != nil {
		t.Fatal(err)
	}
	otherMember, err := workidentity.EncodeWorkRefV1("OTHER_WORK", otherID)
	if err != nil {
		t.Fatal(err)
	}
	f.due.refs = append([]authority.DueWorkRef{{
		Ref:     workidentity.WorkRef{WorkType: "OTHER_WORK", WorkID: otherID},
		DueAtMs: f.due.refs[0].DueAtMs, Member: otherMember,
	}}, f.due.refs...)
	stats, err := f.worker.RunOnce(context.Background())
	if err != nil || stats.Candidates != 1 || stats.Claimed != 1 ||
		f.lease.claimCalls != 1 {
		t.Fatalf("stats=%+v err=%v claims=%d", stats, err, f.lease.claimCalls)
	}
}

func testRotationBucket(t *testing.T, shard uint32) rediskey.AuthorityBucketRef {
	t.Helper()
	namespace, err := rediskey.NewNamespace("mail-delivery-rotation-test", "z1")
	if err != nil {
		t.Fatal(err)
	}
	profile := rediskey.PhysicalProfile{
		ProfileVersion: "redis-physical-v1", Namespace: namespace,
		AuthorityLayoutVersion: "v1", WorkflowLayoutVersion: "v1",
		WorkflowHashVersion: 1, WorkflowBucketCount: 64,
	}
	calculator, err := actorrouting.NewCalculator("classic-player",
		actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608130002}, 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := profile.AuthorityBucketForShard(calculator, actorrouting.DomainPlayer, shard)
	if err != nil {
		t.Fatal(err)
	}
	return bucket
}

func TestWorkerRotatesOwnedBucketSubset(t *testing.T) {
	firstBucket := testRotationBucket(t, 0)
	secondBucket := testRotationBucket(t, 1)
	worker := &Worker{config: normalizedConfig{
		OwnedBuckets:    []rediskey.AuthorityBucketRef{firstBucket, secondBucket},
		BucketsPerRound: 1,
	}}
	first := worker.nextRoundBuckets()
	second := worker.nextRoundBuckets()
	third := worker.nextRoundBuckets()
	if len(first) != 1 || len(second) != 1 || len(third) != 1 {
		t.Fatalf("round bucket counts: first=%d second=%d third=%d", len(first), len(second), len(third))
	}
	if first[0].VirtualShard() != 0 || second[0].VirtualShard() != 1 || third[0].VirtualShard() != 0 {
		t.Fatalf("owned bucket cursor order: first=%d second=%d third=%d", first[0].VirtualShard(), second[0].VirtualShard(), third[0].VirtualShard())
	}
}

func TestObserveDueRotatesAndRequiresFullOwnedCoverage(t *testing.T) {
	firstBucket := testRotationBucket(t, 0)
	secondBucket := testRotationBucket(t, 1)
	due := &fakeDue{}
	worker := &Worker{config: normalizedConfig{
		DueIndex: due, OwnedBuckets: []rediskey.AuthorityBucketRef{firstBucket, secondBucket},
		BucketsPerRound: 1, RoundTimeBudget: time.Second,
	}, observations: make(map[string]DueIndexObservation, 2)}

	first, err := worker.ObserveDue(context.Background(), 8)
	if err != nil {
		t.Fatal(err)
	}
	if first.Available || due.calls != 1 {
		t.Fatalf("first observation=%+v calls=%d, want partial bounded proof", first, due.calls)
	}
	second, err := worker.ObserveDue(context.Background(), 8)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Available || due.calls != 2 {
		t.Fatalf("second observation=%+v calls=%d, want complete bounded proof", second, due.calls)
	}
}
