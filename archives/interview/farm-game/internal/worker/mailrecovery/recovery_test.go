package mailrecovery

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/player/mail"
	"farmgame/internal/storage/authoritystore"
	authority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workidentity"
)

const testRecipient = uint64(10001)

type fakeLease struct {
	mu sync.Mutex

	payloads  map[string][]byte
	claims    map[string]authority.WorkClaim
	retries   map[string]uint64
	completed map[string]bool

	claimTTL         time.Duration
	renewFence       bool
	rescheduleStatus authority.WorkLeaseStatus
	completeStatus   authority.WorkLeaseStatus
	hashOverride     string

	claimCalls      int
	renewCalls      int
	rescheduleCalls int
	completeCalls   int
	lastProof       authority.WorkCompletionProof
}

func newFakeLease() *fakeLease {
	return &fakeLease{payloads: make(map[string][]byte), claims: make(map[string]authority.WorkClaim), retries: make(map[string]uint64), completed: make(map[string]bool)}
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
	payload := append([]byte(nil), f.payloads[workID]...)
	decoded, err := DecodeMailClaimProjectionPayload(payload)
	subject := testRecipient
	payloadHash := fmt.Sprintf("%064x", 1)
	if err == nil {
		subject = decoded.RecipientPlayerID
		payloadHash = MailClaimProjectionPayloadHash(decoded)
	}
	if f.claimTTL <= 0 {
		f.claimTTL = 2 * time.Second
	}
	claim := authority.WorkClaim{Status: authority.WorkClaimClaimed, WorkID: workID, WorkType: workType,
		SubjectPlayerID: subject, OperationID: "operation-1", Payload: payload,
		PayloadHash: payloadHash, RetryCount: f.retries[workID], WorkVersion: 1,
		LeaseEpoch: uint64(f.claimCalls), AcceptedTTLms: f.claimTTL.Milliseconds(), LeaseHolderBootID: bootID}
	if f.hashOverride != "" {
		claim.PayloadHash = f.hashOverride
	}
	if holderID == "" || bootID == "" || token == "" {
		return authority.WorkClaim{}, errors.New("fake worker identity missing")
	}
	f.claims[workID] = claim
	_ = ttlMs
	return claim, nil
}

func (f *fakeLease) RenewWorkLeaseWithBoot(_ context.Context, bucket rediskey.AuthorityBucketRef, workType, workID, holderID, bootID string, epoch uint64, token string, ttlMs int64) (authority.WorkLeaseRenewResult, error) {
	if err := bucket.Validate(); err != nil {
		return authority.WorkLeaseRenewResult{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.renewCalls++
	if f.renewFence {
		return authority.WorkLeaseRenewResult{Status: authority.WorkLeaseFenceMismatch}, nil
	}
	claim, ok := f.claims[workID]
	if !ok || claim.LeaseEpoch != epoch || workType != FamilyMailClaimProjection || holderID == "" || bootID == "" || token == "" {
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
	if !ok || claim.LeaseEpoch != epoch || version != claim.WorkVersion || workType != FamilyMailClaimProjection || holderID == "" || bootID == "" || token == "" {
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
	claim, ok := f.claims[workID]
	if !ok || claim.LeaseEpoch != epoch || claim.WorkVersion != version || workType != FamilyMailClaimProjection || holderID == "" || bootID == "" || token == "" {
		return authority.WorkRescheduleResult{Status: authority.WorkLeaseFenceMismatch}, nil
	}
	if f.rescheduleStatus != authority.WorkLeaseUnknown {
		return authority.WorkRescheduleResult{Status: f.rescheduleStatus}, nil
	}
	f.retries[workID] = retry
	delete(f.claims, workID)
	return authority.WorkRescheduleResult{Status: authority.WorkLeaseRenewed, RetryCount: retry}, nil
}

type fakeDue struct {
	mu    sync.Mutex
	refs  []authority.DueWorkRef
	limit int
}

func (f *fakeDue) ListDueWork(_ context.Context, bucket rediskey.AuthorityBucketRef, limit int) ([]authority.DueWorkRef, error) {
	if err := bucket.Validate(); err != nil {
		return nil, err
	}
	// Mirror the physical bound enforced by WorkLeaseStore.ListDueWork so an
	// out-of-range probe fails here exactly as it would against Redis.
	if limit <= 0 || limit > 4096 {
		return nil, fmt.Errorf("%w: due limit", authoritystore.ErrInvalidRequest)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.limit = limit
	if limit > len(f.refs) {
		limit = len(f.refs)
	}
	return append([]authority.DueWorkRef(nil), f.refs[:limit]...), nil
}

type fakeMailEntries struct {
	mu      sync.Mutex
	results []mail.ProjectionResult
	errs    []error
	calls   []mail.ClaimProjectionOrder
}

func (f *fakeMailEntries) InsertMailIfAbsent(context.Context, mail.MailEntry) (mail.InsertResult, error) {
	return mail.InsertResult{}, errors.New("unexpected insert")
}

func (f *fakeMailEntries) MarkMailRead(context.Context, uint64, string, string) (mail.MutationResult, error) {
	return mail.MutationResult{}, errors.New("unexpected mark read")
}

func (f *fakeMailEntries) DeleteMail(context.Context, uint64, string, string, mail.DeleteAuthorization) (mail.MutationResult, error) {
	return mail.MutationResult{}, errors.New("unexpected delete")
}

func (f *fakeMailEntries) ApplyMailClaimProjection(_ context.Context, order mail.ClaimProjectionOrder) (mail.ProjectionResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, order)
	if len(f.errs) > 0 {
		err := f.errs[0]
		f.errs = f.errs[1:]
		return mail.ProjectionResult{}, err
	}
	if len(f.results) == 0 {
		return mail.ProjectionResult{Status: mail.ProjectionApplied}, nil
	}
	result := f.results[0]
	f.results = f.results[1:]
	return result, nil
}

func testBucket(t *testing.T) rediskey.AuthorityBucketRef {
	t.Helper()
	ns, err := rediskey.NewNamespace("test", "z1")
	if err != nil {
		t.Fatal(err)
	}
	profile := rediskey.PhysicalProfile{ProfileVersion: "redis-physical-v1", Namespace: ns, AuthorityLayoutVersion: "v1", WorkflowLayoutVersion: "v1", WorkflowHashVersion: 1, WorkflowBucketCount: 256}
	calculator, err := actorrouting.NewCalculator("classic-player", actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608040001}, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := profile.AuthorityBucketForActor(calculator, actorrouting.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: testRecipient})
	if err != nil {
		t.Fatal(err)
	}
	return bucket
}

func testPayload(t *testing.T, suffix string) (string, []byte, MailClaimProjectionPayload) {
	t.Helper()
	workID, err := workidentity.StableOpaqueID("mail-projection-order", suffix)
	if err != nil {
		t.Fatal(err)
	}
	mailID, err := workidentity.StableOpaqueID(mail.MailIDNamespace, fmt.Sprint(testRecipient), "delivery-"+suffix)
	if err != nil {
		t.Fatal(err)
	}
	payload := MailClaimProjectionPayload{RecipientPlayerID: testRecipient, MailID: mailID, ClaimReceiptRef: "receipt-" + suffix,
		ClaimMaterialHash: fmt.Sprintf("%064x", len(suffix)+1), ClaimedAtMs: 1_725_000_000_000}
	return workID, marshalMailClaimProjectionPayload(payload), payload
}

func dueRef(t *testing.T, workID string) authority.DueWorkRef {
	t.Helper()
	member, err := workidentity.EncodeWorkRefV1(FamilyMailClaimProjection, workID)
	if err != nil {
		t.Fatal(err)
	}
	return authority.DueWorkRef{Ref: workidentity.WorkRef{WorkType: FamilyMailClaimProjection, WorkID: workID}, DueAtMs: time.Now().UnixMilli(), Member: member}
}

func newTestWorker(t *testing.T, lease *fakeLease, due *fakeDue, entries *fakeMailEntries, bucket rediskey.AuthorityBucketRef) *Worker {
	t.Helper()
	worker, err := NewWorker(Config{WorkLease: lease, DueIndex: due, MailEntries: entries, Buckets: []rediskey.AuthorityBucketRef{bucket},
		Owner: "mail-worker-1", Token: "mail-token-1", BootID: "mail-boot-1", ClaimLimit: 8, MaxConcurrent: 2,
		LeaseTTL: 2 * time.Second, SafetyMargin: 10 * time.Millisecond, OperationTimeout: 100 * time.Millisecond,
		RetryDelay: time.Millisecond, Now: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

func TestWorkerProjectionSuccessCompletesFencedOrder(t *testing.T) {
	bucket := testBucket(t)
	workID, raw, payload := testPayload(t, "success")
	lease := newFakeLease()
	lease.payloads[workID] = raw
	due := &fakeDue{refs: []authority.DueWorkRef{dueRef(t, workID)}}
	entries := &fakeMailEntries{results: []mail.ProjectionResult{{Status: mail.ProjectionApplied}}}
	worker := newTestWorker(t, lease, due, entries, bucket)

	stats, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Claimed != 1 || stats.Applied != 1 || stats.Completed != 1 || stats.Rescheduled != 0 || lease.completeCalls != 1 {
		t.Fatalf("stats=%+v completes=%d", stats, lease.completeCalls)
	}
	if len(entries.calls) != 1 || entries.calls[0].MailID != payload.MailID || entries.calls[0].RecipientPlayerID != payload.RecipientPlayerID {
		t.Fatalf("projection calls=%+v", entries.calls)
	}
	if lease.lastProof.ParticipantStep != mailProjectionStep || lease.lastProof.ParticipantOutcome != string(mail.ProjectionApplied) {
		t.Fatalf("proof=%+v", lease.lastProof)
	}
}

func TestWorkerProjectionNoChangeCompletesWithoutRetry(t *testing.T) {
	bucket := testBucket(t)
	workID, raw, _ := testPayload(t, "no-change")
	lease := newFakeLease()
	lease.payloads[workID] = raw
	entries := &fakeMailEntries{results: []mail.ProjectionResult{{Status: mail.ProjectionNoChange}}}
	worker := newTestWorker(t, lease, &fakeDue{refs: []authority.DueWorkRef{dueRef(t, workID)}}, entries, bucket)

	stats, err := worker.RunOnce(context.Background())
	if err != nil || stats.NoChange != 1 || stats.Completed != 1 || stats.Rescheduled != 0 || lease.completeCalls != 1 {
		t.Fatalf("stats=%+v err=%v completes=%d", stats, err, lease.completeCalls)
	}
}

func TestWorkerProjectionTimeoutRetriesAndReplaysIdempotently(t *testing.T) {
	bucket := testBucket(t)
	workID, raw, _ := testPayload(t, "retry")
	lease := newFakeLease()
	lease.payloads[workID] = raw
	entries := &fakeMailEntries{results: []mail.ProjectionResult{{Status: mail.ProjectionTimeoutUnknown}, {Status: mail.ProjectionApplied}}}
	due := &fakeDue{refs: []authority.DueWorkRef{dueRef(t, workID)}}
	worker := newTestWorker(t, lease, due, entries, bucket)

	first, firstErr := worker.RunOnce(context.Background())
	if firstErr == nil || first.Unknown != 1 || first.Completed != 0 || first.Rescheduled != 1 || lease.completeCalls != 0 {
		t.Fatalf("first stats=%+v err=%v", first, firstErr)
	}
	second, secondErr := worker.RunOnce(context.Background())
	if secondErr != nil || second.Completed != 1 || second.Applied != 1 || len(entries.calls) != 2 || lease.completeCalls != 1 {
		t.Fatalf("second stats=%+v err=%v calls=%d completes=%d", second, secondErr, len(entries.calls), lease.completeCalls)
	}
	if entries.calls[0].WorkID != entries.calls[1].WorkID || entries.calls[0].WorkVersion != entries.calls[1].WorkVersion {
		t.Fatalf("retry changed identity: %+v", entries.calls)
	}
}

func TestWorkerProjectionFailureRetries(t *testing.T) {
	bucket := testBucket(t)
	workID, raw, _ := testPayload(t, "failure")
	lease := newFakeLease()
	lease.payloads[workID] = raw
	entries := &fakeMailEntries{errs: []error{errors.New("mail backend unavailable")}, results: []mail.ProjectionResult{{Status: mail.ProjectionApplied}}}
	due := &fakeDue{refs: []authority.DueWorkRef{dueRef(t, workID)}}
	worker := newTestWorker(t, lease, due, entries, bucket)

	first, firstErr := worker.RunOnce(context.Background())
	if firstErr == nil || first.Failed != 1 || first.Rescheduled != 1 || first.Completed != 0 {
		t.Fatalf("first stats=%+v err=%v", first, firstErr)
	}
	second, secondErr := worker.RunOnce(context.Background())
	if secondErr != nil || second.Completed != 1 || second.Applied != 1 || len(entries.calls) != 2 {
		t.Fatalf("second stats=%+v err=%v calls=%d", second, secondErr, len(entries.calls))
	}
}

func TestWorkerInvalidPayloadFailsClosedBeforeMailboxMutation(t *testing.T) {
	bucket := testBucket(t)
	workID, raw, _ := testPayload(t, "invalid")
	lease := newFakeLease()
	lease.payloads[workID] = append(raw, 0x28, 0x01) // unknown field
	entries := &fakeMailEntries{}
	worker := newTestWorker(t, lease, &fakeDue{refs: []authority.DueWorkRef{dueRef(t, workID)}}, entries, bucket)

	stats, err := worker.RunOnce(context.Background())
	if err == nil || stats.BadPayload != 1 || stats.Rescheduled != 1 || len(entries.calls) != 0 || lease.completeCalls != 0 {
		t.Fatalf("stats=%+v err=%v calls=%d completes=%d", stats, err, len(entries.calls), lease.completeCalls)
	}
}

func TestWorkerStalePayloadHashFailsClosed(t *testing.T) {
	bucket := testBucket(t)
	workID, raw, _ := testPayload(t, "stale-hash")
	lease := newFakeLease()
	lease.payloads[workID] = raw
	lease.hashOverride = fmt.Sprintf("%064x", 7)
	entries := &fakeMailEntries{}
	worker := newTestWorker(t, lease, &fakeDue{refs: []authority.DueWorkRef{dueRef(t, workID)}}, entries, bucket)

	stats, err := worker.RunOnce(context.Background())
	if err == nil || stats.BadPayload != 1 || stats.Rescheduled != 1 || len(entries.calls) != 0 {
		t.Fatalf("stats=%+v err=%v calls=%d", stats, err, len(entries.calls))
	}
}

func TestWorkerLeaseLossStopsBeforeProjection(t *testing.T) {
	bucket := testBucket(t)
	workID, raw, _ := testPayload(t, "lease-loss")
	lease := newFakeLease()
	lease.payloads[workID] = raw
	lease.claimTTL = 20 * time.Millisecond
	lease.renewFence = true
	lease.rescheduleStatus = authority.WorkLeaseFenceMismatch
	entries := &fakeMailEntries{}
	worker, err := NewWorker(Config{WorkLease: lease, DueIndex: &fakeDue{refs: []authority.DueWorkRef{dueRef(t, workID)}}, MailEntries: entries,
		Buckets: []rediskey.AuthorityBucketRef{bucket}, Owner: "mail-worker-fence", Token: "mail-token-fence", BootID: "mail-boot-fence",
		LeaseTTL: 20 * time.Millisecond, SafetyMargin: 10 * time.Millisecond, OperationTimeout: 10 * time.Millisecond, RetryDelay: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}

	stats, runErr := worker.RunOnce(context.Background())
	if runErr == nil || stats.Claimed != 1 || stats.LeaseLost != 1 || stats.Rescheduled != 1 || lease.renewCalls != 1 || len(entries.calls) != 0 || lease.completeCalls != 0 {
		t.Fatalf("stats=%+v err=%v renews=%d calls=%d completes=%d", stats, runErr, lease.renewCalls, len(entries.calls), lease.completeCalls)
	}
	if !errors.Is(runErr, ErrLeaseLost) {
		t.Fatalf("error=%v, want lease loss", runErr)
	}
}

func TestWorkerUsesFiniteDueIndexBatch(t *testing.T) {
	bucket := testBucket(t)
	firstID, firstRaw, _ := testPayload(t, "bounded-1")
	secondID, secondRaw, _ := testPayload(t, "bounded-2")
	lease := newFakeLease()
	lease.payloads[firstID] = firstRaw
	lease.payloads[secondID] = secondRaw
	due := &fakeDue{refs: []authority.DueWorkRef{dueRef(t, firstID), dueRef(t, secondID)}}
	entries := &fakeMailEntries{}
	worker, err := NewWorker(Config{WorkLease: lease, DueIndex: due, MailEntries: entries, Buckets: []rediskey.AuthorityBucketRef{bucket},
		Owner: "mail-worker-bounded", Token: "mail-token-bounded", BootID: "mail-boot-bounded", ClaimLimit: 1, RetryDelay: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := worker.RunOnce(context.Background())
	if err != nil || due.limit != 1 || stats.Candidates != 1 || stats.Completed != 1 || len(entries.calls) != 1 {
		t.Fatalf("stats=%+v err=%v dueLimit=%d calls=%d", stats, err, due.limit, len(entries.calls))
	}
}

func TestWorkerObserveDueProvesDepthAndOldest(t *testing.T) {
	bucket := testBucket(t)
	firstID, _, _ := testPayload(t, "observe-1")
	secondID, _, _ := testPayload(t, "observe-2")
	first := dueRef(t, firstID)
	second := dueRef(t, secondID)
	first.DueAtMs = 200
	second.DueAtMs = 100
	due := &fakeDue{refs: []authority.DueWorkRef{first, second}}
	worker := newTestWorker(t, newFakeLease(), due, &fakeMailEntries{}, bucket)

	observation, err := worker.ObserveDue(context.Background(), 8)
	if err != nil || !observation.Available || observation.Depth != 2 || observation.OldestDueAtMs != 100 {
		t.Fatalf("observation=%+v err=%v", observation, err)
	}
}

func TestWorkerObserveDueAcceptsPhysicalMaximumWithoutRequesting4097(t *testing.T) {
	bucket := testBucket(t)
	id, _, _ := testPayload(t, "observe-physical-max")
	ref := dueRef(t, id)
	ref.DueAtMs = 101
	due := &fakeDue{refs: []authority.DueWorkRef{ref}}
	worker := newTestWorker(t, newFakeLease(), due, &fakeMailEntries{}, bucket)

	observation, err := worker.ObserveDue(context.Background(), 4096)
	if err != nil || !observation.Available || observation.Depth != 1 || observation.OldestDueAtMs != 101 {
		t.Fatalf("observation=%+v err=%v", observation, err)
	}
	if due.limit > 4096 {
		t.Fatalf("requested due limit=%d exceeds the physical ListDueWork bound", due.limit)
	}
}

func TestWorkerObserveDueFailsClosedWhenBoundIsExhausted(t *testing.T) {
	bucket := testBucket(t)
	firstID, _, _ := testPayload(t, "observe-bound-1")
	secondID, _, _ := testPayload(t, "observe-bound-2")
	due := &fakeDue{refs: []authority.DueWorkRef{dueRef(t, firstID), dueRef(t, secondID)}}
	worker := newTestWorker(t, newFakeLease(), due, &fakeMailEntries{}, bucket)

	observation, err := worker.ObserveDue(context.Background(), 1)
	if err != nil || observation.Available {
		t.Fatalf("observation=%+v err=%v", observation, err)
	}
}
