package authority

import (
	"context"
	"errors"
	"net"
	"sync"
	"syscall"
	"testing"

	"github.com/redis/go-redis/v9"

	"farmgame/internal/storage/authoritystore"
)

// dropClient simulates the two TIMEOUT_UNKNOWN shapes from contract 30 §30-05:
// the request never reached execution (before-apply) or it executed but the
// reply was lost (after-apply/response-lost).
type dropClient struct {
	redis.UniversalClient
	mu          sync.Mutex
	dropBefore  int
	dropAfter   int
	dropNotSent int
}

func (c *dropClient) takeDrop() (before, after, notSent int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	before, after, notSent = c.dropBefore, c.dropAfter, c.dropNotSent
	c.dropBefore, c.dropAfter, c.dropNotSent = 0, 0, 0
	return before, after, notSent
}

func definitiveDialNotSent() error {
	return &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
}

func (c *dropClient) EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd {
	before, after, notSent := c.takeDrop()
	if notSent > 0 {
		return redis.NewCmdResult(nil, definitiveDialNotSent())
	}
	if before > 0 {
		return redis.NewCmdResult(nil, context.DeadlineExceeded)
	}
	cmd := c.UniversalClient.EvalSha(ctx, sha1, keys, args...)
	if after > 0 {
		// The script really executed; only the reply is lost.
		_, _ = cmd.Result()
		return redis.NewCmdResult(nil, context.DeadlineExceeded)
	}
	return cmd
}

// Process intercepts RunNoRetry's explicit EVALSHA command. Returning an
// error after delegating preserves the successful command result internally
// while simulating loss of only the client-visible response.
func (c *dropClient) Process(ctx context.Context, command redis.Cmder) error {
	before, after, notSent := c.takeDrop()
	if notSent > 0 {
		return definitiveDialNotSent()
	}
	if before > 0 {
		return context.DeadlineExceeded
	}
	err := c.UniversalClient.Process(ctx, command)
	if after > 0 && err == nil {
		return context.DeadlineExceeded
	}
	return err
}

func newDropRig(t *testing.T, drop *dropClient) *testRig {
	t.Helper()
	rig := newTestRig(t)
	// Rebuild the store on top of the dropping client, sharing the claimed
	// shard authority state in the same miniredis instance.
	drop.UniversalClient = rig.client
	store, err := NewStore(drop, nil, Options{
		Profile:  testProfile(t),
		Resolver: testCalculator(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	// Preload every script so a dropped reply can only hide an execution that
	// truly happened; otherwise the first NOSCRIPT would mask "before apply".
	if err := store.Registry().EnsureLoaded(context.Background()); err != nil {
		t.Fatal(err)
	}
	rig.store = store
	return rig
}

func TestCommitResponseLostAfterApplyResolvesCommitted(t *testing.T) {
	drop := &dropClient{}
	rig := newDropRig(t, drop)
	loaded := rig.load(t, "claim-c1")
	req := rig.commitReq("claim-c1", loaded.RecordVersion, "unknown-after",
		authoritystore.ModelState{"crop": []byte("remaining=8")})

	drop.mu.Lock()
	drop.dropAfter = 1
	drop.mu.Unlock()
	got, err := rig.store.Commit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != authoritystore.CommitTimeoutUnknown {
		t.Fatalf("status=%s want TIMEOUT_UNKNOWN", got.Status)
	}

	// A new writer takes over and resolves through FenceAndClaim.
	fenceResult := rig.fenceAndClaim(t, "claim-c1", "claim-c2", "resolve-after", req, loaded)
	if fenceResult.Status != authoritystore.FenceResolutionCommitted {
		t.Fatalf("fence=%s want COMMITTED (script applied before reply was lost)", fenceResult.Status)
	}
	if string(fenceResult.Outcome.Payload) != "ok:unknown-after" {
		t.Fatalf("outcome=%+v", fenceResult.Outcome)
	}
	if fenceResult.Resolution.OriginalReceiptID != "receipt-unknown-after" {
		t.Fatalf("resolution=%+v", fenceResult.Resolution)
	}
}

func TestCommitTimeoutBeforeApplyResolvesNotCommitted(t *testing.T) {
	drop := &dropClient{}
	rig := newDropRig(t, drop)
	loaded := rig.load(t, "claim-c1")
	req := rig.commitReq("claim-c1", loaded.RecordVersion, "unknown-before",
		authoritystore.ModelState{"crop": []byte("remaining=8")})

	drop.mu.Lock()
	drop.dropBefore = 1
	drop.mu.Unlock()
	got, err := rig.store.Commit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != authoritystore.CommitTimeoutUnknown {
		t.Fatalf("status=%s want TIMEOUT_UNKNOWN", got.Status)
	}

	fenceResult := rig.fenceAndClaim(t, "claim-c1", "claim-c2", "resolve-before", req, loaded)
	if fenceResult.Status != authoritystore.FenceResolutionNotCommitted {
		t.Fatalf("fence=%s want NOT_COMMITTED (script never executed)", fenceResult.Status)
	}
}

func TestFenceResolutionIsStableAcrossRetryAndNewCommits(t *testing.T) {
	drop := &dropClient{}
	rig := newDropRig(t, drop)
	loaded := rig.load(t, "claim-c1")
	req := rig.commitReq("claim-c1", loaded.RecordVersion, "fence-timeout",
		authoritystore.ModelState{"crop": []byte("remaining=8")})

	// Original commit times out after apply.
	drop.mu.Lock()
	drop.dropAfter = 1
	drop.mu.Unlock()
	if got, err := rig.store.Commit(context.Background(), req); err != nil || got.Status != authoritystore.CommitTimeoutUnknown {
		t.Fatalf("commit=%+v err=%v", got, err)
	}

	// The fence resolution itself loses its reply after applying.
	drop.mu.Lock()
	drop.dropAfter = 1
	drop.mu.Unlock()
	fenceReq := rig.fenceReq("claim-c1", "claim-c2", "resolve-stable", req, loaded)
	lost, err := rig.store.FenceAndClaim(context.Background(), fenceReq)
	if err != nil {
		t.Fatal(err)
	}
	if lost.Status != authoritystore.FenceResolutionTimeoutUnknown {
		t.Fatalf("fence lost=%s", lost.Status)
	}

	// Retry of the same resolve operation must replay the persisted resolution.
	retry, err := rig.store.FenceAndClaim(context.Background(), fenceReq)
	if err != nil {
		t.Fatal(err)
	}
	if retry.Status != authoritystore.FenceResolutionCommitted ||
		retry.Resolution.ResolveOperationID != "resolve-stable" {
		t.Fatalf("retry=%+v", retry)
	}

	// The new writer commits later work; the original fence retry must still
	// return the original resolution, not drift with new writes.
	later := rig.commitReq("claim-c2", retry.Snapshot.RecordVersion, "c2-later",
		authoritystore.ModelState{"crop": []byte("remaining=7")})
	later.IdempotencyKey.SubjectPlayerID = 10001
	later.Receipt.IdempotencyKey = later.IdempotencyKey
	if _, err := rig.store.Commit(context.Background(), later); err != nil {
		t.Fatal(err)
	}
	again, err := rig.store.FenceAndClaim(context.Background(), fenceReq)
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != authoritystore.FenceResolutionCommitted ||
		again.Resolution.CutRecordVersion != retry.Resolution.CutRecordVersion {
		t.Fatalf("resolution drifted: %+v vs %+v", again.Resolution, retry.Resolution)
	}

	// Same resolve operation id with different parameters: reconcile.
	mutated := fenceReq
	mutated.BeforeRecordVersion++
	conflict, err := rig.store.FenceAndClaim(context.Background(), mutated)
	if err != nil {
		t.Fatal(err)
	}
	if conflict.Status != authoritystore.FenceResolutionReconcileRequired {
		t.Fatalf("mutated=%s", conflict.Status)
	}
}

func TestOldWriterLateWriteFailsAfterFence(t *testing.T) {
	drop := &dropClient{}
	rig := newDropRig(t, drop)
	loaded := rig.load(t, "claim-c1")
	req := rig.commitReq("claim-c1", loaded.RecordVersion, "fence-before-apply",
		authoritystore.ModelState{"crop": []byte("remaining=8")})

	drop.mu.Lock()
	drop.dropBefore = 1
	drop.mu.Unlock()
	if got, err := rig.store.Commit(context.Background(), req); err != nil || got.Status != authoritystore.CommitTimeoutUnknown {
		t.Fatalf("commit=%+v err=%v", got, err)
	}

	fenceResult := rig.fenceAndClaim(t, "claim-c1", "claim-c2", "resolve-late", req, loaded)
	if fenceResult.Status != authoritystore.FenceResolutionNotCommitted {
		t.Fatalf("fence=%s", fenceResult.Status)
	}

	// The old writer's late retry arrives after the fence: it must fail, and
	// the already-persisted NOT_COMMITTED resolution must not flip.
	late, err := rig.store.Commit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if late.Status != authoritystore.CommitWriterClaimMismatch {
		t.Fatalf("late commit=%s want WRITER_CLAIM_MISMATCH", late.Status)
	}
	replay, err := rig.store.FenceAndClaim(context.Background(), rig.fenceReq("claim-c1", "claim-c2", "resolve-late", req, loaded))
	if err != nil {
		t.Fatal(err)
	}
	if replay.Status != authoritystore.FenceResolutionNotCommitted {
		t.Fatalf("resolution flipped to %s after late write attempt", replay.Status)
	}
}

func TestNewSourceOrderTerminalScriptsResponseLossApplyExactlyOnce(t *testing.T) {
	for _, path := range []sourceOrderTerminalPath{terminalClaim, terminalComplete, terminalDirectComplete, terminalReschedule} {
		t.Run(string(path), func(t *testing.T) {
			drop := &dropClient{}
			rig := newDropRig(t, drop)
			payload := []byte{0x00, 0xff, 'l', 'o', 's', 't'}
			order := rig.setupOrderWithPayload(t, "response-lost-"+string(path), testNow.UnixMilli(), payload)
			call := prepareSourceOrderTerminalCall(t, rig, order, path)

			drop.mu.Lock()
			drop.dropAfter = 1
			drop.mu.Unlock()
			status, err := call()
			if err != nil || status != WorkLeaseTimeoutUnknown {
				t.Fatalf("response loss status=%s err=%v", status, err)
			}
			assertSourceOrderTerminalAccounting(t, rig, order, 0, 0, false)

			replay, err := call()
			if err != nil || replay != sourceOrderTerminalReplayStatus(path) {
				t.Fatalf("response-loss replay=%s want=%s err=%v", replay, sourceOrderTerminalReplayStatus(path), err)
			}
			assertSourceOrderTerminalAccounting(t, rig, order, 0, 0, false)
		})
	}
}

func TestNewSourceOrderTerminalScriptsDefinitiveNotSentDoNotMutate(t *testing.T) {
	for _, path := range []sourceOrderTerminalPath{terminalClaim, terminalComplete, terminalDirectComplete, terminalReschedule} {
		t.Run(string(path), func(t *testing.T) {
			drop := &dropClient{}
			rig := newDropRig(t, drop)
			payload := []byte{0x00, 0xff, 'n', 'o', 't', '-', 's', 'e', 'n', 't'}
			order := rig.setupOrderWithPayload(t, "not-sent-"+string(path), testNow.UnixMilli(), payload)
			call := prepareSourceOrderTerminalCall(t, rig, order, path)

			drop.mu.Lock()
			drop.dropNotSent = 1
			drop.mu.Unlock()
			status, err := call()
			if err != nil || status != WorkLeaseUnavailableNotSent {
				t.Fatalf("not-sent status=%s err=%v", status, err)
			}
			assertSourceOrderTerminalAccounting(t, rig, order, 1, int64(len(payload)), true)

			applied, err := call()
			if err != nil || applied != sourceOrderTerminalAppliedStatus(path) {
				t.Fatalf("post-not-sent apply=%s want=%s err=%v", applied, sourceOrderTerminalAppliedStatus(path), err)
			}
			assertSourceOrderTerminalAccounting(t, rig, order, 0, 0, false)
		})
	}
}

func TestNewSourceOrderTerminalCommandsDisableAutomaticRedisRetries(t *testing.T) {
	for _, path := range []sourceOrderTerminalPath{terminalClaim, terminalComplete, terminalDirectComplete, terminalReschedule} {
		t.Run(string(path), func(t *testing.T) {
			rig := newTestRig(t)
			client := &noRetryInspectClient{UniversalClient: rig.client, allNoRetry: true}
			store, err := NewStore(client, nil, Options{Profile: testProfile(t), Resolver: testCalculator(t)})
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Registry().EnsureLoaded(context.Background()); err != nil {
				t.Fatal(err)
			}
			rig.store = store
			order := rig.setupOrder(t, "terminal-no-retry-"+string(path), testNow.UnixMilli())
			call := prepareSourceOrderTerminalCall(t, rig, order, path)
			client.resetObservation()

			if status, err := call(); err != nil || status != sourceOrderTerminalAppliedStatus(path) {
				t.Fatalf("terminal status=%s err=%v", status, err)
			}
			if commands, allNoRetry := client.observation(); commands != 1 || !allNoRetry {
				t.Fatalf("commands=%d all_no_retry=%t", commands, allNoRetry)
			}
		})
	}
}

func sourceOrderTerminalAppliedStatus(path sourceOrderTerminalPath) WorkLeaseStatus {
	if path == terminalClaim || path == terminalReschedule {
		return WorkLeaseReconcileRequired
	}
	return WorkLeaseCompleted
}

func sourceOrderTerminalReplayStatus(path sourceOrderTerminalPath) WorkLeaseStatus {
	if path == terminalClaim || path == terminalReschedule {
		return WorkLeaseFenceMismatch
	}
	return WorkLeaseAlreadyCompleted
}

func assertSourceOrderTerminalAccounting(t *testing.T, rig *testRig, order authoritystore.AttachedOrder, wantCount, wantBytes int64, wantDue bool) {
	t.Helper()
	snapshot, err := rig.store.WorkLease().GetSourceOrder(context.Background(), rig.bucket, order.OrderType, order.OrderID)
	if err != nil {
		t.Fatal(err)
	}
	if wantDue && snapshot.State != "PENDING" && snapshot.State != "REWARD_DISPATCHING" {
		t.Fatalf("not-sent call changed state to %q", snapshot.State)
	}
	if !wantDue && snapshot.State != "COMPLETED" && snapshot.State != "RECONCILE_REQUIRED" {
		t.Fatalf("terminal call left nonterminal state %q", snapshot.State)
	}
	count, bytes := sourceOrderBucketUsage(t, rig)
	if count != wantCount || bytes != wantBytes {
		t.Fatalf("bucket accounting=(%d,%d), want=(%d,%d)", count, bytes, wantCount, wantBytes)
	}
	_, _, dueKey, _, workRef, err := rig.store.WorkLease().keysFor(rig.bucket, order.OrderType, order.OrderID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = rig.client.ZScore(context.Background(), dueKey.String(), workRef).Result()
	if wantDue && err != nil {
		t.Fatalf("due member missing: %v", err)
	}
	if !wantDue && !errors.Is(err, redis.Nil) {
		t.Fatalf("terminal due member remains: %v", err)
	}
}

func TestWriteTransportClassification(t *testing.T) {
	if classifyWriteError(syscall.ECONNREFUSED) != fateUnknown {
		t.Fatal("phase-less ECONNREFUSED must remain unknown")
	}
	for name, err := range map[string]error{
		"dns":                 &net.OpError{Op: "dial", Net: "tcp", Err: &net.DNSError{Err: "no such host", Name: "redis.invalid"}},
		"network unreachable": &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ENETUNREACH},
		"host unreachable":    &net.OpError{Op: "dial", Net: "tcp", Err: syscall.EHOSTUNREACH},
		"connection refused":  &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED},
	} {
		if classifyWriteError(err) != fateNotSent {
			t.Fatalf("%s dial failure must be not-sent: %v", name, err)
		}
	}
	if classifyWriteError(&net.OpError{Op: "write", Net: "tcp", Err: syscall.EHOSTUNREACH}) != fateUnknown {
		t.Fatal("post-dial host-unreachable write must remain unknown")
	}
	if classifyWriteError(&net.OpError{Op: "write", Net: "tcp", Err: syscall.ECONNREFUSED}) != fateUnknown {
		t.Fatal("post-dial connection-refused write must remain unknown")
	}
	if classifyWriteError(context.DeadlineExceeded) != fateUnknown {
		t.Fatal("deadline must be unknown")
	}
	if !isContractViolation(errors.New("ERR CROSSSLOT Keys in request don't hash to the same slot")) {
		t.Fatal("CROSSSLOT must be a contract violation")
	}
}

// ── helpers ─────────────────────────────────────────────────────

func (r *testRig) fenceReq(oldClaim, newClaim, resolveOp string, original authoritystore.AuthorityCommitRequest, before authoritystore.AuthoritySnapshot) authoritystore.FenceAndClaimRequest {
	return authoritystore.FenceAndClaimRequest{
		ResolveOperationID:        resolveOp,
		ActorAddress:              r.addr,
		AuthorityLayoutVersion:    "v1",
		ShardWriteFence:           r.fence,
		OldWriterClaimID:          oldClaim,
		NewWriterClaimID:          newClaim,
		OriginalIdempotencyKey:    original.IdempotencyKey,
		CanonicalPayloadHash:      original.CanonicalPayloadHash,
		BeforeRecordVersion:       before.RecordVersion,
		BeforeBusinessStateDigest: before.BusinessStateDigest,
	}
}

func (r *testRig) fenceAndClaim(t *testing.T, oldClaim, newClaim, resolveOp string, original authoritystore.AuthorityCommitRequest, before authoritystore.AuthoritySnapshot) authoritystore.FenceAndClaimResult {
	t.Helper()
	result, err := r.store.FenceAndClaim(context.Background(), r.fenceReq(oldClaim, newClaim, resolveOp, original, before))
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestNoScriptAutoRecovery(t *testing.T) {
	// Fresh server: no script is cached at all, so the first call exercises
	// the NOSCRIPT recovery path end to end.
	server := newTestClientOnly(t)
	registry, err := NewScriptRegistry(server)
	if err != nil {
		t.Fatal(err)
	}
	// EVALSHA against an empty script cache must surface NOSCRIPT, which the
	// registry recovers from transparently.
	reply, err := registry.Run(context.Background(), ScriptPublishResultEntry,
		[]string{"classic-farm:test:z1:proj:result:v2:10001"}, "field", "value", "64")
	if err != nil {
		t.Fatal(err)
	}
	if len(reply) == 0 || reply[0] != "PUBLISHED" {
		t.Fatalf("reply=%v", reply)
	}
}
