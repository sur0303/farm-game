package authority

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/workidentity"
)

func completionProofFor(rig *testRig, order authoritystore.AttachedOrder) WorkCompletionProof {
	payloadHash := sha256.Sum256(order.Payload)
	kind, domain := "PLAYER_ACTOR", "PLAYER"
	proof := WorkCompletionProof{
		SourceAuthorityKind: kind, CanonicalSourceAuthorityID: fmt.Sprint(rig.addr.ActorID),
		AuthorityLayoutVersion: rig.bucket.LayoutVersion(), ActorRouteDomain: domain,
		PayloadHash: fmt.Sprintf("%x", payloadHash[:]), InvocationID: "invocation",
		ParticipantStep: "APPLY_CARE_REWARD", ParticipantOutcome: "APPLIED",
		ParticipantResultHash: fmt.Sprintf("%064x", 1), ParticipantResultVersion: 1,
	}
	proof.ResultProofHash = WorkCompletionProofHash(proof)
	return proof
}

// setupOrder commits one due order and returns it.
func (r *testRig) setupOrder(t *testing.T, operationID string, dueAtMs int64) authoritystore.AttachedOrder {
	t.Helper()
	return r.setupOrderWithPayload(t, operationID, dueAtMs, []byte("payload:"+operationID))
}

func (r *testRig) setupOrderWithPayload(t *testing.T, operationID string, dueAtMs int64, payload []byte) authoritystore.AttachedOrder {
	t.Helper()
	loaded := r.load(t, "claim-w1")
	order := dueOrder(t, "CARE_REWARD", operationID, dueAtMs)
	order.Payload = append([]byte(nil), payload...)
	req := r.commitReq("claim-w1", loaded.RecordVersion, operationID,
		authoritystore.ModelState{"economy": []byte("x")})
	req.AttachedOrders = []authoritystore.AttachedOrder{order}
	if _, err := r.store.Commit(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	return order
}

func sourceOrderBucketUsage(t *testing.T, rig *testRig) (int64, int64) {
	t.Helper()
	metaKey, err := rig.store.keys.BucketMeta(rig.bucket)
	if err != nil {
		t.Fatal(err)
	}
	values, err := rig.client.HMGet(context.Background(), metaKey.String(), "pending_work_count", "pending_work_bytes").Result()
	if err != nil {
		t.Fatal(err)
	}
	parse := func(name string, value interface{}) int64 {
		text, ok := value.(string)
		if !ok {
			t.Fatalf("%s has non-string value %T", name, value)
		}
		parsed, parseErr := strconv.ParseInt(text, 10, 64)
		if parseErr != nil {
			t.Fatalf("%s=%q: %v", name, text, parseErr)
		}
		return parsed
	}
	return parse("pending_work_count", values[0]), parse("pending_work_bytes", values[1])
}

type sourceOrderTerminalPath string

const (
	terminalClaim          sourceOrderTerminalPath = "claim"
	terminalComplete       sourceOrderTerminalPath = "complete"
	terminalDirectComplete sourceOrderTerminalPath = "direct-complete"
	terminalReschedule     sourceOrderTerminalPath = "reschedule"
)

func prepareSourceOrderTerminalCall(t *testing.T, rig *testRig, order authoritystore.AttachedOrder, path sourceOrderTerminalPath) func() (WorkLeaseStatus, error) {
	t.Helper()
	lease := rig.store.WorkLease()
	switch path {
	case terminalClaim:
		_, workKey, _, _, _, err := lease.keysFor(rig.bucket, order.OrderType, order.OrderID)
		if err != nil {
			t.Fatal(err)
		}
		if err := rig.client.HSet(context.Background(), workKey.String(),
			"retry_count", strconv.FormatUint(rig.store.opts.WorkMaxRetryCount, 10)).Err(); err != nil {
			t.Fatal(err)
		}
		return func() (WorkLeaseStatus, error) {
			result, callErr := lease.ClaimWorkWithBoot(context.Background(), rig.bucket, order.OrderType, order.OrderID,
				"terminal-worker", "terminal-boot", "terminal-token", 30_000)
			switch result.Status {
			case WorkClaimReconcileRequired:
				return WorkLeaseReconcileRequired, callErr
			case WorkClaimNotClaimable:
				return WorkLeaseFenceMismatch, callErr
			case WorkClaimUnavailableNotSent:
				return WorkLeaseUnavailableNotSent, callErr
			case WorkClaimTimeoutUnknown:
				return WorkLeaseTimeoutUnknown, callErr
			default:
				return WorkLeaseUnknown, callErr
			}
		}
	case terminalComplete, terminalReschedule:
		claim, err := lease.ClaimWorkWithBoot(context.Background(), rig.bucket, order.OrderType, order.OrderID,
			"terminal-worker", "terminal-boot", "terminal-token", 30_000)
		if err != nil || claim.Status != WorkClaimClaimed {
			t.Fatalf("prepare %s claim=%+v err=%v", path, claim, err)
		}
		if path == terminalComplete {
			proof := completionProofFor(rig, order)
			return func() (WorkLeaseStatus, error) {
				result, callErr := lease.CompleteWorkWithProof(context.Background(), rig.bucket, order.OrderType, order.OrderID,
					"terminal-worker", "terminal-boot", claim.LeaseEpoch, claim.WorkVersion, "terminal-token", proof)
				return result.Status, callErr
			}
		}
		return func() (WorkLeaseStatus, error) {
			result, callErr := lease.RescheduleWork(context.Background(), rig.bucket, order.OrderType, order.OrderID,
				"terminal-worker", "terminal-boot", claim.LeaseEpoch, claim.WorkVersion, "terminal-token",
				rig.store.opts.WorkMaxRetryCount, 1_000, "TERMINAL_TEST")
			return result.Status, callErr
		}
	case terminalDirectComplete:
		proof := completionProofFor(rig, order)
		return func() (WorkLeaseStatus, error) {
			result, callErr := lease.ClaimAndCompleteWithProof(context.Background(), rig.bucket, order.OrderType, order.OrderID,
				"terminal-worker", "terminal-boot", "terminal-token", 30_000, proof)
			return result.Status, callErr
		}
	default:
		t.Fatalf("unknown terminal path %q", path)
		return nil
	}
}

func TestCareSourceOrderAndDueIndexAreCreatedByOneCommit(t *testing.T) {
	rig := newTestRig(t)
	loaded := rig.load(t, "commit-care-order")
	order := dueOrder(t, "CARE_REWARD", "commit-care-order", testNow.UnixMilli())
	req := rig.commitReq("commit-care-order", loaded.RecordVersion, "commit-care-order", authoritystore.ModelState{"care": []byte("resolved")})
	req.AttachedOrders = []authoritystore.AttachedOrder{order}
	result, err := rig.store.Commit(context.Background(), req)
	if err != nil || result.Status != authoritystore.CommitCommitted {
		t.Fatalf("Care authority commit=%+v err=%v", result, err)
	}
	snapshot, err := rig.store.WorkLease().GetSourceOrder(context.Background(), rig.bucket, order.OrderType, order.OrderID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.State != "PENDING" || snapshot.WorkID != order.OrderID || snapshot.TypedPayload == nil || snapshot.NextDueAtMs != order.DueAtMs {
		t.Fatalf("source order snapshot=%+v", snapshot)
	}
	dueAt, found, err := rig.store.WorkLease().DueIndexScore(context.Background(), rig.bucket, order.OrderType, order.OrderID)
	if err != nil || !found || dueAt != order.DueAtMs {
		t.Fatalf("source due index score=%d found=%v err=%v", dueAt, found, err)
	}
	if snapshot.PayloadHash == "" {
		t.Fatal("Care SourceOrder payload_hash was not persisted")
	}
	if snapshot.LeaseHolderID != "" || snapshot.LeaseEpoch != 0 || snapshot.LeaseExpireAtMs != 0 || snapshot.CompletedAtMs != 0 {
		t.Fatalf("pending order leaked idle lease=%+v", snapshot)
	}
	orderKey, err := rig.store.keys.SourceOrder(rig.bucket, order.OrderType, order.OrderID)
	if err != nil {
		t.Fatal(err)
	}
	fields, err := rig.client.HGetAll(context.Background(), orderKey.String()).Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"lease_holder_id", "lease_holder_boot_id", "lease_epoch", "lease_token_hash",
		"lease_expire_at_ms", "current_step", "last_error_class", "completed_at_ms",
		"reconcile_required_at_ms",
	} {
		if _, ok := fields[name]; ok {
			t.Fatalf("pending order still stores idle field %s: %+v", name, fields)
		}
	}
}

func TestVerifySourceOrderAdmissionSingleRTT(t *testing.T) {
	rig := newTestRig(t)
	loaded := rig.load(t, "verify-care-order")
	order := dueOrder(t, "CARE_REWARD", "verify-care-order", testNow.UnixMilli())
	req := rig.commitReq("verify-care-order", loaded.RecordVersion, "verify-care-order", authoritystore.ModelState{"care": []byte("resolved")})
	req.AttachedOrders = []authoritystore.AttachedOrder{order}
	result, err := rig.store.Commit(context.Background(), req)
	if err != nil || result.Status != authoritystore.CommitCommitted {
		t.Fatalf("authority commit=%+v err=%v", result, err)
	}
	ev, err := rig.store.WorkLease().VerifySourceOrderAdmission(context.Background(), rig.bucket, order.OrderType, order.OrderID)
	if err != nil {
		t.Fatal(err)
	}
	if !ev.Found || ev.WorkID != order.OrderID || ev.WorkType != order.OrderType || ev.State != "PENDING" ||
		ev.WorkVersion == 0 || ev.PayloadHash == "" || ev.SubjectPlayerID == 0 || ev.OperationID == "" ||
		ev.NextDueAtMs != order.DueAtMs || ev.DueScoreMs != order.DueAtMs || ev.TrustedNowMs <= 0 ||
		ev.PlacementKind == "" || ev.BucketVersion != 1 || ev.SourceAuthorityKind == "" || ev.ActorRouteDomain == "" {
		t.Fatalf("admission evidence mismatch=%+v", ev)
	}
	if ev.LeaseHolderID != "" || ev.LeaseEpoch != 0 || ev.LeaseExpireAtMs != 0 {
		t.Fatalf("pending order leaked idle lease into evidence=%+v", ev)
	}
	// Missing order must report Found=false without error.
	missingID, err := workidentity.StableOpaqueID("testwork", order.OrderType, "missing-order")
	if err != nil {
		t.Fatal(err)
	}
	missing, err := rig.store.WorkLease().VerifySourceOrderAdmission(context.Background(), rig.bucket, order.OrderType, missingID)
	if err != nil {
		t.Fatal(err)
	}
	if missing.Found {
		t.Fatalf("missing order reported found=%+v", missing)
	}
	// Idle lease fields written by ClaimWork must surface in evidence.
	claim, err := rig.store.WorkLease().ClaimWork(context.Background(), rig.bucket, order.OrderType, order.OrderID,
		"worker-1", "token-1", 30_000)
	if err != nil || claim.Status != WorkClaimClaimed {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	held, err := rig.store.WorkLease().VerifySourceOrderAdmission(context.Background(), rig.bucket, order.OrderType, order.OrderID)
	if err != nil {
		t.Fatal(err)
	}
	if !held.Found || held.State != "REWARD_DISPATCHING" || held.LeaseHolderID != "worker-1" ||
		held.LeaseEpoch != claim.LeaseEpoch || held.LeaseExpireAtMs <= 0 || held.NextDueAtMs != held.LeaseExpireAtMs {
		t.Fatalf("claimed evidence mismatch=%+v claim=%+v", held, claim)
	}
}

func TestWorkLeaseClaimRenewCompleteLifecycle(t *testing.T) {
	rig := newTestRig(t)
	order := rig.setupOrder(t, "work-op-1", testNow.UnixMilli())
	lease := rig.store.WorkLease()

	// Due now: claim succeeds and returns the work facts.
	claim, err := lease.ClaimWork(context.Background(), rig.bucket, order.OrderType, order.OrderID, "worker-1", "token-1", 30_000)
	if err != nil {
		t.Fatal(err)
	}
	if claim.Status != WorkClaimClaimed || claim.LeaseEpoch != 1 || claim.WorkVersion != 1 ||
		string(claim.Payload) != "payload:work-op-1" || claim.OperationID != "work-op-1" ||
		claim.SubjectPlayerID != 10001 {
		t.Fatalf("claim=%+v", claim)
	}

	// Second worker cannot steal a held lease.
	held, err := lease.ClaimWork(context.Background(), rig.bucket, order.OrderType, order.OrderID, "worker-2", "token-2", 30_000)
	if err != nil {
		t.Fatal(err)
	}
	if held.Status != WorkClaimLeaseHeld {
		t.Fatalf("held=%s", held.Status)
	}
	badProof := completionProofFor(rig, order)
	badProof.ResultProofHash = fmt.Sprintf("%064x", 99)
	if _, err := lease.CompleteWorkWithProof(context.Background(), rig.bucket, order.OrderType, order.OrderID, "worker-1", "worker-1", 1, 1, "token-1", badProof); err == nil {
		t.Fatal("malformed Participant result proof was accepted")
	}

	// Wrong holder/token cannot renew or complete.
	badRenew, err := lease.RenewWorkLease(context.Background(), rig.bucket, order.OrderType, order.OrderID, "worker-2", 1, "token-2", 30_000)
	if err != nil {
		t.Fatal(err)
	}
	if badRenew.Status != WorkLeaseFenceMismatch {
		t.Fatalf("badRenew=%s", badRenew.Status)
	}
	badComplete, err := lease.CompleteWorkWithProof(context.Background(), rig.bucket, order.OrderType, order.OrderID, "worker-2", "worker-2", 1, 1, "token-2", completionProofFor(rig, order))
	if err != nil {
		t.Fatal(err)
	}
	if badComplete.Status != WorkLeaseFenceMismatch {
		t.Fatalf("badComplete=%s", badComplete.Status)
	}

	// Owner renews, then completes; due index entry is removed atomically.
	renew, err := lease.RenewWorkLease(context.Background(), rig.bucket, order.OrderType, order.OrderID, "worker-1", 1, "token-1", 30_000)
	if err != nil {
		t.Fatal(err)
	}
	if renew.Status != WorkLeaseRenewed {
		t.Fatalf("renew=%s", renew.Status)
	}
	complete, err := lease.CompleteWorkWithProof(context.Background(), rig.bucket, order.OrderType, order.OrderID, "worker-1", "worker-1", 1, 1, "token-1", completionProofFor(rig, order))
	if err != nil {
		t.Fatal(err)
	}
	if complete.Status != WorkLeaseCompleted {
		t.Fatalf("complete=%s", complete.Status)
	}
	orderKey, err := rig.store.keys.SourceOrder(rig.bucket, order.OrderType, order.OrderID)
	if err != nil {
		t.Fatal(err)
	}
	fields, err := rig.client.HGetAll(context.Background(), orderKey.String()).Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"lease_holder_id", "lease_holder_boot_id", "lease_epoch",
		"lease_token_hash", "lease_expire_at_ms",
	} {
		if _, ok := fields[name]; ok {
			t.Fatalf("completed order still stores idle lease field %s: %+v", name, fields)
		}
	}
	if fields["state"] != "COMPLETED" || fields["completed_at_ms"] == "" || fields["completed_at_ms"] == "0" {
		t.Fatalf("completed order identity=%+v", fields)
	}
	dueKey, err := rig.store.keys.SourceDueIndex(rig.bucket)
	if err != nil {
		t.Fatal(err)
	}
	workRef, err := workRefOf(order)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rig.client.ZScore(context.Background(), dueKey.String(), workRef).Result(); err == nil {
		t.Fatal("completed work must leave the due index")
	}

	// Completion is idempotent-ish for the same lease owner.
	again, err := lease.CompleteWorkWithProof(context.Background(), rig.bucket, order.OrderType, order.OrderID, "worker-1", "worker-1", 1, 1, "token-1", completionProofFor(rig, order))
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != WorkLeaseAlreadyCompleted {
		t.Fatalf("again=%s", again.Status)
	}
}

func TestWorkLeaseClaimAndCompleteWithProof(t *testing.T) {
	rig := newTestRig(t)
	order := rig.setupOrder(t, "work-op-claim-complete", testNow.UnixMilli())
	lease := rig.store.WorkLease()
	proof := completionProofFor(rig, order)

	held, err := lease.ClaimWork(context.Background(), rig.bucket, order.OrderType, order.OrderID, "worker-held", "token-held", 30_000)
	if err != nil || held.Status != WorkClaimClaimed {
		t.Fatalf("held claim=%+v err=%v", held, err)
	}
	blocked, err := lease.ClaimAndCompleteWithProof(context.Background(), rig.bucket, order.OrderType, order.OrderID,
		"direct-1", "direct-1", "direct-token", 30_000, proof)
	if err != nil {
		t.Fatal(err)
	}
	if blocked.Status != WorkLeaseFenceMismatch {
		t.Fatalf("live worker lease status=%s", blocked.Status)
	}

	staleAt := testNow.Add(31 * time.Second)
	rig.server.SetTime(staleAt)
	orderKey, err := rig.store.keys.SourceOrder(rig.bucket, order.OrderType, order.OrderID)
	if err != nil {
		t.Fatal(err)
	}
	if err := rig.client.HSet(context.Background(), orderKey.String(), "deadline_at_ms", strconv.FormatInt(staleAt.UnixMilli()-1, 10)).Err(); err != nil {
		t.Fatal(err)
	}
	complete, err := lease.ClaimAndCompleteWithProof(context.Background(), rig.bucket, order.OrderType, order.OrderID,
		"direct-1", "direct-1", "direct-token", 30_000, proof)
	if err != nil {
		t.Fatal(err)
	}
	if complete.Status != WorkLeaseCompleted {
		t.Fatalf("complete=%s", complete.Status)
	}
	orderKey, err = rig.store.keys.SourceOrder(rig.bucket, order.OrderType, order.OrderID)
	if err != nil {
		t.Fatal(err)
	}
	fields, err := rig.client.HGetAll(context.Background(), orderKey.String()).Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"lease_holder_id", "lease_holder_boot_id", "lease_epoch",
		"lease_token_hash", "lease_expire_at_ms",
	} {
		if _, ok := fields[name]; ok {
			t.Fatalf("completed order still stores idle lease field %s: %+v", name, fields)
		}
	}
	if fields["state"] != "COMPLETED" {
		t.Fatalf("completed order identity=%+v", fields)
	}
	again, err := lease.ClaimAndCompleteWithProof(context.Background(), rig.bucket, order.OrderType, order.OrderID,
		"direct-1", "direct-1", "direct-token", 30_000, proof)
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != WorkLeaseAlreadyCompleted {
		t.Fatalf("again=%s", again.Status)
	}
}

func TestSourceOrderTerminalPathsDecrementExactPayloadAccountingOnce(t *testing.T) {
	payload := []byte{0x00, 0xff, 'p', 0x00, 0x80, 'a', 'y'}
	for _, path := range []sourceOrderTerminalPath{terminalClaim, terminalComplete, terminalDirectComplete, terminalReschedule} {
		t.Run(string(path), func(t *testing.T) {
			rig := newTestRig(t)
			order := rig.setupOrderWithPayload(t, "accounting-"+string(path), testNow.UnixMilli(), payload)
			call := prepareSourceOrderTerminalCall(t, rig, order, path)

			count, bytes := sourceOrderBucketUsage(t, rig)
			if count != 1 || bytes != int64(len(payload)) {
				t.Fatalf("initial bucket accounting=(%d,%d), want (1,%d)", count, bytes, len(payload))
			}
			status, err := call()
			if err != nil {
				t.Fatal(err)
			}
			wantStatus := WorkLeaseCompleted
			if path == terminalClaim || path == terminalReschedule {
				wantStatus = WorkLeaseReconcileRequired
			}
			if status != wantStatus {
				t.Fatalf("terminal status=%s want=%s", status, wantStatus)
			}
			count, bytes = sourceOrderBucketUsage(t, rig)
			if count != 0 || bytes != 0 {
				t.Fatalf("terminal bucket accounting=(%d,%d), want (0,0)", count, bytes)
			}

			replayStatus, err := call()
			if err != nil {
				t.Fatal(err)
			}
			if path == terminalClaim || path == terminalReschedule {
				if replayStatus != WorkLeaseFenceMismatch {
					t.Fatalf("terminal reconcile replay path=%s status=%s", path, replayStatus)
				}
			} else if replayStatus != WorkLeaseAlreadyCompleted {
				t.Fatalf("completion replay=%s", replayStatus)
			}
			count, bytes = sourceOrderBucketUsage(t, rig)
			if count != 0 || bytes != 0 {
				t.Fatalf("replay changed bucket accounting=(%d,%d)", count, bytes)
			}
		})
	}
}

func TestSourceOrderTerminalAccountingCorruptionLeavesRecordAndDueIndexUntouched(t *testing.T) {
	corruptions := []struct {
		name  string
		field string
		value string
		del   bool
	}{
		{name: "count-malformed", field: "pending_work_count", value: "1.5"},
		{name: "count-underflow", field: "pending_work_count", value: "0"},
		{name: "bytes-malformed", field: "pending_work_bytes", value: "not-a-counter"},
		{name: "bytes-underflow", field: "pending_work_bytes", value: "6"},
		{name: "payload-missing", field: "typed_payload", del: true},
	}
	for _, path := range []sourceOrderTerminalPath{terminalClaim, terminalComplete, terminalDirectComplete, terminalReschedule} {
		for _, corruption := range corruptions {
			t.Run(string(path)+"/"+corruption.name, func(t *testing.T) {
				rig := newTestRig(t)
				payload := []byte{0x00, 0xff, 'e', 'x', 'a', 'c', 't'}
				order := rig.setupOrderWithPayload(t, "corrupt-"+string(path)+"-"+corruption.name, testNow.UnixMilli(), payload)
				call := prepareSourceOrderTerminalCall(t, rig, order, path)
				_, workKey, dueKey, metaKey, workRef, err := rig.store.WorkLease().keysFor(rig.bucket, order.OrderType, order.OrderID)
				if err != nil {
					t.Fatal(err)
				}
				targetKey := metaKey.String()
				if corruption.field == "typed_payload" {
					targetKey = workKey.String()
				}
				if corruption.del {
					err = rig.client.HDel(context.Background(), targetKey, corruption.field).Err()
				} else {
					err = rig.client.HSet(context.Background(), targetKey, corruption.field, corruption.value).Err()
				}
				if err != nil {
					t.Fatal(err)
				}
				beforeRecord, err := rig.client.HGetAll(context.Background(), workKey.String()).Result()
				if err != nil {
					t.Fatal(err)
				}
				beforeMeta, err := rig.client.HGetAll(context.Background(), metaKey.String()).Result()
				if err != nil {
					t.Fatal(err)
				}
				beforeDue, err := rig.client.ZScore(context.Background(), dueKey.String(), workRef).Result()
				if err != nil {
					t.Fatalf("missing pre-call due member: %v", err)
				}

				if _, err := call(); !errors.Is(err, errMalformedBlob) {
					t.Fatalf("terminal corruption err=%v, want malformed record", err)
				}
				afterRecord, err := rig.client.HGetAll(context.Background(), workKey.String()).Result()
				if err != nil {
					t.Fatal(err)
				}
				afterMeta, err := rig.client.HGetAll(context.Background(), metaKey.String()).Result()
				if err != nil {
					t.Fatal(err)
				}
				afterDue, err := rig.client.ZScore(context.Background(), dueKey.String(), workRef).Result()
				if err != nil {
					t.Fatalf("terminal corruption removed due member: %v", err)
				}
				if !reflect.DeepEqual(afterRecord, beforeRecord) {
					t.Fatalf("terminal corruption mutated work record\nbefore=%+v\nafter=%+v", beforeRecord, afterRecord)
				}
				if !reflect.DeepEqual(afterMeta, beforeMeta) {
					t.Fatalf("terminal corruption mutated bucket meta\nbefore=%+v\nafter=%+v", beforeMeta, afterMeta)
				}
				if afterDue != beforeDue {
					t.Fatalf("terminal corruption changed due score from %v to %v", beforeDue, afterDue)
				}
			})
		}
	}
}

func TestSourceOrderTerminalWrongTypeDueIndexFailsClosed(t *testing.T) {
	for _, path := range []sourceOrderTerminalPath{terminalClaim, terminalComplete, terminalDirectComplete, terminalReschedule} {
		t.Run(string(path), func(t *testing.T) {
			rig := newTestRig(t)
			order := rig.setupOrder(t, "wrong-due-type-"+string(path), testNow.UnixMilli())
			call := prepareSourceOrderTerminalCall(t, rig, order, path)
			_, workKey, dueKey, metaKey, _, err := rig.store.WorkLease().keysFor(rig.bucket, order.OrderType, order.OrderID)
			if err != nil {
				t.Fatal(err)
			}
			if err := rig.client.Del(context.Background(), dueKey.String()).Err(); err != nil {
				t.Fatal(err)
			}
			if err := rig.client.Set(context.Background(), dueKey.String(), "wrong-type-due-index", 0).Err(); err != nil {
				t.Fatal(err)
			}
			beforeRecord, err := rig.client.HGetAll(context.Background(), workKey.String()).Result()
			if err != nil {
				t.Fatal(err)
			}
			beforeMeta, err := rig.client.HGetAll(context.Background(), metaKey.String()).Result()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := call(); !errors.Is(err, errMalformedBlob) {
				t.Fatalf("wrong-type due index err=%v", err)
			}
			afterRecord, err := rig.client.HGetAll(context.Background(), workKey.String()).Result()
			if err != nil {
				t.Fatal(err)
			}
			afterMeta, err := rig.client.HGetAll(context.Background(), metaKey.String()).Result()
			if err != nil {
				t.Fatal(err)
			}
			dueValue, err := rig.client.Get(context.Background(), dueKey.String()).Result()
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(beforeRecord, afterRecord) || !reflect.DeepEqual(beforeMeta, afterMeta) || dueValue != "wrong-type-due-index" {
				t.Fatal("wrong-type DueIndex allowed a partial terminal mutation")
			}
		})
	}
}

func TestClaimWorkV5TerminalValidatesExactDueMemberBeforeMutation(t *testing.T) {
	for _, corruption := range []struct {
		name   string
		mutate func(*testRig, string, string) error
	}{
		{name: "missing-member", mutate: func(rig *testRig, dueKey, workRef string) error {
			return rig.client.ZRem(context.Background(), dueKey, workRef).Err()
		}},
		{name: "wrong-score", mutate: func(rig *testRig, dueKey, workRef string) error {
			return rig.client.ZAdd(context.Background(), dueKey, redis.Z{Score: float64(testNow.Add(time.Second).UnixMilli()), Member: workRef}).Err()
		}},
	} {
		t.Run(corruption.name, func(t *testing.T) {
			rig := newTestRig(t)
			order := rig.setupOrder(t, "claim-v5-due-"+corruption.name, testNow.UnixMilli())
			call := prepareSourceOrderTerminalCall(t, rig, order, terminalClaim)
			_, workKey, dueKey, metaKey, workRef, err := rig.store.WorkLease().keysFor(rig.bucket, order.OrderType, order.OrderID)
			if err != nil {
				t.Fatal(err)
			}
			if err := corruption.mutate(rig, dueKey.String(), workRef); err != nil {
				t.Fatal(err)
			}
			beforeRecord, err := rig.client.HGetAll(context.Background(), workKey.String()).Result()
			if err != nil {
				t.Fatal(err)
			}
			beforeMeta, err := rig.client.HGetAll(context.Background(), metaKey.String()).Result()
			if err != nil {
				t.Fatal(err)
			}
			beforeDue, beforeDueErr := rig.client.ZScore(context.Background(), dueKey.String(), workRef).Result()

			if _, err := call(); !errors.Is(err, errMalformedBlob) {
				t.Fatalf("claim terminal due corruption err=%v", err)
			}
			afterRecord, err := rig.client.HGetAll(context.Background(), workKey.String()).Result()
			if err != nil {
				t.Fatal(err)
			}
			afterMeta, err := rig.client.HGetAll(context.Background(), metaKey.String()).Result()
			if err != nil {
				t.Fatal(err)
			}
			afterDue, afterDueErr := rig.client.ZScore(context.Background(), dueKey.String(), workRef).Result()
			beforeMissing, afterMissing := errors.Is(beforeDueErr, redis.Nil), errors.Is(afterDueErr, redis.Nil)
			if !reflect.DeepEqual(beforeRecord, afterRecord) || !reflect.DeepEqual(beforeMeta, afterMeta) ||
				beforeDue != afterDue || beforeMissing != afterMissing ||
				(beforeDueErr != nil && !beforeMissing) || (afterDueErr != nil && !afterMissing) {
				t.Fatal("ClaimWorkV5 due validation allowed a partial terminal mutation")
			}
		})
	}
}

func TestWorkLeaseClaimAndCompleteBatchUsesOnePipeline(t *testing.T) {
	rig := newTestRig(t)
	loaded := rig.load(t, "claim-batch")
	first := dueOrder(t, "CARE_REWARD", "work-op-batch-1", testNow.UnixMilli())
	second := dueOrder(t, "CARE_REWARD", "work-op-batch-2", testNow.UnixMilli())
	req := rig.commitReq("claim-batch", loaded.RecordVersion, "work-op-batch-commit", authoritystore.ModelState{"economy": []byte("batch")})
	req.AttachedOrders = []authoritystore.AttachedOrder{first, second}
	committed, err := rig.store.Commit(context.Background(), req)
	if err != nil || committed.Status != authoritystore.CommitCommitted {
		t.Fatalf("setup commit=%+v err=%v", committed, err)
	}
	if err := rig.store.registry.EnsureLoaded(context.Background()); err != nil {
		t.Fatal(err)
	}
	counter := &redisDispatchCounter{}
	rig.client.AddHook(counter)
	lease := rig.store.WorkLease()
	results := lease.ClaimAndCompleteWithProofBatch(context.Background(), []WorkClaimAndCompleteRequest{
		{Bucket: rig.bucket, WorkType: first.OrderType, WorkID: first.OrderID, HolderID: "direct-1", BootID: "direct-1", LeaseToken: "token-1", RequestedTTLms: 30_000, Proof: completionProofFor(rig, first)},
		{Bucket: rig.bucket, WorkType: second.OrderType, WorkID: second.OrderID, HolderID: "direct-1", BootID: "direct-1", LeaseToken: "token-1", RequestedTTLms: 30_000, Proof: completionProofFor(rig, second)},
	})
	if len(results) != 2 {
		t.Fatalf("results=%d", len(results))
	}
	for i, result := range results {
		if result.Err != nil || result.Result.Status != WorkLeaseCompleted {
			t.Fatalf("result[%d]=%+v", i, result)
		}
	}
	if pipelines := counter.pipeline.Load(); pipelines != 1 {
		t.Fatalf("pipeline dispatches=%d, want 1", pipelines)
	}
	count, bytes := sourceOrderBucketUsage(t, rig)
	if count != 0 || bytes != 0 {
		t.Fatalf("batch terminal accounting=(%d,%d), want (0,0)", count, bytes)
	}
}

func TestWorkLeaseNotDueAndExpiryReclaim(t *testing.T) {
	rig := newTestRig(t)
	order := rig.setupOrder(t, "work-op-2", testNow.Add(time.Hour).UnixMilli())
	lease := rig.store.WorkLease()

	notDue, err := lease.ClaimWork(context.Background(), rig.bucket, order.OrderType, order.OrderID, "worker-1", "token-1", 30_000)
	if err != nil {
		t.Fatal(err)
	}
	if notDue.Status != WorkClaimNotDue {
		t.Fatalf("notDue=%s", notDue.Status)
	}

	// Due time passes.
	rig.server.SetTime(testNow.Add(2 * time.Hour))
	claim, err := lease.ClaimWork(context.Background(), rig.bucket, order.OrderType, order.OrderID, "worker-1", "token-1", 30_000)
	if err != nil {
		t.Fatal(err)
	}
	if claim.Status != WorkClaimClaimed {
		t.Fatalf("claim=%s", claim.Status)
	}

	// Worker crashes without completing; lease expiry lets another worker take over.
	staleAt := testNow.Add(2*time.Hour + 31*time.Second)
	rig.server.SetTime(staleAt)
	stale, err := lease.CompleteWorkWithProof(context.Background(), rig.bucket, order.OrderType, order.OrderID, "worker-1", "worker-1", claim.LeaseEpoch, claim.WorkVersion, "token-1", completionProofFor(rig, order))
	if err != nil {
		t.Fatal(err)
	}
	if stale.Status != WorkLeaseExpired {
		t.Fatalf("expired old worker complete=%s", stale.Status)
	}
	takeover, err := lease.ClaimWorkWithBoot(context.Background(), rig.bucket, order.OrderType, order.OrderID, "worker-2", "boot-2", "token-2", 30_000)
	if err != nil {
		t.Fatal(err)
	}
	if takeover.Status != WorkClaimClaimed || takeover.LeaseEpoch != 2 {
		t.Fatalf("takeover=%+v", takeover)
	}
	completed, err := lease.CompleteWorkWithProof(context.Background(), rig.bucket, order.OrderType, order.OrderID, "worker-2", "boot-2", takeover.LeaseEpoch, takeover.WorkVersion, "token-2", completionProofFor(rig, order))
	if err != nil || completed.Status != WorkLeaseCompleted {
		t.Fatalf("new boot completion=%s err=%v", completed.Status, err)
	}
}

func TestListDueCareWorkSkipsBadAndNonCarePages(t *testing.T) {
	rig := newTestRig(t)
	lease := rig.store.WorkLease()
	dueKey, err := rig.store.keys.SourceDueIndex(rig.bucket)
	if err != nil {
		t.Fatal(err)
	}
	entries := make([]redis.Z, 0, 130)
	for i := 0; i < 64; i++ {
		entries = append(entries, redis.Z{Score: float64(testNow.UnixMilli() - 1000), Member: fmt.Sprintf("broken-member-%03d", i)})
	}
	scheduleID, err := workidentity.StableOpaqueID("care-scan-schedule", "1")
	if err != nil {
		t.Fatal(err)
	}
	scheduleMember, err := workidentity.EncodeScheduleRefV1("FARM_OPPORTUNITY_WAKEUP", scheduleID)
	if err != nil {
		t.Fatal(err)
	}
	entries = append(entries, redis.Z{Score: float64(testNow.UnixMilli() - 1000), Member: scheduleMember})
	for i := 0; i < 64; i++ {
		id, idErr := workidentity.StableOpaqueID("non-care", fmt.Sprint(i))
		if idErr != nil {
			t.Fatal(idErr)
		}
		member, encodeErr := workidentity.EncodeWorkRefV1("PLANT", id)
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		entries = append(entries, redis.Z{Score: float64(testNow.UnixMilli() - 1000), Member: member})
	}
	careID, err := workidentity.StableOpaqueID("care-after-page", "1")
	if err != nil {
		t.Fatal(err)
	}
	careMember, err := workidentity.EncodeWorkRefV1("CARE_REWARD", careID)
	if err != nil {
		t.Fatal(err)
	}
	entries = append(entries, redis.Z{Score: float64(testNow.UnixMilli()), Member: careMember})
	if err := rig.client.ZAdd(context.Background(), dueKey.String(), entries...).Err(); err != nil {
		t.Fatal(err)
	}
	refs, err := lease.ListDueCareWork(context.Background(), rig.bucket, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 || refs[0].Ref.WorkType != "CARE_REWARD" || refs[0].Ref.WorkID != careID {
		t.Fatalf("care refs=%+v", refs)
	}
	if _, err := rig.client.ZScore(context.Background(), dueKey.String(), scheduleMember).Result(); err != nil {
		t.Fatalf("care scan removed valid schedule member: %v", err)
	}
	generic, err := lease.ListDueWorkAt(context.Background(), rig.bucket, testNow.UnixMilli(), 1)
	if err != nil || len(generic) != 1 {
		t.Fatalf("generic refs=%+v err=%v", generic, err)
	}
}

func TestListDueWorkPageMutationStableForAllFamiliesAndScheduleModes(t *testing.T) {
	families := []string{"CARE_REWARD", "HARVEST", "PLANT", "UNLOCK_PLOT", "THEFT_VISITOR", "THEFT_GUARD"}
	modes := []ScheduleDueIndexMode{
		ScheduleDueIndexShared,
		ScheduleDueIndexDualShadow,
		ScheduleDueIndexDedicatedFallback,
		ScheduleDueIndexDedicatedOnly,
	}
	for _, mode := range modes {
		for _, family := range families {
			t.Run(mode.String()+"/"+family, func(t *testing.T) {
				rig := newTestRigWithScheduleDueIndexMode(t, mode)
				sharedKey, err := rig.store.keys.SourceDueIndex(rig.bucket)
				if err != nil {
					t.Fatal(err)
				}
				dedicatedKey, err := rig.store.keys.SourceScheduleDueIndex(rig.bucket)
				if err != nil {
					t.Fatal(err)
				}
				const prefixCount = maxDueIndexDiscoveryEntries + 1
				const dueScore = float64(1700000000000)
				shared := make([]redis.Z, 0, prefixCount+1)
				dedicated := make([]redis.Z, 0, prefixCount)
				prefixMembers := make([]interface{}, 0, prefixCount)
				for index := 0; index < prefixCount; index++ {
					id, idErr := workidentity.StableOpaqueID("stable-due-prefix", mode.String(), family, strconv.Itoa(index))
					if idErr != nil {
						t.Fatal(idErr)
					}
					scheduleMember, encodeErr := workidentity.EncodeScheduleRefV1("FARM_OPPORTUNITY_WAKEUP", id)
					if encodeErr != nil {
						t.Fatal(encodeErr)
					}
					member := fmt.Sprintf("malformed-due-member-%05d", index)
					if mode.writesShared() && index%3 == 0 {
						// sr1_ and malformed members sort before wr1_ at the same
						// score, exercising the member half of the continuation.
						member = scheduleMember
					}
					shared = append(shared, redis.Z{Score: dueScore, Member: member})
					prefixMembers = append(prefixMembers, member)
					if mode.writesDedicated() {
						dedicated = append(dedicated, redis.Z{Score: dueScore, Member: scheduleMember})
					}
				}
				targetID, err := workidentity.StableOpaqueID("stable-due-target", mode.String(), family)
				if err != nil {
					t.Fatal(err)
				}
				targetMember, err := workidentity.EncodeWorkRefV1(family, targetID)
				if err != nil {
					t.Fatal(err)
				}
				shared = append(shared, redis.Z{Score: dueScore, Member: targetMember})
				if err := rig.client.ZAdd(context.Background(), sharedKey.String(), shared...).Err(); err != nil {
					t.Fatal(err)
				}
				if len(dedicated) != 0 {
					if err := rig.client.ZAdd(context.Background(), dedicatedKey.String(), dedicated...).Err(); err != nil {
						t.Fatal(err)
					}
				}

				lease := rig.store.WorkLease()
				first, err := lease.ListDueWorkPageAt(context.Background(), rig.bucket, testNow.UnixMilli(), family, DueWorkCursor{}, 1)
				if err != nil || first.Complete || first.RawScanned != maxDueIndexDiscoveryEntries || first.NextCursor.IsZero() || len(first.Refs) != 0 {
					t.Fatalf("first page=%+v err=%v", first, err)
				}
				if first.NextCursor.Index != sharedKey.String() || first.NextCursor.Score != dueScore {
					t.Fatalf("cursor is not bound to physical index/score: %+v", first.NextCursor)
				}

				// Remove the entire old prefix, including the inclusive cursor
				// member, and insert same-score members before it. A mutable rank
				// offset skips the continuously present target after this change;
				// lower_bound(score, member) resumes immediately after the removed
				// cursor pair. Mutations execute in a separate goroutine.
				mutated := make(chan error, 1)
				go func() {
					if removeErr := rig.client.ZRem(context.Background(), sharedKey.String(), prefixMembers...).Err(); removeErr != nil {
						mutated <- removeErr
						return
					}
					inserted := make([]redis.Z, 128)
					for index := range inserted {
						inserted[index] = redis.Z{Score: dueScore, Member: fmt.Sprintf("0-concurrent-insert-%05d", index)}
					}
					mutated <- rig.client.ZAdd(context.Background(), sharedKey.String(), inserted...).Err()
				}()
				if err := <-mutated; err != nil {
					t.Fatal(err)
				}

				stop := make(chan struct{})
				var mutationWG sync.WaitGroup
				mutationWG.Add(1)
				go func() {
					defer mutationWG.Done()
					for index := 0; ; index++ {
						select {
						case <-stop:
							return
						default:
						}
						member := fmt.Sprintf("0-live-mutation-%05d", index%64)
						_ = rig.client.ZAdd(context.Background(), sharedKey.String(), redis.Z{Score: dueScore, Member: member}).Err()
						_ = rig.client.ZRem(context.Background(), sharedKey.String(), member).Err()
					}
				}()
				second, pageErr := lease.ListDueWorkPageAt(context.Background(), rig.bucket, testNow.UnixMilli(), family, first.NextCursor, 1)
				close(stop)
				mutationWG.Wait()
				if pageErr != nil || len(second.Refs) != 1 || second.Refs[0].Ref.WorkType != family || second.Refs[0].Ref.WorkID != targetID {
					t.Fatalf("continuously present target not observed: page=%+v err=%v", second, pageErr)
				}
				if second.Complete {
					t.Fatalf("non-zero traversal claimed whole-index completeness: %+v", second)
				}
				if _, err := rig.client.ZScore(context.Background(), sharedKey.String(), targetMember).Result(); err != nil {
					t.Fatalf("target was not continuously present: %v", err)
				}
			})
		}
	}
}

func TestListDueWorkPageFamilyLimitDoesNotSkipPrefetchedRefs(t *testing.T) {
	rig := newTestRig(t)
	dueKey, err := rig.store.keys.SourceDueIndex(rig.bucket)
	if err != nil {
		t.Fatal(err)
	}
	members := make([]string, 0, 3)
	entries := make([]redis.Z, 0, 3)
	for index := 0; index < 3; index++ {
		id, idErr := workidentity.StableOpaqueID("due-family-consumption", strconv.Itoa(index))
		if idErr != nil {
			t.Fatal(idErr)
		}
		member, encodeErr := workidentity.EncodeWorkRefV1("HARVEST", id)
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		members = append(members, member)
		entries = append(entries, redis.Z{Score: float64(testNow.UnixMilli()), Member: member})
	}
	sort.Strings(members)
	if err := rig.client.ZAdd(context.Background(), dueKey.String(), entries...).Err(); err != nil {
		t.Fatal(err)
	}
	cursor := DueWorkCursor{}
	got := make([]string, 0, len(members))
	for range members {
		page, pageErr := rig.store.WorkLease().ListDueWorkPageAt(context.Background(), rig.bucket, testNow.UnixMilli(), "HARVEST", cursor, 1)
		if pageErr != nil || len(page.Refs) != 1 {
			t.Fatalf("page=%+v err=%v", page, pageErr)
		}
		got = append(got, page.Refs[0].Member)
		cursor = page.NextCursor
	}
	if !reflect.DeepEqual(got, members) {
		t.Fatalf("consumed members=%q want=%q", got, members)
	}
}

func TestListDueWorkPageCursorRejectsDifferentPhysicalClientAndIndex(t *testing.T) {
	rig := newTestRig(t)
	dueKey, err := rig.store.keys.SourceDueIndex(rig.bucket)
	if err != nil {
		t.Fatal(err)
	}
	entries := make([]redis.Z, maxDueIndexDiscoveryEntries+1)
	for index := range entries {
		entries[index] = redis.Z{Score: float64(testNow.UnixMilli()), Member: fmt.Sprintf("cursor-binding-%05d", index)}
	}
	if err := rig.client.ZAdd(context.Background(), dueKey.String(), entries...).Err(); err != nil {
		t.Fatal(err)
	}
	page, err := rig.store.WorkLease().ListDueWorkPageAt(context.Background(), rig.bucket, testNow.UnixMilli(), "CARE_REWARD", DueWorkCursor{}, 1)
	if err != nil || page.NextCursor.IsZero() {
		t.Fatalf("first page=%+v err=%v", page, err)
	}

	other := newTestRig(t)
	if _, err := other.store.WorkLease().ListDueWorkPageAt(context.Background(), other.bucket, testNow.UnixMilli(), "CARE_REWARD", page.NextCursor, 1); !errors.Is(err, authoritystore.ErrInvalidRequest) {
		t.Fatalf("different physical client accepted cursor: %v", err)
	}
	wrongIndex := page.NextCursor
	wrongIndex.Index += ":other"
	if _, err := rig.store.WorkLease().ListDueWorkPageAt(context.Background(), rig.bucket, testNow.UnixMilli(), "CARE_REWARD", wrongIndex, 1); !errors.Is(err, authoritystore.ErrInvalidRequest) {
		t.Fatalf("different physical index accepted cursor: %v", err)
	}
}

func TestListDueWorkPageCompleteRequiresCursorZeroPhysicalProof(t *testing.T) {
	rig := newTestRig(t)
	page, err := rig.store.WorkLease().ListDueWorkPageAt(context.Background(), rig.bucket, testNow.UnixMilli(), "CARE_REWARD", DueWorkCursor{}, 1)
	if err != nil || !page.Complete || !page.NextCursor.IsZero() || page.RawScanned != 0 || len(page.Refs) != 0 {
		t.Fatalf("empty complete page=%+v err=%v", page, err)
	}
}

func TestWorkLeaseRetryStateSurvivesFacadeRestartAndReconciles(t *testing.T) {
	rig := newTestRig(t)
	store, err := NewStore(rig.client, rig.store.Registry(), Options{
		Profile: rig.store.opts.Profile, Resolver: testCalculator(t), WorkTotalDeadlineMs: time.Hour.Milliseconds(), WorkMaxRetryCount: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadAndClaim(context.Background(), authoritystore.LoadAndClaimRequest{
		ActorAddress: rig.addr, AuthorityLayoutVersion: "v1", ShardWriteFence: rig.fence,
		ClaimOperationID: "claim-op-retry", NewWriterClaimID: "claim-retry",
		InitialModelFields: authoritystore.ModelState{"initial": []byte("state")},
	})
	if err != nil {
		t.Fatal(err)
	}
	order := dueOrder(t, "CARE_REWARD", "retry-persist", testNow.UnixMilli())
	req := rig.commitReq("claim-retry", loaded.Snapshot.RecordVersion, "retry-persist", authoritystore.ModelState{"retry": []byte("state")})
	req.AttachedOrders = []authoritystore.AttachedOrder{order}
	if _, err := store.Commit(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	first, err := store.WorkLease().ClaimWorkWithBoot(context.Background(), rig.bucket, order.OrderType, order.OrderID, "worker-a", "boot-a", "token-a", 30_000)
	if err != nil || first.Status != WorkClaimClaimed {
		t.Fatalf("first claim=%+v err=%v", first, err)
	}
	rescheduled, err := store.WorkLease().RescheduleWork(context.Background(), rig.bucket, order.OrderType, order.OrderID, "worker-a", "boot-a", first.LeaseEpoch, first.WorkVersion, "token-a", 1, 1000, "RETRYABLE_ERROR")
	if err != nil || rescheduled.Status != WorkLeaseRenewed {
		t.Fatalf("reschedule=%+v err=%v", rescheduled, err)
	}
	snapshot, err := store.WorkLease().GetSourceOrder(context.Background(), rig.bucket, order.OrderType, order.OrderID)
	if err != nil || snapshot.RetryCount != 1 || snapshot.State != "REWARD_DISPATCHING" || snapshot.NextDueAtMs == 0 {
		t.Fatalf("persisted retry snapshot=%+v err=%v", snapshot, err)
	}
	// A new facade/worker identity must observe the persisted attempt, not a
	// process-local zero value.
	rig.server.SetTime(testNow.Add(31 * time.Second))
	second, err := store.WorkLease().ClaimWorkWithBoot(context.Background(), rig.bucket, order.OrderType, order.OrderID, "worker-b", "boot-b", "token-b", 30_000)
	if err != nil || second.Status != WorkClaimClaimed || second.RetryCount != 1 || second.LeaseHolderBootID != "boot-b" {
		t.Fatalf("restarted claim=%+v err=%v", second, err)
	}
	terminal, err := store.WorkLease().RescheduleWork(context.Background(), rig.bucket, order.OrderType, order.OrderID, "worker-b", "boot-b", second.LeaseEpoch, second.WorkVersion, "token-b", 2, 1000, "PAYLOAD_INVALID")
	if err != nil || terminal.Status != WorkLeaseReconcileRequired {
		t.Fatalf("terminal retry=%+v err=%v", terminal, err)
	}
	snapshot, err = store.WorkLease().GetSourceOrder(context.Background(), rig.bucket, order.OrderType, order.OrderID)
	if err != nil || snapshot.State != "RECONCILE_REQUIRED" || snapshot.RetryCount != 2 || snapshot.ReconcileRequiredAtMs == 0 {
		t.Fatalf("reconcile snapshot=%+v err=%v", snapshot, err)
	}
}

func TestBucketScanLeaseMutualExclusion(t *testing.T) {
	rig := newTestRig(t)
	lease := rig.store.WorkLease()

	first, err := lease.ClaimBucketScanLease(context.Background(), rig.bucket, "scanner-1", 30_000)
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != ScanLeaseClaimed || first.LeaseEpoch != 1 {
		t.Fatalf("first=%+v", first)
	}
	held, err := lease.ClaimBucketScanLease(context.Background(), rig.bucket, "scanner-2", 30_000)
	if err != nil {
		t.Fatal(err)
	}
	if held.Status != ScanLeaseHeldByOther {
		t.Fatalf("held=%s", held.Status)
	}
	renew, err := lease.RenewBucketScanLease(context.Background(), rig.bucket, "scanner-1", 1, 30_000)
	if err != nil {
		t.Fatal(err)
	}
	if renew.Status != ScanLeaseRenewed {
		t.Fatalf("renew=%s", renew.Status)
	}
	mismatch, err := lease.RenewBucketScanLease(context.Background(), rig.bucket, "scanner-1", 7, 30_000)
	if err != nil {
		t.Fatal(err)
	}
	if mismatch.Status != ScanLeaseFenceMismatch {
		t.Fatalf("mismatch=%s", mismatch.Status)
	}
	rig.server.SetTime(testNow.Add(2 * time.Minute))
	takeover, err := lease.ClaimBucketScanLease(context.Background(), rig.bucket, "scanner-2", 30_000)
	if err != nil {
		t.Fatal(err)
	}
	if takeover.Status != ScanLeaseClaimed || takeover.LeaseEpoch != 2 {
		t.Fatalf("takeover=%+v", takeover)
	}
}

func TestListDueWorkReadsExistingDueIndexWithoutClaiming(t *testing.T) {
	rig := newTestRig(t)
	order := rig.setupOrder(t, "work-op-scan", testNow.UnixMilli())
	lease := rig.store.WorkLease()
	refs, err := lease.ListDueWork(context.Background(), rig.bucket, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 || refs[0].Ref.WorkType != order.OrderType || refs[0].Ref.WorkID != order.OrderID {
		t.Fatalf("refs=%+v", refs)
	}
	snapshot, err := lease.GetSourceOrder(context.Background(), rig.bucket, order.OrderType, order.OrderID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.WorkID != order.OrderID || snapshot.WorkType != order.OrderType || snapshot.State != "PENDING" || string(snapshot.TypedPayload) != "payload:work-op-scan" {
		t.Fatalf("source order=%+v", snapshot)
	}
	claim, err := lease.ClaimWork(context.Background(), rig.bucket, order.OrderType, order.OrderID, "scanner-worker", "scanner-token", 30_000)
	if err != nil || claim.Status != WorkClaimClaimed {
		t.Fatalf("claim after read=%+v err=%v", claim, err)
	}
}
