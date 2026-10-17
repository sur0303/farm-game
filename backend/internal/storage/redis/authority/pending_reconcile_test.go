package authority

import (
	"context"
	"errors"
	"testing"

	"github.com/redis/go-redis/v9"

	"farmgame/internal/storage/authoritystore"
)

func completeWithPredecessorDirectScript(t *testing.T, rig *testRig, order authoritystore.AttachedOrder) {
	t.Helper()
	call, err := rig.store.WorkLease().prepareClaimAndComplete(WorkClaimAndCompleteRequest{
		Bucket: rig.bucket, WorkType: order.OrderType, WorkID: order.OrderID,
		HolderID: "predecessor", BootID: "predecessor-boot", LeaseToken: "predecessor-token", RequestedTTLms: 30_000,
		Proof: completionProofFor(rig, order),
	})
	if err != nil {
		t.Fatal(err)
	}
	reply, err := rig.store.registry.Run(context.Background(), ScriptClaimAndCompleteWorkV3, call.keys, call.args...)
	if err != nil || len(reply) == 0 || reply[0] != "COMPLETED" {
		t.Fatalf("predecessor completion reply=%v err=%v", reply, err)
	}
}

func setupPredecessorAccountingLeak(t *testing.T, rig *testRig, operationID string, payload []byte) authoritystore.AttachedOrder {
	t.Helper()
	order := rig.setupOrderWithPayload(t, operationID, testNow.UnixMilli(), payload)
	completeWithPredecessorDirectScript(t, rig, order)
	count, bytes := sourceOrderBucketUsage(t, rig)
	if count != 0 || bytes != int64(len(payload)) {
		t.Fatalf("predecessor leak=(%d,%d), want=(0,%d)", count, bytes, len(payload))
	}
	return order
}

func TestReconcilePendingWorkAccountingRepairsPredecessorLeakFromAuthoritativeOrdersAndSchedules(t *testing.T) {
	rig := newTestRigWithScheduleDueIndexMode(t, ScheduleDueIndexDualShadow)
	loaded := rig.load(t, "pending-reconcile")
	oldOrder := dueOrder(t, "CARE_REWARD", "pending-reconcile-old", testNow.UnixMilli())
	oldOrder.Payload = []byte("old-terminal-payload")
	pendingOrder := dueOrder(t, "CARE_REWARD", "pending-reconcile-current", testNow.UnixMilli())
	pendingOrder.Payload = []byte("current-order")
	schedule := testSchedule(t, "pending-reconcile-schedule", 1, testNow.UnixMilli(), "current-schedule")
	req := rig.commitReq("pending-reconcile", loaded.RecordVersion, "pending-reconcile-create", authoritystore.ModelState{"state": []byte("v1")})
	req.AttachedOrders = []authoritystore.AttachedOrder{oldOrder, pendingOrder}
	req.AttachedSchedules = []authoritystore.AttachedSchedule{schedule}
	if result, err := rig.store.Commit(context.Background(), req); err != nil || result.Status != authoritystore.CommitCommitted {
		t.Fatalf("setup commit=%+v err=%v", result, err)
	}

	completeWithPredecessorDirectScript(t, rig, oldOrder)
	beforeCount, beforeBytes := sourceOrderBucketUsage(t, rig)
	wantCount := int64(2)
	wantBytes := int64(len(pendingOrder.Payload) + len(schedule.Payload))
	if beforeCount != wantCount || beforeBytes != wantBytes+int64(len(oldOrder.Payload)) {
		t.Fatalf("predecessor accounting=(%d,%d), want=(%d,%d)", beforeCount, beforeBytes, wantCount, wantBytes+int64(len(oldOrder.Payload)))
	}

	result, err := rig.store.WorkLease().ReconcilePendingWorkAccounting(context.Background(), rig.bucket, 32)
	if err != nil || result.Status != PendingWorkReconciled || !result.Changed ||
		result.PendingCount != wantCount || result.PendingBytes != wantBytes {
		t.Fatalf("reconcile=%+v err=%v", result, err)
	}
	count, bytes := sourceOrderBucketUsage(t, rig)
	if count != wantCount || bytes != wantBytes {
		t.Fatalf("repaired accounting=(%d,%d), want=(%d,%d)", count, bytes, wantCount, wantBytes)
	}

	again, err := rig.store.WorkLease().ReconcilePendingWorkAccounting(context.Background(), rig.bucket, 32)
	if err != nil || again.Status != PendingWorkReconciled || again.Changed ||
		again.PendingCount != wantCount || again.PendingBytes != wantBytes {
		t.Fatalf("idempotent reconcile=%+v err=%v", again, err)
	}
}

func TestReconcilePendingWorkAccountingExcludesAndRemovesTerminalStaleMember(t *testing.T) {
	rig := newTestRig(t)
	payload := []byte("historical-terminal")
	order := setupPredecessorAccountingLeak(t, rig, "pending-reconcile-stale", payload)
	_, _, dueKey, _, workRef, err := rig.store.WorkLease().keysFor(rig.bucket, order.OrderType, order.OrderID)
	if err != nil {
		t.Fatal(err)
	}
	if err := rig.client.ZAdd(context.Background(), dueKey.String(), redis.Z{Score: float64(testNow.UnixMilli()), Member: workRef}).Err(); err != nil {
		t.Fatal(err)
	}

	result, err := rig.store.WorkLease().ReconcilePendingWorkAccounting(context.Background(), rig.bucket, 8)
	if err != nil || result.Status != PendingWorkReconciled || result.PendingCount != 0 || result.PendingBytes != 0 {
		t.Fatalf("reconcile=%+v err=%v", result, err)
	}
	if _, err := rig.client.ZScore(context.Background(), dueKey.String(), workRef).Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("terminal stale member remains: %v", err)
	}
	count, bytes := sourceOrderBucketUsage(t, rig)
	if count != 0 || bytes != 0 {
		t.Fatalf("terminal leak remains=(%d,%d)", count, bytes)
	}
}

type mutateBeforeReconcileClient struct {
	redis.UniversalClient
	mutate func() error
	done   bool
}

func (c *mutateBeforeReconcileClient) Process(ctx context.Context, command redis.Cmder) error {
	if !c.done && command.FullName() == "evalsha" {
		c.done = true
		if err := c.mutate(); err != nil {
			return err
		}
	}
	return c.UniversalClient.Process(ctx, command)
}

func TestReconcilePendingWorkAccountingConcurrentIndexChangeIsStaleWithoutMutation(t *testing.T) {
	rig := newTestRig(t)
	order := rig.setupOrder(t, "pending-reconcile-race", testNow.UnixMilli())
	metaKey, err := rig.store.keys.BucketMeta(rig.bucket)
	if err != nil {
		t.Fatal(err)
	}
	dueKey, err := rig.store.keys.SourceDueIndex(rig.bucket)
	if err != nil {
		t.Fatal(err)
	}
	if err := rig.client.HSet(context.Background(), metaKey.String(), "pending_work_count", "77", "pending_work_bytes", "777").Err(); err != nil {
		t.Fatal(err)
	}
	client := &mutateBeforeReconcileClient{UniversalClient: rig.client}
	client.mutate = func() error {
		return rig.client.ZAdd(context.Background(), dueKey.String(), redis.Z{
			Score: float64(testNow.UnixMilli()), Member: "wr1_concurrent-unread-member",
		}).Err()
	}
	store, err := NewStore(client, nil, Options{Profile: testProfile(t), Resolver: testCalculator(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Registry().EnsureLoaded(context.Background()); err != nil {
		t.Fatal(err)
	}

	result, err := store.WorkLease().ReconcilePendingWorkAccounting(context.Background(), rig.bucket, 8)
	if err != nil || result.Status != PendingWorkReconcileStaleSnapshot {
		t.Fatalf("concurrent reconcile=%+v err=%v", result, err)
	}
	count, bytes := sourceOrderBucketUsage(t, rig)
	if count != 77 || bytes != 777 {
		t.Fatalf("stale reconciliation mutated counters=(%d,%d)", count, bytes)
	}
	snapshot, err := rig.store.WorkLease().GetSourceOrder(context.Background(), rig.bucket, order.OrderType, order.OrderID)
	if err != nil || snapshot.State != "PENDING" {
		t.Fatalf("stale reconciliation mutated record=%+v err=%v", snapshot, err)
	}
}

func TestReconcilePendingWorkAccountingMalformedRecordFailsBeforeCounterMutation(t *testing.T) {
	rig := newTestRig(t)
	order := rig.setupOrder(t, "pending-reconcile-malformed", testNow.UnixMilli())
	_, workKey, _, metaKey, _, err := rig.store.WorkLease().keysFor(rig.bucket, order.OrderType, order.OrderID)
	if err != nil {
		t.Fatal(err)
	}
	if err := rig.client.HDel(context.Background(), workKey.String(), "typed_payload").Err(); err != nil {
		t.Fatal(err)
	}
	if err := rig.client.HSet(context.Background(), metaKey.String(), "pending_work_count", "77", "pending_work_bytes", "777").Err(); err != nil {
		t.Fatal(err)
	}

	if _, err := rig.store.WorkLease().ReconcilePendingWorkAccounting(context.Background(), rig.bucket, 8); !errors.Is(err, errMalformedBlob) {
		t.Fatalf("malformed reconcile err=%v", err)
	}
	count, bytes := sourceOrderBucketUsage(t, rig)
	if count != 77 || bytes != 777 {
		t.Fatalf("malformed reconciliation mutated counters=(%d,%d)", count, bytes)
	}
}

func TestReconcilePendingWorkAccountingBoundFailsWithoutMutation(t *testing.T) {
	rig := newTestRig(t)
	loaded := rig.load(t, "pending-reconcile-bound")
	first := dueOrder(t, "CARE_REWARD", "pending-reconcile-bound-1", testNow.UnixMilli())
	second := dueOrder(t, "CARE_REWARD", "pending-reconcile-bound-2", testNow.UnixMilli())
	req := rig.commitReq("pending-reconcile-bound", loaded.RecordVersion, "pending-reconcile-bound-create", authoritystore.ModelState{"state": []byte("v1")})
	req.AttachedOrders = []authoritystore.AttachedOrder{first, second}
	if _, err := rig.store.Commit(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	metaKey, err := rig.store.keys.BucketMeta(rig.bucket)
	if err != nil {
		t.Fatal(err)
	}
	if err := rig.client.HSet(context.Background(), metaKey.String(), "pending_work_count", "99", "pending_work_bytes", "999").Err(); err != nil {
		t.Fatal(err)
	}

	if _, err := rig.store.WorkLease().ReconcilePendingWorkAccounting(context.Background(), rig.bucket, 1); !errors.Is(err, authoritystore.ErrCapacityExceeded) {
		t.Fatalf("bounded reconcile err=%v", err)
	}
	count, bytes := sourceOrderBucketUsage(t, rig)
	if count != 99 || bytes != 999 {
		t.Fatalf("bounded failure mutated counters=(%d,%d)", count, bytes)
	}
}

func TestReconcilePendingWorkAccountingResponseLossAndNotSent(t *testing.T) {
	for _, test := range []struct {
		name      string
		arm       func(*dropClient)
		want      PendingWorkReconcileStatus
		wantBytes int64
	}{
		{name: "response-lost", arm: func(drop *dropClient) { drop.dropAfter = 1 }, want: PendingWorkReconcileTimeoutUnknown, wantBytes: 0},
		{name: "not-sent", arm: func(drop *dropClient) { drop.dropNotSent = 1 }, want: PendingWorkReconcileUnavailableNotSent, wantBytes: int64(len("repair-fault-payload"))},
	} {
		t.Run(test.name, func(t *testing.T) {
			drop := &dropClient{}
			rig := newDropRig(t, drop)
			payload := []byte("repair-fault-payload")
			setupPredecessorAccountingLeak(t, rig, "pending-reconcile-"+test.name, payload)

			drop.mu.Lock()
			test.arm(drop)
			drop.mu.Unlock()
			result, err := rig.store.WorkLease().ReconcilePendingWorkAccounting(context.Background(), rig.bucket, 8)
			if err != nil || result.Status != test.want {
				t.Fatalf("fault result=%+v err=%v", result, err)
			}
			count, bytes := sourceOrderBucketUsage(t, rig)
			if count != 0 || bytes != test.wantBytes {
				t.Fatalf("fault accounting=(%d,%d), want=(0,%d)", count, bytes, test.wantBytes)
			}

			retry, err := rig.store.WorkLease().ReconcilePendingWorkAccounting(context.Background(), rig.bucket, 8)
			if err != nil || retry.Status != PendingWorkReconciled || retry.PendingCount != 0 || retry.PendingBytes != 0 {
				t.Fatalf("fault retry=%+v err=%v", retry, err)
			}
		})
	}
}
