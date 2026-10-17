package scaleplan_test

import (
	"context"
	"errors"
	"testing"
	"time"

	core "farmgame/internal/scaleplan"
	store "farmgame/internal/storage/redis/scaleplan"
	"farmgame/internal/testkit/scaleplanstore"
)

func TestMiniredisEvalTimeGate(t *testing.T) {
	h := scaleplanstore.New(t, "realm-a")
	ctx := context.Background()
	got, err := h.Client.Eval(ctx, "local t=redis.call('TIME'); return {t[1],t[2]}", nil).Result()
	if err != nil {
		t.Fatalf("TIME in EVAL: %v", err)
	}
	if got == nil {
		t.Fatal("TIME returned nil")
	}
}

func TestCreateLoadCASAndList(t *testing.T) {
	h := scaleplanstore.New(t, "realm-a")
	ctx := context.Background()
	plan := scaleplanstore.InitialPlan(t, "realm-a", "operation-a", "workload-a")
	lease := h.Lease(t, "nonce-a")
	create, err := h.Backend.CreateOrGet(ctx, lease, core.DurableCreateCommand{Submission: core.IdempotencySubmission{Key: plan.IdempotencyKey, PayloadDigest: plan.PayloadDigest, WorkloadKey: plan.Submission.Workload.Key()}, Plan: plan})
	if err != nil || create.Status != core.CreateCommitted {
		t.Fatalf("create: status=%s err=%v", create.Status, err)
	}
	loaded, found, err := h.Backend.Load(ctx, plan.PlanID)
	if err != nil || !found || loaded.Plan.PlanID != plan.PlanID {
		t.Fatalf("load: found=%v err=%v record=%+v", found, err, loaded)
	}
	candidate := plan.Clone()
	candidate.Revision = 2
	candidate.UpdatedAtMs++
	cas, err := h.Backend.CompareAndSwap(ctx, lease, core.DurableCASCommand{Expected: 1, Plan: candidate})
	if err != nil || cas.Status != core.CASCommitted {
		t.Fatalf("cas: status=%s err=%v", cas.Status, err)
	}
	page, err := h.Backend.ListRecoverable(ctx, core.RecoverableCursor{Limit: 10})
	if err != nil || page.Done != true || len(page.Records) != 1 || page.Records[0].Plan.Revision != 2 {
		t.Fatalf("list: page=%+v err=%v", page, err)
	}
}

func TestCASHonorsExpectedRevision(t *testing.T) {
	h := scaleplanstore.New(t, "realm-a")
	ctx := context.Background()
	plan := scaleplanstore.InitialPlan(t, "realm-a", "operation-a", "workload-a")
	lease := h.Lease(t, "nonce-a")
	command := core.DurableCreateCommand{Submission: core.IdempotencySubmission{Key: plan.IdempotencyKey, PayloadDigest: plan.PayloadDigest, WorkloadKey: plan.Submission.Workload.Key()}, Plan: plan}
	if _, err := h.Backend.CreateOrGet(ctx, lease, command); err != nil {
		t.Fatal(err)
	}
	candidate := plan.Clone()
	candidate.Revision = 2
	candidate.UpdatedAtMs++
	result, err := h.Backend.CompareAndSwap(ctx, lease, core.DurableCASCommand{Expected: 0, Plan: candidate})
	if !errors.Is(err, core.ErrRevisionConflict) || result.Status != core.CASRevisionConflict {
		t.Fatalf("wrong expected: status=%s err=%v", result.Status, err)
	}
	loaded, found, err := h.Backend.Load(ctx, plan.PlanID)
	if err != nil || !found || loaded.Plan.Revision != 1 {
		t.Fatalf("candidate applied on wrong expected: found=%v rev=%d err=%v", found, loaded.Plan.Revision, err)
	}
}

func TestCreateIdempotencyAndSingleActiveWorkload(t *testing.T) {
	h := scaleplanstore.New(t, "realm-a")
	ctx := context.Background()
	lease := h.Lease(t, "nonce-a")
	first := scaleplanstore.InitialPlan(t, "realm-a", "operation-a", "workload-a")
	command := core.DurableCreateCommand{Submission: core.IdempotencySubmission{Key: first.IdempotencyKey, PayloadDigest: first.PayloadDigest, WorkloadKey: first.Submission.Workload.Key()}, Plan: first}
	if _, err := h.Backend.CreateOrGet(ctx, lease, command); err != nil {
		t.Fatal(err)
	}
	replay, err := h.Backend.CreateOrGet(ctx, lease, command)
	if err != nil || replay.Status != core.CreateExistingSamePayload {
		t.Fatalf("replay: status=%s err=%v", replay.Status, err)
	}
	second := scaleplanstore.InitialPlan(t, "realm-a", "operation-b", "workload-a")
	busy, err := h.Backend.CreateOrGet(ctx, lease, core.DurableCreateCommand{Submission: core.IdempotencySubmission{Key: second.IdempotencyKey, PayloadDigest: second.PayloadDigest, WorkloadKey: second.Submission.Workload.Key()}, Plan: second})
	if !errors.Is(err, core.ErrWorkloadBusy) || busy.Status != core.CreateWorkloadBusy {
		t.Fatalf("busy: status=%s err=%v", busy.Status, err)
	}
}

func TestCreateDifferentWorkloadReturnsBusyWithoutCorruptionLatch(t *testing.T) {
	h := scaleplanstore.New(t, "realm-a")
	ctx := context.Background()
	lease := h.Lease(t, "nonce-a")

	player := scaleplanstore.InitialPlan(t, "realm-a", "operation-player", "workload-player")
	farm := scaleplanstore.InitialPlan(t, "realm-a", "operation-farm", "workload-farm")
	playerResult, err := h.Backend.CreateOrGet(ctx, lease, core.DurableCreateCommand{
		Submission: core.IdempotencySubmission{
			Key:           player.IdempotencyKey,
			PayloadDigest: player.PayloadDigest,
			WorkloadKey:   player.Submission.Workload.Key(),
		},
		Plan: player,
	})
	if err != nil || playerResult.Status != core.CreateCommitted {
		t.Fatalf("create player: status=%s err=%v", playerResult.Status, err)
	}

	farmResult, err := h.Backend.CreateOrGet(ctx, lease, core.DurableCreateCommand{
		Submission: core.IdempotencySubmission{
			Key:           farm.IdempotencyKey,
			PayloadDigest: farm.PayloadDigest,
			WorkloadKey:   farm.Submission.Workload.Key(),
		},
		Plan: farm,
	})
	if !errors.Is(err, core.ErrWorkloadBusy) || farmResult.Status != core.CreateWorkloadBusy {
		t.Fatalf("create farm while player active: status=%s err=%v", farmResult.Status, err)
	}

	page, err := h.Backend.ListRecoverable(ctx, core.RecoverableCursor{Limit: 10})
	if err != nil || !page.Done || len(page.Records) != 1 || page.Records[0].Plan.PlanID != player.PlanID {
		t.Fatalf("recoverable plans: page=%+v err=%v", page, err)
	}
	if exists, err := h.Client.Exists(ctx, h.Backend.Keys().CorruptLatch().String()).Result(); err != nil || exists != 0 {
		t.Fatalf("unexpected corruption latch: exists=%d err=%v", exists, err)
	}
}

func TestLeaseFenceAndExpiry(t *testing.T) {
	h := scaleplanstore.New(t, "realm-a", StoreOptionsForTest())
	ctx := context.Background()
	first := h.Lease(t, "nonce-a")
	before, _ := h.Client.Time(ctx).Result()
	if _, err := h.Backend.AcquireMutationLease(ctx, h.Scope(), h.Owner("nonce-b")); !errors.Is(err, store.ErrLeaseBusy) {
		t.Fatalf("busy lease err=%v", err)
	}
	h.Server.SetTime(before.Add(21 * time.Second))
	second, err := h.Backend.AcquireMutationLease(ctx, h.Scope(), h.Owner("nonce-b"))
	if err != nil {
		t.Fatalf("takeover: %v", err)
	}
	if first.Fence == second.Fence {
		t.Fatalf("fence did not advance: %s", first.Fence)
	}
	if _, err := h.Backend.RenewMutationLease(ctx, first); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("stale renew err=%v", err)
	}
}

func TestTerminalCASReleasesActiveAndRetainsIndex(t *testing.T) {
	h := scaleplanstore.New(t, "realm-a")
	ctx := context.Background()
	plan := scaleplanstore.InitialPlan(t, "realm-a", "operation-a", "workload-a")
	lease := h.Lease(t, "nonce-a")
	command := core.DurableCreateCommand{Submission: core.IdempotencySubmission{Key: plan.IdempotencyKey, PayloadDigest: plan.PayloadDigest, WorkloadKey: plan.Submission.Workload.Key()}, Plan: plan}
	if _, err := h.Backend.CreateOrGet(ctx, lease, command); err != nil {
		t.Fatal(err)
	}
	candidate := plan.Clone()
	candidate.Status = core.StatusFailed
	candidate.ErrorCode = "TEST_FAILURE"
	candidate.Revision = 2
	candidate.UpdatedAtMs++
	candidate.CompletedAtMs = candidate.UpdatedAtMs
	result, err := h.Backend.CompareAndSwap(ctx, lease, core.DurableCASCommand{Expected: 1, Plan: candidate})
	if err != nil || result.Status != core.CASCommitted {
		t.Fatalf("terminal CAS: status=%s err=%v", result.Status, err)
	}
	if _, found, err := h.Backend.Load(ctx, plan.PlanID); err != nil || !found {
		t.Fatalf("terminal load: found=%v err=%v", found, err)
	}
	page, err := h.Backend.ListRecoverable(ctx, core.RecoverableCursor{})
	if err != nil || !page.Done || len(page.Records) != 0 {
		t.Fatalf("empty recoverable list: page=%+v err=%v", page, err)
	}
	second := scaleplanstore.InitialPlan(t, "realm-a", "operation-b", "workload-a")
	if _, err := h.Backend.CreateOrGet(ctx, lease, core.DurableCreateCommand{Submission: core.IdempotencySubmission{Key: second.IdempotencyKey, PayloadDigest: second.PayloadDigest, WorkloadKey: second.Submission.Workload.Key()}, Plan: second}); err != nil {
		t.Fatalf("active release: %v", err)
	}
}

func TestBoundedTerminalCleanupDeletesAllTerminalReferences(t *testing.T) {
	h := scaleplanstore.New(t, "realm-a", store.Options{TerminalRetention: time.Second})
	ctx := context.Background()
	plan := scaleplanstore.InitialPlan(t, "realm-a", "operation-a", "workload-a")
	lease := h.Lease(t, "nonce-a")
	command := core.DurableCreateCommand{Submission: core.IdempotencySubmission{Key: plan.IdempotencyKey, PayloadDigest: plan.PayloadDigest, WorkloadKey: plan.Submission.Workload.Key()}, Plan: plan}
	if _, err := h.Backend.CreateOrGet(ctx, lease, command); err != nil {
		t.Fatal(err)
	}
	candidate := plan.Clone()
	candidate.Status = core.StatusFailed
	candidate.ErrorCode = "TEST_FAILURE"
	candidate.Revision = 2
	candidate.UpdatedAtMs++
	candidate.CompletedAtMs = candidate.UpdatedAtMs
	if _, err := h.Backend.CompareAndSwap(ctx, lease, core.DurableCASCommand{Expected: 1, Plan: candidate}); err != nil {
		t.Fatal(err)
	}
	member, err := h.Backend.Keys().PlanMember(plan.PlanID)
	if err != nil {
		t.Fatal(err)
	}
	if score, err := h.Client.ZScore(ctx, h.Backend.Keys().Terminal().String(), member).Result(); err != nil || score <= 0 {
		t.Fatalf("terminal score: %v %v", score, err)
	}
	h.Server.SetTime(time.UnixMilli(1_700_000_000_000 + 2_000))
	cleaned, err := h.Backend.CleanupTerminal(ctx, lease, 1)
	if err != nil || cleaned.Cleaned != 1 || cleaned.MoreDue {
		t.Fatalf("cleanup: %+v %v", cleaned, err)
	}
	if _, found, err := h.Backend.Load(ctx, plan.PlanID); err != nil || found {
		t.Fatalf("load after cleanup: found=%v err=%v", found, err)
	}
	if got, err := h.Client.ZCard(ctx, h.Backend.Keys().Terminal().String()).Result(); err != nil || got != 0 {
		t.Fatalf("terminal index: %d %v", got, err)
	}
	if got, err := h.Client.HLen(ctx, h.Backend.Keys().Accounting().String()).Result(); err != nil || got != 0 {
		t.Fatalf("accounting: %d %v", got, err)
	}
}

func TestCreateAtomicallyCleansExpiredTerminalBeforeCapacityCheck(t *testing.T) {
	h := scaleplanstore.New(t, "realm-a", store.Options{MaxItems: 1, TerminalRetention: time.Second})
	ctx := context.Background()
	first := scaleplanstore.InitialPlan(t, "realm-a", "operation-a", "workload-a")
	lease := h.Lease(t, "nonce-a")
	command := core.DurableCreateCommand{Submission: core.IdempotencySubmission{Key: first.IdempotencyKey, PayloadDigest: first.PayloadDigest, WorkloadKey: first.Submission.Workload.Key()}, Plan: first}
	if _, err := h.Backend.CreateOrGet(ctx, lease, command); err != nil {
		t.Fatal(err)
	}
	terminal := first.Clone()
	terminal.Status = core.StatusFailed
	terminal.ErrorCode = "TEST_FAILURE"
	terminal.Revision = 2
	terminal.UpdatedAtMs++
	terminal.CompletedAtMs = terminal.UpdatedAtMs
	if _, err := h.Backend.CompareAndSwap(ctx, lease, core.DurableCASCommand{Expected: 1, Plan: terminal}); err != nil {
		t.Fatal(err)
	}
	h.Server.SetTime(time.UnixMilli(1_700_000_000_000 + 2_000))
	second := scaleplanstore.InitialPlan(t, "realm-a", "operation-b", "workload-a")
	result, err := h.Backend.CreateOrGet(ctx, lease, core.DurableCreateCommand{Submission: core.IdempotencySubmission{Key: second.IdempotencyKey, PayloadDigest: second.PayloadDigest, WorkloadKey: second.Submission.Workload.Key()}, Plan: second})
	if err != nil || result.Status != core.CreateCommitted {
		t.Fatalf("create after retention: status=%s err=%v", result.Status, err)
	}
	if _, found, err := h.Backend.Load(ctx, first.PlanID); err != nil || found {
		t.Fatalf("expired terminal remains: found=%v err=%v", found, err)
	}
}

func TestCASAtomicallyCleansExpiredTerminal(t *testing.T) {
	h := scaleplanstore.New(t, "realm-a", store.Options{TerminalRetention: time.Second})
	ctx := context.Background()
	lease := h.Lease(t, "nonce-a")
	first := scaleplanstore.InitialPlan(t, "realm-a", "operation-a", "workload-a")
	firstCommand := core.DurableCreateCommand{Submission: core.IdempotencySubmission{Key: first.IdempotencyKey, PayloadDigest: first.PayloadDigest, WorkloadKey: first.Submission.Workload.Key()}, Plan: first}
	if _, err := h.Backend.CreateOrGet(ctx, lease, firstCommand); err != nil {
		t.Fatal(err)
	}
	firstTerminal := first.Clone()
	firstTerminal.Status = core.StatusFailed
	firstTerminal.ErrorCode = "TEST_FAILURE"
	firstTerminal.Revision = 2
	firstTerminal.UpdatedAtMs++
	firstTerminal.CompletedAtMs = firstTerminal.UpdatedAtMs
	if _, err := h.Backend.CompareAndSwap(ctx, lease, core.DurableCASCommand{Expected: 1, Plan: firstTerminal}); err != nil {
		t.Fatal(err)
	}
	second := scaleplanstore.InitialPlan(t, "realm-a", "operation-b", "workload-b")
	secondCommand := core.DurableCreateCommand{Submission: core.IdempotencySubmission{Key: second.IdempotencyKey, PayloadDigest: second.PayloadDigest, WorkloadKey: second.Submission.Workload.Key()}, Plan: second}
	if _, err := h.Backend.CreateOrGet(ctx, lease, secondCommand); err != nil {
		t.Fatal(err)
	}
	h.Server.SetTime(time.UnixMilli(1_700_000_000_000 + 2_000))
	secondCandidate := second.Clone()
	secondCandidate.Revision = 2
	secondCandidate.UpdatedAtMs++
	if _, err := h.Backend.CompareAndSwap(ctx, lease, core.DurableCASCommand{Expected: 1, Plan: secondCandidate}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := h.Backend.Load(ctx, first.PlanID); err != nil || found {
		t.Fatalf("expired terminal remains after CAS: found=%v err=%v", found, err)
	}
	if loaded, found, err := h.Backend.Load(ctx, second.PlanID); err != nil || !found || loaded.Plan.Revision != 2 {
		t.Fatalf("CAS candidate lost: found=%v rev=%d err=%v", found, loaded.Plan.Revision, err)
	}
}

func TestMutationCorruptionLatchesWithZeroWrite(t *testing.T) {
	h := scaleplanstore.New(t, "realm-a")
	ctx := context.Background()
	plan := scaleplanstore.InitialPlan(t, "realm-a", "operation-a", "workload-a")
	lease := h.Lease(t, "nonce-a")
	command := core.DurableCreateCommand{Submission: core.IdempotencySubmission{Key: plan.IdempotencyKey, PayloadDigest: plan.PayloadDigest, WorkloadKey: plan.Submission.Workload.Key()}, Plan: plan}
	if _, err := h.Backend.CreateOrGet(ctx, lease, command); err != nil {
		t.Fatal(err)
	}
	key, _ := h.Backend.Keys().Plan(plan.PlanID)
	if err := h.Client.HSet(ctx, key.String(), "unexpected", "tamper").Err(); err != nil {
		t.Fatal(err)
	}
	candidate := plan.Clone()
	candidate.Revision = 2
	candidate.UpdatedAtMs++
	if _, err := h.Backend.CompareAndSwap(ctx, lease, core.DurableCASCommand{Expected: 1, Plan: candidate}); !errors.Is(err, core.ErrCorruptRecord) {
		t.Fatalf("corruption err=%v", err)
	}
	if got, err := h.Client.Exists(ctx, h.Backend.Keys().CorruptLatch().String()).Result(); err != nil || got != 1 {
		t.Fatalf("latch: exists=%d err=%v", got, err)
	}
	if _, err := h.Backend.CreateOrGet(ctx, lease, command); !errors.Is(err, core.ErrCorruptRecord) {
		t.Fatalf("latched mutation err=%v", err)
	}
}

func TestForgedBlockedSealIsRejectedBeforeRedisAccess(t *testing.T) {
	h := scaleplanstore.New(t, "realm-a")
	lease := h.Lease(t, "nonce-a")
	var forged core.SealedBlockedTerminationMutation
	if _, err := h.Backend.ApplySealedBlockedTermination(context.Background(), lease, forged); !errors.Is(err, core.ErrVerification) {
		t.Fatalf("forged seal err=%v", err)
	}
	if keys, err := h.Client.DBSize(context.Background()).Result(); err != nil || keys != 2 {
		t.Fatalf("forged seal touched redis: keys=%d err=%v", keys, err)
	}
}

func TestApplyThenUnknownLeavesExactCandidateRecoverable(t *testing.T) {
	h := scaleplanstore.New(t, "realm-a", store.Options{AfterMutation: func(name string) error {
		if name == "ApplyPlanV1" {
			return errors.New("response lost")
		}
		return nil
	}})
	ctx := context.Background()
	plan := scaleplanstore.InitialPlan(t, "realm-a", "operation-a", "workload-a")
	lease := h.Lease(t, "nonce-a")
	command := core.DurableCreateCommand{Submission: core.IdempotencySubmission{Key: plan.IdempotencyKey, PayloadDigest: plan.PayloadDigest, WorkloadKey: plan.Submission.Workload.Key()}, Plan: plan}
	result, err := h.Backend.CreateOrGet(ctx, lease, command)
	if !errors.Is(err, core.ErrOutcomeUnknown) || result.Status != core.CreateCommitted {
		t.Fatalf("unknown create: status=%s err=%v", result.Status, err)
	}
	loaded, found, err := h.Backend.Load(ctx, plan.PlanID)
	if err != nil || !found || loaded.Plan.PlanID != plan.PlanID {
		t.Fatalf("unknown replay load: found=%v err=%v", found, err)
	}
}

func StoreOptionsForTest() store.Options { return store.Options{LeaseTTL: 5 * time.Second} }
