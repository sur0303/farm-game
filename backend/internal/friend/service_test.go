package friend

import (
	"fmt"
	"testing"
)

type testEnv struct {
	pairs  *MemoryPairRepository
	quotas *MemoryQuotaRepository
	shares *MemoryShareIssuerRepository
	svc    *DomainService
	tokens int
}

func newTestEnv(t *testing.T, maxFriends int) *testEnv {
	t.Helper()
	env := &testEnv{
		pairs:  NewMemoryPairRepository(),
		quotas: NewMemoryQuotaRepository(maxFriends),
		shares: NewMemoryShareIssuerRepository(0),
	}
	svc, err := NewDomainService(env.pairs, env.quotas, env.shares)
	if err != nil {
		t.Fatal(err)
	}
	env.svc = svc
	return env
}

func (e *testEnv) generateToken(t *testing.T, inviter uint64, nowMs int64) ShareToken {
	t.Helper()
	e.tokens++
	token, outcome, err := e.shares.GenerateShareToken(context0, inviter, fmt.Sprintf("tok-%s-%d", itoa(inviter), e.tokens),
		publicIdentity(inviter, "GENERATE_SHARE_TOKEN", fmt.Sprintf("op-gen-%s-%d", itoa(inviter), e.tokens)), testHash("gen"), nowMs)
	if err != nil || outcome.Status == StatusRejected {
		t.Fatalf("generate token = %v %v", outcome, err)
	}
	return token
}

func (e *testEnv) redeem(t *testing.T, redeemer, inviter uint64, token ShareToken, operationID string, nowMs int64) RedeemShareResult {
	t.Helper()
	result, err := e.svc.RedeemShare(context0, RedeemShareCommand{
		RedeemerPlayerID: redeemer, InviterPlayerID: inviter,
		TokenID: token.TokenID, TokenGeneration: token.TokenGeneration,
		OperationID: operationID, PayloadHash: testHash(operationID), NowMs: nowMs,
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestRedeemShareHappyPath(t *testing.T) {
	env := newTestEnv(t, 300)
	token := env.generateToken(t, 10001, 0)

	result := env.redeem(t, 20002, 10001, token, "op-r1", 100)
	if result.Outcome.Status != StatusSucceeded || result.RelationEpoch != 1 {
		t.Fatalf("redeem = %+v", result.Outcome)
	}
	// Both quota slots occupied at epoch 1.
	for _, player := range []uint64{10001, 20002} {
		snap, err := env.quotas.GetQuota(context0, player)
		if err != nil {
			t.Fatal(err)
		}
		if !containsRelation(snap.ActiveRelations, result.RelationID, 1) {
			t.Fatalf("player %d quota = %+v", player, snap)
		}
	}
	// Relation is ACTIVE.
	pair, err := env.pairs.GetPair(context0, 10001, 20002)
	if err != nil {
		t.Fatal(err)
	}
	if pair.Relation.Status != RelationStatusActive || pair.Relation.RelationEpoch != 1 {
		t.Fatalf("pair = %+v", pair.Relation)
	}

	// Duplicate click with the same operation replays the original outcome.
	replay := env.redeem(t, 20002, 10001, token, "op-r1", 200)
	if replay.Outcome.Status != StatusSucceeded || replay.Outcome.Reason != "" {
		t.Fatalf("replay = %+v", replay.Outcome)
	}
	// A second operation by an existing friend is ALREADY_FRIENDS.
	again := env.redeem(t, 20002, 10001, token, "op-r2", 300)
	if again.Outcome.Status != StatusNoChange || again.Outcome.Reason != ReasonAlreadyFriends {
		t.Fatalf("already friends = %+v", again.Outcome)
	}
	// A third player uses the same token independently.
	third := env.redeem(t, 20003, 10001, token, "op-r3", 400)
	if third.Outcome.Status != StatusSucceeded || third.RelationID == result.RelationID {
		t.Fatalf("third redeem = %+v", third)
	}
	// Same operation id with a different payload conflicts.
	if _, err := env.svc.RedeemShare(context0, RedeemShareCommand{
		RedeemerPlayerID: 20002, InviterPlayerID: 10001,
		TokenID: token.TokenID, TokenGeneration: token.TokenGeneration,
		OperationID: "op-r1", PayloadHash: testHash("different"), NowMs: 500,
	}); err != ErrIdempotentConflict {
		t.Fatalf("payload conflict err = %v", err)
	}
	// Self redemption rejected.
	self := env.redeem(t, 10001, 10001, token, "op-self", 600)
	if self.Outcome.Status != StatusRejected || self.Outcome.Reason != ReasonSelfAdd {
		t.Fatalf("self = %+v", self.Outcome)
	}
}

func containsRelation(relations map[string]uint64, relationID string, epoch uint64) bool {
	active, ok := relations[relationID]
	return ok && active == epoch
}

func TestDeleteAndReactivationBarrierFlow(t *testing.T) {
	env := newTestEnv(t, 300)
	token := env.generateToken(t, 10001, 0)
	if r := env.redeem(t, 20002, 10001, token, "op-r1", 100); r.Outcome.Status != StatusSucceeded {
		t.Fatal(r.Outcome)
	}

	// Delete is the revocation linearization point: permits die immediately.
	outcome, err := env.svc.DeleteFriend(context0, 20002, 10001, "op-del", testHash("del"), 200)
	if err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("delete = %v %v", outcome, err)
	}
	auth, err := NewVisitAuthorizer(env.pairs)
	if err != nil {
		t.Fatal(err)
	}
	if _, visitOutcome, err := auth.AuthorizeVisit(context0, 20002, 10001, 10001, "op-join", "attempt-1", 300, 0); err != nil {
		t.Fatal(err)
	} else if visitOutcome.Status != StatusRejected || visitOutcome.Reason != ReasonVisitNotPermitted {
		t.Fatalf("visit after delete = %v/%v", visitOutcome.Status, visitOutcome.Reason)
	}

	// Barrier PENDING: even a fresh token cannot re-add.
	fresh := env.generateToken(t, 10001, 300) // rotates: previous token still ACTIVE? generate reuses valid token
	if fresh.TokenID != token.TokenID {
		t.Fatalf("expected token reuse, got %s", fresh.TokenID)
	}
	rejected := env.redeem(t, 20002, 10001, fresh, "op-r2", 400)
	if rejected.Outcome.Status != StatusRejected || rejected.Outcome.Reason != ReasonRelationReactivationPending {
		t.Fatalf("pending re-add = %+v", rejected.Outcome)
	}

	// Termination order backfills floors and releases both slots.
	if outcome, err := env.svc.ExecuteTerminationOrder(context0, 10001, 20002, 500); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("termination order = %v %v", outcome, err)
	}
	for _, player := range []uint64{10001, 20002} {
		snap, err := env.quotas.GetQuota(context0, player)
		if err != nil {
			t.Fatal(err)
		}
		if len(snap.ActiveRelations) != 0 {
			t.Fatalf("player %d quota not released: %+v", player, snap.ActiveRelations)
		}
	}

	// READY but the OLD token still cannot reactivate (generation <= floor).
	old := env.redeem(t, 20002, 10001, token, "op-r3", 600)
	if old.Outcome.Status != StatusRejected || old.Outcome.Reason == ReasonReactivationRotateRequired {
		t.Fatalf("old token re-add = %+v", old.Outcome)
	}

	// Inviter explicitly rotates for this pair barrier.
	proof, err := env.pairs.GetBarrierProof(context0, 10001, 20002)
	if err != nil {
		t.Fatal(err)
	}
	env.tokens++
	rotated, _, outcome2, err := env.shares.RotateForPairReactivation(context0, 10001, proof,
		fmt.Sprintf("tok-10001-%d", env.tokens),
		publicIdentity(10001, "ROTATE_SHARE_TOKEN", fmt.Sprintf("op-rot-%d", env.tokens)), testHash("rot"), 700)
	if err != nil || outcome2.Status != StatusSucceeded {
		t.Fatalf("typed rotate = %v %v", outcome2, err)
	}

	// New link works; barrier becomes CONSUMED in the activation commit.
	reactivated := env.redeem(t, 20002, 10001, rotated, "op-r4", 800)
	if reactivated.Outcome.Status != StatusSucceeded || reactivated.RelationEpoch != 3 {
		t.Fatalf("reactivation = %+v epoch=%d", reactivated.Outcome, reactivated.RelationEpoch)
	}
	pair, err := env.pairs.GetPair(context0, 10001, 20002)
	if err != nil {
		t.Fatal(err)
	}
	if pair.Barrier.State != BarrierConsumed {
		t.Fatalf("barrier = %v", pair.Barrier.State)
	}
}

func TestBlockImmediatelyRevokes(t *testing.T) {
	env := newTestEnv(t, 300)
	token := env.generateToken(t, 10001, 0)
	if r := env.redeem(t, 20002, 10001, token, "op-r1", 100); r.Outcome.Status != StatusSucceeded {
		t.Fatal(r.Outcome)
	}
	if outcome, err := env.svc.BlockPlayer(context0, 10001, 20002, "op-b", testHash("b"), 200); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("block = %v %v", outcome, err)
	}
	// Action admission rejected immediately.
	admission, err := env.pairs.CommitFriendActionAdmission(context0, ActionAdmissionCommand{
		Identity:        publicIdentity(20002, "CARE", "op-care"),
		PayloadHash:     testHash("care"),
		VisitorPlayerID: 20002, OwnerPlayerID: 10001, TargetFarmID: 10001,
		Scope: ActionScopeCare, TargetBindingHash: testHash("binding"), NowMs: 300,
	})
	if err != nil {
		t.Fatal(err)
	}
	if admission.Outcome.Status != StatusRejected || admission.Outcome.Reason != ReasonVisitNotPermitted {
		t.Fatalf("admission after block = %v/%v", admission.Outcome.Status, admission.Outcome.Reason)
	}
	// Re-add stays rejected while either direction blocks.
	if outcome, err := env.svc.UnblockPlayer(context0, 10001, 20002, "op-u", testHash("u"), 400); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("unblock = %v %v", outcome, err)
	}
	rejected := env.redeem(t, 20002, 10001, token, "op-r2", 500)
	if rejected.Outcome.Status != StatusRejected {
		t.Fatalf("re-add with stale token after block = %+v", rejected.Outcome)
	}
}

func TestFriendLimitSecondSideReleasesFirst(t *testing.T) {
	env := newTestEnv(t, 1) // limit 1 per player for the test
	token := env.generateToken(t, 10001, 0)
	if r := env.redeem(t, 20002, 10001, token, "op-r1", 100); r.Outcome.Status != StatusSucceeded {
		t.Fatal(r.Outcome)
	}
	// 20003 tries to add 10001 (already at limit) while 20003 is free.
	result := env.redeem(t, 20003, 10001, token, "op-r2", 200)
	if result.Outcome.Status != StatusRejected || result.Outcome.Reason != ReasonFriendLimitReached {
		t.Fatalf("limit = %+v", result.Outcome)
	}
	// The inviter-side reservation was compensated; only the original
	// relation occupies slots.
	snap, err := env.quotas.GetQuota(context0, 10001)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Occupancy() != 1 {
		t.Fatalf("inviter occupancy = %d, want 1 (reservation leaked)", snap.Occupancy())
	}
	snap3, err := env.quotas.GetQuota(context0, 20003)
	if err != nil {
		t.Fatal(err)
	}
	if snap3.Occupancy() != 0 {
		t.Fatalf("redeemer occupancy = %d, want 0", snap3.Occupancy())
	}
}
