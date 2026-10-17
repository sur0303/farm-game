package friend

import (
	"fmt"
	"sync"
	"testing"
)

// One share token redeemed by 100 different players concurrently (33 §9):
// 100 independent pairs/workflows, no global use counter, no errors.
func TestConcurrentRedeemOneToken100Players(t *testing.T) {
	env := newTestEnv(t, 300)
	token := env.generateToken(t, 10001, 0)

	const redeemers = 100
	results := make([]RedeemShareResult, redeemers)
	var wg sync.WaitGroup
	for i := 0; i < redeemers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			redeemer := uint64(20000 + i)
			result, err := env.svc.RedeemShare(context0, RedeemShareCommand{
				RedeemerPlayerID: redeemer, InviterPlayerID: 10001,
				TokenID: token.TokenID, TokenGeneration: token.TokenGeneration,
				OperationID: fmt.Sprintf("op-redeem-%d", i),
				PayloadHash: testHash(fmt.Sprintf("redeem-%d", i)), NowMs: 100,
			})
			if err != nil {
				t.Error(err)
				return
			}
			results[i] = result
		}(i)
	}
	wg.Wait()

	relations := map[string]bool{}
	for i, result := range results {
		if result.Outcome.Status != StatusSucceeded {
			t.Fatalf("redeemer %d = %v/%v", i, result.Outcome.Status, result.Outcome.Reason)
		}
		if relations[result.RelationID] {
			t.Fatalf("duplicate relation id %s", result.RelationID)
		}
		relations[result.RelationID] = true
	}
	// The inviter occupies exactly 100 slots.
	snap, err := env.quotas.GetQuota(context0, 10001)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.ActiveRelations) != redeemers {
		t.Fatalf("inviter slots = %d, want %d", len(snap.ActiveRelations), redeemers)
	}
}

// Both directions add the same pair concurrently while a delete also races
// (33 §9): exactly one activation wins, the loser gets a deterministic
// NO_CHANGE/rejection, and the final state is consistent.
func TestConcurrentAddBothDirectionsAndDelete(t *testing.T) {
	env := newTestEnv(t, 300)
	tokenA := env.generateToken(t, 10001, 0)
	tokenB := env.generateToken(t, 20002, 0)

	var wg sync.WaitGroup
	outcomes := make([]Outcome, 3)
	// A redeems B's link; B redeems A's link; A deletes the pair.
	wg.Add(3)
	go func() {
		defer wg.Done()
		result, err := env.svc.RedeemShare(context0, RedeemShareCommand{
			RedeemerPlayerID: 10001, InviterPlayerID: 20002,
			TokenID: tokenB.TokenID, TokenGeneration: tokenB.TokenGeneration,
			OperationID: "op-a", PayloadHash: testHash("a"), NowMs: 100,
		})
		if err != nil {
			t.Error(err)
			return
		}
		outcomes[0] = result.Outcome
	}()
	go func() {
		defer wg.Done()
		result, err := env.svc.RedeemShare(context0, RedeemShareCommand{
			RedeemerPlayerID: 20002, InviterPlayerID: 10001,
			TokenID: tokenA.TokenID, TokenGeneration: tokenA.TokenGeneration,
			OperationID: "op-b", PayloadHash: testHash("b"), NowMs: 100,
		})
		if err != nil {
			t.Error(err)
			return
		}
		outcomes[1] = result.Outcome
	}()
	go func() {
		defer wg.Done()
		outcome, err := env.svc.DeleteFriend(context0, 10001, 20002, "op-del", testHash("del"), 100)
		if err != nil {
			t.Error(err)
			return
		}
		outcomes[2] = outcome
	}()
	wg.Wait()

	pair, err := env.pairs.GetPair(context0, 10001, 20002)
	if err != nil {
		t.Fatal(err)
	}
	// Deterministic terminal states only.
	for i, outcome := range outcomes {
		switch outcome.Status {
		case StatusSucceeded, StatusNoChange, StatusRejected:
		default:
			t.Fatalf("outcome %d indeterminate: %+v", i, outcome)
		}
	}
	// Delete's public success boundary is the Pair commit. If Delete wins after
	// activation but before quota finalization, active slots are an expected
	// transient until the deterministic termination order runs. Converge that
	// asynchronous boundary before asserting the terminal Pair/quota invariant.
	if pair.Relation.Status == RelationStatusDeleted && pair.PendingTermination != nil && !pair.PendingTermination.Completed {
		termination, terminationErr := env.svc.ExecuteTerminationOrder(context0, 10001, 20002, 101)
		if terminationErr != nil || (termination.Status != StatusSucceeded && termination.Status != StatusNoChange) {
			t.Fatalf("termination convergence=%+v err=%v", termination, terminationErr)
		}
		pair, err = env.pairs.GetPair(context0, 10001, 20002)
		if err != nil {
			t.Fatal(err)
		}
	}
	// Whatever the interleaving, the terminal canonical record and quotas now
	// agree: an ACTIVE relation occupies exactly one slot per side; a DELETED
	// relation occupies none after its termination order converges.
	snapA, err := env.quotas.GetQuota(context0, 10001)
	if err != nil {
		t.Fatal(err)
	}
	snapB, err := env.quotas.GetQuota(context0, 20002)
	if err != nil {
		t.Fatal(err)
	}
	if pair.Relation.Status == RelationStatusActive {
		if len(snapA.ActiveRelations) != 1 || len(snapB.ActiveRelations) != 1 {
			t.Fatalf("active relation but quotas = %d/%d", len(snapA.ActiveRelations), len(snapB.ActiveRelations))
		}
	} else {
		if len(snapA.ActiveRelations) != 0 || len(snapB.ActiveRelations) != 0 {
			t.Fatalf("deleted relation but active slots = %d/%d", len(snapA.ActiveRelations), len(snapB.ActiveRelations))
		}
	}
}

// Block racing activation (33 §9): Block first -> activation rejected;
// activation first -> block terminates the relation. No middle state.
func TestBlockVersusActivate(t *testing.T) {
	for trial := 0; trial < 20; trial++ {
		env := newTestEnv(t, 300)
		token := env.generateToken(t, 10001, 0)
		var wg sync.WaitGroup
		var redeemOutcome, blockOutcome Outcome
		wg.Add(2)
		go func() {
			defer wg.Done()
			result, err := env.svc.RedeemShare(context0, RedeemShareCommand{
				RedeemerPlayerID: 20002, InviterPlayerID: 10001,
				TokenID: token.TokenID, TokenGeneration: token.TokenGeneration,
				OperationID: "op-r", PayloadHash: testHash("r"), NowMs: 100,
			})
			if err != nil {
				t.Error(err)
				return
			}
			redeemOutcome = result.Outcome
		}()
		go func() {
			defer wg.Done()
			outcome, err := env.svc.BlockPlayer(context0, 10001, 20002, "op-block", testHash("block"), 50)
			if err != nil {
				t.Error(err)
				return
			}
			blockOutcome = outcome
		}()
		wg.Wait()

		pair, err := env.pairs.GetPair(context0, 10001, 20002)
		if err != nil {
			t.Fatal(err)
		}
		if !pair.HasActiveBlock() {
			t.Fatalf("trial %d: block missing", trial)
		}
		if pair.Relation.Status == RelationStatusActive {
			t.Fatalf("trial %d: relation ACTIVE despite committed block", trial)
		}
		if blockOutcome.Status != StatusSucceeded {
			t.Fatalf("trial %d: block = %v", trial, blockOutcome.Status)
		}
		switch redeemOutcome.Status {
		case StatusSucceeded:
			// Activation committed first; the block then terminated it.
			if pair.Relation.TerminationReason != TerminationBlocked {
				t.Fatalf("trial %d: termination = %v", trial, pair.Relation.TerminationReason)
			}
		case StatusRejected:
			if redeemOutcome.Reason != ReasonBlocked {
				t.Fatalf("trial %d: redeem reject = %v", trial, redeemOutcome.Reason)
			}
		default:
			t.Fatalf("trial %d: redeem = %v/%v", trial, redeemOutcome.Status, redeemOutcome.Reason)
		}
	}
}

// Permits issued before a delete must not authorize afterwards (stale epoch
// fails closed), and a post-delete re-add must not resurrect them either.
func TestOldEpochPermitFailsClosed(t *testing.T) {
	env := newTestEnv(t, 300)
	token := env.generateToken(t, 10001, 0)
	if r := env.redeem(t, 20002, 10001, token, "op-r1", 100); r.Outcome.Status != StatusSucceeded {
		t.Fatal(r.Outcome)
	}
	auth, _ := NewVisitAuthorizer(env.pairs)
	permit, _, err := auth.AuthorizeVisit(context0, 20002, 10001, 10001, "op-join", "attempt-1", 100, 60000)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.svc.DeleteFriend(context0, 10001, 20002, "op-del", testHash("del"), 200); err != nil {
		t.Fatal(err)
	}
	// Old permit at epoch 1: rejected while deleted.
	bind, err := auth.BindCheck(context0, permit, 300)
	if err != nil {
		t.Fatal(err)
	}
	if bind.Status != StatusRejected {
		t.Fatalf("stale permit bound: %v", bind.Status)
	}
	// Complete the barrier cycle and re-add at epoch 3.
	if _, err := env.svc.ExecuteTerminationOrder(context0, 10001, 20002, 400); err != nil {
		t.Fatal(err)
	}
	proof, err := env.pairs.GetBarrierProof(context0, 10001, 20002)
	if err != nil {
		t.Fatal(err)
	}
	env.tokens++
	rotated, _, _, err := env.shares.RotateForPairReactivation(context0, 10001, proof,
		fmt.Sprintf("tok-10001-%d", env.tokens),
		publicIdentity(10001, "ROTATE_SHARE_TOKEN", fmt.Sprintf("op-rot-%d", env.tokens)), testHash("rot"), 500)
	if err != nil {
		t.Fatal(err)
	}
	if r := env.redeem(t, 20002, 10001, rotated, "op-r2", 600); r.Outcome.Status != StatusSucceeded {
		t.Fatalf("reactivate = %+v", r.Outcome)
	}
	// The epoch-1 permit is still rejected: relation_epoch moved on.
	bind, err = auth.BindCheck(context0, permit, 700)
	if err != nil {
		t.Fatal(err)
	}
	if bind.Status != StatusRejected || bind.Reason != ReasonDomainEpochConflict {
		t.Fatalf("old epoch permit = %v/%v", bind.Status, bind.Reason)
	}
}

// Replay determinism for the public pair mutations: same operation replays
// the stored outcome, different payload conflicts (Unknown resolution).
func TestPairMutationReplayAndConflict(t *testing.T) {
	env := newTestEnv(t, 300)
	token := env.generateToken(t, 10001, 0)
	if r := env.redeem(t, 20002, 10001, token, "op-r1", 100); r.Outcome.Status != StatusSucceeded {
		t.Fatal(r.Outcome)
	}
	first, err := env.svc.DeleteFriend(context0, 10001, 20002, "op-del", testHash("del"), 200)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := env.svc.DeleteFriend(context0, 10001, 20002, "op-del", testHash("del"), 300)
	if err != nil {
		t.Fatal(err)
	}
	if replay.Status != first.Status || replay.Reason != first.Reason || replay.CommittedEpoch != first.CommittedEpoch {
		t.Fatalf("replay = %+v, want %+v", replay, first)
	}
	if _, err := env.svc.DeleteFriend(context0, 10001, 20002, "op-del", testHash("other"), 400); err != ErrIdempotentConflict {
		t.Fatalf("conflict err = %v", err)
	}
	// A new operation on the already-deleted relation is deterministic
	// NO_CHANGE, not an error.
	noop, err := env.svc.DeleteFriend(context0, 10001, 20002, "op-del-2", testHash("del2"), 500)
	if err != nil {
		t.Fatal(err)
	}
	if noop.Status != StatusNoChange || noop.Reason != ReasonNotFriends {
		t.Fatalf("second delete = %v/%v", noop.Status, noop.Reason)
	}
}
