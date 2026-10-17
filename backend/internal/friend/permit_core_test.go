package friend

import "testing"

func TestVisitPermitIssueAndBind(t *testing.T) {
	env := newTestEnv(t, 300)
	token := env.generateToken(t, 10001, 0)
	if r := env.redeem(t, 20002, 10001, token, "op-r1", 100); r.Outcome.Status != StatusSucceeded {
		t.Fatal(r.Outcome)
	}
	auth, err := NewVisitAuthorizer(env.pairs)
	if err != nil {
		t.Fatal(err)
	}

	permit, outcome, err := auth.AuthorizeVisit(context0, 20002, 10001, 10001, "op-join-1", "attempt-1", 200, 0)
	if err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("authorize = %v %v", outcome, err)
	}
	if permit.Scope != ActionScopeVisitFarm || permit.RelationEpoch != 1 || permit.RelationID == "" {
		t.Fatalf("permit = %+v", permit)
	}
	if permit.ExpireAtAuthorityMs != 200+DefaultVisitPermitTTLMs {
		t.Fatalf("permit ttl = %+v", permit)
	}

	// Hub binds while the permit is fresh and the epoch matches.
	bind, err := auth.BindCheck(context0, permit, 300)
	if err != nil || bind.Status != StatusSucceeded {
		t.Fatalf("bind = %v %v", bind, err)
	}

	// Delete before bind: the stale-epoch permit fails closed (33 §7.3).
	if _, err := env.svc.DeleteFriend(context0, 10001, 20002, "op-del", testHash("del"), 400); err != nil {
		t.Fatal(err)
	}
	bind, err = auth.BindCheck(context0, permit, 500)
	if err != nil {
		t.Fatal(err)
	}
	if bind.Status != StatusRejected || bind.Reason != ReasonDomainEpochConflict {
		t.Fatalf("stale bind = %v/%v", bind.Status, bind.Reason)
	}
}

func TestVisitPermitExpiry(t *testing.T) {
	env := newTestEnv(t, 300)
	token := env.generateToken(t, 10001, 0)
	if r := env.redeem(t, 20002, 10001, token, "op-r1", 100); r.Outcome.Status != StatusSucceeded {
		t.Fatal(r.Outcome)
	}
	auth, _ := NewVisitAuthorizer(env.pairs)
	permit, _, err := auth.AuthorizeVisit(context0, 20002, 10001, 10001, "op-join", "attempt", 200, 1000)
	if err != nil {
		t.Fatal(err)
	}
	bind, err := auth.BindCheck(context0, permit, 200+1000)
	if err != nil {
		t.Fatal(err)
	}
	if bind.Status != StatusRejected || bind.Detail != "VISIT_PERMIT_EXPIRED" {
		t.Fatalf("expired bind = %v/%v", bind.Status, bind.Detail)
	}
}

func TestActionAdmissionCommitAndRecovery(t *testing.T) {
	env := newTestEnv(t, 300)
	token := env.generateToken(t, 10001, 0)
	if r := env.redeem(t, 20002, 10001, token, "op-r1", 100); r.Outcome.Status != StatusSucceeded {
		t.Fatal(r.Outcome)
	}
	cmd := ActionAdmissionCommand{
		Identity:        publicIdentity(20002, "THEFT", "op-theft-1"),
		PayloadHash:     testHash("theft"),
		VisitorPlayerID: 20002, OwnerPlayerID: 10001, TargetFarmID: 10001,
		Scope: ActionScopeTheft, TargetBindingHash: testHash("plot-3-cycle-9"), NowMs: 200,
	}
	first, err := env.pairs.CommitFriendActionAdmission(context0, cmd)
	if err != nil {
		t.Fatal(err)
	}
	if first.Outcome.Status != StatusSucceeded || first.Permit.RelationEpoch != 1 || first.DispatchOrderID == "" {
		t.Fatalf("admission = %+v", first)
	}
	if first.Permit.Scope != ActionScopeTheft || first.Permit.ExecutionAcceptedAtAuthorityMs != 200 {
		t.Fatalf("permit = %+v", first.Permit)
	}

	// Same identity + same payload replays the frozen permit — this is the
	// TIMEOUT_UNKNOWN resolution path (33 §8.5).
	replay, err := env.pairs.CommitFriendActionAdmission(context0, cmd)
	if err != nil {
		t.Fatal(err)
	}
	if replay.Permit.PermitID != first.Permit.PermitID || replay.DispatchOrderID != first.DispatchOrderID {
		t.Fatal("replay must return the original permit and dispatch order")
	}
	// GetFriendActionAdmission resolves the same way after a lost response.
	resolved, err := env.pairs.GetFriendActionAdmission(context0, cmd)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Permit.PermitID != first.Permit.PermitID {
		t.Fatal("resolution must return the original permit")
	}

	// Same identity + different payload is an idempotency conflict.
	conflicting := cmd
	conflicting.PayloadHash = testHash("different")
	if _, err := env.pairs.CommitFriendActionAdmission(context0, conflicting); err != ErrIdempotentConflict {
		t.Fatalf("conflict err = %v", err)
	}

	// Worker recovery validates the frozen acceptance window, not the
	// current wall clock (33 §8.5).
	if err := VerifyAdmittedPermit(first.Permit); err != nil {
		t.Fatalf("admitted permit must survive recovery: %v", err)
	}
	// Even after the relation is deleted, the accepted operation may finish.
	if _, err := env.svc.DeleteFriend(context0, 10001, 20002, "op-del", testHash("del"), 300); err != nil {
		t.Fatal(err)
	}
	if err := VerifyAdmittedPermit(first.Permit); err != nil {
		t.Fatalf("accepted permit must not be re-expired by deletion: %v", err)
	}
	// But NEW admissions are rejected after deletion.
	second := cmd
	second.Identity = publicIdentity(20002, "THEFT", "op-theft-2")
	newAdmission, err := env.pairs.CommitFriendActionAdmission(context0, second)
	if err != nil {
		t.Fatal(err)
	}
	if newAdmission.Outcome.Status != StatusRejected {
		t.Fatalf("new admission after delete = %v", newAdmission.Outcome.Status)
	}
}

func TestAdmittedPermitRejectsMalformed(t *testing.T) {
	valid := ActionPermitSnapshot{
		PermitID: "p", RelationID: "r", RelationEpoch: 1,
		VisitorPlayerID: 1, OwnerPlayerID: 2, TargetFarmID: 2,
		OperationID: "op", Scope: ActionScopeCare,
		PayloadHash: []byte("h"), TargetBindingHash: []byte("b"),
		IssuedAtAuthorityMs: 100, ExpireAtAuthorityMs: 200,
		ExecutionAcceptedAtAuthorityMs: 150,
	}
	if err := VerifyAdmittedPermit(valid); err != nil {
		t.Fatal(err)
	}
	late := valid
	late.ExecutionAcceptedAtAuthorityMs = 250
	if err := VerifyAdmittedPermit(late); err == nil {
		t.Fatal("acceptance outside the window must fail")
	}
	visitScoped := valid
	visitScoped.Scope = ActionScopeVisitFarm
	if err := VerifyAdmittedPermit(visitScoped); err == nil {
		t.Fatal("VISIT scope must never authorize Care/Theft")
	}
	wrongFarm := valid
	wrongFarm.TargetFarmID = 999
	if err := VerifyAdmittedPermit(wrongFarm); err == nil {
		t.Fatal("farm id must equal owner id")
	}
}
