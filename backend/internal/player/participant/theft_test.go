package participant

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	contractspb "farmgame/api/gen/contracts"
	playerpb "farmgame/api/gen/player"
	"farmgame/internal/actorrouting"
	"farmgame/internal/contracts"
	"farmgame/internal/identity"
	playeractor "farmgame/internal/player/actor"
	playertask "farmgame/internal/player/task"
	"farmgame/internal/storage/authoritystore"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const theftTestVisitor uint64 = plantTestPlayerID

func newTheftTestServer(t *testing.T, registry *playeractor.Registry, resolvedAt func() int64, nowMs func() int64) *Server {
	t.Helper()
	calc, err := actorrouting.NewCalculator("participant-test", actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608040804}, 64, 64)
	if err != nil {
		t.Fatal(err)
	}
	return NewServer(Options{
		Registry:                registry,
		WorkVersionVerifier:     fixtureWorkVersionVerifier{},
		WorkLeaseVerifier:       fixtureWorkLeaseVerifier{},
		RouteFenceVerifier:      fixtureRouteFenceVerifier{calc: calc},
		WorkAddressVerifier:     fixtureWorkAddressVerifier{},
		IdentityBindingVerifier: fixtureIdentityBindingVerifier{},
		TheftDependencyVerifier: TheftDependencyVerifierFunc(func(_ context.Context, call *contracts.ParticipantCall) error {
			if call == nil || call.Work == nil || call.Work.GetWorkType() != contractspb.WorkType_WORK_TYPE_THEFT || call.ExpectedWorkVersion != 1 {
				return status.Error(codes.FailedPrecondition, "fixture Theft WorkRecord mismatch")
			}
			if resolvedAt != nil {
				call.TrustedResolvedAtMs = resolvedAt()
			}
			return nil
		}),
		TaskConfigRepository: playertask.MustNewP0Repository(),
		TaskTimezone:         playertask.DefaultGameTimezone,
		NowMs:                nowMs,
	})
}

func theftWorkID(t *testing.T, operation string) string {
	t.Helper()
	id, err := identity.WorkflowID(identity.EffectiveIdempotencyKey{
		SubjectPlayerID: theftTestVisitor, StableCommandTypeName: string(identity.WorkTypeTheft), OperationID: operation,
		Root: identity.OperationRootRef{Kind: identity.RootKindFarmActor, CanonicalID: "7002"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func theftDecision(t *testing.T, workID string, owner uint64, gross, lost uint64, outcome contractspb.TheftOutcome) *contractspb.TheftTargetDecision {
	t.Helper()
	cycleID, err := identity.CropCycleID(workID)
	if err != nil {
		t.Fatal(err)
	}
	settlementID, err := identity.TheftSettlementID(workID)
	if err != nil {
		t.Fatal(err)
	}
	attemptID, err := identity.TheftAttemptID(7002, cycleID, theftTestVisitor)
	if err != nil {
		t.Fatal(err)
	}
	decision := &contractspb.TheftTargetDecision{
		WorkflowId: workID, SettlementId: settlementID, TheftAttemptId: attemptID,
		VisitorPlayerId: theftTestVisitor, OwnerPlayerId: owner, FarmId: 7002, PlotId: 3,
		CropCycleId: cycleID, RewardItemId: uint32(playeractor.ItemCarrot), StealSlotIndex: 1,
		AppliedSlotRatioBp: 10000, GrossStolenQuantity: gross, GuardLostQuantity: lost,
		NetReceivedQuantity: gross - lost, CropUnitCoinValue: 15, GuardCoinValue: lost * 15,
		TheftOutcome: outcome, CropRuleVersion: 42,
		GuardDecisionSnapshot: &contractspb.GuardDecisionSnapshot{
			TheftAttemptId: attemptID, GuardVersion: 1, GuardRuleVersion: 1,
			TriggerProbabilityBp: 10000, InterceptPolicy: contractspb.GuardInterceptPolicy_GUARD_INTERCEPT_POLICY_FULL_GROSS,
			RandomAlgorithmVersion: 1, RandomKeyVersion: 1, RandomDrawBp: 1,
			GuardTriggered: lost > 0, GrossStolenQuantity: gross, GuardLostQuantity: lost,
			NetReceivedQuantity: gross - lost, CropUnitCoinValue: 15, GuardCoinValue: lost * 15,
		},
	}
	hash, err := TheftDecisionHash(decision)
	if err != nil {
		t.Fatal(err)
	}
	decision.TheftDecisionHash = append([]byte(nil), hash[:]...)
	return decision
}

func theftVisitorRequest(t *testing.T, workID string, decision *contractspb.TheftTargetDecision) *contractspb.ParticipantRequest {
	t.Helper()
	return makePlantRequest(t, workID, theftTestVisitor, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_SETTLE_THEFT_VISITOR, &contractspb.SettleTheftVisitorPayload{
		VisitorPlayerId: theftTestVisitor, DecideTheftResultHash: bytes.Repeat([]byte{7}, 32), TheftTargetDecision: proto.Clone(decision).(*contractspb.TheftTargetDecision),
	}, false)
}

func theftOwnerRequest(t *testing.T, workID string, owner uint64, decision *contractspb.TheftTargetDecision) *contractspb.ParticipantRequest {
	t.Helper()
	return makePlantRequest(t, workID, owner, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_CREDIT_GUARD_INCOME, &contractspb.CreditGuardIncomePayload{
		OwnerPlayerId: owner, DecideTheftResultHash: bytes.Repeat([]byte{7}, 32), SettleVisitorResultHash: bytes.Repeat([]byte{8}, 32), TheftTargetDecision: proto.Clone(decision).(*contractspb.TheftTargetDecision),
	}, false)
}

func theftRecord(t *testing.T, registry *playeractor.Registry, playerID uint64) *playerpb.PlayerRecord {
	t.Helper()
	record, _, err := registry.Read(context.Background(), playerID)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func TestTheftVisitorNetPositiveCommitsInventoryReceiptAndTask(t *testing.T) {
	trig := newParticipantRig(t, "memory", theftTestVisitor)
	if _, _, err := trig.registry.Ensure(context.Background(), theftTestVisitor, "theft-bootstrap"); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 5, 10, 0, 0, 0, time.FixedZone("CST", 8*60*60)).UnixMilli()
	server := newTheftTestServer(t, trig.registry, func() int64 { return now }, func() int64 { return now })
	workID := theftWorkID(t, "visitor-positive")
	request := theftVisitorRequest(t, workID, theftDecision(t, workID, 7002, 5, 0, contractspb.TheftOutcome_THEFT_OUTCOME_STOLEN))
	response := mustExecute(t, server.ExecuteStep, request)
	if response.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED || response.GetReplayedFromReceipt() {
		t.Fatalf("visitor response=%+v", response)
	}
	result := &contractspb.SettleTheftVisitorResult{}
	mustUnmarshal(t, response.GetTypedResult(), result)
	record := theftRecord(t, trig.registry, theftTestVisitor)
	if result.GetGrantedNetQuantity() != 5 || result.GetInventoryQuantityAfter() != 5 || playeractor.ItemCount(record, playeractor.ItemCarrot) != 5 || record.GetWallet().GetCoin() != playeractor.DefaultInitialCoin {
		t.Fatalf("visitor settlement result=%+v record=%+v", result, record)
	}
	state, err := playertask.DomainStateFromRecord(record)
	if err != nil || state.TaskStateVersion != 1 || state.DailyTasks[2].State != playertask.StateClaimable || state.DailyTasks[1].State != playertask.StateClaimable {
		t.Fatalf("visitor task state=%+v err=%v", state, err)
	}
}

func TestTheftVisitorZeroNetStillCommitsReceiptAndCannotRetry(t *testing.T) {
	trig := newParticipantRig(t, "memory", theftTestVisitor)
	if _, _, err := trig.registry.Ensure(context.Background(), theftTestVisitor, "theft-bootstrap-zero"); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 5, 11, 0, 0, 0, time.FixedZone("CST", 8*60*60)).UnixMilli()
	server := newTheftTestServer(t, trig.registry, func() int64 { return now }, func() int64 { return now })
	workID := theftWorkID(t, "visitor-zero")
	request := theftVisitorRequest(t, workID, theftDecision(t, workID, 7002, 5, 5, contractspb.TheftOutcome_THEFT_OUTCOME_BLOCKED_BY_GUARD))
	first := mustExecute(t, server.ExecuteStep, request)
	before, beforeVersion, err := trig.registry.Read(context.Background(), theftTestVisitor)
	if err != nil {
		t.Fatal(err)
	}
	if beforeVersion != first.GetCommittedRecordVersion() {
		t.Fatalf("record version=%d response=%d", beforeVersion, first.GetCommittedRecordVersion())
	}
	replay := mustExecute(t, server.ExecuteStep, request)
	after, afterVersion, err := trig.registry.Read(context.Background(), theftTestVisitor)
	if err != nil {
		t.Fatal(err)
	}
	if !replay.GetReplayedFromReceipt() || !bytes.Equal(first.GetTypedResult(), replay.GetTypedResult()) || afterVersion != beforeVersion || playeractor.ItemCount(after, playeractor.ItemCarrot) != playeractor.ItemCount(before, playeractor.ItemCarrot) {
		t.Fatalf("zero-net replay first=%+v replay=%+v before=%+v after=%+v", first, replay, before, after)
	}
	state, err := playertask.DomainStateFromRecord(after)
	if err != nil || state.DailyTasks[2].State != playertask.StateClaimable {
		t.Fatalf("zero-net task state=%+v err=%v", state, err)
	}
}

func TestTheftOwnerPositiveCoinAndZeroCoinRejected(t *testing.T) {
	owner := theftTestVisitor + 1
	trig := newParticipantRig(t, "memory", owner)
	if _, _, err := trig.registry.Ensure(context.Background(), owner, "owner-bootstrap"); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.FixedZone("CST", 8*60*60)).UnixMilli()
	server := newTheftTestServer(t, trig.registry, func() int64 { return now }, func() int64 { return now })
	positiveID := theftWorkID(t, "owner-positive")
	positive := theftDecision(t, positiveID, owner, 5, 5, contractspb.TheftOutcome_THEFT_OUTCOME_BLOCKED_BY_GUARD)
	response := mustExecute(t, server.ExecuteStep, theftOwnerRequest(t, positiveID, owner, positive))
	result := &contractspb.CreditGuardIncomeResult{}
	mustUnmarshal(t, response.GetTypedResult(), result)
	record := theftRecord(t, trig.registry, owner)
	if result.GetCreditedNormalCoin() != 75 || record.GetWallet().GetCoin() != playeractor.DefaultInitialCoin+75 || result.GetNormalCoinBalanceAfter() != uint64(record.GetWallet().GetCoin()) {
		t.Fatalf("owner result=%+v record=%+v", result, record)
	}

	zeroID := theftWorkID(t, "owner-zero")
	zero := theftDecision(t, zeroID, owner, 5, 0, contractspb.TheftOutcome_THEFT_OUTCOME_STOLEN)
	before, version, err := trig.registry.Read(context.Background(), owner)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.ExecuteStep(context.Background(), theftOwnerRequest(t, zeroID, owner, zero)); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("zero guard coin err=%v", err)
	}
	after, afterVersion, err := trig.registry.Read(context.Background(), owner)
	if err != nil {
		t.Fatal(err)
	}
	if afterVersion != version || after.GetWallet().GetCoin() != before.GetWallet().GetCoin() {
		t.Fatalf("zero owner call mutated version=%d/%d wallet=%d/%d", version, afterVersion, before.GetWallet().GetCoin(), after.GetWallet().GetCoin())
	}
}

func TestTheftHashForgeryAndConcurrentSettlementFailClosedOrReplay(t *testing.T) {
	trig := newParticipantRig(t, "memory", theftTestVisitor)
	if _, _, err := trig.registry.Ensure(context.Background(), theftTestVisitor, "theft-bootstrap-concurrent"); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 5, 13, 0, 0, 0, time.FixedZone("CST", 8*60*60)).UnixMilli()
	server := newTheftTestServer(t, trig.registry, func() int64 { return now }, func() int64 { return now })
	workID := theftWorkID(t, "concurrent")
	request := theftVisitorRequest(t, workID, theftDecision(t, workID, 7002, 3, 0, contractspb.TheftOutcome_THEFT_OUTCOME_STOLEN))
	for i := 0; i < 8; i++ {
		goRequest := proto.Clone(request).(*contractspb.ParticipantRequest)
		if i == 0 {
			mutateTypedPayload(t, goRequest, func(message proto.Message) {
				message.(*contractspb.SettleTheftVisitorPayload).TheftTargetDecision.TheftDecisionHash[0]++
			})
			if _, err := server.ExecuteStep(context.Background(), goRequest); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("forged Theft hash err=%v", err)
			}
			continue
		}
		// The valid request is intentionally executed concurrently below.
	}

	const workers = 7
	responses := make([]*contractspb.ParticipantResponse, workers)
	errorsSeen := make([]error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			responses[index], errorsSeen[index] = server.ExecuteStep(context.Background(), proto.Clone(request).(*contractspb.ParticipantRequest))
		}(i)
	}
	wg.Wait()
	for i := range responses {
		if errorsSeen[i] != nil || responses[i] == nil || responses[i].GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
			t.Fatalf("concurrent[%d] response=%+v err=%v", i, responses[i], errorsSeen[i])
		}
	}
	record := theftRecord(t, trig.registry, theftTestVisitor)
	if playeractor.ItemCount(record, playeractor.ItemCarrot) != 3 || len(record.GetRecentOperationReceipts()) != 1 {
		t.Fatalf("concurrent settlement duplicated record=%+v", record)
	}
}

func TestTheftMissingDependencyVerifierFailsBeforeMailbox(t *testing.T) {
	trig := newParticipantRig(t, "memory", theftTestVisitor)
	if _, _, err := trig.registry.Ensure(context.Background(), theftTestVisitor, "theft-bootstrap-missing-proof"); err != nil {
		t.Fatal(err)
	}
	calc, err := actorrouting.NewCalculator("participant-test", actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608040804}, 64, 64)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer(Options{
		Registry:                trig.registry,
		WorkVersionVerifier:     fixtureWorkVersionVerifier{},
		WorkLeaseVerifier:       fixtureWorkLeaseVerifier{},
		RouteFenceVerifier:      fixtureRouteFenceVerifier{calc: calc},
		WorkAddressVerifier:     fixtureWorkAddressVerifier{},
		IdentityBindingVerifier: fixtureIdentityBindingVerifier{},
		TaskConfigRepository:    playertask.MustNewP0Repository(),
		TaskTimezone:            playertask.DefaultGameTimezone,
	})
	workID := theftWorkID(t, "missing-proof")
	request := theftVisitorRequest(t, workID, theftDecision(t, workID, 7002, 1, 0, contractspb.TheftOutcome_THEFT_OUTCOME_STOLEN))
	before, version, err := trig.registry.Read(context.Background(), theftTestVisitor)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.ExecuteStep(context.Background(), request); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("missing Theft verifier err=%v", err)
	}
	after, afterVersion, err := trig.registry.Read(context.Background(), theftTestVisitor)
	if err != nil {
		t.Fatal(err)
	}
	if afterVersion != version || playeractor.ItemCount(after, playeractor.ItemCarrot) != playeractor.ItemCount(before, playeractor.ItemCarrot) {
		t.Fatalf("missing proof entered mailbox version=%d/%d record=%+v", version, afterVersion, after)
	}
}

func TestTheftResponseLossAndTaskResolvedAtCrossDay(t *testing.T) {
	trig := newParticipantRig(t, "memory", theftTestVisitor)
	if _, _, err := trig.registry.Ensure(context.Background(), theftTestVisitor, "theft-bootstrap-day"); err != nil {
		t.Fatal(err)
	}
	day1 := time.Date(2026, 8, 5, 23, 50, 0, 0, time.FixedZone("CST", 8*60*60)).UnixMilli()
	currentNow := day1
	resolved := day1
	server := newTheftTestServer(t, trig.registry, func() int64 { return resolved }, func() int64 { return currentNow })
	firstID := theftWorkID(t, "day-one-first")
	firstRequest := theftVisitorRequest(t, firstID, theftDecision(t, firstID, 7002, 1, 0, contractspb.TheftOutcome_THEFT_OUTCOME_STOLEN))
	trig.fault.Inject(authoritystore.MethodCommit, authoritystore.FaultTimeoutUnknownAfterApply)
	first := mustExecute(t, server.ExecuteStep, firstRequest)
	replay := mustExecute(t, server.ExecuteStep, firstRequest)
	if first.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED || !replay.GetReplayedFromReceipt() {
		t.Fatalf("response-loss first=%+v replay=%+v", first, replay)
	}
	trig.registry.Stop()
	trig.registry = playeractor.NewAuthorityRegistry(trig.provider, playeractor.RegistryOptions{MailboxSize: 64})
	t.Cleanup(trig.registry.Stop)
	server = newTheftTestServer(t, trig.registry, func() int64 { return resolved }, func() int64 { return currentNow })
	cold := mustExecute(t, server.ExecuteStep, firstRequest)
	if !cold.GetReplayedFromReceipt() || !bytes.Equal(cold.GetTypedResult(), first.GetTypedResult()) || cold.GetCommittedRecordVersion() != first.GetCommittedRecordVersion() {
		t.Fatalf("reload replay=%+v first=%+v", cold, first)
	}
	stateBeforeSecond, err := playertask.DomainStateFromRecord(theftRecord(t, trig.registry, theftTestVisitor))
	if err != nil {
		t.Fatal(err)
	}
	secondID := theftWorkID(t, "day-one-second")
	resolved = day1
	second := mustExecute(t, server.ExecuteStep, theftVisitorRequest(t, secondID, theftDecision(t, secondID, 7002, 1, 0, contractspb.TheftOutcome_THEFT_OUTCOME_STOLEN)))
	if second == nil {
		t.Fatal("second response is nil")
	}
	stateAfterSecond, err := playertask.DomainStateFromRecord(theftRecord(t, trig.registry, theftTestVisitor))
	if err != nil || stateAfterSecond.TaskStateVersion != stateBeforeSecond.TaskStateVersion {
		t.Fatalf("same-day task advanced twice before=%+v after=%+v err=%v", stateBeforeSecond, stateAfterSecond, err)
	}
	currentNow = day1 + int64(24*time.Hour/time.Millisecond)
	resolved = currentNow
	thirdID := theftWorkID(t, "day-two")
	mustExecute(t, server.ExecuteStep, theftVisitorRequest(t, thirdID, theftDecision(t, thirdID, 7002, 1, 0, contractspb.TheftOutcome_THEFT_OUTCOME_STOLEN)))
	stateAfterDay, err := playertask.DomainStateFromRecord(theftRecord(t, trig.registry, theftTestVisitor))
	if err != nil || stateAfterDay.DailyPeriodID == stateAfterSecond.DailyPeriodID || stateAfterDay.TaskStateVersion <= stateAfterSecond.TaskStateVersion {
		t.Fatalf("cross-day resolved_at did not switch task period before=%+v after=%+v err=%v", stateAfterSecond, stateAfterDay, err)
	}
}
