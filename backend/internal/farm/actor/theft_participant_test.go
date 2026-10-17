package actor

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/contracts"
	"farmgame/internal/farm/domain"
	"farmgame/internal/identity"
	actorruntime "farmgame/internal/logic/actor/runtime"

	"google.golang.org/protobuf/proto"
)

func participantTheftCall(t *testing.T, farmID, visitorID uint64, operationID, cycleID string, nowMs int64) *contracts.ParticipantCall {
	t.Helper()
	pairID, err := identity.CanonicalFriendRelationID(farmID, visitorID)
	if err != nil {
		t.Fatal(err)
	}
	workflowID, err := identity.WorkflowID(identity.EffectiveIdempotencyKey{
		SubjectPlayerID: visitorID, StableCommandTypeName: "THEFT", OperationID: operationID,
		Root: identity.OperationRootRef{Kind: identity.RootKindFriendPair, CanonicalID: pairID},
	})
	if err != nil {
		t.Fatal(err)
	}
	attemptID, err := identity.TheftAttemptID(farmID, cycleID, visitorID)
	if err != nil {
		t.Fatal(err)
	}
	settlementID, err := identity.TheftSettlementID(workflowID)
	if err != nil {
		t.Fatal(err)
	}
	permit := &contractspb.ActionPermitSnapshot{
		PermitId: "permit-" + operationID, VisitorPlayerId: visitorID, OwnerPlayerId: farmID,
		TargetFarmId: farmID, OperationId: operationID,
		ActionScope: contractspb.FriendActionScope_FRIEND_ACTION_SCOPE_THEFT, FriendRelationEpoch: 1,
		CanonicalPayloadHash: bytes.Repeat([]byte{0x41}, 32), TargetBindingHash: bytes.Repeat([]byte{0x42}, 32),
		IssuedAtAuthorityMs: nowMs - 10, ExpireAtAuthorityMs: nowMs + 10_000,
		ExecutionAcceptedAtAuthorityMs: nowMs,
	}
	permitHash, err := contracts.CanonicalHash(permit)
	if err != nil {
		t.Fatal(err)
	}
	payload := &contractspb.DecideTheftPayload{
		VisitorPlayerId: visitorID, OwnerPlayerId: farmID, FarmId: farmID, PlotId: 1,
		CropCycleId: cycleID, TheftAttemptId: attemptID, SettlementId: settlementID,
		ActionPermitSnapshot: permit, ActionPermitSnapshotHash: permitHash[:],
	}
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT)
	if err != nil {
		t.Fatal(err)
	}
	stepHash, err := contracts.CanonicalParticipantHash(spec, payload)
	if err != nil {
		t.Fatal(err)
	}
	invocationID, err := identity.ParticipantInvocationID(identity.WorkTypeTheft, workflowID, identity.StepDecideTheft, 1)
	if err != nil {
		t.Fatal(err)
	}
	return &contracts.ParticipantCall{
		Spec: spec, RouteHeader: &logicrpcpb.ActorRouteHeader{RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM, ActorId: farmID},
		Work:       &contractspb.WorkAddress{WorkId: workflowID, WorkType: contractspb.WorkType_WORK_TYPE_THEFT, PlacementKind: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_INDEPENDENT_WORK_BUCKET, BucketVersion: 1},
		CallerKind: contracts.ParticipantCallerCoordinator, ParticipantStep: string(spec.Step), InvocationID: invocationID,
		OperationID: operationID, SubjectPlayerID: visitorID, ExpectedWorkVersion: 1, RemainingBudgetMs: 5000,
		Payload: payload, CanonicalStepPayloadHash: stepHash,
	}
}

func submitTheftParticipant(t *testing.T, rig *ar1Rig, call *contracts.ParticipantCall, nowMs int64) *contracts.StepOutcome {
	t.Helper()
	value, err := rig.registry.SubmitParticipant(context.Background(), call.RouteHeader.GetActorId(), func(ctx context.Context, actor *FarmActor, guard ParticipantCapacityGuard) (any, error) {
		return actor.DecideTheft(ctx, call, guard, func() int64 { return nowMs })
	})
	if err != nil {
		t.Fatal(err)
	}
	outcome, ok := value.(*contracts.StepOutcome)
	if !ok || outcome == nil {
		t.Fatalf("theft outcome=%T %v", value, value)
	}
	return outcome
}

func prepareParticipantTheftFarm(t *testing.T, rig *ar1Rig, farmID uint64) string {
	t.Helper()
	plant := submitParticipantCall(t, rig.registry, farmID, participantTestCall(t, farmID, "theft-plant", 1, 1), participantTestNowMs)
	if plant.Outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		t.Fatalf("plant=%+v", plant)
	}
	return plant.Result.(*contractspb.CommitPlantResult).GetCropCycleId()
}

func TestParticipantDecideTheftHappyReplayAndGuardProof(t *testing.T) {
	const farmID = uint64(1501)
	rig := ar1MemoryRig(t, farmID)
	cycleID := prepareParticipantTheftFarm(t, rig, farmID)
	call := participantTheftCall(t, farmID, 2001, "theft-a", cycleID, participantTestNowMs+60_000)
	first := submitTheftParticipant(t, rig, call, participantTestNowMs+60_000)
	if first.Outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		t.Fatalf("first=%+v", first)
	}
	decision := first.Result.(*contractspb.DecideTheftResult).GetTheftTargetDecision()
	if decision.GetGrossStolenQuantity() != 5 || decision.GetNetReceivedQuantity() != 5 || decision.GetStealSlotIndex() != 0 ||
		decision.GetGuardLostQuantity() != 0 || decision.GetGuardDecisionSnapshot().GetRandomDrawBp() >= 10000 || len(decision.GetTheftDecisionHash()) != 32 {
		t.Fatalf("decision=%+v", decision)
	}
	replay := submitTheftParticipant(t, rig, call, participantTestNowMs+999_000)
	if !replay.ReplayedFromReceipt || !bytes.Equal(first.CommittedResultHash, replay.CommittedResultHash) ||
		!bytes.Equal(mustParticipantResultBytes(t, first.Result), mustParticipantResultBytes(t, replay.Result)) {
		t.Fatalf("replay=%+v first=%+v", replay, first)
	}
	record, _, err := rig.registry.ReadOutsideMailbox(context.Background(), farmID)
	if err != nil {
		t.Fatal(err)
	}
	if record.GetCore().GetFarmVersion() != 2 || record.GetCore().GetCropCycles()[0].GetStolenTotal() != 5 ||
		len(record.GetRecentTheftAttempts()) != 1 || len(record.GetActiveSettlements()) != 0 {
		t.Fatalf("record theft projection=%+v", record)
	}
	proof, err := decodeTheftParticipantProof(record.GetRecentOperationReceipts()[1].GetResultPayload())
	if err != nil || proof.result.GetTheftTargetDecision().GetGuardDecisionSnapshot().GetRandomDrawBp() != decision.GetGuardDecisionSnapshot().GetRandomDrawBp() {
		t.Fatalf("proof=%+v err=%v", proof, err)
	}
}

func TestParticipantDecideTheftRejectsBeforeAttemptAndDifferentOperationCannotRepeat(t *testing.T) {
	const farmID = uint64(1502)
	rig := ar1MemoryRig(t, farmID)
	cycleID := prepareParticipantTheftFarm(t, rig, farmID)
	immatureCall := participantTheftCall(t, farmID, 2002, "theft-immature", cycleID, participantTestNowMs)
	immature := submitTheftParticipant(t, rig, immatureCall, participantTestNowMs)
	if immature.Outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED || immature.Failure.GetError().GetErrorCode() != commonpb.ErrorCode_ERR_CROP_NOT_MATURE {
		t.Fatalf("immature=%+v", immature)
	}
	legal := participantTheftCall(t, farmID, 2002, "theft-legal", cycleID, participantTestNowMs+60_000)
	first := submitTheftParticipant(t, rig, legal, participantTestNowMs+60_000)
	if first.Outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		t.Fatalf("legal=%+v", first)
	}
	retryDifferentOperation := participantTheftCall(t, farmID, 2002, "theft-other-operation", cycleID, participantTestNowMs+60_001)
	rejected := submitTheftParticipant(t, rig, retryDifferentOperation, participantTestNowMs+60_001)
	if rejected.Outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED || rejected.Failure.GetError().GetErrorCode() != commonpb.ErrorCode_ERR_STEAL_ALREADY_DONE {
		t.Fatalf("same visitor different operation=%+v", rejected)
	}
	record, _, err := rig.registry.ReadOutsideMailbox(context.Background(), farmID)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.GetRecentTheftAttempts()) != 1 || record.GetCore().GetFarmVersion() != 2 {
		t.Fatalf("rejection consumed authority facts: attempts=%d farm_version=%d", len(record.GetRecentTheftAttempts()), record.GetCore().GetFarmVersion())
	}
}

func TestParticipantDecideTheftTenSlotsConcurrentVisitorsAndReload(t *testing.T) {
	const farmID = uint64(1503)
	rig := ar1MemoryRig(t, farmID)
	cycleID := prepareParticipantTheftFarm(t, rig, farmID)
	type result struct {
		outcome *contracts.StepOutcome
		err     error
	}
	results := make(chan result, 10)
	var wg sync.WaitGroup
	for i := uint64(0); i < 10; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			call := participantTheftCall(t, farmID, 2100+i, "theft-race-"+string(rune('a'+i)), cycleID, participantTestNowMs+60_000)
			var value any
			var err error
			for attempt := 0; attempt < 100; attempt++ {
				value, err = rig.registry.SubmitParticipant(context.Background(), farmID, func(ctx context.Context, actor *FarmActor, guard ParticipantCapacityGuard) (any, error) {
					return actor.DecideTheft(ctx, call, guard, func() int64 { return participantTestNowMs + 60_000 })
				})
				if !errors.Is(err, actorruntime.ErrActorBusy) && !errors.Is(err, actorruntime.ErrMailboxFull) {
					break
				}
				time.Sleep(time.Millisecond)
				runtime.Gosched()
			}
			if err != nil {
				results <- result{err: err}
				return
			}
			outcome, ok := value.(*contracts.StepOutcome)
			if !ok {
				results <- result{err: errors.New("invalid theft race outcome")}
				return
			}
			results <- result{outcome: outcome}
		}()
	}
	wg.Wait()
	close(results)
	applied := 0
	for item := range results {
		if item.err != nil {
			t.Fatal(item.err)
		}
		if item.outcome.Outcome == contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
			applied++
		}
	}
	if applied != 10 {
		t.Fatalf("applied=%d, want ten mailbox-ordered slots", applied)
	}
	record, _, err := rig.registry.ReadOutsideMailbox(context.Background(), farmID)
	if err != nil {
		t.Fatal(err)
	}
	cycle := record.GetCore().GetCropCycles()[0]
	if cycle.GetStolenTotal() != 50 || record.GetCore().GetFarmVersion() != 11 || len(record.GetRecentTheftAttempts()) != 10 {
		t.Fatalf("ten-slot conservation: stolen=%d farm_version=%d attempts=%d", cycle.GetStolenTotal(), record.GetCore().GetFarmVersion(), len(record.GetRecentTheftAttempts()))
	}
	for i := uint64(0); i < 2; i++ {
		call := participantTheftCall(t, farmID, 2200+i, "theft-hotspot-overflow-"+string(rune('a'+i)), cycleID, participantTestNowMs+60_000)
		overflow := submitTheftParticipant(t, rig, call, participantTestNowMs+60_000)
		if (overflow.Outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED &&
			overflow.Outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE) ||
			overflow.Failure == nil || overflow.Failure.GetError().GetErrorCode() != commonpb.ErrorCode_ERR_STEAL_QUOTA_EXHAUSTED {
			t.Fatalf("overflow visitor=%d outcome=%+v, want stable quota rejection", 2200+i, overflow)
		}
	}
	record, _, err = rig.registry.ReadOutsideMailbox(context.Background(), farmID)
	if err != nil {
		t.Fatal(err)
	}
	if record.GetCore().GetCropCycles()[0].GetStolenTotal() != 50 || len(record.GetRecentTheftAttempts()) != 10 || record.GetCore().GetFarmVersion() != 11 {
		t.Fatalf("overflow consumed authority facts: cycle=%+v attempts=%d farm_version=%d", record.GetCore().GetCropCycles()[0], len(record.GetRecentTheftAttempts()), record.GetCore().GetFarmVersion())
	}
	if err := rig.registry.BeginPassivate(farmID); err != nil {
		t.Fatal(err)
	}
	if err := rig.registry.FinishPassivate(farmID); err != nil {
		t.Fatal(err)
	}
	call := participantTheftCall(t, farmID, 2100, "theft-race-a", cycleID, participantTestNowMs+60_000)
	replay := submitTheftParticipant(t, rig, call, participantTestNowMs+60_001)
	if !replay.ReplayedFromReceipt || replay.Outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		t.Fatalf("reload replay=%+v", replay)
	}
}

func TestParticipantDecideTheftGuardFullGrossIsSucceeded(t *testing.T) {
	const farmID = uint64(1504)
	rig := ar1MemoryRig(t, farmID)
	cycleID := prepareParticipantTheftFarm(t, rig, farmID)
	value, err := rig.registry.Submit(context.Background(), farmID, func(_ context.Context, actor *FarmActor) (any, error) {
		snapshot := domain.GuardSnapshot{
			SchemaVersion: 1, GuardVersion: 9, ActiveGuardPetID: 7, GuardUntilMs: participantTestNowMs + 120_000,
			Rule: domain.GuardRule{PetConfigVersion: 1, GuardRuleVersion: 2, TriggerProbabilityBP: 10_000, InterceptPolicy: domain.InterceptPolicyFullGross, RandomAlgorithmVersion: 3, RandomKeyVersion: 4},
		}
		snapshot.SnapshotHash = domain.CanonicalGuardSnapshotHash(snapshot)
		if err := actor.SetParticipantGuardSnapshot(snapshot); err != nil {
			return nil, err
		}
		actor.theftPRF = TheftPRF(func(string, domain.GuardSnapshot) (uint32, error) { return 0, nil })
		return nil, nil
	})
	if err != nil || value != nil {
		t.Fatalf("guard setup value=%v err=%v", value, err)
	}
	call := participantTheftCall(t, farmID, 2201, "theft-guard", cycleID, participantTestNowMs+60_000)
	outcome := submitTheftParticipant(t, rig, call, participantTestNowMs+60_000)
	decision := outcome.Result.(*contractspb.DecideTheftResult).GetTheftTargetDecision()
	if outcome.Outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED || decision.GetTheftOutcome() != contractspb.TheftOutcome_THEFT_OUTCOME_BLOCKED_BY_GUARD ||
		decision.GetGrossStolenQuantity() != decision.GetGuardLostQuantity() || decision.GetNetReceivedQuantity() != 0 {
		t.Fatalf("guard outcome=%+v", outcome)
	}
}

func TestParticipantDecideTheftAndHarvestConserveGrossQuantity(t *testing.T) {
	const farmID = uint64(1506)
	rig := ar1MemoryRig(t, farmID)
	cycleID := prepareParticipantTheftFarm(t, rig, farmID)
	theft := participantTheftCall(t, farmID, 2401, "theft-before-harvest", cycleID, participantTestNowMs+60_000)
	if outcome := submitTheftParticipant(t, rig, theft, participantTestNowMs+60_000); outcome.Outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		t.Fatalf("theft=%+v", outcome)
	}
	harvest := participantHarvestCall(t, farmID, "harvest-after-theft", cycleID, 1, 2)
	harvestOutcome, err := submitHarvestParticipantCall(t, rig.registry, farmID, harvest, participantTestNowMs+60_001)
	if err != nil {
		t.Fatal(err)
	}
	if harvestOutcome.Outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		t.Fatalf("harvest=%+v", harvestOutcome)
	}
	decision := harvestOutcome.Result.(*contractspb.CommitHarvestResult).GetHarvestDecision()
	if decision.GetRewardQuantity() != 245 {
		t.Fatalf("owner claimable=%d, want 245", decision.GetRewardQuantity())
	}
	record, _, err := rig.registry.ReadOutsideMailbox(context.Background(), farmID)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.GetCore().GetCropCycles()) != 0 || record.GetCore().GetFarmVersion() != 3 {
		t.Fatalf("harvest did not end typed theft cycle: %+v", record.GetCore())
	}
}

func TestValidateDecideTheftPayloadRejectsPermitForgery(t *testing.T) {
	const farmID = uint64(1505)
	pairID, err := identity.CanonicalFriendRelationID(farmID, 2301)
	if err != nil {
		t.Fatal(err)
	}
	workflowID, err := identity.WorkflowID(identity.EffectiveIdempotencyKey{
		SubjectPlayerID: 2301, StableCommandTypeName: "THEFT", OperationID: "theft-forgery-cycle",
		Root: identity.OperationRootRef{Kind: identity.RootKindFriendPair, CanonicalID: pairID},
	})
	if err != nil {
		t.Fatal(err)
	}
	cycleID, err := identity.CropCycleID(workflowID)
	if err != nil {
		t.Fatal(err)
	}
	call := participantTheftCall(t, farmID, 2301, "theft-forgery", cycleID, participantTestNowMs)
	call.Payload.(*contractspb.DecideTheftPayload).ActionPermitSnapshotHash[0] ^= 1
	if err := ValidateDecideTheftPayload(call); err == nil {
		t.Fatal("forged ActionPermitSnapshot hash was accepted")
	}
	if _, ok := call.Payload.(proto.Message); !ok {
		t.Fatal("theft payload lost typed message shape")
	}
}
