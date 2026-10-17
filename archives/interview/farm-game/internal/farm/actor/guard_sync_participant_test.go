package actor

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"strconv"
	"testing"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/contracts"
	"farmgame/internal/farm/domain"
	"farmgame/internal/identity"
	"farmgame/internal/storage/blobstore"
)

func guardSyncCall(t *testing.T, farmID, version uint64, activePetID uint32, guardUntilMs int64, operationID string) *contracts.ParticipantCall {
	t.Helper()
	domainSnapshot := domain.GuardSnapshot{
		SchemaVersion: 1, GuardVersion: version, ActiveGuardPetID: activePetID, GuardUntilMs: guardUntilMs,
		Rule: domain.GuardRule{
			PetConfigVersion: 11, GuardRuleVersion: 12, TriggerProbabilityBP: 10_000,
			InterceptPolicy: domain.InterceptPolicyFullGross, RandomAlgorithmVersion: 13, RandomKeyVersion: 14,
		},
	}
	domainSnapshot.SnapshotHash = domain.CanonicalGuardSnapshotHash(domainSnapshot)
	wireSnapshot, err := participantProtoGuardSnapshot(domainSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	canonicalFarmID := strconv.FormatUint(farmID, 10)
	orderID, err := identity.SourceOrderID(identity.WorkTypeGuardSyncOrder, identity.SourceKindPlayerActor,
		canonicalFarmID, "PET_GUARD_SLOT", version)
	if err != nil {
		t.Fatal(err)
	}
	payload := &contractspb.ApplyGuardSnapshotPayload{
		OrderId: orderID, SourcePlayerId: farmID, TargetFarmId: farmID, GuardSnapshot: wireSnapshot,
		SnapshotHash: append([]byte(nil), wireSnapshot.GetSnapshotHash()...),
	}
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_GUARD_SNAPSHOT)
	if err != nil {
		t.Fatal(err)
	}
	stepHash, err := contracts.CanonicalParticipantHash(spec, payload)
	if err != nil {
		t.Fatal(err)
	}
	invocationID, err := identity.ParticipantInvocationID(identity.WorkTypeGuardSyncOrder, orderID, identity.StepApplyGuardSnapshot, 1)
	if err != nil {
		t.Fatal(err)
	}
	playerDomain := logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER
	return &contracts.ParticipantCall{
		Spec:        spec,
		RouteHeader: &logicrpcpb.ActorRouteHeader{RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM, ActorId: farmID},
		Work: &contractspb.WorkAddress{
			WorkId: orderID, WorkType: contractspb.WorkType_WORK_TYPE_GUARD_SYNC_ORDER,
			PlacementKind: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET, BucketVersion: 1,
			SourceAuthority: &contractspb.SourceAuthorityRef{
				AuthorityKind:        contractspb.SourceAuthorityKind_SOURCE_AUTHORITY_KIND_PLAYER_ACTOR,
				CanonicalAuthorityId: canonicalFarmID, AuthorityLayoutVersion: "v1",
				ActorRouteDomain: &playerDomain,
			},
		},
		CallerKind: contracts.ParticipantCallerCoordinator, ParticipantStep: string(spec.Step), InvocationID: invocationID,
		OperationID: operationID, SubjectPlayerID: farmID, ExpectedWorkVersion: 1, RemainingBudgetMs: 5_000,
		Payload: payload, CanonicalStepPayloadHash: stepHash,
	}
}

func submitGuardSync(t *testing.T, rig *ar1Rig, call *contracts.ParticipantCall, nowMs int64) (*contracts.StepOutcome, error) {
	t.Helper()
	value, err := rig.registry.SubmitParticipant(context.Background(), call.RouteHeader.GetActorId(), func(ctx context.Context, actor *FarmActor, guard ParticipantCapacityGuard) (any, error) {
		return actor.ApplyGuardSnapshot(ctx, call, guard, func() int64 { return nowMs })
	})
	if err != nil {
		return nil, err
	}
	outcome, ok := value.(*contracts.StepOutcome)
	if !ok || outcome == nil {
		return nil, errors.New("farm participant GuardSync returned an invalid outcome")
	}
	return outcome, nil
}

func TestGuardSyncHotActorImmediateTheftUsesV1(t *testing.T) {
	const farmID = uint64(1601)
	for name, rig := range ar1Rigs(t, farmID) {
		t.Run(name, func(t *testing.T) {
			cycleID := prepareParticipantTheftFarm(t, rig, farmID)
			guard := guardSyncCall(t, farmID, 1, 7, participantTestNowMs+120_000, "guard-v1")
			applied, err := submitGuardSync(t, rig, guard, participantTestNowMs+1)
			if err != nil || applied.Outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
				t.Fatalf("apply v1=%+v err=%v", applied, err)
			}
			theft := submitTheftParticipant(t, rig, participantTheftCall(t, farmID, 2601, "theft-after-v1", cycleID, participantTestNowMs+60_000), participantTestNowMs+60_000)
			decision := theft.Result.(*contractspb.DecideTheftResult).GetTheftTargetDecision()
			if decision.GetTheftOutcome() != contractspb.TheftOutcome_THEFT_OUTCOME_BLOCKED_BY_GUARD ||
				decision.GetGuardDecisionSnapshot().GetGuardVersion() != 1 || decision.GetNetReceivedQuantity() != 0 {
				t.Fatalf("next-turn Theft did not observe committed v1: %+v", decision)
			}
		})
	}
}

func TestGuardSyncDisabledV2ImmediateTheftAndStaleV1CannotRegress(t *testing.T) {
	const farmID = uint64(1602)
	rig := ar1MemoryRig(t, farmID)
	cycleID := prepareParticipantTheftFarm(t, rig, farmID)
	if outcome, err := submitGuardSync(t, rig, guardSyncCall(t, farmID, 1, 7, participantTestNowMs+120_000, "guard-v1"), participantTestNowMs+1); err != nil || outcome.Outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		t.Fatalf("apply v1=%+v err=%v", outcome, err)
	}
	if outcome, err := submitGuardSync(t, rig, guardSyncCall(t, farmID, 2, 0, participantTestNowMs, "guard-expired-v2"), participantTestNowMs+2); err != nil || outcome.Outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		t.Fatalf("apply expired v2=%+v err=%v", outcome, err)
	}
	stale, err := submitGuardSync(t, rig, guardSyncCall(t, farmID, 1, 7, participantTestNowMs+120_000, "guard-v1-late"), participantTestNowMs+3)
	if err != nil || !stale.ReplayedFromReceipt || stale.Outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		t.Fatalf("duplicate v1 did not replay its original result: stale=%+v err=%v", stale, err)
	}
	theft := submitTheftParticipant(t, rig, participantTheftCall(t, farmID, 2602, "theft-after-v2", cycleID, participantTestNowMs+60_000), participantTestNowMs+60_000)
	decision := theft.Result.(*contractspb.DecideTheftResult).GetTheftTargetDecision()
	if decision.GetTheftOutcome() != contractspb.TheftOutcome_THEFT_OUTCOME_STOLEN ||
		decision.GetGuardDecisionSnapshot().GetGuardVersion() != 2 || decision.GetNetReceivedQuantity() == 0 {
		t.Fatalf("disabled v2 regressed or still intercepted: %+v", decision)
	}
}

func TestGuardSyncPreviouslyUnseenStaleV1IsSupersededByV2(t *testing.T) {
	const farmID = uint64(1606)
	rig := ar1MemoryRig(t, farmID)
	if outcome, err := submitGuardSync(t, rig, guardSyncCall(t, farmID, 2, 0, participantTestNowMs, "guard-v2-first"), participantTestNowMs+1); err != nil || outcome.Outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		t.Fatalf("apply v2=%+v err=%v", outcome, err)
	}
	stale, err := submitGuardSync(t, rig, guardSyncCall(t, farmID, 1, 7, participantTestNowMs+120_000, "guard-v1-unseen"), participantTestNowMs+2)
	if err != nil || stale.Outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE ||
		stale.Result.(*contractspb.ApplyGuardSnapshotResult).GetApplyStatus() != contractspb.GuardApplyStatus_GUARD_APPLY_STATUS_SUPERSEDED {
		t.Fatalf("previously unseen stale v1=%+v err=%v", stale, err)
	}
}

func TestGuardSyncSameVersionConflictFailsClosedAndKeepsV2(t *testing.T) {
	const farmID = uint64(1603)
	rig := ar1MemoryRig(t, farmID)
	if outcome, err := submitGuardSync(t, rig, guardSyncCall(t, farmID, 2, 0, participantTestNowMs, "guard-v2"), participantTestNowMs+1); err != nil || outcome.Outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		t.Fatalf("apply v2=%+v err=%v", outcome, err)
	}
	conflict := guardSyncCall(t, farmID, 2, 9, participantTestNowMs+120_000, "guard-v2-conflict")
	rejected, err := submitGuardSync(t, rig, conflict, participantTestNowMs+2)
	if rejected != nil || err == nil || !bytes.Contains([]byte(err.Error()), []byte("PARTICIPANT_IDEMPOTENCY_CONFLICT")) {
		t.Fatalf("same-version conflict did not fail closed: outcome=%+v err=%v", rejected, err)
	}
	value, err := rig.registry.Submit(context.Background(), farmID, func(_ context.Context, actor *FarmActor) (any, error) {
		return actor.theftGuardSnapshot, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := value.(domain.GuardSnapshot)
	wantHash, _ := hex.DecodeString(snapshot.SnapshotHash)
	originalHash := guardSyncCall(t, farmID, 2, 0, participantTestNowMs, "guard-v2").Payload.(*contractspb.ApplyGuardSnapshotPayload).GetSnapshotHash()
	if snapshot.GuardVersion != 2 || snapshot.ActiveGuardPetID != 0 || !bytes.Equal(wantHash, originalHash) {
		t.Fatalf("conflict changed effective v2: snapshot=%+v", snapshot)
	}
}

func TestGuardSyncPassivationReloadPreservesV2AndOperationReplay(t *testing.T) {
	const farmID = uint64(1604)
	for name, rig := range ar1Rigs(t, farmID) {
		t.Run(name, func(t *testing.T) {
			call := guardSyncCall(t, farmID, 2, 0, participantTestNowMs, "guard-v2-reload")
			first, err := submitGuardSync(t, rig, call, participantTestNowMs+1)
			if err != nil {
				t.Fatal(err)
			}
			replay, err := submitGuardSync(t, rig, call, participantTestNowMs+2)
			if err != nil || !replay.ReplayedFromReceipt || !bytes.Equal(first.CommittedResultHash, replay.CommittedResultHash) {
				t.Fatalf("same-operation replay=%+v err=%v", replay, err)
			}
			if err := rig.registry.BeginPassivate(farmID); err != nil {
				t.Fatal(err)
			}
			if err := rig.registry.FinishPassivate(farmID); err != nil {
				t.Fatal(err)
			}
			value, err := rig.registry.Submit(context.Background(), farmID, func(_ context.Context, actor *FarmActor) (any, error) {
				return actor.theftGuardSnapshot, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if snapshot := value.(domain.GuardSnapshot); snapshot.GuardVersion != 2 || snapshot.ActiveGuardPetID != 0 {
				t.Fatalf("cold actor guard=%+v, want disabled v2", snapshot)
			}
			reloadedReplay, err := submitGuardSync(t, rig, call, participantTestNowMs+3)
			if err != nil || !reloadedReplay.ReplayedFromReceipt || !bytes.Equal(first.CommittedResultHash, reloadedReplay.CommittedResultHash) {
				t.Fatalf("reload replay=%+v err=%v", reloadedReplay, err)
			}
		})
	}
}

func TestGuardSyncWriterEpochFenceRejectsOldHolder(t *testing.T) {
	const farmID = uint64(1605)
	rig := ar1MemoryRig(t, farmID)
	if _, err := rig.registry.Submit(context.Background(), farmID, func(context.Context, *FarmActor) (any, error) { return nil, nil }); err != nil {
		t.Fatal(err)
	}
	rig.reassign(t, "farm-holder-b", 2, 2)
	_, err := submitGuardSync(t, rig, guardSyncCall(t, farmID, 1, 7, participantTestNowMs+120_000, "guard-fenced"), participantTestNowMs+1)
	if !errors.Is(err, blobstore.ErrWriterEpochMismatch) {
		t.Fatalf("old writer GuardSync err=%v, want WriterEpoch fence", err)
	}
}
