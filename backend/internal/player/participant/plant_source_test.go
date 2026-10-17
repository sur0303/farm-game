package participant

import (
	"testing"

	commonpb "farmgame/api/gen/common"
	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actorrouting"
	"farmgame/internal/contracts"
	"farmgame/internal/cropcatalog"
	"farmgame/internal/identity"
	playeractor "farmgame/internal/player/actor"
	"farmgame/internal/player/occupancy"
	"farmgame/internal/worker/sourceorder"

	"google.golang.org/protobuf/proto"
)

func TestPlantSourceOccupancyRejectsSecondOperationAndReplays(t *testing.T) {
	trig := newParticipantRig(t, "memory", plantTestPlayerID)
	seedPlayer(t, trig.registry, plantTestPlayerID, 2)
	server := newAllowServer(trig.registry)

	firstID := plantSourceOrderID(t, "first")
	first := mustExecute(t, server.ExecuteStep, plantSourceReserveRequest(t, firstID, plantTestPlayerID))
	if first.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED || first.GetReplayedFromReceipt() {
		t.Fatalf("first=%+v", first)
	}
	record := mustReadRecord(t, trig.registry)
	if playeractor.ItemCount(record, playeractor.ItemCarrotSeed) != 1 {
		t.Fatalf("seeds after first=%d", playeractor.ItemCount(record, playeractor.ItemCarrotSeed))
	}
	cycle, occupied := occupancy.PlotOccupied(record, 3)
	wantCycle, err := identity.CropCycleID(firstID)
	if err != nil || !occupied || cycle != wantCycle {
		t.Fatalf("occupancy=%v cycle=%q want=%q err=%v", occupied, cycle, wantCycle, err)
	}

	replay := mustExecute(t, server.ExecuteStep, plantSourceReserveRequest(t, firstID, plantTestPlayerID))
	if !replay.GetReplayedFromReceipt() || replay.GetCommittedRecordVersion() != first.GetCommittedRecordVersion() {
		t.Fatalf("replay=%+v first=%+v", replay, first)
	}
	if playeractor.ItemCount(mustReadRecord(t, trig.registry), playeractor.ItemCarrotSeed) != 1 {
		t.Fatal("replay deducted a second seed")
	}

	secondID := plantSourceOrderID(t, "second")
	second := mustExecute(t, server.ExecuteStep, plantSourceReserveRequest(t, secondID, plantTestPlayerID))
	if second.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED {
		t.Fatalf("second=%+v", second)
	}
	if second.GetFailure() == nil || second.GetFailure().GetError().GetErrorCode() != commonpb.ErrorCode_ERR_PLOT_NOT_EMPTY {
		t.Fatalf("second failure=%+v", second.GetFailure())
	}
	if playeractor.ItemCount(mustReadRecord(t, trig.registry), playeractor.ItemCarrotSeed) != 1 {
		t.Fatal("rejected second operation deducted another seed")
	}
}

func plantSourceOrderID(t *testing.T, operation string) string {
	t.Helper()
	orderID, err := sourceorder.PlantSourceOrderID(plantTestPlayerID, operation)
	if err != nil {
		t.Fatal(err)
	}
	return orderID
}

func plantSourceReserveRequest(t *testing.T, workID string, playerID uint64) *contractspb.ParticipantRequest {
	t.Helper()
	definition, ok := cropcatalog.ByDefinitionID(cropcatalog.CarrotDefinitionID)
	if !ok {
		t.Fatal("carrot definition missing")
	}
	snapshot, err := cropcatalog.BuildCropRuleSnapshot(definition)
	if err != nil {
		t.Fatal(err)
	}
	snapshotHash, err := contracts.CanonicalHash(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	payload := &contractspb.ReserveSeedPayload{
		PlayerId: playerID, FarmId: playerID, PlotId: 3, ReservationId: mustSeedReservation(t, workID),
		CropDefinitionId: uint32(definition.ID), SeedItemId: uint32(definition.SeedItemID), Quantity: 1,
		CropRuleVersion: definition.CropRuleVersion, CropRuleSnapshotHash: append([]byte(nil), snapshotHash[:]...),
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED)
	if err != nil {
		t.Fatal(err)
	}
	invocation, err := identity.ParticipantInvocationID(spec.WorkType, workID, spec.Step, 1)
	if err != nil {
		t.Fatal(err)
	}
	calc, err := actorrouting.NewCalculator("participant-test", actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608040804}, 64, 64)
	if err != nil {
		t.Fatal(err)
	}
	virtualShard, err := calc.ShardFor(actorrouting.DomainPlayer, playerID)
	if err != nil {
		t.Fatal(err)
	}
	domain := logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER
	work := &contractspb.WorkAddress{
		WorkId: workID, WorkType: contractspb.WorkType_WORK_TYPE_PLANT,
		PlacementKind: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET,
		BucketVersion: 1, BucketId: fixtureWorkflowBucket(workID),
		SourceAuthority: &contractspb.SourceAuthorityRef{
			AuthorityKind:          contractspb.SourceAuthorityKind_SOURCE_AUTHORITY_KIND_PLAYER_ACTOR,
			CanonicalAuthorityId:   sourceorder.CanonicalActorID(playerID),
			AuthorityLayoutVersion: plantTestLayout,
			ActorRouteDomain:       &domain,
		},
	}
	leaseUntilMs := participantTestLeaseUntilMs
	return &contractspb.ParticipantRequest{
		RouteHeader: &logicrpcpb.ActorRouteHeader{
			RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER, ActorId: playerID, VirtualShard: virtualShard,
			ShardRevision: 1, ExpectedHolderId: plantTestHolder, ExpectedHolderBootId: plantTestHolderBoot, ExpectedWriterEpoch: 1,
			ExpectedLeaseEpoch: 1, ExpectedLeaseUntilMs: &leaseUntilMs,
		},
		ParticipantCommand: contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED,
		TypedPayload:       raw,
		CallerContext: &contractspb.ParticipantRequest_Coordinator{Coordinator: &contractspb.CoordinatorWorkContext{
			Work: work, ExpectedWorkVersion: 1,
			ParticipantStep:         contractspb.ParticipantStep_PARTICIPANT_STEP_RESERVE_SEED,
			ParticipantInvocationId: invocation, OperationId: "operation-" + workID,
			SubjectPlayerId: playerID, RemainingBudgetMs: 1000,
		}},
	}
}
