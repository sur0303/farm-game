package actor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	contractspb "farmgame/api/gen/contracts"
	farmpb "farmgame/api/gen/farm"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/contracts"
	"farmgame/internal/farm/domain"
	"farmgame/internal/identity"
	"farmgame/internal/logic/actor/activation"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/blobstore"
	"farmgame/internal/workidentity"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const participantTestNowMs int64 = 1_800_000_000_000

func TestParticipantCapacityEstimatesMatchDeterministicWireSize(t *testing.T) {
	record := InitialRecord(1001)
	before := proto.Clone(record)
	tests := []struct {
		name      string
		operation string
		estimate  func(*farmpb.FarmRecord) (int, error)
	}{
		{name: "plant", operation: "capacity-estimate", estimate: EstimateParticipantCommitPlantModelBytes},
		{name: "harvest", operation: "capacity-estimate-harvest", estimate: EstimateParticipantCommitHarvestModelBytes},
		{name: "theft", operation: "capacity-estimate-theft", estimate: EstimateParticipantDecideTheftModelBytes},
		{name: "guard", operation: "capacity-estimate-guard-sync", estimate: EstimateParticipantApplyGuardSnapshotModelBytes},
		{name: "unlock", operation: "capacity-estimate-unlock", estimate: EstimateParticipantCommitPlotUnlockModelBytes},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.estimate(record)
			if err != nil {
				t.Fatal(err)
			}
			candidate := proto.Clone(record).(*farmpb.FarmRecord)
			if err := appendParticipantReceipt(candidate, tt.operation, make([]byte, sha256.Size),
				make([]byte, participantMaxProofBytes), contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED,
				commonpb.ErrorCode_OK, time.UnixMilli(1).UnixMilli()); err != nil {
				t.Fatal(err)
			}
			encoded, err := marshalRecord(candidate)
			if err != nil {
				t.Fatal(err)
			}
			want := len(encoded) + len("record")
			if got != want {
				t.Fatalf("estimate=%d deterministic wire size=%d", got, want)
			}
			if !proto.Equal(record, before) {
				t.Fatal("capacity estimate mutated its input record")
			}
		})
	}
}

func TestBestEffortParticipantCapacityGuardSkipsModelProjection(t *testing.T) {
	guard := ParticipantCapacityGuard{bestEffort: true}
	if err := guard.CheckModelRecord(nil); err != nil {
		t.Fatalf("best-effort guard unexpectedly projected model: %v", err)
	}

	durableGuard := ParticipantCapacityGuard{}
	if err := durableGuard.CheckModelRecord(nil); !errors.Is(err, ErrParticipantCapacityUnavailable) {
		t.Fatalf("durable guard err=%v, want capacity unavailable", err)
	}
}

func TestFarmMutationReceiptAllowsCanonicalEmptyResult(t *testing.T) {
	wantHeader := domain.OpportunityHeader{
		SourceFarmVersion: 7, DirtyGeneration: 9, NextOpportunityAtMs: participantTestNowMs + 60_000,
		CandidateMask: domain.OpportunityMask(3),
	}
	payload, err := encodeFarmMutationReceiptPayload(&farmpb.FarmCommandResult{}, wantHeader)
	if err != nil {
		t.Fatal(err)
	}
	result, gotHeader, found, err := decodeFarmMutationReceiptPayload(payload)
	if err != nil || !found || result == nil || gotHeader != wantHeader {
		t.Fatalf("decoded result=%+v header=%+v found=%v err=%v", result, gotHeader, found, err)
	}
	record := &farmpb.FarmRecord{
		FarmId: 1001, Core: &farmpb.FarmCore{FarmId: 1001, FarmVersion: wantHeader.SourceFarmVersion},
		RecentOperationReceipts: []*farmpb.FarmOperationReceipt{{
			OperationId: "empty-compact-result", Status: commonpb.CommandStatus_SUCCEEDED, ResultPayload: payload,
		}},
	}
	latest, ok := participantLatestOpportunityHeader(record)
	if !ok || latest != wantHeader {
		t.Fatalf("latest header=%+v found=%v", latest, ok)
	}
}

func participantTestSnapshot() *contractspb.CropRuleSnapshot {
	activity := "activity-carrot"
	waterOffset := int64(10_000)
	pestOffset := int64(20_000)
	return &contractspb.CropRuleSnapshot{
		SchemaVersion:       1,
		CropDefinitionId:    42,
		CropRuleVersion:     7,
		SeedItemId:          1001,
		HarvestItemId:       2001,
		GrowDurationMs:      60_000,
		BaseYield:           250,
		HarvestExp:          13,
		CropUnitCoinValue:   4,
		LegacySeedPlantable: true,
		CompendiumRule: &contractspb.CompendiumUnlockRuleSnapshot{
			CropDefinitionId: 42, UnlockEligible: true, ActivityId: &activity,
			CatalogVersionAtPlant: 11, DefinitionVersionAtPlant: 3,
		},
		StealPlanTemplate: &contractspb.StealPlanTemplate{
			PolicyVersion: 2, TotalCapRatioBp: 2000,
			SlotRatioBp: []uint32{200, 200, 200, 200, 200, 200, 200, 200, 200, 200},
		},
		CarePlanTemplate: &contractspb.CarePlanTemplate{
			CareRuleVersion: 5,
			Incidents: []*contractspb.CarePlanIncidentTemplate{
				{CareType: contractspb.CareType_CARE_TYPE_WATER, Ordinal: 1, ScheduleMode: contractspb.CareScheduleMode_CARE_SCHEDULE_MODE_FIXED_OFFSET, FixedOffsetMs: &waterOffset},
				{CareType: contractspb.CareType_CARE_TYPE_REMOVE_PEST, Ordinal: 1, ScheduleMode: contractspb.CareScheduleMode_CARE_SCHEDULE_MODE_FIXED_OFFSET, FixedOffsetMs: &pestOffset},
			},
		},
	}
}

func participantTestCall(t *testing.T, farmID uint64, operationID string, plotID uint32, expectedPlotVersion uint64) *contracts.ParticipantCall {
	t.Helper()
	workflowID, err := workidentity.WorkflowID(farmID, "PLANT", operationID)
	if err != nil {
		t.Fatal(err)
	}
	reservationID, err := identity.SeedReservationID(workflowID)
	if err != nil {
		t.Fatal(err)
	}
	cycleID, err := identity.CropCycleID(workflowID)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := participantTestSnapshot()
	snapshotHash, err := contracts.CanonicalHash(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT)
	if err != nil {
		t.Fatal(err)
	}
	invocationID, err := identity.ParticipantInvocationID(identity.WorkTypePlant, workflowID, identity.StepCommitPlant, 1)
	if err != nil {
		t.Fatal(err)
	}
	payload := &contractspb.CommitPlantPayload{
		OwnerPlayerId: farmID, FarmId: farmID, PlotId: plotID, ExpectedPlotVersion: expectedPlotVersion,
		ReservationId: reservationID, ReserveSeedResultHash: bytes.Repeat([]byte{0x31}, 32), CropCycleId: cycleID,
		CropRuleSnapshot: snapshot, CropRuleSnapshotHash: snapshotHash[:],
	}
	stepHash, err := contracts.CanonicalParticipantHash(spec, payload)
	if err != nil {
		t.Fatal(err)
	}
	return &contracts.ParticipantCall{
		Spec: spec, RouteHeader: &logicrpcpb.ActorRouteHeader{RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM, ActorId: farmID},
		Work:       &contractspb.WorkAddress{WorkId: workflowID, WorkType: contractspb.WorkType_WORK_TYPE_PLANT, PlacementKind: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_INDEPENDENT_WORK_BUCKET, BucketVersion: 1},
		CallerKind: contracts.ParticipantCallerCoordinator, ParticipantStep: string(spec.Step), InvocationID: invocationID,
		OperationID: operationID, SubjectPlayerID: farmID, ExpectedWorkVersion: 1, RemainingBudgetMs: 5000,
		Payload: payload, CanonicalStepPayloadHash: stepHash,
	}
}

func submitParticipantCall(t *testing.T, registry *Registry, farmID uint64, call *contracts.ParticipantCall, nowMs int64) *contracts.StepOutcome {
	t.Helper()
	value, err := registry.SubmitParticipant(context.Background(), farmID, func(ctx context.Context, actor *FarmActor, guard ParticipantCapacityGuard) (any, error) {
		return actor.CommitPlant(ctx, call, guard, func() int64 { return nowMs })
	})
	if err != nil {
		t.Fatal(err)
	}
	outcome, ok := value.(*contracts.StepOutcome)
	if !ok || outcome == nil {
		t.Fatalf("outcome=%T %v", value, value)
	}
	return outcome
}

func participantHarvestCall(t *testing.T, farmID uint64, operationID, cycleID string, plotID uint32, expectedPlotVersion uint64) *contracts.ParticipantCall {
	t.Helper()
	workflowID, err := workidentity.WorkflowID(farmID, "HARVEST", operationID)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_HARVEST)
	if err != nil {
		t.Fatal(err)
	}
	invocationID, err := identity.ParticipantInvocationID(identity.WorkTypeHarvest, workflowID, identity.StepCommitHarvest, 1)
	if err != nil {
		t.Fatal(err)
	}
	payload := &contractspb.CommitHarvestPayload{
		OwnerPlayerId: farmID, FarmId: farmID, PlotId: plotID,
		CropCycleId: cycleID, ExpectedPlotVersion: expectedPlotVersion,
	}
	stepHash, err := contracts.CanonicalParticipantHash(spec, payload)
	if err != nil {
		t.Fatal(err)
	}
	return &contracts.ParticipantCall{
		Spec: spec, RouteHeader: &logicrpcpb.ActorRouteHeader{RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM, ActorId: farmID},
		Work:       &contractspb.WorkAddress{WorkId: workflowID, WorkType: contractspb.WorkType_WORK_TYPE_HARVEST, PlacementKind: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_INDEPENDENT_WORK_BUCKET, BucketVersion: 1},
		CallerKind: contracts.ParticipantCallerCoordinator, ParticipantStep: string(spec.Step), InvocationID: invocationID,
		OperationID: operationID, SubjectPlayerID: farmID, ExpectedWorkVersion: 1, RemainingBudgetMs: 5000,
		Payload: payload, CanonicalStepPayloadHash: stepHash,
	}
}

func plantParticipantForHarvest(t *testing.T, rig *ar1Rig, farmID uint64, operationID string) (string, uint64) {
	t.Helper()
	plant := submitParticipantCall(t, rig.registry, farmID, participantTestCall(t, farmID, operationID, 1, 1), participantTestNowMs)
	result, ok := plant.Result.(*contractspb.CommitPlantResult)
	if !ok || result == nil {
		t.Fatalf("plant result=%T %v", plant.Result, plant.Result)
	}
	return result.GetCropCycleId(), result.GetCommittedPlotVersion()
}

func submitHarvestParticipantCall(t *testing.T, registry *Registry, farmID uint64, call *contracts.ParticipantCall, nowMs int64) (*contracts.StepOutcome, error) {
	t.Helper()
	value, err := registry.SubmitParticipant(context.Background(), farmID, func(ctx context.Context, actor *FarmActor, guard ParticipantCapacityGuard) (any, error) {
		return actor.CommitHarvest(ctx, call, guard, func() int64 { return nowMs })
	})
	if err != nil {
		return nil, err
	}
	outcome, ok := value.(*contracts.StepOutcome)
	if !ok || outcome == nil {
		return nil, errors.New("farm participant harvest returned an invalid outcome")
	}
	return outcome, nil
}

func TestParticipantCommitPlantPersistsOneReceiptCycleAndReplay(t *testing.T) {
	rig := ar1MemoryRig(t, 1201)
	call := participantTestCall(t, 1201, "plant-participant-1", 1, 1)
	first := submitParticipantCall(t, rig.registry, 1201, call, participantTestNowMs)
	if first.Outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED || first.CommittedRecordVersion == 0 {
		t.Fatalf("first outcome=%+v", first)
	}
	if first.Result == nil || first.Result.(*contractspb.CommitPlantResult).GetPlantAtMs() != participantTestNowMs {
		t.Fatalf("first result=%v", first.Result)
	}
	replay := submitParticipantCall(t, rig.registry, 1201, call, participantTestNowMs+999_000)
	if !replay.ReplayedFromReceipt || replay.Outcome != first.Outcome || replay.CommittedRecordVersion != first.CommittedRecordVersion {
		t.Fatalf("replay=%+v first=%+v", replay, first)
	}
	if !bytes.Equal(replay.CommittedResultHash, first.CommittedResultHash) || !bytes.Equal(mustParticipantResultBytes(t, replay.Result), mustParticipantResultBytes(t, first.Result)) {
		t.Fatal("replay changed the typed result/hash")
	}
	value, err := rig.registry.Submit(context.Background(), 1201, func(_ context.Context, actor *FarmActor) (any, error) {
		return actor.record, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	record := value.(*farmpb.FarmRecord)
	if len(record.GetCore().GetCropCycles()) != 1 || len(record.GetRecentOperationReceipts()) != 1 {
		t.Fatalf("cycles=%d receipts=%d", len(record.GetCore().GetCropCycles()), len(record.GetRecentOperationReceipts()))
	}
	proof, err := decodeParticipantProof(record.GetRecentOperationReceipts()[0].GetResultPayload())
	if err != nil || len(proof.careSnapshot.GetIncidents()) != 2 || proof.header.DirtyGeneration != 1 {
		t.Fatalf("proof=%+v err=%v", proof, err)
	}
	if proof.careSnapshot.GetIncidents()[0].GetIncidentId() == "" {
		t.Fatal("care incident id was not persisted")
	}
}

func mustParticipantResultBytes(t *testing.T, result proto.Message) []byte {
	t.Helper()
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestParticipantCommitPlantPersistsBusinessRejectionsWithoutFarmChange(t *testing.T) {
	tests := []struct {
		name   string
		plot   uint32
		setup  func(*FarmActor)
		expect commonpb.ErrorCode
	}{
		{name: "locked", plot: 6, expect: commonpb.ErrorCode_ERR_PLOT_LOCKED},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rig := ar1MemoryRig(t, 1210+uint64(len(test.name)))
			farmID := 1210 + uint64(len(test.name))
			call := participantTestCall(t, farmID, "plant-reject-"+test.name, test.plot, 99)
			if test.plot == 6 {
				call.Payload.(*contractspb.CommitPlantPayload).ExpectedPlotVersion = 1
			}
			if test.setup != nil {
				_, err := rig.registry.Submit(context.Background(), farmID, func(_ context.Context, actor *FarmActor) (any, error) {
					test.setup(actor)
					return nil, nil
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			value, err := rig.registry.SubmitParticipant(context.Background(), farmID, func(ctx context.Context, actor *FarmActor, guard ParticipantCapacityGuard) (any, error) {
				return actor.CommitPlant(ctx, call, guard, func() int64 { return participantTestNowMs })
			})
			if err != nil {
				t.Fatal(err)
			}
			outcome := value.(*contracts.StepOutcome)
			if outcome.Outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED || outcome.Failure == nil || outcome.Failure.GetError().GetErrorCode() != test.expect {
				t.Fatalf("outcome=%+v", outcome)
			}
			if outcome.CommittedRecordVersion == 0 {
				t.Fatal("rejection did not commit its receipt")
			}
		})
	}
}

func TestParticipantCommitPlantNonEmptyIsDurableNoFarmChange(t *testing.T) {
	const farmID = uint64(1218)
	rig := ar1MemoryRig(t, farmID)
	firstCall := participantTestCall(t, farmID, "plant-nonempty-first", 1, 1)
	_ = submitParticipantCall(t, rig.registry, farmID, firstCall, participantTestNowMs)

	secondCall := participantTestCall(t, farmID, "plant-nonempty-second", 1, 2)
	second := submitParticipantCall(t, rig.registry, farmID, secondCall, participantTestNowMs+1_000)
	if second.Outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED ||
		second.Failure.GetError().GetErrorCode() != commonpb.ErrorCode_ERR_PLOT_NOT_EMPTY {
		t.Fatalf("non-empty outcome=%+v", second)
	}
	replay := submitParticipantCall(t, rig.registry, farmID, secondCall, participantTestNowMs+2_000)
	if !replay.ReplayedFromReceipt || !bytes.Equal(replay.CommittedResultHash, second.CommittedResultHash) {
		t.Fatalf("non-empty replay=%+v", replay)
	}
	record, _, err := rig.registry.ReadOutsideMailbox(context.Background(), farmID)
	if err != nil {
		t.Fatal(err)
	}
	if record.GetCore().GetFarmVersion() != 1 || len(record.GetCore().GetCropCycles()) != 1 || len(record.GetRecentOperationReceipts()) != 2 {
		t.Fatalf("non-empty changed business state: farm_version=%d cycles=%d receipts=%d", record.GetCore().GetFarmVersion(), len(record.GetCore().GetCropCycles()), len(record.GetRecentOperationReceipts()))
	}
}

func TestParticipantCommitPlantConcurrentSamePlotHasOneSuccess(t *testing.T) {
	const farmID = uint64(1219)
	rig := ar1MemoryRig(t, farmID)
	calls := []*contracts.ParticipantCall{
		participantTestCall(t, farmID, "plant-race-a", 1, 1),
		participantTestCall(t, farmID, "plant-race-b", 1, 1),
	}
	type result struct {
		outcome *contracts.StepOutcome
		err     error
	}
	results := make(chan result, len(calls))
	var wg sync.WaitGroup
	for _, call := range calls {
		call := call
		wg.Add(1)
		go func() {
			defer wg.Done()
			value, err := rig.registry.SubmitParticipant(context.Background(), farmID, func(ctx context.Context, actor *FarmActor, guard ParticipantCapacityGuard) (any, error) {
				return actor.CommitPlant(ctx, call, guard, func() int64 { return participantTestNowMs })
			})
			if err != nil {
				results <- result{err: err}
				return
			}
			outcome, ok := value.(*contracts.StepOutcome)
			if !ok {
				results <- result{err: errors.New("participant race returned invalid outcome")}
				return
			}
			results <- result{outcome: outcome}
		}()
	}
	wg.Wait()
	close(results)
	applied, rejected := 0, 0
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		switch result.outcome.Outcome {
		case contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED:
			applied++
		case contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED:
			rejected++
		default:
			t.Fatalf("race outcome=%+v", result.outcome)
		}
	}
	if applied != 1 || rejected != 1 {
		t.Fatalf("race applied=%d rejected=%d", applied, rejected)
	}
	record, _, err := rig.registry.ReadOutsideMailbox(context.Background(), farmID)
	if err != nil {
		t.Fatal(err)
	}
	if record.GetCore().GetFarmVersion() != 1 || len(record.GetCore().GetCropCycles()) != 1 || len(record.GetRecentOperationReceipts()) != 2 {
		t.Fatalf("race persisted duplicate state: farm_version=%d cycles=%d receipts=%d", record.GetCore().GetFarmVersion(), len(record.GetCore().GetCropCycles()), len(record.GetRecentOperationReceipts()))
	}
}

func TestParticipantCommitPlantUsesFrozenSnapshotAfterReload(t *testing.T) {
	rig := ar1MemoryRig(t, 1220)
	call := participantTestCall(t, 1220, "plant-freeze", 1, 1)
	first := submitParticipantCall(t, rig.registry, 1220, call, participantTestNowMs)
	firstResult := first.Result.(*contractspb.CommitPlantResult)
	if err := rig.registry.BeginPassivate(1220); err != nil {
		t.Fatal(err)
	}
	if err := rig.registry.FinishPassivate(1220); err != nil {
		t.Fatal(err)
	}
	replayCall := participantTestCall(t, 1220, "plant-freeze", 1, 1)
	replay := submitParticipantCall(t, rig.registry, 1220, replayCall, participantTestNowMs+999_000)
	if !replay.ReplayedFromReceipt || !bytes.Equal(replay.CommittedResultHash, first.CommittedResultHash) {
		t.Fatalf("reload replay=%+v first=%+v", replay, first)
	}
	call.Payload.(*contractspb.CommitPlantPayload).CropRuleSnapshot.GrowDurationMs = 999_999
	mutatedHash, err := contracts.CanonicalParticipantHash(call.Spec, call.Payload)
	if err != nil {
		t.Fatal(err)
	}
	call.CanonicalStepPayloadHash = mutatedHash
	// The same invocation must replay from the persisted receipt even after
	// passivation; the altered in-memory request is intentionally a conflict.
	_, err = rig.registry.SubmitParticipant(context.Background(), 1220, func(ctx context.Context, actor *FarmActor, guard ParticipantCapacityGuard) (any, error) {
		return actor.CommitPlant(ctx, call, guard, func() int64 { return participantTestNowMs + 10_000 })
	})
	if err == nil {
		t.Fatal("different canonical snapshot must not replay")
	}
	if firstResult.GetMatureAtMs() != participantTestNowMs+60_000 {
		t.Fatalf("first maturity=%d", firstResult.GetMatureAtMs())
	}
}

func TestParticipantTimeoutBeforeApplyResolvesThenRetriesWithoutReceipt(t *testing.T) {
	const farmID = uint64(1225)
	rig := ar1MemoryRig(t, farmID)
	call := participantTestCall(t, farmID, "plant-timeout-before-apply", 1, 1)
	rig.fault.Inject(authoritystore.MethodCommit, authoritystore.FaultTimeoutUnknownBeforeApply)
	value, err := rig.registry.SubmitParticipant(context.Background(), farmID, func(ctx context.Context, actor *FarmActor, guard ParticipantCapacityGuard) (any, error) {
		return actor.CommitPlant(ctx, call, guard, func() int64 { return participantTestNowMs })
	})
	if outcome, ok := value.(*contracts.StepOutcome); (ok && outcome != nil) || !errors.Is(err, blobstore.ErrTimeoutUnknown) {
		t.Fatalf("timeout-before-apply value=%v err=%v", value, err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for rig.registry.Activation().StateOf(farmID) != activation.Active && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if rig.registry.Activation().StateOf(farmID) != activation.Active {
		t.Fatalf("timeout-before-apply state=%s", rig.registry.Activation().StateOf(farmID))
	}
	retry := submitParticipantCall(t, rig.registry, farmID, call, participantTestNowMs+10_000)
	if retry.Outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED || retry.ReplayedFromReceipt {
		t.Fatalf("timeout-before-apply retry=%+v", retry)
	}
	record, _, err := rig.registry.ReadOutsideMailbox(context.Background(), farmID)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.GetCore().GetCropCycles()) != 1 || len(record.GetRecentOperationReceipts()) != 1 {
		t.Fatalf("timeout-before-apply persisted cycles=%d receipts=%d", len(record.GetCore().GetCropCycles()), len(record.GetRecentOperationReceipts()))
	}
}

func TestParticipantPlotIDAdapterCoversBoundariesAndRepeat(t *testing.T) {
	for _, test := range []struct {
		plotID uint32
		index  int32
		valid  bool
	}{
		{plotID: 0, valid: false},
		{plotID: 1, index: 0, valid: true},
		{plotID: 24, index: 23, valid: true},
		{plotID: 25, valid: false},
	} {
		index, valid := participantPublicPlotIndex(test.plotID)
		if valid != test.valid || (valid && index != test.index) {
			t.Fatalf("PlotID %d -> index=%d valid=%v, want index=%d valid=%v", test.plotID, index, valid, test.index, test.valid)
		}
		if valid {
			if roundTrip, ok := participantPlotID(index); !ok || roundTrip != test.plotID {
				t.Fatalf("index %d round trip=%d/%v", index, roundTrip, ok)
			}
		}
	}

	cases := []struct {
		farmID uint64
		plotID uint32
		op     string
		index  int32
	}{
		{farmID: 1240, plotID: 1, op: "plot-adapter-one", index: 0},
		{farmID: 1241, plotID: 24, op: "plot-adapter-twenty-four", index: 23},
	}
	for _, test := range cases {
		t.Run(test.op, func(t *testing.T) {
			rig := ar1MemoryRig(t, test.farmID)
			if test.plotID == 24 {
				if _, err := rig.registry.Submit(context.Background(), test.farmID, func(_ context.Context, actor *FarmActor) (any, error) {
					return nil, actor.SetParticipantPlotUnlockedForTest(24, true)
				}); err != nil {
					t.Fatal(err)
				}
			}
			call := participantTestCall(t, test.farmID, test.op, test.plotID, 1)
			first := submitParticipantCall(t, rig.registry, test.farmID, call, participantTestNowMs)
			if first.Outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
				t.Fatalf("first=%+v", first)
			}
			replay := submitParticipantCall(t, rig.registry, test.farmID, call, participantTestNowMs+1000)
			if !replay.ReplayedFromReceipt {
				t.Fatalf("repeat was not receipt replay: %+v", replay)
			}
			record, _, err := rig.registry.ReadOutsideMailbox(context.Background(), test.farmID)
			if err != nil {
				t.Fatal(err)
			}
			var found int
			for _, plot := range record.GetCore().GetPlots() {
				if plot != nil && plot.GetIndex() == test.index {
					found++
				}
				if plot != nil && plot.GetIndex() == int32(participantMaxPlots) {
					t.Fatalf("PlotID %d created forbidden public index 24", test.plotID)
				}
			}
			if found != 1 || len(record.GetCore().GetCropCycles()) != 1 {
				t.Fatalf("public mapping found=%d plots=%v cycles=%d", found, record.GetCore().GetPlots(), len(record.GetCore().GetCropCycles()))
			}
			if got := record.GetCore().GetCropCycles()[0].GetPlotIndex(); got != test.index {
				t.Fatalf("cycle public index=%d, want %d", got, test.index)
			}
		})
	}
}

func TestParticipantConcurrentResolversSerializeAndKeepCommittedGuard(t *testing.T) {
	const farmID = uint64(1245)
	rig := ar1MemoryRig(t, farmID)
	rig.registry.SetAutoResolveUnknown(false)
	call := participantTestCall(t, farmID, "participant-concurrent-resolver", 1, 1)
	rig.fault.Inject(authoritystore.MethodCommit, authoritystore.FaultTimeoutUnknownAfterApply)
	value, err := rig.registry.SubmitParticipant(context.Background(), farmID, func(ctx context.Context, actor *FarmActor, guard ParticipantCapacityGuard) (any, error) {
		return actor.CommitPlant(ctx, call, guard, func() int64 { return participantTestNowMs })
	})
	if outcome, ok := value.(*contracts.StepOutcome); (ok && outcome != nil) || !errors.Is(err, blobstore.ErrTimeoutUnknown) {
		t.Fatalf("unknown value=%v err=%v", value, err)
	}

	const resolverCount = 8
	results := make(chan struct {
		resolution UnknownResolution
		err        error
	}, resolverCount)
	var wg sync.WaitGroup
	for i := 0; i < resolverCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resolution, resolveErr := rig.registry.ResolveUnknown(context.Background(), farmID)
			results <- struct {
				resolution UnknownResolution
				err        error
			}{resolution: resolution, err: resolveErr}
		}()
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent resolvers did not complete")
	}
	close(results)
	committed := 0
	for result := range results {
		if result.err == nil {
			if result.resolution != UnknownCommitted {
				t.Fatalf("successful resolver resolution=%v", result.resolution)
			}
			committed++
			continue
		}
		if !errors.Is(result.err, activation.ErrInvalidTransition) {
			t.Fatalf("unexpected concurrent resolver error=%v", result.err)
		}
	}
	if committed != 1 {
		t.Fatalf("successful resolver count=%d, want exactly one", committed)
	}
	if got := rig.registry.Activation().StateOf(farmID); got != activation.Active {
		t.Fatalf("resolved activation=%s", got)
	}
	if _, ok := rig.registry.UnknownContext(farmID); ok {
		t.Fatal("unknown context leaked after concurrent resolution")
	}
	value, err = rig.registry.SubmitSystem(context.Background(), farmID, 0, func(context.Context, *FarmActor) (any, error) {
		_, _, guarded := rig.registry.actors[farmID].guardSnapshot()
		return guarded, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if guarded, ok := value.(bool); !ok || guarded {
		t.Fatalf("committed unknown guard=%v", value)
	}
}

func TestParticipantUnknownLeaseLossSettlesGuardBeforeAuthorityEviction(t *testing.T) {
	const farmID = uint64(1247)
	rig := ar1MemoryRig(t, farmID)
	rig.registry.SetAutoResolveUnknown(false)
	if _, err := rig.registry.Submit(context.Background(), farmID, func(context.Context, *FarmActor) (any, error) {
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	call := participantTestCall(t, farmID, "participant-unknown-lease-loss", 1, 1)
	rig.fault.Inject(authoritystore.MethodCommit, authoritystore.FaultTimeoutUnknownAfterApply)
	value, err := rig.registry.SubmitParticipant(context.Background(), farmID, func(ctx context.Context, actor *FarmActor, guard ParticipantCapacityGuard) (any, error) {
		return actor.CommitPlant(ctx, call, guard, func() int64 { return participantTestNowMs })
	})
	if outcome, ok := value.(*contracts.StepOutcome); (ok && outcome != nil) || !errors.Is(err, blobstore.ErrTimeoutUnknown) {
		t.Fatalf("unknown value=%v err=%v", value, err)
	}
	if got := rig.registry.Activation().StateOf(farmID); got != activation.ResolvingUnknown {
		t.Fatalf("activation=%s, want RESOLVING_UNKNOWN", got)
	}
	if _, ok := rig.registry.UnknownContext(farmID); !ok {
		t.Fatal("unknown context was lost before resolution")
	}
	var guarded bool
	if _, err := rig.registry.SubmitSystem(context.Background(), farmID, 0, func(context.Context, *FarmActor) (any, error) {
		_, _, guarded = rig.registry.actors[farmID].guardSnapshot()
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	if !guarded {
		t.Fatal("committed UNKNOWN must retain its delivery Guard before resolution")
	}

	rig.registry.SetWritableCheck(func(uint64) bool { return false })
	invalidBefore := rig.registry.SubscriptionHub().InvalidTotal()
	resolution, err := rig.registry.ResolveUnknown(context.Background(), farmID)
	if !errors.Is(err, activation.ErrStaleRoute) || resolution != UnknownStillUnknown {
		t.Fatalf("resolution=%v err=%v, want stale route with unresolved result", resolution, err)
	}
	if got := rig.registry.SubscriptionHub().InvalidTotal(); got <= invalidBefore {
		t.Fatalf("lease-loss eviction bypassed Guard fence: invalid_total=%d before=%d", got, invalidBefore)
	}
	if got := rig.registry.Activation().StateOf(farmID); got != activation.Absent {
		t.Fatalf("settled fenced eviction state=%s, want ABSENT", got)
	}
}

func TestParticipantCommittedUnknownResolvesGuardThroughDeliveryFence(t *testing.T) {
	const farmID = uint64(1246)
	rig := ar1MemoryRigWithLaneSize(t, farmID, 1)
	rig.registry.SetAutoResolveUnknown(false)
	if _, err := rig.registry.Submit(context.Background(), farmID, func(context.Context, *FarmActor) (any, error) {
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	// Fill the one ordinary Hub event slot. The UNKNOWN turn must retain its
	// immutable candidate; resolver delivery then has to use the lane-full
	// Invalidate+Resync fence before clearing it.
	started := make(chan struct{})
	releaseLane := make(chan struct{})
	var releaseLaneOnce sync.Once
	release := func() {
		releaseLaneOnce.Do(func() { close(releaseLane) })
	}
	t.Cleanup(release)
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- rig.registry.SubscriptionHub().BlockLaneForTest(farmID, started, releaseLane)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("hub lane blocker did not start")
	}
	queuedDone := make(chan error, 1)
	go func() {
		_, err := rig.registry.SubscriptionHub().Execute(context.Background(), farmID, nil, func() (any, error) {
			return nil, nil
		})
		queuedDone <- err
	}()
	deadline := time.Now().Add(time.Second)
	for rig.registry.SubscriptionHub().LaneEventQueueLenForTest(farmID) != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := rig.registry.SubscriptionHub().LaneEventQueueLenForTest(farmID); got != 1 {
		t.Fatalf("hub ordinary queue=%d, want full", got)
	}

	call := participantTestCall(t, farmID, "participant-guard-fence", 1, 1)
	rig.fault.Inject(authoritystore.MethodCommit, authoritystore.FaultTimeoutUnknownAfterApply)
	value, err := rig.registry.SubmitParticipant(context.Background(), farmID, func(ctx context.Context, actor *FarmActor, guard ParticipantCapacityGuard) (any, error) {
		return actor.CommitPlant(ctx, call, guard, func() int64 { return participantTestNowMs })
	})
	if outcome, ok := value.(*contracts.StepOutcome); (ok && outcome != nil) || !errors.Is(err, blobstore.ErrTimeoutUnknown) {
		t.Fatalf("unknown value=%v err=%v", value, err)
	}
	invalidBefore := rig.registry.SubscriptionHub().InvalidTotal()
	var resolution UnknownResolution
	resolutionDone := make(chan struct {
		resolution UnknownResolution
		err        error
	}, 1)
	go func() {
		resolution, resolveErr := rig.registry.ResolveUnknown(context.Background(), farmID)
		resolutionDone <- struct {
			resolution UnknownResolution
			err        error
		}{resolution: resolution, err: resolveErr}
	}()
	select {
	case result := <-resolutionDone:
		// A broken implementation may clear the guard immediately without
		// entering the Hub. Keep the result so the assertion below still proves
		// that no delivery fence was installed.
		resolution, err = result.resolution, result.err
	case <-time.After(25 * time.Millisecond):
		release()
		select {
		case result := <-resolutionDone:
			resolution, err = result.resolution, result.err
		case <-time.After(time.Second):
			t.Fatal("resolver did not finish after releasing Hub lane")
		}
	}
	if err != nil || resolution != UnknownCommitted {
		t.Fatalf("resolution=%v err=%v", resolution, err)
	}
	if got := rig.registry.SubscriptionHub().InvalidTotal(); got <= invalidBefore {
		t.Fatalf("committed UNKNOWN cleared guard without delivery fence: invalid_total %d -> %d", invalidBefore, got)
	}
	value, err = rig.registry.SubmitSystem(context.Background(), farmID, 0, func(context.Context, *FarmActor) (any, error) {
		_, _, guarded := rig.registry.actors[farmID].guardSnapshot()
		return guarded, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if guarded, ok := value.(bool); !ok || guarded {
		t.Fatalf("guard after committed UNKNOWN resolution=%v", value)
	}
	release()
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("hub lane blocker did not release")
	}
	select {
	case err := <-queuedDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued hub event did not drain")
	}
}

func TestParticipantProofCareIDsStableAcrossHundredReceiptDecodes(t *testing.T) {
	rig := ar1MemoryRig(t, 1230)
	call := participantTestCall(t, 1230, "plant-stable-care", 1, 1)
	_ = submitParticipantCall(t, rig.registry, 1230, call, participantTestNowMs)
	record, _, err := rig.registry.ReadOutsideMailbox(context.Background(), 1230)
	if err != nil {
		t.Fatal(err)
	}
	var want string
	var wantDue int64
	for i := 0; i < 100; i++ {
		proof, decodeErr := decodeParticipantProof(record.GetRecentOperationReceipts()[0].GetResultPayload())
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		incident := proof.careSnapshot.GetIncidents()[0]
		if i == 0 {
			want, wantDue = incident.GetIncidentId(), incident.GetDueAtMs()
		} else if incident.GetIncidentId() != want || incident.GetDueAtMs() != wantDue {
			t.Fatalf("iteration=%d incident=%s/%d want=%s/%d", i, incident.GetIncidentId(), incident.GetDueAtMs(), want, wantDue)
		}
	}
}

func TestParticipantCommitHarvestHappyPathFreezesDecisionAndReplaysSilently(t *testing.T) {
	const farmID = uint64(1260)
	rig := ar1MemoryRig(t, farmID)
	cycleID, plantVersion := plantParticipantForHarvest(t, rig, farmID, "harvest-happy-plant")
	call := participantHarvestCall(t, farmID, "harvest-happy", cycleID, 1, plantVersion)
	first, err := submitHarvestParticipantCall(t, rig.registry, farmID, call, participantTestNowMs+60_000)
	if err != nil || first.Outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	result, ok := first.Result.(*contractspb.CommitHarvestResult)
	if !ok || result == nil || result.GetHarvestDecision() == nil {
		t.Fatalf("harvest result=%T %v", first.Result, first.Result)
	}
	decision := result.GetHarvestDecision()
	if decision.GetWorkflowId() != call.Work.GetWorkId() || decision.GetOwnerPlayerId() != farmID ||
		decision.GetFarmId() != farmID || decision.GetPlotId() != 1 || decision.GetCropCycleId() != cycleID ||
		decision.GetCropDefinitionId() != 42 || decision.GetRewardItemId() != 2001 || decision.GetRewardQuantity() != 250 ||
		decision.GetRewardExp() != 13 || decision.GetCropRuleVersion() != 7 || len(decision.GetHarvestDecisionHash()) != 32 {
		t.Fatalf("decision=%v", decision)
	}
	if !proto.Equal(decision.GetCompendiumUnlockSnapshot(), &contractspb.CompendiumUnlockSnapshot{
		CropDefinitionId: 42, UnlockEligible: true, PlantedByPlayerId: farmID,
		ActivityId: ptrString("activity-carrot"), CatalogVersionAtPlant: 11, DefinitionVersionAtPlant: 3,
	}) {
		t.Fatalf("compendium snapshot=%v", decision.GetCompendiumUnlockSnapshot())
	}
	if err := validateHarvestDecisionHash(decision); err != nil {
		t.Fatal(err)
	}
	replay, err := submitHarvestParticipantCall(t, rig.registry, farmID, call, participantTestNowMs+600_000)
	if err != nil || !replay.ReplayedFromReceipt || replay.CommittedRecordVersion != first.CommittedRecordVersion ||
		!bytes.Equal(replay.CommittedResultHash, first.CommittedResultHash) ||
		!bytes.Equal(mustParticipantResultBytes(t, replay.Result), mustParticipantResultBytes(t, first.Result)) {
		t.Fatalf("replay=%+v err=%v first=%+v", replay, err, first)
	}
	record, _, err := rig.registry.ReadOutsideMailbox(context.Background(), farmID)
	if err != nil {
		t.Fatal(err)
	}
	plot := participantPublicPlot(record, 1)
	if plot == nil || plot.GetActiveCropCycleId() != "" || plot.GetPlotVersion() != plantVersion+1 ||
		record.GetCore().GetFarmVersion() != 2 || len(record.GetCore().GetCropCycles()) != 0 || len(record.GetRecentOperationReceipts()) != 2 {
		t.Fatalf("committed harvest record=%v", record)
	}
	proof, err := decodeHarvestParticipantProof(record.GetRecentOperationReceipts()[1].GetResultPayload())
	if err != nil || proof.endReason != domain.CycleEndReasonHarvested || proof.endedCycleID != cycleID || proof.header.DirtyGeneration != 2 {
		t.Fatalf("harvest proof=%+v err=%v", proof, err)
	}
}

func ptrString(value string) *string { return &value }

func TestParticipantCommitHarvestRejectsImmatureStaleCycleAndPlotVersionDurably(t *testing.T) {
	tests := []struct {
		name        string
		now         int64
		cycle       string
		plotVersion uint64
		wantCode    commonpb.ErrorCode
	}{
		{name: "immature", now: participantTestNowMs + 59_999, plotVersion: 2, wantCode: commonpb.ErrorCode_ERR_CROP_NOT_MATURE},
		{name: "stale plot version", now: participantTestNowMs + 60_000, plotVersion: 1, wantCode: commonpb.ErrorCode_ERR_VERSION_CONFLICT},
		{name: "stale cycle", now: participantTestNowMs + 60_000, cycle: "old-cycle-id", plotVersion: 2, wantCode: commonpb.ErrorCode_ERR_VERSION_CONFLICT},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			farmID := uint64(1270 + index)
			rig := ar1MemoryRig(t, farmID)
			cycleID, _ := plantParticipantForHarvest(t, rig, farmID, "harvest-reject-plant")
			if test.cycle == "" {
				test.cycle = cycleID
			}
			call := participantHarvestCall(t, farmID, "harvest-reject-"+test.name, test.cycle, 1, test.plotVersion)
			outcome, err := submitHarvestParticipantCall(t, rig.registry, farmID, call, test.now)
			if err != nil || outcome == nil || outcome.Outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED ||
				outcome.Failure.GetError().GetErrorCode() != test.wantCode || outcome.CommittedRecordVersion == 0 {
				t.Fatalf("outcome=%+v err=%v", outcome, err)
			}
			record, _, err := rig.registry.ReadOutsideMailbox(context.Background(), farmID)
			if err != nil {
				t.Fatal(err)
			}
			if record.GetCore().GetFarmVersion() != 1 || len(record.GetCore().GetCropCycles()) != 1 ||
				participantPublicPlot(record, 1).GetActiveCropCycleId() != cycleID || participantPublicPlot(record, 1).GetPlotVersion() != 2 ||
				len(record.GetRecentOperationReceipts()) != 2 {
				t.Fatalf("rejected harvest changed state=%v", record)
			}
		})
	}
}

func TestParticipantCommitHarvestRejectsNonOwnerDurably(t *testing.T) {
	const farmID = uint64(1273)
	rig := ar1MemoryRig(t, farmID)
	cycleID, plantVersion := plantParticipantForHarvest(t, rig, farmID, "harvest-non-owner-plant")
	call := participantHarvestCall(t, farmID, "harvest-non-owner", cycleID, 1, plantVersion)
	payload := proto.Clone(call.Payload.(*contractspb.CommitHarvestPayload)).(*contractspb.CommitHarvestPayload)
	payload.OwnerPlayerId = farmID + 1
	call.Payload = payload
	call.SubjectPlayerID = farmID + 1
	call.CanonicalStepPayloadHash, _ = contracts.CanonicalParticipantHash(call.Spec, payload)

	outcome, err := submitHarvestParticipantCall(t, rig.registry, farmID, call, participantTestNowMs+60_000)
	if err != nil || outcome == nil || outcome.Outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED ||
		outcome.Failure.GetError().GetErrorCode() != commonpb.ErrorCode_ERR_PERMISSION_DENIED {
		t.Fatalf("non-owner outcome=%+v err=%v", outcome, err)
	}
	record, _, err := rig.registry.ReadOutsideMailbox(context.Background(), farmID)
	if err != nil {
		t.Fatal(err)
	}
	if record.GetCore().GetFarmVersion() != 1 || len(record.GetCore().GetCropCycles()) != 1 ||
		participantPublicPlot(record, 1).GetActiveCropCycleId() != cycleID || len(record.GetRecentOperationReceipts()) != 2 {
		t.Fatalf("non-owner changed state=%v", record)
	}
}

func TestParticipantCommitHarvestSameInvocationHashConflict(t *testing.T) {
	const farmID = uint64(1280)
	rig := ar1MemoryRig(t, farmID)
	cycleID, plantVersion := plantParticipantForHarvest(t, rig, farmID, "harvest-conflict-plant")
	call := participantHarvestCall(t, farmID, "harvest-conflict", cycleID, 1, plantVersion)
	if outcome, err := submitHarvestParticipantCall(t, rig.registry, farmID, call, participantTestNowMs+60_000); err != nil || outcome.Outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		t.Fatalf("first=%+v err=%v", outcome, err)
	}
	conflict := *call
	conflictPayload := proto.Clone(call.Payload.(*contractspb.CommitHarvestPayload)).(*contractspb.CommitHarvestPayload)
	conflictPayload.ExpectedPlotVersion++
	conflict.Payload = conflictPayload
	conflict.CanonicalStepPayloadHash, _ = contracts.CanonicalParticipantHash(call.Spec, conflictPayload)
	_, err := submitHarvestParticipantCall(t, rig.registry, farmID, &conflict, participantTestNowMs+60_000)
	if status.Code(err) != codes.FailedPrecondition || !bytes.Contains([]byte(err.Error()), []byte("IDEMPOTENCY_CONFLICT")) {
		t.Fatalf("conflict err=%v", err)
	}
	record, _, err := rig.registry.ReadOutsideMailbox(context.Background(), farmID)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.GetCore().GetCropCycles()) != 0 || record.GetCore().GetFarmVersion() != 2 || len(record.GetRecentOperationReceipts()) != 2 {
		t.Fatalf("conflict changed committed harvest=%v", record)
	}
}

func TestParticipantCommitHarvestAndLegacyTheftOrderUsesCommittedFarmTurn(t *testing.T) {
	const farmID = uint64(1281)
	rig := ar1MemoryRig(t, farmID)
	cycleID, plantVersion := plantParticipantForHarvest(t, rig, farmID, "harvest-theft-order-plant")
	matureAt := time.UnixMilli(participantTestNowMs + 60_000)
	if _, err := rig.registry.Submit(context.Background(), farmID, func(ctx context.Context, actor *FarmActor) (any, error) {
		return actor.PrepareTheft(ctx, "legacy-theft-before-harvest", []byte("theft-before"), 9001, 1,
			&farmpb.BeginTheftRequest{PlotIndex: 0}, 0, 0, cycleID, matureAt)
	}); err != nil {
		t.Fatal(err)
	}
	record, _, err := rig.registry.ReadOutsideMailbox(context.Background(), farmID)
	if err != nil {
		t.Fatal(err)
	}
	if record.GetCore().GetFarmVersion() != 2 || participantPublicPlot(record, 1).GetPlotVersion() != plantVersion+1 {
		t.Fatalf("theft did not advance the committed farm turn: %v", record)
	}
	_, theftHeader, found, headerErr := decodeFarmMutationReceiptPayload(record.GetRecentOperationReceipts()[1].GetResultPayload())
	if !found || headerErr != nil || theftHeader.SourceFarmVersion != 2 || theftHeader.DirtyGeneration != 2 {
		t.Fatalf("theft opportunity header=%+v found=%v err=%v", theftHeader, found, headerErr)
	}
	harvest := participantHarvestCall(t, farmID, "harvest-after-theft", cycleID, 1, plantVersion)
	result, err := submitHarvestParticipantCall(t, rig.registry, farmID, harvest, participantTestNowMs+60_000)
	if err != nil || result.Outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		t.Fatalf("harvest after theft=%+v err=%v", result, err)
	}
	harvestResult := result.Result.(*contractspb.CommitHarvestResult)
	if harvestResult.GetHarvestDecision().GetRewardQuantity() != 249 {
		t.Fatalf("reward did not use committed theft amount: %v", harvestResult.GetHarvestDecision())
	}
	if harvestResult.GetCommittedFarmVersion() != 3 || harvestResult.GetCommittedPlotVersion() != plantVersion+2 {
		t.Fatalf("harvest did not commit after legal theft: %v", harvestResult)
	}
	record, _, err = rig.registry.ReadOutsideMailbox(context.Background(), farmID)
	if err != nil {
		t.Fatal(err)
	}
	harvestProof, headerErr := decodeHarvestParticipantProof(record.GetRecentOperationReceipts()[2].GetResultPayload())
	if headerErr != nil || harvestProof.header.SourceFarmVersion != 3 || harvestProof.header.DirtyGeneration != 3 {
		t.Fatalf("harvest opportunity header=%+v err=%v", harvestProof.header, headerErr)
	}
	if _, err := rig.registry.Submit(context.Background(), farmID, func(ctx context.Context, actor *FarmActor) (any, error) {
		return actor.PrepareTheft(ctx, "legacy-theft-after-harvest", []byte("theft-after"), 9002, 1,
			&farmpb.BeginTheftRequest{PlotIndex: 0}, 0, 0, cycleID, matureAt)
	}); err == nil {
		t.Fatal("theft after Harvest unexpectedly succeeded")
	}
}

func TestParticipantCommitHarvestReceiptCapacityAndTimeoutUnknown(t *testing.T) {
	t.Run("receipt capacity", func(t *testing.T) {
		const farmID = uint64(1282)
		rig := ar1MemoryRig(t, farmID)
		cycleID, plantVersion := plantParticipantForHarvest(t, rig, farmID, "harvest-capacity-plant")
		if _, err := rig.registry.Submit(context.Background(), farmID, func(ctx context.Context, actor *FarmActor) (any, error) {
			before := proto.Clone(actor.record).(*farmpb.FarmRecord)
			for i := 0; i < maxRecentReceipts; i++ {
				actor.record.RecentOperationReceipts = append(actor.record.RecentOperationReceipts, &farmpb.FarmOperationReceipt{
					OperationId: fmt.Sprintf("legacy-capacity-%d", i), Status: commonpb.CommandStatus_SUCCEEDED,
					CommittedAtMs: participantTestNowMs,
				})
			}
			return nil, actor.save(ctx, before, "seed-receipt-capacity", commonpb.CommandType_COMMAND_TYPE_UNSPECIFIED)
		}); err != nil {
			t.Fatal(err)
		}
		outcome, err := submitHarvestParticipantCall(t, rig.registry, farmID, participantHarvestCall(t, farmID, "harvest-capacity", cycleID, 1, plantVersion), participantTestNowMs+60_000)
		if outcome != nil || err == nil {
			t.Fatalf("capacity outcome=%+v err=%v", outcome, err)
		}
		farmErr, ok := err.(*Error)
		if !ok || farmErr.Code != commonpb.ErrorCode_ERR_RATE_LIMITED {
			t.Fatalf("capacity err=%T %v", err, err)
		}
	})

	t.Run("timeout unknown replays durable result", func(t *testing.T) {
		const farmID = uint64(1283)
		rig := ar1MemoryRig(t, farmID)
		cycleID, plantVersion := plantParticipantForHarvest(t, rig, farmID, "harvest-unknown-plant")
		call := participantHarvestCall(t, farmID, "harvest-unknown", cycleID, 1, plantVersion)
		rig.fault.Inject(authoritystore.MethodCommit, authoritystore.FaultTimeoutUnknownAfterApply)
		first, err := submitHarvestParticipantCall(t, rig.registry, farmID, call, participantTestNowMs+60_000)
		if first != nil || !errors.Is(err, blobstore.ErrTimeoutUnknown) {
			t.Fatalf("unknown first=%+v err=%v", first, err)
		}
		deadline := time.Now().Add(2 * time.Second)
		for rig.registry.Activation().StateOf(farmID) != activation.Active && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if rig.registry.Activation().StateOf(farmID) != activation.Active {
			t.Fatal("Harvest UNKNOWN did not resolve")
		}
		replay, err := submitHarvestParticipantCall(t, rig.registry, farmID, call, participantTestNowMs+600_000)
		if err != nil || replay == nil || !replay.ReplayedFromReceipt || replay.Outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
			t.Fatalf("unknown replay=%+v err=%v", replay, err)
		}
	})
}
