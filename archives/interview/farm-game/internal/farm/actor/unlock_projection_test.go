package actor

import (
	"bytes"
	"context"
	"testing"
	"time"

	contractspb "farmgame/api/gen/contracts"
	farmpb "farmgame/api/gen/farm"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/contracts"
	"farmgame/internal/farm/domain"
	"farmgame/internal/identity"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/blobstore"

	"google.golang.org/protobuf/proto"
)

func TestUnlockPlotGenesisAndProjectionCarriesAuthoritativeRule(t *testing.T) {
	record := InitialRecord(7101)
	actor := &FarmActor{farmID: 7101, record: record}
	snapshot := actor.Snapshot(time.UnixMilli(1_800_000_000_000))
	if got := len(snapshot.GetPlots()); got != DefaultPlotCount {
		t.Fatalf("plot count=%d want=%d", got, DefaultPlotCount)
	}
	for _, plot := range snapshot.GetPlots() {
		if plot == nil {
			t.Fatal("nil PlotView")
		}
		plotID := plot.GetPlotIndex() + 1
		wantState := farmpb.PlotLockState_PLOT_LOCK_STATE_LOCKED
		if plotID <= domain.P0InitialUnlockedPlots {
			wantState = farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED
		}
		if plot.GetLockState() != wantState || plot.GetUnlockRuleVersion() != 1 {
			t.Fatalf("plot %d projection state=%s rule=%d", plotID, plot.GetLockState(), plot.GetUnlockRuleVersion())
		}
	}
	plot6 := snapshot.GetPlots()[5]
	if plot6.GetUnlockOrder() != 6 || plot6.GetRequiredLevel() != 1 || plot6.GetNormalCoinPrice() != 100 || !plot6.GetUnlockCandidate() {
		t.Fatalf("plot6 projection=%+v", plot6)
	}
	if snapshot.GetPlots()[6].GetUnlockCandidate() {
		t.Fatal("plot7 must not be the unlock candidate before plot6")
	}
}

func TestUnlockPlotLegacyUnspecifiedMigrationPreservesDurableEvidence(t *testing.T) {
	record := &farmpb.FarmRecord{FarmId: 7102, Core: &farmpb.FarmCore{FarmId: 7102}}
	for index := int32(0); index < 24; index++ {
		record.Core.Plots = append(record.Core.Plots, &farmpb.Plot{Index: index, PlotVersion: 1})
	}
	// A pre-feature record with a durable post-genesis fact must not be
	// relocked merely because its new lock_state field was absent.
	record.Core.Plots[5].PlotVersion = 2
	record.Core.Plots[5].UnlockWorkflowId = "sid1_legacy-unlock-proof"
	if !normalizeRecord(record) {
		t.Fatal("legacy record was not marked as normalized")
	}
	for index, plot := range record.Core.Plots {
		want := farmpb.PlotLockState_PLOT_LOCK_STATE_LOCKED
		if index < 5 || index == 5 {
			want = farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED
		}
		if plot.GetLockState() != want || plot.GetUnlockRuleVersion() != 1 {
			t.Fatalf("legacy plot index=%d state=%s rule=%d", index, plot.GetLockState(), plot.GetUnlockRuleVersion())
		}
	}
}

type normalizationModelStore struct {
	state         authoritystore.ModelState
	version       uint64
	recordOnlyCAS bool
}

func cloneNormalizationModelState(state authoritystore.ModelState) authoritystore.ModelState {
	clone := make(authoritystore.ModelState, len(state))
	for name, value := range state {
		clone[name] = append([]byte(nil), value...)
	}
	return clone
}

func (s *normalizationModelStore) Get(_ context.Context, _ uint64) ([]byte, uint64, error) {
	data, ok := s.state["record"]
	if !ok {
		return nil, s.version, blobstore.ErrNotFound
	}
	return append([]byte(nil), data...), s.version, nil
}

func (s *normalizationModelStore) Insert(_ context.Context, _ uint64, data []byte) error {
	if _, ok := s.state["record"]; ok {
		return blobstore.ErrExists
	}
	s.state = authoritystore.ModelState{"record": append([]byte(nil), data...)}
	s.version = 1
	return nil
}

func (s *normalizationModelStore) CAS(_ context.Context, _ uint64, expectVersion uint64, data []byte) error {
	s.recordOnlyCAS = true
	if expectVersion != s.version {
		return blobstore.ErrCASConflict
	}
	s.state = authoritystore.ModelState{"record": append([]byte(nil), data...)}
	s.version++
	return nil
}

func (s *normalizationModelStore) GetModelState(_ context.Context, _ uint64) (authoritystore.ModelState, uint64, error) {
	return cloneNormalizationModelState(s.state), s.version, nil
}

func (s *normalizationModelStore) CASEpochModelStateWithOutcomeAndOrders(
	_ context.Context,
	_ uint64,
	expectVersion uint64,
	_ uint64,
	state authoritystore.ModelState,
	_ authoritystore.StoredOperationOutcome,
	_ []authoritystore.AttachedOrder,
) error {
	if expectVersion != s.version {
		return blobstore.ErrCASConflict
	}
	s.state = cloneNormalizationModelState(state)
	s.version++
	return nil
}

func TestUnlockNormalizationPersistsOpportunityModelsAtomically(t *testing.T) {
	const farmID = uint64(7105)
	record := &farmpb.FarmRecord{FarmId: farmID, Core: &farmpb.FarmCore{FarmId: farmID, FarmVersion: 7}}
	for index := int32(0); index < 12; index++ {
		record.Core.Plots = append(record.Core.Plots, &farmpb.Plot{Index: index, PlotVersion: 1})
	}
	if !normalizeRecord(record) {
		t.Fatal("legacy 12-plot record was not normalized")
	}
	initialBytes, err := marshalRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	store := &normalizationModelStore{
		state:   authoritystore.ModelState{"record": initialBytes},
		version: 4,
	}
	actor := &FarmActor{farmID: farmID, store: store, record: record, recordVersion: 4}
	if err := actor.persistNormalizedRecord(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.recordOnlyCAS {
		t.Fatal("normalization used a record-only CAS and discarded compact authority models")
	}
	if store.version != 5 || actor.recordVersion != 5 || len(store.state) != 3 {
		t.Fatalf("version/store mismatch store_version=%d actor_version=%d model_count=%d", store.version, actor.recordVersion, len(store.state))
	}
	var persisted farmpb.FarmRecord
	if err := proto.Unmarshal(store.state["record"], &persisted); err != nil {
		t.Fatal(err)
	}
	if len(persisted.GetCore().GetPlots()) != DefaultPlotCount {
		t.Fatalf("persisted plots=%d want=%d", len(persisted.GetCore().GetPlots()), DefaultPlotCount)
	}
	header, facts, available := actor.OpportunityReadModels()
	if !available || header.SourceFarmVersion != 7 || facts.SourceFarmVersion != 7 {
		t.Fatalf("opportunity models available=%v header_version=%d facts_version=%d", available, header.SourceFarmVersion, facts.SourceFarmVersion)
	}
	if _, ok := store.state[FarmOpportunityHeaderModelField]; !ok {
		t.Fatal("persisted state missing opportunity header")
	}
	if _, ok := store.state[FarmOpportunityFactsModelField]; !ok {
		t.Fatal("persisted state missing opportunity facts")
	}
}

func TestUnlockPlotFarmParticipantCommitReplayAndPlantBoundary(t *testing.T) {
	const farmID uint64 = 7103
	rig := ar1MemoryRig(t, farmID)
	call := unlockParticipantTestCall(t, farmID, "unlock-participant-6", 6, 1)
	firstTurn := submitUnlockParticipantCallWithAction(t, rig.registry, farmID, call, participantTestNowMs)
	first := firstTurn.Outcome
	if first.Outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		t.Fatalf("unlock outcome=%+v", first)
	}
	if firstTurn.Action == nil || firstTurn.Action.Change == nil || len(firstTurn.Action.Change.GetChangedPlots()) != 2 {
		t.Fatalf("unlock change must carry purchased and next-candidate plots: %+v", firstTurn.Action)
	}
	unlocked, next := firstTurn.Action.Change.GetChangedPlots()[0], firstTurn.Action.Change.GetChangedPlots()[1]
	if unlocked.GetPlotIndex() != 5 || unlocked.GetLockState() != farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED || unlocked.GetUnlockCandidate() ||
		next.GetPlotIndex() != 6 || next.GetLockState() != farmpb.PlotLockState_PLOT_LOCK_STATE_LOCKED || !next.GetUnlockCandidate() {
		t.Fatalf("unlock realtime projection unlocked=%+v next=%+v", unlocked, next)
	}
	unlockResult, ok := first.Result.(*contractspb.CommitPlotUnlockResult)
	if !ok || unlockResult.GetCommittedPlotVersion() != 2 || unlockResult.GetCommittedFarmVersion() != 1 {
		t.Fatalf("unlock result=%T %+v", first.Result, first.Result)
	}
	value, err := rig.registry.Submit(context.Background(), farmID, func(_ context.Context, actor *FarmActor) (any, error) {
		return actor.Snapshot(time.UnixMilli(participantTestNowMs)), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := value.(*farmpb.FarmSnapshot)
	if snapshot.GetFarmVersion() != 1 || snapshot.GetPlots()[5].GetLockState() != farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED {
		t.Fatalf("post-unlock snapshot=%+v", snapshot)
	}

	replay := submitUnlockParticipantCall(t, rig.registry, farmID, call, participantTestNowMs+99_000)
	if !replay.ReplayedFromReceipt || replay.CommittedRecordVersion != first.CommittedRecordVersion {
		t.Fatalf("unlock replay=%+v first=%+v", replay, first)
	}
	if !bytes.Equal(replay.CommittedResultHash, first.CommittedResultHash) {
		t.Fatal("unlock replay changed result hash")
	}

	plant := submitParticipantCall(t, rig.registry, farmID, participantTestCall(t, farmID, "plant-after-unlock-6", 6, 2), participantTestNowMs)
	if plant.Outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		t.Fatalf("plant on unlocked plot was rejected: %+v", plant)
	}
}

func unlockParticipantTestCall(t *testing.T, farmID uint64, operationID string, plotID uint32, expectedPlotVersion uint64) *contracts.ParticipantCall {
	t.Helper()
	workflowID, err := identity.WorkflowID(identity.EffectiveIdempotencyKey{
		SubjectPlayerID:       farmID,
		StableCommandTypeName: string(identity.WorkTypeUnlockPlot),
		OperationID:           operationID,
		Root:                  identity.OperationRootRef{Kind: identity.RootKindFarmActor, CanonicalID: "7103"},
	})
	if err != nil {
		t.Fatal(err)
	}
	reservationID, err := identity.UnlockCoinReservationID(workflowID)
	if err != nil {
		t.Fatal(err)
	}
	rule := domain.DefaultPlotUnlockRule()
	ruleHash := domain.PlotUnlockRuleSnapshotHash(rule)
	entry, ok := rule.EntryOf(plotID)
	if !ok {
		t.Fatalf("missing rule entry for plot %d", plotID)
	}
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLOT_UNLOCK)
	if err != nil {
		t.Fatal(err)
	}
	invocationID, err := identity.ParticipantInvocationID(identity.WorkTypeUnlockPlot, workflowID, identity.StepCommitPlotUnlock, 1)
	if err != nil {
		t.Fatal(err)
	}
	predecessor := uint32(0)
	if plotID > domain.P0InitialUnlockedPlots+1 {
		predecessor = plotID - 1
	}
	payload := &contractspb.CommitPlotUnlockPayload{
		OwnerPlayerId: farmID, FarmId: farmID, PlotId: plotID, ExpectedPlotVersion: expectedPlotVersion,
		ExpectedUnlockRuleVersion: rule.RuleVersion, UnlockOrder: entry.UnlockOrder,
		PredecessorPlotId: &predecessor, ReservationId: reservationID,
		ReserveUnlockResultHash: bytes.Repeat([]byte{0x61}, 32), UnlockRuleSnapshotHash: ruleHash[:],
		RequiredPlayerLevel: entry.RequiredLevel, AmountNormalCoin: entry.NormalCoinPrice,
	}
	if plotID == domain.P0InitialUnlockedPlots+1 {
		payload.PredecessorPlotId = nil
	}
	stepHash, err := contracts.CanonicalParticipantHash(spec, payload)
	if err != nil {
		t.Fatal(err)
	}
	return &contracts.ParticipantCall{
		Spec: spec, RouteHeader: &logicrpcpb.ActorRouteHeader{RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM, ActorId: farmID},
		Work:       &contractspb.WorkAddress{WorkId: workflowID, WorkType: contractspb.WorkType_WORK_TYPE_UNLOCK_PLOT, PlacementKind: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_INDEPENDENT_WORK_BUCKET, BucketVersion: 1},
		CallerKind: contracts.ParticipantCallerCoordinator, ParticipantStep: string(spec.Step), InvocationID: invocationID,
		OperationID: operationID, SubjectPlayerID: farmID, ExpectedWorkVersion: 1, RemainingBudgetMs: 5_000,
		Payload: payload, CanonicalStepPayloadHash: stepHash,
	}
}

func submitUnlockParticipantCall(t *testing.T, registry *Registry, farmID uint64, call *contracts.ParticipantCall, nowMs int64) *contracts.StepOutcome {
	t.Helper()
	value, err := registry.SubmitParticipant(context.Background(), farmID, func(ctx context.Context, actor *FarmActor, guard ParticipantCapacityGuard) (any, error) {
		return actor.CommitPlotUnlock(ctx, call, guard, func() int64 { return nowMs })
	})
	if err != nil {
		t.Fatal(err)
	}
	outcome, ok := value.(*contracts.StepOutcome)
	if !ok || outcome == nil {
		t.Fatalf("unlock participant outcome=%T %v", value, value)
	}
	return outcome
}

func submitUnlockParticipantCallWithAction(t *testing.T, registry *Registry, farmID uint64, call *contracts.ParticipantCall, nowMs int64) *CommitPlotUnlockTurnResult {
	t.Helper()
	value, err := registry.SubmitParticipant(context.Background(), farmID, func(ctx context.Context, actor *FarmActor, guard ParticipantCapacityGuard) (any, error) {
		return actor.CommitPlotUnlockWithAction(ctx, call, guard, func() int64 { return nowMs })
	})
	if err != nil {
		t.Fatal(err)
	}
	turn, ok := value.(*CommitPlotUnlockTurnResult)
	if !ok || turn == nil || turn.Outcome == nil {
		t.Fatalf("unlock participant turn=%T %v", value, value)
	}
	return turn
}
