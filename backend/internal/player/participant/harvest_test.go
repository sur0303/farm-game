package participant

import (
	"bytes"
	"context"
	"testing"

	contractspb "farmgame/api/gen/contracts"
	playerpb "farmgame/api/gen/player"
	taskpb "farmgame/api/gen/task"
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

// fixtureHarvestDependencyVerifier represents the Work/Farm repository proof
// which is intentionally outside PlayerActor. A missing map entry is a
// physical-proof failure, not an invitation to trust the typed payload.
type fixtureHarvestDependencyVerifier struct {
	results map[string]*contractspb.CommitHarvestResult
}

func (v fixtureHarvestDependencyVerifier) VerifyHarvestDependency(_ context.Context, call *contracts.ParticipantCall) error {
	if call == nil || call.Work == nil || call.Work.GetWorkType() != contractspb.WorkType_WORK_TYPE_HARVEST || call.ExpectedWorkVersion != 1 {
		return status.Error(codes.FailedPrecondition, "fixture Harvest WorkRecord mismatch")
	}
	payload, ok := call.Payload.(*contractspb.GrantHarvestRewardPayload)
	if !ok {
		return status.Error(codes.FailedPrecondition, "fixture Harvest payload schema mismatch")
	}
	saved, ok := v.results[call.Work.GetWorkId()]
	if !ok || saved == nil || saved.GetHarvestDecision() == nil || !proto.Equal(saved.GetHarvestDecision(), payload.GetHarvestDecision()) {
		return status.Error(codes.FailedPrecondition, "fixture saved COMMIT_HARVEST proof missing")
	}
	wantHash, err := CommitHarvestResultHash(saved)
	if err != nil || !bytes.Equal(wantHash[:], payload.GetCommitHarvestResultHash()) {
		return status.Error(codes.FailedPrecondition, "fixture saved COMMIT_HARVEST result hash mismatch")
	}
	return nil
}

func newHarvestAllowServer(registry *playeractor.Registry, verifier HarvestDependencyVerifier) *Server {
	calc, err := actorrouting.NewCalculator("participant-test", actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608040804}, 64, 64)
	if err != nil {
		panic(err)
	}
	return NewServer(Options{
		Registry:                  registry,
		WorkVersionVerifier:       fixtureWorkVersionVerifier{},
		WorkLeaseVerifier:         fixtureWorkLeaseVerifier{},
		RouteFenceVerifier:        fixtureRouteFenceVerifier{calc: calc},
		WorkAddressVerifier:       fixtureWorkAddressVerifier{},
		IdentityBindingVerifier:   fixtureIdentityBindingVerifier{},
		PlantDependencyVerifier:   fixturePlantWorkRecordVerifier{},
		HarvestDependencyVerifier: verifier,
		TaskConfigRepository:      playertask.MustNewP0Repository(),
		TaskTimezone:              playertask.DefaultGameTimezone,
	})
}

func harvestWorkID(t *testing.T, operation string) string {
	t.Helper()
	workID, err := identity.WorkflowID(identity.EffectiveIdempotencyKey{
		SubjectPlayerID:       plantTestPlayerID,
		StableCommandTypeName: "HARVEST",
		OperationID:           operation,
		Root:                  identity.OperationRootRef{Kind: identity.RootKindFarmActor, CanonicalID: "7001"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return workID
}

func harvestRequest(t *testing.T, workID string, playerID uint64, cropID uint32, eligible bool, plantedBy uint64, activity string, quantity, rewardExp uint64) (*contractspb.ParticipantRequest, *contractspb.CommitHarvestResult) {
	t.Helper()
	snapshot := &contractspb.CompendiumUnlockSnapshot{
		CropDefinitionId: cropID, UnlockEligible: eligible, PlantedByPlayerId: plantedBy,
		CatalogVersionAtPlant: 7, DefinitionVersionAtPlant: 3,
	}
	if activity != "" {
		snapshot.ActivityId = &activity
	}
	decision := &contractspb.HarvestDecision{
		WorkflowId: workID, OwnerPlayerId: playerID, FarmId: 7001, PlotId: 3,
		CropCycleId: mustCropCycle(t, workID), CropDefinitionId: cropID,
		RewardItemId: uint32(playeractor.ItemCarrot), RewardQuantity: quantity,
		RewardExp: rewardExp, CropRuleVersion: 42, CompendiumUnlockSnapshot: snapshot,
	}
	decisionHash, err := HarvestDecisionHash(decision)
	if err != nil {
		t.Fatal(err)
	}
	decision.HarvestDecisionHash = append([]byte(nil), decisionHash[:]...)
	commit := &contractspb.CommitHarvestResult{
		HarvestDecision:      proto.Clone(decision).(*contractspb.HarvestDecision),
		CommittedPlotVersion: 2,
		CommittedFarmVersion: 3,
		FarmRecordVersion:    4,
	}
	commitHash, err := CommitHarvestResultHash(commit)
	if err != nil {
		t.Fatal(err)
	}
	payload := &contractspb.GrantHarvestRewardPayload{
		PlayerId: playerID, CommitHarvestResultHash: append([]byte(nil), commitHash[:]...),
		HarvestDecision: proto.Clone(decision).(*contractspb.HarvestDecision),
	}
	return makePlantRequest(t, workID, playerID, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD, payload, false), commit
}

func ensureHarvestPlayer(t *testing.T, registry *playeractor.Registry) {
	t.Helper()
	if _, _, err := registry.Ensure(context.Background(), plantTestPlayerID, "harvest-bootstrap"); err != nil {
		t.Fatal(err)
	}
}

func harvestTask(rec *playerpb.PlayerRecord) *taskpb.TaskInstance {
	for _, instance := range rec.GetTasks() {
		if instance != nil && instance.GetTaskDefinitionId() == 1 {
			return instance
		}
	}
	return nil
}

func TestHarvestGrantFirstUnlockReplayAndTaskProgress(t *testing.T) {
	trig := newParticipantRig(t, "memory", plantTestPlayerID)
	ensureHarvestPlayer(t, trig.registry)
	verifier := fixtureHarvestDependencyVerifier{results: map[string]*contractspb.CommitHarvestResult{}}
	server := newHarvestAllowServer(trig.registry, verifier)

	firstRequest, firstCommit := harvestRequest(t, harvestWorkID(t, "first"), plantTestPlayerID, 1, true, plantTestPlayerID, "", 5, 10)
	verifier.results[firstRequest.GetCoordinator().GetWork().GetWorkId()] = firstCommit
	first := mustExecute(t, server.ExecuteStep, firstRequest)
	if first.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED || first.GetReplayedFromReceipt() {
		t.Fatalf("first harvest=%+v", first)
	}
	firstResult := &contractspb.GrantHarvestRewardResult{}
	mustUnmarshal(t, first.GetTypedResult(), firstResult)
	if !firstResult.GetCompendiumUnlockDelta().GetNewlyUnlocked() || firstResult.GetCompendiumUnlockDelta().GetCompendiumRevisionAfter() != 1 ||
		firstResult.GetHarvestCountAfter() != 1 || firstResult.GetInventoryQuantityAfter() != 5 || firstResult.GetGrantedExp() != 10 || firstResult.GetPlayerLevelAfter() != 1 {
		t.Fatalf("first harvest result=%+v", firstResult)
	}

	secondRequest, secondCommit := harvestRequest(t, harvestWorkID(t, "second"), plantTestPlayerID, 1, true, plantTestPlayerID, "", 5, 10)
	verifier.results[secondRequest.GetCoordinator().GetWork().GetWorkId()] = secondCommit
	second := mustExecute(t, server.ExecuteStep, secondRequest)
	secondResult := &contractspb.GrantHarvestRewardResult{}
	mustUnmarshal(t, second.GetTypedResult(), secondResult)
	if secondResult.GetCompendiumUnlockDelta().GetNewlyUnlocked() || secondResult.GetCompendiumUnlockDelta().CropDefinitionId != nil ||
		secondResult.GetCompendiumUnlockDelta().GetCompendiumRevisionAfter() != 1 || secondResult.GetHarvestCountAfter() != 2 {
		t.Fatalf("already unlocked harvest result=%+v", secondResult)
	}
	record, version := readPlayerVersion(t, trig.registry)
	if version != second.GetCommittedRecordVersion() || playeractor.ItemCount(record, playeractor.ItemCarrot) != 10 || record.GetProgression().GetExp() != 20 || len(record.GetCodexEntries()) != 1 {
		t.Fatalf("atomic harvest state version=%d record=%+v", version, record)
	}
	if task := harvestTask(record); task == nil || task.GetProgress() != 2 || task.GetState() != taskpb.TaskState_ACTIVE {
		t.Fatalf("legacy harvest task=%+v", task)
	}
	taskState, err := playertask.DomainStateFromRecord(record)
	if err != nil || taskState.TotalHarvestCount != 2 || taskState.CurrentGrowth == nil ||
		taskState.CurrentGrowth.State != playertask.StateClaimable || taskState.CurrentGrowth.Progress != 1 {
		t.Fatalf("TaskDomain harvest state=%+v err=%v", taskState, err)
	}
}

func TestHarvestSnapshotQualificationIsFrozen(t *testing.T) {
	trig := newParticipantRig(t, "memory", plantTestPlayerID)
	ensureHarvestPlayer(t, trig.registry)
	verifier := fixtureHarvestDependencyVerifier{results: map[string]*contractspb.CommitHarvestResult{}}
	server := newHarvestAllowServer(trig.registry, verifier)
	cases := []struct {
		name       string
		cropID     uint32
		eligible   bool
		plantedBy  uint64
		activityID string
		unlocks    bool
	}{
		{name: "archived activity frozen eligible", cropID: 2, eligible: true, plantedBy: plantTestPlayerID, activityID: "archived-winter", unlocks: true},
		{name: "legacy seed frozen ineligible", cropID: 3, eligible: false, plantedBy: plantTestPlayerID, activityID: "legacy-winter", unlocks: false},
		{name: "stolen crop", cropID: 4, eligible: true, plantedBy: plantTestPlayerID + 1, activityID: "", unlocks: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request, commit := harvestRequest(t, harvestWorkID(t, tc.name), plantTestPlayerID, tc.cropID, tc.eligible, tc.plantedBy, tc.activityID, 1, 1)
			verifier.results[request.GetCoordinator().GetWork().GetWorkId()] = commit
			response := mustExecute(t, server.ExecuteStep, request)
			result := &contractspb.GrantHarvestRewardResult{}
			mustUnmarshal(t, response.GetTypedResult(), result)
			if result.GetCompendiumUnlockDelta().GetNewlyUnlocked() != tc.unlocks {
				t.Fatalf("unlock delta=%+v want=%v", result.GetCompendiumUnlockDelta(), tc.unlocks)
			}
		})
	}
	record := mustReadRecord(t, trig.registry)
	if len(record.GetCodexEntries()) != 1 || record.GetCodexEntries()[0].GetCropDefinitionId() != 2 {
		t.Fatalf("frozen compendium entries=%v", record.GetCodexEntries())
	}
}

func TestHarvestSameHashReplayConflictAndTaskOnce(t *testing.T) {
	trig := newParticipantRig(t, "memory", plantTestPlayerID)
	ensureHarvestPlayer(t, trig.registry)
	verifier := fixtureHarvestDependencyVerifier{results: map[string]*contractspb.CommitHarvestResult{}}
	server := newHarvestAllowServer(trig.registry, verifier)
	request, commit := harvestRequest(t, harvestWorkID(t, "replay"), plantTestPlayerID, 5, true, plantTestPlayerID, "", 2, 7)
	verifier.results[request.GetCoordinator().GetWork().GetWorkId()] = commit
	first := mustExecute(t, server.ExecuteStep, request)
	before, beforeVersion := readPlayerVersion(t, trig.registry)
	replay := mustExecute(t, server.ExecuteStep, request)
	if !replay.GetReplayedFromReceipt() || !bytes.Equal(replay.GetTypedResult(), first.GetTypedResult()) || replay.GetCommittedRecordVersion() != first.GetCommittedRecordVersion() {
		t.Fatalf("same-hash replay=%+v first=%+v", replay, first)
	}
	afterReplay, afterReplayVersion := readPlayerVersion(t, trig.registry)
	if afterReplayVersion != beforeVersion || playeractor.ItemCount(afterReplay, playeractor.ItemCarrot) != playeractor.ItemCount(before, playeractor.ItemCarrot) || harvestTask(afterReplay).GetProgress() != 1 {
		t.Fatalf("replay mutated state before=%+v after=%+v", before, afterReplay)
	}

	conflict := proto.Clone(request).(*contractspb.ParticipantRequest)
	mutateTypedPayload(t, conflict, func(message proto.Message) {
		message.(*contractspb.GrantHarvestRewardPayload).HarvestDecision.RewardQuantity++
	})
	if _, err := server.ExecuteStep(context.Background(), conflict); status.Code(err) != codes.FailedPrecondition || status.Convert(err).Message() != "PARTICIPANT_IDEMPOTENCY_CONFLICT" {
		t.Fatalf("same invocation hash conflict=%v", err)
	}
	afterConflict, afterConflictVersion := readPlayerVersion(t, trig.registry)
	if afterConflictVersion != beforeVersion || playeractor.ItemCount(afterConflict, playeractor.ItemCarrot) != playeractor.ItemCount(before, playeractor.ItemCarrot) || harvestTask(afterConflict).GetProgress() != 1 {
		t.Fatalf("conflict mutated state version=%d/%d", beforeVersion, afterConflictVersion)
	}
}

func TestHarvestMissingPhysicalProofFailsClosed(t *testing.T) {
	trig := newParticipantRig(t, "memory", plantTestPlayerID)
	ensureHarvestPlayer(t, trig.registry)
	request, _ := harvestRequest(t, harvestWorkID(t, "missing-proof"), plantTestPlayerID, 1, true, plantTestPlayerID, "", 1, 1)
	server := newAllowServer(trig.registry)
	before, beforeVersion := readPlayerVersion(t, trig.registry)
	beforeSubmitted := trig.registry.RuntimeStats().Submitted
	if _, err := server.ExecuteStep(context.Background(), request); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("missing Harvest physical proof err=%v", err)
	}
	afterSubmitted := trig.registry.RuntimeStats().Submitted
	after, afterVersion := readPlayerVersion(t, trig.registry)
	if afterVersion != beforeVersion || playeractor.ItemCount(after, playeractor.ItemCarrot) != playeractor.ItemCount(before, playeractor.ItemCarrot) || afterSubmitted != beforeSubmitted {
		t.Fatalf("missing proof entered/mutated actor version=%d/%d", beforeVersion, afterVersion)
	}
}

func TestHarvestDecisionHashFailsClosedBeforeMailbox(t *testing.T) {
	trig := newParticipantRig(t, "memory", plantTestPlayerID)
	ensureHarvestPlayer(t, trig.registry)
	request, _ := harvestRequest(t, harvestWorkID(t, "bad-decision-hash"), plantTestPlayerID, 8, true, plantTestPlayerID, "", 1, 1)
	mutateTypedPayload(t, request, func(message proto.Message) {
		payload := message.(*contractspb.GrantHarvestRewardPayload)
		payload.HarvestDecision.HarvestDecisionHash[0]++
	})
	server := newHarvestAllowServer(trig.registry, HarvestDependencyVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }))
	before, beforeVersion := readPlayerVersion(t, trig.registry)
	beforeSubmitted := trig.registry.RuntimeStats().Submitted
	if _, err := server.ExecuteStep(context.Background(), request); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("bad HarvestDecision hash err=%v", err)
	}
	afterSubmitted := trig.registry.RuntimeStats().Submitted
	after, afterVersion := readPlayerVersion(t, trig.registry)
	if afterVersion != beforeVersion || playeractor.ItemCount(after, playeractor.ItemCarrot) != playeractor.ItemCount(before, playeractor.ItemCarrot) || afterSubmitted != beforeSubmitted {
		t.Fatalf("bad decision hash entered/mutated actor version=%d/%d", beforeVersion, afterVersion)
	}
}

func TestHarvestResponseLossAndTimeoutUnknown(t *testing.T) {
	for _, backend := range []string{"memory", "redis"} {
		t.Run(backend, func(t *testing.T) {
			trig := newParticipantRig(t, backend, plantTestPlayerID)
			ensureHarvestPlayer(t, trig.registry)
			verifier := fixtureHarvestDependencyVerifier{results: map[string]*contractspb.CommitHarvestResult{}}
			server := newHarvestAllowServer(trig.registry, verifier)

			beforeRequest, beforeCommit := harvestRequest(t, harvestWorkID(t, "timeout-before-"+backend), plantTestPlayerID, 6, true, plantTestPlayerID, "", 3, 2)
			verifier.results[beforeRequest.GetCoordinator().GetWork().GetWorkId()] = beforeCommit
			trig.fault.Inject(authoritystore.MethodCommit, authoritystore.FaultTimeoutUnknownBeforeApply)
			trig.fault.Inject(authoritystore.MethodFenceAndClaim, authoritystore.FaultTimeoutUnknownBeforeApply)
			unknown := mustExecute(t, server.ExecuteStep, beforeRequest)
			if unknown.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_TIMEOUT_UNKNOWN || unknown.GetReplayedFromReceipt() || !unknown.GetFailure().GetRetryableAfterResolution() {
				t.Fatalf("timeout before=%+v", unknown)
			}
			retried := mustExecute(t, server.ExecuteStep, beforeRequest)
			if retried.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED || retried.GetReplayedFromReceipt() {
				t.Fatalf("timeout-before retry=%+v", retried)
			}

			afterRequest, afterCommit := harvestRequest(t, harvestWorkID(t, "timeout-after-"+backend), plantTestPlayerID, 7, true, plantTestPlayerID, "", 4, 3)
			verifier.results[afterRequest.GetCoordinator().GetWork().GetWorkId()] = afterCommit
			trig.fault.Inject(authoritystore.MethodCommit, authoritystore.FaultTimeoutUnknownAfterApply)
			after := mustExecute(t, server.ExecuteStep, afterRequest)
			if after.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED || after.GetReplayedFromReceipt() {
				t.Fatalf("timeout-after response recovery=%+v", after)
			}
			replay := mustExecute(t, server.ExecuteStep, afterRequest)
			if !replay.GetReplayedFromReceipt() || !bytes.Equal(replay.GetTypedResult(), after.GetTypedResult()) {
				t.Fatalf("response-loss replay=%+v", replay)
			}
			record := mustReadRecord(t, trig.registry)
			if playeractor.ItemCount(record, playeractor.ItemCarrot) != 7 || len(record.GetCodexEntries()) != 2 || harvestTask(record).GetProgress() != 2 {
				t.Fatalf("timeout/recovery applied more than once record=%+v", record)
			}
		})
	}
}
