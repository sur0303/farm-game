package harvestonline

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	contractspb "farmgame/api/gen/contracts"
	"farmgame/internal/contracts"
	"farmgame/internal/identity"
	"farmgame/internal/scenario/harvest"
	"farmgame/internal/workflow"
	"farmgame/internal/workidentity"

	"google.golang.org/protobuf/proto"
)

func TestHarvestOnlineCreateOrLoadAndTerminalReplay(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	repo := workflow.NewMemoryWorkRepository(func() time.Time { return now })
	farm := &onlineParticipant{}
	player := &onlineParticipant{}
	coordinator, err := New(Config{Repository: repo, BucketResolver: onlineBuckets{}, BucketVersion: 1, Now: func() time.Time { return now }, Farm: farm, Player: player})
	if err != nil {
		t.Fatal(err)
	}
	input := onlineInput(t)
	record, created, err := coordinator.CreateOrLoad(context.Background(), input)
	if err != nil || !created || record.State != workflow.WorkStateCreated {
		t.Fatalf("CreateOrLoad record=%v created=%v err=%v", record, created, err)
	}
	if farm.callsCount() != 0 || player.callsCount() != 0 {
		t.Fatal("CreateOrLoad called a Participant")
	}
	result, err := coordinator.Start(context.Background(), input)
	if err != nil || result.Status != commonpb.CommandStatus_SUCCEEDED || result.Result == nil || result.Result.HarvestedQuantity != 50 {
		t.Fatalf("success result=%+v err=%v", result, err)
	}
	farmCalls, playerCalls := farm.callsCount(), player.callsCount()
	replay, err := coordinator.Start(context.Background(), input)
	if err != nil || replay.Created || replay.Status != commonpb.CommandStatus_SUCCEEDED || replay.Result == nil {
		t.Fatalf("terminal replay=%+v err=%v", replay, err)
	}
	if farm.callsCount() != farmCalls || player.callsCount() != playerCalls {
		t.Fatal("same-operation terminal replay invoked a Participant")
	}
}

func TestHarvestOnlineFarmCommitPlayerPendingIsProcessing(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	repo := workflow.NewMemoryWorkRepository(func() time.Time { return now })
	farm := &onlineParticipant{}
	player := &onlineParticipant{loseFirst: true}
	coordinator, err := New(Config{Repository: repo, BucketResolver: onlineBuckets{}, BucketVersion: 1, Now: func() time.Time { return now }, Farm: farm, Player: player})
	if err != nil {
		t.Fatal(err)
	}
	result, err := coordinator.Start(context.Background(), onlineInput(t))
	if !errors.Is(err, workflow.ErrRetryScheduled) || result.Status != commonpb.CommandStatus_PROCESSING || result.Record == nil || result.Record.CurrentStep != 1 {
		t.Fatalf("pending result=%+v err=%v", result, err)
	}
	now = time.UnixMilli(result.Record.NextDueAtMs + 1)
	completed, err := coordinator.Start(context.Background(), onlineInput(t))
	if err != nil || completed.Status != commonpb.CommandStatus_SUCCEEDED || completed.Result == nil {
		t.Fatalf("recovered result=%+v err=%v", completed, err)
	}
}

func TestHarvestOnlineFarmBusinessRejectionIsTerminalAndDoesNotCallPlayer(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	repo := workflow.NewMemoryWorkRepository(func() time.Time { return now })
	farm := &onlineParticipant{farmOutcome: workflow.StepOutcomeRejected}
	player := &onlineParticipant{}
	coordinator, err := New(Config{Repository: repo, BucketResolver: onlineBuckets{}, BucketVersion: 1, Now: func() time.Time { return now }, Farm: farm, Player: player})
	if err != nil {
		t.Fatal(err)
	}
	result, err := coordinator.Start(context.Background(), onlineInput(t))
	if err != nil || result.Status != commonpb.CommandStatus_REJECTED || result.Record.State != workflow.WorkStateRejected {
		t.Fatalf("rejection result=%+v err=%v", result, err)
	}
	if player.callsCount() != 0 {
		t.Fatal("Player was called after Farm rejection")
	}
}

func TestHarvestOnlinePayloadConflictMapsPublicRejectionWithoutResume(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	repo := workflow.NewMemoryWorkRepository(func() time.Time { return now })
	farm := &onlineParticipant{}
	player := &onlineParticipant{}
	coordinator, err := New(Config{Repository: repo, BucketResolver: onlineBuckets{}, BucketVersion: 1, Now: func() time.Time { return now }, Farm: farm, Player: player})
	if err != nil {
		t.Fatal(err)
	}
	input := onlineInput(t)
	if _, _, err := coordinator.CreateOrLoad(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	changed := input
	changed.PlotID++
	result, err := coordinator.Start(context.Background(), changed)
	if !errors.Is(err, workflow.ErrIdempotencyConflict) || result.Status != commonpb.CommandStatus_REJECTED ||
		result.Outcome != harvest.OutcomeRejected || result.Error == nil ||
		result.Error.GetErrorCode() != commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT {
		t.Fatalf("payload conflict result=%+v err=%v", result, err)
	}
	if farm.callsCount() != 0 || player.callsCount() != 0 {
		t.Fatal("payload conflict attempted Resume/Participant work")
	}
}

func onlineInput(t *testing.T) harvest.HarvestInput {
	t.Helper()
	cycle, err := workidentity.StableOpaqueID("harvest-online-test-cycle", "one")
	if err != nil {
		t.Fatal(err)
	}
	return harvest.HarvestInput{SubjectPlayerID: 10001, OperationID: "online-harvest-1", TargetFarmID: 10001, PlotID: 3, CropCycleID: cycle, ExpectedPlotVersion: 7}
}

type onlineBuckets struct{}

func (onlineBuckets) IndependentWorkBucket(string, uint32) (uint32, error) { return 1, nil }
func (onlineBuckets) AuthorityPersistenceBucket(workidentity.RouteDomain, uint64, uint32) (uint32, error) {
	return 0, nil
}

type onlineParticipant struct {
	mu          sync.Mutex
	loseFirst   bool
	farmOutcome workflow.StepOutcome
	stored      map[string]workflow.StepResult
	calls       int
}

func (p *onlineParticipant) Invoke(_ context.Context, call workflow.StepCall) (workflow.StepResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	if p.stored == nil {
		p.stored = make(map[string]workflow.StepResult)
	}
	if result, ok := p.stored[call.InvocationID]; ok {
		result.Replayed = true
		return result, nil
	}
	result, err := onlineResult(call, p.farmOutcome)
	if err != nil {
		return workflow.StepResult{}, err
	}
	p.stored[call.InvocationID] = result
	if p.loseFirst {
		p.loseFirst = false
		return workflow.StepResult{}, errors.New("online response lost")
	}
	return result, nil
}

func (p *onlineParticipant) callsCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

func onlineResult(call workflow.StepCall, farmOutcome workflow.StepOutcome) (workflow.StepResult, error) {
	if call.StepName == string(identity.StepCommitHarvest) && farmOutcome != workflow.StepOutcomeUnspecified && farmOutcome != workflow.StepOutcomeApplied {
		return workflow.StepResult{Outcome: farmOutcome, ErrorClass: "CROP_NOT_MATURE"}, nil
	}
	var message proto.Message
	command := contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_HARVEST
	if call.StepName == string(identity.StepCommitHarvest) {
		payload := &contractspb.CommitHarvestPayload{}
		if err := proto.Unmarshal(call.Payload, payload); err != nil {
			return workflow.StepResult{}, err
		}
		decision := &contractspb.HarvestDecision{WorkflowId: call.Work.WorkID, OwnerPlayerId: payload.GetOwnerPlayerId(), FarmId: payload.GetFarmId(), PlotId: payload.GetPlotId(), CropCycleId: payload.GetCropCycleId(), CropDefinitionId: 7, RewardItemId: 702, RewardQuantity: 50, RewardExp: 10, CropRuleVersion: 42, CompendiumUnlockSnapshot: &contractspb.CompendiumUnlockSnapshot{CropDefinitionId: 7, UnlockEligible: true, PlantedByPlayerId: payload.GetOwnerPlayerId(), CatalogVersionAtPlant: 9, DefinitionVersionAtPlant: 11}}
		decision.HarvestDecisionHash = decisionHashOnline(decision)
		message = &contractspb.CommitHarvestResult{HarvestDecision: decision, CommittedPlotVersion: 8, CommittedFarmVersion: 9, FarmRecordVersion: 10}
	} else {
		command = contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD
		payload := &contractspb.GrantHarvestRewardPayload{}
		if err := proto.Unmarshal(call.Payload, payload); err != nil {
			return workflow.StepResult{}, err
		}
		decision := payload.GetHarvestDecision()
		crop := decision.GetCropDefinitionId()
		message = &contractspb.GrantHarvestRewardResult{CropCycleId: decision.GetCropCycleId(), RewardItemId: decision.GetRewardItemId(), GrantedQuantity: decision.GetRewardQuantity(), GrantedExp: decision.GetRewardExp(), CompendiumUnlockDelta: &contractspb.CompendiumUnlockDelta{NewlyUnlocked: true, CropDefinitionId: &crop, CompendiumRevisionAfter: 3}, PlayerRecordVersion: 12}
	}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	if err != nil {
		return workflow.StepResult{}, err
	}
	spec, err := contracts.LookupParticipantCommand(command)
	if err != nil {
		return workflow.StepResult{}, err
	}
	hash, err := contracts.CanonicalParticipantHash(spec, message)
	if err != nil {
		return workflow.StepResult{}, err
	}
	return workflow.StepResult{Outcome: workflow.StepOutcomeApplied, Result: data, ResultHash: hash}, nil
}

func decisionHashOnline(decision *contractspb.HarvestDecision) []byte {
	clone := proto.Clone(decision).(*contractspb.HarvestDecision)
	clone.HarvestDecisionHash = nil
	hash, err := contracts.CanonicalHash(clone)
	if err != nil {
		panic(err)
	}
	return hash[:]
}
