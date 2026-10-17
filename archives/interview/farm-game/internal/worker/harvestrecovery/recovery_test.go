package harvestrecovery

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"
	"time"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actorrouting"
	"farmgame/internal/contracts"
	"farmgame/internal/identity"
	"farmgame/internal/scenario/harvest"
	"farmgame/internal/workflow"
	"farmgame/internal/workidentity"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestHarvestRecoveryRestartAndTakeoverReusesExactWorkAndInvocation(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	clock := func() time.Time { return now }
	repo := workflow.NewMemoryWorkRepository(clock)
	root := recoveryRoot(t, "harvest-worker-restart")
	address, err := root.Address(1, 7)
	if err != nil {
		t.Fatal(err)
	}
	record, err := workflow.NewWorkRecord(address, 10001, "harvest-worker-restart", root.PayloadBytes, root.PayloadHash,
		[]string{string(identity.StepCommitHarvest), string(identity.StepGrantHarvestReward)}, now, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := repo.CreateIfAbsent(context.Background(), record); err != nil {
		t.Fatal(err)
	}

	farm := &recoveryParticipant{loseFirst: true}
	player := &recoveryParticipant{}
	first, err := New(Config{
		Repository: repo, Farm: farm, Player: player, Route: recoveryRoute,
		Owner: "worker-a", Token: "token-a", BootID: "boot-a", LeaseTTL: time.Second,
		SafetyMargin: 100 * time.Millisecond, OperationTimeout: time.Second, Now: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := first.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Harvest.Claimed != 1 || stats.Harvest.Retried != 1 || stats.Harvest.Completed != 0 {
		t.Fatalf("first recovery stats=%+v", stats.Harvest)
	}

	stored, err := repo.Get(context.Background(), FamilyHarvest, root.WorkflowID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.State != workflow.WorkStateStepUnknown || stored.Steps[0].PayloadHash == ([32]byte{}) {
		t.Fatalf("Farm response-loss boundary was not durable: %+v", stored)
	}
	firstCall := farm.callsCopy()[0]
	now = time.UnixMilli(stored.NextDueAtMs + 1)

	second, err := New(Config{
		Repository: repo, Farm: farm, Player: player, Route: recoveryRoute,
		Owner: "worker-b", Token: "token-b", BootID: "boot-b", LeaseTTL: time.Second,
		SafetyMargin: 100 * time.Millisecond, OperationTimeout: time.Second, Now: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	stats, err = second.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Harvest.Claimed != 1 || stats.Harvest.Completed != 1 {
		t.Fatalf("takeover recovery stats=%+v", stats.Harvest)
	}

	farmCalls := farm.callsCopy()
	if len(farmCalls) != 2 {
		t.Fatalf("Farm ResumeStep calls=%d, want 2", len(farmCalls))
	}
	secondCall := farmCalls[1]
	if firstCall.GetRecovery().GetWork().GetWorkId() != secondCall.GetRecovery().GetWork().GetWorkId() ||
		firstCall.GetRecovery().GetParticipantInvocationId() != secondCall.GetRecovery().GetParticipantInvocationId() ||
		!bytesEqual(firstCall.GetTypedPayload(), secondCall.GetTypedPayload()) {
		t.Fatalf("takeover changed durable WorkAddress/invocation/payload: first=%v second=%v", firstCall.GetRecovery(), secondCall.GetRecovery())
	}
	if firstCall.GetRecovery().GetCallerInstanceId() != "worker-a" || firstCall.GetRecovery().GetCallerBootId() != "boot-a" ||
		firstCall.GetRecovery().GetLeaseOwnerToken() != "token-a" || firstCall.GetRecovery().GetLeaseEpoch() == 0 {
		t.Fatalf("first lease identity=%v", firstCall.GetRecovery())
	}
	if secondCall.GetRecovery().GetCallerInstanceId() != "worker-b" || secondCall.GetRecovery().GetCallerBootId() != "boot-b" ||
		secondCall.GetRecovery().GetLeaseOwnerToken() != "token-b" ||
		secondCall.GetRecovery().GetLeaseEpoch() <= firstCall.GetRecovery().GetLeaseEpoch() {
		t.Fatalf("takeover lease identity=%v first_epoch=%d", secondCall.GetRecovery(), firstCall.GetRecovery().GetLeaseEpoch())
	}
	if len(player.callsCopy()) != 1 {
		t.Fatalf("Player ResumeStep calls=%d, want one after Farm replay", len(player.callsCopy()))
	}
	completed, err := repo.Get(context.Background(), FamilyHarvest, root.WorkflowID)
	if err != nil {
		t.Fatal(err)
	}
	if completed.State != workflow.WorkStateCompleted || completed.Steps[1].Status != workflow.StepApplied {
		t.Fatalf("completed record=%+v", completed)
	}
}

// TestHarvestRecoveryStaleWorkProofReloadsSameInvocation drives the harvest
// worker recovery path with a WorkVersion proof mismatch on the first Farm
// ResumeStep (another driver advanced the record). The RPC adapter must
// surface workflow.ErrStaleWorkProof, the engine must reload the
// authoritative record and re-invoke the SAME invocation with the same
// payload — never StepUnknown/REJECTED, never a double-apply — and the rest
// of the workflow (Player GRANT) still converges.
func TestHarvestRecoveryStaleWorkProofReloadsSameInvocation(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	clock := func() time.Time { return now }
	repo := workflow.NewMemoryWorkRepository(clock)
	root := recoveryRoot(t, "harvest-worker-stale")
	address, err := root.Address(1, 7)
	if err != nil {
		t.Fatal(err)
	}
	record, err := workflow.NewWorkRecord(address, 10001, "harvest-worker-stale", root.PayloadBytes, root.PayloadHash,
		[]string{string(identity.StepCommitHarvest), string(identity.StepGrantHarvestReward)}, now, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := repo.CreateIfAbsent(context.Background(), record); err != nil {
		t.Fatal(err)
	}

	farm := &recoveryParticipant{staleOnce: true}
	player := &recoveryParticipant{}
	composition, err := New(Config{
		Repository: repo, Farm: farm, Player: player, Route: recoveryRoute,
		Owner: "worker-stale", Token: "token-stale", BootID: "boot-stale", LeaseTTL: time.Second,
		SafetyMargin: 100 * time.Millisecond, OperationTimeout: time.Second, Now: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := composition.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Harvest.Claimed != 1 || stats.Harvest.Completed != 1 {
		t.Fatalf("recovery stats=%+v", stats.Harvest)
	}

	farmCalls := farm.callsCopy()
	if len(farmCalls) != 2 {
		t.Fatalf("Farm ResumeStep calls=%d, want 1 stale + 1 reload success", len(farmCalls))
	}
	if farmCalls[0].GetRecovery().GetParticipantInvocationId() != farmCalls[1].GetRecovery().GetParticipantInvocationId() {
		t.Fatalf("stale-proof reload changed invocation: %s -> %s",
			farmCalls[0].GetRecovery().GetParticipantInvocationId(), farmCalls[1].GetRecovery().GetParticipantInvocationId())
	}
	if !bytesEqual(farmCalls[0].GetTypedPayload(), farmCalls[1].GetTypedPayload()) {
		t.Fatal("stale-proof reload changed payload")
	}
	if len(player.callsCopy()) != 1 {
		t.Fatalf("Player ResumeStep calls=%d, want 1", len(player.callsCopy()))
	}
	completed, err := repo.Get(context.Background(), FamilyHarvest, root.WorkflowID)
	if err != nil {
		t.Fatal(err)
	}
	if completed.State != workflow.WorkStateCompleted || completed.Steps[0].Status != workflow.StepApplied || completed.Steps[1].Status != workflow.StepApplied {
		t.Fatalf("completed record=%+v", completed)
	}
	if completed.Steps[0].ErrorClass != "" || completed.LastErrorClass != "" {
		t.Fatalf("stale proof leaked error class: %+v", completed)
	}
}

func TestHarvestRecoveryStopsBeforePlayerAfterLeaseDeadline(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	clock := func() time.Time { return now }
	repo := workflow.NewMemoryWorkRepository(clock)
	root := recoveryRoot(t, "harvest-worker-deadline")
	address, err := root.Address(1, 8)
	if err != nil {
		t.Fatal(err)
	}
	record, err := workflow.NewWorkRecord(address, 10001, "harvest-worker-deadline", root.PayloadBytes, root.PayloadHash,
		[]string{string(identity.StepCommitHarvest), string(identity.StepGrantHarvestReward)}, now, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := repo.CreateIfAbsent(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	farm := &recoveryParticipant{onCall: func() { now = now.Add(time.Second) }}
	player := &recoveryParticipant{}
	composition, err := New(Config{
		Repository: repo, Farm: farm, Player: player, Route: recoveryRoute,
		Owner: "worker-deadline", Token: "token-deadline", BootID: "boot-deadline",
		LeaseTTL: 100 * time.Millisecond, SafetyMargin: 20 * time.Millisecond,
		OperationTimeout: time.Second, Now: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := composition.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Harvest.LeaseLost != 1 || len(farm.callsCopy()) != 1 || len(player.callsCopy()) != 0 {
		t.Fatalf("deadline stats=%+v Farm=%d Player=%d", stats.Harvest, len(farm.callsCopy()), len(player.callsCopy()))
	}
	stored, err := repo.Get(context.Background(), FamilyHarvest, root.WorkflowID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.CurrentStep != 0 || stored.State != workflow.WorkStateCreated {
		t.Fatalf("expired lease unexpectedly advanced WorkRecord: %+v", stored)
	}
}

func TestSharedRecoveryStopsLateHarvestAfterMixedClaimDeadline(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	clock := func() time.Time { return now }
	baseRepo := workflow.NewMemoryWorkRepository(clock)
	repo := &countingWorkRepository{WorkRepository: baseRepo}
	claimTiming := workflow.NewClaimTimingRegistry()

	plantID, err := workidentity.StableOpaqueID("hv4-mixed-plant", "mixed-batch")
	if err != nil {
		t.Fatal(err)
	}
	plantPayload := []byte("mixed-plant")
	plantRecord, err := workflow.NewWorkRecord(workidentity.WorkAddress{
		WorkID: plantID, WorkType: "PLANT", PlacementKind: workidentity.IndependentWorkBucket,
		BucketVersion: 1, BucketID: 7,
	}, 10001, "hv4-mixed-plant", plantPayload, sha256.Sum256(plantPayload), []string{"PLANT_STEP"},
		now.Add(-2*time.Second), now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := baseRepo.CreateIfAbsent(context.Background(), plantRecord); err != nil {
		t.Fatal(err)
	}

	root := recoveryRoot(t, "hv4-mixed-harvest")
	harvestAddress, err := root.Address(1, 7)
	if err != nil {
		t.Fatal(err)
	}
	harvestRecord, err := workflow.NewWorkRecord(harvestAddress, 10001, "hv4-mixed-harvest",
		root.PayloadBytes, root.PayloadHash,
		[]string{string(identity.StepCommitHarvest), string(identity.StepGrantHarvestReward)},
		now.Add(-time.Second), now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := baseRepo.CreateIfAbsent(context.Background(), harvestRecord); err != nil {
		t.Fatal(err)
	}

	farm := &recoveryParticipant{}
	player := &recoveryParticipant{}
	harvest, err := New(Config{
		Repository: repo, Farm: farm, Player: player, Route: recoveryRoute,
		Owner: "worker-mixed", Token: "token-mixed", BootID: "boot-mixed",
		ClaimLimit: 2, LeaseTTL: time.Second, SafetyMargin: 200 * time.Millisecond,
		OperationTimeout: time.Second, Now: clock, ClaimTiming: claimTiming,
	})
	if err != nil {
		t.Fatal(err)
	}

	loop, err := workflow.NewRecoveryLoop(repo, map[string]workflow.WorkHandler{
		"PLANT":       mixedBatchPlantHandler{advance: func() { now = now.Add(900 * time.Millisecond) }},
		FamilyHarvest: harvest.HarvestHandler,
	}, workflow.RecoveryConfig{
		Owner: "worker-mixed", Token: "token-mixed", ClaimLimit: 2,
		LeaseTTL: time.Second, SafetyMargin: 200 * time.Millisecond,
		ClaimTiming: claimTiming, ClaimNow: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := loop.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if repo.claims != 1 {
		t.Fatalf("ClaimDue calls=%d, want exactly one shared claim", repo.claims)
	}
	if stats.Claimed != 2 || stats.Completed != 1 || stats.Retried != 1 || stats.Failed != 0 {
		t.Fatalf("mixed recovery stats=%+v", stats)
	}
	if len(farm.callsCopy()) != 0 || len(player.callsCopy()) != 0 {
		t.Fatalf("late Harvest started a participant: Farm=%d Player=%d", len(farm.callsCopy()), len(player.callsCopy()))
	}
	stored, err := baseRepo.Get(context.Background(), FamilyHarvest, harvestAddress.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.LastErrorClass != "LEASE_BUDGET_EXHAUSTED" || stored.RetryCount != 1 {
		t.Fatalf("late Harvest was not fail-closed before participant execution: %+v", stored)
	}
}

func recoveryRoot(t *testing.T, operationID string) harvest.RootIntent {
	t.Helper()
	cycle, err := workidentity.StableOpaqueID("harvest-worker-cycle", operationID)
	if err != nil {
		t.Fatal(err)
	}
	root, err := harvest.BuildRoot(harvest.HarvestInput{
		SubjectPlayerID: 10001, OperationID: operationID, TargetFarmID: 10001,
		PlotID: 3, CropCycleID: cycle, ExpectedPlotVersion: 7,
	})
	if err != nil {
		t.Fatal(err)
	}
	return root
}

type harvestRecoveryRouteResolver struct{ assignment actorrouting.Assignment }

func (r harvestRecoveryRouteResolver) Resolve(context.Context, actorrouting.Address) (actorrouting.Assignment, error) {
	return r.assignment, nil
}

func TestHarvestRecoveryRouteLeaseProofCompatibility(t *testing.T) {
	assignment := actorrouting.Assignment{Kind: actorrouting.DomainFarm, ShardID: 3, ShardRevision: 4,
		HolderPod: "holder", HolderBootID: "boot", Endpoint: "farm:1", WriterEpoch: 5,
		LeaseEpoch: 6, LeaseUntilMs: 8_000}
	provider := recoveryRouteProvider(harvestRecoveryRouteResolver{assignment}, "realm-a")
	header, err := provider(context.Background(), workflow.StepCall{},
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_HARVEST, 10001)
	if err != nil {
		t.Fatal(err)
	}
	if header.GetExpectedLeaseEpoch() != 6 || header.GetExpectedLeaseUntilMs() != 8_000 {
		t.Fatalf("recovery producer omitted lease proof: %+v", header)
	}
	legacy := proto.Clone(header).(*logicrpcpb.ActorRouteHeader)
	legacy.ExpectedLeaseEpoch, legacy.ExpectedLeaseUntilMs = 0, nil
	partial := proto.Clone(header).(*logicrpcpb.ActorRouteHeader)
	partial.ExpectedLeaseEpoch = 0
	if !sameAssignment(header, assignment) || !sameAssignment(legacy, assignment) || sameAssignment(partial, assignment) {
		t.Fatalf("mixed-version assignment current=%v legacy=%v partial=%v", sameAssignment(header, assignment), sameAssignment(legacy, assignment), sameAssignment(partial, assignment))
	}
}

func recoveryRoute(_ context.Context, _ workflow.StepCall, command contractspb.ParticipantCommandType, actorID uint64) (*logicrpcpb.ActorRouteHeader, error) {
	domain := logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM
	if command == contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD {
		domain = logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER
	}
	if actorID == 0 {
		return nil, errors.New("missing recovery route actor")
	}
	return &logicrpcpb.ActorRouteHeader{
		RouteDomain: domain, ActorId: actorID, VirtualShard: 3, ShardRevision: 4,
		ExpectedHolderId: "holder", ExpectedHolderBootId: "boot", ExpectedWriterEpoch: 5,
	}, nil
}

type recoveryParticipant struct {
	mu        sync.Mutex
	loseFirst bool
	stored    map[string]*contractspb.ParticipantResponse
	calls     []*contractspb.ParticipantRequest
	onCall    func()
	// staleOnce simulates a WorkVersion proof mismatch on the first ResumeStep
	// call for each new invocation; subsequent calls execute normally. The RPC
	// adapter must surface it as workflow.ErrStaleWorkProof and the engine must
	// reload and re-invoke the same invocation.
	staleOnce bool
}

type countingWorkRepository struct {
	workflow.WorkRepository
	claims int
}

func (r *countingWorkRepository) ClaimDue(ctx context.Context, owner, token string, limit int, ttl time.Duration) ([]*workflow.ClaimedWork, error) {
	r.claims++
	return r.WorkRepository.ClaimDue(ctx, owner, token, limit, ttl)
}

type mixedBatchPlantHandler struct {
	advance func()
}

func (h mixedBatchPlantHandler) Resume(context.Context, *workflow.WorkRecord, workflow.LeaseController) error {
	if h.advance != nil {
		h.advance()
	}
	return nil
}

func (p *recoveryParticipant) ResumeStep(_ context.Context, request *contractspb.ParticipantRequest, _ ...grpc.CallOption) (*contractspb.ParticipantResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stored == nil {
		p.stored = make(map[string]*contractspb.ParticipantResponse)
	}
	clone := proto.Clone(request).(*contractspb.ParticipantRequest)
	p.calls = append(p.calls, clone)
	if replay, ok := p.stored[request.GetRecovery().GetParticipantInvocationId()]; ok {
		response := proto.Clone(replay).(*contractspb.ParticipantResponse)
		response.ReplayedFromReceipt = true
		return response, nil
	}
	if p.onCall != nil {
		p.onCall()
	}
	if p.staleOnce {
		p.staleOnce = false
		return nil, status.Error(codes.FailedPrecondition, "participant proof: WorkVersion mismatch")
	}
	response, err := recoveryResponse(request)
	if err != nil {
		return nil, err
	}
	p.stored[request.GetRecovery().GetParticipantInvocationId()] = proto.Clone(response).(*contractspb.ParticipantResponse)
	if p.loseFirst {
		p.loseFirst = false
		return nil, errors.New("response lost after participant receipt")
	}
	return response, nil
}

func (p *recoveryParticipant) callsCopy() []*contractspb.ParticipantRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := make([]*contractspb.ParticipantRequest, len(p.calls))
	for i, call := range p.calls {
		result[i] = proto.Clone(call).(*contractspb.ParticipantRequest)
	}
	return result
}

func recoveryResponse(request *contractspb.ParticipantRequest) (*contractspb.ParticipantResponse, error) {
	command := request.GetParticipantCommand()
	var message proto.Message
	switch command {
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_HARVEST:
		payload := &contractspb.CommitHarvestPayload{}
		if err := proto.Unmarshal(request.GetTypedPayload(), payload); err != nil {
			return nil, err
		}
		decision := &contractspb.HarvestDecision{
			WorkflowId: request.GetRecovery().GetWork().GetWorkId(), OwnerPlayerId: payload.GetOwnerPlayerId(),
			FarmId: payload.GetFarmId(), PlotId: payload.GetPlotId(), CropCycleId: payload.GetCropCycleId(),
			CropDefinitionId: 7, RewardItemId: 702, RewardQuantity: 50, RewardExp: 10, CropRuleVersion: 42,
			CompendiumUnlockSnapshot: &contractspb.CompendiumUnlockSnapshot{
				CropDefinitionId: 7, UnlockEligible: true, PlantedByPlayerId: payload.GetOwnerPlayerId(),
				CatalogVersionAtPlant: 9, DefinitionVersionAtPlant: 11,
			},
		}
		decision.HarvestDecisionHash = recoveryDecisionHash(decision)
		message = &contractspb.CommitHarvestResult{HarvestDecision: decision, CommittedPlotVersion: 8, CommittedFarmVersion: 9, FarmRecordVersion: 10}
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD:
		payload := &contractspb.GrantHarvestRewardPayload{}
		if err := proto.Unmarshal(request.GetTypedPayload(), payload); err != nil {
			return nil, err
		}
		decision := payload.GetHarvestDecision()
		cropID := decision.GetCropDefinitionId()
		message = &contractspb.GrantHarvestRewardResult{
			CropCycleId: decision.GetCropCycleId(), RewardItemId: decision.GetRewardItemId(),
			GrantedQuantity: decision.GetRewardQuantity(), GrantedExp: decision.GetRewardExp(),
			CompendiumUnlockDelta: &contractspb.CompendiumUnlockDelta{NewlyUnlocked: true, CropDefinitionId: &cropID, CompendiumRevisionAfter: 3},
			PlayerRecordVersion:   12,
		}
	default:
		return nil, errors.New("unexpected Harvest command")
	}
	typed, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	if err != nil {
		return nil, err
	}
	spec, err := contracts.LookupParticipantCommand(command)
	if err != nil {
		return nil, err
	}
	hash, err := contracts.CanonicalParticipantHash(spec, message)
	if err != nil {
		return nil, err
	}
	return &contractspb.ParticipantResponse{
		ParticipantCommand: command, ParticipantInvocationId: request.GetRecovery().GetParticipantInvocationId(),
		Outcome:     contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED,
		TypedResult: typed, ParticipantResultHash: hash[:], CommittedRecordVersion: 12,
	}, nil
}

func recoveryDecisionHash(decision *contractspb.HarvestDecision) []byte {
	clone := proto.Clone(decision).(*contractspb.HarvestDecision)
	clone.HarvestDecisionHash = nil
	hash, err := contracts.CanonicalHash(clone)
	if err != nil {
		panic(err)
	}
	return hash[:]
}

func bytesEqual(left, right []byte) bool { return string(left) == string(right) }
