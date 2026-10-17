package participant

import (
	"bytes"
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	playerpb "farmgame/api/gen/player"
	taskpb "farmgame/api/gen/task"
	"farmgame/internal/actorrouting"
	"farmgame/internal/contracts"
	"farmgame/internal/identity"
	playeractor "farmgame/internal/player/actor"
	playeringress "farmgame/internal/player/ingress"
	playertask "farmgame/internal/player/task"
	"farmgame/internal/storage/authoritystore"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const careTestFarmID uint64 = 7001

// careTaskRepository is an immutable test double for the versioned
// TaskConfigRepository. It deliberately exposes repository-owned task IDs and
// config versions instead of allowing the Participant to hard-code either.
type careTaskRepository struct {
	assignments map[string]playertask.Assignment
	snapshots   map[uint64]*playertask.Config
	chain       *playertask.GrowthChain
}

func (r careTaskRepository) AssignmentForPeriod(periodID string) (playertask.Assignment, bool) {
	assignment, ok := r.assignments[periodID]
	return assignment, ok
}

func (r careTaskRepository) DailySnapshot(version uint64) (*playertask.Config, bool) {
	cfg, ok := r.snapshots[version]
	return cfg, ok
}

func (r careTaskRepository) LatestGrowthChain() (*playertask.GrowthChain, bool) {
	return r.chain, r.chain != nil
}

func newCareTaskRepository() careTaskRepository {
	const (
		version = uint64(91)
		hash    = "care-task-config-v91"
	)
	cfg := &playertask.Config{
		Version: version, Timezone: playertask.DefaultGameTimezone, PayloadHash: hash,
		Daily: [playertask.DailySlotCount]playertask.DailyDefinition{
			{Kind: playertask.DailyLogin, TaskID: "repo.login.v1", Reward: playertask.RewardBundle{Coin: 1}},
			{Kind: playertask.DailyFriendVisit, TaskID: "repo.friend_visit.v1", Reward: playertask.RewardBundle{Coin: 2}},
			{Kind: playertask.DailyFriendInteract, TaskID: "repo.friend_interact.v1", Reward: playertask.RewardBundle{Coin: 3}},
		},
	}
	assignments := make(map[string]playertask.Assignment)
	for _, period := range []string{"20260803", "20260804", "20260805"} {
		assignments[period] = playertask.Assignment{PeriodID: period, ConfigVersion: version, ConfigPayloadHash: hash}
	}
	return careTaskRepository{
		assignments: assignments,
		snapshots:   map[uint64]*playertask.Config{version: cfg},
		chain:       &playertask.GrowthChain{Version: 1},
	}
}

type fixtureCareDependencyVerifier struct {
	proofs map[string]CareDependencyProof
}

func (v fixtureCareDependencyVerifier) VerifyCareDependency(_ context.Context, call *contracts.ParticipantCall) (CareDependencyProof, error) {
	if call == nil || call.Work == nil {
		return CareDependencyProof{}, status.Error(codes.FailedPrecondition, "fixture Care SourceOrder call missing")
	}
	proof, ok := v.proofs[call.Work.GetWorkId()]
	if !ok {
		return CareDependencyProof{}, status.Error(codes.FailedPrecondition, "fixture Care SourceOrder is not present")
	}
	return proof, nil
}

func careDependencyForRequest(t *testing.T, request *contractspb.ParticipantRequest) CareDependencyVerifier {
	t.Helper()
	call, err := contracts.ValidateAndDecode(request, contracts.ParticipantCallerCoordinator)
	if err != nil {
		t.Fatal(err)
	}
	payload := call.Payload.(*contractspb.ApplyCareRewardPayload)
	snapshot := payload.GetCareResolutionSnapshot()
	return fixtureCareDependencyVerifier{proofs: map[string]CareDependencyProof{
		call.Work.GetWorkId(): {
			SourceOrderFound: true,
			SourceOrderID:    call.Work.GetWorkId(),
			SourceOrderType:  contractspb.WorkType_WORK_TYPE_CARE_REWARD,
			SourceAuthority:  proto.Clone(call.Work.GetSourceAuthority()).(*contractspb.SourceAuthorityRef),
			Snapshot:         proto.Clone(snapshot).(*contractspb.CareResolutionSnapshot),
			SnapshotHash:     append([]byte(nil), payload.GetCareResolutionHash()...),
			OrderState:       CareRewardOrderStatePending,
			DueIndexBound:    true,
			WorkVersion:      call.ExpectedWorkVersion,
			InvocationID:     call.InvocationID,
			ParticipantStep:  call.ParticipantStep,
			ExecutionMode:    CareDependencyExecutionModeCoordinator,
			FenceValid:       true,
		},
	}}
}

func careRequest(t *testing.T, playerID, ownerID uint64, label string, careContext contractspb.CareContext, resolvedAtMs, rewardExp uint64) *contractspb.ParticipantRequest {
	t.Helper()
	cycle, err := identity.WorkflowID(identity.EffectiveIdempotencyKey{
		SubjectPlayerID:       ownerID,
		StableCommandTypeName: "PLANT",
		OperationID:           "care-cycle-" + label,
		Root:                  identity.OperationRootRef{Kind: identity.RootKindFarmActor, CanonicalID: strconv.FormatUint(careTestFarmID, 10)},
	})
	if err != nil {
		t.Fatal(err)
	}
	incident, err := identity.CareIncidentID(cycle, "WATER", 1)
	if err != nil {
		t.Fatal(err)
	}
	orderID, err := identity.SourceOrderID(identity.WorkTypeCareReward, identity.SourceKindFarmActor, strconv.FormatUint(careTestFarmID, 10), incident, 3)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := &contractspb.CareResolutionSnapshot{
		SourceFarmId:              careTestFarmID,
		SourceOwnerPlayerId:       ownerID,
		CarerPlayerId:             playerID,
		PlotId:                    3,
		CropCycleId:               cycle,
		IncidentId:                incident,
		CareType:                  contractspb.CareType_CARE_TYPE_WATER,
		CareContext:               careContext,
		ResolvedAtMs:              int64(resolvedAtMs),
		CommittedPlotVersion:      2,
		CommittedFarmVersion:      3,
		CareRewardRuleVersion:     42,
		RewardExp:                 rewardExp,
		FriendInteractionEligible: careContext == contractspb.CareContext_CARE_CONTEXT_FRIEND,
	}
	hash, err := CareResolutionHash(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.CareResolutionHash = append([]byte(nil), hash[:]...)
	payload := &contractspb.ApplyCareRewardPayload{
		CarerPlayerId:          playerID,
		CareRewardOrderId:      orderID,
		CareResolutionSnapshot: snapshot,
		CareResolutionHash:     append([]byte(nil), hash[:]...),
	}
	return makeCareRequest(t, orderID, playerID, payload)
}

func makeCareRequest(t *testing.T, orderID string, playerID uint64, payload *contractspb.ApplyCareRewardPayload) *contractspb.ParticipantRequest {
	t.Helper()
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_CARE_REWARD)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	invocation, err := identity.ParticipantInvocationID(spec.WorkType, orderID, spec.Step, 1)
	if err != nil {
		t.Fatal(err)
	}
	calc, err := actorrouting.NewCalculator("participant-test", actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608040804}, 64, 64)
	if err != nil {
		t.Fatal(err)
	}
	shard, err := calc.ShardFor(actorrouting.DomainPlayer, playerID)
	if err != nil {
		t.Fatal(err)
	}
	farmRoute := logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM
	work := &contractspb.WorkAddress{
		WorkId:        orderID,
		WorkType:      contractspb.WorkType_WORK_TYPE_CARE_REWARD,
		PlacementKind: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET,
		BucketVersion: 1,
		BucketId:      fixtureWorkflowBucket(orderID),
		SourceAuthority: &contractspb.SourceAuthorityRef{
			AuthorityKind:          contractspb.SourceAuthorityKind_SOURCE_AUTHORITY_KIND_FARM_ACTOR,
			CanonicalAuthorityId:   strconv.FormatUint(careTestFarmID, 10),
			AuthorityLayoutVersion: plantTestLayout,
			ActorRouteDomain:       &farmRoute,
		},
	}
	leaseUntilMs := participantTestLeaseUntilMs
	return &contractspb.ParticipantRequest{
		RouteHeader: &logicrpcpb.ActorRouteHeader{
			RouteDomain:          spec.TargetRouteDomain,
			ActorId:              playerID,
			VirtualShard:         shard,
			ShardRevision:        1,
			ExpectedHolderId:     plantTestHolder,
			ExpectedHolderBootId: plantTestHolderBoot,
			ExpectedWriterEpoch:  1,
			ExpectedLeaseEpoch:   1,
			ExpectedLeaseUntilMs: &leaseUntilMs,
		},
		ParticipantCommand: contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_CARE_REWARD,
		TypedPayload:       raw,
		CallerContext: &contractspb.ParticipantRequest_Coordinator{Coordinator: &contractspb.CoordinatorWorkContext{
			Work:                    work,
			ExpectedWorkVersion:     1,
			ParticipantStep:         contractspb.ParticipantStep_PARTICIPANT_STEP_APPLY_CARE_REWARD,
			ParticipantInvocationId: invocation,
			OperationId:             "operation-" + orderID,
			SubjectPlayerId:         playerID,
			RemainingBudgetMs:       1000,
		}},
	}
}

func newCareAllowServer(registry *playeractor.Registry, nowMs func() int64, dependency ...CareDependencyVerifier) *Server {
	return newCareServerWithTaskRepository(registry, nowMs, newCareTaskRepository(), dependency...)
}

func newCareServerWithTaskRepository(registry *playeractor.Registry, nowMs func() int64, taskRepository playertask.Repository, dependency ...CareDependencyVerifier) *Server {
	calc, err := actorrouting.NewCalculator("participant-test", actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608040804}, 64, 64)
	if err != nil {
		panic(err)
	}
	var careDependency CareDependencyVerifier
	if len(dependency) > 0 {
		careDependency = dependency[0]
	}
	return NewServer(Options{
		Registry:                registry,
		WorkVersionVerifier:     fixtureWorkVersionVerifier{},
		WorkLeaseVerifier:       fixtureWorkLeaseVerifier{},
		RouteFenceVerifier:      fixtureRouteFenceVerifier{calc: calc},
		WorkAddressVerifier:     fixtureWorkAddressVerifier{},
		IdentityBindingVerifier: fixtureIdentityBindingVerifier{},
		PlantDependencyVerifier: fixturePlantWorkRecordVerifier{},
		CareDependencyVerifier:  careDependency,
		TaskConfigRepository:    taskRepository,
		NowMs:                   nowMs,
		TaskTimezone:            playertask.DefaultGameTimezone,
	})
}

func careTask(rec *playerpb.PlayerRecord) *taskpb.TaskDomainInstance {
	return careTaskKind(rec, playertask.DailyFriendInteract)
}

func careTaskKind(rec *playerpb.PlayerRecord, kind playertask.DailyKind) *taskpb.TaskDomainInstance {
	for _, instance := range rec.GetTaskDomainState().GetDailyTasks() {
		if instance != nil && instance.GetKind() == int32(kind) {
			return instance
		}
	}
	return nil
}

func careDependencyFixture(t *testing.T, request *contractspb.ParticipantRequest) fixtureCareDependencyVerifier {
	t.Helper()
	verifier, ok := careDependencyForRequest(t, request).(fixtureCareDependencyVerifier)
	if !ok {
		t.Fatal("care dependency fixture has unexpected verifier type")
	}
	return verifier
}

func careRequestWithFarmVersion(t *testing.T, base *contractspb.ParticipantRequest, farmVersion uint64) *contractspb.ParticipantRequest {
	t.Helper()
	request := proto.Clone(base).(*contractspb.ParticipantRequest)
	payload := &contractspb.ApplyCareRewardPayload{}
	mustUnmarshal(t, request.GetTypedPayload(), payload)
	snapshot := payload.GetCareResolutionSnapshot()
	snapshot.CommittedFarmVersion = farmVersion
	hash, err := CareResolutionHash(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.CareResolutionHash = append([]byte(nil), hash[:]...)
	payload.CareResolutionHash = append([]byte(nil), hash[:]...)
	payload.CareRewardOrderId, err = identity.SourceOrderID(
		identity.WorkTypeCareReward,
		identity.SourceKindFarmActor,
		strconv.FormatUint(snapshot.GetSourceFarmId(), 10),
		snapshot.GetIncidentId(), farmVersion,
	)
	if err != nil {
		t.Fatal(err)
	}
	return makeCareRequest(t, payload.GetCareRewardOrderId(), payload.GetCarerPlayerId(), payload)
}

func careResult(t *testing.T, response *contractspb.ParticipantResponse) *contractspb.ApplyCareRewardResult {
	t.Helper()
	result := &contractspb.ApplyCareRewardResult{}
	mustUnmarshal(t, response.GetTypedResult(), result)
	return result
}

func TestApplyCareRewardOwnerAndResponseLossReplay(t *testing.T) {
	trig := newParticipantRig(t, "memory", plantTestPlayerID)
	if _, _, err := trig.registry.Ensure(context.Background(), plantTestPlayerID, "care-owner-bootstrap"); err != nil {
		t.Fatal(err)
	}
	resolved := time.Date(2026, time.August, 4, 23, 59, 0, 0, time.FixedZone("CST", 8*60*60)).UnixMilli()
	delivery := time.Date(2026, time.August, 5, 0, 5, 0, 0, time.FixedZone("CST", 8*60*60)).UnixMilli()
	request := careRequest(t, plantTestPlayerID, plantTestPlayerID, "owner-replay", contractspb.CareContext_CARE_CONTEXT_OWNER, uint64(resolved), 7)
	server := newCareAllowServer(trig.registry, func() int64 { return delivery }, careDependencyForRequest(t, request))

	// Treat the first response as lost. The durable ParticipantReceipt is the
	// only source used by this retry, so this is the same recovery path as a
	// dispatcher response-loss replay.
	trig.fault.Inject(authoritystore.MethodCommit, authoritystore.FaultTimeoutUnknownAfterApply)
	first := mustExecute(t, server.ExecuteStep, request)
	firstResult := careResult(t, first)
	if first.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED || first.GetReplayedFromReceipt() ||
		firstResult.GetGrantedExp() != 7 || firstResult.GetFriendInteractionFactCommitted() || firstResult.GetFriendInteractionTaskAdvanced() ||
		firstResult.GetTaskPeriodKey() != "20260804" {
		t.Fatalf("owner first response=%+v result=%+v", first, firstResult)
	}

	replay := mustExecute(t, server.ExecuteStep, request)
	if !replay.GetReplayedFromReceipt() || !bytes.Equal(replay.GetTypedResult(), first.GetTypedResult()) ||
		!bytes.Equal(replay.GetParticipantResultHash(), first.GetParticipantResultHash()) {
		t.Fatalf("owner replay=%+v first=%+v", replay, first)
	}
	collision := proto.Clone(request).(*contractspb.ParticipantRequest)
	collisionPayload := &contractspb.ApplyCareRewardPayload{}
	mustUnmarshal(t, collision.GetTypedPayload(), collisionPayload)
	collisionPayload.CareResolutionSnapshot.RewardExp++
	collisionBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(collisionPayload)
	if err != nil {
		t.Fatal(err)
	}
	collision.TypedPayload = collisionBytes
	if response, err := server.ExecuteStep(context.Background(), collision); response != nil ||
		status.Code(err) != codes.FailedPrecondition || status.Convert(err).Message() != "PARTICIPANT_IDEMPOTENCY_CONFLICT" {
		t.Fatalf("same invocation collision response=%v err=%v", response, err)
	}
	record, version, err := trig.registry.Read(context.Background(), plantTestPlayerID)
	if err != nil {
		t.Fatal(err)
	}
	if record.GetProgression().GetExp() != 7 || version != first.GetCommittedRecordVersion() || record.GetTaskDomainState() != nil || careTask(record) != nil {
		t.Fatalf("owner record exp=%d version=%d task_state=%v task=%v response=%d", record.GetProgression().GetExp(), version, record.GetTaskDomainState(), careTask(record), first.GetCommittedRecordVersion())
	}
	call, err := contracts.ValidateAndDecode(request, contracts.ParticipantCallerCoordinator)
	if err != nil {
		t.Fatal(err)
	}
	if receipt := playeringress.FindParticipantOperationReceipt(record, call.InvocationID); receipt == nil || len(receipt.GetResultPayload()) == 0 {
		t.Fatalf("owner participant receipt missing: %v", receipt)
	}
}

func TestApplyCareRewardFriendUsesResolvedPeriodAndAdvancesOnce(t *testing.T) {
	trig := newParticipantRig(t, "memory", plantTestPlayerID)
	if _, _, err := trig.registry.Ensure(context.Background(), plantTestPlayerID, "care-friend-bootstrap"); err != nil {
		t.Fatal(err)
	}
	loc, err := time.LoadLocation(playertask.DefaultGameTimezone)
	if err != nil {
		t.Fatal(err)
	}
	resolved := time.Date(2026, time.August, 4, 23, 59, 0, 0, loc).UnixMilli()
	delivery := time.Date(2026, time.August, 5, 0, 5, 0, 0, loc).UnixMilli()
	request := careRequest(t, plantTestPlayerID, 8001, "friend-cross-day", contractspb.CareContext_CARE_CONTEXT_FRIEND, uint64(resolved), 11)
	server := newCareAllowServer(trig.registry, func() int64 { return delivery }, careDependencyForRequest(t, request))

	first := mustExecute(t, server.ExecuteStep, request)
	result := careResult(t, first)
	if first.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED || result.GetGrantedExp() != 11 ||
		!result.GetFriendInteractionFactCommitted() || result.GetFriendInteractionTaskAdvanced() || result.GetTaskPeriodKey() != "20260804" {
		t.Fatalf("friend first response=%+v result=%+v", first, result)
	}
	replay := mustExecute(t, server.ExecuteStep, request)
	if !replay.GetReplayedFromReceipt() || !bytes.Equal(replay.GetTypedResult(), first.GetTypedResult()) {
		t.Fatalf("friend replay=%+v first=%+v", replay, first)
	}
	record, version, err := trig.registry.Read(context.Background(), plantTestPlayerID)
	if err != nil {
		t.Fatal(err)
	}
	interaction := careTask(record)
	visit := careTaskKind(record, playertask.DailyFriendVisit)
	if record.GetProgression().GetExp() != 11 || interaction == nil || interaction.GetProgress() != 0 ||
		interaction.GetState() != int32(playertask.StateInProgress) || visit == nil || visit.GetProgress() != 0 ||
		visit.GetState() != int32(playertask.StateInProgress) || record.GetTaskDomainState().GetDailyPeriodId() != "20260805" ||
		record.GetTaskDomainState().GetClaimableCount() != 0 || record.GetTaskDomainState().GetTaskStateVersion() != 1 ||
		version != first.GetCommittedRecordVersion() {
		t.Fatalf("friend record exp=%d visit=%+v interaction=%+v task_state=%+v version=%d response=%d", record.GetProgression().GetExp(), visit, interaction, record.GetTaskDomainState(), version, first.GetCommittedRecordVersion())
	}
}

func TestApplyCareRewardRejectsHashAndTargetBeforeActorTurn(t *testing.T) {
	trig := newParticipantRig(t, "memory", plantTestPlayerID)
	if _, _, err := trig.registry.Ensure(context.Background(), plantTestPlayerID, "care-validation-bootstrap"); err != nil {
		t.Fatal(err)
	}
	resolved := uint64(time.Date(2026, time.August, 4, 12, 0, 0, 0, time.FixedZone("CST", 8*60*60)).UnixMilli())
	valid := careRequest(t, plantTestPlayerID, 8001, "validation", contractspb.CareContext_CARE_CONTEXT_FRIEND, resolved, 5)
	server := newCareAllowServer(trig.registry, nil, careDependencyForRequest(t, valid))

	badHash := proto.Clone(valid).(*contractspb.ParticipantRequest)
	badPayload := &contractspb.ApplyCareRewardPayload{}
	mustUnmarshal(t, badHash.GetTypedPayload(), badPayload)
	badPayload.CareResolutionHash[0] ^= 0xff
	badHash.TypedPayload, _ = proto.MarshalOptions{Deterministic: true}.Marshal(badPayload)
	if response, err := server.ExecuteStep(context.Background(), badHash); response != nil || status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("bad snapshot hash response=%v err=%v", response, err)
	}

	wrongTarget := proto.Clone(valid).(*contractspb.ParticipantRequest)
	wrongTarget.RouteHeader.ActorId = 8002
	if response, err := server.ExecuteStep(context.Background(), wrongTarget); response != nil || status.Code(err) != codes.InvalidArgument {
		t.Fatalf("wrong target response=%v err=%v", response, err)
	}
	record, version, err := trig.registry.Read(context.Background(), plantTestPlayerID)
	if err != nil {
		t.Fatal(err)
	}
	if record.GetProgression().GetExp() != 0 || version != 1 {
		t.Fatalf("invalid Care mutated actor: exp=%d version=%d", record.GetProgression().GetExp(), version)
	}
}

func TestApplyCareRewardConcurrentSameOrderGrantsOnce(t *testing.T) {
	trig := newParticipantRig(t, "memory", plantTestPlayerID)
	if _, _, err := trig.registry.Ensure(context.Background(), plantTestPlayerID, "care-concurrent-bootstrap"); err != nil {
		t.Fatal(err)
	}
	resolved := time.Date(2026, time.August, 4, 12, 0, 0, 0, time.FixedZone("CST", 8*60*60)).UnixMilli()
	request := careRequest(t, plantTestPlayerID, 8001, "concurrent", contractspb.CareContext_CARE_CONTEXT_FRIEND, uint64(resolved), 13)
	server := newCareAllowServer(trig.registry, func() int64 { return resolved }, careDependencyForRequest(t, request))
	const workers = 24
	responses := make(chan *contractspb.ParticipantResponse, workers)
	errors := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			response, err := server.ExecuteStep(context.Background(), request)
			if err != nil {
				errors <- err
				return
			}
			responses <- response
		}()
	}
	wg.Wait()
	close(responses)
	close(errors)
	for err := range errors {
		t.Fatalf("concurrent Care error: %v", err)
	}
	var first *contractspb.ParticipantResponse
	for response := range responses {
		if response.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED || response.GetTypedResult() == nil {
			t.Fatalf("concurrent Care response=%+v", response)
		}
		if first == nil {
			first = response
		}
	}
	if first == nil {
		t.Fatal("concurrent Care returned no responses")
	}
	record, version, err := trig.registry.Read(context.Background(), plantTestPlayerID)
	if err != nil {
		t.Fatal(err)
	}
	if record.GetProgression().GetExp() != 13 || version != first.GetCommittedRecordVersion() {
		t.Fatalf("concurrent Care double grant exp=%d version=%d first=%d", record.GetProgression().GetExp(), version, first.GetCommittedRecordVersion())
	}
	interaction := careTask(record)
	if interaction == nil || interaction.GetProgress() != 1 || interaction.GetState() != int32(playertask.StateClaimable) {
		t.Fatalf("concurrent Care interaction=%+v", interaction)
	}
	visit := careTaskKind(record, playertask.DailyFriendVisit)
	if visit == nil || visit.GetProgress() != 1 || visit.GetState() != int32(playertask.StateClaimable) ||
		record.GetTaskDomainState().GetClaimableCount() != 2 || record.GetTaskDomainState().GetTaskStateVersion() != 1 {
		t.Fatalf("concurrent Care visit=%+v task_state=%+v", visit, record.GetTaskDomainState())
	}
}

func TestApplyCareRewardRejectsMissingSourceOrderVerifier(t *testing.T) {
	trig := newParticipantRig(t, "memory", plantTestPlayerID)
	if _, _, err := trig.registry.Ensure(context.Background(), plantTestPlayerID, "care-missing-source-verifier"); err != nil {
		t.Fatal(err)
	}
	resolved := time.Date(2026, time.August, 4, 12, 0, 0, 0, time.FixedZone("CST", 8*60*60)).UnixMilli()
	request := careRequest(t, plantTestPlayerID, 8001, "missing-source-verifier", contractspb.CareContext_CARE_CONTEXT_FRIEND, uint64(resolved), 5)
	server := newCareAllowServer(trig.registry, func() int64 { return resolved })
	if response, err := server.ExecuteStep(context.Background(), request); response != nil || status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("missing source verifier response=%v err=%v", response, err)
	}
	record, version, err := trig.registry.Read(context.Background(), plantTestPlayerID)
	if err != nil {
		t.Fatal(err)
	}
	if record.GetProgression().GetExp() != 0 || version != 1 || record.GetTaskDomainState() != nil {
		t.Fatalf("missing source verifier mutated PlayerRecord: exp=%d version=%d task=%v", record.GetProgression().GetExp(), version, record.GetTaskDomainState())
	}
}

func TestApplyCareRewardRejectsSecondSelfConsistentOrderID(t *testing.T) {
	trig := newParticipantRig(t, "memory", plantTestPlayerID)
	if _, _, err := trig.registry.Ensure(context.Background(), plantTestPlayerID, "care-second-order-bootstrap"); err != nil {
		t.Fatal(err)
	}
	resolved := time.Date(2026, time.August, 4, 12, 0, 0, 0, time.FixedZone("CST", 8*60*60)).UnixMilli()
	valid := careRequest(t, plantTestPlayerID, 8001, "second-order-original", contractspb.CareContext_CARE_CONTEXT_FRIEND, uint64(resolved), 5)
	// Keep the incident and all payload fields self-consistent, but change the
	// committed Farm version so the deterministic SourceOrderID is a second
	// order for the same incident. The fixture authority contains only valid.
	forged := careRequestWithFarmVersion(t, valid, 4)
	server := newCareAllowServer(trig.registry, func() int64 { return resolved }, careDependencyForRequest(t, valid))
	if response, err := server.ExecuteStep(context.Background(), forged); response != nil || status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("second self-consistent order response=%v err=%v", response, err)
	}
	record, version, err := trig.registry.Read(context.Background(), plantTestPlayerID)
	if err != nil {
		t.Fatal(err)
	}
	if record.GetProgression().GetExp() != 0 || version != 1 {
		t.Fatalf("forged second order mutated PlayerRecord: exp=%d version=%d", record.GetProgression().GetExp(), version)
	}
}

func TestApplyCareRewardRejectsUndispatchableSourceState(t *testing.T) {
	trig := newParticipantRig(t, "memory", plantTestPlayerID)
	if _, _, err := trig.registry.Ensure(context.Background(), plantTestPlayerID, "care-source-state-bootstrap"); err != nil {
		t.Fatal(err)
	}
	resolved := time.Date(2026, time.August, 4, 12, 0, 0, 0, time.FixedZone("CST", 8*60*60)).UnixMilli()
	request := careRequest(t, plantTestPlayerID, 8001, "source-state", contractspb.CareContext_CARE_CONTEXT_FRIEND, uint64(resolved), 5)
	fixture := careDependencyFixture(t, request)
	call, err := contracts.ValidateAndDecode(request, contracts.ParticipantCallerCoordinator)
	if err != nil {
		t.Fatal(err)
	}
	proof := fixture.proofs[call.Work.GetWorkId()]
	proof.OrderState = CareRewardOrderStateUnspecified
	fixture.proofs[call.Work.GetWorkId()] = proof
	server := newCareAllowServer(trig.registry, func() int64 { return resolved }, fixture)
	if response, err := server.ExecuteStep(context.Background(), request); response != nil || status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("undispatchable source state response=%v err=%v", response, err)
	}
	record, version, err := trig.registry.Read(context.Background(), plantTestPlayerID)
	if err != nil {
		t.Fatal(err)
	}
	if record.GetProgression().GetExp() != 0 || version != 1 {
		t.Fatalf("undispatchable source state mutated PlayerRecord: exp=%d version=%d", record.GetProgression().GetExp(), version)
	}
}

func TestApplyCareRewardRejectsSourceSnapshotOrHashMismatch(t *testing.T) {
	mutations := []struct {
		name   string
		mutate func(*CareDependencyProof)
	}{
		{name: "snapshot", mutate: func(proof *CareDependencyProof) {
			proof.Snapshot = proto.Clone(proof.Snapshot).(*contractspb.CareResolutionSnapshot)
			proof.Snapshot.RewardExp++
		}},
		{name: "hash", mutate: func(proof *CareDependencyProof) {
			proof.SnapshotHash = bytes.Repeat([]byte{0x7f}, 32)
		}},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			trig := newParticipantRig(t, "memory", plantTestPlayerID)
			if _, _, err := trig.registry.Ensure(context.Background(), plantTestPlayerID, "care-source-proof-"+mutation.name); err != nil {
				t.Fatal(err)
			}
			resolved := time.Date(2026, time.August, 4, 12, 0, 0, 0, time.FixedZone("CST", 8*60*60)).UnixMilli()
			request := careRequest(t, plantTestPlayerID, 8001, "source-proof-"+mutation.name, contractspb.CareContext_CARE_CONTEXT_FRIEND, uint64(resolved), 5)
			fixture := careDependencyFixture(t, request)
			call, err := contracts.ValidateAndDecode(request, contracts.ParticipantCallerCoordinator)
			if err != nil {
				t.Fatal(err)
			}
			proof := fixture.proofs[call.Work.GetWorkId()]
			mutation.mutate(&proof)
			fixture.proofs[call.Work.GetWorkId()] = proof
			server := newCareAllowServer(trig.registry, func() int64 { return resolved }, fixture)
			if response, err := server.ExecuteStep(context.Background(), request); response != nil || status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("source proof mismatch response=%v err=%v", response, err)
			}
			record, version, err := trig.registry.Read(context.Background(), plantTestPlayerID)
			if err != nil {
				t.Fatal(err)
			}
			if record.GetProgression().GetExp() != 0 || version != 1 {
				t.Fatalf("source proof mismatch mutated PlayerRecord: exp=%d version=%d", record.GetProgression().GetExp(), version)
			}
		})
	}
}

func TestApplyCareRewardFriendTaskDomainUsesRepositoryBatchAndReplay(t *testing.T) {
	trig := newParticipantRig(t, "memory", plantTestPlayerID)
	if _, _, err := trig.registry.Ensure(context.Background(), plantTestPlayerID, "care-task-domain-bootstrap"); err != nil {
		t.Fatal(err)
	}
	loc, err := time.LoadLocation(playertask.DefaultGameTimezone)
	if err != nil {
		t.Fatal(err)
	}
	resolved := time.Date(2026, time.August, 4, 12, 0, 0, 0, loc).UnixMilli()
	request := careRequest(t, plantTestPlayerID, 8001, "task-domain-batch", contractspb.CareContext_CARE_CONTEXT_FRIEND, uint64(resolved), 17)
	server := newCareAllowServer(trig.registry, func() int64 { return resolved }, careDependencyForRequest(t, request))
	first := mustExecute(t, server.ExecuteStep, request)
	result := careResult(t, first)
	if result.GetTaskPeriodKey() != "20260804" || !result.GetFriendInteractionTaskAdvanced() {
		t.Fatalf("friend TaskDomain result=%+v", result)
	}
	record, firstVersion, err := trig.registry.Read(context.Background(), plantTestPlayerID)
	if err != nil {
		t.Fatal(err)
	}
	state := record.GetTaskDomainState()
	visit := careTaskKind(record, playertask.DailyFriendVisit)
	interaction := careTaskKind(record, playertask.DailyFriendInteract)
	if state == nil || len(state.GetDailyTasks()) != playertask.DailySlotCount || state.GetDailyPeriodId() != "20260804" ||
		state.GetDailyConfigVersion() != 91 || state.GetTaskStateVersion() != 1 || state.GetClaimableCount() != 2 ||
		visit == nil || interaction == nil || visit.GetTaskId() != "repo.friend_visit.v1" || interaction.GetTaskId() != "repo.friend_interact.v1" ||
		visit.GetConfigVersion() != 91 || interaction.GetConfigVersion() != 91 || visit.GetProgress() != 1 || interaction.GetProgress() != 1 ||
		visit.GetState() != int32(playertask.StateClaimable) || interaction.GetState() != int32(playertask.StateClaimable) {
		t.Fatalf("TaskDomain repository/batch state=%+v visit=%+v interaction=%+v", state, visit, interaction)
	}
	replay := mustExecute(t, server.ExecuteStep, request)
	if !replay.GetReplayedFromReceipt() || !bytes.Equal(replay.GetTypedResult(), first.GetTypedResult()) || replay.GetCommittedRecordVersion() != first.GetCommittedRecordVersion() {
		t.Fatalf("TaskDomain replay=%+v first=%+v", replay, first)
	}
	replayedRecord, replayedVersion, err := trig.registry.Read(context.Background(), plantTestPlayerID)
	if err != nil {
		t.Fatal(err)
	}
	if replayedVersion != firstVersion || replayedRecord.GetTaskDomainState().GetTaskStateVersion() != state.GetTaskStateVersion() ||
		replayedRecord.GetProgression().GetExp() != 17 {
		t.Fatalf("TaskDomain replay changed truth: version=%d/%d state_version=%d/%d exp=%d", replayedVersion, firstVersion, replayedRecord.GetTaskDomainState().GetTaskStateVersion(), state.GetTaskStateVersion(), replayedRecord.GetProgression().GetExp())
	}
}

func TestApplyCareRewardFailsClosedWhenTaskRepositoryIsMissing(t *testing.T) {
	trig := newParticipantRig(t, "memory", plantTestPlayerID)
	if _, _, err := trig.registry.Ensure(context.Background(), plantTestPlayerID, "care-missing-task-repository"); err != nil {
		t.Fatal(err)
	}
	resolved := time.Date(2026, time.August, 4, 12, 0, 0, 0, time.FixedZone("CST", 8*60*60)).UnixMilli()
	request := careRequest(t, plantTestPlayerID, 8001, "missing-task-repository", contractspb.CareContext_CARE_CONTEXT_FRIEND, uint64(resolved), 5)
	server := newCareServerWithTaskRepository(trig.registry, func() int64 { return resolved }, nil, careDependencyForRequest(t, request))
	if response, err := server.ExecuteStep(context.Background(), request); response != nil || status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("missing TaskConfigRepository response=%v err=%v", response, err)
	}
	record, version, err := trig.registry.Read(context.Background(), plantTestPlayerID)
	if err != nil {
		t.Fatal(err)
	}
	if record.GetProgression().GetExp() != 0 || version != 1 {
		t.Fatalf("missing TaskConfigRepository mutated PlayerRecord: exp=%d version=%d", record.GetProgression().GetExp(), version)
	}
}

func TestApplyCareRewardPeriodIsServerConfiguredAcrossInstances(t *testing.T) {
	loc, err := time.LoadLocation(playertask.DefaultGameTimezone)
	if err != nil {
		t.Fatal(err)
	}
	resolved := time.Date(2026, time.August, 4, 23, 59, 0, 0, loc).UnixMilli()
	// The Participant payload has no timezone field. Two independently built
	// service instances use the same immutable P0 server configuration and
	// therefore classify the same source occurrence identically.
	trigA := newParticipantRig(t, "memory", plantTestPlayerID)
	trigB := newParticipantRig(t, "memory", plantTestPlayerID)
	for _, trig := range []*participantTestRig{trigA, trigB} {
		if _, _, err := trig.registry.Ensure(context.Background(), plantTestPlayerID, "care-period-bootstrap"); err != nil {
			t.Fatal(err)
		}
	}
	requestA := careRequest(t, plantTestPlayerID, 8001, "period-instance-a", contractspb.CareContext_CARE_CONTEXT_FRIEND, uint64(resolved), 3)
	requestB := careRequest(t, plantTestPlayerID, 8001, "period-instance-b", contractspb.CareContext_CARE_CONTEXT_FRIEND, uint64(resolved), 3)
	serverA := newCareAllowServer(trigA.registry, func() int64 { return time.Date(2026, time.August, 5, 0, 5, 0, 0, loc).UnixMilli() }, careDependencyForRequest(t, requestA))
	serverB := newCareAllowServer(trigB.registry, func() int64 { return time.Date(2026, time.August, 5, 0, 5, 0, 0, loc).UnixMilli() }, careDependencyForRequest(t, requestB))
	resultA := careResult(t, mustExecute(t, serverA.ExecuteStep, requestA))
	resultB := careResult(t, mustExecute(t, serverB.ExecuteStep, requestB))
	if resultA.GetTaskPeriodKey() != "20260804" || resultB.GetTaskPeriodKey() != "20260804" || resultA.GetTaskPeriodKey() != resultB.GetTaskPeriodKey() {
		t.Fatalf("service instances diverged on server-configured period: A=%s B=%s", resultA.GetTaskPeriodKey(), resultB.GetTaskPeriodKey())
	}
}

func TestValidateCareDependencyProofRequiresInvocationFence(t *testing.T) {
	resolved := time.Date(2026, time.August, 4, 12, 0, 0, 0, time.FixedZone("CST", 8*60*60)).UnixMilli()
	request := careRequest(t, plantTestPlayerID, 8001, "invocation-fence", contractspb.CareContext_CARE_CONTEXT_FRIEND, uint64(resolved), 5)
	call, err := contracts.ValidateAndDecode(request, contracts.ParticipantCallerCoordinator)
	if err != nil {
		t.Fatal(err)
	}
	fixture := careDependencyFixture(t, request)
	valid := fixture.proofs[call.Work.GetWorkId()]
	mutations := []struct {
		name   string
		mutate func(*CareDependencyProof)
	}{
		{name: "wrong-order-type", mutate: func(proof *CareDependencyProof) { proof.SourceOrderType = contractspb.WorkType_WORK_TYPE_PLANT }},
		{name: "missing-fence", mutate: func(proof *CareDependencyProof) { proof.FenceValid = false }},
		{name: "missing-due-index", mutate: func(proof *CareDependencyProof) { proof.DueIndexBound = false }},
		{name: "wrong-invocation", mutate: func(proof *CareDependencyProof) { proof.InvocationID = "other-invocation" }},
		{name: "wrong-coordinator-mode", mutate: func(proof *CareDependencyProof) { proof.ExecutionMode = CareDependencyExecutionModeRecoveryLease }},
		{name: "unexpected-valid-lease", mutate: func(proof *CareDependencyProof) { proof.LeaseValid = true }},
		{name: "unexpected-lease", mutate: func(proof *CareDependencyProof) { proof.LeaseOwnerToken = "unexpected" }},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			proof := valid
			mutation.mutate(&proof)
			if err := validateCareDependencyProof(call, proof); err == nil {
				t.Fatal("expected invocation/fence proof rejection")
			}
		})
	}
	workerRequest := proto.Clone(request).(*contractspb.ParticipantRequest)
	workerRequest.CallerContext = &contractspb.ParticipantRequest_Recovery{Recovery: &contractspb.InternalCallerContext{
		CallerType:              contractspb.InternalCallerType_INTERNAL_CALLER_TYPE_WORKER_RECOVERY,
		CallerInstanceId:        "worker-1",
		CallerBootId:            "worker-boot-1",
		Work:                    proto.Clone(call.Work).(*contractspb.WorkAddress),
		ExpectedWorkVersion:     call.ExpectedWorkVersion,
		ParticipantStep:         request.GetCoordinator().GetParticipantStep(),
		ParticipantInvocationId: call.InvocationID,
		LeaseOwnerToken:         "lease-token",
		LeaseEpoch:              7,
		RemainingBudgetMs:       1000,
	}}
	workerCall, err := contracts.ValidateAndDecode(workerRequest, contracts.ParticipantCallerWorkerRecovery)
	if err != nil {
		t.Fatal(err)
	}
	workerProof := valid
	workerProof.OrderState = CareRewardOrderStateRewardDispatching
	workerProof.ExecutionMode = CareDependencyExecutionModeRecoveryLease
	workerProof.LeaseValid = true
	workerProof.LeaseOwnerToken = "lease-token"
	workerProof.LeaseEpoch = 7
	workerProof.LeaseExpireAtMs = resolved + int64(time.Hour/time.Millisecond)
	if err := validateCareDependencyProof(workerCall, workerProof); err != nil {
		t.Fatalf("valid recovery lease proof rejected: %v", err)
	}
	workerProof.LeaseValid = false
	if err := validateCareDependencyProof(workerCall, workerProof); err == nil {
		t.Fatal("recovery proof without explicit lease validity was accepted")
	}
}
