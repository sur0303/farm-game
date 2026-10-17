package ingress

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/contracts"
	"farmgame/internal/cropcatalog"
	"farmgame/internal/friend"
	"farmgame/internal/identity"
	plantscenario "farmgame/internal/scenario/plant"
	theftscenario "farmgame/internal/scenario/theft"
	redisauthority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workflow"
	"farmgame/internal/workidentity"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

const lookupTestSubject = uint64(10001)

type lookupTestBucketResolver struct{}

func (lookupTestBucketResolver) IndependentWorkBucket(string, uint32) (uint32, error) {
	return 7, nil
}

func (lookupTestBucketResolver) AuthorityPersistenceBucket(workidentity.RouteDomain, uint64, uint32) (uint32, error) {
	return 0, nil
}

type lookupFixture struct {
	repo       *workflow.MemoryWorkRepository
	reader     *OperationResultReader
	root       plantscenario.RootIntent
	record     *workflow.WorkRecord
	resolver   lookupTestBucketResolver
	operation  string
	targetFarm uint64
}

func newLookupFixture(t *testing.T, operationID string) *lookupFixture {
	t.Helper()
	now := time.UnixMilli(1_800_000_000_000)
	definition, ok := cropcatalog.ByDefinitionID(cropcatalog.CarrotDefinitionID)
	if !ok {
		t.Fatal("carrot definition missing")
	}
	snapshot, err := cropcatalog.BuildCropRuleSnapshot(definition)
	if err != nil {
		t.Fatal(err)
	}
	root, err := plantscenario.BuildRoot(plantscenario.PlantInput{
		SubjectPlayerID:         lookupTestSubject,
		OperationID:             operationID,
		TargetFarmID:            lookupTestSubject,
		PlotID:                  3,
		ExpectedPlotVersion:     7,
		CropDefinitionID:        uint32(definition.ID),
		ExpectedCropRuleVersion: definition.CropRuleVersion,
		SeedItemID:              uint32(definition.SeedItemID),
		CropRuleSnapshot:        snapshot,
	})
	if err != nil {
		t.Fatal(err)
	}
	resolver := lookupTestBucketResolver{}
	address, err := root.AddressForResolver(1, resolver)
	if err != nil {
		t.Fatal(err)
	}
	repo := workflow.NewMemoryWorkRepository(func() time.Time { return now })
	record, created, err := repo.CreateIfAbsent(context.Background(), mustLookupRecord(t, address, root, now))
	if err != nil || !created {
		t.Fatalf("create record=%v created=%v err=%v", record, created, err)
	}
	reader, err := NewOperationResultReader(OperationResultReaderConfig{
		Repository: repo, BucketResolver: resolver, BucketVersion: 1,
		RetryAfter: 250 * time.Millisecond, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	return &lookupFixture{repo: repo, reader: reader, root: root, record: record,
		resolver: resolver, operation: operationID, targetFarm: lookupTestSubject}
}

func mustLookupRecord(t *testing.T, address workidentity.WorkAddress, root plantscenario.RootIntent, now time.Time) *workflow.WorkRecord {
	t.Helper()
	record, err := workflow.NewWorkRecord(address, lookupTestSubject, root.Key.OperationID,
		root.PayloadBytes, root.PayloadHash,
		[]string{string(identity.StepReserveSeed), string(identity.StepCommitPlant), string(identity.StepCommitSeedReservation), string(identity.StepReleaseSeedReservation)},
		now, now.Add(24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func plantLookupRequest(operationID string, targetFarmID uint64) *publicpb.GetOperationResultRequest {
	return &publicpb.GetOperationResultRequest{
		OriginalCommandType: commonpb.CommandType_PLANT,
		OriginalOperationId: operationID,
		OriginalTarget: &publicpb.OperationTargetLocator{
			Target: &publicpb.OperationTargetLocator_TargetFarmId{TargetFarmId: targetFarmID},
		},
	}
}

func TestOperationResultCareReturnsNotFoundForSafeSameOperationReplay(t *testing.T) {
	fixture := newLookupFixture(t, "care-result-replay")
	request := &publicpb.GetOperationResultRequest{
		OriginalCommandType: commonpb.CommandType_CARE,
		OriginalOperationId: "care-result-replay",
		OriginalTarget: &publicpb.OperationTargetLocator{
			Target: &publicpb.OperationTargetLocator_TargetFarmId{TargetFarmId: 20002},
		},
	}
	result, err := fixture.reader.Lookup(context.Background(), lookupTestSubject, request)
	if err != nil {
		t.Fatal(err)
	}
	if result.GetLookupStatus() != publicpb.OperationLookupStatus_OPERATION_LOOKUP_NOT_FOUND ||
		result.GetOriginalCommandType() != commonpb.CommandType_CARE ||
		result.GetOriginalOperationId() != request.GetOriginalOperationId() {
		t.Fatalf("CARE lookup=%+v, want honest NOT_FOUND bound to original operation", result)
	}
}

func updateLookupRecord(t *testing.T, fixture *lookupFixture, state workflow.WorkState) *workflow.WorkRecord {
	t.Helper()
	record, err := fixture.repo.Get(context.Background(), string(identity.WorkTypePlant), fixture.root.WorkflowID)
	if err != nil {
		t.Fatal(err)
	}
	record.State = state
	switch state {
	case workflow.WorkStateRunning:
		record.CurrentStep = 1
	case workflow.WorkStateStepUnknown:
		record.CurrentStep = 1
		record.Steps[1].Status = workflow.StepUnknown
	case workflow.WorkStateRejected:
		record.CurrentStep = 0
		record.Steps[0].Status = workflow.StepRejected
		record.Steps[0].ErrorClass = "ERR_INSUFFICIENT_ITEM"
	case workflow.WorkStateCompensated:
		record.CurrentStep = 3
		record.Steps[0].Status = workflow.StepApplied
		record.Steps[1].Status = workflow.StepRejected
		record.Steps[1].ErrorClass = "ERR_PLOT_NOT_EMPTY"
		record.Steps[3].Status = workflow.StepApplied
	case workflow.WorkStateCompleted:
		record.CurrentStep = 2
		record.Steps[0].Status = workflow.StepApplied
		record.Steps[1].Status = workflow.StepApplied
		record.Steps[2].Status = workflow.StepApplied
		farmResult := &contractspb.CommitPlantResult{
			FarmId: lookupTestSubject, PlotId: 3, CropCycleId: fixture.root.CropCycleID,
			CropDefinitionId:       fixture.root.Payload.GetCropRuleSnapshot().GetCropDefinitionId(),
			AppliedCropRuleVersion: fixture.root.Payload.GetCropRuleSnapshot().GetCropRuleVersion(),
			CommittedPlotVersion:   fixture.root.Payload.GetExpectedPlotVersion() + 1,
			CommittedFarmVersion:   88,
		}
		resultBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(farmResult)
		if err != nil {
			t.Fatal(err)
		}
		record.Steps[1].Result = resultBytes
		spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT)
		if err != nil {
			t.Fatal(err)
		}
		record.Steps[1].ResultHash, err = contracts.CanonicalParticipantHash(spec, farmResult)
		if err != nil {
			t.Fatal(err)
		}
	case workflow.WorkStateFailed:
		record.LastErrorClass = "DEADLINE_EXCEEDED"
	default:
		record.CurrentStep = 0
	}
	updated, err := fixture.repo.Advance(context.Background(), record, workflow.AdvanceOptions{ExpectedWorkVersion: record.WorkVersion})
	if err != nil {
		t.Fatal(err)
	}
	return updated
}

func TestOperationResultReaderMapsPlantStatesAndDoesNotMutateWorkRecord(t *testing.T) {
	tests := []struct {
		name          string
		state         workflow.WorkState
		lookup        publicpb.OperationLookupStatus
		wantRetry     uint32
		wantStatus    commonpb.CommandStatus
		wantErrorCode commonpb.ErrorCode
	}{
		{name: "created", state: workflow.WorkStateCreated, lookup: publicpb.OperationLookupStatus_OPERATION_LOOKUP_PROCESSING, wantRetry: 250},
		{name: "running", state: workflow.WorkStateRunning, lookup: publicpb.OperationLookupStatus_OPERATION_LOOKUP_PROCESSING, wantRetry: 250},
		{name: "step-unknown", state: workflow.WorkStateStepUnknown, lookup: publicpb.OperationLookupStatus_OPERATION_LOOKUP_PROCESSING, wantRetry: 250},
		{name: "completed", state: workflow.WorkStateCompleted, lookup: publicpb.OperationLookupStatus_OPERATION_LOOKUP_COMPLETED, wantStatus: commonpb.CommandStatus_SUCCEEDED},
		{name: "rejected", state: workflow.WorkStateRejected, lookup: publicpb.OperationLookupStatus_OPERATION_LOOKUP_COMPLETED, wantStatus: commonpb.CommandStatus_REJECTED, wantErrorCode: commonpb.ErrorCode_ERR_INSUFFICIENT_ITEM},
		{name: "compensated", state: workflow.WorkStateCompensated, lookup: publicpb.OperationLookupStatus_OPERATION_LOOKUP_COMPLETED, wantStatus: commonpb.CommandStatus_REJECTED, wantErrorCode: commonpb.ErrorCode_ERR_PLOT_NOT_EMPTY},
		{name: "expired", state: workflow.WorkStateFailed, lookup: publicpb.OperationLookupStatus_OPERATION_LOOKUP_EXPIRED},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newLookupFixture(t, "plant-"+test.name)
			updateLookupRecord(t, fixture, test.state)
			before, err := fixture.repo.Get(context.Background(), string(identity.WorkTypePlant), fixture.root.WorkflowID)
			if err != nil {
				t.Fatal(err)
			}
			got, err := fixture.reader.Lookup(context.Background(), lookupTestSubject, plantLookupRequest(fixture.operation, fixture.targetFarm))
			if err != nil {
				t.Fatal(err)
			}
			if got.LookupStatus != test.lookup || got.OriginalOperationId != fixture.operation || got.OriginalCommandType != commonpb.CommandType_PLANT {
				t.Fatalf("result=%v", got)
			}
			if got.RetryAfterMs != test.wantRetry {
				t.Fatalf("retry_after_ms=%d want %d", got.RetryAfterMs, test.wantRetry)
			}
			if test.wantStatus != commonpb.CommandStatus_COMMAND_STATUS_UNSPECIFIED {
				if got.CompletedOutcome == nil || got.CompletedOutcome.Status != test.wantStatus {
					t.Fatalf("completed outcome=%v", got.CompletedOutcome)
				}
				if test.wantErrorCode != commonpb.ErrorCode_ERROR_CODE_UNSPECIFIED &&
					(got.CompletedOutcome.Error == nil || got.CompletedOutcome.Error.ErrorCode != test.wantErrorCode) {
					t.Fatalf("completed error=%v", got.CompletedOutcome.Error)
				}
			}
			if test.state == workflow.WorkStateCompleted {
				var plantResult publicpb.PlantResult
				if err := proto.Unmarshal(got.CompletedOutcome.ResultPayload, &plantResult); err != nil {
					t.Fatal(err)
				}
				if plantResult.TargetFarmId != fixture.targetFarm || plantResult.PlotId != 2 ||
					plantResult.CropCycleId != fixture.root.CropCycleID || plantResult.CommittedFarmVersion != 88 {
					t.Fatalf("PlantResult=%v", &plantResult)
				}
			}
			after, err := fixture.repo.Get(context.Background(), string(identity.WorkTypePlant), fixture.root.WorkflowID)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("lookup mutated WorkRecord before=%+v after=%+v", before, after)
			}
		})
	}
}

func TestOperationResultReaderFailedWithoutBusinessOutcomeReturnsUnavailableTerminal(t *testing.T) {
	fixture := newLookupFixture(t, "plant-failed-definition-drift")
	record, err := fixture.repo.Get(context.Background(), string(identity.WorkTypePlant), fixture.root.WorkflowID)
	if err != nil {
		t.Fatal(err)
	}
	record.State = workflow.WorkStateFailed
	record.LastErrorClass = "DEFINITION_DRIFT"
	before, err := fixture.repo.Advance(context.Background(), record, workflow.AdvanceOptions{ExpectedWorkVersion: record.WorkVersion})
	if err != nil {
		t.Fatal(err)
	}
	got, err := fixture.reader.Lookup(context.Background(), lookupTestSubject, plantLookupRequest(fixture.operation, fixture.targetFarm))
	if err != nil {
		t.Fatal(err)
	}
	if got.LookupStatus != publicpb.OperationLookupStatus_OPERATION_LOOKUP_COMPLETED ||
		got.CompletedRetryDirective != commonpb.RetryDirective_REFRESH_AUTHORITY_STATE ||
		got.CompletedOutcome == nil || got.CompletedOutcome.Status != commonpb.CommandStatus_UNAVAILABLE ||
		got.CompletedOutcome.GetError().GetErrorCode() != commonpb.ErrorCode_ERR_INTERNAL {
		t.Fatalf("failed lookup=%v", got)
	}
	after, err := fixture.repo.Get(context.Background(), string(identity.WorkTypePlant), fixture.root.WorkflowID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("lookup mutated failed WorkRecord before=%+v after=%+v", before, after)
	}
}

func TestOperationResultReaderProcessingLeaseAndRepeatedPollingAreReadOnly(t *testing.T) {
	fixture := newLookupFixture(t, "plant-processing-retry")
	claimed, err := fixture.repo.ClaimDue(context.Background(), "worker-a", "token-a", 1, time.Minute)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim=%v err=%v", claimed, err)
	}
	before, err := fixture.repo.Get(context.Background(), string(identity.WorkTypePlant), fixture.root.WorkflowID)
	if err != nil {
		t.Fatal(err)
	}
	first, err := fixture.reader.Lookup(context.Background(), lookupTestSubject, plantLookupRequest(fixture.operation, fixture.targetFarm))
	if err != nil {
		t.Fatal(err)
	}
	second, err := fixture.reader.Lookup(context.Background(), lookupTestSubject, plantLookupRequest(fixture.operation, fixture.targetFarm))
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(first, second) || first.LookupStatus != publicpb.OperationLookupStatus_OPERATION_LOOKUP_PROCESSING || first.RetryAfterMs == 0 || first.RetryAfterMs > operationResultMaxRetryAfterMs {
		t.Fatalf("poll results first=%v second=%v", first, second)
	}
	after, err := fixture.repo.Get(context.Background(), string(identity.WorkTypePlant), fixture.root.WorkflowID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) || before.WorkVersion == 0 || before.LeaseOwner != "worker-a" || before.LeaseTokenHash == "" {
		t.Fatalf("read changed version/state/lease before=%+v after=%+v", before, after)
	}
}

func TestOperationResultReaderNotFoundIsolationLocatorAndMalformedID(t *testing.T) {
	fixture := newLookupFixture(t, "plant-isolated")
	notFound, err := fixture.reader.Lookup(context.Background(), lookupTestSubject, plantLookupRequest("plant-missing", fixture.targetFarm))
	if err != nil || notFound.LookupStatus != publicpb.OperationLookupStatus_OPERATION_LOOKUP_NOT_FOUND {
		t.Fatalf("missing=%v err=%v", notFound, err)
	}
	wrongSubject, err := fixture.reader.Lookup(context.Background(), lookupTestSubject+1, plantLookupRequest(fixture.operation, fixture.targetFarm))
	if err != nil || wrongSubject.LookupStatus != publicpb.OperationLookupStatus_OPERATION_LOOKUP_NOT_FOUND {
		t.Fatalf("wrong subject=%v err=%v", wrongSubject, err)
	}
	wrongTarget, err := fixture.reader.Lookup(context.Background(), lookupTestSubject, plantLookupRequest(fixture.operation, fixture.targetFarm+1))
	if err != nil || wrongTarget.LookupStatus != publicpb.OperationLookupStatus_OPERATION_LOOKUP_NOT_FOUND {
		t.Fatalf("wrong target=%v err=%v", wrongTarget, err)
	}
	malformed := plantLookupRequest("bad operation", fixture.targetFarm)
	if _, err := fixture.reader.Lookup(context.Background(), lookupTestSubject, malformed); !errors.Is(err, ErrOperationResultMalformedOperationID) {
		t.Fatalf("malformed operation err=%v", err)
	}
	claim, err := fixture.reader.Lookup(context.Background(), lookupTestSubject, &publicpb.GetOperationResultRequest{
		OriginalCommandType: commonpb.CommandType_CLAIM_TASK, OriginalOperationId: fixture.operation,
	})
	if err != nil || claim.GetLookupStatus() != publicpb.OperationLookupStatus_OPERATION_LOOKUP_NOT_FOUND ||
		claim.GetOriginalCommandType() != commonpb.CommandType_CLAIM_TASK {
		t.Fatalf("claim retry lookup=%v err=%v", claim, err)
	}
	unsupported := plantLookupRequest(fixture.operation, fixture.targetFarm)
	unsupported.OriginalCommandType = commonpb.CommandType_BUY
	if _, err := fixture.reader.Lookup(context.Background(), lookupTestSubject, unsupported); !errors.Is(err, ErrOperationResultUnsupportedCommand) {
		t.Fatalf("unsupported command err=%v", err)
	}
	if _, err := fixture.reader.Lookup(context.Background(), lookupTestSubject, &publicpb.GetOperationResultRequest{
		OriginalCommandType: commonpb.CommandType_PLANT, OriginalOperationId: fixture.operation,
		OriginalTarget: &publicpb.OperationTargetLocator{Target: &publicpb.OperationTargetLocator_PeerPlayerId{PeerPlayerId: 22}},
	}); !errors.Is(err, ErrOperationResultInvalidLocator) {
		t.Fatalf("wrong locator kind err=%v", err)
	}
}

type timeoutOperationRepository struct {
	workflow.WorkRepository
	err error
}

func (r timeoutOperationRepository) Get(context.Context, string, string) (*workflow.WorkRecord, error) {
	return nil, r.err
}

func TestOperationResultReaderRepositoryTimeoutAndServiceTrustedSubject(t *testing.T) {
	fixture := newLookupFixture(t, "plant-timeout")
	timeoutReader, err := NewOperationResultReader(OperationResultReaderConfig{
		Repository:     timeoutOperationRepository{err: context.DeadlineExceeded},
		BucketResolver: fixture.resolver, BucketVersion: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := timeoutReader.Lookup(context.Background(), lookupTestSubject, plantLookupRequest(fixture.operation, fixture.targetFarm)); !errors.Is(err, ErrOperationResultRepositoryUnavailable) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout err=%v", err)
	}

	updateLookupRecord(t, fixture, workflow.WorkStateCreated)
	service := NewService(nil)
	service.SetOperationResultReader(fixture.reader)
	requestPayload, err := proto.Marshal(plantLookupRequest(fixture.operation, fixture.targetFarm))
	if err != nil {
		t.Fatal(err)
	}
	response, err := service.ExecuteCommand(context.Background(), &logicrpcpb.ExecuteCommandRequest{
		Command: &commonpb.ClientCommandEnvelope{
			RequestId: "query-1", CommandType: commonpb.CommandType_GET_OPERATION_RESULT, Payload: requestPayload,
		},
		Trusted: &logicrpcpb.TrustedCommandContext{
			Session:                     &logicrpcpb.SessionPermitSnapshot{PlayerId: lookupTestSubject},
			RecoveryCompleteAtAdmission: false,
		},
		Dispatch: &logicrpcpb.DispatchContext{RouteMode: logicrpcpb.RouteMode_ROUTE_MODE_SERVICE_LB},
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetBusiness().Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("query outcome=%v", response.GetBusiness())
	}
	var query publicpb.GetOperationResultResult
	if err := proto.Unmarshal(response.GetBusiness().ResultPayload, &query); err != nil {
		t.Fatal(err)
	}
	if query.LookupStatus != publicpb.OperationLookupStatus_OPERATION_LOOKUP_PROCESSING || query.OriginalOperationId != fixture.operation {
		t.Fatalf("query=%v", &query)
	}
}

func TestOperationResultTheftLocatorUsesFriendPairWorkflowRoot(t *testing.T) {
	reader, err := NewOperationResultReader(OperationResultReaderConfig{
		Repository: workflow.NewMemoryWorkRepository(time.Now), BucketResolver: lookupTestBucketResolver{}, BucketVersion: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	const visitor, owner = uint64(30003), uint64(20002)
	pairID, err := identity.CanonicalFriendRelationID(visitor, owner)
	if err != nil {
		t.Fatal(err)
	}
	workflowID, address, err := reader.operationAddress(visitor, "theft-result-root", owner, string(identity.WorkTypeTheft))
	if err != nil {
		t.Fatal(err)
	}
	want, err := identity.WorkflowID(identity.EffectiveIdempotencyKey{
		SubjectPlayerID:       visitor,
		StableCommandTypeName: string(identity.WorkTypeTheft),
		OperationID:           "theft-result-root",
		Root:                  identity.OperationRootRef{Kind: identity.RootKindFriendPair, CanonicalID: pairID},
	})
	if err != nil {
		t.Fatal(err)
	}
	if workflowID != want || address.WorkID != want || address.WorkType != string(identity.WorkTypeTheft) {
		t.Fatalf("workflow=%s address=%+v want=%s", workflowID, address, want)
	}
}

func TestOperationResultTheftAdmissionCrashGapIsProcessingThenTerminal(t *testing.T) {
	server := miniredis.RunT(t)
	now := time.UnixMilli(1_800_000_000_000)
	server.SetTime(now)
	client := redis.NewClient(&redis.Options{Addr: server.Addr(), PoolSize: 16})
	t.Cleanup(func() { _ = client.Close() })
	namespace, err := rediskey.NewNamespace("test", "theft-result-gap")
	if err != nil {
		t.Fatal(err)
	}
	profile := friend.DefaultFriendRedisProfile(namespace)
	profile.PairBucketCount, profile.QuotaBucketCount, profile.ShareBucketCount = 8, 8, 8
	repositories, err := friend.NewRedisFriendshipRepositories(client, friend.RedisFriendshipOptions{Profile: profile})
	if err != nil {
		t.Fatal(err)
	}
	const visitor, owner = uint64(31_001), uint64(32_002)
	pairID, err := friend.RelationIDFor(visitor, owner)
	if err != nil {
		t.Fatal(err)
	}
	activateHash := sha256.Sum256([]byte("theft-result-gap-activate"))
	activated, err := repositories.Pair.ActivateFriendRelation(context.Background(), friend.ActivateCommand{
		InviterPlayerID: visitor, RedeemerPlayerID: owner, TargetRelationEpoch: 1,
		TokenID: "theft-result-gap-token", TokenGeneration: 1, OperationID: "theft-result-gap-activate",
	}, friend.CommitIdentity{WorkAddressID: pairID, ParticipantInvocationID: "theft-result-gap-activate", ParticipantStep: "ACTIVATE"}, activateHash[:])
	if err != nil || activated.Status != friend.StatusSucceeded {
		t.Fatalf("activate=%+v err=%v", activated, err)
	}
	payloadHash := sha256.Sum256([]byte("theft-result-gap-payload"))
	targetHash := sha256.Sum256([]byte("theft-result-gap-target"))
	const operationID = "theft-result-gap-operation"
	command := friend.ActionAdmissionCommand{Identity: friend.CommitIdentity{SubjectPlayerID: visitor,
		CommandType: "THEFT", OperationID: operationID}, PayloadHash: payloadHash[:], VisitorPlayerID: visitor,
		OwnerPlayerID: owner, TargetFarmID: owner, Scope: friend.ActionScopeTheft, TargetBindingHash: targetHash[:],
		PermitTTLMs: 100, WorkflowTTLMs: 60_000}
	admission, err := repositories.Pair.CommitFriendActionAdmission(context.Background(), command)
	if err != nil || admission.Outcome.Status != friend.StatusSucceeded {
		t.Fatalf("admission=%+v err=%v", admission, err)
	}
	repo := workflow.NewMemoryWorkRepository(func() time.Time { return now })
	resolver := lookupTestBucketResolver{}
	reader, err := NewOperationResultReader(OperationResultReaderConfig{Repository: repo, BucketResolver: resolver,
		BucketVersion: 1, TheftAdmissionLookup: repositories, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	request := &publicpb.GetOperationResultRequest{OriginalCommandType: commonpb.CommandType_THEFT,
		OriginalOperationId: operationID, OriginalTarget: &publicpb.OperationTargetLocator{
			Target: &publicpb.OperationTargetLocator_TargetFarmId{TargetFarmId: owner}}}
	processing, err := reader.Lookup(context.Background(), visitor, request)
	if err != nil || processing.LookupStatus != publicpb.OperationLookupStatus_OPERATION_LOOKUP_PROCESSING {
		t.Fatalf("query after admission before WorkRecord=%v err=%v", processing, err)
	}
	conflicting := command
	conflictingHash := sha256.Sum256([]byte("different-payload"))
	conflicting.PayloadHash = conflictingHash[:]
	if _, err := repositories.Pair.CommitFriendActionAdmission(context.Background(), conflicting); !errors.Is(err, friend.ErrIdempotentConflict) {
		t.Fatalf("same operation different payload err=%v", err)
	}
	cycleID, err := workidentity.StableOpaqueID("theft-result-gap-cycle", "one")
	if err != nil {
		t.Fatal(err)
	}
	permit := admission.Permit
	root, err := theftscenario.BuildRoot(theftscenario.Input{VisitorPlayerID: visitor, OwnerPlayerID: owner,
		TargetFarmID: owner, PlotID: 3, CropCycleID: cycleID, OperationID: operationID,
		ActionPermitSnapshot: &contractspb.ActionPermitSnapshot{PermitId: permit.PermitID,
			VisitorPlayerId: permit.VisitorPlayerID, OwnerPlayerId: permit.OwnerPlayerID, TargetFarmId: permit.TargetFarmID,
			OperationId: permit.OperationID, ActionScope: contractspb.FriendActionScope_FRIEND_ACTION_SCOPE_THEFT,
			FriendRelationEpoch: permit.RelationEpoch, CanonicalPayloadHash: permit.PayloadHash,
			TargetBindingHash: permit.TargetBindingHash, IssuedAtAuthorityMs: permit.IssuedAtAuthorityMs,
			ExpireAtAuthorityMs: permit.ExpireAtAuthorityMs, ExecutionAcceptedAtAuthorityMs: permit.ExecutionAcceptedAtAuthorityMs},
		AdmissionHash: targetHash[:], WorkflowDeadlineAtMs: admission.WorkflowDeadlineAtMs})
	if err != nil {
		t.Fatal(err)
	}
	address, err := root.AddressForResolver(1, resolver)
	if err != nil {
		t.Fatal(err)
	}
	record, err := workflow.NewWorkRecord(address, visitor, operationID, root.PayloadBytes, root.PayloadHash,
		[]string{string(identity.StepDecideTheft), string(identity.StepSettleTheftVisitor), string(identity.StepCreditGuardIncome)},
		now, time.UnixMilli(admission.WorkflowDeadlineAtMs))
	if err != nil {
		t.Fatal(err)
	}
	record, created, err := repo.CreateIfAbsent(context.Background(), record)
	if err != nil || !created {
		t.Fatalf("recovery create record=%v created=%v err=%v", record, created, err)
	}
	record.State = workflow.WorkStateRejected
	record.Steps[0].Status = workflow.StepRejected
	record.Steps[0].ErrorClass = "ERR_CROP_NOT_MATURE"
	record, err = repo.Advance(context.Background(), record, workflow.AdvanceOptions{ExpectedWorkVersion: record.WorkVersion})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := reader.Lookup(context.Background(), visitor, request)
	if err != nil || terminal.LookupStatus != publicpb.OperationLookupStatus_OPERATION_LOOKUP_COMPLETED ||
		terminal.CompletedOutcome.GetStatus() != commonpb.CommandStatus_REJECTED {
		t.Fatalf("terminal query=%v record=%v err=%v", terminal, record, err)
	}
}

func TestOperationResultTheftRejectedBeforeWorkRecoversFromPair(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	namespace, err := rediskey.NewNamespace("test", "theft-result-rejected-pair")
	if err != nil {
		t.Fatal(err)
	}
	profile := friend.DefaultFriendRedisProfile(namespace)
	profile.PairBucketCount, profile.QuotaBucketCount, profile.ShareBucketCount = 1, 1, 1
	repositories, err := friend.NewRedisFriendshipRepositories(client, friend.RedisFriendshipOptions{Profile: profile})
	if err != nil {
		t.Fatal(err)
	}
	const visitor, owner = uint64(41_001), uint64(42_002)
	pairID, err := friend.RelationIDFor(visitor, owner)
	if err != nil {
		t.Fatal(err)
	}
	activationHash := sha256.Sum256([]byte("rejected-pair-activate"))
	if outcome, err := repositories.Pair.ActivateFriendRelation(context.Background(), friend.ActivateCommand{
		InviterPlayerID: visitor, RedeemerPlayerID: owner, TargetRelationEpoch: 1, TokenID: "rejected-pair-token",
		TokenGeneration: 1, OperationID: "rejected-pair-activate",
	}, friend.CommitIdentity{WorkAddressID: pairID, ParticipantInvocationID: "rejected-pair-activate", ParticipantStep: "ACTIVATE"}, activationHash[:]); err != nil || outcome.Status != friend.StatusSucceeded {
		t.Fatalf("activate=%+v err=%v", outcome, err)
	}
	blockHash := sha256.Sum256([]byte("rejected-pair-block"))
	if outcome, err := repositories.Pair.BlockPlayer(context.Background(), friend.PairMutationCommand{
		Identity:    friend.CommitIdentity{SubjectPlayerID: visitor, CommandType: "BLOCK_PLAYER", OperationID: "rejected-pair-block"},
		PayloadHash: blockHash[:], CallerPlayerID: visitor, PeerPlayerID: owner,
	}); err != nil || outcome.Status != friend.StatusSucceeded {
		t.Fatalf("block=%+v err=%v", outcome, err)
	}
	payloadHash := sha256.Sum256([]byte("rejected-pair-payload"))
	targetHash := sha256.Sum256([]byte("rejected-pair-target"))
	const operationID = "rejected-pair-operation"
	admission, err := repositories.Pair.CommitFriendActionAdmission(context.Background(), friend.ActionAdmissionCommand{
		Identity:    friend.CommitIdentity{SubjectPlayerID: visitor, CommandType: "THEFT", OperationID: operationID},
		PayloadHash: payloadHash[:], VisitorPlayerID: visitor, OwnerPlayerID: owner, TargetFarmID: owner,
		Scope: friend.ActionScopeTheft, TargetBindingHash: targetHash[:], PermitTTLMs: 100, WorkflowTTLMs: 60_000,
	})
	if err != nil || admission.Outcome.Status != friend.StatusRejected || admission.DispatchOrderID != "" {
		t.Fatalf("admission=%+v err=%v", admission, err)
	}
	reader, err := NewOperationResultReader(OperationResultReaderConfig{
		Repository: workflow.NewMemoryWorkRepository(time.Now), BucketResolver: lookupTestBucketResolver{},
		BucketVersion: 1, TheftAdmissionLookup: repositories,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := &publicpb.GetOperationResultRequest{OriginalCommandType: commonpb.CommandType_THEFT,
		OriginalOperationId: operationID, OriginalTarget: &publicpb.OperationTargetLocator{
			Target: &publicpb.OperationTargetLocator_TargetFarmId{TargetFarmId: owner}}}
	result, err := reader.Lookup(context.Background(), visitor, request)
	if err != nil || result.GetLookupStatus() != publicpb.OperationLookupStatus_OPERATION_LOOKUP_COMPLETED ||
		result.GetCompletedOutcome().GetStatus() != commonpb.CommandStatus_REJECTED ||
		result.GetCompletedOutcome().GetError().GetErrorCode() != commonpb.ErrorCode_ERR_FRIEND_PERMIT_INVALID {
		t.Fatalf("rejected lookup=%v err=%v", result, err)
	}
}

type staticResultDirectory struct {
	entry redisauthority.ResultEntry
	found bool
	err   error
}

func (d staticResultDirectory) LookupResult(context.Context, uint64, string) (redisauthority.ResultEntry, bool, error) {
	return d.entry, d.found, d.err
}

func TestOperationResultDirectoryTerminalHitAndProjectionLossFallback(t *testing.T) {
	fixture := newLookupFixture(t, "plant-projection-fallback")
	updateLookupRecord(t, fixture, workflow.WorkStateCompleted)
	reader, err := NewOperationResultReader(OperationResultReaderConfig{
		Repository: fixture.repo, BucketResolver: fixture.resolver, BucketVersion: 1,
		ResultDirectory: staticResultDirectory{found: false},
	})
	if err != nil {
		t.Fatal(err)
	}
	fromWork, err := reader.Lookup(context.Background(), lookupTestSubject, plantLookupRequest(fixture.operation, fixture.targetFarm))
	if err != nil || fromWork.GetCompletedOutcome().GetStatus() != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("projection miss fallback=%v err=%v", fromWork, err)
	}

	const visitor, owner = uint64(51_001), uint64(52_002)
	const operationID = "theft-projection-hit"
	baseReader, err := NewOperationResultReader(OperationResultReaderConfig{
		Repository:     timeoutOperationRepository{err: errors.New("Work must not be read")},
		BucketResolver: lookupTestBucketResolver{}, BucketVersion: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	digest, rootKind, rootID, err := operationResultDigest(visitor, operationID, owner, string(identity.WorkTypeTheft))
	if err != nil {
		t.Fatal(err)
	}
	workflowID, address, err := baseReader.operationAddress(visitor, operationID, owner, string(identity.WorkTypeTheft))
	if err != nil || workflowID == "" {
		t.Fatal(err)
	}
	outcome := &publicpb.StoredOperationOutcome{OriginalCommandType: commonpb.CommandType_THEFT,
		OriginalOperationId: operationID, Status: commonpb.CommandStatus_NO_CHANGE,
		FarmContext: &publicpb.StoredFarmOutcomeContext{TargetFarmId: owner}}
	outcomeBytes, _ := proto.MarshalOptions{Deterministic: true}.Marshal(outcome)
	payloadHash := sha256.Sum256([]byte("theft-projection-payload"))
	bound, err := redisauthority.EncodeResultBinding(redisauthority.ResultBinding{SubjectPlayerID: visitor,
		CommandType: "THEFT", OperationID: operationID, PayloadHash: payloadHash[:], Outcome: outcomeBytes})
	if err != nil {
		t.Fatal(err)
	}
	entry := redisauthority.ResultEntry{CommandType: "THEFT", OperationDigest: digest, RootKind: string(rootKind),
		RootID: rootID, SourceRef: operationResultWorkSource(address), FinalStatusClass: "NO_CHANGE", MinimalOutcome: bound}
	reader, err = NewOperationResultReader(OperationResultReaderConfig{
		Repository:     timeoutOperationRepository{err: errors.New("Work must not be read")},
		BucketResolver: lookupTestBucketResolver{}, BucketVersion: 1,
		ResultDirectory: staticResultDirectory{entry: entry, found: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := &publicpb.GetOperationResultRequest{OriginalCommandType: commonpb.CommandType_THEFT,
		OriginalOperationId: operationID, OriginalTarget: &publicpb.OperationTargetLocator{
			Target: &publicpb.OperationTargetLocator_TargetFarmId{TargetFarmId: owner}}}
	projected, err := reader.Lookup(context.Background(), visitor, request)
	if err != nil || projected.GetCompletedOutcome().GetStatus() != commonpb.CommandStatus_NO_CHANGE {
		t.Fatalf("projection hit=%v err=%v", projected, err)
	}
}

func TestOperationResultReaderRetryAfterIsBounded(t *testing.T) {
	fixture := newLookupFixture(t, "plant-bounded-retry")
	reader, err := NewOperationResultReader(OperationResultReaderConfig{
		Repository: fixture.repo, BucketResolver: fixture.resolver, BucketVersion: 1,
		RetryAfter: 2 * time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := reader.Lookup(context.Background(), lookupTestSubject, plantLookupRequest(fixture.operation, fixture.targetFarm))
	if err != nil {
		t.Fatal(err)
	}
	if got.LookupStatus != publicpb.OperationLookupStatus_OPERATION_LOOKUP_PROCESSING || got.RetryAfterMs != operationResultMaxRetryAfterMs {
		t.Fatalf("bounded result=%v", got)
	}
}

func TestOperationResultReaderPreservesOriginalOperationIDInTerminalOutcome(t *testing.T) {
	fixture := newLookupFixture(t, "plant-original-id")
	updateLookupRecord(t, fixture, workflow.WorkStateCompleted)
	got, err := fixture.reader.Lookup(context.Background(), lookupTestSubject, plantLookupRequest(fixture.operation, fixture.targetFarm))
	if err != nil {
		t.Fatal(err)
	}
	if got.CompletedOutcome == nil || got.CompletedOutcome.OriginalOperationId != fixture.operation ||
		got.CompletedOutcome.OriginalCommandType != commonpb.CommandType_PLANT ||
		!bytes.Contains(got.CompletedOutcome.ResultPayload, []byte(fixture.root.CropCycleID[:4])) {
		// The final assertion is intentionally only a byte-presence check for
		// the stable crop-cycle identity; the typed payload is decoded above in
		// the state matrix test.
		t.Fatalf("terminal identity/result=%v", got.CompletedOutcome)
	}
}
