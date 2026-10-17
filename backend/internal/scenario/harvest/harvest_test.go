package harvest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	contractspb "farmgame/api/gen/contracts"
	"farmgame/internal/contracts"
	"farmgame/internal/identity"
	"farmgame/internal/workflow"
	"farmgame/internal/workidentity"

	"google.golang.org/protobuf/proto"
)

type harvestFixture struct {
	now         time.Time
	input       HarvestInput
	repo        *workflow.MemoryWorkRepository
	coordinator *Coordinator
	farm        *scriptedParticipant
	player      *scriptedParticipant
	buckets     testBucketResolver
}

func newHarvestFixture(t *testing.T) *harvestFixture {
	t.Helper()
	f := &harvestFixture{
		now: time.UnixMilli(1_800_000_000_000),
		input: HarvestInput{
			SubjectPlayerID: 10001, OperationID: "op-harvest-0001", TargetFarmID: 10001,
			PlotID: 3, CropCycleID: testCycle(t, "cycle-1"), ExpectedPlotVersion: 7,
		},
		farm:   newScriptedParticipant(participantFarm),
		player: newScriptedParticipant(participantPlayer),
	}
	f.repo = workflow.NewMemoryWorkRepository(func() time.Time { return f.now })
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Repository: f.repo, Player: f.player, Farm: f.farm, BucketResolver: f.buckets,
		BucketVersion: 1, Now: func() time.Time { return f.now },
	})
	if err != nil {
		t.Fatal(err)
	}
	f.coordinator = coordinator
	return f
}

func TestHarvestCreateRootAndConflictBeforeEffects(t *testing.T) {
	f := newHarvestFixture(t)
	root, err := BuildRoot(f.input)
	if err != nil {
		t.Fatal(err)
	}
	want, err := identity.WorkflowID(root.Key)
	if err != nil || root.WorkflowID != want {
		t.Fatalf("workflow id=%q want=%q err=%v", root.WorkflowID, want, err)
	}
	record, created, err := f.coordinator.Create(context.Background(), f.input)
	if err != nil || !created || record.State != workflow.WorkStateCreated {
		t.Fatalf("create record=%v created=%v err=%v", record, created, err)
	}
	if len(f.farm.calls()) != 0 || len(f.player.calls()) != 0 {
		t.Fatal("Participant called before WorkRecord creation")
	}
	for i, step := range record.Steps {
		wantInvocation, err := identity.ParticipantInvocationID(identity.WorkTypeHarvest, root.WorkflowID, identity.ParticipantStep(step.Name), 1)
		if err != nil || step.InvocationID != wantInvocation {
			t.Fatalf("step %d invocation=%q want=%q err=%v", i, step.InvocationID, wantInvocation, err)
		}
	}
	loaded, created, err := f.coordinator.Create(context.Background(), f.input)
	if err != nil || created || loaded.WorkVersion != record.WorkVersion {
		t.Fatalf("replay load=%v created=%v err=%v", loaded, created, err)
	}
	changed := f.input
	changed.CropCycleID = testCycle(t, "cycle-other")
	if _, _, err := f.coordinator.Create(context.Background(), changed); !errors.Is(err, workflow.ErrIdempotencyConflict) {
		t.Fatalf("payload hash conflict err=%v", err)
	}
}

func TestHarvestSuccessBindsFarmResultAndPlayerDecision(t *testing.T) {
	f := newHarvestFixture(t)
	record, _, err := f.coordinator.Start(context.Background(), f.input)
	if err != nil {
		t.Fatal(err)
	}
	if record.State != workflow.WorkStateCompleted || OutcomeFor(record) != OutcomeSucceeded {
		t.Fatalf("state=%s outcome=%s", record.State, OutcomeFor(record))
	}
	if got := f.farm.stepNames(); fmt.Sprint(got) != "[COMMIT_HARVEST]" {
		t.Fatalf("Farm steps=%v", got)
	}
	if got := f.player.stepNames(); fmt.Sprint(got) != "[GRANT_HARVEST_REWARD]" {
		t.Fatalf("Player steps=%v", got)
	}
	farmCall := f.farm.calls()[0]
	playerCall := f.player.calls()[0]
	farmResult, farmHash, err := DecodeCommitHarvestResult(record)
	if err != nil {
		t.Fatal(err)
	}
	grantPayload := &contractspb.GrantHarvestRewardPayload{}
	if err := proto.Unmarshal(playerCall.Payload, grantPayload); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(grantPayload.GetCommitHarvestResultHash(), farmHash[:]) || playerCall.DependsOnResultHash != farmHash {
		t.Fatalf("Grant dependency=%x call=%x Farm=%x", grantPayload.GetCommitHarvestResultHash(), playerCall.DependsOnResultHash, farmHash)
	}
	if !proto.Equal(grantPayload.GetHarvestDecision(), farmResult.GetHarvestDecision()) {
		t.Fatal("Grant payload did not carry the frozen Farm decision")
	}
	if f.farm.applyCount(farmCall.InvocationID) != 1 || f.player.applyCount(playerCall.InvocationID) != 1 {
		t.Fatal("participant effect was duplicated")
	}
}

func TestHarvestFarmRejectionAndNoChangeDoNotCallPlayer(t *testing.T) {
	for _, outcome := range []workflow.StepOutcome{workflow.StepOutcomeRejected, workflow.StepOutcomeNoChange} {
		t.Run(outcomeName(outcome), func(t *testing.T) {
			f := newHarvestFixture(t)
			f.farm.outcome = outcome
			record, _, err := f.coordinator.Start(context.Background(), f.input)
			if err != nil {
				t.Fatal(err)
			}
			if record.State != workflow.WorkStateRejected || OutcomeFor(record) == OutcomeSucceeded {
				t.Fatalf("state=%s outcome=%s", record.State, OutcomeFor(record))
			}
			if len(f.player.calls()) != 0 {
				t.Fatalf("Player called for Farm %s", outcomeName(outcome))
			}
			if outcome == workflow.StepOutcomeNoChange && OutcomeFor(record) != OutcomeNoChange {
				t.Fatalf("NO_CHANGE mapped to %s", OutcomeFor(record))
			}
		})
	}
}

func TestHarvestFarmResponseUnknownReplaysSameInvocation(t *testing.T) {
	f := newHarvestFixture(t)
	f.farm.loseOnce = true
	record, _, err := f.coordinator.Start(context.Background(), f.input)
	if !errors.Is(err, workflow.ErrRetryScheduled) {
		t.Fatalf("start err=%v", err)
	}
	if record.State != workflow.WorkStateStepUnknown || record.Steps[0].Status != workflow.StepUnknown {
		t.Fatalf("unknown boundary record=%+v", record)
	}
	first := f.farm.calls()[0]
	f.now = time.UnixMilli(record.NextDueAtMs + 1)
	replayed, _, err := f.coordinator.Start(context.Background(), f.input)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.State != workflow.WorkStateCompleted || len(f.farm.calls()) != 2 || !f.farm.wasReplayed(first.InvocationID) {
		t.Fatalf("replay state=%s calls=%d replayed=%v", replayed.State, len(f.farm.calls()), f.farm.wasReplayed(first.InvocationID))
	}
	second := f.farm.calls()[1]
	if first.InvocationID != second.InvocationID || !bytes.Equal(first.Payload, second.Payload) || first.PayloadHash != second.PayloadHash {
		t.Fatal("Farm replay changed invocation or payload")
	}
}

func TestHarvestPlayerResponseUnknownAndTerminalReplay(t *testing.T) {
	f := newHarvestFixture(t)
	f.player.loseOnce = true
	record, _, err := f.coordinator.Start(context.Background(), f.input)
	if !errors.Is(err, workflow.ErrRetryScheduled) || record.State != workflow.WorkStateStepUnknown || record.CurrentStep != 1 {
		t.Fatalf("Player unknown boundary record=%+v err=%v", record, err)
	}
	f.now = time.UnixMilli(record.NextDueAtMs + 1)
	replayed, _, err := f.coordinator.Start(context.Background(), f.input)
	if err != nil || replayed.State != workflow.WorkStateCompleted {
		t.Fatalf("Player replay record=%v err=%v", replayed, err)
	}
	farmCalls, playerCalls := len(f.farm.calls()), len(f.player.calls())
	terminal, created, err := f.coordinator.Start(context.Background(), f.input)
	if err != nil || created || terminal.State != workflow.WorkStateCompleted {
		t.Fatalf("terminal replay=%v created=%v err=%v", terminal, created, err)
	}
	if len(f.farm.calls()) != farmCalls || len(f.player.calls()) != playerCalls {
		t.Fatal("terminal replay invoked a Participant")
	}
}

func TestHarvestPlayerReconcileRequiredRemainsRecoverable(t *testing.T) {
	f := newHarvestFixture(t)
	f.player.reconcileOnce = true
	record, _, err := f.coordinator.Start(context.Background(), f.input)
	if !errors.Is(err, workflow.ErrRetryScheduled) || record.State == workflow.WorkStateFailed ||
		record.State != workflow.WorkStateStepUnknown || record.Steps[1].Status != workflow.StepUnknown {
		t.Fatalf("reconcile result must remain recoverable: record=%+v err=%v", record, err)
	}
	first := f.player.calls()[0]
	f.now = time.UnixMilli(record.NextDueAtMs + 1)
	recovered, _, err := f.coordinator.Start(context.Background(), f.input)
	if err != nil || recovered.State != workflow.WorkStateCompleted {
		t.Fatalf("reconcile recovery record=%+v err=%v", recovered, err)
	}
	calls := f.player.calls()
	if len(calls) != 2 || calls[1].InvocationID != first.InvocationID || !bytes.Equal(calls[1].Payload, first.Payload) {
		t.Fatalf("reconcile recovery changed invocation/payload: first=%+v calls=%+v", first, calls)
	}
}

func TestHarvestPlayerIdempotencyConflictIsStableRejected(t *testing.T) {
	f := newHarvestFixture(t)
	f.player.outcome = workflow.StepOutcomeRejected
	f.player.rejectionErrorClass = ErrorClassParticipantIdempotencyConflict
	record, _, err := f.coordinator.Start(context.Background(), f.input)
	if err != nil || record.State != workflow.WorkStateRejected || OutcomeFor(record) != OutcomeRejected {
		t.Fatalf("idempotency conflict record=%+v outcome=%s err=%v", record, OutcomeFor(record), err)
	}
	if len(f.player.calls()) != 1 {
		t.Fatalf("idempotency conflict was retried: calls=%d", len(f.player.calls()))
	}
}

func TestHarvestCrashAfterFarmEffectBeforeCASIsRecovered(t *testing.T) {
	f := newHarvestFixture(t)
	f.farm.panicOnce = true
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected injected Farm crash")
			}
		}()
		_, _, _ = f.coordinator.Start(context.Background(), f.input)
	}()
	stored, err := f.repo.Get(context.Background(), string(identity.WorkTypeHarvest), mustRoot(t, f.input).WorkflowID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Steps[0].Status != workflow.StepPending || len(stored.Steps[0].Payload) == 0 {
		t.Fatalf("crash did not leave frozen Farm call=%+v", stored.Steps[0])
	}
	recovered, _, err := f.coordinator.Start(context.Background(), f.input)
	if err != nil || recovered.State != workflow.WorkStateCompleted {
		t.Fatalf("recovered=%v err=%v", recovered, err)
	}
	if f.farm.applyCount(stored.Steps[0].InvocationID) != 1 {
		t.Fatal("Farm effect duplicated after crash")
	}
}

func TestHarvestLeaseLossDoesNotAdvanceWork(t *testing.T) {
	f := newHarvestFixture(t)
	record, _, err := f.coordinator.Create(context.Background(), f.input)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := f.repo.ClaimDue(context.Background(), "worker-a", "token-a", 1, time.Minute)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim=%v err=%v", claimed, err)
	}
	if err := f.coordinator.Engine().Resume(context.Background(), claimed[0].Record, workflow.LeaseFence{Owner: "worker-b", Epoch: claimed[0].Record.LeaseEpoch}, nil); !errors.Is(err, workflow.ErrLeaseLost) {
		t.Fatalf("wrong lease err=%v", err)
	}
	stored, err := f.repo.Get(context.Background(), record.Address.WorkType, record.Address.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	if len(f.farm.calls()) != 0 || stored.Steps[0].Status != workflow.StepPending {
		t.Fatalf("lease loss changed work/effect: calls=%d step=%s", len(f.farm.calls()), stored.Steps[0].Status)
	}
}

func TestHarvestMalformedDecisionHashFailsClosed(t *testing.T) {
	f := newHarvestFixture(t)
	// Stop after the Farm effect so the non-terminal WorkRecord can be
	// inspected without allowing the Player step to complete.
	f.player.loseOnce = true
	record, _, err := f.coordinator.Start(context.Background(), f.input)
	if !errors.Is(err, workflow.ErrRetryScheduled) || record.CurrentStep != 1 {
		t.Fatalf("expected Player pending boundary record=%+v err=%v", record, err)
	}
	// Replace the durable Farm result with a deterministic, canonically hashed
	// result whose nested decision hash is wrong. The next resume must not call
	// Player and must fail closed rather than recompute a reward.
	stored, err := f.repo.Get(context.Background(), record.Address.WorkType, record.Address.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	result, _, err := DecodeCommitHarvestResult(stored)
	if err != nil {
		t.Fatal(err)
	}
	result.HarvestDecision.HarvestDecisionHash[0]++
	resultBytes, err := deterministicProto(result)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_HARVEST)
	if err != nil {
		t.Fatal(err)
	}
	resultHash, err := contracts.CanonicalParticipantHash(spec, result)
	if err != nil {
		t.Fatal(err)
	}
	stored.Steps[0].Result, stored.Steps[0].ResultHash = resultBytes, resultHash
	stored.CurrentStep, stored.State = 1, workflow.WorkStateStepUnknown
	if _, err := f.repo.Advance(context.Background(), stored, workflow.AdvanceOptions{ExpectedWorkVersion: stored.WorkVersion}); err != nil {
		t.Fatal(err)
	}
	current, err := f.repo.Get(context.Background(), record.Address.WorkType, record.Address.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.coordinator.Resume(context.Background(), current); !errors.Is(err, workflow.ErrDefinitionDrift) {
		t.Fatalf("malformed decision err=%v", err)
	}
	if len(f.player.calls()) != 1 { // only the original unknown call
		t.Fatalf("malformed result reached Player again: %d calls", len(f.player.calls()))
	}
	final, err := f.repo.Get(context.Background(), record.Address.WorkType, record.Address.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	if final.State != workflow.WorkStateFailed {
		t.Fatalf("malformed result state=%s", final.State)
	}
}

func mustRoot(t *testing.T, input HarvestInput) RootIntent {
	t.Helper()
	root, err := BuildRoot(input)
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func testCycle(t *testing.T, name string) string {
	t.Helper()
	cycle, err := workidentity.StableOpaqueID("harvest-test-cycle", name)
	if err != nil {
		t.Fatal(err)
	}
	return cycle
}

type testBucketResolver struct{}

func (testBucketResolver) IndependentWorkBucket(workID string, bucketVersion uint32) (uint32, error) {
	if bucketVersion == 0 || workID == "" {
		return 0, errors.New("invalid bucket request")
	}
	var bucket uint32
	for _, r := range workID {
		bucket = bucket*33 + uint32(r)
	}
	return bucket & 15, nil
}

func (testBucketResolver) AuthorityPersistenceBucket(workidentity.RouteDomain, uint64, uint32) (uint32, error) {
	return 0, errors.New("authority bucket is outside Harvest test")
}

const (
	participantFarm   = "farm"
	participantPlayer = "player"
)

type scriptedCall struct {
	StepName            string
	InvocationID        string
	Payload             []byte
	PayloadHash         [32]byte
	DependsOnResultHash [32]byte
}

type scriptedParticipant struct {
	mu                  sync.Mutex
	name                string
	outcome             workflow.StepOutcome
	loseOnce            bool
	reconcileOnce       bool
	rejectionErrorClass string
	panicOnce           bool
	store               map[string]workflow.StepResult
	callsMade           []scriptedCall
	replayed            map[string]bool
	apply               map[string]int
}

func newScriptedParticipant(name string) *scriptedParticipant {
	return &scriptedParticipant{name: name, store: make(map[string]workflow.StepResult), replayed: make(map[string]bool), apply: make(map[string]int)}
}

func (p *scriptedParticipant) Invoke(_ context.Context, call workflow.StepCall) (workflow.StepResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.callsMade = append(p.callsMade, scriptedCall{StepName: call.StepName, InvocationID: call.InvocationID, Payload: append([]byte(nil), call.Payload...), PayloadHash: call.PayloadHash, DependsOnResultHash: call.DependsOnResultHash})
	if result, ok := p.store[call.InvocationID]; ok {
		p.replayed[call.InvocationID] = true
		result.Replayed = true
		return result, nil
	}
	if p.reconcileOnce {
		p.reconcileOnce = false
		return workflow.StepResult{Outcome: workflow.StepOutcomeRejected, ErrorClass: ErrorClassReconcileRequired}, nil
	}
	result, err := p.buildResult(call)
	if err != nil {
		return workflow.StepResult{}, err
	}
	p.store[call.InvocationID] = result
	p.apply[call.InvocationID]++
	if p.panicOnce {
		p.panicOnce = false
		panic("injected participant crash after durable effect")
	}
	if p.loseOnce {
		p.loseOnce = false
		return workflow.StepResult{}, errors.New("injected response loss after durable effect")
	}
	return result, nil
}

func (p *scriptedParticipant) buildResult(call workflow.StepCall) (workflow.StepResult, error) {
	if p.outcome == workflow.StepOutcomeUnspecified {
		p.outcome = workflow.StepOutcomeApplied
	}
	if p.outcome == workflow.StepOutcomeRejected || p.outcome == workflow.StepOutcomeNoChange {
		errorClass := p.rejectionErrorClass
		if errorClass == "" {
			errorClass = "CROP_NOT_MATURE"
		}
		return workflow.StepResult{Outcome: p.outcome, ErrorClass: errorClass}, nil
	}
	var message proto.Message
	specCommand := contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_HARVEST
	if call.StepName == string(identity.StepCommitHarvest) {
		payload := &contractspb.CommitHarvestPayload{}
		if err := proto.Unmarshal(call.Payload, payload); err != nil {
			return workflow.StepResult{}, err
		}
		decision := &contractspb.HarvestDecision{
			WorkflowId:    call.Work.WorkID,
			OwnerPlayerId: payload.GetOwnerPlayerId(), FarmId: payload.GetFarmId(), PlotId: payload.GetPlotId(),
			CropCycleId: payload.GetCropCycleId(), CropDefinitionId: 7, RewardItemId: 702,
			RewardQuantity: 50, RewardExp: 10, CropRuleVersion: 42,
			CompendiumUnlockSnapshot: &contractspb.CompendiumUnlockSnapshot{CropDefinitionId: 7, UnlockEligible: true, PlantedByPlayerId: payload.GetOwnerPlayerId(), CatalogVersionAtPlant: 9, DefinitionVersionAtPlant: 11},
		}
		decision.HarvestDecisionHash = decisionHash(decision)
		message = &contractspb.CommitHarvestResult{HarvestDecision: decision, CommittedPlotVersion: payload.GetExpectedPlotVersion() + 1, CommittedFarmVersion: 8, FarmRecordVersion: 9}
	} else {
		specCommand = contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD
		payload := &contractspb.GrantHarvestRewardPayload{}
		if err := proto.Unmarshal(call.Payload, payload); err != nil {
			return workflow.StepResult{}, err
		}
		decision := payload.GetHarvestDecision()
		crop := decision.GetCropDefinitionId()
		message = &contractspb.GrantHarvestRewardResult{
			CropCycleId: decision.GetCropCycleId(), RewardItemId: decision.GetRewardItemId(), GrantedQuantity: decision.GetRewardQuantity(),
			InventoryQuantityAfter: 50, GrantedExp: decision.GetRewardExp(), PlayerLevelAfter: 2, HarvestCountAfter: 1,
			CompendiumUnlockDelta: &contractspb.CompendiumUnlockDelta{NewlyUnlocked: true, CropDefinitionId: &crop, CompendiumRevisionAfter: 3}, PlayerRecordVersion: 12,
		}
	}
	resultBytes, err := deterministicProto(message)
	if err != nil {
		return workflow.StepResult{}, err
	}
	spec, err := contracts.LookupParticipantCommand(specCommand)
	if err != nil {
		return workflow.StepResult{}, err
	}
	hash, err := contracts.CanonicalParticipantHash(spec, message)
	if err != nil {
		return workflow.StepResult{}, err
	}
	return workflow.StepResult{Outcome: p.outcome, Result: resultBytes, ResultHash: hash}, nil
}

func decisionHash(decision *contractspb.HarvestDecision) []byte {
	clone := proto.Clone(decision).(*contractspb.HarvestDecision)
	clone.HarvestDecisionHash = nil
	hash, err := contracts.CanonicalHash(clone)
	if err != nil {
		panic(err)
	}
	return hash[:]
}

func (p *scriptedParticipant) calls() []scriptedCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]scriptedCall(nil), p.callsMade...)
}

func (p *scriptedParticipant) stepNames() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := make([]string, 0, len(p.callsMade))
	for _, call := range p.callsMade {
		result = append(result, callStepName(call.StepName))
	}
	return result
}

func callStepName(step string) string { return step }

func (p *scriptedParticipant) applyCount(invocation string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.apply[invocation]
}

func (p *scriptedParticipant) wasReplayed(invocation string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.replayed[invocation]
}

func outcomeName(outcome workflow.StepOutcome) string {
	if outcome == workflow.StepOutcomeNoChange {
		return "NO_CHANGE"
	}
	return "REJECTED"
}
