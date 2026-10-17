package theft

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	contractspb "farmgame/api/gen/contracts"
	"farmgame/internal/contracts"
	"farmgame/internal/identity"
	"farmgame/internal/workflow"
	"farmgame/internal/workidentity"

	"google.golang.org/protobuf/proto"
)

type fixedBucketResolver struct{}

func (fixedBucketResolver) IndependentWorkBucket(string, uint32) (uint32, error) { return 7, nil }
func (fixedBucketResolver) AuthorityPersistenceBucket(workidentity.RouteDomain, uint64, uint32) (uint32, error) {
	return 7, nil
}

type theftFixture struct {
	t           *testing.T
	now         time.Time
	repo        workflow.WorkRepository
	coordinator *Coordinator
	visitor     *fakeParticipant
	owner       *fakeParticipant
	farm        *fakeParticipant
	input       TheftInput
	decision    decisionConfig
	record      *workflow.WorkRecord
	maxAttempts uint64
}

type decisionConfig struct {
	gross         uint64
	lost          uint64
	net           uint64
	coin          uint64
	guard         bool
	badDecision   bool
	badResultHash bool
}

func newTheftFixture(t *testing.T) *theftFixture {
	t.Helper()
	cycle, err := workidentity.StableOpaqueID("theft-test-cycle-v1", "cycle-1")
	if err != nil {
		t.Fatal(err)
	}
	permitID, err := workidentity.StableOpaqueID("theft-test-permit-v1", "permit-1")
	if err != nil {
		t.Fatal(err)
	}
	f := &theftFixture{
		t:   t,
		now: time.UnixMilli(1_800_000_000_000),
		input: TheftInput{
			VisitorPlayerID: 10001,
			OwnerPlayerID:   20002,
			TargetFarmID:    20002,
			PlotID:          3,
			CropCycleID:     cycle,
			OperationID:     "theft-op-0001",
			ActionPermitSnapshot: &contractspb.ActionPermitSnapshot{
				PermitId:                       permitID,
				VisitorPlayerId:                10001,
				OwnerPlayerId:                  20002,
				TargetFarmId:                   20002,
				OperationId:                    "theft-op-0001",
				ActionScope:                    contractspb.FriendActionScope_FRIEND_ACTION_SCOPE_THEFT,
				FriendRelationEpoch:            9,
				CanonicalPayloadHash:           bytes.Repeat([]byte{0x11}, sha256.Size),
				TargetBindingHash:              bytes.Repeat([]byte{0x22}, sha256.Size),
				IssuedAtAuthorityMs:            1_000,
				ExpireAtAuthorityMs:            2_000,
				ExecutionAcceptedAtAuthorityMs: 1_100,
			},
		},
		decision:    decisionConfig{gross: 10, lost: 0, net: 10, coin: 0, guard: false},
		maxAttempts: defaultMaxStepAttempts,
	}
	f.visitor = newFakeParticipant("visitor")
	f.owner = newFakeParticipant("owner")
	f.farm = newFakeParticipant("farm")
	f.installScripts()
	f.repo = workflow.NewMemoryWorkRepository(func() time.Time { return f.now })
	f.coordinator, err = NewCoordinator(CoordinatorConfig{
		Repository: f.repo, Visitor: f.visitor, Owner: f.owner, Farm: f.farm,
		BucketResolver: fixedBucketResolver{}, BucketVersion: 1,
		Now: func() time.Time { return f.now }, MaxStepAttempts: f.maxAttempts,
	})
	if err != nil {
		t.Fatal(err)
	}
	f.record, _, err = f.coordinator.Create(context.Background(), f.input)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *theftFixture) installScripts() {
	f.farm.script = func(call workflow.StepCall) (workflow.StepResult, error) {
		if f.decision.badResultHash {
			result := appliedTheftResult(f.t, call, f.decision)
			result.ResultHash[0]++
			return result, nil
		}
		return appliedTheftResult(f.t, call, f.decision), nil
	}
	f.visitor.script = func(call workflow.StepCall) (workflow.StepResult, error) {
		payload := &contractspb.SettleTheftVisitorPayload{}
		if err := proto.Unmarshal(call.Payload, payload); err != nil {
			return workflow.StepResult{}, err
		}
		decision := payload.GetTheftTargetDecision()
		return appliedParticipantResult(f.t,
			contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_SETTLE_THEFT_VISITOR,
			&contractspb.SettleTheftVisitorResult{
				SettlementId: decision.GetSettlementId(), RewardItemId: decision.GetRewardItemId(),
				GrantedNetQuantity: decision.GetNetReceivedQuantity(), PlayerRecordVersion: 301,
			}), nil
	}
	f.owner.script = func(call workflow.StepCall) (workflow.StepResult, error) {
		payload := &contractspb.CreditGuardIncomePayload{}
		if err := proto.Unmarshal(call.Payload, payload); err != nil {
			return workflow.StepResult{}, err
		}
		decision := payload.GetTheftTargetDecision()
		return appliedParticipantResult(f.t,
			contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_CREDIT_GUARD_INCOME,
			&contractspb.CreditGuardIncomeResult{
				SettlementId: decision.GetSettlementId(), CreditedNormalCoin: decision.GetGuardCoinValue(),
				NormalCoinBalanceAfter: decision.GetGuardCoinValue() + 100, PlayerRecordVersion: 401,
			}), nil
	}
}

func (f *theftFixture) runToTerminal(t *testing.T) {
	t.Helper()
	if err := f.coordinator.Resume(context.Background(), f.record); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if got := StateFor(f.record); got != StateCompleted && got != StateRejected && got != StateNoChange && got != StateReconcileRequired {
		t.Fatalf("not terminal: state=%s record=%+v", got, f.record)
	}
}

func (f *theftFixture) retry(t *testing.T) error {
	t.Helper()
	f.now = f.now.Add(time.Second)
	return f.coordinator.Resume(context.Background(), f.record)
}

type fakeReceipt struct {
	payloadHash [32]byte
	result      workflow.StepResult
}

type fakeParticipant struct {
	mu          sync.Mutex
	name        string
	script      func(workflow.StepCall) (workflow.StepResult, error)
	receipts    map[string]fakeReceipt
	calls       []workflow.StepCall
	loseFirst   bool
	lost        map[string]bool
	alwaysError error
	// staleOnce returns a stale WorkVersion proof error for the invocation on
	// its first call; subsequent calls execute the script normally.
	staleOnce map[string]bool
}

func newFakeParticipant(name string) *fakeParticipant {
	return &fakeParticipant{name: name, receipts: make(map[string]fakeReceipt), lost: make(map[string]bool), staleOnce: make(map[string]bool)}
}

func (p *fakeParticipant) Invoke(ctx context.Context, call workflow.StepCall) (workflow.StepResult, error) {
	if err := ctx.Err(); err != nil {
		return workflow.StepResult{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, call)
	if receipt, ok := p.receipts[call.InvocationID]; ok {
		if receipt.payloadHash != call.PayloadHash {
			return workflow.StepResult{Outcome: workflow.StepOutcomeRejected, ErrorClass: ErrorClassParticipantIdempotencyConflict}, nil
		}
		result := cloneStepResult(receipt.result)
		result.Replayed = true
		return result, nil
	}
	if p.alwaysError != nil {
		return workflow.StepResult{}, p.alwaysError
	}
	if p.staleOnce[call.InvocationID] {
		// Simulate the participant verifier rejecting the call because another
		// driver advanced the WorkRecord (stale WorkVersion proof). The engine
		// must reload and re-invoke, never classify as REJECTED. The RPC
		// adapter converts the verifier's FailedPrecondition into
		// workflow.ErrStaleWorkProof; the scenario fake speaks the engine
		// primitive directly.
		delete(p.staleOnce, call.InvocationID)
		return workflow.StepResult{}, workflow.ErrStaleWorkProof
	}
	if p.script == nil {
		return workflow.StepResult{}, errors.New("fake participant script is missing")
	}
	result, err := p.script(call)
	if err != nil {
		return workflow.StepResult{}, err
	}
	p.receipts[call.InvocationID] = fakeReceipt{payloadHash: call.PayloadHash, result: cloneStepResult(result)}
	if p.loseFirst && !p.lost[call.InvocationID] {
		p.lost[call.InvocationID] = true
		return workflow.StepResult{}, errors.New("response lost after participant receipt")
	}
	return result, nil
}

func (p *fakeParticipant) callsCopy() []workflow.StepCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := make([]workflow.StepCall, len(p.calls))
	copy(result, p.calls)
	return result
}

func cloneStepResult(result workflow.StepResult) workflow.StepResult {
	result.Result = append([]byte(nil), result.Result...)
	return result
}

func appliedTheftResult(t *testing.T, call workflow.StepCall, config decisionConfig) workflow.StepResult {
	t.Helper()
	payload := &contractspb.DecideTheftPayload{}
	if err := proto.Unmarshal(call.Payload, payload); err != nil {
		t.Fatal(err)
	}
	decision := &contractspb.TheftTargetDecision{
		WorkflowId: call.Work.WorkID, SettlementId: payload.GetSettlementId(), TheftAttemptId: payload.GetTheftAttemptId(),
		VisitorPlayerId: payload.GetVisitorPlayerId(), OwnerPlayerId: payload.GetOwnerPlayerId(), FarmId: payload.GetFarmId(),
		PlotId: payload.GetPlotId(), CropCycleId: payload.GetCropCycleId(), RewardItemId: 701,
		StealSlotIndex: 1, AppliedSlotRatioBp: 200, GrossStolenQuantity: config.gross,
		GuardLostQuantity: config.lost, NetReceivedQuantity: config.net, CropUnitCoinValue: 2,
		GuardCoinValue: config.coin, TheftOutcome: contractspb.TheftOutcome_THEFT_OUTCOME_STOLEN,
		CropRuleVersion: 42,
		GuardDecisionSnapshot: &contractspb.GuardDecisionSnapshot{
			TheftAttemptId: payload.GetTheftAttemptId(), GuardVersion: 3, GuardRuleVersion: 4,
			TriggerProbabilityBp: 5000, InterceptPolicy: contractspb.GuardInterceptPolicy_GUARD_INTERCEPT_POLICY_FULL_GROSS,
			RandomAlgorithmVersion: 1, RandomKeyVersion: 1, RandomDrawBp: 10,
			GuardTriggered: config.guard, GrossStolenQuantity: config.gross, GuardLostQuantity: config.lost,
			NetReceivedQuantity: config.net, CropUnitCoinValue: 2, GuardCoinValue: config.coin,
		},
	}
	if config.guard {
		decision.TheftOutcome = contractspb.TheftOutcome_THEFT_OUTCOME_BLOCKED_BY_GUARD
	}
	if config.badDecision {
		decision.FarmId++
	}
	want, err := contracts.CanonicalHash(decision)
	if err != nil {
		t.Fatal(err)
	}
	decision.TheftDecisionHash = append([]byte(nil), want[:]...)
	result := &contractspb.DecideTheftResult{TheftTargetDecision: decision, CommittedPlotVersion: 8, CommittedFarmVersion: 9, FarmRecordVersion: 10}
	return appliedParticipantResult(t, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT, result)
}

func appliedParticipantResult(t *testing.T, command contractspb.ParticipantCommandType, message proto.Message) workflow.StepResult {
	t.Helper()
	bytes, err := deterministicProto(message)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := contracts.LookupParticipantCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := contracts.CanonicalParticipantHash(spec, message)
	if err != nil {
		t.Fatal(err)
	}
	return workflow.StepResult{Outcome: workflow.StepOutcomeApplied, Result: bytes, ResultHash: hash}
}

func TestCreateIfAbsentFreezesDeterministicTheftIdentityAndSurvivesResponseLoss(t *testing.T) {
	base := newTheftFixture(t)
	root, err := BuildRoot(base.input)
	if err != nil {
		t.Fatal(err)
	}
	wantSettlement, _ := identity.TheftSettlementID(root.WorkflowID)
	wantAttempt, _ := identity.TheftAttemptID(base.input.TargetFarmID, base.input.CropCycleID, base.input.VisitorPlayerID)
	if root.WorkflowID == "" || root.SettlementID != wantSettlement || root.TheftAttemptID != wantAttempt ||
		root.ExecutionAcceptedAt != base.input.ActionPermitSnapshot.GetExecutionAcceptedAtAuthorityMs() {
		t.Fatalf("root identities=%+v", root)
	}
	for index, step := range base.record.Steps {
		want, err := identity.ParticipantInvocationID(identity.WorkTypeTheft, root.WorkflowID, identity.ParticipantStep(step.Name), 1)
		if err != nil || step.InvocationID != want {
			t.Fatalf("step %d invocation=%q want=%q err=%v", index, step.InvocationID, want, err)
		}
	}

	underlying := workflow.NewMemoryWorkRepository(func() time.Time { return base.now })
	var lost atomic.Bool
	repo := &createResponseLostRepository{WorkRepository: underlying, lost: &lost}
	base.repo = repo
	base.coordinator, err = NewCoordinator(CoordinatorConfig{Repository: repo, Visitor: base.visitor, Owner: base.owner, Farm: base.farm,
		BucketResolver: fixedBucketResolver{}, BucketVersion: 1, Now: func() time.Time { return base.now }})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := base.coordinator.Create(context.Background(), base.input); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Create response loss err=%v", err)
	}
	replayed, created, err := base.coordinator.Create(context.Background(), base.input)
	if err != nil || created || replayed.Address.WorkID != root.WorkflowID || len(base.farm.callsCopy()) != 0 {
		t.Fatalf("Create replay record=%v created=%v err=%v farmCalls=%d", replayed, created, err, len(base.farm.callsCopy()))
	}
	changed := base.input
	changed.PlotID++
	if _, _, err := base.coordinator.Create(context.Background(), changed); !errors.Is(err, workflow.ErrIdempotencyConflict) {
		t.Fatalf("payload conflict err=%v", err)
	}
}

func TestTheftWorkflowRootUsesCanonicalFriendPairForEitherVisitorOrdering(t *testing.T) {
	base := newTheftFixture(t)
	pairID, err := identity.CanonicalFriendRelationID(base.input.VisitorPlayerID, base.input.OwnerPlayerID)
	if err != nil {
		t.Fatal(err)
	}
	want, err := identity.WorkflowID(identity.EffectiveIdempotencyKey{
		SubjectPlayerID:       base.input.VisitorPlayerID,
		StableCommandTypeName: string(identity.WorkTypeTheft),
		OperationID:           base.input.OperationID,
		Root:                  identity.OperationRootRef{Kind: identity.RootKindFriendPair, CanonicalID: pairID},
	})
	if err != nil {
		t.Fatal(err)
	}
	root, err := BuildRoot(base.input)
	if err != nil || root.WorkflowID != want || root.Key.Root.Kind != identity.RootKindFriendPair || root.Key.Root.CanonicalID != pairID {
		t.Fatalf("root=%+v want workflow=%s err=%v", root, want, err)
	}

	// Public visitor ordering is not a canonical-pair ordering guarantee. The
	// same owner/friend pair must therefore derive the same FRIEND_PAIR root
	// even when the visitor ID is numerically greater than the owner ID.
	reversed := base.input
	reversed.VisitorPlayerID = 30003
	reversed.OwnerPlayerID = 20002
	reversed.TargetFarmID = 20002
	reversed.ActionPermitSnapshot = proto.Clone(base.input.ActionPermitSnapshot).(*contractspb.ActionPermitSnapshot)
	reversed.ActionPermitSnapshot.VisitorPlayerId = reversed.VisitorPlayerID
	reversed.ActionPermitSnapshot.OwnerPlayerId = reversed.OwnerPlayerID
	reversed.ActionPermitSnapshot.TargetFarmId = reversed.TargetFarmID
	reversed.ActionPermitSnapshot.OperationId = reversed.OperationID
	root, err = BuildRoot(reversed)
	if err != nil {
		t.Fatalf("reversed visitor root: %v", err)
	}
	reversedPair, err := identity.CanonicalFriendRelationID(reversed.VisitorPlayerID, reversed.OwnerPlayerID)
	if err != nil {
		t.Fatal(err)
	}
	if root.Key.Root.Kind != identity.RootKindFriendPair || root.Key.Root.CanonicalID != reversedPair {
		t.Fatalf("reversed root key=%+v pair=%s", root.Key.Root, reversedPair)
	}
}

type createResponseLostRepository struct {
	workflow.WorkRepository
	lost *atomic.Bool
}

func (r *createResponseLostRepository) CreateIfAbsent(ctx context.Context, candidate *workflow.WorkRecord) (*workflow.WorkRecord, bool, error) {
	record, created, err := r.WorkRepository.CreateIfAbsent(ctx, candidate)
	if err == nil && r.lost.CompareAndSwap(false, true) {
		return record, created, context.DeadlineExceeded
	}
	return record, created, err
}

func TestFarmVisitorOwnerResponseLossReplaysSameInvocationAndCompletes(t *testing.T) {
	f := newTheftFixture(t)
	f.decision = decisionConfig{gross: 10, lost: 10, net: 0, coin: 20, guard: true}
	f.farm.loseFirst, f.visitor.loseFirst, f.owner.loseFirst = true, true, true
	f.installScripts()

	if err := f.coordinator.Resume(context.Background(), f.record); !errors.Is(err, workflow.ErrRetryScheduled) {
		t.Fatalf("Farm response loss err=%v", err)
	}
	if StateFor(f.record) != StateFarmDeciding || len(f.farm.callsCopy()) != 1 {
		t.Fatalf("after Farm loss state=%s calls=%d", StateFor(f.record), len(f.farm.callsCopy()))
	}
	if err := f.retry(t); !errors.Is(err, workflow.ErrRetryScheduled) {
		t.Fatalf("Visitor response loss err=%v", err)
	}
	if StateFor(f.record) != StateVisitorSettling || len(f.visitor.callsCopy()) != 1 {
		t.Fatalf("after Visitor loss state=%s calls=%d", StateFor(f.record), len(f.visitor.callsCopy()))
	}
	if err := f.retry(t); !errors.Is(err, workflow.ErrRetryScheduled) {
		t.Fatalf("Owner response loss err=%v", err)
	}
	if StateFor(f.record) != StateOwnerSettling || len(f.owner.callsCopy()) != 1 {
		t.Fatalf("after Owner loss state=%s calls=%d", StateFor(f.record), len(f.owner.callsCopy()))
	}
	if err := f.retry(t); err != nil {
		t.Fatalf("Owner receipt replay err=%v", err)
	}
	if StateFor(f.record) != StateCompleted || OutcomeFor(f.record) != OutcomeSucceeded {
		t.Fatalf("final state=%s outcome=%s", StateFor(f.record), OutcomeFor(f.record))
	}
	if len(f.farm.callsCopy()) != 2 || len(f.visitor.callsCopy()) != 2 || len(f.owner.callsCopy()) != 2 {
		t.Fatalf("replay call counts farm=%d visitor=%d owner=%d", len(f.farm.callsCopy()), len(f.visitor.callsCopy()), len(f.owner.callsCopy()))
	}
	assertSameInvocation(t, f.farm.callsCopy())
	assertSameInvocation(t, f.visitor.callsCopy())
	assertSameInvocation(t, f.owner.callsCopy())
	result, err := PublicResult(f.record)
	if err != nil || result.GetSettlementId() == "" || result.GetCommittedFarmVersion() != 9 {
		t.Fatalf("public result=%v err=%v", result, err)
	}
}

func TestAcceptedWorkflowRestartAfterPermitExpiryCompletesFromReceipts(t *testing.T) {
	f := newTheftFixture(t)
	f.decision = decisionConfig{gross: 10, lost: 10, net: 0, coin: 20, guard: true}
	f.farm.loseFirst = true
	f.installScripts()
	if err := f.coordinator.Resume(context.Background(), f.record); !errors.Is(err, workflow.ErrRetryScheduled) {
		t.Fatalf("Farm response loss err=%v", err)
	}
	if len(f.farm.receipts) != 1 || StateFor(f.record) != StateFarmDeciding {
		t.Fatalf("Farm receipt/state after response loss receipts=%d state=%s", len(f.farm.receipts), StateFor(f.record))
	}
	// Simulate downtime crossing the short permit window, then construct a new
	// coordinator as a restarted recovery process. The accepted WorkRecord's
	// independent deadline remains authoritative.
	f.now = f.now.Add(time.Hour)
	if f.now.UnixMilli() <= f.input.ActionPermitSnapshot.GetExpireAtAuthorityMs() {
		t.Fatal("test clock did not cross permit expiry")
	}
	restarted, err := NewCoordinator(CoordinatorConfig{Repository: f.repo, Visitor: f.visitor, Owner: f.owner, Farm: f.farm,
		BucketResolver: fixedBucketResolver{}, BucketVersion: 1, Now: func() time.Time { return f.now }})
	if err != nil {
		t.Fatal(err)
	}
	reloaded, err := f.repo.Get(context.Background(), string(identity.WorkTypeTheft), f.record.Address.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Resume(context.Background(), reloaded); err != nil {
		t.Fatalf("restart recovery after permit expiry: %v", err)
	}
	if StateFor(reloaded) != StateCompleted || len(f.farm.callsCopy()) != 2 ||
		len(f.visitor.callsCopy()) != 1 || len(f.owner.callsCopy()) != 1 {
		t.Fatalf("terminal=%s calls farm/visitor/owner=%d/%d/%d", StateFor(reloaded), len(f.farm.callsCopy()),
			len(f.visitor.callsCopy()), len(f.owner.callsCopy()))
	}
}

func assertSameInvocation(t *testing.T, calls []workflow.StepCall) {
	t.Helper()
	if len(calls) != 2 || calls[0].InvocationID != calls[1].InvocationID || calls[0].PayloadHash != calls[1].PayloadHash ||
		calls[0].PayloadHash == ([32]byte{}) || calls[1].WorkVersion <= calls[0].WorkVersion {
		t.Fatalf("invocation was not stable: %+v", calls)
	}
}

func TestNetZeroStillSettlesVisitorAndCoinZeroSkipsOwner(t *testing.T) {
	guarded := newTheftFixture(t)
	guarded.decision = decisionConfig{gross: 10, lost: 10, net: 0, coin: 20, guard: true}
	guarded.installScripts()
	guarded.runToTerminal(t)
	if len(guarded.visitor.callsCopy()) != 1 || len(guarded.owner.callsCopy()) != 1 {
		t.Fatalf("net=0 calls visitor=%d owner=%d", len(guarded.visitor.callsCopy()), len(guarded.owner.callsCopy()))
	}
	visitorPayload := &contractspb.SettleTheftVisitorPayload{}
	if err := proto.Unmarshal(guarded.visitor.callsCopy()[0].Payload, visitorPayload); err != nil {
		t.Fatal(err)
	}
	if visitorPayload.GetTheftTargetDecision().GetNetReceivedQuantity() != 0 {
		t.Fatalf("visitor payload net=%d", visitorPayload.GetTheftTargetDecision().GetNetReceivedQuantity())
	}

	noGuard := newTheftFixture(t)
	noGuard.decision = decisionConfig{gross: 10, lost: 0, net: 10, coin: 0, guard: false}
	noGuard.installScripts()
	noGuard.runToTerminal(t)
	if len(noGuard.visitor.callsCopy()) != 1 || len(noGuard.owner.callsCopy()) != 0 || StateFor(noGuard.record) != StateCompleted {
		t.Fatalf("coin=0 state=%s visitor=%d owner=%d", StateFor(noGuard.record), len(noGuard.visitor.callsCopy()), len(noGuard.owner.callsCopy()))
	}
	if noGuard.record.Steps[2].Status != workflow.StepPending {
		t.Fatalf("coin=0 owner step mutated: %s", noGuard.record.Steps[2].Status)
	}
}

func TestFarmRejectAndNoChangeTerminateWithoutPlayerCalls(t *testing.T) {
	for _, test := range []struct {
		name    string
		outcome workflow.StepOutcome
		want    State
	}{
		{name: "reject", outcome: workflow.StepOutcomeRejected, want: StateRejected},
		{name: "no_change", outcome: workflow.StepOutcomeNoChange, want: StateNoChange},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newTheftFixture(t)
			f.farm.script = func(workflow.StepCall) (workflow.StepResult, error) {
				return workflow.StepResult{Outcome: test.outcome, ErrorClass: "FARM_DECISION_REJECTED"}, nil
			}
			if err := f.coordinator.Resume(context.Background(), f.record); err != nil {
				t.Fatal(err)
			}
			if StateFor(f.record) != test.want || len(f.visitor.callsCopy()) != 0 || len(f.owner.callsCopy()) != 0 {
				t.Fatalf("state=%s visitor=%d owner=%d", StateFor(f.record), len(f.visitor.callsCopy()), len(f.owner.callsCopy()))
			}
		})
	}
}

type captureResultPublisher struct {
	publication ResultPublication
}

func (p *captureResultPublisher) Publish(_ context.Context, publication ResultPublication) error {
	p.publication = publication
	return nil
}

func TestPublishResultAcceptsSucceededNoChangeAndRejectedTerminals(t *testing.T) {
	for _, test := range []struct {
		name        string
		farmOutcome workflow.StepOutcome
		want        commonpb.CommandStatus
	}{
		{name: "succeeded", farmOutcome: workflow.StepOutcomeApplied, want: commonpb.CommandStatus_SUCCEEDED},
		{name: "no_change", farmOutcome: workflow.StepOutcomeNoChange, want: commonpb.CommandStatus_NO_CHANGE},
		{name: "rejected", farmOutcome: workflow.StepOutcomeRejected, want: commonpb.CommandStatus_REJECTED},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newTheftFixture(t)
			if test.farmOutcome != workflow.StepOutcomeApplied {
				f.farm.script = func(workflow.StepCall) (workflow.StepResult, error) {
					return workflow.StepResult{Outcome: test.farmOutcome, ErrorClass: "ERR_CROP_NOT_MATURE"}, nil
				}
			}
			if err := f.coordinator.Resume(context.Background(), f.record); err != nil {
				t.Fatal(err)
			}
			publisher := &captureResultPublisher{}
			if err := PublishResult(context.Background(), f.record, publisher); err != nil {
				t.Fatal(err)
			}
			publication := publisher.publication
			if publication.Outcome.GetStatus() != test.want || publication.Outcome.GetOriginalOperationId() != f.input.OperationID ||
				publication.SubjectPlayerID != f.input.VisitorPlayerID || !bytes.Equal(publication.PayloadHash, f.input.ActionPermitSnapshot.GetCanonicalPayloadHash()) {
				t.Fatalf("publication=%+v", publication)
			}
		})
	}
}

func TestHashAndDecisionMismatchFailClosedAsReconcileRequired(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*theftFixture)
	}{
		{name: "result_hash", mutate: func(f *theftFixture) { f.decision.badResultHash = true; f.installScripts() }},
		{name: "decision_identity", mutate: func(f *theftFixture) { f.decision.badDecision = true; f.installScripts() }},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newTheftFixture(t)
			test.mutate(f)
			if err := f.coordinator.Resume(context.Background(), f.record); err == nil {
				t.Fatal("inconsistent Farm response unexpectedly succeeded")
			}
			if StateFor(f.record) != StateReconcileRequired || len(f.visitor.callsCopy()) != 0 || len(f.owner.callsCopy()) != 0 {
				t.Fatalf("state=%s visitor=%d owner=%d", StateFor(f.record), len(f.visitor.callsCopy()), len(f.owner.callsCopy()))
			}
		})
	}
}

func TestRetryExhaustionPersistsReconcileRequired(t *testing.T) {
	f := newTheftFixture(t)
	f.maxAttempts = 2
	f.farm.alwaysError = errors.New("response lost")
	f.coordinator, _ = NewCoordinator(CoordinatorConfig{Repository: f.repo, Visitor: f.visitor, Owner: f.owner, Farm: f.farm,
		BucketResolver: fixedBucketResolver{}, BucketVersion: 1, Now: func() time.Time { return f.now }, MaxStepAttempts: 2})
	if err := f.coordinator.Resume(context.Background(), f.record); !errors.Is(err, workflow.ErrRetryScheduled) {
		t.Fatalf("first retry err=%v", err)
	}
	if err := f.retry(t); !errors.Is(err, workflow.ErrRetryScheduled) {
		t.Fatalf("second retry err=%v state=%s", err, StateFor(f.record))
	}
	if err := f.retry(t); err == nil || StateFor(f.record) != StateReconcileRequired {
		t.Fatalf("exhaustion err=%v state=%s", err, StateFor(f.record))
	}
	if len(f.farm.callsCopy()) != 2 || f.record.RetryCount != 2 {
		t.Fatalf("retry count calls=%d retry=%d", len(f.farm.callsCopy()), f.record.RetryCount)
	}
}

func TestDirectAndWorkerLeaseFenceDoNotDoubleAdvance(t *testing.T) {
	f := newTheftFixture(t)
	claimed, err := f.repo.ClaimDue(context.Background(), "worker-a", "boot-token", 1, time.Minute)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim=%v err=%v", claimed, err)
	}
	address := f.record.Address
	direct, err := workflow.NewDirectDispatcher(f.repo, f.coordinator.Engine(), workflow.DispatcherConfig{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := direct.DispatchOnce(context.Background(), address); !errors.Is(err, workflow.ErrLeaseLost) {
		t.Fatalf("direct crossed live worker lease: %v", err)
	}
	if len(f.farm.callsCopy()) != 0 {
		t.Fatal("direct invoked Farm under worker lease")
	}
	if err := f.coordinator.Engine().Resume(context.Background(), claimed[0].Record,
		workflow.LeaseFence{Owner: "worker-a", Epoch: claimed[0].Record.LeaseEpoch}, nil); err != nil {
		t.Fatalf("worker resume err=%v", err)
	}
	final, err := f.repo.Get(context.Background(), address.WorkType, address.WorkID)
	if err != nil || StateFor(final) != StateCompleted || len(f.farm.callsCopy()) != 1 {
		t.Fatalf("worker final=%s calls=%d err=%v", StateFor(final), len(f.farm.callsCopy()), err)
	}
}

// TestTheftStaleProofCoordinatorReloadsAndCompletes drives the Theft workflow
// through the Coordinator fast path (RPCInvoker, unleased). Each participant
// observes a stale WorkVersion proof on its first invocation; the engine must
// reload the authoritative record and re-invoke the same invocation, converge
// to COMPLETED, never record REJECTED/StepUnknown, and never re-apply a
// receipt (WF-WORKVERSION-PROOF-RACE unified theft coverage).
func TestTheftStaleProofCoordinatorReloadsAndCompletes(t *testing.T) {
	f := newTheftFixture(t)
	f.decision = decisionConfig{gross: 10, lost: 10, net: 0, coin: 20, guard: true}
	for _, participant := range []*fakeParticipant{f.farm, f.visitor, f.owner} {
		participant.staleOnce = map[string]bool{}
		for _, step := range f.record.Steps {
			participant.staleOnce[step.InvocationID] = true
		}
	}
	f.installScripts()

	if err := f.coordinator.Resume(context.Background(), f.record); err != nil {
		t.Fatalf("coordinator resume err=%v", err)
	}
	if StateFor(f.record) != StateCompleted || OutcomeFor(f.record) != OutcomeSucceeded {
		t.Fatalf("state=%s outcome=%s", StateFor(f.record), OutcomeFor(f.record))
	}
	for _, step := range f.record.Steps {
		if step.Status == workflow.StepUnknown || step.Status == workflow.StepRejected {
			t.Fatalf("step %s left %s after stale-proof reload", step.Name, step.Status)
		}
	}
	// Each participant: one stale-proof failure + one successful invoke per
	// step (2 calls for steps actually executed, 1 call for the owner when
	// guard coin > 0). Receipt replay must not double-apply.
	assertTheftSingleApply(t, f)
}

// TestTheftStaleProofWorkerLeaseReloadsAndCompletes drives the same stale
// proof through the worker-lease path (Engine.Resume with a LeaseFence after
// ClaimDue), which is the path theftonline/worker.go assembles with
// NewWorkerRecoveryInvoker.
func TestTheftStaleProofWorkerLeaseReloadsAndCompletes(t *testing.T) {
	f := newTheftFixture(t)
	f.decision = decisionConfig{gross: 10, lost: 10, net: 0, coin: 20, guard: true}
	for _, participant := range []*fakeParticipant{f.farm, f.visitor, f.owner} {
		participant.staleOnce = map[string]bool{}
		for _, step := range f.record.Steps {
			participant.staleOnce[step.InvocationID] = true
		}
	}
	f.installScripts()

	claimed, err := f.repo.ClaimDue(context.Background(), "worker-theft", "boot-token", 1, time.Minute)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim=%v err=%v", claimed, err)
	}
	if err := f.coordinator.Engine().Resume(context.Background(), claimed[0].Record,
		workflow.LeaseFence{Owner: "worker-theft", Epoch: claimed[0].Record.LeaseEpoch}, nil); err != nil {
		t.Fatalf("worker lease resume err=%v", err)
	}
	final, err := f.repo.Get(context.Background(), f.record.Address.WorkType, f.record.Address.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	if StateFor(final) != StateCompleted || OutcomeFor(final) != OutcomeSucceeded {
		t.Fatalf("state=%s outcome=%s", StateFor(final), OutcomeFor(final))
	}
	for _, step := range final.Steps {
		if step.Status == workflow.StepUnknown || step.Status == workflow.StepRejected {
			t.Fatalf("step %s left %s after stale-proof reload", step.Name, step.Status)
		}
	}
	assertTheftSingleApply(t, f)
}

// assertTheftSingleApply asserts every executed Theft step was applied exactly
// once (no re-apply on reload) and each participant saw exactly 2 calls for
// its own step (one stale-proof failure + one success).
func assertTheftSingleApply(t *testing.T, f *theftFixture) {
	t.Helper()
	bindings := []struct {
		participant *fakeParticipant
		index      int
	}{
		{participant: f.farm, index: 0},
		{participant: f.visitor, index: 1},
		{participant: f.owner, index: 2},
	}
	for _, binding := range bindings {
		step := &f.record.Steps[binding.index]
		if step.Status != workflow.StepApplied {
			continue
		}
		calls := 0
		for _, call := range binding.participant.callsCopy() {
			if call.InvocationID == step.InvocationID {
				calls++
			}
		}
		if calls != 2 {
			t.Fatalf("participant %s step %s calls=%d, want 1 stale + 1 success", binding.participant.name, step.Name, calls)
		}
		if receipt, ok := binding.participant.receipts[step.InvocationID]; !ok || receipt.payloadHash != step.PayloadHash {
			t.Fatalf("participant %s step %s receipt missing/mismatched", binding.participant.name, step.Name)
		}
	}
}
