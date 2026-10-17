// Package unlockplot contains the durable four-step UnlockPlot workflow.
// The workflow root is the owning FarmActor; coin effects are still performed
// only by the PlayerActor participant and are never debited by this package.
package unlockplot

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strconv"
	"time"

	contractspb "farmgame/api/gen/contracts"
	"farmgame/internal/contracts"
	"farmgame/internal/farm/domain"
	"farmgame/internal/identity"
	"farmgame/internal/workflow"
	"farmgame/internal/workidentity"

	"google.golang.org/protobuf/proto"
)

const (
	defaultWorkflowDeadline = 24 * time.Hour
	firstUnlockPlot         = domain.P0InitialUnlockedPlots + 1
	maxUnlockPlot           = domain.P0MaxPlots
)

var (
	ErrInvalidInput = errors.New("unlockplot: invalid input")
	ErrRootPayload  = errors.New("unlockplot: invalid durable root payload")
	ErrPriorResult  = errors.New("unlockplot: invalid prior typed result")
)

// UnlockInput is trusted coordinator input. RequiredPlayerLevel, amount and
// RuleSnapshotHash are copied from the server-owned versioned rule projection;
// they are not accepted from the public client as authority.
type UnlockInput struct {
	SubjectPlayerID           uint64
	OperationID               string
	TargetFarmID              uint64
	PlotID                    uint32
	ExpectedPlotVersion       uint64
	ExpectedUnlockRuleVersion uint64
	RequiredPlayerLevel       uint32
	AmountNormalCoin          uint64
	RuleSnapshotHash          []byte
}

type RootIntent struct {
	Key           identity.EffectiveIdempotencyKey
	WorkflowID    string
	ReservationID string
	Payload       *contractspb.CommitPlotUnlockPayload
	PayloadBytes  []byte
	PayloadHash   [32]byte
}

// DefaultRuleSnapshotHash gives the workflow a stable hash of the complete P0
// rule shape. It is deliberately independent of client data and changes when
// the frozen sequence/price formula changes.
func DefaultRuleSnapshotHash() [32]byte {
	return domain.PlotUnlockRuleSnapshotHash(domain.DefaultPlotUnlockRule())
}

func BuildRoot(input UnlockInput) (RootIntent, error) {
	if input.RuleSnapshotHash == nil {
		defaultHash := DefaultRuleSnapshotHash()
		input.RuleSnapshotHash = defaultHash[:]
	}
	if err := validateInput(input); err != nil {
		return RootIntent{}, err
	}
	key := identity.EffectiveIdempotencyKey{
		SubjectPlayerID: input.SubjectPlayerID, StableCommandTypeName: string(identity.WorkTypeUnlockPlot),
		OperationID: input.OperationID, Root: identity.OperationRootRef{Kind: identity.RootKindFarmActor, CanonicalID: strconv.FormatUint(input.TargetFarmID, 10)},
	}
	workflowID, err := identity.WorkflowID(key)
	if err != nil {
		return RootIntent{}, fmt.Errorf("%w: workflow identity: %v", ErrInvalidInput, err)
	}
	reservationID, err := identity.UnlockCoinReservationID(workflowID)
	if err != nil {
		return RootIntent{}, fmt.Errorf("%w: reservation identity: %v", ErrInvalidInput, err)
	}
	var predecessor *uint32
	if input.PlotID > firstUnlockPlot {
		value := input.PlotID - 1
		predecessor = &value
	}
	payload := &contractspb.CommitPlotUnlockPayload{
		OwnerPlayerId: input.SubjectPlayerID, FarmId: input.TargetFarmID, PlotId: input.PlotID,
		ExpectedPlotVersion: input.ExpectedPlotVersion, ExpectedUnlockRuleVersion: input.ExpectedUnlockRuleVersion,
		UnlockOrder: input.PlotID, PredecessorPlotId: predecessor, ReservationId: reservationID,
		UnlockRuleSnapshotHash: append([]byte(nil), input.RuleSnapshotHash...), RequiredPlayerLevel: input.RequiredPlayerLevel,
		AmountNormalCoin: input.AmountNormalCoin,
	}
	payloadBytes, err := deterministicProto(payload)
	if err != nil {
		return RootIntent{}, fmt.Errorf("%w: deterministic root: %v", ErrInvalidInput, err)
	}
	payloadHash, err := contracts.CanonicalHash(payload)
	if err != nil {
		return RootIntent{}, fmt.Errorf("%w: root hash: %v", ErrInvalidInput, err)
	}
	return RootIntent{Key: key, WorkflowID: workflowID, ReservationID: reservationID, Payload: proto.Clone(payload).(*contractspb.CommitPlotUnlockPayload), PayloadBytes: payloadBytes, PayloadHash: payloadHash}, nil
}

func BuildRootPayload(input UnlockInput) ([]byte, [32]byte, error) {
	root, err := BuildRoot(input)
	if err != nil {
		return nil, [32]byte{}, err
	}
	return append([]byte(nil), root.PayloadBytes...), root.PayloadHash, nil
}

func (r RootIntent) Address(bucketVersion, bucketID uint32) (workidentity.WorkAddress, error) {
	if bucketVersion == 0 {
		return workidentity.WorkAddress{}, fmt.Errorf("%w: bucket version", ErrInvalidInput)
	}
	address := workidentity.WorkAddress{WorkID: r.WorkflowID, WorkType: string(identity.WorkTypeUnlockPlot), PlacementKind: workidentity.IndependentWorkBucket, BucketVersion: bucketVersion, BucketID: bucketID}
	if err := address.ValidateShape(); err != nil {
		return workidentity.WorkAddress{}, fmt.Errorf("%w: work address: %v", ErrInvalidInput, err)
	}
	return address, nil
}

func (r RootIntent) AddressForResolver(bucketVersion uint32, resolver workidentity.BucketResolver) (workidentity.WorkAddress, error) {
	if resolver == nil || bucketVersion == 0 {
		return workidentity.WorkAddress{}, fmt.Errorf("%w: bucket resolver and version", ErrInvalidInput)
	}
	bucketID, err := resolver.IndependentWorkBucket(r.WorkflowID, bucketVersion)
	if err != nil {
		return workidentity.WorkAddress{}, fmt.Errorf("%w: resolve bucket: %v", ErrInvalidInput, err)
	}
	address, err := r.Address(bucketVersion, bucketID)
	if err != nil {
		return workidentity.WorkAddress{}, err
	}
	if err := address.Validate(resolver); err != nil {
		return workidentity.WorkAddress{}, fmt.Errorf("%w: validate bucket: %v", ErrInvalidInput, err)
	}
	return address, nil
}

func validateInput(input UnlockInput) error {
	if input.SubjectPlayerID == 0 || input.OperationID == "" || input.TargetFarmID == 0 || input.TargetFarmID != input.SubjectPlayerID ||
		input.PlotID < firstUnlockPlot || input.PlotID > maxUnlockPlot || input.ExpectedPlotVersion == 0 || input.ExpectedUnlockRuleVersion == 0 ||
		input.RequiredPlayerLevel == 0 || input.AmountNormalCoin == 0 || len(input.RuleSnapshotHash) != sha256.Size {
		return fmt.Errorf("%w: required fields or ownership", ErrInvalidInput)
	}
	rule := domain.DefaultPlotUnlockRule()
	entry, ok := rule.EntryOf(input.PlotID)
	if !ok || input.ExpectedUnlockRuleVersion != rule.RuleVersion || input.RequiredPlayerLevel != entry.RequiredLevel || input.AmountNormalCoin != entry.NormalCoinPrice {
		return fmt.Errorf("%w: trusted P0 rule projection mismatch", ErrInvalidInput)
	}
	defaultHash := DefaultRuleSnapshotHash()
	if !bytes.Equal(input.RuleSnapshotHash, defaultHash[:]) {
		return fmt.Errorf("%w: rule snapshot hash", ErrInvalidInput)
	}
	key := identity.EffectiveIdempotencyKey{SubjectPlayerID: input.SubjectPlayerID, StableCommandTypeName: string(identity.WorkTypeUnlockPlot), OperationID: input.OperationID, Root: identity.OperationRootRef{Kind: identity.RootKindFarmActor, CanonicalID: strconv.FormatUint(input.TargetFarmID, 10)}}
	if _, err := key.OperationDigest(); err != nil {
		return fmt.Errorf("%w: idempotency key: %v", ErrInvalidInput, err)
	}
	return nil
}

type Definition struct{ player, farm workflow.ParticipantInvoker }

var _ workflow.Definition = (*Definition)(nil)

func NewDefinition(player, farm workflow.ParticipantInvoker) (*Definition, error) {
	if player == nil || farm == nil {
		return nil, fmt.Errorf("%w: participant invokers required", ErrInvalidInput)
	}
	return &Definition{player: player, farm: farm}, nil
}

func (*Definition) WorkType() string { return string(identity.WorkTypeUnlockPlot) }

func (*Definition) StepNames() []string {
	return []string{string(identity.StepReserveUnlockCoin), string(identity.StepCommitPlotUnlock), string(identity.StepCommitUnlockCoinReservation), string(identity.StepReleaseUnlockCoinReservation)}
}

func (d *Definition) Invoker(stepIndex int) workflow.ParticipantInvoker {
	if d == nil {
		return nil
	}
	var delegate workflow.ParticipantInvoker
	switch stepIndex {
	case 0, 2, 3:
		delegate = d.player
	case 1:
		delegate = d.farm
	default:
		return nil
	}
	return dependencyBoundInvoker{delegate: delegate, stepIndex: stepIndex}
}

type dependencyBoundInvoker struct {
	delegate  workflow.ParticipantInvoker
	stepIndex int
}

func (i dependencyBoundInvoker) Invoke(ctx context.Context, call workflow.StepCall) (workflow.StepResult, error) {
	dependency, err := dependencyHashFromPayload(i.stepIndex, call.Payload)
	if err != nil {
		return workflow.StepResult{}, err
	}
	call.DependsOnResultHash = dependency
	return i.delegate.Invoke(ctx, call)
}

func dependencyHashFromPayload(stepIndex int, payload []byte) ([32]byte, error) {
	if stepIndex == 0 {
		return [32]byte{}, nil
	}
	var dependency []byte
	switch stepIndex {
	case 1:
		message := &contractspb.CommitPlotUnlockPayload{}
		if err := proto.Unmarshal(payload, message); err != nil {
			return [32]byte{}, fmt.Errorf("%w: CommitPlotUnlock payload: %v", ErrPriorResult, err)
		}
		dependency = message.GetReserveUnlockResultHash()
	case 2:
		message := &contractspb.CommitUnlockCoinReservationPayload{}
		if err := proto.Unmarshal(payload, message); err != nil {
			return [32]byte{}, fmt.Errorf("%w: CommitUnlock payload: %v", ErrPriorResult, err)
		}
		dependency = message.GetCommitPlotUnlockResultHash()
	case 3:
		message := &contractspb.ReleaseUnlockCoinReservationPayload{}
		if err := proto.Unmarshal(payload, message); err != nil {
			return [32]byte{}, fmt.Errorf("%w: ReleaseUnlock payload: %v", ErrPriorResult, err)
		}
		dependency = message.GetTerminalFarmResultHash()
	default:
		return [32]byte{}, fmt.Errorf("%w: step index %d", ErrInvalidInput, stepIndex)
	}
	if len(dependency) != sha256.Size {
		return [32]byte{}, fmt.Errorf("%w: step %d dependency hash", ErrPriorResult, stepIndex)
	}
	var result [32]byte
	copy(result[:], dependency)
	return result, nil
}

func commandForStep(step string) (contractspb.ParticipantCommandType, error) {
	switch step {
	case string(identity.StepReserveUnlockCoin):
		return contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_UNLOCK_COIN, nil
	case string(identity.StepCommitPlotUnlock):
		return contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLOT_UNLOCK, nil
	case string(identity.StepCommitUnlockCoinReservation):
		return contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_UNLOCK_COIN_RESERVATION, nil
	case string(identity.StepReleaseUnlockCoinReservation):
		return contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RELEASE_UNLOCK_COIN_RESERVATION, nil
	default:
		return contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_UNSPECIFIED, fmt.Errorf("%w: unknown UnlockPlot step %q", ErrInvalidInput, step)
	}
}

func (d *Definition) BuildPayload(record *workflow.WorkRecord, stepIndex int) ([]byte, error) {
	root, err := decodeRoot(record)
	if err != nil {
		return nil, err
	}
	switch stepIndex {
	case 0:
		return deterministicProto(&contractspb.ReserveUnlockCoinPayload{PlayerId: root.GetOwnerPlayerId(), FarmId: root.GetFarmId(), PlotId: root.GetPlotId(), ReservationId: root.GetReservationId(), RequiredPlayerLevel: root.GetRequiredPlayerLevel(), AmountNormalCoin: root.GetAmountNormalCoin(), UnlockRuleVersion: root.GetExpectedUnlockRuleVersion(), UnlockRuleSnapshotHash: append([]byte(nil), root.GetUnlockRuleSnapshotHash()...)})
	case 1:
		if err := validateReserveStepStatus(record); err != nil {
			return nil, err
		}
		reserve := &contractspb.ReserveUnlockCoinResult{}
		reserveHash, err := durableResultHash(record, 0, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_UNLOCK_COIN, reserve)
		if err != nil {
			return nil, err
		}
		if reserve.GetReservationId() != root.GetReservationId() || reserve.GetReservedAmountNormalCoin() != root.GetAmountNormalCoin() || reserve.GetPlayerLevelAtReserve() < root.GetRequiredPlayerLevel() {
			return nil, fmt.Errorf("%w: reserve result identity/eligibility", ErrPriorResult)
		}
		payload := proto.Clone(root).(*contractspb.CommitPlotUnlockPayload)
		payload.ReserveUnlockResultHash = append([]byte(nil), reserveHash[:]...)
		return deterministicProto(payload)
	case 2, 3:
		if err := validateReserveStepStatus(record); err != nil {
			return nil, err
		}
		if err := validateFarmBranchStatus(record, stepIndex); err != nil {
			return nil, err
		}
		farm := &contractspb.CommitPlotUnlockResult{}
		farmHash, err := durableResultHash(record, 1, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLOT_UNLOCK, farm)
		if err != nil {
			return nil, err
		}
		if err := validateFarmResult(root, farm); err != nil {
			return nil, err
		}
		if stepIndex == 2 {
			return deterministicProto(&contractspb.CommitUnlockCoinReservationPayload{PlayerId: root.GetOwnerPlayerId(), FarmId: root.GetFarmId(), PlotId: root.GetPlotId(), ReservationId: root.GetReservationId(), AmountNormalCoin: root.GetAmountNormalCoin(), CommitPlotUnlockResultHash: append([]byte(nil), farmHash[:]...)})
		}
		reason := "FARM_REJECTED"
		if record.Steps[1].Status == workflow.StepNoChange {
			reason = "FARM_NO_CHANGE"
		}
		return deterministicProto(&contractspb.ReleaseUnlockCoinReservationPayload{PlayerId: root.GetOwnerPlayerId(), FarmId: root.GetFarmId(), PlotId: root.GetPlotId(), ReservationId: root.GetReservationId(), AmountNormalCoin: root.GetAmountNormalCoin(), ReleaseReason: reason, TerminalFarmResultHash: append([]byte(nil), farmHash[:]...)})
	default:
		return nil, fmt.Errorf("%w: step index %d", ErrInvalidInput, stepIndex)
	}
}

func (*Definition) Decide(record *workflow.WorkRecord, stepIndex int, result workflow.StepResult) (workflow.Decision, error) {
	switch stepIndex {
	case 0:
		switch result.Outcome {
		case workflow.StepOutcomeApplied, workflow.StepOutcomeNoChange:
			return workflow.Decision{NextStep: 1}, nil
		case workflow.StepOutcomeRejected:
			return workflow.Decision{Terminal: workflow.WorkStateRejected}, nil
		default:
			return workflow.Decision{}, fmt.Errorf("%w: reserve outcome %d", ErrInvalidInput, result.Outcome)
		}
	case 1:
		switch result.Outcome {
		case workflow.StepOutcomeApplied:
			return workflow.Decision{NextStep: 2}, nil
		case workflow.StepOutcomeNoChange, workflow.StepOutcomeRejected:
			return workflow.Decision{NextStep: 3}, nil
		default:
			return workflow.Decision{}, fmt.Errorf("%w: Farm unlock outcome %d", ErrInvalidInput, result.Outcome)
		}
	case 2:
		if result.Outcome != workflow.StepOutcomeApplied && result.Outcome != workflow.StepOutcomeNoChange {
			return workflow.Decision{Terminal: workflow.WorkStateFailed}, nil
		}
		if err := validateFinalPlayerResult(record, stepIndex, result); err != nil {
			return workflow.Decision{}, err
		}
		return workflow.Decision{Terminal: workflow.WorkStateCompleted}, nil
	case 3:
		if result.Outcome != workflow.StepOutcomeApplied && result.Outcome != workflow.StepOutcomeNoChange {
			return workflow.Decision{Terminal: workflow.WorkStateFailed}, nil
		}
		if err := validateFinalPlayerResult(record, stepIndex, result); err != nil {
			return workflow.Decision{}, err
		}
		return workflow.Decision{Terminal: workflow.WorkStateCompensated}, nil
	default:
		return workflow.Decision{}, fmt.Errorf("%w: step index %d", ErrInvalidInput, stepIndex)
	}
}

type Coordinator struct {
	repository workflow.WorkRepository
	definition *Definition
	engine     *workflow.Engine
	clock      func() time.Time
	deadline   time.Duration
	bucketVer  uint32
	resolver   workidentity.BucketResolver
}

type CoordinatorConfig struct {
	Repository     workflow.WorkRepository
	Player         workflow.ParticipantInvoker
	Farm           workflow.ParticipantInvoker
	BucketResolver workidentity.BucketResolver
	BucketVersion  uint32
	Deadline       time.Duration
	Now            func() time.Time
}

func NewCoordinator(config CoordinatorConfig) (*Coordinator, error) {
	if config.Repository == nil || config.BucketResolver == nil || config.BucketVersion == 0 {
		return nil, fmt.Errorf("%w: repository, bucket resolver and version", ErrInvalidInput)
	}
	definition, err := NewDefinition(config.Player, config.Farm)
	if err != nil {
		return nil, err
	}
	clock := config.Now
	if clock == nil {
		clock = time.Now
	}
	deadline := config.Deadline
	if deadline <= 0 {
		deadline = defaultWorkflowDeadline
	}
	engine, err := workflow.NewEngine(config.Repository, []workflow.Definition{definition}, workflow.WithEngineClock(clock))
	if err != nil {
		return nil, err
	}
	return &Coordinator{repository: config.Repository, definition: definition, engine: engine, clock: clock, deadline: deadline, bucketVer: config.BucketVersion, resolver: config.BucketResolver}, nil
}

func (c *Coordinator) Definition() *Definition {
	if c == nil {
		return nil
	}
	return c.definition
}
func (c *Coordinator) Engine() *workflow.Engine {
	if c == nil {
		return nil
	}
	return c.engine
}

func (c *Coordinator) Create(ctx context.Context, input UnlockInput) (*workflow.WorkRecord, bool, error) {
	if c == nil || c.engine == nil {
		return nil, false, fmt.Errorf("%w: coordinator", ErrInvalidInput)
	}
	root, err := BuildRoot(input)
	if err != nil {
		return nil, false, err
	}
	address, err := root.AddressForResolver(c.bucketVer, c.resolver)
	if err != nil {
		return nil, false, err
	}
	return c.engine.Create(ctx, c.definition, address, input.SubjectPlayerID, input.OperationID, root.PayloadBytes, root.PayloadHash, c.clock().Add(c.deadline))
}

func (c *Coordinator) Resume(ctx context.Context, record *workflow.WorkRecord) error {
	if c == nil || c.engine == nil {
		return fmt.Errorf("%w: coordinator", ErrInvalidInput)
	}
	return c.engine.Resume(ctx, record, workflow.LeaseFence{}, nil)
}

func (c *Coordinator) Start(ctx context.Context, input UnlockInput) (*workflow.WorkRecord, bool, error) {
	record, created, err := c.Create(ctx, input)
	if err != nil {
		return nil, false, err
	}
	if record.Terminal() {
		return record, created, nil
	}
	if err := c.Resume(ctx, record); err != nil {
		return record, created, err
	}
	return record, created, nil
}

func decodeRoot(record *workflow.WorkRecord) (*contractspb.CommitPlotUnlockPayload, error) {
	if record == nil || record.Address.WorkType != string(identity.WorkTypeUnlockPlot) {
		return nil, fmt.Errorf("%w: record/work type", ErrRootPayload)
	}
	root := &contractspb.CommitPlotUnlockPayload{}
	if err := proto.Unmarshal(record.Payload, root); err != nil {
		return nil, fmt.Errorf("%w: protobuf: %v", ErrRootPayload, err)
	}
	canonical, err := contracts.CanonicalHash(root)
	if err != nil || canonical != record.PayloadHash {
		return nil, fmt.Errorf("%w: canonical root hash", ErrRootPayload)
	}
	deterministic, err := deterministicProto(root)
	if err != nil || !bytes.Equal(deterministic, record.Payload) {
		return nil, fmt.Errorf("%w: deterministic root bytes", ErrRootPayload)
	}
	if record.SubjectPlayerID == 0 || root.GetOwnerPlayerId() != record.SubjectPlayerID || root.GetFarmId() != root.GetOwnerPlayerId() || root.GetPlotId() < firstUnlockPlot || root.GetPlotId() > maxUnlockPlot || root.GetExpectedPlotVersion() == 0 || root.GetExpectedUnlockRuleVersion() == 0 || root.GetUnlockOrder() != root.GetPlotId() || root.GetRequiredPlayerLevel() != root.GetPlotId()-domain.P0InitialUnlockedPlots || root.GetAmountNormalCoin() != uint64(root.GetPlotId()-domain.P0InitialUnlockedPlots)*100 || len(root.GetUnlockRuleSnapshotHash()) != sha256.Size || len(root.GetReserveUnlockResultHash()) != 0 {
		return nil, fmt.Errorf("%w: root fields", ErrRootPayload)
	}
	defaultHash := DefaultRuleSnapshotHash()
	if !bytes.Equal(root.GetUnlockRuleSnapshotHash(), defaultHash[:]) {
		return nil, fmt.Errorf("%w: rule snapshot hash", ErrRootPayload)
	}
	key := identity.EffectiveIdempotencyKey{SubjectPlayerID: record.SubjectPlayerID, StableCommandTypeName: string(identity.WorkTypeUnlockPlot), OperationID: record.OperationID, Root: identity.OperationRootRef{Kind: identity.RootKindFarmActor, CanonicalID: strconv.FormatUint(root.GetFarmId(), 10)}}
	wantWorkflow, err := identity.WorkflowID(key)
	if err != nil || wantWorkflow != record.Address.WorkID {
		return nil, fmt.Errorf("%w: workflow identity", ErrRootPayload)
	}
	wantReservation, err := identity.UnlockCoinReservationID(record.Address.WorkID)
	if err != nil || wantReservation != root.GetReservationId() {
		return nil, fmt.Errorf("%w: reservation identity", ErrRootPayload)
	}
	return root, nil
}

func validateReserveStepStatus(record *workflow.WorkRecord) error {
	if record == nil || len(record.Steps) == 0 || (record.Steps[0].Status != workflow.StepApplied && record.Steps[0].Status != workflow.StepNoChange) {
		return fmt.Errorf("%w: ReserveUnlockCoin is not successful", ErrPriorResult)
	}
	return nil
}

func validateFarmBranchStatus(record *workflow.WorkRecord, stepIndex int) error {
	if record == nil || len(record.Steps) <= 1 {
		return fmt.Errorf("%w: Farm step missing", ErrPriorResult)
	}
	status := record.Steps[1].Status
	if stepIndex == 2 && status != workflow.StepApplied {
		return fmt.Errorf("%w: commit coin requires applied Farm unlock", ErrPriorResult)
	}
	if stepIndex == 3 && status != workflow.StepRejected && status != workflow.StepNoChange {
		return fmt.Errorf("%w: release coin requires rejected/no-change Farm unlock", ErrPriorResult)
	}
	return nil
}

func durableResultHash(record *workflow.WorkRecord, index int, command contractspb.ParticipantCommandType, result proto.Message) ([32]byte, error) {
	if err := unmarshalDurableResult(record, index, command, result); err != nil {
		return [32]byte{}, err
	}
	return record.Steps[index].ResultHash, nil
}

func unmarshalDurableResult(record *workflow.WorkRecord, index int, command contractspb.ParticipantCommandType, result proto.Message) error {
	if record == nil || index < 0 || index >= len(record.Steps) || result == nil || !workflow.StepTerminal(record.Steps[index].Status) || len(record.Steps[index].Result) == 0 || record.Steps[index].ResultHash == ([32]byte{}) {
		return fmt.Errorf("%w: step %d missing terminal result", ErrPriorResult, index)
	}
	step := &record.Steps[index]
	if err := proto.Unmarshal(step.Result, result); err != nil {
		return fmt.Errorf("%w: step %d protobuf: %v", ErrPriorResult, index, err)
	}
	deterministic, err := deterministicProto(result)
	if err != nil || !bytes.Equal(deterministic, step.Result) {
		return fmt.Errorf("%w: step %d result bytes", ErrPriorResult, index)
	}
	spec, err := contracts.LookupParticipantCommand(command)
	if err != nil {
		return err
	}
	hash, err := contracts.CanonicalParticipantHash(spec, result)
	if err != nil || hash != step.ResultHash {
		return fmt.Errorf("%w: step %d result hash", ErrPriorResult, index)
	}
	return nil
}

func validateFarmResult(root *contractspb.CommitPlotUnlockPayload, result *contractspb.CommitPlotUnlockResult) error {
	if root == nil || result == nil || result.GetFarmId() != root.GetFarmId() || result.GetPlotId() != root.GetPlotId() || result.GetAppliedUnlockRuleVersion() != root.GetExpectedUnlockRuleVersion() || len(result.GetUnlockDecisionHash()) != sha256.Size {
		return fmt.Errorf("%w: Farm unlock result identity", ErrPriorResult)
	}
	return nil
}

func validateFinalPlayerResult(record *workflow.WorkRecord, stepIndex int, result workflow.StepResult) error {
	root, err := decodeRoot(record)
	if err != nil {
		return err
	}
	var command contractspb.ParticipantCommandType
	var message proto.Message
	if stepIndex == 2 {
		command = contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_UNLOCK_COIN_RESERVATION
		message = &contractspb.CommitUnlockCoinReservationResult{}
	} else if stepIndex == 3 {
		command = contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RELEASE_UNLOCK_COIN_RESERVATION
		message = &contractspb.ReleaseUnlockCoinReservationResult{}
	} else {
		return fmt.Errorf("%w: final step %d", ErrPriorResult, stepIndex)
	}
	if len(result.Result) == 0 || result.ResultHash == ([32]byte{}) {
		return fmt.Errorf("%w: final result missing", ErrPriorResult)
	}
	if err := proto.Unmarshal(result.Result, message); err != nil {
		return fmt.Errorf("%w: final result protobuf: %v", ErrPriorResult, err)
	}
	deterministic, err := deterministicProto(message)
	if err != nil || !bytes.Equal(deterministic, result.Result) {
		return fmt.Errorf("%w: final result bytes", ErrPriorResult)
	}
	spec, err := contracts.LookupParticipantCommand(command)
	if err != nil {
		return err
	}
	hash, err := contracts.CanonicalParticipantHash(spec, message)
	if err != nil || hash != result.ResultHash {
		return fmt.Errorf("%w: final result hash", ErrPriorResult)
	}
	switch value := message.(type) {
	case *contractspb.CommitUnlockCoinReservationResult:
		if value.GetReservationId() != root.GetReservationId() || value.GetReservationState() != contractspb.ReservationTerminalState_RESERVATION_TERMINAL_STATE_CONSUMED {
			return fmt.Errorf("%w: commit reservation state", ErrPriorResult)
		}
	case *contractspb.ReleaseUnlockCoinReservationResult:
		if value.GetReservationId() != root.GetReservationId() || value.GetReservationState() != contractspb.ReservationTerminalState_RESERVATION_TERMINAL_STATE_RELEASED {
			return fmt.Errorf("%w: release reservation state", ErrPriorResult)
		}
	}
	return nil
}

func deterministicProto(message proto.Message) ([]byte, error) {
	if message == nil {
		return nil, fmt.Errorf("%w: nil protobuf", ErrInvalidInput)
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(message)
}

// TerminalResults is a read-only projection of the durable typed step
// records. It is used by online ingress and worker tests; it is never a
// second result store and never consults live actor state.
type TerminalResults struct {
	Root    *contractspb.CommitPlotUnlockPayload
	Reserve *contractspb.ReserveUnlockCoinResult
	Farm    *contractspb.CommitPlotUnlockResult
	Commit  *contractspb.CommitUnlockCoinReservationResult
	Release *contractspb.ReleaseUnlockCoinReservationResult

	FarmStatus     workflow.StepStatus
	FarmErrorClass string
}

func ReadTerminalResults(record *workflow.WorkRecord) (TerminalResults, error) {
	root, err := decodeRoot(record)
	if err != nil {
		return TerminalResults{}, err
	}
	result := TerminalResults{Root: root}
	if record == nil || len(record.Steps) != 4 {
		return TerminalResults{}, fmt.Errorf("%w: step count", ErrPriorResult)
	}
	result.FarmStatus = record.Steps[1].Status
	result.FarmErrorClass = record.Steps[1].ErrorClass
	if workflow.StepTerminal(record.Steps[0].Status) {
		result.Reserve = &contractspb.ReserveUnlockCoinResult{}
		if err := unmarshalDurableResult(record, 0, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_UNLOCK_COIN, result.Reserve); err != nil {
			return TerminalResults{}, err
		}
	}
	if workflow.StepTerminal(record.Steps[1].Status) {
		result.Farm = &contractspb.CommitPlotUnlockResult{}
		if err := unmarshalDurableResult(record, 1, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLOT_UNLOCK, result.Farm); err != nil {
			return TerminalResults{}, err
		}
	}
	if workflow.StepTerminal(record.Steps[2].Status) {
		result.Commit = &contractspb.CommitUnlockCoinReservationResult{}
		if err := unmarshalDurableResult(record, 2, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_UNLOCK_COIN_RESERVATION, result.Commit); err != nil {
			return TerminalResults{}, err
		}
	}
	if workflow.StepTerminal(record.Steps[3].Status) {
		result.Release = &contractspb.ReleaseUnlockCoinReservationResult{}
		if err := unmarshalDurableResult(record, 3, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RELEASE_UNLOCK_COIN_RESERVATION, result.Release); err != nil {
			return TerminalResults{}, err
		}
	}
	return result, nil
}

func DecodeRoot(record *workflow.WorkRecord) (*contractspb.CommitPlotUnlockPayload, error) {
	root, err := decodeRoot(record)
	if err != nil {
		return nil, err
	}
	return proto.Clone(root).(*contractspb.CommitPlotUnlockPayload), nil
}
