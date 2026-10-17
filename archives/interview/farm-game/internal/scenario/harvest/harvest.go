// Package harvest contains the durable, two-participant Harvest workflow.
//
// It owns canonical Harvest identity, typed payload construction, and the
// durable state machine only. PlayerActor and FarmActor stay behind the
// workflow.ParticipantInvoker ports supplied by composition.
package harvest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	contractspb "farmgame/api/gen/contracts"
	"farmgame/internal/contracts"
	"farmgame/internal/identity"
	"farmgame/internal/workflow"
	"farmgame/internal/workidentity"

	"google.golang.org/protobuf/proto"
)

const (
	defaultWorkflowDeadline = 24 * time.Hour
	maxHarvestPlotID        = 24
)

var (
	ErrInvalidInput = errors.New("harvest: invalid input")
	ErrRootPayload  = errors.New("harvest: invalid durable root payload")
	ErrPriorResult  = errors.New("harvest: invalid prior typed result")
)

// Stable classes carried by the Participant adapter and WorkRecord. A Player
// reward rejection is terminal only when the class proves that the durable
// invocation identity conflicts with this WorkRecord; reconciliation and all
// other non-success results remain replayable.
const (
	ErrorClassParticipantIdempotencyConflict = "PARTICIPANT_IDEMPOTENCY_CONFLICT"
	ErrorClassIdempotencyConflict            = "ERR_IDEMPOTENCY_CONFLICT"
	ErrorClassReconcileRequired              = "RECONCILE_REQUIRED"
	ErrorClassParticipantUnavailableNotSent  = "PARTICIPANT_UNAVAILABLE_NOT_SENT"
)

// HarvestInput is the trusted, already-admitted Harvest intent. Plot and
// cycle values come from the current Farm view/command admission; the
// Coordinator never derives a reward from them.
type HarvestInput struct {
	SubjectPlayerID     uint64
	OperationID         string
	TargetFarmID        uint64
	PlotID              uint32
	CropCycleID         string
	ExpectedPlotVersion uint64
}

// RootIntent is the immutable canonical root carried by the WorkRecord. The
// root payload is also the exact typed Farm payload for COMMIT_HARVEST; no
// reward quantity, experience, or compendium result is put in this root.
type RootIntent struct {
	Key          identity.EffectiveIdempotencyKey
	WorkflowID   string
	Payload      *contractspb.CommitHarvestPayload
	PayloadBytes []byte
	PayloadHash  [32]byte
}

// BuildRoot derives the root-scoped v2 workflow ID and freezes the complete
// trusted Harvest command before any Participant effect can be attempted.
func BuildRoot(input HarvestInput) (RootIntent, error) {
	if err := validateInput(input); err != nil {
		return RootIntent{}, err
	}
	key := identity.EffectiveIdempotencyKey{
		SubjectPlayerID:       input.SubjectPlayerID,
		StableCommandTypeName: string(identity.WorkTypeHarvest),
		OperationID:           input.OperationID,
		Root: identity.OperationRootRef{
			Kind:        identity.RootKindFarmActor,
			CanonicalID: strconv.FormatUint(input.TargetFarmID, 10),
		},
	}
	workflowID, err := identity.WorkflowID(key)
	if err != nil {
		return RootIntent{}, fmt.Errorf("%w: workflow id: %v", ErrInvalidInput, err)
	}
	payload := &contractspb.CommitHarvestPayload{
		OwnerPlayerId:       input.SubjectPlayerID,
		FarmId:              input.TargetFarmID,
		PlotId:              input.PlotID,
		CropCycleId:         input.CropCycleID,
		ExpectedPlotVersion: input.ExpectedPlotVersion,
	}
	payloadBytes, err := deterministicProto(payload)
	if err != nil {
		return RootIntent{}, fmt.Errorf("%w: marshal: %v", ErrInvalidInput, err)
	}
	payloadHash, err := contracts.CanonicalHash(payload)
	if err != nil {
		return RootIntent{}, fmt.Errorf("%w: root hash: %v", ErrInvalidInput, err)
	}
	return RootIntent{
		Key:          key,
		WorkflowID:   workflowID,
		Payload:      proto.Clone(payload).(*contractspb.CommitHarvestPayload),
		PayloadBytes: append([]byte(nil), payloadBytes...),
		PayloadHash:  payloadHash,
	}, nil
}

// BuildRootPayload is the compact root builder used by callers that only
// need the durable bytes and canonical payload hash.
func BuildRootPayload(input HarvestInput) ([]byte, [32]byte, error) {
	root, err := BuildRoot(input)
	if err != nil {
		return nil, [32]byte{}, err
	}
	return append([]byte(nil), root.PayloadBytes...), root.PayloadHash, nil
}

// Address returns the independent workflow placement. The physical bucket
// is resolved by composition, not guessed by this scenario package.
func (r RootIntent) Address(bucketVersion, bucketID uint32) (workidentity.WorkAddress, error) {
	if bucketVersion == 0 {
		return workidentity.WorkAddress{}, fmt.Errorf("%w: bucket version", ErrInvalidInput)
	}
	address := workidentity.WorkAddress{
		WorkID:        r.WorkflowID,
		WorkType:      string(identity.WorkTypeHarvest),
		PlacementKind: workidentity.IndependentWorkBucket,
		BucketVersion: bucketVersion,
		BucketID:      bucketID,
	}
	if err := address.ValidateShape(); err != nil {
		return workidentity.WorkAddress{}, fmt.Errorf("%w: work address: %v", ErrInvalidInput, err)
	}
	return address, nil
}

// AddressForResolver resolves and validates the frozen W31 independent-work
// placement for this workflow.
func (r RootIntent) AddressForResolver(bucketVersion uint32, resolver workidentity.BucketResolver) (workidentity.WorkAddress, error) {
	if resolver == nil || bucketVersion == 0 {
		return workidentity.WorkAddress{}, fmt.Errorf("%w: bucket resolver and version", ErrInvalidInput)
	}
	bucketID, err := resolver.IndependentWorkBucket(r.WorkflowID, bucketVersion)
	if err != nil {
		return workidentity.WorkAddress{}, fmt.Errorf("%w: resolve bucket: %w", ErrInvalidInput, err)
	}
	address, err := r.Address(bucketVersion, bucketID)
	if err != nil {
		return workidentity.WorkAddress{}, err
	}
	if err := address.Validate(resolver); err != nil {
		return workidentity.WorkAddress{}, fmt.Errorf("%w: validate bucket: %w", ErrInvalidInput, err)
	}
	return address, nil
}

func validateInput(input HarvestInput) error {
	if input.SubjectPlayerID == 0 || input.TargetFarmID == 0 || input.PlotID == 0 ||
		input.PlotID > maxHarvestPlotID || input.ExpectedPlotVersion == 0 ||
		input.OperationID == "" || strings.TrimSpace(input.CropCycleID) == "" {
		return fmt.Errorf("%w: required fields", ErrInvalidInput)
	}
	if input.TargetFarmID != input.SubjectPlayerID {
		return fmt.Errorf("%w: Harvest farm must be owned by subject player", ErrInvalidInput)
	}
	if workidentity.ValidateStableOpaqueID(input.CropCycleID) != nil {
		return fmt.Errorf("%w: crop cycle id must be stable", ErrInvalidInput)
	}
	key := identity.EffectiveIdempotencyKey{
		SubjectPlayerID:       input.SubjectPlayerID,
		StableCommandTypeName: string(identity.WorkTypeHarvest),
		OperationID:           input.OperationID,
		Root: identity.OperationRootRef{
			Kind:        identity.RootKindFarmActor,
			CanonicalID: strconv.FormatUint(input.TargetFarmID, 10),
		},
	}
	if _, err := key.OperationDigest(); err != nil {
		return fmt.Errorf("%w: idempotency key: %v", ErrInvalidInput, err)
	}
	return nil
}

// Definition is the frozen two-step Harvest plan. The effect ports remain
// typed at composition time and contain no concrete Actor implementation.
type Definition struct {
	player workflow.ParticipantInvoker
	farm   workflow.ParticipantInvoker
}

var _ workflow.Definition = (*Definition)(nil)

// NewDefinition constructs COMMIT_HARVEST(Farm) →
// GRANT_HARVEST_REWARD(Player).
func NewDefinition(player, farm workflow.ParticipantInvoker) (*Definition, error) {
	if player == nil || farm == nil {
		return nil, fmt.Errorf("%w: participant invokers required", ErrInvalidInput)
	}
	return &Definition{player: player, farm: farm}, nil
}

func (*Definition) WorkType() string { return string(identity.WorkTypeHarvest) }

func (*Definition) StepNames() []string {
	return []string{string(identity.StepCommitHarvest), string(identity.StepGrantHarvestReward)}
}

func (d *Definition) Invoker(stepIndex int) workflow.ParticipantInvoker {
	if d == nil {
		return nil
	}
	switch stepIndex {
	case 0:
		return dependencyBoundInvoker{delegate: d.farm, stepIndex: stepIndex}
	case 1:
		return dependencyBoundInvoker{delegate: d.player, stepIndex: stepIndex}
	default:
		return nil
	}
}

// dependencyBoundInvoker makes the Participant dependency hash come from the
// durable typed payload. This keeps the Farm Result Hash binding explicit on
// every Coordinator/Worker replay. It also turns a Player rejection that is
// not an explicit idempotency conflict into UNKNOWN before the generic Engine
// can fold it into FAILED. The generic Engine has no business-specific
// "retryable rejected" state; Harvest therefore normalizes that result at its
// own Participant boundary.
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
	result, err := i.delegate.Invoke(ctx, call)
	if err != nil {
		return workflow.StepResult{}, err
	}
	return normalizeParticipantStepResult(i.stepIndex, result), nil
}

func normalizeParticipantStepResult(stepIndex int, result workflow.StepResult) workflow.StepResult {
	if stepIndex != 1 || result.Outcome != workflow.StepOutcomeRejected || isIrrecoverablePlayerConflict(result.ErrorClass) {
		return result
	}
	result.Outcome = workflow.StepOutcomeTimeoutUnknown
	result.Result = nil
	result.ResultHash = [32]byte{}
	if strings.TrimSpace(result.ErrorClass) == "" {
		result.ErrorClass = ErrorClassReconcileRequired
	}
	return result
}

func isIrrecoverablePlayerConflict(errorClass string) bool {
	switch strings.ToUpper(strings.TrimSpace(errorClass)) {
	case ErrorClassParticipantIdempotencyConflict, ErrorClassIdempotencyConflict, "IDEMPOTENCY_CONFLICT":
		return true
	default:
		return false
	}
}

func dependencyHashFromPayload(stepIndex int, payload []byte) ([32]byte, error) {
	if stepIndex == 0 {
		return [32]byte{}, nil
	}
	if stepIndex != 1 {
		return [32]byte{}, fmt.Errorf("%w: step index %d", ErrInvalidInput, stepIndex)
	}
	message := &contractspb.GrantHarvestRewardPayload{}
	if err := proto.Unmarshal(payload, message); err != nil {
		return [32]byte{}, fmt.Errorf("%w: GrantHarvestReward payload: %v", ErrPriorResult, err)
	}
	if len(message.GetCommitHarvestResultHash()) != 32 {
		return [32]byte{}, fmt.Errorf("%w: Farm result dependency hash", ErrPriorResult)
	}
	var result [32]byte
	copy(result[:], message.GetCommitHarvestResultHash())
	return result, nil
}

func commandForStep(step string) (contractspb.ParticipantCommandType, error) {
	switch step {
	case string(identity.StepCommitHarvest):
		return contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_HARVEST, nil
	case string(identity.StepGrantHarvestReward):
		return contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD, nil
	default:
		return contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_UNSPECIFIED, fmt.Errorf("%w: unknown Harvest step %q", ErrInvalidInput, step)
	}
}

// BuildPayload always reconstructs the current step from the frozen root and
// durable prior StepRecord. It never reads process-local result state.
func (d *Definition) BuildPayload(record *workflow.WorkRecord, stepIndex int) ([]byte, error) {
	root, err := DecodeRoot(record)
	if err != nil {
		return nil, err
	}
	switch stepIndex {
	case 0:
		return deterministicProto(root)
	case 1:
		if err := requireFarmCommitted(record); err != nil {
			return nil, err
		}
		farmResult, farmHash, err := DecodeCommitHarvestResult(record)
		if err != nil {
			return nil, err
		}
		if err := validateCommitHarvestResult(root, farmResult); err != nil {
			return nil, err
		}
		if farmResult.GetHarvestDecision().GetWorkflowId() != record.Address.WorkID {
			return nil, fmt.Errorf("%w: Farm result workflow identity", ErrPriorResult)
		}
		return deterministicProto(&contractspb.GrantHarvestRewardPayload{
			PlayerId:                root.GetOwnerPlayerId(),
			CommitHarvestResultHash: append([]byte(nil), farmHash[:]...),
			HarvestDecision:         proto.Clone(farmResult.GetHarvestDecision()).(*contractspb.HarvestDecision),
		})
	default:
		return nil, fmt.Errorf("%w: step index %d", ErrInvalidInput, stepIndex)
	}
}

// Decide folds one terminal Participant response. Farm rejection/no-change
// are final and never reach Player. Player results are normalized by
// dependencyBoundInvoker: only an explicit idempotency conflict is allowed to
// become a terminal rejection; reconciliation stays UNKNOWN and is replayed.
func (*Definition) Decide(record *workflow.WorkRecord, stepIndex int, result workflow.StepResult) (workflow.Decision, error) {
	switch stepIndex {
	case 0:
		switch result.Outcome {
		case workflow.StepOutcomeApplied:
			return workflow.Decision{NextStep: 1}, nil
		case workflow.StepOutcomeRejected, workflow.StepOutcomeNoChange:
			return workflow.Decision{Terminal: workflow.WorkStateRejected}, nil
		default:
			return workflow.Decision{}, fmt.Errorf("%w: Farm outcome %d", ErrInvalidInput, result.Outcome)
		}
	case 1:
		switch result.Outcome {
		case workflow.StepOutcomeApplied, workflow.StepOutcomeNoChange:
			if err := validateGrantStepResult(record, result); err != nil {
				return workflow.Decision{}, err
			}
			return workflow.Decision{Terminal: workflow.WorkStateCompleted}, nil
		case workflow.StepOutcomeRejected:
			if isIrrecoverablePlayerConflict(result.ErrorClass) {
				return workflow.Decision{Terminal: workflow.WorkStateRejected}, nil
			}
			return workflow.Decision{}, fmt.Errorf("%w: unnormalized recoverable Player rejection %q", ErrPriorResult, result.ErrorClass)
		default:
			return workflow.Decision{}, fmt.Errorf("%w: Player outcome %d", ErrInvalidInput, result.Outcome)
		}
	default:
		return workflow.Decision{}, fmt.Errorf("%w: step index %d", ErrInvalidInput, stepIndex)
	}
}

// Coordinator is the scenario-facing wrapper around the one generic Engine.
// Create is always completed before Resume can invoke a Participant.
type Coordinator struct {
	repository     workflow.WorkRepository
	definition     *Definition
	engine         *workflow.Engine
	clock          func() time.Time
	deadline       time.Duration
	bucketVersion  uint32
	bucketResolver workidentity.BucketResolver
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
		return nil, fmt.Errorf("%w: repository, bucket resolver and bucket version", ErrInvalidInput)
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
	return &Coordinator{
		repository: config.Repository, definition: definition, engine: engine,
		clock: clock, deadline: deadline, bucketVersion: config.BucketVersion,
		bucketResolver: config.BucketResolver,
	}, nil
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

// Create performs only canonical root construction, placement, and
// CreateIfAbsent. It has no Participant side effect.
func (c *Coordinator) Create(ctx context.Context, input HarvestInput) (*workflow.WorkRecord, bool, error) {
	if c == nil || c.engine == nil {
		return nil, false, fmt.Errorf("%w: coordinator", ErrInvalidInput)
	}
	root, err := BuildRoot(input)
	if err != nil {
		return nil, false, err
	}
	address, err := root.AddressForResolver(c.bucketVersion, c.bucketResolver)
	if err != nil {
		return nil, false, err
	}
	return c.engine.Create(ctx, c.definition, address, input.SubjectPlayerID, input.OperationID,
		root.PayloadBytes, root.PayloadHash, c.clock().Add(c.deadline))
}

// Resume advances an existing record without a Worker lease. Worker recovery
// supplies the LeaseFence through Engine.Resume directly.
func (c *Coordinator) Resume(ctx context.Context, record *workflow.WorkRecord) error {
	if c == nil || c.engine == nil {
		return fmt.Errorf("%w: coordinator", ErrInvalidInput)
	}
	return c.engine.Resume(ctx, record, workflow.LeaseFence{}, nil)
}

// Start creates/loads the durable root and performs one online resume pass.
// A retry-scheduled error is returned with the durable record so callers can
// expose PROCESSING without changing operation identity.
func (c *Coordinator) Start(ctx context.Context, input HarvestInput) (*workflow.WorkRecord, bool, error) {
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

// DecodeRoot validates the immutable root before any typed downstream
// payload is built.
func DecodeRoot(record *workflow.WorkRecord) (*contractspb.CommitHarvestPayload, error) {
	if record == nil {
		return nil, fmt.Errorf("%w: nil record", ErrRootPayload)
	}
	if record.Address.WorkType != string(identity.WorkTypeHarvest) {
		return nil, fmt.Errorf("%w: work type", ErrRootPayload)
	}
	root := &contractspb.CommitHarvestPayload{}
	if err := proto.Unmarshal(record.Payload, root); err != nil {
		return nil, fmt.Errorf("%w: protobuf: %v", ErrRootPayload, err)
	}
	canonical, err := contracts.CanonicalHash(root)
	if err != nil || canonical != record.PayloadHash {
		return nil, fmt.Errorf("%w: root canonical hash", ErrRootPayload)
	}
	deterministic, err := deterministicProto(root)
	if err != nil || !bytes.Equal(deterministic, record.Payload) {
		return nil, fmt.Errorf("%w: root bytes are not frozen deterministic protobuf", ErrRootPayload)
	}
	if root.GetOwnerPlayerId() == 0 || root.GetOwnerPlayerId() != record.SubjectPlayerID ||
		root.GetFarmId() != root.GetOwnerPlayerId() || root.GetPlotId() == 0 ||
		root.GetPlotId() > maxHarvestPlotID || root.GetCropCycleId() == "" ||
		workidentity.ValidateStableOpaqueID(root.GetCropCycleId()) != nil || root.GetExpectedPlotVersion() == 0 {
		return nil, fmt.Errorf("%w: root fields", ErrRootPayload)
	}
	key := identity.EffectiveIdempotencyKey{
		SubjectPlayerID:       record.SubjectPlayerID,
		StableCommandTypeName: string(identity.WorkTypeHarvest),
		OperationID:           record.OperationID,
		Root: identity.OperationRootRef{
			Kind:        identity.RootKindFarmActor,
			CanonicalID: strconv.FormatUint(root.GetFarmId(), 10),
		},
	}
	wantWorkflowID, err := identity.WorkflowID(key)
	if err != nil || wantWorkflowID != record.Address.WorkID {
		return nil, fmt.Errorf("%w: workflow identity", ErrRootPayload)
	}
	return root, nil
}

// DecodeCommitHarvestResult validates and returns the durable Farm typed
// result and its Participant Result Hash.
func DecodeCommitHarvestResult(record *workflow.WorkRecord) (*contractspb.CommitHarvestResult, [32]byte, error) {
	result := &contractspb.CommitHarvestResult{}
	hash, err := durableResultHash(record, 0, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_HARVEST, result)
	if err != nil {
		return nil, [32]byte{}, err
	}
	return result, hash, nil
}

// DecodeGrantHarvestRewardResult validates and returns the durable Player
// typed result and its Participant Result Hash.
func DecodeGrantHarvestRewardResult(record *workflow.WorkRecord) (*contractspb.GrantHarvestRewardResult, [32]byte, error) {
	result := &contractspb.GrantHarvestRewardResult{}
	hash, err := durableResultHash(record, 1, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD, result)
	if err != nil {
		return nil, [32]byte{}, err
	}
	return result, hash, nil
}

// DecodeResults returns both typed terminal results after validating the
// Farm→Player binding. It is used by online result projection and tests.
func DecodeResults(record *workflow.WorkRecord) (*contractspb.CommitHarvestResult, *contractspb.GrantHarvestRewardResult, error) {
	root, err := DecodeRoot(record)
	if err != nil {
		return nil, nil, err
	}
	farm, _, err := DecodeCommitHarvestResult(record)
	if err != nil {
		return nil, nil, err
	}
	if err := validateCommitHarvestResult(root, farm); err != nil {
		return nil, nil, err
	}
	if farm.GetHarvestDecision().GetWorkflowId() != record.Address.WorkID {
		return nil, nil, fmt.Errorf("%w: Farm result workflow identity", ErrPriorResult)
	}
	grant, _, err := DecodeGrantHarvestRewardResult(record)
	if err != nil {
		return nil, nil, err
	}
	if err := validateGrantHarvestResult(root, farm.GetHarvestDecision(), grant); err != nil {
		return nil, nil, err
	}
	return farm, grant, nil
}

func durableResultHash(record *workflow.WorkRecord, index int, command contractspb.ParticipantCommandType, result proto.Message) ([32]byte, error) {
	if err := unmarshalDurableResult(record, index, command, result); err != nil {
		return [32]byte{}, err
	}
	return record.Steps[index].ResultHash, nil
}

func requireFarmCommitted(record *workflow.WorkRecord) error {
	if record == nil || len(record.Steps) < 2 {
		return fmt.Errorf("%w: Farm step is missing", ErrPriorResult)
	}
	if record.Steps[0].Status != workflow.StepApplied {
		return fmt.Errorf("%w: Farm step is not committed, got %s", ErrPriorResult, record.Steps[0].Status)
	}
	return nil
}

func unmarshalDurableResult(record *workflow.WorkRecord, index int, command contractspb.ParticipantCommandType, result proto.Message) error {
	if record == nil || index < 0 || index >= len(record.Steps) || result == nil {
		return fmt.Errorf("%w: step %d", ErrPriorResult, index)
	}
	step := &record.Steps[index]
	if !workflow.StepTerminal(step.Status) || len(step.Result) == 0 || step.ResultHash == ([32]byte{}) {
		return fmt.Errorf("%w: step %d is missing terminal typed result", ErrPriorResult, index)
	}
	if err := proto.Unmarshal(step.Result, result); err != nil {
		return fmt.Errorf("%w: step %d protobuf: %v", ErrPriorResult, index, err)
	}
	deterministic, err := deterministicProto(result)
	if err != nil || !bytes.Equal(deterministic, step.Result) {
		return fmt.Errorf("%w: step %d result bytes", ErrPriorResult, index)
	}
	spec, err := contracts.LookupParticipantCommand(command)
	if err != nil {
		return fmt.Errorf("%w: step %d command: %v", ErrPriorResult, index, err)
	}
	hash, err := contracts.CanonicalParticipantHash(spec, result)
	if err != nil || hash != step.ResultHash {
		return fmt.Errorf("%w: step %d result hash", ErrPriorResult, index)
	}
	return nil
}

func validateCommitHarvestResult(root *contractspb.CommitHarvestPayload, result *contractspb.CommitHarvestResult) error {
	if root == nil || result == nil || result.GetHarvestDecision() == nil {
		return fmt.Errorf("%w: Farm result/decision missing", ErrPriorResult)
	}
	decision := result.GetHarvestDecision()
	if decision.GetWorkflowId() == "" {
		return fmt.Errorf("%w: Farm result workflow identity", ErrPriorResult)
	}
	if decision.GetOwnerPlayerId() != root.GetOwnerPlayerId() || decision.GetFarmId() != root.GetFarmId() ||
		decision.GetPlotId() != root.GetPlotId() || decision.GetCropCycleId() != root.GetCropCycleId() ||
		decision.GetCropDefinitionId() == 0 || decision.GetRewardItemId() == 0 ||
		decision.GetRewardQuantity() > math.MaxUint32 || decision.GetCropRuleVersion() == 0 ||
		decision.GetCompendiumUnlockSnapshot() == nil || len(decision.GetHarvestDecisionHash()) != 32 ||
		result.GetCommittedPlotVersion() == 0 || result.GetCommittedFarmVersion() == 0 || result.GetFarmRecordVersion() == 0 {
		return fmt.Errorf("%w: Farm result identity or versions", ErrPriorResult)
	}
	snapshot := decision.GetCompendiumUnlockSnapshot()
	if snapshot.GetCropDefinitionId() != decision.GetCropDefinitionId() || snapshot.GetPlantedByPlayerId() != root.GetOwnerPlayerId() {
		return fmt.Errorf("%w: compendium snapshot binding", ErrPriorResult)
	}
	if err := validateHarvestDecisionHash(decision); err != nil {
		return err
	}
	return nil
}

func validateHarvestDecisionHash(decision *contractspb.HarvestDecision) error {
	if decision == nil || len(decision.GetHarvestDecisionHash()) != 32 {
		return fmt.Errorf("%w: HarvestDecision hash", ErrPriorResult)
	}
	withoutHash := proto.Clone(decision).(*contractspb.HarvestDecision)
	withoutHash.HarvestDecisionHash = nil
	want, err := contracts.CanonicalHash(withoutHash)
	if err != nil || !bytes.Equal(want[:], decision.GetHarvestDecisionHash()) {
		return fmt.Errorf("%w: HarvestDecision hash mismatch", ErrPriorResult)
	}
	return nil
}

func validateGrantStepResult(record *workflow.WorkRecord, result workflow.StepResult) error {
	root, err := DecodeRoot(record)
	if err != nil {
		return err
	}
	farm, _, err := DecodeCommitHarvestResult(record)
	if err != nil {
		return err
	}
	if err := validateCommitHarvestResult(root, farm); err != nil {
		return err
	}
	if farm.GetHarvestDecision().GetWorkflowId() != record.Address.WorkID {
		return fmt.Errorf("%w: Farm result workflow identity", ErrPriorResult)
	}
	if len(result.Result) == 0 || result.ResultHash == ([32]byte{}) {
		return fmt.Errorf("%w: Player typed result missing", ErrPriorResult)
	}
	grant := &contractspb.GrantHarvestRewardResult{}
	if err := proto.Unmarshal(result.Result, grant); err != nil {
		return fmt.Errorf("%w: Player protobuf: %v", ErrPriorResult, err)
	}
	deterministic, err := deterministicProto(grant)
	if err != nil || !bytes.Equal(deterministic, result.Result) {
		return fmt.Errorf("%w: Player result bytes", ErrPriorResult)
	}
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD)
	if err != nil {
		return fmt.Errorf("%w: Player command: %v", ErrPriorResult, err)
	}
	hash, err := contracts.CanonicalParticipantHash(spec, grant)
	if err != nil || hash != result.ResultHash {
		return fmt.Errorf("%w: Player result hash", ErrPriorResult)
	}
	return validateGrantHarvestResult(root, farm.GetHarvestDecision(), grant)
}

func validateGrantHarvestResult(root *contractspb.CommitHarvestPayload, decision *contractspb.HarvestDecision, result *contractspb.GrantHarvestRewardResult) error {
	if root == nil || decision == nil || result == nil || result.GetCropCycleId() != decision.GetCropCycleId() ||
		result.GetRewardItemId() != decision.GetRewardItemId() || result.GetGrantedQuantity() != decision.GetRewardQuantity() ||
		result.GetGrantedExp() != decision.GetRewardExp() || result.GetCompendiumUnlockDelta() == nil || result.GetPlayerRecordVersion() == 0 {
		return fmt.Errorf("%w: Player result identity", ErrPriorResult)
	}
	delta := result.GetCompendiumUnlockDelta()
	if delta.GetNewlyUnlocked() {
		if delta.GetCropDefinitionId() == 0 || delta.GetCropDefinitionId() != decision.GetCropDefinitionId() || delta.GetCompendiumRevisionAfter() == 0 {
			return fmt.Errorf("%w: new compendium delta", ErrPriorResult)
		}
	} else if delta.CropDefinitionId != nil {
		return fmt.Errorf("%w: non-new compendium delta carries crop", ErrPriorResult)
	}
	return nil
}

// Outcome is the public semantic class derived from durable WorkRecord state.
// It deliberately distinguishes Farm NO_CHANGE from a generic rejection.
type Outcome uint8

const (
	OutcomeProcessing Outcome = iota
	OutcomeSucceeded
	OutcomeRejected
	OutcomeNoChange
	OutcomeFailed
)

func (o Outcome) String() string {
	switch o {
	case OutcomeProcessing:
		return "PROCESSING"
	case OutcomeSucceeded:
		return "SUCCEEDED"
	case OutcomeRejected:
		return "REJECTED"
	case OutcomeNoChange:
		return "NO_CHANGE"
	case OutcomeFailed:
		return "FAILED"
	default:
		return "UNSPECIFIED"
	}
}

// OutcomeFor maps only durable workflow facts. A Farm commit with a pending
// Player step remains PROCESSING, even if the Farm projection is already empty.
func OutcomeFor(record *workflow.WorkRecord) Outcome {
	if record == nil || !record.Terminal() {
		return OutcomeProcessing
	}
	if record.State == workflow.WorkStateCompleted {
		return OutcomeSucceeded
	}
	if record.State == workflow.WorkStateRejected && len(record.Steps) > 0 {
		if record.Steps[0].Status == workflow.StepNoChange {
			return OutcomeNoChange
		}
		return OutcomeRejected
	}
	return OutcomeFailed
}

func deterministicProto(message proto.Message) ([]byte, error) {
	if message == nil {
		return nil, fmt.Errorf("%w: nil protobuf", ErrInvalidInput)
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(message)
}
