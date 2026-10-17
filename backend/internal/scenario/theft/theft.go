// Package theft implements the durable THEFT cross-authority state machine.
//
// The package owns only workflow composition and the frozen typed payload
// bindings. Farm and Player business effects stay behind ParticipantInvoker;
// WorkRepository remains the durable recovery boundary.
package theft

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	contractspb "farmgame/api/gen/contracts"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/contracts"
	"farmgame/internal/identity"
	"farmgame/internal/workflow"
	"farmgame/internal/workidentity"

	"google.golang.org/protobuf/proto"
)

const (
	defaultWorkflowDeadline = 24 * time.Hour
	defaultMaxStepAttempts  = 4
	maxTheftPlotID          = 24
	maxOperationIDBytes     = 64
)

var (
	ErrInvalidInput   = errors.New("theft: invalid input")
	ErrRootPayload    = errors.New("theft: invalid durable root payload")
	ErrPriorResult    = errors.New("theft: invalid prior typed result")
	ErrRetryExhausted = errors.New("theft: participant retry budget exhausted")
	ErrTerminalResult = errors.New("theft: terminal result is unavailable")
)

const (
	ErrorClassParticipantIdempotencyConflict = "PARTICIPANT_IDEMPOTENCY_CONFLICT"
	ErrorClassIdempotencyConflict            = "ERR_IDEMPOTENCY_CONFLICT"
	ErrorClassReconcileRequired              = "RECONCILE_REQUIRED"
)

// ParticipantClient is the existing typed Participant port expressed through
// the generic workflow call envelope. The concrete RPC adapters and the fake
// participants both implement this interface.
type ParticipantClient = workflow.ParticipantInvoker

// TheftInput is trusted, already-admitted command data. In particular, the
// ActionPermitSnapshot is supplied by the Friendship authority; callers must
// not manufacture it from public request bytes.
type TheftInput struct {
	VisitorPlayerID uint64
	OwnerPlayerID   uint64
	TargetFarmID    uint64
	PlotID          uint32
	CropCycleID     string
	OperationID     string

	ActionPermitSnapshot *contractspb.ActionPermitSnapshot
	// AdmissionHash is the frozen target-binding hash from Action Admission.
	// Empty means "use the permit's target_binding_hash"; a supplied value must
	// match it exactly.
	AdmissionHash []byte
	// WorkflowDeadlineAtMs is the independent absolute deadline frozen by
	// FRIEND_PAIR at Execution-Accept. Production dispatch and worker recovery
	// must pass the same value; zero is retained only for isolated scenario
	// fixtures that do not originate from Pair admission.
	WorkflowDeadlineAtMs int64
}

// Input is retained as a concise scenario-facing spelling.
type Input = TheftInput

// RootIntent is the immutable root which is stored in WorkRecord.Payload.
// PayloadHash is the canonical hash of DecideTheftPayload and is distinct
// from the Action Permit's canonical_payload_hash.
type RootIntent struct {
	Key                 identity.EffectiveIdempotencyKey
	WorkflowID          string
	SettlementID        string
	TheftAttemptID      string
	Payload             *contractspb.DecideTheftPayload
	PayloadBytes        []byte
	PayloadHash         [32]byte
	ActionPermitHash    [32]byte
	AdmissionHash       []byte
	ExecutionAcceptedAt int64
}

// BuildRoot derives all deterministic identities and freezes the complete
// ActionPermit/Admission evidence before CreateIfAbsent is attempted.
func BuildRoot(input TheftInput) (RootIntent, error) {
	if err := validateInput(input); err != nil {
		return RootIntent{}, err
	}
	permit := proto.Clone(input.ActionPermitSnapshot).(*contractspb.ActionPermitSnapshot)
	permitHash, err := contracts.CanonicalHash(permit)
	if err != nil {
		return RootIntent{}, fmt.Errorf("%w: action permit hash: %v", ErrInvalidInput, err)
	}

	pairID, err := identity.CanonicalFriendRelationID(input.VisitorPlayerID, input.OwnerPlayerID)
	if err != nil {
		return RootIntent{}, fmt.Errorf("%w: pair identity: %v", ErrInvalidInput, err)
	}
	key := identity.EffectiveIdempotencyKey{
		SubjectPlayerID:       input.VisitorPlayerID,
		StableCommandTypeName: string(identity.WorkTypeTheft),
		OperationID:           input.OperationID,
		Root: identity.OperationRootRef{
			Kind:        identity.RootKindFriendPair,
			CanonicalID: pairID,
		},
	}
	workflowID, err := identity.WorkflowID(key)
	if err != nil {
		return RootIntent{}, fmt.Errorf("%w: workflow id: %v", ErrInvalidInput, err)
	}
	settlementID, err := identity.TheftSettlementID(workflowID)
	if err != nil {
		return RootIntent{}, fmt.Errorf("%w: settlement id: %v", ErrInvalidInput, err)
	}
	attemptID, err := identity.TheftAttemptID(input.TargetFarmID, input.CropCycleID, input.VisitorPlayerID)
	if err != nil {
		return RootIntent{}, fmt.Errorf("%w: theft attempt id: %v", ErrInvalidInput, err)
	}

	payload := &contractspb.DecideTheftPayload{
		VisitorPlayerId:          input.VisitorPlayerID,
		OwnerPlayerId:            input.OwnerPlayerID,
		FarmId:                   input.TargetFarmID,
		PlotId:                   input.PlotID,
		CropCycleId:              input.CropCycleID,
		TheftAttemptId:           attemptID,
		SettlementId:             settlementID,
		ActionPermitSnapshot:     permit,
		ActionPermitSnapshotHash: append([]byte(nil), permitHash[:]...),
	}
	payloadBytes, err := deterministicProto(payload)
	if err != nil {
		return RootIntent{}, fmt.Errorf("%w: root bytes: %v", ErrInvalidInput, err)
	}
	payloadHash, err := contracts.CanonicalHash(payload)
	if err != nil {
		return RootIntent{}, fmt.Errorf("%w: root hash: %v", ErrInvalidInput, err)
	}
	return RootIntent{
		Key:                 key,
		WorkflowID:          workflowID,
		SettlementID:        settlementID,
		TheftAttemptID:      attemptID,
		Payload:             proto.Clone(payload).(*contractspb.DecideTheftPayload),
		PayloadBytes:        append([]byte(nil), payloadBytes...),
		PayloadHash:         payloadHash,
		ActionPermitHash:    permitHash,
		AdmissionHash:       append([]byte(nil), permit.GetTargetBindingHash()...),
		ExecutionAcceptedAt: permit.GetExecutionAcceptedAtAuthorityMs(),
	}, nil
}

// BuildRootPayload is useful to admission code which only needs the durable
// root bytes and canonical hash.
func BuildRootPayload(input TheftInput) ([]byte, [32]byte, error) {
	root, err := BuildRoot(input)
	if err != nil {
		return nil, [32]byte{}, err
	}
	return append([]byte(nil), root.PayloadBytes...), root.PayloadHash, nil
}

func validateInput(input TheftInput) error {
	if input.VisitorPlayerID == 0 || input.OwnerPlayerID == 0 || input.TargetFarmID == 0 ||
		input.VisitorPlayerID == input.OwnerPlayerID || input.TargetFarmID != input.OwnerPlayerID ||
		input.PlotID == 0 || input.PlotID > maxTheftPlotID ||
		strings.TrimSpace(input.CropCycleID) == "" || strings.TrimSpace(input.OperationID) == "" ||
		len(input.OperationID) > maxOperationIDBytes || input.ActionPermitSnapshot == nil {
		return fmt.Errorf("%w: required identity fields", ErrInvalidInput)
	}
	if workidentity.ValidateStableOpaqueID(input.CropCycleID) != nil {
		return fmt.Errorf("%w: crop cycle id", ErrInvalidInput)
	}
	permit := input.ActionPermitSnapshot
	if workidentity.ValidateStableOpaqueID(permit.GetPermitId()) != nil ||
		permit.GetVisitorPlayerId() != input.VisitorPlayerID ||
		permit.GetOwnerPlayerId() != input.OwnerPlayerID ||
		permit.GetTargetFarmId() != input.TargetFarmID ||
		permit.GetOperationId() != input.OperationID ||
		permit.GetActionScope() != contractspb.FriendActionScope_FRIEND_ACTION_SCOPE_THEFT ||
		permit.GetFriendRelationEpoch() == 0 ||
		len(permit.GetCanonicalPayloadHash()) != sha256.Size ||
		len(permit.GetTargetBindingHash()) != sha256.Size ||
		permit.GetIssuedAtAuthorityMs() <= 0 ||
		permit.GetExpireAtAuthorityMs() <= permit.GetIssuedAtAuthorityMs() ||
		permit.GetExecutionAcceptedAtAuthorityMs() < permit.GetIssuedAtAuthorityMs() ||
		permit.GetExecutionAcceptedAtAuthorityMs() >= permit.GetExpireAtAuthorityMs() {
		return fmt.Errorf("%w: action permit evidence", ErrInvalidInput)
	}
	if len(input.AdmissionHash) != 0 && !bytes.Equal(input.AdmissionHash, permit.GetTargetBindingHash()) {
		return fmt.Errorf("%w: admission hash mismatch", ErrInvalidInput)
	}
	pairID, err := identity.CanonicalFriendRelationID(input.VisitorPlayerID, input.OwnerPlayerID)
	if err != nil {
		return fmt.Errorf("%w: pair identity: %v", ErrInvalidInput, err)
	}
	key := identity.EffectiveIdempotencyKey{
		SubjectPlayerID:       input.VisitorPlayerID,
		StableCommandTypeName: string(identity.WorkTypeTheft),
		OperationID:           input.OperationID,
		Root: identity.OperationRootRef{
			Kind:        identity.RootKindFriendPair,
			CanonicalID: pairID,
		},
	}
	if _, err := key.OperationDigest(); err != nil {
		return fmt.Errorf("%w: idempotency key: %v", ErrInvalidInput, err)
	}
	return nil
}

// Address returns an independent-work placement for this workflow.
func (r RootIntent) Address(bucketVersion, bucketID uint32) (workidentity.WorkAddress, error) {
	if bucketVersion == 0 {
		return workidentity.WorkAddress{}, fmt.Errorf("%w: bucket version", ErrInvalidInput)
	}
	address := workidentity.WorkAddress{
		WorkID:        r.WorkflowID,
		WorkType:      string(identity.WorkTypeTheft),
		PlacementKind: workidentity.IndependentWorkBucket,
		BucketVersion: bucketVersion,
		BucketID:      bucketID,
	}
	if err := address.ValidateShape(); err != nil {
		return workidentity.WorkAddress{}, fmt.Errorf("%w: work address: %v", ErrInvalidInput, err)
	}
	return address, nil
}

// AddressForResolver resolves and validates the versioned physical bucket.
func (r RootIntent) AddressForResolver(bucketVersion uint32, resolver workidentity.BucketResolver) (workidentity.WorkAddress, error) {
	if resolver == nil || bucketVersion == 0 {
		return workidentity.WorkAddress{}, fmt.Errorf("%w: bucket resolver/version", ErrInvalidInput)
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

// Definition is the frozen three-participant plan:
// DECIDE_THEFT(Farm) → SETTLE_THEFT_VISITOR(Player) →
// CREDIT_GUARD_INCOME(Owner Player, only when guard_coin_value > 0).
type Definition struct {
	visitor     workflow.ParticipantInvoker
	owner       workflow.ParticipantInvoker
	farm        workflow.ParticipantInvoker
	maxAttempts uint64
}

var _ workflow.Definition = (*Definition)(nil)

type DefinitionOption func(*Definition)

// WithMaxStepAttempts bounds retries of one deterministic invocation. A
// value of zero is ignored and leaves the frozen default.
func WithMaxStepAttempts(attempts uint64) DefinitionOption {
	return func(d *Definition) {
		if attempts > 0 {
			d.maxAttempts = attempts
		}
	}
}

func NewDefinition(visitor, owner, farm workflow.ParticipantInvoker, options ...DefinitionOption) (*Definition, error) {
	if visitor == nil || owner == nil || farm == nil {
		return nil, fmt.Errorf("%w: visitor, owner and farm Participant ports are required", ErrInvalidInput)
	}
	d := &Definition{visitor: visitor, owner: owner, farm: farm, maxAttempts: defaultMaxStepAttempts}
	for _, option := range options {
		if option != nil {
			option(d)
		}
	}
	return d, nil
}

// NewDefinitionWithPlayer is a convenience for compositions where visitor
// and owner use the same typed Player client.
func NewDefinitionWithPlayer(player, farm workflow.ParticipantInvoker, options ...DefinitionOption) (*Definition, error) {
	return NewDefinition(player, player, farm, options...)
}

func (*Definition) WorkType() string { return string(identity.WorkTypeTheft) }

func (*Definition) StepNames() []string {
	return []string{
		string(identity.StepDecideTheft),
		string(identity.StepSettleTheftVisitor),
		string(identity.StepCreditGuardIncome),
	}
}

func (d *Definition) Invoker(stepIndex int) workflow.ParticipantInvoker {
	if d == nil {
		return nil
	}
	var delegate workflow.ParticipantInvoker
	switch stepIndex {
	case 0:
		delegate = d.farm
	case 1:
		delegate = d.visitor
	case 2:
		delegate = d.owner
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
		message := &contractspb.SettleTheftVisitorPayload{}
		if err := proto.Unmarshal(payload, message); err != nil {
			return [32]byte{}, fmt.Errorf("%w: visitor payload: %v", ErrPriorResult, err)
		}
		dependency = message.GetDecideTheftResultHash()
	case 2:
		message := &contractspb.CreditGuardIncomePayload{}
		if err := proto.Unmarshal(payload, message); err != nil {
			return [32]byte{}, fmt.Errorf("%w: owner payload: %v", ErrPriorResult, err)
		}
		dependency = message.GetSettleVisitorResultHash()
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
	case string(identity.StepDecideTheft):
		return contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT, nil
	case string(identity.StepSettleTheftVisitor):
		return contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_SETTLE_THEFT_VISITOR, nil
	case string(identity.StepCreditGuardIncome):
		return contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_CREDIT_GUARD_INCOME, nil
	default:
		return contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_UNSPECIFIED, fmt.Errorf("%w: unknown Theft step %q", ErrInvalidInput, step)
	}
}

func (d *Definition) BuildPayload(record *workflow.WorkRecord, stepIndex int) ([]byte, error) {
	if record == nil || d == nil {
		return nil, fmt.Errorf("%w: record/definition", ErrInvalidInput)
	}
	if stepIndex < 0 || stepIndex >= len(record.Steps) {
		return nil, fmt.Errorf("%w: step index %d", ErrInvalidInput, stepIndex)
	}
	step := &record.Steps[stepIndex]
	if step.Status == workflow.StepUnknown && step.Attempts >= d.maxAttempts {
		return nil, fmt.Errorf("%w: step %s attempts=%d", ErrRetryExhausted, step.Name, step.Attempts)
	}
	root, err := DecodeRoot(record)
	if err != nil {
		return nil, err
	}
	switch stepIndex {
	case 0:
		return deterministicProto(root)
	case 1:
		farm, farmHash, err := DecodeDecideTheftResult(record)
		if err != nil {
			return nil, err
		}
		if record.Steps[0].Status != workflow.StepApplied {
			return nil, fmt.Errorf("%w: visitor step requires applied Farm decision", ErrPriorResult)
		}
		if err := validateTheftDecision(root, farm.GetTheftTargetDecision()); err != nil {
			return nil, err
		}
		return deterministicProto(&contractspb.SettleTheftVisitorPayload{
			VisitorPlayerId:       root.GetVisitorPlayerId(),
			DecideTheftResultHash: append([]byte(nil), farmHash[:]...),
			TheftTargetDecision:   proto.Clone(farm.GetTheftTargetDecision()).(*contractspb.TheftTargetDecision),
		})
	case 2:
		farm, farmHash, err := DecodeDecideTheftResult(record)
		if err != nil {
			return nil, err
		}
		if record.Steps[0].Status != workflow.StepApplied {
			return nil, fmt.Errorf("%w: owner step requires applied Farm decision", ErrPriorResult)
		}
		if err := validateTheftDecision(root, farm.GetTheftTargetDecision()); err != nil {
			return nil, err
		}
		if farm.GetTheftTargetDecision().GetGuardCoinValue() == 0 {
			return nil, fmt.Errorf("%w: owner step is not required for zero guard coin", ErrPriorResult)
		}
		if err := validateVisitorStep(record, root, farm.GetTheftTargetDecision()); err != nil {
			return nil, err
		}
		visitorHash := record.Steps[1].ResultHash
		return deterministicProto(&contractspb.CreditGuardIncomePayload{
			OwnerPlayerId:           root.GetOwnerPlayerId(),
			DecideTheftResultHash:   append([]byte(nil), farmHash[:]...),
			SettleVisitorResultHash: append([]byte(nil), visitorHash[:]...),
			TheftTargetDecision:     proto.Clone(farm.GetTheftTargetDecision()).(*contractspb.TheftTargetDecision),
		})
	default:
		return nil, fmt.Errorf("%w: step index %d", ErrInvalidInput, stepIndex)
	}
}

func (d *Definition) Decide(record *workflow.WorkRecord, stepIndex int, result workflow.StepResult) (workflow.Decision, error) {
	if record == nil || stepIndex < 0 || stepIndex >= len(record.Steps) {
		return workflow.Decision{}, fmt.Errorf("%w: decide record/step", ErrInvalidInput)
	}
	root, err := DecodeRoot(record)
	if err != nil {
		return workflow.Decision{}, err
	}
	switch stepIndex {
	case 0:
		switch result.Outcome {
		case workflow.StepOutcomeApplied:
			if err := validateFarmStepResult(root, result); err != nil {
				return workflow.Decision{}, err
			}
			farm := &contractspb.DecideTheftResult{}
			if err := decodeTypedResult(result, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT, farm); err != nil {
				return workflow.Decision{}, err
			}
			if farm.GetTheftTargetDecision().GetWorkflowId() != record.Address.WorkID {
				return workflow.Decision{}, fmt.Errorf("%w: Farm workflow identity", ErrPriorResult)
			}
			return workflow.Decision{NextStep: 1}, nil
		case workflow.StepOutcomeNoChange:
			return workflow.Decision{Terminal: workflow.WorkStateRejected}, nil
		case workflow.StepOutcomeRejected:
			if isReconcileClass(result.ErrorClass) {
				return workflow.Decision{Terminal: workflow.WorkStateFailed}, nil
			}
			return workflow.Decision{Terminal: workflow.WorkStateRejected}, nil
		default:
			return workflow.Decision{}, fmt.Errorf("%w: Farm outcome %d", ErrInvalidInput, result.Outcome)
		}
	case 1:
		switch result.Outcome {
		case workflow.StepOutcomeApplied, workflow.StepOutcomeNoChange:
			farm, _, err := DecodeDecideTheftResult(record)
			if err != nil {
				return workflow.Decision{}, err
			}
			if err := validateTheftDecision(root, farm.GetTheftTargetDecision()); err != nil {
				return workflow.Decision{}, err
			}
			if err := validateVisitorStepResult(root, farm.GetTheftTargetDecision(), result, result.Outcome == workflow.StepOutcomeApplied); err != nil {
				return workflow.Decision{}, err
			}
			if farm.GetTheftTargetDecision().GetGuardCoinValue() > 0 {
				return workflow.Decision{NextStep: 2}, nil
			}
			return workflow.Decision{Terminal: workflow.WorkStateCompleted}, nil
		case workflow.StepOutcomeRejected:
			if isReconcileClass(result.ErrorClass) {
				return workflow.Decision{Terminal: workflow.WorkStateFailed}, nil
			}
			return workflow.Decision{Terminal: workflow.WorkStateRejected}, nil
		default:
			return workflow.Decision{}, fmt.Errorf("%w: Visitor outcome %d", ErrInvalidInput, result.Outcome)
		}
	case 2:
		switch result.Outcome {
		case workflow.StepOutcomeApplied, workflow.StepOutcomeNoChange:
			farm, _, err := DecodeDecideTheftResult(record)
			if err != nil {
				return workflow.Decision{}, err
			}
			if err := validateTheftDecision(root, farm.GetTheftTargetDecision()); err != nil {
				return workflow.Decision{}, err
			}
			if farm.GetTheftTargetDecision().GetGuardCoinValue() == 0 {
				return workflow.Decision{}, fmt.Errorf("%w: owner receipt for zero guard coin", ErrPriorResult)
			}
			if err := validateVisitorStep(record, root, farm.GetTheftTargetDecision()); err != nil {
				return workflow.Decision{}, err
			}
			if err := validateOwnerStepResult(root, farm.GetTheftTargetDecision(), result, result.Outcome == workflow.StepOutcomeApplied); err != nil {
				return workflow.Decision{}, err
			}
			return workflow.Decision{Terminal: workflow.WorkStateCompleted}, nil
		case workflow.StepOutcomeRejected:
			if isReconcileClass(result.ErrorClass) {
				return workflow.Decision{Terminal: workflow.WorkStateFailed}, nil
			}
			return workflow.Decision{Terminal: workflow.WorkStateRejected}, nil
		default:
			return workflow.Decision{}, fmt.Errorf("%w: Owner outcome %d", ErrInvalidInput, result.Outcome)
		}
	default:
		return workflow.Decision{}, fmt.Errorf("%w: step index %d", ErrInvalidInput, stepIndex)
	}
}

func validateFarmStepResult(root *contractspb.DecideTheftPayload, result workflow.StepResult) error {
	farm := &contractspb.DecideTheftResult{}
	if err := decodeTypedResult(result, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT, farm); err != nil {
		return err
	}
	if farm.GetCommittedPlotVersion() == 0 || farm.GetCommittedFarmVersion() == 0 || farm.GetFarmRecordVersion() == 0 {
		return fmt.Errorf("%w: Farm versions", ErrPriorResult)
	}
	return validateTheftDecision(root, farm.GetTheftTargetDecision())
}

func validateVisitorStepResult(root *contractspb.DecideTheftPayload, decision *contractspb.TheftTargetDecision, result workflow.StepResult, required bool) error {
	if len(result.Result) == 0 {
		if required {
			return fmt.Errorf("%w: Visitor typed result missing", ErrPriorResult)
		}
		if result.ResultHash != ([32]byte{}) {
			return fmt.Errorf("%w: Visitor result hash without bytes", ErrPriorResult)
		}
		return nil
	}
	visitor := &contractspb.SettleTheftVisitorResult{}
	if err := decodeTypedResult(result, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_SETTLE_THEFT_VISITOR, visitor); err != nil {
		return err
	}
	if visitor.GetSettlementId() != decision.GetSettlementId() ||
		visitor.GetRewardItemId() != decision.GetRewardItemId() ||
		visitor.GetGrantedNetQuantity() != decision.GetNetReceivedQuantity() ||
		visitor.GetPlayerRecordVersion() == 0 || visitor.GetGrantedNetQuantity() > math.MaxUint64 {
		return fmt.Errorf("%w: Visitor result identity/version", ErrPriorResult)
	}
	if root == nil || visitor.GetSettlementId() != root.GetSettlementId() {
		return fmt.Errorf("%w: Visitor settlement binding", ErrPriorResult)
	}
	return nil
}

func validateOwnerStepResult(root *contractspb.DecideTheftPayload, decision *contractspb.TheftTargetDecision, result workflow.StepResult, required bool) error {
	if len(result.Result) == 0 {
		if required {
			return fmt.Errorf("%w: Owner typed result missing", ErrPriorResult)
		}
		if result.ResultHash != ([32]byte{}) {
			return fmt.Errorf("%w: Owner result hash without bytes", ErrPriorResult)
		}
		return nil
	}
	owner := &contractspb.CreditGuardIncomeResult{}
	if err := decodeTypedResult(result, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_CREDIT_GUARD_INCOME, owner); err != nil {
		return err
	}
	if owner.GetSettlementId() != decision.GetSettlementId() ||
		owner.GetCreditedNormalCoin() != decision.GetGuardCoinValue() || owner.GetPlayerRecordVersion() == 0 {
		return fmt.Errorf("%w: Owner result identity/version", ErrPriorResult)
	}
	if root == nil || owner.GetSettlementId() != root.GetSettlementId() {
		return fmt.Errorf("%w: Owner settlement binding", ErrPriorResult)
	}
	return nil
}

func decodeTypedResult(result workflow.StepResult, command contractspb.ParticipantCommandType, message proto.Message) error {
	if len(result.Result) == 0 || result.ResultHash == ([32]byte{}) {
		return fmt.Errorf("%w: typed result/hash missing", ErrPriorResult)
	}
	if err := proto.Unmarshal(result.Result, message); err != nil {
		return fmt.Errorf("%w: typed result protobuf: %v", ErrPriorResult, err)
	}
	deterministic, err := deterministicProto(message)
	if err != nil || !bytes.Equal(deterministic, result.Result) {
		return fmt.Errorf("%w: typed result bytes", ErrPriorResult)
	}
	spec, err := contracts.LookupParticipantCommand(command)
	if err != nil {
		return fmt.Errorf("%w: command: %v", ErrPriorResult, err)
	}
	hash, err := contracts.CanonicalParticipantHash(spec, message)
	if err != nil || hash != result.ResultHash {
		return fmt.Errorf("%w: typed result hash", ErrPriorResult)
	}
	return nil
}

func validateTheftDecision(root *contractspb.DecideTheftPayload, decision *contractspb.TheftTargetDecision) error {
	if root == nil || decision == nil || decision.GetWorkflowId() == "" ||
		decision.GetSettlementId() != root.GetSettlementId() ||
		decision.GetTheftAttemptId() != root.GetTheftAttemptId() ||
		decision.GetVisitorPlayerId() != root.GetVisitorPlayerId() ||
		decision.GetOwnerPlayerId() != root.GetOwnerPlayerId() ||
		decision.GetFarmId() != root.GetFarmId() || decision.GetPlotId() != root.GetPlotId() ||
		decision.GetCropCycleId() != root.GetCropCycleId() || decision.GetRewardItemId() == 0 ||
		decision.GetAppliedSlotRatioBp() == 0 || decision.GetGrossStolenQuantity() == 0 ||
		decision.GetCropRuleVersion() == 0 || len(decision.GetTheftDecisionHash()) != sha256.Size {
		return fmt.Errorf("%w: TheftTargetDecision identity", ErrPriorResult)
	}
	if decision.GetTheftOutcome() != contractspb.TheftOutcome_THEFT_OUTCOME_STOLEN &&
		decision.GetTheftOutcome() != contractspb.TheftOutcome_THEFT_OUTCOME_BLOCKED_BY_GUARD {
		return fmt.Errorf("%w: unsupported Theft outcome", ErrPriorResult)
	}
	if decision.GetGuardLostQuantity()+decision.GetNetReceivedQuantity() != decision.GetGrossStolenQuantity() {
		return fmt.Errorf("%w: gross/net/guard quantity mismatch", ErrPriorResult)
	}
	guard := decision.GetGuardDecisionSnapshot()
	if guard == nil || guard.GetTheftAttemptId() != decision.GetTheftAttemptId() ||
		guard.GetTriggerProbabilityBp() > 10000 || guard.GetGrossStolenQuantity() != decision.GetGrossStolenQuantity() ||
		guard.GetGuardLostQuantity() != decision.GetGuardLostQuantity() ||
		guard.GetNetReceivedQuantity() != decision.GetNetReceivedQuantity() ||
		guard.GetCropUnitCoinValue() != decision.GetCropUnitCoinValue() ||
		guard.GetGuardCoinValue() != decision.GetGuardCoinValue() {
		return fmt.Errorf("%w: GuardDecisionSnapshot mismatch", ErrPriorResult)
	}
	// Player PetState starts with guard_version=0 and does not emit a
	// GuardSyncOrder until the first successful feed or real switch.  A
	// no-pet Theft therefore carries an explicit zero-version, zero-rule
	// snapshot; requiring a fabricated non-zero version here would make the
	// first legal no-pet theft impossible.  Once a guard version exists, all
	// frozen rule/random-version fields remain mandatory for replay proof.
	if guard.GetGuardVersion() == 0 {
		if guard.GetGuardRuleVersion() != 0 || guard.GetTriggerProbabilityBp() != 0 ||
			guard.GetInterceptPolicy() != contractspb.GuardInterceptPolicy_GUARD_INTERCEPT_POLICY_UNSPECIFIED ||
			guard.GetRandomAlgorithmVersion() != 0 || guard.GetRandomKeyVersion() != 0 ||
			guard.GetGuardTriggered() || guard.GetGuardLostQuantity() != 0 || guard.GetGuardCoinValue() != 0 {
			return fmt.Errorf("%w: zero-version GuardDecisionSnapshot is not an explicit no-pet snapshot", ErrPriorResult)
		}
	} else if guard.GetGuardRuleVersion() == 0 || guard.GetRandomAlgorithmVersion() == 0 ||
		guard.GetRandomKeyVersion() == 0 || guard.GetInterceptPolicy() != contractspb.GuardInterceptPolicy_GUARD_INTERCEPT_POLICY_FULL_GROSS {
		return fmt.Errorf("%w: versioned GuardDecisionSnapshot rule is incomplete", ErrPriorResult)
	}
	if guard.GetGuardTriggered() {
		if decision.GetTheftOutcome() != contractspb.TheftOutcome_THEFT_OUTCOME_BLOCKED_BY_GUARD ||
			decision.GetGuardLostQuantity() != decision.GetGrossStolenQuantity() || decision.GetNetReceivedQuantity() != 0 {
			return fmt.Errorf("%w: blocked guard decision mismatch", ErrPriorResult)
		}
	} else if decision.GetTheftOutcome() != contractspb.TheftOutcome_THEFT_OUTCOME_STOLEN || decision.GetGuardLostQuantity() != 0 ||
		decision.GetNetReceivedQuantity() != decision.GetGrossStolenQuantity() || decision.GetGuardCoinValue() != 0 {
		return fmt.Errorf("%w: unblocked guard decision mismatch", ErrPriorResult)
	}
	if decision.GetGuardLostQuantity() > 0 {
		if decision.GetCropUnitCoinValue() == 0 || decision.GetGuardCoinValue() == 0 ||
			decision.GetGuardLostQuantity() > math.MaxUint64/decision.GetCropUnitCoinValue() ||
			decision.GetGuardLostQuantity()*decision.GetCropUnitCoinValue() != decision.GetGuardCoinValue() {
			return fmt.Errorf("%w: guard coin value mismatch", ErrPriorResult)
		}
	} else if decision.GetGuardCoinValue() != 0 {
		return fmt.Errorf("%w: guard coin without loss", ErrPriorResult)
	}
	withoutHash := proto.Clone(decision).(*contractspb.TheftTargetDecision)
	withoutHash.TheftDecisionHash = nil
	want, err := contracts.CanonicalHash(withoutHash)
	if err != nil || !bytes.Equal(want[:], decision.GetTheftDecisionHash()) {
		return fmt.Errorf("%w: TheftDecision hash mismatch", ErrPriorResult)
	}
	return nil
}

func validateVisitorStep(record *workflow.WorkRecord, root *contractspb.DecideTheftPayload, decision *contractspb.TheftTargetDecision) error {
	if record == nil || len(record.Steps) < 2 ||
		(record.Steps[1].Status != workflow.StepApplied && record.Steps[1].Status != workflow.StepNoChange) {
		return fmt.Errorf("%w: Visitor receipt is not terminal-success", ErrPriorResult)
	}
	if record.Steps[1].Status == workflow.StepApplied {
		result := workflow.StepResult{Outcome: workflow.StepOutcomeApplied, Result: record.Steps[1].Result, ResultHash: record.Steps[1].ResultHash}
		return validateVisitorStepResult(root, decision, result, true)
	}
	if len(record.Steps[1].Result) != 0 {
		result := workflow.StepResult{Outcome: workflow.StepOutcomeNoChange, Result: record.Steps[1].Result, ResultHash: record.Steps[1].ResultHash}
		return validateVisitorStepResult(root, decision, result, false)
	}
	return nil
}

// DecodeRoot verifies the immutable root bytes, canonical hash, deterministic
// encoding, all identities and the frozen ActionPermit/Admission evidence.
func DecodeRoot(record *workflow.WorkRecord) (*contractspb.DecideTheftPayload, error) {
	if record == nil || record.Address.WorkType != string(identity.WorkTypeTheft) {
		return nil, fmt.Errorf("%w: record/work type", ErrRootPayload)
	}
	root := &contractspb.DecideTheftPayload{}
	if err := proto.Unmarshal(record.Payload, root); err != nil {
		return nil, fmt.Errorf("%w: protobuf: %v", ErrRootPayload, err)
	}
	deterministic, err := deterministicProto(root)
	if err != nil || !bytes.Equal(deterministic, record.Payload) {
		return nil, fmt.Errorf("%w: root bytes", ErrRootPayload)
	}
	hash, err := contracts.CanonicalHash(root)
	if err != nil || hash != record.PayloadHash {
		return nil, fmt.Errorf("%w: root canonical hash", ErrRootPayload)
	}
	if root.GetVisitorPlayerId() == 0 || root.GetOwnerPlayerId() == 0 || root.GetFarmId() != root.GetOwnerPlayerId() ||
		root.GetVisitorPlayerId() == root.GetOwnerPlayerId() || root.GetPlotId() == 0 || root.GetPlotId() > maxTheftPlotID ||
		workidentity.ValidateStableOpaqueID(root.GetCropCycleId()) != nil || root.GetTheftAttemptId() == "" || root.GetSettlementId() == "" ||
		root.GetActionPermitSnapshot() == nil || len(root.GetActionPermitSnapshotHash()) != sha256.Size {
		return nil, fmt.Errorf("%w: root fields", ErrRootPayload)
	}
	permit := root.GetActionPermitSnapshot()
	permitHash, err := contracts.CanonicalHash(permit)
	if err != nil || !bytes.Equal(permitHash[:], root.GetActionPermitSnapshotHash()) {
		return nil, fmt.Errorf("%w: ActionPermit hash", ErrRootPayload)
	}
	if permit.GetVisitorPlayerId() != root.GetVisitorPlayerId() || permit.GetOwnerPlayerId() != root.GetOwnerPlayerId() ||
		permit.GetTargetFarmId() != root.GetFarmId() || permit.GetOperationId() != record.OperationID ||
		permit.GetActionScope() != contractspb.FriendActionScope_FRIEND_ACTION_SCOPE_THEFT ||
		len(permit.GetCanonicalPayloadHash()) != sha256.Size || len(permit.GetTargetBindingHash()) != sha256.Size ||
		permit.GetExecutionAcceptedAtAuthorityMs() < permit.GetIssuedAtAuthorityMs() ||
		permit.GetExecutionAcceptedAtAuthorityMs() >= permit.GetExpireAtAuthorityMs() {
		return nil, fmt.Errorf("%w: ActionPermit binding", ErrRootPayload)
	}
	pairID, err := identity.CanonicalFriendRelationID(root.GetVisitorPlayerId(), root.GetOwnerPlayerId())
	if err != nil {
		return nil, fmt.Errorf("%w: pair identity: %v", ErrRootPayload, err)
	}
	key := identity.EffectiveIdempotencyKey{
		SubjectPlayerID:       root.GetVisitorPlayerId(),
		StableCommandTypeName: string(identity.WorkTypeTheft),
		OperationID:           record.OperationID,
		Root:                  identity.OperationRootRef{Kind: identity.RootKindFriendPair, CanonicalID: pairID},
	}
	wantWorkflow, err := identity.WorkflowID(key)
	if err != nil || wantWorkflow != record.Address.WorkID {
		return nil, fmt.Errorf("%w: workflow identity", ErrRootPayload)
	}
	wantSettlement, err := identity.TheftSettlementID(record.Address.WorkID)
	if err != nil || wantSettlement != root.GetSettlementId() {
		return nil, fmt.Errorf("%w: settlement identity", ErrRootPayload)
	}
	wantAttempt, err := identity.TheftAttemptID(root.GetFarmId(), root.GetCropCycleId(), root.GetVisitorPlayerId())
	if err != nil || wantAttempt != root.GetTheftAttemptId() {
		return nil, fmt.Errorf("%w: attempt identity", ErrRootPayload)
	}
	return root, nil
}

// DecodeDecideTheftResult verifies the durable Farm receipt and returns its
// Participant result hash.
func DecodeDecideTheftResult(record *workflow.WorkRecord) (*contractspb.DecideTheftResult, [32]byte, error) {
	if record == nil || len(record.Steps) < 1 || record.Steps[0].Status != workflow.StepApplied {
		return nil, [32]byte{}, fmt.Errorf("%w: Farm step is not applied", ErrPriorResult)
	}
	result := &contractspb.DecideTheftResult{}
	if err := decodeTypedResult(workflow.StepResult{Result: record.Steps[0].Result, ResultHash: record.Steps[0].ResultHash}, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT, result); err != nil {
		return nil, [32]byte{}, err
	}
	if result.GetTheftTargetDecision() == nil || result.GetTheftTargetDecision().GetWorkflowId() != record.Address.WorkID {
		return nil, [32]byte{}, fmt.Errorf("%w: Farm workflow identity", ErrPriorResult)
	}
	return result, record.Steps[0].ResultHash, nil
}

// DecodeResults validates all receipts necessary for a successful Theft.
func DecodeResults(record *workflow.WorkRecord) (*contractspb.DecideTheftResult, *contractspb.SettleTheftVisitorResult, *contractspb.CreditGuardIncomeResult, error) {
	root, err := DecodeRoot(record)
	if err != nil {
		return nil, nil, nil, err
	}
	farm, _, err := DecodeDecideTheftResult(record)
	if err != nil {
		return nil, nil, nil, err
	}
	if err := validateTheftDecision(root, farm.GetTheftTargetDecision()); err != nil {
		return nil, nil, nil, err
	}
	if len(record.Steps) < 2 || (record.Steps[1].Status != workflow.StepApplied && record.Steps[1].Status != workflow.StepNoChange) {
		return nil, nil, nil, fmt.Errorf("%w: Visitor receipt incomplete", ErrTerminalResult)
	}
	var visitor *contractspb.SettleTheftVisitorResult
	if record.Steps[1].Status == workflow.StepApplied || len(record.Steps[1].Result) != 0 {
		visitor = &contractspb.SettleTheftVisitorResult{}
		if err := validateVisitorStepResult(root, farm.GetTheftTargetDecision(), workflow.StepResult{Result: record.Steps[1].Result, ResultHash: record.Steps[1].ResultHash}, record.Steps[1].Status == workflow.StepApplied); err != nil {
			return nil, nil, nil, err
		}
		if len(record.Steps[1].Result) != 0 {
			if err := proto.Unmarshal(record.Steps[1].Result, visitor); err != nil {
				return nil, nil, nil, fmt.Errorf("%w: Visitor result: %v", ErrTerminalResult, err)
			}
		}
	}
	var owner *contractspb.CreditGuardIncomeResult
	if farm.GetTheftTargetDecision().GetGuardCoinValue() > 0 {
		if len(record.Steps) < 3 || (record.Steps[2].Status != workflow.StepApplied && record.Steps[2].Status != workflow.StepNoChange) {
			return nil, nil, nil, fmt.Errorf("%w: Owner receipt incomplete", ErrTerminalResult)
		}
		if record.Steps[2].Status == workflow.StepApplied || len(record.Steps[2].Result) != 0 {
			owner = &contractspb.CreditGuardIncomeResult{}
			if err := validateOwnerStepResult(root, farm.GetTheftTargetDecision(), workflow.StepResult{Result: record.Steps[2].Result, ResultHash: record.Steps[2].ResultHash}, record.Steps[2].Status == workflow.StepApplied); err != nil {
				return nil, nil, nil, err
			}
			if len(record.Steps[2].Result) != 0 {
				if err := proto.Unmarshal(record.Steps[2].Result, owner); err != nil {
					return nil, nil, nil, fmt.Errorf("%w: Owner result: %v", ErrTerminalResult, err)
				}
			}
		}
	}
	return farm, visitor, owner, nil
}

// State is the externally useful state projection of a generic WorkRecord.
// The generic repository remains the authority; these names are derived from
// persisted step payload/receipt boundaries and never from process memory.
type State string

const (
	StateCreated           State = "CREATED"
	StateFarmDeciding      State = "FARM_DECIDING"
	StateFarmDecided       State = "FARM_DECIDED"
	StateVisitorSettling   State = "VISITOR_SETTLING"
	StateVisitorSettled    State = "VISITOR_SETTLED"
	StateOwnerSettling     State = "OWNER_SETTLING"
	StateCompleted         State = "COMPLETED"
	StateRejected          State = "REJECTED"
	StateNoChange          State = "NO_CHANGE"
	StateReconcileRequired State = "RECONCILE_REQUIRED"
)

func (s State) String() string { return string(s) }

func isReconcileClass(value string) bool {
	return strings.EqualFold(strings.TrimSpace(value), ErrorClassReconcileRequired)
}

func StateFor(record *workflow.WorkRecord) State {
	if record == nil || len(record.Steps) < 3 {
		return StateReconcileRequired
	}
	if record.State == workflow.WorkStateFailed || isReconcileClass(record.LastErrorClass) {
		return StateReconcileRequired
	}
	for _, step := range record.Steps {
		if isReconcileClass(step.ErrorClass) {
			return StateReconcileRequired
		}
	}
	if record.State == workflow.WorkStateCompleted {
		return StateCompleted
	}
	if record.State == workflow.WorkStateRejected {
		if record.Steps[0].Status == workflow.StepNoChange {
			return StateNoChange
		}
		return StateRejected
	}

	farm := &record.Steps[0]
	switch farm.Status {
	case workflow.StepPending:
		if len(farm.Payload) == 0 {
			return StateCreated
		}
		return StateFarmDeciding
	case workflow.StepUnknown:
		return StateFarmDeciding
	case workflow.StepRejected:
		return StateRejected
	case workflow.StepNoChange:
		return StateNoChange
	case workflow.StepApplied:
		// continue below
	default:
		return StateReconcileRequired
	}

	visitor := &record.Steps[1]
	switch visitor.Status {
	case workflow.StepPending:
		if len(visitor.Payload) == 0 {
			return StateFarmDecided
		}
		return StateVisitorSettling
	case workflow.StepUnknown:
		return StateVisitorSettling
	case workflow.StepRejected:
		return StateRejected
	case workflow.StepApplied, workflow.StepNoChange:
		// continue below
	default:
		return StateReconcileRequired
	}
	decision, _, err := DecodeDecideTheftResult(record)
	if err != nil {
		return StateReconcileRequired
	}
	if decision.GetTheftTargetDecision().GetGuardCoinValue() == 0 {
		if record.State == workflow.WorkStateCompleted {
			return StateCompleted
		}
		return StateVisitorSettled
	}
	owner := &record.Steps[2]
	switch owner.Status {
	case workflow.StepPending:
		if len(owner.Payload) == 0 {
			return StateVisitorSettled
		}
		return StateOwnerSettling
	case workflow.StepUnknown:
		return StateOwnerSettling
	case workflow.StepRejected:
		return StateRejected
	case workflow.StepApplied, workflow.StepNoChange:
		if record.State == workflow.WorkStateCompleted {
			return StateCompleted
		}
		return StateOwnerSettling
	default:
		return StateReconcileRequired
	}
}

// Outcome is the stable public status projection. Intermediate states are
// all PROCESSING; final result bytes exist only for SUCCEEDED.
type Outcome string

const (
	OutcomeProcessing        Outcome = "PROCESSING"
	OutcomeSucceeded         Outcome = "SUCCEEDED"
	OutcomeRejected          Outcome = "REJECTED"
	OutcomeNoChange          Outcome = "NO_CHANGE"
	OutcomeReconcileRequired Outcome = "RECONCILE_REQUIRED"
)

func (o Outcome) String() string { return string(o) }

func OutcomeFor(record *workflow.WorkRecord) Outcome {
	switch StateFor(record) {
	case StateCompleted:
		return OutcomeSucceeded
	case StateRejected:
		return OutcomeRejected
	case StateNoChange:
		return OutcomeNoChange
	case StateReconcileRequired:
		return OutcomeReconcileRequired
	default:
		return OutcomeProcessing
	}
}

// Coordinator composes the theft Definition with the existing durable
// WorkRepository and generic Engine. Create always completes before Resume can
// invoke a Participant.
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
	Repository      workflow.WorkRepository
	Visitor         workflow.ParticipantInvoker
	Owner           workflow.ParticipantInvoker
	Player          workflow.ParticipantInvoker // fallback for Visitor and Owner
	Farm            workflow.ParticipantInvoker
	BucketResolver  workidentity.BucketResolver
	BucketVersion   uint32
	Deadline        time.Duration
	Now             func() time.Time
	MaxStepAttempts uint64
}

func NewCoordinator(config CoordinatorConfig) (*Coordinator, error) {
	if config.Repository == nil || config.BucketResolver == nil || config.BucketVersion == 0 {
		return nil, fmt.Errorf("%w: repository, bucket resolver and bucket version", ErrInvalidInput)
	}
	visitor, owner := config.Visitor, config.Owner
	if config.Player != nil {
		if visitor == nil {
			visitor = config.Player
		}
		if owner == nil {
			owner = config.Player
		}
	}
	options := []DefinitionOption{}
	if config.MaxStepAttempts > 0 {
		options = append(options, WithMaxStepAttempts(config.MaxStepAttempts))
	}
	definition, err := NewDefinition(visitor, owner, config.Farm, options...)
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
	return &Coordinator{repository: config.Repository, definition: definition, engine: engine, clock: clock,
		deadline: deadline, bucketVersion: config.BucketVersion, bucketResolver: config.BucketResolver}, nil
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

func (c *Coordinator) Repository() workflow.WorkRepository {
	if c == nil {
		return nil
	}
	return c.repository
}

// Create performs canonical identity, placement and atomic CreateIfAbsent
// only. It has no Participant side effect.
func (c *Coordinator) Create(ctx context.Context, input TheftInput) (*workflow.WorkRecord, bool, error) {
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
	deadline := c.clock().Add(c.deadline)
	if input.WorkflowDeadlineAtMs > 0 {
		deadline = time.UnixMilli(input.WorkflowDeadlineAtMs)
		if !deadline.After(time.UnixMilli(input.ActionPermitSnapshot.GetExpireAtAuthorityMs())) {
			return nil, false, fmt.Errorf("%w: workflow deadline does not outlive permit", ErrInvalidInput)
		}
	}
	return c.engine.Create(ctx, c.definition, address, input.VisitorPlayerID, input.OperationID,
		root.PayloadBytes, root.PayloadHash, deadline)
}

// Resume advances an existing unleased record. Worker recovery should call
// Engine.Resume with the LeaseFence supplied by workflow.RecoveryLoop.
func (c *Coordinator) Resume(ctx context.Context, record *workflow.WorkRecord) error {
	if c == nil || c.engine == nil {
		return fmt.Errorf("%w: coordinator", ErrInvalidInput)
	}
	return c.engine.Resume(ctx, record, workflow.LeaseFence{}, nil)
}

// Start creates/loads the durable root and performs one online pass.
func (c *Coordinator) Start(ctx context.Context, input TheftInput) (*workflow.WorkRecord, bool, error) {
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

// PublicResult projects only the frozen Farm decision and the committed Farm
// version. It refuses to manufacture a result while any necessary receipt is
// incomplete.
func PublicResult(record *workflow.WorkRecord) (*publicpb.TheftResult, error) {
	if StateFor(record) != StateCompleted || OutcomeFor(record) != OutcomeSucceeded {
		return nil, ErrTerminalResult
	}
	farm, _, _, err := DecodeResults(record)
	if err != nil {
		return nil, err
	}
	decision := farm.GetTheftTargetDecision()
	if decision == nil || decision.GetPlotId() == 0 {
		return nil, fmt.Errorf("%w: public decision", ErrTerminalResult)
	}
	return &publicpb.TheftResult{
		TargetFarmId:         decision.GetFarmId(),
		PlotId:               decision.GetPlotId() - 1,
		CropCycleId:          decision.GetCropCycleId(),
		CommittedFarmVersion: farm.GetCommittedFarmVersion(),
		SettlementId:         decision.GetSettlementId(),
	}, nil
}

func deterministicProto(message proto.Message) ([]byte, error) {
	if message == nil {
		return nil, fmt.Errorf("%w: nil protobuf", ErrInvalidInput)
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(message)
}
