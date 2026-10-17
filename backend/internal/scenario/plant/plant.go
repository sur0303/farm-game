// Package plant contains the Plant-only cross-authority workflow definition.
//
// It owns scenario composition and durable payload derivation. PlayerActor and
// FarmActor remain behind workflow.ParticipantInvoker; this package deliberately
// has no dependency on either concrete actor implementation.
package plant

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	contractspb "farmgame/api/gen/contracts"
	"farmgame/internal/contracts"
	"farmgame/internal/identity"
	"farmgame/internal/workflow"
	"farmgame/internal/workidentity"

	"google.golang.org/protobuf/proto"
)

const (
	// Quantity is frozen by the P0 Plant contract: one command consumes one
	// server-mapped seed.
	Quantity uint32 = 1

	// ReleaseReasonFarmRejected is the stable reason carried by the
	// compensation payload. It is part of the Participant contract.
	ReleaseReasonFarmRejected = "FARM_REJECTED"

	defaultWorkflowDeadline = 24 * time.Hour
	participantMaxCareItems = 32
	participantMaxPlots     = 24
)

var (
	ErrInvalidInput = errors.New("plant: invalid input")
	ErrRootPayload  = errors.New("plant: invalid durable root payload")
	ErrPriorResult  = errors.New("plant: invalid prior typed result")
)

// PlantInput is the trusted, already-admitted Plant intent consumed by the
// coordinator. SeedItemID and CropRuleSnapshot are server-side values; they
// are not copied from an untrusted public request without the caller's
// admission/crop-definition lookup having run first.
type PlantInput struct {
	SubjectPlayerID         uint64
	OperationID             string
	TargetFarmID            uint64
	PlotID                  uint32
	ExpectedPlotVersion     uint64
	CropDefinitionID        uint32
	ExpectedCropRuleVersion uint64
	SeedItemID              uint32
	CropRuleSnapshot        *contractspb.CropRuleSnapshot
}

// RootIntent is the immutable result of building a Plant root. PayloadBytes
// are deterministic protobuf bytes; PayloadHash is the canonical semantic
// hash used for WorkRecord idempotency conflict detection.
type RootIntent struct {
	Key           identity.EffectiveIdempotencyKey
	WorkflowID    string
	ReservationID string
	CropCycleID   string

	Payload      *contractspb.CommitPlantPayload
	PayloadBytes []byte
	PayloadHash  [32]byte
}

// BuildRoot derives the v2 root-scoped workflow identity and freezes the
// complete supplied CropRuleSnapshot before a WorkRecord can be created.
// The Farm actor is the effective idempotency root for Plant.
func BuildRoot(input PlantInput) (RootIntent, error) {
	if err := validateInput(input); err != nil {
		return RootIntent{}, err
	}

	key := identity.EffectiveIdempotencyKey{
		SubjectPlayerID:       input.SubjectPlayerID,
		StableCommandTypeName: string(identity.WorkTypePlant),
		OperationID:           input.OperationID,
		Root:                  identity.OperationRootRef{Kind: identity.RootKindFarmActor, CanonicalID: strconv.FormatUint(input.TargetFarmID, 10)},
	}
	workflowID, err := identity.WorkflowID(key)
	if err != nil {
		return RootIntent{}, fmt.Errorf("%w: workflow id: %v", ErrInvalidInput, err)
	}
	reservationID, err := identity.SeedReservationID(workflowID)
	if err != nil {
		return RootIntent{}, fmt.Errorf("%w: reservation id: %v", ErrInvalidInput, err)
	}
	cropCycleID, err := identity.CropCycleID(workflowID)
	if err != nil {
		return RootIntent{}, fmt.Errorf("%w: crop cycle id: %v", ErrInvalidInput, err)
	}

	snapshot := proto.Clone(input.CropRuleSnapshot).(*contractspb.CropRuleSnapshot)
	snapshotHash, err := contracts.CanonicalHash(snapshot)
	if err != nil {
		return RootIntent{}, fmt.Errorf("%w: crop rule snapshot hash: %v", ErrInvalidInput, err)
	}
	// CommitPlantPayload is already the frozen typed Plant carrier and includes
	// every field needed by Farm. A zero ReserveSeedResultHash distinguishes
	// this root intent from the later Farm step payload.
	payload := &contractspb.CommitPlantPayload{
		OwnerPlayerId:        input.SubjectPlayerID,
		FarmId:               input.TargetFarmID,
		PlotId:               input.PlotID,
		ExpectedPlotVersion:  input.ExpectedPlotVersion,
		ReservationId:        reservationID,
		CropCycleId:          cropCycleID,
		CropRuleSnapshot:     snapshot,
		CropRuleSnapshotHash: append([]byte(nil), snapshotHash[:]...),
	}
	payloadBytes, err := deterministicProto(payload)
	if err != nil {
		return RootIntent{}, fmt.Errorf("%w: marshal: %v", ErrInvalidInput, err)
	}
	rootHash, err := contracts.CanonicalHash(payload)
	if err != nil {
		return RootIntent{}, fmt.Errorf("%w: root hash: %v", ErrInvalidInput, err)
	}
	return RootIntent{
		Key:           key,
		WorkflowID:    workflowID,
		ReservationID: reservationID,
		CropCycleID:   cropCycleID,
		Payload:       proto.Clone(payload).(*contractspb.CommitPlantPayload),
		PayloadBytes:  append([]byte(nil), payloadBytes...),
		PayloadHash:   rootHash,
	}, nil
}

// BuildRootPayload is a compact builder for callers that only need the
// durable root bytes and canonical hash.
func BuildRootPayload(input PlantInput) ([]byte, [32]byte, error) {
	root, err := BuildRoot(input)
	if err != nil {
		return nil, [32]byte{}, err
	}
	return append([]byte(nil), root.PayloadBytes...), root.PayloadHash, nil
}

// Address returns the independent-work placement for this Plant workflow.
// Bucket placement is supplied by composition because the generic runtime
// intentionally does not own the physical bucket resolver.
func (r RootIntent) Address(bucketVersion, bucketID uint32) (workidentity.WorkAddress, error) {
	if bucketVersion == 0 {
		return workidentity.WorkAddress{}, fmt.Errorf("%w: bucket version", ErrInvalidInput)
	}
	address := workidentity.WorkAddress{
		WorkID:        r.WorkflowID,
		WorkType:      string(identity.WorkTypePlant),
		PlacementKind: workidentity.IndependentWorkBucket,
		BucketVersion: bucketVersion,
		BucketID:      bucketID,
	}
	if err := address.ValidateShape(); err != nil {
		return workidentity.WorkAddress{}, fmt.Errorf("%w: work address: %v", ErrInvalidInput, err)
	}
	return address, nil
}

// AddressForResolver resolves and validates the immutable physical placement
// for this workflow. The runtime owns WorkRecord persistence, while the
// composition layer owns the versioned bucket resolver.
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

func validateInput(input PlantInput) error {
	if input.SubjectPlayerID == 0 || input.TargetFarmID == 0 || input.PlotID == 0 ||
		input.ExpectedPlotVersion == 0 || input.CropDefinitionID == 0 ||
		input.ExpectedCropRuleVersion == 0 || input.SeedItemID == 0 ||
		input.CropRuleSnapshot == nil {
		return fmt.Errorf("%w: required fields", ErrInvalidInput)
	}
	if input.TargetFarmID != input.SubjectPlayerID {
		return fmt.Errorf("%w: Plant farm must be owned by subject player", ErrInvalidInput)
	}
	if input.OperationID == "" {
		return fmt.Errorf("%w: operation id", ErrInvalidInput)
	}
	snapshot := input.CropRuleSnapshot
	if snapshot.GetCropDefinitionId() != input.CropDefinitionID ||
		snapshot.GetCropRuleVersion() != input.ExpectedCropRuleVersion || snapshot.GetSeedItemId() != input.SeedItemID {
		return fmt.Errorf("%w: crop rule snapshot does not match trusted input", ErrInvalidInput)
	}
	if err := validateCropRuleSnapshot(snapshot); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	if _, err := (identity.EffectiveIdempotencyKey{
		SubjectPlayerID:       input.SubjectPlayerID,
		StableCommandTypeName: string(identity.WorkTypePlant),
		OperationID:           input.OperationID,
		Root:                  identity.OperationRootRef{Kind: identity.RootKindFarmActor, CanonicalID: strconv.FormatUint(input.TargetFarmID, 10)},
	}).OperationDigest(); err != nil {
		return fmt.Errorf("%w: idempotency key: %v", ErrInvalidInput, err)
	}
	return nil
}

// Definition is the Plant state machine consumed by workflow.Engine.
// Player and Farm are intentionally generic participant ports.
type Definition struct {
	player workflow.ParticipantInvoker
	farm   workflow.ParticipantInvoker
}

var _ workflow.Definition = (*Definition)(nil)

// NewDefinition constructs the four-step Plant plan:
// ReserveSeed(Player) → CommitPlant(Farm) → CommitSeedReservation(Player),
// with ReleaseSeedReservation(Player) as the Farm-rejected branch.
func NewDefinition(player, farm workflow.ParticipantInvoker) (*Definition, error) {
	if player == nil || farm == nil {
		return nil, fmt.Errorf("%w: participant invokers required", ErrInvalidInput)
	}
	return &Definition{player: player, farm: farm}, nil
}

func (*Definition) WorkType() string { return string(identity.WorkTypePlant) }

func (*Definition) StepNames() []string {
	return []string{
		string(identity.StepReserveSeed),
		string(identity.StepCommitPlant),
		string(identity.StepCommitSeedReservation),
		string(identity.StepReleaseSeedReservation),
	}
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

// dependencyBoundInvoker binds the generic StepCall dependency field to the
// exact dependency hash already carried by the deterministic typed payload.
// This matters for the Farm-rejected branch: the generic runtime's successful
// predecessor chain cannot see a rejected Farm step, while the Participant
// release contract must still carry that Farm result hash. The source remains
// the durable payload, never process-local result state.
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
	var dependency []byte
	switch stepIndex {
	case 0:
		return [32]byte{}, nil
	case 1:
		message := &contractspb.CommitPlantPayload{}
		if err := proto.Unmarshal(payload, message); err != nil {
			return [32]byte{}, fmt.Errorf("%w: CommitPlant payload: %v", ErrPriorResult, err)
		}
		dependency = message.GetReserveSeedResultHash()
	case 2:
		message := &contractspb.CommitSeedReservationPayload{}
		if err := proto.Unmarshal(payload, message); err != nil {
			return [32]byte{}, fmt.Errorf("%w: CommitSeedReservation payload: %v", ErrPriorResult, err)
		}
		dependency = message.GetCommitPlantResultHash()
	case 3:
		message := &contractspb.ReleaseSeedReservationPayload{}
		if err := proto.Unmarshal(payload, message); err != nil {
			return [32]byte{}, fmt.Errorf("%w: ReleaseSeedReservation payload: %v", ErrPriorResult, err)
		}
		dependency = message.GetTerminalFarmResultHash()
	default:
		return [32]byte{}, fmt.Errorf("%w: step index %d", ErrInvalidInput, stepIndex)
	}
	if len(dependency) != 32 {
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
	case string(identity.StepReserveSeed):
		return contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED, nil
	case string(identity.StepCommitPlant):
		return contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT, nil
	case string(identity.StepCommitSeedReservation):
		return contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_SEED_RESERVATION, nil
	case string(identity.StepReleaseSeedReservation):
		return contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RELEASE_SEED_RESERVATION, nil
	default:
		return contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_UNSPECIFIED, fmt.Errorf("%w: unknown Plant step %q", ErrInvalidInput, step)
	}
}

// BuildPayload always decodes the root and the durable prior StepRecord. It
// never consults process-local result memory, caller/config objects, or a
// previous Invoke response.
func (d *Definition) BuildPayload(record *workflow.WorkRecord, stepIndex int) ([]byte, error) {
	root, err := decodeRoot(record)
	if err != nil {
		return nil, err
	}
	switch stepIndex {
	case 0:
		return deterministicProto(&contractspb.ReserveSeedPayload{
			PlayerId:             root.GetOwnerPlayerId(),
			FarmId:               root.GetFarmId(),
			PlotId:               root.GetPlotId(),
			ReservationId:        root.GetReservationId(),
			CropDefinitionId:     root.GetCropRuleSnapshot().GetCropDefinitionId(),
			SeedItemId:           root.GetCropRuleSnapshot().GetSeedItemId(),
			Quantity:             Quantity,
			CropRuleVersion:      root.GetCropRuleSnapshot().GetCropRuleVersion(),
			CropRuleSnapshotHash: append([]byte(nil), root.GetCropRuleSnapshotHash()...),
		})
	case 1:
		if err := validateReserveStepStatus(record); err != nil {
			return nil, err
		}
		reserveHash, err := durableResultHash(record, 0, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED, &contractspb.ReserveSeedResult{})
		if err != nil {
			return nil, err
		}
		reserve := &contractspb.ReserveSeedResult{}
		if err := unmarshalDurableResult(record, 0, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED, reserve); err != nil {
			return nil, err
		}
		if reserve.GetReservationId() != root.GetReservationId() || reserve.GetSeedItemId() != root.GetCropRuleSnapshot().GetSeedItemId() || reserve.GetReservedQuantity() != Quantity {
			return nil, fmt.Errorf("%w: reserve result identity", ErrPriorResult)
		}
		payload := proto.Clone(root).(*contractspb.CommitPlantPayload)
		payload.ReserveSeedResultHash = append([]byte(nil), reserveHash[:]...)
		return deterministicProto(payload)
	case 2, 3:
		if err := validateReserveStepStatus(record); err != nil {
			return nil, err
		}
		if err := validateFarmBranchStatus(record, stepIndex); err != nil {
			return nil, err
		}
		farmHash, err := durableResultHash(record, 1, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT, &contractspb.CommitPlantResult{})
		if err != nil {
			return nil, err
		}
		farmResult := &contractspb.CommitPlantResult{}
		if err := unmarshalDurableResult(record, 1, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT, farmResult); err != nil {
			return nil, err
		}
		if err := validateFarmResult(root, farmResult); err != nil {
			return nil, err
		}
		if stepIndex == 2 {
			return deterministicProto(&contractspb.CommitSeedReservationPayload{
				PlayerId:              root.GetOwnerPlayerId(),
				FarmId:                root.GetFarmId(),
				PlotId:                root.GetPlotId(),
				CropCycleId:           root.GetCropCycleId(),
				ReservationId:         root.GetReservationId(),
				CommitPlantResultHash: append([]byte(nil), farmHash[:]...),
			})
		}
		return deterministicProto(&contractspb.ReleaseSeedReservationPayload{
			PlayerId:               root.GetOwnerPlayerId(),
			FarmId:                 root.GetFarmId(),
			PlotId:                 root.GetPlotId(),
			ReservationId:          root.GetReservationId(),
			SeedItemId:             root.GetCropRuleSnapshot().GetSeedItemId(),
			Quantity:               Quantity,
			ReleaseReason:          ReleaseReasonFarmRejected,
			TerminalFarmResultHash: append([]byte(nil), farmHash[:]...),
		})
	default:
		return nil, fmt.Errorf("%w: step index %d", ErrInvalidInput, stepIndex)
	}
}

// Decide folds one terminal participant outcome into the Plant state machine.
// A Farm rejection is the only path to Release. A Player finalization or
// release rejection fails closed because the Farm result must never be
// compensated after it may have committed.
func (*Definition) Decide(record *workflow.WorkRecord, stepIndex int, result workflow.StepResult) (workflow.Decision, error) {
	if stepIndex >= 1 && stepIndex <= 3 {
		if err := validateReserveStepStatus(record); err != nil {
			return workflow.Decision{}, err
		}
	}
	switch stepIndex {
	case 0:
		switch result.Outcome {
		case workflow.StepOutcomeApplied, workflow.StepOutcomeNoChange:
			return workflow.Decision{NextStep: 1}, nil
		case workflow.StepOutcomeRejected:
			return workflow.Decision{Terminal: workflow.WorkStateRejected}, nil
		default:
			return workflow.Decision{}, fmt.Errorf("%w: ReserveSeed outcome %d", ErrInvalidInput, result.Outcome)
		}
	case 1:
		switch result.Outcome {
		case workflow.StepOutcomeApplied, workflow.StepOutcomeNoChange:
			return workflow.Decision{NextStep: 2}, nil
		case workflow.StepOutcomeRejected:
			return workflow.Decision{NextStep: 3}, nil
		default:
			return workflow.Decision{}, fmt.Errorf("%w: CommitPlant outcome %d", ErrInvalidInput, result.Outcome)
		}
	case 2:
		switch result.Outcome {
		case workflow.StepOutcomeApplied, workflow.StepOutcomeNoChange:
			if err := validateFarmBranchResult(record, stepIndex); err != nil {
				return workflow.Decision{}, err
			}
			if err := validateFinalPlayerResult(record, stepIndex, result); err != nil {
				return workflow.Decision{}, err
			}
			return workflow.Decision{Terminal: workflow.WorkStateCompleted}, nil
		case workflow.StepOutcomeRejected:
			return workflow.Decision{Terminal: workflow.WorkStateFailed}, nil
		default:
			return workflow.Decision{}, fmt.Errorf("%w: CommitSeedReservation outcome %d", ErrInvalidInput, result.Outcome)
		}
	case 3:
		switch result.Outcome {
		case workflow.StepOutcomeApplied, workflow.StepOutcomeNoChange:
			if err := validateFarmBranchResult(record, stepIndex); err != nil {
				return workflow.Decision{}, err
			}
			if err := validateFinalPlayerResult(record, stepIndex, result); err != nil {
				return workflow.Decision{}, err
			}
			return workflow.Decision{Terminal: workflow.WorkStateCompensated}, nil
		case workflow.StepOutcomeRejected:
			return workflow.Decision{Terminal: workflow.WorkStateFailed}, nil
		default:
			return workflow.Decision{}, fmt.Errorf("%w: ReleaseSeedReservation outcome %d", ErrInvalidInput, result.Outcome)
		}
	default:
		return workflow.Decision{}, fmt.Errorf("%w: step index %d", ErrInvalidInput, stepIndex)
	}
}

// Coordinator is the thin scenario-facing composition around the generic
// workflow.Engine. It creates the durable WorkRecord before calling Resume.
type Coordinator struct {
	repository     workflow.WorkRepository
	definition     *Definition
	engine         *workflow.Engine
	clock          func() time.Time
	deadline       time.Duration
	bucketVer      uint32
	bucketResolver workidentity.BucketResolver
}

// CoordinatorConfig supplies only generic persistence, participant ports and
// physical bucket placement. It contains no Gate or Actor implementation.
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
		repository:     config.Repository,
		definition:     definition,
		engine:         engine,
		clock:          clock,
		deadline:       deadline,
		bucketVer:      config.BucketVersion,
		bucketResolver: config.BucketResolver,
	}, nil
}

// Definition returns the registered Plant definition for worker composition.
func (c *Coordinator) Definition() *Definition {
	if c == nil {
		return nil
	}
	return c.definition
}

// Engine returns the one shared generic engine; it does not create a second
// runtime or repository.
func (c *Coordinator) Engine() *workflow.Engine {
	if c == nil {
		return nil
	}
	return c.engine
}

// Create builds the v2 root and atomically creates or loads its WorkRecord.
// It has no participant side effects.
func (c *Coordinator) Create(ctx context.Context, input PlantInput) (*workflow.WorkRecord, bool, error) {
	if c == nil || c.engine == nil {
		return nil, false, fmt.Errorf("%w: coordinator", ErrInvalidInput)
	}
	root, err := BuildRoot(input)
	if err != nil {
		return nil, false, err
	}
	address, err := root.AddressForResolver(c.bucketVer, c.bucketResolver)
	if err != nil {
		return nil, false, err
	}
	return c.engine.Create(ctx, c.definition, address, input.SubjectPlayerID, input.OperationID,
		root.PayloadBytes, root.PayloadHash, c.clock().Add(c.deadline))
}

// Resume advances an existing record through the same Definition. A worker
// takeover should use Engine.Resume with the lease fence supplied by the
// generic RecoveryLoop; this coordinator method is the unleased online path.
func (c *Coordinator) Resume(ctx context.Context, record *workflow.WorkRecord) error {
	if c == nil || c.engine == nil {
		return fmt.Errorf("%w: coordinator", ErrInvalidInput)
	}
	return c.engine.Resume(ctx, record, workflow.LeaseFence{}, nil)
}

// Start creates the WorkRecord first and then performs one online resume pass.
// ErrRetryScheduled means the durable record is safe to recover from the due
// index; callers should retain the original operation ID.
func (c *Coordinator) Start(ctx context.Context, input PlantInput) (*workflow.WorkRecord, bool, error) {
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

func decodeRoot(record *workflow.WorkRecord) (*contractspb.CommitPlantPayload, error) {
	if record == nil {
		return nil, fmt.Errorf("%w: nil record", ErrRootPayload)
	}
	if record.Address.WorkType != string(identity.WorkTypePlant) {
		return nil, fmt.Errorf("%w: work type", ErrRootPayload)
	}
	root := &contractspb.CommitPlantPayload{}
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
	if root.GetOwnerPlayerId() == 0 || root.GetOwnerPlayerId() != record.SubjectPlayerID || root.GetFarmId() != root.GetOwnerPlayerId() || root.GetPlotId() == 0 || root.GetPlotId() > participantMaxPlots ||
		root.GetExpectedPlotVersion() == 0 || root.GetReservationId() == "" || root.GetCropCycleId() == "" ||
		root.GetCropRuleSnapshot() == nil || len(root.GetCropRuleSnapshotHash()) != 32 || len(root.GetReserveSeedResultHash()) != 0 {
		return nil, fmt.Errorf("%w: root fields", ErrRootPayload)
	}
	if err := validateCropRuleSnapshot(root.GetCropRuleSnapshot()); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrRootPayload, err)
	}
	snapshotHash, err := contracts.CanonicalHash(root.GetCropRuleSnapshot())
	if err != nil || !bytes.Equal(snapshotHash[:], root.GetCropRuleSnapshotHash()) {
		return nil, fmt.Errorf("%w: crop rule snapshot hash", ErrRootPayload)
	}
	key := identity.EffectiveIdempotencyKey{
		SubjectPlayerID:       record.SubjectPlayerID,
		StableCommandTypeName: string(identity.WorkTypePlant),
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
	wantReservation, err := identity.SeedReservationID(record.Address.WorkID)
	if err != nil || wantReservation != root.GetReservationId() {
		return nil, fmt.Errorf("%w: reservation identity", ErrRootPayload)
	}
	wantCycle, err := identity.CropCycleID(record.Address.WorkID)
	if err != nil || wantCycle != root.GetCropCycleId() {
		return nil, fmt.Errorf("%w: crop cycle identity", ErrRootPayload)
	}
	return root, nil
}

func durableResultHash(record *workflow.WorkRecord, index int, command contractspb.ParticipantCommandType, result proto.Message) ([32]byte, error) {
	if err := unmarshalDurableResult(record, index, command, result); err != nil {
		return [32]byte{}, err
	}
	step := &record.Steps[index]
	return step.ResultHash, nil
}

func validateFarmBranchStatus(record *workflow.WorkRecord, stepIndex int) error {
	if record == nil || len(record.Steps) <= 1 {
		return fmt.Errorf("%w: Farm step is missing", ErrPriorResult)
	}
	status := record.Steps[1].Status
	switch stepIndex {
	case 2:
		if status != workflow.StepApplied && status != workflow.StepNoChange {
			return fmt.Errorf("%w: CommitSeedReservation requires successful Farm status, got %s", ErrPriorResult, status)
		}
	case 3:
		if status != workflow.StepRejected {
			return fmt.Errorf("%w: ReleaseSeedReservation requires rejected Farm status, got %s", ErrPriorResult, status)
		}
	default:
		return fmt.Errorf("%w: Farm branch step %d", ErrPriorResult, stepIndex)
	}
	return nil
}

func validateReserveStepStatus(record *workflow.WorkRecord) error {
	if record == nil || len(record.Steps) == 0 {
		return fmt.Errorf("%w: ReserveSeed step is missing", ErrPriorResult)
	}
	status := record.Steps[0].Status
	if status != workflow.StepApplied && status != workflow.StepNoChange {
		return fmt.Errorf("%w: downstream step requires successful ReserveSeed status, got %s", ErrPriorResult, status)
	}
	return nil
}

func validateFarmBranchResult(record *workflow.WorkRecord, stepIndex int) error {
	if err := validateFarmBranchStatus(record, stepIndex); err != nil {
		return err
	}
	root, err := decodeRoot(record)
	if err != nil {
		return err
	}
	farmResult := &contractspb.CommitPlantResult{}
	if err := unmarshalDurableResult(record, 1, contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT, farmResult); err != nil {
		return err
	}
	return validateFarmResult(root, farmResult)
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

func validateFarmResult(root *contractspb.CommitPlantPayload, result *contractspb.CommitPlantResult) error {
	if root == nil || result == nil || result.GetFarmId() != root.GetFarmId() || result.GetPlotId() != root.GetPlotId() ||
		result.GetCropCycleId() != root.GetCropCycleId() || result.GetCropDefinitionId() != root.GetCropRuleSnapshot().GetCropDefinitionId() ||
		result.GetAppliedCropRuleVersion() != root.GetCropRuleSnapshot().GetCropRuleVersion() {
		return fmt.Errorf("%w: Farm result identity", ErrPriorResult)
	}
	return nil
}

func validateFinalPlayerResult(record *workflow.WorkRecord, stepIndex int, result workflow.StepResult) error {
	root, err := decodeRoot(record)
	if err != nil {
		return err
	}
	if len(result.Result) == 0 || result.ResultHash == ([32]byte{}) {
		return fmt.Errorf("%w: final Player typed result is missing", ErrPriorResult)
	}
	var (
		command contractspb.ParticipantCommandType
		message proto.Message
	)
	switch stepIndex {
	case 2:
		command = contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_SEED_RESERVATION
		message = &contractspb.CommitSeedReservationResult{}
	case 3:
		command = contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RELEASE_SEED_RESERVATION
		message = &contractspb.ReleaseSeedReservationResult{}
	default:
		return fmt.Errorf("%w: final Player step %d", ErrPriorResult, stepIndex)
	}
	if err := proto.Unmarshal(result.Result, message); err != nil {
		return fmt.Errorf("%w: final Player protobuf: %v", ErrPriorResult, err)
	}
	deterministic, err := deterministicProto(message)
	if err != nil || !bytes.Equal(deterministic, result.Result) {
		return fmt.Errorf("%w: final Player result bytes", ErrPriorResult)
	}
	spec, err := contracts.LookupParticipantCommand(command)
	if err != nil {
		return fmt.Errorf("%w: final Player command: %v", ErrPriorResult, err)
	}
	hash, err := contracts.CanonicalParticipantHash(spec, message)
	if err != nil || hash != result.ResultHash {
		return fmt.Errorf("%w: final Player result hash", ErrPriorResult)
	}
	switch message := message.(type) {
	case *contractspb.CommitSeedReservationResult:
		if message.GetReservationId() != root.GetReservationId() ||
			message.GetSeedItemId() != root.GetCropRuleSnapshot().GetSeedItemId() ||
			message.GetReservationState() != contractspb.ReservationTerminalState_RESERVATION_TERMINAL_STATE_CONSUMED {
			return fmt.Errorf("%w: CommitSeedReservation result identity/state", ErrPriorResult)
		}
	case *contractspb.ReleaseSeedReservationResult:
		if message.GetReservationId() != root.GetReservationId() ||
			message.GetSeedItemId() != root.GetCropRuleSnapshot().GetSeedItemId() ||
			message.GetReservationState() != contractspb.ReservationTerminalState_RESERVATION_TERMINAL_STATE_RELEASED {
			return fmt.Errorf("%w: ReleaseSeedReservation result identity/state", ErrPriorResult)
		}
	}
	return nil
}

func validateCropRuleSnapshot(snapshot *contractspb.CropRuleSnapshot) error {
	if snapshot == nil || snapshot.GetSchemaVersion() != 1 || snapshot.GetCropDefinitionId() == 0 ||
		snapshot.GetCropRuleVersion() == 0 || snapshot.GetSeedItemId() == 0 || snapshot.GetHarvestItemId() == 0 ||
		snapshot.GetGrowDurationMs() <= 0 || snapshot.GetBaseYield() == 0 ||
		snapshot.GetBaseYield() > uint64(math.MaxInt32) || snapshot.GetBaseYield()%50 != 0 ||
		snapshot.GetCropDefinitionId() > uint32(math.MaxInt32) || snapshot.GetCropRuleVersion() > uint64(math.MaxInt32) {
		return errors.New("CropRuleSnapshot top-level fields are not valid for Farm CommitPlant")
	}
	compendium := snapshot.GetCompendiumRule()
	if compendium == nil || compendium.GetCropDefinitionId() != snapshot.GetCropDefinitionId() ||
		compendium.GetCatalogVersionAtPlant() == 0 || compendium.GetDefinitionVersionAtPlant() == 0 {
		return errors.New("CropRuleSnapshot compendium template is incomplete")
	}
	steal := snapshot.GetStealPlanTemplate()
	if steal == nil || steal.GetPolicyVersion() == 0 || steal.GetTotalCapRatioBp() != 2000 || len(steal.GetSlotRatioBp()) != 10 {
		return errors.New("CropRuleSnapshot steal template is not P0")
	}
	for _, ratio := range steal.GetSlotRatioBp() {
		if ratio != 200 {
			return errors.New("CropRuleSnapshot steal template must have ten 2-percent slots")
		}
	}
	care := snapshot.GetCarePlanTemplate()
	if care == nil || care.GetCareRuleVersion() == 0 || len(care.GetIncidents()) > participantMaxCareItems {
		return errors.New("CropRuleSnapshot care template is incomplete")
	}
	seenCareTypes := make(map[contractspb.CareType]struct{}, len(care.GetIncidents()))
	for _, incident := range care.GetIncidents() {
		if incident == nil || incident.GetOrdinal() != 1 {
			return errors.New("CropRuleSnapshot care incident is incomplete")
		}
		if _, exists := seenCareTypes[incident.GetCareType()]; exists {
			return errors.New("CropRuleSnapshot care incident type is duplicated")
		}
		seenCareTypes[incident.GetCareType()] = struct{}{}
		switch incident.GetCareType() {
		case contractspb.CareType_CARE_TYPE_WATER, contractspb.CareType_CARE_TYPE_REMOVE_PEST:
		default:
			return errors.New("CropRuleSnapshot care incident type is unknown")
		}
		switch incident.GetScheduleMode() {
		case contractspb.CareScheduleMode_CARE_SCHEDULE_MODE_FIXED_OFFSET:
			if incident.FixedOffsetMs == nil || incident.GetFixedOffsetMs() <= 0 || incident.GetFixedOffsetMs() >= snapshot.GetGrowDurationMs() ||
				incident.RandomWindowStartMs != nil || incident.RandomWindowEndMs != nil || incident.RandomAlgorithmVersion != nil {
				return errors.New("CropRuleSnapshot fixed care incident is invalid")
			}
		case contractspb.CareScheduleMode_CARE_SCHEDULE_MODE_DETERMINISTIC_RANDOM_WINDOW:
			if incident.RandomWindowStartMs == nil || incident.RandomWindowEndMs == nil || incident.RandomAlgorithmVersion == nil ||
				incident.GetRandomAlgorithmVersion() == 0 || incident.GetRandomWindowStartMs() <= 0 ||
				incident.GetRandomWindowEndMs() < incident.GetRandomWindowStartMs() || incident.GetRandomWindowEndMs() >= snapshot.GetGrowDurationMs() || incident.FixedOffsetMs != nil {
				return errors.New("CropRuleSnapshot random care incident is invalid")
			}
		default:
			return errors.New("CropRuleSnapshot care incident schedule is unknown")
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
