package contracts

import (
	"fmt"
	"strings"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/identity"

	"google.golang.org/protobuf/proto"
)

// ParticipantCommandSpec is the frozen registry entry of one participant
// command (contract 29 §7.8). The catalog declares schema bindings and
// routing shape only; handlers and business adjudication are wired by
// later tasks, never by SCB-1.
type ParticipantCommandSpec struct {
	Command contractspb.ParticipantCommandType
	// Step is the stable participant-step name; for the fifteen frozen
	// commands it always equals the command name (29 §7.8).
	Step identity.ParticipantStep
	// WorkType is the only work type allowed to first-execute this step.
	WorkType identity.WorkType
	// TargetRouteDomain is the authority actor domain of the participant.
	TargetRouteDomain logicrpcpb.RouteDomain
	NewPayload        func() proto.Message
	NewResult         func() proto.Message
	MaxPayloadBytes   int
}

// StableCommandName maps a wire enum to its registry-frozen stable name.
func StableCommandName(cmd contractspb.ParticipantCommandType) (string, error) {
	name := contractspb.ParticipantCommandType_name[int32(cmd)]
	if name == "" || cmd == contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_UNSPECIFIED {
		return "", fmt.Errorf("contracts: unknown participant command %d", int32(cmd))
	}
	return strings.TrimPrefix(name, "PARTICIPANT_COMMAND_TYPE_"), nil
}

// StableWorkTypeName maps a wire enum to its registry-frozen stable name.
func StableWorkTypeName(t contractspb.WorkType) (string, error) {
	name := contractspb.WorkType_name[int32(t)]
	if name == "" || t == contractspb.WorkType_WORK_TYPE_UNSPECIFIED {
		return "", fmt.Errorf("contracts: unknown work type %d", int32(t))
	}
	return strings.TrimPrefix(name, "WORK_TYPE_"), nil
}

// StableStepName maps a wire enum to its registry-frozen stable name.
func StableStepName(s contractspb.ParticipantStep) (string, error) {
	name := contractspb.ParticipantStep_name[int32(s)]
	if name == "" || s == contractspb.ParticipantStep_PARTICIPANT_STEP_UNSPECIFIED {
		return "", fmt.Errorf("contracts: unknown participant step %d", int32(s))
	}
	return strings.TrimPrefix(name, "PARTICIPANT_STEP_"), nil
}

const defaultMaxStepPayloadBytes = 16 * 1024

var participantCatalog = []ParticipantCommandSpec{
	{
		Command: contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_UNLOCK_COIN,
		Step:    identity.StepReserveUnlockCoin, WorkType: identity.WorkTypeUnlockPlot,
		TargetRouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER,
		NewPayload:        func() proto.Message { return &contractspb.ReserveUnlockCoinPayload{} },
		NewResult:         func() proto.Message { return &contractspb.ReserveUnlockCoinResult{} },
		MaxPayloadBytes:   defaultMaxStepPayloadBytes,
	},
	{
		Command: contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLOT_UNLOCK,
		Step:    identity.StepCommitPlotUnlock, WorkType: identity.WorkTypeUnlockPlot,
		TargetRouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM,
		NewPayload:        func() proto.Message { return &contractspb.CommitPlotUnlockPayload{} },
		NewResult:         func() proto.Message { return &contractspb.CommitPlotUnlockResult{} },
		MaxPayloadBytes:   defaultMaxStepPayloadBytes,
	},
	{
		Command: contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_UNLOCK_COIN_RESERVATION,
		Step:    identity.StepCommitUnlockCoinReservation, WorkType: identity.WorkTypeUnlockPlot,
		TargetRouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER,
		NewPayload:        func() proto.Message { return &contractspb.CommitUnlockCoinReservationPayload{} },
		NewResult:         func() proto.Message { return &contractspb.CommitUnlockCoinReservationResult{} },
		MaxPayloadBytes:   defaultMaxStepPayloadBytes,
	},
	{
		Command: contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RELEASE_UNLOCK_COIN_RESERVATION,
		Step:    identity.StepReleaseUnlockCoinReservation, WorkType: identity.WorkTypeUnlockPlot,
		TargetRouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER,
		NewPayload:        func() proto.Message { return &contractspb.ReleaseUnlockCoinReservationPayload{} },
		NewResult:         func() proto.Message { return &contractspb.ReleaseUnlockCoinReservationResult{} },
		MaxPayloadBytes:   defaultMaxStepPayloadBytes,
	},
	{
		Command: contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED,
		Step:    identity.StepReserveSeed, WorkType: identity.WorkTypePlant,
		TargetRouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER,
		NewPayload:        func() proto.Message { return &contractspb.ReserveSeedPayload{} },
		NewResult:         func() proto.Message { return &contractspb.ReserveSeedResult{} },
		MaxPayloadBytes:   defaultMaxStepPayloadBytes,
	},
	{
		Command: contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT,
		Step:    identity.StepCommitPlant, WorkType: identity.WorkTypePlant,
		TargetRouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM,
		NewPayload:        func() proto.Message { return &contractspb.CommitPlantPayload{} },
		NewResult:         func() proto.Message { return &contractspb.CommitPlantResult{} },
		MaxPayloadBytes:   defaultMaxStepPayloadBytes,
	},
	{
		Command: contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_SEED_RESERVATION,
		Step:    identity.StepCommitSeedReservation, WorkType: identity.WorkTypePlant,
		TargetRouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER,
		NewPayload:        func() proto.Message { return &contractspb.CommitSeedReservationPayload{} },
		NewResult:         func() proto.Message { return &contractspb.CommitSeedReservationResult{} },
		MaxPayloadBytes:   defaultMaxStepPayloadBytes,
	},
	{
		Command: contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RELEASE_SEED_RESERVATION,
		Step:    identity.StepReleaseSeedReservation, WorkType: identity.WorkTypePlant,
		TargetRouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER,
		NewPayload:        func() proto.Message { return &contractspb.ReleaseSeedReservationPayload{} },
		NewResult:         func() proto.Message { return &contractspb.ReleaseSeedReservationResult{} },
		MaxPayloadBytes:   defaultMaxStepPayloadBytes,
	},
	{
		Command: contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_HARVEST,
		Step:    identity.StepCommitHarvest, WorkType: identity.WorkTypeHarvest,
		TargetRouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM,
		NewPayload:        func() proto.Message { return &contractspb.CommitHarvestPayload{} },
		NewResult:         func() proto.Message { return &contractspb.CommitHarvestResult{} },
		MaxPayloadBytes:   defaultMaxStepPayloadBytes,
	},
	{
		Command: contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD,
		Step:    identity.StepGrantHarvestReward, WorkType: identity.WorkTypeHarvest,
		TargetRouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER,
		NewPayload:        func() proto.Message { return &contractspb.GrantHarvestRewardPayload{} },
		NewResult:         func() proto.Message { return &contractspb.GrantHarvestRewardResult{} },
		MaxPayloadBytes:   defaultMaxStepPayloadBytes,
	},
	{
		Command: contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT,
		Step:    identity.StepDecideTheft, WorkType: identity.WorkTypeTheft,
		TargetRouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM,
		NewPayload:        func() proto.Message { return &contractspb.DecideTheftPayload{} },
		NewResult:         func() proto.Message { return &contractspb.DecideTheftResult{} },
		MaxPayloadBytes:   defaultMaxStepPayloadBytes,
	},
	{
		Command: contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_SETTLE_THEFT_VISITOR,
		Step:    identity.StepSettleTheftVisitor, WorkType: identity.WorkTypeTheft,
		TargetRouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER,
		NewPayload:        func() proto.Message { return &contractspb.SettleTheftVisitorPayload{} },
		NewResult:         func() proto.Message { return &contractspb.SettleTheftVisitorResult{} },
		MaxPayloadBytes:   defaultMaxStepPayloadBytes,
	},
	{
		Command: contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_CREDIT_GUARD_INCOME,
		Step:    identity.StepCreditGuardIncome, WorkType: identity.WorkTypeTheft,
		TargetRouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER,
		NewPayload:        func() proto.Message { return &contractspb.CreditGuardIncomePayload{} },
		NewResult:         func() proto.Message { return &contractspb.CreditGuardIncomeResult{} },
		MaxPayloadBytes:   defaultMaxStepPayloadBytes,
	},
	{
		Command: contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_CARE_REWARD,
		Step:    identity.StepApplyCareReward, WorkType: identity.WorkTypeCareReward,
		TargetRouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER,
		NewPayload:        func() proto.Message { return &contractspb.ApplyCareRewardPayload{} },
		NewResult:         func() proto.Message { return &contractspb.ApplyCareRewardResult{} },
		MaxPayloadBytes:   defaultMaxStepPayloadBytes,
	},
	{
		Command: contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_GUARD_SNAPSHOT,
		Step:    identity.StepApplyGuardSnapshot, WorkType: identity.WorkTypeGuardSyncOrder,
		TargetRouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM,
		NewPayload:        func() proto.Message { return &contractspb.ApplyGuardSnapshotPayload{} },
		NewResult:         func() proto.Message { return &contractspb.ApplyGuardSnapshotResult{} },
		MaxPayloadBytes:   defaultMaxStepPayloadBytes,
	},
}

// LookupParticipantCommand returns the registered spec for a command.
// Unknown commands must fail closed before any actor mailbox.
func LookupParticipantCommand(cmd contractspb.ParticipantCommandType) (ParticipantCommandSpec, error) {
	for _, spec := range participantCatalog {
		if spec.Command == cmd {
			return spec, nil
		}
	}
	return ParticipantCommandSpec{}, fmt.Errorf("contracts: participant command %d not registered", int32(cmd))
}

// ParticipantCatalog returns a copy of the full frozen catalog.
func ParticipantCatalog() []ParticipantCommandSpec {
	out := make([]ParticipantCommandSpec, len(participantCatalog))
	copy(out, participantCatalog)
	return out
}

// WorkTypeSpec is the frozen registration of one work type (contract 29
// §6.2 + 27 §7.8.2): placement, allowed source authority kinds and the
// participant steps it may first-execute.
type WorkTypeSpec struct {
	WorkType  identity.WorkType
	Placement contractspb.WorkPlacementKind
	// AllowedSourceKinds is empty for INDEPENDENT_WORK_BUCKET. Source
	// orders must declare their source authority kind explicitly.
	AllowedSourceKinds []identity.SourceAuthorityKind
	// Steps lists the frozen participant steps of this work type. Orders
	// whose steps are authority RPCs (no actor participant) stay empty.
	Steps []identity.ParticipantStep
}

var workTypeRegistry = []WorkTypeSpec{
	{WorkType: identity.WorkTypeUnlockPlot, Placement: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_INDEPENDENT_WORK_BUCKET,
		Steps: []identity.ParticipantStep{identity.StepReserveUnlockCoin, identity.StepCommitPlotUnlock, identity.StepCommitUnlockCoinReservation, identity.StepReleaseUnlockCoinReservation}},
	{WorkType: identity.WorkTypePlant, Placement: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_INDEPENDENT_WORK_BUCKET,
		Steps: []identity.ParticipantStep{identity.StepReserveSeed, identity.StepCommitPlant, identity.StepCommitSeedReservation, identity.StepReleaseSeedReservation}},
	{WorkType: identity.WorkTypeHarvest, Placement: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_INDEPENDENT_WORK_BUCKET,
		Steps: []identity.ParticipantStep{identity.StepCommitHarvest, identity.StepGrantHarvestReward}},
	{WorkType: identity.WorkTypeTheft, Placement: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_INDEPENDENT_WORK_BUCKET,
		Steps: []identity.ParticipantStep{identity.StepDecideTheft, identity.StepSettleTheftVisitor, identity.StepCreditGuardIncome}},
	{WorkType: identity.WorkTypeTheftVisitor, Placement: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET,
		AllowedSourceKinds: []identity.SourceAuthorityKind{identity.SourceKindFarmActor},
		Steps:              []identity.ParticipantStep{identity.StepSettleTheftVisitor}},
	{WorkType: identity.WorkTypeTheftGuard, Placement: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET,
		AllowedSourceKinds: []identity.SourceAuthorityKind{identity.SourceKindFarmActor},
		Steps:              []identity.ParticipantStep{identity.StepCreditGuardIncome}},
	{WorkType: identity.WorkTypeAddFriend, Placement: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_INDEPENDENT_WORK_BUCKET},
	{WorkType: identity.WorkTypeCareReward, Placement: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET,
		AllowedSourceKinds: []identity.SourceAuthorityKind{identity.SourceKindFarmActor},
		Steps:              []identity.ParticipantStep{identity.StepApplyCareReward}},
	{WorkType: identity.WorkTypeGuardSyncOrder, Placement: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET,
		AllowedSourceKinds: []identity.SourceAuthorityKind{identity.SourceKindPlayerActor},
		Steps:              []identity.ParticipantStep{identity.StepApplyGuardSnapshot}},
	{WorkType: identity.WorkTypeFriendTerminationOrder, Placement: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET,
		AllowedSourceKinds: []identity.SourceAuthorityKind{identity.SourceKindFriendPair},
		Steps: []identity.ParticipantStep{
			identity.StepCaptureShareGenerationFloor,
			identity.StepCommitReactivationFloors,
			identity.StepRemoveActiveRelationIfEpoch,
			identity.StepRevokeFarmSubscription,
			identity.StepMarkFriendIndexDirty,
		}},
	{WorkType: identity.WorkTypeFriendActionDispatchOrder, Placement: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET,
		AllowedSourceKinds: []identity.SourceAuthorityKind{identity.SourceKindFriendPair}},
	{WorkType: identity.WorkTypeResultPublishOrder, Placement: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET,
		AllowedSourceKinds: []identity.SourceAuthorityKind{identity.SourceKindPlayerActor, identity.SourceKindFarmActor, identity.SourceKindFriendPair}},
	{WorkType: identity.WorkTypeMailDeliveryOrder, Placement: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET,
		AllowedSourceKinds: []identity.SourceAuthorityKind{identity.SourceKindPlayerActor, identity.SourceKindFarmActor, identity.SourceKindFriendPair},
		Steps:              []identity.ParticipantStep{identity.StepResumeMailDeliveryOrder}},
	{WorkType: identity.WorkTypeMailClaimProjectionOrder, Placement: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET,
		AllowedSourceKinds: []identity.SourceAuthorityKind{identity.SourceKindPlayerActor},
		Steps:              []identity.ParticipantStep{identity.StepApplyMailClaimProjection}},
}

// LookupWorkType returns the registered spec for a work type.
func LookupWorkType(t identity.WorkType) (WorkTypeSpec, error) {
	for _, spec := range workTypeRegistry {
		if spec.WorkType == t {
			return spec, nil
		}
	}
	return WorkTypeSpec{}, fmt.Errorf("contracts: work type %q not registered", t)
}

// WorkTypeRegistry returns a copy of the full work-type registry.
func WorkTypeRegistry() []WorkTypeSpec {
	out := make([]WorkTypeSpec, len(workTypeRegistry))
	copy(out, workTypeRegistry)
	return out
}
