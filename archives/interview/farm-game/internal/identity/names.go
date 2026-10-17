package identity

import (
	"strconv"
	"unicode/utf8"
)

// WorkType is a frozen stable work-type name (contract 29 §6.2, 31 §8.1A).
// The string value is the exact input to StableOpaqueID derivations; never
// localize, rename, or derive it from enum ordinals.
type WorkType string

const (
	WorkTypeUnlockPlot                WorkType = "UNLOCK_PLOT"
	WorkTypePlant                     WorkType = "PLANT"
	WorkTypeHarvest                   WorkType = "HARVEST"
	WorkTypeTheft                     WorkType = "THEFT"
	WorkTypeTheftVisitor              WorkType = "THEFT_VISITOR"
	WorkTypeTheftGuard                WorkType = "THEFT_GUARD"
	WorkTypeAddFriend                 WorkType = "ADD_FRIEND"
	WorkTypeCareReward                WorkType = "CARE_REWARD"
	WorkTypeGuardSyncOrder            WorkType = "GUARD_SYNC_ORDER"
	WorkTypeFriendTerminationOrder    WorkType = "FRIEND_TERMINATION_ORDER"
	WorkTypeFriendActionDispatchOrder WorkType = "FRIEND_ACTION_DISPATCH_ORDER"
	WorkTypeResultPublishOrder        WorkType = "RESULT_PUBLISH_ORDER"
	WorkTypeMailDeliveryOrder         WorkType = "MAIL_DELIVERY_ORDER"
	WorkTypeMailClaimProjectionOrder  WorkType = "MAIL_CLAIM_PROJECTION_ORDER"
)

// WorkTypes lists every registered P0 work type (contract 29 §6.2 +
// 27 §7.8.2). Unknown combinations must fail closed at repositories and
// ingresses, so this list is the single registration point.
var WorkTypes = []WorkType{
	WorkTypeUnlockPlot,
	WorkTypePlant,
	WorkTypeHarvest,
	WorkTypeTheft,
	WorkTypeTheftVisitor,
	WorkTypeTheftGuard,
	WorkTypeAddFriend,
	WorkTypeCareReward,
	WorkTypeGuardSyncOrder,
	WorkTypeFriendTerminationOrder,
	WorkTypeFriendActionDispatchOrder,
	WorkTypeResultPublishOrder,
	WorkTypeMailDeliveryOrder,
	WorkTypeMailClaimProjectionOrder,
}

// ParticipantStep is a frozen stable participant-step name (contract 29
// §6.2/§7.8, 33 §3.3, 29 §7.16). Free-form strings are forbidden for
// correctness state.
type ParticipantStep string

const (
	StepReserveUnlockCoin            ParticipantStep = "RESERVE_UNLOCK_COIN"
	StepCommitPlotUnlock             ParticipantStep = "COMMIT_PLOT_UNLOCK"
	StepCommitUnlockCoinReservation  ParticipantStep = "COMMIT_UNLOCK_COIN_RESERVATION"
	StepReleaseUnlockCoinReservation ParticipantStep = "RELEASE_UNLOCK_COIN_RESERVATION"
	StepReserveSeed                  ParticipantStep = "RESERVE_SEED"
	StepCommitPlant                  ParticipantStep = "COMMIT_PLANT"
	StepCommitSeedReservation        ParticipantStep = "COMMIT_SEED_RESERVATION"
	StepReleaseSeedReservation       ParticipantStep = "RELEASE_SEED_RESERVATION"
	StepCommitHarvest                ParticipantStep = "COMMIT_HARVEST"
	StepGrantHarvestReward           ParticipantStep = "GRANT_HARVEST_REWARD"
	StepDecideTheft                  ParticipantStep = "DECIDE_THEFT"
	StepSettleTheftVisitor           ParticipantStep = "SETTLE_THEFT_VISITOR"
	StepCreditGuardIncome            ParticipantStep = "CREDIT_GUARD_INCOME"
	StepApplyCareReward              ParticipantStep = "APPLY_CARE_REWARD"
	StepApplyGuardSnapshot           ParticipantStep = "APPLY_GUARD_SNAPSHOT"
	StepCaptureShareGenerationFloor  ParticipantStep = "CAPTURE_SHARE_GENERATION_FLOOR"
	StepCommitReactivationFloors     ParticipantStep = "COMMIT_REACTIVATION_GENERATION_FLOORS"
	StepRemoveActiveRelationIfEpoch  ParticipantStep = "REMOVE_ACTIVE_RELATION_IF_EPOCH_MATCHES"
	StepRevokeFarmSubscription       ParticipantStep = "REVOKE_FARM_SUBSCRIPTION"
	StepMarkFriendIndexDirty         ParticipantStep = "MARK_FRIEND_INDEX_DIRTY"
	StepResumeMailDeliveryOrder      ParticipantStep = "RESUME_MAIL_DELIVERY_ORDER"
	StepApplyMailClaimProjection     ParticipantStep = "APPLY_MAIL_CLAIM_PROJECTION"
)

// ParticipantSteps lists every registered P0 participant step.
var ParticipantSteps = []ParticipantStep{
	StepReserveUnlockCoin,
	StepCommitPlotUnlock,
	StepCommitUnlockCoinReservation,
	StepReleaseUnlockCoinReservation,
	StepReserveSeed,
	StepCommitPlant,
	StepCommitSeedReservation,
	StepReleaseSeedReservation,
	StepCommitHarvest,
	StepGrantHarvestReward,
	StepDecideTheft,
	StepSettleTheftVisitor,
	StepCreditGuardIncome,
	StepApplyCareReward,
	StepApplyGuardSnapshot,
	StepCaptureShareGenerationFloor,
	StepCommitReactivationFloors,
	StepRemoveActiveRelationIfEpoch,
	StepRevokeFarmSubscription,
	StepMarkFriendIndexDirty,
	StepResumeMailDeliveryOrder,
	StepApplyMailClaimProjection,
}

// SourceAuthorityKind is a frozen stable source-authority name (contract 31
// §8.1A). Actor sources additionally carry and re-check a RouteDomain;
// non-actor sources (FRIEND_PAIR) must never forge one.
type SourceAuthorityKind string

const (
	SourceKindPlayerActor SourceAuthorityKind = "PLAYER_ACTOR"
	SourceKindFarmActor   SourceAuthorityKind = "FARM_ACTOR"
	SourceKindFriendPair  SourceAuthorityKind = "FRIEND_PAIR"
)

// SourceAuthorityKinds lists every registered source authority kind.
var SourceAuthorityKinds = []SourceAuthorityKind{
	SourceKindPlayerActor,
	SourceKindFarmActor,
	SourceKindFriendPair,
}

// CanonicalUint renders an unsigned integer as decimal ASCII without sign
// or leading zeros, as required for every canonical_* hash input (contract
// 31 §8.1). Zero is rejected because canonical entity IDs are positive.
func CanonicalUint(v uint64) (string, error) {
	if v == 0 {
		return "", ErrInvalidIdentity
	}
	return strconv.FormatUint(v, 10), nil
}

// validStableName enforces the registry-frozen uppercase ASCII name shape
// [A-Z0-9_]{1,64} shared with internal/workidentity.
func validStableName(value string) bool {
	if len(value) == 0 || len(value) > 64 || !utf8.ValidString(value) {
		return false
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		if (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '_' {
			return false
		}
	}
	return true
}
