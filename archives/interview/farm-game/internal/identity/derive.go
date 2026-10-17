package identity

import (
	"fmt"

	"farmgame/internal/workidentity"
)

// Frozen StableOpaqueID v1 namespaces (contract 31 §8.1A, 33A §2.7). They
// are part of the protocol: never rename, truncate, or replace with UUIDs.
const (
	nsWorkflowIDV2                 = "farm-workflow-id-v2"
	nsSourceOrderIDV1              = "farm-source-order-id-v1"
	nsFriendRelationIDV1           = "farm-friend-relation-id-v1"
	nsFriendQuotaReservationIDV1   = "farm-friend-quota-reservation-id-v1"
	nsFriendQuotaReleaseFenceV1    = "farm-friend-quota-release-fence-v1"
	nsShareTokenValidationRcptIDV1 = "farm-share-token-validation-receipt-id-v1"
	nsFriendCommitAttemptIDV1      = "friend-commit-attempt-id-v1"
)

// WorkflowID derives the deterministic independent-workflow identity from a
// root-scoped EffectiveIdempotencyKey (contract 27 §6.1 / 31 §8.1A):
//
//	sid1("farm-workflow-id-v2", subject, command, operation_id,
//	     root_kind, canonical_root_id)
//
// workflow_schema_version never enters the input. The legacy
// workidentity.WorkflowID ("farm-workflow-id-v1", 3 fields) remains for the
// current main chain only; new consumers must use this v2 derivation.
func WorkflowID(key EffectiveIdempotencyKey) (string, error) {
	if err := key.Validate(); err != nil {
		return "", err
	}
	subject, err := CanonicalUint(key.SubjectPlayerID)
	if err != nil {
		return "", err
	}
	return workidentity.StableOpaqueID(nsWorkflowIDV2, subject,
		key.StableCommandTypeName, key.OperationID,
		string(key.Root.Kind), key.Root.CanonicalID)
}

// SourceOrderID derives the deterministic source-order identity (contract
// 31 §8.1A). kind is the source authority kind (PLAYER_ACTOR/FARM_ACTOR/
// FRIEND_PAIR), never a route domain guess; canonicalAuthorityID is the
// decimal actor ID or canonical pair ID.
func SourceOrderID(orderType WorkType, kind SourceAuthorityKind, canonicalAuthorityID, sourceFactID string, sourceFactVersion uint64) (string, error) {
	if !validStableName(string(orderType)) {
		return "", fmt.Errorf("%w: order type", ErrInvalidIdentity)
	}
	switch kind {
	case SourceKindPlayerActor, SourceKindFarmActor, SourceKindFriendPair:
	default:
		return "", fmt.Errorf("%w: source authority kind %q", ErrInvalidIdentity, kind)
	}
	if canonicalAuthorityID == "" || sourceFactID == "" {
		return "", fmt.Errorf("%w: source authority id / fact id", ErrInvalidIdentity)
	}
	version, err := CanonicalUint(sourceFactVersion)
	if err != nil {
		return "", fmt.Errorf("%w: source fact version", ErrInvalidIdentity)
	}
	return workidentity.StableOpaqueID(nsSourceOrderIDV1, string(orderType),
		string(kind), canonicalAuthorityID, sourceFactID, version)
}

// FriendRelationID derives the canonical pair relation identity (contract
// 31 §8.1A / 33A §2.7). Callers must pass low/high player IDs in canonical
// order (low < high).
func FriendRelationID(lowPlayerID, highPlayerID uint64) (string, error) {
	low, err := CanonicalUint(lowPlayerID)
	if err != nil {
		return "", err
	}
	high, err := CanonicalUint(highPlayerID)
	if err != nil {
		return "", err
	}
	if lowPlayerID >= highPlayerID {
		return "", fmt.Errorf("%w: friend relation id requires low < high", ErrInvalidIdentity)
	}
	return workidentity.StableOpaqueID(nsFriendRelationIDV1, low, high)
}

// CanonicalFriendRelationID accepts either participant ordering and derives
// the same canonical pair identity. FriendRelationID remains the strict
// low/high golden-vector primitive; operation paths use this wrapper because
// a visitor may be numerically greater than the farm owner.
func CanonicalFriendRelationID(playerA, playerB uint64) (string, error) {
	if playerA == 0 || playerB == 0 || playerA == playerB {
		return "", fmt.Errorf("%w: friend relation requires two distinct players", ErrInvalidIdentity)
	}
	if playerA > playerB {
		playerA, playerB = playerB, playerA
	}
	return FriendRelationID(playerA, playerB)
}

// FriendQuotaReservationID derives the quota reservation identity (contract
// 31 §8.1A / 33A §2.7).
func FriendQuotaReservationID(addFriendWorkflowID string, quotaPlayerID uint64, relationID string) (string, error) {
	if err := workidentity.ValidateStableOpaqueID(addFriendWorkflowID); err != nil {
		return "", fmt.Errorf("%w: add friend workflow id", ErrInvalidIdentity)
	}
	if err := workidentity.ValidateStableOpaqueID(relationID); err != nil {
		return "", fmt.Errorf("%w: relation id", ErrInvalidIdentity)
	}
	player, err := CanonicalUint(quotaPlayerID)
	if err != nil {
		return "", err
	}
	return workidentity.StableOpaqueID(nsFriendQuotaReservationIDV1, addFriendWorkflowID, player, relationID)
}

// FriendQuotaReleaseFenceID derives the quota release fence identity
// (contract 33A §2.7). terminatedRelationEpoch is the captured active epoch
// of the terminated relation, never a post-delete incremented epoch.
func FriendQuotaReleaseFenceID(quotaPlayerID uint64, relationID string, terminatedRelationEpoch uint64) (string, error) {
	if err := workidentity.ValidateStableOpaqueID(relationID); err != nil {
		return "", fmt.Errorf("%w: relation id", ErrInvalidIdentity)
	}
	player, err := CanonicalUint(quotaPlayerID)
	if err != nil {
		return "", err
	}
	epoch, err := CanonicalUint(terminatedRelationEpoch)
	if err != nil {
		return "", fmt.Errorf("%w: terminated relation epoch", ErrInvalidIdentity)
	}
	return workidentity.StableOpaqueID(nsFriendQuotaReleaseFenceV1, player, relationID, epoch)
}

// ShareTokenValidationReceiptID derives the share-token validation receipt
// identity (contract 31 §8.1A / 33A §2.7).
func ShareTokenValidationReceiptID(addFriendWorkflowID string, inviterPlayerID, redeemerPlayerID, tokenGeneration uint64) (string, error) {
	if err := workidentity.ValidateStableOpaqueID(addFriendWorkflowID); err != nil {
		return "", fmt.Errorf("%w: add friend workflow id", ErrInvalidIdentity)
	}
	inviter, err := CanonicalUint(inviterPlayerID)
	if err != nil {
		return "", err
	}
	redeemer, err := CanonicalUint(redeemerPlayerID)
	if err != nil {
		return "", err
	}
	generation, err := CanonicalUint(tokenGeneration)
	if err != nil {
		return "", fmt.Errorf("%w: token generation", ErrInvalidIdentity)
	}
	return workidentity.StableOpaqueID(nsShareTokenValidationRcptIDV1, addFriendWorkflowID, inviter, redeemer, generation)
}

// FriendCommitAttemptID derives the friendship authority commit-attempt
// identity (contract 33A §2.7). attemptDigestHex is the 64-char lowercase
// hex operation digest.
func FriendCommitAttemptID(canonicalPairID, attemptDigestHex string, attemptOrdinal uint64) (string, error) {
	if err := workidentity.ValidateStableOpaqueID(canonicalPairID); err != nil {
		return "", fmt.Errorf("%w: canonical pair id", ErrInvalidIdentity)
	}
	if len(attemptDigestHex) != 64 {
		return "", fmt.Errorf("%w: attempt digest", ErrInvalidIdentity)
	}
	for _, c := range attemptDigestHex {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return "", fmt.Errorf("%w: attempt digest not lowercase hex", ErrInvalidIdentity)
		}
	}
	ordinal, err := CanonicalUint(attemptOrdinal)
	if err != nil {
		return "", fmt.Errorf("%w: attempt ordinal", ErrInvalidIdentity)
	}
	return workidentity.StableOpaqueID(nsFriendCommitAttemptIDV1,
		string(SourceKindFriendPair), canonicalPairID, attemptDigestHex, ordinal)
}

// ParticipantInvocationID derives the deterministic per-step invocation
// identity (contract 29 §6.2/§7.8, 31 §8.1A). Retries, coordinator-to-worker
// handoff and lease changes must never alter it.
func ParticipantInvocationID(workType WorkType, workID string, step ParticipantStep, stepInstance uint64) (string, error) {
	if !validStableName(string(workType)) {
		return "", fmt.Errorf("%w: work type", ErrInvalidIdentity)
	}
	if !validStableName(string(step)) {
		return "", fmt.Errorf("%w: participant step", ErrInvalidIdentity)
	}
	return workidentity.ParticipantInvocationID(string(workType), workID, string(step), stepInstance)
}

// UnlockCoinReservationID derives the unlock-coin reservation identity
// (contract 29 §7.9.1).
func UnlockCoinReservationID(unlockPlotWorkflowID string) (string, error) {
	return workidentity.UnlockCoinReservationID(unlockPlotWorkflowID)
}

// SeedReservationID derives the seed reservation identity (contract 29
// §7.10.1).
func SeedReservationID(plantWorkflowID string) (string, error) {
	return workidentity.SeedReservationID(plantWorkflowID)
}

// CropCycleID derives the crop cycle identity (contract 29 §7.10.2).
func CropCycleID(plantWorkflowID string) (string, error) {
	return workidentity.CropCycleID(plantWorkflowID)
}

// TheftSettlementID derives the theft settlement identity (contract 31
// §8.1A).
func TheftSettlementID(theftWorkflowID string) (string, error) {
	return workidentity.TheftSettlementID(theftWorkflowID)
}

// CareIncidentID derives the care incident identity (contract 29 §7.10.2).
func CareIncidentID(cropCycleID string, careType string, ordinal uint64) (string, error) {
	return workidentity.CareIncidentID(cropCycleID, careType, ordinal)
}

// TheftAttemptID derives the per-friend per-cycle theft attempt identity
// (contract 29 §7.13.1): one business attempt per friend per crop cycle,
// independent of operation_id rotation.
func TheftAttemptID(targetFarmID uint64, cropCycleID string, visitorPlayerID uint64) (string, error) {
	return workidentity.TheftAttemptID(targetFarmID, cropCycleID, visitorPlayerID)
}
