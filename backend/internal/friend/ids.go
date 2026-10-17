package friend

// Stable id derivation for the friendship authority. All ids use the shared
// SCB-1 StableOpaqueID algorithm (contract 31/33A §2.7); golden vectors are
// pinned in ids_test.go.

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"strings"

	"farmgame/internal/workidentity"
)

// PairMutationIdentityKind preserves the frozen operation identity domain
// used by canonical RedeemShare and DeleteFriend. The numeric values are
// intentionally stable across retirement of the old roster receipt store.
type PairMutationIdentityKind uint8

const (
	PairMutationIdentityRedeemShare  PairMutationIdentityKind = 1
	PairMutationIdentityDeleteFriend PairMutationIdentityKind = 2
)

// ValidPair reports whether two non-zero distinct players form a pair.
func ValidPair(left, right uint64) bool { return left != 0 && right != 0 && left != right }

// CanonicalPair returns the stable low/high player ordering.
func CanonicalPair(left, right uint64) (uint64, uint64) {
	if left > right {
		left, right = right, left
	}
	return left, right
}

// PairMutationIdentity derives the canonical application operation id and
// payload hash. Its byte layout is frozen so replay identity does not change
// when the retired roster Store and its receipts are physically removed.
func PairMutationIdentity(kind PairMutationIdentityKind, left, right uint64, source string, expectedEpoch uint64) (string, []byte, error) {
	if !ValidPair(left, right) ||
		(kind != PairMutationIdentityRedeemShare && kind != PairMutationIdentityDeleteFriend) ||
		strings.TrimSpace(source) == "" {
		return "", nil, ErrInvalidPair
	}
	left, right = CanonicalPair(left, right)
	buffer := make([]byte, 0, 2+8+8+8+len(source))
	var number [8]byte
	buffer = append(buffer, byte(kind))
	binary.BigEndian.PutUint64(number[:], left)
	buffer = append(buffer, number[:]...)
	binary.BigEndian.PutUint64(number[:], right)
	buffer = append(buffer, number[:]...)
	binary.BigEndian.PutUint64(number[:], expectedEpoch)
	buffer = append(buffer, number[:]...)
	buffer = append(buffer, []byte(source)...)
	sum := sha256.Sum256(buffer)
	return fmt.Sprintf("friend-%d-%x", kind, sum[:12]), sum[:], nil
}

func stableID(namespace string, fields ...string) (string, error) {
	return workidentity.StableOpaqueID(namespace, fields...)
}

// AddFriendWorkflowID derives the deterministic workflow id of one
// REDEEM_SHARE operation (33A §2.7).
func AddFriendWorkflowID(redeemerPlayerID uint64, operationID, pairID string) (string, error) {
	return stableID("farm-workflow-id-v2", itoa(redeemerPlayerID), "REDEEM_SHARE", operationID, "FRIEND_PAIR", pairID)
}

// QuotaReservationID derives the reservation id of one workflow participant
// (33 §5.1, 33A §5.3).
func QuotaReservationID(workflowID string, playerID uint64, relationID string) (string, error) {
	return stableID("farm-friend-quota-reservation-id-v1", workflowID, itoa(playerID), relationID)
}

// ShareTokenValidationReceiptID derives the validation receipt id (33A §2.7).
func ShareTokenValidationReceiptID(workflowID string, inviterPlayerID, redeemerPlayerID, tokenGeneration uint64) (string, error) {
	return stableID("farm-share-token-validation-receipt-id-v1", workflowID, itoa(inviterPlayerID), itoa(redeemerPlayerID), itoa(tokenGeneration))
}

// QuotaReleaseFenceID derives the relation release fence id (33A §5.4).
func QuotaReleaseFenceID(playerID uint64, relationID string, capturedRelationEpoch uint64) (string, error) {
	return stableID("farm-friend-quota-release-fence-v1", itoa(playerID), relationID, itoa(capturedRelationEpoch))
}

// ReactivationRotateReceiptID derives the typed rotate receipt id binding
// one barrier revision (33A §6.4).
func ReactivationRotateReceiptID(pairID string, barrierRevision uint64, tokenID string) (string, error) {
	return stableID("farm-friend-reactivation-rotate-receipt-id-v1", pairID, itoa(barrierRevision), tokenID)
}

// ActionDispatchOrderID derives the FriendActionDispatchOrder id
// (33A §4.8): source_fact_id = operation digest, source_fact_version =
// accepted relation epoch.
func ActionDispatchOrderID(pairID, operationDigest string, acceptedRelationEpoch uint64) (string, error) {
	return stableID("farm-source-order-id-v1", "FRIEND_ACTION_DISPATCH_ORDER", "FRIEND_PAIR", pairID, operationDigest, itoa(acceptedRelationEpoch))
}
