package mail

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"unicode/utf8"

	"farmgame/internal/workidentity"
)

// ErrInvalidIdentity rejects malformed recipients, delivery ids or hashes.
var ErrInvalidIdentity = errors.New("mail: invalid identity input")

// MailIDNamespace is the contract-35 Q35-03 deterministic mail id
// namespace. mail_id = StableOpaqueID("farm-mail-id-v1", recipient,
// delivery_id).
const MailIDNamespace = "farm-mail-id-v1"

// hash namespaces for the three separated hashes (35 M5/Q35-03).
const (
	deliveryPayloadHashNS = "farm-mail-delivery-payload-hash-v1"
	claimMaterialHashNS   = "farm-mail-claim-material-hash-v1"
)

// digest namespaces for Redis hash-field identities (35A §2.4/§5). Digests
// only locate fields; the stored blobs still carry the full identity for
// collision and wrong-payload verification.
const (
	deliveryDigestNS   = "farm-mail-delivery-digest-v1"
	mutationDigestNS   = "farm-mail-mutation-digest-v1"
	projectionDigestNS = "farm-mail-projection-digest-v1"
)

// CanonicalPlayerID renders a player id as an unsigned decimal ASCII string
// with no sign and no leading zeros (31 §8.1).
func CanonicalPlayerID(playerID uint64) (string, error) {
	if playerID == 0 {
		return "", fmt.Errorf("%w: player id", ErrInvalidIdentity)
	}
	return strconv.FormatUint(playerID, 10), nil
}

// ValidateDeliveryID accepts server-generated printable ASCII delivery ids.
// Clients never supply delivery ids (35 Q35-02).
func ValidateDeliveryID(deliveryID string) error {
	if len(deliveryID) < 1 || len(deliveryID) > 128 {
		return fmt.Errorf("%w: delivery id length", ErrInvalidIdentity)
	}
	for i := 0; i < len(deliveryID); i++ {
		if deliveryID[i] < 0x21 || deliveryID[i] > 0x7e {
			return fmt.Errorf("%w: delivery id charset", ErrInvalidIdentity)
		}
	}
	return nil
}

// ValidateMailID enforces the StableOpaqueID shape (35 Q35-03).
func ValidateMailID(mailID string) error {
	if err := workidentity.ValidateStableOpaqueID(mailID); err != nil {
		return fmt.Errorf("%w: mail id", ErrInvalidIdentity)
	}
	return nil
}

// MailID derives the deterministic mail id for one delivery.
func MailID(recipientPlayerID uint64, deliveryID string) (string, error) {
	recipient, err := CanonicalPlayerID(recipientPlayerID)
	if err != nil {
		return "", err
	}
	if err := ValidateDeliveryID(deliveryID); err != nil {
		return "", err
	}
	return workidentity.StableOpaqueID(MailIDNamespace, recipient, deliveryID)
}

// ValidateHash enforces the lowercase 64-hex SHA-256 digest shape. An empty
// hash is only valid where the contract allows "no attachment".
func ValidateHash(h string, allowEmpty bool) error {
	if h == "" && allowEmpty {
		return nil
	}
	if len(h) != 64 {
		return fmt.Errorf("%w: hash length", ErrInvalidIdentity)
	}
	if _, err := hex.DecodeString(h); err != nil {
		return fmt.Errorf("%w: hash hex", ErrInvalidIdentity)
	}
	for i := 0; i < len(h); i++ {
		if h[i] >= 'A' && h[i] <= 'F' {
			return fmt.Errorf("%w: hash case", ErrInvalidIdentity)
		}
	}
	return nil
}

// DeliveryPayloadHash implements 35 §2.3:
// CanonicalHash(recipient + delivery_id + source + title/body snapshot +
// claim_material_hash + visible/expire/claim_end + template/config
// version). Reward content enters only through claim_material_hash, keeping
// the three hash duties separated (M5).
func DeliveryPayloadHash(e MailEntry) (string, error) {
	recipient, err := CanonicalPlayerID(e.RecipientPlayerID)
	if err != nil {
		return "", err
	}
	if err := ValidateDeliveryID(e.DeliveryID); err != nil {
		return "", err
	}
	if err := ValidateHash(e.ClaimMaterialHash, true); err != nil {
		return "", err
	}
	pre, _ := appendLP(nil, deliveryPayloadHashNS)
	for _, f := range []string{
		recipient,
		e.DeliveryID,
		string(e.SourceType),
		e.SourceID,
		e.TitleSnapshot,
		e.BodySnapshot,
		e.ClaimMaterialHash,
		itoaSigned(e.VisibleAtMs),
		itoaSigned(e.ExpireAtMs),
		itoaSigned(e.ClaimEndAtMs),
		e.TemplateVersion,
		e.RewardConfigVersion,
	} {
		if pre, err = appendLP(pre, f); err != nil {
			return "", err
		}
	}
	digest := sha256.Sum256(pre)
	return hex.EncodeToString(digest[:]), nil
}

// ClaimMaterialHash implements 35 §2.3:
// CanonicalHash(recipient + mail_id + mail_version + delivery_id +
// RewardBundle canonical fields + claim_end_at_ms +
// reward_config_version). It binds the PlayerActor claim fence to the exact
// frozen reward content.
func ClaimMaterialHash(e MailEntry) (string, error) {
	recipient, err := CanonicalPlayerID(e.RecipientPlayerID)
	if err != nil {
		return "", err
	}
	if err := ValidateMailID(e.MailID); err != nil {
		return "", err
	}
	if e.MailVersion == 0 {
		return "", fmt.Errorf("%w: mail version", ErrInvalidIdentity)
	}
	pre, _ := appendLP(nil, claimMaterialHashNS)
	for _, f := range []string{
		recipient,
		e.MailID,
		itoa(e.MailVersion),
		e.DeliveryID,
	} {
		if pre, err = appendLP(pre, f); err != nil {
			return "", err
		}
	}
	pre = e.Reward.canonical(pre)
	if pre, err = appendLP(pre, itoaSigned(e.ClaimEndAtMs)); err != nil {
		return "", err
	}
	if pre, err = appendLP(pre, e.RewardConfigVersion); err != nil {
		return "", err
	}
	digest := sha256.Sum256(pre)
	return hex.EncodeToString(digest[:]), nil
}

// DeliveryDigest derives the mailbox HASH field identity of one delivery
// dedup record (35A §2.4).
func DeliveryDigest(recipientPlayerID uint64, deliveryID string) (string, error) {
	recipient, err := CanonicalPlayerID(recipientPlayerID)
	if err != nil {
		return "", err
	}
	if err := ValidateDeliveryID(deliveryID); err != nil {
		return "", err
	}
	return lpDigest(deliveryDigestNS, recipient, deliveryID), nil
}

// MutationDigest derives the stable mutation identity
// recipient + mail_id + kind + mutation_id (35A §5). The mutation id is
// server-derived (e.g. from the public operation digest or a deterministic
// command key); clients never pick a Redis field.
func MutationDigest(recipientPlayerID uint64, mailID string, kind MutationKind, mutationID string) (string, error) {
	recipient, err := CanonicalPlayerID(recipientPlayerID)
	if err != nil {
		return "", err
	}
	if err := ValidateMailID(mailID); err != nil {
		return "", err
	}
	if kind != MutationKindMarkRead && kind != MutationKindDelete {
		return "", fmt.Errorf("%w: mutation kind", ErrInvalidIdentity)
	}
	if mutationID == "" || !utf8.ValidString(mutationID) || len(mutationID) > 128 {
		return "", fmt.Errorf("%w: mutation id", ErrInvalidIdentity)
	}
	return lpDigest(mutationDigestNS, recipient, mailID, string(kind), mutationID), nil
}

// ProjectionDigest derives the stable claim-projection work identity
// recipient + work_id + work_version (35A §5).
func ProjectionDigest(recipientPlayerID uint64, workID string, workVersion uint64) (string, error) {
	recipient, err := CanonicalPlayerID(recipientPlayerID)
	if err != nil {
		return "", err
	}
	if workID == "" || !utf8.ValidString(workID) || len(workID) > 128 || workVersion == 0 {
		return "", fmt.Errorf("%w: projection work identity", ErrInvalidIdentity)
	}
	return lpDigest(projectionDigestNS, recipient, workID, itoa(workVersion)), nil
}

func lpDigest(namespace string, fields ...string) string {
	pre, _ := appendLP(nil, namespace)
	for _, f := range fields {
		pre, _ = appendLP(pre, f)
	}
	digest := sha256.Sum256(pre)
	return hex.EncodeToString(digest[:])
}
