package mail

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"unicode/utf8"
)

// ErrInvalidClaimMaterial rejects construction or acceptance of untrusted
// claim material (35 M4/Q35-06).
var ErrInvalidClaimMaterial = errors.New("mail: invalid trusted claim material")

// ClaimMaterialAudience is the only accepted audience (35 Q35-06).
const ClaimMaterialAudience = "PLAYER_AUTHORITY"

// The issuer allowlist is intentionally closed. These are the only local
// trusted mail-claim construction paths in the current deployment.
const (
	ClaimMaterialIssuerIngress     = "player-svr/mail-claim-ingress"
	ClaimMaterialIssuerCoordinator = "player-svr/mail-claim-coordinator"
)

var claimMaterialIssuerAllowlist = map[string]struct{}{
	ClaimMaterialIssuerIngress:     {},
	ClaimMaterialIssuerCoordinator: {},
}

// IsAllowedClaimMaterialIssuer reports whether issuer is a trusted local
// constructor identity. Callers must not replace this with a non-empty check.
func IsAllowedClaimMaterialIssuer(issuer string) bool {
	_, ok := claimMaterialIssuerAllowlist[issuer]
	return ok
}

// ValidateClaimMaterialIssuer applies the closed issuer allowlist.
func ValidateClaimMaterialIssuer(issuer string) error {
	if !IsAllowedClaimMaterialIssuer(issuer) {
		return fmt.Errorf("%w: issuer is not allowlisted", ErrInvalidClaimMaterial)
	}
	return nil
}

// ClaimMaterialSchemaVersion stamps the material shape.
const ClaimMaterialSchemaVersion = "TrustedMailClaimMaterialV1"

// maxMaterialTTLMs bounds how long one issued material may be accepted by a
// new Actor Turn. It never shortens the business claim window (35 §2.6).
const maxMaterialTTLMs = int64(15) * 60 * 1000

// TrustedMailClaimMaterial is the immutable object built from a strong-read
// MailEntry by a trusted module (PlayerMailClaimIngress/ClaimMailCoordinator)
// and handed to PlayerActor. H5, Gate and ordinary internal services can
// never construct it (35 M4). PlayerActor does not read MailEntryStore.
type TrustedMailClaimMaterial struct {
	SchemaVersion       string
	MailID              string
	MailVersion         uint64
	RecipientPlayerID   uint64
	SourceType          SourceType
	SourceID            string
	DeliveryID          string
	Reward              RewardBundle
	RewardConfigVersion string
	ClaimMaterialHash   string
	VisibleAtMs         int64
	ClaimEndAtMs        int64
	IssuedAtMs          int64
	MaterialExpireAtMs  int64
	Issuer              string
	Audience            string

	// bindingDigest seals every material field, including fields intentionally
	// outside the business claim hash. It is set only by Issue; a hand-built
	// or tampered material is not trusted by ValidateForTurn.
	bindingDigest [32]byte
}

// ClaimMaterialIssuer is the trusted-only constructor port (35 Q35-06).
// The issuer identity comes from the trusted service context (29), never
// from the request payload.
type ClaimMaterialIssuer struct {
	// Issuer is the stable trusted service identity (e.g.
	// "player-svr/mail-claim-ingress").
	Issuer string
	// MaterialTTLMs is clamped to maxMaterialTTLMs.
	MaterialTTLMs int64
}

// Issue builds the immutable material from a strongly-read entry. It
// re-derives the claim material hash from the immutable fields and refuses
// any mismatch, so a tampered or inconsistent entry can never reach the
// PlayerActor turn. This is a fast pre-check only; the Actor turn remains
// the final business decision point (35 §2.6).
func (i ClaimMaterialIssuer) Issue(entry MailEntry, nowMs int64) (TrustedMailClaimMaterial, error) {
	if err := ValidateClaimMaterialIssuer(i.Issuer); err != nil {
		return TrustedMailClaimMaterial{}, err
	}
	if nowMs <= 0 {
		return TrustedMailClaimMaterial{}, fmt.Errorf("%w: issued time", ErrInvalidClaimMaterial)
	}
	if err := validateClaimMaterialEntry(entry); err != nil {
		return TrustedMailClaimMaterial{}, err
	}
	if nowMs < entry.VisibleAtMs || nowMs >= entry.ClaimEndAtMs {
		return TrustedMailClaimMaterial{}, fmt.Errorf("%w: claim window", ErrInvalidClaimMaterial)
	}
	want, err := ClaimMaterialHash(entry)
	if err != nil {
		return TrustedMailClaimMaterial{}, err
	}
	if want != entry.ClaimMaterialHash {
		return TrustedMailClaimMaterial{}, fmt.Errorf("%w: claim material hash mismatch", ErrInvalidClaimMaterial)
	}
	ttl := i.MaterialTTLMs
	if ttl <= 0 || ttl > maxMaterialTTLMs {
		ttl = maxMaterialTTLMs
	}
	if nowMs > int64(1<<63-1)-ttl {
		return TrustedMailClaimMaterial{}, fmt.Errorf("%w: material TTL overflow", ErrInvalidClaimMaterial)
	}
	material := TrustedMailClaimMaterial{
		SchemaVersion:       ClaimMaterialSchemaVersion,
		MailID:              entry.MailID,
		MailVersion:         entry.MailVersion,
		RecipientPlayerID:   entry.RecipientPlayerID,
		SourceType:          entry.SourceType,
		SourceID:            entry.SourceID,
		DeliveryID:          entry.DeliveryID,
		Reward:              cloneRewardBundle(entry.Reward),
		RewardConfigVersion: entry.RewardConfigVersion,
		ClaimMaterialHash:   entry.ClaimMaterialHash,
		VisibleAtMs:         entry.VisibleAtMs,
		ClaimEndAtMs:        entry.ClaimEndAtMs,
		IssuedAtMs:          nowMs,
		MaterialExpireAtMs:  nowMs + ttl,
		Issuer:              i.Issuer,
		Audience:            ClaimMaterialAudience,
	}
	material.bindingDigest = claimMaterialBindingDigest(material)
	return material, nil
}

// ValidateForTurn re-checks the acceptance-side invariants an Actor turn
// relies on (35 §2.6 step 1). No client-provided reward or identity field is
// trusted: all material fields are structurally validated, the canonical
// claim material hash is recomputed, and the issuer/seal are checked.
func (m TrustedMailClaimMaterial) ValidateForTurn(recipientPlayerID uint64, trustedNowMs int64) error {
	if m.SchemaVersion != ClaimMaterialSchemaVersion {
		return fmt.Errorf("%w: schema", ErrInvalidClaimMaterial)
	}
	if m.Audience != ClaimMaterialAudience {
		return fmt.Errorf("%w: audience", ErrInvalidClaimMaterial)
	}
	if err := ValidateClaimMaterialIssuer(m.Issuer); err != nil {
		return err
	}
	if recipientPlayerID == 0 || m.RecipientPlayerID != recipientPlayerID {
		return fmt.Errorf("%w: recipient", ErrInvalidClaimMaterial)
	}
	if trustedNowMs <= 0 {
		return fmt.Errorf("%w: trusted time", ErrInvalidClaimMaterial)
	}
	if err := m.validateBoundFields(); err != nil {
		return err
	}
	if m.bindingDigest == ([32]byte{}) {
		return fmt.Errorf("%w: unissued material", ErrInvalidClaimMaterial)
	}
	if want := claimMaterialBindingDigest(m); want != m.bindingDigest {
		return fmt.Errorf("%w: material binding mismatch", ErrInvalidClaimMaterial)
	}
	entry := m.mailEntry()
	wantHash, err := ClaimMaterialHash(entry)
	if err != nil {
		return fmt.Errorf("%w: claim material hash input: %v", ErrInvalidClaimMaterial, err)
	}
	if wantHash != m.ClaimMaterialHash {
		return fmt.Errorf("%w: claim material hash mismatch", ErrInvalidClaimMaterial)
	}
	if trustedNowMs < m.IssuedAtMs {
		return fmt.Errorf("%w: material not yet issued", ErrInvalidClaimMaterial)
	}
	if trustedNowMs < m.VisibleAtMs {
		return fmt.Errorf("%w: claim not visible", ErrInvalidClaimMaterial)
	}
	if trustedNowMs >= m.ClaimEndAtMs {
		return fmt.Errorf("%w: claim window ended", ErrInvalidClaimMaterial)
	}
	if trustedNowMs >= m.MaterialExpireAtMs {
		return fmt.Errorf("%w: material expired", ErrInvalidClaimMaterial)
	}
	return nil
}

func validateClaimMaterialEntry(entry MailEntry) error {
	if !entry.HasAttachment() {
		return fmt.Errorf("%w: no attachment", ErrInvalidClaimMaterial)
	}
	if entry.RecipientPlayerID == 0 {
		return fmt.Errorf("%w: recipient", ErrInvalidClaimMaterial)
	}
	if err := ValidateMailID(entry.MailID); err != nil {
		return fmt.Errorf("%w: mail id: %v", ErrInvalidClaimMaterial, err)
	}
	if entry.MailVersion == 0 {
		return fmt.Errorf("%w: mail version", ErrInvalidClaimMaterial)
	}
	if err := ValidateDeliveryID(entry.DeliveryID); err != nil {
		return fmt.Errorf("%w: delivery id: %v", ErrInvalidClaimMaterial, err)
	}
	if err := validateClaimMaterialSource(entry.SourceType, entry.SourceID); err != nil {
		return err
	}
	if err := entry.Reward.Validate(LimitsV1()); err != nil {
		return fmt.Errorf("%w: reward: %v", ErrInvalidClaimMaterial, err)
	}
	if err := validateClaimMaterialVersion(entry.RewardConfigVersion); err != nil {
		return fmt.Errorf("%w: reward config version: %v", ErrInvalidClaimMaterial, err)
	}
	if entry.VisibleAtMs <= 0 || entry.ExpireAtMs <= entry.VisibleAtMs ||
		entry.ClaimEndAtMs <= entry.VisibleAtMs || entry.ClaimEndAtMs > entry.ExpireAtMs {
		return fmt.Errorf("%w: entry time bounds", ErrInvalidClaimMaterial)
	}
	if err := ValidateHash(entry.ClaimMaterialHash, false); err != nil {
		return fmt.Errorf("%w: claim material hash shape: %v", ErrInvalidClaimMaterial, err)
	}
	derivedMailID, err := MailID(entry.RecipientPlayerID, entry.DeliveryID)
	if err != nil || derivedMailID != entry.MailID {
		return fmt.Errorf("%w: mail identity binding", ErrInvalidClaimMaterial)
	}
	return nil
}

func (m TrustedMailClaimMaterial) validateBoundFields() error {
	if m.RecipientPlayerID == 0 {
		return fmt.Errorf("%w: recipient", ErrInvalidClaimMaterial)
	}
	if m.MailVersion == 0 {
		return fmt.Errorf("%w: mail version", ErrInvalidClaimMaterial)
	}
	if err := ValidateMailID(m.MailID); err != nil {
		return fmt.Errorf("%w: mail id: %v", ErrInvalidClaimMaterial, err)
	}
	if err := ValidateDeliveryID(m.DeliveryID); err != nil {
		return fmt.Errorf("%w: delivery id: %v", ErrInvalidClaimMaterial, err)
	}
	if err := validateClaimMaterialSource(m.SourceType, m.SourceID); err != nil {
		return err
	}
	if err := m.Reward.Validate(LimitsV1()); err != nil || m.Reward.IsZero() {
		return fmt.Errorf("%w: reward", ErrInvalidClaimMaterial)
	}
	if err := validateClaimMaterialVersion(m.RewardConfigVersion); err != nil {
		return fmt.Errorf("%w: reward config version: %v", ErrInvalidClaimMaterial, err)
	}
	if m.VisibleAtMs <= 0 || m.ClaimEndAtMs <= m.VisibleAtMs {
		return fmt.Errorf("%w: claim window bounds", ErrInvalidClaimMaterial)
	}
	if m.IssuedAtMs < m.VisibleAtMs || m.IssuedAtMs >= m.ClaimEndAtMs {
		return fmt.Errorf("%w: issued time bounds", ErrInvalidClaimMaterial)
	}
	if m.MaterialExpireAtMs <= m.IssuedAtMs {
		return fmt.Errorf("%w: material TTL bounds", ErrInvalidClaimMaterial)
	}
	if ttl := m.MaterialExpireAtMs - m.IssuedAtMs; ttl > maxMaterialTTLMs {
		return fmt.Errorf("%w: material TTL exceeds bound", ErrInvalidClaimMaterial)
	}
	if err := ValidateHash(m.ClaimMaterialHash, false); err != nil {
		return fmt.Errorf("%w: claim material hash shape: %v", ErrInvalidClaimMaterial, err)
	}
	derivedMailID, err := MailID(m.RecipientPlayerID, m.DeliveryID)
	if err != nil || derivedMailID != m.MailID {
		return fmt.Errorf("%w: mail identity binding", ErrInvalidClaimMaterial)
	}
	return nil
}

func (m TrustedMailClaimMaterial) mailEntry() MailEntry {
	return MailEntry{
		MailID:              m.MailID,
		MailVersion:         m.MailVersion,
		RecipientPlayerID:   m.RecipientPlayerID,
		DeliveryID:          m.DeliveryID,
		Reward:              cloneRewardBundle(m.Reward),
		RewardConfigVersion: m.RewardConfigVersion,
		ClaimMaterialHash:   m.ClaimMaterialHash,
		VisibleAtMs:         m.VisibleAtMs,
		ClaimEndAtMs:        m.ClaimEndAtMs,
	}
}

func validateClaimMaterialSource(sourceType SourceType, sourceID string) error {
	switch sourceType {
	case SourceTypeSystem, SourceTypeOperation, SourceTypeTest:
	default:
		return fmt.Errorf("%w: source type", ErrInvalidClaimMaterial)
	}
	if len(sourceID) > 128 || !utf8.ValidString(sourceID) {
		return fmt.Errorf("%w: source id", ErrInvalidClaimMaterial)
	}
	return nil
}

func validateClaimMaterialVersion(version string) error {
	if len(version) > 64 || !utf8.ValidString(version) {
		return fmt.Errorf("version label")
	}
	return nil
}

func cloneRewardBundle(reward RewardBundle) RewardBundle {
	out := reward
	out.Items = append([]ItemGrant(nil), reward.Items...)
	return out
}

func claimMaterialBindingDigest(m TrustedMailClaimMaterial) [32]byte {
	pre, err := appendLP(nil, "TrustedMailClaimMaterialBindingV1")
	if err != nil {
		return [32]byte{}
	}
	for _, field := range []string{
		m.SchemaVersion,
		m.MailID,
		itoa(m.MailVersion),
		itoa(m.RecipientPlayerID),
		string(m.SourceType),
		m.SourceID,
		m.DeliveryID,
		m.RewardConfigVersion,
		m.ClaimMaterialHash,
		itoaSigned(m.VisibleAtMs),
		itoaSigned(m.ClaimEndAtMs),
		itoaSigned(m.IssuedAtMs),
		itoaSigned(m.MaterialExpireAtMs),
		m.Issuer,
		m.Audience,
	} {
		if pre, err = appendLP(pre, field); err != nil {
			return [32]byte{}
		}
	}
	if pre, err = appendLPRaw(pre, m.Reward.canonical(nil)); err != nil {
		return [32]byte{}
	}
	return sha256.Sum256(pre)
}
