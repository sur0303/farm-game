package mail

import "fmt"

// DeliveryParams is the immutable, self-contained snapshot every trusted
// delivery path must freeze before calling InsertMailIfAbsent (35 Q35-04).
// Worker retries replay exactly this snapshot; they never re-read "current"
// templates or reward configs.
type DeliveryParams struct {
	RecipientPlayerID   uint64
	DeliveryID          string
	SourceType          SourceType
	SourceID            string
	TitleSnapshot       string
	BodySnapshot        string
	Reward              RewardBundle
	TemplateVersion     string
	RewardConfigVersion string
	// CreatedAtMs is the trusted source time frozen at creation; it is the
	// pagination key (35A §3). Direct inserts use the mail store's Redis
	// TIME; order-sourced deliveries use the source commit's captured time.
	CreatedAtMs int64
	VisibleAtMs int64
	// RetentionMs is the requested visibility retention; 0 selects the
	// default. It is clamped to MaxRetentionMs (35 Q35-07).
	RetentionMs int64
}

// BuildMailEntry derives the stable identities, hashes and maintenance
// times for one delivery and validates every hard bound before the first
// store call.
func BuildMailEntry(p DeliveryParams, l Limits) (MailEntry, error) {
	if err := l.Validate(); err != nil {
		return MailEntry{}, err
	}
	if _, err := CanonicalPlayerID(p.RecipientPlayerID); err != nil {
		return MailEntry{}, err
	}
	if err := ValidateDeliveryID(p.DeliveryID); err != nil {
		return MailEntry{}, err
	}
	switch p.SourceType {
	case SourceTypeSystem, SourceTypeOperation, SourceTypeTest:
	default:
		return MailEntry{}, fmt.Errorf("%w: source type", ErrInvalidRequest)
	}
	if len(p.TitleSnapshot) > l.MaxTitleBytes {
		return MailEntry{}, fmt.Errorf("%w: title too large", ErrInvalidRequest)
	}
	if len(p.BodySnapshot) > l.MaxBodyBytes {
		return MailEntry{}, fmt.Errorf("%w: body too large", ErrInvalidRequest)
	}
	if len(p.SourceID) > 128 {
		return MailEntry{}, fmt.Errorf("%w: source id", ErrInvalidRequest)
	}
	if len(p.TemplateVersion) > 64 || len(p.RewardConfigVersion) > 64 {
		return MailEntry{}, fmt.Errorf("%w: version label", ErrInvalidRequest)
	}
	if p.CreatedAtMs <= 0 || p.VisibleAtMs <= 0 {
		return MailEntry{}, fmt.Errorf("%w: trusted times required", ErrInvalidRequest)
	}
	if err := p.Reward.Validate(l); err != nil {
		return MailEntry{}, err
	}
	retention := p.RetentionMs
	if retention == 0 {
		retention = l.DefaultRetentionMs
	}
	if retention < 0 || retention > l.MaxRetentionMs {
		return MailEntry{}, fmt.Errorf("%w: retention", ErrInvalidRequest)
	}

	mailID, err := MailID(p.RecipientPlayerID, p.DeliveryID)
	if err != nil {
		return MailEntry{}, err
	}
	entry := MailEntry{
		MailID:              mailID,
		MailVersion:         1,
		RecipientPlayerID:   p.RecipientPlayerID,
		DeliveryID:          p.DeliveryID,
		SourceType:          p.SourceType,
		SourceID:            p.SourceID,
		TitleSnapshot:       p.TitleSnapshot,
		BodySnapshot:        p.BodySnapshot,
		Reward:              p.Reward,
		TemplateVersion:     p.TemplateVersion,
		RewardConfigVersion: p.RewardConfigVersion,
		CreatedAtMs:         p.CreatedAtMs,
		VisibleAtMs:         p.VisibleAtMs,
		ExpireAtMs:          p.VisibleAtMs + retention,
	}
	// P0: claim_end_at_ms = expire_at_ms (35 Q35-07).
	entry.ClaimEndAtMs = entry.ExpireAtMs
	// physical_purge_at_ms is a maintenance parameter, excluded from the
	// claim hash, and must cover every evidence window (35 §2.7, 35A §6.2).
	entry.PhysicalPurgeAtMs = entry.ClaimEndAtMs + l.ClaimMaterialRetainAfterClaimEndMs

	if entry.HasAttachment() {
		if entry.ClaimMaterialHash, err = ClaimMaterialHash(entry); err != nil {
			return MailEntry{}, err
		}
	}
	if entry.DeliveryPayloadHash, err = DeliveryPayloadHash(entry); err != nil {
		return MailEntry{}, err
	}
	return entry, nil
}

// NewUserState builds the initial state written atomically with the entry.
func NewUserState(entry MailEntry) MailUserState {
	return MailUserState{
		MailID:            entry.MailID,
		ClaimMaterialHash: entry.ClaimMaterialHash,
		StateVersion:      1,
	}
}

// NewDeliveryDedup builds the dedup record committed with the insert.
func NewDeliveryDedup(entry MailEntry, l Limits, committedAtMs int64) DeliveryDedup {
	return DeliveryDedup{
		RecipientPlayerID:     entry.RecipientPlayerID,
		DeliveryID:            entry.DeliveryID,
		DeliveryPayloadHash:   entry.DeliveryPayloadHash,
		MailID:                entry.MailID,
		FirstCommittedAtMs:    committedAtMs,
		DeliveryRetainUntilMs: committedAtMs + l.DeliveryRecordRetainMs,
	}
}

// DecideDeleteEligibility implements the server-side decision of 35A §4.3.
// The repository never invokes the delete script on REJECT/UNKNOWN; only
// the two server-constructed allow values reach Lua.
type DeleteDecision string

const (
	DeleteAllow               DeleteDecision = "ALLOW"
	DeleteAllowWithClaimProof DeleteDecision = "ALLOW_WITH_CLAIM_PROOF"
	DeleteRejectUnclaimed     DeleteDecision = "REJECT_UNCLAIMED"
	DeleteProcessing          DeleteDecision = "PROCESSING"
)

// ClaimProof is the trusted evidence read from PlayerAuthority
// MailClaimState by the (future) claim wiring (35A §4.3). It is only
// meaningful when fully bound: claimed=true, the exact mail id, the exact
// claim_material_hash and a non-empty claim receipt reference. Clients can
// never construct it; only trusted server modules may.
type ClaimProof struct {
	MailID            string
	Claimed           bool
	ClaimMaterialHash string
	ReceiptRef        string
}

// Validate enforces the structural binding of a ClaimProof.
func (p ClaimProof) Validate() error {
	if !p.Claimed {
		return fmt.Errorf("%w: proof is not claimed", ErrInvalidRequest)
	}
	if err := ValidateMailID(p.MailID); err != nil {
		return err
	}
	if err := ValidateHash(p.ClaimMaterialHash, false); err != nil {
		return err
	}
	if p.ReceiptRef == "" {
		return fmt.Errorf("%w: empty claim receipt ref", ErrInvalidRequest)
	}
	return nil
}

// DeleteAuthorization is the server-constructed delete decision handed to
// the repository (35A §4.3). A plain ALLOW and a proof-carrying
// ALLOW_WITH_CLAIM_PROOF are strictly distinguishable; the script
// re-verifies the proof against the immutable entry.
type DeleteAuthorization struct {
	Eligibility DeleteEligibility
	// ClaimProof is required iff Eligibility ==
	// EligibilityAllowWithClaimProof and forbidden otherwise.
	ClaimProof *ClaimProof
}

// AllowDelete builds a plain authorization (no attachment, claim window
// passed, or the projection already shows claimed).
func AllowDelete() DeleteAuthorization {
	return DeleteAuthorization{Eligibility: EligibilityAllow}
}

// AllowDeleteWithProof builds the proof-carrying authorization.
func AllowDeleteWithProof(proof ClaimProof) DeleteAuthorization {
	p := proof
	return DeleteAuthorization{Eligibility: EligibilityAllowWithClaimProof, ClaimProof: &p}
}

// Validate checks the authorization shape against the target mail id.
func (a DeleteAuthorization) Validate(mailID string) error {
	switch a.Eligibility {
	case EligibilityAllow:
		if a.ClaimProof != nil {
			return fmt.Errorf("%w: plain ALLOW must not carry a proof", ErrInvalidRequest)
		}
		return nil
	case EligibilityAllowWithClaimProof:
		if a.ClaimProof == nil {
			return fmt.Errorf("%w: missing claim proof", ErrInvalidRequest)
		}
		if err := a.ClaimProof.Validate(); err != nil {
			return err
		}
		if a.ClaimProof.MailID != mailID {
			return fmt.Errorf("%w: claim proof binds a different mail", ErrInvalidRequest)
		}
		return nil
	default:
		return fmt.Errorf("%w: delete eligibility", ErrInvalidRequest)
	}
}

// DecideDeleteEligibility maps entry/state/proof to a delete decision.
// nowMs is trusted server time. A proof that does not bind this exact mail
// (mail id + claim material hash) is treated as "no proof".
func DecideDeleteEligibility(entry MailEntry, state MailUserState, nowMs int64, proof *ClaimProof) DeleteDecision {
	if !entry.HasAttachment() {
		return DeleteAllow
	}
	if nowMs >= entry.ClaimEndAtMs {
		return DeleteAllow
	}
	if state.ClaimedProjection {
		return DeleteAllow
	}
	if proof == nil {
		// The authoritative claim state could not be read: never call the
		// delete script on unknown (35A §4.3).
		return DeleteProcessing
	}
	if proof.Claimed && proof.MailID == entry.MailID && proof.ClaimMaterialHash == entry.ClaimMaterialHash {
		return DeleteAllowWithClaimProof
	}
	return DeleteRejectUnclaimed
}
