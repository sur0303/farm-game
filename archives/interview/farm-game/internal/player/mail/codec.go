package mail

import "fmt"

// Bounded value codecs (35A §2.4). Every mailbox HASH value is an LP field
// sequence whose first field is a schema marker. Lua reads only the few
// scalar positions it needs via lp_fields; it never parses body or reward
// content. Field order is frozen; any change requires a new schema marker.

const (
	entrySchema      = "MailEntryV1"
	stateSchema      = "MailUserStateV1"
	dedupSchema      = "DeliveryDedupV1"
	mutationSchema   = "MutationReceiptV1"
	projectionSchema = "ProjectionReceiptV1"
)

// MailEntryV1 field positions (0-based, including the schema marker).
const (
	entryFSchema = iota
	entryFMailID
	entryFMailVersion
	entryFRecipient
	entryFDeliveryID
	entryFSourceType
	entryFSourceID
	entryFTitle
	entryFBody
	entryFRewardBlob
	entryFTemplateVersion
	entryFRewardConfigVersion
	entryFDeliveryPayloadHash
	entryFClaimMaterialHash
	entryFCreatedAtMs
	entryFVisibleAtMs
	entryFExpireAtMs
	entryFClaimEndAtMs
	entryFPhysicalPurgeAtMs
	entryFHasAttachment
	entryFieldCount
)

// MailUserStateV1 field positions.
const (
	stateFSchema = iota
	stateFMailID
	stateFReadAtMs
	stateFDeletedAtMs
	stateFExpiredAtMs
	stateFClaimedProjection
	stateFClaimReceiptRef
	stateFClaimMaterialHash
	stateFClaimedProjectionAtMs
	stateFClaimProjectionVersion
	stateFStateVersion
	stateFAutoEvicted
	stateFieldCount
)

const (
	dedupFSchema = iota
	dedupFRecipient
	dedupFDeliveryID
	dedupFDeliveryPayloadHash
	dedupFMailID
	dedupFFirstCommittedAtMs
	dedupFRetainUntilMs
	dedupFTombstone
	dedupFieldCount
)

const (
	mutationFSchema = iota
	mutationFKind
	mutationFMailID
	mutationFMutationID
	mutationFResultStatus
	mutationFAppliedAtMs
	mutationFMailboxRevision
	mutationFRetainUntilMs
	mutationFieldCount
)

const (
	projectionFSchema = iota
	projectionFWorkID
	projectionFWorkVersion
	projectionFMailID
	projectionFClaimMaterialHash
	projectionFResultStatus
	projectionFAppliedAtMs
	projectionFRetainUntilMs
	projectionFieldCount
)

func boolByte(v bool) string {
	if v {
		return "1"
	}
	return "0"
}

// EncodeMailEntry encodes the immutable entry and enforces the hard bounds
// before any byte reaches the store (35A PM4).
func EncodeMailEntry(e MailEntry, l Limits) ([]byte, error) {
	if err := ValidateMailID(e.MailID); err != nil {
		return nil, err
	}
	recipient, err := CanonicalPlayerID(e.RecipientPlayerID)
	if err != nil {
		return nil, err
	}
	if e.MailVersion == 0 {
		return nil, fmt.Errorf("%w: mail version", ErrMalformedBlob)
	}
	if err := ValidateDeliveryID(e.DeliveryID); err != nil {
		return nil, err
	}
	if len(e.TitleSnapshot) > l.MaxTitleBytes {
		return nil, fmt.Errorf("%w: title too large", ErrMalformedBlob)
	}
	if len(e.BodySnapshot) > l.MaxBodyBytes {
		return nil, fmt.Errorf("%w: body too large", ErrMalformedBlob)
	}
	if err := e.Reward.Validate(l); err != nil {
		return nil, err
	}
	if err := ValidateHash(e.DeliveryPayloadHash, false); err != nil {
		return nil, err
	}
	if err := ValidateHash(e.ClaimMaterialHash, !e.HasAttachment()); err != nil {
		return nil, err
	}
	if e.HasAttachment() && e.ClaimMaterialHash == "" {
		return nil, fmt.Errorf("%w: attachment without claim material hash", ErrMalformedBlob)
	}
	if !e.HasAttachment() && e.ClaimMaterialHash != "" {
		return nil, fmt.Errorf("%w: text mail with claim material hash", ErrMalformedBlob)
	}
	if e.CreatedAtMs <= 0 || e.VisibleAtMs <= 0 || e.ExpireAtMs <= e.VisibleAtMs ||
		e.ClaimEndAtMs <= 0 || e.PhysicalPurgeAtMs < e.ClaimEndAtMs {
		return nil, fmt.Errorf("%w: time bounds", ErrMalformedBlob)
	}

	rewardBlob := e.Reward.canonical(nil)
	fields := []string{
		entrySchema,
		e.MailID,
		itoa(e.MailVersion),
		recipient,
		e.DeliveryID,
		string(e.SourceType),
		e.SourceID,
		e.TitleSnapshot,
		e.BodySnapshot,
		"", // reward blob appended as raw field below
		e.TemplateVersion,
		e.RewardConfigVersion,
		e.DeliveryPayloadHash,
		e.ClaimMaterialHash,
		itoaSigned(e.CreatedAtMs),
		itoaSigned(e.VisibleAtMs),
		itoaSigned(e.ExpireAtMs),
		itoaSigned(e.ClaimEndAtMs),
		itoaSigned(e.PhysicalPurgeAtMs),
		boolByte(e.HasAttachment()),
	}
	var blob []byte
	for i, f := range fields {
		if i == entryFRewardBlob {
			if blob, err = appendLPRaw(blob, rewardBlob); err != nil {
				return nil, err
			}
			continue
		}
		if blob, err = appendLP(blob, f); err != nil {
			return nil, err
		}
	}
	if len(blob) > l.MaxMailEntryBytes {
		return nil, fmt.Errorf("%w: entry exceeds %d bytes", ErrMalformedBlob, l.MaxMailEntryBytes)
	}
	return blob, nil
}

// DecodeMailEntry parses and fully validates an entry blob.
func DecodeMailEntry(blob []byte) (MailEntry, error) {
	fields, err := parseLP(blob, entryFieldCount)
	if err != nil {
		return MailEntry{}, err
	}
	if fields[entryFSchema] != entrySchema {
		return MailEntry{}, fmt.Errorf("%w: entry schema", ErrMalformedBlob)
	}
	e := MailEntry{
		MailID:              fields[entryFMailID],
		SourceType:          SourceType(fields[entryFSourceType]),
		SourceID:            fields[entryFSourceID],
		TitleSnapshot:       fields[entryFTitle],
		BodySnapshot:        fields[entryFBody],
		TemplateVersion:     fields[entryFTemplateVersion],
		RewardConfigVersion: fields[entryFRewardConfigVersion],
		DeliveryPayloadHash: fields[entryFDeliveryPayloadHash],
		ClaimMaterialHash:   fields[entryFClaimMaterialHash],
	}
	if e.MailVersion, err = parseUint(fields[entryFMailVersion]); err != nil {
		return MailEntry{}, err
	}
	if e.RecipientPlayerID, err = parseUint(fields[entryFRecipient]); err != nil {
		return MailEntry{}, err
	}
	e.DeliveryID = fields[entryFDeliveryID]
	for _, tc := range []struct {
		dst  *int64
		name int
	}{
		{&e.CreatedAtMs, entryFCreatedAtMs},
		{&e.VisibleAtMs, entryFVisibleAtMs},
		{&e.ExpireAtMs, entryFExpireAtMs},
		{&e.ClaimEndAtMs, entryFClaimEndAtMs},
		{&e.PhysicalPurgeAtMs, entryFPhysicalPurgeAtMs},
	} {
		if *tc.dst, err = atoi(fields[tc.name]); err != nil {
			return MailEntry{}, err
		}
	}
	reward, err := decodeRewardCanonical(fields[entryFRewardBlob])
	if err != nil {
		return MailEntry{}, err
	}
	e.Reward = reward
	if has := fields[entryFHasAttachment]; has != boolByte(e.HasAttachment()) {
		return MailEntry{}, fmt.Errorf("%w: attachment flag", ErrMalformedBlob)
	}
	return e, nil
}

func decodeRewardCanonical(blob string) (RewardBundle, error) {
	var r RewardBundle
	rest := []byte(blob)
	for len(rest) > 0 {
		kind, remainder, err := takeLP(rest)
		if err != nil {
			return RewardBundle{}, err
		}
		rest = remainder
		id, remainder, err := takeLP(rest)
		if err != nil {
			return RewardBundle{}, err
		}
		rest = remainder
		qty, remainder, err := takeLP(rest)
		if err != nil {
			return RewardBundle{}, err
		}
		rest = remainder
		n, err := atoi(qty)
		if err != nil {
			return RewardBundle{}, err
		}
		switch kind {
		case "COIN":
			r.Coin = n
		case "EXP":
			r.Exp = n
		case "ITEM":
			itemID, err := atoi(id)
			if err != nil {
				return RewardBundle{}, err
			}
			r.Items = append(r.Items, ItemGrant{ItemID: int32(itemID), Count: n})
		default:
			return RewardBundle{}, fmt.Errorf("%w: reward kind", ErrMalformedBlob)
		}
	}
	return r, nil
}

func takeLP(blob []byte) (string, []byte, error) {
	if len(blob) < 4 {
		return "", nil, fmt.Errorf("%w: reward LP length", ErrMalformedBlob)
	}
	size := int(blob[0])<<24 | int(blob[1])<<16 | int(blob[2])<<8 | int(blob[3])
	blob = blob[4:]
	if size > len(blob) {
		return "", nil, fmt.Errorf("%w: reward LP field", ErrMalformedBlob)
	}
	return string(blob[:size]), blob[size:], nil
}

// EncodeMailUserState encodes the mutable per-mail state.
func EncodeMailUserState(s MailUserState, l Limits) ([]byte, error) {
	if err := ValidateMailID(s.MailID); err != nil {
		return nil, err
	}
	if err := ValidateHash(s.ClaimMaterialHash, true); err != nil {
		return nil, err
	}
	if s.StateVersion == 0 {
		return nil, fmt.Errorf("%w: state version", ErrMalformedBlob)
	}
	fields := []string{
		stateSchema,
		s.MailID,
		itoaSigned(s.ReadAtMs),
		itoaSigned(s.DeletedAtMs),
		itoaSigned(s.ExpiredAtMs),
		boolByte(s.ClaimedProjection),
		s.ClaimReceiptRef,
		s.ClaimMaterialHash,
		itoaSigned(s.ClaimedProjectionAtMs),
		itoa(s.ClaimProjectionVersion),
		itoa(s.StateVersion),
		boolByte(s.AutoEvicted),
	}
	var blob []byte
	var err error
	for _, f := range fields {
		if blob, err = appendLP(blob, f); err != nil {
			return nil, err
		}
	}
	if len(blob) > l.MaxMailStateBytes {
		return nil, fmt.Errorf("%w: state exceeds %d bytes", ErrMalformedBlob, l.MaxMailStateBytes)
	}
	return blob, nil
}

// DecodeMailUserState parses a state blob.
func DecodeMailUserState(blob []byte) (MailUserState, error) {
	fields, err := parseLP(blob, stateFieldCount)
	if err != nil {
		return MailUserState{}, err
	}
	if fields[stateFSchema] != stateSchema {
		return MailUserState{}, fmt.Errorf("%w: state schema", ErrMalformedBlob)
	}
	s := MailUserState{
		MailID:            fields[stateFMailID],
		ClaimReceiptRef:   fields[stateFClaimReceiptRef],
		ClaimMaterialHash: fields[stateFClaimMaterialHash],
	}
	for _, tc := range []struct {
		dst  *int64
		name int
	}{
		{&s.ReadAtMs, stateFReadAtMs},
		{&s.DeletedAtMs, stateFDeletedAtMs},
		{&s.ExpiredAtMs, stateFExpiredAtMs},
		{&s.ClaimedProjectionAtMs, stateFClaimedProjectionAtMs},
	} {
		if *tc.dst, err = atoi(fields[tc.name]); err != nil {
			return MailUserState{}, err
		}
	}
	if s.ClaimProjectionVersion, err = parseUint(fields[stateFClaimProjectionVersion]); err != nil {
		return MailUserState{}, err
	}
	if s.StateVersion, err = parseUint(fields[stateFStateVersion]); err != nil {
		return MailUserState{}, err
	}
	s.ClaimedProjection = fields[stateFClaimedProjection] == "1"
	s.AutoEvicted = fields[stateFAutoEvicted] == "1"
	return s, nil
}

// EncodeDeliveryDedup encodes a delivery dedup/tombstone record.
func EncodeDeliveryDedup(d DeliveryDedup, l Limits) ([]byte, error) {
	recipient, err := CanonicalPlayerID(d.RecipientPlayerID)
	if err != nil {
		return nil, err
	}
	if err := ValidateDeliveryID(d.DeliveryID); err != nil {
		return nil, err
	}
	if err := ValidateMailID(d.MailID); err != nil {
		return nil, err
	}
	if err := ValidateHash(d.DeliveryPayloadHash, false); err != nil {
		return nil, err
	}
	fields := []string{
		dedupSchema,
		recipient,
		d.DeliveryID,
		d.DeliveryPayloadHash,
		d.MailID,
		itoaSigned(d.FirstCommittedAtMs),
		itoaSigned(d.DeliveryRetainUntilMs),
		boolByte(d.Tombstone),
	}
	var blob []byte
	for _, f := range fields {
		if blob, err = appendLP(blob, f); err != nil {
			return nil, err
		}
	}
	if len(blob) > l.MaxDeliveryRecordBytes {
		return nil, fmt.Errorf("%w: delivery record exceeds %d bytes", ErrMalformedBlob, l.MaxDeliveryRecordBytes)
	}
	return blob, nil
}

// DecodeDeliveryDedup parses a delivery record.
func DecodeDeliveryDedup(blob []byte) (DeliveryDedup, error) {
	fields, err := parseLP(blob, dedupFieldCount)
	if err != nil {
		return DeliveryDedup{}, err
	}
	if fields[dedupFSchema] != dedupSchema {
		return DeliveryDedup{}, fmt.Errorf("%w: dedup schema", ErrMalformedBlob)
	}
	d := DeliveryDedup{
		DeliveryID:          fields[dedupFDeliveryID],
		DeliveryPayloadHash: fields[dedupFDeliveryPayloadHash],
		MailID:              fields[dedupFMailID],
	}
	if d.RecipientPlayerID, err = parseUint(fields[dedupFRecipient]); err != nil {
		return DeliveryDedup{}, err
	}
	if d.FirstCommittedAtMs, err = atoi(fields[dedupFFirstCommittedAtMs]); err != nil {
		return DeliveryDedup{}, err
	}
	if d.DeliveryRetainUntilMs, err = atoi(fields[dedupFRetainUntilMs]); err != nil {
		return DeliveryDedup{}, err
	}
	d.Tombstone = fields[dedupFTombstone] == "1"
	return d, nil
}

// EncodeMutationReceipt encodes a MarkRead/Delete stable result.
func EncodeMutationReceipt(r MutationReceipt, l Limits) ([]byte, error) {
	if r.Kind != MutationKindMarkRead && r.Kind != MutationKindDelete {
		return nil, fmt.Errorf("%w: mutation kind", ErrMalformedBlob)
	}
	if err := ValidateMailID(r.MailID); err != nil {
		return nil, err
	}
	fields := []string{
		mutationSchema,
		string(r.Kind),
		r.MailID,
		r.MutationID,
		r.ResultStatus,
		itoaSigned(r.AppliedAtMs),
		itoa(r.MailboxRevision),
		itoaSigned(r.RetainUntilMs),
	}
	var blob []byte
	var err error
	for _, f := range fields {
		if blob, err = appendLP(blob, f); err != nil {
			return nil, err
		}
	}
	if len(blob) > l.MaxReceiptBytes {
		return nil, fmt.Errorf("%w: mutation receipt exceeds %d bytes", ErrMalformedBlob, l.MaxReceiptBytes)
	}
	return blob, nil
}

// DecodeMutationReceipt parses a mutation receipt.
func DecodeMutationReceipt(blob []byte) (MutationReceipt, error) {
	fields, err := parseLP(blob, mutationFieldCount)
	if err != nil {
		return MutationReceipt{}, err
	}
	if fields[mutationFSchema] != mutationSchema {
		return MutationReceipt{}, fmt.Errorf("%w: mutation schema", ErrMalformedBlob)
	}
	r := MutationReceipt{
		Kind:         MutationKind(fields[mutationFKind]),
		MailID:       fields[mutationFMailID],
		MutationID:   fields[mutationFMutationID],
		ResultStatus: fields[mutationFResultStatus],
	}
	if r.AppliedAtMs, err = atoi(fields[mutationFAppliedAtMs]); err != nil {
		return MutationReceipt{}, err
	}
	if r.MailboxRevision, err = parseUint(fields[mutationFMailboxRevision]); err != nil {
		return MutationReceipt{}, err
	}
	if r.RetainUntilMs, err = atoi(fields[mutationFRetainUntilMs]); err != nil {
		return MutationReceipt{}, err
	}
	return r, nil
}

// EncodeProjectionReceipt encodes a claim-projection work receipt.
func EncodeProjectionReceipt(r ProjectionReceipt, l Limits) ([]byte, error) {
	if r.WorkID == "" || r.WorkVersion == 0 {
		return nil, fmt.Errorf("%w: projection work identity", ErrMalformedBlob)
	}
	if err := ValidateMailID(r.MailID); err != nil {
		return nil, err
	}
	if err := ValidateHash(r.ClaimMaterialHash, false); err != nil {
		return nil, err
	}
	fields := []string{
		projectionSchema,
		r.WorkID,
		itoa(r.WorkVersion),
		r.MailID,
		r.ClaimMaterialHash,
		r.ResultStatus,
		itoaSigned(r.AppliedAtMs),
		itoaSigned(r.RetainUntilMs),
	}
	var blob []byte
	var err error
	for _, f := range fields {
		if blob, err = appendLP(blob, f); err != nil {
			return nil, err
		}
	}
	if len(blob) > l.MaxReceiptBytes {
		return nil, fmt.Errorf("%w: projection receipt exceeds %d bytes", ErrMalformedBlob, l.MaxReceiptBytes)
	}
	return blob, nil
}

// DecodeProjectionReceipt parses a projection receipt.
func DecodeProjectionReceipt(blob []byte) (ProjectionReceipt, error) {
	fields, err := parseLP(blob, projectionFieldCount)
	if err != nil {
		return ProjectionReceipt{}, err
	}
	if fields[projectionFSchema] != projectionSchema {
		return ProjectionReceipt{}, fmt.Errorf("%w: projection schema", ErrMalformedBlob)
	}
	r := ProjectionReceipt{
		WorkID:            fields[projectionFWorkID],
		MailID:            fields[projectionFMailID],
		ClaimMaterialHash: fields[projectionFClaimMaterialHash],
		ResultStatus:      fields[projectionFResultStatus],
	}
	if r.WorkVersion, err = parseUint(fields[projectionFWorkVersion]); err != nil {
		return ProjectionReceipt{}, err
	}
	if r.AppliedAtMs, err = atoi(fields[projectionFAppliedAtMs]); err != nil {
		return ProjectionReceipt{}, err
	}
	if r.RetainUntilMs, err = atoi(fields[projectionFRetainUntilMs]); err != nil {
		return ProjectionReceipt{}, err
	}
	return r, nil
}

func parseUint(v string) (uint64, error) {
	n, err := atoi(v)
	if err != nil {
		return 0, err
	}
	return uint64(n), nil
}
