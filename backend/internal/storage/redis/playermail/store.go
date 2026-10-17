package playermail

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"

	"farmgame/internal/player/mail"
	"farmgame/internal/storage/redis/rediskey"
)

// Store is the Redis adapter behind the PlayerMail ports (35A §10). It
// only calls the fixed script family, only addresses explicit fields, and
// classifies every transport failure into typed send fates.
type Store struct {
	client   redis.UniversalClient
	registry *scriptRegistry
	keys     KeyBuilderV1
	fields   FieldBuilderV1
	limits   mail.Limits
}

// NewStore validates the profile and limits and preloads the script family.
func NewStore(client redis.UniversalClient, profile Profile, limits mail.Limits) (*Store, error) {
	keys, err := NewKeyBuilderV1(profile)
	if err != nil {
		return nil, err
	}
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	registry, err := newScriptRegistry(client)
	if err != nil {
		return nil, err
	}
	return &Store{client: client, registry: registry, keys: keys, limits: limits}, nil
}

// EnsureLoaded preloads the script family on every known primary.
func (s *Store) EnsureLoaded(ctx context.Context) error {
	return s.registry.ensureLoaded(ctx)
}

// ScriptSHA exposes the frozen SHA-1 of one script for goldens.
func (s *Store) ScriptSHA(name string) (string, bool) {
	return s.registry.scriptSHA(name)
}

// Limits returns the validated limit profile.
func (s *Store) Limits() mail.Limits { return s.limits }

// Keys returns the key builder (maintenance tooling derives bucket keys).
func (s *Store) Keys() KeyBuilderV1 { return s.keys }

type mailboxKeys struct {
	bucket     uint32
	tag        string
	box        rediskey.RedisKey
	index      rediskey.RedisKey
	safeEvict  rediskey.RedisKey
	expireDue  rediskey.RedisKey
	gcDue      rediskey.RedisKey
	receiptDue rediskey.RedisKey
}

func (s *Store) mailbox(recipientPlayerID uint64) (mailboxKeys, error) {
	bucket, err := s.keys.BucketOf(recipientPlayerID)
	if err != nil {
		return mailboxKeys{}, err
	}
	tag, err := s.keys.HashTag(bucket)
	if err != nil {
		return mailboxKeys{}, err
	}
	mk := mailboxKeys{bucket: bucket, tag: tag}
	if mk.box, err = s.keys.Mailbox(bucket, recipientPlayerID); err != nil {
		return mailboxKeys{}, err
	}
	if mk.index, err = s.keys.VisibleIndex(bucket, recipientPlayerID); err != nil {
		return mailboxKeys{}, err
	}
	if mk.safeEvict, err = s.keys.SafeEvictIndex(bucket, recipientPlayerID); err != nil {
		return mailboxKeys{}, err
	}
	if mk.expireDue, err = s.keys.ExpireDueIndex(bucket); err != nil {
		return mailboxKeys{}, err
	}
	if mk.gcDue, err = s.keys.GcDueIndex(bucket); err != nil {
		return mailboxKeys{}, err
	}
	if mk.receiptDue, err = s.keys.ReceiptDueIndex(bucket); err != nil {
		return mailboxKeys{}, err
	}
	return mk, nil
}

func (s *Store) runScript(ctx context.Context, name string, keys []rediskey.RedisKey, args ...interface{}) ([]string, error) {
	if err := s.keys.AssertSameSlot(keys...); err != nil {
		return nil, fmt.Errorf("%w: %v", mail.ErrContractViolation, err)
	}
	raw := make([]string, 0, len(keys))
	for _, k := range keys {
		raw = append(raw, k.String())
	}
	out, err := s.registry.run(ctx, name, raw, args...)
	if err != nil {
		if isContractViolation(err) {
			return nil, fmt.Errorf("%w: %v", mail.ErrContractViolation, err)
		}
		return nil, err
	}
	if len(out) > 0 && out[0] == "CROSS_SLOT" {
		return nil, fmt.Errorf("%w: script rejected key layout", mail.ErrContractViolation)
	}
	if len(out) > 0 && out[0] == "RECONCILE_REQUIRED" {
		detail := ""
		if len(out) > 1 {
			detail = out[1]
		}
		return nil, fmt.Errorf("%w: reconcile required: %s", mail.ErrContractViolation, detail)
	}
	return out, nil
}

func summaryFromTail(tail []string) (mail.MailboxSummary, error) {
	if len(tail) < 7 {
		return mail.MailboxSummary{}, fmt.Errorf("%w: short summary tail", mail.ErrContractViolation)
	}
	var s mail.MailboxSummary
	var err error
	if s.MailboxRevision, err = parseUintField(tail[0]); err != nil {
		return s, err
	}
	if s.VisibleCount, err = parseIntField(tail[1]); err != nil {
		return s, err
	}
	if s.UnreadCount, err = parseIntField(tail[2]); err != nil {
		return s, err
	}
	if s.ClaimableCount, err = parseIntField(tail[3]); err != nil {
		return s, err
	}
	if s.RetainedCount, err = parseIntField(tail[4]); err != nil {
		return s, err
	}
	if s.RetainedBytes, err = parseIntField(tail[5]); err != nil {
		return s, err
	}
	if s.DeliveryRecords, err = parseIntField(tail[6]); err != nil {
		return s, err
	}
	return s, nil
}

func parseUintField(v string) (uint64, error) {
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: summary field %q", mail.ErrContractViolation, v)
	}
	return n, nil
}

func parseIntField(v string) (int64, error) {
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: summary field %q", mail.ErrContractViolation, v)
	}
	return n, nil
}

// InsertMailIfAbsent implements mail.MailEntryRepository.
func (s *Store) InsertMailIfAbsent(ctx context.Context, entry mail.MailEntry) (mail.InsertResult, error) {
	entryBlob, err := mail.EncodeMailEntry(entry, s.limits)
	if err != nil {
		return mail.InsertResult{}, err
	}
	state := mail.NewUserState(entry)
	stateBlob, err := mail.EncodeMailUserState(state, s.limits)
	if err != nil {
		return mail.InsertResult{}, err
	}
	recipient, err := mail.CanonicalPlayerID(entry.RecipientPlayerID)
	if err != nil {
		return mail.InsertResult{}, err
	}
	digest, err := mail.DeliveryDigest(entry.RecipientPlayerID, entry.DeliveryID)
	if err != nil {
		return mail.InsertResult{}, err
	}
	deliveryField, err := s.fields.DeliveryField(digest)
	if err != nil {
		return mail.InsertResult{}, err
	}
	entryField, err := s.fields.EntryField(entry.MailID)
	if err != nil {
		return mail.InsertResult{}, err
	}
	stateField, err := s.fields.StateField(entry.MailID)
	if err != nil {
		return mail.InsertResult{}, err
	}
	member, err := mail.EncodeSortMember(entry.CreatedAtMs, entry.MailID)
	if err != nil {
		return mail.InsertResult{}, err
	}
	expireRef, err := mail.EncodeDueRefV1(mail.MailDueRefV1{
		Kind: mail.DueKindExpireVisibility, PlayerID: entry.RecipientPlayerID, MailID: entry.MailID,
		MailVersion: entry.MailVersion, ExpectedStateVersion: state.StateVersion,
	})
	if err != nil {
		return mail.InsertResult{}, err
	}
	gcRef, err := mail.EncodeDueRefV1(mail.MailDueRefV1{
		Kind: mail.DueKindPhysicalGC, PlayerID: entry.RecipientPlayerID, MailID: entry.MailID,
		MailVersion: entry.MailVersion, ExpectedStateVersion: state.StateVersion,
	})
	if err != nil {
		return mail.InsertResult{}, err
	}
	mk, err := s.mailbox(entry.RecipientPlayerID)
	if err != nil {
		return mail.InsertResult{}, err
	}
	hasAttachment := "0"
	if entry.HasAttachment() {
		hasAttachment = "1"
	}
	out, err := s.runScript(ctx, ScriptInsertMailIfAbsent,
		[]rediskey.RedisKey{mk.box, mk.index, mk.safeEvict, mk.expireDue, mk.gcDue},
		mk.tag,
		entryField, string(entryBlob),
		stateField, string(stateBlob),
		deliveryField, recipient, entry.DeliveryID, entry.DeliveryPayloadHash,
		entry.MailID, string(member), expireRef, gcRef,
		entry.VisibleAtMs, entry.ExpireAtMs, entry.PhysicalPurgeAtMs, hasAttachment,
		s.limits.MaxVisibleCount, s.limits.MaxRetainedCount, s.limits.MaxRetainedBytes,
		s.limits.MaxDeliveryRecords, s.limits.MaxLazyEvictPerInsert,
		s.limits.MaxMailEntryBytes, s.limits.MaxMailStateBytes, s.limits.DeliveryRecordRetainMs)
	if err != nil {
		if errors.Is(err, mail.ErrContractViolation) {
			return mail.InsertResult{}, err
		}
		return mail.InsertResult{Status: insertTransportStatus(err)}, nil
	}
	switch out[0] {
	case "INSERTED":
		summary, err := summaryFromTail(out[2:])
		if err != nil {
			return mail.InsertResult{}, err
		}
		return mail.InsertResult{Status: mail.InsertInserted, MailID: out[1], Summary: summary}, nil
	case "ALREADY_DELIVERED":
		summary, err := summaryFromTail(out[2:])
		if err != nil {
			return mail.InsertResult{}, err
		}
		return mail.InsertResult{Status: mail.InsertAlreadyDelivered, MailID: out[1], Summary: summary}, nil
	case "DELIVERY_ID_CONFLICT":
		return mail.InsertResult{Status: mail.InsertDeliveryConflict}, nil
	case "MAILBOX_FULL_PROTECTED":
		return mail.InsertResult{Status: mail.InsertMailboxFullProtected}, nil
	case "CAPACITY_EXCEEDED":
		return mail.InsertResult{Status: mail.InsertCapacityExceeded}, nil
	case "NOT_VISIBLE_YET":
		return mail.InsertResult{Status: mail.InsertNotVisibleYet}, nil
	}
	return mail.InsertResult{}, fmt.Errorf("%w: insert status %q", mail.ErrContractViolation, out[0])
}

func insertTransportStatus(err error) mail.InsertStatus {
	if classifyWriteError(err) == fateNotSent {
		return mail.InsertUnavailableNotSent
	}
	return mail.InsertTimeoutUnknown
}

// preRead loads entry/state blobs for sort member derivation. Missing
// values yield empty strings; scripts treat empty sort members as unused.
func (s *Store) preRead(ctx context.Context, mk mailboxKeys, mailID string) (entry mail.MailEntry, state mail.MailUserState, sortMember string, err error) {
	entryField, err := s.fields.EntryField(mailID)
	if err != nil {
		return entry, state, "", err
	}
	stateField, err := s.fields.StateField(mailID)
	if err != nil {
		return entry, state, "", err
	}
	vals, err := s.client.HMGet(ctx, mk.box.String(), entryField, stateField).Result()
	if err != nil {
		return entry, state, "", err
	}
	if vals[0] != nil {
		blob, ok := vals[0].(string)
		if !ok {
			return entry, state, "", fmt.Errorf("%w: entry blob type", mail.ErrContractViolation)
		}
		if entry, err = mail.DecodeMailEntry([]byte(blob)); err != nil {
			return entry, state, "", err
		}
		member, merr := mail.EncodeSortMember(entry.CreatedAtMs, entry.MailID)
		if merr != nil {
			return entry, state, "", merr
		}
		sortMember = string(member)
	}
	if vals[1] != nil {
		blob, ok := vals[1].(string)
		if !ok {
			return entry, state, "", fmt.Errorf("%w: state blob type", mail.ErrContractViolation)
		}
		if state, err = mail.DecodeMailUserState([]byte(blob)); err != nil {
			return entry, state, "", err
		}
	}
	return entry, state, sortMember, nil
}

// MarkMailRead implements mail.MailEntryRepository.
func (s *Store) MarkMailRead(ctx context.Context, recipientPlayerID uint64, mailID, mutationID string) (mail.MutationResult, error) {
	return s.mutate(ctx, recipientPlayerID, mailID, mutationID, mail.MutationKindMarkRead, mail.AllowDelete())
}

// DeleteMail implements mail.MailEntryRepository. The authorization is
// structurally validated before any script runs (FIX-2); DeleteMailV2 then
// re-verifies the proof against the immutable entry.
func (s *Store) DeleteMail(ctx context.Context, recipientPlayerID uint64, mailID, mutationID string, auth mail.DeleteAuthorization) (mail.MutationResult, error) {
	if err := auth.Validate(mailID); err != nil {
		return mail.MutationResult{}, err
	}
	return s.mutate(ctx, recipientPlayerID, mailID, mutationID, mail.MutationKindDelete, auth)
}

func (s *Store) mutate(ctx context.Context, recipientPlayerID uint64, mailID, mutationID string, kind mail.MutationKind, auth mail.DeleteAuthorization) (mail.MutationResult, error) {
	digest, err := mail.MutationDigest(recipientPlayerID, mailID, kind, mutationID)
	if err != nil {
		return mail.MutationResult{}, err
	}
	mutationField, err := s.fields.MutationField(digest)
	if err != nil {
		return mail.MutationResult{}, err
	}
	entryField, err := s.fields.EntryField(mailID)
	if err != nil {
		return mail.MutationResult{}, err
	}
	stateField, err := s.fields.StateField(mailID)
	if err != nil {
		return mail.MutationResult{}, err
	}
	receiptIndexField, err := s.fields.ReceiptIndexField(mailID)
	if err != nil {
		return mail.MutationResult{}, err
	}
	receiptKind := ReceiptDueMutation
	if kind != mail.MutationKindMarkRead && kind != mail.MutationKindDelete {
		return mail.MutationResult{}, mail.ErrInvalidRequest
	}
	receiptField := mutationField
	receiptDueMember, err := EncodeReceiptDueRefV1(ReceiptDueRef{
		Kind: ReceiptDueKind(receiptKind), PlayerID: recipientPlayerID,
		MailID: mailID, ReceiptField: receiptField,
	})
	if err != nil {
		return mail.MutationResult{}, err
	}
	mk, err := s.mailbox(recipientPlayerID)
	if err != nil {
		return mail.MutationResult{}, err
	}
	_, _, sortMember, err := s.preRead(ctx, mk, mailID)
	if err != nil {
		return mail.MutationResult{}, err
	}
	var out []string
	if kind == mail.MutationKindMarkRead {
		out, err = s.runScript(ctx, ScriptMarkMailReadV3,
			[]rediskey.RedisKey{mk.box, mk.safeEvict, mk.receiptDue},
			mk.tag, mutationField, stateField, entryField, mailID, sortMember,
			mutationID, s.limits.MutationReceiptRetainMs, receiptIndexField,
			receiptDueMember, s.limits.MaxReceiptItems, s.limits.MaxReceiptTotalBytes,
			s.limits.MaxReceiptBytes)
	} else {
		proofMailID, proofHash, proofReceiptRef := "", "", ""
		if auth.ClaimProof != nil {
			proofMailID = auth.ClaimProof.MailID
			proofHash = auth.ClaimProof.ClaimMaterialHash
			proofReceiptRef = auth.ClaimProof.ReceiptRef
		}
		out, err = s.runScript(ctx, ScriptDeleteMailV3,
			[]rediskey.RedisKey{mk.box, mk.index, mk.safeEvict, mk.receiptDue},
			mk.tag, mutationField, stateField, entryField, mailID, sortMember,
			string(auth.Eligibility), proofMailID, proofHash, proofReceiptRef,
			mutationID, s.limits.MutationReceiptRetainMs, receiptIndexField,
			receiptDueMember, s.limits.MaxReceiptItems, s.limits.MaxReceiptTotalBytes,
			s.limits.MaxReceiptBytes)
	}
	if err != nil {
		if errors.Is(err, mail.ErrContractViolation) {
			return mail.MutationResult{}, err
		}
		return mail.MutationResult{Status: mutationTransportStatus(err)}, nil
	}
	if len(out) > 0 && out[0] == "CLAIM_PROOF_INVALID" {
		return mail.MutationResult{}, fmt.Errorf("%w: claim proof rejected by store", mail.ErrContractViolation)
	}
	if len(out) > 0 && out[0] == "CAPACITY_EXCEEDED" {
		return mail.MutationResult{}, fmt.Errorf("%w: receipt", mail.ErrReceiptCapacityExceeded)
	}
	return parseMutationReply(out)
}

func parseMutationReply(out []string) (mail.MutationResult, error) {
	if len(out) == 0 {
		return mail.MutationResult{}, fmt.Errorf("%w: empty mutation reply", mail.ErrContractViolation)
	}
	status := out[0]
	extra := out[1:]
	if status == "RECEIPT" {
		if len(extra) < 2 {
			return mail.MutationResult{}, fmt.Errorf("%w: short receipt reply", mail.ErrContractViolation)
		}
		status = extra[0]
		extra = extra[1:]
	}
	if len(extra) < 1 {
		return mail.MutationResult{}, fmt.Errorf("%w: short mutation reply", mail.ErrContractViolation)
	}
	applied, err := parseIntField(extra[0])
	if err != nil {
		return mail.MutationResult{}, err
	}
	summary, err := summaryFromTail(extra[1:])
	if err != nil {
		return mail.MutationResult{}, err
	}
	res := mail.MutationResult{Summary: summary, AppliedAtMs: applied}
	switch status {
	case "READ_MARKED":
		res.Status = mail.MutationReadMarked
	case "DELETED":
		res.Status = mail.MutationDeleted
	case "NO_CHANGE":
		res.Status = mail.MutationNoChange
	case "MAIL_NOT_FOUND":
		res.Status = mail.MutationMailNotFound
	case "REJECT_UNCLAIMED":
		res.Status = mail.MutationRejectUnclaimed
	default:
		return mail.MutationResult{}, fmt.Errorf("%w: mutation status %q", mail.ErrContractViolation, status)
	}
	return res, nil
}

func mutationTransportStatus(err error) mail.MutationStatus {
	if classifyWriteError(err) == fateNotSent {
		return mail.MutationUnavailableNotSent
	}
	return mail.MutationTimeoutUnknown
}

// ApplyMailClaimProjection implements mail.MailEntryRepository.
func (s *Store) ApplyMailClaimProjection(ctx context.Context, order mail.ClaimProjectionOrder) (mail.ProjectionResult, error) {
	digest, err := mail.ProjectionDigest(order.RecipientPlayerID, order.WorkID, order.WorkVersion)
	if err != nil {
		return mail.ProjectionResult{}, err
	}
	projectionField, err := s.fields.ProjectionField(digest)
	if err != nil {
		return mail.ProjectionResult{}, err
	}
	entryField, err := s.fields.EntryField(order.MailID)
	if err != nil {
		return mail.ProjectionResult{}, err
	}
	stateField, err := s.fields.StateField(order.MailID)
	if err != nil {
		return mail.ProjectionResult{}, err
	}
	receiptIndexField, err := s.fields.ReceiptIndexField(order.MailID)
	if err != nil {
		return mail.ProjectionResult{}, err
	}
	receiptDueMember, err := EncodeReceiptDueRefV1(ReceiptDueRef{
		Kind: ReceiptDueProjection, PlayerID: order.RecipientPlayerID,
		MailID: order.MailID, ReceiptField: projectionField,
	})
	if err != nil {
		return mail.ProjectionResult{}, err
	}
	if err := mail.ValidateHash(order.ClaimMaterialHash, false); err != nil {
		return mail.ProjectionResult{}, err
	}
	mk, err := s.mailbox(order.RecipientPlayerID)
	if err != nil {
		return mail.ProjectionResult{}, err
	}
	_, _, sortMember, err := s.preRead(ctx, mk, order.MailID)
	if err != nil {
		return mail.ProjectionResult{}, err
	}
	out, err := s.runScript(ctx, ScriptApplyMailClaimProjectionV3,
		[]rediskey.RedisKey{mk.box, mk.safeEvict, mk.receiptDue},
		mk.tag, projectionField, stateField, entryField, order.MailID, sortMember,
		order.WorkID, order.WorkVersion, order.ClaimReceiptRef, order.ClaimMaterialHash,
		order.ClaimedAtMs, s.limits.ProjectionReceiptRetainMs, receiptIndexField,
		receiptDueMember, s.limits.MaxReceiptItems, s.limits.MaxReceiptTotalBytes,
		s.limits.MaxReceiptBytes)
	if err != nil {
		if errors.Is(err, mail.ErrContractViolation) {
			return mail.ProjectionResult{}, err
		}
		return mail.ProjectionResult{Status: projectionTransportStatus(err)}, nil
	}
	if len(out) > 0 && out[0] == "CAPACITY_EXCEEDED" {
		return mail.ProjectionResult{}, fmt.Errorf("%w: receipt", mail.ErrReceiptCapacityExceeded)
	}
	return parseProjectionReply(out)
}

func parseProjectionReply(out []string) (mail.ProjectionResult, error) {
	if len(out) == 0 {
		return mail.ProjectionResult{}, fmt.Errorf("%w: empty projection reply", mail.ErrContractViolation)
	}
	status := out[0]
	extra := out[1:]
	if status == "RECEIPT" {
		if len(extra) < 2 {
			return mail.ProjectionResult{}, fmt.Errorf("%w: short projection receipt", mail.ErrContractViolation)
		}
		status = extra[0]
		extra = extra[1:]
	}
	if len(extra) < 1 {
		return mail.ProjectionResult{}, fmt.Errorf("%w: short projection reply", mail.ErrContractViolation)
	}
	summary, err := summaryFromTail(extra[1:])
	if err != nil {
		return mail.ProjectionResult{}, err
	}
	res := mail.ProjectionResult{Summary: summary}
	switch status {
	case "APPLIED":
		res.Status = mail.ProjectionApplied
	case "NO_CHANGE":
		res.Status = mail.ProjectionNoChange
	case "TARGET_ALREADY_PURGED":
		res.Status = mail.ProjectionTargetAlreadyPurged
	case "CLAIM_MATERIAL_HASH_MISMATCH":
		res.Status = mail.ProjectionHashMismatch
	default:
		return mail.ProjectionResult{}, fmt.Errorf("%w: projection status %q", mail.ErrContractViolation, status)
	}
	return res, nil
}

func projectionTransportStatus(err error) mail.ProjectionStatus {
	if classifyWriteError(err) == fateNotSent {
		return mail.ProjectionUnavailableNotSent
	}
	return mail.ProjectionTimeoutUnknown
}

// GetEntry implements mail.MailReadRepository.
func (s *Store) GetEntry(ctx context.Context, recipientPlayerID uint64, mailID string) (mail.MailEntry, mail.MailUserState, error) {
	mk, err := s.mailbox(recipientPlayerID)
	if err != nil {
		return mail.MailEntry{}, mail.MailUserState{}, err
	}
	entry, state, _, err := s.preRead(ctx, mk, mailID)
	if err != nil {
		return mail.MailEntry{}, mail.MailUserState{}, err
	}
	if entry.MailID == "" {
		return mail.MailEntry{}, mail.MailUserState{}, mail.ErrMailNotFound
	}
	return entry, state, nil
}

// GetSummary implements mail.MailReadRepository.
func (s *Store) GetSummary(ctx context.Context, recipientPlayerID uint64) (mail.MailboxSummary, error) {
	mk, err := s.mailbox(recipientPlayerID)
	if err != nil {
		return mail.MailboxSummary{}, err
	}
	vals, err := s.client.HMGet(ctx, mk.box.String(),
		MetaMailboxRevision, MetaVisibleCount, MetaUnreadCount, MetaClaimableCount,
		MetaRetainedCount, MetaRetainedBytes, MetaDeliveryRecords).Result()
	if err != nil {
		return mail.MailboxSummary{}, err
	}
	tail := make([]string, 0, 7)
	for _, v := range vals {
		if v == nil {
			tail = append(tail, "0")
			continue
		}
		str, ok := v.(string)
		if !ok {
			return mail.MailboxSummary{}, fmt.Errorf("%w: summary field type", mail.ErrContractViolation)
		}
		tail = append(tail, str)
	}
	return summaryFromTail(tail)
}

var errNilReply = errors.New("playermail: nil reply")
