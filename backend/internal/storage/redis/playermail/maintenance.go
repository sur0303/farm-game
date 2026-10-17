package playermail

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"

	"farmgame/internal/player/mail"
	"farmgame/internal/storage/redis/rediskey"
)

// ClaimScanLease implements advisory duplicate-scan coordination. It is not
// a write-authority fence; all maintenance mutations enforce their own
// version/hash CAS and idempotency. TTL clamps are shared with the domain.
func (s *Store) ClaimScanLease(ctx context.Context, bucketID uint32, ownerToken string, requestedTTLMs int64) (mail.ScanLeaseResult, error) {
	if ownerToken == "" || len(ownerToken) > 128 || requestedTTLMs <= 0 {
		return mail.ScanLeaseResult{}, mail.ErrInvalidRequest
	}
	key, err := s.keys.ScanLease(bucketID)
	if err != nil {
		return mail.ScanLeaseResult{}, err
	}
	tag, err := s.keys.HashTag(bucketID)
	if err != nil {
		return mail.ScanLeaseResult{}, err
	}
	out, err := s.runScript(ctx, ScriptClaimMailScanLease,
		[]rediskey.RedisKey{key}, tag, ownerToken, requestedTTLMs, mail.MinScanLeaseTTLMs, mail.MaxScanLeaseTTLMs)
	if err != nil {
		if errors.Is(err, mail.ErrContractViolation) {
			return mail.ScanLeaseResult{}, err
		}
		if classifyWriteError(err) == fateNotSent {
			return mail.ScanLeaseResult{Status: mail.ScanLeaseUnavailableNotSent}, nil
		}
		return mail.ScanLeaseResult{Status: mail.ScanLeaseTimeoutUnknown}, nil
	}
	switch out[0] {
	case "LEASED":
		ttl, err := parseIntField(out[1])
		if err != nil {
			return mail.ScanLeaseResult{}, err
		}
		epoch, err := parseUintField(out[2])
		if err != nil {
			return mail.ScanLeaseResult{}, err
		}
		return mail.ScanLeaseResult{Status: mail.ScanLeaseAcquired, AcceptedTTLMs: ttl, LeaseEpoch: epoch}, nil
	case "HELD_BY_OTHER":
		return mail.ScanLeaseResult{Status: mail.ScanLeaseHeldByOther}, nil
	}
	return mail.ScanLeaseResult{}, fmt.Errorf("%w: lease status %q", mail.ErrContractViolation, out[0])
}

// ListExpireDue implements mail.MailMaintenanceRepository.
func (s *Store) ListExpireDue(ctx context.Context, bucketID uint32, nowMs int64, limit int) ([]mail.DueCandidate, error) {
	key, err := s.keys.ExpireDueIndex(bucketID)
	if err != nil {
		return nil, err
	}
	return s.listDue(ctx, key, nowMs, limit)
}

// ListGcDue implements mail.MailMaintenanceRepository.
func (s *Store) ListGcDue(ctx context.Context, bucketID uint32, nowMs int64, limit int) ([]mail.DueCandidate, error) {
	key, err := s.keys.GcDueIndex(bucketID)
	if err != nil {
		return nil, err
	}
	return s.listDue(ctx, key, nowMs, limit)
}

// ListReceiptDue returns a bounded set of receipt-expiry references. The
// worker can feed each member to GcReceipt without scanning mailbox hashes.
func (s *Store) ListReceiptDue(ctx context.Context, bucketID uint32, nowMs int64, limit int) ([]ReceiptDueCandidate, error) {
	if limit <= 0 || limit > 512 {
		return nil, mail.ErrInvalidRequest
	}
	key, err := s.keys.ReceiptDueIndex(bucketID)
	if err != nil {
		return nil, err
	}
	members, err := s.client.ZRangeByScoreWithScores(ctx, key.String(), &redis.ZRangeBy{
		Min: "0", Max: fmt.Sprintf("%d", nowMs), Offset: 0, Count: int64(limit),
	}).Result()
	if err != nil {
		return nil, err
	}
	out := make([]ReceiptDueCandidate, 0, len(members))
	for _, z := range members {
		member, ok := z.Member.(string)
		if !ok {
			return nil, fmt.Errorf("%w: receipt due member type", mail.ErrContractViolation)
		}
		ref, err := DecodeReceiptDueRefV1(member)
		if err != nil {
			return nil, fmt.Errorf("%w: receipt due member: %v", mail.ErrContractViolation, err)
		}
		bucket, berr := s.keys.BucketOf(ref.PlayerID)
		if berr != nil || bucket != bucketID {
			return nil, fmt.Errorf("%w: receipt due bucket", mail.ErrContractViolation)
		}
		out = append(out, ReceiptDueCandidate{Ref: ref, DueAtMs: int64(z.Score), Member: member})
	}
	return out, nil
}

func (s *Store) listDue(ctx context.Context, key rediskey.RedisKey, nowMs int64, limit int) ([]mail.DueCandidate, error) {
	if limit <= 0 || limit > 512 {
		return nil, mail.ErrInvalidRequest
	}
	members, err := s.client.ZRangeByScoreWithScores(ctx, key.String(), &redis.ZRangeBy{
		Min: "0", Max: fmt.Sprintf("%d", nowMs), Offset: 0, Count: int64(limit),
	}).Result()
	if err != nil {
		return nil, err
	}
	out := make([]mail.DueCandidate, 0, len(members))
	for _, z := range members {
		member, ok := z.Member.(string)
		if !ok {
			return nil, fmt.Errorf("%w: due member type", mail.ErrContractViolation)
		}
		ref, err := mail.DecodeDueRefV1(member)
		if err != nil {
			return nil, fmt.Errorf("%w: due member: %v", mail.ErrContractViolation, err)
		}
		out = append(out, mail.DueCandidate{Ref: ref, DueAtMs: int64(z.Score), Member: member})
	}
	return out, nil
}

// RegisterDueRef implements mail.MailMaintenanceRepository.
func (s *Store) RegisterDueRef(ctx context.Context, ref mail.MailDueRefV1, scoreMs int64) error {
	if scoreMs <= 0 {
		return mail.ErrInvalidRequest
	}
	member, err := mail.EncodeDueRefV1(ref)
	if err != nil {
		return err
	}
	bucket, err := s.keys.BucketOf(ref.PlayerID)
	if err != nil {
		return err
	}
	var key rediskey.RedisKey
	switch ref.Kind {
	case mail.DueKindExpireVisibility:
		key, err = s.keys.ExpireDueIndex(bucket)
	case mail.DueKindPhysicalGC:
		key, err = s.keys.GcDueIndex(bucket)
	default:
		return mail.ErrInvalidRequest
	}
	if err != nil {
		return err
	}
	return s.client.ZAdd(ctx, key.String(), redis.Z{Score: float64(scoreMs), Member: member}).Err()
}

// ExpireVisibility implements mail.MailMaintenanceRepository.
func (s *Store) ExpireVisibility(ctx context.Context, ref mail.MailDueRefV1) (mail.ExpireResult, error) {
	if ref.Kind != mail.DueKindExpireVisibility {
		return mail.ExpireResult{}, mail.ErrInvalidRequest
	}
	mk, err := s.mailbox(ref.PlayerID)
	if err != nil {
		return mail.ExpireResult{}, err
	}
	entryField, err := s.fields.EntryField(ref.MailID)
	if err != nil {
		return mail.ExpireResult{}, err
	}
	stateField, err := s.fields.StateField(ref.MailID)
	if err != nil {
		return mail.ExpireResult{}, err
	}
	dueMember, err := mail.EncodeDueRefV1(ref)
	if err != nil {
		return mail.ExpireResult{}, err
	}
	entry, state, sortMember, err := s.preRead(ctx, mk, ref.MailID)
	if err != nil {
		return mail.ExpireResult{}, err
	}
	freshDueMember, freshMailVersion, freshStateVersion, err := currentDueMember(ref, entry, state)
	if err != nil {
		return mail.ExpireResult{}, err
	}
	out, err := s.runScript(ctx, ScriptExpireMailVisibilityV2,
		[]rediskey.RedisKey{mk.box, mk.index, mk.safeEvict, mk.expireDue},
		mk.tag, stateField, entryField, ref.MailID, sortMember, dueMember,
		ref.MailVersion, ref.ExpectedStateVersion,
		freshDueMember, freshMailVersion, freshStateVersion)
	if err != nil {
		if errors.Is(err, mail.ErrContractViolation) {
			return mail.ExpireResult{}, err
		}
		if classifyWriteError(err) == fateNotSent {
			return mail.ExpireResult{Status: mail.ExpireUnavailableNotSent}, nil
		}
		return mail.ExpireResult{Status: mail.ExpireTimeoutUnknown}, nil
	}
	switch out[0] {
	case "EXPIRED":
		stateVersion, err := parseUintField(out[1])
		if err != nil {
			return mail.ExpireResult{}, err
		}
		expireAt, err := parseIntField(out[2])
		if err != nil {
			return mail.ExpireResult{}, err
		}
		summary, err := summaryFromTail(out[3:])
		if err != nil {
			return mail.ExpireResult{}, err
		}
		return mail.ExpireResult{Status: mail.ExpireApplied, Summary: summary, CurrentStateVersion: stateVersion, ExpireAtMs: expireAt}, nil
	case "TARGET_ALREADY_PURGED":
		return mail.ExpireResult{Status: mail.ExpireTargetAlreadyPurged}, nil
	case "STALE_REF":
		return parseExpireRefReply(out, mail.ExpireStaleRef)
	case "NO_CHANGE":
		return parseExpireRefReply(out, mail.ExpireNoChange)
	case "NOT_DUE":
		return parseExpireRefReply(out, mail.ExpireNotDue)
	}
	return mail.ExpireResult{}, fmt.Errorf("%w: expire status %q", mail.ErrContractViolation, out[0])
}

func parseExpireRefReply(out []string, status mail.ExpireStatus) (mail.ExpireResult, error) {
	if len(out) < 3 {
		return mail.ExpireResult{}, fmt.Errorf("%w: short expire reply", mail.ErrContractViolation)
	}
	stateVersion, err := parseUintField(out[1])
	if err != nil {
		return mail.ExpireResult{}, err
	}
	expireAt, err := parseIntField(out[2])
	if err != nil {
		return mail.ExpireResult{}, err
	}
	return mail.ExpireResult{Status: status, CurrentStateVersion: stateVersion, ExpireAtMs: expireAt}, nil
}

// GcMail implements mail.MailMaintenanceRepository.
func (s *Store) GcMail(ctx context.Context, ref mail.MailDueRefV1) (mail.GcResult, error) {
	if ref.Kind != mail.DueKindPhysicalGC {
		return mail.GcResult{}, mail.ErrInvalidRequest
	}
	mk, err := s.mailbox(ref.PlayerID)
	if err != nil {
		return mail.GcResult{}, err
	}
	entryField, err := s.fields.EntryField(ref.MailID)
	if err != nil {
		return mail.GcResult{}, err
	}
	stateField, err := s.fields.StateField(ref.MailID)
	if err != nil {
		return mail.GcResult{}, err
	}
	receiptIndexField, err := s.fields.ReceiptIndexField(ref.MailID)
	if err != nil {
		return mail.GcResult{}, err
	}
	dueMember, err := mail.EncodeDueRefV1(ref)
	if err != nil {
		return mail.GcResult{}, err
	}
	// The delivery digest derives from the immutable entry; when the entry
	// is already gone the script takes the purged/reconcile path and the
	// dedup record needs no field.
	deliveryField := ""
	entry, state, _, err := s.preRead(ctx, mk, ref.MailID)
	if err != nil {
		return mail.GcResult{}, err
	}
	if entry.MailID != "" {
		digest, derr := mail.DeliveryDigest(entry.RecipientPlayerID, entry.DeliveryID)
		if derr != nil {
			return mail.GcResult{}, derr
		}
		if deliveryField, err = s.fields.DeliveryField(digest); err != nil {
			return mail.GcResult{}, err
		}
	}
	freshDueMember, freshMailVersion, freshStateVersion, err := currentDueMember(ref, entry, state)
	if err != nil {
		return mail.GcResult{}, err
	}
	out, err := s.runScript(ctx, ScriptGcMailV4,
		[]rediskey.RedisKey{mk.box, mk.index, mk.safeEvict, mk.gcDue, mk.receiptDue},
		mk.tag, stateField, entryField, deliveryField, receiptIndexField,
		ref.MailID, dueMember, ref.MailVersion, ref.ExpectedStateVersion,
		s.limits.MaxReceiptItems, s.limits.MaxReceiptTotalBytes,
		freshDueMember, freshMailVersion, freshStateVersion)
	if err != nil {
		if errors.Is(err, mail.ErrContractViolation) {
			return mail.GcResult{}, err
		}
		if classifyWriteError(err) == fateNotSent {
			return mail.GcResult{Status: mail.GcUnavailableNotSent}, nil
		}
		return mail.GcResult{Status: mail.GcTimeoutUnknown}, nil
	}
	switch out[0] {
	case "PURGED":
		freed, err := parseIntField(out[1])
		if err != nil {
			return mail.GcResult{}, err
		}
		return mail.GcResult{Status: mail.GcPurged, FreedBytes: freed}, nil
	case "TARGET_ALREADY_PURGED":
		return mail.GcResult{Status: mail.GcTargetAlreadyPurged}, nil
	case "STALE_REF":
		return parseGcRefReply(out, mail.GcStaleRef)
	case "NOT_DUE":
		return parseGcRefReply(out, mail.GcNotDue)
	}
	return mail.GcResult{}, fmt.Errorf("%w: gc status %q", mail.ErrContractViolation, out[0])
}

// GcReceipt removes one expired mutation/projection receipt. The script
// verifies the indexed field, member and embedded mail id in one same-slot
// operation; an active receipt is never removed and its due score is
// atomically advanced to retain_until using Redis TIME.
func (s *Store) GcReceipt(ctx context.Context, ref ReceiptDueRef) (ReceiptGcResult, error) {
	if err := ref.validate(); err != nil {
		return ReceiptGcResult{}, err
	}
	mk, err := s.mailbox(ref.PlayerID)
	if err != nil {
		return ReceiptGcResult{}, err
	}
	receiptIndexField, err := s.fields.ReceiptIndexField(ref.MailID)
	if err != nil {
		return ReceiptGcResult{}, err
	}
	dueMember, err := EncodeReceiptDueRefV1(ref)
	if err != nil {
		return ReceiptGcResult{}, err
	}
	out, err := s.runScript(ctx, ScriptGcMailReceiptV2,
		[]rediskey.RedisKey{mk.box, mk.receiptDue},
		mk.tag, receiptIndexField, ref.ReceiptField, dueMember, ref.MailID,
		s.limits.MaxReceiptItems, s.limits.MaxReceiptTotalBytes)
	if err != nil {
		if errors.Is(err, mail.ErrContractViolation) {
			return ReceiptGcResult{}, err
		}
		if classifyWriteError(err) == fateNotSent {
			return ReceiptGcResult{Status: ReceiptGcUnavailableNotSent}, nil
		}
		return ReceiptGcResult{Status: ReceiptGcTimeoutUnknown}, nil
	}
	if len(out) == 0 {
		return ReceiptGcResult{}, fmt.Errorf("%w: empty receipt gc reply", mail.ErrContractViolation)
	}
	switch out[0] {
	case "PURGED":
		if len(out) < 2 {
			return ReceiptGcResult{}, fmt.Errorf("%w: short receipt gc reply", mail.ErrContractViolation)
		}
		freed, err := parseIntField(out[1])
		if err != nil {
			return ReceiptGcResult{}, err
		}
		return ReceiptGcResult{Status: ReceiptGcPurged, FreedBytes: freed}, nil
	case "NOT_DUE":
		if len(out) < 2 {
			return ReceiptGcResult{}, fmt.Errorf("%w: short receipt gc reply", mail.ErrContractViolation)
		}
		retainUntil, err := parseIntField(out[1])
		if err != nil {
			return ReceiptGcResult{}, err
		}
		return ReceiptGcResult{Status: ReceiptGcNotDue, RetainUntil: retainUntil}, nil
	case "TARGET_ALREADY_PURGED":
		return ReceiptGcResult{Status: ReceiptGcTargetAlreadyPurged}, nil
	}
	return ReceiptGcResult{}, fmt.Errorf("%w: receipt gc status %q", mail.ErrContractViolation, out[0])
}

func parseGcRefReply(out []string, status mail.GcStatus) (mail.GcResult, error) {
	if len(out) < 4 {
		return mail.GcResult{}, fmt.Errorf("%w: short gc reply", mail.ErrContractViolation)
	}
	mailVersion, err := parseUintField(out[1])
	if err != nil {
		return mail.GcResult{}, err
	}
	stateVersion, err := parseUintField(out[2])
	if err != nil {
		return mail.GcResult{}, err
	}
	purgeAt, err := parseIntField(out[3])
	if err != nil {
		return mail.GcResult{}, err
	}
	return mail.GcResult{
		Status:              status,
		CurrentMailVersion:  mailVersion,
		CurrentStateVersion: stateVersion,
		PurgeAtMs:           purgeAt,
	}, nil
}

func currentDueMember(ref mail.MailDueRefV1, entry mail.MailEntry, state mail.MailUserState) (string, uint64, uint64, error) {
	if entry.MailID == "" || state.MailID == "" {
		return "", 0, 0, nil
	}
	if entry.MailID != ref.MailID || state.MailID != ref.MailID || entry.MailVersion == 0 || state.StateVersion == 0 {
		return "", 0, 0, mail.ErrContractViolation
	}
	fresh := ref
	fresh.MailVersion = entry.MailVersion
	fresh.ExpectedStateVersion = state.StateVersion
	member, err := mail.EncodeDueRefV1(fresh)
	if err != nil {
		return "", 0, 0, err
	}
	return member, entry.MailVersion, state.StateVersion, nil
}
