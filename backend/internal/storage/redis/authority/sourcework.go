package authority

import (
	"context"
	"fmt"
	"strconv"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
)

var sourceWorkFields = []string{
	"schema_version", "work_id", "work_type", "placement_kind",
	"bucket_version", "bucket_id", "source_authority_kind",
	"canonical_source_authority_id", "authority_layout_version",
	"actor_route_domain", "subject_player_id", "operation_id", "payload_hash",
	"state", "work_version", "current_step", "typed_payload", "next_due_at_ms",
	"retry_count", "last_error_class", "lease_holder_id", "lease_holder_boot_id", "lease_epoch",
	"lease_token_hash", "lease_expire_at_ms", "created_at_ms", "updated_at_ms",
	"completed_at_ms",
}

func sourceWorkIdleDefault(field string) (string, bool) {
	switch field {
	case "current_step", "last_error_class", "lease_holder_id", "lease_holder_boot_id", "lease_token_hash":
		return "", true
	case "lease_epoch", "lease_expire_at_ms", "completed_at_ms":
		return "0", true
	default:
		return "", false
	}
}

var _ authoritystore.SourceWorkReader = (*Store)(nil)

// ReadSourceWork reads the existing SourceOrder and Redis TIME. The two reads
// are intentionally separate: Redis Cluster cannot put a keyless TIME command
// and a keyed HMGET into one MULTI/EXEC transaction. ReadAtMs is diagnostic
// metadata, not a consistency fence; the source record itself remains the
// authoritative proof.
func (s *Store) ReadSourceWork(ctx context.Context, source authoritystore.ActorAddress, workType, workID string) (authoritystore.SourceWorkRecord, error) {
	if err := ctx.Err(); err != nil {
		return authoritystore.SourceWorkRecord{}, err
	}
	if s == nil || s.client == nil || source.ActorID == 0 ||
		(source.Domain != actorrouting.DomainPlayer && source.Domain != actorrouting.DomainFarm) {
		return authoritystore.SourceWorkRecord{}, fmt.Errorf("%w: source work address", authoritystore.ErrInvalidRequest)
	}
	bucket, err := s.bucketFor(source)
	if err != nil {
		return authoritystore.SourceWorkRecord{}, err
	}
	_, workKey, _, _, _, err := s.WorkLease().keysFor(bucket, workType, workID)
	if err != nil {
		return authoritystore.SourceWorkRecord{}, err
	}
	now, err := s.client.Time(ctx).Result()
	if err != nil {
		return authoritystore.SourceWorkRecord{}, err
	}
	values, err := s.client.HMGet(ctx, workKey.String(), sourceWorkFields...).Result()
	if err != nil {
		return authoritystore.SourceWorkRecord{}, err
	}
	if len(values) != len(sourceWorkFields) {
		return authoritystore.SourceWorkRecord{}, fmt.Errorf("%w: source work field count", errMalformedBlob)
	}
	if values[0] == nil {
		return authoritystore.SourceWorkRecord{}, authoritystore.ErrSourceWorkNotFound
	}
	strings := make([]string, len(values))
	for i, value := range values {
		if value == nil {
			if def, ok := sourceWorkIdleDefault(sourceWorkFields[i]); ok {
				strings[i] = def
				continue
			}
			return authoritystore.SourceWorkRecord{}, fmt.Errorf("%w: source work field %s", errMalformedBlob, sourceWorkFields[i])
		}
		switch value := value.(type) {
		case string:
			strings[i] = value
		case []byte:
			strings[i] = string(value)
		default:
			return authoritystore.SourceWorkRecord{}, fmt.Errorf("%w: source work field type %s", errMalformedBlob, sourceWorkFields[i])
		}
	}
	parseUint := func(index int, bits int) (uint64, error) {
		value, parseErr := strconv.ParseUint(strings[index], 10, bits)
		if parseErr != nil {
			return 0, fmt.Errorf("%w: source work field %s", errMalformedBlob, sourceWorkFields[index])
		}
		return value, nil
	}
	parseInt := func(index int) (int64, error) {
		value, parseErr := strconv.ParseInt(strings[index], 10, 64)
		if parseErr != nil {
			return 0, fmt.Errorf("%w: source work field %s", errMalformedBlob, sourceWorkFields[index])
		}
		return value, nil
	}
	bucketVersion, err := parseUint(4, 32)
	if err != nil {
		return authoritystore.SourceWorkRecord{}, err
	}
	bucketID, err := parseUint(5, 32)
	if err != nil {
		return authoritystore.SourceWorkRecord{}, err
	}
	subject, err := parseUint(10, 64)
	if err != nil {
		return authoritystore.SourceWorkRecord{}, err
	}
	workVersion, err := parseUint(14, 64)
	if err != nil {
		return authoritystore.SourceWorkRecord{}, err
	}
	nextDue, err := parseInt(17)
	if err != nil {
		return authoritystore.SourceWorkRecord{}, err
	}
	retryCount, err := parseUint(18, 64)
	if err != nil {
		return authoritystore.SourceWorkRecord{}, err
	}
	leaseEpoch, err := parseUint(22, 64)
	if err != nil {
		return authoritystore.SourceWorkRecord{}, err
	}
	leaseExpire, err := parseInt(24)
	if err != nil {
		return authoritystore.SourceWorkRecord{}, err
	}
	createdAt, err := parseInt(25)
	if err != nil {
		return authoritystore.SourceWorkRecord{}, err
	}
	updatedAt, err := parseInt(26)
	if err != nil {
		return authoritystore.SourceWorkRecord{}, err
	}
	completedAt, err := parseInt(27)
	if err != nil {
		return authoritystore.SourceWorkRecord{}, err
	}
	return authoritystore.SourceWorkRecord{
		SchemaVersion: strings[0], WorkID: strings[1], WorkType: strings[2],
		PlacementKind: strings[3], BucketVersion: uint32(bucketVersion), BucketID: uint32(bucketID),
		SourceAuthorityKind: strings[6], CanonicalSourceAuthority: strings[7],
		AuthorityLayoutVersion: strings[8], ActorRouteDomain: actorrouting.RouteDomain(strings[9]),
		SubjectPlayerID: subject, OperationID: strings[11], PayloadHash: strings[12],
		State: strings[13], WorkVersion: workVersion, CurrentStep: strings[15],
		TypedPayload: []byte(strings[16]), NextDueAtMs: nextDue, RetryCount: retryCount,
		LastErrorClass: strings[19], LeaseHolderID: strings[20], LeaseHolderBootID: strings[21], LeaseEpoch: leaseEpoch,
		LeaseTokenHash: strings[23], LeaseExpireAtMs: leaseExpire, CreatedAtMs: createdAt,
		UpdatedAtMs: updatedAt, CompletedAtMs: completedAt, ReadAtMs: now.UnixMilli(),
	}, nil
}
