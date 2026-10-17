package theftrecovery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"farmgame/internal/workidentity"

	"github.com/redis/go-redis/v9"
)

// WorkLeaseKeyResolver supplies the already validated Pair Authority keys.
// The worker does not construct authority keys by string concatenation; the
// production Pair adapter owns this resolver and uses its frozen key builder.
type WorkLeaseKeyResolver interface {
	WorkKey(WorkAddress) (string, error)
	DueIndexKey(WorkAddress) (string, error)
	BucketMetaKey(WorkAddress) (string, error)
	RecoveryKey(WorkAddress) (string, error)
}

// WorkLeaseKeyFuncs is convenient for adapters and tests while keeping the
// Redis lease implementation independent of a particular authority package.
type WorkLeaseKeyFuncs struct {
	Work       func(WorkAddress) (string, error)
	Due        func(WorkAddress) (string, error)
	BucketMeta func(WorkAddress) (string, error)
	Recovery   func(WorkAddress) (string, error)
}

func (f WorkLeaseKeyFuncs) WorkKey(address WorkAddress) (string, error) {
	if f.Work == nil {
		return "", ErrInvalidConfig
	}
	return f.Work(address)
}

func (f WorkLeaseKeyFuncs) DueIndexKey(address WorkAddress) (string, error) {
	if f.Due == nil {
		return "", ErrInvalidConfig
	}
	return f.Due(address)
}

func (f WorkLeaseKeyFuncs) BucketMetaKey(address WorkAddress) (string, error) {
	if f.BucketMeta == nil {
		return "", ErrInvalidConfig
	}
	return f.BucketMeta(address)
}

func (f WorkLeaseKeyFuncs) RecoveryKey(address WorkAddress) (string, error) {
	if f.Recovery == nil {
		return "", ErrInvalidConfig
	}
	return f.Recovery(address)
}

type RedisWorkLeaseConfig struct {
	Client redis.UniversalClient
	Keys   WorkLeaseKeyResolver
	MinTTL time.Duration
	MaxTTL time.Duration
}

// RedisWorkLeaseRepository is the recovery-only WorkLease implementation.
// Every mutating operation is one Redis Lua invocation and all adjudicated
// timestamps come from Redis TIME. Recovery metadata lives in a sidecar hash
// because FRIEND_PAIR source-order manifests are exact and cannot be widened
// with unrecognised fields.
type RedisWorkLeaseRepository struct {
	client redis.UniversalClient
	keys   WorkLeaseKeyResolver
	minTTL time.Duration
	maxTTL time.Duration
}

func pairSourceOrderChargedBytes(order DispatchOrder) int64 {
	return int64(512 + 16 + len(order.Work.WorkID) + 16 + len(order.Work.WorkType) + 16 + len(order.Payload) +
		16 + len(order.WorkRef) + 16 + len(HexHash(order.PayloadHash)) + 16 + len("0"))
}

func NewRedisWorkLeaseRepository(config RedisWorkLeaseConfig) (*RedisWorkLeaseRepository, error) {
	if config.Client == nil || config.Keys == nil {
		return nil, fmt.Errorf("%w: Redis client and key resolver are required", ErrInvalidConfig)
	}
	if config.MinTTL <= 0 {
		config.MinTTL = 100 * time.Millisecond
	}
	if config.MaxTTL <= 0 {
		config.MaxTTL = 2 * time.Minute
	}
	if config.MinTTL > config.MaxTTL {
		return nil, fmt.Errorf("%w: lease TTL bounds", ErrInvalidConfig)
	}
	return &RedisWorkLeaseRepository{client: config.Client, keys: config.Keys,
		minTTL: config.MinTTL, maxTTL: config.MaxTTL}, nil
}

var (
	redisClaimTheftWork       = redis.NewScript(luaClaimTheftWork)
	redisVerifyTheftWorkLease = redis.NewScript(luaVerifyTheftWorkLease)
	redisRenewTheftWork       = redis.NewScript(luaRenewTheftWork)
	redisCompleteTheftWork    = redis.NewScript(luaCompleteTheftWork)
	redisRescheduleTheftWork  = redis.NewScript(luaRescheduleTheftWork)
)

const luaTheftWorkAddressCheck = `
local address = redis.call('HMGET', KEYS[1],
  'work_id', 'work_type', 'placement_kind', 'bucket_version', 'bucket_id',
  'source_authority_kind', 'canonical_source_authority_id', 'work_ref', 'payload_hash')
if address[1] ~= ARGV[8] or address[2] ~= ARGV[9] or address[3] ~= ARGV[10] or
   address[4] ~= ARGV[11] or address[5] ~= ARGV[12] or address[6] ~= ARGV[13] or
   address[7] ~= ARGV[14] or address[8] ~= ARGV[7] or address[9] ~= ARGV[15] then
  return {'ADDRESS_MISMATCH'}
end
`

// ARGV: holder, boot, token_hash, requested_ttl, min_ttl, max_ttl, work_ref,
// work_id, work_type, placement, bucket_version, bucket_id, source_kind,
// canonical_source, payload_hash, source_proof, deadline_ms.
const luaClaimTheftWork = luaTheftWorkAddressCheck + `
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'NOT_FOUND'}
end
local state = redis.call('HGET', KEYS[1], 'state') or ''
if state ~= 'PENDING' then
  return {'NOT_CLAIMABLE', state}
end
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local lease_expire = tonumber(redis.call('HGET', KEYS[1], 'lease_expire_at_ms') or '0')
if lease_expire > now then
  return {'LEASE_HELD', tostring(lease_expire)}
end
local next_due = tonumber(redis.call('HGET', KEYS[1], 'next_due_at_ms') or '0')
if next_due > now then
  return {'NOT_DUE', tostring(next_due)}
end
local existing_source = redis.call('HGET', KEYS[3], 'source_proof_hash') or ''
if existing_source ~= '' and existing_source ~= ARGV[16] then
  return {'PROOF_MISMATCH'}
end
local deadline = redis.call('HGET', KEYS[3], 'deadline_at_ms') or ''
if deadline == '' then
  deadline = ARGV[17]
elseif tonumber(deadline) ~= tonumber(ARGV[17]) then
  return {'DEADLINE_MISMATCH'}
end
local ttl = tonumber(ARGV[4])
local min_ttl = tonumber(ARGV[5])
local max_ttl = tonumber(ARGV[6])
if ttl < min_ttl then ttl = min_ttl end
if ttl > max_ttl then ttl = max_ttl end
local epoch = tonumber(redis.call('HGET', KEYS[1], 'lease_epoch') or '0') + 1
local expire = now + ttl
redis.call('HSET', KEYS[3],
  'source_proof_hash', ARGV[16], 'deadline_at_ms', deadline,
  'holder_boot_id', ARGV[2], 'state', 'PENDING')
redis.call('HSET', KEYS[1],
  'lease_holder_id', ARGV[1], 'lease_epoch', tostring(epoch),
  'lease_token_hash', ARGV[3], 'lease_expire_at_ms', tostring(expire),
  'next_due_at_ms', tostring(expire), 'updated_at_ms', tostring(now))
redis.call('ZADD', KEYS[2], tostring(expire), ARGV[7])
local retry = redis.call('HGET', KEYS[1], 'retry_count') or '0'
local result_proof = redis.call('HGET', KEYS[3], 'result_proof_hash') or ''
local last_error = redis.call('HGET', KEYS[3], 'last_error_class') or ''
return {'CLAIMED', tostring(epoch), redis.call('HGET', KEYS[1], 'work_version') or '0',
  tostring(expire), tostring(ttl), ARGV[15], ARGV[16], result_proof, retry, deadline,
  tostring(expire), last_error, ARGV[1], ARGV[2]}
`

// ARGV: holder, boot, epoch, token_hash, work_version, work_ref, work_id,
// work_type, placement, bucket_version, bucket_id, source_kind,
// canonical_source, payload_hash, typed_payload, source_proof, deadline_ms.
const luaVerifyTheftWorkLease = `
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'NOT_FOUND'}
end
local v = redis.call('HMGET', KEYS[1], 'state', 'lease_holder_id', 'lease_epoch',
  'lease_token_hash', 'lease_expire_at_ms', 'work_version', 'work_ref', 'work_id',
  'work_type', 'placement_kind', 'bucket_version', 'bucket_id',
  'source_authority_kind', 'canonical_source_authority_id', 'payload_hash', 'typed_payload')
if v[8] ~= ARGV[7] or v[9] ~= ARGV[8] or v[10] ~= ARGV[9] or
   v[11] ~= ARGV[10] or v[12] ~= ARGV[11] or v[13] ~= ARGV[12] or
   v[14] ~= ARGV[13] or v[15] ~= ARGV[14] or v[16] ~= ARGV[15] or v[7] ~= ARGV[6] then
  return {'ADDRESS_MISMATCH'}
end
if v[1] ~= 'PENDING' or v[2] ~= ARGV[1] or v[3] ~= ARGV[3] or
   v[4] ~= ARGV[4] or v[6] ~= ARGV[5] then
  return {'FENCE_MISMATCH'}
end
local recovery = redis.call('HMGET', KEYS[3], 'holder_boot_id', 'source_proof_hash', 'deadline_at_ms', 'state')
if recovery[1] ~= ARGV[2] or recovery[2] ~= ARGV[16] or recovery[3] ~= ARGV[17] or recovery[4] ~= 'PENDING' then
  return {'FENCE_MISMATCH'}
end
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local expire = tonumber(v[5] or '0')
if expire <= now then
  return {'LEASE_EXPIRED'}
end
local due = redis.call('ZSCORE', KEYS[2], ARGV[6])
if not due or tonumber(due) ~= expire then
  return {'DUE_INDEX_MISMATCH'}
end
return {'VERIFIED', v[6], v[3], tostring(expire), tostring(math.floor(tonumber(due))), tostring(now)}
`

const luaTheftRenewAddressCheck = `
local address = redis.call('HMGET', KEYS[1],
  'work_id', 'work_type', 'placement_kind', 'bucket_version', 'bucket_id',
  'source_authority_kind', 'canonical_source_authority_id', 'work_ref', 'payload_hash')
if address[1] ~= ARGV[10] or address[2] ~= ARGV[11] or address[3] ~= ARGV[12] or
   address[4] ~= ARGV[13] or address[5] ~= ARGV[14] or address[6] ~= ARGV[15] or
   address[7] ~= ARGV[16] or address[8] ~= ARGV[9] or address[9] ~= ARGV[17] then
  return {'ADDRESS_MISMATCH'}
end
`

// ARGV: holder, boot, epoch, token_hash, work_version, requested_ttl,
// min_ttl, max_ttl, work_ref, work_id, work_type, placement, bucket_version,
// bucket_id, source_kind, canonical_source, payload_hash, source_proof.
const luaRenewTheftWork = luaTheftRenewAddressCheck + `
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'NOT_FOUND'}
end
local v = redis.call('HMGET', KEYS[1], 'state', 'lease_holder_id', 'lease_epoch',
  'lease_token_hash', 'lease_expire_at_ms', 'work_version', 'payload_hash')
if v[1] ~= 'PENDING' then
  return {'NOT_FOUND'}
end
if v[2] ~= ARGV[1] or v[3] ~= ARGV[3] or v[4] ~= ARGV[4] or v[6] ~= ARGV[5] or
   v[7] ~= ARGV[17] then
  return {'FENCE_MISMATCH'}
end
local source = redis.call('HGET', KEYS[3], 'source_proof_hash') or ''
if source ~= ARGV[18] then
  return {'PROOF_MISMATCH'}
end
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
if tonumber(v[5]) <= now then
  return {'LEASE_EXPIRED'}
end
local ttl = tonumber(ARGV[6])
local min_ttl = tonumber(ARGV[7])
local max_ttl = tonumber(ARGV[8])
if ttl < min_ttl then ttl = min_ttl end
if ttl > max_ttl then ttl = max_ttl end
local expire = now + ttl
redis.call('HSET', KEYS[1], 'lease_expire_at_ms', tostring(expire),
  'next_due_at_ms', tostring(expire), 'updated_at_ms', tostring(now))
redis.call('ZADD', KEYS[2], tostring(expire), ARGV[9])
redis.call('HSET', KEYS[3], 'holder_boot_id', ARGV[2])
return {'RENEWED', tostring(expire), tostring(ttl), v[6]}
`

// ARGV: holder, boot, epoch, token_hash, work_version, work_ref, payload_hash,
// source_proof, result_proof, work_id, work_type, typed_payload.
const luaCompleteTheftWork = `
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'NOT_FOUND'}
end
local v = redis.call('HMGET', KEYS[1], 'state', 'lease_holder_id', 'lease_epoch',
  'lease_token_hash', 'work_version', 'payload_hash', 'work_id', 'work_type', 'typed_payload', 'work_ref')
if v[6] ~= ARGV[7] or v[7] ~= ARGV[10] or v[8] ~= ARGV[11] or v[9] ~= ARGV[12] or v[10] ~= ARGV[6] then
  return {'ADDRESS_MISMATCH'}
end
if v[1] == 'COMPLETED' then
  return {'ALREADY_COMPLETED'}
end
if v[1] ~= 'PENDING' then
  return {'FENCE_MISMATCH'}
end
if v[2] ~= ARGV[1] or v[3] ~= ARGV[3] or v[4] ~= ARGV[4] then
  return {'FENCE_MISMATCH'}
end
if v[5] ~= ARGV[5] or v[6] ~= ARGV[7] then
  return {'STALE_VERSION'}
end
local source = redis.call('HGET', KEYS[3], 'source_proof_hash') or ''
if source ~= ARGV[8] then
  return {'PROOF_MISMATCH'}
end
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local expire = tonumber(redis.call('HGET', KEYS[1], 'lease_expire_at_ms') or '0')
if expire <= now then
  return {'LEASE_EXPIRED'}
end
local charged = 512 + 16 + #v[7] + 16 + #v[8] + 16 + #v[9] + 16 + #ARGV[6] +
  16 + #v[6] + 16 + 1
local pending_count = tonumber(redis.call('HGET', KEYS[4], 'pending_work_count') or '')
local pending_bytes = tonumber(redis.call('HGET', KEYS[4], 'pending_work_bytes') or '')
if not pending_count or not pending_bytes or pending_count < 1 or pending_bytes < charged then
  return {'COUNTER_RECONCILE'}
end
redis.call('HSET', KEYS[1], 'state', 'COMPLETED', 'completed_at_ms', tostring(now),
  'updated_at_ms', tostring(now), 'lease_expire_at_ms', '0')
redis.call('ZREM', KEYS[2], ARGV[6])
redis.call('HINCRBY', KEYS[4], 'pending_work_count', -1)
redis.call('HINCRBY', KEYS[4], 'pending_work_bytes', -charged)
redis.call('HSET', KEYS[3], 'result_proof_hash', ARGV[9], 'state', 'COMPLETED',
  'holder_boot_id', ARGV[2], 'last_error_class', '')
return {'COMPLETED'}
`

// ARGV: holder, boot, epoch, token_hash, work_version, work_ref, payload,
// source_proof, result_proof, retry_count, deadline_ms, next_due_ms,
// error_class, terminal_state, work_id, work_type, typed_payload.
const luaRescheduleTheftWork = `
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {'NOT_FOUND'}
end
local v = redis.call('HMGET', KEYS[1], 'state', 'lease_holder_id', 'lease_epoch',
  'lease_token_hash', 'work_version', 'payload_hash', 'retry_count', 'work_id', 'work_type', 'typed_payload', 'work_ref')
if v[6] ~= ARGV[7] or v[8] ~= ARGV[15] or v[9] ~= ARGV[16] or v[10] ~= ARGV[17] or v[11] ~= ARGV[6] then
  return {'ADDRESS_MISMATCH'}
end
if v[1] == 'RECONCILE_REQUIRED' then
  return {'RECONCILE_REQUIRED'}
end
if v[1] ~= 'PENDING' then
  return {'FENCE_MISMATCH'}
end
if v[2] ~= ARGV[1] or v[3] ~= ARGV[3] or v[4] ~= ARGV[4] or v[5] ~= ARGV[5] or
   v[6] ~= ARGV[7] then
  return {'FENCE_MISMATCH'}
end
local source = redis.call('HGET', KEYS[3], 'source_proof_hash') or ''
if source ~= ARGV[8] then
  return {'PROOF_MISMATCH'}
end
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local expire = tonumber(redis.call('HGET', KEYS[1], 'lease_expire_at_ms') or '0')
if expire <= now then
  return {'LEASE_EXPIRED'}
end
local current_retry = tonumber(v[7] or '0')
local requested_retry = tonumber(ARGV[10])
if requested_retry < current_retry then
  return {'STALE_VERSION'}
end
local terminal = ARGV[14]
if terminal ~= '' then
  local charged = 512 + 16 + #v[8] + 16 + #v[9] + 16 + #v[10] + 16 + #ARGV[6] +
    16 + #v[6] + 16 + 1
  local pending_count = tonumber(redis.call('HGET', KEYS[4], 'pending_work_count') or '')
  local pending_bytes = tonumber(redis.call('HGET', KEYS[4], 'pending_work_bytes') or '')
  if not pending_count or not pending_bytes or pending_count < 1 or pending_bytes < charged then
    return {'COUNTER_RECONCILE'}
  end
  redis.call('HSET', KEYS[1], 'state', terminal, 'retry_count', tostring(requested_retry),
    'next_due_at_ms', '0', 'lease_holder_id', '', 'lease_token_hash', '',
    'lease_expire_at_ms', '0', 'updated_at_ms', tostring(now), 'completed_at_ms', tostring(now))
  redis.call('ZREM', KEYS[2], ARGV[6])
  redis.call('HINCRBY', KEYS[4], 'pending_work_count', -1)
  redis.call('HINCRBY', KEYS[4], 'pending_work_bytes', -charged)
  redis.call('HSET', KEYS[3], 'deadline_at_ms', ARGV[11], 'last_error_class', ARGV[13],
    'result_proof_hash', ARGV[9], 'holder_boot_id', ARGV[2], 'state', terminal)
  return {'RECONCILE_REQUIRED'}
end
redis.call('HSET', KEYS[1], 'state', 'PENDING', 'retry_count', tostring(requested_retry),
  'next_due_at_ms', ARGV[12], 'lease_holder_id', '', 'lease_token_hash', '',
  'lease_expire_at_ms', '0', 'updated_at_ms', tostring(now))
redis.call('ZADD', KEYS[2], ARGV[12], ARGV[6])
redis.call('HSET', KEYS[3], 'deadline_at_ms', ARGV[11], 'last_error_class', ARGV[13],
  'result_proof_hash', ARGV[9], 'holder_boot_id', ARGV[2])
return {'RESCHEDULED'}
`

func (r *RedisWorkLeaseRepository) keysFor(order DispatchOrder) ([]string, error) {
	if r == nil || r.client == nil || r.keys == nil {
		return nil, fmt.Errorf("%w: nil Redis WorkLease repository", ErrInvalidConfig)
	}
	work, err := r.keys.WorkKey(order.Work)
	if err != nil {
		return nil, err
	}
	due, err := r.keys.DueIndexKey(order.Work)
	if err != nil {
		return nil, err
	}
	recovery, err := r.keys.RecoveryKey(order.Work)
	if err != nil {
		return nil, err
	}
	meta, err := r.keys.BucketMetaKey(order.Work)
	if err != nil {
		return nil, err
	}
	if work == "" || due == "" || recovery == "" || meta == "" {
		return nil, fmt.Errorf("%w: empty work lease key", ErrInvalidConfig)
	}
	return []string{work, due, recovery, meta}, nil
}

func (r *RedisWorkLeaseRepository) Claim(ctx context.Context, request ClaimRequest) (LeaseClaim, error) {
	if err := validateLeaseIdentity(request.HolderID, request.BootID, request.LeaseToken); err != nil {
		return LeaseClaim{}, err
	}
	prepared, err := prepareLeaseOrder(request.Order)
	if err != nil {
		return LeaseClaim{}, err
	}
	request.Order = prepared
	keys, err := r.keysFor(request.Order)
	if err != nil {
		return LeaseClaim{}, err
	}
	args := []interface{}{request.HolderID, request.BootID, leaseTokenHash(request.LeaseToken),
		request.RequestedTTL.Milliseconds(), r.minTTL.Milliseconds(), r.maxTTL.Milliseconds(),
		request.Order.WorkRef, request.Order.Work.WorkID, request.Order.Work.WorkType,
		request.Order.Work.PlacementKind, request.Order.Work.BucketVersion,
		strconv.FormatUint(uint64(request.Order.Work.BucketID), 10), request.Order.Work.SourceAuthorityKind,
		request.Order.Work.CanonicalSourceAuthority, HexHash(request.Order.PayloadHash),
		HexHash(SourceProofHash(request.Order)), request.Order.DeadlineAtMs}
	reply, runErr := redisClaimTheftWork.Run(ctx, r.client, keys, args...).Result()
	if runErr != nil {
		return LeaseClaim{Status: claimTransportStatus(runErr)}, classifyLeaseTransport(runErr)
	}
	values, err := redisStrings(reply)
	if err != nil {
		return LeaseClaim{}, err
	}
	if len(values) == 0 {
		return LeaseClaim{}, ErrReceiptUnavailable
	}
	switch values[0] {
	case "CLAIMED":
		if len(values) < 14 {
			return LeaseClaim{}, fmt.Errorf("%w: short claim reply", ErrInvalidDispatch)
		}
		epoch, parseErr := parseUint(values[1])
		if parseErr != nil {
			return LeaseClaim{}, parseErr
		}
		version, parseErr := parseUint(values[2])
		if parseErr != nil {
			return LeaseClaim{}, parseErr
		}
		expire, parseErr := parseInt64(values[3])
		if parseErr != nil {
			return LeaseClaim{}, parseErr
		}
		ttl, parseErr := parseInt64(values[4])
		if parseErr != nil {
			return LeaseClaim{}, parseErr
		}
		retry, parseErr := parseUint(values[8])
		if parseErr != nil {
			return LeaseClaim{}, parseErr
		}
		deadline, parseErr := parseInt64(values[9])
		if parseErr != nil {
			return LeaseClaim{}, parseErr
		}
		nextDue, parseErr := parseInt64(values[10])
		if parseErr != nil {
			return LeaseClaim{}, parseErr
		}
		payloadHash, err := hex.DecodeString(values[5])
		if err != nil {
			return LeaseClaim{}, err
		}
		sourceProof, err := hex.DecodeString(values[6])
		if err != nil {
			return LeaseClaim{}, err
		}
		resultProof, err := decodeOptionalHex(values[7])
		if err != nil {
			return LeaseClaim{}, err
		}
		return LeaseClaim{Status: LeaseClaimed, WorkVersion: version, RetryCount: retry,
			DeadlineAtMs: deadline, NextDueAtMs: nextDue, LastErrorClass: values[11],
			HolderID: values[12], BootID: values[13], LeaseEpoch: epoch,
			LeaseExpireAtMs: expire, AcceptedTTL: time.Duration(ttl) * time.Millisecond,
			PayloadHash: payloadHash, SourceProofHash: sourceProof, ResultProofHash: resultProof}, nil
	case "LEASE_HELD":
		return LeaseClaim{Status: LeaseHeld}, nil
	case "NOT_DUE":
		return LeaseClaim{Status: LeaseNotDue}, nil
	case "NOT_FOUND":
		return LeaseClaim{Status: LeaseNotFound}, nil
	case "NOT_CLAIMABLE":
		return LeaseClaim{Status: LeaseNotClaimable}, nil
	case "ADDRESS_MISMATCH", "PROOF_MISMATCH", "DEADLINE_MISMATCH":
		return LeaseClaim{}, fmt.Errorf("%w: %s", ErrInvalidDispatch, values[0])
	default:
		return LeaseClaim{}, fmt.Errorf("%w: claim status %s", ErrInvalidDispatch, values[0])
	}
}

// VerifyRecoveryLease atomically validates the current Pair source lease and
// its DueIndex score using Redis TIME. It is the dedicated admission proof for
// source-placed DECIDE_THEFT and never consults a destination WorkRecord.
func (r *RedisWorkLeaseRepository) VerifyRecoveryLease(ctx context.Context, request VerifyRecoveryLeaseRequest) (RecoveryLeaseVerification, error) {
	if err := validateLeaseFence(request.Fence); err != nil {
		return RecoveryLeaseVerification{}, err
	}
	prepared, err := prepareLeaseOrder(request.Order)
	if err != nil {
		return RecoveryLeaseVerification{}, err
	}
	if prepared.WorkVersion == 0 || prepared.WorkVersion != request.Fence.WorkVersion {
		return RecoveryLeaseVerification{}, fmt.Errorf("%w: Pair WorkVersion", ErrLeaseLost)
	}
	keys, err := r.keysFor(prepared)
	if err != nil {
		return RecoveryLeaseVerification{}, err
	}
	args := []interface{}{request.Fence.HolderID, request.Fence.BootID, request.Fence.LeaseEpoch,
		leaseTokenHash(request.Fence.LeaseToken), request.Fence.WorkVersion, prepared.WorkRef,
		prepared.Work.WorkID, prepared.Work.WorkType, prepared.Work.PlacementKind,
		prepared.Work.BucketVersion, strconv.FormatUint(uint64(prepared.Work.BucketID), 10),
		prepared.Work.SourceAuthorityKind, prepared.Work.CanonicalSourceAuthority, HexHash(prepared.PayloadHash),
		prepared.Payload, HexHash(SourceProofHash(prepared)), prepared.DeadlineAtMs}
	reply, runErr := redisVerifyTheftWorkLease.Run(ctx, r.client, keys[:3], args...).Result()
	if runErr != nil {
		return RecoveryLeaseVerification{}, classifyLeaseTransport(runErr)
	}
	values, err := redisStrings(reply)
	if err != nil {
		return RecoveryLeaseVerification{}, err
	}
	if len(values) == 0 {
		return RecoveryLeaseVerification{}, ErrReceiptUnavailable
	}
	if values[0] != "VERIFIED" {
		switch values[0] {
		case "NOT_FOUND", "ADDRESS_MISMATCH", "FENCE_MISMATCH", "LEASE_EXPIRED", "DUE_INDEX_MISMATCH":
			return RecoveryLeaseVerification{}, fmt.Errorf("%w: %s", ErrLeaseLost, values[0])
		default:
			return RecoveryLeaseVerification{}, fmt.Errorf("%w: verify status %s", ErrInvalidDispatch, values[0])
		}
	}
	if len(values) != 6 {
		return RecoveryLeaseVerification{}, fmt.Errorf("%w: short verify reply", ErrInvalidDispatch)
	}
	workVersion, err := parseUint(values[1])
	if err != nil {
		return RecoveryLeaseVerification{}, err
	}
	leaseEpoch, err := parseUint(values[2])
	if err != nil {
		return RecoveryLeaseVerification{}, err
	}
	expireAtMs, err := parseInt64(values[3])
	if err != nil {
		return RecoveryLeaseVerification{}, err
	}
	dueScoreMs, err := parseInt64(values[4])
	if err != nil {
		return RecoveryLeaseVerification{}, err
	}
	nowMs, err := parseInt64(values[5])
	if err != nil {
		return RecoveryLeaseVerification{}, err
	}
	return RecoveryLeaseVerification{WorkVersion: workVersion, LeaseEpoch: leaseEpoch,
		LeaseExpireAtMs: expireAtMs, DueIndexScoreMs: dueScoreMs, TrustedNowMs: nowMs}, nil
}

func (r *RedisWorkLeaseRepository) Renew(ctx context.Context, request RenewRequest) (LeaseRenewResult, error) {
	if err := validateLeaseFence(request.Fence); err != nil {
		return LeaseRenewResult{}, err
	}
	prepared, err := prepareLeaseOrder(request.Order)
	if err != nil {
		return LeaseRenewResult{}, err
	}
	request.Order = prepared
	keys, err := r.keysFor(request.Order)
	if err != nil {
		return LeaseRenewResult{}, err
	}
	args := []interface{}{request.Fence.HolderID, request.Fence.BootID, request.Fence.LeaseEpoch,
		leaseTokenHash(request.Fence.LeaseToken), request.Fence.WorkVersion, request.RequestedTTL.Milliseconds(),
		r.minTTL.Milliseconds(), r.maxTTL.Milliseconds(), request.Order.WorkRef,
		request.Order.Work.WorkID, request.Order.Work.WorkType, request.Order.Work.PlacementKind,
		request.Order.Work.BucketVersion, strconv.FormatUint(uint64(request.Order.Work.BucketID), 10),
		request.Order.Work.SourceAuthorityKind, request.Order.Work.CanonicalSourceAuthority,
		HexHash(request.PayloadHash), HexHash(request.SourceProofHash)}
	reply, runErr := redisRenewTheftWork.Run(ctx, r.client, keys, args...).Result()
	if runErr != nil {
		return LeaseRenewResult{Status: renewTransportStatus(runErr)}, classifyLeaseTransport(runErr)
	}
	values, err := redisStrings(reply)
	if err != nil {
		return LeaseRenewResult{}, err
	}
	if len(values) == 0 {
		return LeaseRenewResult{}, ErrReceiptUnavailable
	}
	switch values[0] {
	case "RENEWED":
		if len(values) < 4 {
			return LeaseRenewResult{}, fmt.Errorf("%w: short renew reply", ErrInvalidDispatch)
		}
		expire, err := parseInt64(values[1])
		if err != nil {
			return LeaseRenewResult{}, err
		}
		ttl, err := parseInt64(values[2])
		if err != nil {
			return LeaseRenewResult{}, err
		}
		version, err := parseUint(values[3])
		if err != nil {
			return LeaseRenewResult{}, err
		}
		return LeaseRenewResult{Status: LeaseRenewed, LeaseExpireAtMs: expire,
			AcceptedTTL: time.Duration(ttl) * time.Millisecond, WorkVersion: version}, nil
	case "FENCE_MISMATCH", "PROOF_MISMATCH":
		return LeaseRenewResult{Status: LeaseRenewFenceMismatch}, nil
	case "LEASE_EXPIRED":
		return LeaseRenewResult{Status: LeaseRenewExpired}, nil
	case "NOT_FOUND":
		return LeaseRenewResult{Status: LeaseRenewNotFound}, nil
	default:
		return LeaseRenewResult{}, fmt.Errorf("%w: renew status %s", ErrInvalidDispatch, values[0])
	}
}

func (r *RedisWorkLeaseRepository) Complete(ctx context.Context, request CompleteRequest) (LeaseCompleteResult, error) {
	if err := validateLeaseFence(request.Fence); err != nil {
		return LeaseCompleteResult{}, err
	}
	prepared, err := prepareLeaseOrder(request.Order)
	if err != nil {
		return LeaseCompleteResult{}, err
	}
	request.Order = prepared
	if len(request.Proof.PayloadHash) != sha256.Size || len(request.Proof.SourceProofHash) != sha256.Size || len(request.Proof.ResultProofHash) != sha256.Size {
		return LeaseCompleteResult{}, ErrResultProofMissing
	}
	keys, err := r.keysFor(request.Order)
	if err != nil {
		return LeaseCompleteResult{}, err
	}
	args := []interface{}{request.Fence.HolderID, request.Fence.BootID, request.Fence.LeaseEpoch,
		leaseTokenHash(request.Fence.LeaseToken), request.Fence.WorkVersion, request.Order.WorkRef,
		HexHash(request.Proof.PayloadHash), HexHash(request.Proof.SourceProofHash), HexHash(request.Proof.ResultProofHash),
		request.Order.Work.WorkID, request.Order.Work.WorkType, request.Order.Payload}
	reply, runErr := redisCompleteTheftWork.Run(ctx, r.client, keys, args...).Result()
	if runErr != nil {
		return LeaseCompleteResult{Status: completeTransportStatus(runErr)}, classifyLeaseTransport(runErr)
	}
	values, err := redisStrings(reply)
	if err != nil {
		return LeaseCompleteResult{}, err
	}
	if len(values) == 0 {
		return LeaseCompleteResult{}, ErrReceiptUnavailable
	}
	switch values[0] {
	case "COMPLETED":
		return LeaseCompleteResult{Status: LeaseCompleted}, nil
	case "ALREADY_COMPLETED":
		return LeaseCompleteResult{Status: LeaseAlreadyCompleted}, nil
	case "FENCE_MISMATCH", "PROOF_MISMATCH":
		return LeaseCompleteResult{Status: LeaseCompleteFenceMismatch}, nil
	case "STALE_VERSION":
		return LeaseCompleteResult{Status: LeaseCompleteStaleVersion}, nil
	case "LEASE_EXPIRED":
		return LeaseCompleteResult{Status: LeaseCompleteFenceMismatch}, nil
	case "NOT_FOUND":
		return LeaseCompleteResult{Status: LeaseCompleteNotFound}, nil
	default:
		return LeaseCompleteResult{}, fmt.Errorf("%w: complete status %s", ErrInvalidDispatch, values[0])
	}
}

func (r *RedisWorkLeaseRepository) Reschedule(ctx context.Context, request RescheduleRequest) (LeaseRescheduleResult, error) {
	if err := validateLeaseFence(request.Fence); err != nil {
		return LeaseRescheduleResult{}, err
	}
	if request.DeadlineAtMs <= 0 || request.NextDueAtMs < 0 || request.NextDueAtMs > request.DeadlineAtMs ||
		(request.TerminalState != "" && request.TerminalState != ReconcileRequiredState) {
		return LeaseRescheduleResult{}, fmt.Errorf("%w: reschedule bounds", ErrInvalidDispatch)
	}
	prepared, err := prepareLeaseOrder(request.Order)
	if err != nil {
		return LeaseRescheduleResult{}, err
	}
	request.Order = prepared
	keys, err := r.keysFor(request.Order)
	if err != nil {
		return LeaseRescheduleResult{}, err
	}
	args := []interface{}{request.Fence.HolderID, request.Fence.BootID, request.Fence.LeaseEpoch,
		leaseTokenHash(request.Fence.LeaseToken), request.Fence.WorkVersion, request.Order.WorkRef,
		HexHash(request.Proof.PayloadHash), HexHash(request.Proof.SourceProofHash), HexHash(request.Proof.ResultProofHash),
		request.RetryCount, request.DeadlineAtMs, request.NextDueAtMs, request.ErrorClass, request.TerminalState,
		request.Order.Work.WorkID, request.Order.Work.WorkType, request.Order.Payload}
	reply, runErr := redisRescheduleTheftWork.Run(ctx, r.client, keys, args...).Result()
	if runErr != nil {
		return LeaseRescheduleResult{Status: rescheduleTransportStatus(runErr)}, classifyLeaseTransport(runErr)
	}
	values, err := redisStrings(reply)
	if err != nil {
		return LeaseRescheduleResult{}, err
	}
	if len(values) == 0 {
		return LeaseRescheduleResult{}, ErrReceiptUnavailable
	}
	switch values[0] {
	case "RESCHEDULED":
		return LeaseRescheduleResult{Status: LeaseRescheduled}, nil
	case "RECONCILE_REQUIRED":
		return LeaseRescheduleResult{Status: LeaseReconcileRequired}, nil
	case "FENCE_MISMATCH", "PROOF_MISMATCH":
		return LeaseRescheduleResult{Status: LeaseRescheduleFenceMismatch}, nil
	case "STALE_VERSION":
		return LeaseRescheduleResult{Status: LeaseRescheduleStaleVersion}, nil
	case "LEASE_EXPIRED":
		return LeaseRescheduleResult{Status: LeaseRescheduleExpired}, nil
	case "NOT_FOUND":
		return LeaseRescheduleResult{Status: LeaseRescheduleNotFound}, nil
	default:
		return LeaseRescheduleResult{}, fmt.Errorf("%w: reschedule status %s", ErrInvalidDispatch, values[0])
	}
}

func prepareLeaseOrder(order DispatchOrder) (DispatchOrder, error) {
	if err := validateDispatchOrderShape(order); err != nil {
		return DispatchOrder{}, err
	}
	if order.WorkRef == "" {
		ref, err := workidentityForLeaseRef(order)
		if err != nil {
			return DispatchOrder{}, err
		}
		order.WorkRef = ref
	}
	ref, err := workidentity.DecodeWorkRefV1(order.WorkRef)
	if err != nil || ref.WorkType != order.Work.WorkType || ref.WorkID != order.Work.WorkID {
		return DispatchOrder{}, fmt.Errorf("%w: work ref binding", ErrInvalidDispatch)
	}
	if order.DeadlineAtMs <= 0 {
		return DispatchOrder{}, fmt.Errorf("%w: deadline required", ErrInvalidDispatch)
	}
	return order, nil
}

func validateLeaseIdentity(holderID, bootID, leaseToken string) error {
	if strings.TrimSpace(holderID) == "" || strings.TrimSpace(bootID) == "" || strings.TrimSpace(leaseToken) == "" {
		return fmt.Errorf("%w: holder/boot/token", ErrInvalidConfig)
	}
	return nil
}

func validateLeaseFence(fence LeaseFence) error {
	if err := validateLeaseIdentity(fence.HolderID, fence.BootID, fence.LeaseToken); err != nil {
		return err
	}
	if fence.LeaseEpoch == 0 || fence.WorkVersion == 0 {
		return fmt.Errorf("%w: lease epoch/version", ErrInvalidDispatch)
	}
	return nil
}

func workidentityForLeaseRef(order DispatchOrder) (string, error) {
	ref, err := encodeWorkRefForLease(order.Work.WorkType, order.Work.WorkID)
	if err != nil {
		return "", fmt.Errorf("%w: work ref: %v", ErrInvalidDispatch, err)
	}
	return ref, nil
}

// Small wrappers keep redislease.go independent from the exported naming of
// the shared identity package in the core file and make malformed replies
// fail closed before they reach the worker state machine.
var encodeWorkRefForLease = func(workType, workID string) (string, error) {
	return workidentity.EncodeWorkRefV1(workType, workID)
}

func leaseTokenHash(token string) string {
	h := sha256.New()
	_, _ = h.Write([]byte("lease-token"))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(token))
	return hex.EncodeToString(h.Sum(nil))
}

func decodeOptionalHex(value string) ([]byte, error) {
	if value == "" {
		return nil, nil
	}
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return nil, err
	}
	return decoded, nil
}

func parseUint(value string) (uint64, error) {
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: uint %q", ErrInvalidDispatch, value)
	}
	return parsed, nil
}

func parseInt64(value string) (int64, error) {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: int %q", ErrInvalidDispatch, value)
	}
	return parsed, nil
}

func redisStrings(value interface{}) ([]string, error) {
	items, ok := value.([]interface{})
	if !ok {
		return nil, fmt.Errorf("%w: Redis reply type %T", ErrInvalidDispatch, value)
	}
	result := make([]string, len(items))
	for i, item := range items {
		switch typed := item.(type) {
		case string:
			result[i] = typed
		case []byte:
			result[i] = string(typed)
		case nil:
			result[i] = ""
		default:
			result[i] = fmt.Sprint(typed)
		}
	}
	return result, nil
}

func classifyLeaseTransport(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return ErrReceiptUnavailable
	}
	return err
}

func claimTransportStatus(err error) LeaseClaimStatus {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return LeaseTimeoutUnknown
	}
	return LeaseUnavailableNotSent
}

func renewTransportStatus(err error) LeaseRenewStatus {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return LeaseRenewTimeoutUnknown
	}
	return LeaseRenewUnavailableNotSent
}

func completeTransportStatus(err error) LeaseCompleteStatus {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return LeaseCompleteTimeoutUnknown
	}
	return LeaseCompleteUnavailableNotSent
}

func rescheduleTransportStatus(err error) LeaseRescheduleStatus {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return LeaseRescheduleTimeoutUnknown
	}
	return LeaseRescheduleUnavailableNotSent
}
