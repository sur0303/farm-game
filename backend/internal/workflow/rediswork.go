package workflow

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"farmgame/internal/sourceordermetrics"
	"farmgame/internal/workidentity"

	"github.com/redis/go-redis/v9"
)

// This file implements the Redis-backed WorkRepository for the generic work
// family. It lives in the workflow package's own key/Lua family and does not
// touch the authority adapter scripts (internal/storage/redis/authority).
// Lease and due adjudication use Redis TIME (repository-trusted time);
// callers pass requested TTLs only and receive accepted TTLs back.

const luaCreateWork = `
local t = redis.call('TIME')
local now_ms = t[1] * 1000 + math.floor(t[2] / 1000)
if redis.call('EXISTS', KEYS[1]) == 1 then
  return {0}
end
redis.call('HSET', KEYS[1],
  'data', ARGV[1],
  'work_type', ARGV[4],
  'work_version', 1,
  'lease_owner', '',
  'lease_epoch', 0,
  'lease_token_hash', '',
  'lease_expire_at_ms', 0,
  'next_due_at_ms', ARGV[2],
  'terminal', 0,
  'updated_at_ms', now_ms,
  'completed_at_ms', 0)
redis.call('ZADD', KEYS[2], ARGV[2], ARGV[3])
return {1}
`

const luaAdvanceWork = `
local t = redis.call('TIME')
local now_ms = t[1] * 1000 + math.floor(t[2] / 1000)
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {-1}
end
local version = tonumber(redis.call('HGET', KEYS[1], 'work_version') or '0')
if version ~= tonumber(ARGV[1]) then
  return {-2}
end
if (redis.call('HGET', KEYS[1], 'terminal') or '0') == '1' then
  return {-4}
end
local owner = redis.call('HGET', KEYS[1], 'lease_owner') or ''
local epoch = tonumber(redis.call('HGET', KEYS[1], 'lease_epoch') or '0')
local expire = tonumber(redis.call('HGET', KEYS[1], 'lease_expire_at_ms') or '0')
if ARGV[2] ~= '' then
  if owner ~= ARGV[2] or epoch ~= tonumber(ARGV[3]) or expire <= now_ms then
    return {-3}
  end
elseif owner ~= '' and expire > now_ms then
  return {-3}
end
local new_version = version + 1
local terminal = tonumber(ARGV[7])
local release = tonumber(ARGV[4])
local next_due = tonumber(ARGV[6])
local completed_at = 0
local token_hash = redis.call('HGET', KEYS[1], 'lease_token_hash') or ''
if terminal == 1 then
  owner = ''
  token_hash = ''
  expire = 0
  next_due = 0
  completed_at = now_ms
  redis.call('ZREM', KEYS[2], ARGV[8])
elseif release == 1 then
  owner = ''
  token_hash = ''
  expire = 0
  redis.call('ZADD', KEYS[2], next_due, ARGV[8])
else
  redis.call('ZADD', KEYS[2], next_due, ARGV[8])
end
redis.call('HSET', KEYS[1],
  'data', ARGV[5],
  'work_version', new_version,
  'lease_owner', owner,
  'lease_token_hash', token_hash,
  'lease_expire_at_ms', expire,
  'next_due_at_ms', next_due,
  'terminal', terminal,
  'updated_at_ms', now_ms,
  'completed_at_ms', completed_at)
return {1, new_version}
`

const luaClaimDueWork = `
local t = redis.call('TIME')
local now_ms = t[1] * 1000 + math.floor(t[2] / 1000)
local refs = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now_ms, 'LIMIT', 0, tonumber(ARGV[1]))
local expire_at = now_ms + tonumber(ARGV[4])
local result = {}
for _, ref in ipairs(refs) do
  local key = ARGV[5] .. ref
  if redis.call('EXISTS', key) == 0 then
    redis.call('ZREM', KEYS[1], ref)
  elseif (redis.call('HGET', key, 'terminal') or '0') == '1' then
    redis.call('ZREM', KEYS[1], ref)
  else
    local next_due = tonumber(redis.call('HGET', key, 'next_due_at_ms') or '0')
    local lease_expire = tonumber(redis.call('HGET', key, 'lease_expire_at_ms') or '0')
    if next_due <= now_ms and lease_expire <= now_ms then
      local version = tonumber(redis.call('HGET', key, 'work_version') or '0') + 1
      local epoch = tonumber(redis.call('HGET', key, 'lease_epoch') or '0') + 1
      redis.call('HSET', key,
        'work_version', version,
        'lease_owner', ARGV[2],
        'lease_epoch', epoch,
        'lease_token_hash', ARGV[3],
        'lease_expire_at_ms', expire_at,
        'next_due_at_ms', expire_at,
        'updated_at_ms', now_ms)
      redis.call('ZADD', KEYS[1], expire_at, ref)
      table.insert(result, ref)
      table.insert(result, redis.call('HGET', key, 'data'))
      table.insert(result, tostring(version))
      table.insert(result, tostring(epoch))
      table.insert(result, tostring(expire_at))
    end
  end
end
return result
`

// luaClaimDueWorkByTypes filters before acquiring a lease. The due index is
// mutable, so the loop advances its rank only past entries it leaves in
// place; entries removed or rescheduled do not advance the offset. This also
// backfills work_type for records written before that hash field existed.
const luaClaimDueWorkByTypes = `
local t = redis.call('TIME')
local now_ms = t[1] * 1000 + math.floor(t[2] / 1000)
local limit = tonumber(ARGV[1])
local expire_at = now_ms + tonumber(ARGV[4])
local allowed = {}
for i = 7, #ARGV do
  allowed[ARGV[i]] = true
end
local result = {}
local offset = 0
local page_size = tonumber(ARGV[6])
while (#result / 5) < limit do
  local refs = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now_ms, 'LIMIT', offset, page_size)
  if #refs == 0 then
    break
  end
  for _, ref in ipairs(refs) do
    if (#result / 5) >= limit then
      break
    end
    local key = ARGV[5] .. ref
    if redis.call('EXISTS', key) == 0 then
      redis.call('ZREM', KEYS[1], ref)
    elseif (redis.call('HGET', key, 'terminal') or '0') == '1' then
      redis.call('ZREM', KEYS[1], ref)
    else
      local work_type = redis.call('HGET', key, 'work_type') or ''
      if work_type == '' then
        local ok, record = pcall(cjson.decode, redis.call('HGET', key, 'data') or '')
        if ok and record['address'] and record['address']['WorkType'] then
          work_type = record['address']['WorkType']
          redis.call('HSET', key, 'work_type', work_type)
        end
      end
      if allowed[work_type] then
        local next_due = tonumber(redis.call('HGET', key, 'next_due_at_ms') or '0')
        local lease_expire = tonumber(redis.call('HGET', key, 'lease_expire_at_ms') or '0')
        if next_due <= now_ms and lease_expire <= now_ms then
          local version = tonumber(redis.call('HGET', key, 'work_version') or '0') + 1
          local epoch = tonumber(redis.call('HGET', key, 'lease_epoch') or '0') + 1
          redis.call('HSET', key,
            'work_version', version,
            'lease_owner', ARGV[2],
            'lease_epoch', epoch,
            'lease_token_hash', ARGV[3],
            'lease_expire_at_ms', expire_at,
            'next_due_at_ms', expire_at,
            'updated_at_ms', now_ms)
          redis.call('ZADD', KEYS[1], expire_at, ref)
          table.insert(result, ref)
          table.insert(result, redis.call('HGET', key, 'data'))
          table.insert(result, tostring(version))
          table.insert(result, tostring(epoch))
          table.insert(result, tostring(expire_at))
        else
          offset = offset + 1
        end
      else
        offset = offset + 1
      end
    end
  end
  if #refs < page_size then
    break
  end
end
return result
`

const luaClaimSpecificWork = `
local t = redis.call('TIME')
local now_ms = t[1] * 1000 + math.floor(t[2] / 1000)
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {-1}
end
if (redis.call('HGET', KEYS[1], 'terminal') or '0') == '1' then
  return {-4}
end
local next_due = tonumber(redis.call('HGET', KEYS[1], 'next_due_at_ms') or '0')
local lease_expire = tonumber(redis.call('HGET', KEYS[1], 'lease_expire_at_ms') or '0')
if lease_expire > now_ms then
  return {-3}
end
if next_due > now_ms then
  return {-5}
end
local version = tonumber(redis.call('HGET', KEYS[1], 'work_version') or '0') + 1
local epoch = tonumber(redis.call('HGET', KEYS[1], 'lease_epoch') or '0') + 1
local expire = now_ms + tonumber(ARGV[4])
redis.call('HSET', KEYS[1],
  'work_version', version, 'lease_owner', ARGV[1], 'lease_epoch', epoch,
  'lease_token_hash', ARGV[3], 'lease_expire_at_ms', expire,
  'next_due_at_ms', expire, 'updated_at_ms', now_ms)
redis.call('ZADD', KEYS[2], expire, ARGV[5])
return {1, redis.call('HGET', KEYS[1], 'data'), version, epoch, expire}
`

const luaRenewWorkLease = `
local t = redis.call('TIME')
local now_ms = t[1] * 1000 + math.floor(t[2] / 1000)
if redis.call('EXISTS', KEYS[1]) == 0 then
  return {-1}
end
if (redis.call('HGET', KEYS[1], 'terminal') or '0') == '1' then
  return {-4}
end
local owner = redis.call('HGET', KEYS[1], 'lease_owner') or ''
local epoch = tonumber(redis.call('HGET', KEYS[1], 'lease_epoch') or '0')
local token = redis.call('HGET', KEYS[1], 'lease_token_hash') or ''
local expire = tonumber(redis.call('HGET', KEYS[1], 'lease_expire_at_ms') or '0')
if owner ~= ARGV[1] or epoch ~= tonumber(ARGV[2]) or token ~= ARGV[3] or expire <= now_ms then
  return {-3}
end
local new_expire = now_ms + tonumber(ARGV[4])
redis.call('HSET', KEYS[1], 'lease_expire_at_ms', new_expire, 'next_due_at_ms', new_expire, 'updated_at_ms', now_ms)
redis.call('ZADD', KEYS[2], new_expire, ARGV[5])
return {1, new_expire}
`

const luaPendingDueWork = `
local t = redis.call('TIME')
local now_ms = t[1] * 1000 + math.floor(t[2] / 1000)
return redis.call('ZCOUNT', KEYS[1], '-inf', now_ms)
`

// RedisWorkRepository is the durable Redis implementation of WorkRepository.
// One instance owns one hash-tag family; every script touches keys of a
// single slot.
type RedisWorkRepository struct {
	client redis.UniversalClient
	prefix string
}

func NewRedisWorkRepository(client redis.UniversalClient, prefix string) (*RedisWorkRepository, error) {
	prefix = strings.TrimSuffix(strings.TrimSpace(prefix), ":")
	if client == nil || prefix == "" {
		return nil, errors.New("workflow: redis client and prefix required")
	}
	return &RedisWorkRepository{client: client, prefix: prefix}, nil
}

func (s *RedisWorkRepository) keys() WorkKeys  { return WorkKeys{prefix: s.prefix} }
func (s *RedisWorkRepository) keyBase() string { return s.keys().base() }
func (s *RedisWorkRepository) dueKey() string  { return s.keys().Due() }

func (s *RedisWorkRepository) workRef(workType, workID string) (string, error) {
	ref, err := workidentity.EncodeWorkRefV1(workType, workID)
	if err != nil {
		return "", fmt.Errorf("%w: work ref", ErrInvalid)
	}
	return ref, nil
}

func (s *RedisWorkRepository) recordKey(workType, workID string) (string, error) {
	ref, err := s.workRef(workType, workID)
	if err != nil {
		return "", err
	}
	return s.keyBase() + ":record:" + ref, nil
}

func (s *RedisWorkRepository) recordKeyPrefix() string { return s.keyBase() + ":record:" }

func (s *RedisWorkRepository) CreateIfAbsent(ctx context.Context, candidate *WorkRecord) (*WorkRecord, bool, error) {
	if err := candidate.Validate(); err != nil {
		return nil, false, err
	}
	key, err := s.recordKey(candidate.Address.WorkType, candidate.Address.WorkID)
	if err != nil {
		return nil, false, err
	}
	ref, err := s.workRef(candidate.Address.WorkType, candidate.Address.WorkID)
	if err != nil {
		return nil, false, err
	}
	created := cloneWorkRecord(candidate)
	created.WorkVersion = 1
	created.LeaseOwner = ""
	created.LeaseEpoch = 0
	created.LeaseTokenHash = ""
	created.LeaseExpireAtMs = 0
	data, err := encodeWorkRecord(created)
	if err != nil {
		return nil, false, err
	}
	reply, err := s.client.Eval(ctx, luaCreateWork, []string{key, s.dueKey()},
		data, created.NextDueAtMs, ref, created.Address.WorkType).Slice()
	if err != nil {
		return nil, false, err
	}
	if len(reply) == 1 && stringValue(reply[0]) == "1" {
		return created, true, nil
	}
	existing, err := s.Get(ctx, candidate.Address.WorkType, candidate.Address.WorkID)
	if err != nil {
		return nil, false, err
	}
	if !sameWorkIntent(existing, candidate) {
		return nil, false, ErrIdempotencyConflict
	}
	return existing, false, nil
}

func (s *RedisWorkRepository) Get(ctx context.Context, workType, workID string) (*WorkRecord, error) {
	key, err := s.recordKey(workType, workID)
	if err != nil {
		return nil, err
	}
	values, err := s.client.HMGet(ctx, key,
		"data", "work_version", "lease_owner", "lease_epoch", "lease_token_hash",
		"lease_expire_at_ms", "next_due_at_ms", "updated_at_ms", "completed_at_ms").Result()
	if err != nil {
		return nil, err
	}
	if len(values) != 9 || values[0] == nil {
		return nil, ErrNotFound
	}
	return decodeStoredWork(values)
}

func decodeStoredWork(values []interface{}) (*WorkRecord, error) {
	data, ok := values[0].(string)
	if !ok {
		return nil, fmt.Errorf("workflow: invalid stored record %T", values[0])
	}
	record, err := decodeWorkRecord([]byte(data))
	if err != nil {
		return nil, err
	}
	if record.WorkVersion, err = uintValue(values[1]); err != nil {
		return nil, err
	}
	record.LeaseOwner = stringValue(values[2])
	if record.LeaseEpoch, err = uintValue(values[3]); err != nil {
		return nil, err
	}
	record.LeaseTokenHash = stringValue(values[4])
	if record.LeaseExpireAtMs, err = intValue(values[5]); err != nil {
		return nil, err
	}
	if record.NextDueAtMs, err = intValue(values[6]); err != nil {
		return nil, err
	}
	if record.UpdatedAtMs, err = intValue(values[7]); err != nil {
		return nil, err
	}
	if record.CompletedAtMs, err = intValue(values[8]); err != nil {
		return nil, err
	}
	return record, nil
}

func (s *RedisWorkRepository) Advance(ctx context.Context, next *WorkRecord, opts AdvanceOptions) (*WorkRecord, error) {
	if err := next.Validate(); err != nil {
		return nil, err
	}
	key, err := s.recordKey(next.Address.WorkType, next.Address.WorkID)
	if err != nil {
		return nil, err
	}
	ref, err := s.workRef(next.Address.WorkType, next.Address.WorkID)
	if err != nil {
		return nil, err
	}
	terminal := 0
	if next.Terminal() {
		terminal = 1
	}
	release := 0
	if opts.ReleaseLease {
		release = 1
	}
	data, err := encodeWorkRecord(next)
	if err != nil {
		return nil, err
	}
	sourceordermetrics.ObserveWorkflowAdvance(len(data))
	reply, err := s.client.Eval(ctx, luaAdvanceWork, []string{key, s.dueKey()},
		opts.ExpectedWorkVersion,
		opts.LeaseOwner,
		opts.LeaseEpoch,
		release,
		data,
		next.NextDueAtMs,
		terminal,
		ref,
	).Slice()
	if err != nil {
		return nil, err
	}
	if len(reply) == 0 {
		return nil, errors.New("workflow: malformed advance reply")
	}
	switch stringValue(reply[0]) {
	case "1":
		updated := cloneWorkRecord(next)
		newVersion, err := uintValue(reply[1])
		if err != nil {
			return nil, err
		}
		updated.WorkVersion = newVersion
		if updated.Terminal() || opts.ReleaseLease {
			updated.LeaseOwner = ""
			updated.LeaseTokenHash = ""
			updated.LeaseExpireAtMs = 0
		}
		if updated.Terminal() {
			updated.NextDueAtMs = 0
		}
		return updated, nil
	case "-1":
		return nil, ErrNotFound
	case "-2":
		return nil, ErrRevisionConflict
	case "-3":
		return nil, ErrLeaseLost
	case "-4":
		return nil, ErrTerminal
	default:
		return nil, fmt.Errorf("workflow: unexpected advance status %v", reply[0])
	}
}

func (s *RedisWorkRepository) ClaimDue(ctx context.Context, owner, token string, limit int, ttl time.Duration) ([]*ClaimedWork, error) {
	owner = strings.TrimSpace(owner)
	if owner == "" || strings.TrimSpace(token) == "" || ttl <= 0 {
		return nil, ErrInvalid
	}
	if limit <= 0 {
		limit = 32
	}
	tokenHash := leaseTokenHash(token)
	raw, err := s.client.Eval(ctx, luaClaimDueWork, []string{s.dueKey()},
		limit, owner, tokenHash, ttl.Milliseconds(), s.recordKeyPrefix()).Slice()
	if err != nil {
		return nil, err
	}
	return decodeClaimedWorks(raw, owner, tokenHash, ttl)
}

func (s *RedisWorkRepository) ClaimDueByTypes(ctx context.Context, owner, token string, workTypes []string, limit int, ttl time.Duration) ([]*ClaimedWork, error) {
	owner = strings.TrimSpace(owner)
	if owner == "" || strings.TrimSpace(token) == "" || ttl <= 0 || len(workTypes) == 0 {
		return nil, ErrInvalid
	}
	if limit <= 0 {
		limit = 32
	}
	args := make([]interface{}, 0, 6+len(workTypes))
	tokenHash := leaseTokenHash(token)
	args = append(args, limit, owner, tokenHash, ttl.Milliseconds(), s.recordKeyPrefix(), 64)
	seen := make(map[string]struct{}, len(workTypes))
	for _, workType := range workTypes {
		workType = strings.TrimSpace(workType)
		if workType == "" {
			return nil, ErrInvalid
		}
		if _, ok := seen[workType]; ok {
			continue
		}
		seen[workType] = struct{}{}
		args = append(args, workType)
	}
	raw, err := s.client.Eval(ctx, luaClaimDueWorkByTypes, []string{s.dueKey()}, args...).Slice()
	if err != nil {
		return nil, err
	}
	return decodeClaimedWorks(raw, owner, tokenHash, ttl)
}

func decodeClaimedWorks(raw []interface{}, owner, tokenHash string, ttl time.Duration) ([]*ClaimedWork, error) {
	if len(raw)%5 != 0 {
		return nil, errors.New("workflow: malformed claim reply")
	}
	claimed := make([]*ClaimedWork, 0, len(raw)/5)
	for offset := 0; offset < len(raw); offset += 5 {
		ref := stringValue(raw[offset])
		workRef, err := workidentity.DecodeWorkRefV1(ref)
		if err != nil {
			return nil, err
		}
		data, ok := raw[offset+1].(string)
		if !ok {
			return nil, fmt.Errorf("workflow: invalid claimed payload %T", raw[offset+1])
		}
		record, err := decodeWorkRecord([]byte(data))
		if err != nil {
			return nil, err
		}
		if record.Address.WorkType != workRef.WorkType || record.Address.WorkID != workRef.WorkID {
			return nil, fmt.Errorf("%w: claimed ref does not match record", ErrWorkRefMismatch)
		}
		if record.WorkVersion, err = uintValue(raw[offset+2]); err != nil {
			return nil, err
		}
		if record.LeaseEpoch, err = uintValue(raw[offset+3]); err != nil {
			return nil, err
		}
		if record.LeaseExpireAtMs, err = intValue(raw[offset+4]); err != nil {
			return nil, err
		}
		record.LeaseOwner = owner
		record.LeaseTokenHash = tokenHash
		record.NextDueAtMs = record.LeaseExpireAtMs
		claimed = append(claimed, &ClaimedWork{Record: record, AcceptedTTL: ttl})
	}
	return claimed, nil
}

func (s *RedisWorkRepository) ClaimWork(ctx context.Context, workType, workID, owner, token string, ttl time.Duration) (*ClaimedWork, error) {
	owner = strings.TrimSpace(owner)
	if owner == "" || strings.TrimSpace(token) == "" || ttl <= 0 {
		return nil, ErrInvalid
	}
	key, err := s.recordKey(workType, workID)
	if err != nil {
		return nil, err
	}
	ref, err := s.workRef(workType, workID)
	if err != nil {
		return nil, err
	}
	raw, err := s.client.Eval(ctx, luaClaimSpecificWork, []string{key, s.dueKey()},
		owner, workType, leaseTokenHash(token), ttl.Milliseconds(), ref).Slice()
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, ErrInvalid
	}
	switch stringValue(raw[0]) {
	case "-1":
		return nil, ErrNotFound
	case "-3":
		return nil, ErrLeaseLost
	case "-4":
		return nil, ErrTerminal
	case "-5":
		return nil, ErrNotDue
	case "1":
		if len(raw) != 5 {
			return nil, ErrInvalid
		}
		data, ok := raw[1].(string)
		if !ok {
			return nil, ErrInvalid
		}
		record, err := decodeWorkRecord([]byte(data))
		if err != nil {
			return nil, err
		}
		record.WorkVersion, err = uintValue(raw[2])
		if err != nil {
			return nil, err
		}
		record.LeaseEpoch, err = uintValue(raw[3])
		if err != nil {
			return nil, err
		}
		record.LeaseOwner = owner
		record.LeaseTokenHash = leaseTokenHash(token)
		record.LeaseExpireAtMs, err = intValue(raw[4])
		if err != nil {
			return nil, err
		}
		record.NextDueAtMs = record.LeaseExpireAtMs
		return &ClaimedWork{Record: record, AcceptedTTL: ttl}, nil
	default:
		return nil, ErrInvalid
	}
}

func (s *RedisWorkRepository) RenewLease(ctx context.Context, workType, workID, owner, token string, epoch uint64, ttl time.Duration) (*WorkRecord, error) {
	if strings.TrimSpace(owner) == "" || strings.TrimSpace(token) == "" || epoch == 0 || ttl <= 0 {
		return nil, ErrInvalid
	}
	key, err := s.recordKey(workType, workID)
	if err != nil {
		return nil, err
	}
	ref, err := s.workRef(workType, workID)
	if err != nil {
		return nil, err
	}
	reply, err := s.client.Eval(ctx, luaRenewWorkLease, []string{key, s.dueKey()},
		owner, epoch, leaseTokenHash(token), ttl.Milliseconds(), ref).Slice()
	if err != nil {
		return nil, err
	}
	if len(reply) == 0 {
		return nil, errors.New("workflow: malformed renew reply")
	}
	switch stringValue(reply[0]) {
	case "1":
		record, err := s.Get(ctx, workType, workID)
		if err != nil {
			return nil, err
		}
		return record, nil
	case "-1":
		return nil, ErrNotFound
	case "-3":
		return nil, ErrLeaseLost
	case "-4":
		return nil, ErrTerminal
	default:
		return nil, fmt.Errorf("workflow: unexpected renew status %v", reply[0])
	}
}

func (s *RedisWorkRepository) PendingDue(ctx context.Context) (int64, error) {
	return s.client.Eval(ctx, luaPendingDueWork, []string{s.dueKey()}).Int64()
}

// ObserveDueByTypes proves a complete typed view only when the entire shared
// due prefix fits inside limit. This keeps the read bounded and prevents a
// partial family count from masquerading as DueIndex truth.
func (s *RedisWorkRepository) ObserveDueByTypes(ctx context.Context, workTypes []string, limit int) (DueIndexObservation, error) {
	if len(workTypes) == 0 || limit <= 0 || limit > 4096 {
		return DueIndexObservation{}, ErrInvalid
	}
	allowed := make(map[string]struct{}, len(workTypes))
	for _, workType := range workTypes {
		workType = strings.TrimSpace(workType)
		if workType == "" {
			return DueIndexObservation{}, ErrInvalid
		}
		allowed[workType] = struct{}{}
	}
	now, err := s.client.Time(ctx).Result()
	if err != nil {
		return DueIndexObservation{}, err
	}
	entries, err := s.client.ZRangeByScoreWithScores(ctx, s.dueKey(), &redis.ZRangeBy{
		Min: "-inf", Max: fmt.Sprintf("%d", now.UnixMilli()), Count: int64(limit + 1),
	}).Result()
	if err != nil {
		return DueIndexObservation{}, err
	}
	if len(entries) > limit {
		return DueIndexObservation{}, nil
	}
	observation := DueIndexObservation{Available: true}
	for _, entry := range entries {
		ref, decodeErr := workidentity.DecodeWorkRefV1(stringValue(entry.Member))
		if decodeErr != nil {
			return DueIndexObservation{}, nil
		}
		if _, ok := allowed[ref.WorkType]; !ok {
			continue
		}
		observation.Depth++
		dueAt := int64(entry.Score)
		if observation.OldestDueAtMs == 0 || dueAt < observation.OldestDueAtMs {
			observation.OldestDueAtMs = dueAt
		}
	}
	return observation, nil
}
