package workflow

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	workflowpb "farmgame/api/gen/workflow"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

const createWorkflowScript = `
if redis.call('EXISTS', KEYS[1]) == 1 then
  return 0
end
redis.call('HSET', KEYS[1],
  'data', ARGV[1],
  'revision', ARGV[2],
  'lease_owner', '',
  'lease_epoch', 0,
  'lease_until_ms', 0,
  'next_retry_at_ms', ARGV[3],
  'terminal', 0)
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[4])
return 1
`

const updateWorkflowScript = `
if redis.call('EXISTS', KEYS[1]) == 0 then
  return -1
end
local revision = tonumber(redis.call('HGET', KEYS[1], 'revision') or '0')
if revision ~= tonumber(ARGV[1]) then
  return -2
end
local current_owner = redis.call('HGET', KEYS[1], 'lease_owner') or ''
local current_epoch = tonumber(redis.call('HGET', KEYS[1], 'lease_epoch') or '0')
local current_until = tonumber(redis.call('HGET', KEYS[1], 'lease_until_ms') or '0')
if ARGV[2] ~= '' then
  if current_owner ~= ARGV[2] or current_epoch ~= tonumber(ARGV[3]) or current_until <= tonumber(ARGV[4]) then
    return -3
  end
elseif current_owner ~= '' and current_until > tonumber(ARGV[4]) then
  return -3
end
redis.call('HSET', KEYS[1],
  'data', ARGV[5],
  'revision', ARGV[6],
  'lease_owner', '',
  'lease_epoch', current_epoch,
  'lease_until_ms', 0,
  'next_retry_at_ms', ARGV[7],
  'terminal', ARGV[8])
if ARGV[8] == '1' then
  redis.call('ZREM', KEYS[2], ARGV[9])
else
  redis.call('ZADD', KEYS[2], ARGV[7], ARGV[9])
end
return 1
`

const claimDueScript = `
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
local result = {}
for _, id in ipairs(ids) do
  local key = ARGV[5] .. id
  if redis.call('EXISTS', key) == 0 then
    redis.call('ZREM', KEYS[1], id)
  else
    local terminal = redis.call('HGET', key, 'terminal') or '0'
    local next_retry = tonumber(redis.call('HGET', key, 'next_retry_at_ms') or '0')
    local lease_until = tonumber(redis.call('HGET', key, 'lease_until_ms') or '0')
    if terminal == '1' then
      redis.call('ZREM', KEYS[1], id)
    elseif next_retry <= tonumber(ARGV[1]) and lease_until <= tonumber(ARGV[1]) then
      local revision = tonumber(redis.call('HGET', key, 'revision') or '0') + 1
      local epoch = tonumber(redis.call('HGET', key, 'lease_epoch') or '0') + 1
      redis.call('HSET', key,
        'revision', revision,
        'lease_owner', ARGV[3],
        'lease_epoch', epoch,
        'lease_until_ms', ARGV[4])
      redis.call('ZADD', KEYS[1], ARGV[4], id)
      table.insert(result, id)
      table.insert(result, redis.call('HGET', key, 'data'))
      table.insert(result, tostring(revision))
      table.insert(result, tostring(epoch))
      table.insert(result, ARGV[4])
    end
  end
end
return result
`

type RedisStore struct {
	client redis.UniversalClient
	prefix string
}

func NewRedisStore(client redis.UniversalClient, prefix string) (*RedisStore, error) {
	prefix = strings.TrimSuffix(strings.TrimSpace(prefix), ":")
	if client == nil || prefix == "" {
		return nil, errors.New("workflow: redis client and prefix required")
	}
	return &RedisStore{client: client, prefix: prefix}, nil
}

// Redis Cluster requires every key touched by one Lua script to share a hash
// slot. The tag intentionally groups the due index and its records.
func (s *RedisStore) keyBase() string            { return s.prefix + ":{workflow}" }
func (s *RedisStore) recordKey(id string) string { return s.keyBase() + ":record:" + id }
func (s *RedisStore) recordKeyPrefix() string    { return s.keyBase() + ":record:" }
func (s *RedisStore) dueKey() string             { return s.keyBase() + ":due" }

func (s *RedisStore) CreateOrLoad(ctx context.Context, candidate *workflowpb.WorkflowRecord) (*workflowpb.WorkflowRecord, bool, error) {
	if err := Validate(candidate); err != nil {
		return nil, false, err
	}
	created := clone(candidate)
	created.WorkflowRevision = 1
	created.LeaseOwner = ""
	created.LeaseEpoch = 0
	created.LeaseUntilMs = 0
	data, err := marshalRecord(created)
	if err != nil {
		return nil, false, err
	}
	inserted, err := s.client.Eval(ctx, createWorkflowScript,
		[]string{s.recordKey(created.WorkflowId), s.dueKey()},
		data, created.WorkflowRevision, created.NextRetryAtMs, created.WorkflowId).Int()
	if err != nil {
		return nil, false, err
	}
	if inserted == 1 {
		return created, true, nil
	}
	existing, err := s.Get(ctx, created.WorkflowId)
	if err != nil {
		return nil, false, err
	}
	if !sameIntent(existing, created) {
		return nil, false, ErrIdempotencyConflict
	}
	return existing, false, nil
}

func (s *RedisStore) Get(ctx context.Context, id string) (*workflowpb.WorkflowRecord, error) {
	values, err := s.client.HMGet(ctx, s.recordKey(id),
		"data", "revision", "lease_owner", "lease_epoch", "lease_until_ms", "next_retry_at_ms").Result()
	if err != nil {
		return nil, err
	}
	if len(values) != 6 || values[0] == nil {
		return nil, ErrNotFound
	}
	record, err := decodeRecord(values[0])
	if err != nil {
		return nil, err
	}
	if record.WorkflowRevision, err = uintValue(values[1]); err != nil {
		return nil, err
	}
	record.LeaseOwner = stringValue(values[2])
	if record.LeaseEpoch, err = uintValue(values[3]); err != nil {
		return nil, err
	}
	if record.LeaseUntilMs, err = intValue(values[4]); err != nil {
		return nil, err
	}
	if record.NextRetryAtMs, err = intValue(values[5]); err != nil {
		return nil, err
	}
	return record, nil
}

func (s *RedisStore) Update(ctx context.Context, next *workflowpb.WorkflowRecord, options UpdateOptions) (*workflowpb.WorkflowRecord, error) {
	if err := Validate(next); err != nil || options.ExpectedRevision == 0 {
		return nil, ErrInvalid
	}
	current, err := s.Get(ctx, next.WorkflowId)
	if err != nil {
		return nil, err
	}
	if !sameIntent(current, next) {
		return nil, ErrIdempotencyConflict
	}
	if Terminal(current.State) {
		return nil, ErrTerminal
	}
	now := options.Now
	if now.IsZero() {
		now = time.Now()
	}
	updated := clone(next)
	updated.WorkflowRevision = options.ExpectedRevision + 1
	updated.LeaseOwner = ""
	updated.LeaseEpoch = current.LeaseEpoch
	updated.LeaseUntilMs = 0
	data, err := marshalRecord(updated)
	if err != nil {
		return nil, err
	}
	terminal := 0
	if Terminal(updated.State) {
		terminal = 1
	}
	status, err := s.client.Eval(ctx, updateWorkflowScript,
		[]string{s.recordKey(updated.WorkflowId), s.dueKey()},
		options.ExpectedRevision,
		options.LeaseOwner,
		options.LeaseEpoch,
		now.UnixMilli(),
		data,
		updated.WorkflowRevision,
		updated.NextRetryAtMs,
		terminal,
		updated.WorkflowId,
	).Int()
	if err != nil {
		return nil, err
	}
	switch status {
	case 1:
		return updated, nil
	case -1:
		return nil, ErrNotFound
	case -2:
		return nil, ErrRevisionConflict
	case -3:
		return nil, ErrLeaseLost
	default:
		return nil, fmt.Errorf("workflow: unexpected update status %d", status)
	}
}

func (s *RedisStore) ClaimDue(ctx context.Context, owner string, now time.Time, limit int, lease time.Duration) ([]*workflowpb.WorkflowRecord, error) {
	owner = strings.TrimSpace(owner)
	if owner == "" {
		return nil, ErrInvalid
	}
	if limit <= 0 {
		limit = 32
	}
	if lease <= 0 {
		lease = 5 * time.Second
	}
	raw, err := s.client.Eval(ctx, claimDueScript, []string{s.dueKey()},
		now.UnixMilli(), limit, owner, now.Add(lease).UnixMilli(), s.recordKeyPrefix()).Slice()
	if err != nil {
		return nil, err
	}
	if len(raw)%5 != 0 {
		return nil, errors.New("workflow: malformed claim response")
	}
	result := make([]*workflowpb.WorkflowRecord, 0, len(raw)/5)
	for offset := 0; offset < len(raw); offset += 5 {
		record, err := decodeRecord(raw[offset+1])
		if err != nil {
			return nil, err
		}
		record.WorkflowId = stringValue(raw[offset])
		record.WorkflowRevision, err = uintValue(raw[offset+2])
		if err != nil {
			return nil, err
		}
		record.LeaseOwner = owner
		record.LeaseEpoch, err = uintValue(raw[offset+3])
		if err != nil {
			return nil, err
		}
		record.LeaseUntilMs, err = intValue(raw[offset+4])
		if err != nil {
			return nil, err
		}
		result = append(result, record)
	}
	return result, nil
}

// PendingDue 返回 due zset 中 next_retry_at_ms <= now 的元素数,即待领取 backlog。
func (s *RedisStore) PendingDue(ctx context.Context, now time.Time) (int64, error) {
	return s.client.ZCount(ctx, s.dueKey(), "-inf", strconv.FormatInt(now.UnixMilli(), 10)).Result()
}

func marshalRecord(record *workflowpb.WorkflowRecord) ([]byte, error) {
	return proto.MarshalOptions{Deterministic: true}.Marshal(record)
}

func decodeRecord(value any) (*workflowpb.WorkflowRecord, error) {
	var data []byte
	switch got := value.(type) {
	case string:
		data = []byte(got)
	case []byte:
		data = got
	default:
		return nil, fmt.Errorf("workflow: invalid record payload %T", value)
	}
	var record workflowpb.WorkflowRecord
	if err := proto.Unmarshal(data, &record); err != nil {
		return nil, fmt.Errorf("workflow: decode: %w", err)
	}
	return &record, nil
}

func stringValue(value any) string {
	if value == nil {
		return ""
	}
	return fmt.Sprint(value)
}

func uintValue(value any) (uint64, error) {
	return strconv.ParseUint(stringValue(value), 10, 64)
}

func intValue(value any) (int64, error) {
	return strconv.ParseInt(stringValue(value), 10, 64)
}
