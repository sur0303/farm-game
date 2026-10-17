package ticket

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"farmgame/internal/gateway/redisscript"
	"farmgame/internal/gateway/session"
	"farmgame/internal/observability/redisworkload"
	"farmgame/internal/storage/redis/rediskey"
)

// RedisStore 是票据的 Redis 实现,遵循 30-redis.md §4:
//   - 键为 connection:ticket:{sha256}:hash,只存 SHA256(ticket),不存原文;
//   - 值用 hash 保存绑定字段;
//   - 核销使用 Lua 脚本原子"读+删",并发只能一个成功(GETDEL/Lua 语义)。
type RedisStore struct {
	cli     redis.UniversalClient
	scripts *redisscript.Registry
	metrics *Metrics // optional; nil-safe
}

// NewRedisStore 创建 Redis 票据存储。
func NewRedisStore(cli redis.UniversalClient) *RedisStore {
	scripts, err := redisscript.New(cli,
		redisscript.Definition{Name: redeemScriptName, Source: redeemScript},
		redisscript.Definition{Name: activateScriptName, Source: activateScript},
		redisscript.Definition{Name: saveScriptName, Source: saveScript},
	)
	if err != nil {
		panic("ticket: build Redis script registry: " + err.Error())
	}
	return &RedisStore{cli: cli, scripts: scripts}
}

// SetMetrics 挂接票据操作指标(接入层容量观测)。服务启动时调用一次。
func (r *RedisStore) SetMetrics(m *Metrics) {
	r.metrics = m
}

// redeemScript 原子地读取并删除票据 hash,返回字段列表;不存在或已过期返回空。
// KEYS[1]=ticket key;ARGV[1]=now unix ms。删除后即使连接断开原票据也不得恢复。
const redeemScript = `
local k = KEYS[1]
local exists = redis.call('EXISTS', k)
if exists == 0 then return {} end
local exp = redis.call('HGET', k, 'expires_at')
if exp and tonumber(exp) > tonumber(ARGV[1]) then
  local player = redis.call('HGET', k, 'player_id')
  local sid = redis.call('HGET', k, 'session_id')
  local sae = redis.call('HGET', k, 'session_authority_epoch')
  local ase = redis.call('HGET', k, 'active_session_epoch')
  local issued = redis.call('HGET', k, 'issued_at')
  redis.call('DEL', k)
  return {player, sid, sae, ase, issued, exp}
end
redis.call('DEL', k)
return {}
`

const redeemScriptName = "redeem_ticket_v1"

// Keep every uint64 fence as a bounded canonical decimal string. Redis Lua
// numbers are doubles, so tonumber(2^53+1) would silently collapse identities.
const ticketUint64DecimalLua = `
local UINT64_MAX = '18446744073709551615'

local function canonical_u64(value)
  if type(value) ~= 'string' or string.match(value, '^%d+$') == nil then return nil end
  local canonical = string.gsub(value, '^0+', '')
  if canonical == '' then canonical = '0' end
  if string.len(canonical) > string.len(UINT64_MAX) or
     (string.len(canonical) == string.len(UINT64_MAX) and canonical > UINT64_MAX) then
    return nil
  end
  return canonical
end

local function compare_u64(left, right)
  if string.len(left) < string.len(right) then return -1 end
  if string.len(left) > string.len(right) then return 1 end
  if left < right then return -1 end
  if left > right then return 1 end
  return 0
end

local function increment_u64(value)
  if value == UINT64_MAX then return nil end
  local digits = {}
  local carry = 1
  for index = string.len(value), 1, -1 do
    local digit = string.byte(value, index) - 48 + carry
    if digit >= 10 then
      digit = digit - 10
      carry = 1
    else
      carry = 0
    end
    digits[index] = string.char(48 + digit)
  end
  local prefix = ''
  if carry == 1 then prefix = '1' end
  return prefix .. table.concat(digits)
end
`

const activateScript = ticketUint64DecimalLua + `
local ticket = KEYS[1]
local authority = KEYS[2]
if redis.call('EXISTS', ticket) == 0 then return {-1} end
local status = redis.call('HGET', ticket, 'status') or 'UNUSED'
if status == 'SUPERSEDED' then return {-2} end
if status == 'CONSUMED' then
  local activation_gate = redis.call('HGET', ticket, 'activation_gate_id') or ''
  local activation_boot = redis.call('HGET', ticket, 'activation_gate_boot_id') or ''
  local activation_connection = redis.call('HGET', ticket, 'activation_connection_id') or ''
  local activation_generation = redis.call('HGET', ticket, 'activation_connection_generation') or '0'
  local activation_revision = canonical_u64(redis.call('HGET', ticket, 'activation_route_revision') or '0')
  if activation_gate ~= ARGV[2] or activation_boot ~= ARGV[3] or
     activation_connection ~= ARGV[4] or activation_generation ~= ARGV[5] then
    return {-3}
  end

  -- The tombstone resolves only an Unknown outcome for the activation that is
  -- still authoritative now. Comparing the complete route fence plus session
  -- and authority epochs prevents a delayed A replay from being installed by
  -- a Gate after B activation, revoke, or an exact route clear already won.
  local ticket_player = redis.call('HGET', ticket, 'player_id') or '0'
  local ticket_authority = canonical_u64(redis.call('HGET', ticket, 'session_authority_epoch') or '0')
  local ticket_session = redis.call('HGET', ticket, 'session_id') or ''
  local ticket_epoch = canonical_u64(redis.call('HGET', ticket, 'reserved_session_epoch') or redis.call('HGET', ticket, 'active_session_epoch') or '0')
  local current_authority = canonical_u64(redis.call('HGET', authority, 'authority_epoch') or '0')
  local current_session = redis.call('HGET', authority, 'session_id') or ''
  local current_epoch = canonical_u64(redis.call('HGET', authority, 'active_epoch') or redis.call('HGET', authority, 'epoch') or '0')
  local current_revision = canonical_u64(redis.call('HGET', authority, 'route_revision') or '0')
  if not activation_revision or not ticket_authority or not ticket_epoch or
     not current_authority or not current_epoch or not current_revision then
    return redis.error_reply('ticket: malformed activation uint64 fence')
  end
  if ticket_player ~= ARGV[6] or compare_u64(ticket_authority, current_authority) ~= 0 or
     ticket_session ~= current_session or compare_u64(ticket_epoch, current_epoch) ~= 0 or
     activation_gate ~= (redis.call('HGET', authority, 'route_gate_id') or '') or
     activation_boot ~= (redis.call('HGET', authority, 'route_gate_boot_id') or '') or
     activation_connection ~= (redis.call('HGET', authority, 'route_connection_id') or '') or
     activation_generation ~= (redis.call('HGET', authority, 'route_connection_generation') or '0') or
     compare_u64(activation_revision, current_revision) ~= 0 then
    redis.call('HSET', ticket, 'status', 'SUPERSEDED')
    redis.call('PEXPIRE', ticket, ARGV[9])
    return {-2}
  end

  -- Never grant a fresh full TTL. Return only the Redis-authoritative route
  -- lease that remains; a renewal may legitimately have extended it since the
  -- first ClientHello while preserving every identity fence above.
  local replay_lease_expire = tonumber(redis.call('HGET', authority, 'route_lease_expire_at_ms') or '0')
  local replay_redis_time = redis.call('TIME')
  local replay_now_ms = tonumber(replay_redis_time[1]) * 1000 + math.floor(tonumber(replay_redis_time[2]) / 1000)
  local replay_remaining_ttl = replay_lease_expire - replay_now_ms
  if replay_remaining_ttl <= 0 then return {-1} end
  return {
    2,
    ticket_player,
    ticket_session,
    ticket_authority,
    ticket_epoch,
    redis.call('HGET', ticket, 'issued_at'),
    redis.call('HGET', ticket, 'expires_at'),
    redis.call('HGET', ticket, 'activation_old_gate_id') or '',
    redis.call('HGET', ticket, 'activation_old_gate_boot_id') or '',
    redis.call('HGET', ticket, 'activation_old_connection_id') or '',
    redis.call('HGET', ticket, 'activation_old_connection_generation') or '0',
    redis.call('HGET', ticket, 'activation_old_session_id') or '',
    redis.call('HGET', ticket, 'activation_old_session_epoch') or '0',
    redis.call('HGET', ticket, 'activation_old_route_revision') or '0',
    redis.call('HGET', ticket, 'activation_old_lease_expire_at_ms') or '0',
    activation_revision,
    tostring(replay_lease_expire),
    tostring(replay_remaining_ttl)
  }
end
local expires = tonumber(redis.call('HGET', ticket, 'expires_at') or '0')
if expires <= tonumber(ARGV[1]) then
  redis.call('DEL', ticket)
  return {-1}
end

local player = redis.call('HGET', ticket, 'player_id') or '0'
if player ~= ARGV[6] then return {-1} end
local ticket_authority = canonical_u64(redis.call('HGET', ticket, 'session_authority_epoch') or '0')
local current_authority = canonical_u64(redis.call('HGET', authority, 'authority_epoch') or '0')
local reserved = canonical_u64(redis.call('HGET', ticket, 'reserved_session_epoch') or redis.call('HGET', ticket, 'active_session_epoch') or '0')
local active = canonical_u64(redis.call('HGET', authority, 'active_epoch') or redis.call('HGET', authority, 'epoch') or '0')
if not ticket_authority or not current_authority or not reserved or not active then
  return redis.error_reply('ticket: malformed session epoch')
end
if compare_u64(ticket_authority, current_authority) ~= 0 or compare_u64(reserved, active) <= 0 then
  redis.call('HSET', ticket, 'status', 'SUPERSEDED')
  redis.call('PEXPIRE', ticket, ARGV[9])
  return {-2}
end

local old_gate = redis.call('HGET', authority, 'route_gate_id') or ''
local old_boot = redis.call('HGET', authority, 'route_gate_boot_id') or ''
local old_connection = redis.call('HGET', authority, 'route_connection_id') or ''
local old_generation = redis.call('HGET', authority, 'route_connection_generation') or '0'
local old_session = redis.call('HGET', authority, 'session_id') or ''
local old_epoch = active
local old_revision = canonical_u64(redis.call('HGET', authority, 'route_revision') or '0')
local old_lease = redis.call('HGET', authority, 'route_lease_expire_at_ms') or '0'
if not old_revision then return redis.error_reply('ticket: malformed route revision') end
local revision = increment_u64(old_revision)
if not revision then return redis.error_reply('ticket: route revision overflow') end
local accepted_ttl = tonumber(ARGV[7])
local redis_time = redis.call('TIME')
local redis_now_ms = tonumber(redis_time[1]) * 1000 + math.floor(tonumber(redis_time[2]) / 1000)
local lease_expire = redis_now_ms + accepted_ttl

redis.call('HSET', authority,
  'active_epoch', reserved,
  'epoch', reserved,
  'session_id', redis.call('HGET', ticket, 'session_id'),
  'route_gate_id', ARGV[2],
  'route_gate_boot_id', ARGV[3],
  'route_connection_id', ARGV[4],
  'route_connection_generation', ARGV[5],
  'route_revision', revision,
  'route_lease_expire_at_ms', lease_expire)
redis.call('PEXPIRE', authority, ARGV[8])

redis.call('HSET', ticket,
  'status', 'CONSUMED',
  'activation_gate_id', ARGV[2],
  'activation_gate_boot_id', ARGV[3],
  'activation_connection_id', ARGV[4],
  'activation_connection_generation', ARGV[5],
  'activation_old_gate_id', old_gate,
  'activation_old_gate_boot_id', old_boot,
  'activation_old_connection_id', old_connection,
  'activation_old_connection_generation', old_generation,
  'activation_old_session_id', old_session,
  'activation_old_session_epoch', old_epoch,
  'activation_old_route_revision', old_revision,
  'activation_old_lease_expire_at_ms', old_lease,
  'activation_route_revision', revision,
  'activation_lease_expire_at_ms', lease_expire,
  'activation_accepted_ttl_ms', accepted_ttl)
redis.call('PEXPIRE', ticket, ARGV[9])

return {
  1,
  player,
  redis.call('HGET', ticket, 'session_id'),
  ticket_authority,
  reserved,
  redis.call('HGET', ticket, 'issued_at'),
  redis.call('HGET', ticket, 'expires_at'),
  old_gate, old_boot, old_connection, old_generation, old_session,
  old_epoch, old_revision, old_lease, revision, tostring(lease_expire), tostring(accepted_ttl)
}
`

const activateScriptName = "activate_ticket_route_v1"

// saveScript makes the ticket body and TTL visible at one linearization point.
// The former TxPipeline required MULTI + HSET + EXPIRE + EXEC for every issued
// ticket; one script preserves the no-unbounded-ticket invariant with one warm
// Redis command.
const saveScript = ticketUint64DecimalLua + `
local ttl = tonumber(ARGV[8])
if not ttl or ttl < 1 then return 0 end
local player = canonical_u64(ARGV[1])
local authority = canonical_u64(ARGV[3])
local reserved = canonical_u64(ARGV[4])
local active = canonical_u64(ARGV[5])
if not player or not authority or not reserved or not active then
  return redis.error_reply('ticket: malformed uint64 binding')
end
redis.call('HSET', KEYS[1],
  'player_id', player,
  'session_id', ARGV[2],
  'session_authority_epoch', authority,
  'reserved_session_epoch', reserved,
  'active_session_epoch', active,
  'status', 'UNUSED',
  'issued_at', ARGV[6],
  'expires_at', ARGV[7])
if redis.call('PEXPIRE', KEYS[1], ttl) ~= 1 then return 0 end
if redis.call('EXISTS', KEYS[1]) ~= 1 then return 0 end
return 1
`

const saveScriptName = "save_ticket_v1"

// EnsureScriptsLoaded preloads the closed ConnectionTicket script family on
// every Redis primary. A later NOSCRIPT remains recoverable by exact source.
func (r *RedisStore) EnsureScriptsLoaded(ctx context.Context) error {
	ctx = redisworkload.WithComponent(ctx, redisworkload.ComponentGateTicket)
	if r == nil || r.scripts == nil {
		return errors.New("ticket: Redis script registry unavailable")
	}
	return r.scripts.EnsureLoaded(ctx)
}

const activationTombstoneTTL = time.Minute

// Save 将票据绑定写入 hash 并设置 TTL。
func (r *RedisStore) Save(ctx context.Context, token string, bound Bound, ttl time.Duration) (err error) {
	ctx = redisworkload.WithComponent(ctx, redisworkload.ComponentGateTicket)
	start := time.Now()
	defer func() { r.metrics.observe("save", start, err) }()
	var key string
	key, err = r.key(token, bound.PlayerID)
	if err != nil {
		return err
	}
	ttlMS, ttlErr := redisTTLMilliseconds(ttl)
	if ttlErr != nil {
		return ttlErr
	}
	exp := bound.ExpiresAt
	if exp.IsZero() {
		exp = time.Now().Add(time.Duration(ttlMS) * time.Millisecond)
	}
	reserved := bound.ReservedSessionEpoch
	if reserved == 0 {
		reserved = bound.ActiveSessionEpoch
	}
	value, err := r.scripts.Run(ctx, saveScriptName, []string{key},
		bound.PlayerID, bound.SessionID, bound.SessionAuthorityEpoch, reserved,
		bound.ActiveSessionEpoch, bound.IssuedAt.UnixMilli(), exp.UnixMilli(),
		ttlMS).Int64()
	if err != nil {
		return err
	}
	if value != 1 {
		return fmt.Errorf("ticket: malformed save result %d", value)
	}
	return nil
}

// Redeem 原子核销;成功返回绑定,失败(不存在/已过期/已用)返回 ErrRedeemFailed。
func (r *RedisStore) Redeem(ctx context.Context, token string) (bound Bound, err error) {
	ctx = redisworkload.WithComponent(ctx, redisworkload.ComponentGateTicket)
	start := time.Now()
	defer func() { r.metrics.observe("redeem", start, err) }()
	var key string
	key, err = r.key(token, 0)
	if err != nil {
		return bound, ErrRedeemFailed
	}
	// RunScript 自动处理 SHA 缓存与 EVALSHA 回退到 EVAL,并正确映射 redis.Nil。
	res, err := r.scripts.Run(ctx, redeemScriptName, []string{key}, time.Now().UnixMilli()).Result()
	if err == redis.Nil {
		return bound, ErrRedeemFailed
	}
	if err != nil {
		return bound, err
	}
	return parseRedeemResult(res)
}

// Activate atomically consumes a v1 ticket and replaces the player's active
// SessionRoute. Both keys use {player_id}, so the script is valid on Redis
// Cluster as well as standalone Redis.
func (r *RedisStore) Activate(ctx context.Context, token string, route session.RouteFence, leaseTTL time.Duration) (bound Bound, result *session.ActivationResult, err error) {
	ctx = redisworkload.WithComponent(ctx, redisworkload.ComponentGateTicket)
	start := time.Now()
	defer func() { r.metrics.observe("activate", start, err) }()
	var playerID uint64
	playerID, err = PlayerIDFromToken(token)
	if err != nil || route.GatewayID == "" || route.GatewayBootID == "" ||
		route.ConnectionID == "" || route.ConnectionGeneration == 0 {
		return bound, nil, ErrRedeemFailed
	}
	if leaseTTL <= 0 {
		leaseTTL = session.DefaultRouteLeaseTTL
	}
	leaseTTLMS, ttlErr := redisTTLMilliseconds(leaseTTL)
	if ttlErr != nil {
		return bound, nil, ErrRedeemFailed
	}
	now := time.Now()
	var raw any
	raw, err = r.scripts.Run(ctx, activateScriptName, []string{
		rediskey.ConnectionTicketForPlayer(playerID, Hash(token)),
		rediskey.Session(playerID),
	}, now.UnixMilli(), route.GatewayID, route.GatewayBootID, route.ConnectionID,
		route.ConnectionGeneration, fmt.Sprint(playerID), leaseTTLMS,
		session.DefaultSessionTTL.Milliseconds(), activationTombstoneTTL.Milliseconds()).Result()
	if err != nil {
		return bound, nil, err
	}
	values, ok := raw.([]interface{})
	if !ok || len(values) == 0 {
		return bound, nil, ErrRedeemFailed
	}
	code, parseErr := resultI64(values[0])
	if parseErr != nil {
		return bound, nil, ErrRedeemFailed
	}
	switch code {
	case -2:
		return bound, nil, session.ErrActivationSuperseded
	case -3:
		return bound, nil, ErrRedeemFailed
	case 1, 2:
		return parseActivation(values, route)
	default:
		return bound, nil, ErrRedeemFailed
	}
}

func redisTTLMilliseconds(value time.Duration) (int64, error) {
	if value <= 0 {
		return 0, errors.New("ticket: TTL must be positive")
	}
	milliseconds := value.Milliseconds()
	if milliseconds == 0 {
		milliseconds = 1
	}
	return milliseconds, nil
}

func (r *RedisStore) key(token string, expectedPlayerID uint64) (string, error) {
	playerID, err := PlayerIDFromToken(token)
	if err != nil {
		// Legacy ticket keys remain readable during a rolling local upgrade.
		if ValidTokenFormat(token) {
			return rediskey.ConnectionTicket(Hash(token)), nil
		}
		return "", err
	}
	if expectedPlayerID != 0 && playerID != expectedPlayerID {
		return "", ErrInvalidTicket
	}
	return rediskey.ConnectionTicketForPlayer(playerID, Hash(token)), nil
}

func parseActivation(values []interface{}, route session.RouteFence) (Bound, *session.ActivationResult, error) {
	if len(values) != 18 {
		return Bound{}, nil, ErrRedeemFailed
	}
	playerID, err := resultU64(values[1])
	if err != nil {
		return Bound{}, nil, ErrRedeemFailed
	}
	authorityEpoch, err := resultU64(values[3])
	if err != nil {
		return Bound{}, nil, ErrRedeemFailed
	}
	reserved, err := resultU64(values[4])
	if err != nil {
		return Bound{}, nil, ErrRedeemFailed
	}
	issuedMS, _ := resultI64(values[5])
	expiresMS, _ := resultI64(values[6])
	revision, err := resultU64(values[15])
	if err != nil {
		return Bound{}, nil, ErrRedeemFailed
	}
	leaseMS, err := resultI64(values[16])
	if err != nil {
		return Bound{}, nil, ErrRedeemFailed
	}
	acceptedTTLMS, err := resultI64(values[17])
	if err != nil || acceptedTTLMS <= 0 {
		return Bound{}, nil, ErrRedeemFailed
	}
	bound := Bound{
		PlayerID: playerID, SessionID: resultString(values[2]),
		SessionAuthorityEpoch: authorityEpoch, ReservedSessionEpoch: reserved,
		IssuedAt: time.UnixMilli(issuedMS), ExpiresAt: time.UnixMilli(expiresMS),
	}
	sessionView := &session.Session{
		PlayerID: playerID, SessionID: bound.SessionID, AuthorityEpoch: authorityEpoch,
		ActiveSessionEpoch: reserved, GatewayID: route.GatewayID,
		ExpiresAt: time.Now().Add(session.DefaultSessionTTL),
	}
	activeRoute := session.ActiveRoute{
		PlayerID: playerID, SessionID: bound.SessionID, SessionEpoch: reserved,
		RouteRevision: revision, LeaseExpiresAt: time.UnixMilli(leaseMS), RouteFence: route,
	}
	oldGeneration, _ := resultU64(values[10])
	oldEpoch, _ := resultU64(values[12])
	oldRevision, _ := resultU64(values[13])
	oldLeaseMS, _ := resultI64(values[14])
	var oldRoute *session.ActiveRoute
	if oldGate := resultString(values[7]); oldGate != "" && oldEpoch != 0 {
		oldRoute = &session.ActiveRoute{
			PlayerID: playerID, SessionID: resultString(values[11]), SessionEpoch: oldEpoch,
			RouteRevision: oldRevision, LeaseExpiresAt: time.UnixMilli(oldLeaseMS),
			RouteFence: session.RouteFence{
				GatewayID: oldGate, GatewayBootID: resultString(values[8]),
				ConnectionID: resultString(values[9]), ConnectionGeneration: oldGeneration,
			},
		}
	}
	return bound, &session.ActivationResult{
		Session: sessionView, Route: activeRoute, OldRoute: oldRoute,
		AcceptedLeaseTTL: time.Duration(acceptedTTLMS) * time.Millisecond,
	}, nil
}

func resultString(value interface{}) string {
	if got, ok := redisResultString(value); ok {
		return got
	}
	return fmt.Sprint(value)
}

func resultU64(value interface{}) (uint64, error) {
	var result uint64
	_, err := fmt.Sscan(resultString(value), &result)
	return result, err
}

func resultI64(value interface{}) (int64, error) {
	var result int64
	_, err := fmt.Sscan(resultString(value), &result)
	return result, err
}

func parseRedeemResult(res interface{}) (Bound, error) {
	arr, ok := res.([]interface{})
	if !ok || len(arr) != 6 {
		return Bound{}, ErrRedeemFailed
	}
	b := Bound{}
	if v, ok := redisResultString(arr[0]); ok {
		if _, err := fmt.Sscan(v, &b.PlayerID); err != nil {
			return Bound{}, ErrRedeemFailed
		}
	}
	if v, ok := redisResultString(arr[1]); ok {
		b.SessionID = v
	}
	if v, ok := redisResultString(arr[2]); ok {
		if _, err := fmt.Sscan(v, &b.SessionAuthorityEpoch); err != nil {
			return Bound{}, ErrRedeemFailed
		}
	}
	if v, ok := redisResultString(arr[3]); ok {
		if _, err := fmt.Sscan(v, &b.ActiveSessionEpoch); err != nil {
			return Bound{}, ErrRedeemFailed
		}
	}
	var issuedMs, expMs int64
	if v, ok := redisResultString(arr[4]); ok {
		if _, err := fmt.Sscan(v, &issuedMs); err != nil {
			return Bound{}, ErrRedeemFailed
		}
	}
	if v, ok := redisResultString(arr[5]); ok {
		if _, err := fmt.Sscan(v, &expMs); err != nil {
			return Bound{}, ErrRedeemFailed
		}
	}
	if b.PlayerID == 0 || b.SessionID == "" || b.SessionAuthorityEpoch == 0 ||
		b.ActiveSessionEpoch == 0 || issuedMs == 0 || expMs == 0 {
		return Bound{}, ErrRedeemFailed
	}
	b.IssuedAt = time.UnixMilli(issuedMs)
	b.ExpiresAt = time.UnixMilli(expMs)
	return b, nil
}

func redisResultString(value interface{}) (string, bool) {
	switch typed := value.(type) {
	case string:
		return typed, true
	case []byte:
		return string(typed), true
	default:
		return "", false
	}
}
