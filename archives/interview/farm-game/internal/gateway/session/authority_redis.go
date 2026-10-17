package session

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"farmgame/internal/gateway/redisscript"
	"farmgame/internal/observability/redisworkload"
	"farmgame/internal/storage/redis/rediskey"
)

// RedisAuthority 是 Authority 的 Redis 实现,代次跨多 gateway 线性化(见 30-redis.md §2/§5)。
// 使用 INCR 原子提升 epoch;session hash 存活跃会话视图并带 TTL。
type epochCacheEntry struct {
	value     uint64
	expiresAt time.Time
}

type RedisAuthority struct {
	cli     redis.UniversalClient
	scripts *redisscript.Registry
	cfg     Config
	// Permit 用一次 HMGET 读取同一玩家的 active/authority epoch 快照，
	// 并刻意绕过可选的按玩家缓存以保留即时顶号/撤销 Fence。
	activeCache    sync.Map // uint64(playerID) -> epochCacheEntry
	authorityCache sync.Map // uint64(playerID) -> epochCacheEntry
	globalEpoch    struct {
		mu     sync.Mutex
		value  atomic.Uint64
		loaded atomic.Bool
	}
}

// Redis Lua numbers are IEEE-754 doubles and cannot preserve uint64 fences.
// Every script that orders or increments an epoch/revision prepends these
// decimal-string helpers and converts only TTL/time values to Lua numbers.
const uint64DecimalLua = `
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

const reserveCandidateScript = uint64DecimalLua + `
local current_active = canonical_u64(redis.call('HGET', KEYS[1], 'active_epoch') or redis.call('HGET', KEYS[1], 'epoch') or '0')
if not current_active then return redis.error_reply('session: malformed active epoch') end
local current_next = canonical_u64(redis.call('HGET', KEYS[1], 'next_epoch') or current_active)
if not current_next then return redis.error_reply('session: malformed next epoch') end
if compare_u64(current_next, current_active) < 0 then current_next = current_active end
local authority = canonical_u64(redis.call('HGET', KEYS[1], 'authority_epoch') or '0')
local configured = canonical_u64(ARGV[1])
if not authority or not configured then return redis.error_reply('session: malformed authority epoch') end
if compare_u64(authority, configured) < 0 then authority = configured end
local reserved = increment_u64(current_next)
if not reserved then return redis.error_reply('session: session epoch overflow') end
redis.call('HSET', KEYS[1],
  'authority_epoch', authority,
  'next_epoch', reserved,
  'active_epoch', current_active)
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return {reserved, current_active, authority}
`

const activateCandidateScript = uint64DecimalLua + `
local authority = canonical_u64(redis.call('HGET', KEYS[1], 'authority_epoch') or '0')
local expected_authority = canonical_u64(ARGV[1])
local active = canonical_u64(redis.call('HGET', KEYS[1], 'active_epoch') or redis.call('HGET', KEYS[1], 'epoch') or '0')
local reserved = canonical_u64(ARGV[2])
if not authority or not expected_authority or not active or not reserved then
  return redis.error_reply('session: malformed activation epoch')
end
if compare_u64(authority, expected_authority) ~= 0 or compare_u64(reserved, active) <= 0 then return {0} end

local old_gate = redis.call('HGET', KEYS[1], 'route_gate_id') or ''
local old_boot = redis.call('HGET', KEYS[1], 'route_gate_boot_id') or ''
local old_connection = redis.call('HGET', KEYS[1], 'route_connection_id') or ''
local old_generation = redis.call('HGET', KEYS[1], 'route_connection_generation') or '0'
local old_session = redis.call('HGET', KEYS[1], 'session_id') or ''
local old_epoch = active
local old_revision = canonical_u64(redis.call('HGET', KEYS[1], 'route_revision') or '0')
local old_lease = redis.call('HGET', KEYS[1], 'route_lease_expire_at_ms') or '0'
if not old_revision then return redis.error_reply('session: malformed route revision') end
local revision = increment_u64(old_revision)
if not revision then return redis.error_reply('session: route revision overflow') end

local accepted_ttl = tonumber(ARGV[8])
local redis_time = redis.call('TIME')
local redis_now_ms = tonumber(redis_time[1]) * 1000 + math.floor(tonumber(redis_time[2]) / 1000)
local lease_expire = redis_now_ms + accepted_ttl
redis.call('HSET', KEYS[1],
  'active_epoch', reserved,
  'epoch', reserved,
  'session_id', ARGV[3],
  'route_gate_id', ARGV[4],
  'route_gate_boot_id', ARGV[5],
  'route_connection_id', ARGV[6],
  'route_connection_generation', ARGV[7],
  'route_revision', revision,
  'route_lease_expire_at_ms', lease_expire)
redis.call('PEXPIRE', KEYS[1], ARGV[9])
return {1, old_gate, old_boot, old_connection, old_generation, old_session, old_epoch, old_revision, old_lease, revision, tostring(lease_expire), tostring(accepted_ttl)}
`

const clearRouteScript = `
if (redis.call('HGET', KEYS[1], 'route_gate_id') or '') ~= ARGV[1] then return 0 end
if (redis.call('HGET', KEYS[1], 'route_gate_boot_id') or '') ~= ARGV[2] then return 0 end
if (redis.call('HGET', KEYS[1], 'route_connection_id') or '') ~= ARGV[3] then return 0 end
if (redis.call('HGET', KEYS[1], 'route_connection_generation') or '0') ~= ARGV[4] then return 0 end
if (redis.call('HGET', KEYS[1], 'active_epoch') or '0') ~= ARGV[5] then return 0 end
if (redis.call('HGET', KEYS[1], 'route_revision') or '0') ~= ARGV[6] then return 0 end
redis.call('HDEL', KEYS[1],
  'route_gate_id', 'route_gate_boot_id', 'route_connection_id',
  'route_connection_generation', 'route_lease_expire_at_ms')
return 1
`

const renewRouteScript = `
if (redis.call('HGET', KEYS[1], 'route_gate_id') or '') ~= ARGV[1] then return {0, 0, 0} end
if (redis.call('HGET', KEYS[1], 'route_gate_boot_id') or '') ~= ARGV[2] then return {0, 0, 0} end
if (redis.call('HGET', KEYS[1], 'route_connection_id') or '') ~= ARGV[3] then return {0, 0, 0} end
if (redis.call('HGET', KEYS[1], 'route_connection_generation') or '0') ~= ARGV[4] then return {0, 0, 0} end
if (redis.call('HGET', KEYS[1], 'active_epoch') or '0') ~= ARGV[5] then return {0, 0, 0} end
if (redis.call('HGET', KEYS[1], 'route_revision') or '0') ~= ARGV[6] then return {0, 0, 0} end
local accepted_ttl = tonumber(ARGV[7])
local redis_time = redis.call('TIME')
local redis_now_ms = tonumber(redis_time[1]) * 1000 + math.floor(tonumber(redis_time[2]) / 1000)
local current_lease_expire = tonumber(redis.call('HGET', KEYS[1], 'route_lease_expire_at_ms') or '0')
if current_lease_expire <= redis_now_ms then
  return {2, 0, tostring(current_lease_expire)}
end
local lease_expire = redis_now_ms + accepted_ttl
redis.call('HSET', KEYS[1], 'route_lease_expire_at_ms', lease_expire)
redis.call('PEXPIRE', KEYS[1], ARGV[8])
return {1, tostring(accepted_ttl), tostring(lease_expire)}
`

const raiseAuthorityEpochScript = uint64DecimalLua + `
local configured = canonical_u64(ARGV[1])
if not configured then return redis.error_reply('session: invalid configured authority epoch') end
local stored_raw = redis.call('GET', KEYS[1])
if not stored_raw then
  redis.call('SET', KEYS[1], configured)
  return configured
end
local stored = canonical_u64(stored_raw)
if not stored then return redis.error_reply('session: malformed stored authority epoch') end
if compare_u64(configured, stored) > 0 then
  redis.call('SET', KEYS[1], configured)
  return configured
end
return stored
`

const revokePlayerScript = uint64DecimalLua + `
local authority = canonical_u64(redis.call('HGET', KEYS[1], 'authority_epoch') or '0')
local configured = canonical_u64(ARGV[1])
local expected = canonical_u64(ARGV[3])
if not authority or not configured or not expected then
  return redis.error_reply('session: malformed revoke authority epoch')
end
if compare_u64(authority, configured) < 0 then authority = configured end
if expected ~= '0' and compare_u64(authority, expected) ~= 0 then return {0, authority} end

local active = canonical_u64(redis.call('HGET', KEYS[1], 'active_epoch') or redis.call('HGET', KEYS[1], 'epoch') or '0')
if not active then return redis.error_reply('session: malformed active epoch') end
local next_epoch = canonical_u64(redis.call('HGET', KEYS[1], 'next_epoch') or active)
if not next_epoch then return redis.error_reply('session: malformed next epoch') end
if compare_u64(next_epoch, active) < 0 then next_epoch = active end

-- Compute every increment before the first write. In particular, either
-- MaxUint64 fence leaves the complete hash and its TTL untouched.
local next_authority = increment_u64(authority)
local next_session = increment_u64(next_epoch)
if not next_authority then return redis.error_reply('session: authority epoch overflow') end
if not next_session then return redis.error_reply('session: session epoch overflow') end
redis.call('HSET', KEYS[1],
  'authority_epoch', next_authority,
  'next_epoch', next_session,
  'active_epoch', next_session,
  'epoch', next_session)
redis.call('HDEL', KEYS[1],
  'session_id', 'route_gate_id', 'route_gate_boot_id', 'route_connection_id',
  'route_connection_generation', 'route_lease_expire_at_ms')
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return {1, next_authority, next_session}
`

const (
	raiseAuthorityEpochScriptName = "raise_authority_epoch_v1"
	reserveCandidateScriptName    = "reserve_candidate_v1"
	activateCandidateScriptName   = "activate_candidate_v1"
	clearRouteScriptName          = "clear_route_v1"
	renewRouteScriptName          = "renew_route_v1"
	revokePlayerScriptName        = "revoke_player_v1"
)

// NewRedis 创建 Redis Authority。
func NewRedis(cli redis.UniversalClient, cfg Config) *RedisAuthority {
	if len(cfg.Secret) == 0 {
		panic("session: Redis authority permit secret is required")
	}
	if cfg.PermitTTL <= 0 {
		cfg.PermitTTL = DefaultPermitTTL
	}
	scripts, err := redisscript.New(cli,
		redisscript.Definition{Name: raiseAuthorityEpochScriptName, Source: raiseAuthorityEpochScript},
		redisscript.Definition{Name: reserveCandidateScriptName, Source: reserveCandidateScript},
		redisscript.Definition{Name: activateCandidateScriptName, Source: activateCandidateScript},
		redisscript.Definition{Name: clearRouteScriptName, Source: clearRouteScript},
		redisscript.Definition{Name: renewRouteScriptName, Source: renewRouteScript},
		redisscript.Definition{Name: revokePlayerScriptName, Source: revokePlayerScript},
	)
	if err != nil {
		panic("session: build Redis script registry: " + err.Error())
	}
	return &RedisAuthority{cli: cli, scripts: scripts, cfg: cfg}
}

// EnsureScriptsLoaded preloads every fixed Gate SessionAuthority script on all
// Redis primaries. Runtime NOSCRIPT recovery remains exact-source and bounded.
func (a *RedisAuthority) EnsureScriptsLoaded(ctx context.Context) error {
	ctx = redisworkload.WithComponent(ctx, redisworkload.ComponentGateSession)
	if a == nil || a.scripts == nil {
		return errors.New("session: Redis script registry unavailable")
	}
	return a.scripts.EnsureLoaded(ctx)
}

func (a *RedisAuthority) cacheTTL() time.Duration {
	if a == nil {
		return 0
	}
	return a.cfg.EpochCacheTTL
}

// InvalidateEpochCache drops cached epoch values for playerID. Called after
// ClientHello activation (Redis ticket store path) and revoke, where the
// underlying session epoch has changed without going through RedisAuthority.
func (a *RedisAuthority) InvalidateEpochCache(playerID uint64) {
	if a == nil {
		return
	}
	a.activeCache.Delete(playerID)
	a.authorityCache.Delete(playerID)
}

func (a *RedisAuthority) loadCached(cache *sync.Map, playerID uint64) (uint64, bool) {
	if ttl := a.cacheTTL(); ttl <= 0 {
		return 0, false
	}
	if existing, ok := cache.Load(playerID); ok {
		entry := existing.(epochCacheEntry)
		if time.Now().Before(entry.expiresAt) {
			return entry.value, true
		}
		cache.Delete(playerID)
	}
	return 0, false
}

func (a *RedisAuthority) storeCached(cache *sync.Map, playerID, value uint64) {
	ttl := a.cacheTTL()
	if ttl <= 0 {
		return
	}
	cache.Store(playerID, epochCacheEntry{value: value, expiresAt: time.Now().Add(ttl)})
}

func (a *RedisAuthority) Reserve(ctx context.Context, playerID uint64, sessionID string) (*Candidate, error) {
	ctx = redisworkload.WithComponent(ctx, redisworkload.ComponentGateSession)
	if playerID == 0 {
		return nil, errors.New("session: player_id required")
	}
	globalAuthority, err := a.ensureAuthorityEpoch(ctx)
	if err != nil {
		return nil, err
	}
	raw, err := a.scripts.Run(ctx, reserveCandidateScriptName, []string{rediskey.Session(playerID)},
		globalAuthority, DefaultSessionTTL.Milliseconds()).Result()
	if err != nil {
		return nil, err
	}
	values, ok := raw.([]interface{})
	if !ok || len(values) != 3 {
		return nil, errors.New("session: malformed reserve result")
	}
	reserved, err := redisU64(values[0])
	if err != nil {
		return nil, err
	}
	active, err := redisU64(values[1])
	if err != nil {
		return nil, err
	}
	authorityEpoch, err := redisU64(values[2])
	if err != nil {
		return nil, err
	}
	if sessionID == "" {
		sessionID = newSessionID()
	}
	return &Candidate{
		PlayerID: playerID, SessionID: sessionID, AuthorityEpoch: authorityEpoch,
		ReservedSessionEpoch: reserved, ActiveSessionEpoch: active,
	}, nil
}

func (a *RedisAuthority) Activate(ctx context.Context, req ActivationRequest) (*ActivationResult, error) {
	ctx = redisworkload.WithComponent(ctx, redisworkload.ComponentGateSession)
	if err := validateActivationRequest(req); err != nil {
		return nil, err
	}
	leaseTTL := req.LeaseTTL
	if leaseTTL <= 0 {
		leaseTTL = DefaultRouteLeaseTTL
	}
	leaseTTLMS := positiveDurationMilliseconds(leaseTTL)
	raw, err := a.scripts.Run(ctx, activateCandidateScriptName, []string{rediskey.Session(req.PlayerID)},
		req.AuthorityEpoch, req.ReservedSessionEpoch, req.SessionID,
		req.Route.GatewayID, req.Route.GatewayBootID, req.Route.ConnectionID,
		req.Route.ConnectionGeneration, leaseTTLMS, DefaultSessionTTL.Milliseconds()).Result()
	if err != nil {
		return nil, err
	}
	values, ok := raw.([]interface{})
	if !ok || len(values) == 0 {
		return nil, errors.New("session: malformed activate result")
	}
	code, err := redisU64(values[0])
	if err != nil || code != 1 {
		return nil, ErrActivationSuperseded
	}
	// active_epoch 已提升,旧缓存立即失效。
	a.activeCache.Delete(req.PlayerID)
	return parseActivationResult(req, values)
}

func (a *RedisAuthority) CompareAndClearRoute(ctx context.Context, expected ActiveRoute) (bool, error) {
	ctx = redisworkload.WithComponent(ctx, redisworkload.ComponentGateSession)
	if expected.PlayerID == 0 {
		return false, nil
	}
	value, err := a.scripts.Run(ctx, clearRouteScriptName, []string{rediskey.Session(expected.PlayerID)},
		expected.GatewayID, expected.GatewayBootID, expected.ConnectionID,
		expected.ConnectionGeneration, expected.SessionEpoch, expected.RouteRevision).Int64()
	return value == 1, err
}

func (a *RedisAuthority) BatchGetActiveRoutes(ctx context.Context, playerIDs []uint64) (map[uint64]ActiveRoute, error) {
	ctx = redisworkload.WithComponent(ctx, redisworkload.ComponentGateSession)
	if a == nil || a.cli == nil {
		return nil, errors.New("session: active route resolver unavailable")
	}
	ids, err := normalizeActiveRouteIDs(playerIDs)
	if err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	routes := make(map[uint64]ActiveRoute, len(ids))
	pipe := a.cli.Pipeline()
	clock := pipe.Time(ctx)
	commands := make([]*redis.MapStringStringCmd, 0, len(ids))
	for _, playerID := range ids {
		commands = append(commands, pipe.HGetAll(ctx, rediskey.Session(playerID)))
	}
	_, execErr := pipe.Exec(ctx)
	redisNow, timeErr := clock.Result()
	if timeErr != nil {
		if execErr != nil {
			return routes, errors.Join(timeErr, execErr)
		}
		return routes, timeErr
	}
	var itemErrors []error
	for index, playerID := range ids {
		fields, commandErr := commands[index].Result()
		if commandErr != nil {
			itemErrors = append(itemErrors, fmt.Errorf("session: read active route %d: %w", playerID, commandErr))
			continue
		}
		route, ok := decodeActiveRouteHash(playerID, fields, redisNow)
		if ok {
			routes[playerID] = route
		}
	}
	if execErr != nil && !errors.Is(execErr, redis.Nil) {
		itemErrors = append(itemErrors, execErr)
	}
	return routes, errors.Join(itemErrors...)
}

func decodeActiveRouteHash(playerID uint64, fields map[string]string, now time.Time) (ActiveRoute, bool) {
	if playerID == 0 || len(fields) == 0 {
		return ActiveRoute{}, false
	}
	routePresent := false
	for _, key := range []string{
		"session_id", "active_epoch", "epoch", "route_gate_id", "route_gate_boot_id",
		"route_connection_id", "route_connection_generation", "route_revision",
		"route_lease_expire_at_ms",
	} {
		if strings.TrimSpace(fields[key]) != "" {
			routePresent = true
			break
		}
	}
	if !routePresent {
		return ActiveRoute{}, false
	}
	epochText := strings.TrimSpace(fields["active_epoch"])
	if epochText == "" {
		epochText = strings.TrimSpace(fields["epoch"])
	}
	sessionEpoch, epochErr := strconv.ParseUint(epochText, 10, 64)
	connectionGeneration, generationErr := strconv.ParseUint(strings.TrimSpace(fields["route_connection_generation"]), 10, 64)
	routeRevision, revisionErr := strconv.ParseUint(strings.TrimSpace(fields["route_revision"]), 10, 64)
	leaseExpiresAtMS, leaseErr := strconv.ParseInt(strings.TrimSpace(fields["route_lease_expire_at_ms"]), 10, 64)
	if epochErr != nil || generationErr != nil || revisionErr != nil || leaseErr != nil {
		return ActiveRoute{}, false
	}
	route := ActiveRoute{
		RouteFence: RouteFence{
			GatewayID:            strings.TrimSpace(fields["route_gate_id"]),
			GatewayBootID:        strings.TrimSpace(fields["route_gate_boot_id"]),
			ConnectionID:         strings.TrimSpace(fields["route_connection_id"]),
			ConnectionGeneration: connectionGeneration,
		},
		PlayerID: playerID, SessionID: strings.TrimSpace(fields["session_id"]),
		SessionEpoch: sessionEpoch, RouteRevision: routeRevision,
		LeaseExpiresAt: time.UnixMilli(leaseExpiresAtMS),
	}
	if !validActiveRoute(route, playerID, now) {
		return ActiveRoute{}, false
	}
	return route, true
}

type renewRouteCommand struct {
	key     string
	args    []interface{}
	command *redis.Cmd
}

func (a *RedisAuthority) RenewRoutes(ctx context.Context, routes []ActiveRoute, leaseTTL time.Duration) ([]RouteRenewal, error) {
	ctx = redisworkload.WithComponent(ctx, redisworkload.ComponentGateSession)
	if leaseTTL <= 0 {
		leaseTTL = DefaultRouteLeaseTTL
	}
	leaseTTLMS := positiveDurationMilliseconds(leaseTTL)
	pipe := a.cli.Pipeline()
	commands := make([]renewRouteCommand, 0, len(routes))
	for _, expected := range routes {
		// Every route has a different ownership fence and can succeed or fail
		// independently, so a physical pipeline is safe. The preloaded steady
		// path sends only the compact SHA instead of the full Lua body per route.
		key := rediskey.Session(expected.PlayerID)
		args := []interface{}{
			expected.GatewayID, expected.GatewayBootID, expected.ConnectionID,
			expected.ConnectionGeneration, expected.SessionEpoch, expected.RouteRevision,
			leaseTTLMS, DefaultSessionTTL.Milliseconds(),
		}
		commands = append(commands, renewRouteCommand{
			key: key, args: args,
			command: a.scripts.EvalSha(ctx, pipe, renewRouteScriptName, []string{key}, args...),
		})
	}
	if len(commands) == 0 {
		return []RouteRenewal{}, nil
	}
	// Exec returns the first command error even though Redis may have produced
	// valid replies for other commands in the same physical pipeline. Parse each
	// command independently. Only a definitely-not-executed NOSCRIPT item is
	// retried with the immutable exact source; successful items are never replayed.
	_, execErr := pipe.Exec(ctx)
	results := make([]RouteRenewal, 0, len(routes))
	itemErrors := make([]error, 0)
	for index, expected := range routes {
		result := RouteRenewal{Route: expected}
		value, err := commands[index].command.Result()
		if redisscript.IsNoScript(err) {
			value, err = a.scripts.Eval(ctx, renewRouteScriptName,
				[]string{commands[index].key}, commands[index].args...).Result()
		}
		if err != nil {
			result.Transient = true
			itemErrors = append(itemErrors, fmt.Errorf("session: renew route result %d: %w", index, err))
			results = append(results, result)
			continue
		}
		values, ok := value.([]interface{})
		if !ok || len(values) != 3 {
			result.Transient = true
			itemErrors = append(itemErrors, fmt.Errorf("session: malformed renew route result %d", index))
			results = append(results, result)
			continue
		}
		code, codeErr := redisU64(values[0])
		acceptedMS, ttlErr := redisI64(values[1])
		expiresMS, expiresErr := redisI64(values[2])
		if codeErr != nil || ttlErr != nil || expiresErr != nil || code > 2 {
			result.Transient = true
			itemErrors = append(itemErrors, fmt.Errorf("session: malformed renew route fields %d", index))
			results = append(results, result)
			continue
		}
		if code == 1 {
			if acceptedMS <= 0 || expiresMS <= 0 {
				result.Transient = true
				itemErrors = append(itemErrors, fmt.Errorf("session: invalid accepted renew ttl %d", index))
				results = append(results, result)
				continue
			}
			result.Renewed = true
			result.AcceptedLeaseTTL = time.Duration(acceptedMS) * time.Millisecond
			result.Route.LeaseExpiresAt = time.UnixMilli(expiresMS)
		} else if code == 2 {
			result.LeaseExpired = true
			if expiresMS > 0 {
				result.Route.LeaseExpiresAt = time.UnixMilli(expiresMS)
			}
		}
		results = append(results, result)
	}
	if len(itemErrors) == 0 && execErr != nil && !errors.Is(execErr, redis.Nil) &&
		!redisscript.IsNoScript(execErr) {
		itemErrors = append(itemErrors, execErr)
	}
	return results, errors.Join(itemErrors...)
}

func (a *RedisAuthority) PlayerAuthorityEpoch(ctx context.Context, playerID uint64) (uint64, error) {
	ctx = redisworkload.WithComponent(ctx, redisworkload.ComponentGateSession)
	if cached, ok := a.loadCached(&a.authorityCache, playerID); ok {
		return cached, nil
	}
	global, err := a.ensureAuthorityEpoch(ctx)
	if err != nil {
		return 0, err
	}
	value, err := a.cli.HGet(ctx, rediskey.Session(playerID), "authority_epoch").Result()
	if errors.Is(err, redis.Nil) {
		a.storeCached(&a.authorityCache, playerID, global)
		return global, nil
	}
	if err != nil {
		return 0, err
	}
	local, err := parseU64(value)
	if err != nil {
		return 0, err
	}
	if local < global {
		a.storeCached(&a.authorityCache, playerID, global)
		return global, nil
	}
	a.storeCached(&a.authorityCache, playerID, local)
	return local, nil
}

// sessionEpochs reads both command-admission fences in one Redis command. A
// single hash snapshot avoids both the redundant per-command HGET and a mixed
// generation assembled across two separately linearized reads. Permit must
// never use the optional per-player cache: a newer Gate activation or account
// revocation has to fence the old connection on its very next command.
func (a *RedisAuthority) sessionEpochs(ctx context.Context, playerID uint64) (uint64, uint64, error) {
	global, err := a.ensureAuthorityEpoch(ctx)
	if err != nil {
		return 0, 0, err
	}
	values, err := a.cli.HMGet(ctx, rediskey.Session(playerID),
		"active_epoch", "epoch", "authority_epoch").Result()
	if err != nil {
		return 0, 0, err
	}
	if len(values) != 3 {
		return 0, 0, errors.New("session: malformed epoch snapshot")
	}
	active := uint64(0)
	activeValue := values[0]
	if activeValue == nil {
		activeValue = values[1]
	}
	if activeValue != nil {
		active, err = redisU64(activeValue)
		if err != nil {
			return 0, 0, err
		}
	}
	authority := global
	if values[2] != nil {
		local, parseErr := redisU64(values[2])
		if parseErr != nil {
			return 0, 0, parseErr
		}
		if local > authority {
			authority = local
		}
	}
	return active, authority, nil
}

func (a *RedisAuthority) Begin(ctx context.Context, playerID uint64, gatewayID string) (*Session, error) {
	candidate, err := a.Reserve(ctx, playerID, "")
	if err != nil {
		return nil, err
	}
	result, err := a.Activate(ctx, ActivationRequest{
		PlayerID: candidate.PlayerID, SessionID: candidate.SessionID,
		AuthorityEpoch: candidate.AuthorityEpoch, ReservedSessionEpoch: candidate.ReservedSessionEpoch,
		Route:    RouteFence{GatewayID: gatewayID, GatewayBootID: "legacy", ConnectionID: newSessionID(), ConnectionGeneration: 1},
		LeaseTTL: DefaultSessionTTL,
	})
	if err != nil {
		return nil, err
	}
	return result.Session, nil
}

func (a *RedisAuthority) ActiveEpoch(ctx context.Context, playerID uint64) (uint64, error) {
	ctx = redisworkload.WithComponent(ctx, redisworkload.ComponentGateSession)
	if cached, ok := a.loadCached(&a.activeCache, playerID); ok {
		return cached, nil
	}
	v, err := a.cli.HGet(ctx, rediskey.Session(playerID), "active_epoch").Result()
	if errors.Is(err, redis.Nil) {
		v, err = a.cli.HGet(ctx, rediskey.Session(playerID), "epoch").Result()
		if errors.Is(err, redis.Nil) {
			a.storeCached(&a.activeCache, playerID, 0)
			return 0, nil
		}
	}
	if err != nil {
		return 0, err
	}
	value, err := parseU64(v)
	if err != nil {
		return 0, err
	}
	a.storeCached(&a.activeCache, playerID, value)
	return value, nil
}

func (a *RedisAuthority) AuthorityEpoch(ctx context.Context) (uint64, error) {
	ctx = redisworkload.WithComponent(ctx, redisworkload.ComponentGateSession)
	return a.ensureAuthorityEpoch(ctx)
}

func (a *RedisAuthority) Revoke(ctx context.Context, playerID uint64) error {
	ctx = redisworkload.WithComponent(ctx, redisworkload.ComponentGateSession)
	global, err := a.ensureAuthorityEpoch(ctx)
	if err != nil {
		return err
	}
	if err := a.scripts.Run(ctx, revokePlayerScriptName, []string{rediskey.Session(playerID)},
		global, DefaultSessionTTL.Milliseconds(), 0).Err(); err != nil {
		return err
	}
	a.activeCache.Delete(playerID)
	a.authorityCache.Delete(playerID)
	return nil
}

func (a *RedisAuthority) RevokeIfCurrent(ctx context.Context, playerID, expectedAuthorityEpoch uint64) (bool, error) {
	ctx = redisworkload.WithComponent(ctx, redisworkload.ComponentGateSession)
	if playerID == 0 || expectedAuthorityEpoch == 0 {
		return false, nil
	}
	global, err := a.ensureAuthorityEpoch(ctx)
	if err != nil {
		return false, err
	}
	raw, err := a.scripts.Run(ctx, revokePlayerScriptName, []string{rediskey.Session(playerID)},
		global, DefaultSessionTTL.Milliseconds(), expectedAuthorityEpoch).Result()
	if err != nil {
		return false, err
	}
	values, ok := raw.([]interface{})
	if !ok || len(values) == 0 {
		return false, errors.New("session: malformed revoke result")
	}
	code, err := redisU64(values[0])
	if err != nil {
		return false, err
	}
	if code == 1 {
		a.activeCache.Delete(playerID)
		a.authorityCache.Delete(playerID)
	}
	return code == 1, nil
}

func (a *RedisAuthority) Permit(ctx context.Context, s *Session, req PermitRequest) (*Permit, error) {
	ctx = redisworkload.WithComponent(ctx, redisworkload.ComponentGateSession)
	if req.Action.NeedsOperationBinding() && req.OperationID == "" {
		return nil, errors.New("session: owner_write permit requires operation_id")
	}
	active, authority, err := a.sessionEpochs(ctx, s.PlayerID)
	if err != nil {
		return nil, err
	}
	if s.ActiveSessionEpoch == 0 || s.ActiveSessionEpoch != active || s.AuthorityEpoch != authority {
		return nil, fmt.Errorf("%w: active_epoch=%d want=%d authority_epoch=%d want=%d",
			ErrStaleSession, active, s.ActiveSessionEpoch, authority, s.AuthorityEpoch)
	}
	ttl := a.cfg.PermitTTL
	if req.TTL > 0 {
		ttl = req.TTL
	}
	p := &Permit{
		PlayerID:             s.PlayerID,
		SessionID:            s.SessionID,
		AuthorityEpoch:       s.AuthorityEpoch,
		ActiveSessionEpoch:   s.ActiveSessionEpoch,
		GatewayID:            req.GatewayID,
		GatewayBootID:        req.GatewayBootID,
		ConnectionID:         req.ConnectionID,
		ConnectionGeneration: req.ConnectionGeneration,
		RouteRevision:        req.RouteRevision,
		RequestID:            req.RequestID,
		Action:               req.Action,
		OperationID:          req.OperationID,
		ExpiresAt:            time.Now().Add(ttl),
	}
	p.Sign(a.cfg.Secret)
	return p, nil
}

func validateActivationRequest(req ActivationRequest) error {
	if req.PlayerID == 0 || req.SessionID == "" || req.AuthorityEpoch == 0 || req.ReservedSessionEpoch == 0 ||
		req.Route.GatewayID == "" || req.Route.GatewayBootID == "" ||
		req.Route.ConnectionID == "" || req.Route.ConnectionGeneration == 0 {
		return errors.New("session: invalid activation request")
	}
	return nil
}

func parseActivationResult(req ActivationRequest, values []interface{}) (*ActivationResult, error) {
	if len(values) != 12 {
		return nil, errors.New("session: malformed activate result")
	}
	oldGeneration, _ := redisU64(values[4])
	oldEpoch, _ := redisU64(values[6])
	oldRevision, _ := redisU64(values[7])
	oldLeaseMS, _ := redisI64(values[8])
	revision, err := redisU64(values[9])
	if err != nil {
		return nil, err
	}
	leaseExpiresMS, err := redisI64(values[10])
	if err != nil {
		return nil, err
	}
	acceptedTTLMS, err := redisI64(values[11])
	if err != nil || acceptedTTLMS <= 0 {
		return nil, errors.New("session: malformed accepted lease ttl")
	}
	var oldRoute *ActiveRoute
	oldGate := redisString(values[1])
	oldBoot := redisString(values[2])
	oldConnection := redisString(values[3])
	if oldGate != "" && oldConnection != "" && oldEpoch != 0 {
		oldRoute = &ActiveRoute{
			PlayerID: req.PlayerID, SessionID: redisString(values[5]), SessionEpoch: oldEpoch,
			RouteRevision: oldRevision, LeaseExpiresAt: time.UnixMilli(oldLeaseMS),
			RouteFence: RouteFence{
				GatewayID: oldGate, GatewayBootID: oldBoot, ConnectionID: oldConnection,
				ConnectionGeneration: oldGeneration,
			},
		}
	}
	session := &Session{
		PlayerID: req.PlayerID, SessionID: req.SessionID, AuthorityEpoch: req.AuthorityEpoch,
		ActiveSessionEpoch: req.ReservedSessionEpoch, GatewayID: req.Route.GatewayID,
		ExpiresAt: time.Now().Add(DefaultSessionTTL),
	}
	route := ActiveRoute{
		PlayerID: req.PlayerID, SessionID: req.SessionID, SessionEpoch: req.ReservedSessionEpoch,
		RouteRevision: revision, LeaseExpiresAt: time.UnixMilli(leaseExpiresMS), RouteFence: req.Route,
	}
	return &ActivationResult{
		Session: session, Route: route, OldRoute: oldRoute,
		AcceptedLeaseTTL: time.Duration(acceptedTTLMS) * time.Millisecond,
	}, nil
}

func redisString(value interface{}) string {
	switch typed := value.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return fmt.Sprint(value)
	}
}

func redisU64(value interface{}) (uint64, error) {
	return strconv.ParseUint(redisString(value), 10, 64)
}

func redisI64(value interface{}) (int64, error) {
	return strconv.ParseInt(redisString(value), 10, 64)
}

// ensureAuthorityEpoch 提升并保持全局权威代次不低于配置初值,返回当前值。
// 提升由 Lua 在一个线性化点完成；Redis SET 没有 MAX 选项，读后比较再写也会回退。
// 全局代次是灾备围栏，一次进程里几乎不变，命中后不再打 Redis；
// 不走 EpochCacheTTL（那个只约束按玩家的顶号代次）。
func (a *RedisAuthority) ensureAuthorityEpoch(ctx context.Context) (uint64, error) {
	if a.globalEpoch.loaded.Load() {
		return a.globalEpoch.value.Load(), nil
	}
	value, err := a.loadAndRaiseAuthorityEpoch(ctx)
	if err != nil {
		return 0, err
	}
	a.globalEpoch.mu.Lock()
	if !a.globalEpoch.loaded.Load() {
		a.globalEpoch.value.Store(value)
		a.globalEpoch.loaded.Store(true)
	} else {
		value = a.globalEpoch.value.Load()
	}
	a.globalEpoch.mu.Unlock()
	return value, nil
}

func (a *RedisAuthority) loadAndRaiseAuthorityEpoch(ctx context.Context) (uint64, error) {
	if a.cfg.AuthorityEpoch == 0 {
		v, err := a.cli.Get(ctx, rediskey.SessionAuthorityEpoch()).Result()
		if errors.Is(err, redis.Nil) {
			return 0, nil
		}
		if err != nil {
			return 0, err
		}
		return parseU64(v)
	}
	v, err := a.scripts.Run(ctx, raiseAuthorityEpochScriptName,
		[]string{rediskey.SessionAuthorityEpoch()}, a.cfg.AuthorityEpoch).Text()
	if err != nil {
		return 0, err
	}
	return parseU64(v)
}

func positiveDurationMilliseconds(value time.Duration) int64 {
	milliseconds := value.Milliseconds()
	if milliseconds == 0 {
		milliseconds = 1
	}
	return milliseconds
}

func parseU64(s string) (uint64, error) {
	return strconv.ParseUint(s, 10, 64)
}
