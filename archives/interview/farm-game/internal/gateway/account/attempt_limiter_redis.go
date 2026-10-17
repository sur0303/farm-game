package account

import (
	"context"
	"errors"
	"strconv"
	"time"

	"farmgame/internal/gateway/redisscript"
	"farmgame/internal/observability/redisworkload"

	"github.com/redis/go-redis/v9"
)

// Redis TIME is used instead of Gate wall clocks so all Gate instances refill
// the same bucket against one time authority. Each script touches one key;
// account/IP/device keys therefore remain distributable across Cluster slots
// instead of creating one global auth hot slot.
const consumeAttemptRedisScriptName = "consume_login_attempt_v1"

const consumeAttemptRedisScript = `
local capacity = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local redis_time = redis.call('TIME')
local now_ms = tonumber(redis_time[1]) * 1000 + math.floor(tonumber(redis_time[2]) / 1000)
local state = redis.call('HMGET', KEYS[1], 'tokens', 'updated_at_ms')
local tokens = tonumber(state[1])
local updated_at_ms = tonumber(state[2])
if tokens == nil or updated_at_ms == nil then
  tokens = capacity
  updated_at_ms = now_ms
end
local elapsed_ms = now_ms - updated_at_ms
if elapsed_ms < 0 then elapsed_ms = 0 end
tokens = math.min(capacity, tokens + elapsed_ms * capacity / window_ms)
local allowed = 0
local retry_after_ms = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
else
  retry_after_ms = math.ceil((1 - tokens) * window_ms / capacity)
end
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'updated_at_ms', now_ms)
redis.call('PEXPIRE', KEYS[1], math.ceil(window_ms * 2))
return {allowed, retry_after_ms}
`

type RedisAttemptLimiter struct {
	client  redis.UniversalClient
	scripts *redisscript.Registry
	cfg     AttemptLimiterConfig
}

func NewRedisAttemptLimiter(client redis.UniversalClient, cfg AttemptLimiterConfig) (*RedisAttemptLimiter, error) {
	if client == nil {
		return nil, errors.New("account: Redis attempt limiter client required")
	}
	if err := validateAttemptLimiterConfig(&cfg); err != nil {
		return nil, err
	}
	scripts, err := redisscript.New(client, redisscript.Definition{
		Name: consumeAttemptRedisScriptName, Source: consumeAttemptRedisScript,
	})
	if err != nil {
		return nil, err
	}
	return &RedisAttemptLimiter{client: client, scripts: scripts, cfg: cfg}, nil
}

// EnsureScriptsLoaded preloads the rate-limit script on every Redis primary so
// the three independent login dimensions use compact EVALSHA in steady state.
func (l *RedisAttemptLimiter) EnsureScriptsLoaded(ctx context.Context) error {
	ctx = redisworkload.WithComponent(ctx, redisworkload.ComponentGateAccount)
	if l == nil || l.scripts == nil {
		return errors.New("account: Redis attempt limiter script registry unavailable")
	}
	return l.scripts.EnsureLoaded(ctx)
}

type redisAttemptCommand struct {
	dimension string
	key       string
	burst     int
	windowMS  int64
	command   *redis.Cmd
}

func (l *RedisAttemptLimiter) Allow(ctx context.Context, attempt LoginAttempt) (AttemptDecision, error) {
	if l == nil || l.client == nil {
		return AttemptDecision{}, errors.New("account: Redis attempt limiter unavailable")
	}
	ctx = redisworkload.WithComponent(ctx, redisworkload.ComponentGateAccount)
	pipe := l.client.Pipeline()
	commands := make([]redisAttemptCommand, 0, 3)
	for _, dimension := range attemptDimensions(attempt, l.cfg) {
		if dimension.identifier == "" || dimension.policy.Burst == 0 {
			continue
		}
		// The dimensions are independent risk buckets, so one physical pipeline
		// is safe. Queue only EVALSHA; a NOSCRIPT item is definitely not executed
		// and is the only item eligible for an exact-source fallback below.
		key := attemptKey(l.cfg, dimension.name, dimension.identifier)
		windowMS := dimension.policy.Window.Milliseconds()
		commands = append(commands, redisAttemptCommand{
			dimension: dimension.name, key: key,
			burst: dimension.policy.Burst, windowMS: windowMS,
			command: l.scripts.EvalSha(ctx, pipe, consumeAttemptRedisScriptName,
				[]string{key}, dimension.policy.Burst, windowMS),
		})
	}
	if len(commands) == 0 {
		return AttemptDecision{Allowed: true}, nil
	}
	_, execErr := pipe.Exec(ctx)
	decision := AttemptDecision{Allowed: true}
	for _, command := range commands {
		value, err := command.command.Result()
		if redisscript.IsNoScript(err) {
			value, err = l.scripts.Eval(ctx, consumeAttemptRedisScriptName,
				[]string{command.key}, command.burst, command.windowMS).Result()
		}
		values, ok := value.([]interface{})
		if err != nil || !ok || len(values) != 2 {
			if err == nil {
				err = errors.New("account: malformed Redis attempt limit result")
			}
			return AttemptDecision{}, err
		}
		allowed, err := redisAttemptInt64(values[0])
		if err != nil {
			return AttemptDecision{}, err
		}
		retryAfterMS, err := redisAttemptInt64(values[1])
		if err != nil {
			return AttemptDecision{}, err
		}
		if allowed != 1 {
			decision.Allowed = false
			decision.LimitedBy = append(decision.LimitedBy, command.dimension)
			retryAfter := time.Duration(retryAfterMS) * time.Millisecond
			if retryAfter > decision.RetryAfter {
				decision.RetryAfter = retryAfter
			}
		}
	}
	if execErr != nil && !redisscript.IsNoScript(execErr) {
		return AttemptDecision{}, execErr
	}
	return decision, nil
}

func redisAttemptInt64(value any) (int64, error) {
	switch typed := value.(type) {
	case int64:
		return typed, nil
	case string:
		return strconv.ParseInt(typed, 10, 64)
	case []byte:
		return strconv.ParseInt(string(typed), 10, 64)
	default:
		return 0, errors.New("account: malformed Redis attempt limit number")
	}
}

func (l *RedisAttemptLimiter) ResetAccount(ctx context.Context, username string) error {
	if l == nil || l.client == nil {
		return nil
	}
	ctx = redisworkload.WithComponent(ctx, redisworkload.ComponentGateAccount)
	key := attemptKey(l.cfg, "account", normalizeRiskUsername(username))
	if key == "" {
		return nil
	}
	return l.client.Del(ctx, key).Err()
}

// The Redis client is shared with sessions, tickets and credentials and is
// owned by gatewayapp. Closing the limiter must not close that shared client.
func (l *RedisAttemptLimiter) Close() error { return nil }
