package friendshipservice

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"farmgame/internal/socialprofile"

	"github.com/redis/go-redis/v9"
)

const createShareProjectionScript = `
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
local tm = redis.call('TIME')
local now = tm[1] * 1000 + math.floor(tm[2] / 1000)
local ttl = tonumber(ARGV[4])
local retention = tonumber(ARGV[5])
redis.call('HSET', KEYS[1],
  'inviter_player_id', ARGV[1], 'inviter_label', ARGV[2],
  'created_at_ms', tostring(now), 'expires_at_ms', tostring(now + ttl),
  'accept_mode', ARGV[3])
redis.call('PEXPIRE', KEYS[1], ttl + retention)
return 1
`

const resolveShareProjectionScript = `
local values = redis.call('HMGET', KEYS[1], 'inviter_player_id', 'inviter_label', 'created_at_ms', 'expires_at_ms')
if not values[1] then return {0} end
local tm = redis.call('TIME')
local now = tm[1] * 1000 + math.floor(tm[2] / 1000)
if tonumber(values[4]) <= now then return {0} end
return {1, values[1], values[2], values[3], values[4]}
`

const alignShareProjectionScript = `
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
redis.call('HSET', KEYS[1], 'created_at_ms', ARGV[1], 'expires_at_ms', ARGV[2])
redis.call('PEXPIREAT', KEYS[1], tonumber(ARGV[2]) + tonumber(ARGV[3]))
return 1
`

// RedisShareProjectionStore retains the frozen public token-to-issuer lookup
// while Friendship ShareIssuer remains canonical. This is not relationship
// authority and performs no FriendPair, Block, Quota, or FriendIndex write.
type RedisShareProjectionStore struct {
	client              redis.UniversalClient
	prefix              string
	ttl                 time.Duration
	projectionRetention time.Duration
}

func NewRedisShareProjectionStore(client redis.UniversalClient, prefix string, ttl time.Duration) (*RedisShareProjectionStore, error) {
	prefix = strings.TrimSuffix(strings.TrimSpace(prefix), ":")
	retention := socialprofile.Current().ShareProjectionRetention
	if client == nil || prefix == "" || ttl <= 0 || retention <= 0 || ttl > time.Duration(1<<63-1)-retention {
		return nil, fmt.Errorf("%w: Redis share projection store", ErrInvalidArgument)
	}
	return &RedisShareProjectionStore{client: client, prefix: prefix, ttl: ttl, projectionRetention: retention}, nil
}

func (s *RedisShareProjectionStore) key(token string) string { return s.prefix + ":token:" + token }

func (s *RedisShareProjectionStore) Create(ctx context.Context, playerID uint64) (string, error) {
	if s == nil || playerID == 0 {
		return "", fmt.Errorf("%w: share player", ErrInvalidArgument)
	}
	for attempt := 0; attempt < 3; attempt++ {
		var raw [24]byte
		if _, err := rand.Read(raw[:]); err != nil {
			return "", err
		}
		token := base64.RawURLEncoding.EncodeToString(raw[:])
		created, err := s.client.Eval(ctx, createShareProjectionScript, []string{s.key(token)},
			strconv.FormatUint(playerID, 10), "农场主", ShareAcceptModeMulti,
			s.ttl.Milliseconds(), s.projectionRetention.Milliseconds()).Int()
		if err != nil {
			return "", err
		}
		if created == 1 {
			return token, nil
		}
	}
	return "", errors.New("friendship service: failed to allocate unique share token")
}

func (s *RedisShareProjectionStore) Resolve(ctx context.Context, token string) (ShareProjection, error) {
	token = strings.TrimSpace(token)
	if s == nil || token == "" {
		return ShareProjection{}, ErrShareLinkInvalid
	}
	values, err := s.client.Eval(ctx, resolveShareProjectionScript, []string{s.key(token)}).Slice()
	if err != nil {
		return ShareProjection{}, err
	}
	if len(values) != 5 || fmt.Sprint(values[0]) != "1" {
		return ShareProjection{}, ErrShareLinkInvalid
	}
	return decodeShareProjection(values[1:])
}

func (s *RedisShareProjectionStore) ResolveForAdmission(ctx context.Context, token string) (ShareProjection, error) {
	token = strings.TrimSpace(token)
	if s == nil || token == "" {
		return ShareProjection{}, ErrShareLinkInvalid
	}
	values, err := s.client.HMGet(ctx, s.key(token),
		"inviter_player_id", "inviter_label", "created_at_ms", "expires_at_ms").Result()
	if err != nil {
		return ShareProjection{}, err
	}
	return decodeShareProjection(values)
}

func (s *RedisShareProjectionStore) AlignAuthorityToken(ctx context.Context, token string, issuedAtMs, expiresAtMs int64) error {
	token = strings.TrimSpace(token)
	if s == nil || token == "" || issuedAtMs <= 0 || expiresAtMs <= issuedAtMs {
		return fmt.Errorf("%w: share authority projection alignment", ErrInvalidArgument)
	}
	aligned, err := s.client.Eval(ctx, alignShareProjectionScript, []string{s.key(token)},
		issuedAtMs, expiresAtMs, s.projectionRetention.Milliseconds()).Int()
	if err != nil {
		return err
	}
	if aligned != 1 {
		return ErrShareLinkInvalid
	}
	return nil
}

func decodeShareProjection(values []any) (ShareProjection, error) {
	if len(values) != 4 || values[0] == nil || values[2] == nil || values[3] == nil {
		return ShareProjection{}, ErrShareLinkInvalid
	}
	playerID, playerErr := strconv.ParseUint(fmt.Sprint(values[0]), 10, 64)
	createdAt, createdErr := strconv.ParseInt(fmt.Sprint(values[2]), 10, 64)
	expiresAt, expiresErr := strconv.ParseInt(fmt.Sprint(values[3]), 10, 64)
	if playerErr != nil || createdErr != nil || expiresErr != nil || playerID == 0 || createdAt <= 0 || expiresAt <= createdAt {
		return ShareProjection{}, ErrShareLinkInvalid
	}
	return ShareProjection{InviterPlayerID: playerID, CreatedAtMs: createdAt, ExpiresAtMs: expiresAt,
		AcceptMode: ShareAcceptModeMulti}, nil
}
