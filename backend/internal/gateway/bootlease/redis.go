package bootlease

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"farmgame/internal/observability/redisworkload"

	"github.com/redis/go-redis/v9"
)

const DefaultRedisPrefix = "farm:v03:gate-boot-lease"

type RedisStore struct {
	client redis.Cmdable
	prefix string
	now    func() time.Time
}

func NewRedisStore(client redis.Cmdable, prefix string) (*RedisStore, error) {
	if client == nil {
		return nil, errors.New("gate boot lease: nil redis client")
	}
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = DefaultRedisPrefix
	}
	return &RedisStore{client: client, prefix: strings.TrimSuffix(prefix, ":"), now: time.Now}, nil
}

// key hashes externally configured identifiers. The gateway hash tag keeps
// all boot generations for one gateway colocated without accepting Redis key
// syntax from configuration.
func (s *RedisStore) key(fence Fence) string {
	gatewayHash := sha256.Sum256([]byte(fence.GatewayID))
	bootHash := sha256.Sum256([]byte(fence.BootID))
	return s.prefix + ":{" + hex.EncodeToString(gatewayHash[:8]) + "}:" + hex.EncodeToString(bootHash[:16])
}

func (s *RedisStore) Renew(ctx context.Context, fence Fence, ttl time.Duration) (Record, error) {
	ctx = redisworkload.WithComponent(ctx, redisworkload.ComponentGateBootLease)
	if !fence.Valid() || ttl <= 0 {
		return Record{}, ErrInvalidFence
	}
	expiresAt := s.now().Add(ttl)
	// The value is diagnostic only. Redis TTL is the liveness authority.
	if err := s.client.Set(ctx, s.key(fence), expiresAt.UnixMilli(), ttl).Err(); err != nil {
		return Record{}, err
	}
	return Record{Fence: fence, ExpiresAt: expiresAt}, nil
}

func (s *RedisStore) Alive(ctx context.Context, fence Fence) (bool, error) {
	ctx = redisworkload.WithComponent(ctx, redisworkload.ComponentGateBootLease)
	if !fence.Valid() {
		return false, ErrInvalidFence
	}
	count, err := s.client.Exists(ctx, s.key(fence)).Result()
	return count > 0, err
}
