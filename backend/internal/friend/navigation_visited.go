package friend

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const DefaultNavigationVisitedTTL = 30 * time.Minute

type NavigationVisitedRepository interface {
	MarkVisited(context.Context, VisitFence) error
	ListVisited(context.Context, uint64) (map[uint64]struct{}, error)
}

var markNavigationVisitedScript = redis.NewScript(`
local visited_added = redis.call('SADD', KEYS[2], ARGV[1])
local fence_added = redis.call('SADD', KEYS[3], ARGV[2])
if visited_added == 1 or fence_added == 1 then
  redis.call('HINCRBY', KEYS[1], 'version', 1)
end
redis.call('PEXPIRE', KEYS[1], ARGV[3])
redis.call('PEXPIRE', KEYS[2], ARGV[3])
redis.call('PEXPIRE', KEYS[3], ARGV[3])
return visited_added
`)

// RedisNavigationRepository stores the bounded, lossy runtime navigation.
// RedisNavigationVisitedRepository remains an alias for the pre-V3 ingress
// port so existing farm ingress wiring keeps compiling unchanged.
//
// The legacy navigation state key is the single optimistic-CAS fence for the
// whole viewer-local key family. MarkVisited only ever advances the legacy
// version: a visit therefore makes a previously persisted top-k record stale
// (its version lags the legacy fence) and the next top-k reader rebuilds
// instead of trusting a window that contains the visited farm. In full-tail
// mode no top-k key is ever read, written, deleted, expired or incremented.
type RedisNavigationRepository struct {
	client redis.UniversalClient
	keys   FriendshipKeyBuilder
	ttl    time.Duration

	mu         sync.RWMutex
	windowMode NavigationWindowMode
	windowSize int
}

type RedisNavigationVisitedRepository = RedisNavigationRepository

func NewRedisNavigationRepository(client redis.UniversalClient, profile FriendRedisProfile, ttl time.Duration) (*RedisNavigationRepository, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: friend navigation redis client", ErrInvalidCommand)
	}
	keys, err := NewFriendshipKeyBuilder(profile)
	if err != nil {
		return nil, err
	}
	if ttl <= 0 {
		ttl = DefaultNavigationVisitedTTL
	}
	return &RedisNavigationRepository{client: client, keys: keys, ttl: ttl}, nil
}

func NewRedisNavigationVisitedRepository(client redis.UniversalClient, profile FriendRedisProfile, ttl time.Duration) (*RedisNavigationVisitedRepository, error) {
	return NewRedisNavigationRepository(client, profile, ttl)
}

// ConfigureNavigationWindowMode sets the active persisted window strategy for
// this process. Callers that never enable top-k (for example farm-svr's
// visited-mark ingress) keep the full-tail default and never touch the top-k
// record. player-svr configures top-k before serving GET_NEXT when the
// experiment is enabled.
func (r *RedisNavigationRepository) ConfigureNavigationWindowMode(mode NavigationWindowMode, size int) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.windowMode = ParseNavigationWindowMode(string(mode))
	r.windowSize = size
	r.mu.Unlock()
}

func (r *RedisNavigationRepository) topKEnabled() bool {
	if r == nil {
		return false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.windowMode == NavigationWindowModeTopK
}

func (r *RedisNavigationRepository) MarkVisited(ctx context.Context, fence VisitFence) error {
	if r == nil || fence.ViewerPlayerID == 0 || fence.TargetFarmID == 0 || fence.ViewerPlayerID == fence.TargetFarmID ||
		fence.ActiveSessionEpoch == 0 || fence.SubscriptionID == "" {
		return fmt.Errorf("%w: friend navigation VisitFence", ErrInvalidCommand)
	}
	stateKey, err := r.keys.FriendNavigationStateKey(fence.ViewerPlayerID)
	if err != nil {
		return err
	}
	visitedKey, err := r.keys.FriendNavigationVisitedKey(fence.ViewerPlayerID)
	if err != nil {
		return err
	}
	fencesKey, err := r.keys.FriendNavigationFencesKey(fence.ViewerPlayerID)
	if err != nil {
		return err
	}
	fenceMember := strconv.FormatUint(fence.ActiveSessionEpoch, 10) + ":" + fence.SubscriptionID + ":" + strconv.FormatUint(fence.TargetFarmID, 10)
	// Only the legacy version is advanced: it is the single optimistic-CAS
	// fence, and the top-k record becomes stale (never split) on any visit.
	return markNavigationVisitedScript.Run(ctx, r.client, []string{stateKey.String(), visitedKey.String(), fencesKey.String()},
		fence.TargetFarmID, fenceMember, r.ttl.Milliseconds()).Err()
}

func (r *RedisNavigationRepository) ListVisited(ctx context.Context, viewer uint64) (map[uint64]struct{}, error) {
	if r == nil || viewer == 0 {
		return nil, fmt.Errorf("%w: friend navigation viewer", ErrInvalidCommand)
	}
	key, err := r.keys.FriendNavigationVisitedKey(viewer)
	if err != nil {
		return nil, err
	}
	values, err := r.client.SMembers(ctx, key.String()).Result()
	if err != nil {
		return nil, err
	}
	if len(values) > maxVisited {
		return nil, ErrFriendAuthorityCapacity
	}
	visited := make(map[uint64]struct{}, len(values))
	for _, value := range values {
		farmID, parseErr := strconv.ParseUint(value, 10, 64)
		if parseErr != nil || farmID == 0 {
			return nil, ErrFriendReconcile
		}
		visited[farmID] = struct{}{}
	}
	return visited, nil
}
