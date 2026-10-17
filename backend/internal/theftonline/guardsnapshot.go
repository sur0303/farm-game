package theftonline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"farmgame/internal/farm/actor"
	"farmgame/internal/farm/domain"

	"github.com/redis/go-redis/v9"
)

var (
	ErrGuardSnapshotUnavailable = errors.New("theft online: GuardSync snapshot unavailable")
	ErrGuardSnapshotConflict    = errors.New("theft online: GuardSync snapshot conflict")
	ErrGuardSnapshotSuperseded  = errors.New("theft online: GuardSync snapshot superseded")
)

// RedisGuardSnapshotStore is the production read side of GuardSync used by a
// FarmActor activation. Guard snapshots are immutable by version and are kept
// in Redis so a Farm restart cannot silently lose the pet decision input.
// Missing data is the authoritative zero snapshot (no active guard); Redis
// failures are returned so the actor fails closed instead of using memory.
type RedisGuardSnapshotStore struct {
	client redis.UniversalClient
	prefix string
}

// guardSnapshotMonotonicPut compares and writes under one Redis
// linearization point. ARGV: encoded snapshot, incoming guard version.
var guardSnapshotMonotonicPut = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
if not current then
  redis.call('SET', KEYS[1], ARGV[1])
  return {'STORED'}
end
if current == ARGV[1] then
  return {'REPLAY'}
end
local currentVersion = string.match(current, '"GuardVersion":([0-9]+)')
local incomingVersion = tonumber(ARGV[2])
if not currentVersion or not incomingVersion then
  return {'CORRUPT'}
end
currentVersion = tonumber(currentVersion)
if incomingVersion < currentVersion then
  return {'SUPERSEDED'}
end
if incomingVersion == currentVersion then
  return {'CONFLICT'}
end
redis.call('SET', KEYS[1], ARGV[1])
return {'ADVANCED'}
`)

func NewRedisGuardSnapshotStore(client redis.UniversalClient, prefix string) (*RedisGuardSnapshotStore, error) {
	prefix = strings.TrimSuffix(strings.TrimSpace(prefix), ":")
	if client == nil || prefix == "" {
		return nil, fmt.Errorf("%w: Redis client and prefix are required", ErrGuardSnapshotUnavailable)
	}
	return &RedisGuardSnapshotStore{client: client, prefix: prefix}, nil
}

var _ actor.ParticipantGuardSnapshotReader = (*RedisGuardSnapshotStore)(nil)

func (s *RedisGuardSnapshotStore) key(farmID uint64) string {
	return s.prefix + ":farm:" + strconv.FormatUint(farmID, 10)
}

func (s *RedisGuardSnapshotStore) ReadParticipantGuardSnapshot(ctx context.Context, farmID uint64) (domain.GuardSnapshot, error) {
	if s == nil || s.client == nil || farmID == 0 {
		return domain.GuardSnapshot{}, ErrGuardSnapshotUnavailable
	}
	data, err := s.client.Get(ctx, s.key(farmID)).Bytes()
	if errors.Is(err, redis.Nil) {
		return domain.GuardSnapshot{}, nil
	}
	if err != nil {
		return domain.GuardSnapshot{}, fmt.Errorf("%w: Redis GET: %v", ErrGuardSnapshotUnavailable, err)
	}
	var snapshot domain.GuardSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return domain.GuardSnapshot{}, fmt.Errorf("%w: decode: %v", ErrGuardSnapshotUnavailable, err)
	}
	return normalizeGuardSnapshot(snapshot)
}

// Put is used by the GuardSync writer and by the real Redis E2E harness. It
// permits only monotonic version advancement or an exact replay.
func (s *RedisGuardSnapshotStore) Put(ctx context.Context, farmID uint64, snapshot domain.GuardSnapshot) error {
	if s == nil || s.client == nil || farmID == 0 {
		return ErrGuardSnapshotUnavailable
	}
	normalized, err := normalizeGuardSnapshot(snapshot)
	if err != nil {
		return err
	}
	data, err := json.Marshal(normalized)
	if err != nil {
		return fmt.Errorf("%w: encode: %v", ErrGuardSnapshotUnavailable, err)
	}
	reply, err := guardSnapshotMonotonicPut.Run(ctx, s.client, []string{s.key(farmID)}, data,
		strconv.FormatUint(normalized.GuardVersion, 10)).StringSlice()
	if err != nil {
		return fmt.Errorf("%w: Redis atomic put: %v", ErrGuardSnapshotUnavailable, err)
	}
	if len(reply) != 1 {
		return fmt.Errorf("%w: Redis atomic put reply", ErrGuardSnapshotUnavailable)
	}
	switch reply[0] {
	case "STORED", "REPLAY", "ADVANCED":
		return nil
	case "SUPERSEDED":
		return ErrGuardSnapshotSuperseded
	case "CONFLICT":
		return ErrGuardSnapshotConflict
	case "CORRUPT":
		return fmt.Errorf("%w: stored snapshot version", ErrGuardSnapshotUnavailable)
	default:
		return fmt.Errorf("%w: Redis atomic put status %q", ErrGuardSnapshotUnavailable, reply[0])
	}
}

func normalizeGuardSnapshot(snapshot domain.GuardSnapshot) (domain.GuardSnapshot, error) {
	if snapshot.GuardVersion == 0 {
		if snapshot != (domain.GuardSnapshot{}) {
			return domain.GuardSnapshot{}, fmt.Errorf("%w: zero guard version has non-zero fields", ErrGuardSnapshotConflict)
		}
		return snapshot, nil
	}
	if snapshot.SchemaVersion == 0 || snapshot.SnapshotHash == "" ||
		snapshot.SnapshotHash != domain.CanonicalGuardSnapshotHash(snapshot) {
		return domain.GuardSnapshot{}, fmt.Errorf("%w: snapshot hash is invalid", ErrGuardSnapshotConflict)
	}
	if err := domain.ValidateGuardRule(snapshot.Rule); err != nil {
		return domain.GuardSnapshot{}, fmt.Errorf("%w: guard rule: %v", ErrGuardSnapshotConflict, err)
	}
	return snapshot, nil
}
