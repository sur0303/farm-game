package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const cleanupTimeout = 30 * time.Second

// deleteOwnedSessionKeyScript atomically deletes a Session hash only when both
// the fixture ownership marker (route_connection_id) and generation still match.
// Wrong type, missing key, missing fields, or any foreign replacement => skip.
var deleteOwnedSessionKeyScript = redis.NewScript(`
local keyType = redis.call('TYPE', KEYS[1])
if type(keyType) == 'table' then
  keyType = keyType['ok']
end
if keyType ~= 'hash' then
  return 0
end
local marker = redis.call('HGET', KEYS[1], 'route_connection_id')
local generation = redis.call('HGET', KEYS[1], 'route_connection_generation')
if marker == false or generation == false then
  return 0
end
if marker ~= ARGV[1] or tostring(generation) ~= tostring(ARGV[2]) then
  return 0
end
return redis.call('DEL', KEYS[1])
`)

// ownedSessionKey is a Session hash this run created. Cleanup may DEL it only
// when the stored ownership marker/generation still match this run's fixture.
type ownedSessionKey struct {
	Key        string
	Marker     string
	Generation string
}

// exactKeyTracker records every Session key created by a probe run. Cleanup may
// delete only those exact keys — never SCAN, FLUSH*, namespace wildcards, or the
// global SessionAuthorityEpoch key.
type exactKeyTracker struct {
	mu   sync.Mutex
	keys map[string]ownedSessionKey
	// beforeAtomicDelete, when set, runs after the owned snapshot is prepared
	// and immediately before each atomic compare-and-delete. Tests only.
	beforeAtomicDelete func(ownedSessionKey)
}

func newExactKeyTracker() *exactKeyTracker {
	return &exactKeyTracker{keys: make(map[string]ownedSessionKey)}
}

func (t *exactKeyTracker) TrackOwned(key, marker, generation string) {
	if t == nil || key == "" || marker == "" || generation == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.keys[key] = ownedSessionKey{Key: key, Marker: marker, Generation: generation}
}

func (t *exactKeyTracker) Keys() []string {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]string, 0, len(t.keys))
	for key := range t.keys {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func (t *exactKeyTracker) owned() []ownedSessionKey {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]ownedSessionKey, 0, len(t.keys))
	for _, item := range t.keys {
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

func (t *exactKeyTracker) Cleanup(ctx context.Context, client redis.UniversalClient) error {
	owned := t.owned()
	if len(owned) == 0 {
		return nil
	}
	var errs []error
	for _, item := range owned {
		if hook := t.beforeAtomicDelete; hook != nil {
			hook(item)
		}
		if err := compareAndDeleteOwnedSessionKey(ctx, client, item); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func compareAndDeleteOwnedSessionKey(ctx context.Context, client redis.UniversalClient, item ownedSessionKey) error {
	if item.Key == "" || item.Marker == "" || item.Generation == "" {
		return nil
	}
	if _, err := deleteOwnedSessionKeyScript.Run(ctx, client, []string{item.Key}, item.Marker, item.Generation).Result(); err != nil {
		return fmt.Errorf("atomic ownership delete %s: %w", item.Key, err)
	}
	return nil
}

// cleanupExactKeys always uses a fresh bounded background context so cleanup
// still runs after the probe context is cancelled or past its deadline.
func cleanupExactKeys(deps *probeDeps) error {
	if deps == nil || deps.tracker == nil || deps.client == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	return deps.tracker.Cleanup(ctx, deps.client)
}

func withCleanup(deps *probeDeps, report probeReport, err error) (probeReport, error) {
	cleanupErr := cleanupExactKeys(deps)
	switch {
	case err != nil && cleanupErr != nil:
		return report, errors.Join(err, fmt.Errorf("exact-key cleanup: %w", cleanupErr))
	case cleanupErr != nil:
		return report, fmt.Errorf("exact-key cleanup: %w", cleanupErr)
	default:
		return report, err
	}
}

func formatGeneration(generation uint64) string {
	return strconv.FormatUint(generation, 10)
}
