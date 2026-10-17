package scaleplan

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"time"

	"farmgame/internal/actorrouting"
	core "farmgame/internal/scaleplan"
)

type FakePlacements struct {
	mu                     sync.Mutex
	values                 map[placementKey]core.ShardPlacement
	revision               uint64
	shardSetCaptures       int
	invalidateNextValidate bool
}

type placementKey struct {
	domain actorrouting.RouteDomain
	shard  uint32
}

func NewFakePlacements(values ...core.ShardPlacement) *FakePlacements {
	f := &FakePlacements{values: make(map[placementKey]core.ShardPlacement), revision: 1}
	for _, value := range values {
		if value.SnapshotRevision > f.revision {
			f.revision = value.SnapshotRevision
		}
		f.values[placementKey{domain: value.Domain, shard: value.VirtualShard}] = value
	}
	f.installRevisionLocked()
	return f
}

func (f *FakePlacements) CaptureShard(ctx context.Context, domain actorrouting.RouteDomain, shard uint32) (core.ShardPlacementCapture, error) {
	if err := fakeContextErr(ctx); err != nil {
		return core.ShardPlacementCapture{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	value, found := f.values[placementKey{domain: domain, shard: shard}]
	return core.ShardPlacementCapture{Token: f.tokenLocked(), Placement: value, Found: found}, nil
}

func (f *FakePlacements) CaptureShards(ctx context.Context, keys []core.ShardKey) (core.ShardSetCapture, error) {
	if err := fakeContextErr(ctx); err != nil {
		return core.ShardSetCapture{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.shardSetCaptures++
	out := make([]core.ShardPlacement, 0, len(keys))
	seen := make(map[placementKey]struct{}, len(keys))
	for _, requested := range keys {
		key := placementKey{domain: requested.Domain, shard: requested.VirtualShard}
		if _, duplicate := seen[key]; duplicate {
			return core.ShardSetCapture{}, fmt.Errorf("%w: duplicate requested shard", core.ErrInvalidPlan)
		}
		seen[key] = struct{}{}
		value, found := f.values[key]
		if !found {
			return core.ShardSetCapture{}, fmt.Errorf("%w: requested shard missing", core.ErrConflict)
		}
		out = append(out, value)
	}
	return core.ShardSetCapture{Token: f.tokenLocked(), Placements: out}, nil
}

func (f *FakePlacements) CaptureHolderShards(ctx context.Context, holder core.HolderRef) (core.HolderShardCapture, error) {
	if err := fakeContextErr(ctx); err != nil {
		return core.HolderShardCapture{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]core.ShardPlacement, 0)
	for _, value := range f.values {
		if value.Active && value.Holder == holder {
			out = append(out, value)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Domain != out[j].Domain {
			return out[i].Domain < out[j].Domain
		}
		return out[i].VirtualShard < out[j].VirtualShard
	})
	return core.HolderShardCapture{Token: f.tokenLocked(), Holder: holder, Placements: out}, nil
}

func (f *FakePlacements) Revalidate(ctx context.Context, token core.PlacementCaptureToken) error {
	if err := fakeContextErr(ctx); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.invalidateNextValidate {
		f.invalidateNextValidate = false
		return fmt.Errorf("%w: injected placement capture change", core.ErrConflict)
	}
	if token != f.tokenLocked() {
		return fmt.Errorf("%w: placement capture changed", core.ErrConflict)
	}
	return nil
}

func (f *FakePlacements) Set(value core.ShardPlacement) {
	f.mu.Lock()
	f.revision++
	f.values[placementKey{domain: value.Domain, shard: value.VirtualShard}] = value
	f.installRevisionLocked()
	f.mu.Unlock()
}

func (f *FakePlacements) Move(domain actorrouting.RouteDomain, shard uint32, target core.HolderRef, endpoint string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := placementKey{domain: domain, shard: shard}
	value, found := f.values[key]
	if !found {
		return false
	}
	value.Holder = target
	value.Endpoint = endpoint
	value.ExpectedSourceFence.ExpectedHolderID = target.HolderID
	value.ExpectedSourceFence.ExpectedHolderBootID = target.BootID
	value.ExpectedSourceFence.ExpectedAssignmentRevision++
	value.ExpectedSourceFence.ExpectedWriterEpoch++
	value.ExpectedSourceFence.ExpectedLeaseEpoch++
	f.revision++
	f.values[key] = value
	f.installRevisionLocked()
	return true
}

func (f *FakePlacements) ShardSetCaptureCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.shardSetCaptures
}

func (f *FakePlacements) InvalidateNextRevalidate() {
	f.mu.Lock()
	f.invalidateNextValidate = true
	f.mu.Unlock()
}

// tokenLocked derives every field from the current placement set rather than
// from a counter. That distinction is the whole point of the renew tests: a
// token stamped with a bump counter would change whenever anything touched the
// store, including a renew, and would therefore "pass" the liveness exemption
// tests for the wrong reason.
func (f *FakePlacements) tokenLocked() core.PlacementCaptureToken {
	keys := make([]placementKey, 0, len(f.values))
	for key := range f.values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].domain != keys[j].domain {
			return keys[i].domain < keys[j].domain
		}
		return keys[i].shard < keys[j].shard
	})

	var active uint64
	var assignment, fencing uint64
	epochs := sha256.New()
	for _, key := range keys {
		value := f.values[key]
		if value.Active {
			active++
		}
		fence := value.ExpectedSourceFence
		// LeaseUntil is deliberately absent from this digest. It is the one
		// field a same-generation renew moves.
		fmt.Fprintf(epochs, "%s:%d:%d:%s:%s\n", key.domain, key.shard,
			fence.ExpectedLeaseEpoch, fence.ExpectedHolderID, fence.ExpectedHolderBootID)
		assignment += fence.ExpectedAssignmentRevision
		fencing += fence.ExpectedWriterEpoch
	}

	return core.PlacementCaptureToken{
		ID:                       fmt.Sprintf("fake-capture-%d", f.revision),
		Scope:                    "fake-realm/domain-global",
		SnapshotRevision:         f.revision,
		SnapshotChecksum:         fmt.Sprintf("fake-checksum-%d", f.revision),
		TopologyFingerprint:      "fake-topology-v1",
		ActiveCardinality:        active,
		HolderRegistryRevision:   f.revision,
		HolderRegistryChecksum:   hex.EncodeToString(epochs.Sum(nil))[:32],
		AuthorityCaptureRevision: f.revision,
		LeaseEpochDigest:         hex.EncodeToString(epochs.Sum(nil)),
		AssignmentRevision:       assignment,
		FencingEpoch:             fencing,
	}
}

// RenewLease models the holder-side same-generation renew that section 8.6
// exempts from the control gate. It may only push the local deadline forward.
// It must not touch holder identity, writer epoch, lease epoch or assignment
// revision, and it deliberately does not bump f.revision -- a renew is not a
// placement change, and modelling it as one would hide the very bug these
// tests exist to catch.
func (f *FakePlacements) RenewLease(domain actorrouting.RouteDomain, shard uint32, until time.Time) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := placementKey{domain: domain, shard: shard}
	value, found := f.values[key]
	if !found || !until.After(value.LeaseUntil) {
		return false
	}
	value.LeaseUntil = until
	f.values[key] = value
	return true
}

// Unassign models a control-plane placement withdrawal: unlike a renew this is
// a real change and must invalidate any capture bracketing it.
func (f *FakePlacements) Unassign(domain actorrouting.RouteDomain, shard uint32) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := placementKey{domain: domain, shard: shard}
	value, found := f.values[key]
	if !found || !value.Active {
		return false
	}
	value.Active = false
	value.ExpectedSourceFence.ExpectedAssignmentRevision++
	value.ExpectedSourceFence.ExpectedWriterEpoch++
	f.revision++
	f.values[key] = value
	f.installRevisionLocked()
	return true
}

func (f *FakePlacements) installRevisionLocked() {
	for key, value := range f.values {
		value.SnapshotRevision = f.revision
		f.values[key] = value
	}
}

func fakeContextErr(ctx context.Context) error {
	if ctx == nil {
		return core.ErrInvalidPlan
	}
	return ctx.Err()
}
