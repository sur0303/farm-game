package authority

import (
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
)

func fenceTestAuthority(hashVersion uint32, hashSeed uint64, rev, writer, lease uint64) authoritystore.ShardWriteAuthority {
	return authoritystore.ShardWriteAuthority{
		AuthorityLayoutVersion: "v1",
		HashVersion:            hashVersion,
		HashSeed:               hashSeed,
		Domain:                 actorrouting.DomainPlayer,
		VirtualShard:           0,
		HolderID:               "holder-2",
		HolderBootID:           "boot-2",
		AssignmentRevision:     rev,
		WriterEpoch:            writer,
		LeaseEpoch:             lease,
		State:                  authoritystore.ShardAuthorityActive,
		LeaseUntilMs:           time.Now().Add(time.Hour).UnixMilli(),
	}
}

func installFenceTestGuard(t *testing.T, registry *LeaseGuardRegistry, auth authoritystore.ShardWriteAuthority) {
	t.Helper()
	guard, err := authoritystore.NewHolderLeaseGuard("holder-2", "boot-2", time.Second, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	result := authoritystore.ShardAuthorityTransitionResult{
		Status:        authoritystore.ShardTransitionApplied,
		Authority:     auth,
		AcceptedTTLms: 30000,
	}
	if _, err := InstallHolderLeaseGrant(registry, actorrouting.DomainPlayer, 0, guard, time.Now(), result, time.Now); err != nil {
		t.Fatal(err)
	}
}

// TestSameEpochButDifferentHashIsNotSameGeneration proves F4: identical epochs
// with a different hash profile are a different generation identity.
func TestSameEpochButDifferentHashIsNotSameGeneration(t *testing.T) {
	a := fenceTestAuthority(1, 100, 1, 1, 1)
	b := fenceTestAuthority(1, 200, 1, 1, 1)
	if authorityGenerationEqual(a, b) {
		t.Fatal("same epochs with a different hash seed must not be the same generation")
	}
	c := fenceTestAuthority(9, 100, 1, 1, 1)
	if authorityGenerationEqual(a, c) {
		t.Fatal("same epochs with a different hash version must not be the same generation")
	}
	d := fenceTestAuthority(1, 100, 1, 1, 2)
	if authorityGenerationEqual(a, d) {
		t.Fatal("a different lease epoch must not be the same generation")
	}
	if !authorityGenerationEqual(a, fenceTestAuthority(1, 100, 1, 1, 1)) {
		t.Fatal("identical authorities must be equal")
	}
}

// TestFenceDoesNotMatchDifferentHashVersion proves that a fence with a
// different hash version is rejected and never fences the installed guard.
func TestFenceDoesNotMatchDifferentHashVersion(t *testing.T) {
	registry := NewLeaseGuardRegistry()
	installFenceTestGuard(t, registry, fenceTestAuthority(1, 100, 1, 1, 1))

	if err := registry.FenceIfMatches(actorrouting.DomainPlayer, 0, fenceTestAuthority(9, 100, 1, 1, 1)); err == nil {
		t.Fatal("fence with a different hash version must be rejected")
	}
	snapshot, ok := registry.Snapshot(actorrouting.DomainPlayer, 0)
	if !ok || !snapshot.Admitted {
		t.Fatal("installed guard must stay admitted after a rejected fence")
	}
}

// TestRemoveDoesNotMatchDifferentHashSeed proves that a remove with a
// different hash seed is rejected and never deletes the installed guard.
func TestRemoveDoesNotMatchDifferentHashSeed(t *testing.T) {
	registry := NewLeaseGuardRegistry()
	installFenceTestGuard(t, registry, fenceTestAuthority(1, 100, 1, 1, 1))

	if err := registry.RemoveIfMatches(actorrouting.DomainPlayer, 0, fenceTestAuthority(1, 999, 1, 1, 1)); err == nil {
		t.Fatal("remove with a different hash seed must be rejected")
	}
	snapshot, ok := registry.Snapshot(actorrouting.DomainPlayer, 0)
	if !ok || !snapshot.Admitted {
		t.Fatal("installed guard must survive a rejected remove")
	}
}

// TestSameGenerationRefreshRequiresExactFence proves that refreshing an
// existing generation requires the exact generation identity (including hash);
// a same-epoch/different-hash install is rejected and no second guard is
// created.
func TestSameGenerationRefreshRequiresExactFence(t *testing.T) {
	registry := NewLeaseGuardRegistry()
	installFenceTestGuard(t, registry, fenceTestAuthority(1, 100, 1, 1, 1))

	guard, err := authoritystore.NewHolderLeaseGuard("holder-2", "boot-2", time.Second, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	result := authoritystore.ShardAuthorityTransitionResult{
		Status:        authoritystore.ShardTransitionApplied,
		Authority:     fenceTestAuthority(1, 999, 1, 1, 1),
		AcceptedTTLms: 30000,
	}
	if _, err := registry.InstallIfMatches(actorrouting.DomainPlayer, 0, fenceTestAuthority(1, 999, 1, 1, 1), guard, time.Now(), result, time.Now); err == nil {
		t.Fatal("install with a different hash must be rejected")
	}
	if len(registry.SnapshotAll()) != 1 {
		t.Fatalf("guards=%d, want 1", len(registry.SnapshotAll()))
	}
	snapshot, ok := registry.Snapshot(actorrouting.DomainPlayer, 0)
	if !ok || snapshot.Authority.HashSeed != 100 {
		t.Fatalf("installed guard hash must stay unchanged, got %+v", snapshot.Authority)
	}
}
