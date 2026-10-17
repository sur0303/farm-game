package verifiedholder

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/holderkey"
)

func testSnapshot(revision uint64, uid, boot string, now time.Time) WorkloadSnapshot {
	key := WorkloadKey{Realm: "r1", Domain: actorrouting.DomainFarm, Namespace: "farm", Kind: "StatefulSet", Name: "farm-svr"}
	return WorkloadSnapshot{
		Key: key, WorkloadUID: uid, KubeGeneration: 2, KubeResourceVersion: "opaque-rv", KubeListResourceVersion: "opaque-list-rv",
		DiscoveryRevision: revision, CapturedAt: now,
		Holders: []Holder{{
			WorkloadKey: key, WorkloadUID: uid, Domain: actorrouting.DomainFarm,
			HolderID: "farm-svr-1", BootID: boot, PodName: "farm-svr-1", PodUID: "pod-1", Ordinal: 1,
			LogicEndpoint: "198.18.0.199:9000", ControlEndpoint: "198.18.0.199:9001", TLSDNSIdentity: "farm-svr-1",
			Lifecycle: LifecycleReadyZeroShard, ProcessReady: true, CapacityWeight: 1,
			AttestationHash: "sha256:abc", VerifiedAt: now, VerifiedUntil: now.Add(time.Minute),
		}},
	}
}

func TestReplaceWorkloadUsesControllerRevisionNotKubeResourceVersionOrdering(t *testing.T) {
	now := time.Unix(100, 0)
	store := NewStore(func() time.Time { return now })
	first := testSnapshot(1, "workload-old", "boot-old", now)
	first.KubeResourceVersion = "999"
	if err := store.ReplaceWorkload(first); err != nil {
		t.Fatal(err)
	}
	next := testSnapshot(2, "workload-new", "boot-new", now)
	next.KubeResourceVersion = "10"
	if err := store.ReplaceWorkload(next); err != nil {
		t.Fatalf("opaque resourceVersion must not be ordered: %v", err)
	}
	got, found := store.Snapshot(next.Key)
	if !found || got.WorkloadUID != "workload-new" || got.Holders[0].BootID != "boot-new" {
		t.Fatalf("unexpected replacement: %+v", got)
	}
	if err := store.ReplaceWorkload(first); !errors.Is(err, ErrStaleWorkloadSnapshot) {
		t.Fatalf("expected stale revision rejection, got %v", err)
	}
}

func TestListDomainFiltersExpiredAndReturnsCopies(t *testing.T) {
	now := time.Unix(100, 0)
	store := NewStore(func() time.Time { return now })
	snapshot := testSnapshot(1, "workload", "boot", now)
	if err := store.ReplaceWorkload(snapshot); err != nil {
		t.Fatal(err)
	}
	listed := store.ListDomain(actorrouting.DomainFarm)
	if len(listed) != 1 {
		t.Fatalf("expected one holder, got %d", len(listed))
	}
	listed[0].BootID = "mutated"
	again := store.ListDomain(actorrouting.DomainFarm)
	if len(again) != 1 || again[0].BootID != "boot" {
		t.Fatalf("store leaked mutable view: %+v", again)
	}
	now = now.Add(2 * time.Minute)
	if got := store.ListDomain(actorrouting.DomainFarm); len(got) != 0 {
		t.Fatalf("expired holder remained eligible: %+v", got)
	}
}

func TestVerifiedHolderKeyCopiesAndSameBootContinuity(t *testing.T) {
	now := time.Unix(100, 0)
	store := NewStore(func() time.Time { return now })
	firstKey, err := holderkey.New(bytes.Repeat([]byte{1}, 32))
	if err != nil {
		t.Fatal(err)
	}
	secondKey, err := holderkey.New(bytes.Repeat([]byte{2}, 32))
	if err != nil {
		t.Fatal(err)
	}
	first := testSnapshot(1, "workload", "boot", now)
	first.Holders[0].CommitProofKey = firstKey
	if err := store.ReplaceWorkload(first); err != nil {
		t.Fatal(err)
	}
	listed := store.ListVerifiedDomain(actorrouting.DomainFarm)
	if len(listed) != 1 || len(listed[0].CommitProofKey.PublicKey) != 32 {
		t.Fatalf("verified key missing: %+v", listed)
	}
	listed[0].CommitProofKey.PublicKey[0] ^= 0xff
	again := store.ListVerifiedDomain(actorrouting.DomainFarm)
	if !again[0].CommitProofKey.Equal(firstKey) {
		t.Fatal("verified key accessor leaked mutable bytes")
	}
	revoked := testSnapshot(2, "workload", "boot", now)
	if err := store.ReplaceWorkload(revoked); err != nil {
		t.Fatal(err)
	}
	rebound := testSnapshot(3, "workload", "boot", now)
	rebound.Holders[0].CommitProofKey = secondKey
	if err := store.ReplaceWorkload(rebound); !errors.Is(err, ErrHolderKeyRebind) {
		t.Fatalf("same boot key rebound err=%v", err)
	}
	rotated := testSnapshot(4, "workload", "boot-2", now)
	rotated.Holders[0].CommitProofKey = secondKey
	if err := store.ReplaceWorkload(rotated); err != nil {
		t.Fatalf("new boot key rotation failed: %v", err)
	}
}

func TestListVerifiedDomainExposesFreshWarmingWithoutPlacementEligibility(t *testing.T) {
	now := time.Unix(100, 0)
	store := NewStore(func() time.Time { return now })
	snapshot := testSnapshot(1, "workload", "boot", now)
	snapshot.Holders[0].Lifecycle = LifecycleWarming
	snapshot.Holders[0].ProcessReady = false
	if err := store.ReplaceWorkload(snapshot); err != nil {
		t.Fatal(err)
	}
	if got := store.ListDomain(actorrouting.DomainFarm); len(got) != 0 {
		t.Fatalf("WARMING holder leaked into placement-eligible view: %+v", got)
	}
	verified := store.ListVerifiedDomain(actorrouting.DomainFarm)
	if len(verified) != 1 || verified[0].Lifecycle != LifecycleWarming {
		t.Fatalf("fresh WARMING holder missing from verified control view: %+v", verified)
	}
}

func TestListVerifiedDomainKeepsReconcilingServingIdentityWithoutPlacementEligibility(t *testing.T) {
	now := time.Unix(100, 0)
	store := NewStore(func() time.Time { return now })
	snapshot := testSnapshot(1, "workload", "boot", now)
	snapshot.Holders[0].Lifecycle = LifecycleServing
	snapshot.Holders[0].ProcessReady = false
	snapshot.Holders[0].ServingShardCount = 7
	if err := store.ReplaceWorkload(snapshot); err != nil {
		t.Fatal(err)
	}
	if got := store.ListDomain(actorrouting.DomainFarm); len(got) != 0 {
		t.Fatalf("reconciling Holder leaked into placement-eligible view: %+v", got)
	}
	verified := store.ListVerifiedDomain(actorrouting.DomainFarm)
	if len(verified) != 1 || verified[0].Lifecycle != LifecycleServing || verified[0].ProcessReady {
		t.Fatalf("reconciling serving identity missing from verified control view: %+v", verified)
	}
}
