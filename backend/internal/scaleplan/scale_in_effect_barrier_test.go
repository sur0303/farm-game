package scaleplan

import (
	"testing"
	"time"

	"farmgame/internal/actorrouting"
)

func TestScaleInChildEffectBarrierAllowsMonotonicHolderRefresh(t *testing.T) {
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	before := HolderState{
		Ref:     HolderRef{Domain: actorrouting.DomainFarm, HolderID: "farm-svr-2", BootID: "boot-2", PodUID: "pod-2", WorkloadUID: "set-1", Ordinal: 2},
		PodName: "farm-svr-2", LogicEndpoint: "farm-svr-2:9090", HolderControlEndpoint: "farm-svr-2:19101", TLSDNSIdentity: "holder-control",
		State: HolderServing, ProcessReady: true, ServingShardCount: 341, SnapshotRevision: 2731,
		AssignmentKnownFresh: true, PlacementEligible: false, PlacementRevision: 2, LeaseUntil: now.Add(time.Minute),
	}
	after := before
	after.ServingShardCount--
	after.ProcessReady = false
	after.SnapshotRevision += 32
	after.LeaseUntil = after.LeaseUntil.Add(30 * time.Second)
	exclusion := PlacementExclusion{Holder: before.Ref, Excluded: true, Revision: 2, OwnerPlanID: "scale-in"}
	if !stableScaleInVictimAcrossChildBarrier(before, after, exclusion, now) {
		t.Fatal("normal heartbeat/snapshot refresh invalidated the scale-in child-effect barrier")
	}
}

func TestScaleInChildEffectBarrierRejectsSafetyDrift(t *testing.T) {
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	base := HolderState{
		Ref:     HolderRef{Domain: actorrouting.DomainFarm, HolderID: "farm-svr-2", BootID: "boot-2", PodUID: "pod-2", WorkloadUID: "set-1", Ordinal: 2},
		PodName: "farm-svr-2", LogicEndpoint: "farm-svr-2:9090", HolderControlEndpoint: "farm-svr-2:19101", TLSDNSIdentity: "holder-control",
		State: HolderServing, ProcessReady: true, ServingShardCount: 341, SnapshotRevision: 2731,
		AssignmentKnownFresh: true, PlacementEligible: false, PlacementRevision: 2, LeaseUntil: now.Add(time.Minute),
	}
	exclusion := PlacementExclusion{Holder: base.Ref, Excluded: true, Revision: 2, OwnerPlanID: "scale-in"}
	tests := map[string]func(*HolderState){
		"boot":               func(value *HolderState) { value.Ref.BootID = "replacement" },
		"endpoint":           func(value *HolderState) { value.LogicEndpoint = "replacement:9090" },
		"lifecycle":          func(value *HolderState) { value.State = HolderDraining; value.Draining = true },
		"shard count growth": func(value *HolderState) { value.ServingShardCount++ },
		"snapshot rollback":  func(value *HolderState) { value.SnapshotRevision-- },
		"lease rollback":     func(value *HolderState) { value.LeaseUntil = value.LeaseUntil.Add(-time.Second) },
		"exclusion":          func(value *HolderState) { value.PlacementRevision++ },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			after := base
			mutate(&after)
			if stableScaleInVictimAcrossChildBarrier(base, after, exclusion, now) {
				t.Fatal("unsafe holder drift passed the scale-in child-effect barrier")
			}
		})
	}
}

func TestPinnedScaleInRuntimeAllowsOnlySnapshotReconcileUnready(t *testing.T) {
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	exclusion := PlacementExclusion{Revision: 2}
	victim := HolderState{
		Ref:           HolderRef{Domain: actorrouting.DomainFarm, HolderID: "farm-svr-1", BootID: "boot-1", PodUID: "pod-1", WorkloadUID: "set-1", Ordinal: 1},
		LogicEndpoint: "farm-svr-1:9090", HolderControlEndpoint: "farm-svr-1:19101", TLSDNSIdentity: "holder-control",
		State: HolderServing, ProcessReady: false, ServingShardCount: 32, SnapshotRevision: 42,
		AssignmentKnownFresh: true, PlacementEligible: false, PlacementRevision: 2, LeaseUntil: now.Add(time.Minute),
	}
	if !pinnedScaleInVictimAdmissible(victim, exclusion, now) {
		t.Fatal("exact excluded serving victim was rejected during snapshot reconciliation")
	}
	target := victim
	target.Ref = HolderRef{Domain: actorrouting.DomainFarm, HolderID: "farm-svr-0", BootID: "boot-0", PodUID: "pod-0", WorkloadUID: "set-1", Ordinal: 0}
	target.PlacementEligible, target.PlacementRevision = true, 1
	if !pinnedScaleInTargetAdmissible(target, "set-1", now) {
		t.Fatal("exact serving target was rejected during snapshot reconciliation")
	}

	unsafeVictim := victim
	unsafeVictim.Draining = true
	if pinnedScaleInVictimAdmissible(unsafeVictim, exclusion, now) {
		t.Fatal("draining victim passed pinned admission")
	}
	unsafeTarget := target
	unsafeTarget.Ref.BootID = ""
	if pinnedScaleInTargetAdmissible(unsafeTarget, "set-1", now) {
		t.Fatal("non-exact target passed pinned admission")
	}
}
