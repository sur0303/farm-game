package autorebalance

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	core "farmgame/internal/scaleplan"
)

func TestPlannedScaleOutMovesTheTheoreticalMinimum(t *testing.T) {
	topology := testTopology(t, 2, []int{6, 6}, false)
	plan, err := PlannedScaleOut(topology, "farm-svr-2", DefaultPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if plan.ExpectedReplicas != 2 || plan.DesiredReplicas != 3 || len(plan.Moves) != 4 {
		t.Fatalf("unexpected plan: expected=%d desired=%d moves=%d", plan.ExpectedReplicas, plan.DesiredReplicas, len(plan.Moves))
	}
	counts := resultingCounts(topology, plan.Moves)
	if counts["farm-svr-0"] != 4 || counts["farm-svr-1"] != 4 || counts["farm-svr-2"] != 4 {
		t.Fatalf("not balanced with minimum movement: %#v", counts)
	}
	for _, move := range plan.Moves {
		if move.TargetHolderID != "farm-svr-2" || move.TargetOrdinal != 2 {
			t.Fatalf("wrong target: %+v", move)
		}
	}
}

func TestAdoptedScaleOutRecognizesVerifiedNewestZeroShardPod(t *testing.T) {
	topology := testTopology(t, 3, []int{6, 6}, true)
	plan, err := AdoptedScaleOut(topology, DefaultPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if plan.ExpectedReplicas != 2 || plan.DesiredReplicas != 3 || len(plan.Moves) != 4 {
		t.Fatalf("unexpected adopted plan: %+v", plan)
	}
	if plan.Operation != core.OperationScaleOut {
		t.Fatalf("operation=%s", plan.Operation)
	}
}

func TestAdoptedScaleOutDoesNotReplanAfterTargetOwnsShards(t *testing.T) {
	topology := testTopology(t, 3, []int{4, 4, 4}, false)
	if _, err := AdoptedScaleOut(topology, DefaultPolicy()); err != ErrNoPlan {
		t.Fatalf("err=%v want ErrNoPlan", err)
	}
}

func TestScaleOutRejectsPreexistingCountSkewItCannotRepair(t *testing.T) {
	topology := testTopology(t, 2, []int{10, 2}, false)
	if _, err := PlannedScaleOut(topology, "farm-svr-2", DefaultPolicy()); !errors.Is(err, ErrInvalidTopology) {
		t.Fatalf("err=%v", err)
	}
}

func TestScaleInDrainsHighestOrdinalAndBalancesRemainingHolders(t *testing.T) {
	topology := testTopology(t, 3, []int{4, 4, 4}, false)
	plan, err := ScaleIn(topology, 2, DefaultPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if plan.ExpectedReplicas != 3 || plan.DesiredReplicas != 2 || len(plan.Moves) != 4 {
		t.Fatalf("unexpected plan: %+v", plan)
	}
	counts := resultingCounts(topology, plan.Moves)
	if counts["farm-svr-0"] != 6 || counts["farm-svr-1"] != 6 || counts["farm-svr-2"] != 0 {
		t.Fatalf("unexpected distribution: %#v", counts)
	}
	for _, move := range plan.Moves {
		if move.SourceHolderID != "farm-svr-2" || move.TargetOrdinal >= 2 {
			t.Fatalf("unsafe scale-in move: %+v", move)
		}
	}
}

func TestScaleInAllowsVerifiedHighestOrdinalAlreadyAtZeroShard(t *testing.T) {
	topology := testTopology(t, 3, []int{6, 6, 0}, false)
	topology.Holders[2].State = core.HolderReadyZeroShard

	plan, err := ScaleIn(topology, 2, DefaultPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if plan.ExpectedReplicas != 3 || plan.DesiredReplicas != 2 || len(plan.Moves) != 0 {
		t.Fatalf("unexpected scale-only plan: %+v", plan)
	}
	if _, err := plan.Normalize(); err != nil {
		t.Fatalf("scale-only plan must be a canonical durable submission: %v", err)
	}
}

func TestDrainHolderPreservesReplicasAndMovesOnlyExactVictimShards(t *testing.T) {
	topology := testTopology(t, 3, []int{4, 4, 4}, false)
	plan, err := DrainHolder(topology, "farm-svr-1", DefaultPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if plan.Operation != core.OperationDrainHolder || plan.ExpectedReplicas != 3 ||
		plan.DesiredReplicas != 3 || len(plan.Moves) != 4 {
		t.Fatalf("unexpected drain plan: %+v", plan)
	}
	counts := resultingCounts(topology, plan.Moves)
	if counts["farm-svr-1"] != 0 || counts["farm-svr-0"] != 6 || counts["farm-svr-2"] != 6 {
		t.Fatalf("unexpected drain distribution: %#v", counts)
	}
	for _, move := range plan.Moves {
		if move.SourceHolderID != "farm-svr-1" || move.TargetHolderID == "farm-svr-1" {
			t.Fatalf("unsafe holder drain move: %+v", move)
		}
	}
	if _, err := plan.Normalize(); err != nil {
		t.Fatalf("drain plan must normalize: %v", err)
	}
}

func TestDrainHolderCanUseVerifiedZeroShardReplacementAsTarget(t *testing.T) {
	topology := testTopology(t, 3, []int{8, 4, 0}, false)
	topology.Holders[2].State = core.HolderReadyZeroShard

	plan, err := DrainHolder(topology, "farm-svr-1", DefaultPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Moves) != 4 {
		t.Fatalf("moves=%d want=4", len(plan.Moves))
	}
	for _, move := range plan.Moves {
		if move.TargetHolderID != "farm-svr-2" {
			t.Fatalf("fresh replacement was not selected: %+v", move)
		}
	}
}

func TestDrainHolderToPinsEveryVictimShardToOneExactReplacement(t *testing.T) {
	topology := testTopology(t, 4, []int{8, 2, 2, 0}, false)
	topology.Holders[3].State = core.HolderReadyZeroShard
	replacement := topology.Holders[3].Ref

	plan, err := DrainHolderTo(topology, "farm-svr-0", replacement, DefaultPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if plan.PinnedTarget != replacement || len(plan.Moves) != 8 {
		t.Fatalf("pinned target or manifest changed: target=%+v moves=%d", plan.PinnedTarget, len(plan.Moves))
	}
	for _, move := range plan.Moves {
		if move.SourceHolderID != "farm-svr-0" || move.TargetHolderID != replacement.HolderID ||
			move.TargetOrdinal != replacement.Ordinal {
			t.Fatalf("move escaped exact replacement: %+v", move)
		}
	}
	counts := resultingCounts(topology, plan.Moves)
	if counts["farm-svr-0"] != 0 || counts[replacement.HolderID] != 8 || counts["farm-svr-1"] != 2 || counts["farm-svr-2"] != 2 {
		t.Fatalf("victim was distributed instead of replaced one-to-one: %#v", counts)
	}
}

func TestDrainHolderToRejectsServingOrDifferentGenerationReplacement(t *testing.T) {
	topology := testTopology(t, 3, []int{4, 4, 0}, false)
	replacement := topology.Holders[2].Ref
	if _, err := DrainHolderTo(topology, "farm-svr-1", replacement, DefaultPolicy()); !errors.Is(err, ErrInvalidTopology) {
		t.Fatalf("serving replacement accepted: %v", err)
	}
	topology.Holders[2].State = core.HolderReadyZeroShard
	replacement.BootID = "different-boot"
	if _, err := DrainHolderTo(topology, "farm-svr-1", replacement, DefaultPolicy()); !errors.Is(err, ErrInvalidTopology) {
		t.Fatalf("unverified replacement generation accepted: %v", err)
	}
}

func TestScaleInCanContractSpareIntoVerifiedZeroShardReplacements(t *testing.T) {
	topology := testTopology(t, 3, []int{0, 0, 12}, false)
	topology.Holders[0].State = core.HolderReadyZeroShard
	topology.Holders[1].State = core.HolderReadyZeroShard

	plan, err := ScaleIn(topology, 2, DefaultPolicy())
	if err != nil {
		t.Fatal(err)
	}
	counts := resultingCounts(topology, plan.Moves)
	if counts["farm-svr-0"] != 6 || counts["farm-svr-1"] != 6 || counts["farm-svr-2"] != 0 {
		t.Fatalf("unexpected final rollout distribution: %#v", counts)
	}
}

func TestDrainHolderBindsZeroShardVictimWithoutSkippingFence(t *testing.T) {
	topology := testTopology(t, 3, []int{6, 0, 6}, false)
	topology.Holders[1].State = core.HolderReadyZeroShard
	plan, err := DrainHolder(topology, "farm-svr-1", DefaultPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if plan.VictimHolderID != "farm-svr-1" || len(plan.Moves) != 0 || plan.Operation != core.OperationDrainHolder {
		t.Fatalf("plan=%+v", plan)
	}
	if _, err := plan.Normalize(); err != nil {
		t.Fatalf("zero-shard drain must remain a durable plan: %v", err)
	}
}

func TestPlannerIsDeterministic(t *testing.T) {
	topology := testTopology(t, 3, []int{6, 6}, true)
	first, err := AdoptedScaleOut(topology, DefaultPolicy())
	if err != nil {
		t.Fatal(err)
	}
	second, err := AdoptedScaleOut(topology, DefaultPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("planner changed output:\nfirst=%+v\nsecond=%+v", first, second)
	}
}

func testTopology(t *testing.T, replicas int32, shardCounts []int, addZeroTarget bool) Topology {
	t.Helper()
	ref := core.WorkloadRef{Realm: "realm-a", Domain: actorrouting.DomainFarm, Namespace: "farm", Kind: "StatefulSet", Name: "farm-svr"}
	state := core.WorkloadState{Ref: ref, UID: "workload-a", ResourceVersion: "rv-10", Replicas: replicas, ReadyReplicas: replicas}
	holders := make([]core.HolderState, 0, len(shardCounts)+1)
	placements := make([]core.ShardPlacement, 0)
	shard := uint32(0)
	for ordinal, count := range shardCounts {
		holder := testHolder(int32(ordinal), count, state)
		holders = append(holders, holder)
		for i := 0; i < count; i++ {
			placements = append(placements, core.ShardPlacement{
				Domain: ref.Domain, VirtualShard: shard, Active: true, KnownFresh: true,
				SnapshotRevision: 10, Holder: holder.Ref, Endpoint: holder.LogicEndpoint,
			})
			shard++
		}
	}
	if addZeroTarget {
		holders = append(holders, core.HolderState{
			Ref:     core.HolderRef{Domain: ref.Domain, HolderID: "farm-svr-2", BootID: "boot-2", PodUID: "pod-2", WorkloadUID: state.UID, Ordinal: 2},
			PodName: "farm-svr-2", LogicEndpoint: "farm-svr-2:9000", State: core.HolderReadyZeroShard,
			ProcessReady: true, AssignmentKnownFresh: true, PlacementEligible: true, PlacementRevision: 1, LeaseUntil: time.Now().Add(time.Hour),
		})
	}
	return Topology{
		Workload:   state,
		Token:      core.PlacementCaptureToken{ID: "capture-a", ActiveCardinality: uint64(len(placements))},
		Placements: placements, Holders: holders,
	}
}

func testHolder(ordinal int32, count int, state core.WorkloadState) core.HolderState {
	id := "farm-svr-" + string(rune('0'+ordinal))
	return core.HolderState{
		Ref:     core.HolderRef{Domain: state.Ref.Domain, HolderID: id, BootID: "boot-" + id, PodUID: "pod-" + id, WorkloadUID: state.UID, Ordinal: ordinal},
		PodName: id, LogicEndpoint: id + ":9000", State: core.HolderServing, ProcessReady: true,
		ServingShardCount: uint32(count), AssignmentKnownFresh: true, PlacementEligible: true, PlacementRevision: 1, LeaseUntil: time.Now().Add(time.Hour),
	}
}

func resultingCounts(topology Topology, moves []core.ShardMoveIntent) map[string]int {
	counts := make(map[string]int)
	for _, placement := range topology.Placements {
		counts[placement.Holder.HolderID]++
	}
	for _, move := range moves {
		counts[move.SourceHolderID]--
		counts[move.TargetHolderID]++
	}
	return counts
}
