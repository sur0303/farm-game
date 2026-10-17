package codec_test

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	core "farmgame/internal/scaleplan"
	"farmgame/internal/scaleplan/codec"
	"farmgame/internal/testkit/scaleplanstore"
)

func TestCanonicalPlanRoundTripAndChecksum(t *testing.T) {
	plan := scaleplanstore.InitialPlan(t, "realm-a", "operation-a", "workload-a")
	encoded, err := codec.EncodePlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := codec.DecodePlan(encoded)
	if err != nil {
		t.Fatal(err)
	}
	reencoded, err := codec.EncodePlan(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, reencoded) {
		t.Fatal("codec changed canonical bytes")
	}
	checksum := codec.Checksum(encoded)
	if !codec.VerifyChecksum(encoded, checksum) {
		t.Fatal("checksum did not verify")
	}
	if codec.VerifyChecksum(append(encoded, ' '), checksum) {
		t.Fatal("checksum accepted changed bytes")
	}
}

func TestDecodeRejectsUnknownOrNonCanonicalJSON(t *testing.T) {
	plan := scaleplanstore.InitialPlan(t, "realm-a", "operation-a", "workload-a")
	encoded, err := codec.EncodePlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.DecodePlan(append(encoded, ' ')); err == nil {
		t.Fatal("accepted trailing whitespace")
	}
	if _, err := codec.DecodePlan(bytes.Replace(encoded, []byte(`"plan_id"`), []byte(`"unknown_plan_id"`), 1)); err == nil {
		t.Fatal("accepted unknown field")
	}
}

func TestPinnedDrainTargetRoundTrip(t *testing.T) {
	target := core.HolderRef{
		Domain: actorrouting.DomainFarm, HolderID: "farm-svr-2", BootID: "boot-new",
		PodUID: "pod-new", WorkloadUID: "workload-a", Ordinal: 2,
	}
	submission := core.Submission{
		IdempotencyKey: "drain-pinned-a", Realm: "realm-a", Domain: actorrouting.DomainFarm,
		Operation: core.OperationDrainHolder,
		Workload: core.WorkloadRef{
			Realm: "realm-a", Domain: actorrouting.DomainFarm, Namespace: "farm",
			Kind: "StatefulSet", Name: "farm-svr",
		},
		VictimHolderID: "farm-svr-1", PinnedTarget: target,
		ExpectedReplicas: 3, DesiredReplicas: 3,
		Moves: []core.ShardMoveIntent{{
			Domain: actorrouting.DomainFarm, VirtualShard: 7, SourceHolderID: "farm-svr-1",
			TargetHolderID: target.HolderID, TargetOrdinal: target.Ordinal,
		}},
		RoutePlanTTLms: time.Hour.Milliseconds(), PlanTTLms: (24 * time.Hour).Milliseconds(),
		RecoveryRetentionMs: (7 * 24 * time.Hour).Milliseconds(),
		Limits: core.FunctionalLimits{
			MaxRoutePlansInflight: core.FunctionalMaxInflight,
			MaxShardsPerWave:      core.FunctionalMaxShardsPerWave,
		},
	}
	plan, err := core.BuildInitialPlan(submission, time.Unix(1_800_000_000, 0))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := codec.EncodePlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(encoded, []byte(`"pinned_target"`)) {
		t.Fatalf("canonical JSON omitted pinned target: %s", encoded)
	}
	decoded, err := codec.DecodePlan(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(plan, decoded) {
		t.Fatalf("pinned target changed across round-trip: want=%+v got=%+v", plan.Submission.PinnedTarget, decoded.Submission.PinnedTarget)
	}
}
