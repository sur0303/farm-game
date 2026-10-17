package scaleplan_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	core "farmgame/internal/scaleplan"
	"farmgame/internal/scaleplan/codec"
	store "farmgame/internal/storage/redis/scaleplan"
	"farmgame/internal/testkit/scaleplanstore"
)

func TestRedisA06FieldsRoundTripAfterBackendReopen(t *testing.T) {
	h := scaleplanstore.New(t, "realm-a")
	ctx := context.Background()
	plans := scaleplanstore.ScaleInCleanupPlans(t, "realm-a", "operation-a06-redis", "workload-a06-redis")
	lease := h.Lease(t, "nonce-a")

	create, err := h.Backend.CreateOrGet(ctx, lease, a06Submission(plans[0]))
	if err != nil || create.Status != core.CreateCommitted {
		t.Fatalf("create: status=%s err=%v", create.Status, err)
	}
	persistA06CASSequence(t, h, lease, plans)

	reopened, err := store.NewBackend(h.Client, store.Options{Namespace: h.Namespace, Realm: h.Realm})
	if err != nil {
		t.Fatalf("reopen backend: %v", err)
	}
	loaded, found, err := reopened.Load(ctx, plans[len(plans)-1].PlanID)
	if err != nil || !found {
		t.Fatalf("load after reopen: found=%v err=%v", found, err)
	}
	assertA06Plan(t, loaded.Plan, plans[len(plans)-1])

	key, err := reopened.Keys().Plan(plans[len(plans)-1].PlanID)
	if err != nil {
		t.Fatalf("plan key: %v", err)
	}
	canonical, err := h.Client.HGet(ctx, key.String(), "canonical_json").Result()
	if err != nil {
		t.Fatalf("canonical JSON: %v", err)
	}
	checksum, err := h.Client.HGet(ctx, key.String(), "checksum_sha256").Result()
	if err != nil {
		t.Fatalf("checksum: %v", err)
	}
	if !codec.VerifyChecksum([]byte(canonical), checksum) {
		t.Fatalf("stored checksum does not bind canonical JSON")
	}
	decoded, err := codec.DecodePlan([]byte(canonical))
	if err != nil {
		t.Fatalf("decode stored canonical JSON: %v", err)
	}
	assertA06Plan(t, decoded, plans[len(plans)-1])
}

func TestRedisA06ApplyUnknownPreservesCleanupFields(t *testing.T) {
	lost := false
	h := scaleplanstore.New(t, "realm-a", store.Options{AfterMutation: func(name string) error {
		if name == "ApplyPlanV1" && !lost {
			lost = true
			return errors.New("response lost")
		}
		return nil
	}})
	ctx := context.Background()
	plans := scaleplanstore.ScaleInCleanupPlans(t, "realm-a", "operation-a06-unknown", "workload-a06-unknown")
	lease := h.Lease(t, "nonce-a")

	create, err := h.Backend.CreateOrGet(ctx, lease, a06Submission(plans[0]))
	if !errors.Is(err, core.ErrOutcomeUnknown) || create.Status != core.CreateCommitted {
		t.Fatalf("unknown create: status=%s err=%v", create.Status, err)
	}
	persistA06CASSequence(t, h, lease, plans)

	reopened, err := store.NewBackend(h.Client, store.Options{Namespace: h.Namespace, Realm: h.Realm})
	if err != nil {
		t.Fatalf("reopen backend: %v", err)
	}
	loaded, found, err := reopened.Load(ctx, plans[len(plans)-1].PlanID)
	if err != nil || !found {
		t.Fatalf("load after unknown apply: found=%v err=%v", found, err)
	}
	assertA06Plan(t, loaded.Plan, plans[len(plans)-1])
}

func a06Submission(plan core.Plan) core.DurableCreateCommand {
	return core.DurableCreateCommand{
		Submission: core.IdempotencySubmission{
			Key: plan.IdempotencyKey, PayloadDigest: plan.PayloadDigest, WorkloadKey: plan.Submission.Workload.Key(),
		},
		Plan: plan,
	}
}

func persistA06CASSequence(t *testing.T, h *scaleplanstore.Harness, lease core.BackendMutationLease, plans []core.Plan) {
	t.Helper()
	for i := 1; i < len(plans); i++ {
		result, err := h.Backend.CompareAndSwap(context.Background(), lease, core.DurableCASCommand{
			Expected: plans[i-1].Revision, Plan: plans[i],
		})
		if err != nil || result.Status != core.CASCommitted {
			t.Fatalf("CAS %d: status=%s err=%v", i, result.Status, err)
		}
	}
}

func assertA06Plan(t *testing.T, got, want core.Plan) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("A0.6 plan changed across Redis round-trip: want=%+v got=%+v", want, got)
	}
	if got.CleanupIntent == nil {
		t.Fatal("round-tripped plan lost cleanup intent")
	}
	if got.CleanupIntent.FailureCode != "SCALE_IN_FAILURE" ||
		got.CleanupIntent.ScaleResolution != core.ScaleResolutionNotCommitted ||
		got.CleanupIntent.ReleaseOutcome != core.PlacementReleaseReleased {
		t.Fatalf("round-tripped cleanup intent lost A0.6 fields: %+v", got.CleanupIntent)
	}
}
