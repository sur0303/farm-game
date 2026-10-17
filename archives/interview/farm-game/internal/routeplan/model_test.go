package routeplan

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/holderidentity"
	"farmgame/internal/storage/authoritystore"
)

func testPlan(now int64) Plan {
	return Plan{
		PlanID: "plan-1", TransitionID: "transition-1", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		SourceHolderID: "holder-a", SourceBootID: "boot-a", SourceEndpoint: "holder-a.internal:443",
		TargetHolderID: "holder-b", TargetBootID: "boot-b", TargetEndpoint: "holder-b.internal:443",
		ExpectedSourceFence: authoritystore.ShardWriteFence{ExpectedHolderID: "holder-a", ExpectedHolderBootID: "boot-a", ExpectedAssignmentRevision: 3, ExpectedWriterEpoch: 4, ExpectedLeaseEpoch: 5},
		RequestedTTLms:      30_000, Operation: OperationPlannedDrain, Phase: PhasePending, Revision: 1,
		CreatedAtMs: now, UpdatedAtMs: now, ExpiresAtMs: now + time.Hour.Milliseconds(),
	}
}

func TestPlanCodecPersistsExactHolderGenerationsAndOwnerRetention(t *testing.T) {
	plan := testPlan(1_900_000_000_000)
	plan.SourceDeploymentIdentity = holderidentity.DeploymentIdentity{PodUID: "pod-a", WorkloadUID: "workload-a", Ordinal: 0}
	plan.TargetDeploymentIdentity = holderidentity.DeploymentIdentity{PodUID: "pod-b", WorkloadUID: "workload-b", Ordinal: 1}
	plan.OwnerScalePlanID = "scale-plan-1"
	plan.OwnerRetainUntilMs = plan.ExpiresAtMs + 37*24*time.Hour.Milliseconds()
	envelope, err := EncodePlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodePlan(envelope)
	if err != nil || decoded != plan {
		t.Fatalf("decode=%+v err=%v", decoded, err)
	}
	mutated := plan
	mutated.TargetDeploymentIdentity.PodUID = "other-pod"
	mutated.Revision++
	mutated.UpdatedAtMs++
	if err := ValidatePlanUpdate(plan, mutated); !errors.Is(err, ErrPlanConflict) {
		t.Fatalf("immutable generation update err=%v", err)
	}
}

func TestPlanCodecReadsExactLegacyV1BytesWithoutReinterpretation(t *testing.T) {
	canonical := []byte(`{"schema_version":"route-plan-v1","plan_id":"legacy-plan","transition_id":"legacy-transition","domain":"PLAYER","virtual_shard":0,"source_holder_id":"holder-a","source_boot_id":"boot-a","source_endpoint":"holder-a.internal:443","target_holder_id":"holder-b","target_boot_id":"boot-b","target_endpoint":"holder-b.internal:443","expected_source_fence":{"holder_id":"holder-a","holder_boot_id":"boot-a","assignment_revision":3,"writer_epoch":4,"lease_epoch":5},"requested_ttl_ms":30000,"operation":"planned-drain","phase":"PENDING","attempt":0,"revision":1,"created_at_ms":1900000000000,"updated_at_ms":1900000000000,"started_at_ms":0,"completed_at_ms":0,"expires_at_ms":1900003600000,"error_code":"","takeover_proof":{"expected_holder_boot_id":"","holder_unavailable":false,"lease_expired":false,"evidence":""}}`)
	envelope := PlanEnvelope{SchemaVersion: LegacyPlanSchemaVersion, PlanID: "legacy-plan", Revision: 1,
		CanonicalJSON: canonical, Checksum: checksumBytes(canonical), UpdatedAtMs: 1_900_000_000_000, ExpiresAtMs: 1_900_003_600_000}
	plan, err := DecodePlan(envelope)
	if err != nil || plan.PlanID != "legacy-plan" || !plan.SourceDeploymentIdentity.IsZero() ||
		!plan.TargetDeploymentIdentity.IsZero() || plan.OwnerScalePlanID != "" || plan.OwnerRetainUntilMs != 0 {
		t.Fatalf("legacy plan=%+v err=%v", plan, err)
	}
	encoded, err := EncodePlan(plan)
	if err != nil || encoded.SchemaVersion != PlanSchemaVersion || bytes.Equal(encoded.CanonicalJSON, canonical) {
		t.Fatalf("upgraded envelope=%+v err=%v", encoded, err)
	}
}

func TestOwnedPlanAllowsParentDeadlineBeforeChildLogicalExpiry(t *testing.T) {
	plan := testPlan(1_900_000_000_000)
	plan.SourceDeploymentIdentity = holderidentity.DeploymentIdentity{PodUID: "pod-a", WorkloadUID: "workload", Ordinal: 0}
	plan.TargetDeploymentIdentity = holderidentity.DeploymentIdentity{PodUID: "pod-b", WorkloadUID: "workload", Ordinal: 1}
	plan.OwnerScalePlanID = "scale-plan-late-child"
	plan.OwnerRetainUntilMs = plan.CreatedAtMs + time.Minute.Milliseconds()
	if plan.OwnerRetainUntilMs >= plan.ExpiresAtMs {
		t.Fatal("fixture does not exercise late child boundary")
	}
	if err := plan.Validate(); err != nil {
		t.Fatalf("late child rejected: %v", err)
	}
}

func TestPlanCodecCanonicalChecksumAndCorruption(t *testing.T) {
	plan := testPlan(1_900_000_000_000)
	envelope, err := EncodePlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodePlan(envelope)
	if err != nil || decoded != plan {
		t.Fatalf("decode=%+v err=%v", decoded, err)
	}
	for _, suffix := range []string{"{}", "{"} {
		candidate := envelope
		candidate.CanonicalJSON = append(append([]byte(nil), envelope.CanonicalJSON...), []byte(suffix)...)
		candidate.Checksum, _ = PlanChecksumFromJSON(candidate.CanonicalJSON)
		if _, err := DecodePlan(candidate); !errors.Is(err, ErrPlanCorrupt) {
			t.Fatalf("suffix %q err=%v", suffix, err)
		}
	}
	bad := envelope
	bad.Checksum = strings.Repeat("0", 64)
	if _, err := DecodePlan(bad); !errors.Is(err, ErrPlanCorrupt) {
		t.Fatalf("checksum corruption err=%v", err)
	}
	oversized := envelope
	oversized.CanonicalJSON = bytes.Repeat([]byte{'x'}, MaxCanonicalPlanBytes+1)
	oversized.Checksum = strings.Repeat("0", MaxPlanChecksumBytes)
	if _, err := DecodePlan(oversized); !errors.Is(err, ErrPlanCorrupt) {
		t.Fatalf("oversized envelope err=%v", err)
	}
}

// PlanChecksumFromJSON is a test-only helper kept local to this file so the
// production codec has one checksum implementation.
func PlanChecksumFromJSON(value []byte) (string, error) {
	return checksumBytes(value), nil
}

func checksumBytes(value []byte) string {
	// Use the public codec by temporarily decoding is not possible for malformed
	// JSON; this helper mirrors SHA-256 only to construct a corrupt envelope.
	// The assertion is about decoder rejection, not checksum semantics.
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func TestMemoryPlanStoreCASPhaseReplayCapacityAndRestartList(t *testing.T) {
	now := time.UnixMilli(1_900_000_000_000)
	store, err := NewMemoryPlanStore(MemoryPlanStoreOptions{MaxItems: 1, MaxBytes: 1 << 20, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	plan := testPlan(now.UnixMilli())
	result, err := store.CompareAndSwap(context.Background(), 0, plan)
	if err != nil || result.Status != CASCommitted {
		t.Fatalf("create status=%s err=%v", result.Status, err)
	}
	replay, err := store.CompareAndSwap(context.Background(), 0, plan)
	if err != nil || replay.Status != CASAlreadyCommitted {
		t.Fatalf("replay status=%s err=%v", replay.Status, err)
	}
	badRevision := plan
	badRevision.Revision = 3
	if _, err := store.CompareAndSwap(context.Background(), 1, badRevision); !errors.Is(err, ErrPlanConflict) {
		t.Fatalf("revision err=%v", err)
	}
	advanced := plan
	advanced.Phase = PhaseDraining
	advanced.Revision = 2
	advanced.UpdatedAtMs++
	result, err = store.CompareAndSwap(context.Background(), 1, advanced)
	if err != nil || result.Status != CASCommitted {
		t.Fatalf("advance status=%s err=%v", result.Status, err)
	}
	second := testPlan(now.UnixMilli())
	second.PlanID = "plan-2"
	second.TransitionID = "transition-2"
	if _, err := store.CompareAndSwap(context.Background(), 0, second); !errors.Is(err, ErrPlanCapacity) {
		t.Fatalf("capacity err=%v", err)
	}
	incomplete, err := store.ListIncomplete(context.Background())
	if err != nil || len(incomplete) != 1 || incomplete[0].Plan.PlanID != plan.PlanID {
		t.Fatalf("incomplete=%+v err=%v", incomplete, err)
	}
	now = now.Add(2 * time.Hour)
	if _, found, err := store.Load(context.Background(), plan.PlanID); err != nil || !found {
		t.Fatalf("audit-retained load found=%t err=%v", found, err)
	}
	if incomplete, err := store.ListIncomplete(context.Background()); err != nil || len(incomplete) != 1 {
		t.Fatalf("audit-retained incomplete entries=%v err=%v", incomplete, err)
	}
	now = now.Add(24 * time.Hour)
	if _, found, err := store.Load(context.Background(), plan.PlanID); err != nil || found {
		t.Fatalf("physical expiry found=%t err=%v", found, err)
	}
}

func TestMemoryPlanStoreFreezesAuditAndTerminalContent(t *testing.T) {
	now := int64(1_900_000_000_000)
	store, err := NewMemoryPlanStore(DefaultMemoryPlanStoreOptions())
	if err != nil {
		t.Fatal(err)
	}
	plan := testPlan(now)
	if _, err := store.CompareAndSwap(context.Background(), 0, plan); err != nil {
		t.Fatal(err)
	}
	audit := plan
	audit.Attempt = 1
	audit.StartedAtMs = now + 1
	audit.UpdatedAtMs = now + 1
	audit.Revision = 2
	if _, err := store.CompareAndSwap(context.Background(), 1, audit); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Plan){
		"attempt regression": func(p *Plan) { p.Attempt = 0 },
		"frozen target":      func(p *Plan) { p.TargetHolderID = "other" },
		"expiry rewrite":     func(p *Plan) { p.ExpiresAtMs++ },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := audit
			candidate.Revision = 3
			candidate.UpdatedAtMs++
			mutate(&candidate)
			if _, err := store.CompareAndSwap(context.Background(), 2, candidate); !errors.Is(err, ErrPlanConflict) {
				t.Fatalf("CAS err=%v", err)
			}
		})
	}
	paused := audit
	paused.Phase = PhasePaused
	paused.ErrorCode = "MANUAL_PAUSE"
	paused.UpdatedAtMs++
	paused.CompletedAtMs = paused.UpdatedAtMs
	paused.Revision = 3
	if _, err := store.CompareAndSwap(context.Background(), 2, paused); err != nil {
		t.Fatal(err)
	}
	if replay, err := store.CompareAndSwap(context.Background(), 0, paused); err != nil || replay.Status != CASAlreadyCommitted {
		t.Fatalf("terminal replay=%s err=%v", replay.Status, err)
	}
	changed := paused
	changed.Revision++
	changed.UpdatedAtMs++
	changed.CompletedAtMs = changed.UpdatedAtMs
	changed.ErrorCode = "CHANGED"
	if _, err := store.CompareAndSwap(context.Background(), paused.Revision, changed); !errors.Is(err, ErrPlanConflict) {
		t.Fatalf("terminal mutation err=%v", err)
	}
}

func TestPhaseTransitionTableIsMonotonicAndTerminal(t *testing.T) {
	allowed := [][2]Phase{{PhasePending, PhaseDraining}, {PhaseDraining, PhaseAssigning}, {PhaseAssigning, PhasePreparing}, {PhasePreparing, PhaseActive}, {PhaseActive, PhaseSucceeded}, {PhasePending, PhasePaused}, {PhaseActive, PhaseFailed}}
	for _, pair := range allowed {
		if !CanAdvancePhase(pair[0], pair[1]) {
			t.Fatalf("%s -> %s rejected", pair[0], pair[1])
		}
	}
	for _, pair := range [][2]Phase{{PhaseDraining, PhasePending}, {PhaseActive, PhaseDraining}, {PhaseSucceeded, PhaseActive}, {PhaseFailed, PhasePending}} {
		if CanAdvancePhase(pair[0], pair[1]) {
			t.Fatalf("%s -> %s accepted", pair[0], pair[1])
		}
	}
}
