package readiness

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func testConfigs(threshold time.Duration) []FamilyConfig {
	configs := make([]FamilyConfig, 0, MaxFamilies)
	for _, family := range AllFamilies() {
		enabled := family != FamilyGuardSync && family != FamilyMail
		configs = append(configs, FamilyConfig{Family: family, Enabled: enabled, Critical: true,
			FreshnessThreshold: threshold, RequireDueProof: enabled})
	}
	return configs
}

func readyRegistry(t *testing.T, now time.Time, threshold time.Duration) (*Registry, map[Family]*LoopHandle) {
	t.Helper()
	registry, err := NewRegistry(testConfigs(threshold), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	handles := make(map[Family]*LoopHandle)
	for _, family := range AllFamilies() {
		if family == FamilyGuardSync || family == FamilyMail {
			continue
		}
		if err := registry.SetDependencies(family, Dependencies{RepositoryInitialized: true, RouteProviderUsable: true, ParticipantClientUsable: true}); err != nil {
			t.Fatal(err)
		}
		handle, err := registry.StartLoop(family)
		if err != nil {
			t.Fatal(err)
		}
		handle.ScanStarted(now)
		handle.ScanSucceeded(now, DueObservation{DepthAvailable: true, OldestDueAgeAvailable: true})
		handles[family] = handle
	}
	return registry, handles
}

func TestRegistryAggregateRequiresEveryEnabledCriticalFamilyReady(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	registry, handles := readyRegistry(t, now, 5*time.Second)
	if snapshot := registry.Snapshot(); !snapshot.Ready || snapshot.State != StateReady {
		t.Fatalf("ready snapshot=%+v", snapshot)
	}

	handles[FamilyPlant].ScanFailed(now.Add(time.Millisecond), "redis unavailable: instance=high-cardinality")
	snapshot := registry.SnapshotAt(now.Add(time.Millisecond))
	if snapshot.Ready || snapshot.State != StateDegraded {
		t.Fatalf("Plant repository fault snapshot=%+v", snapshot)
	}
	plant := findFamily(t, snapshot, FamilyPlant)
	if plant.State != StateDegraded || plant.WorkLeaseSchedulerHealthy || plant.LastErrorClass != "REDIS_UNAVAILABLE__INSTANCE_HIGH_CARDINALITY" {
		t.Fatalf("Plant fault=%+v", plant)
	}
	if harvest := findFamily(t, snapshot, FamilyHarvest); harvest.State != StateReady {
		t.Fatalf("Harvest changed with Plant fault: %+v", harvest)
	}
}

func TestPlantRepositoryDisconnectedIsFailedAndNotReady(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	registry, _ := readyRegistry(t, now, 5*time.Second)
	if err := registry.SetDependencies(FamilyPlant, Dependencies{
		RepositoryInitialized: false, RouteProviderUsable: true, ParticipantClientUsable: true,
	}); err != nil {
		t.Fatal(err)
	}
	snapshot := registry.SnapshotAt(now)
	if snapshot.Ready || snapshot.State != StateFailed || findFamily(t, snapshot, FamilyPlant).State != StateFailed {
		t.Fatalf("disconnected Plant repository snapshot=%+v", snapshot)
	}
	if harvest := findFamily(t, snapshot, FamilyHarvest); harvest.State != StateReady {
		t.Fatalf("Harvest changed with disconnected Plant repository: %+v", harvest)
	}
}

func TestCareFaultIsIsolatedAndCanRecover(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	registry, handles := readyRegistry(t, now, 5*time.Second)
	handles[FamilyCare].ScanFailed(now.Add(time.Second), "repository_disconnected")
	fault := registry.SnapshotAt(now.Add(time.Second))
	if fault.Ready || fault.State != StateDegraded || findFamily(t, fault, FamilyCare).State != StateDegraded {
		t.Fatalf("Care fault=%+v", fault)
	}
	for _, family := range []Family{FamilyPlant, FamilyHarvest, FamilyTheft, FamilyFriendProjection} {
		if item := findFamily(t, fault, family); item.State != StateReady {
			t.Fatalf("%s did not continue during Care fault: %+v", family, item)
		}
	}
	handles[FamilyCare].ScanStarted(now.Add(2 * time.Second))
	handles[FamilyCare].ScanSucceeded(now.Add(2*time.Second), DueObservation{DepthAvailable: true, OldestDueAgeAvailable: true})
	if recovered := registry.SnapshotAt(now.Add(2 * time.Second)); !recovered.Ready {
		t.Fatalf("Care did not recover: %+v", recovered)
	}
}

func TestCareRepositoryDisconnectedFailsAggregateButOtherFamiliesContinue(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	registry, _ := readyRegistry(t, now, 5*time.Second)
	if err := registry.SetDependencies(FamilyCare, Dependencies{
		RepositoryInitialized: false, RouteProviderUsable: true, ParticipantClientUsable: true,
	}); err != nil {
		t.Fatal(err)
	}
	snapshot := registry.SnapshotAt(now)
	if snapshot.Ready || snapshot.State != StateFailed || findFamily(t, snapshot, FamilyCare).State != StateFailed {
		t.Fatalf("disconnected Care repository snapshot=%+v", snapshot)
	}
	for _, family := range []Family{FamilyPlant, FamilyHarvest, FamilyTheft, FamilyFriendProjection} {
		if item := findFamily(t, snapshot, family); item.State != StateReady {
			t.Fatalf("%s did not continue during Care repository fault: %+v", family, item)
		}
	}
}

func TestStaleSuccessAndUnknownDueProofAreDegraded(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	registry, handles := readyRegistry(t, now, time.Second)
	if snapshot := registry.SnapshotAt(now.Add(time.Second + time.Nanosecond)); snapshot.Ready || findFamily(t, snapshot, FamilyPlant).State != StateDegraded {
		t.Fatalf("stale successful scan remained ready: %+v", snapshot)
	}
	handles[FamilyPlant].ScanSucceeded(now.Add(2*time.Second), DueObservation{})
	if item := findFamily(t, registry.SnapshotAt(now.Add(2*time.Second)), FamilyPlant); item.State != StateDegraded || item.DueDepthAvailable || item.OldestDueAgeAvailable {
		t.Fatalf("unknown DueIndex proof was not explicit: %+v", item)
	}
}

func TestLoopGenerationProvidesHappensBeforeFence(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	registry, handles := readyRegistry(t, now, time.Second)
	old := handles[FamilyPlant]
	replacement, err := registry.StartLoop(FamilyPlant)
	if err != nil {
		t.Fatal(err)
	}
	if old.Stop("late_old_exit") {
		t.Fatal("stale loop handle changed replacement lifecycle")
	}
	replacement.ScanSucceeded(now, DueObservation{DepthAvailable: true, OldestDueAgeAvailable: true})
	if item := findFamily(t, registry.SnapshotAt(now), FamilyPlant); item.State != StateReady || !item.ScanLoopAlive {
		t.Fatalf("replacement lifecycle=%+v", item)
	}
	replacement.Stop("panic")
	if item := findFamily(t, registry.SnapshotAt(now), FamilyPlant); item.State != StateFailed || item.ScanLoopAlive {
		t.Fatalf("panic exit lifecycle=%+v", item)
	}
}

func TestDisabledGuardAndMailNeverBlock(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	registry, _ := readyRegistry(t, now, time.Second)
	snapshot := registry.SnapshotAt(now)
	if !snapshot.Ready || findFamily(t, snapshot, FamilyGuardSync).State != StateDisabled || findFamily(t, snapshot, FamilyMail).State != StateDisabled {
		t.Fatalf("disabled optional Families=%+v", snapshot)
	}
}

func TestMetricsExposeOnlyBoundedFamilyLabelsAndRequiredNames(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	registry, _ := readyRegistry(t, now, time.Second)
	promRegistry := prometheus.NewRegistry()
	metrics, err := NewMetrics(promRegistry, registry)
	if err != nil {
		t.Fatal(err)
	}
	metrics.ObserveScan(FamilyPlant, 15*time.Millisecond, ScanStats{Claimed: 2, Completed: 1, Retried: 1, StepUnknown: 1})
	metrics.ObserveTerminalLatency(FamilyPlant, 250*time.Millisecond)
	metrics.ObserveCounts(Family("operation-id-must-not-become-a-label"), ScanStats{Claimed: 1})
	names := []string{
		"farm_worker_family_ready", "farm_worker_due_depth", "farm_worker_oldest_due_age_ms",
		"farm_worker_scan_duration_ms", "farm_worker_claimed_total", "farm_worker_completed_total",
		"farm_worker_retried_total", "farm_worker_step_unknown_total", "farm_worker_direct_dispatch_hit_ratio",
		"farm_worker_worker_takeover_ratio", "farm_worker_terminal_latency_ms", "farm_worker_owned_bucket_count",
		"farm_worker_assignment_revision", "farm_worker_bucket_scanned_total",
	}
	if count, err := testutil.GatherAndCount(promRegistry, names...); err != nil || count == 0 {
		t.Fatalf("required metric samples count=%d err=%v", count, err)
	}
	metricFamilies, err := promRegistry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	found := make(map[string]bool, len(names))
	allowedFamilies := make(map[string]bool, MaxFamilies)
	for _, family := range AllFamilies() {
		allowedFamilies[string(family)] = true
	}
	for _, metricFamily := range metricFamilies {
		found[metricFamily.GetName()] = true
		for _, metric := range metricFamily.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() != "family" {
					t.Fatalf("metric %s has non-bounded label %s", metricFamily.GetName(), label.GetName())
				}
				if !allowedFamilies[label.GetValue()] {
					t.Fatalf("metric %s has unbounded Family value %q", metricFamily.GetName(), label.GetValue())
				}
			}
		}
	}
	for _, name := range names {
		if !found[name] {
			t.Fatalf("required metric %s is missing", name)
		}
	}
}

func findFamily(t *testing.T, snapshot Snapshot, family Family) FamilySnapshot {
	t.Helper()
	for _, item := range snapshot.Families {
		if item.Family == family {
			return item
		}
	}
	t.Fatalf("missing Family %s", family)
	return FamilySnapshot{}
}
