package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	remoteroute "farmgame/internal/actorrouting/remote"
	"farmgame/internal/authoritycomposition"
	"farmgame/internal/storage/redis/rediskey"
	workerreadiness "farmgame/internal/worker/readiness"
	"farmgame/internal/workidentity"

	"github.com/prometheus/client_golang/prometheus"
)

func TestSuppressExpectedOnlyRequiresEveryJoinedCauseToMatch(t *testing.T) {
	expected := errors.New("expected retry")
	if got := suppressExpectedOnly(errors.Join(expected, expected), expected); got != nil {
		t.Fatalf("expected-only tree was not suppressed: %v", got)
	}
	mixed := errors.Join(expected, context.DeadlineExceeded)
	if got := suppressExpectedOnly(mixed, expected); got == nil || !errors.Is(got, context.DeadlineExceeded) {
		t.Fatalf("mixed error tree was suppressed: %v", got)
	}
}

func mainTestRegistry(t *testing.T) *workerreadiness.Registry {
	t.Helper()
	configs := make([]workerreadiness.FamilyConfig, 0, workerreadiness.MaxFamilies)
	for _, family := range workerreadiness.AllFamilies() {
		enabled := family == workerreadiness.FamilyPlant
		configs = append(configs, workerreadiness.FamilyConfig{Family: family, Enabled: enabled, Critical: true,
			FreshnessThreshold: time.Second, RequireDueProof: enabled})
	}
	registry, err := workerreadiness.NewRegistry(configs, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.SetDependencies(workerreadiness.FamilyPlant, workerreadiness.Dependencies{
		RepositoryInitialized: true, RouteProviderUsable: true, ParticipantClientUsable: true,
	}); err != nil {
		t.Fatal(err)
	}
	return registry
}

func TestHealthIsLivenessWhileTransientScanFailureChangesReadinessAndRecovers(t *testing.T) {
	registry := mainTestRegistry(t)
	metricsRegistry := prometheus.NewRegistry()
	metrics, err := workerreadiness.NewMetrics(metricsRegistry, registry)
	if err != nil {
		t.Fatal(err)
	}
	health := newHealthServer("", metricsRegistry, registry)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	rounds := make(chan struct{}, 2)
	stopped := make(chan struct{})
	blocked := make(chan struct{})
	release := make(chan struct{})
	secondBlocked := make(chan struct{})
	allowRecovery := make(chan struct{})
	var attempts atomic.Uint32
	go runObservedLoop(ctx, time.Nanosecond, registry, metrics, []workerreadiness.Family{workerreadiness.FamilyPlant}, func(context.Context) observedRound {
		if attempts.Add(1) == 1 {
			close(blocked)
			<-release
			return observedRound{err: errors.New("transient Redis delay/error")}
		}
		if attempts.Load() == 2 {
			close(secondBlocked)
			<-allowRecovery
		}
		return observedRound{due: map[workerreadiness.Family]workerreadiness.DueObservation{
			workerreadiness.FamilyPlant: {DepthAvailable: true, OldestDueAgeAvailable: true},
		}}
	}, started, rounds, stopped)
	<-started
	<-blocked
	assertStatus(t, health.Handler, "/healthz", http.StatusOK)
	assertStatus(t, health.Handler, "/readyz", http.StatusServiceUnavailable)

	close(release)
	<-rounds
	<-secondBlocked
	assertStatus(t, health.Handler, "/healthz", http.StatusOK)
	assertStatus(t, health.Handler, "/readyz", http.StatusServiceUnavailable)
	close(allowRecovery)
	<-rounds
	assertStatus(t, health.Handler, "/healthz", http.StatusOK)
	assertStatus(t, health.Handler, "/readyz", http.StatusOK)
	cancel()
	<-stopped
}

func TestRemoteRouteProjectionUnknownFailsWorkerReadinessOnly(t *testing.T) {
	health := newHealthServer("", prometheus.NewRegistry(), nil, func() bool { return false })
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	response := httptest.NewRecorder()
	health.Handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), `"route_projection_required":true`) ||
		!strings.Contains(response.Body.String(), `"route_projection_fresh":false`) {
		t.Fatalf("route readiness response code/body=%d/%s", response.Code, response.Body.String())
	}
	assertStatus(t, health.Handler, "/healthz", http.StatusOK)
}

func TestRemoteAssignmentMetricsExposeTransportRecovery(t *testing.T) {
	metricsRegistry := prometheus.NewRegistry()
	pump := &remoteroute.RemoteAssignmentPump{}
	assignment := &authoritycomposition.AssignmentComposition{Pump: pump}
	registerRemoteAssignmentMetrics(metricsRegistry, assignment)
	pump.Reconnects.Add(2)
	pump.Resyncs.Add(3)

	health := newHealthServer("", metricsRegistry, nil)
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	health.Handler.ServeHTTP(response, request)
	body := response.Body.String()
	if response.Code != http.StatusOK ||
		!strings.Contains(body, "farm_actor_route_assignment_reconnects_total 2") ||
		!strings.Contains(body, "farm_actor_route_assignment_resyncs_total 3") {
		t.Fatalf("route recovery metrics code/body=%d/%s", response.Code, body)
	}
}

func TestEmptySweepBackoffIsBounded(t *testing.T) {
	base := 100 * time.Millisecond
	max := 2 * time.Second
	want := []time.Duration{base, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, 1600 * time.Millisecond, max, max}
	for index, expected := range want {
		if got := emptySweepBackoff(base, max, index+1); got != expected {
			t.Fatalf("empty sweep %d delay=%s want=%s", index+1, got, expected)
		}
	}
}

func TestAdaptiveShardSweepBacksOffOnlyAfterCompleteEmptySweep(t *testing.T) {
	base, max := time.Second, 4*time.Second
	var sweep adaptiveShardSweep
	if got := sweep.next(base, max, 7, 4, 2, 0, nil); got != base || sweep.emptySweeps != 0 {
		t.Fatalf("partial sweep delay=%s state=%+v", got, sweep)
	}
	if got := sweep.next(base, max, 7, 4, 2, 0, nil); got != base || sweep.emptySweeps != 1 {
		t.Fatalf("first complete empty sweep delay=%s state=%+v", got, sweep)
	}
	if got := sweep.next(base, max, 7, 4, 4, 0, nil); got != 2*time.Second || sweep.emptySweeps != 2 {
		t.Fatalf("second empty sweep delay=%s state=%+v", got, sweep)
	}
	if got := sweep.next(base, max, 7, 4, 1, 1, nil); got != base || sweep.emptySweeps != 0 || sweep.scanned != 0 {
		t.Fatalf("work reset delay=%s state=%+v", got, sweep)
	}
	sweep.emptySweeps = 3
	if got := sweep.next(base, max, 7, 4, 1, 0, errors.New("redis unavailable")); got != base || sweep.emptySweeps != 0 {
		t.Fatalf("error reset delay=%s state=%+v", got, sweep)
	}
}

func TestAdaptiveShardSweepResetsOnAssignmentRevision(t *testing.T) {
	base := time.Second
	sweep := adaptiveShardSweep{revision: 3, owned: 4, scanned: 2, emptySweeps: 2}
	if got := sweep.next(base, 4*time.Second, 4, 6, 2, 0, nil); got != base || sweep.revision != 4 || sweep.owned != 6 || sweep.scanned != 2 || sweep.emptySweeps != 0 {
		t.Fatalf("revision reset delay=%s state=%+v", got, sweep)
	}
}

func TestScanLoopPanicMarksNotReadyWithoutTakingLivenessDown(t *testing.T) {
	registry := mainTestRegistry(t)
	metricsRegistry := prometheus.NewRegistry()
	metrics, err := workerreadiness.NewMetrics(metricsRegistry, registry)
	if err != nil {
		t.Fatal(err)
	}
	health := newHealthServer("", metricsRegistry, registry)
	started := make(chan struct{})
	stopped := make(chan struct{})
	go runObservedLoop(context.Background(), time.Second, registry, metrics,
		[]workerreadiness.Family{workerreadiness.FamilyPlant}, func(context.Context) observedRound {
			panic("fixture panic")
		}, started, nil, stopped)
	<-started
	<-stopped
	assertStatus(t, health.Handler, "/healthz", http.StatusOK)
	assertStatus(t, health.Handler, "/readyz", http.StatusServiceUnavailable)
	plant := registry.Snapshot().Families[0]
	if plant.State != workerreadiness.StateFailed || plant.ScanLoopAlive || plant.LastErrorClass != "LOOP_PANIC" {
		t.Fatalf("panic lifecycle=%+v", plant)
	}
}

func TestProductionFamilyMatrixKeepsFailedExternalFriendProjectionVisibleAndNonBlocking(t *testing.T) {
	registry, err := workerreadiness.NewRegistry(workerFamilyConfigs(time.Second, false, false), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range []workerreadiness.Family{
		workerreadiness.FamilyPlant, workerreadiness.FamilyHarvest, workerreadiness.FamilyCare, workerreadiness.FamilyTheft,
	} {
		if err := registry.SetDependencies(family, workerreadiness.Dependencies{
			RepositoryInitialized: true, RouteProviderUsable: true, ParticipantClientUsable: true,
		}); err != nil {
			t.Fatal(err)
		}
		handle, err := registry.StartLoop(family)
		if err != nil {
			t.Fatal(err)
		}
		handle.ScanSucceeded(time.Now(), workerreadiness.DueObservation{DepthAvailable: true, OldestDueAgeAvailable: true})
	}
	snapshot := registry.Snapshot()
	if !snapshot.Ready || snapshot.State != workerreadiness.StateReady {
		t.Fatalf("failed non-critical external projection masked owned Family readiness: %+v", snapshot)
	}
	assertStatus(t, newHealthServer("", prometheus.NewRegistry(), registry).Handler, "/readyz", http.StatusOK)
	friend := familySnapshot(snapshot, workerreadiness.FamilyFriendProjection)
	if friend.State != workerreadiness.StateFailed || !friend.Enabled || friend.Critical {
		t.Fatalf("Friend projection row=%+v", friend)
	}
	guard := familySnapshot(snapshot, workerreadiness.FamilyGuardSync)
	if guard.State != workerreadiness.StateDisabled || guard.Critical {
		t.Fatalf("disabled Guard row=%+v", guard)
	}
	mail := familySnapshot(snapshot, workerreadiness.FamilyMail)
	if mail.State != workerreadiness.StateDisabled || mail.Critical {
		t.Fatalf("disabled Mail row=%+v", mail)
	}
}

func TestEnabledOptionalFamiliesAreCriticalAndFailClosedWithoutImplementations(t *testing.T) {
	registry, err := workerreadiness.NewRegistry(workerFamilyConfigs(time.Second, true, true), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := registry.Snapshot()
	for _, family := range []workerreadiness.Family{workerreadiness.FamilyGuardSync, workerreadiness.FamilyMail} {
		item := familySnapshot(snapshot, family)
		if !item.Enabled || !item.Critical || item.State != workerreadiness.StateFailed {
			t.Fatalf("enabled optional Family %s=%+v", family, item)
		}
	}
	if snapshot.Ready || snapshot.State != workerreadiness.StateFailed {
		t.Fatalf("enabled unimplemented optional Families did not fail closed: %+v", snapshot)
	}
}

// TestGuardSyncDefaultsOnInAuthorityModeSoPetDefenceCannotSilentlyLapse 钉死
// 守卫同步的默认值。这个默认值曾经是 false,是 GUARD_SYNC 还没实现时留下的;
// 实现补齐后没人改回来,于是喂狗会推进 Player 侧 guard_until、挂出
// GuardSyncOrder,却没有消费者把快照送到 FarmActor——防卫静默失效,偷菜照常
// 成功,而且没有任何报错。回归成 false 不会被别的测试发现,只能在这里钉住。
func TestGuardSyncDefaultsOnInAuthorityModeSoPetDefenceCannotSilentlyLapse(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value string
		mode  authoritycomposition.Mode
		want  bool
	}{
		{name: "authority default is on", mode: authoritycomposition.ModeAuthority, want: true},
		{name: "legacy default stays off", mode: authoritycomposition.ModeLegacy, want: false},
		{name: "operator can still force it off", value: "false", mode: authoritycomposition.ModeAuthority, want: false},
		{name: "operator can still force it on", value: "true", mode: authoritycomposition.ModeLegacy, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("GUARD_SYNC_WORKER_ENABLED", tc.value)
			if got := guardSyncEnabled(tc.mode); got != tc.want {
				t.Fatalf("guardSyncEnabled(%s) with env %q = %v, want %v", tc.mode, tc.value, got, tc.want)
			}
		})
	}
}

func familySnapshot(snapshot workerreadiness.Snapshot, family workerreadiness.Family) workerreadiness.FamilySnapshot {
	for _, item := range snapshot.Families {
		if item.Family == family {
			return item
		}
	}
	return workerreadiness.FamilySnapshot{}
}

func assertStatus(t *testing.T, handler http.Handler, path string, want int) {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, path, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != want {
		t.Fatalf("%s status=%d want=%d body=%s", path, response.Code, want, response.Body.String())
	}
}

func setWorkerTupleEnvironment(t *testing.T) {
	t.Helper()
	for name, value := range map[string]string{
		rediskey.RedisNamespaceEnvironmentEnv: "worker-test",
		rediskey.RedisNamespaceZoneEnv:        "zone7",
		rediskey.RedisPhysicalProfileEnv:      "redis-worker-v7",
		rediskey.RedisAuthorityLayoutEnv:      "v1",
		rediskey.RedisWorkflowLayoutEnv:       "v1",
		rediskey.RedisWorkflowHashEnv:         "1",
		rediskey.RedisWorkflowBucketCountEnv:  "512",
	} {
		t.Setenv(name, value)
	}
}

func TestRemoteWorkerReceivesExactTuple(t *testing.T) {
	setWorkerTupleEnvironment(t)
	tuple, err := requiredRemoteKeyspaceTuple(authoritycomposition.MembershipModeComposed, authoritycomposition.AssignmentModeRemoteWatch)
	if err != nil {
		t.Fatal(err)
	}
	profile, err := workerPhysicalProfile(tuple)
	if err != nil {
		t.Fatal(err)
	}
	if profile.ProfileVersion != "redis-worker-v7" || profile.Namespace.Environment != "worker-test" ||
		profile.Namespace.Zone != "zone7" || profile.AuthorityLayoutVersion != "v1" ||
		profile.WorkflowLayoutVersion != "v1" || profile.WorkflowHashVersion != 1 || profile.WorkflowBucketCount != 512 {
		t.Fatalf("profile=%+v", profile)
	}
}

func TestRemoteWorkerNoDefaultNamespace(t *testing.T) {
	setWorkerTupleEnvironment(t)
	t.Setenv(rediskey.RedisNamespaceEnvironmentEnv, "")
	if _, err := requiredRemoteKeyspaceTuple(authoritycomposition.MembershipModeComposed, authoritycomposition.AssignmentModeRemoteWatch); err == nil {
		t.Fatal("remote worker accepted a missing namespace through static defaults")
	}
}

func TestWorkerUsesExpectedNamespace(t *testing.T) {
	setWorkerTupleEnvironment(t)
	tuple, err := requiredRemoteKeyspaceTuple(authoritycomposition.MembershipModeComposed, authoritycomposition.AssignmentModeRemoteWatch)
	if err != nil {
		t.Fatal(err)
	}
	profile, err := workerPhysicalProfile(tuple)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := rediskey.NewWorkflowKeys(profile)
	if err != nil {
		t.Fatal(err)
	}
	workflowID, err := workidentity.StableOpaqueID("worker-keyspace-proof")
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := profile.WorkflowBucket(workflowID)
	if err != nil {
		t.Fatal(err)
	}
	due, err := keys.DueIndex(bucket)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(due.String(), "classic-farm:worker-test:zone7:wf:v1:") {
		t.Fatalf("worker due key=%q", due.String())
	}
}
