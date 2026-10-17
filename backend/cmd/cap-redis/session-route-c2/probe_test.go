package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"farmgame/internal/friend"
	"farmgame/internal/logic/downlink"
	"farmgame/internal/storage/redis/rediskey"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func testDeps(t *testing.T) (context.Context, *probeDeps, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	hook := newRedisMetricHook()
	client := redis.NewClient(&redis.Options{Addr: server.Addr(), Dialer: hook.countingDialer})
	client.AddHook(hook)
	t.Cleanup(func() { _ = client.Close() })
	deps := newProbeDeps(client, hook, t.Name())
	t.Cleanup(func() {
		_ = cleanupExactKeys(deps)
	})
	return context.Background(), deps, server
}

func TestExactKeyCleanupDoesNotDeleteForeignSentinel(t *testing.T) {
	ctx, deps, _ := testDeps(t)
	foreign := "session:current:{999999999}:hash"
	if err := deps.client.Set(ctx, foreign, "sentinel", 0).Err(); err != nil {
		t.Fatal(err)
	}
	card := buildFixtureCard(fixturePlan{
		FriendCount: 3, OnlineRatioPct: 100,
		PlayerIDBase: deterministicPlayerIDBase(deps.runID, 0, 0),
		RunID:        deps.runID,
	})
	if err := installSessionRoutes(
		ctx, deps.client, deps.authority, deps.tracker,
		deps.runID, deps.ownershipGeneration, card, routeStateActive,
	); err != nil {
		t.Fatal(err)
	}
	owned := deps.tracker.Keys()
	if len(owned) == 0 {
		t.Fatal("expected owned keys")
	}
	for _, key := range owned {
		if key == rediskey.SessionAuthorityEpoch() {
			t.Fatalf("tracker must not own global epoch key %s", key)
		}
	}
	if err := deps.tracker.Cleanup(ctx, deps.client); err != nil {
		t.Fatal(err)
	}
	for _, key := range owned {
		if deps.client.Exists(ctx, key).Val() != 0 {
			t.Fatalf("owned key remains after cleanup: %s", key)
		}
	}
	if deps.client.Exists(ctx, foreign).Val() != 1 {
		t.Fatalf("foreign sentinel deleted: %s", foreign)
	}
}

func TestCleanupPreservesPreexistingGlobalEpoch(t *testing.T) {
	ctx, deps, _ := testDeps(t)
	epochKey := rediskey.SessionAuthorityEpoch()
	if err := deps.client.Set(ctx, epochKey, "99", 0).Err(); err != nil {
		t.Fatal(err)
	}
	card := buildFixtureCard(fixturePlan{
		FriendCount: 2, OnlineRatioPct: 100,
		PlayerIDBase: deterministicPlayerIDBase(deps.runID, 1, 0),
		RunID:        deps.runID,
	})
	if err := installSessionRoutes(
		ctx, deps.client, deps.authority, deps.tracker,
		deps.runID, deps.ownershipGeneration, card, routeStateActive,
	); err != nil {
		t.Fatal(err)
	}
	for _, key := range deps.tracker.Keys() {
		if key == epochKey {
			t.Fatal("SessionAuthorityEpoch must never be tracked")
		}
	}
	if err := cleanupExactKeys(deps); err != nil {
		t.Fatal(err)
	}
	got, err := deps.client.Get(ctx, epochKey).Result()
	if err != nil {
		t.Fatalf("global epoch missing after cleanup: %v", err)
	}
	if got != "99" {
		t.Fatalf("global epoch mutated: got=%s want=99", got)
	}
}

func TestCleanupSkipsTrackedKeyReplacedByForeignValue(t *testing.T) {
	ctx, deps, _ := testDeps(t)
	card := buildFixtureCard(fixturePlan{
		FriendCount: 1, OnlineRatioPct: 100,
		PlayerIDBase: deterministicPlayerIDBase(deps.runID, 2, 0),
		RunID:        deps.runID,
	})
	if err := installSessionRoutes(
		ctx, deps.client, deps.authority, deps.tracker,
		deps.runID, deps.ownershipGeneration, card, routeStateActive,
	); err != nil {
		t.Fatal(err)
	}
	playerID := card.ExpectedActive[0]
	key := rediskey.Session(playerID)
	if err := deps.client.Del(ctx, key).Err(); err != nil {
		t.Fatal(err)
	}
	if err := deps.client.Set(ctx, key, "foreign-replacement", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := cleanupExactKeys(deps); err != nil {
		t.Fatal(err)
	}
	got, err := deps.client.Get(ctx, key).Result()
	if err != nil {
		t.Fatalf("foreign replacement deleted: %v", err)
	}
	if got != "foreign-replacement" {
		t.Fatalf("foreign replacement mutated: %q", got)
	}
}

func TestCleanupAtomicCompareAndDeleteSkipsRaceReplacement(t *testing.T) {
	ctx, deps, _ := testDeps(t)
	card := buildFixtureCard(fixturePlan{
		FriendCount: 1, OnlineRatioPct: 100,
		PlayerIDBase: deterministicPlayerIDBase(deps.runID, 3, 0),
		RunID:        deps.runID,
	})
	if err := installSessionRoutes(
		ctx, deps.client, deps.authority, deps.tracker,
		deps.runID, deps.ownershipGeneration, card, routeStateActive,
	); err != nil {
		t.Fatal(err)
	}
	playerID := card.ExpectedActive[0]
	key := rediskey.Session(playerID)
	const foreignValue = "foreign-race-winner"
	// Deterministic TOCTOU: replace the key after owned snapshot preparation and
	// immediately before the atomic compare-and-delete script executes.
	deps.tracker.beforeAtomicDelete = func(item ownedSessionKey) {
		if item.Key != key {
			return
		}
		if err := deps.client.Del(ctx, key).Err(); err != nil {
			t.Fatalf("race replace del: %v", err)
		}
		if err := deps.client.Set(ctx, key, foreignValue, 0).Err(); err != nil {
			t.Fatalf("race replace set: %v", err)
		}
	}
	t.Cleanup(func() { deps.tracker.beforeAtomicDelete = nil })

	if err := cleanupExactKeys(deps); err != nil {
		t.Fatal(err)
	}
	got, err := deps.client.Get(ctx, key).Result()
	if err != nil {
		t.Fatalf("foreign race winner deleted by non-atomic cleanup: %v", err)
	}
	if got != foreignValue {
		t.Fatalf("foreign race winner mutated: %q", got)
	}
}

func TestInstallMissingRefusesPreexistingForeignSessionKey(t *testing.T) {
	ctx, deps, _ := testDeps(t)
	card := buildFixtureCard(fixturePlan{
		FriendCount: 3, OnlineRatioPct: 0,
		RouteStateMode: routeStateMissing,
		PlayerIDBase:   deterministicPlayerIDBase(deps.runID, 4, 0),
		RunID:          deps.runID,
	})
	if len(card.ExpectedMissing) == 0 {
		t.Fatal("expected missing friends")
	}
	foreignID := card.ExpectedMissing[1]
	key := rediskey.Session(foreignID)
	if err := deps.client.Set(ctx, key, "foreign-preexist", 0).Err(); err != nil {
		t.Fatal(err)
	}
	err := installSessionRoutes(
		ctx, deps.client, deps.authority, deps.tracker,
		deps.runID, deps.ownershipGeneration, card, routeStateMissing,
	)
	if err == nil {
		t.Fatal("expected missing-state install to fail on pre-existing foreign Session key")
	}
	if !strings.Contains(err.Error(), "pre-exists") {
		t.Fatalf("unexpected install error: %v", err)
	}
	got, getErr := deps.client.Get(ctx, key).Result()
	if getErr != nil {
		t.Fatalf("foreign key must survive refused install: %v", getErr)
	}
	if got != "foreign-preexist" {
		t.Fatalf("foreign key mutated: %q", got)
	}
	if len(deps.tracker.Keys()) != 0 {
		t.Fatalf("tracker must stay empty after refused missing install: %v", deps.tracker.Keys())
	}
}

func TestExpiredAndMalformedFailClosedOnProductionKeyspace(t *testing.T) {
	ctx, deps, _ := testDeps(t)
	for i, mode := range []routeState{routeStateExpired, routeStateMalformed} {
		plan := fixturePlan{
			FriendCount: 5, OnlineRatioPct: 0, DispatchBatch: 10,
			RouteStateMode: mode, Iterations: 1, CellIndex: 10 + i, RunID: deps.runID,
			PlayerIDBase: deterministicPlayerIDBase(deps.runID, 10+i, 0),
			CellName:     string(mode),
		}
		card := buildFixtureCard(plan)
		if err := installSessionRoutes(
			ctx, deps.client, deps.authority, deps.tracker,
			deps.runID, deps.ownershipGeneration, card, mode,
		); err != nil {
			t.Fatal(err)
		}
		for _, playerID := range append(card.ExpectedExpired, card.ExpectedMalformed...) {
			key := rediskey.Session(playerID)
			if !strings.HasPrefix(key, "session:current:{") || !strings.HasSuffix(key, "}:hash") {
				t.Fatalf("non-production keyspace: %s", key)
			}
			if deps.client.Exists(ctx, key).Val() != 1 {
				t.Fatalf("expected fixture key missing: %s", key)
			}
		}
		routes, err := deps.authority.BatchGetActiveRoutes(ctx, card.FriendIDs)
		if err != nil {
			t.Fatal(err)
		}
		if len(routes) != 0 {
			t.Fatalf("mode=%s must fail closed, got routes=%v", mode, routes)
		}
	}
}

func TestOnlineRatiosChangeFixtureCardinality(t *testing.T) {
	friends := 100
	seen := map[int]struct{}{}
	for _, ratio := range onlineRatiosPct {
		card := buildFixtureCard(fixturePlan{FriendCount: friends, OnlineRatioPct: ratio, PlayerIDBase: 1})
		got := len(card.ExpectedActive)
		want := expectedOnlineCount(friends, ratio)
		if got != want {
			t.Fatalf("ratio=%d active=%d want=%d", ratio, got, want)
		}
		seen[got] = struct{}{}
	}
	if len(seen) != len(onlineRatiosPct) {
		t.Fatalf("online ratios did not change cardinality uniquely: %v", seen)
	}
}

func TestDispatchBatchSizesObservedAfterCompletion(t *testing.T) {
	ctx, deps, _ := testDeps(t)
	for i, batch := range dispatchBatches {
		plan := fixturePlan{
			FriendCount: 30, OnlineRatioPct: 100, DispatchBatch: batch,
			RouteStateMode: routeStateActive, Iterations: 1, CellIndex: 20 + i, RunID: deps.runID,
			PlayerIDBase: deterministicPlayerIDBase(deps.runID, 20+i, 0),
			CellName:     "batch-observe",
		}
		card := buildFixtureCard(plan)
		index := friend.NewMemoryFriendIndexRepository()
		if err := installFriendIndex(ctx, index, card); err != nil {
			t.Fatal(err)
		}
		if err := installSessionRoutes(
			ctx, deps.client, deps.authority, deps.tracker,
			deps.runID, deps.ownershipGeneration, card, routeStateActive,
		); err != nil {
			t.Fatal(err)
		}
		gate := newGateCapture()
		config := downlink.DefaultDispatcherConfig()
		config.BatchSize = batch
		config.LaneQueueSize = 4096
		config.RPCTimeout = time.Second
		dispatcher := downlink.NewDispatcherWithConfig(gate, config)
		transport, err := friend.NewDirectOpportunityHintTransport(index, deps.authority, dispatcher)
		if err != nil {
			t.Fatal(err)
		}
		if err := transport.PublishTargetedHint(ctx, card.OwnerID, card.OwnerID, 1, 1); err != nil {
			dispatcher.Close()
			t.Fatal(err)
		}
		stats := transport.Stats()
		waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		if err := gate.waitDelivered(waitCtx, int(stats.Enqueued)); err != nil {
			cancel()
			dispatcher.Close()
			t.Fatalf("batch=%d wait: %v", batch, err)
		}
		cancel()
		dispatcher.Close()
		sizes, delivered, _ := gate.snapshot()
		if delivered != int(stats.Enqueued) || delivered != 30 {
			t.Fatalf("batch=%d delivered=%d enqueued=%d", batch, delivered, stats.Enqueued)
		}
		sum := 0
		for _, size := range sizes {
			sum += size
			if size <= 0 || size > batch {
				t.Fatalf("batch=%d observed size=%d invalid", batch, size)
			}
			if batch == 1 && size != 1 {
				t.Fatalf("batch=1 observed size=%d", size)
			}
		}
		if sum != delivered {
			t.Fatalf("batch=%d sum=%d delivered=%d sizes=%v", batch, sum, delivered, sizes)
		}
	}
}

func TestProbeSourceForbidsRedisAdminOperations(t *testing.T) {
	root := "."
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	// Split tokens so this test file itself does not contain the forbidden
	// call shapes as contiguous source text in production files under scan.
	forbiddenSnippets := []string{
		"namespaceKeyPatterns",
		"deleteNamespace",
		"Config" + "Set(",
		"SlowLog" + "Reset",
		"Flush" + "All(",
		"Flush" + "DB(",
		"FLUSH" + "ALL",
		"FLUSH" + "DB",
		"SLOWLOG " + "RESET",
		"CONFIG " + "SET",
	}
	var files []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasSuffix(name, ".go") && !strings.HasSuffix(name, "_test.go") {
			files = append(files, filepath.Join(root, name))
		}
	}
	if len(files) == 0 {
		t.Fatal("no probe sources found")
	}
	for _, path := range files {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		text := string(data)
		for _, snippet := range forbiddenSnippets {
			if strings.Contains(text, snippet) {
				t.Fatalf("%s contains forbidden snippet %q", path, snippet)
			}
		}
		// Reject broad SCAN+DEL cleanup loops copied from the parent harness.
		if strings.Contains(text, "Scan(") && strings.Contains(text, "Del(") {
			t.Fatalf("%s combines Scan and Del; exact-key cleanup only", path)
		}
	}
}

func TestMatrixPlansCoverRequiredDimensions(t *testing.T) {
	plans := matrixPlans("full", 1, "matrix-run")
	var (
		friendsSeen = map[int]bool{}
		ratiosSeen  = map[int]bool{}
		batchesSeen = map[int]bool{}
		statesSeen  = map[routeState]bool{}
	)
	for _, plan := range plans {
		friendsSeen[plan.FriendCount] = true
		if plan.FriendCount > 0 && plan.RouteStateMode == routeStateActive {
			ratiosSeen[plan.OnlineRatioPct] = true
		}
		if plan.RouteStateMode == routeStateActive {
			batchesSeen[plan.DispatchBatch] = true
		}
		statesSeen[plan.RouteStateMode] = true
		wantBase := deterministicPlayerIDBase(plan.RunID, plan.CellIndex, 0)
		if plan.PlayerIDBase != wantBase {
			t.Fatalf("cell %d player base not deterministic: got=%d want=%d", plan.CellIndex, plan.PlayerIDBase, wantBase)
		}
	}
	for _, friends := range friendCounts {
		if !friendsSeen[friends] {
			t.Fatalf("missing friend count %d", friends)
		}
	}
	for _, ratio := range onlineRatiosPct {
		if !ratiosSeen[ratio] {
			t.Fatalf("missing online ratio %d", ratio)
		}
	}
	for _, batch := range dispatchBatches {
		if !batchesSeen[batch] {
			t.Fatalf("missing dispatch batch %d", batch)
		}
	}
	for _, state := range routeStateModes {
		if state == routeStateActive {
			continue
		}
		if !statesSeen[state] {
			t.Fatalf("missing route state %s", state)
		}
	}
}

func TestSmokeProbeCorrectnessOnMiniredis(t *testing.T) {
	ctx, deps, _ := testDeps(t)
	report, err := runProbe(ctx, deps, "smoke", 1, deps.runID)
	if err != nil {
		t.Fatal(err)
	}
	if report.TopologyLabel != topologySingleNode || report.ClusterVerification != topologyClusterNV {
		t.Fatalf("topology labels=%s/%s", report.TopologyLabel, report.ClusterVerification)
	}
	if report.Supports30MDAU {
		t.Fatal("single-node probe must not claim 30M DAU support")
	}
	if !report.CorrectnessOK {
		t.Fatalf("correctness failed: %+v", report.Cells)
	}
	for _, cell := range report.Cells {
		if cell.ObservedMalformed.Available || cell.MalformedCount.Available {
			t.Fatalf("observed malformed must be unavailable: %+v", cell)
		}
		if cell.RouteStateMode == string(routeStateMalformed) && cell.ExpectedMalformed == 0 {
			t.Fatalf("malformed cell missing expected_malformed: %+v", cell)
		}
	}
}

func TestObservedMalformedUnavailableWhileFailClosedHolds(t *testing.T) {
	ctx, deps, _ := testDeps(t)
	plan := fixturePlan{
		FriendCount: 8, OnlineRatioPct: 0, DispatchBatch: 10,
		RouteStateMode: routeStateMalformed, Iterations: 1,
		CellIndex: 40, RunID: deps.runID,
		PlayerIDBase: deterministicPlayerIDBase(deps.runID, 40, 0),
		CellName:     "malformed-unavailable",
	}
	cell, err := runCell(ctx, deps, plan)
	if err != nil {
		t.Fatal(err)
	}
	if !cell.CorrectnessOK {
		t.Fatalf("fail-closed correctness failed: %v", cell.Violations)
	}
	if cell.ExpectedMalformed != 8 {
		t.Fatalf("expected_malformed=%d want=8", cell.ExpectedMalformed)
	}
	if cell.ObservedMalformed.Available || cell.MalformedCount.Available {
		t.Fatalf("observed malformed must be unavailable, got %+v / %+v", cell.ObservedMalformed, cell.MalformedCount)
	}
	if cell.FilteredCount != 8 {
		t.Fatalf("collapsed missing_routes observed_filtered=%d want=8", cell.FilteredCount)
	}
}

func TestCleanupAfterCancelledContextStillRuns(t *testing.T) {
	ctx, deps, _ := testDeps(t)
	card := buildFixtureCard(fixturePlan{
		FriendCount: 2, OnlineRatioPct: 100,
		PlayerIDBase: deterministicPlayerIDBase(deps.runID, 50, 0),
		RunID:        deps.runID,
	})
	if err := installSessionRoutes(
		ctx, deps.client, deps.authority, deps.tracker,
		deps.runID, deps.ownershipGeneration, card, routeStateActive,
	); err != nil {
		t.Fatal(err)
	}
	owned := deps.tracker.Keys()
	if len(owned) == 0 {
		t.Fatal("expected owned keys")
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	report, err := runProbe(cancelled, deps, "smoke", 1, deps.runID)
	if err == nil {
		t.Fatal("expected cancelled probe error")
	}
	if !errors.Is(err, context.Canceled) {
		// First Redis op should surface cancellation; cleanup must not swallow it.
		if !strings.Contains(err.Error(), "canceled") && !strings.Contains(err.Error(), "cancelled") {
			t.Fatalf("expected cancellation in error, got %v", err)
		}
	}
	_ = report
	for _, key := range owned {
		if deps.client.Exists(ctx, key).Val() != 0 {
			t.Fatalf("owned key survived cancelled-context cleanup: %s", key)
		}
	}
}

func TestCleanupAfterDeadlineStillRunsAndJoinsFailure(t *testing.T) {
	ctx, deps, _ := testDeps(t)
	card := buildFixtureCard(fixturePlan{
		FriendCount: 2, OnlineRatioPct: 100,
		PlayerIDBase: deterministicPlayerIDBase(deps.runID, 51, 0),
		RunID:        deps.runID,
	})
	if err := installSessionRoutes(
		ctx, deps.client, deps.authority, deps.tracker,
		deps.runID, deps.ownershipGeneration, card, routeStateActive,
	); err != nil {
		t.Fatal(err)
	}
	owned := append([]string(nil), deps.tracker.Keys()...)

	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if err := expired.Err(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("setup deadline context: %v", err)
	}
	// Probe deadline/cancel must not prevent cleanup: cleanupExactKeys uses a fresh ctx.
	report, err := withCleanup(deps, probeReport{RunID: deps.runID}, context.DeadlineExceeded)
	if err == nil {
		t.Fatal("expected original deadline error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("original deadline error lost: %v", err)
	}
	_ = report
	for _, key := range owned {
		if deps.client.Exists(ctx, key).Val() != 0 {
			t.Fatalf("owned key survived deadline cleanup: %s", key)
		}
	}

	// Force a cleanup failure and ensure it is joined with the original error.
	deps.tracker.TrackOwned(rediskey.Session(1), "marker", "1")
	_ = deps.client.Close()
	_, joined := withCleanup(deps, probeReport{}, context.Canceled)
	if joined == nil {
		t.Fatal("expected joined cleanup failure")
	}
	if !errors.Is(joined, context.Canceled) {
		t.Fatalf("original error not preserved: %v", joined)
	}
	if !strings.Contains(joined.Error(), "exact-key cleanup") {
		t.Fatalf("cleanup failure not surfaced: %v", joined)
	}
}
