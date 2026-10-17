package friend

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"farmgame/internal/storage/redis/rediskey"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newStandaloneNavigationRepositories(t *testing.T) (*RedisFriendshipRepositories, func()) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	namespace, err := rediskey.NewNamespace("test", "z1")
	if err != nil {
		t.Fatal(err)
	}
	repositories, err := NewRedisFriendshipRepositories(client, RedisFriendshipOptions{
		Profile: DefaultFriendRedisProfile(namespace), Limits: FriendRedisLimits{MaxFriends: 300},
	})
	if err != nil {
		_ = client.Close()
		t.Fatal(err)
	}
	return repositories, func() { _ = client.Close() }
}

func topKStateKeyFor(t *testing.T, repositories *RedisFriendshipRepositories, viewer uint64) rediskey.RedisKey {
	t.Helper()
	key, err := repositories.Navigation.keys.FriendNavigationTopKStateKey(viewer)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func TestRedisNavigationRepositoryRoundTripsCompleteStateAndCASProtectsVisited(t *testing.T) {
	repositories, cleanup := newStandaloneNavigationRepositories(t)
	defer cleanup()
	ctx := context.Background()
	const viewer uint64 = 91

	navigation := NewNavigation(viewer, 7, 60_000, 1_000)
	navigation.ScanPosition = 3
	navigation.CandidateWindow = []CandidateWindowEntry{{
		FarmID: 92, SourceFarmVersion: 4, ActionableAtMs: 900, ActionMask: ActionCanWater, ValidatedAtMs: 1_001,
	}}
	navigation.pendingCandidates = []CandidateWindowEntry{{
		FarmID: 95, SourceFarmVersion: 5, ActionableAtMs: 800, ActionMask: ActionUnknown,
	}}
	navigation.coarseScanDone = true
	navigation.TailComplete = true
	if !navigation.MarkVisited(VisitFence{ViewerPlayerID: viewer, TargetFarmID: 93,
		ActiveSessionEpoch: 4, SubscriptionID: "sub-93"}) {
		t.Fatal("expected in-memory visited mark")
	}

	initial, err := repositories.Navigation.Load(ctx, viewer)
	if err != nil || initial.Navigation != nil || initial.Version != 0 {
		t.Fatalf("initial snapshot=%+v err=%v", initial, err)
	}
	if err := repositories.Navigation.Save(ctx, NavigationSnapshot{Navigation: navigation, Version: initial.Version}); err != nil {
		t.Fatalf("save initial navigation: %v", err)
	}

	loaded, err := repositories.Navigation.Load(ctx, viewer)
	if err != nil || loaded.Navigation == nil || loaded.Version != 1 {
		t.Fatalf("loaded snapshot=%+v err=%v", loaded, err)
	}
	if loaded.Navigation.RosterRevision != 7 || loaded.Navigation.ScanPosition != 3 ||
		loaded.Navigation.ExpireAtMs != navigation.ExpireAtMs || len(loaded.Navigation.CandidateWindow) != 1 ||
		loaded.Navigation.CandidateWindow[0] != navigation.CandidateWindow[0] ||
		len(loaded.Navigation.pendingCandidates) != 1 ||
		loaded.Navigation.pendingCandidates[0] != navigation.pendingCandidates[0] ||
		!loaded.Navigation.coarseScanDone || !loaded.Navigation.TailComplete {
		t.Fatalf("complete state was not preserved: %+v", loaded.Navigation)
	}
	if _, ok := loaded.Navigation.Visited[93]; !ok {
		t.Fatalf("visited state missing after round trip: %+v", loaded.Navigation.Visited)
	}

	stale := loaded
	if err := repositories.Navigation.MarkVisited(ctx, VisitFence{ViewerPlayerID: viewer, TargetFarmID: 92,
		ActiveSessionEpoch: 4, SubscriptionID: "sub-92"}); err != nil {
		t.Fatalf("concurrent visited mark: %v", err)
	}
	if err := repositories.Navigation.Save(ctx, stale); !errors.Is(err, ErrNavigationConflict) {
		t.Fatalf("stale save err=%v, want ErrNavigationConflict", err)
	}

	fresh, err := repositories.Navigation.Load(ctx, viewer)
	if err != nil || fresh.Navigation == nil || fresh.Version != 2 {
		t.Fatalf("fresh snapshot=%+v err=%v", fresh, err)
	}
	if _, ok := fresh.Navigation.Visited[93]; !ok {
		t.Fatalf("original visited mark lost: %+v", fresh.Navigation.Visited)
	}
	if _, ok := fresh.Navigation.Visited[92]; !ok {
		t.Fatalf("concurrent visited mark lost: %+v", fresh.Navigation.Visited)
	}
	if len(fresh.Navigation.CandidateWindow) != 0 {
		t.Fatalf("visited target remained in candidate window: %+v", fresh.Navigation.CandidateWindow)
	}
}

func TestFriendNavigationKeysShareOneRedisSlot(t *testing.T) {
	namespace, err := rediskey.NewNamespace("test", "z1")
	if err != nil {
		t.Fatal(err)
	}
	builder, err := NewFriendshipKeyBuilder(DefaultFriendRedisProfile(namespace))
	if err != nil {
		t.Fatal(err)
	}
	state, err := builder.FriendNavigationStateKey(91)
	if err != nil {
		t.Fatal(err)
	}
	visited, err := builder.FriendNavigationVisitedKey(91)
	if err != nil {
		t.Fatal(err)
	}
	fences, err := builder.FriendNavigationFencesKey(91)
	if err != nil {
		t.Fatal(err)
	}
	topK, err := builder.FriendNavigationTopKStateKey(91)
	if err != nil {
		t.Fatal(err)
	}
	// The atomic top-k save script touches all four keys in one Lua
	// operation; the same-slot assertion must cover every key it uses.
	if err := builder.AssertSameSlot(state, visited, fences, topK); err != nil {
		t.Fatalf("navigation keys are cross-slot: %v", err)
	}
}

// TestTopKEnableAfterFullTailNoTopKKeyExists covers the first top-k enable
// when the legacy full-tail record exists but no top-k record was ever
// written: the load fails closed to legacy, the reconciliation rebuilds, and
// the next atomic save publishes both versioned records.
func TestTopKEnableAfterFullTailNoTopKKeyExists(t *testing.T) {
	repositories, cleanup := newStandaloneNavigationRepositories(t)
	defer cleanup()
	ctx := context.Background()
	const viewer uint64 = 90

	// 1. Full-tail default writes only the legacy record; the top-k key must
	// not be created.
	legacy := NewNavigation(viewer, 7, 60_000, 1_000)
	legacy.ScanPosition = 4
	legacy.CandidateWindow = []CandidateWindowEntry{{FarmID: 92, SourceFarmVersion: 4, ActionableAtMs: 900, ActionMask: ActionCanWater, ValidatedAtMs: 1_001}}
	legacy.pendingCandidates = []CandidateWindowEntry{{FarmID: 95, SourceFarmVersion: 5, ActionableAtMs: 800, ActionMask: ActionUnknown}}
	legacy.coarseScanDone = true
	legacy.TailComplete = true
	if !legacy.MarkVisited(VisitFence{ViewerPlayerID: viewer, TargetFarmID: 93, ActiveSessionEpoch: 4, SubscriptionID: "sub-93"}) {
		t.Fatal("expected in-memory visited mark")
	}
	initial, err := repositories.Navigation.Load(ctx, viewer)
	if err != nil {
		t.Fatal(err)
	}
	if err := repositories.Navigation.Save(ctx, NavigationSnapshot{Navigation: legacy, Version: initial.Version}); err != nil {
		t.Fatalf("save full-tail navigation: %v", err)
	}
	topKKey := topKStateKeyFor(t, repositories, viewer)
	if exists, err := repositories.Navigation.client.Exists(ctx, topKKey.String()).Result(); err != nil || exists != 0 {
		t.Fatalf("full-tail save touched the top-k key: exists=%d err=%v", exists, err)
	}

	// 2. Enable top-k. No top-k record exists, so the load fails closed to
	// the legacy record (mode full-tail, complete tail).
	repositories.Navigation.ConfigureNavigationWindowMode(NavigationWindowModeTopK, DefaultTopKWindowSize)
	loaded, err := repositories.Navigation.Load(ctx, viewer)
	if err != nil || loaded.Navigation == nil {
		t.Fatalf("loaded=%+v err=%v", loaded, err)
	}
	if loaded.Navigation.WindowMode != NavigationWindowModeFullTail {
		t.Fatalf("missing top-k record must fail closed to legacy, got mode=%q", loaded.Navigation.WindowMode)
	}
	if loaded.Navigation.RosterRevision != 7 || !loaded.Navigation.TailComplete {
		t.Fatalf("legacy fallback lost state: %+v", loaded.Navigation)
	}
	if _, ok := loaded.Navigation.Visited[93]; !ok {
		t.Fatalf("visited mark lost in fallback: %+v", loaded.Navigation.Visited)
	}

	// 3. Reconcile to top-k rebuilds the scan; the atomic save publishes both
	// records with the same version.
	rebuilt := loaded.Navigation.ReconcileWindowMode(NavigationWindowModeTopK, DefaultTopKWindowSize)
	if rebuilt.coarseScanDone || rebuilt.TailComplete {
		t.Fatalf("top-k reconcile must force a rebuild: %+v", rebuilt)
	}
	snapshot := NavigationSnapshot{Navigation: rebuilt, Version: loaded.Version}
	if err := repositories.Navigation.Save(ctx, snapshot); err != nil {
		t.Fatalf("atomic top-k save: %v", err)
	}
	after, err := repositories.Navigation.Load(ctx, viewer)
	if err != nil || after.Navigation == nil {
		t.Fatalf("after top-k save=%+v err=%v", after, err)
	}
	if after.Navigation.WindowMode != NavigationWindowModeTopK || after.Navigation.WindowSize != DefaultTopKWindowSize {
		t.Fatalf("top-k record not served after enable: %+v", after.Navigation)
	}
	if _, ok := after.Navigation.Visited[93]; !ok {
		t.Fatalf("visited mark lost after top-k enable: %+v", after.Navigation.Visited)
	}
	// The two versioned records are written by one CAS, so versions match and
	// a later load serves the top-k record without a rebuild.
	stateKey, err := repositories.Navigation.keys.FriendNavigationStateKey(viewer)
	if err != nil {
		t.Fatal(err)
	}
	topKVersion, err := repositories.Navigation.client.HGet(ctx, topKKey.String(), navigationTopKVersionField).Result()
	if err != nil {
		t.Fatal(err)
	}
	legacyVersion, err := repositories.Navigation.client.HGet(ctx, stateKey.String(), navigationStateVersionField).Result()
	if err != nil {
		t.Fatal(err)
	}
	if topKVersion != legacyVersion {
		t.Fatalf("versioned records split: top-k=%s legacy=%s", topKVersion, legacyVersion)
	}
}

// TestMarkVisitedInterleavingCannotSplitVersions proves that a visited mark
// racing between Load and Save aborts the whole atomic save (no split
// records) and that subsequent saves heal instead of wedging.
func TestMarkVisitedInterleavingCannotSplitVersions(t *testing.T) {
	repositories, cleanup := newStandaloneNavigationRepositories(t)
	defer cleanup()
	ctx := context.Background()
	const viewer uint64 = 91

	repositories.Navigation.ConfigureNavigationWindowMode(NavigationWindowModeTopK, DefaultTopKWindowSize)
	navigation := NewNavigation(viewer, 7, 60_000, 1_000)
	navigation.CandidateWindow = []CandidateWindowEntry{{FarmID: 92, SourceFarmVersion: 4, ActionableAtMs: 900, ActionMask: ActionCanWater, ValidatedAtMs: 1_001}}
	navigation.coarseScanDone = true
	navigation.TailComplete = true
	initial, err := repositories.Navigation.Load(ctx, viewer)
	if err != nil {
		t.Fatal(err)
	}
	if err := repositories.Navigation.Save(ctx, NavigationSnapshot{Navigation: navigation, Version: initial.Version}); err != nil {
		t.Fatalf("initial top-k save: %v", err)
	}

	stale, err := repositories.Navigation.Load(ctx, viewer)
	if err != nil || stale.Navigation == nil || stale.Version != 1 {
		t.Fatalf("loaded=%+v err=%v", stale, err)
	}
	// MarkVisited advances only the legacy fence version. The stale snapshot
	// was loaded before the mark, so its CAS must fail as one atomic unit.
	if err := repositories.Navigation.MarkVisited(ctx, VisitFence{ViewerPlayerID: viewer, TargetFarmID: 95,
		ActiveSessionEpoch: 4, SubscriptionID: "sub-95"}); err != nil {
		t.Fatal(err)
	}
	if err := repositories.Navigation.Save(ctx, stale); !errors.Is(err, ErrNavigationConflict) {
		t.Fatalf("interleaved save err=%v, want ErrNavigationConflict", err)
	}

	// The interleaving must not wedge future saves: reload, rebuild and save.
	fresh, err := repositories.Navigation.Load(ctx, viewer)
	if err != nil || fresh.Navigation == nil {
		t.Fatalf("fresh=%+v err=%v", fresh, err)
	}
	if _, ok := fresh.Navigation.Visited[95]; !ok {
		t.Fatalf("visited mark lost: %+v", fresh.Navigation.Visited)
	}
	// The legacy fence advanced past the top-k record, so the top-k record is
	// stale and the load fails closed to the legacy placeholder.
	if fresh.Navigation.WindowMode != NavigationWindowModeFullTail {
		t.Fatalf("stale top-k record served after visit: mode=%q", fresh.Navigation.WindowMode)
	}
	rebuilt := fresh.Navigation.ReconcileWindowMode(NavigationWindowModeTopK, DefaultTopKWindowSize)
	if err := repositories.Navigation.Save(ctx, NavigationSnapshot{Navigation: rebuilt, Version: fresh.Version}); err != nil {
		t.Fatalf("healing save: %v", err)
	}
	healed, err := repositories.Navigation.Load(ctx, viewer)
	if err != nil || healed.Navigation == nil {
		t.Fatalf("healed=%+v err=%v", healed, err)
	}
	if healed.Navigation.WindowMode != NavigationWindowModeTopK {
		t.Fatalf("healed state not top-k: %+v", healed.Navigation)
	}
	if _, ok := healed.Navigation.Visited[95]; !ok {
		t.Fatalf("healed state lost visited mark: %+v", healed.Navigation.Visited)
	}
}

// TestOldBinaryLegacyWriteThenTopKLoadRebuildSave simulates an old binary
// writing only the legacy record (and only the legacy fence version), then a
// new top-k reader detecting the mismatch, rebuilding and healing.
func TestOldBinaryLegacyWriteThenTopKLoadRebuildSave(t *testing.T) {
	repositories, cleanup := newStandaloneNavigationRepositories(t)
	defer cleanup()
	ctx := context.Background()
	const viewer uint64 = 92

	// Old binary writes a complete legacy full-tail and a legacy-only visit.
	legacy := NewNavigation(viewer, 7, 60_000, 1_000)
	legacy.ScanPosition = 4
	legacy.CandidateWindow = []CandidateWindowEntry{{FarmID: 91, SourceFarmVersion: 4, ActionableAtMs: 900, ActionMask: ActionCanWater, ValidatedAtMs: 1_001}}
	legacy.pendingCandidates = []CandidateWindowEntry{{FarmID: 95, SourceFarmVersion: 5, ActionableAtMs: 800, ActionMask: ActionUnknown}}
	legacy.coarseScanDone = true
	legacy.TailComplete = true
	initial, err := repositories.Navigation.Load(ctx, viewer)
	if err != nil {
		t.Fatal(err)
	}
	if err := repositories.Navigation.Save(ctx, NavigationSnapshot{Navigation: legacy, Version: initial.Version}); err != nil {
		t.Fatalf("legacy write: %v", err)
	}
	if err := repositories.Navigation.MarkVisited(ctx, VisitFence{ViewerPlayerID: viewer, TargetFarmID: 96,
		ActiveSessionEpoch: 4, SubscriptionID: "sub-96"}); err != nil {
		t.Fatalf("legacy visit: %v", err)
	}

	// New binary enables top-k. The stale/missing top-k record must fail
	// closed to the legacy record (which preserves the visit).
	repositories.Navigation.ConfigureNavigationWindowMode(NavigationWindowModeTopK, DefaultTopKWindowSize)
	loaded, err := repositories.Navigation.Load(ctx, viewer)
	if err != nil || loaded.Navigation == nil {
		t.Fatalf("loaded=%+v err=%v", loaded, err)
	}
	if loaded.Navigation.WindowMode != NavigationWindowModeFullTail {
		t.Fatalf("top-k served without a fresh record: mode=%q", loaded.Navigation.WindowMode)
	}
	if _, ok := loaded.Navigation.Visited[96]; !ok {
		t.Fatalf("legacy-only visit lost: %+v", loaded.Navigation.Visited)
	}
	rebuilt := loaded.Navigation.ReconcileWindowMode(NavigationWindowModeTopK, DefaultTopKWindowSize)
	if err := repositories.Navigation.Save(ctx, NavigationSnapshot{Navigation: rebuilt, Version: loaded.Version}); err != nil {
		t.Fatalf("healing save: %v", err)
	}
	healed, err := repositories.Navigation.Load(ctx, viewer)
	if err != nil || healed.Navigation == nil {
		t.Fatalf("healed=%+v err=%v", healed, err)
	}
	if healed.Navigation.WindowMode != NavigationWindowModeTopK {
		t.Fatalf("healed state not top-k: %+v", healed.Navigation)
	}
	if _, ok := healed.Navigation.Visited[96]; !ok {
		t.Fatalf("healed state lost visit: %+v", healed.Navigation.Visited)
	}
}

// TestFullTailDefaultNeverTouchesTopKKey proves the normal full-tail path
// performs no top-k key read/write/delete/expire: a stale pre-existing
// garbage top-k key must not be decoded, deleted or refreshed.
func TestFullTailDefaultNeverTouchesTopKKey(t *testing.T) {
	repositories, cleanup := newStandaloneNavigationRepositories(t)
	defer cleanup()
	ctx := context.Background()
	const viewer uint64 = 93

	// A stale top-k key with garbage payload pre-exists (e.g. from an old
	// experiment). Full-tail loads must never decode it, saves must never
	// delete it, and visits must never touch it.
	topKKey := topKStateKeyFor(t, repositories, viewer)
	if err := repositories.Navigation.client.HSet(ctx, topKKey.String(), navigationTopKVersionField, 1,
		navigationTopKPayloadField, "{not-json").Err(); err != nil {
		t.Fatal(err)
	}

	navigation := NewNavigation(viewer, 7, 60_000, 1_000)
	navigation.CandidateWindow = []CandidateWindowEntry{{FarmID: 92, SourceFarmVersion: 4, ActionableAtMs: 900, ActionMask: ActionCanWater, ValidatedAtMs: 1_001}}
	navigation.coarseScanDone = true
	navigation.TailComplete = true
	initial, err := repositories.Navigation.Load(ctx, viewer)
	if err != nil || initial.Navigation != nil {
		t.Fatalf("initial=%+v err=%v", initial, err)
	}
	if err := repositories.Navigation.Save(ctx, NavigationSnapshot{Navigation: navigation, Version: initial.Version}); err != nil {
		t.Fatalf("full-tail save: %v", err)
	}
	if err := repositories.Navigation.MarkVisited(ctx, VisitFence{ViewerPlayerID: viewer, TargetFarmID: 94,
		ActiveSessionEpoch: 4, SubscriptionID: "sub-94"}); err != nil {
		t.Fatalf("full-tail visit: %v", err)
	}
	loaded, err := repositories.Navigation.Load(ctx, viewer)
	if err != nil || loaded.Navigation == nil {
		t.Fatalf("full-tail load with garbage top-k key failed: %+v err=%v", loaded, err)
	}
	if loaded.Navigation.WindowMode != NavigationWindowModeFullTail || !loaded.Navigation.TailComplete {
		t.Fatalf("full-tail state corrupted by stale top-k key: %+v", loaded.Navigation)
	}
	// The stale key must still exist untouched (it expires on its own TTL).
	exists, err := repositories.Navigation.client.Exists(ctx, topKKey.String()).Result()
	if err != nil || exists != 1 {
		t.Fatalf("full-tail path modified the stale top-k key: exists=%d err=%v", exists, err)
	}
	raw, err := repositories.Navigation.client.HGet(ctx, topKKey.String(), navigationTopKPayloadField).Result()
	if err != nil || raw != "{not-json" {
		t.Fatalf("full-tail path rewrote the stale top-k key: %q err=%v", raw, err)
	}
}

// TestMarkedFencesSurviveTopKSaveLoad proves in-memory markedFences are
// published to the separate Redis fence set by the atomic top-k save.
func TestMarkedFencesSurviveTopKSaveLoad(t *testing.T) {
	repositories, cleanup := newStandaloneNavigationRepositories(t)
	defer cleanup()
	ctx := context.Background()
	const viewer uint64 = 94

	repositories.Navigation.ConfigureNavigationWindowMode(NavigationWindowModeTopK, DefaultTopKWindowSize)
	navigation := NewNavigation(viewer, 7, 60_000, 1_000)
	navigation.CandidateWindow = []CandidateWindowEntry{{FarmID: 92, SourceFarmVersion: 4, ActionableAtMs: 900, ActionMask: ActionCanWater, ValidatedAtMs: 1_001}}
	navigation.coarseScanDone = true
	navigation.TailComplete = true
	if !navigation.MarkVisited(VisitFence{ViewerPlayerID: viewer, TargetFarmID: 93,
		ActiveSessionEpoch: 4, SubscriptionID: "sub-93"}) {
		t.Fatal("expected in-memory visited mark")
	}
	if len(navigation.markedFences) == 0 {
		t.Fatal("expected marked fences")
	}
	initial, err := repositories.Navigation.Load(ctx, viewer)
	if err != nil {
		t.Fatal(err)
	}
	if err := repositories.Navigation.Save(ctx, NavigationSnapshot{Navigation: navigation, Version: initial.Version}); err != nil {
		t.Fatalf("top-k save: %v", err)
	}
	loaded, err := repositories.Navigation.Load(ctx, viewer)
	if err != nil || loaded.Navigation == nil {
		t.Fatalf("loaded=%+v err=%v", loaded, err)
	}
	if len(loaded.Navigation.markedFences) != len(navigation.markedFences) {
		t.Fatalf("marked fences lost: got %d want %d: %+v", len(loaded.Navigation.markedFences), len(navigation.markedFences), loaded.Navigation.markedFences)
	}
	for fence := range navigation.markedFences {
		if _, ok := loaded.Navigation.markedFences[fence]; !ok {
			t.Fatalf("marked fence %q missing after top-k round trip", fence)
		}
	}
	fencesKey, err := repositories.Navigation.keys.FriendNavigationFencesKey(viewer)
	if err != nil {
		t.Fatal(err)
	}
	members, err := repositories.Navigation.client.SMembers(ctx, fencesKey.String()).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != len(navigation.markedFences) {
		t.Fatalf("redis fence set missing members: %v", members)
	}
}

// TestExactNegativesAreNotPersisted proves request-local exact negatives do
// not survive a round trip, so a negative farm that later becomes actionable
// (without any roster revision) is re-checked and returned. The save follows
// the service invariant that a NONE answer invalidates the scan before
// persisting.
func TestExactNegativesAreNotPersisted(t *testing.T) {
	repositories, cleanup := newStandaloneNavigationRepositories(t)
	defer cleanup()
	ctx := context.Background()
	const viewer uint64 = 95

	repositories.Navigation.ConfigureNavigationWindowMode(NavigationWindowModeTopK, DefaultTopKWindowSize)
	navigation := NewNavigation(viewer, 7, 60_000, 1_000)
	navigation.exactNegatives = map[uint64]struct{}{92: {}}
	navigation.coarseScanDone = true
	navigation.TailComplete = true
	// The service invalidates the scan before persisting a NONE answer so a
	// later maturity can be discovered without a roster revision.
	navigation.InvalidateCandidates()
	initial, err := repositories.Navigation.Load(ctx, viewer)
	if err != nil {
		t.Fatal(err)
	}
	if err := repositories.Navigation.Save(ctx, NavigationSnapshot{Navigation: navigation, Version: initial.Version}); err != nil {
		t.Fatalf("save: %v", err)
	}
	loaded, err := repositories.Navigation.Load(ctx, viewer)
	if err != nil || loaded.Navigation == nil {
		t.Fatalf("loaded=%+v err=%v", loaded, err)
	}
	if len(loaded.Navigation.exactNegatives) != 0 {
		t.Fatalf("exact negatives were persisted: %+v", loaded.Navigation.exactNegatives)
	}
	if loaded.Navigation.coarseScanDone || loaded.Navigation.TailComplete {
		t.Fatalf("persisted NONE state must force a fresh scan: %+v", loaded.Navigation)
	}
	// The negative farm is not skipped: it is coarse-scanned and exact-checked
	// again, and when it matures it is returned.
	roster := []uint64{92}
	rows := map[uint64]FarmOpportunityCandidate{92: {FarmID: 92, State: CandidateYes, ActionableAtMs: 900}}
	exact := map[uint64]ActionMask{92: ActionCanSteal}
	navigator, err := NewNavigator(&fakeCandidateReader{rows: rows}, &fakeEvaluator{exact: exact}, 4)
	if err != nil {
		t.Fatal(err)
	}
	next, err := navigator.Next(ctx, loaded.Navigation, roster, 7, 2_000)
	if err != nil {
		t.Fatal(err)
	}
	if !next.Found || next.FarmID != 92 {
		t.Fatalf("matured negative farm not returned: %+v", next)
	}
}

func TestOldReaderNeverAcceptsTruncatedTopKAsCompleteTail(t *testing.T) {
	repositories, cleanup := newStandaloneNavigationRepositories(t)
	defer cleanup()
	ctx := context.Background()
	const viewer uint64 = 96

	repositories.Navigation.ConfigureNavigationWindowMode(NavigationWindowModeTopK, 2)
	navigation := NewNavigation(viewer, 7, 60_000, 1_000)
	navigation.ScanPosition = 4
	navigation.pendingCandidates = []CandidateWindowEntry{
		{FarmID: 94, SourceFarmVersion: 4, ActionableAtMs: 900, ActionMask: ActionUnknown},
		{FarmID: 95, SourceFarmVersion: 5, ActionableAtMs: 800, ActionMask: ActionUnknown},
	}
	navigation.coarseScanDone = true
	navigation.TailComplete = false
	navigation.WindowMode = NavigationWindowModeTopK
	navigation.WindowSize = 2
	if !navigation.MarkVisited(VisitFence{ViewerPlayerID: viewer, TargetFarmID: 93,
		ActiveSessionEpoch: 4, SubscriptionID: "sub-93"}) {
		t.Fatal("expected in-memory visited mark")
	}
	initial, err := repositories.Navigation.Load(ctx, viewer)
	if err != nil || initial.Navigation != nil {
		t.Fatalf("initial snapshot=%+v err=%v", initial, err)
	}
	if err := repositories.Navigation.Save(ctx, NavigationSnapshot{Navigation: navigation, Version: initial.Version}); err != nil {
		t.Fatalf("save top-k navigation: %v", err)
	}
	loaded, err := repositories.Navigation.Load(ctx, viewer)
	if err != nil || loaded.Navigation == nil {
		t.Fatalf("loaded snapshot=%+v err=%v", loaded, err)
	}
	if loaded.Navigation.TailComplete || loaded.Navigation.WindowMode != NavigationWindowModeTopK ||
		loaded.Navigation.WindowSize != 2 {
		t.Fatalf("top-k state not preserved: %+v", loaded.Navigation)
	}

	stateKey, err := repositories.Navigation.keys.FriendNavigationStateKey(viewer)
	if err != nil {
		t.Fatal(err)
	}
	rawPayload, err := repositories.Navigation.client.HGet(ctx, stateKey.String(), navigationStatePayloadField).Result()
	if err != nil {
		t.Fatal(err)
	}
	var legacyWire navigationWire
	if err := json.Unmarshal([]byte(rawPayload), &legacyWire); err != nil {
		t.Fatalf("legacy payload decode: %v", err)
	}
	if legacyWire.TailComplete != nil {
		t.Fatalf("legacy-key payload must not carry TailComplete: %+v", legacyWire)
	}
	if legacyWire.CoarseScanDone || len(legacyWire.PendingCandidates) != 0 || len(legacyWire.CandidateWindow) != 0 {
		t.Fatalf("legacy payload must be an incomplete placeholder, got: %+v", legacyWire)
	}

	// Deleting the top-k record simulates an old reader that never wrote it;
	// the legacy placeholder remains. A new reader must rescan instead of
	// serving the truncated payload as complete.
	topKKey := topKStateKeyFor(t, repositories, viewer)
	if err := repositories.Navigation.client.Del(ctx, topKKey.String()).Err(); err != nil {
		t.Fatal(err)
	}
	afterOldRead, err := repositories.Navigation.Load(ctx, viewer)
	if err != nil {
		t.Fatal(err)
	}
	if afterOldRead.Navigation == nil || afterOldRead.Navigation.TailComplete {
		t.Fatalf("old-reader round trip served truncated tail as complete: %+v", afterOldRead.Navigation)
	}
	// The service layer re-hydrates the top-k mode after the storage boundary
	// decodes the legacy fallback as full-tail-incomplete.
	afterOldRead.Navigation.WindowMode = NavigationWindowModeTopK
	afterOldRead.Navigation.WindowSize = 2
	if afterOldRead.Navigation.ScanPosition != 0 || afterOldRead.Navigation.coarseScanDone {
		t.Fatalf("legacy placeholder must force a cold rescan: %+v", afterOldRead.Navigation)
	}
	roster := []uint64{94, 95, 96}
	rows := map[uint64]FarmOpportunityCandidate{
		94: {FarmID: 94, State: CandidateYes, ActionableAtMs: 900},
		95: {FarmID: 95, State: CandidateYes, ActionableAtMs: 800},
		96: {FarmID: 96, State: CandidateYes, ActionableAtMs: 700},
	}
	exact := map[uint64]ActionMask{94: ActionNone, 95: ActionNone, 96: ActionCanWater}
	navigator, err := NewNavigator(&fakeCandidateReader{rows: rows}, &fakeEvaluator{exact: exact}, 4)
	if err != nil {
		t.Fatal(err)
	}
	next, err := navigator.Next(ctx, afterOldRead.Navigation, roster, 7, 2_000)
	if err != nil {
		t.Fatal(err)
	}
	if !next.Found || next.FarmID != 96 {
		t.Fatalf("old-reader round trip next = %+v, want rescan FOUND 96", next)
	}
}

// TestOldReaderRoundTripNeverReturnsFalseNoneAfterRebuild covers an
// old-reader legacy placeholder that must be rebuilt; a roster too large to
// fit the top-k bound keeps the tail truncated and the exhausted answer stays
// UNKNOWN, never a false NONE.
func TestOldReaderRoundTripNeverReturnsFalseNoneAfterRebuild(t *testing.T) {
	repositories, cleanup := newStandaloneNavigationRepositories(t)
	defer cleanup()
	ctx := context.Background()
	const viewer uint64 = 97

	repositories.Navigation.ConfigureNavigationWindowMode(NavigationWindowModeTopK, 2)
	navigation := NewNavigation(viewer, 7, 60_000, 1_000)
	navigation.CandidateWindow = []CandidateWindowEntry{{
		FarmID: 92, SourceFarmVersion: 4, ActionableAtMs: 900, ActionMask: ActionUnknown,
	}}
	navigation.coarseScanDone = true
	navigation.TailComplete = false
	navigation.WindowMode = NavigationWindowModeTopK
	navigation.WindowSize = 2
	initial, err := repositories.Navigation.Load(ctx, viewer)
	if err != nil {
		t.Fatal(err)
	}
	if err := repositories.Navigation.Save(ctx, NavigationSnapshot{Navigation: navigation, Version: initial.Version}); err != nil {
		t.Fatalf("save top-k navigation: %v", err)
	}
	topKKey := topKStateKeyFor(t, repositories, viewer)
	if err := repositories.Navigation.client.Del(ctx, topKKey.String()).Err(); err != nil {
		t.Fatal(err)
	}
	afterOldRead, err := repositories.Navigation.Load(ctx, viewer)
	if err != nil {
		t.Fatal(err)
	}
	if afterOldRead.Navigation == nil || afterOldRead.Navigation.TailComplete {
		t.Fatalf("truncated legacy tail served as complete: %+v", afterOldRead.Navigation)
	}
	afterOldRead.Navigation.WindowMode = NavigationWindowModeTopK
	afterOldRead.Navigation.WindowSize = 2
	// 10 coarse candidates with a bound of 2: every rebuilt tail stays
	// truncated and the rescan budget ends while unchecked farms remain, so
	// the answer must be UNKNOWN, never a false NONE.
	roster := make([]uint64, 0, 10)
	rows := make(map[uint64]FarmOpportunityCandidate, 10)
	exact := make(map[uint64]ActionMask, 10)
	for id := uint64(1); id <= 10; id++ {
		roster = append(roster, id)
		rows[id] = FarmOpportunityCandidate{FarmID: id, State: CandidateYes, ActionableAtMs: int64(1000 - id)}
		exact[id] = ActionNone
	}
	navigator, err := NewNavigator(&fakeCandidateReader{rows: rows}, &fakeEvaluator{exact: exact}, 4)
	if err != nil {
		t.Fatal(err)
	}
	next, err := navigator.Next(ctx, afterOldRead.Navigation, roster, 7, 2_000)
	if err != nil {
		t.Fatal(err)
	}
	if next.Status == NavigationResultNone || next.Exhausted {
		t.Fatalf("exhausted truncated top-k returned false NONE: %+v", next)
	}
	if !next.Unknown || next.Status != NavigationResultUnknown {
		t.Fatalf("exhausted truncated top-k = %+v, want UNKNOWN", next)
	}
}

// TestImmediateRollbackFromTopKToFullTailPreservesVisitedState covers the
// rollback of a persisted top-k record to the full-tail default: the load
// fails closed to the legacy placeholder, visited marks survive, the answer
// is produced by a fresh full-tail scan, and the top-k key is left to expire
// (never deleted, read or re-created).
func TestImmediateRollbackFromTopKToFullTailPreservesVisitedState(t *testing.T) {
	repositories, cleanup := newStandaloneNavigationRepositories(t)
	defer cleanup()
	ctx := context.Background()
	const viewer uint64 = 99

	// Persist a top-k state with a visited mark.
	repositories.Navigation.ConfigureNavigationWindowMode(NavigationWindowModeTopK, DefaultTopKWindowSize)
	navigation := NewNavigation(viewer, 7, 60_000, 1_000)
	navigation.WindowMode = NavigationWindowModeTopK
	navigation.WindowSize = DefaultTopKWindowSize
	navigation.CandidateWindow = []CandidateWindowEntry{{FarmID: 91, SourceFarmVersion: 4, ActionableAtMs: 900, ActionMask: ActionCanWater, ValidatedAtMs: 1_001}}
	navigation.coarseScanDone = true
	navigation.TailComplete = true
	if !navigation.MarkVisited(VisitFence{ViewerPlayerID: viewer, TargetFarmID: 93,
		ActiveSessionEpoch: 4, SubscriptionID: "sub-93"}) {
		t.Fatal("expected in-memory visited mark")
	}
	initial, err := repositories.Navigation.Load(ctx, viewer)
	if err != nil {
		t.Fatal(err)
	}
	if err := repositories.Navigation.Save(ctx, NavigationSnapshot{Navigation: navigation, Version: initial.Version}); err != nil {
		t.Fatalf("top-k save: %v", err)
	}
	topKKey := topKStateKeyFor(t, repositories, viewer)
	if exists, err := repositories.Navigation.client.Exists(ctx, topKKey.String()).Result(); err != nil || exists != 1 {
		t.Fatalf("top-k key missing after top-k save: exists=%d err=%v", exists, err)
	}

	// Roll back to the full-tail default. The load must fail closed to the
	// legacy placeholder (never serve the top-k payload), preserving visited.
	repositories.Navigation.ConfigureNavigationWindowMode(NavigationWindowModeFullTail, 0)
	rolled, err := repositories.Navigation.Load(ctx, viewer)
	if err != nil || rolled.Navigation == nil {
		t.Fatalf("rolled=%+v err=%v", rolled, err)
	}
	if rolled.Navigation.WindowMode != NavigationWindowModeFullTail {
		t.Fatalf("rollback served top-k mode: %+v", rolled.Navigation)
	}
	if rolled.Navigation.TailComplete || rolled.Navigation.coarseScanDone {
		t.Fatalf("rollback must force a rebuild, not serve the top-k tail: %+v", rolled.Navigation)
	}
	if _, ok := rolled.Navigation.Visited[93]; !ok {
		t.Fatalf("visited mark lost on rollback: %+v", rolled.Navigation.Visited)
	}

	// A full-tail save after rollback answers from a fresh scan and never
	// deletes or rewrites the stale top-k key (it expires on its own TTL).
	rebuilt := rolled.Navigation.ReconcileWindowMode(NavigationWindowModeFullTail, 0)
	if err := repositories.Navigation.Save(ctx, NavigationSnapshot{Navigation: rebuilt, Version: rolled.Version}); err != nil {
		t.Fatalf("full-tail save after rollback: %v", err)
	}
	final, err := repositories.Navigation.Load(ctx, viewer)
	if err != nil || final.Navigation == nil {
		t.Fatalf("final=%+v err=%v", final, err)
	}
	if final.Navigation.WindowMode != NavigationWindowModeFullTail {
		t.Fatalf("final mode=%q, want full-tail", final.Navigation.WindowMode)
	}
	if _, ok := final.Navigation.Visited[93]; !ok {
		t.Fatalf("visited mark lost after rollback save: %+v", final.Navigation.Visited)
	}
	exists, err := repositories.Navigation.client.Exists(ctx, topKKey.String()).Result()
	if err != nil || exists != 1 {
		t.Fatalf("full-tail rollback deleted the stale top-k key: exists=%d err=%v", exists, err)
	}
}

// TestLegacyKeysContainNoTopKFields asserts the legacy payload written by a
// top-k save never carries experiment-only fields that could make an old
// reader mistake the placeholder for a complete full-tail.
func TestLegacyKeysContainNoTopKFields(t *testing.T) {
	repositories, cleanup := newStandaloneNavigationRepositories(t)
	defer cleanup()
	ctx := context.Background()
	const viewer uint64 = 98

	repositories.Navigation.ConfigureNavigationWindowMode(NavigationWindowModeTopK, 2)
	navigation := NewNavigation(viewer, 7, 60_000, 1_000)
	navigation.WindowMode = NavigationWindowModeTopK
	navigation.WindowSize = 2
	navigation.coarseScanDone = true
	navigation.TailComplete = true
	initial, err := repositories.Navigation.Load(ctx, viewer)
	if err != nil {
		t.Fatal(err)
	}
	if err := repositories.Navigation.Save(ctx, NavigationSnapshot{Navigation: navigation, Version: initial.Version}); err != nil {
		t.Fatalf("save: %v", err)
	}
	stateKey, err := repositories.Navigation.keys.FriendNavigationStateKey(viewer)
	if err != nil {
		t.Fatal(err)
	}
	rawPayload, err := repositories.Navigation.client.HGet(ctx, stateKey.String(), navigationStatePayloadField).Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"window_size", "tail_complete", "exact_negatives"} {
		if strings.Contains(rawPayload, forbidden) {
			t.Fatalf("legacy payload must not contain %q: %s", forbidden, rawPayload)
		}
	}
}
