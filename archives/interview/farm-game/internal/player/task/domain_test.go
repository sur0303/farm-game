package task_test

import (
	"errors"
	"testing"
	"time"

	"farmgame/internal/player/task"
)

const testTZ = "Asia/Shanghai"

// fakeRepo is the in-memory TaskConfigRepository double. Assignments and
// snapshots are immutable publication records; tests mutate the maps only
// to simulate new publications or outages.
type fakeRepo struct {
	assignments map[string]task.Assignment
	snapshots   map[uint64]*task.Config
	chain       *task.GrowthChain
	chainOK     bool
}

func (r *fakeRepo) AssignmentForPeriod(periodID string) (task.Assignment, bool) {
	assignment, ok := r.assignments[periodID]
	return assignment, ok
}

func (r *fakeRepo) DailySnapshot(version uint64) (*task.Config, bool) {
	cfg, ok := r.snapshots[version]
	return cfg, ok
}

func (r *fakeRepo) LatestGrowthChain() (*task.GrowthChain, bool) {
	if !r.chainOK {
		return nil, false
	}
	return r.chain, true
}

func dailyConfig(version uint64, hash string, loginCoin, visitCoin, interactCoin int64) *task.Config {
	return &task.Config{
		Version:     version,
		Timezone:    testTZ,
		PayloadHash: hash,
		Daily: [task.DailySlotCount]task.DailyDefinition{
			{Kind: task.DailyLogin, TaskID: "login", Reward: task.RewardBundle{Coin: loginCoin}},
			{Kind: task.DailyFriendVisit, TaskID: "visit", Reward: task.RewardBundle{Coin: visitCoin}},
			{Kind: task.DailyFriendInteract, TaskID: "interact", Reward: task.RewardBundle{Coin: interactCoin}},
		},
	}
}

func defaultChain() *task.GrowthChain {
	return &task.GrowthChain{
		Version: 3,
		Definitions: []task.GrowthDefinition{
			{TaskID: "harvest-3", DefinitionVersion: 1, Metric: task.GrowthMetricTotalHarvest, Target: 3, Reward: task.RewardBundle{Coin: 50}},
			{TaskID: "harvest-10", DefinitionVersion: 1, Metric: task.GrowthMetricTotalHarvest, Target: 10, Reward: task.RewardBundle{Coin: 100}},
			{TaskID: "level-5", DefinitionVersion: 1, Metric: task.GrowthMetricHighestLevel, Target: 5, Reward: task.RewardBundle{Coin: 200}},
		},
	}
}

func newRepo() *fakeRepo {
	v7 := dailyConfig(7, "hash-v7", 10, 20, 30)
	v8 := dailyConfig(8, "hash-v8", 15, 25, 35)
	return &fakeRepo{
		assignments: map[string]task.Assignment{
			"20260803": {PeriodID: "20260803", ConfigVersion: 7, EffectiveAtMs: 0, ConfigPayloadHash: "hash-v7"},
			"20260804": {PeriodID: "20260804", ConfigVersion: 8, EffectiveAtMs: 0, ConfigPayloadHash: "hash-v8"},
			"20260807": {PeriodID: "20260807", ConfigVersion: 7, EffectiveAtMs: 0, ConfigPayloadHash: "hash-v7"},
		},
		snapshots: map[uint64]*task.Config{7: v7, 8: v8},
		chain:     defaultChain(),
		chainOK:   true,
	}
}

func newDomain(t *testing.T, repo task.Repository) *task.Domain {
	t.Helper()
	domain, err := task.NewDomain(repo, testTZ)
	if err != nil {
		t.Fatalf("NewDomain: %v", err)
	}
	return domain
}

// shMs builds a millisecond timestamp in the Asia/Shanghai civil clock.
func shMs(t *testing.T, y int, m time.Month, d, hh, mm, ss int) int64 {
	t.Helper()
	loc := mustLocation(t, testTZ)
	return time.Date(y, m, d, hh, mm, ss, 0, loc).UnixMilli()
}

var (
	day1Noon  = int64(0)
	day1Late  = int64(0)
	day2Early = int64(0)
	day2Noon  = int64(0)
	day5Noon  = int64(0)
)

func initTimes(t *testing.T) {
	t.Helper()
	if day1Noon == 0 {
		day1Noon = shMs(t, 2026, 8, 3, 12, 0, 0)
		day1Late = shMs(t, 2026, 8, 3, 23, 0, 0)
		day2Early = shMs(t, 2026, 8, 4, 0, 30, 0)
		day2Noon = shMs(t, 2026, 8, 4, 12, 0, 0)
		day5Noon = shMs(t, 2026, 8, 7, 12, 0, 0)
	}
}

func dailyID(period, id string) string { return task.DailyInstanceID(period, id) }

type mustHelper struct{ t *testing.T }

func must(t *testing.T) mustHelper {
	t.Helper()
	return mustHelper{t: t}
}

func (m mustHelper) fact(result task.FactResult, err error) task.FactResult {
	m.t.Helper()
	if err != nil {
		m.t.Fatalf("unexpected fact error: %v", err)
	}
	return result
}

func (m mustHelper) claim(outcome task.ClaimOutcome, err error) task.ClaimOutcome {
	m.t.Helper()
	if err != nil {
		m.t.Fatalf("unexpected claim error: %v", err)
	}
	return outcome
}

// actualClaimable independently counts CLAIMABLE instances in the state.
func actualClaimable(st *task.DomainState) int32 {
	var count int32
	for i := range st.DailyTasks {
		if st.DailyTasks[i].State == task.StateClaimable {
			count++
		}
	}
	if st.CurrentGrowth != nil && st.CurrentGrowth.State == task.StateClaimable {
		count++
	}
	return count
}

// TestDailyFactFastNoChange covers repeated login/visit/interact facts:
// the first witness satisfies the task, every later same-kind fact of the
// period fast-paths NO_CHANGE without version churn (Q34-05).
func TestDailyFactFastNoChange(t *testing.T) {
	initTimes(t)
	domain := newDomain(t, newRepo())
	st := &task.DomainState{}

	steps := []struct {
		name          string
		act           func() (task.FactResult, error)
		wantStatus    task.FactStatus
		wantClaimable int32
		wantVersion   uint64
	}{
		{"first login", func() (task.FactResult, error) { return domain.RecordLogin(st, day1Noon) }, task.FactApplied, 1, 1},
		{"repeat login", func() (task.FactResult, error) { return domain.RecordLogin(st, day1Noon) }, task.FactNoChange, 1, 1},
		{"third login same period", func() (task.FactResult, error) { return domain.RecordLogin(st, day1Late) }, task.FactNoChange, 1, 1},
		{"first visit", func() (task.FactResult, error) { return domain.RecordFriendVisit(st, "visit-op-1", day1Noon) }, task.FactApplied, 2, 2},
		{"repeat visit another friend", func() (task.FactResult, error) { return domain.RecordFriendVisit(st, "visit-op-2", day1Noon) }, task.FactNoChange, 2, 2},
		{"first interact (care)", func() (task.FactResult, error) {
			return domain.RecordFriendInteraction(st, "care-order-1", day1Noon, day1Noon)
		}, task.FactApplied, 3, 3},
		{"repeat interact (theft)", func() (task.FactResult, error) {
			return domain.RecordFriendInteraction(st, "theft-settle-1", day1Noon, day1Noon)
		}, task.FactNoChange, 3, 3},
	}
	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			result := must(t).fact(step.act())
			if result.Status != step.wantStatus {
				t.Fatalf("status = %s, want %s", result.Status, step.wantStatus)
			}
			if st.TaskStateVersion != step.wantVersion {
				t.Fatalf("task_state_version = %d, want %d", st.TaskStateVersion, step.wantVersion)
			}
			if st.ClaimableCount != step.wantClaimable || st.ClaimableCount != actualClaimable(st) {
				t.Fatalf("claimable_count = %d (actual %d), want %d", st.ClaimableCount, actualClaimable(st), step.wantClaimable)
			}
		})
	}
}

// TestInteractionImpliesVisit covers Q34-03: a care/theft participant
// already proves a legal visit, so the same batch satisfies both the
// visit and interact tasks with a single version bump.
func TestInteractionImpliesVisit(t *testing.T) {
	initTimes(t)
	domain := newDomain(t, newRepo())
	st := &task.DomainState{}

	result := must(t).fact(domain.RecordFriendInteraction(st, "theft-fully-intercepted-by-pet", day1Noon, day1Noon))
	if result.Status != task.FactApplied {
		t.Fatalf("status = %s, want APPLIED", result.Status)
	}
	visit := st.DailyTasks[1]
	interact := st.DailyTasks[2]
	if visit.State != task.StateClaimable || interact.State != task.StateClaimable {
		t.Fatalf("visit=%s interact=%s, both want CLAIMABLE", visit.State, interact.State)
	}
	if st.TaskStateVersion != 1 {
		t.Fatalf("task_state_version = %d, want a single bump for the batch", st.TaskStateVersion)
	}
	if st.ClaimableCount != 2 {
		t.Fatalf("claimable_count = %d, want 2", st.ClaimableCount)
	}
	// The visit task is already satisfied; an explicit CONFIRM_FRIEND_VISIT
	// afterwards is a persistent NO_CHANGE, not a conflict.
	result = must(t).fact(domain.RecordFriendVisit(st, "visit-op-late", day1Late))
	if result.Status != task.FactNoChange || result.StateChanged {
		t.Fatalf("late visit = %s changed=%v, want NO_CHANGE without state change", result.Status, result.StateChanged)
	}
}

// TestLazyPeriodSwitch covers the strict-midnight lazy switch, discarding
// yesterday's unclaimed tasks and never backfilling skipped days (Q34-02).
func TestLazyPeriodSwitch(t *testing.T) {
	initTimes(t)
	domain := newDomain(t, newRepo())
	st := &task.DomainState{}

	must(t).fact(domain.RecordLogin(st, day1Noon))
	must(t).fact(domain.RecordFriendVisit(st, "visit-1", day1Noon))
	// Login is claimed, visit stays unclaimed across the boundary.
	claimed := must(t).claim(domain.Claim(st, dailyID("20260803", "login"), day1Noon))
	if claimed.Status != task.ClaimSucceeded {
		t.Fatalf("claim login = %s, want SUCCEEDED", claimed.Status)
	}
	versionBefore := st.TaskStateVersion

	switched, err := domain.EnsureDailyPeriod(st, day2Early)
	if err != nil {
		t.Fatalf("EnsureDailyPeriod: %v", err)
	}
	if !switched {
		t.Fatal("expected a lazy period switch after midnight")
	}
	if st.DailyPeriodID != "20260804" {
		t.Fatalf("daily_period_id = %s, want 20260804", st.DailyPeriodID)
	}
	for i := range st.DailyTasks {
		if st.DailyTasks[i].State != task.StateInProgress {
			t.Fatalf("daily task %d state = %s after switch, want IN_PROGRESS (unclaimed discarded)", i, st.DailyTasks[i].State)
		}
		if st.DailyTasks[i].PeriodID != "20260804" {
			t.Fatalf("daily task %d period = %s, want 20260804", i, st.DailyTasks[i].PeriodID)
		}
	}
	if st.TaskStateVersion != versionBefore+1 {
		t.Fatalf("switch bumped version to %d, want %d", st.TaskStateVersion, versionBefore+1)
	}
	if st.ClaimableCount != 0 {
		t.Fatalf("claimable_count = %d after switch, want 0", st.ClaimableCount)
	}

	// A daily instance of the previous period can never be claimed again.
	outcome := must(t).claim(domain.Claim(st, dailyID("20260803", "visit"), day2Early))
	if outcome.Status != task.ClaimInstanceExpired {
		t.Fatalf("claim old visit = %s, want TASK_INSTANCE_EXPIRED", outcome.Status)
	}
	// Even the instance already claimed yesterday expires for new operations.
	outcome = must(t).claim(domain.Claim(st, dailyID("20260803", "login"), day2Early))
	if outcome.Status != task.ClaimInstanceExpired {
		t.Fatalf("claim old claimed login = %s, want TASK_INSTANCE_EXPIRED", outcome.Status)
	}
	// Same-operation replay is answered by PlayerActor's operation receipt
	// before this pure domain is called. At the domain boundary the old
	// instance remains expired for every later evaluation.
}

// TestLazySwitchSkipsIntermediateDays proves a player absent for several
// days lands directly in the current period with no per-day backfill.
func TestLazySwitchSkipsIntermediateDays(t *testing.T) {
	initTimes(t)
	domain := newDomain(t, newRepo())
	st := &task.DomainState{}

	must(t).fact(domain.RecordLogin(st, day1Noon))
	switched, err := domain.EnsureDailyPeriod(st, day5Noon)
	if err != nil {
		t.Fatalf("EnsureDailyPeriod: %v", err)
	}
	if !switched || st.DailyPeriodID != "20260807" {
		t.Fatalf("switched=%v period=%s, want direct switch to 20260807", switched, st.DailyPeriodID)
	}
	for i := range st.DailyTasks {
		if st.DailyTasks[i].PeriodID != "20260807" {
			t.Fatalf("task %d period = %s, want 20260807 (no intermediate days)", i, st.DailyTasks[i].PeriodID)
		}
	}
}

// TestLateCrossMidnightFact covers delayed participant delivery: the fact
// belongs to its real occurrence period; once that period expired the
// fact is idempotently acknowledged as IGNORED_EXPIRED and never pollutes
// the new day (Q34-02/Q34-05).
func TestLateCrossMidnightFact(t *testing.T) {
	initTimes(t)
	domain := newDomain(t, newRepo())
	st := &task.DomainState{}

	// Care resolved yesterday 23:00 but delivered today 00:30.
	result := must(t).fact(domain.RecordFriendInteraction(st, "care-late", day1Late, day2Early))
	if result.Status != task.FactIgnoredExpired {
		t.Fatalf("late care = %s, want IGNORED_EXPIRED", result.Status)
	}
	if !result.StateChanged {
		t.Fatal("the lazy switch itself must still be committed")
	}
	if st.DailyTasks[2].State != task.StateInProgress {
		t.Fatalf("interact = %s, want IN_PROGRESS (expired fact must not pollute the new day)", st.DailyTasks[2].State)
	}
	versionAfter := st.TaskStateVersion
	// Replaying the same delivery is acknowledged again without churn.
	result = must(t).fact(domain.RecordFriendInteraction(st, "care-late", day1Late, day2Early))
	if result.Status != task.FactIgnoredExpired || result.StateChanged {
		t.Fatalf("replay = %s changed=%v, want idempotent IGNORED_EXPIRED", result.Status, result.StateChanged)
	}
	if st.TaskStateVersion != versionAfter {
		t.Fatal("expired replay bumped the version")
	}
	// A same-day interaction still applies normally.
	result = must(t).fact(domain.RecordFriendInteraction(st, "care-today", day2Early, day2Early))
	if result.Status != task.FactApplied {
		t.Fatalf("today care = %s, want APPLIED", result.Status)
	}
}

// TestFactFromFutureRejected pins the fail-closed handling of a fact whose
// occurrence period is ahead of the current period (impossible under a
// trusted clock; indicates a source bug).
func TestFactFromFutureRejected(t *testing.T) {
	initTimes(t)
	domain := newDomain(t, newRepo())
	st := &task.DomainState{}

	_, err := domain.RecordFriendInteraction(st, "care-future", day5Noon, day2Noon)
	if !errors.Is(err, task.ErrFactFromFuture) {
		t.Fatalf("err = %v, want ErrFactFromFuture", err)
	}
}

// TestHarvestCumulativeProgress covers batch harvest counting (Q34-03):
// a batch of five ended CropCycles increments five. Participant replay is
// filtered by PlayerActor's unified Participant Receipt before this pure
// domain method is invoked, so this test only feeds newly admitted facts.
func TestHarvestCumulativeProgress(t *testing.T) {
	initTimes(t)
	domain := newDomain(t, newRepo())
	st := &task.DomainState{}

	steps := []struct {
		name         string
		count        int64
		wantTotal    int64
		wantVersion  uint64
		wantProgress int64 // progress on the current growth task
	}{
		{"three cycles", 3, 3, 1, 3},
		{"batch of five", 5, 8, 2, 3}, // CLAIMABLE task does not churn
		{"one more cycle", 1, 9, 3, 3},
	}
	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			result := must(t).fact(domain.RecordHarvest(st, step.count, day1Noon))
			if result.Status != task.FactApplied {
				t.Fatalf("status = %s, want APPLIED", result.Status)
			}
			if st.TotalHarvestCount != step.wantTotal {
				t.Fatalf("total_harvest_count = %d, want %d", st.TotalHarvestCount, step.wantTotal)
			}
			if st.TaskStateVersion != step.wantVersion {
				t.Fatalf("version = %d, want %d", st.TaskStateVersion, step.wantVersion)
			}
			if st.CurrentGrowth == nil || st.CurrentGrowth.Progress != step.wantProgress {
				t.Fatalf("growth progress = %+v, want progress %d", st.CurrentGrowth, step.wantProgress)
			}
		})
	}
	if st.CurrentGrowth.State != task.StateClaimable {
		t.Fatalf("growth = %s, want CLAIMABLE after 3 harvests", st.CurrentGrowth.State)
	}
}

// TestLevelStateType covers the level indicator: max(progress,
// level_after) so out-of-order delivery never regresses (Q34-05).
func TestLevelStateType(t *testing.T) {
	initTimes(t)
	domain := newDomain(t, newRepo())
	st := &task.DomainState{}

	steps := []struct {
		name        string
		level       int32
		wantStatus  task.FactStatus
		wantHighest int32
		wantVersion uint64
	}{
		{"level up to 4", 4, task.FactApplied, 4, 1},
		{"out of order 3", 3, task.FactNoChange, 4, 1},
		{"repeat 4", 4, task.FactNoChange, 4, 1},
		{"level up to 6", 6, task.FactApplied, 6, 2},
	}
	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			result := must(t).fact(domain.RecordLevel(st, step.level, day1Noon))
			if result.Status != step.wantStatus {
				t.Fatalf("status = %s, want %s", result.Status, step.wantStatus)
			}
			if st.HighestLevel != step.wantHighest {
				t.Fatalf("highest_level = %d, want %d", st.HighestLevel, step.wantHighest)
			}
			if st.TaskStateVersion != step.wantVersion {
				t.Fatalf("version = %d, want %d", st.TaskStateVersion, step.wantVersion)
			}
		})
	}
}

// TestGrowthChainContinuous covers Q34-08: claim advances the linear
// cursor, the next task activates immediately and becomes CLAIMABLE when
// the permanent indicators already satisfy it, but is never auto-claimed.
func TestGrowthChainContinuous(t *testing.T) {
	initTimes(t)
	domain := newDomain(t, newRepo())
	st := &task.DomainState{}

	must(t).fact(domain.RecordHarvest(st, 12, day1Noon))
	if st.CurrentGrowth == nil || st.CurrentGrowth.TaskID != "harvest-3" || st.CurrentGrowth.State != task.StateClaimable {
		t.Fatalf("current growth = %+v, want claimable harvest-3", st.CurrentGrowth)
	}

	first := must(t).claim(domain.Claim(st, task.GrowthInstanceID("harvest-3", 1), day1Noon))
	if first.Status != task.ClaimSucceeded || first.Reward.Coin != 50 {
		t.Fatalf("first claim = %s coin %d, want SUCCEEDED 50", first.Status, first.Reward.Coin)
	}
	if first.NextGrowth == nil {
		t.Fatal("expected next_growth_task in the claim outcome")
	}
	if first.NextGrowth.TaskID != "harvest-10" || first.NextGrowth.State != task.StateClaimable {
		t.Fatalf("next growth = %+v, want immediately claimable harvest-10", first.NextGrowth)
	}
	// Not auto-claimed: the new task waits for a new operation.
	if st.CurrentGrowth == nil || st.CurrentGrowth.State != task.StateClaimable || st.CurrentGrowth.ClaimedAtMs != 0 {
		t.Fatalf("current growth = %+v, want claimable and unclaimed harvest-10", st.CurrentGrowth)
	}

	second := must(t).claim(domain.Claim(st, task.GrowthInstanceID("harvest-10", 1), day1Noon))
	if second.Status != task.ClaimSucceeded || second.Reward.Coin != 100 {
		t.Fatalf("second claim = %s coin %d, want SUCCEEDED 100", second.Status, second.Reward.Coin)
	}
	if second.NextGrowth == nil || second.NextGrowth.TaskID != "level-5" || second.NextGrowth.State != task.StateInProgress {
		t.Fatalf("next growth = %+v, want in-progress level-5", second.NextGrowth)
	}

	must(t).fact(domain.RecordLevel(st, 5, day1Noon))
	if st.CurrentGrowth.State != task.StateClaimable {
		t.Fatalf("level-5 = %s, want CLAIMABLE after reaching level 5", st.CurrentGrowth.State)
	}

	third := must(t).claim(domain.Claim(st, task.GrowthInstanceID("level-5", 1), day1Noon))
	if third.Status != task.ClaimSucceeded || third.Reward.Coin != 200 {
		t.Fatalf("third claim = %s coin %d, want SUCCEEDED 200", third.Status, third.Reward.Coin)
	}
	if third.NextGrowth != nil || st.CurrentGrowth != nil {
		t.Fatalf("chain should have ended: next=%+v current=%+v", third.NextGrowth, st.CurrentGrowth)
	}

	// A different operation re-claiming an advanced-past growth instance
	// gets a persistent ALREADY_CLAIMED.
	again := must(t).claim(domain.Claim(st, task.GrowthInstanceID("harvest-3", 1), day1Noon))
	if again.Status != task.ClaimAlreadyClaimed || again.Reward.Coin != 0 {
		t.Fatalf("re-claim = %s coin %d, want TASK_ALREADY_CLAIMED without reward", again.Status, again.Reward.Coin)
	}

	view, err := domain.ListView(st, day1Noon)
	if err != nil {
		t.Fatalf("ListView: %v", err)
	}
	if view.CurrentGrowth != nil {
		t.Fatalf("growth task still shown after chain end: %+v", view.CurrentGrowth)
	}
}

// TestGrowthChainExtension covers lazy activation of a newly published
// chain tail: players who finished the old chain pick up the new task on
// their next turn, immediately claimable if already satisfied (Q34-07).
func TestGrowthChainExtension(t *testing.T) {
	initTimes(t)
	repo := newRepo()
	domain := newDomain(t, repo)
	st := &task.DomainState{}

	must(t).fact(domain.RecordHarvest(st, 30, day1Noon))
	for _, id := range []string{"harvest-3", "harvest-10"} {
		must(t).claim(domain.Claim(st, task.GrowthInstanceID(id, 1), day1Noon))
	}
	must(t).fact(domain.RecordLevel(st, 5, day1Noon))
	must(t).claim(domain.Claim(st, task.GrowthInstanceID("level-5", 1), day1Noon))
	if st.CurrentGrowth != nil {
		t.Fatal("chain should have ended")
	}

	// Publish an extended chain: a new tail task appears lazily.
	repo.chain = &task.GrowthChain{
		Version: 4,
		Definitions: append(append([]task.GrowthDefinition(nil), defaultChain().Definitions...),
			task.GrowthDefinition{TaskID: "harvest-25", DefinitionVersion: 1, Metric: task.GrowthMetricTotalHarvest, Target: 25, Reward: task.RewardBundle{Coin: 500}}),
	}
	if _, err := domain.EnsureDailyPeriod(st, day1Late); err != nil {
		t.Fatalf("EnsureDailyPeriod: %v", err)
	}
	if st.CurrentGrowth == nil || st.CurrentGrowth.TaskID != "harvest-25" {
		t.Fatalf("current growth = %+v, want lazily activated harvest-25", st.CurrentGrowth)
	}
	if st.CurrentGrowth.State != task.StateClaimable {
		t.Fatalf("harvest-25 = %s, want immediately CLAIMABLE (30 >= 25) but not auto-claimed", st.CurrentGrowth.State)
	}
}

// TestClaimTerminalMatrix is the table-driven claim terminal mapping of
// Q34-06.
func TestClaimTerminalMatrix(t *testing.T) {
	initTimes(t)

	type setup struct {
		login, visit, interact bool
	}
	cases := []struct {
		name       string
		setup      setup
		instanceID string
		preClaim   bool // claim the target instance once first
		wantStatus task.ClaimStatus
		wantCoin   int64
		wantChange bool
	}{
		{"success login", setup{login: true}, dailyID("20260803", "login"), false, task.ClaimSucceeded, 10, true},
		{"success visit", setup{visit: true}, dailyID("20260803", "visit"), false, task.ClaimSucceeded, 20, true},
		{"success interact", setup{interact: true}, dailyID("20260803", "interact"), false, task.ClaimSucceeded, 30, true},
		{"not completed", setup{}, dailyID("20260803", "login"), false, task.ClaimNotCompleted, 0, false},
		{"already claimed new operation", setup{login: true}, dailyID("20260803", "login"), true, task.ClaimAlreadyClaimed, 0, false},
		{"unknown growth instance", setup{}, "growth/ghost/1", false, task.ClaimInstanceNotFound, 0, false},
		{"daily instance of another period", setup{login: true}, dailyID("20260801", "login"), false, task.ClaimInstanceExpired, 0, false},
		{"garbage instance id", setup{}, "nonsense", false, task.ClaimInstanceNotFound, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			domain := newDomain(t, newRepo())
			st := &task.DomainState{}
			if _, err := domain.EnsureDailyPeriod(st, day1Noon); err != nil {
				t.Fatalf("EnsureDailyPeriod: %v", err)
			}
			if tc.setup.login {
				must(t).fact(domain.RecordLogin(st, day1Noon))
			}
			if tc.setup.visit {
				must(t).fact(domain.RecordFriendVisit(st, "v", day1Noon))
			}
			if tc.setup.interact {
				must(t).fact(domain.RecordFriendInteraction(st, "c", day1Noon, day1Noon))
			}
			if tc.preClaim {
				first := must(t).claim(domain.Claim(st, tc.instanceID, day1Noon))
				if first.Status != task.ClaimSucceeded {
					t.Fatalf("pre-claim = %s, want SUCCEEDED", first.Status)
				}
			}
			versionBefore := st.TaskStateVersion
			outcome := must(t).claim(domain.Claim(st, tc.instanceID, day1Noon))
			if outcome.Status != tc.wantStatus {
				t.Fatalf("status = %s, want %s", outcome.Status, tc.wantStatus)
			}
			if outcome.Reward.Coin != tc.wantCoin {
				t.Fatalf("reward coin = %d, want %d", outcome.Reward.Coin, tc.wantCoin)
			}
			if outcome.StateChanged != tc.wantChange {
				t.Fatalf("state_changed = %v, want %v", outcome.StateChanged, tc.wantChange)
			}
			wantVersion := versionBefore
			if tc.wantChange {
				wantVersion++
			}
			if st.TaskStateVersion != wantVersion {
				t.Fatalf("version = %d, want %d", st.TaskStateVersion, wantVersion)
			}
		})
	}
}

// TestRejectedClaimCanBeReevaluatedAfterTruthChanges pins the boundary
// between TaskDomain and PlayerActor receipts. The pure domain has no
// operation_id: after task truth changes it evaluates the instance again
// and can succeed. PlayerActor's REQUIRED StoredOperationOutcome, tested
// at the authority layer, preserves the old operation's rejection.
func TestRejectedClaimCanBeReevaluatedAfterTruthChanges(t *testing.T) {
	initTimes(t)
	domain := newDomain(t, newRepo())
	st := &task.DomainState{}

	must(t).fact(domain.RecordLogin(st, day1Noon))
	// Visit is IN_PROGRESS, so the current domain evaluation is rejected.
	outcome := must(t).claim(domain.Claim(st, dailyID("20260803", "visit"), day1Noon))
	if outcome.Status != task.ClaimNotCompleted {
		t.Fatalf("status = %s, want TASK_NOT_COMPLETED", outcome.Status)
	}
	// The task becomes claimable later.
	must(t).fact(domain.RecordFriendVisit(st, "v", day1Late))
	// A later admitted operation reaches the domain and claims successfully.
	outcome = must(t).claim(domain.Claim(st, dailyID("20260803", "visit"), day1Late))
	if outcome.Status != task.ClaimSucceeded {
		t.Fatalf("second evaluation = %s, want SUCCEEDED", outcome.Status)
	}
}

// TestConfigVersionBinding covers Q34-07: every player of one period uses
// the assignment version (never a local "latest"), instances bind their
// creation version, and rewards are read frozen from the instance.
func TestConfigVersionBinding(t *testing.T) {
	initTimes(t)
	repo := newRepo()
	domain := newDomain(t, repo)

	early := &task.DomainState{}
	late := &task.DomainState{}
	must(t).fact(domain.RecordLogin(early, shMs(t, 2026, 8, 3, 0, 30, 0)))
	must(t).fact(domain.RecordLogin(late, day1Late))
	if early.DailyConfigVersion != 7 || late.DailyConfigVersion != 7 {
		t.Fatalf("daily_config_version early=%d late=%d, both want 7 (assignment, not latest)", early.DailyConfigVersion, late.DailyConfigVersion)
	}

	// A same-day publication of v8 without an assignment change must not
	// drift the period. (Day 2 has its own v8 assignment.)
	outcome := must(t).claim(domain.Claim(early, dailyID("20260803", "login"), day1Late))
	if outcome.Reward.Coin != 10 {
		t.Fatalf("reward = %d, want frozen v7 coin 10", outcome.Reward.Coin)
	}

	// Day 2 binds v8 with its own reward table.
	if _, err := domain.EnsureDailyPeriod(early, day2Noon); err != nil {
		t.Fatalf("EnsureDailyPeriod: %v", err)
	}
	if early.DailyConfigVersion != 8 {
		t.Fatalf("day2 config version = %d, want 8", early.DailyConfigVersion)
	}
	must(t).fact(domain.RecordLogin(early, day2Noon))
	outcome = must(t).claim(domain.Claim(early, dailyID("20260804", "login"), day2Noon))
	if outcome.Reward.Coin != 15 {
		t.Fatalf("day2 reward = %d, want v8 coin 15", outcome.Reward.Coin)
	}
}

// TestGrowthRewardFrozenAtActivation proves an activated growth instance
// keeps its bound definition_version and reward even when a newer chain
// republishes the task (Q34-07).
func TestGrowthRewardFrozenAtActivation(t *testing.T) {
	initTimes(t)
	repo := newRepo()
	domain := newDomain(t, repo)
	st := &task.DomainState{}

	must(t).fact(domain.RecordHarvest(st, 3, day1Noon))
	if st.CurrentGrowth == nil || st.CurrentGrowth.DefinitionVersion != 1 {
		t.Fatalf("current growth = %+v, want definition_version 1", st.CurrentGrowth)
	}

	// Republish the chain: harvest-3 now has definition_version 2 with a
	// different target/reward; the tail stays compatible with the cursor.
	repo.chain = &task.GrowthChain{
		Version: 4,
		Definitions: []task.GrowthDefinition{
			{TaskID: "harvest-3", DefinitionVersion: 2, Metric: task.GrowthMetricTotalHarvest, Target: 99, Reward: task.RewardBundle{Coin: 999}},
			{TaskID: "harvest-10", DefinitionVersion: 1, Metric: task.GrowthMetricTotalHarvest, Target: 10, Reward: task.RewardBundle{Coin: 100}},
			{TaskID: "level-5", DefinitionVersion: 1, Metric: task.GrowthMetricHighestLevel, Target: 5, Reward: task.RewardBundle{Coin: 200}},
		},
	}
	outcome := must(t).claim(domain.Claim(st, task.GrowthInstanceID("harvest-3", 1), day1Noon))
	if outcome.Status != task.ClaimSucceeded || outcome.Reward.Coin != 50 {
		t.Fatalf("claim = %s coin %d, want frozen definition v1 reward 50", outcome.Status, outcome.Reward.Coin)
	}
	if outcome.NextGrowth == nil || outcome.NextGrowth.ConfigVersion != 4 {
		t.Fatalf("next growth = %+v, want activation from chain v4", outcome.NextGrowth)
	}
}

// TestFailClosedOnConfigOutage is the table-driven degradation matrix
// (Q34-07/Q34-08): unknown period, missing snapshot, hash drift, timezone
// drift and missing growth chain all fail closed without touching state.
func TestFailClosedOnConfigOutage(t *testing.T) {
	initTimes(t)

	cases := []struct {
		name    string
		mutate  func(repo *fakeRepo)
		wantErr error
	}{
		{"unknown period", func(repo *fakeRepo) { delete(repo.assignments, "20260803") }, task.ErrAssignmentUnavailable},
		{"missing snapshot", func(repo *fakeRepo) { delete(repo.snapshots, uint64(7)) }, task.ErrSnapshotUnavailable},
		{"hash mismatch", func(repo *fakeRepo) {
			assignment := repo.assignments["20260803"]
			assignment.ConfigPayloadHash = "tampered"
			repo.assignments["20260803"] = assignment
		}, task.ErrConfigHashMismatch},
		{"timezone drift", func(repo *fakeRepo) {
			cfg := *repo.snapshots[7]
			cfg.Timezone = "UTC"
			repo.snapshots[7] = &cfg
		}, nil}, // validation error, not a sentinel
		{"growth chain unavailable", func(repo *fakeRepo) { repo.chainOK = false }, task.ErrGrowthChainUnavailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repo := newRepo()
			tc.mutate(repo)
			domain := newDomain(t, repo)
			st := &task.DomainState{}
			_, err := domain.EnsureDailyPeriod(st, day1Noon)
			if err == nil {
				t.Fatal("expected fail-closed error")
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			// The outage must not half-initialize the daily state.
			if st.DailyPeriodID != "" {
				t.Fatalf("daily state mutated despite outage: %+v", st.DailyPeriodID)
			}
		})
	}
}

// TestVersionAndClaimableAccounting pins the monotonic single-bump rule
// and the claimable_count invariant across a mixed day (Q34-08).
func TestVersionAndClaimableAccounting(t *testing.T) {
	initTimes(t)
	domain := newDomain(t, newRepo())
	st := &task.DomainState{}

	steps := []struct {
		name          string
		act           func()
		wantVersion   uint64
		wantClaimable int32
	}{
		{"login", func() { must(t).fact(domain.RecordLogin(st, day1Noon)) }, 1, 1},
		{"login repeat", func() { must(t).fact(domain.RecordLogin(st, day1Noon)) }, 1, 1},
		{"visit", func() { must(t).fact(domain.RecordFriendVisit(st, "v", day1Noon)) }, 2, 2},
		{"interact", func() { must(t).fact(domain.RecordFriendInteraction(st, "c", day1Noon, day1Noon)) }, 3, 3},
		{"claim login", func() { must(t).claim(domain.Claim(st, dailyID("20260803", "login"), day1Noon)) }, 4, 2},
		{"reclaim login", func() { must(t).claim(domain.Claim(st, dailyID("20260803", "login"), day1Noon)) }, 4, 2},
		{"rejected claim keeps version", func() {
			outcome := must(t).claim(domain.Claim(st, "growth/ghost/1", day1Noon))
			if outcome.Status != task.ClaimInstanceNotFound {
				t.Fatalf("status = %s", outcome.Status)
			}
		}, 4, 2},
		{"claim visit", func() { must(t).claim(domain.Claim(st, dailyID("20260803", "visit"), day1Noon)) }, 5, 1},
		{"claim interact", func() { must(t).claim(domain.Claim(st, dailyID("20260803", "interact"), day1Noon)) }, 6, 0},
		{"harvest satisfies growth", func() { must(t).fact(domain.RecordHarvest(st, 3, day1Noon)) }, 7, 1},
		{"claim growth advances to in-progress next", func() { must(t).claim(domain.Claim(st, task.GrowthInstanceID("harvest-3", 1), day1Noon)) }, 8, 0},
		{"harvest satisfies next growth", func() { must(t).fact(domain.RecordHarvest(st, 7, day1Noon)) }, 9, 1},
	}
	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			step.act()
			if st.TaskStateVersion != step.wantVersion {
				t.Fatalf("version = %d, want %d", st.TaskStateVersion, step.wantVersion)
			}
			if st.ClaimableCount != step.wantClaimable {
				t.Fatalf("claimable_count = %d, want %d", st.ClaimableCount, step.wantClaimable)
			}
			if st.ClaimableCount != actualClaimable(st) {
				t.Fatalf("claimable_count = %d but actual CLAIMABLE count is %d", st.ClaimableCount, actualClaimable(st))
			}
		})
	}
}

// TestBoundedState pins the bounded task-truth shape. Daily instances are
// replaced on period switch and only one current growth instance is kept;
// operation and participant receipt histories live outside DomainState.
func TestBoundedState(t *testing.T) {
	repo := newRepo()
	// Generate 25 consecutive day assignments so we can claim 75 times.
	for day := 1; day <= 25; day++ {
		period := time.Date(2026, 8, day, 0, 0, 0, 0, mustLocation(t, testTZ)).Format("20060102")
		repo.assignments[period] = task.Assignment{PeriodID: period, ConfigVersion: 7, ConfigPayloadHash: "hash-v7"}
	}
	domain := newDomain(t, repo)
	st := &task.DomainState{}

	loc := mustLocation(t, testTZ)
	var lastVersion uint64
	for day := 1; day <= 25; day++ {
		noon := time.Date(2026, 8, day, 12, 0, 0, 0, loc).UnixMilli()
		period := time.Date(2026, 8, day, 0, 0, 0, 0, loc).Format("20060102")
		must(t).fact(domain.RecordLogin(st, noon))
		must(t).fact(domain.RecordFriendVisit(st, "v", noon))
		must(t).fact(domain.RecordFriendInteraction(st, "c", noon, noon))
		for _, id := range []string{"login", "visit", "interact"} {
			outcome := must(t).claim(domain.Claim(st, dailyID(period, id), noon))
			if outcome.Status != task.ClaimSucceeded {
				t.Fatalf("day %d claim %s = %s", day, id, outcome.Status)
			}
		}
		if st.TaskStateVersion <= lastVersion {
			t.Fatalf("version not monotonic at day %d: %d <= %d", day, st.TaskStateVersion, lastVersion)
		}
		lastVersion = st.TaskStateVersion
	}
	lastNoon := time.Date(2026, 8, 25, 12, 0, 0, 0, loc).UnixMilli()
	for i := 0; i < 300; i++ {
		must(t).fact(domain.RecordHarvest(st, 1, lastNoon))
	}
	if len(st.DailyTasks) != task.DailySlotCount {
		t.Fatalf("daily task slots = %d, want %d", len(st.DailyTasks), task.DailySlotCount)
	}
	if st.DailyPeriodID != "20260825" {
		t.Fatalf("daily period = %s, want only latest period 20260825", st.DailyPeriodID)
	}
	if st.CurrentGrowth == nil {
		t.Fatal("expected exactly one current growth instance")
	}
	if st.TotalHarvestCount != 300 {
		t.Fatalf("total_harvest_count = %d, want 300", st.TotalHarvestCount)
	}
}

// TestListViewShape pins the fixed P0 list contract (Q34-08): exactly
// three daily tasks plus the current growth task, with period, version
// and next_reset fields; a bare query never completes the login task by
// itself (the actor emits the login fact explicitly).
func TestListViewShape(t *testing.T) {
	initTimes(t)
	domain := newDomain(t, newRepo())
	st := &task.DomainState{}

	view, err := domain.ListView(st, day1Noon)
	if err != nil {
		t.Fatalf("ListView: %v", err)
	}
	if view.DailyPeriodID != "20260803" || view.DailyConfigVer != 7 {
		t.Fatalf("view period=%s version=%d, want 20260803/7", view.DailyPeriodID, view.DailyConfigVer)
	}
	wantIDs := []string{dailyID("20260803", "login"), dailyID("20260803", "visit"), dailyID("20260803", "interact")}
	for i, want := range wantIDs {
		got := view.DailyTasks[i]
		if got.InstanceID != want || got.State != task.StateInProgress || got.Target != 1 {
			t.Fatalf("daily[%d] = %+v, want IN_PROGRESS %s", i, got, want)
		}
	}
	if view.CurrentGrowth == nil || view.CurrentGrowth.TaskID != "harvest-3" {
		t.Fatalf("growth = %+v, want harvest-3", view.CurrentGrowth)
	}
	if want := shMs(t, 2026, 8, 4, 0, 0, 0); view.NextResetAtMs != want {
		t.Fatalf("next_reset_at_ms = %d, want %d", view.NextResetAtMs, want)
	}
	// The query itself did not satisfy the login task.
	if view.DailyTasks[0].State != task.StateInProgress || st.ClaimableCount != 0 {
		t.Fatalf("query must not complete login: %+v claimable=%d", view.DailyTasks[0], st.ClaimableCount)
	}

	must(t).fact(domain.RecordLogin(st, day1Noon))
	view, err = domain.ListView(st, day1Noon)
	if err != nil {
		t.Fatalf("ListView: %v", err)
	}
	if view.DailyTasks[0].State != task.StateClaimable || view.ClaimableCount != 1 || view.TaskStateVersion != 2 {
		t.Fatalf("after login: %+v summary=%+v", view.DailyTasks[0], view.Summary)
	}
}

// TestDSTTimezoneDomain proves the domain honors the configured game
// timezone end to end: with a DST-observing zone the lazy switch follows
// the civil date, and a snapshot declaring a different timezone fails
// closed.
func TestDSTTimezoneDomain(t *testing.T) {
	ny := "America/New_York"
	repo := &fakeRepo{
		assignments: map[string]task.Assignment{
			"20260307": {PeriodID: "20260307", ConfigVersion: 1, ConfigPayloadHash: "ny-v1"},
			"20260308": {PeriodID: "20260308", ConfigVersion: 1, ConfigPayloadHash: "ny-v1"},
		},
		snapshots: map[uint64]*task.Config{1: {
			Version: 1, Timezone: ny, PayloadHash: "ny-v1",
			Daily: [task.DailySlotCount]task.DailyDefinition{
				{Kind: task.DailyLogin, TaskID: "login", Reward: task.RewardBundle{Coin: 1}},
				{Kind: task.DailyFriendVisit, TaskID: "visit", Reward: task.RewardBundle{Coin: 1}},
				{Kind: task.DailyFriendInteract, TaskID: "interact", Reward: task.RewardBundle{Coin: 1}},
			},
		}},
		chain:   defaultChain(),
		chainOK: true,
	}
	domain, err := task.NewDomain(repo, ny)
	if err != nil {
		t.Fatalf("NewDomain: %v", err)
	}
	loc := mustLocation(t, ny)
	st := &task.DomainState{}

	beforeMidnight := time.Date(2026, 3, 7, 23, 30, 0, 0, loc).UnixMilli()
	must(t).fact(domain.RecordLogin(st, beforeMidnight))
	if st.DailyPeriodID != "20260307" {
		t.Fatalf("period = %s, want 20260307", st.DailyPeriodID)
	}
	// 2026-03-08 00:30 EST is after the civil midnight even though UTC
	// only advanced 30 minutes into the 23-hour spring-forward day.
	afterMidnight := time.Date(2026, 3, 8, 0, 30, 0, 0, loc).UnixMilli()
	switched, err := domain.EnsureDailyPeriod(st, afterMidnight)
	if err != nil {
		t.Fatalf("EnsureDailyPeriod: %v", err)
	}
	if !switched || st.DailyPeriodID != "20260308" {
		t.Fatalf("switched=%v period=%s, want switch to 20260308", switched, st.DailyPeriodID)
	}

	// A snapshot declaring a different timezone than the domain fails
	// closed for a state that still needs to bind that snapshot. An already
	// bound period intentionally keeps its frozen config version.
	repo.snapshots[1].Timezone = testTZ
	badState := &task.DomainState{}
	if _, err := domain.EnsureDailyPeriod(badState, afterMidnight+3600_000); err == nil {
		t.Fatal("expected timezone mismatch to fail closed")
	} else if badState.DailyPeriodID != "" {
		t.Fatalf("state mutated despite timezone mismatch: %s", badState.DailyPeriodID)
	}
}

// TestNewDomainValidation pins constructor argument checks.
func TestNewDomainValidation(t *testing.T) {
	if _, err := task.NewDomain(nil, testTZ); err == nil {
		t.Fatal("nil repository must be rejected")
	}
	if _, err := task.NewDomain(newRepo(), "Not/AZone"); err == nil {
		t.Fatal("unknown timezone must be rejected")
	}
	domain, err := task.NewDomain(newRepo(), "")
	if err != nil {
		t.Fatalf("empty timezone should default to %s: %v", task.DefaultGameTimezone, err)
	}
	if domain.GameTimezone() != task.DefaultGameTimezone {
		t.Fatalf("timezone = %s, want %s", domain.GameTimezone(), task.DefaultGameTimezone)
	}
}
