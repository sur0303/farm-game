package livebot

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"farmgame/internal/demokit"
	"farmgame/internal/demoorchestrator"
)

type fakeExecutor struct {
	mu        sync.Mutex
	active    int
	maxActive int
	starts    []time.Time
	specs     []BotSpec
	delay     time.Duration
	failIndex int
	block     bool
	started   chan int
}

func (executor *fakeExecutor) Run(ctx context.Context, spec BotSpec, observer Observer) BotResult {
	executor.mu.Lock()
	executor.active++
	if executor.active > executor.maxActive {
		executor.maxActive = executor.active
	}
	executor.starts = append(executor.starts, time.Now())
	executor.specs = append(executor.specs, spec)
	if executor.started != nil {
		select {
		case executor.started <- spec.Index:
		default:
		}
	}
	executor.mu.Unlock()
	defer func() {
		executor.mu.Lock()
		executor.active--
		executor.mu.Unlock()
	}()

	observer.Login()
	observer.Hello()
	observer.Join()
	observer.Ready()
	observer.Action(spec.PickAction(0), 2*time.Millisecond)
	observer.Reconnect(1)
	if executor.block {
		<-ctx.Done()
		observer.Terminal(BotResult{Status: BotCancelled})
		return BotResult{Status: BotCancelled}
	}
	if executor.delay > 0 {
		timer := time.NewTimer(executor.delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			observer.Terminal(BotResult{Status: BotCancelled})
			return BotResult{Status: BotCancelled}
		}
	}
	status := BotSucceeded
	if executor.failIndex >= 0 && spec.Index == executor.failIndex {
		status = BotFailed
	}
	observer.Terminal(BotResult{Status: status, Ready: true, Reconnects: 1, Latency: 2 * time.Millisecond})
	return BotResult{Status: status, Ready: true, Reconnects: 1, Latency: 2 * time.Millisecond}
}

// sustainedExecutor stands in for a bot that stays online and acts many times.
type sustainedExecutor struct {
	actions int
}

func (executor *sustainedExecutor) Run(_ context.Context, spec BotSpec, observer Observer) BotResult {
	observer.Login()
	observer.Phase(PhaseLogin, 3*time.Millisecond)
	observer.Hello()
	observer.Join()
	observer.Ready()
	for ordinal := 0; ordinal < executor.actions; ordinal++ {
		latency := time.Duration(ordinal+1) * time.Millisecond
		observer.Action(spec.PickAction(ordinal), latency)
		observer.Phase(PhaseAdmission, latency)
	}
	return BotResult{
		Status: BotSucceeded, Ready: true, Online: 250 * time.Millisecond,
		Latency: time.Duration(executor.actions) * time.Millisecond, Rejected: 1, Skipped: 2,
		// Three turns of the plan went unserved: two were skipped for want of
		// a target and one timed out, and a timed-out turn still reached the
		// server.
		Planned: executor.actions + 3, Turns: executor.actions + 1, Timeouts: 1,
	}
}

// A capacity report is read per action, not per bot: a bot that acted fifty
// times must contribute fifty latency samples, and its refusals and skips have
// to stay visible without being counted as failures.
func TestRunnerAggregatesEveryActionOfASustainedBot(t *testing.T) {
	const players = 3
	const actionsPerBot = 20
	store := demoorchestrator.NewEventStore(4096)
	spec := testSpec(players, players)
	spec.SessionDuration = time.Second
	spec.ThinkTime = time.Millisecond
	spec.SessionActions = actionsPerBot
	result, err := NewRunner(store, &sustainedExecutor{actions: actionsPerBot}).Run(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != RunSucceeded {
		t.Fatalf("run status=%s", result.Status)
	}
	if result.Summary.Actions != players*actionsPerBot {
		t.Fatalf("summary actions=%d, want %d", result.Summary.Actions, players*actionsPerBot)
	}
	if result.Summary.Latency.Count != uint64(players*actionsPerBot) {
		t.Fatalf("latency samples=%d, want %d", result.Summary.Latency.Count, players*actionsPerBot)
	}
	report := result.Summary.Latency.Report()
	if report.P50Ms <= 0 || report.P95Ms < report.P50Ms || report.P99Ms < report.P95Ms {
		t.Fatalf("percentiles are not ordered: %+v", report)
	}
	if result.Summary.Rejected != players || result.Summary.Skipped != 2*players {
		t.Fatalf("rejected=%d skipped=%d", result.Summary.Rejected, result.Summary.Skipped)
	}
	if result.Summary.Failed != 0 {
		t.Fatal("server refusals must not be counted as bot failures")
	}
	if result.Summary.OnlinePeak <= 0 || result.Summary.OnlinePeak > players {
		t.Fatalf("online peak=%d, want 1..%d", result.Summary.OnlinePeak, players)
	}
	account := result.Summary.Load()
	if account.Planned != players*(actionsPerBot+3) || account.Timeouts != players {
		t.Fatalf("load account did not aggregate the plan: %+v", account)
	}
	if account.Sent != players*(actionsPerBot+1) || account.Unsent != 2*players {
		t.Fatalf("sent=%d unsent=%d, want %d/%d", account.Sent, account.Unsent, players*(actionsPerBot+1), 2*players)
	}
	if account.Qualified {
		t.Fatalf("a run that delivered %.3f of its plan must not qualify", account.Ratio)
	}
	phases := result.Summary.PhaseReports()
	if admission, ok := phases[PhaseAdmission]; !ok || admission.Count != uint64(players*actionsPerBot) {
		t.Fatalf("admission phase report=%+v", phases[PhaseAdmission])
	}
	if session, ok := phases[PhaseSession]; !ok || session.Count != players {
		t.Fatalf("session phase report=%+v", phases[PhaseSession])
	}
}

// The delivery gate exists because a closed-loop run silently offers less
// load when the server slows down. A step that missed its plan must be marked
// unqualified, whatever its latency looked like.
func TestLoadAccountGatesUnderDeliveredRuns(t *testing.T) {
	// The plan is in turns, and a turn can cost more than one command, so the
	// account is settled against Turns. Reading it off the command count would
	// let a bot that buys before it plants report more load than it planned.
	delivered := Summary{Planned: 1000, Turns: 1000, Actions: 1400, Rejected: 40, Timeouts: 10, Skipped: 3}.Load()
	if delivered.Sent != 1000 || delivered.Unsent != 0 {
		t.Fatalf("sent=%d unsent=%d, want the full plan delivered", delivered.Sent, delivered.Unsent)
	}
	if !delivered.Qualified || delivered.Ratio != 1 {
		t.Fatalf("a fully delivered step must qualify: %+v", delivered)
	}

	short := Summary{Planned: 1000, Turns: 910, Actions: 900, Rejected: 5, Timeouts: 5, Skipped: 90}.Load()
	if short.Sent != 910 || short.Unsent != 90 {
		t.Fatalf("sent=%d unsent=%d, want 910/90", short.Sent, short.Unsent)
	}
	if short.Qualified || short.Ratio >= MinDeliveryRatio {
		t.Fatalf("a step below the delivery floor must not qualify: %+v", short)
	}

	planless := Summary{Actions: 500}.Load()
	if !planless.Planless || planless.Qualified || planless.Ratio != 0 {
		t.Fatalf("a run without a cadence has no plan to judge: %+v", planless)
	}
}

func testSpec(players, maxOnline int) LiveRunSpec {
	return LiveRunSpec{
		RunID:       "run-test",
		Environment: EnvironmentSandbox,
		Players:     players,
		PlayerStart: 100,
		Duration:    2 * time.Second,
		ArrivalRate: 1000,
		MaxOnline:   maxOnline,
		Seed:        42,
		Scenario:    "smoke",
		ActionMix:   map[string]float64{"harvest": 1, "water": 1},
	}
}

func TestRunnerDrainsInFlightBotAfterRunWindow(t *testing.T) {
	store := demoorchestrator.NewEventStore(256)
	spec := testSpec(1, 1)
	spec.Duration = 50 * time.Millisecond
	result, err := NewRunner(store, &fakeExecutor{delay: 100 * time.Millisecond, failIndex: -1}).Run(context.Background(), spec)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Status != RunSucceeded || result.Summary.Succeeded != 1 || result.Summary.Cancelled != 0 {
		t.Fatalf("in-flight bot was cancelled at the run boundary: %#v", result)
	}
}

func TestRunnerNormalLifecycleAndUniqueTerminals(t *testing.T) {
	store := demoorchestrator.NewEventStore(256)
	executor := &fakeExecutor{failIndex: -1}
	result, err := NewRunner(store, executor).Run(context.Background(), testSpec(3, 2))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Status != RunSucceeded || result.Summary.Attempted != 3 || result.Summary.Ready != 3 || result.Summary.Succeeded != 3 || result.Summary.Reconnect != 3 {
		t.Fatalf("unexpected result: %#v", result)
	}
	page := store.Page(result.RunID, 0, 256)
	if len(page.Events) < 4 ||
		page.Events[0].Kind != demokit.KindRunStarted ||
		page.Events[1].Kind != demokit.KindScenarioStarted ||
		page.Events[len(page.Events)-2].Kind != demokit.KindRunCompleted ||
		page.Events[len(page.Events)-1].Kind != demokit.KindStepCompleted ||
		page.Events[len(page.Events)-1].Fields["step"] != "run_terminal" {
		t.Fatalf("lifecycle boundary = %#v", page.Events)
	}
	terminals := map[uint64]int{}
	phases := map[uint64][]string{}
	states := map[uint64][]string{}
	for _, event := range page.Events {
		if event.Player != 0 && event.Kind != demokit.KindBotTerminal {
			phases[event.Player] = append(phases[event.Player], event.Phase)
			state, _ := event.Fields["state"].(string)
			states[event.Player] = append(states[event.Player], state)
		}
		if event.Kind == demokit.KindBotTerminal {
			terminals[event.Player]++
			phases[event.Player] = append(phases[event.Player], event.Phase)
			state, _ := event.Fields["state"].(string)
			states[event.Player] = append(states[event.Player], state)
			if status := event.Fields["status"]; status != string(BotSucceeded) {
				t.Fatalf("player %d terminal status=%v", event.Player, status)
			}
		}
	}
	if len(terminals) != 3 {
		t.Fatalf("terminal players = %#v", terminals)
	}
	for player, count := range terminals {
		if count != 1 {
			t.Fatalf("player %d terminal count = %d", player, count)
		}
		wantPhases := []string{"JOINING", "JOINING", "JOINING", "READY", "READY", "RECONNECT", "CLOSED"}
		if got := phases[player]; !sameStrings(got, wantPhases) {
			t.Fatalf("player %d phases=%v want=%v", player, got, wantPhases)
		}
		wantStates := []string{"joining", "joining", "joining", "ready", "ready", "connecting", "closed"}
		if got := states[player]; !sameStrings(got, wantStates) {
			t.Fatalf("player %d states=%v want=%v", player, got, wantStates)
		}
	}
	encodedEvents, err := json.Marshal(page.Events)
	if err != nil {
		t.Fatal(err)
	}
	assertNoSensitiveEventJSON(t, encodedEvents)
	view, ok := store.RunView(result.RunID)
	if !ok || view.Execution != ExecutionRealSandbox || view.Status != string(RunSucceeded) || view.Mode != demokit.EvidenceModeLive {
		t.Fatalf("RunView = %#v, ok=%v", view, ok)
	}
	if result.CapacityClaim {
		t.Fatal("LIVE bot result must not claim capacity")
	}
}

// The event store is a process-wide lock, so a run that acts thousands of
// times a second must not touch it per action: measured against a control
// server, that append was 96% of all the driver's lock delay and it was the
// reason the run could no longer deliver its planned load. The counters and
// latency histogram carry the evidence instead, and the lifecycle events a
// reviewer reads are untouched.
func TestSustainedRunsKeepActionsOutOfTheEventStore(t *testing.T) {
	store := demoorchestrator.NewEventStore(64)
	producer := NewProducer(store)
	spec := testSpec(1, 1)
	if err := producer.RunStartedAt(spec, time.UnixMilli(1000)); err != nil {
		t.Fatalf("RunStartedAt() error = %v", err)
	}
	sustained := producer.Observer(BotSpec{
		RunID:           spec.RunID,
		PlayerID:        spec.PlayerStart,
		BotID:           "bot-online-0",
		SessionDuration: time.Minute,
	})
	sustained.Ready()
	before := len(store.Page(spec.RunID, 0, 64).Events)
	for ordinal := 0; ordinal < 50; ordinal++ {
		sustained.Action("care", 3*time.Millisecond)
	}
	if after := len(store.Page(spec.RunID, 0, 64).Events); after != before {
		t.Fatalf("sustained run appended %d events for 50 actions", after-before)
	}
	// Dropping the events must not drop the measurement.
	if got := sustained.ActionCount(); got != 50 {
		t.Fatalf("action count = %d, want 50", got)
	}
	if got := sustained.ActionLatency().Count; got != 50 {
		t.Fatalf("latency samples = %d, want every action", got)
	}

	// A single-shot bot acts once, so its action events are bounded by the
	// bot count and remain the evidence that the path ran at all.
	oneShot := producer.Observer(BotSpec{
		RunID:    spec.RunID,
		PlayerID: spec.PlayerStart + 1,
		BotID:    "bot-oneshot-1",
	})
	oneShot.Ready()
	before = len(store.Page(spec.RunID, 0, 64).Events)
	oneShot.Action("care", 3*time.Millisecond)
	if after := len(store.Page(spec.RunID, 0, 64).Events); after != before+2 {
		t.Fatalf("single-shot run appended %d events for one action, want 2", after-before)
	}
}

func TestProducerSceneLifecycleMappingIsBoundedAndDistinct(t *testing.T) {
	store := demoorchestrator.NewEventStore(64)
	producer := NewProducer(store)
	spec := testSpec(1, 1)
	startedAt := time.UnixMilli(1000)
	finishedAt := time.UnixMilli(2000)
	if err := producer.RunStartedAt(spec, startedAt); err != nil {
		t.Fatalf("RunStartedAt() error = %v", err)
	}

	observer := producer.Observer(BotSpec{
		RunID:    spec.RunID,
		PlayerID: spec.PlayerStart,
		BotID:    "bot-100-0",
	})
	observer.Ready()
	observer.Action("harvest", 5*time.Millisecond)
	observer.Reconnect(2)
	observer.Terminal(BotResult{Status: BotSucceeded})
	observer.Terminal(BotResult{Status: BotFailed})
	if err := producer.RunFinishedAt(RunResult{RunID: spec.RunID, Status: RunSucceeded}, finishedAt); err != nil {
		t.Fatalf("RunFinishedAt() error = %v", err)
	}

	events := store.Page(spec.RunID, 0, 64).Events
	wantKinds := []demokit.EventKind{
		demokit.KindRunStarted,
		demokit.KindScenarioStarted,
		demokit.KindBotReady,
		demokit.KindScenarioBarrier,
		demokit.KindBotAction,
		demokit.KindStepCompleted,
		demokit.KindBotReconnect,
		demokit.KindScenarioBarrier,
		demokit.KindBotTerminal,
		demokit.KindRunCompleted,
		demokit.KindStepCompleted,
	}
	if len(events) != len(wantKinds) {
		t.Fatalf("event count = %d, want %d: %#v", len(events), len(wantKinds), events)
	}
	for index, want := range wantKinds {
		if events[index].Kind != want {
			t.Fatalf("event %d kind = %q, want %q", index, events[index].Kind, want)
		}
	}
	if events[1].Message != "live scenario started" || events[1].Fields["scenario"] != spec.Scenario {
		t.Fatalf("scenario marker = %#v", events[1])
	}
	if events[3].Player != 0 || events[3].Fields["barrier"] != "bot_ready" {
		t.Fatalf("ready barrier = %#v", events[3])
	}
	if events[5].Player != 0 || events[5].Fields["step"] != "bot_action" || events[5].Fields["action"] != "harvest" {
		t.Fatalf("action step = %#v", events[5])
	}
	if events[7].Player != 0 || events[7].Fields["barrier"] != "bot_reconnect" {
		t.Fatalf("reconnect barrier = %#v", events[7])
	}
	if events[10].Player != 0 || events[10].Fields["step"] != "run_terminal" || events[10].Fields["status"] != string(RunSucceeded) {
		t.Fatalf("run terminal step = %#v", events[10])
	}
	terminalCount := 0
	for _, event := range events {
		if event.Kind == demokit.KindBotTerminal {
			terminalCount++
		}
	}
	if terminalCount != 1 {
		t.Fatalf("bot terminal count = %d, want 1", terminalCount)
	}
	encoded, err := json.Marshal(events)
	if err != nil {
		t.Fatal(err)
	}
	assertNoSensitiveEventJSON(t, encoded)
}

func TestRunnerPartialFailureAndCancellationAreDistinct(t *testing.T) {
	failedStore := demoorchestrator.NewEventStore(256)
	failed := &fakeExecutor{failIndex: 1}
	result, err := NewRunner(failedStore, failed).Run(context.Background(), testSpec(3, 3))
	if err != nil || result.Status != RunFailed || result.Summary.Failed != 1 || result.Summary.Succeeded != 2 {
		t.Fatalf("partial failure result=%#v err=%v", result, err)
	}
	assertTerminalStates(t, failedStore, result.RunID, map[string]int{
		string(BotSucceeded): 2,
		string(BotFailed):    1,
	})

	store := demoorchestrator.NewEventStore(256)
	started := make(chan int, 4)
	blocking := &fakeExecutor{block: true, started: started}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-started
		cancel()
	}()
	cancelled, err := NewRunner(store, blocking).Run(ctx, testSpec(4, 2))
	if err != nil || cancelled.Status != RunCancelled || cancelled.Summary.Cancelled != 4 || len(cancelled.Bots) != 4 {
		t.Fatalf("cancel result=%#v err=%v", cancelled, err)
	}
	for _, bot := range cancelled.Bots {
		if bot.Status != BotCancelled {
			t.Fatalf("bot %d status=%s", bot.PlayerID, bot.Status)
		}
	}
	page := store.Page(cancelled.RunID, 0, 256)
	last := page.Events[len(page.Events)-1]
	if last.Kind != demokit.KindStepCompleted ||
		last.Fields["step"] != "run_terminal" ||
		last.Fields["status"] != string(RunCancelled) ||
		page.Events[len(page.Events)-2].Kind != demokit.KindRunCancelled {
		t.Fatalf("last event=%#v", last)
	}
	assertTerminalStates(t, store, cancelled.RunID, map[string]int{string(BotCancelled): 4})
}

func assertTerminalStates(t *testing.T, store *demoorchestrator.EventStore, runID string, want map[string]int) {
	t.Helper()
	got := make(map[string]int)
	for _, event := range store.Page(runID, 0, 256).Events {
		if event.Kind != demokit.KindBotTerminal {
			continue
		}
		if event.Phase != "CLOSED" || event.Fields["state"] != "closed" {
			t.Fatalf("terminal mapping=%#v", event)
		}
		status, _ := event.Fields["status"].(string)
		got[status]++
	}
	if len(got) != len(want) {
		t.Fatalf("terminal statuses=%v want=%v", got, want)
	}
	for status, count := range want {
		if got[status] != count {
			t.Fatalf("terminal status %q count=%d want=%d", status, got[status], count)
		}
	}
}

func TestRunnerHonorsMaxOnlineArrivalAndSeed(t *testing.T) {
	spec := testSpec(4, 2)
	spec.ArrivalRate = 20
	executor := &fakeExecutor{delay: 10 * time.Millisecond, failIndex: -1}
	result, err := NewRunner(demoorchestrator.NewEventStore(256), executor).Run(context.Background(), spec)
	if err != nil || result.Status != RunSucceeded {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	executor.mu.Lock()
	maxActive := executor.maxActive
	starts := append([]time.Time(nil), executor.starts...)
	specs := append([]BotSpec(nil), executor.specs...)
	executor.mu.Unlock()
	if maxActive > spec.MaxOnline || len(starts) != spec.Players {
		t.Fatalf("max_active=%d starts=%d", maxActive, len(starts))
	}
	if len(starts) >= 2 && starts[1].Sub(starts[0]) < 35*time.Millisecond {
		t.Fatalf("arrival interval=%s", starts[1].Sub(starts[0]))
	}
	if len(specs) != spec.Players {
		t.Fatalf("specs=%d", len(specs))
	}
	for index, bot := range specs {
		if bot.Seed != makeBotSpecs(spec)[bot.Index].Seed || bot.PickAction(3) != makeBotSpecs(spec)[bot.Index].PickAction(3) {
			t.Fatalf("non-deterministic bot spec at %d: %#v", index, bot)
		}
	}
}

func TestProducerSanitizesFieldsAndEventStoreCapacity(t *testing.T) {
	store := demoorchestrator.NewEventStore(5)
	producer := NewProducer(store)
	if _, err := producer.Append(demokit.Event{
		RunID: "sensitive",
		Kind:  demokit.KindBotAction,
		Fields: map[string]any{
			"password":       "REPLACE_WITH_LOCAL_SECRET",
			"ticket":         "do-not-store",
			"secret":         "do-not-store",
			"safe":           "ok",
			"sensitive_note": "password=do-not-store",
			"nested":         map[string]any{"cookie": "do-not-store", "token": "do-not-store", "value": "ok"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	initialEvents := store.All()
	if len(initialEvents) != 1 || initialEvents[0].Fields["safe"] != "ok" || initialEvents[0].Fields["sensitive_note"] != "redacted" {
		t.Fatalf("sanitized fields=%#v", initialEvents)
	}
	initialJSON, err := json.Marshal(initialEvents)
	if err != nil {
		t.Fatal(err)
	}
	assertNoSensitiveEventJSON(t, initialJSON)
	if containsString(string(initialJSON), "do-not-store") {
		t.Fatalf("sensitive value leaked in %s", initialJSON)
	}
	for index := 0; index < 8; index++ {
		if _, err := producer.Append(demokit.Event{RunID: "sensitive", Kind: demokit.KindBotAction, Fields: map[string]any{"n": index}}); err != nil {
			t.Fatal(err)
		}
	}
	events := store.All()
	if len(events) != 5 {
		t.Fatalf("retained events=%d", len(events))
	}
	encoded, err := json.Marshal(events)
	if err != nil {
		t.Fatal(err)
	}
	assertNoSensitiveEventJSON(t, encoded)
	view, ok := store.RunView("sensitive")
	if !ok || view.EventCount != 9 || view.RetainedEventCount != 5 || view.DroppedEventCount != 4 {
		t.Fatalf("capacity view=%#v ok=%v", view, ok)
	}
}

func assertNoSensitiveEventJSON(t *testing.T, encoded []byte) {
	t.Helper()
	for _, forbidden := range []string{"password", "ticket", "cookie", "secret", "token", "credential", "authorization"} {
		if containsString(string(encoded), forbidden) {
			t.Fatalf("forbidden field %q in %s", forbidden, encoded)
		}
	}
}

func containsString(value, needle string) bool {
	for index := 0; index+len(needle) <= len(value); index++ {
		if value[index:index+len(needle)] == needle {
			return true
		}
	}
	return false
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func TestLiveRunSpecRejectsUnsafeBounds(t *testing.T) {
	cases := []LiveRunSpec{
		func() LiveRunSpec { spec := testSpec(1, 1); spec.Environment = Environment("prod"); return spec }(),
		func() LiveRunSpec { spec := testSpec(MaxPlayers+1, 1); return spec }(),
		func() LiveRunSpec { spec := testSpec(1, 1); spec.ArrivalRate = MaxArrivalRate + 1; return spec }(),
		func() LiveRunSpec { spec := testSpec(1, 1); spec.Duration = MaxDuration + time.Nanosecond; return spec }(),
		func() LiveRunSpec { spec := testSpec(1, 1); spec.RunID = "bad id"; return spec }(),
	}
	for index, spec := range cases {
		if err := spec.Validate(); err == nil {
			t.Errorf("case %d accepted unsafe spec", index)
		}
	}
}
