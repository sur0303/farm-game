package livebot

import (
	"context"
	"errors"
	"time"

	"farmgame/internal/demokit"
	"farmgame/internal/demoorchestrator"
)

// Runner owns only scheduling and lifecycle completion. Protocol behavior is
// delegated to Executor, and all evidence is written by Producer into the
// injected EventStore.
type Runner struct {
	Store    *demoorchestrator.EventStore
	Executor Executor
	Producer *Producer
	Clock    func() time.Time
}

// defaultRunDrainGrace lets already-launched bots finish one in-flight durable
// operation after the offered run window closes. The run window stops new
// arrivals; it must not turn a terminal PROCESSING resolution at the boundary
// into an artificial BotCancelled result.
const defaultRunDrainGrace = 2 * time.Minute

// NewRunner constructs a runner with an injected store and executor.
func NewRunner(store *demoorchestrator.EventStore, executor Executor) *Runner {
	if store == nil {
		store = demoorchestrator.NewEventStore(demoorchestrator.DefaultEventCapacity)
	}
	return &Runner{
		Store:    store,
		Executor: executor,
		Producer: NewProducer(store),
		Clock:    time.Now,
	}
}

// NewRunnerWithExecutor is the argument-order variant convenient for callers
// that start from an adapter and inject the store second.
func NewRunnerWithExecutor(executor Executor, store *demoorchestrator.EventStore) *Runner {
	return NewRunner(store, executor)
}

// Run validates a spec, schedules its bots, and returns a bounded result. Bot
// failures are represented in the result; the returned error is reserved for
// invalid setup or an evidence-store failure.
func (runner *Runner) Run(ctx context.Context, input LiveRunSpec) (RunResult, error) {
	if runner == nil {
		return RunResult{}, errors.New("nil livebot runner")
	}
	spec, err := input.normalized()
	if err != nil {
		return RunResult{RunID: input.RunID}, err
	}
	if runner.Executor == nil {
		return RunResult{RunID: spec.RunID}, errors.New("livebot executor is required")
	}
	store := runner.Store
	if store == nil && runner.Producer != nil {
		store = runner.Producer.Store()
	}
	if store == nil {
		return RunResult{RunID: spec.RunID}, errors.New("livebot event store is required")
	}
	if store.Mode() != demokit.EvidenceModeLive {
		return RunResult{RunID: spec.RunID}, ErrLiveStoreRequired
	}
	producer := runner.Producer
	if producer == nil || producer.Store() != store {
		producer = NewProducer(store)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	now := time.Now
	if runner.Clock != nil {
		now = runner.Clock
	}
	startedAt := now()
	result := RunResult{
		RunID:         spec.RunID,
		Status:        RunRunning,
		Execution:     ExecutionRealSandbox,
		CapacityClaim: false,
		StartedAt:     startedAt,
	}
	if err := producer.RegisterRun(spec, startedAt); err != nil {
		result.Err = err
		return result, err
	}
	if err := producer.RunStartedAt(spec, startedAt); err != nil {
		result.Err = err
		return result, err
	}

	botSpecs := makeBotSpecs(spec)
	completed := make(map[int]workerResult, spec.Players)
	results := make(chan workerResult, spec.Players)
	// The offered duration is a launch window, not a hard cancellation of a
	// command already inside the server. A child context is still available for
	// explicit cancellation and the bounded post-window drain below.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	runDeadline := time.Now().Add(spec.Duration)

	active := 0
	onlinePeak := 0
	nextIndex := 0
	cancelled := false
	windowExpired := false
	interval := time.Duration(float64(time.Second) / spec.ArrivalRate)
	if interval <= 0 {
		interval = time.Nanosecond
	}
	nextLaunch := time.Now()
	// arrival is how far behind its own schedule the scheduler launched each
	// bot. A run whose arrivals drift is not offering the load it claims, so
	// this is read before any latency the same run reports.
	arrival := NewLatencyHistogram()
	observeArrival := func() {
		arrival.Observe(time.Since(nextLaunch))
	}

	launch := func(bot BotSpec) {
		observer := producer.Observer(bot)
		active++
		if active > onlinePeak {
			onlinePeak = active
		}
		go func() {
			worker := workerResult{index: bot.Index, spec: bot, observer: observer, attempted: true}
			worker.result = runExecutor(runCtx, runner.Executor, bot, observer)
			worker.result = normalizeBotResult(worker.result, bot, runCtx)
			if !observer.TerminalEmitted() {
				observer.Terminal(worker.result)
			}
			if terminal, ok := observer.TerminalResult(); ok {
				worker.result = normalizeBotResult(terminal, bot, runCtx)
			}
			worker.result.Attempted = true
			results <- worker
		}()
	}

	record := func(worker workerResult) {
		if active > 0 {
			active--
		}
		completed[worker.index] = worker
	}

	for nextIndex < len(botSpecs) || active > 0 {
		if ctx.Err() != nil {
			cancelled = true
			break
		}
		if !windowExpired && !time.Now().Before(runDeadline) {
			windowExpired = true
			break
		}
		if nextIndex < len(botSpecs) && active < spec.MaxOnline {
			if nextIndex == 0 {
				observeArrival()
				launch(botSpecs[nextIndex])
				nextIndex++
				nextLaunch = nextLaunch.Add(interval)
				continue
			}
			wait := time.Until(nextLaunch)
			deadlineWait := time.Until(runDeadline)
			if deadlineWait <= 0 {
				windowExpired = true
				break
			}
			// Falling behind the arrival schedule means launch now and record
			// the delay; it does not mean the overall launch window expired.
			// Race instrumentation and a busy driver can both make a 1ms
			// cadence late while substantial run time still remains.
			if wait <= 0 {
				observeArrival()
				launch(botSpecs[nextIndex])
				nextIndex++
				nextLaunch = nextLaunch.Add(interval)
				continue
			}
			if deadlineWait < wait {
				wait = deadlineWait
			}
			timer := time.NewTimer(wait)
			select {
			case worker := <-results:
				timer.Stop()
				record(worker)
			case <-ctx.Done():
				stopTimer(timer)
				cancelled = true
			case <-timer.C:
				if ctx.Err() != nil {
					cancelled = true
					break
				}
				if !time.Now().Before(runDeadline) {
					windowExpired = true
					break
				}
				observeArrival()
				launch(botSpecs[nextIndex])
				nextIndex++
				nextLaunch = nextLaunch.Add(interval)
			}
			if cancelled || windowExpired {
				break
			}
			continue
		}
		if active > 0 {
			remaining := time.Until(runDeadline)
			if remaining <= 0 {
				windowExpired = true
				break
			}
			deadlineTimer := time.NewTimer(remaining)
			select {
			case worker := <-results:
				stopTimer(deadlineTimer)
				record(worker)
				// The online cap held this launch back, which is backpressure
				// the run asked for rather than a late scheduler. Re-basing
				// the schedule keeps that wait out of the arrival sample and
				// resumes the configured rate instead of a catch-up burst.
				nextLaunch = time.Now()
			case <-ctx.Done():
				stopTimer(deadlineTimer)
				cancelled = true
			case <-deadlineTimer.C:
				windowExpired = true
			}
			if cancelled || windowExpired {
				break
			}
			continue
		}
	}

	// If the launch window closed before all configured bots were launched, the
	// requested load was not delivered and the run remains cancelled. If all
	// bots were launched, drain them normally even when one final operation is
	// still resolving at the window boundary.
	if windowExpired && nextIndex < len(botSpecs) {
		cancelled = true
	}
	if cancelled {
		cancel()
		for active > 0 {
			record(<-results)
		}
		for nextIndex < len(botSpecs) {
			bot := botSpecs[nextIndex]
			observer := producer.Observer(bot)
			pending := BotResult{
				BotID:    bot.BotID,
				PlayerID: bot.PlayerID,
				Index:    bot.Index,
				Status:   BotCancelled,
				Error:    "run cancelled before launch",
			}
			observer.Terminal(pending)
			completed[bot.Index] = workerResult{index: bot.Index, spec: bot, observer: observer, result: pending}
			nextIndex++
		}
	} else {
		drainTimer := time.NewTimer(defaultRunDrainGrace)
		drainExpired := false
		for active > 0 {
			select {
			case worker := <-results:
				record(worker)
			case <-ctx.Done():
				cancelled = true
				cancel()
				drainExpired = true
			case <-drainTimer.C:
				cancelled = true
				cancel()
				drainExpired = true
			}
			if drainExpired {
				break
			}
		}
		if !drainExpired {
			stopTimer(drainTimer)
		} else {
			for active > 0 {
				record(<-results)
			}
		}
	}

	result.Bots = make([]BotResult, 0, len(botSpecs))
	result.Summary = Summary{Latency: NewLatencyHistogram(), OnlinePeak: onlinePeak}
	result.Summary.MergePhase(PhaseArrival, arrival)
	for _, bot := range botSpecs {
		worker, ok := completed[bot.Index]
		if !ok {
			// This is defensive: the normal paths above always create a
			// terminal state for every bot, including never-launched bots.
			observer := producer.Observer(bot)
			fallback := BotResult{BotID: bot.BotID, PlayerID: bot.PlayerID, Index: bot.Index, Status: BotCancelled}
			observer.Terminal(fallback)
			worker = workerResult{index: bot.Index, spec: bot, observer: observer, result: fallback}
		}
		botResult := worker.result
		botResult.BotID = bot.BotID
		botResult.PlayerID = bot.PlayerID
		botResult.Index = bot.Index
		if worker.attempted {
			botResult.Attempted = true
		}
		var actionLatency LatencyHistogram
		if worker.observer != nil {
			if worker.observer.ReadySeen() {
				botResult.Ready = true
			}
			if botResult.Reconnects < worker.observer.ReconnectCount() {
				botResult.Reconnects = worker.observer.ReconnectCount()
			}
			result.Summary.Actions += worker.observer.ActionCount()
			actionLatency = worker.observer.ActionLatency()
			for phase, histogram := range worker.observer.PhaseHistograms() {
				result.Summary.MergePhase(phase, histogram)
			}
		}
		result.Summary.InboundFrame += botResult.Frames
		result.Summary.Broadcasts += botResult.Broadcasts
		result.Summary.Applied += botResult.BroadcastsApplied
		result.Summary.OperationPushes += botResult.OperationPushes
		result.Summary.ResultQueries += botResult.ResultQueries
		result.Summary.Rejected += botResult.Rejected
		result.Summary.Skipped += botResult.Skipped
		result.Summary.Processing += botResult.Processing
		result.Summary.Pending += botResult.Pending
		result.Summary.ProjectionPending += botResult.ProjectionPending
		result.Summary.Planned += botResult.Planned
		result.Summary.Turns += botResult.Turns
		result.Summary.Timeouts += botResult.Timeouts
		if botResult.Online > 0 {
			sessionPhase := NewLatencyHistogram()
			sessionPhase.Observe(botResult.Online)
			result.Summary.MergePhase(PhaseSession, sessionPhase)
		}
		result.Bots = append(result.Bots, botResult)
		if botResult.Attempted {
			result.Summary.Attempted++
		}
		if botResult.Ready {
			result.Summary.Ready++
		}
		switch botResult.Status {
		case BotSucceeded:
			result.Summary.Succeeded++
		case BotFailed:
			result.Summary.Failed++
		case BotCancelled:
			result.Summary.Cancelled++
		default:
			botResult.Status = BotFailed
			result.Summary.Failed++
		}
		result.Summary.Reconnect += botResult.Reconnects
		// One sample per bot is only right when the bot acted once. An
		// executor that reported every action contributes all of them.
		if actionLatency.Count > 0 {
			result.Summary.Latency.Merge(actionLatency)
		} else {
			result.Summary.Latency.Observe(botResult.Latency)
		}
	}

	if cancelled || result.Summary.Cancelled > 0 {
		result.Status = RunCancelled
	} else if result.Summary.Failed > 0 {
		result.Status = RunFailed
	} else {
		result.Status = RunSucceeded
	}
	result.FinishedAt = now()
	if err := producer.RunFinishedAt(result, result.FinishedAt); err != nil {
		result.Err = err
		return result, err
	}
	if err := producer.Err(); err != nil {
		result.Err = err
		return result, err
	}
	return result, nil
}

// Execute is a descriptive alias for Run.
func (runner *Runner) Execute(ctx context.Context, spec LiveRunSpec) (RunResult, error) {
	return runner.Run(ctx, spec)
}

type workerResult struct {
	index     int
	spec      BotSpec
	observer  *BotObserver
	result    BotResult
	attempted bool
}

func makeBotSpecs(spec LiveRunSpec) []BotSpec {
	result := make([]BotSpec, spec.Players)
	for index := range result {
		playerID := spec.PlayerStart + uint64(index)
		mix := make(map[string]float64, len(spec.ActionMix))
		for action, weight := range spec.ActionMix {
			mix[action] = weight
		}
		result[index] = BotSpec{
			RunID:           spec.RunID,
			BotID:           botID(index, playerID),
			PlayerID:        playerID,
			Index:           index,
			Seed:            splitMix64(spec.Seed + uint64(index)),
			Environment:     spec.Environment,
			Scenario:        spec.ScenarioID,
			ActionMix:       mix,
			SessionDuration: spec.SessionDuration,
			ThinkTime:       spec.ThinkTime,
			SessionActions:  spec.SessionActions,
		}
	}
	return result
}

func runExecutor(ctx context.Context, executor Executor, spec BotSpec, observer Observer) (result BotResult) {
	defer func() {
		if recover() != nil {
			result = BotResult{
				BotID:    spec.BotID,
				PlayerID: spec.PlayerID,
				Index:    spec.Index,
				Status:   BotFailed,
				Error:    "executor panic",
			}
		}
	}()
	return executor.Run(ctx, spec, observer)
}

func normalizeBotResult(result BotResult, spec BotSpec, ctx context.Context) BotResult {
	result.BotID = spec.BotID
	result.PlayerID = spec.PlayerID
	result.Index = spec.Index
	if result.Reconnects < 0 {
		result.Reconnects = 0
	}
	if result.Status != BotSucceeded && result.Status != BotFailed && result.Status != BotCancelled {
		if ctx != nil && ctx.Err() != nil {
			result.Status = BotCancelled
		} else {
			result.Status = BotFailed
		}
	}
	return result
}

func stopTimer(timer *time.Timer) {
	if timer == nil {
		return
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}
