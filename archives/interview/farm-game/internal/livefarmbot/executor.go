// Package livefarmbot adapts the real farmbotclient session to livebot.Executor.
//
// The adapter deliberately keeps credentials outside livebot.BotSpec and event
// evidence. It composes the existing farmbotclient LoginAndConnect, JoinFarm,
// command helpers, and Reconnect APIs; it does not encode FarmFrame messages.
package livefarmbot

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/cropcatalog"
	"farmgame/internal/farmbotclient"
	"farmgame/internal/livebot"
	"farmgame/internal/shopcatalog"

	"google.golang.org/protobuf/proto"
)

// minIdlePause is the floor for an online bot that found nothing to do. It is
// short enough to react to the realtime stream and long enough that an idle
// bot costs nothing measurable.
const minIdlePause = 50 * time.Millisecond

// The one-shot functional harvest scenario may poll for a visible reward.
// Sustained runs do not sleep here: they refresh inventory on the next
// think-time turn and sell only when the Player projection shows the item.
const (
	harvestSettlementProbeWait   = 3 * time.Second
	harvestSettlementProbeEvery  = 200 * time.Millisecond
	unlockProjectionWait         = 2 * time.Second
	unlockProjectionProbeEvery   = 50 * time.Millisecond
	produceRefreshInitialWait    = 10 * time.Second
	produceRefreshInitialBackoff = 5 * time.Second
	produceRefreshMaxBackoff     = 30 * time.Second
	maxSellCount                 = 999
)

// maxPlotUnlocks bounds the warmup so a server that keeps offering a
// candidate cannot turn one bot's startup into an unbounded command loop. The
// farm holds 24 plots and opens with 5.
const (
	maxPlotUnlocks           = 19
	defaultUnlockConcurrency = 4
	maxUnlockConcurrency     = 32
)

// defaultMixedAction is what a mixed scenario falls back to when no mix was
// given.
const defaultMixedAction = "care"

// errDurableOperationProcessing is an internal control result, not a failed
// command. The admission reached the server but has no terminal outcome yet.
var errDurableOperationProcessing = errors.New("livefarmbot: durable operation processing")

// CredentialsProvider is the only credential injection seam. The adapter
// never reads process environment variables or discovers credentials itself.
type CredentialsProvider func(context.Context, uint64) (farmbotclient.Credentials, error)

// Credentials returns credentials for one player. It is useful when callers
// prefer to store a provider behind an interface-like function value.
func (provider CredentialsProvider) Credentials(ctx context.Context, playerID uint64) (farmbotclient.Credentials, error) {
	if provider == nil {
		return farmbotclient.Credentials{}, errors.New("livefarmbot: credential provider is nil")
	}
	return provider(ctx, playerID)
}

// SharedHMACCredentials returns a provider for a caller-injected secret shared
// by all players in a bounded run. The secret is copied into the provider and
// never appears in a BotSpec, event, or result summary.
func SharedHMACCredentials(secret []byte) CredentialsProvider {
	copied := append([]byte(nil), secret...)
	return func(_ context.Context, playerID uint64) (farmbotclient.Credentials, error) {
		if playerID == 0 {
			return farmbotclient.Credentials{}, errors.New("livefarmbot: player id is required")
		}
		if len(copied) == 0 {
			return farmbotclient.Credentials{}, errors.New("livefarmbot: shared HMAC secret is empty")
		}
		return farmbotclient.Credentials{
			PlayerID:    playerID,
			Secret:      append([]byte(nil), copied...),
			BootstrapID: "live-farm-bot-" + strconv.FormatUint(playerID, 10),
		}, nil
	}
}

// NewSharedHMACCredentials is a descriptive alias for SharedHMACCredentials.
func NewSharedHMACCredentials(secret []byte) CredentialsProvider {
	return SharedHMACCredentials(secret)
}

// Options controls the small amount of scenario policy that is not part of
// livebot.BotSpec. A zero TargetFarmID means the bot's own farm.
type Options struct {
	TargetFarmID uint64
	// ActionStartAt is a shared login barrier for sustained runs. Bots remain
	// connected after Join/Ready but issue no unlock or business command until
	// this instant, separating the login ramp from measured farm work.
	ActionStartAt time.Time
	// BusinessStartAt is a second shared barrier after per-bot unlock warmup.
	// It separates cold actor/direct-dispatch preparation from steady-state
	// latency without removing any warmup operation from the workload.
	BusinessStartAt time.Time
	// TargetFarms spreads bots over TargetFarms consecutive farms starting at
	// TargetFarmID, so a run can crowd a known number of farms instead of one.
	// How many bots share a farm is the variable that decides broadcast
	// fan-out, and fan-out is what separates a busy server from a hot one.
	// Zero or one keeps every bot on TargetFarmID.
	TargetFarms      int
	OfferID          int32
	CropDefinitionID int32
	// SeedBuyCount batches the real shop purchase used by sustained Journey.
	// Fixed single-action Plant keeps buying one seed for compatibility.
	SeedBuyCount int32
	// ProduceItemID is what a harvest yields and what the bot sells to fund
	// the next seed. A bot that never sells stops being able to buy once its
	// starting coins are gone, and from then on it applies less load than the
	// run asked for while looking like a server that rejected the work.
	ProduceItemID int32
	CareType      farmbotclient.CareType

	// UnlockAllPlots makes each sustained bot buy plots before the measured
	// loop begins. UnlockPlotLimit bounds how many each bot opens; zero keeps
	// the compatibility default of all 19 initially locked plots.
	UnlockAllPlots  bool
	UnlockPlotLimit int
	// UnlockConcurrency bounds the number of cross-actor UNLOCK_PLOT workflows
	// the shared executor warms up at once. Warmup is not measured load; without
	// this gate, a concurrent bot launch turns every farm's sequential unlock
	// chain into a single 20-bot admission burst.
	UnlockConcurrency int
}

// farmFor assigns a bot to its farm. Consecutive bots land on different farms
// so an arrival ramp fills the farms evenly rather than saturating the first.
func (options Options) farmFor(spec livebot.BotSpec) uint64 {
	if options.TargetFarmID == 0 {
		return spec.PlayerID
	}
	if options.TargetFarms <= 1 {
		return options.TargetFarmID
	}
	return options.TargetFarmID + uint64(spec.Index%options.TargetFarms)
}

// Executor is a real HTTP-HMAC/WSS farm bot executor.
type Executor struct {
	client      *farmbotclient.Client
	credentials CredentialsProvider
	options     Options
	unlockGate  chan struct{}
}

var _ livebot.Executor = (*Executor)(nil)

// New constructs an executor over an already configured farmbotclient.Client.
func New(client *farmbotclient.Client, credentials CredentialsProvider) (*Executor, error) {
	return NewExecutor(client, credentials)
}

// NewExecutor constructs an executor over an already configured client.
func NewExecutor(client *farmbotclient.Client, credentials CredentialsProvider) (*Executor, error) {
	return NewExecutorWithOptions(client, credentials, Options{})
}

// NewExecutorWithOptions constructs an executor with explicit crop/care
// defaults. The client remains the sole wire-protocol implementation.
func NewExecutorWithOptions(client *farmbotclient.Client, credentials CredentialsProvider, options Options) (*Executor, error) {
	if client == nil {
		return nil, errors.New("livefarmbot: farmbotclient is required")
	}
	if credentials == nil {
		return nil, errors.New("livefarmbot: credential provider is required")
	}
	normalized, err := normalizeOptions(options)
	if err != nil {
		return nil, err
	}
	return &Executor{
		client:      client,
		credentials: credentials,
		options:     normalized,
		unlockGate:  make(chan struct{}, normalized.UnlockConcurrency),
	}, nil
}

// NewWithConfig builds the existing farmbotclient and then the adapter. The
// caller still supplies credentials through the provider.
func NewWithConfig(config farmbotclient.Config, credentials CredentialsProvider) (*Executor, error) {
	client, err := farmbotclient.NewClient(config)
	if err != nil {
		return nil, err
	}
	return NewExecutor(client, credentials)
}

func normalizeOptions(options Options) (Options, error) {
	if options.OfferID == 0 {
		options.OfferID = cropcatalog.CarrotOfferID
	}
	if options.CropDefinitionID == 0 {
		options.CropDefinitionID = cropcatalog.CarrotDefinitionID
	}
	definition, ok := cropcatalog.ByDefinitionID(options.CropDefinitionID)
	if !ok {
		return Options{}, errors.New("livefarmbot: crop definition is unsupported")
	}
	if options.SeedBuyCount == 0 {
		options.SeedBuyCount = 1
	}
	if options.SeedBuyCount < 1 || options.SeedBuyCount > maxSellCount {
		return Options{}, fmt.Errorf("livefarmbot: seed buy count must be 1..%d", maxSellCount)
	}
	if options.ProduceItemID == 0 {
		options.ProduceItemID = definition.ProduceItemID
	}
	if options.ProduceItemID != definition.ProduceItemID {
		return Options{}, errors.New("livefarmbot: produce item does not match crop definition")
	}
	if _, ok := cropcatalog.ByOfferID(options.OfferID); !ok {
		return Options{}, errors.New("livefarmbot: shop offer is unsupported")
	}
	if definition.OfferID != options.OfferID {
		return Options{}, errors.New("livefarmbot: shop offer does not match crop definition")
	}
	if options.TargetFarms < 0 {
		return Options{}, errors.New("livefarmbot: target farms must not be negative")
	}
	if options.TargetFarms > 1 && options.TargetFarmID == 0 {
		return Options{}, errors.New("livefarmbot: target farms needs a target farm id to start from")
	}
	if options.UnlockPlotLimit == 0 {
		options.UnlockPlotLimit = maxPlotUnlocks
	}
	if options.UnlockPlotLimit < 1 || options.UnlockPlotLimit > maxPlotUnlocks {
		return Options{}, fmt.Errorf("livefarmbot: unlock plot limit must be 1..%d", maxPlotUnlocks)
	}
	if options.UnlockConcurrency == 0 {
		options.UnlockConcurrency = defaultUnlockConcurrency
	}
	if options.UnlockConcurrency < 1 || options.UnlockConcurrency > maxUnlockConcurrency {
		return Options{}, fmt.Errorf("livefarmbot: unlock concurrency must be 1..%d", maxUnlockConcurrency)
	}
	if options.CareType == publicpb.CareType_CARE_TYPE_UNSPECIFIED {
		options.CareType = publicpb.CareType_CARE_TYPE_WATER
	}
	switch options.CareType {
	case publicpb.CareType_CARE_TYPE_WATER, publicpb.CareType_CARE_TYPE_PEST:
	default:
		return Options{}, errors.New("livefarmbot: care type is unsupported")
	}
	return options, nil
}

// Run executes one real bot lifecycle. Terminal is intentionally not emitted
// here: livebot.Runner owns the exactly-once terminal barrier.
// The return is named so the session accounting deferred below lands on the
// result the caller receives, whichever path returned it.
func (executor *Executor) Run(ctx context.Context, spec livebot.BotSpec, observer livebot.Observer) (result livebot.BotResult) {
	result = livebot.BotResult{
		BotID:    spec.BotID,
		PlayerID: spec.PlayerID,
		Index:    spec.Index,
		Status:   livebot.BotFailed,
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if executor == nil || executor.client == nil || executor.credentials == nil {
		result.Error = "executor is not configured"
		return result
	}
	if observer == nil {
		result.Error = "observer is required"
		return result
	}
	scenario, err := selectScenario(spec)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	if spec.PlayerID == 0 {
		result.Error = "player id is required"
		return result
	}

	credentials, err := executor.credentials.Credentials(ctx, spec.PlayerID)
	if err != nil {
		return failedResult(spec, ctx, "credentials unavailable")
	}
	if credentials.PlayerID == 0 {
		credentials.PlayerID = spec.PlayerID
	}
	if credentials.PlayerID != spec.PlayerID || len(credentials.Secret) == 0 {
		return failedResult(spec, ctx, "credentials are invalid")
	}

	// Keep login and hello as separate observable phases.
	loginStarted := time.Now()
	login, err := executor.client.Login(ctx, credentials)
	if err != nil {
		return failedResult(spec, ctx, "login failed")
	}
	observer.Phase(livebot.PhaseLogin, time.Since(loginStarted))
	observer.Login()

	// ConnectAuthenticated retains credentials for Session.Reconnect.
	helloStarted := time.Now()
	session, err := executor.client.ConnectAuthenticated(ctx, login, credentials)
	if err != nil {
		return failedResult(spec, ctx, "hello failed")
	}
	defer func() { _ = session.Close() }()
	observer.Phase(livebot.PhaseHello, time.Since(helloStarted))
	observer.Hello()
	// The client can time the wire phases the executor cannot see from here:
	// how long admission took, and how much extra a PROCESSING operation spent
	// reaching its durable outcome.
	session.SetPhaseObserver(phaseBridge{observer: observer})
	onlineStarted := time.Now()
	defer func() {
		result.Online = time.Since(onlineStarted)
		stats := session.Stats()
		result.Frames = stats.FramesRead
		result.Broadcasts = stats.BroadcastsDrained
		result.BroadcastsApplied = stats.BroadcastsApplied
		result.OperationPushes = stats.OperationPushes
		result.ResultQueries = stats.ResultQueries
	}()

	var stock *seedStock
	var produce *produceStock
	if scenarioSellsProduce(spec.Scenario) {
		produce = newProduceStock(executor.options.ProduceItemID)
		if login.HomeBootstrap != nil {
			produce.load(login.HomeBootstrap)
		}
	}
	if scenario == "journey" {
		stock = newSeedStock(executor.seedItemID())
		executor.primeJourneyStock(ctx, session, spec, login, stock, produce)
	}

	targetFarmID := executor.options.farmFor(spec)
	// A real H5 session completes its own-farm recovery barrier before it may
	// issue interactive commands in a friend's farm. Joining the target farm
	// directly leaves the session recovery-incomplete and correctly causes
	// CARE/THEFT admission to fail closed.
	if targetFarmID != spec.PlayerID {
		ownJoinOperationID, ownJoinRequestID := DeterministicIDs(spec.RunID, spec.BotID, "join-own", 0)
		ownJoined, joinErr := session.JoinFarm(ctx, spec.PlayerID, ownJoinOperationID, ownJoinRequestID)
		if joinErr != nil {
			return failedResult(spec, ctx, "own-farm recovery barrier failed")
		}
		if ownJoined == nil || ownJoined.Snapshot == nil {
			return failedResult(spec, ctx, "own-farm recovery barrier returned no snapshot")
		}
	}
	joinStarted := time.Now()
	joinOperationID, joinRequestID := DeterministicIDs(spec.RunID, spec.BotID, "join", 0)
	joined, err := session.JoinFarm(ctx, targetFarmID, joinOperationID, joinRequestID)
	if err != nil {
		return failedResult(spec, ctx, "join barrier failed")
	}
	if joined == nil || joined.Snapshot == nil {
		return failedResult(spec, ctx, "join barrier returned no snapshot")
	}
	observer.Phase(livebot.PhaseJoin, time.Since(joinStarted))
	observer.Join()
	observer.Ready()
	result.Ready = true

	if spec.Sustained() {
		if !executor.options.ActionStartAt.IsZero() {
			started := time.Now()
			wait := time.Until(executor.options.ActionStartAt)
			if wait > 0 {
				timer := time.NewTimer(wait)
				select {
				case <-ctx.Done():
					timer.Stop()
					return failedResult(spec, ctx, "action warmup canceled")
				case <-timer.C:
				}
			}
			observer.Phase(livebot.PhaseWarmupPrefix+"login_hold", time.Since(started))
		}
		return executor.runOnline(ctx, session, spec, observer, result, targetFarmID, joined, stock, produce)
	}

	switch scenario {
	case "join":
		result.Status = livebot.BotSucceeded
		return result
	case "journey":
		if err := executor.runJourneyPass(ctx, session, spec, observer, targetFarmID, joined.Snapshot, stock, produce); err != nil {
			return failedResult(spec, ctx, "journey scenario failed")
		}
	case "plant":
		if targetFarmID != spec.PlayerID {
			return failedResult(spec, ctx, "plant requires the bot-owned farm")
		}
		target, ok := findPlantPlot(joined.Snapshot)
		if !ok {
			return failedResult(spec, ctx, "no suitable target plot in join snapshot for plant")
		}
		target.farmID = targetFarmID
		if err := executor.plant(ctx, session, spec, observer, target); err != nil {
			return failedResult(spec, ctx, "plant scenario failed")
		}
	case "care":
		target, ok := findCarePlotForType(joined.Snapshot, executor.options.CareType)
		if !ok {
			return failedResult(spec, ctx, "no unwatered target plot in join snapshot for care")
		}
		target.farmID = targetFarmID
		if err := executor.care(ctx, session, spec, observer, target); err != nil {
			return failedResult(spec, ctx, "care scenario failed")
		}
	case "theft":
		target, ok := findMaturePlot(joined.Snapshot)
		if !ok {
			return failedResult(spec, ctx, "no suitable mature target plot in join snapshot for theft")
		}
		target.farmID = targetFarmID
		if err := executor.theft(ctx, session, spec, observer, target); err != nil {
			return failedResult(spec, ctx, "theft scenario failed")
		}
	case "harvest":
		if targetFarmID != spec.PlayerID {
			return failedResult(spec, ctx, "harvest requires the bot-owned farm")
		}
		target, ok := findMaturePlot(joined.Snapshot)
		if !ok {
			return failedResult(spec, ctx, "no suitable mature target plot in join snapshot for harvest")
		}
		target.farmID = targetFarmID
		if err := executor.harvest(ctx, session, spec, observer, target); err != nil {
			return failedResult(spec, ctx, "harvest scenario failed")
		}
	case "reconnect":
		reconnectOperationID, reconnectRequestID := DeterministicIDs(spec.RunID, spec.BotID, "reconnect", 0)
		if _, err := session.Reconnect(ctx, reconnectOperationID, reconnectRequestID); err != nil {
			return failedResult(spec, ctx, "reconnect failed")
		}
		observer.Reconnect(1)
		observer.Ready()
		result.Reconnects = 1
	}
	result.Status = livebot.BotSucceeded
	return result
}

// phaseBridge forwards the client's wire-phase samples into the run summary.
type phaseBridge struct {
	observer livebot.Observer
}

func (bridge phaseBridge) ObservePhase(phase string, elapsed time.Duration) {
	if bridge.observer == nil {
		return
	}
	switch phase {
	case farmbotclient.PhaseAdmission:
		bridge.observer.Phase(livebot.PhaseAdmission, elapsed)
	case farmbotclient.PhaseResolution:
		bridge.observer.Phase(livebot.PhaseResolution, elapsed)
	}
}

// runOnline keeps one bot connected and acting for its whole session.
//
// This is the shape a capacity test needs: concurrency is the number of bots
// holding a session, not the rate at which short-lived bots come and go. A
// server verdict of REJECTED is a real measurement and does not fail the bot;
// only a transport or protocol failure does.
func (executor *Executor) runOnline(
	ctx context.Context,
	session *farmbotclient.Session,
	spec livebot.BotSpec,
	observer livebot.Observer,
	joinedResult livebot.BotResult,
	targetFarmID uint64,
	joined *farmbotclient.JoinResult,
	stock *seedStock,
	produce *produceStock,
) (result livebot.BotResult) {
	result = joinedResult
	snapshot := joined.Snapshot
	plantPending := make(plantReservations)
	defer func() {
		result.Pending = plantPending.durablePending()
		result.ProjectionPending = len(plantPending)
	}()
	// Buy the rest of the farm before the measured loop starts. Five plots on
	// a ten second crop cannot feed a three second cadence whatever the server
	// does, so a run on a default farm measures the game's supply limit rather
	// than the server's capacity.
	if executor.options.UnlockAllPlots {
		snapshot = executor.unlockPlots(ctx, session, spec, observer, targetFarmID, snapshot)
	}
	if !executor.options.BusinessStartAt.IsZero() {
		started := time.Now()
		wait := time.Until(executor.options.BusinessStartAt)
		if wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				result.Status = livebot.BotCancelled
				return result
			case <-timer.C:
			}
		}
		observer.Phase(livebot.PhaseWarmupPrefix+"business_hold", time.Since(started))
		if spec.ThinkTime > 0 {
			spread := time.Duration(float64(spec.ThinkTime) * spec.JitterFraction(0))
			spreadStarted := time.Now()
			if spread > 0 {
				timer := time.NewTimer(spread)
				select {
				case <-ctx.Done():
					timer.Stop()
					result.Status = livebot.BotCancelled
					return result
				case <-timer.C:
				}
			}
			observer.Phase(livebot.PhaseWarmupPrefix+"business_spread", time.Since(spreadStarted))
		}
	}
	// The clock for the plan starts after warmup. A bot cannot keep a cadence
	// it was not yet running, and 3800 startup unlocks landing at once produced
	// a tail that had nothing to do with the steady state the run is there to
	// measure. The session bound starts here for the same reason: spending the
	// budget on warmup would make a long unlock look like a short online loop.
	started := time.Now()
	deadline := started.Add(spec.SessionDuration)
	// The plan is measured over the action window the loop actually had, so a
	// run cancelled early is not reported as a bot that failed to keep cadence.
	// The bounded projection drain after the loop is deliberately excluded.
	var plannedWindow time.Duration
	defer func() {
		if plannedWindow <= 0 {
			plannedWindow = time.Since(started)
		}
		result.Planned = plannedActions(spec, plannedWindow)
	}()
	// ordinal is the loop counter that keeps every operation identity unique;
	// attempts counts only the turns that reached the server, which is what
	// the action budget is meant to bound and what the plan is compared to.
	attempts := 0
	defer func() { result.Turns = attempts }()
	for ordinal := 0; ; ordinal++ {
		if ctx.Err() != nil {
			break
		}
		if !time.Now().Before(deadline) {
			break
		}
		if spec.SessionActions > 0 && attempts >= spec.SessionActions {
			break
		}
		if ordinal > 0 && !executor.think(ctx, spec, ordinal, deadline) {
			break
		}
		// A gap in the realtime stream means the local view can no longer be
		// proven, so the bot takes a fresh join exactly like a real client.
		if view := session.FarmView(); view != nil && !view.Usable() {
			rejoinStarted := time.Now()
			operationID, requestID := DeterministicIDs(spec.RunID, spec.BotID, "rejoin", ordinal)
			rejoined, err := session.JoinFarm(ctx, targetFarmID, operationID, requestID)
			if err != nil || rejoined == nil || rejoined.Snapshot == nil {
				return terminalOnlineResult(result, spec, ctx, "online rejoin failed")
			}
			observer.Phase(livebot.PhaseJoin, time.Since(rejoinStarted))
			snapshot = rejoined.Snapshot
		}
		if current := currentSnapshot(session, snapshot); current != nil {
			snapshot = current
		}
		plantPending.reconcile(snapshot)
		// A harvest reward is a durable Farm -> Worker -> Player handoff.
		// Probe only on the next turn when a harvest is pending; there is no
		// fixed post-harvest sleep and no inventory probe on every turn.
		executor.refreshProduceStock(ctx, session, spec, ordinal, produce)

		action, target, ok, err := executor.chooseOnlineAction(spec, ordinal, snapshot, targetFarmID, produce, plantPending)
		if err != nil {
			return terminalOnlineResult(result, spec, ctx, "unsupported scenario")
		}
		if !ok {
			// Nothing to act on yet. Without a pause here a bot with no think
			// time would spin on an unchanged snapshot and burn the very CPU
			// the run is trying to measure. If a crop is about to mature
			// sooner than the think time, wait for that instead of letting
			// a ready harvest sit idle for a full cadence.
			result.Skipped++
			if !executor.idle(ctx, spec, ordinal, deadline, snapshot) {
				break
			}
			continue
		}
		target.farmID = targetFarmID
		attempts++
		switch err := executor.perform(ctx, session, spec, observer, action, ordinal, target, stock, produce, plantPending); {
		case err == nil:
		case errors.Is(err, errDurableOperationProcessing):
			result.Processing++
		case isBusinessRejection(err):
			result.Rejected++
		case ctx.Err() != nil:
			result.Status = livebot.BotCancelled
			return result
		case errors.Is(err, context.DeadlineExceeded):
			// The command reached the server and the connection is still
			// healthy; only this bot's patience ran out. Ending the bot here
			// would turn server slowness into a client-side cliff and hide
			// the very saturation the run is looking for.
			result.Timeouts++
		default:
			return terminalOnlineResult(result, spec, ctx, "online action failed")
		}
	}
	plannedWindow = time.Since(started)
	drainPlantReservations(ctx, session, snapshot, plantPending, 5*time.Second)
	result.Status = livebot.BotSucceeded
	if ctx.Err() != nil {
		result.Status = livebot.BotCancelled
	}
	return result
}

// unlockPlots buys every plot the farm will sell, one at a time in the order
// the server dictates, and returns the farm as it stands afterwards.
//
// It stops at the first refusal instead of failing the bot. Running out of
// coins or level is the farm's answer, not a broken client, and a bot with
// some of its plots is still a usable load source; turning that into a dead
// bot would lose more signal than it protects.
func (executor *Executor) unlockPlots(ctx context.Context, session *farmbotclient.Session, spec livebot.BotSpec,
	observer livebot.Observer, targetFarmID uint64, snapshot *farmpb.FarmSnapshot) *farmpb.FarmSnapshot {
	if targetFarmID != spec.PlayerID {
		return snapshot
	}
	for ordinal := 0; ordinal < executor.options.UnlockPlotLimit; ordinal++ {
		if ctx.Err() != nil {
			return snapshot
		}
		candidate, ok := findUnlockCandidate(snapshot)
		if !ok {
			return snapshot
		}
		if executor.unlockGate != nil {
			select {
			case executor.unlockGate <- struct{}{}:
			case <-ctx.Done():
				return snapshot
			}
		}
		err := executor.warmupAction(ctx, session, spec, observer, "unlock", ordinal,
			func(operationID, requestID string) error {
				// Admission establishes the one durable identity. Do not poll a
				// PROCESSING result here: the authoritative proof for choosing the
				// next sequential candidate is the FarmChange below, and a missing
				// projection stops warmup after a bounded wait.
				_, callErr := session.UnlockPlotAdmission(ctx, operationID, requestID, &publicpb.UnlockPlotRequest{
					TargetFarmId:              targetFarmID,
					PlotId:                    candidate.plotID,
					ExpectedPlotVersion:       candidate.plotVersion,
					ExpectedUnlockRuleVersion: candidate.unlockRuleVersion,
				})
				return callErr
			})
		if executor.unlockGate != nil {
			<-executor.unlockGate
		}
		if err != nil {
			// Stop warmup fail-closed at the last proven projection. The
			// measured session can still operate on plots already opened.
			return snapshot
		}
		// The terminal UnlockPlot result can arrive before the realtime FarmChange
		// is applied to the client view. Do not send the same candidate again:
		// wait for the projection to prove that this plot changed, and stop the
		// warmup fail-closed if the bounded observation window expires.
		var updated bool
		snapshot, updated = waitForUnlockProjection(ctx, session, snapshot, candidate.plotID, candidate.plotVersion)
		if !updated {
			return snapshot
		}
	}
	return snapshot
}

func drainPlantReservations(ctx context.Context, session *farmbotclient.Session, fallback *farmpb.FarmSnapshot,
	reservations plantReservations, limit time.Duration) {
	if ctx == nil || session == nil || len(reservations) == 0 || limit <= 0 {
		return
	}
	deadline := time.NewTimer(limit)
	defer deadline.Stop()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for len(reservations) != 0 {
		reservations.reconcile(currentSnapshot(session, fallback))
		if len(reservations) == 0 {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-deadline.C:
			return
		case <-ticker.C:
		}
	}
}

// findUnlockCandidate returns the one plot the server currently offers. Plots
// unlock in a fixed order, so there is never more than one.
func findUnlockCandidate(snapshot *farmpb.FarmSnapshot) (plotTarget, bool) {
	if snapshot == nil {
		return plotTarget{}, false
	}
	for _, plot := range snapshot.GetPlots() {
		if plot == nil || !plot.GetUnlockCandidate() {
			continue
		}
		if plot.GetLockState() != farmpb.PlotLockState_PLOT_LOCK_STATE_LOCKED {
			continue
		}
		return plotTarget{
			plotID:            plot.GetPlotIndex(),
			plotVersion:       plot.GetPlotVersion(),
			unlockRuleVersion: plot.GetUnlockRuleVersion(),
		}, true
	}
	return plotTarget{}, false
}

// think paces one bot. The deterministic jitter keeps a thousand bots from
// acting in lockstep, which would measure a self-inflicted thundering herd
// instead of the server.
func (executor *Executor) think(ctx context.Context, spec livebot.BotSpec, ordinal int, deadline time.Time) bool {
	if spec.ThinkTime <= 0 {
		return true
	}
	return executor.pause(ctx, spec.ThinkTime, spec.JitterFraction(ordinal), deadline)
}

// idle waits after an iteration that found nothing to do. It never returns
// immediately, so a bot cannot poll its own snapshot in a tight loop.
func (executor *Executor) idle(ctx context.Context, spec livebot.BotSpec, ordinal int, deadline time.Time, snapshot *farmpb.FarmSnapshot) bool {
	wait := spec.ThinkTime
	if wait < minIdlePause {
		wait = minIdlePause
	}
	if next := timeUntilNextMature(snapshot); next > 0 && next < wait {
		wait = next
	}
	return executor.pause(ctx, wait, spec.JitterFraction(ordinal), deadline)
}

func (executor *Executor) pause(ctx context.Context, base time.Duration, jitter float64, deadline time.Time) bool {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return false
	}
	wait := time.Duration(float64(base) * (0.8 + 0.4*jitter))
	if wait > remaining {
		wait = remaining
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func currentSnapshot(session *farmbotclient.Session, fallback *farmpb.FarmSnapshot) *farmpb.FarmSnapshot {
	view := session.FarmView()
	if view == nil || !view.Usable() {
		return fallback
	}
	if snapshot := view.Snapshot(); snapshot != nil {
		return snapshot
	}
	return fallback
}

func waitForUnlockProjection(ctx context.Context, session *farmbotclient.Session, fallback *farmpb.FarmSnapshot, plotID uint32, previousVersion uint64) (*farmpb.FarmSnapshot, bool) {
	deadline := time.NewTimer(unlockProjectionWait)
	defer deadline.Stop()
	ticker := time.NewTicker(unlockProjectionProbeEvery)
	defer ticker.Stop()
	for {
		snapshot := currentSnapshot(session, fallback)
		if snapshot != nil {
			for _, plot := range snapshot.GetPlots() {
				if plot == nil || plot.GetPlotIndex() != plotID {
					continue
				}
				if plot.GetPlotVersion() > previousVersion || plot.GetLockState() != farmpb.PlotLockState_PLOT_LOCK_STATE_LOCKED {
					return snapshot, true
				}
			}
		}
		select {
		case <-ctx.Done():
			return snapshot, false
		case <-deadline.C:
			return snapshot, false
		case <-ticker.C:
		}
	}
}

// chooseAction resolves every action the mix offers against the current farm
// and draws from the ones that have a target, so a turn is skipped only when
// the farm offers nothing at all rather than when the blind draw happened to
// name something impossible.
//
// A fixed scenario is not a mix and is left alone: a run that asked for
// nothing but theft must not quietly start planting. Journey is also not a
// mix: it picks the highest-priority legal action on this snapshot, so a
// mature crop is harvested before the bot waters or plants around it.
func (executor *Executor) chooseAction(spec livebot.BotSpec, ordinal int,
	snapshot *farmpb.FarmSnapshot, targetFarmID uint64) (string, plotTarget, bool, error) {
	return executor.chooseActionWithReservations(spec, ordinal, snapshot, targetFarmID, nil, false)
}

func (executor *Executor) chooseActionWithReservations(spec livebot.BotSpec, ordinal int,
	snapshot *farmpb.FarmSnapshot, targetFarmID uint64, pending plantReservations, blockPlant bool) (string, plotTarget, bool, error) {
	if !mixedScenario(spec.Scenario) {
		action, err := normalizeScenario(spec.Scenario)
		if err != nil {
			return "", plotTarget{}, false, err
		}
		if action == "journey" {
			return executor.chooseJourneyActionWithReservations(snapshot, spec.PlayerID, targetFarmID, pending, blockPlant)
		}
		target, ok := executor.selectTargetWithReservations(action, snapshot, spec.PlayerID, targetFarmID, pending, blockPlant)
		return action, target, ok, nil
	}

	// Resolving before drawing costs one scan of the snapshot per candidate,
	// which is cheap next to the command the bot is about to send.
	candidates := make([]string, 0, len(spec.ActionMix))
	for candidate, weight := range spec.ActionMix {
		if weight > 0 {
			candidates = append(candidates, candidate)
		}
	}
	// A mixed scenario with no mix is the single default action, which is what
	// a blind draw resolved to before there was anything to draw from.
	if len(candidates) == 0 {
		candidates = []string{defaultMixedAction}
	}

	targets := make(map[string]plotTarget, len(candidates))
	legal := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		action, err := normalizeScenario(candidate)
		if err != nil {
			continue
		}
		target, ok := executor.selectTargetWithReservations(action, snapshot, spec.PlayerID, targetFarmID, pending, blockPlant)
		if !ok {
			continue
		}
		targets[candidate] = target
		legal = append(legal, candidate)
	}
	if len(legal) == 0 {
		return "", plotTarget{}, false, nil
	}
	drawn := legal[0]
	if len(legal) > 1 {
		drawn = spec.PickActionAmong(ordinal, legal)
	}
	action, err := normalizeScenario(drawn)
	if err != nil {
		return "", plotTarget{}, false, err
	}
	return action, targets[drawn], true, nil
}

func mixedScenario(scenario string) bool {
	switch strings.ToLower(strings.TrimSpace(scenario)) {
	case "", "default", "mixed":
		return true
	default:
		return false
	}
}

func scenarioSellsProduce(scenario string) bool {
	switch strings.ToLower(strings.TrimSpace(scenario)) {
	case "", "default", "mixed", "journey", "harvest":
		return true
	default:
		return false
	}
}

// chooseOnlineAction adds the Player-visible SELL target ahead of the farm
// journey. It is deliberately separate from chooseAction: the latter remains
// a pure farm-snapshot selector used by functional tests, while online runs
// also carry the authenticated inventory projection.
func (executor *Executor) chooseOnlineAction(spec livebot.BotSpec, ordinal int,
	snapshot *farmpb.FarmSnapshot, targetFarmID uint64, produce *produceStock,
	pending plantReservations) (string, plotTarget, bool, error) {
	blockPlant := false
	if targetFarmID == spec.PlayerID && scenarioSellsProduce(spec.Scenario) && produce != nil {
		if target, ok := produce.sellTarget(); ok {
			return "sell", target, true, nil
		}
		// Farm HARVEST empties the public plot before the durable Player reward
		// clears Player-side occupancy. Block only those harvested plots; other
		// empty plots remain legal work while Player projection catches up.
		pending = pending.withBlocked(produce.blockedPlots)
	}
	return executor.chooseActionWithReservations(spec, ordinal, snapshot, targetFarmID, pending, blockPlant)
}

func (executor *Executor) seedItemID() int32 {
	definition, ok := cropcatalog.ByDefinitionID(executor.options.CropDefinitionID)
	if !ok {
		return cropcatalog.CarrotSeedItemID
	}
	return definition.SeedItemID
}

// seedStock is the journey bag: a player who already has a carrot seed
// does not buy another before planting. The single-action plant scenario
// still always buys, so a plant-only run keeps generating that command.
type seedStock struct {
	itemID int32
	count  int64
}

func newSeedStock(itemID int32) *seedStock {
	return &seedStock{itemID: itemID}
}

func (stock *seedStock) has() bool {
	return stock != nil && stock.count > 0
}

func (stock *seedStock) credit(delta int64) {
	if stock == nil || delta <= 0 {
		return
	}
	stock.count += delta
}

func (stock *seedStock) consume() {
	if stock == nil || stock.count <= 0 {
		return
	}
	stock.count--
}

func (stock *seedStock) load(view *publicpb.GetPlayerBootstrapResult) {
	if stock == nil || view == nil || stock.itemID == 0 {
		return
	}
	stock.count = 0
	for _, stack := range view.GetInventory().GetItems() {
		if stack == nil || stack.GetItemId() != stock.itemID {
			continue
		}
		if stack.GetCount() > 0 {
			stock.count += stack.GetCount()
		}
	}
}

// produceStock tracks only Player-visible produce. A successful HARVEST
// records a pending reward, but does not make it sellable; the next turn must
// refresh the authenticated inventory and observe the reward first.
type produceStock struct {
	itemID          int32
	visible         int64
	readyToSell     int64
	pendingReward   int64
	pendingUnknown  bool
	refreshNeeded   bool
	refreshFailures uint
	nextRefreshAt   time.Time
	blockedPlots    map[uint32]struct{}
}

func newProduceStock(itemID int32) *produceStock {
	return &produceStock{itemID: itemID, blockedPlots: make(map[uint32]struct{})}
}

func (stock *produceStock) load(view *publicpb.GetPlayerBootstrapResult) {
	if stock == nil || view == nil || stock.itemID == 0 {
		return
	}
	stock.visible = inventoryItemCount(view, stock.itemID)
	stock.readyToSell = stock.visible
	if stock.readyToSell > maxSellCount {
		stock.readyToSell = maxSellCount
	}
	stock.pendingReward = 0
	stock.pendingUnknown = false
	stock.refreshNeeded = false
	clear(stock.blockedPlots)
	stock.refreshFailures = 0
	stock.nextRefreshAt = time.Time{}
}

func (stock *produceStock) notePendingHarvest(plotID uint32) {
	if stock == nil {
		return
	}
	stock.pendingUnknown = true
	stock.refreshNeeded = true
	stock.blockedPlots[plotID] = struct{}{}
	stock.refreshFailures = 0
	stock.nextRefreshAt = time.Now().Add(produceRefreshInitialWait)
}

func (stock *produceStock) noteHarvest(plotID, quantity uint32) {
	if stock == nil || quantity == 0 {
		return
	}
	stock.pendingReward += int64(quantity)
	stock.refreshNeeded = true
	stock.blockedPlots[plotID] = struct{}{}
	stock.refreshFailures = 0
	stock.nextRefreshAt = time.Now().Add(produceRefreshInitialWait)
}

func (stock *produceStock) needsRefresh() bool {
	return stock != nil && stock.refreshNeeded
}

func (stock *produceStock) refreshDue(now time.Time) bool {
	return stock != nil && stock.refreshNeeded && (stock.nextRefreshAt.IsZero() || !now.Before(stock.nextRefreshAt))
}

func (stock *produceStock) refreshFailed(now time.Time) {
	if stock == nil {
		return
	}
	stock.refreshNeeded = true
	shift := stock.refreshFailures
	if shift > 5 {
		shift = 5
	}
	delay := produceRefreshInitialBackoff * time.Duration(1<<shift)
	if delay > produceRefreshMaxBackoff {
		delay = produceRefreshMaxBackoff
	}
	stock.refreshFailures++
	stock.nextRefreshAt = now.Add(delay)
}

func (stock *produceStock) refresh(view *publicpb.GetPlayerBootstrapResult) bool {
	if stock == nil || view == nil || stock.itemID == 0 {
		return false
	}
	next := inventoryItemCount(view, stock.itemID)
	progressed := next > stock.visible
	if delta := next - stock.visible; delta > 0 {
		if stock.pendingUnknown {
			stock.readyToSell += delta
			stock.pendingUnknown = false
		} else if stock.pendingReward > 0 {
			settled := delta
			if settled > stock.pendingReward {
				settled = stock.pendingReward
			}
			stock.readyToSell += settled
			stock.pendingReward -= settled
		}
	}
	stock.visible = next
	if stock.readyToSell > stock.visible {
		stock.readyToSell = stock.visible
	}
	if stock.pendingReward == 0 && !stock.pendingUnknown {
		stock.refreshNeeded = false
		stock.refreshFailures = 0
		stock.nextRefreshAt = time.Time{}
		clear(stock.blockedPlots)
	}
	return progressed
}

func (stock *produceStock) sellTarget() (plotTarget, bool) {
	if stock == nil || stock.readyToSell <= 0 {
		return plotTarget{}, false
	}
	count := stock.readyToSell
	if count > maxSellCount {
		count = maxSellCount
	}
	return plotTarget{sellCount: uint32(count)}, true
}

func (stock *produceStock) sold(count uint32) {
	if stock == nil || count == 0 {
		return
	}
	stock.readyToSell -= int64(count)
	stock.visible -= int64(count)
	if stock.readyToSell < 0 {
		stock.readyToSell = 0
	}
	if stock.visible < 0 {
		stock.visible = 0
	}
}

func (stock *produceStock) requestRefresh() {
	if stock != nil {
		stock.refreshNeeded = true
		stock.refreshFailures = 0
		stock.nextRefreshAt = time.Time{}
	}
}

// primeJourneyStock seeds the bag from the login projection when the Gate
// attached one. A missing or unusable projection falls back to WSS
// GET_PLAYER_BOOTSTRAP, matching H5. Failure here is not a failed bot: the
// plant path still buys a seed when the bag is empty.
func (executor *Executor) primeJourneyStock(
	ctx context.Context,
	session *farmbotclient.Session,
	spec livebot.BotSpec,
	login farmbotclient.LoginResult,
	stock *seedStock,
	produce *produceStock,
) {
	if view := login.HomeBootstrap; view != nil {
		stock.load(view)
		produce.load(view)
		return
	}
	operationID, requestID := DeterministicIDs(spec.RunID, spec.BotID, "bootstrap", 0)
	result, err := session.GetPlayerBootstrap(ctx, operationID, requestID)
	if err != nil {
		return
	}
	view := decodeBootstrapView(result)
	stock.load(view)
	produce.load(view)
}

func (executor *Executor) refreshProduceStock(ctx context.Context, session *farmbotclient.Session, spec livebot.BotSpec, ordinal int, stock *produceStock) {
	now := time.Now()
	if stock == nil || !stock.refreshDue(now) {
		return
	}
	operationID, requestID := DeterministicIDs(spec.RunID, spec.BotID, "inventory-refresh", ordinal)
	result, err := session.GetPlayerBootstrap(ctx, operationID, requestID)
	view := decodeBootstrapView(result)
	if err != nil || view == nil {
		stock.refreshFailed(now)
		return
	}
	progressed := stock.refresh(view)
	if stock.needsRefresh() {
		if progressed {
			stock.refreshFailures = 0
		}
		// A successful read with no reward is still a deferred projection, not
		// a reason to poll every cadence turn. Back off exactly like a transient
		// read failure while keeping the durable reward pending.
		stock.refreshFailed(now)
	}
}

func decodeBootstrapView(result *farmbotclient.CommandResult) *publicpb.GetPlayerBootstrapResult {
	if result == nil {
		return nil
	}
	var view publicpb.GetPlayerBootstrapResult
	if err := proto.Unmarshal(result.GetPayload(), &view); err != nil {
		return nil
	}
	return &view
}

// journeyOrder is the one-pass farm loop a player actually walks. Harvest
// (or theft on a friend's farm) comes first because a just-planted carrot
// takes ten seconds to mature; watering and planting around it must not
// pick that same crop on the same tick.
func journeyOrder(ownFarm bool) []string {
	if ownFarm {
		return []string{"harvest", "care", "plant"}
	}
	return []string{"theft", "care"}
}

func (executor *Executor) chooseJourneyAction(snapshot *farmpb.FarmSnapshot, playerID, targetFarmID uint64) (string, plotTarget, bool, error) {
	return executor.chooseJourneyActionWithReservations(snapshot, playerID, targetFarmID, nil, false)
}

func (executor *Executor) chooseJourneyActionWithReservations(snapshot *farmpb.FarmSnapshot, playerID, targetFarmID uint64,
	pending plantReservations, blockPlant bool) (string, plotTarget, bool, error) {
	for _, action := range journeyOrder(targetFarmID == playerID) {
		target, ok := executor.selectTargetWithReservations(action, snapshot, playerID, targetFarmID, pending, blockPlant)
		if ok {
			return action, target, true, nil
		}
	}
	return "", plotTarget{}, false, nil
}

// runJourneyPass walks each journey kind at most once. Missing targets are
// skipped, so an empty farm is a successful plant (or a successful no-op
// when even planting has nowhere to go) rather than a failed bot.
func (executor *Executor) runJourneyPass(
	ctx context.Context,
	session *farmbotclient.Session,
	spec livebot.BotSpec,
	observer livebot.Observer,
	targetFarmID uint64,
	snapshot *farmpb.FarmSnapshot,
	stock *seedStock,
	produce *produceStock,
) error {
	for ordinal, action := range journeyOrder(targetFarmID == spec.PlayerID) {
		if current := currentSnapshot(session, snapshot); current != nil {
			snapshot = current
		}
		target, ok := executor.selectTarget(action, snapshot, spec.PlayerID, targetFarmID)
		if !ok {
			continue
		}
		target.farmID = targetFarmID
		if err := executor.perform(ctx, session, spec, observer, action, ordinal, target, stock, produce, nil); err != nil {
			return err
		}
	}
	return nil
}

func (executor *Executor) selectTarget(action string, snapshot *farmpb.FarmSnapshot, playerID, targetFarmID uint64) (plotTarget, bool) {
	return executor.selectTargetWithReservations(action, snapshot, playerID, targetFarmID, nil, false)
}

func (executor *Executor) selectTargetWithReservations(action string, snapshot *farmpb.FarmSnapshot, playerID, targetFarmID uint64,
	pending plantReservations, blockPlant bool) (plotTarget, bool) {
	switch action {
	case "plant":
		if targetFarmID != playerID || blockPlant {
			return plotTarget{}, false
		}
		return findPlantPlotExcluding(snapshot, pending)
	case "care":
		return findCarePlotForType(snapshot, executor.options.CareType)
	case "theft":
		if targetFarmID == playerID {
			return plotTarget{}, false
		}
		return findMaturePlot(snapshot)
	case "harvest":
		if targetFarmID != playerID {
			return plotTarget{}, false
		}
		return findMaturePlot(snapshot)
	default:
		return plotTarget{}, false
	}
}

func (executor *Executor) perform(ctx context.Context, session *farmbotclient.Session, spec livebot.BotSpec, observer livebot.Observer,
	action string, ordinal int, target plotTarget, stock *seedStock, produce *produceStock, pending plantReservations) error {
	switch action {
	case "plant":
		return executor.plantAt(ctx, session, spec, observer, ordinal, target, stock, pending)
	case "care":
		return executor.careAt(ctx, session, spec, observer, ordinal, target)
	case "theft":
		return executor.theftAt(ctx, session, spec, observer, ordinal, target)
	case "harvest":
		return executor.harvestAt(ctx, session, spec, observer, ordinal, target, produce)
	case "sell":
		return executor.sellAt(ctx, session, spec, observer, ordinal, target, produce)
	case "join":
		return nil
	default:
		return errors.New("unsupported online action")
	}
}

// plannedActions is how many commands this bot's cadence asked for over the
// window it was online. Without a think time there is no cadence to compare
// against, so an unbounded run reports no plan rather than a made-up one.
func plannedActions(spec livebot.BotSpec, online time.Duration) int {
	if spec.ThinkTime <= 0 {
		return spec.SessionActions
	}
	if online < 0 {
		online = 0
	}
	// The first action is issued immediately; every later one costs one think
	// time.
	planned := 1 + int(online/spec.ThinkTime)
	if spec.SessionActions > 0 && planned > spec.SessionActions {
		planned = spec.SessionActions
	}
	return planned
}

// isBusinessRejection reports whether the server refused the command on
// business grounds. That verdict is data, not a broken bot.
func isBusinessRejection(err error) bool {
	var rejected *farmbotclient.RejectedError
	if errors.As(err, &rejected) {
		return true
	}
	var unexpected *farmbotclient.UnexpectedStatusError
	return errors.As(err, &unexpected)
}

func terminalOnlineResult(result livebot.BotResult, spec livebot.BotSpec, ctx context.Context, reason string) livebot.BotResult {
	failed := failedResult(spec, ctx, reason)
	result.Status = failed.Status
	result.Error = failed.Error
	return result
}

func normalizeScenario(value string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "default", "join", "mixed":
		return "join", nil
	case "plant", "care", "theft", "harvest", "reconnect", "journey":
		return strings.ToLower(strings.TrimSpace(value)), nil
	default:
		return "", errors.New("unsupported scenario")
	}
}

func selectScenario(spec livebot.BotSpec) (string, error) {
	raw := strings.ToLower(strings.TrimSpace(spec.Scenario))
	scenario, err := normalizeScenario(raw)
	if err != nil {
		return "", err
	}
	if raw != "" && raw != "default" && raw != "mixed" {
		return scenario, nil
	}
	selected := strings.ToLower(strings.TrimSpace(spec.PickAction(0)))
	if selected == "" || selected == "default" || selected == "join" || selected == "mixed" {
		return "join", nil
	}
	return normalizeScenario(selected)
}

type plotTarget struct {
	farmID            uint64
	plotID            uint32
	plotVersion       uint64
	cropCycleID       string
	cropDefinitionID  uint32
	unlockRuleVersion uint64
	sellCount         uint32
}

// plantReservations is the bot's fail-closed overlay for commands accepted
// ahead of the realtime Farm projection. A reserved plot is never selected
// again until a newer plot version or a crop cycle proves that the projection
// advanced. This prevents an old empty PlotView from manufacturing
// ERR_PLOT_NOT_EMPTY retries.
type plantReservation struct {
	version    uint64
	processing bool
}

type plantReservations map[uint32]plantReservation

func (reservations plantReservations) reserve(target plotTarget, processing bool) {
	if reservations != nil {
		// Plot indexes are zero-based; plot 0 is the first real plot, not an
		// unspecified sentinel.
		reservations[target.plotID] = plantReservation{version: target.plotVersion, processing: processing}
	}
}

func (reservations plantReservations) durablePending() int {
	pending := 0
	for _, reservation := range reservations {
		if reservation.processing {
			pending++
		}
	}
	return pending
}

func (reservations plantReservations) withBlocked(blocked map[uint32]struct{}) plantReservations {
	if len(blocked) == 0 {
		return reservations
	}
	combined := make(plantReservations, len(reservations)+len(blocked))
	for plotID, reservation := range reservations {
		combined[plotID] = reservation
	}
	for plotID := range blocked {
		if _, exists := combined[plotID]; !exists {
			combined[plotID] = plantReservation{}
		}
	}
	return combined
}

func (reservations plantReservations) reconcile(snapshot *farmpb.FarmSnapshot) {
	if len(reservations) == 0 || snapshot == nil {
		return
	}
	for _, plot := range snapshot.GetPlots() {
		if plot == nil {
			continue
		}
		reservation, reserved := reservations[plot.GetPlotIndex()]
		if reserved && (plot.GetPlotVersion() > reservation.version || strings.TrimSpace(plot.GetCropCycleId()) != "") {
			delete(reservations, plot.GetPlotIndex())
		}
	}
}

func findPlantPlot(snapshot *farmpb.FarmSnapshot) (plotTarget, bool) {
	return findPlantPlotExcluding(snapshot, nil)
}

func findPlantPlotExcluding(snapshot *farmpb.FarmSnapshot, reservations plantReservations) (plotTarget, bool) {
	if snapshot == nil {
		return plotTarget{}, false
	}
	for _, plot := range snapshot.GetPlots() {
		if plot == nil || plot.GetLockState() == farmpb.PlotLockState_PLOT_LOCK_STATE_LOCKED {
			continue
		}
		if strings.TrimSpace(plot.GetCropCycleId()) != "" {
			continue
		}
		if _, reserved := reservations[plot.GetPlotIndex()]; reserved {
			continue
		}
		return plotTarget{plotID: plot.GetPlotIndex(), plotVersion: plot.GetPlotVersion()}, true
	}
	return plotTarget{}, false
}

const (
	careDueSafetyMargin         = 100 * time.Millisecond
	harvestMaturitySafetyMargin = 500 * time.Millisecond
)

// findCarePlot keeps the historical WATER default for callers and tests that
// do not carry an explicit care type.
func findCarePlot(snapshot *farmpb.FarmSnapshot) (plotTarget, bool) {
	return findCarePlotForType(snapshot, publicpb.CareType_CARE_TYPE_WATER)
}

func findCarePlotForType(snapshot *farmpb.FarmSnapshot, careType publicpb.CareType) (plotTarget, bool) {
	if snapshot == nil {
		return plotTarget{}, false
	}
	now := time.Now().UnixMilli()
	for _, plot := range snapshot.GetPlots() {
		if plot == nil || strings.TrimSpace(plot.GetCropCycleId()) == "" || plot.GetWatered() {
			continue
		}
		// Care is legal only after the frozen incident offset and before
		// maturity. PlotView exposes PlantAt/MatureAt, while the care offset is
		// part of the same versioned crop catalog used by the FarmActor.
		// Unknown legacy timestamps remain eligible because older tests and
		// projections did not carry PlantAtMs; real snapshots always do.
		if !careIncidentDue(plot, careType, now) || plotIsMature(plot, now) {
			continue
		}
		return plotTarget{
			plotID:           plot.GetPlotIndex(),
			plotVersion:      plot.GetPlotVersion(),
			cropCycleID:      plot.GetCropCycleId(),
			cropDefinitionID: uint32(maxInt32(plot.GetCropDefinitionId())),
		}, true
	}
	return plotTarget{}, false
}

func careIncidentDue(plot *farmpb.PlotView, careType publicpb.CareType, nowMs int64) bool {
	if plot == nil || plot.GetPlantAtMs() <= 0 {
		return true
	}
	definition, ok := cropcatalog.ByDefinitionID(plot.GetCropDefinitionId())
	if !ok {
		return false
	}
	offset := definition.WaterOffset
	if careType == publicpb.CareType_CARE_TYPE_PEST {
		offset = definition.RemovePestOffset
	}
	dueAtMs := plot.GetPlantAtMs() + int64(offset/time.Millisecond)
	return nowMs >= dueAtMs+int64(careDueSafetyMargin/time.Millisecond)
}

func findMaturePlot(snapshot *farmpb.FarmSnapshot) (plotTarget, bool) {
	if snapshot == nil {
		return plotTarget{}, false
	}
	now := time.Now().UnixMilli()
	for _, plot := range snapshot.GetPlots() {
		if plot == nil || strings.TrimSpace(plot.GetCropCycleId()) == "" {
			continue
		}
		if !plotHarvestSafe(plot, now) || plot.GetHarvestableCount() <= 0 {
			continue
		}
		return plotTarget{
			plotID:           plot.GetPlotIndex(),
			plotVersion:      plot.GetPlotVersion(),
			cropCycleID:      plot.GetCropCycleId(),
			cropDefinitionID: uint32(maxInt32(plot.GetCropDefinitionId())),
		}, true
	}
	return plotTarget{}, false
}

func plotHarvestSafe(plot *farmpb.PlotView, now int64) bool {
	if plot == nil {
		return false
	}
	if plot.GetCropPhase() == farmpb.CropPhase_MATURE {
		return true
	}
	return plot.GetMatureAtMs() > 0 &&
		plot.GetMatureAtMs()+int64(harvestMaturitySafetyMargin/time.Millisecond) <= now
}

func plotIsMature(plot *farmpb.PlotView, now int64) bool {
	if plot == nil {
		return false
	}
	// The server derives the phase from the clock each time it renders a
	// plot: growing until the maturity instant, mature after it. Nothing
	// happens at that instant, so no push announces it, and a cached view
	// keeps saying GROWING for a crop that has been ready for minutes.
	return plot.GetCropPhase() == farmpb.CropPhase_MATURE ||
		(plot.GetMatureAtMs() > 0 && plot.GetMatureAtMs() <= now)
}

func timeUntilNextMature(snapshot *farmpb.FarmSnapshot) time.Duration {
	if snapshot == nil {
		return 0
	}
	now := time.Now().UnixMilli()
	var soonest int64
	for _, plot := range snapshot.GetPlots() {
		if plot == nil || strings.TrimSpace(plot.GetCropCycleId()) == "" {
			continue
		}
		at := plot.GetMatureAtMs() + int64(harvestMaturitySafetyMargin/time.Millisecond)
		if at <= now {
			continue
		}
		if soonest == 0 || at < soonest {
			soonest = at
		}
	}
	if soonest == 0 {
		return 0
	}
	return time.Duration(soonest-now) * time.Millisecond
}

func maxInt32(value int32) int32 {
	if value < 0 {
		return 0
	}
	return value
}

func (executor *Executor) buyCall(ctx context.Context, session *farmbotclient.Session, count int32) func(string, string) error {
	return func(operationID, requestID string) error {
		_, err := session.Buy(ctx, operationID, requestID, &publicpb.BuyRequest{
			OfferId:              executor.options.OfferID,
			Count:                count,
			ExpectedOfferVersion: shopcatalog.OfferVersionOf(executor.options.OfferID),
		})
		return err
	}
}

func (executor *Executor) plantCall(ctx context.Context, session *farmbotclient.Session, target plotTarget) func(string, string) error {
	return func(operationID, requestID string) error {
		_, err := session.Plant(ctx, operationID, requestID, &publicpb.PlantRequest{
			PlotId:              target.plotID,
			CropDefinitionId:    uint32(executor.options.CropDefinitionID),
			TargetFarmId:        target.farmID,
			ExpectedPlotVersion: target.plotVersion,
		})
		return err
	}
}

func (executor *Executor) plantAdmissionCall(ctx context.Context, session *farmbotclient.Session, target plotTarget) func(string, string) error {
	return func(operationID, requestID string) error {
		_, err := session.PlantAdmission(ctx, operationID, requestID, &publicpb.PlantRequest{
			PlotId:              target.plotID,
			CropDefinitionId:    uint32(executor.options.CropDefinitionID),
			TargetFarmId:        target.farmID,
			ExpectedPlotVersion: target.plotVersion,
		})
		return err
	}
}

func (executor *Executor) careCall(ctx context.Context, session *farmbotclient.Session, target plotTarget) func(string, string) error {
	return func(operationID, requestID string) error {
		_, err := session.Care(ctx, operationID, requestID, &publicpb.CareRequest{
			PlotId:       target.plotID,
			TargetFarmId: target.farmID,
			CropCycleId:  target.cropCycleID,
			CareType:     executor.options.CareType,
		})
		return err
	}
}

func (executor *Executor) theftCall(ctx context.Context, session *farmbotclient.Session, target plotTarget) func(string, string) error {
	return func(operationID, requestID string) error {
		_, err := session.Theft(ctx, operationID, requestID, &publicpb.TheftRequest{
			PlotId:       target.plotID,
			TargetFarmId: target.farmID,
			CropCycleId:  target.cropCycleID,
		})
		return err
	}
}

// harvestCall reports what the harvest yielded through harvested, so the
// caller can sell exactly that much. Selling a guessed amount would be
// rejected whenever the guess was wrong, and those rejections would be
// indistinguishable from the server refusing real work.
func (executor *Executor) harvestCall(ctx context.Context, session *farmbotclient.Session, target plotTarget, harvested *uint32) func(string, string) error {
	return func(operationID, requestID string) error {
		result, err := session.Harvest(ctx, operationID, requestID, &publicpb.HarvestRequest{
			PlotId:              target.plotID,
			TargetFarmId:        target.farmID,
			CropCycleId:         target.cropCycleID,
			ExpectedPlotVersion: target.plotVersion,
		})
		if err != nil {
			return err
		}
		if harvested != nil {
			*harvested = harvestedQuantity(result)
		}
		return nil
	}
}

func (executor *Executor) harvestAdmissionCall(ctx context.Context, session *farmbotclient.Session, target plotTarget, harvested *uint32, pending *bool) func(string, string) error {
	return func(operationID, requestID string) error {
		result, err := session.HarvestAdmission(ctx, operationID, requestID, &publicpb.HarvestRequest{
			PlotId:              target.plotID,
			TargetFarmId:        target.farmID,
			CropCycleId:         target.cropCycleID,
			ExpectedPlotVersion: target.plotVersion,
		})
		if err != nil {
			return err
		}
		if result != nil && result.GetStatus() == commonpb.CommandStatus_PROCESSING {
			if pending != nil {
				*pending = true
			}
			return nil
		}
		if harvested != nil {
			*harvested = harvestedQuantity(result)
		}
		return nil
	}
}

// harvestedQuantity reads the yield out of a harvest outcome. An outcome that
// cannot be decoded yields zero, which skips the sell rather than failing the
// bot: the harvest itself already succeeded.
func harvestedQuantity(result *farmbotclient.CommandResult) uint32 {
	if result == nil {
		return 0
	}
	var outcome publicpb.HarvestResult
	if err := proto.Unmarshal(result.GetPayload(), &outcome); err != nil {
		return 0
	}
	return outcome.GetHarvestedQuantity()
}

func (executor *Executor) sellCall(ctx context.Context, session *farmbotclient.Session, count uint32) func(string, string) error {
	return func(operationID, requestID string) error {
		_, err := session.Sell(ctx, operationID, requestID, &publicpb.SellRequest{
			ItemId: executor.options.ProduceItemID,
			Count:  int64(count),
		})
		return err
	}
}

// The single-action scenarios keep their original ordinals so a replay with
// the same run, bot and action still derives the same operation_id.
func (executor *Executor) plant(ctx context.Context, session *farmbotclient.Session, spec livebot.BotSpec, observer livebot.Observer, target plotTarget) error {
	if err := executor.action(ctx, session, spec, observer, "buy", 0, executor.buyCall(ctx, session, 1)); err != nil {
		return err
	}
	return executor.action(ctx, session, spec, observer, "plant", 1, executor.plantCall(ctx, session, target))
}

func (executor *Executor) care(ctx context.Context, session *farmbotclient.Session, spec livebot.BotSpec, observer livebot.Observer, target plotTarget) error {
	return executor.action(ctx, session, spec, observer, "care", 0, executor.careCall(ctx, session, target))
}

func (executor *Executor) theft(ctx context.Context, session *farmbotclient.Session, spec livebot.BotSpec, observer livebot.Observer, target plotTarget) error {
	return executor.action(ctx, session, spec, observer, "theft", 0, executor.theftCall(ctx, session, target))
}

func (executor *Executor) harvest(ctx context.Context, session *farmbotclient.Session, spec livebot.BotSpec, observer livebot.Observer, target plotTarget) error {
	return executor.harvestThenSell(ctx, session, spec, observer, 0, target)
}

// Each online iteration carries its own ordinal, so a repeated action is a
// new operation rather than an idempotent replay of the previous one.
func (executor *Executor) plantAt(ctx context.Context, session *farmbotclient.Session, spec livebot.BotSpec, observer livebot.Observer,
	ordinal int, target plotTarget, stock *seedStock, reservations plantReservations) error {
	if stock == nil || !stock.has() {
		buyCount := int32(1)
		if stock != nil {
			buyCount = executor.options.SeedBuyCount
		}
		if err := executor.action(ctx, session, spec, observer, "buy", ordinal, executor.buyCall(ctx, session, buyCount)); err != nil {
			return err
		}
		if stock != nil {
			stock.credit(int64(buyCount))
		}
	}
	if reservations == nil {
		if err := executor.action(ctx, session, spec, observer, "plant", ordinal, executor.plantCall(ctx, session, target)); err != nil {
			return err
		}
		if stock != nil {
			stock.consume()
		}
		return nil
	}

	operationID, requestID := DeterministicIDs(spec.RunID, spec.BotID, "plant", ordinal)
	started := time.Now()
	result, err := session.PlantAdmission(ctx, operationID, requestID, &publicpb.PlantRequest{
		PlotId:              target.plotID,
		CropDefinitionId:    uint32(executor.options.CropDefinitionID),
		TargetFarmId:        target.farmID,
		ExpectedPlotVersion: target.plotVersion,
	})
	elapsed := time.Since(started)
	if err != nil {
		return err
	}
	if result == nil {
		return errors.New("livefarmbot: Plant admission returned no result")
	}
	// Reserve on both immediate success and PROCESSING. The command reached a
	// durable identity; until FarmChange advances the plot, retrying this old
	// version is never useful and can only create a false business rejection.
	processing := result.GetStatus() == commonpb.CommandStatus_PROCESSING
	reservations.reserve(target, processing)
	if stock != nil {
		stock.consume()
	}
	if processing {
		observer.Phase(livebot.PhaseActionPrefix+"plant.processing", elapsed)
		return errDurableOperationProcessing
	}
	observer.Action("plant", elapsed)
	observer.Phase(livebot.PhaseActionPrefix+"plant", elapsed)
	return nil
}

func (executor *Executor) careAt(ctx context.Context, session *farmbotclient.Session, spec livebot.BotSpec, observer livebot.Observer, ordinal int, target plotTarget) error {
	return executor.action(ctx, session, spec, observer, "care", ordinal, executor.careCall(ctx, session, target))
}

func (executor *Executor) theftAt(ctx context.Context, session *farmbotclient.Session, spec livebot.BotSpec, observer livebot.Observer, ordinal int, target plotTarget) error {
	return executor.action(ctx, session, spec, observer, "theft", ordinal, executor.theftCall(ctx, session, target))
}

func (executor *Executor) harvestAt(ctx context.Context, session *farmbotclient.Session, spec livebot.BotSpec, observer livebot.Observer, ordinal int, target plotTarget, produce *produceStock) error {
	var harvested uint32
	var pending bool
	if err := executor.action(ctx, session, spec, observer, "harvest", ordinal,
		executor.harvestAdmissionCall(ctx, session, target, &harvested, &pending)); err != nil {
		return err
	}
	if produce != nil {
		if pending {
			produce.notePendingHarvest(target.plotID)
		} else {
			produce.noteHarvest(target.plotID, harvested)
		}
	}
	return nil
}

func (executor *Executor) sellAt(ctx context.Context, session *farmbotclient.Session, spec livebot.BotSpec, observer livebot.Observer, ordinal int, target plotTarget, produce *produceStock) error {
	err := executor.action(ctx, session, spec, observer, "sell", ordinal,
		executor.sellCall(ctx, session, target.sellCount))
	if err != nil {
		if produce != nil {
			produce.requestRefresh()
		}
		return err
	}
	if produce != nil {
		produce.sold(target.sellCount)
	}
	return nil
}

// harvestThenSell is kept for the single-shot functional harvest scenario.
// Sustained runs use harvestAt followed by a later inventory-visible SELL
// turn, so they never issue HARVEST and SELL in one cadence turn.
func (executor *Executor) harvestThenSell(ctx context.Context, session *farmbotclient.Session, spec livebot.BotSpec, observer livebot.Observer, ordinal int, target plotTarget) error {
	var harvested uint32
	if err := executor.action(ctx, session, spec, observer, "harvest", ordinal,
		executor.harvestCall(ctx, session, target, &harvested)); err != nil {
		return err
	}
	if harvested == 0 {
		return nil
	}
	if err := executor.waitForHarvestSettlement(ctx, session, spec, ordinal, harvested); err != nil {
		return err
	}
	return executor.action(ctx, session, spec, observer, "sell", ordinal,
		executor.sellCall(ctx, session, harvested))
}

// waitForHarvestSettlement verifies the Player projection instead of assuming
// that a fixed delay was enough. Harvest commits Farm first and the Worker
// grants the produce to Player from a durable SourceOrder, so SELL is only
// legal once the required quantity is visible in the authenticated inventory.
func (executor *Executor) waitForHarvestSettlement(
	ctx context.Context,
	session *farmbotclient.Session,
	spec livebot.BotSpec,
	ordinal int,
	required uint32,
) error {
	deadline := time.Now().Add(harvestSettlementProbeWait)
	for attempt := 0; ; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		operationID, requestID := DeterministicIDs(
			spec.RunID, spec.BotID, "harvest-settlement", ordinal*1000+attempt,
		)
		result, err := session.GetPlayerBootstrap(ctx, operationID, requestID)
		if err == nil && inventoryItemCount(decodeBootstrapView(result), executor.options.ProduceItemID) >= int64(required) {
			return nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return errors.New("harvest reward did not become visible before SELL")
		}
		wait := harvestSettlementProbeEvery
		if wait > remaining {
			wait = remaining
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func inventoryItemCount(view *publicpb.GetPlayerBootstrapResult, itemID int32) int64 {
	if view == nil || itemID == 0 {
		return 0
	}
	var count int64
	for _, item := range view.GetInventory().GetItems() {
		if item != nil && item.GetItemId() == itemID && item.GetCount() > 0 {
			count += item.GetCount()
		}
	}
	return count
}

func (executor *Executor) action(ctx context.Context, session *farmbotclient.Session, spec livebot.BotSpec, observer livebot.Observer, action string, ordinal int, call func(string, string) error) error {
	elapsed, err := executor.issue(spec, action, ordinal, call)
	if err != nil {
		return err
	}
	observer.Action(action, elapsed)
	observer.Phase(livebot.PhaseActionPrefix+action, elapsed)
	return nil
}

// warmupAction issues a command that sets a bot up rather than one the plan
// asked for. It reports a phase and nothing else: counting warmup as an action
// would credit the run with load it never planned, and folding its latency
// into the run distribution would let a startup burst speak for the steady
// state.
func (executor *Executor) warmupAction(ctx context.Context, session *farmbotclient.Session, spec livebot.BotSpec, observer livebot.Observer, action string, ordinal int, call func(string, string) error) error {
	elapsed, err := executor.issue(spec, action, ordinal, call)
	if err != nil {
		return err
	}
	observer.Phase(livebot.PhaseWarmupPrefix+action, elapsed)
	return nil
}

func (executor *Executor) issue(spec livebot.BotSpec, action string, ordinal int, call func(string, string) error) (time.Duration, error) {
	operationID, requestID := DeterministicIDs(spec.RunID, spec.BotID, action, ordinal)
	start := time.Now()
	if err := call(operationID, requestID); err != nil {
		return 0, err
	}
	return time.Since(start), nil
}

// DeterministicIDs returns printable ASCII operation/request ids that are
// stable for the same run, bot, action, and ordinal. Both are at most 64
// bytes and never contain credential material.
func DeterministicIDs(runID, botID, action string, ordinal int) (string, string) {
	key := strings.Join([]string{runID, botID, action, strconv.Itoa(ordinal)}, "\x00")
	digest := sha256.Sum256([]byte(key))
	encoded := hex.EncodeToString(digest[:])
	return "op-" + encoded[:60], "req-" + encoded[:59]
}

func failedResult(spec livebot.BotSpec, ctx context.Context, reason string) livebot.BotResult {
	status := livebot.BotFailed
	if ctx != nil && ctx.Err() != nil {
		status = livebot.BotCancelled
		reason = "run cancelled"
	}
	return livebot.BotResult{
		BotID:    spec.BotID,
		PlayerID: spec.PlayerID,
		Index:    spec.Index,
		Status:   status,
		Error:    reason,
	}
}
