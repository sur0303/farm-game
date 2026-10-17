package task

import (
	"errors"
	"fmt"
	"strings"
	"time"

	// Alpine runtime images have no /usr/share/zoneinfo. Embed IANA data so
	// DefaultGameTimezone ("Asia/Shanghai") resolves in player-svr.
	_ "time/tzdata"
)

// Sentinel errors for fail-closed paths. A pod that cannot resolve the
// current period assignment or a required snapshot must surface a local
// Loading/Fail-Closed state instead of inventing a version (Q34-07/Q34-08
// degradation rules).
var (
	ErrAssignmentUnavailable  = errors.New("task: daily assignment unavailable for current period")
	ErrSnapshotUnavailable    = errors.New("task: daily config snapshot unavailable")
	ErrGrowthChainUnavailable = errors.New("task: growth chain unavailable")
	ErrConfigHashMismatch     = errors.New("task: assignment config payload hash mismatch")
	ErrFactFromFuture         = errors.New("task: fact occurred_at is ahead of the current period")
	ErrChainIncompatible      = errors.New("task: published growth chain no longer contains the active cursor")
	ErrInvalidArgument        = errors.New("task: invalid argument")
)

// Domain is the pure TaskDomain calculator. It holds no player state: all
// durable state lives in DomainState and every method is a deterministic
// transition over it. The game timezone is configured once per domain;
// config snapshots must declare the same timezone (Q34-02).
type Domain struct {
	repo     Repository
	loc      *time.Location
	timezone string
}

type validatingRepository interface {
	Validate() error
}

// NewDomain builds a Domain for the configured game timezone. Contracts
// freeze P0 to Asia/Shanghai; a DST-observing timezone is equally valid as
// long as the whole deployment agrees on one value.
func NewDomain(repo Repository, gameTimezone string) (*Domain, error) {
	if repo == nil {
		return nil, fmt.Errorf("%w: nil repository", ErrInvalidArgument)
	}
	if gameTimezone == "" {
		gameTimezone = DefaultGameTimezone
	}
	loc, err := time.LoadLocation(gameTimezone)
	if err != nil {
		return nil, fmt.Errorf("%w: unknown game timezone %q", ErrInvalidArgument, gameTimezone)
	}
	if validator, ok := repo.(validatingRepository); ok {
		if err := validator.Validate(); err != nil {
			return nil, fmt.Errorf("%w: repository validation failed: %v", ErrInvalidArgument, err)
		}
	}
	return &Domain{repo: repo, loc: loc, timezone: gameTimezone}, nil
}

// GameTimezone returns the configured game timezone name.
func (d *Domain) GameTimezone() string { return d.timezone }

// kindSlot fixes the display order of the three daily tasks.
func kindSlot(kind DailyKind) int {
	switch kind {
	case DailyLogin:
		return 0
	case DailyFriendVisit:
		return 1
	case DailyFriendInteract:
		return 2
	default:
		return -1
	}
}

// finish bumps task_state_version exactly once when the call changed the
// domain, and realigns claimable_count with the actual CLAIMABLE set
// (Q34-08). Calls that only read state or fast-path NO_CHANGE never bump.
func (d *Domain) finish(st *DomainState, changed bool) {
	if !changed {
		return
	}
	st.TaskStateVersion++
	var claimable int32
	for i := range st.DailyTasks {
		if st.DailyTasks[i].State == StateClaimable {
			claimable++
		}
	}
	if st.CurrentGrowth != nil && st.CurrentGrowth.State == StateClaimable {
		claimable++
	}
	st.ClaimableCount = claimable
}

// ensure performs lazy daily period switch and lazy growth activation
// without bumping the version; the public entry point that called it owns
// the single bump for the whole mutation batch. The fail-closed daily
// switch runs before growth activation so the common config outage leaves
// the state untouched; on any error the caller must discard the candidate
// state and must not commit it.
func (d *Domain) ensure(st *DomainState, nowMs int64) (bool, error) {
	periodID := DailyPeriodID(nowMs, d.loc)
	if st.DailyPeriodID != periodID {
		// Lazy switch: the previous period's unclaimed daily rewards
		// expire and its progress is discarded. There is no catch-up for
		// skipped days and no cross-day claim grace (Q34-02).
		assignment, ok := d.repo.AssignmentForPeriod(periodID)
		if !ok {
			return false, fmt.Errorf("%w: %s", ErrAssignmentUnavailable, periodID)
		}
		cfg, ok := d.repo.DailySnapshot(assignment.ConfigVersion)
		if !ok {
			return false, fmt.Errorf("%w: version %d", ErrSnapshotUnavailable, assignment.ConfigVersion)
		}
		if cfg.PayloadHash != assignment.ConfigPayloadHash {
			return false, fmt.Errorf("%w: period %s version %d", ErrConfigHashMismatch, periodID, assignment.ConfigVersion)
		}
		if err := cfg.Validate(d.timezone); err != nil {
			return false, err
		}
		var next [DailySlotCount]Instance
		for _, def := range cfg.Daily {
			next[kindSlot(def.Kind)] = Instance{
				InstanceID:    DailyInstanceID(periodID, def.TaskID),
				TaskID:        def.TaskID,
				Category:      CategoryDaily,
				Kind:          def.Kind,
				ConfigVersion: cfg.Version,
				PeriodID:      periodID,
				Target:        1,
				State:         StateInProgress,
				Reward:        def.Reward,
				RewardHash:    rewardHash(def.Reward),
			}
		}
		// Stage the complete lazy switch before publishing it to the caller's
		// state. Growth configuration is an independent dependency; if it is
		// unavailable, a half-switched daily period must not leak from this
		// pure transition even though the authority layer would also discard
		// the candidate aggregate on error.
		candidate := *st
		if st.CurrentGrowth != nil {
			growth := *st.CurrentGrowth
			candidate.CurrentGrowth = &growth
		}
		candidate.DailyTasks = next
		candidate.DailyPeriodID = periodID
		candidate.DailyConfigVersion = cfg.Version
		if _, err := d.ensureGrowth(&candidate); err != nil {
			return false, err
		}
		*st = candidate
		return true, nil
	}
	return d.ensureGrowth(st)
}

// ensureGrowth lazily activates the chain entry under the cursor and
// immediately evaluates it against the permanent cumulative indicators. An
// already-satisfied new task becomes CLAIMABLE at once but is never
// auto-claimed (Q34-08).
func (d *Domain) ensureGrowth(st *DomainState) (bool, error) {
	if st.GrowthActive {
		return false, nil
	}
	chain, ok := d.repo.LatestGrowthChain()
	if !ok {
		return false, ErrGrowthChainUnavailable
	}
	if err := chain.Validate(); err != nil {
		return false, err
	}
	if st.GrowthCursor >= len(chain.Definitions) {
		return false, nil // chain ended; a later publication may extend it
	}
	def := chain.Definitions[st.GrowthCursor]
	inst := &Instance{
		InstanceID:        GrowthInstanceID(def.TaskID, def.DefinitionVersion),
		TaskID:            def.TaskID,
		Category:          CategoryGrowth,
		Metric:            def.Metric,
		DefinitionVersion: def.DefinitionVersion,
		ConfigVersion:     chain.Version,
		Target:            def.Target,
		State:             StateInProgress,
		Reward:            def.Reward,
		RewardHash:        rewardHash(def.Reward),
	}
	inst.Progress = d.metricValue(st, def.Metric)
	if inst.Progress >= inst.Target {
		inst.State = StateClaimable
	}
	st.CurrentGrowth = inst
	st.GrowthActive = true
	st.GrowthChainVersion = chain.Version
	return true, nil
}

func (d *Domain) metricValue(st *DomainState, metric GrowthMetric) int64 {
	switch metric {
	case GrowthMetricTotalHarvest:
		return st.TotalHarvestCount
	case GrowthMetricHighestLevel:
		return int64(st.HighestLevel)
	default:
		return 0
	}
}

// refreshGrowth re-evaluates the current growth task after a cumulative
// indicator moved. The metric is frozen on the instance at activation, so
// no config lookup is needed. Progress only advances while the task is
// IN_PROGRESS; a CLAIMABLE/CLAIMED task never churns on later facts
// (Q34-05).
func (d *Domain) refreshGrowth(st *DomainState, metric GrowthMetric) bool {
	inst := st.CurrentGrowth
	if inst == nil || inst.State != StateInProgress || inst.Metric != metric {
		return false
	}
	value := d.metricValue(st, metric)
	if value == inst.Progress {
		return false
	}
	inst.Progress = value
	if inst.Progress >= inst.Target {
		inst.State = StateClaimable
	}
	return true
}

// EnsureDailyPeriod exposes the lazy period switch for write paths such as
// HTTP EnsureOwnerInitialized and GET_TASK_LIST (Q34-02). GET_PLAYER_BOOTSTRAP
// is a home-page read and must not persist period changes. It reports whether
// the domain state changed.
func (d *Domain) EnsureDailyPeriod(st *DomainState, nowMs int64) (bool, error) {
	changed, err := d.ensure(st, nowMs)
	if err != nil {
		return false, err
	}
	d.finish(st, changed)
	return changed, nil
}

// RecordLogin consumes the daily-login fact: the first trusted PlayerActor
// turn of the current period (bootstrap, task query or any business
// command). Password checks, ticket issuance and bare connection setup
// never reach this method (Q34-03).
func (d *Domain) RecordLogin(st *DomainState, nowMs int64) (FactResult, error) {
	changed, err := d.ensure(st, nowMs)
	if err != nil {
		return FactResult{}, err
	}
	login := &st.DailyTasks[kindSlot(DailyLogin)]
	if login.State != StateInProgress {
		d.finish(st, changed)
		return FactResult{Status: FactNoChange, InstanceID: login.InstanceID, StateChanged: changed}, nil
	}
	login.State = StateClaimable
	login.Progress = 1
	login.FirstWitnessID = "trusted-turn"
	d.finish(st, true)
	return FactResult{Status: FactApplied, InstanceID: login.InstanceID, StateChanged: true}, nil
}

// RecordFriendVisit consumes the CONFIRM_FRIEND_VISIT commit: the gate has
// proven an authorized friend subscription and the PlayerActor reliably
// committed the confirmation. A second confirmation in the same period is
// a persistent NO_CHANGE/ALREADY_SATISFIED, never a conflict (Q34-05).
//
// Caller prerequisite (contract 31 §11.7): the envelope operation_id of
// CONFIRM_FRIEND_VISIT was deduplicated by the unified operation receipt
// layer; same-operation replay never reaches the domain. A new
// operation_id for an already-satisfied task reaches here and gets the
// persistent NO_CHANGE.
func (d *Domain) RecordFriendVisit(st *DomainState, witnessID string, nowMs int64) (FactResult, error) {
	changed, err := d.ensure(st, nowMs)
	if err != nil {
		return FactResult{}, err
	}
	visit := &st.DailyTasks[kindSlot(DailyFriendVisit)]
	if visit.State != StateInProgress {
		d.finish(st, changed)
		return FactResult{Status: FactNoChange, InstanceID: visit.InstanceID, StateChanged: changed}, nil
	}
	visit.State = StateClaimable
	visit.Progress = 1
	visit.FirstWitnessID = witnessID
	d.finish(st, true)
	return FactResult{Status: FactApplied, InstanceID: visit.InstanceID, StateChanged: true}, nil
}

// RecordFriendInteraction consumes one accepted friend care (FRIEND
// context only) or one accepted theft attempt — including a theft fully
// intercepted by a pet, which still proves the interaction (Q34-03). The
// participant already proves a legal visit, so the same batch also
// satisfies the visit task when it is still open. occurredAtMs is the
// trusted time captured by the source authority commit: a late delivery
// whose period already expired is IGNORED_EXPIRED and never pollutes the
// new period.
//
// Caller prerequisite (contract 31 §11.7): the APPLY_CARE_REWARD or
// SETTLE_THEFT_VISITOR Participant Receipt was checked by the PlayerActor
// before this call and is committed together with any task change in the
// same Authority Commit; participant replay never reaches the domain.
func (d *Domain) RecordFriendInteraction(st *DomainState, witnessID string, occurredAtMs, nowMs int64) (FactResult, error) {
	if occurredAtMs <= 0 {
		return FactResult{}, fmt.Errorf("%w: occurred_at_ms required", ErrInvalidArgument)
	}
	changed, err := d.ensure(st, nowMs)
	if err != nil {
		return FactResult{}, err
	}
	factPeriod := periodOfFact(occurredAtMs, d.loc)
	if factPeriod > st.DailyPeriodID {
		return FactResult{}, fmt.Errorf("%w: %s > %s", ErrFactFromFuture, factPeriod, st.DailyPeriodID)
	}
	if factPeriod < st.DailyPeriodID {
		d.finish(st, changed)
		return FactResult{Status: FactIgnoredExpired, StateChanged: changed}, nil
	}
	interact := &st.DailyTasks[kindSlot(DailyFriendInteract)]
	if interact.State != StateInProgress {
		d.finish(st, changed)
		return FactResult{Status: FactNoChange, InstanceID: interact.InstanceID, StateChanged: changed}, nil
	}
	interact.State = StateClaimable
	interact.Progress = 1
	interact.FirstWitnessID = witnessID
	visit := &st.DailyTasks[kindSlot(DailyFriendVisit)]
	if visit.State == StateInProgress {
		visit.State = StateClaimable
		visit.Progress = 1
		visit.FirstWitnessID = witnessID
	}
	d.finish(st, true)
	return FactResult{Status: FactApplied, InstanceID: interact.InstanceID, StateChanged: true}, nil
}

// RecordHarvest consumes the permanent harvest counter from the
// GRANT_HARVEST_REWARD participant: count is the number of CropCycles
// successfully ended by the batch, not the number of client commands
// (Q34-03). There is no daily harvest task in P0, so the cumulative
// counter applies regardless of the occurrence period (Q34-05).
//
// Caller prerequisite (contract 31 §11.7): the PlayerActor has already
// verified through the unified Participant Receipt that this
// GRANT_HARVEST_REWARD invocation was never applied, and commits the
// source ParticipantReceipt together with this task change in one
// Authority Commit. Replay of the same participant must be short-
// circuited there; the pure domain keeps no source id set.
func (d *Domain) RecordHarvest(st *DomainState, count int64, nowMs int64) (FactResult, error) {
	if count <= 0 {
		return FactResult{}, fmt.Errorf("%w: harvest count must be positive", ErrInvalidArgument)
	}
	if _, err := d.ensure(st, nowMs); err != nil {
		return FactResult{}, err
	}
	st.TotalHarvestCount += count
	d.refreshGrowth(st, GrowthMetricTotalHarvest)
	d.finish(st, true)
	return FactResult{Status: FactApplied, StateChanged: true}, nil
}

// RecordLevel consumes the committed level_after fact. Level is a
// state-type indicator: progress moves with max(progress, level_after),
// so out-of-order delivery never regresses it, and a player who already
// meets a newly published threshold becomes CLAIMABLE at lazy evaluation
// without leveling again (Q34-03/Q34-07).
func (d *Domain) RecordLevel(st *DomainState, levelAfter int32, nowMs int64) (FactResult, error) {
	if levelAfter <= 0 {
		return FactResult{}, fmt.Errorf("%w: level must be positive", ErrInvalidArgument)
	}
	changed, err := d.ensure(st, nowMs)
	if err != nil {
		return FactResult{}, err
	}
	if levelAfter <= st.HighestLevel {
		d.finish(st, changed)
		return FactResult{Status: FactNoChange, StateChanged: changed}, nil
	}
	st.HighestLevel = levelAfter
	d.refreshGrowth(st, GrowthMetricHighestLevel)
	d.finish(st, true)
	return FactResult{Status: FactApplied, StateChanged: true}, nil
}

// Claim decides one ClaimTask operation inside a single turn (Q34-06).
//
// Caller prerequisite (contract 31 §11.7): the PlayerActor has already run
// the unified operation receipt precheck (receipt:op:<operation_digest>):
// same-operation replay with a matching payload hash is answered from the
// StoredOperationOutcome without entering the domain, and a payload hash
// mismatch is rejected as IDEMPOTENCY_CONFLICT there. The domain therefore
// never sees an operation_id and stores no claim receipt of its own; the
// deterministic ClaimOutcome gives the caller everything needed to build
// the ClaimReceipt/StoredOperationOutcome and to apply assets in the same
// authority commit.
//
// A different operation re-claiming the same instance is decided from
// task truth: CLAIMED instances return TASK_ALREADY_CLAIMED with an empty
// reward and leave DomainState untouched.
func (d *Domain) Claim(st *DomainState, instanceID string, nowMs int64) (ClaimOutcome, error) {
	if instanceID == "" {
		return ClaimOutcome{}, fmt.Errorf("%w: task_instance_id required", ErrInvalidArgument)
	}
	changed, err := d.ensure(st, nowMs)
	if err != nil {
		return ClaimOutcome{}, err
	}
	inst := d.findLiveInstance(st, instanceID)
	if inst == nil {
		status, err := d.classifyDeadInstance(st, instanceID)
		if err != nil {
			return ClaimOutcome{}, err
		}
		// Rejected claims never mutate task truth: no version bump, no
		// red-point generation, no model:task write. The caller persists
		// the terminal outcome in its own StoredOperationOutcome.
		d.finish(st, changed)
		return ClaimOutcome{Status: status, InstanceID: instanceID, StateChanged: changed, Summary: d.Summary(st)}, nil
	}
	switch inst.State {
	case StateClaimed:
		d.finish(st, changed)
		return ClaimOutcome{Status: ClaimAlreadyClaimed, InstanceID: instanceID, StateChanged: changed, Summary: d.Summary(st)}, nil
	case StateInProgress:
		d.finish(st, changed)
		return ClaimOutcome{Status: ClaimNotCompleted, InstanceID: instanceID, StateChanged: changed, Summary: d.Summary(st)}, nil
	}

	outcome := ClaimOutcome{Status: ClaimSucceeded, InstanceID: instanceID, Reward: inst.Reward, StateChanged: true}
	var next *Instance
	if inst.Category == CategoryGrowth {
		// Fail closed before any mutation: advancing the cursor needs the
		// latest published chain and it must still contain the cursor.
		next, err = d.prepareGrowthAdvance(st, inst)
		if err != nil {
			return ClaimOutcome{}, err
		}
		if next != nil {
			view := viewOf(next)
			outcome.NextGrowth = &view
		}
	}
	inst.State = StateClaimed
	inst.ClaimedAtMs = nowMs
	if inst.Category == CategoryGrowth {
		st.GrowthCursor++
		st.CurrentGrowth = next
		st.GrowthActive = next != nil
		if next != nil {
			st.GrowthChainVersion = next.ConfigVersion
		}
	}
	d.finish(st, true)
	outcome.Summary = d.Summary(st)
	return outcome, nil
}

// classifyDeadInstance maps a non-live instance id to its terminal status.
// Daily instances of any other period are expired (no cross-day grace).
// Growth instances behind the cursor were claimed when the cursor passed
// them; anything else does not exist for this player. The published chain
// is required to be prefix-stable (append-only) relative to any activated
// cursor (Q34-07), which keeps the cursor comparison truthful.
func (d *Domain) classifyDeadInstance(st *DomainState, instanceID string) (ClaimStatus, error) {
	if strings.HasPrefix(instanceID, "daily/") {
		return ClaimInstanceExpired, nil
	}
	if taskID, ok := parseGrowthInstanceTaskID(instanceID); ok {
		chain, chainOK := d.repo.LatestGrowthChain()
		if !chainOK {
			return ClaimUnspecified, ErrGrowthChainUnavailable
		}
		for index, def := range chain.Definitions {
			if def.TaskID == taskID {
				if index < st.GrowthCursor {
					return ClaimAlreadyClaimed, nil
				}
				return ClaimInstanceNotFound, nil
			}
		}
	}
	return ClaimInstanceNotFound, nil
}

// parseGrowthInstanceTaskID extracts the task id from a
// "growth/<task_id>/<definition_version>" instance id. Task ids never
// contain '/' (enforced at config validation).
func parseGrowthInstanceTaskID(instanceID string) (string, bool) {
	rest, ok := strings.CutPrefix(instanceID, "growth/")
	if !ok {
		return "", false
	}
	slash := strings.LastIndexByte(rest, '/')
	if slash <= 0 || slash == len(rest)-1 {
		return "", false
	}
	return rest[:slash], true
}

// prepareGrowthAdvance validates the chain step and pre-activates the next
// definition without touching the state, so a config failure leaves the
// domain untouched (Q34-06 fail-closed rule). A nil next instance with a
// nil error means the chain ends after this claim.
func (d *Domain) prepareGrowthAdvance(st *DomainState, inst *Instance) (*Instance, error) {
	chain, ok := d.repo.LatestGrowthChain()
	if !ok {
		return nil, ErrGrowthChainUnavailable
	}
	if err := chain.Validate(); err != nil {
		return nil, err
	}
	if st.GrowthCursor >= len(chain.Definitions) || chain.Definitions[st.GrowthCursor].TaskID != inst.TaskID {
		return nil, fmt.Errorf("%w: cursor %d task %s", ErrChainIncompatible, st.GrowthCursor, inst.TaskID)
	}
	nextIdx := st.GrowthCursor + 1
	if nextIdx >= len(chain.Definitions) {
		return nil, nil
	}
	def := chain.Definitions[nextIdx]
	next := &Instance{
		InstanceID:        GrowthInstanceID(def.TaskID, def.DefinitionVersion),
		TaskID:            def.TaskID,
		Category:          CategoryGrowth,
		Metric:            def.Metric,
		DefinitionVersion: def.DefinitionVersion,
		ConfigVersion:     chain.Version,
		Target:            def.Target,
		State:             StateInProgress,
		Reward:            def.Reward,
		RewardHash:        rewardHash(def.Reward),
	}
	next.Progress = d.metricValue(st, def.Metric)
	if next.Progress >= next.Target {
		// Already satisfied by permanent indicators: immediately claimable,
		// but never recursively auto-claimed (Q34-08).
		next.State = StateClaimable
	}
	return next, nil
}

func (d *Domain) findLiveInstance(st *DomainState, instanceID string) *Instance {
	for i := range st.DailyTasks {
		if st.DailyTasks[i].InstanceID == instanceID {
			return &st.DailyTasks[i]
		}
	}
	if st.CurrentGrowth != nil && st.CurrentGrowth.InstanceID == instanceID {
		return st.CurrentGrowth
	}
	return nil
}

// Summary returns the authoritative red-point source fields. The
// projection layer may only rebuild a copy of these values; it never
// participates in adjudication (Q34-08).
func (d *Domain) Summary(st *DomainState) Summary {
	summary := Summary{
		DailyPeriodID:    st.DailyPeriodID,
		TaskStateVersion: st.TaskStateVersion,
		ClaimableCount:   st.ClaimableCount,
	}
	if st.DailyPeriodID != "" {
		if dayStart, err := time.ParseInLocation("20060102", st.DailyPeriodID, d.loc); err == nil {
			summary.NextResetAtMs = dayStart.AddDate(0, 0, 1).UnixMilli()
		}
	}
	return summary
}

// ListView lazily aligns the period and returns the fixed P0 list: three
// daily tasks plus the current growth task (nil once the chain ended).
func (d *Domain) ListView(st *DomainState, nowMs int64) (ListView, error) {
	changed, err := d.ensure(st, nowMs)
	if err != nil {
		return ListView{}, err
	}
	d.finish(st, changed)
	view := ListView{
		Summary:        d.Summary(st),
		DailyConfigVer: st.DailyConfigVersion,
	}
	for i := range st.DailyTasks {
		view.DailyTasks[i] = viewOf(&st.DailyTasks[i])
	}
	if st.CurrentGrowth != nil {
		growth := viewOf(st.CurrentGrowth)
		view.CurrentGrowth = &growth
	}
	return view, nil
}
