package livebot

import (
	"strings"
	"time"
)

// Phase names are a closed vocabulary. Keeping them fixed is what lets a run
// summary carry per-phase percentiles without an adapter growing unbounded
// label cardinality.
const (
	PhaseLogin      = "login"
	PhaseHello      = "hello"
	PhaseJoin       = "join"
	PhaseAdmission  = "admission"
	PhaseResolution = "resolution"
	PhaseThink      = "think"
	PhaseSession    = "session"
	// PhaseArrival is how late each bot started against the arrival schedule
	// the run asked for. It is the scheduler's own lateness, not a server
	// measurement: a driver that cannot launch on time is not applying the
	// load its configuration claims.
	PhaseArrival = "arrival"

	// PhaseActionPrefix is followed by a sanitized action name, so an action
	// keeps its own distribution alongside the aggregate ones above.
	PhaseActionPrefix = "action."

	// PhaseWarmupPrefix is followed by a sanitized action name and marks a
	// command that put a bot into its starting state. Warmup is reported so
	// its cost is visible, and kept out of the action counts and the run
	// latency so a startup burst cannot speak for the steady state.
	PhaseWarmupPrefix = "warmup."
)

var knownPhases = map[string]struct{}{
	PhaseLogin:      {},
	PhaseHello:      {},
	PhaseJoin:       {},
	PhaseAdmission:  {},
	PhaseResolution: {},
	PhaseThink:      {},
	PhaseSession:    {},
	PhaseArrival:    {},
}

// SafePhaseName maps an adapter-supplied phase onto the closed vocabulary.
func SafePhaseName(phase string) string {
	phase = strings.ToLower(strings.TrimSpace(phase))
	if _, known := knownPhases[phase]; known {
		return phase
	}
	if action, ok := strings.CutPrefix(phase, PhaseActionPrefix); ok {
		return PhaseActionPrefix + safeActionName(action)
	}
	if action, ok := strings.CutPrefix(phase, PhaseWarmupPrefix); ok {
		return PhaseWarmupPrefix + safeActionName(action)
	}
	return "redacted"
}

// ExecutionRealSandbox is the explicit RunView execution marker for a run
// whose scheduler and evidence producer really ran in the bounded sandbox.
const ExecutionRealSandbox = "REAL_SANDBOX"

// RunStatus is deliberately distinct for success, executor failure, and
// cancellation/timeout.
type RunStatus string

const (
	RunSucceeded RunStatus = "succeeded"
	RunFailed    RunStatus = "failed"
	RunCancelled RunStatus = "cancelled"
	RunRunning   RunStatus = "running"
)

// BotStatus is the terminal state returned by one Executor invocation.
type BotStatus string

const (
	BotSucceeded BotStatus = "succeeded"
	BotFailed    BotStatus = "failed"
	BotCancelled BotStatus = "cancelled"

	BotStatusSucceeded = BotSucceeded
	BotStatusFailed    = BotFailed
	BotStatusCancelled = BotCancelled
)

// BotResult is intentionally safe to expose as a result summary. Error is
// retained only in memory for an adapter/test and is never emitted as event
// text or fields.
type BotResult struct {
	BotID      string        `json:"bot_id"`
	PlayerID   uint64        `json:"player_id"`
	Index      int           `json:"index"`
	Status     BotStatus     `json:"status"`
	Attempted  bool          `json:"attempted"`
	Ready      bool          `json:"ready"`
	Reconnects int           `json:"reconnects"`
	Latency    time.Duration `json:"latency"`
	// Online is how long the bot held its connection, and Frames/Broadcasts
	// are what its connection actually drained while it was there. Applied
	// broadcasts were folded into the farm view instead of discarded, which
	// is the difference between a bot that paid for the fan-out around it and
	// one that only read past it.
	Online            time.Duration `json:"online"`
	Frames            uint64        `json:"frames"`
	Broadcasts        uint64        `json:"broadcasts"`
	BroadcastsApplied uint64        `json:"broadcasts_applied"`
	// OperationPushes counts the settlement notifications the server sent for
	// async commands. Zero across a whole run means the resolution latency
	// this run reports is the client's retry cadence, not the server's settle
	// time, and the async phases have to be read with that in mind.
	OperationPushes uint64 `json:"operation_pushes"`
	// ResultQueries counts the GET_OPERATION_RESULT commands the bot sent to
	// find out whether its async commands had settled. They are load too, and
	// a run whose queries dwarf its actions was mostly measuring its own
	// impatience.
	ResultQueries uint64 `json:"result_queries"`
	// Rejected counts server verdicts that refused a command on business
	// grounds, and Skipped counts iterations with no valid target. Neither is
	// a bot failure; both belong in the report.
	Rejected int `json:"rejected"`
	Skipped  int `json:"skipped"`
	// Processing counts durable admissions whose first verdict was PROCESSING;
	// they are delivered turns but never successful actions. Pending is the
	// subset without a terminal proof when the bot exits. ProjectionPending
	// also includes terminal successes whose final FarmChange had not arrived;
	// those are not durable pending operations.
	Processing        int `json:"processing"`
	Pending           int `json:"pending"`
	ProjectionPending int `json:"projection_pending"`
	// Planned is the number of turns this bot's configured cadence should have
	// produced while it was online, Turns is how many of them put a command on
	// the wire, and Timeouts counts commands it sent but never saw answered.
	// Together they are what makes a load figure auditable: a closed-loop bot
	// slows down when the server does, so an unreported shortfall reads as a
	// calm server.
	//
	// Turns is counted rather than reusing Actions because one turn is not one
	// command: planting first buys a seed and harvesting then sells the crop,
	// so a bot keeping perfect cadence issues more commands than the cadence
	// named. Actions remains the load the server saw; Turns is the load the
	// plan can be compared against.
	Planned  int    `json:"planned"`
	Turns    int    `json:"turns"`
	Timeouts int    `json:"timeouts"`
	Error    string `json:"-"`
}

// Terminal reports whether the result has one of the allowed terminal states.
func (result BotResult) Terminal() bool {
	switch result.Status {
	case BotSucceeded, BotFailed, BotCancelled:
		return true
	default:
		return false
	}
}

// LatencyHistogram is a bounded fixed-bucket histogram. It never allocates a
// bucket based on an input value, so an adapter cannot create unbounded
// cardinality through latency evidence.
type LatencyHistogram struct {
	BoundsMs []int64  `json:"bounds_ms"`
	Counts   []uint64 `json:"counts"`
	Count    uint64   `json:"count"`
	SumMs    uint64   `json:"sum_ms"`
	MinMs    uint64   `json:"min_ms"`
	MaxMs    uint64   `json:"max_ms"`
}

// The buckets are dense where a healthy interactive command lands so that an
// interpolated p95/p99 stays close to the real value.
var latencyBoundsMs = []int64{
	1, 2, 3, 5, 8, 12, 20, 30, 50, 75,
	100, 150, 200, 300, 500, 750, 1000, 1500, 2000, 3000, 5000, 10000,
}

// NewLatencyHistogram returns a ready-to-use bounded histogram.
func NewLatencyHistogram() LatencyHistogram {
	bounds := append([]int64(nil), latencyBoundsMs...)
	return LatencyHistogram{BoundsMs: bounds, Counts: make([]uint64, len(bounds)+1)}
}

func (histogram *LatencyHistogram) ensure() {
	if len(histogram.BoundsMs) != len(latencyBoundsMs) || len(histogram.Counts) != len(latencyBoundsMs)+1 {
		*histogram = NewLatencyHistogram()
	}
}

// Observe records positive latency in milliseconds. Values above the final
// bound go to the bounded overflow bucket.
func (histogram *LatencyHistogram) Observe(latency time.Duration) {
	if latency <= 0 {
		return
	}
	histogram.ensure()
	ms := uint64(latency / time.Millisecond)
	if ms == 0 {
		ms = 1
	}
	index := len(latencyBoundsMs)
	for candidate, bound := range latencyBoundsMs {
		if int64(ms) <= bound {
			index = candidate
			break
		}
	}
	histogram.Count++
	histogram.SumMs += ms
	if ms > histogram.MaxMs {
		histogram.MaxMs = ms
	}
	if histogram.MinMs == 0 || ms < histogram.MinMs {
		histogram.MinMs = ms
	}
	histogram.Counts[index]++
}

// Merge folds another histogram of the same shape into this one so per-bot
// samples can be aggregated into one run-level distribution.
func (histogram *LatencyHistogram) Merge(other LatencyHistogram) {
	if other.Count == 0 {
		return
	}
	histogram.ensure()
	if len(other.Counts) != len(histogram.Counts) {
		return
	}
	for index, count := range other.Counts {
		histogram.Counts[index] += count
	}
	histogram.Count += other.Count
	histogram.SumMs += other.SumMs
	if other.MaxMs > histogram.MaxMs {
		histogram.MaxMs = other.MaxMs
	}
	if histogram.MinMs == 0 || (other.MinMs != 0 && other.MinMs < histogram.MinMs) {
		histogram.MinMs = other.MinMs
	}
}

// Percentile estimates the q-quantile in milliseconds by interpolating inside
// the bucket that contains it. The estimate carries that bucket's width as
// its error bound; MinMs and MaxMs remain exact observations.
func (histogram LatencyHistogram) Percentile(q float64) float64 {
	if histogram.Count == 0 || len(histogram.Counts) != len(histogram.BoundsMs)+1 {
		return 0
	}
	if q <= 0 {
		return float64(histogram.MinMs)
	}
	if q >= 1 {
		return float64(histogram.MaxMs)
	}
	rank := q * float64(histogram.Count)
	cumulative := float64(0)
	for index, count := range histogram.Counts {
		if count == 0 {
			continue
		}
		if cumulative+float64(count) < rank {
			cumulative += float64(count)
			continue
		}
		lower := float64(0)
		if index > 0 {
			lower = float64(histogram.BoundsMs[index-1])
		}
		upper := float64(histogram.MaxMs)
		if index < len(histogram.BoundsMs) {
			upper = float64(histogram.BoundsMs[index])
		}
		if upper < lower {
			upper = lower
		}
		position := (rank - cumulative) / float64(count)
		estimate := lower + (upper-lower)*position
		if estimate < float64(histogram.MinMs) {
			estimate = float64(histogram.MinMs)
		}
		if estimate > float64(histogram.MaxMs) {
			estimate = float64(histogram.MaxMs)
		}
		return estimate
	}
	return float64(histogram.MaxMs)
}

// LatencySummary is the reporting view of a histogram. It exists so a run
// summary can carry percentiles without every consumer reimplementing the
// bucket interpolation.
type LatencySummary struct {
	Count uint64  `json:"count"`
	MinMs uint64  `json:"min_ms"`
	MaxMs uint64  `json:"max_ms"`
	AvgMs float64 `json:"avg_ms"`
	P50Ms float64 `json:"p50_ms"`
	P95Ms float64 `json:"p95_ms"`
	P99Ms float64 `json:"p99_ms"`
}

// Report derives the percentile view of this histogram.
func (histogram LatencyHistogram) Report() LatencySummary {
	report := LatencySummary{Count: histogram.Count, MinMs: histogram.MinMs, MaxMs: histogram.MaxMs}
	if histogram.Count == 0 {
		return report
	}
	report.AvgMs = float64(histogram.SumMs) / float64(histogram.Count)
	report.P50Ms = histogram.Percentile(0.50)
	report.P95Ms = histogram.Percentile(0.95)
	report.P99Ms = histogram.Percentile(0.99)
	return report
}

// Summary is a low-cardinality aggregate for one run. Per-player identity is
// kept in events/Results, not in this aggregate.
type Summary struct {
	Attempted int `json:"attempted"`
	Ready     int `json:"ready"`
	Succeeded int `json:"succeeded"`
	Failed    int `json:"failed"`
	Cancelled int `json:"cancelled"`
	Reconnect int `json:"reconnect"`
	// Actions counts every business command a bot completed, which is the
	// throughput denominator once bots stay online for many actions.
	Actions int `json:"actions"`
	// Rejected and Skipped keep server refusals and missing targets out of
	// the failure count while still making them visible. Processing is an
	// admitted durable operation, not a success; Pending remains unproven when
	// the client leaves.
	Rejected          int `json:"rejected"`
	Skipped           int `json:"skipped"`
	Processing        int `json:"processing"`
	Pending           int `json:"pending"`
	ProjectionPending int `json:"projection_pending"`
	// Planned, Turns and Timeouts complete the load account. Planned is the
	// turns the configured cadence asked for and Turns is how many of them
	// reached the server; Actions is the commands those turns cost.
	Planned    int                         `json:"planned"`
	Turns      int                         `json:"turns"`
	Timeouts   int                         `json:"timeouts"`
	OnlinePeak int                         `json:"online_peak"`
	Latency    LatencyHistogram            `json:"latency"`
	Phases     map[string]LatencyHistogram `json:"phases,omitempty"`
	Broadcasts uint64                      `json:"broadcasts_drained"`
	Applied    uint64                      `json:"broadcasts_applied"`
	// OperationPushes is zero when the server never notified a settlement, in
	// which case every async phase in this summary carries the client's retry
	// cadence rather than the server's own timing.
	OperationPushes uint64 `json:"operation_pushes"`
	// ResultQueries is the polling load. Added to Actions it is every command
	// the server had to answer.
	ResultQueries uint64 `json:"result_queries"`
	InboundFrame  uint64 `json:"inbound_frames"`
}

// MinDeliveryRatio is the share of the planned command load a run must
// actually deliver before its latency figures mean anything. Below it the run
// measured a load generator that could not keep up, not a server.
const MinDeliveryRatio = 0.98

// LoadAccount reconciles the load a run intended to place with the load it
// really placed. A closed-loop bot waits for each answer before its next
// think time, so a slow server silently reduces the offered load; without
// this reconciliation that shows up as flat latency instead of a shortfall.
type LoadAccount struct {
	// Planned and Sent are turns, the unit the cadence is expressed in.
	// Actions is the commands those turns cost, which is the load the server
	// actually saw and the figure a throughput number should quote.
	Planned           int `json:"planned"`
	Sent              int `json:"sent"`
	Unsent            int `json:"unsent"`
	Actions           int `json:"actions"`
	Rejected          int `json:"rejected"`
	Processing        int `json:"processing"`
	Pending           int `json:"pending"`
	ProjectionPending int `json:"projection_pending"`
	Timeouts          int `json:"timeouts"`
	Skipped           int `json:"skipped"`
	// Ratio is Sent/Planned. Qualified is false when the run delivered less
	// than MinDeliveryRatio of its plan, which invalidates the step.
	Ratio     float64 `json:"delivery_ratio"`
	Qualified bool    `json:"qualified"`
	// Planless runs have no configured cadence to compare against, so the
	// ratio and the gate carry no meaning.
	Planless bool `json:"planless"`
}

// Load derives the delivered-versus-planned account for this run.
func (summary Summary) Load() LoadAccount {
	account := LoadAccount{
		Planned:           summary.Planned,
		Actions:           summary.Actions,
		Rejected:          summary.Rejected,
		Processing:        summary.Processing,
		Pending:           summary.Pending,
		ProjectionPending: summary.ProjectionPending,
		Timeouts:          summary.Timeouts,
		Skipped:           summary.Skipped,
	}
	// A turn whose command was rejected or timed out still crossed the wire,
	// so it counts as delivered; only a skipped turn never became a request.
	account.Sent = summary.Turns
	if summary.Planned <= 0 {
		// With no cadence there is nothing to compare turns against, and the
		// commands are the only load figure left that means anything.
		account.Planless = true
		account.Sent = summary.Actions + summary.Rejected + summary.Processing + summary.Timeouts
		return account
	}
	if unsent := summary.Planned - account.Sent; unsent > 0 {
		account.Unsent = unsent
	}
	account.Ratio = float64(account.Sent) / float64(summary.Planned)
	account.Qualified = account.Ratio >= MinDeliveryRatio
	return account
}

// PhaseReports derives the percentile view of every recorded phase.
func (summary Summary) PhaseReports() map[string]LatencySummary {
	if len(summary.Phases) == 0 {
		return nil
	}
	reports := make(map[string]LatencySummary, len(summary.Phases))
	for phase, histogram := range summary.Phases {
		reports[phase] = histogram.Report()
	}
	return reports
}

// MergePhase folds one phase histogram into the summary.
func (summary *Summary) MergePhase(phase string, histogram LatencyHistogram) {
	if histogram.Count == 0 {
		return
	}
	if summary.Phases == nil {
		summary.Phases = make(map[string]LatencyHistogram, MaxPhaseCardinality)
	}
	existing, ok := summary.Phases[phase]
	if !ok {
		if len(summary.Phases) >= MaxPhaseCardinality {
			return
		}
		existing = NewLatencyHistogram()
	}
	existing.Merge(histogram)
	summary.Phases[phase] = existing
}

// Reconnects is a compatibility spelling for callers that prefer a plural
// counter. Runner keeps it equal to Reconnect.
func (summary Summary) Reconnects() int {
	return summary.Reconnect
}

// RunResult is the complete bounded result returned by Runner.Run. A runtime
// BotFailed is represented in Status/Summary; Err is reserved for invalid
// configuration or evidence-store failures.
type RunResult struct {
	RunID         string      `json:"run_id"`
	Status        RunStatus   `json:"status"`
	Execution     string      `json:"execution"`
	CapacityClaim bool        `json:"capacity_claim"`
	Summary       Summary     `json:"summary"`
	Bots          []BotResult `json:"bots"`
	StartedAt     time.Time   `json:"started_at"`
	FinishedAt    time.Time   `json:"finished_at"`
	Err           error       `json:"-"`
}

// Result and RunSummary are concise aliases for adapter-facing call sites.
type Result = RunResult
type RunSummary = Summary
