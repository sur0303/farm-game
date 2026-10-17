// Package livebot provides a bounded, in-process LIVE bot scheduler and its
// evidence producer. It never opens a network listener or claims capacity.
package livebot

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Environment is the only deployment context in which a live bot run may be
// started. Production and live environments are intentionally not values of
// this type.
type Environment string

const (
	EnvironmentLocal   Environment = "local"
	EnvironmentCI      Environment = "ci"
	EnvironmentSandbox Environment = "sandbox"

	// Short aliases keep call sites readable while retaining the explicit
	// Environment-prefixed names above.
	EnvLocal   = EnvironmentLocal
	EnvCI      = EnvironmentCI
	EnvSandbox = EnvironmentSandbox
)

const (
	// These limits are process-local safety bounds, not a capacity claim about
	// any game service.
	//
	// MaxPlayers stops a typo from launching an unbounded run; it is not the
	// number of bots one process can drive honestly. That number is lower and
	// has to be measured, because past it the driver's own scheduling delay
	// enters every latency it reports. Qualify the driver against a known-fast
	// server and use the qualified figure, not this ceiling.
	MaxPlayers         = 20000
	MaxArrivalRate     = 1000.0
	MaxDuration        = time.Hour
	MaxOnline          = MaxPlayers
	MaxRunIDLength     = 64
	MaxScenarioLength  = 64
	MaxActionLength    = 64
	MaxActionMixLength = 32

	// MaxPhaseCardinality bounds how many distinct latency phases one run may
	// report, so a misbehaving adapter cannot grow the summary without limit.
	MaxPhaseCardinality = 32

	// A bot may stay online for the whole run, so the session bounds reuse the
	// run duration ceiling. Think time and action count are bounded separately
	// to keep one bot's request rate reviewable.
	MaxSessionDuration = MaxDuration
	MaxThinkTime       = 5 * time.Minute
	MaxSessionActions  = 10000
)

var (
	identifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

	ErrInvalidSpec       = fmt.Errorf("invalid live run spec")
	ErrLiveStoreRequired = fmt.Errorf("livebot requires a LIVE event store")
)

// LiveRunSpec describes one bounded local/CI/sandbox run. The fields are
// scheduling and evidence inputs only; a run is explicitly not a capacity
// claim.
type LiveRunSpec struct {
	RunID       string        `json:"run_id"`
	Environment Environment   `json:"environment"`
	Players     int           `json:"players"`
	PlayerStart uint64        `json:"player_start"`
	Duration    time.Duration `json:"duration"`
	ArrivalRate float64       `json:"arrival_rate"`
	MaxOnline   int           `json:"max_online"`

	// SessionDuration is how long one bot stays connected and keeps acting.
	// Zero keeps the original one-action-then-disconnect behavior, which
	// measures arrival cost rather than sustained concurrency.
	SessionDuration time.Duration `json:"session_duration"`
	// ThinkTime is the pause between two actions of the same online bot.
	ThinkTime time.Duration `json:"think_time"`
	// SessionActions caps the actions one online bot performs. Zero means the
	// session is bounded by SessionDuration alone.
	SessionActions int `json:"session_actions"`

	Seed          uint64             `json:"seed"`
	Scenario      string             `json:"scenario,omitempty"`
	ScenarioID    string             `json:"scenario_id,omitempty"`
	ActionMix     map[string]float64 `json:"action_mix,omitempty"`
	Execution     string             `json:"execution,omitempty"`
	CapacityClaim bool               `json:"capacity_claim"`
}

// Sustained reports whether bots stay online instead of disconnecting after
// one action.
func (spec LiveRunSpec) Sustained() bool {
	return spec.SessionDuration > 0
}

// Validate checks all process-local bounds and rejects unsafe environments.
func (spec LiveRunSpec) Validate() error {
	_, err := spec.normalized()
	return err
}

func (spec LiveRunSpec) normalized() (LiveRunSpec, error) {
	spec.RunID = strings.TrimSpace(spec.RunID)
	if !validIdentifier(spec.RunID, MaxRunIDLength) {
		return LiveRunSpec{}, fmt.Errorf("%w: run_id must be 1-%d identifier characters", ErrInvalidSpec, MaxRunIDLength)
	}

	spec.Environment = Environment(strings.ToLower(strings.TrimSpace(string(spec.Environment))))
	switch spec.Environment {
	case EnvironmentLocal, EnvironmentCI, EnvironmentSandbox:
	default:
		return LiveRunSpec{}, fmt.Errorf("%w: environment must be local, ci, or sandbox; production/live are forbidden", ErrInvalidSpec)
	}

	if spec.Players <= 0 || spec.Players > MaxPlayers {
		return LiveRunSpec{}, fmt.Errorf("%w: players must be between 1 and %d", ErrInvalidSpec, MaxPlayers)
	}
	if spec.PlayerStart == 0 {
		return LiveRunSpec{}, fmt.Errorf("%w: player_start must be positive", ErrInvalidSpec)
	}
	if uint64(spec.Players-1) > ^uint64(0)-spec.PlayerStart {
		return LiveRunSpec{}, fmt.Errorf("%w: player ids overflow uint64", ErrInvalidSpec)
	}
	if spec.Duration <= 0 || spec.Duration > MaxDuration {
		return LiveRunSpec{}, fmt.Errorf("%w: duration must be greater than zero and at most %s", ErrInvalidSpec, MaxDuration)
	}
	if math.IsNaN(spec.ArrivalRate) || math.IsInf(spec.ArrivalRate, 0) || spec.ArrivalRate <= 0 || spec.ArrivalRate > MaxArrivalRate {
		return LiveRunSpec{}, fmt.Errorf("%w: arrival_rate must be greater than zero and at most %.0f", ErrInvalidSpec, MaxArrivalRate)
	}
	if spec.MaxOnline <= 0 || spec.MaxOnline > MaxOnline || spec.MaxOnline > spec.Players {
		return LiveRunSpec{}, fmt.Errorf("%w: max_online must be between 1 and min(players,%d)", ErrInvalidSpec, MaxOnline)
	}
	if spec.SessionDuration < 0 || spec.SessionDuration > MaxSessionDuration {
		return LiveRunSpec{}, fmt.Errorf("%w: session_duration must be between zero and %s", ErrInvalidSpec, MaxSessionDuration)
	}
	if spec.SessionDuration > spec.Duration {
		return LiveRunSpec{}, fmt.Errorf("%w: session_duration must not exceed duration", ErrInvalidSpec)
	}
	if spec.ThinkTime < 0 || spec.ThinkTime > MaxThinkTime {
		return LiveRunSpec{}, fmt.Errorf("%w: think_time must be between zero and %s", ErrInvalidSpec, MaxThinkTime)
	}
	if spec.SessionActions < 0 || spec.SessionActions > MaxSessionActions {
		return LiveRunSpec{}, fmt.Errorf("%w: session_actions must be between zero and %d", ErrInvalidSpec, MaxSessionActions)
	}
	if !spec.Sustained() && (spec.ThinkTime > 0 || spec.SessionActions > 0) {
		return LiveRunSpec{}, fmt.Errorf("%w: think_time and session_actions require a positive session_duration", ErrInvalidSpec)
	}
	if spec.CapacityClaim {
		return LiveRunSpec{}, fmt.Errorf("%w: live bot runs cannot make a capacity claim", ErrInvalidSpec)
	}
	if spec.Execution != "" && spec.Execution != ExecutionRealSandbox {
		return LiveRunSpec{}, fmt.Errorf("%w: execution must be %q", ErrInvalidSpec, ExecutionRealSandbox)
	}
	spec.Execution = ExecutionRealSandbox

	scenario := strings.TrimSpace(spec.Scenario)
	scenarioID := strings.TrimSpace(spec.ScenarioID)
	if scenario != "" && scenarioID != "" && scenario != scenarioID {
		return LiveRunSpec{}, fmt.Errorf("%w: scenario and scenario_id must match", ErrInvalidSpec)
	}
	if scenario == "" {
		scenario = scenarioID
	}
	if scenario == "" {
		scenario = "default"
	}
	if !validIdentifier(scenario, MaxScenarioLength) {
		return LiveRunSpec{}, fmt.Errorf("%w: scenario must be 1-%d identifier characters", ErrInvalidSpec, MaxScenarioLength)
	}
	spec.Scenario = scenario
	spec.ScenarioID = scenario

	mix := make(map[string]float64, len(spec.ActionMix))
	if len(spec.ActionMix) == 0 {
		mix["default"] = 1
	} else {
		if len(spec.ActionMix) > MaxActionMixLength {
			return LiveRunSpec{}, fmt.Errorf("%w: action_mix has more than %d entries", ErrInvalidSpec, MaxActionMixLength)
		}
		var total float64
		for action, weight := range spec.ActionMix {
			action = strings.TrimSpace(action)
			if !validIdentifier(action, MaxActionLength) {
				return LiveRunSpec{}, fmt.Errorf("%w: invalid action identifier %q", ErrInvalidSpec, action)
			}
			if math.IsNaN(weight) || math.IsInf(weight, 0) || weight < 0 {
				return LiveRunSpec{}, fmt.Errorf("%w: action weight for %q must be finite and non-negative", ErrInvalidSpec, action)
			}
			if weight > 1e9 {
				return LiveRunSpec{}, fmt.Errorf("%w: action weight for %q is too large", ErrInvalidSpec, action)
			}
			mix[action] = weight
			total += weight
		}
		if total <= 0 || math.IsInf(total, 0) {
			return LiveRunSpec{}, fmt.Errorf("%w: action_mix must contain a positive weight", ErrInvalidSpec)
		}
	}
	spec.ActionMix = mix
	return spec, nil
}

func validIdentifier(value string, maxLength int) bool {
	return len(value) > 0 && len(value) <= maxLength && identifierPattern.MatchString(value)
}

// BotSpec is the deterministic, secret-free input passed to one Executor
// invocation. It contains no login credential, ticket, cookie, or token.
type BotSpec struct {
	RunID       string             `json:"run_id"`
	BotID       string             `json:"bot_id"`
	PlayerID    uint64             `json:"player_id"`
	Index       int                `json:"index"`
	Seed        uint64             `json:"seed"`
	Environment Environment        `json:"environment"`
	Scenario    string             `json:"scenario"`
	ActionMix   map[string]float64 `json:"action_mix"`

	// SessionDuration, ThinkTime and SessionActions describe how long this bot
	// stays online and how often it acts. A zero SessionDuration means one
	// action and disconnect.
	SessionDuration time.Duration `json:"session_duration"`
	ThinkTime       time.Duration `json:"think_time"`
	SessionActions  int           `json:"session_actions"`
}

// Sustained reports whether this bot stays online for repeated actions.
func (spec BotSpec) Sustained() bool {
	return spec.SessionDuration > 0
}

// PickAction deterministically selects an action from the validated mix. The
// sorted key order and per-bot seed make this independent of map iteration.
func (spec BotSpec) PickAction(ordinal int) string {
	return spec.PickActionAmong(ordinal, nil)
}

// PickActionAmong draws from the mix restricted to the actions the caller says
// are currently possible, renormalising the weights over what is left. A nil
// or empty allowed set means everything in the mix is possible.
//
// A blind draw is only honest when every action can always be performed. In a
// farm it cannot: watering needs a growing crop, harvesting needs a ripe one,
// and a mix that asks for more harvests than plantings can never be satisfied
// because every harvest spends a planting. A blind draw in that world spends
// most of its turns discovering it has nothing to do, and the run measures the
// game's supply of legal targets instead of the server's capacity. Choosing
// among what is legal is also what a player does: someone with nothing to
// water harvests instead. The cost is that the realised mix drifts from the
// declared one, so a run has to report both.
func (spec BotSpec) PickActionAmong(ordinal int, allowed []string) string {
	if ordinal < 0 {
		ordinal = 0
	}
	actions := make([]string, 0, len(spec.ActionMix))
	for action, weight := range spec.ActionMix {
		if weight > 0 && permitted(action, allowed) {
			actions = append(actions, action)
		}
	}
	if len(actions) == 0 {
		return "default"
	}
	sort.Strings(actions)
	var total float64
	for _, action := range actions {
		total += spec.ActionMix[action]
	}
	value := float64(splitMix64(spec.Seed+uint64(ordinal))) / float64(^uint64(0)) * total
	var cumulative float64
	for _, action := range actions {
		cumulative += spec.ActionMix[action]
		if value < cumulative {
			return action
		}
	}
	return actions[len(actions)-1]
}

func permitted(action string, allowed []string) bool {
	if len(allowed) == 0 {
		return true
	}
	for _, candidate := range allowed {
		if candidate == action {
			return true
		}
	}
	return false
}

// JitterFraction returns a deterministic value in [0,1) for one bot and
// ordinal. Pacing can be spread across bots without a shared random source,
// and the same seed still reproduces the same schedule.
func (spec BotSpec) JitterFraction(ordinal int) float64 {
	if ordinal < 0 {
		ordinal = 0
	}
	return float64(splitMix64(spec.Seed^(uint64(ordinal)*0x9e3779b97f4a7c15))) / float64(^uint64(0))
}

func splitMix64(value uint64) uint64 {
	value += 0x9e3779b97f4a7c15
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}

func botID(index int, playerID uint64) string {
	return "bot-" + strconv.FormatUint(playerID, 10) + "-" + strconv.Itoa(index)
}
