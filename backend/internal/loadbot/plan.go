package loadbot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
)

const (
	maxPlayersPerDryRun = 100_000
	maxStepsPerSession  = 10_000
	maxTotalPlanSteps   = 250_000
)

type SessionState string

const (
	StateOffline        SessionState = "OFFLINE"
	StateAuthenticating SessionState = "AUTHENTICATING"
	StateJoiningHome    SessionState = "JOINING_HOME"
	StateInHome         SessionState = "IN_HOME"
	StateFriendList     SessionState = "FRIEND_LIST"
	StateFriendFarm     SessionState = "FRIEND_FARM"
	StateReconnecting   SessionState = "RECONNECTING"
	StateEnded          SessionState = "ENDED"
)

type StepKind string

const (
	StepAuthenticate StepKind = "authenticate"
	StepJoinHome     StepKind = "join_home"
	StepReturnHome   StepKind = "return_home"
	StepAction       StepKind = "action"
	StepRecovery     StepKind = "recovery"
	StepEndSession   StepKind = "end_session"
)

// PlannedStep is one state-safe intent. It is not an executable network
// request: a future adapter must translate it to the public protocol and
// preserve the same operation identity and admission rules.
type PlannedStep struct {
	Sequence    int          `json:"sequence"`
	AtMS        int64        `json:"at_ms"`
	Kind        StepKind     `json:"kind"`
	Action      Action       `json:"action,omitempty"`
	StateBefore SessionState `json:"state_before"`
	StateAfter  SessionState `json:"state_after"`
	Expectation string       `json:"expectation"`
}

// PlayerPlan is a deterministic synthetic player journey.
type PlayerPlan struct {
	PlayerOrdinal int           `json:"player_ordinal"`
	Profile       Profile       `json:"profile"`
	Steps         []PlannedStep `json:"steps"`
}

// DryRunPlan is intentionally local-only evidence of the workload shape. It
// must not be presented as a live capacity result.
type DryRunPlan struct {
	SchemaVersion string       `json:"schema_version"`
	RunID         string       `json:"run_id"`
	Seed          int64        `json:"seed"`
	Players       []PlayerPlan `json:"players"`
	Fingerprint   string       `json:"fingerprint"`
	Execution     string       `json:"execution"`
	CapacityClaim bool         `json:"capacity_claim"`
}

// BuildDryRun produces a reproducible state-machine plan without opening a
// network connection. players and actionsPerSession are intentionally bounded
// so a mistaken local invocation cannot become a load generator.
func BuildDryRun(config Config, players, actionsPerSession int) (DryRunPlan, error) {
	if err := config.Validate(); err != nil {
		return DryRunPlan{}, err
	}
	if players <= 0 || players > maxPlayersPerDryRun {
		return DryRunPlan{}, fmt.Errorf("players must be in [1,%d]", maxPlayersPerDryRun)
	}
	if actionsPerSession <= 0 || actionsPerSession > maxStepsPerSession {
		return DryRunPlan{}, fmt.Errorf("actions_per_session must be in [1,%d]", maxStepsPerSession)
	}
	// A social/recovery action expands into several state transitions. Bound
	// aggregate output rather than trusting the individual dimensions alone.
	if int64(players)*int64(actionsPerSession) > maxTotalPlanSteps {
		return DryRunPlan{}, fmt.Errorf("players * actions_per_session must be <= %d", maxTotalPlanSteps)
	}

	random := rand.New(rand.NewSource(config.Seed))
	result := DryRunPlan{
		SchemaVersion: ConfigSchemaVersion,
		RunID:         config.RunID,
		Seed:          config.Seed,
		Players:       make([]PlayerPlan, 0, players),
		Execution:     "DRY_RUN_ONLY",
		CapacityClaim: false,
	}
	for ordinal := 1; ordinal <= players; ordinal++ {
		profile := pickProfile(random, config.Profiles)
		journey := buildJourney(random, profile, config, ordinal, actionsPerSession)
		result.Players = append(result.Players, journey)
	}
	fingerprint, err := planFingerprint(result)
	if err != nil {
		return DryRunPlan{}, err
	}
	result.Fingerprint = fingerprint
	return result, nil
}

func buildJourney(random *rand.Rand, profile Profile, config Config, ordinal, actionsPerSession int) PlayerPlan {
	steps := make([]PlannedStep, 0, actionsPerSession*3+5)
	state := StateOffline
	nowMS := int64(ordinal-1) * arrivalSpacingMS(config)
	appendStep := func(kind StepKind, action Action, after SessionState, expectation string) {
		steps = append(steps, PlannedStep{
			Sequence:    len(steps) + 1,
			AtMS:        nowMS,
			Kind:        kind,
			Action:      action,
			StateBefore: state,
			StateAfter:  after,
			Expectation: expectation,
		})
		state = after
		nowMS += thinkTimeMS(random, config.Session)
	}

	appendStep(StepAuthenticate, "", StateAuthenticating, "obtain a valid online session without replaying prior operations")
	appendStep(StepJoinHome, "", StateJoiningHome, "wait for subscription snapshot and ready fence")
	appendStep(StepJoinHome, "", StateInHome, "home subscription is ready before any write action")

	for index := 0; index < actionsPerSession; index++ {
		action := pickAction(random, eligibleActions(profile, state, config.Actions))
		switch action {
		case ActionFriendNavigation:
			if state == StateFriendFarm {
				appendStep(StepReturnHome, "", StateJoiningHome, "replace the old friend subscription before navigating again")
				appendStep(StepReturnHome, "", StateInHome, "home subscription is ready again")
			}
			appendStep(StepAction, action, StateFriendList, "read friend candidates; no authority mutation")
			appendStep(StepAction, action, StateFriendFarm, "join an exact valid friend farm before interaction")
		case ActionCare, ActionTheft:
			if state != StateFriendFarm {
				appendStep(StepAction, ActionFriendNavigation, StateFriendList, "find a currently valid friend opportunity")
				appendStep(StepAction, ActionFriendNavigation, StateFriendFarm, "join the target farm and wait for ready")
			}
			appendStep(StepAction, action, StateFriendFarm, "perform one permitted social action; final authority decides")
		case ActionReconnect:
			appendStep(StepRecovery, action, StateReconnecting, "WSS may be lost; retain operation identity for recovery")
			appendStep(StepRecovery, action, StateAuthenticating, "renew ticket through Cookie-based session continuity")
			appendStep(StepRecovery, action, StateJoiningHome, "Fresh Join must establish a new subscription fence")
			appendStep(StepRecovery, action, StateInHome, "resume only after recovery and snapshot readiness")
		case ActionSlowOrJitter:
			appendStep(StepAction, action, state, "simulate a bounded client-side slow-read or network-jitter condition")
		default:
			if state == StateFriendFarm {
				appendStep(StepReturnHome, "", StateJoiningHome, "leave friend presentation and replace the subscription")
				appendStep(StepReturnHome, "", StateInHome, "home subscription is ready again")
			}
			appendStep(StepAction, action, StateInHome, actionExpectation(action))
		}
	}
	if state == StateFriendFarm {
		appendStep(StepReturnHome, "", StateJoiningHome, "return to the home farm using a replacement subscription")
		appendStep(StepReturnHome, "", StateInHome, "home subscription is ready again")
	}
	appendStep(StepEndSession, "", StateEnded, "end synthetic session without issuing more commands")
	return PlayerPlan{PlayerOrdinal: ordinal, Profile: profile, Steps: steps}
}

func eligibleActions(profile Profile, state SessionState, configured []WeightedAction) []WeightedAction {
	allowed := allowedForProfile(profile)
	result := make([]WeightedAction, 0, len(configured))
	for _, candidate := range configured {
		if !allowed[candidate.Action] {
			continue
		}
		if (candidate.Action == ActionCare || candidate.Action == ActionTheft) && state != StateFriendFarm && !allowed[ActionFriendNavigation] {
			continue
		}
		result = append(result, candidate)
	}
	if len(result) == 0 {
		return []WeightedAction{{Action: ActionViewOrIdle, Weight: 1}}
	}
	return result
}

func allowedForProfile(profile Profile) map[Action]bool {
	switch profile {
	case ProfileCasual:
		return actionSet(ActionViewOrIdle, ActionBuyOrPlant, ActionHarvestOrSell, ActionReconnect)
	case ProfileFarmer:
		return actionSet(ActionViewOrIdle, ActionBuyOrPlant, ActionHarvestOrSell, ActionReconnect)
	case ProfileSocial:
		return actionSet(ActionViewOrIdle, ActionFriendNavigation, ActionCare, ActionTheft, ActionReconnect)
	case ProfileHotspotViewer:
		return actionSet(ActionViewOrIdle, ActionFriendNavigation, ActionSlowOrJitter, ActionReconnect)
	case ProfileWeakNetwork:
		return actionSet(ActionViewOrIdle, ActionFriendNavigation, ActionReconnect, ActionSlowOrJitter)
	default:
		return actionSet(ActionViewOrIdle)
	}
}

func actionSet(actions ...Action) map[Action]bool {
	result := make(map[Action]bool, len(actions))
	for _, action := range actions {
		result[action] = true
	}
	return result
}

func pickProfile(random *rand.Rand, profiles []WeightedProfile) Profile {
	target := random.Intn(100)
	current := 0
	for _, profile := range profiles {
		current += profile.Weight
		if target < current {
			return profile.Profile
		}
	}
	return profiles[len(profiles)-1].Profile
}

func pickAction(random *rand.Rand, actions []WeightedAction) Action {
	total := 0
	for _, action := range actions {
		total += action.Weight
	}
	target := random.Intn(total)
	current := 0
	for _, action := range actions {
		current += action.Weight
		if target < current {
			return action.Action
		}
	}
	return actions[len(actions)-1].Action
}

func arrivalSpacingMS(config Config) int64 {
	if config.ArrivalModel == ArrivalClosedLoop {
		return 0
	}
	value := 1000.0 / config.ArrivalRatePerSecond
	if value < 1 {
		return 1
	}
	return int64(math.Round(value))
}

func thinkTimeMS(random *rand.Rand, session SessionConfig) int64 {
	minimum := session.ThinkTimeMin.Milliseconds()
	maximum := session.ThinkTimeMax.Milliseconds()
	if maximum <= minimum {
		return minimum
	}
	return minimum + random.Int63n(maximum-minimum+1)
}

func actionExpectation(action Action) string {
	switch action {
	case ActionBuyOrPlant:
		return "only buy or plant when the authoritative player and farm state allow it"
	case ActionHarvestOrSell:
		return "only harvest mature crops or sell owned inventory; operation result is authoritative"
	case ActionViewOrIdle:
		return "read or wait without authority mutation"
	default:
		return "action is admitted only after the appropriate session and subscription fences"
	}
}

func planFingerprint(plan DryRunPlan) (string, error) {
	copyPlan := plan
	copyPlan.Fingerprint = ""
	encoded, err := json.Marshal(copyPlan)
	if err != nil {
		return "", fmt.Errorf("encode dry-run plan: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

// ValidatePlan verifies that an external renderer did not alter ordering or
// create an impossible state transition.
func ValidatePlan(plan DryRunPlan) error {
	if plan.SchemaVersion != ConfigSchemaVersion {
		return errors.New("unexpected plan schema version")
	}
	if plan.Execution != "DRY_RUN_ONLY" || plan.CapacityClaim {
		return errors.New("plan must remain dry-run-only and make no capacity claim")
	}
	if len(plan.Players) == 0 {
		return errors.New("plan contains no players")
	}
	expected, err := planFingerprint(plan)
	if err != nil {
		return err
	}
	if plan.Fingerprint != expected {
		return errors.New("plan fingerprint mismatch")
	}
	for _, player := range plan.Players {
		if player.PlayerOrdinal <= 0 || len(player.Steps) == 0 {
			return errors.New("player plan is incomplete")
		}
		state := StateOffline
		var lastAt int64
		for index, step := range player.Steps {
			if step.Sequence != index+1 || step.StateBefore != state || step.AtMS < lastAt {
				return fmt.Errorf("player %d has invalid step %d", player.PlayerOrdinal, index+1)
			}
			if !legalTransition(step) {
				return fmt.Errorf("player %d has illegal transition %s -> %s", player.PlayerOrdinal, step.StateBefore, step.StateAfter)
			}
			if (step.Action == ActionCare || step.Action == ActionTheft) && step.StateBefore != StateFriendFarm {
				return fmt.Errorf("player %d executes %s outside a friend farm", player.PlayerOrdinal, step.Action)
			}
			state, lastAt = step.StateAfter, step.AtMS
		}
		if state != StateEnded {
			return fmt.Errorf("player %d does not end its session", player.PlayerOrdinal)
		}
	}
	return nil
}

func legalTransition(step PlannedStep) bool {
	switch step.Kind {
	case StepAuthenticate:
		return (step.StateBefore == StateOffline || step.StateBefore == StateReconnecting) && step.StateAfter == StateAuthenticating
	case StepJoinHome:
		return (step.StateBefore == StateAuthenticating && step.StateAfter == StateJoiningHome) ||
			(step.StateBefore == StateJoiningHome && step.StateAfter == StateInHome)
	case StepReturnHome:
		return (step.StateBefore == StateFriendFarm && step.StateAfter == StateJoiningHome) ||
			(step.StateBefore == StateJoiningHome && step.StateAfter == StateInHome)
	case StepRecovery:
		return (step.StateBefore == StateInHome || step.StateBefore == StateFriendFarm) && step.StateAfter == StateReconnecting ||
			(step.StateBefore == StateReconnecting && step.StateAfter == StateAuthenticating) ||
			(step.StateBefore == StateAuthenticating && step.StateAfter == StateJoiningHome) ||
			(step.StateBefore == StateJoiningHome && step.StateAfter == StateInHome)
	case StepAction:
		return step.StateBefore == step.StateAfter ||
			(step.StateBefore == StateInHome && step.StateAfter == StateFriendList) ||
			(step.StateBefore == StateFriendList && step.StateAfter == StateFriendFarm)
	case StepEndSession:
		return (step.StateBefore == StateInHome || step.StateBefore == StateFriendFarm) && step.StateAfter == StateEnded
	default:
		return false
	}
}
