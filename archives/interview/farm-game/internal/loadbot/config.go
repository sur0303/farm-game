// Package loadbot defines deterministic, side-effect-free workload recipes.
//
// The package intentionally models only decisions and scenario contracts. A
// later transport adapter may execute the returned actions over HTTPS/WSS, but
// this package itself never opens a socket or starts a goroutine.
package loadbot

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

const ConfigSchemaVersion = "mixed-farm-bot/v1"

type ArrivalModel string

const (
	ArrivalOpenLoop   ArrivalModel = "open_loop"
	ArrivalClosedLoop ArrivalModel = "closed_loop"
)

func (model ArrivalModel) Validate() error {
	switch model {
	case ArrivalOpenLoop, ArrivalClosedLoop:
		return nil
	default:
		return fmt.Errorf("unknown arrival model %q", model)
	}
}

type Profile string

const (
	ProfileCasual        Profile = "casual"
	ProfileFarmer        Profile = "farmer"
	ProfileSocial        Profile = "social"
	ProfileHotspotViewer Profile = "hotspot_viewer"
	ProfileWeakNetwork   Profile = "weak_network"
)

func (profile Profile) Validate() error {
	switch profile {
	case ProfileCasual, ProfileFarmer, ProfileSocial, ProfileHotspotViewer, ProfileWeakNetwork:
		return nil
	default:
		return fmt.Errorf("unknown player profile %q", profile)
	}
}

type Action string

const (
	ActionViewOrIdle       Action = "view_or_idle"
	ActionFriendNavigation Action = "friend_navigation"
	ActionBuyOrPlant       Action = "buy_or_plant"
	ActionHarvestOrSell    Action = "harvest_or_sell"
	ActionCare             Action = "care"
	ActionTheft            Action = "theft"
	ActionReconnect        Action = "reconnect"
	ActionSlowOrJitter     Action = "slow_or_jitter"
)

func (action Action) Validate() error {
	switch action {
	case ActionViewOrIdle, ActionFriendNavigation, ActionBuyOrPlant, ActionHarvestOrSell,
		ActionCare, ActionTheft, ActionReconnect, ActionSlowOrJitter:
		return nil
	default:
		return fmt.Errorf("unknown action %q", action)
	}
}

type WeightedProfile struct {
	Profile Profile `json:"profile"`
	Weight  int     `json:"weight"`
}

type WeightedAction struct {
	Action Action `json:"action"`
	Weight int    `json:"weight"`
}

type SessionConfig struct {
	LoginsPerDayMean float64       `json:"logins_per_day_mean"`
	DurationP50      time.Duration `json:"duration_p50"`
	DurationP95      time.Duration `json:"duration_p95"`
	ThinkTimeMin     time.Duration `json:"think_time_min"`
	ThinkTimeMax     time.Duration `json:"think_time_max"`
}

type Config struct {
	SchemaVersion          string            `json:"schema_version"`
	RunID                  string            `json:"run_id"`
	Seed                   int64             `json:"seed"`
	Duration               time.Duration     `json:"duration"`
	ArrivalModel           ArrivalModel      `json:"arrival_model"`
	ArrivalRatePerSecond   float64           `json:"arrival_rate_per_second"`
	MaxOnline              int               `json:"max_online"`
	Session                SessionConfig     `json:"session"`
	Profiles               []WeightedProfile `json:"profiles"`
	Actions                []WeightedAction  `json:"actions"`
	HotFarmViewerCounts    []int             `json:"hot_farm_viewer_counts"`
	StrictTransportEnabled bool              `json:"strict_transport_enabled"`
}

func DefaultConfig() Config {
	return Config{
		SchemaVersion:        ConfigSchemaVersion,
		RunID:                "mixed-farm-smoke",
		Seed:                 20260812,
		Duration:             30 * time.Minute,
		ArrivalModel:         ArrivalOpenLoop,
		ArrivalRatePerSecond: 20,
		MaxOnline:            1000,
		Session: SessionConfig{
			LoginsPerDayMean: 5.5,
			DurationP50:      3 * time.Minute,
			DurationP95:      5 * time.Minute,
			ThinkTimeMin:     500 * time.Millisecond,
			ThinkTimeMax:     8 * time.Second,
		},
		Profiles: []WeightedProfile{
			{Profile: ProfileCasual, Weight: 40},
			{Profile: ProfileFarmer, Weight: 25},
			{Profile: ProfileSocial, Weight: 20},
			{Profile: ProfileHotspotViewer, Weight: 10},
			{Profile: ProfileWeakNetwork, Weight: 5},
		},
		Actions: []WeightedAction{
			{Action: ActionViewOrIdle, Weight: 55},
			{Action: ActionFriendNavigation, Weight: 15},
			{Action: ActionBuyOrPlant, Weight: 10},
			{Action: ActionHarvestOrSell, Weight: 8},
			{Action: ActionCare, Weight: 5},
			{Action: ActionTheft, Weight: 3},
			{Action: ActionReconnect, Weight: 2},
			{Action: ActionSlowOrJitter, Weight: 2},
		},
		HotFarmViewerCounts: []int{50, 200},
	}
}

func (config Config) Validate() error {
	if config.SchemaVersion != ConfigSchemaVersion {
		return fmt.Errorf("schema_version must be %q", ConfigSchemaVersion)
	}
	if strings.TrimSpace(config.RunID) == "" {
		return errors.New("run_id is required")
	}
	if config.Duration <= 0 {
		return errors.New("duration must be > 0")
	}
	if err := config.ArrivalModel.Validate(); err != nil {
		return err
	}
	if math.IsNaN(config.ArrivalRatePerSecond) || math.IsInf(config.ArrivalRatePerSecond, 0) || config.ArrivalRatePerSecond <= 0 {
		return errors.New("arrival_rate_per_second must be finite and > 0")
	}
	if config.MaxOnline <= 0 {
		return errors.New("max_online must be > 0")
	}
	if err := config.Session.validate(); err != nil {
		return err
	}
	if err := validateProfiles(config.Profiles); err != nil {
		return err
	}
	if err := validateActions(config.Actions); err != nil {
		return err
	}
	for _, viewers := range config.HotFarmViewerCounts {
		if viewers <= 0 || viewers > config.MaxOnline {
			return fmt.Errorf("hot farm viewer count %d must be in [1,%d]", viewers, config.MaxOnline)
		}
	}
	if config.StrictTransportEnabled {
		return errors.New("strict_transport_enabled is reserved for a separately reviewed execution adapter")
	}
	return nil
}

func (session SessionConfig) validate() error {
	if math.IsNaN(session.LoginsPerDayMean) || math.IsInf(session.LoginsPerDayMean, 0) || session.LoginsPerDayMean <= 0 {
		return errors.New("logins_per_day_mean must be finite and > 0")
	}
	if session.DurationP50 <= 0 || session.DurationP95 <= 0 || session.DurationP50 > session.DurationP95 {
		return errors.New("session duration percentiles must satisfy 0 < p50 <= p95")
	}
	if session.ThinkTimeMin < 0 || session.ThinkTimeMax < session.ThinkTimeMin {
		return errors.New("think time must satisfy 0 <= min <= max")
	}
	return nil
}

func validateProfiles(profiles []WeightedProfile) error {
	if len(profiles) == 0 {
		return errors.New("at least one profile is required")
	}
	seen := make(map[Profile]struct{}, len(profiles))
	total := 0
	for _, item := range profiles {
		if err := item.Profile.Validate(); err != nil {
			return err
		}
		if item.Weight <= 0 {
			return fmt.Errorf("profile %q must have positive weight", item.Profile)
		}
		if _, exists := seen[item.Profile]; exists {
			return fmt.Errorf("profile %q is duplicated", item.Profile)
		}
		seen[item.Profile] = struct{}{}
		total += item.Weight
	}
	if total != 100 {
		return fmt.Errorf("profile weights sum to %d, want 100", total)
	}
	return nil
}

func validateActions(actions []WeightedAction) error {
	if len(actions) == 0 {
		return errors.New("at least one action is required")
	}
	seen := make(map[Action]struct{}, len(actions))
	total := 0
	for _, item := range actions {
		if err := item.Action.Validate(); err != nil {
			return err
		}
		if item.Weight <= 0 {
			return fmt.Errorf("action %q must have positive weight", item.Action)
		}
		if _, exists := seen[item.Action]; exists {
			return fmt.Errorf("action %q is duplicated", item.Action)
		}
		seen[item.Action] = struct{}{}
		total += item.Weight
	}
	if total != 100 {
		return fmt.Errorf("action weights sum to %d, want 100", total)
	}
	return nil
}

// CanonicalProfiles and CanonicalActions make dry-run output stable even if an
// operator provides an equivalent configuration in a different ordering.
func (config Config) CanonicalProfiles() []WeightedProfile {
	result := append([]WeightedProfile(nil), config.Profiles...)
	sort.Slice(result, func(left, right int) bool { return result[left].Profile < result[right].Profile })
	return result
}

func (config Config) CanonicalActions() []WeightedAction {
	result := append([]WeightedAction(nil), config.Actions...)
	sort.Slice(result, func(left, right int) bool { return result[left].Action < result[right].Action })
	return result
}
