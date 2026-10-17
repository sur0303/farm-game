package task

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"sync"
)

// P0Repository is the server-owned immutable TaskConfigRepository used by the
// production PlayerActor composition.  It contains one published P0 snapshot
// and one append-only growth chain; period assignments are derived from that
// already-published snapshot and are never accepted from a request caller.
//
// A later config publication should be represented by a new repository
// instance (or by a deployment-level immutable snapshot loader), not by
// mutating this value in place.
type P0Repository struct {
	mu         sync.RWMutex
	assignment map[string]Assignment
	daily      map[uint64]*Config
	growth     *GrowthChain
}

const (
	P0TaskConfigVersion     uint64 = 1
	P0GrowthConfigVersion   uint64 = 1
	P0DailyLoginID                 = "daily.login.v1"
	P0DailyFriendVisitID           = "daily.friend_visit.v1"
	P0DailyFriendInteractID        = "daily.friend_interact.v1"
	P0GrowthHarvestID              = "growth.harvest.v1"
)

// NewP0Repository returns the frozen P0 server configuration.  The game
// timezone is deliberately not a parameter: P0 is server-configured to
// Asia/Shanghai and public/task callers cannot override it.
func NewP0Repository() (*P0Repository, error) {
	cfg := Config{
		Version:  P0TaskConfigVersion,
		Timezone: DefaultGameTimezone,
		Daily: [DailySlotCount]DailyDefinition{
			{Kind: DailyLogin, TaskID: P0DailyLoginID, Reward: RewardBundle{Exp: 1}},
			{Kind: DailyFriendVisit, TaskID: P0DailyFriendVisitID, Reward: RewardBundle{Exp: 1}},
			{Kind: DailyFriendInteract, TaskID: P0DailyFriendInteractID, Reward: RewardBundle{Exp: 1}},
		},
	}
	cfg.PayloadHash = p0ConfigHash(cfg)
	if err := cfg.Validate(DefaultGameTimezone); err != nil {
		return nil, fmt.Errorf("task p0 config: %w", err)
	}
	growth := &GrowthChain{
		Version: P0GrowthConfigVersion,
		Definitions: []GrowthDefinition{{
			TaskID: P0GrowthHarvestID, DefinitionVersion: 1,
			Metric: GrowthMetricTotalHarvest, Target: 1,
			Reward: RewardBundle{Exp: 1},
		}},
	}
	if err := growth.Validate(); err != nil {
		return nil, fmt.Errorf("task p0 growth config: %w", err)
	}
	return &P0Repository{
		assignment: make(map[string]Assignment),
		daily:      map[uint64]*Config{cfg.Version: cloneConfig(&cfg)},
		growth:     cloneGrowthChain(growth),
	}, nil
}

// MustNewP0Repository is intended for a process composition root where a
// malformed built-in snapshot is a startup failure.
func MustNewP0Repository() *P0Repository {
	repo, err := NewP0Repository()
	if err != nil {
		panic(err)
	}
	return repo
}

func (r *P0Repository) AssignmentForPeriod(periodID string) (Assignment, bool) {
	if r == nil || !validPeriodID(periodID) {
		return Assignment{}, false
	}
	r.mu.RLock()
	assignment, ok := r.assignment[periodID]
	if ok {
		r.mu.RUnlock()
		return assignment, true
	}
	cfg := r.daily[P0TaskConfigVersion]
	if cfg == nil {
		r.mu.RUnlock()
		return Assignment{}, false
	}
	assignment = Assignment{PeriodID: periodID, ConfigVersion: cfg.Version, ConfigPayloadHash: cfg.PayloadHash}
	r.mu.RUnlock()
	// The map is only a process cache.  Two identical writes are harmless and
	// the value is fully derived from immutable publication data.
	r.mu.Lock()
	if existing, exists := r.assignment[periodID]; exists {
		assignment = existing
	} else {
		r.assignment[periodID] = assignment
	}
	r.mu.Unlock()
	return assignment, true
}

func (r *P0Repository) DailySnapshot(version uint64) (*Config, bool) {
	if r == nil {
		return nil, false
	}
	r.mu.RLock()
	cfg, ok := r.daily[version]
	if ok {
		cfg = cloneConfig(cfg)
	}
	r.mu.RUnlock()
	return cfg, ok
}

func (r *P0Repository) LatestGrowthChain() (*GrowthChain, bool) {
	if r == nil {
		return nil, false
	}
	r.mu.RLock()
	growth := cloneGrowthChain(r.growth)
	ok := growth != nil
	r.mu.RUnlock()
	return growth, ok
}

func validPeriodID(value string) bool {
	if len(value) != 8 {
		return false
	}
	for _, ch := range value {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return value >= "19700101" && value <= "99991231"
}

func p0ConfigHash(cfg Config) string {
	textValue := strconv.FormatUint(cfg.Version, 10) + "|" + cfg.Timezone
	for _, def := range cfg.Daily {
		textValue += fmt.Sprintf("|%d:%s:%d:%d", def.Kind, def.TaskID, def.Reward.Coin, def.Reward.Exp)
	}
	sum := sha256.Sum256([]byte(textValue))
	return hex.EncodeToString(sum[:])
}

func cloneConfig(in *Config) *Config {
	if in == nil {
		return nil
	}
	out := *in
	for i := range out.Daily {
		out.Daily[i].Reward.Items = append([]ItemGrant(nil), out.Daily[i].Reward.Items...)
		if grant := out.Daily[i].Reward.PetUnlock; grant != nil {
			copyGrant := *grant
			out.Daily[i].Reward.PetUnlock = &copyGrant
		}
	}
	return &out
}

func cloneGrowthChain(in *GrowthChain) *GrowthChain {
	if in == nil {
		return nil
	}
	out := *in
	out.Definitions = append([]GrowthDefinition(nil), in.Definitions...)
	for i := range out.Definitions {
		out.Definitions[i].Reward.Items = append([]ItemGrant(nil), in.Definitions[i].Reward.Items...)
		if grant := in.Definitions[i].Reward.PetUnlock; grant != nil {
			copyGrant := *grant
			out.Definitions[i].Reward.PetUnlock = &copyGrant
		}
	}
	return &out
}
