package task

import (
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"

	playerpet "farmgame/internal/player/pet"
)

// DailyDefinition is one of the three fixed daily tasks inside a versioned
// config snapshot. Kind is the semantic identity; TaskID is the stable
// config id used inside instance ids.
type DailyDefinition struct {
	Kind   DailyKind
	TaskID string
	Reward RewardBundle
}

// GrowthDefinition is one immutable entry of the linear growth chain.
// {TaskID, DefinitionVersion} can never be modified in place once
// published (Q34-07); changing target or reward requires a new version.
type GrowthDefinition struct {
	TaskID            string
	DefinitionVersion uint64
	Metric            GrowthMetric
	Target            int64
	Reward            RewardBundle
}

// Config is one immutable, versioned task configuration snapshot loaded by
// TaskConfigRepository. A request never mixes snapshots (Q34-07).
type Config struct {
	Version     uint64
	Timezone    string // must match the domain game timezone
	PayloadHash string
	Daily       [DailySlotCount]DailyDefinition
}

// Validate enforces publish-time invariants so a broken snapshot fails
// closed at load instead of at claim time (Q34-06/Q34-07).
func (c *Config) Validate(gameTimezone string) error {
	return c.validate(gameTimezone, playerpet.DefaultP0Catalog())
}

func (c *Config) validate(gameTimezone string, petCatalog playerpet.Provider) error {
	if c == nil {
		return errors.New("task config: nil snapshot")
	}
	if c.Version == 0 {
		return errors.New("task config: version required")
	}
	if c.Timezone == "" || c.Timezone != gameTimezone {
		return fmt.Errorf("task config: timezone %q does not match game timezone %q", c.Timezone, gameTimezone)
	}
	if c.PayloadHash == "" {
		return errors.New("task config: payload hash required")
	}
	seen := map[DailyKind]bool{}
	ids := map[string]bool{}
	for _, def := range c.Daily {
		switch def.Kind {
		case DailyLogin, DailyFriendVisit, DailyFriendInteract:
		default:
			return fmt.Errorf("task config: daily kind %d is not one of the three fixed P0 tasks", def.Kind)
		}
		if seen[def.Kind] {
			return fmt.Errorf("task config: duplicate daily kind %s", def.Kind)
		}
		seen[def.Kind] = true
		if def.TaskID == "" || ids[def.TaskID] {
			return fmt.Errorf("task config: daily task id %q empty or duplicated", def.TaskID)
		}
		if strings.Contains(def.TaskID, "/") {
			return fmt.Errorf("task config: daily task id %q must not contain '/'", def.TaskID)
		}
		ids[def.TaskID] = true
		if err := validateReward(def.Reward, petCatalog); err != nil {
			return fmt.Errorf("task config: daily %s: %w", def.Kind, err)
		}
	}
	for _, kind := range []DailyKind{DailyLogin, DailyFriendVisit, DailyFriendInteract} {
		if !seen[kind] {
			return fmt.Errorf("task config: missing daily kind %s", kind)
		}
	}
	return nil
}

func validateReward(r RewardBundle, petCatalog playerpet.Provider) error {
	if r.Coin < 0 || r.Exp < 0 {
		return errors.New("negative reward")
	}
	if len(r.Items) > 8 {
		return errors.New("reward items exceed bound of 8")
	}
	for _, item := range r.Items {
		if item.ItemID <= 0 || item.Count <= 0 {
			return fmt.Errorf("invalid item grant %+v", item)
		}
	}
	if r.isZero() {
		return errors.New("empty reward bundle")
	}
	if r.PetUnlock != nil {
		grant := r.PetUnlock
		if grant.PetID <= 0 || grant.PetConfigVersion == 0 || grant.GrantDefinitionVersion == 0 {
			return fmt.Errorf("invalid pet unlock grant %+v", grant)
		}
		if petCatalog == nil {
			return errors.New("pet catalog unavailable")
		}
		if _, ok := petCatalog.DefinitionAt(grant.PetID, grant.PetConfigVersion); !ok {
			return fmt.Errorf("pet unlock grant references unknown pet definition pet_id=%d pet_config_version=%d", grant.PetID, grant.PetConfigVersion)
		}
	}
	return nil
}

// Assignment is the immutable global DailyTaskSetAssignment record
// (Q34-07): exactly one published config version per period_id. It is a
// publication-side record cached in process, never a per-player key.
type Assignment struct {
	PeriodID          string
	ConfigVersion     uint64
	EffectiveAtMs     int64
	ConfigPayloadHash string
}

// GrowthChain is the latest published linear growth chain plus its
// activation config version. Publication must be prefix-stable
// (append-only) relative to any already-activated cursor: entries before
// a player's cursor are exactly the instances that player has claimed,
// which is how a re-claim of an advanced-past instance is answered from
// task truth without any claim history (Q34-07).
type GrowthChain struct {
	Version     uint64
	Definitions []GrowthDefinition
}

// Validate enforces growth chain invariants at load.
func (g *GrowthChain) Validate() error {
	if g == nil {
		return errors.New("task config: nil growth chain")
	}
	if g.Version == 0 {
		return errors.New("task config: growth chain version required")
	}
	seen := map[string]bool{}
	for _, def := range g.Definitions {
		if def.TaskID == "" || seen[def.TaskID] {
			return fmt.Errorf("task config: growth task id %q empty or duplicated", def.TaskID)
		}
		if strings.Contains(def.TaskID, "/") {
			return fmt.Errorf("task config: growth task id %q must not contain '/'", def.TaskID)
		}
		seen[def.TaskID] = true
		if def.DefinitionVersion == 0 {
			return fmt.Errorf("task config: growth %s: definition_version required", def.TaskID)
		}
		if def.Target <= 0 {
			return fmt.Errorf("task config: growth %s: target must be positive", def.TaskID)
		}
		switch def.Metric {
		case GrowthMetricTotalHarvest, GrowthMetricHighestLevel:
		default:
			return fmt.Errorf("task config: growth %s: unsupported metric %d", def.TaskID, def.Metric)
		}
		if err := validateReward(def.Reward, playerpet.DefaultP0Catalog()); err != nil {
			return fmt.Errorf("task config: growth %s: %w", def.TaskID, err)
		}
	}
	return nil
}

// Repository is the read-only, versioned TaskConfigRepository seam
// (Q34-01/Q34-07). Implementations batch-load immutable snapshots and
// cache them in process; a pod that never saw the current period must
// report !ok so the domain fails closed instead of picking a local
// "latest" version.
type Repository interface {
	// AssignmentForPeriod returns the unique published assignment for one
	// daily period id, or !ok when this pod never saw that period.
	AssignmentForPeriod(periodID string) (Assignment, bool)
	// DailySnapshot returns the immutable config snapshot for one version.
	DailySnapshot(version uint64) (*Config, bool)
	// LatestGrowthChain returns the newest published growth chain. Growth
	// definitions are immutable per {task_id, definition_version}; new
	// instances activate from this chain, already-activated instances keep
	// their frozen definition.
	LatestGrowthChain() (*GrowthChain, bool)
}

// rewardHash derives a deterministic hash of a frozen reward bundle. It is
// stored on the instance so later config drift is detectable.
func rewardHash(r RewardBundle) string {
	h := fnv.New64a()
	fmt.Fprintf(h, "coin=%d;exp=%d;", r.Coin, r.Exp)
	items := append([]ItemGrant(nil), r.Items...)
	sort.Slice(items, func(i, j int) bool { return items[i].ItemID < items[j].ItemID })
	for _, item := range items {
		fmt.Fprintf(h, "item=%d:%d;", item.ItemID, item.Count)
	}
	if r.PetUnlock != nil {
		fmt.Fprintf(h, "pet=%d:%d:%d;", r.PetUnlock.PetID, r.PetUnlock.PetConfigVersion, r.PetUnlock.GrantDefinitionVersion)
	}
	return itoa(h.Sum64())
}
