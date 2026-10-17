package task

// Category identifies the P0 task classes fixed by contract 34 §1/§2.8.
type Category int32

const (
	CategoryUnspecified Category = iota
	CategoryDaily
	CategoryGrowth
)

// State is the frozen task instance state machine (Q34-08):
// IN_PROGRESS -> CLAIMABLE -> CLAIMED, IN_PROGRESS/CLAIMABLE -> EXPIRED.
// CLAIMED never regresses. EXPIRED is produced only by lazy period switch
// (previous-period daily instances are discarded, not retained), so the
// value exists for outcomes and views but is never stored long term.
type State int32

const (
	StateUnspecified State = iota
	StateInProgress
	StateClaimable
	StateClaimed
	StateExpired
)

func (s State) String() string {
	switch s {
	case StateInProgress:
		return "IN_PROGRESS"
	case StateClaimable:
		return "CLAIMABLE"
	case StateClaimed:
		return "CLAIMED"
	case StateExpired:
		return "EXPIRED"
	default:
		return "UNSPECIFIED"
	}
}

// DailyKind enumerates the three fixed P0 daily tasks (Q34-08). Visit and
// interact are independent tasks: entering a friend farm never completes
// the interact task by itself.
type DailyKind int32

const (
	DailyUnspecified DailyKind = iota
	DailyLogin
	DailyFriendVisit
	DailyFriendInteract
)

func (k DailyKind) String() string {
	switch k {
	case DailyLogin:
		return "LOGIN"
	case DailyFriendVisit:
		return "FRIEND_VISIT"
	case DailyFriendInteract:
		return "FRIEND_INTERACT"
	default:
		return "UNSPECIFIED"
	}
}

// DailySlotCount is the frozen number of daily tasks shown in P0.
const DailySlotCount = 3

// GrowthMetric is the permanent cumulative indicator a growth task reads.
type GrowthMetric int32

const (
	GrowthMetricUnspecified GrowthMetric = iota
	GrowthMetricTotalHarvest
	GrowthMetricHighestLevel
)

// ItemGrant is a single inventory delta inside a RewardBundle.
type ItemGrant struct {
	ItemID int32
	Count  int64
}

// PetUnlockGrant is the normalized pet unlock structure frozen by Q34-06.
// It only adds the pet to the bounded unlocked set when applied by the
// PlayerActor; the pure domain never mutates pet state itself.
type PetUnlockGrant struct {
	PetID                  int32
	PetConfigVersion       uint64
	GrantDefinitionVersion uint64
}

// RewardBundle is the frozen reward attached to a task instance at
// creation. Clients never submit reward content; claims read this bundle.
type RewardBundle struct {
	Coin      int64
	Exp       int64
	Items     []ItemGrant
	PetUnlock *PetUnlockGrant
}

func (r RewardBundle) isZero() bool {
	return r.Coin == 0 && r.Exp == 0 && len(r.Items) == 0 && r.PetUnlock == nil
}

// Instance is one durable task instance (Q34-08 TaskInstance). Daily
// instances bind task_id + daily_period_id + config_version; growth
// instances bind task_id + definition_version. The reward is frozen at
// creation so a claim never consults the live configuration.
type Instance struct {
	InstanceID        string
	TaskID            string
	Category          Category
	Kind              DailyKind    // daily only
	Metric            GrowthMetric // growth only, frozen from the definition
	DefinitionVersion uint64
	PeriodID          string // daily only, YYYYMMDD in game timezone
	ConfigVersion     uint64 // daily: assignment version; growth: chain version at activation
	Target            int64
	Progress          int64
	State             State
	Reward            RewardBundle
	RewardHash        string
	FirstWitnessID    string
	ClaimedAtMs       int64
}

// DailyInstanceID builds the deterministic instance id of a daily task.
func DailyInstanceID(periodID, taskID string) string {
	return "daily/" + periodID + "/" + taskID
}

// GrowthInstanceID builds the deterministic instance id of a growth task.
func GrowthInstanceID(taskID string, definitionVersion uint64) string {
	return "growth/" + taskID + "/" + itoa(definitionVersion)
}

// DomainState is the complete durable TaskDomainState (Q34-08), mapped
// onto model:task by the owning PlayerActor. Every mutation bumps
// TaskStateVersion at most once per call.
//
// Contract 31 §11.7 boundary: this state deliberately carries NO claim
// receipt history, NO participant/source receipt history and NO witness
// sets beyond the per-instance first witness. Same-operation replay,
// payload-hash conflicts and StoredOperationOutcome are handled by the
// PlayerActor's unified receipt:op:<operation_digest> layer before the
// pure domain is ever called; Harvest/Care/Theft source idempotency is
// handled by the existing Participant Receipt at the same authority
// commit boundary. model:task only stores bounded task truth, cumulative
// indicators, the version and the red-point count.
type DomainState struct {
	DailyPeriodID      string
	DailyConfigVersion uint64
	DailyTasks         [DailySlotCount]Instance
	GrowthCursor       int
	GrowthChainVersion uint64
	GrowthActive       bool
	CurrentGrowth      *Instance
	TotalHarvestCount  int64
	HighestLevel       int32
	TaskStateVersion   uint64
	ClaimableCount     int32
}

// InstanceView is the read model of one instance for TaskListView.
type InstanceView struct {
	InstanceID        string
	TaskID            string
	Category          Category
	Kind              DailyKind
	DefinitionVersion uint64
	PeriodID          string
	ConfigVersion     uint64
	Target            int64
	Progress          int64
	State             State
	Reward            RewardBundle
}

func viewOf(in *Instance) InstanceView {
	return InstanceView{
		InstanceID:        in.InstanceID,
		TaskID:            in.TaskID,
		Category:          in.Category,
		Kind:              in.Kind,
		DefinitionVersion: in.DefinitionVersion,
		PeriodID:          in.PeriodID,
		ConfigVersion:     in.ConfigVersion,
		Target:            in.Target,
		Progress:          in.Progress,
		State:             in.State,
		Reward:            in.Reward,
	}
}

// Summary is the authoritative red-point source (Q34-08): the projection
// is a rebuildable copy of these fields and never participates in
// completion or claim adjudication.
type Summary struct {
	DailyPeriodID    string
	NextResetAtMs    int64
	TaskStateVersion uint64
	ClaimableCount   int32
}

// ListView is the fixed P0 task list: 3 daily tasks + the current growth
// task (nil after the chain ends). No pagination.
type ListView struct {
	Summary
	DailyTasks     [DailySlotCount]InstanceView
	CurrentGrowth  *InstanceView
	DailyConfigVer uint64
}

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}
