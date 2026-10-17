// Package readiness owns the bounded farm-worker Family capability registry.
// It deliberately records only low-cardinality dependency and lifecycle
// facts; business identifiers and raw error strings do not enter this API.
package readiness

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

const (
	// ContractVersion versions both the readiness policy and the freshness
	// interpretation exposed by /readyz.
	ContractVersion = "worker-family-readiness-v1"
	MaxFamilies     = 7
)

type Family string

const (
	FamilyPlant            Family = "PLANT"
	FamilyHarvest          Family = "HARVEST"
	FamilyCare             Family = "CARE"
	FamilyTheft            Family = "THEFT"
	FamilyFriendProjection Family = "FRIEND_PROJECTION"
	FamilyGuardSync        Family = "GUARD_SYNC"
	FamilyMail             Family = "MAIL"
)

var allFamilies = [...]Family{
	FamilyPlant,
	FamilyHarvest,
	FamilyCare,
	FamilyTheft,
	FamilyFriendProjection,
	FamilyGuardSync,
	FamilyMail,
}

// AllFamilies returns a copy of the fixed registry key set so callers cannot
// mutate the package's low-cardinality boundary.
func AllFamilies() []Family {
	return append([]Family(nil), allFamilies[:]...)
}

type State string

const (
	StateDisabled State = "DISABLED"
	StateReady    State = "READY"
	StateDegraded State = "DEGRADED"
	StateFailed   State = "FAILED"
)

var (
	ErrInvalidConfig = errors.New("worker readiness: invalid configuration")
	ErrUnknownFamily = errors.New("worker readiness: unknown family")
)

type FamilyConfig struct {
	Family             Family
	Enabled            bool
	Critical           bool
	FreshnessThreshold time.Duration
	// RequireDueProof makes an unavailable depth or oldest-due observation a
	// DEGRADED state. Production critical Families set this true.
	RequireDueProof bool
}

type Dependencies struct {
	RepositoryInitialized   bool
	RouteProviderUsable     bool
	ParticipantClientUsable bool
}

// DueObservation is intentionally explicit about proof availability. An
// empty, proven DueIndex has Depth=0 and both availability fields true. A
// repository without a bounded proof sets the corresponding field false.
type DueObservation struct {
	Depth                 int64
	DepthAvailable        bool
	OldestDueAt           time.Time
	OldestDueAgeAvailable bool
}

type DispatchObservation struct {
	DirectDispatchHits uint64
	WorkerTakeovers    uint64
	Available          bool
}

type familyRecord struct {
	config FamilyConfig

	dependencies     Dependencies
	loopGeneration   uint64
	loopAlive        bool
	schedulerHealthy bool

	lastAttempt    time.Time
	lastSuccess    time.Time
	lastErrorClass string
	due            DueObservation
	dispatch       DispatchObservation
}

type Registry struct {
	mu      sync.RWMutex
	records map[Family]*familyRecord
	now     func() time.Time
}

func NewRegistry(configs []FamilyConfig, now func() time.Time) (*Registry, error) {
	if len(configs) != MaxFamilies {
		return nil, fmt.Errorf("%w: exactly %d Family entries are required", ErrInvalidConfig, MaxFamilies)
	}
	if now == nil {
		now = time.Now
	}
	want := make(map[Family]struct{}, MaxFamilies)
	for _, family := range allFamilies {
		want[family] = struct{}{}
	}
	records := make(map[Family]*familyRecord, MaxFamilies)
	for _, config := range configs {
		if _, ok := want[config.Family]; !ok {
			return nil, fmt.Errorf("%w: %q", ErrUnknownFamily, config.Family)
		}
		if _, duplicate := records[config.Family]; duplicate {
			return nil, fmt.Errorf("%w: duplicate Family %s", ErrInvalidConfig, config.Family)
		}
		if config.Enabled && config.FreshnessThreshold <= 0 {
			return nil, fmt.Errorf("%w: %s freshness threshold", ErrInvalidConfig, config.Family)
		}
		records[config.Family] = &familyRecord{config: config}
	}
	return &Registry{records: records, now: now}, nil
}

func (r *Registry) SetDependencies(family Family, dependencies Dependencies) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	record, err := r.record(family)
	if err != nil {
		return err
	}
	record.dependencies = dependencies
	return nil
}

// LoopHandle fences lifecycle updates by generation. An old goroutine that
// exits after a replacement starts cannot mark the replacement dead or
// overwrite its newer scan evidence.
type LoopHandle struct {
	registry   *Registry
	family     Family
	generation uint64
}

func (r *Registry) StartLoop(family Family) (*LoopHandle, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	record, err := r.record(family)
	if err != nil {
		return nil, err
	}
	if !record.config.Enabled {
		return nil, fmt.Errorf("%w: disabled Family %s cannot start", ErrInvalidConfig, family)
	}
	record.loopGeneration++
	record.loopAlive = true
	record.schedulerHealthy = false
	record.lastErrorClass = ""
	return &LoopHandle{registry: r, family: family, generation: record.loopGeneration}, nil
}

func (h *LoopHandle) Family() Family {
	if h == nil {
		return ""
	}
	return h.family
}

func (h *LoopHandle) ScanStarted(at time.Time) bool {
	if h == nil || h.registry == nil {
		return false
	}
	if at.IsZero() {
		at = h.registry.now()
	}
	h.registry.mu.Lock()
	defer h.registry.mu.Unlock()
	record, ok := h.registry.current(h)
	if !ok {
		return false
	}
	record.lastAttempt = at
	return true
}

func (h *LoopHandle) ScanSucceeded(at time.Time, due DueObservation) bool {
	if h == nil || h.registry == nil || (due.DepthAvailable && due.Depth < 0) {
		return false
	}
	if at.IsZero() {
		at = h.registry.now()
	}
	h.registry.mu.Lock()
	defer h.registry.mu.Unlock()
	record, ok := h.registry.current(h)
	if !ok {
		return false
	}
	record.lastAttempt = at
	record.lastSuccess = at
	record.lastErrorClass = ""
	record.schedulerHealthy = true
	record.due = due
	return true
}

func (h *LoopHandle) ScanFailed(at time.Time, errorClass string) bool {
	if h == nil || h.registry == nil {
		return false
	}
	if at.IsZero() {
		at = h.registry.now()
	}
	h.registry.mu.Lock()
	defer h.registry.mu.Unlock()
	record, ok := h.registry.current(h)
	if !ok {
		return false
	}
	record.lastAttempt = at
	record.lastErrorClass = boundedClass(errorClass, "SCAN_FAILED")
	record.schedulerHealthy = false
	record.due = DueObservation{}
	return true
}

func (h *LoopHandle) Stop(errorClass string) bool {
	if h == nil || h.registry == nil {
		return false
	}
	h.registry.mu.Lock()
	defer h.registry.mu.Unlock()
	record, ok := h.registry.current(h)
	if !ok {
		return false
	}
	record.loopAlive = false
	record.schedulerHealthy = false
	record.lastErrorClass = boundedClass(errorClass, "LOOP_EXITED")
	return true
}

func (r *Registry) SetDispatchObservation(family Family, observation DispatchObservation) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	record, err := r.record(family)
	if err != nil {
		return err
	}
	record.dispatch = observation
	return nil
}

type FamilySnapshot struct {
	Family                    Family  `json:"family"`
	State                     State   `json:"state"`
	Enabled                   bool    `json:"enabled"`
	Critical                  bool    `json:"critical"`
	RepositoryInitialized     bool    `json:"repository_initialized"`
	RouteProviderUsable       bool    `json:"route_provider_usable"`
	ParticipantClientUsable   bool    `json:"participant_client_usable"`
	ScanLoopAlive             bool    `json:"scan_loop_alive"`
	WorkLeaseSchedulerHealthy bool    `json:"work_lease_scheduler_healthy"`
	FreshnessThresholdMs      int64   `json:"freshness_threshold_ms"`
	LastAttemptAtMs           int64   `json:"last_attempt_at_ms"`
	LastSuccessAtMs           int64   `json:"last_success_at_ms"`
	LastSuccessAgeMs          int64   `json:"last_success_age_ms"`
	LastErrorClass            string  `json:"last_error_class"`
	DueDepth                  int64   `json:"due_depth"`
	DueDepthAvailable         bool    `json:"due_depth_available"`
	OldestDueAgeMs            int64   `json:"oldest_due_age_ms"`
	OldestDueAgeAvailable     bool    `json:"oldest_due_age_available"`
	DirectDispatchHitRatio    float64 `json:"direct_dispatch_hit_ratio"`
	WorkerTakeoverRatio       float64 `json:"worker_takeover_ratio"`
	DispatchRatioAvailable    bool    `json:"dispatch_ratio_available"`
}

type Snapshot struct {
	Version      string           `json:"version"`
	Ready        bool             `json:"ready"`
	State        State            `json:"state"`
	ObservedAtMs int64            `json:"observed_at_ms"`
	Families     []FamilySnapshot `json:"families"`
}

func (r *Registry) Snapshot() Snapshot { return r.SnapshotAt(r.now()) }

func (r *Registry) SnapshotAt(now time.Time) Snapshot {
	if now.IsZero() {
		now = r.now()
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	snapshot := Snapshot{Version: ContractVersion, Ready: true, State: StateReady, ObservedAtMs: now.UnixMilli(), Families: make([]FamilySnapshot, 0, MaxFamilies)}
	for _, family := range allFamilies {
		record := r.records[family]
		item := snapshotFamily(record, now)
		snapshot.Families = append(snapshot.Families, item)
		if !item.Enabled || !item.Critical || item.State == StateReady {
			continue
		}
		snapshot.Ready = false
		if item.State == StateFailed {
			snapshot.State = StateFailed
		} else if snapshot.State != StateFailed {
			snapshot.State = StateDegraded
		}
	}
	return snapshot
}

func snapshotFamily(record *familyRecord, now time.Time) FamilySnapshot {
	item := FamilySnapshot{
		Family: record.config.Family, Enabled: record.config.Enabled, Critical: record.config.Critical,
		RepositoryInitialized:   record.dependencies.RepositoryInitialized,
		RouteProviderUsable:     record.dependencies.RouteProviderUsable,
		ParticipantClientUsable: record.dependencies.ParticipantClientUsable,
		ScanLoopAlive:           record.loopAlive, WorkLeaseSchedulerHealthy: record.schedulerHealthy,
		FreshnessThresholdMs: record.config.FreshnessThreshold.Milliseconds(),
		LastErrorClass:       record.lastErrorClass,
		DueDepth:             record.due.Depth, DueDepthAvailable: record.due.DepthAvailable,
		OldestDueAgeAvailable: record.due.OldestDueAgeAvailable,
	}
	if !record.lastAttempt.IsZero() {
		item.LastAttemptAtMs = record.lastAttempt.UnixMilli()
	}
	if !record.lastSuccess.IsZero() {
		item.LastSuccessAtMs = record.lastSuccess.UnixMilli()
		item.LastSuccessAgeMs = nonNegativeDuration(now.Sub(record.lastSuccess)).Milliseconds()
	}
	if record.due.OldestDueAgeAvailable && !record.due.OldestDueAt.IsZero() {
		item.OldestDueAgeMs = nonNegativeDuration(now.Sub(record.due.OldestDueAt)).Milliseconds()
	}
	if record.dispatch.Available {
		total := record.dispatch.DirectDispatchHits + record.dispatch.WorkerTakeovers
		if total != 0 {
			item.DispatchRatioAvailable = true
			item.DirectDispatchHitRatio = float64(record.dispatch.DirectDispatchHits) / float64(total)
			item.WorkerTakeoverRatio = float64(record.dispatch.WorkerTakeovers) / float64(total)
		}
	}
	switch {
	case !record.config.Enabled:
		item.State = StateDisabled
	case !record.dependencies.RepositoryInitialized || !record.dependencies.RouteProviderUsable ||
		!record.dependencies.ParticipantClientUsable || !record.loopAlive:
		item.State = StateFailed
	case !record.schedulerHealthy || record.lastSuccess.IsZero() ||
		now.Sub(record.lastSuccess) > record.config.FreshnessThreshold:
		item.State = StateDegraded
	case record.config.RequireDueProof && (!record.due.DepthAvailable || !record.due.OldestDueAgeAvailable):
		item.State = StateDegraded
	default:
		item.State = StateReady
	}
	return item
}

func (r *Registry) record(family Family) (*familyRecord, error) {
	record := r.records[family]
	if record == nil {
		return nil, fmt.Errorf("%w: %q", ErrUnknownFamily, family)
	}
	return record, nil
}

func (r *Registry) current(handle *LoopHandle) (*familyRecord, bool) {
	record := r.records[handle.family]
	return record, record != nil && record.loopGeneration == handle.generation
}

func boundedClass(value, fallback string) string {
	value = strings.ToUpper(strings.TrimSpace(value))
	if value == "" {
		value = fallback
	}
	if len(value) > 64 {
		value = value[:64]
	}
	bytes := []byte(value)
	for i, char := range bytes {
		if (char < 'A' || char > 'Z') && (char < '0' || char > '9') && char != '_' {
			bytes[i] = '_'
		}
	}
	return string(bytes)
}

func nonNegativeDuration(value time.Duration) time.Duration {
	if value < 0 {
		return 0
	}
	return value
}
