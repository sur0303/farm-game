package routecontrol

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"farmgame/internal/actorrouting"
	"farmgame/internal/holderidentity"
	"farmgame/internal/netendpoint"
)

// HolderState is the process lifecycle state recorded by HolderRegistry.
// REGISTERING is the construction state; a successful Register returns
// WARMING. A live non-draining boot may repeat READY_ZERO_SHARD <-> SERVING
// as shard placement changes; only DRAINING -> DRAINED is terminal.
type HolderState string

const (
	HolderStateRegistering    HolderState = "REGISTERING"
	HolderStateWarming        HolderState = "WARMING"
	HolderStateReadyZeroShard HolderState = "READY_ZERO_SHARD"
	HolderStateServing        HolderState = "SERVING"
	HolderStateDraining       HolderState = "DRAINING"
	HolderStateDrained        HolderState = "DRAINED"
)

var (
	ErrHolderRegistryInvalid         = errors.New("routecontrol: invalid holder registry request")
	ErrHolderRegistryClosed          = errors.New("routecontrol: holder registry is closed")
	ErrHolderNotFound                = errors.New("routecontrol: holder not found")
	ErrHolderBootConflict            = errors.New("routecontrol: holder boot conflict")
	ErrHolderLeaseExpired            = errors.New("routecontrol: holder lease expired")
	ErrHolderHeartbeatStale          = errors.New("routecontrol: stale holder heartbeat")
	ErrHolderHeartbeatReplay         = errors.New("routecontrol: replayed holder heartbeat")
	ErrHolderStateTransition         = errors.New("routecontrol: invalid holder state transition")
	ErrHolderTargetMismatch          = errors.New("routecontrol: holder target mismatch")
	ErrHolderTargetUnavailable       = errors.New("routecontrol: no eligible holder target")
	ErrHolderRegistryCapacity        = errors.New("routecontrol: holder registry capacity exceeded")
	ErrHolderRegistryCorrupt         = errors.New("routecontrol: corrupt holder registry record")
	ErrHolderRegistryUnavailable     = errors.New("routecontrol: holder registry unavailable")
	ErrHolderRegistryAuthorityLeak   = errors.New("routecontrol: holder registry cannot grant authority")
	ErrHolderInitialServingAdmission = errors.New("routecontrol: static initial serving admission rejected")
)

const (
	DefaultHolderLeaseTTL                = 15 * time.Second
	DefaultHolderMinLeaseTTL             = 2 * time.Second
	DefaultHolderMaxLeaseTTL             = 2 * time.Minute
	DefaultHolderClockSkew               = 5 * time.Second
	DefaultHolderMaxIDBytes              = 128
	DefaultHolderMaxEndpointBytes        = 512
	DefaultHolderMaxTLSIdentityBytes     = 253
	DefaultHolderMaxResourceKeyBytes     = 64
	DefaultHolderMaxResourceValueBytes   = 256
	DefaultHolderMaxResourceSummaryBytes = 4096
	DefaultHolderMaxServingShardCount    = 1 << 20
	DefaultHolderMaxCapacityWeight       = 1 << 30
	DefaultHolderMaxHolderCount          = 4096
	HolderResourceSummaryWirePrefix      = "RS1:"
)

func (s HolderState) String() string {
	return string(s)
}

// HolderRecord is the complete registry fact for one holder boot. It is
// deliberately a liveness/capacity record and contains no Authority token,
// writer claim, or mutation capability.
type HolderRecord struct {
	Domain                actorrouting.RouteDomain          `json:"domain"`
	HolderID              string                            `json:"holder_id"`
	BootID                string                            `json:"boot_id"`
	DeploymentIdentity    holderidentity.DeploymentIdentity `json:"deployment_identity,omitempty"`
	LogicEndpoint         string                            `json:"logic_endpoint"`
	HolderControlEndpoint string                            `json:"holder_control_endpoint"`
	TLSDNSIdentity        string                            `json:"tls_dns_identity"`
	State                 HolderState                       `json:"state"`
	ProcessReady          bool                              `json:"process_ready"`
	Draining              bool                              `json:"draining"`
	ServingShardCount     uint32                            `json:"serving_shard_count"`
	SnapshotRevision      uint64                            `json:"snapshot_revision"`
	StartedAt             time.Time                         `json:"started_at"`
	HeartbeatAt           time.Time                         `json:"heartbeat_at"`
	LeaseUntil            time.Time                         `json:"lease_until"`
	Sequence              uint64                            `json:"sequence"`
	CapacityWeight        uint32                            `json:"capacity_weight"`
	ResourceSummary       map[string]string                 `json:"resource_summary"`
}

// HolderRegistration is the initial record submitted after the internal
// server has started. The zero State is normalized to WARMING.
type HolderRegistration = HolderRecord

// HolderHeartbeat carries only the mutable liveness/status portion of a
// holder record. Endpoint and TLS identity are immutable for a boot.
type HolderHeartbeat struct {
	Domain            actorrouting.RouteDomain `json:"domain"`
	HolderID          string                   `json:"holder_id"`
	BootID            string                   `json:"boot_id"`
	State             HolderState              `json:"state"`
	ProcessReady      bool                     `json:"process_ready"`
	Draining          bool                     `json:"draining"`
	ServingShardCount uint32                   `json:"serving_shard_count"`
	SnapshotRevision  uint64                   `json:"snapshot_revision"`
	HeartbeatAt       time.Time                `json:"heartbeat_at"`
	LeaseUntil        time.Time                `json:"lease_until"`
	Sequence          uint64                   `json:"sequence"`
	CapacityWeight    uint32                   `json:"capacity_weight"`
	ResourceSummary   map[string]string        `json:"resource_summary"`
}

// HolderTarget identifies the exact holder identity a controller is willing
// to use as a target. It is not an authority fence.
type HolderTarget struct {
	Domain                actorrouting.RouteDomain
	HolderID              string
	BootID                string
	LogicEndpoint         string
	HolderControlEndpoint string
	TLSDNSIdentity        string
	MinimumCapacityWeight uint32
}

// TargetPredicate is an explicit alias used by controller callers.
type TargetPredicate = HolderTarget

// HolderRegistryStore is the narrow durable backend used by HolderRegistry.
// Implementations must make RegisterHolder/HeartbeatHolder atomic with their
// own record and bounded index updates.
type HolderRegistryStore interface {
	RegisterHolder(context.Context, HolderRegistration) (HolderRecord, error)
	HeartbeatHolder(context.Context, HolderHeartbeat) (HolderRecord, error)
	GetHolder(context.Context, actorrouting.RouteDomain, string) (HolderRecord, bool, error)
	ListHolders(context.Context, actorrouting.RouteDomain) ([]HolderRecord, error)
	DeleteHolderForTest(context.Context, actorrouting.RouteDomain, string) error
	ExistsHolderForTest(context.Context, actorrouting.RouteDomain, string) (bool, error)
}

// holderHeartbeatCurrentStore is an optional narrow read used only by the
// heartbeat preflight. A durable backend may implement it when the heartbeat
// mutation itself atomically validates the complete registry. Ordinary Get
// and List retain their stable, revision-fenced full-registry semantics.
type holderHeartbeatCurrentStore interface {
	GetHolderForHeartbeat(context.Context, actorrouting.RouteDomain, string) (HolderRecord, bool, error)
}

// InitialServingVerifier is the server-side authorization seam for entering
// SERVING in a static-bootstrap registry. When configured it covers both
// WARMING and READY_ZERO_SHARD sources, preventing a transient zero-shard
// startup from bypassing static proof. With no verifier WARMING -> SERVING is
// denied while READY_ZERO_SHARD -> SERVING remains the dynamic default.
type InitialServingVerifier func(context.Context, HolderRecord, HolderHeartbeat) error

type HolderRegistryOptions struct {
	Now                         func() time.Time
	LeaseTTL                    time.Duration
	MinLeaseTTL                 time.Duration
	MaxLeaseTTL                 time.Duration
	ClockSkew                   time.Duration
	MaxHolderCount              int
	MaxIDBytes                  int
	MaxEndpointBytes            int
	MaxTLSIdentityBytes         int
	MaxResourceKeyBytes         int
	MaxResourceValueBytes       int
	MaxResourceSummaryBytes     int
	MaxServingShardCount        uint32
	MaxCapacityWeight           uint32
	TargetMinimumCapacityWeight uint32
	InitialServingVerifier      InitialServingVerifier
}

func DefaultHolderRegistryOptions() HolderRegistryOptions {
	return HolderRegistryOptions{
		Now:                         time.Now,
		LeaseTTL:                    DefaultHolderLeaseTTL,
		MinLeaseTTL:                 DefaultHolderMinLeaseTTL,
		MaxLeaseTTL:                 DefaultHolderMaxLeaseTTL,
		ClockSkew:                   DefaultHolderClockSkew,
		MaxHolderCount:              DefaultHolderMaxHolderCount,
		MaxIDBytes:                  DefaultHolderMaxIDBytes,
		MaxEndpointBytes:            DefaultHolderMaxEndpointBytes,
		MaxTLSIdentityBytes:         DefaultHolderMaxTLSIdentityBytes,
		MaxResourceKeyBytes:         DefaultHolderMaxResourceKeyBytes,
		MaxResourceValueBytes:       DefaultHolderMaxResourceValueBytes,
		MaxResourceSummaryBytes:     DefaultHolderMaxResourceSummaryBytes,
		MaxServingShardCount:        DefaultHolderMaxServingShardCount,
		MaxCapacityWeight:           DefaultHolderMaxCapacityWeight,
		TargetMinimumCapacityWeight: 1,
	}
}

func (o HolderRegistryOptions) normalized() (HolderRegistryOptions, error) {
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.LeaseTTL <= 0 {
		o.LeaseTTL = DefaultHolderLeaseTTL
	}
	if o.MinLeaseTTL <= 0 {
		o.MinLeaseTTL = DefaultHolderMinLeaseTTL
	}
	if o.MaxLeaseTTL <= 0 {
		o.MaxLeaseTTL = DefaultHolderMaxLeaseTTL
	}
	if o.ClockSkew == 0 {
		o.ClockSkew = DefaultHolderClockSkew
	}
	if o.ClockSkew < 0 {
		return HolderRegistryOptions{}, fmt.Errorf("%w: negative clock skew", ErrHolderRegistryInvalid)
	}
	if o.MaxHolderCount <= 0 {
		o.MaxHolderCount = DefaultHolderMaxHolderCount
	}
	if o.MaxIDBytes <= 0 {
		o.MaxIDBytes = DefaultHolderMaxIDBytes
	}
	if o.MaxEndpointBytes <= 0 {
		o.MaxEndpointBytes = DefaultHolderMaxEndpointBytes
	}
	if o.MaxTLSIdentityBytes <= 0 {
		o.MaxTLSIdentityBytes = DefaultHolderMaxTLSIdentityBytes
	}
	if o.MaxResourceKeyBytes <= 0 {
		o.MaxResourceKeyBytes = DefaultHolderMaxResourceKeyBytes
	}
	if o.MaxResourceValueBytes <= 0 {
		o.MaxResourceValueBytes = DefaultHolderMaxResourceValueBytes
	}
	if o.MaxResourceSummaryBytes <= 0 {
		o.MaxResourceSummaryBytes = DefaultHolderMaxResourceSummaryBytes
	}
	if o.MaxServingShardCount == 0 {
		o.MaxServingShardCount = DefaultHolderMaxServingShardCount
	}
	if o.MaxCapacityWeight == 0 {
		o.MaxCapacityWeight = DefaultHolderMaxCapacityWeight
	}
	if o.MinLeaseTTL > o.LeaseTTL || o.LeaseTTL > o.MaxLeaseTTL {
		return HolderRegistryOptions{}, fmt.Errorf("%w: lease bounds", ErrHolderRegistryInvalid)
	}
	for name, value := range map[string]time.Duration{
		"lease ttl": o.LeaseTTL, "minimum lease ttl": o.MinLeaseTTL,
		"maximum lease ttl": o.MaxLeaseTTL, "clock skew": o.ClockSkew,
	} {
		if value < time.Millisecond {
			return HolderRegistryOptions{}, fmt.Errorf("%w: %s must be at least 1ms", ErrHolderRegistryInvalid, name)
		}
	}
	return o, nil
}

// HolderRegistry validates the logical contract around a durable holder
// backend. It never exposes or grants a write-authority operation.
type HolderRegistry struct {
	store      HolderRegistryStore
	placements PlacementExclusionStore
	opts       HolderRegistryOptions

	mu     sync.RWMutex
	closed bool
}

type Registry = HolderRegistry

func NewHolderRegistry(store HolderRegistryStore, opts HolderRegistryOptions) (*HolderRegistry, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: store required", ErrHolderRegistryInvalid)
	}
	normalized, err := opts.normalized()
	if err != nil {
		return nil, err
	}
	placements, ok := store.(PlacementExclusionStore)
	if !ok || placements == nil {
		return nil, fmt.Errorf("%w: placement exclusion store required", ErrHolderRegistryInvalid)
	}
	return &HolderRegistry{store: store, placements: placements, opts: normalized}, nil
}

func NewRegistry(store HolderRegistryStore, opts HolderRegistryOptions) (*HolderRegistry, error) {
	return NewHolderRegistry(store, opts)
}

func (r *HolderRegistry) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
	return nil
}

func (r *HolderRegistry) checkOpen() error {
	if r == nil || r.store == nil {
		return ErrHolderRegistryInvalid
	}
	r.mu.RLock()
	closed := r.closed
	r.mu.RUnlock()
	if closed {
		return ErrHolderRegistryClosed
	}
	return nil
}

func (r *HolderRegistry) Register(ctx context.Context, input HolderRegistration) (HolderRecord, error) {
	if err := r.checkOpen(); err != nil {
		return HolderRecord{}, err
	}
	if ctx == nil {
		return HolderRecord{}, fmt.Errorf("%w: context required", ErrHolderRegistryInvalid)
	}
	input = cloneHolderRecord(input)
	now := r.opts.Now()
	if input.State == "" {
		input.State = HolderStateWarming
	}
	if input.State != HolderStateWarming {
		return HolderRecord{}, fmt.Errorf("%w: Register must enter WARMING", ErrHolderStateTransition)
	}
	if input.StartedAt.IsZero() {
		input.StartedAt = now
	}
	if input.HeartbeatAt.IsZero() {
		input.HeartbeatAt = now
	}
	if input.LeaseUntil.IsZero() {
		input.LeaseUntil = now.Add(r.opts.LeaseTTL)
	}
	if input.Sequence == 0 {
		input.Sequence = 1
	}
	if input.CapacityWeight == 0 {
		input.CapacityWeight = 1
	}
	if err := r.validateRegistration(input, now); err != nil {
		return HolderRecord{}, err
	}
	previous, found, err := r.store.GetHolder(ctx, input.Domain, input.HolderID)
	if err != nil {
		return HolderRecord{}, err
	}
	if found && previous.BootID != input.BootID && !leaseExpired(previous, now) {
		return HolderRecord{}, fmt.Errorf("%w: %s/%s", ErrHolderBootConflict, input.Domain, input.HolderID)
	}
	if found && previous.BootID == input.BootID && previous.State == HolderStateDrained {
		return HolderRecord{}, fmt.Errorf("%w: drained boot cannot be revived", ErrHolderStateTransition)
	}
	if found && previous.BootID == input.BootID {
		if input.HeartbeatAt.Before(previous.HeartbeatAt) {
			return HolderRecord{}, fmt.Errorf("%w: registration heartbeat time regressed", ErrHolderHeartbeatStale)
		}
		if input.LeaseUntil.Before(previous.LeaseUntil) {
			return HolderRecord{}, fmt.Errorf("%w: registration lease regressed", ErrHolderHeartbeatStale)
		}
	}
	record, err := r.store.RegisterHolder(ctx, input)
	if err != nil {
		return HolderRecord{}, err
	}
	if err := r.validateRecord(record, now); err != nil {
		return HolderRecord{}, err
	}
	if !HolderRecordEqual(record, input) {
		return HolderRecord{}, fmt.Errorf("%w: store returned a different registration candidate", ErrHolderRegistryCorrupt)
	}
	if found {
		if previous.BootID == record.BootID && previous.State != record.State {
			if !validHolderStateTransition(previous.State, record.State) {
				return HolderRecord{}, fmt.Errorf("%w: %s -> %s", ErrHolderStateTransition, previous.State, record.State)
			}
		}
	} else if record.State != HolderStateWarming {
		return HolderRecord{}, fmt.Errorf("%w: initial state=%s", ErrHolderStateTransition, record.State)
	}
	return cloneHolderRecord(record), nil
}

func (r *HolderRegistry) RegisterHolder(ctx context.Context, input HolderRegistration) (HolderRecord, error) {
	return r.Register(ctx, input)
}

func (r *HolderRegistry) Heartbeat(ctx context.Context, input HolderHeartbeat) (HolderRecord, error) {
	if err := r.checkOpen(); err != nil {
		return HolderRecord{}, err
	}
	if ctx == nil {
		return HolderRecord{}, fmt.Errorf("%w: context required", ErrHolderRegistryInvalid)
	}
	input = cloneHolderHeartbeat(input)
	now := r.opts.Now()
	if input.HeartbeatAt.IsZero() {
		input.HeartbeatAt = now
	}
	if input.LeaseUntil.IsZero() {
		input.LeaseUntil = input.HeartbeatAt.Add(r.opts.LeaseTTL)
	}
	if err := r.validateHeartbeat(input, now); err != nil {
		return HolderRecord{}, err
	}
	previous, found, err := r.getHolderForHeartbeat(ctx, input.Domain, input.HolderID)
	if err != nil {
		return HolderRecord{}, err
	}
	if !found {
		return HolderRecord{}, ErrHolderNotFound
	}
	if previous.BootID != input.BootID {
		return HolderRecord{}, fmt.Errorf("%w: heartbeat boot mismatch", ErrHolderBootConflict)
	}
	if previous.State == HolderStateDrained {
		return HolderRecord{}, fmt.Errorf("%w: drained boot cannot heartbeat", ErrHolderStateTransition)
	}
	if previous.LeaseUntil.Before(now) || previous.LeaseUntil.Equal(now) {
		return HolderRecord{}, ErrHolderLeaseExpired
	}
	if input.Sequence <= previous.Sequence {
		if input.Sequence == previous.Sequence {
			return HolderRecord{}, ErrHolderHeartbeatReplay
		}
		return HolderRecord{}, ErrHolderHeartbeatStale
	}
	if input.SnapshotRevision < previous.SnapshotRevision {
		return HolderRecord{}, fmt.Errorf("%w: snapshot revision regressed", ErrHolderHeartbeatStale)
	}
	// Membership removal completes asynchronously after one RouteSnapshot is
	// installed. The Agent can therefore first report SERVING at revision N and
	// then truthfully report READY_ZERO_SHARD at the same revision N after the
	// last local guard is removed. Requiring N+1 here creates a global snapshot
	// barrier which can never be satisfied by an otherwise quiescent scale-in.
	// The candidate still has to prove the exact boot, monotonic revision,
	// ProcessReady, non-draining state, and zero serving shards. ScalePlan also
	// revalidates that no route is placed on this holder before deleting it.
	if input.HeartbeatAt.Before(previous.HeartbeatAt) {
		return HolderRecord{}, fmt.Errorf("%w: heartbeat time regressed", ErrHolderHeartbeatStale)
	}
	if input.LeaseUntil.Before(previous.LeaseUntil) {
		return HolderRecord{}, fmt.Errorf("%w: lease regressed", ErrHolderHeartbeatStale)
	}
	candidate := previous
	candidate.State = input.State
	candidate.ProcessReady = input.ProcessReady
	candidate.Draining = input.Draining
	candidate.ServingShardCount = input.ServingShardCount
	candidate.SnapshotRevision = input.SnapshotRevision
	candidate.HeartbeatAt = input.HeartbeatAt
	candidate.LeaseUntil = input.LeaseUntil
	candidate.Sequence = input.Sequence
	candidate.CapacityWeight = input.CapacityWeight
	candidate.ResourceSummary = cloneResourceSummary(input.ResourceSummary)
	if err := r.validateCandidateRecord(candidate, now); err != nil {
		return HolderRecord{}, err
	}
	if !validHolderStateTransition(previous.State, input.State) {
		return HolderRecord{}, fmt.Errorf("%w: %s -> %s", ErrHolderStateTransition, previous.State, input.State)
	}
	initialServing := previous.State != HolderStateServing && input.State == HolderStateServing
	if initialServing {
		switch {
		case r.opts.InitialServingVerifier != nil:
			if err := r.opts.InitialServingVerifier(ctx, cloneHolderRecord(previous), cloneHolderHeartbeat(input)); err != nil {
				return HolderRecord{}, fmt.Errorf("%w: %v", ErrHolderInitialServingAdmission, err)
			}
		case previous.State == HolderStateWarming:
			return HolderRecord{}, fmt.Errorf("%w: verifier is not configured", ErrHolderInitialServingAdmission)
		}
	}
	record, err := r.store.HeartbeatHolder(ctx, input)
	if err != nil {
		return HolderRecord{}, err
	}
	if err := r.validateRecord(record, now); err != nil {
		return HolderRecord{}, err
	}
	if !HolderRecordEqual(record, candidate) {
		return HolderRecord{}, fmt.Errorf("%w: heartbeat response differs from candidate", ErrHolderRegistryCorrupt)
	}
	if !validHolderStateTransition(previous.State, record.State) {
		return HolderRecord{}, fmt.Errorf("%w: store returned illegal state", ErrHolderStateTransition)
	}
	// Exceptional static admission is checked again after the durable commit.
	// This cannot make Redis and process-local guards one transaction, but it
	// detects a concurrent fence/gate change and forces HolderAgent into its
	// bounded fail-closed reconciliation/drain path. HolderRegistry remains a
	// liveness directory and never grants data-plane authority.
	if initialServing && r.opts.InitialServingVerifier != nil {
		if err := r.opts.InitialServingVerifier(ctx, cloneHolderRecord(previous), cloneHolderHeartbeat(input)); err != nil {
			return HolderRecord{}, fmt.Errorf("%w: post-commit verification: %v", ErrHolderInitialServingAdmission, err)
		}
	}
	return cloneHolderRecord(record), nil
}

func (r *HolderRegistry) getHolderForHeartbeat(ctx context.Context, domain actorrouting.RouteDomain, holderID string) (HolderRecord, bool, error) {
	if store, ok := r.store.(holderHeartbeatCurrentStore); ok {
		return store.GetHolderForHeartbeat(ctx, domain, holderID)
	}
	return r.store.GetHolder(ctx, domain, holderID)
}

func (r *HolderRegistry) HeartbeatHolder(ctx context.Context, input HolderHeartbeat) (HolderRecord, error) {
	return r.Heartbeat(ctx, input)
}

func (r *HolderRegistry) Get(ctx context.Context, domain actorrouting.RouteDomain, holderID string) (HolderRecord, bool, error) {
	if err := r.checkOpen(); err != nil {
		return HolderRecord{}, false, err
	}
	if ctx == nil {
		return HolderRecord{}, false, fmt.Errorf("%w: context required", ErrHolderRegistryInvalid)
	}
	if err := validateDomain(domain); err != nil {
		return HolderRecord{}, false, err
	}
	if err := validateComponent("holder_id", holderID, r.opts.MaxIDBytes); err != nil {
		return HolderRecord{}, false, err
	}
	record, found, err := r.store.GetHolder(ctx, domain, holderID)
	if err != nil || !found {
		return record, found, err
	}
	if err := r.validateRecord(record, r.opts.Now()); err != nil {
		return HolderRecord{}, true, err
	}
	return cloneHolderRecord(record), true, nil
}

func (r *HolderRegistry) GetHolder(ctx context.Context, domain actorrouting.RouteDomain, holderID string) (HolderRecord, bool, error) {
	return r.Get(ctx, domain, holderID)
}

func (r *HolderRegistry) List(ctx context.Context, domain actorrouting.RouteDomain) ([]HolderRecord, error) {
	if err := r.checkOpen(); err != nil {
		return nil, err
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: context required", ErrHolderRegistryInvalid)
	}
	if err := validateDomain(domain); err != nil {
		return nil, err
	}
	records, err := r.store.ListHolders(ctx, domain)
	if err != nil {
		return nil, err
	}
	if len(records) > r.opts.MaxHolderCount {
		return nil, fmt.Errorf("%w: count=%d", ErrHolderRegistryCapacity, len(records))
	}
	now := r.opts.Now()
	result := make([]HolderRecord, 0, len(records))
	for _, record := range records {
		if err := r.validateRecord(record, now); err != nil {
			return nil, err
		}
		if record.Domain != domain {
			return nil, fmt.Errorf("%w: list returned domain=%s", ErrHolderRegistryCorrupt, record.Domain)
		}
		result = append(result, cloneHolderRecord(record))
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].HolderID == result[j].HolderID {
			return result[i].BootID < result[j].BootID
		}
		return result[i].HolderID < result[j].HolderID
	})
	return result, nil
}

func (r *HolderRegistry) ListHolders(ctx context.Context, domain actorrouting.RouteDomain) ([]HolderRecord, error) {
	return r.List(ctx, domain)
}

func (r *HolderRegistry) FindTarget(ctx context.Context, target HolderTarget) (HolderRecord, error) {
	if err := r.checkOpen(); err != nil {
		return HolderRecord{}, err
	}
	records, err := r.List(ctx, target.Domain)
	if err != nil {
		return HolderRecord{}, err
	}
	now := r.opts.Now()
	if target.MinimumCapacityWeight == 0 {
		target.MinimumCapacityWeight = r.opts.TargetMinimumCapacityWeight
	}
	for _, record := range records {
		if !record.MatchesTarget(target, now) {
			continue
		}
		if _, err := r.ValidatePlacementTarget(ctx, record.Domain, record.HolderID, record.BootID); err != nil {
			if errors.Is(err, ErrPlacementExcluded) {
				continue
			}
			return HolderRecord{}, err
		}
		return record, nil
	}
	return HolderRecord{}, ErrHolderTargetUnavailable
}

func (r *HolderRegistry) Target(ctx context.Context, target HolderTarget) (HolderRecord, error) {
	return r.FindTarget(ctx, target)
}

func (r *HolderRegistry) GetPlacementExclusion(ctx context.Context, domain actorrouting.RouteDomain, holderID, bootID string) (PlacementExclusion, bool, error) {
	if err := r.checkOpen(); err != nil {
		return PlacementExclusion{}, false, err
	}
	if ctx == nil {
		return PlacementExclusion{}, false, fmt.Errorf("%w: context required", ErrPlacementExclusionInvalid)
	}
	if err := validateDomain(domain); err != nil {
		return PlacementExclusion{}, false, fmt.Errorf("%w: %v", ErrPlacementExclusionInvalid, err)
	}
	if err := validateComponent("holder_id", holderID, r.opts.MaxIDBytes); err != nil {
		return PlacementExclusion{}, false, fmt.Errorf("%w: %v", ErrPlacementExclusionInvalid, err)
	}
	if err := validateComponent("boot_id", bootID, r.opts.MaxIDBytes); err != nil {
		return PlacementExclusion{}, false, fmt.Errorf("%w: %v", ErrPlacementExclusionInvalid, err)
	}
	state, found, err := r.placements.GetPlacementExclusion(ctx, domain, holderID, bootID)
	if err != nil || !found {
		return state, found, err
	}
	if err := validatePlacementExclusion(state, r.opts.MaxIDBytes); err != nil || state.Domain != domain || state.HolderID != holderID || state.BootID != bootID {
		if err == nil {
			err = ErrPlacementExclusionInvalid
		}
		return PlacementExclusion{}, true, fmt.Errorf("%w: store returned invalid exact state: %v", ErrHolderRegistryCorrupt, err)
	}
	return state, true, nil
}

func (r *HolderRegistry) ExcludePlacement(ctx context.Context, domain actorrouting.RouteDomain, holderID, bootID string, precondition PlacementExclusionPrecondition) (PlacementExclusionResult, error) {
	if err := r.checkOpen(); err != nil {
		return PlacementExclusionResult{}, err
	}
	if ctx == nil {
		return PlacementExclusionResult{}, fmt.Errorf("%w: context required", ErrPlacementExclusionInvalid)
	}
	if err := validatePlacementExclusionPrecondition(precondition, r.opts.MaxIDBytes); err != nil {
		return PlacementExclusionResult{}, err
	}
	current, found, err := r.GetPlacementExclusion(ctx, domain, holderID, bootID)
	if err != nil || !found {
		if err == nil {
			err = ErrHolderNotFound
		}
		return PlacementExclusionResult{}, err
	}
	if current.Excluded {
		if current.OwnerPlanID == precondition.OwnerPlanID && precondition.ExpectedRevision != ^uint64(0) &&
			current.Revision == precondition.ExpectedRevision+1 {
			return PlacementExclusionResult{State: current}, nil
		}
		return PlacementExclusionResult{}, ErrPlacementExclusionConflict
	}
	if current.Revision != precondition.ExpectedRevision {
		return PlacementExclusionResult{}, ErrPlacementExclusionConflict
	}
	result, err := r.placements.ExcludePlacement(ctx, domain, holderID, bootID, precondition)
	if err != nil {
		return PlacementExclusionResult{}, err
	}
	if err := validatePlacementExclusion(result.State, r.opts.MaxIDBytes); err != nil || !result.State.Excluded ||
		result.State.Domain != domain || result.State.HolderID != holderID || result.State.BootID != bootID ||
		result.State.OwnerPlanID != precondition.OwnerPlanID || result.State.Revision <= precondition.ExpectedRevision {
		if err == nil {
			err = ErrPlacementExclusionInvalid
		}
		return PlacementExclusionResult{}, fmt.Errorf("%w: store returned invalid exclusion: %v", ErrHolderRegistryCorrupt, err)
	}
	return result, nil
}

func (r *HolderRegistry) ValidatePlacementTarget(ctx context.Context, domain actorrouting.RouteDomain, holderID, bootID string) (uint64, error) {
	state, found, err := r.GetPlacementExclusion(ctx, domain, holderID, bootID)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrPlacementExclusionUnavailable, err)
	}
	if !found {
		return 0, fmt.Errorf("%w: exact holder generation absent", ErrPlacementExclusionUnavailable)
	}
	if state.Excluded {
		return state.Revision, fmt.Errorf("%w: owner=%s revision=%d", ErrPlacementExcluded, state.OwnerPlanID, state.Revision)
	}
	return state.Revision, nil
}

func (r *HolderRegistry) DeleteHolderForTest(ctx context.Context, domain actorrouting.RouteDomain, holderID string) error {
	if err := r.checkOpen(); err != nil {
		return err
	}
	return r.store.DeleteHolderForTest(ctx, domain, holderID)
}

func (r *HolderRegistry) ExistsHolderForTest(ctx context.Context, domain actorrouting.RouteDomain, holderID string) (bool, error) {
	if err := r.checkOpen(); err != nil {
		return false, err
	}
	return r.store.ExistsHolderForTest(ctx, domain, holderID)
}

func (r *HolderRegistry) validateRegistration(record HolderRecord, now time.Time) error {
	if err := r.validateCandidateRecord(record, now); err != nil {
		return err
	}
	if record.State != HolderStateWarming {
		return fmt.Errorf("%w: registration state=%s", ErrHolderStateTransition, record.State)
	}
	return nil
}

func (r *HolderRegistry) validateCandidateRecord(record HolderRecord, now time.Time) error {
	if err := r.validateRecord(record, now); err != nil {
		return err
	}
	if record.LeaseUntil.Before(now.Add(r.opts.MinLeaseTTL)) {
		return fmt.Errorf("%w: lease is below minimum horizon", ErrHolderRegistryInvalid)
	}
	return nil
}

func (r *HolderRegistry) validateHeartbeat(input HolderHeartbeat, now time.Time) error {
	if err := validateDomain(input.Domain); err != nil {
		return err
	}
	if err := validateComponent("holder_id", input.HolderID, r.opts.MaxIDBytes); err != nil {
		return err
	}
	if err := validateComponent("boot_id", input.BootID, r.opts.MaxIDBytes); err != nil {
		return err
	}
	if input.State == "" || input.State == HolderStateRegistering {
		return fmt.Errorf("%w: heartbeat state=%s", ErrHolderStateTransition, input.State)
	}
	if input.HeartbeatAt.IsZero() {
		input.HeartbeatAt = now
	}
	if input.LeaseUntil.IsZero() {
		input.LeaseUntil = input.HeartbeatAt.Add(r.opts.LeaseTTL)
	}
	if input.Sequence == 0 {
		return fmt.Errorf("%w: sequence must be positive", ErrHolderRegistryInvalid)
	}
	if input.HeartbeatAt.After(now.Add(r.opts.ClockSkew)) ||
		!input.LeaseUntil.After(input.HeartbeatAt) ||
		input.LeaseUntil.Before(now.Add(r.opts.MinLeaseTTL)) ||
		input.LeaseUntil.After(now.Add(r.opts.MaxLeaseTTL).Add(r.opts.ClockSkew)) {
		return fmt.Errorf("%w: heartbeat lease bounds", ErrHolderRegistryInvalid)
	}
	if input.ServingShardCount > r.opts.MaxServingShardCount ||
		input.CapacityWeight > r.opts.MaxCapacityWeight {
		return fmt.Errorf("%w: heartbeat numeric bounds", ErrHolderRegistryInvalid)
	}
	return validateResourceSummary(input.ResourceSummary, r.opts)
}

func (r *HolderRegistry) validateRecord(record HolderRecord, now time.Time) error {
	if err := validateDomain(record.Domain); err != nil {
		return err
	}
	if err := validateComponent("holder_id", record.HolderID, r.opts.MaxIDBytes); err != nil {
		return err
	}
	if err := validateComponent("boot_id", record.BootID, r.opts.MaxIDBytes); err != nil {
		return err
	}
	if err := record.DeploymentIdentity.ValidateOptional(); err != nil {
		return fmt.Errorf("%w: deployment identity: %v", ErrHolderRegistryInvalid, err)
	}
	if err := netendpoint.Validate(record.LogicEndpoint, r.opts.MaxEndpointBytes); err != nil {
		return fmt.Errorf("%w: logic_endpoint must be canonical host:port", ErrHolderRegistryInvalid)
	}
	if err := netendpoint.Validate(record.HolderControlEndpoint, r.opts.MaxEndpointBytes); err != nil {
		return fmt.Errorf("%w: holder_control_endpoint must be canonical host:port", ErrHolderRegistryInvalid)
	}
	if err := validateTLSIdentity(record.TLSDNSIdentity, r.opts.MaxTLSIdentityBytes); err != nil {
		return err
	}
	switch record.State {
	case HolderStateWarming, HolderStateReadyZeroShard, HolderStateServing,
		HolderStateDraining, HolderStateDrained:
	default:
		return fmt.Errorf("%w: unknown state=%q", ErrHolderRegistryInvalid, record.State)
	}
	if record.ServingShardCount > r.opts.MaxServingShardCount {
		return fmt.Errorf("%w: serving shard count=%d", ErrHolderRegistryInvalid, record.ServingShardCount)
	}
	if record.CapacityWeight > r.opts.MaxCapacityWeight {
		return fmt.Errorf("%w: capacity weight=%d", ErrHolderRegistryInvalid, record.CapacityWeight)
	}
	if record.Sequence == 0 {
		return fmt.Errorf("%w: sequence must be positive", ErrHolderRegistryInvalid)
	}
	if record.StartedAt.IsZero() || record.HeartbeatAt.IsZero() || record.LeaseUntil.IsZero() {
		return fmt.Errorf("%w: lifecycle times are required", ErrHolderRegistryInvalid)
	}
	if record.HeartbeatAt.Before(record.StartedAt) {
		return fmt.Errorf("%w: heartbeat precedes start", ErrHolderRegistryInvalid)
	}
	if record.StartedAt.After(now.Add(r.opts.ClockSkew)) ||
		record.HeartbeatAt.After(now.Add(r.opts.ClockSkew)) {
		return fmt.Errorf("%w: lifecycle time is too far in the future", ErrHolderRegistryInvalid)
	}
	if !record.LeaseUntil.After(record.HeartbeatAt) {
		return fmt.Errorf("%w: lease must exceed heartbeat", ErrHolderRegistryInvalid)
	}
	if record.LeaseUntil.After(now.Add(r.opts.MaxLeaseTTL).Add(r.opts.ClockSkew)) {
		return fmt.Errorf("%w: lease exceeds maximum bound", ErrHolderRegistryInvalid)
	}
	if record.State == HolderStateWarming &&
		(record.ProcessReady || record.Draining || record.ServingShardCount != 0) {
		return fmt.Errorf("%w: WARMING must be process-unready, non-draining, and zero-shard", ErrHolderRegistryInvalid)
	}
	if record.State == HolderStateReadyZeroShard &&
		(!record.ProcessReady || record.ServingShardCount != 0) {
		return fmt.Errorf("%w: READY_ZERO_SHARD must be process-ready and zero-shard", ErrHolderRegistryInvalid)
	}
	if record.State == HolderStateServing &&
		(!record.ProcessReady || record.ServingShardCount == 0) {
		return fmt.Errorf("%w: SERVING must be process-ready with a serving shard", ErrHolderRegistryInvalid)
	}
	if record.State == HolderStateDrained && (record.ServingShardCount != 0 || record.ProcessReady) {
		return fmt.Errorf("%w: DRAINED must be process-unready and zero-shard", ErrHolderRegistryInvalid)
	}
	if record.State == HolderStateDraining && !record.Draining {
		return fmt.Errorf("%w: DRAINING requires draining=true", ErrHolderRegistryInvalid)
	}
	if (record.State == HolderStateWarming || record.State == HolderStateReadyZeroShard ||
		record.State == HolderStateServing) && record.Draining {
		return fmt.Errorf("%w: active lifecycle state cannot be draining", ErrHolderRegistryInvalid)
	}
	if record.State == HolderStateDrained && !record.Draining {
		return fmt.Errorf("%w: DRAINED requires draining=true", ErrHolderRegistryInvalid)
	}
	if err := validateResourceSummary(record.ResourceSummary, r.opts); err != nil {
		return err
	}
	return nil
}

// ValidateHolderRecord validates a persisted record's bounded structure and
// time relationships. It intentionally does not require a minimum remaining
// lease horizon, so a fresh holder near expiry remains readable and renewable.
func ValidateHolderRecord(record HolderRecord, now time.Time, options HolderRegistryOptions) error {
	normalized, err := options.normalized()
	if err != nil {
		return err
	}
	validator := HolderRegistry{opts: normalized}
	return validator.validateRecord(cloneHolderRecord(record), now)
}

// ValidateHolderRecordCandidate validates a mutation candidate, including the
// minimum remaining lease horizon required for a new Register or Heartbeat.
func ValidateHolderRecordCandidate(record HolderRecord, now time.Time, options HolderRegistryOptions) error {
	normalized, err := options.normalized()
	if err != nil {
		return err
	}
	validator := HolderRegistry{opts: normalized}
	return validator.validateCandidateRecord(cloneHolderRecord(record), now)
}

// ValidateHolderRegistration validates the complete initial WARMING candidate.
func ValidateHolderRegistration(record HolderRegistration, now time.Time, options HolderRegistryOptions) error {
	normalized, err := options.normalized()
	if err != nil {
		return err
	}
	validator := HolderRegistry{opts: normalized}
	return validator.validateRegistration(cloneHolderRecord(record), now)
}

func (record HolderRecord) MatchesTarget(target HolderTarget, now time.Time) bool {
	if record.Domain != target.Domain || record.HolderID != target.HolderID ||
		record.BootID != target.BootID || record.LogicEndpoint != target.LogicEndpoint ||
		record.HolderControlEndpoint != target.HolderControlEndpoint ||
		record.TLSDNSIdentity != target.TLSDNSIdentity {
		return false
	}
	if !record.LeaseUntil.After(now) || !record.ProcessReady || record.Draining {
		return false
	}
	if record.State != HolderStateReadyZeroShard && record.State != HolderStateServing {
		return false
	}
	return record.CapacityWeight >= target.MinimumCapacityWeight
}

func IsEligibleHolderTarget(record HolderRecord, target HolderTarget, now time.Time) bool {
	return record.MatchesTarget(target, now)
}

func validHolderStateTransition(from, to HolderState) bool {
	if from == to {
		return true
	}
	switch from {
	case HolderStateRegistering:
		return to == HolderStateWarming
	case HolderStateWarming:
		// This is only the structural transition matrix. HolderRegistry additionally
		// requires its server-side InitialServingVerifier before WARMING -> SERVING;
		// dynamic targets therefore cannot authorize themselves by setting State.
		return to == HolderStateReadyZeroShard || to == HolderStateServing || to == HolderStateDraining
	case HolderStateReadyZeroShard:
		return to == HolderStateServing || to == HolderStateDraining
	case HolderStateServing:
		// Dynamic assignment may remove the final shard without ending this
		// Holder boot. Returning to READY_ZERO_SHARD keeps the process eligible
		// for a later fenced assignment; HolderRegistry itself grants no shard
		// authority.
		return to == HolderStateReadyZeroShard || to == HolderStateDraining
	case HolderStateDraining:
		return to == HolderStateDrained
	default:
		return false
	}
}

func ValidHolderStateTransition(from, to HolderState) bool {
	return validHolderStateTransition(from, to)
}

func leaseExpired(record HolderRecord, now time.Time) bool {
	return !record.LeaseUntil.After(now)
}

func validateDomain(domain actorrouting.RouteDomain) error {
	if domain != actorrouting.DomainPlayer &&
		domain != actorrouting.DomainFarm &&
		domain != actorrouting.DomainFarmWorker {
		return fmt.Errorf("%w: domain=%q", ErrHolderRegistryInvalid, domain)
	}
	return nil
}

func validateComponent(name, value string, maxBytes int) error {
	if value == "" || strings.TrimSpace(value) != value || !utf8.ValidString(value) {
		return fmt.Errorf("%w: %s must be trimmed non-empty UTF-8", ErrHolderRegistryInvalid, name)
	}
	if len([]byte(value)) > maxBytes {
		return fmt.Errorf("%w: %s exceeds %d bytes", ErrHolderRegistryInvalid, name, maxBytes)
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("%w: %s contains control characters", ErrHolderRegistryInvalid, name)
		}
	}
	return nil
}

func validateTLSIdentity(value string, maxBytes int) error {
	if err := validateComponent("tls_dns_identity", value, maxBytes); err != nil {
		return err
	}
	if strings.Contains(value, "*") || strings.Contains(value, ":") ||
		strings.Contains(value, "/") || strings.Contains(value, "..") {
		return fmt.Errorf("%w: tls identity must be a DNS name", ErrHolderRegistryInvalid)
	}
	for _, label := range strings.Split(strings.ToLower(value), ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return fmt.Errorf("%w: invalid TLS DNS label", ErrHolderRegistryInvalid)
		}
	}
	return nil
}

func validateResourceSummary(summary map[string]string, opts HolderRegistryOptions) error {
	for key, value := range summary {
		if err := validateResourceText("resource key", key, opts.MaxResourceKeyBytes); err != nil {
			return err
		}
		if err := validateResourceText("resource value", value, opts.MaxResourceValueBytes); err != nil {
			return err
		}
	}
	wire := CanonicalHolderResourceSummaryWire(summary)
	if len(wire) > opts.MaxResourceSummaryBytes {
		return fmt.Errorf("%w: resource summary wire bytes=%d", ErrHolderRegistryInvalid, len(wire))
	}
	return nil
}

// CanonicalHolderResourceSummaryWire is the exact RS1 byte accounting used by
// the core contract and durable adapters. Keys are sorted by byte value and
// every key/value is length framed; callers must validate the summary before
// persisting it.
func CanonicalHolderResourceSummaryWire(summary map[string]string) string {
	keys := make([]string, 0, len(summary))
	for key := range summary {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var wire strings.Builder
	wire.WriteString(HolderResourceSummaryWirePrefix)
	wire.WriteString(strconv.Itoa(len(keys)))
	wire.WriteByte(':')
	for _, key := range keys {
		writeHolderResourceFrame(&wire, key)
		writeHolderResourceFrame(&wire, summary[key])
	}
	return wire.String()
}

func writeHolderResourceFrame(wire *strings.Builder, value string) {
	wire.WriteString(strconv.Itoa(len(value)))
	wire.WriteByte(':')
	wire.WriteString(value)
}

// ResourceSummary text is deliberately narrower than general holder identity
// text so Go and Redis Lua can enforce the exact same byte contract. Printable
// ASCII 0x20..0x7e is accepted, with no leading or trailing ASCII space;
// internal ASCII spaces are allowed.
func validateResourceText(name, value string, maxBytes int) error {
	if value == "" || len(value) > maxBytes || value[0] == ' ' || value[len(value)-1] == ' ' {
		return fmt.Errorf("%w: %s must be non-empty, bounded, and space-trimmed printable ASCII", ErrHolderRegistryInvalid, name)
	}
	for index := 0; index < len(value); index++ {
		if value[index] < 0x20 || value[index] > 0x7e {
			return fmt.Errorf("%w: %s contains non-printable or non-ASCII bytes", ErrHolderRegistryInvalid, name)
		}
	}
	return nil
}

func cloneResourceSummary(input map[string]string) map[string]string {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string]string, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func cloneHolderRecord(input HolderRecord) HolderRecord {
	input.ResourceSummary = cloneResourceSummary(input.ResourceSummary)
	return input
}

func cloneHolderHeartbeat(input HolderHeartbeat) HolderHeartbeat {
	input.ResourceSummary = cloneResourceSummary(input.ResourceSummary)
	return input
}

// HolderRecordDigest is the stable payload identity used by Redis replay
// handling and tests. It is not an authority token.
func HolderRecordDigest(record HolderRecord) string {
	record = cloneHolderRecord(record)
	payload, _ := json.Marshal(struct {
		Domain                actorrouting.RouteDomain          `json:"domain"`
		HolderID              string                            `json:"holder_id"`
		BootID                string                            `json:"boot_id"`
		DeploymentIdentity    holderidentity.DeploymentIdentity `json:"deployment_identity,omitempty"`
		LogicEndpoint         string                            `json:"logic_endpoint"`
		HolderControlEndpoint string                            `json:"holder_control_endpoint"`
		TLSDNSIdentity        string                            `json:"tls_dns_identity"`
		State                 HolderState                       `json:"state"`
		ProcessReady          bool                              `json:"process_ready"`
		Draining              bool                              `json:"draining"`
		ServingShardCount     uint32                            `json:"serving_shard_count"`
		SnapshotRevision      uint64                            `json:"snapshot_revision"`
		StartedAt             int64                             `json:"started_at_ms"`
		HeartbeatAt           int64                             `json:"heartbeat_at_ms"`
		LeaseUntil            int64                             `json:"lease_until_ms"`
		Sequence              uint64                            `json:"sequence"`
		CapacityWeight        uint32                            `json:"capacity_weight"`
		ResourceSummary       map[string]string                 `json:"resource_summary"`
	}{
		record.Domain, record.HolderID, record.BootID, record.DeploymentIdentity,
		record.LogicEndpoint, record.HolderControlEndpoint,
		record.TLSDNSIdentity, record.State, record.ProcessReady, record.Draining,
		record.ServingShardCount, record.SnapshotRevision,
		record.StartedAt.UnixMilli(), record.HeartbeatAt.UnixMilli(),
		record.LeaseUntil.UnixMilli(), record.Sequence, record.CapacityWeight,
		record.ResourceSummary,
	})
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

// HolderRecordEqual compares all liveness fields and resource values after
// cloning the maps, so callers cannot use shared mutable maps accidentally.
func HolderRecordEqual(left, right HolderRecord) bool {
	return HolderRecordDigest(left) == HolderRecordDigest(right)
}

// ReleasePlacementExclusionForOwner reverses an exclusion the named plan owns.
//
// The registry validates and forwards; the store decides. Classification is
// the store's because only it sees the record and the current Holder boot in
// the same operation, and a release that infers the boot from a separate read
// is a release that can act on a rollover it did not observe.
func (r *HolderRegistry) ReleasePlacementExclusionForOwner(ctx context.Context, token NestedControlOperationToken, domain actorrouting.RouteDomain, holderID, bootID string, release PlacementExclusionRelease) (PlacementExclusionResult, error) {
	if err := r.checkOpen(); err != nil {
		return PlacementExclusionResult{}, err
	}
	if ctx == nil {
		return PlacementExclusionResult{}, fmt.Errorf("%w: context required", ErrPlacementExclusionInvalid)
	}
	if r.placements == nil {
		return PlacementExclusionResult{}, ErrPlacementExclusionUnavailable
	}
	if !token.Issued() {
		return PlacementExclusionResult{}, fmt.Errorf("%w: release requires an issued control token", ErrPlacementExclusionInvalid)
	}
	if err := validateDomain(domain); err != nil {
		return PlacementExclusionResult{}, fmt.Errorf("%w: %v", ErrPlacementExclusionInvalid, err)
	}
	if err := validateComponent("holder_id", holderID, r.opts.MaxIDBytes); err != nil {
		return PlacementExclusionResult{}, fmt.Errorf("%w: %v", ErrPlacementExclusionInvalid, err)
	}
	if err := validateComponent("boot_id", bootID, r.opts.MaxIDBytes); err != nil {
		return PlacementExclusionResult{}, fmt.Errorf("%w: %v", ErrPlacementExclusionInvalid, err)
	}
	if err := validatePlacementExclusionRelease(release, r.opts.MaxIDBytes); err != nil {
		return PlacementExclusionResult{}, err
	}
	result, err := r.placements.ReleasePlacementExclusionForOwner(ctx, token, domain, holderID, bootID, release)
	if err != nil {
		return result, err
	}
	// An outcome outside the closed set is the section 17 stop condition: it
	// means the store learned something the seven-row table does not describe,
	// and guessing which side of the safe/unsafe line it falls on is exactly
	// the guess that table exists to prevent.
	switch result.Outcome {
	case PlacementReleaseReleased, PlacementReleaseAlreadyReleased, PlacementReleaseByBootRollover,
		PlacementReleaseNotFound, PlacementReleaseOwnerMismatch, PlacementReleaseRevisionConflict,
		PlacementReleaseMalformed:
		return result, nil
	default:
		return PlacementExclusionResult{}, fmt.Errorf("%w: unenumerated release outcome %q",
			ErrPlacementExclusionUnavailable, string(result.Outcome))
	}
}
