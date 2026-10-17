package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/actorrouting/authority"
	"farmgame/internal/netendpoint"
	routecore "farmgame/internal/routecontrol"
	"farmgame/internal/routecontrol/verifiedholder"
	"farmgame/internal/routeplan"
	scaleplancore "farmgame/internal/scaleplan"
	"farmgame/internal/storage/authoritystore"
)

const (
	initialAssignmentModeOff  = "off"
	initialAssignmentModeAuto = "auto"

	defaultInitialAssignmentTimeout = 5 * time.Minute
	maxInitialAssignmentTimeout     = 30 * time.Minute
	defaultInitialAssignmentHolders = 2
	maxInitialAssignmentHolders     = routecore.DefaultHolderMaxHolderCount
	// The production Redis cluster has cross-node latency per authority claim;
	// a sealed 2048-shard batch can exceed the ordinary 60s holder lease. This
	// TTL is accepted only by the route-controller store while explicit initial
	// assignment is enabled. Normal Holder/RoutePlan requests remain 60s.
	initialAssignmentClaimTTL     = 30 * time.Minute
	initialAssignmentPollInterval = time.Second

	initialAssignmentRefreshRetryBase = time.Second
	initialAssignmentRefreshRetryMax  = 15 * time.Second
)

var (
	ErrInitialAssignmentInvalid             = errors.New("route-controller: invalid initial assignment runtime")
	ErrInitialAssignmentNotReady            = errors.New("route-controller: initial assignment candidates are not ready")
	ErrInitialAssignmentTimeout             = errors.New("route-controller: initial assignment timed out")
	ErrInitialAssignmentHolderCount         = errors.New("route-controller: initial assignment holder count drift")
	ErrInitialAssignmentIdentityDrift       = errors.New("route-controller: initial assignment holder identity drift")
	ErrInitialAssignmentForeignActive       = errors.New("route-controller: initial assignment found a foreign active route")
	ErrInitialAssignmentMalformedGeneration = errors.New("route-controller: initial assignment found a malformed generation")
	ErrInitialAssignmentStaleLease          = errors.New("route-controller: initial assignment found a stale lease")
	ErrInitialAssignmentProjectionStale     = errors.New("route-controller: initial assignment route lease projection is stale")
	ErrInitialAssignmentManager             = errors.New("route-controller: initial assignment manager failed")
)

var initialAssignmentDomains = [...]actorrouting.RouteDomain{
	actorrouting.DomainPlayer,
	actorrouting.DomainFarm,
}

// initialAssignmentManager is deliberately smaller than the RouteAssignmentManager
// API. Initial assignment has no transfer, renew, or FARM_WORKER capability.
type initialAssignmentManager interface {
	CurrentSnapshot() actorrouting.RouteSnapshot
	Claim(context.Context, authority.ClaimRequest) (authoritystore.ShardAuthorityTransitionResult, error)
}

type initialAssignmentBatchManager interface {
	BeginInitialAuthorityBatch() error
	EndInitialAuthorityBatch() error
}

type initialAssignmentAuthorityReader interface {
	ReadShardWriteAuthority(context.Context, actorrouting.RouteDomain, uint32) (authoritystore.ShardWriteAuthority, bool, error)
}

// initialAssignmentCandidateSnapshot is one exact, configured Kubernetes
// workload observation. DiscoveryRevision and CapturedAt are intentionally not
// part of the identity: holder lifecycle/heartbeat changes are expected while
// the initial claims are being published.
type initialAssignmentCandidateSnapshot struct {
	Domain                  actorrouting.RouteDomain
	WorkloadKey             verifiedholder.WorkloadKey
	WorkloadUID             string
	KubeGeneration          int64
	KubeResourceVersion     string
	KubeListResourceVersion string
	Holders                 []routecore.HolderRecord
}

func (s initialAssignmentCandidateSnapshot) sameWorkloadIdentity(other initialAssignmentCandidateSnapshot) bool {
	// resourceVersion values advance for ordinary Pod/StatefulSet status and
	// Holder lifecycle updates during a long claim wave. UID plus generation
	// pins the workload incarnation and immutable spec while allowing those
	// expected observations to advance.
	return s.Domain == other.Domain && s.WorkloadKey == other.WorkloadKey &&
		s.WorkloadUID == other.WorkloadUID && s.KubeGeneration == other.KubeGeneration
}

// initialAssignmentCandidateProvider is the read-only candidate seam. The
// production implementation below is backed by the verified Kubernetes view;
// tests can provide a deterministic fake without constructing Kubernetes.
type initialAssignmentCandidateProvider interface {
	Candidates(context.Context, actorrouting.RouteDomain) (initialAssignmentCandidateSnapshot, error)
	// ObserveGeneration returns the pinned workload generation even when the
	// verified-holder TTL has expired. TTL expiry is not identity drift.
	ObserveGeneration(context.Context, actorrouting.RouteDomain) (initialAssignmentCandidateSnapshot, error)
}

type verifiedInitialAssignmentCandidateProvider struct {
	view   *verifiedholder.Store
	reader routeplan.HolderReader
	keys   map[actorrouting.RouteDomain]verifiedholder.WorkloadKey
	now    func() time.Time
}

func newVerifiedInitialAssignmentCandidateProvider(cfg config, view *verifiedholder.Store, reader routeplan.HolderReader, clocks ...func() time.Time) (*verifiedInitialAssignmentCandidateProvider, error) {
	if len(clocks) > 1 {
		return nil, fmt.Errorf("%w: one candidate provider clock is allowed", ErrInitialAssignmentInvalid)
	}
	if view == nil || reader == nil {
		return nil, fmt.Errorf("%w: verified Kubernetes candidate view and reader are required", ErrInitialAssignmentInvalid)
	}
	player, farm := scalePlanWorkloadRefs(cfg)
	keys := map[actorrouting.RouteDomain]verifiedholder.WorkloadKey{
		actorrouting.DomainPlayer: workloadKeyFromRef(player),
		actorrouting.DomainFarm:   workloadKeyFromRef(farm),
	}
	for _, key := range keys {
		if err := key.Validate(); err != nil {
			return nil, fmt.Errorf("%w: workload key: %w", ErrInitialAssignmentInvalid, err)
		}
	}
	now := time.Now
	if len(clocks) == 1 && clocks[0] != nil {
		now = clocks[0]
	}
	return &verifiedInitialAssignmentCandidateProvider{
		view: view, reader: reader, keys: keys, now: now,
	}, nil
}

func (p *verifiedInitialAssignmentCandidateProvider) ObserveGeneration(ctx context.Context, domain actorrouting.RouteDomain) (initialAssignmentCandidateSnapshot, error) {
	return p.collectCandidates(ctx, domain, false)
}

func (p *verifiedInitialAssignmentCandidateProvider) Candidates(ctx context.Context, domain actorrouting.RouteDomain) (initialAssignmentCandidateSnapshot, error) {
	return p.collectCandidates(ctx, domain, true)
}

func (p *verifiedInitialAssignmentCandidateProvider) collectCandidates(ctx context.Context, domain actorrouting.RouteDomain, requireFreshVerification bool) (initialAssignmentCandidateSnapshot, error) {
	if p == nil || p.view == nil || p.reader == nil || ctx == nil {
		return initialAssignmentCandidateSnapshot{}, fmt.Errorf("%w: candidate provider unavailable", ErrInitialAssignmentInvalid)
	}
	if err := ctx.Err(); err != nil {
		return initialAssignmentCandidateSnapshot{}, err
	}
	key, ok := p.keys[domain]
	if !ok {
		return initialAssignmentCandidateSnapshot{}, fmt.Errorf("%w: unsupported candidate domain %s", ErrInitialAssignmentInvalid, domain)
	}
	workload, found := p.view.Snapshot(key)
	if !found {
		return initialAssignmentCandidateSnapshot{}, fmt.Errorf("%w: workload snapshot %s", ErrInitialAssignmentNotReady, key.String())
	}
	if err := workload.Validate(); err != nil {
		return initialAssignmentCandidateSnapshot{}, fmt.Errorf("%w: workload snapshot %s: %w", ErrInitialAssignmentIdentityDrift, key.String(), err)
	}
	if workload.Key != key || workload.Key.Domain != domain {
		return initialAssignmentCandidateSnapshot{}, fmt.Errorf("%w: configured workload key drift", ErrInitialAssignmentIdentityDrift)
	}

	records, err := p.reader.List(ctx, domain)
	if err != nil {
		return initialAssignmentCandidateSnapshot{}, err
	}
	recordByID := make(map[string]routecore.HolderRecord, len(records))
	for _, record := range records {
		if record.Domain != domain || record.HolderID == "" {
			return initialAssignmentCandidateSnapshot{}, fmt.Errorf("%w: malformed verified holder list", ErrInitialAssignmentIdentityDrift)
		}
		if _, exists := recordByID[record.HolderID]; exists {
			return initialAssignmentCandidateSnapshot{}, fmt.Errorf("%w: duplicate verified holder %s", ErrInitialAssignmentIdentityDrift, record.HolderID)
		}
		if err := validInitialAssignmentEndpointRecord(record); err != nil {
			return initialAssignmentCandidateSnapshot{}, err
		}
		recordByID[record.HolderID] = record
	}

	now := time.Now()
	if p.now != nil {
		now = p.now()
	}
	eligible := make([]routecore.HolderRecord, 0, len(workload.Holders))
	seenEligible := make(map[string]struct{}, len(workload.Holders))
	for _, holder := range workload.Holders {
		if err := holder.Validate(); err != nil {
			return initialAssignmentCandidateSnapshot{}, fmt.Errorf("%w: holder %s: %w", ErrInitialAssignmentIdentityDrift, holder.HolderID, err)
		}
		if holder.Domain != domain || holder.WorkloadKey != key || holder.WorkloadUID != workload.WorkloadUID {
			return initialAssignmentCandidateSnapshot{}, fmt.Errorf("%w: holder workload identity drift", ErrInitialAssignmentIdentityDrift)
		}
		if requireFreshVerification {
			if !holder.Eligible(now) {
				continue
			}
		} else if !holder.ProcessAdmitted() {
			continue
		}
		record, recordFound := recordByID[holder.HolderID]
		if !recordFound {
			if requireFreshVerification {
				return initialAssignmentCandidateSnapshot{}, fmt.Errorf("%w: holder %s/%s is not the exact verified generation", ErrInitialAssignmentIdentityDrift, holder.HolderID, holder.BootID)
			}
			built, _, buildErr := verifiedHolderRecord(holder)
			if buildErr != nil {
				return initialAssignmentCandidateSnapshot{}, fmt.Errorf("%w: holder %s: %w", ErrInitialAssignmentIdentityDrift, holder.HolderID, buildErr)
			}
			record = built
		} else if !sameInitialAssignmentHolderIdentity(holder, record) {
			return initialAssignmentCandidateSnapshot{}, fmt.Errorf("%w: holder %s/%s is not the exact verified generation", ErrInitialAssignmentIdentityDrift, holder.HolderID, holder.BootID)
		}
		if _, exists := seenEligible[holder.HolderID]; exists {
			return initialAssignmentCandidateSnapshot{}, fmt.Errorf("%w: duplicate eligible holder %s", ErrInitialAssignmentIdentityDrift, holder.HolderID)
		}
		seenEligible[holder.HolderID] = struct{}{}
		eligible = append(eligible, record)
	}
	if requireFreshVerification && len(recordByID) != len(eligible) {
		return initialAssignmentCandidateSnapshot{}, fmt.Errorf("%w: verified holder set does not match configured workload snapshot", ErrInitialAssignmentNotReady)
	}
	for _, record := range eligible {
		if err := validateInitialAssignmentCandidate(record, now, requireFreshVerification); err != nil {
			return initialAssignmentCandidateSnapshot{}, err
		}
	}
	eligible, err = sortInitialAssignmentCandidates(eligible)
	if err != nil {
		return initialAssignmentCandidateSnapshot{}, err
	}
	return initialAssignmentCandidateSnapshot{
		Domain: domain, WorkloadKey: workload.Key, WorkloadUID: workload.WorkloadUID,
		KubeGeneration: workload.KubeGeneration, KubeResourceVersion: workload.KubeResourceVersion,
		KubeListResourceVersion: workload.KubeListResourceVersion, Holders: eligible,
	}, nil
}

func sameInitialAssignmentHolderIdentity(holder verifiedholder.Holder, record routecore.HolderRecord) bool {
	// Only the immutable deployment/process generation belongs in this join.
	// Lifecycle, readiness and serving counts legitimately change from
	// READY_ZERO_SHARD to SERVING while the long initial claim batch runs and
	// are validated separately on the latest candidate record.
	return record.Domain == holder.Domain && record.HolderID == holder.HolderID &&
		record.BootID == holder.BootID && record.DeploymentIdentity.PodUID == holder.PodUID &&
		record.DeploymentIdentity.WorkloadUID == holder.WorkloadUID &&
		record.DeploymentIdentity.Ordinal == holder.Ordinal
}

func validInitialAssignmentEndpointRecord(record routecore.HolderRecord) error {
	if err := netendpoint.Validate(record.LogicEndpoint, routecore.DefaultHolderMaxEndpointBytes); err != nil {
		return fmt.Errorf("%w: holder %s logic endpoint: %w", ErrInitialAssignmentIdentityDrift, record.HolderID, err)
	}
	if err := netendpoint.Validate(record.HolderControlEndpoint, routecore.DefaultHolderMaxEndpointBytes); err != nil {
		return fmt.Errorf("%w: holder %s control endpoint: %w", ErrInitialAssignmentIdentityDrift, record.HolderID, err)
	}
	return nil
}

func validateInitialAssignmentCandidate(record routecore.HolderRecord, now time.Time, requireVerifiedLease bool) error {
	if err := validInitialAssignmentEndpointRecord(record); err != nil {
		return err
	}
	if record.Domain != actorrouting.DomainPlayer && record.Domain != actorrouting.DomainFarm ||
		record.HolderID == "" || record.BootID == "" || record.DeploymentIdentity.ValidateExact() != nil ||
		record.TLSDNSIdentity == "" || !record.ProcessReady || record.Draining ||
		(record.State != routecore.HolderStateReadyZeroShard && record.State != routecore.HolderStateServing) ||
		record.CapacityWeight == 0 {
		return fmt.Errorf("%w: holder %s is not an eligible exact target", ErrInitialAssignmentIdentityDrift, record.HolderID)
	}
	if requireVerifiedLease && !record.LeaseUntil.After(now) {
		return fmt.Errorf("%w: holder %s is not an eligible exact target", ErrInitialAssignmentIdentityDrift, record.HolderID)
	}
	return nil
}

func sortInitialAssignmentCandidates(holders []routecore.HolderRecord) ([]routecore.HolderRecord, error) {
	result := append([]routecore.HolderRecord(nil), holders...)
	seenIDs := make(map[string]struct{}, len(result))
	seenOrdinals := make(map[int32]struct{}, len(result))
	for _, holder := range result {
		if _, exists := seenIDs[holder.HolderID]; exists {
			return nil, fmt.Errorf("%w: duplicate holder id %s", ErrInitialAssignmentIdentityDrift, holder.HolderID)
		}
		if _, exists := seenOrdinals[holder.DeploymentIdentity.Ordinal]; exists {
			return nil, fmt.Errorf("%w: duplicate holder ordinal %d", ErrInitialAssignmentIdentityDrift, holder.DeploymentIdentity.Ordinal)
		}
		seenIDs[holder.HolderID] = struct{}{}
		seenOrdinals[holder.DeploymentIdentity.Ordinal] = struct{}{}
	}
	sort.Slice(result, func(i, j int) bool {
		left, right := result[i], result[j]
		if left.DeploymentIdentity.Ordinal != right.DeploymentIdentity.Ordinal {
			return left.DeploymentIdentity.Ordinal < right.DeploymentIdentity.Ordinal
		}
		if left.HolderID != right.HolderID {
			return left.HolderID < right.HolderID
		}
		return left.BootID < right.BootID
	})
	return result, nil
}

func deterministicInitialAssignmentTarget(holders []routecore.HolderRecord, shard uint32) (routecore.HolderRecord, error) {
	sorted, err := sortInitialAssignmentCandidates(holders)
	if err != nil {
		return routecore.HolderRecord{}, err
	}
	if len(sorted) == 0 {
		return routecore.HolderRecord{}, fmt.Errorf("%w: no eligible holders", ErrInitialAssignmentHolderCount)
	}
	return sorted[int(shard)%len(sorted)], nil
}

func validateInitialAssignmentMode(mode string) error {
	if mode != initialAssignmentModeOff && mode != initialAssignmentModeAuto {
		return fmt.Errorf("route-controller: initial assignment mode must be off or auto")
	}
	return nil
}

func initialAssignmentModeFromConfig(cfg config) (string, error) {
	mode := cfg.initialAssignmentMode
	if mode == "" {
		mode = initialAssignmentModeOff
	}
	if err := validateInitialAssignmentMode(mode); err != nil {
		return "", err
	}
	return mode, nil
}

func effectiveInitialAssignmentTimeout(value time.Duration) time.Duration {
	if value == 0 {
		return defaultInitialAssignmentTimeout
	}
	return value
}

func effectiveInitialAssignmentHolderCounts(cfg config) (int, int) {
	player := cfg.initialAssignmentExpectedPlayerHolders
	if player == 0 {
		player = cfg.initialAssignmentPlayerHolders
	}
	if player == 0 {
		player = defaultInitialAssignmentHolders
	}
	farm := cfg.initialAssignmentExpectedFarmHolders
	if farm == 0 {
		farm = cfg.initialAssignmentFarmHolders
	}
	if farm == 0 {
		farm = defaultInitialAssignmentHolders
	}
	return player, farm
}

func validateInitialAssignmentConfig(cfg config) error {
	mode, err := initialAssignmentModeFromConfig(cfg)
	if err != nil {
		return err
	}
	timeout := effectiveInitialAssignmentTimeout(cfg.initialAssignmentTimeout)
	player, farm := effectiveInitialAssignmentHolderCounts(cfg)
	if timeout <= 0 || timeout > maxInitialAssignmentTimeout {
		return fmt.Errorf("route-controller: initial assignment timeout must be greater than zero and at most %s", maxInitialAssignmentTimeout)
	}
	if player <= 0 || player > maxInitialAssignmentHolders || farm <= 0 || farm > maxInitialAssignmentHolders {
		return fmt.Errorf("route-controller: initial assignment expected holder counts must be between 1 and %d", maxInitialAssignmentHolders)
	}
	if mode != initialAssignmentModeAuto {
		return nil
	}
	if !holderDiscoveryUsesK8sCandidates(cfg.holderDiscoveryMode) {
		return errors.New("route-controller: initial assignment auto mode requires Kubernetes holder discovery candidates")
	}
	scaleMode, err := scalePlanModeFromConfig(cfg)
	if err != nil {
		return err
	}
	if scaleMode == scaleplancore.RuntimeModeOff {
		return errors.New("route-controller: initial assignment auto mode requires ScalePlan mutation mode")
	}
	return nil
}

// initialAssignmentRuntime is the one-shot post-bootstrap publisher for the
// PLAYER and FARM domains. FARM_WORKER is intentionally absent from every
// method in this type.
type initialAssignmentRuntime struct {
	config          config
	manager         initialAssignmentManager
	authorityReader initialAssignmentAuthorityReader
	candidates      initialAssignmentCandidateProvider
	control         routeplan.ControlMutationOptions
	refresh         func(context.Context) error
	refreshDomain   func(context.Context, actorrouting.RouteDomain) error
	refreshRetry    func(attempt int) time.Duration
	now             func() time.Time
	playerHolders   int
	farmHolders     int
	pollInterval    time.Duration
	resume          bool
	// persistedRoutingFastPath is set only when Run accepted a complete
	// post-migration snapshot. That path deliberately has no candidate pin: the
	// durable Snapshot and exact Authority tuple are its proof.
	persistedRoutingFastPath bool
	pinned                   map[actorrouting.RouteDomain]initialAssignmentCandidateSnapshot
	batchPublisher           *routecore.InitialAuthorityBatchPublisher
	transitionStore          *authority.SwitchableTransitionStore
	snapshotStore            routecore.RouteSnapshotStore
}

type initialAssignmentRuntimeOptions struct {
	Config          config
	Manager         initialAssignmentManager
	AuthorityReader initialAssignmentAuthorityReader
	Candidates      initialAssignmentCandidateProvider
	Control         routeplan.ControlMutationOptions
	Refresh         func(context.Context) error
	RefreshDomain   func(context.Context, actorrouting.RouteDomain) error
	// RefreshRetry optionally overrides the bounded backoff between
	// batch-boundary refresh attempts. A nil value uses the production
	// exponential backoff; tests inject sub-millisecond delays.
	RefreshRetry    func(attempt int) time.Duration
	Now             func() time.Time
	PollInterval    time.Duration
	BatchPublisher  *routecore.InitialAuthorityBatchPublisher
	TransitionStore *authority.SwitchableTransitionStore
	SnapshotStore   routecore.RouteSnapshotStore
}

func newInitialAssignmentRuntime(cfg config, plan *planRuntime, view *verifiedholder.Store) (*initialAssignmentRuntime, error) {
	mode, err := initialAssignmentModeFromConfig(cfg)
	if err != nil {
		return nil, err
	}
	if mode == initialAssignmentModeOff {
		return nil, nil
	}
	if plan == nil || plan.Manager == nil || plan.AuthorityReader == nil || plan.HolderReader == nil || plan.ControlMutation.Coordinator == nil {
		return nil, fmt.Errorf("%w: RouteAssignmentManager, AuthorityReader, HolderReader and control coordinator are required", ErrInitialAssignmentInvalid)
	}
	provider, err := newVerifiedInitialAssignmentCandidateProvider(cfg, view, plan.HolderReader)
	if err != nil {
		return nil, err
	}
	return newInitialAssignmentRuntimeWithOptions(initialAssignmentRuntimeOptions{
		Config: cfg, Manager: plan.Manager, AuthorityReader: plan.AuthorityReader,
		Candidates: provider, Control: plan.ControlMutation,
		RefreshDomain:  initialAssignmentRefreshDomainFromPlan(plan),
		BatchPublisher: plan.InitialPublisher, TransitionStore: plan.InitialTransitionStore,
		SnapshotStore: plan.SnapshotStore,
		Refresh: func(ctx context.Context) error {
			if plan.Recovery == nil {
				return nil
			}
			return plan.Recovery.Recover(ctx)
		},
	})
}

func newInitialAssignmentRuntimeWithOptions(options initialAssignmentRuntimeOptions) (*initialAssignmentRuntime, error) {
	mode, err := initialAssignmentModeFromConfig(options.Config)
	if err != nil {
		return nil, err
	}
	if mode != initialAssignmentModeAuto || options.Manager == nil || options.AuthorityReader == nil || options.Candidates == nil {
		return nil, fmt.Errorf("%w: auto runtime dependencies are required", ErrInitialAssignmentInvalid)
	}
	if err := options.Control.Validate(); err != nil {
		return nil, err
	}
	player, farm := effectiveInitialAssignmentHolderCounts(options.Config)
	if player <= 0 || farm <= 0 {
		return nil, fmt.Errorf("%w: expected holder counts", ErrInitialAssignmentInvalid)
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.PollInterval <= 0 {
		options.PollInterval = initialAssignmentPollInterval
	}
	if options.RefreshRetry == nil {
		options.RefreshRetry = defaultInitialAssignmentRefreshRetry
	}
	return &initialAssignmentRuntime{
		config: options.Config, manager: options.Manager, authorityReader: options.AuthorityReader,
		candidates: options.Candidates, control: options.Control, refresh: options.Refresh,
		refreshDomain: options.RefreshDomain, refreshRetry: options.RefreshRetry,
		now: options.Now, playerHolders: player, farmHolders: farm,
		pollInterval: options.PollInterval, batchPublisher: options.BatchPublisher,
		transitionStore: options.TransitionStore, snapshotStore: options.SnapshotStore,
	}, nil
}

// defaultInitialAssignmentRefreshRetry is the production bounded exponential
// backoff between batch-boundary refresh attempts.
func defaultInitialAssignmentRefreshRetry(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	delay := initialAssignmentRefreshRetryBase << uint(attempt)
	if delay > initialAssignmentRefreshRetryMax {
		return initialAssignmentRefreshRetryMax
	}
	return delay
}

// initialAssignmentRefreshDomainFromPlan obtains the synchronous discovery hook
// installed by newKubernetesHolderDiscoveryHooks without widening planRuntime's
// public surface. The verified reader is the only production reader that has
// a Kubernetes + HolderControl + TokenReview refresh operation.
func initialAssignmentRefreshDomainFromPlan(plan *planRuntime) func(context.Context, actorrouting.RouteDomain) error {
	if plan == nil || plan.HolderReader == nil {
		return nil
	}
	reader, err := plan.HolderReader.current()
	if err != nil {
		return nil
	}
	verified, ok := reader.(*verifiedRoutePlanHolderReader)
	if !ok || verified == nil || verified.refresh == nil {
		return nil
	}
	return verified.refresh
}

func (r *initialAssignmentRuntime) expectedHolders(domain actorrouting.RouteDomain) (int, error) {
	if r == nil {
		return 0, ErrInitialAssignmentInvalid
	}
	switch domain {
	case actorrouting.DomainPlayer:
		return r.playerHolders, nil
	case actorrouting.DomainFarm:
		return r.farmHolders, nil
	default:
		return 0, fmt.Errorf("%w: unsupported domain %s", ErrInitialAssignmentInvalid, domain)
	}
}

func (r *initialAssignmentRuntime) Run(ctx context.Context) error {
	if r == nil || r.manager == nil || r.authorityReader == nil || r.candidates == nil || ctx == nil {
		return ErrInitialAssignmentInvalid
	}
	r.pinned = nil
	r.persistedRoutingFastPath = false
	if err := ctx.Err(); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return errors.Join(ErrInitialAssignmentTimeout, err)
		}
		return err
	}
	initial := r.manager.CurrentSnapshot()
	if r.config.realm != "" && initial.Realm != r.config.realm {
		return fmt.Errorf("%w: configured realm=%q snapshot realm=%q", ErrInitialAssignmentIdentityDrift, r.config.realm, initial.Realm)
	}
	if _, err := actorrouting.NewStaticResolver(initial); err != nil {
		return fmt.Errorf("%w: initial route snapshot: %w", ErrInitialAssignmentMalformedGeneration, err)
	}
	resume, err := validateInitialAssignmentSnapshot(initial)
	if err != nil {
		return err
	}
	r.resume = resume
	if resume {
		accepted, durableErr := r.validatePersistedActiveRouting(ctx, initial)
		if durableErr != nil {
			return durableErr
		}
		if accepted {
			r.persistedRoutingFastPath = true
			if r.snapshotStore != nil {
				if err := r.verifyDurableSnapshot(ctx); err != nil {
					return err
				}
			}
			return nil
		}
	}
	pinned, err := r.waitForCandidates(ctx)
	if err != nil {
		return err
	}
	r.pinned = pinned

	operationCtx := ctx
	batchOpen := false
	if !resume && r.batchPublisher != nil {
		if r.transitionStore == nil {
			return fmt.Errorf("%w: switchable transition store required", ErrInitialAssignmentInvalid)
		}
		batchManager, ok := r.manager.(initialAssignmentBatchManager)
		if !ok {
			return fmt.Errorf("%w: manager lacks initial batch lifecycle", ErrInitialAssignmentInvalid)
		}
		if err := batchManager.BeginInitialAuthorityBatch(); err != nil {
			return err
		}
		if err := r.transitionStore.BeginBufferedInitialAuthorityBatch(); err != nil {
			return err
		}
		if err := r.batchPublisher.BeginInitialAuthorityBatch(operationCtx, initial); err != nil {
			return err
		}
		batchOpen = true
	}

	for _, domain := range initialAssignmentDomains {
		if _, err := r.expectedHolders(domain); err != nil {
			return err
		}
		shardCount := countOrSnapshotShardCount(r.manager.CurrentSnapshot(), domain)
		if shardCount == 0 {
			return fmt.Errorf("%w: %s has no shards", ErrInitialAssignmentMalformedGeneration, domain)
		}
		for shard := uint32(0); shard < shardCount; shard++ {
			if err := operationCtx.Err(); err != nil {
				return err
			}
			target, err := deterministicInitialAssignmentTarget(pinned[domain].Holders, shard)
			if err != nil {
				return err
			}
			current := r.manager.CurrentSnapshot()
			route, holder, err := initialAssignmentRoute(current, domain, shard)
			if err != nil {
				return err
			}
			switch route.State {
			case actorrouting.ShardStateAssigning:
				if !isZeroGenerationInitialAssignmentRoute(route) {
					return fmt.Errorf("%w: %s/%d assigning route is not a zero-generation placeholder", ErrInitialAssignmentMalformedGeneration, domain, shard)
				}
				if err := r.claim(operationCtx, domain, shard, target); err != nil {
					return err
				}
				// From the first committed claim onward the pinned holders are
				// expected to progress READY_ZERO_SHARD -> SERVING. Their exact
				// deployment and process identities remain immutable.
				r.resume = true
			case actorrouting.ShardStateActive:
				if err := r.ensureActiveRoute(operationCtx, current, domain, shard, route, holder, target); err != nil {
					return err
				}
			default:
				return fmt.Errorf("%w: %s/%d route state=%s", ErrInitialAssignmentMalformedGeneration, domain, shard, route.State)
			}
		}
	}
	if err := r.ensureComplete(operationCtx, pinned); err != nil {
		return err
	}
	if !batchOpen {
		if r.snapshotStore != nil {
			if err := r.verifyDurableSnapshot(operationCtx); err != nil {
				return err
			}
		}
		return nil
	}

	// Candidate identity is checked once more at the batch boundary, after
	// every claim but before the sealed capability can reach the durable CAS.
	// This is deliberately outside the shard loop: a long batch may outlive
	// many verified-holder TTLs while its immutable generation remains valid.
	if err := r.refreshAndVerifyPinnedCandidates(operationCtx, pinned); err != nil {
		return err
	}
	if err := r.ensureComplete(operationCtx, pinned); err != nil {
		return err
	}
	records, err := r.collectBatchRecords(operationCtx, pinned)
	if err != nil {
		return err
	}
	capability, err := r.batchPublisher.SealInitialAuthorityBatch(records)
	if err != nil {
		return err
	}
	commitCtx, resolveControl, err := r.control.Begin(ctx, initialAssignmentBatchControlOperationID(initial.Realm), scaleplancore.ControlOperationBootstrap)
	if err != nil {
		return err
	}
	token, issued := authority.ControlOperationTokenFromContext(commitCtx)
	if !issued || r.control.Coordinator == nil {
		return routecore.ErrInitialAuthorityBatchTokenRequired
	}
	if err := r.control.Coordinator.Verify(commitCtx, token, r.control.Scope); err != nil {
		return fmt.Errorf("%w: batch control token: %v", ErrInitialAssignmentManager, err)
	}
	if _, err := r.batchPublisher.CommitInitialAuthorityBatch(commitCtx, token, capability); err != nil {
		return err
	}
	// The publisher has committed the exact final snapshot and notified Watch.
	// Re-read the durable envelope and every Authority before switching the
	// manager and transition evidence to their irreversible normal paths.
	if err := r.verifyDurableAndAuthorities(commitCtx, pinned); err != nil {
		return err
	}
	if err := r.transitionStore.SealDurable(); err != nil {
		return err
	}
	batchManager, ok := r.manager.(initialAssignmentBatchManager)
	if !ok {
		return fmt.Errorf("%w: manager lacks initial batch lifecycle", ErrInitialAssignmentInvalid)
	}
	if err := batchManager.EndInitialAuthorityBatch(); err != nil {
		return err
	}
	if resolveControl != nil {
		if err := resolveControl(); err != nil {
			return err
		}
	}
	return nil
}

// VerifyReady is the final fail-closed gate immediately before the process
// publishes startup readiness. It repeats the exact candidate/route/Authority
// checks after ScalePlan and RoutePlan recovery have finished, so a long
// startup phase cannot turn an already-expired initial claim into readiness.
func (r *initialAssignmentRuntime) VerifyReady(ctx context.Context) error {
	if r == nil || ctx == nil {
		return ErrInitialAssignmentInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	snapshot := r.manager.CurrentSnapshot()
	resume, err := validateInitialAssignmentSnapshot(snapshot)
	if err != nil {
		return err
	}
	if !resume {
		return fmt.Errorf("%w: startup snapshot is not fully assigned", ErrInitialAssignmentMalformedGeneration)
	}
	r.resume = true

	// A post-migration snapshot is intentionally accepted without reconstructing
	// the initial candidate modulo set. Its proof is the durable Snapshot plus
	// every exact Authority fence, and that proof must be repeated here rather
	// than rejected merely because no initial candidate was pinned.
	if r.persistedRoutingFastPath {
		if r.snapshotStore == nil {
			return fmt.Errorf("%w: post-migration durable snapshot store is required", ErrInitialAssignmentManager)
		}
		if err := r.verifyDurableSnapshot(ctx); err != nil {
			return err
		}
		accepted, err := r.validatePersistedActiveRouting(ctx, snapshot)
		if err != nil {
			return err
		}
		if !accepted {
			return fmt.Errorf("%w: persisted routing fast path is no longer complete", ErrInitialAssignmentMalformedGeneration)
		}
		return r.verifyDurableSnapshot(ctx)
	}

	if len(r.pinned) != len(initialAssignmentDomains) {
		return ErrInitialAssignmentInvalid
	}
	if err := r.refreshAndVerifyPinnedCandidates(ctx, r.pinned); err != nil {
		return err
	}
	if err := r.ensureComplete(ctx, r.pinned); err != nil {
		return err
	}
	if r.snapshotStore != nil {
		return r.verifyDurableSnapshot(ctx)
	}
	return nil
}

func (r *initialAssignmentRuntime) verifyDurableSnapshot(ctx context.Context) error {
	if r == nil || r.snapshotStore == nil || ctx == nil {
		return ErrInitialAssignmentInvalid
	}
	stored, found, err := r.snapshotStore.LoadCurrent(ctx)
	if err != nil {
		return fmt.Errorf("%w: durable snapshot read: %v", ErrInitialAssignmentManager, err)
	}
	if !found || !reflect.DeepEqual(stored.Snapshot, r.manager.CurrentSnapshot()) {
		return fmt.Errorf("%w: durable envelope does not equal manager snapshot", ErrInitialAssignmentManager)
	}
	return nil
}

func (r *initialAssignmentRuntime) verifyDurableAndAuthorities(ctx context.Context, pinned map[actorrouting.RouteDomain]initialAssignmentCandidateSnapshot) error {
	if err := r.verifyDurableSnapshot(ctx); err != nil {
		return err
	}
	return r.ensureComplete(ctx, pinned)
}

func (r *initialAssignmentRuntime) collectBatchRecords(ctx context.Context, pinned map[actorrouting.RouteDomain]initialAssignmentCandidateSnapshot) ([]routecore.InitialAuthorityBatchRecord, error) {
	if r == nil || ctx == nil {
		return nil, ErrInitialAssignmentInvalid
	}
	snapshot := r.manager.CurrentSnapshot()
	records := make([]routecore.InitialAuthorityBatchRecord, 0, int(snapshot.Player.ShardCount)+int(snapshot.Farm.ShardCount))
	for _, domain := range initialAssignmentDomains {
		routes, err := initialAssignmentDomainSnapshot(snapshot, domain)
		if err != nil {
			return nil, err
		}
		for shard, route := range routes.Routes {
			if route.State != actorrouting.ShardStateActive {
				return nil, fmt.Errorf("%w: %s/%d is not ACTIVE at batch seal", ErrInitialAssignmentMalformedGeneration, domain, shard)
			}
			target, err := deterministicInitialAssignmentTarget(pinned[domain].Holders, uint32(shard))
			if err != nil {
				return nil, err
			}
			authorityValue, found, readErr := r.authorityReader.ReadShardWriteAuthority(ctx, domain, uint32(shard))
			if readErr != nil {
				return nil, fmt.Errorf("%w: authority read %s/%d: %v", ErrInitialAssignmentManager, domain, shard, readErr)
			}
			if !found {
				return nil, fmt.Errorf("%w: authority missing %s/%d", ErrInitialAssignmentMalformedGeneration, domain, shard)
			}
			transitionID := initialAssignmentTransitionIDForTarget(snapshot.Realm, domain, uint32(shard), target)
			if err := r.validateInitialAssignmentAuthority(snapshot, domain, uint32(shard), target, authorityValue, transitionID); err != nil {
				return nil, err
			}
			acceptedTTL := authorityValue.LeaseUntilMs - authorityValue.LastTransitionAtMs
			if acceptedTTL <= 0 {
				acceptedTTL = initialAssignmentClaimTTL.Milliseconds()
			}
			records = append(records, routecore.InitialAuthorityBatchRecord{
				Realm: snapshot.Realm, Domain: domain, VirtualShard: uint32(shard),
				HolderID: target.HolderID, HolderBootID: target.BootID, Endpoint: target.LogicEndpoint,
				AuthorityLayoutVersion: authorityValue.AuthorityLayoutVersion,
				HashVersion:            authorityValue.HashVersion, HashSeed: authorityValue.HashSeed,
				AssignmentRevision: authorityValue.AssignmentRevision, WriterEpoch: authorityValue.WriterEpoch,
				LeaseEpoch: authorityValue.LeaseEpoch, LeaseUntilMs: authorityValue.LeaseUntilMs,
				LastTransitionID: authorityValue.LastTransitionID, AcceptedTTLms: acceptedTTL,
			})
		}
	}
	return records, nil
}

func (r *initialAssignmentRuntime) validateInitialAssignmentAuthority(snapshot actorrouting.RouteSnapshot, domain actorrouting.RouteDomain, shard uint32, target routecore.HolderRecord, value authoritystore.ShardWriteAuthority, transitionID string) error {
	if value.Domain != domain || value.VirtualShard != shard || value.State != authoritystore.ShardAuthorityActive ||
		value.HolderID != target.HolderID || value.HolderBootID != target.BootID {
		return fmt.Errorf("%w: authority target %s/%d is foreign", ErrInitialAssignmentForeignActive, domain, shard)
	}
	if r.config.authorityLayout != "" && value.AuthorityLayoutVersion != r.config.authorityLayout {
		return fmt.Errorf("%w: authority layout %s/%d", ErrInitialAssignmentMalformedGeneration, domain, shard)
	}
	if value.HashVersion != snapshot.HashVersion || value.HashSeed != snapshot.HashSeed || value.LastTransitionID != transitionID ||
		value.AssignmentRevision == 0 || value.WriterEpoch == 0 || value.LeaseEpoch == 0 || value.LeaseUntilMs <= 0 {
		return fmt.Errorf("%w: authority identity/generation %s/%d", ErrInitialAssignmentMalformedGeneration, domain, shard)
	}
	if value.LeaseUntilMs <= r.now().UnixMilli() {
		return fmt.Errorf("%w: authority lease %s/%d expired at %d", ErrInitialAssignmentStaleLease, domain, shard, value.LeaseUntilMs)
	}
	return nil
}

func initialAssignmentBatchControlOperationID(realm string) string {
	return boundedInitialAssignmentOperationID(fmt.Sprintf("route-controller/initial-assignment-batch/%s", realm))
}

func countOrSnapshotShardCount(snapshot actorrouting.RouteSnapshot, domain actorrouting.RouteDomain) uint32 {
	switch domain {
	case actorrouting.DomainPlayer:
		return snapshot.Player.ShardCount
	case actorrouting.DomainFarm:
		return snapshot.Farm.ShardCount
	default:
		return 0
	}
}

func (r *initialAssignmentRuntime) waitForCandidates(ctx context.Context) (map[actorrouting.RouteDomain]initialAssignmentCandidateSnapshot, error) {
	result := make(map[actorrouting.RouteDomain]initialAssignmentCandidateSnapshot, len(initialAssignmentDomains))
	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()
	for {
		ready := true
		for _, domain := range initialAssignmentDomains {
			want, err := r.expectedHolders(domain)
			if err != nil {
				return nil, err
			}
			candidateSet, candidateErr := r.candidates.Candidates(ctx, domain)
			if candidateErr != nil {
				if errors.Is(candidateErr, ErrInitialAssignmentNotReady) {
					ready = false
					continue
				}
				return nil, candidateErr
			}
			if len(candidateSet.Holders) > want {
				return nil, fmt.Errorf("%w: domain=%s got=%d want=%d", ErrInitialAssignmentHolderCount, domain, len(candidateSet.Holders), want)
			}
			candidateSet.Holders, err = sortInitialAssignmentCandidates(candidateSet.Holders)
			if err != nil {
				return nil, err
			}
			if len(candidateSet.Holders) < want {
				ready = false
				continue
			}
			if err := r.validateInitialAssignmentCandidateLifecycle(candidateSet.Holders); err != nil {
				return nil, err
			}
			result[domain] = candidateSet
		}
		if ready && len(result) == len(initialAssignmentDomains) {
			return result, nil
		}
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return nil, errors.Join(ErrInitialAssignmentTimeout, ctx.Err())
			}
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (r *initialAssignmentRuntime) validateInitialAssignmentCandidateLifecycle(holders []routecore.HolderRecord) error {
	now := r.now()
	for _, holder := range holders {
		if err := validateInitialAssignmentCandidate(holder, now, !r.resume); err != nil {
			return err
		}
		if !r.resume && holder.State == routecore.HolderStateServing {
			return fmt.Errorf("%w: fresh all-ASSIGNING startup cannot admit SERVING holder %s", ErrInitialAssignmentIdentityDrift, holder.HolderID)
		}
	}
	return nil
}

func (r *initialAssignmentRuntime) refreshAndVerifyPinnedCandidates(ctx context.Context, pinned map[actorrouting.RouteDomain]initialAssignmentCandidateSnapshot) error {
	if r == nil || ctx == nil || pinned == nil {
		return ErrInitialAssignmentInvalid
	}
	// A transient refresh failure (Kubernetes bracket, HolderControl dial or
	// identity RPC, TokenReview) must never look like identity drift. Retry
	// with a bounded backoff until the surrounding initial-assignment context
	// expires; only a genuine immutable generation change is fatal.
	for attempt := 0; ; attempt++ {
		err := r.refreshAndVerifyPinnedCandidatesOnce(ctx, pinned)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return initialAssignmentContextError(ctx)
		}
		if !errors.Is(err, ErrInitialAssignmentNotReady) {
			return err
		}
		delay := initialAssignmentRefreshRetryBase
		if r.refreshRetry != nil {
			delay = r.refreshRetry(attempt)
		}
		select {
		case <-ctx.Done():
			return initialAssignmentContextError(ctx)
		case <-time.After(delay):
		}
	}
}

func initialAssignmentContextError(ctx context.Context) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return errors.Join(ErrInitialAssignmentTimeout, ctx.Err())
	}
	return ctx.Err()
}

func (r *initialAssignmentRuntime) refreshAndVerifyPinnedCandidatesOnce(ctx context.Context, pinned map[actorrouting.RouteDomain]initialAssignmentCandidateSnapshot) error {
	if r == nil || ctx == nil || pinned == nil {
		return ErrInitialAssignmentInvalid
	}
	for _, domain := range initialAssignmentDomains {
		if r.refreshDomain == nil {
			continue
		}
		if err := r.refreshDomain(ctx, domain); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("%w: refresh %s: %v", ErrInitialAssignmentNotReady, domain, err)
		}
	}
	return r.verifyPinnedCandidates(ctx, pinned)
}

func (r *initialAssignmentRuntime) verifyPinnedCandidates(ctx context.Context, pinned map[actorrouting.RouteDomain]initialAssignmentCandidateSnapshot) error {
	for _, domain := range initialAssignmentDomains {
		want, err := r.expectedHolders(domain)
		if err != nil {
			return err
		}
		current, err := r.candidates.ObserveGeneration(ctx, domain)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, ErrInitialAssignmentNotReady) {
				return err
			}
			return fmt.Errorf("%w: domain=%s: %w", ErrInitialAssignmentIdentityDrift, domain, err)
		}
		if len(current.Holders) != want {
			return fmt.Errorf("%w: domain=%s got=%d want=%d", ErrInitialAssignmentHolderCount, domain, len(current.Holders), want)
		}
		current.Holders, err = sortInitialAssignmentCandidates(current.Holders)
		if err != nil {
			return err
		}
		if err := r.validateInitialAssignmentCandidateLifecycle(current.Holders); err != nil {
			return err
		}
		previous, found := pinned[domain]
		if !found || !current.sameWorkloadIdentity(previous) || !sameInitialAssignmentHolderSet(current.Holders, previous.Holders) {
			return fmt.Errorf("%w: domain=%s configured workload or holder generation changed", ErrInitialAssignmentIdentityDrift, domain)
		}
		// Preserve the pinned immutable identity while carrying forward the
		// latest lease/lifecycle/resource-version observation. READY_ZERO_SHARD
		// is expected to become SERVING after its first claim.
		pinned[domain] = current
	}
	return nil
}

func sameInitialAssignmentHolderSet(left, right []routecore.HolderRecord) bool {
	if len(left) != len(right) {
		return false
	}
	left = append([]routecore.HolderRecord(nil), left...)
	right = append([]routecore.HolderRecord(nil), right...)
	left, leftErr := sortInitialAssignmentCandidates(left)
	right, rightErr := sortInitialAssignmentCandidates(right)
	if leftErr != nil || rightErr != nil || len(left) != len(right) {
		return false
	}
	for i := range left {
		if !sameInitialAssignmentRecordIdentity(left[i], right[i]) {
			return false
		}
	}
	return true
}

func sameInitialAssignmentRecordIdentity(left, right routecore.HolderRecord) bool {
	return left.Domain == right.Domain && left.HolderID == right.HolderID && left.BootID == right.BootID &&
		left.DeploymentIdentity == right.DeploymentIdentity && left.LogicEndpoint == right.LogicEndpoint &&
		left.HolderControlEndpoint == right.HolderControlEndpoint && left.TLSDNSIdentity == right.TLSDNSIdentity
}

func (r *initialAssignmentRuntime) validatePersistedActiveRouting(ctx context.Context, snapshot actorrouting.RouteSnapshot) (bool, error) {
	if r == nil || ctx == nil {
		return false, ErrInitialAssignmentInvalid
	}
	initialFinalRevision := uint64(1) + uint64(snapshot.Player.ShardCount) + uint64(snapshot.Farm.ShardCount)
	// Revision 1+PLAYER+FARM is the sealed initial assignment. At that exact
	// checkpoint retain the stricter deterministic-modulo validation below.
	// Any later complete snapshot is Controller-owned placement history and may
	// legitimately differ after ScalePlan migration.
	if snapshot.GlobalRevision <= initialFinalRevision {
		return false, nil
	}
	nowMs := r.now().UnixMilli()
	for _, domain := range initialAssignmentDomains {
		routes, err := initialAssignmentDomainSnapshot(snapshot, domain)
		if err != nil {
			return false, err
		}
		for shard, route := range routes.Routes {
			if route.State != actorrouting.ShardStateActive || route.HolderIndex < 0 || route.HolderIndex >= len(snapshot.Holders) ||
				route.ShardRevision == 0 || route.ExpectedWriterEpoch == 0 || route.ExpectedLeaseEpoch == 0 || route.LeaseUntilMs <= nowMs {
				return false, fmt.Errorf("%w: persisted route %s/%d", ErrInitialAssignmentMalformedGeneration, domain, shard)
			}
			holder := snapshot.Holders[route.HolderIndex]
			if holder.InstanceID == "" || holder.BootID == "" || holder.Endpoint == "" || holder.State != actorrouting.HolderStateActive || holder.Weight == 0 {
				return false, fmt.Errorf("%w: persisted holder %s/%d", ErrInitialAssignmentMalformedGeneration, domain, shard)
			}
			authority, found, err := r.authorityReader.ReadShardWriteAuthority(ctx, domain, uint32(shard))
			if err != nil {
				return false, fmt.Errorf("%w: authority read %s/%d: %v", ErrInitialAssignmentManager, domain, shard, err)
			}
			if !found || authority.State != authoritystore.ShardAuthorityActive || authority.Domain != domain || authority.VirtualShard != uint32(shard) ||
				authority.HolderID != holder.InstanceID || authority.HolderBootID != holder.BootID || authority.LastTransitionID == "" ||
				authority.AssignmentRevision != route.ShardRevision || authority.WriterEpoch != route.ExpectedWriterEpoch || authority.LeaseEpoch != route.ExpectedLeaseEpoch ||
				authority.LeaseUntilMs < route.LeaseUntilMs || authority.LeaseUntilMs <= nowMs || authority.AuthorityLayoutVersion == "" ||
				authority.HashVersion != snapshot.HashVersion || authority.HashSeed != snapshot.HashSeed {
				return false, fmt.Errorf("%w: persisted authority tuple %s/%d", ErrInitialAssignmentMalformedGeneration, domain, shard)
			}
			if r.config.authorityLayout != "" && authority.AuthorityLayoutVersion != r.config.authorityLayout {
				return false, fmt.Errorf("%w: persisted authority layout %s/%d", ErrInitialAssignmentMalformedGeneration, domain, shard)
			}
		}
	}
	return true, nil
}

func validateInitialAssignmentSnapshot(snapshot actorrouting.RouteSnapshot) (bool, error) {
	activeCount, assigningCount := 0, 0
	for _, domain := range initialAssignmentDomains {
		routes, err := initialAssignmentDomainSnapshot(snapshot, domain)
		if err != nil {
			return false, err
		}
		if routes.ShardCount == 0 || int(routes.ShardCount) != len(routes.Routes) {
			return false, fmt.Errorf("%w: incomplete %s route table", ErrInitialAssignmentMalformedGeneration, domain)
		}
		for shard, route := range routes.Routes {
			if route.HolderIndex < 0 || route.HolderIndex >= len(snapshot.Holders) {
				return false, fmt.Errorf("%w: %s/%d holder index", ErrInitialAssignmentMalformedGeneration, domain, shard)
			}
			switch route.State {
			case actorrouting.ShardStateAssigning:
				if !isZeroGenerationInitialAssignmentRoute(route) {
					return false, fmt.Errorf("%w: %s/%d assigning generation", ErrInitialAssignmentMalformedGeneration, domain, shard)
				}
				assigningCount++
			case actorrouting.ShardStateActive:
				activeCount++
				if route.ShardRevision == 0 || route.ExpectedWriterEpoch == 0 || route.ExpectedLeaseEpoch == 0 || route.LeaseUntilMs <= 0 {
					return false, fmt.Errorf("%w: %s/%d active generation", ErrInitialAssignmentMalformedGeneration, domain, shard)
				}
			default:
				return false, fmt.Errorf("%w: %s/%d state=%s", ErrInitialAssignmentMalformedGeneration, domain, shard, route.State)
			}
		}
	}
	if activeCount != 0 && assigningCount != 0 {
		return false, fmt.Errorf("%w: partial PLAYER/FARM activation", ErrInitialAssignmentMalformedGeneration)
	}
	return activeCount != 0, nil
}

func initialAssignmentDomainSnapshot(snapshot actorrouting.RouteSnapshot, domain actorrouting.RouteDomain) (actorrouting.DomainSnapshot, error) {
	switch domain {
	case actorrouting.DomainPlayer:
		return snapshot.Player, nil
	case actorrouting.DomainFarm:
		return snapshot.Farm, nil
	default:
		return actorrouting.DomainSnapshot{}, fmt.Errorf("%w: unsupported domain %s", ErrInitialAssignmentInvalid, domain)
	}
}

func initialAssignmentRoute(snapshot actorrouting.RouteSnapshot, domain actorrouting.RouteDomain, shard uint32) (actorrouting.ShardRoute, actorrouting.HolderRef, error) {
	routes, err := initialAssignmentDomainSnapshot(snapshot, domain)
	if err != nil {
		return actorrouting.ShardRoute{}, actorrouting.HolderRef{}, err
	}
	if shard >= routes.ShardCount || int(shard) >= len(routes.Routes) {
		return actorrouting.ShardRoute{}, actorrouting.HolderRef{}, fmt.Errorf("%w: %s/%d is outside the route table", ErrInitialAssignmentMalformedGeneration, domain, shard)
	}
	route := routes.Routes[shard]
	if route.HolderIndex < 0 || route.HolderIndex >= len(snapshot.Holders) {
		return actorrouting.ShardRoute{}, actorrouting.HolderRef{}, fmt.Errorf("%w: %s/%d holder index", ErrInitialAssignmentMalformedGeneration, domain, shard)
	}
	return route, snapshot.Holders[route.HolderIndex], nil
}

func isZeroGenerationInitialAssignmentRoute(route actorrouting.ShardRoute) bool {
	return route.State == actorrouting.ShardStateAssigning && route.ShardRevision == 1 &&
		route.ExpectedWriterEpoch == 1 && route.ExpectedLeaseEpoch == 1 && route.LeaseUntilMs == 0
}

func (r *initialAssignmentRuntime) claim(ctx context.Context, domain actorrouting.RouteDomain, shard uint32, target routecore.HolderRecord) error {
	// target comes from the immutable candidate pin captured before the batch.
	// Do not re-check its verified-holder TTL here: that TTL is liveness evidence
	// for discovery, not the Authority fence used by this claim.
	snapshot := r.manager.CurrentSnapshot()
	transitionID := initialAssignmentTransitionIDForTarget(snapshot.Realm, domain, shard, target)
	if existing, found, err := r.authorityReader.ReadShardWriteAuthority(ctx, domain, shard); err != nil {
		return fmt.Errorf("%w: authority precondition %s/%d: %w", ErrInitialAssignmentManager, domain, shard, err)
	} else if found {
		if err := r.validateInitialAssignmentAuthority(snapshot, domain, shard, target, existing, transitionID); err != nil {
			return fmt.Errorf("%w: replay precondition %s/%d: %v", ErrInitialAssignmentForeignActive, domain, shard, err)
		}
	}

	claimCtx, resolveControl, err := r.control.Begin(ctx, initialAssignmentControlOperationID(snapshot.Realm, domain, shard), scaleplancore.ControlOperationBootstrap)
	if err != nil {
		return err
	}
	result, claimErr := r.manager.Claim(claimCtx, authority.ClaimRequest{
		TransitionID: transitionID, Domain: domain, VirtualShard: shard,
		NewHolderID: target.HolderID, NewHolderBootID: target.BootID,
		Endpoint: target.LogicEndpoint, RequestedTTL: initialAssignmentClaimTTL,
	})
	if claimErr != nil {
		return fmt.Errorf("%w: claim %s/%d: %w", ErrInitialAssignmentManager, domain, shard, claimErr)
	}
	if resolveControl != nil {
		if resolveErr := resolveControl(); resolveErr != nil {
			return fmt.Errorf("%w: resolve claim %s/%d control operation: %w", ErrInitialAssignmentManager, domain, shard, resolveErr)
		}
	}
	if result.Status != authoritystore.ShardTransitionApplied && result.Status != authoritystore.ShardTransitionAlreadyApplied {
		return fmt.Errorf("%w: claim %s/%d returned status=%s", ErrInitialAssignmentManager, domain, shard, result.Status)
	}
	snapshot = r.manager.CurrentSnapshot()
	if result.AcceptedTTLms <= 0 {
		return fmt.Errorf("%w: claim %s/%d returned no accepted lease", ErrInitialAssignmentManager, domain, shard)
	}
	if err := r.validateInitialAssignmentAuthority(snapshot, domain, shard, target, result.Authority, transitionID); err != nil {
		return fmt.Errorf("%w: claim %s/%d returned malformed or stale authority: %v", ErrInitialAssignmentManager, domain, shard, err)
	}
	return r.ensureActiveRoute(claimCtx, snapshot, domain, shard, actorrouting.ShardRoute{}, actorrouting.HolderRef{}, target)
}

func (r *initialAssignmentRuntime) ensureActiveRoute(ctx context.Context, snapshot actorrouting.RouteSnapshot, domain actorrouting.RouteDomain, shard uint32, route actorrouting.ShardRoute, holder actorrouting.HolderRef, target routecore.HolderRecord) error {
	for attempt := 0; ; attempt++ {
		if route.State == "" {
			var err error
			route, holder, err = initialAssignmentRoute(r.manager.CurrentSnapshot(), domain, shard)
			if err != nil {
				return err
			}
		}
		err := r.validateActiveRoute(ctx, snapshot, domain, shard, route, holder, target)
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrInitialAssignmentProjectionStale) || r.refresh == nil || attempt >= 1 {
			return err
		}
		if refreshErr := r.refresh(ctx); refreshErr != nil {
			return fmt.Errorf("%w: refresh after claim %s/%d: %w", ErrInitialAssignmentManager, domain, shard, refreshErr)
		}
		snapshot = r.manager.CurrentSnapshot()
		var routeErr error
		route, holder, routeErr = initialAssignmentRoute(snapshot, domain, shard)
		if routeErr != nil {
			return routeErr
		}
	}
}

func (r *initialAssignmentRuntime) validateActiveRoute(ctx context.Context, snapshot actorrouting.RouteSnapshot, domain actorrouting.RouteDomain, shard uint32, route actorrouting.ShardRoute, holder actorrouting.HolderRef, target routecore.HolderRecord) error {
	if route.State != actorrouting.ShardStateActive || route.ShardRevision == 0 || route.ExpectedWriterEpoch == 0 ||
		route.ExpectedLeaseEpoch == 0 || route.LeaseUntilMs <= 0 {
		return fmt.Errorf("%w: %s/%d active route generation", ErrInitialAssignmentMalformedGeneration, domain, shard)
	}
	if holder.State != actorrouting.HolderStateActive || holder.Weight == 0 || holder.InstanceID != target.HolderID ||
		holder.BootID != target.BootID || holder.Endpoint != target.LogicEndpoint {
		return fmt.Errorf("%w: %s/%d target=%s/%s", ErrInitialAssignmentForeignActive, domain, shard, holder.InstanceID, holder.BootID)
	}
	if authority, found, err := r.authorityReader.ReadShardWriteAuthority(ctx, domain, shard); err != nil {
		return fmt.Errorf("%w: authority read %s/%d: %w", ErrInitialAssignmentManager, domain, shard, err)
	} else {
		if !found || authority.State != authoritystore.ShardAuthorityActive || authority.Domain != domain || authority.VirtualShard != shard ||
			authority.HolderID != target.HolderID || authority.HolderBootID != target.BootID ||
			authority.AssignmentRevision != route.ShardRevision || authority.WriterEpoch != route.ExpectedWriterEpoch ||
			authority.LeaseEpoch != route.ExpectedLeaseEpoch || authority.LastTransitionID != initialAssignmentTransitionIDForTarget(snapshot.Realm, domain, shard, target) ||
			authority.AuthorityLayoutVersion == "" || authority.HashVersion != snapshot.HashVersion || authority.HashSeed != snapshot.HashSeed {
			return fmt.Errorf("%w: authority tuple %s/%d", ErrInitialAssignmentMalformedGeneration, domain, shard)
		}
		nowMs := r.now().UnixMilli()
		if authority.LeaseUntilMs <= nowMs {
			return fmt.Errorf("%w: authority lease %s/%d expired at %d", ErrInitialAssignmentStaleLease, domain, shard, authority.LeaseUntilMs)
		}
		if route.LeaseUntilMs <= nowMs {
			if authority.LeaseUntilMs > nowMs {
				return fmt.Errorf("%w: %s/%d", ErrInitialAssignmentProjectionStale, domain, shard)
			}
			return fmt.Errorf("%w: route lease %s/%d expired at %d", ErrInitialAssignmentStaleLease, domain, shard, route.LeaseUntilMs)
		}
		if authority.LeaseUntilMs < route.LeaseUntilMs {
			return fmt.Errorf("%w: authority lease regressed for %s/%d", ErrInitialAssignmentStaleLease, domain, shard)
		}
		if r.config.authorityLayout != "" && authority.AuthorityLayoutVersion != r.config.authorityLayout {
			return fmt.Errorf("%w: authority layout %s/%d", ErrInitialAssignmentMalformedGeneration, domain, shard)
		}
	}
	return nil
}

func (r *initialAssignmentRuntime) ensureComplete(ctx context.Context, pinned map[actorrouting.RouteDomain]initialAssignmentCandidateSnapshot) error {
	if err := r.verifyPinnedCandidates(ctx, pinned); err != nil {
		return err
	}
	for _, domain := range initialAssignmentDomains {
		snapshot := r.manager.CurrentSnapshot()
		routes, err := initialAssignmentDomainSnapshot(snapshot, domain)
		if err != nil {
			return err
		}
		for shard, route := range routes.Routes {
			target, targetErr := deterministicInitialAssignmentTarget(pinned[domain].Holders, uint32(shard))
			if targetErr != nil {
				return targetErr
			}
			_, holder, routeErr := initialAssignmentRoute(snapshot, domain, uint32(shard))
			if routeErr != nil {
				return routeErr
			}
			if route.State != actorrouting.ShardStateActive {
				return fmt.Errorf("%w: %s/%d is not ACTIVE", ErrInitialAssignmentMalformedGeneration, domain, shard)
			}
			if err := r.ensureActiveRoute(ctx, snapshot, domain, uint32(shard), route, holder, target); err != nil {
				return err
			}
		}
	}
	if err := r.verifyPinnedCandidates(ctx, pinned); err != nil {
		return err
	}
	return nil
}

func initialAssignmentTransitionID(realm string, domain actorrouting.RouteDomain, shard uint32, targetIdentity ...string) string {
	if len(targetIdentity) == 3 {
		return routecore.InitialAuthorityTransitionID(realm, domain, shard, targetIdentity[0], targetIdentity[1], targetIdentity[2])
	}
	return boundedInitialAssignmentOperationID(fmt.Sprintf("route-controller/initial-assignment/%s/%s/%d", realm, domain, shard))
}

func initialAssignmentTransitionIDForTarget(realm string, domain actorrouting.RouteDomain, shard uint32, target routecore.HolderRecord) string {
	return routecore.InitialAuthorityTransitionID(realm, domain, shard, target.HolderID, target.BootID, target.LogicEndpoint)
}

func initialAssignmentControlOperationID(realm string, domain actorrouting.RouteDomain, shard uint32) string {
	return boundedInitialAssignmentOperationID(fmt.Sprintf("route-controller/initial-assignment-bootstrap/%s/%s/%d", realm, domain, shard))
}

func boundedInitialAssignmentOperationID(raw string) string {
	if len(raw) <= 256 {
		return raw
	}
	digest := sha256.Sum256([]byte(raw))
	return "route-controller/initial-assignment-op/" + hex.EncodeToString(digest[:])
}

var _ initialAssignmentManager = (*authority.RouteAssignmentManager)(nil)
var _ initialAssignmentCandidateProvider = (*verifiedInitialAssignmentCandidateProvider)(nil)
