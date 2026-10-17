package main

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/holderidentity"
	routecore "farmgame/internal/routecontrol"
	"farmgame/internal/routecontrol/verifiedholder"
	"farmgame/internal/routeplan"
)

var errRoutePlanHolderReaderUnavailable = errors.New("route-controller: RoutePlan holder reader unavailable")

// switchableRoutePlanHolderReader lets startup use the legacy durable registry
// while Kubernetes discovery is warming, then atomically switches every
// RoutePlan/recovery caller to the verified Kubernetes + HolderControl view.
// The wrapper is process-local and carries no registration or authority state.
type switchableRoutePlanHolderReader struct {
	mu     sync.RWMutex
	reader routeplan.HolderReader
}

func newSwitchableRoutePlanHolderReader(initial routeplan.HolderReader) (*switchableRoutePlanHolderReader, error) {
	if initial == nil {
		return nil, errRoutePlanHolderReaderUnavailable
	}
	return &switchableRoutePlanHolderReader{reader: initial}, nil
}

func (r *switchableRoutePlanHolderReader) Set(reader routeplan.HolderReader) error {
	if r == nil || reader == nil {
		return errRoutePlanHolderReaderUnavailable
	}
	r.mu.Lock()
	r.reader = reader
	r.mu.Unlock()
	return nil
}

func (r *switchableRoutePlanHolderReader) current() (routeplan.HolderReader, error) {
	if r == nil {
		return nil, errRoutePlanHolderReaderUnavailable
	}
	r.mu.RLock()
	reader := r.reader
	r.mu.RUnlock()
	if reader == nil {
		return nil, errRoutePlanHolderReaderUnavailable
	}
	return reader, nil
}

func (r *switchableRoutePlanHolderReader) Get(ctx context.Context, domain actorrouting.RouteDomain, holderID string) (routecore.HolderRecord, bool, error) {
	reader, err := r.current()
	if err != nil {
		return routecore.HolderRecord{}, false, err
	}
	return reader.Get(ctx, domain, holderID)
}

func (r *switchableRoutePlanHolderReader) GetCached(ctx context.Context, domain actorrouting.RouteDomain, holderID string) (routecore.HolderRecord, bool, error) {
	reader, err := r.current()
	if err != nil {
		return routecore.HolderRecord{}, false, err
	}
	if cached, ok := reader.(routeplan.CachedHolderReader); ok {
		return cached.GetCached(ctx, domain, holderID)
	}
	return reader.Get(ctx, domain, holderID)
}

func (r *switchableRoutePlanHolderReader) List(ctx context.Context, domain actorrouting.RouteDomain) ([]routecore.HolderRecord, error) {
	reader, err := r.current()
	if err != nil {
		return nil, err
	}
	return reader.List(ctx, domain)
}

type verifiedRoutePlanHolderReader struct {
	view     *verifiedholder.Store
	refresh  func(context.Context, actorrouting.RouteDomain) error
	registry routeplan.HolderReader
}

type verifiedPlacementRegistry interface {
	GetPlacementExclusion(context.Context, actorrouting.RouteDomain, string, string) (routecore.PlacementExclusion, bool, error)
	Register(context.Context, routecore.HolderRegistration) (routecore.HolderRecord, error)
}

// materializeVerifiedPlacementCandidates rebuilds the short-lived durable
// placement identity for exact Kubernetes + HolderControl generations before
// RoutePlan recovery. It does not make a holder SERVING, does not modify a
// RouteSnapshot and cannot grant Shard write authority. Existing placement
// exclusions are preserved byte-for-byte.
func materializeVerifiedPlacementCandidates(ctx context.Context, view *verifiedholder.Store, registry verifiedPlacementRegistry) error {
	return materializeVerifiedPlacementCandidatesWithRetry(ctx, view, registry, 100*time.Millisecond)
}

// materializeVerifiedPlacementCandidate installs only the non-authoritative
// placement-generation record for one already attested runtime. Crash takeover
// needs this before Manager admission because a replacement Pod can expose its
// sealed WARMING HolderControl endpoint before its ordinary HolderAgent is able
// to start. This operation neither changes RouteSnapshot nor grants writer
// authority.
func materializeVerifiedPlacementCandidate(ctx context.Context, holder verifiedholder.Holder, registry verifiedPlacementRegistry) error {
	if ctx == nil || registry == nil {
		return errRoutePlanHolderReaderUnavailable
	}
	_, found, err := registry.GetPlacementExclusion(ctx, holder.Domain, holder.HolderID, holder.BootID)
	if err != nil || found {
		return err
	}
	candidate, valid, err := verifiedHolderRecord(holder)
	if err != nil || !valid {
		if err == nil {
			err = errRoutePlanHolderReaderUnavailable
		}
		return err
	}
	candidate.State = routecore.HolderStateWarming
	candidate.ProcessReady = false
	candidate.Draining = false
	candidate.ServingShardCount = 0
	candidate.Sequence = 1
	for {
		if _, err = registry.Register(ctx, candidate); err == nil {
			break
		}
		_, raced, lookupErr := registry.GetPlacementExclusion(ctx, holder.Domain, holder.HolderID, holder.BootID)
		if lookupErr == nil && raced {
			return nil
		}
		if lookupErr != nil || !errors.Is(err, routecore.ErrHolderBootConflict) {
			return errors.Join(err, lookupErr)
		}
		// A replacement Pod must never overwrite the prior boot while its lease
		// is live. Wait inside this bounded caller context and let HolderRegistry
		// admit the exact new generation immediately after expiry.
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(err, ctx.Err())
		case <-timer.C:
		}
	}
	_, found, err = registry.GetPlacementExclusion(ctx, holder.Domain, holder.HolderID, holder.BootID)
	if err != nil {
		return err
	}
	if !found {
		return errRoutePlanHolderReaderUnavailable
	}
	return nil
}

func materializeVerifiedPlacementCandidatesWithRetry(ctx context.Context, view *verifiedholder.Store, registry verifiedPlacementRegistry, retryInterval time.Duration) error {
	if ctx == nil || view == nil || registry == nil {
		return errRoutePlanHolderReaderUnavailable
	}
	if retryInterval <= 0 {
		retryInterval = time.Millisecond
	}
	for _, domain := range []actorrouting.RouteDomain{actorrouting.DomainPlayer, actorrouting.DomainFarm} {
		for _, holder := range view.ListVerifiedDomain(domain) {
			if err := ctx.Err(); err != nil {
				return err
			}
			_, found, err := registry.GetPlacementExclusion(ctx, domain, holder.HolderID, holder.BootID)
			if err != nil {
				return err
			}
			if found {
				continue
			}
			candidate, valid, err := verifiedHolderRecord(holder)
			if err != nil || !valid {
				if err == nil {
					err = errRoutePlanHolderReaderUnavailable
				}
				return err
			}
			// Registration is deliberately WARMING even when HolderControl has
			// already observed SERVING. RoutePlan uses the verified in-memory
			// record for live state; this durable candidate exists only so the
			// exclusion store can identify the exact non-excluded generation.
			candidate.State = routecore.HolderStateWarming
			candidate.ProcessReady = false
			candidate.Draining = false
			candidate.ServingShardCount = 0
			candidate.Sequence = 1
			for {
				if _, err = registry.Register(ctx, candidate); err == nil {
					break
				}
				// A concurrent HolderAgent may have restored the same exact
				// candidate. Re-read the placement identity before failing.
				_, raced, lookupErr := registry.GetPlacementExclusion(ctx, domain, holder.HolderID, holder.BootID)
				if lookupErr == nil && raced {
					break
				}
				if lookupErr != nil || !errors.Is(err, routecore.ErrHolderBootConflict) {
					return errors.Join(err, lookupErr)
				}

				// Bootstrap may have installed a short-lived provisional boot
				// before the real process generated its boot_id. Keep the write
				// fence closed and wait only for that exact conflicting lease to
				// expire; every other registration failure remains fail-closed.
				timer := time.NewTimer(retryInterval)
				select {
				case <-ctx.Done():
					timer.Stop()
					return errors.Join(err, ctx.Err())
				case <-timer.C:
				}
			}
			if _, found, err = registry.GetPlacementExclusion(ctx, domain, holder.HolderID, holder.BootID); err != nil || !found {
				if err == nil {
					err = errRoutePlanHolderReaderUnavailable
				}
				return err
			}
		}
	}
	return nil
}

func newVerifiedRoutePlanHolderReader(view *verifiedholder.Store, refresh ...func(context.Context, actorrouting.RouteDomain) error) (routeplan.HolderReader, error) {
	return newVerifiedRoutePlanHolderReaderWithRegistry(view, nil, refresh...)
}

func newVerifiedRoutePlanHolderReaderWithRegistry(view *verifiedholder.Store, registry routeplan.HolderReader, refresh ...func(context.Context, actorrouting.RouteDomain) error) (routeplan.HolderReader, error) {
	if view == nil || len(refresh) > 1 {
		return nil, errRoutePlanHolderReaderUnavailable
	}
	var refreshFn func(context.Context, actorrouting.RouteDomain) error
	if len(refresh) == 1 {
		if refresh[0] == nil {
			return nil, errRoutePlanHolderReaderUnavailable
		}
		refreshFn = refresh[0]
	}
	return &verifiedRoutePlanHolderReader{view: view, refresh: refreshFn, registry: registry}, nil
}

func (r *verifiedRoutePlanHolderReader) Get(ctx context.Context, domain actorrouting.RouteDomain, holderID string) (routecore.HolderRecord, bool, error) {
	if ctx == nil || r == nil || r.view == nil || holderID == "" {
		return routecore.HolderRecord{}, false, errRoutePlanHolderReaderUnavailable
	}
	if err := ctx.Err(); err != nil {
		return routecore.HolderRecord{}, false, err
	}
	// Informer events discover Pod generations quickly, but the target's
	// ObservedRouteRevision changes without changing the Pod object. Refresh the
	// one affected route domain before an exact RoutePlan lookup so target
	// convergence never waits for the periodic global resync interval.
	var refreshErr error
	if r.refresh != nil {
		refreshErr = r.refresh(ctx, domain)
	}
	return r.getCached(ctx, domain, holderID, refreshErr)
}

func (r *verifiedRoutePlanHolderReader) GetCached(ctx context.Context, domain actorrouting.RouteDomain, holderID string) (routecore.HolderRecord, bool, error) {
	if ctx == nil || r == nil || r.view == nil || holderID == "" {
		return routecore.HolderRecord{}, false, errRoutePlanHolderReaderUnavailable
	}
	if err := ctx.Err(); err != nil {
		return routecore.HolderRecord{}, false, err
	}
	return r.getCached(ctx, domain, holderID, nil)
}

func (r *verifiedRoutePlanHolderReader) getCached(ctx context.Context, domain actorrouting.RouteDomain, holderID string, refreshErr error) (routecore.HolderRecord, bool, error) {
	var selected *verifiedholder.Holder
	for _, holder := range r.view.ListVerifiedDomain(domain) {
		if holder.HolderID != holderID {
			continue
		}
		if selected != nil {
			return routecore.HolderRecord{}, false, errors.New("route-controller: duplicate verified holder id")
		}
		copy := holder
		selected = &copy
	}
	if selected == nil {
		if refreshErr != nil {
			return routecore.HolderRecord{}, false, refreshErr
		}
		return routecore.HolderRecord{}, false, nil
	}
	return overlayRegisteredRoutePlanRecord(*selected, r.registry, ctx)
}

func (r *verifiedRoutePlanHolderReader) List(ctx context.Context, domain actorrouting.RouteDomain) ([]routecore.HolderRecord, error) {
	if ctx == nil || r == nil || r.view == nil {
		return nil, errRoutePlanHolderReaderUnavailable
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Initial assignment may call this once per shard, so it must consume the
	// already verified in-memory view instead of repeating Kubernetes,
	// HolderControl and TokenReview work for every shard.
	holders := r.view.ListVerifiedDomain(domain)
	result := make([]routecore.HolderRecord, 0, len(holders))
	for _, holder := range holders {
		record, _, err := overlayRegisteredRoutePlanRecord(holder, r.registry, ctx)
		if err != nil {
			return nil, err
		}
		result = append(result, record)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].DeploymentIdentity.Ordinal != result[j].DeploymentIdentity.Ordinal {
			return result[i].DeploymentIdentity.Ordinal < result[j].DeploymentIdentity.Ordinal
		}
		return result[i].HolderID < result[j].HolderID
	})
	return result, nil
}

func verifiedHolderRecord(holder verifiedholder.Holder) (routecore.HolderRecord, bool, error) {
	if err := holder.Validate(); err != nil {
		return routecore.HolderRecord{}, false, err
	}
	state, err := verifiedRoutePlanLifecycle(holder.Lifecycle)
	if err != nil {
		return routecore.HolderRecord{}, false, err
	}
	return routecore.HolderRecord{
		Domain: holder.Domain, HolderID: holder.HolderID, BootID: holder.BootID,
		DeploymentIdentity: holderidentity.DeploymentIdentity{PodUID: holder.PodUID, WorkloadUID: holder.WorkloadUID, Ordinal: holder.Ordinal},
		LogicEndpoint:      holder.LogicEndpoint, HolderControlEndpoint: holder.ControlEndpoint,
		TLSDNSIdentity: holder.TLSDNSIdentity, State: state,
		ProcessReady: holder.ProcessReady, Draining: holder.Draining,
		ServingShardCount: holder.ServingShardCount, SnapshotRevision: holder.ObservedRouteRevision,
		StartedAt: holder.VerifiedAt, HeartbeatAt: holder.VerifiedAt, LeaseUntil: holder.VerifiedUntil,
		Sequence: 1, CapacityWeight: holder.CapacityWeight,
	}, true, nil
}

func overlayRegisteredRoutePlanRecord(holder verifiedholder.Holder, registry routeplan.HolderReader, ctx context.Context) (routecore.HolderRecord, bool, error) {
	record, found, err := verifiedHolderRecord(holder)
	if err != nil || !found {
		return record, found, err
	}
	if registry == nil {
		return record, true, nil
	}
	registered, registeredFound, registryErr := registry.Get(ctx, holder.Domain, holder.HolderID)
	if registryErr != nil {
		return routecore.HolderRecord{}, false, registryErr
	}
	if registeredFound && registered.BootID == holder.BootID && registered.DeploymentIdentity == record.DeploymentIdentity && registered.LogicEndpoint != "" {
		record.LogicEndpoint = registered.LogicEndpoint
		record.HolderControlEndpoint = registered.HolderControlEndpoint
		record.TLSDNSIdentity = registered.TLSDNSIdentity
		// Kubernetes + HolderControl verification pins the process generation;
		// the same exact generation's HolderAgent heartbeat is the event-driven
		// source for per-Shard progress, matching UC's immediate ACTIVE report.
		if registered.State != "" {
			record.State = registered.State
			record.ProcessReady = registered.ProcessReady
			record.Draining = registered.Draining
			record.ServingShardCount = registered.ServingShardCount
			record.SnapshotRevision = registered.SnapshotRevision
			record.HeartbeatAt = registered.HeartbeatAt
			record.LeaseUntil = registered.LeaseUntil
			record.Sequence = registered.Sequence
			record.CapacityWeight = registered.CapacityWeight
		}
	}
	return record, true, nil
}

func verifiedRoutePlanLifecycle(value verifiedholder.LifecycleState) (routecore.HolderState, error) {
	switch value {
	case verifiedholder.LifecycleWarming:
		return routecore.HolderStateWarming, nil
	case verifiedholder.LifecycleReadyZeroShard:
		return routecore.HolderStateReadyZeroShard, nil
	case verifiedholder.LifecycleServing:
		return routecore.HolderStateServing, nil
	case verifiedholder.LifecycleDraining:
		return routecore.HolderStateDraining, nil
	default:
		return "", errRoutePlanHolderReaderUnavailable
	}
}

var _ routeplan.HolderReader = (*switchableRoutePlanHolderReader)(nil)
var _ routeplan.HolderReader = (*verifiedRoutePlanHolderReader)(nil)
