package control

import (
	"context"
	"fmt"
	"sort"
	"time"

	"farmgame/internal/actorrouting"
	routecore "farmgame/internal/routecontrol"
	"farmgame/internal/routecontrol/verifiedholder"
	core "farmgame/internal/scaleplan"
)

// VerifiedHolderStateReaderOptions switches ScalePlan target discovery to the
// controller-owned Kubernetes + HolderControl view. Exclusions remain on the
// existing durable seam until the independent exclusion store migration is
// complete. Registry is optional: when the same holder/boot is already
// registered, its LogicEndpoint is the RouteSnapshot identity and must win
// over the Kubernetes Pod IP used only to dial HolderControl.
type VerifiedHolderStateReaderOptions struct {
	View       *verifiedholder.Store
	Exclusions PlacementExclusionReader
	Registry   HolderRecordReader
	Now        func() time.Time
}

type verifiedHolderStateReader struct {
	view       *verifiedholder.Store
	exclusions PlacementExclusionReader
	registry   HolderRecordReader
	now        func() time.Time
}

func NewVerifiedHolderStateReader(options VerifiedHolderStateReaderOptions) (core.HolderStateReader, error) {
	if options.View == nil || options.Exclusions == nil {
		return nil, ErrHolderAdapterInvalid
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	return &verifiedHolderStateReader{
		view: options.View, exclusions: options.Exclusions, registry: options.Registry, now: options.Now,
	}, nil
}

func (r *verifiedHolderStateReader) GetHolder(ctx context.Context, ref core.HolderRef) (core.HolderState, bool, error) {
	if err := holderContext(ctx); err != nil {
		return core.HolderState{}, false, err
	}
	if r == nil || r.view == nil || r.exclusions == nil || !validHolderLookup(ref) {
		return core.HolderState{}, false, ErrHolderAdapterInvalid
	}
	var selected *verifiedholder.Holder
	// Exact lookups are a control-plane evidence path, not placement
	// discovery. Keep a fresh, attested SERVING generation visible while it
	// temporarily reconciles a newer global RouteSnapshot and ProcessReady is
	// false. Callers still have to apply their operation-specific lifecycle
	// admission. ListDomainHolders deliberately remains placement-strict.
	for _, candidate := range r.view.ListVerifiedDomain(ref.Domain) {
		if !matchesHolderLookup(candidate, ref) {
			continue
		}
		if selected != nil {
			return core.HolderState{}, false, fmt.Errorf("%w: ambiguous verified holder lookup", ErrHolderAdapterDrift)
		}
		copy := candidate
		selected = &copy
	}
	if selected == nil {
		return core.HolderState{}, false, nil
	}
	state, err := r.convert(ctx, *selected, false)
	if err != nil {
		return core.HolderState{}, false, err
	}
	return state, true, nil
}

func (r *verifiedHolderStateReader) ListDomainHolders(ctx context.Context, domain actorrouting.RouteDomain) ([]core.HolderState, error) {
	if err := holderContext(ctx); err != nil {
		return nil, err
	}
	if r == nil || r.view == nil || r.exclusions == nil || (domain != actorrouting.DomainPlayer && domain != actorrouting.DomainFarm) {
		return nil, ErrHolderAdapterInvalid
	}
	holders := r.view.ListDomain(domain)
	out := make([]core.HolderState, 0, len(holders))
	for _, holder := range holders {
		state, err := r.convert(ctx, holder, true)
		if err != nil {
			return nil, err
		}
		out = append(out, state)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Ref.Ordinal != out[j].Ref.Ordinal {
			return out[i].Ref.Ordinal < out[j].Ref.Ordinal
		}
		if out[i].Ref.HolderID != out[j].Ref.HolderID {
			return out[i].Ref.HolderID < out[j].Ref.HolderID
		}
		return out[i].Ref.BootID < out[j].Ref.BootID
	})
	return out, nil
}

func (r *verifiedHolderStateReader) convert(ctx context.Context, holder verifiedholder.Holder, requirePlacementAdmission bool) (core.HolderState, error) {
	now := r.now()
	if holder.Validate() != nil || now.Before(holder.VerifiedAt) || !holder.VerifiedUntil.After(now) ||
		(requirePlacementAdmission && !holder.Eligible(now)) {
		return core.HolderState{}, fmt.Errorf("%w: stale or inadmissible verified holder", ErrHolderAdapterDrift)
	}
	ref := core.HolderRef{
		Domain: holder.Domain, HolderID: holder.HolderID, BootID: holder.BootID,
		PodUID: holder.PodUID, WorkloadUID: holder.WorkloadUID, Ordinal: holder.Ordinal,
	}
	if err := ref.ValidateExact(); err != nil {
		return core.HolderState{}, fmt.Errorf("%w: verified holder identity: %v", ErrHolderAdapterDrift, err)
	}
	state, err := mapVerifiedLifecycle(holder.Lifecycle)
	if err != nil {
		return core.HolderState{}, err
	}
	placementEligible, placementRevision, err := r.placementState(ctx, holder)
	if err != nil {
		return core.HolderState{}, err
	}
	logicEndpoint, controlEndpoint, tlsIdentity, err := overlayRegisteredEndpoints(ctx, r.registry, holder)
	if err != nil {
		return core.HolderState{}, err
	}
	return core.HolderState{
		Ref: ref, PodName: holder.PodName,
		LogicEndpoint: logicEndpoint, HolderControlEndpoint: controlEndpoint,
		TLSDNSIdentity: tlsIdentity, State: state,
		ProcessReady: holder.ProcessReady, Draining: holder.Draining,
		ServingShardCount: holder.ServingShardCount,
		SnapshotRevision:  holder.ObservedRouteRevision, AssignmentKnownFresh: holder.ObservedRouteRevision > 0,
		PlacementEligible: placementEligible, PlacementRevision: placementRevision,
		LeaseUntil: holder.VerifiedUntil,
	}, nil
}

func (r *verifiedHolderStateReader) placementState(ctx context.Context, holder verifiedholder.Holder) (bool, uint64, error) {
	exclusion, found, err := r.exclusions.GetPlacementExclusion(ctx, holder.Domain, holder.HolderID, holder.BootID)
	if err != nil {
		return false, 0, err
	}
	if found {
		if err := routecore.ValidatePlacementExclusion(exclusion); err != nil {
			return false, 0, fmt.Errorf("%w: exclusion validation: %v", ErrHolderAdapterDrift, err)
		}
		if exclusion.Domain != holder.Domain || exclusion.HolderID != holder.HolderID || exclusion.BootID != holder.BootID {
			return false, 0, fmt.Errorf("%w: exclusion identity", ErrHolderAdapterDrift)
		}
		return !exclusion.Excluded, exclusion.Revision, nil
	}

	// During M4 migration a newly created, freshly attested zero-shard Pod has
	// no legacy registry row yet. It is safe to admit only this exact state as a
	// scale-out target. Serving/draining holders remain fail-closed until the
	// independent PlacementExclusion store is available.
	if holder.Lifecycle == verifiedholder.LifecycleReadyZeroShard && holder.ProcessReady && !holder.Draining &&
		holder.ServingShardCount == 0 && holder.InflightTurnCount == 0 {
		return true, 1, nil
	}
	return false, 0, nil
}

func overlayRegisteredEndpoints(ctx context.Context, registry HolderRecordReader, holder verifiedholder.Holder) (logic, control, tls string, err error) {
	logic, control, tls = holder.LogicEndpoint, holder.ControlEndpoint, holder.TLSDNSIdentity
	if registry == nil {
		return logic, control, tls, nil
	}
	record, found, err := registry.Get(ctx, holder.Domain, holder.HolderID)
	if err != nil {
		return "", "", "", err
	}
	if !found || record.BootID != holder.BootID || record.LogicEndpoint == "" {
		return logic, control, tls, nil
	}
	return record.LogicEndpoint, record.HolderControlEndpoint, record.TLSDNSIdentity, nil
}

func validHolderLookup(ref core.HolderRef) bool {
	if ref.Domain != actorrouting.DomainPlayer && ref.Domain != actorrouting.DomainFarm {
		return false
	}
	return ref.HolderID != "" && ref.Ordinal >= 0
}

func matchesHolderLookup(holder verifiedholder.Holder, ref core.HolderRef) bool {
	if holder.Domain != ref.Domain || holder.HolderID != ref.HolderID || holder.Ordinal != ref.Ordinal {
		return false
	}
	return (ref.BootID == "" || holder.BootID == ref.BootID) &&
		(ref.PodUID == "" || holder.PodUID == ref.PodUID) &&
		(ref.WorkloadUID == "" || holder.WorkloadUID == ref.WorkloadUID)
}

func mapVerifiedLifecycle(value verifiedholder.LifecycleState) (core.HolderLifecycleState, error) {
	switch value {
	case verifiedholder.LifecycleWarming:
		return core.HolderWarming, nil
	case verifiedholder.LifecycleReadyZeroShard:
		return core.HolderReadyZeroShard, nil
	case verifiedholder.LifecycleServing:
		return core.HolderServing, nil
	case verifiedholder.LifecycleDraining:
		return core.HolderDraining, nil
	default:
		return "", fmt.Errorf("%w: verified lifecycle %q", ErrHolderAdapterDrift, value)
	}
}

var _ core.HolderStateReader = (*verifiedHolderStateReader)(nil)
