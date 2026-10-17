package control

import (
	"context"
	"fmt"
	"sort"

	"farmgame/internal/actorrouting"
	routecore "farmgame/internal/routecontrol"
	core "farmgame/internal/scaleplan"
)

var (
	ErrHolderAdapterInvalid = fmt.Errorf("scaleplan control adapter: invalid holder dependency")
	ErrHolderAdapterDrift   = fmt.Errorf("scaleplan control adapter: holder identity or exclusion drift")
)

// HolderStateReaderOptions binds the exact HolderRegistry read surfaces.
type HolderStateReaderOptions struct {
	Registry   HolderRecordReader
	Exclusions PlacementExclusionReader
}

type holderStateReaderAdapter struct {
	registry   HolderRecordReader
	exclusions PlacementExclusionReader
}

// NewHolderStateReader constructs the production HolderStateReader adapter.
// The exclusion read is required because PlacementEligible and
// PlacementRevision are part of the exact holder snapshot.
func NewHolderStateReader(options HolderStateReaderOptions) (core.HolderStateReader, error) {
	if options.Registry == nil || options.Exclusions == nil {
		return nil, ErrHolderAdapterInvalid
	}
	return &holderStateReaderAdapter{registry: options.Registry, exclusions: options.Exclusions}, nil
}

func (r *holderStateReaderAdapter) GetHolder(ctx context.Context, ref core.HolderRef) (core.HolderState, bool, error) {
	if err := holderContext(ctx); err != nil {
		return core.HolderState{}, false, err
	}
	if r == nil || r.registry == nil || r.exclusions == nil || !ref.Exact() {
		return core.HolderState{}, false, ErrHolderAdapterInvalid
	}
	record, found, err := r.registry.Get(ctx, ref.Domain, ref.HolderID)
	if err != nil || !found {
		return core.HolderState{}, found, err
	}
	if !sameHolderRecordRef(record, ref) {
		return core.HolderState{}, false, nil
	}
	exclusion, found, err := r.exclusions.GetPlacementExclusion(ctx, ref.Domain, ref.HolderID, ref.BootID)
	if err != nil || !found {
		if err != nil {
			return core.HolderState{}, false, err
		}
		return core.HolderState{}, false, fmt.Errorf("%w: exclusion missing", ErrHolderAdapterDrift)
	}
	state, err := holderStateFromRecord(record, exclusion)
	if err != nil {
		return core.HolderState{}, false, err
	}
	return state, true, nil
}

func (r *holderStateReaderAdapter) ListDomainHolders(ctx context.Context, domain actorrouting.RouteDomain) ([]core.HolderState, error) {
	if err := holderContext(ctx); err != nil {
		return nil, err
	}
	if r == nil || r.registry == nil || r.exclusions == nil {
		return nil, ErrHolderAdapterInvalid
	}
	records, err := r.registry.List(ctx, domain)
	if err != nil {
		return nil, err
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].DeploymentIdentity.Ordinal == records[j].DeploymentIdentity.Ordinal {
			if records[i].HolderID == records[j].HolderID {
				return records[i].BootID < records[j].BootID
			}
			return records[i].HolderID < records[j].HolderID
		}
		return records[i].DeploymentIdentity.Ordinal < records[j].DeploymentIdentity.Ordinal
	})
	out := make([]core.HolderState, 0, len(records))
	for _, record := range records {
		if record.Domain != domain {
			return nil, fmt.Errorf("%w: list returned another domain", ErrHolderAdapterDrift)
		}
		ref, err := holderRefFromRecord(record)
		if err != nil {
			return nil, err
		}
		exclusion, found, err := r.exclusions.GetPlacementExclusion(ctx, domain, record.HolderID, record.BootID)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("%w: exclusion missing for %s", ErrHolderAdapterDrift, record.HolderID)
		}
		if exclusion.Domain != domain || exclusion.HolderID != record.HolderID || exclusion.BootID != record.BootID {
			return nil, fmt.Errorf("%w: exclusion identity", ErrHolderAdapterDrift)
		}
		state, err := holderStateFromRecord(record, exclusion)
		if err != nil {
			return nil, err
		}
		if state.Ref != ref {
			return nil, fmt.Errorf("%w: holder reference conversion", ErrHolderAdapterDrift)
		}
		out = append(out, state)
	}
	return out, nil
}

func holderRefFromRecord(record routecore.HolderRecord) (core.HolderRef, error) {
	if record.Domain != actorrouting.DomainPlayer && record.Domain != actorrouting.DomainFarm {
		return core.HolderRef{}, fmt.Errorf("%w: unsupported holder domain", ErrHolderAdapterDrift)
	}
	identity := record.DeploymentIdentity
	if err := identity.ValidateExact(); err != nil {
		return core.HolderRef{}, fmt.Errorf("%w: deployment identity: %v", ErrHolderAdapterDrift, err)
	}
	ref := core.HolderRef{
		Domain: record.Domain, HolderID: record.HolderID, BootID: record.BootID,
		PodUID: identity.PodUID, WorkloadUID: identity.WorkloadUID, Ordinal: identity.Ordinal,
	}
	if err := ref.ValidateExact(); err != nil {
		return core.HolderRef{}, fmt.Errorf("%w: holder reference: %v", ErrHolderAdapterDrift, err)
	}
	return ref, nil
}

func sameHolderRecordRef(record routecore.HolderRecord, ref core.HolderRef) bool {
	converted, err := holderRefFromRecord(record)
	return err == nil && converted == ref
}

func holderStateFromRecord(record routecore.HolderRecord, exclusion routecore.PlacementExclusion) (core.HolderState, error) {
	ref, err := holderRefFromRecord(record)
	if err != nil {
		return core.HolderState{}, err
	}
	if err := routecore.ValidatePlacementExclusion(exclusion); err != nil {
		return core.HolderState{}, fmt.Errorf("%w: exclusion validation: %v", ErrHolderAdapterDrift, err)
	}
	if exclusion.Domain != ref.Domain || exclusion.HolderID != ref.HolderID || exclusion.BootID != ref.BootID ||
		exclusion.Revision == 0 || (!exclusion.Excluded && exclusion.OwnerPlanID != "") {
		return core.HolderState{}, fmt.Errorf("%w: malformed exclusion", ErrHolderAdapterDrift)
	}
	state, err := mapHolderLifecycle(record.State)
	if err != nil {
		return core.HolderState{}, err
	}
	return core.HolderState{
		Ref: ref, PodName: ref.PodUID,
		LogicEndpoint:         record.LogicEndpoint,
		HolderControlEndpoint: record.HolderControlEndpoint,
		TLSDNSIdentity:        record.TLSDNSIdentity,
		State:                 state,
		ProcessReady:          record.ProcessReady,
		Draining:              record.Draining,
		ServingShardCount:     record.ServingShardCount,
		SnapshotRevision:      record.SnapshotRevision,
		AssignmentKnownFresh:  record.SnapshotRevision != 0,
		PlacementEligible:     !exclusion.Excluded,
		PlacementRevision:     exclusion.Revision,
		LeaseUntil:            record.LeaseUntil,
	}, nil
}

func mapHolderLifecycle(value routecore.HolderState) (core.HolderLifecycleState, error) {
	switch value {
	case routecore.HolderStateRegistering, routecore.HolderStateWarming:
		return core.HolderWarming, nil
	case routecore.HolderStateReadyZeroShard:
		return core.HolderReadyZeroShard, nil
	case routecore.HolderStateServing:
		return core.HolderServing, nil
	case routecore.HolderStateDraining:
		return core.HolderDraining, nil
	case routecore.HolderStateDrained:
		return core.HolderDrained, nil
	default:
		return "", fmt.Errorf("%w: holder state %q", ErrHolderAdapterDrift, value)
	}
}

func holderContext(ctx context.Context) error {
	if ctx == nil {
		return ErrHolderAdapterInvalid
	}
	return ctx.Err()
}

var _ core.HolderStateReader = (*holderStateReaderAdapter)(nil)
var _ HolderRecordReader = (*routecore.HolderRegistry)(nil)
var _ PlacementExclusionReader = (*routecore.HolderRegistry)(nil)
