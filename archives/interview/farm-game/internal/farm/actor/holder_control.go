package actor

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"farmgame/internal/actorrouting"
	"farmgame/internal/logic/actor/authority"
	"farmgame/internal/logic/actor/persistence"
	actorruntime "farmgame/internal/logic/actor/runtime"
	"farmgame/internal/storage/blobstore"
)

var (
	ErrHolderControlUnavailable = errors.New("farm actor: holder control unavailable")
	ErrFactsBarrierPrecondition = errors.New("farm actor: facts barrier precondition not met")
	ErrFactsBarrierLegacy       = errors.New("farm actor: legacy registry cannot prove shard facts")
	ErrFactsActorChanged        = errors.New("farm actor: actor binding changed during shard facts barrier")
	ErrFactsActorInvalid        = errors.New("farm actor: invalid authority actor during shard facts barrier")
	ErrFactsLaneInvalid         = errors.New("farm actor: persistence lane is unavailable")
)

type farmFactTarget struct {
	farmID  uint64
	actor   *FarmActor
	binding *authority.Binding
}

// ShardFactsBarrier freezes the domain-specific Calculator.ShardFor mapping
// at construction and performs a complete local actor enumeration.
type ShardFactsBarrier struct {
	registry *Registry
	shardFor actorruntime.ShardForActor
}

// HolderShardRuntime is the frozen farm-domain runtime port consumed by
// holdercontrol.LocalService. Prepare is deliberately a separate capability.
type HolderShardRuntime struct {
	registry *Registry
	facts    *ShardFactsBarrier
}

func NewHolderShardRuntime(registry *Registry, shardFor actorruntime.ShardForActor) (*HolderShardRuntime, error) {
	facts, err := NewShardFactsBarrier(registry, shardFor)
	if err != nil {
		return nil, err
	}
	return &HolderShardRuntime{registry: registry, facts: facts}, nil
}

func (r *HolderShardRuntime) StopNewActivationAndTurns(ctx context.Context, shard uint32) error {
	if r == nil || r.registry == nil {
		return ErrHolderControlUnavailable
	}
	return r.registry.StopNewActivationAndTurns(ctx, shard)
}

func (r *HolderShardRuntime) WaitCurrentTurns(ctx context.Context, shard uint32) error {
	if r == nil || r.registry == nil {
		return ErrHolderControlUnavailable
	}
	return r.registry.WaitCurrentTurns(ctx, shard)
}

func (r *HolderShardRuntime) ConfirmFactsCommitted(ctx context.Context, shard uint32) error {
	if r == nil || r.facts == nil {
		return ErrHolderControlUnavailable
	}
	return r.facts.ConfirmFactsCommitted(ctx, shard)
}

func (r *HolderShardRuntime) GetShardStatus(ctx context.Context, shard uint32) (actorruntime.ShardDrainStatus, error) {
	if r == nil || r.registry == nil {
		return actorruntime.ShardDrainStatus{}, ErrHolderControlUnavailable
	}
	return r.registry.GetShardStatus(ctx, shard)
}

func (r *HolderShardRuntime) RearmShard(ctx context.Context, shard uint32) error {
	if r == nil || r.registry == nil || r.facts == nil || r.facts.shardFor == nil {
		return ErrHolderControlUnavailable
	}
	return r.registry.rearmShard(ctx, shard, r.facts.shardFor)
}

func NewShardFactsBarrier(registry *Registry, shardFor actorruntime.ShardForActor) (*ShardFactsBarrier, error) {
	if registry == nil || shardFor == nil {
		return nil, fmt.Errorf("%w: registry and shard mapper are required", ErrHolderControlUnavailable)
	}
	registry.holderControlMu.Lock()
	if registry.holderShardFor == nil {
		registry.holderShardFor = shardFor
	}
	registry.holderControlMu.Unlock()
	return &ShardFactsBarrier{registry: registry, shardFor: shardFor}, nil
}

func (r *Registry) NewShardFactsBarrier(shardFor actorruntime.ShardForActor) (*ShardFactsBarrier, error) {
	return NewShardFactsBarrier(r, shardFor)
}

func (r *Registry) shardDrainController() (actorruntime.ShardDrainController, error) {
	if r == nil || r.admissionGate == nil {
		return nil, ErrHolderControlUnavailable
	}
	gate, ok := r.admissionGate.(actorruntime.ShardDrainController)
	if !ok || gate == nil {
		return nil, ErrHolderControlUnavailable
	}
	return gate, nil
}

func (r *Registry) StopNewActivationAndTurns(ctx context.Context, shard uint32) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	gate, err := r.shardDrainController()
	if err != nil {
		return err
	}
	return gate.BeginDrain(shard)
}

func (r *Registry) WaitCurrentTurns(ctx context.Context, shard uint32) error {
	if ctx == nil {
		ctx = context.Background()
	}
	gate, err := r.shardDrainController()
	if err != nil {
		return err
	}
	return gate.Wait(ctx, shard)
}

func (r *Registry) RearmShard(ctx context.Context, shard uint32) error {
	var shardFor actorruntime.ShardForActor
	if r != nil {
		r.holderControlMu.RLock()
		shardFor = r.holderShardFor
		r.holderControlMu.RUnlock()
	}
	if r != nil && r.authority != nil && shardFor == nil {
		return ErrHolderControlUnavailable
	}
	return r.rearmShard(ctx, shard, shardFor)
}

func (r *Registry) rearmShard(ctx context.Context, shard uint32, shardFor actorruntime.ShardForActor) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	gate, err := r.shardDrainController()
	if err != nil {
		return err
	}
	rearmer, ok := gate.(actorruntime.ShardDrainRearmer)
	if !ok {
		return ErrHolderControlUnavailable
	}
	status, ok := gate.Status(shard)
	if !ok {
		return ErrHolderControlUnavailable
	}
	if status.Phase == actorruntime.ShardDrainPhaseOpen {
		return rearmer.Rearm(shard)
	}
	if !status.WaitComplete || status.AcceptedJobs != 0 {
		return ErrFactsBarrierPrecondition
	}
	if shardFor != nil && r.authority != nil {
		barrier := &ShardFactsBarrier{registry: r, shardFor: shardFor}
		targets, snapshotErr := barrier.snapshotTargets(shard)
		if snapshotErr != nil {
			return snapshotErr
		}
		if err := barrier.evictDrainedTargets(shard, targets); err != nil {
			return err
		}
	}
	return rearmer.Rearm(shard)
}

func (r *Registry) GetShardStatus(_ context.Context, shard uint32) (actorruntime.ShardDrainStatus, error) {
	gate, err := r.shardDrainController()
	if err != nil {
		return actorruntime.ShardDrainStatus{}, err
	}
	status, ok := gate.Status(shard)
	if !ok {
		return actorruntime.ShardDrainStatus{}, fmt.Errorf("%w: shard %d", ErrHolderControlUnavailable, shard)
	}
	return status, nil
}

func (r *Registry) ConfirmShardFactsCommitted(ctx context.Context, shard uint32, shardFor actorruntime.ShardForActor) error {
	barrier, err := NewShardFactsBarrier(r, shardFor)
	if err != nil {
		return err
	}
	return barrier.ConfirmFactsCommitted(ctx, shard)
}

func (b *ShardFactsBarrier) ConfirmFactsCommitted(ctx context.Context, shard uint32) error {
	if b == nil || b.registry == nil || b.shardFor == nil {
		return ErrHolderControlUnavailable
	}
	r := b.registry
	if r.authority == nil {
		return ErrFactsBarrierLegacy
	}
	if ctx == nil {
		ctx = context.Background()
	}
	gate, err := r.shardDrainController()
	if err != nil {
		return err
	}
	status, ok := gate.Status(shard)
	if !ok || status.Phase != actorruntime.ShardDrainPhaseDraining ||
		!status.WaitComplete || status.AcceptedJobs != 0 {
		return ErrFactsBarrierPrecondition
	}

	targets, err := b.snapshotTargets(shard)
	if err != nil {
		return err
	}
	for _, target := range targets {
		if target.binding.HasPendingUnknown() {
			if err := r.resolveBindingForFacts(ctx, target); err != nil {
				return err
			}
		}
		if err := r.drainActorForFacts(ctx, target); err != nil {
			return err
		}
	}
	// Facts completion is also the Farm generation boundary: no active actor or
	// retired tombstone from this source generation may survive rearm.
	return b.evictDrainedTargets(shard, targets)
}

func (b *ShardFactsBarrier) snapshotTargets(shard uint32) ([]farmFactTarget, error) {
	r := b.registry
	r.mu.Lock()
	defer r.mu.Unlock()
	targets := make([]farmFactTarget, 0, len(r.actors)+len(r.retiredBindings))
	seen := make(map[uint64]struct{}, len(r.actors)+len(r.retiredBindings))
	appendTarget := func(farmID uint64, actor *FarmActor, binding *authority.Binding) error {
		if farmID == 0 || binding == nil {
			return ErrFactsActorInvalid
		}
		if _, duplicate := seen[farmID]; duplicate {
			return ErrFactsActorInvalid
		}
		seen[farmID] = struct{}{}
		address := binding.Address()
		if address.Domain != actorrouting.DomainFarm || address.ActorID != farmID {
			return ErrFactsActorInvalid
		}
		mapped, err := b.shardFor(farmID)
		if err != nil {
			return fmt.Errorf("%w: map farm %d: %v", ErrFactsActorInvalid, farmID, err)
		}
		if mapped == shard {
			targets = append(targets, farmFactTarget{farmID: farmID, actor: actor, binding: binding})
		}
		return nil
	}
	for farmID, actor := range r.actors {
		if actor == nil {
			return nil, ErrFactsActorInvalid
		}
		binding, ok := actor.store.(*authority.Binding)
		if !ok || binding == nil {
			return nil, ErrFactsBarrierLegacy
		}
		if err := appendTarget(farmID, actor, binding); err != nil {
			return nil, err
		}
	}
	for farmID, binding := range r.retiredBindings {
		if err := appendTarget(farmID, nil, binding); err != nil {
			return nil, err
		}
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].farmID < targets[j].farmID })
	return targets, nil
}

func (r *Registry) currentFactActor(target farmFactTarget) (*FarmActor, *authority.Binding, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	actor := r.actors[target.farmID]
	if actor != nil {
		binding, ok := actor.store.(*authority.Binding)
		if !ok || binding == nil || binding != target.binding {
			return nil, nil, ErrFactsActorChanged
		}
		if target.actor != nil && actor != target.actor {
			return nil, nil, ErrFactsActorChanged
		}
		return actor, binding, nil
	}
	binding := r.retiredBindings[target.farmID]
	if binding == nil || binding != target.binding {
		return nil, nil, ErrFactsActorChanged
	}
	return nil, binding, nil
}

func (r *Registry) resolveBindingForFacts(ctx context.Context, target farmFactTarget) error {
	actor, binding, err := r.currentFactActor(target)
	if err != nil {
		return err
	}
	if !binding.HasPendingUnknown() {
		return nil
	}
	if actor != nil {
		resolution, resolveErr := r.ResolveUnknown(ctx, target.farmID)
		if resolveErr == nil {
			switch resolution {
			case UnknownCommitted, UnknownNotCommitted:
				binding.Retire()
				return binding.PrepareForRetirement()
			default:
				return blobstore.ErrTimeoutUnknown
			}
		}
		// Terminal generation loss may have moved the same binding into the
		// tombstone map. If UNKNOWN remains, continue through the lifecycle-only
		// path; otherwise the terminal result is already drainable.
		actor, binding, err = r.currentFactActor(target)
		if err != nil {
			return resolveErr
		}
		if actor != nil {
			return resolveErr
		}
		if !binding.HasPendingUnknown() {
			return binding.PrepareForRetirement()
		}
	}

	_, err = r.exec.SubmitSystem(ctx, target.farmID, actorruntime.SystemKindUnknownResolved, func(ctx context.Context) (any, error) {
		release, err := r.acquireAuthorityCoord(ctx, target.farmID)
		if err != nil {
			return nil, err
		}
		defer release()
		actor, current, err := r.currentFactActor(target)
		if err != nil {
			return nil, err
		}
		if actor != nil {
			return nil, ErrFactsActorChanged
		}
		if !current.HasPendingUnknown() {
			return nil, current.PrepareForRetirement()
		}
		return nil, settleRetiredFarmBinding(ctx, current)
	})
	return err
}

func (b *ShardFactsBarrier) evictDrainedTargets(shard uint32, targets []farmFactTarget) error {
	if b == nil || b.registry == nil || b.shardFor == nil {
		return ErrHolderControlUnavailable
	}
	r := b.registry
	targetByFarm := make(map[uint64]*authority.Binding, len(targets))
	for _, target := range targets {
		if target.farmID == 0 || target.binding == nil || targetByFarm[target.farmID] != nil {
			return ErrFactsActorInvalid
		}
		targetByFarm[target.farmID] = target.binding
	}

	r.mu.Lock()
	currentCount := 0
	validate := func(farmID uint64, actor *FarmActor, binding *authority.Binding) error {
		if farmID == 0 || binding == nil {
			return ErrFactsActorInvalid
		}
		mapped, err := b.shardFor(farmID)
		if err != nil {
			return fmt.Errorf("%w: map farm %d: %v", ErrFactsActorInvalid, farmID, err)
		}
		if mapped != shard {
			return nil
		}
		currentCount++
		if targetByFarm[farmID] != binding {
			return ErrFactsActorChanged
		}
		if actor != nil {
			if _, ok := actor.UnknownContext(); ok {
				return ErrFactsBarrierPrecondition
			}
			if _, _, ok := actor.guardSnapshot(); ok {
				return ErrFactsBarrierPrecondition
			}
		}
		lane := binding.Lane()
		if lane == nil {
			return ErrFactsLaneInvalid
		}
		snapshot := lane.Snapshot()
		if binding.HasPendingUnknown() || snapshot.State != persistence.Draining || snapshot.Dirty ||
			snapshot.MemoryStateVersion > snapshot.PersistedThroughVersion {
			return fmt.Errorf("%w: farm %d terminal lane=%+v", ErrFactsBarrierPrecondition, farmID, snapshot)
		}
		return nil
	}
	seen := make(map[uint64]struct{}, len(r.actors)+len(r.retiredBindings))
	for farmID, actor := range r.actors {
		if actor == nil {
			r.mu.Unlock()
			return ErrFactsActorInvalid
		}
		binding := authorityBindingForFarmActor(actor)
		if err := validate(farmID, actor, binding); err != nil {
			r.mu.Unlock()
			return err
		}
		seen[farmID] = struct{}{}
	}
	for farmID, binding := range r.retiredBindings {
		if _, duplicate := seen[farmID]; duplicate {
			r.mu.Unlock()
			return ErrFactsActorInvalid
		}
		if err := validate(farmID, nil, binding); err != nil {
			r.mu.Unlock()
			return err
		}
	}
	if currentCount != len(targets) {
		r.mu.Unlock()
		return ErrFactsActorChanged
	}
	for _, target := range targets {
		target.binding.Retire()
		delete(r.actors, target.farmID)
		delete(r.retiredBindings, target.farmID)
		delete(r.unknown, target.farmID)
		delete(r.unknownResolved, target.farmID)
		if r.activation != nil {
			r.activation.MarkAbsent(target.farmID)
		}
	}
	r.mu.Unlock()

	if r.subscriptions != nil {
		for _, target := range targets {
			_, _ = r.subscriptions.DrainFarm(context.Background(), target.farmID)
			_, _ = r.subscriptions.RetireFarmIfEmpty(context.Background(), target.farmID)
		}
	}
	return nil
}

func (r *Registry) drainActorForFacts(ctx context.Context, target farmFactTarget) error {
	_, err := r.exec.SubmitSystem(ctx, target.farmID, actorruntime.SystemKindDrain, func(ctx context.Context) (any, error) {
		release, err := r.acquireAuthorityCoord(ctx, target.farmID)
		if err != nil {
			return nil, err
		}
		defer release()
		actor, binding, err := r.currentFactActor(target)
		if err != nil {
			return nil, err
		}
		if binding.HasPendingUnknown() {
			return nil, fmt.Errorf("%w: pending unknown remains", ErrFactsBarrierPrecondition)
		}
		if actor != nil {
			if _, ok := actor.UnknownContext(); ok {
				return nil, fmt.Errorf("%w: unknown context remains", ErrFactsBarrierPrecondition)
			}
			if _, _, ok := actor.guardSnapshot(); ok {
				return nil, fmt.Errorf("%w: post-commit delivery guard remains", ErrFactsBarrierPrecondition)
			}
		}
		lane := binding.Lane()
		if lane == nil {
			return nil, ErrFactsLaneInvalid
		}
		before := lane.Snapshot()
		if before.State == persistence.Draining && !before.Dirty &&
			before.MemoryStateVersion <= before.PersistedThroughVersion {
			return nil, nil
		}
		if binding.Retired() {
			if err := binding.PrepareForRetirement(); err != nil {
				return nil, err
			}
		} else {
			if before.State != persistence.Idle || before.Dirty ||
				before.MemoryStateVersion > before.PersistedThroughVersion {
				return nil, fmt.Errorf("%w: farm %d lane=%+v", ErrFactsBarrierPrecondition, target.farmID, before)
			}
			if err := lane.BeginDrain(); err != nil {
				return nil, err
			}
		}
		after := lane.Snapshot()
		if after.State != persistence.Draining || after.Dirty ||
			after.MemoryStateVersion > after.PersistedThroughVersion ||
			binding.HasPendingUnknown() {
			return nil, fmt.Errorf("%w: farm %d terminal lane=%+v", ErrFactsBarrierPrecondition, target.farmID, after)
		}
		_ = ctx
		return nil, nil
	})
	return err
}
