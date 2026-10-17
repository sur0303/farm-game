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
)

var (
	ErrHolderControlUnavailable = errors.New("player actor: holder control unavailable")
	ErrFactsBarrierPrecondition = errors.New("player actor: facts barrier precondition not met")
	ErrFactsBarrierLegacy       = errors.New("player actor: legacy registry cannot prove shard facts")
	ErrFactsBindingChanged      = errors.New("player actor: binding changed during shard facts barrier")
	ErrFactsBindingInvalid      = errors.New("player actor: invalid authority binding during shard facts barrier")
	ErrFactsLaneInvalid         = errors.New("player actor: persistence lane is unavailable")
)

type playerFactTarget struct {
	playerID uint64
	binding  *authority.Binding
}

// ShardFactsBarrier freezes the domain-specific Calculator.ShardFor mapping
// at construction and performs a complete local binding enumeration.
type ShardFactsBarrier struct {
	registry *Registry
	shardFor actorruntime.ShardForActor
}

// HolderShardRuntime is the frozen player-domain runtime port consumed by
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
	// Compatibility entry point. Authority mode still requires a mapper that a
	// previously constructed facts barrier froze; reopening without complete
	// Binding enumeration fails closed. Production HolderShardRuntime passes its
	// own barrier's mapper directly.
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
		// A successful facts barrier normally leaves no targets. This second
		// terminal check/eviction closes the rearm boundary if an idempotent or
		// older control caller left drained entries behind.
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

// ConfirmShardFactsCommitted is the registry half of the source-holder
// barrier. It is intentionally separate from Stop/Wait; the holder control
// service enforces that ordering before calling this method.
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
		// Stop/Wait has already closed user admission for this shard. Flush via
		// the scheduler before entering the drain mailbox turn so an already
		// in-flight external Redis call can complete without deadlocking its
		// completion turn behind us.
		if r.writeback != nil && target.binding.SoftDirty() {
			if err := r.writeback.FlushNow(ctx, target.playerID); err != nil {
				return fmt.Errorf("%w: flush dirty player %d before drain: %v", ErrFactsBarrierPrecondition, target.playerID, err)
			}
		}
		if err := r.drainBindingForFacts(ctx, target); err != nil {
			return err
		}
	}
	// The facts barrier is also the Binding-generation barrier. Retire and
	// remove every exact drained identity before control can advance to rearm.
	// An empty target list is successful only because both enumerations hold the
	// complete bindings map lock while validating every local binding.
	return b.evictDrainedTargets(shard, targets)
}

func (b *ShardFactsBarrier) snapshotTargets(shard uint32) ([]playerFactTarget, error) {
	r := b.registry
	r.bindingsMu.Lock()
	defer r.bindingsMu.Unlock()
	targets := make([]playerFactTarget, 0)
	for playerID, binding := range r.bindings {
		if playerID == 0 || binding == nil {
			return nil, ErrFactsBindingInvalid
		}
		address := binding.Address()
		if address.Domain != actorrouting.DomainPlayer || address.ActorID != playerID {
			return nil, ErrFactsBindingInvalid
		}
		mapped, err := b.shardFor(playerID)
		if err != nil {
			return nil, fmt.Errorf("%w: map player %d: %v", ErrFactsBindingInvalid, playerID, err)
		}
		if mapped == shard {
			targets = append(targets, playerFactTarget{playerID: playerID, binding: binding})
		}
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].playerID < targets[j].playerID })
	return targets, nil
}

func (b *ShardFactsBarrier) evictDrainedTargets(shard uint32, targets []playerFactTarget) error {
	if b == nil || b.registry == nil || b.shardFor == nil {
		return ErrHolderControlUnavailable
	}
	r := b.registry
	targetByPlayer := make(map[uint64]*authority.Binding, len(targets))
	for _, target := range targets {
		if target.playerID == 0 || target.binding == nil || targetByPlayer[target.playerID] != nil {
			return ErrFactsBindingInvalid
		}
		targetByPlayer[target.playerID] = target.binding
	}

	r.bindingsMu.Lock()
	// Validate the complete current shard set and every terminal lane before
	// mutating the map. A concurrent identity change fails closed without a
	// partial eviction.
	currentCount := 0
	for playerID, binding := range r.bindings {
		if playerID == 0 || binding == nil {
			r.bindingsMu.Unlock()
			return ErrFactsBindingInvalid
		}
		mapped, err := b.shardFor(playerID)
		if err != nil {
			r.bindingsMu.Unlock()
			return fmt.Errorf("%w: map player %d: %v", ErrFactsBindingInvalid, playerID, err)
		}
		if mapped != shard {
			continue
		}
		currentCount++
		if targetByPlayer[playerID] != binding {
			r.bindingsMu.Unlock()
			return ErrFactsBindingChanged
		}
		lane := binding.Lane()
		if lane == nil {
			r.bindingsMu.Unlock()
			return ErrFactsLaneInvalid
		}
		snapshot := lane.Snapshot()
		if binding.HasPendingUnknown() || snapshot.State != persistence.Draining || snapshot.Dirty ||
			snapshot.MemoryStateVersion > snapshot.PersistedThroughVersion {
			r.bindingsMu.Unlock()
			return fmt.Errorf("%w: player %d terminal lane=%+v", ErrFactsBarrierPrecondition, playerID, snapshot)
		}
	}
	if currentCount != len(targets) {
		r.bindingsMu.Unlock()
		return ErrFactsBindingChanged
	}
	for _, target := range targets {
		target.binding.Retire()
		delete(r.bindings, target.playerID)
		if r.activation != nil {
			r.activation.MarkAbsent(target.playerID)
		}
	}
	r.bindingsMu.Unlock()
	return nil
}

func (r *Registry) currentFactBinding(target playerFactTarget) (*authority.Binding, error) {
	r.bindingsMu.Lock()
	current := r.bindings[target.playerID]
	r.bindingsMu.Unlock()
	if current != target.binding || current == nil {
		return nil, ErrFactsBindingChanged
	}
	return current, nil
}

func (r *Registry) resolveBindingForFacts(ctx context.Context, target playerFactTarget) error {
	_, err := r.exec.SubmitSystem(ctx, target.playerID, actorruntime.SystemKindUnknownResolved, func(ctx context.Context) (any, error) {
		release, err := r.acquireAuthorityCoord(ctx, target.playerID)
		if err != nil {
			return nil, err
		}
		defer release()
		current, err := r.currentFactBinding(target)
		if err != nil {
			return nil, err
		}
		if !current.HasPendingUnknown() {
			return nil, nil
		}
		// Facts settlement is lifecycle-only. Retire first so business access is
		// fenced even if FenceAndClaim must be retried by a later barrier call.
		r.retireBindingIfCurrent(target.playerID, current)
		if err := settleRetiredPlayerBinding(ctx, current); err != nil {
			return nil, err
		}
		return nil, nil
	})
	return err
}

func (r *Registry) drainBindingForFacts(ctx context.Context, target playerFactTarget) error {
	_, err := r.exec.SubmitSystem(ctx, target.playerID, actorruntime.SystemKindDrain, func(ctx context.Context) (any, error) {
		release, err := r.acquireAuthorityCoord(ctx, target.playerID)
		if err != nil {
			return nil, err
		}
		defer release()
		current, err := r.currentFactBinding(target)
		if err != nil {
			return nil, err
		}
		if current.HasPendingUnknown() {
			return nil, fmt.Errorf("%w: pending unknown remains", ErrFactsBarrierPrecondition)
		}
		if current.SoftSaveEnabled() && current.SoftDirty() {
			if _, err := current.FlushSoftModelState(ctx, target.playerID); err != nil {
				return nil, fmt.Errorf("%w: flush dirty player %d: %v", ErrFactsBarrierPrecondition, target.playerID, err)
			}
			if r.writeback != nil {
				r.writeback.Forget(target.playerID)
			}
		}
		lane := current.Lane()
		if lane == nil {
			return nil, ErrFactsLaneInvalid
		}
		before := lane.Snapshot()
		if before.State == persistence.Draining && !before.Dirty &&
			before.MemoryStateVersion <= before.PersistedThroughVersion {
			return nil, nil
		}
		if current.Retired() {
			if current.HasPendingUnknown() {
				return nil, fmt.Errorf("%w: pending unknown remains", ErrFactsBarrierPrecondition)
			}
			if err := current.PrepareForRetirement(); err != nil {
				return nil, err
			}
		} else {
			if before.State != persistence.Idle || before.Dirty ||
				before.MemoryStateVersion > before.PersistedThroughVersion {
				return nil, fmt.Errorf("%w: player %d lane=%+v", ErrFactsBarrierPrecondition, target.playerID, before)
			}
			if err := lane.BeginDrain(); err != nil {
				return nil, err
			}
		}
		after := lane.Snapshot()
		if after.State != persistence.Draining || after.Dirty ||
			after.MemoryStateVersion > after.PersistedThroughVersion ||
			current.HasPendingUnknown() {
			return nil, fmt.Errorf("%w: player %d terminal lane=%+v", ErrFactsBarrierPrecondition, target.playerID, after)
		}
		_ = ctx
		return nil, nil
	})
	return err
}
