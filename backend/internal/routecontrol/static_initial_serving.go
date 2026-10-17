package routecontrol

import (
	"context"
	"fmt"
	"time"

	"farmgame/internal/actorrouting"
)

// StaticInitialServingVerifierOptions binds the one exceptional WARMING ->
// SERVING transition to facts obtained from the holder's installed route and
// authority runtimes. Dynamic zero-shard holders do not construct this
// verifier and therefore cannot bypass READY_ZERO_SHARD.
type StaticInitialServingVerifierOptions struct {
	Domain        actorrouting.RouteDomain
	HolderID      string
	HolderBootID  string
	LogicEndpoint string

	Snapshot          func(context.Context) (actorrouting.RouteSnapshot, error)
	ProcessReady      func() bool
	AssignmentHealthy func() bool
	ShardOpen         func(uint32) bool
	CanServeShard     func(actorrouting.RouteDomain, uint32) bool
	Now               func() time.Time

	MinimumRemainingLease time.Duration
}

// NewStaticInitialServingVerifier returns a server-side admission verifier for
// a statically pre-assigned holder. A caller-provided boolean is deliberately
// insufficient: every admission re-derives the exact holder/boot assignment,
// ACTIVE route state, fresh lease, open shard gate, and owned ACTIVE authority
// guard from the process-local verified runtimes.
func NewStaticInitialServingVerifier(options StaticInitialServingVerifierOptions) (InitialServingVerifier, error) {
	if options.Domain != actorrouting.DomainPlayer && options.Domain != actorrouting.DomainFarm {
		return nil, fmt.Errorf("%w: static serving domain", ErrHolderInitialServingAdmission)
	}
	if options.HolderID == "" || options.HolderBootID == "" || options.LogicEndpoint == "" ||
		options.Snapshot == nil || options.ProcessReady == nil || options.AssignmentHealthy == nil ||
		options.ShardOpen == nil || options.CanServeShard == nil {
		return nil, fmt.Errorf("%w: incomplete static serving verifier", ErrHolderInitialServingAdmission)
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.MinimumRemainingLease <= 0 {
		options.MinimumRemainingLease = time.Millisecond
	}

	return func(ctx context.Context, previous HolderRecord, heartbeat HolderHeartbeat) error {
		if ctx == nil {
			return fmt.Errorf("%w: context required", ErrHolderInitialServingAdmission)
		}
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("%w: %v", ErrHolderInitialServingAdmission, err)
		}
		if (previous.State != HolderStateWarming && previous.State != HolderStateReadyZeroShard) ||
			heartbeat.State != HolderStateServing ||
			previous.Domain != options.Domain || heartbeat.Domain != options.Domain ||
			previous.HolderID != options.HolderID || heartbeat.HolderID != options.HolderID ||
			previous.BootID != options.HolderBootID || heartbeat.BootID != options.HolderBootID ||
			previous.LogicEndpoint != options.LogicEndpoint {
			return fmt.Errorf("%w: holder generation mismatch", ErrHolderInitialServingAdmission)
		}
		if !heartbeat.ProcessReady || heartbeat.Draining || heartbeat.ServingShardCount == 0 ||
			!options.ProcessReady() || !options.AssignmentHealthy() {
			return fmt.Errorf("%w: runtime is not ready and serving", ErrHolderInitialServingAdmission)
		}

		snapshot, err := options.Snapshot(ctx)
		if err != nil {
			return fmt.Errorf("%w: read installed snapshot: %v", ErrHolderInitialServingAdmission, err)
		}
		if heartbeat.SnapshotRevision == 0 || snapshot.GlobalRevision < heartbeat.SnapshotRevision {
			return fmt.Errorf("%w: snapshot revision is not installed", ErrHolderInitialServingAdmission)
		}
		domain, err := staticServingDomain(snapshot, options.Domain)
		if err != nil {
			return err
		}
		now := options.Now()
		minimumDeadline := now.Add(options.MinimumRemainingLease).UnixMilli()
		var assigned uint32
		for shard, route := range domain.Routes {
			if route.HolderIndex < 0 || route.HolderIndex >= len(snapshot.Holders) {
				return fmt.Errorf("%w: holder index out of range", ErrHolderInitialServingAdmission)
			}
			holder := snapshot.Holders[route.HolderIndex]
			exactHolder := holder.InstanceID == options.HolderID && holder.BootID == options.HolderBootID
			if holder.InstanceID == options.HolderID && !exactHolder {
				return fmt.Errorf("%w: holder boot differs in installed snapshot", ErrHolderInitialServingAdmission)
			}
			if !exactHolder {
				if options.CanServeShard(options.Domain, uint32(shard)) {
					return fmt.Errorf("%w: runtime serves a shard assigned to another holder", ErrHolderInitialServingAdmission)
				}
				continue
			}
			if holder.Endpoint != options.LogicEndpoint || holder.State != actorrouting.HolderStateActive ||
				route.State != actorrouting.ShardStateActive || route.ShardRevision == 0 ||
				route.ExpectedWriterEpoch == 0 || route.ExpectedLeaseEpoch == 0 ||
				route.LeaseUntilMs < minimumDeadline || !options.ShardOpen(uint32(shard)) ||
				!options.CanServeShard(options.Domain, uint32(shard)) {
				return fmt.Errorf("%w: assigned shard is not fully admitted", ErrHolderInitialServingAdmission)
			}
			assigned++
		}
		if assigned == 0 || assigned != heartbeat.ServingShardCount {
			return fmt.Errorf("%w: serving shard count mismatch", ErrHolderInitialServingAdmission)
		}
		return nil
	}, nil
}

func staticServingDomain(snapshot actorrouting.RouteSnapshot, domain actorrouting.RouteDomain) (actorrouting.DomainSnapshot, error) {
	var selected actorrouting.DomainSnapshot
	switch domain {
	case actorrouting.DomainPlayer:
		selected = snapshot.Player
	case actorrouting.DomainFarm:
		selected = snapshot.Farm
	default:
		return actorrouting.DomainSnapshot{}, fmt.Errorf("%w: invalid domain", ErrHolderInitialServingAdmission)
	}
	if selected.ShardCount == 0 || uint64(selected.ShardCount) != uint64(len(selected.Routes)) {
		return actorrouting.DomainSnapshot{}, fmt.Errorf("%w: incomplete domain snapshot", ErrHolderInitialServingAdmission)
	}
	return selected, nil
}
