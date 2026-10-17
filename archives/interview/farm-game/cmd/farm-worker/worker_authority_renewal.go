package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"farmgame/internal/actorrouting"
	core "farmgame/internal/routecontrol"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/worker/opportunityrecovery"
)

var errFarmWorkerAuthorityRenewal = errors.New(
	"farm-worker: FARM_WORKER authority renewal failed",
)

const workerHolderRegistryRetryBackoff = 10 * time.Millisecond

type farmWorkerAuthorityRenewStore interface {
	RenewShardAuthority(
		context.Context,
		authoritystore.RenewShardAuthorityRequest,
	) (authoritystore.ShardAuthorityTransitionResult, error)
}

type farmWorkerHolderRegistry interface {
	Register(context.Context, core.HolderRegistration) (core.HolderRecord, error)
	Get(
		context.Context,
		actorrouting.RouteDomain,
		string,
	) (core.HolderRecord, bool, error)
	Heartbeat(context.Context, core.HolderHeartbeat) (core.HolderRecord, error)
}

type farmWorkerAuthorityRenewer struct {
	source           actorrouting.RevisionedAssignmentProvider
	store            farmWorkerAuthorityRenewStore
	holders          farmWorkerHolderRegistry
	holderID         string
	bootID           string
	logicEndpoint    string
	controlEndpoint  string
	tlsDNSIdentity   string
	authorityLayout  string
	authorityTTL     time.Duration
	holderTTL        time.Duration
	interval         time.Duration
	operationTimeout time.Duration
	now              func() time.Time
	leaseMode        actorrouting.LeaseCheckMode
	leaseObserver    opportunityrecovery.WorkerAuthorityLeaseObserver
	healthy          atomic.Bool
}

func newFarmWorkerAuthorityRenewer(
	source actorrouting.RevisionedAssignmentProvider,
	store farmWorkerAuthorityRenewStore,
	holders farmWorkerHolderRegistry,
	holderID string,
	bootID string,
	logicEndpoint string,
	controlEndpoint string,
	tlsDNSIdentity string,
	authorityLayout string,
	authorityTTL time.Duration,
	holderTTL time.Duration,
	interval time.Duration,
	operationTimeout time.Duration,
) (*farmWorkerAuthorityRenewer, error) {
	if source == nil || store == nil || holders == nil ||
		holderID == "" || bootID == "" || logicEndpoint == "" ||
		controlEndpoint == "" || tlsDNSIdentity == "" ||
		authorityLayout == "" ||
		authorityTTL <= 0 || authorityTTL > time.Minute ||
		holderTTL <= 0 || holderTTL > time.Minute ||
		interval <= 0 || interval >= holderTTL/2 ||
		holderTTL >= authorityTTL-2*interval ||
		operationTimeout <= 0 || operationTimeout >= interval {
		return nil, errFarmWorkerAuthorityRenewal
	}
	return &farmWorkerAuthorityRenewer{
		source:           source,
		store:            store,
		holders:          holders,
		holderID:         holderID,
		bootID:           bootID,
		logicEndpoint:    logicEndpoint,
		controlEndpoint:  controlEndpoint,
		tlsDNSIdentity:   tlsDNSIdentity,
		authorityLayout:  authorityLayout,
		authorityTTL:     authorityTTL,
		holderTTL:        holderTTL,
		interval:         interval,
		operationTimeout: operationTimeout,
		now:              time.Now,
		leaseMode:        actorrouting.LeaseCheckSnapshot,
	}, nil
}

func (r *farmWorkerAuthorityRenewer) Run(ctx context.Context) error {
	if r == nil || ctx == nil {
		return errFarmWorkerAuthorityRenewal
	}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
		err := r.bootstrap(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("FARM_WORKER authority renewal failed closed: %v", err)
		}
		timer.Reset(r.interval)
	}
}

func (r *farmWorkerAuthorityRenewer) Healthy() bool {
	return r != nil && r.healthy.Load()
}

// SetLeaseObserver connects Redis renewal results to the local WorkerLeaseView.
// It must be called before Run/bootstrap starts; the observer receives only an
// already accepted, fully fenced authority result.
func (r *farmWorkerAuthorityRenewer) SetLeaseObserver(observer opportunityrecovery.WorkerAuthorityLeaseObserver) {
	if r == nil {
		return
	}
	r.leaseObserver = observer
}

func (r *farmWorkerAuthorityRenewer) SetLeaseCheckMode(mode actorrouting.LeaseCheckMode) error {
	if r == nil {
		return errFarmWorkerAuthorityRenewal
	}
	if mode != actorrouting.LeaseCheckSnapshot && mode != actorrouting.LeaseCheckAuthorityDelegated {
		return fmt.Errorf("%w: unsupported lease check mode %q", errFarmWorkerAuthorityRenewal, mode)
	}
	r.leaseMode = mode
	return nil
}

func (r *farmWorkerAuthorityRenewer) bootstrap(ctx context.Context) error {
	if r == nil || ctx == nil {
		return errFarmWorkerAuthorityRenewal
	}
	operationCtx, cancel := context.WithTimeout(ctx, r.operationTimeout)
	defer cancel()
	err := r.renewOnce(operationCtx)
	r.healthy.Store(err == nil)
	return err
}

func (r *farmWorkerAuthorityRenewer) renewOnce(ctx context.Context) error {
	if r == nil || ctx == nil {
		return errFarmWorkerAuthorityRenewal
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	snapshot, err := r.source.GetSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("%w: snapshot: %v", errFarmWorkerAuthorityRenewal, err)
	}
	if snapshot.FarmWorker.ShardCount == 0 ||
		int(snapshot.FarmWorker.ShardCount) != len(snapshot.FarmWorker.Routes) {
		return fmt.Errorf("%w: incomplete FARM_WORKER domain", errFarmWorkerAuthorityRenewal)
	}
	now := time.UnixMilli(r.now().UnixMilli())
	nowMs := now.UnixMilli()
	owned := uint32(0)
	for shard, route := range snapshot.FarmWorker.Routes {
		if route.State != actorrouting.ShardStateActive ||
			route.HolderIndex < 0 || route.HolderIndex >= len(snapshot.Holders) {
			continue
		}
		holder := snapshot.Holders[route.HolderIndex]
		if holder.InstanceID != r.holderID || holder.BootID != r.bootID {
			continue
		}
		owned++
		if route.ShardRevision == 0 || route.ExpectedWriterEpoch == 0 ||
			route.ExpectedLeaseEpoch == 0 ||
			(r.leaseMode == actorrouting.LeaseCheckSnapshot && route.LeaseUntilMs <= nowMs) {
			return fmt.Errorf(
				"%w: stale local route shard=%d",
				errFarmWorkerAuthorityRenewal,
				shard,
			)
		}
		result, renewErr := r.store.RenewShardAuthority(
			ctx,
			authoritystore.RenewShardAuthorityRequest{
				Caller: authoritystore.TrustedShardAuthorityCaller{
					Kind:       authoritystore.ShardAuthorityCallerHolder,
					InstanceID: r.holderID,
					BootID:     r.bootID,
				},
				AuthorityLayoutVersion: r.authorityLayout,
				Domain:                 actorrouting.DomainFarmWorker,
				VirtualShard:           uint32(shard),
				ExpectedFence: authoritystore.ShardWriteFence{
					ExpectedHolderID:           r.holderID,
					ExpectedHolderBootID:       r.bootID,
					ExpectedAssignmentRevision: route.ShardRevision,
					ExpectedWriterEpoch:        route.ExpectedWriterEpoch,
					ExpectedLeaseEpoch:         route.ExpectedLeaseEpoch,
				},
				RequestedTTLms: r.authorityTTL.Milliseconds(),
			},
		)
		if renewErr != nil {
			return fmt.Errorf(
				"%w: shard=%d: %v",
				errFarmWorkerAuthorityRenewal,
				shard,
				renewErr,
			)
		}
		if result.Status != authoritystore.ShardTransitionApplied &&
			result.Status != authoritystore.ShardTransitionAlreadyApplied {
			return fmt.Errorf(
				"%w: shard=%d status=%s",
				errFarmWorkerAuthorityRenewal,
				shard,
				result.Status,
			)
		}
		authority := result.Authority
		if authority.Domain != actorrouting.DomainFarmWorker ||
			authority.VirtualShard != uint32(shard) ||
			authority.HolderID != r.holderID ||
			authority.HolderBootID != r.bootID ||
			authority.AssignmentRevision != route.ShardRevision ||
			authority.WriterEpoch != route.ExpectedWriterEpoch ||
			authority.LeaseEpoch != route.ExpectedLeaseEpoch ||
			authority.LeaseUntilMs <= nowMs {
			return fmt.Errorf(
				"%w: shard=%d returned mismatched authority",
				errFarmWorkerAuthorityRenewal,
				shard,
			)
		}
		if r.leaseObserver != nil {
			if err := r.leaseObserver.ObserveAuthorityLease(opportunityrecovery.WorkerShardLeaseView{
				Shard:              uint32(shard),
				HolderID:           authority.HolderID,
				HolderBootID:       authority.HolderBootID,
				AssignmentRevision: authority.AssignmentRevision,
				WriterEpoch:        authority.WriterEpoch,
				LeaseEpoch:         authority.LeaseEpoch,
				LeaseUntilMs:       authority.LeaseUntilMs,
			}); err != nil {
				return fmt.Errorf("%w: shard=%d lease view: %v", errFarmWorkerAuthorityRenewal, shard, err)
			}
		}
	}
	record, found, err := r.getHolder(ctx)
	if err != nil {
		return fmt.Errorf("%w: holder registry read: %v", errFarmWorkerAuthorityRenewal, err)
	}
	if !found {
		record, err = r.registerHolder(ctx, core.HolderRegistration{
			Domain:                actorrouting.DomainFarmWorker,
			HolderID:              r.holderID,
			BootID:                r.bootID,
			LogicEndpoint:         r.logicEndpoint,
			HolderControlEndpoint: r.controlEndpoint,
			TLSDNSIdentity:        r.tlsDNSIdentity,
			State:                 core.HolderStateWarming,
			StartedAt:             now,
			HeartbeatAt:           now,
			LeaseUntil:            now.Add(r.holderTTL),
			Sequence:              1,
			CapacityWeight:        1,
			ResourceSummary:       map[string]string{"role": "farm-worker-route-holder"},
		})
		if err != nil {
			return fmt.Errorf("%w: holder registry register: %v", errFarmWorkerAuthorityRenewal, err)
		}
	}
	if record.BootID != r.bootID ||
		(record.State != core.HolderStateWarming &&
			record.State != core.HolderStateReadyZeroShard &&
			record.State != core.HolderStateServing) ||
		(record.State == core.HolderStateServing && owned == 0) {
		return fmt.Errorf("%w: holder registry identity/state", errFarmWorkerAuthorityRenewal)
	}
	heartbeat := func(state core.HolderState, serving uint32) error {
		if record.Sequence == ^uint64(0) {
			return fmt.Errorf("%w: holder registry sequence overflow", errFarmWorkerAuthorityRenewal)
		}
		if !now.After(record.HeartbeatAt) {
			now = time.UnixMilli(record.HeartbeatAt.UnixMilli() + 1)
		}
		record, err = r.heartbeatHolder(ctx, core.HolderHeartbeat{
			Domain:            actorrouting.DomainFarmWorker,
			HolderID:          r.holderID,
			BootID:            r.bootID,
			State:             state,
			ProcessReady:      true,
			Draining:          false,
			ServingShardCount: serving,
			SnapshotRevision:  snapshot.GlobalRevision,
			HeartbeatAt:       now,
			LeaseUntil:        now.Add(r.holderTTL),
			Sequence:          record.Sequence + 1,
			CapacityWeight:    record.CapacityWeight,
			ResourceSummary:   record.ResourceSummary,
		})
		return err
	}
	if record.State == core.HolderStateWarming {
		if err := heartbeat(core.HolderStateReadyZeroShard, 0); err != nil {
			return fmt.Errorf("%w: holder registry ready heartbeat: %v", errFarmWorkerAuthorityRenewal, err)
		}
	}
	state := core.HolderStateReadyZeroShard
	serving := uint32(0)
	if owned > 0 {
		state = core.HolderStateServing
		serving = owned
	}
	if err := heartbeat(state, serving); err != nil {
		return fmt.Errorf("%w: holder registry heartbeat: %v", errFarmWorkerAuthorityRenewal, err)
	}
	return nil
}

func (r *farmWorkerAuthorityRenewer) getHolder(
	ctx context.Context,
) (core.HolderRecord, bool, error) {
	for {
		record, found, err := r.holders.Get(
			ctx,
			actorrouting.DomainFarmWorker,
			r.holderID,
		)
		if err == nil || !errors.Is(err, core.ErrHolderRegistryUnavailable) {
			return record, found, err
		}
		if err := waitWorkerHolderRegistryRetry(ctx); err != nil {
			return core.HolderRecord{}, false, ctx.Err()
		}
	}
}

func (r *farmWorkerAuthorityRenewer) registerHolder(
	ctx context.Context,
	registration core.HolderRegistration,
) (core.HolderRecord, error) {
	for {
		record, err := r.holders.Register(ctx, registration)
		if err == nil || !errors.Is(err, core.ErrHolderRegistryUnavailable) {
			return record, err
		}
		if err := waitWorkerHolderRegistryRetry(ctx); err != nil {
			return core.HolderRecord{}, err
		}
	}
}

func (r *farmWorkerAuthorityRenewer) heartbeatHolder(
	ctx context.Context,
	heartbeat core.HolderHeartbeat,
) (core.HolderRecord, error) {
	for {
		record, err := r.holders.Heartbeat(ctx, heartbeat)
		if err == nil || !errors.Is(err, core.ErrHolderRegistryUnavailable) {
			return record, err
		}
		if err := waitWorkerHolderRegistryRetry(ctx); err != nil {
			return core.HolderRecord{}, err
		}
	}
}

func waitWorkerHolderRegistryRetry(ctx context.Context) error {
	timer := time.NewTimer(workerHolderRegistryRetryBackoff)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
