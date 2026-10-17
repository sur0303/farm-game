// Package autofailover turns Kubernetes + HolderControl generation changes
// into durable RoutePlan crash takeovers. It never grants write authority by
// itself: the existing RoutePlan executor, Authority CAS and writer_epoch
// fence remain the only mutation path.
package autofailover

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/holderidentity"
	"farmgame/internal/routecontrol/verifiedholder"
	"farmgame/internal/routeplan"
	"farmgame/internal/storage/authoritystore"
)

var ErrInvalidController = errors.New("routeplan autofailover: invalid controller")

type SnapshotReader interface {
	CurrentSnapshot() actorrouting.RouteSnapshot
}

type AuthorityReader interface {
	ReadShardWriteAuthority(context.Context, actorrouting.RouteDomain, uint32) (authoritystore.ShardWriteAuthority, bool, error)
}

type HolderView interface {
	ListVerifiedDomain(actorrouting.RouteDomain) []verifiedholder.Holder
}

type PlanExecutor interface {
	BeginForegroundBatch(context.Context) (func(), error)
	SubmitCrashTakeover(context.Context, routeplan.TakeoverRequest) (routeplan.PlanRecord, error)
	Execute(context.Context, string) (routeplan.PlanRecord, error)
}

type Options struct {
	Snapshots     SnapshotReader
	Authorities   AuthorityReader
	Holders       HolderView
	PrepareTarget func(context.Context, verifiedholder.Holder) error
	Executor      PlanExecutor
	Domains       []actorrouting.RouteDomain
	Interval      time.Duration
	StepTimeout   time.Duration
	PlanTTL       time.Duration
	// MaxPlansPerPass is retained for configuration compatibility. It is the
	// sliding concurrency limit; MaxAuthorityReads bounds total work per pass.
	MaxPlansPerPass   int
	MaxAuthorityReads int
	Wake              <-chan struct{}
	Ready             func() bool
	Now               func() time.Time
	ReconcileMu       *sync.Mutex
	OnResult          func(Result)
}

type Result struct {
	PlansCompleted uint32
	MissingShards  uint32
	WaitingLease   uint32
	NoTarget       uint32
	AuthorityReads uint32
	LastPlanID     string
	LastDomain     actorrouting.RouteDomain
	LastShard      uint32
	Err            error
}

type Controller struct {
	snapshots         SnapshotReader
	authorities       AuthorityReader
	holders           HolderView
	prepareTarget     func(context.Context, verifiedholder.Holder) error
	executor          PlanExecutor
	domains           []actorrouting.RouteDomain
	interval          time.Duration
	stepTimeout       time.Duration
	planTTL           time.Duration
	maxPlans          int
	maxAuthorityReads int
	wake              <-chan struct{}
	ready             func() bool
	now               func() time.Time
	reconcileMu       *sync.Mutex
	onResult          func(Result)
}

type candidate struct {
	request routeplan.TakeoverRequest
	target  verifiedholder.Holder
}

func New(options Options) (*Controller, error) {
	if options.Snapshots == nil || options.Authorities == nil || options.Holders == nil || options.Executor == nil || options.Ready == nil {
		return nil, ErrInvalidController
	}
	if len(options.Domains) == 0 {
		options.Domains = []actorrouting.RouteDomain{actorrouting.DomainPlayer, actorrouting.DomainFarm}
	}
	for _, domain := range options.Domains {
		if domain != actorrouting.DomainPlayer && domain != actorrouting.DomainFarm {
			return nil, fmt.Errorf("%w: domain %s", ErrInvalidController, domain)
		}
	}
	if options.Interval <= 0 || options.StepTimeout <= 0 || options.PlanTTL <= options.StepTimeout {
		return nil, fmt.Errorf("%w: timing", ErrInvalidController)
	}
	if options.MaxPlansPerPass <= 0 {
		options.MaxPlansPerPass = 32
	}
	if options.MaxAuthorityReads <= 0 {
		options.MaxAuthorityReads = options.MaxPlansPerPass * 4
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.ReconcileMu == nil {
		options.ReconcileMu = new(sync.Mutex)
	}
	return &Controller{
		snapshots: options.Snapshots, authorities: options.Authorities, holders: options.Holders,
		prepareTarget: options.PrepareTarget,
		executor:      options.Executor, domains: append([]actorrouting.RouteDomain(nil), options.Domains...),
		interval: options.Interval, stepTimeout: options.StepTimeout, planTTL: options.PlanTTL,
		maxPlans: options.MaxPlansPerPass, maxAuthorityReads: options.MaxAuthorityReads,
		wake: options.Wake, ready: options.Ready, now: options.Now,
		reconcileMu: options.ReconcileMu, onResult: options.OnResult,
	}, nil
}

func (c *Controller) Run(ctx context.Context) error {
	if c == nil || ctx == nil {
		return ErrInvalidController
	}
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			c.runPass(ctx)
		case <-c.wake:
			c.runPass(ctx)
		}
	}
}

func (c *Controller) runPass(ctx context.Context) {
	result := c.Reconcile(ctx)
	if c.onResult != nil {
		c.onResult(result)
	}
}

func (c *Controller) Reconcile(ctx context.Context) Result {
	result := Result{}
	if c == nil || ctx == nil {
		result.Err = ErrInvalidController
		return result
	}
	if !c.ready() {
		return result
	}
	c.reconcileMu.Lock()
	defer c.reconcileMu.Unlock()

	items, observation, err := c.findCandidates(ctx, c.maxPlans, c.maxAuthorityReads)
	applyObservation(&result, observation)
	if err != nil || len(items) == 0 {
		result.Err = err
		return result
	}

	// Candidate collection freezes an exact per-shard Authority fence. Prepare
	// each distinct target generation once before any RoutePlan is submitted;
	// all Shards targeting that generation reuse the same verified placement.
	prepared := make(map[string]struct{}, len(items))
	for _, item := range items {
		key := generationKey(item.target.HolderID, item.target.BootID)
		if _, found := prepared[key]; found {
			continue
		}
		if c.prepareTarget != nil {
			prepareCtx, prepareCancel := context.WithTimeout(ctx, c.stepTimeout)
			err = c.prepareTarget(prepareCtx, item.target)
			prepareCancel()
			if err != nil {
				result.Err = err
				return result
			}
		}
		prepared[key] = struct{}{}
	}
	releaseBatch, err := c.executor.BeginForegroundBatch(ctx)
	if err != nil || releaseBatch == nil {
		result.Err = errors.Join(err, ErrInvalidController)
		return result
	}
	defer releaseBatch()

	// UC's useful concurrency property is per-Shard progress: MaxPlans is a
	// sliding number of occupied slots, not a fixed batch completion barrier.
	// Submit and Execute are deliberately kept in the same worker so every newly
	// admitted Shard receives fresh, independent deadlines. Concurrent Submit
	// calls for one domain also share Holder verification through singleflight.
	type executionResult struct {
		item candidate
		err  error
	}
	results := make(chan executionResult, c.maxPlans)
	attempted := make(map[candidateKey]struct{}, c.maxAuthorityReads)
	reserved := make(map[string]uint32)
	inflight := 0
	launch := func(item candidate) {
		key := keyForCandidate(item)
		attempted[key] = struct{}{}
		reserved[generationKey(item.target.HolderID, item.target.BootID)]++
		result.LastPlanID = item.request.PlanID
		result.LastDomain = item.request.Domain
		result.LastShard = item.request.VirtualShard
		inflight++
		go func() {
			submitCtx, submitCancel := context.WithTimeout(ctx, c.stepTimeout)
			record, workerErr := c.executor.SubmitCrashTakeover(submitCtx, item.request)
			submitCancel()
			if workerErr == nil {
				executeCtx, executeCancel := context.WithTimeout(ctx, c.stepTimeout)
				_, workerErr = c.executor.Execute(executeCtx, record.Plan.PlanID)
				executeCancel()
			}
			results <- executionResult{item: item, err: workerErr}
		}()
	}
	for _, item := range items {
		launch(item)
	}

	stopScheduling := false
	for inflight > 0 {
		execution := <-results
		inflight--
		generation := generationKey(execution.item.target.HolderID, execution.item.target.BootID)
		if reserved[generation] <= 1 {
			delete(reserved, generation)
		} else {
			reserved[generation]--
		}
		if execution.err == nil {
			result.PlansCompleted++
		} else {
			result.Err = errors.Join(result.Err, execution.err)
		}
		if stopScheduling || ctx.Err() != nil || int(result.AuthorityReads) >= c.maxAuthorityReads {
			continue
		}

		openSlots := c.maxPlans - inflight
		remainingReads := c.maxAuthorityReads - int(result.AuthorityReads)
		next, nextObservation, findErr := c.findCandidatesExcluding(ctx, openSlots, remainingReads, attempted, reserved)
		result.AuthorityReads += nextObservation.AuthorityReads
		result.MissingShards = nextObservation.MissingShards
		result.WaitingLease = nextObservation.WaitingLease
		result.NoTarget = nextObservation.NoTarget
		if findErr != nil {
			result.Err = errors.Join(result.Err, findErr)
			stopScheduling = true
			continue
		}
		for _, item := range next {
			key := generationKey(item.target.HolderID, item.target.BootID)
			if _, found := prepared[key]; !found {
				if c.prepareTarget != nil {
					prepareCtx, prepareCancel := context.WithTimeout(ctx, c.stepTimeout)
					prepareErr := c.prepareTarget(prepareCtx, item.target)
					prepareCancel()
					if prepareErr != nil {
						result.Err = errors.Join(result.Err, prepareErr)
						stopScheduling = true
						break
					}
				}
				prepared[key] = struct{}{}
			}
			launch(item)
		}
	}
	return result
}

type candidateKey struct {
	domain actorrouting.RouteDomain
	shard  uint32
}

func keyForCandidate(item candidate) candidateKey {
	return candidateKey{domain: item.request.Domain, shard: item.request.VirtualShard}
}

func applyObservation(result *Result, value observation) {
	result.MissingShards = value.MissingShards
	result.WaitingLease = value.WaitingLease
	result.NoTarget = value.NoTarget
	result.AuthorityReads += value.AuthorityReads
}

type observation struct {
	MissingShards  uint32
	WaitingLease   uint32
	NoTarget       uint32
	AuthorityReads uint32
}

func (c *Controller) findCandidate(ctx context.Context, maxAuthorityReads int) (*candidate, observation, error) {
	items, result, err := c.findCandidates(ctx, 1, maxAuthorityReads)
	if err != nil || len(items) == 0 {
		return nil, result, err
	}
	return &items[0], result, nil
}

func (c *Controller) findCandidates(ctx context.Context, maxCandidates, maxAuthorityReads int) ([]candidate, observation, error) {
	return c.findCandidatesExcluding(ctx, maxCandidates, maxAuthorityReads, nil, nil)
}

func (c *Controller) findCandidatesExcluding(ctx context.Context, maxCandidates, maxAuthorityReads int, excluded map[candidateKey]struct{}, reserved map[string]uint32) ([]candidate, observation, error) {
	result := observation{}
	items := make([]candidate, 0, maxCandidates)
	snapshot := c.snapshots.CurrentSnapshot()
	now := c.now()
	for _, domain := range c.domains {
		table, err := domainTable(snapshot, domain)
		if err != nil {
			return nil, result, err
		}
		holders := c.holders.ListVerifiedDomain(domain)
		sort.Slice(holders, func(i, j int) bool {
			if holders[i].Ordinal != holders[j].Ordinal {
				return holders[i].Ordinal < holders[j].Ordinal
			}
			if holders[i].HolderID != holders[j].HolderID {
				return holders[i].HolderID < holders[j].HolderID
			}
			return holders[i].BootID < holders[j].BootID
		})
		healthy := make(map[string]verifiedholder.Holder, len(holders))
		counts := make(map[string]uint32, len(holders))
		for generation, count := range reserved {
			counts[generation] = count
		}
		for _, holder := range holders {
			if holder.Eligible(now) {
				healthy[generationKey(holder.HolderID, holder.BootID)] = holder
			}
		}
		for shard := uint32(0); shard < table.ShardCount; shard++ {
			route, source, err := routeAt(snapshot, table, shard)
			if err != nil {
				return nil, result, err
			}
			if route.State != actorrouting.ShardStateActive {
				continue
			}
			if _, ok := healthy[generationKey(source.InstanceID, source.BootID)]; ok {
				counts[generationKey(source.InstanceID, source.BootID)]++
			}
		}

		for shard := uint32(0); shard < table.ShardCount; shard++ {
			route, source, err := routeAt(snapshot, table, shard)
			if err != nil {
				return nil, result, err
			}
			if route.State != actorrouting.ShardStateActive {
				continue
			}
			if _, ok := healthy[generationKey(source.InstanceID, source.BootID)]; ok {
				continue
			}
			result.MissingShards++
			if _, skip := excluded[candidateKey{domain: domain, shard: shard}]; skip {
				continue
			}
			replacement, hasReplacement := replacementFor(holders, source.InstanceID, source.BootID, now)
			if route.LeaseUntilMs > now.UnixMilli() && !hasReplacement {
				result.WaitingLease++
				continue
			}
			if len(items) >= maxCandidates || int(result.AuthorityReads) >= maxAuthorityReads {
				continue
			}
			authorityValue, found, err := c.authorities.ReadShardWriteAuthority(ctx, domain, shard)
			result.AuthorityReads++
			if err != nil {
				return nil, result, err
			}
			if !found || !matchesRouteAuthority(domain, shard, route, source, authorityValue) {
				continue
			}

			proof := routeplan.TakeoverProof{ExpectedHolderBootID: source.BootID}
			if authorityValue.LeaseUntilMs <= now.UnixMilli() {
				proof.LeaseExpired = true
				proof.Evidence = fmt.Sprintf("authority lease expired at %d", authorityValue.LeaseUntilMs)
			} else if hasReplacement {
				proof.HolderUnavailable = true
				proof.Evidence = fmt.Sprintf("kubernetes verified replacement boot %s for holder %s", replacement.BootID, source.InstanceID)
			} else {
				result.WaitingLease++
				continue
			}

			target, ok := chooseTarget(holders, counts, source.InstanceID, source.BootID, now)
			if !ok {
				result.NoTarget++
				continue
			}
			fence := authoritystore.ShardWriteFence{
				ExpectedHolderID: source.InstanceID, ExpectedHolderBootID: source.BootID,
				ExpectedAssignmentRevision: route.ShardRevision,
				ExpectedWriterEpoch:        route.ExpectedWriterEpoch,
				ExpectedLeaseEpoch:         route.ExpectedLeaseEpoch,
			}
			planID, transitionID := deterministicIDs(domain, shard, fence, target)
			items = append(items, candidate{request: routeplan.TakeoverRequest{
				PlanID: planID, TransitionID: transitionID, Domain: domain, VirtualShard: shard,
				SourceHolderID: source.InstanceID, SourceBootID: source.BootID, SourceEndpoint: source.Endpoint,
				TargetHolderID: target.HolderID, TargetBootID: target.BootID,
				TargetDeploymentIdentity: holderidentity.DeploymentIdentity{
					PodUID: target.PodUID, WorkloadUID: target.WorkloadUID, Ordinal: target.Ordinal,
				},
				TargetEndpoint:      target.LogicEndpoint,
				ExpectedSourceFence: fence, RequestedTTL: c.planTTL, Proof: proof,
			}, target: target})
			counts[generationKey(target.HolderID, target.BootID)]++
		}
	}
	return items, result, nil
}

func domainTable(snapshot actorrouting.RouteSnapshot, domain actorrouting.RouteDomain) (actorrouting.DomainSnapshot, error) {
	switch domain {
	case actorrouting.DomainPlayer:
		return snapshot.Player, nil
	case actorrouting.DomainFarm:
		return snapshot.Farm, nil
	default:
		return actorrouting.DomainSnapshot{}, fmt.Errorf("%w: domain %s", ErrInvalidController, domain)
	}
}

func routeAt(snapshot actorrouting.RouteSnapshot, table actorrouting.DomainSnapshot, shard uint32) (actorrouting.ShardRoute, actorrouting.HolderRef, error) {
	if shard >= table.ShardCount || int(shard) >= len(table.Routes) {
		return actorrouting.ShardRoute{}, actorrouting.HolderRef{}, fmt.Errorf("%w: shard %d", ErrInvalidController, shard)
	}
	route := table.Routes[shard]
	if route.HolderIndex < 0 || route.HolderIndex >= len(snapshot.Holders) {
		return actorrouting.ShardRoute{}, actorrouting.HolderRef{}, fmt.Errorf("%w: holder index", ErrInvalidController)
	}
	return route, snapshot.Holders[route.HolderIndex], nil
}

func matchesRouteAuthority(domain actorrouting.RouteDomain, shard uint32, route actorrouting.ShardRoute, source actorrouting.HolderRef, value authoritystore.ShardWriteAuthority) bool {
	return value.State == authoritystore.ShardAuthorityActive && value.Domain == domain && value.VirtualShard == shard &&
		value.HolderID == source.InstanceID && value.HolderBootID == source.BootID &&
		value.AssignmentRevision == route.ShardRevision && value.WriterEpoch == route.ExpectedWriterEpoch &&
		value.LeaseEpoch == route.ExpectedLeaseEpoch
}

func replacementFor(holders []verifiedholder.Holder, holderID, oldBoot string, now time.Time) (verifiedholder.Holder, bool) {
	for _, holder := range holders {
		if holder.HolderID == holderID && holder.BootID != oldBoot && coldLoadTargetEligible(holder, now) {
			return holder, true
		}
	}
	return verifiedholder.Holder{}, false
}

func chooseTarget(holders []verifiedholder.Holder, counts map[string]uint32, sourceID, sourceBoot string, now time.Time) (verifiedholder.Holder, bool) {
	var selected verifiedholder.Holder
	found := false
	for _, holder := range holders {
		if holder.HolderID == sourceID && holder.BootID == sourceBoot {
			continue
		}
		if !coldLoadTargetEligible(holder, now) {
			continue
		}
		if !found || lessLoaded(holder, selected, counts) {
			selected, found = holder, true
		}
	}
	return selected, found
}

// coldLoadTargetEligible is deliberately broader than ordinary placement but
// only for crash takeover. A verified WARMING generation may receive its first
// ASSIGNING shard through HolderControl; it is never considered a healthy owner
// of an already ACTIVE route and remains unavailable to normal traffic.
func coldLoadTargetEligible(holder verifiedholder.Holder, now time.Time) bool {
	if holder.Eligible(now) {
		return true
	}
	if now.IsZero() {
		now = time.Now()
	}
	return holder.Validate() == nil && holder.Lifecycle == verifiedholder.LifecycleWarming &&
		!holder.ProcessReady && !holder.Draining && holder.ServingShardCount == 0 && holder.InflightTurnCount == 0 &&
		holder.CapacityWeight > 0 && !now.Before(holder.VerifiedAt) && holder.VerifiedUntil.After(now)
}

func lessLoaded(left, right verifiedholder.Holder, counts map[string]uint32) bool {
	leftCount := uint64(counts[generationKey(left.HolderID, left.BootID)])
	rightCount := uint64(counts[generationKey(right.HolderID, right.BootID)])
	leftWeight, rightWeight := uint64(left.CapacityWeight), uint64(right.CapacityWeight)
	if leftCount*rightWeight != rightCount*leftWeight {
		return leftCount*rightWeight < rightCount*leftWeight
	}
	if left.Ordinal != right.Ordinal {
		return left.Ordinal < right.Ordinal
	}
	if left.HolderID != right.HolderID {
		return left.HolderID < right.HolderID
	}
	return left.BootID < right.BootID
}

func generationKey(holderID, bootID string) string { return holderID + "\x00" + bootID }

func deterministicIDs(domain actorrouting.RouteDomain, shard uint32, fence authoritystore.ShardWriteFence, target verifiedholder.Holder) (string, string) {
	material := fmt.Sprintf("%s\x00%d\x00%s\x00%s\x00%d\x00%d\x00%d\x00%s\x00%s",
		domain, shard, fence.ExpectedHolderID, fence.ExpectedHolderBootID,
		fence.ExpectedAssignmentRevision, fence.ExpectedWriterEpoch, fence.ExpectedLeaseEpoch,
		target.HolderID, target.BootID)
	digest := sha256.Sum256([]byte(material))
	return fmt.Sprintf("auto-failover-%x", digest[:16]), fmt.Sprintf("auto-takeover-%x", digest[16:])
}
