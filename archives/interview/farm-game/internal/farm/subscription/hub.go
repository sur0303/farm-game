package subscription

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var ErrFarmEventLaneFull = errors.New("farm subscription hub: event lane full")
var ErrFarmEventLaneRetired = errors.New("farm subscription hub: event lane retired")
var ErrJoinAttemptCancelled = errors.New("farm subscription hub: join attempt cancelled")
var ErrCancelTombstoneCapacity = errors.New("farm subscription hub: cancel tombstone capacity exhausted")

const defaultFarmEventLaneSize = 128
const defaultSubscriptionFallbackTTL = 24 * time.Hour
const defaultSubscriptionLeaseTTL = 30 * time.Second
const defaultCancelTombstoneTTL = 2 * time.Minute
const defaultMaxCancelTombstones = 2048
const defaultMaxCancelTombstoneBytes = 512 << 10

type laneRequest struct {
	system bool
	apply  func(*subscriptionState) (any, error)
	done   chan laneResult
}

type laneResult struct {
	value any
	err   error
}

// FarmEventLane 是单 Farm 的 FIFO 有序事件通道(26 §12/27 §5.17 的有序边界),
// system 事件优先于普通事件。
type FarmEventLane struct {
	events chan laneRequest
	system chan laneRequest
	quit   chan struct{}

	// admissionMu closes the check-before-enqueue retirement race. Retirement
	// first installs retiring=true under this mutex, then queues its own system
	// request. Later callers wait for that decision instead of entering an
	// orphaned lane; requests admitted earlier are visible to the retire check.
	admissionMu sync.Mutex
	retiring    bool
	retireDone  chan struct{}
	retired     atomic.Bool
	quitOnce    sync.Once
	// counts/realtime are refreshed after every lane apply so /metrics can
	// sum them without queueing behind Join on every farm.
	counts   atomic.Pointer[JoinStateCounts]
	realtime atomic.Pointer[FarmRealtimeState]
}

func newFarmEventLane(size int) *FarmEventLane {
	if size <= 0 {
		size = defaultFarmEventLaneSize
	}
	lane := &FarmEventLane{
		events: make(chan laneRequest, size),
		system: make(chan laneRequest, 16),
		quit:   make(chan struct{}),
	}
	go lane.run()
	return lane
}

func (l *FarmEventLane) run() {
	state := newSubscriptionState()
	for {
		select {
		case req := <-l.system:
			l.applyRequest(req, state)
			continue
		default:
		}
		select {
		case req := <-l.system:
			l.applyRequest(req, state)
		case req := <-l.events:
			l.applyRequest(req, state)
		case <-l.quit:
			return
		}
	}
}

func (l *FarmEventLane) applyRequest(req laneRequest, state *subscriptionState) {
	value, err := req.apply(state)
	counts := countPresence(state)
	l.counts.Store(&counts)
	realtime := state.realtime
	l.realtime.Store(&realtime)
	req.done <- laneResult{value: value, err: err}
}

func (l *FarmEventLane) execute(ctx context.Context, system bool, apply func(*subscriptionState) (any, error)) (any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	req := laneRequest{apply: apply, done: make(chan laneResult, 1)}
	for {
		l.admissionMu.Lock()
		if l.retired.Load() {
			l.admissionMu.Unlock()
			return nil, ErrFarmEventLaneRetired
		}
		if l.retiring {
			done := l.retireDone
			l.admissionMu.Unlock()
			select {
			case <-done:
				continue
			case <-l.quit:
				return nil, ErrFarmEventLaneRetired
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if system {
			select {
			case l.system <- req:
				l.admissionMu.Unlock()
			case <-ctx.Done():
				l.admissionMu.Unlock()
				return nil, ctx.Err()
			}
		} else {
			select {
			case l.events <- req:
				l.admissionMu.Unlock()
			case <-ctx.Done():
				l.admissionMu.Unlock()
				return nil, ctx.Err()
			default:
				l.admissionMu.Unlock()
				return nil, ErrFarmEventLaneFull
			}
		}
		break
	}
	select {
	case result := <-req.done:
		return result.value, result.err
	case <-l.quit:
		// A preceding request may have completed just before a later retire
		// decision closed quit. Prefer that authoritative result over a false
		// RETIRED retry signal.
		select {
		case result := <-req.done:
			return result.value, result.err
		default:
		}
		return nil, ErrFarmEventLaneRetired
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ActorBinding 把宿主的 Presence/Join 幂等映射别名进 Lane 状态(同 Holder 的
// 有序边界内),事件结束后回收 Join 幂等顺序。Hub 不认识 FarmActor——绑定是
// 唯一的耦合面。
type ActorBinding interface {
	BindState(presence map[string]*Presence, byPlayer map[uint64]string,
		byAttempt map[string]string, idem map[string]JoinIdempotencyEntry,
		order []string, realtime FarmRealtimeState)
	IdemOrder() []string
}

// SubscriptionHub owns Presence independently from FarmActor activation. Its
// boot index lets GateBootReaper inspect only boots actually represented on
// this holder; it never scans every actor or every gateway in the deployment.
type SubscriptionHub struct {
	mu       sync.Mutex
	lanes    map[uint64]*FarmEventLane
	bootRefs map[GateBootFence]map[SubscriptionRef]struct{}
	// farmBoots is the reverse of this farm's contribution to bootRefs, so
	// reindexFarm can drop the previous generation in O(watchers) instead of
	// scanning every subscription on the holder.
	farmBoots map[uint64]map[SubscriptionRef]GateBootFence
	// farmExpiry is the earliest Presence lease or cancel tombstone on a farm,
	// so PruneExpired only enters lanes that are actually due.
	farmExpiry              map[uint64]time.Time
	laneSize                int
	fallbackTTL             time.Duration
	subscriptionLeaseTTL    time.Duration
	maxLeaseExtension       time.Duration
	cancelTombstoneTTL      time.Duration
	maxCancelTombstones     int
	maxCancelTombstoneBytes int
	laneFull                atomic.Uint64
	laneResync              atomic.Uint64
	lanesRetired            atomic.Uint64
	joiningBufferDropped    atomic.Uint64
	versionGaps             atomic.Uint64
	invalidTotal            atomic.Uint64
	// invalidObserver 由指标注册层注入(farm_realtime_invalid_total{reason})。
	invalidObserver atomic.Value // func(reason string)
	// leaseObserver 由指标注册层注入(farm_subscription_lease_results_total{status})。
	leaseObserver atomic.Value // func(status string)
	// leaseExpiredObserver 统计租约过期导致的 Presence 移除
	// (farm_subscription_lease_expired_total),含 PruneExpired 清扫与续租时惰性移除。
	leaseExpiredObserver atomic.Value // func()
}

func NewSubscriptionHub(laneSize int) *SubscriptionHub {
	if laneSize <= 0 {
		laneSize = defaultFarmEventLaneSize
	}
	return &SubscriptionHub{
		lanes:                   make(map[uint64]*FarmEventLane),
		bootRefs:                make(map[GateBootFence]map[SubscriptionRef]struct{}),
		farmBoots:               make(map[uint64]map[SubscriptionRef]GateBootFence),
		farmExpiry:              make(map[uint64]time.Time),
		laneSize:                laneSize,
		fallbackTTL:             defaultSubscriptionFallbackTTL,
		subscriptionLeaseTTL:    defaultSubscriptionLeaseTTL,
		maxLeaseExtension:       defaultSubscriptionLeaseTTL,
		cancelTombstoneTTL:      defaultCancelTombstoneTTL,
		maxCancelTombstones:     defaultMaxCancelTombstones,
		maxCancelTombstoneBytes: defaultMaxCancelTombstoneBytes,
	}
}

func (h *SubscriptionHub) SubscriptionLeaseTTL() time.Duration {
	if h == nil || h.subscriptionLeaseTTL <= 0 {
		return defaultSubscriptionLeaseTTL
	}
	return h.subscriptionLeaseTTL
}

func (h *SubscriptionHub) MaxLeaseExtension() time.Duration {
	if h == nil || h.maxLeaseExtension <= 0 {
		return h.SubscriptionLeaseTTL()
	}
	return h.maxLeaseExtension
}

// SetSubscriptionLeaseConfig is a construction-time hook. The initial TTL is
// what JoinAccepted advertises; maxExtension bounds every renew request. The
// values remain probe parameters rather than protocol constants.
func (h *SubscriptionHub) SetSubscriptionLeaseConfig(initialTTL, maxExtension time.Duration) {
	if h == nil {
		return
	}
	if initialTTL > 0 {
		h.subscriptionLeaseTTL = initialTTL
	}
	if maxExtension > 0 {
		h.maxLeaseExtension = maxExtension
	}
	if h.maxLeaseExtension < h.subscriptionLeaseTTL {
		h.subscriptionLeaseTTL = h.maxLeaseExtension
	}
}

func (h *SubscriptionHub) FallbackTTL() time.Duration {
	if h == nil || h.fallbackTTL <= 0 {
		return defaultSubscriptionFallbackTTL
	}
	return h.fallbackTTL
}

// SetFallbackTTL is a construction-time/test hook. This TTL is intentionally
// long and is never renewed per heartbeat or per pushed change.
func (h *SubscriptionHub) SetFallbackTTL(ttl time.Duration) {
	if h != nil && ttl > 0 {
		h.fallbackTTL = ttl
	}
}

// SetCancelTombstoneLimitsForTest changes only runtime limits. Production
// values remain candidates until W31 load probes establish final sizing.
func (h *SubscriptionHub) SetCancelTombstoneLimitsForTest(items, bytes int, ttl time.Duration) {
	if h == nil {
		return
	}
	if items > 0 {
		h.maxCancelTombstones = items
	}
	if bytes > 0 {
		h.maxCancelTombstoneBytes = bytes
	}
	if ttl > 0 {
		h.cancelTombstoneTTL = ttl
	}
}

func (l *FarmEventLane) closeQuit() {
	l.quitOnce.Do(func() { close(l.quit) })
}

func (l *FarmEventLane) retireIf(ctx context.Context,
	apply func(*subscriptionState) (bool, error)) (bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		l.admissionMu.Lock()
		if l.retired.Load() {
			l.admissionMu.Unlock()
			return true, nil
		}
		if l.retiring {
			done := l.retireDone
			l.admissionMu.Unlock()
			select {
			case <-done:
				continue
			case <-l.quit:
				return true, nil
			case <-ctx.Done():
				return false, ctx.Err()
			}
		}
		l.retiring = true
		l.retireDone = make(chan struct{})
		done := l.retireDone
		req := laneRequest{
			done: make(chan laneResult, 1),
			apply: func(state *subscriptionState) (any, error) {
				retire, err := apply(state)
				l.admissionMu.Lock()
				if retire && err == nil {
					l.retired.Store(true)
				}
				l.retiring = false
				l.retireDone = nil
				close(done)
				if retire && err == nil {
					l.closeQuit()
				}
				l.admissionMu.Unlock()
				return retire, err
			},
		}
		select {
		case l.system <- req:
			l.admissionMu.Unlock()
		case <-ctx.Done():
			l.retiring = false
			l.retireDone = nil
			close(done)
			l.admissionMu.Unlock()
			return false, ctx.Err()
		}
		select {
		case result := <-req.done:
			retired, _ := result.value.(bool)
			return retired, result.err
		case <-l.quit:
			select {
			case result := <-req.done:
				retired, _ := result.value.(bool)
				return retired, result.err
			default:
			}
			if l.retired.Load() {
				return true, nil
			}
			return false, ErrFarmEventLaneRetired
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
}

func (h *SubscriptionHub) lane(farmID uint64) *FarmEventLane {
	h.mu.Lock()
	defer h.mu.Unlock()
	if lane := h.lanes[farmID]; lane != nil {
		return lane
	}
	lane := newFarmEventLane(h.laneSize)
	h.lanes[farmID] = lane
	return lane
}

func (h *SubscriptionHub) forgetRetired(farmID uint64, lane *FarmEventLane) {
	h.mu.Lock()
	if h.lanes[farmID] == lane && lane != nil && lane.retired.Load() {
		delete(h.lanes, farmID)
		delete(h.farmExpiry, farmID)
		h.lanesRetired.Add(1)
	}
	h.mu.Unlock()
}

func (h *SubscriptionHub) liveLane(farmID uint64) *FarmEventLane {
	for {
		lane := h.lane(farmID)
		if !lane.retired.Load() {
			return lane
		}
		h.forgetRetired(farmID, lane)
	}
}

func (h *SubscriptionHub) existingLiveLane(farmID uint64) (*FarmEventLane, bool) {
	for {
		h.mu.Lock()
		lane := h.lanes[farmID]
		h.mu.Unlock()
		if lane == nil {
			return nil, false
		}
		if !lane.retired.Load() {
			return lane, true
		}
		h.forgetRetired(farmID, lane)
	}
}

// Attach binds the owner to the lane as a System event so reactivation cannot
// copy state aliases while Reaper or an exact disconnect is mutating the Hub.
func (h *SubscriptionHub) Attach(farmID uint64, binding ActorBinding) {
	if h == nil || binding == nil {
		return
	}
	_, _ = h.ExecuteSystem(context.Background(), farmID, binding, func() (any, error) { return nil, nil })
}

func (h *SubscriptionHub) Execute(ctx context.Context, farmID uint64, binding ActorBinding,
	apply func() (any, error)) (any, error) {
	return h.executeWithClass(ctx, farmID, binding, false, apply)
}

func (h *SubscriptionHub) ExecuteSystem(ctx context.Context, farmID uint64, binding ActorBinding,
	apply func() (any, error)) (any, error) {
	return h.executeWithClass(ctx, farmID, binding, true, apply)
}

func (h *SubscriptionHub) executeWithClass(ctx context.Context, farmID uint64, binding ActorBinding, system bool,
	apply func() (any, error)) (any, error) {
	if h == nil {
		return apply()
	}
	var value any
	var err error
	for attempt := 0; attempt < 2; attempt++ {
		lane := h.liveLane(farmID)
		value, err = lane.execute(ctx, system, func(state *subscriptionState) (any, error) {
			if binding != nil {
				binding.BindState(state.presence, state.presenceByPlayer, state.presenceByAttempt,
					state.joinIdempotency, state.joinIdempotencyOrder, state.realtime)
			}
			value, err := apply()
			if binding != nil {
				state.joinIdempotencyOrder = binding.IdemOrder()
			}
			h.reindexFarm(farmID, state)
			return value, err
		})
		if !errors.Is(err, ErrFarmEventLaneRetired) {
			break
		}
		h.forgetRetired(farmID, lane)
	}
	if errors.Is(err, ErrFarmEventLaneFull) {
		h.laneFull.Add(1)
	}
	return value, err
}

// ExecuteCreateOrReuseJoining orders Join creation against CancelJoinAttempt
// in the same Farm lane. A retained tombstone wins even when Cancel arrived
// before FirstHop created the Presence. Admission fails closed when the lane
// cannot retain another minimum cancellation proof.
func (h *SubscriptionHub) ExecuteCreateOrReuseJoining(ctx context.Context, farmID uint64,
	binding ActorBinding, joinAttemptID string, now time.Time,
	apply func() (any, error)) (any, error) {
	if h == nil {
		return apply()
	}
	if now.IsZero() {
		now = time.Now()
	}
	var value any
	var err error
	for attempt := 0; attempt < 2; attempt++ {
		lane := h.liveLane(farmID)
		value, err = lane.execute(ctx, true, func(state *subscriptionState) (any, error) {
			h.pruneCancelTombstones(state, now)
			if _, cancelled := state.cancelledAttempts[joinAttemptID]; cancelled {
				return nil, ErrJoinAttemptCancelled
			}
			if !h.hasCancelTombstoneCapacity(state, 1, len(joinAttemptID)+64) {
				return nil, ErrCancelTombstoneCapacity
			}
			if binding != nil {
				binding.BindState(state.presence, state.presenceByPlayer, state.presenceByAttempt,
					state.joinIdempotency, state.joinIdempotencyOrder, state.realtime)
			}
			result, applyErr := apply()
			if binding != nil {
				state.joinIdempotencyOrder = binding.IdemOrder()
			}
			h.reindexFarm(farmID, state)
			return result, applyErr
		})
		if !errors.Is(err, ErrFarmEventLaneRetired) {
			break
		}
		h.forgetRetired(farmID, lane)
	}
	return value, err
}

// SetInvalidObserver 注册失效置位观测回调(指标层)。
func (h *SubscriptionHub) SetInvalidObserver(observe func(reason string)) {
	if h != nil && observe != nil {
		h.invalidObserver.Store(observe)
	}
}

func (h *SubscriptionHub) observeInvalid(reason string) {
	if h == nil {
		return
	}
	if observe, ok := h.invalidObserver.Load().(func(string)); ok && observe != nil {
		observe(reason)
	}
}

// SetLeaseObserver 注册续租结果观测回调(指标层,26 §11.5)。
func (h *SubscriptionHub) SetLeaseObserver(observe func(status string)) {
	if h != nil && observe != nil {
		h.leaseObserver.Store(observe)
	}
}

// SetLeaseExpiredObserver 注册租约过期移除观测回调(指标层)。
func (h *SubscriptionHub) SetLeaseExpiredObserver(observe func()) {
	if h != nil && observe != nil {
		h.leaseExpiredObserver.Store(observe)
	}
}

func (h *SubscriptionHub) observeLeaseStatus(status SubscriptionLeaseStatus) {
	if h == nil {
		return
	}
	if observe, ok := h.leaseObserver.Load().(func(string)); ok && observe != nil {
		observe(subscriptionLeaseStatusLabel(status))
	}
}

func (h *SubscriptionHub) observeLeaseExpired() {
	if h == nil {
		return
	}
	if observe, ok := h.leaseExpiredObserver.Load().(func()); ok && observe != nil {
		observe()
	}
}

// subscriptionLeaseStatusLabel 是指标层的有界标签集,与 wire 枚举一一对应。
func subscriptionLeaseStatusLabel(status SubscriptionLeaseStatus) string {
	switch status {
	case SubscriptionLeaseRenewed:
		return "RENEWED"
	case SubscriptionLeaseNotFound:
		return "NOT_FOUND"
	case SubscriptionLeaseStaleFence:
		return "STALE_FENCE"
	case SubscriptionLeaseClosed:
		return "CLOSED"
	case SubscriptionLeaseRouteStale:
		return "ROUTE_STALE"
	default:
		return "UNAVAILABLE"
	}
}

func (h *SubscriptionHub) ObserveLaneResync(count int) {
	if h != nil && count > 0 {
		h.laneResync.Add(uint64(count))
	}
}

type SubscriptionHubStats struct {
	ActiveLanes  int
	BootFences   int
	LaneFull     uint64
	LaneResync   uint64
	LanesRetired uint64
}

func (h *SubscriptionHub) Stats() SubscriptionHubStats {
	if h == nil {
		return SubscriptionHubStats{}
	}
	h.mu.Lock()
	stats := SubscriptionHubStats{ActiveLanes: len(h.lanes), BootFences: len(h.bootRefs)}
	h.mu.Unlock()
	stats.LaneFull = h.laneFull.Load()
	stats.LaneResync = h.laneResync.Load()
	stats.LanesRetired = h.lanesRetired.Load()
	return stats
}

func (h *SubscriptionHub) reindexFarm(farmID uint64, state *subscriptionState) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.dropFarmBootRefsLocked(farmID)
	if earliest := earliestFarmExpiry(state); earliest.IsZero() {
		delete(h.farmExpiry, farmID)
	} else {
		h.farmExpiry[farmID] = earliest
	}
	if state == nil || len(state.presence) == 0 {
		return
	}
	current := make(map[SubscriptionRef]GateBootFence, len(state.presence))
	for _, presence := range state.presence {
		if presence == nil || presence.GatewayID == "" || presence.GatewayBootID == "" {
			continue
		}
		ref := SubscriptionRef{
			FarmID: farmID, SubscriptionID: presence.SubscriptionID, PresenceEpoch: presence.PresenceEpoch,
		}
		fence := GateBootFence{GatewayID: presence.GatewayID, GatewayBootID: presence.GatewayBootID}
		refs := h.bootRefs[fence]
		if refs == nil {
			refs = make(map[SubscriptionRef]struct{})
			h.bootRefs[fence] = refs
		}
		refs[ref] = struct{}{}
		current[ref] = fence
	}
	if len(current) > 0 {
		h.farmBoots[farmID] = current
	}
}

func earliestFarmExpiry(state *subscriptionState) time.Time {
	var earliest time.Time
	consider := func(expiresAt time.Time) {
		if expiresAt.IsZero() {
			return
		}
		if earliest.IsZero() || expiresAt.Before(earliest) {
			earliest = expiresAt
		}
	}
	if state == nil {
		return earliest
	}
	for _, presence := range state.presence {
		if presence == nil {
			continue
		}
		if !presence.LeaseExpiresAt.IsZero() {
			consider(presence.LeaseExpiresAt)
		} else {
			consider(presence.FallbackExpiresAt)
		}
	}
	for _, tombstone := range state.cancelledAttempts {
		consider(tombstone.ExpiresAt)
	}
	return earliest
}

func (h *SubscriptionHub) dropFarmBootRefsLocked(farmID uint64) {
	previous := h.farmBoots[farmID]
	if len(previous) == 0 {
		delete(h.farmBoots, farmID)
		return
	}
	for ref, fence := range previous {
		refs := h.bootRefs[fence]
		delete(refs, ref)
		if len(refs) == 0 {
			delete(h.bootRefs, fence)
		}
	}
	delete(h.farmBoots, farmID)
}

func (h *SubscriptionHub) BootFences() []GateBootFence {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	result := make([]GateBootFence, 0, len(h.bootRefs))
	for fence := range h.bootRefs {
		result = append(result, fence)
	}
	h.mu.Unlock()
	sort.Slice(result, func(i, j int) bool {
		if result[i].GatewayID == result[j].GatewayID {
			return result[i].GatewayBootID < result[j].GatewayBootID
		}
		return result[i].GatewayID < result[j].GatewayID
	})
	return result
}

func (h *SubscriptionHub) SubscriptionRefs(fence GateBootFence) []SubscriptionRef {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	refs := h.bootRefs[fence]
	result := make([]SubscriptionRef, 0, len(refs))
	for ref := range refs {
		result = append(result, ref)
	}
	h.mu.Unlock()
	return result
}

// ReapBoot removes only exact Presence generations that still belong to the
// dead boot. A newer session on the same gateway or reused subscription ID is
// protected by both the boot fence and PresenceEpoch.
func (h *SubscriptionHub) ReapBoot(ctx context.Context, fence GateBootFence) ([]Presence, error) {
	refs := h.SubscriptionRefs(fence)
	removed := make([]Presence, 0, len(refs))
	for _, ref := range refs {
		lane := h.liveLane(ref.FarmID)
		value, err := lane.execute(ctx, true, func(state *subscriptionState) (any, error) {
			presence := state.presence[ref.SubscriptionID]
			if presence == nil || presence.PresenceEpoch != ref.PresenceEpoch ||
				presence.GatewayID != fence.GatewayID || presence.GatewayBootID != fence.GatewayBootID {
				return Presence{}, nil
			}
			copy := *presence
			removePresence(state, ref.SubscriptionID)
			h.reindexFarm(ref.FarmID, state)
			return copy, nil
		})
		if err != nil {
			return removed, err
		}
		if presence, ok := value.(Presence); ok && presence.SubscriptionID != "" {
			removed = append(removed, presence)
		}
	}
	return removed, nil
}

// DrainFarm is reserved for holder loss/panic. Normal actor passivation does
// not call it, which is the key separation between Actor and Hub lifetimes.
func (h *SubscriptionHub) DrainFarm(ctx context.Context, farmID uint64) ([]Presence, error) {
	lane := h.liveLane(farmID)
	value, err := lane.execute(ctx, true, func(state *subscriptionState) (any, error) {
		removed := make([]Presence, 0, len(state.presence))
		for _, presence := range state.presence {
			if presence != nil {
				removed = append(removed, *presence)
			}
		}
		state.presence = make(map[string]*Presence)
		state.presenceByPlayer = make(map[uint64]string)
		state.presenceByAttempt = make(map[string]string)
		state.joinIdempotency = make(map[string]JoinIdempotencyEntry)
		state.joinIdempotencyOrder = nil
		state.cancelledAttempts = make(map[string]CancelTombstone)
		state.cancelledAttemptOrder = nil
		state.cancelledAttemptBytes = 0
		h.reindexFarm(farmID, state)
		return removed, nil
	})
	if err != nil {
		return nil, err
	}
	removed, _ := value.([]Presence)
	return removed, nil
}

// CancelJoinAttempt installs a tombstone before removing an unarmed Join. If
// the Join has already armed or progressed, it returns the exact subscription
// id and leaves deletion to CleanupSubscription.
func (h *SubscriptionHub) CancelJoinAttempt(ctx context.Context, farmID uint64,
	req CancelJoinAttemptRequest) (CancelJoinAttemptResult, error) {
	if h == nil || farmID == 0 || req.JoinAttemptID == "" || req.OperationID == "" {
		return CancelJoinAttemptResult{Status: CancelAttemptStaleFence}, nil
	}
	if req.Now.IsZero() {
		req.Now = time.Now()
	}
	var value any
	var err error
	for attempt := 0; attempt < 2; attempt++ {
		lane := h.liveLane(farmID)
		value, err = lane.execute(ctx, true, func(state *subscriptionState) (any, error) {
			h.pruneCancelTombstones(state, req.Now)
			if tombstone, ok := state.cancelledAttempts[req.JoinAttemptID]; ok {
				if tombstone.OperationID == req.OperationID && sameCancelFence(tombstone.Fence, req.Fence) {
					return CancelJoinAttemptResult{Status: CancelAttemptCancelled}, nil
				}
				return CancelJoinAttemptResult{Status: CancelAttemptStaleFence}, nil
			}
			subscriptionID := state.presenceByAttempt[req.JoinAttemptID]
			if req.SubscriptionID != "" && subscriptionID != "" && req.SubscriptionID != subscriptionID {
				return CancelJoinAttemptResult{Status: CancelAttemptStaleFence}, nil
			}
			presence := state.presence[subscriptionID]
			if presence != nil {
				if presence.JoinOperationID != req.OperationID || !matchesCancelFence(presence, req.Fence) {
					return CancelJoinAttemptResult{Status: CancelAttemptStaleFence}, nil
				}
				if presence.Armed || presence.Phase != PresenceJoining {
					return CancelJoinAttemptResult{
						Status: CancelAttemptAlreadyProgressed, SubscriptionID: presence.SubscriptionID,
					}, nil
				}
			}
			if err := h.installCancelTombstone(state, req); err != nil {
				return nil, err
			}
			if presence != nil {
				removePresence(state, presence.SubscriptionID)
			}
			h.reindexFarm(farmID, state)
			return CancelJoinAttemptResult{Status: CancelAttemptCancelled}, nil
		})
		if !errors.Is(err, ErrFarmEventLaneRetired) {
			break
		}
		h.forgetRetired(farmID, lane)
	}
	if err != nil {
		return CancelJoinAttemptResult{}, err
	}
	result, _ := value.(CancelJoinAttemptResult)
	return result, nil
}

func (h *SubscriptionHub) pruneCancelTombstones(state *subscriptionState, now time.Time) {
	if state == nil {
		return
	}
	kept := state.cancelledAttemptOrder[:0]
	for _, attemptID := range state.cancelledAttemptOrder {
		tombstone, ok := state.cancelledAttempts[attemptID]
		if !ok || !tombstone.ExpiresAt.After(now) {
			if ok {
				state.cancelledAttemptBytes -= cancelTombstoneBytes(tombstone)
			}
			delete(state.cancelledAttempts, attemptID)
			continue
		}
		kept = append(kept, attemptID)
	}
	state.cancelledAttemptOrder = kept
	if state.cancelledAttemptBytes < 0 {
		state.cancelledAttemptBytes = 0
	}
}

func (h *SubscriptionHub) hasCancelTombstoneCapacity(state *subscriptionState, items, bytes int) bool {
	return state != nil && len(state.cancelledAttempts)+items <= h.maxCancelTombstones &&
		state.cancelledAttemptBytes+bytes <= h.maxCancelTombstoneBytes
}

func (h *SubscriptionHub) installCancelTombstone(state *subscriptionState,
	req CancelJoinAttemptRequest) error {
	ttl := h.cancelTombstoneTTL
	if ttl <= 0 {
		ttl = defaultCancelTombstoneTTL
	}
	tombstone := CancelTombstone{
		JoinAttemptID: req.JoinAttemptID,
		OperationID:   req.OperationID,
		Fence:         req.Fence,
		CancelledAt:   req.Now,
		ExpiresAt:     req.Now.Add(ttl),
	}
	bytes := cancelTombstoneBytes(tombstone)
	if !h.hasCancelTombstoneCapacity(state, 1, bytes) {
		return ErrCancelTombstoneCapacity
	}
	state.cancelledAttempts[req.JoinAttemptID] = tombstone
	state.cancelledAttemptOrder = append(state.cancelledAttemptOrder, req.JoinAttemptID)
	state.cancelledAttemptBytes += bytes
	return nil
}

func cancelTombstoneBytes(value CancelTombstone) int {
	return len(value.JoinAttemptID) + len(value.OperationID) + len(value.Fence.GatewayID) +
		len(value.Fence.GatewayBootID) + len(value.Fence.ConnectionID) + 96
}

func sameCancelFence(left, right LeaveRequest) bool {
	return left.ViewerPlayerID == right.ViewerPlayerID && left.SessionEpoch == right.SessionEpoch &&
		left.GatewayID == right.GatewayID && left.GatewayBootID == right.GatewayBootID &&
		left.ConnectionID == right.ConnectionID &&
		left.TransportGeneration == right.TransportGeneration && left.RouteRevision == right.RouteRevision
}

func matchesCancelFence(presence *Presence, fence LeaveRequest) bool {
	return presence != nil && presence.ViewerPlayerID == fence.ViewerPlayerID &&
		presence.SessionEpoch == fence.SessionEpoch && presence.GatewayID == fence.GatewayID &&
		presence.GatewayBootID == fence.GatewayBootID && presence.ConnectionID == fence.ConnectionID &&
		presence.TransportGeneration == fence.TransportGeneration &&
		presence.RouteRevision == fence.RouteRevision
}

// PresenceCount reads Hub state without activating FarmActor.
func (h *SubscriptionHub) PresenceCount(ctx context.Context, farmID uint64) (int, error) {
	lane, ok := h.existingLiveLane(farmID)
	if !ok {
		return 0, nil
	}
	value, err := lane.execute(ctx, true, func(state *subscriptionState) (any, error) {
		return len(state.presence), nil
	})
	if err != nil {
		return 0, err
	}
	return value.(int), nil
}

// RenewSubscription extends an existing Presence without activating FarmActor.
// It is serialized with Join/Leave/FarmChange by the target Farm Hub lane.
func (h *SubscriptionHub) RenewSubscription(ctx context.Context, farmID uint64,
	req SubscriptionLeaseRequest, trustedNow time.Time) (SubscriptionLeaseResult, error) {
	result := SubscriptionLeaseResult{Status: SubscriptionLeaseUnavailable, ServerTime: trustedNow}
	if h == nil {
		return result, nil
	}
	if trustedNow.IsZero() {
		trustedNow = time.Now()
		result.ServerTime = trustedNow
	}
	lane, ok := h.existingLiveLane(farmID)
	if !ok {
		result.Status = SubscriptionLeaseNotFound
		h.observeLeaseStatus(result.Status)
		return result, nil
	}
	value, err := lane.execute(ctx, true, func(state *subscriptionState) (any, error) {
		current := SubscriptionLeaseResult{Status: SubscriptionLeaseUnavailable, ServerTime: trustedNow}
		presence := state.presence[req.SubscriptionID]
		if presence == nil {
			current.Status = SubscriptionLeaseNotFound
			return current, nil
		}
		if !presence.LeaseExpiresAt.IsZero() && !trustedNow.Before(presence.LeaseExpiresAt) {
			removePresence(state, req.SubscriptionID)
			h.reindexFarm(farmID, state)
			h.observeLeaseExpired()
			current.Status = SubscriptionLeaseClosed
			return current, nil
		}
		if !renewablePresencePhase(presence.Phase) {
			current.Status = SubscriptionLeaseClosed
			return current, nil
		}
		if !matchesSubscriptionLeaseFence(presence, req) {
			current.Status = SubscriptionLeaseStaleFence
			return current, nil
		}
		if req.RequestedTTL <= 0 {
			return current, nil
		}
		accepted := req.RequestedTTL
		if maximum := h.MaxLeaseExtension(); accepted > maximum {
			accepted = maximum
		}
		if accepted <= 0 {
			return current, nil
		}
		presence.LeaseExpiresAt = trustedNow.Add(accepted)
		current.Status = SubscriptionLeaseRenewed
		current.AcceptedTTL = accepted
		return current, nil
	})
	if err != nil {
		return result, err
	}
	result, _ = value.(SubscriptionLeaseResult)
	h.observeLeaseStatus(result.Status)
	return result, nil
}

func renewablePresencePhase(phase PresencePhase) bool {
	switch phase {
	case PresenceJoining, PresenceFlushing, PresenceActivating, PresenceInFarm:
		return true
	default:
		return false
	}
}

func matchesSubscriptionLeaseFence(presence *Presence, req SubscriptionLeaseRequest) bool {
	if presence == nil || req.SubscriptionID == "" || req.JoinAttemptID == "" {
		return false
	}
	return presence.SubscriptionID == req.SubscriptionID &&
		presence.JoinAttemptID == req.JoinAttemptID &&
		presence.ViewerPlayerID == req.Fence.ViewerPlayerID &&
		presence.SessionEpoch == req.Fence.SessionEpoch &&
		presence.GatewayID == req.Fence.GatewayID &&
		presence.GatewayBootID == req.Fence.GatewayBootID &&
		presence.ConnectionID == req.Fence.ConnectionID &&
		presence.TransportGeneration == req.Fence.TransportGeneration &&
		presence.RouteRevision == req.Fence.RouteRevision
}

// PruneExpired closes short Subscription Leases, falling back to the historical
// long TTL only for legacy Presence values that predate LeaseExpiresAt.
func (h *SubscriptionHub) PruneExpired(ctx context.Context, now time.Time) ([]Presence, error) {
	if h == nil {
		return nil, nil
	}
	h.mu.Lock()
	farmIDs := make([]uint64, 0, len(h.farmExpiry))
	for farmID, expiresAt := range h.farmExpiry {
		if expiresAt.IsZero() || !now.Before(expiresAt) {
			farmIDs = append(farmIDs, farmID)
		}
	}
	h.mu.Unlock()
	var removed []Presence
	for _, farmID := range farmIDs {
		lane := h.liveLane(farmID)
		value, err := lane.execute(ctx, true, func(state *subscriptionState) (any, error) {
			h.pruneCancelTombstones(state, now)
			var expired []Presence
			for subscriptionID, presence := range state.presence {
				if presence == nil {
					continue
				}
				expiresAt := presence.LeaseExpiresAt
				if expiresAt.IsZero() {
					expiresAt = presence.FallbackExpiresAt
				}
				if expiresAt.IsZero() || now.Before(expiresAt) {
					continue
				}
				if !presence.LeaseExpiresAt.IsZero() && !now.Before(presence.LeaseExpiresAt) {
					h.observeLeaseExpired()
				}
				expired = append(expired, *presence)
				removePresence(state, subscriptionID)
			}
			h.reindexFarm(farmID, state)
			return expired, nil
		})
		if err != nil {
			return removed, err
		}
		expired, _ := value.([]Presence)
		removed = append(removed, expired...)
		if _, err := h.RetireFarmIfEmpty(ctx, farmID); err != nil {
			return removed, err
		}
	}
	return removed, nil
}

// CleanupPresence 精确移除单个订阅(合同 26 §11.3 末段/HC-ACCESS-28),不扫全量。
func (h *SubscriptionHub) CleanupPresence(ctx context.Context, farmID uint64, req LeaveRequest) (bool, error) {
	lane, ok := h.existingLiveLane(farmID)
	if !ok {
		return false, nil
	}
	value, err := lane.execute(ctx, true, func(state *subscriptionState) (any, error) {
		removed := leaveFenced(state, req)
		h.reindexFarm(farmID, state)
		return removed, nil
	})
	if err != nil {
		return false, err
	}
	removed, _ := value.(bool)
	return removed, nil
}

// CancelJoin 精确取消单个未完成 Join(与 CreateOrReuseJoining 同 Lane 排序)。
func (h *SubscriptionHub) CancelJoin(ctx context.Context, farmID uint64, req CancelJoinRequest) (bool, error) {
	lane, ok := h.existingLiveLane(farmID)
	if !ok {
		return false, nil
	}
	value, err := lane.execute(ctx, true, func(state *subscriptionState) (any, error) {
		canceled := cancelJoin(state, req)
		h.reindexFarm(farmID, state)
		return canceled, nil
	})
	if err != nil {
		return false, err
	}
	canceled, _ := value.(bool)
	return canceled, nil
}

// RetireFarmIfEmpty passivates Hub independently from FarmActor. The retire
// marker is installed inside the lane itself before quit is closed, so a
// concurrent caller either completed earlier or observes RETIRED and retries
// on a fresh lane; it cannot append to an orphaned state map.
func (h *SubscriptionHub) RetireFarmIfEmpty(ctx context.Context, farmID uint64) (bool, error) {
	if h == nil || farmID == 0 {
		return false, nil
	}
	lane, ok := h.existingLiveLane(farmID)
	if !ok {
		return false, nil
	}
	retired, err := lane.retireIf(ctx, func(state *subscriptionState) (bool, error) {
		if len(state.presence) != 0 || len(state.cancelledAttempts) != 0 ||
			len(lane.events) != 0 || len(lane.system) != 0 {
			return false, nil
		}
		return true, nil
	})
	if errors.Is(err, ErrFarmEventLaneRetired) && lane.retired.Load() {
		retired, err = true, nil
	}
	if retired {
		h.forgetRetired(farmID, lane)
	}
	return retired, err
}

func (h *SubscriptionHub) Stop() {
	if h == nil {
		return
	}
	h.mu.Lock()
	lanes := make([]*FarmEventLane, 0, len(h.lanes))
	for farmID, lane := range h.lanes {
		lanes = append(lanes, lane)
		delete(h.lanes, farmID)
	}
	h.bootRefs = make(map[GateBootFence]map[SubscriptionRef]struct{})
	h.farmBoots = make(map[uint64]map[SubscriptionRef]GateBootFence)
	h.farmExpiry = make(map[uint64]time.Time)
	h.mu.Unlock()
	for _, lane := range lanes {
		if lane == nil {
			continue
		}
		lane.admissionMu.Lock()
		first := lane.retired.CompareAndSwap(false, true)
		lane.closeQuit()
		lane.admissionMu.Unlock()
		if first {
			h.lanesRetired.Add(1)
		}
	}
}

// BlockLaneForTest 在目标 Farm Lane 上入队一个阻塞事件,直到 release 关闭;
// started 在事件开始执行时关闭。仅供故障注入测试使用。
func (h *SubscriptionHub) BlockLaneForTest(farmID uint64, started chan<- struct{}, release <-chan struct{}) error {
	lane := h.liveLane(farmID)
	_, err := lane.execute(context.Background(), false, func(*subscriptionState) (any, error) {
		close(started)
		<-release
		return nil, nil
	})
	return err
}

// LaneEventQueueLenForTest 返回 Lane 中排队的普通事件数。仅供故障注入测试使用。
func (h *SubscriptionHub) LaneEventQueueLenForTest(farmID uint64) int {
	lane, ok := h.existingLiveLane(farmID)
	if !ok {
		return 0
	}
	return len(lane.events)
}
