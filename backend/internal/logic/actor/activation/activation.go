// Package activation 实现 Actor 激活控制面:FSM、加载风暴硬门与失败退避。
//
// 状态机对齐合同:
//
//	ABSENT → LOADING → (CLAIM_UNKNOWN | ACTIVE) → QUIESCING → PASSIVATING → ABSENT
//
// QUIESCING 可撤销；进入 PASSIVATING 后旧 Activation 不再复活。Load 的
// TIMEOUT_UNKNOWN 使用独立 CLAIM_UNKNOWN，不能安装半初始化 Actor。
//
// per_shard_activation_rate 本刀以进程级 token bucket 实现(Options.ActivationRatePerSec);
// 节点级硬门为 MaxConcurrentLoads / MaxPendingLoads。
package activation

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"farmgame/internal/actorrouting"
	actorruntime "farmgame/internal/logic/actor/runtime"
)

// State 是激活 FSM 状态。
type State int

const (
	Absent State = iota
	Loading
	ClaimUnknown
	Active
	ResolvingUnknown
	ReconcileRequired
	Draining
	Fenced
	Quiescing
	Passivating
)

func (s State) String() string {
	switch s {
	case Absent:
		return "ABSENT"
	case Loading:
		return "LOADING"
	case ClaimUnknown:
		return "CLAIM_UNKNOWN"
	case Active:
		return "ACTIVE"
	case ResolvingUnknown:
		return "RESOLVING_UNKNOWN"
	case ReconcileRequired:
		return "RECONCILE_REQUIRED"
	case Draining:
		return "DRAINING"
	case Fenced:
		return "FENCED"
	case Quiescing:
		return "QUIESCING"
	case Passivating:
		return "PASSIVATING"
	default:
		return fmt.Sprintf("State(%d)", int(s))
	}
}

// AdmitAction 描述 AdmitUserCommand 对调用方的指示。
type AdmitAction int

const (
	// AdmitProceed 表示可对已激活 actor 执行用户命令(含刚从 PASSIVATING 取消钝化)。
	AdmitProceed AdmitAction = iota
	// AdmitNeedsLoad 表示 ABSENT,调用方应 BeginLoad→加载→CompleteLoad/FailLoad。
	AdmitNeedsLoad
)

var (
	// ErrStaleRoute 表示写权失效或钝化中不可复活;与 actorrouting.ErrStaleRoute 互认。
	ErrStaleRoute = actorrouting.ErrStaleRoute
	// ErrActivationThrottled 表示并发加载槽位或激活速率超限(可重试)。
	ErrActivationThrottled = errors.New("activation: throttled")
	// ErrTooManyPendingLoads 表示 pending 加载队列已满。
	ErrTooManyPendingLoads = errors.New("activation: too many pending loads")
	// ErrLoadBackoff 表示该 key 刚加载失败,仍在退避窗内。
	ErrLoadBackoff = errors.New("activation: load failure backoff")
	// ErrActorBusy 表示 LOADING / RESOLVING_UNKNOWN 等暂不可接用户命令。
	ErrActorBusy = actorruntime.ErrActorBusy
	// ErrInvalidTransition 表示非法 FSM 转移。
	ErrInvalidTransition = errors.New("activation: invalid transition")
)

// Options 对齐计划配置名;零值由 DefaultOptions / New 填默认。
type Options struct {
	// MaxConcurrentLoads 对应 max_concurrent_actor_loads。
	MaxConcurrentLoads int
	// MaxPendingLoads 对应 max_pending_actor_loads。
	// 当并发槽已满时:若 pending 计数已达上限 → ErrTooManyPendingLoads,否则 → ErrActivationThrottled。
	MaxPendingLoads int
	// ActivationRatePerSec 对应 per_shard_activation_rate;本刀为进程级 token bucket。
	// ≤0 表示不限速。
	ActivationRatePerSec float64
	// LoadFailureBackoff 对应 load_failure_backoff。
	LoadFailureBackoff time.Duration
}

// DefaultOptions 生产默认值。
func DefaultOptions() Options {
	return Options{
		MaxConcurrentLoads:   64,
		MaxPendingLoads:      256,
		ActivationRatePerSec: 1000,
		LoadFailureBackoff:   200 * time.Millisecond,
	}
}

func (o Options) withDefaults() Options {
	d := DefaultOptions()
	if o == (Options{}) {
		return d
	}
	if o.MaxConcurrentLoads <= 0 {
		o.MaxConcurrentLoads = d.MaxConcurrentLoads
	}
	if o.MaxPendingLoads <= 0 {
		o.MaxPendingLoads = d.MaxPendingLoads
	}
	// ActivationRatePerSec: 0 表示不限速,保留;负值回默认。
	if o.ActivationRatePerSec < 0 {
		o.ActivationRatePerSec = d.ActivationRatePerSec
	}
	if o.LoadFailureBackoff <= 0 {
		o.LoadFailureBackoff = d.LoadFailureBackoff
	}
	return o
}

type entry struct {
	state        State
	backoffUntil time.Time
	slotHeld     bool
	// passivated 记录该 entry 上一次离开 ACTIVE 是经过完整 passivation;
	// 下一次 BeginLoad 据此计 reactivation,使"重新激活"与冷启动可区分。
	passivated bool
}

// Controller 是可单测的激活控制面(非业务 FarmActor 方法)。
type Controller struct {
	mu              sync.Mutex
	opts            Options
	entries         map[uint64]*entry
	concurrentLoads int
	pendingLoads    int // 等待并发槽的 pending 名额(异步/测试可 EnqueuePending)
	tokens          float64
	lastRefill      time.Time
	now             func() time.Time
	throttledTotal  uint64
	backoffTotal    uint64
	panicTotal      uint64
	loadFailTotal   uint64
	passivateTotal  uint64
	reactivateTotal uint64
	forgottenTotal  uint64
	// stateCounts keeps metrics snapshots O(1). Prometheus invokes each
	// activation GaugeFunc independently; scanning the full entries map for
	// every gauge made /metrics a measurable part of the cold-login CPU path.
	stateCounts [10]int
}

// New 创建 Controller。
func New(opts Options) *Controller {
	opts = opts.withDefaults()
	now := time.Now
	c := &Controller{
		opts:       opts,
		entries:    make(map[uint64]*entry),
		tokens:     opts.ActivationRatePerSec,
		lastRefill: now(),
		now:        now,
	}
	if opts.ActivationRatePerSec <= 0 {
		c.tokens = 0
	}
	return c
}

// SetClock 注入时钟(单测用)。
func (c *Controller) SetClock(now func() time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if now != nil {
		c.now = now
		c.lastRefill = now()
	}
}

// StateOf 返回 key 当前状态;未见过的 key 视为 ABSENT。
func (c *Controller) StateOf(key uint64) State {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.entries[key]; e != nil {
		return e.state
	}
	return Absent
}

// Stats 是控制面观测快照。
type Stats struct {
	ConcurrentLoads int
	TrackedKeys     int
	PendingLoads    int
	// 各 FSM 状态当前 actor 数(含 ABSENT 占位 entry)。
	StateAbsent            int
	StateLoading           int
	StateClaimUnknown      int
	StateActive            int
	StateResolvingUnknown  int
	StateReconcileRequired int
	StateDraining          int
	StateFenced            int
	StateQuiescing         int
	StatePassivating       int
	ThrottledTotal         uint64
	BackoffTotal           uint64
	PanicTotal             uint64
	LoadFailTotal          uint64
	// PassivateCompletedTotal 是完成的 PASSIVATING → ABSENT 钝化次数。
	PassivateCompletedTotal uint64
	// ReactivatedTotal 是经历完整钝化后再次 BeginLoad 的次数。
	ReactivatedTotal uint64
	// EntryForgottenTotal 是钝化完成后从 map 删除的 ABSENT entry 数。
	EntryForgottenTotal uint64
}

// Stats 返回观测快照。
func (c *Controller) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	stats := Stats{
		ConcurrentLoads:         c.concurrentLoads,
		TrackedKeys:             len(c.entries),
		PendingLoads:            c.pendingLoads,
		ThrottledTotal:          c.throttledTotal,
		BackoffTotal:            c.backoffTotal,
		PanicTotal:              c.panicTotal,
		LoadFailTotal:           c.loadFailTotal,
		PassivateCompletedTotal: c.passivateTotal,
		ReactivatedTotal:        c.reactivateTotal,
		EntryForgottenTotal:     c.forgottenTotal,
	}
	stats.StateAbsent = c.stateCounts[Absent]
	stats.StateLoading = c.stateCounts[Loading]
	stats.StateClaimUnknown = c.stateCounts[ClaimUnknown]
	stats.StateActive = c.stateCounts[Active]
	stats.StateResolvingUnknown = c.stateCounts[ResolvingUnknown]
	stats.StateReconcileRequired = c.stateCounts[ReconcileRequired]
	stats.StateDraining = c.stateCounts[Draining]
	stats.StateFenced = c.stateCounts[Fenced]
	stats.StateQuiescing = c.stateCounts[Quiescing]
	stats.StatePassivating = c.stateCounts[Passivating]
	return stats
}

// AdmitUserCommand 在用户命令路径上裁决是否可继续/需加载/应拒绝。
func (c *Controller) AdmitUserCommand(key uint64, writable bool) (AdmitAction, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.ensureLocked(key)
	switch e.state {
	case Absent:
		if c.inBackoffLocked(e) {
			c.backoffTotal++
			return 0, ErrLoadBackoff
		}
		return AdmitNeedsLoad, nil
	case Active:
		return AdmitProceed, nil
	case Quiescing:
		if writable {
			c.setStateLocked(e, Active)
			return AdmitProceed, nil
		}
		// 写权失效:不得回 ACTIVE,保持 QUIESCING,由上层提交钝化/驱逐。
		return 0, ErrStaleRoute
	case Fenced, Draining:
		return 0, ErrStaleRoute
	case Loading, ClaimUnknown, ResolvingUnknown, ReconcileRequired, Passivating:
		return 0, ErrActorBusy
	default:
		return 0, fmt.Errorf("%w: admit from %s", ErrInvalidTransition, e.state)
	}
}

// BeginLoad: ABSENT → LOADING,占用并发加载槽。
func (c *Controller) BeginLoad(key uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.ensureLocked(key)
	if e.state != Absent {
		return fmt.Errorf("%w: BeginLoad from %s", ErrInvalidTransition, e.state)
	}
	if c.inBackoffLocked(e) {
		c.backoffTotal++
		return ErrLoadBackoff
	}
	if err := c.acquireLoadSlotLocked(); err != nil {
		return err
	}
	if e.passivated {
		// 该 key 上次离开 ACTIVE 是完整钝化;本次加载计为重新激活而非冷启动。
		c.reactivateTotal++
		e.passivated = false
	}
	c.setStateLocked(e, Loading)
	e.slotHeld = true
	return nil
}

// CompleteLoad: LOADING → ACTIVE,释放加载槽。
func (c *Controller) CompleteLoad(key uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.entries[key]
	if e == nil || e.state != Loading {
		state := Absent
		if e != nil {
			state = e.state
		}
		return fmt.Errorf("%w: CompleteLoad from %s", ErrInvalidTransition, state)
	}
	c.releaseLoadSlotLocked(e)
	c.setStateLocked(e, Active)
	e.backoffUntil = time.Time{}
	return nil
}

// FailLoad: LOADING → ABSENT,释放槽并启动失败退避。
func (c *Controller) FailLoad(key uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.entries[key]
	if e == nil || e.state != Loading {
		state := Absent
		if e != nil {
			state = e.state
		}
		return fmt.Errorf("%w: FailLoad from %s", ErrInvalidTransition, state)
	}
	c.releaseLoadSlotLocked(e)
	c.setStateLocked(e, Absent)
	e.backoffUntil = c.now().Add(c.opts.LoadFailureBackoff)
	c.loadFailTotal++
	return nil
}

// MarkActive 仅在已 Active 时为幂等;其它状态拒绝(正常路径用 CompleteLoad)。
func (c *Controller) MarkActive(key uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.ensureLocked(key)
	if e.state == Active {
		return nil
	}
	return fmt.Errorf("%w: MarkActive from %s", ErrInvalidTransition, e.state)
}

// MarkClaimUnknown: LOADING → CLAIM_UNKNOWN。加载槽和共享恢复身份都保留。
func (c *Controller) MarkClaimUnknown(key uint64) error {
	return c.transitionFrom(key, Loading, ClaimUnknown, "MarkClaimUnknown")
}

// ResolveClaimUnknownToActive 安装唯一 Actor 后释放加载槽。
func (c *Controller) ResolveClaimUnknownToActive(key uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.ensureLocked(key)
	if e.state != ClaimUnknown {
		return fmt.Errorf("%w: ResolveClaimUnknownToActive from %s", ErrInvalidTransition, e.state)
	}
	c.releaseLoadSlotLocked(e)
	c.setStateLocked(e, Active)
	e.backoffUntil = time.Time{}
	return nil
}

// ResolveClaimUnknownToAbsent 证明 Claim 未执行后释放槽并回退。
func (c *Controller) ResolveClaimUnknownToAbsent(key uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.ensureLocked(key)
	if e.state != ClaimUnknown {
		return fmt.Errorf("%w: ResolveClaimUnknownToAbsent from %s", ErrInvalidTransition, e.state)
	}
	c.releaseLoadSlotLocked(e)
	c.setStateLocked(e, Absent)
	e.backoffUntil = c.now().Add(c.opts.LoadFailureBackoff)
	return nil
}

// FenceClaimUnknown 表示 Shard/WriterEpoch 已换代；本 Holder 不安装 Actor。
func (c *Controller) FenceClaimUnknown(key uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.ensureLocked(key)
	if e.state != ClaimUnknown {
		return fmt.Errorf("%w: FenceClaimUnknown from %s", ErrInvalidTransition, e.state)
	}
	c.releaseLoadSlotLocked(e)
	c.setStateLocked(e, Fenced)
	return nil
}

// MarkLoadReconcileRequired 对无法解释的 Load/Claim 事实 Fail Closed。
func (c *Controller) MarkLoadReconcileRequired(key uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.ensureLocked(key)
	if e.state != Loading && e.state != ClaimUnknown {
		return fmt.Errorf("%w: MarkLoadReconcileRequired from %s", ErrInvalidTransition, e.state)
	}
	c.releaseLoadSlotLocked(e)
	c.setStateLocked(e, ReconcileRequired)
	return nil
}

// BeginPassivate: ACTIVE → QUIESCING。此阶段新消息可以撤销钝化。
func (c *Controller) BeginPassivate(key uint64) error {
	return c.transitionFrom(key, Active, Quiescing, "BeginPassivate")
}

// CancelPassivate: QUIESCING → ACTIVE。
func (c *Controller) CancelPassivate(key uint64) error {
	return c.transitionFrom(key, Quiescing, Active, "CancelPassivate")
}

// CommitPassivate: QUIESCING → PASSIVATING。此后旧 Activation 不可复活。
func (c *Controller) CommitPassivate(key uint64) error {
	return c.transitionFrom(key, Quiescing, Passivating, "CommitPassivate")
}

// FinishPassivate: PASSIVATING → ABSENT,记录钝化 tombstone 供 reactivation 统计。
func (c *Controller) FinishPassivate(key uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.ensureLocked(key)
	if e.state != Passivating {
		return fmt.Errorf("%w: FinishPassivate from %s", ErrInvalidTransition, e.state)
	}
	c.setStateLocked(e, Absent)
	e.passivated = true
	c.passivateTotal++
	return nil
}

// ForgetIfAbsent 在钝化资源释放完成后删除 ABSENT entry,使 entries map
// 按"当前活跃 Actor"回落而不是按历史访问数增长。只删除仍为 ABSENT 的 entry:
// 若新命令已触发 LOADING/ACTIVE,entry 属于新生命周期,不得删除。
// 删除后丢失 passivated tombstone,之后的加载等同冷启动(少计 reactivation,可接受)。
func (c *Controller) ForgetIfAbsent(key uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.entries[key]
	if e == nil || e.state != Absent {
		return false
	}
	delete(c.entries, key)
	c.stateCounts[Absent]--
	c.forgottenTotal++
	return true
}

// BeginDrain: ACTIVE → DRAINING。
func (c *Controller) BeginDrain(key uint64) error {
	return c.transitionFrom(key, Active, Draining, "BeginDrain")
}

// FinishDrain: DRAINING → ABSENT。
func (c *Controller) FinishDrain(key uint64) error {
	return c.transitionFrom(key, Draining, Absent, "FinishDrain")
}

// Fence: ACTIVE → FENCED。
func (c *Controller) Fence(key uint64) error {
	return c.transitionFrom(key, Active, Fenced, "Fence")
}

// FinishFence: FENCED → ABSENT。
func (c *Controller) FinishFence(key uint64) error {
	return c.transitionFrom(key, Fenced, Absent, "FinishFence")
}

// BeginResolveUnknown: ACTIVE → RESOLVING_UNKNOWN。
func (c *Controller) BeginResolveUnknown(key uint64) error {
	return c.transitionFrom(key, Active, ResolvingUnknown, "BeginResolveUnknown")
}

// ResolveUnknownToActive: RESOLVING_UNKNOWN → ACTIVE。
func (c *Controller) ResolveUnknownToActive(key uint64) error {
	return c.transitionFrom(key, ResolvingUnknown, Active, "ResolveUnknownToActive")
}

// ResolveUnknownToAbsent: RESOLVING_UNKNOWN → ABSENT。
func (c *Controller) ResolveUnknownToAbsent(key uint64) error {
	return c.transitionFrom(key, ResolvingUnknown, Absent, "ResolveUnknownToAbsent")
}

// MarkAbsent 强制回到 ABSENT(用于 fence 驱逐 / panic 丢弃);释放可能持有的加载槽。
func (c *Controller) MarkAbsent(key uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.entries[key]
	if e == nil {
		return
	}
	c.releaseLoadSlotLocked(e)
	c.setStateLocked(e, Absent)
	e.backoffUntil = time.Time{}
}

// OnPanic 移除激活态并回 ABSENT;调用方必须丢弃可能损坏的内存对象。
func (c *Controller) OnPanic(key uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.ensureLocked(key)
	c.releaseLoadSlotLocked(e)
	c.setStateLocked(e, Absent)
	e.backoffUntil = time.Time{}
	c.panicTotal++
}

// AcquireLoadSlot 暴露硬门给需要单独占槽的调用方;成功后须 ReleaseLoadSlot。
// 通常优先用 BeginLoad/CompleteLoad/FailLoad。
func (c *Controller) AcquireLoadSlot() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.acquireLoadSlotLocked()
}

// ReleaseLoadSlot 释放一次 AcquireLoadSlot。
func (c *Controller) ReleaseLoadSlot() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.concurrentLoads > 0 {
		c.concurrentLoads--
	}
}

// EnqueuePending 占用一个 pending 加载名额(异步等待或单测灌满 pending 队列)。
// 成功后须 DequeuePending;超限返回 ErrTooManyPendingLoads。
func (c *Controller) EnqueuePending() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pendingLoads >= c.opts.MaxPendingLoads {
		c.throttledTotal++
		return ErrTooManyPendingLoads
	}
	c.pendingLoads++
	return nil
}

// DequeuePending 释放 EnqueuePending 名额。
func (c *Controller) DequeuePending() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pendingLoads > 0 {
		c.pendingLoads--
	}
}

func (c *Controller) transitionFrom(key uint64, from, to State, op string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.ensureLocked(key)
	if e.state != from {
		return fmt.Errorf("%w: %s from %s", ErrInvalidTransition, op, e.state)
	}
	c.setStateLocked(e, to)
	return nil
}

func (c *Controller) setStateLocked(e *entry, state State) {
	if e == nil || e.state == state {
		return
	}
	if e.state >= Absent && e.state <= Passivating {
		c.stateCounts[e.state]--
	}
	c.stateCounts[state]++
	e.state = state
}

func (c *Controller) ensureLocked(key uint64) *entry {
	e := c.entries[key]
	if e == nil {
		e = &entry{state: Absent}
		c.entries[key] = e
		c.stateCounts[Absent]++
	}
	return e
}

func (c *Controller) inBackoffLocked(e *entry) bool {
	if e.backoffUntil.IsZero() {
		return false
	}
	return c.now().Before(e.backoffUntil)
}

func (c *Controller) acquireLoadSlotLocked() error {
	if err := c.takeRateTokenLocked(); err != nil {
		return err
	}
	if c.concurrentLoads >= c.opts.MaxConcurrentLoads {
		// 并发槽已满:若 pending 队列也无空位 → TooManyPending;否则 throttle(同步路径不排队)。
		if c.pendingLoads >= c.opts.MaxPendingLoads {
			c.throttledTotal++
			return ErrTooManyPendingLoads
		}
		c.throttledTotal++
		return ErrActivationThrottled
	}
	c.concurrentLoads++
	return nil
}

func (c *Controller) releaseLoadSlotLocked(e *entry) {
	if e == nil || !e.slotHeld {
		return
	}
	e.slotHeld = false
	if c.concurrentLoads > 0 {
		c.concurrentLoads--
	}
}

func (c *Controller) takeRateTokenLocked() error {
	rate := c.opts.ActivationRatePerSec
	if rate <= 0 {
		return nil
	}
	now := c.now()
	elapsed := now.Sub(c.lastRefill).Seconds()
	if elapsed > 0 {
		c.tokens += elapsed * rate
		if c.tokens > rate {
			c.tokens = rate
		}
		c.lastRefill = now
	}
	if c.tokens < 1 {
		c.throttledTotal++
		return ErrActivationThrottled
	}
	c.tokens--
	return nil
}
