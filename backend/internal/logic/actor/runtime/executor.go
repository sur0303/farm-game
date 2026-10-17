// Package runtime 实现最小 keyed-actor 执行器:同 key 串行、异 key 并行、有界邮箱。
//
// 语义对齐 27 §6.2 / docs/design/50-actor-runtime.md:
//   - 同一 key 的所有请求由单 goroutine 串行处理;
//   - 不同 key 并行;
//   - UserMailbox(有界 item + byte)与 SystemQueue(独立有界)隔离;
//   - 调度优先 SystemQueue,避免 LeaseLost/Drain 等被用户过载饿死;
//   - User 满返回 ErrMailboxFull/ErrActorBusy;System 满返回 ErrSystemQueueFull。
package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultMailboxSize      = 256
	defaultSystemQueueSize  = 32
	defaultUserTaskBytes    = 1024 // Submit/SubmitUser 无 size 时的保守估算
	defaultMaxUserBytesMult = defaultUserTaskBytes

	// RetryAfterBusy 供上层将 UserMailbox 过载映射为 retry_after_ms(建议区间 50–200ms)。
	RetryAfterBusy = 100 * time.Millisecond
)

var (
	// ErrMailboxFull 表示目标 actor 的 UserMailbox 已满(item 或 byte)。
	ErrMailboxFull error = mailboxFullError{}
	// ErrActorBusy 是 UserMailbox 过载的语义别名;与 ErrMailboxFull 对同一拒绝可互相 errors.Is。
	ErrActorBusy error = actorBusyError{}
	// ErrSystemQueueFull 表示 SystemQueue 已满;不得与 user 满混淆。
	ErrSystemQueueFull = errors.New("runtime: system queue full")
	// ErrStopped 表示执行器已关闭。
	ErrStopped = errors.New("runtime: executor stopped")
	// ErrActorPanic 表示 task 发生 panic;mailbox 仍存活,但上层须丢弃可能损坏的 actor。
	ErrActorPanic = errors.New("runtime: actor panic")
	// errMailboxRetired 表示投递目标 mailbox 已退休;Submit 路径会换取新 generation 重试,
	// 不会把任务投递到已关闭的 channel。不对外暴露(errors.Is 不跨 API 边界使用)。
	errMailboxRetired = errors.New("runtime: mailbox retired")
)

type mailboxFullError struct{}

func (mailboxFullError) Error() string { return "runtime: mailbox full" }
func (mailboxFullError) Is(target error) bool {
	return target == ErrMailboxFull || target == ErrActorBusy
}

type actorBusyError struct{}

func (actorBusyError) Error() string { return "runtime: actor busy" }
func (actorBusyError) Is(target error) bool {
	return target == ErrMailboxFull || target == ErrActorBusy
}

// SystemKind 标识系统消息种类(Go 枚举,不必进 proto)。
type SystemKind int

const (
	SystemKindLoadCompleted SystemKind = iota
	SystemKindPersistenceCompleted
	SystemKindUnknownResolved
	SystemKindLeaseLost
	SystemKindDrain
	SystemKindPassivate
	SystemKindConnectionCleanup
	SystemKindStop
	SystemKindActorPanic
)

// Task 是一次在 actor 内串行执行的工作单元,返回业务结果与错误。
type Task func(ctx context.Context) (any, error)

type job struct {
	ctx          context.Context
	task         Task
	done         chan result
	release      func()
	payloadBytes int
	system       bool
	kind         SystemKind
	enqueuedAt   time.Time
}

type result struct {
	val any
	err error
}

// mailbox 是单个 key 的双队列 + 处理 goroutine。
// generation 标识该 mailbox 实例:退休后再激活会创建新 generation,
// 使并发的退休请求能通过 generation 校验避免误删新 mailbox。
type mailbox struct {
	key        uint64
	generation uint64
	userCh     chan job
	// userOverflow is a lazy bounded tail for bursts above the initial channel
	// capacity. Keeping this out of the channel backing array is important for
	// cold-login churn: most keys never have a second queued user task.
	userOverflow []job
	systemCh     chan job
	wake         chan struct{}
	once         sync.Once
	closing      chan struct{}
	// exited 在处理 goroutine 退出后关闭,供退休可观察性断言(含测试)。
	exited chan struct{}
	// retired 只在同时持有 Executor.mu 与 mailbox.mu 时置位;置位后所有
	// enqueue 路径拒绝新任务,Executor.boxes 中对应条目已被删除。
	retired atomic.Bool
	// lastActivity 记录最近一次入队/任务完成时间(UnixNano),供 idle 判定。
	lastActivity atomic.Int64

	mu               sync.Mutex
	userBytes        int
	maxUserBytes     int
	maxUserItems     int
	userEnqueueTimes []time.Time // FIFO with userCh/userOverflow; for oldest_wait gauge
	inFlight         int         // 正在执行的 job 数(0 或 1);退休必须观察到 0
}

// Options 控制 UserMailbox / SystemQueue 容量。零值字段由 NewOptions 填默认。
type Options struct {
	UserMailboxSize        int // user item 最大上限;≤0 则 256
	UserMailboxInitialSize int // 每个 key 首次预留的 channel 槽位;≤0 则等于最大上限
	SystemQueueSize        int // system item 上限;≤0 则 32
	MaxUserBytes           int // user byte 预算;≤0 则 UserMailboxSize * 1KiB
	// AdmissionGate is consulted only by UserMailbox submissions, before a
	// mailbox is created. SystemQueue messages are independent of this gate.
	AdmissionGate AdmissionGate
	// OnPanic 在 task panic 且已向 done 投递 ErrActorPanic 后回调;可选。
	// 调用方应丢弃该 key 的损坏 actor,不得复用内存对象。
	OnPanic func(key uint64)
	// TurnObserver 可选;每个 turn 执行完成(仅 task 耗时,不含队列等待)后回调,
	// 供上层聚合 farm_actor_turn_latency_seconds 直方图。
	TurnObserver func(key uint64, duration time.Duration)
	// BeforeJobExecution is a deterministic execution hook. It runs after a
	// job has been claimed and counted in-flight but before its task starts.
	// Production callers should leave it nil.
	BeforeJobExecution func(key uint64, generation uint64)
}

// Executor 是 keyed-actor 执行器。零值不可用,请用 New / NewOptions。
type Executor struct {
	mu                     sync.RWMutex
	boxes                  map[uint64]*mailbox
	userMailboxSize        int
	userMailboxInitialSize int
	systemQueueSize        int
	maxUserBytes           int
	admissionGate          AdmissionGate
	onPanic                func(key uint64)
	stopped                bool
	stoppedFast            atomic.Bool
	wg                     sync.WaitGroup
	submitted              atomic.Uint64
	completed              atomic.Uint64
	userRejected           atomic.Uint64
	systemRejected         atomic.Uint64
	panicTotal             atomic.Uint64
	turnObserver           func(uint64, time.Duration)
	beforeJob              func(uint64, uint64)
	nextGeneration         atomic.Uint64
	retiredTotal           atomic.Uint64
	retireBusy             atomic.Uint64
	retireStale            atomic.Uint64
	stopOnce               sync.Once
	stopDone               chan struct{}
}

// New 创建执行器。mailboxSize 是每个 key 的 UserMailbox item 容量。
func New(mailboxSize int) *Executor {
	return NewOptions(Options{UserMailboxSize: mailboxSize})
}

// NewOptions 按 Options 创建执行器。
func NewOptions(opts Options) *Executor {
	userSize := opts.UserMailboxSize
	if userSize <= 0 {
		userSize = defaultMailboxSize
	}
	initialUserSize := opts.UserMailboxInitialSize
	if initialUserSize <= 0 {
		initialUserSize = userSize
	}
	if initialUserSize > userSize {
		initialUserSize = userSize
	}
	if initialUserSize <= 0 {
		initialUserSize = 1
	}
	sysSize := opts.SystemQueueSize
	if sysSize <= 0 {
		sysSize = defaultSystemQueueSize
	}
	maxBytes := opts.MaxUserBytes
	if maxBytes <= 0 {
		maxBytes = userSize * defaultMaxUserBytesMult
	}
	return &Executor{
		boxes:                  make(map[uint64]*mailbox),
		userMailboxSize:        userSize,
		userMailboxInitialSize: initialUserSize,
		systemQueueSize:        sysSize,
		maxUserBytes:           maxBytes,
		admissionGate:          opts.AdmissionGate,
		onPanic:                opts.OnPanic,
		turnObserver:           opts.TurnObserver,
		beforeJob:              opts.BeforeJobExecution,
		stopDone:               make(chan struct{}),
	}
}

// SetOnPanic 设置/替换 panic 回调(可在 New 之后挂接 registry 卸载逻辑)。
func (e *Executor) SetOnPanic(fn func(key uint64)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.onPanic = fn
}

// SetTurnObserver 设置/替换 turn 耗时回调(观测 farm_actor_turn_latency_seconds)。
func (e *Executor) SetTurnObserver(fn func(key uint64, duration time.Duration)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.turnObserver = fn
}

// Submit 将 task 投递到 key 的 UserMailbox 并等待串行执行完成。
// 兼容入口,行为与 SubmitUser 相同。
func (e *Executor) Submit(ctx context.Context, key uint64, task Task) (any, error) {
	return e.SubmitUser(ctx, key, task)
}

// SubmitUser 走 UserMailbox;无 size 时按 defaultUserTaskBytes 计入 byte 预算。
func (e *Executor) SubmitUser(ctx context.Context, key uint64, task Task) (any, error) {
	return e.SubmitUserSized(ctx, key, defaultUserTaskBytes, task)
}

// SubmitUserSized 走 UserMailbox,并以 payloadBytes 计入 byte 预算(与 item 双限)。
func (e *Executor) SubmitUserSized(ctx context.Context, key uint64, payloadBytes int, task Task) (any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if payloadBytes < 0 {
		payloadBytes = 0
	}
	j := job{
		ctx:          ctx,
		task:         task,
		done:         make(chan result, 1),
		payloadBytes: payloadBytes,
		enqueuedAt:   time.Now(),
	}
	// Do not create a mailbox for a request that is already canceled. The
	// enqueue-time check below is the admission linearization point for a
	// cancellation that races mailbox lookup/queue selection.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var release func()
	if e.admissionGate != nil {
		var err error
		release, err = e.admissionGate.Admit(ctx, key)
		if err != nil {
			return nil, err
		}
	}
	releaseRejected := func() {
		if release != nil {
			release()
			release = nil
		}
	}
	// mailbox 可能与退休并发:已退休的 mailbox 不再接受任务,换取新 generation 重试,
	// 任务不会被投递到已关闭的 channel。
	for {
		if err := ctx.Err(); err != nil {
			releaseRejected()
			return nil, err
		}
		mb, err := e.mailboxFor(ctx, key)
		if err != nil {
			releaseRejected()
			return nil, err
		}
		j.release = release
		err = e.enqueueUser(mb, j)
		if err == nil {
			release = nil
			e.submitted.Add(1)
			return waitResult(ctx, j.done)
		}
		if ctx.Err() != nil {
			_ = e.RetireIfIdle(key, mb.generation)
		}
		if !errors.Is(err, errMailboxRetired) {
			releaseRejected()
			e.userRejected.Add(1)
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			releaseRejected()
			return nil, err
		}
	}
}

// SubmitSystem 走独立 SystemQueue;满时返回 ErrSystemQueueFull。
func (e *Executor) SubmitSystem(ctx context.Context, key uint64, kind SystemKind, task Task) (any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	j := job{
		ctx:        ctx,
		task:       task,
		done:       make(chan result, 1),
		system:     true,
		kind:       kind,
		enqueuedAt: time.Now(),
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		mb, err := e.mailboxFor(ctx, key)
		if err != nil {
			return nil, err
		}
		err = e.enqueueSystem(mb, j)
		if err == nil {
			e.submitted.Add(1)
			return waitResult(ctx, j.done)
		}
		if ctx.Err() != nil {
			_ = e.RetireIfIdle(key, mb.generation)
			return nil, ctx.Err()
		}
		if !errors.Is(err, errMailboxRetired) {
			e.systemRejected.Add(1)
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
}

func waitResult(ctx context.Context, done <-chan result) (any, error) {
	select {
	case r := <-done:
		return r.val, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (e *Executor) enqueueUser(mb *mailbox, j job) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	if e.stoppedFast.Load() {
		return ErrStopped
	}
	if mb.retired.Load() {
		return errMailboxRetired
	}
	// This check is deliberately under the same mailbox lock as the channel
	// send. A ctx that expires after this admission point is still a queued
	// request, but execJob checks it again before invoking the handler; a ctx
	// already expired here never occupies UserMailbox capacity.
	if j.ctx != nil {
		if err := j.ctx.Err(); err != nil {
			return err
		}
	}
	if mb.userBytes+j.payloadBytes > mb.maxUserBytes {
		return ErrActorBusy
	}
	if len(mb.userCh)+len(mb.userOverflow) >= mb.maxUserItems {
		return ErrMailboxFull
	}
	select {
	case mb.userCh <- j:
	default:
		// Only the hot tail allocates a slice. The common cold-login case has
		// one in-flight task and no queued tail, so the mailbox retains only
		// the small initial channel backing array.
		mb.userOverflow = append(mb.userOverflow, j)
	}
	mb.userBytes += j.payloadBytes
	at := j.enqueuedAt
	if at.IsZero() {
		at = time.Now()
	}
	mb.userEnqueueTimes = append(mb.userEnqueueTimes, at)
	mb.lastActivity.Store(at.UnixNano())
	notifyMailbox(mb)
	return nil
}

func (e *Executor) enqueueSystem(mb *mailbox, j job) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	if e.stoppedFast.Load() {
		return ErrStopped
	}
	if mb.retired.Load() {
		return errMailboxRetired
	}
	if j.ctx != nil {
		if err := j.ctx.Err(); err != nil {
			return err
		}
	}
	select {
	case mb.systemCh <- j:
		at := j.enqueuedAt
		if at.IsZero() {
			at = time.Now()
		}
		mb.lastActivity.Store(at.UnixNano())
		notifyMailbox(mb)
		return nil
	default:
		return ErrSystemQueueFull
	}
}

func notifyMailbox(mb *mailbox) {
	select {
	case mb.wake <- struct{}{}:
	default:
	}
}

func (mb *mailbox) releaseUserBytes(n int) {
	if n <= 0 {
		return
	}
	mb.mu.Lock()
	mb.userBytes -= n
	if mb.userBytes < 0 {
		mb.userBytes = 0
	}
	mb.mu.Unlock()
}

func (mb *mailbox) popUserEnqueueTime() {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	if len(mb.userEnqueueTimes) == 0 {
		return
	}
	mb.userEnqueueTimes = mb.userEnqueueTimes[1:]
}

func (e *Executor) mailboxFor(ctx context.Context, key uint64) (*mailbox, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		e.mu.RLock()
		if e.stopped {
			e.mu.RUnlock()
			return nil, ErrStopped
		}
		mb, ok := e.boxes[key]
		e.mu.RUnlock()
		if ok {
			if mb.retired.Load() {
				// 退休与取引用并发:该 mailbox 已退休,重取以获得新 generation。
				continue
			}
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return mb, nil
		}

		e.mu.Lock()
		if err := ctx.Err(); err != nil {
			e.mu.Unlock()
			return nil, err
		}
		if e.stopped {
			e.mu.Unlock()
			return nil, ErrStopped
		}
		if mb, ok = e.boxes[key]; ok {
			e.mu.Unlock()
			continue
		}
		mb = &mailbox{
			key:          key,
			generation:   e.nextGeneration.Add(1),
			userCh:       make(chan job, e.userMailboxInitialSize),
			systemCh:     make(chan job, e.systemQueueSize),
			wake:         make(chan struct{}, 1),
			closing:      make(chan struct{}),
			exited:       make(chan struct{}),
			maxUserBytes: e.maxUserBytes,
			maxUserItems: e.userMailboxSize,
		}
		mb.lastActivity.Store(time.Now().UnixNano())
		e.boxes[key] = mb
		e.wg.Add(1)
		go e.run(mb)
		e.mu.Unlock()
		return mb, nil
	}
}

// run 是单个 key 的串行处理循环;有 system 待办时优先于 user。
// 退休后 closing 关闭,goroutine 退出并关闭 exited;退休只在双队列皆空
// 且无 in-flight 时发生,retired 置位后 enqueue 全部拒绝,因此退出时不会有残留任务。
func (e *Executor) run(mb *mailbox) {
	defer e.wg.Done()
	defer close(mb.exited)
	for {
		if e.stoppedFast.Load() {
			e.dropQueued(mb)
			return
		}
		j, claimed, retired := e.tryClaim(mb)
		if retired {
			e.dropQueued(mb)
			return
		}
		if claimed {
			if e.stoppedFast.Load() {
				e.rejectClaimedJob(mb, j)
				e.dropQueued(mb)
				return
			}
			e.beforeJobExecution(mb)
			e.execJob(mb, j)
			continue
		}
		select {
		case <-mb.wake:
		case <-mb.closing:
			e.dropQueued(mb)
			return
		}
	}
}

// tryClaim removes one queued job and marks it in-flight while holding the
// mailbox lock. RetireIfIdle takes the same lock, so it can never observe a
// dequeued-but-not-inFlight job.
func (e *Executor) tryClaim(mb *mailbox) (job, bool, bool) {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	if mb.retired.Load() {
		return job{}, false, true
	}
	select {
	case j := <-mb.systemCh:
		mb.inFlight++
		return j, true, false
	default:
	}
	select {
	case j := <-mb.userCh:
		mb.inFlight++
		return j, true, false
	default:
		if len(mb.userOverflow) == 0 {
			return job{}, false, false
		}
		j := mb.userOverflow[0]
		if len(mb.userOverflow) == 1 {
			mb.userOverflow = nil
		} else {
			mb.userOverflow = mb.userOverflow[1:]
		}
		mb.inFlight++
		return j, true, false
	}
}

func (e *Executor) beforeJobExecution(mb *mailbox) {
	e.mu.RLock()
	hook := e.beforeJob
	e.mu.RUnlock()
	if hook == nil {
		return
	}
	func() {
		defer func() { _ = recover() }()
		hook(mb.key, mb.generation)
	}()
}

func (e *Executor) execJob(mb *mailbox, j job) {
	// finish 必须在结果交付给调用方之前标记任务结束:RetireIfIdle 与 idle 扫描
	// 据此判断 mailbox 已无在途任务;若先唤醒调用方再递减,调用方侧的退休判定
	// 会与递减竞争(race 下可稳定复现残留 mailbox)。
	finished := false
	finish := func() {
		if finished {
			return
		}
		finished = true
		mb.mu.Lock()
		mb.inFlight--
		mb.mu.Unlock()
		mb.lastActivity.Store(time.Now().UnixNano())
		if j.release != nil {
			j.release()
			j.release = nil
		}
	}
	defer finish()
	if !j.system {
		mb.releaseUserBytes(j.payloadBytes)
		mb.popUserEnqueueTime()
	}
	if j.ctx.Err() != nil {
		finish()
		j.done <- result{nil, j.ctx.Err()}
		return
	}
	start := time.Now()
	val, err := func() (val any, err error) {
		defer func() {
			if rec := recover(); rec != nil {
				e.panicTotal.Add(1)
				err = fmt.Errorf("%w: %v", ErrActorPanic, rec)
				e.mu.RLock()
				onPanic := e.onPanic
				e.mu.RUnlock()
				if onPanic != nil {
					func() {
						defer func() { _ = recover() }()
						onPanic(mb.key)
					}()
				}
			}
		}()
		return j.task(j.ctx)
	}()
	e.mu.RLock()
	observer := e.turnObserver
	e.mu.RUnlock()
	if observer != nil {
		func() {
			defer func() { _ = recover() }()
			observer(mb.key, time.Since(start))
		}()
	}
	e.completed.Add(1)
	finish()
	j.done <- result{val, err}
}

func (e *Executor) rejectClaimedJob(mb *mailbox, j job) {
	if !j.system {
		mb.releaseUserBytes(j.payloadBytes)
		mb.popUserEnqueueTime()
	}
	mb.mu.Lock()
	if mb.inFlight > 0 {
		mb.inFlight--
	}
	mb.mu.Unlock()
	if j.release != nil {
		j.release()
	}
	j.done <- result{err: ErrStopped}
}

func (e *Executor) dropQueued(mb *mailbox) {
	if mb == nil {
		return
	}
	var dropped []job
	mb.mu.Lock()
	for {
		select {
		case j := <-mb.userCh:
			dropped = append(dropped, j)
		default:
			goto usersDone
		}
	}
usersDone:
	for {
		select {
		case j := <-mb.systemCh:
			dropped = append(dropped, j)
		default:
			goto systemsDone
		}
	}
systemsDone:
	dropped = append(dropped, mb.userOverflow...)
	mb.userOverflow = nil
	mb.userBytes = 0
	mb.userEnqueueTimes = nil
	mb.mu.Unlock()
	for _, j := range dropped {
		if j.release != nil {
			j.release()
		}
		j.done <- result{err: ErrStopped}
	}
}

// Stop 停止执行器,不再接受新 Submit,并等待所有在途 actor goroutine 退出。
func (e *Executor) Stop() {
	if e == nil {
		return
	}
	e.stopOnce.Do(func() {
		e.mu.Lock()
		e.stopped = true
		e.stoppedFast.Store(true)
		for _, mb := range e.boxes {
			mb.once.Do(func() { close(mb.closing) })
		}
		e.mu.Unlock()
		e.wg.Wait()
		e.mu.Lock()
		clear(e.boxes)
		e.mu.Unlock()
		close(e.stopDone)
	})
	<-e.stopDone
}

// Stopped reports whether the executor has crossed its stop linearization
// point. It is used by mailbox-free owner paths that share the executor's
// lifecycle but cannot submit a task just to discover the state.
func (e *Executor) Stopped() bool {
	return e == nil || e.stoppedFast.Load()
}

// ActiveKeys 返回当前有邮箱的 key 数(观测用)。
func (e *Executor) ActiveKeys() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.boxes)
}

// RetireOutcome 描述按 key 退休的结果。
type RetireOutcome int

const (
	// RetireRetired 表示 mailbox 已从 map 摘除并信号 goroutine 退出。
	RetireRetired RetireOutcome = iota
	// RetireNotFound 表示该 key 当前没有 mailbox(可能已退休)。
	RetireNotFound
	// RetireBusy 表示仍有排队或在途任务,mailbox 保留。
	RetireBusy
	// RetireStaleGeneration 表示 map 中已是更新 generation 的 mailbox,
	// 调用方持有的是旧引用,不得退休新 mailbox。
	RetireStaleGeneration
)

func (o RetireOutcome) String() string {
	switch o {
	case RetireRetired:
		return "retired"
	case RetireNotFound:
		return "not_found"
	case RetireBusy:
		return "busy"
	case RetireStaleGeneration:
		return "stale_generation"
	default:
		return "unknown"
	}
}

// MailboxSnapshot 是单 key mailbox 的观测快照,供 idle 判定与 generation 校验。
type MailboxSnapshot struct {
	Generation   uint64
	LastActivity time.Time
	UserQueued   int
	SystemQueued int
	InFlight     bool
	Retired      bool
}

// MailboxSnapshot 读取 key 当前 mailbox 的快照;不存在时 ok=false。
func (e *Executor) MailboxSnapshot(key uint64) (MailboxSnapshot, bool) {
	e.mu.RLock()
	mb, ok := e.boxes[key]
	e.mu.RUnlock()
	if !ok {
		return MailboxSnapshot{}, false
	}
	mb.mu.Lock()
	defer mb.mu.Unlock()
	return MailboxSnapshot{
		Generation:   mb.generation,
		LastActivity: time.Unix(0, mb.lastActivity.Load()),
		UserQueued:   len(mb.userCh) + len(mb.userOverflow),
		SystemQueued: len(mb.systemCh),
		InFlight:     mb.inFlight > 0,
		Retired:      mb.retired.Load(),
	}, true
}

// RetireIfIdle 仅当 mailbox 匹配 generation、双队列皆空且无在途任务时退休它:
// 从 map 摘除、置 retired 并关闭 closing,处理 goroutine 随后退出(exited 关闭)。
// 退休成功后并发的新 Submit 会创建新 generation 的 mailbox,任务不会投递到已关闭 channel。
// Stop() 仍是全局关闭语义;本 API 是普通 actor passivation 使用的按 key 路径。
func (e *Executor) RetireIfIdle(key uint64, generation uint64) RetireOutcome {
	e.mu.Lock()
	mb, ok := e.boxes[key]
	if !ok {
		e.mu.Unlock()
		return RetireNotFound
	}
	if mb.generation != generation {
		e.mu.Unlock()
		e.retireStale.Add(1)
		return RetireStaleGeneration
	}
	mb.mu.Lock()
	if mb.retired.Load() {
		mb.mu.Unlock()
		e.mu.Unlock()
		return RetireNotFound
	}
	if len(mb.userCh)+len(mb.userOverflow) != 0 || len(mb.systemCh) != 0 || mb.inFlight != 0 {
		mb.mu.Unlock()
		e.mu.Unlock()
		e.retireBusy.Add(1)
		return RetireBusy
	}
	mb.retired.Store(true)
	delete(e.boxes, key)
	mb.mu.Unlock()
	e.mu.Unlock()
	mb.once.Do(func() { close(mb.closing) })
	e.retiredTotal.Add(1)
	return RetireRetired
}

// Stats 是执行器观测快照。
type Stats struct {
	ActiveKeys     int
	Queued         int // UserQueued + SystemQueued(兼容旧字段)
	UserQueued     int
	SystemQueued   int
	UserBytes      int           // 当前 UserMailbox 占用字节合计
	OldestUserWait time.Duration // 各 key UserMailbox 队头等待的最大值
	MaxDepth       int           // max(user depth);兼容旧字段
	MaxUserDepth   int
	MaxSystemDepth int
	Submitted      uint64
	Completed      uint64
	Rejected       uint64 // UserRejected + SystemRejected(兼容旧字段)
	UserRejected   uint64
	SystemRejected uint64
	PanicTotal     uint64
	// MailboxRetiredTotal 是成功按 key 退休的 mailbox 数。
	MailboxRetiredTotal uint64
	// MailboxRetireBusyTotal 是因排队/在途任务而保留的退休尝试数。
	MailboxRetireBusyTotal uint64
	// MailboxRetireStaleTotal 是因 generation 不匹配而放弃的退休尝试数。
	MailboxRetireStaleTotal uint64
}

// Stats 复制 mailbox 指针后释放执行器锁再汇总深度/字节,新建 key 不必等完整扫描。
func (e *Executor) Stats() Stats {
	userRejected := e.userRejected.Load()
	systemRejected := e.systemRejected.Load()
	stats := Stats{
		Submitted:               e.submitted.Load(),
		Completed:               e.completed.Load(),
		UserRejected:            userRejected,
		SystemRejected:          systemRejected,
		Rejected:                userRejected + systemRejected,
		PanicTotal:              e.panicTotal.Load(),
		MailboxRetiredTotal:     e.retiredTotal.Load(),
		MailboxRetireBusyTotal:  e.retireBusy.Load(),
		MailboxRetireStaleTotal: e.retireStale.Load(),
	}
	e.mu.RLock()
	stats.ActiveKeys = len(e.boxes)
	mailboxes := make([]*mailbox, 0, len(e.boxes))
	for _, mailbox := range e.boxes {
		mailboxes = append(mailboxes, mailbox)
	}
	e.mu.RUnlock()
	now := time.Now()
	for _, mailbox := range mailboxes {
		mailbox.mu.Lock()
		userDepth := len(mailbox.userCh) + len(mailbox.userOverflow)
		sysDepth := len(mailbox.systemCh)
		stats.UserQueued += userDepth
		stats.SystemQueued += sysDepth
		stats.Queued += userDepth + sysDepth
		if userDepth > stats.MaxUserDepth {
			stats.MaxUserDepth = userDepth
		}
		if sysDepth > stats.MaxSystemDepth {
			stats.MaxSystemDepth = sysDepth
		}
		if userDepth > stats.MaxDepth {
			stats.MaxDepth = userDepth
		}
		stats.UserBytes += mailbox.userBytes
		if len(mailbox.userEnqueueTimes) > 0 {
			wait := now.Sub(mailbox.userEnqueueTimes[0])
			if wait > stats.OldestUserWait {
				stats.OldestUserWait = wait
			}
		}
		mailbox.mu.Unlock()
	}
	return stats
}
