// Package actor implements the farm_id keyed FarmActor. Durable FarmRecord is
// owned by Actor turns; ephemeral Presence is owned by SubscriptionHub and
// serialized through the per-farm FarmEventLane so the two lifetimes can
// passivate independently. Network delivery remains outside both lanes.
package actor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	farmpb "farmgame/api/gen/farm"
	"farmgame/internal/actormodel/fieldv2"
	"farmgame/internal/actorrouting"
	farmactormodelv2 "farmgame/internal/farm/actormodelv2"
	"farmgame/internal/farm/domain"
	"farmgame/internal/gateway/session"
	"farmgame/internal/logic/actor/activation"
	"farmgame/internal/logic/actor/authority"
	"farmgame/internal/logic/actor/idle"
	actorruntime "farmgame/internal/logic/actor/runtime"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/blobstore"

	"google.golang.org/protobuf/proto"
)

const DefaultPlotCount = 24

type Task func(context.Context, *FarmActor) (any, error)

// ParticipantTask is the Farm Participant mailbox seam.  The capacity guard
// is built from the same Authority Binding snapshot that loaded the actor;
// callers cannot manufacture a trusted guard or move the proof work into the
// actor turn.
type ParticipantTask func(context.Context, *FarmActor, ParticipantCapacityGuard) (any, error)

// ParticipantPhysicalCapacity is the immutable physical admission profile
// reported by the concrete AuthorityStore used by this Registry.  It is only
// a preflight; the Authority Commit remains the final atomic capacity fence.
type ParticipantPhysicalCapacity struct {
	MaxReceiptItems       int
	MaxModelBytes         int
	MaxReceiptBytes       int
	MaxSingleReceiptBytes int
	MaxCommitBytes        int
}

var (
	// ErrParticipantReceiptAuthorityUnavailable means that this Registry cannot
	// provide the atomic, writer-fenced receipt read required by a production
	// Participant ingress.  Legacy blobstore registries fail closed here.
	ErrParticipantReceiptAuthorityUnavailable = errors.New("farm actor: atomic participant receipt authority unavailable")
	// ErrParticipantPhysicalCapacity is normalized by the Participant transport
	// to the stable ERR_RATE_LIMITED/ResourceExhausted boundary. It is returned
	// only after a trusted physical projection proves an actual limit rejection.
	ErrParticipantPhysicalCapacity = errors.New("farm actor: participant physical capacity exhausted")
	// ErrParticipantCapacityUnavailable means the physical snapshot/projector
	// was not usable. It is deliberately distinct from a real capacity reject so
	// ingress preserves UNAVAILABLE instead of manufacturing ResourceExhausted.
	ErrParticipantCapacityUnavailable = errors.New("farm actor: participant physical capacity snapshot unavailable")
	// ErrParticipantAdmissionBusy is returned by mailbox-free admission when a
	// same-farm Actor turn already owns the coordination gate. Admission never
	// waits behind that turn's AuthorityStore I/O; the caller can retry.
	ErrParticipantAdmissionBusy = errors.New("farm actor: participant admission coordination busy")
	// ErrParticipantCapacityAdmissionStale means a concurrent authority commit
	// advanced the version between the mailbox-free read and admission check.
	ErrParticipantCapacityAdmissionStale = errors.New("farm actor: participant capacity admission record changed")
)

func completeParticipantPhysicalCapacity(capacity ParticipantPhysicalCapacity) bool {
	return capacity.MaxReceiptItems > 0 && capacity.MaxModelBytes > 0 &&
		capacity.MaxReceiptBytes > 0 && capacity.MaxSingleReceiptBytes > 0 &&
		capacity.MaxCommitBytes > 0
}

func participantPhysicalCapacityFromStore(caps authoritystore.PhysicalCapabilities) (ParticipantPhysicalCapacity, error) {
	if err := caps.Validate(); err != nil {
		return ParticipantPhysicalCapacity{}, err
	}
	capacity := ParticipantPhysicalCapacity{
		MaxReceiptItems:       caps.MaxReceiptItems,
		MaxModelBytes:         caps.MaxModelBytes,
		MaxReceiptBytes:       caps.MaxReceiptBytes,
		MaxSingleReceiptBytes: caps.MaxSingleReceiptBytes,
		MaxCommitBytes:        caps.MaxCommitBytes,
	}
	if !completeParticipantPhysicalCapacity(capacity) {
		return ParticipantPhysicalCapacity{}, errors.New("farm actor: authority participant capacity profile is incomplete")
	}
	return capacity, nil
}

// ParticipantCapacityGuard combines a version-bound physical usage snapshot
// with the concrete Store's exact next CASEpoch projection.  Its trusted
// fields are private so only Registry can construct it.
type ParticipantCapacityGuard struct {
	binding *authority.Binding
	version uint64
	limits  ParticipantPhysicalCapacity
}

// Check must run before a Participant mutates a candidate and again before
// the Authority Commit.  It rejects stale or unavailable physical evidence.
func (g ParticipantCapacityGuard) Check(candidateModelBytes int) error {
	if g.binding == nil || !completeParticipantPhysicalCapacity(g.limits) {
		return fmt.Errorf("%w: participant capacity guard is unavailable", ErrParticipantCapacityUnavailable)
	}
	if g.binding.HasPendingUnknown() {
		return blobstore.ErrTimeoutUnknown
	}
	if g.binding.Version() != g.version {
		return ErrParticipantCapacityAdmissionStale
	}
	usage := g.binding.PhysicalUsage()
	if err := usage.Validate(g.version); err != nil {
		return fmt.Errorf("%w: %v", ErrParticipantCapacityUnavailable, err)
	}
	projection, err := g.binding.ProjectCASEpochPhysicalCommit(g.version, candidateModelBytes)
	if err != nil {
		if errors.Is(err, authoritystore.ErrCapacityExceeded) {
			return ErrParticipantPhysicalCapacity
		}
		return fmt.Errorf("%w: %v", ErrParticipantCapacityUnavailable, err)
	}
	if projection.ModelBytes != candidateModelBytes {
		return fmt.Errorf("%w: physical projector disagreed with candidate model size", ErrParticipantCapacityUnavailable)
	}
	if projection.ModelBytes > g.limits.MaxModelBytes ||
		projection.SingleReceiptBytes > g.limits.MaxSingleReceiptBytes ||
		projection.CommitBytes > g.limits.MaxCommitBytes ||
		usage.ReceiptItems > g.limits.MaxReceiptItems ||
		projection.ReceiptItems > g.limits.MaxReceiptItems-usage.ReceiptItems ||
		usage.ReceiptBytes > g.limits.MaxReceiptBytes ||
		projection.ReceiptBytes > g.limits.MaxReceiptBytes-usage.ReceiptBytes {
		return ErrParticipantPhysicalCapacity
	}
	return nil
}

// PassivationBlocker 原因常量,与交接文档 §6.3.2 的 blocker 快照边界一致。
// Presence 数量与持久化 RETRY_WAIT Workflow 刻意不在其中(阶段 5 已拆耦)。
const (
	BlockerTurnRunning          = "turn_running"
	BlockerUserQueueNonempty    = "user_queue_nonempty"
	BlockerSystemQueueNonempty  = "system_queue_nonempty"
	BlockerPersistenceInFlight  = "persistence_in_flight"
	BlockerResolvingUnknown     = "resolving_unknown"
	BlockerDirtyUnhanded        = "dirty_unhanded"
	BlockerParticipantUnanchor  = "participant_result_unanchored"
	BlockerParticipantAdmission = "participant_admission_in_flight"
)

// PassivationOptions 控制自动 idle passivation;零值表示启用并使用默认值。
// 当前 Redis CAS 在 Actor Turn 内同步完成,persistence_in_flight / dirty_unhanded /
// participant_result_unanchored 均被 turn_running 覆盖;保留原因常量是为将来
// 异步 persistence 预留判定边界,不要为此把持久化异步化。
type PassivationOptions struct {
	// Disable 关闭自动 passivation(保留手工 Begin/FinishPassivate 入口)。
	Disable bool
	// IdleTimeout 是 Actor 最后一次活动到成为退休候选的时间;默认 5 分钟。
	IdleTimeout time.Duration
	// BucketWidth 是 Sweeper 时间桶宽度;默认 30s。
	BucketWidth time.Duration
	// SweepInterval 是后台扫描周期;默认等于 BucketWidth。
	SweepInterval time.Duration
	// MaxPerSweep 限制单次扫描候选数;默认 256。
	MaxPerSweep int
	// BlockedRetryDelay 是被阻塞候选的重试延迟;默认 5s。
	BlockedRetryDelay time.Duration
	// Now 注入时钟(测试用 fake clock);默认 time.Now。
	Now func() time.Time
}

// RegistryOptions 控制 Farm Registry 的激活限流、邮箱容量与自动钝化。
type RegistryOptions struct {
	// MailboxSize is the logical maximum queued user turns per Farm actor.
	// MailboxInitialSize controls only the per-key preallocated channel; a
	// lazy overflow tail preserves the larger burst limit without reserving it
	// for every cold actor.
	MailboxSize        int
	MailboxInitialSize int
	FarmEventLaneSize  int
	Activation         activation.Options
	Passivation        PassivationOptions
	AdmissionGate      actorruntime.AdmissionGate
	// TheftPRF is the server-side deterministic random provider used by the
	// FarmActor Theft turn.  It is never sourced from a Participant payload.
	TheftPRF TheftPRF
	// ParticipantAdmissionSnapshotReader is an optional composition seam for a
	// cold-farm, bounded read-only snapshot. It must not claim/activate an
	// Actor, mutate a Binding, or call a slow AuthorityStore operation. When it
	// is absent, ReadOutsideMailbox fails closed until an Actor is already
	// active and its immutable in-memory record snapshot can be used.
	ParticipantAdmissionSnapshotReader ParticipantAdmissionSnapshotReader
	// ParticipantGuardSnapshotReader is the durable GuardSync read used when a
	// FarmActor is activated. A missing snapshot is represented by the zero
	// GuardSnapshot (no active guard); Registry never creates an in-memory
	// fallback for a configured reader.
	ParticipantGuardSnapshotReader ParticipantGuardSnapshotReader
	// ModelFieldV2Conversion is a default-off v1 conversion gate. Existing v2
	// actors ignore disablement and remain read/write v2.
	ModelFieldV2Conversion fieldv2.ConversionPolicy
	// NewModelLayout controls the layout used for newly created authority rows.
	// Zero keeps the legacy/test default V1; production explicitly sets V2.
	NewModelLayout authoritystore.ModelLayoutVersion
	// OpportunityWakeupEnabled attaches one mutable source-local schedule to
	// every successful Farm authority mutation. It is explicitly feature
	// gated so the A/Pull baseline incurs no scheduler write amplification.
	OpportunityWakeupEnabled bool
}

// ParticipantAdmissionSnapshotReader is intentionally smaller than the frozen
// AuthorityStore interface. A production composition may provide a local,
// bounded read-only snapshot for cold farms; Registry never uses it to mutate
// authority or to establish a Writer Binding. Implementations must honor ctx.
type ParticipantAdmissionSnapshotReader interface {
	ReadParticipantAdmissionSnapshot(context.Context, uint64) (*farmpb.FarmRecord, uint64, error)
}

// ParticipantGuardSnapshotReader supplies the latest persisted GuardSync
// snapshot before a FarmActor can adjudicate Theft. Production implementations
// must be backed by the GuardSync authority and must not call another Actor.
type ParticipantGuardSnapshotReader interface {
	ReadParticipantGuardSnapshot(context.Context, uint64) (domain.GuardSnapshot, error)
}

type Registry struct {
	store         blobstore.Store
	exec          *actorruntime.Executor
	admissionGate actorruntime.AdmissionGate
	activation    *activation.Controller
	mu            sync.Mutex
	// authorityCoordMu protects only the short-lived farm coordination index;
	// it is never held while waiting for an AuthorityStore or while running an
	// Actor turn. Each coordination gate is keyed by farm_id, so an unrelated
	// farm can continue when one farm's authority I/O is slow.
	authorityCoordMu sync.Mutex
	authorityCoords  map[uint64]*farmAuthorityCoord
	actors           map[uint64]*FarmActor
	unknown          map[uint64]UnknownContext
	// unknownResolved retains the authoritative Fence resolution after the
	// Binding has cleared its pending marker. Delivery may still need a retry;
	// losing this runtime fact would make a committed UNKNOWN impossible to
	// hand off safely on the next resolver attempt.
	unknownResolved map[uint64]authority.Resolution
	resolving       map[uint64]bool
	autoResolve     bool
	nextEpoch       atomic.Uint64
	writerEpoch     func(uint64) (uint64, error)
	writable        func(uint64) bool
	resync          func(uint64, []Presence, string, uint64, uint64)
	routeMetrics    *actorrouting.Metrics // optional; writer_epoch_mismatch_total
	// authority 非空时 Registry 运行在 AuthorityStore 主线模式(AR1):激活经
	// LoadFutureGroup singleflight + LoadAndClaim 建立 WriterClaim,save 经
	// Binding 落到 Authority Commit,Unknown 经 FenceAndClaim(C1→C2) 解析。
	// 与 store(blobstore legacy 回滚选项)互斥,同一 Registry 只有一条写主链。
	authority *authority.Provider
	// retiredBindings holds at most one terminal same-generation tombstone per
	// farm after generation loss removed the active Actor. It prevents a stale
	// local FenceSource from immediately reclaiming the same authority and keeps
	// pending UNKNOWN evidence available to lifecycle/facts settlement.
	retiredBindings            map[uint64]*authority.Binding
	modelFieldV2Conversion     fieldv2.ConversionPolicy
	newModelLayout             authoritystore.ModelLayoutVersion
	loadGroup                  *activation.LoadFutureGroup
	participantCapacity        ParticipantPhysicalCapacity
	participantCapacityKnown   bool
	participantCapacityErr     error
	participantAdmissionReader ParticipantAdmissionSnapshotReader
	participantGuardReader     ParticipantGuardSnapshotReader
	theftPRF                   TheftPRF
	opportunityWakeupEnabled   bool
	// cleanupActorScans counts invocations of the legacy connection-wide
	// cleanup path. The exact production path must leave this at zero.
	cleanupActorScans atomic.Uint64
	subscriptions     *SubscriptionHub

	// holderShardFor freezes the mapper used by holder-control facts/rearm so an
	// authority shard cannot reopen without complete actor+tombstone eviction.
	holderControlMu sync.RWMutex
	holderShardFor  actorruntime.ShardForActor

	// 自动 idle passivation(阶段 6):sweeper 只跟踪候选,二次校验与资源释放
	// 在 tryPassivateIdle 内完成;sweepCancel 停止后台扫描。
	sweeper     *idle.Sweeper
	passivation PassivationOptions
	sweepCancel context.CancelFunc
}

// farmAuthorityCoord is a bounded-lifetime per-farm gate for the mutable
// authority.Binding snapshot. refs includes both the current holder and
// waiters, which keeps the gate alive until every operation that acquired the
// farm entry has either entered or abandoned the gate. Once refs reaches zero
// the Registry removes it, so historical farm IDs do not accumulate locks.
type farmAuthorityCoord struct {
	gate          chan struct{}
	refs          int
	admissionRefs int
}

func newFarmAuthorityCoord() *farmAuthorityCoord {
	gate := make(chan struct{}, 1)
	gate <- struct{}{}
	return &farmAuthorityCoord{gate: gate}
}

// acquireAuthorityCoord serializes all authority-sensitive work for one
// farm_id. The Registry mutex protects only map/ref bookkeeping; the channel
// wait and the protected AuthorityStore I/O happen without that mutex.
func (r *Registry) acquireAuthorityCoord(ctx context.Context, farmID uint64) (func(), error) {
	return r.acquireAuthorityCoordMode(ctx, farmID, true)
}

func (r *Registry) acquireAuthorityCoordMode(ctx context.Context, farmID uint64, wait bool) (func(), error) {
	if r == nil || farmID == 0 {
		return nil, errors.New("farm actor: authority coordination unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.authorityCoordMu.Lock()
	coord := r.authorityCoords[farmID]
	if coord == nil {
		coord = newFarmAuthorityCoord()
		r.authorityCoords[farmID] = coord
	}
	coord.refs++
	r.authorityCoordMu.Unlock()

	if !wait {
		if err := ctx.Err(); err != nil {
			r.authorityCoordMu.Lock()
			coord.refs--
			if coord.refs == 0 && r.authorityCoords[farmID] == coord {
				delete(r.authorityCoords, farmID)
			}
			r.authorityCoordMu.Unlock()
			return nil, err
		}
		select {
		case <-coord.gate:
		default:
			r.authorityCoordMu.Lock()
			coord.refs--
			if coord.refs == 0 && r.authorityCoords[farmID] == coord {
				delete(r.authorityCoords, farmID)
			}
			r.authorityCoordMu.Unlock()
			return nil, ErrParticipantAdmissionBusy
		}
	} else {
		select {
		case <-coord.gate:
		case <-ctx.Done():
			r.authorityCoordMu.Lock()
			coord.refs--
			if coord.refs == 0 && r.authorityCoords[farmID] == coord {
				delete(r.authorityCoords, farmID)
			}
			r.authorityCoordMu.Unlock()
			return nil, ctx.Err()
		}
	}

	{
		var once sync.Once
		return func() {
			once.Do(func() {
				coord.gate <- struct{}{}
				r.authorityCoordMu.Lock()
				coord.refs--
				if coord.refs == 0 && r.authorityCoords[farmID] == coord {
					delete(r.authorityCoords, farmID)
				}
				r.authorityCoordMu.Unlock()
			})
		}, nil
	}
}

// acquireParticipantAdmissionCoord is the lifecycle-aware variant used by
// mailbox-free receipt/capacity admission. In addition to the per-farm gate it
// records an external admission reference, so passivation can report the
// participant blocker before its second-stage deletion. The same gate is held
// until the caller has stopped using the activated Actor snapshot.
func (r *Registry) acquireParticipantAdmissionCoord(ctx context.Context, farmID uint64) (func(), error) {
	release, err := r.acquireAuthorityCoordMode(ctx, farmID, false)
	if err != nil {
		return nil, err
	}
	r.authorityCoordMu.Lock()
	coord := r.authorityCoords[farmID]
	if coord == nil {
		r.authorityCoordMu.Unlock()
		release()
		return nil, errors.New("farm actor: participant admission coordination disappeared")
	}
	coord.admissionRefs++
	r.authorityCoordMu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			r.authorityCoordMu.Lock()
			if coord.admissionRefs > 0 {
				coord.admissionRefs--
			}
			r.authorityCoordMu.Unlock()
			release()
		})
	}, nil
}

func (r *Registry) participantAdmissionActive(farmID uint64) bool {
	if r == nil || farmID == 0 {
		return false
	}
	r.authorityCoordMu.Lock()
	coord := r.authorityCoords[farmID]
	active := coord != nil && coord.admissionRefs > 0
	r.authorityCoordMu.Unlock()
	return active
}

func (r *Registry) SetWriterEpochResolver(resolve func(uint64) (uint64, error)) {
	r.writerEpoch = resolve
}

// SetWritableCheck 注入 PASSIVATING 期间写权判定;nil 时默认视为仍可写
// (等价于同 Holder lineage 未 Drain / epoch 仍有效)。单测可强制 false。
func (r *Registry) SetWritableCheck(check func(uint64) bool) {
	r.mu.Lock()
	r.writable = check
	r.mu.Unlock()
}

func (r *Registry) SetResyncNotifier(notify func(uint64, []Presence, string, uint64, uint64)) {
	r.mu.Lock()
	r.resync = notify
	r.mu.Unlock()
}

// SetRouteMetrics attaches shared route metrics for writer_epoch_mismatch_total
// (Batch 2.6). Nil clears. Safe to call before serving traffic.
func (r *Registry) SetRouteMetrics(m *actorrouting.Metrics) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.routeMetrics = m
	r.mu.Unlock()
}

// SetTurnObserver 转发 turn 耗时回调到 runtime executor(观测 turn P99)。
func (r *Registry) SetTurnObserver(observe func(key uint64, duration time.Duration)) {
	if r == nil || r.exec == nil {
		return
	}
	r.exec.SetTurnObserver(observe)
}

// ActivationStats exposes activation.Controller.Stats for RegisterActivationMetrics.
func (r *Registry) ActivationStats() activation.Stats {
	if r == nil || r.activation == nil {
		return activation.Stats{}
	}
	return r.activation.Stats()
}

// CleanupActorScanTotal is exported as subscription_cleanup_actor_scan_total.
// It counts legacy scan invocations rather than actors examined so even an
// empty-registry scan remains observable.
func (r *Registry) CleanupActorScanTotal() uint64 {
	if r == nil {
		return 0
	}
	return r.cleanupActorScans.Load()
}

func NewRegistry(store blobstore.Store, mailboxSize int) *Registry {
	return NewRegistryWithOptions(store, RegistryOptions{MailboxSize: mailboxSize})
}

func NewRegistryWithOptions(store blobstore.Store, opts RegistryOptions) *Registry {
	r := newRegistryShared(opts)
	r.store = store
	return r
}

// NewAuthorityRegistry 以 AuthorityStore 主线构造 Registry(AR1):激活经
// LoadFutureGroup singleflight 合并,LoadAndClaim 建立 WriterClaim 与
// ShardWriteFence;save 经 Binding 落到 Authority Commit;Unknown 经
// FenceAndClaim(C1→C2) 解析。与 blobstore 构造路径互斥(禁止双写)。
func NewAuthorityRegistry(provider *authority.Provider, opts RegistryOptions) *Registry {
	r := newRegistryShared(opts)
	r.authority = provider
	r.modelFieldV2Conversion = opts.ModelFieldV2Conversion
	r.newModelLayout = opts.NewModelLayout
	if r.newModelLayout != authoritystore.ModelLayoutV1 && r.newModelLayout != authoritystore.ModelLayoutV2 {
		r.newModelLayout = authoritystore.ModelLayoutV1
	}
	r.loadGroup = activation.NewLoadFutureGroup(activation.LoadFutureOptions{})
	if provider != nil {
		caps, err := provider.ParticipantPhysicalCapabilities()
		if err == nil {
			r.participantCapacity, err = participantPhysicalCapacityFromStore(caps)
		}
		r.participantCapacityErr = err
		r.participantCapacityKnown = err == nil
		r.writerEpoch = func(farmID uint64) (uint64, error) {
			return provider.WriterEpoch(context.Background(), actorrouting.DomainFarm, farmID)
		}
	}
	return r
}

func newRegistryShared(opts RegistryOptions) *Registry {
	actOpts := opts.Activation
	if actOpts == (activation.Options{}) {
		actOpts = activation.DefaultOptions()
	}
	ctrl := activation.New(actOpts)
	exec := actorruntime.NewOptions(actorruntime.Options{
		UserMailboxSize:        opts.MailboxSize,
		UserMailboxInitialSize: opts.MailboxInitialSize,
		AdmissionGate:          opts.AdmissionGate,
	})
	r := &Registry{
		exec:                       exec,
		admissionGate:              opts.AdmissionGate,
		activation:                 ctrl,
		actors:                     make(map[uint64]*FarmActor),
		unknown:                    make(map[uint64]UnknownContext),
		unknownResolved:            make(map[uint64]authority.Resolution),
		resolving:                  make(map[uint64]bool),
		autoResolve:                true,
		subscriptions:              NewSubscriptionHub(opts.FarmEventLaneSize),
		authorityCoords:            make(map[uint64]*farmAuthorityCoord),
		passivation:                opts.Passivation,
		participantAdmissionReader: opts.ParticipantAdmissionSnapshotReader,
		participantGuardReader:     opts.ParticipantGuardSnapshotReader,
		theftPRF:                   opts.TheftPRF,
		opportunityWakeupEnabled:   opts.OpportunityWakeupEnabled,
		retiredBindings:            make(map[uint64]*authority.Binding),
	}
	exec.SetOnPanic(func(key uint64) {
		r.handlePanic(key)
	})
	if !opts.Passivation.Disable {
		r.sweeper = idle.New(idle.Options{
			IdleTimeout:       opts.Passivation.IdleTimeout,
			BucketWidth:       opts.Passivation.BucketWidth,
			TickInterval:      opts.Passivation.SweepInterval,
			MaxPerTick:        opts.Passivation.MaxPerSweep,
			BlockedRetryDelay: opts.Passivation.BlockedRetryDelay,
			Now:               opts.Passivation.Now,
		}, r.tryPassivateIdle)
	}
	return r
}

// AuthorityMetrics 返回 authority 适配层指标快照(legacy 模式为零值)。
func (r *Registry) AuthorityMetrics() authority.MetricsSnapshot {
	if r == nil || r.authority == nil {
		return authority.MetricsSnapshot{}
	}
	return r.authority.Metrics().Snapshot()
}

// AuthorityEnabled reports whether this Registry is on the writer-fenced
// AuthorityStore path. Legacy registries remain available for compatibility;
// production CARE selects the Incident/AttachedOrder turn only in this mode.
func (r *Registry) AuthorityEnabled() bool {
	return r != nil && r.authority != nil
}

// LoadFutureStats 暴露 singleflight 激活观测(actor_load_singleflight_join_total 等)。
func (r *Registry) LoadFutureStats() activation.LoadFutureStats {
	if r == nil || r.loadGroup == nil {
		return activation.LoadFutureStats{}
	}
	return r.loadGroup.Stats()
}

// AuthorityStats 返回 authority 模式的激活观测(每 Actor 内存指标接缝):
// 当前缓存 Actor 数与其权威记录字节合计。
func (r *Registry) AuthorityStats() (actors int, recordBytesTotal int64) {
	if r == nil || r.authority == nil {
		return 0, 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, actor := range r.actors {
		binding, ok := actor.store.(*authority.Binding)
		if !ok || binding == nil || !binding.Usable() {
			continue
		}
		actors++
		recordBytesTotal += binding.LastRecordBytes()
	}
	return actors, recordBytesTotal
}

// ParticipantPhysicalCapacity returns the immutable profile obtained from the
// concrete AuthorityStore. A zero value means the checked Participant path
// is unavailable; it is never filled from a caller-supplied default.
func (r *Registry) ParticipantPhysicalCapacity() ParticipantPhysicalCapacity {
	if r == nil || !r.participantCapacityKnown {
		return ParticipantPhysicalCapacity{}
	}
	return r.participantCapacity
}

// ParticipantAdmissionError is the construction/preflight gate for the Farm
// Participant. Legacy registries remain usable for the old public command
// path, but cannot expose an atomic receipt admission read.
func (r *Registry) ParticipantAdmissionError() error {
	if r == nil || r.authority == nil {
		return ErrParticipantReceiptAuthorityUnavailable
	}
	if r.participantCapacityErr != nil {
		return r.participantCapacityErr
	}
	if !r.participantCapacityKnown {
		return errors.New("farm actor: authority participant capacity is unknown")
	}
	return nil
}

// StartIdleSweeper 启动后台分桶扫描;幂等。测试可直接调 SweepIdleOnce 而不启动后台。
func (r *Registry) StartIdleSweeper(ctx context.Context) {
	if r == nil || r.sweeper == nil || ctx == nil {
		return
	}
	r.mu.Lock()
	if r.sweepCancel != nil {
		r.mu.Unlock()
		return
	}
	sweepCtx, cancel := context.WithCancel(ctx)
	r.sweepCancel = cancel
	r.mu.Unlock()
	go r.sweeper.Run(sweepCtx)
}

// SweepIdleOnce 同步执行一轮 idle 扫描(测试与运维观测用),返回处理的候选数。
func (r *Registry) SweepIdleOnce(ctx context.Context) int {
	if r == nil || r.sweeper == nil {
		return 0
	}
	return r.sweeper.SweepOnce(ctx)
}

// IdleSweeperStats 暴露 Sweeper 快照给指标注册。
func (r *Registry) IdleSweeperStats() idle.Stats {
	if r == nil || r.sweeper == nil {
		return idle.Stats{}
	}
	return r.sweeper.Stats()
}

// touchActivity 记录 Actor 活动;只更新 Sweeper 桶归属,不创建 Timer。
func (r *Registry) touchActivity(farmID uint64) {
	if r.sweeper != nil && farmID != 0 {
		r.sweeper.Touch(farmID)
	}
}

func (r *Registry) SubscriptionHub() *SubscriptionHub {
	if r == nil {
		return nil
	}
	return r.subscriptions
}

// ValidateFarmHolder checks the current shard assignment without activating an
// Actor. Subscription Hub renewals use this to reject delivery to an old
// Holder after Assignment migration.
func (r *Registry) ValidateFarmHolder(farmID uint64) error {
	if r == nil || farmID == 0 {
		return activation.ErrStaleRoute
	}
	if r.writerEpoch == nil {
		return nil
	}
	_, err := r.writerEpoch(farmID)
	return err
}

// SetAutoResolveUnknown is a deterministic-test hook. Production defaults to
// true so an authoritative read is attempted immediately after the ambiguous
// user turn returns.
func (r *Registry) SetAutoResolveUnknown(enabled bool) {
	r.mu.Lock()
	r.autoResolve = enabled
	r.mu.Unlock()
}

// Activation 暴露控制面(观测/单测)。
func (r *Registry) Activation() *activation.Controller {
	if r == nil {
		return nil
	}
	return r.activation
}

// BeginPassivate 将已 ACTIVE 的 farm 转入 QUIESCING(不接 idle timer;供验收双路径)。
func (r *Registry) BeginPassivate(farmID uint64) error {
	if r == nil || r.activation == nil || farmID == 0 {
		return errors.New("farm actor: registry unavailable")
	}
	if r.participantGuardActive(farmID) {
		return fmt.Errorf("farm actor: passivation blocked by %s", BlockerParticipantUnanchor)
	}
	return r.activation.BeginPassivate(farmID)
}

func (r *Registry) acquireLifecycleCoord(ctx context.Context, farmID uint64) (func(), error) {
	if r == nil || r.authority == nil {
		return func() {}, nil
	}
	return r.acquireAuthorityCoord(ctx, farmID)
}

func (r *Registry) participantGuardActive(farmID uint64) bool {
	r.mu.Lock()
	actor := r.actors[farmID]
	r.mu.Unlock()
	if actor == nil {
		return false
	}
	_, _, _, ok := actor.guardCandidateSnapshot()
	return ok
}

// settlePendingGuardForLifecycle closes a guard before a lifecycle operation
// can discard the Actor. An immutable Participant change is delivered through
// the normal Hub lane; a generic/unknown guard is closed only after the
// idempotent InvalidateFarm + MarkFarmResyncRequired pair succeeds. If neither
// proof is possible, the caller must refuse deletion and leave the Actor
// anchored.
func (r *Registry) settlePendingGuardForLifecycle(ctx context.Context, farmID uint64, actor *FarmActor, reason string) error {
	if actor == nil {
		return nil
	}
	version, state, change, ok := actor.guardCandidateSnapshot()
	if !ok {
		return nil
	}
	if state == guardCommitting {
		return errors.New("farm actor: participant delivery guard is still committing")
	}
	if change != nil && actor.record != nil && actor.record.Core != nil && version == actor.record.Core.FarmVersion {
		_, _, _, err := actor.publishCandidate(nil, r.now())
		if err != nil {
			if _, _, stillPending := actor.guardSnapshot(); stillPending {
				return fmt.Errorf("farm actor: participant guard delivery is unresolved: %w", err)
			}
			return nil
		}
		if _, _, stillPending := actor.guardSnapshot(); stillPending {
			return errors.New("farm actor: participant guard remained after delivery")
		}
		return nil
	}
	if r.subscriptions == nil {
		return errors.New("farm actor: subscription hub unavailable for guard invalidation")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if _, _, err := r.subscriptions.InvalidateFarm(ctx, farmID, version, reason); err != nil {
		return err
	}
	resyncs, err := r.subscriptions.MarkFarmResyncRequired(ctx, farmID, reason, version, actor.activationEpoch)
	if err != nil {
		return err
	}
	r.subscriptions.ObserveLaneResync(len(resyncs))
	actor.guardClear()
	return nil
}

// FinishPassivate 经 SystemQueue 结束钝化并丢弃缓存 actor → ABSENT。
// 走 SubmitSystem，避免 UserMailbox 满时无法完成钝化。
func (r *Registry) FinishPassivate(farmID uint64) error {
	if r == nil || r.activation == nil || r.exec == nil || farmID == 0 {
		return errors.New("farm actor: registry unavailable")
	}
	_, err := r.exec.SubmitSystem(context.Background(), farmID, actorruntime.SystemKindPassivate, func(ctx context.Context) (any, error) {
		release, err := r.acquireLifecycleCoord(ctx, farmID)
		if err != nil {
			return nil, err
		}
		defer release()
		r.mu.Lock()
		actor := r.actors[farmID]
		r.mu.Unlock()
		if err := r.settlePendingGuardForLifecycle(ctx, farmID, actor, "passivation_guard"); err != nil {
			return nil, err
		}
		if r.activation.StateOf(farmID) == activation.Quiescing {
			if err := r.activation.CommitPassivate(farmID); err != nil {
				return nil, err
			}
		}
		if r.activation.StateOf(farmID) != activation.Passivating {
			return nil, fmt.Errorf("farm actor: finish passivate from %s", r.activation.StateOf(farmID))
		}
		r.mu.Lock()
		if err := r.activation.FinishPassivate(farmID); err != nil {
			r.mu.Unlock()
			return nil, err
		}
		if current := r.actors[farmID]; current != nil {
			if binding := authorityBindingForFarmActor(current); binding != nil {
				binding.Retire()
			}
			delete(r.actors, farmID)
		}
		delete(r.unknown, farmID)
		delete(r.unknownResolved, farmID)
		delete(r.retiredBindings, farmID)
		r.mu.Unlock()
		if r.subscriptions != nil {
			_, _ = r.subscriptions.RetireFarmIfEmpty(context.Background(), farmID)
		}
		return nil, nil
	})
	return err
}

// tryPassivateIdle 是 Sweeper 的二次校验与资源释放入口(两阶段 idle passivation)。
//
// 阶段一:校验 activation 状态与 blocker 快照,ACTIVE → QUIESCING;
// 阶段二:经 SystemQueue 二次确认状态后删除 Actor、FinishPassivate → ABSENT、
// 独立退休空 Hub Lane;最后退休 Executor Mailbox/goroutine 并回收 ABSENT entry。
//
// 允许阻塞 passivation 的只有交接文档 §6.2 列出的条件;Presence 与持久化
// RETRY_WAIT Workflow 不是 blocker(阶段 5 已把它们从 Actor 生命周期拆出)。
// 普通 passivation 不清理 Hub Presence,Holder 失权/panic 才走 Drain/Resync 路径。
func (r *Registry) tryPassivateIdle(ctx context.Context, farmID uint64) idle.Result {
	if r == nil || r.exec == nil || r.activation == nil || farmID == 0 {
		return idle.Result{Decision: idle.DecisionRetired}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	switch r.activation.StateOf(farmID) {
	case activation.ResolvingUnknown:
		return idle.Result{Decision: idle.DecisionBusy, Reasons: []string{BlockerResolvingUnknown}}
	case activation.Absent:
		// Actor 已不在(可能上次 passivation 半途崩溃或 panic/失权路径已删):
		// 幂等收尾残留 mailbox 与 ABSENT entry。
		return r.finishIdlePassivation(farmID)
	case activation.Active:
		// 继续两阶段流程。
	default:
		// LOADING / UNKNOWN / DRAINING / FENCED / QUIESCING / PASSIVATING 不归 idle 路径处理。
		return idle.Result{Decision: idle.DecisionNotIdle}
	}

	// blocker 快照:turn_running 覆盖同步 Redis CAS(persistence_in_flight /
	// dirty_unhanded / participant_result_unanchored 当前不可能在 Turn 外存在)。
	snap, hasMailbox := r.exec.MailboxSnapshot(farmID)
	if hasMailbox {
		var reasons []string
		if snap.InFlight {
			reasons = append(reasons, BlockerTurnRunning)
		}
		if snap.UserQueued > 0 {
			reasons = append(reasons, BlockerUserQueueNonempty)
		}
		if snap.SystemQueued > 0 {
			reasons = append(reasons, BlockerSystemQueueNonempty)
		}
		if len(reasons) > 0 {
			return idle.Result{Decision: idle.DecisionBusy, Reasons: reasons}
		}
		if r.idleTimeout() > 0 && r.now().Sub(snap.LastActivity) < r.idleTimeout() {
			// 与候选入桶并发的新活动。
			return idle.Result{Decision: idle.DecisionNotIdle}
		}
	}
	if r.participantAdmissionActive(farmID) {
		return idle.Result{Decision: idle.DecisionBusy, Reasons: []string{BlockerParticipantAdmission}}
	}
	if r.participantGuardActive(farmID) {
		return idle.Result{Decision: idle.DecisionBusy, Reasons: []string{BlockerParticipantUnanchor}}
	}

	// 阶段一:ACTIVE → QUIESCING。新用户命令到达时 AdmitUserCommand 会
	// 取消钝化回 ACTIVE；进入 PASSIVATING 后则不再复活旧 Activation。
	if err := r.activation.BeginPassivate(farmID); err != nil {
		return idle.Result{Decision: idle.DecisionNotIdle}
	}

	// 二次校验:mailbox generation 与队列不变;任何新活动都放弃本次 passivation。
	if hasMailbox {
		current, ok := r.exec.MailboxSnapshot(farmID)
		if !ok || current.Generation != snap.Generation || current.InFlight ||
			current.UserQueued > 0 || current.SystemQueued > 0 {
			_ = r.activation.CancelPassivate(farmID)
			return idle.Result{Decision: idle.DecisionNotIdle}
		}
	}

	// 阶段二:经 SystemQueue 在 Actor 串行域内完成删除,与并发用户命令按
	// 队列顺序裁决:先到的命令若已取消钝化,本任务放弃;本任务先执行完,
	// 后到的命令看到 ABSENT 走重新激活。
	finished, _ := r.exec.SubmitSystem(ctx, farmID, actorruntime.SystemKindPassivate, func(ctx context.Context) (any, error) {
		release, err := r.acquireLifecycleCoord(ctx, farmID)
		if err != nil {
			return false, err
		}
		defer release()
		if r.activation.StateOf(farmID) != activation.Quiescing {
			return false, nil
		}
		r.mu.Lock()
		actor := r.actors[farmID]
		r.mu.Unlock()
		if err := r.settlePendingGuardForLifecycle(ctx, farmID, actor, "passivation_guard"); err != nil {
			return false, err
		}
		if err := r.activation.CommitPassivate(farmID); err != nil {
			return false, nil
		}
		r.mu.Lock()
		if err := r.activation.FinishPassivate(farmID); err != nil {
			r.mu.Unlock()
			return false, nil
		}
		if current := r.actors[farmID]; current != nil {
			if binding := authorityBindingForFarmActor(current); binding != nil {
				binding.Retire()
			}
			delete(r.actors, farmID)
		}
		delete(r.unknown, farmID)
		delete(r.unknownResolved, farmID)
		delete(r.retiredBindings, farmID)
		r.mu.Unlock()
		if r.subscriptions != nil {
			// 普通 passivation 不清 Presence;仅当 Actor 已 ABSENT 且 Presence
			// 已空时 Hub 独立退休空 Lane。
			_, _ = r.subscriptions.RetireFarmIfEmpty(context.Background(), farmID)
		}
		return true, nil
	})
	if done, _ := finished.(bool); !done {
		if r.activation.StateOf(farmID) == activation.Quiescing {
			_ = r.activation.CancelPassivate(farmID)
		}
		return idle.Result{Decision: idle.DecisionNotIdle}
	}
	return r.finishIdlePassivation(farmID)
}

// finishIdlePassivation 在 Actor 已 ABSENT 后退休 Executor Mailbox/goroutine
// 并回收 activation entry。mailbox 退休失败(Busy/Stale)说明新命令已到达并
// 开始重新激活:资源主体已释放,mailbox 留给新生命周期,下轮扫描再处理。
func (r *Registry) finishIdlePassivation(farmID uint64) idle.Result {
	if snap, ok := r.exec.MailboxSnapshot(farmID); ok {
		if !snap.InFlight && snap.UserQueued == 0 && snap.SystemQueued == 0 {
			_ = r.exec.RetireIfIdle(farmID, snap.Generation)
		}
	}
	if r.activation.StateOf(farmID) == activation.Absent {
		r.activation.ForgetIfAbsent(farmID)
	}
	return idle.Result{Decision: idle.DecisionRetired}
}

func (r *Registry) now() time.Time {
	if r.passivation.Now != nil {
		return r.passivation.Now()
	}
	return time.Now()
}

func (r *Registry) idleTimeout() time.Duration {
	if r.passivation.IdleTimeout > 0 {
		return r.passivation.IdleTimeout
	}
	return 5 * time.Minute
}

// NotifyLeaseLost 经 SystemQueue 驱逐失权 FarmActor（合同 SystemKindLeaseLost）。
// UserMailbox 满时仍须可入队；不得在用户 turn 内再 SubmitSystem（会死锁）。
// 用户 turn 内检测到 ErrWriterEpochMismatch 时仍用同步 evictFenced。
func (r *Registry) NotifyLeaseLost(ctx context.Context, farmID uint64) error {
	if r == nil || r.exec == nil || farmID == 0 {
		return errors.New("farm actor: registry unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	_, err := r.exec.SubmitSystem(ctx, farmID, actorruntime.SystemKindLeaseLost, func(ctx context.Context) (any, error) {
		release, err := r.acquireLifecycleCoord(ctx, farmID)
		if err != nil {
			return nil, err
		}
		defer release()
		r.mu.Lock()
		actor := r.actors[farmID]
		tombstone := r.retiredBindings[farmID]
		metrics := r.routeMetrics
		r.mu.Unlock()
		if actor != nil {
			if r.authority == nil {
				if err := r.evictFenced(farmID, actor); err != nil {
					return nil, err
				}
				if metrics != nil {
					metrics.ObserveWriterEpochMismatch()
				}
				return nil, nil
			}
			if _, err := r.retireFarmGeneration(ctx, farmID, actor, "lease_loss_guard"); err != nil {
				return nil, err
			}
			r.mu.Lock()
			tombstone = r.retiredBindings[farmID]
			r.mu.Unlock()
		} else if r.activation != nil {
			r.mu.Lock()
			r.activation.MarkAbsent(farmID)
			r.mu.Unlock()
		}
		// One exact lifecycle settlement attempt is bounded by this system turn.
		// Retry/unavailable retains the tombstone for the facts barrier.
		if err := settleRetiredFarmBinding(ctx, tombstone); err != nil {
			return nil, err
		}
		if metrics != nil {
			metrics.ObserveWriterEpochMismatch()
		}
		return nil, nil
	})
	return err
}

// SubmitSystem 将 task 投到 SystemQueue（不做 AdmitUserCommand / 加载）。
// actor 参数在未激活时可为 nil；调用方须自行判空。
func (r *Registry) SubmitSystem(ctx context.Context, farmID uint64, kind actorruntime.SystemKind, task Task) (any, error) {
	if r == nil || r.exec == nil || farmID == 0 || task == nil {
		return nil, errors.New("farm actor: registry unavailable")
	}
	r.touchActivity(farmID)
	return r.exec.SubmitSystem(ctx, farmID, kind, func(ctx context.Context) (any, error) {
		r.mu.Lock()
		actor := r.actors[farmID]
		r.mu.Unlock()
		return task(ctx, actor)
	})
}

func (r *Registry) Submit(ctx context.Context, farmID uint64, task Task) (any, error) {
	if r == nil || (r.store == nil && r.authority == nil) || r.exec == nil || r.activation == nil || farmID == 0 || task == nil {
		return nil, errors.New("farm actor: registry unavailable")
	}
	r.touchActivity(farmID)
	value, err := r.exec.Submit(ctx, farmID, func(ctx context.Context) (any, error) {
		if r.authority != nil {
			// Binding.Get/CASEpoch mutate the Binding's version, physical usage
			// and pending-Unknown state. Participant receipt reads use the same
			// farm gate and never race those fields with this actor turn. The gate
			// is per farm; its registry index is not held during I/O.
			release, err := r.acquireAuthorityCoord(ctx, farmID)
			if err != nil {
				return nil, err
			}
			defer release()
		}
		if _, err := r.activation.AdmitUserCommand(farmID, r.isWritable(farmID)); err != nil {
			return nil, err
		}
		var actor *FarmActor
		var err error
		if r.authority != nil {
			actor, err = r.actorAuthority(ctx, farmID)
		} else {
			actor, err = r.actor(ctx, farmID)
		}
		if err != nil {
			return nil, err
		}
		value, err := task(ctx, actor)
		if r.authority != nil {
			binding := authorityBindingForFarmActor(actor)
			if binding == nil {
				return value, errors.Join(err, errors.New("farm actor: authority binding unavailable after turn"))
			}
			// Farm callbacks include pure reads, no-change decisions and receipt
			// replays, none of which has a final CAS. Validate after every callback,
			// including error outcomes, so no old-generation fact can escape.
			if generationErr := binding.ValidateCurrentGeneration(ctx); generationErr != nil {
				r.mu.Lock()
				metrics := r.routeMetrics
				r.mu.Unlock()
				if metrics != nil {
					metrics.ObserveWriterEpochMismatch()
				}
				if evictErr := r.evictFencedContext(ctx, farmID, actor); evictErr != nil {
					generationErr = errors.Join(generationErr, evictErr)
				}
				if err != nil {
					return value, errors.Join(generationErr, err)
				}
				return value, generationErr
			}
		}
		if errors.Is(err, blobstore.ErrTimeoutUnknown) {
			unknown, ok := actor.UnknownContext()
			if !ok {
				unknown = UnknownContext{BeforeRecordVersion: actor.recordVersion, ExpectedWriterEpoch: actor.writerEpoch}
			}
			// This transition is deliberately synchronous in the same mailbox
			// turn. Already queued user work will therefore observe
			// RESOLVING_UNKNOWN before it can touch the reverted in-memory record.
			if transitionErr := r.activation.BeginResolveUnknown(farmID); transitionErr != nil {
				return value, errors.Join(err, transitionErr)
			}
			r.mu.Lock()
			r.unknown[farmID] = unknown
			r.mu.Unlock()
		}
		if errors.Is(err, blobstore.ErrWriterEpochMismatch) {
			r.mu.Lock()
			metrics := r.routeMetrics
			r.mu.Unlock()
			if metrics != nil {
				metrics.ObserveWriterEpochMismatch()
			}
			// Already on this key's actor goroutine — sync evict (do not SubmitSystem).
			if evictErr := r.evictFenced(farmID, actor); evictErr != nil {
				err = errors.Join(err, evictErr)
			}
		}
		if errors.Is(err, actorruntime.ErrActorPanic) {
			r.handlePanic(farmID)
		}
		return value, err
	})
	if errors.Is(err, blobstore.ErrTimeoutUnknown) ||
		(errors.Is(err, activation.ErrActorBusy) && r.activation.StateOf(farmID) == activation.ResolvingUnknown) {
		r.scheduleUnknownResolution(farmID)
	}
	return value, err
}

// ReadOutsideMailbox performs the bounded, read-only receipt/admission snapshot
// required by Participant ingress. An activated Actor snapshot is preferred;
// an optional cold-farm adapter is time-bounded. It never calls Binding.Get or
// LoadAndClaim and never establishes/mutates a Writer Binding.
func (r *Registry) ReadOutsideMailbox(ctx context.Context, farmID uint64) (*farmpb.FarmRecord, uint64, error) {
	if r == nil || r.authority == nil || r.exec == nil || farmID == 0 {
		return nil, 0, ErrParticipantReceiptAuthorityUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if r.activation.StateOf(farmID) == activation.ResolvingUnknown {
		return nil, 0, blobstore.ErrTimeoutUnknown
	}
	r.touchActivity(farmID)
	release, err := r.acquireParticipantAdmissionCoord(ctx, farmID)
	if err != nil {
		return nil, 0, err
	}
	defer release()
	if r.activation.StateOf(farmID) == activation.ResolvingUnknown {
		return nil, 0, blobstore.ErrTimeoutUnknown
	}
	r.mu.Lock()
	actor := r.actors[farmID]
	reader := r.participantAdmissionReader
	r.mu.Unlock()
	if actor != nil {
		binding, ok := actor.store.(*authority.Binding)
		if !ok || binding == nil || binding.Version() != actor.recordVersion {
			return nil, 0, ErrParticipantReceiptAuthorityUnavailable
		}
		validate := func() error {
			if err := binding.ValidateCurrentGeneration(ctx); err != nil {
				if evictErr := r.evictFencedContext(ctx, farmID, actor); evictErr != nil {
					return errors.Join(err, evictErr)
				}
				return err
			}
			return nil
		}
		if err := validate(); err != nil {
			return nil, 0, err
		}
		if binding.HasPendingUnknown() {
			return nil, 0, blobstore.ErrTimeoutUnknown
		}
		if actor.record == nil || actor.record.Core == nil || actor.recordVersion == 0 {
			return nil, 0, ErrParticipantReceiptAuthorityUnavailable
		}
		// This is a snapshot of the already activated Actor. No Binding.Get,
		// LoadAndClaim, or mutable Binding field is touched outside its turn.
		record, ok := proto.Clone(actor.record).(*farmpb.FarmRecord)
		if !ok || record == nil {
			return nil, 0, ErrParticipantReceiptAuthorityUnavailable
		}
		normalizeRecord(record)
		if record.FarmId != farmID {
			return nil, 0, errors.New("farm actor: corrupt farm_id")
		}
		if err := validate(); err != nil {
			return nil, 0, err
		}
		return record, actor.recordVersion, nil
	}
	if reader == nil {
		// Cold-farm production composition has no frozen read-only port yet;
		// fail closed rather than activating/claiming from an ingress read.
		return nil, 0, ErrParticipantReceiptAuthorityUnavailable
	}
	// The optional composition seam is explicitly bounded. A slow reader is
	// an unavailable admission snapshot, never a reason to wait on Authority.
	readerCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	record, version, err := reader.ReadParticipantAdmissionSnapshot(readerCtx, farmID)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return nil, 0, ErrParticipantReceiptAuthorityUnavailable
		}
		return nil, 0, err
	}
	if record == nil || version == 0 {
		return nil, 0, ErrParticipantReceiptAuthorityUnavailable
	}
	cloned, ok := proto.Clone(record).(*farmpb.FarmRecord)
	if !ok || cloned == nil {
		return nil, 0, ErrParticipantReceiptAuthorityUnavailable
	}
	normalizeRecord(cloned)
	if cloned.FarmId != farmID {
		return nil, 0, errors.New("farm actor: corrupt farm_id")
	}
	return cloned, version, nil
}

func (r *Registry) participantCapacityGuard(actor *FarmActor) (ParticipantCapacityGuard, error) {
	if r == nil || actor == nil || r.authority == nil || !r.participantCapacityKnown {
		return ParticipantCapacityGuard{}, ErrParticipantCapacityUnavailable
	}
	binding, ok := actor.store.(*authority.Binding)
	if !ok || binding == nil {
		return ParticipantCapacityGuard{}, ErrParticipantCapacityUnavailable
	}
	return ParticipantCapacityGuard{
		binding: binding,
		version: actor.recordVersion,
		limits:  r.participantCapacity,
	}, nil
}

// CheckParticipantAuthorityCapacity repeats the physical admission check after
// all dependency proofs and immediately before mailbox execution. A version
// change is reported separately so the caller may perform one fresh receipt
// read; stale or missing physical usage always fails closed.
func (r *Registry) CheckParticipantAuthorityCapacity(ctx context.Context, farmID, expectedVersion uint64, candidateModelBytes int) error {
	if r == nil || r.authority == nil || farmID == 0 || expectedVersion == 0 {
		return ErrParticipantCapacityUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if r.activation.StateOf(farmID) == activation.ResolvingUnknown {
		return blobstore.ErrTimeoutUnknown
	}
	release, err := r.acquireParticipantAdmissionCoord(ctx, farmID)
	if err != nil {
		return err
	}
	defer release()
	if r.activation.StateOf(farmID) == activation.ResolvingUnknown {
		return blobstore.ErrTimeoutUnknown
	}
	r.mu.Lock()
	actor := r.actors[farmID]
	r.mu.Unlock()
	if actor == nil {
		return ErrParticipantCapacityUnavailable
	}
	guard, err := r.participantCapacityGuard(actor)
	if err != nil {
		return err
	}
	validate := func() error {
		if err := guard.binding.ValidateCurrentGeneration(ctx); err != nil {
			if evictErr := r.evictFencedContext(ctx, farmID, actor); evictErr != nil {
				return errors.Join(err, evictErr)
			}
			return err
		}
		return nil
	}
	if err := validate(); err != nil {
		return err
	}
	if guard.version != expectedVersion {
		return ErrParticipantCapacityAdmissionStale
	}
	checkErr := guard.Check(candidateModelBytes)
	if generationErr := validate(); generationErr != nil {
		if checkErr != nil {
			return errors.Join(generationErr, checkErr)
		}
		return generationErr
	}
	return checkErr
}

// SubmitParticipant executes one Farm Participant task in the existing
// FarmActor mailbox. All Work/Farm proof is expected to have completed before
// this call; the callback performs only local serial adjudication plus the
// one Authority Commit represented by the actor's existing CASEpoch seam.
func (r *Registry) SubmitParticipant(ctx context.Context, farmID uint64, task ParticipantTask) (any, error) {
	if r == nil || task == nil {
		return nil, ErrParticipantReceiptAuthorityUnavailable
	}
	value, err := r.Submit(ctx, farmID, func(ctx context.Context, actor *FarmActor) (any, error) {
		guard, err := r.participantCapacityGuard(actor)
		if err != nil {
			return nil, err
		}
		return task(ctx, actor, guard)
	})
	if r.activation.StateOf(farmID) == activation.ResolvingUnknown &&
		(errors.Is(err, activation.ErrActorBusy) || errors.Is(err, blobstore.ErrTimeoutUnknown)) {
		r.scheduleUnknownResolution(farmID)
		return value, blobstore.ErrTimeoutUnknown
	}
	return value, err
}

type UnknownResolution int

const (
	UnknownStillUnknown UnknownResolution = iota
	UnknownCommitted
	UnknownNotCommitted
)

func (r *Registry) UnknownContext(farmID uint64) (UnknownContext, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	value, ok := r.unknown[farmID]
	return value, ok
}

func (r *Registry) finishUnknownResolution(ctx context.Context, farmID uint64, actor *FarmActor, unknown UnknownContext, committed bool) (UnknownResolution, error) {
	if committed {
		if !actor.hasReceiptForOperation(unknown.OperationID) {
			return UnknownStillUnknown, errors.New("farm actor: committed resolution without business receipt")
		}
		// A committed timeout is not resolved by reloading and forgetting the
		// guard. First consume the immutable candidate through the normal Hub
		// lane, or atomically fence it with Invalidate+Resync.
		if _, _, _, ok := actor.guardCandidateSnapshot(); !ok {
			return UnknownStillUnknown, errors.New("farm actor: committed resolution lost its delivery guard")
		}
		if err := r.settlePendingGuardForLifecycle(ctx, farmID, actor, "unknown_committed_guard"); err != nil {
			return UnknownStillUnknown, err
		}
	} else {
		// The authority read proved NOT_COMMITTED, so no FarmChange may be
		// published. The pre-CAS guard can now be discarded.
		actor.guardClear()
	}
	actor.unknownContext = nil
	r.mu.Lock()
	delete(r.unknown, farmID)
	delete(r.unknownResolved, farmID)
	r.mu.Unlock()
	if err := r.activation.ResolveUnknownToActive(farmID); err != nil {
		return UnknownStillUnknown, err
	}
	if committed {
		return UnknownCommitted, nil
	}
	return UnknownNotCommitted, nil
}

// ResolveUnknown performs an authoritative read in the actor SystemQueue. A
// receipt proves COMMITTED; its absence after a successful read proves this
// operation was not committed in the currently authoritative record.
func (r *Registry) ResolveUnknown(ctx context.Context, farmID uint64) (UnknownResolution, error) {
	if r == nil || r.exec == nil || r.activation == nil || farmID == 0 {
		return UnknownStillUnknown, errors.New("farm actor: registry unavailable")
	}
	if r.authority != nil {
		return r.resolveUnknownAuthority(ctx, farmID)
	}
	value, err := r.exec.SubmitSystem(ctx, farmID, actorruntime.SystemKindUnknownResolved, func(ctx context.Context) (any, error) {
		if r.activation.StateOf(farmID) != activation.ResolvingUnknown {
			return UnknownStillUnknown, activation.ErrInvalidTransition
		}
		r.mu.Lock()
		actor := r.actors[farmID]
		unknown, ok := r.unknown[farmID]
		r.mu.Unlock()
		if actor == nil || !ok || unknown.OperationID == "" {
			return UnknownStillUnknown, errors.New("farm actor: missing unknown context")
		}
		if !r.isWritable(farmID) {
			// Loss of the Holder while UNKNOWN is still resolving is a fenced
			// eviction, not permission to discard the Actor. Reuse the same
			// Guard settlement as NotifyLeaseLost/ResolutionFenced; on a failed
			// handoff evictFenced intentionally leaves the Actor anchored.
			r.evictFenced(farmID, actor)
			return UnknownStillUnknown, activation.ErrStaleRoute
		}
		if err := actor.reloadAuthoritative(ctx); err != nil {
			return UnknownStillUnknown, err
		}
		return r.finishUnknownResolution(ctx, farmID, actor, unknown, actor.hasReceiptForOperation(unknown.OperationID))
	})
	if err != nil {
		return UnknownStillUnknown, err
	}
	return value.(UnknownResolution), nil
}

// resolveUnknownAuthority 是 AuthorityStore 主线的 Unknown 解析(合同 30 §7):
// FenceAndClaim(C1→C2) 在与 Authority Record 同一线性化存储域建立切点,
// 旧 Claim 的迟到写自此返回 WRITER_CLAIM_MISMATCH;结论由持久
// ResolutionReceipt 固定。Committed 要求记录内业务 Receipt 交叉一致,
// 否则 Fail Closed;Retry/Reconcile 保持 RESOLVING_UNKNOWN 由调度器有界重试。
func (r *Registry) resolveUnknownAuthority(ctx context.Context, farmID uint64) (UnknownResolution, error) {
	value, err := r.exec.SubmitSystem(ctx, farmID, actorruntime.SystemKindUnknownResolved, func(ctx context.Context) (any, error) {
		release, err := r.acquireAuthorityCoord(ctx, farmID)
		if err != nil {
			return UnknownStillUnknown, err
		}
		defer release()
		if r.activation.StateOf(farmID) != activation.ResolvingUnknown {
			return UnknownStillUnknown, activation.ErrInvalidTransition
		}
		r.mu.Lock()
		actor := r.actors[farmID]
		unknown, ok := r.unknown[farmID]
		resolved, alreadyResolved := r.unknownResolved[farmID]
		r.mu.Unlock()
		if actor == nil || !ok || unknown.OperationID == "" {
			return UnknownStillUnknown, errors.New("farm actor: missing unknown context")
		}
		binding, ok := actor.store.(*authority.Binding)
		if !ok || binding == nil {
			return UnknownStillUnknown, errors.New("farm actor: authority binding unavailable")
		}
		if !r.isWritable(farmID) {
			// Keep UNKNOWN and its PostCommitDeliveryGuard until the fenced
			// eviction has either delivered the immutable change or installed
			// the idempotent invalid/resync fence.
			r.evictFenced(farmID, actor)
			return UnknownStillUnknown, activation.ErrStaleRoute
		}
		resolution := authority.ResolutionRetry
		if alreadyResolved {
			resolution = resolved
		} else {
			resolution, err = binding.ResolveUnknown(ctx)
			if err != nil {
				return UnknownStillUnknown, err
			}
			switch resolution {
			case authority.ResolutionCommitted, authority.ResolutionNotCommitted:
				r.mu.Lock()
				r.unknownResolved[farmID] = resolution
				r.mu.Unlock()
			}
		}
		switch resolution {
		case authority.ResolutionCommitted, authority.ResolutionNotCommitted:
			// ResolveUnknown clears Binding.pending. Retain the result in
			// unknownResolved until reload and post-commit delivery both finish,
			// so a resolver retry never calls ResolveUnknown on a cleared Binding.
			if err := actor.reloadAuthoritative(ctx); err != nil {
				return UnknownStillUnknown, err
			}
		case authority.ResolutionFenced, authority.ResolutionReconcile:
			// RECONCILE means the exact resolver/claim generation was replaced;
			// retrying it as UNKNOWN can never recover. Treat it as terminal
			// generation loss, retain only the bounded tombstone, and fail stale.
			r.mu.Lock()
			metrics := r.routeMetrics
			r.mu.Unlock()
			if metrics != nil {
				metrics.ObserveWriterEpochMismatch()
			}
			if evictErr := r.evictFenced(farmID, actor); evictErr != nil {
				return UnknownStillUnknown, errors.Join(activation.ErrStaleRoute, evictErr)
			}
			return UnknownStillUnknown, activation.ErrStaleRoute
		default:
			// ResolutionRetry remains isolated for a bounded scheduler retry with
			// the same resolver identity.
			return UnknownStillUnknown, blobstore.ErrTimeoutUnknown
		}
		return r.finishUnknownResolution(ctx, farmID, actor, unknown, resolution == authority.ResolutionCommitted)
	})
	if err != nil {
		return UnknownStillUnknown, err
	}
	return value.(UnknownResolution), nil
}

func (r *Registry) scheduleUnknownResolution(farmID uint64) {
	r.mu.Lock()
	if !r.autoResolve || r.resolving[farmID] {
		r.mu.Unlock()
		return
	}
	r.resolving[farmID] = true
	r.mu.Unlock()
	go func() {
		defer func() {
			r.mu.Lock()
			delete(r.resolving, farmID)
			r.mu.Unlock()
		}()
		delay := 10 * time.Millisecond
		for attempt := 0; attempt < 8; attempt++ {
			if r.activation.StateOf(farmID) != activation.ResolvingUnknown {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, err := r.ResolveUnknown(ctx, farmID)
			cancel()
			if err == nil || r.activation.StateOf(farmID) != activation.ResolvingUnknown {
				return
			}
			timer := time.NewTimer(delay)
			<-timer.C
			if delay < 640*time.Millisecond {
				delay *= 2
			}
		}
	}()
}

func (r *Registry) isWritable(farmID uint64) bool {
	r.mu.Lock()
	check := r.writable
	r.mu.Unlock()
	if check != nil {
		return check(farmID)
	}
	return true
}

func authorityBindingForFarmActor(actor *FarmActor) *authority.Binding {
	if actor == nil {
		return nil
	}
	binding, _ := actor.store.(*authority.Binding)
	return binding
}

// retireFarmGeneration removes one exact active actor while retaining its
// authority Binding as a same-generation tombstone. Guard settlement happens
// first; map replacement and activation-state publication are then serialized
// under r.mu so a fresh actor cannot be marked absent by stale cleanup.
func (r *Registry) retireFarmGeneration(ctx context.Context, farmID uint64, actor *FarmActor, reason string) ([]Presence, error) {
	if actor == nil {
		return nil, nil
	}
	if err := r.settlePendingGuardForLifecycle(ctx, farmID, actor, reason); err != nil {
		return nil, err
	}
	binding := authorityBindingForFarmActor(actor)
	if binding == nil {
		// Legacy blobstore actors have no generation tombstone capability; retain
		// their established exact removal behavior after guard settlement.
		removed := r.removeActor(farmID, actor)
		r.mu.Lock()
		notify := r.resync
		r.mu.Unlock()
		if notify != nil && len(removed) > 0 {
			notify(farmID, removed, "holder_rebuilt", actor.Snapshot(time.Now()).FarmVersion, actor.activationEpoch)
		}
		return removed, nil
	}
	binding.Retire()

	r.mu.Lock()
	if r.actors[farmID] != actor {
		r.mu.Unlock()
		return nil, ErrFactsActorChanged
	}
	delete(r.actors, farmID)
	delete(r.unknown, farmID)
	delete(r.unknownResolved, farmID)
	r.retiredBindings[farmID] = binding
	if r.activation != nil {
		r.activation.MarkAbsent(farmID)
	}
	r.mu.Unlock()

	var removed []Presence
	if r.subscriptions != nil {
		removed, _ = r.subscriptions.DrainFarm(context.Background(), farmID)
		_, _ = r.subscriptions.RetireFarmIfEmpty(context.Background(), farmID)
	}
	r.mu.Lock()
	notify := r.resync
	r.mu.Unlock()
	if notify != nil && len(removed) > 0 {
		notify(farmID, removed, "holder_rebuilt", actor.Snapshot(time.Now()).FarmVersion, actor.activationEpoch)
	}
	return removed, nil
}

func (r *Registry) evictFencedContext(ctx context.Context, farmID uint64, actor *FarmActor) error {
	_, err := r.retireFarmGeneration(ctx, farmID, actor, "fenced_guard")
	if err != nil {
		// A fenced Actor remains anchored when post-commit handoff cannot be
		// proven. The next lifecycle retry uses the same idempotent guard fence.
		return err
	}
	return nil
}

func (r *Registry) evictFenced(farmID uint64, actor *FarmActor) error {
	return r.evictFencedContext(context.Background(), farmID, actor)
}

func (r *Registry) handlePanic(farmID uint64) {
	r.mu.Lock()
	actor := r.actors[farmID]
	if binding := authorityBindingForFarmActor(actor); binding != nil {
		binding.Retire()
		if binding.HasPendingUnknown() {
			r.retiredBindings[farmID] = binding
		}
	}
	delete(r.actors, farmID)
	delete(r.unknown, farmID)
	delete(r.unknownResolved, farmID)
	r.mu.Unlock()
	// 27 §5.17:Supervisor 对 COMMITTING/UNKNOWN/COMMITTED Guard 幂等安装
	// invalid_from_version<=V——不猜测提交成败,但绝不留第三种终态。
	if actor != nil && r.subscriptions != nil {
		if version, _, ok := actor.guardSnapshot(); ok {
			_, _, _ = r.subscriptions.InvalidateFarm(context.Background(), farmID, version, "actor_panic_guard")
		}
	}
	// Publish ABSENT before potentially waiting on Hub cleanup. Submit returns
	// ErrActorPanic as soon as the runtime reports the failed turn.
	if r.activation != nil {
		r.activation.OnPanic(farmID)
	}
	if r.subscriptions != nil {
		_, _ = r.subscriptions.DrainFarm(context.Background(), farmID)
		_, _ = r.subscriptions.RetireFarmIfEmpty(context.Background(), farmID)
	}
}

func (r *Registry) removeActor(farmID uint64, actor *FarmActor) []Presence {
	r.mu.Lock()
	var removed []Presence
	didRemove := false
	if r.actors[farmID] == actor {
		if binding := authorityBindingForFarmActor(actor); binding != nil {
			binding.Retire()
		}
		delete(r.actors, farmID)
		delete(r.unknown, farmID)
		delete(r.unknownResolved, farmID)
		delete(r.retiredBindings, farmID)
		// Publish ABSENT in the same critical section as exact actor deletion.
		if r.activation != nil {
			r.activation.MarkAbsent(farmID)
		}
		didRemove = true
	}
	r.mu.Unlock()
	if didRemove && r.subscriptions != nil {
		removed, _ = r.subscriptions.DrainFarm(context.Background(), farmID)
		_, _ = r.subscriptions.RetireFarmIfEmpty(context.Background(), farmID)
	}
	return removed
}

func (r *Registry) actor(ctx context.Context, farmID uint64) (*FarmActor, error) {
	r.mu.Lock()
	if existing := r.actors[farmID]; existing != nil {
		r.mu.Unlock()
		return existing, nil
	}
	r.mu.Unlock()

	if err := r.activation.BeginLoad(farmID); err != nil {
		return nil, err
	}

	actor := &FarmActor{
		farmID:                   farmID,
		store:                    r.store,
		activationEpoch:          r.nextEpoch.Add(1),
		presence:                 make(map[string]*Presence),
		presenceByPlayer:         make(map[uint64]string),
		presenceByAttempt:        make(map[string]string),
		joinIdempotency:          make(map[string]joinIdempotencyEntry),
		maxSubscriptions:         defaultMaxSubscriptions,
		maxBufferItems:           64,
		maxBufferBytes:           defaultMaxJoinBufferBytes,
		maxJoinDuration:          defaultMaxJoinDuration,
		subscriptionHub:          r.subscriptions,
		theftPRF:                 r.theftPRF,
		opportunityWakeupEnabled: r.opportunityWakeupEnabled,
	}
	if r.subscriptions != nil {
		r.subscriptions.Attach(farmID, actor)
	}
	var err error
	if r.writerEpoch != nil {
		actor.writerEpoch, err = r.writerEpoch(farmID)
		if err != nil {
			_ = r.activation.FailLoad(farmID)
			return nil, err
		}
	}
	if err := actor.loadOrCreate(ctx); err != nil {
		_ = r.activation.FailLoad(farmID)
		return nil, err
	}
	if err := r.installParticipantGuardSnapshot(ctx, actor); err != nil {
		_ = r.activation.FailLoad(farmID)
		return nil, err
	}
	r.mu.Lock()
	if existing := r.actors[farmID]; existing != nil {
		r.mu.Unlock()
		_ = r.activation.CompleteLoad(farmID)
		return existing, nil
	}
	r.actors[farmID] = actor
	r.mu.Unlock()
	if err := r.activation.CompleteLoad(farmID); err != nil {
		return nil, err
	}
	r.touchActivity(farmID)
	return actor, nil
}

// authorityActorForCurrentGeneration validates active actors and terminal
// tombstones against the complete local fence before any cached Farm state is
// reused. A proved generation change settles retained UNKNOWN evidence,
// removes only the exact old tombstone, and permits one fresh activation.
func (r *Registry) authorityActorForCurrentGeneration(ctx context.Context, farmID uint64) (*FarmActor, bool, error) {
	r.mu.Lock()
	actor := r.actors[farmID]
	tombstone := r.retiredBindings[farmID]
	r.mu.Unlock()
	if actor != nil {
		binding := authorityBindingForFarmActor(actor)
		if binding == nil {
			return nil, false, errors.New("farm actor: active authority binding unavailable")
		}
		reusable, err := binding.ReusableForCurrentGeneration(ctx)
		if err != nil {
			if errors.Is(err, authority.ErrBindingRetired) {
				_ = r.evictFenced(farmID, actor)
			}
			return nil, false, err
		}
		if reusable {
			return actor, true, nil
		}
		if _, err := r.retireFarmGeneration(ctx, farmID, actor, "generation_replaced_guard"); err != nil {
			return nil, false, err
		}
		r.mu.Lock()
		tombstone = r.retiredBindings[farmID]
		r.mu.Unlock()
	}
	if tombstone == nil {
		return nil, false, nil
	}
	reusable, err := tombstone.ReusableForCurrentGeneration(ctx)
	if err != nil {
		return nil, false, err
	}
	if reusable {
		return nil, false, authority.ErrBindingRetired
	}
	if tombstone.HasPendingUnknown() {
		if err := settleRetiredFarmBinding(ctx, tombstone); err != nil {
			return nil, false, err
		}
	}
	r.mu.Lock()
	if r.retiredBindings[farmID] == tombstone {
		delete(r.retiredBindings, farmID)
	}
	r.mu.Unlock()
	return nil, false, nil
}

func settleRetiredFarmBinding(ctx context.Context, binding *authority.Binding) error {
	if binding == nil || !binding.HasPendingUnknown() {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	settleCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	resolution, err := binding.ResolveUnknownForRetirement(settleCtx)
	if err != nil {
		return err
	}
	switch resolution {
	case authority.ResolutionCommitted, authority.ResolutionNotCommitted,
		authority.ResolutionFenced, authority.ResolutionReconcile:
		return nil
	default:
		return blobstore.ErrTimeoutUnknown
	}
}

// actorAuthority 是 AuthorityStore 主线的激活路径(合同 30 §5):
// LoadFutureGroup 合并同 Actor 并发激活并共享一份 LoadIdentity
// (activation_generation + claim_operation_id + new_writer_claim_id);
// LoadAndClaim 原子完成"校验 Shard 写权 → 初始化或读取 ActorRecord →
// 换上新 WriterClaim";TIMEOUT_UNKNOWN 进入 CLAIM_UNKNOWN 并以原身份幂等
// 重入(不生成第三个 Claim);WriterEpoch 换代进入 FENCED。
func (r *Registry) actorAuthority(ctx context.Context, farmID uint64) (*FarmActor, error) {
	if existing, ok, err := r.authorityActorForCurrentGeneration(ctx, farmID); err != nil {
		return nil, err
	} else if ok {
		return existing, nil
	}

	handle, owner, err := r.loadGroup.BeginOrJoin(farmID, func() (activation.LoadIdentity, error) {
		claimOpID, claimID := r.authority.NewClaimIdentity(farmID)
		return activation.LoadIdentity{
			ActivationGeneration: r.nextEpoch.Add(1),
			ClaimOperationID:     claimOpID,
			NewWriterClaimID:     claimID,
		}, nil
	})
	if err != nil {
		return nil, err
	}
	if !owner {
		if err := handle.Wait(ctx); err != nil {
			return nil, err
		}
		r.mu.Lock()
		actor := r.actors[farmID]
		r.mu.Unlock()
		if actor == nil {
			return nil, actorruntime.ErrActorBusy
		}
		return actor, nil
	}

	actor, loadErr := r.loadAndClaimActor(ctx, farmID, handle.Identity())
	if completeErr := handle.Complete(loadErr); completeErr != nil {
		return nil, completeErr
	}
	if loadErr != nil {
		return nil, loadErr
	}
	return actor, nil
}

func (r *Registry) loadAndClaimActor(ctx context.Context, farmID uint64, identity activation.LoadIdentity) (*FarmActor, error) {
	if err := r.activation.BeginLoad(farmID); err != nil {
		return nil, err
	}
	// Production selects compact model V2 for new actors. Legacy/test callers
	// may retain V1 explicitly; existing V1 rows remain migration-compatible.
	initialLayout := r.newModelLayout
	initial, _, _, err := buildFarmAuthorityCandidate(initialLayout, InitialRecord(farmID))
	if err != nil {
		_ = r.activation.FailLoad(farmID)
		return nil, err
	}

	binding, status, err := r.authority.ClaimWithModelLayout(ctx, actorrouting.DomainFarm, farmID,
		identity.ClaimOperationID, identity.NewWriterClaimID, initial, initialLayout)
	recovery := false
	if status == authority.ClaimUnknown {
		// CLAIM_UNKNOWN:不安装半初始化 Actor;恢复优先使用原 claim 身份
		// 幂等重入,有界尝试后仍未知则回 ABSENT 并进入失败退避。
		if markErr := r.activation.MarkClaimUnknown(farmID); markErr != nil {
			return nil, markErr
		}
		for attempt := 0; attempt < 3 && status == authority.ClaimUnknown; attempt++ {
			binding, status, err = r.authority.ClaimWithModelLayout(ctx, actorrouting.DomainFarm, farmID,
				identity.ClaimOperationID, identity.NewWriterClaimID, initial, initialLayout)
		}
		switch status {
		case authority.ClaimAcquired:
			// 注意:CLAIM_UNKNOWN → ACTIVE 的转移推迟到 Actor 安全安装之后;
			// 此前任何读/解码/完整性失败都必须能退回 ABSENT,不能留下
			// ACTIVE-without-actor(审阅 P1-1)。
			recovery = true
		case authority.ClaimFenced:
			_ = r.activation.FenceClaimUnknown(farmID)
			return nil, activation.ErrStaleRoute
		case authority.ClaimReconcile:
			_ = r.activation.MarkLoadReconcileRequired(farmID)
			if err == nil {
				err = authority.ErrClaimReconcile
			}
			return nil, err
		case authority.ClaimUnavailable:
			_ = r.activation.ResolveClaimUnknownToAbsent(farmID)
			return nil, blobstore.ErrUnavailable
		default:
			_ = r.activation.ResolveClaimUnknownToAbsent(farmID)
			return nil, blobstore.ErrTimeoutUnknown
		}
	} else {
		switch status {
		case authority.ClaimAcquired:
			// 继续安装。
		case authority.ClaimFenced:
			// Shard/WriterEpoch 已换代:本 Holder 不安装 Actor。
			_ = r.activation.MarkClaimUnknown(farmID)
			_ = r.activation.FenceClaimUnknown(farmID)
			return nil, activation.ErrStaleRoute
		case authority.ClaimUnavailable:
			_ = r.activation.FailLoad(farmID)
			return nil, blobstore.ErrUnavailable
		default:
			_ = r.activation.MarkLoadReconcileRequired(farmID)
			if err == nil {
				err = authority.ErrClaimReconcile
			}
			return nil, err
		}
	}
	// fail 统一处理"已 Claim 但尚未安装 Actor"阶段的一切失败:释放加载槽
	// 并回到可重试的 ABSENT(带失败退避),绝不留下 ACTIVE-without-actor。
	fail := func(err error) (*FarmActor, error) {
		if binding != nil {
			binding.Retire()
		}
		if recovery {
			_ = r.activation.ResolveClaimUnknownToAbsent(farmID)
		} else {
			_ = r.activation.FailLoad(farmID)
		}
		return nil, err
	}
	if err != nil {
		return fail(err)
	}

	snapshot, claimed, err := binding.TakeClaimedModelSnapshot(farmID)
	if err != nil {
		return fail(err)
	}
	if !claimed {
		snapshot, err = binding.GetModelSnapshot(ctx, farmID)
		if err != nil {
			return fail(err)
		}
	}
	rec, modelState, err := decodeFarmAuthoritySnapshot(snapshot, farmID)
	if err != nil {
		return fail(err)
	}
	normalized := normalizeRecord(rec)
	if snapshot.ModelLayout == authoritystore.ModelLayoutV1 {
		eligible, policyErr := r.modelFieldV2Conversion.Eligible(actorrouting.DomainFarm, farmID)
		if policyErr != nil {
			return fail(policyErr)
		}
		if eligible {
			candidate, _, _, buildErr := buildFarmAuthorityCandidate(authoritystore.ModelLayoutV2, rec)
			if buildErr != nil {
				return fail(buildErr)
			}
			if migrateErr := migrateFarmBindingV1ToV2(ctx, binding, snapshot.RecordVersion, candidate); migrateErr != nil {
				return fail(migrateErr)
			}
			snapshot = binding.CurrentModelSnapshot()
			rec, modelState, err = decodeFarmAuthoritySnapshot(snapshot, farmID)
			if err != nil {
				return fail(err)
			}
			normalized = false
		}
	}
	if normalized {
		normalizedState, _, _, buildErr := buildFarmAuthorityCandidate(binding.ModelLayout(), rec)
		if buildErr != nil {
			return fail(buildErr)
		}
		// Normalization and any named projections still linearize through one
		// writer-fenced commit in whichever truth layout was loaded.
		if err := binding.CASEpochModelStateWithOutcomeAndOrders(ctx, farmID, binding.Version(), binding.WriterEpoch(),
			normalizedState, authoritystore.StoredOperationOutcome{Status: authoritystore.TerminalSucceeded}, nil); err != nil {
			return fail(err)
		}
		_, modelState, err = decodeFarmAuthoritySnapshot(binding.CurrentModelSnapshot(), farmID)
		if err != nil {
			return fail(err)
		}
	}

	actor := &FarmActor{
		farmID:                   farmID,
		store:                    binding,
		record:                   rec,
		recordVersion:            binding.Version(),
		activationEpoch:          identity.ActivationGeneration,
		writerEpoch:              binding.WriterEpoch(),
		presence:                 make(map[string]*Presence),
		presenceByPlayer:         make(map[uint64]string),
		presenceByAttempt:        make(map[string]string),
		joinIdempotency:          make(map[string]joinIdempotencyEntry),
		maxSubscriptions:         defaultMaxSubscriptions,
		maxBufferItems:           64,
		maxBufferBytes:           defaultMaxJoinBufferBytes,
		maxJoinDuration:          defaultMaxJoinDuration,
		subscriptionHub:          r.subscriptions,
		theftPRF:                 r.theftPRF,
		opportunityWakeupEnabled: r.opportunityWakeupEnabled,
	}
	if err := installFarmOpportunityModels(actor, modelState); err != nil {
		return fail(err)
	}
	if err := actor.resetParticipantCache(); err != nil {
		return fail(err)
	}
	if err := r.installParticipantGuardSnapshot(ctx, actor); err != nil {
		return fail(err)
	}
	if r.subscriptions != nil {
		r.subscriptions.Attach(farmID, actor)
	}
	r.mu.Lock()
	if existing := r.actors[farmID]; existing != nil {
		r.mu.Unlock()
		binding.Retire()
		if recovery {
			if err := r.activation.ResolveClaimUnknownToActive(farmID); err != nil {
				return nil, err
			}
			return existing, nil
		}
		_ = r.activation.CompleteLoad(farmID)
		return existing, nil
	}
	r.actors[farmID] = actor
	r.mu.Unlock()
	// CLAIM_UNKNOWN → ACTIVE 只在 Actor 已安全安装后发生(审阅 P1-1)。
	if recovery {
		if err := r.activation.ResolveClaimUnknownToActive(farmID); err != nil {
			r.removeActor(farmID, actor)
			return nil, err
		}
		r.touchActivity(farmID)
		return actor, nil
	}
	if err := r.activation.CompleteLoad(farmID); err != nil {
		r.removeActor(farmID, actor)
		return nil, err
	}
	r.touchActivity(farmID)
	return actor, nil
}

func (r *Registry) installParticipantGuardSnapshot(ctx context.Context, actor *FarmActor) error {
	if r == nil || r.participantGuardReader == nil || actor == nil {
		return nil
	}
	snapshot, err := r.participantGuardReader.ReadParticipantGuardSnapshot(ctx, actor.farmID)
	if err != nil {
		return fmt.Errorf("farm actor: load GuardSync snapshot for farm %d: %w", actor.farmID, err)
	}
	if err := actor.SetParticipantGuardSnapshot(snapshot); err != nil {
		return fmt.Errorf("farm actor: install GuardSync snapshot for farm %d: %w", actor.farmID, err)
	}
	return nil
}

func decodeFarmAuthoritySnapshot(snapshot authoritystore.AuthoritySnapshot, farmID uint64) (*farmpb.FarmRecord, authoritystore.ModelState, error) {
	switch snapshot.ModelLayout {
	case authoritystore.ModelLayoutV1:
		for _, name := range []string{"activity_summary", "farm_meta", "guard", "plots"} {
			if _, mixed := snapshot.ModelFields[name]; mixed {
				return nil, nil, errors.New("farm actor: mixed-invalid v1/v2 model fields")
			}
		}
		data, ok := snapshot.ModelFields[authority.RecordModelField]
		if !ok {
			return nil, nil, blobstore.ErrNotFound
		}
		var record farmpb.FarmRecord
		if err := proto.Unmarshal(data, &record); err != nil {
			return nil, nil, fmt.Errorf("farm actor: decode v1 record: %w", err)
		}
		if record.GetFarmId() != farmID {
			return nil, nil, errors.New("farm actor: corrupt farm_id")
		}
		return &record, cloneFarmModelState(snapshot.ModelFields), nil
	case authoritystore.ModelLayoutV2:
		record, header, facts, err := farmactormodelv2.Decode(snapshot.ModelFields)
		if err != nil {
			return nil, nil, fmt.Errorf("farm actor: decode v2 fields: %w", err)
		}
		if record.GetFarmId() != farmID {
			return nil, nil, errors.New("farm actor: corrupt farm_id")
		}
		// Actor opportunity caches consume the established domain codecs. This
		// adapter is in-memory only and is never persisted as a second truth.
		recordBytes, err := marshalRecord(record)
		if err != nil {
			return nil, nil, err
		}
		return record, authoritystore.ModelState{
			authority.RecordModelField:      recordBytes,
			FarmOpportunityHeaderModelField: append([]byte(nil), header...),
			FarmOpportunityFactsModelField:  append([]byte(nil), facts...),
		}, nil
	default:
		return nil, nil, errors.New("farm actor: unknown model layout")
	}
}

func cloneFarmModelState(in authoritystore.ModelState) authoritystore.ModelState {
	out := make(authoritystore.ModelState, len(in))
	for name, value := range in {
		out[name] = append([]byte(nil), value...)
	}
	return out
}

func migrateFarmBindingV1ToV2(ctx context.Context, binding *authority.Binding, version uint64, candidate authoritystore.ModelState) error {
	for attempt := 0; attempt < 3; attempt++ {
		err := binding.MigrateV1ToV2(ctx, binding.Address().ActorID, version, candidate)
		if err == nil {
			return nil
		}
		if !errors.Is(err, blobstore.ErrTimeoutUnknown) {
			return err
		}
		resolution, resolveErr := binding.ResolveUnknown(ctx)
		if resolveErr != nil {
			return resolveErr
		}
		switch resolution {
		case authority.ResolutionCommitted:
			return nil
		case authority.ResolutionNotCommitted:
			continue // same immutable candidate bytes and stable receipt identity
		case authority.ResolutionFenced, authority.ResolutionReconcile:
			return blobstore.ErrWriterEpochMismatch
		default:
			return blobstore.ErrTimeoutUnknown
		}
	}
	return blobstore.ErrTimeoutUnknown
}

func (r *Registry) CleanupConnection(ctx context.Context, connectionID string) error {
	if r == nil || connectionID == "" {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.cleanupActorScans.Add(1)
	r.mu.Lock()
	ids := make([]uint64, 0, len(r.actors))
	for id := range r.actors {
		ids = append(ids, id)
	}
	r.mu.Unlock()
	for _, id := range ids {
		// SystemQueue: must progress when UserMailbox is saturated.
		_, _ = r.exec.SubmitSystem(ctx, id, actorruntime.SystemKindStop, func(context.Context) (any, error) {
			r.mu.Lock()
			actor := r.actors[id]
			r.mu.Unlock()
			if actor != nil {
				actor.LeaveConnection(connectionID)
			}
			return nil, nil
		})
	}
	return nil
}

// CancelJoin delivers an O(1) SystemQueue cleanup to the exact FarmActor.
// It does not scan all active farms and does not activate an absent Actor.
func (r *Registry) CancelJoin(ctx context.Context, farmID uint64, req CancelJoinRequest) (bool, error) {
	if r == nil || r.exec == nil || farmID == 0 || req.OperationID == "" {
		return false, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	value, err := r.exec.SubmitSystem(ctx, farmID, actorruntime.SystemKindConnectionCleanup, func(context.Context) (any, error) {
		r.mu.Lock()
		actor := r.actors[farmID]
		r.mu.Unlock()
		if actor == nil && r.subscriptions != nil {
			return r.subscriptions.CancelJoin(ctx, farmID, req)
		}
		if actor == nil {
			return false, nil
		}
		return actor.CancelJoin(req), nil
	})
	if err != nil {
		return false, err
	}
	canceled, _ := value.(bool)
	r.mu.Lock()
	actorAbsent := r.actors[farmID] == nil
	r.mu.Unlock()
	if canceled && actorAbsent && r.subscriptions != nil {
		_, _ = r.subscriptions.RetireFarmIfEmpty(ctx, farmID)
	}
	return canceled, nil
}

// CancelJoinAttempt orders an unknown-subscription cancellation in the same
// FarmSubscriptionHub lane as CreateOrReuseJoining. It never activates the
// FarmActor and never scans other farms.
func (r *Registry) CancelJoinAttempt(ctx context.Context, farmID uint64,
	req CancelJoinAttemptRequest) (CancelJoinAttemptResult, error) {
	if r == nil || r.subscriptions == nil || farmID == 0 ||
		req.JoinAttemptID == "" || req.OperationID == "" {
		return CancelJoinAttemptResult{Status: CancelAttemptStaleFence}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	result, err := r.subscriptions.CancelJoinAttempt(ctx, farmID, req)
	if err != nil {
		return CancelJoinAttemptResult{}, err
	}
	if result.Status == CancelAttemptCancelled {
		_, _ = r.subscriptions.RetireFarmIfEmpty(ctx, farmID)
	}
	return result, nil
}

// CleanupSubscription conditionally removes one exact Hub Presence in O(1).
// The Hub state is authoritative for realtime subscriptions, so cleanup does
// not activate or scan FarmActors even when the owner actor is passivated.
func (r *Registry) CleanupSubscription(ctx context.Context, farmID uint64,
	req LeaveRequest) (bool, error) {
	if r == nil || r.subscriptions == nil || farmID == 0 ||
		req.SubscriptionID == "" || req.JoinAttemptID == "" {
		return false, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	removed, err := r.subscriptions.CleanupPresence(ctx, farmID, req)
	if err != nil {
		return false, err
	}
	if removed {
		_, _ = r.subscriptions.RetireFarmIfEmpty(ctx, farmID)
	}
	return removed, nil
}

// MarkSubscriptionResyncRequired terminally fences one exact Hub Presence
// without activating the FarmActor. It is used when an ordinary change cannot
// enter the downlink lane, before the reserved terminal control is emitted.
func (r *Registry) MarkSubscriptionResyncRequired(ctx context.Context, farmID uint64,
	req LeaveRequest, reason string, latestKnownVersion, activationEpoch uint64) (*ResyncNotice, bool, error) {
	if r == nil || r.subscriptions == nil {
		return nil, false, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return r.subscriptions.MarkSubscriptionResyncRequired(ctx, farmID, req, reason,
		latestKnownVersion, activationEpoch)
}

// CleanupFarmSubscription removes one exact Presence in O(1) actor-routing
// work. It never activates an absent FarmActor and is safe to retry.
func (r *Registry) CleanupFarmSubscription(ctx context.Context, farmID uint64, req LeaveRequest) (bool, error) {
	if r == nil || r.exec == nil || farmID == 0 || req.SubscriptionID == "" || req.PresenceEpoch == 0 {
		return false, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	value, err := r.exec.SubmitSystem(ctx, farmID, actorruntime.SystemKindConnectionCleanup, func(context.Context) (any, error) {
		r.mu.Lock()
		actor := r.actors[farmID]
		r.mu.Unlock()
		if actor == nil && r.subscriptions != nil {
			return r.subscriptions.CleanupPresence(ctx, farmID, req)
		}
		if actor == nil {
			return false, nil
		}
		return actor.LeaveFenced(req), nil
	})
	if err != nil {
		return false, err
	}
	removed, _ := value.(bool)
	r.mu.Lock()
	actorAbsent := r.actors[farmID] == nil
	r.mu.Unlock()
	if removed && actorAbsent && r.subscriptions != nil {
		_, _ = r.subscriptions.RetireFarmIfEmpty(ctx, farmID)
	}
	return removed, nil
}

func (r *Registry) CleanupConnectionFence(ctx context.Context, route session.ActiveRoute) error {
	if r == nil || route.PlayerID == 0 || route.ConnectionID == "" {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.cleanupActorScans.Add(1)
	r.mu.Lock()
	ids := make([]uint64, 0, len(r.actors))
	for id := range r.actors {
		ids = append(ids, id)
	}
	r.mu.Unlock()
	for _, id := range ids {
		// SystemQueue: fence/resync must not wait behind a full UserMailbox.
		_, _ = r.exec.SubmitSystem(ctx, id, actorruntime.SystemKindStop, func(context.Context) (any, error) {
			r.mu.Lock()
			actor := r.actors[id]
			r.mu.Unlock()
			if actor != nil {
				removed := actor.LeaveConnectionFence(route)
				r.mu.Lock()
				notify := r.resync
				r.mu.Unlock()
				if notify != nil && len(removed) > 0 {
					notify(id, removed, "connection_fence_changed", actor.Snapshot(time.Now()).FarmVersion, actor.activationEpoch)
				}
			}
			return nil, nil
		})
	}
	return nil
}

func (r *Registry) Stop() {
	if r != nil {
		r.mu.Lock()
		cancel := r.sweepCancel
		r.sweepCancel = nil
		r.mu.Unlock()
		if cancel != nil {
			cancel()
		}
	}
	if r != nil && r.exec != nil {
		r.exec.Stop()
	}
	if r != nil && r.subscriptions != nil {
		r.subscriptions.Stop()
	}
}

func (r *Registry) RuntimeStats() actorruntime.Stats {
	if r == nil || r.exec == nil {
		return actorruntime.Stats{}
	}
	return r.exec.Stats()
}

func loadFarmModelState(ctx context.Context, store blobstore.Store, farmID uint64) (authoritystore.ModelState, uint64, error) {
	if modelStore, ok := store.(farmAuthorityModelStore); ok {
		state, version, err := modelStore.GetModelState(ctx, farmID)
		if err != nil {
			return nil, 0, err
		}
		if _, ok := state[authority.RecordModelField]; !ok {
			return state, version, blobstore.ErrNotFound
		}
		return state, version, nil
	}
	data, version, err := store.Get(ctx, farmID)
	if err != nil {
		return nil, version, err
	}
	return authoritystore.ModelState{authority.RecordModelField: data}, version, nil
}

func (a *FarmActor) loadOrCreate(ctx context.Context) error {
	modelState, version, err := loadFarmModelState(ctx, a.store, a.farmID)
	if errors.Is(err, blobstore.ErrNotFound) {
		rec := InitialRecord(a.farmID)
		initialState, _, _, marshalErr := buildFarmAuthorityModelState(rec)
		if marshalErr != nil {
			return marshalErr
		}
		data := initialState[authority.RecordModelField]
		if insertErr := blobstore.InsertAtEpoch(ctx, a.store, a.farmID, a.writerEpoch, data); insertErr != nil {
			if !errors.Is(insertErr, blobstore.ErrExists) {
				return insertErr
			}
			modelState, version, err = loadFarmModelState(ctx, a.store, a.farmID)
		} else {
			a.record, a.recordVersion = rec, 1
			a.opportunityModelsAvailable = false
			return a.resetParticipantCache()
		}
	}
	if err != nil {
		return err
	}
	if err := blobstore.FenceAtEpoch(ctx, a.store, a.farmID, a.writerEpoch); err != nil {
		return err
	}
	data := modelState[authority.RecordModelField]
	var rec farmpb.FarmRecord
	if err := proto.Unmarshal(data, &rec); err != nil {
		return fmt.Errorf("farm actor: decode record: %w", err)
	}
	if rec.FarmId != a.farmID {
		return errors.New("farm actor: corrupt farm_id")
	}
	normalized := normalizeRecord(&rec)
	a.record, a.recordVersion = &rec, version
	if normalized {
		if err := a.persistNormalizedRecord(ctx); err != nil {
			return err
		}
	} else if err := installFarmOpportunityModels(a, modelState); err != nil {
		return err
	}
	return a.resetParticipantCache()
}

func (a *FarmActor) reloadAuthoritative(ctx context.Context) error {
	if snapshotStore, ok := a.store.(interface {
		GetModelSnapshot(context.Context, uint64) (authoritystore.AuthoritySnapshot, error)
	}); ok {
		snapshot, err := snapshotStore.GetModelSnapshot(ctx, a.farmID)
		if err != nil {
			return err
		}
		record, installState, err := decodeFarmAuthoritySnapshot(snapshot, a.farmID)
		if err != nil {
			return err
		}
		normalized := normalizeRecord(record)
		a.record, a.recordVersion = record, snapshot.RecordVersion
		if normalized {
			if err := a.persistNormalizedRecord(ctx); err != nil {
				return err
			}
		} else if err := installFarmOpportunityModels(a, installState); err != nil {
			return err
		}
		return a.resetParticipantCache()
	}
	modelState, version, err := loadFarmModelState(ctx, a.store, a.farmID)
	if err != nil {
		return err
	}
	data := modelState[authority.RecordModelField]
	var rec farmpb.FarmRecord
	if err := proto.Unmarshal(data, &rec); err != nil {
		return fmt.Errorf("farm actor: decode record: %w", err)
	}
	if rec.FarmId != a.farmID {
		return errors.New("farm actor: corrupt farm_id")
	}
	normalized := normalizeRecord(&rec)
	a.record, a.recordVersion = &rec, version
	if normalized {
		if err := a.persistNormalizedRecord(ctx); err != nil {
			return err
		}
	} else if err := installFarmOpportunityModels(a, modelState); err != nil {
		return err
	}
	return a.resetParticipantCache()
}

func InitialRecord(farmID uint64) *farmpb.FarmRecord {
	plots := make([]*farmpb.Plot, 0, DefaultPlotCount)
	for index := 0; index < DefaultPlotCount; index++ {
		lockState := farmpb.PlotLockState_PLOT_LOCK_STATE_LOCKED
		if index < 5 {
			lockState = farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED
		}
		plots = append(plots, &farmpb.Plot{Index: int32(index), PlotVersion: 1, LockState: lockState,
			UnlockRuleVersion: 1})
	}
	return &farmpb.FarmRecord{FarmId: farmID, Core: &farmpb.FarmCore{FarmId: farmID, Plots: plots}}
}

func normalizeRecord(rec *farmpb.FarmRecord) bool {
	if rec == nil {
		return false
	}
	changed := false
	if rec.Core == nil {
		rec.Core = &farmpb.FarmCore{FarmId: rec.FarmId}
		changed = true
	}
	if rec.Core.FarmId == 0 {
		rec.Core.FarmId = rec.FarmId
		changed = true
	}
	// Migration is deterministic and intentionally does not consult a client,
	// a process-local unlock map, or a current player balance. Durable unlock
	// provenance wins; old UNSPECIFIED records use the frozen 1..5 genesis
	// boundary and receive the complete 24-slot projection.
	byIndex := make(map[int32]*farmpb.Plot, len(rec.Core.Plots))
	for _, plot := range rec.Core.Plots {
		if plot == nil || plot.Index < 0 || plot.Index >= DefaultPlotCount {
			continue
		}
		if plot.PlotVersion == 0 {
			plot.PlotVersion = 1
			changed = true
		}
		if plot.LockState == farmpb.PlotLockState_PLOT_LOCK_STATE_UNSPECIFIED {
			// ActiveCropCycleID and a post-genesis PlotVersion are durable
			// evidence that an old record had already used this plot. They
			// must win over the new lock default or a migration would relock
			// a previously purchased/used plot after restart.
			if plot.UnlockWorkflowId != "" || plot.UnlockedAtMs != 0 || plot.ActiveCropCycleId != "" || plot.PlotVersion > 1 || plot.Index < 5 {
				plot.LockState = farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED
			} else {
				plot.LockState = farmpb.PlotLockState_PLOT_LOCK_STATE_LOCKED
			}
			changed = true
		}
		if plot.UnlockRuleVersion == 0 {
			plot.UnlockRuleVersion = 1
			changed = true
		}
		byIndex[plot.Index] = plot
	}
	plots := make([]*farmpb.Plot, 0, DefaultPlotCount)
	for index := int32(0); index < DefaultPlotCount; index++ {
		plot := byIndex[index]
		if plot == nil {
			lockState := farmpb.PlotLockState_PLOT_LOCK_STATE_LOCKED
			if index < 5 {
				lockState = farmpb.PlotLockState_PLOT_LOCK_STATE_UNLOCKED
			}
			plot = &farmpb.Plot{Index: index, PlotVersion: 1, LockState: lockState, UnlockRuleVersion: 1}
			changed = true
		}
		plots = append(plots, plot)
	}
	if len(rec.Core.Plots) != len(plots) {
		changed = true
	}
	rec.Core.Plots = plots
	return changed
}

func (a *FarmActor) persistNormalizedRecord(ctx context.Context) error {
	if a == nil || a.store == nil || a.record == nil || a.recordVersion == 0 {
		return errors.New("farm actor: normalized record persistence context is incomplete")
	}
	modelState, header, facts, err := buildFarmAuthorityCandidate(authorityStoreModelLayout(a.store), a.record)
	if err != nil {
		return err
	}
	if store, ok := a.store.(farmAuthorityModelStore); ok {
		err = store.CASEpochModelStateWithOutcomeAndOrders(ctx, a.farmID, a.recordVersion, a.writerEpoch,
			modelState, authoritystore.StoredOperationOutcome{Status: authoritystore.TerminalSucceeded}, nil)
	} else {
		err = blobstore.CASAtEpoch(ctx, a.store, a.farmID, a.recordVersion, a.writerEpoch,
			modelState[authority.RecordModelField])
	}
	if err != nil {
		return err
	}
	a.recordVersion++
	a.opportunityHeaderV2 = header
	a.opportunityFactsV1 = facts
	a.opportunityModelsAvailable = true
	return nil
}

func marshalRecord(rec *farmpb.FarmRecord) ([]byte, error) {
	return proto.MarshalOptions{Deterministic: true}.Marshal(rec)
}
