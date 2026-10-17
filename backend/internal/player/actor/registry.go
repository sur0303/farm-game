// Package actor implements the player_id keyed PlayerActor. Each turn is
// serialized locally. The default commits a complete PlayerRecord with CAS
// before success; the explicit dirty_best_effort experiment keeps the latest
// encoded model snapshot in the Binding plus an immutable decoded resident
// record, clones that record per turn, and coalesces later writer-fenced
// flushes.
//
// durable_cas 模式每 turn 校验并围绕最近已提交快照工作；dirty_best_effort
// 激活时只从 Redis LoadAndClaim 一次，后续 turn 从已解码的进程内记录
// 复制独立工作副本。LoadAndClaim 仍走 activation.Controller 的并发加载硬门，避免冷
// 登录风暴占满 Redis/CPU 把 Holder heartbeat 饿死。SystemQueue 入口
// (SubmitSystem / NotifyLeaseLost) 在 UserMailbox 满时仍可调度——与 farm
// registry 对称。
//
// 本进程无 Presence/CleanupConnection:断线清理在 farm-svr;Player 侧系统路径
// 面向 LeaseLost / 未来 PersistenceCompleted / Drain。
package actor

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	economypb "farmgame/api/gen/economy"
	playerpb "farmgame/api/gen/player"
	"farmgame/internal/actormodel/fieldv2"
	"farmgame/internal/actorrouting"
	"farmgame/internal/logic/actor/activation"
	"farmgame/internal/logic/actor/authority"
	"farmgame/internal/logic/actor/dirtywriteback"
	"farmgame/internal/logic/actor/idle"
	actorruntime "farmgame/internal/logic/actor/runtime"
	playeractormodelv2 "farmgame/internal/player/actormodelv2"
	"farmgame/internal/publicprofile"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/blobstore"

	"google.golang.org/protobuf/proto"
)

const (
	DefaultInitialCoin = int64(100)
	maxCASRetries      = 5
)

type Turn func(*playerpb.PlayerRecord) (value any, changed bool, err error)

// VersionedTurn is the minimal metadata seam needed by internal Participant
// adapters. currentVersion is the RecordVersion loaded in the same mailbox
// turn; when changed is true, the candidate is committed as currentVersion+1.
// The callback must construct any typed result that carries a record version
// from this value rather than from EconomyVersion or a later read.
type VersionedTurn func(rec *playerpb.PlayerRecord, currentVersion uint64) (value any, changed bool, err error)

// ParticipantVersionedTurn is the Authority-only Participant seam. The guard
// is built from the same freshly loaded Binding snapshot as rec/version and
// must be checked before any business decision mutates rec.
type ParticipantVersionedTurn func(rec *playerpb.PlayerRecord, currentVersion uint64, guard ParticipantCapacityGuard) (value any, changed bool, err error)

// ParticipantVersionedTurnWithOutcome is the extended Participant seam for a
// typed step whose terminal StoredOperationOutcome must carry the original
// result alongside the PlayerRecord mutation. The ordinary seam remains the
// compatibility path for existing Plant steps and uses an empty successful
// outcome.
type ParticipantVersionedTurnWithOutcome func(rec *playerpb.PlayerRecord, currentVersion uint64, guard ParticipantCapacityGuard) (value any, changed bool, outcome authoritystore.StoredOperationOutcome, err error)

// ParticipantVersionedTurnWithOrders is the SourceOrder source-actor seam:
// the same Lua commit writes the player fact and AttachedOrders.
type ParticipantVersionedTurnWithOrders func(rec *playerpb.PlayerRecord, currentVersion uint64, guard ParticipantCapacityGuard) (value any, changed bool, outcome authoritystore.StoredOperationOutcome, orders []authoritystore.AttachedOrder, err error)

// DurableCommandTurn is the PlayerActor command seam used by mail claim,
// pet mutations and welcome-mail bootstrap. It shares one Authority Commit
// for PlayerRecord + terminal outcome + recovery orders, without the
// Participant capacity guard. SourceOrder plant/unlock keep using
// ParticipantVersionedTurnWithOrders.
type DurableCommandTurn func(rec *playerpb.PlayerRecord, currentVersion uint64) (value any, commit bool, outcome authoritystore.StoredOperationOutcome, orders []authoritystore.AttachedOrder, err error)

// SubmitResult returns the business value and the RecordVersion observed from
// the same successful authority commit. For changed=false it is the version
// read by this turn; callers that need the original version of a replayed
// receipt must use the version embedded in that receipt's typed result.
type SubmitResult struct {
	Value                  any
	CommittedRecordVersion uint64
	// ProducedOrders returns immutable copies of the SourceOrders emitted by
	// this Actor turn. Durable mode persists the same orders atomically and may
	// ignore this field. dirty_best_effort uses it to perform one bounded direct
	// Participant dispatch after the mailbox turn has completed.
	ProducedOrders []authoritystore.AttachedOrder
}

func cloneSubmitOrders(orders []authoritystore.AttachedOrder) []authoritystore.AttachedOrder {
	if len(orders) == 0 {
		return nil
	}
	cloned := make([]authoritystore.AttachedOrder, len(orders))
	copy(cloned, orders)
	for i := range cloned {
		cloned[i].Payload = append([]byte(nil), orders[i].Payload...)
	}
	return cloned
}

// SystemTask 是不持有 PlayerRecord 的系统 turn(失权屏障等)。
type SystemTask func(ctx context.Context) (any, error)

// IdleOptions 控制 Player Key 的 Executor Mailbox 空闲回收。
// Player 每 Turn 重新加载权威记录,无缓存 actor,因此只需要回收
// Mailbox/goroutine,不需要 Farm 式 Activation FSM(阶段 6 §6.3.5)。
type IdleOptions struct {
	// Disable 关闭自动回收。
	Disable bool
	// IdleTimeout 是 Mailbox 最后一次活动到成为回收候选的时间;默认 5 分钟。
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

// ParticipantPhysicalCapacity is the conservative physical budget used by
// the Player Participant admission path. The authority adapters enforce
// their own exact limits atomically; these limits let admission reject a
// known-too-large candidate before business code enters the mailbox. A
// Legacy blobstore construction uses conservative defaults. Authority
// construction derives every value from the concrete Provider/Store used by
// Commit; callers cannot supply an independent or wider profile.
type ParticipantPhysicalCapacity struct {
	MaxReceiptItems       int
	MaxModelBytes         int
	MaxReceiptBytes       int
	MaxSingleReceiptBytes int
	MaxCommitBytes        int
}

// ErrParticipantReceiptAuthorityUnavailable means that a Registry cannot
// provide the atomic, epoch-aware receipt admission read required by the
// production Participant path. In particular, a legacy blobstore's separate
// fence-then-Get sequence is never exposed as a safe Participant read.
var ErrParticipantReceiptAuthorityUnavailable = errors.New("player actor: atomic participant receipt authority unavailable")

// ErrParticipantPhysicalCapacity is intentionally normalized by the
// Participant transport to the existing stable receipt-capacity error. It
// covers exhausted, missing and stale physical usage because none is safe to
// admit before business mutation.
var ErrParticipantPhysicalCapacity = errors.New("player actor: participant physical capacity unavailable or exhausted")

// ErrParticipantCapacityAdmissionStale means another valid local commit
// advanced the Binding after the mailbox-free record estimate. The server may
// refresh that estimate once; it is distinct from a stale/untrustworthy Store
// PhysicalUsage snapshot, which always fails closed.
var ErrParticipantCapacityAdmissionStale = errors.New("player actor: participant capacity admission record changed")

func defaultParticipantPhysicalCapacity() ParticipantPhysicalCapacity {
	limits := authoritystore.DefaultLimits()
	return ParticipantPhysicalCapacity{
		MaxReceiptItems:       1024,
		MaxModelBytes:         limits.MaxModelBytes,
		MaxReceiptBytes:       limits.MaxReceiptBytes,
		MaxSingleReceiptBytes: limits.MaxSingleReceiptBytes,
		MaxCommitBytes:        limits.MaxCommitBytes,
	}
}

func completeParticipantPhysicalCapacity(capacity ParticipantPhysicalCapacity) bool {
	return capacity.MaxReceiptItems > 0 && capacity.MaxModelBytes > 0 && capacity.MaxReceiptBytes > 0 &&
		capacity.MaxSingleReceiptBytes > 0 && capacity.MaxCommitBytes > 0
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
		return ParticipantPhysicalCapacity{}, errors.New("player actor: authority participant capacity profile is incomplete")
	}
	return capacity, nil
}

// ParticipantCapacityGuard combines a version-bound aggregate outer Store
// usage snapshot with the concrete Store's exact next-commit projection.
// Its fields are private so callers cannot manufacture a trusted view.
type ParticipantCapacityGuard struct {
	binding    *authority.Binding
	version    uint64
	limits     ParticipantPhysicalCapacity
	bestEffort bool
}

func (g ParticipantCapacityGuard) Check(candidateModelBytes int) error {
	return g.CheckWithOutcome(candidateModelBytes, authoritystore.StoredOperationOutcome{Status: authoritystore.TerminalSucceeded})
}

// PhysicalProjectionRequired is false only for dirty best-effort authority
// mode. The caller must still enforce bounded receipt retention and the
// Registry still validates record version plus writer generation; this only
// avoids building detached full-record size projections that Check discards.
func (g ParticipantCapacityGuard) PhysicalProjectionRequired() bool {
	return !g.bestEffort
}

// CheckWithOutcome is the exact physical admission check for a candidate
// whose outer Authority receipt carries a typed StoredOperationOutcome
// payload. The payload is part of the same commit charge and cannot be
// omitted from a Harvest admission calculation.
func (g ParticipantCapacityGuard) CheckWithOutcome(candidateModelBytes int, outcome authoritystore.StoredOperationOutcome) error {
	if g.bestEffort {
		return nil
	}
	if g.binding == nil || !completeParticipantPhysicalCapacity(g.limits) {
		return fmt.Errorf("%w: participant capacity guard is unavailable", ErrParticipantPhysicalCapacity)
	}
	if g.binding.Version() != g.version {
		return ErrParticipantCapacityAdmissionStale
	}
	usage := g.binding.PhysicalUsage()
	if err := usage.Validate(g.version); err != nil {
		return fmt.Errorf("%w: %v", ErrParticipantPhysicalCapacity, err)
	}
	// Callers estimate the historical model:record carrier. Every Player CAS
	// now also writes model:public_profile, so reserve its enforced maximum to
	// ensure admission is conservative rather than failing late at Commit.
	projectedModelBytes := candidateModelBytes + publicprofile.MaxModelFieldBytes
	projection, err := g.binding.ProjectCASEpochPhysicalCommitWithOutcome(g.version, projectedModelBytes, outcome)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrParticipantPhysicalCapacity, err)
	}
	if projection.ModelBytes != projectedModelBytes || projection.ModelBytes > g.limits.MaxModelBytes ||
		projection.SingleReceiptBytes > g.limits.MaxSingleReceiptBytes || projection.CommitBytes > g.limits.MaxCommitBytes ||
		usage.ReceiptItems+projection.ReceiptItems > g.limits.MaxReceiptItems ||
		usage.ReceiptBytes+projection.ReceiptBytes > g.limits.MaxReceiptBytes {
		return ErrParticipantPhysicalCapacity
	}
	return nil
}

type RegistryOptions struct {
	// MailboxSize is the logical maximum queued user turns per Player actor.
	// MailboxInitialSize controls only the small per-key preallocated channel;
	// bursts use a lazy bounded tail up to MailboxSize.
	MailboxSize        int
	MailboxInitialSize int
	Idle               IdleOptions
	AdmissionGate      actorruntime.AdmissionGate
	// Activation bounds concurrent LoadAndClaim. Zero MaxConcurrentLoads
	// leaves the gate disabled so unit tests keep an unbounded path.
	Activation activation.Options
	// ModelFieldV2Conversion is default-off. It is consulted only for a v1
	// authority snapshot; an already-v2 row remains v2 regardless of policy.
	ModelFieldV2Conversion fieldv2.ConversionPolicy
	// NewModelLayout controls newly created authority rows. Zero preserves the
	// legacy/test default V1; production explicitly selects V2.
	NewModelLayout authoritystore.ModelLayoutVersion
	// DirtyWriteback is used only when the Authority Provider has explicitly
	// enabled soft saves. Zero values use production experiment defaults.
	DirtyWriteback    dirtywriteback.Options
	DirtyDrainTimeout time.Duration
}

type Registry struct {
	store                    blobstore.Store
	exec                     *actorruntime.Executor
	admissionGate            actorruntime.AdmissionGate
	writerEpoch              func(uint64) (uint64, error)
	participantCapacity      ParticipantPhysicalCapacity
	participantCapacityKnown bool
	participantCapacityErr   error
	// leaseLostTotal counts NotifyLeaseLost system turns (observability / tests).
	leaseLostTotal                atomic.Uint64
	admissionSnapshotReuseTotal   atomic.Uint64
	admissionSnapshotRefreshTotal atomic.Uint64

	// authority 非空时 Registry 运行在 AuthorityStore 主线模式:每个 playerID
	// 经 LoadAndClaim 建立 Binding(WriterClaim + ShardWriteFence),写路径走
	// Authority Commit,TIMEOUT_UNKNOWN 经 FenceAndClaim 解析。store 必须为 nil。
	authority                  *authority.Provider
	admissionSnapshotReuseSafe bool
	modelFieldV2Conversion     fieldv2.ConversionPolicy
	newModelLayout             authoritystore.ModelLayoutVersion
	activation                 *activation.Controller
	// bindings 是 playerID → 已声明写绑定的缓存;同一 player 的创建与使用由
	// 该 key 的 Mailbox 串行域保护,bindingsMu 只保护 map 本身。
	bindings   map[uint64]*authority.Binding
	bindingsMu sync.Mutex
	// residentRecords is the decoded hot-state companion to Binding.models in
	// dirty_best_effort mode. Entries are immutable after Store: every caller
	// receives an independent proto clone, and a successful turn publishes a
	// new immutable entry. sync.Map avoids adding one global mutex acquisition
	// to every resident Player turn.
	residentRecords      sync.Map // playerID -> *residentPlayerRecord
	residentRecordHits   atomic.Uint64
	residentRecordMisses atomic.Uint64
	// holderShardFor remembers the first frozen player-domain mapper installed
	// by a facts barrier so the compatibility Registry.RearmShard entry point
	// cannot reopen an Authority shard without the same complete enumeration.
	holderControlMu sync.RWMutex
	holderShardFor  actorruntime.ShardForActor
	// authorityCoordMu protects only the short-lived per-player coordination
	// index; it is never held across AuthorityStore I/O. Each gate is keyed by
	// player_id, so one player's LoadAndClaim cannot stall unrelated logins.
	authorityCoordMu sync.Mutex
	authorityCoords  map[uint64]*playerAuthorityCoord

	sweeper     *idle.Sweeper
	sweepCancel context.CancelFunc
	sweepMu     sync.Mutex

	// lifecycleMu closes mailbox-free authority admission before Stop waits
	// for the operation. The gate admission and this reference are installed
	// under one lock, so Stop cannot race between the stop check and Admit.
	lifecycleMu       sync.Mutex
	outsideAdmissions sync.WaitGroup
	stopped           bool

	writeback         *dirtywriteback.Scheduler
	writebackInitErr  error
	dirtyDrainTimeout time.Duration
}

type residentPlayerRecord struct {
	binding *authority.Binding
	version uint64
	record  *playerpb.PlayerRecord
}

type ResidentRecordStats struct {
	Hits, Misses uint64
}

func (r *Registry) SetWriterEpochResolver(resolve func(uint64) (uint64, error)) {
	r.writerEpoch = resolve
}

// ActivationStats exposes activation.Controller.Stats for
// RegisterActivationMetrics(actor_kind="player"). A registry without an
// activation gate (unit tests) reports an all-zero snapshot.
func (r *Registry) ActivationStats() activation.Stats {
	if r == nil || r.activation == nil {
		return activation.Stats{}
	}
	return r.activation.Stats()
}

func NewRegistry(store blobstore.Store, mailboxSize int) *Registry {
	return NewRegistryWithOptions(store, RegistryOptions{MailboxSize: mailboxSize})
}

func NewRegistryWithOptions(store blobstore.Store, opts RegistryOptions) *Registry {
	capacity := ParticipantPhysicalCapacity{}
	capacityKnown := false
	var capacityErr error
	if store != nil {
		capacity = defaultParticipantPhysicalCapacity()
		capacityKnown = true
	}
	r := &Registry{
		store: store,
		exec: actorruntime.NewOptions(actorruntime.Options{
			UserMailboxSize:        opts.MailboxSize,
			UserMailboxInitialSize: opts.MailboxInitialSize,
			AdmissionGate:          opts.AdmissionGate,
		}),
		admissionGate:            opts.AdmissionGate,
		participantCapacity:      capacity,
		participantCapacityKnown: capacityKnown,
		participantCapacityErr:   capacityErr,
	}
	if opts.Activation.MaxConcurrentLoads > 0 {
		r.activation = activation.New(opts.Activation)
	}
	if !opts.Idle.Disable {
		r.sweeper = idle.New(idle.Options{
			IdleTimeout:       opts.Idle.IdleTimeout,
			BucketWidth:       opts.Idle.BucketWidth,
			TickInterval:      opts.Idle.SweepInterval,
			MaxPerTick:        opts.Idle.MaxPerSweep,
			BlockedRetryDelay: opts.Idle.BlockedRetryDelay,
			Now:               opts.Idle.Now,
		}, r.tryRetireIdle)
	}
	return r
}

// ParticipantPhysicalCapacity returns the immutable admission profile for
// this Registry. It is a local configuration snapshot, not a replacement for
// the AuthorityStore's atomic capacity check.
func (r *Registry) ParticipantPhysicalCapacity() ParticipantPhysicalCapacity {
	if r == nil || !r.participantCapacityKnown {
		return ParticipantPhysicalCapacity{}
	}
	return r.participantCapacity
}

// ParticipantAdmissionError is the construction/preflight gate for the
// production Plant Participant. It requires both an Authority Registry (so
// receipt reads are atomic and writer fenced) and a real adapter capacity
// profile. Legacy PlayerActor callers can continue to use Read/Submit, but
// cannot use this Participant admission capability.
func (r *Registry) ParticipantAdmissionError() error {
	if r == nil || r.authority == nil {
		return ErrParticipantReceiptAuthorityUnavailable
	}
	if r.participantCapacityErr != nil {
		return r.participantCapacityErr
	}
	if !r.participantCapacityKnown {
		return errors.New("player actor: authority participant capacity is unknown")
	}
	return nil
}

// NewAuthorityRegistry 以 AuthorityStore 主线构造 Registry(AR1):每个
// playerID 首次使用时经 LoadAndClaim 建立 Binding,写路径走 Authority
// Commit(ShardWriteFence+WriterClaimID+RecordVersion+幂等 Receipt 原子边界),
// TIMEOUT_UNKNOWN 经 FenceAndClaim(C1→C2) 解析。与 blobstore 构造路径互斥,
// 同一 Registry 同一时刻只有一条写主链(禁止双写)。
func NewAuthorityRegistry(provider *authority.Provider, opts RegistryOptions) *Registry {
	r := NewRegistryWithOptions(nil, opts)
	r.authority = provider
	r.admissionSnapshotReuseSafe = provider != nil && provider.AdmissionSnapshotReuseSafe()
	r.modelFieldV2Conversion = opts.ModelFieldV2Conversion
	r.newModelLayout = opts.NewModelLayout
	if r.newModelLayout != authoritystore.ModelLayoutV1 && r.newModelLayout != authoritystore.ModelLayoutV2 {
		r.newModelLayout = authoritystore.ModelLayoutV1
	}
	r.bindings = make(map[uint64]*authority.Binding)
	r.authorityCoords = make(map[uint64]*playerAuthorityCoord)
	if provider != nil {
		caps, err := provider.ParticipantPhysicalCapabilities()
		if err == nil {
			r.participantCapacity, err = participantPhysicalCapacityFromStore(caps)
		}
		r.participantCapacityErr = err
		r.participantCapacityKnown = err == nil
		r.writerEpoch = func(playerID uint64) (uint64, error) {
			return provider.WriterEpoch(context.Background(), actorrouting.DomainPlayer, playerID)
		}
		if provider.SoftSaveEnabled() {
			r.dirtyDrainTimeout = opts.DirtyDrainTimeout
			if r.dirtyDrainTimeout <= 0 {
				r.dirtyDrainTimeout = 8 * time.Second
			}
			r.writeback, r.writebackInitErr = dirtywriteback.New(opts.DirtyWriteback, r.flushSoftActor)
		}
	}
	return r
}

// ParticipantCapacityProjectionRequired reports whether ingress must build a
// detached PlayerRecord with a worst-case result proof for physical Store
// admission. Dirty writeback keeps version/generation admission but does not
// use that byte projection, so returning false removes two full-record clones
// from each Participant execution without weakening receipt retention.
func (r *Registry) ParticipantCapacityProjectionRequired() bool {
	return r == nil || r.authority == nil || !r.authority.SoftSaveEnabled()
}

func (r *Registry) flushSoftActor(ctx context.Context, playerID uint64) error {
	if r == nil || r.exec == nil || playerID == 0 {
		return errors.New("player actor: dirty writeback unavailable")
	}
	var preparedBinding *authority.Binding
	var attempt *authority.SoftFlushAttempt
	_, err := r.exec.SubmitSystem(ctx, playerID, actorruntime.SystemKindPersistenceCompleted, func(ctx context.Context) (any, error) {
		release, err := r.acquireAuthorityCoord(ctx, playerID)
		if err != nil {
			return nil, err
		}
		defer release()
		r.bindingsMu.Lock()
		binding := r.bindings[playerID]
		r.bindingsMu.Unlock()
		if binding == nil || !binding.SoftDirty() {
			return nil, nil
		}
		preparedBinding = binding
		attempt, _, err = binding.PrepareSoftFlush(playerID)
		return nil, err
	})
	if err != nil || attempt == nil {
		return err
	}
	execution := attempt.Execute(ctx)
	completionCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	completionRan := false
	_, completionErr := r.exec.SubmitSystem(completionCtx, playerID, actorruntime.SystemKindPersistenceCompleted, func(ctx context.Context) (any, error) {
		completionRan = true
		release, err := r.acquireAuthorityCoord(ctx, playerID)
		if err != nil {
			return nil, err
		}
		defer release()
		r.bindingsMu.Lock()
		current := r.bindings[playerID]
		r.bindingsMu.Unlock()
		if current != preparedBinding {
			_ = preparedBinding.AbortSoftFlush(attempt)
			return nil, blobstore.ErrWriterEpochMismatch
		}
		_, err = current.CompleteSoftFlush(attempt, execution)
		if errors.Is(err, blobstore.ErrWriterEpochMismatch) {
			r.retireBindingIfCurrent(playerID, current)
		}
		return nil, err
	})
	if completionErr != nil && !completionRan {
		abortErr := preparedBinding.AbortSoftFlush(attempt)
		return errors.Join(completionErr, abortErr)
	}
	return completionErr
}

// DirtyWritebackStats exposes the experiment queue without changing the
// durable-mode metrics surface.
func (r *Registry) DirtyWritebackStats() dirtywriteback.MetricsSnapshot {
	if r == nil || r.writeback == nil {
		return dirtywriteback.MetricsSnapshot{}
	}
	return r.writeback.Snapshot()
}

// AuthorityMetrics 返回 authority 适配层指标快照(legacy 模式为零值)。
type AdmissionSnapshotStats struct {
	Reused, Refreshed uint64
}

func (r *Registry) AdmissionSnapshotStats() AdmissionSnapshotStats {
	if r == nil {
		return AdmissionSnapshotStats{}
	}
	return AdmissionSnapshotStats{
		Reused: r.admissionSnapshotReuseTotal.Load(), Refreshed: r.admissionSnapshotRefreshTotal.Load(),
	}
}

func (r *Registry) AuthorityMetrics() authority.MetricsSnapshot {
	if r == nil || r.authority == nil {
		return authority.MetricsSnapshot{}
	}
	return r.authority.Metrics().Snapshot()
}

func (r *Registry) ResidentRecordStats() ResidentRecordStats {
	if r == nil {
		return ResidentRecordStats{}
	}
	return ResidentRecordStats{
		Hits: r.residentRecordHits.Load(), Misses: r.residentRecordMisses.Load(),
	}
}

func clonePlayerRecord(record *playerpb.PlayerRecord) *playerpb.PlayerRecord {
	if record == nil {
		return nil
	}
	return proto.Clone(record).(*playerpb.PlayerRecord)
}

// residentRecord returns an independent mutable copy for read callers. The
// stored record is never handed to a caller that can retain or mutate it.
func (r *Registry) residentRecord(binding *authority.Binding) (*playerpb.PlayerRecord, uint64, bool) {
	if r == nil || binding == nil || !binding.SoftSaveEnabled() {
		return nil, 0, false
	}
	actorID := binding.Address().ActorID
	value, ok := r.residentRecords.Load(actorID)
	if !ok {
		r.residentRecordMisses.Add(1)
		return nil, 0, false
	}
	entry, ok := value.(*residentPlayerRecord)
	if !ok || entry == nil || entry.binding != binding || entry.version != binding.Version() || entry.record == nil {
		r.residentRecordMisses.Add(1)
		return nil, 0, false
	}
	record := clonePlayerRecord(entry.record)
	// Daily task rollover remains lazily derived at the same Actor-turn read
	// boundary as before; the immutable cached baseline itself is not mutated.
	normalize(record)
	r.residentRecordHits.Add(1)
	return record, entry.version, true
}

// takeResidentRecord transfers the sole decoded resident record to one
// serialized write turn. PlayerActor mailbox + authorityCoord already exclude
// another turn for the same player; removing the entry also prevents an
// outside read from observing the record while the callback mutates it.
//
// A failed or NO_CHANGE callback never puts this record back. The Binding's
// serialized model snapshot remains unchanged, so the next turn reconstructs
// the last accepted state. This preserves rollback isolation without cloning
// the complete PlayerRecord before every successful hot write.
func (r *Registry) takeResidentRecord(binding *authority.Binding) (*playerpb.PlayerRecord, uint64, bool) {
	if r == nil || binding == nil || !binding.SoftSaveEnabled() {
		return nil, 0, false
	}
	actorID := binding.Address().ActorID
	value, ok := r.residentRecords.LoadAndDelete(actorID)
	if !ok {
		r.residentRecordMisses.Add(1)
		return nil, 0, false
	}
	entry, ok := value.(*residentPlayerRecord)
	if !ok || entry == nil || entry.binding != binding || entry.version != binding.Version() || entry.record == nil {
		r.residentRecordMisses.Add(1)
		return nil, 0, false
	}
	normalize(entry.record)
	r.residentRecordHits.Add(1)
	return entry.record, entry.version, true
}

// rememberResidentRecord clones the caller's mutable working copy before
// publishing it. This intentionally pays one bounded clone at the commit
// boundary so callback-owned result objects can never alias the next turn's
// resident truth.
func (r *Registry) rememberResidentRecord(binding *authority.Binding, record *playerpb.PlayerRecord, version uint64) {
	if r == nil || binding == nil || !binding.SoftSaveEnabled() || record == nil || version == 0 {
		return
	}
	r.residentRecords.Store(binding.Address().ActorID, &residentPlayerRecord{
		binding: binding, version: version, record: clonePlayerRecord(record),
	})
}

// rememberOwnedResidentRecord accepts ownership of a callback's working
// record after its new serialized ModelState has been accepted by the Binding.
// Callers must detach any response value that can alias record first.
func (r *Registry) rememberOwnedResidentRecord(binding *authority.Binding, record *playerpb.PlayerRecord, version uint64) {
	if r == nil || binding == nil || !binding.SoftSaveEnabled() || record == nil || version == 0 {
		return
	}
	r.residentRecords.Store(binding.Address().ActorID, &residentPlayerRecord{
		binding: binding, version: version, record: record,
	})
}

// detachResidentTurnValue prevents a protobuf response retained by the RPC
// layer from aliasing the record that becomes the next immutable resident
// baseline. Non-protobuf values are required by the turn contract to be
// independent value objects; the one production wrapper that carried the
// complete PlayerRecord detaches it at its construction site.
func detachResidentTurnValue(value any) any {
	message, ok := value.(proto.Message)
	if !ok || message == nil {
		return value
	}
	return proto.Clone(message)
}

func (r *Registry) forgetResidentRecord(playerID uint64, binding *authority.Binding) {
	if r == nil || playerID == 0 || binding == nil {
		return
	}
	for {
		value, ok := r.residentRecords.Load(playerID)
		if !ok {
			return
		}
		entry, ok := value.(*residentPlayerRecord)
		if !ok || entry == nil || entry.binding != binding {
			return
		}
		if r.residentRecords.CompareAndDelete(playerID, entry) {
			return
		}
	}
}

// AuthorityStats 返回 authority 模式的绑定观测(每 Actor 内存接缝)。
func (r *Registry) AuthorityStats() (bindings int, recordBytesTotal int64) {
	if r == nil || r.authority == nil {
		return 0, 0
	}
	r.bindingsMu.Lock()
	defer r.bindingsMu.Unlock()
	for _, binding := range r.bindings {
		if binding == nil || !binding.Usable() {
			continue
		}
		bindings++
		recordBytesTotal += binding.LastRecordBytes()
	}
	return bindings, recordBytesTotal
}

// dropBinding retires a normal idle Binding. A pending UNKNOWN is retained as
// a same-generation tombstone so a later facts/lifecycle call can settle its
// exact evidence; an ordinary clean idle binding can be removed immediately.
func (r *Registry) dropBinding(playerID uint64) {
	r.bindingsMu.Lock()
	binding := r.bindings[playerID]
	if binding != nil {
		wasRetired := binding.Retired()
		binding.Retire()
		if !wasRetired && !binding.HasPendingUnknown() {
			delete(r.bindings, playerID)
		}
		// Keep the binding-map mutation and activation transition in one
		// critical section. Otherwise a fresh binding could be installed between
		// delete and MarkAbsent and then have its activation state erased.
		if r.activation != nil {
			r.activation.MarkAbsent(playerID)
		}
	}
	r.bindingsMu.Unlock()
	if binding != nil {
		r.forgetResidentRecord(playerID, binding)
	}
}

// retireAndDropBindingIfCurrent is used only after the FenceSource proved that
// expected belongs to a different complete generation (or an activation install
// failed). Exact identity plus the activation transition are serialized with
// replacement installation.
func (r *Registry) retireAndDropBindingIfCurrent(playerID uint64, expected *authority.Binding) bool {
	if expected == nil {
		return false
	}
	r.bindingsMu.Lock()
	defer r.bindingsMu.Unlock()
	if r.bindings[playerID] != expected {
		return false
	}
	expected.Retire()
	delete(r.bindings, playerID)
	r.forgetResidentRecord(playerID, expected)
	if r.activation != nil {
		r.activation.MarkAbsent(playerID)
	}
	return true
}

// retireBindingIfCurrent leaves a bounded same-generation tombstone in the
// existing bindings index. bindingFor may replace it only after the local
// FenceSource proves a different complete Authority generation. The activation
// transition is serialized with the tombstone installation.
func (r *Registry) retireBindingIfCurrent(playerID uint64, expected *authority.Binding) bool {
	if expected == nil {
		return false
	}
	r.bindingsMu.Lock()
	defer r.bindingsMu.Unlock()
	if r.bindings[playerID] != expected {
		return false
	}
	expected.Retire()
	r.forgetResidentRecord(playerID, expected)
	if r.activation != nil {
		r.activation.MarkAbsent(playerID)
	}
	return true
}

func settleRetiredPlayerBinding(ctx context.Context, binding *authority.Binding) error {
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

// admitOutside protects authority operations that intentionally do not submit
// a Player mailbox turn. They still belong to the same shard admission domain
// as user turns and must remain visible to Stop/Wait until all Binding access
// has completed.
func (r *Registry) admitOutside(ctx context.Context, playerID uint64) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	r.lifecycleMu.Lock()
	if r.stopped {
		r.lifecycleMu.Unlock()
		return nil, actorruntime.ErrStopped
	}
	if err := ctx.Err(); err != nil {
		r.lifecycleMu.Unlock()
		return nil, err
	}
	releaseGate := func() {}
	if r.admissionGate != nil {
		var err error
		releaseGate, err = r.admissionGate.Admit(ctx, playerID)
		if err != nil {
			r.lifecycleMu.Unlock()
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			releaseGate()
			r.lifecycleMu.Unlock()
			return nil, err
		}
	}
	r.outsideAdmissions.Add(1)
	r.lifecycleMu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			releaseGate()
			r.outsideAdmissions.Done()
		})
	}, nil
}

// playerAuthorityCoord serializes authority-sensitive work for one player_id.
// refs keeps the gate alive until every acquirer has entered or abandoned it.
type playerAuthorityCoord struct {
	gate chan struct{}
	refs int
}

func newPlayerAuthorityCoord() *playerAuthorityCoord {
	gate := make(chan struct{}, 1)
	gate <- struct{}{}
	return &playerAuthorityCoord{gate: gate}
}

// acquireAuthorityCoord serializes Binding/AuthorityStore work for one player.
// The index mutex is not held while waiting on the gate or during Redis I/O.
func (r *Registry) acquireAuthorityCoord(ctx context.Context, playerID uint64) (func(), error) {
	if r == nil || playerID == 0 {
		return nil, errors.New("player actor: authority coordination unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.authorityCoordMu.Lock()
	if r.authorityCoords == nil {
		r.authorityCoords = make(map[uint64]*playerAuthorityCoord)
	}
	coord := r.authorityCoords[playerID]
	if coord == nil {
		coord = newPlayerAuthorityCoord()
		r.authorityCoords[playerID] = coord
	}
	coord.refs++
	r.authorityCoordMu.Unlock()

	select {
	case <-coord.gate:
	case <-ctx.Done():
		r.releaseAuthorityCoordRef(playerID, coord)
		return nil, ctx.Err()
	}

	var once sync.Once
	return func() {
		once.Do(func() {
			coord.gate <- struct{}{}
			r.releaseAuthorityCoordRef(playerID, coord)
		})
	}, nil
}

func (r *Registry) releaseAuthorityCoordRef(playerID uint64, coord *playerAuthorityCoord) {
	r.authorityCoordMu.Lock()
	coord.refs--
	if coord.refs == 0 && r.authorityCoords[playerID] == coord {
		delete(r.authorityCoords, playerID)
	}
	r.authorityCoordMu.Unlock()
}

// bindingFor 返回该 player 的已声明写绑定;首次使用时以新 claim 身份
// LoadAndClaim(claim 身份在一次绑定生命周期内不变,响应丢失用原身份重入)。
// initial 仅在记录不存在时由 Store 落库;传 nil 表示只读/写的命令路径,
// 缺失记录经 Get 的 ErrNotFound 暴露(与 legacy 语义一致)。
func (r *Registry) bindingFor(ctx context.Context, playerID uint64, initial authoritystore.ModelState) (*authority.Binding, error) {
	r.bindingsMu.Lock()
	binding := r.bindings[playerID]
	r.bindingsMu.Unlock()
	if binding != nil {
		reusable, err := binding.ReusableForCurrentGeneration(ctx)
		if err != nil {
			return nil, err
		}
		if reusable {
			return binding, nil
		}
		// The FenceSource has proved a different exact generation. Settle any
		// retained UNKNOWN through the retired lifecycle identity before removing
		// only the Binding that was compared and claiming afresh.
		if binding.HasPendingUnknown() {
			if err := settleRetiredPlayerBinding(ctx, binding); err != nil {
				return nil, err
			}
		}
		if !r.retireAndDropBindingIfCurrent(playerID, binding) {
			return r.bindingFor(ctx, playerID, initial)
		}
	}
	slotHeld := false
	if r.activation != nil {
		if err := r.activation.BeginLoad(playerID); err != nil {
			return nil, err
		}
		slotHeld = true
	}
	releaseSlot := func(success bool) error {
		if !slotHeld || r.activation == nil {
			return nil
		}
		slotHeld = false
		if success {
			return r.activation.CompleteLoad(playerID)
		}
		_ = r.activation.FailLoad(playerID)
		return nil
	}
	opID, claimID := r.authority.NewClaimIdentity(playerID)
	var status authority.ClaimStatus
	var err error
	for attempt := 0; attempt < 2; attempt++ {
		if initial == nil {
			binding, status, err = r.authority.Claim(ctx, actorrouting.DomainPlayer, playerID, opID, claimID, initial)
		} else {
			binding, status, err = r.authority.ClaimWithModelLayout(ctx, actorrouting.DomainPlayer, playerID,
				opID, claimID, initial, r.newModelLayout)
		}
		if status != authority.ClaimUnknown {
			break
		}
	}
	switch status {
	case authority.ClaimAcquired:
		r.bindingsMu.Lock()
		if existing := r.bindings[playerID]; existing != nil {
			binding = existing
		} else {
			r.bindings[playerID] = binding
		}
		r.bindingsMu.Unlock()
		if completeErr := releaseSlot(true); completeErr != nil {
			r.retireAndDropBindingIfCurrent(playerID, binding)
			return nil, completeErr
		}
		return binding, nil
	case authority.ClaimUnknown:
		_ = releaseSlot(false)
		return nil, blobstore.ErrTimeoutUnknown
	case authority.ClaimFenced:
		_ = releaseSlot(false)
		return nil, blobstore.ErrWriterEpochMismatch
	case authority.ClaimUnavailable:
		_ = releaseSlot(false)
		return nil, blobstore.ErrUnavailable
	default:
		_ = releaseSlot(false)
		if err == nil {
			err = authority.ErrClaimReconcile
		}
		return nil, err
	}
}

// resolvePending 在新的业务写之前解析上一笔 TIMEOUT_UNKNOWN(合同 30 §7.6:
// Unknown 未解析前保守阻塞该 Actor 的全部业务写)。
func (r *Registry) resolvePending(ctx context.Context, binding *authority.Binding) error {
	if !binding.HasPendingUnknown() {
		return nil
	}
	resolution, err := binding.ResolveUnknown(ctx)
	if err != nil {
		return err
	}
	switch resolution {
	case authority.ResolutionCommitted, authority.ResolutionNotCommitted:
		return nil
	case authority.ResolutionFenced, authority.ResolutionReconcile:
		r.retireBindingIfCurrent(binding.Address().ActorID, binding)
		return blobstore.ErrWriterEpochMismatch
	default:
		return blobstore.ErrTimeoutUnknown
	}
}

// loadBinding performs a writer-fenced Redis refresh. Keep it for public reads,
// activation fallback and conflict/UNKNOWN recovery; ordinary hot writes use
// loadCurrentBinding below and let the final Authority CAS revalidate the
// RecordVersion, WriterClaim and ShardWriteFence atomically.
func (r *Registry) loadBinding(ctx context.Context, binding *authority.Binding) (*playerpb.PlayerRecord, uint64, error) {
	actorID := binding.Address().ActorID
	snapshot, claimed, err := binding.TakeClaimedModelSnapshot(actorID)
	if err != nil {
		return nil, 0, err
	}
	if !claimed {
		snapshot, err = binding.GetModelSnapshot(ctx, actorID)
		if err != nil {
			return nil, 0, err
		}
	}
	record, version, err := r.decodeBindingSnapshot(ctx, binding, snapshot)
	if err != nil {
		return nil, 0, err
	}
	r.rememberResidentRecord(binding, record, version)
	return record, version, nil
}

// loadCurrentBinding returns the last snapshot explicitly supplied by a
// successful LoadAndClaim/Commit without another Redis round trip. Callers
// must hold both the player mailbox turn and authorityCoord for this actor.
// The cached snapshot is never trusted after UNKNOWN, a stale physical-usage
// projection or a CAS conflict; those cases fall back to loadBinding.
func (r *Registry) loadCurrentBinding(ctx context.Context, binding *authority.Binding) (*playerpb.PlayerRecord, uint64, error) {
	if err := binding.ValidateCurrentGeneration(ctx); err != nil {
		return nil, 0, err
	}
	if record, version, ok := r.residentRecord(binding); ok {
		// Keep the same post-read generation fence as the serialized fallback.
		if err := binding.ValidateCurrentGeneration(ctx); err != nil {
			return nil, 0, err
		}
		return record, version, nil
	}
	actorID := binding.Address().ActorID
	snapshot, claimed, err := binding.TakeClaimedModelSnapshot(actorID)
	if err != nil {
		return nil, 0, err
	}
	var record *playerpb.PlayerRecord
	var version uint64
	if claimed {
		record, version, err = r.decodeBindingSnapshot(ctx, binding, snapshot)
		if err != nil {
			return nil, 0, err
		}
	} else {
		fallback := false
		// The mailbox and authority coordination locks serialize this callback
		// with every mutation/flush-completion turn. Decode directly from the
		// borrowed bytes so an ordinary dirty Actor turn does not clone the
		// complete ModelState before immediately unmarshalling it.
		err = binding.VisitCurrentModelSnapshot(func(current authoritystore.AuthoritySnapshot) error {
			// A dirty soft snapshot intentionally has no trustworthy physical-usage
			// projection: several logical turns may still map to one future Redis
			// commit. It is nevertheless the current Actor truth and must not fall
			// back to Redis (which would overwrite newer in-memory mutations).
			softCurrent := binding.SoftSaveEnabled() && binding.SoftDirty()
			if binding.HasPendingUnknown() || current.ActorAddress != binding.Address() ||
				current.RecordVersion == 0 || current.ModelFields == nil ||
				(!softCurrent && current.PhysicalUsage.Validate(current.RecordVersion) != nil) {
				fallback = true
				return nil
			}
			var decodeErr error
			record, version, decodeErr = r.decodeBindingSnapshot(ctx, binding, current)
			return decodeErr
		})
		if err != nil {
			return nil, 0, err
		}
		if fallback {
			return r.loadBinding(ctx, binding)
		}
	}
	// This is the read/no-change linearization point. If lease loss or a
	// generation replacement raced the decode, reject instead of returning the
	// old snapshot. A change after this check linearizes after this read.
	if err := binding.ValidateCurrentGeneration(ctx); err != nil {
		return nil, 0, err
	}
	r.rememberResidentRecord(binding, record, version)
	return record, version, nil
}

func (r *Registry) decodeBindingSnapshot(ctx context.Context, binding *authority.Binding, snapshot authoritystore.AuthoritySnapshot) (*playerpb.PlayerRecord, uint64, error) {
	actorID := binding.Address().ActorID
	record, err := decodePlayerAuthoritySnapshot(snapshot, actorID)
	if err != nil {
		return nil, 0, err
	}
	normalize(record)
	if snapshot.ModelLayout == authoritystore.ModelLayoutV1 {
		eligible, policyErr := r.modelFieldV2Conversion.Eligible(actorrouting.DomainPlayer, actorID)
		if policyErr != nil {
			return nil, 0, policyErr
		}
		if eligible {
			candidate, encodeErr := playeractormodelv2.Encode(record)
			if encodeErr != nil {
				return nil, 0, encodeErr
			}
			if migrateErr := migrateBindingV1ToV2(ctx, binding, snapshot.RecordVersion, candidate); migrateErr != nil {
				return nil, 0, migrateErr
			}
			snapshot = binding.CurrentModelSnapshot()
			record, err = decodePlayerAuthoritySnapshot(snapshot, actorID)
			if err != nil {
				return nil, 0, err
			}
		}
	}
	if err := binding.RestoreSoftMemoryVersion(record.GetActorMemoryVersion()); err != nil {
		return nil, 0, err
	}
	return record, binding.Version(), nil
}

func decodePlayerAuthoritySnapshot(snapshot authoritystore.AuthoritySnapshot, actorID uint64) (*playerpb.PlayerRecord, error) {
	var record *playerpb.PlayerRecord
	switch snapshot.ModelLayout {
	case authoritystore.ModelLayoutV1:
		v2Names, _ := fieldv2.ExactNames(actorrouting.DomainPlayer)
		for _, name := range v2Names {
			if _, mixed := snapshot.ModelFields[name]; mixed {
				return nil, errors.New("player actor: mixed-invalid v1/v2 model fields")
			}
		}
		data, ok := snapshot.ModelFields[authority.RecordModelField]
		if !ok {
			return nil, blobstore.ErrNotFound
		}
		var decoded playerpb.PlayerRecord
		if err := proto.Unmarshal(data, &decoded); err != nil {
			return nil, fmt.Errorf("player actor: decode v1 record: %w", err)
		}
		record = &decoded
	case authoritystore.ModelLayoutV2:
		decoded, err := playeractormodelv2.Decode(snapshot.ModelFields)
		if err != nil {
			return nil, fmt.Errorf("player actor: decode v2 fields: %w", err)
		}
		record = decoded
	default:
		return nil, errors.New("player actor: unknown model layout")
	}
	if record.GetPlayerId() != actorID {
		return nil, errors.New("player actor: corrupt player_id")
	}
	return record, nil
}

func migrateBindingV1ToV2(ctx context.Context, binding *authority.Binding, version uint64, candidate authoritystore.ModelState) error {
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
			// Retry the same immutable candidate/mutation/receipt under the
			// resolver-installed claim. No conversion/remarshal occurs here.
			continue
		case authority.ResolutionFenced, authority.ResolutionReconcile:
			return blobstore.ErrWriterEpochMismatch
		default:
			return blobstore.ErrTimeoutUnknown
		}
	}
	return blobstore.ErrTimeoutUnknown
}

// StartIdleSweeper 启动后台分桶扫描;幂等。测试可直接调 SweepIdleOnce。
func (r *Registry) StartIdleSweeper(ctx context.Context) {
	if r == nil || r.sweeper == nil || ctx == nil {
		return
	}
	r.sweepMu.Lock()
	if r.sweepCancel != nil {
		r.sweepMu.Unlock()
		return
	}
	sweepCtx, cancel := context.WithCancel(ctx)
	r.sweepCancel = cancel
	r.sweepMu.Unlock()
	go r.sweeper.Run(sweepCtx)
}

// SweepIdleOnce 同步执行一轮 idle 扫描(测试用),返回处理的候选数。
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

func (r *Registry) touchActivity(playerID uint64) {
	if r.sweeper != nil && playerID != 0 {
		r.sweeper.Touch(playerID)
	}
}

// tryRetireIdle 回收长期空闲的 Player Mailbox。Player 无缓存 actor 与
// activation entry,资源只有 Mailbox/goroutine;下一次命令会自动创建新
// generation 的 Mailbox 并重新加载权威记录,任务不丢不重。
func (r *Registry) tryRetireIdle(ctx context.Context, playerID uint64) idle.Result {
	if r == nil || r.exec == nil || playerID == 0 {
		return idle.Result{Decision: idle.DecisionRetired}
	}
	snap, ok := r.exec.MailboxSnapshot(playerID)
	if !ok {
		return idle.Result{Decision: idle.DecisionRetired}
	}
	var reasons []string
	if snap.InFlight {
		reasons = append(reasons, "turn_running")
	}
	if snap.UserQueued > 0 {
		reasons = append(reasons, "user_queue_nonempty")
	}
	if snap.SystemQueued > 0 {
		reasons = append(reasons, "system_queue_nonempty")
	}
	if len(reasons) > 0 {
		return idle.Result{Decision: idle.DecisionBusy, Reasons: reasons}
	}
	if r.writeback != nil {
		r.bindingsMu.Lock()
		binding := r.bindings[playerID]
		r.bindingsMu.Unlock()
		if binding != nil && binding.SoftDirty() {
			if err := r.writeback.FlushNow(ctx, playerID); err != nil {
				return idle.Result{Decision: idle.DecisionBusy, Reasons: []string{"dirty_flush_failed"}}
			}
		}
	}
	outcome := r.exec.RetireIfIdle(playerID, snap.Generation)
	if outcome == actorruntime.RetireBusy {
		return idle.Result{Decision: idle.DecisionBusy, Reasons: []string{"turn_running"}}
	}
	if outcome == actorruntime.RetireRetired {
		// Mailbox 已退休:同步释放写绑定;下一次命令以新 claim 身份重新
		// LoadAndClaim(合同 30 §9.3:钝化保留旧 Claim,重激活原子换新 Claim)。
		r.dropBinding(playerID)
	}
	return idle.Result{Decision: idle.DecisionRetired}
}

// SetTurnObserver 转发 turn 耗时回调到 runtime executor(观测 turn P99)。
func (r *Registry) SetTurnObserver(observe func(key uint64, duration time.Duration)) {
	if r == nil || r.exec == nil {
		return
	}
	r.exec.SetTurnObserver(observe)
}

func (r *Registry) Stop() {
	timeout := 8 * time.Second
	if r != nil && r.dirtyDrainTimeout > 0 {
		timeout = r.dirtyDrainTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := r.StopWithContext(ctx); err != nil {
		log.Printf("player actor: final dirty flush incomplete: %v", err)
	}
}

// StopWithContext exposes final dirty-flush failure to lifecycle owners and
// tests. Stop retains the legacy signature and emits the failure explicitly.
func (r *Registry) StopWithContext(ctx context.Context) error {
	if r != nil {
		r.lifecycleMu.Lock()
		r.stopped = true
		r.lifecycleMu.Unlock()
		r.sweepMu.Lock()
		cancel := r.sweepCancel
		r.sweepCancel = nil
		r.sweepMu.Unlock()
		if cancel != nil {
			cancel()
		}
	}
	var closeErr error
	if r != nil && r.writeback != nil {
		closeErr = r.writeback.Close(ctx)
	}
	if r != nil && r.exec != nil {
		r.exec.Stop()
	}
	if r != nil {
		r.outsideAdmissions.Wait()
	}
	return closeErr
}

func (r *Registry) RuntimeStats() actorruntime.Stats {
	if r == nil || r.exec == nil {
		return actorruntime.Stats{}
	}
	return r.exec.Stats()
}

func (r *Registry) Ensure(ctx context.Context, playerID uint64, bootstrapID string) (*playerpb.PlayerRecord, uint64, error) {
	return r.EnsureWithProfile(ctx, playerID, bootstrapID, "", 0)
}

// EnsureWithProfile performs the durable owner bootstrap and binds the
// authority-owned public profile. Existing callers that do not carry Web
// profile data use backend defaults and never overwrite a previously
// provisioned display name. When the authority Ensure already linearized the
// profile, this returns without a second versioned LoadAndClaim.
func (r *Registry) EnsureWithProfile(ctx context.Context, playerID uint64, bootstrapID, displayName string, avatarID uint32) (*playerpb.PlayerRecord, uint64, error) {
	if r == nil || (r.store == nil && r.authority == nil) || r.exec == nil {
		return nil, 0, errors.New("player actor: store unavailable")
	}
	r.touchActivity(playerID)
	// Keep record creation and profile provisioning on the existing durable
	// Ensure path. A second versioned turn is only for old records whose
	// stored profile is still empty; login must not LoadAndClaim twice when
	// ensureAuthority already linearized the profile.
	if r.authority != nil {
		record, recordVersion, err := r.ensureAuthority(ctx, playerID, bootstrapID, displayName, avatarID)
		if err != nil {
			return nil, 0, err
		}
		needsWrite, err := publicProfileNeedsWrite(record, displayName, avatarID)
		if err != nil {
			return nil, 0, err
		}
		if !needsWrite {
			return record, recordVersion, nil
		}
		result, err := r.SubmitVersioned(ctx, playerID, func(rec *playerpb.PlayerRecord, _ uint64) (any, bool, error) {
			before := rec.GetPublicProfile()
			beforeName, beforeAvatar := "", uint32(0)
			if before != nil {
				beforeName, beforeAvatar = before.GetDisplayName(), before.GetAvatarId()
			}
			if err := ProvisionPublicProfile(rec, displayName, avatarID); err != nil {
				return nil, false, err
			}
			changed := before == nil || beforeName != rec.GetPublicProfile().GetDisplayName() || beforeAvatar != rec.GetPublicProfile().GetAvatarId()
			return rec, changed, nil
		})
		if err != nil {
			return nil, 0, err
		}
		if result.Value != nil {
			return result.Value.(*playerpb.PlayerRecord), result.CommittedRecordVersion, nil
		}
		return record, recordVersion, nil
	}
	value, err := r.exec.Submit(ctx, playerID, func(ctx context.Context) (any, error) {
		epoch, epochErr := r.epoch(playerID)
		if epochErr != nil {
			return nil, epochErr
		}
		rec, version, err := r.load(ctx, playerID)
		if err == nil {
			if err := blobstore.FenceAtEpoch(ctx, r.store, playerID, epoch); err != nil {
				return nil, err
			}
			for attempt := 0; attempt < maxCASRetries; attempt++ {
				if attempt > 0 {
					rec, version, err = r.load(ctx, playerID)
					if err != nil {
						return nil, err
					}
				}
				before := rec.GetPublicProfile()
				beforeName, beforeAvatar := "", uint32(0)
				if before != nil {
					beforeName, beforeAvatar = before.GetDisplayName(), before.GetAvatarId()
				}
				if err := ProvisionPublicProfile(rec, displayName, avatarID); err != nil {
					return nil, err
				}
				if before != nil && beforeName == rec.GetPublicProfile().GetDisplayName() && beforeAvatar == rec.GetPublicProfile().GetAvatarId() {
					return loaded{record: rec, version: version}, nil
				}
				data, marshalErr := marshal(rec)
				if marshalErr != nil {
					return nil, marshalErr
				}
				if casErr := blobstore.CASAtEpoch(ctx, r.store, playerID, version, epoch, data); casErr != nil {
					if errors.Is(casErr, blobstore.ErrCASConflict) {
						continue
					}
					return nil, casErr
				}
				return loaded{record: rec, version: version + 1}, nil
			}
			return nil, blobstore.ErrCASConflict
		}
		if !errors.Is(err, blobstore.ErrNotFound) {
			return nil, err
		}
		rec = initialRecord(playerID, bootstrapID)
		rec.PublicProfile = nil
		if err := ProvisionPublicProfile(rec, displayName, avatarID); err != nil {
			return nil, err
		}
		data, err := marshal(rec)
		if err != nil {
			return nil, err
		}
		if err := blobstore.InsertAtEpoch(ctx, r.store, playerID, epoch, data); err != nil {
			if !errors.Is(err, blobstore.ErrExists) {
				return nil, err
			}
			rec, version, err = r.load(ctx, playerID)
			if err != nil {
				return nil, err
			}
			before := rec.GetPublicProfile()
			beforeName, beforeAvatar := "", uint32(0)
			if before != nil {
				beforeName, beforeAvatar = before.GetDisplayName(), before.GetAvatarId()
			}
			if err := ProvisionPublicProfile(rec, displayName, avatarID); err != nil {
				return nil, err
			}
			if before == nil || beforeName != rec.GetPublicProfile().GetDisplayName() || beforeAvatar != rec.GetPublicProfile().GetAvatarId() {
				data, marshalErr := marshal(rec)
				if marshalErr != nil {
					return nil, marshalErr
				}
				if casErr := blobstore.CASAtEpoch(ctx, r.store, playerID, version, epoch, data); casErr != nil {
					return nil, casErr
				}
				version++
			}
			return loaded{record: rec, version: version}, nil
		}
		return loaded{record: rec, version: 1}, nil
	})
	if err != nil {
		return nil, 0, err
	}
	got := value.(loaded)
	return got.record, got.version, nil
}

func (r *Registry) Read(ctx context.Context, playerID uint64) (*playerpb.PlayerRecord, uint64, error) {
	r.touchActivity(playerID)
	if r.authority != nil {
		value, err := r.exec.Submit(ctx, playerID, func(ctx context.Context) (any, error) {
			release, err := r.acquireAuthorityCoord(ctx, playerID)
			if err != nil {
				return nil, err
			}
			defer release()
			binding, err := r.bindingFor(ctx, playerID, nil)
			if err != nil {
				return nil, err
			}
			rec, version, err := r.loadBinding(ctx, binding)
			if err != nil {
				if errors.Is(err, blobstore.ErrWriterEpochMismatch) {
					r.retireBindingIfCurrent(playerID, binding)
				}
				return nil, err
			}
			return loaded{record: rec, version: version}, nil
		})
		if err != nil {
			return nil, 0, err
		}
		got := value.(loaded)
		return got.record, got.version, nil
	}
	value, err := r.exec.Submit(ctx, playerID, func(ctx context.Context) (any, error) {
		rec, version, err := r.load(ctx, playerID)
		if err != nil {
			return nil, err
		}
		return loaded{record: rec, version: version}, nil
	})
	if err != nil {
		return nil, 0, err
	}
	got := value.(loaded)
	return got.record, got.version, nil
}

// ReadRoutedCurrent is the hot read path for a command whose ActorRouteHeader
// has already passed the holder's route/lease fence. It uses the serialized
// Binding snapshot when the concrete Store guarantees version-coupled model
// and PhysicalUsage changes; cold/UNKNOWN/unsupported paths retain the
// writer-fenced Redis load. Callers without a verified route must use Read.
func (r *Registry) ReadRoutedCurrent(ctx context.Context, playerID uint64) (*playerpb.PlayerRecord, uint64, error) {
	if r == nil || playerID == 0 {
		return nil, 0, errors.New("player actor: registry and player_id are required")
	}
	if r.authority == nil {
		return r.Read(ctx, playerID)
	}
	r.touchActivity(playerID)
	value, err := r.exec.Submit(ctx, playerID, func(ctx context.Context) (any, error) {
		release, err := r.acquireAuthorityCoord(ctx, playerID)
		if err != nil {
			return nil, err
		}
		defer release()
		binding, err := r.bindingFor(ctx, playerID, nil)
		if err != nil {
			return nil, err
		}
		var rec *playerpb.PlayerRecord
		var version uint64
		if r.admissionSnapshotReuseSafe && !binding.HasPendingUnknown() {
			rec, version, err = r.loadCurrentBinding(ctx, binding)
		} else {
			rec, version, err = r.loadBinding(ctx, binding)
		}
		if errors.Is(err, blobstore.ErrWriterEpochMismatch) {
			r.retireBindingIfCurrent(playerID, binding)
		}
		return loaded{record: rec, version: version}, err
	})
	if err != nil {
		return nil, 0, err
	}
	got := value.(loaded)
	return got.record, got.version, nil
}

// ReadOutsideMailbox is the Participant admission read path. It performs the
// same writer-fence/record-version validation as Read, but does not submit a
// task to the PlayerActor executor. Per-player authority coordination serializes
// this read with that player's mailbox commit without stalling unrelated players.
func (r *Registry) ReadOutsideMailbox(ctx context.Context, playerID uint64) (*playerpb.PlayerRecord, uint64, error) {
	if r == nil || (r.store == nil && r.authority == nil) || playerID == 0 {
		return nil, 0, errors.New("player actor: store unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if r.authority != nil {
		release, err := r.admitOutside(ctx, playerID)
		if err != nil {
			return nil, 0, err
		}
		defer release()
		r.touchActivity(playerID)
		coordRelease, err := r.acquireAuthorityCoord(ctx, playerID)
		if err != nil {
			return nil, 0, err
		}
		defer coordRelease()
		binding, err := r.bindingFor(ctx, playerID, nil)
		if err != nil {
			return nil, 0, err
		}
		var rec *playerpb.PlayerRecord
		var version uint64
		if r.admissionSnapshotReuseSafe && !binding.HasPendingUnknown() {
			rec, version, err = r.loadCurrentBinding(ctx, binding)
		} else {
			rec, version, err = r.loadBinding(ctx, binding)
		}
		if errors.Is(err, blobstore.ErrWriterEpochMismatch) {
			r.retireBindingIfCurrent(playerID, binding)
		}
		return rec, version, err
	}
	// A legacy blobstore only exposes separate epoch and Get operations. Any
	// "check then read" here would admit a Holder/epoch change between them;
	// do not use it for ParticipantReceipt replay. The production Participant
	// constructor rejects this Registry, and direct callers fail closed too.
	return nil, 0, ErrParticipantReceiptAuthorityUnavailable
}

// CheckParticipantAuthorityCapacity repeats Participant admission against the
// current local Binding snapshot after dependency proof but before mailbox
// submission. A version mismatch means the admission read is stale and fails
// closed; the in-mailbox path obtains a fresh guard after its own authority
// load.
func (r *Registry) CheckParticipantAuthorityCapacity(ctx context.Context, playerID, expectedVersion uint64, candidateModelBytes int) error {
	return r.CheckParticipantAuthorityCapacityWithOutcome(ctx, playerID, expectedVersion, candidateModelBytes, authoritystore.StoredOperationOutcome{Status: authoritystore.TerminalSucceeded})
}

// CheckParticipantAuthorityCapacityWithOutcome repeats Participant admission
// against the current binding using the exact outer outcome shape that the
// in-mailbox commit will submit.
func (r *Registry) CheckParticipantAuthorityCapacityWithOutcome(ctx context.Context, playerID, expectedVersion uint64, candidateModelBytes int, outcome authoritystore.StoredOperationOutcome) error {
	if r == nil || r.authority == nil || playerID == 0 || expectedVersion == 0 {
		return ErrParticipantPhysicalCapacity
	}
	if ctx == nil {
		ctx = context.Background()
	}
	release, err := r.admitOutside(ctx, playerID)
	if err != nil {
		return err
	}
	defer release()
	coord, err := r.acquireAuthorityCoord(ctx, playerID)
	if err != nil {
		return err
	}
	defer coord()
	binding, err := r.bindingFor(ctx, playerID, nil)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrParticipantPhysicalCapacity, err)
	}
	checkErr := (ParticipantCapacityGuard{
		binding: binding, version: expectedVersion, limits: r.participantCapacity,
		bestEffort: binding.SoftSaveEnabled(),
	}).CheckWithOutcome(candidateModelBytes, outcome)
	if generationErr := binding.ValidateCurrentGeneration(ctx); generationErr != nil {
		r.retireBindingIfCurrent(playerID, binding)
		if checkErr != nil {
			return errors.Join(generationErr, checkErr)
		}
		return generationErr
	}
	return checkErr
}

func (r *Registry) Submit(ctx context.Context, playerID uint64, turn Turn) (any, error) {
	if turn == nil {
		return nil, errors.New("player actor: player_id and turn required")
	}
	result, err := r.SubmitVersioned(ctx, playerID, func(rec *playerpb.PlayerRecord, _ uint64) (any, bool, error) {
		return turn(rec)
	})
	return result.Value, err
}

// SubmitVersioned is Submit's version-aware seam. Legacy blobstore CAS and
// NewAuthorityRegistry both return the version from the exact commit/read
// operation that linearized this turn, so ParticipantResponse cannot
// accidentally report EconomyVersion or perform an unlocked follow-up read.
func (r *Registry) SubmitVersioned(ctx context.Context, playerID uint64, turn VersionedTurn) (SubmitResult, error) {
	if r == nil || (r.store == nil && r.authority == nil) || r.exec == nil {
		return SubmitResult{}, errors.New("player actor: store unavailable")
	}
	if playerID == 0 || turn == nil {
		return SubmitResult{}, errors.New("player actor: player_id and turn required")
	}
	r.touchActivity(playerID)
	if r.authority != nil {
		return r.submitAuthorityVersioned(ctx, playerID, turn, nil, nil, nil, nil, 0)
	}
	value, err := r.exec.Submit(ctx, playerID, func(ctx context.Context) (any, error) {
		epoch, epochErr := r.epoch(playerID)
		if epochErr != nil {
			return nil, epochErr
		}
		if err := blobstore.FenceAtEpoch(ctx, r.store, playerID, epoch); err != nil {
			return nil, err
		}
		for attempt := 0; attempt < maxCASRetries; attempt++ {
			rec, version, err := r.load(ctx, playerID)
			if err != nil {
				return nil, err
			}
			value, changed, err := turn(rec, version)
			if err != nil || !changed {
				return SubmitResult{Value: value, CommittedRecordVersion: version}, err
			}
			data, err := marshal(rec)
			if err != nil {
				return nil, err
			}
			if err := blobstore.CASAtEpoch(ctx, r.store, playerID, version, epoch, data); err != nil {
				if errors.Is(err, blobstore.ErrCASConflict) {
					continue
				}
				// This registry has no cached activation: each turn resolves
				// the writer epoch and reloads the record, so neither an
				// unknown write nor an epoch mismatch is retried as success.
				return nil, err
			}
			return SubmitResult{Value: value, CommittedRecordVersion: version + 1}, nil
		}
		return nil, blobstore.ErrCASConflict
	})
	if err != nil {
		return SubmitResult{}, err
	}
	return value.(SubmitResult), nil
}

// SubmitParticipantVersioned keeps all Participant mutation in the Player
// mailbox while supplying the pre-mutation callback with a usage guard from
// the exact Authority load that produced rec/currentVersion.
func (r *Registry) SubmitParticipantVersioned(ctx context.Context, playerID uint64, turn ParticipantVersionedTurn) (SubmitResult, error) {
	if r == nil || r.authority == nil || r.exec == nil {
		return SubmitResult{}, ErrParticipantReceiptAuthorityUnavailable
	}
	if playerID == 0 || turn == nil {
		return SubmitResult{}, errors.New("player actor: player_id and participant turn required")
	}
	r.touchActivity(playerID)
	return r.submitAuthorityVersioned(ctx, playerID, nil, turn, nil, nil, nil, 0)
}

func (r *Registry) SubmitParticipantVersionedFromAdmission(ctx context.Context, playerID, admissionRecordVersion uint64, turn ParticipantVersionedTurn) (SubmitResult, error) {
	if r == nil || r.authority == nil || r.exec == nil {
		return SubmitResult{}, ErrParticipantReceiptAuthorityUnavailable
	}
	if playerID == 0 || admissionRecordVersion == 0 || turn == nil {
		return SubmitResult{}, errors.New("player actor: player_id, admission version and participant turn required")
	}
	r.touchActivity(playerID)
	return r.submitAuthorityVersioned(ctx, playerID, nil, turn, nil, nil, nil, admissionRecordVersion)
}

// SubmitParticipantVersionedWithOutcome is the Harvest-capable variant of
// SubmitParticipantVersioned. It keeps the typed business mutation in the same
// PlayerActor mailbox turn while allowing the Authority receipt/outcome to
// retain the exact original typed result for recovery.
func (r *Registry) SubmitParticipantVersionedWithOutcome(ctx context.Context, playerID uint64, turn ParticipantVersionedTurnWithOutcome) (SubmitResult, error) {
	if r == nil || r.authority == nil || r.exec == nil {
		return SubmitResult{}, ErrParticipantReceiptAuthorityUnavailable
	}
	if playerID == 0 || turn == nil {
		return SubmitResult{}, errors.New("player actor: player_id and participant turn required")
	}
	r.touchActivity(playerID)
	return r.submitAuthorityVersioned(ctx, playerID, nil, nil, turn, nil, nil, 0)
}

func (r *Registry) SubmitParticipantVersionedWithOutcomeFromAdmission(ctx context.Context, playerID, admissionRecordVersion uint64, turn ParticipantVersionedTurnWithOutcome) (SubmitResult, error) {
	if r == nil || r.authority == nil || r.exec == nil {
		return SubmitResult{}, ErrParticipantReceiptAuthorityUnavailable
	}
	if playerID == 0 || admissionRecordVersion == 0 || turn == nil {
		return SubmitResult{}, errors.New("player actor: player_id, admission version and participant turn required")
	}
	r.touchActivity(playerID)
	return r.submitAuthorityVersioned(ctx, playerID, nil, nil, turn, nil, nil, admissionRecordVersion)
}

func (r *Registry) SubmitParticipantVersionedWithOrders(ctx context.Context, playerID uint64, turn ParticipantVersionedTurnWithOrders) (SubmitResult, error) {
	if r == nil || r.authority == nil || r.exec == nil {
		return SubmitResult{}, ErrParticipantReceiptAuthorityUnavailable
	}
	if playerID == 0 || turn == nil {
		return SubmitResult{}, errors.New("player actor: player_id and participant turn required")
	}
	r.touchActivity(playerID)
	return r.submitAuthorityVersioned(ctx, playerID, nil, nil, nil, turn, nil, 0)
}

func (r *Registry) SubmitParticipantVersionedWithOrdersFromAdmission(ctx context.Context, playerID, admissionRecordVersion uint64, turn ParticipantVersionedTurnWithOrders) (SubmitResult, error) {
	if r == nil || r.authority == nil || r.exec == nil {
		return SubmitResult{}, ErrParticipantReceiptAuthorityUnavailable
	}
	if playerID == 0 || admissionRecordVersion == 0 || turn == nil {
		return SubmitResult{}, errors.New("player actor: player_id, admission version and participant turn required")
	}
	r.touchActivity(playerID)
	return r.submitAuthorityVersioned(ctx, playerID, nil, nil, nil, turn, nil, admissionRecordVersion)
}

// SubmitDurableCommand executes a PlayerActor command whose PlayerRecord,
// terminal result and recovery orders must share one Authority Commit.
func (r *Registry) SubmitDurableCommand(ctx context.Context, playerID uint64, turn DurableCommandTurn) (SubmitResult, error) {
	if r == nil || r.authority == nil || r.exec == nil {
		return SubmitResult{}, ErrParticipantReceiptAuthorityUnavailable
	}
	if playerID == 0 || turn == nil {
		return SubmitResult{}, errors.New("player actor: player_id and durable command turn required")
	}
	r.touchActivity(playerID)
	return r.submitAuthorityVersioned(ctx, playerID, nil, nil, nil, nil, turn, 0)
}

// submitAuthority 是 AuthorityStore 主线的命令路径:加载权威记录 → 业务
// turn → Authority Commit。RECORD_VERSION_CONFLICT 在同一 WriterClaim 下
// 允许一次"重载 → 重验 → 新 Candidate",再次冲突即 Fail Closed;
// TIMEOUT_UNKNOWN 立即经 FenceAndClaim 解析:已提交返回原值,确定未提交
// 重载后按原幂等身份有界重试,其余上交 ErrTimeoutUnknown(不盲重试)。
func (r *Registry) submitAuthorityVersioned(ctx context.Context, playerID uint64, turn VersionedTurn, participantTurn ParticipantVersionedTurn, participantTurnWithOutcome ParticipantVersionedTurnWithOutcome, participantTurnWithOrders ParticipantVersionedTurnWithOrders, durableCommandTurn DurableCommandTurn, admissionRecordVersion uint64) (SubmitResult, error) {
	value, err := r.exec.Submit(ctx, playerID, func(ctx context.Context) (any, error) {
		release, err := r.acquireAuthorityCoord(ctx, playerID)
		if err != nil {
			return nil, err
		}
		defer release()
		binding, err := r.bindingFor(ctx, playerID, nil)
		if err != nil {
			return nil, err
		}
		hadPendingUnknown := binding.HasPendingUnknown()
		if err := r.resolvePending(ctx, binding); err != nil {
			return nil, err
		}
		conflicts := 0
		participant := participantTurn != nil || participantTurnWithOutcome != nil || participantTurnWithOrders != nil
		// The outside-mailbox admission read already supplied an audited model
		// and PhysicalUsage snapshot. A Store may explicitly guarantee that both
		// change only with RecordVersion; while the Binding is still at that
		// admission version, the serialized mailbox can reuse it. Version drift,
		// UNKNOWN, unsupported Stores and every conflict retain the conservative
		// Redis refresh. The final Authority Commit always rechecks the Fence.
		canReuseAdmission := participant && admissionRecordVersion != 0 && r.admissionSnapshotReuseSafe &&
			binding.Version() == admissionRecordVersion && !hadPendingUnknown
		if participant {
			if canReuseAdmission {
				r.admissionSnapshotReuseTotal.Add(1)
			} else {
				r.admissionSnapshotRefreshTotal.Add(1)
			}
		}
		forceRedisRefresh := hadPendingUnknown || (participant && !canReuseAdmission)
		for attempt := 0; attempt < maxCASRetries; attempt++ {
			var rec *playerpb.PlayerRecord
			var version uint64
			if forceRedisRefresh {
				rec, version, err = r.loadBinding(ctx, binding)
			} else if binding.SoftSaveEnabled() {
				if resident, residentVersion, ok := r.takeResidentRecord(binding); ok {
					rec, version = resident, residentVersion
					// Keep the same post-read generation fence as the cloned read
					// path. A generation change after this check is caught again
					// before StageSoftModelStateOwned.
					err = binding.ValidateCurrentGeneration(ctx)
				} else {
					rec, version, err = r.loadCurrentBinding(ctx, binding)
					// loadCurrentBinding publishes an immutable companion for read
					// callers. This write turn owns the returned working copy, so
					// remove that extra cold-path clone before invoking the callback.
					if err == nil {
						r.forgetResidentRecord(playerID, binding)
					}
				}
			} else {
				rec, version, err = r.loadCurrentBinding(ctx, binding)
			}
			forceRedisRefresh = false
			if err != nil {
				if errors.Is(err, blobstore.ErrWriterEpochMismatch) {
					r.retireBindingIfCurrent(playerID, binding)
				}
				return nil, err
			}
			var value any
			var changed bool
			var operationResult authoritystore.StoredOperationOutcome
			var attachedOrders []authoritystore.AttachedOrder
			if participantTurn != nil {
				guard := ParticipantCapacityGuard{binding: binding, version: version, limits: r.participantCapacity, bestEffort: binding.SoftSaveEnabled()}
				value, changed, err = participantTurn(rec, version, guard)
			} else if participantTurnWithOrders != nil {
				guard := ParticipantCapacityGuard{binding: binding, version: version, limits: r.participantCapacity, bestEffort: binding.SoftSaveEnabled()}
				value, changed, operationResult, attachedOrders, err = participantTurnWithOrders(rec, version, guard)
			} else if participantTurnWithOutcome != nil {
				guard := ParticipantCapacityGuard{binding: binding, version: version, limits: r.participantCapacity, bestEffort: binding.SoftSaveEnabled()}
				value, changed, operationResult, err = participantTurnWithOutcome(rec, version, guard)
			} else if durableCommandTurn != nil {
				value, changed, operationResult, attachedOrders, err = durableCommandTurn(rec, version)
			} else {
				value, changed, err = turn(rec, version)
			}
			if err != nil || !changed {
				// Callback errors, replay and no-change results have no final CAS.
				// Revalidate in every case so a callback error cannot mask a
				// concurrent generation loss and return facts from the old binding.
				if generationErr := binding.ValidateCurrentGeneration(ctx); generationErr != nil {
					r.retireBindingIfCurrent(playerID, binding)
					if err != nil {
						return nil, errors.Join(generationErr, err)
					}
					return nil, generationErr
				}
				return SubmitResult{Value: value, CommittedRecordVersion: version, ProducedOrders: cloneSubmitOrders(attachedOrders)}, err
			}
			if binding.SoftSaveEnabled() {
				rec.ActorMemoryVersion = version + 1
			}
			modelState, err := buildPlayerAuthorityCandidate(binding.ModelLayout(), rec)
			if err != nil {
				return nil, err
			}
			if participantTurnWithOutcome == nil && participantTurnWithOrders == nil && durableCommandTurn == nil {
				operationResult = authoritystore.StoredOperationOutcome{Status: authoritystore.TerminalSucceeded}
			}
			if durableCommandTurn != nil && operationResult.Status == authoritystore.TerminalUnknown {
				return nil, errors.New("player actor: durable commit requires terminal outcome")
			}
			if binding.SoftSaveEnabled() {
				if r.writebackInitErr != nil || r.writeback == nil {
					if r.writebackInitErr != nil {
						return nil, r.writebackInitErr
					}
					return nil, errors.New("player actor: dirty writeback scheduler unavailable")
				}
				if err := binding.ValidateCurrentGeneration(ctx); err != nil {
					r.retireBindingIfCurrent(playerID, binding)
					return nil, err
				}
				// buildPlayerAuthorityCandidate returns a fresh map whose ownership
				// ends in this mailbox turn, so transfer it instead of cloning the
				// complete Player state a second time.
				memoryVersion, stageErr := binding.StageSoftModelStateOwned(playerID, modelState, nil)
				if stageErr != nil {
					return nil, stageErr
				}
				// Publish an immutable decoded companion for the next turn. The
				// Binding remains the serialized ownership boundary used by flush,
				// drain and reactivation.
				value = detachResidentTurnValue(value)
				r.rememberOwnedResidentRecord(binding, rec, memoryVersion)
				if markErr := r.writeback.Mark(playerID); markErr != nil {
					// Never leave an untracked dirty actor. FlushNow installs an
					// emergency scheduler entry even when the ordinary queue is at
					// capacity, then keeps that entry retryable if Redis is
					// temporarily unavailable. Calling Binding directly here would
					// leave a staged mutation invisible to Scheduler.Close/retry.
					if flushErr := r.writeback.FlushNow(ctx, playerID); flushErr != nil {
						return nil, errors.Join(markErr, flushErr)
					}
				}
				return SubmitResult{Value: value, CommittedRecordVersion: memoryVersion, ProducedOrders: cloneSubmitOrders(attachedOrders)}, nil
			}
			err = binding.CASEpochModelStateWithOutcomeAndOrders(ctx, playerID, version, 0, modelState, operationResult, attachedOrders)
			switch {
			case err == nil:
				r.rememberResidentRecord(binding, rec, version+1)
				return SubmitResult{Value: value, CommittedRecordVersion: version + 1, ProducedOrders: cloneSubmitOrders(attachedOrders)}, nil
			case errors.Is(err, blobstore.ErrCASConflict):
				conflicts++
				if conflicts >= 2 {
					return nil, fmt.Errorf("%w: repeated record version conflict under one writer claim", blobstore.ErrCASConflict)
				}
				// A local snapshot is authoritative only while the expected
				// version remains current. Reload before rebuilding the turn.
				forceRedisRefresh = true
			case errors.Is(err, blobstore.ErrTimeoutUnknown):
				resolution, resolveErr := binding.ResolveUnknown(ctx)
				if resolveErr != nil {
					return nil, resolveErr
				}
				switch resolution {
				case authority.ResolutionCommitted:
					r.rememberResidentRecord(binding, rec, version+1)
					return SubmitResult{Value: value, CommittedRecordVersion: version + 1, ProducedOrders: cloneSubmitOrders(attachedOrders)}, nil
				case authority.ResolutionNotCommitted:
					// 确定未提交:重载后按原幂等身份重试(循环顶部重新加载)。
					forceRedisRefresh = true
				default:
					if resolution == authority.ResolutionFenced || resolution == authority.ResolutionReconcile {
						r.retireBindingIfCurrent(playerID, binding)
						return nil, blobstore.ErrWriterEpochMismatch
					}
					return nil, blobstore.ErrTimeoutUnknown
				}
			case errors.Is(err, authority.ErrBindingRetired):
				return nil, err
			case errors.Is(err, blobstore.ErrWriterEpochMismatch):
				r.retireBindingIfCurrent(playerID, binding)
				return nil, err
			default:
				return nil, err
			}
		}
		return nil, blobstore.ErrCASConflict
	})
	if err != nil {
		return SubmitResult{}, err
	}
	return value.(SubmitResult), nil
}

// ensureAuthority 是 AuthorityStore 主线的 Ensure:LoadAndClaim 携带初始记录
// (记录不存在时由 Store 原子安装,等价 legacy Insert);已存在记录直接返回;
// 仅被空 initial 声明过的 stub 记录经 InstallInitial 权威 Commit 补装。
func (r *Registry) ensureAuthority(ctx context.Context, playerID uint64, bootstrapID, displayName string, avatarID uint32) (*playerpb.PlayerRecord, uint64, error) {
	value, err := r.exec.Submit(ctx, playerID, func(ctx context.Context) (any, error) {
		release, err := r.acquireAuthorityCoord(ctx, playerID)
		if err != nil {
			return nil, err
		}
		defer release()
		initialRecordValue := initialRecord(playerID, bootstrapID)
		initialRecordValue.PublicProfile = nil
		if err := ProvisionPublicProfile(initialRecordValue, displayName, avatarID); err != nil {
			return nil, err
		}
		// New Player records also start in model V2. Existing V1 records remain
		// readable and use the normal conversion path on their next activation.
		initialState, err := buildPlayerAuthorityCandidate(r.newModelLayout, initialRecordValue)
		if err != nil {
			return nil, err
		}
		binding, err := r.bindingFor(ctx, playerID, initialState)
		if err != nil {
			return nil, err
		}
		if err := r.resolvePending(ctx, binding); err != nil {
			return nil, err
		}
		rec, version, err := r.loadBinding(ctx, binding)
		if err == nil {
			return loaded{record: rec, version: version}, nil
		}
		if !errors.Is(err, blobstore.ErrNotFound) {
			return nil, err
		}
		_, err = binding.InstallInitialModelState(ctx, initialState, "bootstrap_"+bootstrapID)
		if err == nil {
			rec, version, loadErr := r.loadBinding(ctx, binding)
			if loadErr != nil {
				return nil, loadErr
			}
			return loaded{record: rec, version: version}, nil
		}
		if errors.Is(err, blobstore.ErrCASConflict) {
			// 并发安装:重读;已有记录则返回。
			rec, version, loadErr := r.loadBinding(ctx, binding)
			if loadErr == nil {
				return loaded{record: rec, version: version}, nil
			}
		}
		return nil, err
	})
	if err != nil {
		return nil, 0, err
	}
	got := value.(loaded)
	return got.record, got.version, nil
}

func buildPlayerAuthorityCandidate(layout authoritystore.ModelLayoutVersion, record *playerpb.PlayerRecord) (authoritystore.ModelState, error) {
	switch layout {
	case authoritystore.ModelLayoutV1:
		return buildPlayerAuthorityModelState(record)
	case authoritystore.ModelLayoutV2:
		return playeractormodelv2.Encode(record)
	default:
		return nil, errors.New("player actor: unknown model layout")
	}
}

// SubmitSystem 投递到 SystemQueue,不做 CAS / Fence。UserMailbox 满时仍可入队。
func (r *Registry) SubmitSystem(ctx context.Context, playerID uint64, kind actorruntime.SystemKind, task SystemTask) (any, error) {
	if r == nil || r.exec == nil || playerID == 0 || task == nil {
		return nil, errors.New("player actor: registry unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.touchActivity(playerID)
	return r.exec.SubmitSystem(ctx, playerID, kind, func(ctx context.Context) (any, error) {
		return task(ctx)
	})
}

// NotifyLeaseLost 经 SystemQueue 注入失权屏障 turn。
// 无缓存 actor 可丢弃;turn 仍必须可调度,供路由在用户过载时投递 LeaseLost。
func (r *Registry) NotifyLeaseLost(ctx context.Context, playerID uint64) error {
	if r == nil || r.exec == nil || playerID == 0 {
		return errors.New("player actor: registry unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	_, err := r.exec.SubmitSystem(ctx, playerID, actorruntime.SystemKindLeaseLost, func(ctx context.Context) (any, error) {
		// Lease loss is serialized with mailbox and outside-mailbox Authority
		// access. Keep the retired Binding as a bounded tombstone so the same
		// generation cannot be immediately re-claimed before a new guard exists.
		if r.authority != nil {
			if r.writeback != nil {
				r.writeback.Forget(playerID)
			}
			release, coordErr := r.acquireAuthorityCoord(ctx, playerID)
			if coordErr != nil {
				return nil, coordErr
			}
			r.bindingsMu.Lock()
			binding := r.bindings[playerID]
			r.bindingsMu.Unlock()
			if binding != nil {
				r.retireBindingIfCurrent(playerID, binding)
				if settleErr := settleRetiredPlayerBinding(ctx, binding); settleErr != nil {
					release()
					return nil, settleErr
				}
			}
			release()
		}
		r.leaseLostTotal.Add(1)
		return nil, nil
	})
	return err
}

// LeaseLostTotal 返回 NotifyLeaseLost 已执行次数(观测/单测)。
func (r *Registry) LeaseLostTotal() uint64 {
	if r == nil {
		return 0
	}
	return r.leaseLostTotal.Load()
}

func (r *Registry) epoch(playerID uint64) (uint64, error) {
	if r.writerEpoch == nil {
		return 0, nil
	}
	return r.writerEpoch(playerID)
}

type loaded struct {
	record  *playerpb.PlayerRecord
	version uint64
}

func (r *Registry) load(ctx context.Context, playerID uint64) (*playerpb.PlayerRecord, uint64, error) {
	data, version, err := r.store.Get(ctx, playerID)
	if err != nil {
		return nil, 0, err
	}
	var rec playerpb.PlayerRecord
	if err := proto.Unmarshal(data, &rec); err != nil {
		return nil, 0, fmt.Errorf("player actor: decode record: %w", err)
	}
	if rec.PlayerId != playerID {
		return nil, 0, errors.New("player actor: corrupt player_id")
	}
	normalize(&rec)
	return &rec, version, nil
}

func initialRecord(playerID uint64, bootstrapID string) *playerpb.PlayerRecord {
	record := &playerpb.PlayerRecord{
		PlayerId:       playerID,
		EconomyVersion: 1,
		BootstrapId:    bootstrapID,
		Wallet:         &economypb.Wallet{Coin: DefaultInitialCoin},
		Inventory:      &economypb.Inventory{},
		Progression:    &economypb.Progression{Level: 1},
		PublicProfile:  &playerpb.PlayerPublicProfile{DisplayName: DefaultDisplayName, AvatarId: DefaultAvatarID},
	}
	EnsureTasks(record, NowMs())
	return record
}

func normalize(rec *playerpb.PlayerRecord) {
	if rec.Wallet == nil {
		rec.Wallet = &economypb.Wallet{}
	}
	if rec.Inventory == nil {
		rec.Inventory = &economypb.Inventory{}
	}
	if rec.Progression == nil {
		rec.Progression = &economypb.Progression{Level: 1}
	}
	if rec.EconomyVersion == 0 {
		rec.EconomyVersion = 1
	}
	EnsureTasks(rec, NowMs())
}

func marshal(rec *playerpb.PlayerRecord) ([]byte, error) {
	return proto.MarshalOptions{Deterministic: true}.Marshal(rec)
}

func NowMs() int64 { return time.Now().UnixMilli() }
