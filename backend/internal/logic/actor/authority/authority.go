// Package authority 把合同 30 的 ActorAuthorityStore 主线适配到现有
// Player/Farm Actor Registry 的持久化接缝:
//
//   - Provider 负责 LoadAndClaim(激活)与 claim 身份分配;
//   - Binding 是单 Actor 的已声明写绑定,实现 blobstore.Store/EpochStore,
//     使既有 Actor save 路径(blobstore.CASAtEpoch)不经修改即落到
//     Authority Commit(ShardWriteFence + WriterClaimID + RecordVersion +
//     幂等 Receipt 的原子边界);
//   - Commit 的 TIMEOUT_UNKNOWN 由 Binding.ResolveUnknown 经
//     FenceAndClaim(C1→C2) 解析,禁止盲重试;
//   - 每个 Binding 内嵌一条 persistence.Lane(EnableSoftSave=false):
//     ACTOR_SOFT 保持关闭,权威提交明确成功前不返回资产成功。
//
// 业务幂等(玩家 operation_id ↔ 结果)仍由记录内 Receipt 承载;Binding 合成的
// 幂等键只覆盖"这一次版本 CAS 是否落库",供 Unknown 解析与防重。
package authority

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync/atomic"

	"farmgame/internal/actorrouting"
	"farmgame/internal/logic/actor/persistence"
	"farmgame/internal/observability/redisworkload"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/blobstore"
)

// RecordModelField 是承载整份 Actor 记录(确定性 Protobuf bytes)的 Model 字段名。
// 命名受 Redis 适配器 validModelFieldName 约束(小写字母/数字/下划线)。
const RecordModelField = "record"

// stableCommandTypeName 是合成幂等键的注册名(shape:[A-Z0-9_])。
const stableCommandTypeName = "AR1_RECORD_COMMIT"

const softFlushCommandTypeName = "AR3_SOFT_FLUSH"

const migrationCommandTypeName = "AR2_MODEL_MIGRATION"

// ErrClaimReconcile 表示 LoadAndClaim 返回 RECONCILE_REQUIRED(同一
// claim_operation_id 绑定了不同 claim,或 claim 已被第三方 Resolver 换代)。
var ErrClaimReconcile = errors.New("authority: claim reconcile required")

// ErrBindingRetired reports that a process-local Binding has crossed a
// terminal lease/generation boundary. A retired Binding is retained only as a
// same-generation tombstone; none of its cached snapshots or store methods are
// usable again.
var ErrBindingRetired = fmt.Errorf("%w: authority binding generation retired", blobstore.ErrWriterEpochMismatch)

// ClaimStatus 是 Provider.Claim 的结果分类。
type ClaimStatus uint8

const (
	// ClaimAcquired 表示 CLAIMED 或 ALREADY_CLAIMED,Binding 可用。
	ClaimAcquired ClaimStatus = iota
	// ClaimUnknown 表示 TIMEOUT_UNKNOWN:须用原 claim 身份幂等重入。
	ClaimUnknown
	// ClaimFenced 表示 WRITER_EPOCH_MISMATCH:本 Holder 已失去 Shard 写权。
	ClaimFenced
	// ClaimReconcile 表示 RECONCILE_REQUIRED:Fail Closed。
	ClaimReconcile
	// ClaimUnavailable 表示 UNAVAILABLE_NOT_SENT:可稍后以原身份重试。
	ClaimUnavailable
)

// Resolution 是 Binding.ResolveUnknown 的结论分类。
type Resolution uint8

const (
	// ResolutionCommitted 表示被质疑的提交已落库(Fence 切点 Receipt 证明)。
	ResolutionCommitted Resolution = iota
	// ResolutionNotCommitted 表示被质疑的提交确定未落库,可用原幂等身份重试。
	ResolutionNotCommitted
	// ResolutionReconcile 表示 RECONCILE_REQUIRED / 第三方 Claim:Fail Closed。
	ResolutionReconcile
	// ResolutionRetry 表示 Fence 自身不可用或未知:保持隔离,按原 Resolver 身份重试。
	ResolutionRetry
	// ResolutionFenced 表示 WriterEpoch 已换代:当前 Holder/Activation 失权。
	ResolutionFenced
)

// FenceSource 提供当前 Holder 对某 Actor 的 ShardWriteFence(合同 30 §5.3:
// 由 Holder Repository Adapter 合并 Assignment 视图与本地已验证 Shard Lease
// 证明后注入;不接受业务调用方自由填写)。
type FenceSource interface {
	WriteFence(ctx context.Context, address authoritystore.ActorAddress) (authoritystore.ShardWriteFence, error)
}

// StaticFenceSource 返回固定 fence(单 Holder 装配与测试)。
type StaticFenceSource struct {
	Fence authoritystore.ShardWriteFence
	Err   error
}

func (s StaticFenceSource) WriteFence(context.Context, authoritystore.ActorAddress) (authoritystore.ShardWriteFence, error) {
	if s.Err != nil {
		return authoritystore.ShardWriteFence{}, s.Err
	}
	return s.Fence, nil
}

// Metrics 是 authority 适配层的指标接缝(计数按合同结果分类)。
type Metrics struct {
	ClaimAcquiredTotal  atomic.Uint64
	ClaimUnknownTotal   atomic.Uint64
	ClaimFencedTotal    atomic.Uint64
	ClaimReconcileTotal atomic.Uint64
	ClaimUnavailable    atomic.Uint64
	CommitOKTotal       atomic.Uint64
	CommitAlreadyTotal  atomic.Uint64
	CommitCASConflict   atomic.Uint64
	CommitIdemConflict  atomic.Uint64
	CommitEpochMismatch atomic.Uint64
	CommitClaimMismatch atomic.Uint64
	CommitUnknownTotal  atomic.Uint64
	CommitUnavailable   atomic.Uint64
	ResolveCommitted    atomic.Uint64
	ResolveNotCommitted atomic.Uint64
	ResolveReconcile    atomic.Uint64
	ResolveRetry        atomic.Uint64
	ResolveFenced       atomic.Uint64
}

// MetricsSnapshot 是 Metrics 的只读快照。
type MetricsSnapshot struct {
	ClaimAcquiredTotal  uint64
	ClaimUnknownTotal   uint64
	ClaimFencedTotal    uint64
	ClaimReconcileTotal uint64
	ClaimUnavailable    uint64
	CommitOKTotal       uint64
	CommitAlreadyTotal  uint64
	CommitCASConflict   uint64
	CommitIdemConflict  uint64
	CommitEpochMismatch uint64
	CommitClaimMismatch uint64
	CommitUnknownTotal  uint64
	CommitUnavailable   uint64
	ResolveCommitted    uint64
	ResolveNotCommitted uint64
	ResolveReconcile    uint64
	ResolveRetry        uint64
	ResolveFenced       uint64
}

// Snapshot 返回当前计数快照。
func (m *Metrics) Snapshot() MetricsSnapshot {
	if m == nil {
		return MetricsSnapshot{}
	}
	return MetricsSnapshot{
		ClaimAcquiredTotal:  m.ClaimAcquiredTotal.Load(),
		ClaimUnknownTotal:   m.ClaimUnknownTotal.Load(),
		ClaimFencedTotal:    m.ClaimFencedTotal.Load(),
		ClaimReconcileTotal: m.ClaimReconcileTotal.Load(),
		ClaimUnavailable:    m.ClaimUnavailable.Load(),
		CommitOKTotal:       m.CommitOKTotal.Load(),
		CommitAlreadyTotal:  m.CommitAlreadyTotal.Load(),
		CommitCASConflict:   m.CommitCASConflict.Load(),
		CommitIdemConflict:  m.CommitIdemConflict.Load(),
		CommitEpochMismatch: m.CommitEpochMismatch.Load(),
		CommitClaimMismatch: m.CommitClaimMismatch.Load(),
		CommitUnknownTotal:  m.CommitUnknownTotal.Load(),
		CommitUnavailable:   m.CommitUnavailable.Load(),
		ResolveCommitted:    m.ResolveCommitted.Load(),
		ResolveNotCommitted: m.ResolveNotCommitted.Load(),
		ResolveReconcile:    m.ResolveReconcile.Load(),
		ResolveRetry:        m.ResolveRetry.Load(),
		ResolveFenced:       m.ResolveFenced.Load(),
	}
}

// Provider 是 Registry 与 ActorAuthorityStore 之间的装配接缝。
type Provider struct {
	store   authoritystore.ActorAuthorityStore
	fence   FenceSource
	layout  string
	bootID  string
	seq     atomic.Uint64
	metrics Metrics
	soft    bool
}

// ProviderOptions 装配 Provider。BootID 缺省时随机生成。
type ProviderOptions struct {
	Store                  authoritystore.ActorAuthorityStore
	Fence                  FenceSource
	AuthorityLayoutVersion string
	BootID                 string
	// EnableSoftSave enables the explicit dirty_best_effort experiment for
	// bindings created by this provider. The default remains false.
	EnableSoftSave bool
}

// NewProvider 构造 Provider;store/fence/layout 必填。
func NewProvider(opts ProviderOptions) (*Provider, error) {
	if opts.Store == nil || opts.Fence == nil || opts.AuthorityLayoutVersion == "" {
		return nil, fmt.Errorf("%w: provider store/fence/layout required", authoritystore.ErrInvalidRequest)
	}
	bootID := opts.BootID
	if bootID == "" {
		var entropy [8]byte
		if _, err := rand.Read(entropy[:]); err != nil {
			return nil, fmt.Errorf("authority: boot id entropy: %w", err)
		}
		bootID = "boot_" + hex.EncodeToString(entropy[:])
	}
	return &Provider{
		store:  opts.Store,
		fence:  opts.Fence,
		layout: opts.AuthorityLayoutVersion,
		bootID: bootID,
		soft:   opts.EnableSoftSave,
	}, nil
}

// Metrics 返回适配层指标接缝。
func (p *Provider) Metrics() *Metrics {
	if p == nil {
		return nil
	}
	return &p.metrics
}

// BootID 返回本 Holder 进程代次 ID(指标/日志用)。
func (p *Provider) BootID() string {
	if p == nil {
		return ""
	}
	return p.bootID
}

// SoftSaveEnabled reports whether this provider creates dirty-writeback
// bindings. It is immutable for the process lifetime.
func (p *Provider) SoftSaveEnabled() bool {
	return p != nil && p.soft
}

// PhysicalCapabilities exposes only the immutable limits reported by the
// concrete Store owned by this Provider. Callers cannot inject a second limit
// function beside the commit path; missing capability support fails closed for
// runtimes that require pre-admission.
func (p *Provider) PhysicalCapabilities() (authoritystore.PhysicalCapabilities, error) {
	if p == nil || p.store == nil {
		return authoritystore.PhysicalCapabilities{}, fmt.Errorf("%w: provider store required", authoritystore.ErrInvalidRequest)
	}
	provider, ok := p.store.(authoritystore.PhysicalCapabilitiesProvider)
	if !ok {
		return authoritystore.PhysicalCapabilities{}, fmt.Errorf("%w: authority store physical capabilities unavailable", authoritystore.ErrInvalidRequest)
	}
	caps, err := provider.PhysicalCapabilities()
	if err != nil {
		return authoritystore.PhysicalCapabilities{}, err
	}
	return caps, caps.Validate()
}

// ParticipantPhysicalCapabilities additionally requires the concrete Store's
// commit projector. Ordinary actors remain compatible with the frozen Store
// interface, while Participant construction fails closed without both halves
// of the physical capacity contract.
func (p *Provider) AdmissionSnapshotReuseSafe() bool {
	if p == nil || p.store == nil {
		return false
	}
	capability, ok := p.store.(authoritystore.AdmissionSnapshotReuseCapable)
	return ok && capability.AdmissionSnapshotReuseSafe()
}

func (p *Provider) ParticipantPhysicalCapabilities() (authoritystore.PhysicalCapabilities, error) {
	caps, err := p.PhysicalCapabilities()
	if err != nil {
		return authoritystore.PhysicalCapabilities{}, err
	}
	if _, ok := p.store.(authoritystore.PhysicalCommitUsageProjector); !ok {
		return authoritystore.PhysicalCapabilities{}, fmt.Errorf("%w: authority store physical commit projector unavailable", authoritystore.ErrInvalidRequest)
	}
	return caps, nil
}

func (p *Provider) projectPhysicalCommit(req authoritystore.AuthorityCommitRequest) (authoritystore.PhysicalCommitUsage, error) {
	if p == nil || p.store == nil {
		return authoritystore.PhysicalCommitUsage{}, fmt.Errorf("%w: provider store required", authoritystore.ErrInvalidRequest)
	}
	projector, ok := p.store.(authoritystore.PhysicalCommitUsageProjector)
	if !ok {
		return authoritystore.PhysicalCommitUsage{}, fmt.Errorf("%w: authority store physical commit projector unavailable", authoritystore.ErrInvalidRequest)
	}
	usage, err := projector.ProjectPhysicalCommit(req)
	if err != nil {
		return authoritystore.PhysicalCommitUsage{}, err
	}
	return usage, usage.Validate()
}

// NewClaimIdentity 为一次新 Activation 生成恰好一对
// claim_operation_id + new_writer_claim_id(合同 30 §5.3:同一对必须幂等,
// 响应丢失后用原身份重入,不得每次生成新 Claim)。
func (p *Provider) NewClaimIdentity(actorID uint64) (claimOperationID, writerClaimID string) {
	gen := p.seq.Add(1)
	claimOperationID = fmt.Sprintf("claim_%s_%d_%d", p.bootID, actorID, gen)
	writerClaimID = fmt.Sprintf("wc_%s_%d_%d", p.bootID, actorID, gen)
	return claimOperationID, writerClaimID
}

// WriterEpoch 返回 fence 中的当前 WriterEpoch(供 Registry 既有
// writerEpoch resolver 接缝使用)。
func (p *Provider) WriterEpoch(ctx context.Context, domain actorrouting.RouteDomain, actorID uint64) (uint64, error) {
	if p == nil {
		return 0, errors.New("authority: nil provider")
	}
	fence, err := p.currentWriteFence(ctx, authoritystore.ActorAddress{Domain: domain, ActorID: actorID})
	if err != nil {
		return 0, err
	}
	return fence.ExpectedWriterEpoch, nil
}

func (p *Provider) currentWriteFence(ctx context.Context, address authoritystore.ActorAddress) (authoritystore.ShardWriteFence, error) {
	if p == nil || p.fence == nil {
		return authoritystore.ShardWriteFence{}, errors.New("authority: nil provider")
	}
	return p.fence.WriteFence(ctx, address)
}

// Claim executes LoadAndClaim with the historical v1 initial-model default.
// Callers creating a domain-specific model should use ClaimWithModelLayout so
// a new record is born in its target layout rather than paying migration cost
// during the first actor activation.
func (p *Provider) Claim(ctx context.Context, domain actorrouting.RouteDomain, actorID uint64,
	claimOperationID, writerClaimID string, initial authoritystore.ModelState) (*Binding, ClaimStatus, error) {
	return p.ClaimWithModelLayout(ctx, domain, actorID, claimOperationID, writerClaimID,
		initial, authoritystore.ModelLayoutV1)
}

// ClaimWithModelLayout is Claim with an explicit initial model layout. Existing
// records ignore the initial state/layout; only a newly created actor uses it.
func (p *Provider) ClaimWithModelLayout(ctx context.Context, domain actorrouting.RouteDomain, actorID uint64,
	claimOperationID, writerClaimID string, initial authoritystore.ModelState,
	initialLayout authoritystore.ModelLayoutVersion) (*Binding, ClaimStatus, error) {
	if p == nil || actorID == 0 || claimOperationID == "" || writerClaimID == "" {
		return nil, ClaimReconcile, fmt.Errorf("%w: claim identity", authoritystore.ErrInvalidRequest)
	}
	if initialLayout != authoritystore.ModelLayoutV1 && initialLayout != authoritystore.ModelLayoutV2 {
		return nil, ClaimReconcile, fmt.Errorf("%w: initial model layout", authoritystore.ErrInvalidRequest)
	}
	address := authoritystore.ActorAddress{Domain: domain, ActorID: actorID}
	fence, err := p.fence.WriteFence(ctx, address)
	if err != nil {
		return nil, ClaimFenced, err
	}
	result, err := p.store.LoadAndClaim(redisworkload.WithComponent(ctx, redisworkload.ComponentActorLoad), authoritystore.LoadAndClaimRequest{
		ActorAddress:           address,
		AuthorityLayoutVersion: p.layout,
		ShardWriteFence:        fence,
		ClaimOperationID:       claimOperationID,
		NewWriterClaimID:       writerClaimID,
		InitialModelFields:     initial,
		InitialModelLayout:     initialLayout,
	})
	if err != nil {
		return nil, ClaimReconcile, err
	}
	binding := &Binding{
		provider:  p,
		address:   address,
		fence:     fence,
		claimOpID: claimOperationID,
		claimID:   writerClaimID,
		version:   result.Snapshot.RecordVersion,
		digest:    result.Snapshot.BusinessStateDigest,
		usage:     result.Snapshot.PhysicalUsage,
		lane:      persistence.NewLane(persistence.Options{EnableSoftSave: p.soft}),
	}
	// Keep one activation-only ownership of the LoadAndClaim snapshot. The
	// binding gets its own stable model copy below; the claimed copy is consumed
	// exactly once by the first activation reader, avoiding two more full model
	// clones on the cold-login path.
	binding.syncSnapshot(result.Snapshot)
	claimedSnapshot := result.Snapshot
	binding.claimedSnapshot = &claimedSnapshot
	switch result.Status {
	case authoritystore.LoadAndClaimClaimed, authoritystore.LoadAndClaimAlreadyClaimed:
		p.metrics.ClaimAcquiredTotal.Add(1)
		return binding, ClaimAcquired, nil
	case authoritystore.LoadAndClaimTimeoutUnknown:
		p.metrics.ClaimUnknownTotal.Add(1)
		return nil, ClaimUnknown, nil
	case authoritystore.LoadAndClaimWriterEpochMismatch:
		p.metrics.ClaimFencedTotal.Add(1)
		return nil, ClaimFenced, nil
	case authoritystore.LoadAndClaimUnavailableNotSent:
		p.metrics.ClaimUnavailable.Add(1)
		return nil, ClaimUnavailable, nil
	default:
		p.metrics.ClaimReconcileTotal.Add(1)
		return nil, ClaimReconcile, ErrClaimReconcile
	}
}

// pendingCommit 记录一次 TIMEOUT_UNKNOWN 的 Commit 证据,供 FenceAndClaim 解析。
type pendingCommit struct {
	key           authoritystore.IdempotencyKey
	hash          authoritystore.PayloadHash
	recordVersion uint64
	beforeDigest  authoritystore.PayloadHash
	laneToken     persistence.Token
}

// fenceIdentity 是一次 Unknown 解析的稳定 Resolver 身份
// (resolve_operation_id + new_writer_claim_id 跨重试不变)。
type fenceIdentity struct {
	resolveOperationID string
	newWriterClaimID   string
}

// Binding 是单 Actor 的已声明写绑定。它实现 blobstore.Store/EpochStore,
// 因此既有 Actor save 路径(blobstore.CASAtEpoch)不经修改即落到 Authority
// Commit 主线;所有方法必须由该 Actor 的串行 turn 域调用。
type Binding struct {
	provider *Provider
	address  authoritystore.ActorAddress
	fence    authoritystore.ShardWriteFence

	claimOpID string
	claimID   string

	version uint64
	digest  authoritystore.PayloadHash
	usage   authoritystore.PhysicalUsage
	layout  authoritystore.ModelLayoutVersion
	models  authoritystore.ModelState

	// persistedVersion/models track the Redis baseline separately from the
	// process-local logical version/models while dirty_best_effort is active.
	// All fields are touched only from this actor's serialized mailbox turn.
	persistedVersion uint64
	persistedModels  authoritystore.ModelState
	softGeneration   uint64
	softActivated    bool
	softDirty        bool
	softSchedules    []authoritystore.AttachedSchedule

	// claimedSnapshot is consumed by the first activation read. It preserves
	// the all-core LoadAndClaim response so Player/Farm do not issue a second
	// independently timed read before installing or migrating the Actor.
	claimedSnapshot *authoritystore.AuthoritySnapshot

	lane *persistence.Lane

	pending *pendingCommit
	fenceID *fenceIdentity

	lastRecordBytes atomic.Int64
	retired         atomic.Bool
}

var _ blobstore.Store = (*Binding)(nil)
var _ blobstore.EpochStore = (*Binding)(nil)

// Address 返回绑定 Actor 地址。
func (b *Binding) Address() authoritystore.ActorAddress { return b.address }

// WriterClaimID 返回当前有效 WriterClaim(FenceAndClaim 换代后为新 Claim)。
func (b *Binding) WriterClaimID() string { return b.claimID }

// WriterEpoch 返回激活时 ShardWriteFence 的 WriterEpoch。
func (b *Binding) WriterEpoch() uint64 { return b.fence.ExpectedWriterEpoch }

// Version 返回最近一次权威交互确认的 RecordVersion。
func (b *Binding) Version() uint64 { return b.version }

// SoftSaveEnabled reports the immutable provider mode for this Binding.
func (b *Binding) SoftSaveEnabled() bool {
	return b != nil && b.provider != nil && b.provider.soft
}

// SoftDirty reports whether the process-local model is ahead of Redis.
func (b *Binding) SoftDirty() bool { return b != nil && b.softDirty }

type SoftFlushResult struct {
	Flushed                bool
	MemoryVersion          uint64
	PersistedRecordVersion uint64
}

// SoftFlushAttempt is an immutable snapshot frozen by one actor mailbox turn.
// Execute performs only Store I/O and may safely run outside that mailbox.
type SoftFlushAttempt struct {
	owner         *Binding
	provider      *Provider
	request       authoritystore.AuthorityCommitRequest
	state         authoritystore.ModelState
	claimOpID     string
	token         persistence.Token
	memoryVersion uint64
	generation    uint64
}

// SoftFlushExecution carries the external I/O result back to the actor
// mailbox. Its fields stay private so only Binding can interpret and install
// the result.
type SoftFlushExecution struct {
	result            authoritystore.CommitResult
	err               error
	refreshedBaseline *authoritystore.AuthoritySnapshot
}

// ModelLayout returns the logical model layout from the most recent complete
// Authority snapshot. It is independent from the static authority key-layout
// version carried by Provider.
func (b *Binding) ModelLayout() authoritystore.ModelLayoutVersion { return b.layout }

// RestoreSoftMemoryVersion restores the actor's logical high-water mark from
// its persisted record. Multiple in-memory turns may map to one physical Redis
// RecordVersion, so the two counters must not be conflated after reactivation.
// It is idempotent after the first activation decode.
func (b *Binding) RestoreSoftMemoryVersion(persistedLogicalVersion uint64) error {
	if !b.SoftSaveEnabled() {
		return nil
	}
	if b.softActivated {
		return nil
	}
	logicalVersion := persistedLogicalVersion
	if logicalVersion < b.persistedVersion {
		logicalVersion = b.persistedVersion
	}
	if logicalVersion == 0 {
		return errors.New("authority: persisted logical version unavailable")
	}
	if err := b.lane.RestorePersistedVersion(logicalVersion); err != nil {
		return err
	}
	b.version = logicalVersion
	b.softActivated = true
	return nil
}

// CurrentModelSnapshot returns the last explicitly committed/loaded snapshot
// without performing I/O. It is used only after Commit or FenceAndClaim has
// supplied an authoritative result. Draining and retired generations expose no
// snapshot, including to no-change/replay turns.
func (b *Binding) CurrentModelSnapshot() authoritystore.AuthoritySnapshot {
	if b == nil || b.unusableError() != nil {
		return authoritystore.AuthoritySnapshot{}
	}
	snapshot := authoritystore.AuthoritySnapshot{
		ActorAddress: b.address, WriterEpoch: b.fence.ExpectedWriterEpoch,
		WriterClaimID: b.claimID, RecordVersion: b.version, ModelLayout: b.layout,
		BusinessStateDigest: b.digest, ModelFields: cloneAuthorityModelState(b.models), PhysicalUsage: b.usage,
	}
	if b.softActivated && b.version != b.persistedVersion {
		snapshot.PhysicalUsage = authoritystore.PhysicalUsage{}
	}
	return snapshot
}

// VisitCurrentModelSnapshot exposes the current model bytes only for the
// duration of visitor. It is the allocation-free hot-turn counterpart of
// CurrentModelSnapshot: callers must already serialize the Actor turn and the
// visitor must neither mutate nor retain ModelFields or their byte slices.
//
// Keeping the borrowed view inside a callback makes the ownership boundary
// explicit. Public/read paths that need an independent value must continue to
// use CurrentModelSnapshot, which returns a defensive copy.
func (b *Binding) VisitCurrentModelSnapshot(visitor func(authoritystore.AuthoritySnapshot) error) error {
	if visitor == nil {
		return errors.New("authority: current model snapshot visitor is nil")
	}
	if b == nil {
		return errors.New("authority: nil binding")
	}
	if err := b.unusableError(); err != nil {
		return err
	}
	snapshot := authoritystore.AuthoritySnapshot{
		ActorAddress: b.address, WriterEpoch: b.fence.ExpectedWriterEpoch,
		WriterClaimID: b.claimID, RecordVersion: b.version, ModelLayout: b.layout,
		BusinessStateDigest: b.digest, ModelFields: b.models, PhysicalUsage: b.usage,
	}
	if b.softActivated && b.version != b.persistedVersion {
		snapshot.PhysicalUsage = authoritystore.PhysicalUsage{}
	}
	return visitor(snapshot)
}

// Lane 暴露该 Actor 的持久化 Lane(观测/测试;EnableSoftSave=false)。
func (b *Binding) Lane() *persistence.Lane { return b.lane }

// HasPendingUnknown 报告是否存在尚未解析的 TIMEOUT_UNKNOWN 提交。
// 存在时合同 30 §7.6 要求保守阻塞该 Actor 的全部业务写。
func (b *Binding) HasPendingUnknown() bool { return b.pending != nil }

// Retire irreversibly fences this process-local Binding generation. It does
// not clear UNKNOWN evidence or mutate the persistence lane; a later Authority
// generation must create a new Binding.
func (b *Binding) Retire() {
	if b != nil {
		b.retired.Store(true)
	}
}

// Usable reports whether cached state and store operations are still legal for
// this exact process-local generation.
func (b *Binding) Usable() bool { return b != nil && b.unusableError() == nil }

// Retired reports the irreversible process-local generation tombstone state.
// It does not imply that a pending UNKNOWN has already reached lifecycle
// settlement; lifecycle owners must use ResolveUnknownForRetirement.
func (b *Binding) Retired() bool { return b != nil && b.retired.Load() }

// PrepareForRetirement drains an already retired generation after all UNKNOWN
// evidence has reached a terminal lifecycle outcome. It is deliberately
// unavailable to active generations: ordinary drains must prove that no dirty
// state exists instead of discarding it.
func (b *Binding) PrepareForRetirement() error {
	if b == nil || b.provider == nil || b.lane == nil {
		return errors.New("authority: binding persistence lane unavailable")
	}
	if !b.retired.Load() {
		return errors.New("authority: binding generation is not retired")
	}
	if b.pending != nil {
		return blobstore.ErrTimeoutUnknown
	}
	return b.lane.BeginGenerationRetirement()
}

// ReusableForCurrentGeneration compares the complete current local fence with
// the fence captured by LoadAndClaim. A changed fence requires a fresh Binding;
// an unusable Binding under the same fence remains a fail-closed tombstone.
func (b *Binding) ReusableForCurrentGeneration(ctx context.Context) (bool, error) {
	if b == nil || b.provider == nil {
		return false, errors.New("authority: nil binding")
	}
	// dirty_best_effort deliberately follows the UC-style resident Actor
	// contract: K8s/RouteControl drain retires the local generation, while the
	// delayed Redis flush performs the writer-fence check. Querying the shard
	// authority on every Actor turn would keep a Redis read on the hot path and
	// defeat the purpose of this experimental mode. A missed drain can therefore
	// produce a response from memory that is later rejected by the flush fence;
	// that bounded inconsistency is an explicit mode trade-off.
	if b.SoftSaveEnabled() {
		if err := b.unusableError(); err != nil {
			return false, err
		}
		return true, nil
	}
	current, err := b.provider.currentWriteFence(ctx, b.address)
	if err != nil {
		return false, err
	}
	if current != b.fence {
		b.Retire()
		return false, nil
	}
	if err := b.unusableError(); err != nil {
		return false, err
	}
	return true, nil
}

// ValidateCurrentGeneration is the final validation point for cached/current
// reads and no-change/replay turns. A generation change retires the old
// Binding so no later caller can observe its snapshot.
func (b *Binding) ValidateCurrentGeneration(ctx context.Context) error {
	if err := b.unusableError(); err != nil {
		return err
	}
	// See ReusableForCurrentGeneration: soft mode trusts the process-local
	// generation during ordinary turns. Flush, drain and explicit lifecycle
	// fencing remain writer-fenced at the Store boundary.
	if b.SoftSaveEnabled() {
		return nil
	}
	current, err := b.provider.currentWriteFence(ctx, b.address)
	if err != nil {
		return err
	}
	if current != b.fence {
		b.Retire()
		return blobstore.ErrWriterEpochMismatch
	}
	return b.unusableError()
}

// LastRecordBytes 是最近一次读/写观测到的记录字节数(每 Actor 内存指标接缝)。
func (b *Binding) LastRecordBytes() int64 { return b.lastRecordBytes.Load() }

// PhysicalUsage returns the last concrete Store snapshot observed by this
// binding. Callers must Validate it against the record version they are about
// to mutate; an unavailable or stale snapshot is never inferred from model
// contents.
func (b *Binding) PhysicalUsage() authoritystore.PhysicalUsage {
	if b == nil || b.unusableError() != nil || b.softDirty || b.usage.Validate(b.persistedVersion) != nil {
		return authoritystore.PhysicalUsage{}
	}
	return b.usage
}

// ProjectCASEpochPhysicalCommit builds the exact outer Authority commit shape
// used by CASEpoch and delegates its physical charge to the concrete Store.
func (b *Binding) ProjectCASEpochPhysicalCommit(expectVersion uint64, candidateModelBytes int) (authoritystore.PhysicalCommitUsage, error) {
	return b.ProjectCASEpochPhysicalCommitWithOutcome(expectVersion, candidateModelBytes, authoritystore.StoredOperationOutcome{Status: authoritystore.TerminalSucceeded})
}

// ProjectCASEpochPhysicalCommitWithOrders mirrors the generic CASEpoch seam
// while charging a candidate that carries one or more source-local orders.
// The order list is part of the same physical commit envelope; callers must
// not use this as an estimate for a separate order write.
func (b *Binding) ProjectCASEpochPhysicalCommitWithOrders(expectVersion uint64, candidateModelBytes int, orders []authoritystore.AttachedOrder) (authoritystore.PhysicalCommitUsage, error) {
	return b.ProjectCASEpochPhysicalCommitWithOutcomeAndOrders(expectVersion, candidateModelBytes,
		authoritystore.StoredOperationOutcome{Status: authoritystore.TerminalSucceeded}, orders)
}

// ProjectCASEpochPhysicalCommitWithOutcome mirrors CASEpoch while accounting
// for a typed StoredOperationOutcome payload carried by the same Authority
// receipt. Harvest uses this to admit the exact outer result envelope before
// entering the PlayerActor business turn.
func (b *Binding) ProjectCASEpochPhysicalCommitWithOutcome(expectVersion uint64, candidateModelBytes int, outcome authoritystore.StoredOperationOutcome) (authoritystore.PhysicalCommitUsage, error) {
	return b.ProjectCASEpochPhysicalCommitWithOutcomeAndOrders(expectVersion, candidateModelBytes, outcome, nil)
}

// ProjectCASEpochPhysicalCommitWithOutcomeAndOrders is the capacity projector
// for the generic CASEpoch + AttachedOrders seam. Existing Plant/Harvest
// callers continue to use the no-order method above.
func (b *Binding) ProjectCASEpochPhysicalCommitWithOutcomeAndOrders(expectVersion uint64, candidateModelBytes int, outcome authoritystore.StoredOperationOutcome, orders []authoritystore.AttachedOrder) (authoritystore.PhysicalCommitUsage, error) {
	if err := b.checkID(b.address.ActorID); err != nil {
		return authoritystore.PhysicalCommitUsage{}, err
	}
	if candidateModelBytes <= len(RecordModelField) {
		return authoritystore.PhysicalCommitUsage{}, fmt.Errorf("%w: candidate model bytes", authoritystore.ErrInvalidRequest)
	}
	data := make([]byte, candidateModelBytes-len(RecordModelField))
	return b.provider.projectPhysicalCommit(b.casepochRequestWithOutcomeAndOrders(expectVersion, data, outcome, orders))
}

// Get 经幂等 LoadAndClaim(原 claim 身份重入,ALREADY_CLAIMED 不改动 Claim
// 状态)读取权威记录。记录存在但尚无 record 字段(只被空 initial 声明过)
// 时返回 blobstore.ErrNotFound。
func (b *Binding) Get(ctx context.Context, id uint64) ([]byte, uint64, error) {
	state, version, err := b.GetModelState(ctx, id)
	if err != nil {
		return nil, 0, err
	}
	data, ok := state[RecordModelField]
	if !ok {
		return nil, version, blobstore.ErrNotFound
	}
	b.lastRecordBytes.Store(int64(len(data)))
	return append([]byte(nil), data...), version, nil
}

// GetModelState returns the complete immutable model-field snapshot from the
// same LoadAndClaim response. Farm uses this to load record/header/facts
// without three independent reads or mixed-version observations.
func (b *Binding) GetModelState(ctx context.Context, id uint64) (authoritystore.ModelState, uint64, error) {
	snapshot, err := b.GetModelSnapshot(ctx, id)
	if err != nil {
		return nil, 0, err
	}
	return cloneAuthorityModelState(snapshot.ModelFields), snapshot.RecordVersion, nil
}

// GetModelSnapshot returns one complete immutable truth set plus its layout.
// It idempotently re-enters LoadAndClaim with the same claim identity. Actor
// activation should use TakeClaimedModelSnapshot once to avoid this extra read.
func (b *Binding) GetModelSnapshot(ctx context.Context, id uint64) (authoritystore.AuthoritySnapshot, error) {
	if err := b.checkID(id); err != nil {
		return authoritystore.AuthoritySnapshot{}, err
	}
	// Once a soft binding has accepted an in-memory turn, Redis is only a
	// persistence baseline. Ordinary actor reads must never replace that newer
	// process-local truth with an older flushed snapshot.
	if b.softActivated {
		return cloneAuthoritySnapshot(b.CurrentModelSnapshot()), nil
	}
	result, err := b.provider.store.LoadAndClaim(redisworkload.WithComponent(ctx, redisworkload.ComponentActorLoad), authoritystore.LoadAndClaimRequest{
		ActorAddress:           b.address,
		AuthorityLayoutVersion: b.provider.layout,
		ShardWriteFence:        b.fence,
		ClaimOperationID:       b.claimOpID,
		NewWriterClaimID:       b.claimID,
	})
	if err != nil {
		b.invalidatePhysicalUsage()
		return authoritystore.AuthoritySnapshot{}, err
	}
	switch result.Status {
	case authoritystore.LoadAndClaimClaimed, authoritystore.LoadAndClaimAlreadyClaimed:
		b.syncSnapshot(result.Snapshot)
		return cloneAuthoritySnapshot(result.Snapshot), nil
	case authoritystore.LoadAndClaimWriterEpochMismatch, authoritystore.LoadAndClaimReconcileRequired:
		// ReconcileRequired:我们的固定 claim 身份已被 Resolver 换代,等同失权。
		b.provider.metrics.ClaimFencedTotal.Add(1)
		b.Retire()
		b.invalidatePhysicalUsage()
		return authoritystore.AuthoritySnapshot{}, blobstore.ErrWriterEpochMismatch
	case authoritystore.LoadAndClaimUnavailableNotSent:
		b.invalidatePhysicalUsage()
		return authoritystore.AuthoritySnapshot{}, blobstore.ErrUnavailable
	default:
		b.invalidatePhysicalUsage()
		return authoritystore.AuthoritySnapshot{}, blobstore.ErrTimeoutUnknown
	}
}

// TakeClaimedModelSnapshot consumes the bounded all-core snapshot returned by
// the successful Claim. It is an activation-only operation; absence means the
// Binding has already been activated and callers must use GetModelSnapshot.
func (b *Binding) TakeClaimedModelSnapshot(id uint64) (authoritystore.AuthoritySnapshot, bool, error) {
	if err := b.checkID(id); err != nil {
		return authoritystore.AuthoritySnapshot{}, false, err
	}
	if b.claimedSnapshot == nil {
		return authoritystore.AuthoritySnapshot{}, false, nil
	}
	// The claimed snapshot is an activation-only transfer: Claim already copied
	// the stable binding model, so returning this consumed value does not expose
	// mutable binding state and avoids another model clone before decoding.
	snapshot := *b.claimedSnapshot
	b.claimedSnapshot = nil
	return snapshot, true, nil
}

func cloneAuthorityModelState(in authoritystore.ModelState) authoritystore.ModelState {
	if in == nil {
		return nil
	}
	out := make(authoritystore.ModelState, len(in))
	for name, value := range in {
		out[name] = append([]byte(nil), value...)
	}
	return out
}

func cloneAuthoritySnapshot(in authoritystore.AuthoritySnapshot) authoritystore.AuthoritySnapshot {
	in.ModelFields = cloneAuthorityModelState(in.ModelFields)
	return in
}

// Insert 实现 blobstore.Store(仅在记录缺失时安装 data)。
func (b *Binding) Insert(ctx context.Context, id uint64, data []byte) error {
	return b.InsertEpoch(ctx, id, 0, data)
}

// InsertEpoch 先读后安装:已存在返回 blobstore.ErrExists。缺失时经
// InstallInitial 提交安装(权威 Commit 边界;LoadAndClaim 的 InitialModelFields
// 对已存在的 stub 记录无效,因此不能用它补写)。
func (b *Binding) InsertEpoch(ctx context.Context, id uint64, _ uint64, data []byte) error {
	if err := b.checkID(id); err != nil {
		return err
	}
	if _, _, err := b.Get(ctx, id); err == nil {
		return blobstore.ErrExists
	} else if !errors.Is(err, blobstore.ErrNotFound) {
		return err
	}
	_, err := b.InstallInitial(ctx, data, "install_"+strconv.FormatUint(id, 10))
	return err
}

// CAS 实现 blobstore.Store(writerEpoch 由绑定 fence 注入)。
func (b *Binding) CAS(ctx context.Context, id uint64, expectVersion uint64, data []byte) error {
	return b.CASEpoch(ctx, id, expectVersion, 0, data)
}

// CASEpoch 是既有 Actor save 路径的落点:一次 Authority Commit 原子校验
// ShardWriteFence + ExpectedWriterClaimID + ExpectedRecordVersion +
// 幂等 Receipt,并同提交保存候选状态与终态 Outcome。
func (b *Binding) CASEpoch(ctx context.Context, id uint64, expectVersion, _ uint64, data []byte) error {
	return b.CASEpochWithOutcome(ctx, id, expectVersion, 0, data, authoritystore.StoredOperationOutcome{Status: authoritystore.TerminalSucceeded})
}

// CASEpochWithOutcome is CASEpoch with an explicit terminal outcome payload.
// Candidate PlayerRecord state, the AuthorityReceipt and this outcome still
// linearize in exactly one Store Commit.
func (b *Binding) CASEpochWithOutcome(ctx context.Context, id uint64, expectVersion, _ uint64, data []byte, outcome authoritystore.StoredOperationOutcome) error {
	return b.CASEpochWithOutcomeAndOrders(ctx, id, expectVersion, 0, data, outcome, nil)
}

// CASEpochWithOrders is the generic source-order seam. State, terminal
// outcome, receipt, AttachedOrders and DueIndex are sent to Store.Commit as
// one writer-fenced Authority Commit.
func (b *Binding) CASEpochWithOrders(ctx context.Context, id uint64, expectVersion, _ uint64, data []byte, orders []authoritystore.AttachedOrder) error {
	return b.CASEpochWithOutcomeAndOrders(ctx, id, expectVersion, 0, data,
		authoritystore.StoredOperationOutcome{Status: authoritystore.TerminalSucceeded}, orders)
}

// CASEpochWithOutcomeAndOrders is CASEpochWithOutcome plus the generic
// AttachedOrders list. It deliberately does not introduce a CARE-specific
// storage API, so Plant/Harvest behavior remains unchanged.
func (b *Binding) CASEpochWithOutcomeAndOrders(ctx context.Context, id uint64, expectVersion, _ uint64, data []byte, outcome authoritystore.StoredOperationOutcome, orders []authoritystore.AttachedOrder) error {
	return b.CASEpochModelStateWithOutcomeAndOrders(ctx, id, expectVersion, 0,
		authoritystore.ModelState{RecordModelField: append([]byte(nil), data...)}, outcome, orders)
}

// CASEpochModelStateWithOutcomeOrdersAndSchedules extends the existing Farm
// authority seam with source-local mutable schedules. State, terminal outcome,
// immutable orders and the current schedule generation are still committed by
// one writer-fenced Store.Commit; there is no post-commit schedule write.
func (b *Binding) CASEpochModelStateWithOutcomeOrdersAndSchedules(ctx context.Context, id uint64, expectVersion, _ uint64, state authoritystore.ModelState, outcome authoritystore.StoredOperationOutcome, orders []authoritystore.AttachedOrder, schedules []authoritystore.AttachedSchedule) error {
	_, err := b.casepochModelStateWithOutcomeOrdersAndSchedules(ctx, id, expectVersion, state, outcome, orders, schedules, false)
	return err
}

// CASEpochModelStateWithOutcomeOrdersAndSchedulesExact is the same atomic
// commit seam, but returns a process-local witness only for the exact first
// COMMITTED response. Source actors use it to propagate the sealed CommitStamp
// to Direct; callers must treat a nil witness as durable Worker fallback.
func (b *Binding) CASEpochModelStateWithOutcomeOrdersAndSchedulesExact(ctx context.Context, id uint64, expectVersion, _ uint64, state authoritystore.ModelState, outcome authoritystore.StoredOperationOutcome, orders []authoritystore.AttachedOrder, schedules []authoritystore.AttachedSchedule) (*authoritystore.ExactCommitEvidence, error) {
	return b.casepochModelStateWithOutcomeOrdersAndSchedules(ctx, id, expectVersion, state, outcome, orders, schedules, true)
}

// CASEpochModelStateWithOutcomeAndOrders commits all named model fields under
// one writer fence, RecordVersion CAS, receipt and optional source orders.
// It is the Farm record/header/facts physical atomicity seam.
func (b *Binding) CASEpochModelStateWithOutcomeAndOrders(ctx context.Context, id uint64, expectVersion, _ uint64, state authoritystore.ModelState, outcome authoritystore.StoredOperationOutcome, orders []authoritystore.AttachedOrder) error {
	_, err := b.casepochModelStateWithOutcomeOrdersAndSchedules(ctx, id, expectVersion, state, outcome, orders, nil, false)
	return err
}

// CASEpochModelStateWithOutcomeAndOrdersExact is the no-schedule exact-output
// variant retained for Farm configurations with source schedules disabled.
func (b *Binding) CASEpochModelStateWithOutcomeAndOrdersExact(ctx context.Context, id uint64, expectVersion, _ uint64, state authoritystore.ModelState, outcome authoritystore.StoredOperationOutcome, orders []authoritystore.AttachedOrder) (*authoritystore.ExactCommitEvidence, error) {
	return b.casepochModelStateWithOutcomeOrdersAndSchedules(ctx, id, expectVersion, state, outcome, orders, nil, true)
}

// StageSoftModelState installs a process-local candidate without Redis I/O.
// It is intentionally unavailable unless the provider was explicitly built
// for dirty_best_effort. Attached business orders are not accepted here;
// callers may carry only the latest mutable opportunity schedule that will be
// reconstructed with the consolidated Farm snapshot.
func (b *Binding) StageSoftModelState(id uint64, state authoritystore.ModelState, schedules []authoritystore.AttachedSchedule) (uint64, error) {
	return b.stageSoftModelState(id, state, schedules, true)
}

// StageSoftModelStateOwned is the allocation-free Actor hot-turn seam. The
// caller transfers exclusive ownership of state to the Binding and must not
// read or mutate the map or any contained byte slice after this call. Actor
// candidate builders produce a fresh ModelState, so Player/Farm mailboxes can
// use this method without sharing mutable state across turns.
//
// StageSoftModelState retains its defensive-copy behavior for general callers
// and tests that do not have this ownership guarantee.
func (b *Binding) StageSoftModelStateOwned(id uint64, state authoritystore.ModelState, schedules []authoritystore.AttachedSchedule) (uint64, error) {
	return b.stageSoftModelState(id, state, schedules, false)
}

func (b *Binding) stageSoftModelState(id uint64, state authoritystore.ModelState, schedules []authoritystore.AttachedSchedule, defensiveCopy bool) (uint64, error) {
	if err := b.checkID(id); err != nil {
		return 0, err
	}
	if !b.SoftSaveEnabled() {
		return 0, persistence.ErrSoftSaveDisabled
	}
	if b.pending != nil {
		return 0, blobstore.ErrTimeoutUnknown
	}
	if state == nil {
		return 0, fmt.Errorf("%w: candidate state required", authoritystore.ErrInvalidRequest)
	}
	if b.layout != authoritystore.ModelLayoutV1 && b.layout != authoritystore.ModelLayoutV2 {
		return 0, fmt.Errorf("%w: unknown current model layout", authoritystore.ErrInvalidRequest)
	}
	if b.layout == authoritystore.ModelLayoutV1 {
		if _, ok := state[RecordModelField]; !ok {
			return 0, fmt.Errorf("%w: record model field is required for v1", authoritystore.ErrInvalidRequest)
		}
	}
	// Do not build and discard a sparse Redis mutation on every in-memory
	// Actor turn. Model names/bytes are fully normalized when the delayed flush
	// builds its immutable request; Stage only enforces the same structural
	// preconditions that BuildActorModelMutation can reject at this point.
	next := b.version + 1
	if err := b.lane.MarkMemoryVersion(next); err != nil {
		return 0, err
	}
	if defensiveCopy {
		state = cloneAuthorityModelState(state)
	}
	b.version = next
	b.models = state
	b.softSchedules = cloneAttachedSchedules(schedules)
	b.softGeneration++
	b.softActivated = true
	b.softDirty = true
	b.invalidatePhysicalUsage()
	b.observeRecordBytes(state)
	return next, nil
}

// PrepareSoftFlush freezes exactly one logical actor version. It must run in
// that actor's mailbox, but performs no Redis I/O.
//
// b.models is an immutable, ownership-transferred ModelState: every soft Actor
// turn installs a fresh map and never mutates a previously installed map or
// byte slice. A flush attempt can therefore retain the current map read-only
// while a newer turn replaces b.models. Copying the complete Actor here used
// to make every delayed flush pay a second full-state allocation even though
// the in-flight generation was already immutable.
func (b *Binding) PrepareSoftFlush(id uint64) (*SoftFlushAttempt, SoftFlushResult, error) {
	if err := b.checkID(id); err != nil {
		return nil, SoftFlushResult{}, err
	}
	if !b.SoftSaveEnabled() {
		return nil, SoftFlushResult{}, persistence.ErrSoftSaveDisabled
	}
	if !b.softDirty {
		return nil, SoftFlushResult{MemoryVersion: b.version, PersistedRecordVersion: b.persistedVersion}, nil
	}
	state := b.models
	schedules := cloneAttachedSchedules(b.softSchedules)
	memoryVersion := b.version
	generation := b.softGeneration
	token, err := b.lane.BeginSoft(memoryVersion)
	if err != nil {
		return nil, SoftFlushResult{}, err
	}
	req, err := b.softFlushRequest(state, schedules, generation)
	if err != nil {
		_ = b.lane.Complete(token, persistence.CompletionNotSent)
		return nil, SoftFlushResult{}, err
	}
	return &SoftFlushAttempt{
		owner: b, provider: b.provider, request: req, state: state, claimOpID: b.claimOpID,
		token: token, memoryVersion: memoryVersion, generation: generation,
	}, SoftFlushResult{}, nil
}

// Execute performs the frozen writer-fenced Store operation outside the actor
// mailbox. Conflict/unknown responses also refresh the physical baseline here,
// so the completion turn never waits on Redis.
func (a *SoftFlushAttempt) Execute(ctx context.Context) SoftFlushExecution {
	if a == nil || a.provider == nil {
		return SoftFlushExecution{err: errors.New("authority: nil soft flush attempt")}
	}
	ctx = redisworkload.WithComponent(ctx, redisworkload.ComponentActorCommit)
	var result authoritystore.CommitResult
	var err error
	if committer, ok := a.provider.store.(authoritystore.SoftFlushCommitter); ok {
		result, err = committer.CommitSoftFlush(ctx, a.request)
	} else if committer, ok := a.provider.store.(authoritystore.AuditedClaimCommitter); ok {
		result, err = committer.CommitAuditedClaim(ctx, a.request)
	} else {
		result, err = a.provider.store.Commit(ctx, a.request)
	}
	execution := SoftFlushExecution{result: result, err: err}
	if err != nil {
		return execution
	}
	switch result.Status {
	case authoritystore.CommitRecordVersionConflict, authoritystore.CommitIdempotencyConflict,
		authoritystore.CommitTimeoutUnknown:
		refresh, refreshErr := a.provider.store.LoadAndClaim(redisworkload.WithComponent(ctx, redisworkload.ComponentActorLoad), authoritystore.LoadAndClaimRequest{
			ActorAddress:           a.request.ActorAddress,
			AuthorityLayoutVersion: a.request.AuthorityLayoutVersion,
			ShardWriteFence:        a.request.ShardWriteFence,
			ClaimOperationID:       a.claimOpID,
			NewWriterClaimID:       a.request.ExpectedWriterClaimID,
		})
		if refreshErr == nil && (refresh.Status == authoritystore.LoadAndClaimClaimed || refresh.Status == authoritystore.LoadAndClaimAlreadyClaimed) {
			snapshot := cloneAuthoritySnapshot(refresh.Snapshot)
			execution.refreshedBaseline = &snapshot
		}
	}
	return execution
}

// CompleteSoftFlush installs one external execution result. It must run in
// the same actor mailbox as PrepareSoftFlush. A newer in-memory generation is
// deliberately left dirty even when the frozen version committed.
func (b *Binding) CompleteSoftFlush(attempt *SoftFlushAttempt, execution SoftFlushExecution) (SoftFlushResult, error) {
	if attempt == nil || attempt.owner != b {
		return SoftFlushResult{}, persistence.ErrStaleCompletion
	}
	if execution.err != nil {
		_ = b.lane.Complete(attempt.token, persistence.CompletionNotSent)
		b.invalidatePhysicalUsage()
		return SoftFlushResult{}, execution.err
	}
	result := execution.result
	switch result.Status {
	case authoritystore.CommitCommitted, authoritystore.CommitAlreadyCommitted:
		if err := b.lane.Complete(attempt.token, persistence.CompletionCommitted); err != nil {
			return SoftFlushResult{}, err
		}
		b.persistedVersion = result.Snapshot.RecordVersion
		// attempt.state is the frozen copy created by PrepareSoftFlush. Execute
		// has completed and the Store contract does not retain request buffers,
		// so ownership can move to the persisted baseline without another full
		// Actor-state clone.
		b.persistedModels = attempt.state
		b.digest = result.Snapshot.BusinessStateDigest
		b.usage = result.Snapshot.PhysicalUsage
		if b.softGeneration == attempt.generation {
			b.softDirty = false
			b.softSchedules = nil
		}
		b.observeRecordBytes(attempt.state)
		// Complete consumes the frozen candidate. Clearing the attempt aliases
		// prevents accidental post-completion mutation through a retained test or
		// diagnostic handle while the Binding owns persistedModels.
		attempt.state = nil
		attempt.request.CandidateState = nil
		if result.Status == authoritystore.CommitCommitted {
			b.provider.metrics.CommitOKTotal.Add(1)
		} else {
			b.provider.metrics.CommitAlreadyTotal.Add(1)
		}
		return SoftFlushResult{Flushed: true, MemoryVersion: attempt.memoryVersion, PersistedRecordVersion: b.persistedVersion}, nil
	case authoritystore.CommitWriterEpochMismatch, authoritystore.CommitWriterClaimMismatch:
		_ = b.lane.Complete(attempt.token, persistence.CompletionNotSent)
		b.Retire()
		b.invalidatePhysicalUsage()
		return SoftFlushResult{}, blobstore.ErrWriterEpochMismatch
	case authoritystore.CommitUnavailableNotSent:
		_ = b.lane.Complete(attempt.token, persistence.CompletionNotSent)
		b.invalidatePhysicalUsage()
		return SoftFlushResult{}, blobstore.ErrUnavailable
	case authoritystore.CommitRecordVersionConflict, authoritystore.CommitIdempotencyConflict:
		_ = b.lane.Complete(attempt.token, persistence.CompletionNotSent)
		b.invalidatePhysicalUsage()
		b.installSoftPersistedBaseline(execution.refreshedBaseline)
		return SoftFlushResult{}, blobstore.ErrCASConflict
	default:
		_ = b.lane.Complete(attempt.token, persistence.CompletionNotSent)
		b.invalidatePhysicalUsage()
		b.installSoftPersistedBaseline(execution.refreshedBaseline)
		return SoftFlushResult{}, blobstore.ErrTimeoutUnknown
	}
}

// AbortSoftFlush releases only lane accounting when the registry cannot post
// the completion turn. Retrying the same generation reuses its idempotency key
// and safely resolves a Redis call that may already have committed.
func (b *Binding) AbortSoftFlush(attempt *SoftFlushAttempt) error {
	if attempt == nil || attempt.owner != b {
		return persistence.ErrStaleCompletion
	}
	return b.lane.Complete(attempt.token, persistence.CompletionNotSent)
}

// FlushSoftModelState is the bounded synchronous composition used only by
// drain/passivation and queue-capacity fallback paths.
func (b *Binding) FlushSoftModelState(ctx context.Context, id uint64) (SoftFlushResult, error) {
	attempt, clean, err := b.PrepareSoftFlush(id)
	if err != nil || attempt == nil {
		return clean, err
	}
	return b.CompleteSoftFlush(attempt, attempt.Execute(ctx))
}

func (b *Binding) softFlushRequest(state authoritystore.ModelState, schedules []authoritystore.AttachedSchedule, generation uint64) (authoritystore.AuthorityCommitRequest, error) {
	mutation, err := b.buildSoftSparseUpdate(state)
	if err != nil {
		return authoritystore.AuthorityCommitRequest{}, err
	}
	claimDigest := sha256.Sum256([]byte(b.claimID))
	key := authoritystore.IdempotencyKey{
		SubjectPlayerID:       b.address.ActorID,
		StableCommandTypeName: softFlushCommandTypeName,
		OperationID:           fmt.Sprintf("soft_flush_%d_%s_%d", b.address.ActorID, hex.EncodeToString(claimDigest[:6]), generation),
		Root: authoritystore.OperationRootRef{
			Kind: rootKind(b.address.Domain), CanonicalID: strconv.FormatUint(b.address.ActorID, 10),
		},
	}
	outcome := authoritystore.StoredOperationOutcome{Status: authoritystore.TerminalSucceeded}
	// BuildActorModelMutation already hashed the complete immutable Actor state.
	// Bind the soft-flush idempotency payload to that digest instead of hashing
	// every model field a second time on the writeback hot path.
	hash := softFlushCanonicalPayloadHash(mutation.ResultingBusinessStateDigest, outcome, schedules)
	return authoritystore.AuthorityCommitRequest{
		CommitRequestID:        commitRequestID(key, hash),
		ActorAddress:           b.address,
		AuthorityLayoutVersion: b.provider.layout,
		ShardWriteFence:        b.fence,
		ExpectedWriterClaimID:  b.claimID,
		ExpectedRecordVersion:  b.persistedVersion,
		IdempotencyKey:         key,
		CanonicalPayloadHash:   hash,
		// state is already the frozen copy owned by this flush attempt. The
		// request and attempt share it read-only until Execute completes.
		CandidateState: state,
		ModelMutation:  mutation,
		Receipt: authoritystore.AuthorityReceipt{
			ReceiptID:            "rcpt_" + key.OperationID,
			IdempotencyKey:       key,
			CanonicalPayloadHash: hash,
			Outcome:              outcome,
		},
		OperationResult:   outcome,
		AttachedSchedules: cloneAttachedSchedules(schedules),
	}, nil
}

func (b *Binding) buildSoftSparseUpdate(candidate authoritystore.ModelState) (*authoritystore.ActorModelMutation, error) {
	baseline := b.persistedModels
	if baseline == nil {
		baseline = authoritystore.ModelState{}
	}
	upserts := make(authoritystore.ModelState)
	for name, value := range candidate {
		if current, exists := baseline[name]; !exists || !bytes.Equal(current, value) {
			// BuildActorModelMutation below takes the one defensive copy owned by
			// the request. candidate itself is immutable for this flush attempt,
			// so copying here as well only doubles the changed-field allocation.
			upserts[name] = value
		}
	}
	deletes := make([]string, 0)
	for name := range baseline {
		if _, remains := candidate[name]; !remains {
			deletes = append(deletes, name)
		}
	}
	return authoritystore.BuildActorModelMutationBorrowed(authoritystore.ModelMutationIntentUpdate,
		b.layout, candidate, upserts, deletes)
}

func (b *Binding) installSoftPersistedBaseline(snapshot *authoritystore.AuthoritySnapshot) {
	if snapshot == nil {
		return
	}
	b.persistedVersion = snapshot.RecordVersion
	b.persistedModels = cloneAuthorityModelState(snapshot.ModelFields)
	b.digest = snapshot.BusinessStateDigest
	b.usage = snapshot.PhysicalUsage
}

func (b *Binding) casepochModelStateWithOutcomeOrdersAndSchedules(ctx context.Context, id uint64, expectVersion uint64, state authoritystore.ModelState, outcome authoritystore.StoredOperationOutcome, orders []authoritystore.AttachedOrder, schedules []authoritystore.AttachedSchedule, wantExact bool) (*authoritystore.ExactCommitEvidence, error) {
	if err := b.checkID(id); err != nil {
		return nil, err
	}
	if b.pending != nil {
		// 合同 30 §7.6:Unknown 未解析前保守阻塞该 Actor 的全部业务写。
		return nil, blobstore.ErrTimeoutUnknown
	}
	req := b.casepochModelRequestWithOutcomeOrdersAndSchedules(expectVersion, state, outcome, orders, schedules)
	if b.layout != authoritystore.ModelLayoutV1 && b.layout != authoritystore.ModelLayoutV2 {
		return nil, fmt.Errorf("%w: unknown current model layout", authoritystore.ErrInvalidRequest)
	}
	if b.layout == authoritystore.ModelLayoutV1 {
		if _, ok := state[RecordModelField]; !ok {
			return nil, fmt.Errorf("%w: record model field is required for v1", authoritystore.ErrInvalidRequest)
		}
	}
	mutation, err := b.buildSparseUpdate(state)
	if err != nil {
		return nil, err
	}
	req.ModelMutation = mutation
	key := req.IdempotencyKey
	hash := req.CanonicalPayloadHash
	nextVersion := expectVersion + 1
	token, err := b.beginDurable(nextVersion)
	if err != nil {
		return nil, err
	}
	var result authoritystore.CommitResult
	if wantExact {
		result, err = b.commitExact(ctx, req)
	} else {
		result, err = b.commitAuditedClaim(ctx, req)
	}
	if err != nil {
		_ = b.lane.Complete(token, persistence.CompletionNotSent)
		b.invalidatePhysicalUsage()
		return nil, err
	}
	m := &b.provider.metrics
	switch result.Status {
	case authoritystore.CommitCommitted:
		m.CommitOKTotal.Add(1)
		_ = b.lane.Complete(token, persistence.CompletionCommitted)
		b.syncSnapshot(result.Snapshot)
		b.observeRecordBytes(state)
		if !wantExact {
			return nil, nil
		}
		evidence, ok := authoritystore.NewExactCommitEvidence(req, result)
		if !ok {
			// The mutation is already durable. Withhold only the Direct proof
			// capability if an adapter violates the sealed-stamp contract.
			return nil, nil
		}
		return &evidence, nil
	case authoritystore.CommitAlreadyCommitted:
		// A replay is durable but never receives a first-commit capability.
		m.CommitAlreadyTotal.Add(1)
		_ = b.lane.Complete(token, persistence.CompletionCommitted)
		b.syncSnapshot(result.Snapshot)
		b.observeRecordBytes(state)
		return nil, nil
	case authoritystore.CommitRecordVersionConflict:
		m.CommitCASConflict.Add(1)
		_ = b.lane.Complete(token, persistence.CompletionNotSent)
		b.invalidatePhysicalUsage()
		return nil, blobstore.ErrCASConflict
	case authoritystore.CommitWriterEpochMismatch:
		m.CommitEpochMismatch.Add(1)
		_ = b.lane.Complete(token, persistence.CompletionNotSent)
		b.Retire()
		b.invalidatePhysicalUsage()
		return nil, blobstore.ErrWriterEpochMismatch
	case authoritystore.CommitWriterClaimMismatch:
		m.CommitClaimMismatch.Add(1)
		_ = b.lane.Complete(token, persistence.CompletionNotSent)
		b.Retire()
		b.invalidatePhysicalUsage()
		return nil, blobstore.ErrWriterEpochMismatch
	case authoritystore.CommitIdempotencyConflict:
		m.CommitIdemConflict.Add(1)
		_ = b.lane.Complete(token, persistence.CompletionNotSent)
		b.invalidatePhysicalUsage()
		return nil, blobstore.ErrCASConflict
	case authoritystore.CommitUnavailableNotSent:
		m.CommitUnavailable.Add(1)
		_ = b.lane.Complete(token, persistence.CompletionNotSent)
		b.invalidatePhysicalUsage()
		return nil, blobstore.ErrUnavailable
	default:
		// TIMEOUT_UNKNOWN never carries a CommitStamp and can only recover
		// through the existing authoritative fence/receipt path.
		m.CommitUnknownTotal.Add(1)
		_ = b.lane.Complete(token, persistence.CompletionTimeoutUnknown)
		b.pending = &pendingCommit{
			key:           key,
			hash:          hash,
			recordVersion: expectVersion,
			beforeDigest:  b.digest,
			laneToken:     token,
		}
		b.invalidatePhysicalUsage()
		return nil, blobstore.ErrTimeoutUnknown
	}
}

// MigrateV1ToV2 commits the exact v2 candidate through the ordinary Commit
// lane and receipt boundary. Every obsolete v1 field is explicitly deleted;
// fields shared by name (Farm opportunity projections) are upserted only when
// their v2 envelopes differ. The caller must retain and reuse candidate bytes
// across UNKNOWN resolution and a NOT_COMMITTED retry.
func (b *Binding) MigrateV1ToV2(ctx context.Context, id, expectVersion uint64, candidate authoritystore.ModelState) error {
	if err := b.checkID(id); err != nil {
		return err
	}
	if b.pending != nil {
		return blobstore.ErrTimeoutUnknown
	}
	if b.layout == authoritystore.ModelLayoutV2 {
		if equalModelStates(b.models, candidate) {
			return nil
		}
		return fmt.Errorf("%w: v2 migration candidate conflict", authoritystore.ErrInvalidRequest)
	}
	if b.layout != authoritystore.ModelLayoutV1 || expectVersion != b.version || b.models == nil {
		return fmt.Errorf("%w: v1 migration snapshot", authoritystore.ErrInvalidRequest)
	}
	upserts := make(authoritystore.ModelState)
	for name, value := range candidate {
		if current, exists := b.models[name]; !exists || !bytes.Equal(current, value) {
			// BuildActorModelMutation performs the defensive copy. The candidate
			// remains owned by this synchronous commit path until it returns.
			upserts[name] = value
		}
	}
	deletes := make([]string, 0)
	for name := range b.models {
		if _, remains := candidate[name]; !remains {
			deletes = append(deletes, name)
		}
	}
	mutation, err := authoritystore.BuildActorModelMutation(authoritystore.ModelMutationIntentMigrateV1ToV2,
		authoritystore.ModelLayoutV2, candidate, upserts, deletes)
	if err != nil {
		return err
	}
	key := authoritystore.IdempotencyKey{
		SubjectPlayerID:       b.address.ActorID,
		StableCommandTypeName: migrationCommandTypeName,
		OperationID: "migrate_" + strconv.FormatUint(b.address.ActorID, 10) + "_" +
			strconv.FormatUint(expectVersion, 10),
		Root: authoritystore.OperationRootRef{Kind: rootKind(b.address.Domain), CanonicalID: strconv.FormatUint(b.address.ActorID, 10)},
	}
	outcome := authoritystore.StoredOperationOutcome{Status: authoritystore.TerminalSucceeded}
	hash := casepochModelStateCanonicalPayloadHash(candidate, outcome, nil)
	req := authoritystore.AuthorityCommitRequest{
		CommitRequestID: commitRequestID(key, hash), ActorAddress: b.address,
		AuthorityLayoutVersion: b.provider.layout, ShardWriteFence: b.fence,
		ExpectedWriterClaimID: b.claimID, ExpectedRecordVersion: expectVersion,
		IdempotencyKey: key, CanonicalPayloadHash: hash,
		CandidateState: cloneAuthorityModelState(candidate), ModelMutation: mutation,
		Receipt: authoritystore.AuthorityReceipt{ReceiptID: "rcpt_" + key.OperationID,
			IdempotencyKey: key, CanonicalPayloadHash: hash, Outcome: outcome},
		OperationResult: outcome,
	}
	return b.commitPrepared(ctx, req)
}

func (b *Binding) buildSparseUpdate(candidate authoritystore.ModelState) (*authoritystore.ActorModelMutation, error) {
	if b.models == nil {
		return nil, fmt.Errorf("%w: current model snapshot unavailable", authoritystore.ErrInvalidRequest)
	}
	upserts := make(authoritystore.ModelState)
	for name, value := range candidate {
		if current, exists := b.models[name]; !exists || !bytes.Equal(current, value) {
			upserts[name] = append([]byte(nil), value...)
		}
	}
	deletes := make([]string, 0)
	for name := range b.models {
		if _, remains := candidate[name]; !remains {
			deletes = append(deletes, name)
		}
	}
	return authoritystore.BuildActorModelMutation(authoritystore.ModelMutationIntentUpdate,
		b.layout, candidate, upserts, deletes)
}

func (b *Binding) commitAuditedClaim(ctx context.Context, req authoritystore.AuthorityCommitRequest) (authoritystore.CommitResult, error) {
	ctx = redisworkload.WithComponent(ctx, redisworkload.ComponentActorCommit)
	if committer, ok := b.provider.store.(authoritystore.AuditedClaimCommitter); ok {
		return committer.CommitAuditedClaim(ctx, req)
	}
	return b.provider.store.Commit(ctx, req)
}

// commitExact is reachable only from the wantExact source-actor branch above.
// A Store without the optional capability still commits normally but cannot
// manufacture proof evidence.
func (b *Binding) commitExact(ctx context.Context, req authoritystore.AuthorityCommitRequest) (authoritystore.CommitResult, error) {
	ctx = redisworkload.WithComponent(ctx, redisworkload.ComponentActorCommit)
	if committer, ok := b.provider.store.(authoritystore.ExactCommitter); ok {
		return committer.CommitExact(ctx, req)
	}
	return b.commitAuditedClaim(ctx, req)
}

func (b *Binding) commitPrepared(ctx context.Context, req authoritystore.AuthorityCommitRequest) error {
	if b.pending != nil {
		return blobstore.ErrTimeoutUnknown
	}
	token, err := b.beginDurable(req.ExpectedRecordVersion + 1)
	if err != nil {
		return err
	}
	result, err := b.commitAuditedClaim(ctx, req)
	if err != nil {
		_ = b.lane.Complete(token, persistence.CompletionNotSent)
		b.invalidatePhysicalUsage()
		return err
	}
	m := &b.provider.metrics
	switch result.Status {
	case authoritystore.CommitCommitted:
		m.CommitOKTotal.Add(1)
		_ = b.lane.Complete(token, persistence.CompletionCommitted)
		b.syncSnapshot(result.Snapshot)
		b.observeRecordBytes(result.Snapshot.ModelFields)
		return nil
	case authoritystore.CommitAlreadyCommitted:
		m.CommitAlreadyTotal.Add(1)
		_ = b.lane.Complete(token, persistence.CompletionCommitted)
		b.syncSnapshot(result.Snapshot)
		b.observeRecordBytes(result.Snapshot.ModelFields)
		return nil
	case authoritystore.CommitRecordVersionConflict:
		m.CommitCASConflict.Add(1)
		_ = b.lane.Complete(token, persistence.CompletionNotSent)
		b.invalidatePhysicalUsage()
		return blobstore.ErrCASConflict
	case authoritystore.CommitWriterEpochMismatch, authoritystore.CommitWriterClaimMismatch:
		m.CommitEpochMismatch.Add(1)
		_ = b.lane.Complete(token, persistence.CompletionNotSent)
		b.Retire()
		b.invalidatePhysicalUsage()
		return blobstore.ErrWriterEpochMismatch
	case authoritystore.CommitIdempotencyConflict:
		m.CommitIdemConflict.Add(1)
		_ = b.lane.Complete(token, persistence.CompletionNotSent)
		b.invalidatePhysicalUsage()
		return blobstore.ErrCASConflict
	case authoritystore.CommitUnavailableNotSent:
		m.CommitUnavailable.Add(1)
		_ = b.lane.Complete(token, persistence.CompletionNotSent)
		b.invalidatePhysicalUsage()
		return blobstore.ErrUnavailable
	default:
		m.CommitUnknownTotal.Add(1)
		_ = b.lane.Complete(token, persistence.CompletionTimeoutUnknown)
		b.pending = &pendingCommit{key: req.IdempotencyKey, hash: req.CanonicalPayloadHash,
			recordVersion: req.ExpectedRecordVersion, beforeDigest: b.digest, laneToken: token}
		b.invalidatePhysicalUsage()
		return blobstore.ErrTimeoutUnknown
	}
}

func (b *Binding) observeRecordBytes(state authoritystore.ModelState) {
	if data, ok := state[RecordModelField]; ok {
		b.lastRecordBytes.Store(int64(len(data)))
		return
	}
	b.lastRecordBytes.Store(int64(authoritystore.ModelStateSerializedBytes(state)))
}

func equalModelStates(a, b authoritystore.ModelState) bool {
	if len(a) != len(b) {
		return false
	}
	for name, value := range a {
		if !bytes.Equal(value, b[name]) {
			return false
		}
	}
	return true
}

func (b *Binding) casepochRequest(expectVersion uint64, data []byte) authoritystore.AuthorityCommitRequest {
	return b.casepochRequestWithOutcome(expectVersion, data, authoritystore.StoredOperationOutcome{Status: authoritystore.TerminalSucceeded})
}

func (b *Binding) casepochRequestWithOutcome(expectVersion uint64, data []byte, outcome authoritystore.StoredOperationOutcome) authoritystore.AuthorityCommitRequest {
	return b.casepochRequestWithOutcomeAndOrders(expectVersion, data, outcome, nil)
}

func (b *Binding) casepochRequestWithOutcomeAndOrders(expectVersion uint64, data []byte, outcome authoritystore.StoredOperationOutcome, orders []authoritystore.AttachedOrder) authoritystore.AuthorityCommitRequest {
	return b.casepochModelRequestWithOutcomeAndOrders(expectVersion,
		authoritystore.ModelState{RecordModelField: append([]byte(nil), data...)}, outcome, orders)
}

func (b *Binding) casepochModelRequestWithOutcomeAndOrders(expectVersion uint64, state authoritystore.ModelState, outcome authoritystore.StoredOperationOutcome, orders []authoritystore.AttachedOrder) authoritystore.AuthorityCommitRequest {
	return b.casepochModelRequestWithOutcomeOrdersAndSchedules(expectVersion, state, outcome, orders, nil)
}

func (b *Binding) casepochModelRequestWithOutcomeOrdersAndSchedules(expectVersion uint64, state authoritystore.ModelState, outcome authoritystore.StoredOperationOutcome, orders []authoritystore.AttachedOrder, schedules []authoritystore.AttachedSchedule) authoritystore.AuthorityCommitRequest {
	key := commitIdempotencyKey(b.address, expectVersion)
	attachedOrders := cloneAttachedOrders(orders)
	if len(attachedOrders) > 1 {
		sort.SliceStable(attachedOrders, func(i, j int) bool {
			return attachedOrders[i].OrderID < attachedOrders[j].OrderID
		})
	}
	attachedSchedules := cloneAttachedSchedules(schedules)
	if len(attachedSchedules) > 1 {
		sort.SliceStable(attachedSchedules, func(i, j int) bool {
			if attachedSchedules[i].ScheduleType != attachedSchedules[j].ScheduleType {
				return attachedSchedules[i].ScheduleType < attachedSchedules[j].ScheduleType
			}
			return attachedSchedules[i].ScheduleID < attachedSchedules[j].ScheduleID
		})
	}
	hash := casepochModelStateCanonicalPayloadHashWithSchedules(state, outcome, attachedOrders, attachedSchedules)
	return authoritystore.AuthorityCommitRequest{
		CommitRequestID:        commitRequestID(key, hash),
		ActorAddress:           b.address,
		AuthorityLayoutVersion: b.provider.layout,
		ShardWriteFence:        b.fence,
		ExpectedWriterClaimID:  b.claimID,
		ExpectedRecordVersion:  expectVersion,
		IdempotencyKey:         key,
		CanonicalPayloadHash:   hash,
		CandidateState:         cloneAuthorityModelState(state),
		Receipt: authoritystore.AuthorityReceipt{
			ReceiptID:            "rcpt_" + key.OperationID,
			IdempotencyKey:       key,
			CanonicalPayloadHash: hash,
			Outcome:              outcome,
		},
		OperationResult:   outcome,
		AttachedOrders:    attachedOrders,
		AttachedSchedules: attachedSchedules,
	}
}

func cloneAttachedSchedules(schedules []authoritystore.AttachedSchedule) []authoritystore.AttachedSchedule {
	if len(schedules) == 0 {
		return nil
	}
	cloned := make([]authoritystore.AttachedSchedule, len(schedules))
	for index, schedule := range schedules {
		cloned[index] = schedule
		cloned[index].Payload = append([]byte(nil), schedule.Payload...)
	}
	return cloned
}

// casepochModelStateCanonicalPayloadHash preserves the historical record-only
// hash. Multi-model commits include names and values in sorted order, so a
// retry cannot silently bind the same version to different Header/Facts.
func casepochModelStateCanonicalPayloadHash(state authoritystore.ModelState, outcome authoritystore.StoredOperationOutcome, orders []authoritystore.AttachedOrder) [sha256.Size]byte {
	return casepochModelStateCanonicalPayloadHashWithSchedules(state, outcome, orders, nil)
}

func casepochModelStateCanonicalPayloadHashWithSchedules(state authoritystore.ModelState, outcome authoritystore.StoredOperationOutcome, orders []authoritystore.AttachedOrder, schedules []authoritystore.AttachedSchedule) [sha256.Size]byte {
	if len(schedules) == 0 {
		if len(state) == 1 {
			if data, ok := state[RecordModelField]; ok {
				return casepochCanonicalPayloadHash(data, outcome, orders)
			}
		}
	}
	hash := sha256.New()
	if len(schedules) == 0 {
		hash.Write([]byte("AR1-MODEL-STATE-COMMIT-V1"))
	} else {
		hash.Write([]byte("AR1-MODEL-STATE-COMMIT-V2-SCHEDULES"))
	}
	names := make([]string, 0, len(state))
	for name := range state {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		writeCanonicalBytes(hash, []byte(name))
		writeCanonicalBytes(hash, state[name])
	}
	var status [4]byte
	binary.BigEndian.PutUint32(status[:], uint32(outcome.Status))
	hash.Write(status[:])
	writeCanonicalBytes(hash, []byte(outcome.ErrorCode))
	writeCanonicalBytes(hash, outcome.Payload)
	for _, order := range orders {
		writeCanonicalBytes(hash, []byte(order.OrderID))
		writeCanonicalBytes(hash, []byte(order.OrderType))
		writeCanonicalBytes(hash, order.Payload)
		var due [8]byte
		binary.BigEndian.PutUint64(due[:], uint64(order.DueAtMs))
		hash.Write(due[:])
		var subject [8]byte
		binary.BigEndian.PutUint64(subject[:], order.SubjectPlayerID)
		hash.Write(subject[:])
	}
	for _, schedule := range schedules {
		writeCanonicalBytes(hash, []byte(schedule.ScheduleID))
		writeCanonicalBytes(hash, []byte(schedule.ScheduleType))
		writeCanonicalBytes(hash, schedule.Payload)
		var generation [8]byte
		binary.BigEndian.PutUint64(generation[:], schedule.Generation)
		hash.Write(generation[:])
		var due [8]byte
		binary.BigEndian.PutUint64(due[:], uint64(schedule.DueAtMs))
		hash.Write(due[:])
		var subject [8]byte
		binary.BigEndian.PutUint64(subject[:], schedule.SubjectPlayerID)
		hash.Write(subject[:])
	}
	var result [sha256.Size]byte
	copy(result[:], hash.Sum(nil))
	return result
}

// softFlushCanonicalPayloadHash is a versioned commitment to the complete
// post-turn state plus the work scheduled by that flush. stateDigest is the
// SHA-256 produced while building the exact ActorModelMutation, so hashing it
// is cryptographically equivalent for idempotency while avoiding a second
// traversal of every serialized Player/Farm model field.
func softFlushCanonicalPayloadHash(stateDigest authoritystore.PayloadHash, outcome authoritystore.StoredOperationOutcome, schedules []authoritystore.AttachedSchedule) [sha256.Size]byte {
	hash := sha256.New()
	hash.Write([]byte("AR1-SOFT-FLUSH-V1"))
	hash.Write(stateDigest[:])
	var status [4]byte
	binary.BigEndian.PutUint32(status[:], uint32(outcome.Status))
	hash.Write(status[:])
	writeCanonicalBytes(hash, []byte(outcome.ErrorCode))
	writeCanonicalBytes(hash, outcome.Payload)
	for _, schedule := range schedules {
		writeCanonicalBytes(hash, []byte(schedule.ScheduleID))
		writeCanonicalBytes(hash, []byte(schedule.ScheduleType))
		writeCanonicalBytes(hash, schedule.Payload)
		var generation [8]byte
		binary.BigEndian.PutUint64(generation[:], schedule.Generation)
		hash.Write(generation[:])
		var due [8]byte
		binary.BigEndian.PutUint64(due[:], uint64(schedule.DueAtMs))
		hash.Write(due[:])
		var subject [8]byte
		binary.BigEndian.PutUint64(subject[:], schedule.SubjectPlayerID)
		hash.Write(subject[:])
	}
	var result [sha256.Size]byte
	copy(result[:], hash.Sum(nil))
	return result
}

// casepochCanonicalPayloadHash preserves the historical CASEpoch hash for
// ordinary model-only writes. An order-bearing commit gets a bounded envelope
// hash so a same-version retry with a different order or terminal outcome is a
// conflict, never a false ALREADY_COMMITTED replay.
func casepochCanonicalPayloadHash(data []byte, outcome authoritystore.StoredOperationOutcome, orders []authoritystore.AttachedOrder) [sha256.Size]byte {
	if len(orders) == 0 {
		return sha256.Sum256(data)
	}
	hash := sha256.New()
	hash.Write([]byte("AR1-CASEPOCH-ATTACHED-ORDERS-V1"))
	writeCanonicalBytes(hash, data)
	var status [4]byte
	binary.BigEndian.PutUint32(status[:], uint32(outcome.Status))
	hash.Write(status[:])
	writeCanonicalBytes(hash, []byte(outcome.ErrorCode))
	writeCanonicalBytes(hash, outcome.Payload)
	for _, order := range orders {
		writeCanonicalBytes(hash, []byte(order.OrderID))
		writeCanonicalBytes(hash, []byte(order.OrderType))
		writeCanonicalBytes(hash, order.Payload)
		var due [8]byte
		binary.BigEndian.PutUint64(due[:], uint64(order.DueAtMs))
		hash.Write(due[:])
		var subject [8]byte
		binary.BigEndian.PutUint64(subject[:], order.SubjectPlayerID)
		hash.Write(subject[:])
	}
	var result [sha256.Size]byte
	copy(result[:], hash.Sum(nil))
	return result
}

func writeCanonicalBytes(hash interface{ Write([]byte) (int, error) }, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = hash.Write(length[:])
	_, _ = hash.Write(value)
}

func cloneAttachedOrders(orders []authoritystore.AttachedOrder) []authoritystore.AttachedOrder {
	if len(orders) == 0 {
		return nil
	}
	cloned := make([]authoritystore.AttachedOrder, len(orders))
	for index, order := range orders {
		cloned[index] = order
		cloned[index].Payload = append([]byte(nil), order.Payload...)
	}
	return cloned
}

// Fence 实现 blobstore.EpochStore。ShardWriteFence 已由 LoadAndClaim/Commit
// 在存储侧原子校验,独立的 Fence 调用在 authority 主线中是 no-op。
func (b *Binding) Fence(_ context.Context, id uint64, _ uint64) error {
	return b.checkID(id)
}

// InstallInitial 在"记录已被空 initial 声明但尚无 record 字段"的 stub 上
// 安装初始记录(bootstrap 幂等键,重复安装命中 Receipt 返回 ALREADY_COMMITTED)。
// 返回安装后的 RecordVersion。
func (b *Binding) InstallInitial(ctx context.Context, data []byte, operationID string) (uint64, error) {
	return b.InstallInitialModelState(ctx, authoritystore.ModelState{RecordModelField: append([]byte(nil), data...)}, operationID)
}

// InstallInitialModelState is the named-model variant used by actors whose
// compact read fields must linearize with their initial record.
func (b *Binding) InstallInitialModelState(ctx context.Context, state authoritystore.ModelState, operationID string) (uint64, error) {
	if err := b.unusableError(); err != nil {
		return 0, err
	}
	if b.layout != authoritystore.ModelLayoutV1 && b.layout != authoritystore.ModelLayoutV2 {
		return 0, fmt.Errorf("%w: unknown initial model layout", authoritystore.ErrInvalidRequest)
	}
	if b.layout == authoritystore.ModelLayoutV1 {
		data, ok := state[RecordModelField]
		if !ok || len(data) == 0 {
			return 0, fmt.Errorf("%w: initial record model field is required", authoritystore.ErrInvalidRequest)
		}
	} else if len(state) == 0 {
		// V2 actors have no legacy record field. Their named fields are
		// validated by the AuthorityStore's V2 mutation contract below.
		return 0, fmt.Errorf("%w: initial V2 model state is required", authoritystore.ErrInvalidRequest)
	}
	if b.pending != nil {
		return 0, blobstore.ErrTimeoutUnknown
	}
	key := authoritystore.IdempotencyKey{
		SubjectPlayerID:       b.address.ActorID,
		StableCommandTypeName: "AR1_INITIAL_INSTALL",
		OperationID:           operationID,
		Root: authoritystore.OperationRootRef{
			Kind:        rootKind(b.address.Domain),
			CanonicalID: strconv.FormatUint(b.address.ActorID, 10),
		},
	}
	outcome := authoritystore.StoredOperationOutcome{Status: authoritystore.TerminalSucceeded}
	hash := casepochModelStateCanonicalPayloadHash(state, outcome, nil)
	expectVersion := b.version
	req := authoritystore.AuthorityCommitRequest{
		CommitRequestID:        commitRequestID(key, hash),
		ActorAddress:           b.address,
		AuthorityLayoutVersion: b.provider.layout,
		ShardWriteFence:        b.fence,
		ExpectedWriterClaimID:  b.claimID,
		ExpectedRecordVersion:  expectVersion,
		IdempotencyKey:         key,
		CanonicalPayloadHash:   hash,
		CandidateState:         cloneAuthorityModelState(state),
		Receipt: authoritystore.AuthorityReceipt{
			ReceiptID:            "rcpt_" + key.OperationID,
			IdempotencyKey:       key,
			CanonicalPayloadHash: hash,
			Outcome:              outcome,
		},
		OperationResult: outcome,
	}
	token, err := b.beginDurable(expectVersion + 1)
	if err != nil {
		return 0, err
	}
	result, err := b.commitAuditedClaim(ctx, req)
	if err != nil {
		_ = b.lane.Complete(token, persistence.CompletionNotSent)
		b.invalidatePhysicalUsage()
		return 0, err
	}
	m := &b.provider.metrics
	switch result.Status {
	case authoritystore.CommitCommitted:
		m.CommitOKTotal.Add(1)
		_ = b.lane.Complete(token, persistence.CompletionCommitted)
		b.syncSnapshot(result.Snapshot)
		b.observeRecordBytes(result.Snapshot.ModelFields)
		return b.version, nil
	case authoritystore.CommitAlreadyCommitted:
		m.CommitAlreadyTotal.Add(1)
		_ = b.lane.Complete(token, persistence.CompletionCommitted)
		b.syncSnapshot(result.Snapshot)
		b.observeRecordBytes(result.Snapshot.ModelFields)
		return b.version, nil
	case authoritystore.CommitRecordVersionConflict:
		m.CommitCASConflict.Add(1)
		_ = b.lane.Complete(token, persistence.CompletionNotSent)
		b.invalidatePhysicalUsage()
		return 0, blobstore.ErrCASConflict
	case authoritystore.CommitWriterEpochMismatch, authoritystore.CommitWriterClaimMismatch:
		m.CommitEpochMismatch.Add(1)
		_ = b.lane.Complete(token, persistence.CompletionNotSent)
		b.Retire()
		b.invalidatePhysicalUsage()
		return 0, blobstore.ErrWriterEpochMismatch
	case authoritystore.CommitIdempotencyConflict:
		m.CommitIdemConflict.Add(1)
		_ = b.lane.Complete(token, persistence.CompletionNotSent)
		b.invalidatePhysicalUsage()
		return 0, blobstore.ErrCASConflict
	case authoritystore.CommitUnavailableNotSent:
		m.CommitUnavailable.Add(1)
		_ = b.lane.Complete(token, persistence.CompletionNotSent)
		b.invalidatePhysicalUsage()
		return 0, blobstore.ErrUnavailable
	default:
		m.CommitUnknownTotal.Add(1)
		_ = b.lane.Complete(token, persistence.CompletionTimeoutUnknown)
		b.pending = &pendingCommit{
			key:           key,
			hash:          hash,
			recordVersion: expectVersion,
			beforeDigest:  b.digest,
			laneToken:     token,
		}
		b.invalidatePhysicalUsage()
		return 0, blobstore.ErrTimeoutUnknown
	}
}

// ResolveUnknown 对最近一次 TIMEOUT_UNKNOWN 提交执行 FenceAndClaim(C1→C2):
// 建立线性化切点,旧 Claim 的迟到写自此返回 WRITER_CLAIM_MISMATCH;结论由
// 持久 ResolutionReceipt 固定,同一 Resolver 身份重试幂等。
func (b *Binding) ResolveUnknown(ctx context.Context) (Resolution, error) {
	return b.resolveUnknown(ctx, false)
}

// ResolveUnknownForRetirement is the lifecycle-only, single-attempt settlement
// path for an UNKNOWN retained by a retired generation. Business reads and
// writes remain fenced by ErrBindingRetired; only the exact pending commit may
// execute FenceAndClaim. A retry outcome keeps the tombstone and evidence for a
// later bounded lifecycle call, while every terminal outcome drains the local
// lane so a facts barrier cannot wedge permanently.
func (b *Binding) ResolveUnknownForRetirement(ctx context.Context) (Resolution, error) {
	return b.resolveUnknown(ctx, true)
}

func (b *Binding) resolveUnknown(ctx context.Context, retirement bool) (Resolution, error) {
	if retirement {
		if b == nil || b.provider == nil || b.lane == nil {
			return ResolutionReconcile, errors.New("authority: binding persistence lane unavailable")
		}
		if !b.retired.Load() {
			return ResolutionReconcile, errors.New("authority: lifecycle settlement requires a retired binding")
		}
	} else if err := b.unusableError(); err != nil {
		return ResolutionReconcile, err
	}
	if b.pending == nil {
		b.invalidatePhysicalUsage()
		return ResolutionReconcile, errors.New("authority: no pending unknown commit")
	}
	if b.fenceID == nil {
		b.fenceID = &fenceIdentity{
			resolveOperationID: "resolve_" + b.pending.key.OperationID,
			newWriterClaimID:   "wf_" + shortHash(b.claimID+"|"+b.pending.key.OperationID),
		}
	}
	pending := b.pending
	result, err := b.provider.store.FenceAndClaim(redisworkload.WithComponent(ctx, redisworkload.ComponentActorCommit), authoritystore.FenceAndClaimRequest{
		ResolveOperationID:        b.fenceID.resolveOperationID,
		ActorAddress:              b.address,
		AuthorityLayoutVersion:    b.provider.layout,
		ShardWriteFence:           b.fence,
		OldWriterClaimID:          b.claimID,
		NewWriterClaimID:          b.fenceID.newWriterClaimID,
		OriginalIdempotencyKey:    pending.key,
		CanonicalPayloadHash:      pending.hash,
		BeforeRecordVersion:       pending.recordVersion,
		BeforeBusinessStateDigest: pending.beforeDigest,
	})
	if err != nil {
		b.invalidatePhysicalUsage()
		return ResolutionRetry, err
	}
	m := &b.provider.metrics
	switch result.Status {
	case authoritystore.FenceResolutionCommitted:
		m.ResolveCommitted.Add(1)
		b.adoptFenceIdentity()
		if err := b.lane.ResolveUnknown(pending.laneToken, true); err != nil {
			return ResolutionRetry, err
		}
		b.syncSnapshot(result.Snapshot)
		b.clearPending()
		if retirement {
			if err := b.PrepareForRetirement(); err != nil {
				return ResolutionRetry, err
			}
		}
		return ResolutionCommitted, nil
	case authoritystore.FenceResolutionNotCommitted:
		m.ResolveNotCommitted.Add(1)
		b.adoptFenceIdentity()
		if err := b.lane.ResolveUnknown(pending.laneToken, false); err != nil {
			return ResolutionRetry, err
		}
		b.syncSnapshot(result.Snapshot)
		b.clearPending()
		if retirement {
			if err := b.PrepareForRetirement(); err != nil {
				return ResolutionRetry, err
			}
		}
		return ResolutionNotCommitted, nil
	case authoritystore.FenceResolutionWriterEpochMismatch:
		m.ResolveFenced.Add(1)
		if err := b.retireUnknownGeneration(pending); err != nil {
			return ResolutionFenced, err
		}
		return ResolutionFenced, nil
	case authoritystore.FenceResolutionWriterClaimMismatch:
		// 第三方已接管 Claim:停止当前 Resolver,Fail Closed。
		m.ResolveReconcile.Add(1)
		if err := b.retireUnknownGeneration(pending); err != nil {
			return ResolutionReconcile, err
		}
		return ResolutionReconcile, nil
	case authoritystore.FenceResolutionReconcileRequired:
		m.ResolveReconcile.Add(1)
		if err := b.retireUnknownGeneration(pending); err != nil {
			return ResolutionReconcile, err
		}
		return ResolutionReconcile, nil
	default:
		// UNAVAILABLE_NOT_SENT / TIMEOUT_UNKNOWN:保持隔离,原身份有界重试。
		m.ResolveRetry.Add(1)
		b.invalidatePhysicalUsage()
		return ResolutionRetry, nil
	}
}

// ForgetUnknown 在 Activation 被丢弃(失权/panic/钝化)时清除本地 Unknown
// 上下文;持久 ResolutionReceipt 已在 Store 侧固定结论,重激活后可重新解析。
func (b *Binding) ForgetUnknown() {
	b.pending = nil
	b.fenceID = nil
	b.invalidatePhysicalUsage()
}

func (b *Binding) adoptFenceIdentity() {
	b.claimID = b.fenceID.newWriterClaimID
}

func (b *Binding) clearPending() {
	b.pending = nil
	b.fenceID = nil
}

// retireUnknownGeneration abandons only process-local lane accounting after
// the Store proved that this writer generation can no longer resolve as the
// active claim. It does not classify the durable business outcome; the next
// authority generation loads the durable record. Clearing the local token is
// what lets lifecycle eviction complete without exposing the old snapshot.
func (b *Binding) retireUnknownGeneration(pending *pendingCommit) error {
	b.Retire()
	b.invalidatePhysicalUsage()
	if pending == nil {
		return errors.New("authority: pending unknown commit is required")
	}
	if err := b.lane.ResolveUnknown(pending.laneToken, false); err != nil {
		return err
	}
	b.clearPending()
	return b.PrepareForRetirement()
}

func (b *Binding) syncSnapshot(snapshot authoritystore.AuthoritySnapshot) {
	if snapshot.RecordVersion == 0 {
		b.invalidatePhysicalUsage()
		return
	}
	b.version = snapshot.RecordVersion
	b.digest = snapshot.BusinessStateDigest
	b.layout = snapshot.ModelLayout
	b.models = cloneAuthorityModelState(snapshot.ModelFields)
	b.persistedVersion = snapshot.RecordVersion
	b.persistedModels = cloneAuthorityModelState(snapshot.ModelFields)
	b.softDirty = false
	b.softSchedules = nil
	if snapshot.PhysicalUsage.Validate(snapshot.RecordVersion) != nil {
		b.invalidatePhysicalUsage()
		return
	}
	b.usage = snapshot.PhysicalUsage
}

func (b *Binding) invalidatePhysicalUsage() {
	b.usage = authoritystore.PhysicalUsage{}
}

func (b *Binding) beginDurable(nextVersion uint64) (persistence.Token, error) {
	if err := b.lane.MarkMemoryVersion(nextVersion); err != nil {
		return persistence.Token{}, err
	}
	return b.lane.BeginDurable(nextVersion)
}

func (b *Binding) unusableError() error {
	if b == nil || b.provider == nil {
		return errors.New("authority: nil binding")
	}
	if b.retired.Load() {
		return ErrBindingRetired
	}
	if b.lane == nil {
		return errors.New("authority: binding persistence lane unavailable")
	}
	if b.lane.Snapshot().State == persistence.Draining {
		return persistence.ErrLaneDraining
	}
	return nil
}

func (b *Binding) checkID(id uint64) error {
	if err := b.unusableError(); err != nil {
		return err
	}
	if id == 0 || id != b.address.ActorID {
		return fmt.Errorf("%w: binding actor id", authoritystore.ErrInvalidRequest)
	}
	return nil
}

// commitIdempotencyKey 为"版本 V 的这一次记录 CAS"合成确定幂等键:
// 同 Actor 的版本严格串行,同一 expectVersion 只对应一次候选写,
// 使 TIMEOUT_UNKNOWN 后的 FenceAndClaim/Lookup 能精确定位该次提交。
func commitIdempotencyKey(address authoritystore.ActorAddress, expectVersion uint64) authoritystore.IdempotencyKey {
	return authoritystore.IdempotencyKey{
		SubjectPlayerID:       address.ActorID,
		StableCommandTypeName: stableCommandTypeName,
		OperationID:           "cas_" + strconv.FormatUint(address.ActorID, 10) + "_" + strconv.FormatUint(expectVersion, 10),
		Root: authoritystore.OperationRootRef{
			Kind:        rootKind(address.Domain),
			CanonicalID: strconv.FormatUint(address.ActorID, 10),
		},
	}
}

func rootKind(domain actorrouting.RouteDomain) authoritystore.RootAuthorityKind {
	if domain == actorrouting.DomainFarm {
		return authoritystore.RootKindFarmActor
	}
	return authoritystore.RootKindPlayerActor
}

func commitRequestID(key authoritystore.IdempotencyKey, hash authoritystore.PayloadHash) string {
	return "crc_" + key.OperationID + "_" + hex.EncodeToString(hash[:4])
}

func shortHash(parts string) string {
	sum := sha256.Sum256([]byte(parts))
	return hex.EncodeToString(sum[:8])
}
