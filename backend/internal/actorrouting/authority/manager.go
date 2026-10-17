// Package authority contains the RouteAssignmentManager control-plane seam.
// It depends on the frozen storage contract but never changes that contract or
// the Redis implementation. The package is below internal/actorrouting so the
// route model remains the single model while avoiding an import cycle with
// authoritystore (which itself owns ActorAddress's route-domain type).
package authority

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/holderidentity"
	"farmgame/internal/holderkey"
	"farmgame/internal/routecontrol"
	core "farmgame/internal/scaleplan"
	"farmgame/internal/storage/authoritystore"
)

var (
	ErrInvalidManager            = errors.New("route authority: invalid manager")
	ErrAuthorityCommitUnknown    = errors.New("route authority: authority transition is unknown")
	ErrAuthorityUnavailable      = errors.New("route authority: authority transition was not sent")
	ErrAuthorityRejected         = errors.New("route authority: authority transition rejected")
	ErrSnapshotPublish           = errors.New("route authority: snapshot publish failed")
	ErrTransitionReconcile       = errors.New("route authority: transition identity conflict")
	ErrAuthorityReconcile        = errors.New("route authority: current authority could not be reconciled")
	ErrStaleTransition           = errors.New("route authority: stale transition publication")
	ErrAuthorityLeaseExpired     = errors.New("route authority: committed authority lease is expired")
	ErrHolderRenewOnly           = errors.New("route authority: holder may only renew its current generation")
	ErrRenewGenerationChanged    = errors.New("route authority: holder renewal changed generation")
	ErrTransitionMemoExpired     = errors.New("route authority: transition memo retention expired")
	ErrTransitionTombstoneCap    = errors.New("route authority: transition tombstone capacity reached")
	ErrTransferTakeoverNeeded    = errors.New("route authority: direct transfer requires explicit failure takeover proof")
	ErrAuthorityAddress          = errors.New("route authority: authority result is outside the requested route address")
	ErrAuthorityOverflow         = errors.New("route authority: authority generation overflow")
	ErrAuthorityGeneration       = errors.New("route authority: authority generation did not progress")
	ErrDrainControllerRequired   = errors.New("route authority: drain controller required")
	ErrDrainInFlight             = errors.New("route authority: drain already in flight for shard")
	ErrProjectionRecovery        = errors.New("route authority: pending projection recovery failed")
	ErrLeaseProjectionStale      = errors.New("route authority: lease projection base changed")
	ErrLeaseProjectionInvalid    = errors.New("route authority: lease projection authority mismatch")
	ErrLeaseProjectionTooClose   = errors.New("route authority: lease projection has insufficient freshness")
	ErrLeaseProjectionPending    = errors.New("route authority: prior lease projection publication is pending")
	ErrAuthorityWaveStagePending = errors.New("route authority: authority wave stage is buffered and awaiting durable snapshot commit")
	ErrDeadlineProjection        = errors.New("route authority: renewed deadline projection failed")
)

// SnapshotPublisher is the only output of the manager. A publisher must
// install a whole RouteSnapshot atomically; it must not expose a partial shard
// patch to watchers.
type SnapshotPublisher interface {
	Publish(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error
}

// authoritySnapshotPublisher is the additive write capability created beside
// the dynamic read/watch provider. Composition gives this capability only to
// the manager; publishers without it cannot receive an initial bootstrap or
// committed same-generation Drain activation.
type authoritySnapshotPublisher interface {
	PublishAuthoritySnapshot(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error
}

// authorityWaveSnapshotPublisher publishes one GlobalRevision that may change
// many owned routes. Sliding Drain coalescing uses this instead of OwnerWave.
type authorityWaveSnapshotPublisher interface {
	PublishAuthorityWaveSnapshot(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error
}

// SnapshotPublisherFunc adapts a function to SnapshotPublisher.
type SnapshotPublisherFunc func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error

func (f SnapshotPublisherFunc) Publish(ctx context.Context, token routecontrol.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
	if f == nil {
		return ErrSnapshotPublish
	}
	return f(ctx, token, snapshot)
}

// AuthorityReader is an optional read seam used before retrying a committed
// transition's snapshot publication. The frozen ShardWriteAuthorityStore
// intentionally exposes only transition operations, so production wiring may
// provide a transport-specific read adapter without changing that contract.
type AuthorityReader interface {
	ReadShardWriteAuthority(context.Context, actorrouting.RouteDomain, uint32) (authoritystore.ShardWriteAuthority, bool, error)
}

// AuthorityBatchReader is the optional bounded all-or-nothing deadline read
// seam. A Redis Cluster implementation pipelines exact per-key Lua reads by
// node, avoiding 2048 serial network round trips without weakening any tuple
// validation performed by Manager.
type AuthorityBatchReader interface {
	ReadShardWriteAuthorityBatch(context.Context, []actorrouting.RouteDomain, []uint32) ([]authoritystore.ShardWriteAuthority, []bool, error)
}

// AuthorityIntentReader is the optional stronger recovery seam. Implementors
// must return the transition digest from the same atomic read as Authority.
// A pre-commit ambiguity is never promoted through AuthorityReader alone.
type AuthorityIntentReader interface {
	ReadShardWriteAuthorityObservation(context.Context, actorrouting.RouteDomain, uint32) (authoritystore.ShardWriteAuthorityObservation, bool, error)
}

type AuthorityReaderFunc func(context.Context, actorrouting.RouteDomain, uint32) (authoritystore.ShardWriteAuthority, bool, error)

func (f AuthorityReaderFunc) ReadShardWriteAuthority(ctx context.Context, domain actorrouting.RouteDomain, shard uint32) (authoritystore.ShardWriteAuthority, bool, error) {
	if f == nil {
		return authoritystore.ShardWriteAuthority{}, false, ErrAuthorityReconcile
	}
	return f(ctx, domain, shard)
}

// ManagerOptions identifies the trusted RouteManager and supplies the current
// full snapshot. The caller identity is owned by the composition root; public
// Claim/Transfer/Unassign methods never accept a client-provided caller role.
type ManagerOptions struct {
	Store                  authoritystore.ShardWriteAuthorityStore
	Publisher              SnapshotPublisher
	AuthorityReader        AuthorityReader
	InitialSnapshot        actorrouting.RouteSnapshot
	AuthorityLayoutVersion string
	RouteManagerID         string
	RouteManagerBootID     string
	Now                    func() time.Time
	MemoLimit              int
	MemoRetention          time.Duration
	TombstoneStore         TransitionTombstoneStore
	// PlacementExclusions is the Manager-owned target-admission and mutation
	// capability. Construction fails without it so no Claim/Transfer/Drain,
	// recovery or publication path can bypass exclusion.
	PlacementExclusions routecontrol.PlacementExclusionController
	// InitializationContext bounds authoritative reads needed to distinguish
	// an inert bootstrap placeholder and to reconstruct pre-commit recoveries.
	// Nil preserves API compatibility with a bounded fail-closed timeout.
	InitializationContext context.Context
	// TargetConvergenceTimeout bounds how long Drain/recovery may revalidate the
	// exact target HolderRegistry facts after an ASSIGNING snapshot revision N
	// is published and before ColdLoad/Prepare. Zero keeps a single attempt
	// (legacy/test). Production composition must set a positive bound.
	TargetConvergenceTimeout time.Duration
	// TargetConvergenceBackoff is the pause between revalidation attempts while
	// waiting for authenticated heartbeats to report SnapshotRevision==N.
	// Ignored when TargetConvergenceTimeout<=0. Zero selects a small default.
	TargetConvergenceBackoff time.Duration
	// After is the wait seam used by the convergence barrier. Nil uses time.After.
	// Tests inject a deterministic virtual clock; production must not sleep for
	// correctness outside this seam.
	After func(time.Duration) <-chan time.Time
	// SnapshotCoalesceWindow is the quiet period after the last already-ready
	// Drain waiter. Zero disables coalescing so each transition publishes
	// immediately. Production composition should set
	// DefaultSnapshotCoalesceWindow. The window does not wait for shards still
	// in Stop/Wait/Facts.
	SnapshotCoalesceWindow time.Duration
	// SnapshotCoalesceBatch is the maximum number of same-state Drain
	// mutations merged into one Snapshot CAS. Zero selects
	// DefaultSnapshotCoalesceBatch when coalescing is enabled. A full batch
	// flushes without waiting out the quiet period.
	SnapshotCoalesceBatch int
	// SnapshotCoalesceMaxWait optionally bounds how long the first already-ready
	// waiter may wait for siblings. Zero leaves this bound disabled so a
	// staggered 32-shard Drain can share one Snapshot; the quiet period after
	// the last already-ready waiter still flushes a partial tail. It is not a
	// cross-shard fence and does not wait for shards still in Stop/Wait/Facts.
	SnapshotCoalesceMaxWait time.Duration
	// DisableDeadlineSnapshotPublication keeps the Redis Authority lease
	// projection independent from Assignment Snapshot publication. The
	// default false preserves the historical contract; authority-delegated
	// data-plane consumers may enable it once Holder LeaseGuard is the final
	// admission fence.
	DisableDeadlineSnapshotPublication bool
	// ControlCoordinator is optional for G21 compatibility. When present, all
	// Manager mutation/publication paths require an issuer-bound token in the
	// caller context and verify it against this coordinator before side
	// effects. The top-level RoutePlan/recovery composition acquires it.
	ControlCoordinator core.ControlMutationCoordinator
	ControlScope       core.MutationScope

	// HolderPublicKeys is a process-local, read-only resolver backed by sealed
	// HolderControl verification. Missing keys are a valid mixed-rollout state;
	// prefer consumers remain fail-closed until publication completes.
	HolderPublicKeys HolderPublicKeyResolver
}

const (
	defaultTargetConvergenceBackoff = 50 * time.Millisecond
	// DefaultSnapshotCoalesceWindow is the quiet period after the last
	// already-ready Drain waiter. Authority CAS of empty shards still staggers
	// arrivals by tens to hundreds of milliseconds; this window has to cover
	// that gap or a 32-shard Recover wave publishes ~3 shards per Snapshot.
	// Do not pair a long first-waiter maxWait with this window: that parked 32
	// waiters, then one large CAS plus heartbeat kicks blocked Redis past the
	// 2s Holder timeout. A full batch still flushes immediately. GlobalRevision
	// stays a cursor.
	DefaultSnapshotCoalesceWindow = 400 * time.Millisecond
	// DefaultSnapshotCoalesceBatch matches the production sliding inflight
	// limit: one Snapshot CAS should cover one UC-sized window, not one shard.
	DefaultSnapshotCoalesceBatch = int(core.ProductionMaxRoutePlansInflight)
	// DefaultSnapshotCoalesceMaxWait is kept for tests that need an explicit
	// first-waiter bound. Production leaves SnapshotCoalesceMaxWait at zero so
	// the quiet period after the last sibling can collect a staggered wave.
	DefaultSnapshotCoalesceMaxWait = 500 * time.Millisecond
)

const defaultInitializationReadTimeout = 5 * time.Second

// TransferMode makes the safety boundary explicit. Planned moves must use
// Drain; the public hot path accepts only a crash/failure takeover proof.
type TransferMode uint8

const (
	TransferModeUnspecified TransferMode = iota
	TransferModeCrashTakeover
	// TransferModeDrainHandoff is Manager-internal control-plane evidence that
	// the transition passed Stop/Wait/Facts. It is never accepted by the public
	// direct Transfer path.
	TransferModeDrainHandoff
	// TransferModeWorkerHandoff is the FARM_WORKER planned-move contract. The
	// Manager first publishes the source route as DRAINING and waits for the
	// source Worker heartbeat to acknowledge the new revision with one fewer
	// serving shard. Only then may the normal fenced Authority transfer run.
	TransferModeWorkerHandoff
)

// TransferTakeoverProof is a trusted RouteManager-side statement about why a
// direct takeover is safe. It is not client input and it never enables a live
// hot migration: the old holder must be unavailable or its lease must have
// expired, and the proof must identify the fenced old boot.
type TransferTakeoverProof struct {
	ExpectedHolderBootID string
	HolderUnavailable    bool
	LeaseExpired         bool
	Evidence             string
}

// TransitionTombstone is the bounded replay floor for a memo evicted from the
// hot/recovery maps. A tombstone is terminal: the original transition result
// is returned with ErrTransitionMemoExpired and the transition is never
// executed again.
type TransitionTombstone struct {
	TransitionID string
	Fingerprint  string
	Result       authoritystore.ShardAuthorityTransitionResult
	ExpiredAt    time.Time
}

// transitionStoreStartupContext supplies a context to transition-store calls
// whose enclosing scope has none: the manager constructor and the *Locked
// helpers. Giving those a caller-supplied context is a 41-call-site change that
// reaches internal/testkit/capstaticroute/initializer.go, which section 11.4
// freezes as explicitly not modified. CF-1 records that collision rather than
// deciding it; A1R resolves it.
func transitionStoreStartupContext() context.Context { return context.Background() }

// TransitionTombstoneStore is the terminal durable replay seam. A manager
// requires the paired TransitionRecoveryStore as well; implementations must
// retain exact IDs for their configured bounded retention and return a
// capacity error instead of forgetting an ID silently.
//
// Every method takes a context because section 8.6 promotes
// LoadTransitionTombstones to a source of universal quiescence evidence, and
// that enumeration runs under the coordinator's guard.Context().
type TransitionTombstoneStore interface {
	LoadTransitionTombstones(context.Context) ([]TransitionTombstone, error)
	LookupTransitionTombstone(context.Context, string) (TransitionTombstone, bool, error)
	SaveTransitionTombstone(context.Context, TransitionTombstone) error
}

// TransitionRecovery wire versions are codec observations, not manager state.
// Legacy canonical records predate the explicit AuthorityAttempted field, so a
// decoded false value is not proof that no AuthorityStore call was made.
const (
	TransitionRecoveryWireUnknown uint8 = iota
	TransitionRecoveryWireLegacy
	TransitionRecoveryWireAuthorityAttempted
)

// TransitionRecovery is the non-terminal, recoverable record for an
// Authority mutation whose complete RouteSnapshot projection is not finished
// yet. It deliberately lives beside the manager's bounded replay seam rather
// than in the frozen AuthorityStore contract. A production adapter may persist
// these records with the same durable ID floor; the memory implementation is
// used by component tests.
type TransitionRecovery struct {
	TransitionID              string
	Fingerprint               string
	TransitionKind            uint8
	Domain                    actorrouting.RouteDomain
	VirtualShard              uint32
	ExpectedFence             authoritystore.ShardWriteFence
	NewHolderID               string
	NewHolderBootID           string
	SourcePodUID              string `json:",omitempty"`
	SourceWorkloadUID         string `json:",omitempty"`
	SourceOrdinal             int32  `json:",omitempty"`
	TargetPodUID              string `json:",omitempty"`
	TargetWorkloadUID         string `json:",omitempty"`
	TargetOrdinal             int32  `json:",omitempty"`
	DeploymentIdentityPresent bool   `json:",omitempty"`
	Endpoint                  string
	RequestedTTLms            int64
	Mode                      TransferMode
	TakeoverProof             TransferTakeoverProof
	InternalDrain             bool
	Result                    authoritystore.ShardAuthorityTransitionResult
	LastState                 actorrouting.ShardRouteState
	// PublishedSnapshotRevision is the minimum locally-applied revision that
	// proves the target observed this transition's ASSIGNING projection. It is
	// frozen when that projection publishes; unrelated shard publications must
	// never move this per-shard admission fence forward.
	PublishedSnapshotRevision uint64 `json:",omitempty"`
	BootstrapProjection       bool
	Published                 bool
	ColdLoaded                bool
	AuthorityAttempted        bool `json:"authority_attempted"`
	// AuthorityAttemptedPresent and RecoveryWireVersion are populated by the
	// durable codec. They deliberately are not serialized: current records
	// always carry AuthorityAttempted, while legacy records retain absence as
	// an ambiguity boundary across upgrade.
	AuthorityAttemptedPresent bool  `json:"-"`
	RecoveryWireVersion       uint8 `json:"-"`
	Committed                 bool
	CreatedAt                 time.Time
	UpdatedAt                 time.Time
}

// TransitionRecoveryStore is the manager-owned replay seam paired with the
// terminal tombstone store. Reservations are made before AuthorityStore side
// effects, then committed records keep an incomplete projection recoverable
// across manager restart. Implementations must be bounded and deterministic.
//
// Every method takes a context for the same reason as TransitionTombstoneStore:
// LoadTransitionRecoveries is the other half of the universal quiescence
// assertion in section 8.6.
type TransitionRecoveryStore interface {
	LoadTransitionRecoveries(context.Context) ([]TransitionRecovery, error)
	LookupTransitionRecovery(context.Context, string) (TransitionRecovery, bool, error)
	ReserveTransitionRecovery(context.Context, TransitionRecovery) error
	CommitTransitionRecovery(context.Context, TransitionRecovery) error
	DeleteTransitionRecovery(context.Context, string) error
}

// ExactTransitionEvidenceStore is the bounded, read-only durable observation
// used by Manager verification. Implementations must observe both evidence
// classes at one point in time and reject physical overlap or corruption.
// It deliberately accepts only the already validated exact transition ID.
type ExactTransitionEvidenceStore interface {
	ObserveTransitionEvidence(context.Context, string) (TransitionRecovery, bool, TransitionTombstone, bool, error)
}

// TransitionObservationRequest is the typed Plan-to-Manager verification
// request. PlannedDrain selects the Manager's private Drain normalization;
// direct requests must be exact crash-takeover Transfers.
type TransitionObservationRequest struct {
	Transfer     TransferRequest
	PlannedDrain bool
}

type TransitionEvidenceClass uint8

const (
	TransitionEvidenceUnspecified TransitionEvidenceClass = iota
	TransitionEvidenceCompleteRecovery
	TransitionEvidenceTombstone
)

// TransitionObservation contains only Manager-validated terminal facts. The
// verifier receives the exact durable value selected by Manager, but never
// computes or supplies a fingerprint itself. Exactly one of Recovery and
// Tombstone is populated according to Class.
type TransitionObservation struct {
	Class       TransitionEvidenceClass
	Fingerprint string
	Result      authoritystore.ShardAuthorityTransitionResult
	Recovery    TransitionRecovery
	Tombstone   TransitionTombstone
}

// PendingDrainControllerFactory supplies only the holder-side prepare
// capability needed by a committed internal Drain recovery. Manager owns the
// durable checkpoint and decides whether the factory may be called; callers
// never replay Drain or an AuthorityStore transition themselves.
type PendingDrainControllerFactory func(context.Context, TransitionRecovery) (DrainController, func() error, error)

// ColdLoadPreparedInspector is an optional read-only capability on a recovery
// DrainController. At durable checkpoint 2 it closes the crash window between
// a successful prepare-only RPC and persistence of ColdLoaded. It must return
// true only for the exact, still-fresh Authority generation supplied.
type ColdLoadPreparedInspector interface {
	ColdLoadPrepared(context.Context, actorrouting.RouteDomain, uint32, authoritystore.ShardWriteAuthority) (bool, error)
}

// PendingProjectionTargetValidator is the mandatory read-only production
// seam used immediately before live/recovery target callbacks, Authority
// commits, and target snapshot publication. snapshotRevision is the Manager's
// immutable current revision while it owns that boundary; implementations
// must require the exact target holder/boot/endpoint, positive capacity, a
// fresh lease, an eligible READY state, and the same applied snapshot
// revision. It grants no AuthorityStore capability.
type PendingProjectionTargetValidator func(context.Context, TransitionRecovery, uint64) error

// WorkerHandoffSourceObservation is the exact stop-new-claim fence a planned
// FARM_WORKER move must observe before Authority can leave the source holder.
// The validator is read-only and must prove that the source heartbeat applied
// SnapshotRevision and reduced ServingShardCount to ExpectedServingShards.
type WorkerHandoffSourceObservation struct {
	Domain                actorrouting.RouteDomain
	VirtualShard          uint32
	SourceHolderID        string
	SourceHolderBootID    string
	SnapshotRevision      uint64
	ExpectedServingShards uint32
}

// WorkerHandoffSourceValidator blocks until the exact source observation is
// visible or the supplied context expires. It grants no Authority capability.
type WorkerHandoffSourceValidator func(context.Context, WorkerHandoffSourceObservation) error

// MemoryTransitionTombstoneStore is a bounded, deterministic component store
// for tests and single-process deployments. It intentionally refuses to
// discard an older ID when full; callers then fail closed at the manager.
type MemoryTransitionTombstoneStore struct {
	mu         sync.Mutex
	limit      int
	entries    map[string]TransitionTombstone
	recoveries map[string]TransitionRecovery
}

func NewMemoryTransitionTombstoneStore(limit int) *MemoryTransitionTombstoneStore {
	if limit <= 0 {
		limit = 1024
	}
	return &MemoryTransitionTombstoneStore{limit: limit, entries: make(map[string]TransitionTombstone), recoveries: make(map[string]TransitionRecovery)}
}

func (s *MemoryTransitionTombstoneStore) LoadTransitionTombstones(_ context.Context) ([]TransitionTombstone, error) {
	if s == nil {
		return nil, ErrTransitionTombstoneCap
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entries := make([]TransitionTombstone, 0, len(s.entries))
	for _, entry := range s.entries {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].ExpiredAt.Equal(entries[j].ExpiredAt) {
			return entries[i].TransitionID < entries[j].TransitionID
		}
		return entries[i].ExpiredAt.Before(entries[j].ExpiredAt)
	})
	return entries, nil
}

func (s *MemoryTransitionTombstoneStore) LookupTransitionTombstone(_ context.Context, id string) (TransitionTombstone, bool, error) {
	if s == nil {
		return TransitionTombstone{}, false, ErrTransitionTombstoneCap
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.entries[id]
	return entry, ok, nil
}

func (s *MemoryTransitionTombstoneStore) SaveTransitionTombstone(_ context.Context, entry TransitionTombstone) error {
	if s == nil || entry.TransitionID == "" || entry.Fingerprint == "" {
		return ErrTransitionTombstoneCap
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, exists := s.entries[entry.TransitionID]; exists && existing.Fingerprint != entry.Fingerprint {
		return ErrTransitionReconcile
	}
	_, terminalExists := s.entries[entry.TransitionID]
	_, recoveryExists := s.recoveries[entry.TransitionID]
	if !terminalExists && !recoveryExists && len(s.entries)+len(s.recoveries) >= s.limit {
		return ErrTransitionTombstoneCap
	}
	if recoveryExists {
		delete(s.recoveries, entry.TransitionID)
	}
	s.entries[entry.TransitionID] = entry
	return nil
}

func (s *MemoryTransitionTombstoneStore) LoadTransitionRecoveries(_ context.Context) ([]TransitionRecovery, error) {
	if s == nil {
		return nil, ErrTransitionTombstoneCap
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	recoveries := make([]TransitionRecovery, 0, len(s.recoveries))
	for _, recovery := range s.recoveries {
		recoveries = append(recoveries, recovery)
	}
	sort.Slice(recoveries, func(i, j int) bool {
		if recoveries[i].UpdatedAt.Equal(recoveries[j].UpdatedAt) {
			return recoveries[i].TransitionID < recoveries[j].TransitionID
		}
		return recoveries[i].UpdatedAt.Before(recoveries[j].UpdatedAt)
	})
	return recoveries, nil
}

func (s *MemoryTransitionTombstoneStore) LookupTransitionRecovery(_ context.Context, id string) (TransitionRecovery, bool, error) {
	if s == nil {
		return TransitionRecovery{}, false, ErrTransitionTombstoneCap
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	recovery, ok := s.recoveries[id]
	return recovery, ok, nil
}

func (s *MemoryTransitionTombstoneStore) ReserveTransitionRecovery(_ context.Context, recovery TransitionRecovery) error {
	if s == nil || recovery.TransitionID == "" || recovery.Fingerprint == "" {
		return ErrTransitionTombstoneCap
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, exists := s.recoveries[recovery.TransitionID]; exists {
		if existing.Fingerprint != recovery.Fingerprint {
			return ErrTransitionReconcile
		}
		return nil
	}
	if _, terminal := s.entries[recovery.TransitionID]; terminal {
		return ErrTransitionReconcile
	}
	if len(s.entries)+len(s.recoveries) >= s.limit {
		return ErrTransitionTombstoneCap
	}
	recovery.Committed = false
	s.recoveries[recovery.TransitionID] = recovery
	return nil
}

func (s *MemoryTransitionTombstoneStore) CommitTransitionRecovery(_ context.Context, recovery TransitionRecovery) error {
	if s == nil || recovery.TransitionID == "" || recovery.Fingerprint == "" || !recovery.Committed {
		return ErrTransitionTombstoneCap
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, exists := s.recoveries[recovery.TransitionID]
	if !exists || existing.Fingerprint != recovery.Fingerprint {
		return ErrTransitionReconcile
	}
	s.recoveries[recovery.TransitionID] = recovery
	return nil
}

func (s *MemoryTransitionTombstoneStore) DeleteTransitionRecovery(_ context.Context, id string) error {
	if s == nil {
		return ErrTransitionTombstoneCap
	}
	s.mu.Lock()
	delete(s.recoveries, id)
	s.mu.Unlock()
	return nil
}

func (s *MemoryTransitionTombstoneStore) ObserveTransitionEvidence(ctx context.Context, id string) (TransitionRecovery, bool, TransitionTombstone, bool, error) {
	if s == nil || ctx == nil || id == "" {
		return TransitionRecovery{}, false, TransitionTombstone{}, false, ErrTransitionReconcile
	}
	if err := ctx.Err(); err != nil {
		return TransitionRecovery{}, false, TransitionTombstone{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	recovery, recoveryFound := s.recoveries[id]
	tombstone, tombstoneFound := s.entries[id]
	if recoveryFound && tombstoneFound {
		return TransitionRecovery{}, false, TransitionTombstone{}, false, ErrTransitionReconcile
	}
	return recovery, recoveryFound, tombstone, tombstoneFound, nil
}

// HolderPublicKeyResolver returns only public verification material for one
// exact domain/holder/boot tuple. It must not consult Redis or expose a private
// key. Route Controller binds it to its verified holder view.
type HolderPublicKeyResolver interface {
	ResolveHolderPublicKey(actorrouting.RouteDomain, string, string) (holderkey.Publication, bool, error)
}

type HolderPublicKeyResolverFunc func(actorrouting.RouteDomain, string, string) (holderkey.Publication, bool, error)

func (f HolderPublicKeyResolverFunc) ResolveHolderPublicKey(domain actorrouting.RouteDomain, holderID, bootID string) (holderkey.Publication, bool, error) {
	if f == nil {
		return holderkey.Publication{}, false, nil
	}
	return f(domain, holderID, bootID)
}

// ClaimRequest is a RouteManager-only new assignment generation.
type ClaimRequest struct {
	TransitionID    string
	Domain          actorrouting.RouteDomain
	VirtualShard    uint32
	NewHolderID     string
	NewHolderBootID string
	Endpoint        string
	RequestedTTL    time.Duration
}

// TransferRequest moves one existing authority generation to a new holder
// generation. ExpectedFence is the old authority tuple observed by the
// RouteManager before it starts the transfer.
type TransferRequest struct {
	TransitionID      string
	Domain            actorrouting.RouteDomain
	VirtualShard      uint32
	ExpectedFence     authoritystore.ShardWriteFence
	NewHolderID       string
	NewHolderBootID   string
	SourcePodUID      string
	SourceWorkloadUID string
	SourceOrdinal     int32
	TargetPodUID      string
	TargetWorkloadUID string
	TargetOrdinal     int32
	// DeploymentIdentityPresent is interpreted together with Mode:
	//   - planned drain: both source and target identities must be exact;
	//   - crash takeover: the source identity must be zero and the target
	//     identity must be exact.
	// This lets a dead source be replaced without weakening the target Pod
	// generation fence.
	DeploymentIdentityPresent bool
	Endpoint                  string
	RequestedTTL              time.Duration
	Mode                      TransferMode
	TakeoverProof             TransferTakeoverProof
}

// UnassignRequest removes the current data-plane holder. The route snapshot
// published after the authority commit is non-active, so no new activation or
// turn can enter the old holder.
type UnassignRequest struct {
	TransitionID  string
	Domain        actorrouting.RouteDomain
	VirtualShard  uint32
	ExpectedFence authoritystore.ShardWriteFence
}

// RenewRequest is the one holder-side authority operation. The manager
// rejects RouteManager/unknown callers here and never changes a generation or
// publishes a new assignment from Renew.
type RenewRequest struct {
	Caller        authoritystore.TrustedShardAuthorityCaller
	Domain        actorrouting.RouteDomain
	VirtualShard  uint32
	ExpectedFence authoritystore.ShardWriteFence
	RequestedTTL  time.Duration
}

// LeaseProjectionResult is one complete, authoritative read wave. Observed is
// always the exact number of shards checked. A successful no-change wave still
// proves freshness without inventing a GlobalRevision.
type LeaseProjectionResult struct {
	Observed            int
	Changed             int
	Published           bool
	GlobalRevision      uint64
	MinimumLeaseUntilMs int64
	CompletedAt         time.Time
}

type pendingLeaseProjection struct {
	base     actorrouting.RouteSnapshot
	snapshot actorrouting.RouteSnapshot
	result   LeaseProjectionResult
}

// RouteAssignmentManager is the sole transition coordinator for a route
// domain. It serializes transitions with snapshot publication so the local
// route never advances before the corresponding ShardWriteAuthority.
type RouteAssignmentManager struct {
	store                    authoritystore.ShardWriteAuthorityStore
	publisher                SnapshotPublisher
	layout                   string
	caller                   authoritystore.TrustedShardAuthorityCaller
	reader                   AuthorityReader
	now                      func() time.Time
	memoLimit                int
	retention                time.Duration
	tombstones               TransitionTombstoneStore
	placementExclusions      routecontrol.PlacementExclusionController
	targetConvergenceTimeout time.Duration
	targetConvergenceBackoff time.Duration
	after                    func(time.Duration) <-chan time.Time
	controlCoordinator       core.ControlMutationCoordinator
	controlScope             core.MutationScope

	publicKeyMu      sync.RWMutex
	holderPublicKeys HolderPublicKeyResolver

	mu sync.Mutex
	// placementMu excludes target admission changes from complete placement
	// effect waves while preserving the existing G3 operation serialization.
	placementMu            sync.RWMutex
	snapshot               actorrouting.RouteSnapshot
	transitions            map[string]*transitionMemo
	recoveries             map[string]*transitionMemo
	tombstone              map[string]TransitionTombstone
	committed              map[authorityKey]authoritystore.ShardWriteAuthority
	bootstrap              map[authorityKey]bool
	drains                 map[drainKey]DrainStage
	drainIDs               map[drainKey]string
	drainFlights           map[drainKey]*drainFlight
	reservations           map[string]*transitionReservation
	reservedSlots          int
	pendingLeaseProjection *pendingLeaseProjection
	deadlineNotifyPending  *actorrouting.RouteSnapshot
	// leaseProjection is the latest same-generation Authority observation. It
	// is intentionally separate from snapshot when deadline-only publication is
	// disabled; Redis remains authoritative and this map is only a local view.
	leaseProjection         map[authorityKey]authoritystore.ShardWriteAuthority
	disableDeadlineSnapshot bool
	// initialBatchOpen keeps FARM_WORKER HolderIndex values byte-for-byte stable
	// while the startup buffer accumulates complete PLAYER/FARM snapshots. It is
	// sealed by EndInitialAuthorityBatch and is never reopened on this Manager.
	initialBatchOpen   bool
	initialBatchSealed bool
	// operationMu excludes Snapshot publication from every Manager-owned
	// generation mutation/recovery while preserving existing transition
	// concurrency outside a publication. ProjectFreshLeases keeps the exclusive
	// boundary for its complete projection. ReconcileRenewedAuthorityDeadlines
	// performs its potentially large Authority read wave without this lock, then
	// acquires it and revalidates the captured Snapshot only when publication is
	// necessary. Neither path holds the state mutex mu across Authority I/O.
	operationMu sync.RWMutex
	// projectionRecovery is a context-aware single-owner token. It serializes
	// durable recovery passes without introducing leadership or another state
	// machine; transition generation remains exclusively under this Manager.
	projectionRecovery      chan struct{}
	snapshotCoalesceWindow  time.Duration
	snapshotCoalesceBatch   int
	snapshotCoalesceMaxWait time.Duration
	snapshotCoalesceCond    *sync.Cond
	snapshotFlushWake       chan struct{}
	pendingSnapshotWaiters  []*snapshotCoalesceWaiter
	snapshotFlushScheduled  bool
	snapshotFlushInFlight   bool
}

type transitionMemo struct {
	fingerprint               string
	result                    authoritystore.ShardAuthorityTransitionResult
	request                   transitionRequest
	lastState                 actorrouting.ShardRouteState
	publishedSnapshotRevision uint64
	bootstrapProjection       bool
	published                 bool
	projectionPending         bool
	coldLoaded                bool
	authorityAttempted        bool
	commitAmbiguous           bool
	blocked                   bool
	blockErr                  error
	createdAt                 time.Time
	lastUsedAt                time.Time
	inFlight                  bool
}

// transitionReservation is created before the AuthorityStore call. It holds
// one bounded memo slot while a Drain is between its local side effects and
// the authority transition, so a later capacity failure cannot occur after
// authority has already advanced.
type transitionReservation struct {
	fingerprint string
	createdAt   time.Time
}

type drainFlight struct {
	key          drainKey
	transitionID string
	fingerprint  string
	done         chan struct{}
	result       authoritystore.ShardAuthorityTransitionResult
	err          error
}

type authorityKey struct {
	domain actorrouting.RouteDomain
	shard  uint32
}

type committedSnapshotPublishError interface {
	SnapshotCommitted() bool
}

func snapshotPublishWasCommitted(err error) bool {
	var committed committedSnapshotPublishError
	return errors.As(err, &committed) && committed.SnapshotCommitted()
}

type deadlineProjectionRoute struct {
	key                  authorityKey
	holderID             string
	holderBootID         string
	assignmentRevision   uint64
	writerEpoch          uint64
	leaseEpoch           uint64
	leaseUntilMs         int64
	expectedTransitionID string
}

// DeadlineProjectionResult is a read-only outcome of one bounded refresh pass.
// A changed pass publishes exactly one complete Snapshot revision regardless
// of how many ACTIVE shard deadlines advanced.
type DeadlineProjectionResult struct {
	Changed       bool
	Published     bool
	ChangedShards int
	// StalledShards counts ACTIVE routes whose durable Authority has already
	// expired. Their holder is gone until a takeover runs, so the pass leaves
	// their published deadline untouched and refreshes every other shard.
	StalledShards    int
	SnapshotRevision uint64
	MinRemainingMs   int64
	ProjectionLagMs  int64
	// SnapshotBytes is the canonical serialized size of the complete snapshot
	// written by this pass. It is zero when no snapshot was published.
	SnapshotBytes int
	// PublicationAttempted becomes true immediately before the only durable
	// Snapshot write in this pass. A false value is a positive no-write proof
	// even when cancellation or validation returned an error; callers may then
	// safely resolve their top-level control token. Once true, only Changed can
	// prove that the publication committed.
	PublicationAttempted bool
	FailureStage         string
	NotificationReplayed bool
}

type transitionRequest struct {
	kind                      transitionKind
	transitionID              string
	domain                    actorrouting.RouteDomain
	virtualShard              uint32
	expectedFence             authoritystore.ShardWriteFence
	newHolderID               string
	newHolderBoot             string
	sourcePodUID              string
	sourceWorkloadUID         string
	sourceOrdinal             int32
	targetPodUID              string
	targetWorkloadUID         string
	targetOrdinal             int32
	deploymentIdentityPresent bool
	endpoint                  string
	ttl                       time.Duration
	mode                      TransferMode
	proof                     TransferTakeoverProof
	internalDrain             bool
}

type transitionKind uint8

const (
	transitionClaim transitionKind = iota + 1
	transitionTransfer
	transitionUnassign
)

// NewRouteAssignmentManager validates the initial complete route table before
// any authority RPC can be attempted.
func NewRouteAssignmentManager(opts ManagerOptions) (*RouteAssignmentManager, error) {
	if opts.Store == nil || opts.Publisher == nil || opts.TombstoneStore == nil || opts.AuthorityLayoutVersion == "" || opts.RouteManagerID == "" {
		return nil, fmt.Errorf("%w: store/publisher/tombstone store/layout/route manager identity required", ErrInvalidManager)
	}
	if _, ok := opts.TombstoneStore.(TransitionRecoveryStore); !ok {
		return nil, fmt.Errorf("%w: bounded transition recovery store required", ErrInvalidManager)
	}
	if opts.PlacementExclusions == nil {
		return nil, fmt.Errorf("%w: mandatory placement exclusion controller required", ErrInvalidManager)
	}
	if opts.ControlCoordinator != nil {
		if err := opts.ControlScope.Validate(); err != nil {
			return nil, fmt.Errorf("%w: control scope: %v", ErrInvalidManager, err)
		}
	}
	if _, err := actorrouting.NewStaticResolver(opts.InitialSnapshot); err != nil {
		return nil, fmt.Errorf("%w: initial snapshot: %v", ErrInvalidManager, err)
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.MemoLimit <= 0 {
		opts.MemoLimit = 1024
	}
	if opts.MemoRetention <= 0 {
		opts.MemoRetention = 15 * time.Minute
	}
	initializationCtx := opts.InitializationContext
	cancelInitialization := func() {}
	if initializationCtx == nil {
		initializationCtx, cancelInitialization = context.WithTimeout(context.Background(), defaultInitializationReadTimeout)
	}
	defer cancelInitialization()
	if err := initializationCtx.Err(); err != nil {
		return nil, fmt.Errorf("%w: initialization authority read: %w", ErrInvalidManager, err)
	}
	after := opts.After
	if after == nil {
		after = time.After
	}
	convergenceBackoff := opts.TargetConvergenceBackoff
	if opts.TargetConvergenceTimeout > 0 && convergenceBackoff <= 0 {
		convergenceBackoff = defaultTargetConvergenceBackoff
	}
	manager := &RouteAssignmentManager{
		store:                    opts.Store,
		publisher:                opts.Publisher,
		layout:                   opts.AuthorityLayoutVersion,
		caller:                   authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerRouteManager, InstanceID: opts.RouteManagerID, BootID: opts.RouteManagerBootID},
		reader:                   opts.AuthorityReader,
		now:                      opts.Now,
		memoLimit:                opts.MemoLimit,
		retention:                opts.MemoRetention,
		tombstones:               opts.TombstoneStore,
		placementExclusions:      opts.PlacementExclusions,
		targetConvergenceTimeout: opts.TargetConvergenceTimeout,
		targetConvergenceBackoff: convergenceBackoff,
		after:                    after,
		controlCoordinator:       opts.ControlCoordinator,
		controlScope:             opts.ControlScope,
		holderPublicKeys:         opts.HolderPublicKeys,
		snapshot:                 actorrouting.CloneRouteSnapshot(opts.InitialSnapshot),
		transitions:              make(map[string]*transitionMemo),
		recoveries:               make(map[string]*transitionMemo),
		tombstone:                make(map[string]TransitionTombstone),
		committed:                make(map[authorityKey]authoritystore.ShardWriteAuthority),
		bootstrap:                make(map[authorityKey]bool),
		drains:                   make(map[drainKey]DrainStage),
		drainIDs:                 make(map[drainKey]string),
		drainFlights:             make(map[drainKey]*drainFlight),
		reservations:             make(map[string]*transitionReservation),
		projectionRecovery:       make(chan struct{}, 1),
		leaseProjection:          make(map[authorityKey]authoritystore.ShardWriteAuthority),
		disableDeadlineSnapshot:  opts.DisableDeadlineSnapshotPublication,
	}
	window := opts.SnapshotCoalesceWindow
	if window < 0 {
		window = 0
	}
	batch := opts.SnapshotCoalesceBatch
	if batch <= 0 {
		batch = DefaultSnapshotCoalesceBatch
	}
	maxWait := opts.SnapshotCoalesceMaxWait
	if maxWait < 0 {
		maxWait = 0
	}
	manager.snapshotCoalesceWindow = window
	manager.snapshotCoalesceBatch = batch
	manager.snapshotCoalesceMaxWait = maxWait
	manager.snapshotCoalesceCond = sync.NewCond(&manager.mu)
	manager.snapshotFlushWake = make(chan struct{}, 1)
	if opts.TombstoneStore != nil {
		entries, err := opts.TombstoneStore.LoadTransitionTombstones(transitionStoreStartupContext())
		if err != nil {
			return nil, fmt.Errorf("%w: load transition tombstones: %v", ErrInvalidManager, err)
		}
		sort.Slice(entries, func(i, j int) bool {
			if entries[i].ExpiredAt.Equal(entries[j].ExpiredAt) {
				return entries[i].TransitionID < entries[j].TransitionID
			}
			return entries[i].ExpiredAt.Before(entries[j].ExpiredAt)
		})
		if len(entries) > opts.MemoLimit {
			entries = entries[len(entries)-opts.MemoLimit:]
		}
		for _, entry := range entries {
			if entry.TransitionID == "" || entry.Fingerprint == "" {
				return nil, fmt.Errorf("%w: malformed transition tombstone", ErrInvalidManager)
			}
			manager.tombstone[entry.TransitionID] = entry
		}
		if recoveryStore, ok := opts.TombstoneStore.(TransitionRecoveryStore); ok {
			recoveries, err := recoveryStore.LoadTransitionRecoveries(transitionStoreStartupContext())
			if err != nil {
				return nil, fmt.Errorf("%w: load transition recoveries: %v", ErrInvalidManager, err)
			}
			for _, recovery := range recoveries {
				if recovery.TransitionID == "" || recovery.Fingerprint == "" {
					return nil, fmt.Errorf("%w: malformed transition recovery", ErrInvalidManager)
				}
				request, err := requestFromRecovery(recovery)
				if err != nil {
					return nil, fmt.Errorf("%w: transition recovery %s: %v", ErrInvalidManager, recovery.TransitionID, err)
				}
				authorityCommitted := recoveryAuthorityCommitted(recovery)
				explicitlyUnattempted := recovery.RecoveryWireVersion >= TransitionRecoveryWireAuthorityAttempted &&
					recovery.AuthorityAttemptedPresent && !recovery.AuthorityAttempted
				if !authorityCommitted && explicitlyUnattempted {
					if err := manager.requireControlToken(initializationCtx); err != nil {
						return nil, fmt.Errorf("%w: transition recovery write token: %v", ErrInvalidManager, err)
					}
					// The durable record proves the Manager crashed before its
					// Authority call boundary. It is safe to release only after the
					// durable delete succeeds; a failed cleanup aborts startup so the
					// single owner can retry without losing the ID floor.
					if err := recoveryStore.DeleteTransitionRecovery(transitionStoreStartupContext(), recovery.TransitionID); err != nil {
						return nil, fmt.Errorf("%w: delete unattempted transition recovery %s: %v", ErrInvalidManager, recovery.TransitionID, err)
					}
					continue
				}
				if recovery.BootstrapProjection {
					manager.bootstrap[authorityKey{domain: request.domain, shard: request.virtualShard}] = true
				}
				promotedCommit := false
				var ambiguityErr error
				if !authorityCommitted {
					// A crash or a transient recovery-store failure may leave the
					// pre-Authority reservation durable even though the Authority
					// mutation committed. Reconcile by transition identity when an
					// authoritative read is available; otherwise retain the record
					// blocked rather than guessing from manager-local state.
					if result, recovered, recoverErr := manager.recoverCommittedTransitionLocked(initializationCtx, recovery, request); recovered {
						recovery.Result = result
						recovery.Committed = true
						recovery.AuthorityAttempted = true
						recovery.AuthorityAttemptedPresent = true
						recovery.RecoveryWireVersion = TransitionRecoveryWireAuthorityAttempted
						authorityCommitted = true
						promotedCommit = true
						if recovery.InternalDrain && recovery.Mode == TransferModeDrainHandoff {
							recovery.LastState = actorrouting.ShardStateAssigning
						} else {
							recovery.LastState = routeStateFor(request.kind)
						}
					} else {
						ambiguityErr = recoverErr
					}
					if err := initializationCtx.Err(); err != nil {
						return nil, fmt.Errorf("%w: recover transition %s: %w", ErrInvalidManager, recovery.TransitionID, err)
					}
				}
				// Published is an intent/evidence bit, not permission to trust a
				// snapshot that is not durably present. A migration-wave publisher
				// may acknowledge an in-process buffered projection before its one
				// final batch CAS. On restart, exact current Snapshot facts demote
				// that record back to pending so ordinary recovery can safely replay.
				published := recovery.Published
				if authorityCommitted && request.kind != transitionUnassign {
					// Exact Snapshot facts promote an ambiguous publication. A mismatch
					// demotes only when the current Authority is still this transition;
					// an older published transition may legitimately have been
					// superseded by a later generation.
					snapshotExact := routeMatchesAuthority(manager.snapshot, recovery.Result.Authority, recovery.LastState, request.endpoint)
					snapshotGenerationExact := routeMatchesAuthorityGeneration(manager.snapshot, recovery.Result.Authority, recovery.LastState, request.endpoint)
					if snapshotExact || snapshotGenerationExact {
						// LeaseUntilMs may advance independently after the exact shard
						// generation was published. The Snapshot still proves publication
						// when every generation field, route state and endpoint match; the
						// deadline projector will catch the liveness cursor up separately.
						published = true
					} else if recovery.Published && manager.reader != nil {
						current, found, readErr := manager.reader.ReadShardWriteAuthority(initializationCtx, request.domain, request.virtualShard)
						if readErr != nil {
							return nil, fmt.Errorf("%w: reconcile published transition %s: %v", ErrInvalidManager, recovery.TransitionID, readErr)
						}
						if found && current.LastTransitionID == recovery.TransitionID &&
							current.HolderID == recovery.Result.Authority.HolderID && current.HolderBootID == recovery.Result.Authority.HolderBootID &&
							current.AssignmentRevision == recovery.Result.Authority.AssignmentRevision &&
							current.WriterEpoch == recovery.Result.Authority.WriterEpoch && current.LeaseEpoch == recovery.Result.Authority.LeaseEpoch {
							published = false
						}
					}
				}
				publicationReconciled := published != recovery.Published
				if published && recovery.LastState == actorrouting.ShardStateAssigning && recovery.PublishedSnapshotRevision == 0 {
					// A publish may have committed before its recovery-marker update.
					// The exact current ASSIGNING route proves publication; using the
					// current revision is conservative when later unrelated shards
					// already advanced the global distribution cursor.
					recovery.PublishedSnapshotRevision = manager.snapshot.GlobalRevision
					publicationReconciled = true
				}
				recovery.Published = published
				projectionComplete := published && (recovery.LastState == actorrouting.ShardStateActive || request.kind == transitionUnassign)
				memo := &transitionMemo{
					fingerprint: recovery.Fingerprint, result: recovery.Result, request: request,
					lastState: recovery.LastState, publishedSnapshotRevision: recovery.PublishedSnapshotRevision,
					bootstrapProjection: recovery.BootstrapProjection, published: published,
					projectionPending: authorityCommitted && !projectionComplete,
					coldLoaded:        recovery.ColdLoaded, authorityAttempted: recovery.AuthorityAttempted,
					commitAmbiguous: !authorityCommitted, createdAt: recovery.CreatedAt,
					lastUsedAt: recovery.UpdatedAt,
				}
				if memo.createdAt.IsZero() {
					memo.createdAt = opts.Now()
				}
				if memo.lastUsedAt.IsZero() {
					memo.lastUsedAt = memo.createdAt
				}
				if !authorityCommitted && errors.Is(ambiguityErr, ErrStaleTransition) {
					memo.blocked = true
					memo.blockErr = ambiguityErr
				}
				manager.transitions[recovery.TransitionID] = memo
				if authorityCommitted {
					manager.committed[authorityKey{domain: request.domain, shard: request.virtualShard}] = recovery.Result.Authority
				}
				if promotedCommit || publicationReconciled {
					if err := manager.requireControlToken(initializationCtx); err != nil {
						return nil, fmt.Errorf("%w: transition recovery write token: %v", ErrInvalidManager, err)
					}
					if err := manager.commitRecoveryLocked(memo); err != nil {
						return nil, fmt.Errorf("%w: persist recovered transition %s: %v", ErrInvalidManager, recovery.TransitionID, err)
					}
				}
			}
		}
	}
	for _, domain := range managedSnapshotDomains(manager.snapshot) {
		routes := snapshotDomain(manager.snapshot, domain)
		for shard := uint32(0); shard < routes.ShardCount; shard++ {
			key := authorityKey{domain: domain, shard: shard}
			if _, recoveryLoaded := manager.transitionsForKeyLocked(key); recoveryLoaded {
				continue
			}
			initial, readErr := manager.isInitialBootstrapPlaceholder(initializationCtx, key, routes.Routes[shard])
			if readErr != nil {
				return nil, fmt.Errorf("%w: bootstrap authority read %s/%d: %w", ErrInvalidManager, domain, shard, readErr)
			}
			if initial {
				// A non-active placeholder may be projected by the first Claim
				// only when an authoritative read proves that no generation has
				// committed yet. A restart with an ASSIGNING projection therefore
				// cannot silently regain bootstrap semantics.
				manager.bootstrap[key] = true
			}
		}
	}
	return manager, nil
}

// controlToken verifies the inherited token at each Manager boundary. With no
// coordinator it deliberately returns the zero token so the G21 publisher and
// stores retain their historical no-op fence behavior.
func (m *RouteAssignmentManager) controlToken(ctx context.Context) (routecontrol.NestedControlOperationToken, error) {
	if m == nil || ctx == nil {
		return routecontrol.NestedControlOperationToken{}, ErrInvalidManager
	}
	return verifyControlOperationToken(ctx, m.controlCoordinator, m.controlScope)
}

func (m *RouteAssignmentManager) requireControlToken(ctx context.Context) error {
	_, err := m.controlToken(ctx)
	return err
}

// publishSnapshot is the sole Manager-to-publisher write seam. It revalidates
// the inherited token immediately before the durable CAS, so a coordinator
// expiry cannot be hidden by a token that was valid at method entry.
func (m *RouteAssignmentManager) publishSnapshot(ctx context.Context, snapshot actorrouting.RouteSnapshot, authority bool) error {
	return m.publishSnapshotKind(ctx, snapshot, authority, false)
}

func (m *RouteAssignmentManager) publishSnapshotKind(ctx context.Context, snapshot actorrouting.RouteSnapshot, authority, wave bool) error {
	token, err := m.controlToken(ctx)
	if err != nil {
		return err
	}
	if wave {
		publisher, ok := m.publisher.(authorityWaveSnapshotPublisher)
		if !ok {
			return fmt.Errorf("%w: publisher lacks authority wave path", ErrSnapshotPublish)
		}
		return publisher.PublishAuthorityWaveSnapshot(ctx, token, snapshot)
	}
	if authority {
		publisher, ok := m.publisher.(authoritySnapshotPublisher)
		if !ok {
			return fmt.Errorf("%w: publisher lacks authority activation path", ErrSnapshotPublish)
		}
		return publisher.PublishAuthoritySnapshot(ctx, token, snapshot)
	}
	return m.publisher.Publish(ctx, token, snapshot)
}

func (m *RouteAssignmentManager) transitionsForKeyLocked(key authorityKey) (string, bool) {
	if m == nil {
		return "", false
	}
	for id, memo := range m.transitions {
		if memo != nil && memo.request.domain == key.domain && memo.request.virtualShard == key.shard {
			return id, true
		}
	}
	for id, memo := range m.recoveries {
		if memo != nil && memo.request.domain == key.domain && memo.request.virtualShard == key.shard {
			return id, true
		}
	}
	return "", false
}

func (m *RouteAssignmentManager) isInitialBootstrapPlaceholder(ctx context.Context, key authorityKey, route actorrouting.ShardRoute) (bool, error) {
	if m == nil || route.State != actorrouting.ShardStateAssigning || route.ShardRevision != 1 || route.ExpectedWriterEpoch != 1 || route.ExpectedLeaseEpoch != 1 {
		return false, nil
	}
	// The frozen snapshot has no explicit "never committed" bit. The only
	// safe way to distinguish the initial placeholder from a restarted stale
	// projection is an authoritative absence read. Without that read seam the
	// manager fails closed instead of treating every ASSIGNING route as fresh.
	authority, found, err := m.readAuthorityLocked(ctx, key)
	if err == nil {
		return !found || authority.AssignmentRevision == 0, nil
	}
	if ctx != nil && ctx.Err() != nil {
		return false, ctx.Err()
	}
	// An opaque adapter cannot prove committed state. The frozen snapshot has
	// no explicit bootstrap bit, so accepting the placeholder without an
	// authoritative absence read would make a restarted failed publication
	// indistinguishable from first bootstrap. Fail closed in that case.
	return false, nil
}

// recoverCommittedTransitionLocked resolves a durable pre-commit ambiguity
// without replaying Claim/Transfer/Unassign. Promotion requires one atomic
// observation carrying both the exact Authority tuple and the digest of the
// complete original intent. Absence remains ambiguous: it is not proof that a
// timed-out command cannot still have committed.
func (m *RouteAssignmentManager) recoverCommittedTransitionLocked(ctx context.Context, recovery TransitionRecovery, request transitionRequest) (authoritystore.ShardAuthorityTransitionResult, bool, error) {
	if m == nil {
		return authoritystore.ShardAuthorityTransitionResult{}, false, ErrInvalidManager
	}
	key := authorityKey{domain: request.domain, shard: request.virtualShard}
	observation, found, err := m.readAuthorityObservationLocked(ctx, key)
	if err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, false, fmt.Errorf("%w: intent observation: %v", ErrAuthorityCommitUnknown, err)
	}
	if !found {
		return authoritystore.ShardAuthorityTransitionResult{}, false, ErrAuthorityCommitUnknown
	}
	authority := observation.Authority
	if authority.LastTransitionID != request.transitionID {
		return authoritystore.ShardAuthorityTransitionResult{}, false, fmt.Errorf("%w: transition_id=%s current_transition_id=%s", ErrStaleTransition, request.transitionID, authority.LastTransitionID)
	}
	wantDigest := transitionIntentDigest(m.layout, request, recovery.RequestedTTLms)
	if observation.LastTransitionDigest != wantDigest {
		return authoritystore.ShardAuthorityTransitionResult{}, false, fmt.Errorf("%w: transition_id=%s intent digest mismatch", ErrStaleTransition, request.transitionID)
	}
	result := recovery.Result
	// A pre-commit reservation does not know whether the original adapter
	// response was APPLIED or ALREADY_APPLIED. Use the canonical first-commit
	// status for a recovered result; the authority tuple remains the source of
	// truth and no second mutation is possible. Recover the original accepted
	// TTL from the authority transition timestamp when available so replay is
	// deterministic for the component/store contract.
	result.Status = authoritystore.ShardTransitionApplied
	result.Authority = authority
	if authority.State == authoritystore.ShardAuthorityActive {
		remaining := authority.LeaseUntilMs - m.now().UnixMilli()
		if remaining <= 0 {
			return authoritystore.ShardAuthorityTransitionResult{}, false, ErrAuthorityLeaseExpired
		}
		accepted := authority.LeaseUntilMs - authority.LastTransitionAtMs
		if accepted <= 0 {
			accepted = recovery.RequestedTTLms
		}
		if accepted <= 0 {
			return authoritystore.ShardAuthorityTransitionResult{}, false, ErrAuthorityReconcile
		}
		result.AcceptedTTLms = accepted
	} else {
		result.AcceptedTTLms = 0
	}
	if err := m.validateCommittedAuthorityLocked(request, result); err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, false, err
	}
	return result, true, nil
}

func (m *RouteAssignmentManager) promoteAmbiguousMemoLocked(ctx context.Context, memo *transitionMemo) error {
	if memo == nil || !memo.commitAmbiguous {
		return nil
	}
	recovery := recoveryForRequest(memo.request, memo.fingerprint, memo.result, memo.createdAt)
	result, recovered, err := m.recoverCommittedTransitionLocked(ctx, recovery, memo.request)
	if !recovered {
		if errors.Is(err, ErrStaleTransition) {
			memo.blocked = true
			memo.blockErr = err
		}
		if err == nil {
			err = ErrAuthorityCommitUnknown
		}
		return err
	}
	previousResult, previousState := memo.result, memo.lastState
	previousPending, previousInFlight, previousUsedAt := memo.projectionPending, memo.inFlight, memo.lastUsedAt
	memo.result = result
	if memo.request.internalDrain {
		memo.lastState = actorrouting.ShardStateAssigning
	} else {
		memo.lastState = routeStateFor(memo.request.kind)
	}
	memo.projectionPending = true
	memo.inFlight = false
	memo.lastUsedAt = m.now()
	if err := m.commitRecoveryReleasingLock(memo); err != nil {
		memo.result, memo.lastState = previousResult, previousState
		memo.projectionPending, memo.inFlight, memo.lastUsedAt = previousPending, previousInFlight, previousUsedAt
		return fmt.Errorf("%w: persist reconciled commit: %v", ErrSnapshotPublish, err)
	}
	memo.commitAmbiguous = false
	m.consumeTransitionReservationLocked(memo.request.transitionID, memo.fingerprint)
	delete(m.bootstrap, authorityKey{domain: memo.request.domain, shard: memo.request.virtualShard})
	m.committed[authorityKey{domain: memo.request.domain, shard: memo.request.virtualShard}] = result.Authority
	return nil
}

func (m *RouteAssignmentManager) retainAmbiguousTransitionLocked(request transitionRequest, fingerprint string, result authoritystore.ShardAuthorityTransitionResult, state actorrouting.ShardRouteState) *transitionMemo {
	now := m.now()
	memo := m.transitionMemoLocked(request.transitionID)
	if memo == nil {
		memo = &transitionMemo{fingerprint: fingerprint, request: request, createdAt: now}
		m.transitions[request.transitionID] = memo
	}
	memo.result = result
	memo.request = request
	memo.lastState = state
	memo.bootstrapProjection = memo.bootstrapProjection || m.bootstrap[authorityKey{domain: request.domain, shard: request.virtualShard}]
	memo.published = false
	memo.projectionPending = false
	memo.authorityAttempted = true
	memo.commitAmbiguous = true
	memo.inFlight = false
	memo.lastUsedAt = now
	return memo
}

func (m *RouteAssignmentManager) discardPrecommitRecoveryLocked(transitionID, fingerprint string) error {
	if err := m.deleteRecoveryLocked(transitionID); err != nil {
		return fmt.Errorf("%w: release definitive precommit recovery: %v", ErrTransitionTombstoneCap, err)
	}
	m.releaseTransitionReservationLocked(transitionID, fingerprint)
	return nil
}

// CurrentSnapshot returns the manager's last successfully published full
// snapshot. It is local-only and safe for constructing a Dynamic provider.
func (m *RouteAssignmentManager) CurrentSnapshot() actorrouting.RouteSnapshot {
	if m == nil {
		return actorrouting.RouteSnapshot{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return actorrouting.CloneRouteSnapshot(m.snapshot)
}

// SetHolderPublicKeyResolver installs the verified, process-local publication
// source. Replacing it is atomic; callers normally install it once before
// startup recovery and leave it for the controller boot.
func (m *RouteAssignmentManager) SetHolderPublicKeyResolver(resolver HolderPublicKeyResolver) error {
	if m == nil || resolver == nil {
		return ErrInvalidManager
	}
	m.publicKeyMu.Lock()
	m.holderPublicKeys = resolver
	m.publicKeyMu.Unlock()
	return nil
}

func (m *RouteAssignmentManager) resolveHolderPublicKey(domain actorrouting.RouteDomain, holderID, bootID string) (holderkey.Publication, bool, error) {
	if m == nil {
		return holderkey.Publication{}, false, ErrInvalidManager
	}
	m.publicKeyMu.RLock()
	resolver := m.holderPublicKeys
	m.publicKeyMu.RUnlock()
	if resolver == nil {
		return holderkey.Publication{}, false, nil
	}
	publication, found, err := resolver.ResolveHolderPublicKey(domain, holderID, bootID)
	if err != nil || !found {
		return holderkey.Publication{}, found, err
	}
	if err := publication.Validate(); err != nil {
		return holderkey.Publication{}, false, fmt.Errorf("%w: holder public key", ErrInvalidManager)
	}
	return publication.Clone(), true, nil
}

// HolderPublicKeyReconcileResult distinguishes a fail-before-write error from
// a committed snapshot whose local watch notification must be retried.
type HolderPublicKeyReconcileResult struct {
	Changed              bool
	PublicationAttempted bool
	Committed            bool
	SnapshotRevision     uint64
}

// ReconcileHolderPublicKeys publishes the complete bounded keyring projection
// as one ordinary RouteSnapshot generation. Unreferenced retired holders and
// exact holder/boot tuples authoritatively absent from the fresh verified view
// lose their keys. A resolver error is indeterminate and aborts publication,
// preserving the current snapshot. Route transitions and key updates share operationMu, so
// neither can overwrite the other's complete snapshot.
func (m *RouteAssignmentManager) ReconcileHolderPublicKeys(ctx context.Context) (HolderPublicKeyReconcileResult, error) {
	var result HolderPublicKeyReconcileResult
	if m == nil || ctx == nil {
		return result, ErrInvalidManager
	}
	release, err := m.acquireExclusiveOperation(ctx)
	if err != nil {
		return result, err
	}
	defer release()

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.initialBatchOpen || m.pendingLeaseProjection != nil {
		return result, fmt.Errorf("%w: snapshot publication boundary is active", ErrSnapshotPublish)
	}
	next := actorrouting.CloneRouteSnapshot(m.snapshot)
	for index := range next.Holders {
		domain, referenced := holderDomainForPublicKey(next, index)
		holder := &next.Holders[index]
		if !referenced {
			holder.ClearCommitProofPublication()
			continue
		}
		publication, found, resolveErr := m.resolveHolderPublicKey(domain, holder.InstanceID, holder.BootID)
		if resolveErr != nil {
			return result, resolveErr
		}
		if !found {
			// The resolver is backed by one fresh, atomically installed verified
			// view. Absence is therefore authoritative revocation for this exact
			// holder/boot. Transient/indeterminate failures return an error above
			// and perform no snapshot write.
			holder.ClearCommitProofPublication()
			continue
		}
		if err := holder.SetCommitProofPublication(publication); err != nil {
			return result, fmt.Errorf("%w: holder public key", ErrInvalidManager)
		}
	}
	if reflect.DeepEqual(next, m.snapshot) {
		result.SnapshotRevision = m.snapshot.GlobalRevision
		return result, nil
	}
	if next.GlobalRevision == ^uint64(0) {
		return result, ErrAuthorityOverflow
	}
	next.GlobalRevision++
	result.Changed = true
	result.PublicationAttempted = true
	result.SnapshotRevision = next.GlobalRevision
	if err := m.publishSnapshot(ctx, next, false); err != nil {
		if snapshotPublishWasCommitted(err) {
			m.snapshot = next
			result.Committed = true
		}
		return result, fmt.Errorf("%w: holder public key projection: %v", ErrSnapshotPublish, err)
	}
	m.snapshot = next
	result.Committed = true
	return result, nil
}

// LeaseProjection returns the latest same-generation Authority observation
// kept separately when deadline-only Snapshot publication is disabled. It is
// an observation cache, not a write-authority grant; Redis remains canonical.
func (m *RouteAssignmentManager) LeaseProjection(domain actorrouting.RouteDomain, shard uint32) (authoritystore.ShardWriteAuthority, bool) {
	if m == nil {
		return authoritystore.ShardWriteAuthority{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	value, ok := m.leaseProjection[authorityKey{domain: domain, shard: shard}]
	return value, ok
}

// BeginInitialAuthorityBatch arms the Manager-local holder-index preservation
// required by the sealed startup batch. It does not grant a second mutation
// authority; claims still pass through the normal Manager and AuthorityStore.
func (m *RouteAssignmentManager) BeginInitialAuthorityBatch() error {
	if m == nil {
		return ErrInvalidManager
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.initialBatchOpen || m.initialBatchSealed {
		return fmt.Errorf("%w: initial authority batch already used", ErrInvalidManager)
	}
	for _, routes := range []actorrouting.DomainSnapshot{m.snapshot.Player, m.snapshot.Farm} {
		if routes.ShardCount == 0 || uint32(len(routes.Routes)) != routes.ShardCount {
			return fmt.Errorf("%w: initial authority batch route table", ErrInvalidManager)
		}
		for _, route := range routes.Routes {
			if route.State != actorrouting.ShardStateAssigning || route.ShardRevision != 1 || route.ExpectedWriterEpoch != 1 || route.ExpectedLeaseEpoch != 1 || route.LeaseUntilMs != 0 {
				return fmt.Errorf("%w: initial authority batch requires all placeholders", ErrInvalidManager)
			}
		}
	}
	m.initialBatchOpen = true
	return nil
}

// EndInitialAuthorityBatch irreversibly returns this Manager to its normal
// publication/compaction behavior. The durable publisher must already have
// committed and verified the final snapshot before this is called.
func (m *RouteAssignmentManager) EndInitialAuthorityBatch() error {
	if m == nil {
		return ErrInvalidManager
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.initialBatchOpen || m.initialBatchSealed {
		return fmt.Errorf("%w: initial authority batch is not open", ErrInvalidManager)
	}
	m.initialBatchOpen = false
	m.initialBatchSealed = true
	return nil
}

func (m *RouteAssignmentManager) initialBatchIsOpen() bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.initialBatchOpen
}

// ProjectFreshLeases performs one all-or-nothing read wave from the trusted
// ShardWriteAuthority source and publishes only same-generation deadline
// advances. It never calls an authority mutation and never accepts Holder
// self-reported facts. The Manager's exclusive operation boundary serializes
// the complete wave with Plan generation mutations.
func (m *RouteAssignmentManager) ProjectFreshLeases(ctx context.Context, minimumRemaining time.Duration) (LeaseProjectionResult, error) {
	if m == nil || ctx == nil || m.reader == nil || minimumRemaining <= 0 || minimumRemaining > time.Minute {
		return LeaseProjectionResult{}, ErrInvalidManager
	}
	if _, bounded := ctx.Deadline(); !bounded {
		return LeaseProjectionResult{}, fmt.Errorf("%w: lease projection deadline required", ErrInvalidManager)
	}
	if err := m.requireControlToken(ctx); err != nil {
		return LeaseProjectionResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return LeaseProjectionResult{}, err
	}
	releaseOperation, err := m.acquireExclusiveOperation(ctx)
	if err != nil {
		return LeaseProjectionResult{}, err
	}
	defer releaseOperation()

	m.mu.Lock()
	if m.initialBatchOpen {
		m.mu.Unlock()
		return LeaseProjectionResult{}, routecontrol.ErrInitialAuthorityBatchOpen
	}
	if m.deadlineNotifyPending != nil {
		m.mu.Unlock()
		return LeaseProjectionResult{}, ErrLeaseProjectionPending
	}
	var pending *pendingLeaseProjection
	if m.pendingLeaseProjection != nil {
		if !reflect.DeepEqual(m.snapshot, m.pendingLeaseProjection.base) {
			m.pendingLeaseProjection = nil
			m.mu.Unlock()
			return LeaseProjectionResult{}, ErrLeaseProjectionStale
		}
		pending = &pendingLeaseProjection{
			base:     actorrouting.CloneRouteSnapshot(m.pendingLeaseProjection.base),
			snapshot: actorrouting.CloneRouteSnapshot(m.pendingLeaseProjection.snapshot),
			result:   m.pendingLeaseProjection.result,
		}
	}
	base := actorrouting.CloneRouteSnapshot(m.snapshot)
	m.mu.Unlock()
	if pending != nil {
		result := pending.result
		publishErr := m.publishSnapshot(ctx, actorrouting.CloneRouteSnapshot(pending.snapshot), false)
		if publishErr != nil {
			if snapshotPublishWasCommitted(publishErr) {
				if err := m.finishLeaseProjectionPublication(pending.base, pending.snapshot, true); err != nil {
					return LeaseProjectionResult{}, err
				}
			}
			return result, fmt.Errorf("%w: retry lease projection: %v", ErrSnapshotPublish, publishErr)
		}
		if err := m.finishLeaseProjectionPublication(pending.base, pending.snapshot, false); err != nil {
			return LeaseProjectionResult{}, err
		}
		result.Published = true
		return result, nil
	}

	candidate := actorrouting.CloneRouteSnapshot(base)
	observedLeases := make(map[authorityKey]authoritystore.ShardWriteAuthority)
	result := LeaseProjectionResult{GlobalRevision: base.GlobalRevision, MinimumLeaseUntilMs: int64(^uint64(0) >> 1)}
	for _, domain := range managedSnapshotDomains(candidate) {
		routes := snapshotDomain(candidate, domain).Routes
		for shard := range routes {
			if err := ctx.Err(); err != nil {
				return LeaseProjectionResult{}, err
			}
			route := routes[shard]
			if route.State != actorrouting.ShardStateActive || route.HolderIndex < 0 || route.HolderIndex >= len(candidate.Holders) {
				return LeaseProjectionResult{}, fmt.Errorf("%w: %s/%d is not an active exact route", ErrLeaseProjectionInvalid, domain, shard)
			}
			holder := candidate.Holders[route.HolderIndex]
			authority, found, err := m.reader.ReadShardWriteAuthority(ctx, domain, uint32(shard))
			if err != nil {
				return LeaseProjectionResult{}, fmt.Errorf("%w: read %s/%d: %v", ErrLeaseProjectionInvalid, domain, shard, err)
			}
			if !found || authority.AuthorityLayoutVersion != m.layout || authority.HashVersion != candidate.HashVersion || authority.HashSeed != candidate.HashSeed ||
				authority.Domain != domain || authority.VirtualShard != uint32(shard) || authority.State != authoritystore.ShardAuthorityActive ||
				authority.HolderID != holder.InstanceID || authority.HolderBootID != holder.BootID ||
				authority.AssignmentRevision != route.ShardRevision || authority.WriterEpoch != route.ExpectedWriterEpoch ||
				authority.LeaseEpoch != route.ExpectedLeaseEpoch || authority.LeaseUntilMs < route.LeaseUntilMs {
				return LeaseProjectionResult{}, fmt.Errorf("%w: tuple %s/%d", ErrLeaseProjectionInvalid, domain, shard)
			}
			result.Observed++
			observedLeases[authorityKey{domain: domain, shard: uint32(shard)}] = authority
			if authority.LeaseUntilMs != route.LeaseUntilMs {
				routes[shard].LeaseUntilMs = authority.LeaseUntilMs
				result.Changed++
			}
			if authority.LeaseUntilMs < result.MinimumLeaseUntilMs {
				result.MinimumLeaseUntilMs = authority.LeaseUntilMs
			}
		}
	}
	result.CompletedAt = m.now()
	if result.Observed == 0 || result.MinimumLeaseUntilMs-result.CompletedAt.UnixMilli() < minimumRemaining.Milliseconds() {
		return LeaseProjectionResult{}, fmt.Errorf("%w: minimum_deadline=%d", ErrLeaseProjectionTooClose, result.MinimumLeaseUntilMs)
	}

	m.mu.Lock()
	if !reflect.DeepEqual(m.snapshot, base) {
		m.mu.Unlock()
		return LeaseProjectionResult{}, ErrLeaseProjectionStale
	}
	if result.Changed == 0 {
		m.mu.Unlock()
		return result, nil
	}
	if m.disableDeadlineSnapshot {
		for key, authority := range observedLeases {
			m.leaseProjection[key] = authority
		}
		m.mu.Unlock()
		return result, nil
	}
	candidate.GlobalRevision = base.GlobalRevision + 1
	result.GlobalRevision = candidate.GlobalRevision
	if err := actorrouting.ValidateRouteSnapshotUpdate(base, candidate); err != nil {
		m.mu.Unlock()
		return LeaseProjectionResult{}, fmt.Errorf("%w: candidate: %v", ErrLeaseProjectionInvalid, err)
	}
	m.pendingLeaseProjection = &pendingLeaseProjection{
		base: actorrouting.CloneRouteSnapshot(base), snapshot: actorrouting.CloneRouteSnapshot(candidate), result: result,
	}
	m.mu.Unlock()
	publishErr := m.publishSnapshot(ctx, candidate, false)
	if publishErr != nil {
		if snapshotPublishWasCommitted(publishErr) {
			if err := m.finishLeaseProjectionPublication(base, candidate, true); err != nil {
				return LeaseProjectionResult{}, err
			}
		}
		return result, fmt.Errorf("%w: lease projection: %v", ErrSnapshotPublish, publishErr)
	}
	if err := m.finishLeaseProjectionPublication(base, candidate, false); err != nil {
		return LeaseProjectionResult{}, err
	}
	result.Published = true
	return result, nil
}

// finishLeaseProjectionPublication advances local Manager state only after the
// complete Snapshot publication is durably known. A committed-notify failure
// uses the same exact-snapshot replay path as deadline reconciliation.
func (m *RouteAssignmentManager) finishLeaseProjectionPublication(base, candidate actorrouting.RouteSnapshot, notificationPending bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	pending := m.pendingLeaseProjection
	if pending == nil || !reflect.DeepEqual(pending.base, base) || !reflect.DeepEqual(pending.snapshot, candidate) || !reflect.DeepEqual(m.snapshot, base) {
		return ErrLeaseProjectionStale
	}
	m.snapshot = actorrouting.CloneRouteSnapshot(candidate)
	m.pendingLeaseProjection = nil
	if notificationPending {
		notify := actorrouting.CloneRouteSnapshot(candidate)
		m.deadlineNotifyPending = &notify
	}
	return nil
}

func (m *RouteAssignmentManager) acquireOperation(ctx context.Context) (func(), error) {
	if err := m.requireControlToken(ctx); err != nil {
		return nil, err
	}
	return m.acquireOperationMode(ctx, false)
}

func (m *RouteAssignmentManager) acquireExclusiveOperation(ctx context.Context) (func(), error) {
	if err := m.requireControlToken(ctx); err != nil {
		return nil, err
	}
	return m.acquireOperationMode(ctx, true)
}

func (m *RouteAssignmentManager) acquireOperationMode(ctx context.Context, exclusive bool) (func(), error) {
	if m == nil || ctx == nil {
		return nil, ErrInvalidManager
	}
	for {
		var acquired bool
		if exclusive {
			acquired = m.operationMu.TryLock()
		} else {
			acquired = m.operationMu.TryRLock()
		}
		if acquired {
			if err := ctx.Err(); err != nil {
				if exclusive {
					m.operationMu.Unlock()
				} else {
					m.operationMu.RUnlock()
				}
				return nil, err
			}
			var once sync.Once
			return func() {
				once.Do(func() {
					if exclusive {
						m.operationMu.Unlock()
					} else {
						m.operationMu.RUnlock()
					}
				})
			}, nil
		}
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

// ReconcileRenewedAuthorityDeadlines projects holder-authorized, monotonic
// deadline extensions for existing ACTIVE generations into one new complete
// RouteSnapshot. It never mutates Authority, creates a transition, changes a
// generation, or consults HolderRegistry. The potentially large Authority read
// wave does not exclude Plan execution. A pass with no changed deadlines
// returns without acquiring the exclusive operation lock. A pass that needs to
// publish acquires that lock after the reads and revalidates its exact captured
// Snapshot before constructing and durably publishing one new revision.
func (m *RouteAssignmentManager) ReconcileRenewedAuthorityDeadlines(ctx context.Context, refreshLead time.Duration) (DeadlineProjectionResult, error) {
	if m == nil || ctx == nil || refreshLead < time.Millisecond {
		return DeadlineProjectionResult{}, ErrInvalidManager
	}
	if _, bounded := ctx.Deadline(); !bounded {
		return DeadlineProjectionResult{}, fmt.Errorf("%w: deadline required", ErrInvalidManager)
	}
	if err := m.requireControlToken(ctx); err != nil {
		return DeadlineProjectionResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return DeadlineProjectionResult{}, err
	}

	m.mu.Lock()
	if m.initialBatchOpen {
		m.mu.Unlock()
		return DeadlineProjectionResult{FailureStage: "initial_authority_batch"}, routecontrol.ErrInitialAuthorityBatchOpen
	}
	if m.pendingLeaseProjection != nil {
		m.mu.Unlock()
		return DeadlineProjectionResult{FailureStage: "lease_projection_pending"}, fmt.Errorf("%w: %w", ErrDeadlineProjection, ErrLeaseProjectionPending)
	}
	hasNotificationPending := m.deadlineNotifyPending != nil
	m.mu.Unlock()
	if hasNotificationPending {
		releaseOperation, err := m.acquireExclusiveOperation(ctx)
		if err != nil {
			return DeadlineProjectionResult{}, err
		}
		m.mu.Lock()
		var notificationPending actorrouting.RouteSnapshot
		if m.deadlineNotifyPending != nil {
			notificationPending = actorrouting.CloneRouteSnapshot(*m.deadlineNotifyPending)
			if !reflect.DeepEqual(m.snapshot, notificationPending) {
				m.deadlineNotifyPending = nil
				notificationPending = actorrouting.RouteSnapshot{}
			}
		}
		m.mu.Unlock()
		if notificationPending.GlobalRevision == 0 {
			releaseOperation()
		} else {
			defer releaseOperation()
			result := DeadlineProjectionResult{SnapshotRevision: notificationPending.GlobalRevision}
			if err := m.publishSnapshot(ctx, notificationPending, false); err != nil {
				result.FailureStage = "notify"
				return result, fmt.Errorf("%w: replay committed revision %d notification: %w", ErrDeadlineProjection, notificationPending.GlobalRevision, err)
			}
			m.mu.Lock()
			if m.deadlineNotifyPending != nil && reflect.DeepEqual(*m.deadlineNotifyPending, notificationPending) {
				m.deadlineNotifyPending = nil
			}
			m.mu.Unlock()
			result.NotificationReplayed = true
			result.Published = true
			result.SnapshotBytes = canonicalSnapshotBytes(notificationPending)
			return result, nil
		}
	}

	m.mu.Lock()
	if m.initialBatchOpen {
		m.mu.Unlock()
		return DeadlineProjectionResult{FailureStage: "initial_authority_batch"}, routecontrol.ErrInitialAuthorityBatchOpen
	}
	if m.pendingLeaseProjection != nil {
		m.mu.Unlock()
		return DeadlineProjectionResult{FailureStage: "lease_projection_pending"}, fmt.Errorf("%w: %w", ErrDeadlineProjection, ErrLeaseProjectionPending)
	}
	captured := actorrouting.CloneRouteSnapshot(m.snapshot)
	if captured.GlobalRevision == ^uint64(0) {
		m.mu.Unlock()
		return DeadlineProjectionResult{}, fmt.Errorf("%w: global revision overflow", ErrDeadlineProjection)
	}
	capturedAtMs := m.now().UnixMilli()
	routeCapacity := captured.Player.ShardCount + captured.Farm.ShardCount +
		captured.FarmWorker.ShardCount
	routes := make([]deadlineProjectionRoute, 0, routeCapacity)
	minimumRemainingMs := int64(^uint64(0) >> 1)
	for _, domain := range managedSnapshotDomains(captured) {
		domainRoutes := snapshotDomain(captured, domain)
		if int(domainRoutes.ShardCount) != len(domainRoutes.Routes) {
			m.mu.Unlock()
			return DeadlineProjectionResult{}, fmt.Errorf("%w: incomplete %s route table", ErrDeadlineProjection, domain)
		}
		for shard := uint32(0); shard < domainRoutes.ShardCount; shard++ {
			route := domainRoutes.Routes[shard]
			if route.State != actorrouting.ShardStateActive {
				continue
			}
			if route.HolderIndex < 0 || route.HolderIndex >= len(captured.Holders) || route.LeaseUntilMs <= 0 {
				m.mu.Unlock()
				return DeadlineProjectionResult{}, fmt.Errorf("%w: incomplete ACTIVE route %s/%d", ErrDeadlineProjection, domain, shard)
			}
			holder := captured.Holders[route.HolderIndex]
			if holder.InstanceID == "" || holder.BootID == "" || route.ShardRevision == 0 || route.ExpectedWriterEpoch == 0 || route.ExpectedLeaseEpoch == 0 {
				m.mu.Unlock()
				return DeadlineProjectionResult{}, fmt.Errorf("%w: incomplete ACTIVE generation %s/%d", ErrDeadlineProjection, domain, shard)
			}
			key := authorityKey{domain: domain, shard: shard}
			expectedTransitionID := ""
			if committed, ok := m.committed[key]; ok &&
				committed.HolderID == holder.InstanceID && committed.HolderBootID == holder.BootID &&
				committed.AssignmentRevision == route.ShardRevision && committed.WriterEpoch == route.ExpectedWriterEpoch &&
				committed.LeaseEpoch == route.ExpectedLeaseEpoch {
				expectedTransitionID = committed.LastTransitionID
			}
			routes = append(routes, deadlineProjectionRoute{
				key: key, holderID: holder.InstanceID, holderBootID: holder.BootID,
				assignmentRevision: route.ShardRevision, writerEpoch: route.ExpectedWriterEpoch,
				leaseEpoch: route.ExpectedLeaseEpoch, leaseUntilMs: route.LeaseUntilMs,
				expectedTransitionID: expectedTransitionID,
			})
			if remaining := route.LeaseUntilMs - capturedAtMs; remaining < minimumRemainingMs {
				minimumRemainingMs = remaining
			}
		}
	}
	m.mu.Unlock()

	result := DeadlineProjectionResult{SnapshotRevision: captured.GlobalRevision}
	if len(routes) != 0 {
		result.MinRemainingMs = minimumRemainingMs
	}
	if len(routes) == 0 || minimumRemainingMs > refreshLead.Milliseconds() {
		return result, nil
	}

	observed := make(map[authorityKey]authoritystore.ShardWriteAuthority, len(routes))
	readNowMs := m.now().UnixMilli()
	currentAuthorities := make([]authoritystore.ShardWriteAuthority, len(routes))
	foundAuthorities := make([]bool, len(routes))
	if batch, ok := m.reader.(AuthorityBatchReader); ok {
		domains := make([]actorrouting.RouteDomain, len(routes))
		shards := make([]uint32, len(routes))
		for index, route := range routes {
			domains[index], shards[index] = route.key.domain, route.key.shard
		}
		var err error
		currentAuthorities, foundAuthorities, err = batch.ReadShardWriteAuthorityBatch(ctx, domains, shards)
		if err != nil {
			result.FailureStage = "read"
			return result, fmt.Errorf("%w: batch Authority read: %w", ErrDeadlineProjection, err)
		}
		if len(currentAuthorities) != len(routes) || len(foundAuthorities) != len(routes) {
			result.FailureStage = "read"
			return result, fmt.Errorf("%w: short batch Authority read", ErrDeadlineProjection)
		}
	} else {
		for index, route := range routes {
			if err := ctx.Err(); err != nil {
				result.FailureStage = "cancelled"
				return result, err
			}
			current, found, err := m.readAuthorityLocked(ctx, route.key)
			if err != nil {
				result.FailureStage = "read"
				return result, fmt.Errorf("%w: read %s/%d: %w", ErrDeadlineProjection, route.key.domain, route.key.shard, err)
			}
			currentAuthorities[index], foundAuthorities[index] = current, found
		}
	}
	for index, route := range routes {
		if err := ctx.Err(); err != nil {
			result.FailureStage = "cancelled"
			return result, err
		}
		current, found := currentAuthorities[index], foundAuthorities[index]
		if !found {
			result.FailureStage = "read"
			return result, fmt.Errorf("%w: Authority absent for %s/%d", ErrDeadlineProjection, route.key.domain, route.key.shard)
		}
		if lag := current.LeaseUntilMs - route.leaseUntilMs; lag > result.ProjectionLagMs {
			result.ProjectionLagMs = lag
		}
		if current.AuthorityLayoutVersion != m.layout || current.HashVersion != captured.HashVersion || current.HashSeed != captured.HashSeed ||
			current.Domain != route.key.domain || current.VirtualShard != route.key.shard || current.State != authoritystore.ShardAuthorityActive ||
			current.HolderID != route.holderID || current.HolderBootID != route.holderBootID ||
			current.AssignmentRevision != route.assignmentRevision || current.WriterEpoch != route.writerEpoch || current.LeaseEpoch != route.leaseEpoch ||
			current.LastTransitionID == "" || current.LastTransitionAtMs <= 0 ||
			(route.expectedTransitionID != "" && current.LastTransitionID != route.expectedTransitionID) {
			result.FailureStage = "validate"
			return result, fmt.Errorf("%w: generation drift for %s/%d", ErrDeadlineProjection, route.key.domain, route.key.shard)
		}
		if current.LeaseUntilMs < route.leaseUntilMs {
			result.FailureStage = "validate"
			return result, fmt.Errorf("%w: deadline regression for %s/%d", ErrDeadlineProjection, route.key.domain, route.key.shard)
		}
		if current.LeaseUntilMs <= readNowMs {
			// A crashed holder stalls its own shards until a takeover runs.
			// Refusing the whole pass would starve every other shard of its
			// deadline refresh and take the entire fleet down with it.
			result.StalledShards++
			continue
		}
		observed[route.key] = current
		if current.LeaseUntilMs > route.leaseUntilMs {
			result.ChangedShards++
		}
	}
	if result.ChangedShards == 0 {
		return result, nil
	}

	// Avoid queueing an exclusive writer behind active Drain readers when the
	// read wave is already stale. The same check is repeated after acquiring
	// the lock because a transition may start between these two points.
	m.mu.Lock()
	snapshotUnchanged := reflect.DeepEqual(m.snapshot, captured)
	m.mu.Unlock()
	if !snapshotUnchanged {
		result.FailureStage = "revalidate"
		return result, fmt.Errorf("%w: Snapshot changed during refresh", ErrDeadlineProjection)
	}

	if m.disableDeadlineSnapshot {
		m.mu.Lock()
		if !reflect.DeepEqual(m.snapshot, captured) {
			m.mu.Unlock()
			result.FailureStage = "revalidate"
			return result, fmt.Errorf("%w: Snapshot changed during refresh", ErrDeadlineProjection)
		}
		for key, current := range observed {
			m.leaseProjection[key] = current
		}
		m.mu.Unlock()
		result.Changed = true
		return result, nil
	}

	releaseOperation, err := m.acquireExclusiveOperation(ctx)
	if err != nil {
		return result, err
	}
	defer releaseOperation()

	m.mu.Lock()
	if m.initialBatchOpen {
		m.mu.Unlock()
		result.FailureStage = "initial_authority_batch"
		return result, routecontrol.ErrInitialAuthorityBatchOpen
	}
	if m.pendingLeaseProjection != nil {
		m.mu.Unlock()
		result.FailureStage = "lease_projection_pending"
		return result, fmt.Errorf("%w: %w", ErrDeadlineProjection, ErrLeaseProjectionPending)
	}
	if !reflect.DeepEqual(m.snapshot, captured) {
		result.FailureStage = "revalidate"
		m.mu.Unlock()
		return result, fmt.Errorf("%w: Snapshot changed during refresh", ErrDeadlineProjection)
	}
	publishNowMs := m.now().UnixMilli()
	next := actorrouting.CloneRouteSnapshot(captured)
	for _, route := range routes {
		current, observedRoute := observed[route.key]
		if !observedRoute {
			continue
		}
		if current.LeaseUntilMs <= publishNowMs {
			m.mu.Unlock()
			return result, fmt.Errorf("%w: Authority expired before publish for %s/%d", ErrDeadlineProjection, route.key.domain, route.key.shard)
		}
		if current.LeaseUntilMs == route.leaseUntilMs {
			continue
		}
		domainRoutes := snapshotDomain(next, route.key.domain)
		candidate := domainRoutes.Routes[route.key.shard]
		if candidate.State != actorrouting.ShardStateActive || candidate.HolderIndex < 0 || candidate.HolderIndex >= len(next.Holders) {
			m.mu.Unlock()
			return result, fmt.Errorf("%w: ACTIVE route changed for %s/%d", ErrDeadlineProjection, route.key.domain, route.key.shard)
		}
		candidate.LeaseUntilMs = current.LeaseUntilMs
		domainRoutes.Routes[route.key.shard] = candidate
		setSnapshotDomain(&next, route.key.domain, domainRoutes)
	}
	next.GlobalRevision++
	if err := actorrouting.ValidateRouteSnapshotUpdate(captured, next); err != nil {
		m.mu.Unlock()
		return result, fmt.Errorf("%w: complete Snapshot validation: %v", ErrDeadlineProjection, err)
	}
	m.mu.Unlock()
	result.PublicationAttempted = true
	if err := m.publishSnapshot(ctx, next, false); err != nil {
		result.FailureStage = "publish"
		if snapshotPublishWasCommitted(err) {
			m.mu.Lock()
			if reflect.DeepEqual(m.snapshot, captured) {
				m.snapshot = next
				for key, current := range observed {
					m.committed[key] = current
				}
				pending := actorrouting.CloneRouteSnapshot(next)
				m.deadlineNotifyPending = &pending
				result.Changed = true
				result.Published = true
				result.SnapshotRevision = next.GlobalRevision
				result.SnapshotBytes = canonicalSnapshotBytes(next)
			}
			m.mu.Unlock()
		}
		return result, fmt.Errorf("%w: publish revision %d: %w", ErrDeadlineProjection, next.GlobalRevision, err)
	}
	m.mu.Lock()
	if !reflect.DeepEqual(m.snapshot, captured) {
		m.mu.Unlock()
		return result, fmt.Errorf("%w: local Snapshot changed after serialized publish", ErrDeadlineProjection)
	}
	m.snapshot = next
	for key, current := range observed {
		m.committed[key] = current
	}
	m.mu.Unlock()
	result.Changed = true
	result.Published = true
	result.SnapshotRevision = next.GlobalRevision
	result.SnapshotBytes = canonicalSnapshotBytes(next)
	return result, nil
}

func canonicalSnapshotBytes(snapshot actorrouting.RouteSnapshot) int {
	stored, err := routecontrol.CanonicalizeRouteSnapshot(snapshot)
	if err != nil {
		return 0
	}
	return len(stored.CanonicalJSON)
}

// RecoverPendingProjections resumes only Manager-owned durable recoveries that
// are already proven committed and whose full RouteSnapshot projection is
// incomplete. It never invokes Claim/Transfer/Unassign or an AuthorityStore
// mutation. Callers must supply a deadline and may run only one bounded pass;
// the Manager enforces a context-aware single recovery owner.
func (m *RouteAssignmentManager) RecoverPendingProjections(ctx context.Context, targetValidator PendingProjectionTargetValidator, drainFactory PendingDrainControllerFactory) error {
	if m == nil || ctx == nil || m.projectionRecovery == nil {
		return ErrInvalidManager
	}
	if _, bounded := ctx.Deadline(); !bounded {
		return fmt.Errorf("%w: recovery deadline required", ErrInvalidManager)
	}
	if err := m.requireControlToken(ctx); err != nil {
		return err
	}
	releaseOperation, err := m.acquireOperation(ctx)
	if err != nil {
		return err
	}
	defer releaseOperation()
	select {
	case m.projectionRecovery <- struct{}{}:
		defer func() { <-m.projectionRecovery }()
	case <-ctx.Done():
		return ctx.Err()
	}

	// A failed all-active publication owns its exact base/candidate pair until
	// retry or stale invalidation. Recovery must not advance the Manager Snapshot
	// across that pending durable CAS boundary.
	m.mu.Lock()
	leaseProjectionPending := m.pendingLeaseProjection != nil
	m.mu.Unlock()
	if leaseProjectionPending {
		return fmt.Errorf("%w: %w", ErrProjectionRecovery, ErrLeaseProjectionPending)
	}

	recoveryStore, ok := m.tombstones.(TransitionRecoveryStore)
	if !ok {
		return fmt.Errorf("%w: durable recovery store required", ErrProjectionRecovery)
	}
	recoveries, err := recoveryStore.LoadTransitionRecoveries(ctx)
	if err != nil {
		return fmt.Errorf("%w: load durable recoveries: %v", ErrProjectionRecovery, err)
	}
	if len(recoveries) > m.memoLimit {
		return fmt.Errorf("%w: durable recovery bound", ErrProjectionRecovery)
	}
	sort.Slice(recoveries, func(i, j int) bool {
		if recoveries[i].Domain != recoveries[j].Domain {
			return recoveries[i].Domain < recoveries[j].Domain
		}
		if recoveries[i].VirtualShard != recoveries[j].VirtualShard {
			return recoveries[i].VirtualShard < recoveries[j].VirtualShard
		}
		if !recoveries[i].CreatedAt.Equal(recoveries[j].CreatedAt) {
			return recoveries[i].CreatedAt.Before(recoveries[j].CreatedAt)
		}
		return recoveries[i].TransitionID < recoveries[j].TransitionID
	})

	for _, recovery := range recoveries {
		if err := ctx.Err(); err != nil {
			return err
		}
		request, err := requestFromRecovery(recovery)
		if err != nil {
			return fmt.Errorf("%w: transition %s identity: %v", ErrProjectionRecovery, recovery.TransitionID, err)
		}
		projectionComplete := recovery.Published &&
			(recovery.LastState == actorrouting.ShardStateActive || request.kind == transitionUnassign)
		if projectionComplete {
			continue
		}
		if !recoveryAuthorityCommitted(recovery) {
			m.mu.Lock()
			memo := m.transitionMemoLocked(recovery.TransitionID)
			if memo == nil || memo.fingerprint != recovery.Fingerprint || !memo.commitAmbiguous {
				m.mu.Unlock()
				return fmt.Errorf("%w: transition %s ambiguous memo mismatch", ErrProjectionRecovery, recovery.TransitionID)
			}
			err = m.promoteAmbiguousMemoLocked(ctx, memo)
			if err == nil {
				recovery = recoveryForMemo(memo)
			}
			m.mu.Unlock()
			if err != nil {
				m.stickyBlockProjection(recovery.TransitionID, err)
				return fmt.Errorf("%w: transition %s commit reconcile: %w", ErrProjectionRecovery, recovery.TransitionID, err)
			}
		}
		if err := m.validatePendingRecovery(recovery, request); err != nil {
			return err
		}
		if request.kind != transitionUnassign && targetValidator == nil {
			return fmt.Errorf("%w: transition %s target validator required", ErrProjectionRecovery, recovery.TransitionID)
		}
		if recovery.InternalDrain && recovery.Mode == TransferModeDrainHandoff {
			err = m.recoverPendingDrain(ctx, recovery, request, targetValidator, drainFactory)
		} else {
			err = m.recoverPendingSnapshot(ctx, recovery, request, targetValidator)
		}
		if err != nil {
			m.stickyBlockProjection(recovery.TransitionID, err)
			return fmt.Errorf("%w: transition %s: %w", ErrProjectionRecovery, recovery.TransitionID, err)
		}
	}
	return nil
}

func (m *RouteAssignmentManager) validatePendingRecovery(recovery TransitionRecovery, request transitionRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	memo := m.transitionMemoLocked(recovery.TransitionID)
	if memo == nil || memo.fingerprint != recovery.Fingerprint || transitionFingerprint(request) != recovery.Fingerprint ||
		transitionFingerprint(memo.request) != recovery.Fingerprint || memo.result != recovery.Result ||
		memo.lastState != recovery.LastState || memo.published != recovery.Published ||
		memo.publishedSnapshotRevision != recovery.PublishedSnapshotRevision ||
		memo.request.internalDrain != recovery.InternalDrain || !memo.projectionPending {
		return fmt.Errorf("%w: transition %s durable memo mismatch", ErrProjectionRecovery, recovery.TransitionID)
	}
	if memo.coldLoaded != recovery.ColdLoaded {
		if !memo.coldLoaded || recovery.ColdLoaded {
			return fmt.Errorf("%w: transition %s durable cold-load checkpoint mismatch", ErrProjectionRecovery, recovery.TransitionID)
		}
		// The only permitted forward discrepancy is an in-process cold-load
		// callback that completed before its durable checkpoint write failed.
		// Persist that already completed checkpoint before continuing; never
		// repeat the holder callback merely to recreate durable evidence.
		if err := m.commitRecoveryReleasingLock(memo); err != nil {
			return fmt.Errorf("%w: transition %s persist completed cold-load checkpoint: %v", ErrProjectionRecovery, recovery.TransitionID, err)
		}
	}
	if memo.blocked {
		if memo.blockErr != nil {
			return memo.blockErr
		}
		return ErrStaleTransition
	}
	if err := transitionMustBeCommitted(memo.result); err != nil {
		return fmt.Errorf("%w: transition %s result: %v", ErrProjectionRecovery, recovery.TransitionID, err)
	}
	// The durable result proves the immutable committed transition identity.
	// Its original lease may legitimately have been extended by the same
	// holder generation while projection was pending; freshness is established
	// from current Authority immediately before each recovery side effect.
	if err := m.validateCommittedAuthorityResultLocked(request, memo.result, false); err != nil {
		return fmt.Errorf("%w: transition %s result tuple: %v", ErrProjectionRecovery, recovery.TransitionID, err)
	}
	return nil
}

func (m *RouteAssignmentManager) recoverPendingSnapshot(ctx context.Context, recovery TransitionRecovery, request transitionRequest, targetValidator PendingProjectionTargetValidator) error {
	m.placementMu.RLock()
	defer m.placementMu.RUnlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	memo := m.transitionMemoLocked(recovery.TransitionID)
	if memo == nil || memo.fingerprint != recovery.Fingerprint || transitionFingerprint(request) != recovery.Fingerprint ||
		(memo.request.internalDrain && memo.request.mode != TransferModeWorkerHandoff) ||
		!memo.projectionPending {
		return ErrTransitionReconcile
	}
	return m.publishMemoLocked(ctx, memo, memo.lastState, true, recovery, targetValidator)
}

func (m *RouteAssignmentManager) recoverPendingDrain(ctx context.Context, recovery TransitionRecovery, request transitionRequest, targetValidator PendingProjectionTargetValidator, factory PendingDrainControllerFactory) error {
	m.placementMu.RLock()
	defer m.placementMu.RUnlock()
	currentAuthority, err := m.reconcilePendingAuthority(ctx, recovery.TransitionID, recovery.Fingerprint)
	if err != nil {
		return err
	}
	// Publication may have reached the durable Snapshot before recovery-record
	// deletion. An exact ACTIVE tuple (including endpoint) proves that holder
	// callbacks and publication already completed; retry only Manager-owned
	// durable completion and never roll it back through ASSIGNING.
	m.mu.Lock()
	memo := m.transitionMemoLocked(recovery.TransitionID)
	alreadyActive := memo != nil && memo.fingerprint == recovery.Fingerprint &&
		routeMatchesAuthority(m.snapshot, memo.result.Authority, actorrouting.ShardStateActive, memo.request.endpoint)
	if alreadyActive {
		err := m.publishMemoLocked(ctx, memo, actorrouting.ShardStateActive, true, recovery, targetValidator)
		m.mu.Unlock()
		return err
	}
	m.mu.Unlock()
	checkpoint, _, err := m.drainCheckpoint(request)
	if err != nil {
		return err
	}
	if checkpoint <= 0 || checkpoint >= 4 {
		return fmt.Errorf("%w: transition %s drain checkpoint=%d", ErrTransitionReconcile, recovery.TransitionID, checkpoint)
	}
	var controller DrainController = rejectingRecoveryDrainController{}
	var closeController func() error
	if checkpoint < 3 {
		if factory == nil {
			return ErrDrainControllerRequired
		}
		controller, closeController, err = factory(ctx, recovery)
		if err != nil {
			return err
		}
		if controller == nil {
			return ErrDrainControllerRequired
		}
	}
	if closeController != nil {
		defer func() { _ = closeController() }()
	}
	if checkpoint == 2 {
		inspector, ok := controller.(ColdLoadPreparedInspector)
		if !ok {
			return fmt.Errorf("%w: cold-load prepared inspector required", ErrProjectionRecovery)
		}
		if err := m.waitPendingProjectionTarget(ctx, request, recovery, targetValidator); err != nil {
			return err
		}
		prepared, inspectErr := inspector.ColdLoadPrepared(ctx, recovery.Domain, recovery.VirtualShard, currentAuthority)
		if inspectErr != nil {
			return fmt.Errorf("%w: inspect prepared generation: %v", ErrProjectionRecovery, inspectErr)
		}
		if prepared {
			// The holder may have prepared this exact generation from the
			// authenticated ASSIGNING Snapshot and extended only its deadline after
			// the recovery pass first read Authority. Re-read the durable tuple at
			// the inspector boundary; never checkpoint or publish the stale
			// pre-inspection deadline.
			preparedAuthority, reconcileErr := m.reconcilePendingAuthority(ctx, recovery.TransitionID, recovery.Fingerprint)
			if reconcileErr != nil {
				return reconcileErr
			}
			if err := m.markDrainColdLoaded(recovery.TransitionID, preparedAuthority); err != nil {
				return err
			}
		}
	}
	_, err = m.drain(ctx, TransferRequest{
		TransitionID: recovery.TransitionID, Domain: recovery.Domain, VirtualShard: recovery.VirtualShard,
		ExpectedFence: recovery.ExpectedFence, NewHolderID: recovery.NewHolderID,
		NewHolderBootID: recovery.NewHolderBootID,
		SourcePodUID:    recovery.SourcePodUID, SourceWorkloadUID: recovery.SourceWorkloadUID, SourceOrdinal: recovery.SourceOrdinal,
		TargetPodUID: recovery.TargetPodUID, TargetWorkloadUID: recovery.TargetWorkloadUID, TargetOrdinal: recovery.TargetOrdinal,
		DeploymentIdentityPresent: recovery.DeploymentIdentityPresent, Endpoint: recovery.Endpoint,
		RequestedTTL: time.Duration(recovery.RequestedTTLms) * time.Millisecond,
	}, controller, recovery, targetValidator)
	return err
}

func (m *RouteAssignmentManager) reconcilePendingAuthority(ctx context.Context, transitionID, fingerprint string) (authoritystore.ShardWriteAuthority, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	memo := m.transitionMemoLocked(transitionID)
	if memo == nil || memo.fingerprint != fingerprint || !memo.projectionPending {
		return authoritystore.ShardWriteAuthority{}, ErrTransitionReconcile
	}
	return m.reconcileAuthorityLocked(ctx, memo)
}

func (m *RouteAssignmentManager) stickyBlockProjection(transitionID string, cause error) {
	if !errors.Is(cause, ErrStaleTransition) {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if memo := m.transitionMemoLocked(transitionID); memo != nil {
		memo.blocked = true
		memo.blockErr = cause
	}
}

// rejectingRecoveryDrainController makes checkpoint >=3 recovery incapable of
// accidentally repeating any holder callback. Drain may only use it to publish
// the already cold-loaded ACTIVE projection.
type rejectingRecoveryDrainController struct{}

func (rejectingRecoveryDrainController) StopNewActivationAndTurns(context.Context, actorrouting.RouteDomain, uint32) error {
	return ErrTransitionReconcile
}
func (rejectingRecoveryDrainController) WaitCurrentTurns(context.Context, actorrouting.RouteDomain, uint32) error {
	return ErrTransitionReconcile
}
func (rejectingRecoveryDrainController) ConfirmFactsCommitted(context.Context, actorrouting.RouteDomain, uint32) error {
	return ErrTransitionReconcile
}
func (rejectingRecoveryDrainController) ColdLoadNewHolder(context.Context, actorrouting.RouteDomain, uint32, authoritystore.ShardWriteAuthority) error {
	return ErrTransitionReconcile
}

// Claim performs Authority Claim first and publishes the matching active
// route only after the store acknowledges APPLIED/ALREADY_APPLIED.
func (m *RouteAssignmentManager) Claim(ctx context.Context, req ClaimRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	releaseOperation, err := m.acquireOperation(ctx)
	if err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, err
	}
	defer releaseOperation()
	return m.runTransition(ctx, transitionRequest{
		kind: transitionClaim, transitionID: req.TransitionID, domain: req.Domain,
		virtualShard: req.VirtualShard, newHolderID: req.NewHolderID,
		newHolderBoot: req.NewHolderBootID, endpoint: req.Endpoint, ttl: req.RequestedTTL,
	})
}

// Transfer performs Authority Transfer first and then publishes the same
// holder/boot/epoch/revision/lease-epoch tuple.
func (m *RouteAssignmentManager) Transfer(ctx context.Context, req TransferRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	releaseOperation, err := m.acquireOperation(ctx)
	if err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, err
	}
	defer releaseOperation()
	return m.runTransition(ctx, transferRequest(req))
}

// TransferValidated is the PlanExecutor production seam. It keeps Manager as
// the sole generation coordinator while requiring the exact target facts both
// immediately before the AuthorityStore call and immediately before the
// matching full-snapshot publication.
func (m *RouteAssignmentManager) TransferValidated(ctx context.Context, req TransferRequest, targetValidator PendingProjectionTargetValidator) (authoritystore.ShardAuthorityTransitionResult, error) {
	if targetValidator == nil {
		return authoritystore.ShardAuthorityTransitionResult{}, fmt.Errorf("%w: target validator required", ErrInvalidManager)
	}
	releaseOperation, err := m.acquireOperation(ctx)
	if err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, err
	}
	defer releaseOperation()
	return m.runTransitionValidated(ctx, transferRequest(req), targetValidator)
}

// HandoffWorkerValidated performs a FARM_WORKER planned move without using the
// actor DrainController contract. It first publishes DRAINING for the current
// source generation. Worker assignment consumers remove non-ACTIVE shards from
// their local claim set, and the source validator proves that stop-new-claim
// fence through HolderRegistry before the durable Authority transfer runs.
func (m *RouteAssignmentManager) HandoffWorkerValidated(
	ctx context.Context,
	req TransferRequest,
	sourceValidator WorkerHandoffSourceValidator,
	targetValidator PendingProjectionTargetValidator,
) (authoritystore.ShardAuthorityTransitionResult, error) {
	if sourceValidator == nil || targetValidator == nil {
		return authoritystore.ShardAuthorityTransitionResult{}, fmt.Errorf(
			"%w: worker source and target validators required",
			ErrInvalidManager,
		)
	}
	request, err := normalizedWorkerHandoffRequest(req)
	if err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, err
	}
	releaseOperation, err := m.acquireOperation(ctx)
	if err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, err
	}
	defer releaseOperation()
	if m.initialBatchIsOpen() {
		return authoritystore.ShardAuthorityTransitionResult{}, routecontrol.ErrInitialAuthorityBatchOpen
	}

	if result, complete, replayErr := m.completedDrainReplay(request); replayErr != nil {
		return result, replayErr
	} else if complete {
		return result, nil
	}
	observation, sourceAckRequired, err := m.publishWorkerDraining(ctx, request)
	if err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, err
	}
	if sourceAckRequired {
		if err := sourceValidator(ctx, observation); err != nil {
			return authoritystore.ShardAuthorityTransitionResult{}, fmt.Errorf(
				"%w: worker source stop-new-claim: %v",
				ErrTransitionReconcile,
				err,
			)
		}
	}
	return m.runTransitionValidated(ctx, request, targetValidator)
}

func normalizedWorkerHandoffRequest(req TransferRequest) (transitionRequest, error) {
	request := transitionRequest{
		kind: transitionTransfer, transitionID: req.TransitionID, domain: req.Domain,
		virtualShard: req.VirtualShard, expectedFence: req.ExpectedFence,
		newHolderID: req.NewHolderID, newHolderBoot: req.NewHolderBootID,
		endpoint: req.Endpoint, ttl: req.RequestedTTL,
		mode: TransferModeWorkerHandoff, internalDrain: true,
	}
	if err := validateTransitionRequest(request); err != nil {
		return transitionRequest{}, err
	}
	ttl, err := durationMillisForTransition(request)
	if err != nil {
		return transitionRequest{}, err
	}
	request.ttl = time.Duration(ttl) * time.Millisecond
	return request, nil
}

func (m *RouteAssignmentManager) publishWorkerDraining(
	ctx context.Context,
	request transitionRequest,
) (WorkerHandoffSourceObservation, bool, error) {
	if m == nil || ctx == nil || request.domain != actorrouting.DomainFarmWorker {
		return WorkerHandoffSourceObservation{}, false, ErrInvalidManager
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if memo := m.transitionMemoLocked(request.transitionID); memo != nil {
		if memo.fingerprint != transitionFingerprint(request) {
			return WorkerHandoffSourceObservation{}, false, fmt.Errorf(
				"%w: transition_id=%s",
				ErrTransitionReconcile,
				request.transitionID,
			)
		}
		if memo.result.Status == authoritystore.ShardTransitionApplied ||
			memo.result.Status == authoritystore.ShardTransitionAlreadyApplied {
			// Authority already moved. The normal transition reconciliation path
			// owns any remaining ACTIVE publication; never drain the new holder.
			return WorkerHandoffSourceObservation{}, false, nil
		}
	}
	if m.pendingLeaseProjection != nil {
		return WorkerHandoffSourceObservation{}, false, ErrLeaseProjectionPending
	}
	table := snapshotDomain(m.snapshot, request.domain)
	if request.virtualShard >= table.ShardCount ||
		int(request.virtualShard) >= len(table.Routes) {
		return WorkerHandoffSourceObservation{}, false, ErrAuthorityAddress
	}
	route := table.Routes[request.virtualShard]
	if route.HolderIndex < 0 || route.HolderIndex >= len(m.snapshot.Holders) {
		return WorkerHandoffSourceObservation{}, false, ErrAuthorityAddress
	}
	holder := m.snapshot.Holders[route.HolderIndex]
	if holder.InstanceID != request.expectedFence.ExpectedHolderID ||
		holder.BootID != request.expectedFence.ExpectedHolderBootID ||
		route.ShardRevision != request.expectedFence.ExpectedAssignmentRevision ||
		route.ExpectedWriterEpoch != request.expectedFence.ExpectedWriterEpoch ||
		route.ExpectedLeaseEpoch != request.expectedFence.ExpectedLeaseEpoch ||
		(route.State != actorrouting.ShardStateActive &&
			route.State != actorrouting.ShardStateDraining) {
		return WorkerHandoffSourceObservation{}, false, ErrStaleTransition
	}
	expectedServing := uint32(0)
	for _, candidate := range table.Routes {
		if candidate.State != actorrouting.ShardStateActive ||
			candidate.HolderIndex < 0 ||
			candidate.HolderIndex >= len(m.snapshot.Holders) {
			continue
		}
		candidateHolder := m.snapshot.Holders[candidate.HolderIndex]
		if candidateHolder.InstanceID == holder.InstanceID &&
			candidateHolder.BootID == holder.BootID {
			expectedServing++
		}
	}
	if route.State == actorrouting.ShardStateActive {
		if expectedServing == 0 {
			return WorkerHandoffSourceObservation{}, false, ErrTransitionReconcile
		}
		expectedServing--
		next := actorrouting.CloneRouteSnapshot(m.snapshot)
		nextTable := snapshotDomain(next, request.domain)
		nextRoute := nextTable.Routes[request.virtualShard]
		nextRoute.State = actorrouting.ShardStateDraining
		nextTable.Routes[request.virtualShard] = nextRoute
		setSnapshotDomain(&next, request.domain, nextTable)
		next.GlobalRevision++
		if err := actorrouting.ValidateWorkerHandoffDrainingSnapshotUpdate(m.snapshot, next); err != nil {
			return WorkerHandoffSourceObservation{}, false, fmt.Errorf(
				"%w: worker draining snapshot: %v",
				ErrSnapshotPublish,
				err,
			)
		}
		if err := m.publishSnapshot(ctx, next, false); err != nil {
			if snapshotPublishWasCommitted(err) {
				m.snapshot = next
			}
			return WorkerHandoffSourceObservation{}, false, fmt.Errorf(
				"%w: worker draining: %v",
				ErrSnapshotPublish,
				err,
			)
		}
		m.snapshot = next
	}
	return WorkerHandoffSourceObservation{
		Domain:                request.domain,
		VirtualShard:          request.virtualShard,
		SourceHolderID:        holder.InstanceID,
		SourceHolderBootID:    holder.BootID,
		SnapshotRevision:      m.snapshot.GlobalRevision,
		ExpectedServingShards: expectedServing,
	}, true, nil
}

func transferRequest(req TransferRequest) transitionRequest {
	return transitionRequest{
		kind: transitionTransfer, transitionID: req.TransitionID, domain: req.Domain,
		virtualShard: req.VirtualShard, expectedFence: req.ExpectedFence,
		newHolderID: req.NewHolderID, newHolderBoot: req.NewHolderBootID,
		sourcePodUID: req.SourcePodUID, sourceWorkloadUID: req.SourceWorkloadUID, sourceOrdinal: req.SourceOrdinal,
		targetPodUID: req.TargetPodUID, targetWorkloadUID: req.TargetWorkloadUID, targetOrdinal: req.TargetOrdinal,
		deploymentIdentityPresent: req.DeploymentIdentityPresent,
		endpoint:                  req.Endpoint, ttl: req.RequestedTTL, mode: req.Mode, proof: req.TakeoverProof,
	}
}

// ObserveTransition is the sole read-only verification entry point for
// Manager transition intent and replay evidence. It never recovers, publishes,
// mutates Authority, or extends retention.
func (m *RouteAssignmentManager) ObserveTransition(ctx context.Context, observation TransitionObservationRequest) (TransitionObservation, error) {
	if m == nil || ctx == nil {
		return TransitionObservation{}, ErrInvalidManager
	}
	if _, bounded := ctx.Deadline(); !bounded {
		return TransitionObservation{}, fmt.Errorf("%w: observation deadline required", ErrInvalidManager)
	}
	request := transferRequest(observation.Transfer)
	var err error
	if observation.PlannedDrain {
		if observation.Transfer.Domain == actorrouting.DomainFarmWorker {
			request, err = normalizedWorkerHandoffRequest(observation.Transfer)
		} else {
			request, err = normalizedDrainRequest(observation.Transfer)
		}
	} else {
		if err = validateTransitionRequest(request); err == nil {
			var ttl int64
			ttl, err = durationMillisForTransition(request)
			request.ttl = time.Duration(ttl) * time.Millisecond
		}
	}
	if err != nil {
		return TransitionObservation{}, err
	}
	fingerprint := transitionFingerprint(request)
	store, ok := m.tombstones.(ExactTransitionEvidenceStore)
	if !ok {
		return TransitionObservation{}, fmt.Errorf("%w: exact durable evidence store required", ErrTransitionReconcile)
	}

	// Manager transition and eviction paths hold the same mutex while changing
	// durable evidence, making the durable read and hot-state comparison one
	// Manager-owned observation boundary.
	m.mu.Lock()
	defer m.mu.Unlock()
	recovery, recoveryFound, tombstone, tombstoneFound, err := store.ObserveTransitionEvidence(ctx, request.transitionID)
	if err != nil {
		return TransitionObservation{}, fmt.Errorf("%w: durable observation", ErrTransitionReconcile)
	}
	if recoveryFound == tombstoneFound {
		return TransitionObservation{}, fmt.Errorf("%w: exact durable evidence missing or overlapping", ErrTransitionReconcile)
	}
	if recoveryFound {
		durableRequest, decodeErr := requestFromRecovery(recovery)
		memo := m.transitionMemoLocked(request.transitionID)
		if decodeErr != nil || recovery.Fingerprint != fingerprint || transitionFingerprint(durableRequest) != fingerprint ||
			memo == nil || memo.fingerprint != fingerprint || memo.request != request || memo.result != recovery.Result ||
			memo.commitAmbiguous || memo.projectionPending || memo.inFlight || memo.blocked || !memo.published ||
			memo.lastState != actorrouting.ShardStateActive || !recovery.Committed || !recovery.Published ||
			recovery.LastState != actorrouting.ShardStateActive || !transitionRecoveryEvidenceEqual(recoveryForMemo(memo), recovery) ||
			transitionMustBeCommitted(recovery.Result) != nil {
			return TransitionObservation{}, fmt.Errorf("%w: incomplete or mismatched retained recovery", ErrTransitionReconcile)
		}
		if err := m.validateCommittedAuthorityResultLocked(request, recovery.Result, false); err != nil {
			return TransitionObservation{}, fmt.Errorf("%w: retained recovery result: %v", ErrTransitionReconcile, err)
		}
		return TransitionObservation{
			Class: TransitionEvidenceCompleteRecovery, Fingerprint: fingerprint,
			Result: recovery.Result, Recovery: recovery,
		}, nil
	}
	if tombstone.TransitionID != request.transitionID || tombstone.Fingerprint != fingerprint ||
		m.transitionMemoLocked(request.transitionID) != nil || transitionMustBeCommitted(tombstone.Result) != nil {
		return TransitionObservation{}, fmt.Errorf("%w: mismatched terminal tombstone", ErrTransitionReconcile)
	}
	if err := m.validateCommittedAuthorityResultLocked(request, tombstone.Result, false); err != nil {
		return TransitionObservation{}, fmt.Errorf("%w: terminal tombstone result: %v", ErrTransitionReconcile, err)
	}
	return TransitionObservation{
		Class: TransitionEvidenceTombstone, Fingerprint: fingerprint,
		Result: tombstone.Result, Tombstone: tombstone,
	}, nil
}

// Unassign performs Authority Unassign first and then publishes a non-active
// assignment. The previous holder index is retained only as an inert route
// reference; Resolve fails closed because the shard state is ASSIGNING.
func (m *RouteAssignmentManager) Unassign(ctx context.Context, req UnassignRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	releaseOperation, err := m.acquireOperation(ctx)
	if err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, err
	}
	defer releaseOperation()
	return m.runTransition(ctx, transitionRequest{
		kind: transitionUnassign, transitionID: req.TransitionID, domain: req.Domain,
		virtualShard: req.VirtualShard, expectedFence: req.ExpectedFence,
	})
}

// Renew is deliberately separate from generation transitions. A holder may
// renew only its own current tuple. It never publishes a generation change,
// but may publish a same-generation deadline projection when the durable
// authority deadline is ahead of the currently published route.
func (m *RouteAssignmentManager) Renew(ctx context.Context, req RenewRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	if m == nil || m.store == nil {
		return authoritystore.ShardAuthorityTransitionResult{}, ErrInvalidManager
	}
	if req.Caller.Kind != authoritystore.ShardAuthorityCallerHolder || req.Caller.InstanceID == "" || req.Caller.BootID == "" ||
		req.ExpectedFence.ExpectedHolderID != req.Caller.InstanceID || req.ExpectedFence.ExpectedHolderBootID != req.Caller.BootID {
		return authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionUnauthorized}, ErrHolderRenewOnly
	}
	ttl, err := durationMillis(req.RequestedTTL)
	if err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, err
	}
	releaseOperation, err := m.acquireOperation(ctx)
	if err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, err
	}
	defer releaseOperation()
	// Renew shares the transition mutex with Claim/Transfer/Unassign. This is
	// deliberate: a delayed holder response must not update local bookkeeping
	// after a newer RouteManager transition has linearized for the same shard.
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pendingLeaseProjection != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, ErrLeaseProjectionPending
	}
	if flight := m.drainFlights[drainKey{domain: req.Domain, shard: req.VirtualShard}]; flight != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, ErrDrainInFlight
	}
	if m.initialBatchOpen {
		return authoritystore.ShardAuthorityTransitionResult{}, routecontrol.ErrInitialAuthorityBatchOpen
	}
	key := authorityKey{domain: req.Domain, shard: req.VirtualShard}
	if !routeMatchesFence(m.snapshot, req.Domain, req.VirtualShard, req.ExpectedFence) {
		return authoritystore.ShardAuthorityTransitionResult{}, fmt.Errorf("%w: local route is not the generation being renewed", ErrStaleTransition)
	}
	result, err := m.store.RenewShardAuthority(ctx, authoritystore.RenewShardAuthorityRequest{
		Caller: req.Caller, AuthorityLayoutVersion: m.layout, Domain: req.Domain,
		VirtualShard: req.VirtualShard, ExpectedFence: req.ExpectedFence, RequestedTTLms: ttl,
	})
	if err == nil && (result.Status == authoritystore.ShardTransitionApplied || result.Status == authoritystore.ShardTransitionAlreadyApplied) {
		if err := validateRenewResult(m.layout, m.snapshot.HashVersion, m.snapshot.HashSeed, req, result); err != nil {
			return result, err
		}
		if current, ok := m.committed[key]; ok {
			if !sameAuthorityGeneration(current, result.Authority) {
				return result, fmt.Errorf("%w: renewal generation differs from local transition", ErrStaleTransition)
			}
			if current.LastTransitionID != result.Authority.LastTransitionID {
				return result, fmt.Errorf("%w: renewal transition identity changed", ErrRenewGenerationChanged)
			}
		}
		// A renewal deadline is a projection update, not a new generation. If
		// an authoritative reader exists, it must prove the current tuple and
		// deadline. Without one, the current complete local route is the only
		// safe lower bound; a response below it is rejected rather than silently
		// rolling a restarted manager's projection backwards.
		hasAuthorityReader := m.reader != nil
		if _, hasInspector := m.store.(shardAuthorityInspector); hasInspector {
			hasAuthorityReader = true
		}
		if hasAuthorityReader {
			current, observed, observeErr := m.readAuthorityLocked(ctx, key)
			if observeErr != nil {
				return result, fmt.Errorf("%w: renewal authority read: %v", ErrAuthorityReconcile, observeErr)
			}
			if !observed {
				return result, fmt.Errorf("%w: renewal authority is absent", ErrAuthorityReconcile)
			}
			if !sameAuthorityGeneration(current, result.Authority) {
				return result, fmt.Errorf("%w: authoritative generation moved during renewal", ErrStaleTransition)
			}
			if result.Authority.LeaseUntilMs < current.LeaseUntilMs {
				return result, fmt.Errorf("%w: renewal deadline regressed from %d to %d", ErrStaleTransition, current.LeaseUntilMs, result.Authority.LeaseUntilMs)
			}
		} else {
			route := snapshotDomain(m.snapshot, req.Domain).Routes[req.VirtualShard]
			if route.LeaseUntilMs <= 0 || result.Authority.LeaseUntilMs < route.LeaseUntilMs {
				return result, fmt.Errorf("%w: renewal deadline cannot be proven against local route", ErrAuthorityReconcile)
			}
			if current, ok := m.committed[key]; ok && sameAuthorityGeneration(current, result.Authority) && result.Authority.LeaseUntilMs < current.LeaseUntilMs {
				return result, fmt.Errorf("%w: renewal deadline regressed from committed cache", ErrStaleTransition)
			}
		}
		authority := result.Authority
		if authority.LeaseUntilMs <= m.now().UnixMilli() {
			return result, ErrAuthorityLeaseExpired
		}
		if cached, ok := m.committed[key]; !ok || sameAuthorityGeneration(cached, authority) && authority.LeaseUntilMs >= cached.LeaseUntilMs {
			m.committed[key] = authority
		}
		if routeMatchesAuthority(m.snapshot, authority, actorrouting.ShardStateActive, "") {
			return result, nil
		}
		next, snapshotErr := m.snapshotForAuthority(m.snapshot, authority, actorrouting.ShardStateActive, "")
		if snapshotErr != nil {
			return result, snapshotErr
		}
		if snapshotErr = actorrouting.ValidateRouteSnapshotUpdate(m.snapshot, next); snapshotErr != nil {
			return result, snapshotErr
		}
		if publishErr := m.publishSnapshot(ctx, next, false); publishErr != nil {
			return result, fmt.Errorf("%w: renewal projection: %v", ErrSnapshotPublish, publishErr)
		}
		m.snapshot = next
	}
	return result, err
}

func (m *RouteAssignmentManager) runTransition(ctx context.Context, request transitionRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	return m.runTransitionValidated(ctx, request, nil)
}

func (m *RouteAssignmentManager) runTransitionValidated(ctx context.Context, request transitionRequest, targetValidator PendingProjectionTargetValidator) (authoritystore.ShardAuthorityTransitionResult, error) {
	if m == nil || m.store == nil || m.publisher == nil {
		return authoritystore.ShardAuthorityTransitionResult{}, ErrInvalidManager
	}
	if err := validateTransitionRequest(request); err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, err
	}
	if request.kind != transitionUnassign {
		m.placementMu.RLock()
		defer m.placementMu.RUnlock()
	}
	ttl, err := durationMillisForTransition(request)
	if err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, err
	}
	request.ttl = time.Duration(ttl) * time.Millisecond
	fingerprint := transitionFingerprint(request)
	validationRecovery := TransitionRecovery{}
	if targetValidator != nil {
		validationRecovery = recoveryForRequest(request, fingerprint, authoritystore.ShardAuthorityTransitionResult{}, m.now())
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pendingLeaseProjection != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, ErrLeaseProjectionPending
	}
	if flight := m.drainFlights[drainKey{domain: request.domain, shard: request.virtualShard}]; flight != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, ErrDrainInFlight
	}
	if err := m.pruneMemosLocked(m.now()); err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, err
	}
	if memo := m.transitionMemoLocked(request.transitionID); memo != nil {
		if memo.fingerprint != fingerprint {
			return memo.result, fmt.Errorf("%w: transition_id=%s", ErrTransitionReconcile, request.transitionID)
		}
		memo.lastUsedAt = m.now()
		if memo.commitAmbiguous {
			if err := m.promoteAmbiguousMemoLocked(ctx, memo); err != nil {
				return memo.result, err
			}
		}
		if memo.blocked {
			if memo.blockErr != nil {
				return memo.result, memo.blockErr
			}
			return memo.result, ErrStaleTransition
		}
		if memo.published {
			return memo.result, nil
		}
		// The authority already committed but snapshot publication may have
		// failed. Retry only the same full snapshot; never call the store again.
		if err := m.publishMemoLocked(ctx, memo, memo.lastState, true, validationRecovery, targetValidator); err != nil {
			return memo.result, err
		}
		return memo.result, nil
	}
	if tombstone, found, err := m.transitionTombstoneLocked(request.transitionID); err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, err
	} else if found {
		if tombstone.Fingerprint != fingerprint {
			return tombstone.Result, fmt.Errorf("%w: transition_id=%s expired with a different digest", ErrTransitionReconcile, request.transitionID)
		}
		return tombstone.Result, fmt.Errorf("%w: transition_id=%s", ErrTransitionMemoExpired, request.transitionID)
	}
	if m.initialBatchOpen && !m.initialBatchClaimAllowedLocked(request) {
		return authoritystore.ShardAuthorityTransitionResult{}, routecontrol.ErrInitialAuthorityBatchOpen
	}
	if err := m.validateRequestAgainstSnapshotLocked(request); err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, err
	}
	if err := m.reserveTransitionSlotLocked(request.transitionID, fingerprint, m.now()); err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, err
	}
	if err := m.reserveRecoveryLocked(request, fingerprint, m.now()); err != nil {
		m.releaseTransitionReservationLocked(request.transitionID, fingerprint)
		return authoritystore.ShardAuthorityTransitionResult{}, fmt.Errorf("%w: recovery reservation: %v", ErrTransitionTombstoneCap, err)
	}
	if err := m.validatePendingProjectionTargetLocked(ctx, request, validationRecovery, targetValidator); err != nil {
		if releaseErr := m.discardPrecommitRecoveryLocked(request.transitionID, fingerprint); releaseErr != nil {
			return authoritystore.ShardAuthorityTransitionResult{}, errors.Join(err, releaseErr)
		}
		return authoritystore.ShardAuthorityTransitionResult{}, err
	}
	if err := m.markAuthorityAttemptedReleasingLock(request, fingerprint, routeStateFor(request.kind)); err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, fmt.Errorf("%w: persist Authority attempt boundary: %v", ErrAuthorityUnavailable, err)
	}
	var result authoritystore.ShardAuthorityTransitionResult
	_ = m.runWithoutStateLock(func() error {
		result, err = m.commitLocked(ctx, request, ttl)
		return nil
	})
	if transitionOutcomeDefinitivelyNotSent(result) {
		if releaseErr := m.discardPrecommitRecoveryLocked(request.transitionID, fingerprint); releaseErr != nil {
			return result, releaseErr
		}
		outcomeErr := transitionMustBeCommitted(result)
		if err != nil {
			return result, fmt.Errorf("%w: store response: %v", outcomeErr, err)
		}
		return result, outcomeErr
	}
	if err != nil || transitionOutcomeAmbiguous(result) {
		m.retainAmbiguousTransitionLocked(request, fingerprint, result, routeStateFor(request.kind))
		if err != nil {
			return result, fmt.Errorf("%w: store response: %v", ErrAuthorityCommitUnknown, err)
		}
		return result, transitionMustBeCommitted(result)
	}
	if err := transitionMustBeCommitted(result); err != nil {
		if releaseErr := m.discardPrecommitRecoveryLocked(request.transitionID, fingerprint); releaseErr != nil {
			return result, releaseErr
		}
		return result, err
	}
	if err := m.validateCommittedAuthorityLocked(request, result); err != nil {
		m.retainAmbiguousTransitionLocked(request, fingerprint, result, routeStateFor(request.kind))
		return result, err
	}
	bootstrapProjection := m.bootstrap[authorityKey{domain: request.domain, shard: request.virtualShard}]
	if m.initialBatchOpen && !bootstrapProjection && request.kind == transitionClaim && result.Status == authoritystore.ShardTransitionAlreadyApplied {
		routes := snapshotDomain(m.snapshot, request.domain)
		if request.virtualShard < routes.ShardCount && int(request.virtualShard) < len(routes.Routes) && isInitialClaimPlaceholder(routes.Routes[request.virtualShard]) {
			// A crash-resumed initial claim has no old in-memory tombstone or
			// recovery marker. The AuthorityStore's exact transition identity,
			// holder and profile checks already proved this ALREADY_APPLIED result.
			bootstrapProjection = true
		}
	}
	m.consumeTransitionReservationLocked(request.transitionID, fingerprint)
	delete(m.bootstrap, authorityKey{domain: request.domain, shard: request.virtualShard})
	m.committed[authorityKey{domain: request.domain, shard: request.virtualShard}] = result.Authority
	now := m.now()
	memo := &transitionMemo{
		fingerprint: fingerprint, result: result, request: request,
		lastState: routeStateFor(request.kind), projectionPending: true,
		authorityAttempted: true, createdAt: now, lastUsedAt: now,
	}
	memo.bootstrapProjection = bootstrapProjection
	m.transitions[request.transitionID] = memo
	if err := m.commitRecoveryReleasingLock(memo); err != nil {
		// Authority is already committed. Keep the pinned in-memory memo and
		// fail closed; a later retry can only use this exact result.
		return result, fmt.Errorf("%w: recovery commit: %v", ErrSnapshotPublish, err)
	}
	if err := m.publishMemoLocked(ctx, memo, memo.lastState, false, validationRecovery, targetValidator); err != nil {
		if pruneErr := m.pruneMemosLocked(m.now()); pruneErr != nil {
			return result, fmt.Errorf("%w: %v", err, pruneErr)
		}
		return result, err
	}
	return result, nil
}

// runWithoutStateLock releases Manager.mu for independent per-shard store I/O
// so sibling Drain CAS can overlap, then reacquires it. The caller must
// already hold m.mu. Independent shard keys stay fenced; GlobalRevision stays
// a cursor and is not used as a cross-shard fence.
func (m *RouteAssignmentManager) runWithoutStateLock(fn func() error) error {
	m.mu.Unlock()
	defer m.mu.Lock()
	return fn()
}

func (m *RouteAssignmentManager) commitLocked(ctx context.Context, request transitionRequest, ttl int64) (authoritystore.ShardAuthorityTransitionResult, error) {
	switch request.kind {
	case transitionClaim:
		return m.store.ClaimShardAuthority(ctx, authoritystore.ClaimShardAuthorityRequest{
			Caller: m.caller, TransitionID: request.transitionID, AuthorityLayoutVersion: m.layout,
			Domain: request.domain, VirtualShard: request.virtualShard,
			NewHolderID: request.newHolderID, NewHolderBootID: request.newHolderBoot,
			RequestedTTLms: ttl,
		})
	case transitionTransfer:
		if request.mode == TransferModeCrashTakeover && request.proof.LeaseExpired {
			store, ok := m.store.(authoritystore.ExpiredTakeoverShardAuthorityStore)
			if !ok {
				return authoritystore.ShardAuthorityTransitionResult{}, fmt.Errorf("%w: expired takeover store required", ErrInvalidManager)
			}
			return store.TakeoverExpiredShardAuthority(ctx, authoritystore.ExpiredTakeoverShardAuthorityRequest{
				Caller: m.caller, TransitionID: request.transitionID, AuthorityLayoutVersion: m.layout,
				Domain: request.domain, VirtualShard: request.virtualShard,
				ExpectedFence: request.expectedFence, NewHolderID: request.newHolderID,
				NewHolderBootID: request.newHolderBoot, RequestedTTLms: ttl,
			})
		}
		return m.store.TransferShardAuthority(ctx, authoritystore.TransferShardAuthorityRequest{
			Caller: m.caller, TransitionID: request.transitionID, AuthorityLayoutVersion: m.layout,
			Domain: request.domain, VirtualShard: request.virtualShard,
			ExpectedFence: request.expectedFence, NewHolderID: request.newHolderID,
			NewHolderBootID: request.newHolderBoot, RequestedTTLms: ttl,
		})
	case transitionUnassign:
		return m.store.UnassignShardAuthority(ctx, authoritystore.UnassignShardAuthorityRequest{
			Caller: m.caller, TransitionID: request.transitionID, AuthorityLayoutVersion: m.layout,
			Domain: request.domain, VirtualShard: request.virtualShard, ExpectedFence: request.expectedFence,
		})
	default:
		return authoritystore.ShardAuthorityTransitionResult{}, ErrInvalidManager
	}
}

func (m *RouteAssignmentManager) publishMemoLocked(ctx context.Context, memo *transitionMemo, state actorrouting.ShardRouteState, requireAuthoritativeRead bool, recovery TransitionRecovery, targetValidator PendingProjectionTargetValidator) error {
	if memo == nil {
		return ErrInvalidManager
	}
	if memo.blocked {
		if memo.blockErr != nil {
			return memo.blockErr
		}
		return ErrStaleTransition
	}
	authority := memo.result.Authority
	if requireAuthoritativeRead {
		var err error
		authority, err = m.reconcileAuthorityLocked(ctx, memo)
		if err != nil {
			if errors.Is(err, ErrStaleTransition) {
				memo.blocked = true
				memo.blockErr = err
			}
			return err
		}
	}
	projectionComplete := state == actorrouting.ShardStateActive || memo.request.kind == transitionUnassign
	if routeMatchesAuthority(m.snapshot, authority, state, memo.request.endpoint) {
		if err := validateTerminalPublishedAuthority(memo.result, authority, state); err != nil {
			return err
		}
		// Exact ACTIVE already installed is terminal publication evidence even
		// when the recovery-marker update failed after publish. A later exclusion
		// gates only fresh target effects; it cannot make this completed transition
		// unreplayable. Nonterminal route matches still require admission.
		if state != actorrouting.ShardStateActive {
			if err := m.validatePendingProjectionTargetLocked(ctx, memo.request, recovery, targetValidator); err != nil {
				return err
			}
		}
		previousState, previousRevision, previousPublished, previousPending, previousUsedAt := memo.lastState, memo.publishedSnapshotRevision, memo.published, memo.projectionPending, memo.lastUsedAt
		memo.lastState = state
		if state == actorrouting.ShardStateAssigning && memo.publishedSnapshotRevision == 0 {
			memo.publishedSnapshotRevision = m.snapshot.GlobalRevision
		}
		memo.published = true
		memo.projectionPending = !projectionComplete
		memo.lastUsedAt = m.now()
		if err := m.updateRecoveryLocked(memo); err != nil {
			memo.lastState, memo.publishedSnapshotRevision, memo.published, memo.projectionPending, memo.lastUsedAt = previousState, previousRevision, previousPublished, previousPending, previousUsedAt
			return fmt.Errorf("%w: transition recovery update: %v", ErrSnapshotPublish, err)
		}
		return nil
	}
	next, err := m.snapshotForAuthority(m.snapshot, authority, state, memo.request.endpoint)
	if err != nil {
		return err
	}
	currentRoute := snapshotDomain(m.snapshot, authority.Domain).Routes[authority.VirtualShard]
	workerHandoffActivation := memo.request.internalDrain &&
		memo.request.mode == TransferModeWorkerHandoff &&
		memo.request.domain == actorrouting.DomainFarmWorker &&
		currentRoute.State == actorrouting.ShardStateDraining &&
		state == actorrouting.ShardStateActive
	authorityActivation := false
	if !workerHandoffActivation &&
		memo.bootstrapProjection &&
		currentRoute.State != actorrouting.ShardStateActive &&
		state == actorrouting.ShardStateActive {
		authorityActivation = true
	} else if memo.request.internalDrain &&
		memo.request.mode == TransferModeDrainHandoff &&
		currentRoute.State == actorrouting.ShardStateAssigning &&
		state == actorrouting.ShardStateActive {
		authorityActivation = true
	}
	if workerHandoffActivation {
		err = actorrouting.ValidateWorkerHandoffActivationSnapshotUpdate(
			m.snapshot,
			next,
		)
	} else if authorityActivation {
		err = actorrouting.ValidateAuthoritySnapshotUpdate(m.snapshot, next)
	} else {
		err = actorrouting.ValidateRouteSnapshotUpdate(m.snapshot, next)
	}
	if err != nil {
		return err
	}
	// Re-check immediately before the external publication call. An
	// ALREADY_APPLIED result may have had only a short remaining lease while
	// snapshot construction/validation was running; publishing after that
	// deadline would install an authority-backed ACTIVE route with no lease.
	if err := validateAuthorityForPublish(m.now(), memo.result, authority, state); err != nil {
		return err
	}
	if err := m.validatePendingProjectionTargetLocked(ctx, memo.request, recovery, targetValidator); err != nil {
		return err
	}
	if m.shouldCoalesceDrainSnapshotLocked(memo) {
		return m.coalesceDrainSnapshotLocked(ctx, memo, state, authority, authorityActivation, projectionComplete)
	}
	var publishErr error
	if authorityActivation {
		_, ok := m.publisher.(authoritySnapshotPublisher)
		if !ok {
			return fmt.Errorf("%w: publisher lacks authority activation path", ErrSnapshotPublish)
		}
		publishErr = m.publishSnapshot(ctx, next, true)
	} else {
		publishErr = m.publishSnapshot(ctx, next, false)
	}
	if publishErr != nil {
		return fmt.Errorf("%w: transition_id=%s: %v", ErrSnapshotPublish, memo.request.transitionID, publishErr)
	}
	m.snapshot = next
	previousState, previousRevision, previousPublished, previousPending, previousUsedAt := memo.lastState, memo.publishedSnapshotRevision, memo.published, memo.projectionPending, memo.lastUsedAt
	memo.lastState = state
	if state == actorrouting.ShardStateAssigning && memo.publishedSnapshotRevision == 0 {
		memo.publishedSnapshotRevision = next.GlobalRevision
	}
	memo.published = true
	memo.projectionPending = !projectionComplete
	memo.lastUsedAt = m.now()
	if err := m.updateRecoveryLocked(memo); err != nil {
		memo.lastState, memo.publishedSnapshotRevision, memo.published, memo.projectionPending, memo.lastUsedAt = previousState, previousRevision, previousPublished, previousPending, previousUsedAt
		return fmt.Errorf("%w: transition recovery update: %v", ErrSnapshotPublish, err)
	}
	// Capacity was preflighted before the AuthorityStore side effect (and a
	// Drain reservation remains pinned through cold load). Do not run a
	// second eviction after publishing: a tombstone-store error must never
	// turn an already published authority transition into an apparent failure.
	return nil
}

func (m *RouteAssignmentManager) validatePendingProjectionTargetLocked(ctx context.Context, request transitionRequest, recovery TransitionRecovery, validator PendingProjectionTargetValidator) error {
	if request.kind == transitionUnassign {
		return nil
	}
	// Public transition entry points construct a pre-commit recovery identity
	// before they inspect an existing durable memo. During replay that fallback
	// must not hide the stronger committed checkpoint: the target may already
	// have entered its fail-closed WARMING/zero-shard state while waiting for the
	// exact RouteSnapshot projection. Refresh from the memo under m.mu so target
	// admission can distinguish that recovery from a fresh placement attempt.
	if memo := m.transitionMemoLocked(request.transitionID); memo != nil && memo.fingerprint == transitionFingerprint(request) {
		memoRecovery := recoveryForMemo(memo)
		if recoveryAuthorityCommitted(memoRecovery) {
			recovery = memoRecovery
		}
	}
	// A transition that already reached ACTIVE before exclusion is a terminal
	// idempotent replay, not a fresh placement. Exclusion gates every incomplete
	// target effect and publication boundary, but must not make a completed
	// RoutePlan unrecoverable merely because the holder was excluded later.
	if recovery.Published && recovery.LastState == actorrouting.ShardStateActive {
		return nil
	}
	requiredRevision := m.snapshot.GlobalRevision
	if recovery.PublishedSnapshotRevision > 0 {
		requiredRevision = recovery.PublishedSnapshotRevision
	}
	return m.runWithoutStateLock(func() error {
		return m.validatePlacementTargetWithRevision(ctx, request, recovery, validator, requiredRevision)
	})
}

func (m *RouteAssignmentManager) validatePlacementTargetWithRevision(ctx context.Context, request transitionRequest, recovery TransitionRecovery, validator PendingProjectionTargetValidator, snapshotRevision uint64) error {
	if m.placementExclusions == nil {
		return fmt.Errorf("%w: placement exclusion admission unavailable", ErrProjectionRecovery)
	} else if revision, err := m.placementExclusions.ValidatePlacementTarget(ctx, request.domain, request.newHolderID, request.newHolderBoot); err != nil {
		return fmt.Errorf("%w: placement admission revision=%d: %v", ErrProjectionRecovery, revision, err)
	}
	if validator != nil {
		if recovery.TransitionID == "" || recovery.Fingerprint != transitionFingerprint(request) {
			return fmt.Errorf("%w: invalid target validation identity", ErrProjectionRecovery)
		}
		if err := validator(ctx, recovery, snapshotRevision); err != nil {
			return fmt.Errorf("%w: target facts: %v", ErrProjectionRecovery, err)
		}
	}
	return nil
}

func (m *RouteAssignmentManager) validatePendingProjectionTarget(ctx context.Context, request transitionRequest, recovery TransitionRecovery, validator PendingProjectionTargetValidator) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.validatePendingProjectionTargetLocked(ctx, request, recovery, validator)
}

// waitPendingProjectionTarget revalidates exact target HolderRegistry facts after
// ASSIGNING revision N is published. It does not hold m.mu across waits, does not
// mutate Authority, and never fabricates readiness: each attempt uses the existing
// production validator capability. Timeout/cancel returns the last fail-closed
// validation error (joined with ctx.Err when canceled) so callers preserve
// authority-committed/snapshot-pending recovery semantics.
func (m *RouteAssignmentManager) waitPendingProjectionTarget(ctx context.Context, request transitionRequest, recovery TransitionRecovery, validator PendingProjectionTargetValidator) error {
	if m == nil {
		return ErrInvalidManager
	}
	if validator == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: target convergence context required", ErrInvalidManager)
	}
	err := m.validatePendingProjectionTarget(ctx, request, recovery, validator)
	if err == nil || m.targetConvergenceTimeout <= 0 {
		return err
	}
	after := m.after
	if after == nil {
		after = time.After
	}
	backoff := m.targetConvergenceBackoff
	if backoff <= 0 {
		backoff = defaultTargetConvergenceBackoff
	}
	deadline := m.now().Add(m.targetConvergenceTimeout)
	for {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return errors.Join(err, ctxErr)
		}
		remaining := deadline.Sub(m.now())
		if remaining <= 0 {
			return err
		}
		wait := backoff
		if wait > remaining {
			wait = remaining
		}
		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-after(wait):
		}
		nextErr := m.validatePendingProjectionTarget(ctx, request, recovery, validator)
		if nextErr == nil {
			return nil
		}
		err = nextErr
	}
}

type shardAuthorityInspector interface {
	InspectShardWriteAuthority(actorrouting.RouteDomain, uint32) (authoritystore.ShardWriteAuthority, bool)
}

func (m *RouteAssignmentManager) reconcileAuthorityLocked(ctx context.Context, memo *transitionMemo) (authoritystore.ShardWriteAuthority, error) {
	if memo == nil {
		return authoritystore.ShardWriteAuthority{}, ErrInvalidManager
	}
	key := authorityKey{domain: memo.request.domain, shard: memo.request.virtualShard}
	var current authoritystore.ShardWriteAuthority
	var ok bool
	var err error
	_ = m.runWithoutStateLock(func() error {
		current, ok, err = m.readAuthorityLocked(ctx, key)
		return nil
	})
	if err != nil {
		return authoritystore.ShardWriteAuthority{}, fmt.Errorf("%w: read %s/%d: %v", ErrAuthorityReconcile, key.domain, key.shard, err)
	}
	if !ok {
		return authoritystore.ShardWriteAuthority{}, fmt.Errorf("%w: authority absent for %s/%d", ErrAuthorityReconcile, key.domain, key.shard)
	}
	if current.AuthorityLayoutVersion != m.layout || current.HashVersion != m.snapshot.HashVersion || current.HashSeed != m.snapshot.HashSeed ||
		current.Domain != memo.request.domain || current.VirtualShard != memo.request.virtualShard || current.LastTransitionID != memo.request.transitionID {
		return authoritystore.ShardWriteAuthority{}, fmt.Errorf("%w: transition_id=%s authoritative address/profile/identity differs", ErrStaleTransition, memo.request.transitionID)
	}
	if !sameAuthorityGeneration(current, memo.result.Authority) {
		return authoritystore.ShardWriteAuthority{}, fmt.Errorf("%w: transition_id=%s current=(holder=%s boot=%s assignment=%d writer=%d lease=%d) memo=(holder=%s boot=%s assignment=%d writer=%d lease=%d)",
			ErrStaleTransition, memo.request.transitionID,
			current.HolderID, current.HolderBootID, current.AssignmentRevision, current.WriterEpoch, current.LeaseEpoch,
			memo.result.Authority.HolderID, memo.result.Authority.HolderBootID, memo.result.Authority.AssignmentRevision, memo.result.Authority.WriterEpoch, memo.result.Authority.LeaseEpoch)
	}
	if current.LeaseUntilMs < memo.result.Authority.LeaseUntilMs {
		// RenewShardAuthority is monotonic for one immutable Authority
		// generation. A lower deadline therefore cannot be produced by a valid
		// holder renewal; keep the committed checkpoint pinned and fail closed.
		return authoritystore.ShardWriteAuthority{}, fmt.Errorf("%w: transition_id=%s authority deadline regressed from %d to %d",
			ErrStaleTransition, memo.request.transitionID, memo.result.Authority.LeaseUntilMs, current.LeaseUntilMs)
	}
	if current.LeaseUntilMs > memo.result.Authority.LeaseUntilMs {
		// LeaseUntilMs is a renewable liveness projection, not part of the
		// immutable per-shard Authority generation. Projection recovery can run
		// after the originally committed deadline has expired while the exact
		// target holder/boot has already renewed the same generation. Persist
		// that authenticated monotonic extension before any Snapshot or holder
		// side effect so another controller restart observes the same evidence.
		if current.LeaseUntilMs <= m.now().UnixMilli() {
			return authoritystore.ShardWriteAuthority{}, fmt.Errorf("%w: transition_id=%s renewed authority deadline expired at %d",
				ErrAuthorityLeaseExpired, memo.request.transitionID, current.LeaseUntilMs)
		}
		previousResult, previousUsedAt := memo.result, memo.lastUsedAt
		memo.result.Authority = current
		memo.lastUsedAt = m.now()
		if err := m.updateRecoveryLocked(memo); err != nil {
			memo.result, memo.lastUsedAt = previousResult, previousUsedAt
			return authoritystore.ShardWriteAuthority{}, fmt.Errorf("%w: persist renewed projection authority: %v", ErrSnapshotPublish, err)
		}
		m.committed[key] = current
	}
	routes := snapshotDomain(m.snapshot, key.domain)
	if key.shard >= routes.ShardCount || int(key.shard) >= len(routes.Routes) {
		return authoritystore.ShardWriteAuthority{}, fmt.Errorf("%w: local route shard out of range", ErrAuthorityReconcile)
	}
	localRoute := routes.Routes[key.shard]
	if localRoute.State == actorrouting.ShardStateActive && current.State == authoritystore.ShardAuthorityActive && current.LeaseUntilMs < localRoute.LeaseUntilMs {
		return authoritystore.ShardWriteAuthority{}, fmt.Errorf("%w: transition_id=%s authoritative deadline %d is below published deadline %d", ErrStaleTransition, memo.request.transitionID, current.LeaseUntilMs, localRoute.LeaseUntilMs)
	}
	return current, nil
}

func (m *RouteAssignmentManager) readAuthorityLocked(ctx context.Context, key authorityKey) (authoritystore.ShardWriteAuthority, bool, error) {
	if m == nil {
		return authoritystore.ShardWriteAuthority{}, false, ErrInvalidManager
	}
	if m.reader != nil {
		current, ok, err := m.reader.ReadShardWriteAuthority(ctx, key.domain, key.shard)
		return current, ok, err
	}
	if inspector, ok := m.store.(shardAuthorityInspector); ok {
		current, found := inspector.InspectShardWriteAuthority(key.domain, key.shard)
		return current, found, nil
	}
	// A manager-local cache is not authoritative after a committed publisher
	// failure or a process restart. Never use it to justify a replay.
	return authoritystore.ShardWriteAuthority{}, false, ErrAuthorityReconcile
}

func (m *RouteAssignmentManager) readAuthorityObservationLocked(ctx context.Context, key authorityKey) (authoritystore.ShardWriteAuthorityObservation, bool, error) {
	if m == nil || m.reader == nil {
		return authoritystore.ShardWriteAuthorityObservation{}, false, ErrAuthorityReconcile
	}
	reader, ok := m.reader.(AuthorityIntentReader)
	if !ok {
		return authoritystore.ShardWriteAuthorityObservation{}, false, fmt.Errorf("%w: intent-aware authority reader required", ErrAuthorityReconcile)
	}
	return reader.ReadShardWriteAuthorityObservation(ctx, key.domain, key.shard)
}

func sameAuthorityGeneration(left, right authoritystore.ShardWriteAuthority) bool {
	return left.AuthorityLayoutVersion == right.AuthorityLayoutVersion &&
		left.HashVersion == right.HashVersion && left.HashSeed == right.HashSeed &&
		left.Domain == right.Domain && left.VirtualShard == right.VirtualShard &&
		left.HolderID == right.HolderID && left.HolderBootID == right.HolderBootID &&
		left.AssignmentRevision == right.AssignmentRevision &&
		left.WriterEpoch == right.WriterEpoch && left.LeaseEpoch == right.LeaseEpoch &&
		left.State == right.State
}

func validateRenewResult(layout string, hashVersion uint32, hashSeed uint64, req RenewRequest, result authoritystore.ShardAuthorityTransitionResult) error {
	authority := result.Authority
	if result.AcceptedTTLms <= 0 || authority.State != authoritystore.ShardAuthorityActive ||
		authority.AuthorityLayoutVersion != layout ||
		authority.HashVersion != hashVersion || authority.HashSeed != hashSeed ||
		authority.Domain != req.Domain || authority.VirtualShard != req.VirtualShard ||
		authority.HolderID != req.Caller.InstanceID || authority.HolderBootID != req.Caller.BootID ||
		authority.AssignmentRevision != req.ExpectedFence.ExpectedAssignmentRevision ||
		authority.WriterEpoch != req.ExpectedFence.ExpectedWriterEpoch ||
		authority.LeaseEpoch != req.ExpectedFence.ExpectedLeaseEpoch || authority.LeaseUntilMs <= 0 {
		return fmt.Errorf("%w: renewal response is not the requested generation", ErrRenewGenerationChanged)
	}
	return nil
}

func validateTerminalPublishedAuthority(result authoritystore.ShardAuthorityTransitionResult, authority authoritystore.ShardWriteAuthority, state actorrouting.ShardRouteState) error {
	if state == actorrouting.ShardStateActive {
		if result.AcceptedTTLms <= 0 || authority.State != authoritystore.ShardAuthorityActive ||
			authority.HolderID == "" || authority.HolderBootID == "" || authority.AssignmentRevision == 0 ||
			authority.WriterEpoch == 0 || authority.LeaseEpoch == 0 || authority.LeaseUntilMs <= 0 {
			return fmt.Errorf("%w: installed ACTIVE route has invalid authority tuple", ErrAuthorityReconcile)
		}
		return nil
	}
	return validateAuthorityForPublish(time.UnixMilli(0), result, authority, state)
}

func validateAuthorityForPublish(now time.Time, result authoritystore.ShardAuthorityTransitionResult, authority authoritystore.ShardWriteAuthority, state actorrouting.ShardRouteState) error {
	if state == actorrouting.ShardStateActive || (state == actorrouting.ShardStateAssigning && authority.State == authoritystore.ShardAuthorityActive) {
		if result.AcceptedTTLms <= 0 || authority.State != authoritystore.ShardAuthorityActive || authority.HolderID == "" || authority.HolderBootID == "" || authority.AssignmentRevision == 0 || authority.WriterEpoch == 0 || authority.LeaseEpoch == 0 || authority.LeaseUntilMs <= now.UnixMilli() {
			return fmt.Errorf("%w: status=%s holder=%s boot=%s lease_until=%d accepted_ttl_ms=%d", ErrAuthorityLeaseExpired, result.Status, authority.HolderID, authority.HolderBootID, authority.LeaseUntilMs, result.AcceptedTTLms)
		}
		return nil
	}
	if result.Status != authoritystore.ShardTransitionApplied && result.Status != authoritystore.ShardTransitionAlreadyApplied {
		return fmt.Errorf("%w: unassign status=%s", ErrAuthorityRejected, result.Status)
	}
	if authority.State != authoritystore.ShardAuthorityUnassigned || authority.HolderID != "" || authority.HolderBootID != "" || authority.LeaseUntilMs != 0 || authority.AssignmentRevision == 0 || authority.WriterEpoch == 0 || authority.LeaseEpoch == 0 {
		return fmt.Errorf("%w: unassign authority is incomplete", ErrAuthorityReconcile)
	}
	return nil
}

func (m *RouteAssignmentManager) validateRequestAgainstSnapshotLocked(request transitionRequest) error {
	if m == nil {
		return ErrInvalidManager
	}
	routes := snapshotDomain(m.snapshot, request.domain)
	if request.virtualShard >= routes.ShardCount || int(request.virtualShard) >= len(routes.Routes) {
		return fmt.Errorf("%w: shard=%d", ErrAuthorityAddress, request.virtualShard)
	}
	route := routes.Routes[request.virtualShard]
	if route.ShardRevision == ^uint64(0) || route.ExpectedWriterEpoch == ^uint64(0) || route.ExpectedLeaseEpoch == ^uint64(0) || m.snapshot.GlobalRevision == ^uint64(0) {
		return ErrAuthorityOverflow
	}
	if request.expectedFence.ExpectedAssignmentRevision == ^uint64(0) || request.expectedFence.ExpectedWriterEpoch == ^uint64(0) || request.expectedFence.ExpectedLeaseEpoch == ^uint64(0) {
		return ErrAuthorityOverflow
	}
	if request.kind == transitionClaim {
		return nil
	}
	if !m.localFenceMatchesLocked(request.domain, request.virtualShard, request.expectedFence) {
		return fmt.Errorf("%w: expected fence does not match the local route", ErrStaleTransition)
	}
	return nil
}

func (m *RouteAssignmentManager) localFenceMatchesLocked(domain actorrouting.RouteDomain, shard uint32, fence authoritystore.ShardWriteFence) bool {
	if routeMatchesFence(m.snapshot, domain, shard, fence) {
		return true
	}
	if current, ok := m.committed[authorityKey{domain: domain, shard: shard}]; ok {
		return current.State == authoritystore.ShardAuthorityActive && current.HolderID == fence.ExpectedHolderID && current.HolderBootID == fence.ExpectedHolderBootID && current.AssignmentRevision == fence.ExpectedAssignmentRevision && current.WriterEpoch == fence.ExpectedWriterEpoch && current.LeaseEpoch == fence.ExpectedLeaseEpoch
	}
	return false
}

func (m *RouteAssignmentManager) validateCommittedAuthorityLocked(request transitionRequest, result authoritystore.ShardAuthorityTransitionResult) error {
	return m.validateCommittedAuthorityResultLocked(request, result, true)
}

// validateCommittedAuthorityResultLocked separates immutable transition
// identity from lease freshness. Normal commits require the returned lease to
// be fresh. Durable projection recovery validates the frozen result without a
// freshness requirement, then reconciles current exact Authority immediately
// before publication or holder preparation.
func (m *RouteAssignmentManager) validateCommittedAuthorityResultLocked(request transitionRequest, result authoritystore.ShardAuthorityTransitionResult, requireFreshLease bool) error {
	if m == nil {
		return ErrInvalidManager
	}
	authority := result.Authority
	if authority.AuthorityLayoutVersion != m.layout || authority.HashVersion != m.snapshot.HashVersion || authority.HashSeed != m.snapshot.HashSeed || authority.Domain != request.domain || authority.VirtualShard != request.virtualShard || authority.LastTransitionID != request.transitionID {
		return ErrAuthorityAddress
	}
	routes := snapshotDomain(m.snapshot, request.domain)
	if authority.VirtualShard >= routes.ShardCount || int(authority.VirtualShard) >= len(routes.Routes) {
		return ErrAuthorityAddress
	}
	route := routes.Routes[authority.VirtualShard]
	if authority.AssignmentRevision == 0 || authority.WriterEpoch == 0 || authority.LeaseEpoch == 0 || authority.AssignmentRevision == ^uint64(0) || authority.WriterEpoch == ^uint64(0) || authority.LeaseEpoch == ^uint64(0) {
		return ErrAuthorityOverflow
	}
	progressed := authority.AssignmentRevision == request.expectedFence.ExpectedAssignmentRevision+1 && authority.WriterEpoch == request.expectedFence.ExpectedWriterEpoch+1 && authority.LeaseEpoch == request.expectedFence.ExpectedLeaseEpoch+1
	claimProgressed := authority.AssignmentRevision > route.ShardRevision && authority.WriterEpoch > route.ExpectedWriterEpoch && authority.LeaseEpoch > route.ExpectedLeaseEpoch
	switch request.kind {
	case transitionClaim, transitionTransfer:
		if authority.State != authoritystore.ShardAuthorityActive || authority.HolderID == "" || authority.HolderBootID == "" || result.AcceptedTTLms <= 0 || authority.LeaseUntilMs <= 0 ||
			(requireFreshLease && authority.LeaseUntilMs <= m.now().UnixMilli()) {
			return fmt.Errorf("%w: active authority result is not projectable", ErrAuthorityLeaseExpired)
		}
		if authority.HolderID != request.newHolderID || authority.HolderBootID != request.newHolderBoot {
			return fmt.Errorf("%w: authority holder %s/%s does not match requested holder %s/%s", ErrAuthorityAddress, authority.HolderID, authority.HolderBootID, request.newHolderID, request.newHolderBoot)
		}
		if request.kind == transitionTransfer {
			if authority.HolderID == request.expectedFence.ExpectedHolderID && authority.HolderBootID == request.expectedFence.ExpectedHolderBootID {
				return fmt.Errorf("%w: transfer retained old holder generation", ErrAuthorityGeneration)
			}
			if !progressed {
				return fmt.Errorf("%w: transfer assignment/writer/lease epochs did not advance exactly once", ErrAuthorityGeneration)
			}
		} else if requireFreshLease {
			if !claimProgressed {
				key := authorityKey{domain: request.domain, shard: request.virtualShard}
				initialReplay := m.initialBatchOpen && result.Status == authoritystore.ShardTransitionAlreadyApplied && isInitialClaimPlaceholder(route)
				if (!m.bootstrap[key] && !initialReplay) || route.State == actorrouting.ShardStateActive {
					return fmt.Errorf("%w: claim assignment/writer/lease epochs did not advance", ErrAuthorityGeneration)
				}
			}
		} else if !progressed {
			// Durable recovery validates the immutable Claim against its original
			// zero fence. The local snapshot may already contain this exact ACTIVE
			// tuple when publication succeeded but recovery cleanup failed, so it
			// must not be used as the progress baseline on restart.
			return fmt.Errorf("%w: claim assignment/writer/lease epochs did not advance", ErrAuthorityGeneration)
		}
	case transitionUnassign:
		if authority.State != authoritystore.ShardAuthorityUnassigned || authority.HolderID != "" || authority.HolderBootID != "" || authority.LeaseUntilMs != 0 {
			return fmt.Errorf("%w: unassign authority result is not projectable", ErrAuthorityAddress)
		}
		if !progressed {
			return fmt.Errorf("%w: unassign assignment/writer/lease epochs did not advance exactly once", ErrAuthorityGeneration)
		}
	}
	return nil
}

func routeMatchesFence(snapshot actorrouting.RouteSnapshot, domain actorrouting.RouteDomain, shard uint32, fence authoritystore.ShardWriteFence) bool {
	routes := snapshotDomain(snapshot, domain)
	if shard >= routes.ShardCount || int(shard) >= len(routes.Routes) {
		return false
	}
	route := routes.Routes[shard]
	if route.State != actorrouting.ShardStateActive {
		return false
	}
	if route.HolderIndex < 0 || route.HolderIndex >= len(snapshot.Holders) {
		return false
	}
	holder := snapshot.Holders[route.HolderIndex]
	return holder.InstanceID == fence.ExpectedHolderID && holder.BootID == fence.ExpectedHolderBootID && route.ShardRevision == fence.ExpectedAssignmentRevision && route.ExpectedWriterEpoch == fence.ExpectedWriterEpoch && route.ExpectedLeaseEpoch == fence.ExpectedLeaseEpoch
}

func (m *RouteAssignmentManager) pruneMemosLocked(now time.Time) error {
	if m == nil {
		return ErrInvalidManager
	}
	for len(m.transitions) > m.memoLimit {
		candidateID := m.oldestUnpinnedMemoIDLocked(m.transitions)
		if candidateID == "" {
			return nil
		}
		if err := m.ensureRecoveryRoomLocked(now); err != nil {
			return err
		}
		m.moveToRecoveryLocked(candidateID, m.transitions[candidateID])
	}
	for id, memo := range m.transitions {
		if memo == nil || m.memoPinnedLocked(id) || now.Sub(memo.lastUsedAt) < m.retention {
			continue
		}
		if err := m.ensureRecoveryRoomLocked(now); err != nil {
			return err
		}
		m.moveToRecoveryLocked(id, memo)
	}
	for len(m.recoveries) > m.memoLimit {
		candidateID := m.oldestUnpinnedMemoIDLocked(m.recoveries)
		if candidateID == "" {
			return nil
		}
		if err := m.expireMemoLocked(candidateID, m.recoveries[candidateID], now); err != nil {
			return err
		}
	}
	return nil
}

func (m *RouteAssignmentManager) oldestUnpinnedMemoIDLocked(memos map[string]*transitionMemo) string {
	if m == nil {
		return ""
	}
	candidateID := ""
	var candidateAt time.Time
	for id, memo := range memos {
		if memo == nil || m.memoPinnedLocked(id) {
			continue
		}
		if candidateID == "" || memo.lastUsedAt.Before(candidateAt) || (memo.lastUsedAt.Equal(candidateAt) && id < candidateID) {
			candidateID = id
			candidateAt = memo.lastUsedAt
		}
	}
	return candidateID
}

func (m *RouteAssignmentManager) memoPinnedLocked(id string) bool {
	if m == nil || id == "" {
		return false
	}
	if memo := m.transitions[id]; memo != nil && memo.projectionPending {
		return true
	}
	if memo := m.recoveries[id]; memo != nil && memo.projectionPending {
		return true
	}
	if _, reserved := m.reservations[id]; reserved {
		return true
	}
	for _, flight := range m.drainFlights {
		if flight != nil && flight.transitionID == id {
			return true
		}
	}
	return false
}

// ensureRecoveryRoomLocked makes the next transition-to-recovery move safe.
// It persists a recoverable tombstone before freeing a bounded recovery slot;
// if that persistence cannot be completed, the caller fails closed before an
// AuthorityStore commit is attempted.
func (m *RouteAssignmentManager) ensureRecoveryRoomLocked(now time.Time) error {
	if len(m.recoveries) < m.memoLimit {
		return nil
	}
	candidateID := m.oldestUnpinnedMemoIDLocked(m.recoveries)
	if candidateID == "" {
		return ErrTransitionTombstoneCap
	}
	return m.expireMemoLocked(candidateID, m.recoveries[candidateID], now)
}

func oldestMemoID(memos map[string]*transitionMemo) string {
	candidateID := ""
	var candidateAt time.Time
	for id, memo := range memos {
		if memo == nil {
			continue
		}
		if candidateID == "" || memo.lastUsedAt.Before(candidateAt) || (memo.lastUsedAt.Equal(candidateAt) && id < candidateID) {
			candidateID = id
			candidateAt = memo.lastUsedAt
		}
	}
	return candidateID
}

func (m *RouteAssignmentManager) reserveMemoSlotLocked(now time.Time) error {
	if m == nil {
		return ErrInvalidManager
	}
	return m.pruneMemosLocked(now)
}

// reserveTransitionSlotLocked must run before the AuthorityStore side effect.
// It accounts for other long-running Drain reservations and either prepares a
// bounded memo slot or fails closed. A reservation is consumed only after a
// complete, projectable authority result is available.
func (m *RouteAssignmentManager) reserveTransitionSlotLocked(id, fingerprint string, now time.Time) error {
	if m == nil || id == "" || fingerprint == "" {
		return ErrInvalidManager
	}
	if existing := m.reservations[id]; existing != nil {
		if existing.fingerprint != fingerprint {
			return ErrTransitionReconcile
		}
		return nil
	}
	if err := m.pruneMemosLocked(now); err != nil {
		return err
	}
	for len(m.transitions)+m.reservedSlots >= m.memoLimit {
		candidateID := m.oldestUnpinnedMemoIDLocked(m.transitions)
		if candidateID == "" {
			return ErrTransitionTombstoneCap
		}
		if err := m.ensureRecoveryRoomLocked(now); err != nil {
			return err
		}
		m.moveToRecoveryLocked(candidateID, m.transitions[candidateID])
	}
	if m.reservations == nil {
		m.reservations = make(map[string]*transitionReservation)
	}
	m.reservations[id] = &transitionReservation{fingerprint: fingerprint, createdAt: now}
	m.reservedSlots++
	return nil
}

func (m *RouteAssignmentManager) releaseTransitionReservationLocked(id, fingerprint string) {
	if m == nil {
		return
	}
	reservation := m.reservations[id]
	if reservation == nil || (fingerprint != "" && reservation.fingerprint != fingerprint) {
		return
	}
	delete(m.reservations, id)
	if m.reservedSlots > 0 {
		m.reservedSlots--
	}
}

func (m *RouteAssignmentManager) consumeTransitionReservationLocked(id, fingerprint string) {
	m.releaseTransitionReservationLocked(id, fingerprint)
}

func (m *RouteAssignmentManager) expireMemoLocked(id string, memo *transitionMemo, now time.Time) error {
	if m == nil || memo == nil {
		return ErrInvalidManager
	}
	entry := TransitionTombstone{TransitionID: id, Fingerprint: memo.fingerprint, Result: memo.result, ExpiredAt: now}
	if existing, ok := m.tombstone[id]; ok {
		if existing.Fingerprint != entry.Fingerprint {
			return ErrTransitionReconcile
		}
		delete(m.recoveries, id)
		return nil
	}
	if m.tombstones != nil {
		if err := m.tombstones.SaveTransitionTombstone(transitionStoreStartupContext(), entry); err != nil {
			return fmt.Errorf("%w: %v", ErrTransitionTombstoneCap, err)
		}
	} else if len(m.tombstone) >= m.memoLimit {
		// Without a durable lookup seam, forgetting an expired ID would make a
		// delayed retry indistinguishable from a new operation. Stop accepting
		// more transitions rather than reopen that safety hole.
		return ErrTransitionTombstoneCap
	}
	if len(m.tombstone) >= m.memoLimit {
		// The durable store is authoritative and can answer old-ID lookups; the
		// manager keeps only a bounded hot index after persisting the entry.
		oldestID := ""
		var oldestAt time.Time
		for oldID, oldEntry := range m.tombstone {
			if oldestID == "" || oldEntry.ExpiredAt.Before(oldestAt) || (oldEntry.ExpiredAt.Equal(oldestAt) && oldID < oldestID) {
				oldestID, oldestAt = oldID, oldEntry.ExpiredAt
			}
		}
		delete(m.tombstone, oldestID)
	}
	m.tombstone[id] = entry
	delete(m.recoveries, id)
	return nil
}

func (m *RouteAssignmentManager) transitionTombstoneLocked(id string) (TransitionTombstone, bool, error) {
	if entry, ok := m.tombstone[id]; ok {
		return entry, true, nil
	}
	if m.tombstones == nil {
		return TransitionTombstone{}, false, nil
	}
	entry, ok, err := m.tombstones.LookupTransitionTombstone(transitionStoreStartupContext(), id)
	if err != nil {
		return TransitionTombstone{}, false, fmt.Errorf("%w: lookup transition tombstone: %v", ErrTransitionMemoExpired, err)
	}
	if ok {
		if len(m.tombstone) < m.memoLimit {
			m.tombstone[id] = entry
		}
	}
	return entry, ok, nil
}

func (m *RouteAssignmentManager) transitionMemoLocked(id string) *transitionMemo {
	if m == nil {
		return nil
	}
	if memo := m.transitions[id]; memo != nil {
		return memo
	}
	return m.recoveries[id]
}

func (m *RouteAssignmentManager) moveToRecoveryLocked(id string, memo *transitionMemo) {
	if m == nil || memo == nil {
		return
	}
	delete(m.transitions, id)
	if m.recoveries == nil {
		m.recoveries = make(map[string]*transitionMemo)
	}
	m.recoveries[id] = memo
}

func transitionMustBeCommitted(result authoritystore.ShardAuthorityTransitionResult) error {
	switch result.Status {
	case authoritystore.ShardTransitionApplied, authoritystore.ShardTransitionAlreadyApplied:
		return nil
	case authoritystore.ShardTransitionTimeoutUnknown, authoritystore.ShardTransitionUnknown:
		return fmt.Errorf("%w: status=%s", ErrAuthorityCommitUnknown, result.Status)
	case authoritystore.ShardTransitionUnavailableNotSent:
		return fmt.Errorf("%w: status=%s", ErrAuthorityUnavailable, result.Status)
	default:
		return fmt.Errorf("%w: status=%s", ErrAuthorityRejected, result.Status)
	}
}

func transitionOutcomeAmbiguous(result authoritystore.ShardAuthorityTransitionResult) bool {
	return result.Status == authoritystore.ShardTransitionUnknown || result.Status == authoritystore.ShardTransitionTimeoutUnknown
}

func transitionOutcomeDefinitivelyNotSent(result authoritystore.ShardAuthorityTransitionResult) bool {
	return result.Status == authoritystore.ShardTransitionUnavailableNotSent
}

func durationMillis(value time.Duration) (int64, error) {
	if value <= 0 || value < time.Millisecond {
		return 0, fmt.Errorf("%w: positive millisecond TTL required", ErrInvalidManager)
	}
	return value.Milliseconds(), nil
}

func durationMillisForTransition(request transitionRequest) (int64, error) {
	if request.kind == transitionUnassign {
		if request.ttl != 0 {
			return 0, fmt.Errorf("%w: unassign must not carry a lease ttl", ErrInvalidManager)
		}
		return 0, nil
	}
	return durationMillis(request.ttl)
}

func validateTransitionRequest(request transitionRequest) error {
	if request.transitionID == "" || !validManagerDomain(request.domain) {
		return fmt.Errorf("%w: transition identity/domain", ErrInvalidManager)
	}
	if request.kind != transitionUnassign && (request.newHolderID == "" || request.newHolderBoot == "") {
		return fmt.Errorf("%w: new holder generation", ErrInvalidManager)
	}
	if request.kind != transitionUnassign && request.endpoint == "" {
		return fmt.Errorf("%w: endpoint required for published holder", ErrInvalidManager)
	}
	if request.kind != transitionClaim && (request.expectedFence.ExpectedHolderID == "" || request.expectedFence.ExpectedHolderBootID == "" || request.expectedFence.ExpectedAssignmentRevision == 0 || request.expectedFence.ExpectedWriterEpoch == 0 || request.expectedFence.ExpectedLeaseEpoch == 0) {
		return fmt.Errorf("%w: complete expected fence required", ErrInvalidManager)
	}
	if request.deploymentIdentityPresent {
		source := holderidentity.DeploymentIdentity{PodUID: request.sourcePodUID, WorkloadUID: request.sourceWorkloadUID, Ordinal: request.sourceOrdinal}
		target := holderidentity.DeploymentIdentity{PodUID: request.targetPodUID, WorkloadUID: request.targetWorkloadUID, Ordinal: request.targetOrdinal}
		if request.kind != transitionTransfer || target.ValidateExact() != nil {
			return fmt.Errorf("%w: exact target deployment identity required", ErrInvalidManager)
		}
		switch {
		case request.internalDrain:
			if source.ValidateExact() != nil {
				return fmt.Errorf("%w: complete deployment identities required for planned drain", ErrInvalidManager)
			}
		case request.mode == TransferModeCrashTakeover:
			if !source.IsZero() {
				return fmt.Errorf("%w: crash takeover must not invent a source deployment identity", ErrInvalidManager)
			}
		default:
			return fmt.Errorf("%w: deployment identity scope", ErrInvalidManager)
		}
	}
	if !request.deploymentIdentityPresent && (request.sourcePodUID != "" || request.sourceWorkloadUID != "" || request.sourceOrdinal != 0 ||
		request.targetPodUID != "" || request.targetWorkloadUID != "" || request.targetOrdinal != 0) {
		return fmt.Errorf("%w: partial deployment identities", ErrInvalidManager)
	}
	if request.kind == transitionTransfer {
		if request.internalDrain {
			if request.mode != TransferModeDrainHandoff &&
				(request.mode != TransferModeWorkerHandoff ||
					request.domain != actorrouting.DomainFarmWorker ||
					request.proof != (TransferTakeoverProof{})) {
				return fmt.Errorf("%w: internal drain mode", ErrInvalidManager)
			}
		} else {
			if request.mode != TransferModeCrashTakeover ||
				request.proof.ExpectedHolderBootID != request.expectedFence.ExpectedHolderBootID ||
				(!request.proof.HolderUnavailable && !request.proof.LeaseExpired) ||
				request.proof.Evidence == "" {
				return ErrTransferTakeoverNeeded
			}
		}
	}
	return nil
}

func transitionFingerprintV1(request transitionRequest) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte("farmgame.route-assignment-transition-fingerprint.v1"))
	write := func(label, value string) {
		var size [4]byte
		binary.BigEndian.PutUint32(size[:], uint32(len(label)))
		_, _ = hash.Write(size[:])
		_, _ = hash.Write([]byte(label))
		binary.BigEndian.PutUint32(size[:], uint32(len(value)))
		_, _ = hash.Write(size[:])
		_, _ = hash.Write([]byte(value))
	}
	f := request.expectedFence
	write("kind", strconv.FormatUint(uint64(request.kind), 10))
	write("mode", strconv.FormatUint(uint64(request.mode), 10))
	write("internal_drain", strconv.FormatBool(request.internalDrain))
	write("transition_id", request.transitionID)
	write("virtual_shard", strconv.FormatUint(uint64(request.virtualShard), 10))
	write("domain", string(request.domain))
	write("new_holder_id", request.newHolderID)
	write("new_holder_boot_id", request.newHolderBoot)
	write("endpoint", request.endpoint)
	write("requested_ttl_ms", strconv.FormatInt(request.ttl.Milliseconds(), 10))
	write("expected_holder_id", f.ExpectedHolderID)
	write("expected_holder_boot_id", f.ExpectedHolderBootID)
	write("expected_assignment_revision", strconv.FormatUint(f.ExpectedAssignmentRevision, 10))
	write("expected_writer_epoch", strconv.FormatUint(f.ExpectedWriterEpoch, 10))
	write("expected_lease_epoch", strconv.FormatUint(f.ExpectedLeaseEpoch, 10))
	write("proof_expected_holder_boot_id", request.proof.ExpectedHolderBootID)
	write("proof_holder_unavailable", strconv.FormatBool(request.proof.HolderUnavailable))
	write("proof_lease_expired", strconv.FormatBool(request.proof.LeaseExpired))
	write("proof_evidence", request.proof.Evidence)
	return "route-transition-fingerprint-v1:" + hex.EncodeToString(hash.Sum(nil))
}

func transitionFingerprint(request transitionRequest) string {
	if !request.deploymentIdentityPresent {
		return transitionFingerprintV1(request)
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte("farmgame.route-assignment-transition-fingerprint.v2"))
	write := func(label, value string) {
		var size [4]byte
		binary.BigEndian.PutUint32(size[:], uint32(len(label)))
		_, _ = hash.Write(size[:])
		_, _ = hash.Write([]byte(label))
		binary.BigEndian.PutUint32(size[:], uint32(len(value)))
		_, _ = hash.Write(size[:])
		_, _ = hash.Write([]byte(value))
	}
	write("legacy_fingerprint", transitionFingerprintV1(request))
	write("source_pod_uid", request.sourcePodUID)
	write("source_workload_uid", request.sourceWorkloadUID)
	write("source_ordinal", strconv.FormatInt(int64(request.sourceOrdinal), 10))
	write("target_pod_uid", request.targetPodUID)
	write("target_workload_uid", request.targetWorkloadUID)
	write("target_ordinal", strconv.FormatInt(int64(request.targetOrdinal), 10))
	return "route-transition-fingerprint-v2:" + hex.EncodeToString(hash.Sum(nil))
}

func transitionIntentDigest(layout string, request transitionRequest, requestedTTLms int64) string {
	kind := ""
	switch request.kind {
	case transitionClaim:
		kind = "claim"
	case transitionTransfer:
		kind = "transfer"
	case transitionUnassign:
		kind = "unassign"
		requestedTTLms = 0
	}
	return authoritystore.ShardAuthorityTransitionIntentDigest(
		kind, layout, string(request.domain), request.virtualShard,
		request.expectedFence, request.newHolderID, request.newHolderBoot, requestedTTLms,
	)
}

func recoveryForRequest(request transitionRequest, fingerprint string, result authoritystore.ShardAuthorityTransitionResult, now time.Time) TransitionRecovery {
	return TransitionRecovery{
		TransitionID: request.transitionID, Fingerprint: fingerprint, TransitionKind: uint8(request.kind),
		Domain: request.domain, VirtualShard: request.virtualShard, ExpectedFence: request.expectedFence,
		NewHolderID: request.newHolderID, NewHolderBootID: request.newHolderBoot,
		SourcePodUID: request.sourcePodUID, SourceWorkloadUID: request.sourceWorkloadUID, SourceOrdinal: request.sourceOrdinal,
		TargetPodUID: request.targetPodUID, TargetWorkloadUID: request.targetWorkloadUID, TargetOrdinal: request.targetOrdinal,
		DeploymentIdentityPresent: request.deploymentIdentityPresent, Endpoint: request.endpoint,
		RequestedTTLms: request.ttl.Milliseconds(), Mode: request.mode, TakeoverProof: request.proof,
		InternalDrain: request.internalDrain, Result: result, LastState: routeStateFor(request.kind),
		Published: false, ColdLoaded: false, AuthorityAttemptedPresent: true,
		RecoveryWireVersion: TransitionRecoveryWireAuthorityAttempted,
		Committed:           result.Status == authoritystore.ShardTransitionApplied || result.Status == authoritystore.ShardTransitionAlreadyApplied,
		CreatedAt:           now, UpdatedAt: now,
	}
}

// transitionRecoveryEvidenceEqual compares the complete durable transition
// evidence while excluding UpdatedAt, which is retention metadata rather than
// transition identity. A successful persistence can be followed by a local
// last-used touch before a read-only observation; requiring nanosecond equality
// would reject otherwise exact recovery evidence. CreatedAt remains exact by
// instant and every authority/intent/checkpoint field remains comparable-exact.
func transitionRecoveryEvidenceEqual(left, right TransitionRecovery) bool {
	leftCreated, rightCreated := left.CreatedAt, right.CreatedAt
	left.CreatedAt, right.CreatedAt = time.Time{}, time.Time{}
	left.UpdatedAt, right.UpdatedAt = time.Time{}, time.Time{}
	return left == right && leftCreated.IsZero() == rightCreated.IsZero() && leftCreated.Equal(rightCreated)
}

func recoveryForMemo(memo *transitionMemo) TransitionRecovery {
	if memo == nil {
		return TransitionRecovery{}
	}
	recovery := recoveryForRequest(memo.request, memo.fingerprint, memo.result, memo.createdAt)
	recovery.LastState = memo.lastState
	recovery.PublishedSnapshotRevision = memo.publishedSnapshotRevision
	recovery.BootstrapProjection = memo.bootstrapProjection
	recovery.Published = memo.published
	recovery.ColdLoaded = memo.coldLoaded
	recovery.AuthorityAttempted = memo.authorityAttempted
	recovery.AuthorityAttemptedPresent = true
	recovery.RecoveryWireVersion = TransitionRecoveryWireAuthorityAttempted
	recovery.Committed = !memo.commitAmbiguous &&
		(memo.result.Status == authoritystore.ShardTransitionApplied || memo.result.Status == authoritystore.ShardTransitionAlreadyApplied)
	recovery.UpdatedAt = memo.lastUsedAt
	return recovery
}

func requestFromRecovery(recovery TransitionRecovery) (transitionRequest, error) {
	kind := transitionKind(recovery.TransitionKind)
	if kind != transitionClaim && kind != transitionTransfer && kind != transitionUnassign {
		return transitionRequest{}, ErrTransitionReconcile
	}
	request := transitionRequest{
		kind: kind, transitionID: recovery.TransitionID, domain: recovery.Domain,
		virtualShard: recovery.VirtualShard, expectedFence: recovery.ExpectedFence,
		newHolderID: recovery.NewHolderID, newHolderBoot: recovery.NewHolderBootID,
		sourcePodUID: recovery.SourcePodUID, sourceWorkloadUID: recovery.SourceWorkloadUID, sourceOrdinal: recovery.SourceOrdinal,
		targetPodUID: recovery.TargetPodUID, targetWorkloadUID: recovery.TargetWorkloadUID, targetOrdinal: recovery.TargetOrdinal,
		deploymentIdentityPresent: recovery.DeploymentIdentityPresent,
		endpoint:                  recovery.Endpoint, ttl: time.Duration(recovery.RequestedTTLms) * time.Millisecond,
		mode: recovery.Mode, proof: recovery.TakeoverProof, internalDrain: recovery.InternalDrain,
	}
	if err := validateTransitionRequest(request); err != nil {
		return transitionRequest{}, err
	}
	if transitionFingerprint(request) != recovery.Fingerprint {
		return transitionRequest{}, ErrTransitionReconcile
	}
	return request, nil
}

func (m *RouteAssignmentManager) recoveryStoreLocked() TransitionRecoveryStore {
	if m == nil || m.tombstones == nil {
		return nil
	}
	store, _ := m.tombstones.(TransitionRecoveryStore)
	return store
}

func (m *RouteAssignmentManager) reserveRecoveryLocked(request transitionRequest, fingerprint string, now time.Time) error {
	store := m.recoveryStoreLocked()
	if store == nil {
		return nil
	}
	recovery := recoveryForRequest(request, fingerprint, authoritystore.ShardAuthorityTransitionResult{}, now)
	recovery.BootstrapProjection = m.bootstrap[authorityKey{domain: request.domain, shard: request.virtualShard}]
	return store.ReserveTransitionRecovery(transitionStoreStartupContext(), recovery)
}

func (m *RouteAssignmentManager) commitRecoveryLocked(memo *transitionMemo) error {
	return m.persistCommitRecovery(memo, false)
}

func (m *RouteAssignmentManager) commitRecoveryReleasingLock(memo *transitionMemo) error {
	return m.persistCommitRecovery(memo, true)
}

func (m *RouteAssignmentManager) persistCommitRecovery(memo *transitionMemo, releaseLock bool) error {
	if m == nil || memo == nil {
		return ErrInvalidManager
	}
	store := m.recoveryStoreLocked()
	if store == nil {
		return nil
	}
	recovery := recoveryForMemo(memo)
	recovery.AuthorityAttempted = true
	recovery.Committed = true
	write := func() error {
		return store.CommitTransitionRecovery(transitionStoreStartupContext(), recovery)
	}
	if releaseLock {
		return m.runWithoutStateLock(write)
	}
	return write()
}

// markAuthorityAttemptedLocked is the durable boundary immediately before the
// only AuthorityStore mutation. TransitionRecoveryStore's commit operation is
// also its existing atomic record-update primitive, so the physical Committed
// flag is set for that write; recoveryAuthorityCommitted additionally requires
// an APPLIED result and therefore never mistakes this marker for a committed
// Authority transition.
func (m *RouteAssignmentManager) markAuthorityAttemptedLocked(request transitionRequest, fingerprint string, state actorrouting.ShardRouteState) error {
	return m.persistAuthorityAttempted(request, fingerprint, state, false)
}

// markAuthorityAttemptedReleasingLock is the Drain/Claim hot path. The caller
// must hold m.mu; Redis I/O runs without it so sibling shards can CAS together.
func (m *RouteAssignmentManager) markAuthorityAttemptedReleasingLock(request transitionRequest, fingerprint string, state actorrouting.ShardRouteState) error {
	return m.persistAuthorityAttempted(request, fingerprint, state, true)
}

func (m *RouteAssignmentManager) persistAuthorityAttempted(request transitionRequest, fingerprint string, state actorrouting.ShardRouteState, releaseLock bool) error {
	store := m.recoveryStoreLocked()
	if store == nil {
		if memo := m.transitionMemoLocked(request.transitionID); memo != nil {
			memo.authorityAttempted = true
		}
		return nil
	}
	recovery := recoveryForRequest(request, fingerprint, authoritystore.ShardAuthorityTransitionResult{}, m.now())
	recovery.LastState = state
	recovery.BootstrapProjection = m.bootstrap[authorityKey{domain: request.domain, shard: request.virtualShard}]
	recovery.AuthorityAttempted = true
	recovery.Committed = true // physical update stage; no APPLIED result yet
	var persistErr error
	write := func() error {
		persistErr = store.CommitTransitionRecovery(transitionStoreStartupContext(), recovery)
		return persistErr
	}
	if releaseLock {
		_ = m.runWithoutStateLock(write)
	} else if err := write(); err != nil {
		return err
	}
	if persistErr != nil {
		return persistErr
	}
	if memo := m.transitionMemoLocked(request.transitionID); memo != nil {
		memo.authorityAttempted = true
		memo.lastUsedAt = recovery.UpdatedAt
	}
	return nil
}

func recoveryAuthorityCommitted(recovery TransitionRecovery) bool {
	return recovery.Committed &&
		(recovery.Result.Status == authoritystore.ShardTransitionApplied || recovery.Result.Status == authoritystore.ShardTransitionAlreadyApplied)
}

func (m *RouteAssignmentManager) deleteRecoveryLocked(id string) error {
	store := m.recoveryStoreLocked()
	if store == nil {
		return nil
	}
	return store.DeleteTransitionRecovery(transitionStoreStartupContext(), id)
}

func (m *RouteAssignmentManager) updateRecoveryLocked(memo *transitionMemo) error {
	if memo == nil {
		return ErrInvalidManager
	}
	// A committed and completely published recovery is durable verification
	// evidence for as long as its exact memo remains hot. Eviction replaces it
	// atomically with a tombstone through SaveTransitionTombstone.
	return m.commitRecoveryReleasingLock(memo)
}

func routeStateFor(kind transitionKind) actorrouting.ShardRouteState {
	if kind == transitionUnassign {
		return actorrouting.ShardStateAssigning
	}
	return actorrouting.ShardStateActive
}

func routeMatchesAuthority(snapshot actorrouting.RouteSnapshot, authority authoritystore.ShardWriteAuthority, state actorrouting.ShardRouteState, endpoint string) bool {
	if !routeMatchesAuthorityGeneration(snapshot, authority, state, endpoint) {
		return false
	}
	route := snapshotDomain(snapshot, authority.Domain).Routes[authority.VirtualShard]
	return route.LeaseUntilMs == authority.LeaseUntilMs
}

// routeMatchesAuthorityGeneration compares the per-shard ownership generation
// and deliberately excludes only LeaseUntilMs. A holder-authenticated renewal
// advances that liveness projection without creating a new generation.
func routeMatchesAuthorityGeneration(snapshot actorrouting.RouteSnapshot, authority authoritystore.ShardWriteAuthority, state actorrouting.ShardRouteState, endpoint string) bool {
	routes := snapshotDomain(snapshot, authority.Domain)
	if authority.VirtualShard >= routes.ShardCount || int(authority.VirtualShard) >= len(routes.Routes) {
		return false
	}
	route := routes.Routes[authority.VirtualShard]
	if route.State != state || route.ShardRevision != authority.AssignmentRevision || route.ExpectedWriterEpoch != authority.WriterEpoch || route.ExpectedLeaseEpoch != authority.LeaseEpoch {
		return false
	}
	if state != actorrouting.ShardStateActive {
		return true
	}
	if route.HolderIndex < 0 || route.HolderIndex >= len(snapshot.Holders) {
		return false
	}
	holder := snapshot.Holders[route.HolderIndex]
	return holder.InstanceID == authority.HolderID && holder.BootID == authority.HolderBootID &&
		(endpoint == "" || holder.Endpoint == endpoint)
}

func (m *RouteAssignmentManager) snapshotForAuthority(current actorrouting.RouteSnapshot, authority authoritystore.ShardWriteAuthority, state actorrouting.ShardRouteState, endpoint string) (actorrouting.RouteSnapshot, error) {
	preserveFarmWorker := m != nil && m.initialBatchOpen
	next, err := snapshotForAuthorityWithOptions(current, authority, state, endpoint, preserveFarmWorker)
	if err != nil || state != actorrouting.ShardStateActive {
		return next, err
	}
	publication, found, err := m.resolveHolderPublicKey(authority.Domain, authority.HolderID, authority.HolderBootID)
	if err != nil {
		return actorrouting.RouteSnapshot{}, err
	}
	if !found {
		return next, nil
	}
	routes := snapshotDomain(next, authority.Domain)
	if authority.VirtualShard >= routes.ShardCount || int(authority.VirtualShard) >= len(routes.Routes) {
		return actorrouting.RouteSnapshot{}, ErrInvalidManager
	}
	index := routes.Routes[authority.VirtualShard].HolderIndex
	if index < 0 || index >= len(next.Holders) {
		return actorrouting.RouteSnapshot{}, ErrInvalidManager
	}
	if err := next.Holders[index].SetCommitProofPublication(publication); err != nil {
		return actorrouting.RouteSnapshot{}, fmt.Errorf("%w: holder public key", ErrInvalidManager)
	}
	return next, nil
}

func snapshotForAuthority(current actorrouting.RouteSnapshot, authority authoritystore.ShardWriteAuthority, state actorrouting.ShardRouteState, endpoint string) (actorrouting.RouteSnapshot, error) {
	return snapshotForAuthorityWithOptions(current, authority, state, endpoint, false)
}

func snapshotForAuthorityWithOptions(current actorrouting.RouteSnapshot, authority authoritystore.ShardWriteAuthority, state actorrouting.ShardRouteState, endpoint string, preserveFarmWorker bool) (actorrouting.RouteSnapshot, error) {
	next := actorrouting.CloneRouteSnapshot(current)
	if !validManagerDomain(authority.Domain) {
		return actorrouting.RouteSnapshot{}, fmt.Errorf("%w: authority domain", ErrInvalidManager)
	}
	routes := snapshotDomain(next, authority.Domain)
	if authority.VirtualShard >= routes.ShardCount || int(authority.VirtualShard) >= len(routes.Routes) {
		return actorrouting.RouteSnapshot{}, fmt.Errorf("%w: authority shard out of range", ErrInvalidManager)
	}
	route := routes.Routes[authority.VirtualShard]
	if state == actorrouting.ShardStateActive {
		index, err := ensureHolderWithOptions(&next, authority.Domain, authority.HolderID, authority.HolderBootID, endpoint, preserveFarmWorker)
		if err != nil {
			return actorrouting.RouteSnapshot{}, err
		}
		route.HolderIndex = index
	} else if route.HolderIndex < 0 || route.HolderIndex >= len(next.Holders) {
		return actorrouting.RouteSnapshot{}, fmt.Errorf("%w: inactive route lost holder index", ErrInvalidManager)
	}
	route.State = state
	route.ShardRevision = authority.AssignmentRevision
	route.ExpectedWriterEpoch = authority.WriterEpoch
	route.ExpectedLeaseEpoch = authority.LeaseEpoch
	route.LeaseUntilMs = authority.LeaseUntilMs
	routes.Routes[authority.VirtualShard] = route
	setSnapshotDomain(&next, authority.Domain, routes)
	if !preserveFarmWorker {
		compactUnreferencedHolders(&next)
	}
	if next.GlobalRevision == ^uint64(0) {
		return actorrouting.RouteSnapshot{}, fmt.Errorf("%w: global revision overflow", ErrInvalidManager)
	}
	next.GlobalRevision++
	return next, nil
}

// compactUnreferencedHolders keeps HolderRef growth bounded across repeated
// boot generations. References are remapped in original slice order, so the
// result is deterministic and all route domains retain their independent
// namespace entries.
func compactUnreferencedHolders(snapshot *actorrouting.RouteSnapshot) {
	if snapshot == nil || len(snapshot.Holders) == 0 {
		return
	}
	referenced := make([]bool, len(snapshot.Holders))
	for _, routes := range []actorrouting.DomainSnapshot{
		snapshot.Player,
		snapshot.Farm,
		snapshot.FarmWorker,
	} {
		for _, route := range routes.Routes {
			if route.HolderIndex >= 0 && route.HolderIndex < len(referenced) {
				referenced[route.HolderIndex] = true
			}
		}
	}
	remap := make([]int, len(snapshot.Holders))
	newHolders := make([]actorrouting.HolderRef, 0, len(snapshot.Holders))
	for oldIndex, holder := range snapshot.Holders {
		if !referenced[oldIndex] {
			remap[oldIndex] = -1
			continue
		}
		remap[oldIndex] = len(newHolders)
		newHolders = append(newHolders, holder)
	}
	for _, routes := range []*actorrouting.DomainSnapshot{
		&snapshot.Player,
		&snapshot.Farm,
		&snapshot.FarmWorker,
	} {
		for index := range routes.Routes {
			oldIndex := routes.Routes[index].HolderIndex
			if oldIndex >= 0 && oldIndex < len(remap) && remap[oldIndex] >= 0 {
				routes.Routes[index].HolderIndex = remap[oldIndex]
			}
		}
	}
	snapshot.Holders = newHolders
}

func ensureHolder(snapshot *actorrouting.RouteSnapshot, domain actorrouting.RouteDomain, holderID, bootID, endpoint string) (int, error) {
	return ensureHolderWithOptions(snapshot, domain, holderID, bootID, endpoint, false)
}

func ensureHolderWithOptions(snapshot *actorrouting.RouteSnapshot, domain actorrouting.RouteDomain, holderID, bootID, endpoint string, preserveFarmWorker bool) (int, error) {
	// HolderRef is physically stored in one slice for the frozen snapshot model,
	// but the namespace is still domain-scoped. Only indexes already referenced
	// by this domain may be replaced; an equal instance ID in the other domain
	// always gets a distinct HolderRef.
	indices := holderIndicesForDomain(*snapshot, domain)
	for i := range indices {
		index := indices[i]
		holder := snapshot.Holders[index]
		if holder.InstanceID == holderID && holder.BootID == bootID {
			if preserveFarmWorker && holderIndexUseCount(*snapshot, actorrouting.DomainFarmWorker, index) > 0 &&
				((endpoint != "" && holder.Endpoint != endpoint) || holder.State != actorrouting.HolderStateActive) {
				continue
			}
			if endpoint != "" {
				snapshot.Holders[index].Endpoint = endpoint
			}
			snapshot.Holders[index].State = actorrouting.HolderStateActive
			return index, nil
		}
	}
	for i := range indices {
		index := indices[i]
		if snapshot.Holders[index].InstanceID == holderID {
			if preserveFarmWorker && holderIndexUseCount(*snapshot, actorrouting.DomainFarmWorker, index) > 0 {
				continue
			}
			if holderIndexUseCount(*snapshot, domain, index) > 1 {
				// A boot replacement for one shard must not mutate a HolderRef
				// still referenced by another shard in the same domain.
				continue
			}
			if endpoint == "" {
				return 0, fmt.Errorf("%w: new holder endpoint required", ErrInvalidManager)
			}
			snapshot.Holders[index].BootID = bootID
			snapshot.Holders[index].Endpoint = endpoint
			snapshot.Holders[index].State = actorrouting.HolderStateActive
			// A boot replacement can never inherit the prior process private key.
			// The Manager's verified resolver may install the new public half on
			// this same complete publication below.
			snapshot.Holders[index].ClearCommitProofPublication()
			return index, nil
		}
	}
	if endpoint == "" {
		return 0, fmt.Errorf("%w: new holder endpoint required", ErrInvalidManager)
	}
	snapshot.Holders = append(snapshot.Holders, actorrouting.HolderRef{InstanceID: holderID, BootID: bootID, Endpoint: endpoint, State: actorrouting.HolderStateActive, Weight: 1})
	return len(snapshot.Holders) - 1, nil
}

func holderIndicesForDomain(snapshot actorrouting.RouteSnapshot, domain actorrouting.RouteDomain) []int {
	routes := snapshotDomain(snapshot, domain)
	seen := make(map[int]struct{}, len(routes.Routes))
	indices := make([]int, 0, len(routes.Routes))
	for _, route := range routes.Routes {
		if _, ok := seen[route.HolderIndex]; ok {
			continue
		}
		seen[route.HolderIndex] = struct{}{}
		indices = append(indices, route.HolderIndex)
	}
	return indices
}

func holderIndexUseCount(snapshot actorrouting.RouteSnapshot, domain actorrouting.RouteDomain, holderIndex int) int {
	routes := snapshotDomain(snapshot, domain)
	count := 0
	for _, route := range routes.Routes {
		if route.HolderIndex == holderIndex {
			count++
		}
	}
	return count
}

func holderDomainForPublicKey(snapshot actorrouting.RouteSnapshot, holderIndex int) (actorrouting.RouteDomain, bool) {
	var selected actorrouting.RouteDomain
	for _, domain := range []actorrouting.RouteDomain{actorrouting.DomainPlayer, actorrouting.DomainFarm} {
		if holderIndexUseCount(snapshot, domain, holderIndex) == 0 {
			continue
		}
		if selected != "" && selected != domain {
			return "", false
		}
		selected = domain
	}
	return selected, selected != ""
}

func snapshotDomain(snapshot actorrouting.RouteSnapshot, domain actorrouting.RouteDomain) actorrouting.DomainSnapshot {
	switch domain {
	case actorrouting.DomainPlayer:
		return snapshot.Player
	case actorrouting.DomainFarm:
		return snapshot.Farm
	case actorrouting.DomainFarmWorker:
		return snapshot.FarmWorker
	default:
		return actorrouting.DomainSnapshot{}
	}
}

func setSnapshotDomain(snapshot *actorrouting.RouteSnapshot, domain actorrouting.RouteDomain, value actorrouting.DomainSnapshot) {
	switch domain {
	case actorrouting.DomainPlayer:
		snapshot.Player = value
	case actorrouting.DomainFarm:
		snapshot.Farm = value
	case actorrouting.DomainFarmWorker:
		snapshot.FarmWorker = value
	}
}

func isInitialClaimPlaceholder(route actorrouting.ShardRoute) bool {
	return route.State == actorrouting.ShardStateAssigning && route.ShardRevision == 1 && route.ExpectedWriterEpoch == 1 && route.ExpectedLeaseEpoch == 1 && route.LeaseUntilMs == 0
}

func (m *RouteAssignmentManager) initialBatchClaimAllowedLocked(request transitionRequest) bool {
	if request.kind != transitionClaim ||
		(request.domain != actorrouting.DomainPlayer && request.domain != actorrouting.DomainFarm) {
		return false
	}
	routes := snapshotDomain(m.snapshot, request.domain)
	return request.virtualShard < routes.ShardCount && int(request.virtualShard) < len(routes.Routes) &&
		isInitialClaimPlaceholder(routes.Routes[request.virtualShard])
}

func validManagerDomain(domain actorrouting.RouteDomain) bool {
	return domain == actorrouting.DomainPlayer ||
		domain == actorrouting.DomainFarm ||
		domain == actorrouting.DomainFarmWorker
}

func managedSnapshotDomains(snapshot actorrouting.RouteSnapshot) []actorrouting.RouteDomain {
	domains := []actorrouting.RouteDomain{
		actorrouting.DomainPlayer,
		actorrouting.DomainFarm,
	}
	if snapshot.FarmWorker.ShardCount != 0 {
		domains = append(domains, actorrouting.DomainFarmWorker)
	}
	return domains
}

// DrainStage is the local control state. It is deliberately not a second
// persisted authority state; the store remains UNASSIGNED/ACTIVE only.
type DrainStage uint8

const (
	DrainStageActive DrainStage = iota
	DrainStageDraining
	DrainStageDrained
	DrainStageAssigning
	DrainStageActiveNew
)

// DrainController connects the manager to a holder runtime. The callbacks are
// intentionally coarse: no socket migration, mailbox migration, or hot actor
// memory transfer is implied.
type DrainController interface {
	StopNewActivationAndTurns(context.Context, actorrouting.RouteDomain, uint32) error
	WaitCurrentTurns(context.Context, actorrouting.RouteDomain, uint32) error
	ConfirmFactsCommitted(context.Context, actorrouting.RouteDomain, uint32) error
	ColdLoadNewHolder(context.Context, actorrouting.RouteDomain, uint32, authoritystore.ShardWriteAuthority) error
}

type drainKey struct {
	domain actorrouting.RouteDomain
	shard  uint32
}

// Drain runs the minimum crash-safe state machine. The transfer authority is
// committed before either ASSIGNING or ACTIVE_NEW snapshot is published; the
// intermediate ASSIGNING snapshot keeps the new holder cold-load gate closed.
// A key has one in-flight Drain. The placeholder memo and slot reservation are
// installed before the first holder callback, and remain pinned through cold
// load even when MemoRetention has elapsed.
func (m *RouteAssignmentManager) Drain(ctx context.Context, req TransferRequest, controller DrainController) (result authoritystore.ShardAuthorityTransitionResult, err error) {
	releaseOperation, err := m.acquireOperation(ctx)
	if err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, err
	}
	defer releaseOperation()
	if m.initialBatchIsOpen() {
		return authoritystore.ShardAuthorityTransitionResult{}, routecontrol.ErrInitialAuthorityBatchOpen
	}
	return m.drain(ctx, req, controller, TransitionRecovery{}, nil)
}

// DrainValidated is the PlanExecutor production seam. The validator is
// threaded through the one Manager-owned Drain flight; it is checked before
// every target-relevant holder callback, immediately before Authority commit,
// and immediately before ASSIGNING/ACTIVE publication.
func (m *RouteAssignmentManager) DrainValidated(ctx context.Context, req TransferRequest, controller DrainController, targetValidator PendingProjectionTargetValidator) (result authoritystore.ShardAuthorityTransitionResult, err error) {
	if targetValidator == nil {
		return authoritystore.ShardAuthorityTransitionResult{}, fmt.Errorf("%w: target validator required", ErrInvalidManager)
	}
	releaseOperation, err := m.acquireOperation(ctx)
	if err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, err
	}
	defer releaseOperation()
	if m.initialBatchIsOpen() {
		return authoritystore.ShardAuthorityTransitionResult{}, routecontrol.ErrInitialAuthorityBatchOpen
	}
	return m.drain(ctx, req, controller, TransitionRecovery{}, targetValidator)
}

func (m *RouteAssignmentManager) drain(ctx context.Context, req TransferRequest, controller DrainController, recovery TransitionRecovery, targetValidator PendingProjectionTargetValidator) (result authoritystore.ShardAuthorityTransitionResult, err error) {
	if controller == nil {
		return authoritystore.ShardAuthorityTransitionResult{}, ErrDrainControllerRequired
	}
	if ctx == nil {
		return authoritystore.ShardAuthorityTransitionResult{}, ErrInvalidManager
	}
	drainRequest, err := normalizedDrainRequest(req)
	if err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, err
	}
	if targetValidator != nil && recovery.TransitionID == "" {
		recovery = recoveryForRequest(drainRequest, transitionFingerprint(drainRequest), authoritystore.ShardAuthorityTransitionResult{}, m.now())
	}
	key := drainKey{domain: req.Domain, shard: req.VirtualShard}
	flight, owner, immediate, beginErr := m.beginDrainFlight(ctx, drainRequest)
	if beginErr != nil {
		return immediate, beginErr
	}
	if !owner {
		if flight == nil {
			// A terminal ACTIVE memo is already the complete result. Replays do
			// not enter the holder callbacks or publish ASSIGNING again.
			return immediate, nil
		}
		return m.waitDrainFlight(ctx, flight)
	}
	defer func() { err = m.finishDrainFlight(flight, result, err) }()

	checkpoint, checkpointResult, checkpointErr := m.drainCheckpoint(drainRequest)
	if checkpointErr != nil {
		return checkpointResult, checkpointErr
	}
	if checkpoint >= 4 {
		return checkpointResult, nil
	}
	stage := m.drainStageFor(key, req.TransitionID)
	if checkpoint < 3 {
		if checkpoint < 2 {
			if checkpoint == 0 {
				if stage < DrainStageDraining {
					if err = m.validatePendingProjectionTarget(ctx, drainRequest, recovery, targetValidator); err != nil {
						return result, err
					}
					if err = controller.StopNewActivationAndTurns(ctx, req.Domain, req.VirtualShard); err != nil {
						return result, err
					}
					m.setDrainStage(key, DrainStageDraining)
				}
				if stage < DrainStageDrained {
					if err = m.validatePendingProjectionTarget(ctx, drainRequest, recovery, targetValidator); err != nil {
						return result, err
					}
					if err = controller.WaitCurrentTurns(ctx, req.Domain, req.VirtualShard); err != nil {
						return result, err
					}
					m.setDrainStage(key, DrainStageDrained)
				}
				if stage < DrainStageAssigning {
					if err = m.validatePendingProjectionTarget(ctx, drainRequest, recovery, targetValidator); err != nil {
						return result, err
					}
					if err = controller.ConfirmFactsCommitted(ctx, req.Domain, req.VirtualShard); err != nil {
						return result, err
					}
					m.setDrainStage(key, DrainStageAssigning)
				}
			}
			if err = m.validatePendingProjectionTarget(ctx, drainRequest, recovery, targetValidator); err != nil {
				return result, err
			}
			result, err = m.runTransitionState(ctx, drainRequest, actorrouting.ShardStateAssigning, recovery, targetValidator)
			if err != nil {
				return result, err
			}
		} else {
			result = checkpointResult
			// An assigning publication failure is a safe retry point. Reconcile
			// the same authority before using the memo again. A cold-loaded
			// checkpoint deliberately skips this branch and can only advance.
			result, err = m.runTransitionState(ctx, drainRequest, actorrouting.ShardStateAssigning, recovery, targetValidator)
			if err != nil {
				return result, err
			}
		}
		// A bounded migration wave must durably publish every ASSIGNING route
		// before any target holder is asked to cold-load. Returning this expected
		// intermediate result lets the wave coordinator commit one shared
		// Snapshot CAS, after which the same transition is replayed idempotently
		// to perform target admission and ACTIVE publication.
		if stage, open := m.authorityWaveBatchMemberStage(req.Domain, req.VirtualShard); open && stage == routecontrol.AuthorityWaveBatchStageAssigning {
			return result, ErrAuthorityWaveStagePending
		}
		checkpoint, checkpointResult, checkpointErr = m.drainCheckpoint(drainRequest)
		if checkpointErr != nil {
			return checkpointResult, checkpointErr
		}
		// Cold-load must receive the current exact Authority tuple. A renewal
		// may extend the same generation after the durable transition result was
		// written, so never pass the stale durable deadline to the target.
		currentAuthority, reconcileErr := m.reconcilePendingAuthority(ctx, req.TransitionID, transitionFingerprint(drainRequest))
		if reconcileErr != nil {
			return result, reconcileErr
		}
		if err = validateAuthorityForPublish(m.now(), checkpointResult, currentAuthority, actorrouting.ShardStateAssigning); err != nil {
			return result, err
		}
		// ASSIGNING publication advanced GlobalRevision to N. Remote holders can
		// only report SnapshotRevision==N after Watch/apply + authenticated
		// heartbeat. Wait without holding Manager locks; keep exact validator.
		if err = m.waitPendingProjectionTarget(ctx, drainRequest, recovery, targetValidator); err != nil {
			return result, err
		}
		result = checkpointResult
		result.Authority = currentAuthority
		if err = m.coldLoadWithTargetAdmission(ctx, drainRequest, recovery, targetValidator, controller, currentAuthority); err != nil {
			return result, err
		}
		// Holder Prepare proves the exact fence through a holder-authorized Renew,
		// which may monotonically extend LeaseUntilMs. Re-read durable Authority
		// after the callback so the cold-loaded checkpoint and later ACTIVE
		// publication bind the exact tuple Prepare accepted, never the pre-call
		// deadline. This is a read/reconcile only; no Authority mutation or holder
		// callback is repeated.
		preparedAuthority, reconcileErr := m.reconcilePendingAuthority(ctx, req.TransitionID, transitionFingerprint(drainRequest))
		if reconcileErr != nil {
			return result, reconcileErr
		}
		if err = validateAuthorityForPublish(m.now(), checkpointResult, preparedAuthority, actorrouting.ShardStateAssigning); err != nil {
			return result, err
		}
		result.Authority = preparedAuthority
		if err = m.markDrainColdLoaded(req.TransitionID, preparedAuthority); err != nil {
			return result, err
		}
	} else {
		result = checkpointResult
	}
	if err = m.publishTransitionState(ctx, req.TransitionID, actorrouting.ShardStateActive, recovery, targetValidator); err != nil {
		return result, err
	}
	m.setDrainStage(key, DrainStageActiveNew)
	return result, nil
}

func (m *RouteAssignmentManager) coldLoadWithTargetAdmission(ctx context.Context, request transitionRequest, recovery TransitionRecovery, validator PendingProjectionTargetValidator, controller DrainController, current authoritystore.ShardWriteAuthority) error {
	// Rehydrate the committed per-shard projection checkpoint from the memo.
	// Passing the caller's pre-commit recovery directly would lose the frozen
	// ASSIGNING revision and start chasing unrelated global publications.
	if err := m.validatePendingProjectionTarget(ctx, request, recovery, validator); err != nil {
		return err
	}
	// Keep placementMu through the prepare-only target callback. This
	// linearizes target preparation with ExcludePlacement without blocking
	// unrelated Manager operations on the remote callback.
	return controller.ColdLoadNewHolder(ctx, request.domain, request.virtualShard, current)
}

// beginDrainFlight installs the fixed in-flight memo and reserves its bounded
// slot before any holder callback. A same-key same-digest caller waits for the
// owner; a conflicting caller fails closed instead of running a second drain.
func (m *RouteAssignmentManager) beginDrainFlight(ctx context.Context, request transitionRequest) (*drainFlight, bool, authoritystore.ShardAuthorityTransitionResult, error) {
	if m == nil {
		return nil, false, authoritystore.ShardAuthorityTransitionResult{}, ErrInvalidManager
	}
	fingerprint := transitionFingerprint(request)
	key := drainKey{domain: request.domain, shard: request.virtualShard}
	m.mu.Lock()
	if m.pendingLeaseProjection != nil {
		m.mu.Unlock()
		return nil, false, authoritystore.ShardAuthorityTransitionResult{}, ErrLeaseProjectionPending
	}
	if current := m.drainFlights[key]; current != nil {
		if current.fingerprint != fingerprint {
			m.mu.Unlock()
			return nil, false, authoritystore.ShardAuthorityTransitionResult{}, ErrDrainInFlight
		}
		m.mu.Unlock()
		return current, false, authoritystore.ShardAuthorityTransitionResult{}, nil
	}
	if err := m.pruneMemosLocked(m.now()); err != nil {
		m.mu.Unlock()
		return nil, false, authoritystore.ShardAuthorityTransitionResult{}, err
	}
	memo := m.transitionMemoLocked(request.transitionID)
	if memo != nil && memo.fingerprint != fingerprint {
		result := memo.result
		m.mu.Unlock()
		return nil, false, result, fmt.Errorf("%w: transition_id=%s", ErrTransitionReconcile, request.transitionID)
	}
	if memo != nil && memo.commitAmbiguous {
		if err := m.promoteAmbiguousMemoLocked(ctx, memo); err != nil {
			result := memo.result
			m.mu.Unlock()
			return nil, false, result, err
		}
	}
	if memo != nil && memo.blocked {
		result, blockErr := memo.result, memo.blockErr
		if blockErr == nil {
			blockErr = ErrStaleTransition
		}
		m.mu.Unlock()
		return nil, false, result, blockErr
	}
	if memo != nil && memo.lastState == actorrouting.ShardStateActive && memo.published && !memo.inFlight {
		if _, reconcileErr := m.reconcileAuthorityLocked(ctx, memo); reconcileErr != nil {
			if errors.Is(reconcileErr, ErrStaleTransition) {
				memo.blocked = true
				memo.blockErr = reconcileErr
			}
			result := memo.result
			m.mu.Unlock()
			return nil, false, result, reconcileErr
		}
		memo.lastUsedAt = m.now()
		result := memo.result
		m.mu.Unlock()
		return nil, false, result, nil
	}
	if memo == nil {
		if tombstone, found, err := m.transitionTombstoneLocked(request.transitionID); err != nil {
			m.mu.Unlock()
			return nil, false, authoritystore.ShardAuthorityTransitionResult{}, err
		} else if found {
			if tombstone.Fingerprint != fingerprint {
				m.mu.Unlock()
				return nil, false, tombstone.Result, fmt.Errorf("%w: transition_id=%s expired with a different digest", ErrTransitionReconcile, request.transitionID)
			}
			m.mu.Unlock()
			return nil, false, tombstone.Result, fmt.Errorf("%w: transition_id=%s", ErrTransitionMemoExpired, request.transitionID)
		}
		if err := m.validateRequestAgainstSnapshotLocked(request); err != nil {
			m.mu.Unlock()
			return nil, false, authoritystore.ShardAuthorityTransitionResult{}, err
		}
		if err := m.reserveTransitionSlotLocked(request.transitionID, fingerprint, m.now()); err != nil {
			m.mu.Unlock()
			return nil, false, authoritystore.ShardAuthorityTransitionResult{}, err
		}
		if err := m.reserveRecoveryLocked(request, fingerprint, m.now()); err != nil {
			m.releaseTransitionReservationLocked(request.transitionID, fingerprint)
			m.mu.Unlock()
			return nil, false, authoritystore.ShardAuthorityTransitionResult{}, fmt.Errorf("%w: recovery reservation: %v", ErrTransitionTombstoneCap, err)
		}
		now := m.now()
		memo = &transitionMemo{fingerprint: fingerprint, request: request, inFlight: true, createdAt: now, lastUsedAt: now}
		m.transitions[request.transitionID] = memo
	}
	if previousID := m.drainIDs[key]; previousID != request.transitionID {
		m.drainIDs[key] = request.transitionID
		m.drains[key] = DrainStageActive
	}
	flight := &drainFlight{key: key, transitionID: request.transitionID, fingerprint: fingerprint, done: make(chan struct{})}
	m.drainFlights[key] = flight
	m.mu.Unlock()
	return flight, true, authoritystore.ShardAuthorityTransitionResult{}, nil
}

func (m *RouteAssignmentManager) waitDrainFlight(ctx context.Context, flight *drainFlight) (authoritystore.ShardAuthorityTransitionResult, error) {
	if ctx == nil {
		return authoritystore.ShardAuthorityTransitionResult{}, ErrInvalidManager
	}
	select {
	case <-flight.done:
		m.mu.Lock()
		result, err := flight.result, flight.err
		m.mu.Unlock()
		return result, err
	case <-ctx.Done():
		return authoritystore.ShardAuthorityTransitionResult{}, ctx.Err()
	}
}

func (m *RouteAssignmentManager) finishDrainFlight(flight *drainFlight, result authoritystore.ShardAuthorityTransitionResult, err error) error {
	if m == nil || flight == nil {
		return err
	}
	m.mu.Lock()
	finalErr := err
	if current := m.drainFlights[flight.key]; current == flight {
		delete(m.drainFlights, flight.key)
	}
	if memo := m.transitionMemoLocked(flight.transitionID); memo != nil {
		if err != nil && memo.inFlight {
			if cleanupErr := m.deleteRecoveryLocked(flight.transitionID); cleanupErr != nil {
				// Retain the exact in-memory/durable reservation for a bounded retry.
				// Ignoring this error could make startup treat a known pre-Authority
				// record as ambiguous forever, while deleting memory would lose the
				// only same-process cleanup handle.
				memo.inFlight = false
				memo.lastUsedAt = m.now()
				finalErr = errors.Join(finalErr, fmt.Errorf("%w: delete pre-Authority recovery: %v", ErrTransitionTombstoneCap, cleanupErr))
			} else {
				delete(m.transitions, flight.transitionID)
				delete(m.recoveries, flight.transitionID)
				m.releaseTransitionReservationLocked(flight.transitionID, flight.fingerprint)
			}
		} else {
			memo.lastUsedAt = m.now()
		}
	}
	flight.result, flight.err = result, finalErr
	close(flight.done)
	m.mu.Unlock()
	return finalErr
}

// drainCheckpoint returns 0 before authority, 2 after ASSIGNING publication,
// 3 after cold load, and 4 after ACTIVE publication. The memo is the only
// durable-in-process checkpoint for side effects that must not be repeated.
func (m *RouteAssignmentManager) drainCheckpoint(request transitionRequest) (int, authoritystore.ShardAuthorityTransitionResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	memo := m.transitionMemoLocked(request.transitionID)
	if memo == nil {
		return 0, authoritystore.ShardAuthorityTransitionResult{}, nil
	}
	if memo.fingerprint != transitionFingerprint(request) {
		return 0, memo.result, fmt.Errorf("%w: transition_id=%s", ErrTransitionReconcile, request.transitionID)
	}
	if memo.lastState == actorrouting.ShardStateActive && memo.published && !memo.inFlight {
		return 4, memo.result, nil
	}
	if memo.lastState == actorrouting.ShardStateAssigning && memo.published && !memo.inFlight {
		if memo.coldLoaded {
			return 3, memo.result, nil
		}
		return 2, memo.result, nil
	}
	if memo.request.internalDrain && memo.lastState == actorrouting.ShardStateAssigning &&
		(memo.result.Status == authoritystore.ShardTransitionApplied || memo.result.Status == authoritystore.ShardTransitionAlreadyApplied) {
		// Authority and the drain side effects are already committed, but the
		// ASSIGNING projection may have failed. Restart/retry at the projection
		// checkpoint; Stop/Wait/Facts must not run a second time.
		return 1, memo.result, nil
	}
	return 0, memo.result, nil
}

func (m *RouteAssignmentManager) drainStageFor(key drainKey, transitionID string) DrainStage {
	if m == nil {
		return DrainStageActive
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.drainIDs[key] != transitionID {
		return DrainStageActive
	}
	return m.drains[key]
}

func normalizedDrainRequest(req TransferRequest) (transitionRequest, error) {
	if req.Domain == actorrouting.DomainFarmWorker {
		return transitionRequest{}, fmt.Errorf(
			"%w: FARM_WORKER requires the worker handoff contract",
			ErrInvalidManager,
		)
	}
	request := transitionRequest{
		kind: transitionTransfer, transitionID: req.TransitionID, domain: req.Domain,
		virtualShard: req.VirtualShard, expectedFence: req.ExpectedFence,
		newHolderID: req.NewHolderID, newHolderBoot: req.NewHolderBootID,
		endpoint: req.Endpoint, ttl: req.RequestedTTL,
		mode: TransferModeDrainHandoff, internalDrain: true,
	}
	if err := validateTransitionRequest(request); err != nil {
		return transitionRequest{}, err
	}
	ttl, err := durationMillisForTransition(request)
	if err != nil {
		return transitionRequest{}, err
	}
	request.ttl = time.Duration(ttl) * time.Millisecond
	return request, nil
}

func (m *RouteAssignmentManager) completedDrainReplay(request transitionRequest) (authoritystore.ShardAuthorityTransitionResult, bool, error) {
	if m == nil {
		return authoritystore.ShardAuthorityTransitionResult{}, false, ErrInvalidManager
	}
	fingerprint := transitionFingerprint(request)
	m.mu.Lock()
	defer m.mu.Unlock()
	memo := m.transitionMemoLocked(request.transitionID)
	if memo == nil || memo.fingerprint != fingerprint || memo.lastState != actorrouting.ShardStateActive || !memo.published {
		if memo != nil && memo.fingerprint != fingerprint {
			return memo.result, false, fmt.Errorf("%w: transition_id=%s", ErrTransitionReconcile, request.transitionID)
		}
		if memo == nil {
			if tombstone, found, err := m.transitionTombstoneLocked(request.transitionID); err != nil {
				return authoritystore.ShardAuthorityTransitionResult{}, false, err
			} else if found {
				if tombstone.Fingerprint != fingerprint {
					return tombstone.Result, false, fmt.Errorf("%w: transition_id=%s expired with a different digest", ErrTransitionReconcile, request.transitionID)
				}
				return tombstone.Result, false, fmt.Errorf("%w: transition_id=%s", ErrTransitionMemoExpired, request.transitionID)
			}
		}
		return authoritystore.ShardAuthorityTransitionResult{}, false, nil
	}
	memo.lastUsedAt = m.now()
	return memo.result, true, nil
}

func (m *RouteAssignmentManager) resumableDrainReplay(request transitionRequest) (authoritystore.ShardAuthorityTransitionResult, bool, error) {
	if m == nil {
		return authoritystore.ShardAuthorityTransitionResult{}, false, ErrInvalidManager
	}
	fingerprint := transitionFingerprint(request)
	m.mu.Lock()
	defer m.mu.Unlock()
	memo := m.transitionMemoLocked(request.transitionID)
	if memo == nil || memo.fingerprint != fingerprint || memo.lastState != actorrouting.ShardStateAssigning || !memo.published || !memo.coldLoaded {
		if memo != nil && memo.fingerprint != fingerprint {
			return memo.result, false, fmt.Errorf("%w: transition_id=%s", ErrTransitionReconcile, request.transitionID)
		}
		return authoritystore.ShardAuthorityTransitionResult{}, false, nil
	}
	return memo.result, true, nil
}

func (m *RouteAssignmentManager) markDrainColdLoaded(transitionID string, coldLoadedAuthority authoritystore.ShardWriteAuthority) error {
	if m == nil {
		return ErrInvalidManager
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if memo := m.transitionMemoLocked(transitionID); memo != nil {
		if coldLoadedAuthority.LastTransitionID != transitionID ||
			!sameAuthorityGeneration(coldLoadedAuthority, memo.result.Authority) ||
			coldLoadedAuthority.LeaseUntilMs <= m.now().UnixMilli() {
			return fmt.Errorf("%w: transition_id=%s cold-load authority changed", ErrTransitionReconcile, transitionID)
		}
		// Persist the exact tuple that Prepare accepted. In particular, a same-
		// generation renewal between Authority commit and ColdLoad must update the
		// durable result before the immutable cold-loaded marker is written.
		memo.result.Authority = coldLoadedAuthority
		memo.coldLoaded = true
		memo.lastUsedAt = m.now()
		if err := m.updateRecoveryLocked(memo); err != nil {
			// Keep the in-process checkpoint set: the remote prepare completed and
			// a bounded retry in this process must not repeat it. The durable error
			// still prevents ACTIVE publication and is surfaced to the caller.
			return fmt.Errorf("%w: cold-load recovery checkpoint: %v", ErrSnapshotPublish, err)
		}
		return nil
	}
	return fmt.Errorf("%w: transition_id=%s", ErrTransitionReconcile, transitionID)
}

// DrainState returns the local state machine stage. Missing shards are ACTIVE
// so callers can treat the zero value as the steady state.
func (m *RouteAssignmentManager) DrainState(domain actorrouting.RouteDomain, shard uint32) DrainStage {
	if m == nil {
		return DrainStageActive
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.drains[drainKey{domain: domain, shard: shard}]
}

func (m *RouteAssignmentManager) setDrainStage(key drainKey, stage DrainStage) {
	m.mu.Lock()
	m.drains[key] = stage
	m.mu.Unlock()
}

func (m *RouteAssignmentManager) runTransitionState(ctx context.Context, request transitionRequest, state actorrouting.ShardRouteState, recovery TransitionRecovery, targetValidator PendingProjectionTargetValidator) (authoritystore.ShardAuthorityTransitionResult, error) {
	if err := validateTransitionRequest(request); err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, err
	}
	ttl, err := durationMillisForTransition(request)
	if err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, err
	}
	request.ttl = time.Duration(ttl) * time.Millisecond
	fingerprint := transitionFingerprint(request)
	recoveryReserved := false
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.pruneMemosLocked(m.now()); err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, err
	}
	if memo := m.transitionMemoLocked(request.transitionID); memo != nil {
		if memo.fingerprint != fingerprint {
			return memo.result, fmt.Errorf("%w: transition_id=%s", ErrTransitionReconcile, request.transitionID)
		}
		if memo.inFlight {
			// Drain installed this placeholder before holder side effects. The
			// owner now fills it with the committed result using its reservation.
			goto commit
		}
		memo.lastUsedAt = m.now()
		if memo.blocked {
			if memo.blockErr != nil {
				return memo.result, memo.blockErr
			}
			return memo.result, ErrStaleTransition
		}
		if memo.lastState == state && memo.published {
			if _, reconcileErr := m.reconcileAuthorityLocked(ctx, memo); reconcileErr != nil {
				if errors.Is(reconcileErr, ErrStaleTransition) {
					memo.blocked = true
					memo.blockErr = reconcileErr
				}
				return memo.result, reconcileErr
			}
			return memo.result, nil
		}
		if err := m.publishMemoLocked(ctx, memo, state, true, recovery, targetValidator); err != nil {
			return memo.result, err
		}
		return memo.result, nil
	}
	if tombstone, found, err := m.transitionTombstoneLocked(request.transitionID); err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, err
	} else if found {
		if tombstone.Fingerprint != fingerprint {
			return tombstone.Result, fmt.Errorf("%w: transition_id=%s expired with a different digest", ErrTransitionReconcile, request.transitionID)
		}
		return tombstone.Result, fmt.Errorf("%w: transition_id=%s", ErrTransitionMemoExpired, request.transitionID)
	}
	if err := m.validateRequestAgainstSnapshotLocked(request); err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, err
	}
	if err := m.reserveTransitionSlotLocked(request.transitionID, fingerprint, m.now()); err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, err
	}
	if err := m.reserveRecoveryLocked(request, fingerprint, m.now()); err != nil {
		m.releaseTransitionReservationLocked(request.transitionID, fingerprint)
		return authoritystore.ShardAuthorityTransitionResult{}, fmt.Errorf("%w: recovery reservation: %v", ErrTransitionTombstoneCap, err)
	}
	recoveryReserved = true
commit:
	if err := m.validatePendingProjectionTargetLocked(ctx, request, recovery, targetValidator); err != nil {
		if recoveryReserved {
			if releaseErr := m.discardPrecommitRecoveryLocked(request.transitionID, fingerprint); releaseErr != nil {
				return authoritystore.ShardAuthorityTransitionResult{}, errors.Join(err, releaseErr)
			}
		}
		return authoritystore.ShardAuthorityTransitionResult{}, err
	}
	if err := m.markAuthorityAttemptedReleasingLock(request, fingerprint, state); err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, fmt.Errorf("%w: persist Authority attempt boundary: %v", ErrAuthorityUnavailable, err)
	}
	var result authoritystore.ShardAuthorityTransitionResult
	_ = m.runWithoutStateLock(func() error {
		result, err = m.commitLocked(ctx, request, ttl)
		return nil
	})
	if transitionOutcomeDefinitivelyNotSent(result) {
		if releaseErr := m.discardPrecommitRecoveryLocked(request.transitionID, fingerprint); releaseErr != nil {
			return result, releaseErr
		}
		outcomeErr := transitionMustBeCommitted(result)
		if err != nil {
			return result, fmt.Errorf("%w: store response: %v", outcomeErr, err)
		}
		return result, outcomeErr
	}
	if err != nil || transitionOutcomeAmbiguous(result) {
		m.retainAmbiguousTransitionLocked(request, fingerprint, result, state)
		if err != nil {
			return result, fmt.Errorf("%w: store response: %v", ErrAuthorityCommitUnknown, err)
		}
		return result, transitionMustBeCommitted(result)
	}
	if err := transitionMustBeCommitted(result); err != nil {
		if recoveryReserved {
			if releaseErr := m.discardPrecommitRecoveryLocked(request.transitionID, fingerprint); releaseErr != nil {
				return result, releaseErr
			}
		}
		return result, err
	}
	if err := m.validateCommittedAuthorityLocked(request, result); err != nil {
		m.retainAmbiguousTransitionLocked(request, fingerprint, result, state)
		return result, err
	}
	bootstrapProjection := m.bootstrap[authorityKey{domain: request.domain, shard: request.virtualShard}]
	if m.initialBatchOpen && !bootstrapProjection && request.kind == transitionClaim && result.Status == authoritystore.ShardTransitionAlreadyApplied {
		routes := snapshotDomain(m.snapshot, request.domain)
		if request.virtualShard < routes.ShardCount && int(request.virtualShard) < len(routes.Routes) && isInitialClaimPlaceholder(routes.Routes[request.virtualShard]) {
			bootstrapProjection = true
		}
	}
	m.consumeTransitionReservationLocked(request.transitionID, fingerprint)
	delete(m.bootstrap, authorityKey{domain: request.domain, shard: request.virtualShard})
	m.committed[authorityKey{domain: request.domain, shard: request.virtualShard}] = result.Authority
	now := m.now()
	memo := m.transitionMemoLocked(request.transitionID)
	if memo == nil {
		memo = &transitionMemo{fingerprint: fingerprint, request: request, createdAt: now}
		m.transitions[request.transitionID] = memo
	}
	memo.result = result
	memo.request = request
	memo.bootstrapProjection = memo.bootstrapProjection || bootstrapProjection
	memo.lastState = state
	memo.published = false
	memo.projectionPending = true
	memo.inFlight = false
	memo.authorityAttempted = true
	memo.createdAt = now
	memo.lastUsedAt = now
	if err := m.commitRecoveryReleasingLock(memo); err != nil {
		return result, fmt.Errorf("%w: recovery commit: %v", ErrSnapshotPublish, err)
	}
	if err := m.publishMemoLocked(ctx, memo, state, false, recovery, targetValidator); err != nil {
		if pruneErr := m.pruneMemosLocked(m.now()); pruneErr != nil {
			return result, fmt.Errorf("%w: %v", err, pruneErr)
		}
		return result, err
	}
	return result, nil
}

func (m *RouteAssignmentManager) publishTransitionState(ctx context.Context, transitionID string, state actorrouting.ShardRouteState, recovery TransitionRecovery, targetValidator PendingProjectionTargetValidator) error {
	// Drain/recovery owns placementMu across the complete target-effect wave.
	// Re-entering RWMutex.RLock here could deadlock behind a queued exclusion.
	m.mu.Lock()
	defer m.mu.Unlock()
	memo := m.transitionMemoLocked(transitionID)
	if memo == nil {
		return fmt.Errorf("%w: transition_id=%s", ErrTransitionReconcile, transitionID)
	}
	return m.publishMemoLocked(ctx, memo, state, true, recovery, targetValidator)
}

// ReleasePlacementExclusionForOwner forwards to the placement controller under
// the caller's inherited token. The Manager adds no policy: it is a Store here,
// and inserting a second opinion between ScalePlan and the record would give
// the release two places to disagree about who owns the exclusion.
func (m *RouteAssignmentManager) ReleasePlacementExclusionForOwner(ctx context.Context, token routecontrol.NestedControlOperationToken, domain actorrouting.RouteDomain, holderID, bootID string, release routecontrol.PlacementExclusionRelease) (routecontrol.PlacementExclusionResult, error) {
	if m == nil || m.placementExclusions == nil {
		return routecontrol.PlacementExclusionResult{}, routecontrol.ErrPlacementExclusionUnavailable
	}
	if !token.Issued() {
		return routecontrol.PlacementExclusionResult{}, fmt.Errorf("%w: release requires an issued control token",
			routecontrol.ErrPlacementExclusionInvalid)
	}
	if m.controlCoordinator != nil {
		if err := m.controlCoordinator.Verify(ctx, token, m.controlScope); err != nil {
			return routecontrol.PlacementExclusionResult{}, fmt.Errorf("%w: %v", ErrControlTokenRejected, err)
		}
	}
	return m.placementExclusions.ReleasePlacementExclusionForOwner(ctx, token, domain, holderID, bootID, release)
}
