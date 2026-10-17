package scaleplan

import (
	"context"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
)

// WorkloadPodState keeps Kubernetes object identity separate from holder
// process identity. A production adapter must derive WorkloadUID from the Pod
// owner reference / StatefulSet API rather than trusting a parsed name.
type WorkloadPodState struct {
	PodName            string
	PodUID             string
	WorkloadUID        string
	ControllerRevision string
	ContainerImage     string
	ContainerImageID   string
	Ordinal            int32
	HolderID           string
	Present            bool
	Ready              bool
}

type WorkloadState struct {
	Ref                WorkloadRef
	UID                string
	ResourceVersion    string
	Generation         int64
	ObservedGeneration int64
	Replicas           int32
	ReadyReplicas      int32
	CurrentRevision    string
	UpdateRevision     string
	UpdateStrategy     string
	Pods               []WorkloadPodState
}

type ScalePrecondition struct {
	WorkloadUID      string
	ResourceVersion  string
	ExpectedReplicas int32
}

type ScaleMutationResult struct {
	Committed bool
	State     WorkloadState
}

type WorkloadScaler interface {
	Get(context.Context, WorkloadRef) (WorkloadState, error)
	Scale(context.Context, WorkloadRef, ScalePrecondition, int32) (ScaleMutationResult, error)
}

type HolderLifecycleState string

const (
	HolderWarming        HolderLifecycleState = "WARMING"
	HolderReadyZeroShard HolderLifecycleState = "READY_ZERO_SHARD"
	HolderServing        HolderLifecycleState = "SERVING"
	HolderDraining       HolderLifecycleState = "DRAINING"
	HolderDrained        HolderLifecycleState = "DRAINED"
)

type HolderState struct {
	Ref                   HolderRef
	PodName               string
	LogicEndpoint         string
	HolderControlEndpoint string
	TLSDNSIdentity        string
	State                 HolderLifecycleState
	ProcessReady          bool
	Draining              bool
	ServingShardCount     uint32
	SnapshotRevision      uint64
	AssignmentKnownFresh  bool
	PlacementEligible     bool
	PlacementRevision     uint64
	LeaseUntil            time.Time
}

type HolderStateReader interface {
	GetHolder(context.Context, HolderRef) (HolderState, bool, error)
	ListDomainHolders(context.Context, actorrouting.RouteDomain) ([]HolderState, error)
}

// PlacementExclusion is a durable, exact-Boot target-admission fact. It is
// intentionally independent from Holder lifecycle DRAINING: an excluded
// holder must remain able to serve and renew its existing shards while it is
// still a legal source for planned RoutePlans.
type PlacementExclusion struct {
	Holder      HolderRef
	Excluded    bool
	Revision    uint64
	OwnerPlanID string
}

type PlacementExclusionPrecondition struct {
	ExpectedRevision uint64
	ExpectedExcluded bool
	OwnerPlanID      string
}

type PlacementExclusionResult struct {
	Committed bool
	State     PlacementExclusion
	// Outcome is populated by ReleaseForOwner. Exclude predates the release
	// seam and leaves it empty.
	Outcome PlacementReleaseOutcome
}

// PlacementReleaseOutcome is the closed ScalePlan-side mapping of the
// owner-scoped route-control release result. Unknown values are never safe.
type PlacementReleaseOutcome string

const (
	PlacementReleaseReleased         PlacementReleaseOutcome = "RELEASED"
	PlacementReleaseAlreadyReleased  PlacementReleaseOutcome = "ALREADY_RELEASED"
	PlacementReleaseBootRollover     PlacementReleaseOutcome = "BOOT_ROLLOVER"
	PlacementReleaseNotFound         PlacementReleaseOutcome = "NOT_FOUND"
	PlacementReleaseOwnerMismatch    PlacementReleaseOutcome = "OWNER_MISMATCH"
	PlacementReleaseRevisionConflict PlacementReleaseOutcome = "REVISION_CONFLICT"
	PlacementReleaseMalformed        PlacementReleaseOutcome = "MALFORMED"
	PlacementReleaseUnknown          PlacementReleaseOutcome = "UNKNOWN"

	// Compatibility spelling for callers that use the route-control name.
	PlacementReleaseByBootRollover = PlacementReleaseBootRollover
)

func (o PlacementReleaseOutcome) Valid() bool {
	switch o {
	case PlacementReleaseReleased, PlacementReleaseAlreadyReleased,
		PlacementReleaseBootRollover, PlacementReleaseNotFound,
		PlacementReleaseOwnerMismatch, PlacementReleaseRevisionConflict,
		PlacementReleaseMalformed, PlacementReleaseUnknown:
		return true
	default:
		return false
	}
}

func (o PlacementReleaseOutcome) TerminalForScalePlan() bool {
	return o == PlacementReleaseReleased || o == PlacementReleaseAlreadyReleased ||
		o == PlacementReleaseBootRollover
}

// PlacementExclusionRelease is the exact owner/revision precondition for
// reversing one exact HolderRef generation.
type PlacementExclusionRelease struct {
	OwnerPlanID      string
	ExpectedRevision uint64
}

type PlacementExclusionPort interface {
	Get(context.Context, HolderRef) (PlacementExclusion, bool, error)
	Exclude(context.Context, HolderRef, PlacementExclusionPrecondition) (PlacementExclusionResult, error)
	ReleaseForOwner(context.Context, HolderRef, PlacementExclusionRelease) (PlacementExclusionResult, error)
}

type ShardPlacement struct {
	Domain              actorrouting.RouteDomain
	VirtualShard        uint32
	Active              bool
	SnapshotRevision    uint64
	Holder              HolderRef
	Endpoint            string
	ExpectedSourceFence authoritystore.ShardWriteFence
	KnownFresh          bool
	LeaseUntil          time.Time
}

// PlacementCaptureToken is an opaque proof handle for an optimistic,
// cross-source capture. Production adapters must internally capture one
// complete durable snapshot, exact Authority and Holder facts, then bracket
// and compare the relevant fields. Revalidate must repeat the end bracket.
//
// The field list is frozen by section 8.2 and every field must stay
// comparable, because Revalidate compares whole tokens with ==. Adding a
// slice or a map would silently make that comparison a compile error; adding
// a clock reading would do something worse, see below.
//
// What this type must NOT carry is as load-bearing as what it does. No field
// may hold an authority lease deadline, or any other quantity a same-
// generation renew advances. Holder-side renew is exempt from the control
// gate and therefore runs continuously during a capture (section 8.6); a
// deadline in the token would invalidate every capture within one renewal
// interval, and scale operations could never assemble complete evidence.
// Lease epoch and assignment revision are the deliberate substitutes: they
// move when placement really changes and stay put across a renew, which is
// the exact boundary the liveness exemption rests on.
// TestCaptureTokenCannotCarryAClockReading enforces this by reflection rather
// than by review, because a reviewer reading a struct is precisely what let
// three inert parameters through this stage already.
type PlacementCaptureToken struct {
	ID string
	// Scope is realm/domain-global. WorkloadUID rides along only to make an
	// exact join possible and must never be used to narrow the set of
	// placements a capture covers.
	Scope       string
	WorkloadUID string

	SnapshotRevision    uint64
	SnapshotChecksum    string
	TopologyFingerprint string

	// ActiveCardinality is what turns "the shards I looked at are unchanged"
	// into "no shard appeared or vanished". Without it a capture cannot tell
	// a complete snapshot from a partial one.
	ActiveCardinality uint64

	HolderRegistryRevision uint64
	HolderRegistryChecksum string

	AuthorityCaptureRevision uint64
	// LeaseEpochDigest aggregates the per-shard authority lease epochs. A sum
	// would be cheaper but two shards moving in opposite directions inside one
	// bracket would cancel out, so this is a digest over sorted pairs.
	LeaseEpochDigest   string
	AssignmentRevision uint64
	FencingEpoch       uint64
}

type ShardPlacementCapture struct {
	Token     PlacementCaptureToken
	Placement ShardPlacement
	Found     bool
}

type ShardKey struct {
	Domain       actorrouting.RouteDomain
	VirtualShard uint32
}

type ShardSetCapture struct {
	Token      PlacementCaptureToken
	Placements []ShardPlacement
}

type HolderShardCapture struct {
	Token      PlacementCaptureToken
	Holder     HolderRef
	Placements []ShardPlacement
}

type ShardPlacementReader interface {
	CaptureShard(context.Context, actorrouting.RouteDomain, uint32) (ShardPlacementCapture, error)
	// CaptureShards must return all requested keys from one complete snapshot
	// identity or fail the whole capture. It cannot combine per-key revisions.
	CaptureShards(context.Context, []ShardKey) (ShardSetCapture, error)
	CaptureHolderShards(context.Context, HolderRef) (HolderShardCapture, error)
	Revalidate(context.Context, PlacementCaptureToken) error
}

type RoutePlanMoveRequest struct {
	PlanID              string
	TransitionID        string
	Domain              actorrouting.RouteDomain
	VirtualShard        uint32
	Source              HolderRef
	SourceEndpoint      string
	Target              HolderRef
	TargetEndpoint      string
	ExpectedSourceFence authoritystore.ShardWriteFence
	RequestedTTLms      int64
	OwnerScalePlanID    string
	OwnerRetainUntilMs  int64
	OwnerWaveID         string
	OwnerWaveSize       uint32
	OwnerWavePosition   uint32
	// PayloadDigest is the ScalePlan-side immutable request digest used for
	// exact adapter comparison. It is not a RoutePlan service field.
	PayloadDigest string
}

type RoutePlanView struct {
	PlanID        string
	PayloadDigest string
	Phase         string
	ErrorCode     string
}

const (
	RoutePlanPhasePending   = "PENDING"
	RoutePlanPhaseDraining  = "DRAINING"
	RoutePlanPhaseAssigning = "ASSIGNING"
	RoutePlanPhasePreparing = "PREPARING"
	RoutePlanPhaseActive    = "ACTIVE"
	RoutePlanPhaseSucceeded = "SUCCEEDED"
	RoutePlanPhaseFailed    = "FAILED"
	RoutePlanPhasePaused    = "PAUSED"
)

func validRoutePlanObservedPhase(phase string) bool {
	switch phase {
	case RoutePlanPhasePending, RoutePlanPhaseDraining, RoutePlanPhaseAssigning, RoutePlanPhasePreparing,
		RoutePlanPhaseActive, RoutePlanPhaseSucceeded, RoutePlanPhaseFailed, RoutePlanPhasePaused:
		return true
	default:
		return false
	}
}

func routePlanNonterminalCanAdvance(from, to string) bool {
	toRank, toValid := routePlanNonterminalRank(to)
	if !toValid {
		return false
	}
	if from == "" {
		return true
	}
	fromRank, fromValid := routePlanNonterminalRank(from)
	return fromValid && toRank >= fromRank
}

func routePlanNonterminalRank(phase string) (int, bool) {
	switch phase {
	case RoutePlanPhasePending:
		return 0, true
	case RoutePlanPhaseDraining:
		return 1, true
	case RoutePlanPhaseAssigning:
		return 2, true
	case RoutePlanPhasePreparing:
		return 3, true
	case RoutePlanPhaseActive:
		return 4, true
	default:
		return 0, false
	}
}

type RoutePlanVerification struct {
	PlanID     string
	Consistent bool
	ErrorCode  string
}

type RoutePlanPort interface {
	EnsureMove(context.Context, RoutePlanMoveRequest) (RoutePlanView, error)
	// GetMove receives the complete expected immutable request so a production
	// adapter can exact-compare an existing RoutePlan after restart. The serving
	// RoutePlan record need not persist ScalePlan's private payload digest; the
	// adapter returns that digest only after every serving immutable field
	// matches this request.
	GetMove(context.Context, RoutePlanMoveRequest) (RoutePlanView, bool, error)
	Verify(context.Context, string) (RoutePlanVerification, error)
}

// BlockedTerminationRequest binds one explicit operator action to one exact
// blocked checkpoint. A conflict never rebases to a later revision.
type BlockedTerminationRequest struct {
	PlanID                   string
	ExpectedRevision         uint64
	ExpectedBlockedReason    string
	ExpectedPayloadDigest    string
	ExpectedCheckpointDigest string
	TerminationID            string
}

type BlockedTerminationStatus string

const (
	BlockedTerminationCommitted           BlockedTerminationStatus = "COMMITTED"
	BlockedTerminationAlreadyCommitted    BlockedTerminationStatus = "ALREADY_COMMITTED"
	BlockedTerminationUnsafe              BlockedTerminationStatus = "UNSAFE"
	BlockedTerminationProofExpired        BlockedTerminationStatus = "PROOF_EXPIRED"
	BlockedTerminationRevisionConflict    BlockedTerminationStatus = "REVISION_CONFLICT"
	BlockedTerminationExpectationMismatch BlockedTerminationStatus = "EXPECTATION_MISMATCH"
	BlockedTerminationNotBlocked          BlockedTerminationStatus = "NOT_BLOCKED"
)

type BlockedTerminationResult struct {
	Status BlockedTerminationStatus
	Record PlanRecord
}

// BlockedQuiescenceProof is a bounded canonical manifest. Every component
// digest is produced from the corresponding versioned manifest below; the
// aggregate EvidenceDigest is deterministic and independently checked.
type BlockedQuiescenceProof struct {
	ProtocolVersion          string
	PlanID                   string
	ExpectedRevision         uint64
	ExpectedBlockedReason    string
	ExpectedPayloadDigest    string
	ExpectedCheckpointDigest string
	EvidenceDigest           string
	WorkloadDigest           string
	PlacementDigest          string
	ExclusionDigest          string
	ChildInventoryDigest     string
	ManagerQuiescenceDigest  string
	CapturedAtMs             int64
	ExpiresAtMs              int64
	EvidenceRetainUntilMs    int64
	IntentCount              uint32
	ChildCount               uint32
	ScaleResolution          ScaleResolution
	ExclusionReleaseOutcome  PlacementReleaseOutcome
}

type BlockedQuiescenceManifest struct {
	Workload              BlockedWorkloadManifest
	Placement             BlockedPlacementManifest
	Exclusion             BlockedExclusionManifest
	Children              BlockedChildInventoryManifest
	ManagerTransitions    BlockedManagerQuiescenceManifest
	CapturedAtMs          int64
	ExpiresAtMs           int64
	EvidenceRetainUntilMs int64
}

type BlockedWorkloadManifest struct {
	State   WorkloadState
	Holders []HolderState
}

type BlockedPlacementManifest struct {
	Complete            bool
	Domain              actorrouting.RouteDomain
	SnapshotRevision    uint64
	SnapshotChecksum    string
	TopologyFingerprint string
	TotalActive         uint32
	Placements          []ShardPlacement
}

type BlockedExclusionManifest struct {
	Present bool
	State   PlacementExclusion
	// ReleaseOutcome is set only when this manifest proves that an active
	// exclusion was released under the quiescence fence.
	ReleaseOutcome PlacementReleaseOutcome
}

type BlockedChildInventoryManifest struct {
	Children []BlockedChildManifest
}

type BlockedChildManifest struct {
	PlanID                string
	TransitionID          string
	PayloadDigest         string
	ParentObservedPhase   string
	Phase                 string
	ErrorCode             string
	DurablyStopped        bool
	Source                HolderRef
	SourceEndpoint        string
	Target                HolderRef
	TargetEndpoint        string
	ExpectedSourceFence   authoritystore.ShardWriteFence
	ManagerTransitionID   string
	ManagerRecoveryID     string
	EvidenceRetainUntilMs int64
}

type BlockedManagerQuiescenceManifest struct {
	// Records carries one entry per resolved move. This is existential
	// evidence: it answers "are the transitions I know about finished".
	Records []BlockedManagerQuiescenceRecord
	// Universal answers the question quiescence actually asks, which is
	// whether any unfinished transition exists in the realm/domain at all,
	// including ones this plan never learned about. Section 8.6 rejected
	// Manager.ObserveTransition as the sole source for exactly this reason:
	// its input is one transition ID, so it cannot express a negative over a
	// set it was never handed.
	Universal ManagerQuiescenceEnumeration
}

// ManagerQuiescenceEnumeration is the enumeration-plus-cardinality half of the
// quiescence evidence frozen in section 8.6. It is built from
// LoadTransitionRecoveries and LoadTransitionTombstones under an EXCLUSIVE
// gate, and the end bracket re-enumerates.
type ManagerQuiescenceEnumeration struct {
	Domain actorrouting.RouteDomain
	// Enumerated distinguishes "the set is empty" from "nobody looked".
	// A zero value must never read as quiescent, so the absence of this flag
	// is BLOCKED_QUIESCENCE_EVIDENCE rather than a pass.
	Enumerated           bool
	UnfinishedRecoveries uint32
	UnfinishedTombstones uint32
	// AssignmentRevision anchors the enumeration to a point in time so the end
	// bracket can tell a still-empty set apart from a set that emptied and
	// refilled.
	AssignmentRevision uint64
	EnumeratedAtMs     int64
}

type BlockedManagerQuiescenceRecord struct {
	ChildPlanID           string
	TransitionID          string
	RecoveryID            string
	Quiescent             bool
	EvidenceRetainUntilMs int64
}

type BlockedQuiescenceFence interface {
	Proof() BlockedQuiescenceProof
	Revalidate(context.Context) error
	Release(context.Context) error
}

type BlockedQuiescencePort interface {
	AcquireBlockedQuiescence(context.Context, Plan) (BlockedQuiescenceManifest, BlockedQuiescenceFence, error)
}

// BlockedExclusionReleasePort is the controlled extension used by blocked
// scale-in termination. The call is made only after Acquire has returned an
// EXCLUSIVE fence and the core has checked the initial exact NOT_COMMITTED
// workload evidence. The returned manifest must be freshly captured after
// release; its closed ReleaseOutcome is evidence, not a forgeable safe bool.
type BlockedExclusionReleasePort interface {
	BlockedQuiescencePort
	ReleaseExclusionForBlockedTermination(context.Context, Plan, BlockedQuiescenceFence) (BlockedQuiescenceManifest, error)
}
