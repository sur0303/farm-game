// Package scaleplan contains the code-only orchestration core for imperative
// scale-out and scale-in plans. It has no Kubernetes, Redis, Route Manager,
// metrics, deployment, or runtime composition dependency.
package scaleplan

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
)

const (
	SchemaVersion                     = "scale-plan-v2"
	BlockedTerminationProtocolVersion = "blocked-termination-v1"
	BlockedTerminationReceiptVersion  = "blocked-termination-receipt-v1"
	MaxOpaqueIDBytes                  = 128
	MaxRealmBytes                     = 128
	MaxNamespaceBytes                 = 253
	MaxWorkloadNameBytes              = 253
	MaxEndpointBytes                  = 512
	MaxErrorCodeBytes                 = 128
	MaxMoves                          = 1 << 20
	MaxPlanTTL                        = 7 * 24 * time.Hour
	MaxRecoveryRetention              = 30 * 24 * time.Hour
	MaxRecoveryHorizon                = MaxPlanTTL + MaxRecoveryRetention
	MaxRoutePlanTTL                   = 24 * time.Hour

	// FunctionalMax* are retained at their original values for callers that
	// built legacy serial submissions with these names.  Schema v2 also admits
	// the production parallel pair below; the payload, rather than process
	// configuration, selects the execution semantics for a persisted plan.
	FunctionalMaxInflight      = uint32(1)
	FunctionalMaxShardsPerWave = uint32(1)

	// LegacyParallelMax* preserve persisted bounded-parallel plans.
	LegacyParallelMaxRoutePlansInflight  = uint32(8)
	LegacyParallelMaxShardsPerWave       = uint32(8)
	ParallelSixteenMaxRoutePlansInflight = uint32(16)
	ParallelSixteenMaxShardsPerWave      = uint32(16)
	LegacyTwentyMaxRoutePlansInflight    = uint32(20)
	LegacyTwentyMaxShardsPerWave         = uint32(20)

	ProductionMaxRoutePlansInflight = uint32(32)
	ProductionMaxShardsPerWave      = uint32(32)
)

var (
	ErrInvalidPlan         = errors.New("scaleplan: invalid plan")
	ErrInvalidTransition   = errors.New("scaleplan: invalid state transition")
	ErrPlanNotFound        = errors.New("scaleplan: plan not found")
	ErrRevisionConflict    = errors.New("scaleplan: revision conflict")
	ErrIdempotencyConflict = errors.New("scaleplan: idempotency conflict")
	ErrWorkloadBusy        = errors.New("scaleplan: workload already has an active plan")
	ErrCapacityExceeded    = errors.New("scaleplan: repository capacity exceeded")
	ErrCorruptRecord       = errors.New("scaleplan: corrupt repository record")
	ErrConflict            = errors.New("scaleplan: observed fact conflicts with frozen intent")
	ErrPending             = errors.New("scaleplan: dependency has not reached the required state")
	ErrPaused              = errors.New("scaleplan: plan is paused")
	ErrBlocked             = errors.New("scaleplan: plan is blocked and needs operator reconciliation")
	ErrVerification        = errors.New("scaleplan: child verification failed")
	ErrOutcomeUnknown      = errors.New("scaleplan: external outcome unknown")
	ErrDependency          = errors.New("scaleplan: dependency failed")
)

type Operation string

const (
	OperationScaleOut    Operation = "SCALE_OUT"
	OperationScaleIn     Operation = "SCALE_IN"
	OperationDrainHolder Operation = "DRAIN_HOLDER"
)

type ExecutionStatus string

const (
	StatusRunning   ExecutionStatus = "RUNNING"
	StatusPaused    ExecutionStatus = "PAUSED"
	StatusBlocked   ExecutionStatus = "BLOCKED_NEEDS_OPERATOR"
	StatusSucceeded ExecutionStatus = "SUCCEEDED"
	StatusFailed    ExecutionStatus = "FAILED"
	StatusCancelled ExecutionStatus = "CANCELLED"
)

func (s ExecutionStatus) Terminal() bool {
	return s == StatusSucceeded || s == StatusFailed || s == StatusCancelled
}

type Phase string

const (
	PhasePending               Phase = "PENDING"
	PhaseScalingUp             Phase = "SCALING_UP"
	PhaseWaitingHolderReady    Phase = "WAITING_HOLDER_READY"
	PhaseSelectingVictim       Phase = "SELECTING_VICTIM"
	PhaseExcludingVictim       Phase = "EXCLUDING_VICTIM"
	PhaseCapturingVictimShards Phase = "CAPTURING_VICTIM_SHARDS"
	PhaseMovingWave            Phase = "MOVING_WAVE"
	PhaseVerifyingWave         Phase = "VERIFYING_WAVE"
	PhaseWaitingZeroShard      Phase = "WAITING_ZERO_SHARD"
	PhaseScalingDown           Phase = "SCALING_DOWN"
	PhaseVerifyingFinal        Phase = "VERIFYING_FINAL"
	PhaseReleasingExclusion    Phase = "RELEASING_EXCLUSION"
)

type WorkloadRef struct {
	Realm     string
	Domain    actorrouting.RouteDomain
	Namespace string
	Kind      string
	Name      string
}

func (r WorkloadRef) Key() string {
	return strings.Join([]string{r.Realm, string(r.Domain), r.Namespace, r.Kind, r.Name}, "\x1f")
}

func (r WorkloadRef) Validate() error {
	if err := validateOpaque("realm", r.Realm, MaxRealmBytes); err != nil {
		return err
	}
	if r.Domain != actorrouting.DomainPlayer && r.Domain != actorrouting.DomainFarm {
		return fmt.Errorf("%w: workload domain", ErrInvalidPlan)
	}
	if err := validateOpaque("namespace", r.Namespace, MaxNamespaceBytes); err != nil {
		return err
	}
	if r.Kind != "StatefulSet" {
		return fmt.Errorf("%w: first version supports StatefulSet only", ErrInvalidPlan)
	}
	return validateOpaque("workload name", r.Name, MaxWorkloadNameBytes)
}

type FunctionalLimits struct {
	MaxRoutePlansInflight uint32
	MaxShardsPerWave      uint32
}

func (l FunctionalLimits) Validate() error {
	if l == (FunctionalLimits{MaxRoutePlansInflight: FunctionalMaxInflight, MaxShardsPerWave: FunctionalMaxShardsPerWave}) ||
		l == (FunctionalLimits{MaxRoutePlansInflight: LegacyParallelMaxRoutePlansInflight, MaxShardsPerWave: LegacyParallelMaxShardsPerWave}) ||
		l == (FunctionalLimits{MaxRoutePlansInflight: ParallelSixteenMaxRoutePlansInflight, MaxShardsPerWave: ParallelSixteenMaxShardsPerWave}) ||
		l == (FunctionalLimits{MaxRoutePlansInflight: LegacyTwentyMaxRoutePlansInflight, MaxShardsPerWave: LegacyTwentyMaxShardsPerWave}) ||
		l == (FunctionalLimits{MaxRoutePlansInflight: ProductionMaxRoutePlansInflight, MaxShardsPerWave: ProductionMaxShardsPerWave}) {
		return nil
	}
	return fmt.Errorf("%w: functional limits must be exactly 1/1, 8/8, 16/16, 20/20, or 32/32", ErrInvalidPlan)
}

func (l FunctionalLimits) LegacySerial() bool {
	return l.MaxRoutePlansInflight == FunctionalMaxInflight && l.MaxShardsPerWave == FunctionalMaxShardsPerWave
}

func (l FunctionalLimits) Parallel() bool {
	return l == (FunctionalLimits{MaxRoutePlansInflight: LegacyParallelMaxRoutePlansInflight, MaxShardsPerWave: LegacyParallelMaxShardsPerWave}) ||
		l == (FunctionalLimits{MaxRoutePlansInflight: ParallelSixteenMaxRoutePlansInflight, MaxShardsPerWave: ParallelSixteenMaxShardsPerWave}) ||
		l == (FunctionalLimits{MaxRoutePlansInflight: LegacyTwentyMaxRoutePlansInflight, MaxShardsPerWave: LegacyTwentyMaxShardsPerWave}) ||
		l == (FunctionalLimits{MaxRoutePlansInflight: ProductionMaxRoutePlansInflight, MaxShardsPerWave: ProductionMaxShardsPerWave})
}

func LegacyParallelFunctionalLimits() FunctionalLimits {
	return FunctionalLimits{MaxRoutePlansInflight: LegacyParallelMaxRoutePlansInflight, MaxShardsPerWave: LegacyParallelMaxShardsPerWave}
}

func ProductionFunctionalLimits() FunctionalLimits {
	return FunctionalLimits{MaxRoutePlansInflight: ProductionMaxRoutePlansInflight, MaxShardsPerWave: ProductionMaxShardsPerWave}
}

// FunctionalLimitsForParallelism maps the operator-facing concurrency profile
// to the exact pair persisted in every ScalePlan. Persisting the pair keeps
// recovery deterministic even if a later controller starts with a different
// default. The 20/20 pair remains decode-compatible for older A9 experiments,
// but new runtime configuration deliberately exposes only 1, 8, 16 and 32.
func FunctionalLimitsForParallelism(parallelism uint32) (FunctionalLimits, error) {
	switch parallelism {
	case FunctionalMaxInflight, LegacyParallelMaxRoutePlansInflight,
		ParallelSixteenMaxRoutePlansInflight, ProductionMaxRoutePlansInflight:
		return FunctionalLimits{MaxRoutePlansInflight: parallelism, MaxShardsPerWave: parallelism}, nil
	default:
		return FunctionalLimits{}, fmt.Errorf("%w: parallelism must be one of 1, 8, 16, or 32", ErrInvalidPlan)
	}
}

type ShardMoveIntent struct {
	Domain         actorrouting.RouteDomain
	VirtualShard   uint32
	SourceHolderID string
	TargetHolderID string
	TargetOrdinal  int32
}

func (m ShardMoveIntent) Validate(domain actorrouting.RouteDomain) error {
	if m.Domain != domain {
		return fmt.Errorf("%w: move domain mismatch", ErrInvalidPlan)
	}
	if err := validateOpaque("source holder id", m.SourceHolderID, MaxOpaqueIDBytes); err != nil {
		return err
	}
	if err := validateOpaque("target holder id", m.TargetHolderID, MaxOpaqueIDBytes); err != nil {
		return err
	}
	if m.SourceHolderID == m.TargetHolderID || m.TargetOrdinal < 0 {
		return fmt.Errorf("%w: invalid move endpoints", ErrInvalidPlan)
	}
	return nil
}

type Submission struct {
	IdempotencyKey string
	Realm          string
	Domain         actorrouting.RouteDomain
	Operation      Operation
	Workload       WorkloadRef
	// VictimHolderID binds an internal DRAIN_HOLDER operation even when the
	// holder is already READY_ZERO_SHARD and therefore has no move manifest.
	// Older persisted drain records may omit it when Moves is non-empty; in
	// that case the first move remains the compatibility binding.
	VictimHolderID string
	// PinnedTarget freezes one exact replacement Pod generation for an
	// internal DRAIN_HOLDER. Every Shard in the manifest must target this
	// HolderRef. A target restart therefore fails closed instead of allowing
	// later Shards to resolve to a different Pod generation.
	PinnedTarget        HolderRef
	ExpectedReplicas    int32
	DesiredReplicas     int32
	Moves               []ShardMoveIntent
	RoutePlanTTLms      int64
	PlanTTLms           int64
	RecoveryRetentionMs int64
	Limits              FunctionalLimits
}

func (s Submission) Clone() Submission {
	s.Moves = append([]ShardMoveIntent(nil), s.Moves...)
	return s
}

// Normalize validates and sorts the explicit manifest. Selection and balancing
// policy deliberately remain outside this package.
func (s Submission) Normalize() (Submission, error) {
	s = s.Clone()
	if err := validateOpaque("idempotency key", s.IdempotencyKey, MaxOpaqueIDBytes); err != nil {
		return Submission{}, err
	}
	if err := validateOpaque("realm", s.Realm, MaxRealmBytes); err != nil {
		return Submission{}, err
	}
	if s.Workload.Realm != s.Realm || s.Workload.Domain != s.Domain {
		return Submission{}, fmt.Errorf("%w: workload scope mismatch", ErrInvalidPlan)
	}
	if err := s.Workload.Validate(); err != nil {
		return Submission{}, err
	}
	if s.ExpectedReplicas < 1 || s.DesiredReplicas < 1 {
		return Submission{}, fmt.Errorf("%w: replicas must be positive", ErrInvalidPlan)
	}
	switch s.Operation {
	case OperationScaleOut:
		if s.DesiredReplicas != s.ExpectedReplicas+1 {
			return Submission{}, fmt.Errorf("%w: first scale-out version permits +1 only", ErrInvalidPlan)
		}
	case OperationScaleIn:
		if s.ExpectedReplicas != s.DesiredReplicas+1 {
			return Submission{}, fmt.Errorf("%w: first scale-in version permits -1 only", ErrInvalidPlan)
		}
	case OperationDrainHolder:
		if s.ExpectedReplicas != s.DesiredReplicas {
			return Submission{}, fmt.Errorf("%w: holder drain must preserve replicas", ErrInvalidPlan)
		}
		if s.VictimHolderID == "" && len(s.Moves) == 0 {
			return Submission{}, fmt.Errorf("%w: holder drain victim required", ErrInvalidPlan)
		}
		if s.VictimHolderID != "" {
			if err := validateOpaque("victim holder id", s.VictimHolderID, MaxOpaqueIDBytes); err != nil {
				return Submission{}, err
			}
		}
		if s.PinnedTarget != (HolderRef{}) {
			if err := s.PinnedTarget.ValidateExact(); err != nil || s.PinnedTarget.Domain != s.Domain ||
				s.PinnedTarget.HolderID == s.VictimHolderID || s.PinnedTarget.Ordinal >= s.DesiredReplicas {
				return Submission{}, fmt.Errorf("%w: pinned drain target", ErrInvalidPlan)
			}
		}
	default:
		return Submission{}, fmt.Errorf("%w: operation", ErrInvalidPlan)
	}
	if s.Operation != OperationDrainHolder && (s.VictimHolderID != "" || s.PinnedTarget != (HolderRef{})) {
		return Submission{}, fmt.Errorf("%w: drain bindings only valid for holder drain", ErrInvalidPlan)
	}
	if len(s.Moves) > MaxMoves {
		return Submission{}, fmt.Errorf("%w: move manifest exceeds limit", ErrInvalidPlan)
	}
	// A scale-out must always move at least one Shard onto the new holder. A
	// scale-in or an explicitly-bound holder drain may legitimately have an
	// empty manifest when the victim
	// StatefulSet ordinal has already reached READY_ZERO_SHARD. The latter is
	// still executed through the durable exclusion/fence/scale state machine;
	// it is not a direct Kubernetes replica decrement.
	if len(s.Moves) == 0 && s.Operation != OperationScaleIn && s.Operation != OperationDrainHolder {
		return Submission{}, fmt.Errorf("%w: move manifest required", ErrInvalidPlan)
	}
	for _, move := range s.Moves {
		if err := move.Validate(s.Domain); err != nil {
			return Submission{}, err
		}
	}
	sort.Slice(s.Moves, func(i, j int) bool {
		if s.Moves[i].Domain != s.Moves[j].Domain {
			return s.Moves[i].Domain < s.Moves[j].Domain
		}
		return s.Moves[i].VirtualShard < s.Moves[j].VirtualShard
	})
	for i := 1; i < len(s.Moves); i++ {
		if s.Moves[i-1].Domain == s.Moves[i].Domain && s.Moves[i-1].VirtualShard == s.Moves[i].VirtualShard {
			return Submission{}, fmt.Errorf("%w: duplicate shard in manifest", ErrInvalidPlan)
		}
	}
	if s.Operation == OperationScaleOut {
		targetID, targetOrdinal := s.Moves[0].TargetHolderID, s.DesiredReplicas-1
		for _, move := range s.Moves {
			if move.TargetHolderID != targetID || move.TargetOrdinal != targetOrdinal {
				return Submission{}, fmt.Errorf("%w: scale-out manifest must target the one new ordinal", ErrInvalidPlan)
			}
		}
	} else if len(s.Moves) > 0 {
		sourceID := s.Moves[0].SourceHolderID
		if s.Operation == OperationDrainHolder && s.VictimHolderID != "" && s.VictimHolderID != sourceID {
			return Submission{}, fmt.Errorf("%w: holder drain victim does not match manifest", ErrInvalidPlan)
		}
		for _, move := range s.Moves {
			if move.SourceHolderID != sourceID {
				return Submission{}, fmt.Errorf("%w: victim manifest must drain one source holder", ErrInvalidPlan)
			}
			if move.TargetOrdinal >= s.DesiredReplicas {
				return Submission{}, fmt.Errorf("%w: scale-in target ordinal must remain after scale-down", ErrInvalidPlan)
			}
			if s.PinnedTarget != (HolderRef{}) &&
				(move.TargetHolderID != s.PinnedTarget.HolderID || move.TargetOrdinal != s.PinnedTarget.Ordinal) {
				return Submission{}, fmt.Errorf("%w: drain manifest escaped pinned target", ErrInvalidPlan)
			}
		}
	}
	if s.RoutePlanTTLms <= 0 || s.RoutePlanTTLms > MaxRoutePlanTTL.Milliseconds() {
		return Submission{}, fmt.Errorf("%w: route plan ttl", ErrInvalidPlan)
	}
	if s.PlanTTLms <= 0 || s.PlanTTLms > MaxPlanTTL.Milliseconds() {
		return Submission{}, fmt.Errorf("%w: scale plan start ttl", ErrInvalidPlan)
	}
	if s.RecoveryRetentionMs <= 0 || s.RecoveryRetentionMs > MaxRecoveryRetention.Milliseconds() {
		return Submission{}, fmt.Errorf("%w: recovery retention", ErrInvalidPlan)
	}
	if s.PlanTTLms > MaxRecoveryHorizon.Milliseconds()-s.RecoveryRetentionMs {
		return Submission{}, fmt.Errorf("%w: recovery horizon", ErrInvalidPlan)
	}
	if err := s.Limits.Validate(); err != nil {
		return Submission{}, err
	}
	return s, nil
}

type WorkloadIdentity struct {
	UID             string
	ResourceVersion string
}

type HolderRef struct {
	Domain      actorrouting.RouteDomain
	HolderID    string
	BootID      string
	PodUID      string
	WorkloadUID string
	Ordinal     int32
}

func (r HolderRef) Exact() bool { return r.ValidateExact() == nil }

func (r HolderRef) ValidateExact() error {
	if r.Domain != actorrouting.DomainPlayer && r.Domain != actorrouting.DomainFarm {
		return fmt.Errorf("%w: holder domain", ErrInvalidPlan)
	}
	for name, value := range map[string]string{"holder id": r.HolderID, "boot id": r.BootID,
		"pod uid": r.PodUID, "workload uid": r.WorkloadUID} {
		if err := validateVisibleASCII(name, value, MaxOpaqueIDBytes); err != nil {
			return err
		}
	}
	if r.Ordinal < 0 {
		return fmt.Errorf("%w: holder ordinal", ErrInvalidPlan)
	}
	return nil
}

type EffectState string

const (
	EffectPending   EffectState = "PENDING"
	EffectCommitted EffectState = "COMMITTED"
	EffectAdopted   EffectState = "ADOPTED_COMMITTED"
)

func validEffectState(value EffectState) bool {
	return value == EffectPending || value == EffectCommitted || value == EffectAdopted
}

type ScaleIntent struct {
	WorkloadUID             string
	ExpectedResourceVersion string
	ExpectedReplicas        int32
	DesiredReplicas         int32
	Effect                  EffectState
}

type ExclusionIntent struct {
	Victim           HolderRef
	ExpectedRevision uint64
	ObservedRevision uint64
	OwnerPlanID      string
	Effect           EffectState
}

// ScaleResolution is persisted before an exclusion release is attempted.
// Pending means the exact workload capture has not yet classified /scale.
type ScaleResolution string

const (
	ScaleResolutionPending      ScaleResolution = "PENDING"
	ScaleResolutionNotCommitted ScaleResolution = "NOT_COMMITTED"
	ScaleResolutionCommitted    ScaleResolution = "COMMITTED"
)

func validScaleResolution(value ScaleResolution) bool {
	return value == ScaleResolutionPending || value == ScaleResolutionNotCommitted ||
		value == ScaleResolutionCommitted
}

// CleanupIntent makes an exclusion release a restartable state-machine
// operation rather than an in-process compensation callback.
type CleanupIntent struct {
	FailureCode     string
	ScaleResolution ScaleResolution
	ReleaseOutcome  PlacementReleaseOutcome
}

type ResolvedMove struct {
	Intent              ShardMoveIntent
	WaveIndex           uint32
	Source              HolderRef
	SourceEndpoint      string
	Target              HolderRef
	TargetEndpoint      string
	ExpectedSourceFence authoritystore.ShardWriteFence
	ChildPlanID         string
	ChildTransitionID   string
	ChildPayloadDigest  string
	ChildPhase          string
	EnsureObserved      bool
	Verified            bool
	Reconciled          bool
}

func (m ResolvedMove) Frozen() bool {
	return m.Source.Exact() && m.Target.Exact() && m.SourceEndpoint != "" && m.TargetEndpoint != "" &&
		m.ExpectedSourceFence.ExpectedHolderID != "" && m.ChildPlanID != "" &&
		m.ChildTransitionID != "" && m.ChildPayloadDigest != ""
}

type BlockedTerminationReceipt struct {
	ProtocolVersion            string
	TerminationID              string
	ExpectedBlockedRevision    uint64
	ExpectedBlockedReason      string
	ExpectedBlockedUpdatedAtMs int64
	ExpectedPayloadDigest      string
	ExpectedCheckpointDigest   string
	ProofDigest                string
	ProvedAtMs                 int64
	ProofExpiresAtMs           int64
	EvidenceRetainUntilMs      int64
	CommittedAtMs              int64
	IntentCount                uint32
	ChildCount                 uint32
	ResultCode                 string
}

type Plan struct {
	SchemaVersion  string
	PlanID         string
	IdempotencyKey string
	PayloadDigest  string
	Submission     Submission

	Status    ExecutionStatus
	Phase     Phase
	WaveIndex uint32
	Attempt   uint64
	Revision  uint64

	WorkloadIdentity WorkloadIdentity
	Target           HolderRef
	Victim           HolderRef
	ScaleIntent      *ScaleIntent
	ExclusionIntent  *ExclusionIntent
	CleanupIntent    *CleanupIntent
	ResolvedMoves    []ResolvedMove

	PausedReason            string
	BlockedReason           string
	ErrorCode               string
	CreatedAtMs             int64
	UpdatedAtMs             int64
	CompletedAtMs           int64
	StartDeadlineMs         int64
	RecoveryRetainUntilMs   int64
	BlockedTermination      *BlockedTerminationReceipt
	BlockedTerminationProof *BlockedQuiescenceProof
}

func (p Plan) Clone() Plan {
	p.Submission = p.Submission.Clone()
	p.ResolvedMoves = append([]ResolvedMove(nil), p.ResolvedMoves...)
	if p.ScaleIntent != nil {
		value := *p.ScaleIntent
		p.ScaleIntent = &value
	}
	if p.ExclusionIntent != nil {
		value := *p.ExclusionIntent
		p.ExclusionIntent = &value
	}
	if p.CleanupIntent != nil {
		value := *p.CleanupIntent
		p.CleanupIntent = &value
	}
	if p.BlockedTermination != nil {
		value := *p.BlockedTermination
		p.BlockedTermination = &value
	}
	if p.BlockedTerminationProof != nil {
		value := *p.BlockedTerminationProof
		p.BlockedTerminationProof = &value
	}
	return p
}

// HasExternalIntent is the durable point after which a start deadline may no
// longer terminalize the Saga. A persisted write-ahead intent means the
// external effect might already have committed even when no response exists.
func (p Plan) HasExternalIntent() bool {
	return p.ScaleIntent != nil || p.ExclusionIntent != nil || p.CleanupIntent != nil || len(p.ResolvedMoves) != 0
}

func (p Plan) Validate() error {
	if p.SchemaVersion != SchemaVersion {
		return fmt.Errorf("%w: schema", ErrInvalidPlan)
	}
	if err := validateOpaque("plan id", p.PlanID, MaxOpaqueIDBytes); err != nil {
		return err
	}
	if err := validateOpaque("idempotency key", p.IdempotencyKey, MaxOpaqueIDBytes); err != nil {
		return err
	}
	if len(p.PayloadDigest) != 64 {
		return fmt.Errorf("%w: payload digest", ErrInvalidPlan)
	}
	normalized, err := p.Submission.Normalize()
	if err != nil || !reflect.DeepEqual(normalized, p.Submission) {
		return fmt.Errorf("%w: non-canonical submission", ErrInvalidPlan)
	}
	expectedDigest, digestErr := SubmissionPayloadDigest(normalized)
	expectedID, idErr := DeterministicPlanID(normalized)
	if digestErr != nil || idErr != nil || p.PayloadDigest != expectedDigest || p.PlanID != expectedID {
		return fmt.Errorf("%w: deterministic identity binding", ErrInvalidPlan)
	}
	if p.IdempotencyKey != p.Submission.IdempotencyKey {
		return fmt.Errorf("%w: idempotency binding", ErrInvalidPlan)
	}
	if !validStatus(p.Status) || !validPhaseForOperation(p.Submission.Operation, p.Phase) {
		return fmt.Errorf("%w: status/phase", ErrInvalidPlan)
	}
	if p.Revision == 0 || p.CreatedAtMs <= 0 || p.UpdatedAtMs < p.CreatedAtMs ||
		p.StartDeadlineMs <= p.CreatedAtMs || p.RecoveryRetainUntilMs <= p.StartDeadlineMs {
		return fmt.Errorf("%w: revision/timestamps", ErrInvalidPlan)
	}
	if p.Status.Terminal() {
		if p.CompletedAtMs != p.UpdatedAtMs || p.CompletedAtMs == 0 {
			return fmt.Errorf("%w: terminal timestamp", ErrInvalidPlan)
		}
		if p.Status == StatusSucceeded && p.Phase != PhaseVerifyingFinal {
			return fmt.Errorf("%w: succeeded before final verification", ErrInvalidPlan)
		}
	} else if p.CompletedAtMs != 0 {
		return fmt.Errorf("%w: nonterminal completion", ErrInvalidPlan)
	}
	if p.Status == StatusPaused && p.PausedReason == "" {
		return fmt.Errorf("%w: paused reason", ErrInvalidPlan)
	}
	if p.Status != StatusPaused && p.PausedReason != "" {
		return fmt.Errorf("%w: stale paused reason", ErrInvalidPlan)
	}
	if p.PausedReason != "" {
		if err := validateOpaque("paused reason", p.PausedReason, MaxErrorCodeBytes); err != nil {
			return err
		}
	}
	if p.Status == StatusBlocked && p.BlockedReason == "" {
		return fmt.Errorf("%w: blocked reason", ErrInvalidPlan)
	}
	if p.Status != StatusBlocked && p.BlockedReason != "" {
		return fmt.Errorf("%w: stale blocked reason", ErrInvalidPlan)
	}
	if p.BlockedReason != "" {
		if err := validateOpaque("blocked reason", p.BlockedReason, MaxErrorCodeBytes); err != nil {
			return err
		}
	}
	if (p.Status == StatusFailed || p.Status == StatusCancelled) && p.ErrorCode == "" {
		return fmt.Errorf("%w: terminal error code", ErrInvalidPlan)
	}
	if p.Status != StatusFailed && p.Status != StatusCancelled && p.ErrorCode != "" {
		return fmt.Errorf("%w: stale error code", ErrInvalidPlan)
	}
	if p.ErrorCode != "" {
		if err := validateOpaque("error code", p.ErrorCode, MaxErrorCodeBytes); err != nil {
			return err
		}
	}
	if p.BlockedTermination != nil {
		receipt := p.BlockedTermination
		if p.BlockedTerminationProof == nil {
			return fmt.Errorf("%w: blocked termination proof missing", ErrInvalidPlan)
		}
		proof := p.BlockedTerminationProof
		proofDigest, proofErr := BlockedQuiescenceProofDigest(*proof)
		if p.Status != StatusFailed || p.ErrorCode != "OPERATOR_SAFE_TERMINATION" || proofErr != nil || proofDigest != receipt.ProofDigest ||
			proof.PlanID != p.PlanID || proof.ExpectedRevision != receipt.ExpectedBlockedRevision ||
			proof.ExpectedBlockedReason != receipt.ExpectedBlockedReason || proof.ExpectedPayloadDigest != p.PayloadDigest ||
			proof.ExpectedCheckpointDigest != receipt.ExpectedCheckpointDigest || proof.CapturedAtMs != receipt.ProvedAtMs ||
			proof.ExpiresAtMs != receipt.ProofExpiresAtMs || proof.EvidenceRetainUntilMs != receipt.EvidenceRetainUntilMs ||
			proof.IntentCount != receipt.IntentCount || proof.ChildCount != receipt.ChildCount ||
			receipt.ProtocolVersion != BlockedTerminationReceiptVersion || receipt.TerminationID == "" ||
			receipt.ExpectedBlockedRevision == 0 || receipt.ExpectedBlockedRevision+1 != p.Revision ||
			receipt.ExpectedBlockedReason == "" || receipt.ExpectedBlockedUpdatedAtMs < p.CreatedAtMs ||
			receipt.ExpectedBlockedUpdatedAtMs > receipt.ProvedAtMs || receipt.ExpectedPayloadDigest != p.PayloadDigest ||
			!canonicalSHA256(receipt.ExpectedCheckpointDigest) || !canonicalSHA256(receipt.ProofDigest) ||
			receipt.ProvedAtMs <= 0 || receipt.ProofExpiresAtMs <= receipt.ProvedAtMs ||
			receipt.EvidenceRetainUntilMs < p.RecoveryRetainUntilMs || receipt.EvidenceRetainUntilMs < receipt.ProofExpiresAtMs ||
			receipt.CommittedAtMs != p.CompletedAtMs || receipt.CommittedAtMs < receipt.ProvedAtMs ||
			receipt.CommittedAtMs >= receipt.ProofExpiresAtMs || receipt.IntentCount != blockedTerminationIntentCount(p) ||
			receipt.ChildCount != uint32(len(p.ResolvedMoves)) || receipt.ResultCode != "OPERATOR_SAFE_TERMINATION" {
			return fmt.Errorf("%w: blocked termination receipt", ErrInvalidPlan)
		}
		if p.ExclusionIntent != nil {
			if proof.ScaleResolution == ScaleResolutionNotCommitted &&
				!proof.ExclusionReleaseOutcome.TerminalForScalePlan() {
				return fmt.Errorf("%w: safe termination release evidence", ErrInvalidPlan)
			}
			if proof.ScaleResolution != ScaleResolutionNotCommitted && proof.ScaleResolution != ScaleResolutionCommitted {
				return fmt.Errorf("%w: safe termination scale evidence", ErrInvalidPlan)
			}
		}
		blocked := p.Clone()
		blocked.Status, blocked.BlockedReason, blocked.ErrorCode = StatusBlocked, receipt.ExpectedBlockedReason, ""
		blocked.Revision, blocked.UpdatedAtMs, blocked.CompletedAtMs = receipt.ExpectedBlockedRevision, receipt.ExpectedBlockedUpdatedAtMs, 0
		blocked.BlockedTermination, blocked.BlockedTerminationProof = nil, nil
		checkpoint, checkpointErr := BlockedCheckpointDigest(blocked)
		if checkpointErr != nil || checkpoint != receipt.ExpectedCheckpointDigest {
			return fmt.Errorf("%w: retained blocked checkpoint", ErrInvalidPlan)
		}
		for name, value := range map[string]string{"termination id": receipt.TerminationID,
			"expected blocked reason": receipt.ExpectedBlockedReason, "result code": receipt.ResultCode} {
			if err := validateOpaque(name, value, MaxOpaqueIDBytes); err != nil {
				return err
			}
		}
	} else if p.ErrorCode == "OPERATOR_SAFE_TERMINATION" || p.BlockedTerminationProof != nil {
		return fmt.Errorf("%w: safe termination evidence missing", ErrInvalidPlan)
	}
	if err := p.validateSemanticBindings(); err != nil {
		return err
	}
	if len(p.ResolvedMoves) > len(p.Submission.Moves) {
		return fmt.Errorf("%w: move bounds", ErrInvalidPlan)
	}
	if len(p.Submission.Moves) == 0 {
		if (p.Submission.Operation != OperationScaleIn && p.Submission.Operation != OperationDrainHolder) ||
			p.WaveIndex != 0 || len(p.ResolvedMoves) != 0 ||
			p.Phase == PhaseMovingWave || p.Phase == PhaseVerifyingWave {
			return fmt.Errorf("%w: scale-only wave bounds", ErrInvalidPlan)
		}
	} else {
		// MOVING_WAVE and VERIFYING_WAVE use WaveIndex as the first move in
		// the current bounded wave.  A completed parallel final wave records
		// the exclusive end index; old serial records used the final move's
		// index, so that legacy checkpoint remains decodable below.
		if p.Phase == PhaseMovingWave || p.Phase == PhaseVerifyingWave {
			if int(p.WaveIndex) >= len(p.Submission.Moves) {
				return fmt.Errorf("%w: active wave bounds", ErrInvalidPlan)
			}
		} else if int(p.WaveIndex) > len(p.Submission.Moves) {
			return fmt.Errorf("%w: wave bounds", ErrInvalidPlan)
		}
	}
	if (p.Phase == PhaseMovingWave || p.Phase == PhaseVerifyingWave) && int(p.WaveIndex) > 0 {
		for index := 0; index < int(p.WaveIndex); index++ {
			if index >= len(p.ResolvedMoves) || !p.ResolvedMoves[index].Verified {
				return fmt.Errorf("%w: prior wave child %d is not verified", ErrInvalidPlan, index)
			}
		}
	}
	switch p.Phase {
	case PhaseMovingWave:
		end := waveEnd(p)
		if len(p.ResolvedMoves) < int(p.WaveIndex) || len(p.ResolvedMoves) > end {
			return fmt.Errorf("%w: moving wave checkpoint", ErrInvalidPlan)
		}
		for index := int(p.WaveIndex); index < len(p.ResolvedMoves); index++ {
			move := p.ResolvedMoves[index]
			verifiedSlidingChild := p.Submission.Limits.Parallel() && move.EnsureObserved &&
				move.ChildPhase == RoutePlanPhaseSucceeded && !move.Reconciled
			if move.Verified && !verifiedSlidingChild && !(move.Reconciled && move.ChildPhase == RoutePlanPhasePaused) {
				return fmt.Errorf("%w: moving wave has verified child %d", ErrInvalidPlan, index)
			}
		}
	case PhaseVerifyingWave:
		if len(p.ResolvedMoves) != waveEnd(p) {
			return fmt.Errorf("%w: verifying wave checkpoint", ErrInvalidPlan)
		}
		for index := int(p.WaveIndex); index < len(p.ResolvedMoves); index++ {
			move := p.ResolvedMoves[index]
			if !move.EnsureObserved ||
				(move.ChildPhase != RoutePlanPhaseSucceeded && !(move.ChildPhase == RoutePlanPhasePaused && move.Verified && move.Reconciled)) {
				return fmt.Errorf("%w: verifying wave child %d is not terminally observed", ErrInvalidPlan, index)
			}
		}
	case PhaseWaitingZeroShard, PhaseScalingDown, PhaseVerifyingFinal:
		if len(p.Submission.Moves) > 0 {
			if p.Submission.Limits.Parallel() && p.WaveIndex != uint32(len(p.Submission.Moves)) {
				return fmt.Errorf("%w: final parallel wave index", ErrInvalidPlan)
			}
			if p.Submission.Limits.LegacySerial() && p.WaveIndex != uint32(len(p.Submission.Moves)-1) &&
				p.WaveIndex != uint32(len(p.Submission.Moves)) {
				return fmt.Errorf("%w: final serial wave index", ErrInvalidPlan)
			}
		}
		if len(p.ResolvedMoves) != len(p.Submission.Moves) {
			return fmt.Errorf("%w: incomplete manifest at final phase", ErrInvalidPlan)
		}
		for _, move := range p.ResolvedMoves {
			if !move.Verified {
				return fmt.Errorf("%w: unverified child at final phase", ErrInvalidPlan)
			}
		}
	}
	for i, move := range p.ResolvedMoves {
		if i >= len(p.Submission.Moves) || move.Intent != p.Submission.Moves[i] || move.WaveIndex != uint32(i) || !move.Frozen() {
			return fmt.Errorf("%w: resolved move %d", ErrInvalidPlan, i)
		}
		if err := validateOpaque("source endpoint", move.SourceEndpoint, MaxEndpointBytes); err != nil {
			return err
		}
		if err := validateOpaque("target endpoint", move.TargetEndpoint, MaxEndpointBytes); err != nil {
			return err
		}
		if move.EnsureObserved != (move.ChildPhase != "") ||
			(move.EnsureObserved && !validRoutePlanObservedPhase(move.ChildPhase)) {
			return fmt.Errorf("%w: child observation checkpoint %d", ErrInvalidPlan, i)
		}
		if move.Verified && (!move.EnsureObserved ||
			(move.ChildPhase != RoutePlanPhaseSucceeded && !(move.ChildPhase == RoutePlanPhasePaused && move.Reconciled))) {
			return fmt.Errorf("%w: verified child %d has no accepted outcome", ErrInvalidPlan, i)
		}
		if move.Reconciled && (!move.Verified || move.ChildPhase != RoutePlanPhasePaused) {
			return fmt.Errorf("%w: invalid reconciled child %d", ErrInvalidPlan, i)
		}
		if usesVictimExclusion(p.Submission.Operation) && move.Source != p.Victim {
			return fmt.Errorf("%w: scale-in child source differs from victim", ErrInvalidPlan)
		}
		if p.Submission.Operation == OperationScaleOut && move.Target != p.Target {
			return fmt.Errorf("%w: scale-out child target differs from frozen target", ErrInvalidPlan)
		}
		if err := validateResolvedMoveIdentity(p, i, move); err != nil {
			return err
		}
	}
	return nil
}

func waveEnd(p Plan) int {
	start := int(p.WaveIndex)
	if start < 0 || start > len(p.Submission.Moves) ||
		(!p.Submission.Limits.LegacySerial() && !p.Submission.Limits.Parallel()) {
		return start
	}
	// Parallel plans use MaxRoutePlansInflight as a sliding execution window,
	// not MaxShardsPerWave as a fixed completion barrier. Freeze the complete
	// intent set once; reconcileWaveChildren admits at most the configured
	// number of non-terminal child plans and backfills a slot immediately when
	// any shard completes.
	if p.Submission.Limits.Parallel() {
		return len(p.Submission.Moves)
	}
	limit := int(p.Submission.Limits.MaxShardsPerWave)
	if limit <= 0 {
		return start
	}
	end := start + limit
	if end > len(p.Submission.Moves) {
		end = len(p.Submission.Moves)
	}
	return end
}

func (p Plan) validateSemanticBindings() error {
	workloadSet := p.WorkloadIdentity != (WorkloadIdentity{})
	if workloadSet && (p.WorkloadIdentity.UID == "" || p.WorkloadIdentity.ResourceVersion == "") {
		return fmt.Errorf("%w: partial workload identity", ErrInvalidPlan)
	}
	if p.ScaleIntent != nil {
		if !validEffectState(p.ScaleIntent.Effect) || !workloadSet || p.ScaleIntent.WorkloadUID != p.WorkloadIdentity.UID ||
			p.ScaleIntent.ExpectedResourceVersion != p.WorkloadIdentity.ResourceVersion ||
			p.ScaleIntent.ExpectedReplicas != p.Submission.ExpectedReplicas ||
			p.ScaleIntent.DesiredReplicas != p.Submission.DesiredReplicas {
			return fmt.Errorf("%w: scale intent binding", ErrInvalidPlan)
		}
	}
	if p.ExclusionIntent != nil {
		if !validEffectState(p.ExclusionIntent.Effect) || !usesVictimExclusion(p.Submission.Operation) ||
			!p.Victim.Exact() || p.ExclusionIntent.Victim != p.Victim ||
			p.ExclusionIntent.OwnerPlanID != p.PlanID || p.ExclusionIntent.ExpectedRevision == 0 ||
			(p.ExclusionIntent.Effect == EffectPending && p.ExclusionIntent.ObservedRevision != 0) ||
			(p.ExclusionIntent.Effect != EffectPending && p.ExclusionIntent.ObservedRevision <= p.ExclusionIntent.ExpectedRevision) {
			return fmt.Errorf("%w: exclusion intent binding", ErrInvalidPlan)
		}
	}
	if err := p.validateCleanupIntent(); err != nil {
		return err
	}
	switch p.Submission.Operation {
	case OperationScaleOut:
		if p.Phase == PhasePending && (workloadSet || p.ScaleIntent != nil || p.Target != (HolderRef{}) ||
			p.Victim != (HolderRef{}) || p.ExclusionIntent != nil || len(p.ResolvedMoves) != 0) {
			return fmt.Errorf("%w: pending scale-out contains durable facts", ErrInvalidPlan)
		}
		if p.Phase != PhasePending && (!workloadSet || p.ScaleIntent == nil) {
			return fmt.Errorf("%w: scale-out phase lacks workload/scale intent", ErrInvalidPlan)
		}
		if p.Phase == PhaseScalingUp && p.ScaleIntent.Effect != EffectPending {
			return fmt.Errorf("%w: scale-out effect committed before scaling phase completes", ErrInvalidPlan)
		}
		if (p.Phase == PhaseWaitingHolderReady || p.Phase == PhaseMovingWave || p.Phase == PhaseVerifyingWave ||
			p.Phase == PhaseVerifyingFinal) && p.ScaleIntent.Effect == EffectPending {
			return fmt.Errorf("%w: scale-out effect remains pending after scaling phase", ErrInvalidPlan)
		}
		if p.Phase == PhaseMovingWave || p.Phase == PhaseVerifyingWave || p.Phase == PhaseVerifyingFinal {
			if !p.Target.Exact() || p.Target.WorkloadUID != p.WorkloadIdentity.UID {
				return fmt.Errorf("%w: scale-out target binding", ErrInvalidPlan)
			}
		}
		if p.ExclusionIntent != nil || p.Victim != (HolderRef{}) {
			return fmt.Errorf("%w: scale-out contains victim exclusion", ErrInvalidPlan)
		}
	case OperationScaleIn, OperationDrainHolder:
		if (p.Phase == PhasePending || p.Phase == PhaseSelectingVictim) &&
			(workloadSet || p.ScaleIntent != nil || p.ExclusionIntent != nil || p.Target != (HolderRef{}) ||
				p.Victim != (HolderRef{}) || len(p.ResolvedMoves) != 0) {
			return fmt.Errorf("%w: pre-selection scale-in contains durable facts", ErrInvalidPlan)
		}
		if p.Phase == PhaseExcludingVictim || p.Phase == PhaseCapturingVictimShards || p.Phase == PhaseMovingWave ||
			p.Phase == PhaseVerifyingWave || p.Phase == PhaseWaitingZeroShard || p.Phase == PhaseScalingDown ||
			p.Phase == PhaseVerifyingFinal || p.Phase == PhaseReleasingExclusion {
			if !workloadSet || !p.Victim.Exact() || p.Victim.WorkloadUID != p.WorkloadIdentity.UID || p.ExclusionIntent == nil {
				return fmt.Errorf("%w: scale-in victim/exclusion binding", ErrInvalidPlan)
			}
		}
		if p.Phase == PhaseExcludingVictim && p.ExclusionIntent.Effect != EffectPending {
			return fmt.Errorf("%w: exclusion effect committed before excluding phase completes", ErrInvalidPlan)
		}
		if (p.Phase == PhaseCapturingVictimShards || p.Phase == PhaseMovingWave || p.Phase == PhaseVerifyingWave ||
			p.Phase == PhaseWaitingZeroShard || p.Phase == PhaseScalingDown || p.Phase == PhaseVerifyingFinal) &&
			p.ExclusionIntent.Effect == EffectPending {
			return fmt.Errorf("%w: exclusion effect remains pending after exclusion phase", ErrInvalidPlan)
		}
		if p.Submission.Operation == OperationScaleIn &&
			(p.Phase == PhaseScalingDown || p.Phase == PhaseVerifyingFinal) && p.ScaleIntent == nil {
			return fmt.Errorf("%w: scale-in final phase lacks scale intent", ErrInvalidPlan)
		}
		if p.Phase == PhaseScalingDown && p.ScaleIntent != nil && p.ScaleIntent.Effect != EffectPending {
			return fmt.Errorf("%w: scale-down effect committed before scaling phase completes", ErrInvalidPlan)
		}
		if p.Phase == PhaseVerifyingFinal && p.ScaleIntent != nil && p.ScaleIntent.Effect == EffectPending {
			return fmt.Errorf("%w: scale-down effect remains pending at final verification", ErrInvalidPlan)
		}
		if p.Phase != PhaseScalingDown && p.Phase != PhaseVerifyingFinal && p.Phase != PhaseReleasingExclusion && p.ScaleIntent != nil {
			return fmt.Errorf("%w: premature scale-in scale intent", ErrInvalidPlan)
		}
		if p.Submission.Operation == OperationDrainHolder && p.ScaleIntent != nil {
			return fmt.Errorf("%w: holder drain cannot mutate replicas", ErrInvalidPlan)
		}
		if p.Submission.PinnedTarget != (HolderRef{}) {
			if p.Phase == PhasePending || p.Phase == PhaseSelectingVictim {
				if p.Target != (HolderRef{}) {
					return fmt.Errorf("%w: pinned drain target frozen before victim selection", ErrInvalidPlan)
				}
			} else if p.Target != p.Submission.PinnedTarget {
				return fmt.Errorf("%w: pinned drain target binding", ErrInvalidPlan)
			}
		} else if p.Submission.Operation == OperationDrainHolder && p.Target != (HolderRef{}) {
			return fmt.Errorf("%w: unpinned holder drain contains target", ErrInvalidPlan)
		}
	}
	return nil
}

func validStatus(status ExecutionStatus) bool {
	switch status {
	case StatusRunning, StatusPaused, StatusBlocked, StatusSucceeded, StatusFailed, StatusCancelled:
		return true
	default:
		return false
	}
}

func validPhaseForOperation(operation Operation, phase Phase) bool {
	switch operation {
	case OperationScaleOut:
		switch phase {
		case PhasePending, PhaseScalingUp, PhaseWaitingHolderReady, PhaseMovingWave, PhaseVerifyingWave, PhaseVerifyingFinal:
			return true
		}
	case OperationScaleIn:
		switch phase {
		case PhasePending, PhaseSelectingVictim, PhaseExcludingVictim, PhaseCapturingVictimShards,
			PhaseMovingWave, PhaseVerifyingWave, PhaseWaitingZeroShard, PhaseScalingDown, PhaseVerifyingFinal,
			PhaseReleasingExclusion:
			return true
		}
	case OperationDrainHolder:
		switch phase {
		case PhasePending, PhaseSelectingVictim, PhaseExcludingVictim, PhaseCapturingVictimShards,
			PhaseMovingWave, PhaseVerifyingWave, PhaseWaitingZeroShard, PhaseVerifyingFinal,
			PhaseReleasingExclusion:
			return true
		}
	}
	return false
}

func (p Plan) validateCleanupIntent() error {
	if p.Phase != PhaseReleasingExclusion {
		if p.CleanupIntent != nil {
			return fmt.Errorf("%w: cleanup intent outside release phase", ErrInvalidPlan)
		}
		return nil
	}
	intent := p.CleanupIntent
	if !usesVictimExclusion(p.Submission.Operation) || intent == nil || p.ExclusionIntent == nil ||
		p.ExclusionIntent.Effect == EffectPending || !validScaleResolution(intent.ScaleResolution) ||
		intent.FailureCode == "" {
		return fmt.Errorf("%w: cleanup intent binding", ErrInvalidPlan)
	}
	if err := validateOpaque("cleanup failure code", intent.FailureCode, MaxErrorCodeBytes); err != nil {
		return err
	}
	if intent.ReleaseOutcome != "" {
		if !intent.ReleaseOutcome.Valid() || !intent.ReleaseOutcome.TerminalForScalePlan() ||
			intent.ScaleResolution != ScaleResolutionNotCommitted {
			return fmt.Errorf("%w: cleanup release outcome", ErrInvalidPlan)
		}
	}
	if intent.ScaleResolution == ScaleResolutionCommitted && intent.ReleaseOutcome != "" {
		return fmt.Errorf("%w: committed scale has release outcome", ErrInvalidPlan)
	}
	if p.ScaleIntent != nil && intent.ScaleResolution == ScaleResolutionCommitted &&
		p.ScaleIntent.Effect == EffectPending {
		return fmt.Errorf("%w: committed cleanup has pending scale effect", ErrInvalidPlan)
	}
	return nil
}

func usesVictimExclusion(operation Operation) bool {
	return operation == OperationScaleIn || operation == OperationDrainHolder
}

func validateVisibleASCII(name, value string, max int) error {
	if value == "" || len(value) > max {
		return fmt.Errorf("%w: %s", ErrInvalidPlan, name)
	}
	for index := 0; index < len(value); index++ {
		if value[index] < 0x21 || value[index] > 0x7e {
			return fmt.Errorf("%w: %s", ErrInvalidPlan, name)
		}
	}
	return nil
}

func validateOpaque(name, value string, max int) error {
	if value == "" || len([]byte(value)) > max || strings.TrimSpace(value) != value || !utf8.ValidString(value) {
		return fmt.Errorf("%w: %s", ErrInvalidPlan, name)
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("%w: %s contains control character", ErrInvalidPlan, name)
		}
	}
	return nil
}
