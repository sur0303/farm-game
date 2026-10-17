package participant

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	contractspb "farmgame/api/gen/contracts"
	"farmgame/internal/contracts"
	"farmgame/internal/worker/sourceorder"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	commitProofFamilyCare = iota
	commitProofFamilyHarvest
	commitProofFamilyCount
)

const (
	commitProofValidationValid = iota
	commitProofValidationMissing
	commitProofValidationInvalid
	commitProofValidationStale
	commitProofValidationRoute
	commitProofValidationCount
)

const (
	commitProofDecisionRedisOff = iota
	commitProofDecisionShadowMatch
	commitProofDecisionShadowMismatch
	commitProofDecisionBypass
	commitProofDecisionRecoveryRequired
	commitProofDecisionCount
)

// CommitProofMetrics has fixed CARE/HARVEST and result dimensions. No work,
// actor, holder, boot, key, or operation identity can create a time series.
type CommitProofMetrics struct {
	validation [commitProofFamilyCount][commitProofValidationCount]atomic.Uint64
	decision   [commitProofFamilyCount][commitProofDecisionCount]atomic.Uint64
}

func commitProofFamily(call *contracts.ParticipantCall) (int, bool) {
	if call == nil {
		return 0, false
	}
	switch call.Spec.Command {
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_CARE_REWARD:
		return commitProofFamilyCare, true
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD:
		return commitProofFamilyHarvest, true
	default:
		return 0, false
	}
}

func (m *CommitProofMetrics) observeValidation(call *contracts.ParticipantCall, result int) {
	family, ok := commitProofFamily(call)
	if m != nil && ok && result >= 0 && result < commitProofValidationCount {
		m.validation[family][result].Add(1)
	}
}

func (m *CommitProofMetrics) observeDecision(call *contracts.ParticipantCall, decision int) {
	family, ok := commitProofFamily(call)
	if m != nil && ok && decision >= 0 && decision < commitProofDecisionCount {
		m.decision[family][decision].Add(1)
	}
}

func (m *CommitProofMetrics) validationCount(family, result int) uint64 {
	if m == nil || family < 0 || family >= commitProofFamilyCount || result < 0 || result >= commitProofValidationCount {
		return 0
	}
	return m.validation[family][result].Load()
}

func (m *CommitProofMetrics) decisionCount(family, decision int) uint64 {
	if m == nil || family < 0 || family >= commitProofFamilyCount || decision < 0 || decision >= commitProofDecisionCount {
		return 0
	}
	return m.decision[family][decision].Load()
}

// RegisterCommitProofMetrics publishes bounded rollout telemetry.
func RegisterCommitProofMetrics(registerer prometheus.Registerer, metrics *CommitProofMetrics, mode sourceorder.CommitProofMode) error {
	if registerer == nil || metrics == nil {
		return errors.New("player participant commit proof metrics configuration is incomplete")
	}
	if err := mode.ValidatePreferRollout(); err != nil {
		return err
	}
	families := []string{"care", "harvest"}
	validationResults := []string{"valid", "missing", "invalid", "stale", "route"}
	decisions := []string{"redis_off", "shadow_match", "shadow_mismatch", "proof_bypass", "durable_recovery_required"}
	for familyIndex, family := range families {
		for resultIndex, result := range validationResults {
			fi, ri := familyIndex, resultIndex
			if err := registerer.Register(prometheus.NewCounterFunc(prometheus.CounterOpts{
				Namespace: "farm", Subsystem: "source_order_commit_proof", Name: "validation_total",
				Help:        "CARE/HARVEST route-fenced commit proof validation results.",
				ConstLabels: prometheus.Labels{"family": family, "result": result},
			}, func() float64 { return float64(metrics.validationCount(fi, ri)) })); err != nil {
				return err
			}
		}
		for decisionIndex, decision := range decisions {
			fi, di := familyIndex, decisionIndex
			if err := registerer.Register(prometheus.NewCounterFunc(prometheus.CounterOpts{
				Namespace: "farm", Subsystem: "source_order_commit_proof", Name: "admission_total",
				Help:        "CARE/HARVEST source admission decisions in off/shadow/prefer rollout.",
				ConstLabels: prometheus.Labels{"family": family, "decision": decision},
			}, func() float64 { return float64(metrics.decisionCount(fi, di)) })); err != nil {
				return err
			}
		}
	}
	for _, candidate := range []sourceorder.CommitProofMode{sourceorder.CommitProofModeOff, sourceorder.CommitProofModeShadow, sourceorder.CommitProofModePrefer} {
		candidate := candidate
		if err := registerer.Register(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "farm", Subsystem: "source_order_commit_proof", Name: "mode",
			Help:        "Configured commit proof mode (exactly one fixed series is 1).",
			ConstLabels: prometheus.Labels{"mode": candidate.String()},
		}, func() float64 {
			if candidate == mode {
				return 1
			}
			return 0
		})); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) verifyCoordinatorCommitProof(ctx context.Context, call *contracts.ParticipantCall) (sourceorder.CommitProofStatement, error) {
	if s == nil || call == nil || call.CallerKind != contracts.ParticipantCallerCoordinator {
		return sourceorder.CommitProofStatement{}, sourceorder.ErrCommitProofInvalid
	}
	if call.SourceOrderCommitProof == nil {
		s.commitProofMetrics.observeValidation(call, commitProofValidationMissing)
		return sourceorder.CommitProofStatement{}, sourceorder.ErrCommitProofMissing
	}
	if s.commitProof == nil || s.commitProofSourceRealm == "" {
		s.commitProofMetrics.observeValidation(call, commitProofValidationInvalid)
		return sourceorder.CommitProofStatement{}, sourceorder.ErrCommitProofInvalid
	}
	proofCtx, cancel, err := contracts.ParticipantBudgetContext(ctx, call)
	if err != nil {
		return sourceorder.CommitProofStatement{}, err
	}
	statement, err := s.commitProof.VerifyParticipant(proofCtx, call, s.commitProofSourceRealm)
	cancel()
	if err != nil {
		result := commitProofValidationInvalid
		switch {
		case errors.Is(err, sourceorder.ErrCommitProofMissing):
			result = commitProofValidationMissing
		case errors.Is(err, sourceorder.ErrCommitProofStale):
			result = commitProofValidationStale
		case errors.Is(err, sourceorder.ErrCommitProofRoute):
			result = commitProofValidationRoute
		}
		s.commitProofMetrics.observeValidation(call, result)
		return sourceorder.CommitProofStatement{}, err
	}
	if err := contracts.CheckParticipantBudget(ctx, call); err != nil {
		return sourceorder.CommitProofStatement{}, err
	}
	s.commitProofMetrics.observeValidation(call, commitProofValidationValid)
	return statement, nil
}

func (s *Server) preferCommitProof(ctx context.Context, call *contracts.ParticipantCall) (sourceorder.CommitProofStatement, bool, error) {
	if call == nil || call.CallerKind != contracts.ParticipantCallerCoordinator {
		return sourceorder.CommitProofStatement{}, false, nil
	}
	switch s.commitProofMode {
	case sourceorder.CommitProofModeOff:
		s.commitProofMetrics.observeDecision(call, commitProofDecisionRedisOff)
		return sourceorder.CommitProofStatement{}, false, nil
	case sourceorder.CommitProofModeShadow:
		statement, err := s.verifyCoordinatorCommitProof(ctx, call)
		return statement, err == nil, nil
	case sourceorder.CommitProofModePrefer:
		statement, err := s.verifyCoordinatorCommitProof(ctx, call)
		if err != nil {
			s.commitProofMetrics.observeDecision(call, commitProofDecisionRecoveryRequired)
			// Reject before the mailbox. Direct does not claim/complete the
			// SourceOrder, so the durable Worker will retry through ResumeStep
			// and the ordinary Redis lease/source verifier.
			return sourceorder.CommitProofStatement{}, false, status.Error(codes.FailedPrecondition, "route-fenced commit proof unavailable; durable Worker recovery required")
		}
		s.commitProofMetrics.observeDecision(call, commitProofDecisionBypass)
		return statement, true, nil
	default:
		return sourceorder.CommitProofStatement{}, false, status.Error(codes.FailedPrecondition, fmt.Sprintf("invalid commit proof mode %d", s.commitProofMode))
	}
}

func shadowProofMatchesRedis(statement sourceorder.CommitProofStatement, call *contracts.ParticipantCall) bool {
	return call != nil && call.TrustedSourceDueAtMs > 0 && statement.DueAtMs == call.TrustedSourceDueAtMs
}

func (s *Server) verifyHarvestSource(ctx context.Context, call *contracts.ParticipantCall) error {
	var (
		statement  sourceorder.CommitProofStatement
		proofValid bool
	)
	if call != nil && call.CallerKind == contracts.ParticipantCallerCoordinator {
		var err error
		statement, proofValid, err = s.preferCommitProof(ctx, call)
		if err != nil {
			return err
		}
		if s.commitProofMode == sourceorder.CommitProofModePrefer && proofValid {
			return contracts.CheckParticipantBudget(ctx, call)
		}
	}
	if s == nil || s.harvestSource == nil {
		return status.Error(codes.FailedPrecondition, "player participant Harvest SourceOrder verifier is not configured")
	}
	proofCtx, cancel, err := contracts.ParticipantBudgetContext(ctx, call)
	if err != nil {
		return err
	}
	err = s.harvestSource.VerifyHarvestSource(proofCtx, call)
	cancel()
	if err != nil {
		if call != nil && call.CallerKind == contracts.ParticipantCallerCoordinator && s.commitProofMode == sourceorder.CommitProofModeShadow {
			s.commitProofMetrics.observeDecision(call, commitProofDecisionShadowMismatch)
		}
		return err
	}
	if call != nil && call.CallerKind == contracts.ParticipantCallerCoordinator && s.commitProofMode == sourceorder.CommitProofModeShadow {
		decision := commitProofDecisionShadowMismatch
		if proofValid && shadowProofMatchesRedis(statement, call) {
			decision = commitProofDecisionShadowMatch
		}
		s.commitProofMetrics.observeDecision(call, decision)
	}
	return contracts.CheckParticipantBudget(ctx, call)
}

func careDependencyProofFromCommitProof(call *contracts.ParticipantCall) (CareDependencyProof, error) {
	if call == nil || call.Work == nil {
		return CareDependencyProof{}, sourceorder.ErrCommitProofInvalid
	}
	payload, ok := call.Payload.(*contractspb.ApplyCareRewardPayload)
	if !ok || payload == nil || payload.GetCareResolutionSnapshot() == nil {
		return CareDependencyProof{}, sourceorder.ErrCommitProofInvalid
	}
	return CareDependencyProof{
		SourceOrderFound: true, SourceOrderID: call.Work.GetWorkId(), SourceOrderType: contractspb.WorkType_WORK_TYPE_CARE_REWARD,
		SourceAuthority: proto.Clone(call.Work.GetSourceAuthority()).(*contractspb.SourceAuthorityRef),
		Snapshot:        proto.Clone(payload.GetCareResolutionSnapshot()).(*contractspb.CareResolutionSnapshot),
		SnapshotHash:    append([]byte(nil), payload.GetCareResolutionHash()...),
		OrderState:      CareRewardOrderStatePending, DueIndexBound: true, WorkVersion: call.ExpectedWorkVersion,
		InvocationID: call.InvocationID, ParticipantStep: call.ParticipantStep,
		ExecutionMode: CareDependencyExecutionModeCoordinator, FenceValid: true,
	}, nil
}
