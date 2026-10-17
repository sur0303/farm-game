package ingress

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"

	commonpb "farmgame/api/gen/common"
	logicrpcpb "farmgame/api/gen/logicrpc"
	publicpb "farmgame/api/gen/public"
	farmactor "farmgame/internal/farm/actor"
	frienddomain "farmgame/internal/friend"
	"farmgame/internal/logic/trusted"
	"farmgame/internal/storage/blobstore"
	"farmgame/internal/workflow"

	"google.golang.org/protobuf/proto"
)

// TheftAdmissionRepository is the narrow PairRepository surface required by
// the public THEFT FirstHop. Keeping the adapter narrow prevents this ingress
// from performing friend mutations other than the atomic ActionAdmission.
// A friend.RedisPairRepository and friend.MemoryPairRepository both satisfy
// this interface.
type TheftAdmissionRepository interface {
	CommitFriendActionAdmission(context.Context, frienddomain.ActionAdmissionCommand) (frienddomain.ActionAdmissionOutcome, error)
	GetFriendActionAdmission(context.Context, frienddomain.ActionAdmissionCommand) (frienddomain.ActionAdmissionOutcome, error)
}

// TheftDispatchPort is the only fast-path dependency after Execution-Accept.
// The ID must identify a persisted FRIEND_ACTION_DISPATCH_ORDER; callers must
// not pass a permit, visitor, quantity, pet result, or unit price.
type TheftDispatchPort interface {
	DispatchTheft(context.Context, string) error
}

// TheftTargetBinding is the durable target material needed to reconstruct a
// TheftInput after the Pair ActionAdmission response has been lost. Pair
// deliberately stores only the target-binding hash; this record preserves
// the already-authenticated plot/cycle tuple without making it an authority
// decision or a client-controlled workflow input.
type TheftTargetBinding struct {
	PairID            string
	DispatchOrderID   string
	VisitorPlayerID   uint64
	OwnerPlayerID     uint64
	TargetFarmID      uint64
	OperationID       string
	PlotID            uint32
	CropCycleID       string
	PayloadHash       []byte
	TargetBindingHash []byte
}

// TheftTargetBindingRecorder is an optional-but-production-required durable
// sidecar used by the concrete dispatch port. It is intentionally separate
// from TheftDispatchPort so the Pair repository remains the sole execution
// acceptance point.
type TheftTargetBindingRecorder interface {
	RecordTheftTargetBinding(context.Context, TheftTargetBinding) error
}

var errTheftIngressUnavailable = errors.New("theft ingress unavailable")

const (
	theftCommandType            = "THEFT"
	theftInitialResultPollDelay = 50 * time.Millisecond
)

// admitTheft is the public THEFT FirstHop. Pair Commit is the sole
// Execution-Accept. A successful commit is returned as PROCESSING because the
// Participant/Workflow state machine is intentionally outside TH3B.
func (s *Service) admitTheft(ctx context.Context, req *logicrpcpb.ExecuteCommandRequest, verified *trusted.VerifiedCommand, now time.Time, result *logicrpcpb.BusinessCommandOutcome) error {
	if s == nil || s.theftPair == nil || s.theftDispatch == nil {
		return errTheftIngressUnavailable
	}
	if req == nil || req.Command == nil || req.Trusted == nil || req.Trusted.Session == nil {
		return fmt.Errorf("%w: trusted command", errTheftIngressUnavailable)
	}
	typed, ok := verified.Decoded.(*publicpb.TheftRequest)
	if !ok || typed == nil {
		return errors.New("THEFT payload is not admitted")
	}
	viewer := req.Trusted.Session.PlayerId
	targetFarm := verified.RouteKey
	if viewer == 0 || targetFarm == 0 || targetFarm == viewer {
		return &farmactor.Error{Code: commonpb.ErrorCode_ERR_PERMISSION_DENIED, Message: "不能偷自己的作物"}
	}
	if strings.TrimSpace(typed.GetCropCycleId()) == "" {
		return errors.New("THEFT crop cycle is required")
	}
	// Public plot_id keeps the long-standing zero-based wire layout.  The
	// FarmActor/Workflow contract is one-based, just like Plant and Harvest;
	// bind the converted target once before Pair admission so recovery cannot
	// reinterpret the public index after response loss.
	if typed.GetPlotId() >= 24 {
		return &farmactor.Error{Code: commonpb.ErrorCode_ERR_PLOT_NOT_FOUND, Message: "作物地块不存在"}
	}
	plotID := typed.GetPlotId() + 1

	payloadHash := append([]byte(nil), verified.PayloadHash...)
	bindingHash := theftTargetBindingHash(viewer, targetFarm, plotID, typed.GetCropCycleId())
	command := frienddomain.ActionAdmissionCommand{
		Identity: frienddomain.CommitIdentity{
			SubjectPlayerID: viewer,
			CommandType:     theftCommandType,
			OperationID:     req.Command.OperationId,
		},
		PayloadHash:       payloadHash,
		VisitorPlayerID:   viewer,
		OwnerPlayerID:     targetFarm,
		TargetFarmID:      targetFarm,
		Scope:             frienddomain.ActionScopeTheft,
		TargetBindingHash: bindingHash,
		NowMs:             now.UnixMilli(),
		PermitTTLMs:       frienddomain.DefaultActionPermitTTL.Milliseconds(),
		WorkflowTTLMs:     workflow.DefaultWorkflowDeadline.Milliseconds(),
	}
	if s.theftTargetRecorder != nil {
		if err := s.theftTargetRecorder.RecordTheftTargetBinding(ctx, TheftTargetBinding{
			VisitorPlayerID: viewer, OwnerPlayerID: targetFarm, TargetFarmID: targetFarm,
			OperationID: req.Command.OperationId, PlotID: plotID, CropCycleID: typed.GetCropCycleId(),
			PayloadHash: append([]byte(nil), payloadHash...), TargetBindingHash: append([]byte(nil), bindingHash...),
		}); err != nil {
			return fmt.Errorf("%w: target binding persistence: %v", errTheftIngressUnavailable, err)
		}
	}

	admission, err := s.commitOrRecoverTheftAdmission(ctx, command)
	if err != nil {
		return err
	}
	if admission.Outcome.Status != frienddomain.StatusSucceeded {
		return theftAdmissionOutcomeError(admission.Outcome)
	}
	if err := validateTheftAdmission(command, admission); err != nil {
		return err
	}
	if s.theftTargetRecorder != nil {
		if err := s.theftTargetRecorder.RecordTheftTargetBinding(ctx, TheftTargetBinding{
			PairID: admission.Permit.RelationID, DispatchOrderID: admission.DispatchOrderID,
			VisitorPlayerID: viewer, OwnerPlayerID: targetFarm, TargetFarmID: targetFarm,
			OperationID: req.Command.OperationId, PlotID: plotID, CropCycleID: typed.GetCropCycleId(),
			PayloadHash: append([]byte(nil), payloadHash...), TargetBindingHash: append([]byte(nil), bindingHash...),
		}); err != nil {
			return fmt.Errorf("%w: admitted target binding persistence: %v", errTheftIngressUnavailable, err)
		}
	}

	// Dispatch is deliberately best-effort. The order is already durable and
	// the worker/due-index path owns recovery when this call is lost or fails.
	_ = s.theftDispatch.DispatchTheft(ctx, admission.DispatchOrderID)
	result.Status = commonpb.CommandStatus_PROCESSING
	result.RetryDirective = logicrpcpb.RetryDirective_POLL_OPERATION_RESULT
	// Workflow.InitialRecoveryDelay is the durable recovery backoff, not a
	// client result-poll hint. The online fast path and Pair result processor
	// can finish in sub-second time; make the first read-only lookup prompt
	// without changing any recovery retry schedule.
	result.RetryAfterMs = uint32(theftInitialResultPollDelay / time.Millisecond)
	result.ResultPayload, err = proto.MarshalOptions{Deterministic: true}.Marshal(&publicpb.TheftResult{
		TargetFarmId: targetFarm,
		PlotId:       typed.GetPlotId(),
		CropCycleId:  typed.GetCropCycleId(),
	})
	return err
}

func (s *Service) commitOrRecoverTheftAdmission(ctx context.Context, command frienddomain.ActionAdmissionCommand) (frienddomain.ActionAdmissionOutcome, error) {
	// This stripe is only a same-process contention optimization. Redis's
	// guarded record-version refresh remains the cross-Pod correctness root.
	// Unrelated Pair admissions use independent stripes.
	stripe := theftPairStripe(command.VisitorPlayerID, command.OwnerPlayerID)
	s.theftAdmissionStripes[stripe].Lock()
	defer s.theftAdmissionStripes[stripe].Unlock()
	var admission frienddomain.ActionAdmissionOutcome
	var err error
	for attempt := 0; attempt < 4; attempt++ {
		admission, err = s.theftPair.CommitFriendActionAdmission(ctx, command)
		if err == nil || !errors.Is(err, frienddomain.ErrFriendRecordVersionConflict) {
			break
		}
		// Pair's record-version conflict is a proven pre-commit result. A
		// concurrent, independent ActionAdmission may have advanced the same
		// Pair record; retrying the same public identity re-reads that Pair
		// snapshot and creates no second business effect.
		if attempt == 3 {
			break
		}
		timer := time.NewTimer(time.Duration(attempt+1) * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return frienddomain.ActionAdmissionOutcome{}, ctx.Err()
		case <-timer.C:
		}
	}
	if err == nil {
		return admission, nil
	}
	if !theftAdmissionMayHaveCommitted(err) {
		return frienddomain.ActionAdmissionOutcome{}, err
	}

	// A lost response is resolved by the same identity and payload hash. It is
	// never resolved by issuing a new short-lived permit or by retrying Commit.
	resolveCtx := ctx
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		var cancel context.CancelFunc
		resolveCtx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
	}
	return s.theftPair.GetFriendActionAdmission(resolveCtx, command)
}

func theftPairStripe(left, right uint64) uint64 {
	if left > right {
		left, right = right, left
	}
	return (left*131 + right*17) % 64
}

func theftAdmissionMayHaveCommitted(err error) bool {
	return errors.Is(err, frienddomain.ErrRedisTimeoutUnknown) ||
		errors.Is(err, blobstore.ErrTimeoutUnknown) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled)
}

func theftAdmissionOutcomeError(outcome frienddomain.Outcome) error {
	if outcome.Reason == frienddomain.ReasonVisitNotPermitted {
		return &farmactor.Error{Code: commonpb.ErrorCode_ERR_FRIEND_PERMIT_INVALID, Message: "好友关系无效或授权已撤销"}
	}
	return &farmactor.Error{Code: commonpb.ErrorCode_ERR_FRIEND_PERMIT_INVALID, Message: "好友操作授权未获准"}
}

func validateTheftAdmission(command frienddomain.ActionAdmissionCommand, admission frienddomain.ActionAdmissionOutcome) error {
	if admission.Outcome.Identity.Key() != command.Identity.Key() ||
		!bytes.Equal(admission.Outcome.PayloadHash, command.PayloadHash) {
		return fmt.Errorf("%w: admission receipt identity", errTheftIngressUnavailable)
	}
	permit := admission.Permit
	if err := frienddomain.VerifyAdmittedPermit(permit); err != nil {
		return fmt.Errorf("%w: %v", errTheftIngressUnavailable, err)
	}
	if permit.OperationID != command.Identity.OperationID ||
		permit.VisitorPlayerID != command.VisitorPlayerID ||
		permit.OwnerPlayerID != command.OwnerPlayerID ||
		permit.TargetFarmID != command.TargetFarmID ||
		permit.Scope != frienddomain.ActionScopeTheft ||
		!bytes.Equal(permit.PayloadHash, command.PayloadHash) ||
		!bytes.Equal(permit.TargetBindingHash, command.TargetBindingHash) ||
		admission.DispatchOrderID == "" || admission.WorkflowDeadlineAtMs <= permit.ExpireAtAuthorityMs ||
		admission.WorkflowDeadlineAtMs-permit.IssuedAtAuthorityMs != command.WorkflowTTLMs {
		return fmt.Errorf("%w: admission permit binding", errTheftIngressUnavailable)
	}
	return nil
}

// theftTargetBindingHash binds target facts that are not independently
// modeled as fields on ActionAdmissionCommand. PayloadHash protects the
// canonical public request; this second digest makes the target plot/cycle
// binding explicit in the Pair permit/order.
func theftTargetBindingHash(viewer, owner uint64, plot uint32, cycle string) []byte {
	h := sha256.New()
	h.Write([]byte("farm-theft-target-binding-v1"))
	writeUint64(h, uint64(viewer))
	writeUint64(h, uint64(owner))
	writeUint64(h, uint64(plot))
	writeString(h, cycle)
	return h.Sum(nil)
}

// TheftTargetBindingHash is exported for the production recovery adapter and
// tests. The bytes are the same frozen hash used by public admission.
func TheftTargetBindingHash(viewer, owner uint64, plot uint32, cycle string) []byte {
	return theftTargetBindingHash(viewer, owner, plot, cycle)
}

func writeUint64(h interface{ Write([]byte) (int, error) }, value uint64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], value)
	_, _ = h.Write(buf[:])
}

func writeString(h interface{ Write([]byte) (int, error) }, value string) {
	writeUint64(h, uint64(len(value)))
	_, _ = h.Write([]byte(value))
}
