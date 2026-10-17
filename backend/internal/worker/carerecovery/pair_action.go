package carerecovery

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"

	commonpb "farmgame/api/gen/common"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/friend"
	"farmgame/internal/worker/theftrecovery"
	"farmgame/internal/workidentity"
)

// PairCareFarmInspector is the existing read-only Farm receipt endpoint. Pair
// CARE recovery uses it to prove that FarmActor already committed the public
// CARE operation; it does not manufacture a public command or bypass Actor
// admission.
type PairCareFarmInspector interface {
	InspectFarmOperation(context.Context, *logicrpcpb.InspectFarmOperationRequest) (*logicrpcpb.InspectFarmOperationResponse, error)
}

type PairCareProcessorConfig struct {
	Pair       *friend.RedisFriendshipRepositories
	Dispatch   *theftrecovery.FriendPairDispatchRepository
	Leases     theftrecovery.WorkLeaseRepository
	Farm       PairCareFarmInspector
	HolderID   string
	BootID     string
	LeaseToken string
	LeaseTTL   time.Duration
}

// PairCareProcessor owns CARE-scoped FRIEND_ACTION_DISPATCH_ORDER and
// RESULT_PUBLISH_ORDER members from the shared Pair DueIndex. It only releases
// an accepted order after a terminal FarmActor receipt is readable. If the
// original CARE has not reached Farm, the order remains due and unchanged.
type PairCareProcessor struct {
	config PairCareProcessorConfig
}

var _ theftrecovery.PairCareOrderProcessor = (*PairCareProcessor)(nil)

func NewPairCareProcessor(config PairCareProcessorConfig) (*PairCareProcessor, error) {
	if config.Pair == nil || config.Dispatch == nil || config.Leases == nil || config.Farm == nil ||
		strings.TrimSpace(config.HolderID) == "" || strings.TrimSpace(config.BootID) == "" ||
		strings.TrimSpace(config.LeaseToken) == "" {
		return nil, fmt.Errorf("care pair recovery: Pair, dispatch, lease, Farm inspector and worker identity are required")
	}
	if config.LeaseTTL <= 0 {
		config.LeaseTTL = 30 * time.Second
	}
	return &PairCareProcessor{config: config}, nil
}

func (p *PairCareProcessor) ProcessCareOrder(ctx context.Context, candidate theftrecovery.DispatchCandidate) (bool, error) {
	if p == nil {
		return false, errors.New("care pair recovery: nil processor")
	}
	ref, err := workidentity.DecodeWorkRefV1(candidate.Member)
	if err != nil {
		return false, theftrecovery.ErrMalformedMember
	}
	switch ref.WorkType {
	case theftrecovery.FriendActionDispatchOrder:
		return p.processDispatch(ctx, candidate)
	case theftrecovery.ResultPublishOrder:
		return p.processResult(ctx, candidate)
	default:
		return false, theftrecovery.ErrMalformedMember
	}
}

func (p *PairCareProcessor) processDispatch(ctx context.Context, candidate theftrecovery.DispatchCandidate) (bool, error) {
	order, err := p.config.Dispatch.LoadActionDispatch(ctx, candidate)
	if err != nil {
		return false, err
	}
	order, err = theftrecovery.NormalizePairActionDispatchOrder(order, friend.ActionScopeCare)
	if err != nil {
		return false, err
	}
	terminal, found, err := p.inspectTerminal(ctx, order.TargetFarmID, order.OperationID)
	if err != nil || !found {
		return false, err
	}
	claim, err := p.config.Leases.Claim(ctx, theftrecovery.ClaimRequest{
		Order: order, HolderID: p.config.HolderID, BootID: p.config.BootID,
		LeaseToken: p.config.LeaseToken, RequestedTTL: p.config.LeaseTTL,
	})
	if err != nil {
		return false, err
	}
	switch claim.Status {
	case theftrecovery.LeaseNotFound, theftrecovery.LeaseNotClaimable:
		return true, nil
	case theftrecovery.LeaseHeld, theftrecovery.LeaseNotDue:
		return false, nil
	case theftrecovery.LeaseClaimed:
		// Continue under the exact Pair Work fence.
	default:
		return false, fmt.Errorf("care pair recovery: claim status %d", claim.Status)
	}
	proof := pairCareResultProof(order.PairID, order.OperationID, order.TargetFarmID, terminal, order.CanonicalPayloadHash)
	completed, err := p.config.Leases.Complete(ctx, theftrecovery.CompleteRequest{
		Order: order,
		Fence: theftrecovery.LeaseFence{HolderID: p.config.HolderID, BootID: p.config.BootID,
			LeaseToken: p.config.LeaseToken, LeaseEpoch: claim.LeaseEpoch, WorkVersion: claim.WorkVersion},
		Proof: theftrecovery.CompletionProof{PayloadHash: order.PayloadHash,
			SourceProofHash: theftrecovery.SourceProofHash(order), ResultProofHash: proof[:]},
	})
	if err != nil {
		return false, err
	}
	switch completed.Status {
	case theftrecovery.LeaseCompleted, theftrecovery.LeaseAlreadyCompleted:
		return true, nil
	case theftrecovery.LeaseCompleteFenceMismatch, theftrecovery.LeaseCompleteStaleVersion:
		return false, theftrecovery.ErrLeaseLost
	default:
		return false, fmt.Errorf("care pair recovery: complete status %d", completed.Status)
	}
}

func (p *PairCareProcessor) processResult(ctx context.Context, candidate theftrecovery.DispatchCandidate) (bool, error) {
	ref, _ := workidentity.DecodeWorkRefV1(candidate.Member)
	binding, err := p.config.Pair.ReadPairActionResultOrder(ctx, candidate.BucketID, ref.WorkType, ref.WorkID)
	if err != nil {
		return false, err
	}
	if binding.Scope != friend.ActionScopeCare {
		return false, theftrecovery.ErrNotTheft
	}
	if binding.AdmissionStatus == friend.StatusRejected {
		// Rejected-before-Work is already a terminal Pair Authority receipt: no
		// Farm operation exists or should be invented. Require the exact frozen
		// rejection shape and complete from a Pair-specific proof rather than
		// misclassifying the expected missing Farm receipt as pending execution.
		if binding.AdmissionReason != friend.ReasonVisitNotPermitted || binding.DispatchOrderID != "" ||
			binding.WorkflowDeadlineAtMs != 0 {
			return false, friend.ErrFriendReconcile
		}
		proof := pairCareAdmissionRejectionProof(binding)
		if err := p.config.Pair.CompletePairActionResultOrder(ctx, binding, "REJECTED", proof[:]); err != nil {
			return false, err
		}
		return true, nil
	}
	if binding.AdmissionStatus != friend.StatusSucceeded {
		return false, friend.ErrFriendReconcile
	}
	terminal, found, err := p.inspectTerminal(ctx, binding.OwnerPlayerID, binding.OperationID)
	if err != nil || !found {
		return false, err
	}
	terminalName, err := careTerminalName(terminal)
	if err != nil {
		return false, err
	}
	proof := pairCareResultProof(binding.PairID, binding.OperationID, binding.OwnerPlayerID, terminal, binding.PayloadHash)
	if err := p.config.Pair.CompletePairActionResultOrder(ctx, binding, terminalName, proof[:]); err != nil {
		return false, err
	}
	return true, nil
}

func (p *PairCareProcessor) inspectTerminal(ctx context.Context, farmID uint64, operationID string) (commonpb.CommandStatus, bool, error) {
	response, err := p.config.Farm.InspectFarmOperation(ctx, &logicrpcpb.InspectFarmOperationRequest{
		FarmId: farmID, OperationId: operationID,
	})
	if err != nil {
		return commonpb.CommandStatus_COMMAND_STATUS_UNSPECIFIED, false, err
	}
	if response == nil {
		return commonpb.CommandStatus_COMMAND_STATUS_UNSPECIFIED, false, errors.New("care pair recovery: nil Farm inspection")
	}
	if response.GetStatus() == commonpb.CommandStatus_REJECTED && response.GetErrorCode() == commonpb.ErrorCode_ERR_SETTLEMENT_NOT_FOUND {
		// Pair admission committed but Farm has not. Without a new Proto command
		// this worker cannot recreate public CARE target fields; leave the order
		// due rather than inventing execution or a terminal rejection.
		return response.GetStatus(), false, nil
	}
	if _, err := careTerminalName(response.GetStatus()); err != nil {
		if response.GetStatus() == commonpb.CommandStatus_PROCESSING {
			return response.GetStatus(), false, nil
		}
		return response.GetStatus(), false, err
	}
	return response.GetStatus(), true, nil
}

func careTerminalName(status commonpb.CommandStatus) (string, error) {
	switch status {
	case commonpb.CommandStatus_SUCCEEDED:
		return "SUCCEEDED", nil
	case commonpb.CommandStatus_NO_CHANGE:
		return "NO_CHANGE", nil
	case commonpb.CommandStatus_REJECTED:
		return "REJECTED", nil
	default:
		return "", fmt.Errorf("care pair recovery: non-terminal Farm status %s", status)
	}
}

func pairCareAdmissionRejectionProof(binding friend.PairActionResultBinding) [sha256.Size]byte {
	h := sha256.New()
	write := func(value []byte) {
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(value)))
		_, _ = h.Write(length[:])
		_, _ = h.Write(value)
	}
	write([]byte("pair-care-admission-rejection-v1"))
	write([]byte(binding.PairID))
	write([]byte(binding.OperationID))
	write([]byte(binding.OperationDigest))
	write([]byte(binding.AdmissionReason))
	write(binding.PayloadHash)
	write(binding.TargetBindingHash)
	var proof [sha256.Size]byte
	copy(proof[:], h.Sum(nil))
	return proof
}

func pairCareResultProof(pairID, operationID string, farmID uint64, status commonpb.CommandStatus, payloadHash []byte) [sha256.Size]byte {
	h := sha256.New()
	write := func(value []byte) {
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(value)))
		_, _ = h.Write(length[:])
		_, _ = h.Write(value)
	}
	write([]byte("pair-care-farm-receipt-v1"))
	write([]byte(pairID))
	write([]byte(operationID))
	var number [8]byte
	binary.BigEndian.PutUint64(number[:], farmID)
	write(number[:])
	binary.BigEndian.PutUint64(number[:], uint64(status))
	write(number[:])
	write(payloadHash)
	var proof [sha256.Size]byte
	copy(proof[:], h.Sum(nil))
	return proof
}
