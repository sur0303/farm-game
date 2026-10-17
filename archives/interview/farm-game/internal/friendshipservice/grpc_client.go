package friendshipservice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	friendshiprpcpb "farmgame/api/gen/friendshiprpc"
	"farmgame/internal/friend"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GRPCClient struct {
	client  friendshiprpcpb.FriendshipServiceClient
	timeout time.Duration
}

func NewGRPCClient(client friendshiprpcpb.FriendshipServiceClient, timeout time.Duration) (*GRPCClient, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: gRPC client", ErrInvalidArgument)
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return &GRPCClient{client: client, timeout: timeout}, nil
}

func (c *GRPCClient) callContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, c.timeout)
}

func (c *GRPCClient) CreateShare(ctx context.Context, input CreateShareInput) (CreateShareResult, error) {
	callCtx, cancel := c.callContext(ctx)
	defer cancel()
	response, err := c.client.CreateShare(callCtx, &friendshiprpcpb.CreateShareRequest{SubjectPlayerId: input.SubjectPlayerID,
		OperationId: input.OperationID, ObservationOnly: input.ObservationOnly})
	if err != nil {
		return CreateShareResult{}, clientRPCError(err, false)
	}
	if response == nil || response.GetShareToken() == "" || response.GetAcceptMode() != ShareAcceptModeMulti ||
		response.GetTokenGeneration() == 0 || response.GetIssuedAtMs() <= 0 || response.GetExpiresAtMs() <= response.GetIssuedAtMs() {
		return CreateShareResult{}, fmt.Errorf("%w: create share", ErrMalformedResponse)
	}
	outcome, err := outcomeFromRPC(response.GetOutcome())
	if err != nil {
		return CreateShareResult{}, err
	}
	return CreateShareResult{Outcome: outcome, ShareToken: response.GetShareToken(), AcceptMode: response.GetAcceptMode(),
		TokenGeneration: response.GetTokenGeneration(), IssuedAtMs: response.GetIssuedAtMs(), ExpiresAtMs: response.GetExpiresAtMs()}, nil
}

func (c *GRPCClient) ResolveShare(ctx context.Context, input ResolveShareInput) (ResolveShareResult, error) {
	callCtx, cancel := c.callContext(ctx)
	defer cancel()
	response, err := c.client.ResolveShare(callCtx, &friendshiprpcpb.ResolveShareRequest{ShareToken: input.ShareToken})
	if err != nil {
		return ResolveShareResult{}, clientRPCError(err, true)
	}
	if response == nil || response.GetInviterPlayerId() == 0 || response.GetAcceptMode() != ShareAcceptModeMulti {
		return ResolveShareResult{}, fmt.Errorf("%w: resolve share", ErrMalformedResponse)
	}
	profile, err := profileFromRPC(response.GetInviterProfile(), response.GetInviterPlayerId())
	if err != nil {
		return ResolveShareResult{}, err
	}
	return ResolveShareResult{InviterPlayerID: response.GetInviterPlayerId(), InviterProfile: profile,
		AcceptMode: response.GetAcceptMode()}, nil
}

func (c *GRPCClient) RedeemShare(ctx context.Context, input RedeemShareInput) (RedeemShareResult, error) {
	callCtx, cancel := c.callContext(ctx)
	defer cancel()
	response, err := c.client.RedeemShare(callCtx, &friendshiprpcpb.RedeemShareRequest{RedeemerPlayerId: input.RedeemerPlayerID,
		ShareToken: input.ShareToken, OperationId: input.OperationID, ObservationOnly: input.ObservationOnly})
	if err != nil {
		return RedeemShareResult{}, clientRPCError(err, true)
	}
	if response == nil || response.GetInviterPlayerId() == 0 {
		return RedeemShareResult{}, fmt.Errorf("%w: redeem share", ErrMalformedResponse)
	}
	outcome, err := outcomeFromRPC(response.GetOutcome())
	if err != nil {
		return RedeemShareResult{}, err
	}
	var pair PairState
	switch outcome.Status {
	case friend.StatusRejected:
		if response.GetPair() != nil {
			pair, err = pairFromRPC(response.GetPair())
			if err != nil {
				return RedeemShareResult{}, err
			}
			if pair.Status == friend.RelationStatusNone {
				return RedeemShareResult{}, fmt.Errorf("%w: rejected redeem empty pair", ErrMalformedResponse)
			}
		}
	case friend.StatusSucceeded, friend.StatusNoChange:
		pair, err = pairFromRPC(response.GetPair())
		if err != nil {
			return RedeemShareResult{}, err
		}
		if pair.Status != friend.RelationStatusActive {
			return RedeemShareResult{}, fmt.Errorf("%w: redeem pair status", ErrMalformedResponse)
		}
	default:
		return RedeemShareResult{}, fmt.Errorf("%w: redeem outcome status", ErrMalformedResponse)
	}
	profile, err := profileFromRPC(response.GetInviterProfile(), response.GetInviterPlayerId())
	if err != nil {
		return RedeemShareResult{}, err
	}
	return RedeemShareResult{Outcome: outcome, Pair: pair, InviterPlayerID: response.GetInviterPlayerId(),
		InviterProfile: profile}, nil
}

func (c *GRPCClient) GetFriendList(ctx context.Context, input GetFriendListInput) (GetFriendListResult, error) {
	callCtx, cancel := c.callContext(ctx)
	defer cancel()
	response, err := c.client.GetFriendList(callCtx, &friendshiprpcpb.GetFriendListRequest{ViewerPlayerId: input.ViewerPlayerID,
		Cursor: input.Cursor, PageSize: uint32(input.PageSize)})
	if err != nil {
		return GetFriendListResult{}, clientRPCError(err, false)
	}
	if response == nil || response.GetValidatedAtMs() <= 0 {
		return GetFriendListResult{}, fmt.Errorf("%w: friend list envelope", ErrMalformedResponse)
	}
	page := friend.FriendPage{Friends: make([]friend.FriendCard, 0, len(response.GetFriends())),
		NextCursor: response.GetNextCursor(), RosterRevision: response.GetRosterRevision(), ValidatedAtMs: response.GetValidatedAtMs()}
	seen := make(map[uint64]struct{}, len(response.GetFriends()))
	for _, row := range response.GetFriends() {
		card, decodeErr := cardFromRPC(row)
		if decodeErr != nil {
			return GetFriendListResult{}, decodeErr
		}
		if _, duplicate := seen[card.FriendPlayerID]; duplicate {
			return GetFriendListResult{}, fmt.Errorf("%w: duplicate friend card", ErrMalformedResponse)
		}
		seen[card.FriendPlayerID] = struct{}{}
		page.Friends = append(page.Friends, card)
	}
	return GetFriendListResult{Page: page}, nil
}

func (c *GRPCClient) DeleteFriend(ctx context.Context, input PairMutationInput) (PairMutationResult, error) {
	callCtx, cancel := c.callContext(ctx)
	defer cancel()
	response, err := c.client.DeleteFriend(callCtx, &friendshiprpcpb.DeleteFriendRequest{CallerPlayerId: input.CallerPlayerID,
		PeerPlayerId: input.PeerPlayerID, OperationId: input.OperationID, ObservationOnly: input.ObservationOnly})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return PairMutationResult{}, fmt.Errorf("%w: %v", ErrNotFriend, err)
		}
		return PairMutationResult{}, clientRPCError(err, false)
	}
	return pairMutationFromRPC(response)
}

func (c *GRPCClient) BlockFriend(ctx context.Context, input PairMutationInput) (PairMutationResult, error) {
	callCtx, cancel := c.callContext(ctx)
	defer cancel()
	response, err := c.client.BlockFriend(callCtx, &friendshiprpcpb.BlockFriendRequest{CallerPlayerId: input.CallerPlayerID,
		PeerPlayerId: input.PeerPlayerID, OperationId: input.OperationID, ObservationOnly: input.ObservationOnly})
	if err != nil {
		return PairMutationResult{}, clientRPCError(err, false)
	}
	return pairMutationFromRPC(response)
}

func (c *GRPCClient) UnblockFriend(ctx context.Context, input PairMutationInput) (PairMutationResult, error) {
	callCtx, cancel := c.callContext(ctx)
	defer cancel()
	response, err := c.client.UnblockFriend(callCtx, &friendshiprpcpb.UnblockFriendRequest{CallerPlayerId: input.CallerPlayerID,
		PeerPlayerId: input.PeerPlayerID, OperationId: input.OperationID, ObservationOnly: input.ObservationOnly})
	if err != nil {
		return PairMutationResult{}, clientRPCError(err, false)
	}
	return pairMutationFromRPC(response)
}

func pairMutationFromRPC(response *friendshiprpcpb.PairMutationResponse) (PairMutationResult, error) {
	if response == nil {
		return PairMutationResult{}, fmt.Errorf("%w: pair mutation", ErrMalformedResponse)
	}
	outcome, err := outcomeFromRPC(response.GetOutcome())
	if err != nil {
		return PairMutationResult{}, err
	}
	pair, err := pairFromRPC(response.GetPair())
	if err != nil {
		return PairMutationResult{}, err
	}
	return PairMutationResult{Outcome: outcome, Pair: pair}, nil
}

func (c *GRPCClient) GetNextFriendOpportunity(ctx context.Context, input GetNextFriendOpportunityInput) (GetNextFriendOpportunityResult, error) {
	callCtx, cancel := c.callContext(ctx)
	defer cancel()
	hintedTargets := make([]*friendshiprpcpb.FriendOpportunityHintedTarget, 0, len(input.HintedTargets))
	for _, target := range input.HintedTargets {
		hintedTargets = append(hintedTargets, &friendshiprpcpb.FriendOpportunityHintedTarget{
			OwnerId: target.OwnerID, OpportunityRevision: target.OpportunityRevision,
		})
	}
	response, err := c.client.GetNextFriendOpportunity(callCtx,
		&friendshiprpcpb.GetNextFriendOpportunityRequest{ViewerPlayerId: input.ViewerPlayerID, HintedTargets: hintedTargets})
	if err != nil {
		return GetNextFriendOpportunityResult{}, clientRPCError(err, false)
	}
	if response == nil {
		return GetNextFriendOpportunityResult{}, fmt.Errorf("%w: next opportunity", ErrMalformedResponse)
	}
	switch response.GetStatus() {
	case friendshiprpcpb.NextOpportunityStatus_NEXT_OPPORTUNITY_STATUS_FOUND:
		card, decodeErr := cardFromRPC(response.GetFriend())
		if decodeErr != nil || card.OpportunityStatus != friend.OpportunityReadReady || !card.Action.Actionable() {
			if decodeErr == nil {
				decodeErr = fmt.Errorf("%w: non-actionable FOUND", ErrMalformedResponse)
			}
			return GetNextFriendOpportunityResult{}, decodeErr
		}
		return GetNextFriendOpportunityResult{Status: NextOpportunityFound, Friend: &card}, nil
	case friendshiprpcpb.NextOpportunityStatus_NEXT_OPPORTUNITY_STATUS_NONE:
		if response.GetFriend() != nil {
			return GetNextFriendOpportunityResult{}, fmt.Errorf("%w: NONE with card", ErrMalformedResponse)
		}
		return GetNextFriendOpportunityResult{Status: NextOpportunityNone}, nil
	case friendshiprpcpb.NextOpportunityStatus_NEXT_OPPORTUNITY_STATUS_UNKNOWN:
		if response.GetFriend() != nil {
			return GetNextFriendOpportunityResult{}, fmt.Errorf("%w: UNKNOWN with card", ErrMalformedResponse)
		}
		return GetNextFriendOpportunityResult{Status: NextOpportunityUnknown}, nil
	default:
		return GetNextFriendOpportunityResult{}, fmt.Errorf("%w: next opportunity status", ErrMalformedResponse)
	}
}

func (c *GRPCClient) AuthorizeVisit(ctx context.Context, input AuthorizeVisitInput) (AuthorizeVisitResult, error) {
	callCtx, cancel := c.callContext(ctx)
	defer cancel()
	response, err := c.client.AuthorizeVisit(callCtx, &friendshiprpcpb.AuthorizeVisitRequest{ViewerPlayerId: input.ViewerPlayerID,
		OwnerPlayerId: input.OwnerPlayerID, TargetFarmId: input.TargetFarmID, JoinOperationId: input.JoinOperationID,
		JoinAttemptId: input.JoinAttemptID, AuthorityNowMs: input.NowMs, AcceptedTtlMs: input.TTLMs})
	if err != nil {
		return AuthorizeVisitResult{}, clientRPCError(err, false)
	}
	if response == nil {
		return AuthorizeVisitResult{}, fmt.Errorf("%w: visit response", ErrMalformedResponse)
	}
	outcome, err := outcomeFromRPC(response.GetOutcome())
	if err != nil {
		return AuthorizeVisitResult{}, err
	}
	if outcome.Status == friend.StatusRejected && response.GetPermit() == nil {
		return AuthorizeVisitResult{Outcome: outcome}, nil
	}
	permitRPC := response.GetPermit()
	if permitRPC == nil || permitRPC.GetPermitId() == "" || permitRPC.GetViewerPlayerId() != input.ViewerPlayerID ||
		permitRPC.GetOwnerPlayerId() != input.OwnerPlayerID || permitRPC.GetTargetFarmId() != input.TargetFarmID ||
		permitRPC.GetRelationId() == "" || permitRPC.GetRelationEpoch() == 0 ||
		permitRPC.GetJoinOperationId() != input.JoinOperationID || permitRPC.GetJoinAttemptId() != input.JoinAttemptID ||
		permitRPC.GetExpireAtAuthorityMs() <= permitRPC.GetIssuedAtAuthorityMs() || len(permitRPC.GetPayloadHash()) == 0 {
		return AuthorizeVisitResult{}, fmt.Errorf("%w: visit permit", ErrMalformedResponse)
	}
	permit := friend.VisitPermitSnapshot{PermitID: permitRPC.GetPermitId(), ViewerPlayerID: permitRPC.GetViewerPlayerId(),
		OwnerPlayerID: permitRPC.GetOwnerPlayerId(), TargetFarmID: permitRPC.GetTargetFarmId(), RelationID: permitRPC.GetRelationId(),
		RelationEpoch: permitRPC.GetRelationEpoch(), JoinOperationID: permitRPC.GetJoinOperationId(), JoinAttemptID: permitRPC.GetJoinAttemptId(),
		Scope: friend.ActionScopeVisitFarm, IssuedAtAuthorityMs: permitRPC.GetIssuedAtAuthorityMs(),
		ExpireAtAuthorityMs: permitRPC.GetExpireAtAuthorityMs(), PayloadHash: append([]byte(nil), permitRPC.GetPayloadHash()...)}
	return AuthorizeVisitResult{Outcome: outcome, Permit: permit}, nil
}

func (c *GRPCClient) CommitFriendActionAdmission(ctx context.Context, command friend.ActionAdmissionCommand) (friend.ActionAdmissionOutcome, error) {
	callCtx, cancel := c.callContext(ctx)
	defer cancel()
	response, err := c.client.CommitFriendActionAdmission(callCtx, admissionCommandToRPC(command))
	if err != nil {
		return friend.ActionAdmissionOutcome{}, clientRPCError(err, false)
	}
	return admissionFromRPC(response, command)
}

func (c *GRPCClient) GetFriendActionAdmission(ctx context.Context, command friend.ActionAdmissionCommand) (friend.ActionAdmissionOutcome, error) {
	callCtx, cancel := c.callContext(ctx)
	defer cancel()
	response, err := c.client.GetFriendActionAdmission(callCtx,
		&friendshiprpcpb.GetFriendActionAdmissionRequest{Admission: admissionCommandToRPC(command)})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return friend.ActionAdmissionOutcome{}, fmt.Errorf("%w: %v", friend.ErrOutcomeNotFound, err)
		}
		return friend.ActionAdmissionOutcome{}, clientRPCError(err, false)
	}
	return admissionFromRPC(response, command)
}

func admissionFromRPC(response *friendshiprpcpb.FriendActionAdmissionResponse, command friend.ActionAdmissionCommand) (friend.ActionAdmissionOutcome, error) {
	if response == nil {
		return friend.ActionAdmissionOutcome{}, fmt.Errorf("%w: admission response", ErrMalformedResponse)
	}
	outcome, err := outcomeFromRPC(response.GetOutcome())
	if err != nil {
		return friend.ActionAdmissionOutcome{}, err
	}
	result := friend.ActionAdmissionOutcome{Outcome: friend.Outcome{Status: outcome.Status, Reason: outcome.Reason,
		Identity: outcome.Identity, CommittedEpoch: outcome.CommittedEpoch, CommittedAtMs: outcome.CommittedAtMs,
		Detail: outcome.Detail, PayloadHash: append([]byte(nil), outcome.PayloadHash...)},
		DispatchOrderID: response.GetDispatchOrderId(), WorkflowDeadlineAtMs: response.GetWorkflowDeadlineAtMs()}
	if result.Outcome.Identity.Key() != command.Identity.Key() || !bytes.Equal(result.Outcome.PayloadHash, command.PayloadHash) {
		return friend.ActionAdmissionOutcome{}, fmt.Errorf("%w: admission outcome identity", ErrMalformedResponse)
	}
	if outcome.Status == friend.StatusRejected && response.GetPermit() == nil {
		return result, nil
	}
	permit, err := actionPermitFromRPC(response.GetPermit())
	if err != nil {
		return friend.ActionAdmissionOutcome{}, err
	}
	if result.DispatchOrderID == "" || result.WorkflowDeadlineAtMs <= permit.ExpireAtAuthorityMs {
		return friend.ActionAdmissionOutcome{}, fmt.Errorf("%w: admission dispatch", ErrMalformedResponse)
	}
	result.Permit = permit
	return result, nil
}

func clientRPCError(err error, shareNotFound bool) error {
	if err == nil {
		return nil
	}
	switch status.Code(err) {
	case codes.InvalidArgument:
		return fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	case codes.NotFound:
		if shareNotFound {
			return fmt.Errorf("%w: %v", ErrShareLinkInvalid, err)
		}
		return fmt.Errorf("%w: %v", ErrNotFriend, err)
	case codes.AlreadyExists:
		return fmt.Errorf("%w: %v", friend.ErrIdempotentConflict, err)
	case codes.PermissionDenied:
		return fmt.Errorf("%w: %v", ErrShareSelfInvite, err)
	case codes.FailedPrecondition:
		return fmt.Errorf("%w: %v", friend.ErrRosterChanged, err)
	case codes.DataLoss:
		return fmt.Errorf("%w: %v", ErrMalformedResponse, err)
	case codes.ResourceExhausted:
		return fmt.Errorf("%w: %v", ErrCapacity, err)
	case codes.DeadlineExceeded:
		return fmt.Errorf("%w: %w: %v", ErrUnavailable, context.DeadlineExceeded, err)
	case codes.Canceled:
		return fmt.Errorf("%w: %w: %v", ErrUnavailable, context.Canceled, err)
	case codes.Unavailable, codes.Unknown:
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	default:
		if errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("%w: %w", ErrUnavailable, context.DeadlineExceeded)
		}
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
}
