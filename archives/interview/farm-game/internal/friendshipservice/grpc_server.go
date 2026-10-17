package friendshipservice

import (
	"context"
	"errors"

	friendshiprpcpb "farmgame/api/gen/friendshiprpc"
	"farmgame/internal/friend"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RPCServer struct {
	friendshiprpcpb.UnimplementedFriendshipServiceServer
	service Client
}

func NewRPCServer(service Client) (*RPCServer, error) {
	if service == nil {
		return nil, ErrInvalidArgument
	}
	return &RPCServer{service: service}, nil
}

func (s *RPCServer) CreateShare(ctx context.Context, request *friendshiprpcpb.CreateShareRequest) (*friendshiprpcpb.CreateShareResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	result, err := s.service.CreateShare(ctx, CreateShareInput{SubjectPlayerID: request.GetSubjectPlayerId(),
		OperationID: request.GetOperationId(), ObservationOnly: request.GetObservationOnly()})
	if err != nil {
		return nil, rpcStatus(err)
	}
	return &friendshiprpcpb.CreateShareResponse{Outcome: outcomeToRPC(result.Outcome), ShareToken: result.ShareToken,
		AcceptMode: result.AcceptMode, TokenGeneration: result.TokenGeneration, IssuedAtMs: result.IssuedAtMs,
		ExpiresAtMs: result.ExpiresAtMs}, nil
}

func (s *RPCServer) ResolveShare(ctx context.Context, request *friendshiprpcpb.ResolveShareRequest) (*friendshiprpcpb.ResolveShareResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	result, err := s.service.ResolveShare(ctx, ResolveShareInput{ShareToken: request.GetShareToken()})
	if err != nil {
		return nil, rpcStatus(err)
	}
	return &friendshiprpcpb.ResolveShareResponse{InviterPlayerId: result.InviterPlayerID,
		InviterProfile: profileToRPC(result.InviterProfile), AcceptMode: result.AcceptMode}, nil
}

func (s *RPCServer) RedeemShare(ctx context.Context, request *friendshiprpcpb.RedeemShareRequest) (*friendshiprpcpb.RedeemShareResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	result, err := s.service.RedeemShare(ctx, RedeemShareInput{RedeemerPlayerID: request.GetRedeemerPlayerId(),
		ShareToken: request.GetShareToken(), OperationID: request.GetOperationId(), ObservationOnly: request.GetObservationOnly()})
	if err != nil {
		return nil, rpcStatus(err)
	}
	response := &friendshiprpcpb.RedeemShareResponse{Outcome: outcomeToRPC(result.Outcome),
		InviterPlayerId: result.InviterPlayerID, InviterProfile: profileToRPC(result.InviterProfile)}
	// Deterministic business rejections can precede creation or lookup of any
	// Pair fact (for example, a full friend quota). Do not serialize the zero
	// PairState as though it were canonical. Successful and no-change results
	// always carry a Pair and the client validates it.
	if result.Outcome.Status != friend.StatusRejected || result.Pair.RelationID != "" || result.Pair.RelationEpoch != 0 ||
		result.Pair.Status != friend.RelationStatusNone {
		response.Pair = pairToRPC(result.Pair)
	}
	return response, nil
}

func (s *RPCServer) GetFriendList(ctx context.Context, request *friendshiprpcpb.GetFriendListRequest) (*friendshiprpcpb.GetFriendListResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	result, err := s.service.GetFriendList(ctx, GetFriendListInput{ViewerPlayerID: request.GetViewerPlayerId(),
		Cursor: request.GetCursor(), PageSize: int(request.GetPageSize())})
	if err != nil {
		return nil, rpcStatus(err)
	}
	response := &friendshiprpcpb.GetFriendListResponse{NextCursor: result.Page.NextCursor,
		RosterRevision: result.Page.RosterRevision, ValidatedAtMs: result.Page.ValidatedAtMs}
	for _, card := range result.Page.Friends {
		response.Friends = append(response.Friends, cardToRPC(card))
	}
	return response, nil
}

func (s *RPCServer) DeleteFriend(ctx context.Context, request *friendshiprpcpb.DeleteFriendRequest) (*friendshiprpcpb.PairMutationResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	return s.pairMutation(ctx, PairMutationInput{CallerPlayerID: request.GetCallerPlayerId(), PeerPlayerID: request.GetPeerPlayerId(),
		OperationID: request.GetOperationId(), ObservationOnly: request.GetObservationOnly()}, s.service.DeleteFriend)
}

func (s *RPCServer) BlockFriend(ctx context.Context, request *friendshiprpcpb.BlockFriendRequest) (*friendshiprpcpb.PairMutationResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	return s.pairMutation(ctx, PairMutationInput{CallerPlayerID: request.GetCallerPlayerId(), PeerPlayerID: request.GetPeerPlayerId(),
		OperationID: request.GetOperationId(), ObservationOnly: request.GetObservationOnly()}, s.service.BlockFriend)
}

func (s *RPCServer) UnblockFriend(ctx context.Context, request *friendshiprpcpb.UnblockFriendRequest) (*friendshiprpcpb.PairMutationResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	return s.pairMutation(ctx, PairMutationInput{CallerPlayerID: request.GetCallerPlayerId(), PeerPlayerID: request.GetPeerPlayerId(),
		OperationID: request.GetOperationId(), ObservationOnly: request.GetObservationOnly()}, s.service.UnblockFriend)
}

func (s *RPCServer) pairMutation(ctx context.Context, input PairMutationInput,
	call func(context.Context, PairMutationInput) (PairMutationResult, error)) (*friendshiprpcpb.PairMutationResponse, error) {
	result, err := call(ctx, input)
	if err != nil {
		return nil, rpcStatus(err)
	}
	return &friendshiprpcpb.PairMutationResponse{Outcome: outcomeToRPC(result.Outcome), Pair: pairToRPC(result.Pair)}, nil
}

func (s *RPCServer) GetNextFriendOpportunity(ctx context.Context, request *friendshiprpcpb.GetNextFriendOpportunityRequest) (*friendshiprpcpb.GetNextFriendOpportunityResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	hintedTargets := make([]FriendOpportunityHintedTarget, 0, len(request.GetHintedTargets()))
	for _, target := range request.GetHintedTargets() {
		if target == nil {
			return nil, status.Error(codes.InvalidArgument, "hinted target required")
		}
		hintedTargets = append(hintedTargets, FriendOpportunityHintedTarget{
			OwnerID: target.GetOwnerId(), OpportunityRevision: target.GetOpportunityRevision(),
		})
	}
	result, err := s.service.GetNextFriendOpportunity(ctx, GetNextFriendOpportunityInput{
		ViewerPlayerID: request.GetViewerPlayerId(), HintedTargets: hintedTargets,
	})
	if err != nil {
		return nil, rpcStatus(err)
	}
	response := &friendshiprpcpb.GetNextFriendOpportunityResponse{}
	switch result.Status {
	case NextOpportunityFound:
		response.Status = friendshiprpcpb.NextOpportunityStatus_NEXT_OPPORTUNITY_STATUS_FOUND
		if result.Friend != nil {
			response.Friend = cardToRPC(*result.Friend)
		}
	case NextOpportunityNone:
		response.Status = friendshiprpcpb.NextOpportunityStatus_NEXT_OPPORTUNITY_STATUS_NONE
	case NextOpportunityUnknown:
		response.Status = friendshiprpcpb.NextOpportunityStatus_NEXT_OPPORTUNITY_STATUS_UNKNOWN
	default:
		return nil, status.Error(codes.DataLoss, "next opportunity status invalid")
	}
	return response, nil
}

func (s *RPCServer) AuthorizeVisit(ctx context.Context, request *friendshiprpcpb.AuthorizeVisitRequest) (*friendshiprpcpb.AuthorizeVisitResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	result, err := s.service.AuthorizeVisit(ctx, AuthorizeVisitInput{ViewerPlayerID: request.GetViewerPlayerId(),
		OwnerPlayerID: request.GetOwnerPlayerId(), TargetFarmID: request.GetTargetFarmId(),
		JoinOperationID: request.GetJoinOperationId(), JoinAttemptID: request.GetJoinAttemptId(),
		NowMs: request.GetAuthorityNowMs(), TTLMs: request.GetAcceptedTtlMs()})
	if err != nil {
		return nil, rpcStatus(err)
	}
	response := &friendshiprpcpb.AuthorizeVisitResponse{Outcome: outcomeToRPC(result.Outcome)}
	if result.Permit.PermitID != "" {
		response.Permit = &friendshiprpcpb.VisitPermit{PermitId: result.Permit.PermitID,
			ViewerPlayerId: result.Permit.ViewerPlayerID, OwnerPlayerId: result.Permit.OwnerPlayerID,
			TargetFarmId: result.Permit.TargetFarmID, RelationId: result.Permit.RelationID,
			RelationEpoch: result.Permit.RelationEpoch, JoinOperationId: result.Permit.JoinOperationID,
			JoinAttemptId: result.Permit.JoinAttemptID, IssuedAtAuthorityMs: result.Permit.IssuedAtAuthorityMs,
			ExpireAtAuthorityMs: result.Permit.ExpireAtAuthorityMs, PayloadHash: append([]byte(nil), result.Permit.PayloadHash...)}
	}
	return response, nil
}

func (s *RPCServer) CommitFriendActionAdmission(ctx context.Context, request *friendshiprpcpb.CommitFriendActionAdmissionRequest) (*friendshiprpcpb.FriendActionAdmissionResponse, error) {
	command, err := admissionCommandFromRPC(request)
	if err != nil {
		return nil, rpcStatus(err)
	}
	result, err := s.service.CommitFriendActionAdmission(ctx, command)
	if err != nil {
		return nil, rpcStatus(err)
	}
	return admissionToRPC(result), nil
}

func (s *RPCServer) GetFriendActionAdmission(ctx context.Context, request *friendshiprpcpb.GetFriendActionAdmissionRequest) (*friendshiprpcpb.FriendActionAdmissionResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	command, err := admissionCommandFromRPC(request.GetAdmission())
	if err != nil {
		return nil, rpcStatus(err)
	}
	result, err := s.service.GetFriendActionAdmission(ctx, command)
	if err != nil {
		return nil, rpcStatus(err)
	}
	return admissionToRPC(result), nil
}

func admissionToRPC(result friend.ActionAdmissionOutcome) *friendshiprpcpb.FriendActionAdmissionResponse {
	response := &friendshiprpcpb.FriendActionAdmissionResponse{Outcome: outcomeToRPC(outcomeFromDomain(result.Outcome)),
		DispatchOrderId: result.DispatchOrderID, WorkflowDeadlineAtMs: result.WorkflowDeadlineAtMs}
	if result.Permit.PermitID != "" {
		response.Permit = actionPermitToRPC(result.Permit)
	}
	return response
}

func rpcStatus(err error) error {
	if err == nil {
		return nil
	}
	if existing, ok := status.FromError(err); ok && existing.Code() != codes.Unknown {
		return existing.Err()
	}
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, ErrInvalidArgument), errors.Is(err, friend.ErrInvalidCommand):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, ErrShareLinkInvalid), errors.Is(err, ErrNotFriend), errors.Is(err, friend.ErrNotFound),
		errors.Is(err, friend.ErrOutcomeNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ErrShareSelfInvite):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, friend.ErrIdempotentConflict):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, friend.ErrRosterChanged), errors.Is(err, friend.ErrInvalidCursor):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, ErrMalformedResponse), errors.Is(err, friend.ErrFriendReconcile):
		return status.Error(codes.DataLoss, err.Error())
	default:
		return status.Error(codes.Unavailable, err.Error())
	}
}
