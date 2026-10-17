package friendshipservice

import (
	"context"
	"errors"
	"testing"
	"time"

	friendshiprpcpb "farmgame/api/gen/friendshiprpc"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/friend"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type mappingApplication struct {
	calls        map[string]int
	observed     map[string]bool
	createErr    error
	redeemErr    error
	redeemResult *RedeemShareResult
	block        bool
}

func newMappingApplication() *mappingApplication {
	return &mappingApplication{calls: make(map[string]int), observed: make(map[string]bool)}
}

func mappingIdentity() friend.CommitIdentity {
	return friend.CommitIdentity{SubjectPlayerID: 1, CommandType: "CARE", OperationID: "operation"}
}

func mappingOutcome() Outcome {
	return Outcome{Status: friend.StatusSucceeded, Identity: mappingIdentity(), PayloadHash: []byte("payload"),
		CommittedEpoch: 3, CommittedAtMs: 10}
}

func mappingProfile() *publicpb.PublicProfileView {
	return &publicpb.PublicProfileView{PlayerId: 2, DisplayName: "friend", AvatarId: 7, Level: 4}
}

func mappingCard() friend.FriendCard {
	return friend.FriendCard{FriendPlayerID: 2, PublicProfile: mappingProfile(), RelationID: "relation", RelationEpoch: 3,
		Action: friend.ActionCanWater | friend.ActionCanVisit, OpportunityStatus: friend.OpportunityReadReady,
		OpportunityRevision: 9, ValidatedAtMs: 11}
}

func mappingPair(changed bool) PairState {
	return PairState{RelationID: "relation", RelationEpoch: 3, Status: friend.RelationStatusActive, Changed: changed}
}

func mappingAdmission() friend.ActionAdmissionOutcome {
	identity := mappingIdentity()
	return friend.ActionAdmissionOutcome{Outcome: friend.Outcome{Status: friend.StatusSucceeded, Identity: identity,
		PayloadHash: []byte("payload"), CommittedEpoch: 3, CommittedAtMs: 10},
		Permit: friend.ActionPermitSnapshot{PermitID: "action-permit", RelationID: "relation", RelationEpoch: 3,
			VisitorPlayerID: 1, OwnerPlayerID: 2, TargetFarmID: 2, OperationID: identity.OperationID,
			Scope: friend.ActionScopeCare, PayloadHash: []byte("payload"), TargetBindingHash: []byte("binding"),
			IssuedAtAuthorityMs: 10, ExpireAtAuthorityMs: 20, ExecutionAcceptedAtAuthorityMs: 10},
		DispatchOrderID: "dispatch", WorkflowDeadlineAtMs: 100}
}

func mappingAdmissionCommand() friend.ActionAdmissionCommand {
	return friend.ActionAdmissionCommand{Identity: mappingIdentity(), PayloadHash: []byte("payload"),
		VisitorPlayerID: 1, OwnerPlayerID: 2, TargetFarmID: 2, Scope: friend.ActionScopeCare,
		TargetBindingHash: []byte("binding"), NowMs: 10, PermitTTLMs: 10, WorkflowTTLMs: 90}
}

func (a *mappingApplication) called(method string) { a.calls[method]++ }

func (a *mappingApplication) CreateShare(ctx context.Context, input CreateShareInput) (CreateShareResult, error) {
	a.called("create")
	if a.block {
		<-ctx.Done()
		return CreateShareResult{}, ctx.Err()
	}
	if a.createErr != nil {
		return CreateShareResult{}, a.createErr
	}
	if input.SubjectPlayerID != 1 || input.OperationID != "operation" {
		return CreateShareResult{}, ErrInvalidArgument
	}
	a.observed["create"] = input.ObservationOnly
	return CreateShareResult{Outcome: mappingOutcome(), ShareToken: "token", AcceptMode: ShareAcceptModeMulti,
		TokenGeneration: 3, IssuedAtMs: 10, ExpiresAtMs: 20}, nil
}
func (a *mappingApplication) ResolveShare(_ context.Context, input ResolveShareInput) (ResolveShareResult, error) {
	a.called("resolve")
	if input.ShareToken != "token" {
		return ResolveShareResult{}, ErrInvalidArgument
	}
	return ResolveShareResult{InviterPlayerID: 2, InviterProfile: mappingProfile(), AcceptMode: ShareAcceptModeMulti}, nil
}
func (a *mappingApplication) RedeemShare(_ context.Context, input RedeemShareInput) (RedeemShareResult, error) {
	a.called("redeem")
	if a.redeemErr != nil {
		return RedeemShareResult{}, a.redeemErr
	}
	if input.RedeemerPlayerID != 1 || input.ShareToken != "token" || input.OperationID != "operation" {
		return RedeemShareResult{}, ErrInvalidArgument
	}
	a.observed["redeem"] = input.ObservationOnly
	if a.redeemResult != nil {
		return *a.redeemResult, nil
	}
	return RedeemShareResult{Outcome: mappingOutcome(), Pair: mappingPair(true), InviterPlayerID: 2, InviterProfile: mappingProfile()}, nil
}
func (a *mappingApplication) GetFriendList(_ context.Context, input GetFriendListInput) (GetFriendListResult, error) {
	a.called("list")
	if input.ViewerPlayerID != 1 || input.Cursor != "cursor" || input.PageSize != 20 {
		return GetFriendListResult{}, ErrInvalidArgument
	}
	return GetFriendListResult{Page: friend.FriendPage{Friends: []friend.FriendCard{mappingCard()}, NextCursor: "next",
		RosterRevision: 5, ValidatedAtMs: 11}}, nil
}
func (a *mappingApplication) DeleteFriend(_ context.Context, input PairMutationInput) (PairMutationResult, error) {
	a.called("delete")
	a.observed["delete"] = input.ObservationOnly
	return mappingPairMutation(input)
}
func (a *mappingApplication) BlockFriend(_ context.Context, input PairMutationInput) (PairMutationResult, error) {
	a.called("block")
	a.observed["block"] = input.ObservationOnly
	return mappingPairMutation(input)
}
func (a *mappingApplication) UnblockFriend(_ context.Context, input PairMutationInput) (PairMutationResult, error) {
	a.called("unblock")
	a.observed["unblock"] = input.ObservationOnly
	return mappingPairMutation(input)
}
func mappingPairMutation(input PairMutationInput) (PairMutationResult, error) {
	if input.CallerPlayerID != 1 || input.PeerPlayerID != 2 || input.OperationID != "operation" {
		return PairMutationResult{}, ErrInvalidArgument
	}
	return PairMutationResult{Outcome: mappingOutcome(), Pair: mappingPair(true)}, nil
}
func (a *mappingApplication) GetNextFriendOpportunity(_ context.Context, input GetNextFriendOpportunityInput) (GetNextFriendOpportunityResult, error) {
	a.called("next")
	if input.ViewerPlayerID != 1 {
		return GetNextFriendOpportunityResult{}, ErrInvalidArgument
	}
	card := mappingCard()
	return GetNextFriendOpportunityResult{Status: NextOpportunityFound, Friend: &card}, nil
}
func (a *mappingApplication) AuthorizeVisit(_ context.Context, input AuthorizeVisitInput) (AuthorizeVisitResult, error) {
	a.called("visit")
	if input.ViewerPlayerID != 1 || input.OwnerPlayerID != 2 || input.TargetFarmID != 2 ||
		input.JoinOperationID != "join" || input.JoinAttemptID != "attempt" || input.NowMs != 10 || input.TTLMs != 10 {
		return AuthorizeVisitResult{}, ErrInvalidArgument
	}
	return AuthorizeVisitResult{Outcome: Outcome{Status: friend.StatusSucceeded, CommittedEpoch: 3},
		Permit: friend.VisitPermitSnapshot{PermitID: "visit-permit", ViewerPlayerID: 1, OwnerPlayerID: 2,
			TargetFarmID: 2, RelationID: "relation", RelationEpoch: 3, JoinOperationID: "join", JoinAttemptID: "attempt",
			Scope: friend.ActionScopeVisitFarm, IssuedAtAuthorityMs: 10, ExpireAtAuthorityMs: 20, PayloadHash: []byte("visit")}}, nil
}
func (a *mappingApplication) CommitFriendActionAdmission(_ context.Context, command friend.ActionAdmissionCommand) (friend.ActionAdmissionOutcome, error) {
	a.called("commit_admission")
	if command.Identity.Key() != mappingIdentity().Key() || string(command.PayloadHash) != "payload" || command.Scope != friend.ActionScopeCare {
		return friend.ActionAdmissionOutcome{}, ErrInvalidArgument
	}
	return mappingAdmission(), nil
}
func (a *mappingApplication) GetFriendActionAdmission(_ context.Context, command friend.ActionAdmissionCommand) (friend.ActionAdmissionOutcome, error) {
	a.called("get_admission")
	if command.Identity.Key() != mappingIdentity().Key() || string(command.PayloadHash) != "payload" {
		return friend.ActionAdmissionOutcome{}, ErrInvalidArgument
	}
	return mappingAdmission(), nil
}

type loopbackRPCClient struct {
	server         *RPCServer
	transportErr   error
	malformed      bool
	redeemResponse *friendshiprpcpb.RedeemShareResponse
}

type semanticMappingApplication struct {
	*mappingApplication
	rejectAdmission bool
	nextStatus      NextOpportunityStatus
}

func (a *semanticMappingApplication) CommitFriendActionAdmission(_ context.Context, command friend.ActionAdmissionCommand) (friend.ActionAdmissionOutcome, error) {
	if !a.rejectAdmission {
		return a.mappingApplication.CommitFriendActionAdmission(context.Background(), command)
	}
	return friend.ActionAdmissionOutcome{Outcome: friend.Outcome{Status: friend.StatusRejected,
		Reason: friend.ReasonVisitNotPermitted, Identity: command.Identity, PayloadHash: append([]byte(nil), command.PayloadHash...)}}, nil
}

func (a *semanticMappingApplication) GetFriendActionAdmission(_ context.Context, command friend.ActionAdmissionCommand) (friend.ActionAdmissionOutcome, error) {
	return a.CommitFriendActionAdmission(context.Background(), command)
}

func (a *semanticMappingApplication) GetNextFriendOpportunity(context.Context, GetNextFriendOpportunityInput) (GetNextFriendOpportunityResult, error) {
	return GetNextFriendOpportunityResult{Status: a.nextStatus}, nil
}

func (c *loopbackRPCClient) CreateShare(ctx context.Context, in *friendshiprpcpb.CreateShareRequest, _ ...grpc.CallOption) (*friendshiprpcpb.CreateShareResponse, error) {
	if c.transportErr != nil {
		return nil, c.transportErr
	}
	if c.malformed {
		return &friendshiprpcpb.CreateShareResponse{}, nil
	}
	return c.server.CreateShare(ctx, in)
}
func (c *loopbackRPCClient) ResolveShare(ctx context.Context, in *friendshiprpcpb.ResolveShareRequest, _ ...grpc.CallOption) (*friendshiprpcpb.ResolveShareResponse, error) {
	return c.server.ResolveShare(ctx, in)
}
func (c *loopbackRPCClient) RedeemShare(ctx context.Context, in *friendshiprpcpb.RedeemShareRequest, _ ...grpc.CallOption) (*friendshiprpcpb.RedeemShareResponse, error) {
	if c.redeemResponse != nil {
		return c.redeemResponse, nil
	}
	return c.server.RedeemShare(ctx, in)
}
func (c *loopbackRPCClient) GetFriendList(ctx context.Context, in *friendshiprpcpb.GetFriendListRequest, _ ...grpc.CallOption) (*friendshiprpcpb.GetFriendListResponse, error) {
	return c.server.GetFriendList(ctx, in)
}
func (c *loopbackRPCClient) DeleteFriend(ctx context.Context, in *friendshiprpcpb.DeleteFriendRequest, _ ...grpc.CallOption) (*friendshiprpcpb.PairMutationResponse, error) {
	return c.server.DeleteFriend(ctx, in)
}
func (c *loopbackRPCClient) BlockFriend(ctx context.Context, in *friendshiprpcpb.BlockFriendRequest, _ ...grpc.CallOption) (*friendshiprpcpb.PairMutationResponse, error) {
	return c.server.BlockFriend(ctx, in)
}
func (c *loopbackRPCClient) UnblockFriend(ctx context.Context, in *friendshiprpcpb.UnblockFriendRequest, _ ...grpc.CallOption) (*friendshiprpcpb.PairMutationResponse, error) {
	return c.server.UnblockFriend(ctx, in)
}
func (c *loopbackRPCClient) GetNextFriendOpportunity(ctx context.Context, in *friendshiprpcpb.GetNextFriendOpportunityRequest, _ ...grpc.CallOption) (*friendshiprpcpb.GetNextFriendOpportunityResponse, error) {
	return c.server.GetNextFriendOpportunity(ctx, in)
}
func (c *loopbackRPCClient) AuthorizeVisit(ctx context.Context, in *friendshiprpcpb.AuthorizeVisitRequest, _ ...grpc.CallOption) (*friendshiprpcpb.AuthorizeVisitResponse, error) {
	return c.server.AuthorizeVisit(ctx, in)
}
func (c *loopbackRPCClient) CommitFriendActionAdmission(ctx context.Context, in *friendshiprpcpb.CommitFriendActionAdmissionRequest, _ ...grpc.CallOption) (*friendshiprpcpb.FriendActionAdmissionResponse, error) {
	return c.server.CommitFriendActionAdmission(ctx, in)
}
func (c *loopbackRPCClient) GetFriendActionAdmission(ctx context.Context, in *friendshiprpcpb.GetFriendActionAdmissionRequest, _ ...grpc.CallOption) (*friendshiprpcpb.FriendActionAdmissionResponse, error) {
	return c.server.GetFriendActionAdmission(ctx, in)
}

func newLoopbackClient(t *testing.T, application Client, timeout time.Duration) (*GRPCClient, *mappingApplication) {
	t.Helper()
	server, err := NewRPCServer(application)
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewGRPCClient(&loopbackRPCClient{server: server}, timeout)
	if err != nil {
		t.Fatal(err)
	}
	app, _ := application.(*mappingApplication)
	return client, app
}

func TestAllFriendshipRPCMethodMappings(t *testing.T) {
	application := newMappingApplication()
	client, _ := newLoopbackClient(t, application, time.Second)
	ctx := context.Background()
	if got, err := client.CreateShare(ctx, CreateShareInput{SubjectPlayerID: 1, OperationID: "operation"}); err != nil || got.ShareToken != "token" {
		t.Fatalf("CreateShare=%+v err=%v", got, err)
	}
	if got, err := client.ResolveShare(ctx, ResolveShareInput{ShareToken: "token"}); err != nil || got.InviterPlayerID != 2 {
		t.Fatalf("ResolveShare=%+v err=%v", got, err)
	}
	if got, err := client.RedeemShare(ctx, RedeemShareInput{RedeemerPlayerID: 1, ShareToken: "token", OperationID: "operation"}); err != nil || got.Pair.RelationID != "relation" {
		t.Fatalf("RedeemShare=%+v err=%v", got, err)
	}
	if got, err := client.GetFriendList(ctx, GetFriendListInput{ViewerPlayerID: 1, Cursor: "cursor", PageSize: 20}); err != nil || len(got.Page.Friends) != 1 {
		t.Fatalf("GetFriendList=%+v err=%v", got, err)
	}
	mutation := PairMutationInput{CallerPlayerID: 1, PeerPlayerID: 2, OperationID: "operation"}
	for name, call := range map[string]func(context.Context, PairMutationInput) (PairMutationResult, error){
		"delete": client.DeleteFriend, "block": client.BlockFriend, "unblock": client.UnblockFriend,
	} {
		if got, err := call(ctx, mutation); err != nil || got.Pair.RelationEpoch != 3 {
			t.Fatalf("%s=%+v err=%v", name, got, err)
		}
	}
	if got, err := client.GetNextFriendOpportunity(ctx, GetNextFriendOpportunityInput{ViewerPlayerID: 1}); err != nil || got.Status != NextOpportunityFound {
		t.Fatalf("GetNextFriendOpportunity=%+v err=%v", got, err)
	}
	if got, err := client.AuthorizeVisit(ctx, AuthorizeVisitInput{ViewerPlayerID: 1, OwnerPlayerID: 2, TargetFarmID: 2,
		JoinOperationID: "join", JoinAttemptID: "attempt", NowMs: 10, TTLMs: 10}); err != nil || got.Permit.PermitID != "visit-permit" {
		t.Fatalf("AuthorizeVisit=%+v err=%v", got, err)
	}
	command := mappingAdmissionCommand()
	if got, err := client.CommitFriendActionAdmission(ctx, command); err != nil || got.DispatchOrderID != "dispatch" {
		t.Fatalf("CommitFriendActionAdmission=%+v err=%v", got, err)
	}
	if got, err := client.GetFriendActionAdmission(ctx, command); err != nil || got.Permit.PermitID != "action-permit" {
		t.Fatalf("GetFriendActionAdmission=%+v err=%v", got, err)
	}
	for _, method := range []string{"create", "resolve", "redeem", "list", "delete", "block", "unblock", "next", "visit", "commit_admission", "get_admission"} {
		if application.calls[method] != 1 {
			t.Fatalf("method %s calls=%d", method, application.calls[method])
		}
	}
}

func TestFriendshipRPCMapsObservationOnlyEndToEnd(t *testing.T) {
	application := newMappingApplication()
	client, _ := newLoopbackClient(t, application, time.Second)
	ctx := context.Background()
	_, _ = client.CreateShare(ctx, CreateShareInput{SubjectPlayerID: 1, OperationID: "operation", ObservationOnly: true})
	_, _ = client.RedeemShare(ctx, RedeemShareInput{RedeemerPlayerID: 1, ShareToken: "token", OperationID: "operation", ObservationOnly: true})
	mutation := PairMutationInput{CallerPlayerID: 1, PeerPlayerID: 2, OperationID: "operation", ObservationOnly: true}
	_, _ = client.DeleteFriend(ctx, mutation)
	_, _ = client.BlockFriend(ctx, mutation)
	_, _ = client.UnblockFriend(ctx, mutation)
	for _, method := range []string{"create", "redeem", "delete", "block", "unblock"} {
		if !application.observed[method] {
			t.Fatalf("%s observation_only was not mapped", method)
		}
	}
}

func TestFriendshipRPCCanonicalErrorMappings(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		code codes.Code
	}{
		{name: "invalid", err: ErrInvalidArgument, code: codes.InvalidArgument},
		{name: "not found", err: ErrShareLinkInvalid, code: codes.NotFound},
		{name: "self invite", err: ErrShareSelfInvite, code: codes.PermissionDenied},
		{name: "idempotency", err: friend.ErrIdempotentConflict, code: codes.AlreadyExists},
		{name: "malformed", err: ErrMalformedResponse, code: codes.DataLoss},
		{name: "unavailable", err: errors.New("redis unavailable"), code: codes.Unavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			application := newMappingApplication()
			application.createErr = test.err
			server, _ := NewRPCServer(application)
			_, err := server.CreateShare(context.Background(), &friendshiprpcpb.CreateShareRequest{SubjectPlayerId: 1, OperationId: "operation"})
			if status.Code(err) != test.code {
				t.Fatalf("status=%v err=%v, want %v", status.Code(err), err, test.code)
			}
		})
	}
}

func TestFriendshipRPCPreservesRejectedAndUnknownSemantics(t *testing.T) {
	application := &semanticMappingApplication{mappingApplication: newMappingApplication(), rejectAdmission: true,
		nextStatus: NextOpportunityUnknown}
	client, _ := newLoopbackClient(t, application, time.Second)
	admission, err := client.CommitFriendActionAdmission(context.Background(), mappingAdmissionCommand())
	if err != nil || admission.Outcome.Status != friend.StatusRejected || admission.Outcome.Reason != friend.ReasonVisitNotPermitted ||
		admission.Permit.PermitID != "" {
		t.Fatalf("rejected admission=%+v err=%v", admission, err)
	}
	next, err := client.GetNextFriendOpportunity(context.Background(), GetNextFriendOpportunityInput{ViewerPlayerID: 1})
	if err != nil || next.Status != NextOpportunityUnknown || next.Friend != nil {
		t.Fatalf("UNKNOWN must not become NONE: next=%+v err=%v", next, err)
	}
}

func TestFriendshipRPCPreservesShareSelfInviteError(t *testing.T) {
	application := newMappingApplication()
	application.redeemErr = ErrShareSelfInvite
	client, _ := newLoopbackClient(t, application, time.Second)
	_, err := client.RedeemShare(context.Background(), RedeemShareInput{
		RedeemerPlayerID: 1, ShareToken: "token", OperationID: "operation",
	})
	if !errors.Is(err, ErrShareSelfInvite) {
		t.Fatalf("self-invite error=%v", err)
	}
}

func TestFriendshipRPCRedeemRejectedWithoutPairAndNoChangeWithPair(t *testing.T) {
	t.Run("friend limit rejection has no pair", func(t *testing.T) {
		application := newMappingApplication()
		application.redeemResult = &RedeemShareResult{
			Outcome:         Outcome{Status: friend.StatusRejected, Reason: friend.ReasonFriendLimitReached},
			InviterPlayerID: 2,
			InviterProfile:  mappingProfile(),
		}
		server, err := NewRPCServer(application)
		if err != nil {
			t.Fatal(err)
		}
		wire, err := server.RedeemShare(context.Background(), &friendshiprpcpb.RedeemShareRequest{
			RedeemerPlayerId: 1, ShareToken: "token", OperationId: "operation",
		})
		if err != nil {
			t.Fatal(err)
		}
		if wire.GetPair() != nil {
			t.Fatalf("rejected response serialized a zero Pair: %+v", wire.GetPair())
		}
		client, _ := newLoopbackClient(t, application, time.Second)
		got, err := client.RedeemShare(context.Background(), RedeemShareInput{
			RedeemerPlayerID: 1, ShareToken: "token", OperationID: "operation",
		})
		if err != nil || got.Outcome.Status != friend.StatusRejected || got.Outcome.Reason != friend.ReasonFriendLimitReached ||
			got.Pair != (PairState{}) || got.InviterPlayerID != 2 || got.InviterProfile.GetPlayerId() != 2 {
			t.Fatalf("rejected redeem=%+v err=%v", got, err)
		}
	})

	t.Run("rejected zero pair field is malformed", func(t *testing.T) {
		transport := &loopbackRPCClient{redeemResponse: &friendshiprpcpb.RedeemShareResponse{
			Outcome: outcomeToRPC(Outcome{Status: friend.StatusRejected, Reason: friend.ReasonFriendLimitReached}),
			Pair:    pairToRPC(PairState{}), InviterPlayerId: 2, InviterProfile: profileToRPC(mappingProfile()),
		}}
		client, err := NewGRPCClient(transport, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		_, err = client.RedeemShare(context.Background(), RedeemShareInput{
			RedeemerPlayerID: 1, ShareToken: "token", OperationID: "operation",
		})
		if !errors.Is(err, ErrMalformedResponse) {
			t.Fatalf("rejected zero Pair error=%v", err)
		}
	})

	t.Run("no change retains canonical pair", func(t *testing.T) {
		application := newMappingApplication()
		application.redeemResult = &RedeemShareResult{
			Outcome:         Outcome{Status: friend.StatusNoChange, Reason: friend.ReasonAlreadyFriends, CommittedEpoch: 3},
			Pair:            mappingPair(false),
			InviterPlayerID: 2,
			InviterProfile:  mappingProfile(),
		}
		client, _ := newLoopbackClient(t, application, time.Second)
		got, err := client.RedeemShare(context.Background(), RedeemShareInput{
			RedeemerPlayerID: 1, ShareToken: "token", OperationID: "operation",
		})
		if err != nil || got.Outcome.Status != friend.StatusNoChange || got.Pair.RelationID != "relation" ||
			got.Pair.RelationEpoch != 3 || got.Pair.Status != friend.RelationStatusActive || got.Pair.Changed {
			t.Fatalf("no-change redeem=%+v err=%v", got, err)
		}
	})

	for _, test := range []struct {
		name   string
		status friend.StatusClass
	}{
		{name: "succeeded", status: friend.StatusSucceeded},
		{name: "no change", status: friend.StatusNoChange},
	} {
		t.Run("non-rejected outcome requires pair "+test.name, func(t *testing.T) {
			application := newMappingApplication()
			application.redeemResult = &RedeemShareResult{
				Outcome: Outcome{Status: test.status}, InviterPlayerID: 2, InviterProfile: mappingProfile(),
			}
			client, _ := newLoopbackClient(t, application, time.Second)
			_, err := client.RedeemShare(context.Background(), RedeemShareInput{
				RedeemerPlayerID: 1, ShareToken: "token", OperationID: "operation",
			})
			if !errors.Is(err, ErrMalformedResponse) {
				t.Fatalf("status=%v missing Pair error=%v", test.status, err)
			}
		})
	}
}

func TestFriendshipRPCConnectionLossTimeoutAndMalformedResponse(t *testing.T) {
	application := newMappingApplication()
	server, _ := NewRPCServer(application)

	lost, _ := NewGRPCClient(&loopbackRPCClient{server: server, transportErr: status.Error(codes.Unavailable, "lost")}, time.Second)
	if _, err := lost.CreateShare(context.Background(), CreateShareInput{SubjectPlayerID: 1, OperationID: "operation"}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("connection loss err=%v", err)
	}

	application.block = true
	timed, _ := NewGRPCClient(&loopbackRPCClient{server: server}, time.Millisecond)
	if _, err := timed.CreateShare(context.Background(), CreateShareInput{SubjectPlayerID: 1, OperationID: "operation"}); !errors.Is(err, ErrUnavailable) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout err=%v", err)
	}
	application.block = false

	malformed, _ := NewGRPCClient(&loopbackRPCClient{server: server, malformed: true}, time.Second)
	if _, err := malformed.CreateShare(context.Background(), CreateShareInput{SubjectPlayerID: 1, OperationID: "operation"}); !errors.Is(err, ErrMalformedResponse) {
		t.Fatalf("malformed response err=%v", err)
	}
}
