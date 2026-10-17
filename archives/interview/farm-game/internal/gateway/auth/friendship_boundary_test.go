package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	publicpb "farmgame/api/gen/public"
	"farmgame/internal/friend"
	"farmgame/internal/friendshipservice"
)

type remoteBoundaryClient struct {
	calls         map[string]int
	redeemErr     error
	lastNextInput friendshipservice.GetNextFriendOpportunityInput
}

type testHTTPIdentities map[string]uint64

func (i testHTTPIdentities) AuthenticateHTTPSession(_ context.Context, token string) (*SessionBinding, error) {
	playerID := i[token]
	if playerID == 0 {
		return nil, ErrHTTPSessionInvalid
	}
	return &SessionBinding{PlayerID: playerID, SessionID: "test", SessionAuthorityEpoch: 1, ActiveSessionEpoch: 1}, nil
}

func authenticatedRequest(method, target, body, token string) *http.Request {
	request := httptest.NewRequest(method, target, bytes.NewBufferString(body))
	if token != "" {
		request.AddCookie(&http.Cookie{Name: HTTPSessionCookieName, Value: token})
	}
	return request
}

func (c *remoteBoundaryClient) CreateShare(context.Context, friendshipservice.CreateShareInput) (friendshipservice.CreateShareResult, error) {
	c.calls["create"]++
	return friendshipservice.CreateShareResult{ShareToken: "token", AcceptMode: friendshipservice.ShareAcceptModeMulti}, nil
}
func (c *remoteBoundaryClient) ResolveShare(context.Context, friendshipservice.ResolveShareInput) (friendshipservice.ResolveShareResult, error) {
	c.calls["resolve"]++
	return friendshipservice.ResolveShareResult{InviterPlayerID: 2,
		InviterProfile: &publicpb.PublicProfileView{PlayerId: 2, DisplayName: "friend", AvatarId: 1, Level: 1},
		AcceptMode:     friendshipservice.ShareAcceptModeMulti}, nil
}
func (c *remoteBoundaryClient) RedeemShare(context.Context, friendshipservice.RedeemShareInput) (friendshipservice.RedeemShareResult, error) {
	c.calls["redeem"]++
	if c.redeemErr != nil {
		return friendshipservice.RedeemShareResult{}, c.redeemErr
	}
	return friendshipservice.RedeemShareResult{Outcome: friendshipservice.Outcome{Status: friend.StatusSucceeded},
		Pair:            friendshipservice.PairState{RelationID: "relation", RelationEpoch: 1, Status: friend.RelationStatusActive, Changed: true},
		InviterPlayerID: 2, InviterProfile: &publicpb.PublicProfileView{PlayerId: 2, DisplayName: "friend", AvatarId: 1, Level: 1}}, nil
}

func TestRemoteFriendshipHTTPAdapterPreservesSelfInviteMessage(t *testing.T) {
	client := &remoteBoundaryClient{calls: make(map[string]int), redeemErr: friendshipservice.ErrShareSelfInvite}
	handler, err := NewFriendshipShareHandler(testHTTPIdentities{"session": 1}, client, 300)
	if err != nil {
		t.Fatal(err)
	}
	request := authenticatedRequest(http.MethodPost, "/api/v1/share/accept", `{"share_token":"token"}`, "session")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	var body httpError
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response %q: %v", response.Body.String(), err)
	}
	if response.Code != http.StatusBadRequest || body.Error != ErrShareSelfInvite.Error() || body.ErrorCode != "ERR_SHARE_LINK_INVALID" {
		t.Fatalf("self-invite status=%d body=%+v", response.Code, body)
	}
	if client.calls["redeem"] != 1 {
		t.Fatalf("redeem calls=%d", client.calls["redeem"])
	}
}
func (c *remoteBoundaryClient) GetFriendList(context.Context, friendshipservice.GetFriendListInput) (friendshipservice.GetFriendListResult, error) {
	c.calls["list"]++
	return friendshipservice.GetFriendListResult{Page: friend.FriendPage{Friends: []friend.FriendCard{{FriendPlayerID: 2,
		PublicProfile: &publicpb.PublicProfileView{PlayerId: 2, DisplayName: "friend", AvatarId: 1, Level: 1},
		RelationID:    "relation", RelationEpoch: 1, Action: friend.ActionCanVisit, OpportunityStatus: friend.OpportunityReadReady}}}}, nil
}
func (c *remoteBoundaryClient) DeleteFriend(context.Context, friendshipservice.PairMutationInput) (friendshipservice.PairMutationResult, error) {
	c.calls["delete"]++
	return friendshipservice.PairMutationResult{Outcome: friendshipservice.Outcome{Status: friend.StatusSucceeded},
		Pair: friendshipservice.PairState{RelationID: "relation", RelationEpoch: 2, Status: friend.RelationStatusDeleted, Changed: true}}, nil
}
func (c *remoteBoundaryClient) BlockFriend(context.Context, friendshipservice.PairMutationInput) (friendshipservice.PairMutationResult, error) {
	return friendshipservice.PairMutationResult{}, nil
}
func (c *remoteBoundaryClient) UnblockFriend(context.Context, friendshipservice.PairMutationInput) (friendshipservice.PairMutationResult, error) {
	return friendshipservice.PairMutationResult{}, nil
}
func (c *remoteBoundaryClient) GetNextFriendOpportunity(_ context.Context, input friendshipservice.GetNextFriendOpportunityInput) (friendshipservice.GetNextFriendOpportunityResult, error) {
	c.calls["next"]++
	c.lastNextInput = input
	return friendshipservice.GetNextFriendOpportunityResult{Status: friendshipservice.NextOpportunityNone}, nil
}
func (c *remoteBoundaryClient) AuthorizeVisit(context.Context, friendshipservice.AuthorizeVisitInput) (friendshipservice.AuthorizeVisitResult, error) {
	return friendshipservice.AuthorizeVisitResult{}, nil
}
func (c *remoteBoundaryClient) CommitFriendActionAdmission(context.Context, friend.ActionAdmissionCommand) (friend.ActionAdmissionOutcome, error) {
	return friend.ActionAdmissionOutcome{}, nil
}
func (c *remoteBoundaryClient) GetFriendActionAdmission(context.Context, friend.ActionAdmissionCommand) (friend.ActionAdmissionOutcome, error) {
	return friend.ActionAdmissionOutcome{}, nil
}

func TestPublicBlockAndUnblockRemainAbsent(t *testing.T) {
	handler, err := NewFriendshipShareHandler(testHTTPIdentities{"session": 1}, &remoteBoundaryClient{calls: make(map[string]int)}, 300)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{
		"/api/v1/friends/block",
		"/api/v1/friends/unblock",
		"/v1/friends/block",
		"/v1/friends/unblock",
	} {
		request := httptest.NewRequest(http.MethodPost, path, nil)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusNotFound {
			t.Fatalf("%s status=%d, public Block/Unblock must remain DEFERRED_NOT_EXPOSED", path, response.Code)
		}
	}
}

func TestFriendshipJSONBodyIsBoundedAndSingleValued(t *testing.T) {
	for _, body := range [][]byte{
		append([]byte(`{"share_token":"`), append(bytes.Repeat([]byte("x"), int(maxFriendshipJSONBodyBytes)), []byte(`"}`)...)...),
		[]byte(`{"share_token":"one"} {"share_token":"two"}`),
	} {
		request := httptest.NewRequest(http.MethodPost, "/api/v1/share/accept", bytes.NewReader(body))
		response := httptest.NewRecorder()
		var decoded ShareAcceptRequest
		if err := decodeFriendshipJSON(response, request, &decoded); err == nil {
			t.Fatalf("body of %d bytes must fail bounded single-value validation", len(body))
		}
	}
}

func TestRemoteFriendshipHTTPAdapterMapsOnlyExistingPublicRoutes(t *testing.T) {
	client := &remoteBoundaryClient{calls: make(map[string]int)}
	handler, err := NewFriendshipShareHandler(testHTTPIdentities{"session": 1}, client, 300)
	if err != nil {
		t.Fatal(err)
	}
	requests := []*http.Request{
		authenticatedRequest(http.MethodPost, "/api/v1/share/create", "", "session"),
		authenticatedRequest(http.MethodGet, "/api/v1/share/resolve?token=token", "", ""),
		authenticatedRequest(http.MethodPost, "/api/v1/share/accept", `{"share_token":"token"}`, "session"),
		authenticatedRequest(http.MethodGet, "/api/v1/friends", "", "session"),
		authenticatedRequest(http.MethodGet, "/api/v1/friends/opportunity", "", "session"),
		authenticatedRequest(http.MethodPost, "/api/v1/friends/revoke", `{"friend_owner_id":"2","operation_id":"op"}`, "session"),
	}
	for _, request := range requests {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("%s status=%d body=%s", request.URL.Path, response.Code, response.Body.String())
		}
		if strings.Contains(response.Body.String(), "UNSPECIFIED") {
			t.Fatalf("%s returned an unspecified optimistic result: %s", request.URL.Path, response.Body.String())
		}
	}
	for _, method := range []string{"create", "resolve", "redeem", "list", "next", "delete"} {
		if client.calls[method] != 1 {
			t.Fatalf("remote method %s calls=%d", method, client.calls[method])
		}
	}
}

func TestFriendOpportunityHTTPAdapterParsesBoundedHintTargetsFromAuthenticatedViewer(t *testing.T) {
	client := &remoteBoundaryClient{calls: make(map[string]int)}
	handler, err := NewFriendshipShareHandler(testHTTPIdentities{"session": 1}, client, 300)
	if err != nil {
		t.Fatal(err)
	}
	request := authenticatedRequest(http.MethodGet,
		"/api/v1/friends/opportunity?hint_target=3:7&hint_target=2:9", "", "session")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if client.lastNextInput.ViewerPlayerID != 1 || len(client.lastNextInput.HintedTargets) != 2 ||
		client.lastNextInput.HintedTargets[0].OwnerID != 3 ||
		client.lastNextInput.HintedTargets[0].OpportunityRevision != 7 ||
		client.lastNextInput.HintedTargets[1].OwnerID != 2 ||
		client.lastNextInput.HintedTargets[1].OpportunityRevision != 9 {
		t.Fatalf("next input=%+v", client.lastNextInput)
	}
}

func TestFriendOpportunityHTTPAdapterRejectsMalformedOrSelfHintTarget(t *testing.T) {
	for _, target := range []string{
		"/api/v1/friends/opportunity?hint_target=bad",
		"/api/v1/friends/opportunity?hint_target=1:7",
		"/api/v1/friends/opportunity?hint_target=2:0",
	} {
		client := &remoteBoundaryClient{calls: make(map[string]int)}
		handler, err := NewFriendshipShareHandler(testHTTPIdentities{"session": 1}, client, 300)
		if err != nil {
			t.Fatal(err)
		}
		request := authenticatedRequest(http.MethodGet, target, "", "session")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest || client.calls["next"] != 0 {
			t.Fatalf("target=%s status=%d calls=%d body=%s", target, response.Code, client.calls["next"], response.Body.String())
		}
	}
}

// nextErrClient fails GetNextFriendOpportunity with the injected error so the
// HTTP adapter classification can be asserted per failure class.
type nextErrClient struct {
	remoteBoundaryClient
	err error
}

func (c *nextErrClient) GetNextFriendOpportunity(ctx context.Context, input friendshipservice.GetNextFriendOpportunityInput) (friendshipservice.GetNextFriendOpportunityResult, error) {
	c.calls["next"]++
	if c.err != nil {
		return friendshipservice.GetNextFriendOpportunityResult{}, c.err
	}
	return c.remoteBoundaryClient.GetNextFriendOpportunity(ctx, input)
}

func TestFriendOpportunityHTTPAdapterClassifiesBackendFailures(t *testing.T) {
	cases := []struct {
		name           string
		err            error
		wantStatus     int
		wantErrorCode  string
		wantRetryAfter bool
	}{
		{name: "capacity", err: friendshipservice.ErrCapacity, wantStatus: http.StatusTooManyRequests, wantErrorCode: "FRIEND_OPPORTUNITY_BUSY", wantRetryAfter: true},
		{name: "deadline", err: fmt.Errorf("%w: %w", friendshipservice.ErrUnavailable, context.DeadlineExceeded), wantStatus: http.StatusServiceUnavailable, wantErrorCode: "FRIEND_OPPORTUNITY_TIMEOUT", wantRetryAfter: true},
		{name: "unavailable", err: friendshipservice.ErrUnavailable, wantStatus: http.StatusServiceUnavailable, wantErrorCode: "FRIEND_OPPORTUNITY_UNAVAILABLE"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inner := &remoteBoundaryClient{calls: make(map[string]int)}
			handler, err := NewFriendshipShareHandler(testHTTPIdentities{"session": 1}, &nextErrClient{remoteBoundaryClient: *inner, err: tc.err}, 300)
			if err != nil {
				t.Fatal(err)
			}
			request := authenticatedRequest(http.MethodGet, "/api/v1/friends/opportunity", "", "session")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			var body httpError
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode response %q: %v", response.Body.String(), err)
			}
			if response.Code != tc.wantStatus {
				t.Fatalf("status=%d want %d body=%+v", response.Code, tc.wantStatus, body)
			}
			if body.ErrorCode != tc.wantErrorCode {
				t.Fatalf("error_code=%q want %q", body.ErrorCode, tc.wantErrorCode)
			}
			if got := response.Header().Get("Retry-After"); (got != "") != tc.wantRetryAfter {
				t.Fatalf("Retry-After=%q want present=%v", got, tc.wantRetryAfter)
			}
		})
	}
}

// TestFriendOpportunityHTTPAdapter429IsNeverNONE pins the contract that an
// overload response can only be retried, never coerced into a negative answer.
func TestFriendOpportunityHTTPAdapter429IsNeverNONE(t *testing.T) {
	inner := &remoteBoundaryClient{calls: make(map[string]int)}
	handler, err := NewFriendshipShareHandler(testHTTPIdentities{"session": 1}, &nextErrClient{remoteBoundaryClient: *inner, err: friendshipservice.ErrCapacity}, 300)
	if err != nil {
		t.Fatal(err)
	}
	request := authenticatedRequest(http.MethodGet, "/api/v1/friends/opportunity", "", "session")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusTooManyRequests {
		t.Fatalf("status=%d want 429", response.Code)
	}
	if strings.Contains(response.Body.String(), `"status":"NONE"`) {
		t.Fatalf("429 response must never look like a NONE answer: %s", response.Body.String())
	}
}
