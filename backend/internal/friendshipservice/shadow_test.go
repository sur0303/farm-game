package friendshipservice

import (
	"context"
	"testing"

	publicpb "farmgame/api/gen/public"
	"farmgame/internal/friend"
)

type shadowTestClient struct {
	create               *CreateShareResult
	list                 GetFriendListResult
	next                 GetNextFriendOpportunityResult
	redeem               RedeemShareResult
	redeemErr            error
	redeemObservation    *RedeemShareResult
	redeemObservationErr error
	pair                 *PairMutationResult
	admission            friend.ActionAdmissionOutcome
	writes               int
	observations         int
	admissionCommits     int
	admissionLookups     int
}

func (c *shadowTestClient) CreateShare(_ context.Context, input CreateShareInput) (CreateShareResult, error) {
	if input.ObservationOnly {
		c.observations++
	} else {
		c.writes++
	}
	if c.create != nil {
		return *c.create, nil
	}
	return CreateShareResult{Outcome: Outcome{Status: friend.StatusSucceeded, CommittedEpoch: 2},
		ShareToken: "token", AcceptMode: ShareAcceptModeMulti, TokenGeneration: 2, IssuedAtMs: 10, ExpiresAtMs: 20}, nil
}
func (c *shadowTestClient) ResolveShare(context.Context, ResolveShareInput) (ResolveShareResult, error) {
	return ResolveShareResult{InviterPlayerID: 2, InviterProfile: testShadowProfile(2), AcceptMode: ShareAcceptModeMulti}, nil
}
func (c *shadowTestClient) RedeemShare(_ context.Context, input RedeemShareInput) (RedeemShareResult, error) {
	if input.ObservationOnly {
		c.observations++
		if c.redeemObservationErr != nil {
			return RedeemShareResult{}, c.redeemObservationErr
		}
		if c.redeemObservation != nil {
			return *c.redeemObservation, nil
		}
	} else {
		c.writes++
	}
	return c.redeem, c.redeemErr
}
func (c *shadowTestClient) GetFriendList(context.Context, GetFriendListInput) (GetFriendListResult, error) {
	return c.list, nil
}
func (c *shadowTestClient) DeleteFriend(_ context.Context, input PairMutationInput) (PairMutationResult, error) {
	return c.pairMutation(input), nil
}
func (c *shadowTestClient) BlockFriend(_ context.Context, input PairMutationInput) (PairMutationResult, error) {
	return c.pairMutation(input), nil
}
func (c *shadowTestClient) UnblockFriend(_ context.Context, input PairMutationInput) (PairMutationResult, error) {
	return c.pairMutation(input), nil
}
func (c *shadowTestClient) pairMutation(input PairMutationInput) PairMutationResult {
	if input.ObservationOnly {
		c.observations++
	} else {
		c.writes++
	}
	if c.pair != nil {
		return *c.pair
	}
	return PairMutationResult{Outcome: Outcome{Status: friend.StatusSucceeded, CommittedEpoch: 4},
		Pair: PairState{RelationID: "relation", RelationEpoch: 4, Status: friend.RelationStatusDeleted, Changed: !input.ObservationOnly}}
}
func (c *shadowTestClient) GetNextFriendOpportunity(context.Context, GetNextFriendOpportunityInput) (GetNextFriendOpportunityResult, error) {
	return c.next, nil
}
func (c *shadowTestClient) AuthorizeVisit(context.Context, AuthorizeVisitInput) (AuthorizeVisitResult, error) {
	return AuthorizeVisitResult{}, nil
}
func (c *shadowTestClient) CommitFriendActionAdmission(context.Context, friend.ActionAdmissionCommand) (friend.ActionAdmissionOutcome, error) {
	c.admissionCommits++
	return c.admission, nil
}
func (c *shadowTestClient) GetFriendActionAdmission(context.Context, friend.ActionAdmissionCommand) (friend.ActionAdmissionOutcome, error) {
	c.admissionLookups++
	return c.admission, nil
}

func testShadowProfile(playerID uint64) *publicpb.PublicProfileView {
	return &publicpb.PublicProfileView{PlayerId: playerID, DisplayName: "farmer", Level: 1}
}

func testShadowCard() friend.FriendCard {
	return friend.FriendCard{FriendPlayerID: 2, PublicProfile: testShadowProfile(2), RelationID: "relation",
		RelationEpoch: 3, Action: friend.ActionCanWater | friend.ActionCanVisit,
		OpportunityStatus: friend.OpportunityReadReady, OpportunityRevision: 8, ValidatedAtMs: 50}
}

func TestShadowReadEqualityAndIntentionalMismatchMetrics(t *testing.T) {
	page := GetFriendListResult{Page: friend.FriendPage{Friends: []friend.FriendCard{testShadowCard()},
		RosterRevision: 4, ValidatedAtMs: 50}}
	formal := &shadowTestClient{list: page, next: GetNextFriendOpportunityResult{Status: NextOpportunityNone}}
	observer := &shadowTestClient{list: page, next: GetNextFriendOpportunityResult{Status: NextOpportunityUnknown}}
	recorder := NewMemoryShadowRecorder()
	shadow, err := NewShadowClient(formal, observer, recorder)
	if err != nil {
		t.Fatal(err)
	}
	gotPage, err := shadow.GetFriendList(context.Background(), GetFriendListInput{ViewerPlayerID: 1, PageSize: 20})
	if err != nil || len(gotPage.Page.Friends) != 1 || recorder.Count("get_friend_list", ShadowEqual) != 1 {
		t.Fatalf("equal list result=%+v err=%v metrics=%d", gotPage, err, recorder.Count("get_friend_list", ShadowEqual))
	}
	gotNext, err := shadow.GetNextFriendOpportunity(context.Background(), GetNextFriendOpportunityInput{ViewerPlayerID: 1})
	if err != nil || gotNext.Status != NextOpportunityNone || recorder.Count("get_next_friend_opportunity", ShadowMismatch) != 0 {
		t.Fatalf("stateful next must run only on the formal path: result=%+v err=%v", gotNext, err)
	}
}

func TestShadowMutationPerformsExactlyOneBusinessWrite(t *testing.T) {
	result := RedeemShareResult{Outcome: Outcome{Status: friend.StatusSucceeded, CommittedEpoch: 3},
		Pair:            PairState{RelationID: "relation", RelationEpoch: 3, Status: friend.RelationStatusActive, Changed: true},
		InviterPlayerID: 2, InviterProfile: testShadowProfile(2)}
	formal := &shadowTestClient{redeem: result}
	observedResult := result
	observedResult.Pair.Changed = false
	observer := &shadowTestClient{redeem: observedResult}
	recorder := NewMemoryShadowRecorder()
	shadow, _ := NewShadowClient(formal, observer, recorder)
	got, err := shadow.RedeemShare(context.Background(), RedeemShareInput{RedeemerPlayerID: 1, ShareToken: "token"})
	if err != nil || !got.Pair.Changed {
		t.Fatalf("formal mutation response changed: result=%+v err=%v", got, err)
	}
	assertSingleShadowMutation(t, formal, observer)
	if recorder.Count("redeem_share", ShadowEqual) != 1 {
		t.Fatalf("expected equality metric, got %d", recorder.Count("redeem_share", ShadowEqual))
	}
}

func TestShadowRejectedRedeemComparesCanonicalObservations(t *testing.T) {
	formalResult := RedeemShareResult{
		Outcome: Outcome{Status: friend.StatusRejected, Reason: friend.ReasonBlocked},
		// The rejected business receipt intentionally has no Pair facts.
		InviterPlayerID: 2, InviterProfile: testShadowProfile(2),
	}
	canonical := RedeemShareResult{
		Outcome:         Outcome{Status: friend.StatusRejected, Reason: friend.ReasonVisitNotPermitted},
		Pair:            PairState{RelationID: "blocked-relation", RelationEpoch: 6, Status: friend.RelationStatusDeleted},
		InviterPlayerID: 2, InviterProfile: testShadowProfile(2),
	}
	formal := &shadowTestClient{redeem: formalResult, redeemObservation: &canonical}
	observer := &shadowTestClient{redeemObservation: &canonical}
	recorder := NewMemoryShadowRecorder()
	shadow, _ := NewShadowClient(formal, observer, recorder)
	got, err := shadow.RedeemShare(context.Background(), RedeemShareInput{
		RedeemerPlayerID: 1, ShareToken: "token", OperationID: "blocked-redeem",
	})
	if err != nil || got.Outcome.Status != friend.StatusRejected || got.Outcome.Reason != friend.ReasonBlocked ||
		got.Pair != (PairState{}) {
		t.Fatalf("formal rejected receipt changed: result=%+v err=%v", got, err)
	}
	assertSingleShadowMutation(t, formal, observer)
	if recorder.Count("redeem_share", ShadowEqual) != 1 {
		t.Fatalf("canonical rejected redeem equality metric=%d", recorder.Count("redeem_share", ShadowEqual))
	}
}

func TestShadowNoChangeReplayComparesOnlyCommittedCanonicalFacts(t *testing.T) {
	t.Run("create share", func(t *testing.T) {
		formalResult := CreateShareResult{Outcome: Outcome{Status: friend.StatusNoChange, Reason: friend.ReasonNone},
			ShareToken: "token", AcceptMode: ShareAcceptModeMulti, TokenGeneration: 7, IssuedAtMs: 10, ExpiresAtMs: 20}
		observedResult := formalResult
		observedResult.Outcome = Outcome{Status: friend.StatusSucceeded, CommittedEpoch: 7}
		formal := &shadowTestClient{create: &formalResult}
		observer := &shadowTestClient{create: &observedResult}
		recorder := NewMemoryShadowRecorder()
		shadow, _ := NewShadowClient(formal, observer, recorder)
		got, err := shadow.CreateShare(context.Background(), CreateShareInput{SubjectPlayerID: 1, OperationID: "replay"})
		if err != nil || got.Outcome.Status != friend.StatusNoChange {
			t.Fatalf("formal create replay=%+v err=%v", got, err)
		}
		assertSingleShadowMutation(t, formal, observer)
		if recorder.Count("create_share", ShadowEqual) != 1 {
			t.Fatalf("create equality metric=%d", recorder.Count("create_share", ShadowEqual))
		}
	})

	t.Run("redeem share", func(t *testing.T) {
		formalResult := RedeemShareResult{Outcome: Outcome{Status: friend.StatusNoChange, Reason: friend.ReasonAlreadyFriends},
			Pair: mappingPair(false), InviterPlayerID: 2, InviterProfile: testShadowProfile(2)}
		observedResult := formalResult
		observedResult.Outcome = Outcome{Status: friend.StatusSucceeded, CommittedEpoch: 3}
		formal := &shadowTestClient{redeem: formalResult}
		observer := &shadowTestClient{redeem: observedResult}
		recorder := NewMemoryShadowRecorder()
		shadow, _ := NewShadowClient(formal, observer, recorder)
		got, err := shadow.RedeemShare(context.Background(), RedeemShareInput{
			RedeemerPlayerID: 1, ShareToken: "token", OperationID: "replay",
		})
		if err != nil || got.Outcome.Status != friend.StatusNoChange {
			t.Fatalf("formal redeem replay=%+v err=%v", got, err)
		}
		assertSingleShadowMutation(t, formal, observer)
		if recorder.Count("redeem_share", ShadowEqual) != 1 {
			t.Fatalf("redeem equality metric=%d", recorder.Count("redeem_share", ShadowEqual))
		}
	})

	for _, method := range []struct {
		name string
		call func(*ShadowClient, context.Context, PairMutationInput) (PairMutationResult, error)
	}{
		{name: "delete_friend", call: func(client *ShadowClient, ctx context.Context, input PairMutationInput) (PairMutationResult, error) {
			return client.DeleteFriend(ctx, input)
		}},
		{name: "block_friend", call: func(client *ShadowClient, ctx context.Context, input PairMutationInput) (PairMutationResult, error) {
			return client.BlockFriend(ctx, input)
		}},
		{name: "unblock_friend", call: func(client *ShadowClient, ctx context.Context, input PairMutationInput) (PairMutationResult, error) {
			return client.UnblockFriend(ctx, input)
		}},
	} {
		t.Run(method.name, func(t *testing.T) {
			formalResult := PairMutationResult{Outcome: Outcome{Status: friend.StatusNoChange, Reason: friend.ReasonAlreadyUnblocked},
				Pair: PairState{RelationID: "relation", RelationEpoch: 4, Status: friend.RelationStatusDeleted}}
			observedResult := formalResult
			observedResult.Outcome = Outcome{Status: friend.StatusSucceeded, CommittedEpoch: 4}
			formal := &shadowTestClient{pair: &formalResult}
			observer := &shadowTestClient{pair: &observedResult}
			recorder := NewMemoryShadowRecorder()
			shadow, _ := NewShadowClient(formal, observer, recorder)
			got, err := method.call(shadow, context.Background(), PairMutationInput{
				CallerPlayerID: 1, PeerPlayerID: 2, OperationID: "replay",
			})
			if err != nil || got.Outcome.Status != friend.StatusNoChange {
				t.Fatalf("formal %s replay=%+v err=%v", method.name, got, err)
			}
			assertSingleShadowMutation(t, formal, observer)
			if recorder.Count(method.name, ShadowEqual) != 1 {
				t.Fatalf("%s equality metric=%d", method.name, recorder.Count(method.name, ShadowEqual))
			}
		})
	}
}

func TestShadowMutationRecordsCanonicalFactMismatch(t *testing.T) {
	formalResult := PairMutationResult{Outcome: Outcome{Status: friend.StatusNoChange},
		Pair: PairState{RelationID: "relation", RelationEpoch: 4, Status: friend.RelationStatusDeleted}}
	observedResult := formalResult
	observedResult.Outcome = Outcome{Status: friend.StatusSucceeded}
	observedResult.Pair.RelationEpoch = 5
	formal := &shadowTestClient{pair: &formalResult}
	observer := &shadowTestClient{pair: &observedResult}
	recorder := NewMemoryShadowRecorder()
	shadow, _ := NewShadowClient(formal, observer, recorder)
	got, err := shadow.UnblockFriend(context.Background(), PairMutationInput{CallerPlayerID: 1, PeerPlayerID: 2, OperationID: "replay"})
	if err != nil || got.Pair.RelationEpoch != 4 {
		t.Fatalf("canonical mismatch altered formal result=%+v err=%v", got, err)
	}
	assertSingleShadowMutation(t, formal, observer)
	if recorder.Count("unblock_friend", ShadowMismatch) != 1 {
		t.Fatalf("canonical mismatch metric=%d", recorder.Count("unblock_friend", ShadowMismatch))
	}
}

func assertSingleShadowMutation(t *testing.T, formal, observer *shadowTestClient) {
	t.Helper()
	if formal.writes != 1 || formal.observations != 1 || observer.writes != 0 || observer.observations != 1 {
		t.Fatalf("formal writes=%d observations=%d remote writes=%d observations=%d",
			formal.writes, formal.observations, observer.writes, observer.observations)
	}
}

func TestShadowActionAdmissionObservesReceiptWithoutSecondCommit(t *testing.T) {
	identity := friend.CommitIdentity{SubjectPlayerID: 1, CommandType: "CARE", OperationID: "op"}
	outcome := friend.ActionAdmissionOutcome{Outcome: friend.Outcome{Status: friend.StatusSucceeded, Identity: identity,
		PayloadHash: []byte("payload"), CommittedEpoch: 3}, Permit: friend.ActionPermitSnapshot{PermitID: "permit"},
		DispatchOrderID: "dispatch", WorkflowDeadlineAtMs: 100}
	formal := &shadowTestClient{admission: outcome}
	observer := &shadowTestClient{admission: outcome}
	shadow, _ := NewShadowClient(formal, observer, NewMemoryShadowRecorder())
	got, err := shadow.CommitFriendActionAdmission(context.Background(), friend.ActionAdmissionCommand{Identity: identity, PayloadHash: []byte("payload")})
	if err != nil || got.DispatchOrderID != "dispatch" {
		t.Fatalf("commit result=%+v err=%v", got, err)
	}
	if formal.admissionCommits != 1 || observer.admissionCommits != 0 || observer.admissionLookups != 1 {
		t.Fatalf("formal commits=%d remote commits=%d remote lookups=%d",
			formal.admissionCommits, observer.admissionCommits, observer.admissionLookups)
	}
}

func TestShadowMalformedObservationDoesNotAlterFormalMutation(t *testing.T) {
	formalResult := RedeemShareResult{Outcome: Outcome{Status: friend.StatusSucceeded, CommittedEpoch: 3},
		Pair:            PairState{RelationID: "relation", RelationEpoch: 3, Status: friend.RelationStatusActive, Changed: true},
		InviterPlayerID: 2, InviterProfile: testShadowProfile(2)}
	formal := &shadowTestClient{redeem: formalResult}
	observer := &shadowTestClient{redeemErr: ErrMalformedResponse}
	recorder := NewMemoryShadowRecorder()
	shadow, _ := NewShadowClient(formal, observer, recorder)
	got, err := shadow.RedeemShare(context.Background(), RedeemShareInput{RedeemerPlayerID: 1, ShareToken: "token"})
	if err != nil || got.Pair.RelationID != formalResult.Pair.RelationID || !got.Pair.Changed {
		t.Fatalf("malformed observation changed formal result: %+v err=%v", got, err)
	}
	assertSingleShadowMutation(t, formal, observer)
	if recorder.Count("redeem_share", ShadowMalformed) != 1 {
		t.Fatalf("malformed metric=%d", recorder.Count("redeem_share", ShadowMalformed))
	}
}

func TestShadowLocalOrRemoteObservationFailureDoesNotAlterFormalMutation(t *testing.T) {
	formalResult := RedeemShareResult{Outcome: Outcome{Status: friend.StatusSucceeded, CommittedEpoch: 3},
		Pair:            PairState{RelationID: "relation", RelationEpoch: 3, Status: friend.RelationStatusActive, Changed: true},
		InviterPlayerID: 2, InviterProfile: testShadowProfile(2)}
	for _, test := range []struct {
		name      string
		localErr  error
		remoteErr error
		result    ShadowResult
	}{
		{name: "local malformed", localErr: ErrMalformedResponse, result: ShadowMalformed},
		{name: "local unavailable", localErr: ErrUnavailable, result: ShadowUnavailable},
		{name: "remote unavailable", remoteErr: ErrUnavailable, result: ShadowUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			formal := &shadowTestClient{redeem: formalResult, redeemObservationErr: test.localErr}
			observer := &shadowTestClient{redeem: formalResult, redeemObservationErr: test.remoteErr}
			recorder := NewMemoryShadowRecorder()
			shadow, _ := NewShadowClient(formal, observer, recorder)
			got, err := shadow.RedeemShare(context.Background(), RedeemShareInput{
				RedeemerPlayerID: 1, ShareToken: "token", OperationID: "operation",
			})
			if err != nil || got.Pair.RelationID != formalResult.Pair.RelationID || !got.Pair.Changed {
				t.Fatalf("observation failure changed formal result: %+v err=%v", got, err)
			}
			assertSingleShadowMutation(t, formal, observer)
			if recorder.Count("redeem_share", test.result) != 1 {
				t.Fatalf("result=%s metric=%d", test.result, recorder.Count("redeem_share", test.result))
			}
		})
	}
}
