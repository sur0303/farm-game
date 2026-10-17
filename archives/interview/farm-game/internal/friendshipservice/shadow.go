package friendshipservice

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"sync"

	"farmgame/internal/friend"

	"google.golang.org/protobuf/proto"
)

type ShadowResult string

const (
	ShadowEqual       ShadowResult = "equal"
	ShadowMismatch    ShadowResult = "mismatch"
	ShadowMalformed   ShadowResult = "malformed"
	ShadowUnavailable ShadowResult = "unavailable"
)

type ShadowRecorder interface {
	Record(method string, result ShadowResult)
}

type ShadowClient struct {
	formal   Client
	observer Client
	recorder ShadowRecorder
}

func NewShadowClient(formal, observer Client, recorder ShadowRecorder) (*ShadowClient, error) {
	if formal == nil || observer == nil || recorder == nil {
		return nil, ErrInvalidArgument
	}
	return &ShadowClient{formal: formal, observer: observer, recorder: recorder}, nil
}

func (c *ShadowClient) record(method string, equal bool, err error) {
	switch {
	case err == nil && equal:
		c.recorder.Record(method, ShadowEqual)
	case err == nil:
		c.recorder.Record(method, ShadowMismatch)
	case errors.Is(err, ErrMalformedResponse):
		c.recorder.Record(method, ShadowMalformed)
	default:
		c.recorder.Record(method, ShadowUnavailable)
	}
}

func shadowObservationError(localErr, remoteErr error) error {
	// Preserve the bounded result vocabulary. If either observation is
	// malformed, prefer that more specific fail-closed class; every other
	// technical failure is unavailable.
	for _, err := range []error{localErr, remoteErr} {
		if errors.Is(err, ErrMalformedResponse) {
			return err
		}
	}
	if localErr != nil {
		return localErr
	}
	return remoteErr
}

func (c *ShadowClient) CreateShare(ctx context.Context, input CreateShareInput) (CreateShareResult, error) {
	formal, err := c.formal.CreateShare(ctx, input)
	if err != nil {
		return formal, err
	}
	input.ObservationOnly = true
	local, localErr := c.formal.CreateShare(ctx, input)
	remote, remoteErr := c.observer.CreateShare(ctx, input)
	observeErr := shadowObservationError(localErr, remoteErr)
	equal := observeErr == nil && local.ShareToken == remote.ShareToken &&
		local.TokenGeneration == remote.TokenGeneration && local.IssuedAtMs == remote.IssuedAtMs &&
		local.ExpiresAtMs == remote.ExpiresAtMs
	c.record("create_share", equal, observeErr)
	return formal, nil
}

func (c *ShadowClient) ResolveShare(ctx context.Context, input ResolveShareInput) (ResolveShareResult, error) {
	formal, err := c.formal.ResolveShare(ctx, input)
	if err != nil {
		return formal, err
	}
	observed, observeErr := c.observer.ResolveShare(ctx, input)
	equal := observeErr == nil && formal.InviterPlayerID == observed.InviterPlayerID && formal.AcceptMode == observed.AcceptMode &&
		proto.Equal(formal.InviterProfile, observed.InviterProfile)
	c.record("resolve_share", equal, observeErr)
	return formal, nil
}

func (c *ShadowClient) RedeemShare(ctx context.Context, input RedeemShareInput) (RedeemShareResult, error) {
	formal, err := c.formal.RedeemShare(ctx, input)
	if err != nil {
		return formal, err
	}
	input.ObservationOnly = true
	local, localErr := c.formal.RedeemShare(ctx, input)
	remote, remoteErr := c.observer.RedeemShare(ctx, input)
	observeErr := shadowObservationError(localErr, remoteErr)
	equal := observeErr == nil && equalPair(local.Pair, remote.Pair) && local.InviterPlayerID == remote.InviterPlayerID &&
		proto.Equal(local.InviterProfile, remote.InviterProfile)
	c.record("redeem_share", equal, observeErr)
	return formal, nil
}

func (c *ShadowClient) GetFriendList(ctx context.Context, input GetFriendListInput) (GetFriendListResult, error) {
	formal, err := c.formal.GetFriendList(ctx, input)
	if err != nil {
		return formal, err
	}
	observed, observeErr := c.observer.GetFriendList(ctx, input)
	c.record("get_friend_list", observeErr == nil && equalFriendPage(formal.Page, observed.Page), observeErr)
	return formal, nil
}

func (c *ShadowClient) DeleteFriend(ctx context.Context, input PairMutationInput) (PairMutationResult, error) {
	return c.shadowPairMutation(ctx, "delete_friend", input, c.formal.DeleteFriend, c.observer.DeleteFriend)
}

func (c *ShadowClient) BlockFriend(ctx context.Context, input PairMutationInput) (PairMutationResult, error) {
	return c.shadowPairMutation(ctx, "block_friend", input, c.formal.BlockFriend, c.observer.BlockFriend)
}

func (c *ShadowClient) UnblockFriend(ctx context.Context, input PairMutationInput) (PairMutationResult, error) {
	return c.shadowPairMutation(ctx, "unblock_friend", input, c.formal.UnblockFriend, c.observer.UnblockFriend)
}

func (c *ShadowClient) shadowPairMutation(ctx context.Context, method string, input PairMutationInput,
	formalCall, observeCall func(context.Context, PairMutationInput) (PairMutationResult, error)) (PairMutationResult, error) {
	formal, err := formalCall(ctx, input)
	if err != nil {
		return formal, err
	}
	input.ObservationOnly = true
	local, localErr := formalCall(ctx, input)
	remote, remoteErr := observeCall(ctx, input)
	observeErr := shadowObservationError(localErr, remoteErr)
	c.record(method, observeErr == nil && equalPair(local.Pair, remote.Pair), observeErr)
	return formal, nil
}

func (c *ShadowClient) GetNextFriendOpportunity(ctx context.Context, input GetNextFriendOpportunityInput) (GetNextFriendOpportunityResult, error) {
	// GET_NEXT advances a per-viewer lossy CandidateWindow. Calling an
	// observer after the formal path would consume the next candidate twice
	// when both sides share Redis. This stateful query is therefore excluded
	// from dual-read shadowing; Header/Facts batch reads remain independently
	// benchmarked and observable.
	return c.formal.GetNextFriendOpportunity(ctx, input)
}

func (c *ShadowClient) AuthorizeVisit(ctx context.Context, input AuthorizeVisitInput) (AuthorizeVisitResult, error) {
	formal, err := c.formal.AuthorizeVisit(ctx, input)
	if err != nil {
		return formal, err
	}
	observed, observeErr := c.observer.AuthorizeVisit(ctx, input)
	equal := observeErr == nil && formal.Outcome.Status == observed.Outcome.Status && formal.Outcome.Reason == observed.Outcome.Reason &&
		reflect.DeepEqual(formal.Permit, observed.Permit)
	c.record("authorize_visit", equal, observeErr)
	return formal, nil
}

func (c *ShadowClient) CommitFriendActionAdmission(ctx context.Context, command friend.ActionAdmissionCommand) (friend.ActionAdmissionOutcome, error) {
	formal, err := c.formal.CommitFriendActionAdmission(ctx, command)
	if err != nil {
		return formal, err
	}
	// A shadow mutation is observed through the receipt/order lookup. Calling
	// Commit on the remote service would create a forbidden second write.
	observed, observeErr := c.observer.GetFriendActionAdmission(ctx, command)
	equal := observeErr == nil && equalAdmission(formal, observed)
	c.record("commit_friend_action_admission", equal, observeErr)
	return formal, nil
}

func (c *ShadowClient) GetFriendActionAdmission(ctx context.Context, command friend.ActionAdmissionCommand) (friend.ActionAdmissionOutcome, error) {
	formal, err := c.formal.GetFriendActionAdmission(ctx, command)
	if err != nil {
		return formal, err
	}
	observed, observeErr := c.observer.GetFriendActionAdmission(ctx, command)
	c.record("get_friend_action_admission", observeErr == nil && equalAdmission(formal, observed), observeErr)
	return formal, nil
}

func equalPair(left, right PairState) bool {
	return left.RelationID == right.RelationID && left.RelationEpoch == right.RelationEpoch && left.Status == right.Status
}

func equalOutcome(left, right Outcome) bool {
	return left.Status == right.Status && left.Reason == right.Reason && left.Identity.Key() == right.Identity.Key() &&
		bytes.Equal(left.PayloadHash, right.PayloadHash) && left.CommittedEpoch == right.CommittedEpoch &&
		left.CommittedAtMs == right.CommittedAtMs && left.Detail == right.Detail
}

func equalFriendPage(left, right friend.FriendPage) bool {
	if left.NextCursor != right.NextCursor || left.RosterRevision != right.RosterRevision || len(left.Friends) != len(right.Friends) {
		return false
	}
	for index := range left.Friends {
		if !equalFriendCard(left.Friends[index], right.Friends[index]) {
			return false
		}
	}
	return true
}

func equalFriendCard(left, right friend.FriendCard) bool {
	return left.FriendPlayerID == right.FriendPlayerID && left.RelationID == right.RelationID &&
		left.RelationEpoch == right.RelationEpoch && left.Action == right.Action &&
		left.OpportunityStatus == right.OpportunityStatus && left.OpportunityRevision == right.OpportunityRevision &&
		reflect.DeepEqual(left.EligibleTheftTargets, right.EligibleTheftTargets) &&
		proto.Equal(left.PublicProfile, right.PublicProfile)
}

func equalAdmission(left, right friend.ActionAdmissionOutcome) bool {
	return equalOutcome(outcomeFromDomain(left.Outcome), outcomeFromDomain(right.Outcome)) &&
		reflect.DeepEqual(left.Permit, right.Permit) && left.DispatchOrderID == right.DispatchOrderID &&
		left.WorkflowDeadlineAtMs == right.WorkflowDeadlineAtMs
}

type MemoryShadowRecorder struct {
	mu     sync.Mutex
	counts map[string]map[ShadowResult]uint64
}

func NewMemoryShadowRecorder() *MemoryShadowRecorder {
	return &MemoryShadowRecorder{counts: make(map[string]map[ShadowResult]uint64)}
}

func (r *MemoryShadowRecorder) Record(method string, result ShadowResult) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.counts[method] == nil {
		r.counts[method] = make(map[ShadowResult]uint64)
	}
	r.counts[method][result]++
}

func (r *MemoryShadowRecorder) Count(method string, result ShadowResult) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counts[method][result]
}
