package friendshipservice

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	publicpb "farmgame/api/gen/public"
	"farmgame/internal/friend"
)

type statelessShareProjections struct{ values map[string]ShareProjection }

func (s *statelessShareProjections) Create(_ context.Context, playerID uint64) (string, error) {
	token := fmt.Sprintf("token-%d", playerID)
	s.values[token] = ShareProjection{InviterPlayerID: playerID, CreatedAtMs: 1, ExpiresAtMs: 2, AcceptMode: ShareAcceptModeMulti}
	return token, nil
}
func (s *statelessShareProjections) Resolve(_ context.Context, token string) (ShareProjection, error) {
	value, ok := s.values[token]
	if !ok {
		return ShareProjection{}, ErrShareLinkInvalid
	}
	return value, nil
}
func (s *statelessShareProjections) ResolveForAdmission(ctx context.Context, token string) (ShareProjection, error) {
	return s.Resolve(ctx, token)
}
func (s *statelessShareProjections) AlignAuthorityToken(_ context.Context, token string, issuedAtMs, expiresAtMs int64) error {
	value, ok := s.values[token]
	if !ok {
		return ErrShareLinkInvalid
	}
	value.CreatedAtMs, value.ExpiresAtMs = issuedAtMs, expiresAtMs
	s.values[token] = value
	return nil
}

type statelessProfiles struct{}

func (statelessProfiles) Read(_ context.Context, playerID uint64) (*publicpb.PublicProfileView, error) {
	return &publicpb.PublicProfileView{PlayerId: playerID, DisplayName: fmt.Sprintf("player-%d", playerID), AvatarId: 1, Level: 1}, nil
}
func (statelessProfiles) BatchGetPublicProfiles(_ context.Context, playerIDs []uint64) (map[uint64]*publicpb.PublicProfileView, error) {
	result := make(map[uint64]*publicpb.PublicProfileView, len(playerIDs))
	for _, playerID := range playerIDs {
		result[playerID] = &publicpb.PublicProfileView{PlayerId: playerID, DisplayName: fmt.Sprintf("player-%d", playerID), AvatarId: 1, Level: 1}
	}
	return result, nil
}

type statelessOpportunities struct {
	actionableAt        map[uint64]int64
	opportunityRevision map[uint64]uint64
	opportunityStatus   map[uint64]friend.OpportunityReadStatus
	actionMask          map[uint64]uint32
}

func (s statelessOpportunities) BatchGetCandidates(_ context.Context, farmIDs []uint64, _ int64) (map[uint64]friend.FarmOpportunityCandidate, error) {
	result := make(map[uint64]friend.FarmOpportunityCandidate, len(farmIDs))
	for _, farmID := range farmIDs {
		actionableAt := int64(900 + farmID)
		if configured := s.actionableAt[farmID]; configured != 0 {
			actionableAt = configured
		}
		result[farmID] = friend.FarmOpportunityCandidate{FarmID: farmID, State: friend.CandidateYes,
			SourceFarmVersion: farmID, ActionableAtMs: actionableAt}
	}
	return result, nil
}

func (statelessOpportunities) BatchEvaluate(_ context.Context, _ uint64, farmIDs []uint64, _ int64) (map[uint64]friend.ActionMask, error) {
	result := make(map[uint64]friend.ActionMask, len(farmIDs))
	for _, farmID := range farmIDs {
		result[farmID] = friend.ActionCanWater
	}
	return result, nil
}

func (s statelessOpportunities) BatchEvaluateFriendOpportunity(_ context.Context, _ uint64, ownerIDs []uint64, _ int64) ([]friend.FriendOpportunityEvaluation, error) {
	result := make([]friend.FriendOpportunityEvaluation, len(ownerIDs))
	for index, ownerID := range ownerIDs {
		status := friend.OpportunityReadReady
		if configured := s.opportunityStatus[ownerID]; configured != friend.OpportunityReadUnknown {
			status = configured
		}
		action := friend.FriendActionCanWater
		if configured := s.actionMask[ownerID]; configured != 0 {
			action = configured
		}
		revision := uint64(1)
		if configured := s.opportunityRevision[ownerID]; configured != 0 {
			revision = configured
		}
		result[index] = friend.FriendOpportunityEvaluation{OwnerPlayerID: ownerID, Status: status,
			ActionMask: action, OpportunityRevision: revision}
	}
	return result, nil
}

type statelessRepairQueue struct {
	viewers []uint64
	err     error
}

type faultingFriendIndex struct {
	delegate    *friend.MemoryFriendIndexRepository
	applyErr    error
	removeErr   error
	applyCalls  int
	removeCalls int
	scanCalls        int
	lookupCalls      int
	batchLookupCalls int
	revisionCalls    int
}

func (i *faultingFriendIndex) Apply(ctx context.Context, viewer uint64, entry friend.FriendIndexEntry) (bool, error) {
	i.applyCalls++
	if i.applyErr != nil {
		return false, i.applyErr
	}
	return i.delegate.Apply(ctx, viewer, entry)
}

func (i *faultingFriendIndex) Remove(ctx context.Context, viewer, peer uint64, relationID string, epoch uint64) (bool, error) {
	i.removeCalls++
	if i.removeErr != nil {
		return false, i.removeErr
	}
	return i.delegate.Remove(ctx, viewer, peer, relationID, epoch)
}

func TestRejectedRedeemHasNoPairReceiptOrProjectionWrite(t *testing.T) {
	pairs := friend.NewMemoryPairRepository()
	quotas := friend.NewMemoryQuotaRepository(1)
	issuers := friend.NewMemoryShareIssuerRepository((7 * 24 * time.Hour).Milliseconds())
	domain, err := friend.NewDomainService(pairs, quotas, issuers)
	if err != nil {
		t.Fatal(err)
	}
	healthyIndex := friend.NewMemoryFriendIndexRepository()
	index := &faultingFriendIndex{delegate: healthyIndex}
	read, err := friend.NewReadAPI(index, pairs, statelessProfiles{}, statelessOpportunities{}, func() int64 { return 1000 })
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(ServiceOptions{
		Shares: &statelessShareProjections{values: make(map[string]ShareProjection)}, Domain: domain,
		Index: index, Read: read, RepairQueue: &statelessRepairQueue{}, Navigation: statelessNavigation{},
		Profiles: statelessProfiles{}, FriendLimit: 1, Now: func() time.Time { return time.UnixMilli(1000) },
	})
	if err != nil {
		t.Fatal(err)
	}
	createAndRedeemStateless(t, service, 2, 1, "fill-limit")
	index.applyCalls = 0
	created, err := service.CreateShare(context.Background(), CreateShareInput{SubjectPlayerID: 3})
	if err != nil {
		t.Fatal(err)
	}
	rejected, err := service.RedeemShare(context.Background(), RedeemShareInput{
		RedeemerPlayerID: 1, ShareToken: created.ShareToken, OperationID: "reject-limit"})
	if err != nil || rejected.Outcome.Status != friend.StatusRejected ||
		rejected.Outcome.Reason != friend.ReasonFriendLimitReached || rejected.Pair.RelationID != "" ||
		rejected.Pair.RelationEpoch != 0 || rejected.Pair.Changed || index.applyCalls != 0 {
		t.Fatalf("rejected=%+v projection_calls=%d err=%v", rejected, index.applyCalls, err)
	}
}

func (i *faultingFriendIndex) Scan(ctx context.Context, viewer uint64, cursor string, limit int) (friend.FriendIndexScan, error) {
	i.scanCalls++
	return i.delegate.Scan(ctx, viewer, cursor, limit)
}

func (i *faultingFriendIndex) Lookup(ctx context.Context, viewer, peer uint64) (friend.FriendIndexEntry, bool, error) {
	i.lookupCalls++
	return i.delegate.Lookup(ctx, viewer, peer)
}

func (i *faultingFriendIndex) BatchLookup(ctx context.Context, viewer uint64, peers []uint64) ([]friend.FriendIndexEntry, error) {
	i.batchLookupCalls++
	return i.delegate.BatchLookup(ctx, viewer, peers)
}

func (i *faultingFriendIndex) RosterRevision(ctx context.Context, viewer uint64) (uint64, error) {
	i.revisionCalls++
	return i.delegate.RosterRevision(ctx, viewer)
}

func (q *statelessRepairQueue) Enqueue(_ context.Context, viewer uint64, _ int64) error {
	if q.err != nil {
		return q.err
	}
	q.viewers = append(q.viewers, viewer)
	return nil
}
func (*statelessRepairQueue) Claim(context.Context, int64, int64, int) ([]friend.FriendIndexRepairLease, error) {
	return nil, nil
}
func (*statelessRepairQueue) Complete(context.Context, friend.FriendIndexRepairLease, int64, int64) error {
	return nil
}
func (*statelessRepairQueue) Retry(context.Context, friend.FriendIndexRepairLease, int64) error {
	return nil
}

type statelessNavigation struct{}

func (statelessNavigation) MarkVisited(context.Context, friend.VisitFence) error { return nil }
func (statelessNavigation) ListVisited(context.Context, uint64) (map[uint64]struct{}, error) {
	return map[uint64]struct{}{}, nil
}

func newStatelessServiceOptions(t *testing.T, repairQueue friend.FriendIndexRepairQueue) (ServiceOptions,
	*friend.MemoryPairRepository, *friend.MemoryQuotaRepository, *friend.MemoryFriendIndexRepository) {
	t.Helper()
	pairs := friend.NewMemoryPairRepository()
	quotas := friend.NewMemoryQuotaRepository(300)
	issuers := friend.NewMemoryShareIssuerRepository((7 * 24 * time.Hour).Milliseconds())
	domain, err := friend.NewDomainService(pairs, quotas, issuers)
	if err != nil {
		t.Fatal(err)
	}
	index := friend.NewMemoryFriendIndexRepository()
	profiles := statelessProfiles{}
	read, err := friend.NewReadAPI(index, pairs, profiles, statelessOpportunities{}, func() int64 { return 1000 })
	if err != nil {
		t.Fatal(err)
	}
	options := ServiceOptions{Shares: &statelessShareProjections{values: make(map[string]ShareProjection)},
		Domain: domain, Index: index, Read: read,
		RepairQueue: repairQueue, Navigation: statelessNavigation{}, Profiles: profiles,
		FriendLimit: 300, Now: func() time.Time { return time.UnixMilli(1000) }}
	return options, pairs, quotas, index
}

func TestGetNextFriendOpportunityV3UsesGlobalOrderAndVisitedFence(t *testing.T) {
	repairQueue := &statelessRepairQueue{}
	options, _, _, index := newStatelessServiceOptions(t, repairQueue)
	countingIndex := &faultingFriendIndex{delegate: index}
	options = replaceStatelessIndex(t, options, countingIndex)
	opportunities := statelessOpportunities{}
	read, err := friend.NewReadAPI(options.Index, options.Domain.Pairs, statelessProfiles{}, opportunities,
		func() int64 { return 1000 })
	if err != nil {
		t.Fatal(err)
	}
	navigator, err := friend.NewNavigator(opportunities, opportunities, friend.DefaultScanBatchSize)
	if err != nil {
		t.Fatal(err)
	}
	navigator.Relations = read
	navigation := friend.NewMemoryNavigationRepository(30 * time.Minute)
	options.Read = read
	options.Navigation = navigation
	options.Navigator = navigator
	options.NavigationState = navigation
	options.NavigationV3Enabled = true
	service, err := NewService(options)
	if err != nil {
		t.Fatal(err)
	}
	createAndRedeemStateless(t, service, 2, 1, "friend-2")
	createAndRedeemStateless(t, service, 3, 1, "friend-3")
	countingIndex.scanCalls = 0

	first, err := service.GetNextFriendOpportunity(context.Background(), GetNextFriendOpportunityInput{ViewerPlayerID: 1})
	if err != nil || first.Status != NextOpportunityFound || first.Friend == nil || first.Friend.FriendPlayerID != 3 {
		t.Fatalf("first=%+v err=%v, want latest farm 3", first, err)
	}
	firstScanCalls := countingIndex.scanCalls
	repeated, err := service.GetNextFriendOpportunity(context.Background(), GetNextFriendOpportunityInput{ViewerPlayerID: 1})
	if err != nil || repeated.Status != NextOpportunityFound || repeated.Friend == nil || repeated.Friend.FriendPlayerID != 3 {
		t.Fatalf("repeated=%+v err=%v, recommendation advanced before visited fence", repeated, err)
	}
	if firstScanCalls != 1 || countingIndex.scanCalls != firstScanCalls || countingIndex.revisionCalls != 1 {
		t.Fatalf("index scans first=%d repeated_delta=%d revision=%d, want one full scan then revision HGET",
			firstScanCalls, countingIndex.scanCalls-firstScanCalls, countingIndex.revisionCalls)
	}
	if err := navigation.MarkVisited(context.Background(), friend.VisitFence{ViewerPlayerID: 1, TargetFarmID: 3,
		ActiveSessionEpoch: 1, SubscriptionID: "sub-3"}); err != nil {
		t.Fatal(err)
	}
	second, err := service.GetNextFriendOpportunity(context.Background(), GetNextFriendOpportunityInput{ViewerPlayerID: 1})
	if err != nil || second.Status != NextOpportunityFound || second.Friend == nil || second.Friend.FriendPlayerID != 2 {
		t.Fatalf("second=%+v err=%v, want remaining farm 2", second, err)
	}
	if err := navigation.MarkVisited(context.Background(), friend.VisitFence{ViewerPlayerID: 1, TargetFarmID: 2,
		ActiveSessionEpoch: 1, SubscriptionID: "sub-2"}); err != nil {
		t.Fatal(err)
	}
	exhausted, err := service.GetNextFriendOpportunity(context.Background(), GetNextFriendOpportunityInput{ViewerPlayerID: 1})
	if err != nil || exhausted.Status != NextOpportunityNone || exhausted.Friend != nil {
		t.Fatalf("exhausted=%+v err=%v", exhausted, err)
	}
}

func TestGetNextFriendOpportunityV3HintRevalidatesAndUpdatesNavigation(t *testing.T) {
	repairQueue := &statelessRepairQueue{}
	options, _, _, index := newStatelessServiceOptions(t, repairQueue)
	opportunities := statelessOpportunities{
		actionableAt:        map[uint64]int64{2: 5_000, 3: 2_000},
		opportunityRevision: map[uint64]uint64{2: 9, 3: 1},
	}
	read, err := friend.NewReadAPI(index, options.Domain.Pairs, statelessProfiles{}, opportunities,
		func() int64 { return 1_000 })
	if err != nil {
		t.Fatal(err)
	}
	navigator, err := friend.NewNavigator(opportunities, opportunities, friend.DefaultScanBatchSize)
	if err != nil {
		t.Fatal(err)
	}
	navigator.Relations = read
	navigation := friend.NewMemoryNavigationRepository(30 * time.Minute)
	options.Read = read
	options.Navigation = navigation
	options.Navigator = navigator
	options.NavigationState = navigation
	options.NavigationV3Enabled = true
	service, err := NewService(options)
	if err != nil {
		t.Fatal(err)
	}
	createAndRedeemStateless(t, service, 2, 1, "friend-2-hint")
	createAndRedeemStateless(t, service, 3, 1, "friend-3-hint")

	hinted, err := service.GetNextFriendOpportunity(context.Background(), GetNextFriendOpportunityInput{
		ViewerPlayerID: 1,
		HintedTargets:  []FriendOpportunityHintedTarget{{OwnerID: 2, OpportunityRevision: 9}},
	})
	if err != nil || hinted.Status != NextOpportunityFound || hinted.Friend == nil || hinted.Friend.FriendPlayerID != 2 {
		t.Fatalf("hinted=%+v err=%v, want exact farm 2", hinted, err)
	}
	snapshot, err := navigation.Load(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Navigation.CandidateWindow) != 1 || snapshot.Navigation.CandidateWindow[0].FarmID != 2 {
		t.Fatalf("candidate window=%+v, want hinted farm 2 persisted with the response", snapshot.Navigation.CandidateWindow)
	}

	stale, err := service.GetNextFriendOpportunity(context.Background(), GetNextFriendOpportunityInput{
		ViewerPlayerID: 1,
		HintedTargets:  []FriendOpportunityHintedTarget{{OwnerID: 2, OpportunityRevision: 10}},
	})
	if err != nil || stale.Status != NextOpportunityUnknown || stale.Friend != nil {
		t.Fatalf("stale hint=%+v err=%v, want UNKNOWN without restoring an older READY", stale, err)
	}
	preserved, err := navigation.Load(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(preserved.Navigation.CandidateWindow) != 1 || preserved.Navigation.CandidateWindow[0].FarmID != 2 {
		t.Fatalf("candidate window=%+v, stale hint must preserve the previous window", preserved.Navigation.CandidateWindow)
	}
}

func replaceStatelessIndex(t *testing.T, options ServiceOptions, index friend.FriendIndexRepository) ServiceOptions {
	t.Helper()
	read, err := friend.NewReadAPI(index, options.Domain.Pairs, statelessProfiles{}, statelessOpportunities{},
		func() int64 { return 1000 })
	if err != nil {
		t.Fatal(err)
	}
	options.Index = index
	options.Read = read
	return options
}

func createAndRedeemStateless(t *testing.T, service *Service, inviter, redeemer uint64, operationID string) RedeemShareResult {
	t.Helper()
	created, err := service.CreateShare(context.Background(), CreateShareInput{SubjectPlayerID: inviter})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.RedeemShare(context.Background(), RedeemShareInput{
		RedeemerPlayerID: redeemer, ShareToken: created.ShareToken, OperationID: operationID,
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestAnyInstanceCanServeSharedStatelessFriendshipDomain(t *testing.T) {
	repairQueue := &statelessRepairQueue{}
	shared, pairs, quotas, index := newStatelessServiceOptions(t, repairQueue)
	instanceA, err := NewService(shared)
	if err != nil {
		t.Fatal(err)
	}
	instanceB, err := NewService(shared)
	if err != nil {
		t.Fatal(err)
	}

	created, err := instanceA.CreateShare(context.Background(), CreateShareInput{SubjectPlayerID: 2})
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := instanceB.ResolveShare(context.Background(), ResolveShareInput{ShareToken: created.ShareToken})
	if err != nil || resolved.InviterPlayerID != 2 {
		t.Fatalf("instance B resolve=%+v err=%v", resolved, err)
	}
	if _, err := instanceB.RedeemShare(context.Background(), RedeemShareInput{RedeemerPlayerID: 1, ShareToken: created.ShareToken}); err != nil {
		t.Fatalf("instance B redeem: %v", err)
	}
	listed, err := instanceA.GetFriendList(context.Background(), GetFriendListInput{ViewerPlayerID: 1, PageSize: 20})
	if err != nil || len(listed.Page.Friends) != 1 || listed.Page.Friends[0].FriendPlayerID != 2 {
		t.Fatalf("instance A list=%+v err=%v", listed, err)
	}
	pair, err := pairs.GetPair(context.Background(), 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]uint64{{1, 2}, {2, 1}} {
		removed, removeErr := index.Remove(context.Background(), edge[0], edge[1], pair.Relation.RelationID, pair.Relation.RelationEpoch)
		if removeErr != nil || !removed {
			t.Fatalf("remove projection %d->%d changed=%v err=%v", edge[0], edge[1], removed, removeErr)
		}
	}
	repairQueue.viewers = nil
	unblocked, err := instanceA.UnblockFriend(context.Background(), PairMutationInput{
		CallerPlayerID: 1, PeerPlayerID: 2, OperationID: "unblock-already-clear",
	})
	if err != nil || unblocked.Outcome.Status != friend.StatusNoChange || unblocked.Pair.Status != friend.RelationStatusActive {
		t.Fatalf("unblock active unblocked relation=%+v err=%v", unblocked, err)
	}
	if len(repairQueue.viewers) != 2 || repairQueue.viewers[0] != 1 || repairQueue.viewers[1] != 2 {
		t.Fatalf("unblock repair viewers=%v, want [1 2]", repairQueue.viewers)
	}
	reconciler, err := friend.NewFriendIndexReconciler(index, pairs, quotas)
	if err != nil {
		t.Fatal(err)
	}
	for _, viewer := range repairQueue.viewers {
		report, reconcileErr := reconciler.ReconcileViewer(context.Background(), viewer)
		if reconcileErr != nil || report.CanonicalRows != 1 || report.ProjectedRows != 1 {
			t.Fatalf("reconcile viewer=%d report=%+v err=%v", viewer, report, reconcileErr)
		}
	}
	repaired, err := instanceB.GetFriendList(context.Background(), GetFriendListInput{ViewerPlayerID: 1, PageSize: 20})
	if err != nil || len(repaired.Page.Friends) != 1 || repaired.Page.Friends[0].FriendPlayerID != 2 {
		t.Fatalf("repaired friend list=%+v err=%v", repaired, err)
	}
	visit, err := instanceA.AuthorizeVisit(context.Background(), AuthorizeVisitInput{ViewerPlayerID: 1, OwnerPlayerID: 2,
		TargetFarmID: 2, JoinOperationID: "join", JoinAttemptID: "attempt", NowMs: 1100, TTLMs: 1000})
	if err != nil || visit.Outcome.Status != friend.StatusSucceeded || visit.Permit.RelationEpoch == 0 {
		t.Fatalf("VisitPermit regression result=%+v err=%v", visit, err)
	}
	action := friend.ActionAdmissionCommand{Identity: friend.CommitIdentity{SubjectPlayerID: 1, CommandType: "CARE", OperationID: "care-op"},
		PayloadHash: []byte("care-payload"), VisitorPlayerID: 1, OwnerPlayerID: 2, TargetFarmID: 2,
		Scope: friend.ActionScopeCare, TargetBindingHash: []byte("binding"), NowMs: 1200,
		PermitTTLMs: 1000, WorkflowTTLMs: 10_000}
	admitted, err := instanceB.CommitFriendActionAdmission(context.Background(), action)
	if err != nil || admitted.Outcome.Status != friend.StatusSucceeded || admitted.Permit.PermitID == "" {
		t.Fatalf("ActionAdmission regression result=%+v err=%v", admitted, err)
	}
	replayed, err := instanceA.GetFriendActionAdmission(context.Background(), action)
	if err != nil || replayed.Permit.PermitID != admitted.Permit.PermitID || replayed.DispatchOrderID != admitted.DispatchOrderID {
		t.Fatalf("cross-instance ActionAdmission replay=%+v err=%v", replayed, err)
	}
}

func TestUnblockFailsClosedBeforeCanonicalCommitWhenRepairEnqueueFails(t *testing.T) {
	enqueueErr := errors.New("repair queue unavailable")
	options, pairs, _, _ := newStatelessServiceOptions(t, &statelessRepairQueue{err: enqueueErr})
	service, err := NewService(options)
	if err != nil {
		t.Fatal(err)
	}
	seedOutcome, err := options.Domain.BlockPlayer(context.Background(), 1, 2, "seed-block", []byte("seed-block"), 900)
	if err != nil || seedOutcome.Status != friend.StatusSucceeded {
		t.Fatalf("seed block outcome=%+v err=%v", seedOutcome, err)
	}
	if _, err := service.UnblockFriend(context.Background(), PairMutationInput{
		CallerPlayerID: 1, PeerPlayerID: 2, OperationID: "unblock",
	}); !errors.Is(err, enqueueErr) {
		t.Fatalf("unblock enqueue error=%v", err)
	}
	pair, err := pairs.GetPair(context.Background(), 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !pair.HasActiveBlock() {
		t.Fatalf("unblock committed despite failed durable repair enqueue: %+v", pair)
	}
}

func TestRepairEnqueueFailurePreventsCanonicalAcceptDeleteAndBlock(t *testing.T) {
	enqueueErr := errors.New("repair queue unavailable")
	t.Run("accept", func(t *testing.T) {
		options, pairs, _, _ := newStatelessServiceOptions(t, &statelessRepairQueue{err: enqueueErr})
		service, err := NewService(options)
		if err != nil {
			t.Fatal(err)
		}
		created, err := service.CreateShare(context.Background(), CreateShareInput{SubjectPlayerID: 2})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = service.RedeemShare(context.Background(), RedeemShareInput{
			RedeemerPlayerID: 1, ShareToken: created.ShareToken, OperationID: "accept-no-repair",
		}); !errors.Is(err, enqueueErr) {
			t.Fatalf("accept enqueue error=%v", err)
		}
		pair, err := pairs.GetPair(context.Background(), 1, 2)
		if err != nil || pair.Relation.Status != friend.RelationStatusNone {
			t.Fatalf("accept committed without repair intent: pair=%+v err=%v", pair, err)
		}
	})
	t.Run("delete", func(t *testing.T) {
		options, pairs, _, _ := newStatelessServiceOptions(t, &statelessRepairQueue{})
		seed, err := NewService(options)
		if err != nil {
			t.Fatal(err)
		}
		createAndRedeemStateless(t, seed, 2, 1, "accept-before-enqueue-failure")
		options.RepairQueue = &statelessRepairQueue{err: enqueueErr}
		service, err := NewService(options)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = service.DeleteFriend(context.Background(), PairMutationInput{
			CallerPlayerID: 1, PeerPlayerID: 2, OperationID: "delete-no-repair",
		}); !errors.Is(err, enqueueErr) {
			t.Fatalf("delete enqueue error=%v", err)
		}
		pair, err := pairs.GetPair(context.Background(), 1, 2)
		if err != nil || pair.Relation.Status != friend.RelationStatusActive || pair.Relation.RelationEpoch != 1 {
			t.Fatalf("delete committed without repair intent: pair=%+v err=%v", pair, err)
		}
	})
	t.Run("block", func(t *testing.T) {
		options, pairs, _, _ := newStatelessServiceOptions(t, &statelessRepairQueue{err: enqueueErr})
		service, err := NewService(options)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = service.BlockFriend(context.Background(), PairMutationInput{
			CallerPlayerID: 1, PeerPlayerID: 2, OperationID: "block-no-repair",
		}); !errors.Is(err, enqueueErr) {
			t.Fatalf("block enqueue error=%v", err)
		}
		pair, err := pairs.GetPair(context.Background(), 1, 2)
		if err != nil || pair.Exists() {
			t.Fatalf("block committed without repair intent: pair=%+v err=%v", pair, err)
		}
	})
}

func TestPairSuccessSurvivesProjectionFailureAndRepairConverges(t *testing.T) {
	projectionErr := errors.New("projection unavailable")
	queue := &statelessRepairQueue{}
	options, pairs, quotas, healthyIndex := newStatelessServiceOptions(t, queue)
	faultIndex := &faultingFriendIndex{delegate: healthyIndex, applyErr: projectionErr}
	options = replaceStatelessIndex(t, options, faultIndex)
	service, err := NewService(options)
	if err != nil {
		t.Fatal(err)
	}
	accepted := createAndRedeemStateless(t, service, 2, 1, "accept-projection-failure")
	if accepted.Outcome.Status != friend.StatusSucceeded || !accepted.Pair.Changed ||
		accepted.Pair.Status != friend.RelationStatusActive {
		t.Fatalf("canonical accept=%+v", accepted)
	}
	omitted, err := service.GetFriendList(context.Background(), GetFriendListInput{ViewerPlayerID: 1, PageSize: 20})
	if err != nil || len(omitted.Page.Friends) != 0 {
		t.Fatalf("projection failure list=%+v err=%v", omitted, err)
	}
	faultIndex.applyErr = nil
	reconciler, err := friend.NewFriendIndexReconciler(healthyIndex, pairs, quotas)
	if err != nil {
		t.Fatal(err)
	}
	for _, viewer := range []uint64{1, 2} {
		if _, err := reconciler.ReconcileViewer(context.Background(), viewer); err != nil {
			t.Fatal(err)
		}
	}
	repaired, err := service.GetFriendList(context.Background(), GetFriendListInput{ViewerPlayerID: 1, PageSize: 20})
	if err != nil || len(repaired.Page.Friends) != 1 || repaired.Page.Friends[0].FriendPlayerID != 2 {
		t.Fatalf("repaired list=%+v err=%v", repaired, err)
	}
	noChange, err := service.RedeemShare(context.Background(), RedeemShareInput{
		RedeemerPlayerID: 1, ShareToken: "token-2",
		OperationID: "accept-no-change",
	})
	if err != nil || noChange.Outcome.Status != friend.StatusNoChange || noChange.Pair.Changed {
		t.Fatalf("accept no-change=%+v err=%v", noChange, err)
	}
}

func TestDeleteAndBlockHideStaleIndexAndFencePermitsDespiteProjectionFailure(t *testing.T) {
	for _, mutation := range []string{"delete", "block"} {
		t.Run(mutation, func(t *testing.T) {
			queue := &statelessRepairQueue{}
			options, pairs, quotas, healthyIndex := newStatelessServiceOptions(t, queue)
			faultIndex := &faultingFriendIndex{delegate: healthyIndex}
			options = replaceStatelessIndex(t, options, faultIndex)
			service, err := NewService(options)
			if err != nil {
				t.Fatal(err)
			}
			createAndRedeemStateless(t, service, 2, 1, "accept-before-"+mutation)
			faultIndex.removeErr = errors.New("projection remove unavailable")
			var result PairMutationResult
			if mutation == "delete" {
				result, err = service.DeleteFriend(context.Background(), PairMutationInput{
					CallerPlayerID: 1, PeerPlayerID: 2, OperationID: "delete-projection-failure"})
			} else {
				result, err = service.BlockFriend(context.Background(), PairMutationInput{
					CallerPlayerID: 1, PeerPlayerID: 2, OperationID: "block-projection-failure"})
			}
			if err != nil || result.Outcome.Status != friend.StatusSucceeded || !result.Pair.Changed ||
				result.Pair.Status != friend.RelationStatusDeleted {
				t.Fatalf("%s result=%+v err=%v", mutation, result, err)
			}
			hidden, listErr := service.GetFriendList(context.Background(), GetFriendListInput{ViewerPlayerID: 1, PageSize: 20})
			if listErr != nil || len(hidden.Page.Friends) != 0 {
				t.Fatalf("stale index leaked after %s: %+v err=%v", mutation, hidden, listErr)
			}
			visit, visitErr := service.AuthorizeVisit(context.Background(), AuthorizeVisitInput{
				ViewerPlayerID: 1, OwnerPlayerID: 2, TargetFarmID: 2, JoinOperationID: "join-" + mutation,
				JoinAttemptID: "attempt-" + mutation, NowMs: 1100, TTLMs: 1000})
			if visitErr != nil || visit.Outcome.Status != friend.StatusRejected {
				t.Fatalf("visit after %s=%+v err=%v", mutation, visit, visitErr)
			}
			admission, admissionErr := service.CommitFriendActionAdmission(context.Background(), friend.ActionAdmissionCommand{
				Identity:    friend.CommitIdentity{SubjectPlayerID: 1, CommandType: "CARE", OperationID: "care-" + mutation},
				PayloadHash: []byte("care-" + mutation), VisitorPlayerID: 1, OwnerPlayerID: 2, TargetFarmID: 2,
				Scope: friend.ActionScopeCare, TargetBindingHash: []byte("binding"), NowMs: 1100,
				PermitTTLMs: 1000, WorkflowTTLMs: 10_000})
			if admissionErr != nil || admission.Outcome.Status != friend.StatusRejected {
				t.Fatalf("admission after %s=%+v err=%v", mutation, admission, admissionErr)
			}
			faultIndex.removeErr = nil
			reconciler, reconcileErr := friend.NewFriendIndexReconciler(healthyIndex, pairs, quotas)
			if reconcileErr != nil {
				t.Fatal(reconcileErr)
			}
			for _, viewer := range []uint64{1, 2} {
				if _, reconcileErr = reconciler.ReconcileViewer(context.Background(), viewer); reconcileErr != nil {
					t.Fatal(reconcileErr)
				}
			}
			if _, exists, lookupErr := healthyIndex.Lookup(context.Background(), 1, 2); lookupErr != nil || exists {
				t.Fatalf("stale projection remains after reconcile: exists=%v err=%v", exists, lookupErr)
			}
			var noChange PairMutationResult
			if mutation == "delete" {
				noChange, err = service.DeleteFriend(context.Background(), PairMutationInput{
					CallerPlayerID: 1, PeerPlayerID: 2, OperationID: "delete-no-change"})
			} else {
				noChange, err = service.BlockFriend(context.Background(), PairMutationInput{
					CallerPlayerID: 1, PeerPlayerID: 2, OperationID: "block-no-change"})
			}
			if err != nil || noChange.Outcome.Status != friend.StatusNoChange || noChange.Pair.Changed {
				t.Fatalf("%s no-change=%+v err=%v", mutation, noChange, err)
			}
		})
	}
}

func TestUnblockNoChangeProjectionFailureKeepsCanonicalReceiptAndRepairs(t *testing.T) {
	options, pairs, quotas, healthyIndex := newStatelessServiceOptions(t, &statelessRepairQueue{})
	faultIndex := &faultingFriendIndex{delegate: healthyIndex}
	options = replaceStatelessIndex(t, options, faultIndex)
	service, err := NewService(options)
	if err != nil {
		t.Fatal(err)
	}
	accepted := createAndRedeemStateless(t, service, 2, 1, "accept-before-unblock-projection")
	for _, direction := range [][2]uint64{{1, 2}, {2, 1}} {
		if _, err := healthyIndex.Remove(context.Background(), direction[0], direction[1],
			accepted.Pair.RelationID, accepted.Pair.RelationEpoch); err != nil {
			t.Fatal(err)
		}
	}
	faultIndex.applyErr = errors.New("projection apply unavailable")
	result, err := service.UnblockFriend(context.Background(), PairMutationInput{
		CallerPlayerID: 1, PeerPlayerID: 2, OperationID: "already-unblocked"})
	if err != nil || result.Outcome.Status != friend.StatusNoChange || result.Pair.Changed ||
		result.Pair.Status != friend.RelationStatusActive {
		t.Fatalf("unblock no-change=%+v err=%v", result, err)
	}
	faultIndex.applyErr = nil
	reconciler, err := friend.NewFriendIndexReconciler(healthyIndex, pairs, quotas)
	if err != nil {
		t.Fatal(err)
	}
	for _, viewer := range []uint64{1, 2} {
		if _, err := reconciler.ReconcileViewer(context.Background(), viewer); err != nil {
			t.Fatal(err)
		}
	}
	if _, exists, err := healthyIndex.Lookup(context.Background(), 1, 2); err != nil || !exists {
		t.Fatalf("unblock repair missing projection: exists=%v err=%v", exists, err)
	}
}

// buildV3NavigationService wires a V3 service whose persisted navigation is
// seeded directly through the memory repository. The returned index is a
// scan-counting wrapper so tests can prove the roster was rebuilt.
func buildV3NavigationService(t *testing.T, windowMode friend.NavigationWindowMode, windowSize int,
	seed *friend.Navigation) (*Service, *friend.MemoryNavigationRepository, *faultingFriendIndex, *statelessRepairQueue) {
	t.Helper()
	repairQueue := &statelessRepairQueue{}
	options, _, _, memoryIndex := newStatelessServiceOptions(t, repairQueue)
	countingIndex := &faultingFriendIndex{delegate: memoryIndex}
	options = replaceStatelessIndex(t, options, countingIndex)
	opportunities := statelessOpportunities{}
	read, err := friend.NewReadAPI(options.Index, options.Domain.Pairs, statelessProfiles{}, opportunities,
		func() int64 { return 1000 })
	if err != nil {
		t.Fatal(err)
	}
	navigator, err := friend.NewNavigator(opportunities, opportunities, friend.DefaultScanBatchSize)
	if err != nil {
		t.Fatal(err)
	}
	navigator.Relations = read
	navigation := friend.NewMemoryNavigationRepository(30 * time.Minute)
	options.Read = read
	options.Navigation = navigation
	options.Navigator = navigator
	options.NavigationState = navigation
	options.NavigationV3Enabled = true
	options.NavigationWindowMode = windowMode
	options.NavigationWindowSize = windowSize
	service, err := NewService(options)
	if err != nil {
		t.Fatal(err)
	}
	if seed != nil {
		snapshot, err := navigation.Load(context.Background(), seed.ViewerPlayerID)
		if err != nil {
			t.Fatal(err)
		}
		if err := navigation.Save(context.Background(), friend.NavigationSnapshot{Navigation: seed, Version: snapshot.Version}); err != nil {
			t.Fatal(err)
		}
	}
	return service, navigation, countingIndex, repairQueue
}

func TestF001PersistedTopKReconcilesToEmptyFullTailConfig(t *testing.T) {
	seed := friend.NewNavigation(1, 7, (30 * time.Minute).Milliseconds(), 1000)
	seed.WindowMode = friend.NavigationWindowModeTopK
	seed.WindowSize = 32
	seed.TailComplete = true
	seed.CandidateWindow = []friend.CandidateWindowEntry{{
		FarmID: 2, SourceFarmVersion: 4, ActionableAtMs: 900, ActionMask: friend.ActionUnknown,
	}}
	seed.MarkVisited(friend.VisitFence{ViewerPlayerID: 1, TargetFarmID: 3,
		ActiveSessionEpoch: 1, SubscriptionID: "sub-3"})

	service, navigation, index, _ := buildV3NavigationService(t, "", 0, seed)
	// No friends exist yet, so a full-tail rebuild scans an empty roster and
	// answers NONE after building a complete tail.
	answer, err := service.GetNextFriendOpportunity(context.Background(), GetNextFriendOpportunityInput{ViewerPlayerID: 1})
	if err != nil {
		t.Fatal(err)
	}
	if answer.Status != NextOpportunityNone {
		t.Fatalf("answer=%+v, want NONE after full-tail rebuild of empty roster", answer)
	}
	persisted, err := navigation.Load(context.Background(), 1)
	if err != nil || persisted.Navigation == nil {
		t.Fatalf("persisted=%+v err=%v", persisted, err)
	}
	if persisted.Navigation.WindowMode != friend.NavigationWindowModeFullTail {
		t.Fatalf("persisted mode=%q, want full-tail after rollback", persisted.Navigation.WindowMode)
	}
	// The persisted tail was rebuilt from the roster, not served from the
	// stale top-k payload: the rebuild drops the truncated candidates.
	if len(persisted.Navigation.CandidateWindow) != 0 {
		t.Fatalf("full-tail rebuild kept truncated top-k candidates: %+v", persisted.Navigation)
	}
	if _, ok := persisted.Navigation.Visited[3]; !ok {
		t.Fatalf("visited mark lost during reconciliation: %+v", persisted.Navigation.Visited)
	}
	if index.scanCalls == 0 {
		t.Fatal("full-tail rollback must rebuild from the roster")
	}
}

func TestF001PersistedTopKWithIncompleteTailReconcilesToFullTail(t *testing.T) {
	seed := friend.NewNavigation(1, 7, (30 * time.Minute).Milliseconds(), 1000)
	seed.WindowMode = friend.NavigationWindowModeTopK
	seed.WindowSize = 32
	seed.TailComplete = false // truncated top-k tail
	seed.CandidateWindow = []friend.CandidateWindowEntry{{
		FarmID: 2, SourceFarmVersion: 4, ActionableAtMs: 900, ActionMask: friend.ActionUnknown,
	}}
	seed.MarkVisited(friend.VisitFence{ViewerPlayerID: 1, TargetFarmID: 3,
		ActiveSessionEpoch: 1, SubscriptionID: "sub-3"})

	service, navigation, index, _ := buildV3NavigationService(t, friend.NavigationWindowModeFullTail, 0, seed)
	answer, err := service.GetNextFriendOpportunity(context.Background(), GetNextFriendOpportunityInput{ViewerPlayerID: 1})
	if err != nil {
		t.Fatal(err)
	}
	if answer.Status != NextOpportunityNone {
		t.Fatalf("answer=%+v, want NONE after full-tail rebuild of empty roster", answer)
	}
	persisted, err := navigation.Load(context.Background(), 1)
	if err != nil || persisted.Navigation == nil {
		t.Fatalf("persisted=%+v err=%v", persisted, err)
	}
	if persisted.Navigation.WindowMode != friend.NavigationWindowModeFullTail {
		t.Fatalf("persisted mode=%q, want full-tail", persisted.Navigation.WindowMode)
	}
	if _, ok := persisted.Navigation.Visited[3]; !ok {
		t.Fatalf("visited mark lost during reconciliation: %+v", persisted.Navigation.Visited)
	}
	if len(persisted.Navigation.CandidateWindow) != 0 {
		t.Fatalf("full-tail rebuild kept truncated top-k candidates: %+v", persisted.Navigation)
	}
	if index.scanCalls == 0 {
		t.Fatal("explicit full-tail switch must rebuild a complete tail before serving")
	}
}

func TestF001UnknownConfigReconcilesPersistedTopK(t *testing.T) {
	seed := friend.NewNavigation(1, 7, (30 * time.Minute).Milliseconds(), 1000)
	seed.WindowMode = friend.NavigationWindowModeTopK
	seed.WindowSize = 32
	seed.TailComplete = true
	seed.CandidateWindow = []friend.CandidateWindowEntry{{
		FarmID: 2, SourceFarmVersion: 4, ActionableAtMs: 900, ActionMask: friend.ActionUnknown,
	}}

	service, navigation, _, _ := buildV3NavigationService(t, "garbage-mode", 0, seed)
	answer, err := service.GetNextFriendOpportunity(context.Background(), GetNextFriendOpportunityInput{ViewerPlayerID: 1})
	if err != nil {
		t.Fatal(err)
	}
	if answer.Status != NextOpportunityNone {
		t.Fatalf("answer=%+v, want NONE", answer)
	}
	persisted, err := navigation.Load(context.Background(), 1)
	if err != nil || persisted.Navigation == nil {
		t.Fatalf("persisted=%+v err=%v", persisted, err)
	}
	if persisted.Navigation.WindowMode != friend.NavigationWindowModeFullTail {
		t.Fatalf("unknown config must fail closed to full-tail, got %q", persisted.Navigation.WindowMode)
	}
}

func TestF001ExplicitFullTailConfigReconcilesPersistedTopKWithVisitedPreserved(t *testing.T) {
	seed := friend.NewNavigation(1, 7, (30 * time.Minute).Milliseconds(), 1000)
	seed.WindowMode = friend.NavigationWindowModeTopK
	seed.WindowSize = 32
	seed.TailComplete = false
	seed.CandidateWindow = []friend.CandidateWindowEntry{{
		FarmID: 2, SourceFarmVersion: 4, ActionableAtMs: 900, ActionMask: friend.ActionUnknown,
	}}
	seed.MarkVisited(friend.VisitFence{ViewerPlayerID: 1, TargetFarmID: 4,
		ActiveSessionEpoch: 1, SubscriptionID: "sub-4"})

	service, navigation, _, _ := buildV3NavigationService(t, friend.NavigationWindowModeFullTail, 0, seed)
	createAndRedeemStateless(t, service, 5, 1, "friend-5")
	createAndRedeemStateless(t, service, 6, 1, "friend-6")
	answer, err := service.GetNextFriendOpportunity(context.Background(), GetNextFriendOpportunityInput{ViewerPlayerID: 1})
	if err != nil {
		t.Fatal(err)
	}
	if answer.Status != NextOpportunityFound || answer.Friend == nil {
		t.Fatalf("answer=%+v, want a rebuilt full-tail recommendation", answer)
	}
	if answer.Friend.FriendPlayerID != 6 {
		t.Fatalf("answer farm=%d, want newest farm 6 under full-tail order", answer.Friend.FriendPlayerID)
	}
	persisted, err := navigation.Load(context.Background(), 1)
	if err != nil || persisted.Navigation == nil {
		t.Fatalf("persisted=%+v err=%v", persisted, err)
	}
	if persisted.Navigation.WindowMode != friend.NavigationWindowModeFullTail {
		t.Fatalf("persisted mode=%q, want full-tail", persisted.Navigation.WindowMode)
	}
	if !persisted.Navigation.TailComplete {
		t.Fatalf("full-tail rebuild must persist a complete tail: %+v", persisted.Navigation)
	}
	if _, ok := persisted.Navigation.Visited[4]; !ok {
		t.Fatalf("visited mark lost during reconciliation: %+v", persisted.Navigation.Visited)
	}
}
