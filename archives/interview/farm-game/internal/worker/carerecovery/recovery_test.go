package carerecovery

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actorrouting"
	"farmgame/internal/contracts"
	"farmgame/internal/identity"
	"farmgame/internal/storage/authoritystore"
	authority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/worker/sourceorder"
	"farmgame/internal/workflow"
	"farmgame/internal/workidentity"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

const (
	testFarmID  = uint64(20002)
	testCarerID = uint64(10001)
)

type fakeLease struct {
	mu                  sync.Mutex
	payload             map[string][]byte
	claims              map[string]authority.WorkClaim
	completed           map[string]bool
	nextEpoch           uint64
	claimCalls          int
	renewCalls          int
	completeCalls       int
	fenceOnRenew        bool
	releaseOnRenew      bool
	payloadHashOverride string
	subjectPlayerID     uint64
	retryCount          uint64
	trustedNowMs        int64
	trustedNowErr       error
}

func newFakeLease() *fakeLease {
	return &fakeLease{payload: make(map[string][]byte), claims: make(map[string]authority.WorkClaim), completed: make(map[string]bool)}
}

func (f *fakeLease) claimSubject() uint64 {
	if f != nil && f.subjectPlayerID != 0 {
		return f.subjectPlayerID
	}
	return testCarerID
}

func (f *fakeLease) payloadHashFor(workID string, payload []byte) string {
	if f.payloadHashOverride != "" {
		return f.payloadHashOverride
	}
	if decoded, decodeErr := DecodeCareRewardPayload(workID, payload); decodeErr == nil {
		if hash, hashErr := CareRewardPayloadHash(decoded); hashErr == nil {
			return fmt.Sprintf("%x", hash[:])
		}
	}
	if decoded, decodeErr := sourceorder.DecodeGrantHarvestPayload(workID, payload); decodeErr == nil {
		if hash, hashErr := sourceorder.HarvestPayloadHash(decoded); hashErr == nil {
			return fmt.Sprintf("%x", hash[:])
		}
	}
	if decoded, decodeErr := sourceorder.DecodeCommitPlantPayload(workID, payload); decodeErr == nil {
		if hash, hashErr := sourceorder.PlantPayloadHash(decoded); hashErr == nil {
			return fmt.Sprintf("%x", hash[:])
		}
	}
	if decoded, decodeErr := sourceorder.DecodeCommitPlotUnlockPayload(workID, payload); decodeErr == nil {
		if hash, hashErr := sourceorder.UnlockPayloadHash(decoded); hashErr == nil {
			return fmt.Sprintf("%x", hash[:])
		}
	}
	return ""
}

func (f *fakeLease) ClaimWork(_ context.Context, bucket rediskey.AuthorityBucketRef, workType, workID, holderID, token string, ttlMs int64) (authority.WorkClaim, error) {
	if err := bucket.Validate(); err != nil {
		return authority.WorkClaim{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claimCalls++
	if f.completed[workID] {
		return authority.WorkClaim{Status: authority.WorkClaimNotClaimable}, nil
	}
	if current, ok := f.claims[workID]; ok && current.Status == authority.WorkClaimClaimed {
		return authority.WorkClaim{Status: authority.WorkClaimLeaseHeld}, nil
	}
	f.nextEpoch++
	claim := authority.WorkClaim{Status: authority.WorkClaimClaimed, WorkID: workID, WorkType: workType,
		SubjectPlayerID: f.claimSubject(), OperationID: "care-operation-1", Payload: append([]byte(nil), f.payload[workID]...),
		RetryCount: f.retryCount, WorkVersion: 1, LeaseEpoch: f.nextEpoch, AcceptedTTLms: ttlMs,
		LeaseExpireAtMs: time.Now().Add(time.Duration(ttlMs) * time.Millisecond).UnixMilli()}
	claim.PayloadHash = f.payloadHashFor(workID, claim.Payload)
	if holderID == "" || token == "" {
		return authority.WorkClaim{}, errors.New("missing fake identity")
	}
	f.claims[workID] = claim
	return claim, nil
}

func (f *fakeLease) TrustedNowMs(context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.trustedNowErr != nil {
		return 0, f.trustedNowErr
	}
	if f.trustedNowMs != 0 {
		return f.trustedNowMs, nil
	}
	return time.Now().UnixMilli(), nil
}

func (f *fakeLease) ClaimWorkWithBoot(ctx context.Context, bucket rediskey.AuthorityBucketRef, workType, workID, holderID, bootID, token string, ttlMs int64) (authority.WorkClaim, error) {
	claim, err := f.ClaimWork(ctx, bucket, workType, workID, holderID, token, ttlMs)
	if err == nil && claim.Status == authority.WorkClaimClaimed {
		f.mu.Lock()
		claim.LeaseHolderBootID = bootID
		f.claims[workID] = claim
		f.mu.Unlock()
	}
	return claim, err
}

func (f *fakeLease) RenewWorkLease(_ context.Context, bucket rediskey.AuthorityBucketRef, workType, workID, holderID string, epoch uint64, token string, ttlMs int64) (authority.WorkLeaseRenewResult, error) {
	if err := bucket.Validate(); err != nil {
		return authority.WorkLeaseRenewResult{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.renewCalls++
	if f.fenceOnRenew {
		return authority.WorkLeaseRenewResult{Status: authority.WorkLeaseFenceMismatch}, nil
	}
	claim, ok := f.claims[workID]
	if !ok || claim.LeaseEpoch != epoch || holderID == "" || token == "" || workType != claim.WorkType {
		return authority.WorkLeaseRenewResult{Status: authority.WorkLeaseFenceMismatch}, nil
	}
	result := authority.WorkLeaseRenewResult{Status: authority.WorkLeaseRenewed, AcceptedTTLms: ttlMs, LeaseExpireAtMs: time.Now().Add(time.Duration(ttlMs) * time.Millisecond).UnixMilli()}
	if f.releaseOnRenew {
		delete(f.claims, workID)
	}
	return result, nil
}

func (f *fakeLease) RenewWorkLeaseWithBoot(ctx context.Context, bucket rediskey.AuthorityBucketRef, workType, workID, holderID, _ string, epoch uint64, token string, ttlMs int64) (authority.WorkLeaseRenewResult, error) {
	return f.RenewWorkLease(ctx, bucket, workType, workID, holderID, epoch, token, ttlMs)
}

func (f *fakeLease) CompleteWork(_ context.Context, bucket rediskey.AuthorityBucketRef, workType, workID, holderID string, epoch, version uint64, token string) (authority.WorkCompleteResult, error) {
	if err := bucket.Validate(); err != nil {
		return authority.WorkCompleteResult{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.completeCalls++
	claim, ok := f.claims[workID]
	if !ok || claim.LeaseEpoch != epoch || version != 1 || workType != claim.WorkType || holderID == "" || token == "" {
		return authority.WorkCompleteResult{Status: authority.WorkLeaseFenceMismatch}, nil
	}
	if f.completed[workID] {
		return authority.WorkCompleteResult{Status: authority.WorkLeaseAlreadyCompleted}, nil
	}
	f.completed[workID] = true
	delete(f.claims, workID)
	return authority.WorkCompleteResult{Status: authority.WorkLeaseCompleted}, nil
}

func (f *fakeLease) CompleteWorkWithProof(ctx context.Context, bucket rediskey.AuthorityBucketRef, workType, workID, holderID, _ string, epoch, version uint64, token string, _ authority.WorkCompletionProof) (authority.WorkCompleteResult, error) {
	return f.CompleteWork(ctx, bucket, workType, workID, holderID, epoch, version, token)
}

func (f *fakeLease) ClaimAndCompleteWithProof(ctx context.Context, bucket rediskey.AuthorityBucketRef, workType, workID, holderID, bootID, token string, ttlMs int64, proof authority.WorkCompletionProof) (authority.WorkCompleteResult, error) {
	claim, err := f.ClaimWorkWithBoot(ctx, bucket, workType, workID, holderID, bootID, token, ttlMs)
	if err != nil {
		return authority.WorkCompleteResult{}, err
	}
	if claim.Status != authority.WorkClaimClaimed {
		return authority.WorkCompleteResult{Status: authority.WorkLeaseFenceMismatch}, nil
	}
	return f.CompleteWorkWithProof(ctx, bucket, workType, workID, holderID, bootID, claim.LeaseEpoch, claim.WorkVersion, token, proof)
}

func (f *fakeLease) RescheduleWork(_ context.Context, bucket rediskey.AuthorityBucketRef, workType, workID, holderID, _ string, epoch, version uint64, token string, retry uint64, _ int64, _ string) (authority.WorkRescheduleResult, error) {
	if err := bucket.Validate(); err != nil {
		return authority.WorkRescheduleResult{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	claim, ok := f.claims[workID]
	if !ok || claim.LeaseEpoch != epoch || claim.WorkVersion != version || holderID == "" || token == "" || workType != claim.WorkType {
		return authority.WorkRescheduleResult{Status: authority.WorkLeaseFenceMismatch}, nil
	}
	delete(f.claims, workID)
	return authority.WorkRescheduleResult{Status: authority.WorkLeaseRenewed, RetryCount: retry}, nil
}

type fakeBatchLease struct {
	*fakeLease
	mu      sync.Mutex
	batches []int
}

func (f *fakeBatchLease) ClaimAndCompleteWithProofBatch(ctx context.Context, requests []authority.WorkClaimAndCompleteRequest) []authority.WorkClaimAndCompleteBatchResult {
	f.mu.Lock()
	f.batches = append(f.batches, len(requests))
	f.mu.Unlock()
	results := make([]authority.WorkClaimAndCompleteBatchResult, len(requests))
	for i, request := range requests {
		results[i].Result, results[i].Err = f.ClaimAndCompleteWithProof(ctx, request.Bucket, request.WorkType, request.WorkID,
			request.HolderID, request.BootID, request.LeaseToken, request.RequestedTTLms, request.Proof)
	}
	return results
}

type fakeDue struct{ refs []authority.DueWorkRef }

type assignmentResolverFunc func(context.Context, actorrouting.Address) (actorrouting.Assignment, error)

func (f assignmentResolverFunc) Resolve(ctx context.Context, address actorrouting.Address) (actorrouting.Assignment, error) {
	return f(ctx, address)
}

func (f fakeDue) ListDueWork(_ context.Context, bucket rediskey.AuthorityBucketRef, limit int) ([]authority.DueWorkRef, error) {
	if err := bucket.Validate(); err != nil {
		return nil, err
	}
	if limit < len(f.refs) {
		return append([]authority.DueWorkRef(nil), f.refs[:limit]...), nil
	}
	return append([]authority.DueWorkRef(nil), f.refs...), nil
}

type fakeParticipant struct {
	mu          sync.Mutex
	requests    []*contractspb.ParticipantRequest
	responses   []*contractspb.ParticipantResponse
	errs        []error
	coordinator bool
}

func (f *fakeParticipant) ResumeStep(_ context.Context, req *contractspb.ParticipantRequest, _ ...grpc.CallOption) (*contractspb.ParticipantResponse, error) {
	return f.call(req)
}

func (f *fakeParticipant) ExecuteStep(_ context.Context, req *contractspb.ParticipantRequest, _ ...grpc.CallOption) (*contractspb.ParticipantResponse, error) {
	return f.call(req)
}

func (f *fakeParticipant) call(req *contractspb.ParticipantRequest) (*contractspb.ParticipantResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, req)
	if len(f.errs) > 0 {
		err := f.errs[0]
		f.errs = f.errs[1:]
		return nil, err
	}
	if len(f.responses) == 0 {
		return nil, errors.New("no fake response")
	}
	response := f.responses[0]
	f.responses = f.responses[1:]
	return response, nil
}

type fakeSourceOrders struct {
	order  authority.SourceOrderSnapshot
	bucket rediskey.AuthorityBucketRef
	onRead func()
}

type gatedCoordinatorParticipant struct {
	mu        sync.Mutex
	requests  []*contractspb.ParticipantRequest
	responses map[string]*contractspb.ParticipantResponse
	started   chan struct{}
	release   chan struct{}
}

func (g *gatedCoordinatorParticipant) ExecuteStep(ctx context.Context, req *contractspb.ParticipantRequest, _ ...grpc.CallOption) (*contractspb.ParticipantResponse, error) {
	g.mu.Lock()
	first := len(g.requests) == 0
	g.requests = append(g.requests, req)
	coordinator, ok := req.GetCallerContext().(*contractspb.ParticipantRequest_Coordinator)
	if !ok {
		g.mu.Unlock()
		return nil, errors.New("missing Coordinator caller context")
	}
	response := g.responses[coordinator.Coordinator.GetParticipantInvocationId()]
	g.mu.Unlock()
	if first {
		close(g.started)
		select {
		case <-g.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if response == nil {
		return nil, errors.New("missing gated response")
	}
	return response, nil
}

func (g *gatedCoordinatorParticipant) RequestCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.requests)
}

type failingSourceOrders struct{}

type mapSourceOrders struct {
	bucket rediskey.AuthorityBucketRef
	orders map[string]authority.SourceOrderSnapshot
}

func (m mapSourceOrders) GetSourceOrder(_ context.Context, bucket rediskey.AuthorityBucketRef, workType, workID string) (authority.SourceOrderSnapshot, error) {
	order, ok := m.orders[workID]
	if !ok || bucket.VirtualShard() != m.bucket.VirtualShard() || order.WorkType != workType {
		return authority.SourceOrderSnapshot{}, errors.New("source order not found")
	}
	order.TypedPayload = append([]byte(nil), order.TypedPayload...)
	return order, nil
}

func (failingSourceOrders) GetSourceOrder(context.Context, rediskey.AuthorityBucketRef, string, string) (authority.SourceOrderSnapshot, error) {
	return authority.SourceOrderSnapshot{}, errors.New("source order readback is forbidden")
}

func (f fakeSourceOrders) GetSourceOrder(_ context.Context, bucket rediskey.AuthorityBucketRef, workType, workID string) (authority.SourceOrderSnapshot, error) {
	if bucket.VirtualShard() != f.bucket.VirtualShard() || workType != f.order.WorkType || workID != f.order.WorkID {
		return authority.SourceOrderSnapshot{}, errors.New("source order not found")
	}
	if f.onRead != nil {
		f.onRead()
	}
	order := f.order
	if order.PayloadHash == "" {
		if payload, decodeErr := DecodeCareRewardPayload(order.WorkID, order.TypedPayload); decodeErr == nil {
			if hash, hashErr := CareRewardPayloadHash(payload); hashErr == nil {
				order.PayloadHash = fmt.Sprintf("%x", hash[:])
			}
		}
	}
	return order, nil
}

func testBucket(t *testing.T) (rediskey.PhysicalProfile, *actorrouting.Calculator, rediskey.AuthorityBucketRef) {
	t.Helper()
	ns, err := rediskey.NewNamespace("test", "z1")
	if err != nil {
		t.Fatal(err)
	}
	profile := rediskey.PhysicalProfile{ProfileVersion: "redis-physical-v1", Namespace: ns, AuthorityLayoutVersion: "v1", WorkflowLayoutVersion: "v1", WorkflowHashVersion: 1, WorkflowBucketCount: 256}
	calculator, err := actorrouting.NewCalculator("classic-farm", actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608040001}, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := profile.AuthorityBucketForActor(calculator, actorrouting.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: testFarmID})
	if err != nil {
		t.Fatal(err)
	}
	return profile, calculator, bucket
}

func testPayload(t *testing.T, orderID, incidentID string) ([]byte, *contractspb.ApplyCareRewardPayload) {
	t.Helper()
	cycleID, err := workidentity.StableOpaqueID("test-care-cycle", incidentID)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := &contractspb.CareResolutionSnapshot{SourceFarmId: testFarmID, SourceOwnerPlayerId: 20003, CarerPlayerId: testCarerID,
		PlotId: 3, CropCycleId: cycleID, IncidentId: incidentID, CareType: contractspb.CareType_CARE_TYPE_WATER,
		CareContext: contractspb.CareContext_CARE_CONTEXT_FRIEND, ResolvedAtMs: 1000, CommittedPlotVersion: 8, CommittedFarmVersion: 12,
		CareRewardRuleVersion: 2, RewardExp: 5, FriendInteractionEligible: true}
	resolutionHash, err := CareResolutionHash(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.CareResolutionHash = append([]byte(nil), resolutionHash[:]...)
	payload := &contractspb.ApplyCareRewardPayload{CarerPlayerId: testCarerID, CareRewardOrderId: orderID, CareResolutionSnapshot: snapshot, CareResolutionHash: append([]byte(nil), snapshot.CareResolutionHash...)}
	bytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return bytes, payload
}

func testResponse(t *testing.T, orderID, incident string, outcome contractspb.ParticipantOutcome) *contractspb.ParticipantResponse {
	t.Helper()
	invocation, err := identity.ParticipantInvocationID(identity.WorkTypeCareReward, orderID, identity.StepApplyCareReward, 1)
	if err != nil {
		t.Fatal(err)
	}
	result := &contractspb.ApplyCareRewardResult{CareRewardOrderId: orderID, IncidentId: incident, GrantedExp: 5, PlayerExpAfter: 10, PlayerLevelAfter: 1, FriendInteractionFactCommitted: true, FriendInteractionTaskAdvanced: true, TaskPeriodKey: "2026-08-05", PlayerRecordVersion: 9}
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_CARE_REWARD)
	if err != nil {
		t.Fatal(err)
	}
	typed, hash, err := contracts.ValidateTypedResult(spec, result)
	if err != nil {
		t.Fatal(err)
	}
	return &contractspb.ParticipantResponse{ParticipantCommand: spec.Command, ParticipantInvocationId: invocation, Outcome: outcome, TypedResult: typed, ParticipantResultHash: hash, CommittedRecordVersion: 9}
}

func testRoute(_ context.Context, actorID uint64) (*logicrpcpb.ActorRouteHeader, error) {
	return &logicrpcpb.ActorRouteHeader{RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER, ActorId: actorID, VirtualShard: 0, ShardRevision: 1, ExpectedHolderId: "player-1", ExpectedHolderBootId: "boot-1", ExpectedWriterEpoch: 1}, nil
}

func TestRecoveryRouteProviderCarriesAndRechecksLeaseFence(t *testing.T) {
	assignment := actorrouting.Assignment{
		ShardID: 3, HolderPod: "player-holder", HolderBootID: "player-boot", Endpoint: "player:9000",
		ShardRevision: 7, WriterEpoch: 9, LeaseEpoch: 11, LeaseUntilMs: 1_900_000_000_000,
	}
	resolver := assignmentResolverFunc(func(_ context.Context, address actorrouting.Address) (actorrouting.Assignment, error) {
		if address.Realm != "classic-farm" || address.Kind != actorrouting.DomainPlayer || address.ID != testCarerID {
			return actorrouting.Assignment{}, actorrouting.ErrNoAssignment
		}
		return assignment, nil
	})
	provider, err := NewPlayerRouteProvider(resolver, "classic-farm")
	if err != nil {
		t.Fatal(err)
	}
	header, err := provider(context.Background(), testCarerID)
	if err != nil {
		t.Fatal(err)
	}
	if header.GetExpectedLeaseEpoch() != assignment.LeaseEpoch || header.ExpectedLeaseUntilMs == nil ||
		header.GetExpectedLeaseUntilMs() != uint64(assignment.LeaseUntilMs) || !sameAssignment(header, assignment) {
		t.Fatalf("route lease fence missing or rejected: header=%+v assignment=%+v", header, assignment)
	}

	extended := assignment
	extended.LeaseUntilMs++
	if !sameAssignment(header, extended) {
		t.Fatal("deadline-only lease extension was rejected")
	}
	shortened := assignment
	shortened.LeaseUntilMs--
	if sameAssignment(header, shortened) {
		t.Fatal("shorter current lease deadline was accepted")
	}
	newEpoch := assignment
	newEpoch.LeaseEpoch++
	if sameAssignment(header, newEpoch) {
		t.Fatal("different lease epoch was accepted")
	}

	incomplete := assignment
	incomplete.LeaseEpoch = 0
	badProvider, err := NewPlayerRouteProvider(assignmentResolverFunc(func(context.Context, actorrouting.Address) (actorrouting.Assignment, error) {
		return incomplete, nil
	}), "classic-farm")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := badProvider(context.Background(), testCarerID); !errors.Is(err, actorrouting.ErrStaleRoute) {
		t.Fatalf("incomplete lease fence err=%v", err)
	}
}

func makeWorker(t *testing.T, lease *fakeLease, due fakeDue, participant *fakeParticipant, payloads map[string][]byte, bucket rediskey.AuthorityBucketRef, calculator *actorrouting.Calculator) *Worker {
	lease.payload = payloads
	worker, err := NewWorker(Config{WorkLease: lease, DueIndex: due, Buckets: []rediskey.AuthorityBucketRef{bucket}, BucketForFarm: func(farmID uint64) (rediskey.AuthorityBucketRef, error) {
		return testBucketForFarm(calculator, farmID)
	}, Route: testRoute, Player: participant, Owner: "worker-1", Token: "token-1", BootID: "boot-1",
		ClaimLimit: 32, MaxConcurrent: 4, LeaseTTL: 2 * time.Second, SafetyMargin: 100 * time.Millisecond, OperationTimeout: time.Second,
		Backoff: workflow.BackoffConfig{Base: time.Millisecond, Max: 5 * time.Millisecond}, Now: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

func testBucketForFarm(calculator *actorrouting.Calculator, farmID uint64) (rediskey.AuthorityBucketRef, error) {
	ns, err := rediskey.NewNamespace("test", "z1")
	if err != nil {
		return rediskey.AuthorityBucketRef{}, err
	}
	profile := rediskey.PhysicalProfile{ProfileVersion: "redis-physical-v1", Namespace: ns, AuthorityLayoutVersion: "v1", WorkflowLayoutVersion: "v1", WorkflowHashVersion: 1, WorkflowBucketCount: 256}
	return profile.AuthorityBucketForActor(calculator, actorrouting.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: farmID})
}

func dueRef(t *testing.T, workID string) authority.DueWorkRef {
	t.Helper()
	member, err := workidentity.EncodeWorkRefV1(FamilyCareReward, workID)
	if err != nil {
		t.Fatal(err)
	}
	return authority.DueWorkRef{Ref: workidentity.WorkRef{WorkType: FamilyCareReward, WorkID: workID}, DueAtMs: time.Now().UnixMilli(), Member: member}
}

func TestWorkerRecoveryGraceDefersFreshDirectOrderUsingRedisEvidence(t *testing.T) {
	_, calculator, bucket := testBucket(t)
	incidentID := testIncidentID(t, "recovery-grace")
	orderID := mustOrderID(t, "recovery-grace")
	payload, _ := testPayload(t, orderID, incidentID)
	trustedNow := time.UnixMilli(1_800_000_000_000)
	member, err := workidentity.EncodeWorkRefV1(FamilyCareReward, orderID)
	if err != nil {
		t.Fatal(err)
	}
	lease := newFakeLease()
	lease.payload[orderID] = payload
	lease.trustedNowMs = trustedNow.UnixMilli()
	order := authority.SourceOrderSnapshot{
		WorkID: orderID, WorkType: FamilyCareReward, TypedPayload: payload,
		CreatedAtMs: trustedNow.Add(-5 * time.Second).UnixMilli(), RetryCount: 0, LeaseEpoch: 0,
	}
	participant := &fakeParticipant{responses: []*contractspb.ParticipantResponse{
		testResponse(t, orderID, incidentID, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED),
	}}
	worker, err := NewWorker(Config{
		WorkLease: lease, SourceOrders: fakeSourceOrders{order: order, bucket: bucket},
		DueIndex: fakeDue{refs: []authority.DueWorkRef{{
			Ref:     workidentity.WorkRef{WorkType: FamilyCareReward, WorkID: orderID},
			DueAtMs: trustedNow.Add(-5 * time.Second).UnixMilli(), Member: member,
		}}}, Buckets: []rediskey.AuthorityBucketRef{bucket},
		BucketForFarm: func(farmID uint64) (rediskey.AuthorityBucketRef, error) { return testBucketForFarm(calculator, farmID) },
		Route:         testRoute, Player: participant, Owner: "worker-grace", Token: "token-grace", BootID: "boot-grace",
		ClaimLimit: 32, MaxConcurrent: 4, LeaseTTL: 2 * time.Second, SafetyMargin: 100 * time.Millisecond,
		OperationTimeout: time.Second, RecoveryGrace: 10 * time.Second,
		// Deliberately skew the process clock. Grace must use Redis TIME and the
		// immutable created_at_ms instead of this local wall clock.
		Now: func() time.Time { return trustedNow.Add(24 * time.Hour) },
	})
	if err != nil {
		t.Fatal(err)
	}
	deferred, err := worker.RunOnce(context.Background())
	if err != nil || deferred.Candidates != 0 || deferred.Claimed != 0 || len(participant.requests) != 0 {
		t.Fatalf("fresh order was not deferred: stats=%+v requests=%d err=%v", deferred, len(participant.requests), err)
	}
	trustedNow = trustedNow.Add(6 * time.Second)
	lease.mu.Lock()
	lease.trustedNowMs = trustedNow.UnixMilli()
	lease.mu.Unlock()
	completed, err := worker.RunOnce(context.Background())
	if err != nil || completed.Claimed != 1 || completed.Completed != 1 || len(participant.requests) != 1 {
		t.Fatalf("aged order was not recovered: stats=%+v requests=%d err=%v", completed, len(participant.requests), err)
	}
}

func TestWorkerRecoveryGraceTakesRedisTimeAfterEvidenceRead(t *testing.T) {
	_, calculator, bucket := testBucket(t)
	incidentID := testIncidentID(t, "recovery-grace-freshness")
	orderID := mustOrderID(t, "recovery-grace-freshness")
	payload, _ := testPayload(t, orderID, incidentID)
	createdAt := time.UnixMilli(1_800_000_000_000)
	lease := newFakeLease()
	lease.payload[orderID] = payload
	// A clock sampled before evidence would still be inside grace.
	lease.trustedNowMs = createdAt.Add(5 * time.Second).UnixMilli()
	order := authority.SourceOrderSnapshot{
		WorkID: orderID, WorkType: FamilyCareReward, TypedPayload: payload,
		CreatedAtMs: createdAt.UnixMilli(), RetryCount: 0, LeaseEpoch: 0,
	}
	var evidenceRead sync.Once
	source := fakeSourceOrders{order: order, bucket: bucket, onRead: func() {
		evidenceRead.Do(func() {
			lease.mu.Lock()
			lease.trustedNowMs = createdAt.Add(11 * time.Second).UnixMilli()
			lease.mu.Unlock()
		})
	}}
	participant := &fakeParticipant{responses: []*contractspb.ParticipantResponse{
		testResponse(t, orderID, incidentID, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED),
	}}
	worker, err := NewWorker(Config{
		WorkLease: lease, SourceOrders: source, DueIndex: fakeDue{refs: []authority.DueWorkRef{dueRef(t, orderID)}},
		Buckets:       []rediskey.AuthorityBucketRef{bucket},
		BucketForFarm: func(farmID uint64) (rediskey.AuthorityBucketRef, error) { return testBucketForFarm(calculator, farmID) },
		Route:         testRoute, Player: participant, Owner: "worker-grace-fresh", Token: "token-grace-fresh", BootID: "boot-grace-fresh",
		LeaseTTL: 2 * time.Second, SafetyMargin: 100 * time.Millisecond, OperationTimeout: time.Second,
		RecoveryGrace: 10 * time.Second, Now: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	stats, runErr := worker.RunOnce(context.Background())
	if runErr != nil || stats.Candidates != 1 || stats.Claimed != 1 || stats.Completed != 1 || len(participant.requests) != 1 {
		t.Fatalf("stale pre-read Redis time extended grace: stats=%+v requests=%d err=%v", stats, len(participant.requests), runErr)
	}
}

func TestWorkerRecoveryGraceNeverDefersRetryOrExpiredTakeover(t *testing.T) {
	for _, tc := range []struct {
		name          string
		retryCount    uint64
		priorEpoch    uint64
		leaseHolder   string
		leaseBoot     string
		leaseToken    string
		leaseExpireMs int64
	}{
		{name: "rescheduled-retry", retryCount: 1, priorEpoch: 1},
		{name: "expired-lease-takeover", priorEpoch: 1, leaseHolder: "dead-worker", leaseBoot: "dead-boot", leaseToken: "dead-token", leaseExpireMs: 1_799_999_999_000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, calculator, bucket := testBucket(t)
			incidentID := testIncidentID(t, "grace-"+tc.name)
			orderID := mustOrderID(t, "grace-"+tc.name)
			payload, _ := testPayload(t, orderID, incidentID)
			trustedNow := time.UnixMilli(1_800_000_000_000)
			lease := newFakeLease()
			lease.payload[orderID] = payload
			lease.retryCount = tc.retryCount
			lease.nextEpoch = tc.priorEpoch
			lease.trustedNowMs = trustedNow.UnixMilli()
			order := authority.SourceOrderSnapshot{
				WorkID: orderID, WorkType: FamilyCareReward, TypedPayload: payload,
				CreatedAtMs: trustedNow.Add(-time.Second).UnixMilli(), RetryCount: tc.retryCount,
				LeaseEpoch: tc.priorEpoch, LeaseHolderID: tc.leaseHolder, LeaseHolderBootID: tc.leaseBoot,
				LeaseTokenHash: tc.leaseToken, LeaseExpireAtMs: tc.leaseExpireMs,
			}
			participant := &fakeParticipant{responses: []*contractspb.ParticipantResponse{
				testResponse(t, orderID, incidentID, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED),
			}}
			worker, err := NewWorker(Config{
				WorkLease: lease, SourceOrders: fakeSourceOrders{order: order, bucket: bucket},
				DueIndex: fakeDue{refs: []authority.DueWorkRef{{
					Ref: workidentity.WorkRef{WorkType: FamilyCareReward, WorkID: orderID}, DueAtMs: trustedNow.UnixMilli(),
				}}}, Buckets: []rediskey.AuthorityBucketRef{bucket},
				BucketForFarm: func(farmID uint64) (rediskey.AuthorityBucketRef, error) { return testBucketForFarm(calculator, farmID) },
				Route:         testRoute, Player: participant, Owner: "worker-grace", Token: "token-grace", BootID: "boot-grace",
				LeaseTTL: 2 * time.Second, SafetyMargin: 100 * time.Millisecond, OperationTimeout: time.Second,
				RecoveryGrace: time.Hour, Now: func() time.Time { return trustedNow },
			})
			if err != nil {
				t.Fatal(err)
			}
			stats, runErr := worker.RunOnce(context.Background())
			if runErr != nil || stats.Candidates != 1 || stats.Claimed != 1 || stats.Completed != 1 || len(participant.requests) != 1 {
				t.Fatalf("retry/takeover was grace-delayed: stats=%+v requests=%d err=%v", stats, len(participant.requests), runErr)
			}
		})
	}
}

func TestWorkerSingleSuccessUsesPlayerRecoveryAndCompletesAfterProof(t *testing.T) {
	_, calculator, bucket := testBucket(t)
	incidentID := testIncidentID(t, "success")
	orderID := mustOrderID(t, "success")
	payload, _ := testPayload(t, orderID, incidentID)
	lease := newFakeLease()
	participant := &fakeParticipant{responses: []*contractspb.ParticipantResponse{testResponse(t, orderID, incidentID, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED)}}
	worker := makeWorker(t, lease, fakeDue{refs: []authority.DueWorkRef{dueRef(t, orderID)}}, participant, map[string][]byte{orderID: payload}, bucket, calculator)
	stats, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Claimed != 1 || stats.Completed != 1 || stats.Rescheduled != 0 || lease.completeCalls != 1 {
		t.Fatalf("stats=%+v complete=%d", stats, lease.completeCalls)
	}
	if len(participant.requests) != 1 {
		t.Fatalf("requests=%d", len(participant.requests))
	}
	request := participant.requests[0]
	if request.GetParticipantCommand() != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_CARE_REWARD || request.GetRouteHeader().GetRouteDomain() != logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER {
		t.Fatalf("request=%+v", request)
	}
	if _, ok := request.GetCallerContext().(*contractspb.ParticipantRequest_Recovery); !ok {
		t.Fatalf("caller=%T", request.GetCallerContext())
	}
}

func TestWorkerTimeoutUnknownRetriesSameInvocationBeforeComplete(t *testing.T) {
	_, calculator, bucket := testBucket(t)
	incidentID := testIncidentID(t, "timeout")
	orderID := mustOrderID(t, "timeout")
	payload, _ := testPayload(t, orderID, incidentID)
	lease := newFakeLease()
	lease.releaseOnRenew = true
	lease.payload[orderID] = payload
	participant := &fakeParticipant{responses: []*contractspb.ParticipantResponse{
		{ParticipantCommand: contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_CARE_REWARD, ParticipantInvocationId: mustInvocation(t, orderID), Outcome: contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_TIMEOUT_UNKNOWN},
		testResponse(t, orderID, incidentID, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED),
	}}
	worker := makeWorker(t, lease, fakeDue{refs: []authority.DueWorkRef{dueRef(t, orderID)}}, participant, map[string][]byte{orderID: payload}, bucket, calculator)
	first, firstErr := worker.RunOnce(context.Background())
	if first.Completed != 0 || first.Rescheduled != 1 || firstErr == nil {
		t.Fatalf("first stats=%+v err=%v", first, firstErr)
	}
	// A fresh Worker instance represents process restart; only the durable
	// DueIndex/SourceOrder state in fakeLease carries the order forward.
	worker = makeWorker(t, lease, fakeDue{refs: []authority.DueWorkRef{dueRef(t, orderID)}}, participant, map[string][]byte{orderID: payload}, bucket, calculator)
	second, secondErr := worker.RunOnce(context.Background())
	if secondErr != nil || second.Completed != 1 || lease.completeCalls != 1 {
		t.Fatalf("second stats=%+v err=%v complete=%d", second, secondErr, lease.completeCalls)
	}
	if len(participant.requests) != 2 {
		t.Fatalf("requests=%d", len(participant.requests))
	}
	firstInvocation := participant.requests[0].GetCallerContext().(*contractspb.ParticipantRequest_Recovery).Recovery.GetParticipantInvocationId()
	secondInvocation := participant.requests[1].GetCallerContext().(*contractspb.ParticipantRequest_Recovery).Recovery.GetParticipantInvocationId()
	if firstInvocation != secondInvocation || firstInvocation != mustInvocation(t, orderID) {
		t.Fatalf("invocations %q %q", firstInvocation, secondInvocation)
	}
}

func TestWorkerResponseLossDoesNotCompleteAndReplaysSameInvocation(t *testing.T) {
	_, calculator, bucket := testBucket(t)
	incidentID := testIncidentID(t, "response-loss")
	orderID := mustOrderID(t, "response-loss")
	payload, _ := testPayload(t, orderID, incidentID)
	lease := newFakeLease()
	lease.releaseOnRenew = true
	participant := &fakeParticipant{
		errs: []error{errors.New("response lost after Player commit")},
		responses: []*contractspb.ParticipantResponse{
			testResponse(t, orderID, incidentID, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED),
		},
	}
	worker := makeWorker(t, lease, fakeDue{refs: []authority.DueWorkRef{dueRef(t, orderID)}}, participant, map[string][]byte{orderID: payload}, bucket, calculator)
	first, firstErr := worker.RunOnce(context.Background())
	if firstErr == nil || first.Unknown != 1 || first.Completed != 0 || lease.completeCalls != 0 || len(participant.requests) != 1 {
		t.Fatalf("first stats=%+v err=%v complete=%d requests=%d", first, firstErr, lease.completeCalls, len(participant.requests))
	}
	worker = makeWorker(t, lease, fakeDue{refs: []authority.DueWorkRef{dueRef(t, orderID)}}, participant, map[string][]byte{orderID: payload}, bucket, calculator)
	second, secondErr := worker.RunOnce(context.Background())
	if secondErr != nil || second.Completed != 1 || lease.completeCalls != 1 || len(participant.requests) != 2 {
		t.Fatalf("second stats=%+v err=%v complete=%d requests=%d", second, secondErr, lease.completeCalls, len(participant.requests))
	}
	firstInvocation := participant.requests[0].GetCallerContext().(*contractspb.ParticipantRequest_Recovery).Recovery.GetParticipantInvocationId()
	secondInvocation := participant.requests[1].GetCallerContext().(*contractspb.ParticipantRequest_Recovery).Recovery.GetParticipantInvocationId()
	if firstInvocation != secondInvocation {
		t.Fatalf("response-loss replay changed invocation: %q != %q", firstInvocation, secondInvocation)
	}
}

func TestWorkerLeaseFenceStopsBeforeParticipant(t *testing.T) {
	_, calculator, bucket := testBucket(t)
	incidentID := testIncidentID(t, "lease-fence")
	orderID := mustOrderID(t, "lease-fence")
	payload, _ := testPayload(t, orderID, incidentID)
	lease := newFakeLease()
	lease.payload[orderID] = payload
	participant := &fakeParticipant{responses: []*contractspb.ParticipantResponse{testResponse(t, orderID, incidentID, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED)}}
	worker, err := NewWorker(Config{
		WorkLease: lease, DueIndex: fakeDue{refs: []authority.DueWorkRef{dueRef(t, orderID)}}, Buckets: []rediskey.AuthorityBucketRef{bucket},
		BucketForFarm: func(farmID uint64) (rediskey.AuthorityBucketRef, error) { return testBucketForFarm(calculator, farmID) },
		Route:         testRoute, Player: participant, Owner: "worker-fence", Token: "token-fence", BootID: "boot-fence",
		LeaseTTL: 50 * time.Millisecond, SafetyMargin: 20 * time.Millisecond, OperationTimeout: 100 * time.Millisecond,
		Backoff: workflow.BackoffConfig{Base: time.Millisecond, Max: 5 * time.Millisecond}, Now: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	lease.fenceOnRenew = true
	stats, runErr := worker.RunOnce(context.Background())
	if runErr == nil || stats.Claimed != 1 || stats.Rescheduled != 1 || len(participant.requests) != 0 || lease.completeCalls != 0 || lease.renewCalls != 1 {
		t.Fatalf("stats=%+v err=%v requests=%d completes=%d renews=%d", stats, runErr, len(participant.requests), lease.completeCalls, lease.renewCalls)
	}
}

func TestWorkerPreDeliveryFailureLeavesOrderForRestart(t *testing.T) {
	_, calculator, bucket := testBucket(t)
	incidentID := testIncidentID(t, "pre-delivery")
	orderID := mustOrderID(t, "pre-delivery")
	payload, _ := testPayload(t, orderID, incidentID)
	lease := newFakeLease()
	lease.releaseOnRenew = true
	lease.payload[orderID] = payload
	participant := &fakeParticipant{responses: []*contractspb.ParticipantResponse{testResponse(t, orderID, incidentID, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED)}}
	routeReady := false
	route := func(ctx context.Context, actorID uint64) (*logicrpcpb.ActorRouteHeader, error) {
		if !routeReady {
			return nil, errors.New("route unavailable before delivery")
		}
		return testRoute(ctx, actorID)
	}
	config := Config{
		WorkLease: lease, DueIndex: fakeDue{refs: []authority.DueWorkRef{dueRef(t, orderID)}}, Buckets: []rediskey.AuthorityBucketRef{bucket},
		BucketForFarm: func(farmID uint64) (rediskey.AuthorityBucketRef, error) { return testBucketForFarm(calculator, farmID) },
		Route:         route, Player: participant, Owner: "worker-pre", Token: "token-pre", BootID: "boot-pre",
		LeaseTTL: 2 * time.Second, SafetyMargin: 100 * time.Millisecond, OperationTimeout: time.Second,
		Backoff: workflow.BackoffConfig{Base: time.Millisecond, Max: 5 * time.Millisecond}, Now: time.Now,
	}
	worker, err := NewWorker(config)
	if err != nil {
		t.Fatal(err)
	}
	first, firstErr := worker.RunOnce(context.Background())
	if firstErr == nil || first.Failed != 1 || first.Completed != 0 || len(participant.requests) != 0 || lease.completeCalls != 0 {
		t.Fatalf("first stats=%+v err=%v requests=%d completes=%d", first, firstErr, len(participant.requests), lease.completeCalls)
	}
	routeReady = true
	worker, err = NewWorker(config)
	if err != nil {
		t.Fatal(err)
	}
	second, secondErr := worker.RunOnce(context.Background())
	if secondErr != nil || second.Completed != 1 || len(participant.requests) != 1 || lease.completeCalls != 1 {
		t.Fatalf("second stats=%+v err=%v requests=%d completes=%d", second, secondErr, len(participant.requests), lease.completeCalls)
	}
}

func TestWorkerBadPayloadIsolatedFromOtherOrder(t *testing.T) {
	_, calculator, bucket := testBucket(t)
	goodID, badID := mustOrderID(t, "good"), mustOrderID(t, "bad")
	goodPayload, _ := testPayload(t, goodID, testIncidentID(t, "good"))
	lease := newFakeLease()
	participant := &fakeParticipant{responses: []*contractspb.ParticipantResponse{testResponse(t, goodID, testIncidentID(t, "good"), contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED)}}
	worker := makeWorker(t, lease, fakeDue{refs: []authority.DueWorkRef{dueRef(t, badID), dueRef(t, goodID)}}, participant, map[string][]byte{badID: []byte("not-protobuf"), goodID: goodPayload}, bucket, calculator)
	stats, err := worker.RunOnce(context.Background())
	if err == nil || stats.BadPayload != 1 || stats.Completed != 1 || len(participant.requests) != 1 {
		t.Fatalf("stats=%+v err=%v requests=%d", stats, err, len(participant.requests))
	}
}

func TestWorkerPayloadHashMismatchNeverCallsParticipant(t *testing.T) {
	_, calculator, bucket := testBucket(t)
	orderID := mustOrderID(t, "hash-mismatch")
	payload, _ := testPayload(t, orderID, testIncidentID(t, "hash-mismatch"))
	lease := newFakeLease()
	lease.payloadHashOverride = fmt.Sprintf("%064x", 99)
	participant := &fakeParticipant{responses: []*contractspb.ParticipantResponse{testResponse(t, orderID, testIncidentID(t, "hash-mismatch"), contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED)}}
	worker := makeWorker(t, lease, fakeDue{refs: []authority.DueWorkRef{dueRef(t, orderID)}}, participant, map[string][]byte{orderID: payload}, bucket, calculator)
	stats, err := worker.RunOnce(context.Background())
	if err == nil || stats.BadPayload != 1 || stats.Completed != 0 || len(participant.requests) != 0 || lease.completeCalls != 0 {
		t.Fatalf("stats=%+v err=%v requests=%d completes=%d", stats, err, len(participant.requests), lease.completeCalls)
	}
}

func TestWorkerConcurrentClaimsFenceOneParticipant(t *testing.T) {
	_, calculator, bucket := testBucket(t)
	orderID := mustOrderID(t, "race")
	payload, _ := testPayload(t, orderID, testIncidentID(t, "race"))
	lease := newFakeLease()
	participant := &fakeParticipant{responses: []*contractspb.ParticipantResponse{testResponse(t, orderID, testIncidentID(t, "race"), contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED)}}
	config := Config{WorkLease: lease, DueIndex: fakeDue{refs: []authority.DueWorkRef{dueRef(t, orderID)}}, Buckets: []rediskey.AuthorityBucketRef{bucket},
		BucketForFarm: func(farmID uint64) (rediskey.AuthorityBucketRef, error) { return testBucketForFarm(calculator, farmID) }, Route: testRoute, Player: participant,
		Owner: "worker-1", Token: "token-1", BootID: "boot-1", MaxConcurrent: 2, LeaseTTL: 2 * time.Second, SafetyMargin: 100 * time.Millisecond, OperationTimeout: time.Second, Now: time.Now}
	lease.payload = map[string][]byte{orderID: payload}
	workerA, err := NewWorker(config)
	if err != nil {
		t.Fatal(err)
	}
	workerB, err := NewWorker(config)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	var statsA, statsB RoundStats
	go func() { defer wg.Done(); statsA, _ = workerA.RunOnce(context.Background()) }()
	go func() { defer wg.Done(); statsB, _ = workerB.RunOnce(context.Background()) }()
	wg.Wait()
	if statsA.Completed+statsB.Completed != 1 || len(participant.requests) != 1 || lease.completeCalls != 1 {
		t.Fatalf("A=%+v B=%+v requests=%d completes=%d", statsA, statsB, len(participant.requests), lease.completeCalls)
	}
}

func TestDirectDispatcherAttemptsBeforeClaimAndLeavesFailureDurable(t *testing.T) {
	profile, calculator, bucket := testBucket(t)
	_ = profile
	orderID := mustOrderID(t, "direct")
	payload, decoded := testPayload(t, orderID, testIncidentID(t, "direct"))
	lease := newFakeLease()
	lease.payload[orderID] = payload
	participant := &fakeParticipant{responses: []*contractspb.ParticipantResponse{testResponse(t, orderID, testIncidentID(t, "direct"), contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED)}}
	order := authority.SourceOrderSnapshot{WorkID: orderID, WorkType: FamilyCareReward, PlacementKind: "SOURCE_PERSISTENCE_BUCKET", BucketVersion: 1, BucketID: bucket.VirtualShard(),
		SourceAuthorityKind: "FARM_ACTOR", CanonicalSourceAuthorityID: fmt.Sprint(testFarmID), AuthorityLayoutVersion: bucket.LayoutVersion(), ActorRouteDomain: "FARM",
		SubjectPlayerID: decoded.GetCarerPlayerId(), OperationID: "care-operation-1", WorkVersion: 1, TypedPayload: payload, State: "PENDING"}
	direct, err := NewDirectDispatcher(DirectConfig{WorkLease: lease, SourceOrders: fakeSourceOrders{order: order, bucket: bucket}, Route: testRoute, Player: participant,
		Owner: "direct-1", Token: "direct-token", BucketForFarm: func(farmID uint64) (rediskey.AuthorityBucketRef, error) { return testBucketForFarm(calculator, farmID) }, LeaseTTL: 2 * time.Second, ItemDeadline: time.Second, Now: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	address, err := careWorkAddress(bucket, orderID, testFarmID)
	if err != nil {
		t.Fatal(err)
	}
	if err := direct.DispatchOnce(context.Background(), address); err != nil {
		t.Fatal(err)
	}
	if len(participant.requests) != 1 || lease.claimCalls != 1 || lease.completeCalls != 1 {
		t.Fatalf("requests=%d claims=%d completes=%d", len(participant.requests), lease.claimCalls, lease.completeCalls)
	}
	direct.Start(context.Background())
	direct.Close()
}

func TestDirectDispatcherBatchesConcurrentCompletions(t *testing.T) {
	_, _, bucket := testBucket(t)
	firstID := mustOrderID(t, "batch-complete-first")
	secondID := mustOrderID(t, "batch-complete-second")
	base := newFakeLease()
	base.payload[firstID] = []byte("first")
	base.payload[secondID] = []byte("second")
	lease := &fakeBatchLease{fakeLease: base}
	direct, err := NewDirectDispatcher(DirectConfig{
		WorkLease: lease, SourceOrders: failingSourceOrders{}, Route: testRoute,
		Player: &fakeParticipant{}, Owner: "direct-batch", Token: "direct-token", BootID: "direct-boot",
		BucketForFarm: func(uint64) (rediskey.AuthorityBucketRef, error) { return bucket, nil },
		MaxItems:      16, CompletionBatchSize: 8, CompletionBatchWait: 20 * time.Millisecond, ItemDeadline: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	direct.Start(context.Background())
	defer direct.Close()
	errCh := make(chan error, 2)
	for _, workID := range []string{firstID, secondID} {
		workID := workID
		go func() {
			_, err := direct.completeDirectProof(context.Background(), authority.WorkClaimAndCompleteRequest{
				Bucket: bucket, WorkType: FamilyCareReward, WorkID: workID,
				HolderID: "direct-batch", BootID: "direct-boot", LeaseToken: "direct-token", RequestedTTLms: 1000,
			})
			errCh <- err
		}()
	}
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if len(lease.batches) != 1 || lease.batches[0] != 2 {
		t.Fatalf("completion batches=%v, want [2]", lease.batches)
	}
}

func TestDirectDispatcherCloseStopsAdmissionThenDrainsDispatchAndCompletion(t *testing.T) {
	_, calculator, bucket := testBucket(t)
	base := newFakeLease()
	orders := make(map[string]authority.SourceOrderSnapshot, 2)
	responses := make(map[string]*contractspb.ParticipantResponse, 2)
	addresses := make([]workidentity.WorkAddress, 0, 2)
	for _, suffix := range []string{"shutdown-first", "shutdown-second"} {
		incidentID := testIncidentID(t, suffix)
		orderID := mustOrderID(t, suffix)
		payload, decoded := testPayload(t, orderID, incidentID)
		hash, err := CareRewardPayloadHash(decoded)
		if err != nil {
			t.Fatal(err)
		}
		base.payload[orderID] = payload
		orders[orderID] = authority.SourceOrderSnapshot{
			WorkID: orderID, WorkType: FamilyCareReward, PlacementKind: "SOURCE_PERSISTENCE_BUCKET",
			BucketVersion: 1, BucketID: bucket.VirtualShard(), SourceAuthorityKind: "FARM_ACTOR",
			CanonicalSourceAuthorityID: fmt.Sprint(testFarmID), AuthorityLayoutVersion: bucket.LayoutVersion(),
			ActorRouteDomain: "FARM", SubjectPlayerID: decoded.GetCarerPlayerId(), OperationID: "shutdown-" + suffix,
			PayloadHash: fmt.Sprintf("%x", hash[:]), State: "PENDING", WorkVersion: 1, TypedPayload: payload,
		}
		responses[mustInvocation(t, orderID)] = testResponse(t, orderID, incidentID, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED)
		address, addressErr := careWorkAddress(bucket, orderID, testFarmID)
		if addressErr != nil {
			t.Fatal(addressErr)
		}
		addresses = append(addresses, address)
	}
	lease := &fakeBatchLease{fakeLease: base}
	participant := &gatedCoordinatorParticipant{
		responses: responses, started: make(chan struct{}), release: make(chan struct{}),
	}
	direct, err := NewDirectDispatcher(DirectConfig{
		WorkLease: lease, SourceOrders: mapSourceOrders{bucket: bucket, orders: orders}, Route: testRoute, Player: participant,
		Owner: "direct-shutdown", Token: "direct-token", BootID: "direct-boot",
		BucketForFarm: func(farmID uint64) (rediskey.AuthorityBucketRef, error) { return testBucketForFarm(calculator, farmID) },
		MaxItems:      8, Workers: 1, CompletionBatchSize: 8, CompletionBatchWait: time.Millisecond, ItemDeadline: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	direct.Start(context.Background())
	for _, address := range addresses {
		if err := direct.Enqueue(address, 128); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-participant.started:
	case <-time.After(time.Second):
		t.Fatal("first dispatch did not reach participant")
	}

	closed := make(chan struct{})
	go func() {
		direct.Close()
		close(closed)
	}()
	deadline := time.Now().Add(time.Second)
	for !direct.closing.Load() && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if !direct.closing.Load() {
		t.Fatal("Close did not stop admission")
	}
	if err := direct.Enqueue(addresses[0], 1); !errors.Is(err, ErrDirectClosed) {
		t.Fatalf("enqueue after admission stop err=%v", err)
	}
	select {
	case <-closed:
		t.Fatal("Close returned before in-flight dispatch was released")
	default:
	}
	close(participant.release)
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("ordered dispatcher drain did not finish")
	}

	stats := direct.Stats()
	if stats.Enqueued != 2 || stats.Depth != 0 || stats.Bytes != 0 || stats.Dispatched != 2 || stats.Completed != 2 || participant.RequestCount() != 2 {
		t.Fatalf("shutdown drain stats=%+v participant requests=%d", stats, participant.RequestCount())
	}
	lease.mu.Lock()
	batchItems := 0
	for _, size := range lease.batches {
		batchItems += size
	}
	lease.mu.Unlock()
	if batchItems != 2 {
		t.Fatalf("completion batch items=%d, want both completions admitted during dispatch drain", batchItems)
	}
}

func TestDirectDispatcherShutdownTimeoutLeavesFullQueueForDueIndex(t *testing.T) {
	_, calculator, bucket := testBucket(t)
	incidentID := testIncidentID(t, "shutdown-timeout")
	orderID := mustOrderID(t, "shutdown-timeout")
	payload, decoded := testPayload(t, orderID, incidentID)
	hash, err := CareRewardPayloadHash(decoded)
	if err != nil {
		t.Fatal(err)
	}
	order := authority.SourceOrderSnapshot{
		WorkID: orderID, WorkType: FamilyCareReward, PlacementKind: "SOURCE_PERSISTENCE_BUCKET",
		BucketVersion: 1, BucketID: bucket.VirtualShard(), SourceAuthorityKind: "FARM_ACTOR",
		CanonicalSourceAuthorityID: fmt.Sprint(testFarmID), AuthorityLayoutVersion: bucket.LayoutVersion(),
		ActorRouteDomain: "FARM", SubjectPlayerID: decoded.GetCarerPlayerId(), OperationID: "shutdown-timeout",
		PayloadHash: fmt.Sprintf("%x", hash[:]), State: "PENDING", WorkVersion: 1, TypedPayload: payload,
	}
	lease := newFakeLease()
	lease.payload[orderID] = payload
	participant := &gatedCoordinatorParticipant{
		responses: map[string]*contractspb.ParticipantResponse{
			mustInvocation(t, orderID): testResponse(t, orderID, incidentID, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED),
		},
		started: make(chan struct{}), release: make(chan struct{}),
	}
	direct, err := NewDirectDispatcher(DirectConfig{
		WorkLease: lease, SourceOrders: mapSourceOrders{bucket: bucket, orders: map[string]authority.SourceOrderSnapshot{orderID: order}},
		Route: testRoute, Player: participant, Owner: "direct-timeout", Token: "direct-token", BootID: "direct-boot",
		BucketForFarm: func(farmID uint64) (rediskey.AuthorityBucketRef, error) { return testBucketForFarm(calculator, farmID) },
		MaxItems:      2, Workers: 1, ItemDeadline: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	direct.Start(context.Background())
	address, err := careWorkAddress(bucket, orderID, testFarmID)
	if err != nil {
		t.Fatal(err)
	}
	if err := direct.Enqueue(address, 1); err != nil {
		t.Fatal(err)
	}
	select {
	case <-participant.started:
	case <-time.After(time.Second):
		t.Fatal("in-flight dispatch did not start")
	}
	// The worker is blocked in the first participant call, so these accepted
	// items fill the queue exactly.
	if err := direct.Enqueue(address, 1); err != nil {
		t.Fatal(err)
	}
	if err := direct.Enqueue(address, 1); err != nil {
		t.Fatal(err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	shutdownErr := direct.Shutdown(shutdownCtx)
	cancel()
	if !errors.Is(shutdownErr, context.DeadlineExceeded) {
		t.Fatalf("shutdown err=%v, want deadline", shutdownErr)
	}
	settleCtx, settleCancel := context.WithTimeout(context.Background(), time.Second)
	defer settleCancel()
	if err := direct.Shutdown(settleCtx); err != nil {
		t.Fatalf("cancelled dispatcher did not settle: %v", err)
	}
	close(participant.release)

	stats := direct.Stats()
	if stats.Enqueued != 3 || stats.Depth != 2 || stats.Bytes != 2 || stats.Dispatched != 1 || stats.Completed != 0 || participant.RequestCount() != 1 {
		t.Fatalf("timeout did not preserve queued DueIndex handoff: stats=%+v requests=%d", stats, participant.RequestCount())
	}
	lease.mu.Lock()
	completed := lease.completed[orderID]
	lease.mu.Unlock()
	if completed {
		t.Fatal("timed-out Direct shutdown completed durable work unexpectedly")
	}
	if err := direct.Enqueue(address, 1); !errors.Is(err, ErrDirectClosed) {
		t.Fatalf("post-timeout admission err=%v", err)
	}
}

func TestDirectDispatcherConcurrentAdmissionShutdownRace(t *testing.T) {
	_, calculator, bucket := testBucket(t)
	orderID := mustOrderID(t, "shutdown-race-stress")
	address, err := careWorkAddress(bucket, orderID, testFarmID)
	if err != nil {
		t.Fatal(err)
	}
	direct, err := NewDirectDispatcher(DirectConfig{
		WorkLease: newFakeLease(), SourceOrders: failingSourceOrders{}, Route: testRoute, Player: &fakeParticipant{},
		Owner: "direct-race", Token: "direct-token", BootID: "direct-boot",
		BucketForFarm: func(farmID uint64) (rediskey.AuthorityBucketRef, error) { return testBucketForFarm(calculator, farmID) },
		MaxItems:      16, Workers: 4, ItemDeadline: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	direct.Start(context.Background())
	start := make(chan struct{})
	var wg sync.WaitGroup
	for worker := 0; worker < 24; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			for attempt := 0; attempt < 100; attempt++ {
				if worker%2 == 0 {
					_ = direct.Enqueue(address, 1)
				} else {
					_ = direct.DispatchOnce(context.Background(), address)
				}
			}
		}(worker)
	}
	close(start)
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := direct.Shutdown(shutdownCtx); err != nil {
		shutdownCancel()
		t.Fatal(err)
	}
	shutdownCancel()
	wg.Wait()
	if err := direct.Enqueue(address, 1); !errors.Is(err, ErrDirectClosed) {
		t.Fatalf("post-race admission err=%v", err)
	}
}

func TestDirectDispatcherCommittedEnvelopeSkipsSourceOrderReadback(t *testing.T) {
	_, calculator, bucket := testBucket(t)
	incidentID := testIncidentID(t, "committed-envelope")
	orderID := mustOrderID(t, "committed-envelope")
	payload, decoded := testPayload(t, orderID, incidentID)
	payloadHash, err := CareRewardPayloadHash(decoded)
	if err != nil {
		t.Fatal(err)
	}
	lease := newFakeLease()
	lease.payload[orderID] = payload
	participant := &fakeParticipant{responses: []*contractspb.ParticipantResponse{
		testResponse(t, orderID, incidentID, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED),
	}}
	direct, err := NewDirectDispatcher(DirectConfig{
		WorkLease: lease, SourceOrders: failingSourceOrders{}, Route: testRoute, Player: participant,
		Owner: "direct-committed", Token: "direct-token",
		BucketForFarm: func(farmID uint64) (rediskey.AuthorityBucketRef, error) { return testBucketForFarm(calculator, farmID) },
		ItemDeadline:  time.Second, Now: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	address, err := careWorkAddress(bucket, orderID, testFarmID)
	if err != nil {
		t.Fatal(err)
	}
	attached := authoritystore.AttachedOrder{
		OrderID: orderID, OrderType: FamilyCareReward, Payload: payload,
		PayloadHash: authoritystore.PayloadHash(payloadHash), DueAtMs: 1000,
		OperationID: "care-committed-operation", SubjectPlayerID: decoded.GetCarerPlayerId(),
	}
	if err := direct.DispatchCommittedOnce(context.Background(), address, attached); err != nil {
		t.Fatal(err)
	}
	if len(participant.requests) != 1 || lease.claimCalls != 1 || lease.completeCalls != 1 {
		t.Fatalf("requests=%d claims=%d completes=%d", len(participant.requests), lease.claimCalls, lease.completeCalls)
	}
}

func TestDirectDispatcherRPCFailureDoesNotClaimOrComplete(t *testing.T) {
	_, calculator, bucket := testBucket(t)
	incidentID := testIncidentID(t, "direct-failure")
	orderID := mustOrderID(t, "direct-failure")
	payload, decoded := testPayload(t, orderID, incidentID)
	lease := newFakeLease()
	lease.payload[orderID] = payload
	participant := &fakeParticipant{errs: []error{errors.New("Player RPC unavailable")}}
	order := authority.SourceOrderSnapshot{WorkID: orderID, WorkType: FamilyCareReward, PlacementKind: "SOURCE_PERSISTENCE_BUCKET", BucketVersion: 1, BucketID: bucket.VirtualShard(),
		SourceAuthorityKind: "FARM_ACTOR", CanonicalSourceAuthorityID: fmt.Sprint(testFarmID), AuthorityLayoutVersion: bucket.LayoutVersion(), ActorRouteDomain: "FARM",
		SubjectPlayerID: decoded.GetCarerPlayerId(), OperationID: "care-operation-1", WorkVersion: 1, TypedPayload: payload, State: "PENDING"}
	direct, err := NewDirectDispatcher(DirectConfig{WorkLease: lease, SourceOrders: fakeSourceOrders{order: order, bucket: bucket}, Route: testRoute, Player: participant,
		Owner: "direct-failure", Token: "direct-token", BucketForFarm: func(farmID uint64) (rediskey.AuthorityBucketRef, error) { return testBucketForFarm(calculator, farmID) }, ItemDeadline: time.Second, Now: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	address, err := careWorkAddress(bucket, orderID, testFarmID)
	if err != nil {
		t.Fatal(err)
	}
	if err := direct.DispatchOnce(context.Background(), address); err == nil {
		t.Fatal("RPC failure must be surfaced")
	}
	if len(participant.requests) != 1 || lease.claimCalls != 0 || lease.completeCalls != 0 {
		t.Fatalf("requests=%d claims=%d completes=%d", len(participant.requests), lease.claimCalls, lease.completeCalls)
	}
}

func mustInvocation(t *testing.T, orderID string) string {
	t.Helper()
	value, err := identity.ParticipantInvocationID(identity.WorkTypeCareReward, orderID, identity.StepApplyCareReward, 1)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func mustOrderID(t *testing.T, name string) string {
	t.Helper()
	value, err := identity.SourceOrderID(identity.WorkTypeCareReward, identity.SourceKindFarmActor, fmt.Sprint(testFarmID), testIncidentID(t, name), 12)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func testIncidentID(t *testing.T, name string) string {
	t.Helper()
	cycleID, err := workidentity.StableOpaqueID("test-care-incident-cycle", name)
	if err != nil {
		t.Fatal(err)
	}
	incidentID, err := identity.CareIncidentID(cycleID, "WATER", 1)
	if err != nil {
		t.Fatal(err)
	}
	return incidentID
}
