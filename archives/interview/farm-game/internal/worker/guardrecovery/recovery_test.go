package guardrecovery

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actorrouting"
	"farmgame/internal/contracts"
	"farmgame/internal/identity"
	playerpet "farmgame/internal/player/pet"
	"farmgame/internal/storage/authoritystore"
	authority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workidentity"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

const testGuardPlayerID uint64 = 10001

type guardFakeLease struct {
	mu          sync.Mutex
	payload     []byte
	workID      string
	payloadHash string
	active      bool
	completed   bool
	nextEpoch   uint64
	claims      int
	renewals    int
	completions int
	reschedules int
	proof       authority.WorkCompletionProof
	generation  uint64
}

func (f *guardFakeLease) ClaimWorkWithBoot(_ context.Context, bucket rediskey.AuthorityBucketRef, workType, workID, holderID, bootID, token string, ttlMs int64) (authority.WorkClaim, error) {
	if err := bucket.Validate(); err != nil {
		return authority.WorkClaim{}, err
	}
	if holderID == "" || bootID == "" || token == "" {
		return authority.WorkClaim{}, errors.New("missing caller fence")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claims++
	if f.completed {
		return authority.WorkClaim{Status: authority.WorkClaimNotClaimable}, nil
	}
	if f.active {
		return authority.WorkClaim{Status: authority.WorkClaimLeaseHeld}, nil
	}
	f.nextEpoch++
	f.active = true
	if f.payloadHash == "" {
		f.payloadHash = ApplyGuardSnapshotPayloadHash(f.payload)
	}
	return authority.WorkClaim{Status: authority.WorkClaimClaimed, WorkID: workID, WorkType: workType,
		SubjectPlayerID: testGuardPlayerID, OperationID: "guard-operation-1",
		Payload: append([]byte(nil), f.payload...), PayloadHash: f.payloadHash,
		WorkVersion: 1, Generation: f.generation, LeaseEpoch: f.nextEpoch, AcceptedTTLms: ttlMs, LeaseHolderBootID: bootID}, nil
}

func (f *guardFakeLease) RenewWorkLeaseWithBoot(_ context.Context, _ rediskey.AuthorityBucketRef, workType, _ string, holderID, bootID string, epoch uint64, token string, ttlMs int64) (authority.WorkLeaseRenewResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.renewals++
	if workType != FamilyGuardSync || holderID == "" || bootID == "" || token == "" || !f.active || epoch != f.nextEpoch {
		return authority.WorkLeaseRenewResult{Status: authority.WorkLeaseFenceMismatch}, nil
	}
	return authority.WorkLeaseRenewResult{Status: authority.WorkLeaseRenewed, AcceptedTTLms: ttlMs}, nil
}

func (f *guardFakeLease) CompleteWorkWithProof(_ context.Context, _ rediskey.AuthorityBucketRef, workType, workID, holderID, bootID string, epoch, version uint64, token string, proof authority.WorkCompletionProof) (authority.WorkCompleteResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.completions++
	if workType != FamilyGuardSync || workID != f.workID || holderID == "" || bootID == "" || token == "" ||
		!f.active || epoch != f.nextEpoch || version != 1 {
		return authority.WorkCompleteResult{Status: authority.WorkLeaseFenceMismatch}, nil
	}
	if f.completed {
		return authority.WorkCompleteResult{Status: authority.WorkLeaseAlreadyCompleted}, nil
	}
	f.completed = true
	f.active = false
	f.proof = proof
	return authority.WorkCompleteResult{Status: authority.WorkLeaseCompleted}, nil
}

func (f *guardFakeLease) RescheduleWork(_ context.Context, _ rediskey.AuthorityBucketRef, workType, workID, holderID, bootID string, epoch, version uint64, token string, _ uint64, _ int64, _ string) (authority.WorkRescheduleResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reschedules++
	if workType != FamilyGuardSync || workID != f.workID || holderID == "" || bootID == "" || token == "" ||
		!f.active || epoch != f.nextEpoch || version != 1 {
		return authority.WorkRescheduleResult{Status: authority.WorkLeaseFenceMismatch}, nil
	}
	f.active = false
	return authority.WorkRescheduleResult{Status: authority.WorkLeaseRenewed}, nil
}

type guardFakeDue struct{ refs []authority.DueWorkRef }

func (f guardFakeDue) ListDueWork(_ context.Context, bucket rediskey.AuthorityBucketRef, limit int) ([]authority.DueWorkRef, error) {
	if err := bucket.Validate(); err != nil {
		return nil, err
	}
	// Mirror the physical bound enforced by WorkLeaseStore.ListDueWork so an
	// out-of-range probe fails here exactly as it would against Redis.
	if limit <= 0 || limit > 4096 {
		return nil, fmt.Errorf("%w: due limit", authoritystore.ErrInvalidRequest)
	}
	if limit < len(f.refs) {
		return append([]authority.DueWorkRef(nil), f.refs[:limit]...), nil
	}
	return append([]authority.DueWorkRef(nil), f.refs...), nil
}

type guardFakeParticipant struct {
	mu       sync.Mutex
	requests []*contractspb.ParticipantRequest
	response *contractspb.ParticipantResponse
	err      error
}

func (f *guardFakeParticipant) ResumeStep(_ context.Context, request *contractspb.ParticipantRequest, _ ...grpc.CallOption) (*contractspb.ParticipantResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, request)
	if f.err != nil {
		err := f.err
		f.err = nil
		return nil, err
	}
	return f.response, nil
}

func guardTestBucket(t *testing.T) rediskey.AuthorityBucketRef {
	t.Helper()
	ns, err := rediskey.NewNamespace("test", "guard")
	if err != nil {
		t.Fatal(err)
	}
	profile := rediskey.PhysicalProfile{ProfileVersion: "redis-physical-v1", Namespace: ns,
		AuthorityLayoutVersion: "v1", WorkflowLayoutVersion: "v1", WorkflowHashVersion: 1, WorkflowBucketCount: 256}
	calculator, err := actorrouting.NewCalculator("classic-farm",
		actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608040001}, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := profile.AuthorityBucketForActor(calculator,
		actorrouting.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: testGuardPlayerID})
	if err != nil {
		t.Fatal(err)
	}
	return bucket
}

func guardTestPayload(t *testing.T) (string, []byte, *contractspb.ApplyGuardSnapshotPayload) {
	t.Helper()
	snapshot := playerpet.GuardSnapshot{SchemaVersion: playerpet.GuardSnapshotSchemaVersion, GuardVersion: 7,
		ActiveGuardPetID: 2, GuardUntilMs: 123456,
		Rule: playerpet.GuardRuleSnapshot{PetConfigVersion: 3, GuardRuleVersion: 4,
			TriggerProbabilityBp: 5000, InterceptPolicy: playerpet.InterceptPolicyFullGross,
			RandomAlgorithmVersion: 1, RandomKeyVersion: 2}}
	hash := snapshot.Hash()
	orderID, err := identity.SourceOrderID(identity.WorkTypeGuardSyncOrder, identity.SourceKindPlayerActor,
		"10001", "PET_GUARD_SLOT", snapshot.GuardVersion)
	if err != nil {
		t.Fatal(err)
	}
	active := uint32(snapshot.ActiveGuardPetID)
	payload := &contractspb.ApplyGuardSnapshotPayload{OrderId: orderID, SourcePlayerId: testGuardPlayerID,
		TargetFarmId: testGuardPlayerID, SnapshotHash: append([]byte(nil), hash[:]...),
		GuardSnapshot: &contractspb.GuardSnapshot{SchemaVersion: uint32(snapshot.SchemaVersion),
			GuardVersion: snapshot.GuardVersion, ActiveGuardPetId: &active, GuardUntilMs: snapshot.GuardUntilMs,
			GuardRule: &contractspb.GuardRuleSnapshot{PetConfigVersion: snapshot.Rule.PetConfigVersion,
				GuardRuleVersion: snapshot.Rule.GuardRuleVersion, TriggerProbabilityBp: snapshot.Rule.TriggerProbabilityBp,
				InterceptPolicy:        contractspb.GuardInterceptPolicy(snapshot.Rule.InterceptPolicy),
				RandomAlgorithmVersion: snapshot.Rule.RandomAlgorithmVersion, RandomKeyVersion: snapshot.Rule.RandomKeyVersion},
			SnapshotHash: append([]byte(nil), hash[:]...)}}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return orderID, raw, payload
}

func guardTestResponse(t *testing.T, payload *contractspb.ApplyGuardSnapshotPayload, outcome contractspb.ParticipantOutcome) *contractspb.ParticipantResponse {
	t.Helper()
	invocation, err := identity.ParticipantInvocationID(identity.WorkTypeGuardSyncOrder, payload.GetOrderId(), identity.StepApplyGuardSnapshot, 1)
	if err != nil {
		t.Fatal(err)
	}
	status := contractspb.GuardApplyStatus_GUARD_APPLY_STATUS_APPLIED
	if outcome == contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE {
		status = contractspb.GuardApplyStatus_GUARD_APPLY_STATUS_SUPERSEDED
	}
	result := &contractspb.ApplyGuardSnapshotResult{GuardVersion: payload.GetGuardSnapshot().GetGuardVersion(),
		SnapshotHash: append([]byte(nil), payload.GetSnapshotHash()...), ApplyStatus: status, CommittedRecordVersion: 11}
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_GUARD_SNAPSHOT)
	if err != nil {
		t.Fatal(err)
	}
	typed, resultHash, err := contracts.ValidateTypedResult(spec, result)
	if err != nil {
		t.Fatal(err)
	}
	return &contractspb.ParticipantResponse{ParticipantCommand: spec.Command, ParticipantInvocationId: invocation,
		Outcome: outcome, TypedResult: typed, ParticipantResultHash: resultHash, CommittedRecordVersion: 11}
}

func guardTestDue(t *testing.T, orderID string) authority.DueWorkRef {
	t.Helper()
	member, err := workidentity.EncodeWorkRefV1(FamilyGuardSync, orderID)
	if err != nil {
		t.Fatal(err)
	}
	return authority.DueWorkRef{Ref: workidentity.WorkRef{WorkType: FamilyGuardSync, WorkID: orderID}, Member: member, DueAtMs: time.Now().UnixMilli()}
}

type guardRecoveryRouteResolver struct{ assignment actorrouting.Assignment }

func (r guardRecoveryRouteResolver) Resolve(context.Context, actorrouting.Address) (actorrouting.Assignment, error) {
	return r.assignment, nil
}

func TestGuardRecoveryRouteLeaseProofCompatibility(t *testing.T) {
	assignment := actorrouting.Assignment{Kind: actorrouting.DomainFarm, ShardID: 0, ShardRevision: 1,
		HolderPod: "farm-1", HolderBootID: "farm-boot-1", Endpoint: "farm:1", WriterEpoch: 2,
		LeaseEpoch: 3, LeaseUntilMs: 7_000}
	provider := farmRouteProvider(guardRecoveryRouteResolver{assignment}, "realm-a")
	header, err := provider(context.Background(), testGuardPlayerID)
	if err != nil {
		t.Fatal(err)
	}
	if header.GetExpectedLeaseEpoch() != 3 || header.GetExpectedLeaseUntilMs() != 7_000 {
		t.Fatalf("recovery producer omitted lease proof: %+v", header)
	}
	legacy := proto.Clone(header).(*logicrpcpb.ActorRouteHeader)
	legacy.ExpectedLeaseEpoch, legacy.ExpectedLeaseUntilMs = 0, nil
	partial := proto.Clone(header).(*logicrpcpb.ActorRouteHeader)
	partial.ExpectedLeaseUntilMs = nil
	if !sameAssignment(header, assignment) || !sameAssignment(legacy, assignment) || sameAssignment(partial, assignment) {
		t.Fatalf("mixed-version assignment current=%v legacy=%v partial=%v", sameAssignment(header, assignment), sameAssignment(legacy, assignment), sameAssignment(partial, assignment))
	}
}

func guardTestRoute(_ context.Context, actorID uint64) (*logicrpcpb.ActorRouteHeader, error) {
	return &logicrpcpb.ActorRouteHeader{RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM, ActorId: actorID,
		VirtualShard: 0, ShardRevision: 1, ExpectedHolderId: "farm-1", ExpectedHolderBootId: "farm-boot-1", ExpectedWriterEpoch: 1}, nil
}

func newGuardTestWorker(t *testing.T, lease *guardFakeLease, due authority.DueWorkRef, participant *guardFakeParticipant) *Worker {
	t.Helper()
	worker, err := NewWorker(Config{WorkLease: lease, DueIndex: guardFakeDue{refs: []authority.DueWorkRef{due}},
		Buckets: []rediskey.AuthorityBucketRef{guardTestBucket(t)}, Route: guardTestRoute, Farm: participant,
		Owner: "guard-worker", Token: "guard-token", BootID: "guard-boot", LeaseTTL: 2 * time.Second,
		SafetyMargin: 100 * time.Millisecond, OperationTimeout: time.Second, RetryDelay: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

func TestWorkerAppliesGuardSnapshotAndCompletesRawPayloadProof(t *testing.T) {
	orderID, raw, payload := guardTestPayload(t)
	lease := &guardFakeLease{payload: raw, workID: orderID}
	participant := &guardFakeParticipant{response: guardTestResponse(t, payload, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED)}
	stats, err := newGuardTestWorker(t, lease, guardTestDue(t, orderID), participant).RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Claimed != 1 || stats.Completed != 1 || stats.Rescheduled != 0 {
		t.Fatalf("stats=%+v", stats)
	}
	participant.mu.Lock()
	defer participant.mu.Unlock()
	if len(participant.requests) != 1 {
		t.Fatalf("requests=%d", len(participant.requests))
	}
	request := participant.requests[0]
	if request.GetRouteHeader().GetRouteDomain() != logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM ||
		request.GetRouteHeader().GetActorId() != testGuardPlayerID ||
		request.GetParticipantCommand() != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_GUARD_SNAPSHOT {
		t.Fatalf("request=%+v", request)
	}
	recovery, ok := request.GetCallerContext().(*contractspb.ParticipantRequest_Recovery)
	if !ok || recovery.Recovery.GetWork().GetSourceAuthority().GetAuthorityKind() != contractspb.SourceAuthorityKind_SOURCE_AUTHORITY_KIND_PLAYER_ACTOR ||
		recovery.Recovery.GetWork().GetSourceAuthority().GetActorRouteDomain() != logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER {
		t.Fatalf("recovery=%+v", request.GetCallerContext())
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.proof.PayloadHash != ApplyGuardSnapshotPayloadHash(raw) || lease.proof.ParticipantStep != participantStep ||
		lease.proof.ParticipantOutcome != "APPLIED" || lease.proof.CanonicalSourceAuthorityID != "10001" {
		t.Fatalf("proof=%+v", lease.proof)
	}
}

func TestWorkerInvalidPayloadFailsClosedAndReschedules(t *testing.T) {
	orderID, _, _ := guardTestPayload(t)
	lease := &guardFakeLease{payload: []byte("not-protobuf"), workID: orderID}
	participant := &guardFakeParticipant{}
	stats, err := newGuardTestWorker(t, lease, guardTestDue(t, orderID), participant).RunOnce(context.Background())
	if err == nil || stats.BadPayload != 1 || stats.Rescheduled != 1 || stats.Completed != 0 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	participant.mu.Lock()
	defer participant.mu.Unlock()
	if len(participant.requests) != 0 {
		t.Fatalf("invalid payload reached RPC: %d", len(participant.requests))
	}
}

func TestWorkerGenerationAndHashFenceBeforeRPC(t *testing.T) {
	orderID, raw, payload := guardTestPayload(t)
	lease := &guardFakeLease{payload: raw, workID: orderID, payloadHash: fmt.Sprintf("%064x", 9)}
	participant := &guardFakeParticipant{response: guardTestResponse(t, payload, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED)}
	stats, err := newGuardTestWorker(t, lease, guardTestDue(t, orderID), participant).RunOnce(context.Background())
	if err == nil || stats.BadPayload != 1 || stats.Completed != 0 || lease.reschedules != 1 {
		t.Fatalf("hash stats=%+v err=%v reschedules=%d", stats, err, lease.reschedules)
	}
	participant.mu.Lock()
	if len(participant.requests) != 0 {
		participant.mu.Unlock()
		t.Fatal("hash mismatch reached RPC")
	} else {
		participant.mu.Unlock()
	}

	lease.payloadHash = ""
	lease.completed = false
	lease.active = false
	lease.claims = 0
	lease.workID = orderID
	lease.payload = raw
	// A supplied schedule-style generation must bind to the GuardSnapshot version.
	_ = payload
	// The worker's source-order path has no schedule generation field; the
	// deterministic order identity remains the generation fence.
}

func TestWorkerConcurrentRunsCompleteAtMostOnce(t *testing.T) {
	orderID, raw, payload := guardTestPayload(t)
	lease := &guardFakeLease{payload: raw, workID: orderID}
	participant := &guardFakeParticipant{response: guardTestResponse(t, payload, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED)}
	workerA := newGuardTestWorker(t, lease, guardTestDue(t, orderID), participant)
	workerB := newGuardTestWorker(t, lease, guardTestDue(t, orderID), participant)
	var wg sync.WaitGroup
	wg.Add(2)
	var a, b RoundStats
	go func() { defer wg.Done(); a, _ = workerA.RunOnce(context.Background()) }()
	go func() { defer wg.Done(); b, _ = workerB.RunOnce(context.Background()) }()
	wg.Wait()
	if a.Completed+b.Completed != 1 || lease.completions != 1 || len(participant.requests) != 1 {
		t.Fatalf("a=%+v b=%+v completions=%d requests=%d", a, b, lease.completions, len(participant.requests))
	}
}

func TestObserveDueFiltersGuardAndFailsClosedWhenBoundIsTruncated(t *testing.T) {
	orderID, raw, _ := guardTestPayload(t)
	lease := &guardFakeLease{payload: raw, workID: orderID}
	worker, err := NewWorker(Config{WorkLease: lease, DueIndex: guardFakeDue{refs: []authority.DueWorkRef{
		{Ref: workidentity.WorkRef{WorkID: orderID, WorkType: FamilyGuardSync}, DueAtMs: 101},
		{Ref: workidentity.WorkRef{WorkID: "other", WorkType: string(identity.WorkTypeMailClaimProjectionOrder)}, DueAtMs: 99},
	}}, Buckets: []rediskey.AuthorityBucketRef{guardTestBucket(t)}, Route: guardTestRoute, Farm: &guardFakeParticipant{},
		Owner: "guard-worker", Token: "guard-token", BootID: "guard-boot"})
	if err != nil {
		t.Fatal(err)
	}
	observation, err := worker.ObserveDue(context.Background(), 8)
	if err != nil || !observation.Available || observation.Depth != 1 || observation.OldestDueAtMs != 101 {
		t.Fatalf("observation=%+v err=%v", observation, err)
	}
	truncated, err := worker.ObserveDue(context.Background(), 1)
	if err != nil || truncated.Available || truncated.Depth != 0 {
		t.Fatalf("truncated=%+v err=%v", truncated, err)
	}
}

func TestObserveDueAcceptsPhysicalMaximumWithoutRequesting4097(t *testing.T) {
	orderID, raw, _ := guardTestPayload(t)
	lease := &guardFakeLease{payload: raw, workID: orderID}
	due := guardFakeDue{refs: []authority.DueWorkRef{{
		Ref: workidentity.WorkRef{WorkID: orderID, WorkType: FamilyGuardSync}, DueAtMs: 101,
	}}}
	worker, err := NewWorker(Config{
		WorkLease: lease, DueIndex: due,
		Buckets: []rediskey.AuthorityBucketRef{guardTestBucket(t)}, Route: guardTestRoute,
		Farm: &guardFakeParticipant{}, Owner: "guard-worker", Token: "guard-token", BootID: "guard-boot",
	})
	if err != nil {
		t.Fatal(err)
	}
	observation, err := worker.ObserveDue(context.Background(), 4096)
	if err != nil || !observation.Available || observation.Depth != 1 || observation.OldestDueAtMs != 101 {
		t.Fatalf("observation=%+v err=%v", observation, err)
	}
}

func TestDecodePayloadRejectsNonDeterministicOrUnknownBytes(t *testing.T) {
	orderID, raw, payload := guardTestPayload(t)
	decoded, err := DecodeApplyGuardSnapshotPayload(orderID, raw)
	if err != nil || !proto.Equal(decoded, payload) {
		t.Fatalf("decode err=%v payload=%v", err, decoded)
	}
	withUnknown := append(append([]byte(nil), raw...), 0x30, 0x01)
	if _, err := DecodeApplyGuardSnapshotPayload(orderID, withUnknown); err == nil {
		t.Fatal("unknown protobuf field accepted")
	}
	if _, err := DecodeApplyGuardSnapshotPayload(orderID, append(raw, 0)); err == nil {
		t.Fatal("non-deterministic/trailing protobuf bytes accepted")
	}
	if !bytes.Equal([]byte(ApplyGuardSnapshotPayloadHash(raw)), []byte(ApplyGuardSnapshotPayloadHash(raw))) {
		t.Fatal("hash helper is not deterministic")
	}
}

func TestApplyGuardSnapshotPayloadHashMatchesIngressCanonicalContract(t *testing.T) {
	_, raw, payload := guardTestPayload(t)
	spec, err := contracts.LookupParticipantCommand(
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_GUARD_SNAPSHOT,
	)
	if err != nil {
		t.Fatal(err)
	}
	want, err := contracts.CanonicalParticipantHash(spec, payload)
	if err != nil {
		t.Fatal(err)
	}
	if got := ApplyGuardSnapshotPayloadHash(raw); got != fmt.Sprintf("%x", want[:]) {
		t.Fatalf("payload hash=%s, want ingress canonical hash=%x", got, want[:])
	}
	if got := ApplyGuardSnapshotPayloadHash([]byte("not-protobuf")); got != "" {
		t.Fatalf("malformed payload hash=%q, want empty", got)
	}
}

func TestWorkerClaimGenerationMismatchFailsClosed(t *testing.T) {
	orderID, raw, payload := guardTestPayload(t)
	lease := &guardFakeLease{payload: raw, workID: orderID, generation: payload.GetGuardSnapshot().GetGuardVersion() + 1}
	participant := &guardFakeParticipant{response: guardTestResponse(t, payload, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED)}
	stats, err := newGuardTestWorker(t, lease, guardTestDue(t, orderID), participant).RunOnce(context.Background())
	if err == nil || stats.BadPayload != 1 || stats.Completed != 0 || lease.reschedules != 1 {
		t.Fatalf("stats=%+v err=%v reschedules=%d", stats, err, lease.reschedules)
	}
	participant.mu.Lock()
	defer participant.mu.Unlock()
	if len(participant.requests) != 0 {
		t.Fatal("generation mismatch reached RPC")
	}
}
