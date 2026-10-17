package carerecovery

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	contractspb "farmgame/api/gen/contracts"
	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
	redisauthority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/worker/sourceorder"
	"farmgame/internal/workidentity"
)

func exactCommitForDirect(t *testing.T, calculator *actorrouting.Calculator, attached authoritystore.AttachedOrder, now time.Time) (*authoritystore.ExactCommitEvidence, actorrouting.Assignment) {
	t.Helper()
	store, err := authoritystore.NewMemoryStore(authoritystore.MemoryOptions{
		Resolver: calculator, AuthorityLayoutVersion: "v1", Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	address := authoritystore.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: testFarmID}
	shard, err := calculator.ShardFor(address.Domain, address.ActorID)
	if err != nil {
		t.Fatal(err)
	}
	authority := authoritystore.ShardWriteAuthority{
		AuthorityLayoutVersion: "v1", Domain: address.Domain, VirtualShard: shard,
		HolderID: "farm-holder-0", HolderBootID: "farm-holder-0-boot-a",
		AssignmentRevision: 7, WriterEpoch: 11, LeaseEpoch: 13,
		State: authoritystore.ShardAuthorityActive, LeaseUntilMs: now.Add(30 * time.Second).UnixMilli(),
	}
	if err := store.SetShardWriteAuthority(authority); err != nil {
		t.Fatal(err)
	}
	fence := authoritystore.ShardWriteFence{
		ExpectedHolderID: authority.HolderID, ExpectedHolderBootID: authority.HolderBootID,
		ExpectedAssignmentRevision: authority.AssignmentRevision, ExpectedWriterEpoch: authority.WriterEpoch,
		ExpectedLeaseEpoch: authority.LeaseEpoch,
	}
	loaded, err := store.LoadAndClaim(context.Background(), authoritystore.LoadAndClaimRequest{
		ActorAddress: address, AuthorityLayoutVersion: "v1", ShardWriteFence: fence,
		ClaimOperationID: "direct-proof-load", NewWriterClaimID: "direct-proof-claim",
		InitialModelFields: authoritystore.ModelState{"record": []byte("before")},
	})
	if err != nil || loaded.Status != authoritystore.LoadAndClaimClaimed {
		t.Fatalf("load=%+v err=%v", loaded, err)
	}
	commitHash := authoritystore.PayloadHash(sha256.Sum256([]byte("direct-proof-commit")))
	key := authoritystore.IdempotencyKey{
		SubjectPlayerID: attached.SubjectPlayerID, StableCommandTypeName: "DIRECT_PROOF_TEST",
		OperationID: "direct-proof-commit", Root: authoritystore.OperationRootRef{Kind: authoritystore.RootKindFarmActor, CanonicalID: fmt.Sprint(testFarmID)},
	}
	outcome := authoritystore.StoredOperationOutcome{Status: authoritystore.TerminalSucceeded, Payload: []byte("committed")}
	request := authoritystore.AuthorityCommitRequest{
		CommitRequestID: "direct-proof-request", ActorAddress: address, AuthorityLayoutVersion: "v1",
		ShardWriteFence: fence, ExpectedWriterClaimID: "direct-proof-claim", ExpectedRecordVersion: loaded.Snapshot.RecordVersion,
		IdempotencyKey: key, CanonicalPayloadHash: commitHash,
		CandidateState: authoritystore.ModelState{"record": []byte("after")},
		Receipt: authoritystore.AuthorityReceipt{ReceiptID: "direct-proof-receipt", IdempotencyKey: key,
			CanonicalPayloadHash: commitHash, Outcome: outcome},
		OperationResult: outcome, AttachedOrders: []authoritystore.AttachedOrder{attached},
	}
	result, err := store.CommitExact(context.Background(), request)
	if err != nil || result.Status != authoritystore.CommitCommitted {
		t.Fatalf("commit=%+v err=%v", result, err)
	}
	exact, ok := authoritystore.NewExactCommitEvidence(request, result)
	if !ok {
		t.Fatal("first COMMITTED output did not expose exact evidence")
	}
	return &exact, actorrouting.Assignment{
		Kind: actorrouting.DomainFarm, ShardID: shard, HolderPod: authority.HolderID, HolderBootID: authority.HolderBootID,
		ShardRevision: authority.AssignmentRevision, AssignmentRevision: authority.AssignmentRevision,
		WriterEpoch: authority.WriterEpoch, LeaseEpoch: authority.LeaseEpoch, LeaseUntilMs: authority.LeaseUntilMs,
		HolderState: actorrouting.HolderStateActive, ShardState: actorrouting.ShardStateActive,
	}
}

func TestDirectPreferPropagatesExactCommitStampIntoProto(t *testing.T) {
	_, calculator, bucket := testBucket(t)
	now := time.UnixMilli(1_800_000_300_000)
	incidentID := testIncidentID(t, "proof-envelope")
	orderID := mustOrderID(t, "proof-envelope")
	payload, decoded := testPayload(t, orderID, incidentID)
	payloadHash, err := CareRewardPayloadHash(decoded)
	if err != nil {
		t.Fatal(err)
	}
	attached := authoritystore.AttachedOrder{
		OrderID: orderID, OrderType: FamilyCareReward, Payload: payload,
		PayloadHash: authoritystore.PayloadHash(payloadHash), DueAtMs: now.Add(-time.Second).UnixMilli(),
		OperationID: "care-proof-envelope", SubjectPlayerID: decoded.GetCarerPlayerId(),
	}
	exact, assignment := exactCommitForDirect(t, calculator, attached, now)
	seed := sha256.Sum256([]byte("direct-holder-boot-proof-key"))
	private := ed25519.NewKeyFromSeed(seed[:])
	signer, err := sourceorder.NewCommitProofSigner("classic-farm", assignment.HolderPod, assignment.HolderBootID, "farm-key-a", private)
	if err != nil {
		t.Fatal(err)
	}
	resolver := assignmentResolverFunc(func(_ context.Context, address actorrouting.Address) (actorrouting.Assignment, error) {
		if address.Realm != "classic-farm" || address.Kind != actorrouting.DomainFarm || address.ID != testFarmID {
			return actorrouting.Assignment{}, actorrouting.ErrNoAssignment
		}
		return assignment, nil
	})
	issuer, err := sourceorder.NewCommitProofIssuer(signer, resolver, "classic-farm", 10*time.Second, 2*time.Second, func() time.Time { return now })
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
		Owner: "direct-proof", Token: "direct-proof-token", CommitProofMode: sourceorder.CommitProofModePrefer,
		CommitProofIssuer: issuer, BucketForFarm: func(farmID uint64) (rediskey.AuthorityBucketRef, error) {
			return testBucketForFarm(calculator, farmID)
		}, ItemDeadline: time.Second, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	address, err := careWorkAddress(bucket, orderID, testFarmID)
	if err != nil {
		t.Fatal(err)
	}
	if err := direct.DispatchCommittedOnceWithEvidence(context.Background(), address, attached, exact); err != nil {
		t.Fatal(err)
	}
	if len(participant.requests) != 1 || participant.requests[0].GetSourceOrderCommitProof() == nil {
		t.Fatalf("Direct requests=%d proof=%v", len(participant.requests), len(participant.requests) == 1 && participant.requests[0].GetSourceOrderCommitProof() != nil)
	}
	proof, err := sourceorder.CommitProofFromProto(participant.requests[0].GetSourceOrderCommitProof())
	if err != nil {
		t.Fatal(err)
	}
	if proof.Statement.SourceRecordVersion != 2 || proof.Statement.CommittedAtMs != now.UnixMilli() ||
		proof.Statement.DueAtMs != attached.DueAtMs || proof.Statement.DestinationInvocationID != mustInvocation(t, orderID) {
		t.Fatalf("propagated statement=%+v", proof.Statement)
	}
}

func TestDirectOffProofGateAllocationsAreModelSizeIndependent(t *testing.T) {
	direct := &DirectDispatcher{config: directConfig{CommitProofMode: sourceorder.CommitProofModeOff}}
	request := &contractspb.ParticipantRequest{}
	for _, size := range []int{1, 1 << 20} {
		order := redisauthority.SourceOrderSnapshot{TypedPayload: make([]byte, size)}
		allocations := testing.AllocsPerRun(1000, func() {
			if err := direct.attachCommitProof(context.Background(), request, workidentity.WorkAddress{}, order,
				[sha256.Size]byte{}, "", "", nil); err != nil {
				panic(err)
			}
		})
		if allocations != 0 {
			t.Fatalf("off proof gate payload bytes=%d allocations=%v, want zero", size, allocations)
		}
	}
}

func TestDirectOffIgnoresExactCarrierAndDoesNoProofWork(t *testing.T) {
	_, calculator, bucket := testBucket(t)
	incidentID := testIncidentID(t, "off-no-proof-work")
	orderID := mustOrderID(t, "off-no-proof-work")
	payload, decoded := testPayload(t, orderID, incidentID)
	payloadHash, err := CareRewardPayloadHash(decoded)
	if err != nil {
		t.Fatal(err)
	}
	participant := &fakeParticipant{responses: []*contractspb.ParticipantResponse{
		testResponse(t, orderID, incidentID, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED),
	}}
	lease := newFakeLease()
	direct, err := NewDirectDispatcher(DirectConfig{
		WorkLease: lease, SourceOrders: failingSourceOrders{}, Route: testRoute, Player: participant,
		Owner: "direct-off", Token: "direct-off-token",
		BucketForFarm: func(farmID uint64) (rediskey.AuthorityBucketRef, error) { return testBucketForFarm(calculator, farmID) },
		Now:           time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	address, err := careWorkAddress(bucket, orderID, testFarmID)
	if err != nil {
		t.Fatal(err)
	}
	attached := authoritystore.AttachedOrder{OrderID: orderID, OrderType: FamilyCareReward, Payload: payload,
		PayloadHash: authoritystore.PayloadHash(payloadHash), DueAtMs: time.Now().UnixMilli(),
		OperationID: "off-no-proof-work", SubjectPlayerID: decoded.GetCarerPlayerId()}
	// A zero exact carrier would fail Open. Off must not inspect or deep-copy it.
	if err := direct.DispatchCommittedOnceWithEvidence(context.Background(), address, attached, &authoritystore.ExactCommitEvidence{}); err != nil {
		t.Fatal(err)
	}
	stats := direct.Stats()
	if len(participant.requests) != 1 || participant.requests[0].GetSourceOrderCommitProof() != nil ||
		stats.ProofAttached != 0 || stats.ProofUnavailable != 0 {
		t.Fatalf("off performed proof work: requests=%d proof=%v stats=%+v", len(participant.requests),
			len(participant.requests) == 1 && participant.requests[0].GetSourceOrderCommitProof() != nil, stats)
	}
}

func TestDirectPreferWithoutExactStampLeavesDurableRecovery(t *testing.T) {
	_, calculator, bucket := testBucket(t)
	incidentID := testIncidentID(t, "missing-proof-envelope")
	orderID := mustOrderID(t, "missing-proof-envelope")
	payload, decoded := testPayload(t, orderID, incidentID)
	payloadHash, err := CareRewardPayloadHash(decoded)
	if err != nil {
		t.Fatal(err)
	}
	seed := sha256.Sum256([]byte("unused-direct-proof-key"))
	signer, _ := sourceorder.NewCommitProofSigner("classic-farm", "farm-holder", "farm-boot", "key", ed25519.NewKeyFromSeed(seed[:]))
	issuer, _ := sourceorder.NewCommitProofIssuer(signer, assignmentResolverFunc(func(context.Context, actorrouting.Address) (actorrouting.Assignment, error) {
		return actorrouting.Assignment{}, actorrouting.ErrNoAssignment
	}), "classic-farm", 10*time.Second, 2*time.Second, time.Now)
	participant := &fakeParticipant{}
	direct, err := NewDirectDispatcher(DirectConfig{
		WorkLease: newFakeLease(), SourceOrders: failingSourceOrders{}, Route: testRoute, Player: participant,
		Owner: "direct-proof", Token: "direct-proof-token", CommitProofMode: sourceorder.CommitProofModePrefer,
		CommitProofIssuer: issuer, BucketForFarm: func(farmID uint64) (rediskey.AuthorityBucketRef, error) { return testBucketForFarm(calculator, farmID) },
	})
	if err != nil {
		t.Fatal(err)
	}
	address, _ := careWorkAddress(bucket, orderID, testFarmID)
	attached := authoritystore.AttachedOrder{OrderID: orderID, OrderType: FamilyCareReward, Payload: payload,
		PayloadHash: authoritystore.PayloadHash(payloadHash), DueAtMs: time.Now().UnixMilli(), OperationID: "missing-proof", SubjectPlayerID: decoded.GetCarerPlayerId()}
	if err := direct.DispatchCommittedOnce(context.Background(), address, attached); err != sourceorder.ErrCommitProofMissing {
		t.Fatalf("missing exact stamp err=%v", err)
	}
	if len(participant.requests) != 0 {
		t.Fatal("prefer mode blindly executed without exact commit evidence")
	}
}
