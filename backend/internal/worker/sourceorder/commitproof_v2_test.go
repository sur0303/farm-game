package sourceorder

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actorrouting"
	"farmgame/internal/contracts"
	"farmgame/internal/identity"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/workidentity"

	"google.golang.org/protobuf/proto"
)

type commitProofResolverFunc func(context.Context, actorrouting.Address) (actorrouting.Assignment, error)

func (f commitProofResolverFunc) Resolve(ctx context.Context, address actorrouting.Address) (actorrouting.Assignment, error) {
	return f(ctx, address)
}

type commitProofRouteVerifierFunc func(context.Context, CommitProofStatement) error

func (f commitProofRouteVerifierFunc) VerifyCommitProofRoute(ctx context.Context, statement CommitProofStatement) error {
	return f(ctx, statement)
}

func commitProofExpected(statement CommitProofStatement) CommitProofExpected {
	return CommitProofExpected{
		Work: statement.Work, WorkVersion: statement.WorkVersion, OperationID: statement.OperationID,
		SubjectPlayerID: statement.SubjectPlayerID, PayloadHash: statement.PayloadHash,
		SourceRealm: statement.SourceRealm, AuthorityLayoutVersion: statement.AuthorityLayoutVersion,
		DestinationRouteDomain: statement.DestinationRouteDomain, DestinationActorID: statement.DestinationActorID,
		DestinationStep: statement.DestinationStep, DestinationInvocationID: statement.DestinationInvocationID,
	}
}

func commitProofAssignment(statement CommitProofStatement) actorrouting.Assignment {
	return actorrouting.Assignment{
		Kind: actorrouting.DomainFarm, ShardID: statement.Work.BucketID,
		HolderPod: statement.SourceHolderID, HolderBootID: statement.SourceHolderBootID,
		ShardRevision: statement.AssignmentRevision, AssignmentRevision: statement.AssignmentRevision,
		WriterEpoch: statement.WriterEpoch, LeaseEpoch: statement.LeaseEpoch, LeaseUntilMs: statement.LeaseUntilMs,
		HolderState: actorrouting.HolderStateActive, ShardState: actorrouting.ShardStateActive,
	}
}

func signedTestCommitProof(t *testing.T) (CommitProofStatement, CommitProof, ed25519.PublicKey) {
	t.Helper()
	statement := testCommitProofStatement(t)
	public, private := testCommitProofKeys()
	signer, err := NewCommitProofSigner(statement.SourceRealm, statement.SourceHolderID, statement.SourceHolderBootID, "player-key-1", private)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := signer.Sign(statement)
	if err != nil {
		t.Fatal(err)
	}
	return statement, proof, public
}

func testCommitProofKeyring(t *testing.T, statement CommitProofStatement, public ed25519.PublicKey) *CommitProofKeyring {
	t.Helper()
	keyring, err := NewCommitProofKeyring([]CommitProofVerificationKey{{
		SourceRealm: statement.SourceRealm, HolderID: statement.SourceHolderID,
		HolderBootID: statement.SourceHolderBootID, KeyID: "player-key-1", PublicKey: public,
		NotBeforeMs: statement.CommittedAtMs - 1_000, NotAfterMs: statement.LeaseUntilMs + 1_000,
	}}, 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return keyring
}

func testParticipantCall(t *testing.T, statement CommitProofStatement, wire *contractspb.SourceOrderCommitProof) *contracts.ParticipantCall {
	t.Helper()
	work, err := commitProofWorkToProto(statement.Work, statement.AuthorityLayoutVersion)
	if err != nil {
		t.Fatal(err)
	}
	command := contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_CARE_REWARD
	if statement.Work.WorkType == string(identity.WorkTypeHarvest) {
		command = contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD
	}
	spec, err := contracts.LookupParticipantCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	var payload proto.Message = &contractspb.ApplyCareRewardPayload{CareResolutionSnapshot: &contractspb.CareResolutionSnapshot{SourceFarmId: statement.Work.SourceActorID}}
	if statement.Work.WorkType == string(identity.WorkTypeHarvest) {
		payload = &contractspb.GrantHarvestRewardPayload{HarvestDecision: &contractspb.HarvestDecision{FarmId: statement.Work.SourceActorID}}
	}
	return &contracts.ParticipantCall{
		Spec: spec, RouteHeader: &logicrpcpb.ActorRouteHeader{
			RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER, ActorId: statement.DestinationActorID,
		},
		Work: work, SourceOrderCommitProof: wire, CallerKind: contracts.ParticipantCallerCoordinator,
		ParticipantStep: statement.DestinationStep, InvocationID: statement.DestinationInvocationID,
		OperationID: statement.OperationID, SubjectPlayerID: statement.SubjectPlayerID,
		ExpectedWorkVersion: statement.WorkVersion, CanonicalStepPayloadHash: statement.PayloadHash, Payload: payload,
	}
}

func TestCommitProofWireRoundTripAndComposedValidation(t *testing.T) {
	statement, proof, public := signedTestCommitProof(t)
	wire, err := CommitProofToProto(proof)
	if err != nil {
		t.Fatal(err)
	}
	roundTrip, err := CommitProofFromProto(wire)
	if err != nil {
		t.Fatal(err)
	}
	if roundTrip.Statement != statement || roundTrip.KeyID != proof.KeyID || string(roundTrip.Signature) != string(proof.Signature) {
		t.Fatalf("round trip=%+v", roundTrip)
	}

	call := testParticipantCall(t, statement, wire)
	expected, err := CommitProofExpectedFromParticipantCall(call, statement.SourceRealm)
	if err != nil {
		t.Fatal(err)
	}
	if expected != commitProofExpected(statement) {
		t.Fatalf("expected=%+v", expected)
	}
	resolver := commitProofResolverFunc(func(ctx context.Context, address actorrouting.Address) (actorrouting.Assignment, error) {
		if err := ctx.Err(); err != nil {
			return actorrouting.Assignment{}, err
		}
		if address.Realm != statement.SourceRealm || address.Kind != actorrouting.DomainFarm || address.ID != statement.Work.SourceActorID {
			return actorrouting.Assignment{}, actorrouting.ErrNoAssignment
		}
		return commitProofAssignment(statement), nil
	})
	route, err := NewLocalCommitProofRouteVerifier(resolver, statement.SourceRealm)
	if err != nil {
		t.Fatal(err)
	}
	validator, err := NewCommitProofValidator(testCommitProofKeyring(t, statement, public), route,
		func() time.Time { return time.UnixMilli(statement.CommittedAtMs + 1_000) })
	if err != nil {
		t.Fatal(err)
	}
	verified, err := validator.VerifyParticipant(context.Background(), call, statement.SourceRealm)
	if err != nil || verified != statement {
		t.Fatalf("verified=%+v err=%v", verified, err)
	}

	// The duplicate layout carried in WorkAddress may not be normalized away:
	// it must match the independently signed statement layout exactly.
	wire.Statement.Work.SourceAuthority.AuthorityLayoutVersion = "other-layout"
	if _, err := CommitProofFromProto(wire); !errors.Is(err, ErrCommitProofInvalid) {
		t.Fatalf("mismatched wire layout accepted: %v", err)
	}
}

func TestHarvestCommitProofWireAndExpectedBinding(t *testing.T) {
	statement := testCommitProofStatement(t)
	workID, err := identity.SourceOrderID(identity.WorkTypeHarvest, identity.SourceKindFarmActor, "42", "harvest-proof-operation", 1)
	if err != nil {
		t.Fatal(err)
	}
	statement.Work.WorkID = workID
	statement.Work.WorkType = string(identity.WorkTypeHarvest)
	statement.OperationID = "harvest-proof-operation"
	statement.DestinationStep = string(identity.StepGrantHarvestReward)
	statement.DestinationInvocationID = "harvest-proof-invocation"
	_, private := testCommitProofKeys()
	signer, err := NewCommitProofSigner(statement.SourceRealm, statement.SourceHolderID, statement.SourceHolderBootID, "player-key-1", private)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := signer.Sign(statement)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := CommitProofToProto(proof)
	if err != nil {
		t.Fatal(err)
	}
	call := testParticipantCall(t, statement, wire)
	expected, err := CommitProofExpectedFromParticipantCall(call, statement.SourceRealm)
	if err != nil {
		t.Fatal(err)
	}
	if expected != commitProofExpected(statement) {
		t.Fatalf("HARVEST expected=%+v", expected)
	}
	decoded, err := CommitProofFromProto(wire)
	if err != nil || decoded.Statement != statement {
		t.Fatalf("HARVEST decoded=%+v err=%v", decoded, err)
	}
	call.Payload.(*contractspb.GrantHarvestRewardPayload).HarvestDecision.FarmId++
	if _, err := CommitProofExpectedFromParticipantCall(call, statement.SourceRealm); !errors.Is(err, ErrCommitProofInvalid) {
		t.Fatalf("HARVEST payload source farm detached from signed source actor: %v", err)
	}
}

func TestCommitProofKeyringFailsClosedAcrossIdentityTimeAndExpectedBindings(t *testing.T) {
	statement, proof, public := signedTestCommitProof(t)
	keyring := testCommitProofKeyring(t, statement, public)
	expected := commitProofExpected(statement)
	if err := keyring.Verify(proof, expected, time.UnixMilli(statement.ExpiresAtMs-1)); err != nil {
		t.Fatalf("valid proof rejected: %v", err)
	}
	if err := keyring.Verify(proof, expected, time.UnixMilli(statement.ExpiresAtMs)); !errors.Is(err, ErrCommitProofStale) {
		t.Fatalf("expiry boundary accepted: %v", err)
	}

	changed := expected
	changed.DestinationInvocationID = "other-invocation"
	if err := keyring.Verify(proof, changed, time.UnixMilli(statement.DueAtMs+1)); !errors.Is(err, ErrCommitProofSignature) {
		t.Fatalf("changed destination binding accepted: %v", err)
	}
	foreign, err := NewCommitProofKeyring([]CommitProofVerificationKey{{
		SourceRealm: statement.SourceRealm, HolderID: "other-holder", HolderBootID: statement.SourceHolderBootID,
		KeyID: proof.KeyID, PublicKey: public, NotBeforeMs: statement.CommittedAtMs - 1, NotAfterMs: statement.LeaseUntilMs,
	}}, 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := foreign.Verify(proof, expected, time.UnixMilli(statement.DueAtMs+1)); !errors.Is(err, ErrCommitProofSignature) {
		t.Fatalf("foreign holder key accepted: %v", err)
	}

	// Construction owns a key copy; operator input mutation cannot alter the
	// immutable trust registry.
	inputKey := append(ed25519.PublicKey(nil), public...)
	copied, err := NewCommitProofKeyring([]CommitProofVerificationKey{{
		SourceRealm: statement.SourceRealm, HolderID: statement.SourceHolderID, HolderBootID: statement.SourceHolderBootID,
		KeyID: proof.KeyID, PublicKey: inputKey, NotBeforeMs: statement.CommittedAtMs - 1, NotAfterMs: statement.LeaseUntilMs,
	}}, 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	inputKey[0]++
	if err := copied.Verify(proof, expected, time.UnixMilli(statement.DueAtMs+1)); err != nil {
		t.Fatalf("caller mutation changed keyring: %v", err)
	}
}

func prepareCommitProofMemoryStatement(t *testing.T, statement *CommitProofStatement) {
	t.Helper()
	calculator, err := actorrouting.NewCalculator(
		"commit-proof-memory-test",
		actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608190001},
		64, 64,
	)
	if err != nil {
		t.Fatal(err)
	}
	shard, err := calculator.ShardFor(actorrouting.DomainFarm, statement.Work.SourceActorID)
	if err != nil {
		t.Fatal(err)
	}
	statement.Work.BucketID = shard
	// A fresh MemoryStore actor starts at RecordVersion 1 after LoadAndClaim.
	statement.SourceRecordVersion = 2
}

func commitProofThroughMemory(t *testing.T, statement CommitProofStatement, order authoritystore.AttachedOrder, suffix string) (authoritystore.AuthorityCommitRequest, authoritystore.CommitResult) {
	t.Helper()
	calculator, err := actorrouting.NewCalculator(
		"commit-proof-memory-test",
		actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608190001},
		64, 64,
	)
	if err != nil {
		t.Fatal(err)
	}
	store, err := authoritystore.NewMemoryStore(authoritystore.MemoryOptions{
		Resolver: calculator, AuthorityLayoutVersion: statement.AuthorityLayoutVersion,
		Now: func() time.Time { return time.UnixMilli(statement.CommittedAtMs) },
	})
	if err != nil {
		t.Fatal(err)
	}
	address := authoritystore.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: statement.Work.SourceActorID}
	shard, err := calculator.ShardFor(address.Domain, address.ActorID)
	if err != nil {
		t.Fatal(err)
	}
	if shard != statement.Work.BucketID {
		t.Fatalf("memory shard=%d, statement bucket=%d", shard, statement.Work.BucketID)
	}
	authority := authoritystore.ShardWriteAuthority{
		AuthorityLayoutVersion: statement.AuthorityLayoutVersion,
		Domain:                 address.Domain, VirtualShard: shard,
		HolderID: statement.SourceHolderID, HolderBootID: statement.SourceHolderBootID,
		AssignmentRevision: statement.AssignmentRevision, WriterEpoch: statement.WriterEpoch,
		LeaseEpoch: statement.LeaseEpoch, State: authoritystore.ShardAuthorityActive,
		LeaseUntilMs: statement.LeaseUntilMs,
	}
	if err := store.SetShardWriteAuthority(authority); err != nil {
		t.Fatal(err)
	}
	fence := authoritystore.ShardWriteFence{
		ExpectedHolderID: authority.HolderID, ExpectedHolderBootID: authority.HolderBootID,
		ExpectedAssignmentRevision: authority.AssignmentRevision, ExpectedWriterEpoch: authority.WriterEpoch,
		ExpectedLeaseEpoch: authority.LeaseEpoch,
	}
	const claimID = "commit-proof-claim"
	loaded, err := store.LoadAndClaim(context.Background(), authoritystore.LoadAndClaimRequest{
		ActorAddress: address, AuthorityLayoutVersion: statement.AuthorityLayoutVersion, ShardWriteFence: fence,
		ClaimOperationID: "commit-proof-load", NewWriterClaimID: claimID,
		InitialModelFields: authoritystore.ModelState{"proof": []byte("initial")},
	})
	if err != nil || loaded.Status != authoritystore.LoadAndClaimClaimed {
		t.Fatalf("memory load=%+v err=%v", loaded, err)
	}
	if loaded.Snapshot.RecordVersion+1 != statement.SourceRecordVersion {
		t.Fatalf("loaded version=%d, statement source version=%d", loaded.Snapshot.RecordVersion, statement.SourceRecordVersion)
	}
	operationID := "commit-proof-" + suffix
	payloadHash := authoritystore.PayloadHash(sha256.Sum256([]byte(operationID + ":payload")))
	key := authoritystore.IdempotencyKey{
		SubjectPlayerID: statement.SubjectPlayerID, StableCommandTypeName: "COMMIT_PROOF_TEST", OperationID: operationID,
		Root: authoritystore.OperationRootRef{Kind: authoritystore.RootKindFarmActor, CanonicalID: fmt.Sprintf("%d", address.ActorID)},
	}
	outcome := authoritystore.StoredOperationOutcome{Status: authoritystore.TerminalSucceeded, Payload: []byte("committed")}
	request := authoritystore.AuthorityCommitRequest{
		CommitRequestID: "commit-request-" + suffix,
		ActorAddress:    address, AuthorityLayoutVersion: statement.AuthorityLayoutVersion, ShardWriteFence: fence,
		ExpectedWriterClaimID: claimID, ExpectedRecordVersion: loaded.Snapshot.RecordVersion,
		IdempotencyKey: key, CanonicalPayloadHash: payloadHash,
		CandidateState: authoritystore.ModelState{"proof": []byte(suffix)},
		Receipt: authoritystore.AuthorityReceipt{ReceiptID: "commit-receipt-" + suffix, IdempotencyKey: key,
			CanonicalPayloadHash: payloadHash, Outcome: outcome},
		OperationResult: outcome, AttachedOrders: []authoritystore.AttachedOrder{order},
	}
	result, err := store.CommitExact(context.Background(), request)
	if err != nil || result.Status != authoritystore.CommitCommitted || result.CommitStamp == nil {
		t.Fatalf("memory commit=%+v err=%v", result, err)
	}
	return request, result
}

func TestCommitProofIssuerRequiresExactCurrentRouteAndBoundedDefiniteEvidence(t *testing.T) {
	statement := testCommitProofStatement(t)
	prepareCommitProofMemoryStatement(t, &statement)
	_, private := testCommitProofKeys()
	signer, err := NewCommitProofSigner(statement.SourceRealm, statement.SourceHolderID, statement.SourceHolderBootID, "player-key-1", private)
	if err != nil {
		t.Fatal(err)
	}
	assignment := commitProofAssignment(statement)
	resolver := commitProofResolverFunc(func(context.Context, actorrouting.Address) (actorrouting.Assignment, error) {
		return assignment, nil
	})
	now := time.UnixMilli(statement.CommittedAtMs + 1_000)
	issuer, err := NewCommitProofIssuer(signer, resolver, statement.SourceRealm, 10*time.Second, 2*time.Second, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := testCareCommitProofPayload(t)
	order := authoritystore.AttachedOrder{
		OrderID: statement.Work.WorkID, OrderType: statement.Work.WorkType,
		Payload: payload, PayloadHash: statement.PayloadHash,
		DueAtMs: statement.DueAtMs, OperationID: statement.OperationID, SubjectPlayerID: statement.SubjectPlayerID,
	}
	authorityRequest, authorityResult := commitProofThroughMemory(t, statement, order, "exact")
	evidence, err := NewDurableCommitEvidence(authorityRequest, authorityResult, statement.Work.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	rawHashOrder := order
	rawHashOrder.PayloadHash = authoritystore.PayloadHash(sha256.Sum256(payload))
	rawHashRequest, rawHashResult := commitProofThroughMemory(t, statement, rawHashOrder, "raw-hash")
	if _, err := NewDurableCommitEvidence(rawHashRequest, rawHashResult, statement.Work.WorkID); !errors.Is(err, ErrCommitProofInvalid) {
		t.Fatalf("raw protobuf SHA minted evidence: %v", err)
	}
	changedRequest := authorityRequest
	changedRequest.AttachedOrders = append([]authoritystore.AttachedOrder(nil), authorityRequest.AttachedOrders...)
	changedRequest.AttachedOrders[0].DueAtMs++
	if _, err := NewDurableCommitEvidence(changedRequest, authorityResult, statement.Work.WorkID); !errors.Is(err, ErrCommitProofInvalid) {
		t.Fatalf("stamp copied to a different request minted evidence: %v", err)
	}
	request := CommitProofMintRequest{
		Work: statement.Work, WorkVersion: statement.WorkVersion, OperationID: statement.OperationID,
		SubjectPlayerID: statement.SubjectPlayerID, PayloadHash: statement.PayloadHash, DueAtMs: statement.DueAtMs,
		AuthorityLayoutVersion: statement.AuthorityLayoutVersion,
		DestinationRouteDomain: statement.DestinationRouteDomain, DestinationActorID: statement.DestinationActorID,
		DestinationStep: statement.DestinationStep, DestinationInvocationID: statement.DestinationInvocationID,
		Commit: evidence,
	}
	minted, err := issuer.Mint(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if minted.Statement.ExpiresAtMs != statement.CommittedAtMs+10_000 || minted.Statement.SourceRecordVersion != statement.SourceRecordVersion {
		t.Fatalf("minted statement=%+v", minted.Statement)
	}
	futureOrder := order
	futureOrder.DueAtMs = now.Add(time.Millisecond).UnixMilli()
	futureAuthority, futureResult := commitProofThroughMemory(t, statement, futureOrder, "future-due")
	futureEvidence, err := NewDurableCommitEvidence(futureAuthority, futureResult, statement.Work.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	futureRequest := request
	futureRequest.DueAtMs = futureAuthority.AttachedOrders[0].DueAtMs
	futureRequest.Commit = futureEvidence
	if _, err := issuer.Mint(context.Background(), futureRequest); !errors.Is(err, ErrCommitProofStale) {
		t.Fatalf("future-due proof minted: %v", err)
	}

	assignment.WriterEpoch++
	if _, err := issuer.Mint(context.Background(), request); !errors.Is(err, ErrCommitProofRoute) {
		t.Fatalf("mismatched current route accepted: %v", err)
	}
	assignment = commitProofAssignment(statement)
	assignment.HolderState = actorrouting.HolderStateDraining
	if _, err := issuer.Mint(context.Background(), request); !errors.Is(err, ErrCommitProofRoute) {
		t.Fatalf("inactive current route accepted: %v", err)
	}
	assignment = commitProofAssignment(statement)
	request.Commit.sourceRecordVersion = 0
	if _, err := issuer.Mint(context.Background(), request); !errors.Is(err, ErrCommitProofInvalid) {
		t.Fatalf("incomplete durable evidence accepted: %v", err)
	}
	request.Commit = evidence
	request.Commit.committedAtMs = now.Add(-time.Minute).UnixMilli()
	if _, err := issuer.Mint(context.Background(), request); !errors.Is(err, ErrCommitProofStale) {
		t.Fatalf("old durable evidence accepted: %v", err)
	}
	unknown := authorityResult
	unknown.Status = authoritystore.CommitTimeoutUnknown
	if _, err := NewDurableCommitEvidence(authorityRequest, unknown, statement.Work.WorkID); !errors.Is(err, ErrCommitProofInvalid) {
		t.Fatalf("unknown Authority commit produced evidence: %v", err)
	}
	already := authorityResult
	already.Status = authoritystore.CommitAlreadyCommitted
	if _, err := NewDurableCommitEvidence(authorityRequest, already, statement.Work.WorkID); !errors.Is(err, ErrCommitProofInvalid) {
		t.Fatalf("ALREADY_COMMITTED Authority result produced evidence: %v", err)
	}
	callerAssembled := authoritystore.CommitResult{
		Status: authoritystore.CommitCommitted,
		Snapshot: authoritystore.AuthoritySnapshot{
			ActorAddress: authorityRequest.ActorAddress, RecordVersion: authorityResult.Snapshot.RecordVersion,
		},
	}
	if _, err := NewDurableCommitEvidence(authorityRequest, callerAssembled, statement.Work.WorkID); !errors.Is(err, ErrCommitProofInvalid) {
		t.Fatalf("caller-assembled COMMITTED result produced evidence: %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := issuer.Mint(canceled, request); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled mint err=%v", err)
	}
}

func TestCommitProofValidatorNeverSkipsCurrentRouteVerification(t *testing.T) {
	statement, proof, public := signedTestCommitProof(t)
	routeCalls := 0
	route := commitProofRouteVerifierFunc(func(context.Context, CommitProofStatement) error {
		routeCalls++
		return ErrCommitProofRoute
	})
	validator, err := NewCommitProofValidator(testCommitProofKeyring(t, statement, public), route,
		func() time.Time { return time.UnixMilli(statement.DueAtMs + 1) })
	if err != nil {
		t.Fatal(err)
	}
	if err := validator.Verify(context.Background(), proof, commitProofExpected(statement)); !errors.Is(err, ErrCommitProofRoute) || routeCalls != 1 {
		t.Fatalf("route verification err=%v calls=%d", err, routeCalls)
	}

	tampered := proof
	tampered.Signature = append([]byte(nil), proof.Signature...)
	tampered.Signature[0]++
	if err := validator.Verify(context.Background(), tampered, commitProofExpected(statement)); !errors.Is(err, ErrCommitProofSignature) || routeCalls != 1 {
		t.Fatalf("tampered proof reached route verifier: err=%v calls=%d", err, routeCalls)
	}
}

func TestCommitProofRouteMoveRaceNeverAcceptsAfterNewGenerationObserved(t *testing.T) {
	statement, proof, public := signedTestCommitProof(t)
	oldAssignment := commitProofAssignment(statement)
	newAssignment := oldAssignment
	newAssignment.AssignmentRevision++
	newAssignment.ShardRevision++
	newAssignment.WriterEpoch++
	newAssignment.LeaseEpoch++
	var moved atomic.Bool
	resolver := commitProofResolverFunc(func(ctx context.Context, _ actorrouting.Address) (actorrouting.Assignment, error) {
		if err := ctx.Err(); err != nil {
			return actorrouting.Assignment{}, err
		}
		if moved.Load() {
			return newAssignment, nil
		}
		return oldAssignment, nil
	})
	route, err := NewLocalCommitProofRouteVerifier(resolver, statement.SourceRealm)
	if err != nil {
		t.Fatal(err)
	}
	validator, err := NewCommitProofValidator(testCommitProofKeyring(t, statement, public), route,
		func() time.Time { return time.UnixMilli(statement.DueAtMs + 1) })
	if err != nil {
		t.Fatal(err)
	}
	const readers = 16
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 100; j++ {
				_ = validator.Verify(context.Background(), proof, commitProofExpected(statement))
			}
		}()
	}
	close(start)
	moved.Store(true)
	wg.Wait()
	for i := 0; i < readers; i++ {
		if err := validator.Verify(context.Background(), proof, commitProofExpected(statement)); !errors.Is(err, ErrCommitProofRoute) {
			t.Fatalf("old proof accepted after source move: %v", err)
		}
	}
}

func TestCommitProofValidatorRejectsFutureDueAndRechecksExpiryAfterRoute(t *testing.T) {
	statement, proof, public := signedTestCommitProof(t)
	routeCalls := 0
	route := commitProofRouteVerifierFunc(func(context.Context, CommitProofStatement) error {
		routeCalls++
		return nil
	})

	future, err := NewCommitProofValidator(testCommitProofKeyring(t, statement, public), route,
		func() time.Time { return time.UnixMilli(statement.DueAtMs - 1) })
	if err != nil {
		t.Fatal(err)
	}
	if err := future.Verify(context.Background(), proof, commitProofExpected(statement)); !errors.Is(err, ErrCommitProofStale) || routeCalls != 0 {
		t.Fatalf("future-due proof err=%v route_calls=%d", err, routeCalls)
	}

	clockReads := 0
	expiresDuringRoute, err := NewCommitProofValidator(testCommitProofKeyring(t, statement, public), route, func() time.Time {
		clockReads++
		if clockReads == 1 {
			return time.UnixMilli(statement.DueAtMs + 1)
		}
		return time.UnixMilli(statement.ExpiresAtMs)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := expiresDuringRoute.Verify(context.Background(), proof, commitProofExpected(statement)); !errors.Is(err, ErrCommitProofStale) || routeCalls != 1 || clockReads != 2 {
		t.Fatalf("expiry-during-route err=%v route_calls=%d clock_reads=%d", err, routeCalls, clockReads)
	}
}

func TestCommitProofOnlyAllowsCareAndHarvestDirectBindings(t *testing.T) {
	statement := testCommitProofStatement(t)
	cases := map[string]func(*CommitProofStatement){
		"player source": func(s *CommitProofStatement) { s.Work.SourceRouteDomain = workidentity.RouteDomainPlayer },
		"plant": func(s *CommitProofStatement) {
			s.Work.WorkType = string(identity.WorkTypePlant)
			s.DestinationStep = string(identity.StepCommitPlant)
		},
		"farm destination":          func(s *CommitProofStatement) { s.DestinationRouteDomain = workidentity.RouteDomainFarm },
		"foreign destination actor": func(s *CommitProofStatement) { s.DestinationActorID++ },
		"wrong step":                func(s *CommitProofStatement) { s.DestinationStep = string(identity.StepGrantHarvestReward) },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			candidate := statement
			mutate(&candidate)
			if _, err := CanonicalCommitProofBytes(candidate, "player-key-1"); !errors.Is(err, ErrCommitProofInvalid) {
				t.Fatalf("invalid binding accepted: %v", err)
			}
		})
	}
}

func TestCommitProofConfigurationParsingIsBoundedAndStaged(t *testing.T) {
	statement, _, public := signedTestCommitProof(t)
	encodedPublic := base64.StdEncoding.EncodeToString(public)
	raw, err := json.Marshal([]map[string]any{{
		"source_realm": statement.SourceRealm, "holder_id": statement.SourceHolderID,
		"holder_boot_id": statement.SourceHolderBootID, "key_id": "player-key-1",
		"public_key_base64": encodedPublic, "not_before_ms": statement.CommittedAtMs - 1,
		"not_after_ms": statement.LeaseUntilMs,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseCommitProofPublicKeysJSON(string(raw), 15*time.Second); err != nil {
		t.Fatalf("valid keyring JSON rejected: %v", err)
	}
	if _, err := ParseCommitProofPublicKeysJSON(string(raw[:len(raw)-1])+`,{"unknown":true}]`, 15*time.Second); !errors.Is(err, ErrCommitProofInvalid) {
		t.Fatalf("unknown keyring field accepted: %v", err)
	}
	if _, err := ParseCommitProofPublicKeysJSON(string(raw)+` {}`, 15*time.Second); !errors.Is(err, ErrCommitProofInvalid) {
		t.Fatalf("trailing keyring JSON accepted: %v", err)
	}

	_, private := testCommitProofKeys()
	decoded, err := DecodeCommitProofPrivateKey(base64.StdEncoding.EncodeToString(private.Seed()))
	if err != nil || !decoded.Equal(private) {
		t.Fatalf("private seed decode err=%v", err)
	}
	if _, err := DecodeCommitProofPrivateKey(base64.StdEncoding.EncodeToString([]byte("short"))); !errors.Is(err, ErrCommitProofInvalid) {
		t.Fatalf("short private key accepted: %v", err)
	}

	for _, test := range []struct {
		raw  string
		mode CommitProofMode
	}{
		{"", CommitProofModeOff}, {"off", CommitProofModeOff}, {"shadow", CommitProofModeShadow},
	} {
		mode, err := ParseCommitProofMode(test.raw)
		if err != nil || mode != test.mode {
			t.Fatalf("parse %q mode=%v err=%v", test.raw, mode, err)
		}
		if err := mode.ValidateShadowOnly(); err != nil {
			t.Fatalf("shadow gate %q err=%v", test.raw, err)
		}
	}
	prefer, err := ParseCommitProofMode("prefer")
	if err != nil || prefer != CommitProofModePrefer || prefer.ValidatePreferRollout() != nil ||
		!errors.Is(prefer.ValidateShadowOnly(), ErrCommitProofBypassUnavailable) {
		t.Fatalf("prefer rollout parsing/gates mode=%v err=%v", prefer, err)
	}
	if mode, err := ParseCommitProofMode("require"); mode != CommitProofModeOff || !errors.Is(err, ErrCommitProofBypassUnavailable) {
		t.Fatalf("require mode did not fail closed: mode=%v err=%v", mode, err)
	}
	if _, err := ParseCommitProofMode("bypass"); !errors.Is(err, ErrCommitProofInvalid) {
		t.Fatalf("unknown mode accepted: %v", err)
	}
	if _, err := ParseCommitProofPublicKeysJSON(strings.Repeat(" ", MaxCommitProofKeyringJSONBytes+1), 15*time.Second); !errors.Is(err, ErrCommitProofInvalid) {
		t.Fatalf("oversized keyring JSON accepted: %v", err)
	}
	tooMany := make([]commitProofKeyJSON, MaxCommitProofVerificationKeys+1)
	for index := range tooMany {
		tooMany[index] = commitProofKeyJSON{SourceRealm: statement.SourceRealm, HolderID: statement.SourceHolderID,
			HolderBootID: statement.SourceHolderBootID, KeyID: fmt.Sprintf("key-%d", index), PublicKeyBase64: encodedPublic,
			NotBeforeMs: statement.CommittedAtMs - 1, NotAfterMs: statement.LeaseUntilMs}
	}
	tooManyJSON, err := json.Marshal(tooMany)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseCommitProofPublicKeysJSON(string(tooManyJSON), 15*time.Second); !errors.Is(err, ErrCommitProofInvalid) {
		t.Fatalf("oversized keyring cardinality accepted: %v", err)
	}
}

func TestCommitProofKeyringRotationOverlapAndRetirement(t *testing.T) {
	statement, oldProof, oldPublic := signedTestCommitProof(t)
	newSeed := sha256.Sum256([]byte("source-order-commit-proof-rotated-key"))
	newPrivate := ed25519.NewKeyFromSeed(newSeed[:])
	newPublic := newPrivate.Public().(ed25519.PublicKey)
	newSigner, err := NewCommitProofSigner(statement.SourceRealm, statement.SourceHolderID, statement.SourceHolderBootID, "player-key-2", newPrivate)
	if err != nil {
		t.Fatal(err)
	}
	newProof, err := newSigner.Sign(statement)
	if err != nil {
		t.Fatal(err)
	}
	entry := func(keyID string, public ed25519.PublicKey) CommitProofVerificationKey {
		return CommitProofVerificationKey{SourceRealm: statement.SourceRealm, HolderID: statement.SourceHolderID,
			HolderBootID: statement.SourceHolderBootID, KeyID: keyID, PublicKey: public,
			NotBeforeMs: statement.CommittedAtMs - 1, NotAfterMs: statement.LeaseUntilMs}
	}
	overlap, err := NewCommitProofKeyring([]CommitProofVerificationKey{
		entry(oldProof.KeyID, oldPublic), entry(newProof.KeyID, newPublic),
	}, 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	verifyAt := time.UnixMilli(statement.DueAtMs + 1)
	for name, proof := range map[string]CommitProof{"old": oldProof, "new": newProof} {
		if err := overlap.Verify(proof, commitProofExpected(statement), verifyAt); err != nil {
			t.Fatalf("%s proof rejected during rotation overlap: %v", name, err)
		}
	}

	retired, err := NewCommitProofKeyring([]CommitProofVerificationKey{entry(newProof.KeyID, newPublic)}, 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := retired.Verify(oldProof, commitProofExpected(statement), verifyAt); !errors.Is(err, ErrCommitProofSignature) {
		t.Fatalf("retired old key remained trusted: %v", err)
	}
	if err := retired.Verify(newProof, commitProofExpected(statement), verifyAt); err != nil {
		t.Fatalf("new key rejected after old-key retirement: %v", err)
	}
}

func TestCommitProofKeyringHardCardinalityAndMillisecondDurations(t *testing.T) {
	statement, _, public := signedTestCommitProof(t)
	entries := make([]CommitProofVerificationKey, MaxCommitProofVerificationKeys+1)
	for i := range entries {
		entries[i] = CommitProofVerificationKey{
			SourceRealm: statement.SourceRealm, HolderID: statement.SourceHolderID,
			HolderBootID: statement.SourceHolderBootID, KeyID: fmt.Sprintf("key-%d", i), PublicKey: public,
			NotBeforeMs: statement.CommittedAtMs - 1, NotAfterMs: statement.LeaseUntilMs,
		}
	}
	if _, err := NewCommitProofKeyring(entries, time.Second); !errors.Is(err, ErrCommitProofInvalid) {
		t.Fatalf("oversized keyring accepted: %v", err)
	}
	if _, err := NewCommitProofKeyring(entries[:1], time.Microsecond); !errors.Is(err, ErrCommitProofInvalid) {
		t.Fatalf("sub-millisecond TTL accepted: %v", err)
	}
}
