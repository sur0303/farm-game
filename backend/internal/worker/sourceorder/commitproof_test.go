package sourceorder

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	contractspb "farmgame/api/gen/contracts"
	"farmgame/internal/contracts"
	"farmgame/internal/identity"
	"farmgame/internal/workidentity"

	"google.golang.org/protobuf/proto"
)

func testCareCommitProofPayload(t *testing.T) ([]byte, [sha256.Size]byte) {
	t.Helper()
	message := &contractspb.ApplyCareRewardPayload{
		CarerPlayerId: 9001, CareRewardOrderId: "care-proof-order",
		CareResolutionSnapshot: &contractspb.CareResolutionSnapshot{
			SourceFarmId: 42, SourceOwnerPlayerId: 42, CarerPlayerId: 9001,
			PlotId: 1, CropCycleId: "proof-cycle", IncidentId: "proof-incident",
		},
		CareResolutionHash: bytesOf(0x41, sha256.Size),
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_CARE_REWARD)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := contracts.CanonicalParticipantHash(spec, message)
	if err != nil {
		t.Fatal(err)
	}
	return payload, hash
}

func bytesOf(value byte, size int) []byte {
	out := make([]byte, size)
	for index := range out {
		out[index] = value
	}
	return out
}

func testCommitProofStatement(t *testing.T) CommitProofStatement {
	t.Helper()
	workID, err := identity.SourceOrderID(identity.WorkTypeCareReward, identity.SourceKindFarmActor, "42", "proof-operation", 1)
	if err != nil {
		t.Fatal(err)
	}
	_, payloadHash := testCareCommitProofPayload(t)
	return CommitProofStatement{
		Work: workidentity.WorkAddress{
			WorkID: workID, WorkType: string(identity.WorkTypeCareReward), PlacementKind: workidentity.SourcePersistenceBucket,
			BucketVersion: 1, BucketID: 17, SourceRouteDomain: workidentity.RouteDomainFarm, SourceActorID: 42,
		},
		WorkVersion: 1, OperationID: "proof-operation", SubjectPlayerID: 9001,
		PayloadHash: payloadHash, DueAtMs: 1_800_000_000_100,
		SourceRealm: "classic-farm", AuthorityLayoutVersion: "v1",
		SourceHolderID: "player-holder-a", SourceHolderBootID: "player-boot-a",
		AssignmentRevision: 7, WriterEpoch: 11, LeaseEpoch: 13, LeaseUntilMs: 1_800_000_030_000,
		SourceRecordVersion: 19, CommittedAtMs: 1_800_000_000_000, ExpiresAtMs: 1_800_000_010_000,
		DestinationRouteDomain: workidentity.RouteDomainPlayer, DestinationActorID: 9001,
		DestinationStep: string(identity.StepApplyCareReward), DestinationInvocationID: "care-proof-invocation",
	}
}

func testCommitProofKeys() (ed25519.PublicKey, ed25519.PrivateKey) {
	seed := sha256.Sum256([]byte("source-order-commit-proof-test-key"))
	private := ed25519.NewKeyFromSeed(seed[:])
	return private.Public().(ed25519.PublicKey), private
}

func TestCommitProofCanonicalGoldenAndDeterministicSignature(t *testing.T) {
	statement := testCommitProofStatement(t)
	canonical, err := CanonicalCommitProofBytes(statement, "player-key-1")
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(canonical)
	const wantDigest = "4868e41b85cb68478d742096f63401b6b23fa1c0b1f8ed3cfed8f14e1fadf602"
	if got := hex.EncodeToString(digest[:]); got != wantDigest {
		t.Fatalf("canonical digest=%s", got)
	}
	public, private := testCommitProofKeys()
	signer, err := NewCommitProofSigner("classic-farm", "player-holder-a", "player-boot-a", "player-key-1", private)
	if err != nil {
		t.Fatal(err)
	}
	first, err := signer.Sign(statement)
	if err != nil {
		t.Fatal(err)
	}
	second, err := signer.Sign(statement)
	if err != nil {
		t.Fatal(err)
	}
	if string(first.Signature) != string(second.Signature) {
		t.Fatal("Ed25519 signature over canonical statement is not deterministic")
	}
	verifier, err := NewCommitProofVerifier("classic-farm", "player-holder-a", "player-boot-a", "player-key-1", public)
	if err != nil {
		t.Fatal(err)
	}
	if err := verifier.Verify(first, statement); err != nil {
		t.Fatalf("valid commit proof rejected: %v", err)
	}
}

func TestCommitProofVerificationBindsSourceIdentityAndEveryFence(t *testing.T) {
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
	verifier, err := NewCommitProofVerifier(statement.SourceRealm, statement.SourceHolderID, statement.SourceHolderBootID, "player-key-1", public)
	if err != nil {
		t.Fatal(err)
	}

	mutations := map[string]func(*CommitProofStatement){
		"work id":          func(s *CommitProofStatement) { s.Work.WorkID = testCommitProofStatement(t).Work.WorkID + "x" },
		"source actor":     func(s *CommitProofStatement) { s.Work.SourceActorID++ },
		"work version":     func(s *CommitProofStatement) { s.WorkVersion++ },
		"operation":        func(s *CommitProofStatement) { s.OperationID = "other-operation" },
		"subject":          func(s *CommitProofStatement) { s.SubjectPlayerID++ },
		"payload":          func(s *CommitProofStatement) { s.PayloadHash[0]++ },
		"due":              func(s *CommitProofStatement) { s.DueAtMs++ },
		"realm":            func(s *CommitProofStatement) { s.SourceRealm = "other-realm" },
		"layout":           func(s *CommitProofStatement) { s.AuthorityLayoutVersion = "v2" },
		"holder":           func(s *CommitProofStatement) { s.SourceHolderID = "player-holder-b" },
		"boot":             func(s *CommitProofStatement) { s.SourceHolderBootID = "player-boot-b" },
		"assignment":       func(s *CommitProofStatement) { s.AssignmentRevision++ },
		"writer":           func(s *CommitProofStatement) { s.WriterEpoch++ },
		"lease epoch":      func(s *CommitProofStatement) { s.LeaseEpoch++ },
		"lease expiry":     func(s *CommitProofStatement) { s.LeaseUntilMs++ },
		"record version":   func(s *CommitProofStatement) { s.SourceRecordVersion++ },
		"commit time":      func(s *CommitProofStatement) { s.CommittedAtMs++ },
		"expiry":           func(s *CommitProofStatement) { s.ExpiresAtMs++ },
		"destination":      func(s *CommitProofStatement) { s.DestinationActorID++ },
		"destination step": func(s *CommitProofStatement) { s.DestinationStep = string(identity.StepGrantHarvestReward) },
		"invocation":       func(s *CommitProofStatement) { s.DestinationInvocationID = "other-invocation" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			expected := statement
			mutate(&expected)
			if err := verifier.Verify(proof, expected); !errors.Is(err, ErrCommitProofSignature) {
				t.Fatalf("mutated expectation accepted: %v", err)
			}
		})
	}

	foreignIdentity, err := NewCommitProofVerifier("classic-farm", "player-holder-b", "player-boot-b", "player-key-1", public)
	if err != nil {
		t.Fatal(err)
	}
	if err := foreignIdentity.Verify(proof, statement); !errors.Is(err, ErrCommitProofSignature) {
		t.Fatalf("foreign holder identity accepted: %v", err)
	}
	proof.Signature[0]++
	if err := verifier.Verify(proof, statement); !errors.Is(err, ErrCommitProofSignature) {
		t.Fatalf("tampered signature accepted: %v", err)
	}
}

func TestCommitProofSignerFailsClosedOnIncompleteSourceFence(t *testing.T) {
	_, private := testCommitProofKeys()
	signer, err := NewCommitProofSigner("classic-farm", "player-holder-a", "player-boot-a", "player-key-1", private)
	if err != nil {
		t.Fatal(err)
	}
	statement := testCommitProofStatement(t)
	statement.LeaseEpoch = 0
	if _, err := signer.Sign(statement); !errors.Is(err, ErrCommitProofInvalid) {
		t.Fatalf("zero lease epoch signed: %v", err)
	}
	statement = testCommitProofStatement(t)
	statement.LeaseUntilMs = statement.CommittedAtMs
	if _, err := signer.Sign(statement); !errors.Is(err, ErrCommitProofInvalid) {
		t.Fatalf("expired source lease signed: %v", err)
	}
	statement = testCommitProofStatement(t)
	statement.SourceHolderBootID = "other-boot"
	if _, err := signer.Sign(statement); !errors.Is(err, ErrCommitProofInvalid) {
		t.Fatalf("foreign source boot signed: %v", err)
	}
}
