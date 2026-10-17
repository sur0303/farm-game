package friend

// Golden vectors from contract 33A §2.7. Any change in algorithm, field
// order or number normalization must fail these tests.

import (
	"bytes"
	"fmt"
	"testing"
)

func TestStableIDGoldenVectors(t *testing.T) {
	relationID, err := RelationIDFor(10001, 20002)
	if err != nil {
		t.Fatal(err)
	}
	if want := "sid1_E8zcH967pq7Tzc5Sc0fXEquIoIjV05veOlVTkccI8to"; relationID != want {
		t.Fatalf("relation id = %q, want %q", relationID, want)
	}

	// Canonical order is direction-independent.
	reversed, err := RelationIDFor(20002, 10001)
	if err != nil {
		t.Fatal(err)
	}
	if reversed != relationID {
		t.Fatalf("reversed pair id = %q, want %q", reversed, relationID)
	}

	workflowID, err := AddFriendWorkflowID(20002, "op-redeem-0001", relationID)
	if err != nil {
		t.Fatal(err)
	}
	if want := "sid1_dFIIRvo0XLh3V0u3P_w20K8gTHY9M_wTy__AQPavdQI"; workflowID != want {
		t.Fatalf("workflow id = %q, want %q", workflowID, want)
	}

	reservationID, err := QuotaReservationID(workflowID, 10001, relationID)
	if err != nil {
		t.Fatal(err)
	}
	if want := "sid1_zhgY0uuAa9NwvuL2zz9-8QoPwTr3ss8BjFhIj5iun0g"; reservationID != want {
		t.Fatalf("reservation id = %q, want %q", reservationID, want)
	}

	validationID, err := ShareTokenValidationReceiptID(workflowID, 10001, 20002, 7)
	if err != nil {
		t.Fatal(err)
	}
	if want := "sid1_YbhYMhVTgFYgC7ZG_EDcVCrXiz2Yyp2UdujLgGdHiEA"; validationID != want {
		t.Fatalf("validation receipt id = %q, want %q", validationID, want)
	}

	fenceID, err := QuotaReleaseFenceID(10001, relationID, 5)
	if err != nil {
		t.Fatal(err)
	}
	if want := "sid1_Ti_lMovux-TmFS8v9BOPHFuGez38omUc_qqP1yResNE"; fenceID != want {
		t.Fatalf("release fence id = %q, want %q", fenceID, want)
	}
}

func TestPairMutationIdentityRemainsStableAfterRosterRetirement(t *testing.T) {
	operationID, payloadHash, err := PairMutationIdentity(
		PairMutationIdentityRedeemShare, 20002, 10001, "share-token", 0,
	)
	if err != nil {
		t.Fatal(err)
	}
	if want := "friend-1-d6116d4ec05ca1a0e38b138c"; operationID != want {
		t.Fatalf("operation id=%q want=%q", operationID, want)
	}
	if got, want := fmt.Sprintf("%x", payloadHash), "d6116d4ec05ca1a0e38b138c09c37ddb80122a021b292e72fcc3427ef08a004c"; got != want {
		t.Fatalf("payload hash=%q want=%q", got, want)
	}
	reversed, reversedHash, err := PairMutationIdentity(
		PairMutationIdentityRedeemShare, 10001, 20002, "share-token", 0,
	)
	if err != nil || reversed != operationID || !bytes.Equal(reversedHash, payloadHash) {
		t.Fatalf("reversed identity=%q hash=%x err=%v", reversed, reversedHash, err)
	}
}

func TestCommitIdentityKeyKinds(t *testing.T) {
	pub := CommitIdentity{SubjectPlayerID: 10001, CommandType: "DELETE_FRIEND", OperationID: "op-1"}
	part := CommitIdentity{ParticipantInvocationID: "inv-1", ParticipantStep: "RESERVE_FRIEND_SLOT:10001"}
	if !pub.Valid() || !part.Valid() {
		t.Fatal("identities should be valid")
	}
	if pub.Key() == part.Key() {
		t.Fatal("public and participant identities must never collide")
	}
	both := CommitIdentity{SubjectPlayerID: 1, CommandType: "X", OperationID: "op", ParticipantInvocationID: "inv", ParticipantStep: "s"}
	if both.Valid() {
		t.Fatal("identity carrying both kinds must be invalid")
	}
	if (CommitIdentity{}).Valid() {
		t.Fatal("empty identity must be invalid")
	}
}
