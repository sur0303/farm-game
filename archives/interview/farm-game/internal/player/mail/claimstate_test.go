package mail

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	playerpb "farmgame/api/gen/player"
)

func testClaimMailID(t *testing.T, deliveryID string) string {
	t.Helper()
	mailID, err := MailID(10001, deliveryID)
	if err != nil {
		t.Fatalf("mail id: %v", err)
	}
	return mailID
}

func testClaimReceipt(mailID, operationID string) ClaimReceipt {
	return ClaimReceipt{
		MailID:               mailID,
		FirstOperationID:     operationID,
		MailVersion:          1,
		ClaimMaterialHash:    bytes.Repeat([]byte{0x42}, sha256.Size),
		RewardResult:         []byte{0x01, 0x02},
		ClaimDecisionAtMs:    100,
		ClaimedAtMs:          100,
		ReceiptRetainUntilMs: 200,
	}
}

func testClaimReceiptProto(mailID, operationID string) *playerpb.MailClaimReceipt {
	r := testClaimReceipt(mailID, operationID)
	return &playerpb.MailClaimReceipt{
		MailId:               r.MailID,
		FirstOperationId:     r.FirstOperationID,
		MailVersion:          r.MailVersion,
		ClaimMaterialHash:    append([]byte(nil), r.ClaimMaterialHash...),
		RewardResult:         append([]byte(nil), r.RewardResult...),
		ClaimDecisionAtMs:    r.ClaimDecisionAtMs,
		ClaimedAtMs:          r.ClaimedAtMs,
		ReceiptRetainUntilMs: r.ReceiptRetainUntilMs,
	}
}

func TestClaimStateRoundTripSortAndLookup(t *testing.T) {
	ids := []string{
		testClaimMailID(t, "CLAIM:10001:1"),
		testClaimMailID(t, "CLAIM:10001:2"),
	}
	sort.Strings(ids)
	record := &playerpb.PlayerRecord{
		MailClaimState: &playerpb.MailClaimState{
			StateVersion:              7,
			ProjectionDirtyGeneration: 3,
			Receipts: []*playerpb.MailClaimReceipt{
				testClaimReceiptProto(ids[1], "op-second"),
				testClaimReceiptProto(ids[0], "op-first"),
			},
		},
	}

	got, err := StateFromPlayerRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	want := ClaimState{
		StateVersion:              7,
		ProjectionDirtyGeneration: 3,
		Receipts: []ClaimReceipt{
			testClaimReceipt(ids[0], "op-first"),
			testClaimReceipt(ids[1], "op-second"),
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded state: got %+v want %+v", got, want)
	}
	if got.Receipts[0].MailID >= got.Receipts[1].MailID {
		t.Fatalf("receipts are not sorted: %+v", got.Receipts)
	}

	hashBefore := append([]byte(nil), got.Receipts[0].ClaimMaterialHash...)
	record.MailClaimState.Receipts[0].ClaimMaterialHash[0] ^= 0xff
	if !bytes.Equal(got.Receipts[0].ClaimMaterialHash, hashBefore) {
		t.Fatal("adapter must deep-copy claim material hash")
	}

	roundTripRecord := &playerpb.PlayerRecord{}
	if err := ApplyStateToPlayerRecord(roundTripRecord, got); err != nil {
		t.Fatal(err)
	}
	roundTrip, err := StateFromPlayerRecord(roundTripRecord)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(roundTrip, got) {
		t.Fatalf("round trip: got %+v want %+v", roundTrip, got)
	}

	empty, err := StateFromPlayerRecord(&playerpb.PlayerRecord{})
	if err != nil {
		t.Fatal(err)
	}
	if empty.StateVersion != 0 || empty.ProjectionDirtyGeneration != 0 || len(empty.Receipts) != 0 {
		t.Fatalf("empty state: %+v", empty)
	}
	if got := LookupClaimStatus(&empty, ids[0]); got != ClaimStatusUnclaimed {
		t.Fatalf("empty lookup: got %s", got)
	}
	if got := LookupClaimStatus(nil, ids[0]); got != ClaimStatusUnknown {
		t.Fatalf("nil lookup: got %s", got)
	}
	if got := got.ClaimStatus(ids[0]); got != ClaimStatusClaimed {
		t.Fatalf("claimed lookup: got %s", got)
	}
	if got := ClaimStatusForMail(got, testClaimMailID(t, "CLAIM:10001:missing")); got != ClaimStatusUnclaimed {
		t.Fatalf("unclaimed lookup: got %s", got)
	}
	if _, ok := got.ReceiptFor(ids[0]); !ok {
		t.Fatal("receipt lookup should find claimed mail")
	}
}

func TestClaimStateRejectsDuplicateMailIDAcrossOperations(t *testing.T) {
	mailID := testClaimMailID(t, "CLAIM:10001:duplicate")
	record := &playerpb.PlayerRecord{
		MailClaimState: &playerpb.MailClaimState{
			StateVersion: 1,
			Receipts: []*playerpb.MailClaimReceipt{
				testClaimReceiptProto(mailID, "operation-a"),
				testClaimReceiptProto(mailID, "operation-b"),
			},
		},
	}
	if _, err := StateFromPlayerRecord(record); err == nil || !errors.Is(err, ErrInvalidClaimState) {
		t.Fatalf("same mail with different operations must fail closed, got %v", err)
	}
}

func TestRecordClaimAdvancesOnceAndKeepsCanonicalOrder(t *testing.T) {
	ids := []string{
		testClaimMailID(t, "CLAIM:10001:record-b"),
		testClaimMailID(t, "CLAIM:10001:record-a"),
	}
	sort.Strings(ids)
	state, changed, err := (ClaimState{}).RecordClaim(testClaimReceipt(ids[1], "operation-b"))
	if err != nil || !changed || state.StateVersion != 1 || state.ProjectionDirtyGeneration != 1 {
		t.Fatalf("first record state=%+v changed=%v err=%v", state, changed, err)
	}
	state, changed, err = state.RecordClaim(testClaimReceipt(ids[0], "operation-a"))
	if err != nil || !changed || state.StateVersion != 2 || state.ProjectionDirtyGeneration != 2 {
		t.Fatalf("second record state=%+v changed=%v err=%v", state, changed, err)
	}
	if len(state.Receipts) != 2 || state.Receipts[0].MailID != ids[0] || state.Receipts[1].MailID != ids[1] {
		t.Fatalf("receipts are not canonical: %+v", state.Receipts)
	}
	replayed, changed, err := state.RecordClaim(testClaimReceipt(ids[0], "different-operation"))
	if err != nil || changed || !reflect.DeepEqual(replayed, state) {
		t.Fatalf("duplicate changed authority: state=%+v changed=%v err=%v", replayed, changed, err)
	}
}

func TestClaimStateRejectsCapacityOverflow(t *testing.T) {
	receipts := make([]*playerpb.MailClaimReceipt, MaxClaimReceipts+1)
	for i := range receipts {
		deliveryID := fmt.Sprintf("CLAIM:10001:%d", i)
		receipts[i] = testClaimReceiptProto(testClaimMailID(t, deliveryID), fmt.Sprintf("operation-%d", i))
	}
	record := &playerpb.PlayerRecord{
		MailClaimState: &playerpb.MailClaimState{
			StateVersion: 1,
			Receipts:     receipts,
		},
	}
	if _, err := StateFromPlayerRecord(record); err == nil || !errors.Is(err, ErrInvalidClaimState) {
		t.Fatalf("capacity overflow must fail closed, got %v", err)
	}
}

func TestClaimStateRejectsMalformedReceiptFields(t *testing.T) {
	mailID := testClaimMailID(t, "CLAIM:10001:malformed")
	tests := []struct {
		name   string
		mutate func(*playerpb.MailClaimReceipt)
	}{
		{name: "mail id", mutate: func(r *playerpb.MailClaimReceipt) { r.MailId = "" }},
		{name: "operation empty", mutate: func(r *playerpb.MailClaimReceipt) { r.FirstOperationId = "" }},
		{name: "operation too long", mutate: func(r *playerpb.MailClaimReceipt) { r.FirstOperationId = strings.Repeat("x", 65) }},
		{name: "version zero", mutate: func(r *playerpb.MailClaimReceipt) { r.MailVersion = 0 }},
		{name: "hash short", mutate: func(r *playerpb.MailClaimReceipt) { r.ClaimMaterialHash = []byte{1} }},
		{name: "hash hex is not raw", mutate: func(r *playerpb.MailClaimReceipt) { r.ClaimMaterialHash = []byte(strings.Repeat("a", 64)) }},
		{name: "result empty", mutate: func(r *playerpb.MailClaimReceipt) { r.RewardResult = nil }},
		{name: "result too large", mutate: func(r *playerpb.MailClaimReceipt) {
			r.RewardResult = bytes.Repeat([]byte{1}, MaxClaimRewardResultBytes+1)
		}},
		{name: "decision time zero", mutate: func(r *playerpb.MailClaimReceipt) { r.ClaimDecisionAtMs = 0 }},
		{name: "claimed before decision", mutate: func(r *playerpb.MailClaimReceipt) { r.ClaimedAtMs = 99 }},
		{name: "retain before claimed", mutate: func(r *playerpb.MailClaimReceipt) { r.ReceiptRetainUntilMs = 100 }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			receipt := testClaimReceiptProto(mailID, "operation-valid")
			tc.mutate(receipt)
			record := &playerpb.PlayerRecord{
				MailClaimState: &playerpb.MailClaimState{
					StateVersion: 1,
					Receipts:     []*playerpb.MailClaimReceipt{receipt},
				},
			}
			if _, err := StateFromPlayerRecord(record); err == nil || !errors.Is(err, ErrInvalidClaimState) {
				t.Fatalf("malformed receipt must fail closed, got %v", err)
			}
		})
	}
}

func TestClaimStateValidateRequiresStrictDomainOrder(t *testing.T) {
	ids := []string{
		testClaimMailID(t, "CLAIM:10001:order-a"),
		testClaimMailID(t, "CLAIM:10001:order-b"),
	}
	sort.Strings(ids)
	state := ClaimState{
		StateVersion: 1,
		Receipts: []ClaimReceipt{
			testClaimReceipt(ids[1], "operation-b"),
			testClaimReceipt(ids[0], "operation-a"),
		},
	}
	if err := state.Validate(); err == nil {
		t.Fatal("domain state with non-canonical receipt order must fail")
	}
}

func mustIssueTestClaimMaterial(t *testing.T, nowMs, ttlMs, retentionMs int64) (MailEntry, TrustedMailClaimMaterial) {
	t.Helper()
	params := rewardDelivery(10001, 77, nowMs)
	params.SourceID = "mail-claim-test"
	params.RewardConfigVersion = "reward-config-v1"
	params.RetentionMs = retentionMs
	entry := mustBuild(t, params)
	material, err := (ClaimMaterialIssuer{
		Issuer:        ClaimMaterialIssuerIngress,
		MaterialTTLMs: ttlMs,
	}).Issue(entry, nowMs)
	if err != nil {
		t.Fatalf("issue claim material: %v", err)
	}
	return entry, material
}

func TestTrustedMailClaimMaterialRejectsTamperedBindings(t *testing.T) {
	nowMs := testNow
	_, original := mustIssueTestClaimMaterial(t, nowMs, 60000, 0)
	if err := original.ValidateForTurn(10001, nowMs+1); err != nil {
		t.Fatal(err)
	}

	otherMailID := testClaimMailID(t, "SYS:10001:78")
	tests := []struct {
		name   string
		mutate func(*TrustedMailClaimMaterial)
	}{
		{name: "schema", mutate: func(m *TrustedMailClaimMaterial) { m.SchemaVersion = "TrustedMailClaimMaterialV0" }},
		{name: "mail id", mutate: func(m *TrustedMailClaimMaterial) { m.MailID = otherMailID }},
		{name: "mail version", mutate: func(m *TrustedMailClaimMaterial) { m.MailVersion++ }},
		{name: "recipient", mutate: func(m *TrustedMailClaimMaterial) { m.RecipientPlayerID = 20002 }},
		{name: "source type", mutate: func(m *TrustedMailClaimMaterial) { m.SourceType = SourceTypeOperation }},
		{name: "source id", mutate: func(m *TrustedMailClaimMaterial) { m.SourceID = "other-source" }},
		{name: "delivery id", mutate: func(m *TrustedMailClaimMaterial) { m.DeliveryID = "SYS:10001:78" }},
		{name: "reward", mutate: func(m *TrustedMailClaimMaterial) { m.Reward.Coin++ }},
		{name: "reward config version", mutate: func(m *TrustedMailClaimMaterial) { m.RewardConfigVersion = "reward-config-v2" }},
		{name: "claim material hash", mutate: func(m *TrustedMailClaimMaterial) { m.ClaimMaterialHash = strings.Repeat("0", 64) }},
		{name: "visible at", mutate: func(m *TrustedMailClaimMaterial) { m.VisibleAtMs++ }},
		{name: "claim end", mutate: func(m *TrustedMailClaimMaterial) { m.ClaimEndAtMs++ }},
		{name: "issued at", mutate: func(m *TrustedMailClaimMaterial) { m.IssuedAtMs++ }},
		{name: "material expire", mutate: func(m *TrustedMailClaimMaterial) { m.MaterialExpireAtMs++ }},
		{name: "issuer", mutate: func(m *TrustedMailClaimMaterial) { m.Issuer = ClaimMaterialIssuerCoordinator }},
		{name: "audience", mutate: func(m *TrustedMailClaimMaterial) { m.Audience = "OTHER_AUTHORITY" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			material := original
			tc.mutate(&material)
			if err := material.ValidateForTurn(10001, nowMs+1); err == nil {
				t.Fatalf("tampered %s must fail", tc.name)
			}
		})
	}
}

func TestTrustedMailClaimMaterialIssuerAllowlist(t *testing.T) {
	nowMs := testNow
	entry, original := mustIssueTestClaimMaterial(t, nowMs, 60000, 0)
	for _, issuer := range []string{"", "player-svr/unknown", "client"} {
		if IsAllowedClaimMaterialIssuer(issuer) {
			t.Fatalf("issuer %q must not be allowlisted", issuer)
		}
		if err := ValidateClaimMaterialIssuer(issuer); err == nil {
			t.Fatalf("issuer %q must fail validation", issuer)
		}
		if _, err := (ClaimMaterialIssuer{Issuer: issuer}).Issue(entry, nowMs); err == nil {
			t.Fatalf("issuer %q must not issue material", issuer)
		}
	}
	if !IsAllowedClaimMaterialIssuer(ClaimMaterialIssuerCoordinator) {
		t.Fatal("coordinator issuer should be explicitly allowlisted")
	}
	coordinator, err := (ClaimMaterialIssuer{
		Issuer: ClaimMaterialIssuerCoordinator,
	}).Issue(entry, nowMs)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.ValidateForTurn(10001, nowMs+1); err != nil {
		t.Fatal(err)
	}
	original.Issuer = "player-svr/unknown"
	if err := original.ValidateForTurn(10001, nowMs+1); err == nil {
		t.Fatal("ValidateForTurn must reject arbitrary issuer")
	}
}

func TestTrustedMailClaimMaterialTurnWindowEdges(t *testing.T) {
	nowMs := testNow
	entry, material := mustIssueTestClaimMaterial(t, nowMs, maxMaterialTTLMs, 1000)
	if err := material.ValidateForTurn(10001, entry.ClaimEndAtMs-1); err != nil {
		t.Fatalf("claim window before end should pass: %v", err)
	}
	if err := material.ValidateForTurn(10001, entry.ClaimEndAtMs); err == nil {
		t.Fatal("now == claim_end must be rejected")
	}

	_, expiring := mustIssueTestClaimMaterial(t, nowMs, 10, 0)
	if err := expiring.ValidateForTurn(10001, expiring.MaterialExpireAtMs-1); err != nil {
		t.Fatalf("material before expiry should pass: %v", err)
	}
	if err := expiring.ValidateForTurn(10001, expiring.MaterialExpireAtMs); err == nil {
		t.Fatal("now == material_expire must be rejected")
	}
}
