package mail

import (
	"strings"
	"testing"
)

func TestSortMemberOrdering(t *testing.T) {
	idA, err := MailID(1, "D:1")
	if err != nil {
		t.Fatal(err)
	}
	idB, err := MailID(1, "D:2")
	if err != nil {
		t.Fatal(err)
	}
	newer, err := EncodeSortMember(2000, idA)
	if err != nil {
		t.Fatal(err)
	}
	older, err := EncodeSortMember(1000, idB)
	if err != nil {
		t.Fatal(err)
	}
	if string(newer) >= string(older) {
		t.Fatal("newer mail must sort before older under lex ascending")
	}
	// Same millisecond: mail id breaks the tie deterministically.
	a, err := EncodeSortMember(1000, idA)
	if err != nil {
		t.Fatal(err)
	}
	b, err := EncodeSortMember(1000, idB)
	if err != nil {
		t.Fatal(err)
	}
	if string(a) == string(b) {
		t.Fatal("same-ms members must differ by mail id")
	}
	if (string(a) < string(b)) != (idA < idB) {
		t.Fatal("same-ms order must follow mail id order")
	}
	// Max time bound is encodable.
	if _, err := EncodeSortMember(1<<62, idA); err != nil {
		t.Fatalf("large time: %v", err)
	}
	if _, err := EncodeSortMember(0, idA); err == nil {
		t.Fatal("zero time must be rejected")
	}
	if _, err := EncodeSortMember(1000, "not-a-mail-id"); err == nil {
		t.Fatal("invalid mail id must be rejected")
	}
}

func TestCursorRejectsTampering(t *testing.T) {
	entry := goldenEntry(t)
	member, err := EncodeSortMember(entry.CreatedAtMs, entry.MailID)
	if err != nil {
		t.Fatal(err)
	}
	cursor, err := EncodeCursor(member)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeCursor(cursor); err != nil {
		t.Fatal(err)
	}
	flipped := []byte(cursor)
	if flipped[12] == 'A' {
		flipped[12] = 'B'
	} else {
		flipped[12] = 'A'
	}
	for _, bad := range []string{"", "!!!", cursor + "A", cursor[:len(cursor)-2], string(flipped)} {
		if _, err := DecodeCursor(bad); err == nil {
			t.Fatalf("cursor %q must be rejected", bad)
		}
	}
}

func TestDueRefValidation(t *testing.T) {
	entry := goldenEntry(t)
	if _, err := EncodeDueRefV1(MailDueRefV1{Kind: "BOGUS", PlayerID: 1, MailID: entry.MailID, MailVersion: 1, ExpectedStateVersion: 1}); err == nil {
		t.Fatal("bad kind must be rejected")
	}
	if _, err := DecodeDueRefV1("wr1_AAAAC0NBUkVfUkVXQVJE"); err == nil {
		t.Fatal("foreign ref prefix must be rejected")
	}
	ref, err := EncodeDueRefV1(MailDueRefV1{Kind: DueKindPhysicalGC, PlayerID: 1, MailID: entry.MailID, MailVersion: 2, ExpectedStateVersion: 3})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeDueRefV1(ref[:len(ref)-1] + "A"); err == nil {
		t.Fatal("non-canonical ref must be rejected")
	}
}

func TestEntryCodecBounds(t *testing.T) {
	l := LimitsV1()
	base := DeliveryParams{
		RecipientPlayerID: 1, DeliveryID: "D:1", SourceType: SourceTypeSystem,
		TitleSnapshot: "t", BodySnapshot: "b", CreatedAtMs: 1000, VisibleAtMs: 1000,
	}
	tooLongTitle := base
	tooLongTitle.TitleSnapshot = strings.Repeat("x", l.MaxTitleBytes+1)
	if _, err := BuildMailEntry(tooLongTitle, l); err == nil {
		t.Fatal("oversized title must be rejected at encode time")
	} else if _, err2 := func() ([]byte, error) {
		e, eerr := BuildMailEntry(base, l)
		if eerr != nil {
			return nil, eerr
		}
		e.TitleSnapshot = tooLongTitle.TitleSnapshot
		return EncodeMailEntry(e, l)
	}(); err2 == nil {
		t.Fatal("EncodeMailEntry must enforce title bound")
	}
	tooManyKinds := base
	tooManyKinds.Reward = RewardBundle{Coin: 1, Exp: 1, Items: []ItemGrant{
		{ItemID: 1, Count: 1}, {ItemID: 2, Count: 1}, {ItemID: 3, Count: 1},
		{ItemID: 4, Count: 1}, {ItemID: 5, Count: 1}, {ItemID: 6, Count: 1},
		{ItemID: 7, Count: 1},
	}}
	if _, err := BuildMailEntry(tooManyKinds, l); err == nil {
		t.Fatal("more than 8 reward kinds must be rejected")
	}
	negative := base
	negative.Reward = RewardBundle{Coin: -1}
	if _, err := BuildMailEntry(negative, l); err == nil {
		t.Fatal("negative grant must be rejected")
	}
}

func TestStateAndDedupRoundTrip(t *testing.T) {
	entry := goldenEntry(t)
	l := LimitsV1()
	state := NewUserState(entry)
	state.ReadAtMs = 123
	state.StateVersion = 4
	blob, err := EncodeMailUserState(state, l)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeMailUserState(blob)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.MailID != state.MailID || decoded.ReadAtMs != 123 || decoded.StateVersion != 4 ||
		decoded.ClaimMaterialHash != entry.ClaimMaterialHash || decoded.ClaimedProjection {
		t.Fatalf("state round trip: %+v", decoded)
	}
	dedup := NewDeliveryDedup(entry, l, 1754200000000)
	dedup.Tombstone = true
	dblob, err := EncodeDeliveryDedup(dedup, l)
	if err != nil {
		t.Fatal(err)
	}
	ddecoded, err := DecodeDeliveryDedup(dblob)
	if err != nil {
		t.Fatal(err)
	}
	if ddecoded.DeliveryID != entry.DeliveryID || !ddecoded.Tombstone ||
		ddecoded.DeliveryPayloadHash != entry.DeliveryPayloadHash {
		t.Fatalf("dedup round trip: %+v", ddecoded)
	}
}

func TestReceiptCodecs(t *testing.T) {
	entry := goldenEntry(t)
	l := LimitsV1()
	mr := MutationReceipt{
		Kind: MutationKindDelete, MailID: entry.MailID, MutationID: "m-1",
		ResultStatus: string(MutationDeleted), AppliedAtMs: 5, MailboxRevision: 9, RetainUntilMs: 100,
	}
	blob, err := EncodeMutationReceipt(mr, l)
	if err != nil {
		t.Fatal(err)
	}
	if decoded, err := DecodeMutationReceipt(blob); err != nil || decoded != mr {
		t.Fatalf("mutation receipt round trip: %+v %v", decoded, err)
	}
	pr := ProjectionReceipt{
		WorkID: "w-1", WorkVersion: 2, MailID: entry.MailID,
		ClaimMaterialHash: entry.ClaimMaterialHash, ResultStatus: string(ProjectionApplied),
		AppliedAtMs: 7, RetainUntilMs: 100,
	}
	pblob, err := EncodeProjectionReceipt(pr, l)
	if err != nil {
		t.Fatal(err)
	}
	if decoded, err := DecodeProjectionReceipt(pblob); err != nil || decoded != pr {
		t.Fatalf("projection receipt round trip: %+v %v", decoded, err)
	}
}

func TestBuildMailEntryValidation(t *testing.T) {
	l := LimitsV1()
	good := DeliveryParams{
		RecipientPlayerID: 1, DeliveryID: "D:1", SourceType: SourceTypeSystem,
		TitleSnapshot: "t", CreatedAtMs: 1000, VisibleAtMs: 1000,
	}
	if _, err := BuildMailEntry(good, l); err != nil {
		t.Fatalf("minimal text mail: %v", err)
	}
	bad := good
	bad.RecipientPlayerID = 0
	if _, err := BuildMailEntry(bad, l); err == nil {
		t.Fatal("recipient 0 must be rejected")
	}
	bad = good
	bad.DeliveryID = ""
	if _, err := BuildMailEntry(bad, l); err == nil {
		t.Fatal("empty delivery id must be rejected")
	}
	bad = good
	bad.RetentionMs = l.MaxRetentionMs + 1
	if _, err := BuildMailEntry(bad, l); err == nil {
		t.Fatal("retention above max must be rejected")
	}
}
