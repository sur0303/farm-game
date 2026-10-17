package mail

import (
	"encoding/hex"
	"testing"
)

// Golden vectors pin the contract-35/35A identity, hash, codec, sort member
// and due-ref bytes. Any intentional change requires a new schema/layout
// version, not an edit of these values.

func goldenEntry(t *testing.T) MailEntry {
	t.Helper()
	entry, err := BuildMailEntry(DeliveryParams{
		RecipientPlayerID:   10001,
		DeliveryID:          "WELCOME:10001:1",
		SourceType:          SourceTypeSystem,
		SourceID:            "welcome-v1",
		TitleSnapshot:       "Welcome!",
		BodySnapshot:        "Hello farmer",
		Reward:              RewardBundle{Coin: 100, Exp: 5, Items: []ItemGrant{{ItemID: 7, Count: 3}}},
		TemplateVersion:     "tpl-v1",
		RewardConfigVersion: "rcfg-v1",
		CreatedAtMs:         1754200000000,
		VisibleAtMs:         1754200000000,
	}, LimitsV1())
	if err != nil {
		t.Fatalf("build golden entry: %v", err)
	}
	return entry
}

func TestGoldenStableIdentities(t *testing.T) {
	entry := goldenEntry(t)
	if want := "sid1_OZjDTRVsi8KuAvp8_hF_0ng7zPWzlCV2CtRUwW6p6zw"; entry.MailID != want {
		t.Fatalf("mail id: got %s want %s", entry.MailID, want)
	}
	if want := "1434f173e1604b1263d8c27387112891208799716da24731f82a0473e96d0f47"; entry.DeliveryPayloadHash != want {
		t.Fatalf("delivery hash: got %s want %s", entry.DeliveryPayloadHash, want)
	}
	if want := "b13b31b9fbc41ce37b2d246b89bdfb8a98fd10843bba1fdf7a5a67b3b37474ee"; entry.ClaimMaterialHash != want {
		t.Fatalf("claim hash: got %s want %s", entry.ClaimMaterialHash, want)
	}
	dd, err := DeliveryDigest(10001, "WELCOME:10001:1")
	if err != nil {
		t.Fatal(err)
	}
	if want := "f280632a2fdca4ac2f1aecaf0bc09218b893118940fd7f0115ab0b74912d010f"; dd != want {
		t.Fatalf("delivery digest: got %s want %s", dd, want)
	}
	md, err := MutationDigest(10001, entry.MailID, MutationKindMarkRead, "mut-0001")
	if err != nil {
		t.Fatal(err)
	}
	if want := "7de897a0cc656e4df373c2789fe30e6aa62333dbb90e3da2e1eae11218d20082"; md != want {
		t.Fatalf("mutation digest: got %s want %s", md, want)
	}
	pd, err := ProjectionDigest(10001, "work-0001", 3)
	if err != nil {
		t.Fatal(err)
	}
	if want := "f587bd895e50496910732f3bfd34a18345e27f9e24f6f86a6bd491d1efae9fa6"; pd != want {
		t.Fatalf("projection digest: got %s want %s", pd, want)
	}
}

func TestGoldenTimes(t *testing.T) {
	entry := goldenEntry(t)
	if want := int64(1756792000000); entry.ExpireAtMs != want {
		t.Fatalf("expire: got %d want %d", entry.ExpireAtMs, want)
	}
	if entry.ClaimEndAtMs != entry.ExpireAtMs {
		t.Fatalf("P0 claim_end must equal expire_at: %d != %d", entry.ClaimEndAtMs, entry.ExpireAtMs)
	}
	if want := int64(1759988800000); entry.PhysicalPurgeAtMs != want {
		t.Fatalf("purge: got %d want %d", entry.PhysicalPurgeAtMs, want)
	}
}

func TestGoldenTextMail(t *testing.T) {
	entry, err := BuildMailEntry(DeliveryParams{
		RecipientPlayerID: 20002, DeliveryID: "NOTICE:1", SourceType: SourceTypeOperation,
		TitleSnapshot: "t", BodySnapshot: "b", CreatedAtMs: 1754200001000, VisibleAtMs: 1754200001000,
	}, LimitsV1())
	if err != nil {
		t.Fatal(err)
	}
	if want := "sid1_qt55TWpphlJC8ve3XzHe9GmU3f-sgjcSDi8ZxdiX36Q"; entry.MailID != want {
		t.Fatalf("text mail id: got %s want %s", entry.MailID, want)
	}
	if want := "f3c14596bb1a6a5017a12a7716b22ba03619a71811de7a0b7710a7dfb74fecd4"; entry.DeliveryPayloadHash != want {
		t.Fatalf("text delivery hash: got %s want %s", entry.DeliveryPayloadHash, want)
	}
	if entry.ClaimMaterialHash != "" {
		t.Fatalf("text mail must carry no claim hash, got %s", entry.ClaimMaterialHash)
	}
}

func TestGoldenSortMember(t *testing.T) {
	entry := goldenEntry(t)
	member, err := EncodeSortMember(entry.CreatedAtMs, entry.MailID)
	if err != nil {
		t.Fatal(err)
	}
	wantHex := "fffffe67918839ff00736964315f4f5a6a445452567369384b75417670385f68465f306e67377a50577a6c4356324374525577573670367a77"
	if got := hex.EncodeToString(member); got != wantHex {
		t.Fatalf("sort member: got %s want %s", got, wantHex)
	}
	cursor, err := EncodeCursor(member)
	if err != nil {
		t.Fatal(err)
	}
	if want := "___-Z5GIOf8Ac2lkMV9PWmpEVFJWc2k4S3VBdnA4X2hGXzBuZzd6UFd6bENWMkN0UlV3VzZwNnp3"; cursor != want {
		t.Fatalf("cursor: got %s want %s", cursor, want)
	}
	roundTrip, err := DecodeCursor(cursor)
	if err != nil {
		t.Fatal(err)
	}
	if string(roundTrip) != string(member) {
		t.Fatalf("cursor round trip mismatch")
	}
	ts, mailID, err := DecodeSortMember(member)
	if err != nil {
		t.Fatal(err)
	}
	if ts != entry.CreatedAtMs || mailID != entry.MailID {
		t.Fatalf("decode: got (%d,%s)", ts, mailID)
	}
}

func TestGoldenDueRef(t *testing.T) {
	entry := goldenEntry(t)
	ref, err := EncodeDueRefV1(MailDueRefV1{
		Kind: DueKindExpireVisibility, PlayerID: 10001, MailID: entry.MailID,
		MailVersion: 1, ExpectedStateVersion: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := "mdr1_AAAAEUVYUElSRV9WSVNJQklMSVRZAAAABTEwMDAxAAAAMHNpZDFfT1pqRFRSVnNpOEt1QXZwOF9oRl8wbmc3elBXemxDVjJDdFJVd1c2cDZ6dwAAAAExAAAAATE"
	if ref != want {
		t.Fatalf("due ref: got %s want %s", ref, want)
	}
	decoded, err := DecodeDueRefV1(ref)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Kind != DueKindExpireVisibility || decoded.PlayerID != 10001 ||
		decoded.MailID != entry.MailID || decoded.MailVersion != 1 || decoded.ExpectedStateVersion != 1 {
		t.Fatalf("due ref round trip: %+v", decoded)
	}
}

func TestGoldenEntryBlob(t *testing.T) {
	entry := goldenEntry(t)
	blob, err := EncodeMailEntry(entry, LimitsV1())
	if err != nil {
		t.Fatal(err)
	}
	if len(blob) != 458 {
		t.Fatalf("entry blob length: got %d want 458", len(blob))
	}
	decoded, err := DecodeMailEntry(blob)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.MailID != entry.MailID || decoded.DeliveryPayloadHash != entry.DeliveryPayloadHash ||
		decoded.ClaimMaterialHash != entry.ClaimMaterialHash || decoded.CreatedAtMs != entry.CreatedAtMs ||
		decoded.Reward.Coin != 100 || decoded.Reward.Exp != 5 || len(decoded.Reward.Items) != 1 ||
		decoded.Reward.Items[0].ItemID != 7 || decoded.Reward.Items[0].Count != 3 {
		t.Fatalf("entry round trip mismatch: %+v", decoded)
	}
}

func TestHashDutySeparation(t *testing.T) {
	entry := goldenEntry(t)
	// Changing reward content must change the claim hash and (through it)
	// the delivery hash, but never the mail id (35 M5).
	changed := entry
	changed.Reward.Coin = 101
	newClaim, err := ClaimMaterialHash(changed)
	if err != nil {
		t.Fatal(err)
	}
	if newClaim == entry.ClaimMaterialHash {
		t.Fatal("claim hash must react to reward content")
	}
	changed.ClaimMaterialHash = newClaim
	newDelivery, err := DeliveryPayloadHash(changed)
	if err != nil {
		t.Fatal(err)
	}
	if newDelivery == entry.DeliveryPayloadHash {
		t.Fatal("delivery hash must react to claim hash")
	}
	newID, err := MailID(entry.RecipientPlayerID, entry.DeliveryID)
	if err != nil {
		t.Fatal(err)
	}
	if newID != entry.MailID {
		t.Fatal("mail id must not depend on payload content")
	}
	// physical_purge_at_ms never enters the claim hash (35 §2.3).
	purged := entry
	purged.PhysicalPurgeAtMs += 1000
	h1, err := ClaimMaterialHash(purged)
	if err != nil {
		t.Fatal(err)
	}
	if h1 != entry.ClaimMaterialHash {
		t.Fatal("purge time must not enter claim hash")
	}
	d1, err := DeliveryPayloadHash(purged)
	if err != nil {
		t.Fatal(err)
	}
	if d1 != entry.DeliveryPayloadHash {
		t.Fatal("purge time must not enter delivery hash")
	}
}
