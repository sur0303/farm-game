package playermail

import (
	"strings"
	"testing"

	"farmgame/internal/player/mail"
	"farmgame/internal/storage/redis/rediskey"
)

func testNamespace(t *testing.T) rediskey.Namespace {
	t.Helper()
	ns, err := rediskey.NewNamespace("test", "z1")
	if err != nil {
		t.Fatal(err)
	}
	return ns
}

func testKeyBuilder(t *testing.T) KeyBuilderV1 {
	t.Helper()
	kb, err := NewKeyBuilderV1(Profile{Namespace: testNamespace(t), BucketCount: DefaultBucketCount})
	if err != nil {
		t.Fatal(err)
	}
	return kb
}

// TestGoldenBuckets pins the XXH64(seed=0) + LP bucket mapping (35A §2.1).
// These values are layout-frozen: changing them is a data migration.
func TestGoldenBuckets(t *testing.T) {
	kb := testKeyBuilder(t)
	cases := map[uint64]uint32{
		1:                    334,
		10001:                2794,
		20002:                2917,
		30000000:             1516,
		18446744073709551615: 1503,
	}
	for player, want := range cases {
		got, err := kb.BucketOf(player)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("player %d: got bucket %d want %d", player, got, want)
		}
	}
	if _, err := kb.BucketOf(0); err == nil {
		t.Fatal("player 0 must be rejected")
	}
}

func TestGoldenKeys(t *testing.T) {
	kb := testKeyBuilder(t)
	bucket, err := kb.BucketOf(10001)
	if err != nil {
		t.Fatal(err)
	}
	tag, err := kb.HashTag(bucket)
	if err != nil {
		t.Fatal(err)
	}
	if want := "classic-farm:test:z1:player-mail:v1:b:2794"; tag != want {
		t.Fatalf("tag: got %s want %s", tag, want)
	}
	box, _ := kb.Mailbox(bucket, 10001)
	if want := "{classic-farm:test:z1:player-mail:v1:b:2794}:box:10001"; box.String() != want {
		t.Fatalf("box: got %s want %s", box, want)
	}
	index, _ := kb.VisibleIndex(bucket, 10001)
	if want := "{classic-farm:test:z1:player-mail:v1:b:2794}:index:10001"; index.String() != want {
		t.Fatalf("index: got %s", index)
	}
	safe, _ := kb.SafeEvictIndex(bucket, 10001)
	if want := "{classic-farm:test:z1:player-mail:v1:b:2794}:safe-evict:10001"; safe.String() != want {
		t.Fatalf("safe-evict: got %s", safe)
	}
	expire, _ := kb.ExpireDueIndex(bucket)
	gc, _ := kb.GcDueIndex(bucket)
	meta, _ := kb.BucketMeta(bucket)
	lease, _ := kb.ScanLease(bucket)
	for key, suffix := range map[rediskey.RedisKey]string{
		expire: ":expire-due",
		gc:     ":gc-due",
		meta:   ":bucket-meta",
		lease:  ":scan-lease",
	} {
		if !strings.HasSuffix(key.String(), suffix) {
			t.Fatalf("key %s missing suffix %s", key, suffix)
		}
	}
	// Every key of one Lua call must share the identical hash tag (35A PM1).
	if err := kb.AssertSameSlot(box, index, safe, expire, gc, meta, lease); err != nil {
		t.Fatalf("same-slot assertion: %v", err)
	}
	otherBucket := (bucket + 1) % DefaultBucketCount
	otherExpire, _ := kb.ExpireDueIndex(otherBucket)
	if err := kb.AssertSameSlot(box, otherExpire); err == nil {
		t.Fatal("cross-bucket keys must fail the slot assertion")
	}
}

func TestKeyBuilderRejectsForeignOwner(t *testing.T) {
	kb := testKeyBuilder(t)
	right, err := kb.BucketOf(10001)
	if err != nil {
		t.Fatal(err)
	}
	wrong := (right + 1) % DefaultBucketCount
	if _, err := kb.Mailbox(wrong, 10001); err == nil {
		t.Fatal("mailbox key with a foreign bucket must be rejected")
	}
	if _, err := kb.VisibleIndex(wrong, 10001); err == nil {
		t.Fatal("index key with a foreign bucket must be rejected")
	}
	if _, err := kb.HashTag(DefaultBucketCount); err == nil {
		t.Fatal("out-of-range bucket must be rejected")
	}
}

func TestFieldBuilder(t *testing.T) {
	var fb FieldBuilderV1
	mailID, err := mail.MailID(10001, "WELCOME:10001:1")
	if err != nil {
		t.Fatal(err)
	}
	entry, err := fb.EntryField(mailID)
	if err != nil {
		t.Fatal(err)
	}
	if entry != "entry:"+mailID {
		t.Fatalf("entry field: %s", entry)
	}
	state, err := fb.StateField(mailID)
	if err != nil || state != "state:"+mailID {
		t.Fatalf("state field: %s %v", state, err)
	}
	digest, err := mail.DeliveryDigest(10001, "WELCOME:10001:1")
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := fb.DeliveryField(digest)
	if err != nil || delivery != "delivery:"+digest {
		t.Fatalf("delivery field: %s %v", delivery, err)
	}
	if _, err := fb.DeliveryField("not-a-digest"); err == nil {
		t.Fatal("raw identities must never reach field names")
	}
	if _, err := fb.EntryField("bogus"); err == nil {
		t.Fatal("unvalidated mail id must be rejected")
	}
}
