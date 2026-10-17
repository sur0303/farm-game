package playermail

import (
	"context"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"

	"farmgame/internal/player/mail"
)

// TestLiveRedisSmoke runs the same atomicity spine against a real Redis
// when FARM_REDIS_URL is set (same gate as the R1 authority adapter).
// Without it the suite skips: no mock may claim REAL_REDIS_PASS.
func TestLiveRedisSmoke(t *testing.T) {
	addr := os.Getenv("FARM_REDIS_URL")
	if addr == "" {
		t.Skip("FARM_REDIS_URL not set: skipping live redis evidence")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	store, err := NewStore(client, Profile{Namespace: testNamespace(t), BucketCount: DefaultBucketCount}, mail.LimitsV1())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureLoaded(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	entry := textEntry(t, 99001, "LIVE:SMOKE:1", testNowMs)
	res, err := store.InsertMailIfAbsent(ctx, entry)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != mail.InsertInserted && res.Status != mail.InsertAlreadyDelivered {
		t.Fatalf("live insert: %s", res.Status)
	}
	again, err := store.InsertMailIfAbsent(ctx, entry)
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != mail.InsertAlreadyDelivered || again.MailID != entry.MailID {
		t.Fatalf("live replay: %+v", again)
	}
	mr, err := store.MarkMailRead(ctx, 99001, entry.MailID, "live-read-1")
	if err != nil {
		t.Fatal(err)
	}
	if mr.Status != mail.MutationReadMarked && mr.Status != mail.MutationNoChange {
		t.Fatalf("live mark read: %s", mr.Status)
	}
	page, err := store.ListPage(ctx, 99001, "", testNowMs+1)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, item := range page.Items {
		if item.MailID == entry.MailID {
			found = true
		}
	}
	if !found {
		t.Fatal("live list page must surface the inserted mail")
	}
}
