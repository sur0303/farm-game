package playermail

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"farmgame/internal/player/mail"
)

const testNowMs = int64(1754200000000)

type testRig struct {
	server *miniredis.Miniredis
	client *redis.Client
	store  *Store
	now    int64
}

func newTestRig(t *testing.T) *testRig {
	t.Helper()
	server := miniredis.RunT(t)
	server.SetTime(time.UnixMilli(testNowMs))
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	ns := testNamespace(t)
	store, err := NewStore(client, Profile{Namespace: ns, BucketCount: DefaultBucketCount}, mail.LimitsV1())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureLoaded(context.Background()); err != nil {
		t.Fatal(err)
	}
	return &testRig{server: server, client: client, store: store, now: testNowMs}
}

func (r *testRig) setNow(ms int64) {
	r.now = ms
	r.server.SetTime(time.UnixMilli(ms))
}

func textEntry(t *testing.T, player uint64, deliveryID string, createdAtMs int64) mail.MailEntry {
	t.Helper()
	entry, err := mail.BuildMailEntry(mail.DeliveryParams{
		RecipientPlayerID: player,
		DeliveryID:        deliveryID,
		SourceType:        mail.SourceTypeSystem,
		TitleSnapshot:     "title-" + deliveryID,
		BodySnapshot:      "body",
		CreatedAtMs:       createdAtMs,
		VisibleAtMs:       createdAtMs,
	}, mail.LimitsV1())
	if err != nil {
		t.Fatal(err)
	}
	return entry
}

func rewardEntry(t *testing.T, player uint64, deliveryID string, createdAtMs int64) mail.MailEntry {
	t.Helper()
	entry, err := mail.BuildMailEntry(mail.DeliveryParams{
		RecipientPlayerID: player,
		DeliveryID:        deliveryID,
		SourceType:        mail.SourceTypeSystem,
		TitleSnapshot:     "title-" + deliveryID,
		BodySnapshot:      "body",
		Reward:            mail.RewardBundle{Coin: 10, Items: []mail.ItemGrant{{ItemID: 1, Count: 2}}},
		CreatedAtMs:       createdAtMs,
		VisibleAtMs:       createdAtMs,
	}, mail.LimitsV1())
	if err != nil {
		t.Fatal(err)
	}
	return entry
}

func mustInsert(t *testing.T, store *Store, entry mail.MailEntry) mail.InsertResult {
	t.Helper()
	res, err := store.InsertMailIfAbsent(context.Background(), entry)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if res.Status != mail.InsertInserted {
		t.Fatalf("insert status: got %s want INSERTED", res.Status)
	}
	return res
}

// dropClient simulates the two TIMEOUT_UNKNOWN shapes: the request never
// reached execution (before-apply) or it executed but the reply was lost
// (after-apply/response-lost). It mirrors the R1 authority fault rig.
type dropClient struct {
	redis.UniversalClient
	mu         sync.Mutex
	dropBefore int
	dropAfter  int
}

func (c *dropClient) EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd {
	c.mu.Lock()
	before, after := c.dropBefore, c.dropAfter
	c.dropBefore, c.dropAfter = 0, 0
	c.mu.Unlock()
	if before > 0 {
		return redis.NewCmdResult(nil, context.DeadlineExceeded)
	}
	cmd := c.UniversalClient.EvalSha(ctx, sha1, keys, args...)
	if after > 0 {
		// The script really executed; only the reply is lost.
		_, _ = cmd.Result()
		return redis.NewCmdResult(nil, context.DeadlineExceeded)
	}
	return cmd
}

func (c *dropClient) armBefore(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dropBefore = n
}

func (c *dropClient) armAfter(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dropAfter = n
}

func newDropRig(t *testing.T, drop *dropClient) *testRig {
	t.Helper()
	rig := newTestRig(t)
	drop.UniversalClient = rig.client
	store, err := NewStore(drop, Profile{Namespace: testNamespace(t), BucketCount: DefaultBucketCount}, mail.LimitsV1())
	if err != nil {
		t.Fatal(err)
	}
	// Preload every script so a dropped reply can only hide an execution
	// that truly happened; otherwise the first NOSCRIPT would mask
	// "before apply".
	if err := store.EnsureLoaded(context.Background()); err != nil {
		t.Fatal(err)
	}
	rig.store = store
	return rig
}
