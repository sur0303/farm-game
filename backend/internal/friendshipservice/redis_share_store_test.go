package friendshipservice

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestDecodeShareProjectionRejectsMalformedAndPreservesIssuer(t *testing.T) {
	projection, err := decodeShareProjection([]any{"42", "ignored-label", "100", "200"})
	if err != nil || projection.InviterPlayerID != 42 || projection.CreatedAtMs != 100 ||
		projection.ExpiresAtMs != 200 || projection.AcceptMode != ShareAcceptModeMulti {
		t.Fatalf("projection=%+v err=%v", projection, err)
	}
	for _, malformed := range [][]any{
		nil,
		{"0", "label", "100", "200"},
		{"42", "label", "200", "100"},
		{"42", "label", "bad", "200"},
	} {
		if _, err := decodeShareProjection(malformed); !errors.Is(err, ErrShareLinkInvalid) {
			t.Fatalf("malformed=%v err=%v", malformed, err)
		}
	}
}

func TestLiveRedisShareProjectionRemainsMultiResolve(t *testing.T) {
	uri := os.Getenv("FARM_REDIS_URL")
	if uri == "" {
		t.Skip("FARM_REDIS_URL not set; skipping live share projection test")
	}
	options, err := redis.ParseURL(uri)
	if err != nil {
		t.Fatalf("parse Redis URL: %v", err)
	}
	client := redis.NewClient(options)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("Redis ping: %v", err)
	}

	store, err := NewRedisShareProjectionStore(client,
		fmt.Sprintf("farm:test:friend-share-projection:%d", time.Now().UnixNano()), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	token, err := store.Create(ctx, 999000301)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_ = client.Del(cleanupCtx, store.key(token)).Err()
	})

	for attempt := 0; attempt < 2; attempt++ {
		resolved, resolveErr := store.Resolve(ctx, token)
		if resolveErr != nil || resolved.InviterPlayerID != 999000301 || resolved.AcceptMode != ShareAcceptModeMulti {
			t.Fatalf("attempt=%d projection=%+v err=%v", attempt, resolved, resolveErr)
		}
	}
	admission, err := store.ResolveForAdmission(ctx, token)
	if err != nil || admission.InviterPlayerID != 999000301 {
		t.Fatalf("admission=%+v err=%v", admission, err)
	}
}
