package ticket

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestParseRedeemResultAcceptsRealRedisStrings(t *testing.T) {
	issued := time.Now().Add(-time.Second).UnixMilli()
	expires := time.Now().Add(time.Minute).UnixMilli()
	got, err := parseRedeemResult([]interface{}{
		"10001",
		"session-1",
		"7",
		"9",
		formatInt64(issued),
		formatInt64(expires),
	})
	if err != nil {
		t.Fatalf("parse real Redis result: %v", err)
	}
	if got.PlayerID != 10001 || got.SessionID != "session-1" ||
		got.SessionAuthorityEpoch != 7 || got.ActiveSessionEpoch != 9 {
		t.Fatalf("unexpected binding: %+v", got)
	}
	if got.IssuedAt.UnixMilli() != issued || got.ExpiresAt.UnixMilli() != expires {
		t.Fatalf("unexpected timestamps: %+v", got)
	}
}

func TestParseRedeemResultAcceptsByteSlices(t *testing.T) {
	got, err := parseRedeemResult([]interface{}{
		[]byte("42"),
		[]byte("session-2"),
		[]byte("1"),
		[]byte("2"),
		[]byte("1000"),
		[]byte("2000"),
	})
	if err != nil {
		t.Fatalf("parse byte result: %v", err)
	}
	if got.PlayerID != 42 || got.SessionID != "session-2" {
		t.Fatalf("unexpected binding: %+v", got)
	}
}

func TestParseRedeemResultRejectsMalformedFields(t *testing.T) {
	_, err := parseRedeemResult([]interface{}{"bad", "session", "1", "2", "1000", "2000"})
	if err != ErrRedeemFailed {
		t.Fatalf("want ErrRedeemFailed, got %v", err)
	}
}

type topLevelCommandCounter struct{ commands atomic.Int64 }

func (h *topLevelCommandCounter) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *topLevelCommandCounter) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.commands.Add(1)
		return next(ctx, cmd)
	}
}

func (h *topLevelCommandCounter) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, commands []redis.Cmder) error {
		h.commands.Add(int64(len(commands)))
		return next(ctx, commands)
	}
}

func TestRedisStoreSaveUsesOneAtomicCommandWithTTL(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	counter := &topLevelCommandCounter{}
	client.AddHook(counter)
	store := NewRedisStore(client)
	bound := Bound{
		PlayerID: 42, SessionID: "session-save", SessionAuthorityEpoch: 3,
		ReservedSessionEpoch: 7, ActiveSessionEpoch: 6,
		IssuedAt: time.Now(), ExpiresAt: time.Now().Add(time.Minute),
	}
	if err := store.Save(context.Background(), "v1.42.warm", bound, time.Minute); err != nil {
		t.Fatalf("warm save script: %v", err)
	}

	const token = "v1.42.measured"
	before := counter.commands.Load()
	if err := store.Save(context.Background(), token, bound, time.Minute); err != nil {
		t.Fatalf("save: %v", err)
	}
	if commands := counter.commands.Load() - before; commands != 1 {
		t.Fatalf("ticket save top-level Redis commands=%d, want exactly 1 EVALSHA", commands)
	}
	key, err := store.key(token, bound.PlayerID)
	if err != nil {
		t.Fatal(err)
	}
	if ttl := server.TTL(key); ttl <= 0 || ttl > time.Minute {
		t.Fatalf("ticket TTL=%s, want (0,1m]", ttl)
	}
	if got, err := client.HGet(context.Background(), key, "session_id").Result(); err != nil || got != bound.SessionID {
		t.Fatalf("stored ticket session_id=%q err=%v", got, err)
	}
}

func BenchmarkRedisStoreSaveCommandCount(b *testing.B) {
	server := miniredis.RunT(b)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	b.Cleanup(func() { _ = client.Close() })
	counter := &topLevelCommandCounter{}
	client.AddHook(counter)
	store := NewRedisStore(client)
	bound := Bound{
		PlayerID: 43, SessionID: "session-benchmark", SessionAuthorityEpoch: 1,
		ReservedSessionEpoch: 2, IssuedAt: time.Now(), ExpiresAt: time.Now().Add(time.Minute),
	}
	const token = "v1.43.benchmark"
	if err := store.Save(context.Background(), token, bound, time.Minute); err != nil {
		b.Fatal(err)
	}
	before := counter.commands.Load()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := store.Save(context.Background(), token, bound, time.Minute); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	commands := counter.commands.Load() - before
	if commands != int64(b.N) {
		b.Fatalf("top-level Redis commands=%d, want exactly %d", commands, b.N)
	}
	b.ReportMetric(float64(commands)/float64(b.N), "redis_commands/op")
}

func formatInt64(value int64) string {
	return strconv.FormatInt(value, 10)
}
