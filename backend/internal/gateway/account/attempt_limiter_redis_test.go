package account

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type attemptRedisCommandLog struct {
	mu       sync.Mutex
	commands []string
}

func (h *attemptRedisCommandLog) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *attemptRedisCommandLog) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, command redis.Cmder) error {
		err := next(ctx, command)
		h.mu.Lock()
		h.commands = append(h.commands, command.Name())
		h.mu.Unlock()
		return err
	}
}

func (h *attemptRedisCommandLog) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, commands []redis.Cmder) error {
		err := next(ctx, commands)
		h.mu.Lock()
		for _, command := range commands {
			h.commands = append(h.commands, command.Name())
		}
		h.mu.Unlock()
		return err
	}
}

func (h *attemptRedisCommandLog) reset() {
	h.mu.Lock()
	h.commands = nil
	h.mu.Unlock()
}

func (h *attemptRedisCommandLog) snapshot() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.commands...)
}

func TestRedisAttemptLimiterNoScriptRecoveryDoesNotReplaySuccessfulItems(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	log := &attemptRedisCommandLog{}
	client.AddHook(log)
	limiter, err := NewRedisAttemptLimiter(client, AttemptLimiterConfig{
		Prefix:  "test:attempt-noscript",
		HashKey: []byte("0123456789abcdef0123456789abcdef"),
		Account: LimitPolicy{Burst: 1, Window: time.Minute},
		IP:      LimitPolicy{Burst: 1, Window: time.Minute},
		Device:  LimitPolicy{Burst: 1, Window: time.Minute},
	})
	if err != nil {
		t.Fatal(err)
	}
	attempt := LoginAttempt{Username: "farmer", ClientIP: "192.0.2.1", DeviceID: "device-1"}
	decision, err := limiter.Allow(ctx, attempt)
	if err != nil || !decision.Allowed {
		t.Fatalf("cold decision=%+v err=%v", decision, err)
	}
	if got, want := log.snapshot(), []string{"evalsha", "evalsha", "evalsha", "eval", "eval", "eval"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("cold command vector=%v want=%v", got, want)
	}

	// Each fallback consumed exactly one token. Replaying any successful item or
	// executing a fallback twice would change this deterministic one-token state.
	log.reset()
	decision, err = limiter.Allow(ctx, attempt)
	if err != nil || decision.Allowed || len(decision.LimitedBy) != 3 {
		t.Fatalf("second decision=%+v err=%v", decision, err)
	}
	if got, want := log.snapshot(), []string{"evalsha", "evalsha", "evalsha"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("warm command vector=%v want=%v", got, want)
	}
}
