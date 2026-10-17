package routecontrol

import (
	"bufio"
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestClassifyRedisRequestFailsClosed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		args       [][]byte
		command    string
		class      string
		keys       int
		wantReject bool
	}{
		{args: byteArgs("GET", "key"), command: "GET", class: "read", keys: 1},
		{args: byteArgs("HSTRLEN", "holder-registry-key", "field"), command: "HSTRLEN", class: "read", keys: 1},
		{args: byteArgs("set", "key", "value"), command: "SET", class: "mutation", keys: 1},
		{args: byteArgs("MULTI"), command: "MULTI", class: "transaction", keys: 0},
		{args: byteArgs("EXEC"), command: "EXEC", class: "transaction", keys: 0},
		{args: byteArgs("DISCARD"), command: "DISCARD", class: "transaction", keys: 0},
		{args: byteArgs("UNWATCH"), command: "UNWATCH", class: "transaction", keys: 0},
		{args: byteArgs("WATCH", "key-a", "key-b"), command: "WATCH", class: "mutation", keys: 2},
		{args: byteArgs("WATCH"), command: "WATCH", wantReject: true},
		{args: byteArgs("EVALSHA", "012345", "2", "key-a", "key-b", "arg"), command: "EVALSHA", class: "mutation", keys: 2},
		{args: byteArgs("EVALSHA", "012345", "0"), command: "EVALSHA", wantReject: true},
		{args: byteArgs("SCRIPT", "FLUSH"), command: "SCRIPT", wantReject: true},
		{args: byteArgs("FLUSHDB"), command: "FLUSHDB", wantReject: true},
		{args: byteArgs("SCAN", "0"), command: "SCAN", wantReject: true},
		{args: byteArgs("MYSTERY", "key"), command: "MYSTERY", wantReject: true},
	}
	for _, test := range tests {
		command, class, _, keys, err := classifyRedisRequest(test.args)
		if command != test.command {
			t.Fatalf("command=%q want=%q", command, test.command)
		}
		if test.wantReject {
			if err == nil {
				t.Fatalf("%s unexpectedly classified", command)
			}
			continue
		}
		if err != nil || class != test.class || len(keys) != test.keys {
			t.Fatalf("%s class=%q keys=%d err=%v", command, class, len(keys), err)
		}
	}
}

func TestRESPFrameRoundTrip(t *testing.T) {
	t.Parallel()
	frames := []string{
		"+OK\r\n",
		"$5\r\nvalue\r\n",
		"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
		"%2\r\n+server\r\n+redis\r\n+proto\r\n:3\r\n",
		"|1\r\n+ttl\r\n:4\r\n$5\r\nvalue\r\n",
	}
	for _, frame := range frames {
		got, err := readRESPFrame(bufio.NewReader(bytes.NewBufferString(frame)))
		if err != nil || string(got) != frame {
			t.Fatalf("frame=%q got=%q err=%v", frame, got, err)
		}
	}
}

func TestRESPProxyLiveExactLedgerAndCleanup(t *testing.T) {
	rawURL := os.Getenv("RT2Q_REDIS_LIVE_URL")
	if rawURL == "" {
		t.Skip("RT2Q_REDIS_LIVE_URL is not set")
	}
	proxy, err := NewRESPProxy(rawURL, "0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	proxyURL, err := proxy.Listen("observer")
	if err != nil {
		t.Fatal(err)
	}
	options, err := redis.ParseURL(proxyURL)
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(options)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	key := "rt2q-live-proxy-test"
	if err := client.Set(ctx, key, "secret-value", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	entries, digest := proxy.Ledger()
	if len(entries) != 1 || entries[0].KeyDigest == "" || entries[0].rawKey != "" || digest == "" {
		t.Fatalf("public ledger=%+v digest=%q", entries, digest)
	}
	cleanup, err := proxy.Cleanup(ctx, "observer")
	if err != nil {
		t.Fatal(err)
	}
	if len(cleanup) != 1 || cleanup[0].ExistsReply != 0 {
		t.Fatalf("cleanup=%+v", cleanup)
	}
}

func TestRESPProxyReplyLossDropsAppliedEVALSHAAndBlocks(t *testing.T) {
	t.Parallel()
	server := miniredis.RunT(t)
	proxy, err := NewRESPProxy("redis://"+server.Addr(), "0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	proxyURL, err := proxy.Listen("controller")
	if err != nil {
		t.Fatal(err)
	}
	options, err := redis.ParseURL(proxyURL)
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(options)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const script = `return {'APPLIED','holder','boot','1','1','1','ACTIVE','1','tid','1','1000'}`
	sha, err := client.ScriptLoad(ctx, script).Result()
	if err != nil {
		t.Fatal(err)
	}
	key := "rt2q-reply-loss-authority"
	if err := proxy.ArmReplyLoss("controller", key, sha, true); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- client.EvalSha(ctx, sha, []string{key}).Err()
	}()
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer waitCancel()
	if err := proxy.WaitFaultTriggered(waitCtx); err != nil {
		t.Fatalf("WaitFaultTriggered: %v", err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("client unexpectedly received dropped APPLIED reply")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("client did not observe connection close after reply drop")
	}
	fault := proxy.Fault()
	if fault == nil || !fault.Triggered || !fault.Blocked {
		t.Fatalf("fault=%+v", fault)
	}
	events := proxy.Events()
	dropped := 0
	for _, event := range events {
		if event.Command == "EVALSHA" && event.ReplyDropped {
			dropped++
		}
	}
	if dropped != 1 {
		t.Fatalf("dropped=%d events=%+v", dropped, events)
	}
	// Later traffic on the same listener must be blocked without correctness sleeps.
	blocked := redis.NewClient(options)
	defer blocked.Close()
	blockCtx, blockCancel := context.WithTimeout(context.Background(), time.Second)
	defer blockCancel()
	if err := blocked.Ping(blockCtx).Err(); err == nil {
		t.Fatal("blocked listener unexpectedly accepted Ping")
	}
	proxy.ClearReplyLoss()
	clearCtx, clearCancel := context.WithTimeout(context.Background(), time.Second)
	defer clearCancel()
	cleared := redis.NewClient(options)
	defer cleared.Close()
	if err := cleared.Ping(clearCtx).Err(); err != nil {
		t.Fatalf("ClearReplyLoss did not restore listener: %v", err)
	}
}

func TestRedisReplyIndicatesTransferApplied(t *testing.T) {
	t.Parallel()
	if !redisReplyIndicatesTransferApplied([]byte("*11\r\n$7\r\nAPPLIED\r\n$1\r\na\r\n")) {
		t.Fatal("APPLIED reply not recognized")
	}
	if redisReplyIndicatesTransferApplied([]byte("*1\r\n$14\r\nALREADY_APPLIED\r\n")) {
		t.Fatal("ALREADY_APPLIED must not match APPLIED drop predicate")
	}
	if redisReplyIndicatesTransferApplied([]byte("-NOSCRIPT No matching script\r\n")) {
		t.Fatal("NOSCRIPT must not be dropped")
	}
}

func byteArgs(values ...string) [][]byte {
	result := make([][]byte, len(values))
	for index, value := range values {
		result[index] = []byte(value)
	}
	return result
}
