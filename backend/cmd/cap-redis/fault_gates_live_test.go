package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
	redisauthority "farmgame/internal/storage/redis/authority"

	"github.com/redis/go-redis/v9"
)

func faultGateRedisURL(t *testing.T) string {
	t.Helper()
	value := strings.TrimSpace(os.Getenv("CAP_REDIS_FAULT_URL"))
	if value == "" {
		t.Skip("CAP_REDIS_FAULT_URL is required for real Redis fault gates")
	}
	return value
}

func faultGateEnvironment(t *testing.T) (*benchmarkEnvironment, *redis.Client, *redis.Options) {
	t.Helper()
	options, err := redis.ParseURL(faultGateRedisURL(t))
	if err != nil {
		t.Fatal("invalid fault Redis URL")
	}
	options.Protocol = 2
	hook := newRedisMetricHook()
	options.Dialer = hook.countingDialer
	client := redis.NewClient(options)
	t.Cleanup(func() { _ = client.Close() })
	zone := fmt.Sprintf("z%d", time.Now().UnixNano()%1_000_000_000)
	environment, err := newBenchmarkEnvironment(context.Background(), client, hook, "capfault", zone)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := environment.deleteNamespace(cleanup); err != nil {
			t.Errorf("fault namespace cleanup: %v", err)
		}
	})
	return environment, client, options
}

func TestCAPRedisConnectionReconstructionAfterServerKill(t *testing.T) {
	environment, client, options := faultGateEnvironment(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	key := fmt.Sprintf("classic-farm:%s:%s:connection-rebuild", environment.namespace.Environment, environment.namespace.Zone)
	conn := client.Conn()
	defer conn.Close()
	if err := conn.Set(ctx, key, "durable", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	clientID, err := conn.ClientID(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	admin := redis.NewClient(options)
	defer admin.Close()
	if err := admin.Do(ctx, "CLIENT", "KILL", "ID", clientID).Err(); err != nil {
		t.Fatal(err)
	}
	before := environment.hook.snapshot().Dials
	if _, err := conn.Get(ctx, key).Result(); err == nil {
		t.Fatal("killed sticky connection unexpectedly remained usable")
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	value, err := client.Get(ctx, key).Result()
	if err != nil || value != "durable" {
		t.Fatalf("read after pool reconstruction value=%q err=%v", value, err)
	}
	after := environment.hook.snapshot().Dials
	if after <= before {
		t.Fatalf("connection was not reconstructed: dials before=%d after=%d", before, after)
	}
	t.Logf("CONNECTION_REBUILD_PASS client_id_hash=%s dials_before=%d dials_after=%d durable_readback=true",
		diagnosticHash("cap-redis-client-id-v1", fmt.Sprint(clientID)), before, after)
}

type dropEvalReplyConn struct {
	net.Conn
	mu      sync.Mutex
	armed   bool
	dropped *atomic.Bool
}

func (c *dropEvalReplyConn) Write(value []byte) (int, error) {
	c.mu.Lock()
	if !c.dropped.Load() && bytes.Contains(bytes.ToUpper(value), []byte("EVALSHA")) {
		c.armed = true
	}
	c.mu.Unlock()
	return c.Conn.Write(value)
}

func (c *dropEvalReplyConn) Read(value []byte) (int, error) {
	c.mu.Lock()
	armed := c.armed && !c.dropped.Load()
	c.mu.Unlock()
	if !armed {
		return c.Conn.Read(value)
	}
	discard := make([]byte, max(4096, len(value)))
	n, err := c.Conn.Read(discard)
	if n == 0 && err != nil {
		return 0, err
	}
	c.dropped.Store(true)
	_ = c.Conn.Close()
	return 0, io.EOF
}

func TestCAPRedisPostWriteResponseLossResolvesCommitted(t *testing.T) {
	environment, _, options := faultGateEnvironment(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	seed, err := loadActor(ctx, environment, actorrouting.DomainPlayer, "uniform", []byte("before-response-loss"))
	if err != nil {
		t.Fatal(err)
	}
	request := commitRequest(seed, "BUY", []byte("after-response-loss"), false)

	var dropped atomic.Bool
	faultOptions := *options
	faultOptions.Protocol = 2
	faultOptions.PoolSize = 1
	faultOptions.MinIdleConns = 0
	faultOptions.MaxRetries = -1
	baseDialer := &net.Dialer{Timeout: 5 * time.Second}
	faultOptions.Dialer = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, dialErr := baseDialer.DialContext(ctx, network, address)
		if dialErr != nil {
			return nil, dialErr
		}
		return &dropEvalReplyConn{Conn: conn, dropped: &dropped}, nil
	}
	faultClient := redis.NewClient(&faultOptions)
	defer faultClient.Close()
	faultStore, err := redisauthority.NewStore(faultClient, nil,
		redisauthority.Options{Profile: environment.profile, Resolver: environment.calculator})
	if err != nil {
		t.Fatal(err)
	}
	commit, err := faultStore.Commit(ctx, request)
	if err != nil || commit.Status != authoritystore.CommitTimeoutUnknown || !dropped.Load() {
		t.Fatalf("fault commit status=%s dropped=%v err=%v", commit.Status, dropped.Load(), err)
	}
	resolveID := fmt.Sprintf("fault-resolve-%d", nextID())
	resolved, err := environment.authority.FenceAndClaim(ctx, authoritystore.FenceAndClaimRequest{
		ResolveOperationID: resolveID, ActorAddress: seed.address, AuthorityLayoutVersion: "v1",
		ShardWriteFence: seed.fence, OldWriterClaimID: seed.claimID, NewWriterClaimID: "new-" + resolveID,
		OriginalIdempotencyKey: seed.key, CanonicalPayloadHash: seed.hash,
		BeforeRecordVersion: seed.snapshot.RecordVersion, BeforeBusinessStateDigest: seed.snapshot.BusinessStateDigest,
	})
	if err != nil || resolved.Status != authoritystore.FenceResolutionCommitted ||
		!bytes.Equal(resolved.Outcome.Payload, seed.outcome.Payload) || resolved.Snapshot.RecordVersion != seed.snapshot.RecordVersion+1 {
		t.Fatalf("resolution=%+v err=%v", resolved, err)
	}
	lookup, err := environment.authority.LookupIdempotencyOutcome(ctx, authoritystore.IdempotencyLookupRequest{
		ActorAddress: seed.address, IdempotencyKey: seed.key, CanonicalPayloadHash: seed.hash,
	})
	if err != nil || lookup.Status != authoritystore.IdempotencyFoundMatch {
		t.Fatalf("lookup=%+v err=%v", lookup, err)
	}
	t.Logf("TIMEOUT_UNKNOWN_RESPONSE_LOSS_PASS commit=%s resolution=%s record_version=%d receipt_match=true",
		commit.Status, resolved.Status, resolved.Snapshot.RecordVersion)
}
