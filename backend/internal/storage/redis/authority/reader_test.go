package authority

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
	"github.com/redis/go-redis/v9"
)

func TestShardAuthorityReaderExactTupleAndCorruption(t *testing.T) {
	rig := newTestRig(t)
	readerOptions := DefaultAuthorityReaderOptions(rig.store.opts.Profile, rig.store.opts.Resolver)
	readerOptions.OperationTimeout = time.Second
	reader, err := NewShardAuthorityReader(rig.client, readerOptions)
	if err != nil {
		t.Fatal(err)
	}
	got, found, err := reader.ReadShardWriteAuthority(context.Background(), rig.addr.Domain, rig.bucket.VirtualShard())
	if err != nil || !found {
		t.Fatalf("read found=%t err=%v", found, err)
	}
	if got.Domain != rig.addr.Domain || got.VirtualShard != rig.bucket.VirtualShard() || got.HolderID != rig.fence.ExpectedHolderID || got.HolderBootID != rig.fence.ExpectedHolderBootID || got.AssignmentRevision != rig.fence.ExpectedAssignmentRevision || got.WriterEpoch != rig.fence.ExpectedWriterEpoch || got.LeaseEpoch != rig.fence.ExpectedLeaseEpoch || got.State.String() != "ACTIVE" {
		t.Fatalf("authority tuple=%+v", got)
	}
	missing, found, err := reader.ReadShardWriteAuthority(context.Background(), actorrouting.DomainFarm, 0)
	if err != nil || found || missing != (authoritystore.ShardWriteAuthority{}) {
		t.Fatalf("missing=%+v found=%t err=%v", missing, found, err)
	}
	key, err := rig.store.Keys().ShardAuthority(rig.bucket)
	if err != nil {
		t.Fatal(err)
	}
	if err := rig.client.HSet(context.Background(), key.String(), "unexpected", "field").Err(); err != nil {
		t.Fatal(err)
	}
	if _, found, err := reader.ReadShardWriteAuthority(context.Background(), rig.addr.Domain, rig.bucket.VirtualShard()); !found || !errors.Is(err, ErrAuthorityReaderCorrupt) {
		t.Fatalf("extra field found=%t err=%v", found, err)
	}
}

func TestShardAuthorityReaderBatchPreservesOrderAndUsesOnePipeline(t *testing.T) {
	rig := newTestRig(t)
	reader, err := NewAuthorityReader(rig.client, DefaultAuthorityReaderOptions(rig.store.opts.Profile, rig.store.opts.Resolver))
	if err != nil {
		t.Fatal(err)
	}
	if sha, err := rig.client.ScriptLoad(context.Background(), readShardAuthorityLua).Result(); err != nil || sha != reader.scriptSHA {
		t.Fatalf("preload script sha=%q err=%v", sha, err)
	}
	hook := &authorityReaderCommandHook{}
	rig.client.AddHook(hook)
	domains := []actorrouting.RouteDomain{rig.addr.Domain, actorrouting.DomainFarm, rig.addr.Domain}
	shards := []uint32{rig.bucket.VirtualShard(), 0, rig.bucket.VirtualShard()}
	values, found, err := reader.ReadShardWriteAuthorityBatch(context.Background(), domains, shards)
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 3 || len(found) != 3 || !found[0] || found[1] || !found[2] || values[0] != values[2] || values[0].HolderID != rig.fence.ExpectedHolderID {
		t.Fatalf("batch values=%+v found=%v", values, found)
	}
	commands, pipelines := hook.snapshot()
	if pipelines != 1 || len(commands) != 0 {
		t.Fatalf("batch used serial commands=%v pipelines=%d", commands, pipelines)
	}
}

func TestShardAuthorityReaderRejectsDigestAndTupleDrift(t *testing.T) {
	rig := newTestRig(t)
	reader, err := NewAuthorityReader(rig.client, DefaultAuthorityReaderOptions(rig.store.opts.Profile, rig.store.opts.Resolver))
	if err != nil {
		t.Fatal(err)
	}
	key, err := rig.store.Keys().ShardAuthority(rig.bucket)
	if err != nil {
		t.Fatal(err)
	}
	if err := rig.client.HSet(context.Background(), key.String(), "last_transition_digest", strings.Repeat("A", 64)).Err(); err != nil {
		t.Fatal(err)
	}
	if _, found, err := reader.Read(context.Background(), rig.addr.Domain, rig.bucket.VirtualShard()); !found || !errors.Is(err, ErrAuthorityReaderCorrupt) {
		t.Fatalf("digest found=%t err=%v", found, err)
	}
	if err := rig.client.HSet(context.Background(), key.String(), "last_transition_digest", strings.Repeat("a", 64), "virtual_shard_id", "999").Err(); err != nil {
		t.Fatal(err)
	}
	if _, found, err := reader.Read(context.Background(), rig.addr.Domain, rig.bucket.VirtualShard()); !found || !errors.Is(err, ErrAuthorityReaderCorrupt) {
		t.Fatalf("tuple found=%t err=%v", found, err)
	}
}

func TestShardAuthorityIntentDigestCrossContract(t *testing.T) {
	fence := authoritystore.ShardWriteFence{ExpectedHolderID: "holder-a", ExpectedHolderBootID: "boot-a", ExpectedAssignmentRevision: 7, ExpectedWriterEpoch: 8, ExpectedLeaseEpoch: 9}
	redisDigest := transitionIntentDigest("transfer", "auth-v1", string(actorrouting.DomainPlayer), 11, fence, "holder-b", "boot-b", 30_000)
	contractDigest := authoritystore.ShardAuthorityTransitionIntentDigest("transfer", "auth-v1", string(actorrouting.DomainPlayer), 11, fence, "holder-b", "boot-b", 30_000)
	if redisDigest != contractDigest {
		t.Fatalf("digest drift redis=%s contract=%s", redisDigest, contractDigest)
	}
}

func TestShardAuthorityReaderContextIsBounded(t *testing.T) {
	rig := newTestRig(t)
	options := DefaultAuthorityReaderOptions(rig.store.opts.Profile, rig.store.opts.Resolver)
	options.OperationTimeout = 20 * time.Millisecond
	reader, err := NewAuthorityReader(rig.client, options)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := reader.Read(ctx, rig.addr.Domain, rig.bucket.VirtualShard()); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled read err=%v", err)
	}
}

func TestShardAuthorityReaderUsesOneReadOnlyScriptWithBoundedNOSCRIPTReload(t *testing.T) {
	rig := newTestRig(t)
	if err := rig.client.ScriptFlush(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	hook := &authorityReaderCommandHook{}
	rig.client.AddHook(hook)
	reader, err := NewAuthorityReader(rig.client, DefaultAuthorityReaderOptions(rig.store.opts.Profile, rig.store.opts.Resolver))
	if err != nil {
		t.Fatal(err)
	}
	if _, found, err := reader.Read(context.Background(), rig.addr.Domain, rig.bucket.VirtualShard()); err != nil || !found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	commands, pipelines := hook.snapshot()
	if pipelines != 0 {
		t.Fatalf("pipeline calls=%d", pipelines)
	}
	evalSHA := 0
	scriptLoad := 0
	for _, command := range commands {
		switch command {
		case "evalsha":
			evalSHA++
		case "script":
			scriptLoad++
		default:
			t.Fatalf("reader issued non-script command %q; all=%v", command, commands)
		}
	}
	if evalSHA != 2 || scriptLoad != 1 {
		t.Fatalf("commands=%v want EVALSHA,NOSCRIPT load,EVALSHA", commands)
	}
	if strings.Contains(strings.ToUpper(readShardAuthorityLua), "HKEYS") || strings.Contains(strings.ToUpper(readShardAuthorityLua), "SCAN") {
		t.Fatal("reader script contains forbidden enumeration")
	}
}

type authorityReaderCommandHook struct {
	mu        sync.Mutex
	commands  []string
	pipelines int
}

func (*authorityReaderCommandHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *authorityReaderCommandHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.mu.Lock()
		h.commands = append(h.commands, strings.ToLower(cmd.Name()))
		h.mu.Unlock()
		return next(ctx, cmd)
	}
}

func (h *authorityReaderCommandHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, commands []redis.Cmder) error {
		h.mu.Lock()
		h.pipelines++
		h.mu.Unlock()
		return next(ctx, commands)
	}
}

func (h *authorityReaderCommandHook) snapshot() ([]string, int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.commands...), h.pipelines
}
