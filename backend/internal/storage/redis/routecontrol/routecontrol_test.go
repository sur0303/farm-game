package routecontrol

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/yuin/gopher-lua"

	"farmgame/internal/actorrouting"
	"farmgame/internal/actorrouting/authority"
	"farmgame/internal/controlfence"
	core "farmgame/internal/routecontrol"
	"farmgame/internal/storage/redis/rediskey"
)

func redisTestSnapshot(revision uint64) actorrouting.RouteSnapshot {
	return actorrouting.RouteSnapshot{
		Realm:          "realm-a",
		GlobalRevision: revision,
		HashVersion:    actorrouting.HashVersionXXH64V2,
		HashSeed:       202608030001,
		Player: actorrouting.DomainSnapshot{
			ShardCount: 1,
			Routes: []actorrouting.ShardRoute{{
				HolderIndex: 0, State: actorrouting.ShardStateActive,
				ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: 1000,
			}},
		},
		Farm: actorrouting.DomainSnapshot{
			ShardCount: 1,
			Routes: []actorrouting.ShardRoute{{
				HolderIndex: 1, State: actorrouting.ShardStateActive,
				ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: 1000,
			}},
		},
		Holders: []actorrouting.HolderRef{
			{InstanceID: "player-1", BootID: "boot-player", Endpoint: "player:1", State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: "farm-1", BootID: "boot-farm", Endpoint: "farm:1", State: actorrouting.HolderStateActive, Weight: 1},
		},
	}
}

func redisTestOptions(t *testing.T) Options {
	t.Helper()
	namespace, err := rediskey.NewNamespace("test", "z1")
	if err != nil {
		t.Fatal(err)
	}
	options := DefaultOptions(namespace)
	options.OperationTimeout = time.Second
	options.Now = func() time.Time { return time.UnixMilli(1234) }
	return options
}

func newRedisTestStores(t *testing.T, options Options) (*miniredis.Miniredis, *redis.Client, *SnapshotStore, *TransitionStore) {
	t.Helper()
	server := miniredis.RunT(t)
	server.SetTime(time.UnixMilli(1234))
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	snapshotStore, err := NewSnapshotStore(client, options)
	if err != nil {
		t.Fatal(err)
	}
	transitionStore, err := NewTransitionStore(client, options)
	if err != nil {
		t.Fatal(err)
	}
	return server, client, snapshotStore, transitionStore
}

type rawTransitionState struct {
	meta           map[string]string
	recoveries     map[string]string
	recoveryIndex  []redis.Z
	tombstones     map[string]string
	tombstoneIndex []redis.Z
}

func captureTransitionState(t *testing.T, client *redis.Client, store *TransitionStore) rawTransitionState {
	t.Helper()
	ctx := context.Background()
	meta, err := client.HGetAll(ctx, store.Keys().Meta().String()).Result()
	if err != nil {
		t.Fatal(err)
	}
	recoveries, err := client.HGetAll(ctx, store.Keys().Recoveries().String()).Result()
	if err != nil {
		t.Fatal(err)
	}
	recoveryIndex, err := client.ZRangeWithScores(ctx, store.Keys().RecoveryIndex().String(), 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	tombstones, err := client.HGetAll(ctx, store.Keys().Tombstones().String()).Result()
	if err != nil {
		t.Fatal(err)
	}
	tombstoneIndex, err := client.ZRangeWithScores(ctx, store.Keys().TombstoneIndex().String(), 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	return rawTransitionState{
		meta: meta, recoveries: recoveries, recoveryIndex: recoveryIndex,
		tombstones: tombstones, tombstoneIndex: tombstoneIndex,
	}
}

func assertTransitionStateUnchanged(t *testing.T, before, after rawTransitionState) {
	t.Helper()
	before = normalizeRawTransitionState(before)
	after = normalizeRawTransitionState(after)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("transition state changed:\n before=%+v\n after=%+v", before, after)
	}
}

func normalizeRawTransitionState(state rawTransitionState) rawTransitionState {
	if state.meta == nil {
		state.meta = map[string]string{}
	}
	if state.recoveries == nil {
		state.recoveries = map[string]string{}
	}
	if state.recoveryIndex == nil {
		state.recoveryIndex = []redis.Z{}
	}
	if state.tombstones == nil {
		state.tombstones = map[string]string{}
	}
	if state.tombstoneIndex == nil {
		state.tombstoneIndex = []redis.Z{}
	}
	return state
}

func TestRedisSnapshotBootstrapCASRestartAndNoScriptReload(t *testing.T) {
	options := redisTestOptions(t)
	_, client, store, _ := newRedisTestStores(t, options)
	ctx := context.Background()

	loaded, found, err := store.LoadCurrent(ctx)
	if err != nil || found || loaded.Revision != 0 {
		t.Fatalf("empty load=%+v found=%t err=%v", loaded, found, err)
	}
	first, err := store.Bootstrap(ctx, core.NestedControlOperationToken{}, redisTestSnapshot(1))
	if err != nil || first.Status != core.SnapshotCASCommitted || first.Stored.CommittedAtMs != 1234 {
		t.Fatalf("bootstrap=%+v err=%v", first, err)
	}
	if exists, err := client.Exists(ctx, snapshotBootstrapFenceKey(store.Keys()).String()).Result(); err != nil || exists != 0 {
		t.Fatalf("zero-token bootstrap fence EXISTS=%d err=%v", exists, err)
	}
	committed, err := store.CompareAndSwap(ctx, core.NestedControlOperationToken{}, 1, redisTestSnapshot(2))
	if err != nil || committed.Status != core.SnapshotCASCommitted {
		t.Fatalf("CAS=%+v err=%v", committed, err)
	}
	replay, err := store.CompareAndSwap(ctx, core.NestedControlOperationToken{}, 2, redisTestSnapshot(2))
	if err != nil || replay.Status != core.SnapshotCASAlreadyCommitted {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	gap, err := store.CompareAndSwap(ctx, core.NestedControlOperationToken{}, 2, redisTestSnapshot(4))
	if err != nil || gap.Status != core.SnapshotCASRevisionConflict {
		t.Fatalf("gap=%+v err=%v", gap, err)
	}
	committedAgain, err := store.CompareAndSwap(ctx, core.NestedControlOperationToken{}, 2, redisTestSnapshot(3))
	if err != nil || committedAgain.Status != core.SnapshotCASCommitted {
		t.Fatalf("second commit=%+v err=%v", committedAgain, err)
	}
	// The candidate is a strict R1 -> R2 request, but current is already R3.
	// This reaches STALE_EXPECTATION without relaxing strict-next validation.
	stale, err := store.CompareAndSwap(ctx, core.NestedControlOperationToken{}, 1, redisTestSnapshot(2))
	if err != nil || stale.Status != core.SnapshotCASStaleExpectation {
		t.Fatalf("stale=%+v err=%v", stale, err)
	}
	restarted, err := NewSnapshotStore(client, options)
	if err != nil {
		t.Fatal(err)
	}
	current, found, err := restarted.LoadCurrent(ctx)
	if err != nil || !found || current.Revision != 3 {
		t.Fatalf("restart load=%+v found=%t err=%v", current, found, err)
	}
	if got, want := SnapshotRedisKey(store).String(), "{classic-farm:test:z1:route-control:v1}:snapshot"; got != want {
		t.Fatalf("snapshot key=%q want=%q", got, want)
	}
	if exists, err := client.Exists(ctx, SnapshotRedisKey(store).String()).Result(); err != nil || exists != 1 {
		t.Fatalf("snapshot EXISTS=%d err=%v", exists, err)
	}
}

func TestRedisSnapshotBootstrapPersistsSeparateFenceWithoutChangingShape(t *testing.T) {
	options := redisTestOptions(t)
	_, client, store, _ := newRedisTestStores(t, options)
	ctx := context.Background()
	issuer := controlfence.NewIssuer()
	token := issuer.Issue("bootstrap-operation", 7)
	first, err := store.Bootstrap(ctx, token, redisTestSnapshot(1))
	if err != nil || first.Status != core.SnapshotCASCommitted {
		t.Fatalf("bootstrap=%+v err=%v", first, err)
	}
	fence, err := client.HGetAll(ctx, snapshotBootstrapFenceKey(store.Keys()).String()).Result()
	if err != nil || !reflect.DeepEqual(fence, map[string]string{
		snapshotFenceFieldSchema:    snapshotBootstrapFenceSchema,
		snapshotFenceFieldOperation: "bootstrap-operation",
		snapshotFenceFieldEpoch:     "7",
	}) {
		t.Fatalf("fence=%v err=%v", fence, err)
	}
	if fields := client.HLen(ctx, store.Keys().Snapshot().String()).Val(); fields != 7 {
		t.Fatalf("snapshot shape fields=%d", fields)
	}
	other := controlfence.NewIssuer().Issue("other-operation", 8)
	replay, err := store.Bootstrap(ctx, other, redisTestSnapshot(1))
	if err != nil || replay.Status != core.SnapshotCASAlreadyCommitted {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	if operation := client.HGet(ctx, snapshotBootstrapFenceKey(store.Keys()).String(), snapshotFenceFieldOperation).Val(); operation != "bootstrap-operation" {
		t.Fatalf("fence operation changed=%q", operation)
	}
}

func TestRedisSnapshotMissingIsOnlyFalseAndCorruptionIsError(t *testing.T) {
	options := redisTestOptions(t)
	_, client, store, _ := newRedisTestStores(t, options)
	ctx := context.Background()
	if _, found, err := store.LoadCurrent(ctx); err != nil || found {
		t.Fatalf("missing load found=%t err=%v", found, err)
	}
	key := store.Keys().Snapshot().String()
	if err := client.HSet(ctx, key, snapshotFieldSchema, core.SnapshotSchemaVersion).Err(); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.LoadCurrent(ctx); !found || err == nil {
		t.Fatalf("partial hash found=%t err=%v", found, err)
	}
}

func TestRedisSnapshotConcurrentCASHasOneCommit(t *testing.T) {
	options := redisTestOptions(t)
	_, _, store, _ := newRedisTestStores(t, options)
	if _, err := store.Bootstrap(context.Background(), core.NestedControlOperationToken{}, redisTestSnapshot(1)); err != nil {
		t.Fatal(err)
	}
	const workers = 32
	results := make(chan core.SnapshotCASResult, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			result, err := store.CompareAndSwap(context.Background(), core.NestedControlOperationToken{}, 1, redisTestSnapshot(2))
			results <- result
			errs <- err
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	committed, replayed := 0, 0
	for result := range results {
		switch result.Status {
		case core.SnapshotCASCommitted:
			committed++
		case core.SnapshotCASAlreadyCommitted:
			replayed++
		default:
			t.Fatalf("unexpected result=%+v", result)
		}
	}
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if committed != 1 || replayed != workers-1 {
		t.Fatalf("commit=%d replay=%d", committed, replayed)
	}
}

func TestRedisSnapshotRevisionStringHandlesUint64Max(t *testing.T) {
	options := redisTestOptions(t)
	_, client, store, _ := newRedisTestStores(t, options)
	max := redisTestSnapshot(math.MaxUint64)
	envelope, err := core.CanonicalizeRouteSnapshot(max)
	if err != nil {
		t.Fatal(err)
	}
	err = client.HSet(context.Background(), store.Keys().Snapshot().String(),
		snapshotFieldSchema, envelope.SchemaVersion,
		snapshotFieldRealm, envelope.Realm,
		snapshotFieldRevision, "18446744073709551615",
		snapshotFieldJSON, envelope.CanonicalJSON,
		snapshotFieldChecksum, envelope.ChecksumSHA256,
		snapshotFieldTopology, envelope.TopologyFingerprint,
		snapshotFieldCommitted, "1234").Err()
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.CompareAndSwap(context.Background(), core.NestedControlOperationToken{}, math.MaxUint64, max)
	if err != nil || result.Status != core.SnapshotCASAlreadyCommitted {
		t.Fatalf("max revision replay=%+v err=%v", result, err)
	}
}

func TestRedisBootstrapExactReplayAndPublisherNotifyRetry(t *testing.T) {
	options := redisTestOptions(t)
	_, _, store, _ := newRedisTestStores(t, options)
	ctx := context.Background()
	var calls int
	failNext := true
	publisher, err := core.NewDurableSnapshotPublisher(store, core.SnapshotNotifierFunc(func(context.Context, actorrouting.RouteSnapshot) error {
		calls++
		if failNext {
			failNext = false
			return errors.New("notify unavailable")
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	first, err := publisher.Bootstrap(ctx, core.NestedControlOperationToken{}, redisTestSnapshot(1))
	if !errors.Is(err, core.ErrSnapshotNotify) || first.Status != core.SnapshotCASCommitted {
		t.Fatalf("first bootstrap result=%+v err=%v", first, err)
	}
	retry, err := publisher.Bootstrap(ctx, core.NestedControlOperationToken{}, redisTestSnapshot(1))
	if err != nil || retry.Status != core.SnapshotCASAlreadyCommitted {
		t.Fatalf("bootstrap retry result=%+v err=%v", retry, err)
	}
	if calls != 2 {
		t.Fatalf("notify calls=%d", calls)
	}

	different := redisTestSnapshot(1)
	different.Holders[0].Endpoint = "player:changed"
	if result, err := store.Bootstrap(ctx, core.NestedControlOperationToken{}, different); !errors.Is(err, core.ErrSnapshotCASConflict) || result.Status != core.SnapshotCASRevisionConflict {
		t.Fatalf("different bootstrap result=%+v err=%v", result, err)
	}
}

func TestRedisSnapshotCASCorruptionFailsClosedWithoutOverwrite(t *testing.T) {
	mutations := map[string]func(*redis.Client, string){
		"schema": func(client *redis.Client, key string) {
			_ = client.HSet(context.Background(), key, snapshotFieldSchema, "route-snapshot-v0").Err()
		},
		"json": func(client *redis.Client, key string) {
			_ = client.HSet(context.Background(), key, snapshotFieldJSON, "{}").Err()
		},
		"checksum": func(client *redis.Client, key string) {
			_ = client.HSet(context.Background(), key, snapshotFieldChecksum, "00").Err()
		},
		"topology": func(client *redis.Client, key string) {
			_ = client.HSet(context.Background(), key, snapshotFieldTopology, "00").Err()
		},
		"revision": func(client *redis.Client, key string) {
			_ = client.HSet(context.Background(), key, snapshotFieldRevision, "01").Err()
		},
		"committed": func(client *redis.Client, key string) {
			_ = client.HSet(context.Background(), key, snapshotFieldCommitted, "01").Err()
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			options := redisTestOptions(t)
			_, client, store, _ := newRedisTestStores(t, options)
			ctx := context.Background()
			if _, err := store.Bootstrap(ctx, core.NestedControlOperationToken{}, redisTestSnapshot(1)); err != nil {
				t.Fatal(err)
			}
			key := store.Keys().Snapshot().String()
			mutate(client, key)
			before, err := client.HGetAll(ctx, key).Result()
			if err != nil {
				t.Fatal(err)
			}
			result, err := store.CompareAndSwap(ctx, core.NestedControlOperationToken{}, 1, redisTestSnapshot(2))
			if err == nil || result.Status == core.SnapshotCASCommitted {
				t.Fatalf("corrupt CAS result=%+v err=%v", result, err)
			}
			after, err := client.HGetAll(ctx, key).Result()
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("corrupt CAS overwrote envelope:\n before=%v\n after=%v", before, after)
			}
		})
	}
}

func TestRedisTransitionStoreRecoveryTombstoneReplayAndRestart(t *testing.T) {
	options := redisTestOptions(t)
	_, _, _, store := newRedisTestStores(t, options)
	recovery := testRecovery("transition-1", "fingerprint-1", false)
	if err := store.ReserveTransitionRecovery(context.Background(), recovery); err != nil {
		t.Fatal(err)
	}
	if err := store.ReserveTransitionRecovery(context.Background(), recovery); err != nil {
		t.Fatalf("same reserve not idempotent: %v", err)
	}
	loaded, found, err := store.LookupTransitionRecovery(context.Background(), recovery.TransitionID)
	if err != nil || !found || loaded.Fingerprint != recovery.Fingerprint || loaded.Committed {
		t.Fatalf("lookup=%+v found=%t err=%v", loaded, found, err)
	}
	all, err := store.LoadTransitionRecoveries(context.Background())
	if err != nil || len(all) != 1 {
		t.Fatalf("recoveries=%+v err=%v", all, err)
	}
	recovery.Committed = true
	recovery.UpdatedAt = time.UnixMilli(2000)
	if err := store.CommitTransitionRecovery(context.Background(), recovery); err != nil {
		t.Fatal(err)
	}
	conflict := recovery
	conflict.Fingerprint = "different"
	if err := store.CommitTransitionRecovery(context.Background(), conflict); !errors.Is(err, authority.ErrTransitionReconcile) {
		t.Fatalf("fingerprint conflict err=%v", err)
	}
	tombstone := authority.TransitionTombstone{
		TransitionID: recovery.TransitionID,
		Fingerprint:  recovery.Fingerprint,
		ExpiredAt:    time.UnixMilli(3000),
		Result:       recovery.Result,
	}
	if err := store.SaveTransitionTombstone(context.Background(), tombstone); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.LookupTransitionRecovery(context.Background(), recovery.TransitionID); err != nil || found {
		t.Fatalf("recovery not replaced found=%t err=%v", found, err)
	}
	gotTombstone, found, err := store.LookupTransitionTombstone(context.Background(), tombstone.TransitionID)
	if err != nil || !found || gotTombstone.Fingerprint != tombstone.Fingerprint {
		t.Fatalf("tombstone=%+v found=%t err=%v", gotTombstone, found, err)
	}
	restarted, err := NewTransitionStore(store.client, options)
	if err != nil {
		t.Fatal(err)
	}
	tombstones, err := restarted.LoadTransitionTombstones(context.Background())
	if err != nil || len(tombstones) != 1 || tombstones[0].TransitionID != tombstone.TransitionID {
		t.Fatalf("restart tombstones=%+v err=%v", tombstones, err)
	}
	if err := restarted.DeleteTransitionRecovery(context.Background(), recovery.TransitionID); err != nil {
		t.Fatalf("idempotent delete=%v", err)
	}
}

func TestRedisTransitionStoreHonorsCanceledCallerContext(t *testing.T) {
	options := redisTestOptions(t)
	_, _, _, store := newRedisTestStores(t, options)
	recovery := testRecovery("canceled-transition", "canceled-fingerprint", true)
	tombstone := authority.TransitionTombstone{TransitionID: "canceled-tombstone", Fingerprint: "canceled-tombstone-fingerprint", ExpiredAt: time.UnixMilli(2000)}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.LoadTransitionRecoveries(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("load recoveries err=%v", err)
	}
	if _, _, err := store.LookupTransitionRecovery(ctx, recovery.TransitionID); !errors.Is(err, context.Canceled) {
		t.Fatalf("lookup recovery err=%v", err)
	}
	if err := store.ReserveTransitionRecovery(ctx, recovery); !errors.Is(err, context.Canceled) {
		t.Fatalf("reserve err=%v", err)
	}
	if err := store.CommitTransitionRecovery(ctx, recovery); !errors.Is(err, context.Canceled) {
		t.Fatalf("commit err=%v", err)
	}
	if err := store.DeleteTransitionRecovery(ctx, recovery.TransitionID); !errors.Is(err, context.Canceled) {
		t.Fatalf("delete err=%v", err)
	}
	if _, err := store.LoadTransitionTombstones(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("load tombstones err=%v", err)
	}
	if _, _, err := store.LookupTransitionTombstone(ctx, tombstone.TransitionID); !errors.Is(err, context.Canceled) {
		t.Fatalf("lookup tombstone err=%v", err)
	}
	if err := store.SaveTransitionTombstone(ctx, tombstone); !errors.Is(err, context.Canceled) {
		t.Fatalf("save tombstone err=%v", err)
	}
}

func TestRedisTransitionLuaMalformedNeverLeavesPartialWrites(t *testing.T) {
	t.Run("delete-meta-corruption", func(t *testing.T) {
		options := redisTestOptions(t)
		_, client, _, store := newRedisTestStores(t, options)
		recovery := testRecovery("malformed-delete", "fp-delete", false)
		if err := store.ReserveTransitionRecovery(context.Background(), recovery); err != nil {
			t.Fatal(err)
		}
		before := captureTransitionState(t, client, store)
		if err := client.HSet(context.Background(), store.Keys().Meta().String(), metaFieldItems, "0").Err(); err != nil {
			t.Fatal(err)
		}
		reply, err := store.runTransition(context.Background(), ScriptDeleteTransitionRecovery, recovery.TransitionID,
			strconv.Itoa(options.MaxTransitionItems), strconv.Itoa(options.MaxTransitionBytes),
			strconv.Itoa(options.MaxTransitionRecordSize))
		if err != nil || firstReply(reply) != "MALFORMED" {
			t.Fatalf("delete malformed reply=%v err=%v", reply, err)
		}
		after := captureTransitionState(t, client, store)
		before.meta[metaFieldItems] = "0"
		assertTransitionStateUnchanged(t, before, after)
	})

	t.Run("commit-index-corruption", func(t *testing.T) {
		options := redisTestOptions(t)
		_, client, _, store := newRedisTestStores(t, options)
		recovery := testRecovery("malformed-commit", "fp-commit", false)
		if err := store.ReserveTransitionRecovery(context.Background(), recovery); err != nil {
			t.Fatal(err)
		}
		recovery.Committed = true
		value, err := encodeRecovery(recovery)
		if err != nil {
			t.Fatal(err)
		}
		before := captureTransitionState(t, client, store)
		if err := client.ZRem(context.Background(), store.Keys().RecoveryIndex().String(), recovery.TransitionID).Err(); err != nil {
			t.Fatal(err)
		}
		reply, err := store.runTransition(context.Background(), ScriptCommitTransitionRecovery, recovery.TransitionID,
			recovery.Fingerprint, value, scoreFor(recovery.UpdatedAt),
			strconv.Itoa(options.MaxTransitionItems), strconv.Itoa(options.MaxTransitionBytes),
			strconv.Itoa(options.MaxTransitionRecordSize))
		if err != nil || firstReply(reply) != "MALFORMED" {
			t.Fatalf("commit malformed reply=%v err=%v", reply, err)
		}
		after := captureTransitionState(t, client, store)
		before.recoveryIndex = nil
		assertTransitionStateUnchanged(t, before, after)
	})

	t.Run("tombstone-meta-corruption", func(t *testing.T) {
		options := redisTestOptions(t)
		_, client, _, store := newRedisTestStores(t, options)
		recovery := testRecovery("malformed-tombstone", "fp-tombstone", false)
		if err := store.ReserveTransitionRecovery(context.Background(), recovery); err != nil {
			t.Fatal(err)
		}
		tombstone := authority.TransitionTombstone{
			TransitionID: recovery.TransitionID,
			Fingerprint:  recovery.Fingerprint,
			ExpiredAt:    time.UnixMilli(3000),
			Result:       recovery.Result,
		}
		value, err := encodeTombstone(tombstone)
		if err != nil {
			t.Fatal(err)
		}
		before := captureTransitionState(t, client, store)
		if err := client.HSet(context.Background(), store.Keys().Meta().String(), metaFieldBytes, "0").Err(); err != nil {
			t.Fatal(err)
		}
		reply, err := store.runTransition(context.Background(), ScriptSaveTransitionTombstone, tombstone.TransitionID,
			tombstone.Fingerprint, value, strconv.Itoa(options.MaxTransitionItems),
			strconv.Itoa(options.MaxTransitionBytes), strconv.Itoa(options.MaxTransitionRecordSize),
			scoreFor(tombstone.ExpiredAt))
		if err != nil || firstReply(reply) != "MALFORMED" {
			t.Fatalf("tombstone malformed reply=%v err=%v", reply, err)
		}
		after := captureTransitionState(t, client, store)
		before.meta[metaFieldBytes] = "0"
		assertTransitionStateUnchanged(t, before, after)
	})
}

func TestRedisTransitionPhysicalIntegrityFailsClosedAndIDsStayDisjoint(t *testing.T) {
	t.Run("physical-memory-ceiling-precedes-materialization", func(t *testing.T) {
		countAt := strings.Index(luaTransitionCommon, "local recovery_count = redis.call('HLEN'")
		memoryAt := strings.Index(luaTransitionCommon, "if not memory_within(KEYS[1]")
		if countAt < 0 || memoryAt < 0 || countAt >= memoryAt {
			t.Fatalf("O(1) cardinality must precede exact MEMORY sampling: count=%d memory=%d", countAt, memoryAt)
		}
		if !strings.Contains(luaTransitionCommon, "redis.pcall('MEMORY', 'USAGE', key, 'SAMPLES', '0')") {
			t.Fatal("transition physical bound does not request exact MEMORY sampling")
		}
		for _, command := range []string{"redis.call('HKEYS'", "redis.call('HGET'", "redis.call('ZRANGE'"} {
			commandAt := strings.Index(luaTransitionCommon, command)
			if memoryAt < 0 || commandAt < 0 || memoryAt >= commandAt {
				t.Fatalf("MEMORY USAGE must precede %s: memory=%d command=%d", command, memoryAt, commandAt)
			}
		}
		if !strings.Contains(luaTransitionCommon, "max_items > math.floor((max_safe_integer - physical_base_overhead) / per_entry)") ||
			!strings.Contains(luaTransitionCommon, "max_bytes > math.floor((max_safe_integer - index_ceiling) / allocator_expansion)") ||
			!strings.Contains(luaTransitionCommon, "hash_ceiling = max_bytes * allocator_expansion + index_ceiling") {
			t.Fatal("transition physical ceiling arithmetic is not checked")
		}
	})

	t.Run("allocator-expansion-bound-accepts-maximum-options", func(t *testing.T) {
		options := redisTestOptions(t)
		options.MaxTransitionItems = maxTransitionItemsLimit
		options.MaxTransitionBytes = maxTransitionBytesLimit
		options.MaxTransitionRecordSize = maxTransitionRecordLimit
		_, _, _, store := newRedisTestStores(t, options)
		if _, err := store.LoadTransitionRecoveries(context.Background()); err != nil {
			t.Fatalf("checked maximum physical bounds rejected: %v", err)
		}
	})

	t.Run("exact-sampling-unsupported-above-five-fails-closed", func(t *testing.T) {
		options := redisTestOptions(t)
		options.MaxTransitionItems = 8
		options.MaxTransitionBytes = 2 << 20
		options.MaxTransitionRecordSize = 1 << 20
		_, client, _, store := newRedisTestStores(t, options)
		for index := 0; index < 6; index++ {
			id := fmt.Sprintf("skew-%d", index)
			value := "fingerprint\x00{}"
			if index == 5 {
				value = "fingerprint\x00" + strings.Repeat("v", 1<<20)
			}
			if err := client.HSet(context.Background(), store.Keys().Recoveries().String(), id, value).Err(); err != nil {
				t.Fatal(err)
			}
			if err := client.ZAdd(context.Background(), store.Keys().RecoveryIndex().String(), redis.Z{Score: float64(index + 1), Member: id}).Err(); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := store.LoadTransitionRecoveries(context.Background()); !errors.Is(err, ErrMalformedRecord) {
			t.Fatalf("unsupported exact sampling above fallback cardinality err=%v", err)
		}
	})

	t.Run("small-set-fallback-samples-entire-large-valid-record", func(t *testing.T) {
		options := redisTestOptions(t)
		options.MaxTransitionItems = 5
		options.MaxTransitionBytes = 1 << 20
		options.MaxTransitionRecordSize = 1 << 20
		_, _, _, store := newRedisTestStores(t, options)
		recovery := testRecovery("large-valid", "large-valid-fingerprint", false)
		recovery.Endpoint = strings.Repeat("e", 900<<10)
		if err := store.ReserveTransitionRecovery(context.Background(), recovery); err != nil {
			t.Fatal(err)
		}
		loaded, found, err := store.LookupTransitionRecovery(context.Background(), recovery.TransitionID)
		if err != nil || !found || loaded.Endpoint != recovery.Endpoint {
			t.Fatalf("large record found=%t bytes=%d err=%v", found, len(loaded.Endpoint), err)
		}
	})

	for _, corruption := range []string{"huge-id", "huge-value"} {
		t.Run(corruption, func(t *testing.T) {
			options := redisTestOptions(t)
			options.MaxTransitionItems = 4
			options.MaxTransitionBytes = 4 << 10
			options.MaxTransitionRecordSize = 2 << 10
			_, client, _, store := newRedisTestStores(t, options)
			id := "bounded-id"
			value := "fingerprint\x00{}"
			if corruption == "huge-id" {
				id = strings.Repeat("i", 1<<20)
			} else {
				value = "fingerprint\x00" + strings.Repeat("v", 1<<20)
			}
			if err := client.HSet(context.Background(), store.Keys().Recoveries().String(), id, value).Err(); err != nil {
				t.Fatal(err)
			}
			if err := client.ZAdd(context.Background(), store.Keys().RecoveryIndex().String(), redis.Z{Score: 1, Member: id}).Err(); err != nil {
				t.Fatal(err)
			}
			if _, err := store.LoadTransitionRecoveries(context.Background()); !errors.Is(err, ErrMalformedRecord) {
				t.Fatalf("physical corruption load err=%v", err)
			}
			if err := store.DeleteTransitionRecovery(context.Background(), "bounded-id"); !errors.Is(err, ErrMalformedRecord) {
				t.Fatalf("physical corruption mutation err=%v", err)
			}
		})
	}

	t.Run("wrong-type", func(t *testing.T) {
		options := redisTestOptions(t)
		_, client, _, store := newRedisTestStores(t, options)
		recovery := testRecovery("wrong-type", "wrong-type-fp", false)
		if err := store.ReserveTransitionRecovery(context.Background(), recovery); err != nil {
			t.Fatal(err)
		}
		before, err := client.HGetAll(context.Background(), store.Keys().Recoveries().String()).Result()
		if err != nil {
			t.Fatal(err)
		}
		if err := client.Del(context.Background(), store.Keys().Meta().String()).Err(); err != nil {
			t.Fatal(err)
		}
		if err := client.Set(context.Background(), store.Keys().Meta().String(), "not-a-hash", 0).Err(); err != nil {
			t.Fatal(err)
		}
		if _, err := store.LoadTransitionRecoveries(context.Background()); !errors.Is(err, ErrMalformedRecord) {
			t.Fatalf("wrong-type load err=%v", err)
		}
		if err := store.DeleteTransitionRecovery(context.Background(), recovery.TransitionID); !errors.Is(err, ErrMalformedRecord) {
			t.Fatalf("wrong-type mutation err=%v", err)
		}
		after, err := client.HGetAll(context.Background(), store.Keys().Recoveries().String()).Result()
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(before, after) {
			t.Fatalf("wrong-type mutation changed recovery=%v", after)
		}
	})

	t.Run("overlap-restart-and-mutation", func(t *testing.T) {
		options := redisTestOptions(t)
		_, client, _, store := newRedisTestStores(t, options)
		recovery := testRecovery("overlap", "overlap-fp", false)
		if err := store.ReserveTransitionRecovery(context.Background(), recovery); err != nil {
			t.Fatal(err)
		}
		tombstone := authority.TransitionTombstone{
			TransitionID: recovery.TransitionID,
			Fingerprint:  recovery.Fingerprint,
			ExpiredAt:    time.UnixMilli(3000),
			Result:       recovery.Result,
		}
		tombstoneValue, err := encodeTombstone(tombstone)
		if err != nil {
			t.Fatal(err)
		}
		recoveryValue, err := encodeRecovery(recovery)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.HSet(context.Background(), store.Keys().Tombstones().String(), tombstone.TransitionID, tombstoneValue).Err(); err != nil {
			t.Fatal(err)
		}
		if err := client.ZAdd(context.Background(), store.Keys().TombstoneIndex().String(), redis.Z{Score: 3000, Member: tombstone.TransitionID}).Err(); err != nil {
			t.Fatal(err)
		}
		if err := client.HSet(context.Background(), store.Keys().Meta().String(),
			metaFieldItems, "2", metaFieldBytes, strconv.Itoa(len(recoveryValue)+len(tombstoneValue))).Err(); err != nil {
			t.Fatal(err)
		}
		before := captureTransitionState(t, client, store)
		if _, err := store.LoadTransitionRecoveries(context.Background()); !errors.Is(err, ErrMalformedRecord) {
			t.Fatalf("overlap load err=%v", err)
		}
		restarted, err := NewTransitionStore(client, options)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := restarted.LoadTransitionTombstones(context.Background()); !errors.Is(err, ErrMalformedRecord) {
			t.Fatalf("overlap restart load err=%v", err)
		}
		observeCtx, cancelObserve := context.WithTimeout(context.Background(), time.Second)
		_, _, _, _, observeErr := restarted.ObserveTransitionEvidence(observeCtx, recovery.TransitionID)
		cancelObserve()
		if !errors.Is(observeErr, ErrMalformedRecord) {
			t.Fatalf("overlap exact observation err=%v", observeErr)
		}
		if err := store.DeleteTransitionRecovery(context.Background(), recovery.TransitionID); !errors.Is(err, ErrMalformedRecord) {
			t.Fatalf("overlap mutation err=%v", err)
		}
		after := captureTransitionState(t, client, store)
		assertTransitionStateUnchanged(t, before, after)
	})

	t.Run("invalid-id-record-fails-closed", func(t *testing.T) {
		invalidIDs := []struct {
			name  string
			value string
		}{
			{name: "invalid-leading", value: string([]byte{0xff})},
			{name: "overlong", value: string([]byte{0xc0, 0xaf})},
			{name: "surrogate", value: string([]byte{0xed, 0xa0, 0x80})},
			{name: "above-unicode-max", value: string([]byte{0xf4, 0x90, 0x80, 0x80})},
			{name: "invalid-continuation", value: string([]byte{0xe2, 0x28, 0xa1})},
			{name: "nbsp", value: "id\u00a0suffix"},
			{name: "c1-control", value: "id\u0080suffix"},
			{name: "ascii-control", value: "id\nsuffix"},
		}
		for _, invalid := range invalidIDs {
			t.Run(invalid.name, func(t *testing.T) {
				options := redisTestOptions(t)
				_, client, _, store := newRedisTestStores(t, options)
				recovery := testRecovery("semantic-id", "semantic-id-fp", false)
				if err := store.ReserveTransitionRecovery(context.Background(), recovery); err != nil {
					t.Fatal(err)
				}
				raw, err := encodeRecovery(recovery)
				if err != nil {
					t.Fatal(err)
				}
				if err := client.HDel(context.Background(), store.Keys().Recoveries().String(), recovery.TransitionID).Err(); err != nil {
					t.Fatal(err)
				}
				if err := client.HSet(context.Background(), store.Keys().Recoveries().String(), invalid.value, raw).Err(); err != nil {
					t.Fatal(err)
				}
				if err := client.ZRem(context.Background(), store.Keys().RecoveryIndex().String(), recovery.TransitionID).Err(); err != nil {
					t.Fatal(err)
				}
				if err := client.ZAdd(context.Background(), store.Keys().RecoveryIndex().String(),
					redis.Z{Score: float64(recovery.UpdatedAt.UnixMilli()), Member: invalid.value}).Err(); err != nil {
					t.Fatal(err)
				}
				before := captureTransitionState(t, client, store)
				if _, err := store.LoadTransitionRecoveries(context.Background()); !errors.Is(err, ErrMalformedRecord) {
					t.Fatalf("invalid ID load err=%v", err)
				}
				if err := store.DeleteTransitionRecovery(context.Background(), recovery.TransitionID); !errors.Is(err, ErrMalformedRecord) {
					t.Fatalf("invalid ID mutation err=%v", err)
				}
				after := captureTransitionState(t, client, store)
				assertTransitionStateUnchanged(t, before, after)
			})
		}
	})

	t.Run("noncanonical-record-fails-closed", func(t *testing.T) {
		options := redisTestOptions(t)
		_, client, _, store := newRedisTestStores(t, options)
		recovery := testRecovery("semantic-json", "semantic-json-fp", false)
		if err := store.ReserveTransitionRecovery(context.Background(), recovery); err != nil {
			t.Fatal(err)
		}
		raw, err := encodeRecovery(recovery)
		if err != nil {
			t.Fatal(err)
		}
		raw = append(raw, ' ')
		if err := client.HSet(context.Background(), store.Keys().Recoveries().String(),
			recovery.TransitionID, raw).Err(); err != nil {
			t.Fatal(err)
		}
		if err := client.HSet(context.Background(), store.Keys().Meta().String(),
			metaFieldBytes, strconv.Itoa(len(raw))).Err(); err != nil {
			t.Fatal(err)
		}
		before := captureTransitionState(t, client, store)
		if _, err := store.LoadTransitionRecoveries(context.Background()); !errors.Is(err, ErrMalformedRecord) {
			t.Fatalf("noncanonical load err=%v", err)
		}
		if err := store.DeleteTransitionRecovery(context.Background(), recovery.TransitionID); !errors.Is(err, ErrMalformedRecord) {
			t.Fatalf("noncanonical mutation err=%v", err)
		}
		after := captureTransitionState(t, client, store)
		assertTransitionStateUnchanged(t, before, after)
	})
}

func TestRedisTransitionStoreCapacityItemsAndBytes(t *testing.T) {
	options := redisTestOptions(t)
	options.MaxTransitionItems = 1
	options.MaxTransitionBytes = 16 << 10
	options.MaxTransitionRecordSize = 8 << 10
	_, _, _, store := newRedisTestStores(t, options)
	first := testRecovery("one", "fp-one", false)
	if err := store.ReserveTransitionRecovery(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	second := testRecovery("two", "fp-two", false)
	if err := store.ReserveTransitionRecovery(context.Background(), second); !errors.Is(err, authority.ErrTransitionTombstoneCap) {
		t.Fatalf("item capacity err=%v", err)
	}
	byteFirst := testRecovery("byte-one", "fp-byte-one", false)
	byteSecond := testRecovery("byte-two", strings.Repeat("f", 96), false)
	byteFirstValue, err := encodeRecovery(byteFirst)
	if err != nil {
		t.Fatal(err)
	}
	byteSecondValue, err := encodeRecovery(byteSecond)
	if err != nil {
		t.Fatal(err)
	}
	options.MaxTransitionItems = 4
	options.MaxTransitionBytes = len(byteFirstValue) + len(byteSecondValue) - 1
	options.MaxTransitionRecordSize = len(byteSecondValue)
	_, _, _, smallStore := newRedisTestStores(t, options)
	if err := smallStore.ReserveTransitionRecovery(context.Background(), byteFirst); err != nil {
		t.Fatalf("byte first recovery err=%v", err)
	}
	if err := smallStore.ReserveTransitionRecovery(context.Background(), byteSecond); !errors.Is(err, authority.ErrTransitionTombstoneCap) {
		t.Fatalf("byte capacity err=%v", err)
	}
}

func TestRedisTransitionConcurrentReserveSameIdentity(t *testing.T) {
	options := redisTestOptions(t)
	_, _, _, store := newRedisTestStores(t, options)
	recovery := testRecovery("concurrent-reserve", "concurrent-fingerprint", false)
	const workers = 32
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			errs <- store.ReserveTransitionRecovery(context.Background(), recovery)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	entries, err := store.LoadTransitionRecoveries(context.Background())
	if err != nil || len(entries) != 1 || entries[0].TransitionID != recovery.TransitionID {
		t.Fatalf("concurrent recoveries=%+v err=%v", entries, err)
	}
}

func TestRedisTransitionConcurrentReserveDifferentIDs(t *testing.T) {
	options := redisTestOptions(t)
	options.MaxTransitionItems = 128
	_, _, _, store := newRedisTestStores(t, options)
	// Miniredis does not implement MEMORY USAGE SAMPLES 0. The production
	// script permits default sampling only when its five samples cover the
	// complete collection, so this concurrency fixture remains at that bound.
	const workers = 5
	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			errs <- store.ReserveTransitionRecovery(context.Background(), testRecovery(
				fmt.Sprintf("concurrent-reserve-%d", i),
				fmt.Sprintf("concurrent-fingerprint-%d", i),
				false,
			))
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	entries, err := store.LoadTransitionRecoveries(context.Background())
	if err != nil || len(entries) != workers {
		t.Fatalf("concurrent recoveries=%d entries=%+v err=%v", len(entries), entries, err)
	}
}

func TestRedisTransitionConcurrentValidOperationsNeverTear(t *testing.T) {
	options := redisTestOptions(t)
	// Full physical validation is intentionally O(bounded set) per operation;
	// leave enough headroom for the race detector's instrumentation while the
	// production default remains independently bounded.
	options.OperationTimeout = 10 * time.Second
	options.MaxTransitionItems = 128
	_, _, _, store := newRedisTestStores(t, options)
	// Two records per identity keep the Miniredis-only exact-sampling fallback
	// within the contract's complete-collection limit of five.
	const ids = 2
	const workers = 48
	const iterations = 6

	recoveries := make([]authority.TransitionRecovery, ids)
	tombstones := make([]authority.TransitionTombstone, ids)
	for i := 0; i < ids; i++ {
		recoveries[i] = testRecovery(
			fmt.Sprintf("concurrent-recovery-%d", i),
			fmt.Sprintf("concurrent-recovery-fingerprint-%d", i),
			false,
		)
		if err := store.ReserveTransitionRecovery(context.Background(), recoveries[i]); err != nil {
			t.Fatal(err)
		}
		tombstones[i] = authority.TransitionTombstone{
			TransitionID: fmt.Sprintf("concurrent-tombstone-%d", i),
			Fingerprint:  fmt.Sprintf("concurrent-tombstone-fingerprint-%d", i),
			ExpiredAt:    time.UnixMilli(int64(3000 + i)),
			Result:       recoveries[i].Result,
		}
		if err := store.SaveTransitionTombstone(context.Background(), tombstones[i]); err != nil {
			t.Fatal(err)
		}
	}

	type result struct {
		operation string
		err       error
	}
	start := make(chan struct{})
	results := make(chan result, workers*iterations)
	var wg sync.WaitGroup
	wg.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func(worker int) {
			defer wg.Done()
			<-start
			for round := 0; round < iterations; round++ {
				index := (worker + round) % ids
				switch (worker + round) % 8 {
				case 0:
					results <- result{
						operation: "reserve",
						err:       store.ReserveTransitionRecovery(context.Background(), recoveries[index]),
					}
				case 1:
					_, err := store.LoadTransitionRecoveries(context.Background())
					results <- result{operation: "load-recoveries", err: err}
				case 2:
					_, _, err := store.LookupTransitionRecovery(context.Background(), recoveries[index].TransitionID)
					results <- result{operation: "lookup-recovery", err: err}
				case 3:
					committed := recoveries[index]
					committed.Committed = true
					committed.UpdatedAt = time.UnixMilli(int64(4000 + index))
					results <- result{
						operation: "commit",
						err:       store.CommitTransitionRecovery(context.Background(), committed),
					}
				case 4:
					results <- result{
						operation: "delete",
						err:       store.DeleteTransitionRecovery(context.Background(), recoveries[index].TransitionID),
					}
				case 5:
					results <- result{
						operation: "save-tombstone",
						err:       store.SaveTransitionTombstone(context.Background(), tombstones[index]),
					}
				case 6:
					_, err := store.LoadTransitionTombstones(context.Background())
					results <- result{operation: "load-tombstones", err: err}
				case 7:
					_, _, err := store.LookupTransitionTombstone(context.Background(), tombstones[index].TransitionID)
					results <- result{operation: "lookup-tombstone", err: err}
				}
			}
		}(worker)
	}
	close(start)
	wg.Wait()
	close(results)

	for result := range results {
		if result.err == nil {
			continue
		}
		if errors.Is(result.err, ErrMalformedRecord) {
			t.Fatalf("%s reported false MALFORMED: %v", result.operation, result.err)
		}
		if result.operation == "commit" && errors.Is(result.err, authority.ErrTransitionReconcile) {
			continue
		}
		t.Fatalf("%s failed during valid concurrent operations: %v", result.operation, result.err)
	}
	if _, err := store.LoadTransitionRecoveries(context.Background()); err != nil {
		t.Fatalf("final recoveries load tore: %v", err)
	}
	if _, err := store.LoadTransitionTombstones(context.Background()); err != nil {
		t.Fatalf("final tombstones load tore: %v", err)
	}
}

func TestRedisTransitionStoreUsesBoundedTimeoutWithoutContextSeam(t *testing.T) {
	namespace, err := rediskey.NewNamespace("test", "timeout")
	if err != nil {
		t.Fatal(err)
	}
	options := DefaultOptions(namespace)
	options.OperationTimeout = 20 * time.Millisecond
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = client.Close() })
	store, err := NewTransitionStore(client, options)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	_, _, err = store.LookupTransitionTombstone(context.Background(), "timeout-id")
	if err == nil {
		t.Fatal("unreachable Redis unexpectedly succeeded")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("unbounded transition operation elapsed=%s", elapsed)
	}
}

func TestRedisExactTransitionObservationIsReadOnlyBoundedAndReloadsNoScript(t *testing.T) {
	options := redisTestOptions(t)
	_, client, _, store := newRedisTestStores(t, options)
	recovery := testRecovery("exact-observation", "exact-observation-fingerprint", false)
	if err := store.ReserveTransitionRecovery(context.Background(), recovery); err != nil {
		t.Fatal(err)
	}
	recovery.Committed = true
	if err := store.CommitTransitionRecovery(context.Background(), recovery); err != nil {
		t.Fatal(err)
	}
	before := captureTransitionState(t, client, store)
	observe := func() (authority.TransitionRecovery, bool, authority.TransitionTombstone, bool, error) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		return store.ObserveTransitionEvidence(ctx, recovery.TransitionID)
	}
	loaded, recoveryFound, _, tombstoneFound, err := observe()
	if err != nil || !recoveryFound || tombstoneFound || loaded.TransitionID != recovery.TransitionID {
		t.Fatalf("recovery observation=%+v recovery_found=%t tombstone_found=%t err=%v", loaded, recoveryFound, tombstoneFound, err)
	}
	noscriptServer := miniredis.RunT(t)
	noscriptClient := redis.NewClient(&redis.Options{Addr: noscriptServer.Addr()})
	t.Cleanup(func() { _ = noscriptClient.Close() })
	restarted, err := NewTransitionStore(noscriptClient, options)
	if err != nil {
		t.Fatal(err)
	}
	noscriptCtx, noscriptCancel := context.WithTimeout(context.Background(), time.Second)
	_, recoveryFound, _, tombstoneFound, err = restarted.ObserveTransitionEvidence(noscriptCtx, "noscript-exact-observation")
	noscriptCancel()
	if err != nil || recoveryFound || tombstoneFound {
		t.Fatalf("NOSCRIPT reload observation recovery_found=%t tombstone_found=%t err=%v", recoveryFound, tombstoneFound, err)
	}
	after := captureTransitionState(t, client, store)
	assertTransitionStateUnchanged(t, before, after)

	tombstone := authority.TransitionTombstone{
		TransitionID: recovery.TransitionID, Fingerprint: recovery.Fingerprint, Result: recovery.Result,
		ExpiredAt: time.UnixMilli(5000),
	}
	if err := store.SaveTransitionTombstone(context.Background(), tombstone); err != nil {
		t.Fatal(err)
	}
	_, recoveryFound, loadedTombstone, tombstoneFound, err := observe()
	if err != nil || recoveryFound || !tombstoneFound || loadedTombstone.TransitionID != recovery.TransitionID {
		t.Fatalf("tombstone observation=%+v recovery_found=%t tombstone_found=%t err=%v", loadedTombstone, recoveryFound, tombstoneFound, err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, _, _, err := store.ObserveTransitionEvidence(canceled, recovery.TransitionID); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled exact observation err=%v", err)
	}
	if strings.Contains(luaObserveTransitionEvidence, "HGETALL") || strings.Contains(luaObserveTransitionEvidence, "SCRIPT FLUSH") ||
		strings.Contains(luaObserveTransitionEvidence, "redis.call('KEYS'") || strings.Contains(luaObserveTransitionEvidence, "redis.call('SCAN'") {
		t.Fatal("exact observation contains a global/materializing/forbidden operation")
	}
}

// TestLiveRedisExactTransitionObservation proves the exact transition script
// against the explicitly enabled company Redis. It owns one unique route-
// control namespace, enumerates only its five known physical keys, and proves
// exact cleanup without KEYS, SCAN, or SCRIPT FLUSH.
func TestLiveRedisExactTransitionObservation(t *testing.T) {
	if os.Getenv("FARM_REDIS_ROUTE_CONTROL_LIVE_TEST") != "1" {
		t.Skip("FARM_REDIS_ROUTE_CONTROL_LIVE_TEST is not enabled")
	}
	uri := os.Getenv("FARM_REDIS_URL")
	if uri == "" {
		t.Fatal("FARM_REDIS_URL is required")
	}
	redisOptions, err := redis.ParseURL(uri)
	if err != nil {
		t.Fatal("parse company Redis URL")
	}
	client := redis.NewClient(redisOptions)
	t.Cleanup(func() { _ = client.Close() })
	namespace, err := rediskey.NewNamespace(fmt.Sprintf("rt2wc3%d", time.Now().UnixNano()), "probe")
	if err != nil {
		t.Fatal(err)
	}
	options := DefaultOptions(namespace)
	options.OperationTimeout = 10 * time.Second
	store, err := NewTransitionStore(client, options)
	if err != nil {
		t.Fatal(err)
	}
	exactKeys := []string{
		store.Keys().Meta().String(), store.Keys().Recoveries().String(), store.Keys().RecoveryIndex().String(),
		store.Keys().Tombstones().String(), store.Keys().TombstoneIndex().String(),
	}
	cleanup := func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = client.Del(cleanupCtx, exactKeys...).Err()
	}
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if exists, err := client.Exists(ctx, exactKeys...).Result(); err != nil || exists != 0 {
		t.Fatalf("isolated exact transition namespace unavailable: exists=%d err=%v", exists, err)
	}
	if err := store.EnsureLoaded(); err != nil {
		t.Fatal(err)
	}
	recovery := testRecovery("live-exact-observation", "route-transition-fingerprint-v1:live", false)
	if err := store.ReserveTransitionRecovery(context.Background(), recovery); err != nil {
		t.Fatal(err)
	}
	recovery.Committed = true
	if err := store.CommitTransitionRecovery(context.Background(), recovery); err != nil {
		t.Fatal(err)
	}
	loaded, recoveryFound, _, tombstoneFound, err := store.ObserveTransitionEvidence(ctx, recovery.TransitionID)
	if err != nil || !recoveryFound || tombstoneFound || loaded != recovery {
		t.Fatalf("exact recovery observation found=%t tombstone=%t equal=%t err=%v", recoveryFound, tombstoneFound, loaded == recovery, err)
	}
	tombstone := authority.TransitionTombstone{TransitionID: recovery.TransitionID, Fingerprint: recovery.Fingerprint, Result: recovery.Result, ExpiredAt: time.Now().Add(time.Minute)}
	if err := store.SaveTransitionTombstone(context.Background(), tombstone); err != nil {
		t.Fatal(err)
	}
	_, recoveryFound, loadedTombstone, tombstoneFound, err := store.ObserveTransitionEvidence(ctx, recovery.TransitionID)
	if err != nil || recoveryFound || !tombstoneFound || loadedTombstone.TransitionID != recovery.TransitionID {
		t.Fatalf("exact tombstone observation recovery=%t found=%t err=%v", recoveryFound, tombstoneFound, err)
	}
	if deleted, err := client.Del(ctx, exactKeys...).Result(); err != nil || deleted == 0 {
		t.Fatalf("exact transition cleanup deleted=%d err=%v", deleted, err)
	}
	if exists, err := client.Exists(ctx, exactKeys...).Result(); err != nil || exists != 0 {
		t.Fatalf("exact transition cleanup EXISTS=%d err=%v", exists, err)
	}
	t.Log("exact company Redis transition observation passed cleanup_EXISTS=0")
}

func TestRedisRouteControlKeysAndScriptsStayScoped(t *testing.T) {
	namespace, err := rediskey.NewNamespace("test", "golden")
	if err != nil {
		t.Fatal(err)
	}
	keys, err := NewKeyBuilderV1(namespace)
	if err != nil {
		t.Fatal(err)
	}
	all := []rediskey.RedisKey{keys.Snapshot(), keys.Meta(), keys.Recoveries(), keys.RecoveryIndex(), keys.Tombstones(), keys.TombstoneIndex()}
	if err := keys.AssertSameSlot(all...); err != nil {
		t.Fatal(err)
	}
	if got, want := keys.Snapshot().String(), "{classic-farm:test:golden:route-control:v1}:snapshot"; got != want {
		t.Fatalf("snapshot key=%q want=%q", got, want)
	}
	for _, def := range builtinScripts() {
		upper := strings.ToUpper(def.source)
		if strings.Contains(upper, "REDIS.CALL('KEYS'") ||
			strings.Contains(upper, "REDIS.CALL('SCAN'") ||
			strings.Contains(upper, "SCRIPT FLUSH") {
			t.Fatalf("forbidden Redis command in %s", def.name)
		}
	}
}

func TestRedisTransitionIdentityPreservesCanonicalUTF8V1(t *testing.T) {
	namespace, err := rediskey.NewNamespace("test", "utf8")
	if err != nil {
		t.Fatal(err)
	}
	keys, err := NewKeyBuilderV1(namespace)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"transition-1", "01234567-89ab-cdef-0123-456789abcdef", "迁移-一", "move-🚜"} {
		if err := keys.ValidateTransitionID(value); err != nil {
			t.Fatalf("canonical UTF-8 transition ID rejected: %q: %v", value, err)
		}
	}
	for _, value := range []string{string([]byte{0xff, 0xfe}), "id\u00a0suffix", "id\u0085suffix", "id\u0080suffix", "id\nsuffix"} {
		if err := keys.ValidateTransitionID(value); !errors.Is(err, ErrInvalidKey) {
			t.Fatalf("invalid UTF-8/control/space transition ID %q error=%v", value, err)
		}
	}
	for _, value := range []string{"sha256:0123456789abcdef", "指纹-一", "fingerprint-✅"} {
		if err := validateTransitionFingerprint(value); err != nil {
			t.Fatalf("canonical UTF-8 fingerprint rejected: %q: %v", value, err)
		}
	}
	for _, value := range []string{string([]byte{0xff}), "fp\u00a0suffix", "fp\u0085suffix", "fp\u0080suffix", "fp\tsuffix"} {
		if err := validateTransitionFingerprint(value); !errors.Is(err, ErrFingerprint) {
			t.Fatalf("invalid UTF-8/control/space fingerprint %q error=%v", value, err)
		}
	}
	for _, contract := range []string{
		"first >= 194 and first <= 223",
		"first == 224 and second < 160",
		"first == 237 and second > 159",
		"first == 244 and second > 143",
		"codepoint >= 127 and codepoint <= 159",
		"codepoint >= 8192 and codepoint <= 8202",
	} {
		if !strings.Contains(luaTransitionCommon, contract) {
			t.Fatalf("Lua canonical UTF-8 contract missing %q", contract)
		}
	}

	options := redisTestOptions(t)
	_, _, _, store := newRedisTestStores(t, options)
	for _, identity := range []struct{ id, fingerprint string }{
		{id: "迁移-一", fingerprint: "指纹-一"},
		{id: "move-🚜", fingerprint: "fingerprint-✅"},
	} {
		recovery := testRecovery(identity.id, identity.fingerprint, false)
		if err := store.ReserveTransitionRecovery(context.Background(), recovery); err != nil {
			t.Fatalf("reserve canonical UTF-8 identity=%q: %v", identity.id, err)
		}
		loaded, found, err := store.LookupTransitionRecovery(context.Background(), identity.id)
		if err != nil || !found || loaded.TransitionID != identity.id || loaded.Fingerprint != identity.fingerprint {
			t.Fatalf("canonical UTF-8 roundtrip id=%q found=%t loaded=%+v err=%v", identity.id, found, loaded, err)
		}
	}
}

func TestTransitionRecordCanonicalRoundTrip(t *testing.T) {
	recovery := testRecovery("canonical-recovery", "canonical-fingerprint", true)
	rawRecovery, err := encodeRecovery(recovery)
	if err != nil {
		t.Fatal(err)
	}
	decodedRecovery, err := decodeRecovery(recovery.TransitionID, string(rawRecovery))
	if err != nil || decodedRecovery != recovery {
		t.Fatalf("recovery round trip=%+v err=%v", decodedRecovery, err)
	}
	tombstone := authority.TransitionTombstone{
		TransitionID: recovery.TransitionID,
		Fingerprint:  recovery.Fingerprint,
		ExpiredAt:    time.UnixMilli(3000),
		Result:       recovery.Result,
	}
	rawTombstone, err := encodeTombstone(tombstone)
	if err != nil {
		t.Fatal(err)
	}
	decodedTombstone, err := decodeTombstone(tombstone.TransitionID, string(rawTombstone))
	if err != nil || !reflect.DeepEqual(decodedTombstone, tombstone) {
		t.Fatalf("tombstone round trip=%+v err=%v", decodedTombstone, err)
	}
}

func TestTransitionRecoveryCodecPreservesLegacyAttemptBoundary(t *testing.T) {
	recovery := testRecovery("legacy-attempt", "legacy-attempt-fingerprint", false)
	raw, err := encodeRecovery(recovery)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"authority_attempted":false`) || strings.Contains(string(raw), `"AuthorityAttempted"`) {
		t.Fatalf("current recovery omitted explicit attempt boundary: %s", raw)
	}
	current, err := decodeRecovery(recovery.TransitionID, string(raw))
	if err != nil || !current.AuthorityAttemptedPresent || current.RecoveryWireVersion != authority.TransitionRecoveryWireAuthorityAttempted {
		t.Fatalf("current boundary=%+v err=%v", current, err)
	}
	legacyWire := strings.Replace(string(raw), `,"authority_attempted":false`, "", 1)
	if legacyWire == string(raw) {
		t.Fatal("failed to construct legacy recovery wire")
	}
	legacy, err := decodeRecovery(recovery.TransitionID, legacyWire)
	if err != nil {
		t.Fatal(err)
	}
	if legacy.AuthorityAttempted || legacy.AuthorityAttemptedPresent || legacy.RecoveryWireVersion != authority.TransitionRecoveryWireLegacy {
		t.Fatalf("legacy absence was not preserved: %+v", legacy)
	}
	attempted := recovery
	attempted.AuthorityAttempted = true
	attemptedRaw, err := encodeRecovery(attempted)
	if err != nil {
		t.Fatal(err)
	}
	legacyAttemptedWire := strings.Replace(string(attemptedRaw), `"authority_attempted":true`, `"AuthorityAttempted":true`, 1)
	legacyAttempted, err := decodeRecovery(recovery.TransitionID, legacyAttemptedWire)
	if err != nil || !legacyAttempted.AuthorityAttempted || !legacyAttempted.AuthorityAttemptedPresent || legacyAttempted.RecoveryWireVersion != authority.TransitionRecoveryWireLegacy {
		t.Fatalf("legacy attempted boundary=%+v err=%v", legacyAttempted, err)
	}
}

func TestTransitionRecoveryCodecKeepsOldNonExactBytesAndCarriesExactIdentity(t *testing.T) {
	legacy := testRecovery("legacy-identity-wire", "route-transition-fingerprint-v1:abc", false)
	raw, err := encodeRecovery(legacy)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"SourcePodUID", "SourceWorkloadUID", "SourceOrdinal", "TargetPodUID", "TargetWorkloadUID", "TargetOrdinal", "DeploymentIdentityPresent"} {
		if strings.Contains(string(raw), field) {
			t.Fatalf("legacy wire gained %s: %s", field, raw)
		}
	}
	decoded, err := decodeRecovery(legacy.TransitionID, string(raw))
	if err != nil || decoded.SourcePodUID != "" || decoded.DeploymentIdentityPresent {
		t.Fatalf("legacy decoded=%+v err=%v", decoded, err)
	}
	exact := legacy
	exact.Fingerprint = "route-transition-fingerprint-v2:def"
	exact.SourcePodUID, exact.SourceWorkloadUID, exact.SourceOrdinal = "pod-a", "workload", 0
	exact.TargetPodUID, exact.TargetWorkloadUID, exact.TargetOrdinal = "pod-b", "workload", 1
	exact.DeploymentIdentityPresent = true
	exactRaw, err := encodeRecovery(exact)
	if err != nil {
		t.Fatal(err)
	}
	exactDecoded, err := decodeRecovery(exact.TransitionID, string(exactRaw))
	if err != nil || !reflect.DeepEqual(exactDecoded, exact) {
		t.Fatalf("exact decoded=%+v want=%+v err=%v", exactDecoded, exact, err)
	}
	targetOnly := legacy
	targetOnly.Fingerprint = "route-transition-fingerprint-v2:target-only"
	targetOnly.TargetPodUID, targetOnly.TargetWorkloadUID, targetOnly.TargetOrdinal = "pod-b", "workload", 1
	targetOnly.DeploymentIdentityPresent = true
	targetOnlyRaw, err := encodeRecovery(targetOnly)
	if err != nil {
		t.Fatal(err)
	}
	targetOnlyDecoded, err := decodeRecovery(targetOnly.TransitionID, string(targetOnlyRaw))
	if err != nil || !reflect.DeepEqual(targetOnlyDecoded, targetOnly) {
		t.Fatalf("target-only decoded=%+v want=%+v err=%v", targetOnlyDecoded, targetOnly, err)
	}
}

func TestRedisRouteControlLuaSourcesParse(t *testing.T) {
	for _, def := range builtinScripts() {
		luaState := lua.NewState()
		_, err := luaState.Load(strings.NewReader(def.source), def.name)
		luaState.Close()
		if err != nil {
			t.Fatalf("%s does not parse: %v", def.name, err)
		}
	}
}

func testRecovery(id, fingerprint string, committed bool) authority.TransitionRecovery {
	now := time.UnixMilli(1000)
	return authority.TransitionRecovery{
		TransitionID:              id,
		Fingerprint:               fingerprint,
		Domain:                    actorrouting.DomainPlayer,
		VirtualShard:              1,
		Committed:                 committed,
		AuthorityAttemptedPresent: true,
		RecoveryWireVersion:       authority.TransitionRecoveryWireAuthorityAttempted,
		CreatedAt:                 now,
		UpdatedAt:                 now,
		LastState:                 actorrouting.ShardStateAssigning,
	}
}
