package session

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"farmgame/internal/storage/redis/rediskey"
)

func newTestRedisAuthority(t *testing.T, ttl time.Duration) (*miniredis.Miniredis, *redis.Client, *RedisAuthority) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	authority := NewRedis(client, Config{
		Secret:         []byte("epoch-cache-test-secret"),
		AuthorityEpoch: 1,
		EpochCacheTTL:  ttl,
	})
	return server, client, authority
}

type redisCommandLog struct {
	mu       sync.Mutex
	commands []string
}

func (h *redisCommandLog) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *redisCommandLog) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		h.mu.Lock()
		h.commands = append(h.commands, cmd.Name())
		h.mu.Unlock()
		return err
	}
}

func (h *redisCommandLog) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, commands []redis.Cmder) error {
		err := next(ctx, commands)
		h.mu.Lock()
		for _, cmd := range commands {
			h.commands = append(h.commands, cmd.Name())
		}
		h.mu.Unlock()
		return err
	}
}

func (h *redisCommandLog) reset() {
	h.mu.Lock()
	h.commands = nil
	h.mu.Unlock()
}

func (h *redisCommandLog) snapshot() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.commands...)
}

type redisBenchmarkCommandCounts struct {
	commands           atomic.Int64
	evalSHA            atomic.Int64
	eval               atomic.Int64
	requestBytes       atomic.Int64
	legacyRequestBytes atomic.Int64
	pipelines          atomic.Int64
}

func (h *redisBenchmarkCommandCounts) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *redisBenchmarkCommandCounts) record(commands []redis.Cmder) bool {
	workload := false
	for _, command := range commands {
		switch command.Name() {
		case "evalsha":
			h.commands.Add(1)
			h.evalSHA.Add(1)
			h.requestBytes.Add(redisRequestBytes(command.Args()))
			h.legacyRequestBytes.Add(redisRenewLegacyEvalRequestBytes(command.Args()))
			workload = true
		case "eval":
			h.commands.Add(1)
			h.eval.Add(1)
			h.requestBytes.Add(redisRequestBytes(command.Args()))
			workload = true
		}
	}
	return workload
}

func redisRequestBytes(args []interface{}) int64 {
	bytes := int64(1 + len(strconv.Itoa(len(args))) + 2)
	for _, arg := range args {
		value := fmt.Sprint(arg)
		bytes += int64(1 + len(strconv.Itoa(len(value))) + 2 + len(value) + 2)
	}
	return bytes
}

func redisRenewLegacyEvalRequestBytes(args []interface{}) int64 {
	if len(args) < 2 {
		return 0
	}
	legacy := append([]interface{}(nil), args...)
	legacy[0] = "eval"
	legacy[1] = renewRouteScript
	return redisRequestBytes(legacy)
}

func (h *redisBenchmarkCommandCounts) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, command redis.Cmder) error {
		err := next(ctx, command)
		h.record([]redis.Cmder{command})
		return err
	}
}

func (h *redisBenchmarkCommandCounts) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, commands []redis.Cmder) error {
		err := next(ctx, commands)
		if h.record(commands) {
			h.pipelines.Add(1)
		}
		return err
	}
}

func TestRedisAuthorityRaisesGlobalEpochWithAtomicExactMax(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	log := &redisCommandLog{}
	client.AddHook(log)
	key := rediskey.SessionAuthorityEpoch()

	cold := NewRedis(client, Config{Secret: []byte("authority-max-cold"), AuthorityEpoch: 7})
	if got, err := cold.AuthorityEpoch(ctx); err != nil || got != 7 {
		t.Fatalf("cold authority epoch=%d want=7 err=%v", got, err)
	}
	if got := log.snapshot(); !reflect.DeepEqual(got, []string{"evalsha", "eval"}) {
		t.Fatalf("cold atomic max commands=%v want [evalsha eval]", got)
	}
	if stored, err := client.Get(ctx, key).Result(); err != nil || stored != "7" {
		t.Fatalf("initialized authority epoch=%q want=7 err=%v", stored, err)
	}

	log.reset()
	lower := NewRedis(client, Config{Secret: []byte("authority-max-lower"), AuthorityEpoch: 3})
	if got, err := lower.AuthorityEpoch(ctx); err != nil || got != 7 {
		t.Fatalf("lower authority epoch=%d want=7 err=%v", got, err)
	}
	if got := log.snapshot(); !reflect.DeepEqual(got, []string{"evalsha"}) {
		t.Fatalf("warm atomic max commands=%v want exactly [evalsha]", got)
	}

	const maximum = ^uint64(0)
	higher := NewRedis(client, Config{Secret: []byte("authority-max-higher"), AuthorityEpoch: maximum})
	if got, err := higher.AuthorityEpoch(ctx); err != nil || got != maximum {
		t.Fatalf("higher authority epoch=%d want=%d err=%v", got, uint64(maximum), err)
	}
	if stored, err := client.Get(ctx, key).Result(); err != nil || stored != strconv.FormatUint(maximum, 10) {
		t.Fatalf("exact uint64 authority epoch=%q want=%d err=%v", stored, uint64(maximum), err)
	}

	if err := client.Set(ctx, key, "not-an-epoch", 0).Err(); err != nil {
		t.Fatal(err)
	}
	malformed := NewRedis(client, Config{Secret: []byte("authority-max-malformed"), AuthorityEpoch: 9})
	if _, err := malformed.AuthorityEpoch(ctx); err == nil {
		t.Fatal("malformed stored authority epoch was overwritten")
	}
	if stored, err := client.Get(ctx, key).Result(); err != nil || stored != "not-an-epoch" {
		t.Fatalf("malformed authority epoch changed to %q err=%v", stored, err)
	}
}

func TestRedisAuthorityConcurrentConfiguredEpochsConvergeToExactMax(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	configured := []uint64{1, 99, 7, 1 << 53, 1<<53 + 1, ^uint64(0) - 1, ^uint64(0)}
	start := make(chan struct{})
	errs := make(chan error, len(configured))
	var wg sync.WaitGroup
	for index, epoch := range configured {
		wg.Add(1)
		go func(index int, epoch uint64) {
			defer wg.Done()
			<-start
			authority := NewRedis(client, Config{
				Secret: []byte("authority-max-concurrent-" + strconv.Itoa(index)), AuthorityEpoch: epoch,
			})
			_, err := authority.AuthorityEpoch(ctx)
			errs <- err
		}(index, epoch)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent authority max: %v", err)
		}
	}
	want := strconv.FormatUint(^uint64(0), 10)
	if got, err := client.Get(ctx, rediskey.SessionAuthorityEpoch()).Result(); err != nil || got != want {
		t.Fatalf("concurrent authority max=%q want=%q err=%v", got, want, err)
	}
}

func TestRedisAuthorityPreservesUint64EpochsAcrossMixedVersionLifecycle(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	const (
		playerID       uint64 = 8_880
		doubleBoundary        = uint64(1 << 53)
		belowBoundary         = doubleBoundary - 1
		aboveBoundary         = doubleBoundary + 1
	)
	authority := NewRedis(client, Config{
		Secret: []byte("uint64-mixed-version-secret"), AuthorityEpoch: aboveBoundary,
	})
	key := rediskey.Session(playerID)

	// Model a rolling-upgrade hash: legacy `epoch` is present without
	// `active_epoch`, and decimal fields may still contain leading zeroes.
	if err := client.HSet(ctx, key,
		"authority_epoch", "000"+strconv.FormatUint(aboveBoundary, 10),
		"epoch", "000"+strconv.FormatUint(belowBoundary, 10),
		"next_epoch", "000"+strconv.FormatUint(doubleBoundary, 10),
		"route_revision", "000"+strconv.FormatUint(doubleBoundary, 10),
	).Err(); err != nil {
		t.Fatal(err)
	}
	candidate, err := authority.Reserve(ctx, playerID, "mixed-version-session")
	if err != nil {
		t.Fatal(err)
	}
	if candidate.ActiveSessionEpoch != belowBoundary ||
		candidate.ReservedSessionEpoch != aboveBoundary ||
		candidate.AuthorityEpoch != aboveBoundary {
		t.Fatalf("mixed-version candidate=%+v, want active=%d reserved/authority=%d",
			candidate, belowBoundary, aboveBoundary)
	}

	// Keep only the legacy active field and place it exactly at 2^53. The
	// reserved 2^53+1 must remain distinct even in real Redis Lua 5.1.
	if err := client.HDel(ctx, key, "active_epoch").Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(ctx, key, "epoch", "000"+strconv.FormatUint(doubleBoundary, 10)).Err(); err != nil {
		t.Fatal(err)
	}
	activated, err := authority.Activate(ctx, ActivationRequest{
		PlayerID: playerID, SessionID: candidate.SessionID,
		AuthorityEpoch: candidate.AuthorityEpoch, ReservedSessionEpoch: candidate.ReservedSessionEpoch,
		Route: RouteFence{
			GatewayID: "uint64-gate", GatewayBootID: "uint64-boot",
			ConnectionID: "uint64-connection", ConnectionGeneration: 1,
		},
		LeaseTTL: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if activated.Route.SessionEpoch != aboveBoundary || activated.Route.RouteRevision != aboveBoundary {
		t.Fatalf("high activation route=%+v, want session/revision=%d", activated.Route, aboveBoundary)
	}
	fields, err := client.HGetAll(ctx, key).Result()
	if err != nil {
		t.Fatal(err)
	}
	if fields["active_epoch"] != strconv.FormatUint(aboveBoundary, 10) ||
		fields["epoch"] != strconv.FormatUint(aboveBoundary, 10) ||
		fields["route_revision"] != strconv.FormatUint(aboveBoundary, 10) {
		t.Fatalf("activation did not store canonical exact uint64 fences: %+v", fields)
	}

	// 2^53 and 2^53+1 compare unequal. A stale conditional revoke must be a
	// pure no-op rather than collapsing both values through a Lua double.
	before := fields
	if revoked, err := authority.RevokeIfCurrent(ctx, playerID, doubleBoundary); err != nil || revoked {
		t.Fatalf("neighbor authority epoch revoked=%v err=%v, want false/nil", revoked, err)
	}
	after, err := client.HGetAll(ctx, key).Result()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("stale high-epoch revoke changed hash: before=%v after=%v", before, after)
	}

	if revoked, err := authority.RevokeIfCurrent(ctx, playerID, aboveBoundary); err != nil || !revoked {
		t.Fatalf("exact high authority revoke=%v err=%v, want true/nil", revoked, err)
	}
	fields, err = client.HGetAll(ctx, key).Result()
	if err != nil {
		t.Fatal(err)
	}
	wantNext := strconv.FormatUint(aboveBoundary+1, 10)
	if fields["authority_epoch"] != wantNext || fields["active_epoch"] != wantNext ||
		fields["epoch"] != wantNext || fields["next_epoch"] != wantNext {
		t.Fatalf("high revoke did not increment exact decimal strings to %s: %+v", wantNext, fields)
	}
}

func TestRedisAuthorityMaxUint64IncrementFailsBeforeWrites(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	authority := NewRedis(client, Config{Secret: []byte("uint64-overflow-secret"), AuthorityEpoch: 1})
	const maximum = ^uint64(0)
	maximumText := strconv.FormatUint(maximum, 10)
	beforeMaximumText := strconv.FormatUint(maximum-1, 10)

	assertUnchanged := func(t *testing.T, key string, before map[string]string, beforeTTL time.Duration) {
		t.Helper()
		after, err := client.HGetAll(ctx, key).Result()
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(after, before) {
			t.Fatalf("overflow changed hash: before=%v after=%v", before, after)
		}
		if afterTTL := server.TTL(key); afterTTL != beforeTTL {
			t.Fatalf("overflow changed TTL: before=%s after=%s", beforeTTL, afterTTL)
		}
	}

	t.Run("session epoch", func(t *testing.T) {
		const playerID uint64 = 8_881
		key := rediskey.Session(playerID)
		if err := client.HSet(ctx, key,
			"authority_epoch", "1", "active_epoch", beforeMaximumText,
			"epoch", beforeMaximumText, "next_epoch", beforeMaximumText,
			"route_revision", "0",
		).Err(); err != nil {
			t.Fatal(err)
		}
		candidate, err := authority.Reserve(ctx, playerID, "maximum-session")
		if err != nil || candidate.ReservedSessionEpoch != maximum {
			t.Fatalf("last reservable epoch candidate=%+v err=%v", candidate, err)
		}
		activated, err := authority.Activate(ctx, ActivationRequest{
			PlayerID: playerID, SessionID: candidate.SessionID,
			AuthorityEpoch: candidate.AuthorityEpoch, ReservedSessionEpoch: candidate.ReservedSessionEpoch,
			Route: RouteFence{
				GatewayID: "maximum-gate", GatewayBootID: "maximum-boot",
				ConnectionID: "maximum-connection", ConnectionGeneration: 1,
			},
			LeaseTTL: time.Second,
		})
		if err != nil || activated.Route.SessionEpoch != maximum {
			t.Fatalf("activate MaxUint64 candidate=%+v err=%v", activated, err)
		}
		if err := client.PExpire(ctx, key, 37*time.Minute).Err(); err != nil {
			t.Fatal(err)
		}
		before, err := client.HGetAll(ctx, key).Result()
		if err != nil {
			t.Fatal(err)
		}
		beforeTTL := server.TTL(key)
		if _, err := authority.Reserve(ctx, playerID, "overflow-reserve"); err == nil {
			t.Fatal("reserve incremented MaxUint64")
		}
		assertUnchanged(t, key, before, beforeTTL)
		if err := authority.Revoke(ctx, playerID); err == nil {
			t.Fatal("revoke incremented MaxUint64 session epoch")
		}
		assertUnchanged(t, key, before, beforeTTL)
	})

	t.Run("authority epoch", func(t *testing.T) {
		const playerID uint64 = 8_882
		key := rediskey.Session(playerID)
		if err := client.HSet(ctx, key,
			"authority_epoch", maximumText, "active_epoch", "7", "epoch", "7", "next_epoch", "7",
		).Err(); err != nil {
			t.Fatal(err)
		}
		if err := client.PExpire(ctx, key, 37*time.Minute).Err(); err != nil {
			t.Fatal(err)
		}
		before, err := client.HGetAll(ctx, key).Result()
		if err != nil {
			t.Fatal(err)
		}
		beforeTTL := server.TTL(key)
		if err := authority.Revoke(ctx, playerID); err == nil {
			t.Fatal("revoke incremented MaxUint64 authority epoch")
		}
		assertUnchanged(t, key, before, beforeTTL)
	})

	t.Run("route revision", func(t *testing.T) {
		const playerID uint64 = 8_883
		key := rediskey.Session(playerID)
		if err := client.HSet(ctx, key,
			"authority_epoch", "1", "active_epoch", "1", "epoch", "1",
			"next_epoch", "2", "route_revision", maximumText,
		).Err(); err != nil {
			t.Fatal(err)
		}
		if err := client.PExpire(ctx, key, 37*time.Minute).Err(); err != nil {
			t.Fatal(err)
		}
		before, err := client.HGetAll(ctx, key).Result()
		if err != nil {
			t.Fatal(err)
		}
		beforeTTL := server.TTL(key)
		if _, err := authority.Activate(ctx, ActivationRequest{
			PlayerID: playerID, SessionID: "overflow-revision-session",
			AuthorityEpoch: 1, ReservedSessionEpoch: 2,
			Route: RouteFence{
				GatewayID: "revision-gate", GatewayBootID: "revision-boot",
				ConnectionID: "revision-connection", ConnectionGeneration: 1,
			},
			LeaseTTL: time.Second,
		}); err == nil {
			t.Fatal("activation incremented MaxUint64 route revision")
		}
		assertUnchanged(t, key, before, beforeTTL)
	})
}

func TestRedisAuthorityCachesGlobalEpochWithoutPlayerTTL(t *testing.T) {
	server, _, authority := newTestRedisAuthority(t, 0)
	ctx := context.Background()

	first, err := authority.AuthorityEpoch(ctx)
	if err != nil {
		t.Fatalf("first authority epoch: %v", err)
	}
	if first != 1 {
		t.Fatalf("first authority epoch=%d, want 1", first)
	}
	commandsAfterLoad := server.CommandCount()

	second, err := authority.AuthorityEpoch(ctx)
	if err != nil {
		t.Fatalf("cached authority epoch: %v", err)
	}
	if second != first {
		t.Fatalf("cached authority epoch=%d, want %d", second, first)
	}
	if server.CommandCount() != commandsAfterLoad {
		t.Fatalf("global epoch cache issued %d extra redis commands", server.CommandCount()-commandsAfterLoad)
	}

	if err := server.Set(rediskey.SessionAuthorityEpoch(), "99"); err != nil {
		t.Fatalf("mutate global epoch: %v", err)
	}
	got, err := authority.AuthorityEpoch(ctx)
	if err != nil {
		t.Fatalf("authority epoch after redis mutate: %v", err)
	}
	if got != first {
		t.Fatalf("process-lifetime cache lost after redis mutate: got=%d want=%d", got, first)
	}
}

func TestRedisAuthorityCachesGlobalEpochUnderConcurrentReaders(t *testing.T) {
	server, _, authority := newTestRedisAuthority(t, 0)
	ctx := context.Background()
	const goroutines = 32

	gotCh := make(chan uint64, goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			got, err := authority.AuthorityEpoch(ctx)
			if err != nil {
				t.Errorf("concurrent authority epoch: %v", err)
			}
			gotCh <- got
		}()
	}
	for i := 0; i < goroutines; i++ {
		if got := <-gotCh; got != 1 {
			t.Fatalf("concurrent authority epoch=%d, want 1", got)
		}
	}
	commandsAfterWarmup := server.CommandCount()
	for i := 0; i < goroutines; i++ {
		got, err := authority.AuthorityEpoch(ctx)
		if err != nil {
			t.Fatalf("post-warmup authority epoch: %v", err)
		}
		if got != 1 {
			t.Fatalf("post-warmup authority epoch=%d, want 1", got)
		}
	}
	if extra := server.CommandCount() - commandsAfterWarmup; extra != 0 {
		t.Fatalf("warm global epoch cache issued %d extra redis commands", extra)
	}
}

func TestRedisAuthorityRoundsPositiveSubMillisecondRouteTTL(t *testing.T) {
	_, client, authority := newTestRedisAuthority(t, 0)
	ctx := context.Background()
	const playerID uint64 = 8_901
	candidate, err := authority.Reserve(ctx, playerID, "sub-ms-session")
	if err != nil {
		t.Fatal(err)
	}
	activated, err := authority.Activate(ctx, ActivationRequest{
		PlayerID: playerID, SessionID: candidate.SessionID,
		AuthorityEpoch: candidate.AuthorityEpoch, ReservedSessionEpoch: candidate.ReservedSessionEpoch,
		Route: RouteFence{
			GatewayID: "sub-ms-gate", GatewayBootID: "sub-ms-boot",
			ConnectionID: "sub-ms-connection", ConnectionGeneration: 1,
		},
		LeaseTTL: time.Nanosecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if activated.AcceptedLeaseTTL != time.Millisecond {
		t.Fatalf("activate accepted TTL=%s want 1ms", activated.AcceptedLeaseTTL)
	}
	redisNow, err := client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(ctx, rediskey.Session(playerID),
		"route_lease_expire_at_ms", redisNow.Add(time.Second).UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	renewals, err := authority.RenewRoutes(ctx, []ActiveRoute{activated.Route}, time.Nanosecond)
	if err != nil || len(renewals) != 1 || !renewals[0].Renewed {
		t.Fatalf("sub-ms renewal=%+v err=%v", renewals, err)
	}
	if renewals[0].AcceptedLeaseTTL != time.Millisecond {
		t.Fatalf("renew accepted TTL=%s want 1ms", renewals[0].AcceptedLeaseTTL)
	}
}

func TestRedisAuthorityPlayerEpochCacheStaysOffWhenTTLZero(t *testing.T) {
	_, client, authority := newTestRedisAuthority(t, 0)
	ctx := context.Background()
	const playerID uint64 = 9001

	session, err := authority.Begin(ctx, playerID, "gate-a")
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	active, err := authority.ActiveEpoch(ctx, playerID)
	if err != nil {
		t.Fatalf("active epoch: %v", err)
	}
	if active != session.ActiveSessionEpoch {
		t.Fatalf("active epoch=%d, want %d", active, session.ActiveSessionEpoch)
	}

	bumped := active + 1
	if err := client.HSet(ctx, rediskey.Session(playerID), "active_epoch", bumped).Err(); err != nil {
		t.Fatalf("bump active epoch: %v", err)
	}
	got, err := authority.ActiveEpoch(ctx, playerID)
	if err != nil {
		t.Fatalf("active epoch after bump: %v", err)
	}
	if got != bumped {
		t.Fatalf("player epoch cache must stay off when TTL=0: got=%d want=%d", got, bumped)
	}
}

func TestRedisAuthorityPermitUsesOneFreshEpochSnapshotCommand(t *testing.T) {
	server, client, authority := newTestRedisAuthority(t, time.Minute)
	commandLog := &redisCommandLog{}
	client.AddHook(commandLog)
	ctx := context.Background()
	const playerID uint64 = 9010

	activeSession, err := authority.Begin(ctx, playerID, "gate-a")
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	// Even when the compatibility epoch cache is populated, command admission
	// must bypass it so a cross-Gate replacement is visible immediately.
	if _, err := authority.ActiveEpoch(ctx, playerID); err != nil {
		t.Fatalf("warm active cache: %v", err)
	}
	if _, err := authority.PlayerAuthorityEpoch(ctx, playerID); err != nil {
		t.Fatalf("warm authority cache: %v", err)
	}
	request := PermitRequest{RequestID: "one-snapshot", Action: ActionReadOnly}
	commandLog.reset()
	before := server.CommandCount()
	if _, err := authority.Permit(ctx, activeSession, request); err != nil {
		t.Fatalf("permit: %v", err)
	}
	if commands := server.CommandCount() - before; commands != 1 {
		t.Fatalf("permit Redis commands=%d, want exactly 1 HMGET", commands)
	}
	if got := commandLog.snapshot(); !reflect.DeepEqual(got, []string{"hmget"}) {
		t.Fatalf("permit command vector=%v, want exactly [hmget]", got)
	}

	if err := client.HSet(ctx, rediskey.Session(playerID), "active_epoch", activeSession.ActiveSessionEpoch+1).Err(); err != nil {
		t.Fatalf("replace active epoch: %v", err)
	}
	commandLog.reset()
	before = server.CommandCount()
	if _, err := authority.Permit(ctx, activeSession, request); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("replaced session permit err=%v, want ErrStaleSession", err)
	}
	if commands := server.CommandCount() - before; commands != 1 {
		t.Fatalf("stale permit Redis commands=%d, want exactly 1 fresh HMGET", commands)
	}
	if got := commandLog.snapshot(); !reflect.DeepEqual(got, []string{"hmget"}) {
		t.Fatalf("stale permit command vector=%v, want exactly [hmget]", got)
	}

	const revokedPlayerID uint64 = 9011
	revokedSession, err := authority.Begin(ctx, revokedPlayerID, "gate-a")
	if err != nil {
		t.Fatalf("begin authority-fence session: %v", err)
	}
	if _, err := authority.ActiveEpoch(ctx, revokedPlayerID); err != nil {
		t.Fatalf("warm authority-fence active cache: %v", err)
	}
	if _, err := authority.PlayerAuthorityEpoch(ctx, revokedPlayerID); err != nil {
		t.Fatalf("warm authority-fence cache: %v", err)
	}
	if err := client.HSet(ctx, rediskey.Session(revokedPlayerID),
		"authority_epoch", revokedSession.AuthorityEpoch+1).Err(); err != nil {
		t.Fatalf("replace player authority epoch: %v", err)
	}
	commandLog.reset()
	before = server.CommandCount()
	if _, err := authority.Permit(ctx, revokedSession, request); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("revoked session permit err=%v, want ErrStaleSession", err)
	}
	if commands := server.CommandCount() - before; commands != 1 {
		t.Fatalf("revoked permit Redis commands=%d, want exactly 1 fresh HMGET", commands)
	}
	if got := commandLog.snapshot(); !reflect.DeepEqual(got, []string{"hmget"}) {
		t.Fatalf("revoked permit command vector=%v, want exactly [hmget]", got)
	}
}

func TestRedisAuthorityRouteHeartbeatUsesWarmSHAAndExactNoScriptFallback(t *testing.T) {
	_, client, authority := newTestRedisAuthority(t, 0)
	ctx := context.Background()
	active, err := authority.Begin(ctx, 9_019, "gate-heartbeat")
	if err != nil {
		t.Fatal(err)
	}
	routes, err := authority.BatchGetActiveRoutes(ctx, []uint64{active.PlayerID})
	if err != nil {
		t.Fatal(err)
	}
	route, ok := routes[active.PlayerID]
	if !ok {
		t.Fatal("active route missing")
	}
	log := &redisCommandLog{}
	client.AddHook(log)

	if results, err := authority.RenewRoutes(ctx, []ActiveRoute{route}, DefaultRouteLeaseTTL); err != nil ||
		len(results) != 1 || !results[0].Renewed {
		t.Fatalf("cold heartbeat=%+v err=%v", results, err)
	}
	if got, want := log.snapshot(), []string{"evalsha", "eval"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("cold heartbeat commands=%v want=%v", got, want)
	}

	log.reset()
	if results, err := authority.RenewRoutes(ctx, []ActiveRoute{route}, DefaultRouteLeaseTTL); err != nil ||
		len(results) != 1 || !results[0].Renewed {
		t.Fatalf("warm heartbeat=%+v err=%v", results, err)
	}
	if got, want := log.snapshot(), []string{"evalsha"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("warm heartbeat commands=%v want=%v", got, want)
	}
}

func BenchmarkRedisAuthorityPermitCommandCount(b *testing.B) {
	server := miniredis.RunT(b)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	b.Cleanup(func() { _ = client.Close() })
	authority := NewRedis(client, Config{
		Secret: []byte("permit-command-count-benchmark"), AuthorityEpoch: 1,
	})
	activeSession, err := authority.Begin(context.Background(), 9020, "gate-benchmark")
	if err != nil {
		b.Fatal(err)
	}
	request := PermitRequest{RequestID: "benchmark", Action: ActionReadOnly}
	if _, err := authority.Permit(context.Background(), activeSession, request); err != nil {
		b.Fatal(err)
	}
	before := server.CommandCount()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := authority.Permit(context.Background(), activeSession, request); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	commands := server.CommandCount() - before
	if commands != b.N {
		b.Fatalf("Redis commands=%d, want exactly %d", commands, b.N)
	}
	b.ReportMetric(float64(commands)/float64(b.N), "redis_commands/op")
}

func BenchmarkRedisAuthorityRenewRoutes20KPreloaded(b *testing.B) {
	const routeCount = 20_000
	server := miniredis.RunT(b)
	client := redis.NewClient(&redis.Options{Addr: server.Addr(), PoolSize: 1})
	b.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	authority := NewRedis(client, Config{
		Secret: []byte("route-heartbeat-20k-benchmark"), AuthorityEpoch: 1,
	})
	if err := authority.EnsureScriptsLoaded(ctx); err != nil {
		b.Fatal(err)
	}
	if _, err := authority.AuthorityEpoch(ctx); err != nil {
		b.Fatal(err)
	}

	routes := make([]ActiveRoute, routeCount)
	leaseExpiresAt := time.Now().Add(10 * time.Minute)
	_, err := client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for index := range routes {
			playerID := uint64(100_000 + index)
			route := ActiveRoute{
				RouteFence: RouteFence{
					GatewayID: "gate-benchmark", GatewayBootID: "boot-benchmark",
					ConnectionID: "connection-" + strconv.Itoa(index), ConnectionGeneration: 1,
				},
				PlayerID: playerID, SessionID: "session-" + strconv.Itoa(index),
				SessionEpoch: 1, RouteRevision: 1, LeaseExpiresAt: leaseExpiresAt,
			}
			routes[index] = route
			key := rediskey.Session(playerID)
			pipe.HSet(ctx, key,
				"authority_epoch", 1, "active_epoch", route.SessionEpoch, "epoch", route.SessionEpoch,
				"session_id", route.SessionID,
				"route_gate_id", route.GatewayID, "route_gate_boot_id", route.GatewayBootID,
				"route_connection_id", route.ConnectionID,
				"route_connection_generation", route.ConnectionGeneration,
				"route_revision", route.RouteRevision,
				"route_lease_expire_at_ms", leaseExpiresAt.UnixMilli(),
			)
			pipe.PExpire(ctx, key, DefaultSessionTTL)
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}

	counts := &redisBenchmarkCommandCounts{}
	client.AddHook(counts)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		renewals, renewErr := authority.RenewRoutes(ctx, routes, DefaultRouteLeaseTTL)
		if renewErr != nil || len(renewals) != routeCount {
			b.Fatalf("renewals=%d want=%d err=%v", len(renewals), routeCount, renewErr)
		}
		for index := range renewals {
			if !renewals[index].Renewed || renewals[index].Transient || renewals[index].LeaseExpired {
				b.Fatalf("renewal[%d]=%+v", index, renewals[index])
			}
		}
	}
	b.StopTimer()

	wantCommands := int64(b.N * routeCount)
	if got := counts.commands.Load(); got != wantCommands {
		b.Fatalf("Redis commands=%d want=%d", got, wantCommands)
	}
	if got := counts.evalSHA.Load(); got != wantCommands {
		b.Fatalf("EVALSHA commands=%d want=%d", got, wantCommands)
	}
	if got := counts.eval.Load(); got != 0 {
		b.Fatalf("full-body EVAL commands=%d want=0", got)
	}
	requestBytesPerRoute := float64(counts.requestBytes.Load()) / float64(b.N*routeCount)
	legacyEvalBytesPerRoute := float64(counts.legacyRequestBytes.Load()) / float64(b.N*routeCount)
	b.ReportMetric(float64(wantCommands)/float64(b.N), "redis_commands/op")
	b.ReportMetric(float64(wantCommands)/float64(b.N*routeCount), "redis_commands/route")
	b.ReportMetric(legacyEvalBytesPerRoute, "legacy_eval_request_bytes/route")
	b.ReportMetric(requestBytesPerRoute, "redis_request_bytes/route")
	b.ReportMetric(float64(counts.pipelines.Load())/float64(b.N), "redis_round_trips/op")
	b.ReportMetric(routeCount, "routes/op")
}

func TestRedisAuthorityPlayerEpochCacheHonorsTTL(t *testing.T) {
	_, client, authority := newTestRedisAuthority(t, time.Minute)
	ctx := context.Background()
	const playerID uint64 = 9002

	session, err := authority.Begin(ctx, playerID, "gate-a")
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	active, err := authority.ActiveEpoch(ctx, playerID)
	if err != nil {
		t.Fatalf("active epoch: %v", err)
	}
	if active != session.ActiveSessionEpoch {
		t.Fatalf("active epoch=%d, want %d", active, session.ActiveSessionEpoch)
	}

	if err := client.HSet(ctx, rediskey.Session(playerID), "active_epoch", active+1).Err(); err != nil {
		t.Fatalf("bump active epoch: %v", err)
	}
	got, err := authority.ActiveEpoch(ctx, playerID)
	if err != nil {
		t.Fatalf("cached active epoch: %v", err)
	}
	if got != active {
		t.Fatalf("player epoch cache with TTL must ignore redis bump: got=%d want=%d", got, active)
	}
}
