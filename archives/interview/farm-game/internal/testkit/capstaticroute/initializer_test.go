package capstaticroute

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
	redisauthority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func testConfig(t *testing.T, shardCount uint32) Config {
	t.Helper()
	namespace, err := rediskey.NewNamespace(fmt.Sprintf("l2r2b%04d", shardCount), "z1")
	if err != nil {
		t.Fatal(err)
	}
	profileID := "functional"
	nonce := "a1b2c3d4e5f60718293a4b5c"
	generation := uint64(1)
	holder := func(id, endpoint, domain string) Holder {
		return Holder{ID: id, BootID: fmt.Sprintf("%s-%s-g%d-%s", id, profileID, generation, nonce), Endpoint: endpoint, Domain: domain}
	}
	return Config{
		RunID: "run-b-static", ProfileID: profileID, Generation: generation, ControllerNonce: nonce,
		Realm: "classic-farm-local-l2-r2", HashSeed: 202608030001, ShardCount: shardCount,
		LeaseTTL: 60 * time.Second, MinimumRemainingLease: 30 * time.Second,
		Profile: rediskey.PhysicalProfile{
			ProfileVersion: "redis-physical-v1", Namespace: namespace, AuthorityLayoutVersion: "v1",
			WorkflowLayoutVersion: "v1", WorkflowHashVersion: 1, WorkflowBucketCount: 256,
		},
		Holders: []Holder{
			holder("player-svr-0", "player-svr-0.player-svr-headless.farm-l2-r2.svc.cluster.local:9100", "PLAYER"),
			holder("player-svr-1", "player-svr-1.player-svr-headless.farm-l2-r2.svc.cluster.local:9100", "PLAYER"),
			holder("farm-svr-0", "farm-svr-0.farm-svr-headless.farm-l2-r2.svc.cluster.local:9090", "FARM"),
			holder("farm-svr-1", "farm-svr-1.farm-svr-headless.farm-l2-r2.svc.cluster.local:9090", "FARM"),
		},
		RouteManagerID: "cap-static-route-init", RouteManagerBootID: "cap-static-route-init-run-b-static",
	}
}

type realRig struct {
	server      *miniredis.Miniredis
	client      *redis.Client
	store       *redisauthority.Store
	reader      *redisauthority.AuthorityReader
	initializer *Initializer
}

func newRealRig(t *testing.T, shardCount uint32) *realRig {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	cfg := testConfig(t, shardCount)
	calculator, err := cfg.Calculator()
	if err != nil {
		t.Fatal(err)
	}
	registry, err := redisauthority.NewScriptRegistry(client)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.EnsureLoaded(context.Background()); err != nil {
		t.Fatal(err)
	}
	store, err := redisauthority.NewStore(client, registry, redisauthority.Options{
		Profile: cfg.Profile, Resolver: calculator, LeaseLimits: authoritystore.DefaultLeaseLimits(),
	})
	if err != nil {
		t.Fatal(err)
	}
	readerOptions := redisauthority.DefaultAuthorityReaderOptions(cfg.Profile, calculator)
	readerOptions.OperationTimeout = 5 * time.Second
	reader, err := redisauthority.NewShardAuthorityReader(client, readerOptions)
	if err != nil {
		t.Fatal(err)
	}
	return &realRig{server: server, client: client, store: store, reader: reader, initializer: &Initializer{Store: store, Reader: reader, Config: cfg}}
}

func TestInitializeFourByFourAndIdempotentRerun(t *testing.T) {
	rig := newRealRig(t, 4)
	first, err := rig.initializer.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assertComplete(t, first, 4)
	if len(rig.server.Keys()) != 8 {
		t.Fatalf("Redis keys=%d want 8", len(rig.server.Keys()))
	}
	second, err := rig.initializer.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assertComplete(t, second, 4)
	if second.Receipt.Counters.ExistingExact != 8 || second.Receipt.Counters.ClaimApplied != 0 {
		t.Fatalf("rerun counters=%+v", second.Receipt.Counters)
	}
	if first.Receipt.RouteSnapshotSHA256 == "" || second.Receipt.RouteSnapshotSHA256 == "" ||
		first.Receipt.ExactLedgerSHA256 == "" || second.Receipt.ExactLedgerSHA256 == "" {
		t.Fatal("missing route snapshot or ledger hash")
	}
}

func TestInitialize1024By1024Completeness(t *testing.T) {
	if testing.Short() {
		t.Skip("1024/1024 completeness is not a short test")
	}
	rig := newRealRig(t, 1024)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	started := time.Now()
	result, err := rig.initializer.Run(ctx)
	finished := time.Now()
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := finished.Sub(started); elapsed >= rig.initializer.Config.LeaseTTL-rig.initializer.Config.MinimumRemainingLease {
		t.Fatalf("initial claim duration %s consumed startup budget", elapsed)
	}
	if remaining := time.UnixMilli(result.Receipt.MinimumLeaseUntilMs).Sub(finished); remaining < rig.initializer.Config.MinimumRemainingLease {
		t.Fatalf("minimum remaining lease=%s", remaining)
	}
	assertComplete(t, result, 1024)
	if got := len(rig.server.Keys()); got != 2048 {
		t.Fatalf("Redis keys=%d want 2048", got)
	}
	if result.Receipt.Counters.ClaimApplied != 2048 || result.Receipt.Counters.Verified != 2048 {
		t.Fatalf("counters=%+v", result.Receipt.Counters)
	}
	if len(result.Ledger.Records) != 2048 || result.Ledger.ExpectedRecordCount != 2048 {
		t.Fatalf("ledger records=%d expected=%d", len(result.Ledger.Records), result.Ledger.ExpectedRecordCount)
	}
	slots := map[uint16]bool{}
	for index, record := range result.Ledger.Records {
		expectedDomain := actorrouting.DomainPlayer
		expectedShard := uint32(index)
		if index >= 1024 {
			expectedDomain = actorrouting.DomainFarm
			expectedShard = uint32(index - 1024)
		}
		if record.Domain != expectedDomain || record.VirtualShard != expectedShard ||
			!strings.HasPrefix(record.RedisKey, "{classic-farm:l2r2b1024:z1:") || len(record.RecordSHA256) != 64 || len(record.TransitionDigest) != 64 ||
			record.AuthorityLayoutVersion != "v1" || record.HashVersion != actorrouting.HashVersionXXH64V2 ||
			record.HashSeed != rig.initializer.Config.HashSeed || record.TransitionID != rig.initializer.Config.TransitionID(expectedDomain, expectedShard) ||
			record.LeaseState != "ACTIVE" || record.LeaseUntilMs <= finished.UnixMilli() {
			t.Fatalf("record %d not exact: %+v", index, record)
		}
		slots[rediskey.ClusterSlot(rediskey.RedisKey(record.RedisKey))] = true
	}
	if len(slots) < 1000 {
		t.Fatalf("only %d distinct slots; possible global hashtag collapse", len(slots))
	}
	calculator, err := rig.initializer.Config.Calculator()
	if err != nil {
		t.Fatal(err)
	}
	keys, err := rediskey.NewAuthorityKeys(rig.initializer.Config.Profile, calculator)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := rig.initializer.Config.Profile.AuthorityBucketForShard(calculator, actorrouting.DomainPlayer, 0)
	if err != nil {
		t.Fatal(err)
	}
	authorityKey, _ := keys.ShardAuthority(bucket)
	bucketMetaKey, _ := keys.BucketMeta(bucket)
	if err := keys.AssertSameSlot(authorityKey, bucketMetaKey); err != nil {
		t.Fatalf("same bucket does not share slot: %v", err)
	}
}

func TestPartialAndTimeoutUnknownNeverPublish(t *testing.T) {
	for _, status := range []authoritystore.ShardAuthorityTransitionStatus{
		authoritystore.ShardTransitionUnavailableNotSent,
		authoritystore.ShardTransitionTimeoutUnknown,
	} {
		t.Run(status.String(), func(t *testing.T) {
			rig := newRealRig(t, 4)
			fault := &faultStore{delegate: rig.store, failDomain: actorrouting.DomainPlayer, failShard: 2, status: status}
			initializer := &Initializer{Store: fault, Reader: rig.reader, Config: rig.initializer.Config}
			result, err := initializer.Run(context.Background())
			if err == nil || result.Receipt.Activatable || result.Receipt.Complete || len(result.Bytes) != 0 || len(result.LedgerBytes) != 0 {
				t.Fatalf("partial result err=%v receipt=%+v bytes=%d ledger=%d", err, result.Receipt, len(result.Bytes), len(result.LedgerBytes))
			}
			if status == authoritystore.ShardTransitionTimeoutUnknown && result.Receipt.FailureClass != "AUTHORITY_COMMIT_UNKNOWN" {
				t.Fatalf("failure class=%s", result.Receipt.FailureClass)
			}
		})
	}
}

func TestExistingHolderAndBootMismatchReject(t *testing.T) {
	for _, mutate := range []func(*Config){
		func(cfg *Config) {
			cfg.Holders[0].ID = "player-svr-replaced"
			cfg.Holders[0].BootID = fmt.Sprintf("%s-%s-g%d-%s", cfg.Holders[0].ID, cfg.ProfileID, cfg.Generation, cfg.ControllerNonce)
		},
		func(cfg *Config) {
			cfg.ControllerNonce = "b1b2c3d4e5f60718293a4b5c"
			for index := range cfg.Holders {
				cfg.Holders[index].BootID = fmt.Sprintf("%s-%s-g%d-%s", cfg.Holders[index].ID, cfg.ProfileID, cfg.Generation, cfg.ControllerNonce)
			}
		},
	} {
		rig := newRealRig(t, 4)
		if _, err := rig.initializer.Run(context.Background()); err != nil {
			t.Fatal(err)
		}
		mismatch := rig.initializer.Config
		mismatch.Holders = append([]Holder(nil), mismatch.Holders...)
		mutate(&mismatch)
		result, err := (&Initializer{Store: rig.store, Reader: rig.reader, Config: mismatch}).Run(context.Background())
		if !errors.Is(err, ErrIdentityConflict) || result.Receipt.Activatable {
			t.Fatalf("identity mismatch err=%v receipt=%+v", err, result.Receipt)
		}
	}
}

func TestLayoutAndHashMismatchReject(t *testing.T) {
	for _, field := range []struct{ name, value string }{{"authority_layout_version", "v2"}, {"hash_version", "1"}} {
		t.Run(field.name, func(t *testing.T) {
			rig := newRealRig(t, 4)
			result, err := rig.initializer.Run(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if err := rig.client.HSet(context.Background(), result.Ledger.Records[0].RedisKey, field.name, field.value).Err(); err != nil {
				t.Fatal(err)
			}
			result, err = rig.initializer.Run(context.Background())
			if err == nil || result.Receipt.Activatable || result.Receipt.FailureClass != "CORRUPT_AUTHORITY" {
				t.Fatalf("field=%s err=%v receipt=%+v", field.name, err, result.Receipt)
			}
		})
	}
}

func TestExpiredInitialLeaseReject(t *testing.T) {
	rig := newRealRig(t, 4)
	if _, err := rig.initializer.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	expired := rig.initializer.Config
	expired.Now = func() time.Time { return time.Now().Add(2 * time.Minute) }
	result, err := (&Initializer{Store: rig.store, Reader: rig.reader, Config: expired}).Run(context.Background())
	if !errors.Is(err, ErrIncomplete) || result.Receipt.Activatable {
		t.Fatalf("expired err=%v receipt=%+v", err, result.Receipt)
	}
}

func assertComplete(t *testing.T, result Result, count uint32) {
	t.Helper()
	if !result.Receipt.Complete || !result.Receipt.BootstrapMaterializable || result.Receipt.Activatable || result.Receipt.CompanyRedisContacted || result.Receipt.FormalL2Authorized {
		t.Fatalf("receipt=%+v", result.Receipt)
	}
	if !result.Ledger.Complete || !result.Ledger.BootstrapMaterializable || result.Ledger.Activatable || result.Ledger.CompanyRedisContacted ||
		len(result.Ledger.Records) != int(count)*2 || len(result.LedgerBytes) == 0 {
		t.Fatalf("ledger complete/count=%t/%d", result.Ledger.Complete, len(result.Ledger.Records))
	}
	if result.Snapshot.GlobalRevision != 1 || result.Receipt.ManagerWorkingRevision < 1 {
		t.Fatalf("durable/working revisions=%d/%d", result.Snapshot.GlobalRevision, result.Receipt.ManagerWorkingRevision)
	}
	if result.Snapshot.Player.ShardCount != count || result.Snapshot.Farm.ShardCount != count ||
		len(result.Snapshot.Player.Routes) != int(count) || len(result.Snapshot.Farm.Routes) != int(count) {
		t.Fatalf("snapshot counts player=%d/%d farm=%d/%d", result.Snapshot.Player.ShardCount, len(result.Snapshot.Player.Routes), result.Snapshot.Farm.ShardCount, len(result.Snapshot.Farm.Routes))
	}
	for _, domain := range []actorrouting.DomainSnapshot{result.Snapshot.Player, result.Snapshot.Farm} {
		for shard, route := range domain.Routes {
			if route.State != actorrouting.ShardStateActive || route.ShardRevision == 0 || route.ExpectedWriterEpoch == 0 || route.ExpectedLeaseEpoch == 0 || route.LeaseUntilMs <= 0 {
				t.Fatalf("shard %d route=%+v", shard, route)
			}
		}
	}
}

type faultStore struct {
	delegate   authoritystore.ShardWriteAuthorityStore
	failDomain actorrouting.RouteDomain
	failShard  uint32
	status     authoritystore.ShardAuthorityTransitionStatus
}

func (f *faultStore) ClaimShardAuthority(ctx context.Context, request authoritystore.ClaimShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	if request.Domain == f.failDomain && request.VirtualShard == f.failShard {
		return authoritystore.ShardAuthorityTransitionResult{Status: f.status}, nil
	}
	return f.delegate.ClaimShardAuthority(ctx, request)
}
func (f *faultStore) RenewShardAuthority(ctx context.Context, request authoritystore.RenewShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	return f.delegate.RenewShardAuthority(ctx, request)
}
func (f *faultStore) TransferShardAuthority(ctx context.Context, request authoritystore.TransferShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	return f.delegate.TransferShardAuthority(ctx, request)
}
func (f *faultStore) UnassignShardAuthority(ctx context.Context, request authoritystore.UnassignShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	return f.delegate.UnassignShardAuthority(ctx, request)
}
