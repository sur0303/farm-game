package capaudit

import (
	"context"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workidentity"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func testProfile(t *testing.T) (rediskey.PhysicalProfile, rediskey.AuthorityShardResolver) {
	t.Helper()
	namespace, err := rediskey.NewNamespace("test", "z1")
	if err != nil {
		t.Fatal(err)
	}
	calculator, err := actorrouting.NewCalculator("classic-farm",
		actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 7}, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	return rediskey.PhysicalProfile{
		ProfileVersion: "redis-physical-v1", Namespace: namespace,
		AuthorityLayoutVersion: "v1", WorkflowLayoutVersion: "v1",
		WorkflowHashVersion: 1, WorkflowBucketCount: 256,
	}, calculator
}

func newDueCheck(t *testing.T, now time.Time, schedule bool) (*DueResidualCheck, *redis.Client, rediskey.AuthorityKeys, rediskey.PhysicalProfile, rediskey.AuthorityShardResolver) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	profile, shards := testProfile(t)
	keys, err := rediskey.NewAuthorityKeys(profile, shards)
	if err != nil {
		t.Fatal(err)
	}
	check, err := NewDueResidualCheck(DueResidualConfig{
		Client: client, Profile: profile, Shards: shards,
		DedicatedScheduleIndex: schedule, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	return check, client, keys, profile, shards
}

func addDue(t *testing.T, client *redis.Client, keys rediskey.AuthorityKeys, profile rediskey.PhysicalProfile,
	shards rediskey.AuthorityShardResolver, domain actorrouting.RouteDomain, shard uint32, member string, dueAt time.Time, dedicated bool) {
	t.Helper()
	ref, err := profile.AuthorityBucketForShard(shards, domain, shard)
	if err != nil {
		t.Fatal(err)
	}
	key, err := keys.SourceDueIndex(ref)
	if dedicated {
		key, err = keys.SourceScheduleDueIndex(ref)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := client.ZAdd(context.Background(), key.String(), redis.Z{
		Score: float64(dueAt.UnixMilli()), Member: member,
	}).Err(); err != nil {
		t.Fatal(err)
	}
}

func stableID(t *testing.T, name string) string {
	t.Helper()
	id, err := workidentity.StableOpaqueID("capaudit-test", name)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func workMember(t *testing.T, name string) string {
	t.Helper()
	ref, err := workidentity.EncodeWorkRefV1("PLANT_CROP", stableID(t, name))
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func scheduleMember(t *testing.T, name string) string {
	t.Helper()
	ref, err := workidentity.EncodeScheduleRefV1("CARE_REWARD", stableID(t, name))
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

// Claiming an item pushes its due score to the lease expiry, so work in
// progress is never far overdue. Only entries past that window are residual.
func TestOnlyEntriesPastTheLeaseWindowCount(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	check, client, keys, profile, shards := newDueCheck(t, now, false)
	addDue(t, client, keys, profile, shards, actorrouting.DomainFarm, 0,
		workMember(t, "future-harvest"), now.Add(time.Hour), false)
	addDue(t, client, keys, profile, shards, actorrouting.DomainFarm, 0,
		workMember(t, "just-claimed"), now.Add(-10*time.Second), false)
	addDue(t, client, keys, profile, shards, actorrouting.DomainFarm, 1,
		workMember(t, "abandoned"), now.Add(-10*time.Minute), false)

	got, err := check.Observe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != 1 {
		t.Fatalf("residual=%d, want only the entry past the lease window", got)
	}
}

func TestBothDomainsAndEveryShardAreWalked(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	check, client, keys, profile, shards := newDueCheck(t, now, false)
	stale := now.Add(-10 * time.Minute)
	addDue(t, client, keys, profile, shards, actorrouting.DomainPlayer, 0, workMember(t, "p0"), stale, false)
	addDue(t, client, keys, profile, shards, actorrouting.DomainPlayer, 1, workMember(t, "p1"), stale, false)
	addDue(t, client, keys, profile, shards, actorrouting.DomainFarm, 0, workMember(t, "f0"), stale, false)
	addDue(t, client, keys, profile, shards, actorrouting.DomainFarm, 1, workMember(t, "f1"), stale, false)

	got, err := check.Observe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != 4 {
		t.Fatalf("residual=%d, want every shard of both domains walked", got)
	}
}

// When the deployment keeps schedules in their own index, leftovers there are
// invisible to a walk of the shared index alone.
func TestTheDedicatedScheduleIndexIsWalkedWhenItIsInUse(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	stale := now.Add(-10 * time.Minute)

	shared, client, keys, profile, shards := newDueCheck(t, now, false)
	addDue(t, client, keys, profile, shards, actorrouting.DomainFarm, 0, scheduleMember(t, "sched1"), stale, true)
	got, err := shared.Observe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != 0 {
		t.Fatalf("residual=%d before the dedicated index is in use", got)
	}

	dedicated, client, keys, profile, shards := newDueCheck(t, now, true)
	addDue(t, client, keys, profile, shards, actorrouting.DomainFarm, 0, scheduleMember(t, "sched1"), stale, true)
	got, err = dedicated.Observe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != 1 {
		t.Fatalf("residual=%d, want the dedicated schedule index counted", got)
	}
}

// An entry nobody can parse is not evidence of health.
func TestAnUnknownMemberShapeFailsTheAudit(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	check, client, keys, profile, shards := newDueCheck(t, now, false)
	addDue(t, client, keys, profile, shards, actorrouting.DomainFarm, 0, "mystery", now.Add(-time.Hour), false)

	if _, err := check.Observe(context.Background()); err == nil {
		t.Fatal("an unparseable due member must fail the audit rather than be ignored")
	}
}

func TestMoreEntriesThanTheAuditCanReadIsAFailure(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	profile, shards := testProfile(t)
	keys, err := rediskey.NewAuthorityKeys(profile, shards)
	if err != nil {
		t.Fatal(err)
	}
	check, err := NewDueResidualCheck(DueResidualConfig{
		Client: client, Profile: profile, Shards: shards, Limit: 2, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	stale := now.Add(-time.Hour)
	for _, id := range []string{"a", "b", "c", "d"} {
		addDue(t, client, keys, profile, shards, actorrouting.DomainPlayer, 0, workMember(t, id), stale, false)
	}

	if _, err := check.Observe(context.Background()); err == nil {
		t.Fatal("reading part of the indexes would report a count that looks clean")
	}
}

func TestDueGateIsNamedByTheContract(t *testing.T) {
	check, _, _, _, _ := newDueCheck(t, time.Now(), false)
	if check.Gate() != "due_residual_unexpected" {
		t.Fatalf("Gate()=%s", check.Gate())
	}
}
