package capaudit

import (
	"context"
	"strings"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/redis/rediskey"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func presenceFixture(t *testing.T) (*ShardPresence, *redis.Client, rediskey.AuthorityKeys, rediskey.PhysicalProfile, rediskey.AuthorityShardResolver) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	profile, shards := testProfile(t)
	keys, err := rediskey.NewAuthorityKeys(profile, shards)
	if err != nil {
		t.Fatal(err)
	}
	presence, err := NewShardPresence(ShardPresenceConfig{Client: client, Profile: profile, Shards: shards})
	if err != nil {
		t.Fatal(err)
	}
	return presence, client, keys, profile, shards
}

func claimEveryShard(t *testing.T, client *redis.Client, keys rediskey.AuthorityKeys,
	profile rediskey.PhysicalProfile, shards rediskey.AuthorityShardResolver) []string {
	t.Helper()
	var claimed []string
	for _, domain := range []actorrouting.RouteDomain{actorrouting.DomainPlayer, actorrouting.DomainFarm} {
		count, err := shards.ShardCount(domain)
		if err != nil {
			t.Fatal(err)
		}
		for shard := uint32(0); shard < count; shard++ {
			ref, err := profile.AuthorityBucketForShard(shards, domain, shard)
			if err != nil {
				t.Fatal(err)
			}
			key, err := keys.ShardAuthority(ref)
			if err != nil {
				t.Fatal(err)
			}
			if err := client.HSet(context.Background(), key.String(), "writer_epoch", "1").Err(); err != nil {
				t.Fatal(err)
			}
			claimed = append(claimed, key.String())
		}
	}
	return claimed
}

// This is the failure the precondition exists for: an audit aimed at a
// keyspace nobody writes finds nothing wrong there, and every gate reads clean.
func TestAnEmptyKeyspaceIsNotACleanRun(t *testing.T) {
	presence, _, _, _, _ := presenceFixture(t)

	err := presence.Verify(context.Background())
	if err == nil {
		t.Fatal("an empty keyspace passed the precondition, so every gate would report a zero nobody observed")
	}
	if !strings.Contains(err.Error(), "not the keyspace under test") {
		t.Fatalf("error must say what is actually wrong, got %v", err)
	}
}

func TestAClaimedKeyspacePassesThePrecondition(t *testing.T) {
	presence, client, keys, profile, shards := presenceFixture(t)
	claimEveryShard(t, client, keys, profile, shards)

	if err := presence.Verify(context.Background()); err != nil {
		t.Fatalf("the keyspace under test was refused: %v", err)
	}
}

// One unclaimed shard means the actors in it cannot be vouched for, so the
// audit has to say so rather than report on the shards it could read.
func TestOneUnclaimedShardWithholdsTheVerdict(t *testing.T) {
	presence, client, keys, profile, shards := presenceFixture(t)
	claimed := claimEveryShard(t, client, keys, profile, shards)
	if err := client.Del(context.Background(), claimed[len(claimed)-1]).Err(); err != nil {
		t.Fatal(err)
	}

	if err := presence.Verify(context.Background()); err == nil {
		t.Fatal("a shard with no authority row was treated as observed")
	}
}

// A failing precondition has to withhold every gate, not just the ones whose
// own read happened to fail.
func TestAFailedPreconditionWithholdsEveryGate(t *testing.T) {
	presence, _, _, _, _ := presenceFixture(t)
	runner, err := NewRunner(Config{
		Before: []Precondition{presence}, Interval: time.Second,
		FreshFor: time.Minute, Timeout: time.Second,
	}, countingCheck{gate: "some_gate"})
	if err != nil {
		t.Fatal(err)
	}
	runner.RunOnce(context.Background())

	if _, err := runner.Value("some_gate"); err == nil {
		t.Fatal("a gate reported a value although the audit could not prove it read the right keyspace")
	}
}

type countingCheck struct{ gate string }

func (c countingCheck) Gate() string { return c.gate }

func (c countingCheck) Observe(context.Context) (int64, error) { return 0, nil }
