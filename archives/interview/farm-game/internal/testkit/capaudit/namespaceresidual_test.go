package capaudit

import (
	"context"
	"testing"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workflow"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func residualFixture(t *testing.T, ceiling int64) (*NamespaceResidual, *miniredis.Miniredis, *redis.Client,
	rediskey.PhysicalProfile, rediskey.AuthorityShardResolver) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	profile, shards := testProfile(t)
	residual, err := NewNamespaceResidual(NamespaceResidualConfig{
		Client: client, Namespace: profile.Namespace, WorkStorePrefix: testPrefix, Ceiling: ceiling,
	})
	if err != nil {
		t.Fatal(err)
	}
	return residual, server, client, profile, shards
}

func leaveBehind(t *testing.T, client *redis.Client, key string) {
	t.Helper()
	if err := client.Set(context.Background(), key, "leftover", 0).Err(); err != nil {
		t.Fatal(err)
	}
}

// projectionKey and workStoreKey come from the builders the services write
// through, so a fixture cannot agree with the count about a keyspace that
// neither of them actually uses.
func projectionKey(t *testing.T, namespace rediskey.Namespace, playerID uint64) string {
	t.Helper()
	keys, err := rediskey.NewProjectionKeys(namespace)
	if err != nil {
		t.Fatal(err)
	}
	key, err := keys.PublicProfile(playerID)
	if err != nil {
		t.Fatal(err)
	}
	return key.String()
}

func workStoreKey(t *testing.T, prefix string) string {
	t.Helper()
	keys, err := workflow.NewWorkKeys(prefix)
	if err != nil {
		t.Fatal(err)
	}
	return keys.Due()
}

// A deployment writes its control plane before it can serve anybody: the
// Holders claim their shards, the route controller records the route, the Gate
// stamps its boot. On a dynamic-routing deployment those rows are never absent,
// so counting them would refuse every run for a reason that is not residue. What
// a previous run leaves is what somebody's play wrote, and that still counts.
func TestTheDeploymentOwnControlPlaneIsNotResidue(t *testing.T) {
	residual, _, client, profile, _ := residualFixture(t, 0)
	tag := "{" + profile.Namespace.KeyPrefix() + ":"
	for _, key := range []string{
		tag + "auth:player:v1:s:0}:shard-authority",
		tag + "auth:farm:v1:s:3}:scan-lease",
		tag + "route-control:v1}:snapshot",
		tag + "route-control:v1:holders:v1}:record:504c:706c30",
		"farm:" + profile.Namespace.Environment + ":gate-boot:{744fa57f3fad918f}:2aa3c",
	} {
		leaveBehind(t, client, key)
	}

	count, err := residual.Count(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count.Keys != 0 {
		t.Fatalf("count=%+v, want the control plane to be ignored", count)
	}

	// One row somebody's play wrote is residue even beside all of the above.
	leaveBehind(t, client, projectionKey(t, profile.Namespace, 900000001))
	count, err = residual.Count(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count.Keys != 1 {
		t.Fatalf("count=%+v, want the projection row counted", count)
	}
}

func TestAnEmptyNamespaceHoldsNothingToReport(t *testing.T) {
	residual, _, _, _, _ := residualFixture(t, 0)

	count, err := residual.Count(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count.Keys != 0 || count.AtLeast {
		t.Fatalf("count=%+v, want a measured zero", count)
	}
}

// The ways a run addresses storage put keys in three shapes: behind the
// namespace hash tag, in front of it, and under the work store's own prefix. A
// count that knows only one of them calls a dirty keyspace clean.
func TestWhatAnEarlierRunLeftBehindIsCountedInEveryKeyShape(t *testing.T) {
	residual, _, client, profile, shards := residualFixture(t, 0)
	keys, err := rediskey.NewAuthorityKeys(profile, shards)
	if err != nil {
		t.Fatal(err)
	}
	// The deployment's own shard rows are written either way and are not what
	// this counts; an Actor record behind the same hash tag is.
	claimEveryShard(t, client, keys, profile, shards)
	ref, err := profile.AuthorityBucketForShard(shards, actorrouting.DomainPlayer, 0)
	if err != nil {
		t.Fatal(err)
	}
	tag, err := keys.HashTag(ref)
	if err != nil {
		t.Fatal(err)
	}
	leaveBehind(t, client, tag+":actor:900000001")
	leaveBehind(t, client, projectionKey(t, profile.Namespace, 900000001))
	leaveBehind(t, client, workStoreKey(t, testPrefix))

	count, err := residual.Count(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count.Keys != 3 || count.AtLeast {
		t.Fatalf("count=%+v, want the three shapes a run leaves and none of the deployment's own rows", count)
	}
}

// Another environment's keys are not this run's residue, and counting them
// would refuse a run that is starting from clean storage.
func TestKeysOfAnotherKeyspaceAreNotThisRunsResidue(t *testing.T) {
	residual, _, client, _, _ := residualFixture(t, 0)
	elsewhere, err := rediskey.NewNamespace("elsewhere", "z9")
	if err != nil {
		t.Fatal(err)
	}
	leaveBehind(t, client, projectionKey(t, elsewhere, 900000001))
	leaveBehind(t, client, workStoreKey(t, "farm:elsewhere:workflow"))

	count, err := residual.Count(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count.Keys != 0 {
		t.Fatalf("count=%+v, want nothing attributed to this run", count)
	}
}

// A row that names no namespace cannot be attributed to one. The connection
// chain's legacy session keys are that shape, so they are outside what this
// count can claim; widening the scan to reach them would count a neighbour's
// sessions as this run's residue.
func TestARowWithNoNamespaceIsOutsideWhatTheCountCanClaim(t *testing.T) {
	residual, _, client, _, _ := residualFixture(t, 0)
	leaveBehind(t, client, rediskey.Session(900000001))

	count, err := residual.Count(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count.Keys != 0 {
		t.Fatalf("count=%+v, want a key nothing ties to this namespace left out", count)
	}
}

// Past the ceiling the exact number changes no decision, so the count stops
// there. What it may not do is stop at something that reads as clean.
func TestMoreResidueThanTheCeilingStillReadsAsResidue(t *testing.T) {
	residual, _, client, profile, _ := residualFixture(t, 2)
	for _, playerID := range []uint64{1, 2, 3, 4} {
		leaveBehind(t, client, projectionKey(t, profile.Namespace, playerID))
	}

	count, err := residual.Count(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count.Keys != 2 || !count.AtLeast {
		t.Fatalf("count=%+v, want at least the ceiling of 2", count)
	}
}

// A keyspace that cannot be read is not an empty one. Reporting zero here is
// the one answer that would let a run start on the last run's leftovers.
func TestAnUnreachableRedisIsUnobservedRatherThanZero(t *testing.T) {
	residual, server, _, _, _ := residualFixture(t, 0)
	server.Close()

	count, err := residual.Count(context.Background())
	if err == nil {
		t.Fatalf("count=%+v came back from a Redis nobody can read", count)
	}
	if count.Keys != 0 || count.AtLeast {
		t.Fatalf("a failed count returned %+v alongside its error", count)
	}
}

// The cursor loop is bounded, so it can end before the keyspace does. Having
// found nothing by then is not evidence of an empty namespace; it is evidence
// that the namespace was never read to the end.
func TestAScanThatRanOutOfPagesCannotCallTheNamespaceEmpty(t *testing.T) {
	endless := func(uint64) ([]string, uint64, error) { return nil, 7, nil }

	if _, err := collect(endless, map[string]struct{}{}, DefaultResidualCeiling); err == nil {
		t.Fatal("a scan that never reached the end of the keyspace reported it clean")
	}

	found := map[string]struct{}{"classic-farm:test:z1:proj:public-profile:v1:1": {}}
	done, err := collect(endless, found, DefaultResidualCeiling)
	if err != nil || done {
		t.Fatalf("done=%v err=%v, want the residue already found to stand as a floor", done, err)
	}
}

func TestAKeyspaceThatCannotBeNamedExactlyIsRefused(t *testing.T) {
	profile, _ := testProfile(t)
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	tests := []struct {
		name   string
		config NamespaceResidualConfig
	}{
		{
			name:   "no work store prefix",
			config: NamespaceResidualConfig{Client: client, Namespace: profile.Namespace},
		},
		{
			name: "a work store prefix that matches more than itself",
			config: NamespaceResidualConfig{Client: client, Namespace: profile.Namespace,
				WorkStorePrefix: "farm:*:workflow"},
		},
		{
			name:   "no namespace",
			config: NamespaceResidualConfig{Client: client, WorkStorePrefix: testPrefix},
		},
		{
			name:   "no client",
			config: NamespaceResidualConfig{Namespace: profile.Namespace, WorkStorePrefix: testPrefix},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewNamespaceResidual(tt.config); err == nil {
				t.Fatal("a count that cannot say which keys belong to the run was accepted")
			}
		})
	}
}

func TestTheResidualIsNamedByTheContract(t *testing.T) {
	if NamespaceResidualMetric != "namespace_residual_before_run" {
		t.Fatalf("NamespaceResidualMetric=%s", NamespaceResidualMetric)
	}
}
