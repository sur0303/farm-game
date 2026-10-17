package carerecovery

import (
	"context"
	"testing"
	"time"

	"farmgame/internal/identity"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workidentity"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestRedisDueObserverFiltersConfiguredFamily(t *testing.T) {
	profile, calculator, bucket := testBucket(t)
	server := miniredis.RunT(t)
	now := time.UnixMilli(1_800_000_000_000)
	server.SetTime(now)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	keys, err := rediskey.NewAuthorityKeys(profile, calculator)
	if err != nil {
		t.Fatal(err)
	}
	dueKey, err := keys.SourceDueIndex(bucket)
	if err != nil {
		t.Fatal(err)
	}
	careID := mustOrderID(t, "due-observer-care")
	harvestID, err := workidentity.StableOpaqueID("due-observer-harvest")
	if err != nil {
		t.Fatal(err)
	}
	careRef, err := workidentity.EncodeWorkRefV1(FamilyCareReward, careID)
	if err != nil {
		t.Fatal(err)
	}
	harvestRef, err := workidentity.EncodeWorkRefV1(string(identity.WorkTypeHarvest), harvestID)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.ZAdd(context.Background(), dueKey.String(),
		redis.Z{Score: float64(now.Add(-2 * time.Second).UnixMilli()), Member: careRef},
		redis.Z{Score: float64(now.Add(-time.Second).UnixMilli()), Member: harvestRef},
	).Err(); err != nil {
		t.Fatal(err)
	}
	care := &redisCareDueObserver{client: client, keys: keys, family: FamilyCareReward}
	harvest := &redisCareDueObserver{client: client, keys: keys, family: string(identity.WorkTypeHarvest)}
	careObservation, err := care.ObserveBuckets(context.Background(), []rediskey.AuthorityBucketRef{bucket}, 8)
	if err != nil {
		t.Fatal(err)
	}
	harvestObservation, err := harvest.ObserveBuckets(context.Background(), []rediskey.AuthorityBucketRef{bucket}, 8)
	if err != nil {
		t.Fatal(err)
	}
	if !careObservation.Available || careObservation.Depth != 1 || careObservation.OldestDueAtMs != now.Add(-2*time.Second).UnixMilli() {
		t.Fatalf("care observation=%+v", careObservation)
	}
	if !harvestObservation.Available || harvestObservation.Depth != 1 || harvestObservation.OldestDueAtMs != now.Add(-time.Second).UnixMilli() {
		t.Fatalf("harvest observation=%+v", harvestObservation)
	}
}
