package main

import (
	"testing"

	"farmgame/internal/actorrouting"
)

func TestFindFarmIDsCoversTwoRealRouteShards(t *testing.T) {
	calculator, err := actorrouting.NewCalculator("lease-real-test",
		actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: fixtureHashSeed}, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := findFarmIDs(calculator, 910_000_000)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 4 {
		t.Fatalf("ids=%v", ids)
	}
	for key, id := range ids {
		shard, shardErr := calculator.ShardFor(actorrouting.DomainFarm, id)
		if shardErr != nil {
			t.Fatal(shardErr)
		}
		want := uint32(key[len("shard_")] - '0')
		if shard != want {
			t.Fatalf("%s id=%d shard=%d want=%d", key, id, shard, want)
		}
	}
}
