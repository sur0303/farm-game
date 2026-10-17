package actorrouting

import (
	"errors"
	"testing"
)

func TestHashProfileRejectsPrototypeAndMissingSeed(t *testing.T) {
	tests := []HashProfile{
		{},
		{Version: 1, Seed: testHashSeed},
		{Version: HashVersionXXH64V2},
		{Version: HashVersionXXH64V2 + 1, Seed: testHashSeed},
	}
	for _, profile := range tests {
		if _, err := NewCalculator("classic-farm", profile, 4096, 4096); !errors.Is(err, ErrUnsupportedHashProfile) {
			t.Fatalf("profile %+v err=%v", profile, err)
		}
	}
}

func TestHashSeedAndDomainChangeAssignment(t *testing.T) {
	left, err := NewCalculator(
		"classic-farm",
		HashProfile{Version: HashVersionXXH64V2, Seed: testHashSeed},
		65521,
		65521,
	)
	if err != nil {
		t.Fatal(err)
	}
	right, err := NewCalculator(
		"classic-farm",
		HashProfile{Version: HashVersionXXH64V2, Seed: testHashSeed + 1},
		65521,
		65521,
	)
	if err != nil {
		t.Fatal(err)
	}
	leftPlayer, _ := left.ShardFor(DomainPlayer, 10001)
	rightPlayer, _ := right.ShardFor(DomainPlayer, 10001)
	leftFarm, _ := left.ShardFor(DomainFarm, 10001)
	if leftPlayer == rightPlayer {
		t.Fatalf("seed change unexpectedly preserved shard %d", leftPlayer)
	}
	if leftPlayer == leftFarm {
		t.Fatalf("domain byte did not separate PLAYER/FARM shard %d", leftPlayer)
	}
}

func TestHashSupportsNonPowerOfTwoShardCount(t *testing.T) {
	const shardCount = uint32(257)
	calculator, err := NewCalculator(
		"classic-farm",
		HashProfile{Version: HashVersionXXH64V2, Seed: testHashSeed},
		shardCount,
		shardCount,
	)
	if err != nil {
		t.Fatal(err)
	}
	for actorID := uint64(1); actorID <= 10000; actorID++ {
		shard, err := calculator.ShardFor(DomainFarm, actorID)
		if err != nil {
			t.Fatal(err)
		}
		if shard >= shardCount {
			t.Fatalf("actor %d shard=%d outside [0,%d)", actorID, shard, shardCount)
		}
	}
}

// This is a deterministic smoke alarm for accidental input collapse, not a
// statistical capacity proof. Production distribution is still validated by
// the 30M-DAU synthetic actor corpus in the routing probe.
func TestHashDistributionSmoke(t *testing.T) {
	const (
		shardCount = uint32(257)
		actors     = uint64(100000)
	)
	calculator, err := NewCalculator(
		"classic-farm",
		HashProfile{Version: HashVersionXXH64V2, Seed: testHashSeed},
		shardCount,
		shardCount,
	)
	if err != nil {
		t.Fatal(err)
	}
	buckets := make([]uint64, shardCount)
	for actorID := uint64(1); actorID <= actors; actorID++ {
		shard, err := calculator.ShardFor(DomainFarm, actorID)
		if err != nil {
			t.Fatal(err)
		}
		buckets[shard]++
	}
	mean := actors / uint64(shardCount)
	for shard, count := range buckets {
		if count == 0 || count > mean*2 {
			t.Fatalf("distribution collapse at shard=%d count=%d mean=%d", shard, count, mean)
		}
	}
}

func BenchmarkCalculatorShardFor(b *testing.B) {
	calculator, err := NewCalculator(
		"classic-farm",
		HashProfile{Version: HashVersionXXH64V2, Seed: testHashSeed},
		4096,
		4096,
	)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := calculator.ShardFor(DomainFarm, uint64(i+1)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStaticResolverResolveActor(b *testing.B) {
	resolver, err := NewStaticResolver(completeSnapshot())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := resolver.ResolveActor(b.Context(), DomainFarm, uint64(i+1)); err != nil {
			b.Fatal(err)
		}
	}
}
