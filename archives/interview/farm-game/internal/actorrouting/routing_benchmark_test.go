package actorrouting

import (
	"context"
	"testing"
	"time"
)

func BenchmarkStaticRouteResolve(b *testing.B) {
	resolver, err := NewStaticResolver(completeSnapshot())
	if err != nil {
		b.Fatal(err)
	}
	client, _ := NewActorRouteClient(resolver, time.Hour)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := client.Resolve(ctx, Address{
			Realm: "classic-farm", Kind: KindFarm, ID: uint64(i%1024 + 1),
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}
