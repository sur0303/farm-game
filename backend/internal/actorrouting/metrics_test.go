package actorrouting

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestRouteMetricsResolveSuccessAndStale(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := RegisterRouteMetrics(reg)
	resolver, err := NewStaticResolver(completeSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	resolver.SetMetrics(metrics)

	ctx := context.Background()
	if _, err := resolver.Resolve(ctx, Address{Realm: "classic-farm", Kind: KindPlayer, ID: 10001}); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(metrics.ResolveTotal.WithLabelValues("PLAYER", ResolveResultSuccess)); got != 1 {
		t.Fatalf("success count=%v, want 1", got)
	}

	snap := completeSnapshot()
	snap.GlobalRevision = 2
	calc, err := NewCalculator(
		snap.Realm,
		HashProfile{Version: snap.HashVersion, Seed: snap.HashSeed},
		snap.Player.ShardCount,
		snap.Farm.ShardCount,
	)
	if err != nil {
		t.Fatal(err)
	}
	shard, err := calc.ShardFor(DomainPlayer, 10001)
	if err != nil {
		t.Fatal(err)
	}
	snap.Player.Routes[shard].LeaseUntilMs = time.Now().Add(-time.Minute).UnixMilli()
	snap.Player.Routes[shard].ShardRevision++
	snap.Player.Routes[shard].ExpectedWriterEpoch++
	snap.Player.Routes[shard].ExpectedLeaseEpoch++
	if err := resolver.Replace(snap); err != nil {
		t.Fatal(err)
	}
	_, err = resolver.Resolve(ctx, Address{Realm: "classic-farm", Kind: KindPlayer, ID: 10001})
	if !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("err=%v, want lease expired", err)
	}
	if got := testutil.ToFloat64(metrics.ResolveTotal.WithLabelValues("PLAYER", ResolveResultStale)); got != 1 {
		t.Fatalf("stale count=%v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.ResolveLeaseExpired); got != 1 {
		t.Fatalf("route_resolve_lease_expired_total=%v, want 1", got)
	}
}

func TestAuthorityDelegatedResolverDoesNotTreatSnapshotDeadlineAsTopologyExpiry(t *testing.T) {
	snapshot := completeSnapshot()
	for index := range snapshot.Player.Routes {
		snapshot.Player.Routes[index].LeaseUntilMs = time.Now().Add(-time.Minute).UnixMilli()
	}
	resolver, err := NewStaticResolverWithOptions(snapshot, ResolverOptions{LeaseCheckMode: LeaseCheckAuthorityDelegated})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.Resolve(context.Background(), Address{Realm: snapshot.Realm, Kind: DomainPlayer, ID: 10001}); err != nil {
		t.Fatalf("authority-delegated resolver rejected topology-only route: %v", err)
	}
}

func TestRouteMetricsRedirectApplied(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := RegisterRouteMetrics(reg)
	resolver, err := NewStaticResolver(completeSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	applier, err := NewRedirectApplier(resolver)
	if err != nil {
		t.Fatal(err)
	}
	applier.SetMetrics(metrics)

	err = applier.ApplyRedirect(context.Background(), RouteRedirect{
		Domain:               DomainPlayer,
		ShardID:              0,
		GlobalRevision:       2,
		ShardRevision:        5,
		ExpectedHolderID:     "player-b",
		ExpectedHolderBootID: "player-b-boot",
		ExpectedWriterEpoch:  9,
		ExpectedLeaseEpoch:   9,
		LeaseUntilMs:         time.Now().Add(time.Minute).UnixMilli(),
		NewHolderIndex:       1,
		Reason:               RedirectReasonStaleRoute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(metrics.RedirectTotal.WithLabelValues("PLAYER", RedirectReasonStaleRoute)); got != 1 {
		t.Fatalf("redirect applied count=%v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.SnapshotRevision); got != 2 {
		t.Fatalf("snapshot_revision=%v, want 2", got)
	}
}

func TestRouteMetricsWatchApplyLag(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := RegisterRouteMetrics(reg)
	metrics.ObserveWatchApplyLag(1250 * time.Microsecond)
	if got := testutil.ToFloat64(metrics.WatchApplyLagMs); got != 1.25 {
		t.Fatalf("route_watch_apply_lag_ms=%v, want 1.25", got)
	}
}

func TestRouteMetricsWriterEpochMismatch(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := RegisterRouteMetrics(reg)
	metrics.ObserveWriterEpochMismatch()
	if got := testutil.ToFloat64(metrics.WriterEpochMismatch); got != 1 {
		t.Fatalf("writer_epoch_mismatch=%v, want 1", got)
	}
}

func TestRouteMetricsSnapshotAgeAdvancesAfterReplace(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := RegisterRouteMetrics(reg)
	resolver, err := NewStaticResolver(completeSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	resolver.SetMetrics(metrics)
	if age := metrics.snapshotAgeSeconds(); age < 0 {
		t.Fatalf("age=%v", age)
	}
	time.Sleep(5 * time.Millisecond)
	next := completeSnapshot()
	next.GlobalRevision = 3
	if err := resolver.Replace(next); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(metrics.SnapshotRevision); got != 3 {
		t.Fatalf("revision=%v, want 3", got)
	}
	if age := metrics.snapshotAgeSeconds(); age < 0 || age > 1 {
		t.Fatalf("age after replace=%v", age)
	}
}
