// Package scaleplanstore provides a miniredis-only harness for A1 tests.
package scaleplanstore

import (
	"context"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	core "farmgame/internal/scaleplan"
	"farmgame/internal/storage/redis/rediskey"
	store "farmgame/internal/storage/redis/scaleplan"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type Harness struct {
	Server    *miniredis.Miniredis
	Client    *redis.Client
	Backend   *store.Backend
	Namespace rediskey.Namespace
	Realm     string
}

func New(t testing.TB, realm string, options ...store.Options) *Harness {
	t.Helper()
	server := miniredis.RunT(t)
	server.SetTime(time.UnixMilli(1_700_000_000_000))
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	namespace, err := rediskey.NewNamespace("dev", "z1")
	if err != nil {
		t.Fatalf("namespace: %v", err)
	}
	var option store.Options
	if len(options) > 0 {
		option = options[0]
	}
	option.Namespace, option.Realm = namespace, realm
	backend, err := store.NewBackend(client, option)
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	return &Harness{Server: server, Client: client, Backend: backend, Namespace: namespace, Realm: realm}
}

func (h *Harness) Scope() core.MutationScope {
	return core.MutationScope{Namespace: "farm", Realm: h.Realm}
}
func (h *Harness) Owner(nonce string) core.MutationLeaseOwner {
	return core.MutationLeaseOwner{ControllerID: "controller-a", ControllerBootID: "boot-a", OperationNonce: nonce}
}
func (h *Harness) Lease(t testing.TB, nonce string) core.BackendMutationLease {
	t.Helper()
	lease, err := h.Backend.AcquireMutationLease(context.Background(), h.Scope(), h.Owner(nonce))
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	return lease
}

func DefaultSubmission(realm, key, workloadName string) core.Submission {
	return core.Submission{IdempotencyKey: key, Realm: realm, Domain: actorrouting.DomainFarm, Operation: core.OperationScaleOut,
		Workload:         core.WorkloadRef{Realm: realm, Domain: routeDomainFarm(), Namespace: "default", Kind: "StatefulSet", Name: workloadName},
		ExpectedReplicas: 1, DesiredReplicas: 2, Moves: []core.ShardMoveIntent{{Domain: actorrouting.DomainFarm, VirtualShard: 1, SourceHolderID: "holder-a", TargetHolderID: "holder-b", TargetOrdinal: 1}},
		RoutePlanTTLms: time.Hour.Milliseconds(), PlanTTLms: time.Hour.Milliseconds(), RecoveryRetentionMs: time.Hour.Milliseconds(), Limits: core.FunctionalLimits{MaxRoutePlansInflight: 1, MaxShardsPerWave: 1}}
}

func routeDomainFarm() actorrouting.RouteDomain { return actorrouting.DomainFarm }

func InitialPlan(t testing.TB, realm, key, workloadName string) core.Plan {
	t.Helper()
	plan, err := core.BuildInitialPlan(DefaultSubmission(realm, key, workloadName), time.UnixMilli(1_700_000_000_100))
	if err != nil {
		t.Fatalf("initial plan: %v", err)
	}
	return plan
}
