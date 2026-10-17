package redisworkload

import (
	"context"
	"fmt"
	"testing"

	"farmgame/internal/actorrouting"
	authority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workidentity"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"
)

func TestAttributedWorkLeaseClassifiesDueScanAndCompletionWithoutChangingResults(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	metrics, err := AttachFromEnvironment(client, prometheus.NewRegistry(), "true")
	if err != nil {
		t.Fatal(err)
	}
	namespace, err := rediskey.NewNamespace("redisworkload", "z1")
	if err != nil {
		t.Fatal(err)
	}
	calculator, err := actorrouting.NewCalculator("redisworkload-test",
		actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202609010001}, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	profile := rediskey.PhysicalProfile{
		ProfileVersion: "p1", Namespace: namespace, AuthorityLayoutVersion: "v1",
		WorkflowLayoutVersion: "v1", WorkflowHashVersion: 1, WorkflowBucketCount: 1,
	}
	store, err := authority.NewStore(client, nil, authority.Options{Profile: profile, Resolver: calculator})
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := profile.AuthorityBucketForShard(calculator, actorrouting.DomainPlayer, 0)
	if err != nil {
		t.Fatal(err)
	}
	lease := NewAttributedWorkLease(store.WorkLease())
	if lease == nil {
		t.Fatal("NewAttributedWorkLease returned nil")
	}

	refs, err := lease.ListDueWork(context.Background(), bucket, 1)
	if err != nil || len(refs) != 0 {
		t.Fatalf("ListDueWork refs=%v err=%v", refs, err)
	}
	if got := testutil.ToFloat64(metrics.commands.WithLabelValues(ComponentDueScan, "time", "ok")); got != 1 {
		t.Fatalf("due_scan TIME commands=%v want=1", got)
	}

	proof := authority.WorkCompletionProof{
		SourceAuthorityKind: "PLAYER_ACTOR", CanonicalSourceAuthorityID: "1",
		AuthorityLayoutVersion: "v1", ActorRouteDomain: "PLAYER",
		PayloadHash: fmt.Sprintf("%064x", 1), InvocationID: "invocation-1",
		ParticipantStep: "TEST_STEP", ParticipantOutcome: "APPLIED",
		ParticipantResultHash: fmt.Sprintf("%064x", 2), ParticipantResultVersion: 1,
	}
	proof.ResultProofHash = authority.WorkCompletionProofHash(proof)
	workID, err := workidentity.StableOpaqueID("testwork", "TEST_WORK", "work-1")
	if err != nil {
		t.Fatal(err)
	}
	completed, err := lease.CompleteWorkWithProof(context.Background(), bucket, "TEST_WORK", workID,
		"worker-1", "boot-1", 1, 1, "lease-token", proof)
	if err != nil || completed.Status != authority.WorkLeaseNotFound {
		t.Fatalf("CompleteWorkWithProof result=%+v err=%v", completed, err)
	}
	workCompleteScripts := testutil.ToFloat64(metrics.commands.WithLabelValues(ComponentWorkComplete, "evalsha", "ok")) +
		testutil.ToFloat64(metrics.commands.WithLabelValues(ComponentWorkComplete, "eval", "ok"))
	if workCompleteScripts < 1 {
		t.Fatalf("work_complete script commands=%v want>=1", workCompleteScripts)
	}
}

func TestNewAttributedWorkLeasePreservesNil(t *testing.T) {
	if got := NewAttributedWorkLease(nil); got != nil {
		t.Fatalf("NewAttributedWorkLease(nil)=%v want=nil", got)
	}
}
