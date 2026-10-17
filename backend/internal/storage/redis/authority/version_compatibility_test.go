package authority

import (
	"context"
	"reflect"
	"testing"

	"github.com/redis/go-redis/v9"

	"farmgame/internal/storage/authoritystore"
)

type actorScriptVersions struct {
	name         string
	loadScript   string
	commitScript string
}

var actorScriptVersionMatrix = []actorScriptVersions{
	{name: "v5_v12", loadScript: ScriptLoadAndClaimActorV5, commitScript: ScriptCommitActorWithOrdersV12},
	{name: "v5_v13", loadScript: ScriptLoadAndClaimActorV5, commitScript: ScriptCommitActorWithOrders},
	{name: "v6_v12", loadScript: ScriptLoadAndClaimActor, commitScript: ScriptCommitActorWithOrdersV12},
	{name: "v6_v13", loadScript: ScriptLoadAndClaimActor, commitScript: ScriptCommitActorWithOrders},
}

func newStoreWithActorScriptVersions(t *testing.T, client redis.UniversalClient, versions actorScriptVersions) *Store {
	t.Helper()
	registry, err := NewScriptRegistry(client)
	if err != nil {
		t.Fatal(err)
	}
	loadDef, ok := registry.scripts[versions.loadScript]
	if !ok {
		t.Fatalf("load script %q is not registered", versions.loadScript)
	}
	commitDef, ok := registry.scripts[versions.commitScript]
	if !ok {
		t.Fatalf("commit script %q is not registered", versions.commitScript)
	}
	// Store calls the current symbolic names. Point only those aliases at the
	// selected frozen sources to model old and new binaries sharing Redis.
	registry.scripts[ScriptLoadAndClaimActor] = loadDef
	registry.scripts[ScriptCommitActorWithOrders] = commitDef
	store, err := NewStore(client, registry, Options{Profile: testProfile(t), Resolver: testCalculator(t)})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func sparseVersionedCommit(
	t *testing.T,
	rig *testRig,
	store *Store,
	claim string,
	before authoritystore.AuthoritySnapshot,
	operationID string,
	value string,
) authoritystore.CommitResult {
	t.Helper()
	candidate := cloneModelState(before.ModelFields)
	candidate["dirty"] = []byte(value)
	mutation, err := authoritystore.BuildActorModelMutation(
		authoritystore.ModelMutationIntentUpdate,
		authoritystore.ModelLayoutV1,
		candidate,
		authoritystore.ModelState{"dirty": candidate["dirty"]},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	req := rig.commitReq(claim, before.RecordVersion, operationID, candidate)
	req.ModelMutation = mutation
	result, err := store.CommitAuditedClaim(context.Background(), req)
	if err != nil || result.Status != authoritystore.CommitCommitted {
		t.Fatalf("versioned commit=%+v err=%v", result, err)
	}
	return result
}

func TestLoadV5V6CommitV12V13MixedVersionsProduceIdenticalRedisData(t *testing.T) {
	var wantSeed map[string]string
	var wantResult map[string]string
	for _, versions := range actorScriptVersionMatrix {
		versions := versions
		t.Run(versions.name, func(t *testing.T) {
			rig := newTestRig(t)
			loaded := rig.load(t, "mixed-seed")
			seedReq := rig.commitReq("mixed-seed", loaded.RecordVersion, "mixed-seed-commit",
				authoritystore.ModelState{"keep": []byte("stable"), "dirty": []byte("seed")})
			seeded, err := rig.store.CommitAuditedClaim(context.Background(), seedReq)
			if err != nil || seeded.Status != authoritystore.CommitCommitted {
				t.Fatalf("seed commit=%+v err=%v", seeded, err)
			}
			actorKey, err := rig.store.Keys().ActorRecord(rig.bucket, rig.addr.ActorID)
			if err != nil {
				t.Fatal(err)
			}
			seedHash := actorRecordHash(t, rig, actorKey.String())
			if wantSeed == nil {
				wantSeed = seedHash
			} else if !reflect.DeepEqual(seedHash, wantSeed) {
				t.Fatalf("matrix did not start from identical Redis data\ngot=%q\nwant=%q", seedHash, wantSeed)
			}

			selected := newStoreWithActorScriptVersions(t, rig.client, versions)
			reclaimed := reclaimWithStore(t, rig, selected, "mixed-claim")
			committed := sparseVersionedCommit(t, rig, selected, "mixed-claim", reclaimed, "mixed-version-commit", "next")
			if string(committed.Snapshot.ModelFields["keep"]) != "stable" || string(committed.Snapshot.ModelFields["dirty"]) != "next" {
				t.Fatalf("mixed-version snapshot=%+v", committed.Snapshot)
			}
			resultHash := actorRecordHash(t, rig, actorKey.String())
			if wantResult == nil {
				wantResult = resultHash
			} else if !reflect.DeepEqual(resultHash, wantResult) {
				t.Fatalf("matrix produced different Redis data\ngot=%q\nwant=%q", resultHash, wantResult)
			}
		})
	}
}

func TestActorScriptRollbackV6V13ToV5V12AndRollForward(t *testing.T) {
	rig := newTestRig(t)
	loaded := rig.load(t, "before-rollback")
	seedReq := rig.commitReq("before-rollback", loaded.RecordVersion, "before-rollback-commit",
		authoritystore.ModelState{"keep": []byte("stable"), "dirty": []byte("new")})
	seeded, err := rig.store.CommitAuditedClaim(context.Background(), seedReq)
	if err != nil || seeded.Status != authoritystore.CommitCommitted {
		t.Fatalf("V6/V13 seed=%+v err=%v", seeded, err)
	}

	rollback := newStoreWithActorScriptVersions(t, rig.client, actorScriptVersionMatrix[0])
	rolledBack := reclaimWithStore(t, rig, rollback, "rollback-v5-v12")
	oldCommitted := sparseVersionedCommit(t, rig, rollback, "rollback-v5-v12", rolledBack, "rollback-old-commit", "old")
	if string(oldCommitted.Snapshot.ModelFields["dirty"]) != "old" {
		t.Fatalf("V5/V12 rollback snapshot=%+v", oldCommitted.Snapshot)
	}

	rollForward := newStoreWithActorScriptVersions(t, rig.client, actorScriptVersionMatrix[3])
	forwardClaim := reclaimWithStore(t, rig, rollForward, "roll-forward-v6-v13")
	forwardCommitted := sparseVersionedCommit(t, rig, rollForward, "roll-forward-v6-v13", forwardClaim, "roll-forward-commit", "new-again")
	if string(forwardCommitted.Snapshot.ModelFields["keep"]) != "stable" || string(forwardCommitted.Snapshot.ModelFields["dirty"]) != "new-again" ||
		forwardCommitted.Snapshot.PhysicalUsage.ReceiptItems != 3 {
		t.Fatalf("roll-forward snapshot=%+v", forwardCommitted.Snapshot)
	}
}

func scriptCached(t *testing.T, client *redis.Client, sha string) bool {
	t.Helper()
	cached, err := client.ScriptExists(context.Background(), sha).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(cached) != 1 {
		t.Fatalf("SCRIPT EXISTS reply=%v", cached)
	}
	return cached[0]
}

func TestActorScriptNOSCRIPTAndReplyLossAcrossVersions(t *testing.T) {
	for _, tc := range []struct {
		name     string
		producer actorScriptVersions
		consumer actorScriptVersions
	}{
		{name: "v6_v13_to_v5_v12", producer: actorScriptVersionMatrix[3], consumer: actorScriptVersionMatrix[0]},
		{name: "v5_v12_to_v6_v13", producer: actorScriptVersionMatrix[0], consumer: actorScriptVersionMatrix[3]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := newTestRig(t)
			drop := &dropClient{UniversalClient: rig.client}
			producer := newStoreWithActorScriptVersions(t, drop, tc.producer)
			rig.store = producer

			loadSHA, _ := producer.Registry().ScriptSHA(ScriptLoadAndClaimActor)
			if scriptCached(t, rig.client, loadSHA) {
				t.Fatalf("producer load SHA %s unexpectedly cached", loadSHA)
			}
			loaded := rig.load(t, "reply-loss-claim")
			if !scriptCached(t, rig.client, loadSHA) {
				t.Fatalf("producer load SHA %s was not recovered after NOSCRIPT", loadSHA)
			}

			commitSHA, _ := producer.Registry().ScriptSHA(ScriptCommitActorWithOrders)
			if scriptCached(t, rig.client, commitSHA) {
				t.Fatalf("producer commit SHA %s unexpectedly cached", commitSHA)
			}
			warmReq := rig.commitReq("reply-loss-claim", loaded.RecordVersion, "reply-loss-warm",
				authoritystore.ModelState{"record": []byte("warm")})
			warmed, err := producer.CommitAuditedClaim(context.Background(), warmReq)
			if err != nil || warmed.Status != authoritystore.CommitCommitted {
				t.Fatalf("producer NOSCRIPT commit=%+v err=%v", warmed, err)
			}
			if !scriptCached(t, rig.client, commitSHA) {
				t.Fatalf("producer commit SHA %s was not recovered after NOSCRIPT", commitSHA)
			}

			lostReq := rig.commitReq("reply-loss-claim", warmed.Snapshot.RecordVersion, "reply-loss-applied",
				authoritystore.ModelState{"record": []byte("applied")})
			drop.mu.Lock()
			drop.dropAfter = 1
			drop.mu.Unlock()
			lost, err := producer.CommitAuditedClaim(context.Background(), lostReq)
			if err != nil || lost.Status != authoritystore.CommitTimeoutUnknown {
				t.Fatalf("producer reply loss=%+v err=%v", lost, err)
			}

			consumer := newStoreWithActorScriptVersions(t, drop, tc.consumer)
			rig.store = consumer
			consumerLoadSHA, _ := consumer.Registry().ScriptSHA(ScriptLoadAndClaimActor)
			if scriptCached(t, rig.client, consumerLoadSHA) {
				t.Fatalf("consumer load SHA %s unexpectedly cached", consumerLoadSHA)
			}
			sameClaim, err := consumer.LoadAndClaim(context.Background(), authoritystore.LoadAndClaimRequest{
				ActorAddress: rig.addr, AuthorityLayoutVersion: "v1", ShardWriteFence: rig.fence,
				ClaimOperationID: "claim-op-reply-loss-claim", NewWriterClaimID: "reply-loss-claim",
			})
			if err != nil || sameClaim.Status != authoritystore.LoadAndClaimAlreadyClaimed {
				t.Fatalf("consumer same claim=%+v err=%v", sameClaim, err)
			}
			if !scriptCached(t, rig.client, consumerLoadSHA) {
				t.Fatalf("consumer load SHA %s was not recovered after NOSCRIPT", consumerLoadSHA)
			}

			consumerCommitSHA, _ := consumer.Registry().ScriptSHA(ScriptCommitActorWithOrders)
			if scriptCached(t, rig.client, consumerCommitSHA) {
				t.Fatalf("consumer commit SHA %s unexpectedly cached", consumerCommitSHA)
			}
			replay, err := consumer.CommitAuditedClaim(context.Background(), lostReq)
			if err != nil || replay.Status != authoritystore.CommitAlreadyCommitted {
				t.Fatalf("cross-version reply-loss replay=%+v err=%v", replay, err)
			}
			if !scriptCached(t, rig.client, consumerCommitSHA) {
				t.Fatalf("consumer commit SHA %s was not recovered after NOSCRIPT", consumerCommitSHA)
			}
			if replay.Snapshot.RecordVersion != warmed.Snapshot.RecordVersion+1 || replay.Snapshot.PhysicalUsage.ReceiptItems != 2 ||
				string(replay.Snapshot.ModelFields["record"]) != "applied" {
				t.Fatalf("cross-version replay snapshot=%+v", replay.Snapshot)
			}
		})
	}
}
