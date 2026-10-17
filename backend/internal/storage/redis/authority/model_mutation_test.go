package authority

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"farmgame/internal/actormodel/fieldv2"
	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/redis/rediskey"
)

func TestRedisSparseMutationAndAtomicV1ToV2Migration(t *testing.T) {
	rig := newTestRig(t)
	loaded := loadState(t, rig, "claim-v1", authoritystore.ModelState{
		"public_profile": []byte("derived-v1"),
		"record":         []byte("record-v1"),
	})

	sparseCandidate := cloneModelState(loaded.ModelFields)
	sparseCandidate["record"] = []byte("record-v1-next")
	sparse, err := authoritystore.BuildActorModelMutation(authoritystore.ModelMutationIntentUpdate,
		authoritystore.ModelLayoutV1, sparseCandidate,
		authoritystore.ModelState{"record": sparseCandidate["record"]}, nil)
	if err != nil {
		t.Fatal(err)
	}
	sparseReq := rig.commitReq("claim-v1", loaded.RecordVersion, "redis-sparse", sparseCandidate)
	sparseReq.ModelMutation = sparse
	sparseResult, err := rig.store.Commit(context.Background(), sparseReq)
	if err != nil || sparseResult.Status != authoritystore.CommitCommitted {
		t.Fatalf("sparse=%+v err=%v", sparseResult, err)
	}
	if sparseResult.Snapshot.RecordVersion != loaded.RecordVersion+1 ||
		string(sparseResult.Snapshot.ModelFields["public_profile"]) != "derived-v1" {
		t.Fatalf("sparse snapshot=%+v", sparseResult.Snapshot)
	}

	v2 := redisPlayerV2DummyState()
	migration, err := authoritystore.BuildActorModelMutation(authoritystore.ModelMutationIntentMigrateV1ToV2,
		authoritystore.ModelLayoutV2, v2, v2, []string{"public_profile", "record"})
	if err != nil {
		t.Fatal(err)
	}
	migrationReq := rig.commitReq("claim-v1", sparseResult.Snapshot.RecordVersion, "redis-migrate-v2", v2)
	migrationReq.ModelMutation = migration
	order := dueOrder(t, "CARE_REWARD", "redis-migrate-v2", testNow.Add(time.Hour).UnixMilli())
	migrationReq.AttachedOrders = []authoritystore.AttachedOrder{order}
	committed, err := rig.store.Commit(context.Background(), migrationReq)
	if err != nil || committed.Status != authoritystore.CommitCommitted {
		t.Fatalf("migration=%+v err=%v", committed, err)
	}
	if committed.Snapshot.ModelLayout != authoritystore.ModelLayoutV2 ||
		committed.Snapshot.RecordVersion != sparseResult.Snapshot.RecordVersion+1 ||
		!reflect.DeepEqual(committed.Snapshot.ModelFields, v2) {
		t.Fatalf("migration snapshot=%+v", committed.Snapshot)
	}

	actorKey, err := rig.store.Keys().ActorRecord(rig.bucket, rig.addr.ActorID)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := rig.client.HGetAll(context.Background(), actorKey.String()).Result()
	if err != nil {
		t.Fatal(err)
	}
	if _, found := hash["model:record"]; found {
		t.Fatalf("committed migration retained model:record: %q", hash)
	}
	if hash["_meta:model_layout_version"] != "2" || hash["_meta:model_manifest"] != manifestLP(v2) {
		t.Fatalf("v2 metadata layout=%q manifest=%x", hash["_meta:model_layout_version"], hash["_meta:model_manifest"])
	}
	workRef, err := workRefOf(order)
	if err != nil {
		t.Fatal(err)
	}
	score, err := rig.client.ZScore(context.Background(), mustKey(t, func() (rediskey.RedisKey, error) {
		return rig.store.Keys().SourceDueIndex(rig.bucket)
	}).String(), workRef).Result()
	if err != nil || int64(score) != order.DueAtMs {
		t.Fatalf("migration due score=%v err=%v", score, err)
	}

	replay, err := rig.store.Commit(context.Background(), migrationReq)
	if err != nil || replay.Status != authoritystore.CommitAlreadyCommitted || replay.Snapshot.ModelLayout != authoritystore.ModelLayoutV2 {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}

	legacy := rig.commitReq("claim-v1", committed.Snapshot.RecordVersion, "redis-revert-v1",
		authoritystore.ModelState{"record": []byte("must-not-revert")})
	if result, err := rig.store.Commit(context.Background(), legacy); err == nil || result.Status == authoritystore.CommitCommitted {
		t.Fatalf("legacy revert=%+v err=%v", result, err)
	}
	after, err := rig.store.loadCurrentSnapshot(context.Background(), rig.bucket, rig.addr)
	if err != nil || after.ModelLayout != authoritystore.ModelLayoutV2 || after.RecordVersion != committed.Snapshot.RecordVersion {
		t.Fatalf("after revert=%+v err=%v", after, err)
	}
}

func TestRedisV2PartialOrUnknownManifestFailsClosedWithoutV1Fallback(t *testing.T) {
	rig := newTestRig(t)
	loaded := loadState(t, rig, "claim-v1", authoritystore.ModelState{"record": []byte("v1")})
	v2 := redisPlayerV2DummyState()
	migration, err := authoritystore.BuildActorModelMutation(authoritystore.ModelMutationIntentMigrateV1ToV2,
		authoritystore.ModelLayoutV2, v2, v2, []string{"record"})
	if err != nil {
		t.Fatal(err)
	}
	req := rig.commitReq("claim-v1", loaded.RecordVersion, "partial-v2", v2)
	req.ModelMutation = migration
	committed, err := rig.store.Commit(context.Background(), req)
	if err != nil || committed.Status != authoritystore.CommitCommitted {
		t.Fatalf("migration=%+v err=%v", committed, err)
	}
	allNames, err := fieldv2.ExactNames(actorrouting.DomainPlayer)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := rig.store.BatchReadModelFields(context.Background(), actorrouting.DomainPlayer, []uint64{rig.addr.ActorID}, allNames)
	if err != nil || len(rows) != 1 || rows[0].Err != nil || !reflect.DeepEqual(rows[0].ModelFields, v2) {
		t.Fatalf("complete v2 batch rows=%+v err=%v", rows, err)
	}
	actorKey, _ := rig.store.Keys().ActorRecord(rig.bucket, rig.addr.ActorID)
	if err := rig.client.HDel(context.Background(), actorKey.String(), "model:profile").Err(); err != nil {
		t.Fatal(err)
	}
	rows, err = rig.store.BatchReadModelFields(context.Background(), actorrouting.DomainPlayer, []uint64{rig.addr.ActorID}, allNames)
	if err != nil || len(rows) != 1 || rows[0].Err == nil {
		t.Fatalf("partial v2 batch rows=%+v err=%v", rows, err)
	}
	load, err := rig.store.LoadAndClaim(context.Background(), authoritystore.LoadAndClaimRequest{
		ActorAddress: rig.addr, AuthorityLayoutVersion: "v1", ShardWriteFence: rig.fence,
		ClaimOperationID: "partial-reload", NewWriterClaimID: "claim-partial",
		InitialModelFields: authoritystore.ModelState{"record": []byte("rescue-must-not-run")},
	})
	if !errors.Is(err, errMalformedBlob) || load.Status == authoritystore.LoadAndClaimClaimed {
		t.Fatalf("partial v2 load=%+v err=%v", load, err)
	}
	if exists, err := rig.client.HExists(context.Background(), actorKey.String(), "model:record").Result(); err != nil || exists {
		t.Fatalf("partial v2 resurrected record exists=%v err=%v", exists, err)
	}
}

func TestRedisMutationMigrationRaceHasOneLinearizedRecordVersion(t *testing.T) {
	rig := newTestRig(t)
	loaded := loadState(t, rig, "claim-race", authoritystore.ModelState{
		"public_profile": []byte("v1-profile"), "record": []byte("v1-record"),
	})
	v1Candidate := cloneModelState(loaded.ModelFields)
	v1Candidate["record"] = []byte("v1-mutated")
	v1Mutation, err := authoritystore.BuildActorModelMutation(authoritystore.ModelMutationIntentUpdate,
		authoritystore.ModelLayoutV1, v1Candidate, authoritystore.ModelState{"record": v1Candidate["record"]}, nil)
	if err != nil {
		t.Fatal(err)
	}
	v1Req := rig.commitReq("claim-race", loaded.RecordVersion, "redis-race-v1", v1Candidate)
	v1Req.ModelMutation = v1Mutation

	v2Candidate := redisPlayerV2DummyState()
	v2Mutation, err := authoritystore.BuildActorModelMutation(authoritystore.ModelMutationIntentMigrateV1ToV2,
		authoritystore.ModelLayoutV2, v2Candidate, v2Candidate, []string{"public_profile", "record"})
	if err != nil {
		t.Fatal(err)
	}
	v2Req := rig.commitReq("claim-race", loaded.RecordVersion, "redis-race-v2", v2Candidate)
	v2Req.ModelMutation = v2Mutation

	results := make(chan authoritystore.CommitResult, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, req := range []authoritystore.AuthorityCommitRequest{v1Req, v2Req} {
		wg.Add(1)
		go func(req authoritystore.AuthorityCommitRequest) {
			defer wg.Done()
			result, commitErr := rig.store.Commit(context.Background(), req)
			results <- result
			errs <- commitErr
		}(req)
	}
	wg.Wait()
	close(results)
	close(errs)
	for commitErr := range errs {
		if commitErr != nil {
			t.Fatal(commitErr)
		}
	}
	committed, conflicted := 0, 0
	for result := range results {
		switch result.Status {
		case authoritystore.CommitCommitted:
			committed++
		case authoritystore.CommitRecordVersionConflict:
			conflicted++
		default:
			t.Fatalf("race status=%s", result.Status)
		}
	}
	if committed != 1 || conflicted != 1 {
		t.Fatalf("race committed=%d conflicted=%d", committed, conflicted)
	}
	after, err := rig.store.loadCurrentSnapshot(context.Background(), rig.bucket, rig.addr)
	if err != nil || after.RecordVersion != loaded.RecordVersion+1 {
		t.Fatalf("race snapshot=%+v err=%v", after, err)
	}
}

func TestRedisV2UnknownManifestAndMalformedEnvelopeFailClosed(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testRig, rediskey.RedisKey, authoritystore.ModelState) error
	}{
		{"unknown-physical-field", func(rig *testRig, actorKey rediskey.RedisKey, _ authoritystore.ModelState) error {
			return rig.client.HSet(context.Background(), actorKey.String(), "model:unknown", "x").Err()
		}},
		{"unknown-manifest", func(rig *testRig, actorKey rediskey.RedisKey, state authoritystore.ModelState) error {
			corrupt := cloneModelState(state)
			corrupt["unknown"] = []byte("x")
			return rig.client.HSet(context.Background(), actorKey.String(),
				"_meta:model_manifest", manifestLP(corrupt), "_meta:model_field_count", len(corrupt)).Err()
		}},
		{"malformed-envelope-with-consistent-aggregate", func(rig *testRig, actorKey rediskey.RedisKey, state authoritystore.ModelState) error {
			corrupt := cloneModelState(state)
			corrupt["profile"] = append(corrupt["profile"], 0)
			return rig.client.HSet(context.Background(), actorKey.String(),
				"model:profile", corrupt["profile"],
				"_meta:model_total_bytes", authoritystore.ModelStateSerializedBytes(corrupt),
				"_meta:business_state_digest", digestHex(businessStateDigest(corrupt))).Err()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := newTestRig(t)
			loaded := loadState(t, rig, "claim-corrupt", authoritystore.ModelState{"record": []byte("v1")})
			v2 := redisPlayerV2DummyState()
			mutation, err := authoritystore.BuildActorModelMutation(authoritystore.ModelMutationIntentMigrateV1ToV2,
				authoritystore.ModelLayoutV2, v2, v2, []string{"record"})
			if err != nil {
				t.Fatal(err)
			}
			req := rig.commitReq("claim-corrupt", loaded.RecordVersion, "corrupt-v2", v2)
			req.ModelMutation = mutation
			if committed, err := rig.store.Commit(context.Background(), req); err != nil || committed.Status != authoritystore.CommitCommitted {
				t.Fatalf("migration=%+v err=%v", committed, err)
			}
			actorKey, _ := rig.store.Keys().ActorRecord(rig.bucket, rig.addr.ActorID)
			if err := tc.mutate(rig, actorKey, v2); err != nil {
				t.Fatal(err)
			}
			if _, err := rig.store.loadCurrentSnapshot(context.Background(), rig.bucket, rig.addr); !errors.Is(err, errMalformedBlob) {
				t.Fatalf("corrupt v2 err=%v", err)
			}
		})
	}
}

func loadState(t *testing.T, rig *testRig, claim string, state authoritystore.ModelState) authoritystore.AuthoritySnapshot {
	t.Helper()
	result, err := rig.store.LoadAndClaim(context.Background(), authoritystore.LoadAndClaimRequest{
		ActorAddress: rig.addr, AuthorityLayoutVersion: "v1", ShardWriteFence: rig.fence,
		ClaimOperationID: "load-" + claim, NewWriterClaimID: claim, InitialModelFields: state,
	})
	if err != nil || result.Status != authoritystore.LoadAndClaimClaimed {
		t.Fatalf("load=%+v err=%v", result, err)
	}
	return result.Snapshot
}

func redisPlayerV2DummyState() authoritystore.ModelState {
	state := authoritystore.ModelState{}
	for _, name := range []string{"compendium", "economy", "inventory", "mail_claim_state", "pet", "profile", "progression", "task"} {
		encoded, err := fieldv2.Marshal(actorrouting.DomainPlayer, name, []byte(name))
		if err != nil {
			panic(err)
		}
		state[name] = encoded
	}
	return state
}
