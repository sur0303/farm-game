package main

import (
	"context"
	"fmt"
	"testing"

	"farmgame/internal/actormodel/fieldv2"
	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
)

func r2ModelState(t *testing.T, domain actorrouting.RouteDomain, generation int) authoritystore.ModelState {
	t.Helper()
	names, err := fieldv2.ExactNames(domain)
	if err != nil {
		t.Fatal(err)
	}
	state := make(authoritystore.ModelState, len(names))
	for _, name := range names {
		state[name], err = fieldv2.Marshal(domain, name, []byte(fmt.Sprintf("cap-r2-%s-%s-v%d", domain, name, generation)))
		if err != nil {
			t.Fatal(err)
		}
	}
	return state
}

func cloneR2ModelState(source authoritystore.ModelState) authoritystore.ModelState {
	result := make(authoritystore.ModelState, len(source))
	for name, value := range source {
		result[name] = append([]byte(nil), value...)
	}
	return result
}

// TestCAPRedisR2ModelV2MigrationAndSparseCommit is opt-in and runs only on the
// unique loopback Redis supplied by the R2 qualification runner. It proves
// both actor domains migrate atomically, remove model:record, stay on V2, and
// accept a real sparse V2 commit without any V1 fallback.
func TestCAPRedisR2ModelV2MigrationAndSparseCommit(t *testing.T) {
	environment, _, _ := faultGateEnvironment(t)
	ctx := context.Background()
	migrations, sparseV1, sparseV2 := 0, 0, 0
	for _, domain := range []actorrouting.RouteDomain{actorrouting.DomainPlayer, actorrouting.DomainFarm} {
		seed, err := loadActor(ctx, environment, domain, "uniform", []byte("cap-r2-v1"))
		if err != nil {
			t.Fatal(err)
		}

		v1Candidate := authoritystore.ModelState{"record": []byte("cap-r2-v1-sparse")}
		v1Mutation, err := authoritystore.BuildActorModelMutation(authoritystore.ModelMutationIntentUpdate,
			authoritystore.ModelLayoutV1, v1Candidate, v1Candidate, nil)
		if err != nil {
			t.Fatal(err)
		}
		v1Request := commitRequest(seed, "AR2_V1_SPARSE_QUALIFICATION", []byte("v1-sparse"), false)
		v1Request.CandidateState, v1Request.ModelMutation = v1Candidate, v1Mutation
		v1Result, err := environment.authority.Commit(ctx, v1Request)
		if err != nil || v1Result.Status != authoritystore.CommitCommitted || v1Result.Snapshot.ModelLayout != authoritystore.ModelLayoutV1 {
			t.Fatalf("%s V1 sparse result=%+v err=%v", domain, v1Result, err)
		}
		sparseV1++
		seed.snapshot = v1Result.Snapshot

		v2Candidate := r2ModelState(t, domain, 1)
		migration, err := authoritystore.BuildActorModelMutation(authoritystore.ModelMutationIntentMigrateV1ToV2,
			authoritystore.ModelLayoutV2, v2Candidate, v2Candidate, []string{"record"})
		if err != nil {
			t.Fatal(err)
		}
		migrationRequest := commitRequest(seed, "AR2_MODEL_MIGRATION", []byte("migrate-v2"), false)
		migrationRequest.CandidateState, migrationRequest.ModelMutation = v2Candidate, migration
		migrationResult, err := environment.authority.Commit(ctx, migrationRequest)
		if err != nil || migrationResult.Status != authoritystore.CommitCommitted ||
			migrationResult.Snapshot.ModelLayout != authoritystore.ModelLayoutV2 ||
			migrationResult.Snapshot.RecordVersion != v1Result.Snapshot.RecordVersion+1 {
			t.Fatalf("%s migration result=%+v err=%v", domain, migrationResult, err)
		}
		migrations++
		seed.snapshot = migrationResult.Snapshot

		v2Next := cloneR2ModelState(migrationResult.Snapshot.ModelFields)
		names, err := fieldv2.ExactNames(domain)
		if err != nil {
			t.Fatal(err)
		}
		v2Next[names[0]], err = fieldv2.Marshal(domain, names[0], []byte("cap-r2-v2-sparse-next"))
		if err != nil {
			t.Fatal(err)
		}
		upsert := authoritystore.ModelState{names[0]: v2Next[names[0]]}
		v2Mutation, err := authoritystore.BuildActorModelMutation(authoritystore.ModelMutationIntentUpdate,
			authoritystore.ModelLayoutV2, v2Next, upsert, nil)
		if err != nil {
			t.Fatal(err)
		}
		v2Request := commitRequest(seed, "AR2_V2_SPARSE_QUALIFICATION", []byte("v2-sparse"), false)
		v2Request.CandidateState, v2Request.ModelMutation = v2Next, v2Mutation
		v2Result, err := environment.authority.Commit(ctx, v2Request)
		if err != nil || v2Result.Status != authoritystore.CommitCommitted ||
			v2Result.Snapshot.ModelLayout != authoritystore.ModelLayoutV2 ||
			v2Result.Snapshot.RecordVersion != migrationResult.Snapshot.RecordVersion+1 {
			t.Fatalf("%s V2 sparse result=%+v err=%v", domain, v2Result, err)
		}
		sparseV2++

		actorKey, err := environment.authority.Keys().ActorRecord(seed.bucket, seed.address.ActorID)
		if err != nil {
			t.Fatal(err)
		}
		exists, err := environment.client.HExists(ctx, actorKey.String(), "model:record").Result()
		if err != nil || exists {
			t.Fatalf("%s V1 fallback field exists=%v err=%v", domain, exists, err)
		}
		rows, err := environment.authority.BatchReadModelFields(ctx, domain, []uint64{seed.address.ActorID}, names)
		if err != nil || len(rows) != 1 || rows[0].Err != nil || rows[0].ModelLayout != authoritystore.ModelLayoutV2 {
			t.Fatalf("%s V2 read rows=%+v err=%v", domain, rows, err)
		}
	}
	if migrations != 2 || sparseV1 != 2 || sparseV2 != 2 {
		t.Fatalf("migration=%d sparse_v1=%d sparse_v2=%d", migrations, sparseV1, sparseV2)
	}
	if err := environment.deleteNamespace(ctx); err != nil {
		t.Fatal(err)
	}
	residual, err := environment.namespaceResidual(ctx)
	if err != nil || residual != 0 {
		t.Fatalf("namespace residual=%d err=%v", residual, err)
	}
	t.Logf("R2_MODEL_V2_PASS domains=2 migrations=2 sparse_v1=2 sparse_v2=2 v1_fallback=0 namespace_residual=0")
}
