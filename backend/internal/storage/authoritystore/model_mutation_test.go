package authoritystore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"farmgame/internal/actormodel/fieldv2"
	"farmgame/internal/actorrouting"
)

func TestMemoryStoreSparseMutationAndAtomicV1ToV2Migration(t *testing.T) {
	store, address, fence := newTestStore(t)
	loaded := mustLoadWithState(t, store, address, fence, "claim-v1", ModelState{
		"public_profile": []byte("derived-v1"),
		"record":         []byte("record-v1"),
	})
	if loaded.ModelLayout != ModelLayoutV1 {
		t.Fatalf("initial model layout=%s", loaded.ModelLayout)
	}

	sparseCandidate := cloneModelState(loaded.ModelFields)
	sparseCandidate["record"] = []byte("record-v1-next")
	sparse, err := BuildActorModelMutation(ModelMutationIntentUpdate, ModelLayoutV1, sparseCandidate,
		ModelState{"record": sparseCandidate["record"]}, nil)
	if err != nil {
		t.Fatal(err)
	}
	sparseReq := testCommit(address, fence, "claim-v1", loaded.RecordVersion, "sparse-v1", sparseCandidate)
	sparseReq.ModelMutation = sparse
	sparseReq.AttachedOrders = []AttachedOrder{{OrderID: "order-sparse", OrderType: "CARE_REWARD", Payload: []byte("due"), DueAtMs: 123}}
	sparseResult, err := store.Commit(context.Background(), sparseReq)
	if err != nil || sparseResult.Status != CommitCommitted {
		t.Fatalf("sparse result=%+v err=%v", sparseResult, err)
	}
	if sparseResult.Snapshot.RecordVersion != loaded.RecordVersion+1 ||
		string(sparseResult.Snapshot.ModelFields["public_profile"]) != "derived-v1" || store.AttachedOrderCount(address) != 1 {
		t.Fatalf("sparse snapshot=%+v orders=%d", sparseResult.Snapshot, store.AttachedOrderCount(address))
	}

	v2 := exactPlayerV2DummyState()
	migration, err := BuildActorModelMutation(ModelMutationIntentMigrateV1ToV2, ModelLayoutV2, v2, v2,
		[]string{"public_profile", "record"})
	if err != nil {
		t.Fatal(err)
	}
	migrationReq := testCommit(address, fence, "claim-v1", sparseResult.Snapshot.RecordVersion, "migrate-v2", v2)
	migrationReq.ModelMutation = migration
	committed, err := store.Commit(context.Background(), migrationReq)
	if err != nil || committed.Status != CommitCommitted {
		t.Fatalf("migration=%+v err=%v", committed, err)
	}
	if committed.Snapshot.ModelLayout != ModelLayoutV2 || committed.Snapshot.RecordVersion != sparseResult.Snapshot.RecordVersion+1 {
		t.Fatalf("migration snapshot=%+v", committed.Snapshot)
	}
	if _, found := committed.Snapshot.ModelFields["record"]; found || !reflect.DeepEqual(committed.Snapshot.ModelFields, v2) {
		t.Fatalf("migration retained v1 or changed candidate: %+v", committed.Snapshot.ModelFields)
	}

	replay, err := store.Commit(context.Background(), migrationReq)
	if err != nil || replay.Status != CommitAlreadyCommitted || replay.Snapshot.ModelLayout != ModelLayoutV2 {
		t.Fatalf("migration replay=%+v err=%v", replay, err)
	}

	legacy := testCommit(address, fence, "claim-v1", committed.Snapshot.RecordVersion, "revert-v1",
		ModelState{"record": []byte("must-not-revert")})
	if _, err := store.Commit(context.Background(), legacy); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("legacy v2 revert err=%v", err)
	}
	after, _ := store.Inspect(address)
	if after.ModelLayout != ModelLayoutV2 || after.RecordVersion != committed.Snapshot.RecordVersion {
		t.Fatalf("revert attempt changed snapshot=%+v", after)
	}
}

func TestExplicitModelMutationFailsClosedOnManifestDigestAndSparseMismatch(t *testing.T) {
	store, address, fence := newTestStore(t)
	loaded := mustLoadWithState(t, store, address, fence, "claim-validation", ModelState{
		"record": []byte("record-v1"), "public_profile": []byte("profile-v1"),
	})
	candidate := cloneModelState(loaded.ModelFields)
	candidate["record"] = []byte("next")
	valid, err := BuildActorModelMutation(ModelMutationIntentUpdate, ModelLayoutV1, candidate,
		ModelState{"record": []byte("next")}, nil)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name   string
		mutate func(*ActorModelMutation)
	}{
		{"manifest", func(m *ActorModelMutation) { m.ResultingManifest = []string{"record"} }},
		{"digest", func(m *ActorModelMutation) { m.ResultingBusinessStateDigest[0] ^= 0xff }},
		{"bytes", func(m *ActorModelMutation) { m.ResultingTotalBytes++ }},
		{"undeclared", func(m *ActorModelMutation) { m.ModelUpserts = ModelState{} }},
		{"duplicate-delete", func(m *ActorModelMutation) { m.ModelDeletes = []string{"missing", "missing"} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mutation := *valid
			mutation.ModelUpserts = cloneModelState(valid.ModelUpserts)
			mutation.ModelDeletes = append([]string(nil), valid.ModelDeletes...)
			mutation.ResultingManifest = append([]string(nil), valid.ResultingManifest...)
			tc.mutate(&mutation)
			req := testCommit(address, fence, "claim-validation", loaded.RecordVersion, "invalid-"+tc.name, candidate)
			req.ModelMutation = &mutation
			if _, err := store.Commit(context.Background(), req); !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("err=%v", err)
			}
		})
	}
	malformedV2 := exactPlayerV2DummyState()
	malformedV2["profile"] = []byte("not-an-amf2-envelope")
	malformedMigration, err := BuildActorModelMutation(ModelMutationIntentMigrateV1ToV2, ModelLayoutV2,
		malformedV2, malformedV2, []string{"public_profile", "record"})
	if err != nil {
		t.Fatal(err)
	}
	malformedReq := testCommit(address, fence, "claim-validation", loaded.RecordVersion, "invalid-v2-envelope", malformedV2)
	malformedReq.ModelMutation = malformedMigration
	if _, err := store.Commit(context.Background(), malformedReq); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("malformed v2 envelope err=%v", err)
	}
	after, _ := store.Inspect(address)
	if after.RecordVersion != loaded.RecordVersion || !reflect.DeepEqual(after.ModelFields, loaded.ModelFields) {
		t.Fatalf("invalid mutations changed snapshot=%+v", after)
	}
}

func TestBorrowedModelMutationValidationAvoidsSecondDeepCopy(t *testing.T) {
	candidate := ModelState{"record": []byte("next")}
	mutation, err := BuildActorModelMutation(ModelMutationIntentUpdate, ModelLayoutV1, candidate,
		ModelState{"record": candidate["record"]}, nil)
	if err != nil {
		t.Fatal(err)
	}
	address := ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: 7}
	defensive, _, err := NormalizeActorModelMutation(address, candidate, mutation)
	if err != nil {
		t.Fatal(err)
	}
	borrowed, _, err := NormalizeActorModelMutationBorrowed(address, candidate, mutation)
	if err != nil {
		t.Fatal(err)
	}

	mutation.ModelUpserts["record"][0] = 'N'
	if string(defensive.ModelUpserts["record"]) != "next" {
		t.Fatal("defensive normalization retained caller-owned model bytes")
	}
	if string(borrowed.ModelUpserts["record"]) != "Next" {
		t.Fatal("borrowed normalization unexpectedly copied model bytes")
	}
}

func TestBorrowedModelMutationBuilderReusesFrozenUpsertBytes(t *testing.T) {
	candidate := ModelState{"record": []byte("immutable-candidate")}
	upserts := ModelState{"record": candidate["record"]}
	defensive, err := BuildActorModelMutation(ModelMutationIntentUpdate, ModelLayoutV1, candidate, upserts, nil)
	if err != nil {
		t.Fatal(err)
	}
	borrowed, err := BuildActorModelMutationBorrowed(ModelMutationIntentUpdate, ModelLayoutV1, candidate, upserts, nil)
	if err != nil {
		t.Fatal(err)
	}
	originalValue := upserts["record"]
	defensiveValue := defensive.ModelUpserts["record"]
	borrowedValue := borrowed.ModelUpserts["record"]
	if &defensiveValue[0] == &originalValue[0] {
		t.Fatal("defensive builder retained caller-owned upsert bytes")
	}
	if &borrowedValue[0] != &originalValue[0] {
		t.Fatal("borrowed builder copied immutable upsert bytes")
	}
	address := ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: 7}
	if _, _, err := NormalizeActorModelMutationBorrowed(address, candidate, borrowed); err != nil {
		t.Fatalf("borrowed mutation failed exact validation: %v", err)
	}
}

func BenchmarkNormalizeActorModelMutation(b *testing.B) {
	candidate := make(ModelState, 8)
	for index := 0; index < 8; index++ {
		candidate[fmt.Sprintf("field_%d", index)] = bytes.Repeat([]byte{byte(index + 1)}, 64<<10)
	}
	mutation, err := BuildActorModelMutation(ModelMutationIntentUpdate, ModelLayoutV1, candidate, candidate, nil)
	if err != nil {
		b.Fatal(err)
	}
	address := ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: 7}
	b.Run("defensive", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			if _, _, err := NormalizeActorModelMutation(address, candidate, mutation); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("borrowed", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			if _, _, err := NormalizeActorModelMutationBorrowed(address, candidate, mutation); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkBuildActorModelMutation(b *testing.B) {
	candidate := make(ModelState, 8)
	for index := 0; index < 8; index++ {
		candidate[fmt.Sprintf("field_%d", index)] = bytes.Repeat([]byte{byte(index + 1)}, 64<<10)
	}
	upserts := ModelState{"field_3": candidate["field_3"]}
	b.Run("sparse_defensive", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(ModelStateSerializedBytes(candidate)))
		for index := 0; index < b.N; index++ {
			if _, err := BuildActorModelMutation(ModelMutationIntentUpdate, ModelLayoutV1, candidate, upserts, nil); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("sparse_borrowed", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(ModelStateSerializedBytes(candidate)))
		for index := 0; index < b.N; index++ {
			if _, err := BuildActorModelMutationBorrowed(ModelMutationIntentUpdate, ModelLayoutV1, candidate, upserts, nil); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func mustLoadWithState(t *testing.T, store *MemoryStore, address ActorAddress, fence ShardWriteFence, claim string, state ModelState) AuthoritySnapshot {
	t.Helper()
	result, err := store.LoadAndClaim(context.Background(), LoadAndClaimRequest{
		ActorAddress: address, AuthorityLayoutVersion: "auth-v1", ShardWriteFence: fence,
		ClaimOperationID: "load-" + claim, NewWriterClaimID: claim, InitialModelFields: state,
	})
	if err != nil || result.Status != LoadAndClaimClaimed {
		t.Fatalf("load=%+v err=%v", result, err)
	}
	return result.Snapshot
}

func exactPlayerV2DummyState() ModelState {
	state := ModelState{}
	for _, name := range []string{"compendium", "economy", "inventory", "mail_claim_state", "pet", "profile", "progression", "task"} {
		encoded, err := fieldv2.Marshal(actorrouting.DomainPlayer, name, []byte(name))
		if err != nil {
			panic(err)
		}
		state[name] = encoded
	}
	return state
}
