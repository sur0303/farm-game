package authoritystore

import (
	"context"
	"sync"
	"testing"
)

func TestMigrationTimeoutUnknownResolvesWithoutFallbackOrRemarshal(t *testing.T) {
	for _, tc := range []struct {
		name       string
		fault      FaultMode
		resolution FenceResolutionStatus
	}{
		{"before_apply", FaultTimeoutUnknownBeforeApply, FenceResolutionNotCommitted},
		{"after_apply", FaultTimeoutUnknownAfterApply, FenceResolutionCommitted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inner, address, fence := newTestStore(t)
			loaded := mustLoadWithState(t, inner, address, fence, "claim-migrate", ModelState{
				"public_profile": []byte("v1-profile"), "record": []byte("v1-record"),
			})
			candidate := exactPlayerV2DummyState()
			mutation, err := BuildActorModelMutation(ModelMutationIntentMigrateV1ToV2, ModelLayoutV2,
				candidate, candidate, []string{"public_profile", "record"})
			if err != nil {
				t.Fatal(err)
			}
			req := testCommit(address, fence, "claim-migrate", loaded.RecordVersion, "timeout-migration", candidate)
			req.ModelMutation = mutation

			faults := NewFaultStore(inner)
			faults.Inject(MethodCommit, tc.fault)
			unknown, err := faults.Commit(context.Background(), req)
			if err != nil || unknown.Status != CommitTimeoutUnknown {
				t.Fatalf("unknown=%+v err=%v", unknown, err)
			}
			resolved, err := faults.FenceAndClaim(context.Background(), FenceAndClaimRequest{
				ResolveOperationID: "resolve-timeout-migration", ActorAddress: address,
				AuthorityLayoutVersion: "auth-v1", ShardWriteFence: fence,
				OldWriterClaimID: "claim-migrate", NewWriterClaimID: "claim-after-resolve",
				OriginalIdempotencyKey: req.IdempotencyKey, CanonicalPayloadHash: req.CanonicalPayloadHash,
				BeforeRecordVersion: loaded.RecordVersion, BeforeBusinessStateDigest: loaded.BusinessStateDigest,
			})
			if err != nil || resolved.Status != tc.resolution {
				t.Fatalf("resolved=%+v err=%v", resolved, err)
			}
			if tc.resolution == FenceResolutionCommitted {
				if resolved.Snapshot.ModelLayout != ModelLayoutV2 {
					t.Fatalf("committed resolution layout=%s", resolved.Snapshot.ModelLayout)
				}
				if _, found := resolved.Snapshot.ModelFields["record"]; found {
					t.Fatal("committed resolution retained v1 record")
				}
			} else {
				if resolved.Snapshot.ModelLayout != ModelLayoutV1 || string(resolved.Snapshot.ModelFields["record"]) != "v1-record" {
					t.Fatalf("not committed resolution changed v1=%+v", resolved.Snapshot)
				}
			}

			// Retry uses the byte-identical candidate/mutation/receipt attempt; only
			// the resolver-installed WriterClaim changes. Receipt lookup remains
			// before RecordVersion, so both outcomes converge idempotently.
			retry := req
			retry.ExpectedWriterClaimID = "claim-after-resolve"
			result, err := faults.Commit(context.Background(), retry)
			if err != nil {
				t.Fatal(err)
			}
			want := CommitCommitted
			if tc.resolution == FenceResolutionCommitted {
				want = CommitAlreadyCommitted
			}
			if result.Status != want || result.Snapshot.ModelLayout != ModelLayoutV2 {
				t.Fatalf("retry=%+v want=%s", result, want)
			}
			if _, found := result.Snapshot.ModelFields["record"]; found {
				t.Fatal("retry/final state retained v1 record")
			}
		})
	}
}

func TestMutationMigrationRaceHasOneLinearizedRecordVersion(t *testing.T) {
	store, address, fence := newTestStore(t)
	loaded := mustLoadWithState(t, store, address, fence, "claim-race", ModelState{
		"public_profile": []byte("v1-profile"), "record": []byte("v1-record"),
	})
	v1Candidate := cloneModelState(loaded.ModelFields)
	v1Candidate["record"] = []byte("v1-mutated")
	v1Mutation, err := BuildActorModelMutation(ModelMutationIntentUpdate, ModelLayoutV1, v1Candidate,
		ModelState{"record": v1Candidate["record"]}, nil)
	if err != nil {
		t.Fatal(err)
	}
	v1Req := testCommit(address, fence, "claim-race", loaded.RecordVersion, "race-v1", v1Candidate)
	v1Req.ModelMutation = v1Mutation

	v2Candidate := exactPlayerV2DummyState()
	v2Mutation, err := BuildActorModelMutation(ModelMutationIntentMigrateV1ToV2, ModelLayoutV2, v2Candidate,
		v2Candidate, []string{"public_profile", "record"})
	if err != nil {
		t.Fatal(err)
	}
	v2Req := testCommit(address, fence, "claim-race", loaded.RecordVersion, "race-v2", v2Candidate)
	v2Req.ModelMutation = v2Mutation

	results := make(chan CommitResult, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, req := range []AuthorityCommitRequest{v1Req, v2Req} {
		wg.Add(1)
		go func(req AuthorityCommitRequest) {
			defer wg.Done()
			result, err := store.Commit(context.Background(), req)
			results <- result
			errs <- err
		}(req)
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	committed, conflicted := 0, 0
	for result := range results {
		switch result.Status {
		case CommitCommitted:
			committed++
		case CommitRecordVersionConflict:
			conflicted++
		default:
			t.Fatalf("race status=%s", result.Status)
		}
	}
	if committed != 1 || conflicted != 1 {
		t.Fatalf("race committed=%d conflicted=%d", committed, conflicted)
	}
	after, _ := store.Inspect(address)
	if after.RecordVersion != loaded.RecordVersion+1 {
		t.Fatalf("race record version=%d", after.RecordVersion)
	}
}
