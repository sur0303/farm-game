package authority

import (
	"context"
	"testing"

	"farmgame/internal/storage/authoritystore"
)

func TestRedisMigrationTimeoutBeforeAndAfterApplyResolution(t *testing.T) {
	for _, tc := range []struct {
		name       string
		afterApply bool
		want       authoritystore.FenceResolutionStatus
	}{
		{"before_apply", false, authoritystore.FenceResolutionNotCommitted},
		{"after_apply", true, authoritystore.FenceResolutionCommitted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			drop := &dropClient{}
			rig := newDropRig(t, drop)
			loaded := loadState(t, rig, "claim-migration", authoritystore.ModelState{
				"public_profile": []byte("v1-profile"), "record": []byte("v1-record"),
			})
			candidate := redisPlayerV2DummyState()
			mutation, err := authoritystore.BuildActorModelMutation(authoritystore.ModelMutationIntentMigrateV1ToV2,
				authoritystore.ModelLayoutV2, candidate, candidate, []string{"public_profile", "record"})
			if err != nil {
				t.Fatal(err)
			}
			req := rig.commitReq("claim-migration", loaded.RecordVersion, "redis-timeout-migration", candidate)
			req.ModelMutation = mutation
			drop.mu.Lock()
			if tc.afterApply {
				drop.dropAfter = 1
			} else {
				drop.dropBefore = 1
			}
			drop.mu.Unlock()
			unknown, err := rig.store.Commit(context.Background(), req)
			if err != nil || unknown.Status != authoritystore.CommitTimeoutUnknown {
				t.Fatalf("unknown=%+v err=%v", unknown, err)
			}

			resolved := rig.fenceAndClaim(t, "claim-migration", "claim-resolved", "resolve-redis-migration", req, loaded)
			if resolved.Status != tc.want {
				t.Fatalf("resolved=%+v want=%s", resolved, tc.want)
			}
			if tc.want == authoritystore.FenceResolutionCommitted {
				if resolved.Snapshot.ModelLayout != authoritystore.ModelLayoutV2 {
					t.Fatalf("committed layout=%s", resolved.Snapshot.ModelLayout)
				}
				if _, present := resolved.Snapshot.ModelFields["record"]; present {
					t.Fatal("committed timeout retained model:record")
				}
			} else if resolved.Snapshot.ModelLayout != authoritystore.ModelLayoutV1 ||
				string(resolved.Snapshot.ModelFields["record"]) != "v1-record" {
				t.Fatalf("not committed changed v1=%+v", resolved.Snapshot)
			}

			// An old-holder retry is fenced after the cut and cannot turn a
			// NOT_COMMITTED resolution into a late migration.
			late, err := rig.store.Commit(context.Background(), req)
			if err != nil || late.Status != authoritystore.CommitWriterClaimMismatch {
				t.Fatalf("late old holder=%+v err=%v", late, err)
			}
			stable, err := rig.store.FenceAndClaim(context.Background(), rig.fenceReq(
				"claim-migration", "claim-resolved", "resolve-redis-migration", req, loaded))
			if err != nil || stable.Status != tc.want {
				t.Fatalf("stable resolution=%+v err=%v", stable, err)
			}

			retry := req
			retry.ExpectedWriterClaimID = "claim-resolved"
			final, err := rig.store.Commit(context.Background(), retry)
			if err != nil {
				t.Fatal(err)
			}
			wantCommit := authoritystore.CommitCommitted
			if tc.want == authoritystore.FenceResolutionCommitted {
				wantCommit = authoritystore.CommitAlreadyCommitted
			}
			if final.Status != wantCommit || final.Snapshot.ModelLayout != authoritystore.ModelLayoutV2 {
				t.Fatalf("final=%+v want=%s", final, wantCommit)
			}
		})
	}
}
