package actor

import (
	"context"
	"sort"
	"testing"
	"time"

	farmpb "farmgame/api/gen/farm"
	"farmgame/internal/actormodel/fieldv2"
	"farmgame/internal/actorrouting"
	"farmgame/internal/logic/actor/authority"
	"farmgame/internal/storage/authoritystore"

	"google.golang.org/protobuf/proto"
)

func enabledFarmV2Policy() fieldv2.ConversionPolicy {
	return fieldv2.ConversionPolicy{Farm: fieldv2.DomainPolicy{ConversionEnabled: true, CohortPermyriad: fieldv2.CohortBasis}}
}

func TestFarmAuthorityRuntimeMigratesAndStaysV2(t *testing.T) {
	const farmID = uint64(29_201)
	for name, rig := range ar1Rigs(t, farmID) {
		t.Run(name, func(t *testing.T) {
			rig.registry.modelFieldV2Conversion = enabledFarmV2Policy()
			value, err := rig.registry.Submit(context.Background(), farmID, func(_ context.Context, actor *FarmActor) (any, error) {
				return actor.Snapshot(time.UnixMilli(1_800_000_000_000)), nil
			})
			if err != nil || len(value.(*farmpb.FarmSnapshot).GetPlots()) != DefaultPlotCount {
				t.Fatalf("snapshot=%v err=%v", value, err)
			}
			assertFarmV2PhysicalTruth(t, rig.modelReader, farmID, 2)
			migration, ok := rig.fault.LastCommitRequest()
			if !ok || migration.ModelMutation == nil || migration.ModelMutation.Intent != authoritystore.ModelMutationIntentMigrateV1ToV2 ||
				!farmContainsName(migration.ModelMutation.ModelDeletes, "record") {
				t.Fatalf("migration=%+v", migration.ModelMutation)
			}

			if _, err := ar1Plant(context.Background(), rig.registry, farmID, "v2-plant", []byte("v2-plant")); err != nil {
				t.Fatal(err)
			}
			commit, ok := rig.fault.LastCommitRequest()
			if !ok || commit.ModelMutation == nil || commit.ModelMutation.Intent != authoritystore.ModelMutationIntentUpdate {
				t.Fatalf("commit=%+v", commit.ModelMutation)
			}
			wantDirty := []string{"activity_summary", "farm_meta", "opportunity_facts", "opportunity_header", "plots"}
			if got := farmSortedNames(commit.ModelMutation.ModelUpserts); !farmEqualNames(got, wantDirty) {
				t.Fatalf("plant dirty=%v", got)
			}
			assertFarmV2PhysicalTruth(t, rig.modelReader, farmID, 3)

			restarted := NewAuthorityRegistry(rig.provider, RegistryOptions{MailboxSize: 8})
			t.Cleanup(restarted.Stop)
			value, err = restarted.Submit(context.Background(), farmID, func(_ context.Context, actor *FarmActor) (any, error) {
				return actor.Snapshot(time.UnixMilli(1_800_000_000_100)), nil
			})
			if err != nil || value.(*farmpb.FarmSnapshot).GetFarmVersion() == 0 {
				t.Fatalf("restart snapshot=%v err=%v", value, err)
			}
			assertFarmV2PhysicalTruth(t, rig.modelReader, farmID, 3)
		})
	}
}

func TestFarmAuthorityMigrationResolvesUnknownBeforeAndAfterApply(t *testing.T) {
	for name, mode := range map[string]authoritystore.FaultMode{
		"before_apply": authoritystore.FaultTimeoutUnknownBeforeApply,
		"after_apply":  authoritystore.FaultTimeoutUnknownAfterApply,
	} {
		t.Run(name, func(t *testing.T) {
			const farmID = uint64(29_202)
			rig := ar1MemoryRig(t, farmID)
			rig.registry.modelFieldV2Conversion = enabledFarmV2Policy()
			rig.fault.Inject(authoritystore.MethodCommit, mode)
			_, err := rig.registry.Submit(context.Background(), farmID, func(_ context.Context, actor *FarmActor) (any, error) {
				return actor.recordVersion, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			wantCommits := uint64(1)
			if mode == authoritystore.FaultTimeoutUnknownBeforeApply {
				wantCommits = 2
			}
			if got := rig.fault.Calls(authoritystore.MethodCommit); got != wantCommits || rig.fault.Calls(authoritystore.MethodFenceAndClaim) != 1 {
				t.Fatalf("commit=%d fence=%d", got, rig.fault.Calls(authoritystore.MethodFenceAndClaim))
			}
			assertFarmV2PhysicalTruth(t, rig.modelReader, farmID, 2)
		})
	}
}

func TestFarmAuthorityMigrationCrashRestartFencesUnresolvedAttempt(t *testing.T) {
	for name, mode := range map[string]authoritystore.FaultMode{
		"before_apply": authoritystore.FaultTimeoutUnknownBeforeApply,
		"after_apply":  authoritystore.FaultTimeoutUnknownAfterApply,
	} {
		t.Run(name, func(t *testing.T) {
			const farmID = uint64(29_204)
			rig := ar1MemoryRig(t, farmID)
			rig.registry.modelFieldV2Conversion = enabledFarmV2Policy()
			rig.fault.Inject(authoritystore.MethodCommit, mode)
			rig.fault.Inject(authoritystore.MethodFenceAndClaim, authoritystore.FaultUnavailableNotSent)
			if _, err := rig.registry.Submit(context.Background(), farmID, func(_ context.Context, actor *FarmActor) (any, error) {
				return actor.recordVersion, nil
			}); err == nil {
				t.Fatal("unresolved migration unexpectedly activated")
			}
			restarted := NewAuthorityRegistry(rig.provider, RegistryOptions{MailboxSize: 8, ModelFieldV2Conversion: enabledFarmV2Policy()})
			t.Cleanup(restarted.Stop)
			value, err := restarted.Submit(context.Background(), farmID, func(_ context.Context, actor *FarmActor) (any, error) {
				return actor.recordVersion, nil
			})
			if err != nil || value.(uint64) != 2 {
				t.Fatalf("version=%v err=%v", value, err)
			}
			assertFarmV2PhysicalTruth(t, rig.modelReader, farmID, 2)
		})
	}
}

func TestFarmRuntimeDecodeRejectsPartialAndMixedV2BeforeInstall(t *testing.T) {
	record := InitialRecord(29_203)
	state, _, _, err := buildFarmAuthorityCandidate(authoritystore.ModelLayoutV2, record)
	if err != nil {
		t.Fatal(err)
	}
	partial := cloneFarmModelState(state)
	delete(partial, "plots")
	if _, _, err := decodeFarmAuthoritySnapshot(authoritystore.AuthoritySnapshot{
		RecordVersion: 2, ModelLayout: authoritystore.ModelLayoutV2, ModelFields: partial,
	}, record.GetFarmId()); err == nil {
		t.Fatal("partial v2 was installable")
	}
	mixed := cloneFarmModelState(state)
	mixed["record"] = []byte("rescue-forbidden")
	if _, _, err := decodeFarmAuthoritySnapshot(authoritystore.AuthoritySnapshot{
		RecordVersion: 2, ModelLayout: authoritystore.ModelLayoutV2, ModelFields: mixed,
	}, record.GetFarmId()); err == nil {
		t.Fatal("mixed v2 was installable")
	}
}

func TestSameInputFarmV1V2RuntimeComparisonFixture(t *testing.T) {
	const farmID = uint64(29_205)
	v1 := ar1MemoryRig(t, farmID)
	v2 := ar1MemoryRig(t, farmID)
	v2.registry.modelFieldV2Conversion = enabledFarmV2Policy()
	read := func(registry *Registry) (*farmpb.FarmRecord, uint64, *authority.Binding) {
		var record *farmpb.FarmRecord
		var version uint64
		var binding *authority.Binding
		_, err := registry.Submit(context.Background(), farmID, func(_ context.Context, actor *FarmActor) (any, error) {
			record = proto.Clone(actor.record).(*farmpb.FarmRecord)
			version = actor.recordVersion
			binding, _ = actor.store.(*authority.Binding)
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return record, version, binding
	}
	_, v1Start, _ := read(v1.registry)
	_, v2Start, _ := read(v2.registry)
	v1Result, err := ar1Plant(context.Background(), v1.registry, farmID, "same-input-plant", []byte("same-input-plant"))
	if err != nil {
		t.Fatal(err)
	}
	v2Result, err := ar1Plant(context.Background(), v2.registry, farmID, "same-input-plant", []byte("same-input-plant"))
	if err != nil {
		t.Fatal(err)
	}
	v1Record, v1Version, v1Binding := read(v1.registry)
	v2Record, v2Version, v2Binding := read(v2.registry)
	if v1Result.Status != v2Result.Status || !proto.Equal(v1Result.Result, v2Result.Result) || !proto.Equal(v1Result.Change, v2Result.Change) ||
		!proto.Equal(v1Record, v2Record) || v1Version-v1Start != 1 || v2Version-v2Start != 1 {
		t.Fatalf("v1 result=%+v version=%d/%d\nv2 result=%+v version=%d/%d", v1Result, v1Start, v1Version, v2Result, v2Start, v2Version)
	}
	v1Replay, err := ar1Plant(context.Background(), v1.registry, farmID, "same-input-plant", []byte("same-input-plant"))
	if err != nil {
		t.Fatal(err)
	}
	v2Replay, err := ar1Plant(context.Background(), v2.registry, farmID, "same-input-plant", []byte("same-input-plant"))
	if err != nil {
		t.Fatal(err)
	}
	_, v1ReplayVersion, _ := read(v1.registry)
	_, v2ReplayVersion, _ := read(v2.registry)
	if !v1Replay.Replayed || !v2Replay.Replayed || v1ReplayVersion != v1Version || v2ReplayVersion != v2Version {
		t.Fatalf("replay v1=%+v/%d v2=%+v/%d", v1Replay, v1ReplayVersion, v2Replay, v2ReplayVersion)
	}
	v1Commit, v1OK := v1.fault.LastCommitRequest()
	v2Commit, v2OK := v2.fault.LastCommitRequest()
	wantDirty := []string{"activity_summary", "farm_meta", "opportunity_facts", "opportunity_header", "plots"}
	if !v1OK || !v2OK || v1Commit.ModelMutation == nil || len(v1Commit.CandidateState) != 3 || v2Commit.ModelMutation == nil ||
		!farmEqualNames(farmSortedNames(v1Commit.ModelMutation.ModelUpserts), []string{"opportunity_facts", "opportunity_header", "record"}) ||
		!farmEqualNames(farmSortedNames(v2Commit.ModelMutation.ModelUpserts), wantDirty) {
		t.Fatalf("physical writes v1=%+v v2=%+v", v1Commit.ModelMutation, v2Commit.ModelMutation)
	}
	if v1Binding.PhysicalUsage().ModelBytes != authoritystore.ModelStateSerializedBytes(v1Commit.CandidateState) ||
		v2Binding.PhysicalUsage().ModelBytes != authoritystore.ModelStateSerializedBytes(v2Commit.CandidateState) ||
		v1Binding.PhysicalUsage().ReceiptItems != 1 || v2Binding.PhysicalUsage().ReceiptItems != 2 {
		t.Fatalf("usage v1=%+v v2=%+v", v1Binding.PhysicalUsage(), v2Binding.PhysicalUsage())
	}
	assertFarmV2PhysicalTruth(t, v2.modelReader, farmID, v2Version)
}

func assertFarmV2PhysicalTruth(t *testing.T, reader authoritystore.ModelFieldBatchReader, farmID, version uint64) {
	t.Helper()
	names, err := fieldv2.ExactNames(actorrouting.DomainFarm)
	if err != nil {
		t.Fatal(err)
	}
	requested := append(append([]string(nil), names...), "record")
	rows, err := reader.BatchReadModelFields(context.Background(), actorrouting.DomainFarm, []uint64{farmID}, requested)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%v err=%v", rows, err)
	}
	row := rows[0]
	if row.Err != nil || !row.Found || row.RecordVersion != version || row.ModelLayout != authoritystore.ModelLayoutV2 ||
		len(row.ModelFields) != len(names) || row.ModelFields["record"] != nil {
		t.Fatalf("row=%+v", row)
	}
	if err := fieldv2.ValidateExactFields(actorrouting.DomainFarm, row.ModelFields); err != nil {
		t.Fatal(err)
	}
}

func farmSortedNames(state authoritystore.ModelState) []string {
	names := make([]string, 0, len(state))
	for name := range state {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func farmContainsName(names []string, want string) bool {
	for _, name := range names {
		if name == want {
			return true
		}
	}
	return false
}

func farmEqualNames(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for index := range a {
		if a[index] != b[index] {
			return false
		}
	}
	return true
}
