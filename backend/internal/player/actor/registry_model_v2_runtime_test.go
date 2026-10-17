package actor

import (
	"context"
	"crypto/sha256"
	"sort"
	"testing"

	playerpb "farmgame/api/gen/player"
	"farmgame/internal/actormodel/fieldv2"
	"farmgame/internal/actorrouting"
	playeractormodelv2 "farmgame/internal/player/actormodelv2"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/workidentity"

	"google.golang.org/protobuf/proto"
)

func enabledPlayerV2Policy() fieldv2.ConversionPolicy {
	return fieldv2.ConversionPolicy{Player: fieldv2.DomainPolicy{ConversionEnabled: true, CohortPermyriad: fieldv2.CohortBasis}}
}

func TestPlayerAuthorityRuntimeMigratesAndStaysV2(t *testing.T) {
	const playerID = uint64(19_201)
	for name, rig := range ar1pRigs(t, playerID) {
		t.Run(name, func(t *testing.T) {
			rig.registry.modelFieldV2Conversion = enabledPlayerV2Policy()
			record, version, err := rig.registry.EnsureWithProfile(context.Background(), playerID, "v2-bootstrap", "v2_farmer", 4)
			if err != nil || record.GetPlayerId() != playerID || version != 2 {
				t.Fatalf("record=%v version=%d err=%v", record, version, err)
			}
			assertPlayerV2PhysicalTruth(t, rig.modelReader, playerID, 2)
			migration, ok := rig.fault.LastCommitRequest()
			if !ok || migration.ModelMutation == nil || migration.ModelMutation.Intent != authoritystore.ModelMutationIntentMigrateV1ToV2 ||
				!containsName(migration.ModelMutation.ModelDeletes, "record") || !containsName(migration.ModelMutation.ModelDeletes, "public_profile") {
				t.Fatalf("migration mutation=%+v", migration.ModelMutation)
			}

			result, err := rig.registry.SubmitVersioned(context.Background(), playerID, func(record *playerpb.PlayerRecord, _ uint64) (any, bool, error) {
				record.Progression.Level++
				return record.Progression.Level, true, nil
			})
			if err != nil || result.CommittedRecordVersion != 3 {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			commit, ok := rig.fault.LastCommitRequest()
			if !ok || commit.ModelMutation == nil || commit.ModelMutation.Intent != authoritystore.ModelMutationIntentUpdate {
				t.Fatalf("v2 commit=%+v", commit.ModelMutation)
			}
			if got := sortedModelNames(commit.ModelMutation.ModelUpserts); !equalNameList(got, []string{"progression"}) {
				t.Fatalf("dirty upserts=%v", got)
			}
			assertPlayerV2PhysicalTruth(t, rig.modelReader, playerID, 3)

			// A fresh Registry with the conversion gate disabled must still decode
			// and write the already-v2 row; no v1 rescue or reversion is allowed.
			restarted := NewAuthorityRegistry(rig.provider, RegistryOptions{MailboxSize: 8})
			t.Cleanup(restarted.Stop)
			reloaded, gotVersion, err := restarted.Read(context.Background(), playerID)
			if err != nil || gotVersion != 3 || reloaded.GetProgression().GetLevel() != 2 {
				t.Fatalf("reloaded=%v version=%d err=%v", reloaded, gotVersion, err)
			}
			assertPlayerV2PhysicalTruth(t, rig.modelReader, playerID, 3)
		})
	}
}

func TestPlayerAuthorityMigrationResolvesUnknownWithoutRemarshal(t *testing.T) {
	const playerID = uint64(19_202)
	for name, mode := range map[string]authoritystore.FaultMode{
		"before_apply": authoritystore.FaultTimeoutUnknownBeforeApply,
		"after_apply":  authoritystore.FaultTimeoutUnknownAfterApply,
	} {
		t.Run(name, func(t *testing.T) {
			rig := ar1pMemoryRig(t, playerID)
			rig.registry.modelFieldV2Conversion = enabledPlayerV2Policy()
			rig.fault.Inject(authoritystore.MethodCommit, mode)
			_, version, err := rig.registry.EnsureWithProfile(context.Background(), playerID, "unknown-bootstrap", "unknown_farmer", 2)
			if err != nil || version != 2 {
				t.Fatalf("version=%d err=%v", version, err)
			}
			wantCommits := uint64(1)
			if mode == authoritystore.FaultTimeoutUnknownBeforeApply {
				wantCommits = 2
			}
			if got := rig.fault.Calls(authoritystore.MethodCommit); got != wantCommits || rig.fault.Calls(authoritystore.MethodFenceAndClaim) != 1 {
				t.Fatalf("commit=%d fence=%d", got, rig.fault.Calls(authoritystore.MethodFenceAndClaim))
			}
			assertPlayerV2PhysicalTruth(t, rig.modelReader, playerID, 2)
		})
	}
}

func TestPlayerAuthorityMigrationCrashRestartFencesUnresolvedAttempt(t *testing.T) {
	for name, mode := range map[string]authoritystore.FaultMode{
		"before_apply": authoritystore.FaultTimeoutUnknownBeforeApply,
		"after_apply":  authoritystore.FaultTimeoutUnknownAfterApply,
	} {
		t.Run(name, func(t *testing.T) {
			const playerID = uint64(19_207)
			rig := ar1pMemoryRig(t, playerID)
			rig.registry.modelFieldV2Conversion = enabledPlayerV2Policy()
			rig.fault.Inject(authoritystore.MethodCommit, mode)
			rig.fault.Inject(authoritystore.MethodFenceAndClaim, authoritystore.FaultUnavailableNotSent)
			if _, _, err := rig.registry.EnsureWithProfile(context.Background(), playerID, "crash-restart", "crash_farmer", 3); err == nil {
				t.Fatal("unresolved migration unexpectedly activated")
			}
			// Simulate process loss: the replacement activation obtains a new
			// WriterClaim, fencing any delayed commit from the abandoned Binding.
			restarted := NewAuthorityRegistry(rig.provider, RegistryOptions{MailboxSize: 8, ModelFieldV2Conversion: enabledPlayerV2Policy()})
			t.Cleanup(restarted.Stop)
			record, version, err := restarted.Read(context.Background(), playerID)
			if err != nil || record.GetPlayerId() != playerID || version != 2 {
				t.Fatalf("record=%v version=%d err=%v", record, version, err)
			}
			assertPlayerV2PhysicalTruth(t, rig.modelReader, playerID, 2)
		})
	}
}

func TestBindingV2ReceiptOnlyCommitHasEmptyPhysicalUpserts(t *testing.T) {
	rig := ar1pMemoryRig(t, 19_203)
	rig.registry.modelFieldV2Conversion = enabledPlayerV2Policy()
	if _, _, err := rig.registry.EnsureWithProfile(context.Background(), 19_203, "receipt-only", "receipt_farmer", 1); err != nil {
		t.Fatal(err)
	}
	binding, err := rig.registry.bindingFor(context.Background(), 19_203, nil)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := binding.CurrentModelSnapshot()
	if err := binding.CASEpochModelStateWithOutcomeAndOrders(context.Background(), 19_203, snapshot.RecordVersion, 0,
		snapshot.ModelFields, authoritystore.StoredOperationOutcome{Status: authoritystore.TerminalSucceeded}, nil); err != nil {
		t.Fatal(err)
	}
	commit, ok := rig.fault.LastCommitRequest()
	if !ok || commit.ModelMutation == nil || len(commit.ModelMutation.ModelUpserts) != 0 || len(commit.ModelMutation.ModelDeletes) != 0 || binding.Version() != 3 {
		t.Fatalf("mutation=%+v version=%d", commit.ModelMutation, binding.Version())
	}
	if binding.HasPendingUnknown() {
		t.Fatal("receipt-only commit left unresolved state")
	}
}

func TestPlayerRuntimeDecodeRejectsPartialAndMixedV2BeforeInstall(t *testing.T) {
	record := initialRecord(19_204, "malformed-v2")
	state, err := playeractormodelv2.Encode(record)
	if err != nil {
		t.Fatal(err)
	}
	partial := clonePlayerRuntimeState(state)
	delete(partial, "inventory")
	if _, err := decodePlayerAuthoritySnapshot(authoritystore.AuthoritySnapshot{
		RecordVersion: 2, ModelLayout: authoritystore.ModelLayoutV2, ModelFields: partial,
	}, record.GetPlayerId()); err == nil {
		t.Fatal("partial v2 was installable")
	}
	mixed := clonePlayerRuntimeState(state)
	mixed["record"] = []byte("rescue-forbidden")
	if _, err := decodePlayerAuthoritySnapshot(authoritystore.AuthoritySnapshot{
		RecordVersion: 2, ModelLayout: authoritystore.ModelLayoutV2, ModelFields: mixed,
	}, record.GetPlayerId()); err == nil {
		t.Fatal("mixed v2 was installable")
	}
}

func TestV2AttachedOrderAndDueIndexRemainAtomicAndReplayable(t *testing.T) {
	const playerID = uint64(19_205)
	rig := ar1pRedisRig(t, playerID)
	rig.registry.modelFieldV2Conversion = enabledPlayerV2Policy()
	if _, _, err := rig.registry.EnsureWithProfile(context.Background(), playerID, "v2-order", "order_farmer", 1); err != nil {
		t.Fatal(err)
	}
	binding, err := rig.registry.bindingFor(context.Background(), playerID, nil)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := binding.CurrentModelSnapshot()
	payload := []byte("same-lane-order")
	orderID, err := workidentity.StableOpaqueID("v2order", "CARE_REWARD", "v2-order-1")
	if err != nil {
		t.Fatal(err)
	}
	order := authoritystore.AttachedOrder{OrderID: orderID, OrderType: "CARE_REWARD", Payload: payload,
		PayloadHash: sha256.Sum256(payload), DueAtMs: 1_800_000_123_456, SubjectPlayerID: playerID}
	outcome := authoritystore.StoredOperationOutcome{Status: authoritystore.TerminalSucceeded, Payload: []byte("order-result")}
	commit := func() error {
		return binding.CASEpochModelStateWithOutcomeAndOrders(context.Background(), playerID, snapshot.RecordVersion, 0,
			snapshot.ModelFields, outcome, []authoritystore.AttachedOrder{order})
	}
	if err := commit(); err != nil {
		t.Fatal(err)
	}
	work, err := rig.sourceReader.ReadSourceWork(context.Background(), authoritystore.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: playerID}, order.OrderType, order.OrderID)
	if err != nil || work.WorkID != order.OrderID || work.NextDueAtMs != order.DueAtMs || string(work.TypedPayload) != string(payload) {
		t.Fatalf("work=%+v err=%v", work, err)
	}
	request, ok := rig.fault.LastCommitRequest()
	if !ok || request.ModelMutation == nil || len(request.ModelMutation.ModelUpserts) != 0 || len(request.AttachedOrders) != 1 || binding.Version() != 3 {
		t.Fatalf("request=%+v version=%d", request, binding.Version())
	}
	if err := commit(); err != nil {
		t.Fatal(err)
	}
	if binding.Version() != 3 {
		t.Fatalf("replay incremented version=%d", binding.Version())
	}
}

func TestSameInputV1V2RuntimeComparisonFixture(t *testing.T) {
	const playerID = uint64(19_206)
	v1 := ar1pMemoryRig(t, playerID)
	v2 := ar1pMemoryRig(t, playerID)
	v2.registry.modelFieldV2Conversion = enabledPlayerV2Policy()

	_, v1Start, err := v1.registry.EnsureWithProfile(context.Background(), playerID, "comparison", "same_farmer", 7)
	if err != nil {
		t.Fatal(err)
	}
	_, v2Start, err := v2.registry.EnsureWithProfile(context.Background(), playerID, "comparison", "same_farmer", 7)
	if err != nil {
		t.Fatal(err)
	}
	turn := func(record *playerpb.PlayerRecord, _ uint64) (any, bool, error) {
		for _, receipt := range record.GetRecentOperationReceipts() {
			if receipt.GetOperationId() == "same-input" {
				return "SUCCEEDED", false, nil
			}
		}
		for _, task := range record.GetTasks() {
			task.UpdatedAtMs = 1_800_000_000_000
		}
		record.Progression.Level = 8
		record.RecentOperationReceipts = append(record.RecentOperationReceipts,
			&playerpb.PlayerOperationReceipt{OperationId: "same-input", ResultPayload: []byte("same-result"), CommittedAtMs: 1_800_000_000_000})
		return "SUCCEEDED", true, nil
	}
	v1Result, err := v1.registry.SubmitVersioned(context.Background(), playerID, turn)
	if err != nil {
		t.Fatal(err)
	}
	v2Result, err := v2.registry.SubmitVersioned(context.Background(), playerID, turn)
	if err != nil {
		t.Fatal(err)
	}
	if v1Result.Value != v2Result.Value || v1Result.CommittedRecordVersion-v1Start != 1 || v2Result.CommittedRecordVersion-v2Start != 1 {
		t.Fatalf("v1=%+v start=%d v2=%+v start=%d", v1Result, v1Start, v2Result, v2Start)
	}
	v1Record, _, err := v1.registry.Read(context.Background(), playerID)
	if err != nil {
		t.Fatal(err)
	}
	v2Record, _, err := v2.registry.Read(context.Background(), playerID)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(v1Record, v2Record) {
		t.Fatalf("business states differ\nv1=%v\nv2=%v", v1Record, v2Record)
	}
	v1Replay, err := v1.registry.SubmitVersioned(context.Background(), playerID, turn)
	if err != nil {
		t.Fatal(err)
	}
	v2Replay, err := v2.registry.SubmitVersioned(context.Background(), playerID, turn)
	if err != nil {
		t.Fatal(err)
	}
	if v1Replay.CommittedRecordVersion != v1Result.CommittedRecordVersion || v2Replay.CommittedRecordVersion != v2Result.CommittedRecordVersion {
		t.Fatalf("replay versions v1=%d v2=%d", v1Replay.CommittedRecordVersion, v2Replay.CommittedRecordVersion)
	}
	assertPlayerV2PhysicalTruth(t, v2.modelReader, playerID, v2Result.CommittedRecordVersion)
	v1Rows, err := v1.modelReader.BatchReadModelFields(context.Background(), actorrouting.DomainPlayer, []uint64{playerID}, []string{"record", "public_profile"})
	if err != nil || len(v1Rows) != 1 || v1Rows[0].ModelLayout != authoritystore.ModelLayoutV1 || len(v1Rows[0].ModelFields["record"]) == 0 {
		t.Fatalf("v1 physical row=%+v err=%v", v1Rows, err)
	}
	v1Binding, _ := v1.registry.bindingFor(context.Background(), playerID, nil)
	v2Binding, _ := v2.registry.bindingFor(context.Background(), playerID, nil)
	if v1Binding.PhysicalUsage().ReceiptItems != 1 || v2Binding.PhysicalUsage().ReceiptItems != 2 {
		t.Fatalf("receipt usage v1=%+v v2=%+v", v1Binding.PhysicalUsage(), v2Binding.PhysicalUsage())
	}
	v1Commit, v1OK := v1.fault.LastCommitRequest()
	v2Commit, v2OK := v2.fault.LastCommitRequest()
	if !v1OK || !v2OK || v1Commit.ModelMutation == nil || v2Commit.ModelMutation == nil ||
		len(v1Commit.CandidateState) != 2 ||
		!equalNameList(sortedModelNames(v1Commit.ModelMutation.ModelUpserts), []string{"public_profile", "record"}) ||
		!equalNameList(sortedModelNames(v2Commit.ModelMutation.ModelUpserts), []string{"progression", "task"}) {
		t.Fatalf("physical writes v1=%+v v2=%+v", v1Commit.ModelMutation, v2Commit.ModelMutation)
	}
	if v1Binding.PhysicalUsage().ModelBytes != authoritystore.ModelStateSerializedBytes(v1Commit.CandidateState) ||
		v2Binding.PhysicalUsage().ModelBytes != authoritystore.ModelStateSerializedBytes(v2Commit.CandidateState) ||
		v1Binding.PhysicalUsage().ModelBytes <= 0 || v2Binding.PhysicalUsage().ModelBytes <= 0 {
		t.Fatalf("physical bytes v1=%+v v2=%+v", v1Binding.PhysicalUsage(), v2Binding.PhysicalUsage())
	}
}

func clonePlayerRuntimeState(in authoritystore.ModelState) authoritystore.ModelState {
	out := make(authoritystore.ModelState, len(in))
	for name, value := range in {
		out[name] = append([]byte(nil), value...)
	}
	return out
}

func assertPlayerV2PhysicalTruth(t *testing.T, reader authoritystore.ModelFieldBatchReader, playerID, version uint64) {
	t.Helper()
	names, err := fieldv2.ExactNames(actorrouting.DomainPlayer)
	if err != nil {
		t.Fatal(err)
	}
	requested := append(append([]string(nil), names...), "record", "public_profile")
	rows, err := reader.BatchReadModelFields(context.Background(), actorrouting.DomainPlayer, []uint64{playerID}, requested)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%v err=%v", rows, err)
	}
	row := rows[0]
	if row.Err != nil || !row.Found || row.RecordVersion != version || row.ModelLayout != authoritystore.ModelLayoutV2 ||
		len(row.ModelFields) != len(names) || row.ModelFields["record"] != nil || row.ModelFields["public_profile"] != nil {
		t.Fatalf("row=%+v", row)
	}
	if err := fieldv2.ValidateExactFields(actorrouting.DomainPlayer, row.ModelFields); err != nil {
		t.Fatal(err)
	}
}

func sortedModelNames(state authoritystore.ModelState) []string {
	names := make([]string, 0, len(state))
	for name := range state {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func containsName(names []string, want string) bool {
	for _, name := range names {
		if name == want {
			return true
		}
	}
	return false
}

func equalNameList(a, b []string) bool {
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
