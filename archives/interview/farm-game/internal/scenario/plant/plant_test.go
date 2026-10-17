package plant

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/contracts"
	"farmgame/internal/identity"
	"farmgame/internal/workflow"
	"farmgame/internal/workidentity"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type plantFixture struct {
	now         time.Time
	repo        *workflow.MemoryWorkRepository
	coordinator *Coordinator
	definition  *Definition
	engine      *workflow.Engine
	player      *scriptedParticipant
	farm        *scriptedParticipant
	buckets     *recordingBucketResolver
	input       PlantInput
	record      *workflow.WorkRecord
	order       *[]string
}

func newPlantFixture(t *testing.T) *plantFixture {
	t.Helper()
	f := &plantFixture{
		now:     time.UnixMilli(1_800_000_000_000),
		input:   testInput(),
		player:  newScriptedParticipant("player"),
		farm:    newScriptedParticipant("farm"),
		buckets: newRecordingBucketResolver(),
	}
	order := make([]string, 0, 4)
	f.order = &order
	f.player.order = f.order
	f.farm.order = f.order
	f.repo = workflow.NewMemoryWorkRepository(func() time.Time { return f.now })
	var err error
	f.coordinator, err = NewCoordinator(CoordinatorConfig{
		Repository: f.repo, Player: f.player, Farm: f.farm,
		BucketResolver: f.buckets, BucketVersion: 1, Now: func() time.Time { return f.now },
	})
	if err != nil {
		t.Fatal(err)
	}
	f.definition = f.coordinator.Definition()
	f.engine = f.coordinator.Engine()
	f.record, _, err = f.coordinator.Create(context.Background(), f.input)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func testInput() PlantInput {
	return PlantInput{
		SubjectPlayerID:         10001,
		OperationID:             "op-plant-0001",
		TargetFarmID:            10001,
		PlotID:                  3,
		ExpectedPlotVersion:     7,
		CropDefinitionID:        7,
		ExpectedCropRuleVersion: 42,
		SeedItemID:              701,
		CropRuleSnapshot: &contractspb.CropRuleSnapshot{
			SchemaVersion:     1,
			CropDefinitionId:  7,
			CropRuleVersion:   42,
			SeedItemId:        701,
			HarvestItemId:     702,
			GrowDurationMs:    60_000,
			BaseYield:         50,
			HarvestExp:        0,
			CropUnitCoinValue: 3,
			CompendiumRule: &contractspb.CompendiumUnlockRuleSnapshot{
				CropDefinitionId:         7,
				CatalogVersionAtPlant:    9,
				DefinitionVersionAtPlant: 11,
			},
			StealPlanTemplate: &contractspb.StealPlanTemplate{
				PolicyVersion:   1,
				TotalCapRatioBp: 2000,
				SlotRatioBp:     []uint32{200, 200, 200, 200, 200, 200, 200, 200, 200, 200},
			},
			CarePlanTemplate: &contractspb.CarePlanTemplate{CareRuleVersion: 4},
		},
	}
}

func TestPlantRootIdentityAndCreateIfAbsentConflict(t *testing.T) {
	f := newPlantFixture(t)
	root, err := BuildRoot(f.input)
	if err != nil {
		t.Fatal(err)
	}
	wantID, err := identity.WorkflowID(root.Key)
	if err != nil || root.WorkflowID != wantID {
		t.Fatalf("workflow id=%q want=%q err=%v", root.WorkflowID, wantID, err)
	}
	if root.Key.Root.Kind != identity.RootKindFarmActor || root.Key.Root.CanonicalID != "10001" {
		t.Fatalf("root=%+v", root.Key.Root)
	}
	wantReservation, _ := identity.SeedReservationID(root.WorkflowID)
	wantCycle, _ := identity.CropCycleID(root.WorkflowID)
	if root.ReservationID != wantReservation || root.CropCycleID != wantCycle {
		t.Fatalf("derived ids reservation=%q cycle=%q", root.ReservationID, root.CropCycleID)
	}
	if f.record.Address.WorkID != root.WorkflowID || f.record.PayloadHash != root.PayloadHash {
		t.Fatalf("record did not use v2 root: address=%q hash=%x", f.record.Address.WorkID, f.record.PayloadHash)
	}
	for i, step := range f.record.Steps {
		wantInvocation, err := identity.ParticipantInvocationID(identity.WorkTypePlant, root.WorkflowID, identity.ParticipantStep(step.Name), 1)
		if err != nil || step.InvocationID != wantInvocation {
			t.Fatalf("step %d invocation=%q want=%q err=%v", i, step.InvocationID, wantInvocation, err)
		}
	}

	again, created, err := f.coordinator.Create(context.Background(), f.input)
	if err != nil || created || again.WorkVersion != f.record.WorkVersion {
		t.Fatalf("same intent record=%v created=%v err=%v", again, created, err)
	}
	changed := f.input
	changed.PlotID++
	if _, _, err := f.coordinator.Create(context.Background(), changed); !errors.Is(err, workflow.ErrIdempotencyConflict) {
		t.Fatalf("same key/different root payload err=%v", err)
	}
}

func TestPlantCreateResolvesIndependentBucketPerWorkflow(t *testing.T) {
	f := newPlantFixture(t)
	firstRoot, err := BuildRoot(f.input)
	if err != nil {
		t.Fatal(err)
	}
	secondInput := f.input
	secondInput.OperationID = "op-plant-0002"
	secondRoot, err := BuildRoot(secondInput)
	if err != nil {
		t.Fatal(err)
	}
	if firstRoot.WorkflowID == secondRoot.WorkflowID {
		t.Fatal("test workflows unexpectedly share an id")
	}
	first, _, err := f.coordinator.Create(context.Background(), f.input)
	if err != nil {
		t.Fatal(err)
	}
	second, created, err := f.coordinator.Create(context.Background(), secondInput)
	if err != nil || !created {
		t.Fatalf("second create=%v created=%v err=%v", second, created, err)
	}
	for _, record := range []*workflow.WorkRecord{first, second} {
		want := f.buckets.bucketFor(record.Address.WorkID, record.Address.BucketVersion)
		if record.Address.BucketID != want {
			t.Fatalf("workflow %q bucket=%d want=%d", record.Address.WorkID, record.Address.BucketID, want)
		}
	}
	calls := f.buckets.callsCopy()
	seen := map[string]bool{}
	for _, call := range calls {
		if call.Version != 1 || (call.WorkID != firstRoot.WorkflowID && call.WorkID != secondRoot.WorkflowID) {
			t.Fatalf("resolver call=%+v", call)
		}
		seen[call.WorkID] = true
	}
	if !seen[firstRoot.WorkflowID] || !seen[secondRoot.WorkflowID] {
		t.Fatalf("resolver did not receive both workflow ids: %+v", calls)
	}
}

func TestPlantBucketResolverFailurePrecedesCreateIfAbsentAndParticipants(t *testing.T) {
	f := newPlantFixture(t)
	input := f.input
	input.OperationID = "op-plant-resolver-error"
	root, err := BuildRoot(input)
	if err != nil {
		t.Fatal(err)
	}
	resolverErr := errors.New("resolver unavailable")
	f.buckets.err = resolverErr
	if _, _, err := f.coordinator.Create(context.Background(), input); !errors.Is(err, resolverErr) {
		t.Fatalf("resolver error=%v", err)
	}
	if _, err := f.repo.Get(context.Background(), string(identity.WorkTypePlant), root.WorkflowID); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("resolver failure created a WorkRecord: %v", err)
	}
	if len(f.player.callsCopy()) != 0 || len(f.farm.callsCopy()) != 0 {
		t.Fatal("resolver failure reached a participant")
	}
}

func TestPlantTrustedRootRequiresOwnerFarmBinding(t *testing.T) {
	input := testInput()
	input.TargetFarmID++
	if _, err := BuildRoot(input); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("mismatched trusted owner/farm accepted: %v", err)
	}
}

func TestPlantCropRuleSnapshotAdmissionMirrorsFarmInvariants(t *testing.T) {
	careIncident := func(schedule contractspb.CareScheduleMode) *contractspb.CarePlanIncidentTemplate {
		return &contractspb.CarePlanIncidentTemplate{CareType: contractspb.CareType_CARE_TYPE_WATER, Ordinal: 1, ScheduleMode: schedule}
	}
	tests := []struct {
		name   string
		mutate func(*contractspb.CropRuleSnapshot)
	}{
		{name: "top_level_schema", mutate: func(snapshot *contractspb.CropRuleSnapshot) { snapshot.SchemaVersion = 2 }},
		{name: "top_level_base_yield", mutate: func(snapshot *contractspb.CropRuleSnapshot) { snapshot.BaseYield = 51 }},
		{name: "top_level_rule_range", mutate: func(snapshot *contractspb.CropRuleSnapshot) { snapshot.CropRuleVersion = uint64(1) << 31 }},
		{name: "compendium_missing", mutate: func(snapshot *contractspb.CropRuleSnapshot) { snapshot.CompendiumRule = nil }},
		{name: "compendium_crop_binding", mutate: func(snapshot *contractspb.CropRuleSnapshot) { snapshot.CompendiumRule.CropDefinitionId++ }},
		{name: "compendium_catalog_version", mutate: func(snapshot *contractspb.CropRuleSnapshot) { snapshot.CompendiumRule.CatalogVersionAtPlant = 0 }},
		{name: "compendium_definition_version", mutate: func(snapshot *contractspb.CropRuleSnapshot) { snapshot.CompendiumRule.DefinitionVersionAtPlant = 0 }},
		{name: "steal_missing", mutate: func(snapshot *contractspb.CropRuleSnapshot) { snapshot.StealPlanTemplate = nil }},
		{name: "steal_policy_version", mutate: func(snapshot *contractspb.CropRuleSnapshot) { snapshot.StealPlanTemplate.PolicyVersion = 0 }},
		{name: "steal_cap_ratio", mutate: func(snapshot *contractspb.CropRuleSnapshot) { snapshot.StealPlanTemplate.TotalCapRatioBp = 1999 }},
		{name: "steal_slot_count", mutate: func(snapshot *contractspb.CropRuleSnapshot) { snapshot.StealPlanTemplate.SlotRatioBp = []uint32{200} }},
		{name: "steal_slot_ratio", mutate: func(snapshot *contractspb.CropRuleSnapshot) { snapshot.StealPlanTemplate.SlotRatioBp[0] = 199 }},
		{name: "care_missing", mutate: func(snapshot *contractspb.CropRuleSnapshot) { snapshot.CarePlanTemplate = nil }},
		{name: "care_rule_version", mutate: func(snapshot *contractspb.CropRuleSnapshot) { snapshot.CarePlanTemplate.CareRuleVersion = 0 }},
		{name: "care_incident_nil", mutate: func(snapshot *contractspb.CropRuleSnapshot) {
			snapshot.CarePlanTemplate.Incidents = []*contractspb.CarePlanIncidentTemplate{nil}
		}},
		{name: "care_incident_ordinal", mutate: func(snapshot *contractspb.CropRuleSnapshot) {
			snapshot.CarePlanTemplate.Incidents = []*contractspb.CarePlanIncidentTemplate{{CareType: contractspb.CareType_CARE_TYPE_WATER, Ordinal: 2, ScheduleMode: contractspb.CareScheduleMode_CARE_SCHEDULE_MODE_FIXED_OFFSET}}
		}},
		{name: "care_incident_type", mutate: func(snapshot *contractspb.CropRuleSnapshot) {
			snapshot.CarePlanTemplate.Incidents = []*contractspb.CarePlanIncidentTemplate{{Ordinal: 1, ScheduleMode: contractspb.CareScheduleMode_CARE_SCHEDULE_MODE_FIXED_OFFSET}}
		}},
		{name: "care_schedule", mutate: func(snapshot *contractspb.CropRuleSnapshot) {
			snapshot.CarePlanTemplate.Incidents = []*contractspb.CarePlanIncidentTemplate{careIncident(contractspb.CareScheduleMode_CARE_SCHEDULE_MODE_UNSPECIFIED)}
		}},
		{name: "care_fixed_offset", mutate: func(snapshot *contractspb.CropRuleSnapshot) {
			snapshot.CarePlanTemplate.Incidents = []*contractspb.CarePlanIncidentTemplate{careIncident(contractspb.CareScheduleMode_CARE_SCHEDULE_MODE_FIXED_OFFSET)}
		}},
		{name: "care_random_window", mutate: func(snapshot *contractspb.CropRuleSnapshot) {
			snapshot.CarePlanTemplate.Incidents = []*contractspb.CarePlanIncidentTemplate{careIncident(contractspb.CareScheduleMode_CARE_SCHEDULE_MODE_DETERMINISTIC_RANDOM_WINDOW)}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := testInput()
			test.mutate(input.CropRuleSnapshot)
			if _, err := BuildRoot(input); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("invalid snapshot accepted: %v", err)
			}
		})
	}
}

func TestPlantDecodeRootRechecksOwnerAndSnapshotBeforeReserve(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*contractspb.CommitPlantPayload)
	}{
		{name: "forged_owner", mutate: func(root *contractspb.CommitPlantPayload) { root.OwnerPlayerId++ }},
		{name: "forged_farm", mutate: func(root *contractspb.CommitPlantPayload) { root.FarmId++ }},
		{name: "missing_compendium", mutate: func(root *contractspb.CommitPlantPayload) { root.CropRuleSnapshot.CompendiumRule = nil }},
		{name: "invalid_steal", mutate: func(root *contractspb.CommitPlantPayload) {
			root.CropRuleSnapshot.StealPlanTemplate.SlotRatioBp[0] = 199
		}},
		{name: "invalid_care", mutate: func(root *contractspb.CommitPlantPayload) { root.CropRuleSnapshot.CarePlanTemplate.CareRuleVersion = 0 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newPlantFixture(t)
			forged := forgedRootRecord(t, f.record, test.mutate)
			if _, err := f.definition.BuildPayload(forged, 0); !errors.Is(err, ErrRootPayload) {
				t.Fatalf("forged root accepted: %v", err)
			}
			if len(f.player.callsCopy()) != 0 || len(f.farm.callsCopy()) != 0 {
				t.Fatal("forged durable root reached a participant")
			}
		})
	}
}

func TestPlantDefinitionOrderAndTargetAuthorityRouting(t *testing.T) {
	f := newPlantFixture(t)
	if err := f.coordinator.Resume(context.Background(), f.record); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(*f.order); got != "[RESERVE_SEED COMMIT_PLANT COMMIT_SEED_RESERVATION]" {
		t.Fatalf("global step order=%s", got)
	}
	// The participant fakes are separate authority ports; inspect each port
	// independently to preserve the expected cross-authority order.
	if got := f.player.stepNames(); fmt.Sprint(got) != "[RESERVE_SEED COMMIT_SEED_RESERVATION]" {
		t.Fatalf("player steps=%v", got)
	}
	if got := f.farm.stepNames(); fmt.Sprint(got) != "[COMMIT_PLANT]" {
		t.Fatalf("farm steps=%v", got)
	}
	if f.record.State != workflow.WorkStateCompleted {
		t.Fatalf("state=%s", f.record.State)
	}
}

func TestPlantReserveRejectionTerminatesWithoutFarmMutation(t *testing.T) {
	f := newPlantFixture(t)
	reserveInvocation := f.record.Steps[0].InvocationID
	f.player.outcomes[reserveInvocation] = workflow.StepOutcomeRejected
	if err := f.coordinator.Resume(context.Background(), f.record); err != nil {
		t.Fatal(err)
	}
	if f.record.State != workflow.WorkStateRejected {
		t.Fatalf("state=%s", f.record.State)
	}
	if len(f.farm.callsCopy()) != 0 {
		t.Fatalf("Farm mutated/called on Reserve rejection: %v", f.farm.stepNames())
	}
	if len(f.player.callsCopy()) != 1 {
		t.Fatalf("player calls=%v", f.player.stepNames())
	}
}

func TestPlantFarmRejectionRunsOneDeterministicReleaseAndCompensates(t *testing.T) {
	f := newPlantFixture(t)
	commitInvocation := f.record.Steps[1].InvocationID
	f.farm.outcomes[commitInvocation] = workflow.StepOutcomeRejected
	if err := f.coordinator.Resume(context.Background(), f.record); err != nil {
		t.Fatal(err)
	}
	if f.record.State != workflow.WorkStateCompensated {
		t.Fatalf("state=%s", f.record.State)
	}
	if got := f.player.stepNames(); fmt.Sprint(got) != "[RESERVE_SEED RELEASE_SEED_RESERVATION]" {
		t.Fatalf("player steps=%v", got)
	}
	if got := f.farm.stepNames(); fmt.Sprint(got) != "[COMMIT_PLANT]" {
		t.Fatalf("farm steps=%v", got)
	}
	release := decodeCallPayload[*contractspb.ReleaseSeedReservationPayload](t, f.player.callsCopy()[1])
	farmCall := f.farm.callsCopy()[0]
	farmResultHash := f.farm.resultHash(farmCall.InvocationID)
	releaseCall := f.player.callsCopy()[1]
	if release.GetReleaseReason() != ReleaseReasonFarmRejected || !bytes.Equal(release.GetTerminalFarmResultHash(), farmResultHash[:]) {
		t.Fatalf("release=%v farm hash=%x", release, farmResultHash)
	}
	if releaseCall.DependsOnResultHash != farmResultHash {
		t.Fatalf("release call dependency=%x farm hash=%x", releaseCall.DependsOnResultHash, farmResultHash)
	}
	if f.player.applyCountFor(releaseCall.InvocationID) != 1 {
		t.Fatal("release was not exactly one durable participant effect")
	}
}

func TestPlantSuccessCommitsReservation(t *testing.T) {
	f := newPlantFixture(t)
	if err := f.coordinator.Resume(context.Background(), f.record); err != nil {
		t.Fatal(err)
	}
	if f.record.State != workflow.WorkStateCompleted {
		t.Fatalf("state=%s", f.record.State)
	}
	if got := f.player.stepNames(); fmt.Sprint(got) != "[RESERVE_SEED COMMIT_SEED_RESERVATION]" {
		t.Fatalf("player steps=%v", got)
	}
	commitPlantCall := f.farm.callsCopy()[0]
	commitReservationCall := f.player.callsCopy()[1]
	plantPayload := decodeCallPayload[*contractspb.CommitPlantPayload](t, commitPlantCall)
	reservationPayload := decodeCallPayload[*contractspb.CommitSeedReservationPayload](t, commitReservationCall)
	reserveHash := f.player.resultHash(f.player.callsCopy()[0].InvocationID)
	farmHash := f.farm.resultHash(commitPlantCall.InvocationID)
	if !bytes.Equal(plantPayload.GetReserveSeedResultHash(), reserveHash[:]) {
		t.Fatalf("CommitPlant dependency hash=%x want=%x", plantPayload.GetReserveSeedResultHash(), reserveHash)
	}
	if !bytes.Equal(reservationPayload.GetCommitPlantResultHash(), farmHash[:]) {
		t.Fatalf("CommitReservation dependency hash=%x want=%x", reservationPayload.GetCommitPlantResultHash(), farmHash)
	}
	if commitPlantCall.DependsOnResultHash != reserveHash || commitReservationCall.DependsOnResultHash != farmHash {
		t.Fatalf("call dependency hashes CommitPlant=%x CommitReservation=%x", commitPlantCall.DependsOnResultHash, commitReservationCall.DependsOnResultHash)
	}
	if f.player.applyCountFor(commitReservationCall.InvocationID) != 1 || f.farm.applyCountFor(commitPlantCall.InvocationID) != 1 {
		t.Fatal("success participant effect duplicated")
	}
}

func TestPlantFinalPlayerResultProof(t *testing.T) {
	tests := []struct {
		name      string
		step      int
		remarshal bool
		mutate    func(proto.Message, *workflow.StepResult)
	}{
		{name: "commit_empty", step: 2, mutate: func(_ proto.Message, result *workflow.StepResult) {
			result.Result = nil
			result.ResultHash = [32]byte{}
		}},
		{name: "commit_malformed", step: 2, mutate: func(_ proto.Message, result *workflow.StepResult) {
			result.Result = []byte{0xff}
			result.ResultHash = sha256.Sum256(result.Result)
		}},
		{name: "commit_wrong_identity", step: 2, remarshal: true, mutate: func(message proto.Message, _ *workflow.StepResult) {
			message.(*contractspb.CommitSeedReservationResult).ReservationId = "sid1_forged"
		}},
		{name: "commit_wrong_seed", step: 2, remarshal: true, mutate: func(message proto.Message, _ *workflow.StepResult) {
			message.(*contractspb.CommitSeedReservationResult).SeedItemId++
		}},
		{name: "commit_wrong_state", step: 2, remarshal: true, mutate: func(message proto.Message, _ *workflow.StepResult) {
			message.(*contractspb.CommitSeedReservationResult).ReservationState = contractspb.ReservationTerminalState_RESERVATION_TERMINAL_STATE_RELEASED
		}},
		{name: "commit_hash_mismatch", step: 2, mutate: func(_ proto.Message, result *workflow.StepResult) { result.ResultHash[0]++ }},
		{name: "release_empty", step: 3, mutate: func(_ proto.Message, result *workflow.StepResult) {
			result.Result = nil
			result.ResultHash = [32]byte{}
		}},
		{name: "release_malformed", step: 3, mutate: func(_ proto.Message, result *workflow.StepResult) {
			result.Result = []byte{0xff}
			result.ResultHash = sha256.Sum256(result.Result)
		}},
		{name: "release_wrong_identity", step: 3, remarshal: true, mutate: func(message proto.Message, _ *workflow.StepResult) {
			message.(*contractspb.ReleaseSeedReservationResult).ReservationId = "sid1_forged"
		}},
		{name: "release_wrong_seed", step: 3, remarshal: true, mutate: func(message proto.Message, _ *workflow.StepResult) {
			message.(*contractspb.ReleaseSeedReservationResult).SeedItemId++
		}},
		{name: "release_wrong_state", step: 3, remarshal: true, mutate: func(message proto.Message, _ *workflow.StepResult) {
			message.(*contractspb.ReleaseSeedReservationResult).ReservationState = contractspb.ReservationTerminalState_RESERVATION_TERMINAL_STATE_CONSUMED
		}},
		{name: "release_hash_mismatch", step: 3, mutate: func(_ proto.Message, result *workflow.StepResult) { result.ResultHash[0]++ }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newPlantFixture(t)
			result, message := validFinalPlayerResult(t, f, test.step)
			test.mutate(message, &result)
			if test.remarshal {
				// Identity/state mutations must be re-marshaled so the proof
				// reaches the canonical-hash check rather than protobuf parsing.
				result.Result, _ = deterministicProto(message)
				spec, _ := contracts.LookupParticipantCommand(commandForStepMust(t, test.step))
				result.ResultHash, _ = contracts.CanonicalParticipantHash(spec, message)
			}
			if _, err := f.definition.Decide(f.record, test.step, result); !errors.Is(err, ErrPriorResult) {
				t.Fatalf("invalid final result accepted: %v", err)
			}
		})
	}

	for _, step := range []int{2, 3} {
		t.Run(fmt.Sprintf("valid-step-%d", step), func(t *testing.T) {
			f := newPlantFixture(t)
			result, _ := validFinalPlayerResult(t, f, step)
			decision, err := f.definition.Decide(f.record, step, result)
			if err != nil {
				t.Fatal(err)
			}
			want := workflow.WorkStateCompleted
			if step == 3 {
				want = workflow.WorkStateCompensated
			}
			if decision.Terminal != want {
				t.Fatalf("terminal=%s want=%s", decision.Terminal, want)
			}
		})
	}
}

func TestPlantRPCRejectsEmptyFinalResult(t *testing.T) {
	for _, outcome := range []contractspb.ParticipantOutcome{
		contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED,
		contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE,
	} {
		for _, command := range []contractspb.ParticipantCommandType{
			contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_SEED_RESERVATION,
			contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RELEASE_SEED_RESERVATION,
		} {
			if _, err := decodeParticipantResponse(&contractspb.ParticipantResponse{
				ParticipantCommand: command, ParticipantInvocationId: "invocation", Outcome: outcome,
			}, command, "invocation"); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("command=%s outcome=%s empty result accepted: %v", command, outcome, err)
			}
		}
	}
}

func TestPlantFarmNoChangeStillCommitsReservation(t *testing.T) {
	f := newPlantFixture(t)
	f.farm.outcomes[f.record.Steps[1].InvocationID] = workflow.StepOutcomeNoChange
	if err := f.coordinator.Resume(context.Background(), f.record); err != nil {
		t.Fatal(err)
	}
	if f.record.State != workflow.WorkStateCompleted {
		t.Fatalf("state=%s", f.record.State)
	}
	if got := fmt.Sprint(*f.order); got != "[RESERVE_SEED COMMIT_PLANT COMMIT_SEED_RESERVATION]" {
		t.Fatalf("order=%s", got)
	}
}

func TestPlantFarmStatusSelectsOnlyTheContractBranch(t *testing.T) {
	tests := []struct {
		name         string
		farmOutcome  workflow.StepOutcome
		stepIndex    int
		wantBuild    bool
		forceRelease bool
	}{
		{name: "applied_commit", farmOutcome: workflow.StepOutcomeApplied, stepIndex: 2, wantBuild: true},
		{name: "no_change_commit", farmOutcome: workflow.StepOutcomeNoChange, stepIndex: 2, wantBuild: true},
		{name: "rejected_release", farmOutcome: workflow.StepOutcomeRejected, stepIndex: 3, wantBuild: true},
		{name: "rejected_cannot_commit", farmOutcome: workflow.StepOutcomeRejected, stepIndex: 2},
		{name: "applied_cannot_release", farmOutcome: workflow.StepOutcomeApplied, stepIndex: 3, forceRelease: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newPlantFixture(t)
			f.farm.outcomes[f.record.Steps[1].InvocationID] = test.farmOutcome
			stopCalls := 0
			if err := f.engine.Resume(context.Background(), f.record, workflow.LeaseFence{}, func() bool {
				stopCalls++
				return stopCalls >= 3
			}); !errors.Is(err, workflow.ErrRetryScheduled) {
				t.Fatalf("setup resume=%v", err)
			}
			if test.forceRelease {
				forged := *f.record
				forged.CurrentStep = 3
				forged.State = workflow.WorkStateRunning
				if _, err := f.definition.BuildPayload(&forged, test.stepIndex); err == nil {
					t.Fatal("APPLIED Farm result built a release payload")
				}
				return
			}
			_, err := f.definition.BuildPayload(f.record, test.stepIndex)
			if test.wantBuild && err != nil {
				t.Fatalf("valid branch rejected: %v", err)
			}
			if !test.wantBuild && !errors.Is(err, ErrPriorResult) {
				t.Fatalf("inconsistent branch error=%v", err)
			}
		})
	}
}

func TestPlantAppliedFarmCannotReachReleaseAndDuplicateSeedEffect(t *testing.T) {
	f := newPlantFixture(t)
	stopCalls := 0
	if err := f.engine.Resume(context.Background(), f.record, workflow.LeaseFence{}, func() bool {
		stopCalls++
		return stopCalls >= 3
	}); !errors.Is(err, workflow.ErrRetryScheduled) {
		t.Fatalf("setup resume=%v", err)
	}
	forged := *f.record
	forged.CurrentStep = 3
	forged.State = workflow.WorkStateRunning
	err := f.engine.Resume(context.Background(), &forged, workflow.LeaseFence{}, nil)
	if !errors.Is(err, ErrPriorResult) || forged.State != workflow.WorkStateFailed {
		t.Fatalf("inconsistent release state=%s err=%v", forged.State, err)
	}
	if len(f.player.callsCopy()) != 1 {
		t.Fatalf("final Player effect was reachable: %v", f.player.stepNames())
	}

	// A forged durable record must also fail if the illegal release cursor is
	// already carrying a terminal Player result; Decide must not fold it to
	// COMPENSATED after an APPLIED Farm result.
	releaseResult, _ := validFinalPlayerResult(t, f, 3)
	f.record.Steps[1].Status = workflow.StepApplied
	terminal := *f.record
	terminal.Steps = append([]workflow.StepRecord(nil), f.record.Steps...)
	terminal.CurrentStep = 3
	terminal.State = workflow.WorkStateRunning
	terminal.Steps[3].Status = workflow.StepApplied
	terminal.Steps[3].Result = append([]byte(nil), releaseResult.Result...)
	terminal.Steps[3].ResultHash = releaseResult.ResultHash
	if err := f.engine.Resume(context.Background(), &terminal, workflow.LeaseFence{}, nil); !errors.Is(err, ErrPriorResult) || terminal.State != workflow.WorkStateFailed {
		t.Fatalf("terminal inconsistent release state=%s err=%v", terminal.State, err)
	}
	if len(f.player.callsCopy()) != 1 {
		t.Fatalf("terminal inconsistent record reached final Player: %v", f.player.stepNames())
	}
}

func TestCoordinatorRPCInvokerBuildsTypedContractRequest(t *testing.T) {
	f := newPlantFixture(t)
	payload, err := f.definition.BuildPayload(f.record, 0)
	if err != nil {
		t.Fatal(err)
	}
	step := f.record.Steps[0]
	call := workflow.StepCall{
		Work: f.record.Address, WorkVersion: f.record.WorkVersion, StepIndex: 0,
		StepName: step.Name, InvocationID: step.InvocationID, Payload: payload,
		PayloadHash: sha256.Sum256(payload), OperationID: f.record.OperationID,
		SubjectPlayerID: f.record.SubjectPlayerID,
	}
	resultMessage := &contractspb.ReserveSeedResult{ReservationId: f.input.OperationID, SeedItemId: f.input.SeedItemID, ReservedQuantity: Quantity, PlayerRecordVersion: 2}
	// Use the actual root-derived reservation identity in the response.
	root, err := BuildRoot(f.input)
	if err != nil {
		t.Fatal(err)
	}
	resultMessage.ReservationId = root.ReservationID
	resultBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(resultMessage)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED)
	if err != nil {
		t.Fatal(err)
	}
	resultHash, err := contracts.CanonicalParticipantHash(spec, resultMessage)
	if err != nil {
		t.Fatal(err)
	}
	var received *contractspb.ParticipantRequest
	client := rpcClientFunc(func(_ context.Context, request *contractspb.ParticipantRequest, _ ...grpc.CallOption) (*contractspb.ParticipantResponse, error) {
		received = proto.Clone(request).(*contractspb.ParticipantRequest)
		return &contractspb.ParticipantResponse{
			ParticipantCommand:      request.GetParticipantCommand(),
			ParticipantInvocationId: request.GetCoordinator().GetParticipantInvocationId(),
			Outcome:                 contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED,
			TypedResult:             resultBytes,
			ParticipantResultHash:   resultHash[:],
			CommittedRecordVersion:  2,
		}, nil
	})
	invoker, err := NewRPCInvoker(client, func(_ context.Context, _ workflow.StepCall, command contractspb.ParticipantCommandType, actorID uint64) (*logicrpcpb.ActorRouteHeader, error) {
		if command != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED || actorID != f.input.SubjectPlayerID {
			return nil, fmt.Errorf("unexpected route target")
		}
		return &logicrpcpb.ActorRouteHeader{RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER, ActorId: actorID}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := invoker.Invoke(context.Background(), call)
	if err != nil {
		t.Fatal(err)
	}
	if got.Outcome != workflow.StepOutcomeApplied || got.ResultHash != resultHash || !bytes.Equal(got.Result, resultBytes) {
		t.Fatalf("result=%+v", got)
	}
	if received == nil || received.GetParticipantCommand() != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED ||
		received.GetRouteHeader().GetRouteDomain() != logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER ||
		received.GetRouteHeader().GetActorId() != f.input.SubjectPlayerID ||
		received.GetCoordinator().GetWork().GetWorkId() != f.record.Address.WorkID ||
		received.GetCoordinator().GetParticipantInvocationId() != step.InvocationID ||
		!bytes.Equal(received.GetTypedPayload(), payload) {
		t.Fatalf("request=%v", received)
	}
}

func TestWorkerRecoveryRPCInvokerUsesResumeStepAndLeaseContext(t *testing.T) {
	f := newPlantFixture(t)
	payload, err := f.definition.BuildPayload(f.record, 0)
	if err != nil {
		t.Fatal(err)
	}
	step := f.record.Steps[0]
	call := workflow.StepCall{
		Work: f.record.Address, WorkVersion: f.record.WorkVersion, StepIndex: 0,
		StepName: step.Name, InvocationID: step.InvocationID, Payload: payload,
		PayloadHash: sha256.Sum256(payload), OperationID: f.record.OperationID,
		SubjectPlayerID: f.record.SubjectPlayerID,
	}
	root, err := BuildRoot(f.input)
	if err != nil {
		t.Fatal(err)
	}
	resultMessage := &contractspb.ReserveSeedResult{ReservationId: root.ReservationID, SeedItemId: f.input.SeedItemID, ReservedQuantity: Quantity, PlayerRecordVersion: 2}
	resultBytes, err := deterministicProto(resultMessage)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED)
	if err != nil {
		t.Fatal(err)
	}
	resultHash, err := contracts.CanonicalParticipantHash(spec, resultMessage)
	if err != nil {
		t.Fatal(err)
	}
	var received *contractspb.ParticipantRequest
	client := workerRPCClientFunc(func(_ context.Context, request *contractspb.ParticipantRequest, _ ...grpc.CallOption) (*contractspb.ParticipantResponse, error) {
		received = proto.Clone(request).(*contractspb.ParticipantRequest)
		return &contractspb.ParticipantResponse{
			ParticipantCommand:      request.GetParticipantCommand(),
			ParticipantInvocationId: request.GetRecovery().GetParticipantInvocationId(),
			Outcome:                 contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED,
			TypedResult:             resultBytes,
			ParticipantResultHash:   resultHash[:],
		}, nil
	})
	invoker, err := NewWorkerRecoveryInvoker(client, func(_ context.Context, _ workflow.StepCall, command contractspb.ParticipantCommandType, actorID uint64) (*logicrpcpb.ActorRouteHeader, error) {
		if command != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED || actorID != f.input.SubjectPlayerID {
			return nil, fmt.Errorf("unexpected worker route target")
		}
		return &logicrpcpb.ActorRouteHeader{RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER, ActorId: actorID}, nil
	}, WorkerLeaseContext{CallerInstanceID: "worker-instance", CallerBootID: "worker-boot", LeaseOwnerToken: "lease-token", LeaseEpoch: 9})
	if err != nil {
		t.Fatal(err)
	}
	got, err := invoker.Invoke(context.Background(), call)
	if err != nil {
		t.Fatal(err)
	}
	if got.Outcome != workflow.StepOutcomeApplied || got.ResultHash != resultHash {
		t.Fatalf("worker result=%+v", got)
	}
	if received == nil || received.GetCoordinator() != nil || received.GetRecovery() == nil ||
		received.GetRecovery().GetCallerType() != contractspb.InternalCallerType_INTERNAL_CALLER_TYPE_WORKER_RECOVERY ||
		received.GetRecovery().GetCallerInstanceId() != "worker-instance" || received.GetRecovery().GetCallerBootId() != "worker-boot" ||
		received.GetRecovery().GetLeaseOwnerToken() != "lease-token" || received.GetRecovery().GetLeaseEpoch() != 9 ||
		received.GetRecovery().GetExpectedWorkVersion() != call.WorkVersion ||
		received.GetRecovery().GetParticipantInvocationId() != call.InvocationID || !bytes.Equal(received.GetTypedPayload(), payload) {
		t.Fatalf("worker ResumeStep request=%v", received)
	}
}

func TestPlantResponseLossAfterEachParticipantEffectReusesInvocationAndDoesNotDuplicate(t *testing.T) {
	for _, stepIndex := range []int{0, 1, 2, 3} {
		t.Run(fmt.Sprintf("step-%d", stepIndex), func(t *testing.T) {
			f := newPlantFixture(t)
			step := f.record.Steps[stepIndex]
			owner := f.player
			if stepIndex == 1 {
				owner = f.farm
			}
			owner.loseOnce[step.InvocationID] = true
			if stepIndex == 1 || stepIndex == 3 {
				f.farm.outcomes[f.record.Steps[1].InvocationID] = workflow.StepOutcomeRejected
			}
			if err := f.coordinator.Resume(context.Background(), f.record); !errors.Is(err, workflow.ErrRetryScheduled) {
				t.Fatalf("first resume=%v", err)
			}
			for attempts := 0; attempts < 8 && !f.record.Terminal(); attempts++ {
				if err := f.coordinator.Resume(context.Background(), f.record); err != nil && !errors.Is(err, workflow.ErrRetryScheduled) {
					t.Fatal(err)
				}
			}
			if !f.record.Terminal() {
				t.Fatalf("did not converge: state=%s", f.record.State)
			}
			calls := owner.callsFor(step.InvocationID)
			if len(calls) != 2 || calls[0].InvocationID != calls[1].InvocationID || !bytes.Equal(calls[0].Payload, calls[1].Payload) || calls[0].PayloadHash != calls[1].PayloadHash || calls[0].DependsOnResultHash != calls[1].DependsOnResultHash {
				t.Fatalf("response-loss calls=%+v", calls)
			}
			if owner.applyCountFor(step.InvocationID) != 1 {
				t.Fatalf("invocation applied %d times", owner.applyCountFor(step.InvocationID))
			}
		})
	}
}

func TestPlantCrashRestartBetweenEveryDurableStepUsesReceiptFirstReplay(t *testing.T) {
	for _, stepIndex := range []int{0, 1, 2, 3} {
		t.Run(fmt.Sprintf("step-%d", stepIndex), func(t *testing.T) {
			f := newPlantFixture(t)
			step := f.record.Steps[stepIndex]
			owner := f.player
			if stepIndex == 1 {
				owner = f.farm
			}
			owner.panicOnce[step.InvocationID] = true
			if stepIndex == 1 || stepIndex == 3 {
				f.farm.outcomes[f.record.Steps[1].InvocationID] = workflow.StepOutcomeRejected
			}
			func() {
				defer func() {
					if recover() == nil {
						t.Fatal("fault injection did not crash after participant effect")
					}
				}()
				_ = f.engine.Resume(context.Background(), f.record, workflow.LeaseFence{}, nil)
			}()
			stored, err := f.repo.Get(context.Background(), f.record.Address.WorkType, f.record.Address.WorkID)
			if err != nil {
				t.Fatal(err)
			}
			if stored.Steps[stepIndex].Status != workflow.StepPending || len(stored.Steps[stepIndex].Payload) == 0 {
				t.Fatalf("crash boundary did not persist frozen pending step: %+v", stored.Steps[stepIndex])
			}
			definition, err := NewDefinition(f.player, f.farm)
			if err != nil {
				t.Fatal(err)
			}
			engine, err := workflow.NewEngine(f.repo, []workflow.Definition{definition}, workflow.WithEngineClock(func() time.Time { return f.now }))
			if err != nil {
				t.Fatal(err)
			}
			if err := engine.Resume(context.Background(), stored, workflow.LeaseFence{}, nil); err != nil {
				t.Fatal(err)
			}
			if !stored.Terminal() {
				t.Fatalf("restart did not finish: %s", stored.State)
			}
			calls := owner.callsFor(step.InvocationID)
			if owner.applyCountFor(step.InvocationID) != 1 || len(calls) != 2 || calls[0].InvocationID != calls[1].InvocationID || !bytes.Equal(calls[0].Payload, calls[1].Payload) || calls[0].PayloadHash != calls[1].PayloadHash || calls[0].DependsOnResultHash != calls[1].DependsOnResultHash {
				t.Fatalf("effect/replay count=%d calls=%d", owner.applyCountFor(step.InvocationID), len(calls))
			}
		})
	}
}

func TestPlantRestartRecreatesParticipantAdaptersAroundDurableReceiptStore(t *testing.T) {
	f := newPlantFixture(t)
	invocation := f.record.Steps[0].InvocationID
	f.player.panicOnce[invocation] = true
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("participant crash was not injected")
			}
		}()
		_ = f.engine.Resume(context.Background(), f.record, workflow.LeaseFence{}, nil)
	}()
	stored, err := f.repo.Get(context.Background(), f.record.Address.WorkType, f.record.Address.WorkID)
	if err != nil {
		t.Fatal(err)
	}

	// Only the receipt stores cross the simulated process boundary. The
	// participant adapter instances, configuration maps and call logs do not.
	restartedPlayer := newScriptedParticipant("player-restarted", f.player.store)
	restartedFarm := newScriptedParticipant("farm-restarted", f.farm.store)
	definition, err := NewDefinition(restartedPlayer, restartedFarm)
	if err != nil {
		t.Fatal(err)
	}
	restartedEngine, err := workflow.NewEngine(f.repo, []workflow.Definition{definition}, workflow.WithEngineClock(func() time.Time { return f.now }))
	if err != nil {
		t.Fatal(err)
	}
	if err := restartedEngine.Resume(context.Background(), stored, workflow.LeaseFence{}, nil); err != nil {
		t.Fatal(err)
	}
	if !stored.Terminal() || !restartedPlayer.wasReplayed(invocation) || f.player.applyCountFor(invocation) != 1 || restartedPlayer.applyCountFor(invocation) != 1 {
		t.Fatalf("restart state=%s replay=%v applies=%d", stored.State, restartedPlayer.wasReplayed(invocation), restartedPlayer.applyCountFor(invocation))
	}
	if calls := restartedPlayer.callsFor(invocation); len(calls) != 1 || calls[0].InvocationID != invocation || calls[0].PayloadHash != f.player.callsFor(invocation)[0].PayloadHash {
		t.Fatalf("restart did not replay exact invocation: %+v", calls)
	}
}

func TestPlantFrozenSnapshotSurvivesCallerAndConfigurationMutation(t *testing.T) {
	f := newPlantFixture(t)
	original := proto.Clone(f.input.CropRuleSnapshot).(*contractspb.CropRuleSnapshot)
	f.input.CropRuleSnapshot.GrowDurationMs = 999_999
	f.input.CropRuleSnapshot.CropUnitCoinValue = 999
	if err := f.coordinator.Resume(context.Background(), f.record); err != nil {
		t.Fatal(err)
	}
	commit := decodeCallPayload[*contractspb.CommitPlantPayload](t, f.farm.callsCopy()[0])
	if !proto.Equal(commit.GetCropRuleSnapshot(), original) {
		t.Fatalf("frozen snapshot changed: got=%v want=%v", commit.GetCropRuleSnapshot(), original)
	}
	root := &contractspb.CommitPlantPayload{}
	if err := proto.Unmarshal(f.record.Payload, root); err != nil {
		t.Fatal(err)
	}
	if root.GetCropRuleSnapshot().GetGrowDurationMs() != original.GetGrowDurationMs() || root.GetCropRuleSnapshot().GetCropUnitCoinValue() != original.GetCropUnitCoinValue() {
		t.Fatalf("durable root changed after caller mutation: %v", root.GetCropRuleSnapshot())
	}
}

func TestPlantMalformedOrMissingPriorTypedResultFailsClosed(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*workflow.WorkRecord)
	}{
		{name: "missing", mutate: func(record *workflow.WorkRecord) {
			record.Steps[0].Status = workflow.StepApplied
			record.Steps[0].Result = nil
			record.Steps[0].ResultHash = [32]byte{}
			record.CurrentStep = 1
			record.State = workflow.WorkStateRunning
		}},
		{name: "malformed", mutate: func(record *workflow.WorkRecord) {
			record.Steps[0].Status = workflow.StepApplied
			record.Steps[0].Result = []byte("not protobuf")
			record.Steps[0].ResultHash = sha256.Sum256(record.Steps[0].Result)
			record.CurrentStep = 1
			record.State = workflow.WorkStateRunning
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newPlantFixture(t)
			record, err := f.repo.Get(context.Background(), f.record.Address.WorkType, f.record.Address.WorkID)
			if err != nil {
				t.Fatal(err)
			}
			test.mutate(record)
			updated, err := f.repo.Advance(context.Background(), record, workflow.AdvanceOptions{ExpectedWorkVersion: record.WorkVersion})
			if err != nil {
				t.Fatal(err)
			}
			if err := f.engine.Resume(context.Background(), updated, workflow.LeaseFence{}, nil); !errors.Is(err, ErrPriorResult) {
				t.Fatalf("resume err=%v", err)
			}
			stored, err := f.repo.Get(context.Background(), record.Address.WorkType, record.Address.WorkID)
			if err != nil {
				t.Fatal(err)
			}
			if stored.State != workflow.WorkStateFailed || len(f.farm.callsCopy()) != 0 {
				t.Fatalf("stored=%s farm calls=%v", stored.State, f.farm.stepNames())
			}
		})
	}
}

func TestPlantRejectedReserveCursorBlocksAllDownstreamPayloads(t *testing.T) {
	for _, stepIndex := range []int{1, 2, 3} {
		t.Run(fmt.Sprintf("downstream-step-%d", stepIndex), func(t *testing.T) {
			f := newPlantFixture(t)
			if stepIndex >= 2 {
				// Install a valid Farm receipt so the Reserve status is the only
				// inconsistent prerequisite for the downstream builder.
				_, _ = validFinalPlayerResult(t, f, stepIndex)
			}
			root, err := BuildRoot(f.input)
			if err != nil {
				t.Fatal(err)
			}
			reservePayload, err := f.definition.BuildPayload(f.record, 0)
			if err != nil {
				t.Fatal(err)
			}
			reserveResult := &contractspb.ReserveSeedResult{
				ReservationId: root.ReservationID, SeedItemId: f.input.SeedItemID,
				ReservedQuantity: Quantity, PlayerRecordVersion: 2,
			}
			reserveResultBytes, err := deterministicProto(reserveResult)
			if err != nil {
				t.Fatal(err)
			}
			spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED)
			if err != nil {
				t.Fatal(err)
			}
			reserveResultHash, err := contracts.CanonicalParticipantHash(spec, reserveResult)
			if err != nil {
				t.Fatal(err)
			}

			forged := *f.record
			forged.Steps = append([]workflow.StepRecord(nil), f.record.Steps...)
			forged.Steps[0].Payload = reservePayload
			forged.Steps[0].PayloadHash = sha256.Sum256(reservePayload)
			forged.Steps[0].Status = workflow.StepRejected
			forged.Steps[0].Result = reserveResultBytes
			forged.Steps[0].ResultHash = reserveResultHash
			forged.Steps[0].ErrorClass = "RESERVE_REJECTED"
			forged.Steps[0].Attempts = 1
			forged.CurrentStep = stepIndex
			forged.State = workflow.WorkStateRunning

			if err := f.engine.Resume(context.Background(), &forged, workflow.LeaseFence{}, nil); !errors.Is(err, ErrPriorResult) {
				t.Fatalf("rejected Reserve cursor reached step %d: %v", stepIndex, err)
			}
			if len(f.farm.callsCopy()) != 0 || len(f.player.callsCopy()) != 0 {
				t.Fatalf("forged cursor invoked participants: farm=%v player=%v", f.farm.stepNames(), f.player.stepNames())
			}
		})
	}
}

func TestPlantWorkerTakeoverReplaysSameInvocationAfterParticipantCommit(t *testing.T) {
	f := newPlantFixture(t)
	claim, err := f.repo.ClaimDue(context.Background(), "worker-a", "token-a", 1, time.Second)
	if err != nil || len(claim) != 1 {
		t.Fatalf("claim=%v err=%v", claim, err)
	}
	invocation := claim[0].Record.Steps[0].InvocationID
	f.player.panicOnce[invocation] = true
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("takeover fault injection did not crash")
			}
		}()
		_ = f.engine.Resume(context.Background(), claim[0].Record, workflow.LeaseFence{Owner: "worker-a", Epoch: claim[0].Record.LeaseEpoch}, nil)
	}()
	f.now = f.now.Add(2 * time.Second)
	taken, err := f.repo.ClaimDue(context.Background(), "worker-b", "token-b", 1, 10*time.Second)
	if err != nil || len(taken) != 1 {
		t.Fatalf("takeover claim=%v err=%v", taken, err)
	}
	if err := f.engine.Resume(context.Background(), taken[0].Record, workflow.LeaseFence{Owner: "worker-b", Epoch: taken[0].Record.LeaseEpoch}, nil); err != nil {
		t.Fatal(err)
	}
	if !taken[0].Record.Terminal() || f.player.applyCountFor(invocation) != 1 {
		t.Fatalf("takeover state=%s applies=%d", taken[0].Record.State, f.player.applyCountFor(invocation))
	}
	calls := f.player.callsFor(invocation)
	if len(calls) != 2 || calls[0].InvocationID != calls[1].InvocationID || !bytes.Equal(calls[0].Payload, calls[1].Payload) || calls[0].PayloadHash != calls[1].PayloadHash || calls[0].DependsOnResultHash != calls[1].DependsOnResultHash {
		t.Fatalf("takeover did not reuse exact call: %+v", calls)
	}
}

func TestPlantScenarioHasNoConcreteActorImplementationDependency(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime caller unavailable")
	}
	dir := filepath.Dir(filename)
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || filepath.Ext(path) != ".go" {
			return err
		}
		if strings.HasSuffix(path, "_test.go") {
			return nil
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		for _, forbidden := range []string{"internal/player/actor", "internal/farm/actor", "internal/workflow.StableID", "internal/workidentity.WorkflowID"} {
			if bytes.Contains(data, []byte(forbidden)) {
				return fmt.Errorf("%s imports or calls forbidden dependency %q", path, forbidden)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

type scriptedParticipant struct {
	mu        sync.Mutex
	name      string
	outcomes  map[string]workflow.StepOutcome
	loseOnce  map[string]bool
	panicOnce map[string]bool
	store     *participantReceiptStore
	replayed  map[string]bool
	calls     []workflow.StepCall
	order     *[]string
}

type participantReceiptStore struct {
	mu       sync.Mutex
	receipts map[string]workflow.StepResult
	results  map[string][32]byte
	applies  map[string]int
}

func newParticipantReceiptStore() *participantReceiptStore {
	return &participantReceiptStore{
		receipts: make(map[string]workflow.StepResult),
		results:  make(map[string][32]byte),
		applies:  make(map[string]int),
	}
}

func newScriptedParticipant(name string, stores ...*participantReceiptStore) *scriptedParticipant {
	store := newParticipantReceiptStore()
	if len(stores) > 0 && stores[0] != nil {
		store = stores[0]
	}
	return &scriptedParticipant{
		name: name, outcomes: make(map[string]workflow.StepOutcome),
		loseOnce: make(map[string]bool), panicOnce: make(map[string]bool),
		store:    store,
		replayed: make(map[string]bool),
	}
}

func (p *scriptedParticipant) Invoke(_ context.Context, call workflow.StepCall) (workflow.StepResult, error) {
	p.mu.Lock()
	p.calls = append(p.calls, cloneStepCall(call))
	if p.order != nil {
		*p.order = append(*p.order, call.StepName)
	}
	p.store.mu.Lock()
	if receipt, ok := p.store.receipts[call.InvocationID]; ok {
		p.store.mu.Unlock()
		p.replayed[call.InvocationID] = true
		p.mu.Unlock()
		receipt.Replayed = true
		return receipt, nil
	}
	result, err := p.buildResult(call)
	if err != nil {
		p.store.mu.Unlock()
		p.mu.Unlock()
		return workflow.StepResult{}, err
	}
	p.store.receipts[call.InvocationID] = result
	p.store.results[call.InvocationID] = result.ResultHash
	p.store.applies[call.InvocationID]++
	p.store.mu.Unlock()
	if p.panicOnce[call.InvocationID] {
		delete(p.panicOnce, call.InvocationID)
		p.mu.Unlock()
		panic("injected participant crash after durable effect")
	}
	if p.loseOnce[call.InvocationID] {
		delete(p.loseOnce, call.InvocationID)
		p.mu.Unlock()
		return workflow.StepResult{}, errors.New("injected response loss after durable effect")
	}
	p.mu.Unlock()
	return result, nil
}

func (p *scriptedParticipant) buildResult(call workflow.StepCall) (workflow.StepResult, error) {
	command, err := commandForStep(call.StepName)
	if err != nil {
		return workflow.StepResult{}, err
	}
	spec, err := contracts.LookupParticipantCommand(command)
	if err != nil {
		return workflow.StepResult{}, err
	}
	resultMessage := spec.NewResult()
	switch call.StepName {
	case string(identity.StepReserveSeed):
		payload := &contractspb.ReserveSeedPayload{}
		if err := proto.Unmarshal(call.Payload, payload); err != nil {
			return workflow.StepResult{}, err
		}
		resultMessage = &contractspb.ReserveSeedResult{ReservationId: payload.GetReservationId(), SeedItemId: payload.GetSeedItemId(), ReservedQuantity: payload.GetQuantity(), PlayerRecordVersion: 2}
	case string(identity.StepCommitPlant):
		payload := &contractspb.CommitPlantPayload{}
		if err := proto.Unmarshal(call.Payload, payload); err != nil {
			return workflow.StepResult{}, err
		}
		resultMessage = &contractspb.CommitPlantResult{FarmId: payload.GetFarmId(), PlotId: payload.GetPlotId(), CropCycleId: payload.GetCropCycleId(), CropDefinitionId: payload.GetCropRuleSnapshot().GetCropDefinitionId(), AppliedCropRuleVersion: payload.GetCropRuleSnapshot().GetCropRuleVersion(), PlantAtMs: 1_800_000_000_100, MatureAtMs: 1_800_000_060_100, CommittedPlotVersion: payload.GetExpectedPlotVersion() + 1, CommittedFarmVersion: 8}
	case string(identity.StepCommitSeedReservation):
		payload := &contractspb.CommitSeedReservationPayload{}
		if err := proto.Unmarshal(call.Payload, payload); err != nil {
			return workflow.StepResult{}, err
		}
		resultMessage = &contractspb.CommitSeedReservationResult{ReservationId: payload.GetReservationId(), ReservationState: contractspb.ReservationTerminalState_RESERVATION_TERMINAL_STATE_CONSUMED, SeedItemId: 701, PlayerRecordVersion: 3}
	case string(identity.StepReleaseSeedReservation):
		payload := &contractspb.ReleaseSeedReservationPayload{}
		if err := proto.Unmarshal(call.Payload, payload); err != nil {
			return workflow.StepResult{}, err
		}
		resultMessage = &contractspb.ReleaseSeedReservationResult{ReservationId: payload.GetReservationId(), ReservationState: contractspb.ReservationTerminalState_RESERVATION_TERMINAL_STATE_RELEASED, SeedItemId: payload.GetSeedItemId(), PlayerRecordVersion: 3}
	default:
		return workflow.StepResult{}, fmt.Errorf("unknown step %q", call.StepName)
	}
	resultBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(resultMessage)
	if err != nil {
		return workflow.StepResult{}, err
	}
	resultHash, err := contracts.CanonicalParticipantHash(spec, resultMessage)
	if err != nil {
		return workflow.StepResult{}, err
	}
	outcome := p.outcomes[call.InvocationID]
	if outcome == workflow.StepOutcomeUnspecified {
		outcome = workflow.StepOutcomeApplied
	}
	return workflow.StepResult{Outcome: outcome, Result: resultBytes, ResultHash: resultHash, ErrorClass: p.name + "_REJECTED"}, nil
}

func (p *scriptedParticipant) stepNames() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, 0, len(p.calls))
	for _, call := range p.calls {
		out = append(out, call.StepName)
	}
	return out
}

func (p *scriptedParticipant) callsCopy() []workflow.StepCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]workflow.StepCall, len(p.calls))
	for i := range p.calls {
		out[i] = cloneStepCall(p.calls[i])
	}
	return out
}

func (p *scriptedParticipant) callsFor(invocation string) []workflow.StepCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []workflow.StepCall
	for _, call := range p.calls {
		if call.InvocationID == invocation {
			out = append(out, cloneStepCall(call))
		}
	}
	return out
}

func (p *scriptedParticipant) resultHash(invocation string) [32]byte {
	p.store.mu.Lock()
	defer p.store.mu.Unlock()
	return p.store.results[invocation]
}

func (p *scriptedParticipant) applyCountFor(invocation string) int {
	p.store.mu.Lock()
	defer p.store.mu.Unlock()
	return p.store.applies[invocation]
}

func (p *scriptedParticipant) wasReplayed(invocation string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.replayed[invocation]
}

type bucketResolutionCall struct {
	WorkID  string
	Version uint32
}

type recordingBucketResolver struct {
	mu    sync.Mutex
	calls []bucketResolutionCall
	err   error
}

func newRecordingBucketResolver() *recordingBucketResolver {
	return &recordingBucketResolver{}
}

func (r *recordingBucketResolver) IndependentWorkBucket(workID string, bucketVersion uint32) (uint32, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, bucketResolutionCall{WorkID: workID, Version: bucketVersion})
	if r.err != nil {
		return 0, r.err
	}
	return r.bucketFor(workID, bucketVersion), nil
}

func (*recordingBucketResolver) AuthorityPersistenceBucket(workidentity.RouteDomain, uint64, uint32) (uint32, error) {
	return 0, errors.New("authority bucket is outside Plant test scope")
}

func (r *recordingBucketResolver) bucketFor(workID string, bucketVersion uint32) uint32 {
	digest := sha256.Sum256(append([]byte{byte(bucketVersion)}, []byte(workID)...))
	return binary.BigEndian.Uint32(digest[:4])
}

func (r *recordingBucketResolver) callsCopy() []bucketResolutionCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]bucketResolutionCall(nil), r.calls...)
}

func forgedRootRecord(t *testing.T, record *workflow.WorkRecord, mutate func(*contractspb.CommitPlantPayload)) *workflow.WorkRecord {
	t.Helper()
	root := &contractspb.CommitPlantPayload{}
	if err := proto.Unmarshal(record.Payload, root); err != nil {
		t.Fatal(err)
	}
	mutate(root)
	snapshotHash, err := contracts.CanonicalHash(root.GetCropRuleSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	root.CropRuleSnapshotHash = append([]byte(nil), snapshotHash[:]...)
	payload, err := deterministicProto(root)
	if err != nil {
		t.Fatal(err)
	}
	rootHash, err := contracts.CanonicalHash(root)
	if err != nil {
		t.Fatal(err)
	}
	forged := *record
	forged.Payload = payload
	forged.PayloadHash = rootHash
	return &forged
}

func validFinalPlayerResult(t *testing.T, f *plantFixture, step int) (workflow.StepResult, proto.Message) {
	t.Helper()
	root, err := BuildRoot(f.input)
	if err != nil {
		t.Fatal(err)
	}
	reserveResult := &contractspb.ReserveSeedResult{
		ReservationId: root.ReservationID, SeedItemId: f.input.SeedItemID,
		ReservedQuantity: Quantity, PlayerRecordVersion: 2,
	}
	reserveBytes, err := deterministicProto(reserveResult)
	if err != nil {
		t.Fatal(err)
	}
	reserveSpec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED)
	if err != nil {
		t.Fatal(err)
	}
	reserveHash, err := contracts.CanonicalParticipantHash(reserveSpec, reserveResult)
	if err != nil {
		t.Fatal(err)
	}
	f.record.Steps[0].Status = workflow.StepApplied
	f.record.Steps[0].Result = reserveBytes
	f.record.Steps[0].ResultHash = reserveHash
	farmResult := &contractspb.CommitPlantResult{
		FarmId: root.Payload.GetFarmId(), PlotId: root.Payload.GetPlotId(), CropCycleId: root.CropCycleID,
		CropDefinitionId: root.Payload.GetCropRuleSnapshot().GetCropDefinitionId(), AppliedCropRuleVersion: root.Payload.GetCropRuleSnapshot().GetCropRuleVersion(),
	}
	farmBytes, err := deterministicProto(farmResult)
	if err != nil {
		t.Fatal(err)
	}
	farmSpec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT)
	if err != nil {
		t.Fatal(err)
	}
	farmHash, err := contracts.CanonicalParticipantHash(farmSpec, farmResult)
	if err != nil {
		t.Fatal(err)
	}
	f.record.Steps[1].Result = farmBytes
	f.record.Steps[1].ResultHash = farmHash
	if step == 2 {
		f.record.Steps[1].Status = workflow.StepApplied
	} else {
		f.record.Steps[1].Status = workflow.StepRejected
	}
	var (
		command contractspb.ParticipantCommandType
		message proto.Message
	)
	switch step {
	case 2:
		command = contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_SEED_RESERVATION
		message = &contractspb.CommitSeedReservationResult{ReservationId: root.ReservationID, ReservationState: contractspb.ReservationTerminalState_RESERVATION_TERMINAL_STATE_CONSUMED, SeedItemId: f.input.SeedItemID, PlayerRecordVersion: 3}
	case 3:
		command = contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RELEASE_SEED_RESERVATION
		message = &contractspb.ReleaseSeedReservationResult{ReservationId: root.ReservationID, ReservationState: contractspb.ReservationTerminalState_RESERVATION_TERMINAL_STATE_RELEASED, SeedItemId: f.input.SeedItemID, PlayerRecordVersion: 3}
	default:
		t.Fatalf("unsupported final step %d", step)
	}
	result, err := deterministicProto(message)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := contracts.LookupParticipantCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := contracts.CanonicalParticipantHash(spec, message)
	if err != nil {
		t.Fatal(err)
	}
	return workflow.StepResult{Outcome: workflow.StepOutcomeApplied, Result: result, ResultHash: hash}, message
}

func commandForStepMust(t *testing.T, step int) contractspb.ParticipantCommandType {
	t.Helper()
	name := (&Definition{}).StepNames()[step]
	command, err := commandForStep(name)
	if err != nil {
		t.Fatal(err)
	}
	return command
}

func cloneStepCall(call workflow.StepCall) workflow.StepCall {
	call.Payload = append([]byte(nil), call.Payload...)
	return call
}

// TestStaleWorkProofErrorClassification locks the exact classification of a
// participant WorkVersion proof mismatch (WF-WORKVERSION-PROOF-RACE) through
// the shared workflow.ClassifyStaleWorkProof classifier:
//   - only gRPC FailedPrecondition + the exact "participant proof: WorkVersion"
//     prefix wraps as workflow.ErrStaleWorkProof (the participant verifier's
//     ErrWorkVersionMismatch text);
//   - the same text under a different gRPC code stays unwrapped;
//   - every other FailedPrecondition message (idempotency conflict, identity
//     mismatch, unregistered step, verifier-not-configured, unrelated text)
//     stays unwrapped so the generic error/INVOCATION_UNKNOWN path owns it and
//     stale proof is never misclassified from an unrelated precondition.
func TestStaleWorkProofErrorClassification(t *testing.T) {
	if workflow.ClassifyStaleWorkProof(nil) != nil {
		t.Fatal("nil must stay nil")
	}
	wrapped := workflow.ClassifyStaleWorkProof(status.Error(codes.FailedPrecondition, "participant proof: WorkVersion mismatch"))
	if !errors.Is(wrapped, workflow.ErrStaleWorkProof) {
		t.Fatalf("WorkVersion mismatch must wrap ErrStaleWorkProof, got %v", wrapped)
	}
	if got := workflow.ClassifyStaleWorkProof(status.Error(codes.Aborted, "participant proof: WorkVersion mismatch")); errors.Is(got, workflow.ErrStaleWorkProof) {
		t.Fatalf("non-FailedPrecondition must not classify as stale proof: %v", got)
	}
	for _, message := range []string{
		"PARTICIPANT_IDEMPOTENCY_CONFLICT",
		"participant proof: identity mismatch",
		`participant proof: step "COMMIT_PLANT" is not registered`,
		"farm participant WorkVersion verifier is not configured",
		"player participant work version verifier is not configured",
		"unrelated precondition failure",
	} {
		if got := workflow.ClassifyStaleWorkProof(status.Error(codes.FailedPrecondition, message)); errors.Is(got, workflow.ErrStaleWorkProof) {
			t.Fatalf("FailedPrecondition %q must not classify as stale proof: %v", message, got)
		}
	}
}

type rpcClientFunc func(context.Context, *contractspb.ParticipantRequest, ...grpc.CallOption) (*contractspb.ParticipantResponse, error)

func (f rpcClientFunc) ExecuteStep(ctx context.Context, request *contractspb.ParticipantRequest, options ...grpc.CallOption) (*contractspb.ParticipantResponse, error) {
	return f(ctx, request, options...)
}

type workerRPCClientFunc func(context.Context, *contractspb.ParticipantRequest, ...grpc.CallOption) (*contractspb.ParticipantResponse, error)

func (f workerRPCClientFunc) ResumeStep(ctx context.Context, request *contractspb.ParticipantRequest, options ...grpc.CallOption) (*contractspb.ParticipantResponse, error) {
	return f(ctx, request, options...)
}

func decodeCallPayload[T proto.Message](t *testing.T, call workflow.StepCall) T {
	t.Helper()
	var result T
	// The four typed payloads are all pointer protobuf messages. Allocate via
	// the concrete type selected by the generic type parameter's zero value.
	switch any(result).(type) {
	case *contractspb.ReleaseSeedReservationPayload:
		result = any(&contractspb.ReleaseSeedReservationPayload{}).(T)
	case *contractspb.CommitPlantPayload:
		result = any(&contractspb.CommitPlantPayload{}).(T)
	case *contractspb.CommitSeedReservationPayload:
		result = any(&contractspb.CommitSeedReservationPayload{}).(T)
	case *contractspb.ReserveSeedPayload:
		result = any(&contractspb.ReserveSeedPayload{}).(T)
	default:
		t.Fatalf("unsupported payload type %T", result)
	}
	if err := proto.Unmarshal(call.Payload, result); err != nil {
		t.Fatal(err)
	}
	return result
}
