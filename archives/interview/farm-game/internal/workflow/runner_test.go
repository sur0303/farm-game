package workflow

import (
	"context"
	"errors"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"
	logicrpcpb "farmgame/api/gen/logicrpc"
	playerpb "farmgame/api/gen/player"
	workflowpb "farmgame/api/gen/workflow"
	"farmgame/internal/workidentity"
	"google.golang.org/protobuf/proto"
)

type fakePlayer struct {
	commits               int
	releases              int
	grants                int
	grantErr              error
	rewards               []*logicrpcpb.ApplyRewardRequest
	committedReservations []string
	releasedReservations  []string
}

func (p *fakePlayer) CommitReservation(_ context.Context, req *logicrpcpb.CommitReservationRequest) (*logicrpcpb.ReservationResponse, error) {
	p.commits++
	p.committedReservations = append(p.committedReservations, req.ReservationId)
	return &logicrpcpb.ReservationResponse{Status: commonpb.CommandStatus_SUCCEEDED, PrivateState: &playerpb.PrivateState{Coin: 90}}, nil
}
func (p *fakePlayer) ReleaseReservation(_ context.Context, req *logicrpcpb.ReleaseReservationRequest) (*logicrpcpb.ReservationResponse, error) {
	p.releases++
	p.releasedReservations = append(p.releasedReservations, req.ReservationId)
	return &logicrpcpb.ReservationResponse{Status: commonpb.CommandStatus_SUCCEEDED}, nil
}
func (p *fakePlayer) GrantItem(context.Context, *logicrpcpb.GrantItemRequest) (*logicrpcpb.GrantItemResponse, error) {
	p.grants++
	if p.grantErr != nil {
		return nil, p.grantErr
	}
	return &logicrpcpb.GrantItemResponse{Status: commonpb.CommandStatus_SUCCEEDED, PrivateState: &playerpb.PrivateState{Coin: 100}}, nil
}
func (p *fakePlayer) ApplyReward(_ context.Context, req *logicrpcpb.ApplyRewardRequest) (*logicrpcpb.ApplyRewardResponse, error) {
	p.grants++
	p.rewards = append(p.rewards, proto.Clone(req).(*logicrpcpb.ApplyRewardRequest))
	if p.grantErr != nil {
		return nil, p.grantErr
	}
	return &logicrpcpb.ApplyRewardResponse{Status: commonpb.CommandStatus_SUCCEEDED, PrivateState: &playerpb.PrivateState{Coin: 100}}, nil
}

type fakeFarm struct {
	inspection *logicrpcpb.InspectFarmOperationResponse
	completes  int
}

func (f *fakeFarm) InspectFarmOperation(context.Context, *logicrpcpb.InspectFarmOperationRequest) (*logicrpcpb.InspectFarmOperationResponse, error) {
	return f.inspection, nil
}
func (f *fakeFarm) CompleteFarmOperation(context.Context, *logicrpcpb.CompleteFarmOperationRequest) (*logicrpcpb.CompleteFarmOperationResponse, error) {
	f.completes++
	f.inspection.Status = commonpb.CommandStatus_SUCCEEDED
	return &logicrpcpb.CompleteFarmOperationResponse{Status: commonpb.CommandStatus_SUCCEEDED}, nil
}

func createAndClaim(t *testing.T, store *MemoryStore, record *workflowpb.WorkflowRecord, owner string, now time.Time) *workflowpb.WorkflowRecord {
	t.Helper()
	record.NextRetryAtMs = now.UnixMilli()
	if _, _, err := store.CreateOrLoad(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimDue(context.Background(), owner, now, 1, 5*time.Second)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim=%v err=%v", claimed, err)
	}
	return claimed[0]
}

func TestRunnerCompensatesPlantWhenFarmNeverCommitted(t *testing.T) {
	store := NewMemoryStore()
	player := &fakePlayer{}
	farm := &fakeFarm{inspection: &logicrpcpb.InspectFarmOperationResponse{
		Status: commonpb.CommandStatus_REJECTED, ErrorCode: commonpb.ErrorCode_ERR_SETTLEMENT_NOT_FOUND,
	}}
	runner, _ := NewRunner(store, player, farm)
	now := time.UnixMilli(1_800_000_000_000)
	runner.now = func() time.Time { return now }
	record := createAndClaim(t, store,
		testWorkflowRecord("plant-1", workflowpb.WorkflowKind_PLANT, 9, 9, testHash(1), now),
		"worker-a", now)
	if err := runner.Process(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	saved, _ := store.Get(context.Background(), record.WorkflowId)
	if saved.State != workflowpb.WorkflowState_COMPENSATED || player.releases != 1 || player.commits != 0 {
		t.Fatalf("saved=%v player=%+v", saved, player)
	}
	wantReservationID, _ := workidentity.SeedReservationID(record.WorkflowId)
	if len(player.releasedReservations) != 1 || player.releasedReservations[0] != wantReservationID {
		t.Fatalf("released reservation=%v want=%q", player.releasedReservations, wantReservationID)
	}
}

func TestRunnerCompletesPlantFromDurableFarmReceipt(t *testing.T) {
	store := NewMemoryStore()
	player := &fakePlayer{}
	farm := &fakeFarm{inspection: &logicrpcpb.InspectFarmOperationResponse{
		Status: commonpb.CommandStatus_PROCESSING,
	}}
	runner, _ := NewRunner(store, player, farm)
	now := time.UnixMilli(1_800_000_000_000)
	runner.now = func() time.Time { return now }
	record := createAndClaim(t, store,
		testWorkflowRecord("plant-committed", workflowpb.WorkflowKind_PLANT, 9, 9, testHash(1), now),
		"worker-a", now)

	if err := runner.Process(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	saved, err := store.Get(context.Background(), record.WorkflowId)
	if err != nil || saved.State != workflowpb.WorkflowState_COMPLETED ||
		player.commits != 1 || player.releases != 0 || farm.completes != 1 {
		t.Fatalf("saved=%v player=%+v farm=%+v err=%v", saved, player, farm, err)
	}
	wantReservationID, _ := workidentity.SeedReservationID(record.WorkflowId)
	if len(player.committedReservations) != 1 || player.committedReservations[0] != wantReservationID {
		t.Fatalf("committed reservation=%v want=%q", player.committedReservations, wantReservationID)
	}
}

func TestRunnerOnlyClosesWorkflowWhenFarmAlreadyCompleted(t *testing.T) {
	store := NewMemoryStore()
	player := &fakePlayer{}
	farm := &fakeFarm{inspection: &logicrpcpb.InspectFarmOperationResponse{
		Status: commonpb.CommandStatus_SUCCEEDED,
	}}
	runner, _ := NewRunner(store, player, farm)
	now := time.UnixMilli(1_800_000_000_000)
	runner.now = func() time.Time { return now }
	record := createAndClaim(t, store,
		testWorkflowRecord("harvest-completed", workflowpb.WorkflowKind_HARVEST, 9, 9, testHash(1), now),
		"worker-a", now)

	if err := runner.Process(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if record.State != workflowpb.WorkflowState_COMPLETED ||
		player.grants != 0 || player.commits != 0 || farm.completes != 0 {
		t.Fatalf("already completed operation repeated a participant: record=%v player=%+v farm=%+v", record, player, farm)
	}
}

func TestRunnerCompletesAllCareSettlementParticipants(t *testing.T) {
	store := NewMemoryStore()
	player := &fakePlayer{}
	farm := &fakeFarm{inspection: &logicrpcpb.InspectFarmOperationResponse{
		Status: commonpb.CommandStatus_PROCESSING,
		Settlement: &farmpb.FarmSettlement{
			SettlementId: "care-1", Kind: farmpb.FarmSettlementKind_CARE,
			Participants: []*farmpb.FarmSettlementParticipant{
				{PlayerId: 10, Role: farmpb.FarmParticipantRole_HELPER, ExpDelta: 2},
				{PlayerId: 9, Role: farmpb.FarmParticipantRole_OWNER, ExpDelta: 1},
			},
		},
	}}
	runner, _ := NewRunner(store, player, farm)
	now := time.UnixMilli(1_800_000_000_000)
	runner.now = func() time.Time { return now }
	record := createAndClaim(t, store,
		testWorkflowRecord("care-1", workflowpb.WorkflowKind_CARE, 9, 10, testHash(1), now),
		"worker-care", now)

	if err := runner.Process(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if record.State != workflowpb.WorkflowState_COMPLETED ||
		player.grants != 2 || farm.completes != 1 {
		t.Fatalf("care record=%v player=%+v farm=%+v", record, player, farm)
	}
}

func TestRunnerCarriesHarvestCodexUnlockAcrossProcessRecovery(t *testing.T) {
	store := NewMemoryStore()
	player := &fakePlayer{}
	farm := &fakeFarm{inspection: &logicrpcpb.InspectFarmOperationResponse{
		Status: commonpb.CommandStatus_PROCESSING,
		Settlement: &farmpb.FarmSettlement{
			SettlementId: "harvest-codex-1", Kind: farmpb.FarmSettlementKind_HARVEST,
			Participants: []*farmpb.FarmSettlementParticipant{{
				PlayerId: 9, Role: farmpb.FarmParticipantRole_HARVESTER,
				ItemId: 2001, ItemCount: 10, ExpDelta: 10, CodexCropDefinitionId: 1,
			}},
		},
	}}
	runner, _ := NewRunner(store, player, farm)
	now := time.UnixMilli(1_800_000_000_000)
	runner.now = func() time.Time { return now }
	record := createAndClaim(t, store,
		testWorkflowRecord("harvest-codex-1", workflowpb.WorkflowKind_HARVEST, 9, 9, testHash(2), now),
		"worker-harvest", now)

	if err := runner.Process(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if record.State != workflowpb.WorkflowState_COMPLETED || len(player.rewards) != 1 ||
		player.rewards[0].CodexCropDefinitionId != 1 || farm.completes != 1 {
		t.Fatalf("recovered codex reward lost: record=%v rewards=%+v completes=%d", record, player.rewards, farm.completes)
	}
}

func TestRunnerRetriesGrantThenCompletesAfterNewClaim(t *testing.T) {
	store := NewMemoryStore()
	player := &fakePlayer{grantErr: errors.New("player temporarily unavailable")}
	farm := &fakeFarm{inspection: &logicrpcpb.InspectFarmOperationResponse{
		Status: commonpb.CommandStatus_PROCESSING,
		Settlement: &farmpb.FarmSettlement{
			SettlementId: "theft-1", BeneficiaryPlayerId: 10, ItemId: 2001, ItemCount: 1,
		},
	}}
	runner, _ := NewRunner(store, player, farm)
	now := time.UnixMilli(1_800_000_000_000)
	runner.now = func() time.Time { return now }
	record := createAndClaim(t, store,
		testWorkflowRecord("theft-1", workflowpb.WorkflowKind_THEFT, 9, 10, testHash(1), now),
		"worker-a", now)

	if err := runner.Process(context.Background(), record); err == nil {
		t.Fatal("first grant should fail")
	}
	if record.State != workflowpb.WorkflowState_RETRY_WAIT || record.Attempts != 1 ||
		record.LeaseOwner != "" || farm.completes != 0 {
		t.Fatalf("after failure record=%v completes=%d", record, farm.completes)
	}

	player.grantErr = nil
	now = now.Add(time.Second)
	claimed, err := store.ClaimDue(context.Background(), "worker-b", now, 1, 5*time.Second)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("second claim=%v err=%v", claimed, err)
	}
	record = claimed[0]
	if err := runner.Process(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if record.State != workflowpb.WorkflowState_COMPLETED || farm.completes != 1 || player.grants != 2 {
		t.Fatalf("completed record=%v farm=%+v player=%+v", record, farm, player)
	}
	if err := runner.Process(context.Background(), record); err != nil || farm.completes != 1 || player.grants != 2 {
		t.Fatalf("terminal replay performed side effects")
	}
}

func TestRunnerRejectsUnclaimedWorkflow(t *testing.T) {
	store := NewMemoryStore()
	runner, _ := NewRunner(store, &fakePlayer{}, &fakeFarm{})
	now := time.UnixMilli(1_800_000_000_000)
	record := testWorkflowRecord("plant-1", workflowpb.WorkflowKind_PLANT, 9, 9, testHash(1), now)
	if err := runner.Process(context.Background(), record); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("unclaimed workflow processed: %v", err)
	}
}
