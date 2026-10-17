package friend

import (
	"context"
	"errors"
	"testing"

	logicrpcpb "farmgame/api/gen/logicrpc"

	"google.golang.org/grpc"
)

const testOpportunityCycleID = "sid1_pK6aJIBpMU-PpnlrN1v-fHZrdkGG_fgNJOFHG70Bg4s"

type testLogicServiceClient struct {
	logicrpcpb.LogicServiceClient
	headerResponse *logicrpcpb.BatchReadFarmOpportunityHeaderResponse
	headerErr      error
}

func (c *testLogicServiceClient) BatchReadFarmOpportunityHeader(context.Context, *logicrpcpb.BatchReadFarmOpportunityHeaderRequest, ...grpc.CallOption) (*logicrpcpb.BatchReadFarmOpportunityHeaderResponse, error) {
	return c.headerResponse, c.headerErr
}

func TestFriendOpportunityRPCExactTargetsAndPerRowUnknown(t *testing.T) {
	valid := &logicrpcpb.FarmOpportunityReadResult{
		OwnerPlayerId:        22,
		Status:               logicrpcpb.FarmOpportunityReadStatus_FARM_OPPORTUNITY_READ_STATUS_FOUND,
		ActionMask:           FriendActionCanTheft | FriendActionCanVisit,
		OpportunityRevision:  9,
		EligibleTheftTargets: []*logicrpcpb.FarmOpportunityTheftTarget{{PlotId: 4, CropCycleId: testOpportunityCycleID}},
	}
	got, err := friendOpportunityEvaluationFromRPC(valid)
	if err != nil || got.Status != OpportunityReadReady || got.ActionMask != valid.ActionMask ||
		len(got.EligibleTheftTargets) != 1 || got.EligibleTheftTargets[0].PlotID != 4 {
		t.Fatalf("evaluation=%+v err=%v", got, err)
	}

	for name, row := range map[string]*logicrpcpb.FarmOpportunityReadResult{
		"malformed target": {
			OwnerPlayerId: 22, Status: logicrpcpb.FarmOpportunityReadStatus_FARM_OPPORTUNITY_READ_STATUS_FOUND,
			ActionMask: FriendActionCanTheft, EligibleTheftTargets: []*logicrpcpb.FarmOpportunityTheftTarget{{PlotId: 4, CropCycleId: "cycle-4"}},
		},
		"mask mismatch": {
			OwnerPlayerId: 22, Status: logicrpcpb.FarmOpportunityReadStatus_FARM_OPPORTUNITY_READ_STATUS_FOUND,
			ActionMask: FriendActionCanTheft,
		},
		"unknown carrying target": {
			OwnerPlayerId: 22, Status: logicrpcpb.FarmOpportunityReadStatus_FARM_OPPORTUNITY_READ_STATUS_UNKNOWN,
			EligibleTheftTargets: []*logicrpcpb.FarmOpportunityTheftTarget{{PlotId: 4, CropCycleId: testOpportunityCycleID}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := friendOpportunityEvaluationFromRPC(row)
			if err != nil || got.Status != OpportunityReadUnknown || got.ActionMask != 0 || len(got.EligibleTheftTargets) != 0 {
				t.Fatalf("evaluation=%+v err=%v", got, err)
			}
		})
	}
}

func TestGRPCOpportunityEvaluatorReadsHeaderCandidates(t *testing.T) {
	client := &testLogicServiceClient{headerResponse: &logicrpcpb.BatchReadFarmOpportunityHeaderResponse{
		Results: []*logicrpcpb.FarmOpportunityHeaderReadResult{
			{OwnerPlayerId: 11, Status: logicrpcpb.FarmOpportunityReadStatus_FARM_OPPORTUNITY_READ_STATUS_FOUND,
				SourceFarmVersion: 8, NextOpportunityAtMs: 900},
			{OwnerPlayerId: 12, Status: logicrpcpb.FarmOpportunityReadStatus_FARM_OPPORTUNITY_READ_STATUS_FOUND,
				SourceFarmVersion: 9, NextOpportunityAtMs: 1100},
			{OwnerPlayerId: 13, Status: logicrpcpb.FarmOpportunityReadStatus_FARM_OPPORTUNITY_READ_STATUS_NOT_FOUND},
			{OwnerPlayerId: 14, Status: logicrpcpb.FarmOpportunityReadStatus_FARM_OPPORTUNITY_READ_STATUS_UNKNOWN},
		},
	}}
	evaluator, err := NewGRPCOpportunityEvaluator(client, 1)
	if err != nil {
		t.Fatal(err)
	}
	candidates, err := evaluator.BatchGetCandidates(context.Background(), []uint64{11, 12, 13, 14}, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if candidates[11].State != CandidateYes || candidates[11].SourceFarmVersion != 8 || candidates[11].ActionableAtMs != 900 ||
		candidates[12].State != CandidateNone || candidates[12].ActionableAtMs != 1100 ||
		candidates[13].State != CandidateUnknown || candidates[14].State != CandidateUnknown {
		t.Fatalf("candidates=%+v", candidates)
	}
}

func TestGRPCOpportunityEvaluatorReturnsHeaderRPCFailure(t *testing.T) {
	wantErr := errors.New("farm read unavailable")
	evaluator, err := NewGRPCOpportunityEvaluator(&testLogicServiceClient{headerErr: wantErr}, 1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = evaluator.BatchGetCandidates(context.Background(), []uint64{11}, 1000)
	if !errors.Is(err, wantErr) {
		t.Fatalf("err=%v want=%v", err, wantErr)
	}
}
