package ingress

import (
	"context"
	"testing"

	farmpb "farmgame/api/gen/farm"
	logicrpcpb "farmgame/api/gen/logicrpc"
	farmactor "farmgame/internal/farm/actor"
	"farmgame/internal/farm/domain"
	"farmgame/internal/farm/readpath"
)

type testHintPublisher struct {
	owner, version, revision uint64
	calls                    int
}

func (p *testHintPublisher) PublishFriendOpportunityHint(_ context.Context, owner, version, revision uint64) error {
	p.owner, p.version, p.revision, p.calls = owner, version, revision, p.calls+1
	return nil
}

type testFriendOpportunityReader struct {
	rows  []readpath.EvaluationResult
	calls int
}

func (r *testFriendOpportunityReader) BatchEvaluate(_ context.Context, _ uint64, _ []uint64, _ int64) ([]readpath.EvaluationResult, error) {
	r.calls++
	return r.rows, nil
}

func TestEmitChangePublishesPullOnlyFriendOpportunityHint(t *testing.T) {
	publisher := &testHintPublisher{}
	service := &Service{}
	service.SetFriendOpportunityHintPublisher(publisher)
	service.EmitChange(10, &farmactor.ActionResult{FriendOpportunityInvalidated: true,
		Change: &farmpb.FarmChange{OwnerId: 10, FarmVersion: 7}})
	wantRevision, _ := domain.FarmOpportunityRevisionForSource(7)
	if publisher.calls != 1 || publisher.owner != 10 || publisher.version != 7 || publisher.revision != wantRevision {
		t.Fatalf("publisher=%+v", publisher)
	}
}

func TestEmitChangeDoesNotPublishForFutureOnlyPlantMutation(t *testing.T) {
	publisher := &testHintPublisher{}
	service := &Service{}
	service.SetFriendOpportunityHintPublisher(publisher)
	service.EmitChange(10, &farmactor.ActionResult{
		Change: &farmpb.FarmChange{OwnerId: 10, FarmVersion: 7},
	})
	if publisher.calls != 0 {
		t.Fatalf("future-only mutation published %d Hint(s)", publisher.calls)
	}
}

func TestBatchEvaluateFarmOpportunityPreservesPerRowUnknown(t *testing.T) {
	reader := &testFriendOpportunityReader{rows: []readpath.EvaluationResult{
		{FarmID: 10, Status: readpath.StatusFound, OpportunityRevision: 7,
			Evaluation: domain.EvaluatedFarmOpportunity{Mask: domain.ViewerCanSteal | domain.ViewerCanWater,
				EligibleTheftTargets: []domain.EligibleTheftTarget{{PlotID: 4, CycleID: "cycle-4"}}}},
		{FarmID: 11, Status: readpath.StatusUnknown},
		{FarmID: 12, Status: readpath.StatusNotFound},
	}}
	service := &Service{}
	service.SetFriendOpportunityReader(reader)
	response, err := service.BatchEvaluateFarmOpportunity(context.Background(), &logicrpcpb.BatchEvaluateFarmOpportunityRequest{
		ViewerPlayerId: 9, OwnerPlayerIds: []uint64{10, 11, 12}, TrustedNowMs: 1000,
	})
	if err != nil || reader.calls != 1 || len(response.GetResults()) != 3 {
		t.Fatalf("response=%v calls=%d err=%v", response, reader.calls, err)
	}
	if response.Results[0].Status != logicrpcpb.FarmOpportunityReadStatus_FARM_OPPORTUNITY_READ_STATUS_FOUND ||
		response.Results[0].ActionMask != uint32(domain.ViewerCanSteal|domain.ViewerCanWater) || response.Results[0].OpportunityRevision != 7 ||
		len(response.Results[0].EligibleTheftTargets) != 1 || response.Results[0].EligibleTheftTargets[0].PlotId != 4 {
		t.Fatalf("found=%v", response.Results[0])
	}
	if response.Results[1].Status != logicrpcpb.FarmOpportunityReadStatus_FARM_OPPORTUNITY_READ_STATUS_UNKNOWN || response.Results[1].ActionMask != 0 {
		t.Fatalf("unknown=%v", response.Results[1])
	}
	if response.Results[2].Status != logicrpcpb.FarmOpportunityReadStatus_FARM_OPPORTUNITY_READ_STATUS_NOT_FOUND {
		t.Fatalf("not found=%v", response.Results[2])
	}
}
