package friend

import (
	"context"
	"testing"

	"farmgame/internal/farm/domain"
	"farmgame/internal/farm/readpath"
)

type fakeAuthorityOpportunityReadPath struct {
	headers []readpath.HeaderResult
	exact   []readpath.EvaluationResult
}

func (f fakeAuthorityOpportunityReadPath) BatchReadHeaders(context.Context, []uint64) ([]readpath.HeaderResult, error) {
	return f.headers, nil
}

func (f fakeAuthorityOpportunityReadPath) BatchEvaluate(context.Context, uint64, []uint64, int64) ([]readpath.EvaluationResult, error) {
	return f.exact, nil
}

func TestAuthorityOpportunityProviderMapsFoundAndUnknown(t *testing.T) {
	provider, err := NewAuthorityOpportunityProvider(fakeAuthorityOpportunityReadPath{
		headers: []readpath.HeaderResult{
			{FarmID: 11, Status: readpath.StatusFound, Header: domain.FarmOpportunityHeaderV2{NextOpportunityAtMs: 900}},
			{FarmID: 12, Status: readpath.StatusUnknown},
			{FarmID: 13, Status: readpath.StatusFound, Header: domain.FarmOpportunityHeaderV2{NextOpportunityAtMs: 1100}},
		},
		exact: []readpath.EvaluationResult{
			{FarmID: 11, Status: readpath.StatusFound, Evaluation: domain.EvaluatedFarmOpportunity{Mask: domain.ViewerCanSteal | domain.ViewerCanWater}},
			{FarmID: 12, Status: readpath.StatusUnknown},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	candidates, err := provider.BatchGetCandidates(context.Background(), []uint64{11, 12, 13, 14}, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if candidates[11].State != CandidateYes || candidates[12].State != CandidateUnknown ||
		candidates[13].State != CandidateNone || candidates[14].State != CandidateUnknown {
		t.Fatalf("candidates=%+v", candidates)
	}
	exact, err := provider.BatchEvaluate(context.Background(), 10001, []uint64{11, 12}, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if exact[11] != ActionCanSteal|ActionCanWater || exact[12] != ActionUnknown {
		t.Fatalf("exact=%+v", exact)
	}
}
