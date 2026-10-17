package friend

import (
	"context"
	"errors"

	"farmgame/internal/farm/domain"
	"farmgame/internal/farm/readpath"
)

// AuthorityOpportunityReadPath is the farm-svr read-only port consumed by
// friend navigation. Its production implementation reads compact Authority
// Header/Facts and never activates FarmActor.
type AuthorityOpportunityReadPath interface {
	BatchReadHeaders(ctx context.Context, farmIDs []uint64) ([]readpath.HeaderResult, error)
	BatchEvaluate(ctx context.Context, viewerPlayerID uint64, farmIDs []uint64, trustedNowMs int64) ([]readpath.EvaluationResult, error)
}

// AuthorityOpportunityProvider is the P0 CandidateReader + ExactEvaluator.
// It deliberately keeps FriendRelation validation outside this adapter and
// keeps final mutation adjudication in FarmActor.
type AuthorityOpportunityProvider struct{ readPath AuthorityOpportunityReadPath }

func NewAuthorityOpportunityProvider(readPath AuthorityOpportunityReadPath) (*AuthorityOpportunityProvider, error) {
	if readPath == nil {
		return nil, errors.New("friend opportunity: authority read path is required")
	}
	return &AuthorityOpportunityProvider{readPath: readPath}, nil
}

func (p *AuthorityOpportunityProvider) BatchGetCandidates(ctx context.Context, farmIDs []uint64, nowMs int64) (map[uint64]FarmOpportunityCandidate, error) {
	rows, err := p.readPath.BatchReadHeaders(ctx, farmIDs)
	if err != nil {
		return nil, err
	}
	result := make(map[uint64]FarmOpportunityCandidate, len(farmIDs))
	for _, row := range rows {
		candidate := FarmOpportunityCandidate{FarmID: row.FarmID, State: CandidateUnknown}
		if row.Err == nil && row.Status == readpath.StatusFound {
			candidate.ActionableAtMs = row.Header.NextOpportunityAtMs
			if row.IsCandidate(nowMs) {
				candidate.State = CandidateYes
			} else {
				candidate.State = CandidateNone
			}
		}
		result[row.FarmID] = candidate
	}
	// Cardinality/order mismatches are degraded per farm to UNKNOWN. The
	// caller will run exact evaluation instead of silently hiding a card.
	for _, farmID := range farmIDs {
		if _, ok := result[farmID]; !ok {
			result[farmID] = FarmOpportunityCandidate{FarmID: farmID, State: CandidateUnknown}
		}
	}
	return result, nil
}

func (p *AuthorityOpportunityProvider) BatchEvaluate(ctx context.Context, viewer uint64, farmIDs []uint64, nowMs int64) (map[uint64]ActionMask, error) {
	rows, err := p.readPath.BatchEvaluate(ctx, viewer, farmIDs, nowMs)
	if err != nil {
		return nil, err
	}
	result := make(map[uint64]ActionMask, len(rows))
	for _, row := range rows {
		if row.Err != nil || row.Status != readpath.StatusFound {
			// Explicit UNKNOWN makes page responses explainable. Navigator also
			// recognizes this bit and keeps the candidate retryable.
			result[row.FarmID] = ActionUnknown
			continue
		}
		result[row.FarmID] = actionMaskFromDomain(row.Evaluation.Mask)
	}
	return result, nil
}

func actionMaskFromDomain(mask domain.ViewerOpportunityMask) ActionMask {
	result := ActionNone
	if mask&domain.ViewerCanSteal != 0 {
		result |= ActionCanSteal
	}
	if mask&domain.ViewerCanWater != 0 {
		result |= ActionCanWater
	}
	if mask&domain.ViewerCanRemovePest != 0 {
		result |= ActionCanRemovePest
	}
	return result
}

var _ CandidateReader = (*AuthorityOpportunityProvider)(nil)
var _ ExactEvaluator = (*AuthorityOpportunityProvider)(nil)
