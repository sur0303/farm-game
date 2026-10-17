package routecontrol

import (
	"context"
	"fmt"
)

func (p *DurableSnapshotPublisher) commitAuthorityWaveBatch(ctx context.Context, token NestedControlOperationToken, capability *authorityWaveBatchCapability) (SnapshotCASResult, error) {
	if p == nil || p.store == nil || capability == nil {
		return SnapshotCASResult{}, ErrAuthorityWaveBatchInvalid
	}
	if err := p.checkToken(token); err != nil {
		return SnapshotCASResult{}, err
	}
	if !token.Issued() {
		return SnapshotCASResult{}, ErrAuthorityWaveBatchTokenRequired
	}
	if err := requireContext(ctx); err != nil {
		return SnapshotCASResult{}, err
	}
	expected, _, steps, members, _, err := AuthorityWaveBatchValues(capability)
	if err != nil {
		return SnapshotCASResult{}, err
	}
	if err := ValidateAuthorityWaveBatch(expected, steps, members); err != nil {
		return SnapshotCASResult{}, err
	}
	batchStore, ok := p.store.(AuthorityWaveBatchStore)
	if !ok {
		return SnapshotCASResult{}, ErrAuthorityWaveBatchUnsupported
	}
	result, err := batchStore.CompareAndSwapAuthorityWaveBatch(ctx, token, capability)
	if err != nil {
		return result, err
	}
	switch result.Status {
	case SnapshotCASCommitted, SnapshotCASAlreadyCommitted:
		if err := p.notifyAuthority(ctx, result.Stored.Snapshot); err != nil {
			return result, &SnapshotPublishError{Status: result.Status, Committed: true, Err: err}
		}
		return result, nil
	case SnapshotCASRevisionConflict, SnapshotCASStaleExpectation:
		return result, fmt.Errorf("%w: authority wave batch status=%s", ErrSnapshotCASConflict, result.Status)
	default:
		return result, fmt.Errorf("%w: authority wave batch status=%q", ErrSnapshotCASConflict, result.Status)
	}
}
