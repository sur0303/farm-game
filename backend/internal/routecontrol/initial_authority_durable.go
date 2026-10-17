package routecontrol

import (
	"context"
	"fmt"
)

func (p *DurableSnapshotPublisher) commitInitialAuthorityBatch(ctx context.Context, token NestedControlOperationToken, capability *initialAuthorityBatchCapability) (SnapshotCASResult, error) {
	if p == nil || p.store == nil || capability == nil {
		return SnapshotCASResult{}, ErrInitialAuthorityBatchInvalid
	}
	if err := p.checkToken(token); err != nil {
		return SnapshotCASResult{}, err
	}
	if !token.Issued() {
		return SnapshotCASResult{}, ErrInitialAuthorityBatchTokenRequired
	}
	if err := requireContext(ctx); err != nil {
		return SnapshotCASResult{}, err
	}
	expected, next, records, now, err := InitialAuthorityBatchValues(capability)
	if err != nil {
		return SnapshotCASResult{}, err
	}
	if err := ValidateInitialAuthorityBatchAt(expected, next, records, "", now); err != nil {
		return SnapshotCASResult{}, err
	}
	batchStore, ok := p.store.(InitialAuthorityBatchStore)
	if !ok {
		return SnapshotCASResult{}, ErrInitialAuthorityBatchUnsupported
	}
	result, err := batchStore.CompareAndSwapInitialAuthorityBatch(ctx, token, capability)
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
		return result, fmt.Errorf("%w: initial authority batch status=%s", ErrSnapshotCASConflict, result.Status)
	default:
		return result, fmt.Errorf("%w: initial authority batch status=%q", ErrSnapshotCASConflict, result.Status)
	}
}
