package routecontrol

import (
	"context"
	"fmt"
	"strconv"

	core "farmgame/internal/routecontrol"
)

var _ core.AuthorityWaveBatchStore = (*SnapshotStore)(nil)

// CompareAndSwapAuthorityWaveBatch uses the same exact-envelope jump CAS as
// startup batching. The unforgeable capability and migration-specific
// validator are independent; ordinary CompareAndSwap remains strict N->N+1.
func (s *SnapshotStore) CompareAndSwapAuthorityWaveBatch(ctx context.Context, token core.NestedControlOperationToken, capability core.AuthorityWaveBatchCapability) (core.SnapshotCASResult, error) {
	if s == nil {
		return core.SnapshotCASResult{}, ErrInvalidOptions
	}
	if !token.Issued() {
		return core.SnapshotCASResult{}, core.ErrAuthorityWaveBatchTokenRequired
	}
	expected, nextSnapshot, steps, members, _, err := core.AuthorityWaveBatchValues(capability)
	if err != nil {
		return core.SnapshotCASResult{}, err
	}
	if err := core.ValidateAuthorityWaveBatch(expected, steps, members); err != nil {
		return core.SnapshotCASResult{}, err
	}
	candidate, err := core.CanonicalizeRouteSnapshot(nextSnapshot)
	if err != nil {
		return core.SnapshotCASResult{}, err
	}
	if len(candidate.CanonicalJSON) > s.opts.MaxSnapshotBytes {
		return core.SnapshotCASResult{Status: core.SnapshotCASRevisionConflict}, fmt.Errorf("%w: snapshot bytes=%d limit=%d", core.ErrSnapshotCapacity, len(candidate.CanonicalJSON), s.opts.MaxSnapshotBytes)
	}
	opCtx, cancel, err := s.context(ctx)
	if err != nil {
		return core.SnapshotCASResult{}, err
	}
	defer cancel()
	if err := opCtx.Err(); err != nil {
		return core.SnapshotCASResult{}, err
	}
	if err := s.keys.AssertSameSlot(s.keys.Snapshot()); err != nil {
		return core.SnapshotCASResult{}, fmt.Errorf("%w: %v", ErrScriptContract, err)
	}
	expectedFields := expected.EnvelopeFields()
	now := s.nowMs()
	reply, err := s.registry.run(opCtx, ScriptCompareAndSwapInitialAuthorityBatch, []string{s.keys.Snapshot().String()},
		s.keys.HashTag(), expectedFields.SchemaVersion, expectedFields.Realm, expectedFields.Revision,
		string(expectedFields.CanonicalJSON), expectedFields.ChecksumSHA256, expectedFields.TopologyFingerprint,
		expectedFields.CommittedAtMs, candidate.SchemaVersion, candidate.Realm, decimalUint64(candidate.Revision),
		string(candidate.CanonicalJSON), candidate.ChecksumSHA256, candidate.TopologyFingerprint,
		strconv.FormatInt(now, 10), strconv.Itoa(s.opts.MaxSnapshotBytes))
	if err != nil {
		return core.SnapshotCASResult{}, err
	}
	if isCrossSlotReply(reply) {
		return core.SnapshotCASResult{}, fmt.Errorf("%w: CROSS_SLOT", ErrScriptContract)
	}
	if len(reply) == 0 {
		return core.SnapshotCASResult{}, fmt.Errorf("%w: empty authority wave batch reply", ErrScriptContract)
	}
	switch reply[0] {
	case "COMMITTED":
		stored, err := decodeReplyStored(reply)
		if err != nil {
			return core.SnapshotCASResult{}, err
		}
		return core.SnapshotCASResult{Status: core.SnapshotCASCommitted, Stored: stored}, nil
	case "ALREADY_COMMITTED":
		stored, err := decodeReplyStored(reply)
		if err != nil {
			return core.SnapshotCASResult{}, err
		}
		return core.SnapshotCASResult{Status: core.SnapshotCASAlreadyCommitted, Stored: stored}, nil
	case "CURRENT_CHANGED":
		return core.SnapshotCASResult{Status: core.SnapshotCASStaleExpectation}, core.ErrSnapshotCASConflict
	case "REVISION_CONFLICT":
		return core.SnapshotCASResult{Status: core.SnapshotCASRevisionConflict}, nil
	case "BOOTSTRAP_REQUIRED":
		return core.SnapshotCASResult{Status: core.SnapshotCASRevisionConflict}, core.ErrSnapshotBootstrapRequired
	case "MALFORMED":
		return core.SnapshotCASResult{}, ErrMalformedRecord
	case "CAPACITY_EXCEEDED":
		return core.SnapshotCASResult{Status: core.SnapshotCASRevisionConflict}, fmt.Errorf("%w: %v", core.ErrSnapshotCapacity, ErrCapacityExceeded)
	default:
		return core.SnapshotCASResult{}, fmt.Errorf("%w: authority wave batch status %q", ErrScriptContract, reply[0])
	}
}
