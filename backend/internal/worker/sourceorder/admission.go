package sourceorder

import (
	"context"
	"errors"
	"fmt"

	"farmgame/internal/contracts"
	authority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
)

// Admission errors distinguish invalid persisted evidence from a dependency
// outage without exposing Redis details. Callers must keep the Redis SourceOrder
// lookup as the authority until Commit Proof integration also proves the source
// holder's actor-generation identity.
var (
	ErrSourceOrderAdmissionFence       = errors.New("sourceorder: admission fence invalid")
	ErrSourceOrderAdmissionUnavailable = errors.New("sourceorder: admission evidence unavailable")
)

// AdmissionFenceReader is the bounded Redis evidence needed at destination
// admission. Implementations use the same Redis TIME and DueIndex as WorkLease
// claim/renew; process-local time and a caller-supplied due value are not proof.
type AdmissionFenceReader interface {
	TrustedNowMs(context.Context) (int64, error)
	DueIndexScore(context.Context, rediskey.AuthorityBucketRef, string, string) (int64, bool, error)
}

// VerifyRedisAdmissionFence validates the SourceOrder mode shared by the
// HARVEST/PLANT/UNLOCK fallback paths. It deliberately does not validate typed
// business identity; each source verifier must first bind the exact payload,
// source actor, physical bucket, layout, work type and participant step.
func VerifyRedisAdmissionFence(ctx context.Context, reader AdmissionFenceReader, bucket rediskey.AuthorityBucketRef, workType string, order authority.SourceOrderSnapshot, call *contracts.ParticipantCall) error {
	if reader == nil || call == nil || call.Work == nil || workType == "" || order.WorkID == "" {
		return fmt.Errorf("%w: incomplete evidence", ErrSourceOrderAdmissionFence)
	}
	if order.WorkVersion == 0 || call.ExpectedWorkVersion == 0 || order.WorkVersion != call.ExpectedWorkVersion {
		return fmt.Errorf("%w: work version", ErrSourceOrderAdmissionFence)
	}
	nowMs, err := reader.TrustedNowMs(ctx)
	if err != nil {
		return fmt.Errorf("%w: trusted time: %v", ErrSourceOrderAdmissionUnavailable, err)
	}
	dueScore, dueBound, err := reader.DueIndexScore(ctx, bucket, workType, order.WorkID)
	if err != nil {
		return fmt.Errorf("%w: DueIndex: %v", ErrSourceOrderAdmissionUnavailable, err)
	}
	if !dueBound || dueScore <= 0 || dueScore != order.NextDueAtMs {
		return fmt.Errorf("%w: DueIndex binding", ErrSourceOrderAdmissionFence)
	}
	ev := &authority.SourceOrderAdmissionEvidence{
		Found: true, State: order.State, WorkVersion: order.WorkVersion,
		OperationID: order.OperationID, NextDueAtMs: order.NextDueAtMs,
		LeaseHolderID: order.LeaseHolderID, LeaseHolderBootID: order.LeaseHolderBootID,
		LeaseEpoch: order.LeaseEpoch, LeaseTokenHash: order.LeaseTokenHash,
		LeaseExpireAtMs: order.LeaseExpireAtMs, DueScoreMs: dueScore, TrustedNowMs: nowMs,
	}
	return VerifyAdmissionEvidence(ev, call)
}

// VerifyAdmissionEvidence applies the exact same direct/worker admission
// fencing without any further Redis I/O. It is safe only when the evidence was
// produced atomically with the authoritative Redis view.
func VerifyAdmissionEvidence(ev *authority.SourceOrderAdmissionEvidence, call *contracts.ParticipantCall) error {
	if ev == nil || !ev.Found || call == nil || call.Work == nil {
		return fmt.Errorf("%w: incomplete evidence", ErrSourceOrderAdmissionFence)
	}
	if ev.WorkVersion == 0 || call.ExpectedWorkVersion == 0 || ev.WorkVersion != call.ExpectedWorkVersion {
		return fmt.Errorf("%w: work version", ErrSourceOrderAdmissionFence)
	}
	if ev.DueScoreMs <= 0 || ev.DueScoreMs != ev.NextDueAtMs {
		return fmt.Errorf("%w: DueIndex binding", ErrSourceOrderAdmissionFence)
	}
	switch call.CallerKind {
	case contracts.ParticipantCallerCoordinator:
		if ev.State != "PENDING" || ev.NextDueAtMs > ev.TrustedNowMs || call.OperationID == "" || call.OperationID != ev.OperationID ||
			ev.LeaseHolderID != "" || ev.LeaseHolderBootID != "" || ev.LeaseEpoch != 0 ||
			ev.LeaseTokenHash != "" || ev.LeaseExpireAtMs != 0 {
			return fmt.Errorf("%w: direct state", ErrSourceOrderAdmissionFence)
		}
	case contracts.ParticipantCallerWorkerRecovery:
		if ev.State != "REWARD_DISPATCHING" ||
			ev.LeaseHolderID != call.CallerInstanceID || ev.LeaseHolderBootID != call.CallerBootID ||
			ev.LeaseEpoch == 0 || ev.LeaseEpoch != call.LeaseEpoch ||
			ev.LeaseTokenHash != authority.HashLeaseToken(call.LeaseOwnerToken) ||
			ev.LeaseExpireAtMs <= ev.TrustedNowMs || ev.NextDueAtMs != ev.LeaseExpireAtMs || ev.DueScoreMs != ev.LeaseExpireAtMs {
			return fmt.Errorf("%w: Worker lease", ErrSourceOrderAdmissionFence)
		}
	default:
		return fmt.Errorf("%w: caller kind", ErrSourceOrderAdmissionFence)
	}
	return nil
}
