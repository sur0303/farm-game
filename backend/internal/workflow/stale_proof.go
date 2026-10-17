package workflow

import (
	"errors"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ClassifyStaleWorkProof is the single shared classifier for a participant
// WorkVersion proof mismatch (WF-WORKVERSION-PROOF-RACE). It wraps only the
// exact verifier failure — gRPC FailedPrecondition carrying the
// "participant proof: WorkVersion" prefix (participantproof.ErrWorkVersionMismatch)
// — as ErrStaleWorkProof, so the engine reloads the authoritative WorkRecord
// and re-evaluates instead of classifying it as INVOCATION_UNKNOWN or leaking
// an internal CAS/proof race as a public business REJECTED.
//
// Every other error (different gRPC code, or a different FailedPrecondition
// message such as idempotency conflict, identity mismatch, unregistered step
// or verifier-not-configured) is returned unchanged so the generic
// error/INVOCATION_UNKNOWN path owns it and stale proof is never
// misclassified from an unrelated precondition.
func ClassifyStaleWorkProof(err error) error {
	if err == nil {
		return nil
	}
	if status.Code(err) == codes.FailedPrecondition && strings.Contains(status.Convert(err).Message(), "participant proof: WorkVersion") {
		return errors.Join(ErrStaleWorkProof, err)
	}
	return err
}
