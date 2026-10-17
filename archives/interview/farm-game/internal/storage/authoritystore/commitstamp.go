package authoritystore

import commitstamp "farmgame/internal/storage/internal/commitstamp"

// CommitStamp is a sealed witness emitted only for a definite, first
// COMMITTED Authority mutation carrying AttachedOrders. It binds the exact
// Commit request, resulting record version, and authority clock observed at
// the linearization point. ALREADY_COMMITTED, orderless, and indeterminate
// results never carry a stamp.
//
// The alias exposes only commitstamp.Stamp's package-private sealed interface.
// Ordinary callers cannot implement, construct, or finalize a non-nil stamp.
type CommitStamp = commitstamp.Stamp

// ExplicitCommitOutput is the read-only validated view of a CommitStamp.
type ExplicitCommitOutput struct {
	recordVersion uint64
	committedAtMs int64
}

func (o ExplicitCommitOutput) RecordVersion() uint64 { return o.recordVersion }
func (o ExplicitCommitOutput) CommittedAtMs() int64  { return o.committedAtMs }

// ExactCommitEvidence is an immutable process-local carrier for the exact
// request and sealed first-COMMITTED result. It is deliberately not a wire or
// persistence type. Source actors may hand it to their in-process Direct
// dispatcher, which must reopen the stamp before minting any route-fenced
// proof. Its private fields prevent callers from manufacturing evidence.
type ExactCommitEvidence struct {
	request AuthorityCommitRequest
	result  CommitResult
}

// NewExactCommitEvidence captures an exact first-COMMITTED output. The request
// and result are deep-copied so later mutation of actor-owned candidate slices
// cannot retarget the evidence. ALREADY_COMMITTED and timeout-unknown results
// never produce evidence.
func NewExactCommitEvidence(request AuthorityCommitRequest, result CommitResult) (ExactCommitEvidence, bool) {
	if _, ok := result.ExplicitCommit(request); !ok {
		return ExactCommitEvidence{}, false
	}
	return ExactCommitEvidence{request: cloneCommitRequest(request), result: cloneCommitResult(result)}, true
}

// Open returns defensive copies only after rechecking the sealed stamp. This
// second check makes a zero, copied, or internally corrupted carrier fail
// closed at the proof-issuance boundary.
func (e ExactCommitEvidence) Open() (AuthorityCommitRequest, CommitResult, bool) {
	if _, ok := e.result.ExplicitCommit(e.request); !ok {
		return AuthorityCommitRequest{}, CommitResult{}, false
	}
	return cloneCommitRequest(e.request), cloneCommitResult(e.result), true
}

func cloneCommitResult(in CommitResult) CommitResult {
	in.Snapshot.ModelFields = cloneModelState(in.Snapshot.ModelFields)
	in.Outcome.Payload = append([]byte(nil), in.Outcome.Payload...)
	return in
}

// ExplicitCommit returns a stamp view only when this result is an exact first
// COMMITTED output for request. A replay (ALREADY_COMMITTED), a caller-built
// result, a stamp copied from another request, or a mismatched snapshot fails
// closed.
func (r CommitResult) ExplicitCommit(request AuthorityCommitRequest) (ExplicitCommitOutput, bool) {
	recordVersion, committedAtMs, issued := commitstamp.VerifyExact(r.CommitStamp, request)
	if r.Status != CommitCommitted || !issued ||
		request.ExpectedRecordVersion == 0 || request.ExpectedRecordVersion == ^uint64(0) ||
		recordVersion != request.ExpectedRecordVersion+1 ||
		r.Snapshot.RecordVersion != recordVersion || r.Snapshot.ActorAddress != request.ActorAddress {
		return ExplicitCommitOutput{}, false
	}
	return ExplicitCommitOutput{recordVersion: recordVersion, committedAtMs: committedAtMs}, true
}
