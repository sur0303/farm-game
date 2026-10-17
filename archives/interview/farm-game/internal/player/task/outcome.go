package task

// FactStatus classifies how one established business fact was consumed
// (Q34-05). Source idempotency (same participant replay) is short-
// circuited by the caller's receipts before these methods are invoked;
// these statuses describe one-time task satisfaction only.
type FactStatus int32

const (
	// FactUnspecified is never returned by a successful call.
	FactUnspecified FactStatus = iota
	// FactApplied means the fact changed task truth (first witness of a
	// daily task, a real cumulative increment, or a growth transition).
	FactApplied
	// FactNoChange means the one-time task was already satisfied in this
	// period (ALREADY_SATISFIED): a later witness from another friend,
	// care or theft is legal but changes nothing and is not an
	// IDEMPOTENCY_CONFLICT.
	FactNoChange
	// FactIgnoredExpired means the fact occurred in an already-expired
	// daily period. It is idempotently acknowledged so the dispatcher or
	// worker can stop retrying, and it never pollutes the new period.
	FactIgnoredExpired
)

func (s FactStatus) String() string {
	switch s {
	case FactApplied:
		return "APPLIED"
	case FactNoChange:
		return "NO_CHANGE"
	case FactIgnoredExpired:
		return "IGNORED_EXPIRED"
	default:
		return "UNSPECIFIED"
	}
}

// FactResult reports the outcome of consuming one authoritative fact.
type FactResult struct {
	Status     FactStatus
	InstanceID string // affected instance when a daily task transitioned
	// StateChanged tells the caller whether DomainState must be committed.
	// It is true when the lazy period switch fired even if the fact itself
	// was a NO_CHANGE fast path. NO_CHANGE and IGNORED_EXPIRED facts
	// without a period switch never set it.
	StateChanged bool
}

// ClaimStatus is the claim terminal classification the pure domain can
// decide from task truth (Q34-06). Same-operation replay, payload-hash
// conflicts (IDEMPOTENCY_CONFLICT) and StoredOperationOutcome persistence
// are decided by the PlayerActor's unified operation receipt layer before
// or around this call; the domain never sees an operation_id.
type ClaimStatus int32

const (
	ClaimUnspecified ClaimStatus = iota
	// ClaimSucceeded maps to SUCCEEDED/NONE.
	ClaimSucceeded
	// ClaimAlreadyClaimed maps to NO_CHANGE/TASK_ALREADY_CLAIMED: a
	// different operation re-claims an already-claimed instance; task
	// truth is unchanged and no second reward is granted.
	ClaimAlreadyClaimed
	// ClaimNotCompleted maps to REJECTED/TASK_NOT_COMPLETED.
	ClaimNotCompleted
	// ClaimInstanceExpired maps to REJECTED/TASK_INSTANCE_EXPIRED: a
	// daily instance from another period. RetryDirective is
	// REFRESH_AUTHORITY_STATE at the protocol layer.
	ClaimInstanceExpired
	// ClaimInstanceNotFound maps to REJECTED/TASK_INSTANCE_NOT_FOUND.
	ClaimInstanceNotFound
)

func (s ClaimStatus) String() string {
	switch s {
	case ClaimSucceeded:
		return "SUCCEEDED"
	case ClaimAlreadyClaimed:
		return "TASK_ALREADY_CLAIMED"
	case ClaimNotCompleted:
		return "TASK_NOT_COMPLETED"
	case ClaimInstanceExpired:
		return "TASK_INSTANCE_EXPIRED"
	case ClaimInstanceNotFound:
		return "TASK_INSTANCE_NOT_FOUND"
	default:
		return "UNSPECIFIED"
	}
}

// ClaimOutcome is the deterministic decision of one ClaimTask turn
// (Q34-06). It carries everything the PlayerActor needs to build the
// ClaimReceipt/StoredOperationOutcome and to apply assets in the same
// authority commit; the pure domain itself stores no receipt.
type ClaimOutcome struct {
	Status     ClaimStatus
	InstanceID string
	// Reward is the frozen bundle on ClaimSucceeded. The caller applies it
	// (coins/items/exp/pet unlock) exactly once, in the same authority
	// commit as the task state change; it is empty for every other status
	// so a re-claim can never double-grant.
	Reward RewardBundle
	// NextGrowth is set when a growth claim advanced the cursor and
	// activated the next chain entry; it may already be CLAIMABLE but is
	// never auto-claimed (Q34-08). Nil when the chain ended.
	NextGrowth *InstanceView
	Summary    Summary
	// StateChanged tells the caller whether DomainState must be committed.
	// It is true only when task truth moved (a successful claim, possibly
	// combined with a lazy period switch). Rejected and NO_CHANGE claims
	// leave task truth untouched and never bump task_state_version or the
	// red-point generation; their terminal state is persisted by the
	// caller's REQUIRED StoredOperationOutcome, not by model:task.
	StateChanged bool
}
