// Package task implements the pure TaskDomain frozen by design contract
// 34 (34-task-system-contract.md, v1.3) within the physical boundary of
// contract 31 §11.7. It contains only deterministic state transitions
// over an explicit DomainState value: no Actor, Redis, Proto, MQ,
// Workflow or wall-clock access. Callers (a future PlayerActor turn)
// supply trusted_now_ms and already-established business facts.
//
// Contract anchors:
//   - Q34-01: TaskDomain lives inside PlayerActor; this package is that
//     domain module, not a TaskActor/task-svr.
//   - Q34-02: daily period derived from configured game timezone and
//     trusted_now_ms; lazy EnsureDailyPeriod on every access; no midnight
//     scan, no scheduler, no cross-day claim grace.
//   - Q34-03: progress only consumes authoritative facts (trusted turn
//     login, CONFIRM_FRIEND_VISIT commit, FRIEND care reward, accepted
//     theft attempt, ended CropCycle, committed level_after).
//   - Q34-05: one-time satisfaction is decided from task truth; repeated
//     facts fast-path NO_CHANGE without version churn; late
//     cross-midnight facts are IGNORED_EXPIRED, never pollute a new
//     period.
//   - Q34-06: claim is decided in one turn from task truth; a successful
//     claim returns the frozen RewardBundle plus the deterministic
//     decision the caller persists as ClaimReceipt/StoredOperationOutcome
//     in the same authority commit; a re-claim with a different operation
//     returns TASK_ALREADY_CLAIMED with an empty reward.
//   - Q34-07: daily instances bind period_id + config_version via
//     DailyTaskSetAssignment; growth instances bind definition_version at
//     activation; unknown period assignment fails closed.
//   - Q34-08: exactly 3 daily tasks + 1 current growth task; linear
//     growth_cursor; claim advances and immediately evaluates the next
//     task but never auto-claims; task_state_version bumps once per
//     mutation batch and claimable_count is the red-point truth.
//   - 31 §11.7: model:task stores ONLY bounded task truth (period,
//     instances, cursor, cumulative indicators, version, red-point
//     count). Operation idempotency (receipt:op:<operation_digest>,
//     payload hash, StoredOperationOutcome) and Harvest/Care/Theft
//     Participant Receipts are enforced by the PlayerActor/Authority
//     Commit layer BEFORE calling this package and are committed
//     atomically with any task change. This package keeps no
//     operation_id, payload hash or source receipt history.
package task
