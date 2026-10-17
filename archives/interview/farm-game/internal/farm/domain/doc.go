// Package domain contains the pure, deterministic Farm domain rules frozen by
// design contracts 32 (core business scenarios) and 36 (simple pet guard).
//
// Every exported decision function is a pure Transition:
//
//	(state, command, trustedNowMs, frozen rules) -> (CandidateState, Decision, *Error)
//
// The input FarmState is never mutated; a successful decision returns a cloned
// CandidateState that the caller (FarmActor) may install only after its
// authority commit succeeds (design 27: BuildCandidateState must not overwrite
// authoritative memory before persist). This package does not know about
// actors, Redis, RPC, workflows, receipts or process entry points:
//
//   - Player assets (coin, seeds, inventory, level) are PlayerActor authority.
//     This package only freezes the prices/levels/rewards the cross-actor
//     workflow must honor, and exposes pure eligibility checks.
//   - Idempotency receipts, operation replay and Unknown resolution are
//     authority-store concerns; business-level de-duplication that is part of
//     the domain itself (per-visitor-per-cycle theft attempt, first-success
//     care resolution) is modeled here.
//   - Randomness is an input: the caller captures one random draw per Farm
//     decision turn and passes it in, keeping every function deterministic
//     and replayable.
//
// Maturity and guard expiry are time-derived from a single trustedNowMs
// captured per turn; no MATURE/EXPIRED state is ever persisted.
package domain
