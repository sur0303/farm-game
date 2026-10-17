// Package compendium implements the pure domain layer of the P0 plant
// compendium and activity archive contract (docs/design/37
// v1.1-unlock-snapshot-seen-fence-capacity-freeze-candidate).
//
// The package is a deterministic, allocation-bounded state machine:
//
//   - PlayerCompendiumState is the single authoritative truth of permanent
//     unlock facts, compendium_revision and compendium_seen_revision. It is
//     treated as an immutable value; every transition returns a new
//     CandidateState and never mutates its input.
//   - Unlock eligibility is adjudicated only from the CompendiumUnlockSnapshot
//     frozen at Plant time and passed through HarvestDecision unchanged
//     (design 32 §3.3/§4.4). Nothing in this package re-reads hot config.
//   - The read path (BuildPageView) merges the authoritative state with a
//     versioned, immutable PlantCatalogSnapshot outside any Actor Turn. It
//     never mutates state and never deletes facts when catalog entries go
//     missing; it reports them as safe placeholders plus a catalog_missing
//     alert list (design 37 §4.2).
//
// The package does no persistence, no RPC, no Redis, no Actor wiring. Fees,
// receipts, StoredOperationOutcome embedding and the single PlayerActor
// Authority Commit are the integrator's job (design 37 §3.2, M3 in
// docs/design/72 §19).
package compendium
