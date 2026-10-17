# CR1 — CropRuleSnapshot handoff

`farm-svr` obtains the Plant rule snapshot at the trusted Plant first hop. It
does not accept seed IDs, timing, yield, care, theft, reward, or compendium
values from the public request.

## Handoff sequence

1. The Plant coordinator resolves the requested `crop_definition_id` with
   `cropcatalog.ByDefinitionID`. It compares the request's expected crop-rule
   version with the current versioned definition before creating a workflow.
   An unknown definition, stale version, or invalid definition is rejected at
   this boundary.
2. The coordinator calls
   `cropcatalog.BuildCropRuleSnapshot(definition)`. The builder maps the
   server-owned seed and harvest item IDs, copies every value needed by an
   in-flight `CropCycle`, and emits the frozen generated
   `contracts.CropRuleSnapshot`.
3. The coordinator obtains the canonical snapshot digest with
   `cropcatalog.CropRuleSnapshotHash`. This uses the shared deterministic
   contract encoding, so the hash covers timing, yield, rewards, compendium
   provenance, theft slots, and care schedules.
4. Only after the snapshot and hash succeed does `farm-svr` create the
   `Plant` workflow root. The root stores the snapshot and hash; subsequent
   `ReserveSeed`, `CommitPlant`, retries, and recovery use those stored bytes.
   The P0 workflow uses the mapped seed once (`quantity = 1`). No participant
   effect occurs for a definition that failed snapshot construction.

The current catalog keeps the old demo projection fields used by existing
demo farm views. Its explicit versioned Plant fields provide P0 integer base
yields, rule versions, coin/experience values, and fixed WATER and
REMOVE_PEST offsets. P0 theft is always ten 200-bp slots with a 2000-bp total
cap. The builder rejects non-exact quantities, overflow, zero versions, bad
IDs, invalid durations, offsets that are not strictly inside the grow
duration, and ActivityID values that are not valid UTF-8. It also rejects a
CropUnitCoinValue above the checked P0 guard-compensation bound: the maximum
uint64 value divided by the largest legal 2-percent slot quantity.

Both care opportunities are represented as ordinal-one `FIXED_OFFSET`
incidents. The compendium rule freezes the catalog and definition versions,
activity ID, and eligibility at Plant. `legacy_seed_plantable` remains part of
that snapshot, so an activity crop already represented by a legacy seed stays
plantable after the activity is archived; a later successful owner harvest is
still the first and only event that unlocks the compendium entry.

The returned protobuf graph is newly allocated for every build. `farm-svr`
must treat it as immutable after handing it to the workflow and must never
re-read the catalog for an existing in-flight work record.
