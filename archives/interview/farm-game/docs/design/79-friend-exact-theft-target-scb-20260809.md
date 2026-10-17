# 79 — SCB: Friend exact Theft targets and mature-water exclusion (2026-08-09)

Status: APPROVED FOR HOTFIX IMPLEMENTATION

## Problem

`FarmOpportunityFactsV1` is authority data committed beside FarmRecord, but the
friend HTTP response currently exposes only an aggregate action mask. That has
two product defects:

1. exact evaluation can advertise WATER after `mature_at_ms`, while FarmActor
   correctly rejects mature WATER with `ERR_CROP_ALREADY_MATURE`;
2. H5 cannot distinguish eligible mature cycles from cycles already consumed by
   this viewer after reload. It can only learn one cycle from a local successful
   terminal, so single/batch controls can submit a known ineligible target.

The old "request submitted" message is admission feedback, not an outcome, and
must be replaced by an exact terminal message.

## Authority semantics

For one active cycle and trusted server time:

- THEFT eligible iff mature, remaining theft slots > 0, and viewer is absent
  from `theft_visitor_ids`;
- WATER eligible iff not mature and an unresolved WATER incident is due;
- a mature cycle never contributes WATER;
- UNKNOWN never becomes NONE and carries no eligible targets.

FarmActor remains final write authority. Read eligibility is display and target
selection data only; Pair/Permit/epoch and the Actor re-adjudicate every write.

## Append-only internal wire

Append to `logicrpc.FarmOpportunityReadResult`:

```proto
message FarmOpportunityTheftTarget {
  uint32 plot_id = 1;
  string crop_cycle_id = 2;
}

repeated FarmOpportunityTheftTarget eligible_theft_targets = 5;
```

Fields 1–4 are unchanged. The request is unchanged. Targets are bounded by the
existing 24-cycle FarmOpportunityFacts limit, unique and ordered by plot ID.
`FOUND` returns the exact viewer-eligible set; NOT_FOUND/UNKNOWN returns none.
The THEFT action bit must equal `len(eligible_theft_targets) > 0`.

The authenticated friend HTTP card appends:

```json
"theft_eligible_targets": [
  { "plot_id": 4, "crop_cycle_id": "..." }
]
```

This list exposes no other visitor IDs. It only exposes plot/cycle identities
already visible after legal Join. Public protobuf command envelopes are
unchanged.

## H5 contract

- parse the field strictly and reject duplicates, invalid plot IDs/cycle IDs,
  UNKNOWN-with-targets, or THEFT-bit/target cardinality mismatch;
- store targets beside action mask and opportunity revision;
- after friend SubscriptionReady, silently refresh the authority roster;
- select single/batch Theft targets only from the exact target set intersected
  with the current Snapshot; never infer eligibility from maturity alone;
- when READY has no eligible Theft targets, disable both controls and show
  `本农场当前没有可偷作物（可能本茬已偷过）` on attempted activation;
- a SUCCEEDED/NO_CHANGE terminal removes the exact target locally, refreshes
  Farm and Player authority, and says the server settled successfully;
- `ERR_STEAL_ALREADY_DONE` removes the exact target locally, refreshes the
  roster, and says explicitly that no duplicate reward was granted;
- every other REJECTED/UNAVAILABLE terminal replaces admission text with an
  honest failure/unavailable message;
- no inventory or Farm mutation is applied optimistically.

## Failure semantics

Missing/malformed/version-inconsistent facts remain UNKNOWN. A page-wide
profile/Pair/opportunity dependency failure still fails the page. No client
fallback derives WATER or THEFT from plot phase. Existing clients ignore field
5 and remain compatible; new clients fail closed if the HTTP detail is absent
or inconsistent.
