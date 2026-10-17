# RT2Q S2 Plan Monotonic ACTIVE Intermediate (2026-08-11)

## Verdict

`s2.plan_monotonic` FAIL on dense polls that observe `ACTIVE` between
`PREPARING` and `SUCCEEDED` was an oracle over-strictness, not a RoutePlan
ordering defect.

Fix head (SSH-signed): `f112087c9ced47f212194e8d357a823aad28be95`.

## Proof

Preserved failure: `/tmp/rt2q-final-s2-0b65e0d7e79c-r2` — typed phase trail
includes `ACTIVE` between required milestones; sparse polls that skip
`ACTIVE` still PASS.

Tip re-check: three fresh isolated E3 PASS on `f112087`
(`/tmp/rt2q-final-s2-f112087c9ced-r{1,2,3}`).

## Fix surface (RT2Q-owned)

`phaseClause` allows known intermediate ranks (`ACTIVE`) between required
milestones. Unit: `TestPhaseClauseAllowsActiveBetweenPreparingAndSucceeded`.
