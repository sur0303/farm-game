# RT2Q S2 phase-polling and PAUSED semantics (2026-08-10)

## Harness issue

Sustained `routectl status` polling during `move-shard` contended with Plan CAS
and DrainValidated snapshot publication. One failure mode left the Plan at
`PENDING` / `attempt=0` (Execute returned before `bumpAttempt`). Another made
Stop-hold flaky under load.

## Harness fix

- Burst-poll only the short PENDING window before Stop is held.
- Resume gentler phase polling after Stop release.
- Skip terminal `PAUSED`/`FAILED` in `plan_phase` observations so oracle
  monotonicity is not poisoned.
- On non-SUCCEEDED move outcomes, capture holders/snapshot/verify diagnostics.

## Production PAUSED contract

`authority_committed_snapshot_pending` terminalizes the Plan as `PAUSED`.
Manager recovery may later publish the matching snapshot, but re-`Execute`
keeps the immutable PAUSED audit record and does not advance to `SUCCEEDED`.
S2 therefore requires a clean first-pass `SUCCEEDED` path; PAUSED is fail-closed
evidence, not a harness wait-for-SUCCEEDED loop.
