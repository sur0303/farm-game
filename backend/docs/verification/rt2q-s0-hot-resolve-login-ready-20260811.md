# RT2Q S0 Hot Resolve / Login Write-Path Readiness (2026-08-11)

## Verdict

Root cause of reproducible final-tip S0 `hot_resolve` INCONCLUSIVE was Gate
login HTTP 401 whose classified server cause is
`blobstore: writer epoch mismatch` during EnsureOwner—not permit-secret
handling, Redis URL leakage, or `/readyz` alone.

Fix head (SSH-signed): `0b65e0d7e79c270cc346aba7da23221a52bec064`.

Final tip re-check: three fresh isolated E3 PASS on
`f112087c9ced47f212194e8d357a823aad28be95`
(`/tmp/rt2q-final-s0-f112087c9ced-r{1,2,3}`). Earlier five consecutive PASS on
`0b65e0d` remain at `/tmp/rt2q-final-s0-0b65e0d7e79c-r{1..5}`.

## Proof

Failing runs (preserved):

- `/tmp/rt2q-s0-diag-6122171-a1` / `a3` with `gate-login-diag` copies
- Gate log sample class: `writer_epoch_mismatch`
- Consumers `/readyz` true and snapshot_installed present before failure

Passing barrier evidence (typed):

- `player_shard_fences_ready` (PLAYER shards 0/1 holder + snapshot endpoint match)
- `business_login_probe_attempt` may show initial `login_status_401` +
  `writer_epoch_mismatch`
- `business_login_ready` after probes succeed on both Gates × both shards
- then 100 `business_resolve` ok

## Fix surface (RT2Q-owned)

Harness only: wait for PLAYER shard fence binding and successful production
Gate login probes before the hot-resolve storm; challenge-scoped player start;
classify Gate login failure lines including writer epoch mismatch. No
correctness sleeps; poll on a real readiness condition.
