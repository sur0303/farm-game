# RT2Q Final Validation Candidate (2026-08-11)

> **Exact-review update:** the candidate described by this document remains
> rejected for freeze/integration by the P1 recorded in
> `rt2q-final-deadline-refresh-p1-handoff-20260811.md`. A replacement closure
> candidate is now implemented and documented in
> `rt2q-final-deadline-refresh-p1-closure-20260811.md`, but it is not a freeze
> point until its new exact-tip long-lived/S0–S6 evidence and independent Exact
> Review complete. The PASS matrix below remains historical pre-fix regression
> evidence, not production acceptance.

## Verdict

**Evidence tip (exact clean, SSH-signed, 21/21 E3 PASS, not pushed):**
`f112087c9ced47f212194e8d357a823aad28be95`

**Validation writeup tip (SSH-signed, docs-only):**
`7952302f4cff6d719633a284e6534913aea0e6c8`

**S0 tip re-check head (SSH-signed, docs-only after evidence tip):**
`147dbb628c89d3ed6dfb658488987eb7109bba5a`

Three fresh isolated E3 S0 PASS:
`/tmp/rt2q-final-s0-147dbb628c89-r{1,2,3}` (also probe1).
Preserved empty-launch artifacts (pre-created artifact-root hygiene):
`/tmp/rt2q-final-s0-147dbb628c89-r{1,2,3}-empty-launch-preserved`.

**Detached final-candidate tip:** branch tip after this docs pin commit
(docs-only after `147dbb6`; same harness/binary semantics as `f112087`).

Independent exact review / freeze / integration acceptance remain pending.
No checkpoint freeze and no push from this workstream.

## S0–S6 tip matrix (three fresh isolated E3 each)

All 21 runs: manifest PASS + oracle PASS on tip `f112087`
(docs-only tip after does not change binaries or harness semantics).
S0 additionally re-checked 3× on `147dbb6` (paths above).

| Scenario | Artifacts |
| --- | --- |
| S0 | `/tmp/rt2q-final-s0-f112087c9ced-r{1,2,3}` and `/tmp/rt2q-final-s0-147dbb628c89-r{1,2,3}` |
| S1 | `/tmp/rt2q-final-s1-f112087c9ced-r{1,2,3}` |
| S2 | `/tmp/rt2q-final-s2-f112087c9ced-r{1,2,3}` |
| S3 | `/tmp/rt2q-final-s3-f112087c9ced-r{1,2,3}` |
| S4 | `/tmp/rt2q-final-s4-f112087c9ced-r{1,2,3}` |
| S5 | `/tmp/rt2q-final-s5-f112087c9ced-r{1,2,3}` |
| S6 | `/tmp/rt2q-final-s6-f112087c9ced-r{1,2,3}` |

Summary log: `/tmp/rt2q-final-all-f112087c9ced-summary.txt`

## Pre-tip S6 proof head

S6 also has three PASS on code tip `d44f216075adb18e42e7f8ca7d4aa354ebc17042`
(`/tmp/rt2q-s6-e3-d44f216075ad-r{1,2,3}`); see
`docs/verification/rt2q-s6-fresh-join-reconcile-20260810.md`.

## Late blockers closed on this tip

### S0 Gate login HTTP 401 / `hot_resolve` INCONCLUSIVE

- **Not** permit-secret mismatch, stale Redis URL leakage, or listener-health alone.
- Typed Gate logs on failing runs classify as `writer_epoch_mismatch`
  (`blobstore: writer epoch mismatch` during EnsureOwner).
- Evidence: first login probe often 401 with that class; after PLAYER shard
  fence binding + successful login probes on both Gates/shards, the 100-resolve
  storm passes.
- Fix head: `0b65e0d7e79c270cc346aba7da23221a52bec064`
  (wait for shard fences + login write-path readiness; challenge-scoped player
  start; classify epoch-mismatch logs). Deterministic unit coverage in
  `cmd/rt2q-route-control-harness/main_test.go`.
- Detail: `docs/verification/rt2q-s0-hot-resolve-login-ready-20260811.md`

Preserved diagnostic / pre-fix failures (not overwritten):

- `/tmp/rt2q-final-s0-6122171-part1-a2` … `a8`
- `/tmp/rt2q-s0-diag-6122171-a1`, `a3` (gate-login-diag with epoch mismatch)
- `/tmp/rt2q-final-s0-6122171e90fb-r1-a2`, `r2`

### S2 `s2.plan_monotonic` FAIL when ACTIVE observed

- Dense `plan_phase` polls that include `ACTIVE` between `PREPARING` and
  `SUCCEEDED` are valid RoutePlan order; sparse polls that skip `ACTIVE` also
  remain valid.
- Fix head: `f112087c9ced47f212194e8d357a823aad28be95`
- Preserved failure: `/tmp/rt2q-final-s2-0b65e0d7e79c-r2`
- Detail: `docs/verification/rt2q-s2-plan-monotonic-active-20260811.md`

## Local gates on tip (this machine)

- `go test` routecontrol + harness + business-driver: PASS
- `go test -race` routecontrol + harness: PASS
- `go vet` owned packages: PASS
- `go build` harness / business-driver / routectl: PASS

## Constraints honored

- No integration merge/rebase/push
- No RT2Q push
- No checkpoint freeze pending independent exact review
