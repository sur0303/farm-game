# R1-T-AUTHCOMP-PANIC-TRIAGE handoff

## Result

Classification: `TEST_TIMING_CONFIRMED`.

This is not an R1-4-candidate-only regression. Baseline `dd7104a3c43cecdc85c8d7962014f9760bf28f2d` reproduced the exact assertion once in C1 (100 non-race target iterations) and once in C2 (50 race target iterations):

```text
fatal flag must be set after an entry panic
```

No race-detector report appeared. The committed candidate and baseline versions of `lease_scheduler.go` and `lease_scheduler_test.go` are identical.

## Mechanical happens-before comparison

The original helper waited only for `EntryCount()==0`. Production order is:

1. A defer in `entry.run` recovers the renewal panic.
2. `handleEntryPanic` stops and deletes the entry while holding `s.mu`, then unlocks.
3. It fences the matching guard after unlocking.
4. `notifyFatal` then CASes `fatal=true`, stores `fatalErr`, and synchronously calls `safeNotifyFatal`.
5. `safeNotifyFatal` contains the callback panic with defer/recover.

Acquiring `s.mu` in `EntryCount` therefore orders the observed deletion, but it does not order the handler's later fence and fatal stores. The helper could return at that intermediate state and immediately observe `Fatal()==false`. Baseline C1/C2 reproduced the exact failure; candidate B1 also reproduced the same faulty assumption in `TestLeaseSchedulerEntryPanicLeavesNoActiveEntry`.

The narrow test-only fix gives both affected tests a per-test `OnFatal` channel. The callback send occurs after entry deletion, guard fencing, the fatal CAS, and the `fatalErr` store. The matching receive is the explicit happens-before barrier before assertions. No sleep was added and no timeout was increased.

## Runs

| ID | worktree / commit | Result | Seed | Key evidence |
|---|---|---:|---|---|
| candidate A1 | candidate / `1ec3dcd…` | PASS | — | target 100/100 |
| candidate A2 | candidate / `1ec3dcd…` | PASS | — | race target 50/50; no race report |
| candidate B1 | candidate / `1ec3dcd…` | FAIL | `1786276506503064082` | sandbox listener denial; same timing failure in sibling test |
| candidate B2 | candidate / `1ec3dcd…` | FAIL | `1786276601662227301` | sandbox listener denial; no race report |
| baseline C1 | primary / `dd7104a…` | FAIL | — | target 99 PASS / 1 exact FAIL |
| baseline C2 | primary / `dd7104a…` | FAIL | — | race target 49 PASS / 1 exact FAIL; no race report |
| baseline C3 | primary / `dd7104a…` | FAIL | `1786276738734200727` | sandbox listener denial |
| baseline C4 | primary / `dd7104a…` | FAIL | `1786276811536054645` | sandbox listener denial; no race report |
| fixed V1 | primary + test fix | PASS | — | target and sibling each 100/100 |
| fixed V2 | primary + test fix | PASS | — | race target and sibling each 50/50; no race report |

The B/C package/shuffle failures remain FAIL. This sandbox denies `miniredis` listeners on `127.0.0.1:0`; the raw logs retain 840/420 `socket: operation not permitted` failures respectively. They are not represented as passing runs.

D was not run because its condition was not met: the exact target reproduced on baseline in both C1 and C2, and the timing cause was mechanically confirmed.

## Audit

- `OnFatal` is scheduler-instance state, not a package global; no mutable package global participates in this path.
- `notifyFatal -> safeNotifyFatal` invokes the callback synchronously in the entry goroutine; it does not launch an asynchronous callback goroutine.
- The callback runs outside scheduler/guard locks and `safeNotifyFatal` recovers its panic.
- The target does not modify `postRenewHook` or `lostGenHook`, so there is no hook-restore leak.
- `Cancel` stops and removes the exact generation under the scheduler mutex; the panic handler performs equivalent local cleanup for its own entry.
- `timeoutSchedulerFixture` registers `t.Cleanup(scheduler.Close)`. `Close` cancels the context, stops remaining entries, and waits on the worker WaitGroup, including cleanup of the second shard installed later by the target test.
- The old 1 ms poll/2 s deadline was not an event barrier for fatal state. The new channel receive is the barrier; no sleep or timeout was widened.
- The injected callback panics immediately and is recovered. Both fixed non-race and race repeats completed, with no evidence of a goroutine leak on this path.

## Controller action

The candidate worktree stayed read-only and clean. After the single triage commit is created on this branch, the Controller must cherry-pick it separately into the R1-4 candidate. This task did not push, merge, or tag.

Raw stdout/stderr and per-run metadata (exit code, UTC/Asia-Shanghai timestamps, commit, isolated `GOCACHE/GOTMPDIR`, and shuffle seed) are under `raw/`. See `triage.json` for the machine-readable result and `SHA256SUMS` for integrity hashes.

## Controller completion-barrier review

The Controller retained the `fatalCh` happens-before edge and additionally waits until `FatalErr` contains the exact `OnFatal callback panicked` diagnostic. This proves the outer `safeNotifyFatal` recovery and diagnostic publication completed, rather than merely proving callback entry. All three homologous tests passed 100/100 normally and 50/50 under race; raw outputs are `raw/controller-fixed-V3*` and `raw/controller-fixed-V4*`. The three consecutive full-suite gate is deferred until this test-only commit is cherry-picked onto the exact R1-4 merge-queue candidate.
