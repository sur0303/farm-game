# RT2Q-0 Route Control Harness Handoff

- `task_id`: `RT2Q0-ROUTE-CONTROL-HARNESS`
- `base_commit`: `29ed7be2f7ddc6df2e837830c165ad50ca62e28f`
- `parent_head`: `763107675ea16a2baa90707a03b683a8a6cd8f30`
- `branch`: `codex/rt2q-route-control-harness-20260808`
- `worktree`: `/data/home/developer_026/CodeBuddy/20260519141303/farm-game-rt2q-route-control-harness-20260808`
- `status`: `COMPLETED`
- `role`: `RT2Q-0` sole main Writer
- `ICR`: `I0 READY`
- `owned_paths`:
  - `cmd/rt2q-route-control-harness/**`
  - `internal/testkit/routecontrol/**`
  - `docs/verification/rt2q-route-control-harness-handoff.md`
  - `docs/verification/route-control-evidence-schema-20260808.md`

## Scope Lock

This task implements test and evidence scaffolding only. Production runtime, store,
proto, composition, A/B files, and existing smoke/e2e mainlines are out of scope.

## Execution Record

Implementation and verification commands will be recorded here before handoff is
closed. The final record will include known limitations and require Q Final to use
a new worktree.

## Delivered Scaffold

Implemented only within the owned paths:

- `internal/testkit/routecontrol/**`
  - E1/E2/E3/E4 provenance and anti-escalation checks.
  - Required manifest validation and exact artifact SHA-256 verification.
  - Independent revision, authority, holder, and fault NDJSON timelines.
  - Fault phases are observation-only; no business verdict is derived.
  - Exact-key registry with caller-owned cleanup interface, per-key delete then
    existence verification, digest-only registry state, and no broad cleanup API.
  - Redaction, generic bounded test-process shutdown, and future oracle metadata
    registration.
- `cmd/rt2q-route-control-harness/**`
  - Default E1 self-check only.
  - E2 is `INCONCLUSIVE` without an external scenario runner.
  - E3/E4 require a real verified attestation and remain `INCONCLUSIVE` until a
    future route-control oracle is implemented.
- `docs/verification/route-control-evidence-schema-20260808.md`
  - JSON/NDJSON shape, gates, cleanup discipline, CLI contract, and limitations.

## Luna Review Closure

Independent Luna Review of `ff1d3b0` reported `P0=0`, `P1=6`, `P2=2`.
The fix scope remains owned-only:

- Artifact hashing now rejects symlink components, resolved paths outside the
  artifact root, missing paths, directories, and all non-regular files.
- Timeline runs use a newly created private directory with exclusive files;
  existing directories/files/symlinks are rejected, and concurrent creators of
  one run directory have one winner without append or duplicate sequences.
- E3/E4 `CanPass` is permanently false in Q0. Caller-supplied attestation
  booleans are untrusted; no trusted verifier capability exists yet.
- Manifest provenance now includes and binds `HeadCommit`; top-level
  TaskID/RunID/BaseCommit/HeadCommit/Branch/Worktree must equal nested values.
  Self-check uses explicit `synthetic-e1`, never a zero SHA, and synthetic
  provenance cannot escalate.
- Redaction covers Authorization Bearer, password/token/auth/API-key values,
  URL credentials, endpoint hosts, timeline provenance, observations, manifest
  reason, and manifest provenance.
- Process shutdown uses a dedicated Unix process group, kills the full group
  on timeout, and rejects arbitrary `io.Writer` values in favor of `nil` or
  `*os.File`.
- `Reason` is required. Manifest read/write APIs require an artifact root and
  re-hash every artifact; wrong or missing SHA/files are rejected.

## Closure Review Hardening

The follow-up closure review is implemented in `f66186c`
(`fix(rt2q): close route-control closure review`) and remains within the
owned paths:

- Linux filesystem access is descriptor-relative: `openat2` uses
  `RESOLVE_BENEATH`, `RESOLVE_NO_SYMLINKS`, `RESOLVE_NO_MAGICLINKS`, and
  `RESOLVE_NO_XDEV`;
  artifact and manifest reads `fstat` the opened descriptor and hash that same
  descriptor. FIFO reads are non-blocking and rejected after the regular-file
  check.
- Timeline run directories and files are created through directory
  descriptors. Manifest output uses a private exclusive temporary file and
  descriptor-relative `linkat` installation, so rename/symlink swaps cannot
  redirect output outside the opened root.
- Redaction is idempotent and covers JSON-quoted password/token/API-key
  values, `X-API-Key` headers, URL credentials, schemeless endpoints with or
  without ports, timeline fields, manifest fields, and sensitive query values
  in URL queries/fragments while retaining non-sensitive fragment observations.
  Artifact `Name`/`Path` are binding fields; writes reject metadata that a
  redactor would change, and `Manifest.Redacted` deep-copies caller artifacts.
- Shutdown cleans the process group even when the leader exits immediately
  after interrupt. Linux holds a leader pidfd, validates original
  group/session/start-time identity, and signals each member through pidfds;
  PID reuse fails closed. Non-Linux process-group capability is fail-closed.
  Typed-nil `*os.File` writers fail fast.
- Unix process-tree tests are isolated in a Unix build-tag test helper;
  Windows and Plan 9 test compilation no longer sees Unix syscall references.
- Seven-character odd-length hexadecimal commit prefixes are accepted; zero
  hashes and synthetic markers cannot represent real provenance. Real
  provenance resolves Git through an absolute `LookPath` result and bounded
  `CommandContext` with ambient Git repository/object/config variables removed,
  binds `head_commit` to the current Git HEAD, and verifies the base commit
  object. Detached branch exit status is distinguished from other failures.
  Synthetic E1 remains explicit and separate.

## Final Review Continuation

The continuation from `080b96d` closes the final review's six P1 and three P2
findings without changing the scaffold-only scope:

- Strong durable filesystem evidence is Linux-only. Windows, Plan 9, js, and
  WASI explicitly fail closed with `ErrCapability`; no portable name-based
  rollback implementation is retained.
- `Timeline.FinalizeManifest` keeps the run-root and original stream file
  descriptors held through sync, seek/hash, manifest installation, and cleanup.
  It is one-shot, owns the artifact list, and the CLI uses it.
- Manifest redaction deep-copies artifacts and rejects sensitive bound
  `Name`/`Path` metadata before hashing. Redaction covers quoted/unquoted
  Authorization Bearer forms and sensitive URL query/fragment pairs.
- Linux process cleanup holds pidfd identity and uses per-member pidfd
  signaling; PID reuse is a fail-closed error. Other platforms without an
  equivalent capability reject process start.
- Git commands have an absolute `LookPath` executable, bounded context,
  sanitized environment, and detached-versus-other exit handling.
- Linux `openat2` adds `RESOLVE_NO_XDEV`; portable entropy failure returns an
  error; exact-key cleanup has atomic closing/closed state and retry semantics.

## RT2Q-0 Closure 4P1+4P2 Continuation

The continuation after `9b810cd` closes the requested closure findings and the
I0 raw-pidfd finding within the same owned paths:

- Linux transfers the raw `SysProcAttr.PidFD` into the held leader capability.
  Start failure and `afterStart` failure signal through that raw identity before
  closing it, then perform bounded original-group cleanup only after full
  identity validation before reaping and returning.
- Process natural exit now runs a bounded group finalizer from the wait
  goroutine. `Wait` and `Shutdown` share idempotent signal/finalize state, do
  not deadlock when concurrent, and close the group capability after cleanup.
  Member `/proc` stat errors other than `ENOENT` remain fail-closed; only the
  explicitly bound pidfd-confirmation exit race is retried by cleanup.
  Leader-first and grandchild paths are covered.
- Timeline stream write, sync, seek, and hash faults permanently poison the
  run. Later records reject with `ErrTimelinePoisoned`; finalization never
  installs a manifest after poison and still closes every stream/root fd.
  Partial stream writes and partial manifest temp writes are rejected.
- Strong filesystem capability code is Linux-only. Windows, other Unix
  targets, Plan 9, js, and WASI return `ErrCapability`; process capability
  tests and filesystem capability tests are split by build tag, with explicit
  unsupported-platform tests.
- Linux rollback removes only child files through the held directory fd and
  deliberately retains the top-level empty directory because Linux has no
  atomic fd-bound `rmdir` primitive. Windows has no rollback implementation and
  fails closed, so there is no `Stat`-then-`Remove` name window on that target.

## RT2Q-0 Closure 3P1+1P2 Continuation

This continuation starts from parent `1e75c63` (`29ed7be` base) and keeps the
strong identity guarantees Linux-only:

- Windows no longer has an `os.Root` or name-based rollback implementation.
  Filesystem evidence and rollback fail closed with `ErrCapability`; Windows
  tests assert that sentinel, while Windows and Plan 9 remain cross-compile
  checked.
- Linux transfers the original raw `SysProcAttr.PidFD` into the process-group
  capability. `afterStart` failure signals the leader through that held fd
  before closing it, never closes the fd and reopens a numeric PID, and
  bounded `cmd.Wait` errors/timeouts are joined into the returned start error.
- `/proc` scan errors other than the kernel's process-gone `ENOENT`/`ESRCH`
  form fail closed and never trigger a negative-PGID fallback. Group/member
  signaling is only done through held pidfd identity after complete validation.
- Every owned member pidfd close error is aggregated and returned from
  `membersLocked` cleanup, signal traversal, and wait traversal. Process group
  close state is serialized across `Wait`/`Shutdown`; a close fault is retained
  as an error instead of becoming a later success.
- Added seams cover raw-fd signal-before-close ordering, `/proc` read and
  directory faults, stuck start-failure wait, and member close faults.

## Closure 3P1+1P2 Verification

| Command | Result |
| --- | --- |
| `gofmt -l cmd/rt2q-route-control-harness internal/testkit/routecontrol` | PASS |
| `git diff --check` | PASS |
| `GOCACHE=/tmp/rt2q0-closure3-count10-cache go test -buildvcs=false -count=10 ./internal/testkit/routecontrol ./cmd/rt2q-route-control-harness` | PASS |
| `GOCACHE=/tmp/rt2q0-closure3-race-cache go test -buildvcs=false -race -count=1 -timeout=180s ./internal/testkit/routecontrol ./cmd/rt2q-route-control-harness` | PASS |
| `GOCACHE=/tmp/rt2q0-closure3-vet-cache go vet ./...` | PASS |
| `GOCACHE=/tmp/rt2q0-closure3-build-cache go build -buildvcs=false ./cmd/...` | PASS |
| `GOOS=windows GOARCH=amd64 go test -buildvcs=false -c ./internal/testkit/routecontrol` and CLI package | PASS |
| `GOOS=plan9 GOARCH=amd64 go test -buildvcs=false -c ./internal/testkit/routecontrol` and CLI package | PASS |
| `/tmp/rt2q0-route-control-harness-closure3 --level E1` | PASS, exit `0`, `E1 self-check PASS` |
| `/tmp/rt2q0-route-control-harness-closure3 --level E3` | PASS, expected exit `2`, `E3 INCONCLUSIVE: real attestation is required` |
| `GOCACHE=/tmp/rt2q0-closure3-full-test-cache go test -buildvcs=false -count=1 -timeout=600s ./...` | ENVIRONMENT-BLOCKED: managed sandbox rejects loopback listeners used by existing diagnostics, httptest, and miniredis tests; route-control and CLI packages pass |

## Verification Record

Commands are run from this worktree. `GOCACHE=/tmp/rt2q0-route-control-gocache`
is used because the default user Go build cache is read-only in this environment.

| Command | Result |
| --- | --- |
| `gofmt -w cmd/rt2q-route-control-harness/*.go internal/testkit/routecontrol/*.go` | PASS |
| `GOCACHE=/tmp/rt2q0-route-control-gocache go test -count=1 ./internal/testkit/routecontrol/... ./cmd/rt2q-route-control-harness` | PASS |
| `GOCACHE=/tmp/rt2q0-route-control-race-cache go test -race -count=1 -timeout=120s ./internal/testkit/routecontrol/... ./cmd/rt2q-route-control-harness` | PASS |
| `GOCACHE=/tmp/rt2q0-route-control-vet-cache go vet ./internal/testkit/routecontrol/... ./cmd/rt2q-route-control-harness` | PASS |
| `GOCACHE=/tmp/rt2q0-route-control-build-cache go build -buildvcs=false ./cmd/rt2q-route-control-harness` | PASS |
| `GOCACHE=/tmp/rt2q0-route-control-cli-cache go run -buildvcs=false ./cmd/rt2q-route-control-harness --level E1` | PASS: `E1 self-check PASS` |
| `/tmp/rt2q0-route-control-harness --level E3` | PASS: raw process exit `2`, `E3 INCONCLUSIVE` |
| `git diff --check` | PASS |

Full repository checks:

| Command | Result |
| --- | --- |
| `GOCACHE=/tmp/rt2q0-route-control-full-vet-cache go vet ./...` | PASS |
| `GOCACHE=/tmp/rt2q0-route-control-full-build-cache go build -buildvcs=false ./cmd/...` | PASS |
| `GOCACHE=/tmp/rt2q0-route-control-full-test-cache go test -buildvcs=false -count=1 -timeout=600s ./...` | ENVIRONMENT-BLOCKED: pre-existing network/miniredis tests fail with `listen tcp 127.0.0.1:0: socket: operation not permitted`; the new route-control packages pass in the same run |
| `I0 可监听环境：focused unit/race/vet/build` | PASS |
| `I0 可监听环境：go test ./...` | PASS |
| `I0 CLI: --level E1` | PASS, exit `0`, `E1 self-check PASS` |
| `I0 CLI: --level E3` | PASS, exit `2`, `E3 INCONCLUSIVE` without attestation |

The local full-test limitation was caused by the managed sandbox's socket
policy. I0 independently re-ran the full repository test in a listen-capable
environment and confirmed PASS. No production file was changed to work around
the local limitation. A prior failure using stale `--scenario`/
`--evidence-level` plan parameters was not an implementation gate.

Post-Luna fix verification:

| Command | Result |
| --- | --- |
| `gofmt -w cmd/rt2q-route-control-harness/*.go internal/testkit/routecontrol/*.go` | PASS |
| `GOCACHE=/tmp/rt2q0-route-control-final-fix-test-cache go test -buildvcs=false -count=1 ./internal/testkit/routecontrol/... ./cmd/rt2q-route-control-harness` | PASS |
| `GOCACHE=/tmp/rt2q0-route-control-final-fix-race-cache go test -buildvcs=false -race -count=1 -timeout=120s ./internal/testkit/routecontrol/... ./cmd/rt2q-route-control-harness` | PASS |
| `GOCACHE=/tmp/rt2q0-route-control-fix-vet-cache go vet ./internal/testkit/routecontrol/... ./cmd/rt2q-route-control-harness` | PASS |
| `GOCACHE=/tmp/rt2q0-route-control-fix-build-cache go build -buildvcs=false -o /tmp/rt2q0-route-control-fix-harness ./cmd/rt2q-route-control-harness` | PASS |
| `/tmp/rt2q0-route-control-fix-harness --level E1` | PASS, exit `0`, `E1 self-check PASS` |
| `/tmp/rt2q0-route-control-fix-harness --level E3` | PASS, exit `2`, `E3 INCONCLUSIVE` |
| `GOCACHE=/tmp/rt2q0-route-control-full-fix-test-cache go test -buildvcs=false -count=1 -timeout=600s ./...` | ENVIRONMENT-BLOCKED locally by existing socket/miniredis tests; `internal/testkit/routecontrol` and `cmd/rt2q-route-control-harness` PASS in the same run |
| `GOCACHE=/tmp/rt2q0-route-control-full-fix-vet-cache go vet ./...` | PASS |
| `GOCACHE=/tmp/rt2q0-route-control-full-fix-build-cache go build -buildvcs=false ./cmd/...` | PASS |
| `gofmt -l ...`, `git diff --check` | PASS |

Final closure verification for `f66186c`:

| Command | Result |
| --- | --- |
| `gofmt -l cmd/rt2q-route-control-harness internal/testkit/routecontrol` | PASS |
| `GOCACHE=/tmp/rt2q0-route-control-final-focused-cache go test -buildvcs=false -count=1 ./internal/testkit/routecontrol ./cmd/rt2q-route-control-harness` | PASS |
| `GOCACHE=/tmp/rt2q0-route-control-final-race-cache go test -buildvcs=false -race -count=1 -timeout=120s ./internal/testkit/routecontrol ./cmd/rt2q-route-control-harness` | PASS |
| `GOCACHE=/tmp/rt2q0-route-control-final-vet-cache go vet ./internal/testkit/routecontrol ./cmd/rt2q-route-control-harness` | PASS |
| `GOCACHE=/tmp/rt2q0-route-control-final-build-cache go build -buildvcs=false -o /tmp/rt2q0-route-control-harness-final ./cmd/rt2q-route-control-harness` | PASS |
| `GOOS=windows GOARCH=amd64 go test -buildvcs=false -c ./internal/testkit/routecontrol` | PASS |
| `GOOS=plan9 GOARCH=amd64 go test -buildvcs=false -c ./internal/testkit/routecontrol` | PASS |
| `/tmp/rt2q0-route-control-harness-final --level E1` | PASS, exit `0`, `E1 self-check PASS` |
| `/tmp/rt2q0-route-control-harness-final --level E3` | PASS, exit `2`, `E3 INCONCLUSIVE: real attestation is required` |
| `git diff --check` | PASS |

Final review continuation verification from this worktree:

| Command | Result |
| --- | --- |
| `gofmt -l cmd/rt2q-route-control-harness internal/testkit/routecontrol` | PASS |
| `GOCACHE=/tmp/rt2q0-focused-cache go test -buildvcs=false -count=10 ./internal/testkit/routecontrol ./cmd/rt2q-route-control-harness` | PASS |
| `GOCACHE=/tmp/rt2q0-final-race-cache go test -buildvcs=false -race -count=1 -timeout=120s ./internal/testkit/routecontrol ./cmd/rt2q-route-control-harness` | PASS |
| `GOCACHE=/tmp/rt2q0-final-vet-cache go vet ./...` | PASS |
| `GOCACHE=/tmp/rt2q0-final-build-cache go build -buildvcs=false ./cmd/...` | PASS |
| `GOOS=windows GOARCH=amd64 go test -buildvcs=false -c ./internal/testkit/routecontrol` and `./cmd/rt2q-route-control-harness` | PASS |
| `GOOS=plan9 GOARCH=amd64 go test -buildvcs=false -c ./internal/testkit/routecontrol` and `./cmd/rt2q-route-control-harness` | PASS |
| `/tmp/rt2q0-route-control-harness-final3 --level E1` | PASS, exit `0`, `E1 self-check PASS` |
| `/tmp/rt2q0-route-control-harness-final3 --level E3` | PASS, exit `2`, `E3 INCONCLUSIVE: real attestation is required` |
| `GOCACHE=/tmp/rt2q0-final-full2-cache go test -buildvcs=false -count=1 -timeout=600s ./...` | ENVIRONMENT-BLOCKED: pre-existing diagnostics/miniredis tests cannot listen on `127.0.0.1:0` under the managed socket policy; route-control and CLI packages pass in the same run |
| `git diff --check` | PASS |

Final closure 4P1+4P2 verification from this worktree:

| Command | Result |
| --- | --- |
| `gofmt -l cmd/rt2q-route-control-harness internal/testkit/routecontrol` | PASS |
| `GOCACHE=/tmp/rt2q0-final-count10-cache go test -buildvcs=false -count=10 ./internal/testkit/routecontrol ./cmd/rt2q-route-control-harness` | PASS |
| `GOCACHE=/tmp/rt2q0-race-recheck-cache go test -buildvcs=false -race -count=1 -timeout=180s ./internal/testkit/routecontrol ./cmd/rt2q-route-control-harness` | PASS |
| `GOCACHE=/tmp/rt2q0-final-vet-cache go vet ./...` | PASS |
| `GOCACHE=/tmp/rt2q0-final-build-cache go build -buildvcs=false ./cmd/...` | PASS |
| `GOOS=windows GOARCH=amd64 go test -buildvcs=false -c ./internal/testkit/routecontrol` and `./cmd/rt2q-route-control-harness` | PASS |
| `GOOS=plan9 GOARCH=amd64 go test -buildvcs=false -c ./internal/testkit/routecontrol` and `./cmd/rt2q-route-control-harness` | PASS |
| `/tmp/rt2q0-route-control-harness-final --level E1` | PASS, exit `0`, `E1 self-check PASS` |
| `/tmp/rt2q0-route-control-harness-final --level E3` | PASS, exit `2`, `E3 INCONCLUSIVE: real attestation is required` |
| `GOCACHE=/tmp/rt2q0-final-full-test-cache go test -buildvcs=false -count=1 -timeout=600s ./...` | ENVIRONMENT-BLOCKED: existing diagnostics and miniredis/Redis tests cannot listen on `127.0.0.1:0`; `internal/testkit/routecontrol` and CLI packages pass in the same run |
| `git diff --check` | PASS |

## Final P1+P2 Ownership Continuation

This continuation closes the remaining after-start reaper and process-group
close ownership findings from `7631076` without changing the scaffold-only
scope:

- afterStart cleanup calls `cmd.Process.Kill` when the raw pidfd is absent or
  pidfd signaling fails. It never falls back to an unpinned numeric PID or
  negative-PGID signal.
- A bounded after-start `Wait` timeout registers the already-running wait
  goroutine in a package-owned supervised reaper registry. The registry keeps
  ownership observable through `SupervisedReaperCount` and drainable through
  `DrainSupervisedReapers` until the real `Wait` returns, and preserves the
  eventual wait error for the drain result.
- Linux member pidfd close failures remain in a group-owned pending set keyed
  by the held descriptor. `group.close`, cleanup, and later `Wait`/`Shutdown`
  passes retry only still-owned descriptors; leader close ownership remains
  held until success.
- `Process.completed` runs a locked finalization retry pass after a prior
  cleanup/close failure and replaces the stale error after successful retry.
  Linux `EINTR`/`EBADF` close results are treated as descriptor-release
  outcomes and are never blindly retried, avoiding close-on-reuse.
- Added regressions cover timeout owner existence followed by unblock and
  registry drain to zero, raw-pidfd-missing child kill/reap, leader close
  fail-once through `Wait` and `Shutdown`, member close fail-once through
  pending ownership and `Shutdown` retry, descriptor convergence, and
  one-shot `EINTR` close behavior.

Final P1+P2 verification from this worktree:

| Command | Result |
| --- | --- |
| `gofmt -l cmd/rt2q-route-control-harness internal/testkit/routecontrol` | PASS |
| `GOCACHE=/tmp/rt2q0-final-count10-cache go test -buildvcs=false -count=10 ./internal/testkit/routecontrol ./cmd/rt2q-route-control-harness` | PASS |
| `GOCACHE=/tmp/rt2q0-final-race-cache go test -buildvcs=false -race -count=1 -timeout=180s ./internal/testkit/routecontrol ./cmd/rt2q-route-control-harness` | PASS |
| `GOCACHE=/tmp/rt2q0-final-vet-cache go vet ./internal/testkit/routecontrol ./cmd/rt2q-route-control-harness` | PASS |
| `GOCACHE=/tmp/rt2q0-final-full-vet-cache go vet ./...` | PASS |
| `GOCACHE=/tmp/rt2q0-final-full-build-cache go build -buildvcs=false ./cmd/...` | PASS |
| `GOOS=windows GOARCH=amd64 go test -buildvcs=false -c` for routecontrol and CLI | PASS |
| `GOOS=plan9 GOARCH=amd64 go test -buildvcs=false -c` for routecontrol and CLI | PASS |
| `/tmp/rt2q0-route-control-harness-final --level E1` | PASS, exit `0`, `E1 self-check PASS` |
| `/tmp/rt2q0-route-control-harness-final --level E3` | PASS, exit `2`, `E3 INCONCLUSIVE: real attestation is required` |
| `GOCACHE=/tmp/rt2q0-final-full-test-cache go test -buildvcs=false -count=1 -timeout=600s ./...` | ENVIRONMENT-BLOCKED: pre-existing diagnostics/miniredis/Redis tests cannot listen on `127.0.0.1:0`; routecontrol and CLI packages pass in the same run |
| `git diff --check` | PASS |

## Current Limitations

- This is a standard-library test/evidence scaffold and does not exercise
  production route runtime, store, proto, composition, smoke, or e2e paths.
- This task proves only the harness's E1 self-check. No E3/E4 run was performed;
  no real attestation or route-control oracle exists here, so E3/E4 cannot be
  declared `PASS`.
- The CLI's E1 self-check is not production evidence and must not be promoted.
- The review fix does not add any production runtime/store/proto/composition
  dependency or any business decision logic.
- Linux receives the only strong descriptor-relative/no-follow
  implementation. Windows and Plan 9 are intentionally fail-closed for
  durable evidence and were compile-checked. Linux rollback may retain an
  empty abandoned run directory; this is intentional to avoid deleting a
  replacement entry through a rename/recreate race.

## Final Gate

Q Final must review this delivery from a **new worktree**. No push, merge,
secret, or child-agent operation is part of this task.

## Commit Record

- base implementation/test scaffold: `e8cb97f`
  (`test(rt2q): add route-control evidence harness scaffold`)
- prior schema/handoff documentation: `ff1d3b0`
- Luna review fix: `2cb9319`
  (`fix(rt2q): close Luna review safety findings`)
- closure review fix: `f66186c`
  (`fix(rt2q): close route-control closure review`)
- final review continuation fix and handoff: `9b810cd`
- closure 4P1+4P2/I0 continuation fix and handoff: this appended commit; full SHA is
  reported in final delivery.
- closure 3P1+1P2 continuation from parent `1e75c63`: this appended commit; full
  SHA is reported in final delivery.
- final P1+P2 ownership continuation from parent `7631076`: this appended local
  commit; no push was performed.
- no push or merge was performed.
