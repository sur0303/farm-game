# Route Dynamic Wave 3 Checkpoint Handoff

- checkpoint branch: `checkpoint/route-dynamic-wave3-20260810`
- frozen parent: `4895ee613a29264e71b8e479d4821d136f099e85`
- RT2I exact head: `ab72420e70c8f3eb707442068073ce483c0b9a28`
- RT2M exact head: `69341be094cd3f8f6bbc26bd8206d3e0c80daca3`
- RT2I merge: `fda768f2a2788c1214ddeb8b06f5f17b6a92d874`
- RT2M merge: `b8dabfce9a7787fe77e82900fb54b5b0687d67c4`
- RT2W C1: `7fb4b7ed0ff560c51df3eadaf9cce03fda4d095d`
- RT2W C2: `79c82db4381b6d759df4dd3aca91f14178622539`
- RT2W C3: `2ac16ffb33d3efafe8e16bfc0dedc3dfca4b918b`
- RT2W exact-head review closure Endpoint/Redis: `c65226a09fc2c1df3b80e25421d07daff6e725ac`
- RT2W exact-head review closure VerificationPort: `ff34b9a3bc1b8ea09b5bf3eb8cf277065965c4bf`
- RT2W final exact-review four-finding closure: `38ce1074c9e62d11f0ff6de7490aab39c53e15fb`
- RT2X controller Boot rollover code closure: `e08ce13c0d4858d216bbe22758f9f40e8d16f4ad`
- RT2Y Plan TTL exact-review Writer correction: `9c03ea4401ab307d1fa500149081cbd983181e9c`
- merge order: frozen Holder checkpoint -> RT2I (including the frozen RT2C holder-only sealed seam) -> RT2M
- status: combined Wave 3 code checkpoint blockers closed in code/component evidence;
  the two earlier and one final Sol/xhigh exact-head finding sets were addressed by the sole Writer;
  no post-fix independent exact-head re-review is claimed; RT2Q Final multi-Pod runtime
  evidence remains pending

## Frozen content

This checkpoint combines the reviewed production composition and deploy contract from RT2I with the reviewed RoutePlan, recovery, Redis stores, route-controller, and `routectl` implementation from RT2M. Both feature heads were pushed before checkpoint construction. The two merge commits are signed, contain the exact reviewed heads as second parents, and were created without conflict or source adjustment.

RT2I exact deploy review at `ab72420e...` reported `P0=0, P1=0, P2=0`, `NO FINDINGS / NO BLOCKING FINDINGS`. RT2M exact Executor and Redis/store reviews at `69341be...` reported no remaining blocking finding. The source handoffs remain:

- `docs/verification/rt2i-route-control-composition-handoff.md`
- `docs/verification/rt2m-routectl-migration-handoff.md`

## Verification

Passed on the combined merge:

- focused count-20 across composition, Holder roots, Manager, RoutePlan, all three affected Redis packages, route-controller, and `routectl`
- full race: `go test -buildvcs=false -race -count=1 -timeout=2400s ./...`
- full non-race rerun: `go test -buildvcs=false -count=1 -timeout=1800s ./...`
- `go vet -buildvcs=false ./...`
- `go build -buildvcs=false ./...`
- proto regeneration and clean diff under `api/proto` and `api/gen`
- base plus remote-watch Compose render with non-secret placeholders
- diff check, forbidden Redis operation scan, added-diff secret scan, merge ancestry, and signed-commit checks
- company Redis production probe with the ignored integration secret config supplied by explicit path
- exact temporary-key company Redis capability probe: `MEMORY_USAGE_SAMPLES_0=PASS`; the probe used exact `SETNX`, `MEMORY USAGE ... SAMPLES 0`, `DEL`, and `EXISTS` operations and verified cleanup, with no secret output

The first full non-race run encountered one unrelated intermittent failure in `TestTheftAdmissionTwoServicesRefreshConcurrentPairRecordVersion`; a second full run passed. A diagnostic count-100 run observed two occurrences, while the frozen parent count-100 diagnostic passed. Wave 3 changes no `internal/farm`, `internal/friend`, `internal/workflow`, or dependency file. No source workaround or out-of-scope change was made. The full race run and the Wave 3 focused count-20 gate passed.

## RT2W closure verification

The unified closure series above additionally passed:

- focused `-count=20` across both command roots and all affected Manager,
  composition, transport, Gate-verification, RoutePlan, and Redis route-control packages;
- Manager restart/retained-completion/atomic-eviction, exact Redis observation,
  AssignmentComposition revision barrier, production verification, direct-peer mTLS,
  adversarial certificate/metadata, combined mTLS+Redis, and concurrent Close tests at
  `-count=100` and again at `-race -count=100`;
- `go test -buildvcs=false -count=1 -timeout=1800s ./...` and
  `go test -race -buildvcs=false -count=1 -timeout=2400s ./...`, both without rerun;
- `go vet -buildvcs=false ./...` and `go build -buildvcs=false ./...`;
- full proto regeneration with no `api/proto` or `api/gen` drift;
- real base+remote Compose rendering with per-service full Redis tuple, distinct
  Logic/HolderControl endpoint, direct Gate peer, and separate verifier TLS assertions;
- company Redis production capability probe (`noeviction`, WAIT, EVAL/EVALSHA,
  NOSCRIPT-without-flush and configured Lua sizes all applicable PASS), the C2 one-key
  exact tuple probe, and the C3 five-known-key exact transition probe. Both exact probes
  verified cleanup with `EXISTS=0`; no credential or Redis URL was printed;
- owned-path, frozen public Proto/Authority/dependency surface, forbidden Redis operation,
  routectl Redis-import, added-secret, large-object, and `git diff --check` scans.

Environment notes: bare focused `go vet`/`go build` first encountered the known linked-
worktree VCS-stamping `git status` exit 128; the required full commands passed with the
repository's established `-buildvcs=false` setting. An additional broader, pre-existing
Q2R2 diagnostic sweep was not used as RT2W pass evidence: eight applicable probes passed,
the proxy-only cluster item skipped, and its raw ProcessHook `ECONNREFUSED` case expected
`UNAVAILABLE_NOT_SENT` but the unchanged conservative classifier returned
`TIMEOUT_UNKNOWN`. No Authority source or out-of-scope test was changed. C3's unit-only
NOSCRIPT setup was corrected before its final signed SHA to use a fresh script cache
instead of `SCRIPT FLUSH`; its exact Redis count-100/race-count-100 and all full gates
then passed again.

## Exact-head Sol/xhigh finding closure evidence

The two review-closure SHAs above additionally passed from this clean worktree:

- focused affected-package `-count=20` for endpoint/Redis and for Manager, composition,
  transport, Gate verification, RoutePlan, route-controller, and `routectl`;
- every new malformed endpoint/TLS/generation/lease/cleanup/timeout-config test at
  `-count=100` and again at `-race -count=100` for the applicable packages;
- `go test -buildvcs=false -count=1 -timeout=1800s ./...` and
  `go test -race -buildvcs=false -count=1 -timeout=2400s ./...` at final code head
  `ff34b9a3bc1b8ea09b5bf3eb8cf277065965c4bf`, both without rerun;
- `go vet -buildvcs=false ./...`, `go build -buildvcs=false ./...`, and full Proto
  regeneration with no `api/proto` or `api/gen` drift;
- rendered base+remote Compose assertions for all five roles' exact seven-field Redis
  tuple, all four remote consumers, distinct Logic/HolderControl endpoints, exact holder
  and Gate TLS identities, direct Gate address, strict verifier timeout, singleton replica,
  attestation, and stop-first update/rollback;
- company Redis production capability plus the separately opted-in exact tuple live probe.
  The latter used a cryptographically unique stable workflow ID, refused SETNX collision,
  performed exact DEL, and proved `EXISTS=0`; captured output contained no Redis URL or
  credential.

Owned-path, forbidden Redis operation, added-secret, large-object, diff, ancestry, and SSH
signature-presence checks were run before the documentation commit. No push was performed.

## Final exact-review four-finding closure

Final Reviews of clean exact head `77b8b04a2bb4f87f0681d729f2920f6d66f8526c`
returned one bounded P1 and three bounded P2 findings. The sole Writer closed all four in
the signed code commit `38ce1074c9e62d11f0ff6de7490aab39c53e15fb`:

- the final `tombstone` verification check now treats Manager's fully validated retained
  `CompleteRecovery` as the ordinary durable replay floor and its atomic exact Tombstone
  replacement as the eviction successor. Transition/current-generation/result checks and
  the distinct recovery predicate remain unchanged in strength. A real Manager →
  PlanExecutor → ProductionVerification test proves the routectl-compatible eleven-check
  PASS before and after forced cross-domain memo/recovery eviction;
- Holder Lua rejects the complete IPv4-mapped IPv6 `/96`, including compressed, dotted,
  and pure-hex spellings in both endpoint argument positions, with direct-script zero-write
  parity against the unchanged Go validator;
- the company Redis tuple probe installs ambiguity cleanup before `SETNX` and uses one
  fixed atomic marker-equals-then-`DEL` operation. It proves `EXISTS=0` only after deleting
  its own marker; a collision or replacement remains untouched and fails safely;
- remote Player/Farm constructors branch on the supplied immutable tuple before namespace
  environment/default access and pass its Namespace directly to friendship/result stores.
  Counter/panic tests cover result-directory and result-publisher paths; nil-tuple static
  defaults remain `dev`/`z1`.

At that code SHA, focused affected packages passed `-count=20`; every new exact regression
passed `-count=100` and `-race -count=100`; full non-race and race suites passed once without
rerun; vet, build, Proto regeneration/clean diff, and base+remote Compose rendering passed.
The explicitly opted-in company Redis production/live run passed and the cryptographically
unique tuple key reported `cleanup_EXISTS=0`; no Redis URL or credential was printed.
Frozen Proto/generated/dependency surfaces, routectl Redis imports, forbidden Redis
operations, added secrets, diff integrity, and SSH signature presence were also checked.

This is Writer closure evidence, not an independent post-fix `NO FINDINGS` review.

## RT2X controller Boot rollover blocker closure

RT2Q discovery showed that the stop-first/Recreate deployment correctly creates a new
controller Boot for every process generation, while the production SnapshotClient used to
pin the first Boot permanently. Reusing the old Boot is forbidden, so RT2Q could not execute
its required controller-restart scenario with consumers left live.

The signed RT2X code commit `e08ce13c0d4858d216bbe22758f9f40e8d16f4ad`, based exactly on
this checkpoint's `abbc60797a3bce63d2feac1781a54b53ecea086b`, closes that bounded
code/component blocker:

- the original `RemoteSnapshotSource` stays unchanged; an optional restart-aware source
  capability accepts the Pump's exact installed complete snapshot and revision;
- the Pump prefers that capability only for a full resync after an established Watch channel
  closes. Initial fetches, Watch-start failures, legacy/fake sources, and hot Resolve remain on
  their existing paths;
- SnapshotClient excludes rollover from every active Watch and ordinary operation, closes and
  unregisters the old Watch before the production rollover RPC, and atomically retires C0 and
  pins C1 only after identity, realm, topology, revision, exact equal-envelope, canonical Boot,
  and bounded-capacity checks pass;
- retired Boots are retained without eviction in an explicitly bounded option-controlled set
  (default 8, hard maximum 64). Capacity exhaustion, a retired Boot, rollback, equal mismatch,
  overlap, or contract drift latches terminal failure; same-Boot transport reconnect remains
  recoverable;
- a higher-revision response is still installed only through the existing Pump complete-
  snapshot monotonic/transition validator. If that installation rejects, AssignmentComposition
  terminates and readiness stays false.

Verification on the exact code content passed affected packages at `-count=20` and
`-race -count=20`; deterministic rollover/concurrency barriers passed at `-count=100` and
`-race -count=100`; full repository non-race and race suites passed once without rerun; vet,
build, full Proto regeneration with zero generated/schema drift, real base+remote Compose
rendering, diff/scope/forbidden-operation/added-secret/large-object checks, and SSH signature
presence passed. No Proto/generated, Authority, Manager, RoutePlan, Holder, Redis, command-root,
hot-routing, dependency, leader/lease/revision-state-machine, or payload/metadata-trust surface
was changed. No push was performed.

This is a Writer code/component closure only. RT2Q Final multi-Pod controller restart and the
rest of its runtime evidence remain pending; this section does not claim RT2Q acceptance,
Premerge, G1/G2/G3, CP, HA, leader election, or autoscaling readiness.

## RT2Y Plan TTL composition blocker and exact-review correction

RT2Q discovered that production route-controller never supplied `PlanExecutorOptions.PlanTTL`, so
the Executor always normalized to `24h`. The first signed RT2Y code commit
`eb765cdc9187d5ea17fd14c62038392a97ca1a30`, based exactly on pushed
`edd53ed6ad14256c994371be845a2da05de24672`, added the strict command-root setting and configured
the Executor.

Independent exact review of clean head `683df6286a016cf823b0d44aebf3a9a6595ec5fc` then found one
P1: production Redis PlanStore construction retained its default `24h` `Options.PlanTTL`. A
configured `25h` or `7d` Executor could therefore start, but its first Plan CAS was rejected by the
store lifetime bound. The same sole Writer addressed that finding in signed corrective code commit
`9c03ea4401ab307d1fa500149081cbd983181e9c`:

- production starts from the existing Redis routeplan `DefaultOptions(namespace)` and assigns the
  same immutable `cfg.planTTL` used by the Executor before PlanStore construction;
- both production construction helpers reuse the existing shared timing validator, without adding
  another parser or default; above-`7d` and unsafe operation/safety combinations fail closed;
- a real Redis PlanStore construction/integration regression proves exact Executor/store behavior
  for `120s`, `25h`, and `7d`: exact-horizon initial CAS succeeds, horizon plus `1ms` fails, and
  physical TTL still adds the unchanged default audit retention;
- no Redis PlanStore implementation, audit policy, Compose value, Proto/generated, Authority,
  Manager, Holder, routectl, consumer, dependency, leader/lease, or revision-state-machine surface
  changed.

The six focused Plan TTL regressions passed `-count=100` and `-race -count=100`; full non-race
passed, and the second full race run passed after the first hit the unrelated existing
`internal/friend.TestConcurrentAddBothDirectionsAndDelete` concurrency failure. Vet, command build,
Proto regeneration/clean diff, real Compose rendering, scope/diff/secret, and cryptographic
corrective-commit signature checks passed. No out-of-scope workaround was made. The dedicated
record is `docs/verification/rt2y-route-controller-plan-ttl-handoff-20260810.md`.

This is same-Writer corrective evidence for the independent finding. It does not claim an
independent post-fix `NO FINDINGS` review or review closure; RT2Q Final runtime evidence remains
pending.

## Remaining gates

RT2W C3 now composes the real non-nil/non-noop read-only `VerificationPort` in the
production route-controller. It observes every explicit direct traffic Gate through
separate TLS 1.3 mTLS and exact leaf identity, and obtains exact transition/recovery/
tombstone evidence through Manager-owned typed observation and bounded exact-ID Redis
reads. The transition, recovery, and tombstone predicates now bind the same immutable
current Authority and durable Snapshot. Manager returns the exact validated durable
recovery or tombstone value and terminal result; generation N cannot pass against current
N+1. Gate verification uses an exact single-DNS-SAN TLS leaf, observes only fresh leases,
and is followed by an Executor final re-read/freshness check. The stable checks, including
the new code-only `final_freshness` check, remain independent and fail closed.

The Endpoint/Redis closure independently enforces the same bounded canonical `host:port`
contract in Go and Holder Redis Lua, revalidates both endpoint roles before Manager side
effects, binds HolderControl to an exact single-DNS-SAN TLS leaf, uses an immutable parsed
remote tuple, and makes company-Redis live tests explicitly opted in with uniquely owned
SETNX keys and exact DEL/EXISTS=0 cleanup. Gate direct-peer addresses use that same
canonical address contract. The VerificationPort closure additionally makes the Gate
verifier timeout environment strict when present.

The original exact-head reviews reported `P0=0`, with Reviewer A `P1=3/P2=1` and Reviewer
B `P1=4/P2=1`; the final exact Reviews then returned one P1 and three P2 findings at
`77b8b04a...`. The three closure SHAs above address every reported item. This handoff
records Writer verification only; it does not upgrade the review state to an independent
post-fix `NO FINDINGS` verdict.

This closes the reported Writer code/component findings. It does **not** declare a fresh
independent exact-head review, RT2Q Final, or production multi-Pod runtime acceptance.

Before Route Control MVP may freeze:

1. execute the still-pending RT2Q Final multi-Pod, planned Drain migration, controller
   restart, commit-unknown recovery, crash takeover, Fresh Join, and four-truth
   reconciliation evidence;
2. perform the still-pending downstream Premerge, G1/G2/G3, and CP decisions only after
   that independent runtime evidence.

No autoscaling readiness is claimed; the explicit Gate peer list and single-controller
non-HA deployment remain fixed pending separate runtime acceptance.
