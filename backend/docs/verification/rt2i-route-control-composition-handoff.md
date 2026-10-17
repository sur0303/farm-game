# RT2I Route-Control Composition Handoff

- task: `RT2I-ROUTE-CONTROL-COMPOSITION` — two Sol reviews merged, then three exact-head Luna lifecycle closeouts
- original base: `6432539c2c1640e405b35eb980d6856c1557c63c`
- Luna review base: `68a31cbda540799366c437200db4d555e643adbf`
- Holder Luna review base: `f2da5c3daf102bb562c56dd323edb4146c4ed632`
- final Luna review base: `f1acc3be96411951ffdf9b03241f8948c79f7d72`
- RT2M deploy addendum base: `5d0ca71e7f94f744f91a2b54ab816b55ff305b42`
- branch: `codex/rt2i-route-control-composition-20260809`
- writer: official OpenAI Sol (`gpt-5.6-sol`), `xhigh`; controlled `PATH` with explicit `openai`; nested writers: `0`
- status: implementation and verification complete; signed commit reported by final delivery
- push: `NOT DONE`

## RT2W unified closure addendum (2026-08-10)

This addendum supersedes only the older `PENDING Final Composition Gate` statement below;
the historical RT2I scope and evidence remain records of their exact earlier heads.

- C1 `7fb4b7ed0ff560c51df3eadaf9cce03fda4d095d` separates mandatory Logic and
  HolderControl endpoints through holder registration, routing, drain, and deployment.
- C2 `79c82db4381b6d759df4dd3aca91f14178622539` validates and fingerprints the complete
  shared remote Redis keyspace tuple for every role.
- C3 `2ac16ffb33d3efafe8e16bfc0dedc3dfca4b918b` composes the real production read-only
  VerificationPort. farm-gate serves exact observations from its one hot
  AssignmentComposition Resolver through a separate sealed TLS 1.3 mTLS endpoint;
  route-controller observes every explicitly configured direct Gate peer.
- Exact-head review closure `c65226a09fc2c1df3b80e25421d07daff6e725ac` makes remote
  Player/Farm composition consume only the already parsed immutable PhysicalProfile,
  applies one canonical address grammar to Holder and direct-Gate endpoints, and binds
  HolderControl server proof to an observed exact single-DNS-SAN TLS leaf.
- Exact-head review closure `ff34b9a3bc1b8ea09b5bf3eb8cf277065965c4bf` makes Gate
  client authorization DNS-only without changing generic URI-capable consumers, applies
  lease freshness inside the composition health/revision bracket and Gate transport, and
  adds the final controller freshness bracket.
- Final exact-review closure `38ce1074c9e62d11f0ff6de7490aab39c53e15fb` makes remote
  Player/Farm friendship and result-store construction select the supplied immutable tuple
  Namespace before any namespace environment/default read. It also closes the adjacent
  Holder Lua IPv4-mapped IPv6 parity and company Redis probe ownership findings.

The production composition blocker and the final four reported code findings are closed at
unit/component scope: missing verifier TLS, an empty/duplicate/malformed peer list, any
stale/ahead/unhealthy/unavailable Gate,
or missing/mismatched durable Manager evidence fails closed. Gate verifier listen may
precede assignment health, but observations remain unavailable until the same composition
is healthy. The two earlier and final pre-fix exact-head Reviews' findings are addressed,
but no post-fix independent exact-head `NO FINDINGS` verdict is claimed. This addendum does
not claim RT2Q Final, runtime multi-Pod evidence, Premerge, G1/G2/G3, CP, or autoscaling
readiness. The controller remains single-replica non-HA.

The final closure SHA passed the affected composition/command/Redis/RoutePlan package set
at `-count=20`, every new exact test at `-count=100` and `-race -count=100`, both full
repository suites, vet, build, clean Proto regeneration, base+remote Compose rendering,
and the explicitly opted-in company Redis tuple probe. That probe used a cryptographically
unique key and atomic marker-equals-then-delete cleanup, proving `EXISTS=0` only after an
owned deletion. No Redis URL or credential was emitted. This remains Writer evidence; no
independent post-fix `NO FINDINGS` verdict is claimed, and RT2Q Final, Premerge, G1/G2/G3,
CP, and autoscaling remain pending exactly as below.

## Ownership

The original RT2I owned set is unchanged:

- `internal/authoritycomposition/**`
- `internal/gatewayapp/**`
- `cmd/player-svr/**`, `cmd/farm-svr/**`, `cmd/farm-worker/**`, `cmd/farm-gate/**`
- `deploy/**`
- this handoff

No RT2M controller/Manager/Plan implementation, Redis script, `internal/holdercontrol`,
`internal/routecontrol/transport`, proto/public API, dependency file, or other root was modified.
The deploy example builds the already-owned existing `cmd/route-controller`; it does not alter it.
The RT2M deploy addendum changes only `deploy/docker-compose.remote-watch.example.yml`,
`deploy/README.md`, and this handoff. It does not modify RT2M code or any other path.

## Endpoint generations and readiness

- Remote Gateway, farm-svr routed PLAYER participants, and farm-worker routes use one
  `EndpointConnectionManager` over the same `DynamicAssignmentProvider`. Every full
  snapshot is endpoint-count and canonical-host:port checked before publication.
- New endpoints are pre-dialed with bounded concurrency and an independent bounded
  endpoint dial timeout. Only a complete successful pool is atomically published for
  the snapshot revision. A dial/validation failure retains no partial generation,
  terminates the composition, and makes readiness false.
- Removed connections are retired only after they are absent from the current snapshot
  and their inflight lease count reaches zero, or after the bounded retirement grace.
  Retirement registration, generation publication, and the closed gate now linearize
  under the same manager lock. `Close` starts no external work while holding that lock,
  admits no later `WaitGroup.Add`, is idempotent, and joins every published generation's
  retirement owner. A deterministic A→B barrier test parks after swap and before the old
  worker starts while `Close` enters its wait.
- The routed RPC hot path performs local resolve/fence checks plus a pool acquire. It
  performs no dial, snapshot fetch, Watch call, or other remote route-control RPC.
- Remote readiness is exactly: the initial full snapshot has been installed, Pump is in
  healthy `WATCHING`, provider/resolver/applier/pool share one revision and endpoint set,
  and (for holder roots) HolderAgent is healthy. It does not require an ACTIVE route,
  an unexpired route at `time.Now`, or a non-empty serving set. Complete all-ASSIGNING /
  DRAINING snapshots can therefore be ready and support `READY_ZERO_SHARD`.
- Ordinary RT2B watch loss remains inside Pump reconnect/resync. Only a final component
  terminal records the exact local terminal error, cancels and joins all Pump/Applier/
  pool owners, and degrades readiness. Runtime logs state only the generic terminal event.
- Runtime membership observation now passes the Runtime lifetime into
  `ObserveContext`. Shutdown linearizes as Runtime cancel → reconciler retry/action
  cancellation and barrier → observer/static-applier join → scheduler and guard cleanup.
  Context-compliant Redis/Port calls therefore unblock without helper goroutines, every
  retry/observer owner is joined, and no late Observe can re-admit a guard. Concurrent
  reconciler Close callers join the same complete `sync.Once` barrier.
- Startup action admission rechecks the bounded action context after network prepare and
  at every local phase boundary: before/after scheduler Reserve, before/after guard
  Commit, and before/after scheduler Activate. The real adapter also checks immediately
  before `InstallIfMatches`. Cancellation through the final post-Activate linearization
  cancels the exact reservation/entry and fences/removes the exact guard inside the same
  action owner using the still-live reconciler lifetime; it remains pending/degraded and
  can never publish Installed/FullyReconciled/Ready. Deterministic phase barriers freeze
  Reserve-complete, guard-installed, and entry-activated cancellation independently.
- Scheduler Close has one `closeOnce` owner and one `closeDone` completion barrier. The
  owner closes the admission gate under the scheduler lock, cancels every entry and joins
  every entry goroutine before publishing done; all concurrent Scheduler.Close and
  Runtime.Close callers wait for that same completion. Runtime Fatal still only cancels
  exact entries and never invokes Scheduler.Close from an entry OnFatal callback, avoiding
  self-wait.

## Mode and Holder lifecycle

- `ACTOR_ASSIGNMENT_MODE` remains strict and defaults to `static`. The accepted matrix
  remains `legacy+static`, `composed+static`, and `composed+remote-watch`.
- `HolderCompositionEnabled` is the sole construction gate for HolderRegistry,
  HolderAgent, and HolderControl: it is true only for `composed+remote-watch`.
  Consequently composed+static needs no Holder TLS, performs no Holder zero-shard
  registration/heartbeat, and preserves its prior static authority lifecycle.
- Holder status combines the real runtime state with assignment health. A terminal
  assignment reports not-ready/draining and calls the bounded HolderAgent fail-closed
  close contract instead of continuing normal READY heartbeats.
- Applier ownership is explicit: static runtimes set `ownsApplier` and own their provider
  watch, while a remote runtime owns only its membership observer and leaves the one
  shared `SnapshotApplier` Run/join lifecycle to `AssignmentComposition`.
- Holder server readiness is not set before its Serve owner exists. Startup proves the
  listener has been accepted by Serve before Agent registration. Immediate Serve failure
  never registers; any later unexpected Serve exit clears readiness, records the local
  terminal cause, and starts bounded Agent close. Serve and fail-close owners are consumed
  during shutdown.
- The HolderProcess wraps the real ProcessStatus source used by every Agent operation.
  Whenever the server is not ready or any terminal is recorded, only readiness/draining
  are overridden (`ProcessReady=false`, `Draining=true`); shard facts, revision, capacity,
  and resource summary remain real. Serve failure racing an in-flight Register converges
  its durable WARMING record to DRAINING/DRAINED and cannot leave READY behind.
- player-svr and farm-svr now derive Holder lifecycle flags from all three local truths:
  Runtime health, assignment health, and shutdown draining. A Runtime Fatal is returned
  as a valid status with `ProcessReady=false` and `Draining=true`, while real serving
  count, snapshot revision, capacity, and resource facts are preserved. The next bounded
  Agent heartbeat moves a durable READY/SERVING record directly to DRAINING (without
  waiting for lease expiry), and bounded Close advances it to DRAINED.
- The Holder server leaf is checked before Agent startup against the configured
  `TLSDNSIdentity`: exactly one raw DNS SAN, canonical lowercase ASCII, exact equality,
  and no wildcard, additional DNS SAN, URI, IP, email, or Common Name identity.
  Remote holder endpoints and DNS identities are explicit required environment inputs.
- Holder-only transport has no fictitious send-timeout setting. `LocalService` operation
  timeout plus the incoming gRPC context remains the unary bound; the RT2C transport seam
  was not changed.
- Business gRPC uses bounded `GracefulStop` with `Stop` fallback, so later Holder, Pump,
  runtime, client, and health cleanup continues. Command-root assignment watcher wrappers
  and Holder terminal monitors are joined before return.

## Startup timeout and deployment contract

- `ROUTE_CONTROL_DIAL_TIMEOUT` wraps only authenticated gRPC Dial.
  `ACTOR_ROUTE_INITIAL_FETCH_TIMEOUT` independently bounds the mandatory initial fetch.
  All four roots share the same `1ms..30s` clamps; pool dial and startup-wait timeouts are
  also independently bounded.
- player-svr and farm-svr create a third, independent `ACTOR_ROUTE_STARTUP_TIMEOUT`
  context only for remote Runtime construction. It covers script warmup and the initial
  context-aware membership Observe/action, and is canceled before the separate assignment
  health wait begins. Static Runtime construction remains unchanged.
- Base/static behavior remains unchanged. `docker-compose.membership.yml` explicitly
  selects `composed+static` and does not add Holder settings.
- `docker-compose.remote-watch.example.yml` now builds and runs a real existing
  `route-controller`, and all four consumers depend on its listener health. It requires
  a durable Redis snapshot and mounts only ignored secret files by path.
- The controller is explicitly single-instance: fixed `container_name`,
  `deploy.replicas=1`, and `stop-first` update/rollback. Scaling it above one is forbidden
  until a separate HA/leader contract is reviewed. The fixed
  `single-replica-recreate-no-overlap-v1` attestation is supplied literally; deployment
  generates one Boot ID per controller generation and keeps it immutable for that
  generation.
- Snapshot clients, plan readers, and routectl operators have independent required exact
  identity lists. Authorization uses only verified TLS leaf DNS SAN identities; plan-read
  identities cannot mutate, and the routectl operator certificate must chain to the
  controller's client CA.
- RT2M Holder calls use the explicitly mounted ignored
  `/run/farm/holder-operator/{client.crt,client.key,holder-ca.crt}` files. The client leaf
  has exactly one DNS SAN, exactly matching the shared player/farm
  `HOLDER_CONTROL_CONTROLLER_IDENTITY` allowlist identity.
- Controller physical-profile, authority-layout, workflow-layout, and workflow-bucket
  inputs map directly from the existing non-secret deployment variables. The default
  plan operation timeout is 30s and the default stop grace is 45s; overrides must retain
  operation timeout plus bounded cleanup margin. No secret material is committed.
- Historical note superseded by the RT2W addendum above: at this earlier RT2I head the
  real Gate resolve/transition/recovery/tombstone Verify Port was still pending. Listener
  health and Compose rendering alone remain insufficient evidence.

## Verification record

Passed before commit:

- focused count-20:
  `go test -count=20 -timeout=600s ./internal/authoritycomposition ./internal/gatewayapp ./cmd/player-svr ./cmd/farm-svr ./cmd/farm-worker ./cmd/farm-gate ./internal/actorrouting ./internal/actorrouting/remote ./internal/holdercontrol ./internal/routecontrol/transport`
- focused race:
  `go test -race -count=1 -timeout=900s` on the same package set
- exact Endpoint swap/Close race:
  `go test -race ./internal/authoritycomposition -run '^TestEndpointConnectionManagerCloseLinearizesPublishedRetirement$' -count=100`
- exact Holder Luna races:
  `go test -race ./internal/authoritycomposition -run '^TestMembershipRuntimeCloseJoinsContextBoundObserverAndRetry$' -count=100` and
  `go test -race ./internal/gatewayapp -run '^TestRuntimeFatalStatusDurablyDrainsHolderWithoutLeaseExpiry$' -count=100`
- startup expiry admission/rollback:
  `go test -race ./internal/authoritycomposition -run '^(TestSchedulerActivateFailureRollbackDoesNotUseExpiredActionContext|TestCommitPreparedRejectsExpiredActionContextBeforeLocalAdmission)$' -count=100`
- final startup/Close barriers:
  `go test -race ./internal/authoritycomposition -run '^(TestRealMembershipCommitChecksContextImmediatelyBeforeGuardInstall|TestMembershipRuntimeStartupDeadlineAtLocalPhaseFailsClosed|TestLeaseSchedulerConcurrentCloseSharesCompleteBarrier|TestRuntimeConcurrentCloseSharesSchedulerBarrier)$' -count=100`
- full: `go test -buildvcs=false -count=1 -timeout=1200s ./...`
- full race: `go test -race -buildvcs=false -count=1 -timeout=1800s ./...`
- vet: `go vet -buildvcs=false ./...`
- build: `go build -buildvcs=false ./...`
- proto clean: `bash scripts/gen-proto.sh`, then
  `git diff --exit-code -- api/proto api/gen`
- Compose render: base plus remote-watch overlay, with non-secret placeholder inputs,
  `docker compose ... config --quiet`
- RT2M deploy addendum static checks: fixed singleton attestation, three independent
  required identity variables, Holder client path/mount parity, Redis layout mappings,
  30s/45s timeout defaults, ignored `.local` inputs, explicit Verify pending language,
  and exact three-file owned-path scope
- `git diff --check`, owned-path/forbidden-pattern scan, and added-diff secret scan

Focused regression coverage includes A→B pre-dial and atomic generation publication,
inflight retirement and swap/Close registration linearization, failed generation preservation,
startup-only dial count, exact static/remote Applier ownership and join, bounded startup
membership actions, every local startup deadline boundary and expired-context rollback,
shared Scheduler/Runtime Close joins, context-bound observer/retry shutdown,
inactive-route readiness, Pump terminal degradation/join, immediate and later Holder Serve
failure (including Register interleaving), assignment-triggered Agent fail-close,
lifecycle-constrained ProcessStatus, Runtime-fatal durable draining, strict SAN rejection,
and bounded gRPC fallback.

The known unrelated `TestOnFatalPanicPreservesPrimaryFatalCause` scheduling flaky did not
occur in this verification run, so no rerun or unrelated edit was needed.

SSH signature verification and the final commit SHA are reported with delivery. No push
was performed.
