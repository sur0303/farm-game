# RT1 minimal Route Authority control handoff

## Identity and delivery boundary

- `task_id`: `RT1-MINIMAL-ROUTE-AUTHORITY-CONTROL`
- frozen base: `3e715a481ca62091e65b2c36977b13fef1461e76`
- branch: `codex/rt1-minimal-route-authority-control-20260804`
- integration owner: `I0`
- corrective-pass starting tip: `320afcc5f3e0db25818fc2ba23c32c282e4fa68f`
- corrective-pass code/test tip: `942a410db1275242d858d8bfd272261e5bd22095`
- independent-semantic-review corrective starting tip:
  `4447863434a516f4d653631e75c2c7c7a95989ad`
- P1 closure starting tip:
  `29d2d3ecee0ba4ae9b4247139c5216f93fdf777a`
- final corrective-pass tip: the handoff commit containing this file; its
  immutable full hash and remote tip are reported in the delivery record below

This is the original RT1 writer worktree. No integration merge, MR, production
`cmd/**` switch, legacy+authority dual write, Proxyless path, socket migration,
or hot Actor memory migration was performed. Public proto/generated files and
the frozen `ShardWriteAuthorityStore` contract were not modified. In
particular, `ActorRouteHeader` was not extended with `ExpectedLeaseEpoch`.

## Pre-implementation audit matrix

| Area | 已有 | 缺口 | 本任务实现 | 留给 I0 |
| --- | --- | --- | --- | --- |
| `internal/actorrouting/**` | `RouteSnapshot`、v2 seeded XXH64 `Calculator`、`StaticResolver`、`StaticAssignmentProvider`、`RedirectApplier`、`RetryingResolver`、atomic whole-table replacement | No manager-owned authority-to-snapshot coordinator; no revisioned dynamic source, gap reload, slow-subscriber cleanup, or complete provider replacement checks | `RouteAssignmentManager` consumes the existing snapshot model; `DynamicAssignmentProvider`/`SnapshotApplier` enforce full snapshots and contiguous revisions; redirect fallback is reload-or-fail-closed; resolver/provider/static paths check cancellation | Wire the production publisher/source and process watchers in composition roots; choose PLAYER/FARM instances |
| `internal/storage/authoritystore/**` (read-only) | Frozen Claim/Renew/Transfer/Unassign, typed result/status, Memory/Redis-compatible store, base `HolderLeaseGuard` | No route snapshot publisher/watch or Registry/runtime install wiring; no public durable transition projection record | Reused unchanged; RT1 wrapper supplies local lifecycle/profile/generation admission and a manager-owned recovery seam | I0 must provide a durable `TransitionRecoveryStore` + reader/inspector adapter; do not alter the public store package |
| `internal/storage/redis/authority/**` (read-only) | Existing Redis AuthorityStore/Lua adapter and component tests | No company Redis credentials/live evidence here; no distributed snapshot publisher/watch | No Redis copy, second store, or static fence source; only the frozen store interface is consumed | Run company Redis/cluster/failover/overflow evidence and provide the distributed publisher |
| `internal/logic/actor/authority/**` | Existing `FenceSource`/authority Provider seams | No dynamic assignment+lease fence and no safe Holder runtime lifecycle seam | `DynamicFenceSource`, `LeaseGuardRegistry`, `LeaseGuardHandle`, `HolderLeaseRuntime`, profile binding, monotonic local deadline conversion, coherent fence construction | Inject into `NewAuthorityRegistry`/runtime and connect admission before Actor activation/turn |
| `internal/gateway/command/**` FirstHop seam | Existing Gate route resolution and frozen internal header | No complete local holder/generation guard validation at Execution-Accept; no cancellation race guard | Minimal `FirstHopRouteValidator`/complete header checks already in RT1; context checks before resolve and immediately before/after local admission | I0/Logic servers must call it at the real RPC/Execution-Accept boundary; no production composition wiring in RT1 |
| Player/Farm `NewAuthorityRegistry` seams | Authority registry constructors and domain separation exist | No production DynamicFenceSource/Holder runtime injection | No writes to these directories; only reusable RT1 authority seams were added | I0 performs final registry/runtime wiring and legacy→authority cutover/rollback switch |
| `cmd/**`, `deploy/**` | Legacy composition remains buildable | Production switch intentionally not in this work package | No files changed | I0 owns composition, rollout, rollback, and no-dual-write cutover |

## Authority and route invariants

`RouteAssignmentManager` is the sole Claim/Transfer/Unassign and holder
generation coordinator. It serializes each operation and projection under one
mutex and uses this order:

```text
validate address/profile/overflow and reserve bounded recovery capacity
    -> ShardWriteAuthorityStore Claim/Transfer/Unassign
    -> validate exact result address, transition ID, holder/boot, state,
       assignment revision, writer epoch, lease epoch, and lease deadline
    -> persist committed TransitionRecovery checkpoint
    -> build and validate one complete RouteSnapshot
    -> SnapshotPublisher.Publish
    -> atomically install the local snapshot / complete the recovery record
```

`APPLIED` and `ALREADY_APPLIED` are the only publishable statuses. Rejected,
transport-error, `UNAVAILABLE_NOT_SENT`, unresolved `TIMEOUT_UNKNOWN`, expired
active result, malformed holder/boot/address/profile, or overflow never
publishes a new route. Unassign accepts zero TTL only on its dedicated path and
requires an `UNASSIGNED` result with empty holder/boot, zero deadline, and
advanced all-three generation tuple; it publishes an inert `ASSIGNING` route.

Claim/Transfer results must bind exactly to the requested
`NewHolderID`/`NewHolderBootID`; the endpoint comes only from that request
bound to the same result. Direct Transfer requires crash/failure takeover mode,
the expected old boot, and explicit unavailable/expired evidence. Planned
moves use Drain.

### Restart bootstrap and committed projection recovery

Bootstrap is now limited to an authoritative absence plus the exact initial
placeholder shape: `ASSIGNING`, assignment/writer/lease revisions all `1`.
An opaque adapter without an authoritative absence reader fails closed. A
restarted ASSIGNING route from Unassign, Drain, or a failed projection retains
its recovery/transition identity and cannot regain bootstrap semantics.

`TransitionRecoveryStore` is a manager-owned seam, not a change to the frozen
storage contract. The manager requires it at construction so a production
composition cannot silently claim restart-safe recovery without an adapter.
Capacity is reserved before the AuthorityStore call. A committed but
incompletely projected transition is represented by a committed recovery
record, a pinned in-memory memo, and the same exact result/request digest; it
is not evicted by `MemoRetention` or capacity pressure. Replay reconciles the
current authority before publication and never performs a second mutation.
Drain checkpoints retain the safe Stop/Wait/Confirm/Authority/ASSIGNING/
ColdLoad/ACTIVE progression. If a pre-commit reservation survives a restart,
an authoritative absence releases it; an ambiguous read remains blocked.
If the recovery marker itself failed after Authority commit, an authoritative
identity read reconstructs the exact authority tuple and resumes projection.
Recovery reconstruction treats `Published` as proof only that `LastState` was
published. In particular, `{Committed:true, Published:true,
LastState:ASSIGNING}` for Drain remains projection/finalization pending and
non-evictable until the final authority-backed `ACTIVE` snapshot is published.
Authority publication first compares the complete target under the provider
publish lock. An identical target is a silent idempotent no-op before any
activation-specific validation. Therefore, if final `ACTIVE` publication
succeeds and recovery deletion then fails or the process crashes, replay of the
stale `ASSIGNING` recovery completes against an already-`ACTIVE` provider,
advances the manager to that same target, and deletes the recovery without a
second Authority mutation or repeated Drain callbacks/cold load.
Construction-time authority reads use `ManagerOptions.InitializationContext`;
legacy nil callers receive a bounded internal timeout rather than an unbounded
`context.Background()` read.

Terminal memo IDs use a bounded exact tombstone floor. Eviction returns
`ErrTransitionMemoExpired`/fails closed rather than treating a delayed ID as a
new operation. Published terminal entries may be compacted only after an exact
tombstone is durable; pending committed recoveries remain pinned. The bounded
retention policy is deterministic and capacity exhaustion fails closed.

## Watch, gap, redirect, lease, and Drain state machines

- `NewDynamicAssignmentProvider` creates two handles over hidden shared state:
  a `DynamicAssignmentProvider` read/watch capability and one paired
  `DynamicAssignmentPublisher` write capability. The read handle implements
  `GetSnapshot`, `Watch`/`WatchFrom`, `CurrentSnapshot`, and `Snapshot`, but its
  public method set has no ordinary or authority publication API and exposes no
  way to recover the write handle for an existing provider. Composition must
  give only the read handle to data-plane/resolver/fence consumers and only the
  paired write handle to `RouteAssignmentManager`.
- `DynamicAssignmentProvider.GetSnapshot` returns a complete clone.
  `WatchFrom(ctx, revision)` emits complete snapshots only. A revision gap,
  closed/reconnected watch, or slow subscriber causes full `GetSnapshot`
  reload; no partial shard guess is installed. The write capability's ordinary
  `Publish` accepts only the same exact snapshot (silent idempotent replay) or
  the next global revision.
  Same-revision content conflicts, per-shard assignment/writer/lease rollback,
  same-generation holder/state switch, and lease deadline rollback fail closed.
- There is no exported activation proof/token and no transient or retained
  proof history. After a committed authority result, the manager alone invokes
  `DynamicAssignmentPublisher.PublishAuthoritySnapshot`; the writer validates
  exactly one initial-placeholder or committed Drain same-generation
  `ASSIGNING -> ACTIVE` transition under its publish lock. The production
  `SnapshotPublisherFunc` deliberately does not implement this method; test
  ergonomics use an explicit test-only authority publisher adapter.
- The capability split is the ownership boundary: possession of the paired
  publisher authorizes publication, while relaxed complete-reload trust is
  separately limited to the exact `*DynamicAssignmentProvider` read handle.
  Wrapping or embedding that handle does not inherit trust, because a wrapper
  can override `GetSnapshot`; `SnapshotApplier` therefore uses the trusted
  resolver install mode only for snapshots fetched/watched directly from the
  exact constructor-produced provider. Bootstrap, gap reload, and
  reconnect may therefore compare a stale `ASSIGNING` resolver directly with
  the provider's current `ACTIVE` complete snapshot, including after later
  ordinary revisions, without proof history. The install still validates
  snapshot shape, routing layout, global revision, every shard's assignment/
  writer/lease non-regression, holder/state/deadline rules, and cancellation at
  the serialized `table.Store` boundary. Public `SnapshotApplier.Apply`,
  `StaticResolver.Replace`, ordinary publisher `Publish`, and all direct
  redirect paths remain strict and cannot claim activation semantics.
- `SnapshotApplier` validates the complete old-to-new snapshot before resolver
  replacement. `NewStaticResolverFromProvider` rechecks cancellation after the
  provider returns and again before returning the constructed resolver.
  `ReplaceContext` performs the decisive cancellation check under `installMu`
  immediately at `table.Store`; snapshot apply and direct redirect therefore
  leave both the table and applied revision/counters unchanged when cancellation
  wins during validation/build/replacement. Redirects carry a higher revision and complete holder/boot/
  assignment/writer/lease/deadline data; incomplete/gapped redirects trigger a
  bounded reload or fail closed and never mark a gap applied. Existing
  `RetryingResolver` preserves the original `operation_id`.
- `FirstHopRouteValidator` checks context before resolution and immediately
  before and after Execution-Accept admission. It validates domain, actor→
  virtual shard, holder identity, holder boot, assignment revision, writer
  epoch, lease epoch via the local guard, and local lease admission before a
  mailbox/turn. A stale route redirects or fails closed; it never forwards to
  another Holder. The legacy holder-epoch callback no longer substitutes
  `context.Background`; without a cancellable context it fails closed.
- `LeaseGuardHandle` converts a trusted Authority absolute deadline once into a
  local injected deadline. Hot admission compares only local time. Same
  generation deadline rollback, same-revision boot/writer/lease/state mismatch,
  profile mismatch, expiry, and explicit Fence revoke the generation. A fenced
  generation cannot be revived by replay; Registry Remove/Replace revokes old
  handles and keeps a per-domain/per-shard monotonic floor. `withFence` derives
  admission and the returned `ShardWriteFence` from one coherent locked
  snapshot, so Fence/expiry cannot race between checks.
  `NewHolderLeaseRuntimeWithClocks` now owns construction of the frozen base
  `HolderLeaseGuard` with the same local monotonic clock, so a caller-supplied
  wall-clock guard cannot expire before the wrapper's local deadline.
- Drain is single-flight by `(RouteDomain, VirtualShard)`:

  ```text
  stop new activation/turns
      -> wait current turns
      -> confirm facts committed
      -> RouteManager Transfer (internal Drain mode)
      -> publish ASSIGNING
      -> cold-load new Holder
      -> publish ACTIVE with the same authority tuple
  ```

  Same-digest concurrent calls wait; conflicts fail closed. Side effects are
  checkpointed and a completed replay returns terminal ACTIVE without repeating
  callbacks or cold load. A newer authority generation blocks an old Drain.
  Crashes rely on lease expiry and higher WriterEpoch; no socket/memory hot
  migration, auto-balance, hotspot splitting, or Proxyless is implemented.

## Contract-to-test mapping

| Contract / finding | Direct regression evidence |
| --- | --- |
| Authority-first, Unassign zero TTL/inactive projection, exact replay | `TestRouteAssignmentManagerAuthorityBeforeSnapshotAndReplay`; `TestRouteAssignmentManagerUnassignPublishesInactiveAndReplays`; `TestRouteAssignmentManagerUnknownDoesNotPublish`; `TestRouteAssignmentManagerRejectsExpiredAlreadyAppliedLease`; `TestRouteAssignmentManagerRequiresExactAuthorityHolderBinding` |
| Read/write capability split; read/redirect cannot obtain authority publication | `TestDynamicAssignmentReadCapabilityHasNoPublicationAPI`; `TestInitialPlaceholderActivationRejectsOrdinarySnapshotPublisher`; `TestRedirectCannotClaimManagerAuthorityActivation` |
| Bootstrap only initial placeholder after authority commit; restart failed publication | `TestRouteAssignmentManagerActivationTraversesDynamicProviderAndStaticResolver`; `TestInitialPlaceholderActivationRejectsOrdinarySnapshotPublisher`; `TestRouteAssignmentManagerBootstrapOnlyInitialPlaceholderAndRestartRecovery`; `TestRouteAssignmentManagerRecoversAuthorityAfterRecoveryMarkerFailure` |
| Manager-owned bootstrap/Drain activation; ordinary publish/local Apply rejected | `TestRouteAssignmentManagerActivationTraversesDynamicProviderAndStaticResolver`; `TestSameGenerationActivationRequiresManagerWriteCapability`; `TestRedirectCannotClaimManagerAuthorityActivation` |
| Final ACTIVE replay after recovery-cleanup crash is idempotent and deletes stale recovery | `TestDrainFinalPublicationReplayIsIdempotentAfterRecoveryCleanupCrash` |
| Stale Claim/Transfer/Drain replay cannot overwrite newer authority | `TestRouteAssignmentManagerDoesNotReplayClaimOverNewTransfer`; `TestRouteAssignmentManagerDoesNotReplayTransferOverNewTransfer`; `TestRouteAssignmentManagerDrainReplayCannotPublishAfterLaterTransfer`; `TestRouteAssignmentManagerReplayWithoutAuthoritativeReadFailsClosed` |
| Committed-but-unpublished recovery, Drain checkpoint/capacity/restart | `TestRouteAssignmentManagerDrainPublicationFailureResumesAfterAssigningCheckpoint`; `TestRouteAssignmentManagerCommittedDrainRecoverySurvivesRestartAndCapacityPressure`; `TestRouteAssignmentManagerDrainSingleFlightAndReservation`; `TestRouteAssignmentManagerDrainRetentionDoesNotPrunePinnedMemo`; `TestRouteAssignmentManagerDrainReplayIsTerminalAndColdLoadOnce` |
| Pre-Authority bounded reservation/release and tombstone floor | `TestRouteAssignmentManagerReservesMemoCapacityBeforeAuthorityCommit`; `TestRouteAssignmentManagerReleasesReservationOnRejectedAndUnknownAuthority`; `TestRouteAssignmentManagerEvictedTransitionTombstoneSurvivesReload`; `TestRouteAssignmentManagerBoundsFailedPublicationRecoveryMemos` |
| Renew same generation/deadline projection and restart proof | `TestRouteAssignmentManagerSerializesDelayedRenewBeforeTransfer`; `TestRouteAssignmentManagerRenewRefreshesProjectionWithoutGenerationChange`; `TestRouteAssignmentManagerRenewAfterRestartRejectsUnprovenDeadlineRegression`; `TestHolderLeaseRuntimeRejectsRenewGenerationChangeAndFences` |
| Snapshot generation/holder/deadline/state monotonicity | `TestStaticResolverRejectsHigherGlobalRevisionPerShardRollback`; `TestStaticResolverRejectsHolderSwitchWithoutGenerationProgression`; `TestDynamicAssignmentProviderRejectsPerShardGenerationRollback`; `TestDynamicAssignmentProviderRejectsSameGenerationLeaseDeadlineRollback`; `TestDynamicAssignmentProviderRejectsSameGenerationActiveToAssigningRollback`; `TestSnapshotApplierRejectsSameRevisionWithDifferentPayload` |
| Provider gap/reconnect/full reload after missed activation and later revisions | `TestSnapshotApplierReconnectFullReloadConvergesAfterMissedActivationAndLaterRevision`; `TestTrustedProviderBootstrapAndGapReloadActivationWhileDirectRedirectFailsClosed`; `TestSnapshotApplierDoesNotTrustEmbeddedDynamicProviderOverride`; `TestSnapshotApplierRevisionGapTriggersGetSnapshot`; `TestSnapshotApplierRevisionedRunReloadsAfterReconnect` |
| Provider replay, slow watcher, cleanup/cancellation | `TestDynamicAssignmentProviderWatchFromPublishesFullSnapshots`; `TestDynamicAssignmentProviderRejectsGlobalRevisionGap`; `TestDynamicAssignmentProviderClosesSlowSubscriber`; `TestDynamicAssignmentProviderWatcherCancellationCleansWithoutPublish`; `TestStaticProviderAndSnapshotApplierRejectCanceledContextBeforeInstall` |
| Cancellation after provider return and during serialized snapshot/redirect install | `TestNewStaticResolverFromProviderRechecksCancellationAfterProviderReturn`; `TestSnapshotApplierCancellationDuringReplacementDoesNotInstallOrAdvance`; `TestDirectRedirectCancellationDuringReplacementDoesNotInstall` |
| Cancellable construction-time bootstrap/recovery authority reads | `TestRouteAssignmentManagerInitializationAuthorityReadsAreCancellable` (`bootstrap absence read`; `precommit recovery read`) |
| Redirect complete/gap reload and operation identity | `TestIncompleteRedirectReloadRequiresExactDynamicProvider`; `internal/actorrouting/redirect_test.go` redirect gap/reload and retry cases; existing bounded `RetryingResolver` tests |
| FirstHop complete tuple/cancellation race | `TestFirstHopCompletesAndValidatesFullRouteHeader`; `TestFirstHopRejectsStaleShardAndLeaseBeforeMailbox`; `TestFirstHopRejectsCanceledContextBeforeStaticResolve`; `TestFirstHopRejectsCancellationRacingExecutionAccept` |
| Lease expiry/floor/profile/domain/TOCTOU/clock | `TestLeaseGuardHandleExpiryCannotBeRevivedBySameGeneration`; `TestLeaseGuardRegistryRemoveAndReplaceRevokeOldHandleAndFloor`; `TestLeaseGuardObserveRejectsNonNewerGenerationDifferences`; `TestDynamicFenceGuardFenceCannotRaceReturnedFence`; `TestDynamicFenceSourceBindsAuthorityProfileAtGuardInstall`; `TestHolderLeaseRuntimeWithClocksUsesLocalDeadlineAfterWallJump`; `TestLeaseGuardUsesConvertedLocalDeadlineAcrossWallClockJump`; `TestDynamicFenceSourceFencesOnNewerAssignmentAndSeparatesDomains` |
| Domain holder isolation, overflow, compaction, takeover boundary | `TestRouteAssignmentManagerDomainsHaveIndependentGenerations`; `TestRouteAssignmentManagerHolderNamespaceIncludesDomain`; `TestRouteAssignmentManagerRejectsAddressAndGenerationOverflowBeforeStore`; `TestRouteAssignmentManagerCompactsUnreferencedHolderRefsDeterministically`; `TestRouteAssignmentManagerRejectsHealthyDirectTransfer` |

## Verification evidence

All current P1-closure commands below used
`GOCACHE=/tmp/farm-game-rt1-sol-fix-v9-gocache` where relevant; the
cache location is only a writable build-cache workaround.

- Focused P1 tests for capability isolation, manager-only initial bootstrap,
  identical final replay/cleanup, missed-watch reconnect, trusted Bootstrap/gap
  reload, and strict direct redirect — PASS.
- Focused preservation regex for cancellation-at-install, committed Drain
  restart/capacity pin, assignment/writer/lease/holder rollback, and injected
  local lease clocks — PASS.
- `go test ./internal/actorrouting/... ./internal/logic/actor/authority/...` — PASS.
- `go test -race ./internal/actorrouting/... ./internal/logic/actor/authority/...` — PASS; no race report.
- `go test ./...` — PASS.
- `go test -race ./...` — PASS; no race report.
- `go vet ./...` — PASS.
- `go build -buildvcs=false ./...` — PASS.
- `scripts/gen-proto.sh` — PASS; generated output unchanged.
- `git diff --exit-code -- api/gen api/proto` — PASS (zero generated/API diff).
- `git diff --check` — PASS.
- Effective task-local Git worktree status after the final commit/push — empty.

The optional live probe was run as:
`go test -count=1 -run Live -v ./internal/storage/redis/authority/...`.
It passed with `TestLiveAuthorityRoundTrip` explicitly **SKIPPED** because
`FARM_REDIS_URL` was not set. Existing miniredis/component tests are not
company Redis evidence.

## Pending platform facts and ICR

1. **Production durable recovery adapter (ICR):** the frozen public
   `ShardWriteAuthorityStore` has no transition recovery/tombstone persistence
   API. RT1 therefore adds the minimal manager-owned `TransitionRecoveryStore`
   and requires it at manager construction. `MemoryTransitionTombstoneStore`
   proves component semantics only. I0/operations must provide a bounded,
   durable implementation with deterministic replay/retention and an
   authoritative current-shard reader. No public storage contract was edited.
2. **Distributed publisher/watch:** no company control-plane transport is
   available in this worktree. I0 must place a distributed full-snapshot
   publisher/watch behind the RT1 provider/recovery seams and preserve the
   contiguous revision/full reload rules.
3. **Composition/wiring:** `cmd/**`, `deploy/**`, Player/Farm Registry wiring,
   Gate/Logic FirstHop wiring, rollout/rollback, and legacy→authority cutover
   remain intentionally deferred to I0. The legacy holder-epoch callback is
   fail-closed until I0 supplies a context-aware path; this is not claimed as
   production wiring.
4. **Real platform evidence:** company Redis ACL/Cluster/failover, Lua numeric
   overflow/double-precision behavior, distributed publisher/watch behavior,
   and real takeover-proof source are **PENDING**. No live result is faked and
   miniredis is not promoted to production evidence.
5. The frozen IDL omits `lease_epoch` from `ActorRouteHeader`; RT1 carries that
   fence in local HolderLeaseGuard/`ShardWriteFence` and does not add an
   `ExpectedLeaseEpoch` header field. No contract change is requested.

## Write set and commits

Touched only the RT1 route-authority/runtime packages and this handoff:

- `internal/actorrouting/**` (manager, provider, resolver/redirect seams and
  direct tests)
- `internal/logic/actor/authority/**` (DynamicFenceSource/lease runtime and
  direct tests)
- `docs/verification/rt1-minimal-route-authority-control-handoff.md`

No `api/proto/**`, `api/gen/**`, `internal/storage/**`, `cmd/**`,
`deploy/**`, Player/Farm/Friend/Workflow actor package, H5, integration merge,
MR, or dual-write path was touched.

Prior RT1 history is retained through the immutable P1-closure starting tip
`29d2d3ecee0ba4ae9b4247139c5216f93fdf777a`, including the earlier
`942a410db1275242d858d8bfd272261e5bd22095` code/test commit. This P1 closure
adds:

- P1 closure fixes: manager-owned read/write provider capabilities, proof-free
  self-contained trusted full reload, identical-target replay before activation
  validation, and stale Drain recovery cleanup after an already-installed final
  `ACTIVE` publication
- delivery commit: contains code, tests, and this final handoff; its exact
  immutable hash is the final delivery tip reported by the writer
- immutable pushed remote tip: the exact fast-forward `ls-remote` result is
  reported by the writer after push

The sandbox worktree uses a read-only external Git administrative directory;
if direct ref/index writes remain unavailable, the same worktree content is
committed and pushed through a task-local administrative clone under `/tmp`.
The effective worktree status used for delivery is recorded as clean after the
final push. No integration branch was changed.
