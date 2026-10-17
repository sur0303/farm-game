# WR2 — SC1 Plant recovery handoff

Status: `IMPLEMENTED / PRODUCER-MIGRATION-PENDING`

WR2 wires `farm-worker` to the existing generic `workflow.WorkRepository` and
`workflow.RecoveryLoop` for SC1 `PLANT` work. No second workflow runtime,
protocol, Actor handler, or Redis key family was added.

## Composition

`internal/worker/plantrecovery` is the testable composition root used by
`cmd/farm-worker`:

```text
Redis client
├── legacy RedisStore ({workflow}) ──> LegacyRepository split ──> RecoveryLoop
└── RedisWorkRepository ({work-v1}) ──> Plant claim-timing view ────> RecoveryLoop
                                             ├── WorkerRecoveryInvoker(Player)
                                             └── WorkerRecoveryInvoker(Farm)
                                                  └── ResumeStep
```

The Plant handler creates the SC1 `plant.Definition` and generic
`workflow.Engine` for the current claimed record. This is intentional: the
worker lease epoch changes on takeover, while the SC1 definition remains the
same. Both invokers receive the current `LeaseFence.Owner`/epoch, the configured
per-boot lease token, and boot identity.

## Lease, replay, and deadline behavior

- `RecoveryLoop` claims through the Redis-backed generic repository with the
  configured owner, token, bounded claim count, TTL, and safety margin.
- The Plant composition records a local monotonic bound immediately before
  each bounded `ClaimDue` call. Every returned Plant record gets its own
  `claim-call-start + accepted_ttl - safety_margin` deadline, which is no
  later than that record's actual claim-start safe deadline. The Plant engine
  combines this absolute bound with the loop check before every new step, so
  a later item in a batch cannot start `ResumeStep` after its original safe
  deadline.
- SC1 `WorkerRecoveryInvoker` always calls generated `WorkerRecoveryIngress`
  `ResumeStep`; it never uses the Coordinator `ExecuteStep` path.
- `Engine.Resume` rebuilds from the persisted `StepRecord`, verifies the
  frozen payload hash, and reuses the existing invocation ID and payload bytes
  on response loss, restart, and takeover.
- The generic engine receives `lease.Expired` as its stop function. After the
  conservative local deadline it may persist an already-resolved result, but it
  does not start another participant step.

## Ownership split

The old `{workflow}` due index contains legacy records from all old scenarios,
including legacy Plant records. Its adapter now explicitly claims only
`LEGACY_HARVEST`, `LEGACY_THEFT`, and `LEGACY_CARE` for the legacy Runner.
`LEGACY_PLANT` records are released at their accepted lease expiry and remain
pending; no legacy handler is registered for them. Generic `PLANT` records are
owned only by the `{work-v1}` Plant loop. This prevents dual processing while
the online Plant producer and any already-created legacy Plant records migrate
to SC1 generic creation.

The split performs a bounded extra scan of the mixed legacy queue so a due
legacy Plant record cannot permanently starve the three compatibility families.
The legacy backlog gauge can still include quarantined legacy Plant records;
that is an explicit migration signal, not permission to process them.

## Routing and participant proof

Player steps resolve `PLAYER:player_id` and Farm `COMMIT_PLANT` resolves
`FARM:farm_id`. Separate endpoint maps reject cross-domain use. Each request
gets the current static route header and the existing route metadata. A stale
route fails the invocation; the durable retry re-resolves the route while
retaining the same operation/work identity, invocation ID, and payload. No
route calculator or Redis lookup is added to an Actor turn.

The composition passes the generated Worker Recovery clients to SC1’s existing
RPC adapter. PC1’s fail-closed repository-backed WorkAddress, WorkVersion,
identity, lease, and Plant dependency proofs remain in the Participant ingress
before mailbox admission. `ACTOR_ROUTE_SNAPSHOT` and endpoints serving the
generated `WorkerRecoveryIngress` services are therefore required for the
generic Plant owner path. Without them, farm-worker runs the legacy
non-Plant loop only and logs Plant recovery as fail-closed.

## Metrics

The composition registers bounded family labels only:

```text
farm_workflow_recovery_claimed_total{family="PLANT|LEGACY"}
farm_workflow_recovery_completed_total{family="PLANT|LEGACY"}
farm_workflow_recovery_retried_total{family="PLANT|LEGACY"}
farm_workflow_recovery_lease_lost_total{family="PLANT|LEGACY"}
```

No workflow ID, operation ID, payload, or arbitrary error text is used as a
metric label.

## Focused evidence

`internal/worker/plantrecovery/recovery_test.go` covers:

- exact `ResumeStep` invocation/payload reuse after response loss and worker
  takeover, including changed lease owner/token/epoch;
- conservative lease deadline stopping the next Plant step;
- ClaimLimit>1 batch processing with a delayed earlier item proving a later
  Plant record sends no participant request after its original safe deadline;
- mixed legacy due-queue ownership, with HARVEST recovered and legacy PLANT
  left pending;
- family-bounded recovery counters.

The current WR2 change set is limited to `cmd/farm-worker`, the new
`internal/worker/plantrecovery` package, and this handoff. It does not migrate
the existing online legacy Plant producer or add Participant service
registration to `player-svr`/`farm-svr`; those remain the next integration
seams under the stated command/handler write restrictions.

## Verification

- `go test -count=1 ./...` — PASS
- `go test -race -count=1 ./...` — PASS
- `go vet ./...` — PASS
- `go build -buildvcs=false ./...` — PASS
- `git diff --check` — PASS (using the repository's required safe-directory
  configuration)

The plain `go build ./...` also ran, but Go could not obtain VCS status from
this workspace's ownership-restricted Git checkout; the no-VCS build above
validates compilation independently of that environment issue.
