# Q1 Postmerge Qualification and Formal Capacity Train

This document is the durable execution order after `CP-Q1-SCOPE-FILTER-CORRECTNESS` enters Backend and H5 integration. It prevents the retained 10/10 correctness diagnostic from being mislabeled as a performance baseline.

## Immutable classification of existing evidence

```text
purpose           = correctness_diagnostic
baseline_eligible = false
supports_30m_dau  = false
```

The old 10/10 runs prove the CARE/THEFT scope closure. They are never renamed or promoted to a formal capacity baseline.

## Global execution order

```text
M0  Q1-POSTMERGE-QUALIFICATION
C1  CAP-REDIS-L1
C2  CAP-SINGLE-POD-L2
B0  CAP-MIXED-B0 (three repeatable rounds)
B1  CAP-STAIR-B1
H1  CAP-HOTFARM-H1
F1  CAP-FAULT-F1
S1  CAP-SOAK-S1 (two hours, then optionally 24 hours)
R1  RT2-ROUTE-R1 only after the dynamic control plane closes
A1  AS-AUTOSCALE-A1 last
```

No capacity conclusion may skip M0. R1 and A1 remain blocked until their stated control-plane prerequisites close.

## Evidence identity required for every postmerge run

Freeze before the test window:

- `backend_integration_commit`
- `h5_integration_commit`
- `backend_image_digest`
- `h5_image_digest`
- `server_config_hash`
- `load_profile_version` and profile hash
- `fixture_id`
- deterministic `random_seed`
- Redis physical/profile version and endpoint hash, never credentials
- unique namespace
- host/machine specification hash
- planned target load and arrival model
- actual test start/end UTC

Record planned and actual separately: target arrivals, attempted/injected arrivals, scheduled-but-unsent, completed, rejected, timeout, and load-generator saturation. Missing identity or observability makes the run `INCONCLUSIVE`, never PASS.

## M0 — Q1-POSTMERGE-QUALIFICATION

Build new Backend and H5 images from the exact postmerge integration heads. Do not reuse binaries or `dist/` from the Writer or Premerge worktrees.

Run one to three rounds:

- Backend full tests and focused race tests
- H5 unit tests, `tsc --noEmit`, production Vite build
- Browser + Go Protocol Bot + Authority Oracle
- F1, F2, and F3
- legacy CARE Due upgrade compatibility
- Harness normal and negative controls

Required result each round:

```text
F3_CHAIN_PASS
InvalidOrders = 0
physical Pair Due residual = 0
unmatched Pair Due residual = 0
CARE CareCompleted = 2
first-click Wire THEFT = 0
second-click Wire THEFT = 1
hard correctness incidents = 0
```

Freeze `CP-Q1-POSTMERGE-QUALIFICATION` only after the exact postmerge combination passes. Any failure blocks all eligible performance work.

## C1 — CAP-REDIS-L1

Measure Redis and serialization independently before distributed capacity claims. Include:

- Actor LoadAndClaim and Commit
- Receipt/Order/Due Commit
- Workflow Create/Advance/Claim
- Pair ActionAdmission and Pair Due
- FriendIndex Batch Read
- ResultDirectory
- SessionRoute Renew
- Subscription Renew
- Shard Lease Renew

For each operation record request/response bytes, Lua name and amplification count, Redis Lua P50/P95/P99, client total latency, Redis CPU, pool wait, pipeline batch size, hot-key and uniform-key cases, and concurrency `1/10/100/300`. Include `NOSCRIPT` and timeout-unknown behavior.

The primary output maps each business command to Redis call count, bytes, scripts, and worst observed latency. Business writes/s must never be presented as Redis ops/s.

## C2 — CAP-SINGLE-POD-L2

Measure four services separately at a safe operating point: CPU roughly 60–70%, stable P99, no growing queue, and zero correctness violations.

### farm-gate

Stable WSS, Login/Ticket/Handshake per second, Ping/Pong, inbound commands, outbound frames/bandwidth, ConnectionWriter queue, slow-client eviction, SessionRoute/Subscription renew, and drain reconnect.

### player-svr

Hot PlayerActor turns/s, cold activation/s, active actors/pod, memory/actor, mailbox wait, Buy/Sell/TaskClaim, participant turns, passivation/reload.

### farm-svr

Join/Snapshot/s, hot/cold FarmActor turns/s, Plant/Harvest/Care/Theft, FarmChange fanout, active farms/pod, hot-actor mailbox.

### farm-worker

Work/s, Due Claim/s, backlog depth, oldest Due age, retry/s, Direct Dispatch miss takeover, CARE/THEFT family isolation, and recovery after Redis latency.

Outputs:

```text
safe_connections_per_gate
safe_player_turns_per_pod
safe_farm_turns_per_pod
safe_worker_steps_per_pod
```

## B0 — CAP-MIXED-B0

This is the first `baseline_eligible=true` train, only after M0/C1/C2.

Use an open-arrival model for Login/WSS, Bootstrap, Join, Friend List/Opportunity, TaskList, Plant/Harvest/Care/Theft, Operation Result Polling, FarmChange fanout, and Lease Renew. Service slowdown must not reduce the scheduled injection rate invisibly.

Run identical `B0-R1`, `B0-R2`, and `B0-R3` with the same commits, images, config, fixture, load mix, and host class. Record target/actual/unsent/completed/rejected/timeout, latency P50/P95/P99, resources, queues, and all hard gates. A saturated generator or incomplete observation is `INCONCLUSIVE`.

## B1 — CAP-STAIR-B1

After B0, run suggested levels `40/60/75/90/100/120%` (organization guidance, not production thresholds). Identify:

- safe capacity line
- P99/queue performance knee
- explicit overload line

Observe QPS linearity, first sustained queue, Redis pool wait, GC, activation storms, and recovery after load drops. Overload must produce bounded rejection/rate limiting/RetryDirective, not unbounded queues followed by mass timeout.

## H1 — CAP-HOTFARM-H1

Run in order:

1. one farm × 200 distinct users
2. ten farms × 200 users
3. optionally 100 and 1000 farms × 200 only after evidence permits

The 200-user definition is one owner plus 199 distinct visitors. Observe mailbox depth/wait, Hub fanout, VersionRing, Gate lanes, slow readers, concurrent Care/Theft, FarmVersion continuity, and hot Redis-key bandwidth.

Inject one non-reader, five slow readers, and random read pauses. Slow clients may block only their own ConnectionWriter, never the FarmActor, Hub, or healthy clients.

For simultaneous Theft from a plot with ten stealable units:

- total successful decrement ≤ 10
- remaining amount ≥ 0
- each operation settles at most once
- visitor credit + guard interception = farm decrement
- FarmVersion is continuous and all clients converge

## F1 — CAP-FAULT-F1

Inject faults under a stable mixed load, not while idle:

- Gate rolling update and hard kill
- Worker kill/pause
- Redis latency, timeout, `NOSCRIPT`, and network partition
- Downlink unavailable and response drop
- client RST and weak network

Verify old connections stop admission, Fresh Join recovers, original operation IDs remain queryable/retryable, workflows finish without duplicate rewards, Pair Due returns to zero, FarmVersion has no gap, and recovery traffic does not storm.

## S1 — CAP-SOAK-S1

After short tests pass, run two hours with stable mixed load, a small real Playwright witness set, and Authority Oracle. Track trends for RSS/heap, goroutines, fd, active actors, mailboxes, timers, gRPC/Redis connections, Receipt/Order/Due, pending journals, stale presence, route/lease drift, and P99.

Investigate monotonic growth, failed passivation, timer leaks, pool growth, slow Due accumulation, and latency degradation. A 24-hour run is later production-grade evidence, not a substitute for the two-hour qualification.

## R1 — RT2-ROUTE-R1 (blocked)

Do not run until RT2D review closure, RT2I composition, RT2M manual migration, RT2Q final multi-Pod evidence, and final G1/G2/G3 are complete.

Then test under load: Watch reconnect, Shard A→B drain, controller restart, crash between Authority switch and Snapshot publication, crash takeover, old-holder storage rejection, and Fresh Join. Reconcile rewards, inventory, and FarmVersion after migration.

## A1 — AS-AUTOSCALE-A1 (last)

Only after dynamic Watch, manual A→B migration, and crash takeover pass:

1. AS0 capacity metrics
2. AS1 dry-run autoscaler
3. AS2 controlled scale-out (`2→3`, new Pod Ready with zero shards before bounded migration)
4. AS3 drain-aware scale-in (`3→2`, victim DRAINING and zero shards before deletion)

Do not apply blind HPA to Player/Farm holders before Dynamic Assignment is proven.

## Hard correctness gates for every performance run

All must remain zero:

```text
duplicate_grant_total
duplicate_debit_total
stale_holder_commit_success_total
farm_version_gap_total
workflow_step_unknown_residual
due_residual
h5_pending_journal_residual
authority_client_mismatch_total
silent_drop_total
unexpected_lease_expiry_total
friend_pair_index_unrepaired
```

Any nonzero hard gate immediately invalidates all performance numbers and marks the run `FAIL_CORRECTNESS`. Performance and correctness are never traded against each other.
