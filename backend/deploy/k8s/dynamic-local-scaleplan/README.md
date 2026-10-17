# Dynamic-local ScalePlan A7 automatic rebalance and holder recovery profile

This directory documents the render-only Kubernetes contract for automatic
PLAYER/FARM Shard rebalance. It does not apply anything to a cluster and does
not replace `static-staging/base`.

## Runtime modes

- `SCALEPLAN_MODE=off` remains the default. No ScalePlan RBAC is rendered and
  the Route Controller does not mount a Kubernetes ServiceAccount token.
- `SCALEPLAN_MODE=manual` retains the explicit ScalePlan API and Kubernetes
  holder discovery from A5.
- `SCALEPLAN_MODE=auto` adds informer-driven reconciliation. Pod events are
  only wakeups: they never mutate the route table directly.

`manual` and `auto` use the dedicated `scaleplan-controller` ServiceAccount.
Its namespaced Role is limited to Pod/StatefulSet discovery and the
StatefulSet scale mutation used by a durable ScalePlan. TokenReview has a
separate one-verb ClusterRole. Player and Farm holders keep no-RBAC service
accounts and read-only projected identity tokens.

## Automatic scale-out

Two paths are supported:

1. Set `scaleplan.farm/safe-desired-replicas=N`. The controller creates a
   durable +1 plan, patches the StatefulSet, waits for the new Pod to become a
   verified `READY_ZERO_SHARD` holder, then moves the theoretical minimum
   number of Shards.
2. Add exactly one StatefulSet replica directly. The informer discovers the
   new Pod; after Kubernetes Ready plus HolderControl identity verification,
   the controller adopts the already committed +1 and runs the same Shard
   movement state machine without issuing a second Kubernetes patch.

The first policy balances by Shard count with deterministic tie-breaking. It
does not guess heat. PLAYER and FARM are independent route domains and can
scale independently.

Scale-out requires the existing holders to be count-balanced (difference at
most one). Under that precondition, moving only `floor(total/new_replicas)`
Shards to the new ordinal is both balanced and movement-minimal. A topology
already skewed by a future heat policy fails closed and requires a separate
global/heat rebalance; this first contract never claims an unbalanced result
is complete.

## Safe scale-in

Do not directly decrement `spec.replicas`: StatefulSet may delete its highest
ordinal before RouteControl has drained its Shards. Instead set the safe
desired annotation to the lower target:

```text
metadata.annotations["scaleplan.farm/safe-desired-replicas"] = "N-1"
```

The automatic controller repeatedly performs one safe step:

```text
exclude highest ordinal from new placement
→ capture its complete Shard set
→ persist and execute one-Shard RoutePlans
→ verify writer_epoch / Lease handoff
→ confirm READY_ZERO_SHARD
→ patch StatefulSet replicas -1
→ verify the Pod disappeared
```

The safe desired annotation is a one-shot control intent. Once the exact
target is reached, RouteController removes it with UID, resourceVersion, and
old-value JSON Patch preconditions. A concurrent replacement intent is never
deleted, and a later direct `replicas +1` is not mistaken for a request to
shrink back to a stale target.

If the highest ordinal is already zero-Shard, the same durable exclusion and
verification path is used with an empty move manifest; Kubernetes is still
not patched directly.

## Safe StatefulSet version rollout

Managed rollout is opt-in and requires automatic ScalePlan mode:

```text
SCALEPLAN_MODE=auto
SCALEPLAN_ROLLOUT_ENABLED=true
SCALEPLAN_ROLLOUT_ANNOTATION=scaleplan.farm/managed-rollout-state
```

When enabled, the PLAYER and FARM StatefulSets are rendered with `OnDelete`.
Kubernetes still creates and health-checks Pods, but it no longer deletes an
old holder generation before RouteControl has moved its Shards. A template
revision mismatch starts this restartable sequence independently per route
domain:

```text
N old holders
-> safe scale N to N+1
-> wait for the spare Pod at the target revision and verify HolderControl
-> choose the highest old-revision ordinal
-> persist DRAIN_HOLDER (replica count unchanged)
-> exclude the exact old BootID and move every Shard
-> prove READY_ZERO_SHARD and no active placement remains
-> delete that exact Pod with a Pod UID precondition
-> wait for the same ordinal to return with target revision, new PodUID and new BootID
-> repeat one old Pod at a time
-> safe scale N+1 back to N
```

The StatefulSet annotation contains only the rollout checkpoint. Every Shard
move remains owned by the existing durable ScalePlan/RoutePlan records. If the
controller restarts between drain and delete, it resumes the same drain PlanID;
if the target revision changes mid-rollout, replica count drifts, or another
plan owns the workload, the rollout becomes `BLOCKED` instead of guessing.

This does not require holders to self-register. The controller discovers Pod
identity and revision from Kubernetes and verifies the game process with the
existing HolderControl RPC. Rollout RBAC adds only exact Pod deletion and
StatefulSet update to the automatic controller role.

## Actor behavior during migration

P0 uses cold Actor migration. Strong facts are persisted before route
handoff. PlayerActor lazily loads on the next command. FarmActor lazily loads
on a fresh Join; subscriptions affected by a Farm Shard move receive the
existing rejoin/recovery semantics. Presence and JoiningBuffer are runtime
state and are not copied between Pods.

## Automatic holder-failure recovery

Kubernetes discovery and RouteControl have deliberately separate jobs:

- the Pod informer discovers Pod creation, deletion and Ready changes;
- HolderControl verifies the running process identity (`boot_id`) and whether
  it can accept Shards;
- RouteControl remains the only component allowed to change Shard ownership,
  `writer_epoch` and assignment revision.

The holders do not register into a second service-discovery system. The
controller rebuilds its verified holder view from Kubernetes plus the
HolderControl handshake.

When a route snapshot still points to a holder generation that is no longer
in the verified view, the controller does not immediately rewrite routing:

```text
missing exact holder generation
-> compare the durable ShardAuthority record
-> while the old Authority lease is live: wait
-> after expiry: create a deterministic CrashTakeover RoutePlan
-> exact-fenced expired takeover CAS increments writer_epoch
-> publish a newer RouteSnapshot
-> the target Pod cold-loads Actors on demand
```

If Kubernetes reports the same StatefulSet holder identity with a new
`boot_id`, the old process generation is already disproved. RouteControl can
use the stricter holder-unavailable takeover path without waiting for the old
lease deadline. In both paths, a recovered old process still carries the old
`writer_epoch`, so Redis rejects its late writes.

PLAYER and FARM are reconciled independently. Within one controller process,
automatic scale reconciliation and automatic failure recovery share the same
mutation lock, preventing them from submitting competing route changes.
Each recovery pass is bounded by both a RoutePlan limit and an exact Authority
read limit, so one large failure cannot monopolize the controller.

## Recovery and safety

- The parent ScalePlan and every child RoutePlan are durable and idempotent.
- On restart, route-controller recovers nonterminal plans and seeds each
  domain controller with the same PlanID before automatic reconciliation.
- PLAYER and FARM controllers share one local reconciliation lock because the
  repository mutation lease is realm-scoped.
- Assignment changes are revalidated immediately before plan submission.
- A direct unsafe external scale-in is detected when an active placement
  points to an ordinal no longer present in `spec.replicas`; it is reported and
  never treated as a safe migration.

The dynamic renderer accepts `off`, `manual`, and `auto`, and emits:

- `SCALEPLAN_AUTO_RECONCILE_INTERVAL=5s`;
- `SCALEPLAN_SAFE_DESIRED_ANNOTATION=scaleplan.farm/safe-desired-replicas`;
- `SCALEPLAN_ROLLOUT_ENABLED=false` by default;
- `SCALEPLAN_ROLLOUT_ANNOTATION=scaleplan.farm/managed-rollout-state`;
- `ROUTE_FAILOVER_RECONCILE_INTERVAL=1s`;
- `ROUTE_FAILOVER_MAX_PLANS_PER_PASS=32`;
- `ROUTE_FAILOVER_MAX_AUTHORITY_READS_PER_PASS=128`.

Prometheus metrics expose pass outcomes, completed takeover plans, exact
Authority reads, missing Shards, lease-waiting Shards and no-target Shards.
Pure render verification is provided by
`scripts/local-k8s/test_dynamic_route_profile.py`.
