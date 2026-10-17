# Dynamic-local Route Control G1/G2/G3 profile

This is a sibling of `static-staging/overlays/local-capacity-r2`; it does not
replace or mutate the 2048-shard static liveness profile.

The materialized runtime contract is:

- namespace `farm-route-dynamic`;
- Route Controller: one replica, `Recreate`, no overlap;
- Player holders: two; Farm holders: two;
- Worker: two; Gate: one; H5: one;
- four PLAYER and four FARM shards;
- `AUTHORITY_MEMBERSHIP_MODE=composed` and
  `ACTOR_ASSIGNMENT_MODE=remote-watch` for every Go consumer;
- `ROUTE_CONTROLLER_LEASE_PROJECTION_MODE=disabled` so a legitimate dynamic
  `ASSIGNING` phase is not judged by the static all-ACTIVE contract;
- one local CA with exact single-DNS-SAN identities for route Watch/Plan,
  HolderControl, and Gate Verification;
- first holder process BootID binds Pod UID; each same-Pod process restart gets
  a fresh random UUID. Pod recreation gets a fresh UID;
- local ephemeral Redis with a fresh run-scoped exact-key namespace;
- NetworkPolicy enforcement is intentionally `NOT_CLAIMED` for this gate.

Materialization and application are performed by:

```text
scripts/local-k8s/materialize-dynamic-route-profile.py
scripts/local-k8s/apply-dynamic-route-profile
```

The apply script starts Holder Pods to obtain their Pod UIDs, bootstraps a
durable zero-shard Snapshot, and starts one initial Controller so all four
holders register `READY_ZERO_SHARD`. It then stops that Controller completely,
runs the external bootstrap Manager to claim the initial 4x4 generations, and
starts a fresh final Controller Boot which loads revision 9. The three Controller
Boot identities are distinct and no processes overlap. It never uses Redis
`KEYS`, global `SCAN`, `SCRIPT FLUSH`, or company Redis.

Acceptance gates:

- **G1 Remote Watch:** every consumer becomes ready on the same complete
  Snapshot revision; local Resolve remains process-local.
- **G2 Planned Migration:** selected PLAYER and FARM shards complete
  A→B→A→B with Drain, monotonic fences, and no overlapping writer.
- **G3 Crash Takeover:** an unavailable source cannot transfer without explicit
  proof; after proof/expiry, the target alone receives the next generation.

This profile is functional verification only. It is not capacity qualification,
HA, NetworkPolicy enforcement evidence, Integration approval, or autoscaling.

## A6 Kubernetes discovery and automatic rebalance boundary

The independent ScalePlan render is controlled by `SCALEPLAN_MODE`, which
defaults to `off`. In `off` mode the rendered profile has no ScalePlan
ServiceAccount, Role, RoleBinding, ClusterRole, or ClusterRoleBinding, and the
Route Controller explicitly sets `automountServiceAccountToken: false`.

`manual` and `auto` default to `HOLDER_DISCOVERY_MODE=k8s-canary`. They add only a
dedicated `scaleplan-controller` ServiceAccount, a namespaced Role/RoleBinding
with exact Pod/StatefulSet discovery and scale verbs, and a separate
TokenReview-only ClusterRole/ClusterRoleBinding. Player and Farm holders use
separate no-RBAC ServiceAccounts with `automountServiceAccountToken: false`.

The holder Pods render a read-only projected token with audience
`classic-farm-holder-control` and `expirationSeconds: 600`. HolderControl reads
the rotating token for `GetRuntimeIdentity`; Route Controller verifies the
exact Pod UID, StatefulSet UID, ordinal, process `boot_id`, mTLS identity and
TokenReview bound claims before admitting a holder to `VerifiedHolderView`.

The informer only discovers and wakes reconciliation. It does not assign
Shards. In `auto` mode, a verified direct `+1` is adopted automatically and a
deterministic minimum-movement manifest is persisted before RouteControl
Drain/Handoff. A safe requested target is carried by the
`scaleplan.farm/safe-desired-replicas` annotation; scale-in drains the highest
ordinal to zero Shards before reducing StatefulSet replicas. Direct external
`spec.replicas -1` is intentionally not considered safe.

The `scaleplan-config` ConfigMap freezes the exact `PLAYER`/`FARM` StatefulSet
allowlist, realm, public domain, and SHA-256 config hash. It is independent of
`static-staging/base` and does not authorize any apply or Kubernetes runtime
action.
