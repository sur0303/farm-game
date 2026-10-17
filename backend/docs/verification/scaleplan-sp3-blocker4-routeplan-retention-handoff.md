# ScalePlan SP3 blocker 4 — exact RoutePlan Get and retention alignment

## Scope and identity

```text
worktree=farm-game-scaleplan-sp3-protocols-20260812
branch=codex/scaleplan-sp3-protocols-20260812
base=867503355f554509d65f4b505f13b917b23655e8
protocol_blocker=ROUTEPLAN_EXACT_GET_AND_RETENTION_ALIGNMENT
reviewed_diff_sha256=7a620a0ca9904bc76420f9755297a49793cc94175ddf8e93f7c1e95ac60e3be7
verdict=PASS
P0=0
P1=0
P2=0
required_fixes=0
```

This closes protocol blocker 4 only. It does not implement or authorize a
ScalePlan production adapter, Kubernetes scaler, deployment, runtime activation
or performance qualification. The frozen G21 runtime identity remains
`2f16f7c6db3f6e9d8ecb76513cbdf3bd27724361`.

## Exact terminal Get contract

Authorized RoutePlan readers now have an exact `GetPlan(plan_id)` RPC and the
matching `routectl plan --plan-id` command. Exact reads use one visible-ASCII,
no-whitespace, at-most-128-byte PlanID grammar across service, memory store,
Redis key construction and CLI. A valid absent or physically expired ID returns
`NotFound`; malformed IDs return `InvalidArgument`.

`GetPlan` reads the exact durable record, including `SUCCEEDED`, `FAILED` and
`PAUSED` plans, while physical evidence remains retained. It does not rely on
the incomplete-plan list.

## Immutable child identity

ScalePlan-owned planned-drain RoutePlans persist and freeze:

```text
source = HolderID / BootID / PodUID / WorkloadUID / Ordinal / endpoint
target = HolderID / BootID / PodUID / WorkloadUID / Ordinal / endpoint
expected source Authority fence
requested RoutePlan TTL
owner ScalePlan ID
owner RecoveryRetainUntilMs
```

The deterministic ScalePlan child payload digest covers the same request.
Exact generations are revalidated before side effects, at every drain callback,
in the production controller binding and in pending projection/cold-load
recovery. The Manager durable recovery record carries the complete exact tuple
and a v2 identity-bound fingerprint. Legacy/manual planned drain and crash
recovery remain explicit non-exact compatibility paths and do not claim
Kubernetes identity.

## Retention and migration

Production RoutePlan audit retention is 38 days. This covers the maximum
ScalePlan start plus recovery horizon:

```text
7d MaxPlanTTL + 30d MaxRecoveryRetention = 37d
```

A child physical expiry must not precede its parent
`RecoveryRetainUntilMs`. Late child creation remains valid when the parent
retention deadline is earlier than the child logical expiry.

Redis bounded reconciliation accepts valid prior 24-hour scores, validates the
complete set before mutation, then atomically extends every retained score and
all shared TTLs to the configured 38-day horizon. Corruption remains zero-write.
Canonical RoutePlan v1 bytes and physical 8-field records remain readable and
upgrade safely to canonical v2 / physical 11-field records. Existing non-exact
Manager recovery JSON and v1 fingerprints remain byte-compatible; exact
planned-drain recovery uses a v2 fingerprint covering all six deployment
identity values.

## Post-observation ownership safety

After a child has been observed, missing, mismatched, malformed, failed,
regressed or verification-inconsistent evidence in either MovingWave or
VerifyingWave durably transitions the parent to `BLOCKED`. The workload lock is
retained. Releasing that ownership requires the separate blocker 5 proof
protocol, which remains unimplemented.

## Verification

Formal performance lock was absent before Go commands. Local/in-process checks
passed:

```text
go test ./...=PASS
affected packages 50x=PASS
affected packages race=PASS
affected packages vet=PASS
git diff --check=PASS
Redis tests=in-process miniredis only
independent final review=PASS P0/P1/P2=0/0/0
```

No external Redis, Kubernetes, network qualification, deployment, performance
sampling or capacity run was performed.

## Remaining state

```text
blocker_1_SERVING_TO_READY_ZERO_SHARD_LIFECYCLE=PASS
blocker_2_DURABLE_MANAGER_WIDE_PLACEMENT_EXCLUSION=PASS
blocker_3_POD_WORKLOAD_ORDINAL_BOOT_IDENTITY_BINDING=PASS
blocker_4_ROUTEPLAN_EXACT_GET_AND_RETENTION_ALIGNMENT=PASS
blocker_5_BLOCKED_TERMINATION_QUIESCENCE_AND_PROOF_OWNERSHIP=OPEN
SP3_0=FAIL_BLOCKED
adapter_implementation_authorized=false
ready_for_sp3=false
supports_30m_dau=false
```

Repository actions at handoff preparation:

```text
push=NOT_PERFORMED
merge=NOT_PERFORMED
rebase_integration=NOT_PERFORMED
tag=NOT_PERFORMED
```
