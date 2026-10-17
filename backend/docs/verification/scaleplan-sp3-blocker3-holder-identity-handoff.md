# ScalePlan SP3 blocker 3 — exact Holder deployment/process identity

## Scope and identity

```text
worktree=farm-game-scaleplan-sp3-protocols-20260812
branch=codex/scaleplan-sp3-protocols-20260812
base=b16c0c8f0ab495d7c60a71566cbb9aec84238b3c
protocol_blocker=POD_WORKLOAD_ORDINAL_BOOT_IDENTITY_BINDING
verdict=PASS
P0=0
P1=0
P2=0
required_fixes=0
```

This closes protocol blocker 3 only. It does not implement a ScalePlan
production adapter, authorize SP3 runtime activation, qualify Kubernetes or
Redis, or change the frozen G21 runtime identity
`2f16f7c6db3f6e9d8ecb76513cbdf3bd27724361`.

## Frozen protocol

A dynamic Holder process identity is:

```text
Domain / HolderID / BootID / PodUID / WorkloadUID / Ordinal
```

The process must explicitly select:

```text
ACTOR_ASSIGNMENT_MODE=remote-watch
ACTOR_HOLDER_IDENTITY_MODE=kubernetes-process-v1
```

It must supply a complete PodUID, WorkloadUID and canonical non-negative int32
ordinal. A configured `ACTOR_HOLDER_BOOT_ID` is forbidden. One cryptographically
random 128-bit BootID is generated before authority construction and then shared
unchanged by authority fences, HolderControl and HolderAgent registration.
HolderAgent caches the resolved Boot across Start retries.

Static compatibility remains a separate zero-deployment-identity path with its
reviewed deterministic Boot. Static manifests and frozen G21 evidence were not
rerun or requalified.

PodUID and WorkloadUID use the exact same visible-ASCII byte grammar in Go and
Redis Lua: every byte is `0x21..0x7e`, with a maximum of 128 bytes. This avoids
UTF-8, Unicode whitespace and normalization ambiguity at the mutation boundary.

## Durable and RPC protocol

Redis Holder storage is isolated from the prior generation:

```text
keyspace=holders:v2
record_schema=holder-record-v5
meta_schema=holder-registry-v5
digest_wire=HD5
record_wire=HR5
register_script=RegisterHolderV6
heartbeat_script=HeartbeatHolderV6
```

PodUID, WorkloadUID and ordinal are immutable registration fields covered by
semantic digest, record wire bytes, record integrity, replay comparison and
aggregate accounting. Heartbeats preserve them. Lua validates identity, int32
ordinal, candidate semantics and limits before any mutation.

Authorized `GetHolders` now projects PodUID, WorkloadUID, ordinal, an explicit
presence bit and HolderControl endpoint in addition to the existing exact
Holder/Boot/liveness facts.

## Trust boundary

The durable Holder fields are claims, not Kubernetes authority. A future
production `HolderStateReader` must independently capture and compare:

- live Pod UID;
- canonical Pod ordinal;
- controller owner reference;
- live StatefulSet UID;
- expected HolderID and workload identity.

Any missing API fact or mismatch must fail closed. That adapter and Kubernetes
join remain unimplemented and unauthorized.

## Verification

Formal performance lock was absent before Go commands. Local/in-process checks
passed:

```text
scoped affected-package tests=PASS
focused blocker tests 50x=PASS
focused race tests=PASS
focused vet=PASS
Redis tests=in-process miniredis only
remote Compose contract tests=PASS
git diff --check=PASS
independent final review=PASS P0/P1/P2=0/0/0
```

Direct Lua tests recompute digest and wire-byte fields after injecting malformed
raw identity values, require `MALFORMED`, and prove zero Redis keys were written.
Coverage includes PodUID and WorkloadUID malformed byte `0xff`, Unicode
whitespace, Unicode letters, internal ASCII space and ordinal above int32.

No external Redis, Kubernetes, network qualification, deployment, performance
sampling or capacity run was performed.

## Remaining state

```text
blocker_1_SERVING_TO_READY_ZERO_SHARD_LIFECYCLE=PASS
blocker_2_DURABLE_MANAGER_WIDE_PLACEMENT_EXCLUSION=PASS
blocker_3_POD_WORKLOAD_ORDINAL_BOOT_IDENTITY_BINDING=PASS
blocker_4_ROUTEPLAN_EXACT_GET_AND_RETENTION_ALIGNMENT=OPEN
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
