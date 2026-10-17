# ScalePlan Core SP0–SP2 code-only handoff

## 1. Status and immutable identities

```text
task=SP0-SP2-SCALEPLAN-CODE-ONLY
mode=CODE_ONLY
worktree=farm-game-scaleplan-core-20260811
branch=codex/scaleplan-core-20260811
base=2f16f7c6db3f6e9d8ecb76513cbdf3bd27724361
candidate_implementation_head=0f52937a795882ce7acb14d3dae7105b7a1e6460
last_independently_reviewed_repository_head=d8a2336662b972029afe9cdad4ec330cd4320e39
last_independently_reviewed_implementation_head=0f52937a795882ce7acb14d3dae7105b7a1e6460
candidate_review=ROUND_7_PASS
independent_review_findings=P0_0_P1_0_P2_0
status=READY_FOR_INDEPENDENT_REVIEW
ready_for_sp3=false
```

The base commit is also the frozen G21 runtime verification identity. ScalePlan commits are code-only descendants and **must not** be represented as a new G21 runtime identity or as runtime retesting.

This handoff does not claim production scale-out, scale-in, Kubernetes behavior, Redis behavior, capacity, performance, or `supports_30m_dau=true`.

## 2. Scope

Implemented only under:

```text
internal/scaleplan/**
internal/testkit/scaleplan/**
docs/verification/scaleplan-core-handoff.md
docs/verification/scaleplan-core-status.json
```

No Route Manager, RoutePlan implementation, Authority, HolderControl, Redis, protobuf, command, deployment, performance, H5, or production composition code was changed.

No Kubernetes, Redis, formal evidence, performance result, image, Pod, or real shard migration was accessed or produced.

## 3. Core model

ScalePlan is an imperative durable Saga. It is not an autoscaler and does not collect CPU, QPS, latency, or capacity signals.

Supported operations:

```text
SCALE_OUT
SCALE_IN
```

Scale-out phases:

```text
PENDING
→ SCALING_UP
→ WAITING_HOLDER_READY
→ MOVING_WAVE
↔ VERIFYING_WAVE
→ VERIFYING_FINAL
→ SUCCEEDED
```

Scale-in phases:

```text
PENDING
→ SELECTING_VICTIM
→ EXCLUDING_VICTIM
→ CAPTURING_VICTIM_SHARDS
→ MOVING_WAVE
↔ VERIFYING_WAVE
→ WAITING_ZERO_SHARD
→ SCALING_DOWN
→ VERIFYING_FINAL
→ SUCCEEDED
```

Nonterminal execution statuses are `RUNNING`, `PAUSED`, and `BLOCKED_NEEDS_OPERATOR`. Terminal statuses are `SUCCEEDED`, `FAILED`, and `CANCELLED`.

## 4. Durable identity and idempotency

The model keeps these identities distinct:

```text
HolderBootID
PodUID
WorkloadUID
Ordinal
```

Plan IDs, child RoutePlan IDs, transition IDs, and payload digests are deterministic and bound to canonical immutable inputs. Durable validation recomputes child identity rather than trusting persisted IDs.

Repository contracts require:

- idempotency-key and payload binding;
- workload single-flight ownership;
- revision CAS with `ValidatePlanUpdate` before commit;
- bounded recoverable-plan enumeration;
- terminal release only after ordinary success/failure or verified safe blocked termination.

The in-memory repository is a fake contract implementation, not a production durability claim.

## 5. Scale-out safety

Scale-out uses a write-ahead `ScaleIntent` before invoking the workload mutation port. It freezes:

```text
WorkloadUID
ExpectedResourceVersion
ExpectedReplicas
DesiredReplicas
```

After scale-up it requires the new target to join the frozen workload and exact current Pod/Holder generation. Every child move must target the frozen `Plan.Target`.

Final verification captures the complete moved shard set in one `CaptureShards` snapshot and performs one end-bracket `Revalidate`. Per-shard reads from mixed revisions are not accepted.

A final WorkloadUID replacement is a durable conflict, not an indefinitely pending condition.

## 6. Scale-in safety

Placement exclusion is separate from Holder lifecycle draining. An excluded source remains a serving/renewing planned-drain source but cannot be selected as a new placement target.

Scale-in ordering is:

```text
freeze exact victim generation
→ persist exclusion intent
→ apply exact exclusion CAS
→ confirm exclusion revision and owner
→ capture complete victim manifest
→ revalidate capture
→ create and verify child moves
→ prove exact victim zero-shard
→ persist scale-down intent
→ revalidate immediate effect barrier
→ invoke scale-down CAS
```

Every scale-in child source equals the frozen exact victim. Every target ordinal is below the desired replica count and must join a currently present remaining StatefulSet Pod with exact WorkloadUID, PodUID, HolderID, HolderBootID, and ordinal.

Before each child effect and before scale-down, Core rechecks the exact exclusion owner/revision, placement/fence facts, holder generation, target join, workload identity, and zero-shard conditions. Cross-fact observations use capture/revalidate contracts and fail closed on drift.

A workload ResourceVersion change before creating the scale-down intent produces durable `BLOCKED_NEEDS_OPERATOR` and no scale effect.

## 7. Child RoutePlan contract

`RoutePlanPort.GetMove` receives the complete expected immutable request:

```text
PlanID
TransitionID
Domain
VirtualShard
Source exact HolderRef
SourceEndpoint
Target exact HolderRef
TargetEndpoint
ExpectedSourceFence
RequestedTTLms
PayloadDigest
```

A production adapter must compare the complete serving RoutePlan immutable move before returning a match.

Child durable observations are edge-specific:

- appending a child permits no observation fields;
- same-phase `MOVING_WAVE` observations permit only known monotonic nonterminal RoutePlan phases;
- skipped polling observations such as `PENDING → PREPARING`, `PENDING → ACTIVE`, and `DRAINING → ACTIVE` are accepted;
- nonterminal regressions are rejected;
- `SUCCEEDED` enters `VERIFYING_WAVE` and is marked verified only after exact `GetMove` plus `Verify`;
- `FAILED` durably fails the parent;
- `PAUSED` enters controlled `BLOCKED_NEEDS_OPERATOR` recovery;
- unknown phase strings durably fail;
- exact immutable conflicts and impossible post-success nonterminal regressions durably fail.

A consistent PAUSED child can be reconciled from either `MOVING_WAVE` or `VERIFYING_WAVE`. Recovery requires exact `GetMove`, successful `Verify`, exact blocked reason, and valid recovery retention.

## 8. Deadlines and blocked recovery

`StartDeadlineMs` and `RecoveryRetainUntilMs` are separate:

- before the first external intent, expiry terminalizes the plan;
- after any durable Scale, Exclusion, or Child intent, ordinary start expiry cannot release workload ownership;
- time is rechecked immediately before first-intent CAS and immediately before `Scale`, `Exclude`, and `EnsureMove` effects;
- recovery-retention expiry enters `BLOCKED_NEEDS_OPERATOR` and keeps workload single-flight ownership.

`ReconcileBlocked` is read-only with respect to production effects. It may adopt only factually verified child/capture outcomes for exact allow-listed blocked reasons.

`OperatorFailBlocked` can release a blocked workload only after the injected safety port proves all of:

```text
NoPendingExternalEffect
PlacementConsistent
WorkloadManageable
Safe
```

## 9. CAS hardening

`ValidatePlanUpdate` validates both current and candidate records and freezes immutable submission/audit identity. Durable facts may first appear or advance only on their assigned state-machine edge.

It rejects:

- same-phase fact injection outside the legal child checkpoint;
- phase or wave progress while remaining PAUSED/BLOCKED;
- non-running durable-fact mutation;
- blocked-reason rewrites outside the exact retention transition;
- blocked recovery under the wrong reason;
- unknown effect states or phase-incompatible effect states;
- ScaleIntent ResourceVersion drift;
- forged child outcomes, IDs, payloads, endpoints, ordinals, holders, or fences.

## 10. Independent review history

```text
Round 1: head=113c295cd7f6ecb28814e2387f8576c5dff504e8 verdict=FAIL P0=0 P1=2 P2=2
Round 2: head=a119e1c0b4eb290f526c8de6e51418d170fd24f3 verdict=FAIL
Round 3: head=f00b051d7cd2d6431c0b2fc7b768af07e99629db verdict=FAIL P0=0 P1=1 P2=2
Round 4: head=07aed80a4542d8466c67007a3d47994e78de2a3b no P0/P1; follow-up P2 recorded
Round 5: head=594866958c6120405059c94d750ab941532c28e6 verdict=FAIL P0=0 P1=0 P2=3
Round 6: head=05d15038d3a74810a73315bdc7473bd8dd442f1a verdict=FAIL P0=0 P1=0 code-P2=2 docs-P2=1
Round 7: repository head=d8a2336662b972029afe9cdad4ec330cd4320e39 implementation head=0f52937a795882ce7acb14d3dae7105b7a1e6460 verdict=PASS P0=0 P1=0 P2=0
```

Round 7 independently confirmed closure of all prior Core and handoff findings. Its PASS authorizes only the recorded `READY_FOR_INDEPENDENT_REVIEW` state. It does not authorize SP3 implementation, integration, runtime activation, or capacity qualification.

## 11. Independent review verdict and verification

```text
round=7
verdict=PASS
P0=0
P1=0
P2=0
required_fixes=0
ready_for_sp3=false
```

The independent reviewer confirmed exact base-to-candidate ancestry, the code-only write set, the refreshed documents, and a clean repository at reviewed repository head `d8a2336662b972029afe9cdad4ec330cd4320e39`.

### Candidate implementation checks

The formal performance lock was absent before Go commands. The following owned-scope checks passed:

```text
go test ./internal/scaleplan/... -count=1
go test ./internal/scaleplan/... -run '^TestScalePlan' -count=50
go test -race ./internal/scaleplan/... -count=1
go vet ./internal/scaleplan/...
go vet ./internal/testkit/scaleplan/...
go test ./internal/testkit/scaleplan/... -count=1
git diff --check
```

Forbidden-dependency and symbolic-link scans were clean. No full-repository test, race, vet, build, runtime test, or performance run was executed.

## 12. SP3-0 production-port compatibility review

A read-only SP3-0 review was completed against repository head `932cad0d11ba66aacb4c486caf43b652dcaddb3c` without modifying production code:

```text
verdict=SP3_0_FAIL_BLOCKED
adapter_implementation_authorized=false
runtime_qualification=false
capacity_qualification=false
```

### Protocol blockers

1. **Holder zero-shard lifecycle:** HolderAgent derives `READY_ZERO_SHARD` after the last shard, but the production HolderRegistry transition matrix does not permit `SERVING → READY_ZERO_SHARD`. Using `DRAINING` would violate both ScalePlan and planned-drain RoutePlan source requirements.
2. **Manager-wide PlacementExclusion:** production has no durable exact-Boot exclusion owner/revision CAS, and no mandatory exclusion target-admission enforcement across all Manager claim/transfer/recovery/publication paths. Exclusion must not be encoded as lifecycle draining, capacity zero, readiness loss, deletion, or lease suppression.
3. **Exact Kubernetes identity join:** production Holder records do not carry PodUID, WorkloadUID, or ordinal. The current deployment boot identity can repeat for a recreated ordinal. Pod owner UID and process-generation binding require a frozen production protocol.
4. **RoutePlan retrieval and retention:** in-process immutable fields are compatible, but remote RPC lacks exact terminal Get-by-ID and RoutePlan retention can be shorter than the ScalePlan recovery horizon.
5. **Blocked termination proof:** no production whole-plan quiescence proof or proof-to-terminal-CAS ownership exists.

### Compatibility summary

```text
ScalePlanRepository=NEEDS_DESIGN
WorkloadScaler.Get/Scale=NEEDS_DESIGN
HolderStateReader=BLOCKED
PlacementExclusionPort=BLOCKED
ShardPlacementReader=BLOCKED
RoutePlanPort.in_process=NEEDS_DESIGN
RoutePlanPort.remote=BLOCKED
BlockedPlanSafetyPort=BLOCKED
Clock=READY
Composition/Startup=NEEDS_DESIGN
Observability=NEEDS_DESIGN
```

Before another SP3-0 review, the production train must independently design, implement, and review the five protocol blockers above. Only then may adapter planning be reconsidered. This Core worktree is not authorized to implement those production protocol changes.

Current production facts remain:

```text
production_adapter=NOT_IMPLEMENTED
real_kubernetes=NOT_RUN
real_redis=NOT_RUN
real_routeplan=NOT_RUN
real_holder_drain=NOT_RUN
real_shard_migration=NOT_RUN
sp3_0_verdict=SP3_0_FAIL_BLOCKED
adapter_implementation_authorized=false
ready_for_sp3=false
supports_30m_dau=false
```

## 13. Repository actions

```text
push=NOT_PERFORMED
merge=NOT_PERFORMED
rebase_integration=NOT_PERFORMED
tag=NOT_PERFORMED
```
