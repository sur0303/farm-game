# ScalePlan SP3 blocker 5 — blocked termination quiescence and proof ownership

## Scope and identity

```text
worktree=farm-game-scaleplan-sp3-protocols-20260812
branch=codex/scaleplan-sp3-protocols-20260812
base=dcbccc8eb340be9cdbf0d4b0420192db3a41f1f1
protocol_blocker=BLOCKED_TERMINATION_QUIESCENCE_AND_PROOF_OWNERSHIP
reviewed_code_bundle_sha256=577dc566093dfde071a7b23b3b1abdadaabfa45ebec6f1e8821961589a0d5734
verdict=PASS
P0=0
P1=0
P2=0
required_fixes=0
```

The final adjustment after the reviewed bundle requires every active placement
holder to be `SERVING`, process-ready, non-draining and serving at least one
shard. The round-7 independent reviewer explicitly reviewed that adjustment and
returned PASS 0/0/0.

This closes protocol blocker 5 only. It does not implement or authorize a
production repository, Kubernetes scaler, quiescence adapter, deployment,
runtime activation, or performance qualification. The frozen G21 runtime
identity remains `2f16f7c6db3f6e9d8ecb76513cbdf3bd27724361`.

## Single proof and terminalization owner

`BlockedTerminationOwner` owns one repository-shared mutation barrier across
Submit, Execute, Step, Pause, Resume, blocked reconciliation and operator safe
termination. Executor accepts only the concrete owner and rejects a repository
identity mismatch. The owner retains a quiescence fence through final
revalidation and the dedicated atomic terminal commit. Mandatory fence cleanup
uses a bounded context that is not canceled with the operator request.

The ordinary repository interface exposes no blocked terminal operation.
Generic CAS rejects `OPERATOR_SAFE_TERMINATION` and proof records. The sealed
repository capability assigns commit time at the atomic linearization point,
checks proof expiry and the exact blocked revision, then persists terminal plan,
receipt and proof while releasing the workload index in the same operation.

## Exact checkpoint, evidence and retention

The deterministic blocked checkpoint digest binds immutable submission and all
durable Saga state, including:

```text
PlanID / revision / blocked reason / timestamps
workload UID and resourceVersion
scale and exclusion intents
complete resolved child set
child source/target HolderID, BootID, PodUID, WorkloadUID and Ordinal
endpoints, Authority fence, child phase and observation state
```

Canonical structured quiescence evidence binds:

```text
workload replicas/generation/resourceVersion
contiguous unique Pods and exact Holder process identities
complete active shard placements and serving holder lifecycle
placement exclusion owner/revision
all child RoutePlans and Manager transition/recovery records
proof capture/expiry and component evidence retention
```

Every child and Manager component retention covers the aggregate evidence
retention, which covers the parent RecoveryRetainUntil deadline and proof
expiry. The durable receipt carries the blocked checkpoint timestamp and a
separately versioned receipt schema. Replay validates the terminal record,
recomputes the persisted proof digest, reconstructs the retained original
blocked checkpoint and rejects canonical-format corruption.

## Failure and retry behavior

Unknown outcome, revision conflict, expired proof, malformed or contradictory
topology, external drift and fence/revalidation failures leave the plan blocked
and retain the workload lock. Apply-then-unknown retries return the exact
already-committed terminal record for the same TerminationID and checkpoint.
A different TerminationID or expectation never rebases.

## Verification

Formal performance lock was absent before Go commands. Local code-only checks
passed:

```text
go test ./...=PASS
internal/scaleplan and internal/testkit/scaleplan -count=50=PASS
focused race=PASS
focused vet=PASS
git diff --check=PASS
independent final review=PASS P0/P1/P2=0/0/0
```

No external Redis, Kubernetes, network qualification, deployment, performance
sampling or capacity run was performed.

## Remaining state

```text
blockers_1_through_5=PASS
SP3_0=NOT_RERUN
adapter_implementation_authorized=false
ready_for_sp3=false
supports_30m_dau=false
```

Five blocker closure does not itself authorize adapters. SP3-0 compatibility
review must be rerun independently before any adapter planning or implementation.

Repository actions at handoff preparation:

```text
push=NOT_PERFORMED
merge=NOT_PERFORMED
rebase_integration=NOT_PERFORMED
tag=NOT_PERFORMED
```
