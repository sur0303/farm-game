# Friend envelope correctness closure — Premerge Gate

## Exact identity

- Remote integration base: `54c6d0a15cd2e0555d309b59319f1ce30fab268c`
- Reviewed source/evidence head: `965f0fc0574b32b1bf6bc0bc4a54a03273563d8c`
- Premerge merge head: `dad1c9efd5318f6c24ebdc021b123e650333b56b`
- Branch: `premerge/friend-envelope-closure-20260810`

Scope diff is restricted to:

```text
internal/friend/**
cmd/cap-redis/**
scripts/perf/cap-redis/**
docs/verification/perf/cap-redis/**
```

No H5, Proto/wire contract, deployment, Actor/Workflow production semantics, public API or capacity threshold changed.

## Mechanical Gate

At exact Premerge merge head:

```text
GOFLAGS=-buildvcs=false go test ./... -count=1                         PASS
GOFLAGS=-buildvcs=false go vet ./...                                   PASS
GOFLAGS=-buildvcs=false go build ./cmd/...                             PASS
GOFLAGS=-buildvcs=false go test -race focused Friend + CAP diagnostics PASS
git diff --check                                                       PASS
scope allowlist                                                        PASS
```

Focused Race includes atomic envelope, Pair batch, Delete convergence and diagnostic failure controls.

## Real Redis Gate

Temporary loopback Redis 7.4, unique test namespace, no persistence:

```text
PhaseBarrier repeats      = 20
AtomicReadTotal           = 60
PresentTotal              = 0
AbsentTotal               = 60
ReconcileTotal            = 0
TechnicalFailureTotal     = 0
verdict                    = REAL_REDIS_PASS
```

The container was exactly destroyed; no CAP/Friend Redis container or Redis process remains. No credential was retained.

## Correctness evidence binding

Independent review remains:

```text
P0=0 / P1=0 / P2=0
P1-01=PASS
P1-02=PASS
EXACT_HEAD_CLOSURE=PASS
```

Retained failed diagnostics remain visible and unchanged. The premerge gate does not mark any previous failed run baseline eligible.

## Manifest boundary

The checked-in L1 Manifest still identifies the pre-fix integration/image and therefore MUST NOT be used for a post-fix Formal run. Premerge authorizes only an integration merge. After integration, a new backend image digest and exact integration Business/Evidence Heads must be frozen before restarting L1 Smoke.

```text
premerge_gate = PASS
formal_l1_authorized = false
baseline_eligible = false
capacity_claim = NONE
```
