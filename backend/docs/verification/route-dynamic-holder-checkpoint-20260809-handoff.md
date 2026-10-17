# CP-ROUTE-DYNAMIC-HOLDER — Handoff

Date: 2026-08-09
Branch: `checkpoint/route-dynamic-holder-20260809`
Parent transport checkpoint: `797bdfa9b729c782719042a0cf53d9886013de50`
RT2D Holder Control feature head: `d5a78f7435e78420317e4e26f6613f00834e59e9`
Checkpoint state before this handoff update: `d5a78f7435e78420317e4e26f6613f00834e59e9`

## Frozen D interface and runtime contract

- Internal-only HolderControl exposes exactly five unary RPCs: `StopNewActivationAndTurns`, `WaitCurrentTurns`, `ConfirmFactsCommitted`, `PrepareShardGeneration`, and `GetShardStatus`.
- Player/Farm actor execution uses an injected actor-routing calculator and a per-shard admission gate. Stop rejects new User turns/activations only for the target shard; accepted work remains counted until actual completion, while SystemQueue and other shards remain independent.
- Facts barriers enumerate all target-shard bindings, resolve pending unknown state, and require Persistence Lane durability before confirmation.
- `PrepareShardGeneration` is prepare-only: it calls Membership preparation, never commits admission and never scans or bulk-loads shard actors. Prepared entries are bounded, expire, and disappear after commit.
- HolderRegistry is durable liveness/capacity state only. It cannot grant write authority and its failure alone cannot trigger takeover. Redis storage uses same-slot fixed Lua, complete records, bounded reads, exact accounting/revision fences, canonical resource wire data, logical leases, and bounded physical cleanup.
- Production Holder control uses the frozen verified TLS peer identity, exact-method ACL, and sealed multi-service server contracts. Remote drain binds domain/shard/holder/boot/generation and validates every phase/status response.
- Planned movement still requires Drain. Direct Transfer remains limited to explicit CrashTakeoverProof and the RouteAssignmentManager remains the only generation-transition coordinator.

## HolderAgent lifecycle MVP

Startup and shutdown remain monotonic for one Holder boot. Dynamic assignment
may move a live process between zero-shard and serving states:

```text
WARMING -> READY_ZERO_SHARD <-> SERVING -> DRAINING -> DRAINED
```

The reverse `SERVING -> READY_ZERO_SHARD` transition reports that a newer
assignment removed the final shard. It does not reopen a closing Holder and
does not grant Authority; Manager fencing remains mandatory before any later
assignment.

The final lifecycle closure freezes these rules:

1. Once Close begins, no new ordinary heartbeat is admitted.
2. Start/Close races cannot leave a new run loop after Close wins.
3. An in-flight heartbeat result is accepted only after state/token and exact durable-record reconciliation; it cannot restart local liveness after closure.
4. A normal heartbeat cannot reverse the final visible DRAINING/DRAINED progression. If final publication fails, the record is allowed to expire by bounded TTL; HolderRegistry still grants no shard write authority.
5. With production dependencies that honor context and configured dial/read/write/RPC bounds, Close completes within `CloseTimeout` plus small scheduler margin.

Production `HolderStore` and Holder gRPC clients honor context and enforce bounded operation/RPC budgets. A deliberately malicious dependency that permanently ignores context is outside the production interface contract: Go cannot both kill its goroutine and prohibit every late side effect. The Agent uses a bounded one-shot result fence so its own Close owner and `closeDone` remain bounded, but this is not a second close protocol or a general dependency-killing framework.

## Review closure

Independent official OpenAI Codex reviews were run in detached, clean worktrees. Runtime and Transport scopes reached P0=0/P1=0 with:

```text
NO FINDINGS / NO BLOCKING FINDINGS
```

The same official OpenAI `gpt-5.6-luna`, reasoning `max`, lifecycle Reviewer then rechecked exact feature head `d5a78f7435e78420317e4e26f6613f00834e59e9` against the frozen L1–L5 contract and the production-dependency P1 admission criteria. Final result:

```text
P0: 0
P1: 0
P2: 0
NO FINDINGS / NO BLOCKING FINDINGS
```

No integration merge has occurred.

## Final gates on exact RT2D feature head

Controller-run gates outside the read-only reviewer sandbox:

```text
go test ./internal/holdercontrol -run <seven Start/Close/HB race tests> -count=100        PASS
go test -race ./internal/routecontrol/... ./internal/storage/redis/routecontrol/... \
  ./internal/holdercontrol/... -count=3                                                   PASS
go test ./... -count=1                                                                    PASS
go vet ./...                                                                              PASS
go build -buildvcs=false ./...                                                            PASS
./scripts/gen-proto.sh followed by clean worktree                                          PASS
git diff --check                                                                          PASS
composition-root forbidden-path scan                                                      PASS
added-line secret scan                                                                     PASS
SSH signature verification                                                                 PASS
feature worktree status                                                                    CLEAN
```

The feature branch `codex/rt2d-holder-control-20260808` is pushed at the exact RT2D head. This checkpoint adds documentation only and freezes that head as the base for RT2I and RT2M.

## Next ownership

- RT2I exclusively owns production composition roots and `deploy/**`.
- RT2M owns RoutePlanStore, PlanExecutor, and `routectl` manual migration while reusing Manager Drain/Transfer.
- RT2Q Final must produce multi-Pod Watch, A→B Drain, Controller restart, Crash takeover, Fresh Join, and Authority reconciliation evidence.
- Autoscaling AS0–AS3 remains blocked until RT2I/RT2M/RT2Q, final Premerge, G1/G2/G3, and `CP-ROUTE-CONTROL-MVP` complete.
