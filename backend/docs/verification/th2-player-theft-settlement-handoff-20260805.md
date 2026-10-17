# TH2 · PlayerActor Theft settlement handoff · 2026-08-05

## Scope delivered

The existing Player Participant Server/Registry/mailbox/Authority Store now
handles both registered Player steps:

- `SETTLE_THEFT_VISITOR`: consumes only the Work-proven
  `TheftTargetDecision`, grants `net_received_quantity` to the frozen produce
  item, and commits the visitor receipt/result proof plus the legal friend
  interaction and TaskDomain fact in one Player Authority Commit.
- `CREDIT_GUARD_INCOME`: accepts only `guard_coin_value > 0`, consumes the
  same Work-proven decision and applied visitor result hash, and commits coin,
  guard-income receipt/result proof in one Player Authority Commit.

The shared ingress, receipt-first lookup, route/work/version/lease checks,
capacity admission, mailbox, `StoredOperationOutcome`, and reload-safe
Participant receipt carrier are reused. Zero net quantity leaves inventory
unchanged but still records the receipt and interaction fact. Zero guard coin
is rejected at Participant admission; the future Coordinator must skip that
step entirely.

The real `participantproof.Verifier` is wired in `cmd/player-svr`, including
Work root, ActionPermit, FARM_DECIDED result/hash, visitor result/hash,
participant step/invocation/version and payload binding checks. The bounded
TaskDomain model is persisted through `PlayerRecord.task_domain_state` with the
server-owned P0 config repository.

## Focused tests

- `TestTheftVisitorNetPositiveCommitsInventoryReceiptAndTask`
- `TestTheftVisitorZeroNetStillCommitsReceiptAndCannotRetry`
- `TestTheftOwnerPositiveCoinAndZeroCoinRejected`
- `TestTheftHashForgeryAndConcurrentSettlementFailClosedOrReplay`
- `TestTheftResponseLossAndTaskResolvedAtCrossDay`

Validation run:

```text
go test ./...
```

passed on 2026-08-05.

## Remaining Workflow integration

1. The Theft Coordinator still needs to build/create the durable Theft Work,
   persist the exact `FARM_DECIDED` result and hash, and dispatch the visitor
   payload from that stored decision.
2. After a successful visitor result it must persist the exact visitor result
   hash and Work version before dispatching the owner step. It must skip
   `CREDIT_GUARD_INCOME` when the frozen guard coin is zero; it must not call
   the Participant to manufacture a zero-income receipt.
3. Workflow must preserve the source-authority resolved timestamp used for
   TaskDomain period classification when advancing the Work record. The Player
   Participant never accepts this timestamp from a caller.
4. Worker recovery and public operation-result integration should use the same
   WorkAddress/step invocation/result-proof fields and exercise the existing
   response-loss/reload path end to end. FarmActor, Friend service, Workflow
   engine, and public ingress were intentionally not changed in this handoff.
