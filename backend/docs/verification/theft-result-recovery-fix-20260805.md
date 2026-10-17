# Theft result recovery verification — 2026-08-05

## Scope

This change closes the Pair `RESULT_PUBLISH_ORDER` lifecycle and restores
public Theft result recovery without changing the frozen Theft participant
workflow, gameplay rules, settlement semantics, route domains, or public
protobuf fields. `FRIEND_PAIR` remains the admission and pre-Work recovery
authority. `ResultDirectory` remains a rebuildable terminal projection.

## Source-order ownership and terminal states

| Pair source order | Producer | Consumer | Durable terminal states |
| --- | --- | --- | --- |
| `FRIEND_ACTION_DISPATCH_ORDER` | Pair action admission when accepted | Theft recovery worker lease/advancer | `COMPLETED`, `RECONCILE_REQUIRED` |
| `RESULT_PUBLISH_ORDER` | Every Pair action admission | Theft result-order processor | `SUCCEEDED`, `NO_CHANGE`, `REJECTED`, `RECONCILE_REQUIRED` |

The result-order processor resolves the immutable Pair binding first. A
rejected-before-Work admission publishes its rejection directly from Pair. An
accepted admission waits while Work is absent or processing, validates the
terminal Work against the Pair operation/payload/target binding, publishes the
terminal projection, and only then terminalizes the result source order.
Publisher failure leaves the source order pending and due for retry.

Both source-order terminal paths use same-slot Lua. Before mutation they
validate the immutable order identity, lease/state where applicable, DueIndex
membership, and bucket counters. Completion atomically:

1. persists the terminal source-order state and proof;
2. removes the exact DueIndex member;
3. decrements `pending_work_count` by one; and
4. decrements `pending_work_bytes` by the original logical charge.

The byte release uses the same frozen accounting expression as Pair admission:

`512 + Σ(16 + len(value))`

for `work_id`, `work_type`, `typed_payload`, `work_ref`, `payload_hash`, and the
original due value (`"0"`). Replay of the same terminal is idempotent. Missing,
malformed, stale, mismatched, or underflowing records return reconciliation and
retain pending state/DueIndex visibility; counters cannot be decremented below
zero.

## Recovery behavior

- Pair result lookup accepts both exact legal manifests: the 22-field base
  rejected-before-Work order and the 23-field accepted order with its dispatch
  anchor. The typed payload binds subject, command operation, Pair, target,
  canonical payload hash, and target-binding hash.
- Accepted Pair admission remains publicly discoverable as `PROCESSING` before
  Work creation. Rejected-before-Work admission is recovered as a terminal
  `REJECTED` outcome rather than repository unavailability.
- Theft publication accepts `SUCCEEDED`, `NO_CHANGE`, and `REJECTED`. The
  projection payload is deterministically encoded with a checksum and binds
  subject, command, operation ID, canonical payload hash, and stored public
  outcome. Publisher validation recomputes the Pair operation digest and Work
  identity before writing.
- `GET_OPERATION_RESULT` checks `ResultDirectory` first for an exactly bound
  legal terminal, then falls back to authoritative Work and Pair reads on a
  miss, read error, malformed entry, or binding mismatch. Projection loss can
  therefore add a fallback read but cannot manufacture a terminal result.
- ResultDirectory replay with identical bytes is idempotent. A different
  terminal under the same operation digest conflicts. Pair admission replay
  retains the original outcome for the same canonical payload and returns
  `IDEMPOTENCY_CONFLICT` for a different payload.

## Focused evidence

The focused suite covers rejected-before-Work lookup, exact counter/byte
release, corrupt-counter fail-closed behavior, all three legal terminal
publications and reads, projection-miss fallback, publisher retry/idempotency,
admission processing-before-Work, and payload conflict:

```text
go test ./internal/friend ./internal/worker/theftrecovery ./internal/theftonline ./internal/player/ingress ./internal/storage/redis/authority ./internal/scenario/theft
```

Result: PASS.

The live integration test was run against `redis:7.4-alpine`:

```text
FRIEND_REDIS_LIVE_ADDR=127.0.0.1:32788 go test ./internal/worker/theftrecovery -run TestPairMixedTerminalLifecycleOver64RealRedis -count=1 -v
```

Result:

```text
REAL_REDIS_PASS: 80 mixed terminal Pair operations released all source-order count, bytes, and due members
--- PASS: TestPairMixedTerminalLifecycleOver64RealRedis
```

After every sequential operation in the single Pair bucket, the test asserts
`pending_work_count == 0`, `pending_work_bytes == 0`, and DueIndex cardinality
zero. The test rotates `SUCCEEDED`, `NO_CHANGE`, and `REJECTED` result-order
terminals and therefore crosses the configured capacity of 64 without a
capacity leak.

## Repository-wide verification

The final verification gate consists of:

```text
go test -race ./internal/friend ./internal/worker/theftrecovery ./internal/theftonline ./internal/player/ingress ./internal/storage/redis/authority ./internal/scenario/theft
go test ./...
go vet ./...
git diff --check
```
