# RT2Q S6 Fresh Join / Business Reconciliation (2026-08-11)

## Verdict

PASS on exact clean detached production head
`d44f216075adb18e42e7f8ca7d4aa354ebc17042`.

Three fresh isolated real-production E3 runs passed:

- `/tmp/rt2q-s6-e3-d44f216075ad-r1`
- `/tmp/rt2q-s6-e3-d44f216075ad-r2`
- `/tmp/rt2q-s6-e3-d44f216075ad-r3`

Each run passed every mandatory trusted v2 clause and exact-key/process cleanup.

## Scenario

1. Succeeded PLAYER A→B migration establishes the current dynamic Snapshot.
2. Gate-1 and worker restart as genuinely fresh consumer processes (new Boot) and
   complete authenticated Snapshot bootstrap at the current Controller
   Boot/revision/generation (TLS/Snapshot proxy facts).
3. Business driver binds T1–T3 BUY through the production Gate access path to the
   current PLAYER shard holder.
4. Owned farm-logic TCP fault proxy drops renew RPCs until the short subscription
   lease expires (`SUBSCRIPTION_LEASE_LOST`), without killing farm-svr (same-Boot
   reclaim under live Authority membership cannot serve Join in-budget).
5. Fresh Join + recovery proves new attempt/subscription fence.
6. Owned Gate access proxy drops exactly one committed T2 CommandResult.
7. Client reopens a fresh Gate session and replays the same `operation_id` with a
   new `request_id`; receipt is exactly once (`same_receipt=true`,
   `duplicate_reward=false`).
8. T3 succeeds; `routectl verify` four-truth binds authority/snapshot/holder/plan;
   hot-path Snapshot unary/stream deltas remain 0; Redis keys and processes clean.

## Trusted clauses

| Clause | Evidence |
| --- | --- |
| `s6.three_operations` | `business_receipt` T1/T2/T3 count=1 |
| `s6.t2_response_lost` | `business_response_lost` T2 committed=true + proxy drop |
| `s6.t2_replay` | `business_replay` same_receipt=true duplicate_reward=false |
| `s6.rejoin` | `subscription_rejoin` SUBSCRIPTION_LEASE_LOST fresh_attempt/fence |
| `s6.four_truth` | `business_reconcile` authority/snapshot/holder/plan |
| cleanup | exact Redis keys + process residue |

## Production seam notes

- Farm same-Boot kill/reclaim is not used for lease-loss: after HolderRegistry
  expiry with Authority still matching, `/readyz` stays false and Join fails for
  the full retry budget. S6 injects renew loss at the owned TCP fault proxy instead.
- Same-connection BUY replay after access-proxy CommandResult drop can stall on
  the WS forwarder; durable reconciliation uses a fresh Gate session with the
  original T2 `operation_id`.

Signed S6 lineage includes `4698d6e`, `a61d3cf`, `c80f79f`, `a46138e`, and
`d44f216`. Tip re-check also passed three fresh E3 on
`f112087c9ced47f212194e8d357a823aad28be95`
(`/tmp/rt2q-final-s6-f112087c9ced-r{1,2,3}`). Independent exact review / freeze /
integration remain pending; no checkpoint freeze and no push.
