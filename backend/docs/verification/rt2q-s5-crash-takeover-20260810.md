# RT2Q S5 Crash Takeover (2026-08-11)

## Verdict

PASS on exact clean detached production head
`b4167fd981863e4ad5a7c330f257c04dc9326a85`.

Three fresh isolated real-production E3 runs passed:

- `/tmp/rt2q-s5-tip1-b4167fd98186`
- `/tmp/rt2q-s5-tip2-b4167fd98186`
- `/tmp/rt2q-s5-tip3-b4167fd98186`

Earlier three PASS runs on `ccbe0f6c9bd11419bf56aa44120210b96cdd093b` remain valid
pre-TTL-restore evidence.

Each run passed every mandatory trusted clause and exact-key/process cleanup.

## Scenario

PLAYER shard 0 starts on holder A. While A is live, production
`TakeoverAfterExpiry` with holder-unavailability proof is rejected
(`takeover_live_rejected`). A is then killed. The harness waits until the real
HolderRegistry record fails the exact PlanExecutor predicate
`Get && LeaseUntil.After(now)` (wall-clock, not List filtering). Authority lease
remains ACTIVE so `TransferShardAuthorityV2` can apply.

## Crash Takeover proof

Production RoutePlan `TakeoverAfterExpiry` then runs with:

- exact old Boot / source fence
- `HolderUnavailable=true` (not a planned Drain Transfer shortcut)
- non-empty operator evidence

Final facts:

- one Authority transfer (`authority_transfer` count=1)
- ACTIVE target Snapshot and authenticated Gate ObserveShard generation
- final `routectl verify` consistent including `gate_resolution`
- restarted old holder old-generation Redis renew returns `FENCE_MISMATCH` with
  durable Authority unchanged (`old_generation_request`)

## Production seam note

`TransferShardAuthorityV2` still rejects Authority `LEASE_EXPIRED`. S5 therefore
proves Crash Takeover via trusted holder-unavailability while Authority remains
current. A future Transfer V3 that accepts expired ACTIVE fences would unlock the
lease-expired proof path without changing PlanExecutor contracts.

Signed implementation heads include `148dde8`, `f663b01`, `ac440f8`, `d58ccb7`,
`07b4f35`, `ccbe0f6`, and `b4167fd`. Tip re-check also passed S0 (retry), S1, S2,
S3, and S4. S6 Fresh Join passed on later tip `d44f216`; final RT2Q tip
regressions and independent review remain pending.
