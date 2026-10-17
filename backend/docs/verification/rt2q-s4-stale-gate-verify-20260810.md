# RT2Q S4 stale Gate Verify fail-closed (2026-08-11)

## Verdict

PASS on exact clean detached production head
`340f748ed8455dbdd5bc4b31a02bb9943b468328` (implementation tip), and re-confirmed
PASS on later tip `b4167fd981863e4ad5a7c330f257c04dc9326a85`
(`/tmp/rt2q-tip-s4-b4167fd98186`).

Three fresh isolated real-production E3 runs passed on `340f748`:

- `/tmp/rt2q-s4-smoke1-340f748ed845`
- `/tmp/rt2q-s4-smoke2-340f748ed845`
- `/tmp/rt2q-s4-smoke3-340f748ed845`

Each run passed every mandatory trusted clause and exact-key/process cleanup.

## Scenario

A real PLAYER migration Plan is driven to `SUCCEEDED` with production
`routectl move-shard` and HolderControl Stop-hold. Controller Gate verifier
peers are pointed at owned mTLS Gate verifier forward proxies that terminate
authenticated ObserveShard, forward to real `farm-gate`, and optionally mutate
the authenticated reply.

## Fail-closed Verify

With the Plan left unmutated, each negative case is armed on gate-1 only while
gate-2 continues to report the current exact Snapshot/generation:

| case | observation fault |
| --- | --- |
| `pending` | State=`ASSIGNING` |
| `stale` | SnapshotRevision−1 with peer binding evidence |
| `unavailable` | authenticated Unavailable |
| `unknown` | WriterEpoch=0 |
| `missing-replay` | AssignmentRevision−1 and Boot suffix |

Production `routectl verify` reports `consistent=false` and exact
`gate_resolution=false` while Authority/Snapshot/target facts remain current.
Stale evidence binds peer identity over TLS plus Boot/SnapshotRevision/
generation from authenticated ObserveShard.

## Clear and converge

After faults are cleared, the same production Verify converges to fully
consistent (`checks=11`) without mutating or replaying the Plan or Authority.
An identical clear ObserveShard forward is recorded as `snapshot_release`.

## Owned seams

- `internal/testkit/routecontrol/gate_verifier_proxy.go`
- focused proxy/oracle tests in `grpc_proxy_test.go` / `trusted_v2_test.go`
- harness `runS4` in `cmd/rt2q-route-control-harness/runner.go`

Signed implementation heads: `03ec0a0`, `340f748`.
S5-S6 and final RT2Q closure remain pending.
