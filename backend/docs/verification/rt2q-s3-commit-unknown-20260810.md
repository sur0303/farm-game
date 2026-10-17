# RT2Q S3 commit-unknown Authority transfer (2026-08-10)

## Verdict

PASS on exact clean detached production head
`5617db5fda2ab1b388b537ac06fd03b90572ffc8`.

Three fresh isolated E3 runs passed:

- `/tmp/rt2q-s3-smoke-5617db5`
- `/tmp/rt2q-s3-smoke2-5617db5`
- `/tmp/rt2q-s3-smoke3-5617db5`

Each run passed every mandatory trusted clause and exact-key/process cleanup.

## Scenario

A FARM shard planned move A→B runs through production `routectl` and real
Holder callbacks. The owned RESP proxy identifies the exact production
`TransferShardAuthorityV2` EVALSHA by the exact shard-authority key and golden
script SHA, forwards it, fully reads its successful APPLIED backend reply,
drops that one reply, and blocks later controller Redis traffic so a PAUSED CAS
cannot persist.

## Four-truth crash barrier

Direct production readers prove before controller kill:

1. Authority is target with the exact move `transition_id`;
2. complete Snapshot still routes source ACTIVE;
3. durable Plan remains `ASSIGNING`;
4. TransitionRecovery records `authority_attempted=true`.

Stop, Wait and Facts have each completed exactly once. The controller is then
killed and a new Boot performs Manager recovery before Plan recovery.

## Final proof

Every passing run proves:

- Transfer EVALSHA forwarded exactly once;
- successful Transfer reply dropped exactly once;
- no repeated Authority mutation, Stop, Wait or Facts;
- Prepare exactly once;
- Plan `SUCCEEDED` revision 7, never PAUSED/FAILED;
- ACTIVE Snapshot and durable replay floor bind the prepared deadline;
- a later same-generation holder Renew is accepted only as a monotonic deadline
  extension by the real fail-closed VerificationPort;
- all seven authenticated consumers converge on the recovery Boot;
- final production `routectl verify` reports all 11 checks consistent;
- exact Redis key and process cleanup PASS.

## Production defects closed while establishing S3

- startup Watch must be served before SnapshotRevision convergence, while Plan
  mutation remains recovery-gated;
- startup Manager recovery must complete before Plan recovery;
- remote consumers must accept the exact planned `ASSIGNING -> ACTIVE` target
  holder change without relaxing other same-generation holder checks;
- Plan attempt `StartedAtMs` and `UpdatedAtMs` must share one clock sample;
- recovery Inspector accepts only same-identity monotonic Prepare deadline and
  Manager re-reads durable Authority before checkpoint/publication;
- replay evidence comparison excludes non-identity `UpdatedAt` touch time;
- production verification recognizes explicit Drain-handoff mode and monotonic
  same-generation deadline renewal while retaining exact replay-floor binding.

Key signed fixes include `9aac55a`, `633b18f`, `d03c69b`, and `5617db5`.
S4-S6 and final RT2Q closure remain pending.
