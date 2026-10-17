# RT2Q S2 Prepare deadline-renewal closure (2026-08-10)

## Verdict

Closed. The earlier post-ASSIGNING SnapshotRevision convergence fix
`3c899900aa770a318102cedaeac35e87d264a0aa` advanced planned Drain to
PREPARING, exposing a second defect: target Prepare legitimately renewed the
same Authority generation and extended `LeaseUntilMs`, but the holder prepare
pipeline required the returned deadline to equal the pre-call deadline.

The signed production correction is
`766f2038170728414278d122a760edaae97cf77c`.

## Corrected contract

- AssignmentRevision, WriterEpoch, LeaseEpoch, HolderID and HolderBootID remain
  exact and immutable throughout Prepare.
- The holder-authorized Renew may only monotonically extend `LeaseUntilMs`.
- `RealMembershipAdapter` returns the exact deadline committed by Renew.
- `PrepareOnlyMembershipInstaller` caches and reports that exact durable
  prepared generation; identity drift or deadline regression remains rejected.
- `RemoteDrainController` accepts only the same exact generation identity and a
  non-regressing deadline.
- After Prepare, Manager re-reads durable Authority without repeating any
  mutation or holder callback. The cold-loaded recovery checkpoint and ACTIVE
  Snapshot bind the exact deadline Prepare accepted.
- Authority mutation, Stop, Wait, Facts and Prepare each remain exactly once.

No Proto, Redis schema/Lua, public API, second state machine, TLS identity or
HolderRegistry authority semantics changed.

## Deterministic verification

Focused deadline-renewal regressions passed `-count=100`; the same set passed
under race `-count=30`. Affected authority-composition, HolderControl, Manager,
RoutePlan and route-controller packages passed.

Three fresh isolated real-production E3 S2 runs passed on exact clean detached
head `5feb60540f2f31decba62a9de39b8f0f276fa79f`:

- `/tmp/rt2q-s2-smoke-5feb605`
- `/tmp/rt2q-s2-smoke2-5feb605`
- `/tmp/rt2q-s2-smoke3-5feb605`

Each run proves observable monotonic Plan phases
`DRAINING -> ASSIGNING -> PREPARING -> SUCCEEDED`, exactly one successful
Stop/Wait/Facts/Prepare, exactly one Authority transfer, and exact-key/process
cleanup PASS. PENDING precedes the held Stop and ACTIVE is an immediate durable
CAS before SUCCEEDED; neither can be deterministically sampled through the
external production API without perturbing execution. Their outcomes are
independently bound by the final SUCCEEDED Plan, exact callback counts and
Authority transfer evidence.

S3-S6, final exact review, Premerge and integration acceptance remain pending.
