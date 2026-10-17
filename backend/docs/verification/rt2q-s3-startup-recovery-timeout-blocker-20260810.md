# RT2Q S3 startup recovery blocker closure (2026-08-10)

## Verdict

Closed. The initial S3 evidence correctly proved that pre-serve Manager recovery
could not wait for consumers to install the ASSIGNING Snapshot, and that Plan
recovery could then freeze `authority_committed_snapshot_pending` as PAUSED.

The final production ordering is:

1. construct the single controller runtime and keep Plan readiness false;
2. open authenticated Watch so consumers can install recovery publications;
3. run bounded Manager pending-projection recovery;
4. run Plan recovery only after Manager recovery succeeds;
5. mark Plan service ready and start the single-owner background recovery loop.

Plan mutation returns `Unavailable` while startup recovery is incomplete. The
validated Plan operation budget is at least target convergence timeout plus
headroom.

Subsequent live work also closed the exact recovery seams exposed after this
ordering fix: planned target activation validation, Prepare deadline
reconciliation, Plan attempt timestamp linearization, replay-evidence touch-time
comparison, explicit Drain mode verification, and monotonic Renew verification.

S3 passed three fresh isolated real-production E3 runs on
`5617db5fda2ab1b388b537ac06fd03b90572ffc8`:

- `/tmp/rt2q-s3-smoke-5617db5`
- `/tmp/rt2q-s3-smoke2-5617db5`
- `/tmp/rt2q-s3-smoke3-5617db5`

The former PAUSED blocker is no longer open. S4-S6 remain pending.
