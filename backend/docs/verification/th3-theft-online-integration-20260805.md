# TH3 Theft P0/P1 directed closure verification — 2026-08-05

This record covers the six directed review findings against baseline
`b977831cf28057b50f4fa609d918a5f5c69e0f31`. The frozen Theft business rules
were not redesigned, and no file under `docs/design/**` was changed.

## Directed fixes

### 1. Cross-Pod Pair record-version refresh

- `RedisPairRepository.CommitFriendActionAdmission` now uses the existing
  guarded record-version refresh. Its candidate proof allows only
  `record_version` to refresh; operation, payload, target binding, permit and
  workflow TTLs, relation epoch, and source-order tuples remain immutable.
- Farm ingress no longer has one process-global Theft mutex. A 64-stripe lock
  keyed by canonical Pair is only a local contention optimization; Redis is
  the cross-Pod correctness boundary.
- `TestTheftAdmissionTwoServicesRefreshConcurrentPairRecordVersion` uses two
  independent Service instances, Redis clients, and Pair repositories against
  the same Redis data. Concurrent valid operations both reach `PROCESSING`,
  while the same operation with a different payload remains an idempotency
  conflict.
- `TestTheftAdmissionDifferentPairsAreNotGloballySerialized` proves a blocked
  Pair does not serialize an unrelated Pair.

### 2. Stable Pair DueIndex discovery and bounded bucket scanning

- Pair discovery no longer stores a rank offset for a mutable ZSET. Every page
  reads the due head at offset zero; a bounded head fingerprint distinguishes
  a moved claim/reschedule prefix from an unchanged malformed or held prefix.
- One round scans 64 buckets by default and never more than 256. The durable
  bucket cursor advances across rounds, so buckets above 256 remain reachable
  without querying 4096 empty buckets every 250 ms.
- Covered by
  `TestRedisPairDiscoveryHeadPagingDoesNotSkipAfterScoreMoves`,
  `TestRedisPairDiscoveryBucketCursorReachesBeyond256`, and
  `TestRedisPairDiscoveryEmptyRoundHonorsBucketQueryBudget`.

### 3. Atomic GuardSync monotonic write

- Guard snapshot compare-and-write is one Redis Lua operation. It accepts an
  exact replay or a strictly higher version, and rejects an older version or a
  different payload at the same version.
- `TestRedisGuardSnapshotStoreConcurrentV3V4NeverRegresses` runs 100 rounds of
  32 concurrent v3/v4 writers and verifies every final snapshot is v4.

### 4. Deployment-provided production Theft PRF key

- `farm-svr` defaults `THEFT_PRF_MODE` to `production` and fails startup before
  opening stores or serving readiness unless `THEFT_PRF_HMAC_SECRET` is at
  least 32 bytes. The secret is injected as
  `farmactor.HMACTheftPRF(secret)`.
- A fixed local key is reachable only under explicitly selected `test` or
  `demo` mode; it is not a production default.
- `TestTheftPRFDeploymentConfigurationFailsClosed` covers missing, short, and
  invalid production configuration plus valid production and explicit demo.
- The real E2E harness supplies an explicit test-mode key.

### 5. Permit expiry no longer truncates accepted workflows

- FRIEND_PAIR admission atomically freezes
  `workflow_deadline_at_ms`, independent of the short permit expiry, in the
  typed receipt and dispatch source order. Replay validates the same immutable
  workflow TTL.
- Online creation and worker recreation both use that exact absolute deadline;
  record/order reconciliation also binds it. Permit expiry is used only for
  first admission.
- `TestFriendPairRecoveryAfterPermitExpiryUsesFrozenWorkflowDeadline` recovers
  a response-loss order after permit expiry without any online WorkRecord.
- `TestAcceptedWorkflowRestartAfterPermitExpiryCompletesFromReceipts` crosses
  permit expiry after a lost Farm response, restarts the coordinator, replays
  the Farm receipt, and completes visitor and owner settlement.

### 6. Pair admission-to-WorkRecord result gap

- `GET_OPERATION_RESULT` has a precise FRIEND_PAIR-root fallback. It derives
  the Pair bucket, operation digest, and ResultPublishOrder locator, then
  validates the persisted ResultPublishOrder and its dispatch anchor. It
  returns `PROCESSING` only when subject, operation digest, Pair root,
  WorkAddress identity, payload and target hashes, and workflow deadline all
  agree.
- Terminal Theft publication now uses the existing Redis ResultDirectory,
  binding subject, operation digest, `FRIEND_PAIR` root, and canonical
  WorkAddress source reference. The removed Theft-specific terminal keyspace
  is not a second result directory.
- `TestOperationResultTheftAdmissionCrashGapIsProcessingThenTerminal` verifies
  `PROCESSING` immediately after atomic admission and before WorkRecord
  creation, payload conflict preservation, and the recovered terminal query.
- `TestRedisResultPublisherUsesExistingResultDirectoryAndBindsPairWorkAddress`
  verifies terminal binding, exact replay, and conflicting terminal rejection.

### E2E-exposed lease ownership correction

The first Theft-pet run exposed that the shared PLANT/HARVEST recovery loop
could lease a THEFT WorkRecord and then count it as `Skipped`, racing the Pair
recovery root. Generic recovery now pushes its registered WorkTypes into the
same atomic Redis claim. `TestRedisTypedClaimDoesNotStealTheftLease` verifies
that THEFT owner/version/epoch are untouched and the Pair worker can claim it
immediately, including compatibility with records lacking the new auxiliary
`work_type` hash field.

## Commands, results, and raw evidence

### Focused related packages

```text
go test -count=1 ./cmd/farm-svr ./internal/farm/ingress ./internal/worker/theftrecovery ./internal/theftonline ./internal/player/ingress ./internal/workflow ./internal/friend ./internal/scenario/theft
```

Result: PASS. Raw output:
`/tmp/th3-p0p1-focused-20260805.log`.

### Full suite

```text
go test -count=1 ./...
```

Result: PASS. Raw output:
`/tmp/th3-p0p1-full-test-20260805.log`.

### Full race suite

```text
go test -race -count=1 ./...
```

Result: PASS. Raw output:
`/tmp/th3-p0p1-full-race-20260805.log`.

### Real Redis multi-process E2E: no pet

```text
PO1_E2E_MODE=THEFT go run ./cmd/po1-plant-e2e
```

Result: PASS using real Redis 7.4.10 and separate `player-svr`, `farm-svr`, and
`farm-worker` processes. Evidence includes immature rejection, one legal 2%
slot under concurrent operations, replay without duplicate reward,
response-loss `PROCESSING`, and Farm/Player/Worker restart to the same
FRIEND_PAIR workflow. Raw output:
`/tmp/th3-p0p1-e2e-theft-20260805.log`.

### Real Redis multi-process E2E: full pet interception

```text
PO1_E2E_MODE=THEFT_PET go run ./cmd/po1-plant-e2e
```

Result: PASS using real Redis 7.4.10 and separate `player-svr`, `farm-svr`, and
`farm-worker` processes. A persisted full-intercept GuardSync snapshot reduced
visitor net crop to zero without changing visitor coin and credited ordinary
guard coins to the owner; response loss and full process restart still
completed all receipts. Recovery logs contain no THEFT lease loss or generic
`Skipped` claim. Raw output:
`/tmp/th3-p0p1-e2e-theft-pet-20260805.log`.

### Diff hygiene

```text
git diff --check
```

Result: PASS.

## Capacity evidence still not completed

There is still no real 200-concurrent-WSS end-to-end capacity run for this
closure. Existing Actor-level tests with 200 in-process subscriptions exercise
fan-out behavior only; they are not equivalent to 200 real WebSocket sessions
through Gateway, routing, FarmActor, Redis, and downlink, and are not claimed
as that evidence here.
