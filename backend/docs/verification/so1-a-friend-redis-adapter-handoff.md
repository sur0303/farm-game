# SO1-A-FRIEND-REDIS-ADAPTER corrective handoff

- task: `SO1-A-FRIEND-REDIS-ADAPTER` corrective closure
- branch: `codex/so1-a-friend-redis-adapter-20260804`
- final focused-fix starting tip: `c8d3848a5a306c380c2d6eb60cf6064690bd3875`
- scope: `internal/friend/**` and this handoff
- public proto/generated changes: none
- integration merge/MR: none
- real Redis evidence: pending; no controlled Redis endpoint was supplied

## 1. Corrective semantic closure

1. **Pre-mutation validation.** `FriendPrepareCommitAttemptV1` distinguishes a truly absent canonical HASH from a malformed existing HASH. It validates the bounded canonical manifest, exact logical bytes, exact bucket metadata identity/counters, the complete fixed cursor schema, and every supplied SourceOrder tuple/exact manifest/DueIndex before writing the OPEN cursor. Each business Lua script repeats the relevant validation before its first marker/canonical mutation. Missing canonical records initialize only in the operation-specific bootstrap path.
2. **Exact unknown-outcome proof.** FR3 receipts bind `attempt_id`, candidate hash, and ordinal. FCM2 is an immutable, bounded, exact-schema proof of the receipt and immutable SourceOrder/release-fence facts; live proofs are size-checked and parsed before Pair replay or Quota reactivation authorization. It does not compare operation A with a later legitimate canonical version B. FRS3 resolution evidence preserves and revalidates the exact attempt/candidate/marker/receipt identity after marker compaction. Only an explicit pre-mutation script result can create NOT_COMMITTED; an unproved unknown becomes RECONCILE_REQUIRED even when the current business state is unchanged.
3. **Bounded recovery and GC.** No production `HGETALL`, key scan, or unbounded ZSET range is used. Bucket meta maintains `attempt_count` and a conservative `attempt_bytes` reservation. Count/bytes are tied to the authority receipt lifecycle and hard bounded before Prepare. Completed attempts remain in the frozen `attempt-due` index until `AttemptRetentionMs`; `CompactCompletedAttemptMarkers` processes at most 128 exact members, writes FRS3 evidence, deletes the two-field FCM2 marker, decrements marker bytes, and removes discovery atomically. OPEN/RECONCILE evidence remains discoverable for the recovery worker. Automatic unknown retries are capped at four.
4. **Pair replay.** Public caller identity must match the caller. Replay first fails closed on canonical corruption, then verifies the exact identity/payload prefix, FR3 candidate/attempt/ordinal, deterministic attempt ID, COMMITTED cursor, and either FCM2 or compacted FRS3 proof. A later legitimate Pair version does not invalidate prior success, and changed payload returns idempotency conflict.
5. **Bilateral reactivation quota proof.** Reactivation reads each player’s exact live reservation plus its immutable QUOTA_RESERVE receipt, verifies receipt identity/payload/candidate/cursor/attempt and FCM2-or-FRS3 evidence, and binds both proof digests/reservation IDs into the Pair candidate. Pair Lua also requires both proof arguments. Quota keys are never passed to Pair Lua: the frozen Pair and Quota tags remain separate, so no cross-slot EVAL was introduced.
6. **Canonical corruption.** Pair and Share canonical HASHes have exact fixed manifests; Quota has a bounded occupancy-derived manifest with reservation/reverse-link validation. Lua and Go validate schema/layout/identity, numeric/state ranges, and exact logical bytes. Truly absent Pair/Quota/Share records bootstrap through Activate/Reserve/Generate; a partial or malformed existing record fails closed without a cursor.
7. **Capacity and lifecycle consistency.** SourceOrder accounting includes a conservative fixed metadata/Redis overhead and all six tuple values `(id,type,blob,work_ref,payload_hash,due_ms)`. The same formula drives preflight, Lua item checks, and `pending_work_bytes`. Receipt, order, cursor, FCM2, aggregate attempt, and script input limits are bounded. The default single-marker limit is 8 KiB, above the complete normal two-order proof shape; smaller configured limits reject before Prepare. Attempt-marker retention cannot exceed receipt retention.
8. **Complete SourceOrder binding.** Prepare, authoritative reads, receipt-missing action recovery, marker proof, and Permit reconstruction now use the same fixed BASE/ACTION_RESULT/DISPATCH shapes. They validate exact HLEN/manifest, schema and placement identity, source authority, work id/type/ref, exact typed payload and SHA-256, DueIndex score, frozen initial state/version/retry/lease/timestamps, and every action/Permit scalar. Missing or additional fields and any binding mismatch return reconciliation before authority state is changed.
9. **Resolution-before-compaction.** FCM2 compaction creates FRS3 with `HSETNX`, accepts replay only when the existing FRS3 value is byte-for-byte identical after canonical parsing, and only then removes the marker and advances cursor/discovery accounting. Fault seams before FRS3, after FRS3, after marker removal, after the cursor resolution id, and after discovery removal prove that an error never leaves a COMMITTED cursor with both FCM2 and FRS3 absent; once FRS3 exists, replay finishes cleanup.
10. **Canonical record/meta envelope.** Pair, Quota, and ShareIssuer reads, Prepare, business writes/replays, resolution, compaction, and Quota reactivation proof validate exact bucket-meta HLEN/manifest/identity/canonical numbers and bounded collection counts before adjudication. Pair/Share fixed records and the bounded occupancy-derived Quota manifest are checked with exact HLEN and logical bytes. An existing authority without meta, partial/extra meta, wrong authority/layout/bucket, malformed canonical numbers, and forged counts fail closed. Meta initialization is enabled only for the operation-specific legal create scripts; an absent non-create writes neither authority nor meta.

## 2. Frozen physical layout and atomicity

The corrective patch adds no key family and does not change the 33A HashTag layout. It specifically removes the abandoned `quota active-index` direction. Canonical keys remain:

```text
Pair:        {classic-farm:<env>:<zone>:friend-auth:pair:v1:b:<bucket>}:...
Quota:       {classic-farm:<env>:<zone>:friend-auth:quota:v1:b:<bucket>}:...
ShareIssuer: {classic-farm:<env>:<zone>:friend-auth:share:v1:b:<bucket>}:...
```

All KEYS passed to one Lua script are asserted same-slot in Go and rechecked by Lua. Pair reactivation performs read-only, exact proof acquisition from the two Quota authorities before the Pair commit; it does not pretend the three distinct authority tags can participate in one Redis transaction.

The bounded recovery schemas are:

```text
friend-commit-attempt-v1 cursor: 13 fixed fields, <= 2 KiB reservation
FCM2 completed marker:            one LP proof value + one complete field, <= 8 KiB
FRS3 resolution:                  exact status/attempt/candidate/marker/receipt binding
bucket-meta additions:            attempt_count, attempt_bytes
```

`receipt-due` and `attempt-due` members are length-prefixed locators. Their decoders reject malformed/trailing data and never discover evidence by scanning partitions.

## 3. Durable regression evidence

The focused suite covers:

- malformed/wrong-identity bucket meta, partial cursor, malformed canonical Pair/Quota/Share, and partial FCM2 with no partial authority mutation;
- corrupt/missing/extra SourceOrder fields and DueIndex, including proof that Prepare did not write a cursor;
- receipt-missing action recovery with corrupt Result/Dispatch schema, source authority, bucket, work ref, typed payload/hash, frozen state/version, and extra fields, including unchanged Pair version;
- exact COMMITTED resolution for operation A after operation B advances the record, across a reconstructed repository client;
- exact Pair replay after a later Unblock and restart, changed-payload conflict, malformed live-marker rejection, and compacted-marker replay;
- release-fence proof/DueIndex corruption and release-fence capacity before Quota mutation;
- unilateral reactivation reservation rejection and bilateral exact reservation/receipt proof success;
- completed-marker retention/compaction, attempt-byte decrement, bounded discovery, and one-identity attempt-capacity pressure;
- injected FCM2 compaction failures at every ordered seam, identical-resolution replay completion, conflicting-resolution fail-closed behavior, and the invariant that COMMITTED never loses both marker and resolution;
- Pair/Quota/ShareIssuer missing/partial/extra/wrong meta, canonical extra fields, forged collection counts, missing-meta writes, reactivation-proof corruption, and absent non-create no-initialization behavior;
- SourceOrder ID/metadata byte accounting and receipt/order/attempt due/counter consistency;
- 40 concurrent reservations against a 30-slot quota, yielding exactly 30 reservations and 10 domain rejections;
- runtime partial mutation, explicit deterministic NOT_COMMITTED fencing, unchanged-but-unproved unknown reconciliation, no blind OPEN resend, and bounded automatic retry;
- frozen hard-limit overflow, NOSCRIPT reload, malformed script reply, delimiter-safe encoding, action permit recovery, barrier/floor/quota cleanup, and full Pair/Quota/Share lifecycle.

The 40-client concurrency case remains a fast miniredis unit test only. Focused commands completed with exit 0:

```text
GOCACHE=/tmp/so1-gocache go test -count=1 -run '^TestRedisFriend' -timeout=180s ./internal/friend
GOCACHE=/tmp/so1-gocache go test -count=1 -timeout=180s ./internal/friend
```

## 4. Final candidate verification

The final directed candidate verification record is:

```text
go test -count=1 ./internal/friend/...                     PASS
go test -count=1 -race ./internal/friend/...               PASS
go vet ./internal/friend/...                               PASS
go test -count=1 ./...                                     PASS
go vet ./...                                               PASS
go build -buildvcs=false ./...                             PASS
./scripts/gen-proto.sh                                     PASS
git diff --exit-code -- api proto api/gen                  PASS (zero diff)
git diff --check                                           PASS
```

No `debug_marker_test.go`, debug print/logging, public proto edit, or generated-file diff remains in the candidate.

### 4.1 Focused closure after final Redis-adapter review

The four deterministic review findings discovered after `c8d3848` are closed:

1. Existing receipt, `ALREADY_COMMITTED`, `ATTEMPT_CONFLICT`, and `REPLAY` paths all pass the same committed-proof gate: COMMITTED cursor, deterministic attempt ID, and FCM2-or-FRS3 proof.
2. Quota business digests are strict lowercase hexadecimal; Quota and ShareIssuer `updated_at_ms` values are non-negative canonical unsigned values.
3. An existing Pair HASH containing only unknown fields fails closed and cannot be treated as absent for recreation.
4. Lua resolution replay rejects non-canonical FRS3 `receiptEpoch` values such as `01`.

The upper controller reran the final working tree outside the Codex network sandbox:

```text
go test -count=1 ./internal/friend/...                     PASS
go test -race -count=1 ./internal/friend/...               PASS
go vet ./internal/friend/...                               PASS
git diff --check -- internal/friend docs/verification/...  PASS
```

A final read-only review was restricted to these four findings and returned `APPROVED` / `READY_FOR_INTEGRATION: YES`. The real-Redis work below remains an Evidence Bundle and does not block component integration.

## 5. Residual real-Redis requirements

These are real integration requirements, not remaining in-package P1 defects:

- run `TestRedisFriendAuthorityRealRedisProbe` against a controlled Redis deployment with the production ACL/TLS/noeviction policy;
- exercise Redis Cluster routing for every script and confirm Pair/Quota/Share proof acquisition never becomes a cross-slot Lua call;
- measure the bounded Quota manifest validation and FCM2/FRS3 scripts at high occupancy on real Redis (miniredis timing is not capacity evidence);
- run the 300-friend quota/concurrency/capacity case on real Redis; the 40-client miniredis test is intentionally retained only as fast unit evidence and is not a substitute;
- exercise failover/connection-loss seams after Prepare, incomplete FCM2, and complete FCM2, then run the recovery worker from `attempt-due`;
- schedule completed-marker compaction and the separate receipt/SourceOrder/attempt-resolution archive/expiry workers; cursor/resolution evidence intentionally has no ordinary TTL and must be archived under the platform retention policy;
- validate production receipt/source retention is no shorter than the configured attempt-marker retention;
- complete the existing AEAD/KMS token-envelope, observability, legacy cutover/backfill, and projection-worker integration work owned outside SO1.

Until those probes run, this handoff claims component-contract/miniredis evidence only, not `REAL_REDIS_PASS` or production capacity.

## 6. Delivery record

The implementation and this handoff are committed together on the named branch and pushed directly to the matching remote branch. The immutable final tip and clean local/remote proof are reported in the delivery response; no integration merge or MR is created.
