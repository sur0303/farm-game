# R2 F1-B / B2-2B physical friendship-roster retirement handoff

Date: 2026-08-10

## Wave4 correctness follow-up: atomic Pair read envelope

- Exact follow-up base: `cd3d98774c0b70354ca8f517ed8783232e2ce2ff` (Controller-committed B2-2B parent). The follow-up remains uncommitted for Controller review.
- Controller composed Backend `2519186194bd48f1305407d456ecb0686c6449e6` with H5 `e3ce394cf77bbe111755fcd5dc576dfd4a557d34`. Runs 1-2 passed; run 3 failed during simulated friend-farm disconnect recovery after the F1 CARE response-loss recovery with `friend: commit attempt reconcile required: bucket pending work count`. Raw evidence is `.agentctl-cp-route-state/evidence/r2-wave4-v2-fault10-20260810T061244Z`; reviewed bundle SHA-256 is `54c4ea4871864cd4d696837561e24fc151d1a99be584398975b495c59437556d`.
- This remains a real `FAIL_CORRECTNESS`, not a timeout or flaky-test waiver. Redis was consistent after the run (`pending_work_count=0`, SourceDue absent), which narrowed the defect but did not justify weakening validation.

The deterministic reproduction proved the defect. The former `validateAuthorityEnvelope` read bucket metadata and the receipt/attempt/SourceDue cardinalities in separate Redis commands. `BatchGetPairs` placed the same commands in one Go pipeline, which reduced network round trips but did not isolate them from other Redis clients. A legitimate `CompletePairActionResultOrder` Lua completion between the metadata read and SourceDue `ZCARD` produces two different valid generations: the captured metadata pending count is exactly one greater than the later SourceDue cardinality. The reader therefore emitted a false `ErrFriendReconcile` even though the completion itself updated SourceDue and both pending counters atomically.

The fix adds one bounded, read-only friendship Redis snapshot script shared by single and batch authority reads:

- one script execution reads canonical key type and exact requested Pair manifest, exact bucket-meta type/manifest/identity/canonical counters, and receipt/attempt/SourceDue key types and cardinalities;
- `GetPair` and `GetPairByRelationID` parse and validate the complete Pair from that same snapshot, so record, relation epoch, block status, metadata and discovery counters cannot come from different commits;
- `BatchGetPairs` deduplicates by physical bucket and pipelines exactly one snapshot invocation per unique bucket. It does not fall back to one round trip per Pair;
- Quota/Share envelope validation uses the same atomic metadata/discovery snapshot. Their existing bounded canonical-record parsing remains unchanged;
- completed attempt compaction remains legal because attempt-due cardinality is a lower bound (`attempt_due <= attempt_count`), while receipt and pending cardinalities remain exact. Current receipt evidence has no reader-side compaction path; receipt creation and attempt preparation are nevertheless covered by the same atomic snapshot.

Persistent corruption still fails closed. The Go validator retains exact meta and Pair manifests, schema/authority/layout/bucket identity, canonical unsigned counter encoding, exact receipt/pending counts, the compacted-attempt lower-bound rule, counter/byte zero parity, canonical record key type, and strict zset key types. A discoverable SourceDue/receipt/attempt orphan without bucket metadata is now also rejected. Tests assert the existing specific classes including `bucket pending work count`, `bucket receipt count`, `bucket attempt count`, `bucket receipt due key type`, and `bucket meta manifest` for both single and batch reads.

Deterministic tests use command-event channels as barriers; they use no sleep or timeout for synchronization:

- `TestRedisFriendPairAtomicSnapshotAcrossPendingWorkCompletion/{GetPair,Permit,BatchGetPairs}` captures the pre-completion metadata count, blocks the snapshot command, commits terminal result work, proves the old mixed observation, releases the snapshot, and proves the canonical read succeeds;
- `TestRedisFriendPairAtomicSnapshotAcrossReceiptAttemptTransitions/{receipt_and_attempt_creation,completed_attempt_compaction}` forces both other bounded lifecycle transitions across the read boundary;
- `TestRedisFriendAuthorityAtomicSnapshotPersistentCorruptionFailsClosed` proves durable metadata/ZSET corruption still returns the specific reconcile classes;
- existing Pair batch tests now prove one pipelined command per unique bucket and shared-bucket deduplication.

The only Lua added by this follow-up is the internal friendship read-only snapshot. No Actor AuthorityStore/Redis Lua/model record, public Proto, H5, Kubernetes, farm-worker source, legacy roster path, friendship location mode, or canonical Pair writer script changed. The bounded snapshot is limited by the existing Pair batch input ceiling; it does not scan Redis or retry an unbounded number of times.

## Scope and evidence authorization

- Exact F1 base: `43cd3f1f1f60e12c6c9187ad677a279e995ace94` (`test(friend): archive legacy observation metrics`).
- The changes are intentionally uncommitted for Controller review/composition.
- This handoff records the evidence-authorized physical retirement of the legacy **relationship roster**. It does not retire the distinct public HTTPS share token-to-issuer projection.
- No public Proto/API, public Block/Unblock route, social-svr/FriendActor/mTLS, Actor AuthorityStore or canonical Friend Lua, H5, Kubernetes manifest, farm-worker source, or Controller state was changed.

The Controller explicitly accepted both prerequisites:

1. `.agentctl-cp-route-state/evidence/r2-f1b-shadow-read-metrics-cpb-20260810T051043Z`, reviewed SHA-256 `5868179e8143eec621f11ee3fa1a42f4b1e2dab826372dfc26ed5dc07439bfc0`, status `ACCEPTED_FOR_B2_2B`. The bounded result was legacy missing=19 / observation class missing=19, legacy extra=0, Pair/Index mismatch=0, unavailable=0, timeout=0, malformed=0, unexplained=0. The 19 were reviewed as `EXPECTED_LEGACY_MISSING_AFTER_FORMAL_DUAL_WRITE_RETIREMENT`; this is classified migration evidence, not a zero-total-difference claim.
2. `.agentctl-cp-route-state/evidence/r2-f1b-retired-cpb-20260810T051347Z`, SHA-256 `d78a4b652913b78952655e38c286d7d241bbd344915355ee445bbdc5267f6db6`, proving the composed `retired` mode remained healthy with the legacy reader structurally unconstructed.

Both Controller runs were local/non-Staging. This Writer did not rerun real Redis, H5, the protocol bot, Hero/Social oracles, or Kubernetes Staging.

## Physical deletions

The following legacy relationship capabilities are removed rather than hidden behind a selector:

- `friendshipservice.LegacyObserver`, bounded observer implementation, Redis roster reader, legacy mode parser/selector, observation metrics, and all associated unit/live tests;
- `FRIEND_LEGACY_MODE`, `SMOKE_FRIEND_LEGACY_MODE`, `shadow_read|retired`, and all Gate/player/smoke construction or manifest fields;
- relationship `friend.Store`, `Mutation`, its Redis Lua/key/receipt implementation, memory implementation, the unused `friend:summary` key helper, and their tests;
- the compatibility `auth.NewShareHandler` path, local relationship fallback branches, old Gateway share Redis store, and their obsolete tests;
- the temporary player `/metrics` archival, legacy summary parser, summary witnesses, and evidence-only smoke fixtures;
- `FRIEND_STORE_PREFIX` from Gate/player/farm production composition, local Docker Compose, backend smoke/perf/demo composition, and command environment snapshots.

Exact deleted files:

```text
internal/friend/store.go
internal/friend/memory.go
internal/friend/memory_test.go
internal/friend/redis.go
internal/friend/redis_live_test.go
internal/friendshipservice/canonical_retirement_test.go
internal/friendshipservice/legacy_metrics.go
internal/friendshipservice/legacy_mode.go
internal/friendshipservice/legacy_mode_test.go
internal/friendshipservice/legacy_observer.go
internal/friendshipservice/legacy_observer_test.go
internal/friendshipservice/redis_legacy_roster.go
internal/friendshipservice/redis_legacy_roster_test.go
internal/friendshipservice/redis_legacy_roster_live_test.go
internal/gateway/auth/share_profile_test.go
internal/gateway/auth/share_redis.go
internal/gateway/auth/share_redis_live_test.go
internal/gateway/auth/share_test.go
scripts/summarize-friend-legacy-metrics
```

The append-only `FriendMutationKind`/`FriendMutationReceipt` public Proto definitions and the historical Tcaplus table message that embeds the receipt remain generated and unchanged. They are no longer consumed by a relationship roster implementation or production HTTP fallback; changing them would violate the no-Proto/no-Actor-layout boundary. Historical B2-1/B2-2A verification documents also remain as durable migration history and therefore still mention the retired names.

## Canonical-only production path

- `FriendPair`, directed Block and `relation_epoch` are the only relationship authority.
- `friendshipservice.Service` derives Redeem/Delete/Block/Unblock outcomes, `changed`, relation ID and epoch exclusively from Domain/Pair outcomes and snapshots.
- Stable Redeem/Delete operation identity was moved out of the deleted roster Store into `PairMutationIdentity`; a golden vector proves its byte layout and output remain unchanged.
- Gate `local_gate` builds canonical Pair/Quota/ShareIssuer/FriendIndex/RepairQueue composition. `remote_shadow` still performs exactly one formal mutation followed only by local/remote canonical observations. `remote_player` still invokes only the RPC factory.
- Farm VisitPermit and ActionAdmission remain Pair/epoch/block/delete fenced; FarmActor remains write-time authority.
- FriendIndex stays acceleration only. Direct Apply/Remove errors cannot rewrite a committed Pair result. The pre-enqueued durable repair and reconciler fill/remove the projection, and read-time Pair revalidation hides stale rows.

## Share-token distinction and observability

The retained `RedisShareProjectionStore` is the renamed HTTPS token-to-issuer lookup required by the frozen public share SCB. `ShareIssuer` remains canonical. The projection performs no FriendPair, Block, Quota, FriendIndex, or relationship receipt mutation; `SHARE_STORE_PREFIX` remains an isolated explicit smoke input. Unit and opt-in `FARM_REDIS_URL` live tests cover its issuer decoding and repeat resolution without consumption.

Legacy missing/extra/observation metrics are removed with their producer. The useful canonical metrics remain live:

- `friend_pair_index_mismatch_total`, incremented from actual reconciler Apply/Remove reports;
- `friend_projection_repair_age_ms`, fed by a bounded process-local timing wrapper around the durable RepairQueue.

No player-ID or unbounded label is added.

## Smoke and production-boundary proofs

`scripts/run-authority-vertical-smoke` and the CP-B wrapper retain the exact `FRIENDSHIP_SERVICE_MODE=local_gate|remote_shadow|remote_player` fail-closed selector, explicit isolated `SHARE_STORE_PREFIX`, player `FARM_ADDR`, secret-redacted start diagnostics, and exact H5 root+port cleanup. The synthetic test proves a Vite process on port 5197 is not matched by a run targeting the default 5194.

The existing manifest field `legacy_fallback:false` is the generic Actor-authority S0 assertion; it is not a friendship mode or rollback selector. No friendship fallback field remains.

The cheap test scans Gate, player-svr, farm-svr, Gateway assembly and the HTTP adapter and fails if any retired environment name, reader/observer, `friend.Store`, `friend.NewRedisStore`, or compatibility constructor returns. It also requires the legacy implementation files to be physically absent and verifies both local Gate and player composition use canonical Redis friendship repositories plus the share projection.

Current static scans find no `FRIEND_LEGACY_MODE` or `FRIEND_STORE_PREFIX` in `cmd/`, `internal/`, `scripts/`, or `deploy/` outside negative assertions in `scripts/run-authority-vertical-smoke-test`. No Kubernetes occurrence exists in this worktree, and Kubernetes files were not edited. Controller should still verify external release values/secret overlays during composition and publication.

Intentional non-production text/schema remnants are exact: the negative static assertions, the historical B2-1/B2-2A verification handoffs and archived Controller evidence, and the append-only Proto/generated Tcaplus receipt definitions described above. Broad `legacy` matches in workflow recovery, permit-secret migration, or unrelated farm tests are outside the retired friendship roster and were not changed.

## Local verification

PASS:

```text
go test -buildvcs=false \
  ./internal/friend ./internal/friendshipservice ./internal/gateway/auth \
  ./internal/gatewayapp ./internal/farm/ingress ./internal/farm/readpath -count=1

go test -race -buildvcs=false \
  ./internal/friend ./internal/friendshipservice ./internal/gateway/auth \
  ./internal/gatewayapp ./internal/farm/ingress -count=1

go test -buildvcs=false ./... -count=1
go vet -buildvcs=false ./...
go build -buildvcs=false \
  ./cmd/farm-gate ./cmd/player-svr ./cmd/farm-svr ./cmd/farm-worker

bash -n scripts/run-authority-vertical-smoke \
  scripts/run-cp-vertical-b-smoke scripts/run-authority-vertical-smoke-test \
  scripts/run-authority-fault-smoke scripts/perf/lib-stack.sh \
  scripts/perf/run-product-anchor-profile scripts/smoke-authority-start.sh \
  scripts/run-theft-200-wss-hot-farm scripts/run-s1a-hardened-10x \
  scripts/test-lease-renew-stress scripts/test-multigate-drain \
  scripts/test-stage5-fault-evidence scripts/test-dualgate-kick

scripts/run-authority-vertical-smoke-test
# authority vertical smoke canonical friendship wiring: PASS

git diff --check
```

The opt-in live share projection test skipped because `FARM_REDIS_URL` was not supplied. No local real-Redis or headed browser result is claimed. Proto input/generated files did not change, so generation was not run.

### Wave4 follow-up local verification

PASS on the `cd3d987...` linked worktree with the uncommitted atomic-read diff:

```text
go test -buildvcs=false ./internal/friend -run \
  'TestRedisFriendPairAtomicSnapshot|TestRedisFriendAuthorityAtomicSnapshot|TestRedisPairBatch' -count=1

go test -buildvcs=false \
  ./internal/friend ./internal/friendshipservice ./internal/farm/ingress \
  ./internal/farm/readpath ./internal/gateway/auth ./internal/gatewayapp -count=1

go test -buildvcs=false \
  ./internal/worker/carerecovery ./internal/worker/theftrecovery \
  ./internal/farm/actor ./internal/player/participant -count=1

go test -race -buildvcs=false \
  ./internal/friend ./internal/friendshipservice \
  ./internal/farm/ingress ./internal/farm/readpath -count=1

go test -buildvcs=false ./... -count=1
go vet -buildvcs=false ./...
go build -buildvcs=false \
  ./cmd/farm-gate ./cmd/player-svr ./cmd/farm-svr \
  ./cmd/farm-worker ./cmd/authority-smoke-fixture

bash -n scripts/run-authority-vertical-smoke \
  scripts/run-cp-vertical-b-smoke scripts/run-authority-vertical-smoke-test
scripts/run-authority-vertical-smoke-test
# authority vertical smoke canonical friendship wiring: PASS

git diff --check
```

Static scans found no production `FRIEND_LEGACY_MODE`, `FRIEND_STORE_PREFIX`, `LegacyObserver`, `RedisLegacyRosterReader`, `friend.NewRedisStore`, or `friend.Store`; the only matches remain negative assertions in `scripts/run-authority-vertical-smoke-test`. Diff scans found no Proto/generated API, Actor AuthorityStore/Lua/model, H5, Kubernetes, farm-worker source, or credential-bearing file change. No local real-Redis, fault-heavy browser, Hero/Social oracle, or Kubernetes Staging run is claimed for this follow-up; Controller must recompose and rerun the serialized Wave4 fault gate.

## Remaining Controller/publication gates

Controller still owns composed real-Redis regression, Hero/Social/fault-heavy CP-B, release-value cleanup verification, Kubernetes Staging, and publication/checkpoint decisions. This handoff does not declare `CP-FRIEND-CANONICAL-ONLY`, Kubernetes Staging, 10/10, formal R2 PASS, or any other checkpoint PASS.
