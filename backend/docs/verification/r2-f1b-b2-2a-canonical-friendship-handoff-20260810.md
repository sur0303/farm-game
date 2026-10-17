# R2 F1-B / B2-2A canonical friendship handoff

Date: 2026-08-10

## Scope and evidence boundary

- Current Controller-committed F1 base for this evidence-capture follow-up: `df6614f401918cac07ca6ba34fdfa4d857bd5a90` (`feat(friend): retire legacy roster authority path`).
- This is the B2-2A canonical-only formal path plus bounded legacy `shadow_read` candidate.
- The final read-only comparator and legacy relationship implementation are intentionally not physically deleted. That evidence-gated deletion is B2-2B in this same Writer thread.
- `RedisLegacyShareStore` remains the HTTPS share-token lookup projection. It is not relationship authority and is not the retired legacy relationship roster.
- No public Proto/API, public Block/Unblock route, social-svr/FriendActor/mTLS, Actor AuthorityStore layout, H5, Kubernetes manifest, farm-worker, deploy, or Controller state was changed.
- The diff is intentionally uncommitted for Controller composition.

Controller-provided composed evidence after committing B2-2A records:

- local real-Redis read-only adapter PASS at `.agentctl-cp-route-state/evidence/r2-f1b-live-redis-20260810T045012Z`, SHA-256 `6f56428ce7c5249321327c78ae3ce94aaa161f20bdf461d67d379aa457248dcb`;
- composed full/race/vet/build PASS at `.agentctl-cp-route-state/evidence/r2-wave3-composed-static-20260810T045242Z`, SHA-256 `cd0cd96d5afdee2cdabd611f41bd8e15601fa67d19877eedacad1c03470c2002` (staticcheck was unavailable and is not claimed);
- headed real-Redis `remote_player + shadow_read` CP-B/H5 F1/F2/F3, protocol-bot, and authority/social-oracle PASS at `.agentctl-cp-route-state/evidence/r2-f1b-shadow-read-cpb-20260810T045449Z`, SHA-256 `1b1ba3881341f787a281c4ec3934c19e3cb67ba764d85910fde1602f32b335cb`, on composed Backend `6c3a226715c73501bf14e2b2671b4a8b56db3eb3` and H5 `e3ce394...`, with `formal_staging=false` and secret scan PASS.

Those are Controller results, not re-claimed as results of this uncommitted harness-only follow-up. The headed run predated player metrics archival, so it does not yet provide the actual bounded classification summary required for B2-2B review.

## Canonical formal path

`FriendPair`, directed Block, and `relation_epoch` are now the only formal relationship facts:

- `friendshipservice.Service` has no `Relationships friend.Store` dependency.
- Redeem, Delete, Block, and Unblock never call legacy `Get` or `Mutate`. `status`, `reason`, `changed`, relation ID, and relation epoch come from Domain/Pair outcomes and snapshots.
- Delete derives its stable mutation identity from the canonical Pair epoch. A non-active relationship returns the frozen Pair result `NO_CHANGE / NOT_FRIENDS`; the legacy-only 404 is no longer a formal result source.
- Pair success is returned even when direct FriendIndex Apply/Remove fails. Both viewers' durable repair intents are enqueued before mutation; direct projection is acceleration only.
- ReadAPI rechecks every FriendIndex candidate against Pair. A deleted/blocked Pair immediately hides a stale row.
- Unblock performs best-effort direct projection only when the canonical relationship is active and unblocked; durable reconciliation remains recovery.
- Farm-svr no longer constructs `friend.RedisStore`, reads `FRIEND_STORE_PREFIX`, or installs `SetFriendChecker`.
- `friend.PermitService` issues and verifies against Pair, including epoch, delete, and either-direction block fencing. VisitPermit and ActionAdmission continue to use Pair authority and FarmActor write-time checks.
- The compatibility-only `auth.NewShareHandler` is documented as a unit-test constructor. Production Gateway composition exclusively uses `NewFriendshipShareHandler`.

The formal behavior is identical under both legacy modes. A legacy observer result, timeout, malformed value, or outage is discarded after metrics and cannot replace the canonical response.

## Legacy mode matrix

`FRIEND_LEGACY_MODE` accepts exactly:

| Mode | Formal path | Legacy roster construction | Observation |
| --- | --- | --- | --- |
| absent | canonical-only | read-only reader | defaults deliberately to `shadow_read` for the B2-2A evidence window |
| `shadow_read` | canonical-only | `RedisLegacyRosterReader` only | bounded read-only Pair/Index/legacy comparison |
| `retired` | canonical-only | none | none |
| empty/unknown | none | none | startup fails closed |

`SelectLegacyObserver` makes the `retired` guarantee structural: its reader factory is not called. In `remote_player`, Gate still invokes only the RPC factory, so it constructs neither Friend authority repositories/scripts/workers nor the legacy reader. player-svr remains the canonical owner.

Each comparison has one 25 ms budget, accepts at most 64 explicit pairs, deduplicates/sorts those pairs, and performs no scan. It classifies `equal`, `missing`, `extra`, `mismatch`, `unavailable`, `timeout`, and `malformed`; unavailable/timeout are never reported as zero difference. No metric has player ID or another unbounded label.

Required metrics are:

- `friend_legacy_missing_total`
- `friend_legacy_extra_total`
- `friend_pair_index_mismatch_total`
- `friend_projection_repair_age_ms`

`friend_legacy_observation_total{result}` records only the seven bounded result classes above. The repair-age gauge is fed by a bounded process-local timing wrapper around the durable RepairQueue; the queue itself remains the durable source and its retry semantics are unchanged.

## Projection and outage coverage

Deterministic tests cover:

- Pair success plus direct Apply failure on Accept, temporary list omission, and reconciler fill.
- Pair success plus direct Remove failure on Delete and Block, stale-index hiding, VisitPermit rejection, ActionAdmission rejection, and reconciler removal.
- Unblock `NO_CHANGE` with direct Apply failure and later repair.
- Accept friend-limit rejection with no Pair result and no projection write.
- replay/`NO_CHANGE`, canonical `changed`, and pre-mutation repair-enqueue failure fencing for Accept/Delete/Block/Unblock.
- a legacy roster that remains unchanged through formal Redeem/Delete/Block/Unblock.
- `shadow_read` equal, missing, extra, Pair/Index mismatch, stale-index mismatch, unavailable, malformed, and strict timeout.
- legacy outage/timeout leaving canonical mutation success unchanged.
- `retired` skipping legacy reader construction.
- Pair-backed permit epoch/delete/block fencing.
- read-only Redis legacy observation preserving all keys/fields.
- remote-player Gate's zero-local-factory invariant and public Block/Unblock absence from the existing B2-1 suite.

## Smoke harness

The harness now also validates `SMOKE_FRIEND_LEGACY_MODE=shadow_read|retired`, defaults it to `shadow_read`, passes it explicitly to Gate and player-svr, and records `friend_legacy_mode` in the credential-free run manifest. Farm-svr no longer receives `FRIEND_STORE_PREFIX`.

H5 lifecycle cleanup is scoped to both the exact H5 root and exact `SMOKE_H5_PORT` (default `5194`). That port is passed as `FARM_E2E_H5_PORT`; cleanup cannot match a Vite process from the same H5 root on port 5197. The cheap test exercises this matching without starting Vite, Redis, services, or a browser.

After browser/protocol actions and the authority audit, but before any final verdict or service teardown, the harness now scrapes only player-svr's private health listener at `http://127.0.0.1:$PLAYER_HEALTH/metrics`. The bounded curl writes the successful source scrape atomically to `player-metrics.prom`. A transport, timeout, or HTTP failure produces `INCONCLUSIVE`; Gate is never scraped. A successful but empty or incomplete scrape reaches the parser and produces `FAIL` rather than synthetic zero values.

`scripts/summarize-friend-legacy-metrics` allowlists the five required metric families and writes `friend-legacy-observation-summary.json`. The summary contains `friend_legacy_mode`, all four scalar values, and all seven allowed `friend_legacy_observation_total` result classes; a class absent from an otherwise present observation family is emitted as numeric zero. Duplicate samples, nonfinite or negative values, extra labels, unknown result labels, malformed required samples, and any wholly missing required base family are rejected. A valid scrape with invalid or incomplete required metrics produces `FAIL`, and no summary is left behind. Unrelated runtime metric names and labels are never copied into the credential-free JSON.

Both files are named in `run-manifest.json` under `witness`:

- `player_metrics`: `player-metrics.prom`
- `friend_legacy_observation_summary`: `friend-legacy-observation-summary.json`

The manifest entries identify required witnesses; an early `INCONCLUSIVE` run may terminate before they exist and cannot be interpreted as zero observations.

A later Controller run on a clean composed Merge Queue can use:

```bash
SMOKE_FRIENDSHIP_SERVICE_MODE=remote_player \
SMOKE_FRIEND_LEGACY_MODE=shadow_read \
SMOKE_H5_PORT=5194 \
H5_ROOT=/absolute/path/to/the/composed-h5-worktree \
SMOKE_REQUIRE_SOCIAL_ORACLE=1 \
SMOKE_E2E_COMMAND_RESULT_FAULTS=1 \
CPB_SMOKE_RUN_ID="r2f1b$(date +%s)" \
CPB_RUN_ID="r2f1b-shadow-read-$(date -u +%Y%m%dT%H%M%SZ)" \
./scripts/run-cp-vertical-b-smoke
```

This is a local smoke/evidence command, not formal Kubernetes Staging.

## Local verification

Committed B2-2A verification recorded PASS:

```text
go test -buildvcs=false \
  ./internal/friendshipservice ./internal/friend ./internal/farm/ingress \
  ./internal/gatewayapp ./cmd/player-svr ./cmd/farm-svr -count=1

go test -race -buildvcs=false \
  ./internal/friendshipservice ./internal/friend ./internal/farm/ingress \
  ./internal/gatewayapp -count=1

go vet -buildvcs=false \
  ./internal/friendshipservice ./internal/friend ./internal/farm/ingress \
  ./internal/gatewayapp ./cmd/player-svr ./cmd/farm-svr

go build -buildvcs=false \
  ./cmd/farm-gate ./cmd/player-svr ./cmd/farm-svr ./cmd/farm-worker

go test -buildvcs=false ./... -count=1
```

This evidence-capture follow-up passed:

```text
bash -n scripts/run-authority-vertical-smoke \
  scripts/run-cp-vertical-b-smoke scripts/run-authority-vertical-smoke-test

python3 -c 'from pathlib import Path; compile(Path("scripts/summarize-friend-legacy-metrics").read_text(), "scripts/summarize-friend-legacy-metrics", "exec")'

scripts/run-authority-vertical-smoke-test
# authority vertical smoke friendship wiring: PASS

git diff --check
```

The follow-up changes only the smoke harness, its standalone parser/test, and this handoff; no Go or Proto input changed, so Go tests were not rerun. The cheap test uses synthetic Prometheus fixtures and does not start Redis, backend services, Vite, or a browser. The prior miniredis and Go results above belong to the committed B2-2A change; this follow-up does not claim a new local live-Redis run. `shellcheck` and `ruff` were unavailable and are not claimed.

## Remaining B2-2B gate

Do not delete the final comparator yet. The Controller must rerun the composed `remote_player + shadow_read` smoke and inspect the actual `friend-legacy-observation-summary.json`; the presence of a scrape or an absent label series alone is not zero-difference evidence. B2-2B requires Controller review of those bounded values showing zero or explicitly accepted/classified differences, followed by:

1. select `retired` in the composed candidate and prove canonical business remains healthy;
2. physically remove the read-only legacy roster adapter, relationship Store/config/key compatibility, and obsolete relationship-roster tests;
3. retain or replace the distinct HTTPS token-to-issuer lookup before changing `RedisLegacyShareStore`;
4. repeat the required Hero/Social, failure matrix, repair, legacy-outage, and staging gates.

B2-2A is not `CP-FRIEND-CANONICAL-ONLY`, Kubernetes Staging, or R2 PASS.
