# R2 F1-A FriendshipDomain boundary handoff

Date: 2026-08-10

## Scope and base

- Exact base: `54c6d0a15cd2e0555d309b59319f1ce30fab268c`
- Branch/worktree: `codex/r2-f1-friendship-boundary-20260809`
- Scope: B2-1 Wave 1 only. B2-2 legacy-roster retirement has not started.
- No changes were made under `deploy/k8s`, `cmd/farm-worker`, Actor `AuthorityStore`, or Controller state.
- No social-svr, FriendActor, mTLS, public Block/Unblock endpoint, or public Proto mutation was added.
- Commit status: intentionally not committed per the post-audit instruction. The bounded diff is left unstaged and commit-ready.

## Verification-harness composition follow-up

The authority vertical smoke harness now accepts `SMOKE_FRIENDSHIP_SERVICE_MODE` with exactly `local_gate`, `remote_shadow`, or `remote_player`. An absent variable defaults to `local_gate` for rollback compatibility; explicitly empty and unknown values fail closed with exit `2` before build/start. The CP-B wrapper validates and exports the same selector as `FRIENDSHIP_SERVICE_MODE` to farm-gate.

Every smoke run allocates and propagates the same isolated `FRIEND_STORE_PREFIX=smoke:<run>:friend` and `SHARE_STORE_PREFIX=smoke:<run>:share` to farm-gate and player-svr. Player-svr also receives its allocated `FARM_ADDR`, enabling canonical player-owned opportunity/list reads. The run manifest records `friendship_service_mode`, `friend_store_prefix`, and `share_store_prefix`; credentials are not included. `remote_player` therefore remains Gate RPC-only, while `remote_shadow` retains one formal mutation followed by local/remote read-only canonical observations.

Cheap wiring coverage is `scripts/run-authority-vertical-smoke-test`; it runs `bash -n`, checks exact mode/prefix/address/manifest seams, and proves empty/unknown mode values fail closed without starting services.

The smoke `start()` diagnostic now renders a shell-quoted, diagnostic-only copy of each argv element. Redis/rediss URI userinfo and allowlisted password/token/secret-style environment assignments are replaced with `<redacted>`; the actual `"$@"` process argv is untouched and no diagnostic string is re-evaluated. The same cheap regression feeds synthetic sentinel values, asserts they cannot appear, and retains executable, mode, address, port, and isolated-prefix fields.

## Result

`FriendshipService` is an internal plaintext gRPC boundary in the `friendshiprpc` namespace. It implements:

- `CreateShare`
- `ResolveShare`
- `RedeemShare`
- `GetFriendList`
- `DeleteFriend`
- `BlockFriend`
- `UnblockFriend`
- `GetNextFriendOpportunity`
- `AuthorizeVisit`
- `CommitFriendActionAdmission`
- `GetFriendActionAdmission` (read-only unknown-result resolution used by shadow and retry)

Every player-svr instance registers the service beside, not inside, `PlayerActor`. The stateless application object uses shared Pair, Quota, ShareIssuer, FriendIndex, legacy projection, navigation and repair repositories. player-svr also composes the public-profile batch reader, Farm opportunity reader, VisitPermit/ActionAdmission service, repair queue worker and reconciler.

In `remote_player`, farm-gate constructs only the authenticated HTTP adapter, the bounded per-IP limiter and the bounded Friendship gRPC client. The local factory contains all Friend Redis repository, Lua-load, repair-worker and hint-subscriber construction and is never invoked by that mode.

## Controller-audit correctness fixes

1. `RedeemShare` omits the RPC `Pair` field for a canonical rejection with no Pair facts (including friend-limit rejection), while retaining Outcome, inviter identity and inviter profile. `SUCCEEDED` and `NO_CHANGE` require a present, structurally valid active Pair at the client boundary; an optional Pair on other rejected results is validated when present.
2. After the single formal mutation, mutation shadow invokes `ObservationOnly=true` on both the local formal client and the remote observer, then compares those two canonical observations. It compares token identity/generation/timestamps for Create; Pair relation ID/epoch/status plus inviter/profile for Redeem; and Pair relation ID/epoch/status for Delete/Block/Unblock. The original formal business response is returned unchanged, so a rejected Redeem may omit Pair facts while equal local/remote observations expose the same blocked/deleted Pair. Synthesized observation Outcome and `changed` values are not compared. ActionAdmission retains one Commit plus the existing remote read-only receipt lookup because its formal receipt is complete.
3. `UnblockFriend` durably enqueues repair for both viewers before the canonical Pair commit. Enqueue failure returns an error without unblocking. A deterministic regression removes both active FriendIndex rows, exercises no-change Unblock, and proves reconciliation restores both projections from Pair/Quota authority.
4. The remote HTTP adapter maps internal self-invite to the same explicit public message as `local_gate` (`cannot open your own share link`) while preserving the frozen `ERR_SHARE_LINK_INVALID` code. No public Block/Unblock endpoint was added.

## Mode matrix

| `FRIENDSHIP_SERVICE_MODE` | Formal result | Gate Friendship authority composition | Shadow behavior |
| --- | --- | --- | --- |
| variable absent | `local_gate` | R1-compatible local service/repositories/workers | none |
| `local_gate` | local Gate path | local Pair/Quota/Share/Index, legacy projections, repair and hint subscriber | none |
| `remote_shadow` | local Gate path | local formal path plus bounded player-svr RPC client | reads compare old/new; formal mutations execute once, then local/remote observation-only reads compare committed state |
| `remote_player` | player-svr RPC | none | none |
| present but empty/unknown | startup failure | none | fail closed |

The absent-variable default is deliberately `local_gate` for frozen-R1 rollback compatibility. An explicitly empty value is invalid; it cannot silently select rollback mode.

RPC calls use `FRIENDSHIP_RPC_TIMEOUT` (default `2s`). Friend HTTP rate limiting uses `GATE_FRIENDSHIP_HTTP_RATE_PER_SEC` (default `20`) and `GATE_FRIENDSHIP_HTTP_RATE_BURST` (default `40`). JSON mutation bodies are capped at 4 KiB and must contain exactly one JSON value.

## Shadow and authority semantics

- Share/Pair/Quota business mutation is invoked exactly once on the formal local client.
- Share creation/redeem/delete/block/unblock perform one formal write followed by local and remote `observation_only` reads; ActionAdmission observes through `GetFriendActionAdmission` instead of calling Commit twice.
- VisitPermit authorization is a deterministic canonical Pair read and does not persist a second business mutation.
- Post-mutation comparisons cover only committed canonical facts and never compare synthesized mutation Outcome/`changed` values. Read comparisons cover complete ActionAdmission receipts, relation ID/status/epoch, profiles, ordered friend pages, action masks, eligible theft targets and FOUND/NONE/UNKNOWN next-opportunity state as applicable.
- UNKNOWN and NONE remain distinct. Malformed local or remote observation data is classified fail-closed as `malformed`.
- Local/remote observation errors or differences increment only the bounded metric `farm_friendship_shadow_comparison_total{method,result}` where result is `equal|mismatch|malformed|unavailable`; they never replace the formal response.
- Pair/Block/relation epoch remains canonical and FriendIndex remains revalidated projection data. VisitPermit and ActionAdmission still read canonical Pair at write-time authority boundaries.
- The frozen social profile remains `friend-limit-v1=300` and `share-token-ttl-v1=7d`; explicit inconsistent overrides fail social-profile validation in player-svr.

## Owned files

- `api/proto/friendshiprpc/friendship_service.proto`
- `api/gen/friendshiprpc/friendship_service.pb.go`
- `api/gen/friendshiprpc/friendship_service_grpc.pb.go`
- `internal/friendshipservice/*.go`
- `cmd/player-svr/friendship.go`
- `cmd/player-svr/main.go`
- `internal/gatewayapp/friendship.go`
- `internal/gatewayapp/friendship_test.go`
- `internal/gatewayapp/app.go`
- `internal/gateway/auth/share.go`
- `internal/gateway/auth/friendship_boundary_test.go`
- `scripts/run-authority-vertical-smoke`
- `scripts/run-cp-vertical-b-smoke`
- `scripts/run-authority-vertical-smoke-test`
- this handoff

## Deterministic evidence

PASS:

```text
# Proto inputs were unchanged by the audit fixes; regeneration was not needed.
# The original B2-1 generation remained clean:
./scripts/gen-proto.sh

go test -buildvcs=false \
  ./internal/friendshipservice ./internal/gateway/auth ./internal/gatewayapp -count=1

bash -n scripts/run-authority-vertical-smoke scripts/run-cp-vertical-b-smoke \
  scripts/run-authority-vertical-smoke-test
scripts/run-authority-vertical-smoke-test

go test -race -buildvcs=false \
  ./internal/friendshipservice ./internal/gateway/auth ./internal/gatewayapp ./internal/friend -count=1

go test -buildvcs=false ./... -count=1

go vet ./...
go build -buildvcs=false ./...
git diff --check
```

The Controller had already reported repository-wide and Friend race PASS in a socket-capable environment before this audit-fix pass. The writable retry reproduced both after the fixes. The tests cover mode parsing and fail-closed selection; the `remote_player` zero-local-factory invariant; all internal method mappings; canonical status/error mapping; rejected Redeem without Pair; required Pair on successful/no-change Redeem; connection loss, timeout and malformed response handling; rejected Pair-less Redeem equality from matching local/remote canonical Pair observations; replay/no-change shadow equality with one formal write; local/remote observation failure classification; canonical mutation mismatch metrics; complete ActionAdmission receipt observation; two stateless service instances sharing authority state; Unblock durable enqueue/reconciliation and enqueue-failure fencing; VisitPermit/ActionAdmission; bounded JSON; explicit self-invite mapping; and public Block/Unblock absence.

No Kubernetes, staging, Hero, F1/F2/F3, or live-Redis PASS is claimed.

For the later Controller run on a clean composed Merge Queue descendant of F1-A `29e53ff2516f0e9f5a81fd4c6a70b4b89b2865a0` with H5 `e3ce394cf77bbe111755fcd5dc576dfd4a557d34`, invoke the real-Redis CP-B harness from the composed backend worktree. The generated run manifest, rather than this example, binds the exact descendant commit:

```bash
cd /data/home/developer_026/CodeBuddy/20260519141303/farm-game-r2-merge-queue-20260809
git merge-base --is-ancestor 29e53ff2516f0e9f5a81fd4c6a70b4b89b2865a0 HEAD
test -z "$(git status --porcelain)"
test "$(git -C ../classic-farm-h5-integration-q1-merge-20260809 rev-parse HEAD)" = e3ce394cf77bbe111755fcd5dc576dfd4a557d34
SMOKE_FRIENDSHIP_SERVICE_MODE=remote_player \
H5_ROOT=/data/home/developer_026/CodeBuddy/20260519141303/classic-farm-h5-integration-q1-merge-20260809 \
SMOKE_REQUIRE_SOCIAL_ORACLE=1 \
SMOKE_E2E_COMMAND_RESULT_FAULTS=1 \
CPB_SMOKE_RUN_ID="r2f1arp$(date +%s)" \
CPB_RUN_ID="r2f1a-remote-player-$(date -u +%Y%m%dT%H%M%SZ)" \
./scripts/run-cp-vertical-b-smoke
```

This local real-Redis smoke invocation is verification evidence only; it is not formal Kubernetes Staging and must not be promoted as a Staging PASS.

## B2-2 remaining work

Continue in this same Friendship Writer thread only after B2-1 review/stability:

1. Add `FRIEND_LEGACY_MODE=shadow_read` and the four frozen legacy/projection metrics.
2. Stop legacy roster writes. `RedeemShare` and `DeleteFriend` still update the R1 roster projection in B2-1, and `changed` remains coupled to that receipt for rollback compatibility.
3. Make canonical Pair/Block the sole formal Create/Delete/Block/Unblock result source, including success despite delayed projection repair.
4. Compare legacy reads against Pair plus FriendIndex, repair until every difference is zero or classified, and prove legacy Redis outage cannot affect formal business.
5. Only then remove the legacy Store, key configuration, compatibility code and tests. Do not delete existing legacy data before this gate.
6. Run Social Hero/F1/F2/F3, projection-failure repair, legacy-outage and live/staging cutover evidence before claiming B2-2 completion.
