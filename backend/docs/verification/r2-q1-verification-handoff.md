# R2-Q1 verification handoff

This worktree adds the fail-closed R2-Q entry and deterministic fixture contracts. It does not implement Friendship, Model Field V2, Worker, Kubernetes, Player/Gate behavior, Proto, Redis Lua, or Actor storage. The assigned base is frozen at:

```text
54c6d0a15cd2e0555d309b59319f1ce30fab268c
```

The R2 H5 entry base is separately frozen at:

```text
e3ce394cf77bbe111755fcd5dc576dfd4a557d34
```

Run the entry from a clean merged candidate:

```bash
scripts/r2-q-verification \
  --merge-candidate-commit "<backend-merge-candidate>" \
  --h5-candidate-commit "<h5-candidate>" \
  --official-url "https://<official-staging-host>" \
  --evidence "<one-exact-bound-evidence-json-per-composed-gate>"
```

The output directory contains `run-manifest.json`, `verdict.json`, `metrics-summary.json`, `fixture-summary.json`, `evidence-index.json`, `checksums.sha256`, and a readable `r2-q-evidence.md`. A non-empty output directory is rejected. Invalid candidate/evidence runs are preserved with explicit reasons. Exit `1` means failed/invalid assembly or a failed selected command; exit `2` means unresolved composition/environment; exit `0` is reserved for every gate passing.

## Binding rules

`run-manifest.json` records exact Backend, H5, deploy, and load-tool commits, clean paths captured before output creation, the assigned R2 base, the separately frozen R2 H5 entry base, merge candidate, static route snapshot hash, profile/config/image hashes, Actor layout and Friendship modes, and the planned and actual test lists. Deploy and load-tool commits must equal the Backend candidate. H5 must be a clean descendant of `e3ce394cf77bbe111755fcd5dc576dfd4a557d34`; the old R1 base and previous R2 H5 base are rejected as invalid assembly.

The merged-candidate route default is `deploy/k8s/static-staging/base/route-snapshot.v1.json`. Isolated Q1 tests may pass an explicit temporary route/config/profile input; they must not change the merged-candidate default. P1 structural tooling is bound as `scripts/validate-k8s-static-staging` and `scripts/test-k8s-static-staging`. Tool/file presence is identity only: it never promotes a rendered result to Kubernetes staging or official-URL PASS.

The default route snapshot and profiles are hashed, not treated as runtime proof. Add image, cluster, Redis, rendered-manifest, and config identity through their command-line inputs or exact-bound evidence. Redis URLs are redacted and are never copied into the manifest. `FLUSHALL` is forbidden. Selected tools receive a unique namespace and must use dynamic ports.

The evidence record consumed by a gate has this minimum shape:

```json
{
  "schema": "farm.r2-q.gate-evidence/v1",
  "run_id": "<coordinator-run-id>",
  "gate_id": "<exact-gate-id>",
  "status": "PASS",
  "environment": "local|rendered_manifest|kubernetes_staging|official_url",
  "binding": {
    "r2_expected_base_commit": "54c6d0a15cd2e0555d309b59319f1ce30fab268c",
    "h5_expected_base_commit": "e3ce394cf77bbe111755fcd5dc576dfd4a557d34",
    "merge_candidate_commit": "<exact>",
    "backend_commit": "<exact>",
    "h5_commit": "<exact>",
    "deploy_commit": "<exact>",
    "load_tool_commit": "<exact>",
    "route_snapshot_sha256": "<exact>",
    "profile_hashes": ["<exact>"],
    "config_hashes": ["<exact>"],
    "image_manifest_sha256": "<exact-or-null>"
  },
  "tests": [{"name": "<planned-test>", "actual": true, "result": "PASS"}],
  "assertions": {"<required-assertion>": true},
  "flushall_used": false,
  "credentials_recorded": false,
  "reason": "<what was actually observed>"
}
```

The coordinator checks every required assertion named by the gate and rejects missing or mismatched bindings. `PASS` in a file is not enough. Local and rendered observations remain local/rendered; they cannot satisfy actual Kubernetes or official URL gates. A localhost URL is always non-official.

## Gate supply map

| Area | Future candidate supplies | Required evidence boundary |
|---|---|---|
| Friendship boundary | F1 composition tests for `local_gate`, `remote_shadow`, and `remote_player`; write-count trace; comparison counters; static scan proving no Gate Friend repository/Lua/repair worker; VisitPermit and ActionAdmission traces | Shadow formal result stays on the old path and has zero shadow business writes. Remote-player evidence proves any healthy player-svr can serve the domain and that public Block/Unblock is absent as required by the fixture. |
| Canonical-only Friend | F2 pair/projection fault matrix, stale-index reads, legacy outage run, shadow diff counters, Redis command/write audit | Pair/Block is the only authority; projection failure may delay list visibility but not canonical success; delete hides/rejects stale index; legacy outage does not fail business; legacy writes/results are zero. |
| Model Field V2 | M1 golden/mutation/fault runner and exact Redis accounting | Supply v1/v2 field equality, one WriterClaim/RecordVersion, no dual write, committed/not-committed/timeout-unknown resolution, old-holder fencing, crash after every phase, idempotent retry, dirty-model matrix, and same-config operation/byte measurements. Do not bypass Actor fences. |
| Worker readiness | W1 family-state endpoint/fixture, fault injection records, kill/restart result and DueIndex audit | Report each family exactly once as `DISABLED`, `READY`, `DEGRADED`, or `FAILED`; include aggregate policy, repository/route/participant/scan/scheduler faults, disabled families, Redis delay with stable liveness, worker kill recovery, invalid order count, and residual count. |
| Static Kubernetes | P1 rendered output plus read-only cluster observations | Render structural validation, deployment, product Pod Ready, Gate RollingUpdate, Worker kill, and same-Holder restart are separate records. Cluster evidence must include actual cluster identity and namespace `farm-staging-system`. Rendered YAML and localhost are not staging. |
| Product regressions | Backend full/race/vet/build, H5 test/TypeScript/build/audit, Hero, Task 3/3, F1/F2/F3, 200-WSS baseline/slow reader, social limits/SCB, retained R1 lease evidence | Each test result is bound to the same candidate/config/image/profile hashes. Hero is repeat evidence, not one browser success. Baseline and slow reader are separate 200-WSS cases. Official H5 auth is separate from every local run. |

The R1 lease formal/real artifacts should be referenced by path and checksum, not copied or reclassified. Existing Hero and 200-WSS tools remain the execution owners; Q1 records their exact tool commit and result rather than reimplementing them.

## Fixture contracts

`docs/verification/r2-q-fixtures/` contains deterministic contracts for friendship boundary/canonical-only behavior, Model Field V2 migration, Worker readiness, static Kubernetes, and product regressions. The Model Field V2 contract explicitly covers the exact Player/Farm core physical model sets, envelope prefixes, golden equality, one WriterClaim/RecordVersion/PersistenceLane, no dual write, atomic old-blob deletion, migration outcomes, old-holder fences, every migration crash phase, retry idempotency, dirty-model commands, and Redis operation/byte comparison. It contains no production measurement digest; M1 must supply observed measurements. All contracts initially declare `SKIPPED_NOT_COMPOSED`; this is intentional and cannot become PASS without candidate-bound evidence.

## Wave4 CP-B harness correction

The immutable diagnostic bundle is outside this worktree at `.agentctl-cp-route-state/evidence/r2-wave4-v2-fault10-20260810T065047Z`, bundle SHA256 `833f9a293511b51c4f83aabbf7f44ec89345df5f08797ada2d10832bed1684f0`. It binds Backend `11f7b218d1066f52ea56b0133aa81bb56c066ec5` and H5 `e3ce394cf77bbe111755fcd5dc576dfd4a557d34`; it is diagnostic local evidence, not formal Staging.

- Run 06 is `INCONCLUSIVE_ENVIRONMENT`. Its controller log assigned both `GATE_CONTROL` and `DOWNLINK_PROXY` port `56471`; the proxy bound it and `gate.log` then reported `listen GateControl 127.0.0.1:56471: listen tcp 127.0.0.1:56471: bind: address already in use`. Farm Gate never became ready. This is neither a product failure nor a PASS.
- Run 09 is `INCONCLUSIVE_TEST_HARNESS_STATIC_ROUTE_EXPIRED`. `fixture.json` and both static Player/Farm routes record creation/claim time `1786345184368` and immutable deadline `1786345239368`, exactly 55 seconds later (`2026-08-10T07:00:39.368Z`). The restarted Gate recorded the lost `CLAIM_TASK` result at `07:00:30Z` and the recovery connection at `07:00:40Z`, after that deadline. The browser had already recorded successful F1 CARE response-loss recovery, simulated reconnect, F2 Gate restart, and F3 theft/farm-change recovery, then remained at `pending_ops=1` with `SUBSCRIPTION_LEASE_LOST` / Fresh Join. Independently, `authority-after.json` retained the same holder identities and writer/lease epochs while Redis ShardAuthority had renewed through `1786345339454`/`1786345339497` (about `07:02:19Z`). This isolates the immutable static snapshot from the healthy renewed authority. The resolver contract also checks the loaded route on every resolution and rejects it when current time is greater than or equal to its static deadline.
- The accidental unsupported Controller `--help` invocation is classified `INCONCLUSIVE_CONTROLLER_INVOCATION`; it is not a baseline/slow-reader result and does not count toward 10/10 or PASS.

The test-only correction in `scripts/run-authority-vertical-smoke` now obtains all ten endpoint ports in one finite allocation while retaining every reservation until the complete set is selected and validated. The same helper controls now cover `scripts/run-theft-200-wss-hot-farm`: it obtains nine service/Caddy ports in one bounded allocation and refreshes each authority route immediately after `theft-hot-audit -mode authority`, before Player/Farm/Worker/Gate/Caddy startup. The default is 3600 seconds; an explicit value must be a non-empty decimal in the bounded 900–3600 second range. Each theft case writes a non-secret `*-route-horizon.json` witness beside its result JSON. Redis authority TTL/renewal, holder identity, writer/lease epoch, route revision, 200 users, 45s duration, baseline/slow-reader semantics, thresholds, TLS, audit, exact H5 root+port cleanup, and Playwright assertion timeouts are unchanged.

Neither invalid run counts toward the required 10/10. The serialized Wave4 CP-B batch must be rerun from zero on one exact clean candidate; the prior eight successes cannot be combined with replacement runs.

## Handoff invariants

- Keep at most one unresolved operation where the contract requires it; resolve `TIMEOUT_UNKNOWN` with the existing Actor Fence/WriterClaim/RecordVersion mechanism.
- Preserve failed and malformed evidence in the run output; do not replace it with a clean-looking summary.
- Do not put Redis passwords, cookies, session tokens, or raw authorization headers in evidence.
- Use dynamic ports and unique namespaces for local/test tools; never use `FLUSHALL`.
- Do not claim R2 completion from this handoff or from fixture file presence. The final verdict must be generated only after all required gates are composed and pass in their required environment.
