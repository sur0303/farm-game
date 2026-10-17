# CAP-SINGLE-POD-L2-R2 Design Review Result

## What this document is

This is the authoring review of the R2 design set: it was written by the same hand that wrote the files, so it is not the independent review the checklist asks for. It records the exact binding, the R2 facts that differ from R1, and the findings that are still open. `independent_design_review` stays `NOT_RUN` in `status.json` until a second reviewer answers `review-checklist.md` against this same Design Head.

## Exact binding

- Design base: `6f239b136e0943ec0dc30e3c92644974fc2b3454`
- Profile: `CAP-SINGLE-POD-L2-R2-v0.1`
- Active binding: `deploy/load/cap-single-pod-r2-binding.json`, SHA-256 `f4d6910e509cecbba76de45d5b928b0b731f57cfecf07c8d8ad8ec2602a77db3`
- Capacity identity: `dyn-friend-r72-capacity-20260813`
- Acceptance identity that carries the product verdict: `dyn-friend-r70-formal-20260813`

## R2 facts that are not R1 facts

### 1. The identity is a capacity identity, and it forfeited product acceptance

`profile.json` sets `product_acceptance_eligible=false` and names `dyn-friend-r70-formal-20260813` as the run that carries the acceptance verdict. The reason is the login posture: `/data/farm-l2-r2/receipts/capacity-identity-binding-dyn-friend-r72-capacity-20260813.json` records the deployment delta as `FARM_APP_PROFILE: showcase -> test`, `LOAD_TEST_TOKEN_SECRET: absent -> injected into farm-gate`, and an opportunity mode ladder that is born on the final rung instead of walking three rungs to reach it. A run that signs its own driver in with a load-test token is not exercising the path a player takes, so no number taken under this binding may be read as a product acceptance verdict. The receipt says the same thing in its `capacity_result_standing`, and `binding.go` repeats it at the trust root so that a reader of the harness sees it too.

This is a narrower statement than "the numbers are invalid". Capacity numbers taken here may be registered against the G1/G2/G3 verdict, because of finding 2. They may not be read as the verdict itself.

### 2. The binaries under load are the binaries that carried G1/G2/G3

The same receipt records that the two images were read for the SHA-256 of every file in `/usr/local/bin` and found identical, with a digest listed for each of the seven binaries: `farm-gate`, `farm-runtime-entrypoint`, `farm-svr`, `farm-worker`, `player-svr`, `route-authority-fence-probe`, `route-controller`. The digests are recorded as the leading sixteen hex characters of each file digest, which is a truncation the reviewer should be aware of when re-deriving them.

The two image digests still differ: acceptance `sha256:93b305db40fbb2592c7ec9fbf77c7de0fb1a3b805cf073cd4c417a23afc8a0ed`, capacity `sha256:06bd1f8a3a9ab31a84e8c0480ddee959116aeca8db21e19cc42d4181a471f2c9`. The receipt bounds the difference to the build context: between the two heads only `scripts/local-k8s` changed — `apply-dynamic-route-profile`, `materialize-dynamic-route-profile.py`, `verify-dynamic-route-state` and their test — and no Go source changed. The verdicts the acceptance identity carries are `FINAL_PRODUCT_G1_REMOTE_WATCH_PASS`, `G2_THREE_DOMAIN_PLANNED_MIGRATION_PASS` and `G3_PROOF_GATED_CRASH_TAKEOVER_PASS`.

### 3. Four per-source-IP ceilings were raised, and three were deliberately left binding

`/data/farm-l2-r2/receipts/capacity-config-deviation-dyn-friend-r72-capacity-20260813.json` records that `GATE_WS_RATE_PER_SEC` went from the code default 10 to 5000, `GATE_WS_RATE_BURST` from 20 to 20000, `GATE_PREAUTH_MAX_CONNECTIONS_PER_IP` from 64 to 16384, and `GATE_PREAUTH_MAX_BYTES_PER_IP` from 1048576 to 1073741824, all scoped per source IP. The evidence is an observation rather than an expectation: a 500-connection rung dialled at 50/s was admitted 119 times and rejected 381 times while the generator kept up and nothing was busy, and the token bucket arithmetic predicts 20 + 10×10 = 120 admissions in the ten seconds the dial takes.

`GATE_PREAUTH_MAX_CONNECTIONS` (4096), `GATE_PREAUTH_MAX_BYTES_POD` (67108864) and `GATE_WS_MAX_CONNS` (unset, unbounded) were left alone, because a pod-wide ceiling filling up is the saturation the run is looking for and a configured connection ceiling would answer the capacity question by assumption.

### 4. The required metric set was narrowed, and every cut carries a reason

`required-metrics.json` narrows `cap-single-pod-l2-r1.required-metrics.v0.1` to twenty-nine fewer metrics, each listed in `scope.dropped_metrics` with a reason and a classification: either the quantity is exported under a different name that nothing reads, or it is not exported at all. The narrowing rests on the survey in `observability-mapping-draft.json`, generated against `dyn-friend-r66-formal-20260813`, and it restores at `T1_CAPACITY_STAIRCASE`.

The correctness list is eleven gates rather than fifteen. The four that were removed — `h5_pending_journal_residual`, `silent_drop_total`, `invalid_order_total`, `wrong_scope_dispatch_total` — are listed with what each one would need in order to exist, not merely as absent. `TestR2RequiresExactlyTheGatesTheOracleMeasures` holds the harness to that list.

`scope.not_narrowable` states the limit of the exercise: `mandatoryRuntimeMetricNames` in `cmd/cap-single-pod/required.go` is compiled into the harness and is demanded of every sample whatever this file says.

### 5. `support_service_primary_queue_items` now has exactly one producer

R2 had no collector adapter configuration of its own; the only collector document in the repository was the shared, unresolved `deploy/load/cap-single-pod-collector.template.json`. The R2 mapping set now lives in `deploy/load/cap-single-pod-r2-collector-adapter.template.json`, which resolves the shared template's `${METRIC_MAPPINGS}` and `${PROMETHEUS_SOURCES}` and leaves the cgroup and identity-file blocks as placeholders, because those are host facts that only exist once the run has a host. It renders with `scripts/perf/cap-single-pod/render-run-plan.py`, which is generic over any JSON template, and the run plan then pins the rendered file by `collector_config_sha256`.

The mapping is: logical name `support_service_primary_queue_items`, metric `farm_workflow_backlog`, value `value`, aggregate `SINGLE`, source `farm-worker` at `http://127.0.0.1:19082/metrics`. The port is a declaration, not a measurement: the adapter schema admits loopback sources only, so the run forwards the worker's metrics endpoint to loopback, and 19082 continues the block `deploy/load/cap-correctness.json` already uses — 19080 farm-gate, 19081 farm-svr — while staying clear of 19090 driver, 19091 collector, 19092 oracle and 19093 correctness.

`SINGLE` is the right aggregate because the gauge carries no labels: the r66 worker scrape shows one unlabelled `farm_workflow_backlog` series. Two properties of that source are load-bearing for the stop rule that reads it, and both are written next to the mapping in this document rather than only in the receipt:

- **Coverage.** `farm_workflow_backlog` is registered in `cmd/farm-worker/main.go` as the `farm`/`workflow`/`backlog` gauge and written by `runBacklogObservationLoop`. It sums the Plant/Unlock composition's `PendingDue`, the Harvest composition's `PendingDue` only when the Plant composition has no generic loop — the two share one physical generic Work DueIndex, so the loop counts it once — and the Care composition's `PendingDue`. It does not include Theft's Pair DueIndex and it does not include opportunity due. Support-service backlog read through this gauge is therefore a floor, not a total.
- **Failure encoding.** When the observation itself fails, the loop sets the gauge to `-1`. That is a step down, not up. The consumer at `cmd/cap-single-pod/collector.go` decides support-service backlog by asking whether the slope of this series is persistently positive, so an observation failure moves the series in the direction that looks healthy. A failing observer will not raise `INCONCLUSIVE_DEPENDENCY`; it will suppress it, and a recovery from `-1` back to a real depth is a positive step that owes nothing to load.

The rejected alternative was `farm_worker_due_depth{family}`, which is closer to the intent and is already labelled per family. It was rejected for two reasons. It is bounded by `WORKER_DUE_OBSERVE_LIMIT`, default 4096: past the limit the family reports as unavailable, so the metric stops rising exactly when the queue keeps growing, which is the failure mode this stop rule exists to catch. And families that are not enabled export `NaN` — the r66 scrape shows `NaN` for `FRIEND_PROJECTION`, `GUARD_SYNC` and `MAIL` — which would poison a `SUM` or `MAX` across the family label. The collector requires each logical name to have exactly one producer, so the two sources cannot be carried side by side under this name.

No code changed to make this mapping possible. `farm_workflow_backlog` is already registered and already in the `farm-worker` scrape.

## Open findings

- `P0-CONTRACT-IDENTITY-01`: `contract.md` is the R1 contract with its title changed. Its identity section still freezes the R1 evidence head `d1899ecab8d71248062ffd70eb4e42048e394a38`, the R1 business commit, the R1 images and the R1 model layout, which contradicts both `profile.json` and the active binding. Its purpose and gate sections still describe an R1 single-Pod baseline and never state that this identity has forfeited product acceptance. The index pins the file exactly as it stands; it is not repaired here, because rewriting another train's contract inside a documentation task would hide the contradiction rather than settle it.
- `P1-CONTRACT-GATE-01`: `contract.md` lists InvalidOrder and wrong-scope dispatch among the Worker hard gates. `required-metrics.json` excludes both as uninstrumented. Prose demanding a gate the machine-readable file omits is a contradiction the reviewer has to resolve before Smoke.
- `P1-CONTRACT-OBSERVABILITY-01`: the evidence and service sections of `contract.md` assume the R1 required metric set. They do not mention the narrowing, the survey it rests on, or the round at which the dropped metrics return.
- `P1-HARNESS-REVIEW-HEAD-01`: `designReviewHead` compiled into `cmd/cap-single-pod/binding.go` is `25ba9675fbab1a1ea796e39a52fefcc16de351c3`, the R1 design review result. `verifyManifest` requires every run manifest to carry that value, so an R2 run would currently attest to an R1 review. Changing the constant needs the independent R2 review to exist first.
- `P1-COLLECTOR-BACKLOG-01`: the `-1` failure encoding and the missing Theft and opportunity due described above are open properties of the mapping, not defects introduced by it. Closing this finding means either a producer whose failure mode is absence rather than a value, or a stop rule that treats a negative sample as missing.
- `P2-CONFIG-DEVIATION-01`: the four raised ceilings live only in a receipt outside the repository. No checked-in R2 contract file mentions them, so a reader of this directory alone would not know the abuse controls were not binding during the run.

## Independent checks actually performed

```text
exact-file design hash index (8/8)              PASS
verify-contract against this directory          PASS
go test ./cmd/cap-single-pod/                   PASS
go vet ./cmd/cap-single-pod/                    PASS
JSON parse of every file added here             PASS
binding trust root re-derived from disk         PASS
cross-file frozen identity binding vs profile   PASS
cross-file frozen identity contract.md          FAIL (P0-CONTRACT-IDENTITY-01)
correctness gate list vs oracle coverage        PASS
```

`go test ./...` and `go vet ./...` were not run. The working tree carries concurrent edits from other trains in `internal/` and `cmd/cap-correctness/`, so a repository-wide result would not describe this change.

## Result

```text
P0=1
P1=4
P2=1
IDENTITY_BINDING=PASS
PRODUCT_ACCEPTANCE_SEPARATION=PASS
CONFIG_DEVIATION_DISCLOSED=PASS
METRIC_NARROWING_JUSTIFIED=PASS
COLLECTOR_MAPPING=PASS
EXACT_FILE_INDEX=PASS
INDEPENDENT_DESIGN_REVIEW=NOT_RUN
OBSERVABILITY_QUALIFICATION=BLOCKED
LOAD_GENERATOR_QUALIFICATION=BLOCKED
SMOKE=BLOCKED
PRODUCT_ACCEPTANCE_CLAIM=NONE
MIXED_B0=BLOCKED
CAPACITY_CLAIM=NONE
```

Nothing here authorises a pressure phase. It authorises the paperwork gate that `verify-contract` enforces, and it leaves `P0-CONTRACT-IDENTITY-01` in front of everything else.
