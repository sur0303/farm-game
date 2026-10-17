# CP-LOCAL-ROUTE-LIVENESS-R2-G22 Handoff — 2026-08-11

## 1. Verdict

G22 closes the G21 Controller Recreate Availability P1 for the local static Route profile.

```text
status = G22_LOCAL_ROUTE_LIVENESS_AND_RESTART_PASS
runtime_code_tip = 2bd112d5f1201ffd4514039694d83742f02ef42c
business_identity = 5ce43715363eea4f18e0925b6992429785053261
run_id = capr2-ab-g22r12-final-20260811
profile = route-scale-1024x1024
```

This is still `LOCAL_FUNCTIONAL_ONLY`. It does not claim full product route liveness, capacity qualification, production HA, NetworkPolicy enforcement, or Integration update.

Final receipt:

```text
/data/farm-l2-r2/receipts/cp-local-route-liveness-r2-g22-final-capr2-ab-g22r12-final-20260811.json
SHA256=f3f87249962114bad7162ef460af3459d4cf01434595d1c0a98ee6edad1f64d3
```

## 2. Closed root causes

The G21 failure had two interacting lifecycle defects:

1. the Remote Pump exhausted the old three-attempt retry envelope before a normal Controller Recreate completed;
2. transient assignment unavailability was translated into irreversible durable Holder `DRAINING`.

G22 fixes these without adding another assignment state machine:

- bounded reconnect defaults: 32 resync attempts and 32 Watch failures;
- exponential backoff: 100ms, capped at 1s, minimum pure-backoff horizon 28.5s;
- controller-Boot changes remain exclusively owned by the existing restart-aware rollover path;
- transient assignment loss fails the local data plane closed but does not request durable Holder drain;
- a `SERVING`, non-draining, temporarily unready Holder heartbeat is deferred without consuming the sticky Agent failure budget;
- the durable 30s Holder lease remains the hard upper safety bound; an expired record is still rejected;
- runtime fatal, explicit shutdown, and planned drain behavior remain draining.

An experimental Watch-silence ordinary-fetch path was removed before final freeze because an ordinary fetch observing a new Controller Boot could latch `BootDrift` terminally and bypass restart-aware rollover ownership.

## 3. Exact R12 runtime evidence

### Static bootstrap and steady liveness

```text
PLAYER shards = 1024
FARM shards = 1024
verified authority records = 2048
first durable Snapshot revision = 1
3xTTL elapsed = 180205ms
successful samples = 91/91
minimum observed remaining lease = 33638ms
```

Candidate:

```text
/data/farm-l2-r2/receipts/cp-local-route-runtime-r2-candidate-capr2-ab-g22r12-final-20260811.json
SHA256=7438ca4319c158382f7fe6a65ff6261726f1917163bf7ca76776b29813e8ea0d
```

Runtime image:

```text
source_head = 2bd112d5f1201ffd4514039694d83742f02ef42c
imageID = sha256:5da8b71646b3d30425c1cba7b388b02cac53bf157b52d822c56d8b3457a7f61c
registry digest = sha256:32154e18c60c61811f75066e21bb43102bee55659930fcf2dae98630a49da3bd
```

### Controller Recreate

```text
old Controller UID != new Controller UID
Controller count always <= 1
Controller outage = 3663ms
all four unchanged Holders Ready = 7813ms
pre-delete minimum remaining lease = 37707ms
post-recovery minimum observed lease = 35579ms
revision = 52 -> 72
observed shards = 2048
post-recovery observation = 91589ms
post-recovery samples = 33/33 Ready
Holder Pod UID / image / restart count unchanged
HolderAgent state = RUNNING
HolderAgent consecutive failures = 0
membership generation unchanged
membership churn = false
publisher overlap = false
```

Recovery receipt:

```text
/data/farm-l2-r2/receipts/route-controller-recovery-capr2-ab-g22r12-final-20260811.json
SHA256=d7beec76590f8d483366f094fb1aee3872037fc7dd10847c22705776db22adb0
```

## 4. Review and gates

Exact detached review:

```text
/tmp/farm-game-g22-independent-review-2bd112d
/tmp/g22-independent-review-2bd112d.md
P0=0 P1=0 P2=0
```

PASS on the exact runtime tip:

```text
go test ./... -count=1
focused go test -race
go vet ./...
exact VCS-stamped go build ./...
./scripts/local-k8s/test-tools
git diff --check
isolated Proto regeneration and byte comparison
Proto ownership gate
production-diff secret gate
changed-large-object gate
```

## 5. Local Redis infrastructure incident

Node DiskPressure evicted the previous `emptyDir` local Redis Pod. Historical file evidence remains, but old live Redis state continuity is explicitly not claimed.

```text
/data/farm-l2-r2/receipts/local-redis-eviction-state-loss-addendum-20260811.json
SHA256=bcf48c2d5540a6bd25293d304a06c92826e116bc91bf8d4e21b3d730f2d2b925
```

Only G22 Go build caches were removed to clear DiskPressure. No Registry GC, Docker volume prune, evidence deletion, Redis flush, broad key scan, or company Redis access occurred. R12 used a fresh Namespace object, generation identity, TLS material, Holder boot identities, and Redis namespace after the new local Redis Pod became Ready.

## 6. Scope exclusions

```text
network_policy_enforcement = NOT_CLAIMED
full_product_route_liveness_proven = false
Gate route consumer = NOT_RUN
Worker route consumer = NOT_RUN
capacity qualification = NOT_RUN
formal L2 = NOT_AUTHORIZED
company Redis contacted = false
Integration updated = false
Autoscaling authorized = false
```

## 7. Next step

Freeze G22, then create a new convergence branch with G22 as the first parent/input and merge only RT2Q tip:

```text
165dae94bc85b3e3f04c85ef45e4c8c56814cb89
```

Do not merge or continue old Premerge `756cbac74e4181af021e8406b952912ed5de3dc4`. The new convergence must complete both-modified-file review and dynamic-local G1/G2/G3 before any Integration update.
