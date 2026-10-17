# CAP-STATIC-ROUTE-INIT-1024-R2 — Agent B Handoff

Date: 2026-08-11

Scope: local Redis Profile B static placement/generation initialization only

Capacity claim: `NONE`

## 1. Current verdict

```text
CODE_AND_COMPONENT_TESTS=PASS
LOCAL_PROFILE_B_1024_PLUS_1024_INITIALIZATION=PASS
LOCAL_PROFILE_B_IDEMPOTENT_RERUN=PASS
CURRENT_PRODUCT_ROUTE_LIVENESS=BLOCKED
ACTIVE_BINDING_AUTHORIZED=false
FORMAL_L2_AUTHORIZED=false
supports_30m_dau=false
company_redis_contacted=false
```

The retained runtime receipts are exact historical initialization evidence. Their
60-second leases have intentionally expired because Agent A's sole dynamic
RouteAssignmentManager publisher was not deployed. They are **not** a currently
activatable product route and must not be applied to the cluster.

## 2. Implementation

Owned additions:

```text
cmd/cap-static-route-init/**
internal/testkit/capstaticroute/**
```

The initializer:

1. defaults to a zero-network dry run;
2. accepts Redis only after explicit `--apply-local-profile-b`;
3. rejects credential-bearing, remote, non-DB-0 and non-allowlisted Redis URLs;
4. creates/reuses a protected Run/Generation/ControllerNonce identity file;
5. constructs two PLAYER and two FARM generation-aware HolderBootIDs;
6. constructs 1024 PLAYER and 1024 FARM placements;
7. uses the existing Redis authority Store and RouteAssignmentManager for every claim and renew;
8. derives each Transition ID from run, profile, generation, domain, shard, holder and holder boot identity;
9. fails closed on Partial, unavailable-not-sent, timeout-unknown, corrupt record, identity drift and expired existing leases;
10. renews every exact shard after all claims so early shards regain the startup budget;
11. reads every shard through a bounded, exact-field, fixed-key Lua reader without `SCAN`;
12. checks layout, hash, domain, shard, holder, boot, revision, writer epoch, lease epoch, deadline, Transition ID and Transition Digest;
13. emits a sorted exact 2048-record ledger with an individual SHA-256 for every record;
14. content-addresses and archives any previous output bundle before a rerun;
15. normalizes the first externally durable RouteSnapshot to `GlobalRevision=1` while retaining the Manager working revision in the receipt;
16. writes ledger and snapshot only after complete verification, with the receipt renamed last as commit marker and output directories synced;
17. self-validates strict JSON and all snapshot/ledger/receipt hashes before publishing files.

No fixed remote deadline, 2100 deadline, `SCAN`, raw business-state write,
`FLUSHALL`, `FLUSHDB`, or `SCRIPT FLUSH` exists in this implementation.

## 3. Tests

Passed on the Agent B worktree:

```text
go test -count=1 -timeout=600s \
  ./internal/testkit/capstaticroute ./cmd/cap-static-route-init

go test -race -count=1 -timeout=900s \
  ./internal/testkit/capstaticroute ./cmd/cap-static-route-init

go test -buildvcs=false -count=1 -timeout=1800s ./...
```

Covered gates:

```text
4/4 real Store/Manager integration
1024/1024 completeness
2048 exact Redis records and exact ledger rows
initialization duration/startup budget
same identity idempotent rerun
partial failure -> no final publish
timeout unknown -> no final publish
holder mismatch
boot mismatch
layout mismatch
hash mismatch
expired initial lease
strict bundle validator and stale freshness rejection
ordinary namespace ownership
same Bucket -> same Redis Cluster slot
different Buckets -> distributed slots
default dry run -> company Redis zero access
remote/credential-bearing Redis rejection
```

The 1024/1024 miniredis integration completes in approximately 5.7 seconds on
this host. This is a component timing observation, not a capacity result.

## 4. Real local Profile B evidence

The G2 files below predate the post-review `GlobalRevision=1` durable-bootstrap
normalization. They remain retained diagnostic evidence for 2048 exact claims,
reads and same-identity renewal, but are not final-source qualification evidence
and are not admissible as the A+B durable bootstrap input. The integrated run
must produce a fresh generation from the reviewed source.

Run identity:

```text
run_id=capr2-static-b-g2-20260811
profile_id=functional
generation=2
namespace_environment=l2r2bstaticg2
namespace_zone=z1
shards=PLAYER 1024 + FARM 1024
lease_ttl=60s
minimum_startup_budget=30s
```

Only the local k3s service `farm-local-redis` was contacted through a temporary
loopback port-forward. No company Redis configuration was read and no company
endpoint was contacted.

### 4.1 First application

```text
elapsed_ms=9292
claim_applied=2048
renew_applied=2048
verified=2048
exact_ledger_records=2048
```

Retained files:

```text
/data/farm-l2-r2/runtime/static-init/capr2-static-b-g2-20260811/attempt1-route-snapshot.json
  SHA256=29e04c421bb988a9775df6b4a3aa3c809c1b087f17ee8026b4513900fb4b891c

/data/farm-l2-r2/runtime/static-init/capr2-static-b-g2-20260811/attempt1-exact-ledger.json
  SHA256=527f4a07b546136f4f9cf59e932b48c9531acf78e14bdc790b6ffd5a03163648

/data/farm-l2-r2/receipts/static-init/capr2-static-b-g2-20260811/attempt1-static-init-receipt.json
  SHA256=ff4cf8583b7fdc617fdd6317b33e7dc84583d2f2aeff539be7dfc123722637ea
```

### 4.2 Exact-identity rerun

The same protected identity was reused before lease expiry:

```text
elapsed_ms=5978
existing_exact=2048
claim_applied=0
renew_applied=2048
verified=2048
remaining_lease_after_validation_ms=55214
```

Final retained files:

```text
/data/farm-l2-r2/runtime/static-init/capr2-static-b-g2-20260811/route-snapshot.json
  SHA256=05a060d86bb05a6ce41ffa2f54f967094353fdf5818fff3e944a90c91dcc97cf

/data/farm-l2-r2/runtime/static-init/capr2-static-b-g2-20260811/exact-ledger.json
  SHA256=4cd1c8a3c1d822286afe52d53260e81da14b96d0683b6194f85228726f6083fa

/data/farm-l2-r2/receipts/static-init/capr2-static-b-g2-20260811/static-init-receipt.json
  SHA256=b74dc50f89f2fd941b8b2466f63c1b6e368051e84163fbc1613bce31940c32d9
```

All protected runtime input and exact data files are mode `0600`; the
non-secret receipt is mode `0640`. Raw ControllerNonce and local Redis URL are
not copied into Git or receipt evidence.

## 5. Governance meaning

The successful receipt says:

```text
status=INITIALIZED_LOCAL_PROFILE_B_NOT_QUALIFIED
binding_scope=LOCAL_FUNCTIONAL_ONLY
activatable=true                 # only at receipt-time freshness
active_binding_authorized=false
capacity_constant_eligible=false
formal_l2_authorized=false
supports_30m_dau=false
company_redis_contacted=false
```

`activatable=true` is subordinate to the receipt deadline and local binding
scope. The validator's `--require-fresh` mode now rejects the retained bundle,
while historical exact-evidence validation remains possible. This distinction
prevents expired static evidence from masquerading as live routing authority.

## 6. Integration sequence with Agent A

A fresh namespace/generation must be used for the integration run. Do not reuse
the now-expired G2 snapshot as current authority.

Required sequence:

```text
1. merge/review Agent B
2. merge/review Agent A
3. create one fresh identity and local Profile B namespace
4. run cap-static-route-init and require a fresh complete bundle
5. immediately start the sole Agent A RouteAssignmentManager publisher
6. start Player/Farm holders with the exact HolderBootIDs
7. prove every consumer receives fresh projection continuously for >=3×TTL
8. only then create LOCAL_FUNCTIONAL_ONLY provisional runtime material
9. deploy Gate/Worker/H5 after holder admission
```

If step 5 cannot begin within the minimum startup budget, abandon that
namespace/generation and preserve the failed evidence. Never extend the static
deadline, edit the snapshot, or restart Pods to hide expiration.

## 7. Remaining work

Agent B does not claim these closed:

```text
Agent A independent semantic review
RT2Q multi-Pod runtime evidence
>=3×TTL fresh projection
A+B activated runtime materializer integration
Local Provisional Binding
product workload deployment
Driver/observability qualification
L2 staircase, mixed, fault, soak or 30M model
```

The existing 2048 G2 authority hashes remain in the ephemeral local Profile B
namespace for evidence retention. No broad cleanup command was run.
