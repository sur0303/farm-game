# cap-static-route-init

Local Profile B-only bootstrap for the R2 static placement/generation route.
It does not authorize production, company Redis, L2 load, or a 30M DAU claim.

## Safety defaults

With no apply flag, the command emits a dry-run intent and performs no Redis
I/O. Redis is contacted only with `--apply-local-profile-b`; that mode accepts
only loopback or the exact local k3s Profile B service name. Credential-bearing,
remote, non-DB-0, and improperly protected URL files are rejected before any
connection.

The command never uses `SCAN`, `FLUSHALL`, `FLUSHDB`, `SCRIPT FLUSH`, or raw
business-state writes. It uses the product shard authority Store,
RouteAssignmentManager, and durable RouteSnapshot Store.

## Dry run

```bash
go run ./cmd/cap-static-route-init --shard-count 1024
```

Run ID, namespace, generation-aware ControllerNonce and four HolderBootIDs are
generated. Supply `--identity-file` in apply mode so reruns reuse the exact
identity.

## Local Profile B apply

```bash
go run ./cmd/cap-static-route-init \
  --apply-local-profile-b \
  --run-id capr2-static-example \
  --profile-id functional \
  --generation 1 \
  --shard-count 1024 \
  --namespace-environment l2r2staticexample \
  --redis-url-file /protected/local-profile-b.redis-url \
  --identity-file /protected/static-init-identity.json \
  --route-out /evidence/route-snapshot.json \
  --ledger-out /evidence/exact-ledger.json \
  --receipt-out /evidence/static-init-receipt.json
```

Route and ledger files are written only after all 2048 authority records are
claimed/identified, renewed and read back exactly. Per-shard Manager bootstrap
updates use an internal working revision recorded in the receipt; the emitted
first durable RouteSnapshot is normalized to `global_revision=1`. The same
local Profile B transaction train then bootstraps the Route Controller durable
snapshot envelope and reads it back exactly before the receipt commit. An exact
rerun advances only the durable GlobalRevision and refreshed deadlines. Before a rerun replaces any
output, the previous artifacts are copied exactly into the receipt directory's
content-addressed `attempt-history/` tree. The receipt is renamed last and is
the commit marker. A complete applied receipt also binds the durable snapshot
revision, canonical checksum, and topology fingerprint. Partial, corrupt, identity-drifted, expired, and
unknown-commit outcomes have `complete=false`,
`bootstrap_materializable=false`, `activatable=false`, and no new route/ledger
output.

A successful static bundle records `bootstrap_materializable=true`: it retained
the configured local startup lease budget and may be consumed only by the
fail-closed local bootstrap tool. `activatable` always remains false because a
static-init receipt is never a product or capacity activation authority. Every
success also records:

```text
bootstrap_materializable=true
activatable=false
binding_scope=LOCAL_FUNCTIONAL_ONLY
active_binding_authorized=false
capacity_constant_eligible=false
supports_30m_dau=false
company_redis_contacted=false
```

A static lease is deliberately short. The bundle must be consumed by the sole
Manager-owned fresh projection before its startup budget expires; file presence
is never route-liveness evidence.

## Offline evidence and freshness validation

```bash
go run ./cmd/cap-static-route-init \
  --validate-bundle \
  --route-out /evidence/route-snapshot.json \
  --ledger-out /evidence/exact-ledger.json \
  --receipt-out /evidence/static-init-receipt.json
```

Add `--require-fresh` for activation admission. Without it, validation checks
strict schema, exact route/ledger/receipt hashes, all per-shard identities and
fences, but treats expired deadlines as historical evidence rather than live
routing authority.
