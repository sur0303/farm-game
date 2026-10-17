# R2 M1-B Actor Model Field V2 runtime candidate handoff (2026-08-10)

## Status and boundary

This diff wires the default-off PlayerActor/FarmActor runtime cutover
foundation on top of M1-A. It is a Controller-review candidate, not formal
`CP-ACTOR-MODEL-FIELDS-V2`, Kubernetes Staging, or R2 PASS. No checked-in
normal/deploy configuration enables conversion.

The static physical `authority_layout_version=v1` is unchanged. The logical
`ModelLayoutV1|V2` discriminator remains metadata inside that same routed
Actor record and is not a per-model version or CAS.

## Composition audit follow-up

Controller composition at Merge Queue `db4822f17d02504ce243b5430cef34df9634cf8e`
ran with both Player and Farm conversion enabled at 10000 permyriad. The H5
F1/F2/F3 browser flow and protocol bot passed, and the browser recorded final
FarmVersion 5, stolen total 10, Player carrot produce 510, Player revision 14,
and passing Pair/index checks. The final authority/social oracle nevertheless
failed because `cmd/authority-smoke-fixture audit` requested and decoded only
`model:record`. V2 correctly has no retained record, so the old harness
silently unmarshaled absent bytes into zero-value protobufs and reported
FarmVersion/produce/task values as zero and stolen total as -1.

The raw run remains preserved at
`.agentctl-cp-route-state/evidence/r2-m1b-v2-cpb-20260810T054423Z`, SHA-256
`c406c5db9e469d5f2baac3d17ed795b2e9b1c64ef56cc2fd39b437a23872c746`.
Its diagnosis is `INCONCLUSIVE_TEST_HARNESS_V1_ONLY_AUDITOR`; it is neither a
product failure nor a product/CP-B pass.

This follow-up makes the auditor request the v1 record alternative plus the
exact bounded v2 registry. The production Redis model reader interprets
absent layout metadata as v1 and, for v2, validates the complete manifest,
field count, total bytes, digest, hash shape, and every core field before
returning anything. The auditor then switches on `ModelLayoutV1|V2`, uses the
production Player/Farm v2 codecs, validates Farm opportunity header/facts and
their source FarmVersion, and derives the same business values from the
reconstructed aggregate. A seen-v2 row can never consult `model:record`.
Partial, mixed, malformed, or unknown v2 returns an honest INCONCLUSIVE reason
before any zero-value business comparison can pass. Pair, index, navigation,
and Social oracle logic is unchanged.

The gated live Player migration cleanup now unlinks the exact isolated
ShardAuthority, ActorRecord, SourceDueIndex, ReceiptDueIndex and BucketMeta
keys built by `rediskey.AuthorityKeys`; it no longer performs a
namespace-pattern `SCAN`. It still never uses `FLUSHALL`.

## Final-layout theft-hot audit follow-up

The final-layout 200-WSS evidence path found the same test-tool defect in
`cmd/theft-hot-audit`: its `loadActorRecord` directly rebuilt Redis keys and
unconditionally fetched `model:record` for the Farm and every successful
visitor. At 10000-permyriad Player/Farm conversion, the missing blob is the
required v2 physical truth, not a zero record and not permission to fall back.
No final-layout 200-WSS PASS is claimed from this writer worktree.

This follow-up constructs one production Redis Authority model reader in the
common configuration and reuses it throughout the audit. Each bounded read
requests the v1 `record` alternative plus the exact Player or Farm v2 schema.
The production reader owns layout metadata parsing (absent means v1), complete
v2 manifest/count/bytes/digest/hash-shape validation, and exact field-envelope
validation. The theft auditor then decodes v2 through the production
Player/Farm codecs. A seen-v2 row never consults `model:record`.

For v1, only a present complete record is decoded. Player rejects every v2
core field; Farm rejects every v2 core field except the two compact
`opportunity_header`/`opportunity_facts` projections that predate v2 and are
also allowed by production activation. Partial, retained-record mixed,
malformed, unknown, bad identity, or zero/missing RecordVersion fails before
the record reaches theft business assertions. The exact Authority
RecordVersion and reconstructed aggregate feed the existing FarmVersion,
crop/theft, Farm receipt, visitor inventory/receipt, workflow, convergence,
and 200-client checks without changing report fields or thresholds.

## Runtime configuration

Both service compositions parse one strict policy before constructing an
Actor Registry:

```text
ACTOR_MODEL_FIELD_V2_PLAYER_CONVERSION_ENABLED=true|false
ACTOR_MODEL_FIELD_V2_PLAYER_COHORT_PERMYRIAD=0..10000
ACTOR_MODEL_FIELD_V2_FARM_CONVERSION_ENABLED=true|false
ACTOR_MODEL_FIELD_V2_FARM_COHORT_PERMYRIAD=0..10000
```

Complete absence means Player disabled/0 and Farm disabled/0. Explicit empty,
whitespace, alternate booleans, malformed integers, values above 10000, or
`enabled=true` without a non-zero cohort fail startup closed. The cohort hash
is the bounded deterministic M1-A `actor-model-field-v2-cohort-v1` hash over
domain and Actor ID. Policy is consulted only for v1; an already-v2 row stays
read/write v2 after enablement is removed.

## Activation and migration state machine

Player and Farm now consume the complete immutable snapshot returned by the
successful `LoadAndClaim`; activation no longer performs a second timed model
read. Both decode exactly one truth layout before installing business memory:

```text
LoadAndClaim complete snapshot
  v1 + policy ineligible -> decode record (+ explicit v1 projections) -> ACTIVE v1
  v1 + policy eligible   -> MIGRATION_REQUIRED
  v2                     -> exact all-core v2 decode -> ACTIVE v2
  unknown/partial/mixed/malformed -> fail closed; install nothing

MIGRATION_REQUIRED
  normalize existing compatibility state
  construct and retain one exact v2 candidate
  Commit(expected fence, claim, RecordVersion,
         stable AR2_MODEL_MIGRATION receipt,
         intent=MIGRATE_V1_TO_V2,
         all v2 upserts, every obsolete v1 delete)
    COMMITTED/ALREADY_COMMITTED -> install returned v2 snapshot -> ACTIVE
    TIMEOUT_UNKNOWN -> FenceAndClaim with the original attempt evidence
      COMMITTED     -> install resolver snapshot -> ACTIVE
      NOT_COMMITTED -> retry the same candidate/mutation/hash/receipt under
                       the resolver-installed claim
      other         -> fail closed; no in-memory v2 install
```

`model:record` (and Player `model:public_profile`) is deleted in the same
linearization as v2 installation and its migration receipt. The resulting
RecordVersion advances exactly once. A replacement activation after an
unresolved process crash obtains a new WriterClaim, so a delayed old-holder
attempt remains fenced; it observes the committed v2 row or repeats the same
version-bound v1 migration. There is no side migration script, second lane,
blind retry, fallback remarshal, retained blob, or dual write.

Farm remains in the activation controller's loading/claim-unknown state until
the explicit commit or authoritative committed resolution has supplied a
snapshot. Player does not cache a separate Actor object, but its per-player
Binding is likewise not returned from `loadBinding` until migration resolves.

## Normal v2 commits and dirty matrix

The Binding retains the most recent complete layout/state snapshot. A normal
v2 commit receives the complete immutable codec candidate, compares canonical
bytes, and builds a deterministic sparse `ModelMutationIntentUpdate`. Only
changed bytes are upserted; undeclared changes, missing/extra/unknown fields,
bad manifests, totals, or digests remain Store validation failures. An outer
receipt/order-only commit can carry zero model upserts and still increments
the one RecordVersion. V1 callers retain the compatible full-state API.

Representative frozen-matrix carrier tests produce:

| Domain / path | byte-dirty v2 fields |
| --- | --- |
| Player BUY | `economy,inventory,task` |
| Player CLAIM_TASK (coin/item/exp fixture) | `economy,inventory,progression,task` |
| Player HARVEST reward | `compendium,inventory,progression,task` |
| Player embedded receipt only | `task` |
| Farm PLANT/WATER/THEFT/HARVEST fixtures | `activity_summary,farm_meta,opportunity_facts,opportunity_header,plots` |
| Farm embedded receipt only | `activity_summary` |
| Outer Authority receipt/order only | no model upserts |

Farm `farm_meta` and both opportunity projections change whenever the frozen
FarmVersion changes. `guard`, Player `pet`, and Player `mail_claim_state`
remain byte-stable deterministic disabled carriers. Runtime enforcement is
byte-derived because the existing generic Store receipt does not expose a
trusted business command label; the command/carrier tests bind the frozen
matrix without inventing a second command identity at storage.

## Read paths and observability

- Player activation decodes all eight fields together. Public-profile batch
  reads request v1/v2 alternatives in one bounded pipeline; complete v2 is
  repository-validated, then the view is built only from `profile` and
  `progression`. `public_profile` and `record` are v1-only compatibility paths.
- Farm activation decodes all six fields together. Farm opportunity batch
  readers unwrap the v2 `opportunity_header`/`opportunity_facts` envelopes and
  never use a blob fallback after v2 is observed.
- Memory and Redis batch readers validate the complete v2 truth set before
  exposing a narrow projection. Redis may omit requested legacy-only
  alternatives after that validation; it never treats them as v2 truth.
- `actor-layout-inspect` reports bounded `v1`/`v2` counts. Exact v2 envelopes
  and field names are required; retained `record`, partial, or malformed v2 is
  itemized as `mixed-invalid`/decode failure and is never sampled as valid v2.

## Same-input and atomic-lane comparison fixtures

Player and Farm same-input fixtures bind policy-off v1 and 10000-permyriad v2
configurations and compare business responses, aggregate protobuf state,
RecordVersion delta, embedded receipt replay, outer receipt counts, full model
bytes, physical write counts, and absence of the old blob. V2 has one expected
extra outer receipt from migration; a business replay advances neither layout.

A Redis-adapter runtime fixture additionally commits a v2 receipt-only
candidate plus `CARE_REWARD` SourceOrder in one Binding call, verifies the
persisted order payload and due timestamp, verifies empty model upserts, and
verifies same-receipt replay does not increment RecordVersion.

These are bounded correctness comparisons. They are not capacity or 30M DAU
claims and are not archived production physical-byte evidence.

## Touched authority surfaces

- `internal/logic/actor/authority`: claimed snapshot retention, layout-aware
  snapshots, v1-to-v2 migration request, sparse v2 mutation, resolver install.
- `internal/player/actor`, `internal/farm/actor`: policy-gated activation,
  exact v2 decode/install, sticky v2 saves/reloads, command carrier fixtures.
- `internal/player/actormodelv2`, `internal/actormodel/fieldv2`: focused
  profile/progression decoder and strict environment parser.
- `internal/player/ingress`, `internal/farm/readpath`: v2 named-field readers.
- `internal/storage/authoritystore`, `internal/storage/redis/authority`:
  fail-closed projected reads and test-only commit observation. No Store/Lua
  protocol or script bytes changed.
- `cmd/player-svr`, `cmd/farm-svr`, `cmd/actor-layout-inspect`: startup policy
  composition and honest v1/v2 inspection.

Friendship Redis/Lua, H5, Controller state, Kubernetes, farm-worker source,
public protobufs, and generated protobuf files are untouched.

Original M1-B file inventory (Controller-committed before this follow-up):

```text
cmd/actor-layout-inspect/inspect.go
cmd/actor-layout-inspect/main_test.go
cmd/farm-svr/main.go
cmd/player-svr/main.go
docs/verification/r2-m1-b-actor-model-field-v2-runtime-handoff-20260810.md
internal/actormodel/fieldv2/config.go
internal/actormodel/fieldv2/config_test.go
internal/farm/actor/actor.go
internal/farm/actor/model_field_v2_test.go
internal/farm/actor/opportunity_models.go
internal/farm/actor/participant.go
internal/farm/actor/registry.go
internal/farm/actor/registry_authority_test.go
internal/farm/actor/registry_model_v2_runtime_test.go
internal/farm/readpath/authority.go
internal/farm/readpath/authority_test.go
internal/logic/actor/authority/authority.go
internal/player/actor/registry.go
internal/player/actor/registry_authority_test.go
internal/player/actor/registry_model_v2_live_test.go
internal/player/actor/registry_model_v2_runtime_test.go
internal/player/actormodelv2/codec.go
internal/player/actormodelv2/codec_test.go
internal/player/ingress/participant.go
internal/player/ingress/profile_test.go
internal/storage/authoritystore/fault.go
internal/storage/authoritystore/memory.go
internal/storage/redis/authority/model_read.go
```

Authority-smoke composition follow-up inventory (Controller-committed before
the theft-hot follow-up):

```text
cmd/authority-smoke-fixture/main.go
cmd/authority-smoke-fixture/main_test.go
docs/verification/r2-m1-b-actor-model-field-v2-runtime-handoff-20260810.md
internal/player/actor/registry_model_v2_live_test.go
```

Exact bounded uncommitted theft-hot follow-up inventory:

```text
cmd/theft-hot-audit/actor_model_reader.go
cmd/theft-hot-audit/actor_model_reader_test.go
cmd/theft-hot-audit/main.go
docs/verification/r2-m1-b-actor-model-field-v2-runtime-handoff-20260810.md
```

## Redis scripts

No Lua source or encoded argument/result shape changed in M1-B. Therefore no
golden hash update is made. The M1-A registered Redis script SHA-1 goldens remain:

```text
LoadAndClaimActorV3                 581f773b3efb6451741a0d5b78d4231b711c4781
CommitActorMutationWithOrdersV6    0bd247132e81067ad473e72b90145052b73b1b28
```

## Verification in this worktree

### Final-layout theft-hot follow-up (HEAD `7550ef97cdcd96fdcc702bc7ea7467c90c6ca2c4`)

```text
go test -buildvcs=false ./cmd/theft-hot-audit ./cmd/authority-smoke-fixture ./internal/actormodel/fieldv2 ./internal/player/actormodelv2 ./internal/farm/actormodelv2 ./internal/storage/redis/authority ./internal/player/actor ./internal/farm/actor -count=1
PASS

go test -race -buildvcs=false ./cmd/theft-hot-audit -count=1
PASS

go vet -buildvcs=false ./...
PASS

go build -buildvcs=false -o /dev/null ./cmd/theft-hot-audit
go build -buildvcs=false -o /dev/null ./cmd/player-svr
go build -buildvcs=false -o /dev/null ./cmd/farm-svr
go build -buildvcs=false -o /dev/null ./cmd/farm-gate
go build -buildvcs=false -o /dev/null ./cmd/farm-worker
PASS (all five)

go test -buildvcs=false ./... -count=1
PASS

git diff --check
PASS
```

No real 200-WSS run was executed or claimed here. The scope audit is limited
to the four theft-hot follow-up files above. No Proto/generated Proto, writer
Lua, AuthorityStore layout, H5, Friendship, Worker, Kubernetes, or Controller
evidence change exists. The Redis SHA-1 goldens remain exactly those in the
preceding section.

### Composition audit follow-up (HEAD `e451314ddab5d569abe06010b8c77fc7303fbb07`)

```text
go test -buildvcs=false ./cmd/authority-smoke-fixture ./internal/actormodel/fieldv2 ./internal/player/actormodelv2 ./internal/farm/actormodelv2 ./internal/logic/actor/authority ./internal/storage/authoritystore ./internal/storage/redis/authority ./internal/player/actor ./internal/farm/actor ./internal/player/ingress ./internal/farm/readpath -count=1
PASS

go test -race -buildvcs=false ./cmd/authority-smoke-fixture ./internal/player/actor -count=1
PASS

go vet -buildvcs=false ./cmd/authority-smoke-fixture ./cmd/player-svr ./cmd/farm-svr ./internal/player/actor ./internal/player/actormodelv2 ./internal/farm/actormodelv2 ./internal/storage/redis/authority
PASS

go build -buildvcs=false -o /dev/null ./cmd/authority-smoke-fixture
go build -buildvcs=false -o /dev/null ./cmd/player-svr
go build -buildvcs=false -o /dev/null ./cmd/farm-svr
go build -buildvcs=false -o /dev/null ./cmd/farm-gate
go build -buildvcs=false -o /dev/null ./cmd/farm-worker
PASS (fixture plus all four services)

go test -buildvcs=false ./... -count=1
PASS

go test -buildvcs=false ./internal/player/actor -run '^TestLivePlayerActorModelV2RuntimeMigration$' -count=1 -v
PASS with SKIP: REDIS_AUTHORITY_LIVE_URL is unset

git diff --check
PASS
```

The scope audit reports only the four follow-up files listed above. No Proto,
generated Proto, Redis Lua, Controller, H5, Friendship, Kubernetes, or
farm-worker source diff exists. The changed-source credential-pattern audit is
clean. The focused Redis authority suite includes the script SHA goldens; the
Lua bytes and corrected SHA-1 values in the preceding section are unchanged.
No headed CP-B rerun is claimed in this writer worktree.

### Original M1-B verification

```text
go test -buildvcs=false ./internal/actormodel/fieldv2 ./internal/player/actormodelv2 ./internal/farm/actormodelv2 ./internal/logic/actor/authority ./internal/storage/authoritystore ./internal/storage/redis/authority ./internal/player/actor ./internal/farm/actor ./internal/player/ingress ./internal/farm/readpath ./cmd/actor-layout-inspect -count=1
PASS

go test -race -buildvcs=false ./internal/actormodel/fieldv2 ./internal/player/actormodelv2 ./internal/farm/actormodelv2 ./internal/logic/actor/authority ./internal/storage/authoritystore ./internal/storage/redis/authority ./internal/player/actor ./internal/farm/actor ./internal/player/ingress ./internal/farm/readpath -count=1
PASS

go vet -buildvcs=false ./cmd/actor-layout-inspect ./cmd/player-svr ./cmd/farm-svr ./internal/actormodel/fieldv2 ./internal/player/actormodelv2 ./internal/farm/actormodelv2 ./internal/logic/actor/authority ./internal/storage/authoritystore ./internal/storage/redis/authority ./internal/player/actor ./internal/farm/actor ./internal/player/ingress ./internal/farm/readpath
PASS

go build -buildvcs=false -o /tmp/r2-m1b-player-svr ./cmd/player-svr
go build -buildvcs=false -o /tmp/r2-m1b-farm-svr ./cmd/farm-svr
go build -buildvcs=false -o /tmp/r2-m1b-farm-gate ./cmd/farm-gate
go build -buildvcs=false -o /tmp/r2-m1b-farm-worker ./cmd/farm-worker
PASS (all four)

go test -buildvcs=false ./... -count=1
PASS

go test -buildvcs=false ./internal/player/actor -run TestLivePlayerActorModelV2RuntimeMigration -count=1 -v
PASS with TestLivePlayerActorModelV2RuntimeMigration SKIP:
REDIS_AUTHORITY_LIVE_URL is unset

git diff --check
PASS
```

The ordinary Redis package and Player/Farm runtime tests execute the concrete
Redis Store and Lua scripts through isolated miniredis namespaces. No
`FLUSHALL` is used. Because the separate real-Redis gate was unset, no M1-B
real-Redis runtime PASS or archived live evidence is claimed.

## Remaining gates

- Controller review/cherry-pick followed by the final-layout 200-WSS baseline
  and slow-reader runs with both conversion cohorts at 10000 permyriad. No
  capacity result is inferred from the deterministic reader tests.
- Controller review/cherry-pick of this bounded follow-up and a headed rerun of
  the preserved CP-B browser/protocol/authority-social composition with the
  layout-aware auditor. The prior raw run remains INCONCLUSIVE.
- Controller composition review at the Merge Queue head, especially Hero/Task
  command coverage and the F1-F3 composition.
- A headed real-Redis runtime migration run with isolated exact-key evidence.
- Controller-reviewed physical byte comparison/evidence beyond the bounded
  logical Store counters in these fixtures.
- Kubernetes Staging and the formal `CP-ACTOR-MODEL-FIELDS-V2` decision.

Disabling new conversion is the rollback control. It does not and cannot
revert an existing v2 Actor to v1.
