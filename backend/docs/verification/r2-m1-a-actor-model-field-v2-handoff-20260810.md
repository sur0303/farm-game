# R2 M1-A Actor Model Field V2 repository/layout handoff

Date: 2026-08-10

## Status, base, and boundary

- Exact Merge Queue base/HEAD: `1c934e61a54b1f229c3df0080a0b865207d91f8f`.
- Branch/worktree: `codex/r2-m1-model-field-v2-20260810`.
- This is a repository, layout, codec, and atomic-migration candidate. Normal
  Player/Farm activation still uses `read=v1|v2, write=v1`; conversion policy is
  default off and the existing Actor binding is not wired to encode or install
  v2 state.
- This therefore is **not** `CP-ACTOR-MODEL-FIELDS-V2`, a production cutover,
  a Kubernetes/Staging PASS, or an R2 PASS. The diff is intentionally left
  uncommitted for Controller review.
- No H5, Controller state, deployment, Friendship Redis/Lua, public Proto, Pet,
  Mail, or Guard gameplay was changed.

The committed Q1 fixture overrides the older M0 retained-blob proposal. A
successful migration removes every obsolete v1 model field, including
`model:record`; there is no retained/inert blob and no dual write.

## Version and authority invariants

`_meta:authority_layout_version=v1` remains the physical routing profile. It
continues to select the existing hash tag, shard, and key layout. The new
`_meta:model_layout_version=1|2` is a logical codec/truth-set discriminator
inside that same Actor hash. Legacy records with no discriminator are read as
model layout 1; every new commit writes it explicitly.

There remains exactly one Authority Group, ShardWriteFence, WriterClaim,
RecordVersion, PersistenceLane, Authority receipt sequence, SourceOrder, and
DueIndex. A model field has no independent version or CAS. The compile-time
schema registry has deterministic manifest bytes for golden review, but does
not persist a second schema/truth manifest: the existing
`_meta:model_manifest`, aggregate bytes, digest, and model-layout discriminator
are the complete physical claims.

## Exact v2 schema and carrier ownership

All persisted v2 fields use a deterministic field-bound `AMF2` envelope:
magic, domain ID, registry field ID, codec version, exact payload length, and
payload. Cross-domain/field substitution, trailing bytes, unknown protobuf
fields, non-canonical protobuf bytes, excess repeated items, and values over
the field cap fail closed.

| Domain | Exact sorted physical truth set | Current business-carrier mapping |
|---|---|---|
| Player | `compendium,economy,inventory,mail_claim_state,pet,profile,progression,task` | identity/public profile in `profile`; wallet/economy version/unlock reservations in `economy`; inventory/item reservations in `inventory`; progression and codex in their named fields; legacy tasks, TaskDomainState, and embedded legacy operation receipts occur once in `task` |
| Farm | `activity_summary,farm_meta,guard,opportunity_facts,opportunity_header,plots` | IDs/version in `farm_meta`; plots/cycles in `plots`; embedded operation receipts, settlements, theft attempts, care attempts, and current Guard proof carriers occur once in `activity_summary`; current deterministic opportunity projection bytes remain in their named fields |

`pet`, `mail_claim_state`, and `guard` use the exact present value `DISABLED`
inside their field-bound envelope. This distinguishes deferred gameplay from a
missing/corrupt field without adding gameplay. Player `public_profile` is not
relationship or business authority and is not retained as an additional v2
physical truth; it can be deterministically rebuilt from `profile` and
`progression` in M1-B.

The converters split the already-generated `PlayerRecord`/`FarmRecord` into
explicit protobuf subsets rather than adding public Proto methods. Decode
rejects any populated protobuf field outside that subset, then reconstructs
the exact legacy aggregate. Rich byte goldens freeze canonical v1 bytes and
every resulting v2 field byte. Farm tests additionally rebuild the current
opportunity projections after round trip.

## Commit and migration state machine

`AuthorityCommitRequest` still carries the bounded complete immutable
`CandidateState`. An optional `ActorModelMutation` adds deterministic sparse
upserts/deletes and binds the exact resulting sorted manifest, total bytes,
business digest, model layout, and intent. A nil mutation adapts the existing
full-state v1 API into the same commit primitive; it cannot write or revert v2.

The Redis commit linearization order is:

```text
Shard fence
→ WriterClaim
→ existing receipt replay/full-key-or-payload conflict
→ RecordVersion
→ stored/candidate item, byte, manifest, digest, sparse-relation, order limits
→ model HDEL/HSET + receipt/outcome/order/due mutation
→ exactly one RecordVersion increment
```

Upserts and deletes must be disjoint and strictly sorted. The complete
candidate must equal `current - deletes + upserts`; missing or redundant
declarations fail. Layout 2 requires the exact domain registry and valid AMF2
envelopes. Ordinary update stays within its current layout. The only layout
transition is the explicit `MIGRATE_V1_TO_V2` intent from 1 to 2, and it
requires an existing `record`, a declared `record` delete, and no `record` in
the resulting candidate.

| Before / event | Result |
|---|---|
| v1 + ordinary update | v1 sparse/full commit under the existing receipt and RecordVersion contract |
| v1 + policy-ineligible activation | v1 remains the only truth; no conversion attempt |
| v1 + qualified migration `COMMITTED` | exact v2 fields/metadata installed, all declared obsolete v1 fields including `record` deleted, receipt/order/due committed, RecordVersion +1 |
| migration `NOT_COMMITTED` | no model, receipt, order, due, layout, digest, or RecordVersion change |
| migration `TIMEOUT_UNKNOWN` | resolve through the existing `FenceAndClaim` attempt cursor; retry the byte-identical candidate/mutation/receipt with only the resolver-installed current claim |
| v2 + ordinary v2 update | same single Actor CAS and sparse relation; no per-model CAS |
| v2 + legacy/full-v1 or migration request | fail closed; v2 cannot revert or consult `model:record` |
| v2 malformed, partial, unknown, wrong-envelope, or aggregate mismatch | fail closed before Actor installation or bounded projection read |

The public-profile compatibility path now checks the model layout before its
legacy record lookup. Seeing v2 without the M1-B profile decoder returns a
fail-closed error and never asks for `model:record`. Redis batch reads fetch and
validate the complete bounded v2 core set in one pipeline before returning a
requested projection, so a narrow read cannot make a partial v2 record useful.

## Default-off cohort policy

The policy has separate Player and Farm enable bits plus a bounded permyriad
cohort. Cohort selection is deterministic SHA-256 over a versioned salt,
domain, and Actor ID; it persists no cohort value and adds no Actor-ID metric.
The default has both domains disabled and cohort zero. Policy is consulted only
for a v1 record. An already-v2 record always selects write layout 2 even after
conversion is disabled.

## Redis script versions and intentional hashes

| Script | SHA-1 | Intentional change |
|---|---|---|
| `LoadAndClaimActorV3` | `581f773b3efb6451741a0d5b78d4231b711c4781` | add the same-record logical model-layout discriminator, legacy-absent-as-v1 handling, and exact v2 manifest checks without changing static routing |
| `CommitActorMutationWithOrdersV6` | `0bd247132e81067ad473e72b90145052b73b1b28` | bind a complete candidate to sparse HSET/HDEL declarations, enforce the one-way atomic migration/delete, and retain existing receipt/outcome/SourceOrder/DueIndex atomicity and replay ordering |

No script was edited under an old registered version name. Tests pin both new
script bytes. No test or implementation uses `FLUSHALL`; the optional live
migration test allocates an isolated namespace and cleans only its exact keys.

## Touched authority surfaces

- `internal/storage/authoritystore`: model layout/mutation contract, memory
  implementation, capacity accounting, migration/unknown/race fixtures.
- `internal/storage/redis/authority`: Store argument/result binding, bounded
  v2 reads, Lua V3/V6 scripts, script goldens, miniredis and live-gated tests.
- `internal/actormodel/fieldv2`: exact registries, AMF2 envelope, caps, and
  default-off domain/cohort policy.
- `internal/player/actormodelv2` and `internal/farm/actormodelv2`: deterministic
  v1/v2 converters; Farm byte/rebuild goldens live beside the private Farm
  model builder.
- `internal/player/ingress`: narrow no-v2-to-v1 public-profile fallback fence.
- This handoff only outside those implementation/test surfaces.

Friendship Redis/Lua and all deployment/Controller/H5 state are untouched.

## Deterministic evidence

The focused tests cover exact Player/Farm v1 and field bytes; nil/empty
canonical round trips; exact manifests; cross-field and malformed envelopes;
unknown fields and capacities; sparse update and atomic SourceOrder/DueIndex;
receipt replay/conflict; fence/claim/version conflicts; committed/no-op
migration; timeout before/after apply; `FenceAndClaim`; old-holder fencing;
idempotent retry; no legacy revert; partial v2; and concurrent v1
mutation/migration with one RecordVersion winner.

Commands and final results for this worktree:

```text
go test -buildvcs=false ./internal/actormodel/fieldv2 ./internal/player/actormodelv2 ./internal/farm/actormodelv2 ./internal/storage/authoritystore ./internal/storage/redis/authority ./internal/logic/actor/authority ./internal/player/actor ./internal/farm/actor ./internal/player/ingress ./internal/farm/readpath -count=1
PASS

go test -buildvcs=false ./internal/storage/redis/authority -run TestLiveActorModelV2Migration -count=1 -v
PASS with TestLiveActorModelV2Migration SKIP: FARM_REDIS_URL is not set
```

The ordinary Redis package tests execute the real Store and Lua scripts through
the repository miniredis rig. The separately gated real-Redis migration case
is present but was not executed here; no `REAL_REDIS_PASS` is claimed.

```text
go test -race -buildvcs=false ./internal/actormodel/fieldv2 ./internal/player/actormodelv2 ./internal/storage/authoritystore ./internal/storage/redis/authority ./internal/farm/actor ./internal/player/ingress -count=1
PASS

go vet -buildvcs=false ./internal/actormodel/fieldv2 ./internal/player/actormodelv2 ./internal/farm/actormodelv2 ./internal/storage/authoritystore ./internal/storage/redis/authority ./internal/logic/actor/authority ./internal/player/actor ./internal/farm/actor ./internal/player/ingress ./internal/farm/readpath
PASS

go test -buildvcs=false ./... -count=1
PASS

git diff --check
PASS
```

## Remaining M1-B and later gates

M1-B must wire Player/Farm activation to bounded-read all core v2 models
together, choose explicit dirty sets from the frozen command matrix, convert a
qualified v1 snapshot once, install memory only after explicit `COMMITTED`, and
exercise normal Actor unknown-response/crash/restart behavior. It must also
wire v2 public-profile and Farm opportunity readers without introducing a
fallback truth, and qualify rollback as “disable new conversion while existing
v2 stays v2.”

Still outside this candidate: Hero/Task decisions, F1-F3 composition, Guard/Pet
/Mail gameplay, production-sized cohort evidence, same-input v1/v2 physical
operation/request/response/model/receipt/order/due byte comparison, real Redis
archived output, Kubernetes/Staging, and the formal
`CP-ACTOR-MODEL-FIELDS-V2` gate.
