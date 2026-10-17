# SCB — Friend roster physical batch reads (2026-08-09)

Status: APPROVED FOR HOTFIX IMPLEMENTATION

## Current physical truth

- PublicProfile is grouped into one BatchGetPublicProfiles RPC per Player holder,
  but the holder currently executes up to eight Registry.Read calls. Each call
  enters the per-player serial executor and loads the full PlayerRecord.
- Pair validation is logically batched by ReadAPI, but RedisPairRepository does
  not implement PairBatchReader. ReadAPI therefore runs up to eight independent
  GetPair calls, each with multiple Redis reads.
- FarmOpportunity is already one RPC plus pipelined model-field HMGETs.

## Player PublicProfile projection

Append `model:public_profile` to the Player Authority model state. Its payload is
a deterministic `public.PublicProfileView` containing only player_id,
display_name, avatar_id and level.

For Authority-mode creation and every changed Player turn:

```text
model:record + model:public_profile + outcome/orders
```

must commit in one writer-fenced RecordVersion CAS. There is no async projector.
Existing/missing projection rows use a second bounded physical model-field
batch to read `model:record` and derive the same profile without entering the
Player serial executor. They remain eligible for compact backfill on the next
Player Authority mutation. Legacy blobstore mode remains on the old reader.

Player-svr BatchGetPublicProfiles uses one bounded
`BatchReadModelFields(DomainPlayer, ids, [public_profile])`; it validates ID and
profile shape in request order. This read bypasses Registry mailboxes. Missing
legacy rows alone use bounded direct record-field fallback reads. Formal compact
byte-volume performance validation still requires migration/backfill evidence.

## Redis Pair batch

RedisPairRepository implements `PairBatchReader`. One batch:

- validates input uniqueness and canonical pair identity;
- groups shared bucket metadata by physical bucket;
- issues one Redis pipeline containing bucket-envelope reads and per-record
  TYPE/HMGET/HLEN reads;
- validates the same fixed metadata manifest, counters, key types, field counts,
  canonical identity, relation and block fields as GetPair;
- preserves input order and the existing absent-pair value;
- fails the complete friend page on corruption or dependency error.

No Pair projection becomes truth and no validation is weakened. The old GetPair
path remains for single reads and must return semantically identical snapshots.

## Bounds and failure

Friend roster remains page-size 20 (profile protocol max 50). All physical reads
are bounded. Missing profile projection falls back; malformed projection fails
closed. Pair corruption fails closed. No client-visible response schema changes
are introduced by this physical-read optimization.
