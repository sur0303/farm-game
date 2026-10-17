# SCB-HERO-SOCIAL-READ — Shared protocol seam decision

Date: 2026-08-08
Checkpoint: CP-HERO-SOCIAL-NO-PET
Status: SHARED_APPEND_REQUIRED

## Mechanical finding

The existing command identities are already frozen and registered:

- `GET_FRIEND_LIST = 14`
- `GET_NEXT_FRIEND_OPPORTUNITY = 15`

The existing next-opportunity result already carries target farm, profile, action mask, opportunity revision, validation time, explicit FOUND/NONE, and a navigation generation. Its request intentionally carries no client cursor or visited set.

The friend-list wire is not sufficient:

- `GetFriendListRequest` has no cursor/page_size;
- `FriendEntryView` contains only PublicProfile and reserves 2..20;
- `GetFriendListResult` reserves 1..50 and contains no page;
- no push identity exists for an opportunity invalidation/pull hint.

Backend and H5 both need the missing fields. They must not invent separate JSON and WSS shapes, so the decision is **not** `NO_SHARED_CHANGE`.

## Append-only SCB

`SCB-HERO-SOCIAL-READ` appends only unused field numbers outside every reserved range:

- request cursor/page_size at 1/2;
- FriendEntry relation/action/opportunity fields at 21+;
- FriendList page/result fields at 51+;
- next-opportunity relation fields at 6+;
- `FRIEND_OPPORTUNITY_CHANGED` push type at 9;
- typed push payload in `friend.proto`.

No existing field number, enum value, payload meaning, reserved range, command identity, or HTTPS share mutation path is changed.

## Authority semantics

- FriendIndex page is projection only.
- Every page row is validated against canonical FriendPair/Block and exact relation_epoch.
- `action_mask` is a server-authoritative read result, not a permit.
- UNKNOWN is explicit and must render Loading/Retry, never NONE.
- VisitPermit and ActionPermit remain internal; neither is exposed to H5.
- Opportunity push is only a pull hint. It does not authorize a button and does not carry a final action mask.
- Final Care/Theft remains ActionAdmission + FarmActor adjudication.

## Action-mask bits

The first three values deliberately match the existing Authority Facts evaluator:

```text
CAN_THEFT = 1
CAN_WATER = 2
CAN_PEST  = 4
CAN_VISIT = 8
```

Friend cards may expose all four. Next-opportunity candidates use the interaction bits and are still revalidated during execution.
