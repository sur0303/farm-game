# SC1 Plant vertical-slice handoff

Status: implemented in the assigned worktree/branch, Plant only.

Exact commit: not created in this sandbox. The assigned branch remains at
`d2fd3f308f471cff1b4f626da085b78096eaa74c` because its linked Git metadata is
read-only; see the final blocker below.

## Scope

This slice adds `internal/scenario/plant/**`. It composes the existing
`workflow.Engine`, `workflow.WorkRepository`, `workflow.ParticipantInvoker`,
Player/Farm Participant protobuf contracts, v2 identity helpers, and canonical
hash helpers. It does not change Actor implementations, Participant handlers,
generated/protocol files, the common Workflow runtime, Redis storage, routing,
Gate/cmd wiring, process composition, deployment, or any later gameplay
scenario.

The coordinator performs `CreateIfAbsent` before its first `Resume` call, so a
caller/process crash cannot create a participant effect without a durable root.
`BuildRoot` clones and deterministically marshals the complete
`CropRuleSnapshot` into the immutable root `CommitPlantPayload` and stores its
canonical snapshot hash. The effective key uses `FARM_ACTOR` plus the canonical
farm ID, and `identity.WorkflowID` (`farm-workflow-id-v2`) is the only new Plant
workflow identity. Reservation and crop-cycle IDs are derived from that
workflow ID with the existing identity helpers.

## Focused P1 closure

The coordinator no longer accepts a fixed bucket ID. It receives the existing
`workidentity.BucketResolver` and frozen bucket version; every `Create` resolves
`IndependentWorkBucket(root.WorkflowID, bucketVersion)` and validates the
resulting `WorkAddress` before calling the generic engine. Resolver failures
therefore occur before `CreateIfAbsent` or any participant effect.

Trusted root admission requires `SubjectPlayerID == TargetFarmID`. Durable root
decoding repeats the owner proof as
`root.OwnerPlayerId == record.SubjectPlayerID == root.FarmId`, before any step
payload is built. The scenario-local snapshot validator mirrors the complete
Farm `CommitPlant` admission invariants: top-level version/range/yield rules,
matching compendium identity and versions, the ten 2-percent steal slots and
cap, and meaningful WATER/REMOVE_PEST care incidents with valid fixed or
deterministic-random schedules. `BuildRoot` and `decodeRoot` both apply it.

The Farm step status selects the only legal branch: applied/no-change can build
`COMMIT_SEED_RESERVATION`, while rejected can build
`RELEASE_SEED_RESERVATION`. Both payload construction and final durable folding
recheck that branch, so a forged terminal release cursor cannot compensate an
APPLIED Farm result. The final Player fold additionally proves the
deterministic typed result, canonical result hash, reservation ID, seed item ID,
and terminal state (`CONSUMED` or `RELEASED`). The RPC adapter rejects empty
terminal APPLIED results and empty terminal NO_CHANGE results for both final
Player commands.

Every downstream builder also requires the durable `RESERVE_SEED` receipt to be
`APPLIED` or `NO_CHANGE` before reading its typed result. A rejected, pending,
missing, or malformed Reserve cursor fails closed before Farm or final Player
invocation; the table-driven regression covers `COMMIT_PLANT`,
`COMMIT_SEED_RESERVATION`, and `RELEASE_SEED_RESERVATION`.

Coordinator and Worker/Recovery transport paths are separate. The Coordinator
adapter calls `ExecuteStep` with `CoordinatorWorkContext`; the recovery adapter
calls `ResumeStep` with `InternalCallerContext` and the supplied worker lease
identity. Scenario restart tests retain only a separate durable receipt store,
recreate the Participant adapter instances, and prove receipt-first replay
with one effect per invocation.

## Durable state and step table

| Durable step | Authority | Typed payload | Required dependency | Terminal fold |
| --- | --- | --- | --- | --- |
| `RESERVE_SEED` | Player | `ReserveSeedPayload` | none | Applied/no-change → Farm; rejected → `REJECTED` |
| `COMMIT_PLANT` | Farm | frozen root `CommitPlantPayload` plus `ReserveSeedResultHash` | durable `RESERVE_SEED` typed result hash | Applied/no-change → Player commit; rejected → Player release |
| `COMMIT_SEED_RESERVATION` | Player | `CommitSeedReservationPayload` plus `CommitPlantResultHash` | durable Farm typed result hash | Applied/no-change → `COMPLETED`; rejected → `FAILED` |
| `RELEASE_SEED_RESERVATION` | Player | `ReleaseSeedReservationPayload` plus `TerminalFarmResultHash` | durable rejected Farm typed result hash | Applied/no-change → `COMPENSATED`; rejected → `FAILED` |

The four `StepRecord` invocation IDs are created once from the workflow ID and
step ordinal. Existing step payload bytes and hashes are reused on retry and
takeover. Later payloads are rebuilt from the durable prior `StepRecord`; a
missing, non-terminal, malformed, non-deterministic, mismatched, or wrongly
typed result fails closed. The scenario adapter also binds the Participant
`StepCall.DependsOnResultHash` from the typed payload, including the Farm
rejection/release branch.

## Crash and response-loss matrix

| Failure boundary | Recovery behavior | Duplicate-effect expectation |
| --- | --- | --- |
| Before root creation | No participant call is possible; next request reuses `CreateIfAbsent`. | Zero effects |
| After root creation, before `RESERVE_SEED` | Resume builds and persists the step once. | One invocation ID |
| After Player reserve effect, before response | Receipt-first replay uses the same invocation and payload hash. | No second seed deduction |
| After Farm commit effect, before response | Receipt-first replay uses the same Farm invocation. | No second CropCycle creation |
| After Player commit/release effect, before response | Receipt-first replay uses the same final invocation. | No second finalization/release |
| Between any durable step CAS and process restart | A new definition/engine resumes from the persisted WorkRecord. | No step is skipped or regenerated |
| Lease expiry after an unknown call | A new worker takes over the record and replays the exact call. | Receipt prevents duplicate effect |
| Caller snapshot/config mutation after Create | The persisted root snapshot and hash remain authoritative. | Retry payload is unchanged |
| Missing or malformed prior typed result | `BUILD_PAYLOAD_FAULT` fails the record closed. | No downstream participant call |

Farm rejection is the only compensation branch. Farm applied and no-change both
continue to Player commit. A Player commit or release rejection is terminal
`FAILED`; the coordinator never compensates after a Farm result that may have
committed.

## Tests run

Passed:

- `GOCACHE=/tmp/sc1-final4-test-cache go test -count=1 ./internal/scenario/plant`
- `GOCACHE=/tmp/sc1-final4-race-cache go test -race -count=1 ./internal/scenario/plant`
- `GOCACHE=/tmp/sc1-final4-vet-cache go vet -buildvcs=false ./internal/scenario/plant`
- `GOCACHE=/tmp/sc1-p1-final3-vet-all-cache go vet ./...`
- `GOCACHE=/tmp/sc1-p1-final3-build-cache go build -buildvcs=false ./...`

The Plant tests cover same-key idempotency/conflict, exact order and authority,
reserve rejection, Farm rejection and deterministic release, Farm no-change,
success finalization, response loss after every effect, crash/restart between
durable steps, worker takeover, frozen snapshots, dependency hashes, malformed
prior results, RPC typed request/response validation, and absence of concrete
Actor implementation imports.

The focused P1 regressions additionally cover per-workflow bucket resolution
and resolver failure ordering, owner-only binding at admission and durable
decode, every malformed nested snapshot requirement, inconsistent Farm branch
statuses, final Player typed-result identity/state/hash failures, the explicit
`ResumeStep` worker wire, adapter recreation around durable receipts, and a
rejected Reserve cursor with valid-looking typed data at every downstream
builder.

The manifest commands were also run:

- `scripts/test-unit.sh` — blocked by existing Redis/miniredis tests because
  the sandbox denies `listen tcp 127.0.0.1:0` with `socket: operation not
  permitted`.
- `scripts/test-race.sh` — the same sandbox-only Redis socket limitation.
- `scripts/verify-s2.sh` — reaches the same unit-test limitation and exits
  before its later race/build stages; those stages were run directly above.
- `GOCACHE=/tmp/sc1-p1-final-full-test-cache go test -count=1 ./...` — the
  Plant package passes, while pre-existing Redis/miniredis and websocket tests
  fail because the sandbox denies local listening sockets (`operation not
  permitted`).
- `go build ./...` — blocked by environment VCS stamping (`error obtaining VCS
  status: exit status 128`); compilation succeeds with `-buildvcs=false`.

## Remaining integration seams and evidence

No Gate, public protocol, `cmd/**`, route implementation, worker loop, or
process composition was added by design. Composition must provide the two
authority-specific Participant RPC clients, a route provider that supplies and
checks the existing route fence, the physical work bucket placement, and the
production `WorkRepository`.

The RPC adapter is production-usable at the existing
`workflow.ParticipantInvoker` boundary. The current common `StepCall` boundary
does not carry a worker lease proof/epoch, so the recovery-loop composition
must continue to supply lease fencing around `Engine.Resume`; this slice does
not alter the forbidden common runtime. The generic runtime also records its
nearest successful predecessor dependency in `StepRecord`; the scenario
adapter supplies the exact Farm-result dependency to Participant calls and
payloads on the rejection/release branch without changing that runtime.

### Agent-sandbox evidence

Focused package tests, race, vet, and the no-VCS full build are the evidence
available in the Agent sandbox. Real Redis/miniredis and deployment/platform
evidence remains pending because the execution sandbox forbids local listening
sockets. Existing Player/Farm handoffs report the same limitation. Full
manifest unit/race commands consequently stop in pre-existing Redis-backed
tests; this is reported rather than masked.

### Host-side pending evidence

The host/orchestrator should rerun the manifest commands with Redis/miniredis
socket access and inspect the final staged diff. The requested commit is also
pending: the worktree `.git` file points to
`/data/home/developer_026/CodeBuddy/20260519141303/farm-game/.git/worktrees/`, which
the sandbox exposes read-only. This agent did not attempt Git metadata
workarounds and did not create a commit, push, or merge; the orchestrator can
commit the two allowed paths after review.
