# RT2X Controller Boot Rollover Handoff

- task: RT2Q-discovered controller Boot-rollover blocker
- frozen base: `abbc60797a3bce63d2feac1781a54b53ecea086b`
- signed code commit: `e08ce13c0d4858d216bbe22758f9f40e8d16f4ad`
- independent corrective-review base: `88b76b46f1d78839bd1b4e8c3f42134aaec5cbd7`
- signed Watch-establishment correction: `e1dc6f8f2c121e98b8eadcbaa3628f11a59883c6`
- signed retired-limit production wiring: `908bca4a9f8e8478c553f102f6f5bfcea4e3a5e3`
- Writer: sole production Writer, OpenAI Sol/xhigh
- push: none
- status: independent P0=0/P1=1/P2=1 findings closed at code/component scope; RT2Q Final runtime evidence remains pending

## Independent corrective review closure

The independent exact review found that the generated gRPC `WatchFrom` call returned locally before
server authorization/subscription and before any controller/Boot envelope. A pre-handshake server
failure could therefore look like exhaustion of an established Watch and incorrectly enable the
restart-aware rollover path. It also found that the bounded retired-Boot option was not wired into
production environment composition.

Both findings are closed without changing Proto, Authority, Manager, RoutePlan, Holder, Redis, or a
command root:

- The production server authorizes first, reads durable current `R`, subscribes from `R` rather than
  caller-controlled history, and fails subscription before sending anything. It then sends `R` as a
  mandatory authenticated complete-current envelope. Any racing `R+1` is queued behind `R`; a caller
  ahead of durable current is rejected rather than receiving an apparently established empty Watch.
- `SnapshotClient.WatchFrom` now bounds, synchronously receives, decodes, and validates that mandatory
  first envelope before returning a channel. Pre-handshake RPC/auth/subscription/Recv/decode/Boot errors
  return directly. The validated handshake is placed in a reserved bounded channel slot before the
  ordinary slow-consumer path starts, so it is neither dropped nor counted as an unvalidated local
  channel. Once returned, the Watch is established and pinned to its accepted Boot.
- Consequently C0 Watch closure enables only the C1 restart-aware full resync; the following C1 Watch
  must validate an authenticated C1 handshake before establishment. A quiet equal-revision C1 cannot
  be skipped before C2. No hot Resolve RPC or patch/delta path was added.
- `NewRemoteSnapshotClientFromEnvironment` now wires `ROUTE_CONTROL_RETIRED_BOOT_LIMIT` into
  `ClientOptions.RetiredBootLimit`. Absence defaults to `8`; explicit empty, malformed, zero, negative,
  or values above the hard maximum `64` fail startup. Retired identities are never evicted or silently
  reset in-process. Capacity exhaustion terminally removes consumer readiness, so the default deployment
  must coordinate consumer process replacement/restart before the ninth rollover.
- The remote-watch Compose example uses `${ROUTE_CONTROL_RETIRED_BOOT_LIMIT-8}` deliberately: an unset
  variable renders `8`, while a present empty variable remains empty and reaches the strict production
  parser, which rejects it before TLS setup. Using the `:-8` form would incorrectly hide an explicit
  empty deployment value.

## Closed boundary

The deployment contract remains stop-first/Recreate, single controller, no overlap, and a new
never-reused Boot for every controller process generation. Consumers can now survive C0→C1→C2
without weakening the existing strict path:

- `RemoteSnapshotSource` is unchanged. `RestartAwareRemoteSnapshotSource` is an optional narrow
  capability whose request binds the Pump's exact installed complete snapshot and revision.
- `RemoteAssignmentPump` uses the optional capability only after an established Watch channel
  has closed. Establishment now means the server subscription exists and the mandatory authenticated
  complete-current controller/Boot envelope has been synchronously validated. Ordinary bootstrap/full
  fetch, any pre-handshake Watch failure, and a source lacking the optional interface retain the old
  behavior.
- Every ordinary unary call and the complete lifetime of every Watch share the accepted Boot pin.
  An unexpected new Boot on those paths remains sticky terminal drift.
- Restart-aware fetch is exclusive with old-Boot publishers. The old Watch unregisters before its
  consumer channel closes, so the production rollover RPC cannot start while that Watch is live.
  A pre-existing ordinary operation that can still publish old Boot makes rollover terminal;
  an operation admitted after exclusive rollover begins waits until the atomic pin transition is
  complete.
- Controller ID, realm, and topology must remain exact. A new-Boot revision cannot be below either
  the Pump-installed baseline or the highest transport-accepted revision. Equal revision requires
  canonical equality of schema, realm, revision, canonical JSON, checksum, topology fingerprint,
  and committed-at envelope metadata, using the Pump baseline rather than an uninstalled
  speculative response.
- A new Boot must be canonical, nonempty, distinct from current, and absent from the retired set.
  The client validates every precondition before atomically retiring C0 and pinning C1. Any later
  C0 response is terminal; after C2, C1 is terminal in the same way.
- Retired identity state is a bounded set with no eviction. `ClientOptions.RetiredBootLimit`
  defaults to 8, accepts only 1..64, and fails closed before capacity would require forgetting an
  identity. Production uses the strictly parsed `ROUTE_CONTROL_RETIRED_BOOT_LIMIT` setting and requires
  coordinated consumer replacement/restart before rollover nine at the default.
- Same-Boot transport reconnect remains recoverable. Rollover rollback, equal mismatch, retired
  identity, overlap, capacity, and contract errors latch terminal. Close cancels the connection
  operations and joins admitted fetch/Watch/resync calls.
- Higher revisions still pass through the existing Pump complete-snapshot monotonic and transition
  checks before installation. A rejected transition terminates AssignmentComposition and makes
  readiness false. No hot Resolve RPC, second source/Pump/provider/resolver, leader election, lease
  state machine, or payload/metadata trust was added.

The only production composition change is the minimal `firstFetchSource` pass-through required to
preserve the optional capability after the already-existing cached startup fetch wrapper.

## Deterministic coverage

Channel/barrier-driven tests, without correctness sleeps, cover:

- real-gRPC authorization and Hub-subscription rejection before `WatchFrom` returns a channel, with
  zero restart-aware fetches;
- real-gRPC durable `R`/racing `R+1` delivery as ordered handshake/update with no gap, rollback, or
  unsafe duplicate, including the minimum ordinary client queue;
- blocked real-gRPC handshake context cancellation, establishment timeout, and client Close/join;
- a real-gRPC Pump-driven quiet equal-revision C0→C1→C2 sequence with a mandatory handshake for every
  generation, C1 retirement by C2, and terminal rejection when C1 later returns;
- direct pre-establishment malformed, capacity, controller, Boot, retired-Boot, realm, and topology
  rejection;
- C0 Watch close → equal identical C1 resync → healthy Pump and C1 Watch;
- C0→C1 valid higher complete revision installation and convergence;
- unchanged same-Boot transport reconnect;
- ordinary-fetch and mid-Watch new-Boot terminal failure;
- active-Watch rollover rejection before any rollover RPC;
- installed/transport revision rollback and equal payload/checksum/committed-envelope mismatch
  without unsafe pin mutation;
- exact Pump baseline binding when the client saw an uninstalled speculative equal-revision
  envelope;
- C0 retirement after C1, C1 retirement after C2, and terminal retired-Boot return;
- duplicate resync serialization, Watch-close/resync/Close cancellation and join, and the existing
  blocked-Watch/concurrent-fetch Close barrier;
- retired-set capacity exhaustion without eviction;
- controller ID, realm, and topology drift;
- optional-capability selection only after a closed Watch and legacy source compatibility;
- AssignmentComposition healthy convergence for equal/higher rollover and terminal false readiness
  for every invalid rollover class, including post-transport higher-transition rejection.

## Verification

The original RT2X verification remains recorded below. The corrective review closure additionally
passed on exact combined code tip `908bca4a9f8e8478c553f102f6f5bfcea4e3a5e3`:

- `go test -buildvcs=false -count=20 -timeout=600s ./internal/actorrouting/remote ./internal/routecontrol/transport ./internal/authoritycomposition`
- the exact corrective transport/composition regression set at `-count=100`
- the same exact corrective set at `-race -count=100`
- the affected packages once with `-race`
- `go test -buildvcs=false -count=1 -timeout=1800s ./...`
- `go test -race -buildvcs=false -count=1 -timeout=2400s ./...`
- `go vet -buildvcs=false ./...`
- `go build -buildvcs=false ./...`
- `./scripts/gen-proto.sh`, followed by zero diff under `api/proto` and `api/gen`
- real base plus remote-watch Compose rendering, including four consumer values plus the resolved
  extension anchor at retired limit `8`, and the existing singleton/stop-first assertions
- exact authorized-path/frozen-surface, added-secret, large-object, forbidden Redis-operation,
  `git diff --check`, ancestry, and clean-worktree checks
- cryptographic SSH `git verify-commit` success for both corrective commits

The earlier code content committed as `e08ce13c0d4858d216bbe22758f9f40e8d16f4ad` had passed:

- `go test -buildvcs=false -count=20 -timeout=300s ./internal/actorrouting/remote ./internal/routecontrol/transport ./internal/authoritycomposition`
- the new deterministic Pump, transport, and composition subsets at `-count=100`
- the same deterministic subsets at `-race -count=100`
- affected packages at `-race -count=20`
- `go test -buildvcs=false -count=1 -timeout=1800s ./...`
- `go test -race -buildvcs=false -count=1 -timeout=2400s ./...`
- `go vet -buildvcs=false ./...`
- `go build -buildvcs=false ./...`
- `./scripts/gen-proto.sh`, followed by zero diff under `api/proto` and `api/gen`
- real base plus remote-watch Compose rendering/assertions
- `git diff --check`, exact authorized-path/frozen-surface checks, forbidden Redis-operation scan,
  added-diff secret scan, large-object check, and SSH signature-presence check

No command root, Proto/generated file, Authority, Manager, RoutePlan, Holder, Redis adapter, or
dependency was changed. The only deployment YAML change wires the bounded retired-Boot setting into
the existing remote-watch example. No push was performed.

## Final Reviewer P2 closure

On exact base `cdf5feb4b26919ac831a30fcdbc3f6f746e9801e`, the remote-watch example's retired-limit
interpolation was corrected from `:-8` to `-8`. Focused tests and direct real Compose renders cover both
states: unset resolves the shared extension anchor and all four consumers to `8`; explicit empty remains
empty at all five rendered locations and is rejected by the strict production parser before TLS setup.
No other deployment behavior changed.

## Remaining gate

RT2Q Final must still run its independent multi-Pod evidence on the final candidate: live consumers
through controller C0→C1 restart, planned A→B Drain, commit-unknown recovery, crash takeover, Fresh
Join, gameplay, and four-truth reconciliation. This handoff does not claim that runtime evidence or
Route Control MVP acceptance.
