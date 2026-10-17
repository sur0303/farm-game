# RT2D Holder Control Handoff

状态：同一 RT2D 主 Writer 对 exact6 Luna 批次的 3 P1 closure
complete；SSH 签名 commit，不 push。
本轮 closure base：`9bae1456a1872f46986b65661fa34b186cd39504`（进入工作时 clean）。
exact5 closure commit：`9bae1456a1872f46986b65661fa34b186cd39504`。
exact5 closure base：`10a0f506a922593d2376c1e8527ce794da121b97`。
上一轮 closure base：`ca017bdb54b971339b432a8f8e57b76a860b5795`。
上一轮 Reviewer closure base：`fce68ce10f2bca748b84c5efe02f81187e62d50f`。
Phase1 signed base：`3e0a6b3d4340c9fa539a6f977562934851d2f8c1`。
Phase2 base：`3aa5f4d36e1efe43baf8845a44e961baeb2a95f2`。
Round-Transport head：本 handoff 所在的最终 SSH-signed commit；最终 SHA 以 handoff 报告为准。

## Delivered

### Core HolderRegistry

Owned implementation：

- `internal/routecontrol/holder_registry.go`
- `internal/routecontrol/holder_registry_test.go`

The registry owns only holder liveness and capacity facts. It records
`domain/holder_id/boot_id/internal_endpoint/tls_dns_identity`, lifecycle state,
process readiness, draining flag, serving count, snapshot revision, start and
heartbeat times, logical lease, monotonic sequence, capacity weight, and a
bounded resource summary.

The externally visible lifecycle keeps startup and shutdown monotonic while
allowing a live dynamic Holder to lose or regain its final shard:

```text
REGISTERING -> WARMING -> READY_ZERO_SHARD <-> SERVING
                                  \------------> DRAINING -> DRAINED
```

`SERVING -> READY_ZERO_SHARD` requires the same live boot to report
`process_ready=true`, `draining=false`, and `serving_shard_count=0` after a
newer assignment snapshot. This only restores target eligibility; the registry
is a liveness directory and cannot grant shard Authority.

`WARMING` has one exact initial flag contract:
`process_ready=false, draining=false, serving_shard_count=0`. Candidate
Register/Heartbeat validation requires the configured minimum remaining lease
horizon. Persisted-record validation checks bounded field structure and time
relationships without requiring `MinLeaseTTL` remaining, so a fresh holder in
its final renewal window remains readable and renewable; only `lease_until <=
now` is stale.

`ResourceSummary` key/value uses one deliberately narrow cross-runtime text
contract: non-empty printable ASCII bytes `0x20..0x7e`, no leading/trailing
ASCII space, with internal ASCII spaces allowed. Core capacity is the exact
canonical `RS1` count/length-framed wire length (including the empty `RS1:0:`
wire), not JSON escaping length. The Redis adapter calls the same core encoder.

`FindTarget` requires the exact domain, holder, boot, internal endpoint and TLS
DNS identity, a fresh logical lease, process readiness, READY_ZERO_SHARD or
SERVING, `draining=false`, and the configured capacity threshold. The package
does not import or expose an Authority mutation capability.

### Redis holder adapter

Owned implementation：

- `internal/storage/redis/routecontrol/holder_store.go`
- `internal/storage/redis/routecontrol/holder_scripts.go`
- `internal/storage/redis/routecontrol/holder_store_test.go`

Physical layout is one fixed hash-tag containing a v3 meta hash, a bounded sorted
index, an exact per-member accounting HASH, and one v3 fixed-field record hash
per `(domain, holder_id)`. All script
keys are checked for byte-identical hash tags. List uses bounded `ZRANGE`; no
`SCAN`, `KEYS`, or `SCRIPT FLUSH` path exists.

The v3 record hash has exactly 20 fields. `resource_summary` is no longer JSON:
it is the explicit `RS1` count/length-framed wire with byte-sorted keys. Both Go
and Lua parse it deterministically and reject leading/trailing bytes, duplicate
keys, non-increasing order, noncanonical lengths, and invalid bounded key/value
text; no unordered Lua table or `cjson.encode` is used as canonical evidence.
Resource key/value validation is byte-identical in Go and Lua and rejects every
non-ASCII byte, including `0xff`, even if `HD3`, `HR3`, integrity, accounting,
and meta totals are all recomputed consistently.
The persisted `record_digest` is independently recomputed as SHA-1 over the
fixed `HD3` semantic-field framing. `record_bytes` is the exact length of the
fixed `HR3` framing of every semantic field plus `record_digest`; Go and Lua
compute the same formula. `record_integrity` remains a non-authenticating
corruption checksum over every other persisted field, but no semantic decision
depends on that checksum alone. Lua candidate, reconcile, reply, and Go
load/reply paths verify the independent digest/resource/byte proofs.

Lua reconcile is two-pass: it validates every existing indexed hash (including
expired hashes), exact identity/score/lease binding, flags, times, positive
sequence, capacity and resource bounds before it classifies any stale member or
trusts aggregate counters. Candidate/aggregate/revision checks all complete
before stale deletion, so malformed/capacity/error returns perform zero writes.

The accounting HASH binds every exact index member to canonical
`record_bytes:resource_bytes`. Existing hashes must match it. An expired missing
hash is cleanable only while its sidecar remains, so meta pre-clean count/bytes
are still exactly provable; missing or forged accounting is MALFORMED. With or
without stale members, reconcile requires pre-clean meta count/record/resource
counters to equal the complete recomputation before ALREADY, REPLAY, cleanup,
replacement, or heartbeat. Cleanup removes index and accounting entries
atomically and rewrites exact live counters.

Go decoding accepts `resource_summary` only when its stored raw bytes are exactly
the canonical `RS1` wire; whitespace, duplicate/out-of-order keys, noncanonical
lengths, and trailing bytes are corrupt. Accounting uses the stored raw length,
never a re-encoded underestimate. Register/Heartbeat capture one preflight clock
value and reuse it for committed reply validation, so a post-commit fallback of
the injected clock cannot reject a valid commit or desynchronize sequence and
revision.

Every Go bounded-snapshot accounting addition now checks the individual record/
resource value against its configured per-value maximum, then proves the running
total is at most `configured_total_max - value` and at most the Lua safe-integer
remainder before adding. No `int` addition occurs before those proofs. Lua's
complete and live aggregate passes use the same pre-add configured-total and
safe-integer checks. A miniredis-backed real `HolderStore` fixture with 2049
expired missing records, maximum-valued accounting sidecars, and forged wrapped
meta counters is Corrupt in both the Go read path and Lua mutation reconcile;
Lua returns before stale cleanup or candidate/meta/index/accounting mutation.

The v3 meta hash has exactly five fields, including canonical decimal-string
`registry_revision`. Lua increments revision with string arithmetic up to
`uint64` max and fails closed at exhaustion; it never converts revision through
a Lua number. A committed Register or Heartbeat increments once. Exact
ALREADY/REPLAY does not increment, except an ALREADY invocation that actually
performs stale cleanup increments once for that cleanup.

Get/List share one complete bounded snapshot implementation, then validate exact `ZCARD ==
meta.holder_count <= MaxHolders` and accounting `HLEN == ZCARD` before any full
`ZRANGE`; the range bound is the proven cardinality and cannot truncate an
extra member. Both validate every indexed member's exact accounting entry,
record bytes, canonical resource bytes, score/lease identity, and the complete
meta aggregates before Get may return an otherwise valid target. They then
re-read only `registry_revision`. A changed revision retries the
whole attempt at most three times; exhaustion is explicit registry Unavailable,
not Corrupt. Exact `HLEN`, required-field existence and `HSTRLEN` checks precede
every production holder `HGETALL`. Jointly missing meta/index/accounting keys
represent an empty physical registry; independent disappearance, wrong types,
extra/missing fields, and stable counter/index/accounting mismatch fail closed.

After `TYPE` observes a meta hash, reads immediately capture its present revision
token before `HLEN`/`HSTRLEN`/`HGETALL`. If TTL removes meta/index/accounting during those
steps, the outer fence observes the token change and retries. A final jointly
missing meta/index/accounting snapshot is empty for List, but Get additionally
checks the target's generated exact record key: a residual target is Corrupt,
while a fully absent target is not found. Register's Lua preflight applies the
same exact-key residual fence. List deliberately does not use SCAN; externally
shortened/tampered TTL residuals are detected by target Get/Register, while the
configured natural TTL ordering prevents such residuals. Three continuously
changing attempts return Unavailable.

The independent holder Lua registry pins both script SHA values, reloads only
the exact missing SHA on `NOSCRIPT`, and retries the same mutation identity.
Mutation timeout re-entry preserves the same registration payload or heartbeat
sequence/digest. Register permits a different boot only after the prior
logical lease has expired. Heartbeat requires exact boot, strictly increasing
sequence and snapshot revision, bounded times, and a legal state transition.
Record, aggregate record, and aggregate resource byte budgets are fail-closed.
Physical TTL is cleanup; `LeaseUntil` is the logical target predicate. Go and
both Lua scripts reject unsafe TTL relationships before any write: record TTL
must strictly exceed `MaxLeaseTTL + ClockSkew` without safe-integer overflow,
and meta/index/accounting receive a strictly longer physical TTL than every
record. Thus natural expiry removes records before the registry proof keys.
Stale sweep derives the exact same-slot record key from every bounded index
member. An existing record is fully validated and must have
`ZSCORE == lease_until_ms` before it can be classified stale from its own lease.
A missing record permits index-only cleanup only when its score is expired.
Fresh missing records, score/record disagreement, and corrupt records return
MALFORMED without cleanup or partial writes.

`DeleteHolderForTest` and `ExistsHolderForTest` are the only cleanup seams and
operate on an exact generated record key.

### HolderAgent and HolderControl service

Owned implementation：

- `internal/holdercontrol/agent.go`
- `internal/holdercontrol/transport.go`
- `internal/holdercontrol/drain_controller.go`
- tests in `internal/holdercontrol/`

`HolderAgent` generates a boot ID with `crypto/rand` unless a test generator is
injected. It is called only after the external server is ready, registers
the exact zero-flag WARMING state, derives READY_ZERO_SHARD/SERVING/DRAINING from an injected
`ProcessStatusSource`, and sends single-flight periodic heartbeats with
monotonic sequences. Consecutive registry/status failures cross an explicit
threshold into sticky UNHEALTHY and cancel the loop; failure never emits
DRAINED or grants takeover/write authority.

Normal Close sends DRAINING, renews DRAINING with a new sequence/lease/status on
every bounded close-poll interval while shards remain, and sends DRAINED only
after the injected local serving-shard count reaches zero. Construction rejects
both `HeartbeatInterval + 2*HeartbeatTimeout >= LeaseTTL` and
`ClosePollInterval + 2*HeartbeatTimeout >= LeaseTTL` using subtraction-only,
overflow-safe strict-budget proofs. Equality is rejected and defaults remain
legal. A registry/status renewal failure is fail closed. Start and Close are
idempotent. The Agent does not start a Pump, membership composition, or
Authority runtime; it is a lifecycle port for RT2I.

Concurrent Start callers join one immutable attempt object containing
`{done, err}`. A later retry cannot reset or replace the result observed by a
waiter from the failed attempt it joined. Once a running agent becomes
UNHEALTHY, Close only cancels and waits for owned resources, returns the sticky
unhealthy error, keeps Status UNHEALTHY after closure, and never publishes
DRAINING or DRAINED even if Registry recovers.

Start-stage status/Register/initial-heartbeat RPCs retain the Start caller
context, but the running heartbeat loop uses an agent-owned background
lifecycle context canceled only by Close or sticky UNHEALTHY. Canceling a
successful Start caller therefore cannot stop periodic liveness.

Start validates that Register returns a legal record exactly equal to the full
registration candidate, including holder/boot, endpoint, TLS identity,
StartedAt, flags/times, capacity and resource summary. Every Register error or
response drift performs a background, heartbeat-timeout-bounded exact Get. A
durable full candidate is installed only as a cleanup baseline: Start still
fails, while the unique worker advances WARMING through DRAINING/DRAINED. A
definite absence returns the original error; a foreign, corrupt, or unreadable
result is sticky UNHEALTHY. During the post-initial-heartbeat Start/Close
handoff, one `mu` critical section decides whether Close has already won. If it
has, no run goroutine is created. Otherwise that same critical section creates
the run owner, sets `runStarted`/Started, and completes the immutable Start
attempt before unlock. Once Close can observe Started, `done` therefore already
has an owner. `startFinishHook` remains a deterministic test seam, but runs only
after ownership and attempt completion and cannot create a CloseTimeout gap.
`closeRequested` also gates the Started fast path, so a third Start cannot
report success while the owning Close is waiting.

Close now only requests closure and waits on a shared `closeDone`; a unique
background close worker owns cleanup independently of every caller context.
The first `startCloseWorkerLocked` call freezes one overflow-safe monotonic
`closeDeadline`; later Close calls cannot extend it. The worker constructs its
context from that exact deadline. Its single `CloseTimeout` includes waiting
for the active Start attempt, stopping the run loop, status reads, Heartbeat
RPCs, reconciliation Gets, polling, and final drain publication. Waiting for
`attempt.done` selects against that deadline. A Register implementation that
ignores its context therefore cannot hold the close worker, shared `closeDone`,
or joined Start waiters forever: timeout completes Close/`closeDone` with
`ErrAgentCloseTimeout`, completes joined Start waiters with `ErrAgentClosed`,
and preserves sticky fail-closed state. If a stubborn Register later
returns after committing, Start observes the completed close fence, returns
`ErrAgentClosed`, does not install/start the run loop, and leaves the WARMING
record to its safe logical/physical TTL; it never starts a second close worker.
The initial-heartbeat admission mutex is the explicit
linearization point: Close before admission skips READY_ZERO_SHARD; an already
admitted/committed heartbeat is installed as the local candidate baseline and
cleanup advances its next sequence. Initial, periodic, and Close heartbeat
errors or response drift are reconciled with the same bounded exact Get and the
correct agent-owned budget: non-closing reconciliation uses HeartbeatTimeout;
once Close is requested its effective deadline is exactly
`min(closeDeadline, now+HeartbeatTimeout)`. In particular, a periodic RPC which
committed before Close canceled `runCtx` no longer passes that canceled context
to Get. The worker waits for the run owner, then publishes from the latest exact
record installed by reconciliation.
Only a durable full candidate installs `N+1`; a proven previous record keeps
`N`, and foreign/missing/unreadable state becomes sticky UNHEALTHY so the agent
can never reuse `N+1` with a different payload. For initial Heartbeat only,
durable `expected` or exact previous WARMING is sequence-proven and does not pass
through the runtime consecutive-failure threshold: Start returns the original
anomaly while the unique worker advances the exact baseline through DRAINING to
DRAINED. A proven periodic `expected` or `previous` outcome also does not count
its cancellation anomaly once Close has requested ownership; normal running
anomalies still count through the configured threshold. Exact candidates are
installed atomically even after closing/closed wins, unless the shared deadline
has already expired, so Close sequences advance without reuse or STALE.

Every `ProcessStatus.Status`, `Registry.Heartbeat`, and reconciliation
`Registry.Get` is invoked through a one-shot buffered-result wrapper. The Agent
caller selects the operation against its bounded context, and a result accepted
after the shared deadline cannot mutate Agent state. The buffer lets a late
dependency return without blocking on a receiver. Go cannot kill a malicious
dependency goroutine which never returns; that internal goroutine may remain,
but the Agent run/Close owner and `closeDone` complete within the shared budget,
no call is issued after deadline, and a timeout is sticky UNHEALTHY plus
`ErrAgentCloseTimeout` without claiming DRAINED. The production Redis
`HolderStore` remains context-compliant; this shield is for adversarial port
implementations and shutdown ownership.

`DialHolderControl` is the only production client constructor. It accepts only
PEM certificate/key/CA material and an explicit strict-canonical DNS
`ServerName` exactly equal to `ExpectedServerIdentity`, builds TLS 1.3 mTLS
credentials, applies bounded dial/RPC and message-size budgets, and owns
connection close. `HolderPeer.Client` is the concrete `*HolderControlClient`;
production drain construction cannot accept an interface or stub. The client
also exposes `CloseContext`; a timeout only bounds the caller's wait while the
client remains permanently closed and the final connection close continues.

The HolderControl package does not export a generic service-registration
factory, registration alias, or `ServiceRegistration` type. The five exact
HolderControl full methods are exposed by `HolderControlFullMethodNames` and
`HolderControlMethodACL`. `LocalService` keeps per-method authorizer checks as
defense in depth and does not trust payload or metadata roles.

### RemoteDrainController

`RemoteDrainController` implements `authority.DrainController`:

```go
var _ authority.DrainController = (*RemoteDrainController)(nil)
```

The constructor freezes source/target holder, boot, TLS identity, domain, and
shard. Manager callback domain/shard arguments are assertions and cannot
silently replace the target. Source calls are exactly Stop, Wait, Facts;
target `ColdLoadNewHolder` sends only the exact Prepare generation. Every RPC
has a bounded timeout and complete response validation: exact holder/boot,
domain/shard, known enum, phase/flags, freshness, and generation binding.
Stop accepts only the explicit STOPPED/WAITED/FACTS successor set; Wait accepts
WAITED/FACTS; Facts accepts FACTS; Prepare accepts OPEN plus the exact fresh
target generation. Optional `GetStatus` verification is role-aware. The
controller never treats HolderRegistry as Authority and never scans or
bulk-loads actor state.

## RT2I required wrapper injection

RT2I remains the only composition writer. It must:

1. Construct the Redis holder store and `routecontrol.HolderRegistry`.
2. Build a `ProcessStatusSource` wrapper from the already-running local
   runtime: process readiness, serving-shard count, snapshot revision,
   capacity weight, resource summary, and draining state.
3. Start the internal HolderControl gRPC listener/server first, then call
   `HolderAgent.Start`.
4. Construct `LocalService` with the runtime's exact authorizer and immutable
   local holder ID, boot ID, clock, and operation timeout. Pass an inline
   generated registration callback to C's sealed
   `transport.NewMultiServiceGRPCServer`:

   ```go
   func(registrar grpc.ServiceRegistrar) {
       holdercontrolpb.RegisterHolderControlServiceServer(registrar, localService)
   }
   ```

5. Merge the five HolderControl ACL entries with the exact RouteSnapshot ACL;
   do not expose the raw registrar after construction.
6. Construct `RemoteDrainController` with fixed source/target peers and pass it
   to the existing RouteAssignmentManager. Do not derive a target from manager
   callback arguments.
7. Call `HolderAgent.Close` before final server shutdown and preserve the
   DRAINING-on-timeout fail-closed behavior for healthy shutdown. A sticky
   UNHEALTHY agent must remain UNHEALTHY and publish no shutdown heartbeat.

This official OpenAI `gpt-5.6-sol`/`xhigh` exact6 closure does not change the
protocol surface. Its write set is exactly HolderAgent, its tests, and this
handoff; no Registry test seam was required. No HolderRegistry/Redis adapter,
executor, player/farm registry, runtime, transport, service, proto/generated
file, drain controller, manager, command, deploy, or `go.mod` file changed. No
nested Agent/Codex process was started, so the controlled-PATH and
explicit-`openai` nested invocation constraint was not activated.

## Verification

Passed in this worktree：

- `go test ./internal/routecontrol/... ./internal/storage/redis/routecontrol/... ./internal/holdercontrol/... -count=20`
- `go test -race ./internal/routecontrol/... ./internal/storage/redis/routecontrol/... ./internal/holdercontrol/... -count=3`
- `go test ./... -count=1`
- `go vet ./internal/routecontrol/... ./internal/storage/redis/routecontrol/... ./internal/holdercontrol/...`
- `go build -buildvcs=false ./...`
- `git diff --check`
- focused OWNED/forbidden/secret scan: the exact6 diff contains only
  `internal/holdercontrol/agent.go`, `internal/holdercontrol/agent_test.go`, and
  this handoff; no Registry/Redis, runtime, transport, service, proto, drain,
  manager, command, or `go.mod` diff; no added credential/private-key signature
  and no production `SCAN`, `KEYS`, or `SCRIPT FLUSH` path

Lua received a final manual order audit after the TTL/resource changes. It
verified the TTL relationship before Redis access, candidate validation before
state mutation, complete existing/meta/accounting validation, and capacity/
revision overflow before the first `remove_stale`, `HSET`, or `ZADD`, plus exact
ALREADY/REPLAY reconcile and missing-record member validity.

Coverage includes state transition and target filtering, input/map cloning,
Redis same-slot and script SHA, strict field/type/corruption handling, byte
capacity, logical/physical expiry, boot replacement, stale/replay heartbeat,
exact score/lease stale cleanup, missing-record fresh/stale split, decimal
revision above `2^53` and exhaustion, ALREADY/REPLAY revision stability,
revision-fenced Get/List retry and Unavailable exhaustion, exact HLEN/field
existence before bounded reads, near-expiry renewal, three-layer WARMING zero
flags, attempt-scoped Start result, sticky runtime UNHEALTHY Close, concurrent
registration, race, TLS identity/root/cert/message bounds, exact ACL list, and
fixed-peer drain/generation checks. This round additionally covers canonical
RS1-vs-JSON capacity, printable-ASCII boundaries, fully rederived `0xff`
corruption, unsafe Go/Lua TTL relations, natural physical TTL ordering, target
residual detection, and unrelated accounting drift on Get.

This exact closure additionally covers expired-record wrong digest, forged exact
`record_bytes`, and `RS1` whitespace/duplicate/order/value drift after
recomputing `record_integrity`, followed by a second mutation with exact
record/index/accounting/meta/revision no-write assertions. It also covers
`sequence=0001` and invalid persisted REGISTERING/READY/SERVING after recomputing
digest/bytes/integrity; direct-Lua `snapshot_revision=01` and wrong schema with
zero created keys; stable over-limit index cardinality before range; exact
pre-clean counters for full and missing stale records; forged meta bytes on
ALREADY; runtime response-drift thresholding; initial drift without duplicate
counting; and a blocking Register where the Close caller times out but the
single background owner later emits DRAINING/DRAINED with no READY_ZERO_SHARD.
Agent coverage now also includes Register commit-then-cancel/response drift with
durable final-state assertions, foreign/unreadable reconciliation, initial/
periodic/Close heartbeat commit-then-cancel sequencing, runtime nil-error drift
thresholding and Close drift suppression, Start-caller cancellation after
success, and DRAINING renewals across multiple lease periods before final
DRAINED.

This exact5 closure adds strict-equality/over/overflow lease-budget tables and
proves defaults remain legal; threshold-one initial Heartbeat expected/previous/
unresolved outcomes (including a canceled Start parent); a stubborn Register
that is never released while the worker, shared `closeDone`, and joined-attempt
completion remain bounded; a late committed Register that cannot reopen run or
close ownership; a Get that blocks until its inherited Close deadline while the
whole Close remains within `CloseTimeout` plus scheduler margin; and the 2049
maximum-sidecar forged-meta Go/Lua corruption fixture described above.

This exact6 closure adds deterministic READY, SERVING, and durable-previous
periodic Heartbeat/Close races at failure threshold one. Each RPC is ambiguous
only after Close cancels `runCtx`; reconciliation Get is held briefly, observes
the exact shared `closeDeadline`, then releases and proves contiguous sequences
through DRAINING/DRAINED without STALE or failure accumulation. A handoff hook
is held across the complete Close call and proves Started already has a run
owner and completed Start barrier. Separate Status, Heartbeat, and Get fakes
ignore context beyond `CloseTimeout`: Close and `closeDone` still complete with
the same sticky timeout, repeated Close neither extends the deadline nor
double-closes, late results do not mutate Agent state, and no DRAINED claim is
made. Companion cases release every interface inside the budget and finish at
durable DRAINED. The deadline also has a maximum-duration, initialize-once
overflow regression.

## Follow-up Q evidence (not a unit gate)

The unit gate deliberately uses miniredis through the real `HolderStore`; it
does not depend on an externally managed Redis Cluster. The subsequent Q owner
must capture evidence from a disposable, isolated real Redis Cluster namespace:

1. Record Redis version/topology, exact hash tag, loaded register/heartbeat
   script SHAs, and the pre-test empty namespace.
2. Seed the same 2049 expired index members, maximum accounting sidecars, and
   forged wrapped meta counters; invoke both a bounded read and one mutation.
3. Capture Go Corrupt mapping plus Lua `MALFORMED`, and byte/exact before/after
   meta, revision, `ZCARD`, `HLEN`, candidate-key absence, and no stale cleanup.
4. Run a Close deadline fault with Registry Get honoring cancellation and retain
   elapsed/deadline evidence showing it cannot outlive `CloseTimeout` except for
   a documented small scheduler margin.
5. Remove only that exact isolated namespace and attach the command transcript;
   do not promote an external Cluster dependency into `go test`.

## Explicitly not implemented in RT2D

- Route Controller Plan/PlanExecutor persistence and restart recovery.
- `routectl` status/move/drain/takeover/verify commands.
- Controller HA or leader election.
- Automatic failure discovery.
- Autoscale/scale-in/scale-out policy and platform adapters.
- RT2I composition and deployment wiring.

Those remain the next owned seams and must not be inferred from HolderRegistry
readiness or holder liveness.
