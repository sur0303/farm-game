# CP-ROUTE-DYNAMIC-TRANSPORT — Handoff

Date: 2026-08-09
Branch: `checkpoint/route-dynamic-transport-20260809`
Parent checkpoint: `c09e3b99b38590272bdaa5940f45c7c61179db7c`
RT2C original closure head: `5d143e5d58edbf48146d2307fa79ef40ee5cc1d0`
RT2C Holder-auth dependency head: `672114e3f5568155546bd0e9c2583fb039921e22`
Checkpoint state before this handoff update: `47b0081abc11c5873e15a0bae5009fd2121cc3fe`

## Frozen C interface

- Internal-only RouteSnapshot gRPC service: `GetSnapshot`, full-envelope `GetCurrentRevision`, and server-streaming full-envelope `WatchFrom`.
- Strict complete envelope fields and validation: schema, realm, revision, canonical JSON, checksum, topology, committed time, controller ID and boot.
- `SnapshotWatchHub` reconciles durable Store state without holding its in-memory mutex during Redis reads, rejects durable disappearance/rollback/same-revision conflict/topology drift, and performs bounded periodic compensation after committed-notify failure.
- Higher complete revision jumps are accepted without duplicating the generation state machine; RT2B Pump remains the final per-shard generation validator.
- Production `RemoteSnapshotSource` is constructible only through TLS 1.3 `Dial`; it requires explicit DNS server identity, expected controller ID, mTLS roots/certificate, pre-decode gRPC message limits, non-ambiguous peer SANs, and sticky terminal envelope/identity failures.
- Server allowlist is mandatory and exact. Empty durable Snapshot makes the route-controller shell fail startup. The role remains read-only, single-controller and non-HA; no mutation, Holder, Plan, routectl or composition RPC exists yet.
- `VerifiedPeerIdentity` exposes only the existing completed, verified TLS peer identity path and enforces one raw DNS or URI SAN; unknown, duplicate, mixed, malformed, CN, IP and email identities fail closed.
- `MethodAuthorizer` is an immutable exact `fullMethod -> peer identities` ACL. The sealed multi-service constructor requires complete per-method ACLs, exact equality for the three Snapshot method identities, construction-time service registration, valid immutable descriptors and transport-owned authorization wrappers. It never exposes the underlying `grpc.Server` or `grpc.ServiceRegistrar`.
- Future RT2D Holder registration must use the sealed multi-service constructor. The standalone `NewGRPCServer` remains Snapshot-only and is not a Holder composition seam.
- Watch fanout shares immutable internal revision pointers and clones at consumption; queues, clients, messages and sends are bounded. Close cancels active stream contexts and reports connection-close errors.

RT2D must consume these frozen transport/identity contracts and must not edit or weaken RT2C files. Holder control may add its own internal service/proto in its owned package, but it cannot trust payload caller roles or turn Registry state into write authority.

## Review and gates

Independent official OpenAI Codex `gpt-5.6-luna`, reasoning `max`, reported exact-head closure at both the original `5d143e5` transport head and the final Holder-auth dependency head `672114e3`:

```text
NO FINDINGS / NO BLOCKING FINDINGS
```

The auth seam was returned to the original RT2C Writer after RT2D ICR found the missing reusable verified-peer/method-ACL boundary. Review rounds closed raw-SAN ambiguity, unsafe optional multi-service registration, ACL intersection, custom handler bypass, mutable descriptors, retained registrar races, registration panic, malformed DER and typed-nil/metadata edges. RT2C still contains no Holder business implementation.

I0 gates on the feature head and this combined checkpoint:

```text
go test -count=20 ./internal/routecontrol/transport                                      PASS
go test -race ./internal/routecontrol/transport                                           PASS
go test ./...                                                                              PASS
go vet ./...                                                                                PASS
go build ./cmd/... (with malformed parent workspace `.git` metadata isolated)             PASS
protoc isolated regeneration diff (original C closure; no later proto changes)             PASS
git diff --check                                                                           PASS
worktree status                                                                            CLEAN
```

The commit containing this handoff is the exact checkpoint base for RT2D. No integration merge has occurred. Final Premerge still starts from the then-latest remote integration and merges frozen heads exactly once. Autoscaling remains blocked.
