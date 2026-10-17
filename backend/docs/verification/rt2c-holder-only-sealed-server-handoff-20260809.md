# RT2C Holder-only sealed gRPC server handoff

- Base: `4895ee613a29264e71b8e479d4821d136f099e85`
- Branch: `codex/rt2c-holder-only-sealed-server-20260809`
- Scope: `internal/routecontrol/transport/server.go`, same-package transport
  tests, and this handoff only
- State: implementation and verification complete; the final SSH-signed
  commit SHA is reported by the Writer, and no push was performed

## Dependency seam

`NewHolderOnlySealedGRPCServer` builds a sealed lifecycle-only server without a
`RouteSnapshotServer` dependency:

```go
sealed, err := transport.NewHolderOnlySealedGRPCServer(
    tlsConfig,
    transport.GRPCServerOptions{MethodAuthorizer: authorizer},
    func(registrar grpc.ServiceRegistrar) {
        holdercontrolpb.RegisterHolderControlServiceServer(registrar, localService)
    },
)
```

The callback is construction-only. The collector validates and copies every
descriptor, requires every registered unary/stream method to have an exact
non-empty ACL entry, wraps every handler with verified-peer authorization, and
rejects ACL entries that do not correspond to a sealed registered method. The
sealed result exposes only `Serve`, `GracefulStop`, `Stop`, and
`GetServiceInfo`; it is not a `grpc.ServiceRegistrar` and never exposes the
underlying `grpc.Server`.

RT2I owns the generated Holder registration callback and composition root. It
must keep the callback inline as above, provide the exact Holder method ACL
set, and retain the raw registrar only inside construction. Use the existing
`NewMultiServiceGRPCServer` when RouteSnapshot and Holder services must share
one server; its signature and Snapshot ACL contract are unchanged. This seam
does not add Holder proto/runtime/cmd/deploy code or a registration factory.

## Verification

The following gates passed on the implementation head:

```text
GOCACHE=/tmp/rt2c-holder-count-cache go test -count=20 ./internal/routecontrol/transport
GOCACHE=/tmp/rt2c-holder-race-cache go test -race ./internal/routecontrol/transport
GOCACHE=/tmp/rt2c-holder-full-test-cache go test -buildvcs=false -count=1 -timeout=600s ./...
GOCACHE=/tmp/rt2c-holder-vet-cache GOFLAGS=-buildvcs=false go vet ./...
GOCACHE=/tmp/rt2c-holder-build-cache go build -buildvcs=false ./...
git diff --check
```

Coverage includes successful service information, TLS 1.3/mTLS validation,
nil authorizer/registration inputs, empty callbacks, missing and extra ACL
methods, immutable descriptor snapshots, no-op/panicking/typed-nil/duplicate
registrations, retained registrar concurrent late calls, forced handler
authorization, and unknown-method fail-closed behavior.
