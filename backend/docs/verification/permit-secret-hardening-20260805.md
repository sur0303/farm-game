# Permit secret hardening verification — 2026-08-05

## Outcome

- FarmGate and farm-svr call the same `internal/permitsecret` startup validator before opening Redis-backed services, listeners, health endpoints, or readiness endpoints.
- With `ACTOR_AUTHORITY_MODE=authority`, `PERMIT_SECRET` is required, must contain at least 32 bytes, must not be whitespace-padded, and must not equal a repository-known development literal. The insecure development switch is rejected in Authority mode.
- With no actor mode selected, secret validation is production-safe by default. Insecure material is accepted only with `PERMIT_SECRET_ALLOW_INSECURE_DEVELOPMENT=1`. Explicit `legacy`/`rollback` mode is the other local-only escape path.
- Both processes log only the selected policy and boolean development state. No secret or secret digest is logged.
- The authentication constructor no longer injects a signing default. The in-memory SessionAuthority generates an instance-local random key when used without one; Redis SessionAuthority rejects omission. FarmGate production composition always supplies the startup-validated shared key.
- HTTP Cookie signing accepts a valid explicit versioned KeyRing, or derives its purpose-separated key from the already validated permit key. Explicit Cookie keys receive the same minimum-length and known-literal checks. Local explicit legacy/development composition retains derivation for demos; production cannot derive from a known default.
- Core Compose requires one shared `PERMIT_SECRET` interpolation for farm-svr and FarmGate. The Kubernetes Gate example loads it from a Secret. Local Compose explicitly declares legacy mode, and local multi-process scripts generate one per-run key unless the caller provides one.

## Startup/config coverage

`internal/permitsecret/config_test.go` covers:

- missing secret rejection;
- short secret rejection;
- known public and long development literal rejection;
- valid Authority material acceptance;
- explicit insecure-development override acceptance outside Authority mode;
- Authority rejection of that override;
- explicit legacy policy visibility;
- signature rejection when Gate and backend verifier keys differ.

`internal/gateway/auth/http_session_test.go` additionally verifies that even a valid explicit Cookie KeyRing cannot cause the auth constructor to inject a missing permit key. Existing Cookie KeyRing rotation, minimum-length, active-version, and derived-key tests remain enabled.

## Verification evidence

Executed from the assigned worktree on 2026-08-05:

```text
go test ./internal/permitsecret ./internal/gateway/session ./internal/gateway/auth ./internal/gatewayapp ./internal/friend ./internal/farm/ingress ./cmd/farm-svr ./internal/gateway/command
PASS

go test ./...
PASS

go test -race ./internal/...
PASS

go vet ./...
PASS

for script in scripts/compose-core-config.sh scripts/test-lease-renew-stress scripts/test-dualgate-kick scripts/test-multigate-drain scripts/test-stage5-fault-evidence; do bash -n "$script"; done
PASS

docker compose -f deploy/docker-compose.yml config --quiet
PASS with non-secret validation-only environment inputs

git diff --check
PASS
```

No external Redis, Kubernetes cluster, or deployed readiness endpoint was needed for this configuration/startup hardening change. No secret values or derived fingerprints were captured in this record.
