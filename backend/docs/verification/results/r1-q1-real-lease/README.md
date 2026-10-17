# R1-Q1 real lease evidence

Formal entry:

```bash
./scripts/test-lease-renew-stress
```

The entry requires branch `codex/r1-q1-real-lease-evidence-20260809`, verifies
that R1-4 commit `1ec3dcd166a7449b40aa934ee7324154474b3c05` is an ancestor and that
all changes above it are test/evidence-only, allocates conflict-checked dynamic
loopback ports, and writes a new immutable run directory here. It uses the real
Redis Authority store, composed membership, one real Gate, one real Player
holder, two real Farm holders, and two isolated typed test proxies. It never
uses a container socket or a global Redis clear.

No retained run is a PASS in this workspace. The canonical attempted run is
`20260809T121750Z-2`: its manifest records the exact R1-4 source and a redacted
Redis endpoint digest, but the sandbox rejected the first loopback socket with
`EPERM`. Both rounds are therefore `INCONCLUSIVE`. Earlier directories are
retained fail-closed development attempts and are not candidates for PASS.

On a Controller host with loopback sockets and the configured Redis endpoint,
the same entry writes raw Gate/Farm Prometheus snapshots, per-batch Holder
events, service logs with Redis data redacted, `round-a.json`, `round-b.json`,
`metrics-summary.json`, and `verdict.json`. Only a `PASS/PASS` pair produces an
overall PASS.
