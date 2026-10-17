# R1-Q Hardening Verification — Wave1 composite entry

This is the Wave1 tool contract for `R1-Q-HARDENING-VERIFICATION`. It prepares
composition and structured evidence; it does not authorize or report a final
PASS. The entry is intentionally fail-closed:

- Backend's frozen base must be `ba6f3aee4f0fcd941d9fa37135af0ce09ffdfa5c`;
  the clean R1 tool commit may be a descendant whose diff stays within the
  owned test-tool/docs paths.
- The read-only H5 add-dir must be exactly
  `b83cae607245391df6e2d605cffba85eb194cb09`.
- Candidate dirty state is recorded before any selected tool is run. A mismatch
  or dirty candidate leaves the top-level verdict as `FAIL`.
- A loopback URL is classified as `localhost`, never as staging. Without a
  non-loopback official URL, `handoff` is `BLOCKED`.
- `A1`, `A3` and `A4` are emitted as `SKIPPED_NOT_COMPOSED`; historical handoff
  documents do not become a current R1 conclusion.
- The historical `cmd/farm-scenario` Scenario Coordinator is checked as a
  reference only. It is absent from frozen Backend
  `ba6f3aee4f0fcd941d9fa37135af0ce09ffdfa5c` and is outside
  this writer's owned `cmd/*probe*` / `cmd/*load*` seams, so Wave1 does not copy
  or import it.
- Social is not inferred. A final SCB entry can be supplied as a manifest, but
  the coordinator records configuration only and does not conclude merge or
  outcome from it.

## Invocation

The default mode is preparation and writes to a unique directory under `/tmp`:

```bash
./scripts/r1-q-hardening-verification
```

The command exits `2` for a valid Wave1 preparation whose handoff is blocked or
whose final gates are not run, `1` for invalid assembly or an executed command
failure, and never returns `0` for a final PASS. A reproducible run can pin its
output and run id:

```bash
./scripts/r1-q-hardening-verification \
  --run-id r1q-wave1-20260809-a \
  --out /tmp/r1q-wave1-20260809-a \
  --h5-root ../classic-farm-h5
```

Optional `--run wss-200` and `--run lease-200` execute the existing local/test
profile tools and preserve their logs under the bundle. Their success is
reported as `OBSERVED_LOCAL_ONLY`, never as staging PASS. `--run cp-b` remains
blocked until a Social SCB entry manifest is supplied; supplying a manifest
still does not infer an outcome.

`--metrics-url URL` invokes `cmd/r1-m0-probe` with GET only. It records response
bytes, SHA-256 and selected low-cardinality Prometheus samples. It does not
write to Redis, invoke `/dev/session`, create a fixture, or send credentials.
`--execute-dev-session-check` only performs a read-only GET of `/dev/session`.

## Bundle schema

Each run writes the following files. Paths in the bundle are relative to the
output directory.

### `run-manifest.json`

Schema: `farm.r1-q.run-manifest/v1`.

```json
{
  "schema": "farm.r1-q.run-manifest/v1",
  "wave": "Wave1",
  "backend": {
    "commit": "<candidate sha>",
    "expected_commit": "ba6f3aee4f0fcd941d9fa37135af0ce09ffdfa5c",
    "base_is_ancestor": true,
    "dirty": false,
    "valid": true
  },
  "h5": {
    "commit": "<observed sha>",
    "expected_commit": "b83cae607245391df6e2d605cffba85eb194cb09",
    "dirty": false,
    "valid": true
  },
  "environment": {
    "showcase_auth": "real_register_login",
    "dev_session": "forbidden",
    "mock_auth": false,
    "localhost_is_not_staging": true
  },
  "actor_layout_v1_baseline": {
    "source_path": "deploy/actor-route.static.example.json",
    "runtime_evidence": false,
    "copied_fixture": false
  }
}
```

`actor-layout-v1-baseline.json` is a reference record containing the source
path, source commit and digest. The route example is not copied into a second
fixture and is not treated as live topology evidence.

### `verdict.json`

Schema: `farm.r1-q.verdict/v1`.

```json
{
  "verdict": "FAIL | INCONCLUSIVE",
  "handoff": "BLOCKED | READY_FOR_OPERATOR_REVIEW",
  "invalid_assembly": false,
  "final_pass_claim_allowed": false,
  "gates": [
    {
      "id": "unassembled.A1",
      "status": "SKIPPED_NOT_COMPOSED",
      "final_pass_eligible": false
    }
  ]
}
```

The schema deliberately has no Wave1 `PASS` path. An invalid commit, dirty
candidate, missing required formal tool, or executed command failure remains a
hard `FAIL`; lack of an official URL and unexecuted gates produce a blocked or
inconclusive handoff.

### `metrics-summary.json`

Schema: `farm.r1-q.metrics-summary/v1`. This is an M0 read-only report with
`http_method: GET`, `response_bytes`, `response_sha256`, endpoint class and
selected metric summaries. Loopback observations are `OBSERVED_LOCAL_ONLY`.

### `lease-batching-evidence.json`

Schema: `farm.r1-q.lease-batching-evidence/v1`. It reserves explicit fields for:

```text
100,000 simulated Sessions and Subscriptions
batch item count / bytes / duration
holder isolation
stale subscription does not resurrect
drain and cleanup
200 real subscriptions (1 owner + 199 viewers)
```

The frozen baseline has the formal 200-subscription tool but does not have one
formal 100k simulator or a batch-byte witness. Those fields therefore remain
`SKIPPED_NOT_COMPOSED` or `NOT_INSTRUMENTED`; the 200-client tool is referenced,
not reimplemented.

### `cp-mvp-hardening-r1-evidence.md`

This human-readable index repeats the gate statuses and explicitly states that
the bundle is Wave1 preparation only. It is generated in the selected output
directory, so the verification run does not dirty production source or H5
business files.

## Existing tool composition

The entry references, rather than duplicates, these formal tools:

| Scope | Existing tool |
|---|---|
| Historical Scenario Coordinator reference | `cmd/farm-scenario` (observed only; absent from frozen Backend) |
| CP-B real H5 register/login and browser witness | `scripts/run-cp-vertical-b-smoke` |
| 200-WSS baseline and slow reader | `scripts/run-theft-200-wss-hot-farm` + `cmd/ws-stress` + `cmd/theft-hot-audit` |
| 200 real subscription batch renew | `scripts/test-lease-renew-stress` + `cmd/lease-stress` |
| Downlink/fault proxy | `cmd/gate-vertical-e2e` + `scripts/run-authority-fault-smoke` |
| Actor layout reference | `deploy/actor-route.static.example.json` |

The local/test profile may use fixture and fault tools. Showcase evidence must
use real register/login, have `/dev/session` unavailable, and have no Mock auth
result. Final staging URL, SCB entry and the later 10/10 gate are operator
inputs for a subsequent wave.
