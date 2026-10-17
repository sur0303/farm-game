# RT2Q Route-Control Evidence Schema

`schema`: `rt2q-route-control-evidence/v1`

This document describes the test/evidence scaffold only. It is not a route
assignment contract and it does not authorize a production rollout or a
business verdict.

## 1. Evidence Levels

| Level | Direct source | Allowed meaning |
| --- | --- | --- |
| `E1` | `local-self-check` | The standard-library harness validated its own recording, manifest, cleanup, redaction, and registration paths. |
| `E2` | `controlled-fixture` | A future deterministic fixture may record observations. This scaffold does not provide an external scenario runner. |
| `E3` | `real-process` | A future real-process run with a real, verified attestation. A fixture or self-check cannot be relabeled as E3. |
| `E4` | `external-attestation` | A future externally attested run with a real, verified attestation. No E4 oracle is implemented here. |

Every manifest and timeline record carries a complete `provenance` object:

```json
{
  "task_id": "RT2Q0-ROUTE-CONTROL-HARNESS",
  "run_id": "opaque-run-id",
  "base_commit": "40-hex-commit",
  "head_commit": "40-hex-commit",
  "branch": "writer-branch",
  "worktree": "/absolute/worktree",
  "collector": "collector-name",
  "source_kind": "local-self-check",
  "declared_level": "E1",
  "recorded_at": "2026-08-08T00:00:00Z",
  "synthetic": false
}
```

The declared level must equal `provenance.declared_level`, and the source kind
must be capable of directly producing that level. Lower-level input cannot be
promoted by changing a label. Seven-character odd-length hexadecimal commit
prefixes are valid, but all-zero, unknown, and invented commits are rejected
for real provenance. Real CLI provenance resolves Git with fixed direct
arguments, binds `head_commit` to the current Git HEAD, and verifies the base
commit object. A synthetic record uses the explicit `synthetic-e1` commit
marker and is restricted to E1 self-checks.

## 2. Manifest

`run-manifest.json` is a strict JSON object. Unknown fields are rejected when
read. Required fields are:

| Field | Requirement |
| --- | --- |
| `schema` | Exactly `rt2q-route-control-evidence/v1`. |
| `task_id`, `run_id`, `branch` | Non-empty safe tokens, exactly bound to the nested provenance. |
| `base_commit`, `head_commit` | 7-64 hexadecimal characters, exactly bound to the nested provenance; synthetic E1 uses `synthetic-e1`. |
| `worktree` | Non-empty provenance string. |
| `evidence_level` | One of `E1`, `E2`, `E3`, `E4`. |
| `status` | `PASS`, `FAIL`, or `INCONCLUSIVE` for the evidence record only. |
| `reason` | Required human-readable evidence limitation or scope. |
| `provenance` | Complete provenance object described above. |
| `artifacts` | At least one artifact with an exact digest. |

Top-level `task_id`, `run_id`, `base_commit`, `head_commit`, `branch`, and
`worktree` must exactly equal their nested provenance fields. `WriteManifest`
and `ReadManifest` both require an artifact root; write re-hashes all artifacts
before creating output, and read re-hashes all artifacts before accepting input.
The manifest output is exclusive and atomic: an existing file or symlink is
rejected and never overwritten. Artifact `name` and `path` are binding metadata,
not redactable free text: if a supplied redactor would change either field,
write rejects the manifest before hashing or output. `Manifest.Redacted` deep
copies the artifact slice and never mutates the caller's backing array.

Each artifact has this shape:

```json
{
  "name": "timeline.ndjson",
  "path": "timeline/revision.ndjson",
  "bytes": 123,
  "sha256": "64-lowercase-hex-characters"
}
```

Artifact paths are relative to the declared artifact root, use slash
separators, and cannot escape that root. Symlink components, resolved paths
outside the root, directories, and other non-regular files are rejected.
`ValidateArtifacts` re-hashes every file and rejects any content or byte-count
drift. On Linux, reads are opened relative to the root descriptor with
`openat2` `RESOLVE_BENEATH|RESOLVE_NO_SYMLINKS|RESOLVE_NO_MAGICLINKS|RESOLVE_NO_XDEV`,
checked with `fstat`, and hashed from that same opened descriptor; FIFO input is
opened non-blocking and rejected. Strong durable filesystem evidence is
Linux-only. Windows, Plan 9, js, and WASI fail closed with
`ErrDurableEvidenceUnavailable`/`ErrCapability`; no name-based rollback or
portable root implementation is used on those targets.

### Attestation gate

`E3` and `E4` cannot be `PASS` without an `attestation` object that has:

```json
{
  "id": "attestation-id",
  "issuer": "independent-issuer",
  "level": "E4",
  "artifact_sha256": "64-lowercase-hex-characters",
  "real": true,
  "verified": true
}
```

Without a real and verified attestation, the only valid high-level status is
`INCONCLUSIVE`. The current Q0 scaffold has no trusted verifier capability:
E3/E4 `CanPass` is permanently false, and even a caller-supplied
`real=true`/`verified=true` attestation cannot produce `PASS`. A future trusted
verifier and independent oracle must observe the relevant system and publish
its own evidence.

## 3. Independent NDJSON Timelines

The timeline writer creates a new private run directory and four independent
files. The target run directory must not already exist, must not be a symlink,
and must have a non-symlink parent. The run-directory capability and all four
file capabilities remain held until finalization. Each file has its own
sequence starting at `1`:

```text
revision.ndjson
authority.ndjson
holder.ndjson
fault.ndjson
```

Every line contains:

```json
{
  "schema": "rt2q-route-control-evidence/v1",
  "stream": "revision",
  "sequence": 1,
  "recorded_at": "2026-08-08T00:00:00Z",
  "provenance": {},
  "observation": {}
}
```

The observation payloads are intentionally observational:

- `revision`: domain, shard, revision, source.
- `authority`: operation, transition identity, observed state, revision.
- `holder`: holder/boot identity, observed state, writer epoch, lease epoch.
- `fault`: name, action, phase, and optional note.

Concurrent creators targeting the same run directory have one winner and one
`run output already exists` failure; no old run is appended and no duplicate
sequence or mixed file handles are possible. Fault phases such as `before-send`, `in-flight`, `after-apply`,
`response-lost`, `recovered`, and `shutdown` are recorded only. The fault
record has no verdict field and the scaffold does not decide whether a fault
proves correctness.

`Timeline.FinalizeManifest` is the one-shot atomic finalization API. It rejects
caller-supplied artifacts, syncs and seeks each held original timeline file,
hashes that descriptor, writes `run-manifest.json` through the same held
run-root, and closes every file/root capability after installation or error.
It therefore remains bound to the original run directory if the visible path
is renamed and recreated before finalization. The CLI uses this API and stores
the manifest inside the timeline run directory.

## 4. Exact-Key Cleanup

The registry accepts an exact-key handle containing only:

- a SHA-256 digest of the key;
- a caller-owned `ExactKeyCleanup` implementation.

The raw key is not stored in the registry, manifest, or timeline. The caller's
adapter may keep the raw key privately to address its backend.

Cleanup is strictly:

```text
for every explicitly registered digest:
    caller.DeleteExact()
    caller.ExistsExact()
    reject residue when ExistsExact() is true
```

The interface has no key discovery or global cleanup operation. It must not be
implemented with `KEYS`, `SCAN`, `SCRIPT FLUSH`, or any equivalent broad
operation. Cleanup atomically enters `closing`, rejects concurrent
`Register`/`Cleanup`, attempts every registered key, and joins individual
errors. A failed cleanup returns to `open` for retry; a successful cleanup
enters `closed` and later registration is rejected.

## 5. Redaction

`Redactor` removes caller-provided secret values, JSON-quoted and unquoted
password/token/auth/API-key values, `Authorization` and `X-API-Key` headers,
URL credentials, and endpoint hosts with or without a URL scheme or port.
It covers `Authorization=Bearer secret`, colon/equals headers, JSON and
single/double-quoted header variants, URL query values, and sensitive
key/value forms inside URL fragments while preserving non-sensitive fragments.
Timeline provenance, observations, manifest reason, and manifest provenance are
redacted; artifact `Name`/`Path` are rejected when redaction would alter them.
Redaction is idempotent. Callers must still avoid
passing raw key material as an observation field; use `KeyDigest` for exact-key
identity.

## 6. Generic Test Process

`StartProcess` wraps `os/exec` without importing production packages. On Linux
the child starts in its own process group with a held leader pidfd. Cleanup
enumerates the original group, rechecks session/group/start-time identity, and
signals members through pidfds rather than an unbound negative PID; leader-first
exit remains bounded, while detected PID reuse fails closed. Non-Linux targets
without an equivalent identity capability reject process start. Output is
limited to `nil` or non-nil `*os.File`; typed-nil writers and arbitrary
`io.Writer` values are rejected so a blocking copy goroutine cannot hold `Wait`
forever.
`Process.Shutdown(ctx)`:

1. sends an interrupt to the process group;
2. waits within the caller deadline;
3. kills the process group when the deadline expires;
4. bounds the post-kill wait.

The wrapper reports process identity and exit timing only. It does not infer a
route-control outcome from a process exit.

Git provenance commands first resolve an absolute executable with `LookPath`,
run under a bounded `CommandContext`, and use a minimal environment with
ambient repository/object/index/config variables removed. Branch lookup treats
only exit status `1` as detached; other non-zero exits remain command errors.

## 7. Future Oracles

`OracleRegistry` stores metadata only:

```json
{
  "id": "route-control.future",
  "description": "observation-only future oracle",
  "minimum_evidence_level": "E3",
  "requires_attestation": true
}
```

Registration accepts no evaluator callback and never assigns `PASS` or `FAIL`.
An E3/E4 oracle must explicitly require attestation. The current CLI registers
metadata in the E1 self-check but does not execute an oracle.

## 8. CLI Contract

```text
go run ./cmd/rt2q-route-control-harness --level E1
```

The default level is E1; the command runs only the E1 self-check and exits `0`
on success.

```text
E1 self-check PASS
```

`--level E2` returns `INCONCLUSIVE` because no external scenario runner is
included. `--level E3` and `--level E4` return `INCONCLUSIVE` when a real
attestation is absent. Use `--attestation <path>` for the attestation input.
Even with a structurally valid attestation, the current CLI remains
`INCONCLUSIVE` because no route-control oracle is implemented.

Exit codes:

| Code | Meaning |
| --- | --- |
| `0` | E1 scaffold self-check completed. |
| `1` | E1 self-check implementation failure. |
| `2` | Usage error or `INCONCLUSIVE`. |

## 9. Scope and Limitations

- No production runtime, store, proto, composition, A/B file, smoke mainline,
  or e2e mainline is imported or modified.
- No real route authority, holder lifecycle, revision watch, Redis operation,
  or business transition is implemented.
- No E3/E4 evidence is generated, and no lower-level result is promoted.
- The handoff remains limited to test/evidence scaffold behavior until a future
  owner supplies real process wiring, attestation, and an independent oracle.
