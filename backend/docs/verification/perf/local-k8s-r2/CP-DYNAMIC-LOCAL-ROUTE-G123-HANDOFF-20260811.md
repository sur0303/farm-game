# CP-DYNAMIC-LOCAL-ROUTE-G123 handoff — 2026-08-11

## 1. Purpose and current verdict

This work package creates and executes the local Kubernetes sibling profile for
Route Control dynamic runtime verification:

- **G1** authenticated complete-Snapshot Remote Watch;
- **G2** planned A→B→A→B migration with Drain;
- **G3** proof-gated crash takeover.

Current verdict at this handoff:

```text
PROFILE IMPLEMENTATION = IN PROGRESS
R1 = FAIL (frozen; not reusable)
G1 = NOT_RUN
G2 = NOT_RUN
G3 = NOT_RUN
CP-ROUTE-CONTROL-MVP = NOT_FROZEN
Integration update = NOT_PERFORMED
AS0-AS3 = NOT_STARTED
```

No dynamic-runtime PASS is claimed by this document.

## 2. Frozen input and Writer isolation

Frozen convergence input:

```text
exact tip = f834383a19790809c7cce062b551191c95dfa3cd
checkpoint = origin/checkpoint/route-control-local-convergence-final-20260811
receipt = /data/farm-l2-r2/receipts/route-control-local-convergence-final-20260811.json
receipt SHA256 = c6e3118ba11b91914680b5ecef7934da2af1f6f52055d7f7ee2703ce34532c5b
```

Dynamic Writer:

```text
worktree = /data/home/developer_026/CodeBuddy/20260519141303/farm-game-dynamic-local-g123-20260811
branch = codex/dynamic-local-g123-20260811
initial profile implementation head = f7cd1d32d028febc3f70627d03866f08461debf7
R1 closure head = d280ee915fde68d7bf4c5ff38abb85796bb8f670
base parent = f834383a19790809c7cce062b551191c95dfa3cd
```

The R1 closure is an SSH-signed follow-up touching only:

```text
scripts/local-k8s/apply-dynamic-route-profile
scripts/local-k8s/bootstrap-dynamic-route
scripts/local-k8s/materialize-dynamic-route-profile.py
```

It performs only these closures:

1. treat bootstrap-only `claims: null` as an empty claim list;
2. increase pre-Controller initial Fetch timeout from 5 seconds to the existing
   bounded maximum of 30 seconds;
3. create an immutable phase-specific failure receipt on every future apply
   failure.

## 3. Dynamic sibling profile contract

The profile is `route-migration-4x4`, separate from static
`route-scale-1024x1024`.

```text
Kubernetes namespace = farm-route-dynamic
Route Controller = 1 replica, Deployment strategy Recreate
Player holders = 2 StatefulSet replicas
Farm holders = 2 StatefulSet replicas
Workers = 2 Deployment replicas
Gate = 1 Deployment replica
H5 = 1 Deployment replica
PLAYER shards = 4
FARM shards = 4
AUTHORITY_MEMBERSHIP_MODE = composed
ACTOR_ASSIGNMENT_MODE = remote-watch
ROUTE_CONTROLLER_LEASE_PROJECTION_MODE = disabled
NetworkPolicy enforcement = NOT_CLAIMED
Capacity qualification = NOT_CLAIMED
```

No NetworkPolicy resource is added by the dynamic profile. The dependency
namespace may contain its own pre-existing local Redis policy, but this work
package does not claim end-to-end enforcement.

All Go consumers use the complete authenticated Snapshot transport. Resolve
continues to use the process-local immutable Snapshot; the profile adds no
remote Resolve path.

## 4. Authority and Controller ownership

Authority ordering remains:

```text
Redis Authority
  -> RouteAssignmentManager
  -> durable complete Snapshot CAS
  -> authenticated complete-Snapshot transport
  -> process-local immutable Snapshot
  -> HolderRegistry / RoutePlan / routectl views
```

HolderRegistry is used only for holder liveness and exact target validation. It
does not grant write authority.

The initial population choreography is deliberately zero-shard first so the
dynamic profile does not use `HOLDER_ALLOW_INITIAL_SERVING`:

1. Create Holder Pods to obtain their Kubernetes Pod UIDs.
2. Bootstrap revision 1 containing four PLAYER and four FARM `ASSIGNING`
   placeholders and no Authority records.
3. Start exactly one initial Controller. All four holders install the complete
   zero-shard view and register `READY_ZERO_SHARD`.
4. Scale the initial Controller to zero and verify that no Controller Pod
   remains.
5. While no Controller is running, execute the production bootstrap Manager to
   Claim all eight initial generations.
6. Start exactly one final Controller with a never-before-used Boot ID. It loads
   the resulting revision 9 and publishes through authenticated Watch.

Three distinct identities are materialized for the zero-shard runtime,
stopped external bootstrap Manager, and final dynamic runtime. They must never
be reused or overlap.

## 5. Holder process Boot identity

`Dockerfile.runtime.prebuilt` now supports the explicit dynamic-only mode:

```text
HOLDER_PROCESS_BOOT_MODE=pod-uid-then-random
```

Contract:

- first process in a new Pod: `<pod-name>-<pod-uid>`;
- every subsequent process in the same Pod: `<pod-name>-<fresh-random-uuid>`;
- Pod recreation: new Pod UID and a new first-process Boot;
- current process Boot is written only to the private in-Pod tmpfs file
  `/var/run/farm-holder/current-boot-id`;
- no raw Boot file is persisted in repository evidence.

This closes the requirement that HolderBootID express each process startup and
not merely the Pod identity.

## 6. Exact TLS identities

R1 generated one local-only CA and four exact single-DNS-SAN leaves:

```text
route-controller.farm-route-dynamic.svc.cluster.local
route-client.farm-route-dynamic.local
holder-control.farm-route-dynamic.svc.cluster.local
farm-gate-verifier.farm-route-dynamic.svc.cluster.local
```

Controller inbound Snapshot/Plan ACLs accept only the route-client identity.
HolderControl and Gate Verification accept only the exact Controller identity.
No CN, payload role, metadata role, IP SAN, URI SAN, or multi-SAN identity is
accepted as authority.

Private keys remain under the protected local runtime directory and Kubernetes
Secret. Receipts contain certificate hashes and identities only.

## 7. Implemented tooling

Committed at `f7cd1d3...`:

```text
deploy/k8s/dynamic-local-g123/README.md
scripts/local-k8s/materialize-dynamic-route-profile.py
scripts/local-k8s/materialize-dynamic-route-secrets
scripts/local-k8s/materialize-dynamic-route-tls
scripts/local-k8s/bootstrap-dynamic-route
scripts/local-k8s/apply-dynamic-route-profile
scripts/local-k8s/test_dynamic_route_profile.py
```

`cmd/rt2q-route-bootstrap` now accepts `--shard-count` from 1 through 64; its
default remains 2 for existing RT2Q behavior. Dynamic local passes 4 and builds
eight Manager Claim targets.

Unit tests use bootstrap-only for 4x4 because miniredis intentionally cannot
satisfy the production transition store's `MEMORY USAGE SAMPLES 0` proof after
five records. Production safety was not relaxed. R2 must prove all eight Claims
against real local Redis.

Tooling tests completed before R1:

```text
scripts/local-k8s/test-tools = PASS
scripts/local-k8s/test_dynamic_route_profile.py = PASS
cmd/rt2q-route-bootstrap tests = PASS
focused route/controller/manager/plan tests = PASS
focused race = PASS
vet = PASS
```

The first full-tree test run had one unrelated open-loop scheduler timing
failure in `cmd/cap-single-pod`; isolated rerun with `-count=3` passed. After the
R1 closure, a fresh `go test ./... -count=1` completed PASS. Both observations
are retained rather than hiding the initial test-environment flake.

## 8. R1 execution and immutable failure

R1 identity:

```text
run_id = dyn-g123-r1-20260811
Redis namespace environment = dyng123r120260811
source head = f7cd1d32d028febc3f70627d03866f08461debf7
```

Observed timeline:

1. Local Registry and local ephemeral Redis were Ready.
2. Dynamic namespace, Secret, TLS, ConfigMap, Services, and four Holder Pods
   were created.
3. Durable bootstrap-only Snapshot revision 1 committed successfully.
4. Receipt finalization attempted `len(claims)` while bootstrap output encoded
   the nil Go slice as JSON `null`, causing a Python `TypeError`.
5. The configured initial Fetch timeout was only 5 seconds. Because the initial
   Controller had not started yet, all four holders exited and Kubernetes
   restarted them.
6. Process restart invalidated the known first-process Boot identities, so R1
   was declared ineligible rather than resumed.

R1 is frozen as FAIL:

```text
/data/farm-l2-r2/receipts/dynamic-route-apply-dyn-g123-r1-20260811.json
SHA256=b436ea26ba8ca49276050b93719f2fd9a9c94caee17d34959dcbc0077ab6f5d1
status=APPLY_FAILED_EVIDENCE_RETAINED
eligible_for_g1_g2_g3=false
```

R1 artifacts:

| Artifact | SHA256 |
|---|---|
| `/data/farm-l2-r2/receipts/route-runtime-image-dyn-g123-r1-20260811.json` | `4b7651c6156408cd8816c3db5941b36e550b784291ec264d67ac26f594d75221` |
| `/data/farm-l2-r2/receipts/dynamic-route-profile-dyn-g123-r1-20260811.json` | `a6bf62714e222f879530ea077c5388170345062e9512bfaf52a850a41947c871` |
| `/data/farm-l2-r2/receipts/dynamic-route-secrets-dyn-g123-r1-20260811.json` | `1d39ab0d00c09f7422d882545c1af7e522117e94221aac096f628106927bdb40` |
| `/data/farm-l2-r2/receipts/dynamic-route-tls-dyn-g123-r1-20260811.json` | `f6b35165c7c825a7a5810b2ac39e9d2334d90e1535ba70089901d22ed2270ffd` |
| `/data/farm-l2-r2/runtime/dynamic-route-dyn-g123-r1-20260811/bootstrap-initial.json` | `5ca661be31481a4ae583ca57f1ce08d4547310c517605b533cdc25bcc78e9809` |
| failure receipt above | `b436ea26ba8ca49276050b93719f2fd9a9c94caee17d34959dcbc0077ab6f5d1` |

R1 Kubernetes namespace was deleted only after the failure receipt was written.
The R1 Redis keyspace remains untouched. No flush or broad scan was performed.

## 9. Current infrastructure state

At handoff:

```text
farm-route-dynamic namespace = absent
farm-l2-r2-deps local Redis Ready replicas = 1
R1 Redis namespace = retained
company Redis contacted = false
Redis KEYS = not used
Redis global SCAN = not used
Redis SCRIPT FLUSH = not used
local Redis flush = not used
Registry GC / Docker volume prune = not used
```

R2 must use a fresh run ID, Redis namespace, TLS material, Secret UID, Controller
Boot generations, Pod UIDs, and HolderBootIDs. R1 identities may not be reused.

## 10. Required R2 sequence

Before deployment:

1. Run Python/shell syntax checks and `scripts/local-k8s/test-tools`.
2. Run bootstrap and focused route tests/race/vet.
3. Run the full tree; record any unrelated flake and require isolated repeat.
4. Verify the SSH signature on closure commit `d280ee9...` and require a clean
   Writer worktree after this handoff document is committed.
5. Publish a new immutable runtime image receipt bound to the new exact head.
6. Materialize a new profile with a new `ROUTE_RUN_ID` and
   `REDIS_NAMESPACE_ENVIRONMENT`.
7. Dry-run the complete Holder and full manifests.
8. Apply. Any failure must emit the new phase-specific immutable receipt.

Do not continue a run after any Holder restart before the initial Claim phase.
Do not modify R1 evidence.

## 11. G1 acceptance

G1 can pass only if all of the following hold continuously:

- exactly one final Controller Pod and Recreate strategy;
- two Player, two Farm, two Worker, one Gate, and one H5 Pod Ready;
- no Holder process restart during bootstrap/G1;
- all four Holder current Boot files match the expected process identities;
- complete Snapshot has four PLAYER and four FARM routes, all ACTIVE;
- all eight Authorities exactly match Snapshot holder/boot/revision/writer
  epoch/lease epoch;
- Player A and Farm A initially serve four shards each;
- Player B and Farm B are `READY_ZERO_SHARD`;
- Controller, Player, Farm, Worker, and Gate observe a coherent complete
  revision through remote Watch;
- malformed/incomplete Watch input is not installed;
- Resolve hot path remains local-only;
- exact TLS identities and ACLs are observed;
- H5 health/root and Gate/Worker readiness regressions pass;
- NetworkPolicy remains explicitly `NOT_CLAIMED`.

A brief Ready sample is insufficient; evidence must include a sustained
observation window and no process/revision regression.

## 12. G2 acceptance

Use `routectl move-shard` and `verify` with exact current fences. For selected
PLAYER and FARM shards execute:

```text
A -> B -> A -> B
```

Each move must:

- finish with Plan phase `SUCCEEDED`;
- use planned Drain, never direct transfer;
- stop new activation/turns, wait current turns, commit facts, then ColdLoad;
- advance assignment revision, writer epoch, and lease epoch monotonically;
- publish only complete Snapshots;
- leave exactly one ACTIVE writer;
- preserve consumer readiness and coherent Watch revision;
- pass Gate production VerificationPort checks;
- produce no Holder/Controller process restart or publisher overlap.

## 13. G3 acceptance

Prefer the shard left on Holder B by G2 so crashing B affects only the selected
shard. Required sequence:

1. Capture exact source fence and lease deadline.
2. Crash/recreate the source Holder process and prove its Boot changed.
3. Attempt early takeover and require rejection while proof/expiry is
   insufficient.
4. Observe the old generation fail closed; do not renew or reuse it.
5. After explicit `CrashTakeoverProof` requirements are satisfied, execute
   `takeover-after-expiry` to Holder A.
6. Require a strictly higher generation, one ACTIVE writer, exact Snapshot /
   Authority / Holder agreement, and successful `routectl verify`.
7. Re-run Worker, Gate, and H5 business/readiness checks.

A direct Transfer without proof is a gate failure.

## 14. Freeze and governance after G3

Only after G1/G2/G3 and business regression all pass:

1. freeze an exact dynamic runtime tip and immutable receipts;
2. perform detached read-only review and close every P0/P1/P2;
3. freeze `CP-ROUTE-CONTROL-MVP`;
4. request explicit approval before updating Integration;
5. start AS0-AS3 only after the approved Integration update.

Until then, the frozen Integration anchor and autoscaling remain unchanged.
