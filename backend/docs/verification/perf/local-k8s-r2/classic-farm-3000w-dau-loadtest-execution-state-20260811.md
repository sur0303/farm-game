# Classic Farm 30M DAU local load-test execution state — 2026-08-11

## 1. Durable directive

The controlling task is:

```text
docs/verification/perf/local-k8s-r2/classic-farm-3000w-dau-loadtest-plan-v1.1.md
```

Plan identity before tracking:

```text
version = v1.1
lines = 1138
SHA256 = 5936653b68872a8cebfb1ad3c667328bb1e60982583be006081972e5dde492c5
```

This file is the durable execution addendum for context-compaction recovery. The
plan must be executed, not merely reviewed. Execution is dependency-gated: the
current action is P-1 friend-opportunity product convergence from the immutable
R7 baseline. No capacity or 30M-DAU claim is permitted before all prerequisite
checkpoints pass.

Only `Cursor Grok 4.5 High Fast` may be used as a Subagent for this task.
Subagents are read-only unless a later explicit narrow task says otherwise.
The primary Writer remains responsible for every edit, command, cluster
mutation, verdict, and verification.

## 2. Frozen source lineage

```text
f834383a19790809c7cce062b551191c95dfa3cd
  frozen Route Control local convergence
  -> f7cd1d32d028febc3f70627d03866f08461debf7
     initial Dynamic Local profile
  -> d280ee915fde68d7bf4c5ff38abb85796bb8f670
     R1 closure: null claims, 30s fetch, phase failure receipt
  -> 0fe5fba8eb9668e384a9d3e25ce740fa88635394
     Dynamic G123 handoff
```

Current Writer:

```text
execution-state source head when created = 0fe5fba8eb9668e384a9d3e25ce740fa88635394
worktree = /data/home/developer_026/CodeBuddy/20260519141303/farm-game-friend-opportunity-convergence-20260812
branch = codex/friend-opportunity-convergence-20260812
P-1 base HEAD = 862fef100de0dfc3b00c91613fd6dcc007ddc81a
P-1 base tree = 6572b235da13e201d72c4728d0deaf960e4d781d
Dynamic R7 parent = 03efc75d6b77e4dc328ea103573af8a29468d152
capacity parent = 1c055cdcbfb5cfaa788b47003bea728d277f321f
H5 source HEAD = 787dc00d100cddafa622bcec3d0fffbd3babf2e3
Integration anchor remains = 5ce43715363eea4f18e0925b6992429785053261
Integration updated by this task = false
```

The Backend merge is SSH-signed and clean at handoff. It passed
`go test ./... -count=1`, `go vet ./...`, focused race tests,
`scripts/local-k8s/test-tools`, Compose fallback rendering, and an independent
read-only review with P0/P1/P2 findings `0/0/0`. These results prove structural
Backend convergence only. H5 immutable image binding, the final product run,
P-1, final G1/G2/G3, and capacity remain incomplete.

The v1.1 plan has stale historical status in section 0.5: the three R1 closure
files are no longer uncommitted and R2 has already been attempted. Treat the
plan's contracts and target phases as controlling, but use this addendum for
current attempt state.

## 3. Immutable prior failures

### R1 — terminal FAIL

```text
run_id = dyn-g123-r1-20260811
source = f7cd1d32d028febc3f70627d03866f08461debf7
receipt = /data/farm-l2-r2/receipts/dynamic-route-apply-dyn-g123-r1-20260811.json
receipt SHA256 = b436ea26ba8ca49276050b93719f2fd9a9c94caee17d34959dcbc0077ab6f5d1
failure = claims:null receipt finalization, followed by 5s Holder initial Fetch exits
eligible_for_g1_g2_g3 = false
```

### R2 — terminal FAIL

```text
run_id = dyn-g123-r2-20260811
source = 0fe5fba8eb9668e384a9d3e25ce740fa88635394
Redis namespace environment = dyng123r220260811
receipt = /data/farm-l2-r2/receipts/dynamic-route-apply-dyn-g123-r2-20260811.json
receipt SHA256 = 21277db008eaf17c02244f9721d4c5d4fe56997ce8da3da575697380edec6fc4
status = APPLY_FAILED_EVIDENCE_RETAINED
failure_stage = zero_shard_runtime_startup
eligible_for_g1_g2_g3 = false
```

R2 reached durable zero-shard Snapshot revision 1 and a healthy initial
Controller. Authenticated `routectl status` succeeded. Holder initial Fetch
therefore worked, but `NewRuntimeForModeWithAssignment` timed out while waiting
for endpoint health; each Holder process restarted and invalidated the run.
Additional independent deployment failures were Gate showcase `ws://` config
and H5 root-image versus `runAsNonRoot`.

### R3 — terminal FAIL

```text
run_id = dyn-g123-r3-20260811
source = 8296c4283bf198acb21de8cc27f74d31b9346a4a
Redis namespace environment = dyng123r320260811
apply receipt = /data/farm-l2-r2/receipts/dynamic-route-apply-dyn-g123-r3-20260811.json
apply receipt SHA256 = 832575c99c4d3e0c2d7e2505f8d201e29d40f9f6d326e897260ab686b4a2bf68
diagnostic receipt = /data/farm-l2-r2/receipts/dynamic-route-diagnostic-dyn-g123-r3-20260811.json
diagnostic receipt SHA256 = 1cf309fcc0f31b31a303186eb4e30c5a7574a14a9a4019ddb9449b2f9d8ee2b5
failure_stage = final_route_verification
eligible_for_gates = false
```

R3 successfully proved revision-1 `READY_ZERO_SHARD`, zero Holder restarts,
stop-first Controller, eight external Manager Claims, and a healthy final
Controller. It failed before business consumers started because the verifier
incorrectly required four HolderRefs in the final process Snapshot. The Manager
correctly compacted the complete route projection to the two referenced ACTIVE
holders; the two B targets remained durable `READY_ZERO_SHARD` records in
HolderRegistry. At diagnostic freeze the Manager-owned deadline refresh had
advanced the complete Snapshot to revision 11 with two HolderRefs, PLAYER index
0 and FARM index 1. R3 is not resumable; R4 changes only this verifier
expectation and uses fresh identities.

### R4 — terminal FAIL

```text
run_id = dyn-g123-r4-20260811
source = 1bab6f94e9079d5c08e5faa767d8e7e153e7f1e2
Redis namespace environment = dyng123r420260811
apply receipt SHA256 = 50df8404d86f5e60791ed45d3e60922777426527496e7fd7116cfe25c4624409
diagnostic receipt = /data/farm-l2-r2/receipts/dynamic-route-diagnostic-dyn-g123-r4-20260811.json
diagnostic receipt SHA256 = 8935635fdd96c1239c51d64fc1242eb42fcd599c7d883b27318aaea59cfec0fc
failure_stage = business_startup
eligible_for_gates = false
```

R4 passed revision-1, stop-first, eight Claims, compact final Snapshot and final
route verification, then failed during business rollout. Three independent
profile/runtime seams were frozen: Farm Service omitted health port 9091 used
by Gate readiness; dynamic pooled Worker clients were not represented in the
existing participant-connection readiness monitor; and H5 nginx resolved the
Gate upstream before Service DNS propagation and restarted twice. R5 adds the
health Service port, a complete-generation pooled readiness adapter, and an H5
Gate-ready init barrier. R4 is not resumable.

### R5 — terminal FAIL

```text
run_id = dyn-g123-r5-20260811
source = fac234803e83596c2b17c5ee534694e71fee706f
Redis namespace environment = dyng123r520260811
apply receipt SHA256 = f3f2f334d9accfa693190fb66b0b8540fca51f0f237abb0ef0c9dc0f9eddc8c7
diagnostic receipt = /data/farm-l2-r2/receipts/dynamic-route-diagnostic-dyn-g123-r5-20260811.json
diagnostic receipt SHA256 = 0acd0924a96379a1f6bdac00f2bc7f16de4b130d0256e1ba42c2478d17b3b54f
failure_stage = final_controller_startup
eligible_for_gates = false
```

R5 passed READY_ZERO_SHARD, stop-first and all eight Claims, then one FARM B
Holder became terminal unhealthy while the final Controller installed the
compact Snapshot generation. The R5 connection-state strengthening had been
placed in global `AssignmentComposition.Healthy`; a transient endpoint-pool
generation install was therefore interpreted by Holder Agent as process
readiness regression three times. R6 restores `Healthy` as the control-plane
contract and moves exact-generation gRPC connection state to a separate
`DataPlaneHealthy` used only by pooled Worker/Gate business readiness. R5 is not
resumable.

### R6 — terminal FAIL

```text
run_id = dyn-g123-r6-20260811
source = 09e81dfd6e93c92d6ca15dae6bf8a364944294e8
Redis namespace environment = dyng123r620260811
apply receipt SHA256 = 1140a2df06b5c922dbfa20ab5f423469259dca3ca51dafda9001b684516ccd22
diagnostic receipt = /data/farm-l2-r2/receipts/dynamic-route-diagnostic-dyn-g123-r6-20260811.json
diagnostic receipt SHA256 = 867bb44789109147c6dade4a503276f332953e48834c8aec98fd21b2d3d64ff1
failure_stage = final_controller_startup
eligible_for_gates = false
```

R6 also passed READY_ZERO_SHARD, stop-first and eight Claims. Although pooled
connection state had moved to `DataPlaneHealthy`, extra Pump-revision and
post-`ReadyFor` fences remained in global `Healthy`; one PLAYER B Holder again
reached terminal process-readiness regression at final generation install. R5
failed FARM B and R6 failed PLAYER B at the same boundary, while R4's original
`Healthy` contract passed it. R7 therefore restores exact pre-R5 `Healthy`
semantics and places all stronger Pump/revision/connection double checks only in
`DataPlaneHealthy`. R6 is not resumable.

### R7 — P-2 PASS; basic infrastructure G1 PASS

```text
run_id = dyn-g123-r7-20260811
source = 13e5b690c5c9eae0e07b8e853557e4309d9c2704
Redis namespace environment = dyng123r720260811
apply receipt SHA256 = 27b3d6d7b9d527b6f26625563ca5103575be679977764281cf42e4757e54e1ac
basic G1 receipt = /data/farm-l2-r2/receipts/dynamic-route-basic-g1-dyn-g123-r7-20260811.json
basic G1 receipt SHA256 = 3c8a0695b28a830ef16be8d40c6049e1661c81722244cee9ec866bc9002098e7
P-2 = PASS
final product G1 = NOT_RUN
capacity = NOT_CLAIMED
```

R7 completed the full phased apply with nine Ready Pods, zero workload/init
restarts, eight Claims, no Controller overlap, exact runtime/H5 image IDs and
immutable receipt chain. The basic infrastructure G1 then observed all seven Go
Watch consumers on the same complete revision 20 after crossing revision 17's
oldest ACTIVE deadline; all remained Ready with no restart. This baseline is not
final G1 because friend-opportunity Backend/H5 convergence has not yet been
applied. The next phase is P-1 exact Backend
`862fef100de0dfc3b00c91613fd6dcc007ddc81a`, capacity parent
`1c055cdcbfb5cfaa788b47003bea728d277f321f`, and H5
`787dc00d100cddafa622bcec3d0fffbd3babf2e3`, followed by V2/shadow checks and
two-Worker OwnedShards qualification. The older Backend
`3a48228c63bf9121c77053b07a7fb758a8fa7d07` is already contained by the
capacity parent and must not be merged again.

R1 through R6 Redis namespaces, evidence, TLS identities, Pod identities, and
Boots must never be reused. R7 is a passed immutable baseline and must not be
mutated into the P-1 candidate. No broad Redis scan, `KEYS`, flush, company
Redis access, Registry GC, or volume prune is authorized.

## 4. R2 root cause and production-safe R3 correction

The zero-shard runtime failure is a Kubernetes readiness/DNS cycle, not a
Manager or zero-shard semantic defect:

```text
Assignment startup
  -> endpoint pool must dial complete Snapshot holder endpoints
  -> Player/Farm headless Services publish only Ready Pod addresses
  -> Holder readiness waits for assignment endpoint health
  -> holder DNS never becomes available
  -> startup context expires
```

The dynamic-only correction is:

```yaml
publishNotReadyAddresses: true
```

for `player-svr-headless` and `farm-svr-headless`. Static profiles are not
modified. `HOLDER_ALLOW_INITIAL_SERVING` remains absent/false. ASSIGNING-only,
authority-free zero-shard revision 1 remains valid.

R3 also must correct:

1. strict phase separation: Holders → bootstrap-only → initial Controller →
   durable ready-zero-shard barrier → Controller stop → external Manager 8
   Claims → final Controller → business consumers;
2. Gate showcase endpoints: `wss://farm.dynamic.local/ws` and
   `https://farm.dynamic.local`, with host-only origin allowlist;
3. H5 non-root runtime: UID/GID 101, no privilege escalation, all capabilities
   dropped, read-only root, bounded writable `emptyDir` mounts for nginx run,
   cache, and tmp paths;
4. immutable phase-specific failure receipts;
5. no NetworkPolicy claim (`network_policy_enforcement=NOT_CLAIMED`).

Public external TLS/Ingress is not required for the immediate R3 bootstrap
checkpoint. Until an exact local web TLS/Ingress is materialized and tested,
H5 public WSS is `NOT_RUN/NOT_CLAIMED`; internal Gate/Worker/route readiness can
still be tested. Do not represent route-control mTLS as public H5 TLS.

## 5. Exact R3 execution gate

R3 must use a new commit, immutable runtime image, RunID, Redis namespace,
profile receipt, TLS CA/leaves, Secret UID, Controller Boots, Pod UIDs, and
Holder process Boots.

Required preflight:

- preserve and commit the controlling v1.1 plan and this state;
- Writer clean;
- Python and shell syntax PASS;
- `scripts/local-k8s/test-tools` PASS;
- dynamic renderer tests PASS;
- bootstrap/Controller/Manager/Holder/Route focused tests PASS;
- focused race PASS;
- full `go test ./... -count=1` PASS or explicitly retained unrelated flake
  followed by repeat evidence;
- `go vet ./...` PASS;
- SSH-signed exact source commit verified with the local allowed signer.

Required phased run:

1. materialize a fresh profile and dry-run every phase manifest;
2. apply namespace/config/Services/Holder StatefulSets only;
3. capture exact four Pod UIDs and expected first-process Boots;
4. commit zero-shard complete Snapshot revision 1;
5. start only the initial Controller and verify its exact Boot;
6. require four exact HolderRegistry records at revision 1:
   `READY_ZERO_SHARD`, process ready, not draining, serving shard count 0,
   positive capacity, expected endpoint and Boot, and sufficient lease margin;
7. require Holder container restart count 0;
8. scale Controller to zero and verify no Controller Pod remains;
9. run the external bootstrap Manager with its distinct Boot and exactly eight
   Claim targets;
10. require complete Snapshot revision 9 and 4 PLAYER + 4 FARM ACTIVE routes;
11. start one final Recreate Controller with a fresh Boot and no overlap;
12. verify 8/8 Snapshot/Authority/holder/boot/fence agreement;
13. only then apply Worker, Gate, and H5 business consumers;
14. sustain readiness and remote Watch observation; short Ready is not PASS;
15. write immutable success/failure evidence and clean only the Kubernetes
    namespace after evidence finalization.

Any Holder restart before Claim, Controller overlap, assignment before durable
ready-zero-shard, or evidence overwrite makes the attempt FAIL and requires a
new run identity.

## 6. Dependency graph for the full task

```text
P-2 fresh Dynamic R3 bootstrap
  -> R3 infrastructure G1 baseline
  -> P-1 Friend Backend/H5 convergence
     (Backend 862fef1 + capacity parent 1c055cd + verified H5 787dc00;
      H5 immutable image binding remains incomplete)
  -> T15/T16 friend qualification + initial T17 OwnedShards qualification
  -> final G1 across old Snapshot Deadline
  -> G2 PLAYER/FARM A->B->A->B with Drain
  -> G3 proof-gated Crash Takeover
  -> detached exact review and CP-ROUTE-CONTROL-MVP
  -> full product F1/F2/F3 + friend opportunity + Authority/Client Oracle
  -> T0 load-generator qualification
  -> T1-T9 component, business, hotspot, slow-reader, Worker tests
  -> T10-T12 fault under load
  -> T13 mixed load
  -> T14 30-minute then 2-hour Soak
  -> single-Pod safe constants, 1->2 scaling, N+1 model
  -> P9 local-only 30M DAU capacity derivation
```

Long-duration tests are blocked while
`ROUTE_CONTROLLER_LEASE_PROJECTION_MODE=disabled` lacks an independently proven
continuous deadline-refresh owner. Before final G1, 30-minute, or 2-hour runs,
either enable Manager-owned projection and rerun deadline/restart gates or
produce evidence for the single alternative owner. Never add a second
assignment state machine.

## 7. Current checkpoint board

```text
P-2 / CP-DYNAMIC-R2-BOOTSTRAP = PASS on immutable R7; P-1 friend-opportunity convergence = IN_PROGRESS
P-1 / Friend convergence = IN_PROGRESS
P0 / G1-G2-G3 = BLOCKED_BY_P-1
P1 / Load tools = preparation allowed; formal PASS NOT_RUN
P2 / Full product = BLOCKED
P3-P9 / all capacity, fault, soak, and 30M model = BLOCKED
T0-T17 formal runs = NOT_RUN
NetworkPolicy enforcement = NOT_CLAIMED
public web TLS/Ingress = NOT_CLAIMED
capacity qualification = NOT_CLAIMED
30M DAU support = NOT_CLAIMED
```

Existing G22 static liveness and RT2Q evidence cannot substitute for Dynamic
G123, full product, load-tool qualification, capacity, or soak evidence.

## 8. Formal run and evidence invariants

- one Run Operator and one formal run on this host/kind/Redis scope at a time;
- image builds and full-tree/high-CPU tests never overlap a formal run;
- every retry receives a new run identity and references immutable prior FAIL;
- exact Backend/H5/Harness/deploy commits, images, Pod image IDs, rendered
  manifests, runtime identities, and evidence hashes are bound together;
- planned, actual, unsent, client timeout, and driver resource use are all
  measured; missing means `NOT_MEASURED`, never zero;
- any nonzero hard correctness gate invalidates the performance result;
- `NOT_RUN`, `NOT_CLAIMED`, `BLOCKED`, `INCONCLUSIVE`, `FAIL`, and `PASS` are
  distinct and may not be collapsed;
- Evidence never stores Redis URLs/passwords, TLS private keys, Cookies,
  ConnectionTickets, or complete WriterClaims.

## 9. Context-recovery next command

After loading this file, do not jump to capacity tooling. Continue at:

```text
Preserve R7 as the immutable P-2/basic-G1 baseline. Form the P-1 Backend/H5
product convergence candidate from exact Backend
`862fef100de0dfc3b00c91613fd6dcc007ddc81a`, capacity parent
`1c055cdcbfb5cfaa788b47003bea728d277f321f`, and H5
`787dc00d100cddafa622bcec3d0fffbd3babf2e3`. Complete the immutable H5 image
binding, Dynamic profile wiring, V2/shadow proof, and two-Worker OwnedShards
proof before using a new identity for final G1. Do not mutate R7.
```
