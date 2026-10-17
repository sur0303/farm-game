# CAP-SINGLE-POD-L2-R2 Controller Binding Readiness

日期：2026-08-10

## Current verdict

```text
GENERIC_HARNESS_REVIEW=PASS
READY_FOR_CONTROLLER_BINDING=true
R2_ACTIVE_BINDING_HEAD=NOT_GENERATED
R2_ACTIVE_BINDING_SHA256=NOT_GENERATED
READY_FOR_STATIC_STAGING=false
```

`READY_FOR_CONTROLLER_BINDING` 只表示 Generic Harness 已通过代码与确定性门；它不表示正式 Runtime identity 已齐备。当前禁止把 blocked R2 Binding 改成 ACTIVE。

## Reviewed Harness

```text
Original reviewed head = eabb83c828a97dd5988d20c1549d4f8bf6c4b9fb
Round-2 finding result = d66a8bf290c0927b8a8bdf4fe6ba7e8be0ac3137
P1 fix implementation  = 73da76b938fb0cea6e95940f3ea540dc3c5306c0
Final reviewed head    = 8f3a22e2ca355aa89096334a41abb188787e220f
Focused review result  = 7ea9dd2af2652a7ab93a675071b4baf8ec3d2d29
Harness binary SHA256  = 7d1fce85aa4b462db80158ddb04ce1a3a73111937dc9f51e027ce115acee5fcb
P0/P1/P2                = 0/0/0
EXACT_HEAD_CLOSURE      = PASS
```

Harness and both review lineages were Controller-composed into Backend Integration:

```text
cbd4b8de722ad0ae42fd6439b65a65b58c81aac2
```

The qualified Backend runtime identity remains `c781775a804af0988380a9cbb0dbd758ff20f6cd`; later Harness/documentation commits are not reclassified as product runtime tests.

## Known product identities

```text
Backend qualified integration = c781775a804af0988380a9cbb0dbd758ff20f6cd
Backend business              = 5ce43715363eea4f18e0925b6992429785053261
H5 integration                = f22dcb447e154e424656b12409e041c32dfb4c5c
Backend local image ID        = sha256:672cf6862dcf06a4a5bfbb6146263b98bf04de930b113f7eb8fd8e8efb3493e9
H5 local image ID             = sha256:eb1bb441ab6c46ef647bb07e360d9f068f9728e07a82f18450ef85b633f96f80
Redis local image ID          = sha256:2a51817f79255c8b69f86a974459c2e0359aff81417d80158f2b9e541e6f4b33
Model layout                  = R2:authority-v1+workflow-v1+friend-authority-v1;Model-Field-V2-runtime-enabled;legacy-relationship-roster-removed
```

Local Docker Image IDs are immutable local identities, but they are not silently promoted to registry manifest digests required by Kubernetes `repo@sha256:...` deployment.

## Static render results

Candidate and W1 local rendering are structurally clean:

```text
candidate status         = CANDIDATE_RENDERED_NOT_DEPLOYED
candidate issues         = 0
candidate config SHA     = 7db19f5c24eb1752edfdf93dd16ee50d8bee564782af2e0541fcbb11cf0a419e
candidate manifest SHA   = 2304712a749b2411c725245bf4268fc30b10d9b19b268a3804e6e9c36cd63545
candidate rendered SHA   = 5c5019904b573b6a7ac390c86b632e9c8a08a039075b77d645f1f39555623723

W1 status                = CANDIDATE_RENDERED_NOT_DEPLOYED
W1 issues                = 0
W1 manifest SHA          = 0ca3b6dea1c54a322d8c0adb0232f308a575c2d9b2e5d9f32e9d48e09944b782
W1 rendered SHA          = 8b498debd3c81f72162e95c606da874d6a711265319816fd493578d46d4b45fd

proposal route snapshot  = 723b4cf7f051e4386321d0ca92d2b097faff15e1584620a632e7568d65e5a7cc
```

Release validator correctly returns `REJECTED` with 19 unresolved checks. It was not applied.

The available kubectl context is `kind-farm-perf`; this is explicitly `PROPOSAL_ONLY_NOT_AUTHORIZED` and was not used for Staging acceptance.

## Missing activation inputs

Controller must not produce an Active R2 Binding until all of these are exact and reviewable:

1. approved formal cluster identity/context and namespace;
2. Backend and H5 registry `repo@manifest-digest` identities;
3. formal staging host, TLS Secret identity and ingress class;
4. controlled Redis network CIDR and reviewed URI/ACL/persistence prerequisites, without recording the raw URI;
5. reviewed zero-unexplained Friendship shadow Evidence binding;
6. R2 Profile, Manifest Schema, Required Metrics, Stop Conditions, Verdict and Environment hashes;
7. runtime Route Snapshot, Lease Profile, Server Config and Redis Config hashes;
8. exact per-service Collector and Oracle config hashes;
9. exact Gate/Player/Farm/Worker Go protocol Driver binaries/configs and their hashes;
10. exact Harness binary plus final Binding SHA activation receipt.

The Harness binary and Binding file cannot directly hash each other without a cycle. Controller must use an external activation receipt that binds `Binding SHA ↔ Harness Binary SHA ↔ Review Result Head ↔ runtime config hashes`; the compile-time trust root continues to contain the exact Binding SHA.

## Evidence files

```text
static-candidate-validation.json
static-w1-validation.json
static-release-validation.json
SHA256SUMS
```

These are local/rendered qualification records only. Their presence cannot satisfy Kubernetes Staging, Observability Qualification or Load Generator Runtime Qualification.
