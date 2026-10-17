# R2 Local K8s implementation progress — 2026-08-10

Status: `M2_BOOTSTRAP_RUNNING_NOT_QUALIFIED`

```text
PLAN_STATUS=APPROVED_FOR_IMPLEMENTATION_WITH_FINDINGS
K3S_INSTALL_AUTHORIZED=true
LOCAL_REGISTRY_AUTHORIZED=true
LOCAL_FUNCTIONAL_K8S_AUTHORIZED=true

COMPANY_REDIS_CONTACTED=false
ACTIVE_BINDING_AUTHORIZED=false
FORMAL_L2_AUTHORIZED=false
MIXED_AUTHORIZED=false
```

## Implemented in this increment

- fixed k3s bootstrap script with binary SHA verification;
- `/data/farm-l2-r2` data root and runtime receipts;
- static CPU Manager bootstrap and exact CPU allocation probe;
- loopback-only immutable local Registry;
- boot-generation-aware Route placement intent renderer;
- 30M assumption manifest v0.1;
- Redis run namespace Cluster Slot golden test;
- revised load execution findings and gates;
- local functional Kustomize overlay rendered with an explicit non-activatable Route intent;
- Profile B local dedicated Redis deployed for Functional-only use;
- runtime-only local CA/TLS and product Secret materialized;
- frozen Backend/H5 images published to the loopback Registry by immutable digest;
- fail-closed four-service Driver HTTP contract and blocked configs implemented.

## Runtime identities

```text
k3s version = v1.36.2+k3s1
k3s binary SHA256 = 65a55ec56c24eab44383086166ec620a491952b7e23941a49ddca6e8a4c4b4de
context = farm-l2-r2
node = farm-l2-r2
k3s data-dir = /data/farm-l2-r2/k3s

registry listen = 127.0.0.1:5001
registry image = registry@sha256:46faa9a1ae6813194b53921a370f2f4f8c5e1aae228a89bceafef5847a6a3278
registry data = /data/farm-l2-r2/registry
```

Runtime receipts remain outside Git under `/data/farm-l2-r2/receipts`:

```text
k3s-bootstrap.json SHA256
= 616cd7f9a2a9e4b7a97f6a1bc44ed75c96a621ed4193ecd687e2bf83d0ecd583

local-registry.json SHA256
= 7f1e03c59d8bb2f21f3d3856486b05fa9fba7cc09578ef6d648167660e033464

cpu-allocation-profile-probe.json SHA256
= 1837854e2fd4376326c92c971a0d913b29bc854c88d6e26be625d5ad070a57a1

frozen-images-local-registry.json SHA256
= 74158aefc30c24a70af5e1c30577bb3fbf3f264cb182944a372c788ef350fc04

functional-render-binding.json SHA256
= 1048dd1f1810bcbbca15767ab2371a4e1618cfa9032eec9af0cbb4309642e042

local-dedicated-redis.json SHA256
= 1b3e8dc81aabc2c0b02e214e924c055e27f2e36dae8be8c01f4d59dea8ae359c
```

No kubeconfig, certificate, company Redis endpoint, Secret, raw container ID or raw cgroup path is committed.

## CPU probe

A Guaranteed target Pod and dependency Pod were run together, then deleted:

```text
Target request      = 4 CPU
Target cpuset       = 4-7
Dependency request  = 10 CPU
Dependency cpuset   = 16-25
Driver reserved     = 8-15
Observer reserved   = 28-31
Unallocated safety  = 26-27
Result              = PASS
```

This proves the initial CPU Manager allocation on the current host only. It is not yet the Formal CPU isolation receipt; IRQ/softirq, CFS throttling and actual workload cgroups remain open.

## Preserved implementation attempts

1. Initial bootstrap attempt stopped before installation because `/data` required privileged directory creation. The script was corrected to create only `/data/farm-l2-r2` through non-interactive sudo and hand ownership to the invoking user.
2. Initial CPU probe was rejected by Kubernetes because the pinned Alpine image defaults to root while `runAsNonRoot=true`. No test workload ran. The probe was corrected to explicit UID/GID 65534, read-only rootfs and dropped capabilities.

These attempts are implementation diagnostics, not capacity samples and are not renamed PASS.

## Current cluster state

- k3s service active;
- node Ready;
- CoreDNS, local-path, metrics-server and Traefik Running;
- local Registry Running and loopback-only;
- local dedicated Redis Profile B Running, `capacity_constant_eligible=false`;
- local TLS and runtime product Secret materialized;
- frozen images present in loopback Registry;
- farm product workload not deployed because Route intent is deliberately `NOT_AUTHORITY_INITIALIZED`;
- no company Redis network request made;
- no L2/Mixed/Soak executed.

## Next implementation work

1. Boot-aware `cap-static-route-init` using the existing authority repository seam;
2. replace render-only 4/4 Route intent with initialized 1024/1024 authority receipt;
3. implement legal Gate adapter by extracting the real ws-stress protocol client;
4. complete Player/Farm legal-ingress decision and Worker full-Work fixture;
5. Prometheus/Collector/Oracle integration;
6. create provisional Binding and only then deploy farm product workloads.
