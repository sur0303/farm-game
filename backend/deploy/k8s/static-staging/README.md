# R2 P1-A/B static-route Kubernetes Staging candidate

This directory is a renderable deployment candidate for namespace
`farm-staging-system`. It is deliberately not a deployment record: local
rendering produces `CANDIDATE_RENDERED_NOT_DEPLOYED`, and no cluster identity,
registry digest, controlled Redis endpoint, TLS material, or formal URL is
present in this worktree.

The root is a Kustomize wrapper. `base/` contains the deployable resource
graph; `overlays/release/` binds release-only image, host, proxy-CIDR,
Redis-CIDR, and the shadow-gated Friendship cutover; `overlays/w1-readiness/`
re-renders the exact Worker family-readiness contract; and
`overlays/local-gate-rollback/` is the explicit Friendship rollback mode.
`secret-templates/` contains schemas only and is intentionally excluded from
the release overlay.

Validate and render locally:

```bash
./scripts/validate-k8s-static-staging --profile candidate
./scripts/validate-k8s-static-staging --profile w1
kubectl kustomize deploy/k8s/static-staging
kubectl kustomize deploy/k8s/static-staging/overlays/release
kubectl kustomize deploy/k8s/static-staging/overlays/w1-readiness
kubectl kustomize deploy/k8s/static-staging/overlays/local-gate-rollback
```

The validator always performs the repository-local structural checks. If
`kubectl kustomize` or `kustomize` is installed it also requires that renderer
to succeed. It checks object counts and identity, selectors and ports, all
three probes, drain hooks, termination grace periods, PDBs, topology spread,
anti-affinity, backend monitoring, static Holder endpoints, Secret references, and
the default-deny/explicit NetworkPolicy graph. It rejects public internal
gRPC, Demo/fault/tool Gate settings, HPA, dynamic route controls, floating
tags, stale Worker family settings, an unbound `remote_player` cutover, and
unresolved release placeholders. It emits manifest, config, route, and
rendered hashes as structured JSON. A clean local verdict is still only
`CANDIDATE_RENDERED_NOT_DEPLOYED`.

## Apply order when prerequisites exist

1. Confirm the cluster identity, Kubernetes version, Ingress class, the
   `ServiceMonitor` CRD/operator, and the `ingress-nginx` and `monitoring`
   namespace labels expected by the policies. Record those facts outside this
   candidate; this repository does not invent them.
2. Create the namespace from `base/resources/namespace.json`.
3. Have the approved Secret manager materialize `farm-staging-secrets` with
   `redis-url`, `permit-secret`, `http-session-cookie-keyring`,
   `web-login-rate-limit-secret`, and `theft-prf-hmac-secret`. Materialize
   `farm-staging-tls` with `tls.crt` and `tls.key`. The files under
   `secret-templates/` must not be applied as values; they are contract
   templates containing deliberate placeholders.
4. Keep the ordinary candidate in `FRIENDSHIP_SERVICE_MODE=remote_shadow`.
   Its formal result remains local; the remote Friendship observation is
   read-only. Do not promote on file presence alone. Run the shadow evidence
   review and bind its zero-unexplained-shadow evidence SHA-256, reviewed flag,
   exact scope, and evidence reference in `platform-release-patch.json`.
5. Bind the release overlay using controlled review: replace both image
   `newName` values and both `sha256:REPLACE_WITH_...` digests in
   `overlays/release/kustomization.yaml`; replace the formal host in
   `platform-release-patch.json` and `ingress-release-patch.json`, the H5 proxy
   CIDR in `platform-release-patch.json`, the reviewed Friendship evidence
   fields in `platform-release-patch.json`, and the managed Redis CIDR in
   `redis-egress-release-patch.json`.
   Do this as a reviewed Kustomize patch edit, never by a broad source-string
   substitution.
6. Run the release validator. It must exit zero before applying:

   ```bash
   ./scripts/validate-k8s-static-staging --profile release
   ```

   In this worktree it is expected to fail closed because the prerequisites
   above have not been supplied.
7. Apply the release overlay only after the release verdict is clean:

   ```bash
   kubectl apply -k deploy/k8s/static-staging/overlays/release
   kubectl -n farm-staging-system rollout status deployment/farm-gate
   kubectl -n farm-staging-system rollout status deployment/farm-worker
   kubectl -n farm-staging-system rollout status deployment/farm-h5
   kubectl -n farm-staging-system rollout status statefulset/player-svr
   kubectl -n farm-staging-system rollout status statefulset/farm-svr
   ```

The backend gRPC Services are ClusterIP-only. The holder route snapshot uses
the exact StatefulSet ordinal DNS names and ports. Player and Farm Holders use
`ACTOR_HOLDER_POD=metadata.name` and a versioned deterministic boot identity
`<pod-name>-static-route-v1`; changing that identity requires a new, reviewed
static snapshot and authority generation. This frozen static compatibility path intentionally does **not** set
`ACTOR_HOLDER_POD_UID`, `ACTOR_HOLDER_WORKLOAD_UID`, or
`ACTOR_HOLDER_ORDINAL`; its Holder records therefore carry the explicit zero
deployment tuple whenever Holder composition is enabled in the new
`holders:v2` / `holder-registry-v5` namespace. The key-layout split prevents
new binaries from reinterpreting the frozen
`holders:v1` / registry-v4 records in place. This path is not admissible to a
ScalePlan Kubernetes adapter and must not be presented as a G21 rerun.

The future dynamic/ScalePlan overlay is a separate protocol mode. It must use
`ACTOR_ASSIGNMENT_MODE=remote-watch`, set the exact token
`ACTOR_HOLDER_IDENTITY_MODE=kubernetes-process-v1`, inject
`ACTOR_HOLDER_POD_UID` from
Downward API `metadata.uid`, inject canonical ordinal from
`metadata.labels['apps.kubernetes.io/pod-index']`, and inject
`ACTOR_HOLDER_WORKLOAD_UID` from a trusted controller-stamped value that was
joined to the Pod controller owner reference and live StatefulSet UID. These
registry fields are holder **claims**, not that trusted join. Native Downward
API cannot expose `ownerReferences[].uid`, so a StatefulSet name or Pod
self-assertion is not an authority proof. A future production ScalePlan
HolderStateReader must compare every claim with its independently captured
Kubernetes Pod/owner/StatefulSet facts and fail closed on any mismatch. In this
exact mode
`ACTOR_HOLDER_BOOT_ID` is forbidden: the process generates one random BootID
once and shares it across authority, HolderControl, and Holder registration.

There is no Dynamic Watch, transfer, takeover, Holder HPA, or mTLS
implementation in this static candidate. Internal gRPC
is plaintext only inside the explicitly allowed NetworkPolicy paths.

## Friendship shadow gate and rollback

The base and W1 candidate explicitly use `FRIENDSHIP_SERVICE_MODE=remote_shadow`
and `FRIENDSHIP_RPC_TIMEOUT=2s`. Formal Friendship results remain on the local
path while remote observations are read-only. The release overlay is deliberately
set to `remote_player`, but its validator gate remains closed until all of these
are bound and reviewed:

- `FRIENDSHIP_SHADOW_EVIDENCE_SHA256`: exactly 64 hexadecimal characters;
- `FRIENDSHIP_SHADOW_EVIDENCE_REVIEWED=true`;
- `FRIENDSHIP_SHADOW_EVIDENCE_SCOPE=zero-unexplained-shadow-v1`; and
- a non-placeholder reviewed `FRIENDSHIP_SHADOW_EVIDENCE_REF`.

The hash and metadata are a reviewed binding prerequisite, not a shadow PASS;
the validator never reads a local evidence file and never invents evidence.
There is no second write, public Block/Unblock endpoint, public/internal gRPC
Ingress, mTLS, or dynamic route control in this deployment graph.

For an explicit Friendship rollback, apply only the reviewed ConfigMap patch
(the local-gate Kustomize overlay is retained as a renderable contract) and
restart the affected processes so the ConfigMap-backed environment is re-read:

```bash
kubectl -n farm-staging-system patch configmap farm-staging-platform \
  --type merge \
  --patch-file deploy/k8s/static-staging/overlays/local-gate-rollback/friendship-local-gate-merge-patch.json
kubectl -n farm-staging-system rollout restart deployment/farm-gate
kubectl -n farm-staging-system rollout restart deployment/farm-worker
kubectl -n farm-staging-system rollout restart statefulset/player-svr
kubectl -n farm-staging-system rollout restart statefulset/farm-svr
```

The rollback overlay sets `FRIENDSHIP_SERVICE_MODE=local_gate` explicitly; it
does not authorize a release cutover or change the static Holder route.

## Worker family-readiness contract

The base and W1 overlay use the exact current product configuration:
`WORKER_READINESS_CONTRACT_VERSION=worker-family-readiness-v1`,
`WORKER_READINESS_MAX_SCAN_AGE_V1=5s`, `WORKER_DUE_OBSERVE_LIMIT=4096`,
`GUARD_SYNC_WORKER_ENABLED=false`, and `MAIL_WORKER_ENABLED=false`.
Readiness remains `/readyz`; startup/liveness remain `/healthz`; static Holder
connections remain unchanged. There is no `WORKER_REQUIRED_FAMILIES` setting,
HPA, or Worker-kill PASS claim.

## External Redis prerequisites

Redis is not deployed here. The controlled service must provide the URI,
authentication/ACL, persistence and recovery policy, network route, and an
approved staging key namespace. The URI is injected only through the
`redis-url` Secret key. The profile is explicit:
`redis-physical-probe-v1` (the versioned local profile name), namespace `staging/z1`, authority layout `v1`,
workflow layout `v1`, hash version `1`, and 256 workflow buckets. The release
NetworkPolicy must be bound to the actual managed Redis CIDR. A local Redis,
anonymous URI, or copied development credential is not a staging prerequisite.
The CPU/memory requests and limits carry the explicit
`r2-p1-a-static-candidate-v1` capacity-profile label; they are sizing
candidates, not capacity conclusions.

## Rollback and evidence runbook

Record the validator JSON, image digests, route snapshot hash, config hash,
cluster identity, and formal URL with every change. Roll back Gate, Worker, and
H5 using their previous immutable digest overlay and `kubectl rollout undo`;
do not change the two Holder replica counts or silently substitute a different
route snapshot. A Holder rollback is valid only when its image understands the
same route schema, Holder IDs, boot IDs, authority layout, and workflow layout.
If the route generation changes, prepare and validate a new versioned snapshot
with the authority owner before applying it.

Required tests remain execution gates, not manifest claims:

- Gate RollingUpdate: start a real H5 session, update one Gate digest, verify
  the old Pod becomes NotReady before termination, receives the drain signal,
  and the session reconnects through another Ready Gate without duplicate
  business effects.
- Gate Drain: send SIGTERM to one Gate Pod and capture `/readyz` 503, drain
  timing, WebSocket close/reconnect, and backend route continuity.
- Worker kill: kill one Worker while a durable due item is eligible, then
  verify the second/replacement Worker claims from the DueIndex with no
  residual or duplicate side effect. `/readyz` is the product contract; the
  base candidate does not claim the full W1 family verdict.
- Same-Holder restart: restart `player-svr-0` and `farm-svr-0`, verify the
  Pod names, snapshot endpoints, boot IDs, route generation, authority fence,
  readiness transition, and post-restart actor behavior all match the static
  contract. Do not scale Holders or allow takeover during this test.

Actual Staging acceptance is blocked until cluster credentials and identity,
registry digests, Redis prerequisites, TLS/formal URL, the reviewed
zero-unexplained-shadow binding, and the formal Gate, Worker, Holder, Hero/Task
evidence are supplied. Rendered manifests and local validator output do not
count as `CP-K8S-STATIC-STAGING` acceptance.
