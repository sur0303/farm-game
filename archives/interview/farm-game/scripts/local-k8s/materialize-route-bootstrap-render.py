#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import tempfile
import time
from pathlib import Path

SCRIPT = Path(__file__).resolve()
REPO = SCRIPT.parents[2]
LAB = Path(os.environ.get("FARM_L2_LAB_ROOT", "/data/farm-l2-r2"))
RUN_ID = os.environ.get("ROUTE_RUN_ID", "")
NAMESPACE_ENV = os.environ.get("REDIS_NAMESPACE_ENVIRONMENT", "")
NAMESPACE_ZONE = os.environ.get("REDIS_NAMESPACE_ZONE", "z1")

if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", RUN_ID):
    raise SystemExit("materialize-route-bootstrap-render: ROUTE_RUN_ID is required")
if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,31}", NAMESPACE_ENV):
    raise SystemExit("materialize-route-bootstrap-render: REDIS_NAMESPACE_ENVIRONMENT is required")
if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,31}", NAMESPACE_ZONE):
    raise SystemExit("materialize-route-bootstrap-render: invalid REDIS_NAMESPACE_ZONE")

STATIC_ROOT = LAB / "runtime/static-init" / RUN_ID
STATIC_RECEIPTS = LAB / "receipts/static-init" / RUN_ID
ROUTE_PATH = STATIC_ROOT / "route-snapshot.json"
LEDGER_PATH = STATIC_ROOT / "exact-ledger.json"
IDENTITY_PATH = STATIC_ROOT / "identity.json"
RECEIPT_PATH = STATIC_RECEIPTS / "static-init-receipt.json"
TLS_RECEIPT = LAB / "receipts" / f"route-control-tls-{RUN_ID}.json"
IMAGE_RECEIPT = LAB / "receipts" / f"route-runtime-image-{RUN_ID}.json"
OUT_ROOT = LAB / "runtime" / f"route-bootstrap-{RUN_ID}"
TREE = OUT_ROOT / "deploy/k8s/static-staging"
OVERLAY = TREE / "overlays/local-capacity-r2/bootstrap"
RENDER_PATH = OUT_ROOT / "bootstrap-render.yaml"
OUT_RECEIPT = LAB / "receipts" / f"route-bootstrap-render-{RUN_ID}.json"


def load(path: Path) -> dict:
    return json.loads(path.read_text())


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def protected(path: Path) -> None:
    mode = stat.S_IMODE(path.stat().st_mode)
    if mode & 0o077:
        raise SystemExit(f"materialize-route-bootstrap-render: protected input mode rejected: {path.name}")

for path in (ROUTE_PATH, LEDGER_PATH, IDENTITY_PATH, RECEIPT_PATH, TLS_RECEIPT, IMAGE_RECEIPT):
    if not path.is_file() or path.stat().st_size == 0:
        raise SystemExit(f"materialize-route-bootstrap-render: missing input {path}")
protected(ROUTE_PATH)
protected(LEDGER_PATH)
protected(IDENTITY_PATH)

route = load(ROUTE_PATH)
ledger = load(LEDGER_PATH)
identity = load(IDENTITY_PATH)
receipt = load(RECEIPT_PATH)
tls_receipt = load(TLS_RECEIPT)
image_receipt = load(IMAGE_RECEIPT)
now_ms = int(time.time() * 1000)

if identity.get("run_id") != RUN_ID or receipt.get("run_id") != RUN_ID or tls_receipt.get("run_id") != RUN_ID or image_receipt.get("run_id") != RUN_ID:
    raise SystemExit("materialize-route-bootstrap-render: run identity mismatch")
if receipt.get("complete") is not True or receipt.get("bootstrap_materializable") is not True or receipt.get("activatable") is not False or receipt.get("durable_snapshot_committed") is not True:
    raise SystemExit("materialize-route-bootstrap-render: static/durable receipt is not bootstrap-materializable and product-inert")
if receipt.get("active_binding_authorized") is not False or receipt.get("binding_scope") != "LOCAL_FUNCTIONAL_ONLY" or receipt.get("company_redis_contacted") is not False:
    raise SystemExit("materialize-route-bootstrap-render: governance mismatch")
if receipt.get("minimum_lease_until_ms", 0) - now_ms < receipt.get("minimum_remaining_lease_ms", 0):
    raise SystemExit("materialize-route-bootstrap-render: static route startup budget expired")
if sha(ROUTE_PATH) != receipt.get("route_snapshot_sha256") or sha(LEDGER_PATH) != receipt.get("exact_ledger_sha256"):
    raise SystemExit("materialize-route-bootstrap-render: route or ledger hash mismatch")
if route.get("global_revision") != receipt.get("durable_snapshot_revision") or route.get("global_revision", 0) < 1:
    raise SystemExit("materialize-route-bootstrap-render: durable revision mismatch")
if ledger.get("complete") is not True or ledger.get("bootstrap_materializable") is not True or ledger.get("activatable") is not False or len(ledger.get("records", [])) != 2048 or route.get("player", {}).get("shard_count") != 1024 or route.get("farm", {}).get("shard_count") != 1024:
    raise SystemExit("materialize-route-bootstrap-render: inert bootstrap ledger or 1024/1024 completeness missing")
namespace_hash = hashlib.sha256(f"{NAMESPACE_ENV}/{NAMESPACE_ZONE}".encode()).hexdigest()
if namespace_hash != receipt.get("namespace_sha256"):
    raise SystemExit("materialize-route-bootstrap-render: Redis namespace mismatch")
nonce = identity.get("controller_nonce", "")
if hashlib.sha256(nonce.encode()).hexdigest() != receipt.get("controller_nonce_sha256"):
    raise SystemExit("materialize-route-bootstrap-render: ControllerNonce mismatch")
profile = identity.get("profile_id")
generation = identity.get("generation")
for holder in receipt.get("holders", []):
    expected = f"{holder['holder_id']}-{profile}-g{generation}-{nonce}"
    if holder.get("holder_boot_id") != expected:
        raise SystemExit("materialize-route-bootstrap-render: HolderBootID mismatch")
if tls_receipt.get("status") != "MATERIALIZED_LOCAL_FUNCTIONAL_ONLY" or tls_receipt.get("secret") != "farm-route-control-tls":
    raise SystemExit("materialize-route-bootstrap-render: route-control TLS receipt mismatch")
if image_receipt.get("status") != "PUBLISHED_LOCAL_ROUTE_RUNTIME_NOT_DEPLOYED":
    raise SystemExit("materialize-route-bootstrap-render: route runtime image not published")
image_ref = image_receipt.get("registry_ref", "")
if not re.fullmatch(r"127\.0\.0\.1:5001/farm/route-runtime@sha256:[0-9a-f]{64}", image_ref):
    raise SystemExit("materialize-route-bootstrap-render: route runtime image identity invalid")
source_head = subprocess.check_output(["git", "-C", str(REPO), "rev-parse", "HEAD"], text=True).strip()
if image_receipt.get("source_head") != source_head:
    raise SystemExit("materialize-route-bootstrap-render: route runtime source head mismatch")

if OUT_ROOT.exists():
    raise SystemExit("materialize-route-bootstrap-render: immutable output already exists")
TREE.parent.mkdir(parents=True, exist_ok=True)
shutil.copytree(REPO / "deploy/k8s/static-staging", TREE)
OVERLAY.mkdir(parents=True, exist_ok=False)
shutil.copy2(ROUTE_PATH, OVERLAY / "route-snapshot.json")
resources_dir = OVERLAY / "resources"
resources_dir.mkdir()
selected_resources = [
    "namespace.json",
    "player-statefulset.json", "player-service.json", "player-headless-service.json",
    "farm-statefulset.json", "farm-service.json", "farm-headless-service.json",
]
for resource in selected_resources:
    shutil.copy2(TREE / "base/resources" / resource, resources_dir / resource)
for resource in ("player-headless-service.json", "farm-headless-service.json"):
    headless = load(resources_dir / resource)
    headless["spec"]["publishNotReadyAddresses"] = True
    (resources_dir / resource).write_text(json.dumps(headless, sort_keys=True, indent=2) + "\n")

platform = load(TREE / "base/resources/platform-config.json")
data = platform.setdefault("data", {})
data.update({
    "FARM_APP_PROFILE": "showcase",
    "CAPACITY_PROFILE_VERSION": "local-route-bootstrap-r2-not-qualified",
    "FRIENDSHIP_SERVICE_MODE": "remote_player",
    "AUTHORITY_MEMBERSHIP_MODE": "composed",
    "ACTOR_ASSIGNMENT_MODE": "remote-watch",
    "REDIS_NAMESPACE_ENVIRONMENT": NAMESPACE_ENV,
    "REDIS_NAMESPACE_ZONE": NAMESPACE_ZONE,
    "REDIS_PHYSICAL_PROFILE_VERSION": "redis-physical-v1",
    "REDIS_AUTHORITY_LAYOUT_VERSION": "v1",
    "REDIS_WORKFLOW_LAYOUT_VERSION": "v1",
    "REDIS_WORKFLOW_HASH_VERSION": "1",
    "REDIS_WORKFLOW_BUCKET_COUNT": "256",
    "ACTOR_AUTHORITY_LEASE_TTL": "60s",
    "ACTOR_AUTHORITY_LEASE_SAFETY_MARGIN": "10s",
    "ACTOR_AUTHORITY_LEASE_RENEW_INTERVAL": "20s",
    "ACTOR_ROUTE_REALM": "classic-farm-local-l2-r2",
    "ROUTE_CONTROL_ADDR": "route-controller.farm-l2-r2.svc.cluster.local:9443",
    "ROUTE_CONTROL_CONTROLLER_ID": "route-controller",
    "ROUTE_CONTROL_SERVER_NAME": "route-controller.farm-l2-r2.svc.cluster.local",
    "ROUTE_CONTROL_SERVER_IDENTITY": "route-controller.farm-l2-r2.svc.cluster.local",
    "ROUTE_CONTROL_CLIENT_CERT_FILE": "/etc/farm/route-control/route-client.crt",
    "ROUTE_CONTROL_CLIENT_KEY_FILE": "/etc/farm/route-control/route-client.key",
    "ROUTE_CONTROL_SERVER_CA_FILE": "/etc/farm/route-control/ca.crt",
    "ROUTE_CONTROL_DIAL_TIMEOUT": "5s",
    "ROUTE_CONTROL_RPC_TIMEOUT": "5s",
    "ACTOR_ROUTE_MAX_RESYNC_ATTEMPTS": "32",
    "ACTOR_ROUTE_MAX_WATCH_FAILURES": "32",
    "ACTOR_ROUTE_RETRY_BACKOFF": "100ms",
    "ACTOR_ROUTE_MAX_RETRY_BACKOFF": "1s",
})
(OVERLAY / "platform-config.json").write_text(json.dumps(platform, sort_keys=True, indent=2) + "\n")

controller_boot = f"route-controller-{profile}-g{generation}-{nonce}"
template = (TREE / "overlays/local-capacity-r2/functional/route-controller.bootstrap-template.yaml").read_text()
(OVERLAY / "route-controller.yaml").write_text(template.replace("__CONTROLLER_BOOT__", controller_boot))

common_patch = [
    {"op": "replace", "path": "/spec/template/metadata/annotations/farm.io~1activation-status", "value": "LOCAL_BOOTSTRAP_REQUIRES_3TTL_PROOF"},
    {"op": "add", "path": "/spec/template/spec/securityContext/runAsUser", "value": 10001},
    {"op": "add", "path": "/spec/template/spec/securityContext/runAsGroup", "value": 10001},
    {"op": "add", "path": "/spec/template/spec/securityContext/fsGroup", "value": 10001},
    {"op": "add", "path": "/spec/template/spec/securityContext/fsGroupChangePolicy", "value": "OnRootMismatch"},
    {"op": "add", "path": "/spec/template/spec/containers/0/volumeMounts/-", "value": {"name": "route-control-tls", "mountPath": "/etc/farm/route-control", "readOnly": True}},
    {"op": "add", "path": "/spec/template/spec/volumes/-", "value": {"name": "route-control-tls", "secret": {"secretName": "farm-route-control-tls", "defaultMode": 288}}},
]
holder_identity = "holder-control.farm-l2-r2.svc.cluster.local"
for kind, patch_name, holder_offset, logic_port, control_port in (
    ("player", "player-patch.json", 0, 9100, 19100),
    ("farm", "farm-patch.json", 2, 9090, 19101),
):
    patch = load(TREE / f"overlays/local-capacity-r2/functional/{patch_name}")
    patch.extend(common_patch)
    # Kubernetes expands $(POD_NAME) from the earlier Downward API env entry.
    patch.append({"op": "replace", "path": "/spec/template/spec/containers/0/env/5/value", "value": f"$(POD_NAME)-{profile}-g{generation}-{nonce}"})
    prefix = "PLAYER" if kind == "player" else "FARM"
    service = "player-svr" if kind == "player" else "farm-svr"
    env = {
        f"{prefix}_LOGIC_ENDPOINT": f"$(POD_NAME).{service}-headless.farm-l2-r2.svc.cluster.local:{logic_port}",
        f"{prefix}_HOLDER_ENDPOINT": f"$(POD_NAME).{service}-headless.farm-l2-r2.svc.cluster.local:{control_port}",
        f"{prefix}_HOLDER_TLS_DNS_IDENTITY": holder_identity,
        f"{prefix}_HOLDER_CONTROL_LISTEN": f":{control_port}",
        f"{prefix}_HOLDER_TLS_CERT_FILE": "/etc/farm/route-control/holder-server.crt",
        f"{prefix}_HOLDER_TLS_KEY_FILE": "/etc/farm/route-control/holder-server.key",
        f"{prefix}_HOLDER_CLIENT_CA_FILE": "/etc/farm/route-control/ca.crt",
        "HOLDER_CONTROL_CONTROLLER_IDENTITY": "route-controller.farm-l2-r2.svc.cluster.local",
        "HOLDER_ALLOW_INITIAL_SERVING": "true",
    }
    for name, value in env.items():
        patch.append({"op": "add", "path": "/spec/template/spec/containers/0/env/-", "value": {"name": name, "value": value}})
    patch.append({"op": "add", "path": "/spec/template/spec/containers/0/ports/-", "value": {"name": "holder-mtls", "containerPort": control_port, "protocol": "TCP"}})
    patch.append({"op": "add", "path": "/spec/template/spec/initContainers", "value": [{
        "name": "route-bootstrap-dns-barrier",
        "image": "registry.example.invalid/farm-game-runtime:static-staging-candidate",
        "imagePullPolicy": "IfNotPresent",
        "command": ["/bin/sh", "-c", """set -eu
for host in player-svr-0.player-svr-headless.farm-l2-r2.svc.cluster.local player-svr-1.player-svr-headless.farm-l2-r2.svc.cluster.local farm-svr-0.farm-svr-headless.farm-l2-r2.svc.cluster.local farm-svr-1.farm-svr-headless.farm-l2-r2.svc.cluster.local; do
  attempt=0
  until nslookup \"$host\" >/dev/null 2>&1; do
    attempt=$((attempt+1))
    [ \"$attempt\" -lt 30 ] || exit 70
    sleep 1
  done
done
"""],
        "resources": {"requests": {"cpu": "2", "memory": "64Mi"}, "limits": {"cpu": "2", "memory": "64Mi"}},
        "securityContext": {"allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}, "readOnlyRootFilesystem": True},
    }]})
    (OVERLAY / patch_name).write_text(json.dumps(patch, sort_keys=True, indent=2) + "\n")

backend_name, backend_digest = image_ref.split("@", 1)
kustomization = f"""apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: farm-l2-r2
resources:
  - resources/namespace.json
  - platform-config.json
  - resources/player-statefulset.json
  - resources/player-service.json
  - resources/player-headless-service.json
  - resources/farm-statefulset.json
  - resources/farm-service.json
  - resources/farm-headless-service.json
  - route-controller.yaml
images:
  - name: registry.example.invalid/farm-game-runtime
    newName: {backend_name}
    digest: {backend_digest}
configMapGenerator:
  - name: farm-static-route
    files:
      - route-snapshot.json=route-snapshot.json
    options:
      disableNameSuffixHash: true
patches:
  - target:
      kind: Namespace
      name: farm-staging-system
    patch: |-
      - op: replace
        path: /metadata/name
        value: farm-l2-r2
  - target:
      kind: StatefulSet
      name: player-svr
    path: player-patch.json
  - target:
      kind: StatefulSet
      name: farm-svr
    path: farm-patch.json
"""
(OVERLAY / "kustomization.yaml").write_text(kustomization)
render = subprocess.run(["kubectl", "kustomize", str(OVERLAY)], check=True, stdout=subprocess.PIPE).stdout
for forbidden in (b"__CONTROLLER_BOOT__", b"sha256:000000", b"static-route-v1", b"RENDER_ONLY_NOT_AUTHORITY_INITIALIZED"):
    if forbidden in render:
        raise SystemExit(f"materialize-route-bootstrap-render: forbidden marker {forbidden!r}")
if b"ACTOR_ASSIGNMENT_MODE: remote-watch" not in render or render.count(b"farm-route-control-tls") < 3:
    raise SystemExit("materialize-route-bootstrap-render: remote route composition missing")
for expected in (
    b'ACTOR_ROUTE_MAX_RESYNC_ATTEMPTS: "32"', b'ACTOR_ROUTE_MAX_WATCH_FAILURES: "32"',
    b"ACTOR_ROUTE_RETRY_BACKOFF: 100ms", b"ACTOR_ROUTE_MAX_RETRY_BACKOFF: 1s",
):
    if expected not in render:
        raise SystemExit(f"materialize-route-bootstrap-render: restart-safe retry profile missing {expected!r}")
if render.count(b'cpu: "2"') != 10 or b'cpu: "1"' in render:
    raise SystemExit("materialize-route-bootstrap-render: full physical-core SMT resource contract missing")
if render.count(b"runAsUser: 10001") != 3 or render.count(b"defaultMode: 288") != 3 or b"ROUTE_CONTROLLER_HOLDER_CLIENT_CERT" not in render:
    raise SystemExit("materialize-route-bootstrap-render: non-root route-control TLS access contract missing")
if render.count(b"publishNotReadyAddresses: true") != 2 or render.count(b"name: route-bootstrap-dns-barrier") != 2:
    raise SystemExit("materialize-route-bootstrap-render: StatefulSet bootstrap discovery contract missing")
if render.count(b"name: HOLDER_ALLOW_INITIAL_SERVING") != 2 or render.count(b'value: "true"') < 2:
    raise SystemExit("materialize-route-bootstrap-render: explicit static-bootstrap Holder admission missing")
RENDER_PATH.write_bytes(render)
os.chmod(RENDER_PATH, 0o600)

value = {
    "schema_version": "farm-local-route-bootstrap-render-receipt.v1",
    "status": "BOOTSTRAP_RENDER_FRESH_NOT_3TTL_PROVEN",
    "run_id": RUN_ID,
    "source_head": source_head,
    "route_snapshot_sha256": sha(ROUTE_PATH),
    "static_init_receipt_sha256": sha(RECEIPT_PATH),
    "route_tls_receipt_sha256": sha(TLS_RECEIPT),
    "runtime_image_ref": image_ref,
    "render_sha256": hashlib.sha256(render).hexdigest(),
    "holder_count": 4,
    "full_physical_core_smt_resources": True,
    "logical_cpus_per_runtime_pod": 2,
    "headless_publish_not_ready_bootstrap": True,
    "holder_dns_startup_barrier": True,
    "holder_initial_serving_admission": "STATIC_BOOTSTRAP_EXPLICIT",
    "remote_watch_reconnect_profile": {
        "max_resync_attempts": 32,
        "max_watch_failures": 32,
        "retry_backoff_ms": 100,
        "max_retry_backoff_ms": 1000,
        "minimum_backoff_horizon_ms": 28500,
    },
    "player_shards": 1024,
    "farm_shards": 1024,
    "cluster_applied": False,
    "three_ttl_proven": False,
    "activatable": False,
    "active_binding_authorized": False,
    "company_redis_contacted": False,
    "capacity_constant_eligible": False,
    "supports_30m_dau": False,
}
OUT_RECEIPT.parent.mkdir(parents=True, exist_ok=True)
fd, temporary = tempfile.mkstemp(prefix=".route-bootstrap-render-", dir=OUT_RECEIPT.parent, text=True)
with os.fdopen(fd, "w") as out:
    json.dump(value, out, sort_keys=True, indent=2)
    out.write("\n")
    out.flush()
    os.fsync(out.fileno())
os.replace(temporary, OUT_RECEIPT)
os.chmod(OUT_RECEIPT, 0o640)
print(json.dumps(value, sort_keys=True, indent=2))
