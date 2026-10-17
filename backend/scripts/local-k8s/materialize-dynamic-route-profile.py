#!/usr/bin/env python3
from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import secrets
import stat
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve()
REPO = SCRIPT.parents[2]
LAB = Path(os.environ.get("FARM_L2_LAB_ROOT", "/data/farm-l2-r2"))
RUN_ID = os.environ.get("ROUTE_RUN_ID", "")
REDIS_ENV = os.environ.get("REDIS_NAMESPACE_ENVIRONMENT", "")
REDIS_ZONE = os.environ.get("REDIS_NAMESPACE_ZONE", "z1")
NAMESPACE = "farm-route-dynamic"
ROUTE_REALM = "classic-farm-dynamic-local"
SCALEPLAN_CONTROLLER_SERVICE_ACCOUNT = "scaleplan-controller"
PLAYER_HOLDER_SERVICE_ACCOUNT = "player-holder"
FARM_HOLDER_SERVICE_ACCOUNT = "farm-holder"
HOLDER_CONTROL_TOKEN_AUDIENCE = "classic-farm-holder-control"
HOLDER_CONTROL_TOKEN_EXPIRATION_SECONDS = 600
HOLDER_CONTROL_TOKEN_MOUNT = "/var/run/secrets/farm/holder-control"
HOLDER_CONTROL_TOKEN_FILE = f"{HOLDER_CONTROL_TOKEN_MOUNT}/token"
BASE = REPO / "deploy/k8s/static-staging/base/resources"
OUT = LAB / "runtime" / f"dynamic-route-{RUN_ID}"
RECEIPT = LAB / "receipts" / f"dynamic-route-profile-{RUN_ID}.json"
IMAGE_RECEIPT = LAB / "receipts" / f"route-runtime-image-{RUN_ID}.json"
H5_IMAGE_RECEIPT = LAB / "receipts" / f"dynamic-h5-image-{RUN_ID}.json"


def choice_env(name: str, default: str, allowed: set[str]) -> str:
    value = os.environ.get(name, default).strip().lower()
    if value not in allowed:
        raise SystemExit(
            f"materialize-dynamic-route-profile: {name} must be one of {sorted(allowed)}"
        )
    return value


def bool_env(name: str, default: bool) -> bool:
    raw = os.environ.get(name, "true" if default else "false").strip().lower()
    if raw in {"1", "true"}:
        return True
    if raw in {"0", "false"}:
        return False
    raise SystemExit(f"materialize-dynamic-route-profile: {name} must be boolean")


def bounded_int_env(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = os.environ.get(name, str(default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise SystemExit(
            f"materialize-dynamic-route-profile: {name} must be an integer"
        ) from exc
    if value < minimum or value > maximum:
        raise SystemExit(
            f"materialize-dynamic-route-profile: {name} must be between {minimum} and {maximum}"
        )
    return value


SCALEPLAN_MODE = choice_env("SCALEPLAN_MODE", "off", {"off", "manual", "auto"})
HOLDER_DISCOVERY_MODE = choice_env(
    "HOLDER_DISCOVERY_MODE",
    "legacy" if SCALEPLAN_MODE == "off" else "k8s-canary",
    {"legacy", "shadow", "k8s-canary", "k8s"},
)
SCALEPLAN_TOKENREVIEW_RBAC = bool_env(
    "SCALEPLAN_TOKENREVIEW_RBAC", HOLDER_DISCOVERY_MODE != "legacy"
)
if SCALEPLAN_MODE == "off" and SCALEPLAN_TOKENREVIEW_RBAC:
    raise SystemExit(
        "materialize-dynamic-route-profile: off mode cannot enable TokenReview RBAC"
    )
if SCALEPLAN_MODE == "off" and HOLDER_DISCOVERY_MODE != "legacy":
    raise SystemExit(
        "materialize-dynamic-route-profile: off mode requires legacy holder discovery"
    )
if HOLDER_DISCOVERY_MODE != "legacy" and not SCALEPLAN_TOKENREVIEW_RBAC:
    raise SystemExit(
        "materialize-dynamic-route-profile: Kubernetes holder discovery requires TokenReview RBAC"
    )
SCALEPLAN_REALM = os.environ.get("SCALEPLAN_REALM", ROUTE_REALM).strip()
if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,127}", SCALEPLAN_REALM):
    raise SystemExit("materialize-dynamic-route-profile: invalid SCALEPLAN_REALM")
if SCALEPLAN_REALM != ROUTE_REALM:
    raise SystemExit(
        "materialize-dynamic-route-profile: SCALEPLAN_REALM must match the route realm"
    )
SCALEPLAN_PUBLIC_DOMAIN = os.environ.get(
    "SCALEPLAN_PUBLIC_DOMAIN", "farm.dynamic.local"
).strip().lower()
if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{0,252}", SCALEPLAN_PUBLIC_DOMAIN):
    raise SystemExit("materialize-dynamic-route-profile: invalid SCALEPLAN_PUBLIC_DOMAIN")
if SCALEPLAN_PUBLIC_DOMAIN != "farm.dynamic.local":
    raise SystemExit(
        "materialize-dynamic-route-profile: SCALEPLAN_PUBLIC_DOMAIN must match the route profile"
    )
SCALEPLAN_AUTO_RECONCILE_INTERVAL = os.environ.get(
    "SCALEPLAN_AUTO_RECONCILE_INTERVAL", "5s"
).strip()
if not re.fullmatch(r"[1-9][0-9]*(ms|s|m)", SCALEPLAN_AUTO_RECONCILE_INTERVAL):
    raise SystemExit(
        "materialize-dynamic-route-profile: invalid SCALEPLAN_AUTO_RECONCILE_INTERVAL"
    )
ROUTE_FAILOVER_RECONCILE_INTERVAL = os.environ.get(
    "ROUTE_FAILOVER_RECONCILE_INTERVAL", "1s"
).strip()
if not re.fullmatch(r"[1-9][0-9]*(ms|s|m)", ROUTE_FAILOVER_RECONCILE_INTERVAL):
    raise SystemExit(
        "materialize-dynamic-route-profile: invalid ROUTE_FAILOVER_RECONCILE_INTERVAL"
    )
ROUTE_FAILOVER_MAX_PLANS_PER_PASS = bounded_int_env(
    "ROUTE_FAILOVER_MAX_PLANS_PER_PASS", 32, 1, 1024
)
ROUTE_FAILOVER_MAX_AUTHORITY_READS_PER_PASS = bounded_int_env(
    "ROUTE_FAILOVER_MAX_AUTHORITY_READS_PER_PASS", 128, 1, 4096
)
if ROUTE_FAILOVER_MAX_AUTHORITY_READS_PER_PASS < ROUTE_FAILOVER_MAX_PLANS_PER_PASS:
    raise SystemExit(
        "materialize-dynamic-route-profile: ROUTE_FAILOVER_MAX_AUTHORITY_READS_PER_PASS "
        "must be at least ROUTE_FAILOVER_MAX_PLANS_PER_PASS"
    )
SCALEPLAN_SAFE_DESIRED_ANNOTATION = os.environ.get(
    "SCALEPLAN_SAFE_DESIRED_ANNOTATION",
    "scaleplan.farm/safe-desired-replicas",
).strip()
if SCALEPLAN_SAFE_DESIRED_ANNOTATION != "scaleplan.farm/safe-desired-replicas":
    raise SystemExit(
        "materialize-dynamic-route-profile: unsupported SCALEPLAN_SAFE_DESIRED_ANNOTATION"
    )
SCALEPLAN_ROLLOUT_ENABLED = bool_env("SCALEPLAN_ROLLOUT_ENABLED", False)
if SCALEPLAN_ROLLOUT_ENABLED and SCALEPLAN_MODE != "auto":
    raise SystemExit(
        "materialize-dynamic-route-profile: managed rollout requires SCALEPLAN_MODE=auto"
    )
SCALEPLAN_ROLLOUT_ANNOTATION = os.environ.get(
    "SCALEPLAN_ROLLOUT_ANNOTATION",
    "scaleplan.farm/managed-rollout-state",
).strip()
if SCALEPLAN_ROLLOUT_ANNOTATION != "scaleplan.farm/managed-rollout-state":
    raise SystemExit(
        "materialize-dynamic-route-profile: unsupported SCALEPLAN_ROLLOUT_ANNOTATION"
    )
SCALEPLAN_ROLLOUT_INTENT_ANNOTATION = os.environ.get(
    "SCALEPLAN_ROLLOUT_INTENT_ANNOTATION",
    "scaleplan.farm/rollout-intent",
).strip()
if SCALEPLAN_ROLLOUT_INTENT_ANNOTATION != "scaleplan.farm/rollout-intent":
    raise SystemExit(
        "materialize-dynamic-route-profile: unsupported SCALEPLAN_ROLLOUT_INTENT_ANNOTATION"
    )
SCALEPLAN_WORKLOAD_ALLOWLIST = (
    {
        "realm": SCALEPLAN_REALM,
        "domain": "PLAYER",
        "namespace": NAMESPACE,
        "kind": "StatefulSet",
        "name": "player-svr",
    },
    {
        "realm": SCALEPLAN_REALM,
        "domain": "FARM",
        "namespace": NAMESPACE,
        "kind": "StatefulSet",
        "name": "farm-svr",
    },
)
SCALEPLAN_ROUTE_DOMAINS = tuple(entry["domain"] for entry in SCALEPLAN_WORKLOAD_ALLOWLIST)
SCALEPLAN_WORKLOAD_ALLOWLIST_JSON = json.dumps(
    list(SCALEPLAN_WORKLOAD_ALLOWLIST), sort_keys=True, separators=(",", ":")
)
SCALEPLAN_CONFIG_DOCUMENT = {
    "schema_version": "scaleplan-render-config.v1",
    "mode": SCALEPLAN_MODE,
    "realm": SCALEPLAN_REALM,
    "route_domains": list(SCALEPLAN_ROUTE_DOMAINS),
    "public_domain": SCALEPLAN_PUBLIC_DOMAIN,
    "workload_allowlist": list(SCALEPLAN_WORKLOAD_ALLOWLIST),
    "tokenreview_rbac": SCALEPLAN_TOKENREVIEW_RBAC,
    "holder_discovery_mode": HOLDER_DISCOVERY_MODE,
    "holder_identity_transport": "K8S_INFORMER_HOLDERCONTROL_TOKENREVIEW_V1",
    "auto_reconcile_interval": SCALEPLAN_AUTO_RECONCILE_INTERVAL,
    "route_failover_reconcile_interval": ROUTE_FAILOVER_RECONCILE_INTERVAL,
    "route_failover_max_plans_per_pass": ROUTE_FAILOVER_MAX_PLANS_PER_PASS,
    "route_failover_max_authority_reads_per_pass": ROUTE_FAILOVER_MAX_AUTHORITY_READS_PER_PASS,
    "safe_desired_annotation": SCALEPLAN_SAFE_DESIRED_ANNOTATION,
    "rollout_enabled": SCALEPLAN_ROLLOUT_ENABLED,
    "rollout_annotation": SCALEPLAN_ROLLOUT_ANNOTATION,
    "rollout_intent_annotation": SCALEPLAN_ROLLOUT_INTENT_ANNOTATION,
}
SCALEPLAN_CONFIG_BYTES = json.dumps(
    SCALEPLAN_CONFIG_DOCUMENT, sort_keys=True, separators=(",", ":")
).encode()
SCALEPLAN_CONFIG_HASH = hashlib.sha256(SCALEPLAN_CONFIG_BYTES).hexdigest()


SCHEDULE_DUE_INDEX_MODE = choice_env(
    "FARM_SCHEDULE_DUE_INDEX_MODE",
    "dedicated_only",
    {"shared", "dual_shadow", "dedicated_fallback", "dedicated_only"},
)
SCHEDULE_DUE_INDEX_DEDICATED_READY = bool_env(
    "FARM_SCHEDULE_DUE_INDEX_DEDICATED_READY",
    True,
)
if SCHEDULE_DUE_INDEX_MODE == "dedicated_only" and not SCHEDULE_DUE_INDEX_DEDICATED_READY:
    raise SystemExit(
        "materialize-dynamic-route-profile: dedicated_only requires dedicated readiness"
    )
if SCHEDULE_DUE_INDEX_MODE != "dedicated_only":
    raise SystemExit(
        "materialize-dynamic-route-profile: a new product run requires dedicated_only; "
        "legacy index modes are migration/rollback only"
    )
# showcase is the product posture and forbids every authentication bypass. A
# capacity run needs the load-test token the driver signs with, so it must be
# materialized under a non-showcase profile and can never carry an acceptance
# verdict.
APP_PROFILE = choice_env("FARM_APP_PROFILE", "showcase", {"local", "test", "showcase"})
LOAD_TEST_LOGIN_ENABLED = APP_PROFILE != "showcase"
OPPORTUNITY_WAKEUP_ATTACH_ENABLED = bool_env(
    "FARM_OPPORTUNITY_WAKEUP_ATTACH_ENABLED",
    True,
)
OPPORTUNITY_HEADER_READ_MODE = choice_env(
    "FARM_OPPORTUNITY_HEADER_READ_MODE",
    "full",
    {"full", "narrow-shadow", "narrow"},
)
OPPORTUNITY_WAKEUP_ENABLED = bool_env(
    "FARM_OPPORTUNITY_WAKEUP_ENABLED",
    True,
)
OPPORTUNITY_WAKEUP_ASSIGNMENT_MODE = choice_env(
    "FARM_OPPORTUNITY_WAKEUP_ASSIGNMENT_MODE",
    "route",
    {"static", "route"},
)
OPPORTUNITY_HINT_TRANSPORT = choice_env(
    "FARM_OPPORTUNITY_HINT_TRANSPORT",
    "pubsub",
    {"pubsub", "shadow-direct", "direct"},
)
OPPORTUNITY_MODE_PAIR = (
    OPPORTUNITY_HEADER_READ_MODE,
    OPPORTUNITY_HINT_TRANSPORT,
)
OPPORTUNITY_MODE_LADDER = (
    ("full", "pubsub"),
    ("narrow-shadow", "shadow-direct"),
    ("narrow", "direct"),
)
OPPORTUNITY_LADDER_STAGES = ("compatibility", "shadow", "final")
if (
    not OPPORTUNITY_WAKEUP_ATTACH_ENABLED
    or not OPPORTUNITY_WAKEUP_ENABLED
    or OPPORTUNITY_WAKEUP_ASSIGNMENT_MODE != "route"
):
    raise SystemExit(
        "materialize-dynamic-route-profile: a new product run requires active route-assigned wakeup"
    )
# A product run earns the final modes one rung at a time, and every rung is
# witnessed by its own traffic receipt. A capacity run measures the shape the
# product ends in, so it may be born on any rung -- it forfeits the acceptance
# verdict either way, and walking the ladder would only measure modes nobody
# ships.
if OPPORTUNITY_MODE_PAIR not in OPPORTUNITY_MODE_LADDER:
    raise SystemExit(
        "materialize-dynamic-route-profile: opportunity modes must name a ladder rung"
    )
if not LOAD_TEST_LOGIN_ENABLED and OPPORTUNITY_MODE_PAIR != OPPORTUNITY_MODE_LADDER[0]:
    raise SystemExit(
        "materialize-dynamic-route-profile: a new product run must start in full/pubsub"
    )
OPPORTUNITY_STAGE = OPPORTUNITY_LADDER_STAGES[
    OPPORTUNITY_MODE_LADDER.index(OPPORTUNITY_MODE_PAIR)
]
LADDER_WALKED_FROM_FIRST_RUNG = OPPORTUNITY_MODE_PAIR == OPPORTUNITY_MODE_LADDER[0]
# The holder ceiling is the product posture by default. A capacity run looks for
# the ceiling the service has, so it must be able to raise the one the platform
# imposes -- otherwise the run measures the limit rather than the software. Any
# raise is a deviation and belongs in the run's deviation receipt.


def k8s_cpu(name: str, default: str) -> str:
    value = os.environ.get(name, default).strip()
    if not re.fullmatch(r"[0-9]+(m|)", value):
        raise SystemExit(f"materialize-dynamic-route-profile: {name} must be a Kubernetes cpu quantity")
    return value


def k8s_memory(name: str, default: str) -> str:
    value = os.environ.get(name, default).strip()
    if not re.fullmatch(r"[0-9]+(Mi|Gi)", value):
        raise SystemExit(f"materialize-dynamic-route-profile: {name} must be a Kubernetes memory quantity")
    return value


HOLDER_CPU_LIMIT = k8s_cpu("FARM_HOLDER_CPU_LIMIT", "1")
HOLDER_MEMORY_LIMIT = k8s_memory("FARM_HOLDER_MEMORY_LIMIT", "1Gi")
# Packed cold-join hits one farm-svr process. Allow that StatefulSet to
# raise without also giving idle player-svr the same box.
PLAYER_SVR_CPU_LIMIT = k8s_cpu("PLAYER_SVR_CPU_LIMIT", HOLDER_CPU_LIMIT)
PLAYER_SVR_MEMORY_LIMIT = k8s_memory("PLAYER_SVR_MEMORY_LIMIT", HOLDER_MEMORY_LIMIT)
FARM_SVR_CPU_LIMIT = k8s_cpu("FARM_SVR_CPU_LIMIT", HOLDER_CPU_LIMIT)
FARM_SVR_MEMORY_LIMIT = k8s_memory("FARM_SVR_MEMORY_LIMIT", HOLDER_MEMORY_LIMIT)
# Gate connection buffers OOM at the old 1/768Mi lab default. Capacity runs
# raise this the same way holders do: env overlay, recorded as a deviation.
GATE_CPU_LIMIT = k8s_cpu("FARM_GATE_CPU_LIMIT", "4")
GATE_MEMORY_LIMIT = k8s_memory("FARM_GATE_MEMORY_LIMIT", "4Gi")
# Product per-source-IP abuse ceilings (10/s, burst 20, 64 conns, 1MiB)
# admit about 60 WS handshakes from one generator host, then S0 fails
# with websocket bad handshake. Raise the per-IP buckets so the run
# measures the service. Leave pod-wide preauth caps at product values.
GATE_WS_RATE_PER_SEC = os.environ.get("GATE_WS_RATE_PER_SEC", "5000").strip()
GATE_WS_RATE_BURST = os.environ.get("GATE_WS_RATE_BURST", "20000").strip()
GATE_PREAUTH_MAX_CONNECTIONS_PER_IP = os.environ.get(
    "GATE_PREAUTH_MAX_CONNECTIONS_PER_IP", "16384"
).strip()
GATE_PREAUTH_MAX_BYTES_PER_IP = os.environ.get(
    "GATE_PREAUTH_MAX_BYTES_PER_IP", "1073741824"
).strip()
if not re.fullmatch(r"[0-9]+", GATE_WS_RATE_PER_SEC):
    raise SystemExit("materialize-dynamic-route-profile: GATE_WS_RATE_PER_SEC must be an integer")
if not re.fullmatch(r"[0-9]+", GATE_WS_RATE_BURST):
    raise SystemExit("materialize-dynamic-route-profile: GATE_WS_RATE_BURST must be an integer")
if not re.fullmatch(r"[0-9]+", GATE_PREAUTH_MAX_CONNECTIONS_PER_IP):
    raise SystemExit("materialize-dynamic-route-profile: GATE_PREAUTH_MAX_CONNECTIONS_PER_IP must be an integer")
if not re.fullmatch(r"[0-9]+", GATE_PREAUTH_MAX_BYTES_PER_IP):
    raise SystemExit("materialize-dynamic-route-profile: GATE_PREAUTH_MAX_BYTES_PER_IP must be an integer")
# Product farm-svr activation defaults (1000/s, 64 concurrent loads) are a
# storm gate. A fill run that leaves them on measures the token bucket, not
# the 2c/4Gi pod. Opt in with env; 0 rate means unlimited. Concurrent/pending
# cannot be 0 (binary treats 0 as "use default 64/256").
ACTIVATION_RATE_PER_SEC = os.environ.get("ACTIVATION_RATE_PER_SEC", "").strip()
MAX_CONCURRENT_ACTOR_LOADS = os.environ.get("MAX_CONCURRENT_ACTOR_LOADS", "").strip()
MAX_PENDING_ACTOR_LOADS = os.environ.get("MAX_PENDING_ACTOR_LOADS", "").strip()
if ACTIVATION_RATE_PER_SEC and not re.fullmatch(r"[0-9]+([.][0-9]+)?", ACTIVATION_RATE_PER_SEC):
    raise SystemExit("materialize-dynamic-route-profile: ACTIVATION_RATE_PER_SEC must be >= 0")
if MAX_CONCURRENT_ACTOR_LOADS and not re.fullmatch(r"[1-9][0-9]*", MAX_CONCURRENT_ACTOR_LOADS):
    raise SystemExit("materialize-dynamic-route-profile: MAX_CONCURRENT_ACTOR_LOADS must be a positive integer")
if MAX_PENDING_ACTOR_LOADS and not re.fullmatch(r"[1-9][0-9]*", MAX_PENDING_ACTOR_LOADS):
    raise SystemExit("materialize-dynamic-route-profile: MAX_PENDING_ACTOR_LOADS must be a positive integer")
HOLDER_PPROF_ENABLED = os.environ.get("HOLDER_PPROF_ENABLED", "").strip()
GATE_PPROF_ENABLED = os.environ.get("GATE_PPROF_ENABLED", "").strip()
if HOLDER_PPROF_ENABLED and HOLDER_PPROF_ENABLED not in {"0", "1"}:
    raise SystemExit("materialize-dynamic-route-profile: HOLDER_PPROF_ENABLED must be 0 or 1")
if GATE_PPROF_ENABLED and GATE_PPROF_ENABLED not in {"0", "1"}:
    raise SystemExit("materialize-dynamic-route-profile: GATE_PPROF_ENABLED must be 0 or 1")

if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", RUN_ID):
    raise SystemExit("materialize-dynamic-route-profile: ROUTE_RUN_ID is required")
if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,31}", REDIS_ENV):
    raise SystemExit("materialize-dynamic-route-profile: REDIS_NAMESPACE_ENVIRONMENT is required")
if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,31}", REDIS_ZONE):
    raise SystemExit("materialize-dynamic-route-profile: invalid REDIS_NAMESPACE_ZONE")
if OUT.exists() or RECEIPT.exists():
    raise SystemExit("materialize-dynamic-route-profile: immutable output already exists")
for prior_path in (LAB / "receipts").glob("dynamic-route-profile-*.json"):
    try:
        prior = json.loads(prior_path.read_text())
    except (OSError, json.JSONDecodeError):
        continue
    if (prior.get("redis_namespace_environment"), prior.get("redis_namespace_zone")) == (REDIS_ENV, REDIS_ZONE):
        raise SystemExit("materialize-dynamic-route-profile: Redis namespace tuple was already materialized")


def load(path: Path) -> dict:
    return json.loads(path.read_text())


def sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def write_exclusive(path: Path, payload: bytes, mode: int) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        path.unlink(missing_ok=True)
        raise


def env_set(container: dict, name: str, *, value: str | None = None, value_from: dict | None = None) -> None:
    env = container.setdefault("env", [])
    env[:] = [entry for entry in env if entry.get("name") != name]
    entry: dict = {"name": name}
    if value_from is not None:
        entry["valueFrom"] = value_from
    else:
        entry["value"] = value
    env.append(entry)


def remove_static_route(pod: dict) -> None:
    for container in pod.get("containers", []):
        container["volumeMounts"] = [mount for mount in container.get("volumeMounts", []) if mount.get("name") != "farm-static-route"]
    pod["volumes"] = [volume for volume in pod.get("volumes", []) if volume.get("name") != "farm-static-route"]


def replace_secret_refs(pod: dict) -> None:
    for container in pod.get("containers", []):
        for entry in container.get("env", []):
            ref = entry.get("valueFrom", {}).get("secretKeyRef", {})
            if ref.get("name") == "farm-staging-secrets":
                ref["name"] = "farm-dynamic-secrets"


def add_mount(container: dict, name: str, path: str, read_only: bool) -> None:
    mounts = container.setdefault("volumeMounts", [])
    if not any(mount.get("name") == name for mount in mounts):
        mounts.append({"name": name, "mountPath": path, "readOnly": read_only})


def add_volume(pod: dict, value: dict) -> None:
    volumes = pod.setdefault("volumes", [])
    if not any(volume.get("name") == value["name"] for volume in volumes):
        volumes.append(value)


def dynamic_metadata(obj: dict) -> dict:
    if obj.get("kind") != "Namespace":
        obj.setdefault("metadata", {})["namespace"] = NAMESPACE
    labels = obj.setdefault("metadata", {}).setdefault("labels", {})
    labels["farm.io/runtime-profile"] = "route-migration-4x4x4"
    return obj


for receipt_path, label in (
    (IMAGE_RECEIPT, "runtime image"),
    (H5_IMAGE_RECEIPT, "exact H5 image"),
):
    if not receipt_path.is_file():
        raise SystemExit(f"materialize-dynamic-route-profile: {label} receipt missing")

image = load(IMAGE_RECEIPT)
h5_image = load(H5_IMAGE_RECEIPT)
head = subprocess.check_output(["git", "-C", str(REPO), "rev-parse", "HEAD"], text=True, timeout=30).strip()
source_worktree_clean = not subprocess.check_output(["git", "-C", str(REPO), "status", "--porcelain"], text=True, timeout=30).strip()
if image.get("status") != "PUBLISHED_LOCAL_ROUTE_RUNTIME_NOT_DEPLOYED" or image.get("run_id") != RUN_ID or image.get("source_head") != head:
    raise SystemExit("materialize-dynamic-route-profile: runtime image receipt mismatch")
runtime_ref = image.get("registry_ref", "")
h5_ref = h5_image.get("registry_ref", "")
if not re.fullmatch(r"127\.0\.0\.1:5001/farm/route-runtime@sha256:[0-9a-f]{64}", runtime_ref):
    raise SystemExit("materialize-dynamic-route-profile: runtime image ref invalid")
if (
    h5_image.get("status") != "PUBLISHED_LOCAL_DYNAMIC_H5_NOT_DEPLOYED"
    or h5_image.get("run_id") != RUN_ID
    or h5_image.get("source_commit") != "fc7be57174e9ff4f18e6dfeb5fc1610d681172cf"
    or not re.fullmatch(r"[0-9a-f]{40}", h5_image.get("source_tree", ""))
    or h5_image.get("build_mode") != "web"
    or h5_image.get("cluster_applied") is not False
    or h5_image.get("company_redis_contacted") is not False
    or h5_image.get("capacity_eligible") is not False
    or not re.fullmatch(r"127\.0\.0\.1:5001/farm/h5@sha256:[0-9a-f]{64}", h5_ref)
):
    raise SystemExit("materialize-dynamic-route-profile: exact H5 image receipt mismatch")

initial_controller_boot = f"route-controller-initial-{RUN_ID}-{secrets.token_hex(8)}"
bootstrap_controller_boot = f"route-controller-bootstrap-{RUN_ID}-{secrets.token_hex(8)}"
final_controller_boot = f"route-controller-final-{RUN_ID}-{secrets.token_hex(8)}"
if len({initial_controller_boot, bootstrap_controller_boot, final_controller_boot}) != 3:
    raise SystemExit("materialize-dynamic-route-profile: Controller Boot collision")
worker_a_boot = f"farm-worker-a-{RUN_ID}-{secrets.token_hex(8)}"
worker_b_boot = f"farm-worker-b-{RUN_ID}-{secrets.token_hex(8)}"
if worker_a_boot == worker_b_boot:
    raise SystemExit("materialize-dynamic-route-profile: FARM_WORKER Boot collision")
route_controller_identity = f"route-controller.{NAMESPACE}.svc.cluster.local"
route_client_identity = f"route-client.{NAMESPACE}.local"
route_operator_identity = f"route-operator.{NAMESPACE}.local"
holder_identity = f"holder-control.{NAMESPACE}.svc.cluster.local"
gate_verifier_identity = f"farm-gate-verifier.{NAMESPACE}.svc.cluster.local"
route_addr = f"route-controller.{NAMESPACE}.svc.cluster.local:9443"

namespace = load(BASE / "namespace.json")
namespace["metadata"]["name"] = NAMESPACE
namespace["metadata"].setdefault("labels", {}).update({
    "farm.io/lab-id": "farm-l2-r2",
    "farm.io/runtime-profile": "route-migration-4x4x4",
    "farm.io/network-policy-enforcement": "NOT_CLAIMED",
})

platform = dynamic_metadata(load(BASE / "platform-config.json"))
data = platform["data"]
data.update({
    "FARM_APP_PROFILE": APP_PROFILE,
    "CAPACITY_PROFILE_VERSION": "route-migration-4x4x4-not-capacity-qualified",
    "ACTOR_AUTHORITY_MODE": "authority",
    "AUTHORITY_MEMBERSHIP_MODE": "composed",
    "ACTOR_ASSIGNMENT_MODE": "remote-watch",
    "ACTOR_ROUTE_REALM": SCALEPLAN_REALM,
    "SCALEPLAN_MODE": SCALEPLAN_MODE,
    "SCALEPLAN_REALM": SCALEPLAN_REALM,
    "SCALEPLAN_ROUTE_DOMAINS": json.dumps(SCALEPLAN_ROUTE_DOMAINS, separators=(",", ":")),
    "SCALEPLAN_PUBLIC_DOMAIN": SCALEPLAN_PUBLIC_DOMAIN,
    "SCALEPLAN_WORKLOAD_ALLOWLIST": SCALEPLAN_WORKLOAD_ALLOWLIST_JSON,
    "SCALEPLAN_CONFIG_HASH": SCALEPLAN_CONFIG_HASH,
    "SCALEPLAN_AUTO_RECONCILE_INTERVAL": SCALEPLAN_AUTO_RECONCILE_INTERVAL,
    "ROUTE_FAILOVER_RECONCILE_INTERVAL": ROUTE_FAILOVER_RECONCILE_INTERVAL,
    "ROUTE_FAILOVER_MAX_PLANS_PER_PASS": str(ROUTE_FAILOVER_MAX_PLANS_PER_PASS),
    "ROUTE_FAILOVER_MAX_AUTHORITY_READS_PER_PASS": str(ROUTE_FAILOVER_MAX_AUTHORITY_READS_PER_PASS),
    "SCALEPLAN_SAFE_DESIRED_ANNOTATION": SCALEPLAN_SAFE_DESIRED_ANNOTATION,
    "SCALEPLAN_ROLLOUT_ENABLED": str(SCALEPLAN_ROLLOUT_ENABLED).lower(),
    "SCALEPLAN_ROLLOUT_ANNOTATION": SCALEPLAN_ROLLOUT_ANNOTATION,
    "SCALEPLAN_ROLLOUT_INTENT_ANNOTATION": SCALEPLAN_ROLLOUT_INTENT_ANNOTATION,
    "SCALEPLAN_TOKENREVIEW_RBAC": str(SCALEPLAN_TOKENREVIEW_RBAC).lower(),
    "HOLDER_DISCOVERY_MODE": HOLDER_DISCOVERY_MODE,
    "HOLDER_DISCOVERY_AUDIENCE": HOLDER_CONTROL_TOKEN_AUDIENCE,
    "REDIS_NAMESPACE_ENVIRONMENT": REDIS_ENV,
    "REDIS_NAMESPACE_ZONE": REDIS_ZONE,
    "REDIS_PHYSICAL_PROFILE_VERSION": "redis-physical-v1",
    "REDIS_AUTHORITY_LAYOUT_VERSION": "v1",
    "REDIS_WORKFLOW_LAYOUT_VERSION": "v1",
    "REDIS_WORKFLOW_HASH_VERSION": "1",
    "REDIS_WORKFLOW_BUCKET_COUNT": "256",
    "FARM_SCHEDULE_DUE_INDEX_MODE": SCHEDULE_DUE_INDEX_MODE,
    "FARM_SCHEDULE_DUE_INDEX_DEDICATED_READY": str(SCHEDULE_DUE_INDEX_DEDICATED_READY).lower(),
    "FARM_FRIEND_NAVIGATION_V3_ENABLED": "1",
    "FRIEND_NAVIGATION_WINDOW_MODE": "top-k",
    "FRIEND_NAVIGATION_WINDOW_SIZE": "32",
    "FARM_OPPORTUNITY_WAKEUP_ENABLED": str(OPPORTUNITY_WAKEUP_ENABLED).lower(),
    "FARM_OPPORTUNITY_WAKEUP_POLL_INTERVAL": "1s",
    "FARM_OPPORTUNITY_WAKEUP_ASSIGNMENT_MODE": OPPORTUNITY_WAKEUP_ASSIGNMENT_MODE,
    "GATE_TRANSPORT": "websocket",
    "GATE_BOOT_REAPER_MODE": "enabled",
    "THEFT_PRF_MODE": "production",
    "WORKFLOW_STORE_PREFIX": f"farm:{REDIS_ENV}:{REDIS_ZONE}:dynamic:workflow:v1",
    "PLAYER_STORE_PREFIX": f"farm:{REDIS_ENV}:{REDIS_ZONE}:dynamic:player:v1",
    "FARM_STORE_PREFIX": f"farm:{REDIS_ENV}:{REDIS_ZONE}:dynamic:farm:v1",
    "SHARE_STORE_PREFIX": f"farm:{REDIS_ENV}:{REDIS_ZONE}:dynamic:share:v1",
    "WEB_ACCOUNT_STORE_PREFIX": f"farm:{REDIS_ENV}:{REDIS_ZONE}:dynamic:web-account:v1",
    "THEFT_GUARD_SNAPSHOT_PREFIX": f"farm:{REDIS_ENV}:{REDIS_ZONE}:dynamic:theft-guard:v1",
    "THEFT_TARGET_BINDING_PREFIX": f"farm:{REDIS_ENV}:{REDIS_ZONE}:dynamic:theft-target:v1",
    "FRIENDSHIP_SERVICE_MODE": "remote_player",
    "PLAYER_ADDR": f"player-svr.{NAMESPACE}.svc.cluster.local:9100",
    "FARM_ADDR": f"farm-svr.{NAMESPACE}.svc.cluster.local:9090",
    "GATEWAY_DOWNLINK_ADDR": f"farm-gate.{NAMESPACE}.svc.cluster.local:9092",
    "LOGIC_HEALTH_ADDR": f"http://farm-svr.{NAMESPACE}.svc.cluster.local:9091",
    "ACTOR_AUTHORITY_LEASE_TTL": "60s",
    "ACTOR_AUTHORITY_LEASE_SAFETY_MARGIN": "10s",
    "ACTOR_AUTHORITY_LEASE_RENEW_INTERVAL": "20s",
    "ROUTE_CONTROL_ADDR": route_addr,
    "ROUTE_CONTROL_CONTROLLER_ID": "route-controller",
    "ROUTE_CONTROL_SERVER_NAME": route_controller_identity,
    "ROUTE_CONTROL_SERVER_IDENTITY": route_controller_identity,
    "ROUTE_CONTROL_CLIENT_CERT_FILE": "/etc/farm/route-control/route-client.crt",
    "ROUTE_CONTROL_CLIENT_KEY_FILE": "/etc/farm/route-control/route-client.key",
    "ROUTE_CONTROL_SERVER_CA_FILE": "/etc/farm/route-control/ca.crt",
    "ROUTE_CONTROL_DIAL_TIMEOUT": "5s",
    "ROUTE_CONTROL_RPC_TIMEOUT": "5s",
    "ROUTE_CONTROL_RETIRED_BOOT_LIMIT": "8",
    "ACTOR_ROUTE_INITIAL_FETCH_TIMEOUT": "30s",
    "ACTOR_ROUTE_STARTUP_TIMEOUT": "120s",
    "ACTOR_ROUTE_ENDPOINT_DIAL_TIMEOUT": "5s",
    "ACTOR_ROUTE_ENDPOINT_RETIRE_GRACE": "5s",
    "ACTOR_ROUTE_MAX_ENDPOINTS": "64",
    "ACTOR_ROUTE_MAX_RESYNC_ATTEMPTS": "32",
    "ACTOR_ROUTE_MAX_WATCH_FAILURES": "32",
    "ACTOR_ROUTE_RETRY_BACKOFF": "100ms",
    "ACTOR_ROUTE_MAX_RETRY_BACKOFF": "1s",
    "GATE_WS_ALLOWED_ORIGINS": "farm.dynamic.local",
    "GATE_WS_RATE_PER_SEC": GATE_WS_RATE_PER_SEC,
    "GATE_WS_RATE_BURST": GATE_WS_RATE_BURST,
    "GATE_PREAUTH_MAX_CONNECTIONS_PER_IP": GATE_PREAUTH_MAX_CONNECTIONS_PER_IP,
    "GATE_PREAUTH_MAX_BYTES_PER_IP": GATE_PREAUTH_MAX_BYTES_PER_IP,
})
if ACTIVATION_RATE_PER_SEC:
    data["ACTIVATION_RATE_PER_SEC"] = ACTIVATION_RATE_PER_SEC
if MAX_CONCURRENT_ACTOR_LOADS:
    data["MAX_CONCURRENT_ACTOR_LOADS"] = MAX_CONCURRENT_ACTOR_LOADS
if MAX_PENDING_ACTOR_LOADS:
    data["MAX_PENDING_ACTOR_LOADS"] = MAX_PENDING_ACTOR_LOADS
if HOLDER_PPROF_ENABLED:
    data["HOLDER_PPROF_ENABLED"] = HOLDER_PPROF_ENABLED
if GATE_PPROF_ENABLED:
    data["GATE_PPROF_ENABLED"] = GATE_PPROF_ENABLED
data.update({
    # Fail closed until an exact local web proxy CIDR is frozen. The dynamic
    # profile does not claim NetworkPolicy or trust all Pod-network clients.
    "GATE_HTTP_TRUSTED_PROXIES": "",
    "GATE_WS_TRUSTED_PROXIES": "",
    "WSS_ENDPOINT": "wss://farm.dynamic.local/ws",
    "H5_PUBLIC_HOST": "farm.dynamic.local",
    "H5_PUBLIC_ORIGIN": "https://farm.dynamic.local",
    "H5_PUBLIC_WSS": "wss://farm.dynamic.local/ws",
    "GATE_BOOT_LEASE_PREFIX": f"farm:{REDIS_ENV}:gate-boot",
    "WEB_LOGIN_RATE_LIMIT_PREFIX": f"farm:{REDIS_ENV}:web-login-limit",
})
scaleplan_config = dynamic_metadata({
    "apiVersion": "v1",
    "kind": "ConfigMap",
    "metadata": {
        "name": "scaleplan-config",
        "labels": {
            "app.kubernetes.io/name": "scaleplan-config",
            "farm.io/scaleplan-profile": "dynamic-local",
        },
        "annotations": {"farm.io/config-hash": SCALEPLAN_CONFIG_HASH},
    },
    "data": {
        "mode": SCALEPLAN_MODE,
        "realm": SCALEPLAN_REALM,
        "route-domains": json.dumps(SCALEPLAN_ROUTE_DOMAINS, separators=(",", ":")),
        "public-domain": SCALEPLAN_PUBLIC_DOMAIN,
        "workload-allowlist": SCALEPLAN_WORKLOAD_ALLOWLIST_JSON,
        "config-hash": SCALEPLAN_CONFIG_HASH,
        "tokenreview-rbac": str(SCALEPLAN_TOKENREVIEW_RBAC).lower(),
        "holder-discovery-mode": HOLDER_DISCOVERY_MODE,
        "holder-identity-transport": "K8S_INFORMER_HOLDERCONTROL_TOKENREVIEW_V1",
        "route-failover-reconcile-interval": ROUTE_FAILOVER_RECONCILE_INTERVAL,
        "route-failover-max-plans-per-pass": str(ROUTE_FAILOVER_MAX_PLANS_PER_PASS),
        "route-failover-max-authority-reads-per-pass": str(ROUTE_FAILOVER_MAX_AUTHORITY_READS_PER_PASS),
        "safe-desired-annotation": SCALEPLAN_SAFE_DESIRED_ANNOTATION,
        "rollout-enabled": str(SCALEPLAN_ROLLOUT_ENABLED).lower(),
        "rollout-annotation": SCALEPLAN_ROLLOUT_ANNOTATION,
        "rollout-intent-annotation": SCALEPLAN_ROLLOUT_INTENT_ANNOTATION,
    },
})
opportunity_mode = dynamic_metadata({
    "apiVersion": "v1",
    "kind": "ConfigMap",
    "metadata": {
        "name": "farm-opportunity-mode",
        "labels": {"app.kubernetes.io/name": "farm-opportunity-mode"},
    },
    "data": {
        "header-read-mode": OPPORTUNITY_HEADER_READ_MODE,
        "hint-transport": OPPORTUNITY_HINT_TRANSPORT,
        "stage": OPPORTUNITY_STAGE,
    },
})
opportunity_mode_volume = {
    "name": "farm-opportunity-mode",
    "configMap": {
        "name": "farm-opportunity-mode",
        "items": [
            {"key": "header-read-mode", "path": "header-read-mode"},
            {"key": "hint-transport", "path": "hint-transport"},
            {"key": "stage", "path": "stage"},
        ],
    },
}

nginx = dynamic_metadata(load(BASE / "nginx-config.json"))
nginx["data"]["default.conf"] = nginx["data"]["default.conf"].replace("farm-staging-system", NAMESPACE)

def runtime_tls_volume(keys: tuple[str, ...]) -> dict:
    return {
        "name": "route-control-tls",
        "secret": {
            "secretName": "farm-route-dynamic-tls", "defaultMode": 0o440,
            "items": [{"key": key, "path": key} for key in keys],
        },
    }


holder_tls_volume = runtime_tls_volume(("ca.crt", "route-client.crt", "route-client.key", "holder-server.crt", "holder-server.key"))
route_client_tls_volume = runtime_tls_volume(("ca.crt", "route-client.crt", "route-client.key"))
gate_tls_volume = runtime_tls_volume(("ca.crt", "route-client.crt", "route-client.key", "gate-verifier.crt", "gate-verifier.key"))
controller_tls_volume = runtime_tls_volume(("ca.crt", "route-controller.crt", "route-controller.key"))
holder_control_token_volume = {
    "name": "holder-control-token",
    "projected": {
        "defaultMode": 256,
        "sources": [{
            "serviceAccountToken": {
                "audience": HOLDER_CONTROL_TOKEN_AUDIENCE,
                "expirationSeconds": HOLDER_CONTROL_TOKEN_EXPIRATION_SECONDS,
                "path": "token",
            },
        }],
    },
}
redis_url_from = {"secretKeyRef": {"name": "farm-dynamic-secrets", "key": "redis-url"}}
authority_redis_url_from = {
    "secretKeyRef": {"name": "farm-dynamic-secrets", "key": "authority-redis-url"}
}
runtime_redis_url_from = {
    "secretKeyRef": {"name": "farm-dynamic-secrets", "key": "runtime-redis-url"}
}
projection_redis_url_from = {
    "secretKeyRef": {"name": "farm-dynamic-secrets", "key": "projection-redis-url"}
}
permit_secret_from = {
    "secretKeyRef": {"name": "farm-dynamic-secrets", "key": "permit-secret"}
}
load_test_token_secret_from = {
    "secretKeyRef": {"name": "farm-dynamic-secrets", "key": "load-test-token-secret"}
}

player_holder_service_account = dynamic_metadata({
    "apiVersion": "v1",
    "kind": "ServiceAccount",
    "metadata": {
        "name": PLAYER_HOLDER_SERVICE_ACCOUNT,
        "labels": {
            "app.kubernetes.io/name": PLAYER_HOLDER_SERVICE_ACCOUNT,
            "farm.io/holder-role": "player",
        },
    },
    "automountServiceAccountToken": False,
})
farm_holder_service_account = dynamic_metadata({
    "apiVersion": "v1",
    "kind": "ServiceAccount",
    "metadata": {
        "name": FARM_HOLDER_SERVICE_ACCOUNT,
        "labels": {
            "app.kubernetes.io/name": FARM_HOLDER_SERVICE_ACCOUNT,
            "farm.io/holder-role": "farm",
        },
    },
    "automountServiceAccountToken": False,
})


def scaleplan_rbac_objects() -> list[dict]:
    if SCALEPLAN_MODE == "off":
        return []
    controller_service_account = dynamic_metadata({
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {
            "name": SCALEPLAN_CONTROLLER_SERVICE_ACCOUNT,
            "labels": {
                "app.kubernetes.io/name": "scaleplan-controller",
                "farm.io/scaleplan-role": "controller",
            },
        },
        "automountServiceAccountToken": True,
    })
    pod_verbs = ["get", "list", "watch"]
    statefulset_verbs = ["get", "list", "watch", "patch"]
    if SCALEPLAN_ROLLOUT_ENABLED:
        pod_verbs.append("delete")
        statefulset_verbs.append("update")
    controller_role = dynamic_metadata({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "Role",
        "metadata": {
            "name": "scaleplan-controller",
            "labels": {"farm.io/scaleplan-role": "controller"},
        },
        "rules": [
            {
                "apiGroups": [""],
                "resources": ["pods"],
                "verbs": pod_verbs,
            },
            {
                "apiGroups": ["apps"],
                "resources": ["statefulsets"],
                "verbs": statefulset_verbs,
            },
            {
                "apiGroups": ["apps"],
                "resources": ["statefulsets/scale"],
                "verbs": ["get", "update"],
            },
        ],
    })
    controller_role_binding = dynamic_metadata({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": {
            "name": "scaleplan-controller",
            "labels": {"farm.io/scaleplan-role": "controller"},
        },
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "Role",
            "name": "scaleplan-controller",
        },
        "subjects": [{
            "kind": "ServiceAccount",
            "name": SCALEPLAN_CONTROLLER_SERVICE_ACCOUNT,
            "namespace": NAMESPACE,
        }],
    })
    objects = [controller_service_account, controller_role, controller_role_binding]
    if SCALEPLAN_TOKENREVIEW_RBAC:
        tokenreview_role = {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRole",
            "metadata": {
                "name": "scaleplan-controller-tokenreview",
                "labels": {"farm.io/scaleplan-role": "controller"},
            },
            "rules": [{
                "apiGroups": ["authentication.k8s.io"],
                "resources": ["tokenreviews"],
                "verbs": ["create"],
            }],
        }
        tokenreview_binding = {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRoleBinding",
            "metadata": {
                "name": "scaleplan-controller-tokenreview",
                "labels": {"farm.io/scaleplan-role": "controller"},
            },
            "roleRef": {
                "apiGroup": "rbac.authorization.k8s.io",
                "kind": "ClusterRole",
                "name": "scaleplan-controller-tokenreview",
            },
            "subjects": [{
                "kind": "ServiceAccount",
                "name": SCALEPLAN_CONTROLLER_SERVICE_ACCOUNT,
                "namespace": NAMESPACE,
            }],
        }
        objects.extend([tokenreview_role, tokenreview_binding])
    return objects


scaleplan_rbac = scaleplan_rbac_objects()


REDIS_CLUSTER_SEED_URLS = ",".join(
    f"redis://farm-local-redis-{index}.farm-local-redis-headless.farm-l2-r2-deps.svc.cluster.local:6379/0"
    for index in range(3)
)


def set_redis_urls(container: dict) -> None:
    env_set(container, "FARM_REDIS_URL", value_from=redis_url_from)
    env_set(container, "FARM_AUTHORITY_REDIS_URL", value_from=authority_redis_url_from)
    env_set(container, "FARM_RUNTIME_REDIS_URL", value_from=runtime_redis_url_from)
    env_set(container, "FARM_PROJECTION_REDIS_URL", value_from=projection_redis_url_from)
    env_set(container, "FARM_REDIS_CLUSTER", value="1")
    env_set(container, "FARM_REDIS_CLUSTER_URLS", value=REDIS_CLUSTER_SEED_URLS)


def holder(kind: str) -> dict:
    name = "player-svr" if kind == "player" else "farm-svr"
    obj = dynamic_metadata(load(BASE / f"{name.split('-')[0]}-statefulset.json"))
    obj["spec"]["replicas"] = 2
    if SCALEPLAN_ROLLOUT_ENABLED:
        # The RouteController owns replacement ordering. Kubernetes must not
        # delete an old holder before its Shards have been fenced and drained.
        obj["spec"]["updateStrategy"] = {"type": "OnDelete"}
    obj["spec"]["template"]["metadata"].setdefault("annotations", {})["farm.io/activation-status"] = "DYNAMIC_REMOTE_WATCH_4X4X4"
    pod = obj["spec"]["template"]["spec"]
    pod["serviceAccountName"] = (
        PLAYER_HOLDER_SERVICE_ACCOUNT if kind == "player" else FARM_HOLDER_SERVICE_ACCOUNT
    )
    pod["automountServiceAccountToken"] = False
    pod["topologySpreadConstraints"][1]["whenUnsatisfiable"] = "ScheduleAnyway"
    pod.get("affinity", {}).get("podAntiAffinity", {}).pop("requiredDuringSchedulingIgnoredDuringExecution", None)
    pod.setdefault("securityContext", {}).update({"runAsUser": 10001, "runAsGroup": 10001, "fsGroup": 10001, "fsGroupChangePolicy": "OnRootMismatch"})
    remove_static_route(pod)
    replace_secret_refs(pod)
    container = pod["containers"][0]
    container["image"] = runtime_ref
    cpu_limit = PLAYER_SVR_CPU_LIMIT if kind == "player" else FARM_SVR_CPU_LIMIT
    memory_limit = PLAYER_SVR_MEMORY_LIMIT if kind == "player" else FARM_SVR_MEMORY_LIMIT
    container["resources"] = {
        "requests": {"cpu": "500m", "memory": "512Mi"},
        "limits": {"cpu": cpu_limit, "memory": memory_limit},
    }
    container.setdefault("securityContext", {}).update({"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}})
    container["env"] = [entry for entry in container.get("env", []) if entry.get("name") != "ACTOR_HOLDER_BOOT_ID"]
    pod_uid_ref = {"fieldRef": {"fieldPath": "metadata.uid"}}
    ordinal_ref = {
        "fieldRef": {
            "fieldPath": "metadata.labels['apps.kubernetes.io/pod-index']",
        },
    }
    env_set(container, "POD_UID", value_from=pod_uid_ref)
    env_set(container, "ACTOR_HOLDER_IDENTITY_MODE", value="kubernetes-process-v1")
    env_set(container, "ACTOR_HOLDER_POD_UID", value_from=pod_uid_ref)
    # StatefulSet UID is injected by the staged apply script after Kubernetes
    # creates the workload and before the first Holder Pod is allowed to start.
    env_set(container, "ACTOR_HOLDER_WORKLOAD_UID", value="PENDING_STAGED_APPLY")
    env_set(container, "ACTOR_HOLDER_ORDINAL", value_from=ordinal_ref)
    env_set(container, "HOLDER_PROCESS_BOOT_MODE", value="pod-uid-then-random")
    env_set(container, "HOLDER_PROCESS_BOOT_DIR", value="/var/run/farm-holder")
    set_redis_urls(container)
    prefix = "PLAYER" if kind == "player" else "FARM"
    if kind == "farm":
        env_set(
            container,
            "FARM_OPPORTUNITY_WAKEUP_ATTACH_ENABLED",
            value=str(OPPORTUNITY_WAKEUP_ATTACH_ENABLED).lower(),
        )
        env_set(container, "FARM_OPPORTUNITY_HEADER_READ_MODE", value=OPPORTUNITY_HEADER_READ_MODE)
        env_set(
            container,
            "FARM_OPPORTUNITY_HEADER_MODE_FILE",
            value="/etc/farm/opportunity-mode/header-read-mode",
        )
        env_set(
            container,
            "FARM_OPPORTUNITY_HEADER_MODE_POLL_INTERVAL",
            value="1s",
        )
        add_mount(
            container,
            "farm-opportunity-mode",
            "/etc/farm/opportunity-mode",
            True,
        )
        add_volume(pod, opportunity_mode_volume)
    logic_port, holder_port = (9100, 19100) if kind == "player" else (9090, 19101)
    env_set(container, f"{prefix}_LOGIC_ENDPOINT", value=f"$(POD_NAME).{name}-headless.{NAMESPACE}.svc.cluster.local:{logic_port}")
    env_set(container, f"{prefix}_HOLDER_ENDPOINT", value=f"$(POD_NAME).{name}-headless.{NAMESPACE}.svc.cluster.local:{holder_port}")
    env_set(container, f"{prefix}_HOLDER_TLS_DNS_IDENTITY", value=holder_identity)
    env_set(container, f"{prefix}_HOLDER_CONTROL_LISTEN", value=f":{holder_port}")
    env_set(container, f"{prefix}_HOLDER_TLS_CERT_FILE", value="/etc/farm/route-control/holder-server.crt")
    env_set(container, f"{prefix}_HOLDER_TLS_KEY_FILE", value="/etc/farm/route-control/holder-server.key")
    env_set(container, f"{prefix}_HOLDER_CLIENT_CA_FILE", value="/etc/farm/route-control/ca.crt")
    env_set(container, "HOLDER_CONTROL_CONTROLLER_IDENTITY", value=route_controller_identity)
    env_set(container, "HOLDER_WORKLOAD_IDENTITY_TOKEN_FILE", value=HOLDER_CONTROL_TOKEN_FILE)
    ports = container.setdefault("ports", [])
    if not any(port.get("name") == "holder-mtls" for port in ports):
        ports.append({"name": "holder-mtls", "containerPort": holder_port, "protocol": "TCP"})
    add_mount(container, "route-control-tls", "/etc/farm/route-control", True)
    add_mount(container, "holder-control-token", HOLDER_CONTROL_TOKEN_MOUNT, True)
    add_mount(container, "holder-process-boot", "/var/run/farm-holder", False)
    add_volume(pod, holder_tls_volume)
    add_volume(pod, holder_control_token_volume)
    add_volume(pod, {"name": "holder-process-boot", "emptyDir": {"medium": "Memory", "sizeLimit": "1Mi"}})
    # Bootstrap-only needs the Pod UID before the first Holder process starts.
    # The Service publishes the Controller endpoint before readiness, and the
    # Controller serves /healthz after loading the durable zero-Shard snapshot
    # but before Holder discovery completes. Waiting for /readyz would deadlock.
    pod["initContainers"] = [{
        "name": "dynamic-route-controller-barrier",
        "image": runtime_ref,
        "imagePullPolicy": "IfNotPresent",
        "command": ["/bin/sh", "-ec"],
        "args": [
            "attempt=0; until "
            f"nslookup player-svr-0.player-svr-headless.{NAMESPACE}.svc.cluster.local >/dev/null 2>&1 && "
            f"nslookup player-svr-1.player-svr-headless.{NAMESPACE}.svc.cluster.local >/dev/null 2>&1 && "
            f"nslookup farm-svr-0.farm-svr-headless.{NAMESPACE}.svc.cluster.local >/dev/null 2>&1 && "
            f"nslookup farm-svr-1.farm-svr-headless.{NAMESPACE}.svc.cluster.local >/dev/null 2>&1 && "
            f"wget -q -T 1 -O - http://route-controller.{NAMESPACE}.svc.cluster.local:8081/healthz >/dev/null 2>&1; do "
            "attempt=$((attempt+1)); [ \"$attempt\" -lt 180 ] || exit 70; sleep 1; done"
        ],
        "resources": {"requests": {"cpu": "10m", "memory": "16Mi"}, "limits": {"cpu": "100m", "memory": "64Mi"}},
        "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}},
    }]
    return obj


player = holder("player")
farm = holder("farm")


def consumer(resource: str, replicas: int, memory: str, tls_volume: dict) -> dict:
    obj = dynamic_metadata(load(BASE / resource))
    obj["spec"]["replicas"] = replicas
    pod = obj["spec"]["template"]["spec"]
    pod["topologySpreadConstraints"][1]["whenUnsatisfiable"] = "ScheduleAnyway"
    pod.get("affinity", {}).get("podAntiAffinity", {}).pop("requiredDuringSchedulingIgnoredDuringExecution", None)
    pod.setdefault("securityContext", {}).update({"runAsUser": 10001, "runAsGroup": 10001, "fsGroup": 10001, "fsGroupChangePolicy": "OnRootMismatch"})
    remove_static_route(pod)
    replace_secret_refs(pod)
    container = pod["containers"][0]
    container["image"] = runtime_ref
    container["resources"] = {"requests": {"cpu": "250m", "memory": memory}, "limits": {"cpu": "1", "memory": memory}}
    container.setdefault("securityContext", {}).update({"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}})
    set_redis_urls(container)
    add_mount(container, "route-control-tls", "/etc/farm/route-control", True)
    add_volume(pod, tls_volume)
    obj["spec"]["template"]["metadata"].setdefault("annotations", {})["farm.io/activation-status"] = "DYNAMIC_REMOTE_WATCH_4X4X4"
    return obj


gate = consumer("gate-deployment.json", 1, "768Mi", gate_tls_volume)
gate["spec"]["template"]["spec"]["containers"][0]["resources"] = {
    "requests": {"cpu": "1", "memory": "1Gi"},
    "limits": {"cpu": GATE_CPU_LIMIT, "memory": GATE_MEMORY_LIMIT},
}


def dynamic_worker(name: str, boot_id: str) -> dict:
    obj = consumer("worker-deployment.json", 1, "768Mi", route_client_tls_volume)
    obj["metadata"]["name"] = name
    obj["metadata"].setdefault("labels", {})["app.kubernetes.io/name"] = name
    obj["spec"]["strategy"] = {"type": "Recreate"}
    obj["spec"]["selector"]["matchLabels"]["app.kubernetes.io/name"] = name
    pod_metadata = obj["spec"]["template"]["metadata"]
    pod_metadata.setdefault("labels", {})["app.kubernetes.io/name"] = name
    pod_metadata["labels"]["farm.io/worker-pool"] = "farm-worker"
    for constraint in obj["spec"]["template"]["spec"].get("topologySpreadConstraints", []):
        constraint["labelSelector"] = {
            "matchLabels": {"farm.io/worker-pool": "farm-worker"},
        }
    container = obj["spec"]["template"]["spec"]["containers"][0]
    env_set(container, "WORKER_ID", value=name)
    env_set(container, "WORKER_BOOT_ID", value=boot_id)
    env_set(container, "PERMIT_SECRET", value_from=permit_secret_from)
    env_set(
        container,
        "FARM_OPPORTUNITY_WAKEUP_ENABLED",
        value=str(OPPORTUNITY_WAKEUP_ENABLED).lower(),
    )
    env_set(
        container,
        "FARM_OPPORTUNITY_WAKEUP_ASSIGNMENT_MODE",
        value=OPPORTUNITY_WAKEUP_ASSIGNMENT_MODE,
    )
    env_set(
        container,
        "FARM_OPPORTUNITY_HINT_TRANSPORT",
        value=OPPORTUNITY_HINT_TRANSPORT,
    )
    env_set(
        container,
        "FARM_OPPORTUNITY_HINT_MODE_FILE",
        value="/etc/farm/opportunity-mode/hint-transport",
    )
    env_set(
        container,
        "FARM_OPPORTUNITY_HINT_MODE_POLL_INTERVAL",
        value="1s",
    )
    add_mount(
        container,
        "farm-opportunity-mode",
        "/etc/farm/opportunity-mode",
        True,
    )
    add_volume(
        obj["spec"]["template"]["spec"],
        opportunity_mode_volume,
    )
    env_set(container, "FARM_WORKER_AUTHORITY_RENEW_TTL", value="60s")
    env_set(container, "FARM_WORKER_AUTHORITY_RENEW_INTERVAL", value="10s")
    env_set(container, "FARM_WORKER_AUTHORITY_RENEW_TIMEOUT", value="2s")
    env_set(container, "FARM_WORKER_ROUTE_HOLDER_ONLY", value="true")
    worker_endpoint = f"{name}.{NAMESPACE}.svc.cluster.local:9201"
    env_set(container, "FARM_WORKER_LOGIC_ENDPOINT", value=worker_endpoint)
    env_set(container, "FARM_WORKER_HOLDER_CONTROL_ENDPOINT", value=worker_endpoint)
    env_set(
        container,
        "FARM_WORKER_HOLDER_TLS_DNS_IDENTITY",
        value=f"holder-control.{NAMESPACE}.svc.cluster.local",
    )
    container["readinessProbe"] = {
        "httpGet": {"path": "/readyz", "port": 9201},
        "initialDelaySeconds": 1,
        "periodSeconds": 2,
        "timeoutSeconds": 1,
        "failureThreshold": 3,
    }
    container["livenessProbe"] = {
        "httpGet": {"path": "/healthz", "port": 9201},
        "initialDelaySeconds": 5,
        "periodSeconds": 5,
        "timeoutSeconds": 1,
        "failureThreshold": 3,
    }
    return obj


worker_a = dynamic_worker("farm-worker-a", worker_a_boot)
worker_b = dynamic_worker("farm-worker-b", worker_b_boot)
gate_container = gate["spec"]["template"]["spec"]["containers"][0]
env_set(gate_container, "GATE_DOWNLINK_WRITER_MAX_ITEMS", value="256")
env_set(gate_container, "GATE_DOWNLINK_WRITER_MAX_BYTES", value="1048576")
env_set(gate_container, "GATE_DOWNLINK_HINT_MAX_ITEMS", value="32")
env_set(gate_container, "GATE_DOWNLINK_HINT_MAX_BYTES", value="65536")
env_set(gate_container, "GATE_DOWNLINK_DRAIN_QUANTUM", value="64")
env_set(gate_container, "GATE_VERIFIER_LISTEN", value=":19200")
env_set(gate_container, "GATE_VERIFIER_TLS_CERT", value="/etc/farm/route-control/gate-verifier.crt")
env_set(gate_container, "GATE_VERIFIER_TLS_KEY", value="/etc/farm/route-control/gate-verifier.key")
env_set(gate_container, "GATE_VERIFIER_CLIENT_CA", value="/etc/farm/route-control/ca.crt")
env_set(gate_container, "GATE_VERIFIER_ALLOWED_CLIENT_IDENTITY", value=route_controller_identity)
if LOAD_TEST_LOGIN_ENABLED:
    env_set(gate_container, "LOAD_TEST_TOKEN_SECRET", value_from=load_test_token_secret_from)
gate_container.setdefault("ports", []).append({"name": "verifier-mtls", "containerPort": 19200, "protocol": "TCP"})

h5 = dynamic_metadata(load(BASE / "h5-deployment.json"))
h5["spec"]["replicas"] = 1
h5_pod = h5["spec"]["template"]["spec"]
h5_pod["topologySpreadConstraints"][1]["whenUnsatisfiable"] = "ScheduleAnyway"
h5_pod.get("affinity", {}).get("podAntiAffinity", {}).pop("requiredDuringSchedulingIgnoredDuringExecution", None)
h5_pod["automountServiceAccountToken"] = False
h5_pod.setdefault("securityContext", {}).update({
    "runAsNonRoot": True, "runAsUser": 101, "runAsGroup": 101,
    "fsGroup": 101, "fsGroupChangePolicy": "OnRootMismatch",
    "seccompProfile": {"type": "RuntimeDefault"},
})
h5_pod["initContainers"] = [{
    "name": "dynamic-gate-ready-barrier", "image": h5_ref, "imagePullPolicy": "IfNotPresent",
    "command": ["/bin/sh", "-ec"],
    "args": [
        "attempt=0; until nslookup farm-gate.farm-route-dynamic.svc.cluster.local >/dev/null 2>&1 && "
        "wget -q -T 2 -O /dev/null http://farm-gate.farm-route-dynamic.svc.cluster.local:8080/readyz 2>/dev/null; do "
        "attempt=$((attempt+1)); [ \"$attempt\" -lt 180 ] || exit 72; sleep 1; done"
    ],
    "resources": {"requests": {"cpu": "10m", "memory": "16Mi"}, "limits": {"cpu": "100m", "memory": "64Mi"}},
    "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}},
}]
h5_container = h5_pod["containers"][0]
h5_container["image"] = h5_ref
h5_container["securityContext"] = {
    "allowPrivilegeEscalation": False,
    "readOnlyRootFilesystem": True,
    "capabilities": {"drop": ["ALL"]},
}
for name, mount_path in (("nginx-run", "/run"), ("nginx-cache", "/var/cache/nginx"), ("nginx-tmp", "/tmp")):
    add_mount(h5_container, name, mount_path, False)
    add_volume(h5_pod, {"name": name, "emptyDir": {"sizeLimit": "64Mi"}})
h5["spec"]["template"]["metadata"].setdefault("annotations", {})["farm.io/activation-status"] = "DYNAMIC_BUSINESS_REGRESSION"


def service(filename: str) -> dict:
    return dynamic_metadata(load(BASE / filename))


route_controller = dynamic_metadata({
    "apiVersion": "apps/v1", "kind": "Deployment",
    "metadata": {"name": "route-controller", "labels": {"app.kubernetes.io/name": "route-controller", "app.kubernetes.io/part-of": "farm-game"}},
    "spec": {
        "replicas": 1, "strategy": {"type": "Recreate"},
        "selector": {"matchLabels": {"app.kubernetes.io/name": "route-controller"}},
        "template": {
            "metadata": {"labels": {"app.kubernetes.io/name": "route-controller", "app.kubernetes.io/component": "route-control", "app.kubernetes.io/part-of": "farm-game", "farm.io/redis-client": "true"}, "annotations": {"farm.io/activation-status": "DYNAMIC_REMOTE_WATCH_4X4X4"}},
            "spec": {
                "terminationGracePeriodSeconds": 45,
                "securityContext": {"runAsNonRoot": True, "runAsUser": 10001, "runAsGroup": 10001, "fsGroup": 10001, "fsGroupChangePolicy": "OnRootMismatch", "seccompProfile": {"type": "RuntimeDefault"}},
                "containers": [{
                    "name": "route-controller", "image": runtime_ref, "imagePullPolicy": "IfNotPresent",
                    "envFrom": [{"configMapRef": {"name": "farm-staging-platform"}}],
                    "env": [
                        {"name": "FARM_SERVICE", "value": "route-controller"},
                        {"name": "ROUTE_CONTROLLER_LISTEN", "value": ":9443"},
                        {"name": "ROUTE_CONTROLLER_HEALTH_LISTEN", "value": ":8081"},
                        {"name": "ROUTE_CONTROLLER_METRICS_LISTEN", "value": ":9090"},
                        {"name": "ROUTE_CONTROLLER_ID", "value": "route-controller"},
                        {"name": "ROUTE_CONTROLLER_BOOT", "value": initial_controller_boot},
                        {"name": "ROUTE_CONTROLLER_REALM", "value": SCALEPLAN_REALM},
                        {"name": "SCALEPLAN_MODE", "value": SCALEPLAN_MODE},
                        {"name": "SCALEPLAN_REALM", "value": SCALEPLAN_REALM},
                        {"name": "SCALEPLAN_ROUTE_DOMAINS", "value": json.dumps(SCALEPLAN_ROUTE_DOMAINS, separators=(",", ":"))},
                        {"name": "SCALEPLAN_PUBLIC_DOMAIN", "value": SCALEPLAN_PUBLIC_DOMAIN},
                        {"name": "SCALEPLAN_WORKLOAD_ALLOWLIST", "value": SCALEPLAN_WORKLOAD_ALLOWLIST_JSON},
                        {"name": "SCALEPLAN_CONFIG_HASH", "value": SCALEPLAN_CONFIG_HASH},
                        {"name": "SCALEPLAN_AUTO_RECONCILE_INTERVAL", "value": SCALEPLAN_AUTO_RECONCILE_INTERVAL},
                        {"name": "ROUTE_FAILOVER_RECONCILE_INTERVAL", "value": ROUTE_FAILOVER_RECONCILE_INTERVAL},
                        {"name": "ROUTE_FAILOVER_MAX_PLANS_PER_PASS", "value": str(ROUTE_FAILOVER_MAX_PLANS_PER_PASS)},
                        {"name": "ROUTE_FAILOVER_MAX_AUTHORITY_READS_PER_PASS", "value": str(ROUTE_FAILOVER_MAX_AUTHORITY_READS_PER_PASS)},
                        {"name": "SCALEPLAN_SAFE_DESIRED_ANNOTATION", "value": SCALEPLAN_SAFE_DESIRED_ANNOTATION},
                        {"name": "SCALEPLAN_ROLLOUT_ENABLED", "value": str(SCALEPLAN_ROLLOUT_ENABLED).lower()},
                        {"name": "SCALEPLAN_ROLLOUT_ANNOTATION", "value": SCALEPLAN_ROLLOUT_ANNOTATION},
                        {"name": "SCALEPLAN_ROLLOUT_INTENT_ANNOTATION", "value": SCALEPLAN_ROLLOUT_INTENT_ANNOTATION},
                        {"name": "HOLDER_DISCOVERY_MODE", "value": HOLDER_DISCOVERY_MODE},
                        {"name": "HOLDER_DISCOVERY_AUDIENCE", "value": HOLDER_CONTROL_TOKEN_AUDIENCE},
                        {"name": "HOLDER_DISCOVERY_RESYNC", "value": "30s"},
                        {"name": "HOLDER_DISCOVERY_VERIFIED_TTL", "value": "45s"},
                        {"name": "HOLDER_DISCOVERY_PLAYER_LOGIC_PORT", "value": "9100"},
                        {"name": "HOLDER_DISCOVERY_PLAYER_CONTROL_PORT", "value": "19100"},
                        {"name": "HOLDER_DISCOVERY_FARM_LOGIC_PORT", "value": "9090"},
                        {"name": "HOLDER_DISCOVERY_FARM_CONTROL_PORT", "value": "19101"},
                        {"name": "HOLDER_DISCOVERY_PLAYER_TLS_IDENTITY", "value": holder_identity},
                        {"name": "HOLDER_DISCOVERY_FARM_TLS_IDENTITY", "value": holder_identity},
                        {"name": "FARM_REDIS_URL", "valueFrom": redis_url_from},
                        {"name": "FARM_AUTHORITY_REDIS_URL", "valueFrom": authority_redis_url_from},
                        {"name": "FARM_RUNTIME_REDIS_URL", "valueFrom": runtime_redis_url_from},
                        {"name": "FARM_PROJECTION_REDIS_URL", "valueFrom": projection_redis_url_from},
                        {"name": "FARM_REDIS_CLUSTER", "value": "1"},
                        {"name": "FARM_REDIS_CLUSTER_URLS", "value": REDIS_CLUSTER_SEED_URLS},
                        {"name": "ROUTE_CONTROLLER_TLS_CERT", "value": "/etc/farm/route-control/route-controller.crt"},
                        {"name": "ROUTE_CONTROLLER_TLS_KEY", "value": "/etc/farm/route-control/route-controller.key"},
                        {"name": "ROUTE_CONTROLLER_TLS_CA", "value": "/etc/farm/route-control/ca.crt"},
                        {"name": "ROUTE_CONTROLLER_HOLDER_CLIENT_CERT", "value": "/etc/farm/route-control/route-controller.crt"},
                        {"name": "ROUTE_CONTROLLER_HOLDER_CLIENT_KEY", "value": "/etc/farm/route-control/route-controller.key"},
                        {"name": "ROUTE_CONTROLLER_HOLDER_CLIENT_CA", "value": "/etc/farm/route-control/ca.crt"},
                        {"name": "ROUTE_CONTROLLER_GATE_VERIFIER_CLIENT_CERT", "value": "/etc/farm/route-control/route-controller.crt"},
                        {"name": "ROUTE_CONTROLLER_GATE_VERIFIER_CLIENT_KEY", "value": "/etc/farm/route-control/route-controller.key"},
                        {"name": "ROUTE_CONTROLLER_GATE_VERIFIER_CLIENT_CA", "value": "/etc/farm/route-control/ca.crt"},
                        {"name": "ROUTE_CONTROLLER_GATE_VERIFIER_PEERS", "value": json.dumps([{"name": "farm-gate", "address": f"farm-gate-verifier.{NAMESPACE}.svc.cluster.local:19200", "server_name": gate_verifier_identity, "expected_identity": gate_verifier_identity}], separators=(",", ":"))},
                        {"name": "ROUTE_CONTROLLER_ALLOWED_CLIENT_IDENTITIES", "value": route_client_identity},
                        {"name": "ROUTE_CONTROLLER_PLAN_READ_IDENTITIES", "value": route_operator_identity},
                        {"name": "ROUTE_CONTROLLER_OPERATOR_IDENTITIES", "value": route_operator_identity},
                        {"name": "ROUTE_CONTROLLER_SINGLE_CONTROLLER_ATTESTATION", "value": "single-replica-recreate-no-overlap-v1"},
                        {"name": "ROUTE_CONTROLLER_PLAN_OPERATION_TIMEOUT", "value": "30s"},
                        {"name": "ROUTE_CONTROLLER_PLAN_TTL", "value": "24h"},
                        {"name": "ROUTE_CONTROLLER_DEADLINE_REFRESH_INTERVAL", "value": "1s"},
                        {"name": "ROUTE_CONTROLLER_DEADLINE_REFRESH_LEAD", "value": "30s"},
                        {"name": "ROUTE_CONTROLLER_LEASE_PROJECTION_MODE", "value": "disabled"},
                    ],
                    "ports": [{"name": "grpc-mtls", "containerPort": 9443}, {"name": "health", "containerPort": 8081}, {"name": "metrics", "containerPort": 9090}],
                    "startupProbe": {"httpGet": {"path": "/healthz", "port": "health"}, "periodSeconds": 2, "failureThreshold": 30},
                    "readinessProbe": {"httpGet": {"path": "/readyz", "port": "health"}, "periodSeconds": 2, "timeoutSeconds": 2, "failureThreshold": 1},
                    "livenessProbe": {"httpGet": {"path": "/healthz", "port": "health"}, "periodSeconds": 10, "timeoutSeconds": 2, "failureThreshold": 3},
                    "volumeMounts": [{"name": "route-control-tls", "mountPath": "/etc/farm/route-control", "readOnly": True}],
                    "resources": {"requests": {"cpu": "250m", "memory": "512Mi"}, "limits": {"cpu": "1", "memory": "1Gi"}},
                    "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}},
                }],
                "volumes": [controller_tls_volume],
            },
        },
    },
})
route_controller_pod = route_controller["spec"]["template"]["spec"]
route_controller_pod["automountServiceAccountToken"] = SCALEPLAN_MODE != "off"
if SCALEPLAN_MODE != "off":
    route_controller_pod["serviceAccountName"] = SCALEPLAN_CONTROLLER_SERVICE_ACCOUNT
else:
    route_controller_pod.pop("serviceAccountName", None)
route_service = dynamic_metadata({
    "apiVersion": "v1", "kind": "Service", "metadata": {"name": "route-controller", "labels": {"app.kubernetes.io/name": "route-controller"}},
    # Holders must fetch the durable read-only RouteSnapshot before they can
    # expose HolderControl and make the Controller Ready. Publishing the sole
    # Controller endpoint during that bounded warming window breaks this
    # bootstrap cycle; mutating RPCs remain guarded by StartupReady.
    "spec": {"publishNotReadyAddresses": True, "selector": {"app.kubernetes.io/name": "route-controller"}, "ports": [{"name": "grpc-mtls", "port": 9443, "targetPort": "grpc-mtls"}, {"name": "health", "port": 8081, "targetPort": "health"}, {"name": "metrics", "port": 9090, "targetPort": "metrics"}]},
})
gate_verifier_service = dynamic_metadata({
    "apiVersion": "v1", "kind": "Service", "metadata": {"name": "farm-gate-verifier", "labels": {"app.kubernetes.io/name": "farm-gate"}},
    "spec": {"selector": {"app.kubernetes.io/name": "farm-gate"}, "ports": [{"name": "verifier-mtls", "port": 19200, "targetPort": "verifier-mtls"}]},
})

final_route_controller = copy.deepcopy(route_controller)
final_container = final_route_controller["spec"]["template"]["spec"]["containers"][0]
for entry in final_container["env"]:
    if entry.get("name") == "ROUTE_CONTROLLER_BOOT":
        entry["value"] = final_controller_boot
        break
else:
    raise SystemExit("materialize-dynamic-route-profile: Controller Boot env missing")

player_service = service("player-service.json")
player_service["spec"]["ports"].append({"name": "health", "port": 9101, "targetPort": "health", "protocol": "TCP"})
player_headless = service("player-headless-service.json")
player_headless["spec"]["publishNotReadyAddresses"] = True
farm_service = service("farm-service.json")
farm_service["spec"]["ports"].append({"name": "health", "port": 9091, "targetPort": "health", "protocol": "TCP"})
farm_headless = service("farm-headless-service.json")
farm_headless["spec"]["publishNotReadyAddresses"] = True
worker_service_a = service("worker-service.json")
worker_service_a["metadata"]["name"] = "farm-worker-a"
worker_service_a["metadata"].setdefault("labels", {})["app.kubernetes.io/name"] = "farm-worker-a"
worker_service_a["spec"]["selector"] = {"app.kubernetes.io/name": "farm-worker-a"}
worker_service_b = copy.deepcopy(worker_service_a)
worker_service_b["metadata"]["name"] = "farm-worker-b"
worker_service_b["metadata"]["labels"]["app.kubernetes.io/name"] = "farm-worker-b"
worker_service_b["spec"]["selector"] = {"app.kubernetes.io/name": "farm-worker-b"}
gate_service = service("gate-service.json")
h5_service = service("h5-service.json")

holder_objects = [
    namespace,
    platform,
    scaleplan_config,
    *scaleplan_rbac,
    player_holder_service_account,
    farm_holder_service_account,
    opportunity_mode,
    player_service,
    player_headless,
    player,
    farm_service,
    farm_headless,
    farm,
]
control_plane_objects = [route_service, route_controller]
final_control_plane_objects = [route_service, final_route_controller]
business_objects = [
    nginx,
    worker_service_a,
    worker_a,
    worker_service_b,
    worker_b,
    gate_service,
    gate_verifier_service,
    gate,
    h5_service,
    h5,
]
objects = holder_objects + control_plane_objects + business_objects

OUT.mkdir(parents=True, mode=0o700)
full_path = OUT / "profile.json"
holders_path = OUT / "holders.json"
control_plane_path = OUT / "control-plane.json"
final_control_plane_path = OUT / "final-control-plane.json"
business_path = OUT / "business.json"
full_bytes = (json.dumps({"apiVersion": "v1", "kind": "List", "items": objects}, sort_keys=True, indent=2) + "\n").encode()
holder_bytes = (json.dumps({"apiVersion": "v1", "kind": "List", "items": holder_objects}, sort_keys=True, indent=2) + "\n").encode()
control_plane_bytes = (json.dumps({"apiVersion": "v1", "kind": "List", "items": control_plane_objects}, sort_keys=True, indent=2) + "\n").encode()
final_control_plane_bytes = (json.dumps({"apiVersion": "v1", "kind": "List", "items": final_control_plane_objects}, sort_keys=True, indent=2) + "\n").encode()
business_bytes = (json.dumps({"apiVersion": "v1", "kind": "List", "items": business_objects}, sort_keys=True, indent=2) + "\n").encode()
for path, payload in (
    (full_path, full_bytes), (holders_path, holder_bytes),
    (control_plane_path, control_plane_bytes), (final_control_plane_path, final_control_plane_bytes),
    (business_path, business_bytes),
):
    write_exclusive(path, payload, 0o600)

value = {
    "schema_version": "farm-dynamic-route-profile.v1",
    "status": "MATERIALIZED_NOT_APPLIED",
    "run_id": RUN_ID,
    "source_head": head,
    "source_worktree_clean": source_worktree_clean,
    "namespace": NAMESPACE,
    "redis_namespace_environment": REDIS_ENV,
    "redis_namespace_zone": REDIS_ZONE,
    "profile": "route-migration-4x4x4",
    "scaleplan": {
        "mode": SCALEPLAN_MODE,
        "controller_service_account": (
            SCALEPLAN_CONTROLLER_SERVICE_ACCOUNT if SCALEPLAN_MODE != "off" else None
        ),
        "realm": SCALEPLAN_REALM,
        "route_domains": list(SCALEPLAN_ROUTE_DOMAINS),
        "public_domain": SCALEPLAN_PUBLIC_DOMAIN,
        "workload_allowlist": list(SCALEPLAN_WORKLOAD_ALLOWLIST),
        "config_hash": SCALEPLAN_CONFIG_HASH,
        "tokenreview_rbac": SCALEPLAN_TOKENREVIEW_RBAC,
        "holder_discovery_mode": HOLDER_DISCOVERY_MODE,
        "holder_identity_transport": "K8S_INFORMER_HOLDERCONTROL_TOKENREVIEW_V1",
        "auto_reconcile_interval": SCALEPLAN_AUTO_RECONCILE_INTERVAL,
        "route_failover_reconcile_interval": ROUTE_FAILOVER_RECONCILE_INTERVAL,
        "route_failover_max_plans_per_pass": ROUTE_FAILOVER_MAX_PLANS_PER_PASS,
        "route_failover_max_authority_reads_per_pass": ROUTE_FAILOVER_MAX_AUTHORITY_READS_PER_PASS,
        "safe_desired_annotation": SCALEPLAN_SAFE_DESIRED_ANNOTATION,
        "rollout_enabled": SCALEPLAN_ROLLOUT_ENABLED,
        "rollout_annotation": SCALEPLAN_ROLLOUT_ANNOTATION,
        "rollout_intent_annotation": SCALEPLAN_ROLLOUT_INTENT_ANNOTATION,
    },
    "controller_replicas": 1,
    "controller_strategy": "Recreate",
    "controller_boot_generations": {
        "initial_zero_shard_runtime": initial_controller_boot,
        "stopped_external_bootstrap": bootstrap_controller_boot,
        "final_dynamic_runtime": final_controller_boot,
    },
    "holder_replicas": {"PLAYER": 2, "FARM": 2},
    "holder_limits": {
        "cpu": HOLDER_CPU_LIMIT,
        "memory": HOLDER_MEMORY_LIMIT,
        "player": {"cpu": PLAYER_SVR_CPU_LIMIT, "memory": PLAYER_SVR_MEMORY_LIMIT},
        "farm": {"cpu": FARM_SVR_CPU_LIMIT, "memory": FARM_SVR_MEMORY_LIMIT},
    },
    "gate_limits": {"cpu": GATE_CPU_LIMIT, "memory": GATE_MEMORY_LIMIT},
    "source_ip_admission": {
        "GATE_WS_RATE_PER_SEC": GATE_WS_RATE_PER_SEC,
        "GATE_WS_RATE_BURST": GATE_WS_RATE_BURST,
        "GATE_PREAUTH_MAX_CONNECTIONS_PER_IP": GATE_PREAUTH_MAX_CONNECTIONS_PER_IP,
        "GATE_PREAUTH_MAX_BYTES_PER_IP": GATE_PREAUTH_MAX_BYTES_PER_IP,
        "pod_wide_preauth_caps": "product-default",
    },
    "activation_storm_gate": {
        "ACTIVATION_RATE_PER_SEC": ACTIVATION_RATE_PER_SEC or "binary-default-1000",
        "MAX_CONCURRENT_ACTOR_LOADS": MAX_CONCURRENT_ACTOR_LOADS or "binary-default-64",
        "MAX_PENDING_ACTOR_LOADS": MAX_PENDING_ACTOR_LOADS or "binary-default-256",
        "opened_for_pod_fill": bool(
            ACTIVATION_RATE_PER_SEC or MAX_CONCURRENT_ACTOR_LOADS or MAX_PENDING_ACTOR_LOADS
        ),
    },
    "consumer_replicas": {
        "FARM_WORKER": {"farm-worker-a": 1, "farm-worker-b": 1},
        "GATE": 1,
        "H5": 1,
    },
    "shards": {"PLAYER": 4, "FARM": 4, "FARM_WORKER": 4},
    "farm_worker_assignments": {
        "farm-worker-a": {"boot_id": worker_a_boot, "shards": [0, 2]},
        "farm-worker-b": {"boot_id": worker_b_boot, "shards": [1, 3]},
    },
    "assignment_mode": "remote-watch",
    "app_profile": APP_PROFILE,
    "load_test_login_enabled": LOAD_TEST_LOGIN_ENABLED,
    "eligible_for_product_acceptance": (
        not LOAD_TEST_LOGIN_ENABLED and LADDER_WALKED_FROM_FIRST_RUNG
    ),
    "lease_projection_mode": "disabled",
    "farm_worker_authority_renewal": {
        "ttl": "60s",
        "interval": "10s",
        "operation_timeout": "2s",
        "pre_verification_holder_only": True,
    },
    "redis_domain_clients": {
        "authority_secret_key": "REPLACE_WITH_LOCAL_SECRET",
        "runtime_secret_key": "REPLACE_WITH_LOCAL_SECRET",
        "projection_secret_key": "REPLACE_WITH_LOCAL_SECRET",
        "physical_failure_domain_isolation": False,
    },
    "schedule_due_index": {
        "mode": SCHEDULE_DUE_INDEX_MODE,
        "dedicated_ready": SCHEDULE_DUE_INDEX_DEDICATED_READY,
    },
    "friend_opportunity": {
        "wakeup_attach_enabled": OPPORTUNITY_WAKEUP_ATTACH_ENABLED,
        "header_read_mode": OPPORTUNITY_HEADER_READ_MODE,
        "wakeup_enabled": OPPORTUNITY_WAKEUP_ENABLED,
        "wakeup_poll_interval": "1s",
        "wakeup_assignment_mode": OPPORTUNITY_WAKEUP_ASSIGNMENT_MODE,
        "hint_transport": OPPORTUNITY_HINT_TRANSPORT,
        "navigation": "navigator-v3",
        "candidate_window_mode": "top-k",
        "candidate_window_size": 32,
        "dynamic_mode_config_map": "farm-opportunity-mode",
        "initial_stage": OPPORTUNITY_STAGE,
        "ladder_walked_from_first_rung": LADDER_WALKED_FROM_FIRST_RUNG,
        "mode_ladder": [
            {
                "stage": stage,
                "header_read_mode": header_mode,
                "hint_transport": hint_mode,
            }
            for stage, (header_mode, hint_mode) in zip(
                OPPORTUNITY_LADDER_STAGES,
                OPPORTUNITY_MODE_LADDER,
            )
        ],
    },
    "final_production_modes": {
        "hint_transport": "direct",
        "header_read_mode": "narrow",
        "schedule_due_index": "dedicated_only",
        "friend_navigation": "navigator-v3",
        "candidate_window": "top-k",
        "wakeup": "active",
        "wakeup_poll_interval": "1s",
        "worker_assignment": "route",
        "actor_persistence": "authority",
        "authority_membership": "composed",
        "friendship_deployment": "remote_player",
        "gate_transport": "websocket",
        "presence_cleanup": "enabled",
        "redis_logical_domains": ["authority", "runtime", "projection"],
        "theft_prf_mode": "production",
    },
    "gate_downlink_capacity": {
        "writer_max_items": 256,
        "writer_max_bytes": 1048576,
        "hint_max_items": 32,
        "hint_max_bytes": 65536,
        "drain_quantum": 64,
    },
    "gate_downlink_transport_security": "PLAINTEXT_LOCAL_LAB_ONLY",
    "holder_boot_contract": "pod UID on first process, fresh random UUID on every same-Pod process restart",
    "farm_worker_boot_contract": (
        "run-scoped prebound Boot IDs; any Worker restart invalidates the P-1 run"
    ),
    "exact_tls_identities": {"route_controller": route_controller_identity, "route_client": route_client_identity, "route_operator": route_operator_identity, "holder_server": holder_identity, "gate_verifier": gate_verifier_identity},
    "runtime_image_ref": runtime_ref,
    "h5_image_ref": h5_ref,
    "h5_image_receipt_sha256": sha_bytes(H5_IMAGE_RECEIPT.read_bytes()),
    "h5_source_commit": h5_image["source_commit"],
    "h5_source_tree": h5_image["source_tree"],
    "h5_attestation_commit": h5_image.get("attestation_commit"),
    "profile_sha256": sha_bytes(full_bytes),
    "platform_config_sha256": sha_bytes(json.dumps(data, sort_keys=True, separators=(",", ":")).encode()),
    "holders_sha256": sha_bytes(holder_bytes),
    "control_plane_sha256": sha_bytes(control_plane_bytes),
    "final_control_plane_sha256": sha_bytes(final_control_plane_bytes),
    "business_sha256": sha_bytes(business_bytes),
    "phase_order": ["holders", "zero-shard-bootstrap", "initial-controller", "ready-zero-shard", "farm-worker-zero-shard-route-holders", "stopped-external-assignment", "final-controller", "final-route-verification", "business"],
    "public_web_tls": "NOT_CLAIMED",
    "network_policy_enforcement": "NOT_CLAIMED",
    "company_redis_contacted": False,
    "cluster_applied": False,
    "capacity_qualified": False,
}
RECEIPT.parent.mkdir(parents=True, exist_ok=True)
write_exclusive(RECEIPT, (json.dumps(value, sort_keys=True, indent=2) + "\n").encode(), 0o640)
print(json.dumps(value, sort_keys=True, indent=2))
