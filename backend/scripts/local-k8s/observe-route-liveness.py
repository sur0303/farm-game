#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

LAB = Path(os.environ.get("FARM_L2_LAB_ROOT", "/data/farm-l2-r2"))
KUBECONFIG = os.environ.get("KUBECONFIG", str(LAB / "runtime/kubeconfig"))
RUN_ID = os.environ.get("ROUTE_RUN_ID", "")
TTL_SECONDS = int(os.environ.get("ROUTE_LEASE_TTL_SECONDS", "60"))
DURATION_SECONDS = int(os.environ.get("ROUTE_LIVENESS_DURATION_SECONDS", str(3 * TTL_SECONDS)))
INTERVAL_SECONDS = float(os.environ.get("ROUTE_LIVENESS_INTERVAL_SECONDS", "2"))
RECEIPT = LAB / "receipts" / f"route-liveness-{RUN_ID}.json"
APPLY_RECEIPT = LAB / "receipts" / f"route-bootstrap-apply-{RUN_ID}.json"
EVIDENCE_DIR = LAB / "evidence" / f"route-liveness-{RUN_ID}"
HOLDER_OBSERVATION_BUDGET_MS = int(os.environ.get("ROUTE_HOLDER_OBSERVATION_BUDGET_MS", "5000"))
SCHEDULER_MARGIN_MS = int(os.environ.get("ROUTE_LIVENESS_SCHEDULER_MARGIN_MS", "5000"))

if not RUN_ID or TTL_SECONDS < 1 or DURATION_SECONDS < 3 * TTL_SECONDS or INTERVAL_SECONDS <= 0 or HOLDER_OBSERVATION_BUDGET_MS <= 0 or SCHEDULER_MARGIN_MS <= 0:
    raise SystemExit("observe-route-liveness: invalid run/TTL/duration/budget")
if not APPLY_RECEIPT.is_file():
    raise SystemExit("observe-route-liveness: bootstrap apply receipt missing")
apply_receipt = json.loads(APPLY_RECEIPT.read_text())
if apply_receipt.get("status") != "BOOTSTRAP_READY_3TTL_NOT_PROVEN" or apply_receipt.get("rollout_ready") is not True:
    raise SystemExit("observe-route-liveness: bootstrap is not ready")

env = dict(os.environ, KUBECONFIG=KUBECONFIG)

def kubectl(*args: str) -> str:
    return subprocess.check_output(["kubectl", *args], env=env, text=True, stderr=subprocess.DEVNULL)


def pod_json(pod: str, path: str, port: int = 8081) -> dict:
    return json.loads(kubectl("exec", "-n", "farm-l2-r2", pod, "--", "wget", "-qO-", f"http://127.0.0.1:{port}{path}"))


def generation_view(snapshot: dict) -> dict:
    def domain_view(name: str) -> dict:
        domain = snapshot[name]
        return {
            "shard_count": int(domain["shard_count"]),
            "routes": [{
                "holder_index": int(route["holder_index"]),
                "state": route["state"],
                "shard_revision": int(route["shard_revision"]),
                "expected_writer_epoch": int(route["expected_writer_epoch"]),
                "expected_lease_epoch": int(route["expected_lease_epoch"]),
            } for route in domain["routes"]],
        }
    return {
        "realm": snapshot["realm"], "hash_version": int(snapshot["hash_version"]), "hash_seed": int(snapshot["hash_seed"]),
        "holders": snapshot["holders"], "player": domain_view("player"), "farm": domain_view("farm"),
    }


def canonical_sha(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def holder_identity_view(evidence: dict) -> dict:
    result: dict[str, list[dict]] = {}
    for domain in ("PLAYER", "FARM"):
        records = evidence.get("holders", {}).get(domain, [])
        if len(records) != 2:
            raise RuntimeError(f"{domain}_HOLDER_REGISTRY_CARDINALITY")
        rows = []
        for record in records:
            if record.get("state") != "SERVING" or record.get("process_ready") is not True or record.get("draining") is not False or int(record.get("serving_shard_count", 0)) != 512:
                raise RuntimeError(f"{domain}_HOLDER_NOT_FACTUALLY_SERVING")
            rows.append({key: record.get(key) for key in (
                "domain", "holder_id", "boot_id", "logic_endpoint", "holder_control_endpoint",
                "tls_dns_identity", "started_at", "capacity_weight", "resource_summary",
            )})
        result[domain] = sorted(rows, key=lambda row: (str(row["holder_id"]), str(row["boot_id"])))
    return result


def capture_holder_runtime(observed: dict[str, list[dict]]) -> dict:
    result: dict[str, dict] = {}
    for component in ("player-svr", "farm-svr"):
        for pod in sorted(observed[component], key=lambda row: row["pod"]):
            port = 9101 if component == "player-svr" else 9091
            ready = pod_json(pod["pod"], "/readyz", port)
            runtime = ready.get("route_runtime")
            if ready.get("status") != "ready" or not isinstance(runtime, dict):
                raise RuntimeError(f"{pod['pod']}_ROUTE_RUNTIME_EVIDENCE_MISSING")
            membership = runtime.get("membership", {})
            if runtime.get("process_ready") is not True or runtime.get("assignment_healthy") is not True or runtime.get("fatal_error_present") is not False or \
                    int(runtime.get("serving_shard_count", 0)) != 512 or int(runtime.get("lease_scheduler_entries", 0)) != 512 or \
                    int(membership.get("installed_memberships", 0)) != 512 or int(membership.get("pending_actions", -1)) != 0 or \
                    int(membership.get("degraded_shards", -1)) != 0 or int(membership.get("inflight_actions", -1)) != 0:
                raise RuntimeError(f"{pod['pod']}_ROUTE_RUNTIME_NOT_STABLE")
            result[pod["pod"]] = runtime
    if len(result) != 4:
        raise RuntimeError("HOLDER_RUNTIME_EVIDENCE_CARDINALITY")
    return result


def holder_runtime_stable_view(evidence: dict) -> dict:
    return {pod: {
        "domain": value.get("domain"), "holder_id": value.get("holder_id"), "holder_boot_id": value.get("holder_boot_id"),
        "serving_shard_count": int(value.get("serving_shard_count", 0)),
        "lease_scheduler_entries": int(value.get("lease_scheduler_entries", 0)),
        "action_token_sequence": int(value.get("membership", {}).get("action_token_sequence", 0)),
        "successful_adds": int(value.get("membership", {}).get("successful_adds", 0)),
        "successful_replaces": int(value.get("membership", {}).get("successful_replaces", 0)),
        "successful_removes": int(value.get("membership", {}).get("successful_removes", 0)),
    } for pod, value in sorted(evidence.items())}


def sample_from_controller_evidence(evidence: dict, observed_at_ms: int) -> dict:
    if evidence.get("status") != "fresh" or int(evidence.get("single_controller_publisher_count", 0)) != 1:
        raise RuntimeError("CONTROLLER_EVIDENCE_NOT_SINGLE_FRESH_PUBLISHER")
    projection = evidence.get("projection", {})
    snapshot = evidence.get("snapshot", {})
    revision = int(projection.get("global_revision", 0))
    minimum = int(projection.get("minimum_lease_until_ms", 0))
    if projection.get("known_fresh") is not True or projection.get("failure_class", "") or revision == 0 or int(snapshot.get("global_revision", 0)) != revision:
        raise RuntimeError("CONTROLLER_EVIDENCE_REVISION_RACE")
    return {
        "observed_at_ms": observed_at_ms, "global_revision": revision,
        "minimum_remaining_lease_ms": minimum - observed_at_ms,
        "observed_shards": int(projection.get("observed_shards", 0)),
        "projection_attempts": int(projection.get("projection_attempts", 0)),
        "projection_successes": int(projection.get("projection_successes", 0)),
        "maximum_successful_wave_duration_ms": int(projection.get("maximum_successful_wave_duration_ms", 0)),
    }


def write_json_immutable(path: Path, value: object) -> None:
    if path.exists():
        raise RuntimeError(f"IMMUTABLE_EVIDENCE_EXISTS:{path.name}")
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}-", dir=path.parent, text=True)
    try:
        with os.fdopen(fd, "w") as out:
            json.dump(value, out, sort_keys=True, indent=2)
            out.write("\n")
            out.flush()
            os.fsync(out.fileno())
        os.chmod(temporary, 0o640)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


if kubectl("config", "current-context").strip() != "farm-l2-r2":
    raise SystemExit("observe-route-liveness: refusing non-lab context")

started_wall_ms = int(time.time() * 1000)
started = time.monotonic()
end = started + DURATION_SECONDS
samples: list[dict] = []
identity: dict[str, dict] | None = None
last_revision = 0
failure = ""
first_controller_evidence: dict | None = None
last_controller_evidence: dict | None = None
first_holder_runtime_evidence: dict | None = None
last_holder_runtime_evidence: dict | None = None
controller_pod = ""

while True:
    sample_started = time.monotonic()
    now_ms = int(time.time() * 1000)
    try:
        pods = json.loads(kubectl("get", "pods", "-n", "farm-l2-r2", "-o", "json"))["items"]
        wanted = {"route-controller": 1, "player-svr": 2, "farm-svr": 2}
        observed: dict[str, list[dict]] = {name: [] for name in wanted}
        for pod in pods:
            labels = pod.get("metadata", {}).get("labels", {})
            name = labels.get("app.kubernetes.io/name")
            if name not in wanted:
                continue
            statuses = pod.get("status", {}).get("containerStatuses", [])
            ready_condition = any(c.get("type") == "Ready" and c.get("status") == "True" for c in pod.get("status", {}).get("conditions", []))
            observed[name].append({
                "pod": pod["metadata"]["name"], "uid": pod["metadata"]["uid"], "ready": ready_condition,
                "restart_count": sum(int(c.get("restartCount", 0)) for c in statuses),
                "image_ids": sorted(c.get("imageID", "") for c in statuses),
            })
        for name, count in wanted.items():
            if len(observed[name]) != count or any(not p["ready"] or p["restart_count"] != 0 for p in observed[name]):
                raise RuntimeError(f"{name.upper()}_POD_NOT_STABLE")
        current_identity = {name: {p["pod"]: {"uid": p["uid"], "image_ids": p["image_ids"]} for p in rows} for name, rows in observed.items()}
        if identity is None:
            identity = current_identity
        elif identity != current_identity:
            raise RuntimeError("POD_OR_IMAGE_IDENTITY_CHANGED")

        controller_pod = observed["route-controller"][0]["pod"]
        readiness = pod_json(controller_pod, "/readyz")
        revision = int(readiness.get("global_revision", 0))
        minimum = int(readiness.get("minimum_lease_until_ms", 0))
        if readiness.get("known_fresh") is not True or readiness.get("failure_class", "") or int(readiness.get("observed_shards", 0)) != 2048:
            raise RuntimeError("HOLDER_ROUTE_CONTROL_FRESHNESS_UNKNOWN")
        if revision < last_revision or revision == 0:
            raise RuntimeError("GLOBAL_REVISION_ROLLBACK_OR_ZERO")
        if minimum - now_ms < 15_000:
            raise RuntimeError("PROJECTED_LEASE_BUDGET_TOO_SMALL")
        last_revision = revision
        samples.append({
            "observed_at_ms": now_ms, "global_revision": revision,
            "minimum_remaining_lease_ms": minimum - now_ms,
            "observed_shards": int(readiness["observed_shards"]),
            "projection_attempts": int(readiness.get("projection_attempts", 0)),
            "projection_successes": int(readiness.get("projection_successes", 0)),
            "maximum_successful_wave_duration_ms": int(readiness.get("maximum_successful_wave_duration_ms", 0)),
        })
        if first_controller_evidence is None:
            first_controller_evidence = pod_json(controller_pod, "/evidencez")
            evidence_sample = sample_from_controller_evidence(first_controller_evidence, int(time.time() * 1000))
            if evidence_sample["global_revision"] < revision:
                raise RuntimeError("FIRST_EVIDENCE_REVISION_ROLLBACK")
            samples[-1] = evidence_sample
            last_revision = evidence_sample["global_revision"]
            first_holder_runtime_evidence = capture_holder_runtime(observed)
    except Exception as exc:
        failure = str(exc)
        break
    if time.monotonic() >= end:
        break
    sleep_for = INTERVAL_SECONDS - (time.monotonic() - sample_started)
    if sleep_for > 0:
        time.sleep(sleep_for)

ended_wall_ms = int(time.time() * 1000)
elapsed = time.monotonic() - started
snapshot_evidence: dict[str, dict] = {}
generation_fingerprint = ""
holder_identity_fingerprint = ""
holder_runtime_evidence: dict[str, dict] = {}
lease_budget_evidence: dict[str, int | bool] = {}
control_plane_counters: dict[str, int | bool] = {}
if not failure:
    try:
        last_controller_evidence = pod_json(controller_pod, "/evidencez")
        final_sample = sample_from_controller_evidence(last_controller_evidence, int(time.time() * 1000))
        if final_sample["global_revision"] < last_revision:
            raise RuntimeError("FINAL_EVIDENCE_REVISION_ROLLBACK")
        if final_sample["global_revision"] > samples[-1]["global_revision"]:
            samples.append(final_sample)
        else:
            samples[-1] = final_sample
        last_holder_runtime_evidence = capture_holder_runtime(observed)
        first_holder_runtime_stable = holder_runtime_stable_view(first_holder_runtime_evidence or {})
        last_holder_runtime_stable = holder_runtime_stable_view(last_holder_runtime_evidence)
        if first_holder_runtime_stable != last_holder_runtime_stable:
            raise RuntimeError("MEMBERSHIP_GUARD_OR_SCHEDULER_CHURN_DETECTED")
        if any(value["successful_replaces"] != 0 or value["successful_removes"] != 0 for value in last_holder_runtime_stable.values()):
            raise RuntimeError("MEMBERSHIP_REPLACE_OR_REMOVE_DETECTED")
        holder_runtime_evidence = {"first": first_holder_runtime_evidence or {}, "last": last_holder_runtime_evidence}
        first_snapshot = first_controller_evidence["snapshot"] if first_controller_evidence else {}
        last_snapshot = last_controller_evidence["snapshot"]
        first_generation = generation_view(first_snapshot)
        last_generation = generation_view(last_snapshot)
        if first_generation != last_generation:
            raise RuntimeError("DEADLINE_PROJECTION_CHANGED_MEMBERSHIP_GENERATION")
        generation_fingerprint = canonical_sha(first_generation)
        first_holders = holder_identity_view(first_controller_evidence or {})
        last_holders = holder_identity_view(last_controller_evidence)
        if first_holders != last_holders:
            raise RuntimeError("HOLDER_IDENTITY_CHANGED_DURING_OBSERVATION")
        holder_identity_fingerprint = canonical_sha(first_holders)
        for domain_name in ("player", "farm"):
            first_routes = first_snapshot[domain_name]["routes"]
            last_routes = last_snapshot[domain_name]["routes"]
            if len(first_routes) != 1024 or len(last_routes) != 1024:
                raise RuntimeError("PARTIAL_SNAPSHOT_EVIDENCE")
            for before, after in zip(first_routes, last_routes):
                if int(after.get("lease_expire_at_ms", 0)) < int(before.get("lease_expire_at_ms", 0)):
                    raise RuntimeError("DEADLINE_ROLLBACK_IN_EVIDENCE")
        final_projection = last_controller_evidence["projection"]
        failure_fields = (
            "observation_timeout_failures", "stale_callback_rejections", "insufficient_deadline_failures",
            "publish_pending_failures", "authority_observation_failures",
        )
        if any(int(final_projection.get(field, 0)) != 0 for field in failure_fields):
            raise RuntimeError("PROJECTION_FAILURE_COUNTER_NONZERO")
        attempts = int(final_projection.get("projection_attempts", 0))
        successes = int(final_projection.get("projection_successes", 0))
        if attempts == 0 or attempts != successes:
            raise RuntimeError("PROJECTION_ATTEMPT_NOT_SUCCESSFUL")
        minimum_observed = min(s["minimum_remaining_lease_ms"] for s in samples)
        projection_interval_ms = int(final_projection.get("projection_interval_ms", 0))
        maximum_wave_ms = int(final_projection.get("maximum_successful_wave_duration_ms", 0))
        observed_pipeline_budget_ms = projection_interval_ms + maximum_wave_ms + HOLDER_OBSERVATION_BUDGET_MS + int(INTERVAL_SECONDS * 1000) + SCHEDULER_MARGIN_MS
        if projection_interval_ms <= 0 or maximum_wave_ms < 0 or observed_pipeline_budget_ms >= minimum_observed:
            raise RuntimeError("OBSERVED_PIPELINE_BUDGET_NOT_BELOW_MINIMUM_LEASE")
        lease_budget_evidence = {
            "projection_interval_ms": projection_interval_ms,
            "maximum_successful_wave_duration_ms": maximum_wave_ms,
            "holder_observation_budget_ms": HOLDER_OBSERVATION_BUDGET_MS,
            "sample_interval_ms": int(INTERVAL_SECONDS * 1000),
            "scheduler_margin_ms": SCHEDULER_MARGIN_MS,
            "observed_pipeline_budget_ms": observed_pipeline_budget_ms,
            "minimum_observed_remaining_lease_ms": minimum_observed,
            "budget_strictly_below_minimum_remaining": True,
        }
        control_plane_counters = {
            "route_controller_publisher_count": int(last_controller_evidence.get("single_controller_publisher_count", 0)),
            "duplicate_publisher_detected": False,
            "projection_attempts": attempts, "projection_successes": successes,
            "stale_callback_rejections": 0, "stale_callback_commits": 0,
            "partial_snapshot_publishes": 0, "pending_unknown_unresolved": 0, "deadline_rollbacks": 0,
        }
        first_path = EVIDENCE_DIR / "first-route-snapshot.json"
        last_path = EVIDENCE_DIR / "last-route-snapshot.json"
        write_json_immutable(first_path, first_snapshot)
        write_json_immutable(last_path, last_snapshot)
        snapshot_evidence = {
            "first": {"path": str(first_path), "sha256": hashlib.sha256(first_path.read_bytes()).hexdigest(), "global_revision": int(first_snapshot["global_revision"])},
            "last": {"path": str(last_path), "sha256": hashlib.sha256(last_path.read_bytes()).hexdigest(), "global_revision": int(last_snapshot["global_revision"])},
        }
    except Exception as exc:
        failure = str(exc)

passed = not failure and elapsed >= DURATION_SECONDS and len(samples) >= int(DURATION_SECONDS / INTERVAL_SECONDS)
if passed and (samples[-1]["global_revision"] <= samples[0]["global_revision"] or len({s["global_revision"] for s in samples}) < 3):
    passed = False
    failure = "FRESH_PROJECTION_REVISION_DID_NOT_ADVANCE"

value = {
    "schema_version": "farm-local-holder-route-control-liveness-receipt.v3",
    "milestone": "CP-LOCAL-ROUTE-RUNTIME-R2-CANDIDATE",
    "status": "HOLDER_ROUTE_CONTROL_LIVENESS_LOCAL_PASS" if passed else "HOLDER_ROUTE_CONTROL_LIVENESS_BLOCKER",
    "tested_components": {
        "route_authority_init": "PASS" if passed else "BLOCKED",
        "route_control_plane_liveness": "PASS" if passed else "BLOCKED",
        "holder_membership_liveness": "PASS" if passed else "BLOCKED",
        "player_holders": "PASS" if passed else "BLOCKED",
        "farm_holders": "PASS" if passed else "BLOCKED",
    },
    "not_tested": [
        "gate_route_consumer_liveness",
        "worker_route_consumer_liveness",
        "full_public_command_path",
        "full_product_functional",
        "capacity_qualification",
    ],
    "run_id": RUN_ID,
    "started_at_ms": started_wall_ms,
    "ended_at_ms": ended_wall_ms,
    "elapsed_ms": int(elapsed * 1000),
    "lease_ttl_ms": TTL_SECONDS * 1000,
    "required_duration_ms": 3 * TTL_SECONDS * 1000,
    "sample_interval_ms": int(INTERVAL_SECONDS * 1000),
    "successful_samples": len(samples),
    "first_revision": samples[0]["global_revision"] if samples else 0,
    "last_revision": samples[-1]["global_revision"] if samples else 0,
    "minimum_observed_remaining_lease_ms": min((s["minimum_remaining_lease_ms"] for s in samples), default=0),
    "pod_and_image_identity": identity or {},
    "timeline": samples,
    "snapshot_evidence": snapshot_evidence,
    "generation_fingerprint": generation_fingerprint,
    "holder_identity_fingerprint": holder_identity_fingerprint,
    "holder_runtime_evidence": holder_runtime_evidence,
    "membership_guard_scheduler_churn_detected": not passed or not bool(holder_runtime_evidence),
    "deadline_only_projection_generation_stable": passed and bool(generation_fingerprint),
    "control_plane_counters": control_plane_counters,
    "lease_budget_evidence": lease_budget_evidence,
    "formal_frozen_observation_only": True,
    "informative_post_window_observation_included": False,
    "failure_class": failure,
    "cluster_applied": True,
    "three_ttl_proven": passed,
    "holder_route_control_liveness_proven": passed,
    "full_product_route_liveness_proven": False,
    "ready_for_independent_review": passed,
    "local_provisional_binding_eligible": False,
    "activatable": False,
    "active_binding_authorized": False,
    "company_redis_contacted": False,
    "formal_l2_authorized": False,
    "capacity_constant_eligible": False,
    "supports_30m_dau": False,
}
RECEIPT.parent.mkdir(parents=True, exist_ok=True)
fd, temporary = tempfile.mkstemp(prefix=".route-liveness-", dir=RECEIPT.parent, text=True)
with os.fdopen(fd, "w") as out:
    json.dump(value, out, sort_keys=True, indent=2)
    out.write("\n")
    out.flush()
    os.fsync(out.fileno())
os.replace(temporary, RECEIPT)
os.chmod(RECEIPT, 0o640)
print(json.dumps({k: value[k] for k in (
    "status", "run_id", "elapsed_ms", "successful_samples", "first_revision", "last_revision",
    "minimum_observed_remaining_lease_ms", "failure_class", "three_ttl_proven",
    "active_binding_authorized", "company_redis_contacted", "supports_30m_dau",
)}, sort_keys=True, indent=2))
raise SystemExit(0 if passed else 1)
