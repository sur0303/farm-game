#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import tempfile
from pathlib import Path

LAB = Path(os.environ.get("FARM_L2_LAB_ROOT", "/data/farm-l2-r2"))
RUN_ID = os.environ.get("ROUTE_RUN_ID", "").strip()
REPO = Path(__file__).resolve().parents[2]


def fail(message: str) -> None:
    raise SystemExit(f"finalize-route-runtime-candidate: {message}")


def load(path: Path) -> dict:
    if not path.is_file() or not path.is_symlink() and not path.exists():
        fail(f"required receipt missing: {path}")
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        fail(f"invalid receipt {path}: {exc}")
    if not isinstance(value, dict):
        fail(f"receipt is not an object: {path}")
    return value


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def require_false(receipt: dict, *fields: str) -> None:
    for field in fields:
        if receipt.get(field) is not False:
            fail(f"{field} must be exactly false")


def main() -> None:
    if not RUN_ID:
        fail("ROUTE_RUN_ID is required")
    paths = {
        "static_init": LAB / "receipts/static-init" / RUN_ID / "static-init-receipt.json",
        "bootstrap_apply": LAB / "receipts" / f"route-bootstrap-apply-{RUN_ID}.json",
        "holder_route_liveness": LAB / "receipts" / f"route-liveness-{RUN_ID}.json",
        "runtime_image": LAB / "receipts" / f"route-runtime-image-{RUN_ID}.json",
        "route_control_tls": LAB / "receipts" / f"route-control-tls-{RUN_ID}.json",
        "bootstrap_render_receipt": LAB / "receipts" / f"route-bootstrap-render-{RUN_ID}.json",
        "identity": LAB / "runtime/static-init" / RUN_ID / "identity.json",
    }
    artifact_paths = {
        "route_snapshot": LAB / "runtime/static-init" / RUN_ID / "route-snapshot.json",
        "bootstrap_render": LAB / "runtime" / f"route-bootstrap-{RUN_ID}" / "bootstrap-render.yaml",
    }
    receipts = {name: load(path) for name, path in paths.items()}
    static = receipts["static_init"]
    apply = receipts["bootstrap_apply"]
    liveness = receipts["holder_route_liveness"]
    image = receipts["runtime_image"]
    tls = receipts["route_control_tls"]
    render_receipt = receipts["bootstrap_render_receipt"]
    identity = receipts["identity"]

    for name, receipt in receipts.items():
        if receipt.get("run_id") != RUN_ID:
            fail(f"{name} run identity mismatch")
    if static.get("schema_version") != "farm-local-static-route-init-receipt.v2" or \
            static.get("status") != "INITIALIZED_LOCAL_PROFILE_B_NOT_QUALIFIED" or \
            static.get("counters", {}).get("verified") != 2048 or static.get("durable_snapshot_revision") != 1 or \
            static.get("complete") is not True or static.get("bootstrap_materializable") is not True or \
            static.get("activatable") is not False:
        fail("static init is not exact, product-inert 2048/2048 durable revision 1")
    if static.get("binding_scope") != "LOCAL_FUNCTIONAL_ONLY":
        fail("static init binding scope mismatch")
    if apply.get("status") != "BOOTSTRAP_READY_3TTL_NOT_PROVEN" or apply.get("rollout_ready") is not True or \
            apply.get("server_side_apply") is not True or apply.get("smt_admission_probe_passed") is not True:
        fail("bootstrap apply receipt is not the reviewed ready state")
    if liveness.get("schema_version") != "farm-local-holder-route-control-liveness-receipt.v3" or \
            liveness.get("milestone") != "CP-LOCAL-ROUTE-RUNTIME-R2-CANDIDATE" or \
            liveness.get("status") != "HOLDER_ROUTE_CONTROL_LIVENESS_LOCAL_PASS" or \
            liveness.get("three_ttl_proven") is not True or \
            liveness.get("holder_route_control_liveness_proven") is not True or \
            liveness.get("full_product_route_liveness_proven") is not False or \
            liveness.get("deadline_only_projection_generation_stable") is not True or \
            liveness.get("membership_guard_scheduler_churn_detected") is not False or \
            liveness.get("lease_budget_evidence", {}).get("budget_strictly_below_minimum_remaining") is not True:
        fail("holder route-control liveness scope/generation/budget is not proven exactly")
    expected_tested = {
        "route_authority_init": "PASS",
        "route_control_plane_liveness": "PASS",
        "holder_membership_liveness": "PASS",
        "player_holders": "PASS",
        "farm_holders": "PASS",
    }
    if liveness.get("tested_components") != expected_tested:
        fail("tested component scope mismatch")
    required_not_tested = {
        "gate_route_consumer_liveness", "worker_route_consumer_liveness",
        "full_public_command_path", "full_product_functional", "capacity_qualification",
    }
    if not required_not_tested.issubset(set(liveness.get("not_tested", []))):
        fail("not-tested scope is incomplete")
    if image.get("status") != "PUBLISHED_LOCAL_ROUTE_RUNTIME_NOT_DEPLOYED" or not image.get("registry_ref", "").startswith("127.0.0.1:5001/"):
        fail("runtime image receipt mismatch")
    if tls.get("status") != "MATERIALIZED_LOCAL_FUNCTIONAL_ONLY" or tls.get("leaf_exactly_one_dns_san") is not True or tls.get("leaf_common_name_present") is not False:
        fail("route-control TLS receipt mismatch")
    if render_receipt.get("status") != "BOOTSTRAP_RENDER_FRESH_NOT_3TTL_PROVEN" or render_receipt.get("activatable") is not False:
        fail("bootstrap render receipt mismatch")
    if identity.get("generation", 0) <= 0 or identity.get("profile_id") != "functional" or not identity.get("controller_nonce"):
        fail("static identity is incomplete")
    for name, path in artifact_paths.items():
        if not path.is_file():
            fail(f"required immutable artifact missing: {name}")
    route_snapshot_sha = sha256(artifact_paths["route_snapshot"])
    render_sha = sha256(artifact_paths["bootstrap_render"])
    static_receipt_sha = sha256(paths["static_init"])
    tls_receipt_sha = sha256(paths["route_control_tls"])
    if route_snapshot_sha != static.get("route_snapshot_sha256") or route_snapshot_sha != render_receipt.get("route_snapshot_sha256"):
        fail("route snapshot SHA binding mismatch")
    if render_sha != apply.get("render_sha256") or render_sha != render_receipt.get("render_sha256"):
        fail("rendered manifest SHA binding mismatch")
    if render_receipt.get("static_init_receipt_sha256") != static_receipt_sha or render_receipt.get("route_tls_receipt_sha256") != tls_receipt_sha:
        fail("render receipt input SHA binding mismatch")
    if render_receipt.get("runtime_image_ref") != image.get("registry_ref") or render_receipt.get("source_head") != image.get("source_head"):
        fail("render runtime image/source binding mismatch")

    pod_images = []
    for component in ("route-controller", "player-svr", "farm-svr"):
        for pod in liveness.get("pod_and_image_identity", {}).get(component, {}).values():
            pod_images.extend(pod.get("image_ids", []))
    if len(pod_images) != 5 or any(value != image.get("registry_ref") for value in pod_images):
        fail("Pod imageID is not exactly bound to the runtime image receipt")
    counters = liveness.get("control_plane_counters", {})
    zero_counter_fields = (
        "stale_callback_rejections", "stale_callback_commits", "partial_snapshot_publishes",
        "pending_unknown_unresolved", "deadline_rollbacks",
    )
    if counters.get("route_controller_publisher_count") != 1 or counters.get("duplicate_publisher_detected") is not False or \
            any(counters.get(field) != 0 for field in zero_counter_fields) or \
            counters.get("projection_attempts", 0) != counters.get("projection_successes", -1):
        fail("single Publisher/projection counter evidence mismatch")
    if len(liveness.get("generation_fingerprint", "")) != 64 or len(liveness.get("holder_identity_fingerprint", "")) != 64:
        fail("deadline-only generation/holder identity evidence missing")
    for label in ("first", "last"):
        item = liveness.get("snapshot_evidence", {}).get(label, {})
        evidence_path = Path(item.get("path", ""))
        expected_path = LAB / "evidence" / f"route-liveness-{RUN_ID}" / f"{label}-route-snapshot.json"
        if evidence_path != expected_path or not evidence_path.is_file() or sha256(evidence_path) != item.get("sha256"):
            fail(f"{label} snapshot evidence path/hash mismatch")

    for receipt in (static, apply, liveness, image, render_receipt):
        require_false(receipt, "active_binding_authorized", "supports_30m_dau", "company_redis_contacted")
    require_false(tls, "active_binding_authorized", "capacity_constant_eligible", "company_redis_contacted")
    require_false(liveness, "activatable", "formal_l2_authorized", "capacity_constant_eligible", "local_provisional_binding_eligible")

    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip()
    dirty = subprocess.check_output(["git", "status", "--short"], cwd=REPO, text=True)
    if dirty:
        fail("source worktree is dirty")
    if image.get("source_head") != head or image.get("verification_identity") != head:
        fail("runtime image is not bound to the exact clean source head")

    value = {
        "schema_version": "farm-cp-local-route-runtime-r2-candidate.v2",
        "checkpoint": "CP-LOCAL-ROUTE-LIVENESS-R2",
        "milestone": "CP-LOCAL-ROUTE-RUNTIME-R2-CANDIDATE",
        "status": "READY_FOR_INDEPENDENT_REVIEW",
        "review_status": "PENDING_INDEPENDENT_A_B_INTEGRATION_REVIEW",
        "run_id": RUN_ID,
        "business_identity": image.get("business_identity"),
        "verification_identity": head,
        "runtime_image": image["registry_ref"],
        "generation": identity["generation"],
        "profile_id": identity["profile_id"],
        "controller_nonce": identity["controller_nonce"],
        "route_authority_init": "PASS",
        "route_control_plane_liveness": "PASS",
        "holder_membership_liveness": "PASS",
        "route_controller_recovery": "PENDING",
        "gate_route_consumer_liveness": "NOT_RUN",
        "worker_route_consumer_liveness": "NOT_RUN",
        "full_product_functional": "NOT_RUN",
        "capacity_qualification": "NOT_RUN",
        "three_ttl_proven": True,
        "ready_for_full_functional_integration": False,
        "full_product_route_liveness_proven": False,
        "local_provisional_binding_eligible": False,
        "activatable": False,
        "active_binding_authorized": False,
        "formal_l2_authorized": False,
        "capacity_constant_eligible": False,
        "supports_30m_dau": False,
        "company_redis_contacted": False,
        "formal_frozen_observation_ms": liveness["elapsed_ms"],
        "informative_post_window_observation_included": False,
        "deadline_only_projection_generation_stable": True,
        "generation_fingerprint": liveness["generation_fingerprint"],
        "holder_identity_fingerprint": liveness["holder_identity_fingerprint"],
        "deadline_projection_snapshots": liveness["snapshot_evidence"],
        "holder_runtime_evidence": liveness["holder_runtime_evidence"],
        "control_plane_counters": counters,
        "lease_budget_evidence": liveness["lease_budget_evidence"],
        "identity_binding": {
            "source_head": head,
            "runtime_image_ref": image["registry_ref"],
            "pod_image_ids": sorted(pod_images),
            "rendered_manifest_sha256": render_sha,
            "route_snapshot_sha256": route_snapshot_sha,
            "static_init_receipt_sha256": static_receipt_sha,
        },
        "evidence": {
            **{name: {"path": str(path), "sha256": sha256(path)} for name, path in paths.items()},
            **{name: {"path": str(path), "sha256": sha256(path)} for name, path in artifact_paths.items()},
        },
    }
    output = LAB / "receipts" / f"cp-local-route-runtime-r2-candidate-{RUN_ID}.json"
    if output.exists():
        fail(f"immutable candidate already exists: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".cp-local-route-runtime-r2-", dir=output.parent, text=True)
    try:
        with os.fdopen(fd, "w") as target:
            json.dump(value, target, sort_keys=True, indent=2)
            target.write("\n")
            target.flush()
            os.fsync(target.fileno())
        os.chmod(temporary, 0o640)
        os.replace(temporary, output)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    print(json.dumps({"path": str(output), "sha256": sha256(output), **value}, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()
