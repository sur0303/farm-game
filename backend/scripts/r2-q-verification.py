#!/usr/bin/env python3
"""Fail-closed R2-Q verification coordinator.

The coordinator owns evidence composition, not product behavior.  It records
candidate identity, configuration identity, planned/actual tests, and the
environment in which an observation was made.  A gate can become PASS only
from an explicit, schema-shaped evidence record with an exact candidate
binding.  Files merely existing never produce a product-gate PASS.

The command is intentionally read-only unless an operator explicitly selects
one of the existing test commands with ``--run``.  Even selected commands are
given a unique namespace and a no-FLUSHALL policy; their output is preserved
and redacted.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, urlopen


R2_BASE_COMMIT = "54c6d0a15cd2e0555d309b59319f1ce30fab268c"
R2_H5_BASE_COMMIT = "e3ce394cf77bbe111755fcd5dc576dfd4a557d34"
TOOL_VERSION = "farm.r2-q.verification/1"
RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
HEX_COMMIT_RE = re.compile(r"^[0-9a-fA-F]{7,64}$")
LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1", "0.0.0.0"}
EXPECTED_NAMESPACE = "farm-staging-system"
PASS = "PASS"
FAIL = "FAIL"
SKIPPED = "SKIPPED_NOT_COMPOSED"
BLOCKED = "BLOCKED_ENVIRONMENT"
INVALID = "INVALID_EVIDENCE"
OBSERVED_LOCAL = "OBSERVED_LOCAL_ONLY"
INCONCLUSIVE = "INCONCLUSIVE"
NOT_RUN = "NOT_RUN"

ALLOWED_EVIDENCE_STATUSES = {PASS, FAIL, SKIPPED, BLOCKED, OBSERVED_LOCAL, INCONCLUSIVE}
FRIENDSHIP_SERVICE_MODES = {"local_gate", "remote_shadow", "remote_player"}
FRIEND_LEGACY_MODES = {"write", "shadow_read", "unavailable", "retired"}
ACTOR_LAYOUT_MODES = {"v1", "v2", "mixed_read", "static_holder"}
REQUIRED_TOOL_NAMES = (
    "r2_entry",
    "r2_python",
    "r1_entry",
    "hero",
    "wss_200",
    "wss_driver",
    "actor_layout_inspector",
    "lease_200",
    "p1_k8s_validator",
    "p1_k8s_test",
)


def _spec(
    gate_id: str,
    area: str,
    *,
    environment: str = "local",
    fixture: str | None = None,
    assertions: tuple[str, ...] = (),
    test_ids: tuple[str, ...] = (),
) -> dict[str, Any]:
    return {
        "id": gate_id,
        "area": area,
        "required_environment": environment,
        "fixture_id": fixture,
        "required_assertions": list(assertions),
        "test_ids": list(test_ids),
    }


GATE_SPECS: list[dict[str, Any]] = [
    _spec("friendship.boundary.assembly", "friendship_boundary", fixture="friendship-boundary-v1", assertions=("assembly_complete",)),
    _spec("friendship.boundary.shadow_no_duplicate_write", "friendship_boundary", fixture="friendship-boundary-v1", assertions=("formal_result_old_path", "shadow_write_count", "shadow_write_count_is_zero")),
    _spec("friendship.boundary.comparison_fields", "friendship_boundary", fixture="friendship-boundary-v1", assertions=("comparison_fields", "difference_counters")),
    _spec("friendship.boundary.remote_player_no_friend_authority", "friendship_boundary", fixture="friendship-boundary-v1", assertions=("gate_friend_authority_repositories", "gate_friend_repair_workers", "gate_friend_lua_or_keys", "remote_player_clean")),
    _spec("friendship.boundary.visit_permit", "friendship_boundary", fixture="friendship-boundary-v1", assertions=("visit_permit",)),
    _spec("friendship.boundary.action_admission", "friendship_boundary", fixture="friendship-boundary-v1", assertions=("action_admission",)),
    _spec("friendship.boundary.public_block_unblock_absent", "friendship_boundary", fixture="friendship-boundary-v1", assertions=("public_block_unblock_absent",)),
    _spec("friendship.canonical.pair_projection_failure", "friendship_canonical", fixture="friendship-canonical-only-v1", assertions=("pair_success", "projection_failure", "canonical_success", "repair_eventually")),
    _spec("friendship.canonical.delete_stale_index", "friendship_canonical", fixture="friendship-canonical-only-v1", assertions=("pair_delete", "stale_index", "list_hidden_or_rejected", "permit_rejected", "index_cleaned")),
    _spec("friendship.canonical.legacy_unavailable", "friendship_canonical", fixture="friendship-canonical-only-v1", assertions=("legacy_unavailable", "canonical_business_healthy")),
    _spec("friendship.canonical.shadow_differences_zero", "friendship_canonical", fixture="friendship-canonical-only-v1", assertions=("shadow_difference_total", "shadow_difference_total_is_zero")),
    _spec("friendship.canonical.no_legacy_write_or_result", "friendship_canonical", fixture="friendship-canonical-only-v1", assertions=("legacy_write_count", "legacy_write_count_is_zero", "formal_result_source")),
    _spec("model.v2.field_equality", "model_field_v2", fixture="model-field-v2-v1", assertions=("v1_v2_field_equality", "v1_v2_business_state_equal"), test_ids=("model.v2.goldens",)),
    _spec("model.v2.one_writer_claim_record_version", "model_field_v2", fixture="model-field-v2-v1", assertions=("writer_claim_count", "record_version_count", "one_writer_claim", "one_record_version"), test_ids=("model.v2.fence_matrix",)),
    _spec("model.v2.no_dual_write", "model_field_v2", fixture="model-field-v2-v1", assertions=("v1_write_count", "v2_write_count", "no_dual_write"), test_ids=("model.v2.migration",)),
    _spec("model.v2.migration_outcomes", "model_field_v2", fixture="model-field-v2-v1", assertions=("committed", "not_committed", "timeout_unknown", "unknown_resolved"), test_ids=("model.v2.migration",)),
    _spec("model.v2.old_holder_fenced", "model_field_v2", fixture="model-field-v2-v1", assertions=("old_holder_claim", "old_holder_commit", "old_holder_fenced"), test_ids=("model.v2.fence_matrix",)),
    _spec("model.v2.crash_after_each_phase", "model_field_v2", fixture="model-field-v2-v1", assertions=("migration_phases", "crash_matrix_complete", "all_crashes_fail_closed"), test_ids=("model.v2.migration",)),
    _spec("model.v2.idempotent_retry", "model_field_v2", fixture="model-field-v2-v1", assertions=("retry_count", "same_receipt", "single_record_version_increment"), test_ids=("model.v2.migration",)),
    _spec("model.v2.dirty_model_matrix", "model_field_v2", fixture="model-field-v2-v1", assertions=("command_matrix", "dirty_models", "receipt_models", "order_models"), test_ids=("model.v2.dirty_matrix",)),
    _spec("model.v2.redis_operation_bytes", "model_field_v2", fixture="model-field-v2-v1", assertions=("v1_redis_operations", "v2_redis_operations", "v1_bytes", "v2_bytes", "exact_config_binding"), test_ids=("model.v2.redis_compare",)),
    _spec("worker.readiness.family_state_matrix", "worker_readiness", fixture="worker-readiness-v1", assertions=("family_states", "exactly_one_state_per_family"), test_ids=("worker.readiness",)),
    _spec("worker.readiness.aggregate", "worker_readiness", fixture="worker-readiness-v1", assertions=("aggregate_state", "critical_enabled_policy"), test_ids=("worker.readiness",)),
    _spec("worker.fault.repository", "worker_readiness", fixture="worker-readiness-v1", assertions=("repository_fault", "affected_family_not_ready", "unaffected_families_continue"), test_ids=("worker.faults",)),
    _spec("worker.fault.route", "worker_readiness", fixture="worker-readiness-v1", assertions=("route_fault", "affected_family_not_ready"), test_ids=("worker.faults",)),
    _spec("worker.fault.participant", "worker_readiness", fixture="worker-readiness-v1", assertions=("participant_fault", "retry_or_unknown_observed"), test_ids=("worker.faults",)),
    _spec("worker.fault.scan", "worker_readiness", fixture="worker-readiness-v1", assertions=("scan_fault", "scan_health_false"), test_ids=("worker.faults",)),
    _spec("worker.fault.scheduler", "worker_readiness", fixture="worker-readiness-v1", assertions=("scheduler_fault", "scheduler_health_false"), test_ids=("worker.faults",)),
    _spec("worker.disabled_families", "worker_readiness", fixture="worker-readiness-v1", assertions=("disabled_families", "disabled_not_counted_as_missing"), test_ids=("worker.readiness",)),
    _spec("worker.redis_delay_liveness", "worker_readiness", fixture="worker-readiness-v1", assertions=("redis_delay", "readiness_changes", "liveness_stable", "no_restart_storm"), test_ids=("worker.faults",)),
    _spec("worker.kill_due_recovery", "worker_readiness", fixture="worker-readiness-v1", assertions=("worker_kill", "due_index_recovery", "no_duplicate_side_effect", "due_residual_zero"), test_ids=("worker.kill_recovery",)),
    _spec("worker.invalid_order", "worker_readiness", fixture="worker-readiness-v1", assertions=("invalid_order_count", "invalid_order_zero"), test_ids=("worker.invalid_order",)),
    _spec("worker.due_residual", "worker_readiness", fixture="worker-readiness-v1", assertions=("due_residual", "due_residual_zero"), test_ids=("worker.kill_recovery",)),
    _spec("kubernetes.render_structural", "static_kubernetes", environment="rendered_manifest", fixture="static-kubernetes-v1", assertions=("rendered", "structural_validation", "required_objects"), test_ids=("kubernetes.render", "kubernetes.static_test")),
    _spec("kubernetes.cluster_deployment", "static_kubernetes", environment="kubernetes_staging", fixture="static-kubernetes-v1", assertions=("cluster_identity", "namespace", "deployment_applied", "image_digests"), test_ids=("kubernetes.deploy",)),
    _spec("kubernetes.pod_ready", "static_kubernetes", environment="kubernetes_staging", fixture="static-kubernetes-v1", assertions=("pod_ready", "product_readiness"), test_ids=("kubernetes.ready",)),
    _spec("kubernetes.gate_rolling_update", "static_kubernetes", environment="kubernetes_staging", fixture="static-kubernetes-v1", assertions=("gate_rolling_update", "old_gate_drained"), test_ids=("kubernetes.gate_rollout",)),
    _spec("kubernetes.worker_kill_recovery", "static_kubernetes", environment="kubernetes_staging", fixture="static-kubernetes-v1", assertions=("worker_kill", "worker_recovered_from_due_index"), test_ids=("kubernetes.worker_kill",)),
    _spec("kubernetes.holder_same_holder_restart", "static_kubernetes", environment="kubernetes_staging", fixture="static-kubernetes-v1", assertions=("holder_restart", "same_holder_identity", "static_route_preserved"), test_ids=("kubernetes.holder_restart",)),
    _spec("h5.official_url", "static_kubernetes", environment="official_url", fixture="static-kubernetes-v1", assertions=("official_url", "real_register_login", "no_mock", "dev_session_unavailable"), test_ids=("h5.official",)),
    _spec("backend.full", "product_regressions", fixture="product-regressions-v1", assertions=("backend_full", "exit_code_zero"), test_ids=("backend.full",)),
    _spec("backend.race", "product_regressions", fixture="product-regressions-v1", assertions=("backend_race", "exit_code_zero"), test_ids=("backend.race",)),
    _spec("backend.vet", "product_regressions", fixture="product-regressions-v1", assertions=("backend_vet", "exit_code_zero"), test_ids=("backend.vet",)),
    _spec("backend.build", "product_regressions", fixture="product-regressions-v1", assertions=("backend_build", "exit_code_zero"), test_ids=("backend.build",)),
    _spec("h5.test", "product_regressions", environment="official_url", fixture="product-regressions-v1", assertions=("h5_test", "exit_code_zero"), test_ids=("h5.test",)),
    _spec("h5.ts", "product_regressions", environment="official_url", fixture="product-regressions-v1", assertions=("h5_ts", "exit_code_zero"), test_ids=("h5.ts",)),
    _spec("h5.build", "product_regressions", environment="official_url", fixture="product-regressions-v1", assertions=("h5_build", "exit_code_zero"), test_ids=("h5.build",)),
    _spec("h5.audit", "product_regressions", environment="official_url", fixture="product-regressions-v1", assertions=("h5_audit", "exit_code_zero"), test_ids=("h5.audit",)),
    _spec("product.hero_repeat", "product_regressions", environment="official_url", fixture="product-regressions-v1", assertions=("hero_repetitions", "hero_success", "real_register_login"), test_ids=("hero.repeat",)),
    _spec("product.task_3_of_3", "product_regressions", environment="official_url", fixture="product-regressions-v1", assertions=("task_repetitions", "task_success"), test_ids=("task.3_of_3",)),
    _spec("product.f1", "product_regressions", environment="official_url", fixture="product-regressions-v1", assertions=("f1_repetitions", "f1_success"), test_ids=("product.f1",)),
    _spec("product.f2", "product_regressions", environment="official_url", fixture="product-regressions-v1", assertions=("f2_repetitions", "f2_success"), test_ids=("product.f2",)),
    _spec("product.f3", "product_regressions", environment="official_url", fixture="product-regressions-v1", assertions=("f3_repetitions", "f3_success"), test_ids=("product.f3",)),
    _spec("wss.200.baseline", "product_regressions", fixture="product-regressions-v1", assertions=("connections", "planned_connections", "baseline_success", "slow_reader_separate"), test_ids=("wss.200.baseline",)),
    _spec("wss.200.slow_reader", "product_regressions", fixture="product-regressions-v1", assertions=("connections", "planned_connections", "slow_reader_success", "baseline_separate"), test_ids=("wss.200.slow_reader",)),
    _spec("social.limits", "product_regressions", environment="official_url", fixture="product-regressions-v1", assertions=("friend_limit", "share_limit", "public_limits"), test_ids=("social.limits",)),
    _spec("social.public_scb", "product_regressions", environment="official_url", fixture="product-regressions-v1", assertions=("public_scb_hash", "append_only", "entry_points"), test_ids=("social.scb",)),
    _spec("r1.lease.formal_real_evidence", "product_regressions", fixture="product-regressions-v1", assertions=("formal_lease_evidence", "real_lease_evidence", "r1_evidence_retained"), test_ids=("r1.lease",)),
]

GATE_BY_ID = {spec["id"]: spec for spec in GATE_SPECS}


TEST_PLANS: dict[str, dict[str, Any]] = {
    "backend.full": {"command": ["go", "test", "./..."], "root": "backend", "environment": "local", "gate_ids": ["backend.full"]},
    "backend.race": {"command": ["go", "test", "-race", "./..."], "root": "backend", "environment": "local", "gate_ids": ["backend.race"]},
    "backend.vet": {"command": ["go", "vet", "./..."], "root": "backend", "environment": "local", "gate_ids": ["backend.vet"]},
    "backend.build": {"command": ["go", "build", "./..."], "root": "backend", "environment": "local", "gate_ids": ["backend.build"]},
    "h5.test": {"command": ["npm", "test", "--", "--runInBand"], "root": "h5", "environment": "official_url", "gate_ids": ["h5.test"]},
    "h5.ts": {"command": ["npx", "tsc", "--noEmit"], "root": "h5", "environment": "official_url", "gate_ids": ["h5.ts"]},
    "h5.build": {"command": ["npm", "run", "build:web"], "root": "h5", "environment": "official_url", "gate_ids": ["h5.build"]},
    "h5.audit": {"command": ["npm", "audit", "--omit=dev", "--audit-level=high"], "root": "h5", "environment": "official_url", "gate_ids": ["h5.audit"]},
    "hero.repeat": {"command": ["scripts/run-cp-vertical-b-smoke"], "root": "backend", "environment": "official_url", "gate_ids": ["product.hero_repeat"]},
    "task.3_of_3": {"command": None, "root": "h5", "environment": "official_url", "gate_ids": ["product.task_3_of_3"]},
    "product.f1": {"command": None, "root": "backend", "environment": "official_url", "gate_ids": ["product.f1"]},
    "product.f2": {"command": None, "root": "backend", "environment": "official_url", "gate_ids": ["product.f2"]},
    "product.f3": {"command": None, "root": "backend", "environment": "official_url", "gate_ids": ["product.f3"]},
    "wss.200.baseline": {"command": ["scripts/run-theft-200-wss-hot-farm"], "root": "backend", "environment": "local", "env": {"THEFT_HOT_CASES": "baseline"}, "gate_ids": ["wss.200.baseline"]},
    "wss.200.slow_reader": {"command": ["scripts/run-theft-200-wss-hot-farm"], "root": "backend", "environment": "local", "env": {"THEFT_HOT_CASES": "slow"}, "gate_ids": ["wss.200.slow_reader"]},
    "social.limits": {"command": None, "root": "h5", "environment": "official_url", "gate_ids": ["social.limits"]},
    "social.scb": {"command": None, "root": "h5", "environment": "official_url", "gate_ids": ["social.public_scb"]},
    "r1.lease": {"command": ["scripts/test-lease-renew-stress"], "root": "backend", "environment": "local", "env": {"LEASE_STRESS_VIEWERS": "199"}, "gate_ids": ["r1.lease.formal_real_evidence"]},
    "model.v2.goldens": {"command": None, "root": "backend", "environment": "local", "gate_ids": ["model.v2.field_equality"]},
    "model.v2.fence_matrix": {"command": None, "root": "backend", "environment": "local", "gate_ids": ["model.v2.one_writer_claim_record_version", "model.v2.old_holder_fenced"]},
    "model.v2.migration": {"command": None, "root": "backend", "environment": "local", "gate_ids": ["model.v2.no_dual_write", "model.v2.migration_outcomes", "model.v2.crash_after_each_phase", "model.v2.idempotent_retry"]},
    "model.v2.dirty_matrix": {"command": None, "root": "backend", "environment": "local", "gate_ids": ["model.v2.dirty_model_matrix"]},
    "model.v2.redis_compare": {"command": None, "root": "backend", "environment": "local", "gate_ids": ["model.v2.redis_operation_bytes"]},
    "worker.readiness": {"command": None, "root": "backend", "environment": "local", "gate_ids": ["worker.readiness.family_state_matrix", "worker.readiness.aggregate", "worker.disabled_families"]},
    "worker.faults": {"command": None, "root": "backend", "environment": "local", "gate_ids": ["worker.fault.repository", "worker.fault.route", "worker.fault.participant", "worker.fault.scan", "worker.fault.scheduler", "worker.redis_delay_liveness"]},
    "worker.kill_recovery": {"command": None, "root": "backend", "environment": "local", "gate_ids": ["worker.kill_due_recovery", "worker.due_residual"]},
    "worker.invalid_order": {"command": None, "root": "backend", "environment": "local", "gate_ids": ["worker.invalid_order"]},
    "kubernetes.render": {"command": ["scripts/validate-k8s-static-staging", "--profile", "candidate"], "root": "backend", "environment": "rendered_manifest", "gate_ids": ["kubernetes.render_structural"]},
    "kubernetes.static_test": {"command": ["scripts/test-k8s-static-staging"], "root": "backend", "environment": "rendered_manifest", "gate_ids": ["kubernetes.render_structural"]},
    "kubernetes.deploy": {"command": None, "root": "backend", "environment": "kubernetes_staging", "gate_ids": ["kubernetes.cluster_deployment"]},
    "kubernetes.ready": {"command": None, "root": "backend", "environment": "kubernetes_staging", "gate_ids": ["kubernetes.pod_ready"]},
    "kubernetes.gate_rollout": {"command": None, "root": "backend", "environment": "kubernetes_staging", "gate_ids": ["kubernetes.gate_rolling_update"]},
    "kubernetes.worker_kill": {"command": None, "root": "backend", "environment": "kubernetes_staging", "gate_ids": ["kubernetes.worker_kill_recovery"]},
    "kubernetes.holder_restart": {"command": None, "root": "backend", "environment": "kubernetes_staging", "gate_ids": ["kubernetes.holder_same_holder_restart"]},
    "h5.official": {"command": None, "root": "h5", "environment": "official_url", "gate_ids": ["h5.official_url"]},
}


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def json_dump(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str | None:
    if not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def redact_url(raw: str) -> str:
    if not raw:
        return ""
    try:
        parsed = urlsplit(raw)
        host = parsed.hostname or ""
        port = f":{parsed.port}" if parsed.port else ""
        return urlunsplit((parsed.scheme, f"{host}{port}", parsed.path, "", ""))
    except ValueError:
        return "<invalid-url>"


def is_local_url(raw: str) -> bool:
    try:
        host = (urlsplit(raw).hostname or "").lower().rstrip(".")
    except ValueError:
        return False
    return host in LOCAL_HOSTS or host.endswith(".localhost")


def classify_url(raw: str) -> dict[str, Any]:
    value = raw.strip()
    if not value:
        return {"configured": False, "class": "missing", "official": False, "formal_staging": False, "redacted": ""}
    try:
        parsed = urlsplit(value)
        valid = parsed.scheme in {"http", "https"} and bool(parsed.netloc)
    except ValueError:
        parsed = None
        valid = False
    if not valid or parsed is None:
        return {"configured": True, "class": "invalid", "official": False, "formal_staging": False, "redacted": redact_url(value)}
    local = is_local_url(value)
    return {
        "configured": True,
        "class": "localhost" if local else "non_local_candidate",
        "official": not local,
        "formal_staging": False,
        "redacted": redact_url(value),
        "scheme": parsed.scheme,
        "host": parsed.hostname,
    }


def redact_text(value: str) -> str:
    """Redact common credentials without trying to parse arbitrary log formats."""

    text = value
    text = re.sub(r"(?i)(redis(?:s)?://)([^\s/@:]+)(?::[^\s/@]*)?@", r"\1<redacted>@", text)
    text = re.sub(r"(?i)(bearer\s+)[A-Za-z0-9._~+/=-]+", r"\1<redacted>", text)
    text = re.sub(r"(?i)(authorization|cookie|password|passwd|secret|token|credential)(\s*[:=]\s*)[^\s,;]+", r"\1\2<redacted>", text)
    text = re.sub(r"(?i)(LOAD_TEST_TOKEN_SECRET|PERMIT_SECRET|THEFT_PRF_HMAC_SECRET)=([^\s]+)", r"\1=<redacted>", text)
    return text


def redact_argv(argv: list[str]) -> list[str]:
    redacted: list[str] = []
    for item in argv:
        if re.search(r"(?i)(password|passwd|secret|token|cookie|credential)=", item):
            key = item.split("=", 1)[0]
            redacted.append(f"{key}=<redacted>")
        elif "redis://" in item.lower() or "rediss://" in item.lower():
            redacted.append(redact_text(item))
        else:
            redacted.append(item)
    return redacted


def git_command(root: Path, *args: str) -> tuple[int, str, str]:
    try:
        completed = subprocess.run(
            ["git", "-C", str(root), *args],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
    except OSError as error:
        return 127, "", str(error)
    return completed.returncode, completed.stdout, completed.stderr


def parse_dirty_paths(output: str) -> list[str]:
    paths: list[str] = []
    for line in output.splitlines():
        if len(line) < 4:
            continue
        path = line[3:]
        if " -> " in path:
            path = path.rsplit(" -> ", 1)[-1]
        paths.append(path)
    return sorted(set(paths))


def candidate_snapshot(root: Path, role: str, expected_base: str) -> dict[str, Any]:
    result: dict[str, Any] = {
        "role": role,
        "root": str(root),
        "expected_base_commit": expected_base,
        "commit": None,
        "tree": None,
        "dirty": None,
        "dirty_files": [],
        "base_is_ancestor": False,
        "available": root.is_dir(),
        "captured_before_output_creation": True,
    }
    if not root.is_dir():
        result["error"] = "worktree does not exist"
        result["valid"] = False
        return result
    code, stdout, stderr = git_command(root, "rev-parse", "HEAD")
    if code != 0:
        result["error"] = redact_text(stderr.strip() or stdout.strip() or "git rev-parse failed")
        result["valid"] = False
        return result
    result["commit"] = stdout.strip()
    tree_code, tree_stdout, _ = git_command(root, "rev-parse", "HEAD^{tree}")
    if tree_code == 0:
        result["tree"] = tree_stdout.strip()
    ancestor_code, _, _ = git_command(root, "merge-base", "--is-ancestor", expected_base, result["commit"])
    result["base_is_ancestor"] = ancestor_code == 0
    status_code, status_stdout, status_stderr = git_command(root, "status", "--porcelain=v1", "--untracked-files=all")
    if status_code != 0:
        result["error"] = redact_text(status_stderr.strip() or status_stdout.strip() or "git status failed")
        result["valid"] = False
        return result
    result["dirty_files"] = parse_dirty_paths(status_stdout)
    result["dirty"] = bool(result["dirty_files"])
    diff_code, diff_stdout, diff_stderr = git_command(root, "diff", "--name-only", f"{expected_base}..{result['commit']}")
    if diff_code == 0:
        result["changed_files_from_base"] = sorted(path for path in diff_stdout.splitlines() if path)
    else:
        result["changed_files_from_base"] = []
        result["diff_error"] = redact_text(diff_stderr.strip() or "git diff failed")
    result["valid"] = bool(result["base_is_ancestor"] and not result["dirty"])
    return result


def bind_expected_commit(snapshot: dict[str, Any], expected: str, *, must_equal: str | None = None) -> dict[str, Any]:
    result = dict(snapshot)
    result["expected_commit"] = expected
    result["commit_matches_expected"] = bool(snapshot.get("commit") and snapshot.get("commit") == expected)
    if must_equal is not None:
        result["must_equal_commit"] = must_equal
        result["same_as_required_commit"] = snapshot.get("commit") == must_equal
    else:
        result["same_as_required_commit"] = True
    result["valid"] = bool(snapshot.get("valid") and result["commit_matches_expected"] and result["same_as_required_commit"])
    return result


def path_binding(root: Path, value: str | None) -> dict[str, Any]:
    if not value:
        return {"configured": False, "path": None, "exists": False, "sha256": None}
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = root / path
    try:
        resolved = path.resolve()
    except OSError:
        resolved = path
    try:
        display = str(resolved.relative_to(root.resolve()))
    except (ValueError, OSError):
        display = str(resolved)
    result: dict[str, Any] = {
        "configured": True,
        "path": display,
        "absolute_path": str(resolved),
        "exists": resolved.is_file(),
        "sha256": sha256_file(resolved),
        "bytes": resolved.stat().st_size if resolved.is_file() else None,
    }
    if resolved.is_file() and resolved.suffix.lower() == ".json":
        try:
            parsed = json.loads(resolved.read_text(encoding="utf-8"))
            if isinstance(parsed, dict):
                result["schema"] = parsed.get("schema")
                result["version"] = parsed.get("version", parsed.get("profile_version", parsed.get("layout_version")))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            result["parse_error"] = type(error).__name__
    return result


def tool_binding(root: Path, relative: str) -> dict[str, Any]:
    path = root / relative
    result: dict[str, Any] = {
        "path": relative,
        "exists": path.is_file() or path.is_dir(),
        "kind": "file" if path.is_file() else ("directory" if path.is_dir() else "missing"),
    }
    code, stdout, stderr = git_command(root, "log", "-1", "--format=%H", "--", relative)
    result["last_changed_commit"] = stdout.strip() if code == 0 and stdout.strip() else None
    if not result["exists"]:
        result["reason"] = "required tool path is absent"
    elif result["last_changed_commit"] is None:
        result["reason"] = "tool path is present but no git history was found"
    return result


def verification_tool_bindings(root: Path) -> dict[str, dict[str, Any]]:
    return {
        "r2_entry": tool_binding(root, "scripts/r2-q-verification"),
        "r2_python": tool_binding(root, "scripts/r2-q-verification.py"),
        "r1_entry": tool_binding(root, "scripts/r1-q-hardening-verification"),
        "hero": tool_binding(root, "scripts/run-cp-vertical-b-smoke"),
        "wss_200": tool_binding(root, "scripts/run-theft-200-wss-hot-farm"),
        "wss_driver": tool_binding(root, "cmd/ws-stress"),
        "actor_layout_inspector": tool_binding(root, "cmd/actor-layout-inspect"),
        "lease_200": tool_binding(root, "scripts/test-lease-renew-stress"),
        "worker": tool_binding(root, "cmd/farm-worker"),
        "p1_k8s_validator": tool_binding(root, "scripts/validate-k8s-static-staging"),
        "p1_k8s_test": tool_binding(root, "scripts/test-k8s-static-staging"),
    }


def read_json(path: Path) -> tuple[Any | None, str | None]:
    try:
        return json.loads(path.read_text(encoding="utf-8")), None
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        return None, f"{type(error).__name__}: {error}"


def load_fixture_contracts(root: Path) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    fixture_dir = root / "docs/verification/r2-q-fixtures"
    contracts: list[dict[str, Any]] = []
    by_id: dict[str, dict[str, Any]] = {}
    errors: list[str] = []
    if not fixture_dir.is_dir():
        errors.append("fixture directory is absent")
    else:
        for path in sorted(fixture_dir.glob("*.json")):
            if path.name.endswith(".schema.json"):
                continue
            raw, error = read_json(path)
            item: dict[str, Any] = {"path": str(path.relative_to(root)), "sha256": sha256_file(path), "valid": False}
            if error:
                item["error"] = error
                errors.append(f"{path.name}: {error}")
                contracts.append(item)
                continue
            if not isinstance(raw, dict):
                item["error"] = "fixture contract must be an object"
                errors.append(f"{path.name}: fixture contract must be an object")
                contracts.append(item)
                continue
            required = ("schema", "fixture_id", "contract_status", "deterministic", "production_semantics_changed", "cases")
            missing = [key for key in required if key not in raw]
            if missing:
                item["error"] = "missing required keys: " + ", ".join(missing)
                errors.append(f"{path.name}: {item['error']}")
                contracts.append(item)
                continue
            if not str(raw["schema"]).startswith("farm.r2-q.fixture/"):
                item["error"] = "unsupported fixture schema"
                errors.append(f"{path.name}: unsupported fixture schema")
                contracts.append(item)
                continue
            if raw["contract_status"] not in {SKIPPED, "COMPOSED", BLOCKED}:
                item["error"] = "invalid contract_status"
                errors.append(f"{path.name}: invalid contract_status")
                contracts.append(item)
                continue
            if raw["deterministic"] is not True or raw["production_semantics_changed"] is not False or not isinstance(raw["cases"], list) or not raw["cases"]:
                item["error"] = "deterministic=true and a non-empty cases array are required"
                errors.append(f"{path.name}: deterministic/cases contract invalid")
                contracts.append(item)
                continue
            fixture_id = raw["fixture_id"]
            if not isinstance(fixture_id, str) or not fixture_id:
                item["error"] = "fixture_id must be a non-empty string"
                errors.append(f"{path.name}: fixture_id invalid")
                contracts.append(item)
                continue
            if fixture_id in by_id:
                item["error"] = "duplicate fixture_id"
                errors.append(f"{path.name}: duplicate fixture_id {fixture_id}")
                contracts.append(item)
                continue
            item.update({"valid": True, "fixture_id": fixture_id, "schema": raw["schema"], "contract_status": raw["contract_status"], "case_count": len(raw["cases"])})
            contracts.append(item)
            by_id[fixture_id] = raw
    summary = {
        "schema": "farm.r2-q.fixture-summary/v1",
        "root": "docs/verification/r2-q-fixtures",
        "valid": not errors,
        "errors": errors,
        "contracts": contracts,
        "all_contracts_initially_composed": bool(contracts) and all(item.get("contract_status") == "COMPOSED" for item in contracts if item.get("valid")),
        "final_pass_claim_allowed": False,
    }
    return summary, by_id


def command_availability() -> dict[str, Any]:
    commands: dict[str, Any] = {}
    for name in ("docker", "kubectl", "go", "node", "npm", "npx", "curl", "Xvfb"):
        executable = shutil.which(name)
        commands[name] = {"available": executable is not None, "path": executable}
    return commands


def redact_env_name(name: str) -> bool:
    return bool(re.search(r"(?i)(password|passwd|secret|token|cookie|credential|authorization)", name))


def run_capture(
    argv: list[str],
    cwd: Path,
    log_path: Path,
    *,
    extra_env: dict[str, str],
    timeout: int,
    namespace: str,
) -> dict[str, Any]:
    started = utc_now()
    process_env = os.environ.copy()
    process_env.update(extra_env)
    process_env.update({
        "R2_Q_NAMESPACE": namespace,
        "R2_Q_DYNAMIC_PORTS_REQUIRED": "1",
        "R2_Q_FLUSHALL_ALLOWED": "0",
    })
    # Do not put the environment in the manifest; only a redacted key list is
    # retained so a future reviewer can see that secrets were not serialized.
    env_keys = sorted(key for key in process_env if key.startswith("R2_Q_") or key in extra_env)
    safe_env_keys = [key for key in env_keys if not redact_env_name(key)]
    try:
        completed = subprocess.run(
            argv,
            cwd=str(cwd),
            env=process_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=timeout,
            check=False,
        )
        output = completed.stdout or ""
        code = completed.returncode
        timed_out = False
    except subprocess.TimeoutExpired as error:
        output = error.stdout or ""
        if isinstance(output, bytes):
            output = output.decode("utf-8", errors="replace")
        output += "\nTIMEOUT\n"
        code = 124
        timed_out = True
    except OSError as error:
        output = f"{type(error).__name__}: {error}\n"
        code = 127
        timed_out = False
    log_path.write_text(redact_text(output), encoding="utf-8")
    return {
        "argv": redact_argv(argv),
        "cwd": str(cwd),
        "started_at": started,
        "finished_at": utc_now(),
        "exit_code": code,
        "timed_out": timed_out,
        "status": PASS if code == 0 else (INCONCLUSIVE if code == 2 else FAIL),
        "log": str(log_path.name),
        "environment_scope": "local",
        "environment_keys": safe_env_keys,
        "flushall_allowed": False,
        "namespace": namespace,
    }


def inspect_metrics(url: str) -> dict[str, Any]:
    endpoint = url.rstrip("/") + "/metrics" if not url.rstrip("/").endswith("/metrics") else url
    request = Request(endpoint, method="GET", headers={"accept": "text/plain", "user-agent": "r2-q-verification"})
    try:
        with urlopen(request, timeout=8) as response:  # noqa: S310 - explicit read-only metrics probe
            body = response.read(4 * 1024 * 1024)
            status = int(response.status)
        classification = classify_url(endpoint)
        return {
            "source": redact_url(endpoint),
            "http_status": status,
            "response_bytes": len(body),
            "response_sha256": sha256_bytes(body),
            "environment": classification["class"],
            "official": classification["official"],
            "status": PASS if 200 <= status < 300 else FAIL,
            "read_only": True,
        }
    except Exception as error:  # network failure is evidence, not a coordinator crash
        return {
            "source": redact_url(endpoint),
            "status": BLOCKED,
            "read_only": True,
            "error_class": type(error).__name__,
            "reason": "metrics endpoint unavailable",
        }


def make_metrics_summary(urls: list[str], run_id: str) -> dict[str, Any]:
    if not urls:
        return {
            "schema": "farm.r2-q.metrics-summary/v1",
            "run_id": run_id,
            "mode": "read_only",
            "status": NOT_RUN,
            "read_only": True,
            "sources": [],
            "reason": "No metrics URL supplied; no runtime metric conclusion is inferred.",
            "final_pass_claim_allowed": False,
        }
    sources = [inspect_metrics(url) for url in urls]
    if any(item.get("status") == FAIL for item in sources):
        status = FAIL
    elif any(item.get("status") == BLOCKED for item in sources):
        status = BLOCKED
    elif all(item.get("environment") == "localhost" for item in sources):
        status = OBSERVED_LOCAL
    else:
        status = INCONCLUSIVE
    return {
        "schema": "farm.r2-q.metrics-summary/v1",
        "run_id": run_id,
        "mode": "read_only",
        "status": status,
        "read_only": True,
        "sources": sources,
        "final_pass_claim_allowed": False,
    }


def rendered_manifest_info(root: Path, value: str | None) -> dict[str, Any]:
    if not value:
        return {"configured": False, "status": SKIPPED, "reason": "No rendered manifest was supplied."}
    binding = path_binding(root, value)
    if not binding["exists"]:
        return {"configured": True, "status": BLOCKED, "reason": "Rendered manifest path does not exist.", "binding": binding}
    if Path(binding["absolute_path"]).suffix.lower() == ".json":
        parsed, error = read_json(Path(binding["absolute_path"]))
        if error or not isinstance(parsed, (dict, list)):
            return {"configured": True, "status": FAIL, "reason": "Rendered JSON is invalid.", "error": error, "binding": binding}
    return {
        "configured": True,
        "status": SKIPPED,
        "reason": "Rendered manifest is hashed only; structural validation requires an explicit evidence record.",
        "binding": binding,
        "environment": "rendered_manifest",
        "formal_staging": False,
    }


def environment_snapshot(args: argparse.Namespace, root: Path, route: dict[str, Any], rendered: dict[str, Any]) -> dict[str, Any]:
    official = classify_url(args.official_url)
    namespace = args.namespace or f"r2q-{re.sub(r'[^a-z0-9-]', '-', args.run_id.lower())[:40].strip('-') or 'run'}"
    namespace = namespace[:63].rstrip("-") or "r2q-run"
    k8s_evidence = None
    if args.kubernetes_identity:
        k8s_evidence = {
            "configured": True,
            "cluster_identity_label": redact_text(args.kubernetes_identity),
            "cluster_identity_sha256": sha256_bytes(redact_text(args.kubernetes_identity).encode("utf-8")),
            "namespace": args.kubernetes_namespace or EXPECTED_NAMESPACE,
            "actual_cluster_required": True,
            "status": BLOCKED,
            "reason": "Identity is recorded only; actual Kubernetes evidence must be supplied per gate.",
        }
    else:
        k8s_evidence = {
            "configured": False,
            "cluster_identity_label": None,
            "cluster_identity_sha256": None,
            "namespace": args.kubernetes_namespace or EXPECTED_NAMESPACE,
            "actual_cluster_required": True,
            "status": BLOCKED,
            "reason": "No actual Kubernetes cluster identity supplied.",
        }
    return {
        "classification": "local",
        "local": {"available": True, "formal_staging": False, "localhost_is_not_staging": True},
        "rendered_manifest": rendered,
        "kubernetes_staging": k8s_evidence,
        "official_url": official,
        "redis": {
            "configured": bool(args.redis_endpoint),
            "endpoint": redact_url(args.redis_endpoint),
            "endpoint_sha256": sha256_bytes(redact_url(args.redis_endpoint).encode("utf-8")) if args.redis_endpoint else None,
            "credentials_recorded": False,
        },
        "namespace": namespace,
        "namespace_is_unique": True,
        "dynamic_ports_required": True,
        "dynamic_ports": [],
        "cluster_tools": command_availability(),
        "localhost_is_not_staging": True,
    }


def binding_for_run(
    candidates: dict[str, Any],
    expected_base: str,
    h5_expected_base: str,
    merge_candidate: str,
    profiles: dict[str, Any],
    route: dict[str, Any],
) -> dict[str, Any]:
    return {
        "r2_expected_base_commit": expected_base,
        "h5_expected_base_commit": h5_expected_base,
        "merge_candidate_commit": merge_candidate,
        "backend_commit": candidates["backend"].get("commit"),
        "h5_commit": candidates["h5"].get("commit"),
        "deploy_commit": candidates["deploy"].get("commit"),
        "load_tool_commit": candidates["load_tool"].get("commit"),
        "route_snapshot_sha256": route.get("sha256"),
        "profile_hashes": sorted(item.get("sha256") for item in profiles.get("profiles", []) if item.get("sha256")),
        "config_hashes": sorted(item.get("sha256") for item in profiles.get("configs", []) if item.get("sha256")),
        "image_manifest_sha256": profiles.get("image_manifest", {}).get("sha256"),
    }


def load_evidence(paths: list[str], root: Path, run_id: str, expected_binding: dict[str, Any]) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]], list[str]]:
    evidence_by_gate: dict[str, dict[str, Any]] = {}
    inputs: list[dict[str, Any]] = []
    errors: list[str] = []
    for raw_path in paths:
        path = Path(raw_path).expanduser()
        if not path.is_absolute():
            path = root / path
        entry: dict[str, Any] = {"path": str(path), "sha256": sha256_file(path), "valid": False}
        inputs.append(entry)
        raw, error = read_json(path)
        if error:
            entry["error"] = error
            errors.append(f"{path}: {error}")
            continue
        if not isinstance(raw, dict):
            entry["error"] = "evidence must be a JSON object"
            errors.append(f"{path}: evidence must be an object")
            continue
        gate_id = raw.get("gate_id")
        if raw.get("schema") != "farm.r2-q.gate-evidence/v1":
            entry["error"] = "unsupported evidence schema"
        elif not isinstance(gate_id, str) or gate_id not in GATE_BY_ID:
            entry["error"] = "unknown gate_id"
        elif raw.get("run_id") not in {run_id, "candidate-bound"}:
            entry["error"] = "evidence run_id does not match this run"
        elif raw.get("status") not in ALLOWED_EVIDENCE_STATUSES:
            entry["error"] = "unsupported evidence status"
        elif not isinstance(raw.get("binding"), dict):
            entry["error"] = "exact candidate binding is required"
        else:
            missing_or_mismatch = [
                key for key, value in expected_binding.items()
                if raw["binding"].get(key) != value
            ]
            if missing_or_mismatch:
                entry["error"] = "candidate/config binding mismatch: " + ", ".join(missing_or_mismatch)
            elif not isinstance(raw.get("assertions", {}), dict):
                entry["error"] = "assertions must be an object"
            elif not isinstance(raw.get("tests", []), list) or not raw.get("tests"):
                entry["error"] = "at least one planned/actual test reference is required"
            elif raw.get("flushall_used", False) is True:
                entry["error"] = "FLUSHALL evidence is forbidden"
            elif raw.get("credentials_recorded", False) is True:
                entry["error"] = "credential-bearing evidence is forbidden"
        if entry.get("error"):
            errors.append(f"{path}: {entry['error']}")
            continue
        assert isinstance(gate_id, str)
        if gate_id in evidence_by_gate:
            entry["error"] = "duplicate evidence for gate"
            errors.append(f"{path}: duplicate evidence for {gate_id}")
            continue
        entry.update({"valid": True, "gate_id": gate_id})
        raw["_source_path"] = str(path)
        raw["_source_sha256"] = entry["sha256"]
        evidence_by_gate[gate_id] = raw
    return evidence_by_gate, inputs, errors


def evidence_environment_allowed(spec: dict[str, Any], evidence: dict[str, Any], environment: dict[str, Any]) -> tuple[bool, str]:
    required = spec["required_environment"]
    observed = evidence.get("environment")
    if required == "local":
        return observed in {"local", "rendered_manifest", "kubernetes_staging", "official_url"}, "local or stronger environment required"
    if required == "rendered_manifest":
        if observed != "rendered_manifest":
            return False, "rendered structural evidence must declare environment=rendered_manifest"
        if not environment["rendered_manifest"].get("configured"):
            return False, "rendered manifest is not configured"
        return True, ""
    if required == "kubernetes_staging":
        if observed != "kubernetes_staging":
            return False, "localhost/rendered evidence cannot satisfy actual Kubernetes staging"
        details = evidence.get("environment_details")
        if not isinstance(details, dict):
            return False, "actual cluster identity and namespace are required"
        if not details.get("actual_cluster") or not details.get("cluster_identity"):
            return False, "actual_cluster=true and cluster_identity are required"
        if details.get("namespace") != EXPECTED_NAMESPACE:
            return False, f"staging namespace must be {EXPECTED_NAMESPACE}"
        return True, ""
    if required == "official_url":
        if observed != "official_url":
            return False, "official URL evidence must declare environment=official_url"
        details = evidence.get("environment_details")
        if not isinstance(details, dict) or not details.get("url"):
            return False, "non-empty official URL is required"
        url_info = classify_url(str(details["url"]))
        if not url_info.get("official"):
            return False, "localhost never satisfies official URL"
        if not environment["official_url"].get("official"):
            return False, "coordinator official URL is missing or local"
        if redact_url(str(details["url"])) != environment["official_url"].get("redacted"):
            return False, "evidence URL does not match the coordinator URL"
        return True, ""
    return True, ""


def gate_result(
    spec: dict[str, Any],
    evidence: dict[str, Any] | None,
    evidence_error: str | None,
    fixture: dict[str, Any] | None,
    environment: dict[str, Any],
) -> dict[str, Any]:
    if evidence_error:
        return {"id": spec["id"], "area": spec["area"], "status": INVALID, "reason": evidence_error, "final_pass_eligible": False, "required_environment": spec["required_environment"]}
    fixture_status = fixture.get("contract_status") if fixture else None
    if fixture is None and spec.get("fixture_id"):
        return {"id": spec["id"], "area": spec["area"], "status": FAIL, "reason": f"required fixture contract {spec['fixture_id']} is absent", "final_pass_eligible": False, "required_environment": spec["required_environment"]}
    if fixture_status == BLOCKED and evidence is None:
        return {"id": spec["id"], "area": spec["area"], "status": BLOCKED, "reason": f"fixture contract {spec['fixture_id']} is blocked by environment", "final_pass_eligible": False, "required_environment": spec["required_environment"]}
    if evidence is None:
        if spec["required_environment"] == "official_url":
            reason = "official URL evidence is required; no local or rendered result is promoted"
            if not environment["official_url"].get("official"):
                reason = "official URL is missing or local; localhost never satisfies formal staging"
            return {"id": spec["id"], "area": spec["area"], "status": BLOCKED, "reason": reason, "final_pass_eligible": False, "required_environment": spec["required_environment"]}
        if spec["required_environment"] == "kubernetes_staging":
            return {"id": spec["id"], "area": spec["area"], "status": BLOCKED, "reason": "actual Kubernetes staging evidence was not supplied; rendered/local evidence is insufficient", "final_pass_eligible": False, "required_environment": spec["required_environment"]}
        return {"id": spec["id"], "area": spec["area"], "status": SKIPPED, "reason": "gate contract exists, but no exact candidate-bound evidence is composed", "final_pass_eligible": False, "required_environment": spec["required_environment"]}
    status = evidence.get("status")
    reason = str(evidence.get("reason") or "evidence supplied")
    result: dict[str, Any] = {
        "id": spec["id"],
        "area": spec["area"],
        "status": status,
        "reason": reason,
        "final_pass_eligible": False,
        "required_environment": spec["required_environment"],
        "evidence": {"path": evidence.get("_source_path"), "sha256": evidence.get("_source_sha256")},
    }
    if status == PASS:
        allowed, environment_reason = evidence_environment_allowed(spec, evidence, environment)
        assertions = evidence.get("assertions", {})
        missing = [key for key in spec["required_assertions"] if assertions.get(key) is not True]
        if not allowed:
            result["status"] = BLOCKED if spec["required_environment"] in {"kubernetes_staging", "official_url"} else INVALID
            result["reason"] = environment_reason
        elif missing:
            result["status"] = INVALID
            result["reason"] = "required assertions are absent or false: " + ", ".join(missing)
        else:
            result["status"] = PASS
            result["final_pass_eligible"] = True
    elif status == OBSERVED_LOCAL and spec["required_environment"] in {"kubernetes_staging", "official_url"}:
        result["status"] = OBSERVED_LOCAL
        result["reason"] = "local observation is retained but cannot satisfy a formal staging/official gate"
    elif status not in ALLOWED_EVIDENCE_STATUSES:
        result["status"] = INVALID
        result["reason"] = "unsupported evidence status"
    return result


def run_selected_tests(
    selected: list[str],
    backend_root: Path,
    h5_root: Path,
    out: Path,
    namespace: str,
    candidates_valid: bool,
) -> tuple[list[dict[str, Any]], list[str]]:
    actual: list[dict[str, Any]] = []
    failures: list[str] = []
    command_dir = out / "commands"
    command_dir.mkdir(parents=True, exist_ok=True)
    for test_id in dict.fromkeys(selected):
        plan = TEST_PLANS.get(test_id)
        if plan is None:
            actual.append({"test_id": test_id, "status": INVALID, "reason": "unknown test id"})
            failures.append(f"unknown selected test: {test_id}")
            continue
        entry: dict[str, Any] = {"test_id": test_id, "planned": True, "environment": plan["environment"], "gate_ids": plan["gate_ids"], "command": redact_argv(plan["command"] or [])}
        if plan["command"] is None:
            entry.update({"status": SKIPPED, "reason": "no executable tool is composed for this fixture slot"})
            actual.append(entry)
            continue
        if not candidates_valid:
            entry.update({"status": BLOCKED, "reason": "invalid candidate assembly prevents test execution"})
            actual.append(entry)
            continue
        cwd = backend_root if plan["root"] == "backend" else h5_root
        argv = list(plan["command"])
        if argv and "/" not in argv[0] and (cwd / argv[0]).is_file():
            argv[0] = str(cwd / argv[0])
        log = command_dir / (re.sub(r"[^A-Za-z0-9_.-]", "-", test_id) + ".log")
        selected_env = {str(key): str(value) for key, value in plan.get("env", {}).items()}
        if test_id == "hero.repeat":
            selected_env["H5_ROOT"] = str(h5_root)
        command_result = run_capture(
            argv,
            cwd,
            log,
            extra_env=selected_env,
            timeout=3600,
            namespace=namespace,
        )
        entry.update({"status": command_result["status"], "command_result": command_result})
        actual.append(entry)
        if command_result["status"] == FAIL:
            failures.append(f"selected test failed: {test_id}")
    return actual, failures


def render_evidence(out: Path, manifest: dict[str, Any], verdict: dict[str, Any], fixture_summary: dict[str, Any], metrics: dict[str, Any]) -> None:
    lines = [
        "# CP MVP Hardening R2 — Q1 evidence",
        "",
        "> This is a fail-closed coordinator bundle. It is not a claim that uncomposed gates passed.",
        "",
        f"- run_id: `{manifest['run_id']}`",
        f"- expected R2 base: `{manifest['r2']['expected_base_commit']}`",
        f"- expected R2 H5 entry base: `{manifest['r2']['h5_entry_base_commit']}`",
        f"- merge candidate: `{manifest['r2']['merge_candidate_commit']}`",
        f"- verdict: `{verdict['verdict']}`",
        f"- handoff: `{verdict['handoff']}`",
        f"- final_pass_claim_allowed: `{str(verdict['final_pass_claim_allowed']).lower()}`",
        "",
        "## Environment boundary",
        "",
        "Localhost and rendered manifests are retained as observations only. Actual Kubernetes staging requires an explicit cluster-bound evidence record, and the official H5 URL is a separate gate.",
        "",
        f"- namespace: `{manifest['environment']['namespace']}` (unique=`{str(manifest['environment']['namespace_is_unique']).lower()}`)",
        f"- official URL: `{manifest['environment']['official_url'].get('redacted', '') or 'missing'}`",
        f"- flushall_allowed: `{str(manifest['security']['flushall_allowed']).lower()}`",
        "",
        "## Gate matrix",
        "",
        "| Gate | Status | Reason |",
        "|---|---|---|",
    ]
    for item in verdict["gates"]:
        reason = str(item["reason"]).replace("|", "\\|").replace("\n", " ")
        lines.append(f"| `{item['id']}` | `{item['status']}` | {reason} |")
    lines.extend([
        "",
        "## Fixture contracts",
        "",
        f"- valid: `{str(fixture_summary['valid']).lower()}`",
        f"- contracts: `{len(fixture_summary['contracts'])}`",
        "- Fixture declarations are deterministic contract inputs; their initial `SKIPPED_NOT_COMPOSED` status is not promoted to PASS by file presence.",
        "",
        "## Evidence discipline",
        "",
        "- Candidate/config binding is exact across Backend, H5, deploy, load tool, route snapshot, and profile hashes.",
        "- Credentials, cookies, tokens, and Redis URLs are redacted; no credential is copied into this bundle.",
        "- Selected tools use a unique namespace, dynamic ports, and `FLUSHALL` forbidden.",
        f"- Metrics status: `{metrics.get('status')}`; metrics are read-only and never a final PASS by themselves.",
        "",
        "## Outputs",
        "",
        "- `run-manifest.json`: exact candidate/config/environment/test binding.",
        "- `verdict.json`: gate statuses and fail-closed transition reasons.",
        "- `metrics-summary.json`: GET-only metric observations.",
        "- `fixture-summary.json`: hashes and validation of deterministic fixture contracts.",
        "- `evidence-index.json` and `checksums.sha256`: evidence inventory and integrity checks.",
    ])
    (out / "r2-q-evidence.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_integrity_files(out: Path, run_id: str, input_evidence: list[dict[str, Any]]) -> None:
    index_path = out / "evidence-index.json"
    checksums_path = out / "checksums.sha256"
    artifacts: list[dict[str, Any]] = []
    for path in sorted(item for item in out.rglob("*") if item.is_file() and item not in {index_path, checksums_path}):
        relative = path.relative_to(out).as_posix()
        artifacts.append({"path": relative, "sha256": sha256_file(path), "bytes": path.stat().st_size})
    index = {
        "schema": "farm.r2-q.evidence-index/v1",
        "run_id": run_id,
        "algorithm": "sha256",
        "artifacts": artifacts,
        "input_evidence": input_evidence,
        "checksums_file": "checksums.sha256",
        "self_hash_included": False,
        "credentials_recorded": False,
        "final_pass_claim_allowed": False,
    }
    json_dump(index_path, index)
    checksum_lines: list[str] = []
    for path in sorted(item for item in out.rglob("*") if item.is_file() and item != checksums_path):
        checksum_lines.append(f"{sha256_file(path)}  {path.relative_to(out).as_posix()}")
    checksums_path.write_text("\n".join(checksum_lines) + "\n", encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare/run the fail-closed R2-Q verification entry.")
    parser.add_argument("--out", help="empty output directory; default is a unique /tmp directory")
    parser.add_argument("--backend-root", default=None, help="Backend worktree (default: repository root)")
    parser.add_argument("--h5-root", default=None, help="H5 worktree (default: sibling ../classic-farm-h5)")
    parser.add_argument("--run-id", default=None, help="stable run identifier")
    parser.add_argument("--expected-r2-base", default=R2_BASE_COMMIT, help="expected R2 base; frozen default is the assigned base")
    parser.add_argument("--h5-base", default=R2_H5_BASE_COMMIT, help=f"expected H5 base; frozen R2 default is {R2_H5_BASE_COMMIT} and overrides must match")
    parser.add_argument("--merge-candidate-commit", default=None, help="exact Backend merge-candidate commit; default is observed HEAD")
    parser.add_argument("--h5-candidate-commit", default=None, help="exact H5 candidate commit; default is observed HEAD")
    parser.add_argument("--official-url", default="", help="official staging URL; loopback is never official")
    parser.add_argument("--route-snapshot", default="deploy/k8s/static-staging/base/route-snapshot.v1.json", help="merged R2 static route snapshot to hash; isolated tests may override explicitly")
    parser.add_argument("--profile", action="append", default=[], help="profile/config path to hash; repeatable")
    parser.add_argument("--config", action="append", default=[], help="additional config path to hash; repeatable")
    parser.add_argument("--image-manifest", default=None, help="image manifest/digest file to hash")
    parser.add_argument("--redis-endpoint", default=os.environ.get("R2_REDIS_ENDPOINT", ""), help="optional Redis endpoint; only redacted URL and hash are recorded")
    parser.add_argument("--rendered-manifest", default=None, help="rendered Kubernetes manifest to hash")
    parser.add_argument("--kubernetes-identity", default="", help="non-secret cluster identity label; does not prove cluster evidence")
    parser.add_argument("--kubernetes-namespace", default=EXPECTED_NAMESPACE, help="expected actual staging namespace")
    parser.add_argument("--namespace", default=None, help="unique local/test namespace label")
    parser.add_argument("--actor-layout-mode", action="append", default=[], choices=sorted(ACTOR_LAYOUT_MODES), help="Actor layout mode(s); repeatable")
    parser.add_argument("--friendship-service-mode", default="local_gate", choices=sorted(FRIENDSHIP_SERVICE_MODES))
    parser.add_argument("--friend-legacy-mode", default="shadow_read", choices=sorted(FRIEND_LEGACY_MODES))
    parser.add_argument("--metrics-url", action="append", default=[], help="GET-only metrics URL; repeatable")
    parser.add_argument("--evidence", action="append", default=[], help="exact candidate-bound gate evidence JSON; repeatable")
    parser.add_argument("--run", action="append", default=[], help="explicitly run a named planned test/tool")
    parser.add_argument("--fixture-root", default=None, help="fixture contract directory override for focused tests")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    script_path = Path(__file__).resolve()
    backend_root = Path(args.backend_root).expanduser().resolve() if args.backend_root else script_path.parents[1]
    h5_root = Path(args.h5_root).expanduser().resolve() if args.h5_root else (backend_root.parent / "classic-farm-h5").resolve()
    args.run_id = args.run_id or f"r2q-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}-{os.getpid()}"
    if not RUN_ID_RE.fullmatch(args.run_id):
        parser.error("--run-id must match [A-Za-z0-9][A-Za-z0-9_.:-]{0,127}")
    if not HEX_COMMIT_RE.fullmatch(args.expected_r2_base) or not HEX_COMMIT_RE.fullmatch(args.h5_base):
        parser.error("commit arguments must be hexadecimal commit ids")

    # Capture candidate state before creating the output directory.  If the
    # candidate is dirty, the invalid run is still preserved in the output.
    backend_probe = candidate_snapshot(backend_root, "backend", args.expected_r2_base)
    h5_probe = candidate_snapshot(h5_root, "h5", args.h5_base)
    merge_candidate = args.merge_candidate_commit or backend_probe.get("commit") or ""
    h5_candidate = args.h5_candidate_commit or h5_probe.get("commit") or ""
    if merge_candidate and not HEX_COMMIT_RE.fullmatch(merge_candidate):
        parser.error("--merge-candidate-commit must be a hexadecimal commit id")
    if h5_candidate and not HEX_COMMIT_RE.fullmatch(h5_candidate):
        parser.error("--h5-candidate-commit must be a hexadecimal commit id")
    backend = bind_expected_commit(backend_probe, merge_candidate)
    h5 = bind_expected_commit(h5_probe, h5_candidate)
    deploy_probe = candidate_snapshot(backend_root, "deploy", args.expected_r2_base)
    load_probe = candidate_snapshot(backend_root, "load_tool", args.expected_r2_base)
    deploy = bind_expected_commit(deploy_probe, backend.get("commit") or "", must_equal=backend.get("commit"))
    load_tool = bind_expected_commit(load_probe, backend.get("commit") or "", must_equal=backend.get("commit"))

    out = Path(args.out).expanduser().resolve() if args.out else Path("/tmp") / f"r2-q-verification-{args.run_id}"
    if out.exists() and any(out.iterdir()):
        parser.error(f"output directory must be empty: {out}")
    out.mkdir(parents=True, exist_ok=True)
    started_at = utc_now()

    if args.fixture_root:
        # The override is intended for schema tests and is still recorded;
        # normal runs always use the repository-local contracts.
        fixture_root = Path(args.fixture_root).expanduser().resolve()
        fixture_summary, fixture_contracts = load_fixture_contracts_from_root(fixture_root, backend_root)
    else:
        fixture_summary, fixture_contracts = load_fixture_contracts(backend_root)
    route = path_binding(backend_root, args.route_snapshot)
    profiles: list[str] = args.profile or [
        "deploy/profiles/redis-physical.w31a.v1.json",
        "deploy/load/q1-perf-baseline-v1.json",
    ]
    profile_items = [path_binding(backend_root, value) for value in profiles]
    config_paths: list[str] = args.config or ["deploy/docker-compose.staging.yml"]
    config_items = [path_binding(backend_root, value) for value in config_paths]
    image_manifest = path_binding(backend_root, args.image_manifest or "deploy/images/r1-1-image-manifest.json")
    rendered = rendered_manifest_info(backend_root, args.rendered_manifest)
    environment = environment_snapshot(args, backend_root, route, rendered)
    actor_layout_modes = args.actor_layout_mode or ["static_holder"]
    tools = verification_tool_bindings(backend_root)
    required_tool_names = list(REQUIRED_TOOL_NAMES)

    candidates = {"backend": backend, "h5": h5, "deploy": deploy, "load_tool": load_tool}
    profile_info = {"profiles": profile_items, "configs": config_items, "items": profile_items + config_items, "image_manifest": image_manifest}
    expected_binding = binding_for_run(candidates, args.expected_r2_base, R2_H5_BASE_COMMIT, merge_candidate, profile_info, route)
    evidence_by_gate, input_evidence, evidence_errors = load_evidence(args.evidence, backend_root, args.run_id, expected_binding)

    assembly_reasons: list[str] = []
    if args.expected_r2_base != R2_BASE_COMMIT:
        assembly_reasons.append(f"expected R2 base override does not equal assigned base {R2_BASE_COMMIT}")
    if args.h5_base != R2_H5_BASE_COMMIT:
        assembly_reasons.append(f"expected H5 base override does not equal frozen R2 H5 entry base {R2_H5_BASE_COMMIT}")
    if not backend.get("valid"):
        assembly_reasons.append("Backend candidate is not an exact clean descendant of the expected R2 base")
    if not h5.get("valid"):
        assembly_reasons.append("H5 candidate is not an exact clean descendant of the expected H5 base")
    if not deploy.get("valid") or not load_tool.get("valid"):
        assembly_reasons.append("deploy/load-tool commit is not exactly bound to Backend candidate")
    if not route.get("exists") or not route.get("sha256"):
        assembly_reasons.append("static route snapshot is missing or unhashed")
    if any(not item.get("exists") or not item.get("sha256") for item in profile_items):
        assembly_reasons.append("one or more required profile versions are missing or unhashed")
    if not fixture_summary.get("valid"):
        assembly_reasons.extend("fixture contract: " + error for error in fixture_summary.get("errors", []))
    missing_tools = [name for name in required_tool_names if not tools[name].get("exists")]
    if missing_tools:
        assembly_reasons.append("required verification tool missing: " + ", ".join(missing_tools))
    if evidence_errors:
        assembly_reasons.extend("invalid evidence: " + error for error in evidence_errors)

    actual_tests, command_failures = run_selected_tests(
        args.run,
        backend_root,
        h5_root,
        out,
        environment["namespace"],
        bool(backend.get("valid") and h5.get("valid") and deploy.get("valid") and load_tool.get("valid")),
    )
    metrics = make_metrics_summary(args.metrics_url, args.run_id)

    gates: list[dict[str, Any]] = []
    assembly_gate_data = [
        ("assembly.r2_base_and_merge_candidate", bool(args.expected_r2_base == R2_BASE_COMMIT and backend.get("valid") and backend.get("commit") == merge_candidate), "Exact assigned R2 base, clean Backend candidate, and merge-candidate commit are bound."),
        ("assembly.backend_h5_clean", bool(backend.get("valid") and h5.get("valid")), "Backend and H5 candidate worktrees are exact clean descendants of their expected bases."),
        ("assembly.deploy_load_tool_binding", bool(deploy.get("valid") and load_tool.get("valid")), "Deploy and load-tool commits exactly equal the Backend candidate commit."),
    ]
    for gate_id, is_pass, reason in assembly_gate_data:
        gates.append({"id": gate_id, "area": "assembly", "status": PASS if is_pass else FAIL, "reason": reason if is_pass else reason + " (see assembly reasons)", "final_pass_eligible": bool(is_pass), "required_environment": "local"})

    for spec in GATE_SPECS:
        fixture = fixture_contracts.get(spec["fixture_id"]) if spec.get("fixture_id") else None
        evidence = evidence_by_gate.get(spec["id"])
        error = None
        if evidence is None and any(item.get("gate_id") == spec["id"] and not item.get("valid") for item in input_evidence):
            error = next((str(item.get("error")) for item in input_evidence if item.get("gate_id") == spec["id"] and item.get("error")), "invalid gate evidence")
        gates.append(gate_result(spec, evidence, error, fixture, environment))

    execution_failures = [f"selected command failed: {test['test_id']}" for test in actual_tests if test.get("status") == FAIL]
    failing_gate_statuses = {FAIL, INVALID}
    failed_gates = [item["id"] for item in gates if item["status"] in failing_gate_statuses]
    blocked_gates = [item["id"] for item in gates if item["status"] == BLOCKED]
    skipped_gates = [item["id"] for item in gates if item["status"] == SKIPPED]
    observed_gates = [item["id"] for item in gates if item["status"] == OBSERVED_LOCAL]
    hard_failure = bool(assembly_reasons or failed_gates or command_failures or execution_failures)
    every_gate_passed = all(item["status"] == PASS for item in gates)
    if hard_failure:
        verdict_value = FAIL
        reason_codes = ["INVALID_ASSEMBLY" if assembly_reasons else "COMPOSED_GATE_FAILED"]
        if command_failures or execution_failures:
            reason_codes.append("SELECTED_TEST_FAILED")
    elif every_gate_passed:
        verdict_value = PASS
        reason_codes = ["ALL_GATES_PASS"]
    else:
        verdict_value = INCONCLUSIVE
        reason_codes = ["UNRESOLVED_GATES"]
    if blocked_gates:
        reason_codes.append("BLOCKED_ENVIRONMENT")
    if skipped_gates:
        reason_codes.append("SKIPPED_NOT_COMPOSED")
    if observed_gates:
        reason_codes.append("LOCAL_OBSERVATION_NOT_FORMAL_STAGING")
    if not environment["official_url"].get("official"):
        reason_codes.append("NO_OFFICIAL_NON_LOOPBACK_URL")
    handoff = "READY_FOR_OPERATOR_REVIEW" if verdict_value != FAIL and environment["official_url"].get("official") else "BLOCKED"
    counts: dict[str, int] = {}
    for item in gates:
        counts[item["status"]] = counts.get(item["status"], 0) + 1

    manifest: dict[str, Any] = {
        "schema": "farm.r2-q.run-manifest/v1",
        "tool": TOOL_VERSION,
        "run_id": args.run_id,
        "started_at": started_at,
        "finished_at": utc_now(),
        "r2": {
            "expected_base_commit": args.expected_r2_base,
            "assigned_base_commit": R2_BASE_COMMIT,
            "h5_entry_base_commit": R2_H5_BASE_COMMIT,
            "h5_base_requested": args.h5_base,
            "merge_candidate_commit": merge_candidate,
            "merge_candidate_explicit": args.merge_candidate_commit is not None,
            "no_reset": True,
            "no_rebase": True,
        },
        "candidates": candidates,
        "candidate_commits": {"backend": backend.get("commit"), "h5": h5.get("commit"), "deploy": deploy.get("commit"), "load_tool": load_tool.get("commit")},
        "backend": backend,
        "h5": h5,
        "deploy": deploy,
        "load_tool": load_tool,
        "clean_state": {"captured_before_output_creation": True, "backend_dirty": backend.get("dirty"), "h5_dirty": h5.get("dirty"), "dirty_paths_are_failure": True},
        "profiles": profile_info,
        "profile_versions": profile_items,
        "config_hashes": [item.get("sha256") for item in config_items if item.get("sha256")],
        "image_hashes": [image_manifest.get("sha256")] if image_manifest.get("sha256") else [],
        "assembly_checks": {
            "route_snapshot_hash_captured": bool(route.get("sha256")),
            "profile_hashes_captured": bool(profile_items) and all(item.get("sha256") for item in profile_items),
            "config_hashes_captured": bool(config_items) and all(item.get("sha256") for item in config_items),
            "image_manifest_hash_captured": bool(image_manifest.get("sha256")),
            "required_tool_paths_present": not missing_tools,
            "file_presence_is_not_product_pass": True,
        },
        "actor_layout": {"requested_modes": actor_layout_modes, "allowed_modes": sorted(ACTOR_LAYOUT_MODES), "v2_runtime_composed": False, "one_writer_claim": True, "one_record_version": True},
        "actor_layout_modes": actor_layout_modes,
        "friendship": {"service_mode": args.friendship_service_mode, "legacy_mode": args.friend_legacy_mode, "allowed_service_modes": sorted(FRIENDSHIP_SERVICE_MODES), "allowed_legacy_modes": sorted(FRIEND_LEGACY_MODES)},
        "friendship_service_mode": args.friendship_service_mode,
        "friendship_legacy_mode": args.friend_legacy_mode,
        "route_snapshot": route,
        "static_route_snapshot_sha256": route.get("sha256"),
        "environment": environment,
        "environment_hashes": {
            "cluster_identity_sha256": environment["kubernetes_staging"].get("cluster_identity_sha256"),
            "redis_endpoint_sha256": environment["redis"].get("endpoint_sha256"),
            "image_manifest_sha256": image_manifest.get("sha256"),
            "config_sha256": [item.get("sha256") for item in config_items if item.get("sha256")],
        },
        "security": {"credentials_recorded": False, "cookies_recorded": False, "tokens_recorded": False, "flushall_allowed": False, "unique_namespace_required": True, "dynamic_ports_required": True, "max_unresolved_operations": 1},
        "tools": tools,
        "planned_tests": [{"test_id": key, "command": redact_argv(value["command"] or []), "command_composed": value["command"] is not None, "environment": value["environment"], "gate_ids": value["gate_ids"]} for key, value in sorted(TEST_PLANS.items())],
        "actual_tests": actual_tests,
        "evidence_inputs": input_evidence,
        "fixture_contracts": fixture_summary,
        "commands": {"selected": [test["test_id"] for test in actual_tests], "failures": command_failures},
        "binding": expected_binding,
        "outputs": {"run_manifest": "run-manifest.json", "verdict": "verdict.json", "metrics_summary": "metrics-summary.json", "fixture_summary": "fixture-summary.json", "evidence_markdown": "r2-q-evidence.md", "evidence_index": "evidence-index.json", "checksums": "checksums.sha256"},
    }
    verdict = {
        "schema": "farm.r2-q.verdict/v1",
        "run_id": args.run_id,
        "verdict": verdict_value,
        "handoff": handoff,
        "invalid_assembly": bool(assembly_reasons),
        "final_pass_claim_allowed": bool(verdict_value == PASS and every_gate_passed and not assembly_reasons),
        "reason_codes": reason_codes,
        "assembly_reasons": assembly_reasons,
        "execution_failures": execution_failures,
        "gate_counts": counts,
        "failed_gates": failed_gates,
        "blocked_environment_gates": blocked_gates,
        "skipped_not_composed_gates": skipped_gates,
        "observed_local_only_gates": observed_gates,
        "gates": gates,
        "planned_test_count": len(TEST_PLANS),
        "actual_test_count": len(actual_tests),
        "command_failures": command_failures,
        "unresolved_operation_limit": 1,
    }

    json_dump(out / "metrics-summary.json", metrics)
    json_dump(out / "fixture-summary.json", fixture_summary)
    json_dump(out / "run-manifest.json", manifest)
    json_dump(out / "verdict.json", verdict)
    render_evidence(out, manifest, verdict, fixture_summary, metrics)
    write_integrity_files(out, args.run_id, input_evidence)
    print(json.dumps({"out": str(out), "verdict": verdict_value, "handoff": handoff}, ensure_ascii=False))
    return 1 if verdict_value == FAIL else (0 if verdict_value == PASS else 2)


def load_fixture_contracts_from_root(fixture_root: Path, repository_root: Path) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    """Same validation as the repository loader, with a test-friendly root."""

    try:
        relative = fixture_root.relative_to(repository_root)
    except ValueError:
        relative = fixture_root
    # Keep the loader implementation single-sourced by temporarily applying
    # the same directory shape through a small local implementation.
    contracts: list[dict[str, Any]] = []
    by_id: dict[str, dict[str, Any]] = {}
    errors: list[str] = []
    if not fixture_root.is_dir():
        errors.append("fixture directory is absent")
    else:
        for path in sorted(fixture_root.glob("*.json")):
            if path.name.endswith(".schema.json"):
                continue
            raw, error = read_json(path)
            item: dict[str, Any] = {"path": str(relative / path.name), "sha256": sha256_file(path), "valid": False}
            if error:
                item["error"] = error
                errors.append(f"{path.name}: {error}")
            elif not isinstance(raw, dict):
                item["error"] = "fixture contract must be an object"
                errors.append(f"{path.name}: fixture contract must be an object")
            else:
                missing = [key for key in ("schema", "fixture_id", "contract_status", "deterministic", "production_semantics_changed", "cases") if key not in raw]
                if missing:
                    item["error"] = "missing required keys: " + ", ".join(missing)
                    errors.append(f"{path.name}: {item['error']}")
                elif not str(raw["schema"]).startswith("farm.r2-q.fixture/") or raw["contract_status"] not in {SKIPPED, "COMPOSED", BLOCKED} or raw["deterministic"] is not True or raw["production_semantics_changed"] is not False or not isinstance(raw["cases"], list) or not raw["cases"]:
                    item["error"] = "fixture schema/status/determinism/cases invalid"
                    errors.append(f"{path.name}: {item['error']}")
                elif raw["fixture_id"] in by_id:
                    item["error"] = "duplicate fixture_id"
                    errors.append(f"{path.name}: duplicate fixture_id {raw['fixture_id']}")
                else:
                    item.update({"valid": True, "fixture_id": raw["fixture_id"], "schema": raw["schema"], "contract_status": raw["contract_status"], "case_count": len(raw["cases"])})
                    by_id[raw["fixture_id"]] = raw
            contracts.append(item)
    return {
        "schema": "farm.r2-q.fixture-summary/v1",
        "root": str(relative),
        "valid": not errors,
        "errors": errors,
        "contracts": contracts,
        "all_contracts_initially_composed": bool(contracts) and all(item.get("contract_status") == "COMPOSED" for item in contracts if item.get("valid")),
        "final_pass_claim_allowed": False,
    }, by_id


if __name__ == "__main__":
    raise SystemExit(main())
