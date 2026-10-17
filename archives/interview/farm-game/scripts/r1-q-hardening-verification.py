#!/usr/bin/env python3
"""Compose the R1-Q Wave1 verification entry without making a final claim.

This file is deliberately an evidence coordinator, not a second application
fixture. It records the frozen candidates, references the existing formal
tools, optionally runs only explicitly selected tools, and writes a fail-closed
evidence bundle. Local runs are always labelled local-only.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, urlopen


BACKEND_COMMIT = "ba6f3aee4f0fcd941d9fa37135af0ce09ffdfa5c"
H5_COMMIT = "b83cae607245391df6e2d605cffba85eb194cb09"
TOOL_VERSION = "r1-q-hardening-verification/1"
LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1", "0.0.0.0"}
RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def json_dump(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")


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
        netloc = f"{host}{port}"
        return urlunsplit((parsed.scheme, netloc, parsed.path, parsed.query, ""))
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
        return {"configured": False, "class": "missing", "official": False, "redacted": ""}
    try:
        parsed = urlsplit(value)
        valid = parsed.scheme in {"http", "https"} and bool(parsed.netloc)
    except ValueError:
        valid = False
        parsed = None
    if not valid:
        return {"configured": True, "class": "invalid", "official": False, "redacted": redact_url(value)}
    local = is_local_url(value)
    return {
        "configured": True,
        "class": "localhost" if local else "non_local_candidate",
        "official": not local,
        "redacted": redact_url(value),
        "scheme": parsed.scheme,
        "host": parsed.hostname,
    }


def run_capture(args: list[str], cwd: Path, log_path: Path, env: dict[str, str] | None = None,
                timeout: int = 3600) -> dict[str, Any]:
    started = utc_now()
    process_env = os.environ.copy()
    if env:
        process_env.update(env)
    try:
        completed = subprocess.run(
            args,
            cwd=str(cwd),
            env=process_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=timeout,
            check=False,
        )
        output = completed.stdout
        code = completed.returncode
        timed_out = False
    except subprocess.TimeoutExpired as error:
        output = error.stdout or ""
        if isinstance(output, bytes):
            output = output.decode("utf-8", errors="replace")
        output += "\nTIMEOUT\n"
        code = 124
        timed_out = True
    log_path.write_text(output, encoding="utf-8")
    return {
        "argv": args,
        "started_at": started,
        "finished_at": utc_now(),
        "exit_code": code,
        "timed_out": timed_out,
        "log": log_path.name,
    }


def git_command(root: Path, *args: str) -> tuple[int, str, str]:
    completed = subprocess.run(
        ["git", "-C", str(root), *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    return completed.returncode, completed.stdout, completed.stderr


def candidate_info(root: Path, expected: str, ignored_prefixes: tuple[str, ...], allow_descendant: bool = False) -> dict[str, Any]:
    result: dict[str, Any] = {
        "root": str(root),
        "expected_commit": expected,
        "commit": None,
        "dirty": None,
        "dirty_files": [],
        "commit_matches": False,
        "base_is_ancestor": False,
        "changed_files": [],
        "out_of_scope_files": [],
        "available": root.is_dir(),
    }
    if not root.is_dir():
        result["error"] = "worktree does not exist"
        return result
    code, stdout, stderr = git_command(root, "rev-parse", "HEAD")
    if code != 0:
        result["error"] = (stderr.strip() or stdout.strip() or "git rev-parse failed")
        return result
    result["commit"] = stdout.strip()
    result["commit_matches"] = result["commit"] == expected
    if allow_descendant:
        ancestor_code, _, _ = git_command(root, "merge-base", "--is-ancestor", expected, result["commit"])
        result["base_is_ancestor"] = ancestor_code == 0
        diff_code, diff_stdout, diff_stderr = git_command(root, "diff", "--name-only", f"{expected}..{result['commit']}")
        if diff_code == 0:
            result["changed_files"] = sorted(path for path in diff_stdout.splitlines() if path)
        else:
            result["error"] = diff_stderr.strip() or "git diff failed"
    code, stdout, stderr = git_command(root, "status", "--porcelain=v1", "--untracked-files=all")
    if code != 0:
        result["error"] = (stderr.strip() or stdout.strip() or "git status failed")
        return result
    dirty_files: list[str] = []
    for line in stdout.splitlines():
        if len(line) < 4:
            continue
        path = line[3:]
        if " -> " in path:
            path = path.rsplit(" -> ", 1)[-1]
        if not any(path == prefix.rstrip("/") or path.startswith(prefix) for prefix in ignored_prefixes):
            dirty_files.append(path)
    result["dirty_files"] = sorted(set(dirty_files))
    result["dirty"] = bool(dirty_files)
    if allow_descendant:
        out_of_scope = []
        for path in result["changed_files"]:
            if path.startswith("scripts/r1-") or path.startswith("docs/verification/r1-q-"):
                continue
            parts = path.split("/", 2)
            if len(parts) >= 2 and parts[0] == "cmd" and ("probe" in parts[1] or "load" in parts[1]):
                continue
            out_of_scope.append(path)
        result["out_of_scope_files"] = out_of_scope
        result["valid"] = bool(result["base_is_ancestor"] and not result["dirty"] and not out_of_scope)
    else:
        result["valid"] = bool(result["commit_matches"] and not result["dirty"])
    return result


def check_tools(root: Path) -> dict[str, Any]:
    paths = {
        "scenario_entry": "scripts/r1-q-hardening-verification",
        "existing_scenario_coordinator": "cmd/farm-scenario",
        "cp_vertical_b": "scripts/run-cp-vertical-b-smoke",
        "wss_200": "scripts/run-theft-200-wss-hot-farm",
        "lease_200": "scripts/test-lease-renew-stress",
        "authority_fault": "scripts/run-authority-fault-smoke",
        "authority_audit": "cmd/theft-hot-audit",
        "lease_driver": "cmd/lease-stress",
        "wss_driver": "cmd/ws-stress",
        "fault_proxy": "cmd/gate-vertical-e2e",
        "actor_layout_reference": "deploy/actor-route.static.example.json",
        "redis_profile": "deploy/profiles/redis-physical.w31a.v1.json",
    }
    found: dict[str, Any] = {}
    for name, relative in paths.items():
        path = root / relative
        found[name] = {"path": relative, "exists": path.exists(), "kind": "file" if path.is_file() else ("directory" if path.is_dir() else "missing")}
    return found


def load_social_manifest(path_value: str | None) -> dict[str, Any]:
    if not path_value:
        return {
            "status": "BLOCKED_SCB_FINAL_ENTRY_UNSET",
            "configured": False,
            "final_entry": None,
            "reason": "No final SCB entry manifest was supplied; no Social conclusion is inferred.",
        }
    path = Path(path_value).expanduser()
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        return {"status": "FAIL", "configured": True, "path": str(path), "error": f"invalid Social entry manifest: {error}"}
    if not isinstance(raw, dict) or not isinstance(raw.get("final_entry"), str) or not raw["final_entry"].strip():
        return {"status": "FAIL", "configured": True, "path": str(path), "error": "Social entry manifest requires a non-empty string final_entry"}
    return {
        "status": "MANIFEST_CONFIGURED_NOT_CONCLUDED",
        "configured": True,
        "path": str(path),
        "sha256": sha256_file(path),
        "final_entry": raw["final_entry"].strip(),
        "merge_state": raw.get("merge_state", "UNSPECIFIED"),
        "calls": raw.get("calls", []),
        "reason": "Manifest configuration is recorded only; the coordinator does not guess SCB merge or outcome.",
    }


def gate(gate_id: str, status: str, reason: str, evidence: str | None = None) -> dict[str, Any]:
    item: dict[str, Any] = {"id": gate_id, "status": status, "reason": reason, "final_pass_eligible": False}
    if evidence:
        item["evidence"] = evidence
    return item


def inspect_dev_session(url: str) -> dict[str, Any]:
    endpoint = url.rstrip("/") + "/dev/session"
    request = Request(endpoint, method="GET", headers={"accept": "application/json"})
    try:
        with urlopen(request, timeout=8) as response:  # noqa: S310 - explicit read-only verification URL
            body = response.read(1024)
            status = int(response.status)
        available = 200 <= status < 300
        return {
            "endpoint": redact_url(endpoint),
            "http_status": status,
            "bytes_sampled": len(body),
            "available": available,
            "status": "FAIL" if available else "OBSERVED_UNAVAILABLE",
        }
    except Exception as error:  # urllib errors are evidence, not a coordinator crash
        return {"endpoint": redact_url(endpoint), "status": "OBSERVED_UNAVAILABLE", "available": False, "error_class": type(error).__name__}


def write_metrics_not_run(path: Path, reason: str) -> dict[str, Any]:
    result = {
        "schema": "farm.r1-q.metrics-summary/v1",
        "mode": "read_only",
        "status": "NOT_RUN",
        "read_only": True,
        "sources": [],
        "reason": reason,
        "final_pass_claim_allowed": False,
    }
    json_dump(path, result)
    return result


def run_m0_probe(root: Path, out: Path, urls: list[str], run_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
    if not urls:
        return write_metrics_not_run(out / "metrics-summary.json", "No metrics URL supplied."), {}
    sources: list[dict[str, Any]] = []
    command_results: dict[str, Any] = {}
    for index, url in enumerate(urls, start=1):
        probe_path = out / f"metrics-{index}.json"
        command = ["go", "run", "./cmd/r1-m0-probe", "-url", url, "-json-out", str(probe_path), "-run-id", run_id]
        result = run_capture(command, root, out / f"metrics-{index}.log", {"GOCACHE": os.environ.get("FARM_GO_BUILD_CACHE", "/tmp/farmgame-go-cache")}, 180)
        command_results[f"m0_{index}"] = result
        if probe_path.is_file():
            try:
                sources.append(json.loads(probe_path.read_text(encoding="utf-8")))
            except json.JSONDecodeError:
                sources.append({"status": "FAIL", "source": redact_url(url), "error": "probe output was not JSON"})
        else:
            sources.append({"status": "FAIL", "source": redact_url(url), "error": "probe did not create output"})
    if any(item.get("status") == "FAIL" for item in sources):
        status = "FAIL"
    elif any(item.get("status") == "OBSERVED_UNAVAILABLE" for item in sources):
        status = "INCONCLUSIVE"
    elif all(not item.get("official", False) for item in sources):
        status = "OBSERVED_LOCAL_ONLY"
    else:
        status = "OBSERVED_NOT_FINAL"
    summary = {
        "schema": "farm.r1-q.metrics-summary/v1",
        "mode": "read_only",
        "status": status,
        "read_only": True,
        "run_id": run_id,
        "sources": sources,
        "command_results": command_results,
        "final_pass_claim_allowed": False,
    }
    json_dump(out / "metrics-summary.json", summary)
    return summary, command_results


def make_actor_layout_reference(root: Path, out: Path, backend: dict[str, Any]) -> dict[str, Any]:
    relative = "deploy/actor-route.static.example.json"
    source = root / relative
    reference = {
        "schema": "farm.r1-q.actor-layout-reference/v1",
        "status": "REFERENCE_ONLY" if source.is_file() else "MISSING_REFERENCE",
        "layout_version": "v1",
        "source_path": relative,
        "source_commit": backend.get("commit"),
        "source_sha256": sha256_file(source),
        "runtime_evidence": False,
        "copied_fixture": False,
        "reason": "Static route example is referenced, not copied or treated as a runtime topology witness.",
        "final_pass_claim_allowed": False,
    }
    json_dump(out / "actor-layout-v1-baseline.json", reference)
    return reference


def make_lease_evidence(out: Path, run_id: str, lease_command: dict[str, Any] | None,
                        observation_path: str | None) -> dict[str, Any]:
    if not lease_command or "exit_code" not in lease_command:
        real_status = "NOT_RUN"
    elif lease_command["exit_code"] == 0:
        real_status = "OBSERVED_LOCAL_ONLY"
    elif lease_command["exit_code"] == 2:
        real_status = "INCONCLUSIVE"
    else:
        real_status = "FAIL"
    evidence: dict[str, Any] = {
        "schema": "farm.r1-q.lease-batching-evidence/v1",
        "status": "PREPARED",
        "run_id": run_id,
        "simulation": {
            "sessions": 100000,
            "subscriptions": 100000,
            "status": "SKIPPED_NOT_COMPOSED",
            "reason": "No formal 100k Session/Subscription simulator is present in the frozen baseline.",
        },
        "real_subscription": {
            "target": 200,
            "owner_plus_viewers": "1+199",
            "status": real_status,
            "tool": "scripts/test-lease-renew-stress + cmd/lease-stress",
            "command_result": lease_command,
        },
        "batch_metrics": {
            "item_count": {"status": "NOT_INSTRUMENTED", "metric": "farm_gate_subscription_lease_renew_entries_total"},
            "bytes": {"status": "NOT_INSTRUMENTED", "reason": "Existing formal lease tool has no batch-byte witness."},
            "duration_ms": {"status": "NOT_INSTRUMENTED", "metric": "farm_gate_subscription_lease_renew_duration_seconds"},
        },
        "invariants": {
            "holder_isolation": "NOT_COMPOSED",
            "stale_does_not_resurrect": "NOT_COMPOSED",
            "drain": "NOT_COMPOSED",
            "real_200_subscription": real_status,
        },
        "source_observation": {
            "path": observation_path,
            "sha256": sha256_file(Path(observation_path)) if observation_path else None,
        },
        "final_pass_claim_allowed": False,
    }
    json_dump(out / "lease-batching-evidence.json", evidence)
    return evidence


def command_gate_status(result: dict[str, Any] | None, reason: str) -> tuple[str, str]:
    if not result:
        return "NOT_RUN", reason
    if "exit_code" not in result:
        return str(result.get("status", "NOT_RUN")), str(result.get("reason", reason))
    if result.get("exit_code") == 0:
        return "OBSERVED_LOCAL_ONLY", "Command completed on loopback/test profile; not staging evidence."
    if result.get("exit_code") == 2:
        return "INCONCLUSIVE", "Existing command returned its inconclusive exit code."
    return "FAIL", f"Existing command failed with exit {result.get('exit_code')}."


def render_evidence(out: Path, manifest: dict[str, Any], verdict: dict[str, Any], lease: dict[str, Any],
                    metrics: dict[str, Any]) -> None:
    lines = [
        "# CP MVP Hardening R1 — Wave1 evidence",
        "",
        "> This bundle prepares composition only. It is not a final PASS report.",
        "",
        f"- run_id: `{manifest['run_id']}`",
        f"- backend: `{manifest['backend'].get('commit')}` (expected `{BACKEND_COMMIT}`)",
        f"- H5 add-dir: `{manifest['h5'].get('commit')}` (expected `{H5_COMMIT}`)",
        f"- verdict: `{verdict['verdict']}`",
        f"- handoff: `{verdict['handoff']}`",
        f"- final_pass_claim_allowed: `{str(verdict['final_pass_claim_allowed']).lower()}`",
        "",
        "## Assembly policy",
        "",
        "Invalid commit or dirty candidate assembly remains `FAIL`. Loopback execution is labelled "
        "`OBSERVED_LOCAL_ONLY`; it is never upgraded to staging. Without an official non-loopback URL, "
        "handoff remains `BLOCKED`.",
        "",
        "## Gate matrix",
        "",
        "| Gate | Status | Reason |",
        "|---|---|---|",
    ]
    for item in verdict["gates"]:
        reason = item["reason"].replace("|", "\\|").replace("\n", " ")
        lines.append(f"| `{item['id']}` | `{item['status']}` | {reason} |")
    lines.extend([
        "",
        "## Structured artifacts",
        "",
        "- `run-manifest.json`: frozen candidates, tool composition and environment classification.",
        "- `verdict.json`: fail-closed gate statuses; no final PASS claim is possible in Wave1.",
        "- `metrics-summary.json`: M0 GET-only metric/inspect and byte observations, when requested.",
        "- `actor-layout-v1-baseline.json`: reference-only pointer to the static route example; not runtime evidence.",
        "- `lease-batching-evidence.json`: 100k simulation and batch-byte gaps remain explicit.",
        "",
        "## Lease boundary",
        "",
        f"- 100k simulated Session/Subscription: `{lease['simulation']['status']}`.",
        f"- 200 real subscription composition: `{lease['real_subscription']['status']}`.",
        "- Batch item count, bytes and duration are schema slots; missing instrumentation is not PASS.",
        "",
        "## M0 boundary",
        "",
        f"- metrics status: `{metrics.get('status')}`; read_only=`{str(metrics.get('read_only', True)).lower()}`.",
        "- No write, fixture mutation, `/dev/session` backdoor or Mock result is used by the coordinator.",
        "",
        "## Explicitly unassembled gates",
        "",
        "A1, A3 and A4 are emitted as `SKIPPED_NOT_COMPOSED` by policy. The historical Scenario "
        "Coordinator is also reference-only when absent from frozen Backend. Social remains dependent "
        "on the final SCB entry manifest and is not inferred from an older route or UI path.",
        "",
    ])
    (out / "cp-mvp-hardening-r1-evidence.md").write_text("\n".join(lines), encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare/run the R1-Q Wave1 composite verification entry.")
    parser.add_argument("--out", help="empty output directory; default is a unique /tmp directory")
    parser.add_argument("--backend-root", default=None, help="backend worktree (default: repository root)")
    parser.add_argument("--h5-root", default=None, help="read-only H5 add-dir (default: ../classic-farm-h5)")
    parser.add_argument("--run-id", default=None, help="stable run identifier")
    parser.add_argument("--official-url", default="", help="official staging URL; loopback is never staging")
    parser.add_argument("--social-entry-manifest", default=None, help="manifest naming the final SCB Social entry")
    parser.add_argument("--metrics-url", action="append", default=[], help="GET-only metrics URL; repeatable")
    parser.add_argument("--lease-observation", default=None, help="optional read-only lease evidence reference")
    parser.add_argument("--run", action="append", choices=["cp-b", "wss-200", "lease-200"], default=[], help="explicitly run an existing local/test-profile tool")
    parser.add_argument("--execute-dev-session-check", action="store_true", help="GET /dev/session to prove it is unavailable; never sends credentials")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    script_path = Path(__file__).resolve()
    backend_root = Path(args.backend_root).expanduser().resolve() if args.backend_root else script_path.parents[1]
    h5_root = Path(args.h5_root).expanduser().resolve() if args.h5_root else (backend_root.parent / "classic-farm-h5").resolve()
    run_id = args.run_id or f"r1q-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}-{os.getpid()}"
    if not RUN_ID_RE.fullmatch(run_id):
        parser.error("--run-id must match [A-Za-z0-9][A-Za-z0-9_.:-]{0,127}")
    if args.out:
        out = Path(args.out).expanduser().resolve()
        out.mkdir(parents=True, exist_ok=True)
        if any(out.iterdir()):
            parser.error(f"output directory must be empty: {out}")
    else:
        out = Path("/tmp") / f"r1-q-hardening-{run_id}"
        out.mkdir(parents=True, exist_ok=False)
    run_started_at = utc_now()
    command_dir = out / "commands"
    command_dir.mkdir()

    backend = candidate_info(backend_root, BACKEND_COMMIT, ("docs/verification/results/",), allow_descendant=True)
    h5 = candidate_info(h5_root, H5_COMMIT, ("test-results/", "dist/"))
    tools = check_tools(backend_root)
    # The historical Scenario Coordinator is deliberately only observed here:
    # cmd/farm-scenario is not part of frozen Backend ba6 and is outside this
    # Wave1 writer's owned cmd/*probe* / cmd/*load* seam.
    tool_failures = [
        name for name, item in tools.items()
        if not item["exists"] and name != "existing_scenario_coordinator"
    ]
    scenario_coordinator = tools["existing_scenario_coordinator"]
    assembly_reasons: list[str] = []
    if not backend.get("valid", False):
        assembly_reasons.append("backend candidate commit/dirty check failed")
    if not h5.get("valid", False):
        assembly_reasons.append("H5 add-dir candidate commit/dirty check failed")
    if tool_failures:
        assembly_reasons.append("required formal tool missing: " + ", ".join(sorted(tool_failures)))

    official = classify_url(args.official_url)
    social = load_social_manifest(args.social_entry_manifest)
    actor_layout = make_actor_layout_reference(backend_root, out, backend)
    if actor_layout["status"] == "MISSING_REFERENCE":
        assembly_reasons.append("actor layout v1 reference missing")

    metrics, metric_commands = run_m0_probe(backend_root, out, args.metrics_url, run_id)

    command_results: dict[str, dict[str, Any]] = {}
    for selected in dict.fromkeys(args.run):
        if not backend.get("valid", False) or not h5.get("valid", False):
            command_results[selected] = {"status": "NOT_RUN_INVALID_ASSEMBLY", "reason": "Invalid candidate assembly prevents execution."}
            continue
        if selected == "cp-b":
            if not social.get("configured", False):
                command_results[selected] = {"status": "BLOCKED_SCB_FINAL_ENTRY_UNSET"}
                continue
            command_results[selected] = {
                "status": "NOT_RUN_SCB_ENTRY_NOT_PROVEN",
                "reason": "Existing CP-B browser flow cannot prove final SCB entry from this manifest alone.",
            }
            continue
        if selected == "wss-200":
            command = [str(backend_root / "scripts/run-theft-200-wss-hot-farm")]
            env = {"THEFT_HOT_RESULT_DIR": str(out / "theft-200-wss"), "THEFT_HOT_RUN_STAMP": run_id, "THEFT_HOT_CASES": "both"}
        else:
            command = [str(backend_root / "scripts/test-lease-renew-stress")]
            env = {"LEASE_STRESS_VIEWERS": "199", "LEASE_STRESS_DURATION": os.environ.get("R1_Q_LEASE_DURATION", "60s")}
        command_results[selected] = run_capture(command, backend_root, command_dir / f"{selected}.log", env)

    dev_session = None
    if args.execute_dev_session_check and args.official_url:
        dev_session = inspect_dev_session(args.official_url)
    elif args.execute_dev_session_check:
        dev_session = {"status": "BLOCKED_NO_URL", "available": None}

    lease_command = command_results.get("lease-200")
    lease = make_lease_evidence(out, run_id, lease_command if lease_command and "exit_code" in lease_command else None, args.lease_observation)

    gates: list[dict[str, Any]] = []
    gates.extend([
        gate("assembly.backend_candidate", "PASS" if backend.get("valid") else "FAIL", "Frozen backend commit and clean candidate." if backend.get("valid") else "Frozen backend commit or dirty check failed."),
        gate("assembly.h5_add_dir_candidate", "PASS" if h5.get("valid") else "FAIL", "Frozen H5 add-dir commit and clean candidate." if h5.get("valid") else "Frozen H5 commit or dirty check failed."),
        gate("showcase.real_register_login", "BLOCKED_NO_OFFICIAL_URL" if not official.get("official") else "NOT_RUN", "Official URL is required; no local run is promoted to staging." if not official.get("official") else "Official URL configured; real Playwright register/login still needs execution.", command_results.get("cp-b", {}).get("log")),
        gate("showcase.dev_session_unavailable", "FAIL" if dev_session and dev_session.get("status") == "FAIL" else (dev_session.get("status") if dev_session else "NOT_RUN"), "`/dev/session` must be unavailable; no Mock session path is accepted."),
        gate("showcase.no_mock", "NOT_RUN", "No Mock result is accepted; use real register/login in the formal H5 flow."),
        gate("showcase.test_profile_fixture_fault_tools", "READY_COMPOSED", "Existing fixture/fault tools remain available for local/test profile."),
        gate(
            "composition.scenario_coordinator",
            "READY_COMPOSED" if scenario_coordinator["exists"] else "SKIPPED_NOT_COMPOSED",
            "Existing Scenario Coordinator is available for test-profile composition."
            if scenario_coordinator["exists"] else
            "cmd/farm-scenario is absent from frozen Backend ba6; it is referenced but not copied or imported.",
        ),
        gate("social.final_scb_entry", social["status"], social.get("reason", "Social entry is not concluded."), social.get("path")),
        gate("model.m0_metrics_inspect_bytes", metrics.get("status", "NOT_RUN"), "M0 is GET-only and records response bytes; it is not a final capacity verdict.", "metrics-summary.json"),
        gate("lease.synthetic_100k_session_subscription", lease["simulation"]["status"], lease["simulation"]["reason"]),
        gate("lease.real_200_subscription", command_gate_status(lease_command, "Existing 200-subscription tool is available but was not run.")[0], command_gate_status(lease_command, "Existing 200-subscription tool is available but was not run.")[1], "lease-batching-evidence.json"),
        gate("lease.batch_item_bytes_duration", "NOT_INSTRUMENTED", "Batch item count has a metric slot; bytes and duration evidence are not yet composed."),
        gate("lease.holder_isolation_stale_drain", "SKIPPED_NOT_COMPOSED", "No single existing formal tool covers holder isolation, stale non-resurrection and drain together."),
        gate("hero.3_of_3", "NOT_RUN", "Three-client Hero gate is reserved for the final SCB-aware H5 entry."),
        gate("task.3_of_3", "NOT_RUN", "Three-task gate is reserved for the final SCB-aware H5 entry."),
        gate("hardening.f1", "NOT_RUN", "F1 requires final SCB-aware H5/Backend composition."),
        gate("hardening.f2", "NOT_RUN", "F2 requires final SCB-aware H5/Backend composition."),
        gate("hardening.f3", "NOT_RUN", "F3 requires final SCB-aware H5/Backend composition."),
        gate("wss.200_baseline", command_gate_status(command_results.get("wss-200"), "Existing 200-WSS baseline/slow tool was not run.")[0], command_gate_status(command_results.get("wss-200"), "Existing 200-WSS baseline/slow tool was not run.")[1], command_results.get("wss-200", {}).get("log")),
        gate("wss.200_slow_reader", command_gate_status(command_results.get("wss-200"), "Existing 200-WSS slow-reader tool was not run.")[0], command_gate_status(command_results.get("wss-200"), "Existing 200-WSS slow-reader tool was not run.")[1], command_results.get("wss-200", {}).get("log")),
        gate("final.10_of_10", "NOT_RUN", "Final 10/10 remains a later release gate; Wave1 only records its slot."),
        gate("unassembled.A1", "SKIPPED_NOT_COMPOSED", "A1 gate is not assembled in R1-Q Wave1."),
        gate("unassembled.A3", "SKIPPED_NOT_COMPOSED", "A3 gate is not assembled in R1-Q Wave1."),
        gate("unassembled.A4", "SKIPPED_NOT_COMPOSED", "A4 gate is not assembled in R1-Q Wave1."),
    ])

    invalid_assembly = bool(assembly_reasons)
    failing_commands = [name for name, result in command_results.items() if result.get("exit_code", 0) not in {0, 2}]
    if failing_commands:
        assembly_reasons.extend(f"composed command failed: {name}" for name in failing_commands)
    if invalid_assembly:
        verdict_value = "FAIL"
        reason_codes = ["INVALID_ASSEMBLY", *assembly_reasons]
    elif failing_commands or any(item["status"] == "FAIL" for item in gates):
        verdict_value = "FAIL"
        reason_codes = ["COMPOSED_GATE_FAILED", *assembly_reasons]
    else:
        verdict_value = "INCONCLUSIVE"
        reason_codes = ["WAVE1_PREPARE_ONLY"]
    handoff = "READY_FOR_OPERATOR_REVIEW" if official.get("official") and not invalid_assembly else "BLOCKED"
    if not official.get("official"):
        reason_codes.append("NO_OFFICIAL_URL")
    if social["status"] == "BLOCKED_SCB_FINAL_ENTRY_UNSET":
        reason_codes.append("SCB_FINAL_ENTRY_UNSET")
    counts: dict[str, int] = {}
    for item in gates:
        counts[item["status"]] = counts.get(item["status"], 0) + 1
    verdict = {
        "schema": "farm.r1-q.verdict/v1",
        "run_id": run_id,
        "wave": "Wave1",
        "verdict": verdict_value,
        "handoff": handoff,
        "invalid_assembly": invalid_assembly,
        "final_pass_claim_allowed": False,
        "reason_codes": reason_codes,
        "gate_counts": counts,
        "gates": gates,
        "unassembled_gates": ["A1", "A3", "A4", "composition.scenario_coordinator", "lease.synthetic_100k_session_subscription", "lease.holder_isolation_stale_drain"],
    }
    manifest = {
        "schema": "farm.r1-q.run-manifest/v1",
        "tool": TOOL_VERSION,
        "run_id": run_id,
        "wave": "Wave1",
        "started_at": run_started_at,
        "backend": backend,
        "h5": h5,
        "environment": {
            "official_url": official,
            "localhost_is_not_staging": True,
            "showcase_auth": "real_register_login",
            "dev_session": "forbidden",
            "mock_auth": False,
            "test_profile_fixture_and_fault_tools": True,
        },
        "social": social,
        "model": {"name": "M0", "metrics_inspect_read_only": True, "byte_report": True},
        "targets": {"lease_simulated_sessions": 100000, "lease_simulated_subscriptions": 100000, "lease_real_subscriptions": 200, "hero": "3/3", "task": "3/3", "hardening": ["F1", "F2", "F3"], "wss": ["baseline", "slow-reader"], "final_repetitions": 10},
        "tools": tools,
        "commands": command_results | metric_commands,
        "actor_layout_v1_baseline": actor_layout,
        "outputs": {"run_manifest": "run-manifest.json", "verdict": "verdict.json", "metrics_summary": "metrics-summary.json", "actor_layout_reference": "actor-layout-v1-baseline.json", "lease_batching_evidence": "lease-batching-evidence.json", "evidence_markdown": "cp-mvp-hardening-r1-evidence.md"},
        "finished_at": utc_now(),
    }
    json_dump(out / "run-manifest.json", manifest)
    json_dump(out / "verdict.json", verdict)
    render_evidence(out, manifest, verdict, lease, metrics)
    print(json.dumps({"out": str(out), "verdict": verdict_value, "handoff": handoff}, ensure_ascii=False))
    if verdict_value == "FAIL":
        return 1
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
