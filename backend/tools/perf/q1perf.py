#!/usr/bin/env python3
"""Q1 performance evidence manifest, sampler, parser, and reporter.

This tool coordinates existing real-protocol Go bots and Playwright witnesses.
It does not implement an HTTP-only synthetic load path and never writes Redis.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import hashlib
import ipaddress
import json
import math
import os
import pathlib
import platform
import re
import resource
import socket
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from typing import Any, Iterable

SCHEMA = "farm-game.q1-perf-evidence.v1"
TASK_ID = "Q1-PERF-BASELINE-V1"
BASELINE_RECORDED = "BASELINE_RECORDED"
PERF_REGRESSION_PASS = "PERF_REGRESSION_PASS"
FAIL_CORRECTNESS = "FAIL_CORRECTNESS"
INCONCLUSIVE_ENVIRONMENT = "INCONCLUSIVE_ENVIRONMENT"
BASELINE_BLOCKED_BY_UNCLASSIFIED_F3 = "BASELINE_BLOCKED_BY_UNCLASSIFIED_F3"
F3_FAULT_CLASSIFICATIONS = {
    "F3_CHAIN_PASS",
    "HARNESS_NOT_ARMED",
    "TRAFFIC_BYPASSED_PROXY",
    "MATCHER_BUG",
    "PRODUCT_FARM_CHANGE_MISSING",
    "PRODUCT_REJOIN_MISSING",
    "H5_FRESH_JOIN_MISSING",
    "CONTRACT_CONFLICT",
}
F3_HARNESS_OR_CONTRACT_CLASSIFICATIONS = {
    "HARNESS_NOT_ARMED", "TRAFFIC_BYPASSED_PROXY", "MATCHER_BUG", "CONTRACT_CONFLICT",
}
NAMESPACE_RE = re.compile(r"^q1perf-[a-z0-9][a-z0-9-]{5,47}$")
# Secret-bearing data fields, not metric names such as
# farm_gate_ticket_operations_total.
SECRET_KEY_RE = re.compile(r"(?:^|_)(?:password|passwd|secret|token|ticket|cookie|authorization)$", re.I)
ALLOWED_BACKEND_DIFF_PREFIXES = (
    "cmd/perf-baseline/",
    "tools/perf/",
    "scripts/perf/",
    "deploy/load/",
    "docs/verification/perf/",
)


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def read_json(path: os.PathLike[str] | str) -> Any:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def atomic_json(path: os.PathLike[str] | str, value: Any) -> None:
    target = pathlib.Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(target.name + ".tmp")
    with open(temporary, "w", encoding="utf-8") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
    os.replace(temporary, target)


def canonical_hash(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def run_git(root: str, *args: str, check: bool = True) -> str:
    proc = subprocess.run(
        ["git", "-C", root, *args], text=True, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, check=False,
    )
    if check and proc.returncode != 0:
        raise RuntimeError(f"git -C {root} {' '.join(args)}: {proc.stderr.strip()}")
    return proc.stdout.strip()


def ensure_commit(root: str, commit: str) -> str:
    resolved = run_git(root, "rev-parse", f"{commit}^{{commit}}")
    if resolved != commit:
        raise ValueError(f"commit must be a full exact SHA: requested={commit} resolved={resolved}")
    return resolved


def dirty_paths(root: str) -> list[str]:
    lines = run_git(root, "status", "--porcelain").splitlines()
    return [line[3:] if len(line) > 3 else line for line in lines]


def committed_diff_paths(root: str, base: str, head: str) -> list[str]:
    if base == head:
        return []
    return [line for line in run_git(root, "diff", "--name-only", f"{base}..{head}").splitlines() if line]


def sanitize(value: Any, key: str = "") -> Any:
    if SECRET_KEY_RE.search(key):
        return "<redacted>"
    if isinstance(value, dict):
        return {str(k): sanitize(v, str(k)) for k, v in value.items()}
    if isinstance(value, list):
        return [sanitize(item, key) for item in value]
    if isinstance(value, str):
        # Do not allow a credential-bearing URI to leak through an innocuous key.
        try:
            parsed = urllib.parse.urlsplit(value)
            if parsed.scheme and (parsed.username is not None or parsed.password is not None):
                host = parsed.hostname or ""
                port = f":{parsed.port}" if parsed.port else ""
                return urllib.parse.urlunsplit((parsed.scheme, host + port, parsed.path, parsed.query, parsed.fragment))
        except ValueError:
            return "<redacted-invalid-uri>"
    return value


def redis_target_identity(target: str) -> tuple[str, str]:
    """Return a non-secret display identity and hash for a Redis endpoint."""
    parsed = urllib.parse.urlsplit(target if "://" in target else "redis://" + target)
    host = (parsed.hostname or "unknown").lower()
    port = parsed.port or 6379
    db = (parsed.path or "/0").lstrip("/") or "0"
    identity = f"{host}:{port}/{db}"
    return identity, "sha256:" + hashlib.sha256(identity.encode()).hexdigest()


def read_mem_total() -> int:
    try:
        for line in pathlib.Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            if line.startswith("MemTotal:"):
                return int(line.split()[1]) * 1024
    except OSError:
        pass
    return 0


def cpu_model() -> str:
    try:
        for line in pathlib.Path("/proc/cpuinfo").read_text(encoding="utf-8").splitlines():
            if line.lower().startswith("model name"):
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or "unknown"


def host_manifest() -> dict[str, Any]:
    nofile = resource.getrlimit(resource.RLIMIT_NOFILE)
    host = {
        "hostname": socket.gethostname(),
        "kernel": platform.release(),
        "os": platform.platform(),
        "architecture": platform.machine(),
        "cpu_model": cpu_model(),
        "logical_cpu_count": os.cpu_count() or 0,
        "memory_total_bytes": read_mem_total(),
        "ulimit_nofile_soft": nofile[0],
        "ulimit_nofile_hard": nofile[1],
        "go_version": command_version(["go", "version"]),
        "node_version": command_version(["node", "--version"]),
        "docker_version": command_version(["docker", "version", "--format", "{{.Server.Version}}"]),
    }
    stable = {k: v for k, v in host.items() if k != "hostname"}
    host["machine_spec_hash"] = canonical_hash(stable)
    return host


def command_version(command: list[str]) -> str:
    try:
        proc = subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=10, check=False)
        return proc.stdout.strip().splitlines()[0][:300] if proc.stdout.strip() else "unavailable"
    except (OSError, subprocess.TimeoutExpired):
        return "unavailable"


def planned_load(config: dict[str, Any]) -> dict[str, Any]:
    profiles = config.get("profiles", {})
    result: dict[str, Any] = {}
    for profile, body in profiles.items():
        if not isinstance(body, dict):
            continue
        result[profile] = sanitize(body)
    return result


def cmd_manifest(args: argparse.Namespace) -> int:
    config = read_json(args.config)
    if config.get("task_id") != TASK_ID:
        raise ValueError(f"config task_id must be {TASK_ID}")
    if not NAMESPACE_RE.fullmatch(args.namespace):
        raise ValueError("namespace must match q1perf-[a-z0-9][a-z0-9-]{5,47}")
    backend_commit = ensure_commit(args.backend_root, args.backend_commit)
    h5_commit = ensure_commit(args.h5_root, args.h5_commit)
    backend_head = run_git(args.backend_root, "rev-parse", "HEAD")
    h5_head = run_git(args.h5_root, "rev-parse", "HEAD")
    load_head = run_git(args.load_root, "rev-parse", "HEAD")
    diff_paths = committed_diff_paths(args.backend_root, backend_commit, backend_head)
    forbidden = [path for path in diff_paths if not path.startswith(ALLOWED_BACKEND_DIFF_PREFIXES)]
    if forbidden:
        raise ValueError(f"backend checkout differs from frozen commit outside Q1 write set: {forbidden}")
    if h5_head != h5_commit:
        raise ValueError(f"H5 checkout HEAD {h5_head} != frozen commit {h5_commit}")
    _, redis_hash = redis_target_identity(args.redis_target)
    config_hash = canonical_hash(config)
    manifest = {
        "schema": SCHEMA,
        "task_id": TASK_ID,
        "run_id": args.run_id,
        "run_kind": args.run_kind,
        "baseline_eligible": bool(config.get("baseline_eligible", True)),
        "protocol_maturity": "CURRENT_HARDENED_JOIN",
        "started_at": utc_now(),
        "finished_at": None,
        "test_window": {"started_at": utc_now(), "finished_at": None},
        "backend_commit": backend_commit,
        "backend_checkout_commit": backend_head,
        "backend_q1_only_diff_paths": diff_paths,
        "h5_commit": h5_commit,
        "h5_checkout_commit": h5_head,
        "load_tool_commit": load_head,
        "load_tool_dirty_paths": dirty_paths(args.load_root),
        "backend_dirty_paths": dirty_paths(args.backend_root),
        "h5_dirty_paths": dirty_paths(args.h5_root),
        "host": host_manifest(),
        "redis": {
            "mode": args.redis_mode,
            "endpoint_hash": redis_hash,
            "namespace": args.namespace,
            "secret_recorded": False,
            "flushall_allowed": False,
            "cleanup_strategy": "destroy_exact_ephemeral_container" if args.redis_mode == "ephemeral_local" else "unlink_exact_run_namespace_only",
            "cleanup_completed": False,
        },
        "config_path": os.path.relpath(os.path.abspath(args.config), os.path.abspath(args.load_root)),
        "config_hash": config_hash,
        "planned_load": planned_load(config),
        "actual_load": {},
        "comparison_baseline": args.comparison_baseline or None,
        "capacity_claim": "NOT_A_30M_DAU_CAPACITY_PROOF",
    }
    atomic_json(pathlib.Path(args.out) / "baseline-run-manifest.json", sanitize(manifest))
    return 0


def parse_proc_status(pid: int) -> dict[str, int]:
    result: dict[str, int] = {}
    try:
        lines = pathlib.Path(f"/proc/{pid}/status").read_text(encoding="utf-8").splitlines()
    except OSError:
        return {"alive": 0}
    result["alive"] = 1
    wanted = {"VmRSS": "rss_bytes", "VmSize": "virtual_bytes", "Threads": "threads"}
    for line in lines:
        name = line.split(":", 1)[0]
        if name not in wanted:
            continue
        fields = line.split()
        value = int(fields[1]) if len(fields) > 1 else 0
        if len(fields) > 2 and fields[2].lower() == "kb":
            value *= 1024
        result[wanted[name]] = value
    try:
        result["fd"] = len(list(pathlib.Path(f"/proc/{pid}/fd").iterdir()))
    except OSError:
        result["fd"] = 0
    try:
        stat_fields = pathlib.Path(f"/proc/{pid}/stat").read_text(encoding="utf-8").split()
        ticks = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
        result["cpu_seconds_total"] = (int(stat_fields[13]) + int(stat_fields[14])) / ticks
    except (OSError, ValueError, IndexError):
        pass
    try:
        for line in pathlib.Path(f"/proc/{pid}/io").read_text(encoding="utf-8").splitlines():
            key, raw = line.split(":", 1)
            if key in {"read_bytes", "write_bytes", "rchar", "wchar"}:
                result[f"io_{key}"] = int(raw.strip())
    except (OSError, ValueError):
        pass
    return result


def host_network() -> dict[str, dict[str, int]]:
    result: dict[str, dict[str, int]] = {}
    try:
        lines = pathlib.Path("/proc/net/dev").read_text(encoding="utf-8").splitlines()[2:]
    except OSError:
        return result
    for line in lines:
        if ":" not in line:
            continue
        iface, raw = line.split(":", 1)
        fields = raw.split()
        if len(fields) >= 16:
            result[iface.strip()] = {
                "rx_bytes": int(fields[0]), "rx_packets": int(fields[1]),
                "tx_bytes": int(fields[8]), "tx_packets": int(fields[9]),
            }
    return result


def parse_prometheus(text: str) -> dict[str, float]:
    # Preserve labels in the key. Values are enough for evidence summaries and
    # this parser intentionally does not attempt full Prometheus exposition.
    keep_prefixes = (
        "go_", "process_", "farmgate_", "farm_gate_", "farm_actor_",
        "farm_workflow_", "farm_subscription_", "farm_event_", "farm_theft_",
        "actor_route_", "grpc_server_",
    )
    result: dict[str, float] = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        fields = line.rsplit(None, 1)
        if len(fields) != 2 or not fields[0].startswith(keep_prefixes):
            continue
        try:
            value = float(fields[1])
        except ValueError:
            continue
        if math.isfinite(value):
            result[fields[0]] = value
    return result


def fetch_metrics(url: str) -> dict[str, Any]:
    try:
        with urllib.request.urlopen(url, timeout=2) as response:  # noqa: S310 - local/test URL supplied by runner
            return {"ok": True, "values": parse_prometheus(response.read().decode("utf-8", "replace"))}
    except Exception as exc:  # Evidence collection must continue if one target disappears.
        return {"ok": False, "error": type(exc).__name__}


def parse_name_value(values: Iterable[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for value in values:
        if "=" not in value:
            raise ValueError(f"expected name=value, got {value!r}")
        name, raw = value.split("=", 1)
        if not re.fullmatch(r"[a-zA-Z0-9_-]+", name):
            raise ValueError(f"invalid evidence label {name!r}")
        result[name] = raw
    return result


def cmd_sample(args: argparse.Namespace) -> int:
    processes = {name: int(raw) for name, raw in parse_name_value(args.process).items()}
    metrics = parse_name_value(args.metrics)
    output = pathlib.Path(args.out)
    output.parent.mkdir(parents=True, exist_ok=True)
    stop_file = pathlib.Path(args.stop_file)
    started = time.monotonic()
    with open(output, "a", encoding="utf-8", buffering=1) as handle:
        while not stop_file.exists() and time.monotonic() - started < args.max_seconds:
            sample = {
                "sampled_at": utc_now(),
                "monotonic_elapsed_seconds": time.monotonic() - started,
                "processes": {name: parse_proc_status(pid) for name, pid in processes.items()},
                "network_scope": "host_network_namespace",
                "host_network": host_network(),
                "prometheus": {name: fetch_metrics(url) for name, url in metrics.items()},
            }
            handle.write(json.dumps(sample, ensure_ascii=False, sort_keys=True) + "\n")
            time.sleep(args.interval)
    return 0


def load_samples(paths: Iterable[str]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for path in paths:
        try:
            for line in pathlib.Path(path).read_text(encoding="utf-8").splitlines():
                if line.strip():
                    result.append(json.loads(line))
        except (OSError, json.JSONDecodeError):
            continue
    return result


def summarize_samples(paths: Iterable[str]) -> dict[str, Any]:
    samples = load_samples(paths)
    process_max: dict[str, dict[str, float]] = {}
    metric_max: dict[str, dict[str, float]] = {}
    previous_cpu: dict[str, tuple[float, float]] = {}
    metrics_scrape_failures = 0
    cpu_count = max(1, os.cpu_count() or 1)
    for sample in samples:
        elapsed = float(sample.get("monotonic_elapsed_seconds", 0))
        for name, values in sample.get("processes", {}).items():
            maxima = process_max.setdefault(name, {})
            for key, value in values.items():
                if isinstance(value, (int, float)):
                    maxima[key] = max(maxima.get(key, float("-inf")), value)
            if "cpu_seconds_total" in values and name in previous_cpu:
                old_elapsed, old_cpu = previous_cpu[name]
                wall_delta = elapsed - old_elapsed
                if wall_delta > 0:
                    cpu_percent = max(0.0, (float(values["cpu_seconds_total"]) - old_cpu) / wall_delta * 100.0)
                    maxima["cpu_percent_one_core_scale"] = max(maxima.get("cpu_percent_one_core_scale", 0), cpu_percent)
                    maxima["cpu_machine_percent"] = max(maxima.get("cpu_machine_percent", 0), cpu_percent / cpu_count)
            if "cpu_seconds_total" in values:
                previous_cpu[name] = (elapsed, float(values["cpu_seconds_total"]))
        for name, scrape in sample.get("prometheus", {}).items():
            if not scrape.get("ok"):
                metrics_scrape_failures += 1
                continue
            maxima = metric_max.setdefault(name, {})
            for key, value in scrape.get("values", {}).items():
                if isinstance(value, (int, float)):
                    maxima[key] = max(maxima.get(key, float("-inf")), value)
    network_delta: dict[str, dict[str, int]] = {}
    if len(samples) >= 2:
        first, last = samples[0].get("host_network", {}), samples[-1].get("host_network", {})
        for iface, values in last.items():
            if iface not in first:
                continue
            network_delta[iface] = {key: max(0, int(value) - int(first[iface].get(key, 0))) for key, value in values.items()}
    return {
        "sample_count": len(samples),
        "process_max": process_max,
        "prometheus_max": metric_max,
        "metrics_scrape_failures": metrics_scrape_failures,
        "host_network_delta": network_delta,
        "network_scope": "host_network_namespace",
    }


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[int((len(ordered) - 1) * q)]


def cmd_run_bot_arrivals(args: argparse.Namespace) -> int:
    if args.arrivals <= 0 or args.rate <= 0:
        raise ValueError("arrivals and rate must be positive")
    if args.max_concurrency <= 0:
        raise ValueError("max concurrency must be positive")
    output = pathlib.Path(args.out)
    evidence_dir = pathlib.Path(args.evidence_dir)
    evidence_dir.mkdir(parents=True, exist_ok=True)
    started_at = utc_now()
    start = time.monotonic()
    child_usage_start = resource.getrusage(resource.RUSAGE_CHILDREN)
    active: dict[int, tuple[subprocess.Popen[Any], float, Any]] = {}
    arrival_lags: list[float] = []
    completion_ms: list[float] = []
    launched = passed = failed = timed_out = spawn_errors = peak_active = 0
    verdicts: list[dict[str, Any]] = []

    def reap(block: bool = False) -> None:
        nonlocal passed, failed
        while True:
            finished: list[int] = []
            for index, (proc, launched_at, log_handle) in active.items():
                code = proc.poll()
                if code is None:
                    continue
                log_handle.close()
                completion_ms.append((time.monotonic() - launched_at) * 1000.0)
                verdict_path = evidence_dir / f"bot-{index:06d}.json"
                verdict = read_json(verdict_path) if verdict_path.exists() else {"verdict": "INCONCLUSIVE", "reason": "verdict missing"}
                verdicts.append({"index": index, "exit_code": code, "verdict": verdict.get("verdict", "INCONCLUSIVE"), "steps": verdict.get("steps", {})})
                if code == 0 and verdict.get("verdict") == "PASS":
                    passed += 1
                else:
                    failed += 1
                finished.append(index)
            for index in finished:
                del active[index]
            if finished or not block or not active:
                return
            time.sleep(0.01)

    for index in range(args.arrivals):
        scheduled = start + index / args.rate
        while True:
            reap()
            now = time.monotonic()
            if now >= scheduled and len(active) < args.max_concurrency:
                break
            time.sleep(min(0.01, max(0.001, scheduled - now)))
        lag_ms = max(0.0, (time.monotonic() - scheduled) * 1000.0)
        arrival_lags.append(lag_ms)
        username = f"{args.user_prefix}{index:06d}"[:24]
        verdict_path = evidence_dir / f"bot-{index:06d}.json"
        log_path = evidence_dir / f"bot-{index:06d}.log"
        log_handle = open(log_path, "w", encoding="utf-8")
        command = [
            args.bot, "--http-url", args.http_url, "--ws-url", args.ws_url,
            "--user", username, "--password", args.password,
            "--json-out", str(verdict_path), *args.bot_extra_arg,
        ]
        try:
            proc = subprocess.Popen(command, stdout=log_handle, stderr=subprocess.STDOUT, text=True)
        except OSError as exc:
            log_handle.write(f"spawn failed: {type(exc).__name__}\n")
            log_handle.close()
            spawn_errors += 1
            continue
        launched += 1
        active[index] = (proc, time.monotonic(), log_handle)
        peak_active = max(peak_active, len(active))

    deadline = time.monotonic() + args.completion_timeout
    while active and time.monotonic() < deadline:
        reap(block=True)
    for index, (proc, _, log_handle) in list(active.items()):
        proc.kill()
        proc.wait(timeout=5)
        log_handle.close()
        timed_out += 1
        failed += 1
        verdicts.append({"index": index, "exit_code": proc.returncode, "verdict": "INCONCLUSIVE", "reason": "completion timeout"})
        del active[index]
    child_usage_end = resource.getrusage(resource.RUSAGE_CHILDREN)
    elapsed = max(0.001, time.monotonic() - start)
    schedule_lag_violations = sum(1 for value in arrival_lags if value > args.max_arrival_lag_ms)
    report = {
        "run_id": f"bot-arrivals-{int(time.time() * 1000)}",
        "scenario": "authority-business-journey",
        "configured_connections": args.arrivals,
        "connect_rate_per_second": args.rate,
        "configured_duration_seconds": args.arrivals / args.rate,
        "started_at": started_at,
        "finished_at": utc_now(),
        "attempted": launched,
        "ready": passed,
        "active_at_end": 0,
        "peak_active": peak_active,
        "login_errors": 0,
        "connect_errors": 0,
        "hello_errors": 0,
        "join_errors": 0,
        "scenario_attempted": launched,
        "scenario_succeeded": passed,
        "scenario_errors": failed,
        "unexpected_disconnects": 0,
        "ready_rate": passed / launched if launched else 0,
        "elapsed_ms": elapsed * 1000.0,
        "qps": passed / elapsed,
        "ready_p50_ms": percentile(completion_ms, 0.50),
        "ready_p95_ms": percentile(completion_ms, 0.95),
        "ready_p99_ms": percentile(completion_ms, 0.99),
        "ready_max_ms": max(completion_ms, default=0.0),
        "scenario_p95_ms": percentile(completion_ms, 0.95),
        "scheduled_arrivals": args.arrivals,
        "launched_arrivals": launched,
        "dropped_arrivals": args.arrivals - launched,
        "arrival_lag_p50_ms": percentile(arrival_lags, 0.50),
        "arrival_lag_p95_ms": percentile(arrival_lags, 0.95),
        "arrival_lag_p99_ms": percentile(arrival_lags, 0.99),
        "arrival_lag_max_ms": max(arrival_lags, default=0.0),
        "max_arrival_lag_ms": args.max_arrival_lag_ms,
        "arrival_lag_violation_count": schedule_lag_violations,
        "spawn_errors": spawn_errors,
        "timed_out": timed_out,
        "child_user_cpu_seconds": child_usage_end.ru_utime - child_usage_start.ru_utime,
        "child_system_cpu_seconds": child_usage_end.ru_stime - child_usage_start.ru_stime,
        "bot_verdicts": verdicts,
    }
    atomic_json(output, sanitize(report))
    return 0 if launched == args.arrivals and passed == launched and schedule_lag_violations == 0 else (1 if failed else 2)


def status_from_ws(reports: list[dict[str, Any]], planned: int) -> tuple[str, str, dict[str, int]]:
    actual = sum(int(report.get("attempted", 0)) for report in reports)
    ready = sum(int(report.get("ready", 0)) for report in reports)
    errors = sum(
        int(report.get(key, 0))
        for report in reports
        for key in ("login_errors", "connect_errors", "hello_errors", "join_errors", "scenario_errors", "unexpected_disconnects")
    )
    invariants = {
        "protocol_or_scenario_errors": errors,
        "planned_arrivals_missing": max(0, planned - actual),
        "ready_arrivals_missing": max(0, actual - ready),
    }
    lag_violations = sum(int(report.get("arrival_lag_violation_count", 0)) for report in reports)
    invariants["arrival_lag_violation_count"] = lag_violations
    if errors > 0:
        return "FAIL", "real protocol driver reported errors", invariants
    if lag_violations > 0:
        return "INCONCLUSIVE", "load generator exceeded the frozen arrival-lag budget", invariants
    if actual < planned:
        return "INCONCLUSIVE", "actual arrivals did not reach the frozen plan", invariants
    if ready < actual:
        return "FAIL", "some injected clients did not reach the Ready barrier", invariants
    return "PASS", "planned open-arrival load reached Ready without driver errors", invariants


def cmd_ingest_ws(args: argparse.Namespace) -> int:
    reports = [read_json(path) for path in args.input]
    planned = args.planned if args.planned is not None else sum(int(report.get("configured_connections", 0)) for report in reports)
    actual = sum(int(report.get("attempted", 0)) for report in reports)
    ready = sum(int(report.get("ready", 0)) for report in reports)
    configured_rates = sum(int(report.get("connect_rate_per_second", 0)) for report in reports)
    configured_duration = max((float(report.get("configured_duration_seconds", 0)) for report in reports), default=0)
    status, reason, invariants = status_from_ws(reports, planned)
    planned_window = max((planned / configured_rates) if configured_rates > 0 else configured_duration, 0.001)
    actual_qps = actual / planned_window
    completed = sum(
        int(report.get("scenario_succeeded", 0)) if int(report.get("scenario_attempted", 0)) > 0 else int(report.get("ready", 0))
        for report in reports
    )
    resources = summarize_samples(args.samples)
    loadgen_machine_cpu = sum(
        float(values.get("cpu_machine_percent", 0))
        for name, values in resources.get("process_max", {}).items()
        if name.startswith("load")
    )
    if status == "PASS" and loadgen_machine_cpu >= args.load_generator_cpu_stop_percent:
        status = "INCONCLUSIVE"
        reason = f"load generator reached {loadgen_machine_cpu:.1f}% of machine CPU"
    metrics = {
        "planned_arrivals": planned,
        "actual_injected_arrivals": actual,
        "admitted_ready": ready,
        "completed": completed,
        "schedule_drop": max(0, planned - actual),
        "planned_qps": configured_rates,
        "actual_injected_qps": actual_qps,
        "completed_qps": completed / planned_window,
        "arrival_qps_measurement": "attempted / frozen launch window; per-arrival lag pending ws-stress instrumentation",
        "p50_ms": max((float(report.get("ready_p50_ms", 0)) for report in reports), default=0),
        "p95_ms": max((float(report.get("ready_p95_ms", 0)) for report in reports), default=0),
        "p99_ms": max((float(report.get("ready_p99_ms", 0)) for report in reports), default=0),
        "scenario_p95_ms": max((float(report.get("scenario_p95_ms", 0)) for report in reports), default=0),
        "arrival_lag_p50_ms": max((float(report.get("arrival_lag_p50_ms", 0)) for report in reports), default=0),
        "arrival_lag_p95_ms": max((float(report.get("arrival_lag_p95_ms", 0)) for report in reports), default=0),
        "arrival_lag_p99_ms": max((float(report.get("arrival_lag_p99_ms", 0)) for report in reports), default=0),
        "arrival_lag_max_ms": max((float(report.get("arrival_lag_max_ms", 0)) for report in reports), default=0),
        "peak_connections": sum(int(report.get("peak_active", 0)) for report in reports),
        "driver_reports": reports,
        "resources": resources,
        "load_generator_machine_cpu_percent_max": loadgen_machine_cpu,
        "load_generator_cpu_stop_percent": args.load_generator_cpu_stop_percent,
    }
    result = {
        "schema": SCHEMA,
        "name": args.name,
        "profile": args.profile,
        "status": status,
        "failure_class": "correctness" if status == "FAIL" else ("environment" if status == "INCONCLUSIVE" else "none"),
        "reason": reason,
        "planned": {"arrivals": planned, "qps": configured_rates, "open_arrival": True},
        "actual": {"injected": actual, "ready": ready, "completed": completed, "schedule_drop": max(0, planned - actual)},
        "metrics": metrics,
        "correctness_invariants": invariants,
        "redis_endpoint_hash": args.redis_endpoint_hash,
        "namespace": args.namespace,
        "evidence": args.input + args.samples,
        "finished_at": utc_now(),
    }
    atomic_json(args.out, sanitize(result))
    return 0 if status == "PASS" else (1 if status == "FAIL" else 2)


def read_ndjson(path: pathlib.Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    if not path.exists():
        return records
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            records.append(value)
    return records


def parse_timestamp(value: str) -> dt.datetime | None:
    value = value.strip()
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def proxy_log_plan_observations(path: pathlib.Path, utc_offset_hours: float) -> list[dict[str, Any]]:
    observations: list[dict[str, Any]] = []
    if not path.exists():
        return observations
    timezone = dt.timezone(dt.timedelta(hours=utc_offset_hours))
    pattern = re.compile(
        r"^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) fault plan updated: .*"
        r"FailFarmChangeAllowRejoin:(true|false)"
    )
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        match = pattern.search(line)
        if not match:
            continue
        local = dt.datetime.strptime(match.group(1), "%Y/%m/%d %H:%M:%S").replace(tzinfo=timezone)
        observations.append({
            "observed_at": local.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
            "source": "proxy.log",
            "http_status": None,
            "plan": {"fail_farm_change_allow_rejoin": match.group(2) == "true"},
        })
    return observations


def fault_plan_observations(root: pathlib.Path, utc_offset_hours: float) -> list[dict[str, Any]]:
    observed = read_ndjson(root / "fault-plan-observations.ndjson")
    if observed:
        return observed
    return proxy_log_plan_observations(root / "proxy.log", utc_offset_hours)


def event_at_or_after(event: dict[str, Any], boundary: dt.datetime | None) -> bool:
    if boundary is None:
        return True
    timestamp = parse_timestamp(str(event.get("timestamp", event.get("observed_at", ""))))
    return timestamp is not None and timestamp >= boundary


def analyze_f3(root: pathlib.Path, utc_offset_hours: float = 8.0) -> dict[str, Any]:
    browser = read_json(root / "browser-verdict.json") if (root / "browser-verdict.json").exists() else {}
    plans = fault_plan_observations(root, utc_offset_hours)
    armed = [item for item in plans if item.get("plan", {}).get("fail_farm_change_allow_rejoin") is True]
    arm_time = parse_timestamp(str(armed[0].get("observed_at", ""))) if armed else None
    protocol = read_ndjson(root / "protocol-events.ndjson")
    after_arm = [event for event in protocol if event_at_or_after(event, arm_time)] if armed else []
    tap_path = root / "ws-frame-tap.ndjson"
    tap = read_ndjson(tap_path)
    tap_after_arm = [event for event in tap if event_at_or_after(event, arm_time)] if armed else []
    theft_commands = [event for event in tap_after_arm if event.get("event") == "CLIENT_COMMAND" and event.get("command_type") == "THEFT"]
    theft_operation_ids = sorted({str(event.get("operation_id")) for event in theft_commands if event.get("operation_id")})
    command_results = [
        event for event in tap_after_arm
        if event.get("event") == "COMMAND_RESULT" and str(event.get("operation_id", "")) in theft_operation_ids
    ]
    theft_connections = {str(event.get("connection_id")) for event in theft_commands if event.get("connection_id")}
    theft_target_farms = {str(event.get("target_farm_id")) for event in theft_commands if event.get("target_farm_id")}
    theft_cycles = {str(event.get("crop_cycle_id")) for event in theft_commands if event.get("crop_cycle_id")}
    target_subscriptions = {
        str(event.get("subscription_id")) for event in tap
        if event.get("event") in {"COMMAND_RESULT", "SERVER_PUSH"} and event.get("subscription_id") and
        str(event.get("connection_id", "")) in theft_connections and
        (not theft_target_farms or str(event.get("target_farm_id", "")) in theft_target_farms)
    }
    all_post_arm_farm_changes = [event for event in after_arm if event.get("item_type") == "FARM_CHANGE"]

    def is_target_farm_change(event: dict[str, Any]) -> bool:
        if not theft_commands:
            return False
        # A Theft FarmChange fan-outs to owner and visitor subscriptions. The
        # exact crop_cycle_id binds both deliveries to the tapped operation;
        # requiring the visitor subscription would incorrectly discard the
        # owner's first failed delivery. Fall back to subscription only when a
        # cycle witness is unavailable.
        if theft_cycles:
            return str(event.get("crop_cycle_id", "")) in theft_cycles
        if target_subscriptions:
            return any(str(event.get("item_id", "")).startswith(subscription + ":") for subscription in target_subscriptions)
        return False

    farm_changes = [event for event in all_post_arm_farm_changes if is_target_farm_change(event)]
    all_fault_events = [event for event in after_arm if event.get("type") == "FAIL_FARM_CHANGE"]
    fault_events = [event for event in all_fault_events if is_target_farm_change(event)]
    all_rejoin_events = [
        event for event in after_arm
        if event.get("item_type") == "REJOIN_REQUIRED" or event.get("type") == "REJOIN_REQUIRED"
    ]
    rejoin_events = [
        event for event in all_rejoin_events
        if target_subscriptions and any(str(event.get("item_id", "")).startswith(subscription + ":") for subscription in target_subscriptions)
    ]
    redis_dump = read_json(root / "theft-redis-dump.json") if (root / "theft-redis-dump.json").exists() else {}
    orders = redis_dump.get("orders", []) if isinstance(redis_dump, dict) else []
    matching_orders = [order for order in orders if str(order.get("operation_id", "")) in theft_operation_ids]
    authority_commit_proven = any(order.get("dispatch_order_id") and order.get("state") for order in matching_orders)

    farm_change_item_ids = sorted({str(event.get("item_id")) for event in farm_changes if event.get("item_id")})
    if not armed:
        classification = "HARNESS_NOT_ARMED"
        decisive = "no read-back or proxy update proves fail_farm_change_allow_rejoin=true"
    elif not protocol:
        classification = "TRAFFIC_BYPASSED_PROXY"
        decisive = "the fault proxy retained no DeliverBatch/ActivateSubscription traffic"
    elif farm_changes and not fault_events:
        classification = "MATCHER_BUG"
        decisive = "the target cycle/subscription FARM_CHANGE reached the armed proxy but FAIL_FARM_CHANGE did not fire"
    elif fault_events and not rejoin_events:
        classification = "PRODUCT_REJOIN_MISSING"
        decisive = "FAIL_FARM_CHANGE fired but no REJOIN_REQUIRED was observed"
    elif fault_events and rejoin_events and browser.get("verdict") == "PASS" and any(
        step.get("step") == "f3-farm-change-failure-rejoined" for step in browser.get("steps", [])
    ) and authority_commit_proven:
        classification = "F3_CHAIN_PASS"
        decisive = "the same tapped Theft operation is bound to a durable order, injected FarmChange failure, REJOIN_REQUIRED, fresh Join, and converged H5 fact"
    elif fault_events and rejoin_events and browser.get("verdict") != "PASS" and not any(
        step.get("step") == "f3-farm-change-failure-rejoined" for step in browser.get("steps", [])
    ):
        classification = "H5_FRESH_JOIN_MISSING"
        decisive = "fault and REJOIN were observed, but the H5 did not complete its F3 fresh-Join witness"
    elif theft_commands and authority_commit_proven and not farm_changes:
        classification = "PRODUCT_FARM_CHANGE_MISSING"
        decisive = "the same tapped Theft operation has a durable Pair order, but no post-arm FARM_CHANGE reached the proxy"
    else:
        classification = "CONTRACT_CONFLICT"
        if tap_path.exists() and not theft_commands:
            contract_detail = "H5_WIRE_THEFT_MISSING"
            decisive = "the plan was armed, but the H5 action emitted no wire THEFT command; the fault/product delivery chain never entered T1"
        else:
            contract_detail = "EVIDENCE_IDENTITY_MISSING"
            decisive = (
                "the plan was armed and earlier traffic proves the proxy path, but retained evidence cannot bind "
                "one wire Theft operation to an Authority commit and downstream FarmChange"
            )
    if classification != "CONTRACT_CONFLICT":
        contract_detail = None
    assert classification in F3_FAULT_CLASSIFICATIONS

    chain = {
        "T0_fault_plan_armed": {
            "proven": bool(armed),
            "observed_at": armed[0].get("observed_at") if armed else None,
            "plan_id": None,
            "note": "the B0 fault controller has no plan-id field; future evidence records UTC read-back observations",
        },
        "T1_h5_operation_on_wire": {
            "proven": bool(theft_commands),
            "operation_ids": theft_operation_ids,
            "target_farm_ids": sorted({str(event.get("target_farm_id")) for event in theft_commands if event.get("target_farm_id")}),
            "crop_cycle_ids": sorted({str(event.get("crop_cycle_id")) for event in theft_commands if event.get("crop_cycle_id")}),
        },
        "T2_authority_commit": {"proven": authority_commit_proven, "matching_order_count": len(matching_orders)},
        "T3_farm_change_generated": {"proven": bool(farm_changes), "unique_item_count": len(farm_change_item_ids)},
        "T4_downlink_item_constructed": {"proven": bool(farm_changes), "item_ids": farm_change_item_ids},
        "T5_proxy_observed_farm_change": {"proven": bool(farm_changes), "delivery_attempt_count": len(farm_changes)},
        "T6_fault_injected": {"proven": bool(fault_events), "injection_attempt_count": len(fault_events)},
        "T8_rejoin_required": {"proven": bool(rejoin_events), "count": len(rejoin_events)},
        "T9_h5_fresh_join": {
            "proven": any(step.get("step") == "f3-farm-change-failure-rejoined" for step in browser.get("steps", [])),
        },
        "T10_snapshot_contains_committed_fact": {
            "proven": browser.get("verdict") == "PASS" and any(
                step.get("step") == "f3-farm-change-failure-rejoined" for step in browser.get("steps", [])
            ),
        },
    }
    return {
        "schema": SCHEMA,
        "task": "Q1-F3-FAULT-CHAIN-TRIAGE",
        "classification": classification,
        "contract_detail": contract_detail,
        "baseline_state": "CORRECTNESS_ANCHOR_PASS" if classification == "F3_CHAIN_PASS" else BASELINE_BLOCKED_BY_UNCLASSIFIED_F3,
        "decisive_evidence": decisive,
        "chain": chain,
        "facts": {
            "browser_verdict": browser.get("verdict", "MISSING"),
            "proxy_traffic_event_count": len(protocol),
            "post_arm_proxy_event_count": len(after_arm),
            "post_arm_farm_change_count": len(all_post_arm_farm_changes),
            "target_bound_post_arm_farm_change_count": len(farm_changes),
            "target_subscription_ids": sorted(target_subscriptions),
            "fail_farm_change_count": len(all_fault_events),
            "target_bound_fail_farm_change_count": len(fault_events),
            "rejoin_required_count": len(all_rejoin_events),
            "target_bound_rejoin_required_count": len(rejoin_events),
            "tapped_theft_command_count": len(theft_commands),
            "matching_redis_order_count": len(matching_orders),
        },
        "evidence_files": {
            "plan": "fault-plan-observations.ndjson" if (root / "fault-plan-observations.ndjson").exists() else "proxy.log",
            "proxy_events": "protocol-events.ndjson",
            "wire_tap": "ws-frame-tap.ndjson" if (root / "ws-frame-tap.ndjson").exists() else None,
            "redis_dump": "theft-redis-dump.json" if (root / "theft-redis-dump.json").exists() else None,
            "browser": "browser-verdict.json" if (root / "browser-verdict.json").exists() else None,
        },
        "generated_at": utc_now(),
    }


def cmd_triage_f3(args: argparse.Namespace) -> int:
    result = analyze_f3(pathlib.Path(args.evidence_dir), args.proxy_log_utc_offset_hours)
    atomic_json(args.out, sanitize(result))
    print(result["classification"])
    return 0


def cmd_watch_fault(args: argparse.Namespace) -> int:
    output = pathlib.Path(args.out)
    output.parent.mkdir(parents=True, exist_ok=True)
    stop = pathlib.Path(args.stop_file)
    deadline = time.monotonic() + args.max_seconds
    last_hash = ""
    with open(output, "a", encoding="utf-8", buffering=1) as handle:
        while not stop.exists() and time.monotonic() < deadline:
            observed: dict[str, Any] = {"observed_at": utc_now(), "source": "GET /admin/plan"}
            try:
                with urllib.request.urlopen(args.admin_url.rstrip("/") + "/admin/plan", timeout=2) as response:  # noqa: S310
                    observed["http_status"] = response.status
                    observed["plan"] = json.loads(response.read().decode("utf-8", "replace"))
            except Exception as exc:
                observed.update({"http_status": None, "error": type(exc).__name__})
            digest = canonical_hash(observed.get("plan", {"error": observed.get("error")}))
            if digest != last_hash:
                handle.write(json.dumps(sanitize(observed), ensure_ascii=False, sort_keys=True) + "\n")
                last_hash = digest
            time.sleep(args.interval)
    return 0


class RedisReadOnlyClient:
    """Minimal RESP2 client restricted to the read-only commands used by Q1."""

    ALLOWED = {"AUTH", "SELECT", "SCAN", "TYPE", "HMGET", "GET", "ZRANGE"}

    def __init__(self, host: str, port: int, db: int, password: str = "") -> None:
        try:
            address = ipaddress.ip_address(socket.gethostbyname(host))
        except ValueError as exc:
            raise ValueError(f"invalid Redis host: {host}") from exc
        if not address.is_loopback:
            raise ValueError("theft dump is restricted to ephemeral loopback Redis")
        self.sock = socket.create_connection((host, port), timeout=5)
        self.reader = self.sock.makefile("rb")
        if password:
            self.command("AUTH", password)
        if db:
            self.command("SELECT", str(db))

    def close(self) -> None:
        self.reader.close()
        self.sock.close()

    def command(self, *parts: str) -> Any:
        command = parts[0].upper()
        if command not in self.ALLOWED:
            raise ValueError(f"Redis command {command} is not read-only/allowed")
        encoded = [part.encode("utf-8") for part in parts]
        payload = b"*" + str(len(encoded)).encode() + b"\r\n"
        for part in encoded:
            payload += b"$" + str(len(part)).encode() + b"\r\n" + part + b"\r\n"
        self.sock.sendall(payload)
        return self._response()

    def _response(self) -> Any:
        prefix = self.reader.read(1)
        if not prefix:
            raise RuntimeError("Redis closed the diagnostic connection")
        line = self.reader.readline()
        if not line.endswith(b"\r\n"):
            raise RuntimeError("malformed Redis response")
        body = line[:-2]
        if prefix == b"+":
            return body
        if prefix == b"-":
            raise RuntimeError("Redis diagnostic command failed: " + body.decode("utf-8", "replace"))
        if prefix == b":":
            return int(body)
        if prefix == b"$":
            length = int(body)
            if length < 0:
                return None
            data = self.reader.read(length)
            if self.reader.read(2) != b"\r\n":
                raise RuntimeError("malformed Redis bulk response")
            return data
        if prefix == b"*":
            length = int(body)
            return None if length < 0 else [self._response() for _ in range(length)]
        raise RuntimeError("unsupported Redis response")

    def scan(self, pattern: str, limit: int) -> list[str]:
        cursor = "0"
        found: set[str] = set()
        while True:
            response = self.command("SCAN", cursor, "MATCH", pattern, "COUNT", "1000")
            if not isinstance(response, list) or len(response) != 2:
                raise RuntimeError("malformed Redis SCAN response")
            cursor = redis_text(response[0])
            for key in response[1] or []:
                found.add(redis_text(key))
                if len(found) > limit:
                    raise RuntimeError(f"Redis diagnostic key limit exceeded ({limit})")
            if cursor == "0":
                return sorted(found)


def redis_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return str(value)


def redis_hash_fields(client: RedisReadOnlyClient, key: str, fields: list[str]) -> dict[str, str | None]:
    values = client.command("HMGET", key, *fields)
    return {field: (redis_text(value) if value is not None else None) for field, value in zip(fields, values)}


def cmd_dump_redis_theft(args: argparse.Namespace) -> int:
    password = os.environ.get("Q1PERF_REDIS_PASSWORD", "")
    client = RedisReadOnlyClient(args.host, args.port, args.db, password)
    try:
        order_pattern = f"*classic-farm:{args.namespace}:*:friend-auth:pair:*:order:*"
        due_pattern = f"*classic-farm:{args.namespace}:*:friend-auth:pair:*:due"
        target_pattern = args.target_binding_prefix.rstrip(":") + ":theft-*"
        workflow_pattern = args.workflow_prefix.rstrip(":") + ":{workflow}:record:*"
        order_keys = client.scan(order_pattern, args.max_keys)
        due_keys = client.scan(due_pattern, args.max_keys)
        target_keys = client.scan(target_pattern, args.max_keys)
        workflow_keys = client.scan(workflow_pattern, args.max_keys)
        order_fields = [
            "schema_version", "work_id", "work_type", "state", "work_ref",
            "canonical_source_authority_id", "operation_id", "visitor_player_id",
            "owner_player_id", "target_farm_id", "dispatch_order_id", "created_at_ms",
            "updated_at_ms", "retry_count", "next_due_at_ms", "work_version",
            "target_binding_hash", "permit_payload_hash", "workflow_deadline_at_ms",
        ]
        due_scores: dict[str, str] = {}
        due_indexes: list[dict[str, Any]] = []
        for key in due_keys:
            values = client.command("ZRANGE", key, "0", "-1", "WITHSCORES") or []
            members = []
            for index in range(0, len(values), 2):
                member = redis_text(values[index])
                score = redis_text(values[index + 1]) if index + 1 < len(values) else ""
                members.append({"work_ref": member, "due_score": score})
                due_scores[member] = score
            if members:
                due_indexes.append({"key_hash": canonical_hash(key), "members": members})
        orders: list[dict[str, Any]] = []
        for key in order_keys:
            fields = redis_hash_fields(client, key, order_fields)
            fields["order_id"] = fields.get("work_id")
            fields["source_operation_id"] = fields.get("operation_id")
            fields["source_player_id"] = fields.get("visitor_player_id")
            fields["target_player_id"] = fields.get("owner_player_id")
            fields["due_score"] = due_scores.get(fields.get("work_ref") or "")
            fields["workflow_id"] = None
            fields["crop_cycle_id"] = None
            fields["theft_attempt_id"] = None
            fields["last_error"] = None
            fields["redis_key_hash"] = canonical_hash(key)
            orders.append(fields)
        bindings: list[dict[str, Any]] = []
        for key in target_keys:
            raw = client.command("GET", key)
            if raw is None:
                continue
            try:
                binding = json.loads(redis_text(raw))
            except json.JSONDecodeError:
                binding = {"decode_error": True, "value_sha256": "sha256:" + hashlib.sha256(raw).hexdigest()}
            binding["redis_key_hash"] = canonical_hash(key)
            bindings.append(binding)
        workflows = []
        for key in workflow_keys:
            fields = redis_hash_fields(client, key, ["revision", "terminal", "next_retry_at_ms", "lease_epoch", "lease_until_ms"])
            fields["workflow_id"] = key.rsplit(":", 1)[-1]
            fields["redis_key_hash"] = canonical_hash(key)
            workflows.append(fields)
        binding_by_dispatch = {
            str(binding.get("DispatchOrderID")): binding
            for binding in bindings if isinstance(binding, dict) and binding.get("DispatchOrderID")
        }
        for order in orders:
            binding = binding_by_dispatch.get(str(order.get("dispatch_order_id", "")))
            if binding:
                order["target_binding"] = binding
                order["crop_cycle_id"] = binding.get("CropCycleID")
        result = {
            "schema": SCHEMA,
            "task": "WF-THEFT-INVALID-ORDER-DIAG",
            "probe_mode": "READ_ONLY_EPHEMERAL_LOOPBACK",
            "namespace": args.namespace,
            "secret_recorded": False,
            "commands_allowed": sorted(RedisReadOnlyClient.ALLOWED),
            "orders": orders,
            "due_indexes": due_indexes,
            "target_bindings": bindings,
            "workflows": workflows,
            "counts": {
                "orders": len(orders), "due_members": sum(len(index["members"]) for index in due_indexes),
                "target_bindings": len(bindings), "workflows": len(workflows),
            },
            "field_limits": {
                "last_error": "not persisted by the production worker; only bounded aggregate log telemetry exists",
                "theft_attempt_id": "not present in Pair source-order fields",
                "workflow_id": "not present in Pair source-order fields; workflow records are listed separately",
                "proxy_plan_id": "the B0 fault controller has no plan-id contract",
                "browser_operation_id": "provided separately by ws-frame-tap.ndjson",
            },
            "generated_at": utc_now(),
        }
        atomic_json(args.out, sanitize(result))
    finally:
        client.close()
    return 0


def analyze_invalid_orders(root: pathlib.Path, utc_offset_hours: float = 8.0) -> dict[str, Any]:
    worker_path = root / "worker.log"
    rounds: list[dict[str, Any]] = []
    pattern = re.compile(
        r"^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}).*recovery round theft=\{"
        r"Discovered:(\d+).*InvalidOrders:(\d+).*Errors:(\d+).*LastError:(.*)\}$"
    )
    timezone = dt.timezone(dt.timedelta(hours=utc_offset_hours))
    if worker_path.exists():
        for line in worker_path.read_text(encoding="utf-8", errors="replace").splitlines():
            match = pattern.search(line)
            if not match:
                continue
            local = dt.datetime.strptime(match.group(1), "%Y/%m/%d %H:%M:%S").replace(tzinfo=timezone)
            rounds.append({
                "observed_at": local.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
                "discovered": int(match.group(2)), "invalid_orders": int(match.group(3)),
                "errors": int(match.group(4)), "last_error": match.group(5),
            })
    plans = fault_plan_observations(root, utc_offset_hours)
    armed = [item for item in plans if item.get("plan", {}).get("fail_farm_change_allow_rejoin") is True]
    arm_time = parse_timestamp(str(armed[0].get("observed_at", ""))) if armed else None
    invalid_rounds = [item for item in rounds if item["invalid_orders"] > 0]
    invalid_processing_observations = sum(item["invalid_orders"] for item in invalid_rounds)
    first_invalid = parse_timestamp(invalid_rounds[0]["observed_at"]) if invalid_rounds else None
    predates_f3 = bool(first_invalid and arm_time and first_invalid < arm_time)
    dump = read_json(root / "theft-redis-dump.json") if (root / "theft-redis-dump.json").exists() else {}
    orders = dump.get("orders", []) if isinstance(dump, dict) else []
    due_indexes = dump.get("due_indexes", []) if isinstance(dump, dict) else []
    indexed_due_refs = {
        str(member.get("work_ref"))
        for index in due_indexes if isinstance(index, dict)
        for member in index.get("members", []) if isinstance(member, dict) and member.get("work_ref")
    }
    declared_due_count = int(dump.get("counts", {}).get("due_members", 0) or 0) if isinstance(dump, dict) else 0
    tap = read_ndjson(root / "ws-frame-tap.ndjson")
    wire_command_by_operation = {
        str(event.get("operation_id")): str(event.get("command_type"))
        for event in tap
        if event.get("event") == "CLIENT_COMMAND" and event.get("operation_id") and event.get("command_type")
    }
    theft_ops = {operation for operation, command in wire_command_by_operation.items() if command == "THEFT"}
    dispatch_operation = {
        str(order.get("order_id")): str(order.get("source_operation_id") or order.get("operation_id"))
        for order in orders
        if order.get("order_id") and (order.get("source_operation_id") or order.get("operation_id"))
    }
    enriched_orders = []
    for source in orders:
        order = dict(source)
        operation = str(order.get("source_operation_id") or order.get("operation_id") or "")
        if not operation and order.get("dispatch_order_id"):
            operation = dispatch_operation.get(str(order.get("dispatch_order_id")), "")
        order["correlated_operation_id"] = operation or None
        order["source_command_type"] = wire_command_by_operation.get(operation)
        order["due_candidate_at_dump"] = order.get("due_score") is not None
        enriched_orders.append(order)
    due_orders = [order for order in enriched_orders if order["due_candidate_at_dump"]]
    order_due_refs = {
        str(order.get("work_ref") or "order:" + str(order.get("order_id")))
        for order in due_orders if order.get("work_ref") or order.get("order_id")
    }
    physical_due_refs = indexed_due_refs | order_due_refs
    physical_due_count = max(declared_due_count, len(physical_due_refs), len(due_orders))
    unmatched_due_count = max(0, physical_due_count - len(due_orders))
    matched = [order for order in due_orders if str(order.get("correlated_operation_id", "")) in theft_ops]
    care_due = [order for order in due_orders if order.get("source_command_type") == "CARE"]
    if physical_due_count == 0 and invalid_processing_observations == 0:
        classification = "NO_INVALID_PAIR_DUE_RESIDUE"
        conclusion = "no InvalidOrder processing observation or persistent Pair DueIndex candidate remains after scoped recovery"
    elif physical_due_count == 0 and invalid_processing_observations > 0:
        classification = "INVALID_ORDER_PROCESSING_WITHOUT_PERSISTENT_RESIDUE"
        conclusion = "worker InvalidOrder processing was observed even though no candidate remained due at the diagnostic dump"
    elif physical_due_count > 0 and invalid_processing_observations == 0:
        classification = "PERSISTENT_PAIR_DUE_RESIDUE"
        conclusion = "one or more Pair source orders remained due without a corresponding InvalidOrder processing observation"
    elif predates_f3 and due_orders and physical_due_count == len(due_orders) and len(care_due) == len(due_orders):
        classification = "PRODUCT_WORKER_RECOVERY_SCOPE_FILTER_BUG"
        conclusion = "both persistent invalid DueIndex candidates resolve to the pre-F3 F1 CARE operation; Theft recovery repeatedly treats them as Theft and asks for a Theft target binding"
    elif predates_f3:
        classification = "PREEXISTING_INVALID_CANDIDATES_NOT_ATTRIBUTABLE_TO_F3"
        conclusion = "invalid candidates were already recurring before the F3 plan was armed and therefore are not root-cause evidence for the later F3 operation"
    elif due_orders and theft_ops and matched:
        classification = "CORRELATED_TO_TAPPED_F3_OPERATION"
        conclusion = "one or more still-due source orders share the tapped browser Theft operation_id"
    elif due_orders and theft_ops:
        classification = "NOT_THE_TAPPED_F3_OPERATION"
        conclusion = "still-due source-order operation_ids do not match the tapped browser Theft operation_id"
    else:
        classification = "CORRELATION_NOT_PROVABLE_FROM_RETAINED_EVIDENCE"
        conclusion = "the retained run has aggregate per-round counters but no source-order IDs or browser wire operation_id"
    order_rows = []
    wanted = [
        "order_id", "workflow_id", "source_operation_id", "correlated_operation_id", "source_command_type",
        "source_player_id", "target_farm_id", "target_player_id", "crop_cycle_id", "theft_attempt_id",
        "created_at_ms", "updated_at_ms", "retry_count", "due_score", "due_candidate_at_dump",
        "last_error", "target_binding",
    ]
    for order in enriched_orders:
        order_rows.append({key: order.get(key) for key in wanted})
    return {
        "schema": SCHEMA,
        "task": "WF-THEFT-INVALID-ORDER-DIAG",
        "classification": classification,
        "conclusion": conclusion,
        "counter_semantics": "InvalidOrders is a per-recovery-round processing count, not a unique-order cardinality",
        "observations": {
            "rounds_with_invalid_orders": len(invalid_rounds),
            "invalid_processing_observations": invalid_processing_observations,
            "max_invalid_orders_in_one_round": max((item["invalid_orders"] for item in invalid_rounds), default=0),
            "max_discovered_in_one_round": max((item["discovered"] for item in rounds), default=0),
            "first_invalid_at": invalid_rounds[0]["observed_at"] if invalid_rounds else None,
            "f3_plan_armed_at": armed[0].get("observed_at") if armed else None,
            "first_invalid_predates_f3_plan": predates_f3,
            "unique_order_count": len(due_orders) if isinstance(dump, dict) and dump else None,
            "dumped_source_order_count": len(orders) if isinstance(dump, dict) and dump else None,
            "unique_due_candidate_count": physical_due_count if isinstance(dump, dict) and dump else None,
            "declared_due_member_count": declared_due_count if isinstance(dump, dict) and dump else None,
            "indexed_due_member_count": len(indexed_due_refs) if isinstance(dump, dict) and dump else None,
            "mapped_due_order_count": len(due_orders) if isinstance(dump, dict) and dump else None,
            "unmatched_due_member_count": unmatched_due_count if isinstance(dump, dict) and dump else None,
            "care_due_candidate_count": len(care_due),
            "tapped_theft_operation_ids": sorted(theft_ops),
            "matching_due_order_count": len(matched),
        },
        "orders": order_rows,
        "rounds": rounds,
        "evidence_limit": None if isinstance(dump, dict) and dump else "ephemeral Redis was correctly destroyed before a per-order dump existed; identity-level retroactive correlation is impossible",
        "generated_at": utc_now(),
    }


def cmd_triage_invalid_orders(args: argparse.Namespace) -> int:
    result = analyze_invalid_orders(pathlib.Path(args.evidence_dir), args.proxy_log_utc_offset_hours)
    atomic_json(args.out, sanitize(result))
    print(result["classification"])
    return 0


def cmd_ingest_correctness(args: argparse.Namespace) -> int:
    root = pathlib.Path(args.evidence_dir)
    verdict_path = root / "verdict.json"
    data = read_json(verdict_path) if verdict_path.exists() else {"verdict": "INCONCLUSIVE", "reason": "verdict.json missing"}
    browser = read_json(root / "browser-verdict.json") if (root / "browser-verdict.json").exists() else {}
    protocol = read_json(root / "protocol-verdict.json") if (root / "protocol-verdict.json").exists() else {}
    raw = str(data.get("verdict", "INCONCLUSIVE")).upper()
    triage_path = root / "f3-fault-chain-triage.json"
    triage = read_json(triage_path) if triage_path.exists() else {}
    triage_class = str(triage.get("classification", ""))
    invalid_path = root / "theft-invalid-order-triage.json"
    invalid_triage = read_json(invalid_path) if invalid_path.exists() else {}
    invalid_class = str(invalid_triage.get("classification", ""))
    invalid_product_bug = invalid_class == "PRODUCT_WORKER_RECOVERY_SCOPE_FILTER_BUG"
    invalid_observations = invalid_triage.get("observations", {})
    invalid_due_count = int(invalid_observations.get("unique_due_candidate_count", 0) or 0)
    invalid_processing_count = int(invalid_observations.get("invalid_processing_observations", 0) or 0)
    invalid_violation = invalid_due_count > 0 or invalid_processing_count > 0
    f3_blocked = (
        browser.get("verdict") == "FAIL" and protocol.get("verdict") in ({}, "PASS") and
        triage_class in F3_HARNESS_OR_CONTRACT_CLASSIFICATIONS
    )
    if invalid_violation:
        status, failure_class = "FAIL", "correctness"
    elif raw == "PASS" and browser.get("verdict") in ({}, "PASS") and protocol.get("verdict") in ({}, "PASS"):
        status, failure_class = "PASS", "none"
    elif f3_blocked:
        status, failure_class = "INCONCLUSIVE", "fault_harness"
    elif raw == "INCONCLUSIVE":
        status, failure_class = "INCONCLUSIVE", "environment"
    else:
        status, failure_class = "FAIL", "correctness"
    failed_browser_steps = 0 if browser.get("verdict") in ({}, "PASS") else 1
    failed_protocol_steps = sum(1 for step in protocol.get("steps", {}).values() if step.get("status") != "ok")
    correctness_signals = {
        "combined_anchor_failure": 0 if raw == "PASS" else (1 if raw == "FAIL" else 0),
        "browser_witness_failure": failed_browser_steps,
        "protocol_bot_step_failure": failed_protocol_steps,
        "persistent_invalid_pair_due_candidates": invalid_due_count,
        "worker_invalid_order_processing_observations": invalid_processing_count,
    }
    incidents = []
    if invalid_violation:
        correlated = []
        if invalid_due_count:
            correlated.append("persistent_invalid_pair_due_candidates")
        if invalid_processing_count:
            correlated.append("worker_invalid_order_processing_observations")
        incidents.append({
            "incident_id": "care-pair-due-scope-filter-1" if invalid_product_bug else "pair-recovery-invariant-1",
            "severity": "HARD",
            "classification": invalid_class or "PAIR_RECOVERY_INVARIANT_VIOLATION",
            "correlated_signals": correlated,
        })
    elif status == "FAIL":
        incidents.append({
            "incident_id": "product-correctness-anchor-1",
            "severity": "HARD",
            "classification": triage_class or "UNCLASSIFIED_CORRECTNESS_FAILURE",
            "correlated_signals": [name for name, count in correctness_signals.items() if count],
        })
    invariants = {
        "hard_correctness_incident_total": len(incidents),
        "persistent_pair_due_residue_total": invalid_due_count,
        "worker_invalid_order_processing_total": invalid_processing_count,
    }
    reason = str(data.get("reason", "correctness anchor complete"))
    if invalid_product_bug:
        reason = "F1 CARE passed at the H5, but its two Pair DueIndex orders remain pending and are repeatedly misclassified by Theft recovery"
    elif invalid_violation:
        reason = f"Pair recovery invariant violation: invalid_processing={invalid_processing_count}, persistent_due={invalid_due_count}"
    elif f3_blocked:
        reason = f"F3 evidence chain blocked ({triage_class}); product correctness attribution is not proven"
    result = {
        "schema": SCHEMA,
        "name": args.name,
        "profile": "product_correctness_anchor",
        "status": status,
        "failure_class": failure_class,
        "reason": reason,
        "baseline_state": "BASELINE_BLOCKED_BY_PRODUCT_CORRECTNESS" if invalid_violation else (BASELINE_BLOCKED_BY_UNCLASSIFIED_F3 if f3_blocked else None),
        "fault_chain_classification": triage_class or None,
        "invalid_order_classification": invalid_class or None,
        "planned": {"playwright_h5_witnesses": 2, "go_protocol_bots": 1, "faults": ["F1", "F2", "F3", "lost_claim_result"]},
        "actual": {
            "playwright_verdict": browser.get("verdict", "PENDING"),
            "protocol_verdict": protocol.get("verdict", "PENDING"),
            "fault_chain_classification": triage_class or "NOT_AVAILABLE",
            "invalid_order_classification": invalid_class or "NOT_AVAILABLE",
            "persistent_invalid_pair_due_candidates": invalid_due_count,
            "worker_invalid_order_processing_observations": invalid_processing_count,
        },
        "metrics": {"browser_steps": browser.get("steps", []), "protocol_steps": protocol.get("steps", {})},
        "correctness_invariants": invariants,
        "correctness_signals": correctness_signals,
        "correctness_incidents": incidents,
        "namespace": args.namespace,
        "redis_endpoint_hash": args.redis_endpoint_hash,
        "evidence": [str(path.relative_to(root)) for path in root.rglob("*") if path.is_file() and path.stat().st_size < 10_000_000],
        "finished_at": utc_now(),
    }
    atomic_json(args.out, sanitize(result))
    return 0 if status == "PASS" else (1 if status == "FAIL" else 2)


def cmd_ingest_theft(args: argparse.Namespace) -> int:
    root = pathlib.Path(args.evidence_dir)
    drivers = sorted(root.glob("theft-200-wss-*-driver.json"))
    audits = sorted(root.glob("theft-200-wss-*-audit.json"))
    if not drivers or not audits:
        status, failure_class, reason = "INCONCLUSIVE", "environment", "theft driver/audit evidence missing"
    else:
        status, failure_class, reason = "PASS", "none", "200-WSS baseline and slow-reader audits passed"
    driver_data = [read_json(path) for path in drivers]
    audit_data = [read_json(path) for path in audits]
    invariant_counts = {
        "duplicate_business_effect_total": 0,
        "farm_version_gap_total": 0,
        "farm_version_regression_total": 0,
        "workflow_residue_total": 0,
        "due_residue_total": 0,
        "asset_conservation_violation_total": 0,
        "participant_receipt_mismatch_total": 0,
        "unexpected_healthy_disconnect_total": 0,
    }
    for driver in driver_data:
        theft = driver.get("theft_hot", {})
        invariant_counts["farm_version_gap_total"] += int(theft.get("farm_version_gaps", 0))
        invariant_counts["farm_version_regression_total"] += int(theft.get("farm_version_regressions", 0))
        invariant_counts["unexpected_healthy_disconnect_total"] += int(driver.get("unexpected_disconnects", 0))
        if int(theft.get("terminal_errors", 0)) or int(theft.get("same_operation_retry_errors", 0)):
            invariant_counts["duplicate_business_effect_total"] += int(theft.get("terminal_errors", 0)) + int(theft.get("same_operation_retry_errors", 0))
    for audit in audit_data:
        invariant_counts["workflow_residue_total"] += int(audit.get("overdue_nonterminal_workflows", 0))
        invariant_counts["participant_receipt_mismatch_total"] += int(audit.get("participant_receipt_mismatches", 0))
        invariant_counts["unexpected_healthy_disconnect_total"] += int(audit.get("unexpected_healthy_disconnects", 0))
        false_checks = [name for name, passed in audit.get("checks", {}).items() if not passed]
        invariant_counts["asset_conservation_violation_total"] += len(false_checks)
    violations = sum(invariant_counts.values())
    if violations:
        status, failure_class, reason = "FAIL", "correctness", f"theft correctness invariants violated: {violations}"
    elif len(drivers) < args.expected_cases or len(audits) < args.expected_cases:
        status, failure_class, reason = "INCONCLUSIVE", "environment", "not all planned theft cases produced evidence"
    metrics = {
        "cases": len(driver_data),
        "planned_connections": args.expected_cases * 200,
        "actual_injected_connections": sum(int(data.get("attempted", 0)) for data in driver_data),
        "ready_connections": sum(int(data.get("ready", 0)) for data in driver_data),
        "terminal_succeeded": sum(int(data.get("theft_hot", {}).get("terminal_succeeded", 0)) for data in driver_data),
        "terminal_no_change": sum(int(data.get("theft_hot", {}).get("terminal_no_change", 0)) for data in driver_data),
        "terminal_rejected": sum(int(data.get("theft_hot", {}).get("terminal_rejected", 0)) for data in driver_data),
        "processing_observations": sum(int(data.get("theft_hot", {}).get("operation_result_processing", 0)) for data in driver_data),
        "p95_admission_ms_max": max((float(data.get("theft_hot", {}).get("theft_admission_p95_ms", 0)) for data in driver_data), default=0),
        "p99_admission_ms_max": max((float(data.get("theft_hot", {}).get("theft_admission_p99_ms", 0)) for data in driver_data), default=0),
        "p95_terminal_ms_max": max((float(data.get("theft_hot", {}).get("theft_terminal_p95_ms", 0)) for data in driver_data), default=0),
        "p99_terminal_ms_max": max((float(data.get("theft_hot", {}).get("theft_terminal_p99_ms", 0)) for data in driver_data), default=0),
        "driver_reports": driver_data,
        "authority_audits": audit_data,
    }
    result = {
        "schema": SCHEMA,
        "name": args.name,
        "profile": "hot_farm_theft",
        "status": status,
        "failure_class": failure_class,
        "reason": reason,
        "planned": {"cases": args.expected_cases, "connections_per_case": 200, "slow_reader_cases": 1},
        "actual": {"driver_cases": len(driver_data), "audit_cases": len(audit_data), "connections": metrics["actual_injected_connections"]},
        "metrics": metrics,
        "correctness_invariants": invariant_counts,
        "redis_endpoint_hash": args.redis_endpoint_hash,
        "namespace": args.namespace,
        "evidence": [str(path.relative_to(root)) for path in root.iterdir() if path.is_file()],
        "finished_at": utc_now(),
    }
    atomic_json(args.out, sanitize(result))
    return 0 if status == "PASS" else (1 if status == "FAIL" else 2)


def flatten_numeric(prefix: str, value: Any, out: dict[str, float]) -> None:
    if isinstance(value, bool):
        return
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        out[prefix] = float(value)
    elif isinstance(value, dict):
        for key, child in value.items():
            flatten_numeric(f"{prefix}.{key}" if prefix else str(key), child, out)


def aggregate_scenarios(scenarios: list[dict[str, Any]]) -> dict[str, Any]:
    profile_metrics: dict[str, Any] = {}
    total_planned = total_actual = total_completed = total_drop = 0
    for scenario in scenarios:
        actual = scenario.get("actual", {})
        planned = scenario.get("planned", {})
        planned_n = int(planned.get("arrivals", planned.get("connections_per_case", 0) * planned.get("cases", 0)) or 0)
        actual_n = int(actual.get("injected", actual.get("connections", 0)) or 0)
        completed = int(actual.get("completed", actual.get("connections", 0)) or 0)
        drop = int(actual.get("schedule_drop", max(0, planned_n - actual_n)) or 0)
        total_planned += planned_n
        total_actual += actual_n
        total_completed += completed
        total_drop += drop
        profile_metrics[scenario.get("name", "unnamed")] = {
            "profile": scenario.get("profile"),
            "status": scenario.get("status"),
            "planned": planned,
            "actual": actual,
            "metrics": scenario.get("metrics", {}),
            "correctness_invariants": scenario.get("correctness_invariants", {}),
            "correctness_signals": scenario.get("correctness_signals", {}),
            "correctness_incidents": scenario.get("correctness_incidents", []),
            "baseline_state": scenario.get("baseline_state"),
            "fault_chain_classification": scenario.get("fault_chain_classification"),
        }
    return {
        "schema": SCHEMA,
        "profiles": profile_metrics,
        "load_totals": {
            "planned_arrivals": total_planned,
            "actual_injected_arrivals": total_actual,
            "completed": total_completed,
            "schedule_drop": total_drop,
            "actual_to_planned_ratio": (total_actual / total_planned) if total_planned else None,
        },
    }


def default_observability_gaps(scenarios: list[dict[str, Any]]) -> list[dict[str, str]]:
    gaps = [
        {"metric": "actor_record_bytes", "status": "PENDING_OBSERVABILITY", "owner": "A3", "impact": "record-size percentiles cannot be compared yet"},
        {"metric": "commit_bytes", "status": "PENDING_OBSERVABILITY", "owner": "A3", "impact": "Redis request/response write amplification is not yet measured"},
        {"metric": "open_arrival_lag_ms", "status": "PENDING_OBSERVABILITY", "owner": "Q1", "impact": "planned and attempted counts are exact; per-arrival scheduler lag is not emitted by ws-stress"},
        {"metric": "connection_writer_queue_depth", "status": "PENDING_OBSERVABILITY", "owner": "Gate", "impact": "queue rejection/kick counters exist but queue depth is not exported"},
        {"metric": "redis_session_route_qps", "status": "PENDING_OBSERVABILITY", "owner": "Gate/A4", "impact": "Redis aggregate commandstats cannot attribute SessionRoute exactly"},
        {"metric": "redis_subscription_lease_qps", "status": "PENDING_OBSERVABILITY", "owner": "A4", "impact": "lease counters exist; exact Redis QPS attribution is pending"},
        {"metric": "mixed_open_arrival_operations", "status": "PARTIAL", "owner": "Q1 + scenario owners", "impact": "open-arrival mix drives login/Join plus authority-confirmed Buy/Plant protocol journeys; Hero and hot-farm anchors cover SELL/WATER/THEFT/CLAIM_TASK/Friend paths as closed correctness witnesses"},
        {"metric": "safe_connections_per_gate", "status": "NOT_FROZEN", "owner": "Q1", "impact": "first-run staircase records a growth curve only"},
        {"metric": "supports_30m_dau", "status": "PROHIBITED_CLAIM", "owner": "Q1", "impact": "this baseline is not final capacity proof"},
    ]
    if not any(s.get("profile") == "product_correctness_anchor" and s.get("status") == "PASS" for s in scenarios):
        gaps.append({"metric": "playwright_h5_witness", "status": "MISSING_OR_FAILED", "owner": "Q1", "impact": "no passing real-H5 correctness anchor in this run"})
    return gaps


def compare_candidate(metrics: dict[str, Any], baseline_path: str, threshold: float) -> tuple[bool, list[str]]:
    baseline = read_json(baseline_path)
    current_flat: dict[str, float] = {}
    baseline_flat: dict[str, float] = {}
    flatten_numeric("", metrics.get("profiles", {}), current_flat)
    flatten_numeric("", baseline.get("profiles", {}), baseline_flat)
    reasons: list[str] = []
    latency_suffixes = ("p95_ms", "p99_ms", "scenario_p95_ms", "p95_admission_ms_max", "p99_admission_ms_max", "p95_terminal_ms_max", "p99_terminal_ms_max")
    compared = 0
    for key, old in baseline_flat.items():
        if not key.endswith(latency_suffixes) or old <= 0 or key not in current_flat:
            continue
        compared += 1
        new = current_flat[key]
        if new > old * (1 + threshold):
            reasons.append(f"{key}: {new:.3f} > baseline {old:.3f} * {1 + threshold:.3f}")
    if compared == 0:
        reasons.append("no comparable latency metrics found")
        return False, reasons
    return not reasons, reasons


def report_markdown(manifest: dict[str, Any], scenarios: list[dict[str, Any]], verdict: dict[str, Any], gaps: list[dict[str, str]]) -> str:
    if verdict["verdict"] == BASELINE_RECORDED:
        boundary_note = "This run records a repeatable pre-hardening baseline."
    elif verdict["verdict"] == PERF_REGRESSION_PASS:
        boundary_note = "This run records a candidate comparison against the frozen baseline."
    elif verdict.get("baseline_state") == BASELINE_BLOCKED_BY_UNCLASSIFIED_F3:
        boundary_note = "No performance baseline was recorded: F3 stopped pressure, but the retained chain does not yet justify product attribution."
    elif verdict["verdict"] == FAIL_CORRECTNESS:
        boundary_note = "Baseline performance was not recorded: the run stopped at the correctness hard gate before pressure profiles."
    else:
        boundary_note = "No baseline conclusion was recorded because the run was environmentally inconclusive."
    lines = [
        f"# {TASK_ID} · Performance Baseline Report",
        "",
        f"- **Verdict:** `{verdict['verdict']}` (`{verdict['classification']}`)",
        f"- **Baseline state:** `{verdict.get('baseline_state', 'NOT_RECORDED')}`",
        f"- **Run:** `{manifest['run_id']}`",
        f"- **Window:** `{manifest['test_window']['started_at']}` → `{manifest['test_window']['finished_at']}`",
        f"- **Backend:** `{manifest['backend_commit']}`",
        f"- **H5:** `{manifest['h5_commit']}`",
        f"- **Load tool:** `{manifest['load_tool_commit']}`",
        f"- **Config hash:** `{manifest['config_hash']}`",
        f"- **Redis:** `{manifest['redis']['mode']}`, endpoint `{manifest['redis']['endpoint_hash']}`, namespace `{manifest['redis']['namespace']}`",
        f"- **Machine:** `{manifest['host']['cpu_model']}`, {manifest['host']['logical_cpu_count']} logical CPUs, {manifest['host']['memory_total_bytes']} bytes RAM, spec `{manifest['host']['machine_spec_hash']}`",
        "",
        f"> {boundary_note} It is **not** evidence that the system supports 30 million DAU.",
        "",
        "## Profile results",
        "",
        "| Scenario | Profile | Status | Planned | Actual | Reason |",
        "|---|---|---:|---:|---:|---|",
    ]
    for scenario in scenarios:
        planned = scenario.get("planned", {})
        actual = scenario.get("actual", {})
        planned_n = planned.get("arrivals", planned.get("connections_per_case", "—"))
        actual_n = actual.get("injected", actual.get("connections", "—"))
        reason = str(scenario.get("reason", "")).replace("|", "\\|")
        lines.append(f"| {scenario.get('name')} | {scenario.get('profile')} | `{scenario.get('status')}` | {planned_n} | {actual_n} | {reason} |")
    lines.extend(["", "## Planned versus actual load", ""])
    for scenario in scenarios:
        if "arrivals" not in scenario.get("planned", {}):
            continue
        p, a = scenario["planned"], scenario.get("actual", {})
        lines.append(
            f"- **{scenario.get('name')}**: planned `{p.get('arrivals')}` at `{p.get('qps')}/s`; "
            f"attempted `{a.get('injected')}`, Ready `{a.get('ready')}`, completed `{a.get('completed')}`, schedule drop `{a.get('schedule_drop')}`."
        )
    lines.extend(["", "## Correctness invariants", ""])
    invariant_total = 0
    for scenario in scenarios:
        invariants = scenario.get("correctness_invariants", {})
        incidents = scenario.get("correctness_incidents", [])
        if incidents:
            count = len({item.get("incident_id") for item in incidents if item.get("severity") == "HARD"})
        else:
            hard = {key: value for key, value in invariants.items() if isinstance(value, int) and value != 0 and "missing" not in key}
            count = sum(value for value in hard.values() if value > 0)
        invariant_total += count
        lines.append(f"- **{scenario.get('name')} incidents**: `{json.dumps(incidents, ensure_ascii=False, sort_keys=True)}`")
        lines.append(f"- **{scenario.get('name')} signals**: `{json.dumps(scenario.get('correctness_signals', {}), ensure_ascii=False, sort_keys=True)}`")
    lines.append(f"- **Unique hard correctness incidents:** `{invariant_total}`")
    lines.extend(["", "## Resource and latency evidence", ""])
    for scenario in scenarios:
        metrics = scenario.get("metrics", {})
        selected = {key: metrics[key] for key in (
            "planned_qps", "actual_injected_qps", "completed_qps", "p50_ms", "p95_ms", "p99_ms",
            "scenario_p95_ms", "peak_connections", "p95_admission_ms_max", "p99_admission_ms_max",
            "p95_terminal_ms_max", "p99_terminal_ms_max",
        ) if key in metrics}
        if selected:
            lines.append(f"- **{scenario.get('name')}**: `{json.dumps(selected, ensure_ascii=False, sort_keys=True)}`")
    lines.extend(["", "Raw CPU/RSS/goroutine/fd/GC/network samples are retained in each scenario evidence directory.", "", "## Observability gaps", ""])
    for gap in gaps:
        lines.append(f"- `{gap['metric']}` — **{gap['status']}** ({gap['owner']}): {gap['impact']}")
    lines.extend(["", "## Evidence boundary", "", "- Go Protocol Bots carry connection and workload pressure; Playwright is limited to real-page witnesses.", "- Redis secrets are not recorded and `FLUSHALL` is prohibited.", "- Every local Redis target is an exact ephemeral container; cleanup destroys that container rather than touching unrelated data.", "- Capacity SLOs and `safe_connections_per_gate` remain unfrozen until larger isolated staircases and soak runs complete.", ""])
    return "\n".join(lines)


def cmd_finalize(args: argparse.Namespace) -> int:
    out = pathlib.Path(args.out)
    manifest_path = out / "baseline-run-manifest.json"
    manifest = read_json(manifest_path)
    scenarios = [read_json(path) for path in sorted((out / "scenarios").glob("*.json"))]
    metrics = aggregate_scenarios(scenarios)
    hard_incident_ids: set[str] = set()
    legacy_hard_violations = 0
    correctness_signal_count = 0
    for scenario in scenarios:
        incidents = scenario.get("correctness_incidents")
        if isinstance(incidents, list):
            for incident in incidents:
                if incident.get("severity") == "HARD" and incident.get("incident_id"):
                    hard_incident_ids.add(str(incident["incident_id"]))
        else:
            for key, value in scenario.get("correctness_invariants", {}).items():
                if not isinstance(value, int) or value <= 0 or "missing" in key:
                    continue
                legacy_hard_violations += value
        correctness_signal_count += sum(
            value for value in scenario.get("correctness_signals", {}).values()
            if isinstance(value, int) and value > 0
        )
    hard_violations = len(hard_incident_ids) + legacy_hard_violations
    failed_correctness = any(s.get("status") == "FAIL" and s.get("failure_class") == "correctness" for s in scenarios)
    inconclusive = any(s.get("status") == "INCONCLUSIVE" for s in scenarios)
    blocked_f3 = any(s.get("baseline_state") == BASELINE_BLOCKED_BY_UNCLASSIFIED_F3 for s in scenarios)
    missing_profiles = args.expected_scenarios - len(scenarios)
    reasons: list[str] = []
    comparison: dict[str, Any] | None = None
    baseline_state = "NOT_RECORDED"
    if hard_violations or failed_correctness:
        verdict, classification = FAIL_CORRECTNESS, "FAIL"
        baseline_state = "NOT_RECORDED_CORRECTNESS"
        reasons.append(f"unique hard correctness incident count={hard_violations}")
        failed_names = [s.get("name", "unnamed") for s in scenarios if s.get("status") == "FAIL"]
        if failed_names:
            reasons.append("failed correctness scenarios=" + ",".join(failed_names))
        reasons.append("remaining pressure profiles were not executed because the correctness stop condition fired")
    elif blocked_f3:
        verdict, classification = INCONCLUSIVE_ENVIRONMENT, "INCONCLUSIVE"
        baseline_state = BASELINE_BLOCKED_BY_UNCLASSIFIED_F3
        reasons.append("F3 stopped the run, but the evidence chain does not prove a product correctness defect")
        reasons.append("remaining pressure profiles were correctly not executed")
    elif inconclusive or missing_profiles > 0:
        verdict, classification = INCONCLUSIVE_ENVIRONMENT, "INCONCLUSIVE"
        if inconclusive:
            reasons.append("one or more scenarios were inconclusive")
        if missing_profiles > 0:
            reasons.append(f"missing scenario results={missing_profiles}")
    elif args.run_kind == "baseline" and not manifest.get("baseline_eligible", True):
        verdict, classification = INCONCLUSIVE_ENVIRONMENT, "INCONCLUSIVE"
        reasons.append("tooling smoke config is explicitly not eligible for Baseline V1 publication")
    elif args.run_kind == "candidate":
        if not args.baseline_metrics:
            verdict, classification = INCONCLUSIVE_ENVIRONMENT, "INCONCLUSIVE"
            reasons.append("candidate run has no baseline metrics")
        else:
            passed, comparison_reasons = compare_candidate(metrics, args.baseline_metrics, args.regression_threshold)
            comparison = {"baseline_metrics": args.baseline_metrics, "regression_threshold": args.regression_threshold, "passed": passed, "reasons": comparison_reasons}
            if passed:
                verdict, classification = PERF_REGRESSION_PASS, "PASS"
                baseline_state = "CANDIDATE_COMPARISON_RECORDED"
                reasons.append("correctness passed and comparable target metrics did not regress")
            else:
                verdict, classification = INCONCLUSIVE_ENVIRONMENT, "INCONCLUSIVE"
                reasons.extend(comparison_reasons)
    else:
        verdict, classification = BASELINE_RECORDED, "PASS"
        baseline_state = BASELINE_RECORDED
        reasons.append("all configured baseline profiles completed with zero hard correctness incidents")
    gaps = default_observability_gaps(scenarios)
    # Re-reporting retained evidence must not move the measured test window.
    # The first finalize freezes it; later format-only regeneration preserves it.
    completion_time = manifest.get("finished_at") or utc_now()
    verdict_data = {
        "schema": SCHEMA,
        "task_id": TASK_ID,
        "run_id": manifest["run_id"],
        "verdict": verdict,
        "classification": classification,
        "baseline_state": baseline_state,
        "reasons": reasons,
        "hard_correctness_invariant_count": hard_violations,
        "hard_correctness_incident_count": hard_violations,
        "correctness_signal_count": correctness_signal_count,
        "scenario_statuses": {s.get("name", "unnamed"): s.get("status") for s in scenarios},
        "comparison": comparison,
        "supports_30m_dau": False,
        "finished_at": completion_time,
    }
    manifest["finished_at"] = completion_time
    manifest["test_window"]["finished_at"] = manifest["test_window"].get("finished_at") or completion_time
    manifest["actual_load"] = metrics["load_totals"]
    manifest["redis"]["cleanup_completed"] = bool(args.cleanup_completed)
    atomic_json(manifest_path, sanitize(manifest))
    atomic_json(out / "baseline-verdict.json", verdict_data)
    atomic_json(out / "baseline-metrics.json", sanitize(metrics))
    atomic_json(out / "observability-gaps.json", {"schema": SCHEMA, "gaps": gaps})
    (out / "performance-baseline-report.md").write_text(report_markdown(manifest, scenarios, verdict_data, gaps), encoding="utf-8")
    return 0 if classification == "PASS" else (1 if classification == "FAIL" else 2)


def cmd_record_status(args: argparse.Namespace) -> int:
    result = {
        "schema": SCHEMA,
        "name": args.name,
        "profile": args.profile,
        "status": args.status,
        "failure_class": args.failure_class,
        "reason": args.reason,
        "planned": {"arrivals": args.planned} if args.planned is not None else {},
        "actual": {"injected": 0, "ready": 0, "completed": 0, "schedule_drop": args.planned or 0},
        "metrics": {},
        "correctness_invariants": {},
        "redis_endpoint_hash": args.redis_endpoint_hash,
        "namespace": args.namespace,
        "evidence": args.evidence,
        "finished_at": utc_now(),
    }
    atomic_json(args.out, sanitize(result))
    return 0 if args.status == "PASS" else (1 if args.status == "FAIL" else 2)


def cmd_endpoint_hash(args: argparse.Namespace) -> int:
    _, digest = redis_target_identity(args.target)
    print(digest)
    return 0


def cmd_list_stages(args: argparse.Namespace) -> int:
    config = read_json(args.config)
    body = config.get("profiles", {}).get(args.profile, {})
    for stage in body.get("stages", []):
        if stage.get("enabled", True):
            print(json.dumps(stage, ensure_ascii=False, sort_keys=True))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    manifest = sub.add_parser("manifest")
    manifest.add_argument("--config", required=True)
    manifest.add_argument("--out", required=True)
    manifest.add_argument("--run-id", required=True)
    manifest.add_argument("--run-kind", choices=("baseline", "candidate"), default="baseline")
    manifest.add_argument("--backend-root", required=True)
    manifest.add_argument("--backend-commit", required=True)
    manifest.add_argument("--h5-root", required=True)
    manifest.add_argument("--h5-commit", required=True)
    manifest.add_argument("--load-root", required=True)
    manifest.add_argument("--redis-target", required=True)
    manifest.add_argument("--redis-mode", choices=("ephemeral_local", "shared_namespaced"), required=True)
    manifest.add_argument("--namespace", required=True)
    manifest.add_argument("--comparison-baseline", default="")
    manifest.set_defaults(func=cmd_manifest)

    arrivals = sub.add_parser("run-bot-arrivals")
    arrivals.add_argument("--bot", required=True)
    arrivals.add_argument("--http-url", required=True)
    arrivals.add_argument("--ws-url", required=True)
    arrivals.add_argument("--arrivals", type=int, required=True)
    arrivals.add_argument("--rate", type=float, required=True)
    arrivals.add_argument("--max-arrival-lag-ms", type=float, default=250.0)
    arrivals.add_argument("--max-concurrency", type=int, default=128)
    arrivals.add_argument("--completion-timeout", type=float, default=120.0)
    arrivals.add_argument("--user-prefix", required=True)
    arrivals.add_argument("--password", default="smoke123")
    arrivals.add_argument("--bot-extra-arg", action="append", default=[])
    arrivals.add_argument("--evidence-dir", required=True)
    arrivals.add_argument("--out", required=True)
    arrivals.set_defaults(func=cmd_run_bot_arrivals)

    sample = sub.add_parser("sample")
    sample.add_argument("--out", required=True)
    sample.add_argument("--stop-file", required=True)
    sample.add_argument("--interval", type=float, default=1.0)
    sample.add_argument("--max-seconds", type=float, default=3600)
    sample.add_argument("--process", action="append", default=[])
    sample.add_argument("--metrics", action="append", default=[])
    sample.set_defaults(func=cmd_sample)

    ws = sub.add_parser("ingest-ws")
    ws.add_argument("--input", action="append", required=True)
    ws.add_argument("--samples", action="append", default=[])
    ws.add_argument("--out", required=True)
    ws.add_argument("--name", required=True)
    ws.add_argument("--profile", required=True)
    ws.add_argument("--planned", type=int)
    ws.add_argument("--namespace", required=True)
    ws.add_argument("--redis-endpoint-hash", required=True)
    ws.add_argument("--load-generator-cpu-stop-percent", type=float, default=85.0)
    ws.set_defaults(func=cmd_ingest_ws)

    watch_fault = sub.add_parser("watch-fault")
    watch_fault.add_argument("--admin-url", required=True)
    watch_fault.add_argument("--out", required=True)
    watch_fault.add_argument("--stop-file", required=True)
    watch_fault.add_argument("--interval", type=float, default=0.05)
    watch_fault.add_argument("--max-seconds", type=float, default=300.0)
    watch_fault.set_defaults(func=cmd_watch_fault)

    dump_theft = sub.add_parser("dump-redis-theft")
    dump_theft.add_argument("--host", default="127.0.0.1")
    dump_theft.add_argument("--port", type=int, required=True)
    dump_theft.add_argument("--db", type=int, default=0)
    dump_theft.add_argument("--namespace", required=True)
    dump_theft.add_argument("--workflow-prefix", required=True)
    dump_theft.add_argument("--target-binding-prefix", default="farm:v121:m1:theft-target")
    dump_theft.add_argument("--max-keys", type=int, default=500)
    dump_theft.add_argument("--out", required=True)
    dump_theft.set_defaults(func=cmd_dump_redis_theft)

    triage_f3 = sub.add_parser("triage-f3")
    triage_f3.add_argument("--evidence-dir", required=True)
    triage_f3.add_argument("--out", required=True)
    triage_f3.add_argument("--proxy-log-utc-offset-hours", type=float, default=8.0)
    triage_f3.set_defaults(func=cmd_triage_f3)

    triage_orders = sub.add_parser("triage-invalid-orders")
    triage_orders.add_argument("--evidence-dir", required=True)
    triage_orders.add_argument("--out", required=True)
    triage_orders.add_argument("--proxy-log-utc-offset-hours", type=float, default=8.0)
    triage_orders.set_defaults(func=cmd_triage_invalid_orders)

    correctness = sub.add_parser("ingest-correctness")
    correctness.add_argument("--evidence-dir", required=True)
    correctness.add_argument("--out", required=True)
    correctness.add_argument("--name", default="product-correctness-anchor")
    correctness.add_argument("--namespace", required=True)
    correctness.add_argument("--redis-endpoint-hash", required=True)
    correctness.set_defaults(func=cmd_ingest_correctness)

    theft = sub.add_parser("ingest-theft")
    theft.add_argument("--evidence-dir", required=True)
    theft.add_argument("--out", required=True)
    theft.add_argument("--name", default="hot-farm-theft-baseline-and-slow-reader")
    theft.add_argument("--expected-cases", type=int, default=2)
    theft.add_argument("--namespace", required=True)
    theft.add_argument("--redis-endpoint-hash", required=True)
    theft.set_defaults(func=cmd_ingest_theft)

    finalize = sub.add_parser("finalize")
    finalize.add_argument("--out", required=True)
    finalize.add_argument("--run-kind", choices=("baseline", "candidate"), default="baseline")
    finalize.add_argument("--expected-scenarios", type=int, required=True)
    finalize.add_argument("--cleanup-completed", action="store_true")
    finalize.add_argument("--baseline-metrics", default="")
    finalize.add_argument("--regression-threshold", type=float, default=0.10)
    finalize.set_defaults(func=cmd_finalize)

    record = sub.add_parser("record-status")
    record.add_argument("--out", required=True)
    record.add_argument("--name", required=True)
    record.add_argument("--profile", required=True)
    record.add_argument("--status", choices=("PASS", "FAIL", "INCONCLUSIVE"), required=True)
    record.add_argument("--failure-class", choices=("none", "correctness", "environment"), required=True)
    record.add_argument("--reason", required=True)
    record.add_argument("--planned", type=int)
    record.add_argument("--namespace", required=True)
    record.add_argument("--redis-endpoint-hash", required=True)
    record.add_argument("--evidence", action="append", default=[])
    record.set_defaults(func=cmd_record_status)

    endpoint = sub.add_parser("endpoint-hash")
    endpoint.add_argument("--target", required=True)
    endpoint.set_defaults(func=cmd_endpoint_hash)

    stages = sub.add_parser("list-stages")
    stages.add_argument("--config", required=True)
    stages.add_argument("--profile", required=True)
    stages.set_defaults(func=cmd_list_stages)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return int(args.func(args))
    except (ValueError, RuntimeError, OSError, json.JSONDecodeError) as exc:
        print(f"q1perf: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
