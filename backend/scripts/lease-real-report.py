#!/usr/bin/env python3
"""Build machine-checkable R1-Q1 real lease evidence from raw artifacts."""

from __future__ import annotations

import argparse
import json
import math
import re
from pathlib import Path
from typing import Any


SAMPLE_RE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})?\s+([^\s]+)(?:\s+\d+)?$")
LABEL_RE = re.compile(r'(\w+)="((?:\\.|[^"\\])*)"(?:,|$)')


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain an object")
    return value


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def parse_prom(path: Path) -> dict[tuple[str, tuple[tuple[str, str], ...]], float]:
    result: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        if not raw or raw.startswith("#"):
            continue
        match = SAMPLE_RE.match(raw)
        if not match:
            continue
        name, encoded_labels, encoded_value = match.groups()
        try:
            value = float(encoded_value)
        except ValueError:
            continue
        labels: dict[str, str] = {}
        remaining = encoded_labels or ""
        offset = 0
        while offset < len(remaining):
            label = LABEL_RE.match(remaining, offset)
            if not label:
                break
            labels[label.group(1)] = bytes(label.group(2), "utf-8").decode("unicode_escape")
            offset = label.end()
        result[(name, tuple(sorted(labels.items())))] = value
    return result


def metric_sum(samples: dict, name: str, labels: dict[str, str] | None = None) -> float:
    wanted = labels or {}
    total = 0.0
    for (sample_name, encoded), value in samples.items():
        present = dict(encoded)
        if sample_name == name and all(present.get(key) == item for key, item in wanted.items()):
            total += value
    return total


def metric_value(samples: dict, name: str, labels: dict[str, str] | None = None) -> float:
    return metric_sum(samples, name, labels)


def delta(before: dict, after: dict, name: str, labels: dict[str, str] | None = None) -> float:
    return metric_sum(after, name, labels) - metric_sum(before, name, labels)


def histogram_max_upper(before: dict, after: dict, name: str, labels: dict[str, str]) -> float | None:
    count_delta = delta(before, after, name + "_count", labels)
    if count_delta <= 0:
        return None
    buckets: list[tuple[float, float]] = []
    for (sample_name, encoded), value in after.items():
        if sample_name != name + "_bucket":
            continue
        present = dict(encoded)
        if not all(present.get(key) == item for key, item in labels.items()):
            continue
        raw_upper = present.get("le", "")
        upper = math.inf if raw_upper == "+Inf" else float(raw_upper)
        before_value = before.get((sample_name, encoded), 0.0)
        buckets.append((upper, value - before_value))
    for upper, count in sorted(buckets):
        if count >= count_delta - 1e-9:
            return upper
    return None


def counter_delta(before: dict, after: dict, lane: str, outcome: str) -> int:
    return round(delta(before, after, "farm_gate_lease_batch_events_total", {"lane": lane, "outcome": outcome}))


def lane_summary(before: dict, after: dict, lane: str) -> dict[str, Any]:
    events = {outcome: counter_delta(before, after, lane, outcome) for outcome in (
        "planned", "attempted", "succeeded", "partial", "lost", "transient", "oversize"
    )}
    return {
        "events": events,
        "maximum_batch_items_upper_bound": histogram_max_upper(before, after, "farm_gate_lease_batch_items", {"lane": lane}),
        "maximum_estimated_bytes_upper_bound": histogram_max_upper(before, after, "farm_gate_lease_batch_estimated_bytes", {"lane": lane}),
        "maximum_dispatch_seconds_upper_bound": histogram_max_upper(before, after, "farm_gate_lease_batch_dispatch_duration_seconds", {"lane": lane}),
        "maximum_due_lag_seconds_upper_bound": histogram_max_upper(before, after, "farm_gate_lease_batch_due_lag_seconds", {"lane": lane}),
        "holder_isolated_batches": round(delta(before, after, "farm_gate_lease_batch_holder_isolated_total", {"lane": lane})),
        "stale_non_resurrection": round(delta(before, after, "farm_gate_lease_batch_stale_non_resurrection_total", {"lane": lane})),
    }


def percentile(values: list[int], fraction: float) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, math.ceil(len(ordered) * fraction) - 1))
    return ordered[index]


def proxy_distribution(state: dict[str, Any]) -> dict[str, Any]:
    events = state.get("events", [])
    item_values = [int(event.get("items", 0)) for event in events]
    byte_values = [int(event.get("estimated_bytes", 0)) for event in events]
    outcome: dict[str, int] = {}
    statuses: dict[str, int] = {}
    for event in events:
        key = str(event.get("outcome", "unknown"))
        outcome[key] = outcome.get(key, 0) + 1
        for status_name, count in event.get("statuses", {}).items():
            statuses[status_name] = statuses.get(status_name, 0) + int(count)
    return {
        "batches": len(events),
        "items": {"minimum": min(item_values, default=0), "p50": percentile(item_values, .5), "p95": percentile(item_values, .95), "maximum": max(item_values, default=0)},
        "estimated_bytes": {"minimum": min(byte_values, default=0), "p50": percentile(byte_values, .5), "p95": percentile(byte_values, .95), "maximum": max(byte_values, default=0)},
        "outcomes": outcome,
        "statuses": statuses,
    }


def check(report: dict[str, Any], name: str, passed: bool, detail: str) -> None:
    report["checks"][name] = bool(passed)
    if not passed:
        report["failures"].append(f"{name}: {detail}")


def unavailable_report(round_name: str, reason: str) -> dict[str, Any]:
    return {
        "schema_version": "r1-q1-real-lease-round/v1", "round": round_name,
        "status": "INCONCLUSIVE", "checks": {}, "failures": [reason],
    }


def round_a(run_dir: Path, manifest: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    required = [
        "round-a-client.json", "round-a-gate-before.prom", "round-a-gate-after.prom",
        "round-a-farm-a-before.prom", "round-a-farm-a-after.prom", "round-a-proxy-a.json",
    ]
    missing = [name for name in required if not (run_dir / name).is_file()]
    if missing:
        return unavailable_report("A", "missing artifacts: " + ", ".join(missing)), {}
    client = load_json(run_dir / "round-a-client.json")
    gate_before = parse_prom(run_dir / "round-a-gate-before.prom")
    gate_after = parse_prom(run_dir / "round-a-gate-after.prom")
    farm_before = parse_prom(run_dir / "round-a-farm-a-before.prom")
    farm_after = parse_prom(run_dir / "round-a-farm-a-after.prom")
    proxy = load_json(run_dir / "round-a-proxy-a.json")
    cfg = manifest["lease_config"]
    total = int(manifest["rounds"]["a"]["subscriptions"])
    cycles = int(manifest["rounds"]["a"]["minimum_cycles"])
    max_items = int(cfg["batch_max_items"])
    max_bytes = int(cfg["batch_max_bytes"])
    max_dispatch = float(cfg["max_dispatch_ms"]) / 1000.0
    due_waterline = float(cfg["due_lag_waterline_ms"]) / 1000.0
    deadline_waterline = int(cfg["deadline_remaining_waterline_ms"])
    lanes = {lane: lane_summary(gate_before, gate_after, lane) for lane in ("session_route", "subscription")}
    report: dict[str, Any] = {
        "schema_version": "r1-q1-real-lease-round/v1", "round": "A", "status": "PASS",
        "topology": "real Redis + real farm-gate + real player-svr + two real farm-svr (Round A targets one Holder)",
        "client": client, "lanes": lanes, "proxy": {"state": proxy, "distribution": proxy_distribution(proxy)},
        "checks": {}, "failures": [],
        "waterlines": {"minimum_cycles": cycles, "due_lag_max_ms": int(due_waterline * 1000), "deadline_remaining_min_ms": deadline_waterline},
    }
    check(report, "subscription_ready_200_of_200", client.get("subscriptions_ready") == total == 200, f"ready={client.get('subscriptions_ready')} total={total}")
    check(report, "unexpected_rejoin_zero", client.get("unexpected_rejoins") == 0, f"unexpected={client.get('unexpected_rejoins')}")
    for lane, summary in lanes.items():
        events = summary["events"]
        unresolved_at_scrape = events["attempted"] - events["succeeded"]
        check(report, f"{lane}_unresolved_at_scrape_bounded", events["attempted"] > 0 and 0 <= unresolved_at_scrape <= 1, f"attempted={events['attempted']} succeeded={events['succeeded']} unresolved={unresolved_at_scrape} cap=1")
        check(report, f"{lane}_aggregate_twenty_cycles", events["succeeded"] >= total * cycles, f"succeeded={events['succeeded']} want>={total * cycles}")
        check(report, f"{lane}_no_lost_transient_partial", events["lost"] == events["transient"] == events["partial"] == 0, str(events))
        check(report, f"{lane}_batch_items_bounded", summary["maximum_batch_items_upper_bound"] is not None and summary["maximum_batch_items_upper_bound"] <= max_items, f"upper={summary['maximum_batch_items_upper_bound']} cap={max_items}")
        check(report, f"{lane}_nonoversize_bytes_bounded", events["oversize"] == 0 and summary["maximum_estimated_bytes_upper_bound"] is not None and summary["maximum_estimated_bytes_upper_bound"] <= max_bytes, f"upper={summary['maximum_estimated_bytes_upper_bound']} cap={max_bytes} oversize={events['oversize']}")
        check(report, f"{lane}_dispatch_bounded", summary["maximum_dispatch_seconds_upper_bound"] is not None and summary["maximum_dispatch_seconds_upper_bound"] <= max_dispatch, f"upper={summary['maximum_dispatch_seconds_upper_bound']} cap={max_dispatch}")
        check(report, f"{lane}_due_lag_below_waterline", summary["maximum_due_lag_seconds_upper_bound"] is not None and summary["maximum_due_lag_seconds_upper_bound"] <= due_waterline, f"upper={summary['maximum_due_lag_seconds_upper_bound']} waterline={due_waterline}")
    check(report, "subscription_each_identity_twenty_cycles", proxy.get("renewed_identities") == total and int(proxy.get("minimum_renewals_per_identity", 0)) >= cycles, f"identities={proxy.get('renewed_identities')} min={proxy.get('minimum_renewals_per_identity')}")
    check(report, "expired_zero", round(delta(gate_before, gate_after, "farm_gate_subscription_lease_local_expired_total")) == 0 and round(delta(farm_before, farm_after, "farm_subscription_lease_expired_total")) == 0, "Gate or Hub expiration advanced")
    check(report, "fail_closed_zero", round(delta(gate_before, gate_after, "farm_gate_subscription_lease_fail_closed_total")) == 0, "fail-closed counter advanced")
    check(report, "stale_revived_zero", proxy.get("terminal_reappeared") == 0 and lanes["subscription"]["stale_non_resurrection"] == 0, f"proxy={proxy.get('terminal_reappeared')}")
    check(report, "mixed_partition_zero", proxy.get("mixed_farm_calls") == 0, f"mixed={proxy.get('mixed_farm_calls')}")
    check(report, "subscription_exact_batch_bounds", int(proxy.get("maximum_items", 0)) <= max_items and int(proxy.get("maximum_non_oversize_bytes", 0)) <= max_bytes and proxy.get("oversize_dispatched_alone") is True, "proxy exact batch observation exceeded config")
    check(report, "subscription_deadline_remaining_above_waterline", int(proxy.get("minimum_deadline_remaining_ms") or -1) >= deadline_waterline, f"minimum={proxy.get('minimum_deadline_remaining_ms')} waterline={deadline_waterline}")
    route_lower = int(cfg["route_ttl_ms"]) - int(cfg["route_safety_margin_ms"]) - int(cfg["renew_interval_ms"]) - int(cfg["max_jitter_ms"]) - int(cfg["due_lag_waterline_ms"]) - int(cfg["max_dispatch_ms"])
    report["route_deadline_remaining_derived_lower_ms"] = route_lower
    check(report, "session_route_deadline_remaining_above_waterline", route_lower >= deadline_waterline, f"derived={route_lower} waterline={deadline_waterline}")
    if report["failures"]:
        report["status"] = "FAIL"
    metrics = {"round": "A", "lanes": lanes, "gate_fail_closed_delta": round(delta(gate_before, gate_after, "farm_gate_subscription_lease_fail_closed_total")), "gate_expired_delta": round(delta(gate_before, gate_after, "farm_gate_subscription_lease_local_expired_total")), "farm_expired_delta": round(delta(farm_before, farm_after, "farm_subscription_lease_expired_total"))}
    return report, metrics


def round_b(run_dir: Path, manifest: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    required = [
        "round-b-client.json", "round-b-gate-before.prom", "round-b-gate-pre-drain.prom",
        "round-b-gate-drain-1.prom", "round-b-gate-drain-2.prom", "round-b-proxy-a.json",
        "round-b-proxy-b.json", "round-b-proxy-a-drain-1.json", "round-b-proxy-a-drain-2.json",
        "round-b-proxy-b-drain-1.json", "round-b-proxy-b-drain-2.json", "gate-exit.json",
    ]
    missing = [name for name in required if not (run_dir / name).is_file()]
    if missing:
        return unavailable_report("B", "missing artifacts: " + ", ".join(missing)), {}
    client = load_json(run_dir / "round-b-client.json")
    before = parse_prom(run_dir / "round-b-gate-before.prom")
    pre_drain = parse_prom(run_dir / "round-b-gate-pre-drain.prom")
    drain1 = parse_prom(run_dir / "round-b-gate-drain-1.prom")
    drain2 = parse_prom(run_dir / "round-b-gate-drain-2.prom")
    proxy_a = load_json(run_dir / "round-b-proxy-a.json")
    proxy_b = load_json(run_dir / "round-b-proxy-b.json")
    proxy_a_d1 = load_json(run_dir / "round-b-proxy-a-drain-1.json")
    proxy_a_d2 = load_json(run_dir / "round-b-proxy-a-drain-2.json")
    proxy_b_d1 = load_json(run_dir / "round-b-proxy-b-drain-1.json")
    proxy_b_d2 = load_json(run_dir / "round-b-proxy-b-drain-2.json")
    gate_exit = load_json(run_dir / "gate-exit.json")
    lanes = {lane: lane_summary(before, pre_drain, lane) for lane in ("session_route", "subscription")}
    report: dict[str, Any] = {
        "schema_version": "r1-q1-real-lease-round/v1", "round": "B", "status": "PASS",
        "topology": "two real Farm holders behind isolated typed test proxies + real Gate SIGTERM Drain",
        "client": client, "lanes_before_drain": lanes,
        "holders": {"faulted": {"state": proxy_a, "distribution": proxy_distribution(proxy_a)}, "healthy": {"state": proxy_b, "distribution": proxy_distribution(proxy_b)}},
        "drain": {"gate_exit": gate_exit}, "checks": {}, "failures": [],
    }
    total = int(manifest["rounds"]["b"]["subscriptions"])
    check(report, "two_holder_subscription_ready", client.get("subscriptions_ready") == total and len(client.get("partition_ready", {})) == 2 and min(client.get("partition_ready", {}).values(), default=0) > 0, str(client.get("partition_ready")))
    check(report, "partial_exact_terminal_results", int(proxy_a.get("injected_stale", 0)) == 1 and int(proxy_a.get("injected_closed", 0)) == 1, f"stale={proxy_a.get('injected_stale')} closed={proxy_a.get('injected_closed')}")
    stale_delta = round(delta(before, pre_drain, "farm_gate_subscription_lease_results_total", {"status": "STALE_FENCE"}))
    closed_delta = round(delta(before, pre_drain, "farm_gate_subscription_lease_results_total", {"status": "CLOSED"}))
    check(report, "gate_observed_stale_closed", stale_delta >= 1 and closed_delta >= 1, f"stale={stale_delta} closed={closed_delta}")
    check(report, "partial_each_terminal_client_final", client.get("rejoins") == 2 and client.get("unexpected_rejoins") == 0, f"rejoins={client.get('rejoins')} unexpected={client.get('unexpected_rejoins')}")
    check(report, "stale_closed_not_revived", proxy_a.get("terminal_reappeared") == 0, f"terminal_reappeared={proxy_a.get('terminal_reappeared')}")
    start = int(proxy_a.get("unavailable_window_started_ms", 0))
    until = int(proxy_a.get("unavailable_window_until_ms", 0))
    healthy_during = [event for event in proxy_b.get("events", []) if start <= int(event.get("at_ms", 0)) <= until and event.get("outcome") == "FORWARDED" and event.get("statuses", {}).get("SUBSCRIPTION_LEASE_RENEWED", 0) > 0]
    check(report, "temporary_holder_failure_injected", int(proxy_a.get("unavailable_calls", 0)) > 0 and until > start > 0, f"calls={proxy_a.get('unavailable_calls')} window={start}..{until}")
    check(report, "healthy_holder_continued_during_peer_failure", len(healthy_during) > 0, f"healthy batches in failure window={len(healthy_during)}")
    check(report, "failed_holder_recovered", int(proxy_a.get("first_recovery_success_ms", 0)) >= until > 0, f"recovery={proxy_a.get('first_recovery_success_ms')} until={until}")
    check(report, "mixed_partition_zero", proxy_a.get("mixed_farm_calls") == 0 and proxy_b.get("mixed_farm_calls") == 0, f"a={proxy_a.get('mixed_farm_calls')} b={proxy_b.get('mixed_farm_calls')}")
    check(report, "drain_client_queue_empty", client.get("drain_notices") == total and client.get("connections_closed") == total, f"notices={client.get('drain_notices')} closed={client.get('connections_closed')}")
    backlog_ok = all(metric_value(snapshot, "farm_gate_lease_batch_backlog", {"lane": lane}) == 0 for snapshot in (drain1, drain2) for lane in ("session_route", "subscription"))
    check(report, "drain_planner_backlog_zero", backlog_ok, "one or more drain backlog gauges non-zero")
    drain_counts = {lane: round(delta(pre_drain, drain1, "farm_gate_lease_batch_drain_total", {"lane": lane})) for lane in ("session_route", "subscription")}
    check(report, "both_planners_drained", all(value == 1 for value in drain_counts.values()), str(drain_counts))
    scheduler_stable = all(round(delta(drain1, drain2, "farm_gate_lease_batch_events_total", {"lane": lane, "outcome": outcome})) == 0 for lane in ("session_route", "subscription") for outcome in ("planned", "attempted", "succeeded", "partial", "lost", "transient"))
    proxy_stable = proxy_a_d1.get("renew_calls") == proxy_a_d2.get("renew_calls") and proxy_b_d1.get("renew_calls") == proxy_b_d2.get("renew_calls")
    check(report, "no_schedule_resurrection", scheduler_stable and proxy_stable, f"scheduler_stable={scheduler_stable} proxy_stable={proxy_stable}")
    active_zero = metric_value(drain2, "farmgate_ws_conn_active") == 0
    check(report, "goroutine_queue_no_residual", gate_exit.get("exit_code") == 0 and active_zero and client.get("connections_closed") == total, f"gate_exit={gate_exit} conn_active={metric_value(drain2, 'farmgate_ws_conn_active')}")
    if report["failures"]:
        report["status"] = "FAIL"
    metrics = {
        "round": "B", "lanes_before_drain": lanes, "stale_results_delta": stale_delta,
        "closed_results_delta": closed_delta, "drain_counts": drain_counts,
        "drain_backlog": {f"{point}_{lane}": metric_value(snapshot, "farm_gate_lease_batch_backlog", {"lane": lane}) for point, snapshot in (("first", drain1), ("second", drain2)) for lane in ("session_route", "subscription")},
        "goroutines_before_drain": metric_value(pre_drain, "go_goroutines"),
        "goroutines_during_drain": metric_value(drain2, "go_goroutines"),
        "ws_conn_active_during_drain": metric_value(drain2, "farmgate_ws_conn_active"),
    }
    return report, metrics


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--reason", default="")
    args = parser.parse_args()
    run_dir = Path(args.run_dir).resolve()
    manifest_path = run_dir / "run-manifest.json"
    if not manifest_path.is_file():
        raise SystemExit("run-manifest.json is required")
    manifest = load_json(manifest_path)
    try:
        report_a, metrics_a = round_a(run_dir, manifest)
    except Exception as exc:  # retained as explicit INCONCLUSIVE, never PASS
        report_a, metrics_a = unavailable_report("A", f"report assembly error: {exc}"), {}
    try:
        report_b, metrics_b = round_b(run_dir, manifest)
    except Exception as exc:
        report_b, metrics_b = unavailable_report("B", f"report assembly error: {exc}"), {}
    if args.reason:
        if report_a["status"] == "INCONCLUSIVE":
            report_a["failures"].append(args.reason)
        if report_b["status"] == "INCONCLUSIVE":
            report_b["failures"].append(args.reason)
    write_json(run_dir / "round-a.json", report_a)
    write_json(run_dir / "round-b.json", report_b)
    metrics = {"schema_version": "r1-q1-real-lease-metrics/v1", "round_a": metrics_a, "round_b": metrics_b}
    write_json(run_dir / "metrics-summary.json", metrics)
    statuses = [report_a["status"], report_b["status"]]
    overall = "PASS" if statuses == ["PASS", "PASS"] else ("FAIL" if "FAIL" in statuses else "INCONCLUSIVE")
    verdict = {
        "schema_version": "r1-q1-real-lease-verdict/v1", "status": overall,
        "r1_4_commit": manifest.get("r1_4_commit"), "source_head": manifest.get("source_head"),
        "round_a": report_a["status"], "round_b": report_b["status"],
        "mock_pass": False, "flushall_used": False, "docker_socket_used": False,
        "reason": args.reason,
    }
    write_json(run_dir / "verdict.json", verdict)
    print(overall)
    return 0 if overall == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
