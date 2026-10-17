#!/usr/bin/env python3
import argparse, hashlib, json, math, pathlib, statistics, sys


def load(path):
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def gate_total(gates):
    return sum(int(value) for value in gates.values())


def cv(values):
    if not values or statistics.mean(values) == 0:
        return 0.0
    return statistics.pstdev(values) / statistics.mean(values)


FAULT_CHECKS = (
    "noscript_recovery",
    "connection_reconstruction",
    "post_write_response_loss_classified_timeout_unknown",
    "timeout_unknown_resolved_from_durable_receipt",
    "same_identity_replay_already_committed",
    "cap_exact_cleanup",
    "q2r_isolated_instance_destroyed",
    "no_script_flush",
    "no_namespace_wide_scan_by_fault_tests",
)
FAULT_ATTACHMENTS = {
    "connection-and-response-loss.txt",
    "noscript-and-idempotent-replay.txt",
    "fault-gate-cleanup.txt",
    "fault-gate-environment.txt",
}


def fault_attachment_checks(path, test_head):
    connection = (path / "connection-and-response-loss.txt").read_text(encoding="utf-8")
    noscript = (path / "noscript-and-idempotent-replay.txt").read_text(encoding="utf-8")
    cleanup = (path / "fault-gate-cleanup.txt").read_text(encoding="utf-8")
    environment = (path / "fault-gate-environment.txt").read_text(encoding="utf-8")
    if "--- FAIL:" in connection or "--- FAIL:" in noscript:
        return None
    checks = {
        "noscript_recovery": "Q2R2-NOSCRIPT OK" in noscript,
        "connection_reconstruction": "CONNECTION_REBUILD_PASS" in connection and "durable_readback=true" in connection,
        "post_write_response_loss_classified_timeout_unknown": "commit=TIMEOUT_UNKNOWN" in connection,
        "timeout_unknown_resolved_from_durable_receipt": "resolution=COMMITTED" in connection and "receipt_match=true" in connection,
        "same_identity_replay_already_committed": "Q2R2-7 OK" in noscript and "ALREADY_COMMITTED" in noscript,
        "cap_exact_cleanup": "CAP_DBSIZE_AFTER_EXACT_CLEANUP=0" in cleanup,
        "q2r_isolated_instance_destroyed": "Q2R_INSTANCE_TEARDOWN=destroyed_unique_ephemeral_container" in cleanup,
        "no_script_flush": "SCRIPT_FLUSH_USED=false" in cleanup,
        "no_namespace_wide_scan_by_fault_tests": "NAMESPACE_WIDE_SCAN_USED_BY_FAULT_TESTS=false" in cleanup,
    }
    if f"TEST_HEAD={test_head}" not in environment or "BIND=loopback_unique_ephemeral" not in environment:
        return None
    return checks if all(checks.values()) else None


R2_QUALIFICATION_ATTACHMENTS={"static-retirement.json","static-retirement.txt","model-v2-live.txt","remote-player-rpc.txt","cleanup.txt"}

def load_r2_qualification(train,expected_frozen):
    path=train/"r2-qualification"/"r2-qualification.json"
    if not path.is_file(): return None
    try:
        report=load(path)
        if report.get("schema_version")!="cap-redis-l1-r2.qualification.v1" or report.get("verdict")!="PASS":return None
        if report.get("business_head")!=expected_frozen["backend_business_commit"] or not lowercase_sha(report.get("verification_head")):return None
        if report.get("legacy_relationship_read_total")!=0 or report.get("legacy_relationship_write_total")!=0 or report.get("v1_fallback_total")!=0:return None
        if report.get("model_v2_migrations_observed",0)<2 or report.get("model_v2_sparse_commits_observed",0)<2 or report.get("remote_player_rpc_observed") is not True:return None
        if not all(report.get("checks",{}).values()):return None
        attachments=report.get("attachments",{})
        if set(attachments)!=R2_QUALIFICATION_ATTACHMENTS:return None
        for name,expected in attachments.items():
            attachment=path.parent/name
            if pathlib.Path(name).name!=name or len(expected)!=64 or not attachment.is_file() or hashlib.sha256(attachment.read_bytes()).hexdigest()!=expected:return None
        cleanup=(path.parent/"cleanup.txt").read_text(encoding="utf-8")
        if "CAP_DBSIZE_AFTER_EXACT_CLEANUP=0" not in cleanup or "Q2R_INSTANCE_TEARDOWN=destroyed_unique_ephemeral_container" not in cleanup or "SCRIPT_FLUSH_USED=false" not in cleanup or "FLUSHALL_USED=false" not in cleanup:return None
        return report
    except (KeyError,OSError,TypeError,ValueError,json.JSONDecodeError):return None


def load_fault_qualification(train, expected_frozen):
    path = train / "faults" / "fault-gate-verdict.json"
    if not path.is_file():
        return None
    try:
        report = load(path)
        if report.get("schema_version") != "cap-redis-fault-gates-v1" or report.get("verdict") != "PASS":
            return None
        if report.get("train_run_id") != train.name:
            return None
        if report.get("backend_business_commit") != expected_frozen["backend_business_commit"]:
            return None
        test_head = report.get("test_head", "")
        if len(test_head) != 40 or any(char not in "0123456789abcdef" for char in test_head):
            return None
        checks = report.get("checks", {})
        if not all(checks.get(check) is True for check in FAULT_CHECKS):
            return None
        attachments = report.get("attachments", {})
        if set(attachments) != FAULT_ATTACHMENTS:
            return None
        for name, expected_hash in attachments.items():
            if pathlib.Path(name).name != name or len(expected_hash) != 64:
                return None
            attachment = path.parent / name
            if not attachment.is_file() or hashlib.sha256(attachment.read_bytes()).hexdigest() != expected_hash:
                return None
        derived_checks = fault_attachment_checks(path.parent, test_head)
        if derived_checks is None or any(checks[check] != derived_checks[check] for check in FAULT_CHECKS):
            return None
        return report
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError):
        return None


BUSINESS_COMMANDS = ("BUY", "SELL", "CLAIM_TASK", "PLANT", "HARVEST", "WATER", "THEFT")
BUSINESS_ATTACHMENTS = {
    "business-command-amplification.json",
    "h5-bundle.json",
    "correctness-witness.json",
    "cleanup.txt",
    "tool-artifacts.json",
    "qualification.json",
}
BUSINESS_REVIEW_CHECKS = (
    "p0_zero", "p1_zero", "p2_zero", "exact_head_closure", "real_h5_end_to_end",
    "operation_bound", "background_excluded", "raw_identity_absent",
    "correctness_oracle", "redis_teardown",
)


def lowercase_sha(value):
    return isinstance(value, str) and len(value) == 40 and all(char in "0123456789abcdef" for char in value)


def load_business_qualification(train, expected_frozen):
    review_path = train / "business-trace" / "review-verdict.json"
    if not review_path.is_file():
        return None
    try:
        review = load(review_path)
        if review.get("schema_version") != "cap-redis-business-trace-review-v1" or review.get("verdict") != "PASS":
            return None
        if review.get("train_run_id") != train.name:
            return None
        run_name = review.get("trace_run", "")
        if pathlib.Path(run_name).name != run_name or not run_name:
            return None
        run = review_path.parent / run_name
        attachments = review.get("attachments", {})
        if set(attachments) != BUSINESS_ATTACHMENTS:
            return None
        for name, expected_hash in attachments.items():
            path = run / name
            if not path.is_file() or len(expected_hash) != 64 or hashlib.sha256(path.read_bytes()).hexdigest() != expected_hash:
                return None
        if not lowercase_sha(review.get("reviewed_evidence_head")) or not lowercase_sha(review.get("review_result_head")):
            return None
        if not all(review.get("checks", {}).get(check) is True for check in BUSINESS_REVIEW_CHECKS):
            return None

        trace, qualification = load(run / "business-command-amplification.json"), load(run / "qualification.json")
        witness, bundle = load(run / "correctness-witness.json"), load(run / "h5-bundle.json")
        tool_artifacts = load(run / "tool-artifacts.json")
        cleanup = (run / "cleanup.txt").read_text(encoding="utf-8")
        if trace.get("schema_version") != "cap-redis-business-amplification-trace-v1" or trace.get("verdict") != "PASS":
            return None
        if trace.get("train_run_id") != train.name or trace.get("backend_business_commit") != expected_frozen["backend_business_commit"]:
            return None
        if trace.get("h5_commit") != expected_frozen["h5_commit"] or not lowercase_sha(trace.get("tool_head")):
            return None
        if review.get("tool_head") != trace["tool_head"] or review.get("aggregate_sha256") != trace.get("aggregate_sha256"):
            return None
        rows = trace.get("rows", [])
        if len(rows) != len(BUSINESS_COMMANDS) or {row.get("command") for row in rows} != set(BUSINESS_COMMANDS):
            return None
        for row in rows:
            if row.get("status") != "PASS" or row.get("redis_ops", 0) <= 0 or row.get("total_bytes", 0) <= 0:
                return None
            if row.get("total_bytes") != row.get("request_bytes", 0) + row.get("response_bytes", 0):
                return None
            if row.get("actor_commit", 0) <= 0 or row.get("receipts") != row.get("actor_commit"):
                return None
            if row.get("redis_errors") != 0 or row.get("unanswered_requests") != 0:
                return None
            binding = row.get("operation_binding_sha256", "")
            if len(binding) != 64 or any(char not in "0123456789abcdef" for char in binding):
                return None
        if len({row["operation_binding_sha256"] for row in rows}) != len(rows):
            return None
        trace_checks = trace.get("checks", {})
        for check in ("exact_command_set", "all_browser_windows_pass", "all_rows_have_redis_transport",
                      "all_rows_have_actor_commit_and_receipt", "no_redis_error_or_unanswered_request",
                      "every_window_bound_to_operation", "causal_filter_excludes_background"):
            if trace_checks.get(check) is not True:
                return None
        if trace_checks.get("raw_redis_arguments_persisted") is not False or trace_checks.get("raw_identity_persisted") is not False:
            return None
        if qualification.get("schema_version") != "cap-redis-business-trace-qualification-v1" or qualification.get("verdict") != "PASS":
            return None
        if qualification.get("tool_head") != trace["tool_head"] or not all(qualification.get("checks", {}).values()):
            return None
        qualification_attachments = qualification.get("attachments", {})
        if set(qualification_attachments) != BUSINESS_ATTACHMENTS - {"qualification.json"}:
            return None
        for name, expected_hash in qualification_attachments.items():
            if hashlib.sha256((run / name).read_bytes()).hexdigest() != expected_hash:
                return None
        if witness.get("browser") != "PASS" or witness.get("protocol") != "PASS" or witness.get("authority_oracle") != "PASS" or witness.get("stack") != "PASS":
            return None
        if set(witness.get("exact_traced_commands", [])) != set(BUSINESS_COMMANDS):
            return None
        if bundle.get("mode") != "PREBUILT_STATIC_BUNDLE" or not bundle.get("files"):
            return None
        if tool_artifacts.get("schema_version") != "cap-redis-business-trace-tool-artifacts-v1" or tool_artifacts.get("tool_head") != trace["tool_head"]:
            return None
        artifacts = tool_artifacts.get("artifacts", {})
        if set(artifacts) != {"resp-trace-proxy", "business-trace-web", "business-trace.spec.ts", "run-business-trace"}:
            return None
        if not all(len(item.get("sha256", "")) == 64 and item.get("bytes", 0) > 0 for item in artifacts.values()):
            return None
        if "REDIS_TEARDOWN=DESTROYED_UNIQUE_EPHEMERAL_INSTANCE" not in cleanup or "SCRIPT_FLUSH_USED=false" not in cleanup or "FLUSHALL_USED=false" not in cleanup:
            return None
        return {"review": review, "trace": trace, "qualification": qualification, "run": run_name}
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError):
        return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--train-dir", required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--mode", choices=("smoke", "full"), required=True)
    args = parser.parse_args()
    train = pathlib.Path(args.train_dir)
    profile, manifest = load(args.profile), load(args.manifest)
    rounds = [load(path) for path in sorted((train / "rounds").glob("*.json"))]
    if not rounds:
        raise SystemExit("no round JSON")
    expected_frozen = manifest["frozen_identity"]
    task_id=expected_frozen["task_id"]
    is_r2=task_id=="CAP-REDIS-L1-R2"
    reasons = []
    for report in rounds:
        if report["frozen_identity"] != expected_frozen:
            reasons.append(f"{report['runtime_identity']['run_id']}: frozen identity drift")
        if report["runtime_identity"]["profile_hash"] != manifest["profile_hash"]:
            reasons.append(f"{report['runtime_identity']['run_id']}: profile hash drift")
        if report["verdict"] != "PASS":
            reasons.append(f"{report['runtime_identity']['run_id']}: round verdict={report['verdict']}")
        if gate_total(report["hard_correctness_gates"]) != 0:
            reasons.append(f"{report['runtime_identity']['run_id']}: hard correctness gate nonzero")
        if is_r2:
            r2=report.get("r2_hard_assertions",{})
            if r2.get("friend_complete_snapshot_v2_observed") is not True or r2.get("friend_batch_calls_match_unique_buckets") is not True or r2.get("friend_batch_expected_unique_buckets",0)<=0 or r2.get("friend_batch_expected_unique_buckets")!=r2.get("friend_batch_observed_script_calls"):
                reasons.append(f"{report['runtime_identity']['run_id']}: R2 Friend CompleteSnapshotV2 observation incomplete")

    formal = [report for report in rounds if report["runtime_identity"]["round_kind"] == "formal"]
    warmup = [report for report in rounds if report["runtime_identity"]["round_kind"] == "warmup"]
    if args.mode == "full" and (len(warmup) != 1 or len(formal) != 5):
        reasons.append(f"expected warmup=1/formal=5 got {len(warmup)}/{len(formal)}")

    comparable = {}
    for report in formal:
        for cell in report["cells"]:
            key = (cell["operation"], cell["key_distribution"], cell["concurrency"])
            comparable.setdefault(key, []).append(cell)
    if formal:
        expected_keys = {(cell["operation"], cell["key_distribution"], cell["concurrency"]) for cell in formal[0]["cells"]}
        for report in formal[1:]:
            got = {(cell["operation"], cell["key_distribution"], cell["concurrency"]) for cell in report["cells"]}
            if got != expected_keys:
                reasons.append(f"{report['runtime_identity']['run_id']}: cell matrix drift")

    aggregates = []
    for key, cells in sorted(comparable.items()):
        metrics = {}
        for field in ("redis_commands_per_op", "pipeline_items_per_op", "request_bytes_per_op", "response_bytes_per_op",
                      "redis_client_queue_us_per_op", "allocs_per_op", "client_cpu_us_per_op", "redis_cpu_us_per_op"):
            values = [float(cell[field]) for cell in cells]
            metrics[field] = {"median": statistics.median(values), "min": min(values), "max": max(values), "cross_round_cv": cv(values)}
        for field in ("client_latency_us", "lua_execution_us", "network_rtt_us", "serialization_us", "deserialization_us", "payload_bytes"):
            metrics[field] = {
                quantile: {"median": statistics.median(float(cell[field][quantile]) for cell in cells),
                           "min": min(float(cell[field][quantile]) for cell in cells),
                           "max": max(float(cell[field][quantile]) for cell in cells)}
                for quantile in ("p50", "p95", "p99")
            }
        aggregates.append({"operation": key[0], "key_distribution": key[1], "concurrency": key[2], "formal_rounds": len(cells), "metrics": metrics})

    required_gaps = []
    fault_qualification = load_fault_qualification(train, expected_frozen) if args.mode == "full" else None
    business_qualification = load_business_qualification(train, expected_frozen) if args.mode == "full" else None
    r2_qualification=load_r2_qualification(train,expected_frozen) if is_r2 else None
    if args.mode == "full":
        # These are fail-closed: primitive microbenchmarks must never be
        # relabeled as the requested business-command amplification proof.
        required_gaps = [] if business_qualification else [
            "actual end-to-end Redis trace per BUY/SELL/PLANT/HARVEST/WATER/THEFT/CLAIM_TASK is not yet captured and independently reviewed",
        ]
        if is_r2 and r2_qualification is None:
            required_gaps += ["R2 legacy-roster, Model-Field-V2, and remote_player qualification is missing or invalid"]
        if fault_qualification is None:
            required_gaps += [
                "NOSCRIPT recovery evidence is not yet attached to this train",
                "connection reconstruction evidence is not yet attached to this train",
                "post-write response-loss TIMEOUT_UNKNOWN resolution evidence is not yet attached to this train",
            ]
    verdict = "SMOKE_PASS" if args.mode == "smoke" and not reasons else "PASS"
    if reasons:
        verdict = "FAIL_CORRECTNESS" if any("correctness" in reason for reason in reasons) else "INCONCLUSIVE"
    elif required_gaps:
        verdict = "INCONCLUSIVE_REQUIRED_GAPS"

    summary = {
        "schema_version": "cap-redis-l1-r2.summary.v1" if is_r2 else "cap-redis-l1-r1.summary.v1",
        "task_id": task_id,
        "purpose": "r2_exact_product_redis_and_serialization_qualification" if is_r2 else "current_r1_integration_redis_and_serialization_baseline",
        "baseline_eligible": False,
        "supports_30m_dau": False,
        "frozen_identity": expected_frozen,
        "profile_hash": manifest["profile_hash"],
        "warmup_rounds": len(warmup),
        "formal_rounds": len(formal),
        "aggregates": aggregates,
        "observability_required_gaps": required_gaps,
        "fault_qualification": ({
            "verdict": fault_qualification["verdict"],
            "test_head": fault_qualification["test_head"],
            "checks": fault_qualification["checks"],
        } if fault_qualification else {"verdict": "NOT_QUALIFIED"}),
        "r2_qualification":(r2_qualification if r2_qualification else {"verdict":"NOT_QUALIFIED"}) if is_r2 else {"verdict":"NOT_APPLICABLE"},
        "r2_hard_assertions":({
            "legacy_relationship_read_total":r2_qualification["legacy_relationship_read_total"] if r2_qualification else None,
            "legacy_relationship_write_total":r2_qualification["legacy_relationship_write_total"] if r2_qualification else None,
            "v1_fallback_total":r2_qualification["v1_fallback_total"] if r2_qualification else None,
            "friend_complete_snapshot_v2_observed":all(r.get("r2_hard_assertions",{}).get("friend_complete_snapshot_v2_observed") is True for r in rounds),
            "friend_batch_script_calls_equal_unique_buckets":all(r.get("r2_hard_assertions",{}).get("friend_batch_calls_match_unique_buckets") is True for r in rounds),
            "invalid_order_total":sum(r["hard_correctness_gates"].get("invalid_order_total",0) for r in rounds),
            "workflow_step_unknown_residual":sum(r["hard_correctness_gates"].get("workflow_step_unknown_residual",0) for r in rounds),
            "due_residual":sum(r["hard_correctness_gates"].get("due_residual",0) for r in rounds),
            "namespace_residual_total":sum(r["hard_correctness_gates"].get("namespace_residual_total",0) for r in rounds),
            "correctness_violations":sum(c.get("correctness_violations",0) for r in rounds for c in r["cells"]),
            "required_gaps":len(required_gaps),
        } if is_r2 else {}),
        "business_trace_qualification": ({
            "verdict": business_qualification["review"]["verdict"],
            "trace_run": business_qualification["run"],
            "tool_head": business_qualification["trace"]["tool_head"],
            "aggregate_sha256": business_qualification["trace"]["aggregate_sha256"],
        } if business_qualification else {"verdict": "NOT_INDEPENDENTLY_REVIEWED"}),
        "verdict": verdict,
        "verdict_reasons": reasons,
    }
    (train / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    commands = list(BUSINESS_COMMANDS)
    if business_qualification:
        trace = business_qualification["trace"]
        amplification = {
            "schema_version": "cap-redis-l1-r2.business-amplification.v1" if is_r2 else "cap-redis-l1-r1.business-amplification.v1",
            "measurement_source": trace["measurement_source"],
            "trace_run": business_qualification["run"],
            "aggregate_sha256": trace["aggregate_sha256"],
            "rows": [{
                "command": row["command"], "actor_commit": row["actor_commit"],
                "workflow_writes": row["workflow_writes"], "orders": row["orders"],
                "due": row["due"], "receipts": row["receipts"], "redis_ops": row["redis_ops"],
                "total_bytes": row["total_bytes"], "verdict": "MEASURED",
            } for row in trace["rows"]],
        }
    else:
        amplification = {
            "schema_version": "cap-redis-l1-r2.business-amplification.v1" if is_r2 else "cap-redis-l1-r1.business-amplification.v1",
            "measurement_source": "PENDING_ACTUAL_END_TO_END_TRACE",
            "rows": [{"command": command, "actor_commit": None, "workflow_writes": None, "orders": None,
                      "due": None, "receipts": None, "redis_ops": None, "total_bytes": None,
                      "verdict": "NOT_MEASURED"} for command in commands],
        }
    (train / "business-command-amplification.json").write_text(json.dumps(amplification, indent=2) + "\n", encoding="utf-8")

    lines = [
        f"# {task_id} report", "", f"- Verdict: **{verdict}**",
        "- Classification: exact R2 Product Redis/serialization qualification" if is_r2 else "- Classification: current R1 Integration Redis/serialization baseline",
        "- `baseline_eligible=false`", "- `supports_30m_dau=false`",
        f"- Warmup/formal rounds: `{len(warmup)}/{len(formal)}`", f"- Profile hash: `{manifest['profile_hash']}`", "",
    ]
    if required_gaps:
        lines += ["## Required gaps (L1 cannot PASS)", ""] + [f"- {gap}" for gap in required_gaps] + [""]
    if fault_qualification:
        lines += ["## Fault qualification", "",
                  f"- Verdict: `PASS` (test head `{fault_qualification['test_head']}`)",
                  "- NOSCRIPT exact-source reload: `PASS`",
                  "- Connection reconstruction after server-side kill: `PASS`",
                  "- Post-write response loss: `TIMEOUT_UNKNOWN` resolved `COMMITTED` from the durable receipt",
                  "- Fault Redis instances were isolated and exactly destroyed; no `SCRIPT FLUSH` was used.", ""]
    if reasons:
        lines += ["## Invalid or incomparable evidence", ""] + [f"- {reason}" for reason in reasons] + [""]
    lines += ["## Primitive matrix", "", "| Operation | Key distribution | C | Redis commands/op | Req bytes/op | Resp bytes/op | Client P99 us | Lua P99 us | Cross-round commands CV |",
              "|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for item in aggregates:
        m = item["metrics"]
        lines.append("| {operation} | {key_distribution} | {concurrency} | {cmd:.3f} | {req:.1f} | {resp:.1f} | {client:.1f} | {lua:.1f} | {variation:.4f} |".format(
            **item, cmd=m["redis_commands_per_op"]["median"], req=m["request_bytes_per_op"]["median"],
            resp=m["response_bytes_per_op"]["median"], client=m["client_latency_us"]["p99"]["median"],
            lua=m["lua_execution_us"]["p99"]["median"], variation=m["redis_commands_per_op"]["cross_round_cv"]))
    lines += ["", "## Business-command amplification", ""]
    if business_qualification:
        lines += ["Measured through an operation-bound transparent RESP proxy around real H5 command journeys; background polling is excluded by in-memory operation/derived-work aliases.", "",
                  "| Command | Actor Commit | Workflow writes | Order | Due | Receipt | Redis Ops | Total bytes |",
                  "|---|---:|---:|---:|---:|---:|---:|---:|"]
        for row in amplification["rows"]:
            lines.append(f"| {row['command']} | {row['actor_commit']} | {row['workflow_writes']} | {row['orders']} | {row['due']} | {row['receipts']} | {row['redis_ops']} | {row['total_bytes']} |")
    else:
        lines += ["The required table remains `NOT_MEASURED`; primitive results are not substituted or summed into a fake business trace.", "",
                  "| Command | Actor Commit | Workflow writes | Order | Due | Receipt | Redis Ops | Total bytes |",
                  "|---|---:|---:|---:|---:|---:|---:|---:|"]
        lines.extend(f"| {command} | — | — | — | — | — | — | — |" for command in commands)
    lines += ["", "Warmup results are retained but excluded from aggregate statistics. No Mixed B0 eligibility is granted by this report.", ""]
    (train / "report.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"CAP_REDIS_SUMMARY={train/'summary.json'} VERDICT={verdict}")
    return 0 if verdict in ("SMOKE_PASS", "PASS", "INCONCLUSIVE_REQUIRED_GAPS") else 1


if __name__ == "__main__":
    sys.exit(main())
