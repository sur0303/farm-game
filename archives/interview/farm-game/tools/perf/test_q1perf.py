import argparse
import importlib.util
import json
import pathlib
import tempfile
import unittest

MODULE_PATH = pathlib.Path(__file__).with_name("q1perf.py")
SPEC = importlib.util.spec_from_file_location("q1perf", MODULE_PATH)
q1perf = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(q1perf)


class Q1PerfTest(unittest.TestCase):
    def test_redis_hash_ignores_credentials(self):
        identity_a, digest_a = q1perf.redis_target_identity("redis://:developer_006@example.invalid:6380/4")
        identity_b, digest_b = q1perf.redis_target_identity("redis://cache.local:6380/4")
        self.assertEqual("cache.local:6380/4", identity_a)
        self.assertEqual(identity_a, identity_b)
        self.assertEqual(digest_a, digest_b)
        self.assertNotIn("secret", digest_a)

    def test_sanitize_redacts_nested_secret_and_credential_uri(self):
        value = {
            "redis_password": "REPLACE_WITH_LOCAL_SECRET",
            "endpoint": "redis://user:developer_005@example.invalid:6379/0",
            "safe": {"namespace": "q1perf-safe-123456"},
        }
        got = q1perf.sanitize(value)
        self.assertEqual("<redacted>", got["redis_password"])
        self.assertEqual("redis://redis.example:6379/0", got["endpoint"])
        self.assertEqual("q1perf-safe-123456", got["safe"]["namespace"])
        self.assertNotIn("never-write-me", json.dumps(got))
        self.assertNotIn("password@", json.dumps(got))

    def test_ingest_ws_records_planned_vs_actual(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            report = {
                "configured_connections": 20,
                "connect_rate_per_second": 10,
                "configured_duration_seconds": 5,
                "attempted": 20,
                "ready": 20,
                "peak_active": 20,
                "scenario_attempted": 0,
                "scenario_succeeded": 0,
                "login_errors": 0,
                "connect_errors": 0,
                "hello_errors": 0,
                "join_errors": 0,
                "scenario_errors": 0,
                "unexpected_disconnects": 0,
                "ready_p50_ms": 2,
                "ready_p95_ms": 4,
                "ready_p99_ms": 6,
            }
            input_path = root / "driver.json"
            output_path = root / "scenario.json"
            input_path.write_text(json.dumps(report))
            rc = q1perf.cmd_ingest_ws(argparse.Namespace(
                input=[str(input_path)], samples=[], out=str(output_path),
                name="connections-20", profile="gate_connection_staircase",
                planned=20, namespace="q1perf-test-123456",
                redis_endpoint_hash="sha256:test", load_generator_cpu_stop_percent=85.0,
            ))
            self.assertEqual(0, rc)
            result = json.loads(output_path.read_text())
            self.assertEqual("PASS", result["status"])
            self.assertEqual(20, result["planned"]["arrivals"])
            self.assertEqual(20, result["actual"]["injected"])
            self.assertEqual(0, result["actual"]["schedule_drop"])
            self.assertEqual(10, result["metrics"]["actual_injected_qps"])

    def _manifest(self, run_kind="baseline"):
        return {
            "schema": q1perf.SCHEMA,
            "task_id": q1perf.TASK_ID,
            "run_id": "q1perf-test-123456",
            "run_kind": run_kind,
            "started_at": "2026-08-09T00:00:00Z",
            "finished_at": None,
            "test_window": {"started_at": "2026-08-09T00:00:00Z", "finished_at": None},
            "backend_commit": "a" * 40,
            "h5_commit": "b" * 40,
            "load_tool_commit": "c" * 40,
            "config_hash": "sha256:config",
            "host": {
                "cpu_model": "test", "logical_cpu_count": 8,
                "memory_total_bytes": 1024, "machine_spec_hash": "sha256:machine",
            },
            "redis": {
                "mode": "ephemeral_local", "endpoint_hash": "sha256:redis",
                "namespace": "q1perf-test-123456", "cleanup_completed": False,
            },
            "planned_load": {}, "actual_load": {},
        }

    def _scenario(self, status="PASS", failure_class="none", invariants=None):
        return {
            "name": "connections-20",
            "profile": "gate_connection_staircase",
            "status": status,
            "failure_class": failure_class,
            "reason": "test",
            "planned": {"arrivals": 20, "qps": 10},
            "actual": {"injected": 20, "ready": 20, "completed": 20, "schedule_drop": 0},
            "metrics": {"p95_ms": 4, "p99_ms": 6},
            "correctness_invariants": invariants or {},
        }

    def test_finalize_baseline_recorded_and_no_capacity_claim(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            (root / "scenarios").mkdir()
            (root / "baseline-run-manifest.json").write_text(json.dumps(self._manifest()))
            (root / "scenarios" / "one.json").write_text(json.dumps(self._scenario()))
            rc = q1perf.cmd_finalize(argparse.Namespace(
                out=str(root), run_kind="baseline", expected_scenarios=1,
                cleanup_completed=True, baseline_metrics="", regression_threshold=0.1,
            ))
            self.assertEqual(0, rc)
            verdict = json.loads((root / "baseline-verdict.json").read_text())
            self.assertEqual(q1perf.BASELINE_RECORDED, verdict["verdict"])
            self.assertFalse(verdict["supports_30m_dau"])
            report = (root / "performance-baseline-report.md").read_text()
            self.assertIn("not", report.lower())
            self.assertIn("30 million DAU", report)

    def test_finalize_correctness_failure_wins(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            (root / "scenarios").mkdir()
            (root / "baseline-run-manifest.json").write_text(json.dumps(self._manifest()))
            failed = self._scenario("FAIL", "correctness", {"farm_version_gap_total": 1})
            (root / "scenarios" / "one.json").write_text(json.dumps(failed))
            rc = q1perf.cmd_finalize(argparse.Namespace(
                out=str(root), run_kind="baseline", expected_scenarios=1,
                cleanup_completed=True, baseline_metrics="", regression_threshold=0.1,
            ))
            self.assertEqual(1, rc)
            verdict = json.loads((root / "baseline-verdict.json").read_text())
            self.assertEqual(q1perf.FAIL_CORRECTNESS, verdict["verdict"])
            self.assertEqual("FAIL", verdict["classification"])

    def test_f3_plan_armed_without_bound_wire_operation_is_contract_conflict(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            (root / "browser-verdict.json").write_text(json.dumps({"verdict": "FAIL", "steps": [{"step": "water-cycle-cleared-on-harvest"}]}))
            (root / "proxy.log").write_text(
                "2026/08/09 10:00:01 fault plan updated: {FailFarmChangeAllowRejoin:true}\n"
            )
            (root / "protocol-events.ndjson").write_text(
                json.dumps({"timestamp": "2026-08-09T10:00:00Z", "type": "DELIVER_BATCH_OK", "item_type": "FARM_CHANGE"}) + "\n"
            )
            result = q1perf.analyze_f3(root, utc_offset_hours=0)
            self.assertEqual("CONTRACT_CONFLICT", result["classification"])
            self.assertEqual("EVIDENCE_IDENTITY_MISSING", result["contract_detail"])
            self.assertTrue(result["chain"]["T0_fault_plan_armed"]["proven"])
            self.assertFalse(result["chain"]["T1_h5_operation_on_wire"]["proven"])
            self.assertEqual(0, result["facts"]["post_arm_proxy_event_count"])

    def test_f3_chain_pass_binds_owner_fault_by_exact_cycle(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            (root / "browser-verdict.json").write_text(json.dumps({
                "verdict": "PASS", "steps": [{"step": "f3-farm-change-failure-rejoined"}],
            }))
            (root / "fault-plan-observations.ndjson").write_text(json.dumps({
                "observed_at": "2026-08-09T10:00:00Z", "http_status": 200,
                "plan": {"fail_farm_change_allow_rejoin": True},
            }) + "\n")
            tap = [
                {"timestamp": "2026-08-09T09:59:59Z", "event": "SERVER_PUSH", "connection_id": "c1", "target_farm_id": 22, "subscription_id": "sub-visitor"},
                {"timestamp": "2026-08-09T10:00:01Z", "event": "CLIENT_COMMAND", "connection_id": "c1", "command_type": "THEFT", "operation_id": "theft-op", "target_farm_id": 22, "crop_cycle_id": "cycle-1"},
            ]
            (root / "ws-frame-tap.ndjson").write_text("".join(json.dumps(item) + "\n" for item in tap))
            proxy = [
                {"timestamp": "2026-08-09T10:00:02Z", "type": "FAIL_FARM_CHANGE", "item_type": "FARM_CHANGE", "item_id": "sub-owner:change:5", "crop_cycle_id": "cycle-1"},
                {"timestamp": "2026-08-09T10:00:03Z", "type": "DELIVER_BATCH_OK", "item_type": "REJOIN_REQUIRED", "item_id": "sub-visitor:rejoin:x"},
            ]
            (root / "protocol-events.ndjson").write_text("".join(json.dumps(item) + "\n" for item in proxy))
            (root / "theft-redis-dump.json").write_text(json.dumps({
                "orders": [{"operation_id": "theft-op", "dispatch_order_id": "dispatch-1", "state": "COMPLETED"}],
            }))
            result = q1perf.analyze_f3(root, utc_offset_hours=0)
            self.assertEqual("F3_CHAIN_PASS", result["classification"])
            self.assertEqual(1, result["chain"]["T3_farm_change_generated"]["unique_item_count"])
            self.assertTrue(result["chain"]["T8_rejoin_required"]["proven"])

    def test_invalid_order_counter_is_per_round_and_predates_f3(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            (root / "proxy.log").write_text(
                "2026/08/09 10:00:10 fault plan updated: {FailFarmChangeAllowRejoin:true}\n"
            )
            (root / "worker.log").write_text(
                "2026/08/09 10:00:01 recovery round theft={Discovered:2 MalformedMembers:0 NonTheft:0 InvalidOrders:2 Claimed:0 Completed:0 ResultsCompleted:0 Retried:0 Reconciled:0 LeaseHeld:0 LeaseLost:0 Errors:2 LastError:theft online: target binding unavailable: not found}\n"
                "2026/08/09 10:00:05 recovery round theft={Discovered:2 MalformedMembers:0 NonTheft:0 InvalidOrders:2 Claimed:0 Completed:0 ResultsCompleted:0 Retried:0 Reconciled:0 LeaseHeld:0 LeaseLost:0 Errors:2 LastError:theft online: target binding unavailable: not found}\n"
            )
            result = q1perf.analyze_invalid_orders(root, utc_offset_hours=0)
            self.assertEqual("INVALID_ORDER_PROCESSING_WITHOUT_PERSISTENT_RESIDUE", result["classification"])
            self.assertEqual(4, result["observations"]["invalid_processing_observations"])
            self.assertEqual(2, result["observations"]["max_invalid_orders_in_one_round"])
            self.assertIsNone(result["observations"]["unique_order_count"])

    def test_invalid_order_clean_rounds_are_not_misclassified_as_preexisting_invalids(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            (root / "worker.log").write_text(
                "2026/08/09 10:00:01 recovery round theft={Discovered:2 MalformedMembers:0 NonTheft:0 NotOwned:0 InvalidOrders:0 Claimed:0 Completed:0 ResultsCompleted:0 CareCompleted:2 Retried:0 Reconciled:0 LeaseHeld:0 LeaseLost:0 Errors:0 LastError:}\n"
            )
            (root / "theft-redis-dump.json").write_text(json.dumps({"orders": [
                {"order_id": "care-dispatch", "due_score": None},
                {"order_id": "care-result", "due_score": None},
            ]}))
            result = q1perf.analyze_invalid_orders(root, utc_offset_hours=0)
            self.assertEqual("NO_INVALID_PAIR_DUE_RESIDUE", result["classification"])
            self.assertEqual(0, result["observations"]["rounds_with_invalid_orders"])
            self.assertEqual(0, result["observations"]["invalid_processing_observations"])
            self.assertIsNone(result["observations"]["first_invalid_at"])

    def test_invalid_analyzer_detects_each_negative_control(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            (root / "worker.log").write_text(
                "2026/08/09 10:00:01 recovery round theft={Discovered:1 MalformedMembers:0 NonTheft:0 NotOwned:0 InvalidOrders:1 Claimed:0 Completed:0 ResultsCompleted:0 CareCompleted:0 Retried:0 Reconciled:0 LeaseHeld:0 LeaseLost:0 Errors:1 LastError:injected-invalid}\n"
            )
            (root / "theft-redis-dump.json").write_text(json.dumps({"orders": []}))
            invalid = q1perf.analyze_invalid_orders(root, utc_offset_hours=0)
            self.assertEqual("INVALID_ORDER_PROCESSING_WITHOUT_PERSISTENT_RESIDUE", invalid["classification"])
            self.assertEqual(1, invalid["observations"]["invalid_processing_observations"])

            (root / "worker.log").write_text("")
            (root / "theft-redis-dump.json").write_text(json.dumps({"orders": [
                {"order_id": "injected-due", "due_score": "1"},
            ]}))
            due = q1perf.analyze_invalid_orders(root, utc_offset_hours=0)
            self.assertEqual("PERSISTENT_PAIR_DUE_RESIDUE", due["classification"])
            self.assertEqual(1, due["observations"]["unique_due_candidate_count"])

            # Physical DueIndex residue must fail even when the corresponding
            # source order is missing/corrupt and therefore cannot be joined.
            (root / "theft-redis-dump.json").write_text(json.dumps({
                "counts": {"due_members": 1, "orders": 0},
                "due_indexes": [{"members": [{"work_ref": "orphan-work-ref", "due_score": "1"}]}],
                "orders": [],
            }))
            orphan = q1perf.analyze_invalid_orders(root, utc_offset_hours=0)
            self.assertEqual("PERSISTENT_PAIR_DUE_RESIDUE", orphan["classification"])
            self.assertEqual(1, orphan["observations"]["unique_due_candidate_count"])
            self.assertEqual(1, orphan["observations"]["unmatched_due_member_count"])

    def test_invalid_order_and_due_negative_controls_fail_then_clean_control_passes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            (root / "verdict.json").write_text(json.dumps({"verdict": "PASS", "reason": "all witnesses passed"}))
            (root / "browser-verdict.json").write_text(json.dumps({"verdict": "PASS", "steps": []}))
            (root / "protocol-verdict.json").write_text(json.dumps({"verdict": "PASS", "steps": {}}))
            (root / "f3-fault-chain-triage.json").write_text(json.dumps({"classification": "F3_CHAIN_PASS"}))

            def ingest(classification, invalid_count, due_count, name):
                (root / "theft-invalid-order-triage.json").write_text(json.dumps({
                    "classification": classification,
                    "observations": {
                        "invalid_processing_observations": invalid_count,
                        "unique_due_candidate_count": due_count,
                    },
                }))
                output = root / f"{name}.json"
                rc = q1perf.cmd_ingest_correctness(argparse.Namespace(
                    evidence_dir=str(root), out=str(output), name="product-correctness-anchor",
                    namespace="q1perf-negative-control", redis_endpoint_hash="sha256:test",
                ))
                return rc, json.loads(output.read_text())

            invalid_rc, invalid = ingest("INVALID_ORDER_PROCESSING_WITHOUT_PERSISTENT_RESIDUE", 1, 0, "invalid")
            self.assertEqual(1, invalid_rc)
            self.assertEqual("FAIL", invalid["status"])
            self.assertEqual(1, invalid["correctness_invariants"]["worker_invalid_order_processing_total"])
            self.assertEqual(1, invalid["correctness_invariants"]["hard_correctness_incident_total"])

            due_rc, due = ingest("PERSISTENT_PAIR_DUE_RESIDUE", 0, 1, "due")
            self.assertEqual(1, due_rc)
            self.assertEqual("FAIL", due["status"])
            self.assertEqual(1, due["correctness_invariants"]["persistent_pair_due_residue_total"])
            self.assertEqual(1, due["correctness_invariants"]["hard_correctness_incident_total"])

            clean_rc, clean = ingest("NO_INVALID_PAIR_DUE_RESIDUE", 0, 0, "clean")
            self.assertEqual(0, clean_rc)
            self.assertEqual("PASS", clean["status"])
            self.assertEqual(0, clean["correctness_invariants"]["hard_correctness_incident_total"])

    def test_invalid_orders_correlate_both_due_records_to_care(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            (root / "proxy.log").write_text(
                "2026/08/09 10:00:10 fault plan updated: {FailFarmChangeAllowRejoin:true}\n"
            )
            (root / "worker.log").write_text(
                "2026/08/09 10:00:01 recovery round theft={Discovered:2 MalformedMembers:0 NonTheft:0 InvalidOrders:2 Claimed:0 Completed:0 ResultsCompleted:0 Retried:0 Reconciled:0 LeaseHeld:0 LeaseLost:0 Errors:2 LastError:theft online: target binding unavailable: not found}\n"
            )
            (root / "ws-frame-tap.ndjson").write_text(json.dumps({
                "event": "CLIENT_COMMAND", "operation_id": "care-op", "command_type": "CARE",
            }) + "\n")
            (root / "theft-redis-dump.json").write_text(json.dumps({"orders": [
                {"order_id": "dispatch", "source_operation_id": "care-op", "due_score": "0"},
                {"order_id": "result", "dispatch_order_id": "dispatch", "due_score": "0"},
            ]}))
            result = q1perf.analyze_invalid_orders(root, utc_offset_hours=0)
            self.assertEqual("PRODUCT_WORKER_RECOVERY_SCOPE_FILTER_BUG", result["classification"])
            self.assertEqual(2, result["observations"]["care_due_candidate_count"])
            self.assertEqual({"CARE"}, {order["source_command_type"] for order in result["orders"]})

    def test_ingest_unclassified_f3_is_inconclusive_not_product_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            (root / "verdict.json").write_text(json.dumps({"verdict": "FAIL", "reason": "H5_FAIL_SERVER_PASS"}))
            (root / "browser-verdict.json").write_text(json.dumps({"verdict": "FAIL", "steps": []}))
            (root / "protocol-verdict.json").write_text(json.dumps({"verdict": "PASS", "steps": {}}))
            (root / "f3-fault-chain-triage.json").write_text(json.dumps({"classification": "CONTRACT_CONFLICT"}))
            output = root / "scenario.json"
            rc = q1perf.cmd_ingest_correctness(argparse.Namespace(
                evidence_dir=str(root), out=str(output), name="product-correctness-anchor",
                namespace="q1perf-test-123456", redis_endpoint_hash="sha256:test",
            ))
            self.assertEqual(2, rc)
            scenario = json.loads(output.read_text())
            self.assertEqual("INCONCLUSIVE", scenario["status"])
            self.assertEqual("fault_harness", scenario["failure_class"])
            self.assertEqual(q1perf.BASELINE_BLOCKED_BY_UNCLASSIFIED_F3, scenario["baseline_state"])
            self.assertEqual(0, scenario["correctness_invariants"]["hard_correctness_incident_total"])
            self.assertEqual(2, sum(scenario["correctness_signals"].values()))

    def test_ingest_care_due_scope_bug_is_one_hard_incident(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            (root / "verdict.json").write_text(json.dumps({"verdict": "PASS", "reason": "all witnesses passed"}))
            (root / "browser-verdict.json").write_text(json.dumps({"verdict": "PASS", "steps": [{"step": "f3-farm-change-failure-rejoined"}]}))
            (root / "protocol-verdict.json").write_text(json.dumps({"verdict": "PASS", "steps": {}}))
            (root / "f3-fault-chain-triage.json").write_text(json.dumps({"classification": "F3_CHAIN_PASS"}))
            (root / "theft-invalid-order-triage.json").write_text(json.dumps({
                "classification": "PRODUCT_WORKER_RECOVERY_SCOPE_FILTER_BUG",
                "observations": {"unique_due_candidate_count": 2},
            }))
            output = root / "scenario.json"
            rc = q1perf.cmd_ingest_correctness(argparse.Namespace(
                evidence_dir=str(root), out=str(output), name="product-correctness-anchor",
                namespace="q1perf-test-123456", redis_endpoint_hash="sha256:test",
            ))
            self.assertEqual(1, rc)
            scenario = json.loads(output.read_text())
            self.assertEqual("FAIL", scenario["status"])
            self.assertEqual("PRODUCT_WORKER_RECOVERY_SCOPE_FILTER_BUG", scenario["correctness_incidents"][0]["classification"])
            self.assertEqual(1, scenario["correctness_invariants"]["hard_correctness_incident_total"])
            self.assertEqual(2, scenario["correctness_invariants"]["persistent_pair_due_residue_total"])

    def test_finalize_counts_correlated_signals_as_one_incident(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            (root / "scenarios").mkdir()
            (root / "baseline-run-manifest.json").write_text(json.dumps(self._manifest()))
            scenario = self._scenario("FAIL", "correctness", {"hard_correctness_incident_total": 1})
            scenario["correctness_signals"] = {"browser_witness_failure": 1, "combined_anchor_failure": 1}
            scenario["correctness_incidents"] = [{
                "incident_id": "f3-1", "severity": "HARD", "correlated_signals": ["browser_witness_failure", "combined_anchor_failure"],
            }]
            (root / "scenarios" / "one.json").write_text(json.dumps(scenario))
            rc = q1perf.cmd_finalize(argparse.Namespace(
                out=str(root), run_kind="baseline", expected_scenarios=1,
                cleanup_completed=True, baseline_metrics="", regression_threshold=0.1,
            ))
            self.assertEqual(1, rc)
            verdict = json.loads((root / "baseline-verdict.json").read_text())
            self.assertEqual(1, verdict["hard_correctness_incident_count"])
            self.assertEqual(2, verdict["correctness_signal_count"])

    def test_finalize_missing_scenario_is_inconclusive(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            (root / "scenarios").mkdir()
            (root / "baseline-run-manifest.json").write_text(json.dumps(self._manifest()))
            (root / "scenarios" / "one.json").write_text(json.dumps(self._scenario()))
            rc = q1perf.cmd_finalize(argparse.Namespace(
                out=str(root), run_kind="baseline", expected_scenarios=2,
                cleanup_completed=True, baseline_metrics="", regression_threshold=0.1,
            ))
            self.assertEqual(2, rc)
            verdict = json.loads((root / "baseline-verdict.json").read_text())
            self.assertEqual(q1perf.INCONCLUSIVE_ENVIRONMENT, verdict["verdict"])


if __name__ == "__main__":
    unittest.main()
