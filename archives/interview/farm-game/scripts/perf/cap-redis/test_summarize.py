#!/usr/bin/env python3
import hashlib
import importlib.util
import json
import pathlib
import tempfile
import unittest

MODULE_PATH = pathlib.Path(__file__).with_name("summarize.py")
SPEC = importlib.util.spec_from_file_location("cap_redis_summarize", MODULE_PATH)
SUMMARY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SUMMARY)


class FaultQualificationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.train = pathlib.Path(self.temp.name) / "run-1"
        self.faults = self.train / "faults"
        self.faults.mkdir(parents=True)
        self.attachment_content = {
            "connection-and-response-loss.txt": (
                "CONNECTION_REBUILD_PASS durable_readback=true\n"
                "TIMEOUT_UNKNOWN_RESPONSE_LOSS_PASS commit=TIMEOUT_UNKNOWN "
                "resolution=COMMITTED receipt_match=true\nPASS\n"
            ),
            "noscript-and-idempotent-replay.txt": (
                "Q2R2-NOSCRIPT OK\nQ2R2-7 OK ALREADY_COMMITTED\nPASS\n"
            ),
            "fault-gate-cleanup.txt": (
                "CAP_DBSIZE_AFTER_EXACT_CLEANUP=0\n"
                "Q2R_INSTANCE_TEARDOWN=destroyed_unique_ephemeral_container\n"
                "SCRIPT_FLUSH_USED=false\n"
                "NAMESPACE_WIDE_SCAN_USED_BY_FAULT_TESTS=false\n"
            ),
            "fault-gate-environment.txt": (
                f"TEST_HEAD={'a' * 40}\nBIND=loopback_unique_ephemeral\n"
            ),
        }
        for name, content in self.attachment_content.items():
            (self.faults / name).write_text(content, encoding="utf-8")
        self.report = {
            "schema_version": "cap-redis-fault-gates-v1",
            "train_run_id": self.train.name,
            "test_head": "a" * 40,
            "backend_business_commit": "b" * 40,
            "verdict": "PASS",
            "checks": {check: True for check in SUMMARY.FAULT_CHECKS},
            "attachments": {
                name: hashlib.sha256((self.faults / name).read_bytes()).hexdigest()
                for name in self.attachment_content
            },
        }
        self.path = self.faults / "fault-gate-verdict.json"
        self.write_report()

    def tearDown(self):
        self.temp.cleanup()

    def write_report(self):
        self.path.write_text(json.dumps(self.report), encoding="utf-8")

    def load(self):
        return SUMMARY.load_fault_qualification(
            self.train, {"backend_business_commit": "b" * 40}
        )

    def test_accepts_complete_hash_bound_evidence(self):
        self.assertEqual(self.load()["verdict"], "PASS")

    def test_rejects_tampered_attachment(self):
        (self.faults / "connection-and-response-loss.txt").write_text(
            "changed\n", encoding="utf-8"
        )
        self.assertIsNone(self.load())

    def test_rejects_hash_bound_but_incomplete_output(self):
        name = "connection-and-response-loss.txt"
        (self.faults / name).write_text("PASS\n", encoding="utf-8")
        self.report["attachments"][name] = hashlib.sha256(
            (self.faults / name).read_bytes()
        ).hexdigest()
        self.write_report()
        self.assertIsNone(self.load())

    def test_rejects_missing_check(self):
        self.report["checks"][SUMMARY.FAULT_CHECKS[0]] = False
        self.write_report()
        self.assertIsNone(self.load())

    def test_rejects_wrong_business_binding(self):
        self.report["backend_business_commit"] = "c" * 40
        self.write_report()
        self.assertIsNone(self.load())

    def test_rejects_attachment_path_escape(self):
        self.report["attachments"] = {"../probe.txt": "0" * 64}
        self.write_report()
        self.assertIsNone(self.load())

    def test_rejects_missing_required_attachment(self):
        self.report["attachments"].pop("fault-gate-environment.txt")
        self.write_report()
        self.assertIsNone(self.load())


class R2QualificationTest(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.train=pathlib.Path(self.temp.name)/"run-r2";self.out=self.train/"r2-qualification";self.out.mkdir(parents=True)
        contents={"static-retirement.json":"{}\n","static-retirement.txt":"PASS\n","model-v2-live.txt":"R2_MODEL_V2_PASS\n","remote-player-rpc.txt":"PASS\n","cleanup.txt":"CAP_DBSIZE_AFTER_EXACT_CLEANUP=0\nQ2R_INSTANCE_TEARDOWN=destroyed_unique_ephemeral_container\nSCRIPT_FLUSH_USED=false\nFLUSHALL_USED=false\n"}
        for name,value in contents.items():(self.out/name).write_text(value)
        self.report={"schema_version":"cap-redis-l1-r2.qualification.v1","verdict":"PASS","business_head":"b"*40,"verification_head":"c"*40,"legacy_relationship_read_total":0,"legacy_relationship_write_total":0,"v1_fallback_total":0,"model_v2_migrations_observed":2,"model_v2_sparse_commits_observed":2,"remote_player_rpc_observed":True,"checks":{"all":True},"attachments":{name:hashlib.sha256((self.out/name).read_bytes()).hexdigest() for name in contents}}
        self.write()
    def tearDown(self):self.temp.cleanup()
    def write(self):(self.out/"r2-qualification.json").write_text(json.dumps(self.report))
    def load(self):return SUMMARY.load_r2_qualification(self.train,{"backend_business_commit":"b"*40})
    def test_accepts_hash_bound_r2_qualification(self):self.assertEqual(self.load()["verdict"],"PASS")
    def test_rejects_nonzero_v1_fallback(self):self.report["v1_fallback_total"]=1;self.write();self.assertIsNone(self.load())
    def test_rejects_tampered_attachment(self):(self.out/"model-v2-live.txt").write_text("changed\n");self.assertIsNone(self.load())


class BusinessQualificationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.train = pathlib.Path(self.temp.name) / "run-1"
        self.business = self.train / "business-trace"
        self.run = self.business / "trace-1"
        self.run.mkdir(parents=True)
        rows = []
        for index, command in enumerate(SUMMARY.BUSINESS_COMMANDS):
            rows.append({
                "command": command, "status": "PASS", "actor_commit": 1,
                "workflow_writes": 0, "orders": 0, "due": 0, "receipts": 1,
                "redis_ops": 1, "request_bytes": 10, "response_bytes": 5,
                "total_bytes": 15, "redis_errors": 0, "unanswered_requests": 0,
                "operation_binding_sha256": f"{index + 1:064x}",
            })
        self.trace = {
            "schema_version": "cap-redis-business-amplification-trace-v1",
            "train_run_id": self.train.name,
            "backend_business_commit": "b" * 40,
            "backend_evidence_head": "e" * 40,
            "h5_commit": "h" * 40,
            "tool_head": "1" * 40,
            "measurement_source": "REAL_H5",
            "rows": rows,
            "checks": {
                "exact_command_set": True, "all_browser_windows_pass": True,
                "all_rows_have_redis_transport": True,
                "all_rows_have_actor_commit_and_receipt": True,
                "no_redis_error_or_unanswered_request": True,
                "every_window_bound_to_operation": True,
                "causal_filter_excludes_background": True,
                "raw_redis_arguments_persisted": False, "raw_identity_persisted": False,
            },
            "aggregate_sha256": "a" * 64, "verdict": "PASS",
        }
        (self.run / "business-command-amplification.json").write_text(json.dumps(self.trace), encoding="utf-8")
        (self.run / "correctness-witness.json").write_text(json.dumps({
            "browser": "PASS", "protocol": "PASS", "authority_oracle": "PASS", "stack": "PASS",
            "exact_traced_commands": list(SUMMARY.BUSINESS_COMMANDS),
        }), encoding="utf-8")
        (self.run / "h5-bundle.json").write_text(json.dumps({"mode": "PREBUILT_STATIC_BUNDLE", "files": [{"path": "index.html"}]}), encoding="utf-8")
        (self.run / "cleanup.txt").write_text(
            "REDIS_TEARDOWN=DESTROYED_UNIQUE_EPHEMERAL_INSTANCE\nSCRIPT_FLUSH_USED=false\nFLUSHALL_USED=false\n", encoding="utf-8"
        )
        (self.run / "tool-artifacts.json").write_text(json.dumps({
            "schema_version": "cap-redis-business-trace-tool-artifacts-v1", "tool_head": "1" * 40,
            "artifacts": {name: {"bytes": 1, "sha256": "a" * 64} for name in
                          ["resp-trace-proxy", "business-trace-web", "business-trace.spec.ts", "run-business-trace"]},
        }), encoding="utf-8")
        self.write_qualification()
        self.write_review()

    def tearDown(self):
        self.temp.cleanup()

    def hash(self, name):
        return hashlib.sha256((self.run / name).read_bytes()).hexdigest()

    def write_qualification(self):
        names = SUMMARY.BUSINESS_ATTACHMENTS - {"qualification.json"}
        qualification = {
            "schema_version": "cap-redis-business-trace-qualification-v1",
            "tool_head": "1" * 40, "verdict": "PASS", "checks": {"all": True},
            "attachments": {name: self.hash(name) for name in names},
        }
        (self.run / "qualification.json").write_text(json.dumps(qualification), encoding="utf-8")

    def write_review(self):
        review = {
            "schema_version": "cap-redis-business-trace-review-v1", "verdict": "PASS",
            "train_run_id": self.train.name, "trace_run": self.run.name,
            "reviewed_evidence_head": "c" * 40, "review_result_head": "d" * 40,
            "tool_head": "1" * 40, "aggregate_sha256": "a" * 64,
            "checks": {check: True for check in SUMMARY.BUSINESS_REVIEW_CHECKS},
            "attachments": {name: self.hash(name) for name in SUMMARY.BUSINESS_ATTACHMENTS},
        }
        (self.business / "review-verdict.json").write_text(json.dumps(review), encoding="utf-8")

    def load(self):
        return SUMMARY.load_business_qualification(
            self.train, {"backend_business_commit": "b" * 40, "h5_commit": "h" * 40}
        )

    def test_accepts_independently_reviewed_operation_bound_trace(self):
        self.assertEqual(self.load()["trace"]["verdict"], "PASS")

    def test_rejects_qualification_without_review(self):
        (self.business / "review-verdict.json").unlink()
        self.assertIsNone(self.load())

    def test_rejects_tampered_trace(self):
        with (self.run / "business-command-amplification.json").open("a", encoding="utf-8") as handle:
            handle.write(" ")
        self.assertIsNone(self.load())

    def test_rejects_false_review_check(self):
        review = json.loads((self.business / "review-verdict.json").read_text())
        review["checks"][SUMMARY.BUSINESS_REVIEW_CHECKS[0]] = False
        (self.business / "review-verdict.json").write_text(json.dumps(review), encoding="utf-8")
        self.assertIsNone(self.load())

    def test_rejects_missing_operation_binding(self):
        self.trace["rows"][0]["operation_binding_sha256"] = ""
        (self.run / "business-command-amplification.json").write_text(json.dumps(self.trace), encoding="utf-8")
        self.write_qualification()
        self.write_review()
        self.assertIsNone(self.load())


if __name__ == "__main__":
    unittest.main()
