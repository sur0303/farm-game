#!/usr/bin/env python3
"""Focused unit checks for the R2-Q coordinator and fixture contracts."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("r2_q_verification", ROOT / "scripts/r2-q-verification.py")
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def environment() -> dict:
    return {
        "rendered_manifest": {"configured": True},
        "official_url": {"official": False, "redacted": ""},
    }


class CoordinatorStatusTransitionTests(unittest.TestCase):
    def test_frozen_h5_default_rejects_old_r1_and_previous_r2_bases(self) -> None:
        frozen_h5 = "e3ce394cf77bbe111755fcd5dc576dfd4a557d34"
        old_r1_h5 = "fdb9849c0aad613b9a1c45ab8bf33a692c5ee967"
        previous_r2_h5 = "bb6785964cb819cd2d1c9f77b55d0f07cf8db356"
        parser = MODULE.build_parser()
        self.assertEqual(MODULE.R2_H5_BASE_COMMIT, frozen_h5)
        self.assertEqual(parser.parse_args([]).h5_base, frozen_h5)
        self.assertIn(frozen_h5, parser.format_help())
        self.assertEqual(parser.parse_args([]).route_snapshot, "deploy/k8s/static-staging/base/route-snapshot.v1.json")
        self.assertNotEqual(old_r1_h5, MODULE.R2_H5_BASE_COMMIT)
        self.assertNotEqual(previous_r2_h5, MODULE.R2_H5_BASE_COMMIT)

        with tempfile.TemporaryDirectory(prefix="r2-q-h5-base-test-") as temporary:
            missing_h5 = Path(temporary) / "missing-h5"
            for label, rejected_base in (("r1", old_r1_h5), ("previous-r2", previous_r2_h5)):
                output = Path(temporary) / f"run-{label}"
                exit_code = MODULE.main([
                    "--out", str(output),
                    "--run-id", f"r2q-h5-base-regression-{label}",
                    "--backend-root", str(ROOT),
                    "--h5-root", str(missing_h5),
                    "--h5-base", rejected_base,
                ])
                manifest = json.loads((output / "run-manifest.json").read_text())
                verdict = json.loads((output / "verdict.json").read_text())
                self.assertEqual(exit_code, 1)
                self.assertEqual(manifest["r2"]["h5_entry_base_commit"], frozen_h5)
                self.assertEqual(manifest["r2"]["h5_base_requested"], rejected_base)
                self.assertEqual(manifest["binding"]["h5_expected_base_commit"], frozen_h5)
                self.assertTrue(any("expected H5 base override" in reason for reason in verdict["assembly_reasons"]))
                self.assertFalse(verdict["final_pass_claim_allowed"])

    def test_later_backend_merge_candidate_stays_separate_from_entry_base(self) -> None:
        entry_base = "54c6d0a15cd2e0555d309b59319f1ce30fab268c"
        later_candidate = "a" * 40
        snapshot = {"commit": later_candidate, "valid": True, "base_is_ancestor": True, "dirty": False}
        bound = MODULE.bind_expected_commit(snapshot, later_candidate)
        self.assertTrue(bound["valid"])
        self.assertEqual(MODULE.R2_BASE_COMMIT, entry_base)
        self.assertNotEqual(entry_base, later_candidate)
        binding = MODULE.binding_for_run(
            {
                "backend": {"commit": later_candidate},
                "h5": {"commit": "b" * 40},
                "deploy": {"commit": later_candidate},
                "load_tool": {"commit": later_candidate},
            },
            entry_base,
            MODULE.R2_H5_BASE_COMMIT,
            later_candidate,
            {"profiles": [], "configs": [], "image_manifest": {}},
            {},
        )
        self.assertEqual(binding["r2_expected_base_commit"], entry_base)
        self.assertEqual(binding["merge_candidate_commit"], later_candidate)
        self.assertNotEqual(binding["r2_expected_base_commit"], binding["merge_candidate_commit"])

    def test_uncomposed_gate_is_skipped_and_not_eligible(self) -> None:
        spec = MODULE.GATE_BY_ID["model.v2.field_equality"]
        result = MODULE.gate_result(spec, None, None, {"contract_status": MODULE.SKIPPED}, environment())
        self.assertEqual(result["status"], MODULE.SKIPPED)
        self.assertFalse(result["final_pass_eligible"])

    def test_local_pass_requires_every_required_assertion(self) -> None:
        spec = MODULE.GATE_BY_ID["friendship.boundary.visit_permit"]
        evidence = {
            "status": MODULE.PASS,
            "environment": "local",
            "assertions": {"visit_permit": True},
            "_source_path": "evidence.json",
            "_source_sha256": "a" * 64,
        }
        result = MODULE.gate_result(spec, evidence, None, {"contract_status": MODULE.SKIPPED}, environment())
        self.assertEqual(result["status"], MODULE.PASS)
        self.assertTrue(result["final_pass_eligible"])

        missing = dict(evidence)
        missing["assertions"] = {"visit_permit": False}
        result = MODULE.gate_result(spec, missing, None, {"contract_status": MODULE.SKIPPED}, environment())
        self.assertEqual(result["status"], MODULE.INVALID)
        self.assertFalse(result["final_pass_eligible"])

    def test_localhost_never_satisfies_official_or_staging(self) -> None:
        official = MODULE.GATE_BY_ID["h5.official_url"]
        evidence = {
            "status": MODULE.PASS,
            "environment": "official_url",
            "environment_details": {"url": "http://localhost:8080"},
            "assertions": {key: True for key in official["required_assertions"]},
        }
        result = MODULE.gate_result(official, evidence, None, {"contract_status": MODULE.SKIPPED}, environment())
        self.assertEqual(result["status"], MODULE.BLOCKED)
        self.assertFalse(result["final_pass_eligible"])

        staging = MODULE.GATE_BY_ID["kubernetes.pod_ready"]
        local_evidence = {
            "status": MODULE.PASS,
            "environment": "local",
            "assertions": {key: True for key in staging["required_assertions"]},
        }
        result = MODULE.gate_result(staging, local_evidence, None, {"contract_status": MODULE.SKIPPED}, environment())
        self.assertEqual(result["status"], MODULE.BLOCKED)

    def test_actual_staging_requires_cluster_identity_and_namespace(self) -> None:
        spec = MODULE.GATE_BY_ID["kubernetes.pod_ready"]
        evidence = {
            "status": MODULE.PASS,
            "environment": "kubernetes_staging",
            "environment_details": {
                "actual_cluster": True,
                "cluster_identity": "cluster-a",
                "namespace": "farm-staging-system",
            },
            "assertions": {key: True for key in spec["required_assertions"]},
        }
        result = MODULE.gate_result(spec, evidence, None, {"contract_status": MODULE.SKIPPED}, environment())
        self.assertEqual(result["status"], MODULE.PASS)

    def test_redaction_removes_credentials_without_changing_scope(self) -> None:
        self.assertEqual(MODULE.redact_url("redis://user:developer_005@example.invalid:6379/0"), "redis://redis.example:6379/0")
        redacted = MODULE.redact_text("Authorization: Bearer abc123 token=secret redis://u:p@host:6379/0")
        self.assertNotIn("abc123", redacted)
        self.assertNotIn("secret", redacted)
        self.assertNotIn("u:p", redacted)


class FixtureSchemaTests(unittest.TestCase):
    def test_fixture_contracts_are_deterministic_and_initially_fail_closed(self) -> None:
        fixture_dir = ROOT / "docs/verification/r2-q-fixtures"
        schema = json.loads((fixture_dir / "r2-q-fixture-contracts.schema.json").read_text())
        self.assertEqual(schema["title"], "R2-Q deterministic verification fixture contract")
        summary, contracts = MODULE.load_fixture_contracts(ROOT)
        self.assertTrue(summary["valid"], summary["errors"])
        self.assertGreaterEqual(len(contracts), 6)
        for fixture_id, contract in contracts.items():
            self.assertTrue(contract["deterministic"], fixture_id)
            self.assertEqual(contract["contract_status"], MODULE.SKIPPED, fixture_id)
            self.assertIsInstance(contract["cases"], list)
            self.assertTrue(contract["cases"], fixture_id)

    def test_model_fixture_contains_every_required_migration_dimension(self) -> None:
        path = ROOT / "docs/verification/r2-q-fixtures/model-field-v2-fixtures.json"
        fixture = json.loads(path.read_text())
        self.assertEqual(fixture["fixture_id"], "model-field-v2-v1")
        self.assertEqual(fixture["contract_status"], MODULE.SKIPPED)
        self.assertEqual(fixture["domains"]["player"]["core_physical_models"], ["profile", "economy", "inventory", "progression", "task", "pet", "compendium", "mail_claim_state"])
        self.assertEqual(fixture["domains"]["farm"]["core_physical_models"], ["farm_meta", "plots", "guard", "activity_summary", "opportunity_header", "opportunity_facts"])
        self.assertEqual(fixture["domains"]["player"]["authoritative_models"], fixture["domains"]["player"]["core_physical_models"])
        self.assertEqual(fixture["domains"]["farm"]["authoritative_models"], fixture["domains"]["farm"]["core_physical_models"])
        self.assertNotIn("retained_v1_inert", fixture)
        serialized = json.dumps(fixture)
        self.assertNotIn("golden_digest", serialized)
        for forbidden in ("model:operation_receipts", "model:settlements", "model:theft_attempts", "model:care_attempts", "model:record"):
            self.assertNotIn(forbidden, serialized)
        self.assertEqual(fixture["layout_v2"]["envelope_prefixes"], {"receipt": "receipt:*", "outcome": "outcome:*", "order": "order:*", "due": "due:*"})
        self.assertTrue(fixture["authority_fences"]["no_dual_write"])
        self.assertGreaterEqual(len(fixture["golden_cases"]), 2)
        for case in fixture["golden_cases"][:2]:
            self.assertNotIn("model:record", case["expected"]["model_fields"])
            self.assertTrue(case["expected"]["v1_v2_business_state_equal"])
            self.assertFalse(case["expected"]["old_blob_present_after_commit"])
            self.assertTrue(case["expected"]["old_blob_deleted_atomically"])
            self.assertFalse(case["expected"]["v1_fallback"])
        self.assertGreaterEqual(len(fixture["migration_cases"]), 5)
        committed = next(item for item in fixture["migration_cases"] if item["case_id"] == "v1-migration-committed")
        self.assertFalse(committed["old_blob_present_after_commit"])
        self.assertTrue(committed["old_blob_deleted_atomically"])
        self.assertEqual(committed["v1_retention"], "none")
        self.assertFalse(committed["v1_fallback"])
        timeout = next(item for item in fixture["migration_cases"] if item["response"] == "TIMEOUT_UNKNOWN")
        self.assertEqual(timeout["max_unresolved_operations"], 1)
        self.assertEqual(timeout["resolved_responses"], ["COMMITTED", "NOT_COMMITTED"])
        phases = {item["phase"] for item in fixture["crash_after_each_phase"]}
        self.assertIn("send_commit", phases)
        self.assertIn("layout_v2_visible", phases)
        self.assertEqual(fixture["authority_fences"]["max_unresolved_operations"], 1)
        self.assertFalse(fixture["redis_comparison"]["redis_flushall_allowed"])
        self.assertTrue(fixture["redis_comparison"]["measurements_are_m1_observations"])

    def test_static_fixture_matches_landed_p1_composition(self) -> None:
        path = ROOT / "docs/verification/r2-q-fixtures/static-kubernetes-fixtures.json"
        fixture = json.loads(path.read_text())
        self.assertEqual(fixture["topology"]["h5_replicas"], 2)
        self.assertEqual(fixture["route_snapshot"], "deploy/k8s/static-staging/base/route-snapshot.v1.json")
        self.assertEqual(fixture["candidate_tools"], {
            "validator": "scripts/validate-k8s-static-staging",
            "test": "scripts/test-k8s-static-staging",
        })
        bindings = MODULE.verification_tool_bindings(ROOT)
        self.assertEqual(bindings["p1_k8s_validator"]["path"], "scripts/validate-k8s-static-staging")
        self.assertEqual(bindings["p1_k8s_test"]["path"], "scripts/test-k8s-static-staging")
        self.assertIn("p1_k8s_validator", MODULE.REQUIRED_TOOL_NAMES)
        self.assertIn("p1_k8s_test", MODULE.REQUIRED_TOOL_NAMES)


if __name__ == "__main__":
    unittest.main()
