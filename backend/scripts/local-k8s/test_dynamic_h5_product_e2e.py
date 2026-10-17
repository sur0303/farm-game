#!/usr/bin/env python3
import importlib.machinery
import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("verify-dynamic-h5-product-e2e")
LOADER = importlib.machinery.SourceFileLoader(
    "verify_dynamic_h5_product_e2e",
    str(SCRIPT),
)
SPEC = importlib.util.spec_from_loader(LOADER.name, LOADER)
VERIFY = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = VERIFY
LOADER.exec_module(VERIFY)


class DynamicH5ProductE2ETest(unittest.TestCase):
    def test_port_forward_uses_service_ports_not_container_ports(self) -> None:
        self.assertEqual(VERIFY.GATE_SERVICE_PORT, 8080)
        self.assertEqual(VERIFY.H5_SERVICE_PORT, 80)

    def test_optional_evidence_label_preserves_default_and_names_rerun(self) -> None:
        self.assertEqual(VERIFY.evidence_label(""), "")
        self.assertEqual(
            VERIFY.receipt_name("final", "", "run-1"),
            "dynamic-h5-product-e2e-final-run-1.json",
        )
        self.assertEqual(
            VERIFY.receipt_name(
                "final",
                VERIFY.evidence_label("final-g1-post-deadline"),
                "run-1",
            ),
            "dynamic-h5-product-e2e-final-final-g1-post-deadline-run-1.json",
        )
        for invalid in ("Final-G1", "../g1", "g1_post", "g1/other"):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    VERIFY.evidence_label(invalid)

    def test_stage_contract_covers_complete_mode_ladder(self) -> None:
        self.assertEqual(VERIFY.STAGES, {
            "compatibility": (
                "full",
                "pubsub",
                "DYNAMIC_OPPORTUNITY_COMPATIBILITY_ACTIVATED_NOT_TRAFFIC_VERIFIED",
            ),
            "shadow": (
                "narrow-shadow",
                "shadow-direct",
                "DYNAMIC_OPPORTUNITY_SHADOW_ACTIVATED_NOT_TRAFFIC_VERIFIED",
            ),
            "final": (
                "narrow",
                "direct",
                "DYNAMIC_OPPORTUNITY_FINAL_ACTIVATED_NOT_TRAFFIC_VERIFIED",
            ),
        })

    def test_local_proxy_and_playwright_configs_are_valid_modules(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            root_path = Path(root)
            proxy = root_path / "proxy.mjs"
            runner = root_path / "playwright.mjs"
            proxy.write_text(VERIFY.vite_proxy_config(
                18080,
                18081,
                18082,
                "test certificate",
                "test private key",
            ))
            runner.write_text(
                VERIFY.render_playwright_config(root_path / "h5", 18082)
            )
            for path in (proxy, runner):
                subprocess.run(
                    ["node", "--check", str(path)],
                    check=True,
                    capture_output=True,
                    text=True,
                )
            self.assertIn("ws:true", proxy.read_text())
            self.assertIn(
                "web-friend-opportunity-realtime.spec.ts",
                runner.read_text(),
            )
            self.assertIn(
                "baseURL:'https://farm.dynamic.local:18082'",
                runner.read_text(),
            )
            self.assertIn("--host-resolver-rules=", runner.read_text())
            self.assertIn("ignoreHTTPSErrors:true", runner.read_text())
            self.assertIn("https:{cert:", proxy.read_text())
            self.assertIn("allowedHosts:['farm.dynamic.local']", proxy.read_text())

    def test_receipt_write_is_immutable(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "receipt.json"
            VERIFY.write_exclusive(path, {"status": "first"})
            self.assertEqual(json.loads(path.read_text()), {"status": "first"})
            with self.assertRaises(FileExistsError):
                VERIFY.write_exclusive(path, {"status": "second"})

    def test_reserved_ports_are_distinct(self) -> None:
        ports = VERIFY.available_ports(3)
        self.assertEqual(len(ports), len(set(ports)))


if __name__ == "__main__":
    unittest.main()
