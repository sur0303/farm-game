#!/usr/bin/env python3
from __future__ import annotations

import json
import pathlib
import subprocess
import sys
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[3]
TOOLS = ROOT / "scripts/perf/cap-single-pod"
SCHEMA = ROOT / "docs/verification/perf/cap-single-pod/run-plan.schema.json"
TEMPLATE = ROOT / "deploy/load/cap-single-pod-r1-run-plan.template.json"
VALUES = ROOT / "deploy/load/cap-single-pod-r1-run-plan.values.example.json"


class HarnessToolsTest(unittest.TestCase):
    def run_tool(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run([sys.executable, *arguments], text=True, capture_output=True, check=False)

    def test_render_then_schema_validate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = pathlib.Path(directory) / "plan.json"
            rendered = self.run_tool(str(TOOLS / "render-run-plan.py"), "--template", str(TEMPLATE), "--values", str(VALUES), "--out", str(output))
            self.assertEqual(rendered.returncode, 0, rendered.stderr)
            validated = self.run_tool(str(TOOLS / "verify-json-schema.py"), "--schema", str(SCHEMA), "--document", str(output))
            self.assertEqual(validated.returncode, 0, validated.stderr)

    def test_schema_rejects_unknown_field_without_echoing_value(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            document = pathlib.Path(directory) / "invalid.json"
            data = json.loads(VALUES.read_text(encoding="utf-8"))
            output = pathlib.Path(directory) / "plan.json"
            rendered = self.run_tool(str(TOOLS / "render-run-plan.py"), "--template", str(TEMPLATE), "--values", str(VALUES), "--out", str(output))
            self.assertEqual(rendered.returncode, 0)
            plan = json.loads(output.read_text(encoding="utf-8"))
            plan["password"] = "must-not-be-echoed"
            document.write_text(json.dumps(plan), encoding="utf-8")
            validated = self.run_tool(str(TOOLS / "verify-json-schema.py"), "--schema", str(SCHEMA), "--document", str(document))
            self.assertEqual(validated.returncode, 1)
            self.assertNotIn("must-not-be-echoed", validated.stderr)

    def test_renderer_rejects_unused_values_and_existing_output(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            values = pathlib.Path(directory) / "values.json"
            data = json.loads(VALUES.read_text(encoding="utf-8"))
            data["UNUSED"] = True
            values.write_text(json.dumps(data), encoding="utf-8")
            output = pathlib.Path(directory) / "plan.json"
            rendered = self.run_tool(str(TOOLS / "render-run-plan.py"), "--template", str(TEMPLATE), "--values", str(values), "--out", str(output))
            self.assertEqual(rendered.returncode, 2)
            output.write_text("{}", encoding="utf-8")
            rendered = self.run_tool(str(TOOLS / "render-run-plan.py"), "--template", str(TEMPLATE), "--values", str(VALUES), "--out", str(output))
            self.assertEqual(rendered.returncode, 2)


if __name__ == "__main__":
    unittest.main()
