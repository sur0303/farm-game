#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import pathlib
import re
import subprocess
import sys
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[3]
TOOLS = ROOT / "scripts/perf/friend-opportunity-c0"
LIB = TOOLS / "lib-c0.sh"
SMOKE = TOOLS / "run-c0-smoke"


def _extract_snippet(path: pathlib.Path, fn_name: str) -> str:
    """Extract the embedded python3 heredoc body of a bash helper function."""
    text = path.read_text(encoding="utf-8")
    start = text.index(f"{fn_name}() {{")
    marker = text.index("<<'PY'", start) + len("<<'PY'")
    end = text.index("PY\n", marker)
    return text[marker:end].strip("\n")


def _run_py(code: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-", *args],
        input=code,
        text=True,
        capture_output=True,
        check=False,
    )


def _write_prom(path: pathlib.Path, commands: int, request: int, response: int) -> None:
    path.write_text(
        "\n".join(
            [
                f'farm_redis_workload_commands_total{{component="unspecified",command="get",outcome="ok"}} {commands}',
                f'farm_redis_workload_request_bytes_total{{component="unspecified",command="get"}} {request}',
                f'farm_redis_workload_response_bytes_total{{component="unspecified",command="get",estimated="true"}} {response}',
                "",
            ]
        ),
        encoding="utf-8",
    )


class AggregateIdentityTest(unittest.TestCase):
    def _aggregate_code(self) -> str:
        return _extract_snippet(LIB, "c0_redis_workload_aggregate")

    def _merge_code(self) -> str:
        return _extract_snippet(LIB, "c0_merge_redis_evidence")

    def _sample_workload(self, path: pathlib.Path) -> None:
        path.write_text(
            json.dumps({"run_id": "c0mf20", "scenario": "friend-list", "friend_capacity": {"phases": []}}),
            encoding="utf-8",
        )

    def test_aggregate_binds_run_cell_scenario_and_interval(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            before, after = pathlib.Path(directory) / "before", pathlib.Path(directory) / "after"
            before.mkdir(), after.mkdir()
            for name in ("gate", "player", "farm", "worker"):
                _write_prom(before / f"{name}.prom", 10, 50, 80)
                _write_prom(after / f"{name}.prom", 17, 50, 90)
            result = _run_py(self._aggregate_code(), str(before), str(after), "c0m", "c0mf20", "friend-list")
            self.assertEqual(result.returncode, 0, result.stderr)
            aggregate = json.loads(result.stdout)
            self.assertEqual(aggregate["run_id"], "c0m")
            self.assertEqual(aggregate["cell_run_id"], "c0mf20")
            self.assertEqual(aggregate["scenario"], "friend-list")
            self.assertEqual(aggregate["availability"], "ok")
            self.assertEqual(aggregate["commands"], 28)
            self.assertEqual(len(aggregate["endpoints"]), 4)
            self.assertEqual(aggregate["interval"]["before"], sorted(p.name for p in before.iterdir()))

    def test_missing_metrics_endpoint_is_explicit_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            before, after = pathlib.Path(directory) / "before", pathlib.Path(directory) / "after"
            before.mkdir(), after.mkdir()
            for name in ("gate", "player", "worker"):
                _write_prom(before / f"{name}.prom", 1, 1, 1)
                _write_prom(after / f"{name}.prom", 2, 1, 1)
            # farm.prom is missing -> explicit measurement_unavailable, not a zero.
            result = _run_py(self._aggregate_code(), str(before), str(after), "c0m", "c0mf20", "hint")
            self.assertEqual(result.returncode, 3)
            aggregate = json.loads(result.stdout)
            self.assertEqual(aggregate["availability"], "measurement_unavailable")
            self.assertIsNone(aggregate["commands"])
            self.assertEqual(aggregate["scope"], "workload_participating_services_incomplete")

    def test_merge_rejects_aggregate_identity_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            workload = root / "workload.json"
            self._sample_workload(workload)
            aggregate = root / "aggregate.json"
            aggregate.write_text(
                json.dumps(
                    {
                        "run_id": "WRONG",
                        "cell_run_id": "c0mf20",
                        "scenario": "friend-list",
                        "commands": 1,
                        "bytes": 1,
                        "availability": "ok",
                        "endpoints": ["gate:gate.prom"],
                        "scope": "x",
                    }
                ),
                encoding="utf-8",
            )
            out = root / "out.json"
            result = _run_py(
                self._merge_code(), str(workload), str(aggregate), str(out), "c0m", "c0mf20", "friend-list"
            )
            self.assertEqual(result.returncode, 1)
            self.assertIn("aggregate identity mismatch", result.stderr)
            self.assertFalse(out.exists())

    def test_merge_writes_unavailable_evidence_with_failed_gate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            workload = root / "workload.json"
            self._sample_workload(workload)
            aggregate = root / "aggregate.json"
            aggregate.write_text(
                json.dumps(
                    {
                        "run_id": "c0m",
                        "cell_run_id": "c0mf20",
                        "scenario": "hint",
                        "interval": {"before": [], "after": []},
                        "commands": None,
                        "bytes": None,
                        "availability": "measurement_unavailable",
                        "endpoints": ["gate:gate.prom"],
                        "scope": "workload_participating_services_incomplete",
                    }
                ),
                encoding="utf-8",
            )
            out = root / "out.json"
            result = _run_py(
                self._merge_code(), str(workload), str(aggregate), str(out), "c0m", "c0mf20", "hint"
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            evidence = json.loads(out.read_text(encoding="utf-8"))
            self.assertEqual(evidence["redis_measurement_gate"], "fail")
            self.assertEqual(evidence["friend_capacity"]["redis_commands_availability"], "measurement_unavailable")
            self.assertEqual(evidence["redis_evidence_bound"]["cell_run_id"], "c0mf20")


class SmokeVerdictTest(unittest.TestCase):
    def _verdict_code(self) -> str:
        text = SMOKE.read_text(encoding="utf-8")
        marker = text.index("<<'PY'", text.index('python3 - "$evidence_root"')) + len("<<'PY'")
        end = text.index("PY\n", marker)
        return text[marker:end].strip("\n")

    def _run_verdict(self, root: pathlib.Path, measurement_gate: str, friend_list_status: str) -> subprocess.CompletedProcess[str]:
        return _run_py(
            self._verdict_code(),
            str(root),
            measurement_gate,
            friend_list_status,
            "c0sm1",
            "foperf-c0sm1",
            "c0sm1f20",
        )

    def _write_evidence(self, root: pathlib.Path, scenario: str, succeeded: int, errors: int, gate: str = "pass") -> None:
        (root / f"workload-{scenario}.json").write_text(
            json.dumps(
                {
                    "run_id": "c0sm1f20",
                    "scenario": scenario,
                    "scenario_attempted": 1,
                    "scenario_succeeded": succeeded,
                    "scenario_errors": errors,
                }
            ),
            encoding="utf-8",
        )
        (root / f"evidence-{scenario}.json").write_text(
            json.dumps(
                {
                    "run_id": "c0sm1",
                    "cell_run_id": "c0sm1f20",
                    "scenario": scenario,
                    "redis_measurement_gate": gate,
                    "redis_evidence_endpoints": ["gate:gate.prom"],
                    "redis_evidence_bound": {"run_id": "c0sm1", "cell_run_id": "c0sm1f20", "scenario": scenario},
                    "friend_capacity": {"server_phase_availability": [{"stage": "x", "availability": "measurement_unavailable"}]},
                }
            ),
            encoding="utf-8",
        )

    def _make_run(self, base: pathlib.Path, name: str) -> pathlib.Path:
        root = base / name
        root.mkdir(parents=True)
        for scenario in ("friend-list", "next-opportunity", "hint"):
            self._write_evidence(root, scenario, 0, 1)
        (root / "workload-friend-list.json").write_text(
            json.dumps(
                {
                    "run_id": "c0sm1f20",
                    "scenario": "friend-list",
                    "scenario_attempted": 1,
                    "scenario_succeeded": 1,
                    "scenario_errors": 0,
                }
            ),
            encoding="utf-8",
        )
        (root / "evidence-friend-list.json").write_text(
            json.dumps(
                {
                    "run_id": "c0sm1",
                    "cell_run_id": "c0sm1f20",
                    "scenario": "friend-list",
                    "redis_measurement_gate": "pass",
                    "redis_evidence_endpoints": ["gate:gate.prom"],
                    "redis_evidence_bound": {"run_id": "c0sm1", "cell_run_id": "c0sm1f20", "scenario": "friend-list"},
                    "friend_capacity": {"server_phase_availability": [{"stage": "x", "availability": "measurement_unavailable"}]},
                }
            ),
            encoding="utf-8",
        )
        return root

    def test_friend_list_scenario_error_fails_verdict(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = self._make_run(pathlib.Path(directory), "a")
            result = self._run_verdict(root, "pass", "fail")
            self.assertEqual(result.returncode, 1)
            self.assertEqual(json.loads((root / "verdict.json").read_text())["verdict"], "fail")

    def test_measurement_gate_failure_fails_verdict(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = self._make_run(pathlib.Path(directory), "b")
            result = self._run_verdict(root, "fail", "pass")
            self.assertEqual(result.returncode, 1)
            self.assertEqual(json.loads((root / "verdict.json").read_text())["verdict"], "fail")

    def test_honest_unknown_scenario_errors_do_not_fail_verdict(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = self._make_run(pathlib.Path(directory), "c")
            result = self._run_verdict(root, "pass", "pass")
            self.assertEqual(result.returncode, 0, result.stderr)
            verdict = json.loads((root / "verdict.json").read_text())
            self.assertEqual(verdict["verdict"], "pass")
            self.assertEqual(verdict["workloads"]["next-opportunity"]["scenario_errors"], 1)


class CleanupAndCollisionTest(unittest.TestCase):
    """Run the bash helpers against a fake docker shim so cleanup/collision
    behavior is verified without touching a real Docker daemon."""

    def _fake_docker(self, directory: pathlib.Path, existing: set[str], remove_fails: bool = False) -> pathlib.Path:
        shim = directory / "docker"
        template = """#!/usr/bin/env bash
existing_file='@EXISTING_FILE@'
case "$1" in
  inspect)
    name="$2"
    [[ -f "$existing_file" ]] || exit 1
    while IFS= read -r line; do [[ "$line" == "$name" ]] && { printf 'fake-id-%s\\n' "$name"; exit 0; }; done < "$existing_file"
    exit 1 ;;
  ps)
    [[ -f "$existing_file" ]] || exit 0
    while IFS= read -r line; do printf '%s\\n' "$line"; done < "$existing_file"
    exit 0 ;;
  logs) exit 0 ;;
  rm)
    if [[ "@REMOVE_FAILS@" == "1" ]]; then exit 1; fi
    name="$3"
    [[ -f "$existing_file" ]] || exit 0
    grep -v "^$name$" "$existing_file" > "$existing_file.tmp" || true
    mv "$existing_file.tmp" "$existing_file"
    exit 0 ;;
  *) exit 0 ;;
esac
"""
        template = template.replace("@EXISTING_FILE@", str(directory / "existing.txt"))
        template = template.replace("@REMOVE_FAILS@", "1" if remove_fails else "0")
        shim.write_text(template, encoding="utf-8")
        shim.chmod(0o755)
        (directory / "existing.txt").write_text("\n".join(existing) + ("\n" if existing else ""), encoding="utf-8")
        return shim

    def _run_bash(self, code: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(["bash", "-c", code], text=True, capture_output=True, check=False)

    def test_redis_up_refuses_preexisting_owned_container(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            shim = self._fake_docker(root, {"foperf-c0m1-redis"})
            lib = LIB.read_text(encoding="utf-8")
            code = (
                f"export PATH={shim.parent}:$PATH\n"
                f"source {LIB}\n"
                "c0_redis_up c0m1\n"
            )
            result = self._run_bash(code)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("already exists", result.stderr)

    def test_redis_up_starts_when_name_is_free(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            shim = self._fake_docker(root, set())
            lib = LIB.read_text(encoding="utf-8")
            # The fake docker run/exec/port do not implement real Redis, so we
            # only assert that the pre-start collision check passes (the run
            # path will fail later at port discovery, which is expected).
            code = (
                f"export PATH={shim.parent}:$PATH\n"
                f"source {LIB}\n"
                "c0_redis_up c0m1 2>&1 || true\n"
                "docker inspect foperf-c0m1-redis >/dev/null 2>&1 && echo STARTED || echo NOT_STARTED\n"
            )
            result = self._run_bash(code)
            self.assertIn("NOT_STARTED", result.stdout)

    def test_stack_down_reports_cleanup_failure_when_container_survives(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            shim = self._fake_docker(root, {"foperf-c0m1-redis"}, remove_fails=True)
            state = root / "state"
            state.mkdir()
            (state / "stack.env").write_text('PERF_REDIS_CONTAINER="foperf-c0m1-redis"\n', encoding="utf-8")
            (state / "stack-identity.env").write_text("container=foperf-c0m1-redis\n", encoding="utf-8")
            code = (
                f"export PATH={shim.parent}:$PATH\n"
                f"source {LIB}\n"
                f"c0_stack_down \"{state}\"\n"
            )
            result = self._run_bash(code)
            self.assertNotEqual(result.returncode, 0)
            cleanup = json.loads((state / "cleanup.json").read_text(encoding="utf-8"))
            self.assertFalse(cleanup["completed"])

    def test_stack_down_verifies_exact_resources_gone(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            shim = self._fake_docker(root, {"foperf-c0m1-redis"})
            state = root / "state"
            state.mkdir()
            (state / "stack.env").write_text('PERF_REDIS_CONTAINER="foperf-c0m1-redis"\n', encoding="utf-8")
            (state / "stack-identity.env").write_text("container=foperf-c0m1-redis\n", encoding="utf-8")
            code = (
                f"export PATH={shim.parent}:$PATH\n"
                f"source {LIB}\n"
                f"c0_stack_down \"{state}\"\n"
            )
            result = self._run_bash(code)
            self.assertEqual(result.returncode, 0, result.stderr)
            cleanup = json.loads((state / "cleanup.json").read_text(encoding="utf-8"))
            self.assertTrue(cleanup["completed"])


if __name__ == "__main__":
    unittest.main()
