#!/usr/bin/env python3
import importlib.machinery
import importlib.util
import subprocess
import sys
import unittest
from pathlib import Path
from unittest import mock

SCRIPT_DIR = Path(__file__).resolve().parent


def load(name: str, filename: str):
    loader = importlib.machinery.SourceFileLoader(
        name,
        str(SCRIPT_DIR / filename),
    )
    spec = importlib.util.spec_from_loader(loader.name, loader)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    loader.exec_module(module)
    return module


G2 = load("verify_dynamic_g2_test", "verify-dynamic-g2-planned-migration")
G3 = load("verify_dynamic_g3_test", "verify-dynamic-g3-crash-takeover")


class DynamicG23Test(unittest.TestCase):
    def test_g2_diagnostic_mode_is_explicit(self) -> None:
        self.assertFalse(G2.diagnostic_mode([]))
        self.assertTrue(G2.diagnostic_mode(["--diagnostic"]))
        with self.assertRaises(SystemExit):
            G2.diagnostic_mode(["--unknown"])

    def test_g2_diagnostic_coherence_ignores_business_mode(self) -> None:
        route_runtime = {
            "snapshot_revision": 7,
            "assignment_state": "WATCHING",
            "assignment_healthy": True,
            "process_ready": True,
        }
        consumers = {
            "player": {
                "app": "player-svr",
                "revision": 7,
                "readyz": {"route_runtime": route_runtime},
            },
            "farm": {
                "app": "farm-svr",
                "revision": 7,
                "readyz": {"route_runtime": route_runtime},
                "narrow_mode": 0,
            },
            "worker": {
                "app": "farm-worker-a",
                "revision": 7,
                "worker_assignment_revision": 7,
                "worker_owned_shards": 1,
                "worker_assignment_ready": 1,
                "direct_mode": 0,
                "readyz": {
                    "ready": True,
                    "route_projection_fresh": True,
                },
            },
            "worker-b": {
                "app": "farm-worker-b",
                "revision": 7,
                "worker_assignment_revision": 7,
                "worker_owned_shards": 3,
                "worker_assignment_ready": 1,
                "direct_mode": 0,
                "readyz": {
                    "ready": True,
                    "route_projection_fresh": True,
                },
            },
            "gate": {
                "app": "farm-gate",
                "revision": 7,
                "readyz": {},
            },
        }
        snapshot = {
            "revision": 7,
            "canonical_json": {
                "farm_worker": {"shard_count": 4},
            },
        }
        self.assertTrue(G2.route_coherent(snapshot, consumers))
        consumers["worker"]["worker_assignment_revision"] = 6
        self.assertFalse(G2.route_coherent(snapshot, consumers))
        consumers["worker"]["worker_assignment_revision"] = 7
        consumers["worker-b"]["worker_owned_shards"] = 2
        self.assertFalse(G2.route_coherent(snapshot, consumers))

    def test_g2_route_identity_and_target_are_exact(self) -> None:
        snapshot = {
            "revision": 7,
            "canonical_json": {
                "farm_worker": {
                    "routes": [{
                        "holder_index": 0,
                        "state": "ACTIVE",
                        "shard_revision": 2,
                        "expected_writer_epoch": 3,
                        "expected_lease_epoch": 4,
                        "lease_expire_at_ms": 5000,
                    }],
                },
                "holders": [{
                    "instance_id": "farm-worker-a",
                    "boot_id": "boot-a",
                    "endpoint": "farm-worker-a:9201",
                }],
            },
        }
        source = G2.route_identity(snapshot, "FARM_WORKER", 0)
        self.assertEqual(source["holder_id"], "farm-worker-a")
        self.assertEqual(source["assignment_revision"], 2)
        target = G2.eligible_target(
            {"holders": [{
                "holder_id": "farm-worker-b",
                "boot_id": "boot-b",
                "logic_endpoint": "farm-worker-b:9201",
                "state": "SERVING",
                "process_ready": True,
                "draining": False,
                "capacity_weight": 1,
                "snapshot_revision": 7,
                "lease_until_ms": 6000,
            }]},
            "farm-worker-a",
            7,
            1000,
        )
        self.assertEqual(target["holder_id"], "farm-worker-b")
        target["snapshot_revision"] = 8
        self.assertEqual(
            G2.eligible_target(
                {"holders": [target]},
                "farm-worker-a",
                7,
                1000,
            )["holder_id"],
            "farm-worker-b",
        )

    def test_g2_requires_complete_fence_advance(self) -> None:
        before = {
            "assignment_revision": 3,
            "writer_epoch": 4,
            "lease_epoch": 5,
        }
        self.assertTrue(G2.strictly_advanced(before, {
            "assignment_revision": 4,
            "writer_epoch": 5,
            "lease_epoch": 6,
        }))
        self.assertFalse(G2.strictly_advanced(before, {
            "assignment_revision": 4,
            "writer_epoch": 4,
            "lease_epoch": 6,
        }))

    def test_g2_retries_transient_plan_verification(self) -> None:
        verified = {
            "consistent": True,
            "checks": [{"name": "final_freshness", "ok": True}],
        }
        with (
            mock.patch.object(
                G2.G1,
                "run_ctl",
                side_effect=[
                    subprocess.CalledProcessError(1, ["routectl", "verify"]),
                    {
                        "consistent": False,
                        "checks": [{"name": "final_freshness", "ok": False}],
                    },
                    verified,
                ],
            ) as run_ctl,
            mock.patch.object(G2.time, "sleep"),
        ):
            result = G2.wait_plan_verified(
                Path("/tmp/routectl"),
                {},
                "plan-1",
                "PLAYER",
                0,
            )
        self.assertEqual(result, verified)
        self.assertEqual(run_ctl.call_count, 3)

    def test_g2_retries_idempotent_move_until_succeeded(self) -> None:
        succeeded = {"plan": {"phase": "SUCCEEDED"}}
        with (
            mock.patch.object(
                G2.G1,
                "run_ctl",
                side_effect=[
                    subprocess.CalledProcessError(
                        1,
                        ["routectl", "move-shard"],
                    ),
                    {"plan": {"phase": "RUNNING"}},
                    succeeded,
                ],
            ) as run_ctl,
            mock.patch.object(G2.time, "sleep"),
        ):
            result = G2.wait_plan_moved(
                Path("/tmp/routectl"),
                {},
                ["move-shard", "--plan-id", "plan-1"],
            )
        self.assertEqual(result, succeeded)
        self.assertEqual(run_ctl.call_count, 3)

    def test_g3_diagnostic_contract_is_separate_from_formal(self) -> None:
        diagnostic = G3.run_contract(True)
        formal = G3.run_contract(False)
        self.assertEqual(diagnostic["g2_label"], "g2-diagnostic")
        self.assertEqual(
            diagnostic["g3_status"],
            "G3_PROOF_GATED_CRASH_TAKEOVER_DIAGNOSTIC_PASS",
        )
        self.assertEqual(formal["g2_label"], "g2-planned-migration")
        self.assertEqual(
            formal["g3_status"],
            "G3_PROOF_GATED_CRASH_TAKEOVER_PASS",
        )

    def test_g3_uses_holder_unavailable_while_authority_active(self) -> None:
        source = {
            "holder_id": "farm-worker-b",
            "holder_boot_id": "old-boot",
            "assignment_revision": 8,
            "writer_epoch": 9,
            "lease_epoch": 10,
        }
        target = {
            "holder_id": "farm-worker-a",
            "boot_id": "new-target-boot",
            "logic_endpoint": "farm-worker-a:9201",
        }
        arguments = G3.takeover_arguments(
            "run-1",
            "takeover",
            source,
            target,
        )
        self.assertIn("--holder-unavailable", arguments)
        self.assertNotIn("--lease-expired", arguments)
        self.assertIn("old-boot", arguments)

    def test_g3_recovers_every_shard_the_crashed_boot_held(self) -> None:
        snapshot = {
            "canonical_json": {
                "holders": [
                    {"instance_id": "farm-worker-a", "boot_id": "a-boot"},
                    {"instance_id": "farm-worker-b", "boot_id": "b-boot"},
                    {"instance_id": "farm-worker-b", "boot_id": "b-old-boot"},
                ],
                "farm_worker": {
                    "routes": [
                        {"state": "ACTIVE", "holder_index": 1},
                        {"state": "ACTIVE", "holder_index": 1},
                        {"state": "ACTIVE", "holder_index": 0},
                        {"state": "ASSIGNING", "holder_index": 1},
                        {"state": "ACTIVE", "holder_index": 2},
                    ],
                },
            },
        }
        source = {"holder_id": "farm-worker-b", "holder_boot_id": "b-boot"}
        self.assertEqual(G3.stranded_shards(snapshot, source), [1])

    def test_g3_fence_probe_runs_from_the_published_image(self) -> None:
        arguments = G3.fence_probe_command(
            ["kubectl"],
            "route-controller-1",
            {
                "holder_id": "farm-worker-b",
                "holder_boot_id": "old-boot",
                "assignment_revision": 8,
                "writer_epoch": 9,
                "lease_epoch": 10,
            },
            {
                "holder_id": "farm-worker-a",
                "boot_id": "new-target-boot",
            },
            {
                "realm": "classic-farm-dynamic-local",
                "hash_version": 2,
                "hash_seed": 7,
                "player": {"shard_count": 4},
                "farm": {"shard_count": 4},
            },
        )
        self.assertEqual(
            arguments[:6],
            [
                "kubectl",
                "-n",
                G3.G1.NAMESPACE,
                "exec",
                "route-controller-1",
                "--",
            ],
        )
        self.assertEqual(arguments[6], G3.PROBE_PATH)
        self.assertTrue(G3.PROBE_PATH.startswith("/usr/local/bin/"))
        self.assertNotIn("cp", arguments)
        self.assertNotIn("/tmp", " ".join(arguments))

    def test_g3_preserves_terminal_plan_diagnostics(self) -> None:
        paused = {
            "plan": {
                "phase": "PAUSED",
                "error_code": "TARGET_HOLDER_NOT_READY",
            },
        }
        result = subprocess.CompletedProcess(
            ["routectl", "takeover-after-expiry"],
            1,
            stdout='{"plan":{"phase":"PAUSED",'
            '"error_code":"TARGET_HOLDER_NOT_READY"}}',
            stderr="",
        )
        with mock.patch.object(G3.subprocess, "run", return_value=result):
            self.assertEqual(
                G3.run_plan_ctl(
                    Path("/tmp/routectl"),
                    {},
                    ["takeover-after-expiry"],
                ),
                paused,
            )

    def test_g3_retries_transient_takeover_submission(self) -> None:
        transient = subprocess.CalledProcessError(
            1,
            ["routectl", "takeover-after-expiry"],
            stderr='{"grpc_code":"Aborted"}',
        )
        succeeded = {"plan": {"phase": "SUCCEEDED"}}
        with (
            mock.patch.object(
                G3,
                "run_plan_ctl",
                side_effect=[transient, succeeded],
            ) as run_ctl,
            mock.patch.object(G3.time, "sleep"),
        ):
            self.assertEqual(
                G3.wait_takeover(
                    Path("/tmp/routectl"),
                    {},
                    ["takeover-after-expiry"],
                ),
                succeeded,
            )
        self.assertEqual(run_ctl.call_count, 2)

    def test_g3_source_pod_matches_holder_or_worker_app(self) -> None:
        pods = [
            {"name": "player-svr-0", "app": "player-svr"},
            {"name": "farm-worker-a-abc", "app": "farm-worker-a"},
        ]
        self.assertEqual(
            G3.source_pod(pods, "player-svr-0")["name"],
            "player-svr-0",
        )
        self.assertEqual(
            G3.source_pod(pods, "farm-worker-a")["name"],
            "farm-worker-a-abc",
        )

    def test_g3_restart_rotates_boot_without_replica_overlap(self) -> None:
        with mock.patch.object(G3.subprocess, "run") as run:
            boot_id = G3.crash_source_with_new_boot(
                ["kubectl"],
                "run-1",
                "farm-worker-a",
                "farm-worker-a-old",
            )
        self.assertEqual(
            boot_id,
            "farm-worker-a-run-1-g3-restart",
        )
        commands = [call.args[0] for call in run.call_args_list]
        self.assertIn("--replicas=0", commands[0])
        self.assertIn("--for=delete", commands[1])
        self.assertIn(f"WORKER_BOOT_ID={boot_id}", commands[2])
        self.assertEqual(len(commands), 3)
        self.assertFalse(any("--replicas=1" in command for command in commands))

    def test_g3_old_source_unavailable_after_lease_or_boot_change(self) -> None:
        self.assertTrue(
            G3.old_source_unavailable(
                {"holders": []},
                "farm-worker-b",
                "old-boot",
                1000,
            )
        )
        self.assertFalse(
            G3.old_source_unavailable(
                {
                    "holders": [
                        {
                            "holder_id": "farm-worker-b",
                            "boot_id": "old-boot",
                            "lease_until_ms": 2000,
                        }
                    ]
                },
                "farm-worker-b",
                "old-boot",
                1000,
            )
        )
        self.assertTrue(
            G3.old_source_unavailable(
                {
                    "holders": [
                        {
                            "holder_id": "farm-worker-b",
                            "boot_id": "new-boot",
                            "lease_until_ms": 2000,
                        }
                    ]
                },
                "farm-worker-b",
                "old-boot",
                1000,
            )
        )


if __name__ == "__main__":
    unittest.main()
