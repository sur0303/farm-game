#!/usr/bin/env python3
import copy
import importlib.machinery
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

SCRIPT = Path(__file__).with_name("verify-dynamic-final-g1")
LOADER = importlib.machinery.SourceFileLoader(
    "verify_dynamic_final_g1",
    str(SCRIPT),
)
SPEC = importlib.util.spec_from_loader(LOADER.name, LOADER)
VERIFY = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = VERIFY
LOADER.exec_module(VERIFY)


class DynamicFinalG1Test(unittest.TestCase):
    def test_active_deadlines_require_all_three_complete_domains(self) -> None:
        route = {
            "holder_index": 0,
            "state": "ACTIVE",
            "shard_revision": 1,
            "expected_writer_epoch": 1,
            "expected_lease_epoch": 1,
            "lease_expire_at_ms": 200,
        }
        snapshot = {
            "canonical_json": {
                domain: {
                    "shard_count": 4,
                    "routes": [dict(route) for _ in range(4)],
                }
                for domain in ("player", "farm", "farm_worker")
            }
        }
        self.assertEqual(len(VERIFY.active_deadlines(snapshot)), 12)
        snapshot["canonical_json"]["farm_worker"]["routes"][0]["state"] = (
            "DRAINING"
        )
        with self.assertRaisesRegex(ValueError, "non-ACTIVE"):
            VERIFY.active_deadlines(snapshot)

    def test_prometheus_scalar_rejects_missing_and_ambiguous_values(self) -> None:
        metrics = (
            "# TYPE route gauge\n"
            "farm_actor_route_snapshot_revision 19\n"
        )
        self.assertEqual(
            VERIFY.prometheus_scalar(
                metrics,
                "farm_actor_route_snapshot_revision",
            ),
            19,
        )
        with self.assertRaisesRegex(ValueError, "missing or ambiguous"):
            VERIFY.prometheus_scalar("", "missing")
        with self.assertRaisesRegex(ValueError, "missing or ambiguous"):
            VERIFY.prometheus_scalar(
                "metric{a=\"1\"} 1\nmetric{a=\"2\"} 2\n",
                "metric",
            )

    @staticmethod
    def coherent_snapshot(revision: int, shard_count: int = 4) -> dict:
        return {
            "revision": revision,
            "canonical_json": {"farm_worker": {"shard_count": shard_count}},
        }

    def test_coherent_requires_final_modes_and_worker_assignment(self) -> None:
        revision = 23
        consumers = {}
        for name, app in (
            ("player-0", "player-svr"),
            ("player-1", "player-svr"),
            ("farm-0", "farm-svr"),
            ("farm-1", "farm-svr"),
            ("worker-a", "farm-worker-a"),
            ("worker-b", "farm-worker-b"),
            ("gate", "farm-gate"),
        ):
            value = {
                "app": app,
                "revision": revision,
                "readyz": {"status": "ready"},
            }
            if app in ("player-svr", "farm-svr"):
                value["readyz"]["route_runtime"] = {
                    "snapshot_revision": revision,
                    "assignment_state": "WATCHING",
                    "assignment_healthy": True,
                    "process_ready": True,
                }
            if app.startswith("farm-worker-"):
                value.update({
                    "worker_assignment_revision": revision,
                    "worker_owned_shards": 2,
                    "worker_assignment_ready": 1,
                    "direct_mode": 1,
                })
                value["readyz"].update({
                    "ready": True,
                    "route_projection_fresh": True,
                })
            if app == "farm-svr":
                value["narrow_mode"] = 1
            consumers[name] = value
        snapshot = self.coherent_snapshot(revision)
        self.assertTrue(VERIFY.coherent(snapshot, consumers))
        consumers["worker-a"]["direct_mode"] = 0
        self.assertFalse(VERIFY.coherent(snapshot, consumers))
        consumers["worker-a"]["direct_mode"] = 1

        # A planned migration leaves the domain unbalanced on purpose.
        consumers["worker-a"]["worker_owned_shards"] = 1
        consumers["worker-b"]["worker_owned_shards"] = 3
        self.assertTrue(VERIFY.coherent(snapshot, consumers))
        consumers["worker-b"]["worker_owned_shards"] = 2
        self.assertFalse(VERIFY.coherent(snapshot, consumers))

    def test_restart_observed_requires_transport_resync_and_revision_advance(
        self,
    ) -> None:
        before = {
            "player": {
                "app": "player-svr",
                "revision": 7,
                "readyz": {
                    "route_runtime": {
                        "assignment_reconnects": 1,
                        "assignment_resyncs": 1,
                    },
                },
            },
            "farm": {
                "app": "farm-svr",
                "revision": 7,
                "readyz": {
                    "route_runtime": {
                        "assignment_reconnects": 1,
                        "assignment_resyncs": 1,
                    },
                },
            },
            "worker": {
                "app": "farm-worker-a",
                "revision": 7,
                "readyz": {},
                "worker_remote_reconnects": 1,
                "worker_remote_resyncs": 1,
            },
            "gate": {
                "app": "farm-gate",
                "revision": 7,
                "readyz": {},
            },
        }
        after = copy.deepcopy(before)
        for value in after.values():
            value["revision"] = 8
        for name in ("player", "farm"):
            route_runtime = after[name]["readyz"]["route_runtime"]
            route_runtime["assignment_reconnects"] = 2
            route_runtime["assignment_resyncs"] = 2
        after["worker"]["worker_remote_reconnects"] = 2
        after["worker"]["worker_remote_resyncs"] = 2

        self.assertTrue(VERIFY.restart_observed(before, after))
        after["worker"]["worker_remote_resyncs"] = 1
        self.assertFalse(VERIFY.restart_observed(before, after))
        after["worker"]["worker_remote_resyncs"] = 2
        after["gate"]["revision"] = 7
        self.assertFalse(VERIFY.restart_observed(before, after))

    def test_collect_coherent_retries_transient_not_ready(self) -> None:
        snapshots = [{"revision": 11}, {"revision": 11}]
        with (
            mock.patch.object(VERIFY, "command_json", return_value={"items": []}),
            mock.patch.object(VERIFY, "pod_observations", return_value=[]),
            mock.patch.object(
                VERIFY,
                "require_healthy_pods",
                side_effect=[ValueError("transient not ready"), None],
            ),
            mock.patch.object(VERIFY, "run_ctl", side_effect=snapshots),
            mock.patch.object(
                VERIFY,
                "consumer_observation",
                return_value={"consumer": {"revision": 11}},
            ),
            mock.patch.object(VERIFY, "coherent", return_value=True),
            mock.patch.object(VERIFY.time, "sleep"),
        ):
            snapshot, consumers, pods = VERIFY.collect_coherent(
                [],
                Path("/tmp/routectl"),
                {},
                attempts=2,
            )
        self.assertEqual(snapshot["revision"], 11)
        self.assertEqual(consumers["consumer"]["revision"], 11)
        self.assertEqual(pods, [])

    def test_collect_coherent_waits_for_revision_after_restart(self) -> None:
        snapshots = [
            {"revision": 11},
            {"revision": 11},
            {"revision": 12},
            {"revision": 12},
        ]
        consumers = [
            {"consumer": {"revision": 11}},
            {"consumer": {"revision": 12}},
        ]
        with (
            mock.patch.object(VERIFY, "command_json", return_value={"items": []}),
            mock.patch.object(VERIFY, "pod_observations", return_value=[]),
            mock.patch.object(VERIFY, "require_healthy_pods"),
            mock.patch.object(VERIFY, "run_ctl", side_effect=snapshots),
            mock.patch.object(
                VERIFY,
                "consumer_observation",
                side_effect=consumers,
            ),
            mock.patch.object(VERIFY, "coherent", return_value=True),
            mock.patch.object(VERIFY.time, "sleep"),
        ):
            snapshot, observed, pods = VERIFY.collect_coherent(
                [],
                Path("/tmp/routectl"),
                {},
                attempts=2,
                minimum_revision=11,
            )
        self.assertEqual(snapshot["revision"], 12)
        self.assertEqual(observed["consumer"]["revision"], 12)
        self.assertEqual(pods, [])

    def test_write_exclusive_is_immutable(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "receipt.json"
            VERIFY.write_exclusive(path, {"status": "PASS"})
            with self.assertRaises(FileExistsError):
                VERIFY.write_exclusive(path, {"status": "OVERWRITE"})


if __name__ == "__main__":
    unittest.main()
