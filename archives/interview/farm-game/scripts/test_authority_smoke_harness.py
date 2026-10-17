#!/usr/bin/env python3
"""Focused regressions for the authority/CP-B smoke harness boundary."""

from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import socket
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "authority_smoke_harness", ROOT / "scripts/authority-smoke-harness.py"
)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)

FIXED_NOW_MS = 1_786_345_184_368
OLD_FIXTURE_DEADLINE_MS = FIXED_NOW_MS + 55_000
RUN09_POST_LOSS_RECONNECT_MS = 1_786_345_240_000


def route_snapshot(deadline_ms: int = OLD_FIXTURE_DEADLINE_MS) -> dict:
    return {
        "realm": "smoke-authority",
        "global_revision": 1,
        "hash_version": 2,
        "hash_seed": 202608060001,
        "player": {
            "shard_count": 1,
            "routes": [
                {
                    "holder_index": 0,
                    "state": "ACTIVE",
                    "shard_revision": 1,
                    "expected_writer_epoch": 1,
                    "expected_lease_epoch": 1,
                    "lease_expire_at_ms": deadline_ms,
                }
            ],
        },
        "farm": {
            "shard_count": 1,
            "routes": [
                {
                    "holder_index": 1,
                    "state": "ACTIVE",
                    "shard_revision": 1,
                    "expected_writer_epoch": 1,
                    "expected_lease_epoch": 1,
                    "lease_expire_at_ms": deadline_ms,
                }
            ],
        },
        "holders": [
            {
                "instance_id": "smoke-player",
                "boot_id": "smoke-player-run",
                "endpoint": "127.0.0.1:41001",
                "state": "ACTIVE",
                "weight": 1,
            },
            {
                "instance_id": "smoke-farm",
                "boot_id": "smoke-farm-run",
                "endpoint": "127.0.0.1:41002",
                "state": "ACTIVE",
                "weight": 1,
            },
        ],
    }


class FakeSocket:
    def __init__(self, owner: "SocketFactory", port: int) -> None:
        self.owner = owner
        self.port = port
        self.bound = False
        self.closed = False

    def bind(self, address: tuple[str, int]) -> None:
        if address != ("127.0.0.1", 0):
            raise AssertionError(f"unexpected bind address: {address!r}")
        if any(sock.closed for sock in self.owner.sockets if sock is not self):
            raise AssertionError("an earlier reservation closed before allocation completed")
        self.bound = True

    def getsockname(self) -> tuple[str, int]:
        if not self.bound:
            raise AssertionError("getsockname before bind")
        return ("127.0.0.1", self.port)

    def close(self) -> None:
        self.closed = True


class SocketFactory:
    def __init__(self, ports: list[int]) -> None:
        self.ports = iter(ports)
        self.sockets: list[FakeSocket] = []

    def __call__(self, family: int, kind: int) -> FakeSocket:
        if family != socket.AF_INET or kind != socket.SOCK_STREAM:
            raise AssertionError("allocator did not request an IPv4 TCP socket")
        result = FakeSocket(self, next(self.ports))
        self.sockets.append(result)
        return result


class PortAllocationTests(unittest.TestCase):
    def test_single_bounded_allocation_retains_and_releases_distinct_ports(self) -> None:
        expected = list(range(41000, 41010))
        factory = SocketFactory(expected)
        self.assertEqual(MODULE.allocate_bound_ports(10, factory), expected)
        self.assertEqual(len(factory.sockets), 10)
        self.assertTrue(all(sock.closed for sock in factory.sockets))

    def test_duplicate_endpoint_assignment_is_rejected_and_released(self) -> None:
        factory = SocketFactory([42000] * 10)
        with self.assertRaisesRegex(MODULE.HarnessContractError, "duplicate"):
            MODULE.allocate_bound_ports(10, factory)
        self.assertTrue(all(sock.closed for sock in factory.sockets))

    def test_real_allocator_returns_valid_unique_endpoint_set(self) -> None:
        ports = MODULE.allocate_bound_ports(MODULE.DEFAULT_PORT_COUNT)
        self.assertEqual(MODULE.validate_ports(ports, MODULE.DEFAULT_PORT_COUNT), ports)
        self.assertEqual(len(set(ports)), MODULE.DEFAULT_PORT_COUNT)


class RouteHorizonTests(unittest.TestCase):
    def test_refresh_rewrites_only_all_player_farm_deadlines_and_writes_witness(self) -> None:
        original = route_snapshot()
        with tempfile.TemporaryDirectory(prefix="authority-route-horizon-") as temporary:
            route_path = Path(temporary) / "route-snapshot.json"
            witness_path = Path(temporary) / "route-horizon.json"
            route_path.write_text(json.dumps(original), encoding="utf-8")
            witness = MODULE.refresh_route_snapshot(
                route_path,
                witness_path,
                MODULE.DEFAULT_ROUTE_HORIZON_SECONDS,
                now_ms=FIXED_NOW_MS,
            )
            refreshed = json.loads(route_path.read_text(encoding="utf-8"))
            deadline = FIXED_NOW_MS + MODULE.DEFAULT_ROUTE_HORIZON_SECONDS * 1000
            self.assertEqual(witness["route_deadline_ms"], deadline)
            self.assertEqual(witness["route_horizon_seconds"], 3600)
            self.assertEqual(witness["route_count"], 2)
            self.assertEqual(witness["domains"], ["player", "farm"])
            self.assertEqual(witness, json.loads(witness_path.read_text(encoding="utf-8")))
            for domain in MODULE.ROUTE_DOMAINS:
                self.assertEqual(refreshed[domain]["routes"][0]["lease_expire_at_ms"], deadline)

            protected_refreshed = copy.deepcopy(refreshed)
            for domain in MODULE.ROUTE_DOMAINS:
                protected_refreshed[domain]["routes"][0]["lease_expire_at_ms"] = (
                    original[domain]["routes"][0]["lease_expire_at_ms"]
                )
            self.assertEqual(protected_refreshed, original)
            serialized_witness = json.dumps(witness).lower()
            for forbidden in ("redis://", "password", "cookie", "token", "authorization"):
                self.assertNotIn(forbidden, serialized_witness)

    def test_old_55_second_fixture_expires_inside_cpb_gate_but_refresh_does_not(self) -> None:
        cpb_gate_deadline = FIXED_NOW_MS + MODULE.MIN_ROUTE_HORIZON_SECONDS * 1000
        self.assertLess(OLD_FIXTURE_DEADLINE_MS, RUN09_POST_LOSS_RECONNECT_MS)
        self.assertLess(OLD_FIXTURE_DEADLINE_MS, cpb_gate_deadline)
        refreshed_deadline = FIXED_NOW_MS + MODULE.DEFAULT_ROUTE_HORIZON_SECONDS * 1000
        self.assertGreater(refreshed_deadline, RUN09_POST_LOSS_RECONNECT_MS)
        self.assertGreaterEqual(refreshed_deadline, cpb_gate_deadline)

    def test_empty_malformed_expired_or_structurally_invalid_snapshot_fails_closed(self) -> None:
        invalid_documents: list[tuple[str, str | dict]] = [
            ("empty", ""),
            ("malformed", "{"),
            ("not-object", []),
            ("missing-shape", {}),
            ("expired", route_snapshot(FIXED_NOW_MS)),
        ]
        empty_routes = route_snapshot()
        empty_routes["player"] = {"shard_count": 1, "routes": []}
        invalid_documents.append(("empty-routes", empty_routes))
        shared_holder = route_snapshot()
        shared_holder["farm"]["routes"][0]["holder_index"] = 0
        invalid_documents.append(("shared-holder", shared_holder))

        with tempfile.TemporaryDirectory(prefix="authority-route-invalid-") as temporary:
            for label, document in invalid_documents:
                with self.subTest(label=label):
                    route_path = Path(temporary) / f"{label}.json"
                    witness_path = Path(temporary) / f"{label}.witness.json"
                    if isinstance(document, str):
                        route_path.write_text(document, encoding="utf-8")
                    else:
                        route_path.write_text(json.dumps(document), encoding="utf-8")
                    with self.assertRaises(MODULE.HarnessContractError):
                        MODULE.refresh_route_snapshot(
                            route_path,
                            witness_path,
                            3600,
                            now_ms=FIXED_NOW_MS,
                        )
                    self.assertFalse(witness_path.exists())

    def test_empty_malformed_or_out_of_bounds_horizon_fails_closed(self) -> None:
        for value in ("", "abc", "0", "055", "899", "3601", -1, True):
            with self.subTest(value=value):
                with self.assertRaises(MODULE.HarnessContractError):
                    MODULE.validate_route_horizon(value)

    def test_refresh_is_ordered_after_fixture_init_and_before_first_service(self) -> None:
        smoke_script = (ROOT / "scripts/run-authority-vertical-smoke").read_text(encoding="utf-8")
        fixture_position = smoke_script.index('"$BIN/authority-smoke-fixture" init')
        refresh_position = smoke_script.index('"$HARNESS_HELPER" refresh-route')
        service_position = smoke_script.index("# --- 6. Start services")
        self.assertLess(fixture_position, refresh_position)
        self.assertLess(refresh_position, service_position)
        self.assertNotIn("pick_port()", smoke_script)
        self.assertIn('allocate-ports --count "$PORT_COUNT"', smoke_script)
        self.assertIn('"ACTOR_AUTHORITY_LEASE_TTL=55s"', smoke_script)
        self.assertIn('ROUTE_HORIZON_WITNESS="$OUT/route-horizon.json"', smoke_script)

        theft_script = (ROOT / "scripts/run-theft-200-wss-hot-farm").read_text(encoding="utf-8")
        redis_start_position = theft_script.index('docker run -d --rm --name "$REDIS_CONTAINER"')
        redis_port_position = theft_script.index('redis_port=$(docker port "$REDIS_CONTAINER"')
        redis_ready_position = theft_script.index('docker exec "$REDIS_CONTAINER" redis-cli ping')
        theft_allocate_position = theft_script.index('allocate-ports --count "$port_count"')
        authority_position = theft_script.index('"$BUILD_DIR/theft-hot-audit" -mode authority')
        theft_refresh_position = theft_script.index('"$HARNESS_HELPER" refresh-route')
        first_service_position = theft_script.index('start_process "${case_prefix}-player.log"')
        driver_wait_position = theft_script.index('wait "$driver_pid" || driver_status=$?')
        due_zero_position = theft_script.index('wait_worker_pair_due_zero "http://127.0.0.1:${worker_health}/readyz"')
        final_metrics_position = theft_script.index('curl -fsS "http://127.0.0.1:${local_port}/metrics" >"${case_prefix}-${local_name}-metrics.prom"')
        self.assertLess(redis_start_position, redis_port_position)
        self.assertLess(redis_port_position, redis_ready_position)
        self.assertLess(redis_ready_position, theft_allocate_position)
        self.assertLess(theft_allocate_position, authority_position)
        self.assertLess(authority_position, theft_refresh_position)
        self.assertLess(theft_refresh_position, first_service_position)
        self.assertLess(driver_wait_position, due_zero_position)
        self.assertLess(due_zero_position, final_metrics_position)
        self.assertNotIn("pick_port()", theft_script)
        self.assertIn('allocate-ports --count "$port_count"', theft_script)
        self.assertIn("declare -A seen_endpoint_ports=()", theft_script)
        self.assertIn("duplicate theft hot endpoint port", theft_script)
        self.assertIn("theft hot endpoint port count mismatch", theft_script)
        self.assertIn('THEFT_HOT_ROUTE_HORIZON_SECONDS', theft_script)
        self.assertIn('THEFT_HOT_DRAIN_TIMEOUT_SECONDS', theft_script)
        self.assertIn('for name in ("CARE", "THEFT")', theft_script)
        self.assertIn('item.get("due_depth_available") is not True', theft_script)
        self.assertIn('item.get("due_depth") != 0', theft_script)
        self.assertIn('--witness "$route_horizon_witness"', theft_script)
        for mapping in (
            'local player_port="${endpoint_ports[0]}"',
            'local farm_port="${endpoint_ports[1]}"',
            'local player_health="${endpoint_ports[2]}"',
            'local farm_health="${endpoint_ports[3]}"',
            'local worker_health="${endpoint_ports[4]}"',
            'local gate_http="${endpoint_ports[5]}"',
            'local gate_downlink="${endpoint_ports[6]}"',
            'local gate_control="${endpoint_ports[7]}"',
            'local wss_port="${endpoint_ports[8]}"',
        ):
            self.assertIn(mapping, theft_script)
        for preserved in (
            "-visitors 199",
            "-conns 200 -rate 200 -duration 45s",
            '"ACTOR_AUTHORITY_LEASE_TTL=55s"',
            '-theft-slow-reader="$slow_reader"',
            "-theft-expected-changes 10",
            "-theft-terminal-timeout 25s",
            "-theft-poll-interval 50ms",
            "-theft-retry=true",
            "-mode prepare",
            "-mode audit",
            '-tls-ca "$ca_file"',
            "--internal-certs",
        ):
            self.assertIn(preserved, theft_script)


if __name__ == "__main__":
    unittest.main()
