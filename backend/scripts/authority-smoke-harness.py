#!/usr/bin/env python3
"""Fail-closed test-harness helpers for authority vertical smoke runs."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import sys
import tempfile
import time
from typing import Any, Callable, Sequence


DEFAULT_PORT_COUNT = 10
MAX_PORT_COUNT = 64
MIN_TCP_PORT = 1024
MAX_TCP_PORT = 65535

# CP-B is a bounded single-browser run. Keep the test-only static snapshot
# comfortably beyond its gate, but reject accidental unbounded configuration.
DEFAULT_ROUTE_HORIZON_SECONDS = 3600
MIN_ROUTE_HORIZON_SECONDS = 900
MAX_ROUTE_HORIZON_SECONDS = 3600
ROUTE_DOMAINS = ("player", "farm")


class HarnessContractError(ValueError):
    """The smoke harness input cannot be used without weakening the test."""


def _strict_positive_decimal(value: str, label: str) -> int:
    if not isinstance(value, str) or re.fullmatch(r"[1-9][0-9]*", value) is None:
        raise HarnessContractError(f"{label} must be a non-empty positive decimal integer")
    return int(value)


def validate_port_count(value: str | int) -> int:
    count = value if type(value) is int else _strict_positive_decimal(value, "port count")
    if type(count) is not int or not 1 <= count <= MAX_PORT_COUNT:
        raise HarnessContractError(f"port count must be in [1,{MAX_PORT_COUNT}]")
    return count


def validate_ports(ports: Sequence[int], expected_count: int) -> list[int]:
    expected = validate_port_count(expected_count)
    normalized = list(ports)
    if len(normalized) != expected:
        raise HarnessContractError(f"allocated {len(normalized)} ports, expected {expected}")
    for index, port in enumerate(normalized):
        if type(port) is not int or not MIN_TCP_PORT <= port <= MAX_TCP_PORT:
            raise HarnessContractError(
                f"port[{index}] must be an integer in [{MIN_TCP_PORT},{MAX_TCP_PORT}]"
            )
    if len(set(normalized)) != expected:
        raise HarnessContractError("duplicate endpoint port allocation rejected")
    return normalized


def allocate_bound_ports(
    count: int,
    socket_factory: Callable[..., Any] = socket.socket,
) -> list[int]:
    """Select a finite port set while retaining every socket until validation."""

    expected = validate_port_count(count)
    reservations: list[Any] = []
    try:
        for _ in range(expected):
            reservation = socket_factory(socket.AF_INET, socket.SOCK_STREAM)
            reservations.append(reservation)
            reservation.bind(("127.0.0.1", 0))
        ports = [reservation.getsockname()[1] for reservation in reservations]
        return validate_ports(ports, expected)
    finally:
        for reservation in reservations:
            reservation.close()


def validate_route_horizon(value: str | int) -> int:
    horizon = value if type(value) is int else _strict_positive_decimal(value, "route horizon")
    if type(horizon) is not int or not MIN_ROUTE_HORIZON_SECONDS <= horizon <= MAX_ROUTE_HORIZON_SECONDS:
        raise HarnessContractError(
            "route horizon seconds must be in "
            f"[{MIN_ROUTE_HORIZON_SECONDS},{MAX_ROUTE_HORIZON_SECONDS}]"
        )
    return horizon


def _positive_int(value: Any, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise HarnessContractError(f"{label} must be a positive integer")
    return value


def _non_empty_string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise HarnessContractError(f"{label} must be a non-empty string")
    return value


def validate_route_snapshot(snapshot: Any, now_ms: int) -> list[int]:
    """Validate the fixture shape and return every current route deadline."""

    _positive_int(now_ms, "current time")
    if not isinstance(snapshot, dict):
        raise HarnessContractError("route snapshot must be a JSON object")
    _non_empty_string(snapshot.get("realm"), "route snapshot realm")
    _positive_int(snapshot.get("global_revision"), "global_revision")
    _positive_int(snapshot.get("hash_version"), "hash_version")
    _positive_int(snapshot.get("hash_seed"), "hash_seed")

    holders = snapshot.get("holders")
    if not isinstance(holders, list) or not holders:
        raise HarnessContractError("route snapshot holders must be a non-empty array")
    holder_identities: set[tuple[str, str]] = set()
    for index, holder in enumerate(holders):
        if not isinstance(holder, dict):
            raise HarnessContractError(f"holders[{index}] must be an object")
        identity = (
            _non_empty_string(holder.get("instance_id"), f"holders[{index}].instance_id"),
            _non_empty_string(holder.get("boot_id"), f"holders[{index}].boot_id"),
        )
        if identity in holder_identities:
            raise HarnessContractError(f"holders[{index}] duplicates holder identity")
        holder_identities.add(identity)
        _non_empty_string(holder.get("endpoint"), f"holders[{index}].endpoint")
        if holder.get("state") != "ACTIVE":
            raise HarnessContractError(f"holders[{index}].state must be ACTIVE")
        _positive_int(holder.get("weight"), f"holders[{index}].weight")

    deadlines: list[int] = []
    holder_indices_by_domain: dict[str, set[int]] = {}
    for domain in ROUTE_DOMAINS:
        domain_snapshot = snapshot.get(domain)
        if not isinstance(domain_snapshot, dict):
            raise HarnessContractError(f"{domain} route snapshot must be an object")
        shard_count = _positive_int(domain_snapshot.get("shard_count"), f"{domain}.shard_count")
        routes = domain_snapshot.get("routes")
        if not isinstance(routes, list) or len(routes) != shard_count:
            raise HarnessContractError(f"{domain}.routes length must equal shard_count")
        holder_indices_by_domain[domain] = set()
        for shard_index, route in enumerate(routes):
            label = f"{domain}.routes[{shard_index}]"
            if not isinstance(route, dict):
                raise HarnessContractError(f"{label} must be an object")
            holder_index = route.get("holder_index")
            if type(holder_index) is not int or not 0 <= holder_index < len(holders):
                raise HarnessContractError(f"{label}.holder_index is out of range")
            holder_indices_by_domain[domain].add(holder_index)
            if route.get("state") != "ACTIVE":
                raise HarnessContractError(f"{label}.state must be ACTIVE")
            _positive_int(route.get("shard_revision"), f"{label}.shard_revision")
            _positive_int(route.get("expected_writer_epoch"), f"{label}.expected_writer_epoch")
            _positive_int(route.get("expected_lease_epoch"), f"{label}.expected_lease_epoch")
            deadline = _positive_int(route.get("lease_expire_at_ms"), f"{label}.lease_expire_at_ms")
            if deadline <= now_ms:
                raise HarnessContractError(f"{label}.lease_expire_at_ms is already expired")
            deadlines.append(deadline)

    if holder_indices_by_domain["player"] & holder_indices_by_domain["farm"]:
        raise HarnessContractError("Player and Farm routes must not share holder indices")
    if not deadlines:
        raise HarnessContractError("route snapshot contains no Player/Farm routes")
    return deadlines


def _encoded_json(value: Any) -> bytes:
    return (json.dumps(value, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(content)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)


def refresh_route_snapshot(
    route_path: Path,
    witness_path: Path,
    horizon_seconds: str | int,
    *,
    now_ms: int | None = None,
) -> dict[str, Any]:
    """Rewrite only route deadlines and emit a non-secret deterministic witness."""

    horizon = validate_route_horizon(horizon_seconds)
    observed_now_ms = int(time.time() * 1000) if now_ms is None else now_ms
    _positive_int(observed_now_ms, "current time")
    if route_path.resolve() == witness_path.resolve():
        raise HarnessContractError("route snapshot and horizon witness paths must differ")
    try:
        snapshot = json.loads(route_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise HarnessContractError(f"cannot load route snapshot: {error}") from error

    original = copy.deepcopy(snapshot)
    original_deadlines = validate_route_snapshot(snapshot, observed_now_ms)
    deadline_ms = observed_now_ms + horizon * 1000
    for domain in ROUTE_DOMAINS:
        for route in snapshot[domain]["routes"]:
            route["lease_expire_at_ms"] = deadline_ms
    validate_route_snapshot(snapshot, observed_now_ms)

    # Assert internally that holder identity, route/lease/writer epochs, route
    # revision, topology, and every other field were untouched.
    comparison = copy.deepcopy(snapshot)
    for domain in ROUTE_DOMAINS:
        for index, route in enumerate(comparison[domain]["routes"]):
            route["lease_expire_at_ms"] = original[domain]["routes"][index]["lease_expire_at_ms"]
    if comparison != original:
        raise HarnessContractError("route horizon refresh changed protected route fields")

    route_content = _encoded_json(snapshot)
    witness = {
        "schema": "farm.test.authority-static-route-horizon/v1",
        "scope": "test-only-static-route-snapshot",
        "route_snapshot": route_path.name,
        "refreshed_at_ms": observed_now_ms,
        "route_horizon_seconds": horizon,
        "route_deadline_ms": deadline_ms,
        "original_min_deadline_ms": min(original_deadlines),
        "original_max_deadline_ms": max(original_deadlines),
        "route_count": len(original_deadlines),
        "domains": list(ROUTE_DOMAINS),
        "route_snapshot_sha256": hashlib.sha256(route_content).hexdigest(),
    }
    _atomic_write(route_path, route_content)
    _atomic_write(witness_path, _encoded_json(witness))
    return witness


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    ports = subparsers.add_parser("allocate-ports", help="allocate one bounded distinct port set")
    ports.add_argument("--count", default=str(DEFAULT_PORT_COUNT))

    horizon = subparsers.add_parser("validate-horizon", help="validate a bounded route horizon")
    horizon.add_argument("--horizon-seconds", required=True)

    refresh = subparsers.add_parser("refresh-route", help="refresh a test-only static route deadline")
    refresh.add_argument("--route", type=Path, required=True)
    refresh.add_argument("--witness", type=Path, required=True)
    refresh.add_argument("--horizon-seconds", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "allocate-ports":
            count = validate_port_count(args.count)
            # stdout is intentionally machine-only: one decimal port per line.
            print("\n".join(str(port) for port in allocate_bound_ports(count)))
            return 0
        if args.command == "validate-horizon":
            print(validate_route_horizon(args.horizon_seconds))
            return 0
        witness = refresh_route_snapshot(args.route, args.witness, args.horizon_seconds)
        print(witness["route_deadline_ms"])
        return 0
    except (HarnessContractError, OSError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
