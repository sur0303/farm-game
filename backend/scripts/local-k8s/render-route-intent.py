#!/usr/bin/env python3
"""Render a boot-generation-aware local RouteSnapshot intent.

This tool never contacts Redis and never claims that writer/lease epochs exist.
Its output is an input to cap-static-route-init, not an activatable authority
receipt.  The explicit status prevents a rendered file from being promoted.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import secrets
import tempfile
from pathlib import Path

PROFILES = {"functional", "l2-gate", "l2-player", "l2-farm", "l2-worker", "mixed"}
NONCE_RE = re.compile(r"[a-z0-9]{20,40}")


def holder_index(profile: str, domain: str, shard: int) -> int:
    if domain == "player":
        return 0 if profile == "l2-player" else shard % 2
    return 2 if profile == "l2-farm" else 2 + shard % 2


def atomic_write(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(value, sort_keys=True, indent=2).encode() + b"\n"
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", required=True, choices=sorted(PROFILES))
    parser.add_argument("--generation", required=True, type=int)
    parser.add_argument("--nonce", help="DNS-safe >=96-bit controller nonce; generated when omitted")
    parser.add_argument("--shard-count", type=int, default=1024)
    parser.add_argument("--namespace", default="farm-l2-r2")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--receipt", type=Path)
    args = parser.parse_args()

    if args.generation <= 0:
        parser.error("--generation must be positive")
    if args.shard_count not in (4, 1024):
        parser.error("only render-only 4 or frozen local 1024 shard profiles are accepted")
    nonce = args.nonce or secrets.token_hex(12)
    if not NONCE_RE.fullmatch(nonce):
        parser.error("--nonce must be 20..40 lowercase alphanumeric characters")

    holders = []
    for name, service, port in (
        ("player-svr-0", "player-svr-headless", 9100),
        ("player-svr-1", "player-svr-headless", 9100),
        ("farm-svr-0", "farm-svr-headless", 9090),
        ("farm-svr-1", "farm-svr-headless", 9090),
    ):
        boot = f"{name}-{args.profile}-g{args.generation}-{nonce}"
        if len(boot) > 253:
            parser.error("generated boot ID is too long")
        holders.append({
            "instance_id": name,
            "boot_id": boot,
            "endpoint": f"{name}.{service}.{args.namespace}.svc.cluster.local:{port}",
            "state": "ACTIVE",
            "weight": 1,
        })

    def domain(name: str) -> dict[str, object]:
        return {
            "shard_count": args.shard_count,
            "routes": [
                {
                    "holder_index": holder_index(args.profile, name, shard),
                    "state": "ACTIVE",
                    "shard_revision": 0,
                    "expected_writer_epoch": 0,
                    "expected_lease_epoch": 0,
                    "lease_expire_at_ms": 0,
                }
                for shard in range(args.shard_count)
            ],
        }

    snapshot = {
        "realm": "classic-farm-local-l2-r2",
        "global_revision": 0,
        "hash_version": 2,
        "hash_seed": 202608030001,
        "player": domain("player"),
        "farm": domain("farm"),
        "holders": holders,
    }
    atomic_write(args.output, snapshot)
    snapshot_sha = hashlib.sha256(args.output.read_bytes()).hexdigest()
    receipt_path = args.receipt or args.output.with_suffix(args.output.suffix + ".receipt.json")
    receipt = {
        "schema_version": "farm-local-route-intent-receipt.v1",
        "status": "PLACEMENT_INTENT_NOT_AUTHORITY_INITIALIZED",
        "profile": args.profile,
        "generation": args.generation,
        "shard_count": args.shard_count,
        "route_intent_sha256": snapshot_sha,
        "holder_boot_ids": [holder["boot_id"] for holder in holders],
        "company_redis_contacted": False,
        "activatable": False,
        "formal_l2_authorized": False,
    }
    atomic_write(receipt_path, receipt)
    print(json.dumps(receipt, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
