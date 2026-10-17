#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path


EXPECTED_DEPLOYMENTS = {"farm-worker-a", "farm-worker-b"}


def extract_worker_deployments(manifest: dict) -> dict:
    if manifest.get("apiVersion") != "v1" or manifest.get("kind") != "List":
        raise ValueError("business manifest must be a Kubernetes v1 List")
    items = manifest.get("items")
    if not isinstance(items, list):
        raise ValueError("business manifest items must be a list")

    selected = [
        item
        for item in items
        if item.get("kind") == "Deployment"
        and item.get("metadata", {}).get("name") in EXPECTED_DEPLOYMENTS
    ]
    names = [item.get("metadata", {}).get("name") for item in selected]
    if len(selected) != len(EXPECTED_DEPLOYMENTS) or set(names) != EXPECTED_DEPLOYMENTS:
        raise ValueError("business manifest must contain exactly both FARM_WORKER deployments")

    return {"apiVersion": "v1", "kind": "List", "items": selected}


def main() -> int:
    if len(sys.argv) != 2:
        raise SystemExit(f"usage: {Path(sys.argv[0]).name} <business-manifest.json>")
    try:
        manifest = json.loads(Path(sys.argv[1]).read_text())
        output = extract_worker_deployments(manifest)
    except (OSError, json.JSONDecodeError, ValueError) as error:
        raise SystemExit(f"extract_dynamic_worker_route_holders: {error}") from error
    json.dump(output, sys.stdout, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
