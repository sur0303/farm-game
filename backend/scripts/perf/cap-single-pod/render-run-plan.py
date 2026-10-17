#!/usr/bin/env python3
"""Render a non-executable run-plan template from a typed JSON values file."""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
import sys

PLACEHOLDER = re.compile(r"^\$\{([A-Z0-9_]+)\}$")


def render(value: object, replacements: dict[str, object], used: set[str]) -> object:
    if isinstance(value, dict):
        return {key: render(item, replacements, used) for key, item in value.items()}
    if isinstance(value, list):
        return [render(item, replacements, used) for item in value]
    if isinstance(value, str):
        match = PLACEHOLDER.fullmatch(value)
        if match:
            name = match.group(1)
            if name not in replacements:
                raise ValueError(f"missing replacement {name}")
            used.add(name)
            return replacements[name]
        if "${" in value:
            raise ValueError("partial placeholders are forbidden")
    return value


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--template", required=True, type=pathlib.Path)
    parser.add_argument("--values", required=True, type=pathlib.Path)
    parser.add_argument("--out", required=True, type=pathlib.Path)
    args = parser.parse_args()
    if not all(path.is_absolute() for path in (args.template, args.values, args.out)):
        parser.error("all paths must be absolute")
    if args.out.exists():
        print("output already exists", file=sys.stderr)
        return 2
    try:
        template = json.loads(args.template.read_text(encoding="utf-8"))
        replacements = json.loads(args.values.read_text(encoding="utf-8"))
        if not isinstance(replacements, dict):
            raise ValueError("values must be an object")
        used: set[str] = set()
        document = render(template, replacements, used)
        unused = sorted(set(replacements) - used)
        if unused:
            raise ValueError("unused replacements: " + ",".join(unused))
        encoded = json.dumps(document, indent=2, sort_keys=False) + "\n"
        args.out.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(args.out, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(encoded)
    except (OSError, json.JSONDecodeError, ValueError) as error:
        print(f"RUN_PLAN_RENDER=FAIL class={type(error).__name__}", file=sys.stderr)
        return 2
    print(f"RUN_PLAN_RENDER=PASS out={args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
