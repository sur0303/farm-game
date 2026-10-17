#!/usr/bin/env python3
"""Fail-closed local JSON Schema runner for CAP-SINGLE-POD evidence inputs."""

from __future__ import annotations

import argparse
import json
import pathlib
import sys

from jsonschema import Draft202012Validator, FormatChecker


def load_json(path: pathlib.Path) -> object:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", required=True, type=pathlib.Path)
    parser.add_argument("--document", required=True, type=pathlib.Path)
    args = parser.parse_args()

    if not args.schema.is_absolute() or not args.document.is_absolute():
        parser.error("schema and document paths must be absolute")

    try:
        schema = load_json(args.schema)
        document = load_json(args.document)
        Draft202012Validator.check_schema(schema)
        validator = Draft202012Validator(schema, format_checker=FormatChecker())
        errors = sorted(validator.iter_errors(document), key=lambda item: tuple(str(part) for part in item.absolute_path))
    except (OSError, json.JSONDecodeError) as error:
        print(f"SCHEMA_VALIDATION=FAIL class=INPUT_ERROR detail={type(error).__name__}", file=sys.stderr)
        return 2
    except Exception as error:  # schema implementation failures are never a pass
        print(f"SCHEMA_VALIDATION=FAIL class=SCHEMA_ERROR detail={type(error).__name__}", file=sys.stderr)
        return 2

    if errors:
        for error in errors[:50]:
            location = "/".join(str(part) for part in error.absolute_path) or "$"
            print(f"SCHEMA_VIOLATION path={location} validator={error.validator}", file=sys.stderr)
        if len(errors) > 50:
            print(f"SCHEMA_VIOLATION additional={len(errors) - 50}", file=sys.stderr)
        print("SCHEMA_VALIDATION=FAIL", file=sys.stderr)
        return 1

    print(f"SCHEMA_VALIDATION=PASS schema={args.schema.name} document={args.document.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
