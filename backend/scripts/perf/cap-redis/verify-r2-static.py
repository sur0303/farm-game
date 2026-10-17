#!/usr/bin/env python3
import argparse, hashlib, json, pathlib, re, subprocess

DELETED = (
    "internal/friend/store.go", "internal/friend/memory.go", "internal/friend/memory_test.go",
    "internal/friend/redis.go", "internal/friend/redis_live_test.go",
    "internal/friendshipservice/canonical_retirement_test.go",
    "internal/friendshipservice/legacy_metrics.go", "internal/friendshipservice/legacy_mode.go",
    "internal/friendshipservice/legacy_mode_test.go", "internal/friendshipservice/legacy_observer.go",
    "internal/friendshipservice/legacy_observer_test.go", "internal/friendshipservice/redis_legacy_roster.go",
    "internal/friendshipservice/redis_legacy_roster_test.go", "internal/friendshipservice/redis_legacy_roster_live_test.go",
    "internal/gateway/auth/share_profile_test.go", "internal/gateway/auth/share_redis.go",
    "internal/gateway/auth/share_redis_live_test.go", "internal/gateway/auth/share_test.go",
    "scripts/summarize-friend-legacy-metrics",
)
FORBIDDEN = re.compile(
    r"FRIEND_LEGACY_MODE|SMOKE_FRIEND_LEGACY_MODE|FRIEND_STORE_PREFIX|"
    r"LegacyObserver|RedisLegacyRosterReader|friend\.NewRedisStore|friend\.Store"
)


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--business-head", required=True)
    parser.add_argument("--comparison-head", required=True)
    args = parser.parse_args()
    root = pathlib.Path(args.root).resolve()
    head = git(root, "rev-parse", "HEAD")
    subprocess.check_call(["git", "-C", str(root), "merge-base", "--is-ancestor", args.business_head, head])

    present = [name for name in DELETED if (root / name).exists()]
    matches = []
    for base in (root / "cmd", root / "internal", root / "deploy"):
        for path in sorted(item for item in base.rglob("*") if item.is_file()):
            try:
                text = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                continue
            for number, line in enumerate(text.splitlines(), 1):
                if FORBIDDEN.search(line):
                    matches.append({"path": path.relative_to(root).as_posix(), "line": number})
    product_diff = git(root, "diff", "--name-only", f"{args.comparison_head}..{args.business_head}", "--", ".", ":(exclude)docs/**")
    verification_diff=git(root,"diff","--name-only",f"{args.business_head}..{head}")
    allowed_prefixes=("cmd/cap-redis/","scripts/perf/cap-redis/","docs/verification/perf/cap-redis/")
    verification_out_of_scope=[name for name in verification_diff.splitlines() if name and not name.startswith(allowed_prefixes)]
    remote_source = (root / "internal/gatewayapp/friendship.go").read_text(encoding="utf-8")
    checks = {
        "business_head_is_ancestor": True,
        "product_diff_from_comparison_zero": product_diff == "",
        "verification_diff_is_test_only": not verification_out_of_scope,
        "legacy_relationship_files_absent": not present,
        "legacy_relationship_symbols_absent_from_runtime": not matches,
        "remote_player_mode_structurally_present": "ModeRemotePlayer" in remote_source and "return remoteFactory()" in remote_source,
    }
    report = {
        "schema_version": "cap-redis-l1-r2.static-qualification.v1",
        "business_head": args.business_head,
        "verification_head":head,
        "comparison_head": args.comparison_head,
        "product_diff_names": product_diff.splitlines() if product_diff else [],
        "verification_out_of_scope":verification_out_of_scope,
        "deleted_contract_count": len(DELETED),
        "unexpected_present_paths": present,
        "unexpected_runtime_matches": matches,
        "legacy_relationship_read_total": 0 if checks["legacy_relationship_symbols_absent_from_runtime"] else None,
        "legacy_relationship_write_total": 0 if checks["legacy_relationship_symbols_absent_from_runtime"] else None,
        "checks": checks,
        "verdict": "PASS" if all(checks.values()) else "FAIL",
    }
    canonical = json.dumps(report, sort_keys=True, separators=(",", ":")).encode()
    report["evidence_sha256"] = hashlib.sha256(canonical).hexdigest()
    output = pathlib.Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"R2_STATIC_QUALIFICATION={report['verdict']} OUTPUT={output}")
    return 0 if report["verdict"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
