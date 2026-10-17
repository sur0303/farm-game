#!/usr/bin/env python3
import argparse, hashlib, json, pathlib, subprocess, sys


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def output(*args):
    return subprocess.check_output(args, text=True).strip()


def machine_spec():
    cpu_model = ""
    for line in output("lscpu").splitlines():
        if line.startswith("Model name:"):
            cpu_model = line.split(":", 1)[1].strip()
            break
    memory_kib = 0
    for line in pathlib.Path("/proc/meminfo").read_text().splitlines():
        if line.startswith("MemTotal:"):
            memory_kib = int(line.split()[1])
            break
    go_version = output("go", "version").removeprefix("go version ")
    docker_server = output("docker", "version", "--format", "{{.Server.Version}}")
    return {
        "kernel": output("uname", "-srvmo"),
        "cpu_model": cpu_model,
        "logical_cpus": output("nproc"),
        "memory_bytes": str(memory_kib * 1024),
        "go_version": go_version,
        "docker_server": docker_server,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", required=True)
    parser.add_argument("--manifest", required=True)
    args = parser.parse_args()
    profile = json.load(open(args.profile, encoding="utf-8"))
    manifest = json.load(open(args.manifest, encoding="utf-8"))
    expected = {
        "profile_hash": digest(profile),
        "redis_config_hash": digest(profile["redis"]),
        "route_snapshot_hash": digest(profile["route_snapshot"]),
        "lease_profile_hash": digest(profile["lease_profile"]),
    }
    errors = []
    for field, value in expected.items():
        if manifest.get(field) != value:
            errors.append(f"{field} drift: manifest={manifest.get(field)} actual={value}")
    actual_machine = machine_spec()
    if manifest.get("machine_spec") != actual_machine:
        errors.append(f"machine spec drift: manifest={manifest.get('machine_spec')} actual={actual_machine}")
    if manifest.get("machine_spec_hash") != digest(actual_machine):
        errors.append("machine spec hash drift")
    governor_path = pathlib.Path("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
    governor = governor_path.read_text().strip() if governor_path.exists() else "unavailable"
    if manifest.get("cpu_governor") != governor:
        errors.append(f"CPU governor drift: manifest={manifest.get('cpu_governor')} actual={governor}")
    if errors:
        for error in errors:
            print("MANIFEST_PREFLIGHT_ERROR:", error, file=sys.stderr)
        return 1
    print("MANIFEST_PREFLIGHT=PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
