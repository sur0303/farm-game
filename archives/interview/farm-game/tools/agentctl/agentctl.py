#!/usr/bin/env python3
"""AC1 Agent Controller (multi-model CP1).

This module intentionally uses only the Python standard library.  It is a
small controller for long-lived tmux + multi-runner tasks; it is not part of
the farm game runtime and does not know anything about the game's protocol.

CP1 keeps the Codex CLI path working while introducing Profile resolution,
RunnerAdapter, and generic runner session fields so Cursor/CodeBuddy runners
can land in later checkpoints without breaking legacy Manifests.
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as _datetime
import fcntl
import hashlib
import json
import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Protocol, Sequence


DEFAULT_CODEX_PATH = "/data/home/developer_026/.nvm/versions/node/v22.23.1/bin/codex"
DEFAULT_CURSOR_AGENT_PATH = "/data/home/developer_026/.local/bin/agent"
DEFAULT_CODEBUDDY_PATH = "/data/home/developer_026/.nvm/versions/node/v22.23.1/bin/codebuddy"
DEFAULT_STALL_SECONDS = 900
STATE_SCHEMA_VERSION = 3
DEFAULT_PROFILES_PATH = Path(__file__).resolve().parent / "profiles.json"

DEFAULT_EXECUTION_POLICY = {
    "initial_writer_profile": "codex-luna-primary",
    "fixer_profile": "cursor-grok45-fixer",
    "reviewer_profile": "codex-luna-review",
    "diagnoser_profile": "codex-sol-xhigh-diagnose",
    "backup_profile": "codebuddy-kimi-k3-backup",
    "max_fix_cycles_before_diagnosis": 2,
    "max_post_diagnosis_fix_cycles": 1,
    "require_fresh_review_session": True,
    "automatic_provider_switch": False,
}

SUPPORTED_RUNNERS = frozenset({"codex-cli", "cursor-cli", "codebuddy-cli"})
IMPLEMENTED_RUNNERS = frozenset({"codex-cli", "cursor-cli", "codebuddy-cli"})

PRIMARY_PHASES = (
    "ICR",
    "IMPLEMENT",
    "FOCUSED_REVIEW",
    "FIX",
    "VERIFY",
    "DIAGNOSE",
    "PREMERGE",
    "DONE",
)
LEGACY_PHASE_ALIASES = {
    "IMPLEMENTING": "IMPLEMENT",
    "FIXING": "FIX",
}
AUXILIARY_STATES = (
    "REGISTERED",
    "READY",
    "QUEUED",
    "RUNNING",
    "SUSPECTED_STALL",
    "VERIFYING",
    "READY_FOR_REVIEW",
    "WAITING_CAPACITY",
    "CONTRACT_CONFLICT",
    "FAILED",
    "STOPPED",
)
ALL_STATES = frozenset(PRIMARY_PHASES + AUXILIARY_STATES + tuple(LEGACY_PHASE_ALIASES))
TERMINAL_STATES = frozenset({"DONE", "STOPPED"})
MAX_CONCURRENCY = 3

# This graph is deliberately explicit.  A caller may not jump over a review
# or pre-merge gate merely by writing a different string into state.
STATE_TRANSITIONS: Mapping[str, frozenset[str]] = {
    "REGISTERED": frozenset(
        {
            "READY",
            "QUEUED",
            "RUNNING",
            "ICR",
            "READY_FOR_REVIEW",
            "VERIFY",
            "CONTRACT_CONFLICT",
            "FAILED",
            "STOPPED",
        }
    ),
    "READY": frozenset(
        {
            "QUEUED",
            "RUNNING",
            "ICR",
            "IMPLEMENT",
            "READY_FOR_REVIEW",
            "VERIFY",
            "WAITING_CAPACITY",
            "CONTRACT_CONFLICT",
            "FAILED",
            "STOPPED",
        }
    ),
    "RUNNING": frozenset(
        {
            "SUSPECTED_STALL",
            "READY_FOR_REVIEW",
            "WAITING_CAPACITY",
            "VERIFYING",
            "CONTRACT_CONFLICT",
            "FAILED",
            "STOPPED",
            "ICR",
            "IMPLEMENT",
            "FOCUSED_REVIEW",
            "FIX",
            "VERIFY",
            "PREMERGE",
            "DONE",
        }
    ),
    "ICR": frozenset(
        {
            "QUEUED",
            "RUNNING",
            "IMPLEMENT",
            "READY_FOR_REVIEW",
            "WAITING_CAPACITY",
            "CONTRACT_CONFLICT",
            "FAILED",
            "STOPPED",
        }
    ),
    "IMPLEMENT": frozenset(
        {
            "QUEUED",
            "RUNNING",
            "FOCUSED_REVIEW",
            "FIX",
            "VERIFY",
            "VERIFYING",
            "READY_FOR_REVIEW",
            "WAITING_CAPACITY",
            "CONTRACT_CONFLICT",
            "FAILED",
            "STOPPED",
        }
    ),
    "FOCUSED_REVIEW": frozenset(
        {
            "QUEUED",
            "RUNNING",
            "FIX",
            "VERIFY",
            "PREMERGE",
            "VERIFYING",
            "READY_FOR_REVIEW",
            "CONTRACT_CONFLICT",
            "FAILED",
            "STOPPED",
        }
    ),
    "FIX": frozenset(
        {
            "QUEUED",
            "RUNNING",
            "FOCUSED_REVIEW",
            "VERIFY",
            "PREMERGE",
            "VERIFYING",
            "READY_FOR_REVIEW",
            "WAITING_CAPACITY",
            "CONTRACT_CONFLICT",
            "FAILED",
            "STOPPED",
        }
    ),
    "VERIFY": frozenset(
        {
            "RUNNING",
            "PREMERGE",
            "DONE",
            "FIX",
            "READY_FOR_REVIEW",
            "CONTRACT_CONFLICT",
            "FAILED",
            "STOPPED",
        }
    ),
    "PREMERGE": frozenset(
        {
            "RUNNING",
            "VERIFYING",
            "DONE",
            "FIX",
            "READY_FOR_REVIEW",
            "CONTRACT_CONFLICT",
            "FAILED",
            "STOPPED",
        }
    ),
    "VERIFYING": frozenset(
        {
            "RUNNING",
            "READY_FOR_REVIEW",
            "DONE",
            "FIX",
            "WAITING_CAPACITY",
            "CONTRACT_CONFLICT",
            "FAILED",
            "STOPPED",
        }
    ),
    "READY_FOR_REVIEW": frozenset(
        {
            "QUEUED",
            "RUNNING",
            "FOCUSED_REVIEW",
            "PREMERGE",
            "VERIFYING",
            "FIX",
            "DONE",
            "CONTRACT_CONFLICT",
            "FAILED",
            "STOPPED",
        }
    ),
    "WAITING_CAPACITY": frozenset(
        {
            "QUEUED",
            "RUNNING",
            "READY",
            "ICR",
            "IMPLEMENT",
            "FOCUSED_REVIEW",
            "FIX",
            "VERIFY",
            "PREMERGE",
            "VERIFYING",
            "READY_FOR_REVIEW",
            "CONTRACT_CONFLICT",
            "FAILED",
            "STOPPED",
        }
    ),
    "QUEUED": frozenset(
        {"RUNNING", "READY", "CONTRACT_CONFLICT", "FAILED", "STOPPED"}
    ),
    "SUSPECTED_STALL": frozenset(
        {
            "QUEUED",
            "RUNNING",
            "WAITING_CAPACITY",
            "READY_FOR_REVIEW",
            "CONTRACT_CONFLICT",
            "FAILED",
            "STOPPED",
        }
    ),
    "CONTRACT_CONFLICT": frozenset({"ICR", "READY", "FAILED", "STOPPED"}),
    "FAILED": frozenset(
        {"READY", "QUEUED", "RUNNING", "ICR", "IMPLEMENT", "FIX", "CONTRACT_CONFLICT", "STOPPED"}
    ),
    "STOPPED": frozenset({"READY", "QUEUED", "RUNNING", "ICR", "IMPLEMENT", "FIX", "STOPPED"}),
    "DONE": frozenset({"STOPPED"}),
}

# Older task JSON files used the phase names as status values as well.  Keep
# their transition surface equivalent while new states use canonical names.
for _legacy_phase, _canonical_phase in LEGACY_PHASE_ALIASES.items():
    STATE_TRANSITIONS[_legacy_phase] = STATE_TRANSITIONS[_canonical_phase]

PHASE_TRANSITIONS: Mapping[str, frozenset[str]] = {
    # Multi-model pipeline: verify/fix/review/diagnose without DONE regressions.
    "ICR": frozenset({"IMPLEMENT", "VERIFY", "FIX", "FOCUSED_REVIEW", "DIAGNOSE"}),
    "IMPLEMENT": frozenset({"FOCUSED_REVIEW", "VERIFY", "FIX", "DIAGNOSE"}),
    "FOCUSED_REVIEW": frozenset({"FIX", "VERIFY", "PREMERGE", "DIAGNOSE"}),
    "FIX": frozenset({"FOCUSED_REVIEW", "VERIFY", "PREMERGE", "DIAGNOSE"}),
    "VERIFY": frozenset({"FIX", "FOCUSED_REVIEW", "DIAGNOSE", "PREMERGE", "DONE"}),
    "DIAGNOSE": frozenset({"FIX", "FOCUSED_REVIEW", "VERIFY", "PREMERGE"}),
    "PREMERGE": frozenset({"DONE", "FIX"}),
    "DONE": frozenset(),
}

TASK_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
SHA_RE = re.compile(r"^[0-9a-fA-F]{7,64}$")
HEX_SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")
PATH_GLOB_META = set("*?[")
CAPACITY_RE = re.compile(
    r"(?:\b14003\b|full[\s_-]*capacity|capacity[\s_-]*full|resource[\s_-]*limit)",
    re.IGNORECASE,
)
SESSION_ID_KEYS = (
    "session_id",
    "sessionId",
    "conversation_id",
    "conversationId",
    "chat_id",
    "chatId",
)
THREAD_ID_KEYS = ("thread_id", "threadId")


class AgentCtlError(RuntimeError):
    """A user-actionable controller error."""


class ManifestError(AgentCtlError):
    """A malformed or unsafe manifest."""


class StateError(AgentCtlError):
    """An invalid or unreadable task state."""


def utc_now() -> str:
    return _datetime.datetime.now(_datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def _timestamp(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return _datetime.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None


def _json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def atomic_write_text(path: Path, text: str) -> None:
    """Write a file atomically in its destination directory."""

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
        try:
            directory_fd = os.open(path.parent, os.O_RDONLY)
        except OSError:
            directory_fd = None
        if directory_fd is not None:
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    finally:
        with contextlib.suppress(FileNotFoundError):
            temporary_path.unlink()


def atomic_write_json(path: Path, value: Mapping[str, Any]) -> None:
    atomic_write_text(path, _json_dump(value))


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise ManifestError(f"cannot read file for sha256: {path}: {exc}") from exc
    return digest.hexdigest()


def _require_string(value: Any, name: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str) or (not allow_empty and not value.strip()):
        raise ManifestError(f"{name} must be a non-empty string")
    if "\x00" in value:
        raise ManifestError(f"{name} contains NUL")
    return value


def _require_string_list(value: Any, name: str, *, allow_empty: bool = True) -> list[str]:
    if not isinstance(value, list) or (not allow_empty and not value):
        raise ManifestError(f"{name} must be a list of strings")
    result: list[str] = []
    for index, item in enumerate(value):
        result.append(_require_string(item, f"{name}[{index}]").strip())
    return result


def validate_task_id(task_id: str) -> str:
    if not isinstance(task_id, str) or not TASK_ID_RE.fullmatch(task_id):
        raise ManifestError(
            "task_id must match [A-Za-z0-9][A-Za-z0-9_.-]{0,63} (tmux-safe)"
        )
    return task_id


def _validate_write_pattern(value: Any, name: str) -> str:
    pattern = _require_string(value, name).strip().replace("\\", "/")
    if pattern.startswith("/") or re.match(r"^[A-Za-z]:/", pattern):
        raise ManifestError(f"{name} must be relative to the repository: {pattern}")
    parts = [part for part in pattern.split("/") if part not in ("", ".")]
    if not parts or any(part == ".." for part in parts):
        raise ManifestError(f"{name} contains an unsafe path: {pattern}")
    return "/".join(parts)


def normalize_write_set(value: Any, name: str, *, allow_empty: bool = True) -> list[str]:
    if not isinstance(value, list) or (not allow_empty and not value):
        raise ManifestError(f"{name} must be a list of relative path patterns")
    result = []
    for index, item in enumerate(value):
        result.append(_validate_write_pattern(item, f"{name}[{index}]"))
    return result


def canonical_phase(value: str) -> str:
    """Return the current phase spelling while accepting last-round aliases."""

    if not isinstance(value, str):
        raise StateError(f"invalid pipeline phase: {value!r}")
    return LEGACY_PHASE_ALIASES.get(value, value)


def normalize_legacy_state(value: Mapping[str, Any]) -> tuple[dict[str, Any], bool]:
    """Normalize state spellings emitted by the previous controller version.

    The migration is deliberately narrow: it only changes the two old phase
    spellings, schema marker, and CP1 runner/session aliases. Other state
    fields remain untouched so an older state file can still be resumed.
    """

    normalized = dict(value)
    changed = False
    for key in ("phase", "status"):
        raw = normalized.get(key)
        canonical = LEGACY_PHASE_ALIASES.get(raw, raw)
        if canonical != raw:
            normalized[key] = canonical
            changed = True
    if _sync_session_aliases(normalized):
        changed = True
    for counter in (
        "repair_cycle_count",
        "same_failure_signature_count",
        "review_round_count",
        "post_diagnosis_fix_count",
    ):
        if counter not in normalized:
            normalized[counter] = 0
            changed = True
    if "execution_policy" not in normalized:
        model = normalized.get("model")
        effort = normalized.get("reasoning_effort")
        if isinstance(model, str) and isinstance(effort, str) and model and effort:
            policy, _profile, _name = resolve_execution_policy(
                {
                    "model": model,
                    "reasoning_effort": effort,
                    "role": normalized.get("role", "writer"),
                }
            )
            normalized["execution_policy"] = policy
            normalized.setdefault("active_profile", policy["initial_writer_profile"])
            normalized.setdefault("runner_kind", "codex-cli")
            changed = True
    if normalized.get("schema_version") != STATE_SCHEMA_VERSION:
        normalized["schema_version"] = STATE_SCHEMA_VERSION
        changed = True
    return normalized, changed


def default_profiles_path() -> Path:
    configured = os.environ.get("AGENTCTL_PROFILES")
    if configured:
        return Path(configured).expanduser()
    return DEFAULT_PROFILES_PATH


def load_profiles(path: Path | str | None = None) -> dict[str, dict[str, Any]]:
    """Load logical runner profiles used by execution_policy."""

    target = Path(path) if path is not None else default_profiles_path()
    if not target.is_file():
        raise ManifestError(f"profiles file missing: {target}")
    try:
        raw = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ManifestError(f"profiles file unreadable: {target}: {exc}") from exc
    if not isinstance(raw, Mapping) or not raw:
        raise ManifestError("profiles root must be a non-empty JSON object")
    profiles: dict[str, dict[str, Any]] = {}
    for name, item in raw.items():
        profile_id = _require_string(name, "profile id")
        if not isinstance(item, Mapping):
            raise ManifestError(f"profile {profile_id} must be an object")
        runner = _require_string(item.get("runner"), f"profiles.{profile_id}.runner")
        if runner not in SUPPORTED_RUNNERS:
            raise ManifestError(
                f"profiles.{profile_id}.runner must be one of {sorted(SUPPORTED_RUNNERS)}"
            )
        model = _require_string(item.get("model"), f"profiles.{profile_id}.model")
        role = _require_string(item.get("role", "writer"), f"profiles.{profile_id}.role")
        write_mode = _require_string(
            item.get("write_mode", "workspace-write"), f"profiles.{profile_id}.write_mode"
        )
        if write_mode not in {"workspace-write", "read-only"}:
            raise ManifestError(
                f"profiles.{profile_id}.write_mode must be workspace-write or read-only"
            )
        entry: dict[str, Any] = {
            "runner": runner,
            "model": model,
            "role": role,
            "write_mode": write_mode,
            "fresh_session": bool(item.get("fresh_session", False)),
        }
        if "reasoning_effort" in item:
            entry["reasoning_effort"] = _require_string(
                item["reasoning_effort"], f"profiles.{profile_id}.reasoning_effort"
            )
        if "cli_model" in item:
            entry["cli_model"] = _require_string(
                item["cli_model"], f"profiles.{profile_id}.cli_model"
            )
        profiles[profile_id] = entry
    return profiles


def profile_cli_model(profile: Mapping[str, Any]) -> str:
    """Return the CLI model id, preferring an explicit cli_model override."""

    return str(profile.get("cli_model") or profile["model"])


def legacy_codex_profile(model: str, reasoning_effort: str, *, role: str = "writer") -> dict[str, Any]:
    """Map pre-CP1 model/reasoning_effort fields onto an ephemeral Codex profile."""

    write_mode = "read-only" if role == "reviewer" else "workspace-write"
    return {
        "runner": "codex-cli",
        "model": model,
        "reasoning_effort": reasoning_effort,
        "role": role if role in {"writer", "reviewer"} else "writer",
        "write_mode": write_mode,
        "fresh_session": role == "reviewer",
        "legacy": True,
    }


def validate_execution_policy(
    policy: Mapping[str, Any],
    profiles: Mapping[str, Mapping[str, Any]],
    *,
    allow_unimplemented: bool = True,
) -> dict[str, Any]:
    if not isinstance(policy, Mapping):
        raise ManifestError("execution_policy must be an object")
    resolved = dict(DEFAULT_EXECUTION_POLICY)
    for key, value in policy.items():
        if key not in DEFAULT_EXECUTION_POLICY:
            raise ManifestError(f"execution_policy unknown field: {key}")
        resolved[key] = value
    for key in (
        "initial_writer_profile",
        "fixer_profile",
        "reviewer_profile",
        "diagnoser_profile",
        "backup_profile",
    ):
        profile_id = _require_string(resolved[key], f"execution_policy.{key}")
        if profile_id not in profiles and not str(profile_id).startswith("legacy-codex:"):
            raise ManifestError(f"execution_policy.{key} unknown profile: {profile_id}")
        if profile_id in profiles:
            runner = profiles[profile_id]["runner"]
            if runner not in IMPLEMENTED_RUNNERS and not allow_unimplemented:
                raise ManifestError(
                    f"execution_policy.{key} profile {profile_id} uses unimplemented runner {runner}"
                )
    for key in ("max_fix_cycles_before_diagnosis", "max_post_diagnosis_fix_cycles"):
        value = resolved[key]
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ManifestError(f"execution_policy.{key} must be a non-negative integer")
    resolved["require_fresh_review_session"] = bool(resolved["require_fresh_review_session"])
    resolved["automatic_provider_switch"] = bool(resolved["automatic_provider_switch"])
    return resolved


def resolve_profile(
    profile_id: str,
    profiles: Mapping[str, Mapping[str, Any]],
    *,
    legacy_model: str | None = None,
    legacy_effort: str | None = None,
    role: str = "writer",
) -> dict[str, Any]:
    if profile_id.startswith("legacy-codex:"):
        if not legacy_model or not legacy_effort:
            raise ManifestError("legacy Codex profile requires model and reasoning_effort")
        profile = legacy_codex_profile(legacy_model, legacy_effort, role=role)
        profile["profile_id"] = profile_id
        return profile
    if profile_id not in profiles:
        raise ManifestError(f"unknown profile: {profile_id}")
    profile = dict(profiles[profile_id])
    profile["profile_id"] = profile_id
    profile.setdefault("reasoning_effort", "high")
    return profile


def resolve_execution_policy(
    manifest: Mapping[str, Any],
    *,
    profiles: Mapping[str, Mapping[str, Any]] | None = None,
    profiles_path: Path | str | None = None,
) -> tuple[dict[str, Any], dict[str, Any], str]:
    """Return (policy, active_writer_profile, active_profile_id)."""

    loaded = dict(profiles) if profiles is not None else load_profiles(profiles_path)
    role = str(manifest.get("role", "writer"))
    raw_policy = manifest.get("execution_policy")
    if raw_policy is None:
        model = _require_string(manifest.get("model"), "model")
        effort = _require_string(manifest.get("reasoning_effort"), "reasoning_effort")
        legacy_id = (
            "legacy-codex:"
            + hashlib.sha256(f"{model}|{effort}|{role}".encode()).hexdigest()[:12]
        )
        loaded[legacy_id] = legacy_codex_profile(model, effort, role=role)
        policy = dict(DEFAULT_EXECUTION_POLICY)
        policy["initial_writer_profile"] = legacy_id
        if role == "reviewer":
            policy["reviewer_profile"] = legacy_id
        active_id = legacy_id
    else:
        policy = validate_execution_policy(raw_policy, loaded)
        active_id = str(policy["initial_writer_profile"])
        if role == "reviewer":
            active_id = str(policy["reviewer_profile"])
    active = resolve_profile(
        active_id,
        loaded,
        legacy_model=str(manifest["model"]) if manifest.get("model") else None,
        legacy_effort=str(manifest["reasoning_effort"]) if manifest.get("reasoning_effort") else None,
        role=role,
    )
    # CP1 can only launch Codex. Other profiles remain declared for CP2+.
    if active["runner"] != "codex-cli":
        raise ManifestError(
            f"CP1 can only launch codex-cli profiles; {active_id} uses {active['runner']}"
        )
    return policy, active, active_id


def _sync_session_aliases(state: dict[str, Any]) -> bool:
    """Keep runner_* and legacy codex_* session fields mirrored."""

    changed = False
    session = state.get("runner_session_id") or state.get("codex_session_id")
    thread = state.get("runner_thread_id") or state.get("codex_thread_id")
    if session and state.get("runner_session_id") != session:
        state["runner_session_id"] = session
        changed = True
    if session and state.get("codex_session_id") != session:
        state["codex_session_id"] = session
        changed = True
    if thread and state.get("runner_thread_id") != thread:
        state["runner_thread_id"] = thread
        changed = True
    if thread and state.get("codex_thread_id") != thread:
        state["codex_thread_id"] = thread
        changed = True
    return changed


def _get_runner_session_id(state: Mapping[str, Any]) -> str | None:
    value = state.get("runner_session_id") or state.get("codex_session_id")
    return str(value) if value else None


def _set_runner_session_ids(
    state: dict[str, Any],
    *,
    session_id: str | None = None,
    thread_id: str | None = None,
) -> bool:
    changed = False
    if session_id:
        if state.get("runner_session_id") != session_id:
            state["runner_session_id"] = session_id
            changed = True
        if state.get("codex_session_id") != session_id:
            state["codex_session_id"] = session_id
            changed = True
    if thread_id:
        if state.get("runner_thread_id") != thread_id:
            state["runner_thread_id"] = thread_id
            changed = True
        if state.get("codex_thread_id") != thread_id:
            state["codex_thread_id"] = thread_id
            changed = True
    return changed


def normalize_semantic_locks(value: Any, name: str = "semantic_locks") -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ManifestError(f"{name} must be a list of strings")
    result: list[str] = []
    for index, item in enumerate(value):
        lock = _require_string(item, f"{name}[{index}]").strip()
        if lock not in result:
            result.append(lock)
    return result


def path_matches_pattern(path: str, pattern: str) -> bool:
    """Match a Git-relative path against one conservative write-set pattern."""

    import fnmatch

    path = path.replace("\\", "/").lstrip("./")
    pattern = pattern.replace("\\", "/").lstrip("./")
    if pattern.endswith("/**"):
        prefix = pattern[:-3].rstrip("/")
        return path == prefix or path.startswith(prefix + "/")
    if fnmatch.fnmatchcase(path, pattern):
        return True
    # A literal directory owns its contents even when the manifest omitted
    # the trailing /**.  This keeps the gate useful for small manifests.
    return not any(char in pattern for char in PATH_GLOB_META) and path.startswith(pattern + "/")


def semantic_locks_overlap(left: str, right: str) -> bool:
    left = left.strip().rstrip("/")
    right = right.strip().rstrip("/")
    return left == right or left.startswith(right + "/") or right.startswith(left + "/")


def find_semantic_lock_conflicts(
    locks: Sequence[str], existing_states: Sequence[Mapping[str, Any]], *, task_id: str | None = None
) -> list[str]:
    conflicts: list[str] = []
    for state in existing_states:
        if task_id is not None and state.get("task_id") == task_id:
            continue
        if state.get("status") in TERMINAL_STATES:
            continue
        other = normalize_semantic_locks(state.get("semantic_locks", []))
        for lock in locks:
            for held in other:
                if semantic_locks_overlap(lock, held):
                    conflicts.append(f"task {state.get('task_id', '<unknown>')}: semantic lock {lock} overlaps {held}")
    return conflicts


def validate_manifest(
    manifest: Mapping[str, Any],
    *,
    profiles: Mapping[str, Mapping[str, Any]] | None = None,
    profiles_path: Path | str | None = None,
) -> None:
    """Validate the JSON shape without touching the filesystem."""

    if not isinstance(manifest, Mapping):
        raise ManifestError("manifest root must be a JSON object")
    required = (
        "task_id",
        "repo",
        "worktree",
        "branch",
        "base_commit",
        "prompt_file",
        "owned_write_set",
        "forbidden_write_set",
        "authoritative_inputs",
        "acceptance_commands",
        "review_scope",
    )
    missing = [name for name in required if name not in manifest]
    if missing:
        raise ManifestError(f"manifest missing required fields: {', '.join(missing)}")
    has_legacy_model = "model" in manifest and "reasoning_effort" in manifest
    has_policy = "execution_policy" in manifest
    if not has_legacy_model and not has_policy:
        raise ManifestError(
            "manifest requires model/reasoning_effort or execution_policy"
        )
    validate_task_id(manifest["task_id"])
    for field in ("repo", "worktree", "branch", "prompt_file"):
        _require_string(manifest[field], field)
    if has_legacy_model:
        for field in ("model", "reasoning_effort"):
            _require_string(manifest[field], field)
    base_commit = _require_string(manifest["base_commit"], "base_commit")
    if not SHA_RE.fullmatch(base_commit):
        raise ManifestError("base_commit must be a hexadecimal Git commit id")
    role = manifest.get("role", "writer")
    if role not in {"writer", "reviewer"}:
        raise ManifestError("role must be writer or reviewer")
    # Resolve profiles early so register fails closed on unknown/unimplemented
    # launch profiles while still allowing fixer/backup declarations for CP2+.
    resolve_execution_policy(manifest, profiles=profiles, profiles_path=profiles_path)
    owned = normalize_write_set(
        manifest["owned_write_set"], "owned_write_set", allow_empty=role == "reviewer"
    )
    forbidden = normalize_write_set(manifest["forbidden_write_set"], "forbidden_write_set")
    for left in owned:
        for right in forbidden:
            if patterns_overlap(left, right):
                raise ManifestError(
                    f"owned_write_set overlaps forbidden_write_set: {left} vs {right}"
                )
    inputs = manifest["authoritative_inputs"]
    if not isinstance(inputs, list):
        raise ManifestError("authoritative_inputs must be a list")
    for index, item in enumerate(inputs):
        if not isinstance(item, Mapping):
            raise ManifestError(f"authoritative_inputs[{index}] must be an object")
        _require_string(item.get("path"), f"authoritative_inputs[{index}].path")
        _require_string(item.get("section"), f"authoritative_inputs[{index}].section")
        if "sha256" in item:
            digest = _require_string(item["sha256"], f"authoritative_inputs[{index}].sha256")
            if not HEX_SHA256_RE.fullmatch(digest):
                raise ManifestError(f"authoritative_inputs[{index}].sha256 must be 64 hex characters")
    _require_string_list(manifest["acceptance_commands"], "acceptance_commands", allow_empty=False)
    review_scope = manifest["review_scope"]
    if isinstance(review_scope, str):
        _require_string(review_scope, "review_scope")
    elif isinstance(review_scope, list):
        _require_string_list(review_scope, "review_scope", allow_empty=False)
    else:
        raise ManifestError("review_scope must be a string or list of strings")
    if "frozen_summary" in manifest:
        _require_string_list(manifest["frozen_summary"], "frozen_summary")
    normalize_semantic_locks(manifest.get("semantic_locks", []))
    if "risk" in manifest:
        if not isinstance(manifest["risk"], (str, Mapping)):
            raise ManifestError("risk must be a string or object")
        if isinstance(manifest["risk"], str):
            _require_string(manifest["risk"], "risk")
    dependencies = manifest.get("dependencies", [])
    if not isinstance(dependencies, list):
        raise ManifestError("dependencies must be a list")
    for index, dependency in enumerate(dependencies):
        if isinstance(dependency, str):
            _require_string(dependency, f"dependencies[{index}]")
            continue
        if not isinstance(dependency, Mapping):
            raise ManifestError(f"dependencies[{index}] must be a path or object")
        dep_path = dependency.get("path") or dependency.get("repo") or dependency.get("worktree")
        _require_string(dep_path, f"dependencies[{index}].path")
        if "commit" in dependency:
            commit = _require_string(dependency["commit"], f"dependencies[{index}].commit")
            if not SHA_RE.fullmatch(commit):
                raise ManifestError(f"dependencies[{index}].commit must be a Git commit id")
    if role == "reviewer":
        review_of = manifest.get("review_of") or manifest.get("writer_task_id")
        validate_task_id(_require_string(review_of, "review_of"))


def _resolved_file(raw_path: str, base: Path) -> Path:
    path = Path(raw_path).expanduser()
    if not path.is_absolute():
        path = base / path
    return path.resolve()


def _run_process(
    argv: Sequence[str],
    *,
    cwd: Path | None = None,
    input_text: str | None = None,
    timeout: float = 20,
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            [str(item) for item in argv],
            cwd=str(cwd) if cwd is not None else None,
            input=input_text,
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
    except FileNotFoundError as exc:
        raise AgentCtlError(f"executable not found: {argv[0]}") from exc
    except subprocess.TimeoutExpired as exc:
        raise AgentCtlError(f"command timed out: {shlex.join([str(item) for item in argv])}") from exc


def _git_output(worktree: Path, args: Sequence[str], *, check: bool = True) -> str:
    result = _run_process(["git", "-C", str(worktree), *args], cwd=None)
    if check and result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise ManifestError(f"git command failed in {worktree}: {' '.join(args)}: {detail}")
    # Preserve the leading status column from `git status --porcelain`.
    # Stripping the whole output turns a first-line ` M internal/x.go` into
    # `M internal/x.go`, and the porcelain parser then drops the first path
    # character. Trailing whitespace is not semantically meaningful here.
    return result.stdout.rstrip()


def _git_worktree_roots(repo: Path) -> set[Path]:
    output = _git_output(repo, ["worktree", "list", "--porcelain"])
    roots: set[Path] = set()
    for line in output.splitlines():
        if line.startswith("worktree "):
            roots.add(Path(line[len("worktree ") :]).resolve())
    return roots


def validate_git_context(manifest: Mapping[str, Any], *, manifest_path: Path) -> dict[str, Path | str]:
    repo = _resolved_file(manifest["repo"], manifest_path.parent)
    worktree = _resolved_file(manifest["worktree"], manifest_path.parent)
    if not repo.is_dir():
        raise ManifestError(f"repo is not a directory: {repo}")
    if not worktree.is_dir():
        raise ManifestError(f"worktree is not a directory: {worktree}")
    repo_root = Path(_git_output(repo, ["rev-parse", "--show-toplevel"])).resolve()
    worktree_root = Path(_git_output(worktree, ["rev-parse", "--show-toplevel"])).resolve()
    if repo_root != repo:
        raise ManifestError(f"repo must be a Git worktree root: {repo}")
    if worktree_root != worktree:
        raise ManifestError(f"worktree must be a Git worktree root: {worktree}")
    if _git_output(worktree, ["rev-parse", "--is-inside-work-tree"]) != "true":
        raise ManifestError(f"not a Git worktree: {worktree}")
    repo_common = Path(_git_output(repo, ["rev-parse", "--path-format=absolute", "--git-common-dir"])).resolve()
    worktree_common = Path(
        _git_output(worktree, ["rev-parse", "--path-format=absolute", "--git-common-dir"])
    ).resolve()
    if repo_common != worktree_common:
        raise ManifestError("repo and worktree do not belong to the same Git repository")
    if worktree not in _git_worktree_roots(repo):
        raise ManifestError(f"worktree is not registered with repo: {worktree}")
    branch = _git_output(worktree, ["symbolic-ref", "--quiet", "--short", "HEAD"], check=False)
    if not branch or branch != manifest["branch"]:
        raise ManifestError(
            f"branch mismatch: manifest={manifest['branch']!r}, current={branch or '<detached>'!r}"
        )
    head = _git_output(worktree, ["rev-parse", "HEAD"])
    requested_base = _git_output(worktree, ["rev-parse", f"{manifest['base_commit']}^{{commit}}"])
    if head != requested_base:
        raise ManifestError(
            f"base commit mismatch: manifest={requested_base}, current HEAD={head}; "
            "register expects a clean worktree at the declared base"
        )
    return {"repo": repo, "worktree": worktree, "branch": branch, "head": head}


def validate_manifest_files(
    manifest: Mapping[str, Any], *, manifest_path: Path, worktree: Path
) -> tuple[Path, str, list[dict[str, str]]]:
    prompt_file = _resolved_file(manifest["prompt_file"], worktree)
    if not prompt_file.is_file():
        raise ManifestError(f"prompt_file does not exist: {prompt_file}")
    prompt_hash = sha256_file(prompt_file)
    inputs: list[dict[str, str]] = []
    for index, item in enumerate(manifest["authoritative_inputs"]):
        input_path = _resolved_file(item["path"], worktree)
        if not input_path.is_file():
            raise ManifestError(f"authoritative_inputs[{index}].path does not exist: {input_path}")
        actual = sha256_file(input_path)
        expected = item.get("sha256")
        if expected is not None and actual.lower() != str(expected).lower():
            raise ManifestError(
                f"sha256 drift for authoritative input {input_path}: expected {expected}, got {actual}"
            )
        inputs.append({"path": str(input_path), "section": item["section"], "sha256": actual})
    return prompt_file, prompt_hash, inputs


def dependency_snapshot(
    manifest: Mapping[str, Any], *, worktree: Path
) -> list[dict[str, str | None]]:
    """Resolve and freeze dependency commits without copying dependency text."""

    result: list[dict[str, str | None]] = []
    for index, raw in enumerate(manifest.get("dependencies", [])):
        if isinstance(raw, str):
            raw_path = raw
            expected = None
        else:
            raw_path = raw.get("path") or raw.get("repo") or raw.get("worktree")
            expected = raw.get("commit")
        path = _resolved_file(str(raw_path), worktree)
        if not path.is_dir():
            raise ManifestError(f"dependency[{index}] is not a directory: {path}")
        try:
            root = Path(_git_output(path, ["rev-parse", "--show-toplevel"])).resolve()
            head = _git_output(path, ["rev-parse", "HEAD"])
        except AgentCtlError as exc:
            raise ManifestError(f"dependency[{index}] is not a Git repository: {path}: {exc}") from exc
        if root != path:
            raise ManifestError(f"dependency[{index}] must be a Git root: {path}")
        if expected is not None and head.lower() != str(expected).lower():
            raise ManifestError(
                f"dependency commit mismatch for {path}: expected {expected}, got {head}"
            )
        result.append({"path": str(path), "commit": head})
    return result


def dependency_snapshot_stale(snapshot: Sequence[Mapping[str, Any]]) -> str | None:
    for item in snapshot:
        path = Path(str(item.get("path", "")))
        expected = str(item.get("commit", ""))
        try:
            actual = _git_output(path, ["rev-parse", "HEAD"])
        except AgentCtlError:
            return f"dependency unavailable: {path}"
        if actual != expected:
            return f"dependency commit drift: {path}: registered {expected}, current {actual}"
    return None


def _literal_prefix(pattern: str) -> tuple[str, ...]:
    prefix: list[str] = []
    for part in pattern.split("/"):
        if any(char in PATH_GLOB_META for char in part):
            break
        prefix.append(part)
    return tuple(prefix)


def _prefixes_compatible(left: tuple[str, ...], right: tuple[str, ...]) -> bool:
    for index in range(min(len(left), len(right))):
        if left[index] != right[index]:
            return False
    return True


def patterns_overlap(left: str, right: str) -> bool:
    """Conservative relative-path overlap check.

    Exact literal paths and incompatible literal prefixes are provably
    disjoint.  Anything involving a shared glob prefix is treated as a
    conflict, which is intentionally fail-closed for concurrent agents.
    """

    left = left.strip("/")
    right = right.strip("/")
    left_has_glob = any(char in PATH_GLOB_META for char in left)
    right_has_glob = any(char in PATH_GLOB_META for char in right)
    if not left_has_glob and not right_has_glob:
        return left == right or left.startswith(right + "/") or right.startswith(left + "/")
    return _prefixes_compatible(_literal_prefix(left), _literal_prefix(right))


def find_write_conflicts(
    owned: Sequence[str],
    forbidden: Sequence[str],
    existing_states: Sequence[Mapping[str, Any]],
) -> list[str]:
    conflicts: list[str] = []
    for left in owned:
        for right in forbidden:
            if patterns_overlap(left, right):
                conflicts.append(f"manifest owned {left} overlaps manifest forbidden {right}")
    for state in existing_states:
        task_id = state.get("task_id", "<unknown>")
        if state.get("status") in TERMINAL_STATES:
            continue
        other_owned = [] if state.get("role") == "reviewer" else state.get("owned_write_set", [])
        other_forbidden = [] if state.get("role") == "reviewer" else state.get("forbidden_write_set", [])
        for left in owned:
            for right in other_owned:
                if patterns_overlap(left, right):
                    conflicts.append(f"task {task_id}: owned {left} overlaps owned {right}")
            for right in other_forbidden:
                if patterns_overlap(left, right):
                    conflicts.append(f"task {task_id}: owned {left} overlaps forbidden {right}")
        for left in forbidden:
            for right in other_owned:
                if patterns_overlap(left, right):
                    conflicts.append(f"task {task_id}: forbidden {left} overlaps owned {right}")
    return conflicts


def extract_session_ids_from_value(value: Any) -> tuple[str | None, str | None]:
    """Return (session_id, thread_id) from one decoded Codex event."""

    session_id: str | None = None
    thread_id: str | None = None

    def visit(value: Any) -> None:
        nonlocal session_id, thread_id
        if isinstance(value, Mapping):
            for key in SESSION_ID_KEYS:
                item = value.get(key)
                if isinstance(item, str) and item and session_id is None:
                    session_id = item
            for key in THREAD_ID_KEYS:
                item = value.get(key)
                if isinstance(item, str) and item and thread_id is None:
                    thread_id = item
            for nested in value.values():
                visit(nested)
        elif isinstance(value, list):
            for nested in value:
                visit(nested)

    visit(value)
    return session_id, thread_id


def extract_session_ids_from_jsonl(path: Path) -> tuple[str | None, str | None]:
    """Return (session_id, thread_id) from Codex JSONL events."""

    session_id: str | None = None
    thread_id: str | None = None
    if not path.is_file():
        return None, None
    try:
        with path.open("rb") as handle:
            first = handle.read(256 * 1024)
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            tail = b""
            if size > len(first):
                handle.seek(max(0, size - 2 * 1024 * 1024))
                tail = handle.read(2 * 1024 * 1024)
        sample = (first + b"\n" + tail).decode("utf-8", errors="replace")
        for line in sample.splitlines():
            try:
                line_session, line_thread = extract_session_ids_from_value(json.loads(line))
                if session_id is None and line_session is not None:
                    session_id = line_session
                if thread_id is None and line_thread is not None:
                    thread_id = line_thread
            except json.JSONDecodeError:
                continue
    except OSError:
        return None, None
    if session_id is None and thread_id is not None:
        # `codex exec resume` calls this identifier a session id even though
        # JSONL versions commonly expose it as thread_id.
        session_id = thread_id
    return session_id, thread_id


def classify_capacity_text(text: str) -> bool:
    return bool(CAPACITY_RE.search(text))


def classify_capacity_event(event: Mapping[str, Any]) -> bool:
    """Classify only Codex failure events, never tool or document output."""

    event_type = str(event.get("type") or event.get("event") or "").strip().lower()
    if not (
        event_type == "error"
        or event_type.endswith(".error")
        or event_type == "failed"
        or event_type.endswith(".failed")
    ):
        return False
    return classify_capacity_text(json.dumps(event, ensure_ascii=False))


def _sample_jsonl(path: Path, limit: int = 2 * 1024 * 1024) -> str:
    """Read only a bounded head/tail sample of a potentially long JSONL log."""

    try:
        with path.open("rb") as handle:
            head = handle.read(min(256 * 1024, limit))
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            if size <= len(head):
                return head.decode("utf-8", errors="replace")
            handle.seek(max(0, size - limit))
            tail = handle.read(limit)
        return (head + b"\n" + tail).decode("utf-8", errors="replace")
    except OSError:
        return ""


def _event_text(value: Any) -> str | None:
    if isinstance(value, str):
        text = value.strip()
        return text if text else None
    if isinstance(value, list):
        parts = [_event_text(item) for item in value]
        joined = " ".join(part for part in parts if part)
        return joined.strip() or None
    if isinstance(value, Mapping):
        for key in ("text", "message", "output_text", "last_message", "content", "summary"):
            if key in value:
                candidate = _event_text(value[key])
                if candidate:
                    return candidate
    return None


def _find_event_field(value: Any, keys: Sequence[str]) -> str | None:
    if isinstance(value, Mapping):
        for key in keys:
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
        for nested in value.values():
            found = _find_event_field(nested, keys)
            if found:
                return found
    elif isinstance(value, list):
        for nested in value:
            found = _find_event_field(nested, keys)
            if found:
                return found
    return None


def summarize_jsonl(path: Path) -> dict[str, Any]:
    """Extract a short, bounded status summary from Codex JSONL events."""

    summary: dict[str, Any] = {
        "event_count": 0,
        "session_id": None,
        "thread_id": None,
        "last_agent_message": None,
        "current_command": None,
        "last_event_type": None,
        "capacity_error": False,
        "tests": [],
    }
    seen_tests: list[str] = []
    for line in _sample_jsonl(path).splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, Mapping):
            continue
        summary["event_count"] += 1
        event_type = str(event.get("type") or event.get("event") or "")
        if event_type:
            summary["last_event_type"] = event_type
        event_session, event_thread = extract_session_ids_from_value(event)
        summary["session_id"] = summary["session_id"] or event_session
        summary["thread_id"] = summary["thread_id"] or event_thread
        summary["capacity_error"] = summary["capacity_error"] or classify_capacity_event(event)
        command = _find_event_field(event, ("command", "command_line", "cmd", "tool_name"))
        if command:
            summary["current_command"] = command[-240:]
        role = str(event.get("role") or event.get("author") or "").lower()
        nested_item = event.get("item") if isinstance(event.get("item"), Mapping) else {}
        nested_type = str(nested_item.get("type", "")).lower()
        candidate = None
        if (
            role in {"assistant", "agent", "codex"}
            or "message" in event_type
            or "output" in event_type
            or "message" in nested_type
        ):
            candidate = _event_text(
                event.get("message")
                or event.get("text")
                or event.get("content")
                or nested_item
                or event
            )
        if candidate:
            summary["last_agent_message"] = candidate[-1000:]
            for piece in candidate.splitlines():
                lowered = piece.lower()
                if "test" in lowered or "pytest" in lowered or "unittest" in lowered:
                    item = piece.strip()[-240:]
                    if item and item not in seen_tests:
                        seen_tests.append(item)
        if event_type.lower().endswith("test") or "test" in event_type.lower():
            item = event_type[-240:]
            if item not in seen_tests:
                seen_tests.append(item)
    summary["tests"] = seen_tests[-20:]
    if summary["session_id"] is None and summary["thread_id"]:
        summary["session_id"] = summary["thread_id"]
    return summary


def _tail_text(path: Path, limit: int = 2 * 1024 * 1024) -> str:
    try:
        with path.open("rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            handle.seek(max(0, size - limit), os.SEEK_SET)
            return handle.read(limit).decode("utf-8", errors="replace")
    except OSError:
        return ""


def _text_from_offset(path: Path, offset: int, limit: int = 2 * 1024 * 1024) -> str:
    try:
        with path.open("rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            start = min(max(0, int(offset)), size)
            if size - start > limit:
                start = size - limit
            handle.seek(start, os.SEEK_SET)
            return handle.read(limit).decode("utf-8", errors="replace")
    except OSError:
        return ""


def logs_classify_capacity(
    paths: Sequence[Path], offsets: Mapping[str, int] | None = None
) -> bool:
    for path in paths:
        key = "jsonl" if path.name in {"codex.jsonl", "raw-events.jsonl"} else "plain"
        if offsets is None:
            text = _tail_text(path)
        else:
            text = _text_from_offset(path, int(offsets.get(key, 0)))
        if key == "plain":
            # codex.log contains only Codex stderr. Unlike JSONL stdout, it
            # does not contain normal tool output or documents read by an
            # agent, so a direct signature check remains appropriate here.
            if classify_capacity_text(text):
                return True
            continue
        for line in text.splitlines():
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(event, Mapping) and classify_capacity_event(event):
                return True
    return False


def _read_exit_code(path: Path) -> int | None:
    try:
        text = path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    if not text:
        return None
    try:
        return int(text.splitlines()[-1].strip())
    except ValueError:
        return None


class FileLock:
    """Linux advisory lock for one task or the registration index."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self.handle: Any = None

    def __enter__(self) -> "FileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a+", encoding="utf-8")
        fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if self.handle is not None:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
            self.handle.close()
            self.handle = None


class StateStore:
    def __init__(self, state_dir: Path):
        self.state_dir = Path(state_dir).expanduser().resolve()
        self.tasks_dir = self.state_dir / "tasks"
        self.locks_dir = self.state_dir / "locks"

    def ensure_layout(self) -> None:
        self.tasks_dir.mkdir(parents=True, exist_ok=True)
        self.locks_dir.mkdir(parents=True, exist_ok=True)

    def task_path(self, task_id: str) -> Path:
        validate_task_id(task_id)
        return self.tasks_dir / f"{task_id}.json"

    def task_dir(self, task_id: str) -> Path:
        validate_task_id(task_id)
        return self.tasks_dir / task_id

    def lock(self, task_id: str) -> FileLock:
        validate_task_id(task_id)
        return FileLock(self.locks_dir / f"{task_id}.lock")

    def registry_lock(self) -> FileLock:
        return FileLock(self.locks_dir / "registry.lock")

    def scheduler_lock(self) -> FileLock:
        """Serialize capacity check plus process launch across all tasks."""

        return FileLock(self.locks_dir / "scheduler.lock")

    def load(self, task_id: str) -> dict[str, Any]:
        path = self.task_path(task_id)
        try:
            with path.open("r", encoding="utf-8") as handle:
                value = json.load(handle)
        except FileNotFoundError as exc:
            raise StateError(f"task not found: {task_id}") from exc
        except (OSError, json.JSONDecodeError) as exc:
            raise StateError(f"cannot read task state: {path}: {exc}") from exc
        if not isinstance(value, dict) or value.get("task_id") != task_id:
            raise StateError(f"invalid state file for task {task_id}")
        value, migrated = normalize_legacy_state(value)
        if value.get("status") not in ALL_STATES:
            raise StateError(f"invalid state status for task {task_id}: {value.get('status')!r}")
        if migrated:
            # Controller operations already hold the task flock when loading
            # state.  Persisting here makes a read/status operation enough to
            # repair old IMPLEMENTING/FIXING files before the next command.
            atomic_write_json(path, value)
        return value

    def write(self, state: Mapping[str, Any]) -> None:
        task_id = validate_task_id(str(state.get("task_id")))
        atomic_write_json(self.task_path(task_id), dict(state))

    def iter_states(self) -> Iterator[dict[str, Any]]:
        if not self.tasks_dir.exists():
            return
        for path in sorted(self.tasks_dir.glob("*.json")):
            try:
                with path.open("r", encoding="utf-8") as handle:
                    value = json.load(handle)
            except (OSError, json.JSONDecodeError) as exc:
                raise StateError(f"cannot inspect task state {path}: {exc}") from exc
            if not isinstance(value, dict):
                raise StateError(f"invalid task state file: {path}")
            value, _ = normalize_legacy_state(value)
            if value.get("status") not in ALL_STATES:
                raise StateError(f"invalid task state file: {path}")
            yield value


class TmuxAdapter:
    """Subprocess-backed tmux adapter.

    tmux receives one shell-command string for the pane.  It is constructed
    with shlex.join from an argv list, so manifest values never become raw
    shell syntax.
    """

    def __init__(self, binary: str | None = None):
        self.binary = binary or shutil.which("tmux") or "tmux"

    @staticmethod
    def session_name(task_id: str) -> str:
        validate_task_id(task_id)
        return f"ac1-{task_id}"

    @staticmethod
    def shell_command(argv: Sequence[str]) -> str:
        if not argv:
            raise AgentCtlError("tmux command cannot be empty")
        for item in argv:
            if "\x00" in str(item):
                raise AgentCtlError("command argument contains NUL")
        return shlex.join([str(item) for item in argv])

    def _run(self, args: Sequence[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
        result = _run_process([self.binary, *args])
        if check and result.returncode != 0:
            detail = (result.stderr or result.stdout).strip()
            raise AgentCtlError(f"tmux failed: {shlex.join([self.binary, *args])}: {detail}")
        return result

    def has_session(self, session: str) -> bool:
        return self._run(["has-session", "-t", session], check=False).returncode == 0

    def new_session(self, session: str, cwd: Path, command: Sequence[str]) -> None:
        if not re.fullmatch(r"^[A-Za-z0-9_.-]+$", session):
            raise AgentCtlError(f"unsafe tmux session name: {session!r}")
        command_text = self.shell_command(command)
        self._run(["new-session", "-d", "-s", session, "-c", str(cwd), command_text])
        # Setting this on the named window keeps the pane/session inspectable
        # after Codex exits.  It is intentionally scoped to this session.
        self._run(["set-option", "-t", session, "remain-on-exit", "on"])

    def capture(self, session: str, lines: int = 80) -> str:
        if lines <= 0:
            raise AgentCtlError("capture lines must be positive")
        result = self._run(
            ["capture-pane", "-p", "-t", session, "-S", f"-{int(lines)}"]
        )
        return result.stdout

    def kill_session(self, session: str) -> None:
        self._run(["kill-session", "-t", session], check=False)

    def list_sessions(self) -> list[str]:
        result = self._run(["list-sessions", "-F", "#{session_name}"], check=False)
        if result.returncode != 0:
            return []
        return [line.strip() for line in result.stdout.splitlines() if line.strip()]


class RunnerAdapter(Protocol):
    """Minimal runner contract shared by Codex/Cursor/CodeBuddy adapters."""

    runner_kind: str

    def build_initial_argv(
        self,
        *,
        model: str,
        reasoning_effort: str,
        worktree: Path,
        last_message: Path,
        read_only: bool = False,
    ) -> list[str]: ...

    def build_resume_argv(
        self,
        *,
        session_id: str,
        model: str,
        reasoning_effort: str,
        worktree: Path,
        last_message: Path,
        read_only: bool = False,
    ) -> list[str]: ...


class CodexCliRunner:
    """Codex CLI runner (CP1 default and only implemented launcher)."""

    runner_kind = "codex-cli"

    def __init__(self, binary: str | None = None):
        self.binary = binary or resolve_codex_binary()
        self._reasoning_support: dict[str, bool] = {}

    def _help(self, mode: str) -> str:
        args = [self.binary, "exec"]
        if mode == "resume":
            args.append("resume")
        args.append("--help")
        try:
            result = _run_process(args)
        except AgentCtlError:
            return ""
        return f"{result.stdout}\n{result.stderr}"

    def supports_reasoning_effort(self, mode: str) -> bool:
        if mode not in self._reasoning_support:
            self._reasoning_support[mode] = "--reasoning-effort" in self._help(mode)
        return self._reasoning_support[mode]

    def _reasoning_args(self, mode: str, effort: str) -> list[str]:
        if self.supports_reasoning_effort(mode):
            return ["--reasoning-effort", effort]
        # The installed Codex CLI may expose this setting only through the
        # generic TOML override.  json.dumps gives a valid, single argv TOML
        # string without shell interpolation.
        return ["-c", f"model_reasoning_effort={json.dumps(effort)}"]

    def build_initial_argv(
        self,
        *,
        model: str,
        reasoning_effort: str,
        worktree: Path,
        last_message: Path,
        read_only: bool = False,
    ) -> list[str]:
        # The dedicated Linux builder already isolates writers in one Git
        # worktree and enforces owned/forbidden write sets before merge. Its
        # bwrap workspace-write mount maps repository ownership to nobody and
        # is therefore not writable, so writers must use the host sandbox.
        #
        # Reviewer/diagnoser Runs also need host sandbox: Findings/Diagnosis
        # JSON live under the controller state dir (outside the worktree), and
        # Codex `read-only` blocks those writes. No-mutation is enforced after
        # exit via diff fingerprint / write-set gates instead.
        del read_only
        sandbox = "danger-full-access"
        argv = [
            self.binary,
            "exec",
            "--sandbox",
            sandbox,
            "--json",
            "--model",
            model,
            *self._reasoning_args("initial", reasoning_effort),
            "--cd",
            str(worktree),
            "--output-last-message",
            str(last_message),
            "-",
        ]
        return argv

    def build_resume_argv(
        self,
        *,
        session_id: str,
        model: str,
        reasoning_effort: str,
        worktree: Path,
        last_message: Path,
        read_only: bool = False,
    ) -> list[str]:
        del worktree  # Codex resume stays attached to the original thread cwd.
        del read_only
        sandbox = "danger-full-access"
        argv = [
            self.binary,
            "exec",
            "--sandbox",
            sandbox,
            "resume",
            session_id,
            "--json",
            "--model",
            model,
            *self._reasoning_args("resume", reasoning_effort),
            "--output-last-message",
            str(last_message),
            "-",
        ]
        return argv


# Backward-compatible alias used by existing tests and callers.
CodexAdapter = CodexCliRunner


class CursorCliRunner:
    """Cursor Agent CLI runner used for Grok-oriented Fixer Runs."""

    runner_kind = "cursor-cli"
    prompt_mode = "argv"

    def __init__(self, binary: str | None = None):
        self.binary = binary or resolve_cursor_binary()

    def build_initial_argv(
        self,
        *,
        model: str,
        reasoning_effort: str,
        worktree: Path,
        last_message: Path,
        read_only: bool = False,
    ) -> list[str]:
        del reasoning_effort, last_message  # Cursor CLI has no Codex-equivalent flags.
        argv = [
            self.binary,
            "--print",
            "--output-format",
            "stream-json",
            "--model",
            model,
            "--workspace",
            str(worktree),
            "--trust",
        ]
        if read_only:
            argv.extend(["--mode", "ask"])
        else:
            # Fixer Runs must be able to edit the owned write set and rerun tests.
            argv.extend(["--force", "--sandbox", "disabled"])
        return argv

    def build_resume_argv(
        self,
        *,
        session_id: str,
        model: str,
        reasoning_effort: str,
        worktree: Path,
        last_message: Path,
        read_only: bool = False,
    ) -> list[str]:
        del last_message, reasoning_effort
        argv = [
            self.binary,
            "--resume",
            session_id,
            "--print",
            "--output-format",
            "stream-json",
            "--model",
            model,
            "--workspace",
            str(worktree),
            "--trust",
        ]
        if read_only:
            argv.extend(["--mode", "ask"])
        else:
            argv.extend(["--force", "--sandbox", "disabled"])
        return argv


class CodeBuddyCliRunner:
    """CodeBuddy CLI runner used for Kimi backup / challenger Runs."""

    runner_kind = "codebuddy-cli"
    prompt_mode = "argv"

    def __init__(self, binary: str | None = None):
        self.binary = binary or resolve_codebuddy_binary()

    def build_initial_argv(
        self,
        *,
        model: str,
        reasoning_effort: str,
        worktree: Path,
        last_message: Path,
        read_only: bool = False,
    ) -> list[str]:
        del reasoning_effort, last_message, worktree  # cwd comes from the worker.
        argv = [
            self.binary,
            "--print",
            "--output-format",
            "stream-json",
            "--model",
            model,
        ]
        if read_only:
            argv.extend(["--permission-mode", "plan"])
        else:
            argv.append("--dangerously-skip-permissions")
        return argv

    def build_resume_argv(
        self,
        *,
        session_id: str,
        model: str,
        reasoning_effort: str,
        worktree: Path,
        last_message: Path,
        read_only: bool = False,
    ) -> list[str]:
        del reasoning_effort, last_message, worktree
        argv = [
            self.binary,
            "--resume",
            session_id,
            "--print",
            "--output-format",
            "stream-json",
            "--model",
            model,
        ]
        if read_only:
            argv.extend(["--permission-mode", "plan"])
        else:
            argv.append("--dangerously-skip-permissions")
        return argv


def runner_for_kind(
    kind: str,
    *,
    codex_binary: str | None = None,
    cursor_binary: str | None = None,
    codebuddy_binary: str | None = None,
) -> RunnerAdapter:
    if kind == "codex-cli":
        return CodexCliRunner(codex_binary)
    if kind == "cursor-cli":
        return CursorCliRunner(cursor_binary)
    if kind == "codebuddy-cli":
        return CodeBuddyCliRunner(codebuddy_binary)
    raise AgentCtlError(f"runner not implemented: {kind}")


def resolve_codex_binary(configured: str | None = None) -> str:
    selected = configured or os.environ.get("AGENTCTL_CODEX")
    if selected:
        selected_path = Path(selected).expanduser()
        if selected_path.is_file():
            return str(selected_path)
        return shutil.which(selected) or str(selected_path)
    default = Path(DEFAULT_CODEX_PATH)
    if default.is_file() and os.access(default, os.X_OK):
        return str(default)
    return shutil.which("codex") or str(default)


def resolve_cursor_binary(configured: str | None = None) -> str:
    selected = configured or os.environ.get("AGENTCTL_CURSOR_AGENT")
    if selected:
        selected_path = Path(selected).expanduser()
        if selected_path.is_file():
            return str(selected_path)
        return shutil.which(selected) or str(selected_path)
    default = Path(DEFAULT_CURSOR_AGENT_PATH)
    if default.is_file() and os.access(default, os.X_OK):
        return str(default)
    return shutil.which("agent") or shutil.which("cursor-agent") or str(default)


def resolve_codebuddy_binary(configured: str | None = None) -> str:
    selected = configured or os.environ.get("AGENTCTL_CODEBUDDY")
    if selected:
        selected_path = Path(selected).expanduser()
        if selected_path.is_file():
            return str(selected_path)
        return shutil.which(selected) or str(selected_path)
    default = Path(DEFAULT_CODEBUDDY_PATH)
    if default.is_file() and os.access(default, os.X_OK):
        return str(default)
    return shutil.which("codebuddy") or shutil.which("cbc") or str(default)


def _default_state_dir() -> Path:
    configured = os.environ.get("AGENTCTL_STATE_DIR")
    if configured:
        return Path(configured).expanduser()
    return Path.home() / ".local" / "state" / "ac1-agentctl"


def _safe_read(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def _manifest_copy(manifest: Mapping[str, Any]) -> dict[str, Any]:
    # JSON round-trip makes the persisted configuration independent from the
    # caller's mutable Python object.
    return json.loads(json.dumps(manifest, ensure_ascii=False))


def _make_context_pack(
    manifest: Mapping[str, Any],
    *,
    manifest_path: Path,
    prompt_file: Path,
    prompt_hash: str,
    authoritative_inputs: list[dict[str, str]],
    base_commit: str | None = None,
    dependencies: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    frozen_summary = manifest.get("frozen_summary")
    if frozen_summary is None:
        frozen_summary = [
            "Read only the cited sections of authoritative inputs.",
            "Keep changes inside owned_write_set and do not touch forbidden_write_set.",
        ]
    return {
        "version": 1,
        "manifest_path": str(manifest_path),
        "manifest_sha256": sha256_file(manifest_path),
        "prompt_reference": {"path": str(prompt_file), "sha256": prompt_hash},
        "authoritative_inputs": authoritative_inputs,
        "base_commit": base_commit,
        "dependencies": [dict(item) for item in dependencies],
        "semantic_locks": list(manifest.get("semantic_locks", [])),
        "risk": manifest.get("risk", "unspecified"),
        "frozen_summary": list(frozen_summary),
        "acceptance_contract": {
            "acceptance_commands": list(manifest["acceptance_commands"]),
            "review_scope": manifest["review_scope"],
            "owned_write_set": list(manifest["owned_write_set"]),
            "forbidden_write_set": list(manifest["forbidden_write_set"]),
            "semantic_locks": list(manifest.get("semantic_locks", [])),
            "risk": manifest.get("risk", "unspecified"),
        },
        "generated_at": utc_now(),
    }


def _initial_state(
    manifest: Mapping[str, Any],
    *,
    manifest_path: Path,
    git_context: Mapping[str, Path | str],
    prompt_file: Path,
    prompt_hash: str,
    authoritative_inputs: list[dict[str, str]],
    dependencies: Sequence[Mapping[str, Any]],
    task_dir: Path,
) -> dict[str, Any]:
    task_id = manifest["task_id"]
    session = TmuxAdapter.session_name(task_id)
    policy, active_profile, active_profile_id = resolve_execution_policy(manifest)
    logs = {
        "jsonl": str(task_dir / "codex.jsonl"),
        "plain": str(task_dir / "codex.log"),
        "last_message": str(task_dir / "last-message.txt"),
        "exit_code": str(task_dir / "exit-code"),
        "runner_pid": str(task_dir / "runner.pid"),
    }
    now = utc_now()
    return {
        "schema_version": STATE_SCHEMA_VERSION,
        "task_id": task_id,
        "manifest_path": str(manifest_path),
        "manifest_sha256": sha256_file(manifest_path),
        "manifest": _manifest_copy(manifest),
        "repo": str(git_context["repo"]),
        "worktree": str(git_context["worktree"]),
        "branch": str(git_context["branch"]),
        "base_commit": str(git_context["head"]),
        "owned_write_set": list(manifest["owned_write_set"]),
        "forbidden_write_set": list(manifest["forbidden_write_set"]),
        "phase": "ICR",
        "status": "REGISTERED",
        "role": manifest.get("role", "writer"),
        "review_of": manifest.get("review_of") or manifest.get("writer_task_id"),
        "read_only": manifest.get("role", "writer") == "reviewer",
        "semantic_locks": list(manifest.get("semantic_locks", [])),
        "risk": manifest.get("risk", "unspecified"),
        "dependencies": [dict(item) for item in dependencies],
        "guards": [],
        "guard": None,
        "failure_count": 0,
        "repair_cycle_count": 0,
        "same_failure_signature_count": 0,
        "review_round_count": 0,
        "post_diagnosis_fix_count": 0,
        "queue_reason": None,
        "queue_mode": None,
        "queued_model": None,
        "queued_reasoning_effort": None,
        "queued_prompt": None,
        "queued_phase": None,
        "review_fingerprint": None,
        "review_completed": False,
        "review_invalidated": False,
        "review_baseline_fingerprint": None,
        "tmux_session": session,
        "execution_policy": policy,
        "active_profile": active_profile_id,
        "runner_kind": active_profile["runner"],
        "model": active_profile["model"],
        "reasoning_effort": active_profile.get("reasoning_effort", "high"),
        "prompt_file": str(prompt_file),
        "prompt_sha256": prompt_hash,
        "runner_session_id": None,
        "runner_thread_id": None,
        "codex_session_id": None,
        "codex_thread_id": None,
        "run_count": 0,
        "run_mode": None,
        "current_run_id": None,
        "registered_at": now,
        "updated_at": now,
        "last_checkpoint": None,
        "evidence_revision": 0,
        "logs": logs,
        "runtime": {
            "tmux_alive": False,
            "process_running": False,
            "exit_code": None,
            "jsonl_bytes": 0,
            "plain_bytes": 0,
            "log_size": 0,
            "last_log_growth_at": None,
            "last_log_mtime": None,
            "run_started_at": None,
            "warning": None,
            "last_agent_message": None,
            "current_command": None,
            "jsonl_summary": {},
        },
        "context_pack": _make_context_pack(
            manifest,
            manifest_path=manifest_path,
            prompt_file=prompt_file,
            prompt_hash=prompt_hash,
            authoritative_inputs=authoritative_inputs,
            base_commit=str(git_context["head"]),
            dependencies=dependencies,
        ),
        "history": [
            {
                "at": now,
                "from": None,
                "to": "REGISTERED",
                "phase": "ICR",
                "note": "manifest registered",
                "tests": [],
                "commit": None,
            }
        ],
    }


def validate_state_transition(current: str, target: str) -> None:
    current = LEGACY_PHASE_ALIASES.get(current, current)
    target = LEGACY_PHASE_ALIASES.get(target, target)
    if current not in ALL_STATES:
        raise StateError(f"unknown current state: {current}")
    if target not in ALL_STATES:
        raise StateError(f"unknown target state: {target}")
    if current == target:
        return
    if target not in STATE_TRANSITIONS.get(current, frozenset()):
        raise StateError(f"illegal state transition: {current} -> {target}")


def validate_phase_transition(current: str, target: str) -> None:
    current = canonical_phase(current)
    target = canonical_phase(target)
    if current not in PRIMARY_PHASES or target not in PRIMARY_PHASES:
        raise StateError(f"invalid pipeline phase: {current} -> {target}")
    if current == target:
        return
    if target not in PHASE_TRANSITIONS[current]:
        raise StateError(f"illegal pipeline phase transition: {current} -> {target}")


def _record_history(
    state: dict[str, Any],
    *,
    old_status: str,
    new_status: str,
    note: str | None = None,
    tests: Sequence[str] = (),
    commit: str | None = None,
) -> None:
    state.setdefault("history", []).append(
        {
            "at": utc_now(),
            "from": old_status,
            "to": new_status,
            "phase": state.get("phase"),
            "note": note,
            "tests": list(tests),
            "commit": commit,
        }
    )


def _set_status(
    state: dict[str, Any],
    target: str,
    *,
    note: str | None = None,
    tests: Sequence[str] = (),
    commit: str | None = None,
) -> None:
    current = state["status"]
    validate_state_transition(current, target)
    if current != target:
        _record_history(
            state,
            old_status=current,
            new_status=target,
            note=note,
            tests=tests,
            commit=commit,
        )
        state["status"] = target


def _log_paths(state: Mapping[str, Any]) -> list[Path]:
    logs = state.get("logs", {})
    return [Path(logs.get("jsonl", "")), Path(logs.get("plain", ""))]


def _git_changed_paths(worktree: Path) -> list[str]:
    output = _git_output(worktree, ["status", "--porcelain=v1", "--untracked-files=all"], check=False)
    paths: list[str] = []
    for line in output.splitlines():
        if len(line) < 4:
            continue
        raw = line[3:].strip()
        if " -> " in raw:
            raw = raw.split(" -> ", 1)[1]
        raw = raw.strip('"').replace("\\", "/")
        if raw and raw not in paths:
            paths.append(raw)
    return paths


def _git_diff_fingerprint(worktree: Path) -> str:
    digest = hashlib.sha256()
    # Keep the subprocess construction explicit; untracked files are added
    # below so review evidence changes when their contents change too.
    result = _run_process(["git", "-C", str(worktree), "diff", "--no-ext-diff", "--binary"])
    digest.update(result.stdout.encode("utf-8", errors="replace"))
    for relative in _git_changed_paths(worktree):
        path = worktree / relative
        digest.update(relative.encode("utf-8"))
        try:
            digest.update(path.read_bytes())
        except OSError:
            digest.update(b"<unreadable>")
    return digest.hexdigest()


def _changed_files_fingerprint(worktree: Path) -> str:
    return _git_diff_fingerprint(worktree)


def _review_fingerprint(state: Mapping[str, Any]) -> str:
    """Fingerprint the exact review target and its current evidence.

    A diff-only fingerprint is insufficient: a clean controller-owned commit,
    a changed contract/context reference, or a new checkpoint must permit a
    fresh focused review.  The changed-file digest itself includes both the
    tracked diff and the bytes of every changed/untracked path.
    """

    worktree = Path(str(state["worktree"]))
    manifest_path = Path(str(state.get("manifest_path", "")))
    current_manifest_hash: str | None = None
    if manifest_path.is_file():
        with contextlib.suppress(ManifestError):
            current_manifest_hash = sha256_file(manifest_path)
    context_pack = state.get("context_pack", {})
    history = state.get("history", [])
    payload = {
        "head": _git_output(worktree, ["rev-parse", "HEAD"]),
        "base_commit": state.get("base_commit"),
        "manifest_sha256": state.get("manifest_sha256"),
        "manifest_file_sha256": current_manifest_hash,
        "context_pack_sha256": hashlib.sha256(
            _json_dump(context_pack).encode("utf-8")
        ).hexdigest(),
        "last_checkpoint": state.get("last_checkpoint"),
        "evidence_revision": state.get("evidence_revision", 0),
        "history_revision": len(history) if isinstance(history, list) else 0,
        "run_count": state.get("run_count", 0),
        "failure_count": state.get("failure_count", 0),
        "changed_files_fingerprint": _git_diff_fingerprint(worktree),
    }
    return hashlib.sha256(_json_dump(payload).encode("utf-8")).hexdigest()


class Controller:
    def __init__(
        self,
        *,
        state_dir: Path | str | None = None,
        codex: CodexAdapter | None = None,
        cursor: CursorCliRunner | None = None,
        codebuddy: CodeBuddyCliRunner | None = None,
        tmux: TmuxAdapter | Any | None = None,
        stall_seconds: int = DEFAULT_STALL_SECONDS,
        max_concurrency: int = MAX_CONCURRENCY,
        now: Callable[[], str] = utc_now,
    ):
        self.store = StateStore(Path(state_dir) if state_dir is not None else _default_state_dir())
        self.codex = codex or CodexCliRunner()
        self.cursor = cursor or CursorCliRunner()
        self.codebuddy = codebuddy or CodeBuddyCliRunner()
        self.runner: RunnerAdapter = self.codex
        self.tmux = tmux or TmuxAdapter()
        self.stall_seconds = max(1, int(stall_seconds))
        self.max_concurrency = max(1, int(max_concurrency))
        self.now = now

    def _select_runner(self, runner_kind: str) -> RunnerAdapter:
        if runner_kind == "cursor-cli":
            self.runner = self.cursor
        elif runner_kind == "codebuddy-cli":
            self.runner = self.codebuddy
        elif runner_kind == "codex-cli":
            self.runner = self.codex
        else:
            self.runner = runner_for_kind(
                runner_kind,
                codex_binary=getattr(self.codex, "binary", None),
                cursor_binary=getattr(self.cursor, "binary", None),
                codebuddy_binary=getattr(self.codebuddy, "binary", None),
            )
        return self.runner

    def _refresh_runtime(self, state: dict[str, Any]) -> bool:
        runtime = state.setdefault("runtime", {})
        before = json.dumps(state, sort_keys=True, ensure_ascii=False)
        try:
            tmux_alive = bool(self.tmux.has_session(state["tmux_session"]))
            tmux_warning = None
        except (AgentCtlError, OSError) as exc:
            tmux_alive = False
            tmux_warning = f"tmux unavailable: {exc}"
        ids = extract_session_ids_from_jsonl(Path(state["logs"]["jsonl"]))
        if ids[0] and not _get_runner_session_id(state):
            _set_runner_session_ids(state, session_id=ids[0])
        if ids[1] and not (state.get("runner_thread_id") or state.get("codex_thread_id")):
            _set_runner_session_ids(state, thread_id=ids[1])
        if _get_runner_session_id(state) is None and (
            state.get("runner_thread_id") or state.get("codex_thread_id")
        ):
            _set_runner_session_ids(
                state,
                session_id=str(state.get("runner_thread_id") or state.get("codex_thread_id")),
            )
        jsonl_summary = summarize_jsonl(Path(state["logs"]["jsonl"]))
        if jsonl_summary.get("session_id") and not _get_runner_session_id(state):
            _set_runner_session_ids(state, session_id=str(jsonl_summary["session_id"]))
        if jsonl_summary.get("thread_id") and not (
            state.get("runner_thread_id") or state.get("codex_thread_id")
        ):
            _set_runner_session_ids(state, thread_id=str(jsonl_summary["thread_id"]))
        _sync_session_aliases(state)
        runtime["jsonl_summary"] = jsonl_summary
        runtime["last_agent_message"] = jsonl_summary.get("last_agent_message")
        runtime["current_command"] = jsonl_summary.get("current_command")
        if not runtime["last_agent_message"]:
            last_message_text = _safe_read(Path(state["logs"]["last_message"])).strip()
            if last_message_text:
                runtime["last_agent_message"] = last_message_text[-1000:]
        jsonl_path, plain_path = _log_paths(state)
        sizes: list[int] = []
        mtimes: list[float] = []
        for path in (jsonl_path, plain_path):
            try:
                info = path.stat()
            except OSError:
                sizes.append(0)
                continue
            sizes.append(info.st_size)
            mtimes.append(info.st_mtime)
        previous_size = int(runtime.get("log_size") or 0)
        log_size = sum(sizes)
        grew = log_size > previous_size
        now = self.now()
        if grew:
            runtime["last_log_growth_at"] = now
        elif runtime.get("last_log_growth_at") is None and log_size:
            last_mtime = max(mtimes) if mtimes else time.time()
            runtime["last_log_growth_at"] = _datetime.datetime.fromtimestamp(
                last_mtime, tz=_datetime.timezone.utc
            ).isoformat().replace("+00:00", "Z")
        exit_code = _read_exit_code(Path(state["logs"]["exit_code"]))
        runtime.update(
            {
                "tmux_alive": tmux_alive,
                "process_running": tmux_alive and exit_code is None,
                "exit_code": exit_code,
                "jsonl_bytes": sizes[0],
                "plain_bytes": sizes[1],
                "log_size": log_size,
                "last_log_mtime": (
                    _datetime.datetime.fromtimestamp(max(mtimes), tz=_datetime.timezone.utc)
                    .isoformat()
                    .replace("+00:00", "Z")
                    if mtimes
                    else None
                ),
                "log_grew": grew,
                "capacity_signal": bool(jsonl_summary.get("capacity_error")),
            }
        )
        if tmux_warning:
            runtime["warning"] = tmux_warning
        elif isinstance(runtime.get("warning"), str) and runtime["warning"].startswith("tmux unavailable"):
            runtime["warning"] = None
        start_time = _timestamp(runtime.get("run_started_at"))
        if start_time is not None and runtime.get("process_running"):
            age = max(0, time.time() - start_time)
            growth_time = _timestamp(runtime.get("last_log_growth_at")) or start_time
            if time.time() - growth_time >= self.stall_seconds:
                runtime["warning"] = (
                    f"STALLED: no log growth for {int(time.time() - growth_time)}s; process was not killed"
                )
            runtime["active_seconds"] = int(age)
        else:
            runtime["active_seconds"] = None
        after = json.dumps(state, sort_keys=True, ensure_ascii=False)
        return before != after

    def _write_if_changed(self, before: str, state: dict[str, Any]) -> None:
        after = json.dumps(state, sort_keys=True, ensure_ascii=False)
        if before != after:
            state["updated_at"] = self.now()
            self.store.write(state)

    def register(self, manifest_path: Path | str) -> dict[str, Any]:
        manifest_path = Path(manifest_path).expanduser().resolve()
        if not manifest_path.is_file():
            raise ManifestError(f"manifest does not exist: {manifest_path}")
        try:
            with manifest_path.open("r", encoding="utf-8") as handle:
                manifest = json.load(handle)
        except (OSError, json.JSONDecodeError) as exc:
            raise ManifestError(f"cannot read manifest {manifest_path}: {exc}") from exc
        validate_manifest(manifest)
        git_context = validate_git_context(manifest, manifest_path=manifest_path)
        prompt_file, prompt_hash, authoritative_inputs = validate_manifest_files(
            manifest, manifest_path=manifest_path, worktree=Path(git_context["worktree"])
        )
        dependencies = dependency_snapshot(
            manifest, worktree=Path(git_context["worktree"])
        )
        task_id = manifest["task_id"]
        self.store.ensure_layout()
        with self.store.registry_lock():
            state_path = self.store.task_path(task_id)
            if state_path.exists():
                raise StateError(f"task already registered: {task_id}")
            existing = list(self.store.iter_states())
            conflict_forbidden = [] if manifest.get("role", "writer") == "reviewer" else normalize_write_set(
                manifest["forbidden_write_set"], "forbidden_write_set"
            )
            conflicts = find_write_conflicts(
                normalize_write_set(
                    manifest["owned_write_set"],
                    "owned_write_set",
                    allow_empty=manifest.get("role", "writer") == "reviewer",
                ),
                conflict_forbidden,
                existing,
            )
            conflicts.extend(
                find_semantic_lock_conflicts(
                    normalize_semantic_locks(manifest.get("semantic_locks", [])), existing
                )
            )
            if conflicts:
                raise StateError("write-set conflict/semantic-lock conflict; registration failed closed: " + "; ".join(conflicts))
            if manifest.get("role", "writer") == "reviewer":
                review_of = manifest.get("review_of") or manifest.get("writer_task_id")
                try:
                    review_target = next(item for item in existing if item.get("task_id") == review_of)
                except StopIteration as exc:
                    raise StateError(f"review target task is not registered: {review_of}") from exc
                fingerprint = _review_fingerprint(review_target)
                if any(
                    item.get("role") == "reviewer"
                    and item.get("review_of") == review_of
                    and item.get("review_completed")
                    and item.get("review_fingerprint") == fingerprint
                    for item in existing
                ):
                    raise StateError(
                        "duplicate review refused: no new commit, evidence, or contract change"
                    )
            task_dir = self.store.task_dir(task_id)
            task_dir.mkdir(parents=True, exist_ok=False)
            state = _initial_state(
                manifest,
                manifest_path=manifest_path,
                git_context=git_context,
                prompt_file=prompt_file,
                prompt_hash=prompt_hash,
                authoritative_inputs=authoritative_inputs,
                dependencies=dependencies,
                task_dir=task_dir,
            )
            self.store.write(state)
        return self.status(task_id)

    def _load_locked(self, task_id: str) -> dict[str, Any]:
        self.store.ensure_layout()
        return self.store.load(task_id)

    def _set_guard(self, state: dict[str, Any], guard: str, note: str) -> None:
        guards = state.setdefault("guards", [])
        if guard not in guards:
            guards.append(guard)
        state["guard"] = guard
        state.setdefault("runtime", {})["warning"] = f"{guard}: {note}"
        if state.get("status") not in TERMINAL_STATES and state.get("status") != "CONTRACT_CONFLICT":
            _set_status(state, "CONTRACT_CONFLICT", note=f"{guard}: {note}")

    def _clear_guard(self, state: dict[str, Any]) -> None:
        state["guards"] = []
        state["guard"] = None
        warning = state.setdefault("runtime", {}).get("warning")
        if isinstance(warning, str) and any(warning.startswith(name + ":") for name in (
            "CONTEXT_STALE", "BASE_STALE", "DEPENDENCY_STALE", "LOCK_CONFLICT", "POLICY_VIOLATION"
        )):
            state["runtime"]["warning"] = None

    def _write_set_violations(self, state: Mapping[str, Any]) -> list[str]:
        if state.get("role", "writer") == "reviewer":
            return []
        worktree = Path(str(state["worktree"]))
        try:
            changed = _git_changed_paths(worktree)
        except AgentCtlError as exc:
            return [str(exc)]
        owned = list(state.get("owned_write_set", []))
        forbidden = list(state.get("forbidden_write_set", []))
        violations: list[str] = []
        for path in changed:
            if any(path_matches_pattern(path, pattern) for pattern in forbidden):
                violations.append(f"forbidden path changed: {path}")
            elif not any(path_matches_pattern(path, pattern) for pattern in owned):
                violations.append(f"path outside owned_write_set: {path}")
        return violations

    def _context_issue(
        self, state: Mapping[str, Any], *, prompt_override: str | None = None
    ) -> tuple[str, str] | None:
        manifest_path = Path(str(state.get("manifest_path", "")))
        try:
            if sha256_file(manifest_path) != state.get("manifest_sha256"):
                return "CONTEXT_STALE", f"manifest hash drift: {manifest_path}"
        except ManifestError as exc:
            return "CONTEXT_STALE", str(exc)
        worktree = Path(str(state["worktree"]))
        prompt_path = Path(prompt_override).expanduser().resolve() if prompt_override else Path(str(state["prompt_file"]))
        try:
            prompt_hash = sha256_file(prompt_path)
        except ManifestError as exc:
            return "CONTEXT_STALE", str(exc)
        if prompt_override is None and prompt_hash != state.get("prompt_sha256"):
            return "CONTEXT_STALE", f"prompt hash drift: {prompt_path}"
        for item in state.get("context_pack", {}).get("authoritative_inputs", []):
            path = Path(str(item.get("path", "")))
            try:
                actual = sha256_file(path)
            except ManifestError as exc:
                return "CONTEXT_STALE", str(exc)
            if actual != item.get("sha256"):
                return "CONTEXT_STALE", f"authoritative input hash drift: {path}"
        try:
            branch = _git_output(worktree, ["symbolic-ref", "--quiet", "--short", "HEAD"], check=False)
            head = _git_output(worktree, ["rev-parse", "HEAD"])
        except AgentCtlError as exc:
            return "BASE_STALE", str(exc)
        if branch != state.get("branch") or head != state.get("base_commit"):
            return "BASE_STALE", f"registered {state.get('branch')}@{state.get('base_commit')}, current {branch}@{head}"
        dependency_problem = dependency_snapshot_stale(state.get("dependencies", []))
        if dependency_problem:
            return "DEPENDENCY_STALE", dependency_problem
        conflicts = find_semantic_lock_conflicts(
            state.get("semantic_locks", []), list(self.store.iter_states()), task_id=str(state["task_id"])
        )
        if conflicts:
            return "LOCK_CONFLICT", "; ".join(conflicts)
        violations = self._write_set_violations(state)
        if violations:
            return "POLICY_VIOLATION", "; ".join(violations)
        return None

    def _revalidate_or_raise(
        self, state: dict[str, Any], *, prompt_override: str | None = None
    ) -> None:
        issue = self._context_issue(state, prompt_override=prompt_override)
        if issue is None:
            self._clear_guard(state)
            return
        guard, note = issue
        self._set_guard(state, guard, note)
        self.store.write(state)
        raise StateError(f"{guard}: {note}")

    def _active_task_count(self, *, exclude: str | None = None) -> int:
        count = 0
        for state in self.store.iter_states():
            if exclude and state.get("task_id") == exclude:
                continue
            if state.get("status") not in {
                "RUNNING",
                "SUSPECTED_STALL",
                "WAITING_CAPACITY",
                # A migrated old file may still describe a live run using its
                # phase name before the next watch normalizes it to RUNNING.
                "IMPLEMENT",
                "FIX",
            }:
                continue
            exit_code = _read_exit_code(Path(str(state.get("logs", {}).get("exit_code", ""))))
            if exit_code is not None:
                continue
            try:
                if self.tmux.has_session(str(state.get("tmux_session"))):
                    count += 1
            except (AgentCtlError, OSError):
                # An unknown process is treated as occupied: scheduling must
                # fail closed rather than start a fourth Agent.
                count += 1
        return count

    def _prepare_prompt(self, state: Mapping[str, Any], override: str | None = None) -> tuple[Path, str]:
        raw = override if override is not None else str(state["prompt_file"])
        base = Path.cwd() if override is not None else Path(state["worktree"])
        prompt = _resolved_file(raw, base)
        if not prompt.is_file():
            raise AgentCtlError(f"prompt file does not exist: {prompt}")
        digest = sha256_file(prompt)
        if override is None and state.get("prompt_sha256") and digest != state["prompt_sha256"]:
            raise AgentCtlError(
                f"prompt hash drift: registered {state['prompt_sha256']}, current {digest}; "
                "use resume --prompt to make an explicit change"
            )
        return prompt, digest

    def _clear_exit_code(self, state: Mapping[str, Any]) -> None:
        atomic_write_text(Path(state["logs"]["exit_code"]), "")

    def _runner_command(
        self,
        *,
        state: Mapping[str, Any],
        codex_argv: Sequence[str],
        prompt_file: Path,
        append_logs: bool,
        prompt_mode: str = "stdin",
    ) -> list[str]:
        script = Path(__file__).resolve()
        return [
            sys.executable,
            str(script),
            "_run-codex",
            "--codex-argv-json",
            json.dumps(list(codex_argv), ensure_ascii=False, separators=(",", ":")),
            "--worktree",
            str(state["worktree"]),
            "--prompt-file",
            str(prompt_file),
            "--prompt-mode",
            prompt_mode,
            "--jsonl",
            str(state["logs"]["jsonl"]),
            "--plain-log",
            str(state["logs"]["plain"]),
            "--last-message",
            str(state["logs"]["last_message"]),
            "--exit-code",
            str(state["logs"]["exit_code"]),
            "--pid-file",
            str(state["logs"]["runner_pid"]),
            "--state-path",
            str(self.store.task_path(state["task_id"])),
            "--lock-path",
            str(self.store.locks_dir / f"{state['task_id']}.lock"),
            *(["--append"] if append_logs else []),
        ]

    def _session_active(self, state: Mapping[str, Any]) -> bool:
        try:
            return bool(self.tmux.has_session(state["tmux_session"])) and (
                _read_exit_code(Path(state["logs"]["exit_code"])) is None
            )
        except (AgentCtlError, OSError):
            return False

    def _recycle_finished_session(self, state: Mapping[str, Any]) -> None:
        try:
            has_session = bool(self.tmux.has_session(state["tmux_session"]))
        except (AgentCtlError, OSError):
            has_session = False
        if not has_session:
            return
        if _read_exit_code(Path(state["logs"]["exit_code"])) is None:
            raise AgentCtlError(
                f"tmux session is already active: {state['tmux_session']}; use status or stop"
            )
        self.tmux.kill_session(state["tmux_session"])

    def _profile_slug(self, profile_id: str | None, role: str | None) -> str:
        raw = profile_id or role or "run"
        slug = re.sub(r"[^a-z0-9]+", "-", str(raw).lower()).strip("-")
        return (slug or "run")[:48]

    def _bind_run_logs(
        self,
        state: dict[str, Any],
        *,
        mode: str,
        model: str,
        prompt_file: Path,
    ) -> dict[str, str]:
        """Create runs/<run_id>/ artifacts and point state logs at them."""

        task_id = str(state["task_id"])
        task_dir = self.store.task_dir(task_id)
        run_count = int(state.get("run_count") or 0) + 1
        run_id = f"run-{run_count:03d}-{self._profile_slug(state.get('active_profile'), state.get('role'))}"
        run_dir = task_dir / "runs" / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        logs = {
            "jsonl": str(run_dir / "raw-events.jsonl"),
            "plain": str(run_dir / "runner.log"),
            "last_message": str(run_dir / "last-message.md"),
            "exit_code": str(run_dir / "exit-code"),
            "runner_pid": str(run_dir / "runner.pid"),
        }
        for path in logs.values():
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        # Create empty artifacts immediately so stop/status never races the worker
        # process opening files (e.g. kill during FAKE_CODEX_SLEEP).
        for key in ("jsonl", "plain", "last_message", "runner_pid"):
            path = Path(logs[key])
            if not path.exists():
                path.write_text("", encoding="utf-8")
        # Compatibility aliases at the task root for older status tooling.
        for alias, target_name in (
            ("codex.jsonl", "jsonl"),
            ("codex.log", "plain"),
            ("last-message.txt", "last_message"),
            ("exit-code", "exit_code"),
            ("runner.pid", "runner_pid"),
        ):
            alias_path = task_dir / alias
            target = Path(logs[target_name])
            with contextlib.suppress(OSError):
                if alias_path.is_symlink() or alias_path.exists():
                    alias_path.unlink()
                alias_path.symlink_to(target)
        meta = {
            "run_id": run_id,
            "task_id": task_id,
            "mode": mode,
            "runner_kind": state.get("runner_kind"),
            "profile_id": state.get("active_profile"),
            "model": model,
            "role": state.get("role"),
            "read_only": bool(state.get("read_only")),
            "prompt_file": str(prompt_file),
            "started_at": self.now(),
            "parent_run_id": state.get("current_run_id"),
        }
        atomic_write_json(run_dir / "meta.json", meta)
        history = list(state.get("runs") or [])
        history.append(meta)
        state["runs"] = history[-50:]
        state["logs"] = logs
        state["current_run_id"] = run_id
        state["run_count"] = run_count
        return logs

    def _launch(
        self,
        state: dict[str, Any],
        *,
        mode: str,
        prompt_file: Path,
        model: str,
        reasoning_effort: str,
        session_id: str | None = None,
        force_fresh_logs: bool = False,
        retain_registered_prompt: bool = True,
    ) -> None:
        self._recycle_finished_session(state)
        runner_kind = str(state.get("runner_kind") or "codex-cli")
        self._select_runner(runner_kind)
        worktree = Path(state["worktree"])
        read_only = state.get("role") == "reviewer" or state.get("read_only") is True
        logs = self._bind_run_logs(state, mode=mode, model=model, prompt_file=prompt_file)
        runtime = state["runtime"]
        runtime["run_log_offsets"] = {"jsonl": 0, "plain": 0}
        self._clear_exit_code(state)
        last_message = Path(logs["last_message"])
        if mode == "initial":
            launch_argv = self.runner.build_initial_argv(
                model=model,
                reasoning_effort=reasoning_effort,
                worktree=worktree,
                last_message=last_message,
                read_only=read_only,
            )
        else:
            if not session_id:
                raise AgentCtlError("cannot resume without a runner session id")
            launch_argv = self.runner.build_resume_argv(
                session_id=session_id,
                model=model,
                reasoning_effort=reasoning_effort,
                worktree=worktree,
                last_message=last_message,
                read_only=read_only,
            )
        prompt_mode = str(getattr(self.runner, "prompt_mode", "stdin"))
        # Each Run directory starts empty; force_fresh_logs is retained so
        # callers can document provider switches without changing bind behavior.
        _ = force_fresh_logs
        runner = self._runner_command(
            state=state,
            codex_argv=launch_argv,
            prompt_file=prompt_file,
            append_logs=False,
            prompt_mode=prompt_mode,
        )
        self.tmux.new_session(state["tmux_session"], worktree, runner)
        state["run_mode"] = mode
        state["model"] = model
        state["reasoning_effort"] = reasoning_effort
        state["runner_kind"] = runner_kind
        if retain_registered_prompt:
            state["prompt_file"] = str(prompt_file)
            state["prompt_sha256"] = sha256_file(prompt_file)
        state["runtime"]["run_started_at"] = self.now()
        state["runtime"]["warning"] = None
        state["last_checkpoint"] = None
        _set_status(state, "RUNNING", note=f"{mode} {runner_kind} {state['current_run_id']} started")

    def _start_once(self, task_id: str) -> dict[str, Any]:
        with self.store.lock(task_id):
            state = self._load_locked(task_id)
            before = json.dumps(state, sort_keys=True, ensure_ascii=False)
            self._refresh_runtime(state)
            if state["status"] not in {"REGISTERED", "READY", "QUEUED"}:
                raise StateError(
                    f"start requires REGISTERED, READY, or QUEUED; current state is {state['status']} (use resume for recovery)"
                )
            self._revalidate_or_raise(state)
            if self._active_task_count(exclude=task_id) >= self.max_concurrency:
                if state["status"] != "QUEUED":
                    _set_status(state, "QUEUED", note=f"max_concurrency={self.max_concurrency}; waiting for a slot")
                state["queue_reason"] = f"max_concurrency={self.max_concurrency}"
                state["updated_at"] = self.now()
                self.store.write(state)
                return self._status_view(state)
            if state.get("role") == "reviewer":
                review_of = state.get("review_of")
                if not review_of:
                    raise StateError("reviewer manifest has no review_of target")
                target = self.store.load(str(review_of))
                fingerprint = _review_fingerprint(target)
                for other in self.store.iter_states():
                    if (
                        other.get("role") == "reviewer"
                        and other.get("review_of") == review_of
                        and other.get("review_completed")
                        and other.get("review_fingerprint") == fingerprint
                    ):
                        raise StateError(
                            "duplicate review refused: no new commit, evidence, or contract change"
                        )
                state["review_fingerprint"] = fingerprint
                state["review_baseline_fingerprint"] = _changed_files_fingerprint(
                    Path(state["worktree"])
                )
            queued_resume = state.get("status") == "QUEUED" and state.get("queue_mode") == "resume"
            prompt, prompt_hash = self._prepare_prompt(state)
            launch_mode = "initial"
            launch_session_id = None
            launch_model = state["model"]
            launch_effort = state["reasoning_effort"]
            if queued_resume:
                launch_mode = "resume"
                launch_session_id = _get_runner_session_id(state)
                if not launch_session_id:
                    raise AgentCtlError("queued resume has no runner session/thread id")
                launch_model = state.get("queued_model") or state["model"]
                launch_effort = state.get("queued_reasoning_effort") or state["reasoning_effort"]
                queued_phase = state.get("queued_phase")
                if queued_phase:
                    validate_phase_transition(state["phase"], canonical_phase(str(queued_phase)))
                    state["phase"] = canonical_phase(str(queued_phase))
            self._launch(
                state,
                mode=launch_mode,
                prompt_file=prompt,
                model=launch_model,
                reasoning_effort=launch_effort,
                session_id=launch_session_id,
            )
            state["prompt_sha256"] = prompt_hash
            state["queue_reason"] = None
            state["queue_mode"] = None
            state["queued_model"] = None
            state["queued_reasoning_effort"] = None
            state["queued_prompt"] = None
            state["queued_phase"] = None
            state["updated_at"] = self.now()
            self.store.write(state)
        return self._status_view(state)

    def start(self, task_id: str) -> dict[str, Any]:
        with self.store.scheduler_lock():
            self._start_once(task_id)
        return self.status(task_id)

    def _verify_dir(self, task_id: str) -> Path:
        path = self.store.task_dir(task_id) / "verify"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _bugpack_dir(self, task_id: str) -> Path:
        path = self.store.task_dir(task_id) / "bugpacks"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _next_artifact_id(self, directory: Path, prefix: str) -> str:
        existing = sorted(directory.glob(f"{prefix}-*.json"))
        return f"{prefix}-{len(existing) + 1:03d}"

    def _latest_verify_path(self, task_id: str) -> Path | None:
        directory = self._verify_dir(task_id)
        items = sorted(directory.glob("verify-*.json"))
        return items[-1] if items else None

    def _load_verify_artifact(self, task_id: str, verify_id: str | None) -> dict[str, Any]:
        if verify_id:
            path = self._verify_dir(task_id) / f"{verify_id}.json"
            if not path.is_file() and not verify_id.startswith("verify-"):
                path = self._verify_dir(task_id) / f"verify-{verify_id}.json"
        else:
            found = self._latest_verify_path(task_id)
            if found is None:
                raise StateError("no verify artifact found; run agentctl verify first")
            path = found
        if not path.is_file():
            raise StateError(f"verify artifact missing: {path}")
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise StateError(f"cannot read verify artifact {path}: {exc}") from exc
        if not isinstance(raw, Mapping):
            raise StateError(f"invalid verify artifact: {path}")
        raw = dict(raw)
        raw["_path"] = str(path)
        return raw

    def _build_bug_pack(
        self,
        state: Mapping[str, Any],
        *,
        verify: Mapping[str, Any],
        profile_id: str,
    ) -> Path:
        task_id = str(state["task_id"])
        pack_id = self._next_artifact_id(self._bugpack_dir(task_id), "bugpack")
        failed = [
            item
            for item in verify.get("commands", [])
            if isinstance(item, Mapping) and item.get("exit_code") not in (0, "0", None)
        ]
        if not failed and verify.get("result") == "PASS":
            raise StateError("refusing to build Bug Pack from a PASS verify artifact")
        owned = list(state.get("owned_write_set") or [])
        forbidden = list(state.get("forbidden_write_set") or [])
        payload = {
            "bugpack_id": pack_id,
            "task_id": task_id,
            "profile_id": profile_id,
            "from_verify": verify.get("verify_id"),
            "goal": (
                f"Fix the failing acceptance checks for task {task_id}. "
                "Stay inside owned_write_set and do not expand scope."
            ),
            "non_goals": [
                "Do not redesign the multi-model controller.",
                "Do not modify forbidden_write_set paths.",
                "Do not delete or weaken acceptance tests to force a pass.",
            ],
            "failed_commands": failed,
            "failure_signatures": list(verify.get("failure_signatures") or []),
            "owned_write_set": owned,
            "forbidden_write_set": forbidden,
            "diff_fingerprint": _git_diff_fingerprint(Path(str(state["worktree"]))),
            "invariants": list(state.get("context_pack", {}).get("frozen_summary") or []),
            "acceptance": list(
                state.get("context_pack", {})
                .get("acceptance_contract", {})
                .get("acceptance_commands")
                or state.get("manifest", {}).get("acceptance_commands")
                or []
            ),
            "generated_at": self.now(),
        }
        json_path = self._bugpack_dir(task_id) / f"{pack_id}.json"
        md_path = self._bugpack_dir(task_id) / f"{pack_id}.md"
        atomic_write_json(json_path, payload)
        failed_blocks = []
        for item in failed:
            failed_blocks.append(
                "\n".join(
                    [
                        f"### `{item.get('command')}`",
                        f"exit_code={item.get('exit_code')}",
                        "stdout:",
                        "```",
                        str(item.get("stdout") or "")[-4000:],
                        "```",
                        "stderr:",
                        "```",
                        str(item.get("stderr") or "")[-4000:],
                        "```",
                    ]
                )
            )
        def _bullets(items: Sequence[Any], *, empty: str, fmt: str = "- {}") -> list[str]:
            values = [fmt.format(item) for item in items]
            return values or [empty]

        markdown_lines = [
            f"# Bug Pack {pack_id}",
            "",
            f"Task: `{task_id}`",
            f"Fixer profile: `{profile_id}`",
            f"From verify: `{verify.get('verify_id')}`",
            "",
            "## Goal",
            payload["goal"],
            "",
            "## Non-goals",
            *_bullets(payload["non_goals"], empty="- (none)"),
            "",
            "## Invariants",
            *_bullets(payload["invariants"], empty="- (none declared)"),
            "",
            "## Allowed paths",
            *_bullets(owned, empty="- (none)", fmt="- `{}`"),
            "",
            "## Forbidden paths",
            *_bullets(forbidden, empty="- (none)", fmt="- `{}`"),
            "",
            "## Failure signatures",
            *_bullets(payload["failure_signatures"], empty="- (none)", fmt="- `{}`"),
            "",
            "## Failed commands",
            *(failed_blocks or ["- (none)"]),
            "",
            "## Acceptance to re-run",
            *_bullets(payload["acceptance"], empty="- (none)", fmt="- `{}`"),
            "",
            "After editing, do not claim success. `agentctl verify` decides.",
            "",
        ]
        markdown = "\n".join(markdown_lines)
        atomic_write_text(md_path, markdown)
        return md_path

    def verify(self, task_id: str) -> dict[str, Any]:
        """Run Manifest acceptance_commands and persist a verify artifact."""

        with self.store.lock(task_id):
            state = self._load_locked(task_id)
            if self._session_active(state):
                raise AgentCtlError(
                    f"task is still running in {state['tmux_session']}; stop or wait before verify"
                )
            self._revalidate_or_raise(state)
            commands = list(
                state.get("context_pack", {})
                .get("acceptance_contract", {})
                .get("acceptance_commands")
                or state.get("manifest", {}).get("acceptance_commands")
                or []
            )
            if not commands:
                raise StateError("manifest has no acceptance_commands")
            worktree = Path(str(state["worktree"]))
            results: list[dict[str, Any]] = []
            failure_signatures: list[str] = []
            overall = "PASS"
            for command in commands:
                completed = subprocess.run(
                    command,
                    shell=True,
                    cwd=str(worktree),
                    text=True,
                    capture_output=True,
                    env=os.environ.copy(),
                )
                item = {
                    "command": command,
                    "exit_code": completed.returncode,
                    "stdout": completed.stdout[-8000:],
                    "stderr": completed.stderr[-8000:],
                }
                results.append(item)
                if completed.returncode != 0:
                    overall = "CODE_FAILURE"
                    signature = command
                    for line in (completed.stderr or completed.stdout or "").splitlines():
                        if "FAIL:" in line or "ERROR:" in line or "AssertionError" in line:
                            signature = line.strip()[:240]
                            break
                    failure_signatures.append(signature)
            verify_id = self._next_artifact_id(self._verify_dir(task_id), "verify")
            artifact = {
                "verify_id": verify_id,
                "task_id": task_id,
                "result": overall,
                "commands": results,
                "failure_signatures": failure_signatures,
                "diff_fingerprint": _git_diff_fingerprint(worktree),
                "generated_at": self.now(),
            }
            atomic_write_json(self._verify_dir(task_id) / f"{verify_id}.json", artifact)
            previous_phase = str(state.get("phase") or "")
            previous_profile = str(state.get("active_profile") or "")
            previous_runner = str(state.get("runner_kind") or "")
            validate_phase_transition(state["phase"], "VERIFY")
            state["phase"] = "VERIFY"
            state["last_verify_id"] = verify_id
            state["last_verify_result"] = overall
            state["last_failure_signatures"] = failure_signatures
            fixer_profile = str((state.get("execution_policy") or {}).get("fixer_profile") or "")
            backup_profile = str((state.get("execution_policy") or {}).get("backup_profile") or "")
            came_from_fixer = (
                previous_phase == "FIX"
                or previous_profile in {fixer_profile, backup_profile}
                or previous_runner in {"cursor-cli", "codebuddy-cli"}
            )
            if overall == "CODE_FAILURE" and came_from_fixer:
                prior = set(state.get("prior_failure_signatures") or [])
                current = set(failure_signatures)
                if prior and current & prior:
                    state["same_failure_signature_count"] = int(
                        state.get("same_failure_signature_count") or 0
                    ) + 1
                else:
                    state["same_failure_signature_count"] = 1
                state["repair_cycle_count"] = int(state.get("repair_cycle_count") or 0) + 1
                state["prior_failure_signatures"] = failure_signatures
            elif overall == "PASS":
                state["same_failure_signature_count"] = 0
                state["prior_failure_signatures"] = []
            if overall == "PASS":
                _set_status(state, "READY_FOR_REVIEW", note=f"{verify_id} PASS")
            else:
                if state["status"] not in {"READY_FOR_REVIEW", "FIX", "READY", "IMPLEMENT"}:
                    _set_status(state, "READY_FOR_REVIEW", note=f"{verify_id} {overall}")
                else:
                    _record_history(
                        state,
                        old_status=state["status"],
                        new_status=state["status"],
                        note=f"{verify_id} {overall}",
                    )
            brief = self._write_controller_brief(state)
            state["updated_at"] = self.now()
            self.store.write(state)
            view = self._status_view(state)
            view["verify"] = artifact
            view["controller_brief"] = brief
            return view

    def fix(
        self,
        task_id: str,
        *,
        model: str | None = None,
        reasoning_effort: str | None = None,
        prompt: str | None = None,
        profile: str | None = None,
        from_verify: str | None = None,
    ) -> dict[str, Any]:
        """Run a focused fix.

        Without --profile, resume the original writer Thread (legacy CP1 path).
        With --profile (or the Manifest fixer_profile), start a fresh Fixer Run
        for that Profile and feed it a Bug Pack derived from verify evidence.
        """

        if profile is None and from_verify is None:
            return self.resume(
                task_id,
                model=model,
                reasoning_effort=reasoning_effort,
                prompt=prompt,
                phase="FIX",
                _writer_only=True,
            )

        with self.store.scheduler_lock():
            with self.store.lock(task_id):
                state = self._load_locked(task_id)
                self._refresh_runtime(state)
                if state.get("role", "writer") != "writer":
                    raise StateError("fix requires a writer task; reviewer recovery is forbidden")
                if self._session_active(state):
                    raise AgentCtlError(f"task is already running in {state['tmux_session']}")
                if state["status"] in {"DONE", "CONTRACT_CONFLICT"}:
                    raise StateError(f"cannot fix task in state {state['status']}")
                verify = self._load_verify_artifact(task_id, from_verify)
                if verify.get("result") == "PASS":
                    raise StateError("latest verify PASS; refusing fixer launch without failures")
                profiles = load_profiles()
                policy = dict(state.get("execution_policy") or DEFAULT_EXECUTION_POLICY)
                profile_id = profile or str(policy.get("fixer_profile") or "cursor-grok45-fixer")
                resolved = resolve_profile(profile_id, profiles)
                if resolved["runner"] not in IMPLEMENTED_RUNNERS:
                    raise AgentCtlError(
                        f"fixer profile {profile_id} uses unimplemented runner {resolved['runner']}"
                    )
                bug_pack = (
                    Path(prompt).expanduser().resolve()
                    if prompt
                    else self._build_bug_pack(state, verify=verify, profile_id=profile_id)
                )
                if not bug_pack.is_file():
                    raise AgentCtlError(f"bug pack prompt missing: {bug_pack}")
                self._revalidate_or_raise(state, prompt_override=str(bug_pack))
                if self._active_task_count(exclude=task_id) >= self.max_concurrency:
                    raise AgentCtlError(
                        f"max_concurrency={self.max_concurrency}; free a slot before fixer launch"
                    )
                previous_kind = str(state.get("runner_kind") or "codex-cli")
                selected_model = model or profile_cli_model(resolved)
                selected_effort = reasoning_effort or str(resolved.get("reasoning_effort") or "high")
                validate_phase_transition(state["phase"], "FIX")
                state["phase"] = "FIX"
                state["active_profile"] = profile_id
                state["runner_kind"] = resolved["runner"]
                state["read_only"] = resolved.get("write_mode") == "read-only"
                state["last_bugpack"] = str(bug_pack)
                # Provider switches cannot resume a foreign session.
                if previous_kind != resolved["runner"] or bool(resolved.get("fresh_session")):
                    state["runner_session_id"] = None
                    state["runner_thread_id"] = None
                    state["codex_session_id"] = None
                    state["codex_thread_id"] = None
                if state.get("last_diagnosis_path"):
                    state["post_diagnosis_fix_count"] = int(
                        state.get("post_diagnosis_fix_count") or 0
                    ) + 1
                self._launch(
                    state,
                    mode="initial",
                    prompt_file=bug_pack,
                    model=selected_model,
                    reasoning_effort=selected_effort,
                    force_fresh_logs=previous_kind != resolved["runner"],
                    retain_registered_prompt=False,
                )
                self._write_controller_brief(state)
                state["updated_at"] = self.now()
                self.store.write(state)
        return self.status(task_id)

    def _findings_dir(self, task_id: str) -> Path:
        path = self.store.task_dir(task_id) / "findings"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _diagnosis_dir(self, task_id: str) -> Path:
        path = self.store.task_dir(task_id) / "diagnosis"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _write_controller_brief(self, state: dict[str, Any]) -> dict[str, Any]:
        task_id = str(state["task_id"])
        suggestions = self._suggest_next_actions(state)
        required = []
        findings_path = state.get("last_findings_path")
        if findings_path and Path(str(findings_path)).is_file():
            with contextlib.suppress(OSError, json.JSONDecodeError):
                payload = json.loads(Path(str(findings_path)).read_text(encoding="utf-8"))
                for item in payload.get("findings") or []:
                    if isinstance(item, Mapping) and str(item.get("severity", "")).upper() in {
                        "HIGH",
                        "REQUIRED",
                        "BLOCKER",
                    }:
                        required.append(str(item.get("finding_id") or item.get("id") or "?"))
                if payload.get("verdict") == "REQUIRED_FIX":
                    for item in payload.get("findings") or []:
                        if isinstance(item, Mapping):
                            fid = str(item.get("finding_id") or item.get("id") or "")
                            if fid and fid not in required:
                                required.append(fid)
        failed_commands: list[Any] = []
        if state.get("last_verify_id"):
            with contextlib.suppress(StateError, OSError, json.JSONDecodeError, TypeError):
                verify = self._load_verify_artifact(task_id, str(state.get("last_verify_id")))
                failed_commands = [
                    item.get("command")
                    for item in verify.get("commands") or []
                    if isinstance(item, Mapping) and item.get("exit_code") not in (0, "0", None)
                ]
        brief = {
            "task_id": task_id,
            "phase": state.get("phase"),
            "status": state.get("status"),
            "current_profile": state.get("active_profile"),
            "runner_kind": state.get("runner_kind"),
            "current_run_id": state.get("current_run_id"),
            "repair_cycle_count": int(state.get("repair_cycle_count") or 0),
            "same_failure_signature_count": int(state.get("same_failure_signature_count") or 0),
            "review_round_count": int(state.get("review_round_count") or 0),
            "post_diagnosis_fix_count": int(state.get("post_diagnosis_fix_count") or 0),
            "last_verify_result": state.get("last_verify_result"),
            "failed_commands": failed_commands,
            "failure_signatures": list(state.get("last_failure_signatures") or []),
            "required_findings": required,
            "suggested_next_actions": suggestions,
            "generated_at": self.now(),
        }
        task_dir = self.store.task_dir(task_id)
        json_path = task_dir / "controller-brief.json"
        md_path = task_dir / "controller-brief.md"
        atomic_write_json(json_path, brief)

        def _bullets(items: Sequence[Any], *, empty: str, fmt: str = "- {}") -> list[str]:
            values = [fmt.format(item) for item in items]
            return values or [empty]

        lines = [
            f"# Controller Brief · {task_id}",
            "",
            f"- phase: `{brief['phase']}`",
            f"- status: `{brief['status']}`",
            f"- profile: `{brief['current_profile']}`",
            f"- repair_cycle_count: {brief['repair_cycle_count']}",
            f"- same_failure_signature_count: {brief['same_failure_signature_count']}",
            f"- last_verify_result: `{brief['last_verify_result']}`",
            "",
            "## Suggested next actions",
            *_bullets(brief["suggested_next_actions"], empty="- WAIT_CONTROLLER"),
            "",
            "## Failure signatures",
            *_bullets(brief["failure_signatures"], empty="- (none)", fmt="- `{}`"),
            "",
            "## Required findings",
            *_bullets(brief["required_findings"], empty="- (none)", fmt="- `{}`"),
            "",
        ]
        atomic_write_text(md_path, "\n".join(lines))
        state["controller_brief_path"] = str(json_path)
        return brief

    def _suggest_next_actions(self, state: Mapping[str, Any]) -> list[str]:
        policy = state.get("execution_policy") or DEFAULT_EXECUTION_POLICY
        max_fix = int(policy.get("max_fix_cycles_before_diagnosis") or 2)
        max_post = int(policy.get("max_post_diagnosis_fix_cycles") or 1)
        repair = int(state.get("repair_cycle_count") or 0)
        post = int(state.get("post_diagnosis_fix_count") or 0)
        verify_result = state.get("last_verify_result")
        findings_verdict = state.get("last_findings_verdict")
        suggestions: list[str] = []
        if state.get("guard") in {"CONTRACT_CONFLICT", "MODEL_MISMATCH", "POLICY_VIOLATION"}:
            return ["NEEDS_HUMAN", "WAIT_CONTROLLER"]
        if verify_result == "CODE_FAILURE":
            if repair >= max_fix and post >= max_post:
                suggestions.extend(["SUGGEST_CODEBUDDY_BACKUP", "NEEDS_HUMAN", "WAIT_CONTROLLER"])
            elif repair >= max_fix:
                suggestions.append("SUGGEST_SOL_DIAGNOSE")
            else:
                suggestions.append("SUGGEST_GROK_FIX")
        elif verify_result == "PASS":
            if findings_verdict == "REQUIRED_FIX":
                suggestions.append("SUGGEST_GROK_FIX")
            elif findings_verdict == "PASS" and state.get("review_completed"):
                suggestions.append("READY_FOR_HANDOFF")
            else:
                suggestions.append("SUGGEST_LUNA_REVIEW")
        else:
            suggestions.append("WAIT_CONTROLLER")
        return suggestions

    def next_action(self, task_id: str) -> dict[str, Any]:
        with self.store.lock(task_id):
            state = self._load_locked(task_id)
            brief = self._write_controller_brief(state)
            state["updated_at"] = self.now()
            self.store.write(state)
            return {
                "task_id": task_id,
                "suggested_next_actions": brief["suggested_next_actions"],
                "controller_brief": brief,
            }

    def runs(self, task_id: str) -> dict[str, Any]:
        with self.store.lock(task_id):
            state = self._load_locked(task_id)
            return {
                "task_id": task_id,
                "current_run_id": state.get("current_run_id"),
                "run_count": state.get("run_count", 0),
                "runs": list(state.get("runs") or []),
            }

    def _launch_profile_run(
        self,
        state: dict[str, Any],
        *,
        profile_id: str,
        phase: str,
        prompt_file: Path,
        clear_session: bool = True,
        role_override: str | None = None,
    ) -> None:
        profiles = load_profiles()
        resolved = resolve_profile(profile_id, profiles)
        if resolved["runner"] not in IMPLEMENTED_RUNNERS:
            raise AgentCtlError(
                f"profile {profile_id} uses unimplemented runner {resolved['runner']}"
            )
        previous_kind = str(state.get("runner_kind") or "codex-cli")
        validate_phase_transition(state["phase"], phase)
        state["phase"] = phase
        state["active_profile"] = profile_id
        state["runner_kind"] = resolved["runner"]
        state["read_only"] = resolved.get("write_mode") == "read-only"
        if role_override:
            state["role"] = role_override
        elif resolved.get("role") in {"writer", "reviewer"}:
            # Keep task role as writer for fixer/diagnoser/backup; only reviewer
            # profile flips the temporary run role semantics via read_only.
            pass
        if clear_session or previous_kind != resolved["runner"] or resolved.get("fresh_session"):
            state["runner_session_id"] = None
            state["runner_thread_id"] = None
            state["codex_session_id"] = None
            state["codex_thread_id"] = None
        self._launch(
            state,
            mode="initial",
            prompt_file=prompt_file,
            model=profile_cli_model(resolved),
            reasoning_effort=str(resolved.get("reasoning_effort") or "high"),
            force_fresh_logs=True,
            retain_registered_prompt=False,
        )

    def review(
        self,
        task_id: str,
        *,
        profile: str | None = None,
    ) -> dict[str, Any]:
        """Start a fresh read-only Luna Reviewer Session and request Findings."""

        with self.store.scheduler_lock():
            with self.store.lock(task_id):
                state = self._load_locked(task_id)
                self._refresh_runtime(state)
                if self._session_active(state):
                    raise AgentCtlError(f"task is already running in {state['tmux_session']}")
                verify = self._load_verify_artifact(task_id, state.get("last_verify_id"))
                if verify.get("result") != "PASS":
                    raise StateError("review requires a PASS verify artifact")
                policy = dict(state.get("execution_policy") or DEFAULT_EXECUTION_POLICY)
                profile_id = profile or str(policy.get("reviewer_profile") or "codex-luna-review")
                if policy.get("require_fresh_review_session", True) is False:
                    pass
                findings_id = self._next_artifact_id(self._findings_dir(task_id), "findings")
                findings_path = self._findings_dir(task_id) / f"{findings_id}.json"
                fingerprint = _git_diff_fingerprint(Path(str(state["worktree"])))
                state["review_target_fingerprint"] = fingerprint
                state["pending_findings_path"] = str(findings_path)
                state["review_invalidated"] = False
                prompt = self._write_review_pack(
                    state,
                    verify=verify,
                    findings_path=findings_path,
                    fingerprint=fingerprint,
                )
                self._revalidate_or_raise(state, prompt_override=str(prompt))
                if self._active_task_count(exclude=task_id) >= self.max_concurrency:
                    raise AgentCtlError("max_concurrency reached; free a slot before review")
                state["read_only"] = True
                self._launch_profile_run(
                    state,
                    profile_id=profile_id,
                    phase="FOCUSED_REVIEW",
                    prompt_file=prompt,
                    clear_session=True,
                )
                state["review_round_count"] = int(state.get("review_round_count") or 0) + 1
                self._write_controller_brief(state)
                state["updated_at"] = self.now()
                self.store.write(state)
        return self.status(task_id)

    def _write_review_pack(
        self,
        state: Mapping[str, Any],
        *,
        verify: Mapping[str, Any],
        findings_path: Path,
        fingerprint: str,
    ) -> Path:
        task_id = str(state["task_id"])
        pack_dir = self.store.task_dir(task_id) / "review-packs"
        pack_dir.mkdir(parents=True, exist_ok=True)
        pack_id = self._next_artifact_id(pack_dir, "reviewpack")
        path = pack_dir / f"{pack_id}.md"
        owned = list(state.get("owned_write_set") or [])
        forbidden = list(state.get("forbidden_write_set") or [])
        frozen = list(state.get("context_pack", {}).get("frozen_summary") or [])
        owned_lines = [f"- `{item}`" for item in owned] or ["- (none)"]
        forbidden_lines = [f"- `{item}`" for item in forbidden] or ["- (none)"]
        frozen_lines = [f"- {item}" for item in frozen] or ["- (none)"]
        body = "\n".join(
            [
                f"# Review Pack {pack_id}",
                "",
                "You are an independent read-only reviewer. Do not modify files.",
                "Do not resume any prior writer conversation assumptions.",
                "",
                f"Task: `{task_id}`",
                f"Diff fingerprint: `{fingerprint}`",
                f"Verify: `{verify.get('verify_id')}` result=`{verify.get('result')}`",
                "",
                "## Owned write set",
                *owned_lines,
                "",
                "## Forbidden write set",
                *forbidden_lines,
                "",
                "## Frozen summary",
                *frozen_lines,
                "",
                "## Required output",
                f"Write JSON to `{findings_path}` with shape:",
                "```json",
                '{',
                '  "verdict": "PASS | REQUIRED_FIX | CONTRACT_CONFLICT",',
                '  "findings": [',
                '    {',
                '      "finding_id": "F-001",',
                '      "severity": "HIGH",',
                '      "invariant": "...",',
                '      "file": "...",',
                '      "evidence": "...",',
                '      "required_fix": "...",',
                '      "acceptance": "..."',
                '    }',
                '  ]',
                '}',
                "```",
                "",
                "Only emit Required Findings that are bounded and actionable.",
                "",
            ]
        )
        atomic_write_text(path, body)
        # Seed an empty findings file so harvest can distinguish missing vs unread.
        if not findings_path.exists():
            atomic_write_json(
                findings_path,
                {
                    "verdict": "PENDING",
                    "findings": [],
                    "task_id": task_id,
                    "reviewpack": pack_id,
                },
            )
        return path

    def diagnose(
        self,
        task_id: str,
        *,
        profile: str | None = None,
    ) -> dict[str, Any]:
        """Start a read-only Sol Diagnoser Run after repeated fix failures."""

        with self.store.scheduler_lock():
            with self.store.lock(task_id):
                state = self._load_locked(task_id)
                self._refresh_runtime(state)
                if self._session_active(state):
                    raise AgentCtlError(f"task is already running in {state['tmux_session']}")
                policy = dict(state.get("execution_policy") or DEFAULT_EXECUTION_POLICY)
                max_fix = int(policy.get("max_fix_cycles_before_diagnosis") or 2)
                if int(state.get("repair_cycle_count") or 0) < max_fix:
                    raise StateError(
                        f"diagnose requires repair_cycle_count>={max_fix}; "
                        f"current={state.get('repair_cycle_count') or 0}"
                    )
                if state.get("last_verify_result") == "PASS" and state.get("last_findings_verdict") != "REQUIRED_FIX":
                    raise StateError("diagnose refused: verify PASS and no required findings")
                profile_id = profile or str(policy.get("diagnoser_profile") or "codex-sol-xhigh-diagnose")
                diagnosis_id = self._next_artifact_id(self._diagnosis_dir(task_id), "diagnosis")
                diagnosis_path = self._diagnosis_dir(task_id) / f"{diagnosis_id}.json"
                state["pending_diagnosis_path"] = str(diagnosis_path)
                prompt = self._write_diagnosis_pack(state, diagnosis_path=diagnosis_path)
                self._revalidate_or_raise(state, prompt_override=str(prompt))
                if self._active_task_count(exclude=task_id) >= self.max_concurrency:
                    raise AgentCtlError("max_concurrency reached; free a slot before diagnose")
                state["read_only"] = True
                self._launch_profile_run(
                    state,
                    profile_id=profile_id,
                    phase="DIAGNOSE",
                    prompt_file=prompt,
                    clear_session=True,
                )
                self._write_controller_brief(state)
                state["updated_at"] = self.now()
                self.store.write(state)
        return self.status(task_id)

    def _write_diagnosis_pack(self, state: Mapping[str, Any], *, diagnosis_path: Path) -> Path:
        task_id = str(state["task_id"])
        pack_dir = self.store.task_dir(task_id) / "diagnosis-packs"
        pack_dir.mkdir(parents=True, exist_ok=True)
        pack_id = self._next_artifact_id(pack_dir, "diagpack")
        path = pack_dir / f"{pack_id}.md"
        verify = None
        with contextlib.suppress(StateError):
            verify = self._load_verify_artifact(task_id, state.get("last_verify_id"))
        body = "\n".join(
            [
                f"# Diagnosis Pack {pack_id}",
                "",
                "You are a read-only Sol diagnoser. Do not modify files.",
                "Explain why prior fixer cycles failed and produce a minimal fix plan.",
                "",
                f"Task: `{task_id}`",
                f"repair_cycle_count: {state.get('repair_cycle_count')}",
                f"same_failure_signature_count: {state.get('same_failure_signature_count')}",
                f"failure_signatures: {state.get('last_failure_signatures')}",
                f"last_verify: {verify.get('verify_id') if verify else None}",
                f"last_findings: {state.get('last_findings_path')}",
                "",
                f"Write diagnosis JSON to `{diagnosis_path}` with keys:",
                "root_cause, why_previous_fixes_failed, minimal_fix_plan,",
                "files_allowed, files_forbidden, new_tests_required, contract_conflict, confidence.",
                "",
            ]
        )
        atomic_write_text(path, body)
        if not diagnosis_path.exists():
            atomic_write_json(
                diagnosis_path,
                {
                    "root_cause": "PENDING",
                    "why_previous_fixes_failed": [],
                    "minimal_fix_plan": [],
                    "files_allowed": list(state.get("owned_write_set") or []),
                    "files_forbidden": list(state.get("forbidden_write_set") or []),
                    "new_tests_required": [],
                    "contract_conflict": False,
                    "confidence": "LOW",
                },
            )
        return path

    def fallback(
        self,
        task_id: str,
        *,
        profile: str | None = None,
        reason: str,
        from_verify: str | None = None,
    ) -> dict[str, Any]:
        """Manual CodeBuddy/Kimi backup fixer launch (never automatic)."""

        if not reason or not str(reason).strip():
            raise ManifestError("fallback requires an explicit --reason")
        with self.store.scheduler_lock():
            with self.store.lock(task_id):
                state = self._load_locked(task_id)
                self._refresh_runtime(state)
                if self._session_active(state):
                    raise AgentCtlError(f"task is already running in {state['tmux_session']}")
                if (state.get("execution_policy") or {}).get("automatic_provider_switch"):
                    raise StateError("automatic_provider_switch is forbidden; use explicit fallback")
                verify = self._load_verify_artifact(task_id, from_verify or state.get("last_verify_id"))
                if verify.get("result") == "PASS":
                    raise StateError("fallback refused for PASS verify")
                policy = dict(state.get("execution_policy") or DEFAULT_EXECUTION_POLICY)
                profile_id = profile or str(policy.get("backup_profile") or "codebuddy-kimi-k3-backup")
                bug_pack = self._build_bug_pack(state, verify=verify, profile_id=profile_id)
                handoff = self.store.task_dir(task_id) / "current-handoff.md"
                atomic_write_text(
                    handoff,
                    "\n".join(
                        [
                            f"# Provider Handoff · {task_id}",
                            "",
                            f"reason: {reason}",
                            f"from_profile: {state.get('active_profile')}",
                            f"to_profile: {profile_id}",
                            f"diff_fingerprint: {_git_diff_fingerprint(Path(str(state['worktree'])))}",
                            f"verify: {verify.get('verify_id')}",
                            "",
                            "Do not rediscuss frozen decisions. Execute the Bug Pack only.",
                            "",
                        ]
                    ),
                )
                self._revalidate_or_raise(state, prompt_override=str(bug_pack))
                if self._active_task_count(exclude=task_id) >= self.max_concurrency:
                    raise AgentCtlError("max_concurrency reached; free a slot before fallback")
                state["fallback_reason"] = str(reason)
                state["read_only"] = False
                self._launch_profile_run(
                    state,
                    profile_id=profile_id,
                    phase="FIX",
                    prompt_file=bug_pack,
                    clear_session=True,
                )
                self._write_controller_brief(state)
                state["updated_at"] = self.now()
                self.store.write(state)
        return self.status(task_id)

    def _harvest_pending_artifacts(self, state: dict[str, Any]) -> None:
        """Collect findings/diagnosis after a reviewer/diagnoser Run exits."""

        worktree = Path(str(state["worktree"]))
        current_fp = _git_diff_fingerprint(worktree)
        pending_findings = state.get("pending_findings_path")
        if pending_findings:
            path = Path(str(pending_findings))
            target_fp = state.get("review_target_fingerprint")
            if target_fp and target_fp != current_fp:
                state["review_invalidated"] = True
                state["last_findings_verdict"] = "INVALIDATED"
                state["guard"] = "POLICY_VIOLATION"
                state.setdefault("guards", [])
                if "POLICY_VIOLATION" not in state["guards"]:
                    state["guards"].append("POLICY_VIOLATION")
            elif path.is_file():
                with contextlib.suppress(OSError, json.JSONDecodeError):
                    payload = json.loads(path.read_text(encoding="utf-8"))
                    verdict = str(payload.get("verdict") or "PENDING")
                    if verdict != "PENDING":
                        state["last_findings_path"] = str(path)
                        state["last_findings_verdict"] = verdict
                        state["review_completed"] = verdict in {"PASS", "REQUIRED_FIX", "CONTRACT_CONFLICT"}
                        state["pending_findings_path"] = None
                        if verdict == "CONTRACT_CONFLICT":
                            state["guard"] = "CONTRACT_CONFLICT"
                            state.setdefault("guards", [])
                            if "CONTRACT_CONFLICT" not in state["guards"]:
                                state["guards"].append("CONTRACT_CONFLICT")
        pending_diagnosis = state.get("pending_diagnosis_path")
        if pending_diagnosis:
            path = Path(str(pending_diagnosis))
            if path.is_file():
                with contextlib.suppress(OSError, json.JSONDecodeError):
                    payload = json.loads(path.read_text(encoding="utf-8"))
                    if payload.get("root_cause") not in {None, "", "PENDING"}:
                        state["last_diagnosis_path"] = str(path)
                        state["pending_diagnosis_path"] = None
                        if payload.get("contract_conflict") is True:
                            state["guard"] = "CONTRACT_CONFLICT"
                            state.setdefault("guards", [])
                            if "CONTRACT_CONFLICT" not in state["guards"]:
                                state["guards"].append("CONTRACT_CONFLICT")

    def dispatch_once(self) -> dict[str, Any]:
        """Start at most the available queued tasks; never run a daemon loop."""

        self.store.ensure_layout()
        started: list[str] = []
        queued: list[str] = []
        with self.store.scheduler_lock():
            candidates = [
                item
                for item in self.store.iter_states()
                if item.get("status") in {"REGISTERED", "READY", "QUEUED"}
            ]
            for item in sorted(candidates, key=lambda value: (value.get("registered_at", ""), value["task_id"])):
                if self._active_task_count() >= self.max_concurrency:
                    try:
                        result = self._start_once(str(item["task_id"]))
                        if result.get("status") == "QUEUED":
                            queued.append(str(item["task_id"]))
                    except AgentCtlError:
                        queued.append(str(item["task_id"]))
                    continue
                try:
                    result = self._start_once(str(item["task_id"]))
                except AgentCtlError:
                    # A stale/unsafe task remains visible for a human; a single
                    # dispatch must not spin or silently retry it.
                    continue
                if result.get("status") == "RUNNING":
                    started.append(str(item["task_id"]))
                elif result.get("status") == "QUEUED":
                    queued.append(str(item["task_id"]))
        return {
            "started": started,
            "queued": sorted(set(queued)),
            "max_concurrency": self.max_concurrency,
            "active": self._active_task_count(),
        }

    def _resume_once(
        self,
        task_id: str,
        *,
        model: str | None = None,
        reasoning_effort: str | None = None,
        prompt: str | None = None,
        phase: str | None = None,
        writer_only: bool = False,
    ) -> dict[str, Any]:
        with self.store.lock(task_id):
            state = self._load_locked(task_id)
            self._refresh_runtime(state)
            if writer_only and state.get("role", "writer") != "writer":
                raise StateError("fix requires a writer task; reviewer recovery is forbidden")
            if self._session_active(state):
                raise AgentCtlError(f"task is already running in {state['tmux_session']}")
            if state["status"] in {"DONE", "CONTRACT_CONFLICT"}:
                raise StateError(f"cannot resume task in state {state['status']}")
            self._revalidate_or_raise(state, prompt_override=prompt)
            session_id = _get_runner_session_id(state)
            if not session_id:
                session_id, thread_id = extract_session_ids_from_jsonl(Path(state["logs"]["jsonl"]))
                _set_runner_session_ids(state, session_id=session_id, thread_id=thread_id)
                session_id = _get_runner_session_id(state)
            if not session_id:
                raise AgentCtlError("no runner session/thread id found; start the task first")
            prompt_file, prompt_hash = self._prepare_prompt(state, prompt)
            selected_model = model or state["model"]
            selected_effort = reasoning_effort or state["reasoning_effort"]
            _require_string(selected_model, "model")
            _require_string(selected_effort, "reasoning_effort")
            if phase is not None:
                requested_phase = canonical_phase(phase)
                validate_phase_transition(state["phase"], requested_phase)
                state["phase"] = requested_phase
            if self._active_task_count(exclude=task_id) >= self.max_concurrency:
                if state["status"] != "QUEUED":
                    _set_status(
                        state,
                        "QUEUED",
                        note=f"max_concurrency={self.max_concurrency}; resume waits for a slot",
                    )
                state["queue_reason"] = f"max_concurrency={self.max_concurrency}"
                state["queue_mode"] = "resume"
                state["queued_model"] = selected_model
                state["queued_reasoning_effort"] = selected_effort
                state["queued_prompt"] = str(prompt_file)
                state["queued_phase"] = canonical_phase(phase) if phase is not None else None
                state["prompt_file"] = str(prompt_file)
                state["prompt_sha256"] = prompt_hash
                state.setdefault("context_pack", {})["prompt_reference"] = {
                    "path": str(prompt_file),
                    "sha256": prompt_hash,
                }
                state["updated_at"] = self.now()
                self.store.write(state)
                return self._status_view(state)
            self._launch(
                state,
                mode="resume",
                prompt_file=prompt_file,
                model=selected_model,
                reasoning_effort=selected_effort,
                session_id=session_id,
            )
            state["prompt_sha256"] = prompt_hash
            state.setdefault("context_pack", {})["prompt_reference"] = {
                "path": str(prompt_file),
                "sha256": prompt_hash,
            }
            state["updated_at"] = self.now()
            self.store.write(state)
        return self._status_view(state)

    def resume(
        self,
        task_id: str,
        *,
        model: str | None = None,
        reasoning_effort: str | None = None,
        prompt: str | None = None,
        phase: str | None = None,
        _writer_only: bool = False,
    ) -> dict[str, Any]:
        with self.store.scheduler_lock():
            self._resume_once(
                task_id,
                model=model,
                reasoning_effort=reasoning_effort,
                prompt=prompt,
                phase=phase,
                writer_only=_writer_only,
            )
        return self.status(task_id)

    def status(self, task_id: str) -> dict[str, Any]:
        with self.store.lock(task_id):
            state = self._load_locked(task_id)
            before = json.dumps(state, sort_keys=True, ensure_ascii=False)
            self._refresh_runtime(state)
            self._write_if_changed(before, state)
            return self._status_view(state)

    def _status_view(self, state: Mapping[str, Any]) -> dict[str, Any]:
        runtime = dict(state.get("runtime", {}))
        return {
            "task_id": state["task_id"],
            "status": state["status"],
            "phase": state["phase"],
            "role": state.get("role", "writer"),
            "review_of": state.get("review_of"),
            "guard": state.get("guard"),
            "guards": list(state.get("guards", [])),
            "semantic_locks": list(state.get("semantic_locks", [])),
            "failure_count": state.get("failure_count", 0),
            "repair_cycle_count": state.get("repair_cycle_count", 0),
            "same_failure_signature_count": state.get("same_failure_signature_count", 0),
            "review_round_count": state.get("review_round_count", 0),
            "post_diagnosis_fix_count": state.get("post_diagnosis_fix_count", 0),
            "repo": state["repo"],
            "worktree": state["worktree"],
            "branch": state["branch"],
            "base_commit": state["base_commit"],
            "tmux_session": state["tmux_session"],
            "execution_policy": dict(state.get("execution_policy") or {}),
            "active_profile": state.get("active_profile"),
            "runner_kind": state.get("runner_kind"),
            "model": state["model"],
            "reasoning_effort": state["reasoning_effort"],
            "runner_session_id": _get_runner_session_id(state),
            "runner_thread_id": state.get("runner_thread_id") or state.get("codex_thread_id"),
            "codex_session_id": state.get("codex_session_id") or _get_runner_session_id(state),
            "codex_thread_id": state.get("codex_thread_id") or state.get("runner_thread_id"),
            "current_run_id": state.get("current_run_id"),
            "run_count": state.get("run_count", 0),
            "last_findings_verdict": state.get("last_findings_verdict"),
            "last_findings_path": state.get("last_findings_path"),
            "logs": dict(state.get("logs", {})),
            "runtime": runtime,
            "summary": {
                "last_agent_message": runtime.get("last_agent_message"),
                "current_command": runtime.get("current_command"),
                "last_log_growth_at": runtime.get("last_log_growth_at"),
                "capacity_error": runtime.get("capacity_signal", False),
                "exit_code": runtime.get("exit_code"),
                "tests": runtime.get("jsonl_summary", {}).get("tests", []),
            },
            "git": self._git_snapshot(Path(state["worktree"])),
            "last_checkpoint": state.get("last_checkpoint"),
            "updated_at": state.get("updated_at"),
        }

    @staticmethod
    def _git_snapshot(worktree: Path) -> dict[str, Any]:
        if not worktree.is_dir():
            return {"error": f"worktree missing: {worktree}"}
        result: dict[str, Any] = {}
        try:
            result["head"] = _git_output(worktree, ["rev-parse", "HEAD"])
            result["branch"] = _git_output(
                worktree, ["symbolic-ref", "--quiet", "--short", "HEAD"], check=False
            ) or "<detached>"
            changed = _git_output(worktree, ["status", "--porcelain=v1"], check=False)
            result["changed_files"] = [line for line in changed.splitlines() if line]
            upstream = _git_output(
                worktree,
                ["rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"],
                check=False,
            )
            if upstream:
                counts = _git_output(
                    worktree,
                    ["rev-list", "--left-right", "--count", f"{upstream}...HEAD"],
                    check=False,
                )
                result["remote"] = {"upstream": upstream, "ahead_behind": counts or None}
            else:
                result["remote"] = {"upstream": None, "ahead_behind": None}
        except AgentCtlError as exc:
            result["error"] = str(exc)
        return result

    def list_tasks(self) -> list[dict[str, Any]]:
        self.store.ensure_layout()
        result: list[dict[str, Any]] = []
        for state in self.store.iter_states():
            result.append(self.status(state["task_id"]))
        return sorted(result, key=lambda item: item["task_id"])

    def capture(self, task_id: str, lines: int = 80) -> dict[str, Any]:
        with self.store.lock(task_id):
            state = self._load_locked(task_id)
            output = self.tmux.capture(state["tmux_session"], lines)
            return {"task_id": task_id, "tmux_session": state["tmux_session"], "output": output}

    def checkpoint(
        self,
        task_id: str,
        *,
        phase: str,
        note: str | None = None,
        commit: str | None = None,
        tests: Sequence[str] = (),
    ) -> dict[str, Any]:
        if phase not in ALL_STATES:
            raise StateError(f"unknown phase/state: {phase}")
        requested_phase = canonical_phase(phase) if phase in PRIMARY_PHASES or phase in LEGACY_PHASE_ALIASES else phase
        with self.store.lock(task_id):
            state = self._load_locked(task_id)
            before = json.dumps(state, sort_keys=True, ensure_ascii=False)
            self._refresh_runtime(state)
            current_phase = state["phase"]
            active = bool(state["runtime"].get("process_running"))
            target_status = state["status"]
            if requested_phase in PRIMARY_PHASES:
                validate_phase_transition(current_phase, requested_phase)
                state["phase"] = requested_phase
                if requested_phase == "DONE":
                    target_status = "DONE"
                elif active:
                    target_status = "RUNNING"
            elif phase == "RUNNING" and not active:
                raise StateError("RUNNING checkpoint requires an active tmux/Codex process")
            if state["status"] != target_status:
                _set_status(state, target_status, note=note, tests=tests, commit=commit)
            state["last_checkpoint"] = {
                "at": self.now(),
                "run_count": state.get("run_count", 0),
                "target": requested_phase,
                "note": note,
                "commit": commit,
                "tests": list(tests),
            }
            state["evidence_revision"] = int(state.get("evidence_revision") or 0) + 1
            self._write_if_changed(before, state)
        return self.status(task_id)

    def advance(
        self,
        task_id: str,
        *,
        phase: str,
        note: str | None = None,
        commit: str | None = None,
        tests: Sequence[str] = (),
    ) -> dict[str, Any]:
        return self.checkpoint(task_id, phase=phase, note=note, commit=commit, tests=tests)

    def watch_once(self, task_id: str) -> dict[str, Any]:
        dispatch_needed = False
        with self.store.lock(task_id):
            state = self._load_locked(task_id)
            before = json.dumps(state, sort_keys=True, ensure_ascii=False)
            self._refresh_runtime(state)
            runtime = state["runtime"]
            capacity = logs_classify_capacity(
                _log_paths(state), state["runtime"].get("run_log_offsets")
            )
            exit_code = runtime.get("exit_code")
            if exit_code is not None:
                self._harvest_pending_artifacts(state)
                self._write_controller_brief(state)
            if state["status"] not in TERMINAL_STATES:
                if capacity:
                    if state["status"] != "WAITING_CAPACITY":
                        _set_status(
                            state,
                            "WAITING_CAPACITY",
                            note="capacity/resource-limit signal detected; no retry or cleanup performed",
                        )
                    runtime["warning"] = "WAITING_CAPACITY: detected 14003/full capacity/resource limit"
                    dispatch_needed = True
                elif exit_code is not None:
                    checkpoint = state.get("last_checkpoint") or {}
                    explicit_for_run = checkpoint.get("run_count") == state.get("run_count")
                    if exit_code == 0 and state.get("role") == "reviewer":
                        target_issue: str | None = None
                        review_of = state.get("review_of")
                        target_fingerprint: str | None = None
                        if not review_of:
                            target_issue = "reviewer has no review target"
                        else:
                            try:
                                target = self.store.load(str(review_of))
                                target_fingerprint = _review_fingerprint(target)
                            except AgentCtlError as exc:
                                target_issue = str(exc)
                        if target_issue or target_fingerprint != state.get("review_fingerprint"):
                            state["review_invalidated"] = True
                            state["review_completed"] = False
                            self._set_guard(
                                state,
                                "CONTEXT_STALE",
                                target_issue or "review target changed during review",
                            )
                        else:
                            baseline = state.get("review_baseline_fingerprint")
                            current = _changed_files_fingerprint(Path(state["worktree"]))
                            if baseline and current != baseline:
                                self._set_guard(
                                    state,
                                    "POLICY_VIOLATION",
                                    "reviewer changed the target worktree; reviewer is read-only",
                                )
                            else:
                                state["review_completed"] = True
                                if state["status"] != "READY_FOR_REVIEW":
                                    _set_status(
                                        state,
                                        "READY_FOR_REVIEW",
                                        note="read-only reviewer exited 0; evidence recorded",
                                    )
                                dispatch_needed = True
                    elif exit_code == 0:
                        violations = self._write_set_violations(state)
                        if violations:
                            self._set_guard(
                                state,
                                "POLICY_VIOLATION",
                                "; ".join(violations),
                            )
                        elif explicit_for_run and checkpoint.get("target") not in (None, "RUNNING"):
                            target = canonical_phase(str(checkpoint["target"]))
                            if state["status"] == "RUNNING":
                                _set_status(
                                    state,
                                    target,
                                    note="retained explicit checkpoint after clean process exit",
                                )
                            dispatch_needed = True
                        elif state["status"] != "READY_FOR_REVIEW":
                            _set_status(
                                state,
                                "READY_FOR_REVIEW",
                                note="Codex exited 0; review is still required",
                            )
                            dispatch_needed = True
                    elif exit_code != 0:
                        state["failure_count"] = int(state.get("failure_count") or 0) + 1
                        suggestion = ""
                        if state["failure_count"] >= 2:
                            suggestion = "; repeated technical failure: consider Sol xhigh (manual only)"
                        runtime["warning"] = f"Codex exited non-zero ({exit_code}){suggestion}"
                        if state["status"] != "FAILED":
                            _set_status(
                                state,
                                "FAILED",
                                note=f"Codex exited non-zero ({exit_code}){suggestion}",
                            )
                elif runtime.get("tmux_alive"):
                    if isinstance(runtime.get("warning"), str) and runtime["warning"].startswith("STALLED:"):
                        if state["status"] != "SUSPECTED_STALL":
                            _set_status(
                                state,
                                "SUSPECTED_STALL",
                                note="no log growth; process was not killed",
                            )
                    elif state["status"] != "RUNNING":
                        _set_status(
                            state,
                            "RUNNING",
                            note="tmux alive and no current capacity signal; current phase retained",
                        )
                    if runtime.get("log_grew") and state["status"] == "SUSPECTED_STALL":
                        _set_status(state, "RUNNING", note="log growth resumed")
                    if state["status"] == "RUNNING" and (
                        runtime.get("log_grew")
                        or (runtime.get("warning") or "").startswith("WAITING_CAPACITY")
                    ):
                        runtime["warning"] = None
                elif state["status"] not in {"REGISTERED", "READY", "QUEUED"}:
                    runtime["warning"] = "tmux session is not present; no automatic retry was attempted"
            if exit_code is not None:
                dispatch_needed = True
            self._write_if_changed(before, state)
            result = self._status_view(state)
        if dispatch_needed:
            self.dispatch_once()
        return result

    def stop(self, task_id: str) -> dict[str, Any]:
        with self.store.lock(task_id):
            state = self._load_locked(task_id)
            before = json.dumps(state, sort_keys=True, ensure_ascii=False)
            try:
                self.tmux.kill_session(state["tmux_session"])
            except (AgentCtlError, OSError) as exc:
                if state["status"] not in {"DONE", "STOPPED"}:
                    raise
                state["runtime"]["warning"] = str(exc)
            if state["status"] not in {"DONE", "STOPPED"}:
                _set_status(state, "STOPPED", note="tmux session stopped; Git data retained")
            self._write_if_changed(before, state)
        return self.status(task_id)


def doctor_report(*, codex_binary: str | None = None, tmux_binary: str | None = None) -> dict[str, Any]:
    checks: dict[str, Any] = {}

    checks["python"] = {"ok": True, "path": sys.executable, "version": sys.version.split()[0]}

    try:
        profiles = load_profiles()
        unimplemented = sorted(
            {
                f"{name}:{item['runner']}"
                for name, item in profiles.items()
                if item["runner"] not in IMPLEMENTED_RUNNERS
            }
        )
        checks["profiles"] = {
            "ok": True,
            "path": str(default_profiles_path()),
            "count": len(profiles),
            "implemented_runners": sorted(IMPLEMENTED_RUNNERS),
            "declared_unimplemented": unimplemented,
        }
    except ManifestError as exc:
        checks["profiles"] = {"ok": False, "path": str(default_profiles_path()), "error": str(exc)}

    selected_cursor = resolve_cursor_binary()
    cursor_path = Path(selected_cursor).expanduser()
    if cursor_path.is_file() and os.access(cursor_path, os.X_OK):
        try:
            result = _run_process([selected_cursor, "--version"])
            help_result = _run_process([selected_cursor, "--help"])
            help_text = f"{help_result.stdout}\n{help_result.stderr}"
            checks["cursor_agent"] = {
                "ok": result.returncode == 0 and help_result.returncode == 0,
                "path": selected_cursor,
                "version": (result.stdout or result.stderr).strip().splitlines()[0:1][0]
                if (result.stdout or result.stderr).strip()
                else "",
                "print_mode": "--print" in help_text,
                "stream_json": "stream-json" in help_text,
                "resume": "--resume" in help_text,
            }
        except AgentCtlError as exc:
            checks["cursor_agent"] = {"ok": False, "path": selected_cursor, "error": str(exc)}
    else:
        checks["cursor_agent"] = {
            "ok": False,
            "path": selected_cursor,
            "error": "Cursor agent executable not found or not executable",
        }

    selected_codebuddy = resolve_codebuddy_binary()
    codebuddy_path = Path(selected_codebuddy).expanduser()
    if codebuddy_path.is_file() and os.access(codebuddy_path, os.X_OK):
        try:
            result = _run_process([selected_codebuddy, "--version"])
            help_result = _run_process([selected_codebuddy, "--help"])
            help_text = f"{help_result.stdout}\n{help_result.stderr}"
            checks["codebuddy"] = {
                "ok": result.returncode == 0 and help_result.returncode == 0,
                "path": selected_codebuddy,
                "version": (result.stdout or result.stderr).strip().splitlines()[:1][0]
                if (result.stdout or result.stderr).strip()
                else "",
                "print_mode": "--print" in help_text or "-p" in help_text,
                "stream_json": "stream-json" in help_text,
                "resume": "--resume" in help_text or "-r" in help_text,
            }
        except AgentCtlError as exc:
            checks["codebuddy"] = {"ok": False, "path": selected_codebuddy, "error": str(exc)}
    else:
        checks["codebuddy"] = {
            "ok": False,
            "path": selected_codebuddy,
            "error": "CodeBuddy executable not found or not executable",
        }

    git_path = shutil.which("git")
    if git_path:
        result = _run_process([git_path, "--version"])
        checks["git"] = {
            "ok": result.returncode == 0,
            "path": git_path,
            "version": (result.stdout or result.stderr).strip(),
        }
    else:
        checks["git"] = {"ok": False, "error": "git not found on PATH"}

    selected_tmux = tmux_binary or shutil.which("tmux")
    if selected_tmux:
        try:
            result = _run_process([selected_tmux, "-V"])
            checks["tmux"] = {
                "ok": result.returncode == 0,
                "path": selected_tmux,
                "version": (result.stdout or result.stderr).strip(),
            }
        except AgentCtlError as exc:
            checks["tmux"] = {"ok": False, "path": selected_tmux, "error": str(exc)}
    else:
        checks["tmux"] = {"ok": False, "error": "tmux not found on PATH"}

    selected_codex = resolve_codex_binary(codex_binary)
    codex_path = Path(selected_codex).expanduser()
    if codex_path.is_file() and os.access(codex_path, os.X_OK):
        try:
            result = _run_process([selected_codex, "--version"])
            help_result = _run_process([selected_codex, "exec", "--help"])
            help_text = f"{help_result.stdout}\n{help_result.stderr}"
            checks["codex"] = {
                "ok": result.returncode == 0 and help_result.returncode == 0,
                "path": selected_codex,
                "version": (result.stdout or result.stderr).strip(),
                "exec_json": "--json" in help_text,
                "reasoning_effort_flag": "--reasoning-effort" in help_text,
            }
        except AgentCtlError as exc:
            checks["codex"] = {"ok": False, "path": selected_codex, "error": str(exc)}
    else:
        checks["codex"] = {"ok": False, "path": selected_codex, "error": "Codex executable not found or not executable"}
    return {"ok": all(item.get("ok", False) for item in checks.values()), "checks": checks}


def _format_status(item: Mapping[str, Any]) -> str:
    runtime = item.get("runtime", {})
    git = item.get("git", {})
    lines = [
        f"{item['task_id']}: status={item['status']} phase={item['phase']}",
        f"  role={item.get('role', 'writer')} guard={item.get('guard') or '<none>'} failure_count={item.get('failure_count', 0)} repair_cycle_count={item.get('repair_cycle_count', 0)}",
        f"  worktree={item['worktree']}",
        f"  branch={item['branch']} HEAD={git.get('head', '<unknown>')}",
        f"  tmux={item['tmux_session']} alive={runtime.get('tmux_alive')} running={runtime.get('process_running')}",
        f"  profile={item.get('active_profile') or '<none>'} runner={item.get('runner_kind') or 'codex-cli'}",
        f"  model={item['model']} reasoning_effort={item['reasoning_effort']}",
        f"  runner_session_id={item.get('runner_session_id') or item.get('codex_session_id') or '<pending>'}",
        f"  log_size={runtime.get('log_size', 0)} active_seconds={runtime.get('active_seconds')}",
        f"  changed_files={len(git.get('changed_files', [])) if isinstance(git.get('changed_files'), list) else '<unknown>'}",
    ]
    summary = item.get("summary", {})
    if summary.get("current_command"):
        lines.append(f"  current_command={summary['current_command']}")
    if summary.get("last_agent_message"):
        message = str(summary["last_agent_message"]).replace("\n", " ")
        lines.append(f"  last_agent_message={message[-240:]}")
    if summary.get("tests"):
        lines.append(f"  tests={len(summary['tests'])} structured events")
    if runtime.get("warning"):
        lines.append(f"  warning={runtime['warning']}")
    remote = git.get("remote")
    if remote:
        lines.append(f"  remote={remote.get('upstream') or '<none>'} ahead_behind={remote.get('ahead_behind') or '<n/a>'}")
    return "\n".join(lines)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="agentctl",
        description="AC1 Linux multi-agent controller for tmux + multi-runner profiles (CP1: Codex)",
    )
    parser.add_argument("--state-dir", help="persistent state/log directory (default: AGENTCTL_STATE_DIR or ~/.local/state/ac1-agentctl)")
    parser.add_argument("--codex", dest="codex_binary", help="Codex executable path")
    parser.add_argument(
        "--cursor-agent",
        dest="cursor_binary",
        help="Cursor agent executable path (default: AGENTCTL_CURSOR_AGENT or ~/.local/bin/agent)",
    )
    parser.add_argument(
        "--codebuddy",
        dest="codebuddy_binary",
        help="CodeBuddy executable path (default: AGENTCTL_CODEBUDDY or codebuddy on PATH)",
    )
    parser.add_argument("--tmux", dest="tmux_binary", help="tmux executable path")
    parser.add_argument("--json", action="store_true", help="emit command result as JSON")
    parser.add_argument("--stall-seconds", type=int, default=DEFAULT_STALL_SECONDS)
    parser.add_argument("--max-concurrency", type=int, default=MAX_CONCURRENCY)
    commands = parser.add_subparsers(dest="command", required=True)

    commands.add_parser(
        "doctor",
        help="check Python, Git, tmux, Codex, Cursor agent, CodeBuddy, and profiles",
    )
    register = commands.add_parser("register", help="validate and atomically register a task manifest")
    register.add_argument("manifest")
    start = commands.add_parser("start", help="start a new Codex run in the fixed task tmux session")
    start.add_argument("task_id")
    resume = commands.add_parser("resume", help="resume the saved runner session in the same worktree")
    resume.add_argument("task_id")
    resume.add_argument("--model")
    resume.add_argument("--reasoning-effort")
    resume.add_argument("--prompt", help="prompt file override")
    status = commands.add_parser("status", help="show task runtime, logs, and Git status")
    status.add_argument("task_id", nargs="?")
    commands.add_parser("list", help="list registered tasks")
    capture = commands.add_parser("capture", help="capture the task tmux pane")
    capture.add_argument("task_id")
    capture.add_argument("--lines", type=int, default=80)
    for name in ("checkpoint", "advance"):
        command = commands.add_parser(name, help="record a validated pipeline phase checkpoint")
        command.add_argument("task_id")
        command.add_argument("--phase", required=True, choices=sorted(ALL_STATES))
        command.add_argument("--note")
        command.add_argument("--commit")
        command.add_argument("--test", dest="tests", action="append", default=[])
    watch = commands.add_parser("watch-once", help="classify one task once without retrying or killing it")
    watch.add_argument("task_id", nargs="?")
    commands.add_parser("dispatch-once", help="start queued tasks up to max-concurrency once")
    verify = commands.add_parser("verify", help="run acceptance_commands and write verify artifacts")
    verify.add_argument("task_id")
    fix = commands.add_parser(
        "fix",
        help="focused fix: legacy writer resume, or --profile fixer with Bug Pack",
    )
    fix.add_argument("task_id")
    fix.add_argument("--model")
    fix.add_argument("--reasoning-effort")
    fix.add_argument("--prompt", help="explicit prompt/Bug Pack file override")
    fix.add_argument("--profile", help="fixer profile id (e.g. cursor-grok45-fixer)")
    fix.add_argument(
        "--from",
        dest="from_verify",
        help="verify artifact id to build the Bug Pack from (default: latest)",
    )
    review = commands.add_parser(
        "review",
        help="start a fresh read-only reviewer session and request findings JSON",
    )
    review.add_argument("task_id")
    review.add_argument("--profile", help="reviewer profile id (default: execution_policy)")
    diagnose = commands.add_parser(
        "diagnose",
        help="start a read-only Sol diagnoser after repeated fix failures",
    )
    diagnose.add_argument("task_id")
    diagnose.add_argument("--profile", help="diagnoser profile id (default: execution_policy)")
    fallback = commands.add_parser(
        "fallback",
        help="manual CodeBuddy/Kimi backup fixer (never automatic)",
    )
    fallback.add_argument("task_id")
    fallback.add_argument("--profile", help="backup profile id")
    fallback.add_argument("--reason", required=True, help="why fallback is approved")
    fallback.add_argument("--from", dest="from_verify", help="verify artifact id")
    next_cmd = commands.add_parser("next", help="suggest next actions from controller brief")
    next_cmd.add_argument("task_id")
    runs_cmd = commands.add_parser("runs", help="list Run artifacts for a task")
    runs_cmd.add_argument("task_id")
    stop = commands.add_parser("stop", help="stop only the task tmux session")
    stop.add_argument("task_id")
    return parser


def _controller_from_args(args: argparse.Namespace) -> Controller:
    return Controller(
        state_dir=args.state_dir,
        codex=CodexCliRunner(args.codex_binary),
        cursor=CursorCliRunner(args.cursor_binary),
        codebuddy=CodeBuddyCliRunner(args.codebuddy_binary),
        tmux=TmuxAdapter(args.tmux_binary),
        stall_seconds=args.stall_seconds,
        max_concurrency=args.max_concurrency,
    )


def _emit(value: Any, *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))
    elif isinstance(value, Mapping) and "task_id" in value and "status" in value:
        print(_format_status(value))
    elif isinstance(value, list):
        for item in value:
            print(_format_status(item))
            print()
    elif isinstance(value, Mapping) and "output" in value:
        print(value["output"], end="" if str(value["output"]).endswith("\n") else "\n")
    elif isinstance(value, Mapping):
        print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(value)


def _runner_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="agentctl _run-codex", add_help=False)
    parser.add_argument("--codex-argv-json", required=True)
    parser.add_argument("--worktree", required=True)
    parser.add_argument("--prompt-file", required=True)
    parser.add_argument("--prompt-mode", choices=("stdin", "argv"), default="stdin")
    parser.add_argument("--jsonl", required=True)
    parser.add_argument("--plain-log", required=True)
    parser.add_argument("--last-message", required=True)
    parser.add_argument("--exit-code", required=True)
    parser.add_argument("--pid-file", required=True)
    parser.add_argument("--state-path")
    parser.add_argument("--lock-path")
    parser.add_argument("--append", action="store_true")
    return parser


def _persist_worker_session_ids(
    state_path: str | None,
    lock_path: str | None,
    session_id: str | None,
    thread_id: str | None,
) -> None:
    if not state_path or not lock_path or (session_id is None and thread_id is None):
        return
    target = Path(state_path)
    if not target.is_file():
        return
    try:
        with FileLock(Path(lock_path)):
            with target.open("r", encoding="utf-8") as handle:
                state = json.load(handle)
            changed = False
            if _set_runner_session_ids(state, session_id=session_id, thread_id=thread_id):
                changed = True
            if changed:
                state["updated_at"] = utc_now()
                atomic_write_json(target, state)
    except (OSError, json.JSONDecodeError, StateError):
        # The controller/status path will retry extraction. A worker must
        # never turn a successful Codex run into a failure because state
        # persistence raced with registration or shutdown.
        return


def run_codex_worker(argv: Sequence[str]) -> int:
    args = _runner_parser().parse_args(list(argv))
    try:
        codex_argv = json.loads(args.codex_argv_json)
    except json.JSONDecodeError as exc:
        print(f"agentctl runner: invalid codex argv JSON: {exc}", file=sys.stderr)
        return 2
    if not isinstance(codex_argv, list) or not all(isinstance(item, str) for item in codex_argv):
        print("agentctl runner: codex argv must be a JSON string list", file=sys.stderr)
        return 2
    jsonl_path = Path(args.jsonl)
    plain_path = Path(args.plain_log)
    last_message = Path(args.last_message)
    exit_code_path = Path(args.exit_code)
    pid_path = Path(args.pid_file)
    jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    plain_path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(last_message, "")
    atomic_write_text(pid_path, f"{os.getpid()}\n")
    mode = "a" if args.append else "w"
    prompt_mode = str(args.prompt_mode)
    prompt_handle = None
    try:
        if prompt_mode == "argv":
            prompt_text = Path(args.prompt_file).read_text(encoding="utf-8")
            codex_argv = list(codex_argv) + [prompt_text]
        else:
            prompt_handle = open(args.prompt_file, "rb")
    except OSError as exc:
        atomic_write_text(plain_path, f"[agentctl] prompt open failed: {exc}\n")
        atomic_write_text(exit_code_path, "2\n")
        return 2
    jsonl_handle = jsonl_path.open(mode, encoding="utf-8", errors="replace")
    plain_handle = plain_path.open(mode, encoding="utf-8", errors="replace")
    if args.append:
        plain_handle.write(f"\n[agentctl] resume started at {utc_now()}\n")
    else:
        plain_handle.write(f"[agentctl] start started at {utc_now()}\n")
    plain_handle.write(f"[agentctl] prompt_mode={prompt_mode}\n")
    plain_handle.flush()
    child: subprocess.Popen[bytes] | None = None
    child_lock = threading.Lock()

    def forward_signal(signum: int, _frame: Any) -> None:
        with child_lock:
            current = child
        if current is not None and current.poll() is None:
            with contextlib.suppress(OSError):
                current.terminate()

    previous_term = signal.signal(signal.SIGTERM, forward_signal)
    previous_int = signal.signal(signal.SIGINT, forward_signal)
    try:
        try:
            child = subprocess.Popen(
                codex_argv,
                cwd=args.worktree,
                stdin=prompt_handle if prompt_handle is not None else subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except OSError as exc:
            plain_handle.write(f"[agentctl] runner spawn failed: {exc}\n")
            plain_handle.flush()
            atomic_write_text(exit_code_path, "127\n")
            return 127

        def drain(stream: Any, destination: Any, *, json_stream: bool) -> None:
            while True:
                chunk = stream.readline()
                if not chunk:
                    break
                text = chunk.decode("utf-8", errors="replace")
                destination.write(text)
                destination.flush()
                if json_stream:
                    with contextlib.suppress(json.JSONDecodeError):
                        event_session, event_thread = extract_session_ids_from_value(json.loads(text))
                        _persist_worker_session_ids(
                            args.state_path,
                            args.lock_path,
                            event_session or event_thread,
                            event_thread,
                        )
                output_stream = sys.stdout if json_stream else sys.stderr
                output_stream.write(text)
                output_stream.flush()

        stdout_thread = threading.Thread(
            target=drain, args=(child.stdout, jsonl_handle), kwargs={"json_stream": True}, daemon=True
        )
        stderr_thread = threading.Thread(
            target=drain, args=(child.stderr, plain_handle), kwargs={"json_stream": False}, daemon=True
        )
        stdout_thread.start()
        stderr_thread.start()
        return_code = child.wait()
        stdout_thread.join(timeout=10)
        stderr_thread.join(timeout=10)
        plain_handle.write(f"[agentctl] runner exit_code={return_code} at {utc_now()}\n")
        plain_handle.flush()
        atomic_write_text(exit_code_path, f"{return_code}\n")
        return 0 if return_code == 0 else 1
    finally:
        signal.signal(signal.SIGTERM, previous_term)
        signal.signal(signal.SIGINT, previous_int)
        if prompt_handle is not None:
            prompt_handle.close()
        jsonl_handle.close()
        plain_handle.close()


def main(argv: Sequence[str] | None = None) -> int:
    raw_args = list(sys.argv[1:] if argv is None else argv)
    if raw_args and raw_args[0] == "_run-codex":
        return run_codex_worker(raw_args[1:])
    parser = _parser()
    args = parser.parse_args(raw_args)
    try:
        if args.command == "doctor":
            report = doctor_report(codex_binary=args.codex_binary, tmux_binary=args.tmux_binary)
            _emit(report, as_json=args.json)
            return 0 if report["ok"] else 1
        controller = _controller_from_args(args)
        if args.command == "register":
            value = controller.register(args.manifest)
        elif args.command == "start":
            value = controller.start(args.task_id)
        elif args.command == "resume":
            value = controller.resume(
                args.task_id,
                model=args.model,
                reasoning_effort=args.reasoning_effort,
                prompt=args.prompt,
            )
        elif args.command == "verify":
            value = controller.verify(args.task_id)
        elif args.command == "fix":
            value = controller.fix(
                args.task_id,
                model=args.model,
                reasoning_effort=args.reasoning_effort,
                prompt=args.prompt,
                profile=args.profile,
                from_verify=args.from_verify,
            )
        elif args.command == "review":
            value = controller.review(args.task_id, profile=args.profile)
        elif args.command == "diagnose":
            value = controller.diagnose(args.task_id, profile=args.profile)
        elif args.command == "fallback":
            value = controller.fallback(
                args.task_id,
                profile=args.profile,
                reason=args.reason,
                from_verify=args.from_verify,
            )
        elif args.command == "next":
            value = controller.next_action(args.task_id)
        elif args.command == "runs":
            value = controller.runs(args.task_id)
        elif args.command == "status":
            value = controller.list_tasks() if args.task_id is None else controller.status(args.task_id)
        elif args.command == "list":
            value = controller.list_tasks()
        elif args.command == "capture":
            value = controller.capture(args.task_id, args.lines)
        elif args.command in {"checkpoint", "advance"}:
            value = controller.checkpoint(
                args.task_id,
                phase=args.phase,
                note=args.note,
                commit=args.commit,
                tests=args.tests,
            )
        elif args.command == "watch-once":
            if args.task_id is None:
                value = [controller.watch_once(item["task_id"]) for item in controller.list_tasks()]
            else:
                value = controller.watch_once(args.task_id)
        elif args.command == "dispatch-once":
            value = controller.dispatch_once()
        elif args.command == "stop":
            value = controller.stop(args.task_id)
        else:
            parser.error(f"unsupported command: {args.command}")
            return 2
        _emit(value, as_json=args.json)
        return 0
    except (AgentCtlError, OSError, ValueError) as exc:
        print(f"agentctl: error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
