#!/usr/bin/env bash
set -euo pipefail

if (( $# < 4 )); then
  echo "usage: $0 LABEL WORKTREE EXPECTED_COMMIT COMMAND [ARG ...]" >&2
  exit 64
fi

label=$1
worktree=$2
expected_commit=$3
shift 3

case "$label" in
  *[!A-Za-z0-9._-]*|'')
    echo "unsafe label: $label" >&2
    exit 64
    ;;
esac

results_dir=$(cd "$(dirname "$0")" && pwd)
raw_log="$results_dir/raw/${label}.stdout-stderr.log"
meta_log="$results_dir/raw/${label}.meta.txt"
scratch="/tmp/r1-t-authcomp-panic-triage-${label}-$$"
gocache="$scratch/gocache"
gotmpdir="$scratch/gotmp"
mkdir -p "$gocache" "$gotmpdir"

cleanup() {
  case "$scratch" in
    /tmp/r1-t-authcomp-panic-triage-*) rm -rf -- "$scratch" ;;
    *) echo "refusing to remove unexpected scratch path: $scratch" >&2 ;;
  esac
}
trap cleanup EXIT

actual_commit=$(git -C "$worktree" rev-parse HEAD)
if [[ "$actual_commit" != "$expected_commit" ]]; then
  echo "HEAD mismatch: got $actual_commit want $expected_commit" >&2
  exit 65
fi

start_utc=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
start_local=$(TZ=Asia/Shanghai date '+%Y-%m-%dT%H:%M:%S%:z')
start_epoch=$(date '+%s')
command_display=$(printf '%q ' "$@")
command_display=${command_display% }

{
  printf 'label=%s\n' "$label"
  printf 'worktree=%s\n' "$worktree"
  printf 'commit=%s\n' "$actual_commit"
  printf 'command=%s\n' "$command_display"
  printf 'process_isolation=independent go test process\n'
  printf 'gocache=%s\n' "$gocache"
  printf 'gotmpdir=%s\n' "$gotmpdir"
  printf 'started_utc=%s\n' "$start_utc"
  printf 'started_asia_shanghai=%s\n' "$start_local"
  printf 'completed_utc=PENDING\n'
  printf 'completed_asia_shanghai=PENDING\n'
  printf 'duration_seconds=PENDING\n'
  printf 'exit_code=PENDING\n'
  printf 'shuffle_seeds=PENDING\n'
} > "$meta_log"

set +e
(
  cd "$worktree" || exit 125
  GOCACHE="$gocache" GOTMPDIR="$gotmpdir" "$@"
) 2>&1 | tee "$raw_log"
exit_code=${PIPESTATUS[0]}
set -e

end_epoch=$(date '+%s')
end_utc=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
end_local=$(TZ=Asia/Shanghai date '+%Y-%m-%dT%H:%M:%S%:z')
duration=$((end_epoch - start_epoch))
shuffle_seeds=$(sed -n 's/.*-test\.shuffle[ =]\{1,\}\([0-9-][0-9-]*\).*/\1/p' "$raw_log" | sort -u | paste -sd, -)
if [[ -z "$shuffle_seeds" ]]; then
  shuffle_seeds=none
fi

{
  printf 'label=%s\n' "$label"
  printf 'worktree=%s\n' "$worktree"
  printf 'commit=%s\n' "$actual_commit"
  printf 'command=%s\n' "$command_display"
  printf 'process_isolation=independent go test process\n'
  printf 'gocache=%s\n' "$gocache"
  printf 'gotmpdir=%s\n' "$gotmpdir"
  printf 'started_utc=%s\n' "$start_utc"
  printf 'started_asia_shanghai=%s\n' "$start_local"
  printf 'completed_utc=%s\n' "$end_utc"
  printf 'completed_asia_shanghai=%s\n' "$end_local"
  printf 'duration_seconds=%s\n' "$duration"
  printf 'exit_code=%s\n' "$exit_code"
  printf 'shuffle_seeds=%s\n' "$shuffle_seeds"
} > "$meta_log"

exit "$exit_code"
