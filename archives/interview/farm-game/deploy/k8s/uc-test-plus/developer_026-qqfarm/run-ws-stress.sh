#!/usr/bin/env bash
# Run an in-cluster ws-stress Job against farm-gate ClusterIP.
# Usage: run-ws-stress.sh [scenario] [conns] [rate] [duration] [player-start]
set -euo pipefail
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}"
CTX="${KUBE_CONTEXT:-uc-test-plus}"
NS=developer_026-qqfarm
SCENARIO="${1:-join-accepted}"
CONNS="${2:-200}"
RATE="${3:-100}"
DURATION="${4:-20s}"
PLAYER_START="${5:-900000000}"
RECONNECT_LOOPS="${WS_STRESS_RECONNECT_LOOPS:-}"
RECONNECT_ARG=""
if [ -n "$RECONNECT_LOOPS" ]; then
  RECONNECT_ARG="            - -reconnect-loops=${RECONNECT_LOOPS}"
fi
COLD_POOL_SIZE="${WS_STRESS_COLD_POOL_SIZE:-}"
COLD_POOL_ARG=""
if [ -n "$COLD_POOL_SIZE" ]; then
  COLD_POOL_ARG="            - -cold-pool-size=${COLD_POOL_SIZE}"
fi
COLD_LAUNCH_DURATION="${WS_STRESS_COLD_LAUNCH_DURATION:-}"
COLD_LAUNCH_ARG=""
if [ -n "$COLD_LAUNCH_DURATION" ]; then
  COLD_LAUNCH_ARG="            - -cold-launch-duration=${COLD_LAUNCH_DURATION}"
fi
SKIP_FRIEND_OPPORTUNITY_ARG=""
case "${WS_STRESS_SKIP_FRIEND_OPPORTUNITY:-}" in
  1|true|TRUE|yes|YES) SKIP_FRIEND_OPPORTUNITY_ARG="            - -skip-friend-opportunity" ;;
esac
FRIEND_OPPORTUNITY_MAX_ATTEMPTS_ARG=""
if [ -n "${WS_STRESS_FRIEND_OPPORTUNITY_MAX_ATTEMPTS:-}" ]; then
  FRIEND_OPPORTUNITY_MAX_ATTEMPTS_ARG="            - -friend-opportunity-max-attempts=${WS_STRESS_FRIEND_OPPORTUNITY_MAX_ATTEMPTS}"
fi
PRIORITY_CLASS="${WS_STRESS_PRIORITY_CLASS:-}"
PRIORITY_ARG=""
if [ -n "$PRIORITY_CLASS" ]; then
  PRIORITY_ARG="      priorityClassName: ${PRIORITY_CLASS}"
fi
IMAGE="${WS_STRESS_IMAGE:-mirrors.tencent.com/uc-backend/farm-ws-stress@sha256:40d3ea59a936d2369eb6203e975dfea21ef6fae84af09ad56361c2af8dd4abb5}"
# Keep the disposable load client schedulable on the shared test cluster. The
# client can still be raised explicitly with WS_STRESS_CPU/WS_STRESS_MEMORY;
# defaulting to 2 CPU/4Gi avoids a false "backend outage" caused by a Pending
# load-generator Pod.
CPU="${WS_STRESS_CPU:-2}"
MEM="${WS_STRESS_MEMORY:-4Gi}"
WAIT="${WS_STRESS_WAIT:-420s}"
NAME="ws-stress-${SCENARIO}-$(date +%H%M%S)-${RANDOM}"
NAME=$(echo "$NAME" | tr '[:upper:]' '[:lower:]' | tr -cd 'a-z0-9-')

kubectl --context "$CTX" -n "$NS" delete job "$NAME" --ignore-not-found
kubectl --context "$CTX" -n "$NS" apply -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: ${NAME}
  namespace: ${NS}
  labels:
    app.kubernetes.io/name: ws-stress
    app.kubernetes.io/part-of: farm-game
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 600
  template:
    metadata:
      labels:
        app: ws-stress
        app.kubernetes.io/name: ws-stress
    spec:
${PRIORITY_ARG}
      restartPolicy: Never
      imagePullSecrets:
        - name: uc-registry
      containers:
        - name: ws-stress
          image: ${IMAGE}
          imagePullPolicy: IfNotPresent
          command: ["/usr/local/bin/ws-stress"]
          env:
            - name: LOAD_TEST_TOKEN_SECRET
              valueFrom:
                secretKeyRef:
                  name: farm-secrets
                  key: load-test-token-secret
          args:
            - -http-url=http://farm-gate:8080
            - -ws-url=ws://farm-gate:8080/ws
            - -scenario=${SCENARIO}
            - -conns=${CONNS}
            - -rate=${RATE}
            - -duration=${DURATION}
            - -player-start=${PLAYER_START}
            - -environment=developer_026-qqfarm
${RECONNECT_ARG}
${COLD_POOL_ARG}
${COLD_LAUNCH_ARG}
${SKIP_FRIEND_OPPORTUNITY_ARG}
${FRIEND_OPPORTUNITY_MAX_ATTEMPTS_ARG}
          resources:
            requests: {cpu: "${CPU}", memory: "${MEM}"}
            limits: {cpu: "${CPU}", memory: "${MEM}"}
EOF
echo "job=${NAME}"
# Fail closed if the disposable generator cannot be scheduled promptly. A
# late-starting generator would overlap a different backend lifecycle state
# while the caller incorrectly treats the result as one experiment.
pod=""
for _ in $(seq 1 120); do
  pod=$(kubectl --context "$CTX" -n "$NS" get pods -l "job-name=${NAME}" \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  [ -n "$pod" ] && break
  sleep 1
done
if [ -z "$pod" ]; then
  echo "load generator Pod was not created within 120s: ${NAME}" >&2
  kubectl --context "$CTX" -n "$NS" describe job "$NAME" >&2 || true
  kubectl --context "$CTX" -n "$NS" delete job "$NAME" --ignore-not-found >/dev/null || true
  exit 1
fi
started_at=""
for _ in $(seq 1 120); do
  started_at=$(kubectl --context "$CTX" -n "$NS" get pod "$pod" \
    -o jsonpath='{.status.containerStatuses[0].state.running.startedAt}' 2>/dev/null || true)
  [ -n "$started_at" ] && break
  sleep 1
done
if [ -z "$started_at" ]; then
  echo "load generator Pod was not Running within 120s: ${pod}" >&2
  kubectl --context "$CTX" -n "$NS" describe pod "$pod" >&2 || true
  kubectl --context "$CTX" -n "$NS" delete job "$NAME" --ignore-not-found >/dev/null || true
  exit 1
fi
echo "load_pod=${pod} started_at=${started_at}"
if kubectl --context "$CTX" -n "$NS" wait --for=condition=complete "job/${NAME}" --timeout="$WAIT"; then
  :
elif kubectl --context "$CTX" -n "$NS" wait --for=condition=failed "job/${NAME}" --timeout=30s; then
  echo "load generator Job failed: ${NAME}" >&2
  kubectl --context "$CTX" -n "$NS" logs "job/${NAME}" || true
  exit 1
else
  echo "load generator Job timed out: ${NAME}" >&2
  kubectl --context "$CTX" -n "$NS" describe job "$NAME" >&2 || true
  exit 1
fi
kubectl --context "$CTX" -n "$NS" logs "job/${NAME}"
