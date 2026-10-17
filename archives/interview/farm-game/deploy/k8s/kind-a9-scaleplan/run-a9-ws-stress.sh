#!/usr/bin/env bash
# Run an in-cluster ws-stress Job against the a9-scaleplan kind farm-gate.
# Usage: run-a9-ws-stress.sh [scenario] [conns] [rate] [duration] [player-start]
set -euo pipefail
CTX=kind-a9-scaleplan
NS=a9-scaleplan
SCENARIO="${1:-join}"
CONNS="${2:-200}"
RATE="${3:-100}"
DURATION="${4:-20s}"
PLAYER_START="${5:-950000000}"
IMAGE="${WS_STRESS_IMAGE:-farm-l2-r2-registry:5000/farm/ws-stress@sha256:872229994692ec33e3d2265f179bfca394103b6ee4a4f6099253a2f12b282f25}"
CPU="${WS_STRESS_CPU:-8}"
MEM="${WS_STRESS_MEMORY:-12Gi}"
NAME="ws-stress-${SCENARIO}-$(date +%H%M%S)"
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
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 3600
  template:
    metadata:
      labels:
        app: ws-stress
    spec:
      restartPolicy: Never
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
            - -environment=a9-scaleplan
          resources:
            requests: {cpu: "${CPU}", memory: ${MEM}}
            limits: {cpu: "${CPU}", memory: ${MEM}}
EOF
echo "job=${NAME}"
kubectl --context "$CTX" -n "$NS" wait --for=condition=complete "job/${NAME}" --timeout=300s || \
  kubectl --context "$CTX" -n "$NS" wait --for=condition=failed "job/${NAME}" --timeout=30s || true
kubectl --context "$CTX" -n "$NS" logs "job/${NAME}" | tee "/tmp/a9-ws-${SCENARIO}-${CONNS}-r${RATE}.log"
