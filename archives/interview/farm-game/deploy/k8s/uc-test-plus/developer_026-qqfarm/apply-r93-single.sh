#!/usr/bin/env bash
# Port lab r93 (authority + remote-watch + Redis Cluster 3m3s + packed 4/0/4/0)
# onto UC developer_026-qqfarm. Does not touch developer_026-dev, infra-test, or farm-l2-r2.
set -euo pipefail

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO=$(CDPATH= cd -- "$ROOT/../../../.." && pwd)
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}"
CTX="${KUBE_CONTEXT:-uc-test-plus}"
NS=developer_026-qqfarm
# Do not inherit lab ROUTE_RUN_ID / REDIS_NAMESPACE_ENVIRONMENT.
RUN_ID="dyn-friend-uc-r93-20260814"
REDIS_ENV="uctestr9320260814"
REDIS_ZONE="z1"
REALM=classic-farm-dynamic-uc
REDIS_URL='redis://farm-redis:6379/0'
REDIS_CLUSTER_URLS='redis://farm-redis-0.farm-redis-headless.developer_026-qqfarm.svc.cluster.local:6379/0,redis://farm-redis-1.farm-redis-headless.developer_026-qqfarm.svc.cluster.local:6379/0,redis://farm-redis-2.farm-redis-headless.developer_026-qqfarm.svc.cluster.local:6379/0'
LOAD_TEST_TOKEN="${LOAD_TEST_TOKEN:-}"

kubectl() { command kubectl --context "$CTX" "$@"; }

if ! command kubectl config get-contexts "$CTX" >/dev/null 2>&1; then
  echo "apply-r93: context $CTX is missing" >&2
  exit 1
fi
cluster=$(command kubectl config view --minify --context "$CTX" -o jsonpath='{.clusters[0].name}')
if [[ "$CTX" != "uc-test-plus" || "$cluster" == "farm-l2-r2" || "$cluster" == "kind-farm-perf" ]]; then
  echo "apply-r93: refusing non-UC context ctx=$CTX cluster=$cluster" >&2
  exit 1
fi

echo "== r93 migrate $NS run=$RUN_ID redis_env=$REDIS_ENV =="

echo "-- pull secret"
kubectl apply -f "$ROOT/00-namespace.yaml"
kubectl -n developer_026-dev get secret uc-registry -o json \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); d["metadata"]={"name":"uc-registry","namespace":"developer_026-qqfarm"};
[d.pop(k,None) for k in ("resourceVersion","uid","creationTimestamp","managedFields")]; print(json.dumps(d))' \
  | kubectl apply -f -
kubectl apply -f "$ROOT/15-pull-secret-sa.yaml"

echo "-- scale down legacy consumers"
kubectl -n "$NS" scale deploy/farm-gate --replicas=0 --timeout=60s 2>/dev/null || true
kubectl -n "$NS" scale deploy/farm-worker --replicas=0 --timeout=60s 2>/dev/null || true
kubectl -n "$NS" scale sts/player-svr --replicas=0 --timeout=60s 2>/dev/null || true
kubectl -n "$NS" scale sts/farm-svr --replicas=0 --timeout=60s 2>/dev/null || true
kubectl -n "$NS" delete deploy/farm-worker svc/farm-worker --ignore-not-found
kubectl -n "$NS" wait --for=delete pod -l app.kubernetes.io/name=farm-gate --timeout=90s 2>/dev/null || true
kubectl -n "$NS" wait --for=delete pod -l app.kubernetes.io/name=player-svr --timeout=90s 2>/dev/null || true
kubectl -n "$NS" wait --for=delete pod -l app.kubernetes.io/name=farm-svr --timeout=90s 2>/dev/null || true

echo "-- TLS"
TLS_DIR=$(mktemp -d)
trap 'rm -rf "$TLS_DIR" "${BOOTSTRAP_BIN:-}"' EXIT
openssl req -x509 -newkey rsa:3072 -nodes -sha256 -days 14 \
  -subj "/CN=farm-dynamic-route-uc-ca" \
  -keyout "$TLS_DIR/ca.key" -out "$TLS_DIR/ca.crt" >/dev/null 2>&1
generate_leaf() {
  local name="$1" identity="$2"
  local key="$TLS_DIR/$name.key" cert="$TLS_DIR/$name.crt" csr="$TLS_DIR/$name.csr" config="$TLS_DIR/$name.cnf"
  cat >"$config" <<EOF
[req]
distinguished_name = dn
prompt = no
req_extensions = req_ext
[dn]
O = farm-dynamic-route-uc
[req_ext]
subjectAltName = DNS:$identity
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth,clientAuth
[leaf_ext]
subjectAltName = DNS:$identity
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth,clientAuth
EOF
  openssl req -new -newkey rsa:3072 -nodes -sha256 -config "$config" -keyout "$key" -out "$csr" >/dev/null 2>&1
  openssl x509 -req -days 14 -sha256 -in "$csr" -CA "$TLS_DIR/ca.crt" -CAkey "$TLS_DIR/ca.key" -CAcreateserial \
    -extfile "$config" -extensions leaf_ext -out "$cert" >/dev/null 2>&1
}
generate_leaf route-controller "route-controller.$NS.svc.cluster.local"
generate_leaf route-client "route-client.$NS.local"
generate_leaf route-operator "route-operator.$NS.local"
generate_leaf gate-verifier "farm-gate-verifier.$NS.svc.cluster.local"
generate_leaf holder-server "holder-control.$NS.svc.cluster.local"
kubectl -n "$NS" create secret generic farm-route-dynamic-tls \
  --from-file=ca.crt="$TLS_DIR/ca.crt" \
  --from-file=route-controller.crt="$TLS_DIR/route-controller.crt" \
  --from-file=route-controller.key="$TLS_DIR/route-controller.key" \
  --from-file=route-client.crt="$TLS_DIR/route-client.crt" \
  --from-file=route-client.key="$TLS_DIR/route-client.key" \
  --from-file=route-operator.crt="$TLS_DIR/route-operator.crt" \
  --from-file=route-operator.key="$TLS_DIR/route-operator.key" \
  --from-file=gate-verifier.crt="$TLS_DIR/gate-verifier.crt" \
  --from-file=gate-verifier.key="$TLS_DIR/gate-verifier.key" \
  --from-file=holder-server.crt="$TLS_DIR/holder-server.crt" \
  --from-file=holder-server.key="$TLS_DIR/holder-server.key" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "-- secrets + run identity"
if ! kubectl -n "$NS" get configmap farm-r93-identity >/dev/null 2>&1; then
  WORKER_A_BOOT="farm-worker-a-$(python3 -c 'import secrets; print(secrets.token_hex(16))')"
  WORKER_B_BOOT="farm-worker-b-$(python3 -c 'import secrets; print(secrets.token_hex(16))')"
  kubectl -n "$NS" create configmap farm-r93-identity \
    --from-literal=run-id="$RUN_ID" \
    --from-literal=redis-env="$REDIS_ENV" \
    --from-literal=worker-a-boot="$WORKER_A_BOOT" \
    --from-literal=worker-b-boot="$WORKER_B_BOOT" \
    --from-literal=controller-boot-initial="rc-boot-initial-$RUN_ID" \
    --from-literal=controller-boot-assign="rc-boot-assign-$RUN_ID" \
    --from-literal=controller-boot-final="rc-boot-final-$RUN_ID"
fi
WORKER_A_BOOT=$(kubectl -n "$NS" get configmap farm-r93-identity -o jsonpath='{.data.worker-a-boot}')
WORKER_B_BOOT=$(kubectl -n "$NS" get configmap farm-r93-identity -o jsonpath='{.data.worker-b-boot}')
CTRL_BOOT_INITIAL=$(kubectl -n "$NS" get configmap farm-r93-identity -o jsonpath='{.data.controller-boot-initial}')
CTRL_BOOT_ASSIGN=$(kubectl -n "$NS" get configmap farm-r93-identity -o jsonpath='{.data.controller-boot-assign}')
CTRL_BOOT_FINAL=$(kubectl -n "$NS" get configmap farm-r93-identity -o jsonpath='{.data.controller-boot-final}')

if [[ -z "$LOAD_TEST_TOKEN" ]]; then
  LOAD_TEST_TOKEN=$(kubectl -n "$NS" get secret farm-secrets -o jsonpath='{.data.load-test-token-secret}' 2>/dev/null | base64 -d || true)
fi
if [[ -z "$LOAD_TEST_TOKEN" ]]; then
  LOAD_TEST_TOKEN=$(openssl rand -hex 24)
fi

if [[ -z "$(kubectl -n "$NS" get secret farm-secrets -o jsonpath='{.data.theft-prf-hmac-secret}' 2>/dev/null || true)" ]]; then
  python3 - "$NS" "$REDIS_URL" "$LOAD_TEST_TOKEN" <<'PY' | kubectl apply -f -
import base64, json, secrets, sys
ns, redis_url, load_test = sys.argv[1:]
b64 = lambda n: base64.urlsafe_b64encode(secrets.token_bytes(n)).rstrip(b"=").decode()
print(json.dumps({
  "apiVersion": "v1",
  "kind": "Secret",
  "metadata": {"name": "farm-secrets", "namespace": ns, "labels": {"farm.io/secret-scope": "uc-r93-local"}},
  "type": "Opaque",
  "stringData": {
    "redis-url": redis_url,
    "authority-redis-url": redis_url,
    "runtime-redis-url": redis_url,
    "projection-redis-url": redis_url,
    "permit-secret": b64(48),
    "http-session-cookie-keyring": "2=" + b64(48) + ",1=" + b64(48),
    "web-login-rate-limit-secret": b64(48),
    "theft-prf-hmac-secret": b64(48),
    "load-test-token-secret": load_test,
  },
}))
PY
else
  kubectl -n "$NS" patch secret farm-secrets --type=merge -p "$(python3 - <<PY
import json
print(json.dumps({"stringData": {
  "redis-url": "$REDIS_URL",
  "authority-redis-url": "$REDIS_URL",
  "runtime-redis-url": "$REDIS_URL",
  "projection-redis-url": "$REDIS_URL",
  "load-test-token-secret": "$LOAD_TEST_TOKEN",
}}))
PY
)"
fi

kubectl apply -f "$ROOT/20-config.yaml"

echo "-- redis cluster 3m3s"
if kubectl -n "$NS" get deploy farm-redis >/dev/null 2>&1; then
  kubectl -n "$NS" delete deploy farm-redis --wait=true
fi
kubectl apply -f "$ROOT/10-redis.yaml"
kubectl -n "$NS" rollout status sts/farm-redis --timeout=180s
for _ in $(seq 1 60); do
  ready=$(kubectl -n "$NS" get sts farm-redis -o jsonpath='{.status.readyReplicas}')
  [[ "$ready" == "6" ]] && break
  sleep 2
done
[[ "$(kubectl -n "$NS" get sts farm-redis -o jsonpath='{.status.readyReplicas}')" == "6" ]]

cluster_ok() {
  kubectl -n "$NS" exec farm-redis-0 -- redis-cli cluster info 2>/dev/null | tr -d '\r' | grep -q 'cluster_state:ok'
}
if ! cluster_ok; then
  ips=()
  for i in 0 1 2 3 4 5; do
    ip=$(kubectl -n "$NS" get pod "farm-redis-$i" -o jsonpath='{.status.podIP}')
    [[ -n "$ip" ]] || { echo "apply-r93: farm-redis-$i has no IP" >&2; exit 1; }
    ips+=("$ip:6379")
  done
  kubectl -n "$NS" exec farm-redis-0 -- redis-cli --cluster create "${ips[@]}" --cluster-replicas 1 --cluster-yes
fi
ok=0
for _ in $(seq 1 20); do
  if cluster_ok; then ok=1; break; fi
  sleep 1
done
[[ "$ok" == "1" ]] || { echo "apply-r93: redis cluster_state is not ok" >&2; exit 1; }
slots=$(kubectl -n "$NS" exec farm-redis-0 -- redis-cli cluster info | tr -d '\r' | awk -F: '/cluster_slots_assigned/{print $2}')
[[ "$slots" == "16384" ]] || { echo "apply-r93: expected 16384 slots, got '$slots'" >&2; exit 1; }
echo "redis cluster ok slots=$slots"

echo "-- holders + controller(0) [single-holder]"
kubectl apply -f "$ROOT/26-route-controller.yaml"
kubectl apply -f "$ROOT/30-player-svr.yaml"
kubectl apply -f "$ROOT/40-farm-svr.yaml"
for pod in player-svr-0 farm-svr-0; do
  for _ in $(seq 1 90); do
    uid=$(kubectl -n "$NS" get pod "$pod" -o jsonpath='{.metadata.uid}' 2>/dev/null || true)
    [[ -n "$uid" ]] && break
    sleep 1
  done
  [[ -n "${uid:-}" ]] || { echo "apply-r93: $pod UID missing" >&2; exit 1; }
done

echo "-- build bootstrap"
BOOTSTRAP_BIN=$(mktemp)
(cd "$REPO" && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -buildvcs=false -trimpath -o "$BOOTSTRAP_BIN" ./cmd/rt2q-route-bootstrap)

echo "-- bootstrap helper pod"
kubectl -n "$NS" delete pod farm-route-bootstrap --ignore-not-found
kubectl -n "$NS" run farm-route-bootstrap --image=redis:7-alpine --restart=Never --command -- sleep 1800
kubectl -n "$NS" wait --for=condition=Ready pod/farm-route-bootstrap --timeout=90s
kubectl -n "$NS" cp "$BOOTSTRAP_BIN" farm-route-bootstrap:/tmp/rt2q-route-bootstrap
kubectl -n "$NS" exec farm-route-bootstrap -- chmod +x /tmp/rt2q-route-bootstrap

pod_uid() { kubectl -n "$NS" get pod "$1" -o jsonpath='{.metadata.uid}'; }
player_a_uid=$(pod_uid player-svr-0)
farm_a_uid=$(pod_uid farm-svr-0)
player_a_boot="player-svr-0-$player_a_uid"
farm_a_boot="farm-svr-0-$farm_a_uid"
challenge=$(printf '%s' "$RUN_ID" | sha256sum | cut -c1-32)

bootstrap_args=(
  --challenge "$challenge" --realm "$REALM"
  --controller-id route-controller
  --redis-environment "$REDIS_ENV" --redis-zone "$REDIS_ZONE" --shard-count 4
  --player-a player-svr-0 --player-a-boot "$player_a_boot" --player-a-endpoint "player-svr-0.player-svr-headless.$NS.svc.cluster.local:9100"
  --farm-a farm-svr-0 --farm-a-boot "$farm_a_boot" --farm-a-endpoint "farm-svr-0.farm-svr-headless.$NS.svc.cluster.local:9090"
  --farm-worker-a farm-worker-a --farm-worker-a-boot "$WORKER_A_BOOT" --farm-worker-a-endpoint "farm-worker-a.$NS.svc.cluster.local:9201"
  --farm-worker-b farm-worker-b --farm-worker-b-boot "$WORKER_B_BOOT" --farm-worker-b-endpoint "farm-worker-b.$NS.svc.cluster.local:9201"
)

run_bootstrap() {
  local phase="$1" boot="$2"; shift 2
  kubectl -n "$NS" exec farm-route-bootstrap -- env \
    FARM_REDIS_CLUSTER=1 \
    FARM_REDIS_CLUSTER_URLS="$REDIS_CLUSTER_URLS" \
    RT2Q_BOOTSTRAP_REDIS_URL="$REDIS_URL" \
    RT2Q_BOOTSTRAP_REDIS_CLUSTER_URLS="$REDIS_CLUSTER_URLS" \
    /tmp/rt2q-route-bootstrap --controller-boot "$boot" "${bootstrap_args[@]}" "$@"
}

echo "-- bootstrap-only"
run_bootstrap initial "$CTRL_BOOT_INITIAL" --bootstrap-only | tee /tmp/uc-r93-bootstrap-initial.json
python3 - <<'PY'
import json
d=json.load(open("/tmp/uc-r93-bootstrap-initial.json"))
assert d.get("initial_revision")==1 and d.get("final_revision")==1, d
print("bootstrap-only ok", d.get("bootstrap_status"), "rev", d.get("final_revision"))
PY

echo "-- start initial controller"
kubectl -n "$NS" set env deploy/route-controller ROUTE_CONTROLLER_BOOT="$CTRL_BOOT_INITIAL"
kubectl -n "$NS" scale deploy/route-controller --replicas=1
kubectl -n "$NS" rollout status deploy/route-controller --timeout=180s
for pod in player-svr-0 farm-svr-0; do
  kubectl -n "$NS" wait --for=condition=Ready pod/"$pod" --timeout=240s
done

echo "-- workers"
python3 - "$ROOT/60-farm-worker.yaml" "$WORKER_A_BOOT" "$WORKER_B_BOOT" <<'PY' | kubectl apply -f -
import sys
text=open(sys.argv[1]).read()
# first REPLACE_AT_APPLY is worker-a, second is worker-b
text=text.replace("value: REPLACE_AT_APPLY", "value: "+sys.argv[2], 1)
text=text.replace("value: REPLACE_AT_APPLY", "value: "+sys.argv[3], 1)
sys.stdout.write(text)
PY
kubectl -n "$NS" rollout status deploy/farm-worker-a --timeout=180s
kubectl -n "$NS" rollout status deploy/farm-worker-b --timeout=180s

echo "-- stop controller for assign"
for pod in player-svr-0 farm-svr-0; do
  restarts=$(kubectl -n "$NS" get pod "$pod" -o jsonpath='{.status.containerStatuses[0].restartCount}')
  [[ "$restarts" == "0" ]] || { echo "apply-r93: $pod restarted ($restarts) before assign" >&2; exit 1; }
  observed=$(kubectl -n "$NS" exec "$pod" -- cat /var/run/farm-holder/current-boot-id)
  expected="${pod}-$(pod_uid "$pod")"
  [[ "$observed" == "$expected" ]] || { echo "apply-r93: boot mismatch $pod $observed != $expected" >&2; exit 1; }
done
kubectl -n "$NS" scale deploy/route-controller --replicas=0
kubectl -n "$NS" wait --for=delete pod -l app.kubernetes.io/name=route-controller --timeout=90s 2>/dev/null || true
[[ -z "$(kubectl -n "$NS" get pod -l app.kubernetes.io/name=route-controller -o name)" ]]

echo "-- assign packed 4/4 [single-holder]"
run_bootstrap assign "$CTRL_BOOT_ASSIGN" | tee /tmp/uc-r93-bootstrap-assign.json
python3 - <<'PY'
import json
d=json.load(open("/tmp/uc-r93-bootstrap-assign.json"))
claims=d.get("claims") or []
assert d.get("final_revision",0)>d.get("initial_revision",0) and len(claims)>=8, d
print("assign ok claims", len(claims), "rev", d.get("final_revision"))
PY

echo "-- start final controller"
kubectl -n "$NS" set env deploy/route-controller ROUTE_CONTROLLER_BOOT="$CTRL_BOOT_FINAL"
kubectl -n "$NS" scale deploy/route-controller --replicas=1
kubectl -n "$NS" rollout status deploy/route-controller --timeout=180s
for pod in player-svr-0 farm-svr-0; do
  kubectl -n "$NS" wait --for=condition=Ready pod/"$pod" --timeout=240s
done

echo "-- verify packed Ready [single-holder]"
for pair in farm-svr-0:9091 player-svr-0:9101; do
  pod=${pair%%:*}; port=${pair#*:}
  body=$(kubectl -n "$NS" exec "$pod" -- wget -q -O - "http://127.0.0.1:${port}/readyz")
  echo "$pod /readyz $body"
  python3 -c 'import json,sys; d=json.loads(sys.argv[1]); rt=d.get("route_runtime") or {}; assert d.get("status")=="ready" and int(rt.get("serving_shard_count",0))==4, d' "$body"
done

echo "-- gate + remaining demo surfaces"
kubectl apply -f "$ROOT/50-farm-gate.yaml"
kubectl apply -f "$ROOT/70-farm-h5.yaml"
kubectl apply -f "$ROOT/80-observability.yaml"
kubectl -n "$NS" rollout status deploy/farm-gate --timeout=180s
kubectl -n "$NS" rollout status deploy/farm-h5 --timeout=120s
kubectl -n "$NS" delete pod farm-route-bootstrap --ignore-not-found

echo "== r93 Ready =="
kubectl -n "$NS" get pods,sts,deploy,svc
echo "run_id=$RUN_ID redis_env=$REDIS_ENV packed=player-svr-0/farm-svr-0 serving_shard_count=4 [single-holder]"
