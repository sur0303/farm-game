# Reproduction commands

```bash
bash /data/home/developer_026/.codex/skills/prepare-qqfarm-loadtest/scripts/preflight.sh \
  --repo /data/home/developer_026/CodeBuddy/20260519141303/farm-game-scaleplan-local-kind-qqfarm-20260816 \
  --require-ready

kubectl --context uc-test-plus -n developer_026-qqfarm set env deployment/farm-gate \
  GATE_FRIENDSHIP_HTTP_RATE_PER_SEC=10000 \
  GATE_FRIENDSHIP_HTTP_RATE_BURST=20000 \
  GATE_PPROF_ENABLED=1

# fixture parameters: viewer=931005000, viewer-count=300,
# fake-base=7810050000, friend-count=50, density=20, concurrency=32,
# route=redis-current and fixed qqfarm confirmation.

kubectl --context uc-test-plus -n developer_026-qqfarm exec <gate-pod> -- \
  wget -qO /tmp/gate-opportunity-r100.cpu.pprof \
  'http://127.0.0.1:8080/debug/pprof/profile?seconds=12'

bash deploy/k8s/uc-test-plus/developer_026-qqfarm/run-ws-stress.sh \
  thick-login 300 100 30s 931005000

go tool pprof -top -nodecount=50 /tmp/gate-opportunity-r100.cpu.pprof

kubectl --context uc-test-plus -n developer_026-qqfarm set env deployment/farm-gate \
  GATE_FRIENDSHIP_HTTP_RATE_PER_SEC=20 \
  GATE_FRIENDSHIP_HTTP_RATE_BURST=40 \
  GATE_PPROF_ENABLED-
```

