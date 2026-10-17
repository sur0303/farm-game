# G2 50 好友 Thick Login 复现实录

固定环境：`uc-test-plus / developer_026-qqfarm`，代码 `992013db4f3b10c1892f61758cb8e1ba41a312de`，runtime `farm-route-runtime@sha256:0d53388836c45fb5ee8de7e38cfee5a78b0e87cde0f97ac0a0dd10295b9ec026`，压测器 `farm-ws-stress@sha256:d59aaf496e03e3b9fd6abb4f1818315ea6eccabe5ce815c6985c3289bd40bac6`。

每一档前后执行：

```bash
bash /data/home/developer_026/.codex/skills/prepare-qqfarm-loadtest/scripts/preflight.sh \
  --repo /data/home/developer_026/CodeBuddy/20260519141303/farm-game-scaleplan-local-kind-qqfarm-20260816 \
  --require-ready
```

好友 fixture 的固定参数：`--viewer-count=300 --friend-count=50 --density=20 --concurrency=32`，每档 viewer 与 fake friend 区间不重叠；fixture 使用 `--route=redis-current` 和固定确认串 `uc-test-plus/developer_026-qqfarm/uctestscaleplanqqfarm20260816/z1`。具体 fixture 输出保存在同目录的 `fixture-*.json`。

压测前临时放宽 Gate 好友 HTTP 保护限流（仅测试窗口）：

```bash
kubectl --context uc-test-plus -n developer_026-qqfarm set env deployment/farm-gate \
  GATE_FRIENDSHIP_HTTP_RATE_PER_SEC=10000 GATE_FRIENDSHIP_HTTP_RATE_BURST=20000
kubectl --context uc-test-plus -n developer_026-qqfarm rollout status deployment/farm-gate --timeout=180s
```

各档实际压测命令（脚本会创建带时间戳的 Job；对应 Job 日志中的 JSON 已保存）：

```bash
bash deploy/k8s/uc-test-plus/developer_026-qqfarm/run-ws-stress.sh thick-login 300 500 30s 923000000  # dkqi6r1hlq6t
bash deploy/k8s/uc-test-plus/developer_026-qqfarm/run-ws-stress.sh thick-login 300 100 30s 923100000  # dkqi9c47mn2c
bash deploy/k8s/uc-test-plus/developer_026-qqfarm/run-ws-stress.sh thick-login 300 20  30s 923200000  # dkqiafugcsh6
bash deploy/k8s/uc-test-plus/developer_026-qqfarm/run-ws-stress.sh thick-login 300 50  30s 923300000  # dkqibb35379p
bash deploy/k8s/uc-test-plus/developer_026-qqfarm/run-ws-stress.sh thick-login 300 75  30s 923400000  # dkqic7p6pohx
bash deploy/k8s/uc-test-plus/developer_026-qqfarm/run-ws-stress.sh thick-login 300 85  30s 923500000  # dkqidmuwkk6d
```

压测后恢复默认保护配置并再次预检：

```bash
kubectl --context uc-test-plus -n developer_026-qqfarm set env deployment/farm-gate \
  GATE_FRIENDSHIP_HTTP_RATE_PER_SEC=20 GATE_FRIENDSHIP_HTTP_RATE_BURST=40
kubectl --context uc-test-plus -n developer_026-qqfarm rollout status deployment/farm-gate --timeout=180s
bash /data/home/developer_026/.codex/skills/prepare-qqfarm-loadtest/scripts/preflight.sh \
  --repo /data/home/developer_026/CodeBuddy/20260519141303/farm-game-scaleplan-local-kind-qqfarm-20260816 \
  --require-ready
```

恢复后的观测：Gate、PlayerSvr、FarmSvr、两个 Worker、Controller 均 Ready，restart=0；Gate `farmgate_ws_conn_rejected_total=0`、writer queue=0，Farm `farm_downlink_queue_full_total=0`，Player/Farm mailbox reject=0。临时 fixture Pod 已删除。
