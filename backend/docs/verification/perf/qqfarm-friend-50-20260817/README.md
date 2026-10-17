# 50 好友 thick-login 证据

本目录保存 2026-08-17（UTC 2026-08-16）在 `uc-test-plus/developer_026-qqfarm` 的结果、复现参数和环境快照。每个 `thick-login-*.json` 是对应 Job 日志中的完整结果对象；Job 名、run ID、fixture 和配置切换均保留在本 README。

## 固定范围

- Worktree：`/data/home/developer_026/CodeBuddy/20260519141303/farm-game-scaleplan-local-kind-qqfarm-20260816`
- Branch：`codex/scaleplan-local-kind-qqfarm-r1`
- 基线 commit：`5c516034b3f56df1c8bc2e1900b0ec18bced8867`
- Runtime image：`mirrors.tencent.com/uc-backend/farm-route-runtime@sha256:0d53388836c45fb5ee8de7e38cfee5a78b0e87cde0f97ac0a0dd10295b9ec026`
- ws-stress image：`mirrors.tencent.com/uc-backend/farm-ws-stress@sha256:d59aaf496e03e3b9fd6abb4f1818315ea6eccabe5ce815c6985c3289bd40bac6`
- 拓扑：1 Gate、1 PlayerSvr、1 FarmSvr、2 Workers、1 Controller、3 主 3 从 Redis
- fixture：50 个共享好友；机会密度 20%；稳定 `sid1_` crop cycle ID

## 数据范围

- 单账号 smoke：viewer `920000020`，fixture `qqfarmfriends50smoke`
- 200 人复测：viewer `922000000–922000199`，fixture `qqfarmfriends50x200r2`
- fake friend pool：`7600000000–7600000049`
- 200 人关系：10,000 条 canonical Pair，20,000 条双向 FriendIndex 行

## 复现命令

fixture 使用 namespace 固定确认串，仅允许写入专用 qqfarm 测试 namespace；每次执行前后运行：

```bash
bash /data/home/developer_026/.codex/skills/prepare-qqfarm-loadtest/scripts/preflight.sh \
  --repo /data/home/developer_026/CodeBuddy/20260519141303/farm-game-scaleplan-local-kind-qqfarm-20260816 \
  --require-ready
```

严格登录链路：

```bash
bash deploy/k8s/uc-test-plus/developer_026-qqfarm/run-ws-stress.sh \
  thick-login 200 5 50s 922000000
```

`run-ws-stress.sh` 只把 Job 日志打印到终端；本目录中的 JSON 文件是从 Job 原始日志逐字保存的，不能只依据 Job exit code 判定成功。

## 结果摘要

- `dkqhq0flyvl8`：1/1，50 好友，首屏 20 张卡片，机会 `FOUND`，全部错误 0。
- `dkqhqvfzpy0c`：200 人、100/s；登录/Bootstrap 均完成，但 185 个好友请求被 Gate 当前 20 req/s、burst 40 的保护性限流返回 429；不是容量 PASS。
- `dkqhryopeyhe`：200 人、5/s、50s；200/200 完整成功，好友启动 P95 96.275966ms，queue-full 0，业务 Pod restart 0。
- `dkqi6r1hlq6t`：300 人、500/s；59/300，好友接口 503、Bootstrap 超时，属于冷 Actor/下游超时失败。
- `dkqi9c47mn2c`：300 人、100/s；177/300，123 个好友启动 503，属于冷 Actor/好友 RPC 超时失败。
- `dkqiafugcsh6`：300 人、20/s；300/300，好友启动 P95 99.351012ms，功能 PASS。
- `dkqibb35379p`：300 人、50/s；300/300，好友启动 P95 148.559037ms，功能 PASS。
- `dkqic7p6pohx`：300 人、75/s；300/300，但好友启动 P95 2302.662807ms、Ready P95 2522.809353ms，延迟拐点档，不作为安全容量。
- `dkidmuwkk6d`：300 人、85/s；256/300，44 个好友启动 503，失败档。

### G2 临时配置与恢复

默认配置是 `GATE_FRIENDSHIP_HTTP_RATE_PER_SEC=20`、`GATE_FRIENDSHIP_HTTP_RATE_BURST=40`。为执行 Thick Login 容量档，先用 Kubernetes 临时设置为 `10000/20000`，压测完成后明确恢复为 `20/40`；这几档不能解释为生产默认限流下的容量。恢复后 `prepare-qqfarm-loadtest --require-ready` 通过，Gate、PlayerSvr、FarmSvr、两个 Worker、Controller 均 Ready，业务 Pod restart=0。

本轮所有厚登录档均为 300 个新 viewer、每人 50 个好友、20% opportunity 密度、30s Job；每个档使用不重叠 viewer/fake-player 区间。由于 fixture 的产品安全上限是 300 个 viewer/批次，500/s 等档是 300 人短批探索，未宣称满足方案要求的 10 分钟持续阶梯。

### 观察到的瓶颈

高档失败原因是好友服务返回 503（`friend roster unavailable` / `friend opportunity unavailable`）及少量 Bootstrap `ERR_TIMEOUT`，不是 Gate 429。Gate 的 `ws_conn_rejected_total`、FarmSvr `farm_downlink_queue_full_total`、Actor mailbox reject 均为 0；Player Actor turn 延迟和冷 Actor 数随批次显著增长。`FRIENDSHIP_RPC_TIMEOUT` 当前为 2s，75/s 已出现好友启动 P95 约 2.30s，85/s 开始部分 503。因此当前 50 好友厚登录探索区间为：20/50/s 功能通过，75/s 延迟拐点，85/s 失败；这不是 10 分钟生产安全容量结论。

前两次 50 好友 fixture 曾使用普通 `crop_cycle_id`，被生产协议校验降为 UNKNOWN；之后已改为正式稳定 opaque ID，并通过 direct read、FarmSvr RPC 与真实 thick-login 复验。正式结果只采用修复后的 `dkqhq0flyvl8` 和 `dkqhryopeyhe`。
