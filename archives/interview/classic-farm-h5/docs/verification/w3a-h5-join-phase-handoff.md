# W3A · H5 JoinPhase、PlayerBootstrap 与恢复屏障交接

日期：2026-08-03
状态：`READY_TO_COMMIT`
工作树：`classic-farm-h5-w3a`
分支：`codex/w3a-h5-join-phase-20260803`

本文只记录本批实现和验证事实。接入/Join 语义以 26 号为准，公网协议以 28 号为准；本文不能覆盖合同。

## 1. 本批完成范围

### 1.1 唯一 Join 主链

生产 Canonical Adapter 只接受：

```text
CommandResult(ACCEPTED + JoinResultContext)
→ FarmSnapshotPush(V)
→ FarmDeltaBatchPush(V+1...N)*
→ SubscriptionReadyPush(N)
→ applied_farm_version == N
→ 叠加 Recovery/PlayerBootstrap 屏障
→ InFarm（FARM_INTERACTIVE 的兼容名）
```

- ACCEPTED、Snapshot、Delta、Ready 校验 `target_farm_id + subscription_id + join_attempt_id`；
- Snapshot 只建立版本基线，Ready 前不开放经营写；
- Catch-up 批内严格连续，Live FarmChange 只允许当前版本 `+1`；
- 旧 attempt、旧 subscription 和旧 farm Push 不污染当前页面；
- `REJOIN_REQUIRED` 使用新的 Join 意图执行 Fresh Join；
- Canonical Join ACCEPTED 的 `result_payload` 必须为空，旧的内嵌 `FarmCommandResult` 直接按协议错误拒绝。

### 1.2 独立 PlayerBootstrap

- 新增 `GET_PLAYER_BOOTSTRAP` 客户端命令映射；
- ServerHello 后与 Join、Pending 恢复和好友读取并行发起；
- 私有 Wallet/Inventory/Codex 只从 PlayerBootstrap 或玩家命令结果更新，不再依赖 FarmSnapshot；
- `economy_version` 单调应用，迟到的低版本 Bootstrap 不覆盖更新后的资产；
- `FARM_INTERACTIVE` 同时要求 Join Ready、PlayerBootstrap 和本次重连恢复屏障完成。

### 1.3 Pending Operation 恢复

- IndexedDB Journal 按 `environment + player_id` 扫描；
- 每条恢复记录先发送原 operation 的结果查询，再继续有界退避轮询；
- Journal 读取或首轮查询无法进入 WSS 写路径时 Fail Closed：不开放经营写，保留记录并重试；
- `PROCESSING` 继续恢复，终态才清理 Journal；
- 本批没有把“查询已发出”描述成所有 Workflow 已终态，只把它作为本轮恢复首轮已成功建立的边界。

### 1.4 协议一致性修复

- `FARM_CHANGE` 解码当前冻结的 `FarmChangePush { change }` 包装；
- 测试 Codec 复用生成的 `CommandType` 枚举，不再维护会漂移的手写数字；
- `FARM_GAME_ROOT` 可让恢复 E2E 指向明确的后端 worktree；
- 根目录 `node_modules` 符号链接使用精确忽略规则，不再误入 Git 状态。

## 2. 验证证据

2026-08-03 在 Linux 当前工作树执行：

```text
npm test -- --run
→ 12 个测试文件，71 个测试通过

npm run build:web
npm run build:staging
npm run build:wechat
npm run audit:build
→ 全部通过；三种构建均通过 real-backend boundary audit

FARM_WORKFLOW_E2E=1 FARM_GAME_ROOT=<farm-game-w3b> npm run e2e:web
→ 2/2 通过，耗时 41.6s
```

浏览器 E2E 覆盖：

1. Redis Web 账号注册、退出后重新登录并保持相同 player_id；
2. 分享链接建立真实好友关系；
3. Buy → Plant → Mature → Harvest；
4. Harvest 后停止 Worker/Player，再恢复未决 Workflow；
5. 重启全部后端后重新连接，作物、地块和访客资产仍一致。

故障测试期间出现的短暂 `ECONNREFUSED 127.0.0.1:8080` 是脚本主动停止 Gate 的预期窗口，最终断言通过。

## 3. 未闭合项

1. Gate 的 `recovery_complete_at_admission` 当前仍恒为 false。H5 已本地 Fail Closed，但 26/29 尚未给出浏览器到 Gate 的受信完成信号；不能由客户端普通字段自行声明。需要后续单独冻结并实现 RecoveryAdmission 接缝。
2. 当前物理 `FarmFrame` 仍是 `kind + body`，不是 28 号最终建议的顶层 oneof；业务命令也尚未全部切到 Typed Payload。属于 W11B-W。
3. 本批真实 E2E 是 Direct WSS、单 Gate。双 Gate Lease、TCONND、200 人同场、慢连接和容量仍未验证。
4. Pending 恢复的公网 `RetryDirective`/NOT_FOUND 完整矩阵仍依赖 W7B/W7C；本批只保证首轮恢复失败不开放写。

## 4. 接手约束

- 不得把私有资产重新塞进 FarmSnapshot；
- 不得恢复 Snapshot 即 InFarm 的旧路径；
- 不得对 Canonical Join 同时兼容空 payload 和旧内嵌 payload；
- 后续改 Proto 必须重新生成 Go/TS，并同时跑黄金向量、全 H5 构建和真实 E2E；
- 本文件所在提交即本批代码基线，最终 commit/hash 由分支日志记录。
