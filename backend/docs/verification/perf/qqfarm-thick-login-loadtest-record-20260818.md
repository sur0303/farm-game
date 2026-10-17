# QQFarm Thick Login 压测记录（uc-test-plus）

- 日期：2026-08-18
- 集群：`uc-test-plus`
- Namespace：`developer_026-qqfarm`
- 工作树：`/data/home/developer_026/CodeBuddy/20260519141303/farm-game-scaleplan-local-kind-qqfarm-20260816`
- 分支：`codex/scaleplan-local-kind-qqfarm-r1`
- 状态：工作树未提交，本文档仅记录，不 commit

## 1. 测试链路与口径

压测工具为 `ws-stress` 虚拟客户端，不是 `live-farm-bot`，也不使用真实账号密码。认证方式为：

```text
auth_mode=load_test_token
POST /api/v1/auth/login
→ WS ClientHello/ServerHello
→ 登录附带 home_bootstrap 投影
→ JOIN_FARM
→ Snapshot
→ ClientRecoveryComplete
→ GET /api/v1/friends/opportunity（异步）
```

每个玩家只执行一次登录/首页 Ready 流程，不执行种植、收获等持续游戏行为。

本轮使用新生成的 player ID，没有预先建立好友关系，因此 Friend opportunity 分支是服务端明确返回 `NONE`，不是客户端伪造，也不是完整好友列表查询。

注意：压测参数中的 `rate` 是客户端启动虚拟玩家的速率，不是 RPC QPS。旧输出中的 `qps` 是 `ready / 整个 duration`，不能当作真实登录 QPS。

## 2. 部署与运行状态

### 2.1 镜像

```text
Runtime:
mirrors.tencent.com/uc-backend/farm-route-runtime@sha256:006f221cbdd87bbd7278055bd35d196968de79843640c0df53a46aa70ae28380

ws-stress（含序号修复、静态编译、压测客户端退出修复）：
mirrors.tencent.com/uc-backend/farm-ws-stress@sha256:62306838252ce31b0fb93541de1bdf1eea2cdbae2f2588c63176734835128371
```

### 2.2 Redis 临时拓扑

按用户要求，压测期间不恢复 Redis 从节点：

```text
farm-redis-0/1/2：3 个 Master，cluster_state=ok，16384 slots
farm-redis-3/4/5：已临时 scale down，未执行 FLUSHALL
```

每个从节点原本请求 `500m CPU / 2Gi`，临时缩容释放约 `1.5 CPU request / 6Gi memory request`。

**恢复动作必须等待用户通知。**

### 2.3 Gate 调度事故

一次 4 Pod 压测使用每 Pod `2 CPU / 8Gi` request，导致集群按 request 调度失败：

```text
0/157 nodes are available: 152 Insufficient cpu
farm-gate 被 Preempted
新 farm-gate Pending
```

该轮结果全部无效，错误主要为 `connection refused`、`context deadline exceeded`、`bad handshake`。后续压测改为每 Pod `500m CPU / 2Gi` request、`2 CPU / 8Gi` limit，Gate 保持 `1/1 Ready`。

## 3. 单 Pod 压测结果

所有测试均为 Thick Login、无好友关系、opportunity 返回 NONE。

| 配置 | attempted | ready | Ready rate | Ready P95 | 主要结论 |
|---|---:|---:|---:|---:|---|
| 1 / 1/s smoke | 1 | 1 | 100% | 约 41ms | 全链路通过 |
| 3000 / 1000/s | 3000 | 3000 | 100% | 49.9ms | 目标档通过 |
| 5000 / 1500/s | 5000 | 5000 | 100% | 38.8ms | 无错误 |
| 10000 / 2000/s | 10000 | 10000 | 100% | 2.91s | 全部成功，但延迟饱和 |
| 15000 / 3000/s | 15000 | 9595 | 64.0% | 14.83s | 连接/超时开始大量失败 |
| 3000 / 3000/s | 3000 | 3000 | 100% | 530.9ms | 瞬时 3000/s 通过 |
| 3000 / 5000/s | 3000 | 3000 | 100% | 979.4ms | 延迟升高但无错误 |
| 3000 / 10000/s | 3000 | 3000 | 100% | 1.08s | 单 Pod 瞬时突发仍通过 |

### 3.1 3000 / 3000/s 细分

```text
login P95：427.7ms
bootstrap P95：434.8ms
hello P95：27.1ms
join P95：76.0ms
friend opportunity P95：37.6ms
friend opportunity completed：3000/3000
friend opportunity found：0
friend opportunity none：3000
friend startup errors：0
```

## 4. 持续 3000/s 聚合测试

配置：4 个独立压测 Pod，每个 `15000 connections / 750/s / 25s`，聚合目标约 `3000/s`。压测 Pod 使用 `500m CPU / 2Gi` request，避免驱逐 Gate。

```text
目标连接数：60000
实际 attempted：58896
ready：42234
ready rate：约 71.7%
Gate：测试期间保持 1/1 Ready
```

各 Pod Ready rate：

```text
A：10669 / 14752 = 72.3%
B：10939 / 15000 = 72.9%
C：11034 / 15000 = 73.6%
D： 9592 / 14144 = 67.8%
```

延迟明显进入排队区：

```text
Ready P95：约 9.4s～12.4s
Login P95：约 5.6s～5.8s
Bootstrap P95：约 8.0s～11.3s
```

Friend opportunity 在高压下出现超时/服务繁忙，已不再是 100% 完成；这属于下游压力结果，不应计为正常 NONE。

## 5. RPC / Actor 指标

来源：Player/Farm `/metrics`，比较压测前后 counter。窗口按配置的约 25s 粗略估算，RPC 数是服务端内部 gRPC 调用数，不是玩家登录数。

### 5.1 gRPC 调用

```text
farm_rpc_calls_total：约 265580
内部 RPC 调用速率：约 10600/s

EnsureOwnerInitialized OK：52003
EnsureOwnerInitialized ResourceExhausted：6121
EnsureOwnerInitialized Unavailable：42
ArmSnapshot：42234
ExecuteCommand：42234
GetPublicProfile：42234
GetNextFriendOpportunity：36654
CleanupSubscription：42269
```

### 5.2 Actor 指标

```text
farm_actor_submit_total：约 279812（约 11200/s）
  actor_kind=farm：169641
  actor_kind=player：110171

farm_actor_activation_throttled_total：6121
  actor_kind=player：6121

farm_actor_activation_load_fail_total：36
farm_actor_mailbox_rejected_total：0
farm_actor_system_queue_rejected_total：0
```

原始指标采样目录：

```text
/tmp/qqfarm-real-rpc-qps-1786983854/
```

## 6. 瓶颈结论

当前瓶颈不是 CPU 满载，而是：

1. **Player Actor 激活 admission**
   - `EnsureOwnerInitialized` 有 6121 次 `ResourceExhausted`
   - `farm_actor_activation_throttled_total=6121`
   - 这是最直接的后端拒绝证据

2. **Actor 数据/内存压力**
   压测后采样：

   ```text
   player-svr：约 60m CPU / 7.4Gi memory
   farm-svr：约 47m CPU / 6.0Gi memory
   farm-gate：约 13m CPU / 3.8Gi memory
   ```

   CPU 很低，但 Player/Farm 内存已经接近各自 `8Gi` limit。该路径主要等待 Redis、Actor 激活、投影和 RPC，不是纯 CPU 计算。

3. **连接和登录等待**
   高压时出现：

   ```text
   context deadline exceeded
   connection refused（仅 Gate 被抢占的无效轮次）
   network timeout
   ```

4. **Gate 错误映射需要继续检查**
   高压聚合测试中出现约 6100 次登录 `401 ERR_UNAUTHENTICATED`，数量与 Actor activation throttle 接近。低压时 401 为 0，因此更像高压下后端 `ResourceExhausted` 到登录错误的映射/超时副作用，不应直接判断为 secret 配置错误。

5. **不是 Mailbox 饱和**
   Mailbox reject 和 system queue reject 都是 0，说明当前主要不是 Actor mailbox 队列溢出。

## 7. 当前有效结论

```text
无错误稳定档：5000 connections
3000 个玩家瞬时 3000/s：100% Ready
3000/s 持续 20 秒、约 60000 玩家：Ready 约 71.7%
```

因此：

> 3000 个玩家的瞬时登录链路没有问题；真正持续 3000/s 时，首先触发的是 Player Actor 激活 admission 和内存压力，而不是 CPU 极限。

## 8. 后续建议

- 保持 Redis 3 Master，等待用户通知后再恢复 3 个 Slave。
- 继续保留 Gate/Player/Farm `/metrics` 的压测前后采样。
- 重点调节或进一步观测 `MAX_CONCURRENT_ACTOR_LOADS`、`MAX_PENDING_ACTOR_LOADS`、Actor load latency 和 Player 内存。
- 检查高压下 `ResourceExhausted → 401` 的 Gate 登录错误映射。
- 后续压测继续使用低 request 的多 Pod generator，避免压测 Pod 驱逐 Gate。
- 不把 `friend_opportunity_none` 当作好友场景成功；当前只是无好友关系的 NONE 分支。
- Redis 从节点恢复动作必须等待用户明确通知。
