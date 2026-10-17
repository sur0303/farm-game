# SCB-SOCIAL-PUBLIC-API-V1 · 社交公网入口与迁移合同

版本：`v1.1-i0-confirmed`
日期：`2026-08-09`
状态：`I0_USER_CONFIRMED / I1_MIGRATION_OPEN`
审计角色：`R1-I0-SOCIAL-PARAMETERS` 唯一 Writer / 参数与装配审计者

## 0. 合同地位、冻结输入与非目标

本文只冻结下列七项社交能力的公网传输入口、认证和可靠性合同：

```text
CreateShare
RedeemShare
FriendList
DeleteFriend
Block
Unblock
NextOpportunity
```

本文对上述七项能力的“HTTPS 还是 WSS”选择是窄域权威。它不重写好友业务语义、Join、Care、Theft、
Session 或内部 RPC：

- Session、Cookie、Ticket、WSS 激活和 Join Fence 继续服从
  [26-access-session-contract.md](./26-access-session-contract.md)；
- WSS FarmFrame、CommandResult 和通用 `operation_id` 语义继续服从
  [28-command-catalog-public-protocol.md](./28-command-catalog-public-protocol.md)；
- 好友 Pair、ShareIssuer、删除、拉黑、名单和机会语义继续服从
  [33-friendship-share-access-contract.md](./33-friendship-share-access-contract.md)；
- [40-core-technical-closure.md](./40-core-technical-closure.md) 仍是总体导航；本文只把其“低频 HTTPS
  接口”范围具体化为本合同列出的社交接口。

若上述文档把本合同七项能力列作 WSS Command，而本文把其公网主入口冻结为 HTTPS，则只在“公网传输
入口”这一维以本文为准；业务状态机、Authority、Permit 和执行接受边界仍由原权威文档拥有。任何实现
批次不得借本合同把 JOIN/WATER/PEST/THEFT 改成 HTTP。

本次机械审计的冻结快照：

| 仓库 | commit | 权限 |
|---|---|---|
| Backend | `dd7104a3c43cecdc85c8d7962014f9760bf28f2d` | I0 参数、合同、测试与非 R1-4 装配文件可写 |
| H5 | `fdb9849c0aad613b9a1c45ab8bf33a692c5ee967` | 只读机械核验；无业务改动 |

I0 实现审计覆盖 `FriendQuota`、Friend API、player-svr/FriendshipDomain、gateway share handler、真实
farm-gate 入口、showcase/local/staging/Kubernetes 配置、H5 `src/friends/**`、压测 profile 与 R1-Q manifest。
机器权威源为
[`social-product-profile-v1.json`](../../internal/socialprofile/social-product-profile-v1.json)。

本批明确不做：

- 不修改 Proto、生成代码或 H5；
- 不迁移 `FriendshipDomain`，不改变 Pair/ShareIssuer/Quota 的 Authority 归属；
- 不删除 legacy Store、Handler、CommandSpec 或字段；
- 不实现 Block/Unblock 或任何新入口；
- 不声称当前部署已经满足本文所有目标字段、错误和观测要求；
- 不 push、merge 或 tag。

## 1. 判定词和唯一结论

判定词只表达“当前快照是否存在可机械证明的产品公网入口”，不替代后续 I1 合规验收：

| 判定 | 精确定义 |
|---|---|
| `READY_HTTP_PRIMARY` | H5 存在真实 HTTPS JSON 调用，Backend 白名单内存在同路径 Method/Handler；对应 WSS Command 未形成第二条 enabled 产品主线。仍可能有本合同列明的兼容缺口。 |
| `READY_WSS_PRIMARY` | WSS CommandSpec 已 enabled，Typed Payload/Handler 主链可用，且没有并行 HTTP 产品主入口。 |
| `CONTRACT_CONFLICT` | 没有端到端产品入口，或存在无法从冻结快照裁决的双入口。只有领域方法或 Proto/Decoder 不算产品化。 |
| `DEFERRED_NOT_EXPOSED` | 用户明确延后；H5 无入口、HTTP 无 route、WSS disabled，不构成当前 Showcase R1 阻塞。领域合同存在不等于 F05 已实现。 |

七项能力的唯一结论如下；不得在后续文档再给出第二套判定：

| 能力 | 唯一结论 | 当前真实 H5 入口 | 当前真实 Backend 入口 | 结论理由 |
|---|---|---|---|---|
| CreateShare | `READY_HTTP_PRIMARY` | `POST /api/v1/share/create` | `ShareHandler.handleCreate` | H5 与 Backend 路径/Method 同向；WSS `GENERATE_SHARE_TOKEN` disabled。 |
| RedeemShare | `READY_HTTP_PRIMARY` | `POST /api/v1/share/accept` | `ShareHandler.handleAccept` | Hero Social E2E 实际用分享链接让 B/C 接受 A；WSS `REDEEM_SHARE` disabled。路径中的 `accept` 是现行兼容名，能力语义仍叫 RedeemShare。 |
| FriendList | `READY_HTTP_PRIMARY` | `GET /api/v1/friends` | `ShareHandler.handleListFriends` | H5 解析分页、revision、精确机会字段；WSS `GET_FRIEND_LIST` disabled。 |
| DeleteFriend | `READY_HTTP_PRIMARY` | `POST /api/v1/friends/revoke` | `ShareHandler.handleRevokeFriend` | H5 生成 UUID 并调用现行 Handler；WSS `DELETE_FRIEND` disabled。路径中的 `revoke` 是 V1 兼容名。 |
| Block | `DEFERRED_NOT_EXPOSED` | 无 | 无 HTTP route/handler；WSS disabled | 用户明确延后；领域方法存在不等于 F05 已实现。 |
| Unblock | `DEFERRED_NOT_EXPOSED` | 无 | 无 HTTP route/handler；WSS disabled | 同上。 |
| NextOpportunity | `READY_HTTP_PRIMARY` | `GET /api/v1/friends/opportunity` | `ShareHandler.handleFriendOpportunity` | Hero Social E2E 验证 A→B→C 和明确 NONE；WSS `GET_NEXT_FRIEND_OPPORTUNITY` disabled。 |

七项中没有 `READY_WSS_PRIMARY`。

`READY_HTTP_PRIMARY` 不表示当前 Handler 已经满足本文第 4～7 节的 `operation_id` 原样身份、稳定错误码、
Retry directive、Token URL 安全或兼容删除门。那些差距是必须迁移的 I1 阻塞项，不得用 READY 标签掩盖。

## 2. 通道冻结：唯一公网入口

### 2.1 七项社交能力

V1 冻结下列公网主入口。五条已存在的 URL 在 V1 内不为 REST 命名整齐而改名；Block/Unblock 当前不
分配公网路由，后续如重新进入产品范围必须另开合同与验收批次：

| 能力 | 主通道 | Method + Path | 编码 | 当前状态 |
|---|---|---|---|---|
| CreateShare | HTTPS | `POST /api/v1/share/create` | JSON | 已有兼容实现 |
| RedeemShare | HTTPS | `POST /api/v1/share/accept` | JSON | 已有兼容实现 |
| FriendList | HTTPS | `GET /api/v1/friends?cursor=&page_size=` | JSON | 已有兼容实现 |
| DeleteFriend | HTTPS | `POST /api/v1/friends/revoke` | JSON | 已有兼容实现 |
| Block | 无 | — | — | `DEFERRED_NOT_EXPOSED` |
| Unblock | 无 | — | — | `DEFERRED_NOT_EXPOSED` |
| NextOpportunity | HTTPS | `GET /api/v1/friends/opportunity` | JSON | 已有兼容实现 |

“唯一”意味着：同一产品能力不得同时把 HTTPS 与 WSS 都设为 enabled 并让 H5 按环境、页面或失败类型
随机选择。HTTPS Handler 可调用同一个 FriendshipDomain；它不是第二份好友 Authority。

以下不算第二条公网业务入口：

- HTTPS Handler 到 `FriendshipDomain`/FarmRead 的受信内部调用；
- Redeem 成功后单独发送 WSS `JOIN_FARM(inviter_player_id)`；
- NextOpportunity 返回候选后通过 WSS Join/Care/Theft 执行实时动作；
- 服务端在好友 `SUBSCRIPTION_READY` 后内部标记 Navigation visited。

### 2.2 不得改写的 WSS 主线

下列能力保持 `WSS + Binary Protobuf FarmFrame`，不为“社交接口都走 HTTPS”而迁移：

| 产品动作 | WSS Command | 当前机械状态 | 冻结结论 |
|---|---|---|---|
| 进入自己/好友农场 | `JOIN_FARM` | CommandSpec `Enabled=true`，`IdempotencyRuntimeRequired`，FARM Actor Affinity | `READY_WSS_PRIMARY`；Redeem 与 Join 是两个独立结果。 |
| 浇水 | `CARE(CARE_TYPE_WATER)` | CommandSpec `Enabled=true`；完整 Payload 走 Farm 入口 | `READY_WSS_PRIMARY`。 |
| 除虫 | `CARE(CARE_TYPE_PEST)` | CommandSpec `Enabled=true`；仅 `crop_cycle_id + incident_id` 完整时 validator 放行 | `READY_WSS_PRIMARY`；不完整旧形状 fail closed，不等于 PEST 未定义。 |
| 偷菜 | `THEFT` | CommandSpec `Enabled=true`，好友 ActionAdmission 后进入 Workflow | `READY_WSS_PRIMARY`。 |

好友 HTTP 列表中的 `action_mask` 只是响应组装时的精确读视图，不是 ActionPermit，也不允许 H5 通过
HTTP 直接提交 WATER/PEST/THEFT。点击后的 WSS 命令继续由当前 Subscription、Pair Admission 和
FarmActor 最终裁决。

## 3. 当前快照的机械证据

### 3.1 H5

| 证据 | 文件位置 | 机械事实 |
|---|---|---|
| HTTP 公共请求器 | H5 `src/friends/game-friend-service.ts:26-40` | `fetch` 使用 `credentials:'include'`、JSON、`cache:'no-store'`。 |
| FriendList | 同文件 `:131-155` | 调用 `/api/v1/friends`，读取 cursor/page_size、roster revision 与 validated time。 |
| NextOpportunity | 同文件 `:158-181` | 调用 `/api/v1/friends/opportunity`，FOUND 必须含可操作的 READY 卡片。 |
| 分享预览/兑换 | 同文件 `:184-198` | 预览把 Token 放 query；兑换 POST `/api/v1/share/accept`，请求没有 `operation_id`。 |
| DeleteFriend | 同文件 `:200-207` | 调用 `/api/v1/friends/revoke`，H5 生成 UUID `operation_id`。 |
| CreateShare | 同文件 `:214-225` | POST `/api/v1/share/create`，body 为 `{}`；生成 URL 时把 Token 放 query。 |
| Hero Social E2E | H5 `src/test/e2e/cp-social-navigation.spec.ts:49-90` | 创建分享、B/C 接受、列表显示 2 人、连续访问不同好友，第三次明确 NONE。 |

H5 白名单内没有 Block/Unblock 方法、请求或 E2E 操作。

### 3.2 Backend HTTP 与领域实现

| 证据 | 文件位置 | 机械事实 |
|---|---|---|
| HTTP route switch | `internal/gateway/auth/share.go:332-348` | 只注册 create/resolve/accept/list/opportunity/revoke；没有 block/unblock。 |
| HTTP 身份 | 同文件 `:351-366` | 从 `farm_session` Cookie 经 `AuthenticateHTTPSession` 得到 `SessionBinding`。 |
| Create | 同文件 `:369-388` | POST，身份来自 Cookie，返回 `share_token + MULTI_ACCEPT`。 |
| Redeem | 同文件 `:417-541` | POST，Token 解析后调用 Authority Redeem，再更新兼容 Store/投影。 |
| FriendList | 同文件 `:598-665` | GET，正式 ReadAPI 路径组装 Pair 校验后的卡片和精确机会字段；另保留兼容 fallback。 |
| NextOpportunity | 同文件 `:667-745` | GET，服务端 visited、分页扫描、明确 FOUND/NONE；未知依赖返回 503。 |
| Delete | 同文件 `:766-845` | POST，调用 `DomainService.DeleteFriend`，再更新兼容 Store 和通知。 |
| Block/Unblock 领域能力 | `internal/friend/service.go:272-294` | Pair Authority 方法存在，但没有公网 HTTP Handler。 |
| 好友精确读 | `internal/friend/read.go:72-177` | FriendIndex 候选必须再过 Pair、Profile 和 Farm opportunity 批量校验。 |

### 3.3 WSS Catalog/Proto

`internal/gateway/commandcatalog/catalog.go` 的静态事实：

- `JOIN_FARM` 在 `:26-49` enabled；
- `GET_FRIEND_LIST` 与 `GET_NEXT_FRIEND_OPPORTUNITY` 由 `playerServiceQuery` 构造，而该 helper 在
  `:363-390` 固定 `Enabled=false`；
- share token 三命令、`REDEEM_SHARE`、三条好友关系写在 `:247-282` 登记；对应 helper 在
  `:464-519` 固定 `Enabled=false`；
- `CARE` 与 `THEFT` 在 `:309-360` enabled。

`api/proto/public/commands.proto` 证明这些社交 WSS Typed Payload/Result 已分配消息和字段，但“消息存在”
不等于 Handler enabled。`internal/gateway/commandcatalog/decoders.go` 同样只证明 Decoder 存在。

## 4. HTTPS 公共信封、认证与可靠性

### 4.1 认证唯一来源

除第 8 节列为 deprecated 的匿名分享预览外，七项能力全部使用浏览器自动携带的签名 HttpOnly
`farm_session` Cookie：

```text
HTTPS request
→ Cookie farm_session
→ HTTPIdentityResolver.AuthenticateHTTPSession
→ SessionBinding.player_id
→ subject/viewer/caller
```

冻结规则：

1. `player_id`、`viewer_id`、`caller_id`、`inviter_player_id` 不允许由 JSON body/query 自报；
2. H5 的本地 `viewerId` 只可用于 UI/缓存关联，不是服务端授权输入；
3. Cookie 必须延续第 26 号的 HttpOnly/Secure/SameSite/authority epoch 规则；
4. 未认证固定 `HTTP 401 + ERR_UNAUTHENTICATED + REAUTHENTICATE`；
5. Redeem 的 Token 只证明邀请来源，不替代 Cookie、Pair 校验、VisitPermit 或 ActionPermit；
6. HTTP 社交请求不要求存在当前 Farm Subscription。Redeem 是否要求“Recovery complete”目前无法由
   可见 HTTP Handler 证明，列为 I1 阻塞，不允许客户端伪造 recovery 标志补洞。

### 4.2 JSON 与 ID 表示

请求/响应必须是 UTF-8 JSON，`Content-Type: application/json`。缓存规则固定为 `no-store`；响应不得被
共享缓存保存。

所有 `uint64` 实体 ID、epoch、revision 在 JSON 中使用十进制字符串：

```json
{
  "peer_player_id": "18446744073709551615",
  "relation_epoch": "7",
  "roster_revision": "19"
}
```

H5 不得先转换为 JavaScript `Number`。`avatar_id`、`level`、`action_mask`、`page_size` 等已受明确小范围
约束的 `uint32` 可以使用 JSON number。

`operation_id` 是不透明 ASCII string，长度 `1..64` 字节；只做相等比较。Token、cursor、relation_id、
crop_cycle_id 同样是不透明值，不按字典序或数值推断新旧。

### 4.3 `operation_id` 与幂等身份

V1 目标合同对所有社交写要求客户端在第一次发送前生成并保留 `operation_id`：

| 能力 | 是否要求 | Operation Root | Canonical Payload 最小材料 |
|---|---:|---|---|
| CreateShare | 是 | `FRIEND_SHARE_ISSUER:subject_player_id` | command + 空业务参数；随机 Token 候选是服务端材料，不进入客户端意图 |
| RedeemShare | 是 | `FRIEND_PAIR:canonical_pair(subject, token_inviter)` | command + Token 的服务端 digest/代次 + inviter；日志/Receipt 不保存原始 Bearer Token |
| DeleteFriend | 是 | `FRIEND_PAIR:canonical_pair(subject, peer)` | command + peer_player_id |
| Block | 是 | `FRIEND_PAIR:canonical_pair(subject, peer)` | command + peer_player_id |
| Unblock | 是 | `FRIEND_PAIR:canonical_pair(subject, peer)` | command + peer_player_id |
| FriendList | 否 | 无 | 安全查询，直接重查 |
| NextOpportunity | 否 | 无 | 安全查询，服务端拥有 cursor/visited |

有效幂等键固定为：

```text
subject_player_id + logical_operation + operation_id + OperationRootRef
```

相同键和相同 Canonical Payload 返回原终态；相同键但不同 peer/Token digest 返回
`ERR_IDEMPOTENCY_CONFLICT`。网络重试必须保留 `operation_id`，不得用新的 UUID 猜测重做。

当前兼容差距必须显式承认：

- Create H5 发送 `{}`；Backend 以随机 candidate 派生内部 share operation；
- Redeem H5 只发送 `share_token`；Backend 用 Token/Pair 派生 `MutationIdentity`；
- Delete H5 发送 UUID，但 Backend 再将它与 kind/Pair/epoch Hash 成另一内部 operation ID，未原样贯穿
  `DomainService`；
- 因此当前 Create/Redeem/Delete 不得宣称已经满足“客户端 operation_id 原样端到端恢复”。

兼容期允许 Backend 继续接受上述旧请求，但新增实现必须：

1. 接受并原样回传客户端 `operation_id`；
2. 以客户端 ID 形成公共 `CommitIdentity`，内部 participant ID 另行派生；
3. 成功和错误响应都回传 `operation_id`（查询除外）；
4. 不把 HTTP request ID、Token、relation epoch 或随机 candidate 冒充客户端业务意图；
5. 在支持可靠 Operation lookup 前，不对 post-accept unknown 指示自动创建新 operation。

### 4.4 成功与错误信封

为兼容当前 H5，V1 使用扁平 JSON，不把 `error` 从 string 改成嵌套 object。新增字段只能 additive：

成功响应公共字段：

```json
{
  "status": "SUCCEEDED",
  "operation_id": "client-op-id",
  "retry_directive": "NONE"
}
```

- 查询可省略 `operation_id`；
- `status` 只使用 `SUCCEEDED | NO_CHANGE | PROCESSING`；当前同步 HTTP 主线在没有可恢复 locator 前
  不得伪造 `PROCESSING`；
- `NO_CHANGE` 可 additive 携带稳定 `reason`，但 HTTP 仍为 200；
- 业务字段继续位于顶层，当前 H5 可以忽略新增公共字段。

错误响应固定为：

```json
{
  "error": "human readable, non-secret message",
  "error_code": "ERR_UNAUTHENTICATED",
  "retry_directive": "REAUTHENTICATE",
  "retry_after_ms": 0,
  "operation_id": "client-op-id"
}
```

`retry_after_ms` 只在非零且 directive 允许重试/轮询时出现。响应、日志和 Trace 均不得包含 Cookie、原始
ShareToken 或完整分享 URL。

### 4.5 HTTP、错误码与 Retry directive 映射

以下映射是社交 HTTPS 的唯一 H5 动作表；页面不得再按文案猜动作：

| HTTP | `error_code` / 场景 | `retry_directive` | H5 动作 |
|---:|---|---|---|
| 200 | `SUCCEEDED` / `NO_CHANGE` | `NONE` | 应用结果；写操作结束 pending record。 |
| 400 | `ERR_INVALID_ARGUMENT` | `NONE` | 不自动重试；记录脱敏诊断。 |
| 401 | `ERR_UNAUTHENTICATED` | `REAUTHENTICATE` | 走第 26 号登录/换票流程；不在未认证状态重放写。 |
| 404 | `ERR_SHARE_LINK_INVALID` | `NONE` | 清除短期 Token，展示链接无效/过期；不泄露具体 Token 状态。 |
| 404 | `ERR_NOT_FRIEND`（仅当前 legacy 兼容） | `NONE` | 刷新名单；Delete 不重放。目标 Pair 语义改为 200 `NO_CHANGE / NOT_FRIENDS`。 |
| 409 | `ERR_FRIEND_LIMIT_REACHED` | `NONE` | 展示上限，不自动重试。 |
| 409 | `ERR_FRIEND_PERMIT_INVALID` / `ERR_BLOCKED` | `NONE` | 刷新名单/关系，不自动重试。 |
| 409 | `ERR_IDEMPOTENCY_CONFLICT` | `NONE` | Fail Closed；不得换 Payload 继续用原 ID。 |
| 409 | `ERR_FRIEND_ROSTER_CHANGED` | `REFRESH_AUTHORITY_STATE` | 丢弃旧 cursor，从第一页重查。 |
| 409 | `ERR_RELATION_REACTIVATION_PENDING` | `REFRESH_AUTHORITY_STATE` | 不重放旧 Token；等待新分享链。 |
| 429 | `ERR_RATE_LIMITED` 查询 | `REFRESH_AUTHORITY_STATE` | 按 `retry_after_ms` 重查。 |
| 429 | `ERR_RATE_LIMITED` 写且服务端证明未接受 | `RETRY_SAME_OPERATION` | 同 operation_id 有界重试。 |
| 503 | `FRIEND_OPPORTUNITY_TEMPORARILY_UNAVAILABLE` | `REFRESH_AUTHORITY_STATE` | 保持 Loading/Unavailable，按退避重查；不得转成 NONE。 |
| 503 | 写操作，服务端证明未跨 Execution-Accept | `RETRY_SAME_OPERATION` | 同 operation_id 有界重试。 |
| 503/504 | 写操作 post-accept unknown 且已提供可靠 locator | `POLL_OPERATION_RESULT` | 只查询原 operation；不直接重做。 |
| 500/503/504 | 无法证明接受状态且没有可靠 locator（当前兼容缺口） | `REFRESH_AUTHORITY_STATE` | 禁止自动重放；刷新关系/分享状态并提示用户。 |

当前 Handler 的多条 400/404/405/503 只返回文案或不完整 `error_code`，且未返回 Retry directive；这属于
I1 阻塞。迁移前 H5 必须采取最后一行的保守动作，不能把未知 503 当作“换 operation_id 再试”。

## 5. 七项 HTTPS 业务合同

### 5.1 CreateShare

```http
POST /api/v1/share/create
Cookie: farm_session=...
Content-Type: application/json

{"operation_id":"<1..64 ASCII>"}
```

成功业务字段：

```json
{
  "status": "SUCCEEDED",
  "operation_id": "...",
  "retry_directive": "NONE",
  "share_token": "<opaque bearer>",
  "accept_mode": "MULTI_ACCEPT"
}
```

冻结语义：

- subject/inviter 只来自 Cookie；
- 当前有效 Token 存在时返回同一 Token 和 `NO_CHANGE`，普通重复点击不得主动轮换；
- 同一 Token 可被多个不同已认证玩家兑换；
- `share_token` 是 Bearer secret，不进入日志、Trace、Metric、错误和截图上报；
- 分享 URL 使用 Fragment 或短期 landing code，不允许继续新生成 query 形式；
- TTL 只由 `share-token-ttl-v1` 决定，默认 `7d`；兑换边界由 Redis Authority 的可信服务端时间裁决，
  `now_ms == expires_at_ms` 返回 `SHARE_TOKEN_EXPIRED`。显式环境值必须同时绑定 profile 且一致，否则
  farm-gate 启动失败。

### 5.2 RedeemShare

现行 V1 路径名保持 `/accept`，逻辑能力名和幂等 CommandType 统一叫 RedeemShare/`REDEEM_SHARE`：

```http
POST /api/v1/share/accept
Cookie: farm_session=...
Content-Type: application/json

{"share_token":"<opaque bearer>","operation_id":"<1..64 ASCII>"}
```

成功业务字段至少保持当前兼容字段，并 additive 补齐稳定身份：

```json
{
  "status": "SUCCEEDED",
  "operation_id": "...",
  "retry_directive": "NONE",
  "target_owner_id": "123",
  "inviter_label": "...",
  "avatar_id": 1,
  "level": 8,
  "relation_id": "...",
  "relation_epoch": "4",
  "changed": true
}
```

冻结语义：

- Redeem 只建立或确认 FriendRelation；进入邀请者农场必须另发 WSS `JOIN_FARM`；
- 已是好友返回 `NO_CHANGE`，不能重复占名额；
- Token 失效的公开错误统一为 `ERR_SHARE_LINK_INVALID`，服务端可在安全审计中区分 expired/revoked/stale；
- 任一方向拉黑、删除后重激活屏障、双方好友上限继续服从第 33 号；
- 收到终态后立即清除原始 Token；若未来返回 `PROCESSING + locator`，H5 只短期保留 Token 到 locator
  落盘，随后清除；
- Redeem 成功后的 Join 失败不回滚好友关系。

### 5.3 FriendList

```http
GET /api/v1/friends?cursor=<opaque>&page_size=<1..50>
Cookie: farm_session=...
```

响应保持现行顶层结构：

```json
{
  "friends": [],
  "next_cursor": "",
  "roster_revision": "19",
  "validated_at_ms": 1786200000000
}
```

每个 friend card 至少包含当前 H5 实际验证的：

```text
owner_id/display_name/avatar_id/level
relation_id/relation_epoch
action_mask/opportunity_status/opportunity_revision
theft_eligible_targets/validated_at_ms
```

冻结语义：

- viewer 只来自 Cookie；cursor 绑定 viewer 和 roster revision；
- 默认页长 20、上限 50；错误页长返回 `ERR_INVALID_ARGUMENT`，不能静默放大；
- FriendIndex 只是候选，返回前必须由 Pair Authority 验证并做精确机会读取；
- 单卡未知保持 `opportunity_status=UNKNOWN`，不能伪造成 action mask 0 的明确无机会；
- roster revision 变化返回 `ERR_FRIEND_ROSTER_CHANGED`，H5 从第一页重查；
- 整体依赖失败不得返回 `friends:[]` 冒充空名单。

### 5.4 DeleteFriend

V1 保持已用路径和旧字段名，避免在本合同批次发明第二条可调用入口：

```http
POST /api/v1/friends/revoke
Cookie: farm_session=...
Content-Type: application/json

{"friend_owner_id":"123","operation_id":"<1..64 ASCII>"}
```

`friend_owner_id` 是 HTTP V1 对逻辑 `peer_player_id` 的兼容别名；服务端内部只允许一种规范化字段。若未来
发布更整齐的 path/field，只能在版本化迁移后替换，不得让 H5 长期随机使用两个别名。

成功保持：

```json
{
  "status": "SUCCEEDED",
  "operation_id": "...",
  "retry_directive": "NONE",
  "removed": true,
  "relation_epoch": "5"
}
```

冻结语义：

- Pair Authority 提交是权限撤销和公开成功点；不等待 quota、Hub、FriendIndex 全部清理；
- 已非好友的目标合同为 `HTTP 200 + NO_CHANGE + reason=NOT_FRIENDS`，不得重复释放名额；当前 legacy
  预读返回的 `404 ERR_NOT_FRIEND` 只在兼容窗口保留；
- 当前 Handler 还依赖 legacy `relationships.Get/Mutate` 和 canonical Domain 双写。本批不迁移它，但后续
  必须保证 canonical Pair 成功不因投影/通知失败被误报成“业务未发生”；
- H5 收到成功后可本地移除卡片，再以 FriendList 权威刷新；本地 Set 不是授权来源。

### 5.5 Block

当前状态为 `DEFERRED_NOT_EXPOSED`。没有分配 HTTP path，没有 H5 入口，`BLOCK_PLAYER` WSS Command
保持 disabled。第 33 号冻结的方向性 Block/关系终止语义只是未来领域合同，不是 F05 已实现声明。

### 5.6 Unblock

当前状态为 `DEFERRED_NOT_EXPOSED`。没有分配 HTTP path，没有 H5 入口，`UNBLOCK_PLAYER` WSS Command
保持 disabled。未来解除拉黑仍不得自动恢复 FriendRelation，但这不是当前产品能力。

### 5.7 NextOpportunity

按现行链路冻结：

```http
GET /api/v1/friends/opportunity
Cookie: farm_session=...
```

H5 不上传 cursor、candidate window、visited IDs 或目标好友。成功只允许：

```json
{"status":"FOUND","friend":{"owner_id":"123"}}
```

或：

```json
{"status":"NONE"}
```

冻结语义：

- FOUND 必须含经过当前 Pair 和 FarmRead 精确校验、`opportunity_status=READY` 且 action 可执行的卡片；
- NONE 只在服务端完成本轮明确扫描时返回；
- 任一关键依赖未知返回 503 `FRIEND_OPPORTUNITY_TEMPORARILY_UNAVAILABLE`，不得返回 200 UNKNOWN/NONE；
- 当前 H5 类型虽能表示 `UNKNOWN`，Backend 正式路径使用可识别错误表达整体未知；不得同时维护两种
  “整体未知”主语义；
- 好友农场 WSS `SUBSCRIPTION_READY` 后由服务端最佳努力标记 visited；没有客户端导航 Ack；
- NextOpportunity 不替代 `CONFIRM_FRIEND_VISIT` 任务命令，也不执行 Join/Care/Theft。

## 6. HTTPS/WSS 双入口冲突与废弃登记

### 6.1 七项能力的 WSS Command

下列 Command 在 Proto/Decoder/Catalog 中存在，但当前 `Availability.Enabled=false`。本合同把它们冻结为
`DEPRECATED_FOR_PUBLIC_ENTRY`：不得接新 Handler，不得通过 feature flag 打开成第二条公网主线。

| 逻辑能力 | 旧 WSS Command | 当前状态 | 本合同处置 |
|---|---|---|---|
| CreateShare | `GENERATE_SHARE_TOKEN` | registered + decoder，disabled | 保持 fail closed；HTTPS 是唯一主入口。 |
| RedeemShare | `REDEEM_SHARE` | registered + decoder，disabled | 同上。 |
| FriendList | `GET_FRIEND_LIST` | registered + decoder，disabled | 同上。 |
| DeleteFriend | `DELETE_FRIEND` | registered + decoder，disabled | 同上。 |
| Block | `BLOCK_PLAYER` | registered + decoder，disabled | `DEFERRED_NOT_EXPOSED`；不启用 WSS。 |
| Unblock | `UNBLOCK_PLAYER` | registered + decoder，disabled | 同上。 |
| NextOpportunity | `GET_NEXT_FRIEND_OPPORTUNITY` | registered + decoder，disabled | 保持 HTTPS 现链路。 |

`ROTATE_SHARE_TOKEN`、`REVOKE_SHARE_TOKEN` 也保持 disabled；它们不在本次七项产品化判定内，不因本文获得
READY，也不得绕过后续单独的公网入口审计。

### 6.2 文档与实现冲突清单

| ID | 冲突 | 当前裁决 |
|---|---|---|
| `DUAL-01` | 第 28/40 号把社交能力列入 33 类 WSS Command；H5/HTTP Handler 已形成真实 HTTPS 主链，WSS Catalog 对应项 disabled。 | 七项通道以本文为窄域权威；旧 WSS Command deprecated/fail closed。 |
| `DUAL-02` | 第 26 号 `HC-ACCESS-01` 把 HTTPS 描述为只负责 Auth/Cookie/Ticket；第 40 号允许低频接口，当前实现已有社交 HTTPS。 | `HC-ACCESS-01` 对本合同七项作窄域例外：HTTPS 还承载这里列出的低频社交 JSON。 |
| `DUAL-03` | H5 Create/Redeem 未发客户端 operation_id，Delete 的 ID 又被 Backend 转换。 | 兼容期可继续调用，但不满足目标可靠性；按第 9 节迁移。 |
| `DUAL-04` | H5 把 ShareToken 放 URL query，`GET /share/resolve?token=` 也把 Bearer 放 query；第 33 号优先 Fragment/landing code。 | query 形式和 resolve Handler deprecated；安全迁移后按删除门移除。 |
| `DUAL-05` | 当前 HTTP 错误不总有稳定 code/directive，H5 只读取 HTTP status + error 文案。 | additive 补齐第 4.4/4.5 节；迁移前未知写失败禁止自动重放。 |
| `DUAL-06` | 历史 gatewayapp 内部 fallback 为 200，而 Authority 候选为 300。 | `CLOSED_I0`：canonical farm-gate 入口先绑定 `friend-limit-v1=300`；HTTP fallback、Authority 与压测从同一嵌入式 profile 取值。租约文件中的历史 fallback 不可从 canonical 进程入口到达。 |
| `DUAL-07` | 历史 gatewayapp 内部 fallback 为 24h，而 Authority 候选为 7d。 | `CLOSED_I0`：gatewayapp 默认值与 canonical 入口均直接读取 `socialprofile.Current()` 的 `share-token-ttl-v1=7d`；显式 override 不绑定/不一致即启动失败；Authority 兑换继续使用 Redis TIME。 |
| `DUAL-08` | Redeem/Delete 在 canonical Authority 成功后仍可能因投影/legacy 阶段失败返回 503。 | 不回滚 canonical 事实；在可靠 lookup/稳定 outcome 接线前，H5 只刷新权威状态，不创建新意图。 |
| `DUAL-09` | NextOpportunity H5 union 有 200 `UNKNOWN`，Backend 正式路径对整体 unknown 返回 503。 | 公网主语义固定 503 暂时不可用；卡片级 unknown 只属于 FriendList。 |

### 6.3 Handler 废弃登记

| Handler/路径 | 状态 | 删除前提 |
|---|---|---|
| `GET /api/v1/share/resolve?token=` / `handleResolve` | `DEPRECATED_SECURITY_COMPAT` | H5 不再需要 query Token 预览；Fragment/landing code 已上线；Token 日志泄露探针通过；第 9.3 节门全部满足。 |
| 当前 create/accept/list/revoke/opportunity Handler | `V1_PRIMARY_COMPAT` | 本合同不授权删除或改名。 |
| Block/Unblock Handler | `DEFERRED_NOT_EXPOSED / ABSENT` | 当前不进入删除或发布流程；未来重新立项后才定义公网 route。 |
| 七项 WSS business Handler | `ABSENT/DISABLED` | 不新增；Catalog/Proto 按第 7 节保留兼容形状。 |

## 7. Proto append-only、reserved 与 deprecated 策略

本批不改 Proto。后续 Proto owner 必须一次性执行以下规则：

1. 已发布的 `CommandType` 数值永不改号、永不复用；HTTPS 成为主入口也不能把旧数值分配给新命令；
2. 七项旧 WSS Command enum 在拥有该 enum 的唯一 Proto 中标记 `deprecated=true`，但只有在最小支持
   协议版本和滚动兼容审计通过后才能从 Catalog 完整性集合移出；
3. `api/proto/public/commands.proto` 现有 Request/Result 消息不删除、不改名、不改字段语义；
4. 新字段只能追加。`FriendEntryView` 已保留 `2..20`，只能从现有最高号后追加；
   `GetFriendListResult` 已保留 `1..50`，只能从 55 后追加；Share/Relation Result 已保留 `1..50`，
   只能从 51 后追加；
5. 删除已发布字段时同时 `reserved <number>` 和 `reserved "<name>"`；不得 reserve 尚未发布的猜测字段；
6. deprecated 只表示“不再用于新主入口”，不授权在兼容窗口丢弃 unknown fields、拒绝旧帧或回收编号；
7. JSON 与 Proto 是两个明确编码合同。不得为了复用 Proto field number 把 HTTPS JSON 偷运进
   `bytes payload`，也不得让 HTTP Handler 调用 WSS Command Handler 形成第二条公网入口；
8. 未来若保留旧 Decoder 仅用于明确拒绝，拒绝码固定 `ERR_COMMAND_UNIMPLEMENTED`，不得伪造成功。

## 8. H5 迁移合同

### 8.1 必做顺序

迁移按可回滚顺序进行，不允许先删 Backend 兼容再发布 H5：

1. Backend additive：现有五条 HTTP 路径先接受目标 `operation_id`、补齐稳定成功/错误公共字段和
   观测；旧 body/字段继续可用；
2. Backend 安全入口：提供不把原始 Token 放 query 的 Fragment/landing code 流程；旧 resolve 仅兼容；
3. H5 写身份：Create/Redeem 在发送前创建 operation_id；Delete 保留现有 UUID，并验证响应原样回传；
4. H5 Token：Create 用 Fragment/landing code 生成分享 URL；启动后立即移除地址栏 Token；Redeem 终态
   或 locator 建立后清除，不写长期 localStorage/IndexedDB/日志；
5. H5 错误：统一消费 `error_code + retry_directive + retry_after_ms`；页面文案不参与恢复决策；
6. H5 查询：FriendList/NextOpportunity 继续 HTTPS，不迁到 WSS；卡片 UNKNOWN 与整体 503 分开呈现；
7. H5 Block/Unblock：当前 deferred，不增加 UI；未来重新立项时必须先有 Backend 合同测试和产品 E2E；
8. 最后才关闭 query Token、缺失 operation_id、旧错误形状等兼容分支。

### 8.2 写操作 Journal

在 HTTP operation lookup 尚未落地前，H5 至少保存内存/短期 pending record：

```text
logical_operation
operation_id
peer_player_id? / token_digest?
created_at_ms
last_retry_directive
```

原始 ShareToken 是例外：只放当前标签页短生命周期容器，不能为了通用 Journal 落长期存储。Delete/Block/
Unblock 可持久保存不含秘密的 peer 和 operation ID。收到未知 5xx 且无 locator 时，pending 状态进入
`REFRESH_AUTHORITY_STATE_REQUIRED`，不得自动换 ID 重发。

### 8.3 不迁移项

- JOIN、WATER、PEST、THEFT 继续走现有 WSS command runner；
- Friend action_mask 仍只是展示/候选，不缓存成 Permit；
- Redeem 后进入邀请者农场仍走新的 WSS Join operation/attempt；
- 本批禁止 H5 暴露 Block/Unblock 页面。

## 9. 兼容窗口与删除门

### 9.1 窗口

推荐冻结的最小兼容窗口为：

```text
从“Backend additive 版本全量 + 首个新 H5 生产版本发布”两者较晚者开始
同时满足 >= 30 个自然日
且跨越 >= 2 个 H5 生产版本
```

I0 未确认前该值不能被缩短。安全漏洞可以紧急关闭入口，但必须由安全 owner 留下例外记录和回滚/用户
影响方案，不能由实现者自行跳过门。

### 9.2 兼容项

窗口内 Backend 必须同时接受：

- Create `{}` 与 `{operation_id}`；
- Redeem `{share_token}` 与 `{share_token,operation_id}`；
- 当前 Delete `friend_owner_id`；
- 当前 H5 只读取 `error` string 的行为，以及 additive 新错误字段。

兼容不表示两套业务 Authority：两种请求在规范化后必须进入同一 Root/Handler。旧请求生成的 server-side
兼容身份必须可观测，并且不能返回需要客户端 lookup 的 `PROCESSING`。

### 9.3 删除门

删除任一 legacy Command/Handler/字段前必须全部满足：

| Gate | 必须证据 |
|---|---|
| `G1_BACKEND` | 新旧请求合同测试、幂等重放/冲突测试、错误/Retry 映射测试全部通过。 |
| `G2_H5` | 至少两个生产 H5 版本已发送新形状；支持矩阵中没有只能发送旧形状的 build。 |
| `G3_TRAFFIC` | 连续 14 天，受支持 build 的 legacy 请求计数为 0；unknown build 单独审计，不被静默忽略。 |
| `G4_RECOVERY` | post-accept unknown 有可靠 outcome/locator，或证明所有可返回重试错误都发生在接受边界前。 |
| `G5_SECURITY` | URL/Referer/access log/error/telemetry 中原始 Token 探针为 0；脱敏测试通过。 |
| `G6_PRODUCT` | Block/Unblock 当前 deferred；未来重新立项发布时，H5 入口、方向语义、NO_CHANGE、删除关系和不自动恢复好友 E2E 通过。 |
| `G7_PROTO` | 最小协议版本、滚动部署、deprecated enum、reserved/append-only 兼容检查通过。 |
| `G8_ROLLBACK` | 回滚到前一 Backend/H5 版本不会重新产生双入口或不可读取结果。 |
| `G9_OWNER` | I0 表对应项有产品/API/安全 owner 明确确认。 |

任何一门失败都只能延长兼容，不得删除。

## 10. 后续迁移阻塞项

| ID | 阻塞项 | 影响能力 | 解除证据 |
|---|---|---|---|
| `I1-01` | Create/Redeem 客户端 operation_id 未接线；Delete 未原样贯穿 | 三项写 | H5+Backend 定向合同测试证明同 ID 同 Payload 重放、换 Payload 冲突。 |
| `I1-02` | HTTP success/error 未稳定携带 status/error_code/retry_directive | 五项已用 HTTP | JSON golden tests + H5 directive tests。 |
| `I1-03` | post-accept unknown 没有已验证 HTTP operation lookup/locator | Redeem/Delete/Create | 故障注入证明不重复改变关系/Token，H5 不创建新意图。 |
| `I1-04` | Token 仍在分享 URL query 和 resolve query | Create/Redeem | Fragment/landing code E2E；访问日志/Referer/遥测零泄露。 |
| `I1-05` | Block/Unblock 没有 HTTP Handler/H5 产品入口 | Block/Unblock | `DEFERRED_NOT_EXPOSED`；不阻塞当前 Showcase R1，未来重新立项时另行定义解除证据。 |
| `I1-06` | 历史好友上限默认冲突 | Redeem | `CLOSED_I0`：`friend-limit-v1=300`、单一机器源、299/300/301、双侧并发及 grandfathered overflow 测试。 |
| `I1-07` | 历史 ShareToken TTL 默认冲突 | Create/Redeem | `CLOSED_I0`：`share-token-ttl-v1=7d`、可信时间边界、复用/Rotate/Revoke/关系隔离测试。 |
| `I1-08` | Redeem HTTP 只证明 Cookie 身份，未证明第 33 号 Recovery-complete 准入 | Redeem | 受信服务端恢复门或明确批准移除该前置；禁止客户端自报。 |
| `I1-09` | canonical Authority 与 legacy Store/投影阶段可能造成已提交后 503 | Redeem/Delete | fault tests + stable outcome；本批不迁 Domain、不删 legacy。 |
| `I1-10` | 旧 WSS social Command 仍在 33-count Catalog/Proto 合同中 | 七项 | Proto/Catalog owner 按第 7/9 节完成 deprecated 和版本门；当前保持 disabled。 |
| `I1-11` | 当前 Hero Social E2E 不覆盖 Delete、错误和重试 | 当前写能力 | 定向 E2E 补齐并在目标环境留 raw result；deferred Block/Unblock 不纳入当前 Gate。 |
| `I1-12` | 白名单不含生产路由装配与配置，无法证明当前部署确实注入正式 ReadAPI/Authority/TTL/limit | 全部 | 独立验证记录给出 repo/branch/commit/config/test/raw result；不反写长期合同。 |

## 11. I0 用户确认表

I0 已由用户最终确认。`CONFIRMED_TARGET / I1_OPEN` 表示目标合同已确认但迁移仍未完成，不能据此宣称
operation_id、Fragment 或其他后续缺口已经上线。

| I0 | 需要确认的冻结项 | 推荐答案 | 当前记录 |
|---|---|---|---|
| `I0-01` | 七项唯一判定 | 5 项 `READY_HTTP_PRIMARY`；Block/Unblock `DEFERRED_NOT_EXPOSED`；七项无 WSS primary | `CONFIRMED` |
| `I0-02` | V1 是否保留现行 create/accept/friends/revoke/opportunity URL，避免本批发明重命名入口 | `YES`；Block/Unblock 当前不分配 HTTP route | `CONFIRMED` |
| `I0-03` | 写操作身份 | 客户端 operation_id 为公共业务身份；当前 server-derived 只作限时兼容 | `CONFIRMED_TARGET / I1_OPEN` |
| `I0-04` | 旧 WSS social Command | 保持 disabled，标为 public-entry deprecated；永不复用 enum/field number | `CONFIRMED` |
| `I0-05` | JOIN/WATER/完整 PEST/THEFT | 保持 JOIN/CARE/THEFT WSS 主线，不纳入社交 HTTP 整齐化 | `CONFIRMED` |
| `I0-06` | 分享 Token URL | 迁 Fragment/landing code；query create/resolve deprecated | `CONFIRMED_TARGET / I1_OPEN` |
| `I0-07` | 兼容窗口 | 至少 30 日且跨至少 2 个 H5 生产版本，删除还需全部 Gate | `CONFIRMED_TARGET / I1_OPEN` |
| `I0-08` | 好友上限 | `FRIEND_LIMIT_PROFILE=friend-limit-v1`，effective max friends=`300` | `CONFIRMED / IMPLEMENTED` |
| `I0-09` | ShareToken TTL | `SHARE_TOKEN_TTL_PROFILE=share-token-ttl-v1`，default TTL=`7d`；可信服务端时间裁决兑换边界 | `CONFIRMED / IMPLEMENTED` |
| `I0-10` | Block/Unblock 产品范围 | `DEFERRED_NOT_EXPOSED`：H5 无入口、HTTP 无 route、WSS disabled，不阻塞当前 Showcase R1 | `CONFIRMED` |

## 12. 定向验证清单

本合同只允许使用下列白名单定向核验，不要求通读其他设计文档：

```bash
git rev-parse HEAD
git -C ../classic-farm-h5 rev-parse HEAD

rg -n '/api/v1/(share|friends)|operation_id|nextOpportunity|revoke' \
  ../classic-farm-h5/src/friends \
  ../classic-farm-h5/src/test/e2e/cp-social-navigation.spec.ts \
  internal/gateway/auth/share.go

rg -n 'GENERATE_SHARE_TOKEN|REDEEM_SHARE|GET_FRIEND_LIST|DELETE_FRIEND|BLOCK_PLAYER|UNBLOCK_PLAYER|GET_NEXT_FRIEND_OPPORTUNITY|JOIN_FARM|CARE|THEFT' \
  api/proto/public/commands.proto internal/gateway/commandcatalog internal/friend

go test ./internal/gateway/commandcatalog ./internal/friend
```

验收时至少机械断言：

1. 七项结论恰好各出现一次；
2. 五条现行 H5 HTTP 路径与 Backend route switch 一致；
3. Block/Unblock 在 route switch/H5 白名单内不存在；
4. 七条旧 WSS social spec 仍 disabled，不能因 Decoder 存在误判 READY；
5. JOIN/CARE/THEFT 仍 enabled；完整 PEST 通过 validator，不完整 PEST fail closed；
6. H5 Hero Social E2E 的分享、列表、A→B→C、NONE 证据仍成立；
7. Proto 与 H5 无业务变更；Backend 参数实现不触碰 R1-4 lease 写集。

## 13. 最终冻结摘要

```text
CreateShare       → HTTPS JSON primary
RedeemShare       → HTTPS JSON primary
FriendList        → HTTPS JSON primary
DeleteFriend      → HTTPS JSON primary
Block             → DEFERRED_NOT_EXPOSED（H5 none / HTTP none / WSS disabled）
Unblock           → DEFERRED_NOT_EXPOSED（H5 none / HTTP none / WSS disabled）
NextOpportunity   → HTTPS JSON primary（按现链路）

JOIN_FARM         → WSS Protobuf primary
WATER             → WSS CARE primary
PEST              → WSS CARE primary（完整Incident字段；不完整fail closed）
THEFT             → WSS Protobuf primary
```

本合同的核心不是把注册表做得整齐，而是确保一个产品意图只有一个公网主入口、一个可信身份来源和一个
可恢复幂等身份。现有五条 HTTP 主链继续服务兼容流量；Block/Unblock deferred 且不得宣称 F05 已实现；
旧 WSS 社交 Command 保持 disabled/deprecated。I0 参数已经闭合，但 operation_id、Fragment 与兼容删除门
仍是 I1 缺口。
