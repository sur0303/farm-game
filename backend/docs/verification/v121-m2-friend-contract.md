# M2：分享、好友关系与 ActionPermit

版本：v1.21-m2  
日期：2026-07-29  
状态：代码、H5 回归与公司 Redis 实测通过

## 1. 原实现的问题

原分享和好友闭环能演示，但没有形成生产合同：

- 接受分享后会删除 Token，同一个分享不能被多个玩家接受；
- Token 已删除但好友关系写失败时，用户既没加上好友，也无法重试；
- Redis Lua 同时写 relation 和两个玩家索引，但 Key 不在同一 cluster
  hash slot；
- 好友关系只有字符串 `ACTIVE`，撤销时直接删除，无法识别撤销前签发的旧授权；
- 好友上限通过“先读两个列表、再写关系”实现，并发下会超限；
- 接受和撤销没有持久 Receipt，超时后无法区分未执行和已经执行；
- FarmOperationCoordinator 直接查询 `AreFriends`，没有绑定操作、动作和关系代次的授权。

## 2. 当前合同

### MULTI_ACCEPT 分享

分享 Token 在 TTL 内不因成功接受而删除：

```text
ShareToken
  inviter_player_id
  accept_mode = MULTI_ACCEPT
  created_at / expires_at
```

同一 Token 可以被多个不同登录账号接受；自邀请仍被拒绝。Token 仅提供邀请人
身份，不直接授予访问农场的权限。

### Canonical FriendRelation

每一对玩家只有一条无序 canonical 记录：

```text
(player_low_id, player_high_id)
status = ACTIVE | REVOKED
relation_epoch
revision
created_at / updated_at
```

撤销不删除记录，而是写 `REVOKED` tombstone 并递增 epoch；重新成为好友再次
递增 epoch。这样旧授权永远不能与新关系混淆。

Redis 当前将 relation、两个列表索引和 mutation receipt 放在统一
`{friend}` hash slot，由一个 Lua 原子完成：

- 双方好友上限检查；
- relation 创建、撤销或重新激活；
- 两个好友列表索引更新；
- Mutation Receipt 写入。

### FriendMutationReceipt

接受分享使用 `token + canonical pair` 派生稳定 operation id；撤销接受 H5
生成的 operation id。Receipt 保存：

- `operation_id`；
- `payload_hash`；
- mutation kind；
- 原始结果；
- 是否实际改变关系；
- 提交时间。

相同 ID、相同载荷返回原结果；相同 ID、不同载荷返回
`ERR_IDEMPOTENCY_CONFLICT`。撤销响应丢失后，即使客户端重新发起一个操作，
服务端也会收敛到 `removed=false`，而不是把已达成的目标当成失败。

### ActionPermit

Care/Water 和 Theft 不再只做布尔好友查询。`farm-svr /
FarmOperationCoordinator` 向 FriendshipDomain 申请短期 HMAC Permit：

```text
viewer_player_id
target_owner_id
relation_epoch
operation_id
action = CARE | THEFT
issued_at / expires_at
signature
```

进入 FarmActor mailbox 后，在修改 FarmRecord 前验证签名、Viewer、Target、
Operation、Action 和有效期。Permit 的签发会读取当前 ACTIVE relation；
撤销或重新加好友后 relation epoch 变化，旧 Permit 无法通过完整校验。

Gate 仍只做认证和首跳路由，不签发或推进好友业务授权。

### 好友列表首屏与在线变更通知

H5 在 `ServerHello` 后、首次 `JoinFarm` 前拉取一次 `/api/v1/friends`。加载中、
权威空列表和加载失败是三个不同 UI 状态；失败时提供明确重试，不再把错误伪装成
“暂无好友”。

好友 accept/revoke 的 Redis mutation 成功后，Gate 向双方本机在线连接的既有
`ConnectionWriter` 入队 `FRIEND_ROSTER_CHANGED`。通知只携带 counterpart、
relation epoch 和 mutation kind；H5 收到后重新拉取权威列表并显示短提示，通知
本身不作为好友权限事实。

因此删除了 5 秒好友列表轮询。断线重连和窗口重新获得焦点仍会拉权威列表，覆盖
通知丢失。当前 targeted push 是单 Gate 本地连接索引；多 Gate 生产部署必须由
SessionRoute/跨 Gate 事件总线先把失效事件投递到连接所属 Gate，不能宣称本地
registry 已解决跨实例通知。

## 3. 验证

服务端全量测试：

```text
go test ./...
PASS
```

H5 全量测试：

```text
npm test
13 files / 58 tests passed
```

真实 Staging 双浏览器验证：

```text
首次好友列表 data-state=ready
accept share 后邀请方无需轮询即收到 FRIEND_ROSTER_CHANGED
好友数量与列表刷新，并显示在线提示
4 条 Staging E2E / 31.8s PASS
```

公司持久 Redis 实测：

- canonical pair 只产生一条 relation；
- Mutation Receipt 可重放；
- 双方好友上限在 Lua 内原子检查；
- revoke 后 epoch `1 → 2`；
- re-friend 后 epoch `2 → 3`；
- 双索引与 relation 一致；
- 同一个 Redis Share Token 可由两个不同 Viewer 接受并再次解析；
- 测试均使用唯一临时前缀，结束后删除精确测试 Key。

## 4. 后续使用

M3 的 CareWorkflow 和 TheftWorkflow 复用本阶段的 ActionPermit、Relation
Epoch 和 Receipt。权限只决定操作是否可以进入 FarmActor；作物是否成熟、
事件是否存在、可偷份额和最终奖励仍由 FarmActor、PlayerActor 和持久
Workflow 分别裁决。
