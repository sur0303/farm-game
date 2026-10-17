# Actor Model Field V2：Schema 与迁移前置设计

状态：M0 preflight，仅观测与设计。冻结代码基线：`ba6f3aee4f0fcd941d9fa37135af0ce09ffdfa5c`。

本阶段没有切换生产 Redis layout，没有迁移记录、双读双写、删除 blob 或引入 SoftSave；没有修改 Lua
业务语义、Proto、Gateway、Friendship、Route 或 H5。下文的 v2 是后续门禁方案，不是本提交已经启用的行为。

## 1. 代码事实

当前 `authority_layout_version=v1` 的一个 Actor 是一个 Redis Hash。`_meta:schema_version=2` 是现有 Redis
Hash 结构版本，不是 Model Field v2。`_meta:model_manifest` 是现有字段名的 LP 清单；
`_meta:model_total_bytes` 和 `_meta:model_field_count` 由同一次 Lua Commit 校验。

当前模型是：

| Domain | `model:*` | 编码与角色 | 真实构建入口 |
|---|---|---|---|
| Player | `record` | deterministic protobuf `player.PlayerRecord`，权威聚合 | `buildPlayerAuthorityModelState` |
| Player | `public_profile` | 固定、确定性只读投影；不参与业务裁决 | `PublicProfileProjection` |
| Farm | `record` | deterministic protobuf `farm.FarmRecord`，权威聚合 | `buildFarmAuthorityModelState` |
| Farm | `opportunity_header` | 确定性只读投影 | `deriveFarmOpportunityModels` |
| Farm | `opportunity_facts` | 确定性只读投影 | `deriveFarmOpportunityModels` |

Player `record` 当前包含身份、经济版本、钱包、库存、等级经验、两类任务载体、图鉴条目、两类预留和
内嵌操作 Receipt。Farm `record` 当前包含 FarmCore、内嵌操作 Receipt、Settlement、TheftAttempt 和
CareAttempt。完整字段与逐命令写集合见
[`command-model-mutation-matrix.json`](command-model-mutation-matrix.json)。矩阵条目来自命名的 Go
mutation/model builder，不从产品设计反推。

现有 Actor Binding 对一次变化提交重建完整 `ModelState`；Redis Commit Lua 对清单里的每个 `model:*`
执行写入，并追加一条外层 Authority Receipt。因此当前命令写放大为：

- Player 变化提交写完整 `record`，同时重写 `public_profile`，即使投影字节没变；
- Farm 变化提交写完整 `record`、`opportunity_header`、`opportunity_facts`；
- Participant 的 REJECTED/NO_CHANGE 若要落内嵌持久 Receipt，仍会重写上述完整模型；
- CARE 成功还在相同 Authority Commit 中创建一条 `CARE_REWARD` SourceOrder；
- 一次业务命令可包含多个 Actor/Participant Commit，Store 接缝没有可靠的 BUY/SELL 等业务命令标签。

## 2. M0 观测口径

Player/Farm authority 模式注册以下 Prometheus Histogram。它们只消费 Commit 已有参数/结果和 Redis Hook
上下文，没有返回值，也没有任何代码读取指标做准入、容量或业务裁决。

| 指标 | 标签 | 实际口径 |
|---|---|---|
| `actor_record_serialized_bytes` | `domain` | 候选 `model:record` 的已序列化 value 长度 |
| `authority_commit_request_bytes` | `domain,command` | 现有 `AuthorityCommitSerializedBytes` 的存储无关逻辑 envelope |
| `authority_commit_response_bytes` | `domain` | CommitResult 的确定性逻辑 envelope；不含 Go/RESP 开销 |
| `receipt_bytes` | `domain` | Redis Store 可投影时为外层 Authority Receipt 的准确物理 LP bytes；否则为逻辑降级值 |
| `order_bytes` | `domain` | 每条 AttachedOrder 的逻辑 envelope；不冒充落库 Hash 总字节 |
| `model_state_bytes` | `domain` | 完整候选 ModelState 的 field-name + value bytes |
| `serialization_duration_ms` | `domain` | Commit 接缝做确定性 envelope 计数和物理 Receipt 投影所耗时间 |
| `redis_round_trips` | `domain` | 带该 Commit context 的实际 go-redis Process/Pipeline dispatch 次数 |

`serialization_duration_ms` 不是 Actor 在进入 Store 前完成的 protobuf marshal 时间；现有窄接缝无法在不侵入
Player/Farm 写路径的情况下安全取得该时间，因此明确降级为上述 sizing/projection duration。

`command` 只能是 Store 当前可得的有界稳定名 `AR1_RECORD_COMMIT`、`AR1_INITIAL_INSTALL` 或 `OTHER`。
它不能安全区分 BUY/SELL/PLANT 等业务命令，所以没有解析 payload 或伪造标签。所有未知值归一为 `OTHER`；
没有 `operation_id`、`player_id`、`actor_id` 等无界标签。业务级写放大按命令的关联需要 Controller 后续提供
有界、可信的 Actor turn observation context，或通过受控压测与矩阵离线关联。

## 3. 只读布局检查器

`cmd/actor-layout-inspect` 直接读取 v1 Redis 物理记录，不构造 Registry，不 Claim/激活 Actor，也没有
Commit、修复或迁移接口。除连接探测 `PING` 外，采样器接口只允许：

1. `SCAN` v1 Player/Farm Actor key；
2. 对选中 Actor `HGETALL`；
3. 对选中 Actor 所在 bucket 做有界 SourceOrder `SCAN`，再 `HGETALL`。

输出分别给出 `actor_record_serialized_bytes`、完整 Redis Actor Hash 的逻辑 bytes、ModelState bytes、每个
当前/实际发现的 model field、外层 operation/resolution Receipt，以及 SourceOrder Hash/typed payload 的
count/P50/P95/P99/max。逻辑 Hash bytes 是 field-name + value bytes，不含 Redis allocator、dict、RESP 或
网络 framing。

采样是 bounded SCAN 的首批 key，不宣称随机或生产代表性。每个选中 bucket 最多检查 10,000 个 order key；
达到上限、样本不足、单项 SCAN/HGETALL/解码失败都写入 `failures`，报告 `status=partial` 并返回 2。
完整请求返回 0；参数、连接或输出失败返回 1。Redis URL 从不进入报告，连接错误和逐项错误会再次清除
URL/password。工具不会把采样错误传播给 Player/Farm 服务，因为它是独立进程。

示例：

```bash
go run ./cmd/actor-layout-inspect \
  --redis-url "$ACTOR_INSPECT_REDIS_URL" \
  --namespace production:cn1 \
  --sample-players 1000 \
  --sample-farms 1000 \
  --out actor-layout-v1-production-sample.json
```

## 4. V2 目标 Schema

### 4.1 两个 version 不混用

- `_meta:authority_layout_version` 仍为 `v1`：ActorAddress 到 Redis Hash Tag/Shard 的映射不变；
- 新增 `_meta:model_layout_version`：值只允许 `1` 或 `2`，决定 Model codec 与唯一权威字段集合；
- 现有 `_meta:schema_version=2` 继续表示 Redis adapter 结构，不能拿来判断 model layout；
- WriterClaim、RecordVersion、ShardWriteFence 和 PersistenceLane 仍各只有一套。V2 不创建新 Actor、Store、
  版本序列或第二提交 Lane。

### 4.2 Manifest

现有 `_meta:model_manifest` 继续是 Lua 用于物理完整性/容量的全部 `model:*` 字段名清单。V2 另存
`_meta:model_schema_manifest`，格式固定为：

```text
"AMF2" || uint16_be(entry_count) ||
sorted repeated LP(field_name) || LP(codec_id) || byte(role)
```

`LP` 是 `uint32_be(length) || bytes`。`role` 只有：

- `1 = AUTHORITATIVE`：当前唯一裁决真值；
- `2 = DERIVED`：只能由 AUTHORITATIVE 字段重建，不能反向裁决；
- `3 = RETAINED_V1_INERT`：回滚证据，V2 reader 必须不可见。

编译期 schema 注册表为每个 domain/layout 固定 exact manifest。Load 时要求 stored manifest、字段集合和
codec id 与注册表完全相等；未知/缺失/重复字段一律 fail closed，不进行“尽量读取”。现有
`_meta:business_state_digest` 仍可按完整确定性 ModelState 计算，作为 CAS/Fence 证据；manifest role 才决定
业务可见性。不能通过把 RETAINED 字段纳入 digest 就让其成为第二真值。

### 4.3 Player V2 候选映射

| field | 从当前 PlayerRecord 搬运的逻辑内容 | 角色 |
|---|---|---|
| `profile` | `player_id/bootstrap_id/public_profile` | AUTHORITATIVE |
| `economy` | `economy_version/wallet/unlock_coin_reservations` | AUTHORITATIVE |
| `inventory` | `inventory/item_reservations` | AUTHORITATIVE |
| `progression` | `progression` | AUTHORITATIVE |
| `task` | `tasks/task_domain_state`，具体兼容去留待 Task Controller | AUTHORITATIVE，未过门禁 |
| `compendium` | `codex_entries` | AUTHORITATIVE |
| `operation_receipts` | `recent_operation_receipts` | AUTHORITATIVE |
| `public_profile` | 从 `profile + progression` 重建 | DERIVED |
| `record` | 转换前原始 v1 bytes，只保留、不更新 | RETAINED_V1_INERT |

这些是基于当前字段共变关系的逻辑边界，不是已经冻结的 protobuf。现有 Proto 没有为 `profile/economy/...`
提供全部独立 envelope；后续必须先冻结 codec 与 golden fixtures，不能在本 M0 用手写切片冒充 schema。
Pet 不加入 Player V2；当前 public Pet 命令 disabled，且 `PlayerRecord` 没有 PetState。MailEntry 使用独立
player-mail authority，不塞入 Actor。Mail/Pet 均 deferred。

### 4.4 Farm V2 候选映射

| field | 从当前 FarmRecord 搬运的逻辑内容 | 角色 |
|---|---|---|
| `farm_meta` | `farm_id/core.farm_id/core.farm_version` | AUTHORITATIVE |
| `plots` | `core.plots/core.crop_cycles` | AUTHORITATIVE |
| `operation_receipts` | `recent_operation_receipts`，含现有 Participant proof | AUTHORITATIVE |
| `settlements` | `active_settlements` | AUTHORITATIVE |
| `theft_attempts` | `recent_theft_attempts` | AUTHORITATIVE |
| `care_attempts` | `recent_care_attempts` | AUTHORITATIVE |
| `opportunity_header` | 从 active Farm fields/receipts 重建 | DERIVED |
| `opportunity_facts` | 从 active Farm fields/receipts 重建 | DERIVED |
| `record` | 转换前原始 v1 bytes，只保留、不更新 | RETAINED_V1_INERT |

Guard/Pet 的最终落点仍 deferred，本方案不新增 `guard` 字段；当前 Guard Snapshot proof 仍随
`operation_receipts` 保留。按 plot 分桶没有数据依据，也不进入 V2 首版。

## 5. 命令到稀疏 Model Mutation

后续 V2 Commit 仍是一次 Actor CAS，但只把矩阵声明的变化字段作为 bounded mutation set 传给同一 Lua
线性化点；Receipt、SourceOrder、RecordVersion、WriterClaim 校验仍原子完成。Actor/Binding 必须同时持有
不可变的完整候选 snapshot 来计算 digest、容量和安装成功后的内存状态，不能让 Store 局部写成为第二个
业务对象。

初步收益方向由真实 mutation 得出：BUY 不必重写 Farm 或不变的 Player compendium；普通 SELL 不必重写
task/compendium；Farm CARE 不必重写 Settlement；Participant REJECTED/NO_CHANGE 通常只写
`operation_receipts` 和外层 Receipt。PLANT/HARVEST/THEFT 仍会原子修改多个字段，并不因拆分而变成多条
事务。是否值得引入稀疏 Lua mutation，必须以生产检查器样本和上述 metrics 的 P95/P99 数据过门，不能用
本地初始 fixture 外推。

## 6. V1 → V2 状态机（后续实现）

### 6.1 读策略：支持两个版本，但每条记录只读一套

所谓 dual-read 是 binary 能读取 `model_layout_version=1` 或 `2` 的能力，不是在同一 Actor 上同时读两套：

- v1：只解码 `model:record`，现有 derived 字段按现规则校验/安装；
- v2：只解码 manifest 中 AUTHORITATIVE 字段，再重建/校验 DERIVED；`model:record` 永远不可见；
- 缺 version 的既有记录只可按 golden allowlist 识别为 v1；allowlist 必须覆盖代码当前明确支持的
  record-only legacy 和带当前 derived projection 两类，其他缺失/损坏不得 fallback 到另一版本；
- V2 读失败时隔离 Actor，禁止用 retained v1 blob“救活”并继续写。

因此任一 RecordVersion 只有一个裁决真值。

### 6.2 `write-v2` 开关

开关只控制“尚为 v1 的 Actor 是否可以转换”：

- off：v1 继续 v1；已经 v2 的 Actor 必须继续 v2，不能隐式降级；
- on：新 Actor 可直接安装 v2；已激活/加载的 v1 Actor在下一次本来就要发生的成功写中转换；
- 只读/no-change 且原本不落 Commit 的 Turn 不为迁移制造一次写；
- 不允许后台扫描 Claim Actor，不允许工具触发转换。

开关必须按 domain + deterministic actor cohort 配置，cohort 只决定资格，不进入持久真值或指标高基数标签。

### 6.3 旧 blob 的原子转换

转换发生在正常 WriterClaim 下、相同 PersistenceLane 的下一次业务 Commit：

1. 从已验证的 v1 snapshot 保留原始 `model:record` bytes；
2. 业务 mutation 仍先产生一份完整候选 PlayerRecord/FarmRecord；
3. 确定性 converter 把候选拆成 V2 AUTHORITATIVE 字段并生成 DERIVED 字段；
4. 同一个 Commit 校验 expected WriterClaim/RecordVersion/Shard fence，一次性写入 V2 fields、manifest、
   `model_layout_version=2`、原业务 Receipt/Order，并只推进一次 RecordVersion；
5. 原始 v1 `record` 以 RETAINED_V1_INERT 原样保留，之后任何 V2 Commit 都不得更新它。

第 4 步成功前内存仍是 v1；明确失败不安装候选。禁止先写 V2 field 后翻 version，也禁止同时更新 v1
aggregate 与 v2 fields。

### 6.4 `TIMEOUT_UNKNOWN`

转换与普通业务 mutation 是同一个原 CommitAttempt。返回 `TIMEOUT_UNKNOWN` 后必须沿现有
`RESOLVING_UNKNOWN → FenceAndClaim(C1→C2)` 解析：

- 保留原 CommitRequestID、EffectiveIdempotencyKey、payload hash、before RecordVersion/digest 和转换 bytes；
- 不能重新 marshal 当前内存对象形成“近似相同”的第二请求；
- `COMMITTED`：安装解析得到的 V2 snapshot；
- `NOT_COMMITTED`：只有在 C2 WriterClaim 下重新验证业务前置条件后，才可创建下一 ordinal 的尝试；
- resolver 自身 unknown 时保持 Actor 隔离，不能读取 retained v1 继续服务。

WriterClaim、RecordVersion 和 PersistenceLane 仍是唯一顺序/裁决机制；model version 不成为第二个 CAS。

### 6.5 Rollback

首选回滚是关闭新转换并修复/前滚 reader；已经 v2 的 Actor继续 v2。若确需数据降级，只能由显式、审计的
per-Actor v2→v1 converter：在当前 WriterClaim、expected RecordVersion 和同一 Lane 内从全部 V2
AUTHORITATIVE 字段重建一个最新 v1 record，原子设置 `model_layout_version=1` 并推进一次 RecordVersion。
旧 retained blob 只是 golden/取证输入，因其已陈旧不能直接恢复。回滚也不得双写，V2 fields 可保留为
INERT 证据但 v1 reader 不可见；M0 不实现该操作，更不删除任何 blob。

## 7. Golden fixtures 与门禁

V2 开写前至少冻结这些逐字节 fixture：

- Player/Farm v1 initial、典型增长、容量边界与未知字段 fixture；
- v1 bytes → exact V2 manifest/field bytes；同输入重复转换必须 byte-identical；
- V2 active fields → exact v1 rollback bytes；业务语义 round-trip 等价；
- 每个字段的 deterministic codec、字段排序、nil/empty、整数边界和 malformed fail-closed；
- derived projection 从 active fields 重建与存储值一致；
- 各矩阵命令只修改允许字段，但一次 RecordVersion/Receipt/Order 原子性不变；
- conversion before/after apply 的 `TIMEOUT_UNKNOWN`、C1→C2、replay/conflict；
- write-v2 on/off、cohort、混合 v1/v2 Actor、旧 binary 不能误写 v2；
- sparse write 的物理 bytes/round trips 与 V1 生产 P50/P95/P99 对比。

进入实现前还必须有：授权生产样本、容量预算、Lua/Store 合同审阅、回滚演练、灰度 SLO，以及 Controller
对 Hero/Task 组合以及相关 Task 兼容载体的批准。未过 Controller 门时，`task` 只能保持候选；Guard/Pet
仍按本方案 deferred，
本 preflight 不替 Controller 作组合裁决。
