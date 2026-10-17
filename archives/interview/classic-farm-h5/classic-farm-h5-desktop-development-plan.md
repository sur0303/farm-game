# 经典农场 H5 电脑端开发实施方案

> **历史归档（2026-08-01）**：本文记录早期无可用后端时的开发方案。当前可运行的本地模拟环境已删除，Web、Staging、WeChat 均连接真实 farm-game；下文相关描述只用于决策追溯，不能作为启动说明。

版本：v0.9
更新时间：2026-07-25  
状态：实施中。H5 Mock 垂直闭环、Staging/WeChat 认证边界、断线恢复、移动视口验收、微信 `web-view` 壳层、非 Mock 构建审计、服务端地址校验和交接清单已完成；正式 HTTPS/WSS/Proto 联调与真机发布仍待服务端和小程序主体条件。Staging/WeChat 已设置 `canonical-pending` 保护，不会误发临时 Mock 帧。

> 2026-07-26 服务端接入更新：电脑端 H5 将连接 Linux/Docker 测试服务器上的 TCONND BY_WS/WSS，链路为 `H5 → TCONND → gateproxy → Unix gRPC GateStream → farm-gate`；农场项目不再新增自研公网 WSS Adapter。

> 2026-07-25 帧协议更新：H5 每次 `WebSocket.send()` 发送一个序列化的 `FarmFrame` protobuf，不再自行拼接 `total_size/meta_size/FrameMeta`；业务请求、结果和推送放在 FarmFrame.body。

关联文档：

- [经典农场项目设计交接文档](./classic-farm-agent-handoff.md)
- [经典农场 P0 通信合同](./classic-farm-communication-contract.md)
- [经典农场 H5 接入微信小程序实施方案](./classic-farm-h5-wechat-miniprogram-deployment.md)
- [个人主体优先的独立 H5 首发方案](./classic-farm-h5-personal-first-launch-plan.md)

---

## 0. 先给结论

当前先做一个可以在电脑浏览器中运行、调试、联机和验收的 H5 农场客户端；暂时不依赖个人小程序账号，也不把 H5 改写成微信小游戏包。

推荐路线：

```text
TypeScript + Vite
  ├── 浏览器运行时：电脑开发、Mock、测试环境联调
  ├── DOM/CSS Grid 优先：先把状态、交互和自动化验收做扎实
  ├── 可替换渲染边界：出现明确动效或性能需求后再引入 Canvas/Pixi
  ├── HTTPS + WSS：对齐交接文档的正式通信合同
  ├── Protobuf：生成客户端类型和编解码代码
  └── 平台适配层：现在 Browser，后续增加 WeChatWebView
```

第一条必须跑通的链路不是“先把界面做漂亮”，而是：

```text
电脑打开 H5
  → 开发身份登录或测试身份登录
  → 获取一次性 connection_ticket
  → 建立 WSS
  → 按服务端确认的 canonical WSS 鉴权首帧完成认证（不预设 ClientHello）
  → JoinFarm
  → 获得明确 farm_version 的空农场 Snapshot
  → 能安全退出、刷新和重连
```

这条链路通过以后，才逐步加入播种、收获、好友访问和偷菜。

边界必须明确：`SessionPermit` 和 `FriendPermit` 是 Gateway/Logic 之间的服务端可信上下文，不能下发给 H5，也不能由 H5 保存或随命令提交。H5 只持有短时一次性 `connection_ticket`，票据在服务端确认的 WSS 鉴权握手完成后即失效；在服务端确认具体首帧前，客户端不得自行假设 `ClientHello` 或 URL 参数。

### 0.1 本阶段成功标准

“电脑端 H5 开发完成”不等于“微信小程序上线完成”。本方案把成功拆成三层：

| 层级 | 目标 | 通过标准 |
|---|---|---|
| L1：本地可开发 | 不依赖微信账号即可开发画面和交互 | 已通过：`npm run dev`、Mock 个人经营、好友访问、分享创建与落地、浇水和偷菜恢复；39 个单元测试、3 条桌面/移动/分享 E2E、非 Mock 构建审计通过 |
| L2：测试环境可联机 | H5 经 TCONND/gateproxy/GateStream 连接真实 `farm-gate` 和 `farm-logic` | 待真实平台探针：登录、WSS、Join、个人命令、重连和幂等联调 |
| L3：微信可承载 | 同一套 H5 被小程序 `web-view` 打开 | 壳层、最小分享落地页和登录桥接已具备；HTTPS 业务域名、正式 share_token、iOS/Android 真机待验收 |

当前 L1 已完成，正在等待服务端进入 L2；L3 的小程序壳代码和分享入口边界已完成，待正式域名、小程序主体和真机条件后推进。

### 0.3 开发模式的认证边界

电脑端没有 `wx.login()`，因此不能假装自己已经完成微信登录。三种模式必须使用三条明确的认证路径：

```text
Mock：
  MockAuthProvider → Fake Auth → 一次性测试 connection_ticket

Staging：
  测试账号/测试登录接口 → HTTPS → farm-gate
  → 服务端创建会话 → 一次性 connection_ticket

WeChat：
  小程序壳 wx.login() → code2Session
  → launch_ticket → H5 exchange
  → 一次性 connection_ticket
```

Staging 的测试登录接口只能部署在隔离测试环境，并且使用测试账号白名单或内部身份认证；它必须返回与生产交接流程兼容的 `connection_ticket`，而不是直接给 H5 一个长期 Token。生产构建、生产 Gateway 和生产域名都不得接受该测试登录方式。

### 0.2 明确不做的事情

- 不在 H5 中保存金币、地块、仓库、好友关系或偷菜结果作为权威数据；
- 不由客户端权威判定作物是否成熟；客户端可根据服务端时间偏移和 `mature_at` 展示倒计时，但操作能否成功只由服务端裁决；
- 不用前端乐观更新掩盖服务端未提交的业务事实；
- 不用浏览器存储代替 TcaplusDB；本地只保存 UI 偏好、最近路由和最小化的未决操作恢复日志；
- 不在 H5 中放 AppSecret、`session_key` 或长期平台密钥；
- 不把 `wx.miniProgram.postMessage` 设计成实时 RPC；
- 不为了当前 H5 第一阶段引入完整微信小游戏运行时、`game.json` 或小游戏专用 API；
- 不在没有通信骨架和验收用例的情况下先堆完整美术和复杂动画。

### 0.4 个人主体下的微信承载决策

当前只能申请个人主体时，必须把“个人注册资格”和“当前 H5 能否被承载”分开判断：

| 路线 | 能否直接复用当前 H5 | 个人主体下的处理 |
|---|---|---|
| 小程序 `web-view` | 可以复用，但取决于后台是否允许业务域名和 `web-view` | 先用真实个人主体后台验证；不允许时不寻找跳转或代理绕过 |
| 独立 H5 | 可以复用 | 继续在电脑/手机浏览器运行，不依赖小程序主体 |
| 原生微信小游戏 | 不能直接复用 DOM/CSS 画面 | 复用农场领域状态、服务端协议、好友权限和 `operation_id`，另做 Canvas/小游戏渲染与输入适配 |

好友访问、浇水和偷菜由服务端根据登录身份、分享链接/邀请码和 `FriendPermit` 裁决，不需要微信支付，也不需要读取微信好友列表。因此“不做充值”能减少商业化限制，但不能自动解除个人主体的 `web-view` 限制。当前优先级是继续完成 H5 和服务端联调；个人账号只用于后续能力验证，不改变电脑 Mock 和正式协议接入路线。

---

## 1. 问题定义和硬约束

### 1.1 要解决的问题

实现一个可以在电脑浏览器中运行的经典农场 H5 客户端，接入交接文档已经冻结的 HTTPS/WSS/Protobuf 通信合同，完成 P0a 的个人经营、社交访问、实时互动和断线恢复；未来只增加微信平台适配层，不重写游戏核心。

### 1.2 为什么现在先做 H5

- 当前项目已有完整的后端业务设计和通信合同；H5 客户端和最小微信壳已实现，正式服务端源码仍待提供；
- H5 可以脱离微信账号、业务域名和审核流程先验证业务闭环；
- 电脑浏览器更适合调试 Protobuf、WSS、状态机、错误恢复和多窗口并发；
- 先定义平台边界，可以避免把微信 API 判断散落在业务代码里；
- 后续接入微信时，核心代码仍然只认识 `BrowserRuntime` 或 `WeChatWebViewRuntime`，不需要改种植和收获逻辑。

### 1.3 必须继承的交接结论

| 交接结论 | H5 的对应约束 |
|---|---|
| H5 通过 HTTPS 完成登录和分享落地 | `HttpClient` 统一管理 API；分享数据由后端生成 |
| H5 通过 WSS 发送命令、接收快照和实时变化 | `WsClient` 设置 `binaryType=arraybuffer`，禁止把业务协议换成临时 JSON |
| Gateway 负责连接，不拥有农场权威事实 | H5 不把 Gateway 返回的中间状态当成最终提交 |
| `PlayerFarmActor(owner_id)` 串行裁决同一 Owner | H5 对同一操作显示 Pending，不自行合并或重排业务结果 |
| TcaplusDB 是权威业务存储 | 只有服务端确认提交后，H5 才展示成功奖励或资产变化 |
| Redis 只保存可重建状态 | H5 不依赖 Redis 语义，不根据缓存结果确认资产 |
| 相同 `operation_id` 负责重试幂等 | 超时、断线和用户重复点击都复用原操作 ID |
| `farm_version` 负责公开农场变化顺序 | 发现版本跳跃时停止继续应用变化，重新 `JoinFarm` |
| 跨 Owner 偷菜靠持久 Settlement/Outbox 推进 | H5 能展示 `Processing`，不能把处理中显示成已到账 |

---

## 2. 现有状态和信息缺口

### 2.1 当前已知现状

当前仓库已经具备设计文档和可运行客户端，包含：

- P0/P0a 业务边界；
- 个人经营、好友访问、浇水、偷菜的流程和失败语义；
- `farm-gate`、`farm-logic`、`farm-worker` 三类业务进程，以及 TCONND/gateproxy 接入边界；
- `PlayerFarmActor(owner_id)` 权威边界；
- TcaplusDB + Redis 的存储决策；
- HTTPS/WSS/Protobuf 通信合同；
- `operation_id`、`farm_version`，以及仅存在于服务端可信链路中的 SessionPermit、FriendPermit 等核心概念。
- `h5/` TypeScript/Vite 工程、Mock Auth、FakeFarmServer、浏览器 WSS 传输、状态 Store、Pending 操作日志和 Playwright E2E；
- `wechat-shell/` 普通微信小程序 `web-view` 壳层，以及 `wx.login()` 到 `launch_ticket` 的交接代码；
- H5 服务端接入清单和服务端交接单。

当前仍缺少：

- 生成后的 `.proto` 客户端代码；
- 可联调的真实 Gateway/Logic/Worker；
- 微信 AppID、AppSecret 和业务域名；
- 已确定的美术资源、屏幕适配基线和最终 UI 视觉稿。

因此第一阶段必须允许 Mock 数据和测试身份存在，但 Mock 只能是开发工具，不能混入生产权限和生产业务路径。

### 2.2 需要尽早补齐的信息

以下信息不影响先搭骨架，但会影响后续实现：

| 信息 | 影响 | 默认处理 |
|---|---|---|
| 服务端真实启动时间 | 决定 L2 联调时间 | 先做 Mock，再对接测试 Gateway |
| Proto 最终生成方式 | 决定包脚本和类型来源 | 先以通信合同为源，禁止手写重复字段 |
| 画面风格和资源尺寸 | 影响布局和加载器 | 先使用占位资源，锁定逻辑尺寸和响应式规则 |
| 是否需要移动浏览器直接访问 | 影响触摸输入和竖屏布局 | 默认兼容桌面 + 移动浏览器，微信适配后置 |
| 是否有真实玩家社交关系 | 影响分享与权限联调 | 先用测试 Owner；服务端生成并校验 FriendPermit |
| 是否加入音效、广告、支付 | 影响审核和平台能力 | P0 先不做，另立评审分支 |

---

## 3. 客户端技术路线评估

### 3.1 方案 A：TypeScript + Vite + DOM/CSS Grid（推荐）

#### 实现思路

- 用 TypeScript 组织业务和状态；
- 用 Vite 提供开发服务器、构建和环境变量；
- 农场地块、作物、选中态、商店、背包、金币和好友列表优先使用 DOM/CSS Grid；
- 通过 `FarmRenderer` 隔离渲染实现，后续有明确的动效、同屏对象量或性能证据时，再替换局部为 Canvas/Pixi；
- 业务状态和渲染状态分离，DOM、Canvas 或引擎都不是业务数据的拥有者。

#### 优点

- 最贴合当前 H5 和未来 `web-view`；
- 依赖少，问题容易定位；
- 农场 UI 主要是面板、卡片和有限地块网格，DOM 更容易做响应式、可访问性和 E2E 定位；
- 不会被完整游戏引擎的场景、资源和生命周期模型绑死；
- 后续如果动画复杂，可以在 `FarmRenderer` 内部替换成 Phaser/Pixi，而不动通信和业务状态。

#### 风险

- 大量逐帧动画和粒子效果不是 DOM 的强项；
- 如果视觉复杂度增长，需要保留性能基准并局部迁移渲染器，而不是让业务层依赖某个 UI 技术。

### 3.2 方案 B：Phaser 等完整 2D 引擎

#### 优点

- 场景、精灵、动画、输入和资源加载较成熟；
- 如果未来农场要加入大量动画、拖拽、地图和特效，开发速度可能更快。

#### 风险

- 引擎状态和服务端状态容易混在一起；
- 包体、插件、生命周期和微信 `web-view` 兼容性更复杂；
- 当前 P0 的主要难点是通信和一致性，不是碰撞或渲染能力；
- 过早采用会增加首屏、调试和资源管理成本。

### 3.3 方案 C：直接做微信小游戏原生客户端

当前 H5 第一阶段暂不采用，但在“只能使用个人主体且后台不提供 `web-view`”时，它是微信端的正式 fallback，而不是平台规避方案。它要求重新考虑运行时、工程配置、资源包、微信 API 和发布路线；可以复用农场领域状态、服务端协议、好友权限和 `operation_id`，但不能直接复用 DOM/CSS 画面。只有个人主体后台验证失败后，才启动这条独立客户端分支。

### 3.4 Tradeoff 矩阵

| 维度 | A：DOM/CSS Grid | B：完整 2D 引擎 | C：原生小游戏 |
|---|---|---|---|
| 当前启动成本 | 低 | 中 | 高 |
| 对现有 H5 约束的匹配 | 高 | 中 | 低 |
| 业务状态可控性 | 高 | 中 | 中 |
| UI/表单/列表开发 | 高 | 中 | 低到中 |
| 动画和特效能力 | 中 | 高 | 高 |
| 后续微信 `web-view` 适配 | 高 | 中 | 不适用 |
| 现在的推荐度 | **推荐** | 设复杂度出现后再评估 | 个人主体无 `web-view` 时作为 fallback |

### 3.5 当前选择

选择方案 A，但保留渲染层替换接口：

```text
FarmViewStore / OwnerPrivateStore / CommandService / ConnectionManager
                  ↓
             FarmRenderer
          ┌───────┴──────────────┐
    DomFarmRenderer       Canvas/PixiRenderer
       （P0）               （按证据后置）
```

这不是为了提前做多套客户端，而是为了明确：渲染层不能成为服务端状态和通信层的事实拥有者。

P0 的性能决策门槛：先用固定数量的地块和占位资源建立基线；如果 DOM 更新、输入响应或动效需求在目标浏览器上达不到验收指标，再只替换渲染器，不改 Store、Domain 和 Network。性能门槛的具体数值在第一批页面和资源冻结后记录，不能凭感觉提前引入引擎。

---

## 4. 推荐工程结构

第一版工程建议如下：

```text
h5/
  package.json
  tsconfig.json
  vite.config.ts
  index.html
  public/
    assets/
  src/
    main.ts
    app/
      App.ts
      BootFlow.ts
      Environment.ts
    platform/
      runtime.ts
      browser-runtime.ts
      wechat-webview-runtime.ts       # 后续接入，不在第一阶段实现
      navigation.ts
      visibility.ts
    auth/
      auth-provider.ts
      mock-auth-provider.ts
      staging-auth-provider.ts
      wechat-auth-provider.ts         # 后续由小程序壳交接 ticket
      connection-ticket.ts            # 短时一次性票据，不持久化 Permit
    network/
      http-client.ts
      ws-client.ts
      reconnect-controller.ts
      heartbeat.ts
      request-trace.ts
    protocol/
      generated/                      # 由 proto 生成，禁止手改
      codec.ts
      envelopes.ts
      protocol-version.ts
    state/
      owner-private-store.ts           # 自己的钱包、仓库、成长
      farm-view-store.ts               # 按 target_owner_id 隔离公开农场视图
      connection-store.ts
      pending-operation-store.ts
      share-store.ts
      selectors.ts
    persistence/
      indexeddb-operation-journal.ts   # 按环境/账号隔离的最小未决操作恢复日志
    domain/
      command-service.ts
      snapshot-reducer.ts
      change-reducer.ts
      operation-reconciler.ts
      farm-version-guard.ts
    scenes/
      boot/
      farm/
      friend-farm/
      share/
      error/
    render/
      farm-renderer.ts
      dom-farm-renderer.ts
      canvas-renderer.ts               # 后置可选实现
      dom-panels.ts
      asset-loader.ts
      viewport.ts
    ui/
      components/
      notifications/
      loading/
    test/
      fixtures/
      fake-server/
      contract/
      e2e/
```

### 4.1 模块依赖规则

```text
scenes / ui / render
          ↓
domain / state
          ↓
network / auth / protocol
          ↓
platform
```

规则：

- `scenes` 不直接操作 WebSocket；只能调用 `CommandService`；
- `render` 只读 Store 和 Selector，不直接修改业务状态；
- `OwnerPrivateStore` 和 `FarmViewStore` 只接受服务端确认后的事件、快照和操作结果；`PendingOperationStore` 只记录客户端操作生命周期；
- `network` 不知道“播种”还是“偷菜”，只负责传输信封和协议错误；
- `platform` 只处理浏览器、微信 web-view、导航和可见性差异；
- `protocol/generated` 由通信合同生成，不能在业务代码中复制字段定义。

### 4.2 前端状态的四个边界

```text
OwnerPrivateState(self_owner_id)
  ├── wallet / inventory / progression
  └── 只接受服务端返回给本人的私有资产结果

FarmViewState[target_owner_id]
  ├── farm / plots / crop_cycles
  ├── farm_version / plot_version
  └── 可公开的 presence / permissions

PendingOperationJournal
  ├── environment / player_id / session_epoch
  ├── operation_id
  ├── command_type / target_owner_id
  ├── created_at / last_status / retry_count
  └── status: Created | Sending | AwaitingResult | Processing | Resolved | Unknown

ViewState
  ├── 当前页面和选中地块
  ├── loading/error/toast
  ├── viewport/动画帧
  └── UI 偏好
```

约束：

- 好友农场 Snapshot 只能更新对应的 `FarmViewState[target_owner_id]`，不能覆盖自己的钱包和仓库；
- `PendingOperationJournal` 只记录恢复查询所需的最小信息，不能保存 Permit、票据、可信价格或资产副本；
- 日志记录必须包含 `environment + player_id + session_epoch + operation_id`；查询范围按 `environment + player_id` 隔离，`session_epoch` 只记录操作发起时的会话元数据，不能阻止新连接恢复旧 operation_id；客户端传给查询接口的 Owner/路由字段只用于选择本地记录，服务端仍以 operation_id 绑定的权威 Owner 为准；
- 终态 `Succeeded`、`NoChange`、`Rejected` 到达后删除对应恢复日志；`Unavailable` 或断线未知继续保留；
- 退出登录或切换 `player_id` 时，旧身份日志不得继续驱动自动查询；新 `session_epoch` 建立后仍应查询同一环境/玩家范围内的未决 operation_id；可保留脱敏诊断记录，但不能跨玩家自动重试；
- `ViewState` 只能从确认状态和 Pending 状态派生，不能反向改写权威确认状态；
- `CommandResult` 与 `FarmChange` 到达顺序不固定，统一交给 `operation-reconciler`：公开农场事实按版本去重，命令结果负责结束 Pending 并更新允许返回的私有资产。

---

## 5. 三种运行模式

### 5.1 Mock 模式：前端开发模式

用途：没有服务端时开发页面、交互、状态展示和错误态。

特征：

- 使用固定 Fixture 生成 Owner、Farm、Plot、CropCycle、Inventory；
- `MockAuthProvider` 只返回本地一次性 `connection_ticket`，不伪造或暴露 SessionPermit；
- 页面先停留在明确的开发登录页，用户点击测试账号后才获取 ticket 和建立连接；这个入口用于验证未登录、登录中、登录失败和重新登录状态，不代表生产认证；
- `FakeServer` 必须接受真实 `ClientFrame`、返回真实 `ServerFrame`，使用与 Staging 相同的 Protobuf 编解码和状态枚举；
- `FakeServer` 使用可注入 `FakeClock`，模拟成功、拒绝、无变化、处理中、不可用、超时、重复请求、版本跳跃和断线；
- Mock 页面已经提供好友农场只读切换，用于提前验证 `target_owner_id` 隔离；真实好友访问必须由服务端生成并校验 `FriendPermit`；
- Mock 已加入 `WaterCrop`，验证好友可读访问与被授权护理的边界；`BeginTheft` 以明确的 TargetCommitted → Processing → Completed Mock 状态机演示客户端处理，真实线上仍必须由 Settlement/Outbox/Worker 裁决。
- 客户端操作目标改为用户点击的具体地块，并携带该地块的 `plot_version` 与 `crop_cycle_id`；不再把“第一个空地”当成最终交互模型。
- Fake Server 已按连接保存认证玩家上下文，双连接测试不会共享全局登录身份；浏览器双上下文共享服务进程的联机 E2E 仍待独立测试夹具接入。
- 偷菜 Mock 已覆盖目标提交后断线、服务端继续完成、重连按原 `operation_id` 查询终态；真实实现需由 Worker/Outbox 提供同等可靠性。
- 不请求生产域名，不写真实数据库；
- Mock 操作也必须遵守 `operation_id`、状态流转和 `farm_version` 规则，不能做成“点击即改数据”的假逻辑。

禁止在页面组件中直接调用 `fakePlant()`、`fakeHarvest()` 之类的内存方法。Mock 和 Staging 必须经过同一个 `WsClient → codec → reducer/reconciler` 路径，否则 Mock 测通不能证明真实协议可用。

### 5.2 Staging 模式：真实后端联调模式

用途：验证 H5 与真实 Gateway/Logic/Worker 的通信和业务语义。

特征：

- 使用测试 HTTPS/WSS 域名；
- 使用隔离测试数据库和测试 Owner；
- 使用独立的开发登录提供者或测试票据；
- 日志携带 `trace_id`、`session_id`、`operation_id`、`owner_id`、`farm_version`；
- 任何测试奖励和测试数据都不能进入生产表。

### 5.3 WeChat 模式：后续小程序承载模式

用途：验证小程序壳 + H5 `web-view`。

特征：

- 壳层调用 `wx.login()`，服务端完成 `code2Session`；
- 壳层向 H5 交接短时、一次性 `launch_ticket`；
- H5 只调用 `/api/v1/auth/exchange`，不接触 `AppSecret` 和 `session_key`；
- H5 的业务连接仍然使用浏览器 WSS，除非以后明确迁移到壳层 `wx.connectSocket`；
- H5 业务代码不改变，只替换 `AuthProvider` 和 `Runtime`。
- 客户端已提供 `WeChatAuthProvider`：从 WebView URL 读取短时 `launch_ticket`，通过 HTTPS 换取一次性 `connection_ticket`；小程序壳和真实 exchange 接口仍待后续接入。

环境切换必须在构建配置中完成，不能让用户通过 URL 参数切换到 Mock 或开发登录。当前使用 `npm run build` 生成 Mock 构建、`npm run build:staging` 生成 Staging 构建；构建产物中的环境标识、API/WSS 地址和是否包含 Mock 代码必须可审计。

生产构建必须在打包阶段排除 Mock Auth、Fixture 和故障注入入口；仅靠运行时隐藏按钮不算隔离。

当前已通过构建产物检查：Mock 构建包含 Mock 动态 chunk，Staging/WeChat 构建不包含 Mock 代码；E2E 启动命令固定 `--mode mock` 且不复用未知环境的旧服务进程。

---

## 6. 通信和状态设计

### 6.1 启动主流程

```text
Boot
  → 读取环境配置
  → 创建 Runtime、AuthProvider、HttpClient、WsClient
  → 通过 HTTPS 登录或兑换交接票据
  → 获取一次性 connection_ticket
  → 建立 WSS
  → 按 canonical 合同发送服务端确认的鉴权首帧或连接参数
  → 等待服务端确认的认证响应
  → 发送 JoinFarm
  → 校验 Snapshot
  → 写入对应 FarmViewState
  → 渲染 FarmScene
```

任一步骤失败，都进入可重试的错误状态，不允许展示一个看起来正常但实际上没有服务端身份的农场。

连接状态机固定为：

```text
Disconnected
  → Connecting
  → Authenticating
  → Authenticated
  → Joining
  → InFarm
  → Reconnecting
```

认证响应只代表连接认证完成，不代表已经进入农场；只有 `JoinFarm` Snapshot 校验并应用成功后才能进入 `InFarm` 并开放业务按钮。当前 canonical Proto 草案没有定义 `ClientHello/ServerHello`，真实实现必须以服务端交付的 Proto、鉴权位置和 golden vectors 为准。

### 6.2 个人经营命令

第一批命令按交接文档实现：

| 命令 | 前置状态 | 成功后客户端行为 |
|---|---|---|
| `BuyOffer` | 已登录、商店可用 | 等服务端结果后更新金币/库存 |
| `PlantCrop` | 地块空闲、库存有种子 | 只显示 Pending；成功后应用 CropCycle 和版本 |
| `HarvestCrop` | 服务端判定已成熟 | 成功后更新库存、金币、地块和版本 |
| `SellItem` | 背包有可出售物品 | 成功后更新库存和金币 |

每个命令都必须带：

- `operation_id`；
- 目标实体定位信息；
- `crop_cycle_id` 或 `expected_plot_version`（地块操作适用时）；
- 当前观察到的 `farm_version` 可作为缺口检测基线，但不作为所有命令的全农场严格 CAS；
- 客户端协议版本；
- 当前会话上下文由服务端绑定，客户端不能伪造 Owner 权限。

这样可以保证旧视图不会误操作到下一茬，同时避免两个互不争用的地块因为共享 `farm_version` 而产生假冲突。

同一个仍未结束的用户意图在超时、重连或重复点击时复用原 `operation_id`；一个操作已进入终态后，用户再次明确发起的新动作必须生成新的 `operation_id`。

### 6.3 结果处理规则

```text
Succeeded
  → 应用服务端返回的确认结果和 farm_version

Processing
  → 显示处理中，不显示到账或最终奖励

NoChange
  → 保留当前确认状态，结束 Pending，提示“已经浇过”等幂等业务结果

Rejected
  → 保留当前确认状态，结束 Pending，显示可理解且不可盲目重试的原因

Unavailable
  → 标记为 Unknown，不新建 operation_id
  → 恢复连接后查询原 operation_id
  → 必要时重新 JoinFarm

连接断开且命令未确认
  → 刷新登录或重新获取一次性 connection_ticket
  → 恢复 WSS，并完成服务端确认的鉴权首帧/认证响应
  → 查询 PendingOperationJournal 中的原 operation_id
  → 再 JoinFarm 获取最新 Snapshot
```

客户端不根据“请求已经发出”“按钮已经点击”来判断业务成功。

对外业务结果只有五态：`Succeeded / Processing / NoChange / Rejected / Unavailable`。`Sending`、`AwaitingResult`、`Unknown` 等只属于客户端 Pending 生命周期，不能伪装成新的服务端结果状态。

### 6.4 未决操作恢复日志

为防止页面刷新后丢失“请求可能已经执行”的信息，客户端用 IndexedDB 保存最小恢复日志：

```text
operation_id
command_type
target_owner_id
created_at
last_status
minimal_query_routing
```

规则：

- 发送前先可靠写入日志，再发送命令；日志写入失败时禁止发送写命令；
- 收到 `Succeeded`、`NoChange` 或 `Rejected` 后删除；
- 收到 `Processing`、`Unavailable`，或连接在结果前断开时保留；
- 新连接认证成功后先查询这些 `operation_id`，再用 Snapshot 收敛公开农场状态；
- 查询使用指数退避和服务端 `retry_after_ms`（如有）；达到客户端设定的查询上限后停止自动重试，向用户显示“结果仍在确认中”，不能自动创建新 operation_id；
- 日志设置服务端合同一致的最长保留期，超期后进入人工可解释的 Unknown，而不是自动重做；
- 页面刷新、浏览器崩溃和网络切换都不得产生一个新的 `operation_id` 来替代原操作。

### 6.5 Snapshot、Change 和 CommandResult 的应用规则

收到 `JoinFarm` Snapshot 时：

1. 校验目标 Owner、权限和协议版本；
2. 记录服务端 `farm_version`；
3. 清理或重置当前目标农场的 Change 缓冲；
4. 只把允许公开的农场字段写入 `FarmViewState[target_owner_id]`；
5. 只有自己的私有响应才允许更新 `OwnerPrivateState(self_owner_id)`；
6. 渲染完成后再允许用户操作。

收到 Change 时：

- `version == current + 1`：应用并推进版本；
- `version <= current`：视为重复或旧消息，丢弃并记录；
- `version > current + 1`：停止继续应用，标记 Gap，重新 JoinFarm；
- Change 不能跨 Owner 混用，也不能用一个农场的版本推进另一个农场。

同一订阅内的 `FarmChange` 按 `farm_version` 保序；`CommandResult` 可以在相关 `FarmChange` 前后到达。两者的合并规则如下：

| 到达情况 | 客户端处理 |
|---|---|
| `FarmChange` 先到 | 按连续 `farm_version` 应用公开变化，操作仍保持 Pending，等待 CommandResult 结束生命周期 |
| `CommandResult` 先到且只包含私有结果 | 更新允许返回的私有资产并结束/推进 Pending；不能凭结果手工推进公开农场版本 |
| `CommandResult` 先到且携带完整公开实体变化 | 只有在协议明确标记其版本和实体覆盖范围时，才能按同一版本合并；否则等待 FarmChange 或重新 Join |
| 重复 `FarmChange` 或重复结果 | 按版本/operation_id 去重，不重复播放奖励或结束新操作 |
| `FarmChange` 跳版本 | 停止应用该订阅后续变化，标记 Gap 并重新 JoinFarm |

`CommandResult` 和 `FarmChange` 可能先后到达：

- Change 先到：先按 `farm_version` 应用公开变化，操作仍保持 Pending，等待结果结束其生命周期；
- Result 先到：先结束或推进 Pending，并应用结果中允许返回的私有资产；只有结果携带完整公开实体变化时才按版本合并，否则等待 Change 或重新 Join，不能只推进版本号；
- 后到的消息如果版本已应用则去重，不能再次播放奖励、重复入账或回退状态；
- 成熟展示使用服务端时间偏移和本地单调时钟估算，与 `mature_at` 比较，但播种、收获、偷菜等结果仍以上述服务端消息为准。

### 6.6 服务端时间同步

客户端不使用墙上时钟作为业务计时器。每次服务端确认的认证响应或带服务端时间的 HTTPS 响应都记录一组同步样本：

```text
client_monotonic_before_request
server_time_ms
client_monotonic_after_response
```

P0 使用响应前后单调时钟的中点估计客户端与服务端的时间偏移；重连后重新校准。`estimated_server_now` 只用于倒计时和展示，服务端仍必须在 `HarvestCrop` 等命令处理时重新依据权威时间裁决。

---

## 7. 分阶段实施路径

### 阶段 0：工程和协议基线

#### 交付物

- H5 工程可以启动、构建和测试；
- TypeScript、格式检查、单元测试和 E2E 测试命令固定；
- 环境配置分为 Mock、Staging、WeChat；
- 先冻结第一版正式 `.proto`，再生成 Go/TypeScript 代码、脚本和版本记录；
- App/Runtime/Network/State 目录骨架；
- 最小错误页、加载页和日志字段。

#### 验收

- 新环境可以按 README 启动；
- 生产密钥不出现在前端构建产物；
- 构建产物没有本地调试登录开关；
- 协议生成代码可重复生成，生成前后无非预期差异。

### 阶段 1：垂直技术骨架

#### 交付物

```text
Mock/开发登录
  → 一次性 connection_ticket
  → WSS 建连
  → canonical WSS 鉴权首帧 / 认证响应（以服务端最终合同为准）
  → JoinFarm
  → 空农场 Snapshot
  → 刷新后恢复
  → 断线后重连
```

#### 必测场景

- 首次打开；
- 刷新页面；
- 关闭后重新打开；
- WSS 在 Join 前断开；
- Join 后断开；
- 收到重复 Snapshot；
- 收到版本跳跃；
- WSS 鉴权响应超时；
- ticket 过期或会话失效。

#### Go/No-Go

没有这一阶段的稳定证据，不进入个人经营业务。否则后续每个按钮都会重复实现一套不可靠的连接和错误处理。

### 阶段 2：P0a-1 个人经营

按交接文档顺序实现：

1. `BuyOffer`；
2. `PlantCrop`；
3. `HarvestCrop`；
4. `SellItem`。

#### 验收重点

- 双击播种只扣一次；
- 快速重复收获不重复入库；
- 修改本机时间最多影响倒计时展示，不能让服务端提前允许收获；
- 请求超时后复用相同 `operation_id`；
- 服务端拒绝后客户端回到服务端确认状态；
- TcaplusDB 提交失败或结果未知时不显示成功；
- 刷新页面后资产与之前一致；
- 两个浏览器窗口登录同一 Owner 时，顶号和旧会话行为符合合同。

### 阶段 3：P0a-2 社交访问和分享

#### 交付物

- 后端创建 `share_token` 后进入好友农场；
- 展示服务端裁决后的访问能力；`FriendPermit` 只在服务端内部传递，不下发 H5；
- `JoinFarm(target_owner_id)`；
- 访问权限撤销、过期分享和重复接受；
- 目标农场只展示被授权的公开信息；
- 退出好友农场后重新 Join 自己的农场。

#### H5 重点

- 目标农场和自己的农场使用不同的 Store scope；
- 离开目标农场时停止应用目标农场 Change；
- 不把好友关系写进 URL 作为可信权限；
- URL 中只携带短期分享标识，最终由服务端确认。

### 阶段 4：P0a-3 实时互动

按交接文档实现：

- `WaterCrop`；
- `BeginTheft`；
- 目标农场实时成员和订阅；
- `GrantLoot` / `CompleteTheft` 的处理中状态展示。

#### 验收重点

- 两个或多个浏览器窗口同时操作同一地块；
- 同一块地的版本严格递增；
- 旧窗口收到版本缺口后重新 Join，而不是猜测中间状态；
- 访客没有权限或名额已满时显示明确错误；
- 偷菜先显示 Processing，只有收到最终确认才显示到账；
- Worker 延迟、重复投递和重启不导致 H5 重复奖励。

### 阶段 5：稳定性、体验和微信迁移准备

#### 交付物

- 网络弱、断线、切后台、刷新、重复点击的可回归测试；
- 资源带内容哈希，入口 HTML 可回滚；
- 协议版本和 H5 版本显示在诊断信息中；
- 错误码映射为用户可理解的提示；
- 首屏加载、WSS 建连、JoinFarm 和首个 Snapshot 的耗时埋点；
- `WeChatWebViewRuntime` 和微信壳接入接口定义完成；
- README 写清本地、测试、生产三套启动方式。

这一阶段结束后，再申请或使用合适主体的小程序账号进行 `web-view` 接入。

---

## 8. “拷打”失败模式清单

| 质疑 | 如果不处理会怎样 | 方案中的处理 |
|---|---|---|
| 用户点击播种后能不能马上改界面？ | 客户端显示已播种，但服务端可能拒绝或超时 | 只显示 Pending；成功结果回来后再改确认状态 |
| 请求发出但浏览器断网，是否重新生成 ID？ | 同一操作可能执行两次 | 原 `operation_id` 查询或重试，禁止生成新 ID |
| WSS 重连后直接继续发命令可以吗？ | 连接可能尚未认证或快照已过期 | 重新完成服务端确认的鉴权首帧/认证响应，再检查未决操作，再 Join |
| 收到版本 10，但本地是 8 怎么办？ | 直接应用会产生状态缺口 | 标记 Gap，停止应用 Change，重新 Join |
| 客户端自己算成熟时间可以吗？ | 修改电脑时间或时区后可制造错误展示，若服务端信任则可作弊 | 用服务端时间偏移和 `mature_at` 展示倒计时；操作成功仍只信服务端 |
| 前端存一份金币方便刷新可以吗？ | 本地数据与权威状态分叉，可能造成错误操作 | 本地只存偏好和临时状态，刷新后以 Snapshot 为准 |
| 偷菜开始就算成功吗？ | 跨 Owner 结算中断后会重复或丢奖励 | UI 使用五态，并明确区分 Succeeded/Processing/Unavailable |
| Mock 接口能否直接作为测试登录？ | 开发后门可能随构建进入生产 | Mock Auth 只在独立开发构建启用，生产构建无该代码路径 |
| 先做全套美术可以吗？ | 通信和状态模型没验证，返工成本高 | 先用占位资源跑通垂直链路，再替换视觉资源 |
| 把所有东西放一个 React/Canvas 组件可以吗？ | UI、渲染、连接和业务状态互相污染 | 按 Runtime/Network/State/Domain/Render 分层 |
| H5 能否直接调用微信 `wx.*`？ | 电脑浏览器和微信 web-view 环境分支散落 | 统一由 `Runtime` 和 `PlatformAdapter` 封装 |
| `postMessage` 能不能同步农场变化？ | 消息到达时机不稳定，实时状态会丢 | 实时状态只走 WSS，postMessage 只传非实时分享数据 |

---

## 9. 测试策略

### 9.1 单元测试

重点测试纯函数和状态边界：

- `snapshot-reducer`：完整 Snapshot 是否正确建立状态；
- `change-reducer`：连续版本、重复版本和 Gap；
- `operation-reconciler`：Succeeded/Processing/NoChange/Rejected/Unavailable，以及 Result/Change 两种到达顺序；
- `farm-version-guard`：不同 Owner、旧版本和跨目标农场；
- `LinkGenerator`：普通浏览器路由、Hash 路由和未来 web-view 路由；
- 资源加载失败和默认资源降级。

### 9.2 通信合同测试

- `.proto` 编解码黄金样例；
- 服务端确认的 WSS 鉴权首帧和认证响应；
- 错误码和结果状态映射；
- Snapshot 和 Change 的字段兼容；
- 旧客户端读取新服务端允许的向后兼容字段；
- 非法字段、过大消息、未知枚举和协议版本错误。

### 9.3 Fake Server 和集成测试

Fake Server 至少支持以下故障注入：

- 响应延迟；
- 响应丢失；
- 连接在发送前、发送后、响应前断开；
- 重复响应；
- Change 重复、乱序、跳版本；
- `Unavailable`；
- `Processing` 长时间不结束；
- ticket 失效；
- Session 顶号。

Fake Server 的测试输入输出必须是正式 Protobuf 帧，并使用可推进的 `FakeClock`；不能通过直接调用前端 Store 来模拟服务端。

### 9.4 浏览器 E2E 测试

用两个独立浏览器上下文模拟两个玩家，至少覆盖：

```text
A 登录 → 播种 → B 访问 A → B 浇水/偷菜
→ A 收到实时变化 → B 断线 → B 重连 → 状态无缺口
```

工具默认选择：Vitest 负责单元/合同测试，Playwright 负责双浏览器上下文 E2E；如果仓库已有统一测试工具，必须保持测试输入仍然经过正式 Protobuf 帧和 Fake Server。

### 9.5 人工验收

- Chrome/Edge 最新版本；
- 电脑窗口缩放；
- 慢网、断网、刷新、切换标签页；
- 两个账号同时点同一地块；
- 控制台无未处理 Promise、协议解析错误或重复定时器；
- 任何失败提示都能告诉用户“是否成功、是否需要重试、是否可以继续”。

---

## 10. 电脑端开发和联调方式

### 10.1 本地 Mock

```text
浏览器 H5: http://localhost:5173
Fake Server: 内存状态或独立测试进程
```

适合做画面、交互、状态机和异常测试，不需要真实域名。

### 10.2 本地 H5 + 测试后端

```text
浏览器 H5: http://localhost:5173
浏览器访问:  /api 和 /ws
Vite Proxy:  https://staging-farm.example.com / wss://staging-farm.example.com
```

需要处理：

- 本地开发优先通过 Vite 同源代理转发 `/api` 和 `/ws`，减少跨域 Cookie、预检和证书差异；
- 如果必须直接跨域，再显式配置 CORS，禁止用 `*` 搭配凭证；
- WSS 证书和反向代理升级头；
- Gateway 校验 WebSocket `Origin` 白名单；测试和生产分别配置；
- 测试登录；
- 测试数据库隔离；
- 浏览器刷新后 Cookie/Session 行为；
- 日志中的 `trace_id`、`operation_id` 和 `farm_version`。

Vite 配置示意：

```ts
export default defineConfig({
  server: {
    proxy: {
      '/api': {
        target: 'https://staging-farm.example.com',
        changeOrigin: true,
      },
      '/ws': {
        target: 'wss://staging-farm.example.com',
        ws: true,
        changeOrigin: true,
      },
    },
  },
});
```

代理只解决本地开发路径，不替代 Gateway 的 Origin、票据核销、消息大小和速率限制。

### 10.3 两个玩家测试

- 使用两个浏览器 Profile 或两个隐身窗口；
- 不要只用同一个窗口切换 URL，因为这会掩盖 Session 和并发问题；
- 固定测试 Owner A/B 和地块 ID；
- 每次测试记录输入、服务端结果、客户端展示和最终数据库事实；
- 任何“看起来对”的结果都要能通过服务端查询或测试日志验证。

### 10.4 构建和部署

生产 H5 采用：

```text
带内容哈希的 JS/CSS/图片
  → 上传 CDN/静态托管
  → 先验证资源可访问
  → 再切换入口 HTML
  → 保留上一版入口和资源
```

协议或后端发布时保持向后兼容，避免已经打开的 H5 因为静态资源或字段版本不一致立刻失效。

---

## 11. 后续接入微信的保留接口

H5 现在就按以下接口写，未来不改业务场景：

```ts
interface Runtime {
  kind: 'browser' | 'wechat-webview';
  navigate(url: string): void;
  goBack(): void;
  isVisible(): boolean;
}

interface AuthProvider {
  acquireConnectionTicket(): Promise<ConnectionTicket>;
}

interface ShareBridge {
  createShare(target: ShareTarget): Promise<ShareCard>;
  openNativeShare(share: ShareCard): void;
}
```

当前实现：

- `BrowserRuntime` 使用浏览器路由和 `visibilitychange`；
- `MockAuthProvider` 从本地 Fake Auth 获取一次性测试票据；
- `StagingAuthProvider` 通过 HTTPS 登录获取一次性 `connection_ticket`；
- `ShareBridge` 先生成可复制的测试链接。

未来微信实现：

- 小程序壳调用 `wx.login()`；
- 服务端产生一次性 `launch_ticket`；
- H5 兑换票据；
- `navigate()` 在 web-view 中调用 `wx.miniProgram.navigateTo/redirectTo`；
- 分享由小程序原生分享页承接；
- H5 业务状态、WSS 和 Actor 规则保持不变。

---

## 12. 版本、观测和回滚

每次测试和发布至少记录：

- H5 版本；
- 协议版本；
- 后端镜像版本；
- 规则配置版本；
- 测试 Owner；
- 浏览器/平台；
- `trace_id`、`session_id`、`operation_id`、`farm_version`。

首批客户端指标：

- 首屏加载成功率和耗时；
- 登录/票据建立成功率；
- WSS 建连成功率、认证成功率和重连成功率；
- `JoinFarm` 成功率；
- Snapshot Gap 次数；
- `Unavailable`、`Processing` 和最终完成比例；
- 客户端未捕获异常、协议解析失败和资源加载失败；
- 个人经营命令的端到端耗时。

回滚优先级：

1. 停止切换入口 HTML；
2. 回滚到上一版静态资源；
3. 保持后端协议向后兼容；
4. 根据 `operation_id` 查询不确定命令；
5. 不通过清 Redis、重启 Gateway 或清浏览器缓存来修复权威业务事实。

---

## 13. 待确认项和默认决策

| 决策项 | 当前状态 | 默认决策 |
|---|---|---|
| H5 是否采用完整游戏引擎 | 已暂定 | P0 用 DOM/CSS Grid；有动效或性能证据后再评估 Canvas/Pixi/Phaser |
| 后端是否已经可联调 | 未知 | 先实现 Fake Server，真实服务可用后进入 Staging |
| 登录方式 | 微信账号尚未准备 | 先 Mock/测试登录，生产登录沿用一次性票据 |
| 美术资源 | 未冻结 | 先占位资源，先锁交互和通信 |
| 是否支持移动浏览器 | 未确认 | L1 只保证桌面浏览器；L2 后补最小触摸/竖屏回归，微信接入前必须完成移动真机验收 |
| 是否需要支付/广告/音频 | 未进入 P0 | 先不做，另行评估审核与平台依赖 |
| 是否要做 P0b | 后置 | P0a-1、P0a-2、P0a-3 稳定后再排 |

### 13.1 进入下一阶段的硬门槛

满足以下条件，才开始写真实个人经营：

- H5 能通过 HTTPS 稳定取得并核销一次性 `connection_ticket`；
- WSS 首帧认证和 JoinFarm 有自动化测试；
- 刷新、断线、重连和版本 Gap 有明确行为；
- Mock 和 Staging 的状态结构一致；
- 操作结果不会因为重复点击、重复响应或超时而重复改变 UI；
- 至少有一条从客户端日志追到服务端 `operation_id` 的完整链路。

满足以下条件，才开始微信接入：

- L2 测试链路稳定；
- 已确定非个人主体的小程序账号；
- 已准备可控制的 HTTPS 业务域名；
- H5 的 `Runtime`、`AuthProvider`、`ShareBridge` 没有业务代码分支污染；
- 有 iOS/Android 真机回归计划。

---

## 14. 第一批实现任务

按下面顺序开工：

1. 创建 `h5/` TypeScript/Vite 工程和基础脚本；
2. 冻结第一版正式 `.proto`，生成 Go/TypeScript 类型和编解码代码；
3. 选择并固定 `npm + Vitest + Playwright` 测试脚本；
4. 创建 Environment、Runtime、AuthProvider 接口和 Staging 测试登录边界；
5. 建立走真实 Protobuf 帧的 Fake Server、FakeClock 和第一批 Fixture；
6. 实现 WSS 状态机：Disconnected → Connecting → Authenticating → Authenticated → Joining → InFarm → Reconnecting；
7. 按服务端最终 Proto 和鉴权合同实现 WSS 首帧、认证响应、JoinFarm 和一次性 `connection_ticket` 核销链路；不得根据旧 Mock 猜测添加 `ClientHello/ServerHello`；
8. 创建按环境/账号隔离的 `OwnerPrivateStore`、`FarmViewStore`、`PendingOperationJournal`；
9. 实现 Snapshot/Change/CommandResult reconciler、时间偏移和 `farm_version` Gap 保护；
10. 用 DOM/CSS Grid 做最小农场画面：地块、作物占位、金币、连接状态和错误提示；
11. 加入 `BuyOffer`、`PlantCrop`、`HarvestCrop`、`SellItem`；
12. 用两个浏览器上下文做个人经营、消息乱序、Processing 恢复和并发测试；
13. 通过 Vite Proxy 接入真实测试后端；
14. 微信壳骨架可与客户端并行维护；通过 L2 后，再接入正式 HTTPS 域名、share_token 和真机验收。

这份顺序是故意把“平台问题”和“业务问题”拆开：在正式服务端联调和 H5 核心闭环证据完成之前，注册个人小程序、制作分享卡片或堆美术资源都不是最高优先级。
## 15. Latest implementation checkpoint

The H5 client and the minimal WeChat `web-view` shell are now both in the
repository. The shell performs `wx.login()`, exchanges the one-time code for a
short-lived `launch_ticket`, and opens the same H5 build. It does not own farm
state or contain platform secrets. Local evidence now includes desktop/mobile
E2E, reconnect-and-renew coverage, binary UUID operation IDs, safe uint64
guards, non-Mock build auditing, and handoff URL validation. The remaining
integration gate is the formal HTTPS/WSS/Proto server. The Gongfeng
`developer_026/farm-game` project now provides a canonical design Proto and WSS frame
specification, but its README and tree show S1 design-only status: no runnable
Gateway/Logic/Worker entrypoints or Staging endpoints are present. The exact
wire mismatch and handoff requirements are recorded in
`h5/PROTOCOL-RECONCILIATION.md`. Real-device verification in WeChat DevTools
and the Mini Program console follows the server integration gate.

Protocol audit addendum (2026-07-25): the Gongfeng `developer_026/farm-game`
project provides canonical Proto and WSS frame design, but remains an S1
design repository without runnable Gateway/Logic/Worker entrypoints. Earlier
`ClientHello/ServerHello` references in this plan describe the provisional Mock
vertical slice and must not be treated as the canonical server handshake. The
required reconciliation and handoff items are recorded in
`h5/PROTOCOL-RECONCILIATION.md`.
