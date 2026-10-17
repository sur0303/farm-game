# 本地 Docker 运行

本目录提供三条主要入口：

- `docker-compose.probe-only.yml`：只启动 TCONND、gateproxy 和 `tconnd-probe`，先验收
  GateStream 边界；
- `docker-compose.local.yml`：完整 H5 联调栈，直接连接公司持久 Redis，不启动本地 Redis。
- `docker-compose.yml`：核心业务编排，使用共享 DevCloud Redis，不启动本地 Redis 容器。

真实配置放在已被 Git 忽略的 `secrets/redis.conf.local`。单节点可以填写 `host`、`port`、
`password`；三主三从 Redis Cluster 应填写 `cluster_urls`，至少提供三个可达 seed，
并保持每个 URL 使用数据库 `0`。客户端会从 seed 自动发现六个节点的拓扑，副本不会被
硬编码成业务路由。推荐通过包装脚本加载，
避免把密码写入仓库、提交的 `.env` 或命令行：

```bash
cd ..
./scripts/prepare-local-compose.sh
./scripts/company-redis-compose.sh config --quiet
./scripts/company-redis-compose.sh up -d --build
```

`docker-compose.local.yml` 不再创建一次性 `redis:7.4-alpine`。业务数据使用
`farm:v118:primary:*` 前缀，具体决策见
[`31-persistent-redis-authority.md`](../docs/design/31-persistent-redis-authority.md)。

Redis 连接配置优先级为：`FARM_*_REDIS_CLUSTER_URLS`（或通用
`FARM_REDIS_CLUSTER_URLS`）→ `FARM_*_REDIS_URL` → 通用 `FARM_REDIS_URL`。
Cluster 模式使用 `redis.NewClusterClient`，不支持 Redis logical DB；所有业务 key
通过 hash tag 固定同一原子操作的 slot，Lua/CAS 不得跨 tag 组合 key。

TCONND/gateproxy 是平台镜像，不能用项目自建适配器替代。当前工作区发布记录中的
镜像线索是：

```bash
export TCONND_IMAGE=mirrors.tencent.com/mfcn/tconnd:0.0.13
export GATEPROXY_IMAGE=mirrors.tencent.com/mfcn/gateproxy:0.0.34
```

实际使用前先执行：

```bash
cd /data/home/developer_026/CodeBuddy/20260519141303/farm-game
scripts/probe-tconnd-assets
```

若提示 registry 权限，需要先完成平台镜像仓库登录。当前 `tconnd_gcp.xml` 是
BY_WS 候选配置，不代表目标镜像已经支持该 parser。

镜像可用后，按顺序运行：

```bash
cd deploy
./tconnd-probe/build-image.sh
docker compose -f docker-compose.probe-only.yml up --build
docker compose -f docker-compose.local.yml up --build
```

`tconnd-probe` 在宿主机编译后再装入 scratch 镜像，生成的 ELF 已被 `.gitignore`
排除。这样 Docker 构建阶段不需要访问 `git.internal.example.invalid`，仓库也不会提交平台相关的
本机二进制。

H5 本地 staging 配置位于 sibling 项目的 ignored 文件
`classic-farm-h5/.env.staging.local`，API 走 `localhost:8080`，WebSocket 直连
`ws://localhost:9980/ws`。

业务镜像运行层显式安装了 `wget`，因此 Compose 的 `/healthz`、`/readyz` 健康检查
不依赖 Alpine 基础镜像是否暴露 busybox 的同名 applet。

## HTTP Session Cookie 密钥轮换

Gate 与 farm-svr 共享的 `PERMIT_SECRET` 必须由 Secret 管理系统注入，至少包含 32 字节
随机材料；缺失、过短和仓库已知开发值都会在打开监听端口前终止进程。Authority 模式不接受
开发逃生开关。未配置 Cookie KeyRing 时，Gate 仅从已经通过这项启动校验的 Permit Secret
做用途隔离派生。

本地 Compose 显式选择 `ACTOR_AUTHORITY_MODE=legacy`，因此可以使用其醒目的开发占位值，
启动日志会标记 `insecure_development=true`。不使用显式 legacy 模式的临时演示必须同时显式
设置 `PERMIT_SECRET_ALLOW_INSECURE_DEVELOPMENT=1`；该开关不能出现在生产配置中。

RT2I 路由组合默认仍是 `ACTOR_ASSIGNMENT_MODE=static`；需要 per-shard Holder
membership 时叠加 `docker-compose.membership.yml`，仍保持 composed+static。这个静态 overlay
不会构造 HolderRegistry/Agent/HolderControl，因此不要求 Holder TLS，也不发送零分片 Holder 心跳。经过认证的
remote-watch 示例位于 `docker-compose.remote-watch.example.yml`。该示例现在要求显式
`kubernetes-process-v1` 以及可信来源提供的 PodUID、WorkloadUID 和 canonical ordinal；这些
Compose 变量仍只是 Holder claims，不是 Kubernetes owner-reference 证明，也不能替代未来
ScalePlan Adapter 对真实 Pod/StatefulSet API 的独立 join。它会实际构建并启动
`route-controller`，四个消费者只有在 controller 监听健康后才启动。该 controller 必须是单实例：
文件同时用固定 `container_name`、`deploy.replicas=1` 和 `stop-first` 更新策略表达约束，禁止使用
`--scale route-controller`；在另行评审 HA/leader 合同前不能移除这些限制。controller 启动前还要求
Redis 中已有 durable snapshot。合并 RT2M 后，启动还必须携带固定 literal
`ROUTE_CONTROLLER_SINGLE_CONTROLLER_ATTESTATION=single-replica-recreate-no-overlap-v1`；部署系统必须
以 Recreate/no-overlap 语义生成新的 `ROUTE_CONTROLLER_BOOT`，并保证该 Boot ID 在一个 controller
generation 内不可变。`ROUTE_CONTROLLER_PLAN_OPERATION_TIMEOUT` 默认 `30s`，而
`stop_grace_period` 默认 `45s`；任何覆盖都必须保持 grace 至少为 operation timeout 加有界清理余量。
`ROUTE_CONTROLLER_PLAN_TTL`/`--plan-ttl` 控制 Executor 新建 Plan 的逻辑 expiry horizon，并以同一个
immutable 值配置生产 Redis Plan store 的 lifetime 上限；未设置时默认 `24h`、最大 `7d`，并且按
Executor 的毫秒精度必须严格大于 effective operation timeout 加 `1s` safety。显式空值、畸形、`0`、
负值、溢出、越界或不足的 TTL 都会在启动前失败，不会静默回退；remote-watch Compose 特意使用
`${ROUTE_CONTROLLER_PLAN_TTL-24h}` 保留显式空值。RT2Q 短周期运行可设置
`ROUTE_CONTROLLER_PLAN_TTL=120s`（默认 `30s` operation timeout 满足该约束）。Redis store 的其余
默认值和语义保持不变，尤其是 audit retention 不从 Plan TTL 派生或覆盖。

完整全表的 static liveness 观测是独立 sibling profile：默认
`ROUTE_CONTROLLER_LEASE_PROJECTION_MODE=disabled`；只有所有 shard 必须持续 ACTIVE 的 static profile
才可显式设置 `all-active`（本地 capacity overlay 已固定该值）。dynamic migration profile 必须保持
`disabled`，避免合法 ASSIGNING 阶段被 static all-active readiness 合同阻塞；它使用下述 Manager deadline
reconciliation 与 Plan recovery。

长期 ACTIVE 路由还依赖单 controller 将 Holder 直接 Renew 后的同 generation Authority deadline
批量投影为新的完整 Snapshot。`ROUTE_CONTROLLER_DEADLINE_REFRESH_INTERVAL`（默认 `1s`、最大
`30s`）控制唯一 recovery/execution owner 的检查 cadence；
`ROUTE_CONTROLLER_DEADLINE_REFRESH_LEAD`（默认 `30s`、最大 `59s`）控制进入 refresh window 的
投影阈值。lead 必须严格大于 interval、Redis operation timeout 与 `5s` Watch/apply headroom 之和；
显式空值、非 canonical duration、非正数或越界值均在启动前失败。remote-watch Compose 显式固定这
两个值。该默认 lead 对应同一示例中已验证的 Holder lease profile：TTL=`60s`、safety
margin=`10s`、renew interval=`40s`；所有 Holder 角色必须使用相同 profile，任何覆盖必须继续满足
`poll + exact reads + CAS/publish + Watch/apply + jitter < lead < TTL`，不能只改单侧。示例还将
`ROUTE_CONTROLLER_METRICS_LISTEN=0.0.0.0:9090` 显式绑定，并在 `/metrics`
暴露 `route_deadline_refresh_*`、`route_snapshot_min_remaining_ms` 与
`route_deadline_projection_lag_ms`；生产监控必须告警 refresh error/CAS conflict/read error 增长及最小
remaining 进入传播预算。refresh 只读取当前完整 Snapshot 中 ACTIVE shard 的精确 Authority key，任意读取、generation
验证或完整 Snapshot CAS 失败都会整批 fail closed；ASSIGNING 仍由 Plan/recovery pipeline 独占。
请求 Resolve 热路径继续只读进程内不可变 Snapshot、远程调用数为零。当前 single-controller 合同下，
controller 若在 Snapshot lease horizon 内不能恢复，消费者会在本地 deadline 到期后按设计 fail closed；
因此该能力不是 HA，也不能作为放宽单副本 Recreate/no-overlap 的依据。

RT2X 已补齐 stop-first/Recreate 的 consumer Boot rollover 边界，但不放宽上述部署纪律：每个新
controller process generation 仍必须使用从未复用的新 Boot。普通 initial/full unary 和所有活跃
Watch 始终钉住当前 Boot；只有一个已经建立的 Watch 完整关闭并在 client 注销后，
RemoteAssignmentPump 的 full resync 才可调用 restart-aware fetch。新 Boot 只有在 controller ID、
realm、topology、revision floor 和完整 envelope 检查全部通过后才会原子替换 pin，旧 Boot 同时进入
有界 retired set。production consumer 通过 `ROUTE_CONTROL_RETIRED_BOOT_LIMIT` 配置容量：未设置时
默认 `8`、硬上限 `64`，任何显式的空值、畸形、`0` 或越界值都会使启动失败。容量耗尽会
terminal fail closed、移除 consumer readiness，且不会驱逐或忘记旧 identity，也不会在进程内静默重置。
因此默认配置必须在第九次 rollover 前协调替换/重启 consumer process；仅 controller restart 不能恢复
已经耗尽的 consumer。相同 Boot reconnect 保持原语义，Resolve 热路径
不增加远程调用。该 code/component closure 尚未执行 RT2Q Final 多 Pod controller restart runtime
evidence，因此 RT2Q 仍为 pending，不能据此宣告 Route Control MVP runtime acceptance。

controller 的三组入站身份彼此独立：`ROUTE_CONTROLLER_ALLOWED_CLIENT_IDENTITIES` 只授权 snapshot
客户端，`ROUTE_CONTROLLER_PLAN_READ_IDENTITIES` 只授权 plan read，
`ROUTE_CONTROLLER_OPERATOR_IDENTITIES` 才授权 routectl mutation。三者都只能匹配已经由
`ROUTE_CONTROLLER_TLS_CA` client CA 验证的 TLS leaf DNS SAN；不信任自报 identity、CN 或未验证 SAN，
plan-read identity 不能执行 mutation。routectl operator 证书同样必须由该 client CA 验证，并以
operator exact identity 授权。

controller 调用 Holder 使用独立的出站文件
`/run/farm/holder-operator/{client.crt,client.key,holder-ca.crt}`。示例把真实 cert/key/CA 从被 Git
忽略的 `deploy/secrets/holder-operator-*.local` 以只读 volume 挂载；不要把 PEM 内容写入 Compose、
环境变量或日志。client leaf 必须恰好有一个 DNS SAN，且 exact 等于 player-svr 与 farm-svr 共同使用的
`HOLDER_CONTROL_CONTROLLER_IDENTITY` allowlist 值；两个 Holder root 不得配置成不同 controller
identity。

controller 调用每个 traffic-serving farm-gate 的只读 verifier 也使用第三套独立凭据：
`/run/farm/gate-verifier/{client.crt,client.key,server-ca.crt}`。每个 remote-watch farm-gate
在 `GATE_VERIFIER_LISTEN` 提供内部 unary observation，并从独立的
`GATE_VERIFIER_{TLS_CERT,TLS_KEY,CLIENT_CA}` 文件完成 TLS 1.3 mTLS；允许的 controller
leaf DNS SAN 由 `GATE_VERIFIER_ALLOWED_CLIENT_IDENTITY` 精确指定。Verifier 不复用 Holder
证书、业务 listener 或业务 endpoint，也不接受 CN、IP、URI、多 SAN、intermediate identity
或 metadata 自报身份。

`ROUTE_CONTROLLER_GATE_VERIFIER_PEERS` 必须是非空、有界的 JSON direct-peer 列表，并覆盖
每个 traffic-serving Gate。每项都必须包含唯一 name、直接 `host:port`、`server_name` 和与其
相同的 exact verified leaf DNS identity；不能用一个 load-balanced 地址代替逐 Gate observation。
增加 Gate replica 前必须先为新实例提供可直连的 verifier 地址并原子更新该列表。缺少、重复、
畸形、stale、ahead、unhealthy 或 unavailable 的任一 peer 都使 `gate_resolution` fail closed。
当前示例只列出一个固定 farm-gate；这不是 autoscaling 合同。

RT2M 的 Redis 物理合同也显式复用业务部署值：
`ROUTE_CONTROLLER_REDIS_PHYSICAL_PROFILE <- REDIS_PHYSICAL_PROFILE_VERSION`、
`ROUTE_CONTROLLER_AUTHORITY_LAYOUT <- REDIS_AUTHORITY_LAYOUT_VERSION`、
`ROUTE_CONTROLLER_WORKFLOW_LAYOUT <- REDIS_WORKFLOW_LAYOUT_VERSION`、
`ROUTE_CONTROLLER_WORKFLOW_BUCKET_COUNT <- REDIS_WORKFLOW_BUCKET_COUNT`。这些是非 secret 的精确布局值；
Redis URL 仍只由 Secret/部署环境注入。

Wave 3 RT2W unified closure 已在下列独立 signed commits 完成：

- `7fb4b7ed0ff560c51df3eadaf9cce03fda4d095d`：Logic 与 HolderControl endpoint 分离；
- `79c82db4381b6d759df4dd3aca91f14178622539`：所有 remote role 的完整 Redis tuple/fingerprint；
- `2ac16ffb33d3efafe8e16bfc0dedc3dfca4b918b`：真实 production VerificationPort、Manager
  durable evidence、exact Redis observation 与逐 Gate mTLS observation。

因此 production command root 现在必须注入 non-nil/non-noop 的真实只读 VerificationPort；
`gate_resolution`、`transition`、`recovery`、`tombstone` 四项仍保持独立，任何依赖缺失或事实不精确
都会 overall false。这里的“composed”仅表示真实 adapter 已接线并通过 unit/component、real loopback
mTLS 和 exact company-Redis probe；它不等于 RT2Q Final 或 multi-Pod runtime evidence。后者仍待独立
终验，controller 仍是 single-replica non-HA，Gate autoscaling 仍无批准证据。

`farm_session` 使用版本化 HMAC KeyRing。密钥只能由环境 Secret 注入：

```text
HTTP_SESSION_COOKIE_ACTIVE_VERSION=2
HTTP_SESSION_COOKIE_KEYRING=2=<base64url-new-key>,1=<base64url-old-key>
```

每把 key 解码后至少 32 字节。进程不会记录密钥或 Cookie 正文。滚动轮换顺序：

```text
1. 所有 Gate 先部署 old+new KeyRing，active 仍为旧版本
2. 所有 Gate 都能识别新版本后，将 active 切到新版本
3. 等待至少 Cookie TTL + 最长滚动发布窗口
4. 从所有 Gate 的 KeyRing 删除旧版本
```

第 1 步中的旧 key 必须与当前版本实际签名 key 完全一致。首次从历史“Permit Secret 派生
Cookie key”迁移时，应在 Secret 管理工具中按
`HMAC-SHA256(key=PERMIT_SECRET, message="farm-game/http-cookie/v1")` 生成并 base64url
编码 v1 key，不能直接换成随机 v1，否则现存 Cookie 会立即失效。显式 legacy/开发模式未
配置 KeyRing 时仍保持原派生行为。

## Web 账号登录限流

账号密码登录和注册都会在 PBKDF2 之前经过账号、客户端 IP、设备三个独立 Token Bucket：

```text
account：WEB_ACCOUNT_MAX_FAILURES / WEB_ACCOUNT_FAILURE_WINDOW
ip：     WEB_LOGIN_IP_BURST / WEB_LOGIN_IP_WINDOW
device： WEB_LOGIN_DEVICE_BURST / WEB_LOGIN_DEVICE_WINDOW
```

默认分别为账号 `5/5m`、IP `30/1m`、设备 `10/1m`。账号认证或注册成功后只清空账号桶；
IP 和设备桶继续统计总 KDF/注册压力。设备 ID 来自 H5 的稳定浏览器 ID，但它可被客户端修改，
只能作为风险信号，不能用于认证、路由或资产归属。

配置公司 Redis 时，三个桶由所有 Gate 共享。每个维度使用独立的单 key Lua 和 Redis `TIME`：

- 多 Gate 不能靠切换实例绕过；
- 不把所有登录流量放入同一个 Redis Cluster slot；
- Pipeline 只减少网络往返，不把三个维度伪装成跨 slot 事务；
- Redis 失败时登录/注册 fail-closed 为 503，避免绕过昂贵 KDF 前的保护。

原始用户名、IP 和设备 ID 不进入 Redis key；Gate 使用用途隔离的 HMAC 摘要。生产应注入至少
32 字节的：

```text
WEB_LOGIN_RATE_LIMIT_SECRET=<high-entropy-secret>
WEB_LOGIN_RATE_LIMIT_PREFIX=farm:prod:web-login-limit
```

未配置专用 Secret 时，为兼容本地环境，从已通过启动策略校验的 `PERMIT_SECRET` 做用途隔离
派生；正式环境不得配置仓库开发值或不安全开发开关。

只有对端地址命中 `GATE_HTTP_TRUSTED_PROXIES` 时才采纳转发头。XFF 从右向左剥离可信代理，
避免代理采用 append 策略时让客户端伪造左侧地址绕过 IP 桶。显式配置非法 CIDR 时不会回退为
信任其他代理。

开启 `GATE_WS_METRICS=1` 后，`/metrics` 同时暴露固定低基数指标：

```text
farm_gate_account_auth_attempts_total
farm_gate_account_auth_limited_total
farm_gate_account_auth_retry_after_seconds
```

指标标签只含 `login/register/change_password`、`allowed/limited/error`、`account/ip/device`，不含原始标识。

## 修改密码与管理员禁用

```text
POST /v1/accounts/change-password
POST /api/v1/accounts/change-password
```

请求体：`username`、`current_password`、`new_password`。成功后会：

1. CAS 更新 password verifier；
2. 调用无条件 `SessionAuthority.Revoke(player_id)`；
3. 清除当前浏览器 `farm_session` Cookie，强制重新登录。

管理员禁用账号（封禁）需要共享 token，不会开放给玩家 Cookie：

```text
WEB_ACCOUNT_ADMIN_TOKEN=<high-entropy-secret>
POST /v1/admin/accounts/disable
POST /api/v1/admin/accounts/disable
Header: X-Farm-Admin-Token: <token>
# 或 Authorization: Bearer <token>
Body: {"username":"farmer_one"}
```

未配置 `WEB_ACCOUNT_ADMIN_TOKEN` 时禁用路由不会注册。禁用后登录统一返回无效凭据，
并同样无条件 Revoke 全部会话。

## ClientHello 前的 PRE_AUTH 资源边界

HTTP Upgrade 或 TCONND START 只代表 transport 已建立，不能让连接在未认证状态无限占用 Gate
内存和调度资源。Direct WebSocket 与 gateproxy 共享同一个进程内 PRE_AUTH Controller：

```text
GATE_PREAUTH_MAX_CONNECTIONS=4096
GATE_PREAUTH_MAX_CONNECTIONS_PER_IP=64
GATE_PREAUTH_MAX_FRAMES_PER_CONNECTION=1
GATE_PREAUTH_MAX_BYTES_PER_CONNECTION=65536
GATE_PREAUTH_MAX_BYTES_PER_IP=1048576
GATE_PREAUTH_MAX_BYTES_POD=67108864
GATE_PREAUTH_HELLO_TIMEOUT=10s
```

只有成功完成并激活 `ClientHello` 才会释放 PRE_AUTH 预算并进入正常 SessionLane。超时、断开、
非法首帧、第二个未认证 FarmFrame 或字节超限都会释放完整 `(connection_id, generation)`
Fence。gateproxy 的失败只关闭对应 TCONND 连接，不关闭承载其他玩家的 GateStream。

这些值只是防止无界占用的安全默认，不是 3000 万 DAU 容量结论；上线前应按 CLB NAT 分布、
真实 ClientHello 大小、连接建立峰值和 Pod 内存压测调整。无法解析的来源 IP 仍消耗 Pod
预算，但不会创建可由客户端任意控制名称的 IP bucket。

开启 `GATE_WS_METRICS=1` 后同时暴露：

```text
farm_gate_preauth_connections
farm_gate_preauth_bytes
farm_gate_preauth_rejected_total{reason=...}
farm_gate_preauth_transitions_total{result=...}
farm_gate_preauth_duration_seconds
```

标签只使用固定的拒绝原因和退出结果，不含 IP、连接 ID 或玩家 ID。

Redis Lua 的跨 Gate 探针默认跳过；需要显式授权并提供隔离测试环境：

```bash
FARM_REDIS_LIVE_TEST=1 go test ./internal/gateway/account \
  -run TestRedisAttemptLimiterSharedAcrossGateInstancesLive -count=1
```

探针只创建随机 `farm:test:web-login-limit:*` key，结束时精确删除，不执行 `KEYS` 或清库。

本地 Compose 默认使用 MemoryStore；Tcaplus 相关连接仅由独立探针验证，
不属于当前 player-svr / farm-svr / farm-worker / farm-gate 业务编排。

## R2 P1-A/B 静态路由 Kubernetes Staging candidate

静态路由 Kubernetes 候选位于 [`k8s/static-staging`](k8s/static-staging/README.md)。
它只提供可渲染、可结构校验的 manifest，不代表已经部署或通过 Staging 验收；
需要先补齐不可提交的 Secret、镜像 digest、受控 Redis、TLS 和正式 URL。
