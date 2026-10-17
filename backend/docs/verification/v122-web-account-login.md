# 经典农场 Web 账号注册与登录

日期：2026-07-30  
状态：本地/独立 H5 可用；生产身份平台替换点保留

## 1. 为什么增加账号系统

原 Staging 登录只有两组固定的 `platform_token -> player_id`，适合自动化回归，
但不适合产品演示：

- 每增加一位体验者都要同时修改 Gate 环境变量和 H5 下拉框；
- 固定 token 不是用户凭据，无法表达注册、密码校验和账号恢复；
- 同一浏览器配置的窗口共享 HttpOnly Cookie，不能被误认为多个独立用户。

没有采用“前端随便填写 player_id”的方案。它虽然实现最快，但任何人都能冒充其他
玩家，会直接破坏 PlayerActor 权威、好友权限和资产安全。

当前选择：

```text
H5 注册/登录表单
  -> HTTPS POST /api/v1/accounts/register 或 /api/v1/session/login
  -> WebAccountService 校验凭据并解析稳定 player_id
  -> SessionAuthority.Begin
  -> player-svr EnsureOwnerInitialized
  -> HttpOnly farm_session + 一次性 connection_ticket
  -> WSS ClientHello
```

固定的 10001/10002 Staging 身份继续保留，只用于现有端到端测试，不再是 Web 产品
模式的账号来源。

## 2. 持久化合同

公司 Redis 当前承担账号凭据的持久化：

```text
farm:v122:primary:web-account:{web-account}:player-id-sequence
farm:v122:primary:web-account:{web-account}:username:<normalized_username>
```

注册使用 Redis Lua 原子完成“用户名不存在检查、player_id 分配、凭据写入”。默认
网页账号从 `1000001` 开始，避免与 10001/10002 演示身份冲突。用户名统一转为小写，
只接受 3–24 位字母、数字和下划线。

密码不以明文或可逆加密保存。当前记录格式为：

```text
pbkdf2-sha256$600000$<per-account-random-salt>$<32-byte-verifier>
```

每个账号使用 16 字节随机盐，比较使用常量时间函数。格式包含算法和成本参数，后续可
在成功登录时升级到 Argon2id，而不需要重置全部密码。600,000 次
PBKDF2-HMAC-SHA256 与 OWASP 当前 FIPS 场景建议一致；独立身份平台冻结时优先评估
Argon2id。

参考：

- https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html
- https://pages.nist.gov/800-63-4/sp800-63b.html

## 3. 会话与重连

账号密码只用于用户显式点击“登录”或“注册并进入”。首次成功后：

- 浏览器保存的只有 HttpOnly `farm_session`；
- `WebAuthProvider` 立即清除内存中的密码；
- 弱网重连调用 `/api/v1/auth/renew`，不重复提交密码，也不推进
  `active_session_epoch`；
- 同账号在另一个页面重新登录时，旧 WSS 收到 `KICK /
  ERR_SESSION_EXPIRED`，停止自动抢回会话。

一个浏览器配置在同一域名下只有一个当前 HTTP 登录账号，这是 Cookie 安全模型的正常
约束。H5 使用同源 `storage` 事件检测其他窗口切换账号，并让旧窗口回到登录页，避免
出现 WSS 显示账号 A、HTTP Cookie 却已经属于账号 B 的混合身份。多用户联调必须使用
独立浏览器配置、隐私窗口或 Playwright BrowserContext。

## 4. 当前防护和未冻结边界

已实现：

- 4 KiB 请求体上限和严格 JSON 字段；
- 统一的“用户名或密码错误”，不通过响应区分账号是否存在；
- 缺失账号也执行一次 dummy 密码派生，减小明显的时序差异；
- 单 Gate 内按用户名统计失败次数，5 次失败后锁定 1 分钟；
- 注册用户名唯一性和 player_id 分配由 Redis 原子保证；
- 账号成功后仍必须经过原 SessionAuthority、Owner 建档和 ticket 链路。

尚不能称为完整生产身份平台：

- 失败次数限制需要升级为 Redis/统一风控，而不是单 Gate 内存；
- 尚无常见弱密码 blocklist、找回密码、修改密码、邮箱/手机验证和 MFA；
- 公网部署必须由 HTTPS/WSS、CSRF/CORS、安全 Cookie 与审计策略共同保护；
- 最终若接企业或微信身份，应替换 WebAccountService 的认证入口，不改变后面的
  `player_id -> SessionAuthority -> Actor` 合同。

## 5. 验证结果

```text
go test ./...                                                   PASS
npm test                                                       61/61 PASS
npm run build:web                                              PASS
npm run build:staging                                          PASS
playwright --config playwright.web.config.ts                   1/1 PASS
```

真实双 BrowserContext 验证覆盖：

1. 注册两个新账号，分配不同 player_id；
2. 两个账号完成分享加好友，双方好友列表均为 1；
3. 关闭第一个注册页面后，在新 BrowserContext 账号密码登录；
4. 登录返回原 player_id，并恢复持久好友关系。
