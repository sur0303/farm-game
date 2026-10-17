# r14 bootstrap-readonly + MAX_CONCURRENT_ACTOR_LOADS=2000

- 日期：2026-08-17
- challenge：`d3188d437d62366e92eed26c884ef7ac`
- Player image：`farm-route-runtime@sha256:20bce078…4060a64`（`bootstrap-readonly-20260817`）
- `MAX_CONCURRENT_ACTOR_LOADS=2000` / `MAX_PENDING_ACTOR_LOADS=4096`
- Gate friendship：`10000/20000`
- Bot pool：`950000000–950004999`（5000）

## 变更相对 r12

- `GET_PLAYER_BOOTSTRAP` 改为 Read-only（无 CAS / 无 welcome mail SourceOrder）
- Actor load 并发上限 2000

## thick-login 阶梯（3000 玩家 / 60s）

| 档 | run | player-start | ready | Ready P95 | boot_err | friend_err | FOUND | 判定 |
|---|---|---:|---:|---:|---:|---:|---:|---|
| 200/s | `dkr8kdf7vrxm` | 950000000 | **3000** | **46.9 ms** | 0 | 0 | 3000 | **PASS** |
| 1000/s | `dkr8n1gwuyjr` | 950002000 | **887** | 5667 ms | 742 | 1158 | 887 | **FAIL** |

### 对照 r12（welcome-on-bootstrap）

| 档 | r12 ready | r14 ready | r12 boot_err | r14 boot_err | r12 friend_err | r14 friend_err |
|---|---:|---:|---:|---:|---:|---:|
| 200/s | 3000 | 3000 | 0 | 0 | 0 | 0 |
| 1000/s | 304 | **887** | 2371 | **742** | 325 | 1158 |

1000/s 仍 FAIL，但 Bootstrap 错误从 2371→742，Ready 从 304→887。主失败变成好友机会 503（1158）+ Bootstrap TIMEOUT/BUSY（587+155）+ Join BUSY/stale（129+24）。

### 1000/s 失败分布（`dkr8n1gwuyjr`）

```text
1158  friend opportunity 503
 587  bootstrap ERR_TIMEOUT
 155  bootstrap ERR_SERVER_BUSY
 129  join actor busy
  48  network timeout
  24  join stale route
  12  REJOIN_REQUIRED
```

### 可读信号

- 200/s：`mail_red_point_players=0`（r12 同档为 3000）→ welcome mail 已不挂在 Bootstrap。
- 1000/s：`profiles_loaded=2045` 而 `ready=887` → Bootstrap 读通的人多于完整 Ready；卡在 friend opportunity / Join。
- Farm `queue-full=0`；测后 Player/Farm/Gate restart=0，仍 4 shard。

## 结论

readonly Bootstrap + load=2000 **改善了 1000/s 的 Bootstrap 吞吐，但没有把 thick-login 推到无损**。下一瓶颈更偏好友机会 503 与 Join/actor busy，而不是单纯 LoadAndClaim 排队。
