# Clean Join vs opportunity 2026-08-17 r2

Gate-first clean reset（revision 1 预热 Worker 后拉起 Gate 并保持，claims 时不缩 Gate）。未跑 probe 脚本。

- Farm uid `bb0e3679-02de-49c1-a240-57a1c8c7eca7`（不是中毒的 `f23c708a`）
- Gate image `244ab98e`（recovery pending-join 修复）
- Holder image `0d533888`，`DOWNLINK_BATCH_SIZE=32`，无 Holder pprof
- Redis FLUSHALL 后空库

## Join 10000 阶梯

| 档 | run | ready | disc | peak | Ready P95 | queue-full Δ | 判定 |
|---|---|---|---|---|---|---|---|
| smoke 1 | `dkqycyyy45mr` | 1 | 0 | 1 | 39.7 ms | — | PASS |
| 2500/s | `dkqyh79c1kf1` | 10000 | 0 | 10000 | **79.0 ms** | 0 | **无损 PASS** |
| 2750/s | `dkqylchc93bd` | 10000 | 0 | 10000 | 285.3 ms | 0 | 功能过、延迟超 200ms |
| 3000/s | `dkqykuj36bx2` | 9999 | 0 | 10000 | 271.6 ms | **+125** | FAIL：1 次 `join_barrier: network timeout` |

Join 极限：无损容量 **2500/s**；2750 还能全就绪但 P95 已掉出 200ms；3000 开始丢 1 个 Join 并打满 downlink queue。

## Opportunity-only thick-login 300 人阶梯

50 好友 / 密度 20%。测窗内 Gate 好友限流临时 `10000/20000`，测后已恢复 `20/40`。

| 档 | run | ready | disc | FOUND | Ready P95 | friend startup P95 | 判定 |
|---|---|---|---|---|---|---|---|
| smoke 1（无好友） | `dkqyhok4irpc` | 1 | 0 | NONE | 37.7 ms | 20.7 ms | PASS |
| 100/s | `dkqyj1l9b3us` | 300 | 0 | 300 | **263.4 ms** | **219.3 ms** | **舒适 PASS** |
| 150/s | `dkqymb8u6xqt` | 300 | 0 | 300 | 1750.5 ms | 1411.5 ms | 功能过、延迟拐点 |
| 200/s | `dkqymz2ad9yz` | 300 | 0 | 300 | 2000.3 ms | 1727.4 ms | 功能过、P95≈2s |
| 250/s | `dkqynn1a455j` | 200 | 0 | 200 | 2360.5 ms | 1966.2 ms | FAIL：100 个 `friend opportunity unavailable` 503 |

Opportunity 极限：舒适容量 **100/s**；150–200 仍能 300/300 FOUND，但 Ready/好友启动 P95 已到 1.4–2.0s；**250/s** 硬失败。对照脏库+pprof `dkqvj3yzpuf6` 在 100/s 就已 ~1630/1473 ms。
