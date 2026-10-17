# r5 Redis 6GB + 3000×60 thick-login

Redis `maxmemory 6gb` / Pod limit `8Gi` / request `2Gi`。emptyDir `8Gi`。无持久化，扩容后重建 3m3s cluster 并 Skill clean reset。

## 前置数据（fixture 已写入）

10 组 × 300 viewer × 60 好友，密度 20%。viewer `937000000–937002999`，fake `7860000000+`。

每组：`activated_pairs=18000`，`index_rows_applied=36000`，`farm_records_seeded=60`（Header+Facts），`initialized_players=60`。合计 18 万 Pair、36 万 Index 行、600 份农场 Header/Facts。Replica 由 master 同步，未单独写 slave。

写入后三 master 约 `450–460MB / 6GB`。

## Smoke

| 档 | run | ready | 判定 |
|---|---|---|---|
| Join 1 `1787400000000` | `dkr2chk9j60p` | 1 | PASS |
| thick 1 `938000000` | `dkr2clwpp7xl` | 1 NONE | PASS |

## 3000 玩家 thick-login 1000/s 60s

`player-start=937000000`，Gate 好友限流 `10000/20000`。压测器 4CPU/8Gi（8CPU/16Gi 无法调度）。run `dkr2h3m0aa0s`。

| 项 | 值 |
|---|---|
| attempted | 3000 |
| ready | **886** |
| peak_active | 1350 |
| FOUND | 886 / 0 NONE |
| Ready P95 | 7818 ms |
| friend startup P95 | 1982 ms |
| bootstrap_errors | 1770（TIMEOUT 1612 + BUSY 158） |
| friend 503 | 344 |
| queue-full Δ | 0 |
| 判定 | **FAIL** |

测后 `player-svr-0` Ready=false、restart=0、`/readyz` 无响应。Farm/Gate/Worker 仍 Ready restart=0。未原地重启 Holder。
