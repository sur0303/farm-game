# qqfarm recovery-marker Gate 修复对照（2026-08-17）

只换 Gate 镜像，Holder / Redis / Worker 未重建。新镜像从现网 `0d533888...9ec026` 抽出 Player/Farm/Worker/Controller，只重编带 pending-Join 收下 `ClientRecoveryComplete` 的 `farm-gate`。

- Gate image：`farm-route-runtime@sha256:244ab98e60699b8bdcde102c1fdb266003806a517b4b8462422554fdc22fc741`
- Farm/Player 仍是：`farm-route-runtime@sha256:0d53388836c45fb5ee8de7e38cfee5a78b0e87cde0f97ac0a0dd10295b9ec026`
- Gate Pod：`farm-gate-57cd676cbc-cfmxk`，pprof 关

| 档 | Ready | P95 | Snapshot P95 | disc | peak | RECOVERY_INCOMPLETE | run |
|---|---:|---:|---:|---:|---:|---:|---|
| 修复前 2500（同拓扑，旧 Gate） | 10000 | 76.4 ms | 24 ms | **49** | 9951 | **49** | `dkqxet5svbzv` |
| Join smoke | 1 | 62 ms | 20 ms | 0 | 1 | 0 | `dkqxtzqgjfjo` |
| 修复后 2500 | 10000 | 150.9 ms | 60 ms | **0** | **10000** | **0** | `dkqxudo48ect` |

`handle: gate: recovery incomplete` 日志 0；`slow_client_kick` 0。断线根因已消掉。本档 P95 高于上一档，Gate 刚滚到新节点，不作为容量结论。
