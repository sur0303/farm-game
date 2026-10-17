# qqfarm downlink batch=64 Join A/B

- 日期：2026-08-17
- namespace：`developer_026-qqfarm`
- challenge：`3dd0d2b359b211fbfb149d76ae55bd64`
- FarmSvr：`DOWNLINK_BATCH_SIZE=64`

## Deploy

Skill 两阶段 bootstrap：claim 前预热 Worker A/B。

- bootstrap-only：`COMMITTED` revision 1
- assign：`ALREADY_COMMITTED` revision 13，12 claims
- Join smoke：`dkqwr2u7qpwu`，1/1 Ready，错误 0

## 2500/s control

- run：`dkqwrtrupqtd`
- player-start：`1786938500000`
- Ready 10000/10000
- P50/P95/P99：55.08 / 222.35 / 252.89 ms
- queue-full Δ：0
- unexpected_disconnects：24
- peak_active：9976
- 判定：FAIL（断线 + P95>200ms）
- 未上探 3000/s

测后 smoke `dkqwsbmy61jo` PASS，`--require-ready` PASS。
