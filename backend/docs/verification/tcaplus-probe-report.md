# TcaplusDB S2 Probe Report

日期：2026-07-26
状态：`owner_probe` 与 `OwnerCoreRecord` 的真实 PB Insert/Get/RecordVersion CAS 已通过；结果未知/故障注入仍待专用窗口。

## 已实现

- `internal/storage/ownerstore/TcaplusStore` 使用官方 `tcaplus-go-api` `PBClient`。
- `Get` 返回记录和 SDK `PBOpt.Version`；`Insert` 使用条件创建；`CAS` 使用 `CheckDataVersionAutoIncrease`。
- SDK 错误归一为 `NOT_FOUND`、`ALREADY_EXISTS`、`VERSION_CONFLICT`、`TIMEOUT_OR_UNKNOWN`、`UNAVAILABLE`；`MemoryStore` 同时提供真实冲突和 Owner epoch 检查。
- `cmd/tcaplus-verify` 保留官方 SDK 的并发条件 Create/Get/CAS/延迟探针入口。
- `cmd/tcaplus-owner-verify` / `scripts/probe-tcaplus-owner` 验证生产 `OwnerCoreRecord`
  表的 Insert/Get/RecordVersion CAS 和旧版本冲突；建表前不会伪造通过结果。

## 真实执行状态

已使用本地未提交配置连接以下资源（凭据不写入报告）：

- Dir：`198.18.0.5:9999`
- App/接入 ID：`221`
- Zone/表格组 ID：`1`
- PB 表：`owner_probe`（表格组 `farm_owner_probe`）
- SDK：`tcaplus-go-api@v0.6.30`，运行时 Go API `3.55.0.000029.GoApi_20250922`

控制台资源记录（用户已创建）：集群 ID `19107675254`，表格组 `farm_owner_probe`，
探针表 ID `tcaplus-0788b343`；随后创建的生产表 `OwnerCoreRecord` 表 ID
`tcaplus-3235d9ca`。两张表都在 Zone 1，`farm-logic` 生产路径使用
`OwnerCoreRecord`，不能把 `owner_probe` 当作 OwnerCore schema。

执行命令形态（签名从本地安全配置读取，不要提交）：

```text
go run ./cmd/tcaplus-verify -dir <dir> -app <app> -zone <zone> -sig <signature> -table owner_probe
```

| 项目 | 状态 |
|---|---|
| 条件 Insert 并发 | 已通过：16 个并发创建者中恰好 1 个成功、15 个被拒 |
| Get/RecordVersion | 已通过：读回 `counter=0`、RecordVersion=`1` |
| CAS 成功 | 已通过：使用 version=`1` 更新成功 |
| CAS 冲突 | 已通过：复用旧 version 被 `tcapsvr_fail_invalid_version` 拒绝 |
| 结果未知判定 | 待网络故障注入 |
| 1.8/16/32/64/128 KiB 延迟 | 16/32/64/128KiB 已通过；1.8KiB 仅作为 OwnerCore 大小模型，不在当前命令中写入 |

实测延迟（`replace x50`，本次探针输出）：

| Blob | P50 | P99 |
|---:|---:|---:|
| 16KiB | 29.382478ms | 30.547423ms |
| 32KiB | 29.497394ms | 30.580053ms |
| 64KiB | 30.132630ms | 32.409299ms |
| 128KiB | 30.833641ms | 32.267160ms |

OwnerCoreRecord 专项验收（专用 owner `880000000000002`，默认已清理）：

| 项目 | 状态 |
|---|---|
| Insert | 已通过 |
| Get | 已通过，读回 `owner_meta.bootstrap_id`，`plots=12` |
| RecordVersion CAS | 已通过，初始 version=`1` |
| 旧版本 CAS | 已通过，`tcapsvr_fail_invalid_version` |

探针使用专用主键 `880000000000001`，结束时执行删除清理；本次未留下业务测试记录。

仍未完成的真实项：结果未知（超时/断连后 Get + operation marker）与切主/短时不可用故障注入；基本 PB CRUD/CAS、并发条件创建和写放大延迟已具备真实证据。

OwnerCore 专项：生产 `OwnerCoreRecord` 已在控制台创建，并已用专用 owner 完成真实
Insert/Get/RecordVersion CAS 验收；测试记录默认已删除。
