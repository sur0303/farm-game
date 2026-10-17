# 51 · TcaplusDB 最小验证(连真库)

对应交接文档任务六(后半)。程序:`cmd/tcaplus-verify/main.go`,测试表 proto:`cmd/tcaplus-verify/verifypb/probe.proto`。目的:把交接文档 §9.6/§17.2 列为"待平台确认"的 SDK 语义**用真库实测**,并采集写放大/延迟回填容量模型。

真库:集群 `developer_026-1784962020844MzD`,dir `198.18.0.5:9999`,接入 ID 221（凭据只保存在被忽略的 `deploy/secrets/tcaplus.conf.local`）。SDK:`git.internal.example.invalid/gcloud_storage_group/tcaplus-go-api@v0.6.30`。

---

## 1. 验证项(对齐交接文档 §17.2)

| 项 | 程序检查 | 状态 |
|---|---|---|
| Dial + 鉴权 | 用 app/zone/signature 连 dir 服务 | ✅ 已连通并通过鉴权(见 §3) |
| Insert / Get | 写入并读回 OwnerProbe | 待建表后运行 |
| RecordVersion 条件写(CAS) | 正确版本 Update 成功;旧版本 Update 应被拒 | 待建表后运行 |
| SchemaFree HashMap/HMSet 或 FieldUpdate | 一次更新 PlayerEconomyModel 的 Wallet/Inventory/ShopCounter 多字段；验证全成或全败、返回版本与 writer_epoch fencing | 待新增 probe 表/程序 |
| 异步回写乱序保护 | version=10 保存期间产生 version=11；禁止 v10 晚完成覆盖 v11，验证单 in-flight + captured_version 合同 | MemoryStore 先测，真库待测 |
| 写放大 / 延迟 | 16/32/64/128KB blob Replace × N,测 P50/P99 | 待建表后运行 |
| 写成功持久确认点 / 切主回退 | (需平台配合切主演练) | 待安排 |
| PendingWork 发现能力 | 确认二级索引/条件查询/变更流/有序分页能力，验证能否按 pending_at 分片发现且不扫全表 | 待平台文档与真环境探针 |

CAS 语义(已从 SDK 源码确认):`option.PBOpt{Version: v, VersionPolicy: CheckDataVersionAutoIncrease}` —— 仅当 `v>0` 且与服务端版本一致时写入成功,并使服务端版本自增 1;不一致则拒绝。这分别是 PlayerCore 与 FarmCore 单记录乐观锁所需；两条记录之间没有事务。

Player 普通软资产改为异步增量回写后，真库探针必须额外冻结：

1. HMSet/FieldUpdate 是否能把一个 PlayerEconomyModel 的所有字段作为一次原子写；
2. 若不同 Hash Field 可能部分成功，则 Wallet/Inventory/ShopCounter 必须编码为一个物理 value；
3. 增量写能否同时校验 `writer_epoch`，防止旧 Holder 回写；
4. 保存成功返回的版本如何与 Actor `captured_version/saved_version` 对齐；
5. 单 Player 单 in-flight 的吞吐、同 Player 多次修改合并率和批量字段大小；
6. Tcaplus 超时结果未知时，如何读取并判断 captured_version 是否已经写入。

PendingWork 探针必须额外回答：PlayerCore/FarmCore Outbox 已提交但发现索引尚未登记时，平台是否存在可重放变更来源或可分页补偿查询。任何独立索引/Redis ZSet 都不能在没有补偿机制时被当作唯一可靠工作来源。

## 2. 前置:建表

程序操作测试表 `owner_probe`(主键 `owner_id`,含 `counter` + `blob`),结构见 `verifypb/probe.proto`。**需先在集群控制台**:
1. 创建一个表格组(zone),记录其 **zone id**;
2. 在该 zone 下按 probe.proto 建表 `owner_probe`(generic/PB 表)。

## 3. 已验证到的程度(2026-07-25)

- SDK 已集成进本模块并 `go build ./cmd/tcaplus-verify/` 通过。
- dir url 需带 `tcp://` scheme(程序已自动补全)。
- 用 app=221、signature=集群密码 连接 `tcp://198.18.0.5:9999`:**dir 服务响应并通过鉴权**,返回 `-2583 (DIR_ERR_IS_NOT_THIS_ZONE_API)`——即所填 zone id 不属于该 app。这证明**凭据与网络链路 OK**,只差正确的 zone id 与已建的表。

## 4. 运行

```sh
go run ./cmd/tcaplus-verify \
  -dir 198.18.0.5:9999 -app 221 -zone <真实ZONE> -sig '<从本地安全配置读取>' -table owner_probe
```

未提供 `-sig`/`-zone` 时程序打印建表与运行指引后退出(不阻塞)。

## 5. 实测数据(回填)

> 运行成功后，把完整 Replace、PlayerEconomyModel 增量 HMSet/FieldUpdate 与 Farm CAS 分别测量。旧 D4 典型记录约 1.8KB 只能作为字段量参考，不能继续统一用 16KB Replace 代表所有写路径。

| 写路径/载荷 | 记录≈KB | P50 | P99 | 备注 |
|---|---|---|---|---|
| Player 增量 1/2/4/8KB | — | 待测 | 待测 | HMSet/单字段回退分别测 |
| Farm CAS 4/8/16KB | — | 待测 | 待测 | 权威热记录 |
| 16KB Replace | — | 待测 | 待测 | 对照档 |
| 32KB | — | 待测 | 待测 | |
| 64KB | — | 待测 | 待测 | |
| 128KB | — | 待测 | 待测 | 病态尾部 |

## 6. 验收

程序 `go build` 通过并能真实连接 dir 服务鉴权成功;CRUD/CAS/延迟实测在建表 + 提供 zone id 后一次运行产出。运行时逻辑正确性由真库返回码判定,程序对每项打印 `[OK]/[FAIL]/[WARN]`。
