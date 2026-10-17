# Tcaplus 表格上传

当前已经创建的 `farm_owner_probe / owner_probe` 只用于真实 SDK 探针，不能作为
player-svr / farm-svr 的在线业务表。

## 直接上传文件

控制台“新建表格”时，直接上传这一份自包含的业务表定义：

`deploy/tcaplus/owner_core_upload/OwnerCoreRecord.proto`

它已经把 OwnerCore 的嵌套消息展开在同一个文件中，并引用 Tcaplus 控制台内置的
`tcaplusservice.optionv1.proto`。不需要把项目其他 Proto 文件一起上传。

如果控制台要求选择历史依赖文件，才改用 canonical 源文件及其依赖：

1. `api/proto/tcapluspb/options.proto`
2. `api/proto/common/enums.proto`
3. `api/proto/economy/economy.proto`
4. `api/proto/farm/farm.proto`
5. `api/proto/settlement/settlement.proto`
6. `api/proto/ownercore/ownercore.proto`

上面的单文件上传物和 canonical 源文件字段号完全一致；后续修改表结构时应先改
`api/proto/ownercore/ownercore.proto`，再同步更新单文件上传物。

主表定义是 `ownercore.proto` 中带有
`(tcaplusservice.tcaplus_primary_key) = "owner_id"` 的 `OwnerCoreRecord`，因此
Tcaplus PB 表名应为 `OwnerCoreRecord`，主键为 `owner_id`。不要把
`deploy/tcaplus/owner_probe.proto` 当作生产表上传。

建表成功后，在探针环境设置 `TCAPLUS_OWNER_TABLE=OwnerCoreRecord`（默认值已经是该
名称），并用一个测试 owner 做 Insert/Get/CAS 验证。当前 player-svr / farm-svr /
farm-worker / farm-gate 不读取这张探针表。

验收命令：

```bash
./scripts/probe-tcaplus-owner \
  -dir 198.18.0.5:9999 -app 221 -zone <ZONE> -sig '<从本地安全配置读取>'
```

该命令使用专用测试 `owner_id`，默认结束时删除记录；加 `-keep` 才会保留。
