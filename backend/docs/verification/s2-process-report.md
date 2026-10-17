# S2 多进程纵向验证报告

日期：2026-07-26

状态：真实 Tcaplus OwnerCoreRecord 与 farm-gate/farm-logic 跨进程登录建档链路已通过；真实 TCONND/gateproxy GateStream 仍按 [TCONND 探针报告](./tconnd-probe-report.md) 单独验收。

## 覆盖范围

`scripts/test-s2-process` 启动两个独立进程：

```text
HTTP client → farm-gate → gRPC BootstrapService → farm-logic → Tcaplus OwnerCoreRecord
```

脚本使用专用 owner，开始前清理记录，结束时再次清理；集群签名只从未提交的
`deploy/secrets/tcaplus.conf.local` 读取，不打印到终端或日志。

## 已验证

- farm-logic 显式打开 Tcaplus `OwnerCoreRecord`（App 221 / Zone 1）；
- farm-gate `/readyz` 等待远程 Logic gRPC channel Ready；
- `/api/v1/dev/session` 使用测试平台票据完成 HTTPS 登录；
- Login → BootstrapService → OwnerCoreRecord 条件建档成功；
- 使用相同 `bootstrap_id` 的第二次登录成功且不重复 Insert；
- 响应包含十进制字符串 `player_id`、一次性 `connection_ticket`、Session epoch 和 `wss_endpoint`；
- 记录使用 `OwnerCoreRecord` 的真实 schema，初始农场包含 12 个空地块；
- SDK `txhdb_record_not_exist` 被正确归一为 `ErrOwnerNotFound`，不会误判为 `ErrOwnerExists`。

## 可重复命令

```bash
S2_PROCESS_OWNER=880000000000003 ./scripts/test-s2-process
```

该命令只验证到 farm-gate 的登录/建档边界；ClientHello、ServerHello、Join、下行和重连
由 `scripts/test-s2-e2e` 使用 staging Playwright 配置执行，但该命令会 fail-closed 要求
真实 TCONND 探针报告、部署 URL 和可用证书/镜像。
