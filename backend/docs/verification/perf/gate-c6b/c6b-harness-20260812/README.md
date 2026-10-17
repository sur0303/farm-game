# C6B Gate Capacity Harness Evidence

确定性单进程 harness 证据,针对 C6B 的四个验收维度:

1. 1% / 5% / 20% 慢客户端 cohort 隔离(慢客户端只淘汰自己);
2. Hint / FarmChange / CommandResult 混合流量下的 quota 语义;
3. Hint soft drop 后，WritePump 排空会恢复 Gate 软配额；被丢 Hint 对应的
   业务机会由既有 Navigator Pull 链补偿（本 harness 不模拟该跨服务查询）；
4. Drain quantum 行为:每量子返回 select,CommandResult 不被饿死,且
   pending/re-notify 保证剩余帧无需再次入队也会全部排空。

证据形态:仓库内确定性 Go 测试(`internal/gateway/downlink/c6b_harness_test.go`、
`internal/gateway/downlink/writer_test.go`、`internal/gateway/transport/websocket/
websocket_test.go`)的 `go test -race` 输出。本目录不是多 Pod 集群验收。

```bash
# 复现全部 C6B 证据
go test -race -count=1 -run 'TestC6B|TestLevelTriggeredNotifier|TestConnectionWriterPendingReArms|TestHintPolicy|TestWritePumpDrainQuantum' ./internal/gateway/downlink/ ./internal/gateway/transport/websocket/
```

治理立场(与既有 perf 文档一致):

```text
supports_30m_dau=false
multi_pod_evidence=MISSING
capacity_claim=NONE
```
