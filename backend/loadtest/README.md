# loadtest — W5B 压测工具的扩展接缝层

本目录**不是**第二套压测 Harness。它只做三件事:

1. 以类型化参数生成既有工具的命令行(`cmd/lease-stress`、`cmd/ws-stress`)
   和既有脚本的环境变量(`scripts/test-multigate-drain`)。
2. 以 preset 声明后续 Owner 需要的扩展场景:广播慢连接、完整玩法、Soak、Drain。
3. 以 `EvidenceRule` 冻结每个场景申报 `CAPACITY_PASS` 前必须真实采集的证据。

## 证据纪律(不可绕过)

- 证据等级按 docs/design/72 §16.5:`UNIT_PASS` / `RACE_PASS` / `INTEGRATION_PASS` /
  `REAL_REDIS_PASS` / `MULTI_PROCESS_E2E_PASS` / `CAPACITY_PASS`,低级不得冒充高级。
- preset 只是参数声明。**没有真实运行 + 采齐 `RequiredEvidence` 的场景,
  结论只能是 `INCONCLUSIVE`(doc 60 §2.3),不得标记 `CAPACITY_PASS`。**
- Mock/Fake(如 testkit 的内存存储)产出的结果最高只能支撑
  `DOMAIN_LOGIC_PASS` / `UNIT_PASS`。

## 已知接缝缺口(只记录,不越权改生产代码)

- ICR-Q0-1:`cmd/ws-stress` 尚无"慢连接比例/下行消费限速"参数,广播慢连接场景
  需要在 ws-stress 增加 per-conn 消费限速旋钮(生产 cmd 入口,属后续 Owner 写集)。
- ICR-Q0-2:`internal/gateway/transport.FakeTransport` 不暴露队列长度,
  积压观测需经 Gate `/metrics` 或消费端计数,不能从 Fake 内部读。
