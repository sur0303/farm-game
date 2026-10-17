# Friend Opportunity C5 Evidence

该目录保存好友机会 `schedule-due` 专用索引的可重复单节点 Redis 证据。

当前选定证据：

- `schedule-due-c5-smoke-20260811T203751Z-1755493/evidence.json`
- Redis 7.4，2 CPU / 2 GiB；
- 100,000 个普通共享 DueIndex 成员；
- `supports_30m_dau=false`。

重跑：

```bash
scripts/perf/friend-opportunity-c5/run-schedule-due-index-c5 smoke
```

报告只用于比较共享和专用发现路径，不代表公司 Redis Cluster 或 3000 万 DAU 已验收。
