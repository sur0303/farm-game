# W9A-L FenceAndClaim / ResolutionReceipt 竞态交接

日期：2026-08-03
状态：`DETERMINISTIC_FAKE_PROOF_PASS / REDIS_LUA_PROOF_PENDING`

## 1. 本包证明的合同

本包在 W8A-L `MemoryStore` 的同一互斥线性化域内，为原 Commit `R` 与 Fence `F` 建立了可控闸门。
测试先让两个 goroutine 都到达闸门，再显式选择唯一顺序：

```text
R → F
Commit(C1) 写 State + AuthorityReceipt
→ FenceAndClaim(C1→C2) 在切点看到 Receipt
→ 固定 ResolutionReceipt(COMMITTED)
```

```text
F → R
FenceAndClaim(C1→C2) 在切点看到 before version/digest 且无 Receipt
→ 固定 ResolutionReceipt(NOT_COMMITTED)
→ 迟到 Commit(C1) 返回 WRITER_CLAIM_MISMATCH
```

不存在 Fence 已返回 `NOT_COMMITTED`，迟到 C1 写又成功的第三种结果。

## 2. 新增测试

文件：

```text
internal/storage/authoritystore/fence_race_test.go
```

覆盖：

1. 两个 goroutine 均已进入后，显式放行 R→F；
2. 两个 goroutine 均已进入后，显式放行 F→R；
3. F→R 后精确查询原 IdempotencyKey 不得出现 AuthorityReceipt；
4. Fence `TIMEOUT_UNKNOWN_AFTER_APPLY` 时，切点和 C2 已持久存在；同一
   `resolve_operation_id + new_writer_claim_id` 重试返回原 ResolutionReceipt；
5. C2 随后继续提交其他业务，原 ResolutionReceipt 的 CutVersion/CutDigest 不漂移；
6. 相同 ResolveOperationID 但不同 NewWriterClaimID 返回 `RECONCILE_REQUIRED`；
7. Fence `TIMEOUT_UNKNOWN_BEFORE_APPLY` 不改变 C1/RecordVersion；同一请求重试后才建立 C2 切点。

## 3. 验证证据

```text
go test ./internal/storage/authoritystore -count=1       PASS
go test -race ./internal/storage/authoritystore -count=1 PASS
go vet ./...                                              PASS
go test ./... -count=1                                   PASS
git diff --check                                          PASS
```

## 4. 不能提前宣称的内容

- 本包证明的是 InMemory Fake 的线性化逻辑，不是 Redis Lua 已经具有同样原子性；
- `FenceAndClaimActorV1` 的同槽 Lua、ScriptRegistry、NOSCRIPT/Timeout 分类和 Repository 尚属 W31-B/C；
- Shard WriterEpoch Claim/Renew/Transfer 与跨 Epoch E8→E9 接管尚属 W8C-L；
- 公司 Redis 的 Failover、持久化 RPO/RTO 和权限隔离证据仍缺失；
- 普通 GET/重读仍不是 Unknown 解析切点，后续实现不得用“暂时没查到 Receipt”替代 Fence；
- `ResolutionReceipt` 的压缩、归档和 Expired Evidence 仍属 W31-B/C，恢复窗口内不得删除。

## 5. 下一步

```text
W8C-L
→ 实现 ShardWriteAuthority Claim/Renew/Transfer/Unassign
→ Holder 单调 Deadline 与 Lease 过期 Self-Fence
→ 证明 E8 Commit 与 E9 换代也只有两种合法顺序

随后关闭 GAP-ROUTE-HASH-PROFILE
→ W31-B/C 把本包证明映射到固定 Redis Lua 脚本族
```
