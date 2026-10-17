// Package rediskey 集中定义 farm-game 的 Redis key 构造函数。
//
// 本文件保留接入与会话链已上线的 legacy runtime key，不能用于新的
// PlayerActor/FarmActor 权威记录、Receipt、Order 或 DueIndex。权威、独立
// Workflow、Runtime 与 Projection 的版本化物理布局由 keyspace.go 中的
// Builder 生成，语义以 31-actor-authority-redis-physical-contract.md 为准。
package rediskey

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

func ConnectionTicket(hash string) string {
	return fmt.Sprintf("connection:ticket:{%s}:hash", hash)
}

// ConnectionTicketForPlayer co-locates a candidate ticket and its
// SessionAuthority in the same Redis Cluster hash slot. ClientHello activation
// can therefore atomically consume the ticket and replace the active route.
func ConnectionTicketForPlayer(playerID uint64, hash string) string {
	return fmt.Sprintf("session:current:{%d}:ticket:%s:hash", playerID, hash)
}

// ── 会话(Session)──────────────────────────────────────────────
// 当前可写会话与代次。丢失后:提升 session_authority_epoch 强制重登。

// Session 返回玩家当前会话信息 key(hash:epoch/gateway/writable_farm/lease)。
func Session(ownerID uint64) string {
	return fmt.Sprintf("session:current:{%d}:hash", ownerID)
}

// SessionAuthorityEpoch 返回全局会话权威代次 key(灾难切换时提升)。
func SessionAuthorityEpoch() string {
	return "session:authority.epoch:global:string"
}

// ── 路由提示(Routing hint)────────────────────────────────────
// owner_id → virtual shard / 逻辑节点。丢失后:重新定位,不影响正确性。

func RouteHint(ownerID uint64) string {
	return fmt.Sprintf("route:owner.node:{%d}:string", ownerID)
}

// ── 连接绑定(Gateway binding)─────────────────────────────────
// owner_id → 当前所在网关节点,用于跨节点推送与顶号踢线。

func ConnBinding(ownerID uint64) string {
	return fmt.Sprintf("gate:conn.binding:{%d}:string", ownerID)
}

// ── 限流(Rate limit)──────────────────────────────────────────
// token bucket。丢失后:用保守默认重建。

func RateLimitPlayer(ownerID uint64) string {
	return fmt.Sprintf("ratelimit:player:{%d}:hash", ownerID)
}

func RateLimitIP(ip string) string {
	return fmt.Sprintf("ratelimit:ip:%s:hash", ip)
}

// ── Legacy 热结果缓存(Result cache)───────────────────────────
// 仅保留兼容；新 ResultDirectory 使用 ProjectionKeys。

func OperationResultCache(ownerID uint64, operationID string) string {
	digest := sha256.Sum256([]byte(operationID))
	return fmt.Sprintf("opresult:cache:{%d}:%s:string", ownerID, hex.EncodeToString(digest[:]))
}

// ── Legacy 只读视图(Projection)───────────────────────────────
// 仅保留兼容；新查询副本使用 ProjectionKeys，不能作为最终业务授权。

func FarmPublicSummary(ownerID uint64) string {
	return fmt.Sprintf("farm:public.summary:{%d}:hash", ownerID)
}

// ── 重连状态(Reconnect)──────────────────────────────────────
// reconnect token / 连接元数据。丢失后:完整登录并重新 Join。

func ReconnectToken(token string) string {
	return fmt.Sprintf("reconnect:token:%s:string", token)
}

// ── 在场集合(Presence set,可重建)────────────────────────────
// 某农场当前在线访客集,用于实时广播订阅。丢失后:客户端重新 Join 重建。
// Legacy key 使用 {target_owner_id} 只为兼容旧 Lua/测试，不表示它与
// FarmActor Authority 属于同一个持久化原子域。

func FarmOnline(targetOwnerID uint64) string {
	return fmt.Sprintf("farm:online:{%d}:set", targetOwnerID)
}

// ── Legacy 分布式锁(Lock)────────────────────────────────────
// 不得用它代替 ShardWriteAuthority、WriterEpoch 或 WriterClaim。

func LockOwner(ownerID uint64) string {
	return fmt.Sprintf("lock:owner:{%d}:string", ownerID)
}
