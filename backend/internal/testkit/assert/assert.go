// Package assert 收敛合同要求的五类复用断言:
// 同 operation 重试、同 operation 不同 payload 冲突、TIMEOUT_UNKNOWN、
// 旧 writer Fence、旧 Subscription。断言只观察公开表面(proto 状态/
// 哨兵错误/lease 状态),不依赖任何未导出实现,也不伪造证据等级。
package assert

import (
	"errors"

	commonpb "farmgame/api/gen/common"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/farm/subscription"
	"farmgame/internal/storage/blobstore"
)

// TB 是 testing.TB 的最小子集,便于在非 *testing.T 上下文复用。
type TB interface {
	Helper()
	Fatalf(format string, args ...any)
}

// SameOperationRetried 断言同 operation 重试复用了首次建立的服务端身份
// (SubscriptionID / ReservationID / Receipt 等),即两次观察值相等且非空。
func SameOperationRetried(t TB, first, retry, what string) {
	t.Helper()
	if first == "" {
		t.Fatalf("%s: first identity is empty, cannot prove retry reuse", what)
	}
	if first != retry {
		t.Fatalf("%s: retry identity %q differs from first %q (must reuse)", what, retry, first)
	}
}

// PayloadConflict 断言同 operation 不同 payload 被拒绝:
// CommandStatus_REJECTED + ERR_IDEMPOTENCY_CONFLICT(合同 28 §L2-PROTO-13/
// player ingress 可见表面)。
func PayloadConflict(t TB, status commonpb.CommandStatus, code commonpb.ErrorCode) {
	t.Helper()
	if status != commonpb.CommandStatus_REJECTED || code != commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT {
		t.Fatalf("payload conflict must surface as REJECTED/ERR_IDEMPOTENCY_CONFLICT, got %s/%s", status, code)
	}
}

// TimeoutUnknown 断言 err 是 blobstore 边界的 TIMEOUT_UNKNOWN。
func TimeoutUnknown(t TB, err error) {
	t.Helper()
	if !errors.Is(err, blobstore.ErrTimeoutUnknown) {
		t.Fatalf("want ErrTimeoutUnknown, got %v", err)
	}
}

// TimeoutUnknownProcessing 断言 TIMEOUT_UNKNOWN 对客户端只暴露为
// PROCESSING(不得转成终态拒绝,合同 28 §L2-PROTO-19)。
func TimeoutUnknownProcessing(t TB, status commonpb.CommandStatus) {
	t.Helper()
	if status != commonpb.CommandStatus_PROCESSING {
		t.Fatalf("TIMEOUT_UNKNOWN must surface as PROCESSING, got %s", status)
	}
}

// OldWriterFenced 断言旧 writer 的写被 Fence 拒绝(合同 30 §30-01/30-06)。
func OldWriterFenced(t TB, err error) {
	t.Helper()
	if !errors.Is(err, blobstore.ErrWriterFenced) {
		t.Fatalf("want ErrWriterFenced (old writer fenced), got %v", err)
	}
}

// StaleSubscriptionPush 断言旧 Subscription 的下行被标记 STALE_SUBSCRIPTION
// (合同 27 §D20 break-before-make / 26 §HC-ACCESS-29)。
func StaleSubscriptionPush(t TB, status logicrpcpb.DownlinkItemStatus) {
	t.Helper()
	if status != logicrpcpb.DownlinkItemStatus_STALE_SUBSCRIPTION {
		t.Fatalf("stale subscription push must be STALE_SUBSCRIPTION, got %s", status)
	}
}

// StaleSubscriptionRenew 断言旧/已终结 Subscription 的续租被拒绝且不复活:
// 只接受 StaleFence 或 Closed 两类终态(合同 26 §HC-ACCESS-29 续租不复活)。
func StaleSubscriptionRenew(t TB, status subscription.SubscriptionLeaseStatus) {
	t.Helper()
	switch status {
	case subscription.SubscriptionLeaseStaleFence, subscription.SubscriptionLeaseClosed:
	default:
		t.Fatalf("stale renew must be StaleFence or Closed, got status=%d", status)
	}
}
