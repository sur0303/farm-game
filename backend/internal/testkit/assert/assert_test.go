package assert

import (
	"errors"
	"fmt"
	"testing"

	commonpb "farmgame/api/gen/common"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/farm/subscription"
	"farmgame/internal/storage/blobstore"
)

// fakeTB 捕获 Fatalf 以验证断言失败路径。
type fakeTB struct {
	failed  bool
	message string
}

func (f *fakeTB) Helper() {}
func (f *fakeTB) Fatalf(format string, args ...any) {
	f.failed = true
	f.message = fmt.Sprintf(format, args...)
}

func TestSameOperationRetried(t *testing.T) {
	tb := &fakeTB{}
	SameOperationRetried(tb, "sub-1", "sub-1", "subscription")
	if tb.failed {
		t.Fatalf("equal identities must pass: %s", tb.message)
	}
	SameOperationRetried(tb, "sub-1", "sub-2", "subscription")
	if !tb.failed {
		t.Fatal("divergent identities must fail")
	}
	tb = &fakeTB{}
	SameOperationRetried(tb, "", "", "subscription")
	if !tb.failed {
		t.Fatal("empty identity must fail (no proof of reuse)")
	}
}

func TestPayloadConflict(t *testing.T) {
	tb := &fakeTB{}
	PayloadConflict(tb, commonpb.CommandStatus_REJECTED, commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT)
	if tb.failed {
		t.Fatalf("canonical conflict must pass: %s", tb.message)
	}
	PayloadConflict(tb, commonpb.CommandStatus_SUCCEEDED, commonpb.ErrorCode_ERR_UNKNOWN_COMMAND)
	if !tb.failed {
		t.Fatal("non-conflict outcome must fail")
	}
}

func TestTimeoutUnknown(t *testing.T) {
	tb := &fakeTB{}
	TimeoutUnknown(tb, fmt.Errorf("wrap: %w", blobstore.ErrTimeoutUnknown))
	if tb.failed {
		t.Fatalf("wrapped ErrTimeoutUnknown must pass: %s", tb.message)
	}
	TimeoutUnknown(tb, blobstore.ErrUnavailable)
	if !tb.failed {
		t.Fatal("ErrUnavailable must not pass TimeoutUnknown")
	}
	TimeoutUnknownProcessing(tb, commonpb.CommandStatus_PROCESSING)
	if tb.failed && tb.message == "" {
		t.Fatal("unreachable")
	}
}

func TestOldWriterFenced(t *testing.T) {
	tb := &fakeTB{}
	OldWriterFenced(tb, blobstore.ErrWriterFenced)
	if tb.failed {
		t.Fatalf("ErrWriterFenced must pass: %s", tb.message)
	}
	OldWriterFenced(tb, errors.New("other"))
	if !tb.failed {
		t.Fatal("unrelated error must fail")
	}
}

func TestStaleSubscription(t *testing.T) {
	tb := &fakeTB{}
	StaleSubscriptionPush(tb, logicrpcpb.DownlinkItemStatus_STALE_SUBSCRIPTION)
	if tb.failed {
		t.Fatalf("STALE_SUBSCRIPTION must pass: %s", tb.message)
	}
	StaleSubscriptionPush(tb, logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED)
	if !tb.failed {
		t.Fatal("QUEUE_ACCEPTED must fail stale assertion")
	}

	tb = &fakeTB{}
	StaleSubscriptionRenew(tb, subscription.SubscriptionLeaseStaleFence)
	StaleSubscriptionRenew(tb, subscription.SubscriptionLeaseClosed)
	if tb.failed {
		t.Fatalf("stale-fence/closed renew must pass: %s", tb.message)
	}
	StaleSubscriptionRenew(tb, subscription.SubscriptionLeaseRenewed)
	if !tb.failed {
		t.Fatal("renewed must fail stale assertion")
	}
}
