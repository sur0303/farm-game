package downlink

import (
	"context"
	"errors"
	"testing"

	commonpb "farmgame/api/gen/common"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestDeliverBatchMetrics 验证 downlink 指标:batch_items 直方图记录批量大小
// (ServerPush 接收者放大)、delivered_total 记录成功入队数、bindings_active 为
// 当前连接绑定数。
func TestDeliverBatchMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := NewConnectionRegistry()
	r.SetMetrics(NewMetrics(reg, r))
	w := NewConnectionWriter(3, 16, 1<<20)
	r.BindWithPresence("c", "sub", 3, 1, w)

	frame := sessionPushFrame(t)
	req := &logicrpcpb.DeliverBatchRequest{Items: []*logicrpcpb.DownlinkItem{
		{ItemId: "a", SubscriptionId: "sub", ConnectionId: "c", TransportGeneration: 3, PresenceEpoch: 1, Frame: frame},
		{ItemId: "b", SubscriptionId: "sub", ConnectionId: "c", TransportGeneration: 3, PresenceEpoch: 1, Frame: frame},
	}}
	resp, err := r.DeliverBatch(context.Background(), req)
	if err != nil || len(resp.Results) != 2 || resp.Results[0].Status != logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED {
		t.Fatalf("resp=%v err=%v", resp, err)
	}

	metricFamilies, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	seenBatch, seenDelivered, seenBindings := false, false, false
	for _, mf := range metricFamilies {
		switch mf.GetName() {
		case "farmgate_downlink_batch_items":
			seenBatch = true
			h := mf.GetMetric()[0].GetHistogram()
			if h.GetSampleCount() != 1 || h.GetSampleSum() != 2 {
				t.Fatalf("batch histogram samples=%d sum=%v, want 1/2", h.GetSampleCount(), h.GetSampleSum())
			}
		case "farmgate_downlink_delivered_total":
			seenDelivered = true
			if got := mf.GetMetric()[0].GetCounter().GetValue(); got != 2 {
				t.Fatalf("delivered=%v, want 2", got)
			}
		case "farmgate_downlink_bindings_active":
			seenBindings = true
			if got := mf.GetMetric()[0].GetGauge().GetValue(); got != 1 {
				t.Fatalf("bindings=%v, want 1", got)
			}
		}
	}
	if !seenBatch || !seenDelivered || !seenBindings {
		t.Fatalf("missing metrics: batch=%v delivered=%v bindings=%v", seenBatch, seenDelivered, seenBindings)
	}
}

func TestPendingJoinMetricsUseStrictFenceByDefault(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := NewConnectionRegistry()
	metrics := NewMetrics(reg, r)
	r.SetMetrics(metrics)
	w := NewConnectionWriter(7, 8, 1<<20)
	r.BindWithPresence("compat", "", 7, 0, w)
	if !r.PrepareJoinAttempt("compat", 7, "join-compat", 42, "attempt-compat") {
		t.Fatal("prepare strict Join")
	}
	legacy := &logicrpcpb.DownlinkItem{
		ItemId: "legacy-snapshot", SubscriptionId: "sub-compat", ConnectionId: "compat",
		TransportGeneration: 7, PresenceEpoch: 1,
		Frame: typedPushFrame(t, commonpb.PushType_FARM_SNAPSHOT, "sub-compat"),
	}
	resp, err := r.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{Items: []*logicrpcpb.DownlinkItem{legacy}})
	if err != nil || resp.Results[0].Status != logicrpcpb.DownlinkItemStatus_STALE_SUBSCRIPTION {
		t.Fatalf("unfenced legacy snapshot=%v err=%v", resp, err)
	}
	if got := testutil.ToFloat64(metrics.LegacyImplicitBind); got != 0 {
		t.Fatalf("legacy_implicit_bind_total=%v, want 0", got)
	}
	if got := testutil.ToFloat64(metrics.PendingJoinCurrent); got != 1 {
		t.Fatalf("pending_join_current=%v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.JoinStaleCallback); got != 1 {
		t.Fatalf("join_stale_callback_total=%v, want 1", got)
	}
	if !r.CancelJoin("compat", 7, "join-compat", 42) {
		t.Fatal("cancel strict Join")
	}
	if got := testutil.ToFloat64(metrics.PendingJoinCurrent); got != 0 {
		t.Fatalf("pending_join_current=%v after cancel", got)
	}
}

func TestPendingJoinTimeoutLatchMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := NewConnectionRegistry()
	metrics := NewMetrics(reg, r)
	r.SetMetrics(metrics)
	r.BindWithPresence("conn", "", 3, 0, NewConnectionWriter(3, 8, 1<<20))
	if !r.PrepareJoin("conn", 3, "join-timeout", 42) ||
		!r.AcceptJoin("conn", 3, "join-timeout", 42, "sub-timeout") {
		t.Fatal("prepare/accept timeout join")
	}
	if !r.ObserveJoinTimeout("conn", 3, "join-timeout", 42) ||
		!r.CancelJoin("conn", 3, "join-timeout", 42) {
		t.Fatal("observe/cancel timeout join")
	}
	if got := testutil.ToFloat64(metrics.JoinPendingTimeout); got != 1 {
		t.Fatalf("join_pending_timeout_total=%v", got)
	}
	if got := testutil.ToFloat64(metrics.JoinAcceptWithoutSnapshot); got != 1 {
		t.Fatalf("join_accept_without_snapshot_total=%v", got)
	}
	if got := testutil.ToFloat64(metrics.PendingJoinCurrent); got != 0 {
		t.Fatalf("pending_join_current=%v after close", got)
	}
	if r.AcceptJoin("conn", 3, "join-timeout", 42, "sub-timeout") {
		t.Fatal("late ACCEPTED reopened closed join")
	}
	if got := testutil.ToFloat64(metrics.JoinStaleCallback); got != 1 {
		t.Fatalf("join_stale_callback_total=%v", got)
	}
}

// TestNilMetricsSafe 验证未挂指标时调用路径不 panic。
func TestNilMetricsSafe(t *testing.T) {
	r := NewConnectionRegistry()
	w := NewConnectionWriter(3, 16, 1<<20)
	r.BindWithPresence("c", "sub", 3, 1, w)
	frame := sessionPushFrame(t)
	resp, err := r.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{Items: []*logicrpcpb.DownlinkItem{
		{ItemId: "i", SubscriptionId: "sub", ConnectionId: "c", TransportGeneration: 3, PresenceEpoch: 1, Frame: frame},
	}})
	if err != nil || resp.Results[0].Status != logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED {
		t.Fatalf("resp=%v err=%v", resp, err)
	}
}

// TestWriterMetricsLiveSnapshot verifies the reworked observability: writer
// enqueue/reject counters are recorded inside the writer on every admission
// path, and queue/hint GaugeFuncs aggregate live writer snapshots under the
// registry lock so dequeue decrements are visible on the next scrape.
func TestWriterMetricsLiveSnapshot(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := NewConnectionRegistry()
	metrics := NewMetrics(reg, r)
	r.SetMetrics(metrics)
	w := NewConnectionWriterWithCapacity(5, WriterCapacity{
		MaxItems: 16, MaxBytes: 1 << 20,
		Hint: HintPolicy{MaxHintItems: 1, MaxHintBytes: 1024},
	})
	r.BindWithPresence("c-metrics", "farm-a", 5, 1, w)

	if err := w.EnqueueKind(5, FrameKindHint, []byte("h1")); err != nil {
		t.Fatal(err)
	}
	if err := w.EnqueueKind(5, FrameKindHint, []byte("h2")); !errors.Is(err, ErrHintQuota) {
		t.Fatalf("second hint=%v, want ErrHintQuota", err)
	}
	if err := w.EnqueueKind(5, FrameKindFarmChange, []byte("change")); err != nil {
		t.Fatal(err)
	}
	// The queue is still full of the accepted frames: gauges must reflect them.
	if got := testutil.ToFloat64(metrics.WriterQueueItems); got != 2 {
		t.Fatalf("writer queue items=%v, want 2", got)
	}
	if got := testutil.ToFloat64(metrics.WriterHintItems); got != 1 {
		t.Fatalf("writer hint items=%v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.WriterEnqueuedItems.WithLabelValues("hint")); got != 1 {
		t.Fatalf("writer enqueued hint=%v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.WriterEnqueuedItems.WithLabelValues("farm_change")); got != 1 {
		t.Fatalf("writer enqueued farm_change=%v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.WriterRejected.WithLabelValues("hint", "reject", "quota")); got != 1 {
		t.Fatalf("writer rejected hint/quota=%v, want 1", got)
	}
	// Dequeue drops the queue gauges: the snapshot is live, not drift.
	if _, ok := w.Dequeue(); !ok {
		t.Fatal("dequeue")
	}
	if got := testutil.ToFloat64(metrics.WriterQueueItems); got != 1 {
		t.Fatalf("writer queue items after dequeue=%v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.WriterHintItems); got != 0 {
		t.Fatalf("writer hint items after dequeue=%v, want 0", got)
	}
}

// TestWriterMetricsNoIdentityLabels asserts that every writer metric family
// only ever uses the bounded FrameKind/result/reason label whitelist.
func TestWriterMetricsNoIdentityLabels(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := NewConnectionRegistry()
	metrics := NewMetrics(reg, r)
	r.SetMetrics(metrics)
	w := NewConnectionWriter(6, 16, 1<<20)
	r.BindWithPresence("identity-sentinel", "farm-a", 6, 1, w)
	_ = w.EnqueueKind(6, FrameKindHint, []byte("h"))
	_ = w.EnqueueKind(6, FrameKindCommandResult, []byte("r"))
	_ = w.EnqueueKind(7, FrameKindHint, []byte("stale")) // rejected
	metricFamilies, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range metricFamilies {
		if mf.GetName() == "farm_gate_pending_join_age_seconds" {
			continue
		}
		for _, metric := range mf.GetMetric() {
			for _, label := range metric.GetLabel() {
				switch label.GetName() {
				case "kind", "result", "reason", "quantile", "le":
				default:
					t.Fatalf("metric %s carries identity label %q=%q", mf.GetName(), label.GetName(), label.GetValue())
				}
			}
		}
	}
}

// TestSetMetricsIdempotentNilSafe verifies that attaching metrics twice or
// re-attaching nil never panics: process wiring and tests may construct
// registries/collectors in any order before serving traffic.
func TestSetMetricsIdempotentNilSafe(t *testing.T) {
	r := NewConnectionRegistry()
	first := NewMetrics(prometheus.NewRegistry(), r)
	second := NewMetrics(prometheus.NewRegistry(), r)
	r.SetMetrics(first)
	r.SetMetrics(first) // idempotent
	r.SetMetrics(second)
	r.SetMetrics(nil)
	w := NewConnectionWriter(9, 8, 1<<20)
	r.BindWithPresence("c-safe", "sub", 9, 1, w)
	if err := w.EnqueueKind(9, FrameKindHint, []byte("h")); err != nil {
		t.Fatalf("enqueue with nil metrics: %v", err)
	}
}
