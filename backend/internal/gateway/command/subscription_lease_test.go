package command

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/gateway/auth"
	"farmgame/internal/gateway/downlink"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/protobuf/proto"
)

// leaseLogic 提供可编排的 BatchRenewSubscriptions 与 CleanupSubscription 捕获。
// 续租语义覆盖 26 §11.5:Gate 只凭本地单调时钟 + Holder accepted_ttl 续约,
// 临时失败保留旧 deadline,显式 Fence 失败与本地过期 fail closed。
type leaseLogic struct {
	testLogic
	renew   func(ctx context.Context, req *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error)
	cleaned chan *logicrpcpb.CleanupSubscriptionRequest
}

func newLeaseLogic() *leaseLogic {
	logic := &leaseLogic{
		cleaned: make(chan *logicrpcpb.CleanupSubscriptionRequest, 8),
	}
	logic.renew = func(_ context.Context, req *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error) {
		return leaseRenewResponse(req, logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_RENEWED, 30_000), nil
	}
	return logic
}

func (l *leaseLogic) BatchRenewSubscriptions(ctx context.Context, req *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error) {
	return l.renew(ctx, req)
}

func (l *leaseLogic) CleanupSubscription(_ context.Context, req *logicrpcpb.CleanupSubscriptionRequest) (*logicrpcpb.CleanupSubscriptionResponse, error) {
	l.cleaned <- proto.Clone(req).(*logicrpcpb.CleanupSubscriptionRequest)
	return &logicrpcpb.CleanupSubscriptionResponse{Removed: true}, nil
}

type blockingPartitionLeaseLogic struct {
	*leaseLogic
	started chan struct{}
	release chan struct{}
}

func (l *blockingPartitionLeaseLogic) BatchRenewSubscriptionsPartition(_ context.Context, _ string,
	req *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error) {
	close(l.started)
	<-l.release
	return leaseRenewResponse(req, logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_RENEWED, 30_000), nil
}

// leaseRenewResponse 按 entry 镜像身份(farm/subscription/attempt),
// 与 sameSubscriptionLeaseIdentity 的校验口径一致。
func leaseRenewResponse(req *logicrpcpb.BatchRenewSubscriptionsRequest,
	status logicrpcpb.SubscriptionLeaseStatus, acceptedTTLMs uint32) *logicrpcpb.BatchRenewSubscriptionsResponse {
	results := make([]*logicrpcpb.SubscriptionLeaseResult, len(req.GetEntries()))
	for i, entry := range req.GetEntries() {
		results[i] = &logicrpcpb.SubscriptionLeaseResult{
			TargetFarmId:   entry.GetTargetFarmId(),
			SubscriptionId: entry.GetSubscriptionId(),
			JoinAttemptId:  entry.GetJoinAttemptId(),
			Status:         status,
			AcceptedTtlMs:  acceptedTTLMs,
		}
	}
	return &logicrpcpb.BatchRenewSubscriptionsResponse{Results: results}
}

const leaseTestConn = "lease-conn"

func newLeaseGate(t *testing.T, logic LogicClient) *Gate {
	t.Helper()
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	gate := NewGate(authSvc, logic, 64<<10)
	activateHello(t, gate, authSvc, leaseTestConn)
	t.Cleanup(func() { gate.Disconnect(leaseTestConn, 1) })
	return gate
}

// installActiveLease 直接安装 Active 订阅租约前置(与 joinOwnFarm 同手法),
// 这些用例覆盖续租状态机而非 Join 屏障本身。
func installActiveLease(t *testing.T, gate *Gate, deadline time.Time) {
	t.Helper()
	gate.mu.Lock()
	defer gate.mu.Unlock()
	current := gate.connections[leaseTestConn]
	current.currentFarmID = 101
	current.currentSubscriptionID = "sub-1"
	current.currentJoinAttemptID = "attempt-1"
	current.currentPresenceEpoch = 7
	current.subscriptionLeaseDeadline = deadline
	current.subscriptionLeaseTerminal = false
	current.pendingJoin = nil
	gate.connections[leaseTestConn] = current
	if candidate, ok := subscriptionLeaseCandidateFromActive(leaseTestConn, current); ok {
		candidate.deadline = deadline
		itemNow := time.Now()
		gate.scheduleSubscriptionLeaseLocked(candidate, itemNow)
		// Explicit maintenance tests intentionally bypass stable due phases.
	}
}

func installPendingLease(t *testing.T, gate *Gate, leaseDeadline time.Time) {
	t.Helper()
	gate.mu.Lock()
	defer gate.mu.Unlock()
	current := gate.connections[leaseTestConn]
	current.pendingJoin = &pendingJoinState{
		operationID: "op-pending", farmID: 202, subscriptionID: "sub-pending",
		attemptID: "attempt-pending", accepted: true, state: pendingJoinPending,
		deadline: time.Now().Add(time.Minute), leaseDeadline: leaseDeadline,
	}
	gate.connections[leaseTestConn] = current
	if candidate, ok := subscriptionLeaseCandidateFromPending(leaseTestConn, current); ok {
		gate.scheduleSubscriptionLeaseLocked(candidate, time.Now())
	}
}

func leaseConnection(t *testing.T, gate *Gate) connection {
	t.Helper()
	gate.mu.Lock()
	defer gate.mu.Unlock()
	return gate.connections[leaseTestConn]
}

func waitLeaseCleanup(t *testing.T, logic *leaseLogic) *logicrpcpb.CleanupSubscriptionRequest {
	t.Helper()
	select {
	case req := <-logic.cleaned:
		return req
	case <-time.After(2 * time.Second):
		t.Fatal("missing CleanupSubscription(SUBSCRIPTION_LEASE_EXPIRED)")
		return nil
	}
}

func assertNoLeaseCleanup(t *testing.T, logic *leaseLogic) {
	t.Helper()
	select {
	case req := <-logic.cleaned:
		t.Fatalf("unexpected CleanupSubscription: %v", req)
	case <-time.After(150 * time.Millisecond):
	}
}

func TestRenewSubscriptionLeasesExtendsActiveDeadline(t *testing.T) {
	logic := newLeaseLogic()
	gate := newLeaseGate(t, logic)
	old := time.Now().Add(20 * time.Second)
	installActiveLease(t, gate, old)

	var captured *logicrpcpb.BatchRenewSubscriptionsRequest
	logic.renew = func(_ context.Context, req *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error) {
		captured = proto.Clone(req).(*logicrpcpb.BatchRenewSubscriptionsRequest)
		return leaseRenewResponse(req, logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_RENEWED, 30_000), nil
	}
	before := time.Now()
	if err := gate.RenewSubscriptionLeases(context.Background()); err != nil {
		t.Fatal(err)
	}

	current := leaseConnection(t, gate)
	if current.subscriptionLeaseTerminal {
		t.Fatal("renewed subscription marked terminal")
	}
	deadline := current.subscriptionLeaseDeadline
	if !deadline.After(old) {
		t.Fatalf("deadline not extended: %v <= %v", deadline, old)
	}
	// 默认 margin=1s:deadline ≈ renewStarted+29s,不得越过 renewStarted+30s。
	if deadline.After(before.Add(30*time.Second)) || deadline.Before(before.Add(27*time.Second)) {
		t.Fatalf("deadline %v outside conservative window", deadline)
	}
	if captured == nil || len(captured.GetEntries()) != 1 {
		t.Fatalf("captured request=%v", captured)
	}
	entry := captured.GetEntries()[0]
	if entry.GetTargetFarmId() != 101 || entry.GetSubscriptionId() != "sub-1" ||
		entry.GetJoinAttemptId() != "attempt-1" || entry.GetRequestedTtlMs() != 30_000 {
		t.Fatalf("entry=%v", entry)
	}
	target := entry.GetTarget()
	if target == nil || target.GetTargetPlayerId() != 101 || target.GetGatewayId() != "gw" ||
		target.GetConnectionId() != leaseTestConn || target.GetConnectionGeneration() != 1 {
		t.Fatalf("target fence=%v", target)
	}
	assertNoLeaseCleanup(t, logic)
}

func TestRenewSubscriptionLeasesExtendsPendingDeadline(t *testing.T) {
	logic := newLeaseLogic()
	gate := newLeaseGate(t, logic)
	old := time.Now().Add(20 * time.Second)
	installPendingLease(t, gate, old)

	if err := gate.RenewSubscriptionLeases(context.Background()); err != nil {
		t.Fatal(err)
	}
	current := leaseConnection(t, gate)
	pending := current.pendingJoin
	if pending == nil || pending.leaseTerminal {
		t.Fatalf("pending=%+v", pending)
	}
	if !pending.leaseDeadline.After(old) {
		t.Fatalf("pending lease deadline not extended: %v <= %v", pending.leaseDeadline, old)
	}
	assertNoLeaseCleanup(t, logic)
}

func TestRenewSubscriptionLeasesTransientErrorPreservesDeadline(t *testing.T) {
	logic := newLeaseLogic()
	gate := newLeaseGate(t, logic)
	old := time.Now().Add(20 * time.Second)
	installActiveLease(t, gate, old)

	logic.renew = func(context.Context, *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error) {
		return nil, errors.New("holder unavailable")
	}
	if err := gate.RenewSubscriptionLeases(context.Background()); err == nil {
		t.Fatal("expected renew error")
	}
	current := leaseConnection(t, gate)
	if current.subscriptionLeaseTerminal || current.currentFarmID != 101 {
		t.Fatalf("transient error must not fence subscription: %+v", current)
	}
	if !current.subscriptionLeaseDeadline.Equal(old) {
		t.Fatalf("transient error moved deadline: %v want %v", current.subscriptionLeaseDeadline, old)
	}
	assertNoLeaseCleanup(t, logic)

	// 临时失败不延长也不剥夺本地权利:下一批成功续租仍可在旧 deadline 前完成。
	logic.renew = func(_ context.Context, req *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error) {
		return leaseRenewResponse(req, logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_RENEWED, 30_000), nil
	}
	if err := gate.RenewSubscriptionLeases(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := leaseConnection(t, gate).subscriptionLeaseDeadline; !got.After(old) {
		t.Fatalf("recovery renew did not extend: %v", got)
	}
}

func TestRenewSubscriptionLeasesTerminalStatusesExpireSubscription(t *testing.T) {
	for _, status := range []logicrpcpb.SubscriptionLeaseStatus{
		logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_NOT_FOUND,
		logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_STALE_FENCE,
		logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_CLOSED,
	} {
		t.Run(strings.TrimPrefix(status.String(), "SUBSCRIPTION_LEASE_"), func(t *testing.T) {
			logic := newLeaseLogic()
			gate := newLeaseGate(t, logic)
			installActiveLease(t, gate, time.Now().Add(20*time.Second))
			calls := 0
			logic.renew = func(_ context.Context, req *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error) {
				calls++
				return leaseRenewResponse(req, status, 0), nil
			}
			if err := gate.RenewSubscriptionLeases(context.Background()); err != nil {
				t.Fatal(err)
			}
			current := leaseConnection(t, gate)
			// 无 registry 时投递失败 -> terminateSubscription 精确清除订阅现场。
			if current.currentFarmID != 0 || current.currentSubscriptionID != "" ||
				current.currentJoinAttemptID != "" || current.currentPresenceEpoch != 0 {
				t.Fatalf("terminal subscription not cleared: %+v", current)
			}
			cleanup := waitLeaseCleanup(t, logic)
			if cleanup.GetReason() != logicrpcpb.CleanupReason_CLEANUP_REASON_SUBSCRIPTION_LEASE_EXPIRED ||
				cleanup.GetSubscription().GetFarmId() != 101 ||
				cleanup.GetSubscription().GetSubscriptionId() != "sub-1" ||
				cleanup.GetSubscription().GetJoinAttemptId() != "attempt-1" {
				t.Fatalf("cleanup=%v", cleanup)
			}
			// 终态不可复活:后续批次不再产生候选,也不再发起 RPC。
			if err := gate.RenewSubscriptionLeases(context.Background()); err != nil {
				t.Fatal(err)
			}
			if calls != 1 {
				t.Fatalf("terminal subscription revived into renew batch, calls=%d", calls)
			}
		})
	}
}

func TestRenewSubscriptionLeasesUnavailableKeepsLeaseUntilDeadline(t *testing.T) {
	for _, status := range []logicrpcpb.SubscriptionLeaseStatus{
		logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_ROUTE_STALE,
		logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_UNAVAILABLE,
		logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_UNSPECIFIED,
	} {
		t.Run(strings.TrimPrefix(status.String(), "SUBSCRIPTION_LEASE_"), func(t *testing.T) {
			logic := newLeaseLogic()
			gate := newLeaseGate(t, logic)
			old := time.Now().Add(20 * time.Second)
			installActiveLease(t, gate, old)
			logic.renew = func(_ context.Context, req *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error) {
				return leaseRenewResponse(req, status, 0), nil
			}
			if err := gate.RenewSubscriptionLeases(context.Background()); err != nil {
				t.Fatal(err)
			}
			current := leaseConnection(t, gate)
			if current.subscriptionLeaseTerminal || current.currentFarmID != 101 {
				t.Fatalf("%s must not fence a lease inside its deadline: %+v", status, current)
			}
			if !current.subscriptionLeaseDeadline.Equal(old) {
				t.Fatalf("%s moved deadline: %v", status, current.subscriptionLeaseDeadline)
			}
			assertNoLeaseCleanup(t, logic)
		})
	}
}

// 迟到响应:UNAVAILABLE 本身不终态,但响应落地时本地 deadline 已越过则必须终态,
// 不能用一次迟到的非终态结果无限续命。
func TestRenewSubscriptionLeasesLateUnavailableResponseExpiresAtDeadline(t *testing.T) {
	logic := newLeaseLogic()
	gate := newLeaseGate(t, logic)
	installActiveLease(t, gate, time.Now().Add(80*time.Millisecond))
	logic.renew = func(_ context.Context, req *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error) {
		time.Sleep(150 * time.Millisecond)
		return leaseRenewResponse(req, logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_UNAVAILABLE, 0), nil
	}
	if err := gate.RenewSubscriptionLeases(context.Background()); err != nil {
		t.Fatal(err)
	}
	current := leaseConnection(t, gate)
	if current.currentFarmID != 0 {
		t.Fatalf("late UNAVAILABLE inside expired deadline must terminate: %+v", current)
	}
	waitLeaseCleanup(t, logic)
}

// 跨 generation:候选快照后连接被替换(重连新 generation),迟到响应不得作用于新连接。
func TestRenewSubscriptionLeasesSkipsReplacedGeneration(t *testing.T) {
	logic := newLeaseLogic()
	gate := newLeaseGate(t, logic)
	installActiveLease(t, gate, time.Now().Add(20*time.Second))
	freshDeadline := time.Now().Add(45 * time.Second)
	logic.renew = func(_ context.Context, req *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error) {
		gate.mu.Lock()
		current := gate.connections[leaseTestConn]
		current.generation = 2
		current.currentSubscriptionID = "sub-new"
		current.currentJoinAttemptID = "attempt-new"
		current.subscriptionLeaseDeadline = freshDeadline
		gate.connections[leaseTestConn] = current
		gate.mu.Unlock()
		return leaseRenewResponse(req, logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_RENEWED, 30_000), nil
	}
	if err := gate.RenewSubscriptionLeases(context.Background()); err != nil {
		t.Fatal(err)
	}
	current := leaseConnection(t, gate)
	if !current.subscriptionLeaseDeadline.Equal(freshDeadline) {
		t.Fatalf("stale response rewrote new generation deadline: %v", current.subscriptionLeaseDeadline)
	}
	if current.subscriptionLeaseTerminal || current.currentSubscriptionID != "sub-new" {
		t.Fatalf("stale response fenced new generation: %+v", current)
	}
	assertNoLeaseCleanup(t, logic)
}

// 跨 context generation:同一连接内订阅上下文已翻代,旧候选响应不得落地。
func TestRenewSubscriptionLeasesSkipsContextGenerationBump(t *testing.T) {
	logic := newLeaseLogic()
	gate := newLeaseGate(t, logic)
	old := time.Now().Add(20 * time.Second)
	installActiveLease(t, gate, old)
	logic.renew = func(_ context.Context, req *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error) {
		gate.mu.Lock()
		current := gate.connections[leaseTestConn]
		current.subscriptionContextGeneration++
		gate.connections[leaseTestConn] = current
		gate.mu.Unlock()
		return leaseRenewResponse(req, logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_RENEWED, 30_000), nil
	}
	if err := gate.RenewSubscriptionLeases(context.Background()); err != nil {
		t.Fatal(err)
	}
	current := leaseConnection(t, gate)
	if !current.subscriptionLeaseDeadline.Equal(old) {
		t.Fatalf("stale context response moved deadline: %v", current.subscriptionLeaseDeadline)
	}
	assertNoLeaseCleanup(t, logic)
}

func TestRenewSubscriptionLeasesAppliesMixedPartialResultsPerItem(t *testing.T) {
	logic := newLeaseLogic()
	gate := newLeaseGate(t, logic)
	old := time.Now().Add(20 * time.Second)
	installActiveLease(t, gate, old)

	gate.mu.Lock()
	second := gate.connections[leaseTestConn]
	secondBinding := *second.binding
	secondBinding.PlayerID = 102
	secondBinding.Route.PlayerID = 102
	secondBinding.Route.ConnectionID = "lease-conn-2"
	second.binding = &secondBinding
	second.currentSubscriptionID = "sub-2"
	second.currentJoinAttemptID = "attempt-2"
	second.subscriptionLeaseDeadline = old
	gate.connections["lease-conn-2"] = second
	if candidate, ok := subscriptionLeaseCandidateFromActive("lease-conn-2", second); ok {
		gate.scheduleSubscriptionLeaseLocked(candidate, time.Now())
	}
	gate.mu.Unlock()
	t.Cleanup(func() { gate.Disconnect("lease-conn-2", 1) })

	logic.renew = func(_ context.Context, req *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error) {
		response := leaseRenewResponse(req, logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_RENEWED, 30_000)
		for _, result := range response.Results {
			if result.SubscriptionId == "sub-2" {
				result.Status = logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_CLOSED
				result.AcceptedTtlMs = 0
			}
		}
		return response, nil
	}
	if err := gate.RenewSubscriptionLeases(context.Background()); err != nil {
		t.Fatal(err)
	}
	if current := leaseConnection(t, gate); !current.subscriptionLeaseDeadline.After(old) || current.currentSubscriptionID != "sub-1" {
		t.Fatalf("successful mixed item not renewed: %+v", current)
	}
	gate.mu.Lock()
	second = gate.connections["lease-conn-2"]
	gate.mu.Unlock()
	if second.currentFarmID != 0 || second.currentSubscriptionID != "" {
		t.Fatalf("closed mixed item not removed: %+v", second)
	}
	waitLeaseCleanup(t, logic)
}

func TestRenewSubscriptionLeasesConcurrentCloseRejectsLateSuccess(t *testing.T) {
	logic := &blockingPartitionLeaseLogic{
		leaseLogic: newLeaseLogic(), started: make(chan struct{}), release: make(chan struct{}),
	}
	gate := newLeaseGate(t, logic)
	installActiveLease(t, gate, time.Now().Add(20*time.Second))
	done := make(chan error, 1)
	go func() { done <- gate.RenewSubscriptionLeases(context.Background()) }()
	<-logic.started
	disconnected := make(chan struct{})
	go func() {
		gate.Disconnect(leaseTestConn, 1)
		close(disconnected)
	}()
	select {
	case <-disconnected:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("partition dispatch held Gate.mu and blocked Disconnect")
	}
	close(logic.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	gate.mu.Lock()
	_, resurrected := gate.connections[leaseTestConn]
	gate.mu.Unlock()
	if resurrected {
		t.Fatal("late renew resurrected disconnected subscription")
	}
	_, subscriptionBacklog := gate.leaseBatchBacklog()
	if subscriptionBacklog != 0 {
		t.Fatalf("late renew requeued closed subscription backlog=%d", subscriptionBacklog)
	}
}

func TestExpireSubscriptionLeasesSweepsOverdue(t *testing.T) {
	t.Run("active", func(t *testing.T) {
		logic := newLeaseLogic()
		gate := newLeaseGate(t, logic)
		installActiveLease(t, gate, time.Now().Add(-time.Second))
		if expired := gate.ExpireSubscriptionLeases(time.Now()); expired != 1 {
			t.Fatalf("expired=%d", expired)
		}
		current := leaseConnection(t, gate)
		if current.currentFarmID != 0 || current.currentSubscriptionID != "" {
			t.Fatalf("overdue active subscription not cleared: %+v", current)
		}
		cleanup := waitLeaseCleanup(t, logic)
		if cleanup.GetReason() != logicrpcpb.CleanupReason_CLEANUP_REASON_SUBSCRIPTION_LEASE_EXPIRED {
			t.Fatalf("cleanup reason=%v", cleanup.GetReason())
		}
		// 幂等:同一连接再次清扫不得重复过期/重复清理。
		if expired := gate.ExpireSubscriptionLeases(time.Now()); expired != 0 {
			t.Fatalf("second sweep expired=%d", expired)
		}
		assertNoLeaseCleanup(t, logic)
	})

	t.Run("pending", func(t *testing.T) {
		logic := newLeaseLogic()
		gate := newLeaseGate(t, logic)
		installPendingLease(t, gate, time.Now().Add(-time.Second))
		if expired := gate.ExpireSubscriptionLeases(time.Now()); expired != 1 {
			t.Fatalf("expired=%d", expired)
		}
		current := leaseConnection(t, gate)
		if current.pendingJoin == nil || !current.pendingJoin.leaseTerminal ||
			current.pendingJoin.state != pendingJoinClosed {
			t.Fatalf("overdue pending join not terminated: %+v", current.pendingJoin)
		}
		cleanup := waitLeaseCleanup(t, logic)
		if cleanup.GetSubscription().GetFarmId() != 202 ||
			cleanup.GetSubscription().GetJoinAttemptId() != "attempt-pending" {
			t.Fatalf("cleanup=%v", cleanup)
		}
	})

	t.Run("future deadline untouched", func(t *testing.T) {
		logic := newLeaseLogic()
		gate := newLeaseGate(t, logic)
		installActiveLease(t, gate, time.Now().Add(time.Minute))
		if expired := gate.ExpireSubscriptionLeases(time.Now()); expired != 0 {
			t.Fatalf("expired=%d", expired)
		}
		assertNoLeaseCleanup(t, logic)
	})
}

// Holder 返回畸形结果时 fail closed:结果数不符/身份不符/accepted_ttl 越界,
// Gate 返回错误并对已越 deadline 的租约执行本地过期,但不误伤仍在 deadline 内的租约。
func TestRenewSubscriptionLeasesFailClosedOnMalformedResponses(t *testing.T) {
	cases := map[string]func(ctx context.Context, req *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error){
		"nil response": func(context.Context, *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error) {
			return nil, nil
		},
		"count mismatch": func(context.Context, *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error) {
			return &logicrpcpb.BatchRenewSubscriptionsResponse{}, nil
		},
		"identity mismatch": func(_ context.Context, req *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error) {
			response := leaseRenewResponse(req, logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_RENEWED, 30_000)
			response.Results[0].SubscriptionId = "sub-other"
			return response, nil
		},
		"renewed with zero ttl": func(_ context.Context, req *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error) {
			return leaseRenewResponse(req, logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_RENEWED, 0), nil
		},
		"renewed with ttl above requested": func(_ context.Context, req *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error) {
			return leaseRenewResponse(req, logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_RENEWED, 60_000), nil
		},
		"renewed with ttl below proven scheduling window": func(_ context.Context, req *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error) {
			return leaseRenewResponse(req, logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_RENEWED, 5_000), nil
		},
	}
	for name, script := range cases {
		t.Run(name, func(t *testing.T) {
			logic := newLeaseLogic()
			gate := newLeaseGate(t, logic)
			old := time.Now().Add(time.Minute)
			installActiveLease(t, gate, old)
			logic.renew = script
			if err := gate.RenewSubscriptionLeases(context.Background()); err == nil {
				t.Fatal("malformed response must return error")
			}
			// fail closed 不误伤仍在 deadline 内的租约:只报错并计指标,
			// 租约在本地 deadline 前保持可用。
			current := leaseConnection(t, gate)
			if current.subscriptionLeaseTerminal || current.currentFarmID != 101 ||
				!current.subscriptionLeaseDeadline.Equal(old) {
				t.Fatalf("fail-closed must not fence healthy lease: %+v", current)
			}
			assertNoLeaseCleanup(t, logic)
		})
	}

	// RPC 期间 deadline 被越过:畸形结果触发 fail closed,本地过期清扫精确生效。
	t.Run("deadline passing during malformed rpc sweeps", func(t *testing.T) {
		logic := newLeaseLogic()
		gate := newLeaseGate(t, logic)
		installActiveLease(t, gate, time.Now().Add(80*time.Millisecond))
		logic.renew = func(context.Context, *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error) {
			time.Sleep(150 * time.Millisecond)
			return &logicrpcpb.BatchRenewSubscriptionsResponse{}, nil
		}
		if err := gate.RenewSubscriptionLeases(context.Background()); err == nil {
			t.Fatal("malformed response must return error")
		}
		waitLeaseCleanup(t, logic)
		if current := leaseConnection(t, gate); current.currentFarmID != 0 {
			t.Fatalf("overdue lease must be swept after fail-closed: %+v", current)
		}
	})
}

func TestRenewSubscriptionLeasesFailClosedWhenRenewerUnsupported(t *testing.T) {
	logic := &testLogic{}
	gate := newLeaseGate(t, logic)
	installActiveLease(t, gate, time.Now().Add(time.Minute))
	err := gate.RenewSubscriptionLeases(context.Background())
	if err == nil || !strings.Contains(err.Error(), "does not support") {
		t.Fatalf("err=%v", err)
	}
	// 不支持续租时只报错并 fail closed 清扫;未到期租约不被误伤,
	// 到期清扫语义由 TestExpireSubscriptionLeasesSweepsOverdue 覆盖。
	if current := leaseConnection(t, gate); current.currentFarmID != 101 {
		t.Fatalf("unsupported renewer must not fence healthy lease: %+v", current)
	}
}

func TestSetSubscriptionLeaseConfigValidatesBoundaries(t *testing.T) {
	logic := newLeaseLogic()
	gate := newLeaseGate(t, logic)
	for _, cfg := range [][2]time.Duration{
		{0, 0},
		{-time.Second, 0},
		{time.Duration(^uint32(0))*time.Millisecond + time.Millisecond, 0},
		{30 * time.Second, 30 * time.Second},
		{30 * time.Second, -time.Second},
	} {
		if err := gate.SetSubscriptionLeaseConfig(cfg[0], cfg[1]); err == nil {
			t.Fatalf("config ttl=%s margin=%s must be rejected", cfg[0], cfg[1])
		}
	}
	if err := gate.SetSubscriptionLeaseConfig(10*time.Second, time.Second); err != nil {
		t.Fatal(err)
	}
	installActiveLease(t, gate, time.Now().Add(20*time.Second))
	var captured *logicrpcpb.BatchRenewSubscriptionsRequest
	logic.renew = func(_ context.Context, req *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error) {
		captured = proto.Clone(req).(*logicrpcpb.BatchRenewSubscriptionsRequest)
		return leaseRenewResponse(req, logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_RENEWED, 10_000), nil
	}
	before := time.Now()
	if err := gate.RenewSubscriptionLeases(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := captured.GetEntries()[0].GetRequestedTtlMs(); got != 10_000 {
		t.Fatalf("requested ttl=%d", got)
	}
	// margin=1s:deadline ≈ renewStarted+9s。
	if got := leaseConnection(t, gate).subscriptionLeaseDeadline; got.After(before.Add(10*time.Second)) ||
		got.Before(before.Add(8*time.Second)) {
		t.Fatalf("deadline %v outside configured margin window", got)
	}
}

func TestRenewSubscriptionLeasesMetrics(t *testing.T) {
	logic := newLeaseLogic()
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	gate := NewGate(authSvc, logic, 64<<10)
	metrics := NewMetrics(prometheus.NewRegistry())
	gate.SetMetrics(metrics)
	activateHello(t, gate, authSvc, leaseTestConn)
	t.Cleanup(func() { gate.Disconnect(leaseTestConn, 1) })

	installActiveLease(t, gate, time.Now().Add(20*time.Second))
	if err := gate.RenewSubscriptionLeases(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(metrics.SubscriptionLeaseRenewBatches); got != 1 {
		t.Fatalf("batches=%v", got)
	}
	if got := testutil.ToFloat64(metrics.SubscriptionLeaseRenewEntries); got != 1 {
		t.Fatalf("entries=%v", got)
	}
	if got := testutil.ToFloat64(metrics.SubscriptionLeaseResults.WithLabelValues(LeaseStatusRenewed)); got != 1 {
		t.Fatalf("results{RENEWED}=%v", got)
	}
	if got := testutil.ToFloat64(metrics.LeaseInFlight.WithLabelValues("subscription")); got != 0 {
		t.Fatalf("lease in-flight after terminal processing=%v", got)
	}

	// 终态结果:results{NOT_FOUND} + local_expired{active}。
	logic.renew = func(_ context.Context, req *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error) {
		return leaseRenewResponse(req, logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_NOT_FOUND, 0), nil
	}
	if err := gate.RenewSubscriptionLeases(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(metrics.SubscriptionLeaseResults.WithLabelValues(LeaseStatusNotFound)); got != 1 {
		t.Fatalf("results{NOT_FOUND}=%v", got)
	}
	if got := testutil.ToFloat64(metrics.SubscriptionLeaseLocalExpired.WithLabelValues(LeasePhaseActive)); got != 1 {
		t.Fatalf("local_expired{active}=%v", got)
	}
	waitLeaseCleanup(t, logic)

	// 本地过期:local_expired{pending}。
	installPendingLease(t, gate, time.Now().Add(-time.Second))
	if expired := gate.ExpireSubscriptionLeases(time.Now()); expired != 1 {
		t.Fatalf("expired=%d", expired)
	}
	if got := testutil.ToFloat64(metrics.SubscriptionLeaseLocalExpired.WithLabelValues(LeasePhasePending)); got != 1 {
		t.Fatalf("local_expired{pending}=%v", got)
	}
	waitLeaseCleanup(t, logic)

	// fail-closed:RPC 错误计数。
	logic.renew = func(context.Context, *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error) {
		return nil, errors.New("holder unavailable")
	}
	installActiveLease(t, gate, time.Now().Add(20*time.Second))
	if err := gate.RenewSubscriptionLeases(context.Background()); err == nil {
		t.Fatal("expected renew error")
	}
	if got := testutil.ToFloat64(metrics.SubscriptionLeaseFailClosed.WithLabelValues(LeaseFailClosedRPCError)); got != 1 {
		t.Fatalf("fail_closed{RPC_ERROR}=%v", got)
	}
}

// 第 2 项(Active/Pending 两阶段控制保留队列)在 Gate 侧的端到端证据:
// PendingJoin 阶段的租约过期,REJOIN_REQUIRED 必须走与业务共享的
// ConnectionWriter FIFO(DeliverBatch 控制通道),不强制断开整条账号会话。
func TestSubscriptionLeaseExpiredQueuesRejoinRequiredForPendingJoin(t *testing.T) {
	logic := newLeaseLogic()
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	gate := NewGate(authSvc, logic, 64<<10)
	registry := downlink.NewConnectionRegistry()
	gate.SetDownlinkRegistry(registry)
	activateHello(t, gate, authSvc, "c")
	t.Cleanup(func() { gate.Disconnect("c", 1) })

	join := encodeTypedCommandFrame(t, 2, &commonpb.ClientCommandEnvelope{
		OperationId: "op-join", RequestId: "req-join",
		CommandType: commonpb.CommandType_JOIN_FARM, Payload: joinFarmPayload(t, 101),
	})
	if _, err := gate.HandleBinary(context.Background(), "c", 1, join); err != nil {
		t.Fatalf("JOIN_FARM: %v", err)
	}
	if _, ok := registry.Dequeue("c", 1); !ok {
		t.Fatal("missing queued Join ACCEPTED")
	}

	// 让 PendingJoin 的租约本地过期。
	gate.mu.Lock()
	current := gate.connections["c"]
	if current.pendingJoin == nil || !current.pendingJoin.accepted ||
		current.pendingJoin.leaseDeadline.IsZero() {
		t.Fatalf("pending join lease precondition missing: %+v", current.pendingJoin)
	}
	if candidate, ok := subscriptionLeaseCandidateFromPending("c", current); ok {
		gate.removeSubscriptionLeaseLocked(candidate)
	}
	current.pendingJoin.leaseDeadline = time.Now().Add(-time.Second)
	gate.connections["c"] = current
	if candidate, ok := subscriptionLeaseCandidateFromPending("c", current); ok {
		gate.scheduleSubscriptionLeaseLocked(candidate, time.Now())
	}
	gate.mu.Unlock()

	if expired := gate.ExpireSubscriptionLeases(time.Now()); expired != 1 {
		t.Fatalf("expired=%d", expired)
	}

	raw, ok := registry.Dequeue("c", 1)
	if !ok {
		t.Fatal("REJOIN_REQUIRED must be queued on the connection FIFO")
	}
	var frame commonpb.FarmFrame
	if err := proto.Unmarshal(raw, &frame); err != nil {
		t.Fatal(err)
	}
	push := frame.GetServerPush()
	if push == nil {
		t.Fatal("missing server push")
	}
	if push.PushType != commonpb.PushType_REJOIN_REQUIRED {
		t.Fatalf("push=%s", push.PushType)
	}
	pushCtx := push.GetFarmSubscription()
	if pushCtx == nil || pushCtx.GetTargetFarmId() != 101 ||
		pushCtx.GetSubscriptionId() != "server-subscription" || pushCtx.GetTargetPlayerId() != 101 {
		t.Fatalf("push context=%v", pushCtx)
	}
	var rejoin farmpb.RejoinRequiredPush
	if err := proto.Unmarshal(push.Payload, &rejoin); err != nil {
		t.Fatal(err)
	}
	if rejoin.Reason != farmpb.RejoinReason_REJOIN_REASON_SUBSCRIPTION_LEASE_LOST {
		t.Fatalf("rejoin reason=%s", rejoin.Reason)
	}

	cleanup := waitLeaseCleanup(t, logic)
	if cleanup.GetReason() != logicrpcpb.CleanupReason_CLEANUP_REASON_SUBSCRIPTION_LEASE_EXPIRED ||
		cleanup.GetSubscription().GetFarmId() != 101 ||
		cleanup.GetSubscription().GetSubscriptionId() != "server-subscription" ||
		cleanup.GetSubscription().GetJoinAttemptId() == "" {
		t.Fatalf("cleanup=%v", cleanup)
	}
	// 投递成功时不走 terminate:PendingJoin 保留在 leaseTerminal,等待客户端 Fresh Join。
	gate.mu.Lock()
	current = gate.connections["c"]
	gate.mu.Unlock()
	if current.pendingJoin == nil || !current.pendingJoin.leaseTerminal ||
		current.pendingJoin.state != pendingJoinClosed {
		t.Fatalf("pending join after queued terminal: %+v", current.pendingJoin)
	}
}

// 投递被保留队列拒绝(无匹配订阅 Fence -> STALE_SUBSCRIPTION)时,
// 必须精确终止该订阅并异步 Cleanup,不能静默保留一个服务端已失效的订阅。
func TestSubscriptionLeaseExpiredTerminatesWhenDeliveryRejected(t *testing.T) {
	logic := newLeaseLogic()
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	gate := NewGate(authSvc, logic, 64<<10)
	registry := downlink.NewConnectionRegistry()
	gate.SetDownlinkRegistry(registry)
	activateHello(t, gate, authSvc, leaseTestConn)
	t.Cleanup(func() { gate.Disconnect(leaseTestConn, 1) })
	installActiveLease(t, gate, time.Now().Add(-time.Second))

	if expired := gate.ExpireSubscriptionLeases(time.Now()); expired != 1 {
		t.Fatalf("expired=%d", expired)
	}
	current := leaseConnection(t, gate)
	if current.currentFarmID != 0 || current.currentSubscriptionID != "" {
		t.Fatalf("rejected delivery must terminate subscription: %+v", current)
	}
	cleanup := waitLeaseCleanup(t, logic)
	if cleanup.GetReason() != logicrpcpb.CleanupReason_CLEANUP_REASON_SUBSCRIPTION_LEASE_EXPIRED {
		t.Fatalf("cleanup=%v", cleanup)
	}
}
