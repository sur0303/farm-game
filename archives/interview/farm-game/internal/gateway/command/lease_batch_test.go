package command

import (
	"context"
	"testing"
	"time"

	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/gateway/auth"
)

type holderPartitionLeaseLogic struct {
	*leaseLogic
	byFarm map[uint64]string
}

func (l *holderPartitionLeaseLogic) SubscriptionLeasePartition(_ context.Context, farmID uint64) (string, error) {
	partition, ok := l.byFarm[farmID]
	if !ok || partition == "" {
		return "", errNoPartition{}
	}
	return partition, nil
}

type errNoPartition struct{}

func (errNoPartition) Error() string { return "no partition" }

func (l *holderPartitionLeaseLogic) BatchRenewSubscriptionsPartition(ctx context.Context, _ string,
	req *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error) {
	return l.BatchRenewSubscriptions(ctx, req)
}

func scheduleActiveLeaseWithPartition(t *testing.T, gate *Gate, connectionID string, farmID uint64,
	subscriptionID, attemptID string, deadline time.Time, partition string) {
	t.Helper()
	gate.mu.Lock()
	defer gate.mu.Unlock()
	current := gate.connections[connectionID]
	current.currentFarmID = farmID
	current.currentSubscriptionID = subscriptionID
	current.currentJoinAttemptID = attemptID
	current.currentPresenceEpoch = 1
	current.subscriptionLeaseDeadline = deadline
	current.subscriptionLeaseTerminal = false
	current.pendingJoin = nil
	gate.connections[connectionID] = current
	candidate, ok := subscriptionLeaseCandidateFromActive(connectionID, current)
	if !ok {
		t.Fatal("active lease candidate missing")
	}
	candidate.holderPartition = partition
	gate.scheduleSubscriptionLeaseLocked(candidate, time.Now())
}

func TestResolveSubscriptionLeasePartitionUsesThePhysicalHolder(t *testing.T) {
	logic := &holderPartitionLeaseLogic{
		leaseLogic: newLeaseLogic(),
		byFarm:     map[uint64]string{9: "exact-holder"},
	}
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	gate := NewGate(authSvc, logic, 64<<10)
	if got := gate.resolveSubscriptionLeasePartition(9); got != "exact-holder" {
		t.Fatalf("resolved partition=%q", got)
	}
	static := NewGate(authSvc, newLeaseLogic(), 64<<10)
	want := subscriptionLeaseFarmPartition(9)
	if got := static.resolveSubscriptionLeasePartition(9); got != want {
		t.Fatalf("static partition=%q want=%q", got, want)
	}
}

func cloneLeaseConnection(t *testing.T, gate *Gate, connectionID string, playerID uint64) {
	t.Helper()
	gate.mu.Lock()
	defer gate.mu.Unlock()
	current := gate.connections[leaseTestConn]
	binding := *current.binding
	binding.PlayerID = playerID
	binding.Route.PlayerID = playerID
	binding.Route.ConnectionID = connectionID
	current.binding = &binding
	current.generation = 1
	gate.connections[connectionID] = current
}

func TestRenewSubscriptionLeasesDispatchesFarmsThatShareAHolderTogether(t *testing.T) {
	logic := newLeaseLogic()
	var entries int
	logic.renew = func(_ context.Context, req *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error) {
		entries = len(req.GetEntries())
		return leaseRenewResponse(req, logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_RENEWED, 30_000), nil
	}
	gate := newLeaseGate(t, logic)
	deadline := time.Now().Add(20 * time.Second)
	cloneLeaseConnection(t, gate, "share-b", 102)
	t.Cleanup(func() { gate.Disconnect("share-b", 1) })
	scheduleActiveLeaseWithPartition(t, gate, leaseTestConn, 101, "sub-a", "attempt-a", deadline, "holder-a")
	scheduleActiveLeaseWithPartition(t, gate, "share-b", 202, "sub-b", "attempt-b", deadline, "holder-a")
	if err := gate.RenewSubscriptionLeases(context.Background()); err != nil {
		t.Fatal(err)
	}
	if entries != 2 {
		t.Fatalf("shared-holder dispatch entries=%d want 2", entries)
	}
}

func TestRenewSubscriptionLeasesKeepsDistinctHoldersIsolated(t *testing.T) {
	logic := newLeaseLogic()
	var sizes []int
	logic.renew = func(_ context.Context, req *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error) {
		sizes = append(sizes, len(req.GetEntries()))
		return leaseRenewResponse(req, logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_RENEWED, 30_000), nil
	}
	gate := newLeaseGate(t, logic)
	deadline := time.Now().Add(20 * time.Second)
	cloneLeaseConnection(t, gate, "iso-b", 102)
	t.Cleanup(func() { gate.Disconnect("iso-b", 1) })
	scheduleActiveLeaseWithPartition(t, gate, leaseTestConn, 101, "sub-a", "attempt-a", deadline, "holder-a")
	scheduleActiveLeaseWithPartition(t, gate, "iso-b", 202, "sub-b", "attempt-b", deadline, "holder-b")
	if err := gate.RenewSubscriptionLeases(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := gate.RenewSubscriptionLeases(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sizes) != 2 || sizes[0] != 1 || sizes[1] != 1 {
		t.Fatalf("distinct holders must not share a dispatch: %v", sizes)
	}
}

func TestAcceptJoinNamesTheResolvedHolderBeforeScheduling(t *testing.T) {
	logic := &holderPartitionLeaseLogic{
		leaseLogic: newLeaseLogic(),
		byFarm:     map[uint64]string{77: "holder-from-resolver"},
	}
	gate := newLeaseGate(t, logic)
	gate.mu.Lock()
	current := gate.connections[leaseTestConn]
	current.pendingJoin = &pendingJoinState{
		operationID: "op-77", farmID: 77, attemptID: "attempt-77",
		state: pendingJoinPending,
	}
	gate.connections[leaseTestConn] = current
	gate.mu.Unlock()
	if !gate.acceptJoin(leaseTestConn, 1, "op-77", 77, "sub-77") {
		t.Fatal("acceptJoin rejected")
	}
	want := subscriptionLeaseDispatchHolder("gw", current.binding.Route.GatewayBootID, "holder-from-resolver")
	gate.mu.Lock()
	defer gate.mu.Unlock()
	stats := gate.subscriptionLeasePlanner.Stats()
	if stats.Backlog != 1 {
		t.Fatalf("backlog=%d", stats.Backlog)
	}
	batch := gate.subscriptionLeasePlanner.Plan(time.Now().Add(time.Hour), true)
	if batch.Holder != want {
		t.Fatalf("scheduled holder=%q want=%q", batch.Holder, want)
	}
}
