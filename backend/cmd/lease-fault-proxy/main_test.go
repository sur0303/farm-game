package main

import (
	"testing"

	logicrpcpb "farmgame/api/gen/logicrpc"
)

func TestBatchShapeUsesProductionSubscriptionEstimateAndFarmPartition(t *testing.T) {
	request := &logicrpcpb.BatchRenewSubscriptionsRequest{Entries: []*logicrpcpb.SubscriptionLeaseEntry{
		{TargetFarmId: 10, SubscriptionId: "a", JoinAttemptId: "j-a"},
		{TargetFarmId: 10, SubscriptionId: "b", JoinAttemptId: "j-b"},
	}}
	items, estimated, farms := batchShape(request)
	if items != 2 || farms != 1 {
		t.Fatalf("items=%d farms=%d", items, farms)
	}
	if estimated <= 2*128 {
		t.Fatalf("estimate=%d does not include proto payloads", estimated)
	}
	request.Entries[1].TargetFarmId = 11
	_, _, farms = batchShape(request)
	if farms != 2 {
		t.Fatalf("mixed farm count=%d", farms)
	}
}

func TestEntryKeyIgnoresInjectedFenceButBindsSubscriptionIdentity(t *testing.T) {
	left := &logicrpcpb.SubscriptionLeaseEntry{TargetFarmId: 10, SubscriptionId: "a", JoinAttemptId: "j"}
	right := &logicrpcpb.SubscriptionLeaseEntry{TargetFarmId: 10, SubscriptionId: "a", JoinAttemptId: "j",
		Target: &logicrpcpb.DownlinkTargetFence{ConnectionGeneration: 99}}
	if entryKey(left) != entryKey(right) {
		t.Fatal("faulted target fence must retain the canonical subscription identity")
	}
	right.SubscriptionId = "other"
	if entryKey(left) == entryKey(right) {
		t.Fatal("different subscription identity collided")
	}
}
