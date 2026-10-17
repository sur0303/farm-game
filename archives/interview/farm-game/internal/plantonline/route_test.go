package plantonline

import (
	"context"
	"testing"
	"time"

	contractspb "farmgame/api/gen/contracts"
	"farmgame/internal/actorrouting"
	"farmgame/internal/contracts"
	"farmgame/internal/workflow"
)

func TestRouteDomainIncludesApplyCareReward(t *testing.T) {
	domain, err := routeDomain(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_CARE_REWARD)
	if err != nil || domain != actorrouting.DomainPlayer {
		t.Fatalf("APPLY_CARE_REWARD domain=%q err=%v, want PLAYER", domain, err)
	}
	resolver, err := actorrouting.NewStaticResolver(testPlantOnlineRouteSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	route := CoordinatorRouteProvider(resolver, resolver.Realm())
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_CARE_REWARD)
	if err != nil {
		t.Fatal(err)
	}
	header, err := route(context.Background(), workflow.StepCall{}, spec.Command, 1)
	if err != nil {
		t.Fatal(err)
	}
	fence, err := NewRouteFence(resolver, resolver.Realm(), actorrouting.DomainPlayer, "player-a", "player-a-boot")
	if err != nil {
		t.Fatal(err)
	}
	if err := fence.VerifyRoute(context.Background(), &contracts.ParticipantCall{Spec: spec, RouteHeader: header}); err != nil {
		t.Fatalf("APPLY_CARE_REWARD player route fence rejected: %v", err)
	}
}

func TestRouteFenceRejectsMismatchedPhysicalAssignment(t *testing.T) {
	resolver, err := actorrouting.NewStaticResolver(testPlantOnlineRouteSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	route := CoordinatorRouteProvider(resolver, resolver.Realm())
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED)
	if err != nil {
		t.Fatal(err)
	}
	header, err := route(context.Background(), workflow.StepCall{}, spec.Command, 1)
	if err != nil {
		t.Fatal(err)
	}
	fence, err := NewRouteFence(resolver, resolver.Realm(), actorrouting.DomainPlayer, "player-a", "player-a-boot")
	if err != nil {
		t.Fatal(err)
	}
	call := &contracts.ParticipantCall{Spec: spec, RouteHeader: header}
	if header.GetExpectedLeaseEpoch() != 1 || header.ExpectedLeaseUntilMs == nil || header.GetExpectedLeaseUntilMs() == 0 {
		t.Fatalf("route omitted lease fence: %+v", header)
	}
	if err := fence.VerifyRoute(context.Background(), call); err != nil {
		t.Fatalf("valid physical proof rejected: %v", err)
	}
	observedDeadline := header.GetExpectedLeaseUntilMs()
	olderDeadline := observedDeadline - 1
	header.ExpectedLeaseUntilMs = &olderDeadline
	if err := fence.VerifyRoute(context.Background(), call); err != nil {
		t.Fatalf("monotonic lease deadline extension rejected: %v", err)
	}
	header.ExpectedLeaseUntilMs = &observedDeadline
	header.ExpectedWriterEpoch++
	if err := fence.VerifyRoute(context.Background(), call); err == nil {
		t.Fatal("mismatched writer epoch was accepted")
	}
	header.ExpectedWriterEpoch--
	header.ShardRevision++
	if err := fence.VerifyRoute(context.Background(), call); err == nil {
		t.Fatal("mismatched shard revision was accepted")
	}
	header.ShardRevision--
	header.ExpectedLeaseEpoch++
	if err := fence.VerifyRoute(context.Background(), call); err == nil {
		t.Fatal("mismatched lease epoch was accepted")
	}
	header.ExpectedLeaseEpoch--
	shortened := header.GetExpectedLeaseUntilMs() + 1
	header.ExpectedLeaseUntilMs = &shortened
	if err := fence.VerifyRoute(context.Background(), call); err == nil {
		t.Fatal("shortened assignment lease deadline was accepted")
	}
	header.ExpectedLeaseEpoch, header.ExpectedLeaseUntilMs = 0, nil
	if err := fence.VerifyRoute(context.Background(), call); err != nil {
		t.Fatalf("fully absent legacy lease proof rejected in compatibility phase: %v", err)
	}
	header.ExpectedLeaseEpoch = 1
	if err := fence.VerifyRoute(context.Background(), call); err == nil {
		t.Fatal("partial legacy lease proof was accepted")
	}
}

func testPlantOnlineRouteSnapshot() actorrouting.RouteSnapshot {
	leaseUntil := time.Now().Add(time.Hour).UnixMilli()
	route := func(holder int) actorrouting.ShardRoute {
		return actorrouting.ShardRoute{
			HolderIndex: holder, State: actorrouting.ShardStateActive,
			ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1,
			LeaseUntilMs: leaseUntil,
		}
	}
	return actorrouting.RouteSnapshot{
		Realm: "plant-online-test", GlobalRevision: 7,
		HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 202608050001,
		Player: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{route(0)}},
		Farm:   actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{route(1)}},
		Holders: []actorrouting.HolderRef{
			{InstanceID: "player-a", BootID: "player-a-boot", Endpoint: "player:9100", State: actorrouting.HolderStateActive},
			{InstanceID: "farm-a", BootID: "farm-a-boot", Endpoint: "farm:9090", State: actorrouting.HolderStateActive},
		},
	}
}
