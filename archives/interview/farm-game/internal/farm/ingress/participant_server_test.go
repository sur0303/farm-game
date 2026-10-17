package ingress

import (
	"bytes"
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	contractspb "farmgame/api/gen/contracts"
	farmpb "farmgame/api/gen/farm"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actorrouting"
	"farmgame/internal/contracts"
	farmactor "farmgame/internal/farm/actor"
	"farmgame/internal/identity"
	"farmgame/internal/logic/actor/activation"
	"farmgame/internal/logic/actor/authority"
	"farmgame/internal/player/pet"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/blobstore"
	"farmgame/internal/workidentity"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const ingressTestNowMs int64 = 1_800_000_000_000

func ingressTestLeaseDeadline() *uint64 {
	value := uint64(ingressTestNowMs + 60_000)
	return &value
}

type farmParticipantTestRig struct {
	registry *farmactor.Registry
	route    *logicrpcpb.ActorRouteHeader
	fault    *authoritystore.FaultStore
}

func newFarmParticipantTestRig(t *testing.T, farmID uint64, limits authoritystore.Limits) farmParticipantTestRig {
	t.Helper()
	calculator, err := actorrouting.NewCalculator("farm-fi1-test", actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608040011}, 64, 64)
	if err != nil {
		t.Fatal(err)
	}
	store, err := authoritystore.NewMemoryStore(authoritystore.MemoryOptions{
		Resolver: calculator, AuthorityLayoutVersion: "v1", Limits: limits,
		Now: func() time.Time { return time.UnixMilli(ingressTestNowMs) },
	})
	if err != nil {
		t.Fatal(err)
	}
	shard, err := calculator.ShardFor(actorrouting.DomainFarm, farmID)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetShardWriteAuthority(authoritystore.ShardWriteAuthority{
		AuthorityLayoutVersion: "v1", Domain: actorrouting.DomainFarm, VirtualShard: shard,
		HolderID: "farm-holder-a", HolderBootID: "farm-holder-a-boot", AssignmentRevision: 1,
		WriterEpoch: 1, LeaseEpoch: 1, State: authoritystore.ShardAuthorityActive,
		LeaseUntilMs: ingressTestNowMs + 60_000,
	}); err != nil {
		t.Fatal(err)
	}
	fault := authoritystore.NewFaultStore(store)
	provider, err := authority.NewProvider(authority.ProviderOptions{
		Store: fault,
		Fence: authority.StaticFenceSource{Fence: authoritystore.ShardWriteFence{
			ExpectedHolderID: "farm-holder-a", ExpectedHolderBootID: "farm-holder-a-boot",
			ExpectedAssignmentRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1,
		}},
		AuthorityLayoutVersion: "v1", BootID: "farm-holder-a-boot",
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := farmactor.NewAuthorityRegistry(provider, farmactor.RegistryOptions{MailboxSize: 16})
	t.Cleanup(registry.Stop)
	// Receipt/capacity admission is intentionally mailbox-free and fails closed
	// on a cold farm until composition supplies a bounded read-only snapshot.
	// Warm the test Actor once so the remaining cases exercise the activated
	// snapshot path rather than accidentally reintroducing LoadAndClaim.
	if _, err := registry.Submit(context.Background(), farmID, func(context.Context, *farmactor.FarmActor) (any, error) {
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	return farmParticipantTestRig{registry: registry, fault: fault, route: &logicrpcpb.ActorRouteHeader{
		RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM, ActorId: farmID, VirtualShard: shard,
		ShardRevision: 1, ExpectedHolderId: "farm-holder-a", ExpectedHolderBootId: "farm-holder-a-boot",
		ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, ExpectedLeaseUntilMs: ingressTestLeaseDeadline(),
	}}
}

func ingressTestSnapshot() *contractspb.CropRuleSnapshot {
	waterOffset, pestOffset := int64(10_000), int64(20_000)
	return &contractspb.CropRuleSnapshot{
		SchemaVersion: 1, CropDefinitionId: 42, CropRuleVersion: 7, SeedItemId: 1001,
		HarvestItemId: 2001, GrowDurationMs: 60_000, BaseYield: 250, HarvestExp: 13,
		CropUnitCoinValue: 4, LegacySeedPlantable: true,
		CompendiumRule: &contractspb.CompendiumUnlockRuleSnapshot{
			CropDefinitionId: 42, UnlockEligible: true, CatalogVersionAtPlant: 11, DefinitionVersionAtPlant: 3,
		},
		StealPlanTemplate: &contractspb.StealPlanTemplate{PolicyVersion: 2, TotalCapRatioBp: 2000, SlotRatioBp: []uint32{200, 200, 200, 200, 200, 200, 200, 200, 200, 200}},
		CarePlanTemplate: &contractspb.CarePlanTemplate{CareRuleVersion: 5, Incidents: []*contractspb.CarePlanIncidentTemplate{
			{CareType: contractspb.CareType_CARE_TYPE_WATER, Ordinal: 1, ScheduleMode: contractspb.CareScheduleMode_CARE_SCHEDULE_MODE_FIXED_OFFSET, FixedOffsetMs: &waterOffset},
			{CareType: contractspb.CareType_CARE_TYPE_REMOVE_PEST, Ordinal: 1, ScheduleMode: contractspb.CareScheduleMode_CARE_SCHEDULE_MODE_FIXED_OFFSET, FixedOffsetMs: &pestOffset},
		}},
	}
}

func ingressTestPayload(t *testing.T, farmID uint64, operationID string, plotID uint32) (*contractspb.CommitPlantPayload, string) {
	t.Helper()
	workflowID, err := workidentity.WorkflowID(farmID, "PLANT", operationID)
	if err != nil {
		t.Fatal(err)
	}
	reservationID, err := identity.SeedReservationID(workflowID)
	if err != nil {
		t.Fatal(err)
	}
	cycleID, err := identity.CropCycleID(workflowID)
	if err != nil {
		t.Fatal(err)
	}
	return &contractspb.CommitPlantPayload{
		OwnerPlayerId: farmID, FarmId: farmID, PlotId: plotID, ExpectedPlotVersion: 1,
		ReservationId: reservationID, ReserveSeedResultHash: bytes.Repeat([]byte{0x42}, 32),
		CropCycleId: cycleID, CropRuleSnapshot: ingressTestSnapshot(),
		CropRuleSnapshotHash: mustIngressHash(t, ingressTestSnapshot()),
	}, workflowID
}

func mustIngressHash(t *testing.T, message proto.Message) []byte {
	t.Helper()
	hash, err := contracts.CanonicalHash(message)
	if err != nil {
		t.Fatal(err)
	}
	return hash[:]
}

func ingressTestRequest(t *testing.T, route *logicrpcpb.ActorRouteHeader, farmID uint64, operationID string, payload *contractspb.CommitPlantPayload, workflowID string) *contractspb.ParticipantRequest {
	t.Helper()
	invocationID, err := identity.ParticipantInvocationID(identity.WorkTypePlant, workflowID, identity.StepCommitPlant, 1)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return &contractspb.ParticipantRequest{
		RouteHeader: proto.Clone(route).(*logicrpcpb.ActorRouteHeader),
		CallerContext: &contractspb.ParticipantRequest_Coordinator{Coordinator: &contractspb.CoordinatorWorkContext{
			Work:                &contractspb.WorkAddress{WorkId: workflowID, WorkType: contractspb.WorkType_WORK_TYPE_PLANT, PlacementKind: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_INDEPENDENT_WORK_BUCKET, BucketVersion: 1},
			ExpectedWorkVersion: 1, ParticipantStep: contractspb.ParticipantStep_PARTICIPANT_STEP_COMMIT_PLANT,
			ParticipantInvocationId: invocationID, OperationId: operationID, SubjectPlayerId: farmID, RemainingBudgetMs: 5_000,
		}},
		ParticipantCommand: contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT,
		TypedPayload:       raw,
	}
}

func ingressHarvestPayload(t *testing.T, farmID uint64, operationID, cycleID string, plotID uint32, expectedPlotVersion uint64) (*contractspb.CommitHarvestPayload, string) {
	t.Helper()
	workflowID, err := workidentity.WorkflowID(farmID, "HARVEST", operationID)
	if err != nil {
		t.Fatal(err)
	}
	return &contractspb.CommitHarvestPayload{
		OwnerPlayerId: farmID, FarmId: farmID, PlotId: plotID,
		CropCycleId: cycleID, ExpectedPlotVersion: expectedPlotVersion,
	}, workflowID
}

func ingressHarvestRequest(t *testing.T, route *logicrpcpb.ActorRouteHeader, farmID uint64, operationID string, payload *contractspb.CommitHarvestPayload, workflowID string) *contractspb.ParticipantRequest {
	t.Helper()
	invocationID, err := identity.ParticipantInvocationID(identity.WorkTypeHarvest, workflowID, identity.StepCommitHarvest, 1)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return &contractspb.ParticipantRequest{
		RouteHeader: proto.Clone(route).(*logicrpcpb.ActorRouteHeader),
		CallerContext: &contractspb.ParticipantRequest_Coordinator{Coordinator: &contractspb.CoordinatorWorkContext{
			Work:                &contractspb.WorkAddress{WorkId: workflowID, WorkType: contractspb.WorkType_WORK_TYPE_HARVEST, PlacementKind: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_INDEPENDENT_WORK_BUCKET, BucketVersion: 1},
			ExpectedWorkVersion: 1, ParticipantStep: contractspb.ParticipantStep_PARTICIPANT_STEP_COMMIT_HARVEST,
			ParticipantInvocationId: invocationID, OperationId: operationID, SubjectPlayerId: farmID, RemainingBudgetMs: 5_000,
		}},
		ParticipantCommand: contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_HARVEST,
		TypedPayload:       raw,
	}
}

func ingressGuardSyncRequest(t *testing.T, route *logicrpcpb.ActorRouteHeader, farmID, version uint64, activePetID uint32, operationID string) *contractspb.ParticipantRequest {
	t.Helper()
	petSnapshot := pet.GuardSnapshot{
		SchemaVersion: pet.GuardSnapshotSchemaVersion, GuardVersion: version, ActiveGuardPetID: int32(activePetID),
		GuardUntilMs: ingressTestNowMs + 120_000,
		Rule: pet.GuardRuleSnapshot{PetConfigVersion: 11, GuardRuleVersion: 12, TriggerProbabilityBp: 10_000,
			InterceptPolicy: pet.InterceptPolicyFullGross, RandomAlgorithmVersion: 13, RandomKeyVersion: 14},
	}
	hash := petSnapshot.Hash()
	active := activePetID
	payload := &contractspb.ApplyGuardSnapshotPayload{
		SourcePlayerId: farmID, TargetFarmId: farmID, SnapshotHash: append([]byte(nil), hash[:]...),
		GuardSnapshot: &contractspb.GuardSnapshot{
			SchemaVersion: uint32(petSnapshot.SchemaVersion), GuardVersion: version, ActiveGuardPetId: &active,
			GuardUntilMs: petSnapshot.GuardUntilMs, SnapshotHash: append([]byte(nil), hash[:]...),
			GuardRule: &contractspb.GuardRuleSnapshot{PetConfigVersion: 11, GuardRuleVersion: 12,
				TriggerProbabilityBp: 10_000, InterceptPolicy: contractspb.GuardInterceptPolicy_GUARD_INTERCEPT_POLICY_FULL_GROSS,
				RandomAlgorithmVersion: 13, RandomKeyVersion: 14},
		},
	}
	canonicalFarmID := strconv.FormatUint(farmID, 10)
	orderID, err := identity.SourceOrderID(identity.WorkTypeGuardSyncOrder, identity.SourceKindPlayerActor,
		canonicalFarmID, "PET_GUARD_SLOT", version)
	if err != nil {
		t.Fatal(err)
	}
	payload.OrderId = orderID
	invocationID, err := identity.ParticipantInvocationID(identity.WorkTypeGuardSyncOrder, orderID, identity.StepApplyGuardSnapshot, 1)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	playerDomain := logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER
	return &contractspb.ParticipantRequest{
		RouteHeader: proto.Clone(route).(*logicrpcpb.ActorRouteHeader),
		CallerContext: &contractspb.ParticipantRequest_Coordinator{Coordinator: &contractspb.CoordinatorWorkContext{
			Work: &contractspb.WorkAddress{WorkId: orderID, WorkType: contractspb.WorkType_WORK_TYPE_GUARD_SYNC_ORDER,
				PlacementKind: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET, BucketVersion: 1,
				SourceAuthority: &contractspb.SourceAuthorityRef{AuthorityKind: contractspb.SourceAuthorityKind_SOURCE_AUTHORITY_KIND_PLAYER_ACTOR,
					CanonicalAuthorityId: canonicalFarmID, AuthorityLayoutVersion: "v1", ActorRouteDomain: &playerDomain}},
			ExpectedWorkVersion: 1, ParticipantStep: contractspb.ParticipantStep_PARTICIPANT_STEP_APPLY_GUARD_SNAPSHOT,
			ParticipantInvocationId: invocationID, OperationId: operationID, SubjectPlayerId: farmID, RemainingBudgetMs: 5_000,
		}},
		ParticipantCommand: contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_GUARD_SNAPSHOT,
		TypedPayload:       raw,
	}
}

func passingFarmParticipantOptions(registry *farmactor.Registry, now func() time.Time) Options {
	return Options{
		Registry:                registry,
		WorkVersionVerifier:     WorkVersionVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
		WorkLeaseVerifier:       WorkLeaseVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
		RouteFenceVerifier:      contracts.ParticipantRouteFenceVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
		WorkAddressVerifier:     contracts.ParticipantWorkAddressVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
		IdentityBindingVerifier: contracts.ParticipantIdentityBindingVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
		CommitPlantDependency:   CommitPlantDependencyVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
		CommitHarvestDependency: CommitHarvestDependencyVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
		NowMs:                   func() int64 { return ingressTestNowMs }, Now: now,
	}
}

func TestFarmParticipantServerGuardSyncProductionPathReplayAndSourceFence(t *testing.T) {
	const farmID = uint64(1320)
	rig := newFarmParticipantTestRig(t, farmID, authoritystore.Limits{})
	options := passingFarmParticipantOptions(rig.registry, func() time.Time { return time.UnixMilli(ingressTestNowMs) })
	var routeProofs atomic.Int32
	options.RouteFenceVerifier = contracts.ParticipantRouteFenceVerifierFunc(func(_ context.Context, call *contracts.ParticipantCall) error {
		routeProofs.Add(1)
		if call.RouteHeader.GetExpectedWriterEpoch() != rig.route.GetExpectedWriterEpoch() {
			return status.Error(codes.FailedPrecondition, "WRITER_EPOCH_MISMATCH")
		}
		return nil
	})
	server, err := NewServerChecked(options)
	if err != nil {
		t.Fatal(err)
	}
	request := ingressGuardSyncRequest(t, rig.route, farmID, 1, 7, "guard-sync-order-v1")
	first, err := server.ExecuteStep(context.Background(), request)
	if err != nil || first.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		t.Fatalf("production GuardSync first=%+v err=%v", first, err)
	}
	var result contractspb.ApplyGuardSnapshotResult
	if err := proto.Unmarshal(first.GetTypedResult(), &result); err != nil || result.GetGuardVersion() != 1 ||
		result.GetApplyStatus() != contractspb.GuardApplyStatus_GUARD_APPLY_STATUS_APPLIED {
		t.Fatalf("production GuardSync result=%+v err=%v", &result, err)
	}
	replay, err := server.ExecuteStep(context.Background(), request)
	if err != nil || !replay.GetReplayedFromReceipt() || !bytes.Equal(first.GetTypedResult(), replay.GetTypedResult()) ||
		!bytes.Equal(first.GetParticipantResultHash(), replay.GetParticipantResultHash()) {
		t.Fatalf("production GuardSync replay=%+v err=%v", replay, err)
	}
	// Physical routing remains independently fenced before the durable
	// Participant receipt is consulted; WorkVersion/lease checks are the
	// receipt-first portion of admission.
	if routeProofs.Load() != 2 {
		t.Fatalf("route proof calls=%d, want one per admitted RPC", routeProofs.Load())
	}

	fenced := ingressGuardSyncRequest(t, rig.route, farmID, 2, 0, "guard-sync-order-v2-fenced")
	fenced.RouteHeader.ExpectedWriterEpoch++
	if _, err := server.ExecuteStep(context.Background(), fenced); status.Code(err) != codes.FailedPrecondition ||
		!bytes.Contains([]byte(err.Error()), []byte("WRITER_EPOCH_MISMATCH")) {
		t.Fatalf("route/writer fenced GuardSync err=%v", err)
	}
	forgedSource := ingressGuardSyncRequest(t, rig.route, farmID, 2, 0, "guard-sync-order-v2-forged")
	forgedSource.GetCoordinator().GetWork().GetSourceAuthority().CanonicalAuthorityId = strconv.FormatUint(farmID+1, 10)
	if _, err := server.ExecuteStep(context.Background(), forgedSource); status.Code(err) != codes.InvalidArgument && status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("forged GuardSync source err=%v", err)
	}
	record, _, err := rig.registry.ReadOutsideMailbox(context.Background(), farmID)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.GetRecentOperationReceipts()) != 1 {
		t.Fatalf("fenced/forged calls entered mailbox: receipts=%d", len(record.GetRecentOperationReceipts()))
	}
}

func activateFarmParticipantSubscription(t *testing.T, registry *farmactor.Registry, farmID, viewerID uint64, subscriptionID string) {
	t.Helper()
	_, err := registry.Submit(context.Background(), farmID, func(_ context.Context, actor *farmactor.FarmActor) (any, error) {
		join, err := actor.Join(farmactor.JoinRequest{
			OperationID: "participant-join-" + subscriptionID, JoinAttemptID: "participant-attempt-" + subscriptionID,
			SubscriptionID: subscriptionID, ViewerPlayerID: viewerID, SessionEpoch: 1,
			GatewayID: "participant-gate", GatewayBootID: "participant-boot", ConnectionID: "participant-conn-" + subscriptionID,
			TransportGeneration: 1, RouteRevision: 1,
		}, time.UnixMilli(ingressTestNowMs))
		if err != nil {
			return nil, err
		}
		if _, err := actor.ConfirmSnapshotQueued(join); err != nil {
			return nil, err
		}
		_, err = actor.DrainChangesOrConfirm(join)
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func participantFarmChangeItems(t *testing.T, downlink *resultCaptureDownlink) []*farmpb.FarmChange {
	t.Helper()
	downlink.mu.Lock()
	items := append([]*logicrpcpb.DownlinkItem(nil), downlink.items...)
	downlink.mu.Unlock()
	changes := make([]*farmpb.FarmChange, 0)
	for _, item := range items {
		var frame commonpb.FarmFrame
		if err := proto.Unmarshal(item.GetFrame(), &frame); err != nil {
			t.Fatal(err)
		}
		push := frame.GetServerPush()
		if push == nil || push.GetPushType() != commonpb.PushType_FARM_CHANGE {
			continue
		}
		var payload farmpb.FarmChangePush
		if err := proto.Unmarshal(push.GetPayload(), &payload); err != nil {
			t.Fatal(err)
		}
		if payload.GetChange() != nil {
			changes = append(changes, payload.GetChange())
		}
	}
	return changes
}

func waitFarmParticipantResyncOrCleanup(t *testing.T, registry *farmactor.Registry, farmID uint64, subscriptionID string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		value, err := registry.Submit(context.Background(), farmID, func(_ context.Context, actor *farmactor.FarmActor) (any, error) {
			phase, ok := actor.PresencePhase(subscriptionID)
			return []any{phase, ok}, nil
		})
		if err == nil {
			state := value.([]any)
			if !state[1].(bool) || state[0].(farmactor.PresencePhase) == farmactor.PresenceResyncRequired {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for Farm Participant resync or exact terminal cleanup")
}

func TestFarmParticipantCommitPlantPublishesOnceAndReceiptReplayIsSilent(t *testing.T) {
	rig := newFarmParticipantTestRig(t, 1310, authoritystore.Limits{})
	downlink := &resultCaptureDownlink{}
	service := NewService(rig.registry, nil)
	service.SetDownlinkEnqueuer(downlink)
	var observed atomic.Int32
	var mailboxAvailable atomic.Int32
	options := passingFarmParticipantOptions(rig.registry, func() time.Time { return time.UnixMilli(ingressTestNowMs) })
	options.CommitPlantObserver = func(farmID uint64, action *farmactor.ActionResult) {
		observed.Add(1)
		if _, err := rig.registry.Submit(context.Background(), farmID, func(context.Context, *farmactor.FarmActor) (any, error) {
			mailboxAvailable.Add(1)
			return nil, nil
		}); err != nil {
			mailboxAvailable.Store(-1)
		}
		service.EmitChange(farmID, action)
	}
	server, err := NewServerChecked(options)
	if err != nil {
		t.Fatal(err)
	}
	activateFarmParticipantSubscription(t, rig.registry, 1310, 801, "sub-participant-a")
	activateFarmParticipantSubscription(t, rig.registry, 1310, 802, "sub-participant-b")

	payload, workflowID := ingressTestPayload(t, 1310, "farm-ingress-observed", 1)
	request := ingressTestRequest(t, rig.route, 1310, "farm-ingress-observed", payload, workflowID)
	first, err := server.ExecuteStep(context.Background(), request)
	if err != nil || first.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		t.Fatalf("first=%v err=%v", first, err)
	}
	if observed.Load() != 1 {
		t.Fatalf("observer calls=%d, want 1", observed.Load())
	}
	if mailboxAvailable.Load() != 1 {
		t.Fatalf("observer ran before Actor mailbox turn returned: mailbox result=%d", mailboxAvailable.Load())
	}
	changes := participantFarmChangeItems(t, downlink)
	if len(changes) != 2 {
		t.Fatalf("FarmChange count=%d, want one for each current subscription", len(changes))
	}
	for _, change := range changes {
		if change.GetOwnerId() != 1310 || change.GetFarmVersion() != 1 || len(change.GetChangedPlots()) != 1 {
			t.Fatalf("unexpected committed change=%v", change)
		}
	}

	replay, err := server.ExecuteStep(context.Background(), request)
	if err != nil || !replay.GetReplayedFromReceipt() || replay.GetOutcome() != first.GetOutcome() ||
		!bytes.Equal(replay.GetParticipantResultHash(), first.GetParticipantResultHash()) ||
		!bytes.Equal(replay.GetTypedResult(), first.GetTypedResult()) {
		t.Fatalf("replay=%v err=%v", replay, err)
	}
	if observed.Load() != 1 || len(participantFarmChangeItems(t, downlink)) != 2 {
		t.Fatalf("replay duplicated realtime publication: observer=%d changes=%d", observed.Load(), len(participantFarmChangeItems(t, downlink)))
	}
}

func TestFarmParticipantDownlinkRejectionResyncsWithoutChangingCommittedResult(t *testing.T) {
	rig := newFarmParticipantTestRig(t, 1311, authoritystore.Limits{})
	downlink := &failChangeDownlink{failChanges: true}
	service := NewService(rig.registry, nil)
	service.SetDownlinkEnqueuer(downlink)
	var observed atomic.Int32
	options := passingFarmParticipantOptions(rig.registry, func() time.Time { return time.UnixMilli(ingressTestNowMs) })
	options.CommitPlantObserver = func(farmID uint64, action *farmactor.ActionResult) {
		observed.Add(1)
		service.EmitChange(farmID, action)
	}
	server, err := NewServerChecked(options)
	if err != nil {
		t.Fatal(err)
	}
	const subscriptionID = "sub-participant-reject"
	activateFarmParticipantSubscription(t, rig.registry, 1311, 803, subscriptionID)
	payload, workflowID := ingressTestPayload(t, 1311, "farm-ingress-downlink-reject", 1)
	request := ingressTestRequest(t, rig.route, 1311, "farm-ingress-downlink-reject", payload, workflowID)
	response, err := server.ExecuteStep(context.Background(), request)
	if err != nil || response.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		t.Fatalf("committed Participant was rejected after downlink failure: response=%v err=%v", response, err)
	}
	if observed.Load() != 1 {
		t.Fatalf("observer calls=%d, want 1", observed.Load())
	}
	record, _, err := rig.registry.ReadOutsideMailbox(context.Background(), 1311)
	if err != nil {
		t.Fatal(err)
	}
	if record.GetCore().GetFarmVersion() != 1 || len(record.GetCore().GetCropCycles()) != 1 || len(record.GetRecentOperationReceipts()) != 1 {
		t.Fatalf("downlink failure altered committed Farm result: version=%d cycles=%d receipts=%d", record.GetCore().GetFarmVersion(), len(record.GetCore().GetCropCycles()), len(record.GetRecentOperationReceipts()))
	}
	if required := waitDownlinkRejoin(t, func() *farmpb.RejoinRequiredPush {
		return downlink.findRejoin(subscriptionID, farmpb.RejoinReason_REJOIN_REASON_DELIVERY_FAILED)
	}); required == nil {
		t.Fatal("downlink rejection did not produce REJOIN_REQUIRED")
	}
	waitFarmParticipantResyncOrCleanup(t, rig.registry, 1311, subscriptionID)
}

func TestFarmParticipantObserverSkipsRejectedAndReceiptOnlyPaths(t *testing.T) {
	rig := newFarmParticipantTestRig(t, 1312, authoritystore.Limits{})
	var observed atomic.Int32
	options := passingFarmParticipantOptions(rig.registry, func() time.Time { return time.UnixMilli(ingressTestNowMs) })
	options.CommitPlantObserver = func(uint64, *farmactor.ActionResult) { observed.Add(1) }
	server, err := NewServerChecked(options)
	if err != nil {
		t.Fatal(err)
	}
	locked, lockedWorkflow := ingressTestPayload(t, 1312, "farm-ingress-locked", 6)
	lockedResponse, err := server.ExecuteStep(context.Background(), ingressTestRequest(t, rig.route, 1312, "farm-ingress-locked", locked, lockedWorkflow))
	if err != nil || lockedResponse.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED {
		t.Fatalf("locked response=%v err=%v", lockedResponse, err)
	}
	if observed.Load() != 0 {
		t.Fatalf("observer called for rejected Participant: %d", observed.Load())
	}

	first, firstWorkflow := ingressTestPayload(t, 1312, "farm-ingress-applied-before-repeat", 1)
	if response, err := server.ExecuteStep(context.Background(), ingressTestRequest(t, rig.route, 1312, "farm-ingress-applied-before-repeat", first, firstWorkflow)); err != nil || response.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		t.Fatalf("first applied response=%v err=%v", response, err)
	}
	if observed.Load() != 1 {
		t.Fatalf("observer calls after applied Participant=%d, want 1", observed.Load())
	}
	repeat, repeatWorkflow := ingressTestPayload(t, 1312, "farm-ingress-nonempty-repeat", 1)
	repeatResponse, err := server.ExecuteStep(context.Background(), ingressTestRequest(t, rig.route, 1312, "farm-ingress-nonempty-repeat", repeat, repeatWorkflow))
	if err != nil || repeatResponse.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED {
		t.Fatalf("non-empty repeat response=%v err=%v", repeatResponse, err)
	}
	if observed.Load() != 1 {
		t.Fatalf("observer called for no-change/rejected repeat: %d", observed.Load())
	}
}

func TestFarmParticipantObserverPredicateSkipsNoChangeReplayAndMissingAction(t *testing.T) {
	change := &farmpb.FarmChange{OwnerId: 1314, FarmVersion: 1}
	cases := []struct {
		name string
		turn *farmactor.CommitPlantTurnResult
		want bool
	}{
		{name: "applied", turn: &farmactor.CommitPlantTurnResult{
			Outcome: &contracts.StepOutcome{Outcome: contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED},
			Action:  &farmactor.ActionResult{Change: change},
		}, want: true},
		{name: "rejected", turn: &farmactor.CommitPlantTurnResult{
			Outcome: &contracts.StepOutcome{Outcome: contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED},
			Action:  &farmactor.ActionResult{Change: change},
		}},
		{name: "no_change", turn: &farmactor.CommitPlantTurnResult{
			Outcome: &contracts.StepOutcome{Outcome: contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE},
			Action:  &farmactor.ActionResult{Change: change},
		}},
		{name: "receipt_replay", turn: &farmactor.CommitPlantTurnResult{
			Outcome: &contracts.StepOutcome{Outcome: contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED, ReplayedFromReceipt: true},
			Action:  &farmactor.ActionResult{Change: change},
		}},
		{name: "noncommitted", turn: &farmactor.CommitPlantTurnResult{
			Outcome: &contracts.StepOutcome{Outcome: contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_TIMEOUT_UNKNOWN},
		}},
		{name: "missing_change", turn: &farmactor.CommitPlantTurnResult{
			Outcome: &contracts.StepOutcome{Outcome: contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED},
			Action:  &farmactor.ActionResult{},
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if got := shouldObserveCommitPlant(test.turn); got != test.want {
				t.Fatalf("shouldObserveCommitPlant=%v, want %v", got, test.want)
			}
		})
	}
}

func TestFarmParticipantObserverSkipsNoncommittedCommit(t *testing.T) {
	rig := newFarmParticipantTestRig(t, 1313, authoritystore.Limits{})
	var observed atomic.Int32
	options := passingFarmParticipantOptions(rig.registry, func() time.Time { return time.UnixMilli(ingressTestNowMs) })
	options.CommitPlantObserver = func(uint64, *farmactor.ActionResult) { observed.Add(1) }
	server, err := NewServerChecked(options)
	if err != nil {
		t.Fatal(err)
	}
	payload, workflowID := ingressTestPayload(t, 1313, "farm-ingress-not-sent", 1)
	rig.fault.Inject(authoritystore.MethodCommit, authoritystore.FaultUnavailableNotSent)
	_, err = server.ExecuteStep(context.Background(), ingressTestRequest(t, rig.route, 1313, "farm-ingress-not-sent", payload, workflowID))
	if status.Code(err) != codes.Unavailable || observed.Load() != 0 {
		t.Fatalf("noncommitted commit err=%v observer=%d", err, observed.Load())
	}
	record, _, err := rig.registry.ReadOutsideMailbox(context.Background(), 1313)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.GetRecentOperationReceipts()) != 0 || len(record.GetCore().GetCropCycles()) != 0 || record.GetCore().GetFarmVersion() != 0 {
		t.Fatalf("noncommitted path changed Farm: version=%d cycles=%d receipts=%d", record.GetCore().GetFarmVersion(), len(record.GetCore().GetCropCycles()), len(record.GetRecentOperationReceipts()))
	}
}

func TestFarmParticipantServerCommitPlantReplayAndConflict(t *testing.T) {
	rig := newFarmParticipantTestRig(t, 1301, authoritystore.Limits{})
	server, err := NewServerChecked(passingFarmParticipantOptions(rig.registry, func() time.Time { return time.UnixMilli(ingressTestNowMs) }))
	if err != nil {
		t.Fatal(err)
	}
	payload, workflowID := ingressTestPayload(t, 1301, "farm-ingress-1", 1)
	request := ingressTestRequest(t, rig.route, 1301, "farm-ingress-1", payload, workflowID)
	first, err := server.ExecuteStep(context.Background(), request)
	if err != nil || first.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		t.Fatalf("first=%v err=%v", first, err)
	}
	second, err := server.ExecuteStep(context.Background(), request)
	if err != nil || !second.GetReplayedFromReceipt() || !bytes.Equal(first.GetParticipantResultHash(), second.GetParticipantResultHash()) || !bytes.Equal(first.GetTypedResult(), second.GetTypedResult()) {
		t.Fatalf("replay=%v err=%v", second, err)
	}
	mutated := proto.Clone(payload).(*contractspb.CommitPlantPayload)
	mutated.CropRuleSnapshot.GrowDurationMs++
	mutated.CropRuleSnapshotHash = mustIngressHash(t, mutated.CropRuleSnapshot)
	conflictRequest := ingressTestRequest(t, rig.route, 1301, "farm-ingress-1", mutated, workflowID)
	_, err = server.ExecuteStep(context.Background(), conflictRequest)
	if status.Code(err) != codes.FailedPrecondition || !bytes.Contains([]byte(err.Error()), []byte("IDEMPOTENCY_CONFLICT")) {
		t.Fatalf("conflict err=%v", err)
	}
	record, _, err := rig.registry.ReadOutsideMailbox(context.Background(), 1301)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.GetCore().GetCropCycles()) != 1 || len(record.GetRecentOperationReceipts()) != 1 {
		t.Fatalf("cycles=%d receipts=%d", len(record.GetCore().GetCropCycles()), len(record.GetRecentOperationReceipts()))
	}
}

func TestFarmParticipantServerTimeoutUnknownReturnsProcessingAndReplaysAfterResolve(t *testing.T) {
	rig := newFarmParticipantTestRig(t, 1305, authoritystore.Limits{})
	server, err := NewServerChecked(passingFarmParticipantOptions(rig.registry, func() time.Time { return time.UnixMilli(ingressTestNowMs) }))
	if err != nil {
		t.Fatal(err)
	}
	payload, workflowID := ingressTestPayload(t, 1305, "farm-ingress-unknown", 1)
	request := ingressTestRequest(t, rig.route, 1305, "farm-ingress-unknown", payload, workflowID)
	rig.fault.Inject(authoritystore.MethodCommit, authoritystore.FaultTimeoutUnknownAfterApply)
	unknown, err := server.ExecuteStep(context.Background(), request)
	if err != nil || unknown.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_TIMEOUT_UNKNOWN || unknown.GetTypedResult() != nil {
		t.Fatalf("unknown=%v err=%v", unknown, err)
	}
	if unknown.GetFailure().GetError().GetErrorCode() != commonpb.ErrorCode_ERR_TIMEOUT || !unknown.GetFailure().GetRetryableAfterResolution() {
		t.Fatalf("unknown failure=%v", unknown.GetFailure())
	}
	deadline := time.Now().Add(2 * time.Second)
	for rig.registry.Activation().StateOf(1305) != activation.Active && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if rig.registry.Activation().StateOf(1305) != activation.Active {
		t.Fatal("unknown did not resolve back to ACTIVE")
	}
	replayed, err := server.ExecuteStep(context.Background(), request)
	if err != nil || !replayed.GetReplayedFromReceipt() || replayed.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		t.Fatalf("resolved replay=%v err=%v", replayed, err)
	}
}

func TestFarmParticipantServerPendingUnknownRetryRemainsTimeoutUnknown(t *testing.T) {
	rig := newFarmParticipantTestRig(t, 1306, authoritystore.Limits{})
	rig.registry.SetAutoResolveUnknown(false)
	server, err := NewServerChecked(passingFarmParticipantOptions(rig.registry, func() time.Time { return time.UnixMilli(ingressTestNowMs) }))
	if err != nil {
		t.Fatal(err)
	}
	payload, workflowID := ingressTestPayload(t, 1306, "farm-ingress-pending-unknown", 1)
	request := ingressTestRequest(t, rig.route, 1306, "farm-ingress-pending-unknown", payload, workflowID)
	rig.fault.Inject(authoritystore.MethodCommit, authoritystore.FaultTimeoutUnknownAfterApply)
	first, err := server.ExecuteStep(context.Background(), request)
	if err != nil || first.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_TIMEOUT_UNKNOWN {
		t.Fatalf("first unknown=%v err=%v", first, err)
	}
	// Receipt lookup and capacity admission both see the same ResolvingUnknown
	// lifecycle and must return a retryable Participant outcome, not a generic
	// activation or capacity transport error.
	second, err := server.ExecuteStep(context.Background(), request)
	if err != nil || second.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_TIMEOUT_UNKNOWN || !second.GetFailure().GetRetryableAfterResolution() {
		t.Fatalf("pending retry=%v err=%v", second, err)
	}
}

func TestFarmParticipantCapacityErrorsKeepNonCapacitySemantics(t *testing.T) {
	tests := []struct {
		name string
		err  error
		code codes.Code
		msg  string
	}{
		{name: "physical reject", err: farmactor.ErrParticipantPhysicalCapacity, code: codes.ResourceExhausted, msg: "ERR_RATE_LIMITED"},
		{name: "snapshot unavailable", err: farmactor.ErrParticipantCapacityUnavailable, code: codes.Unavailable, msg: "CAPACITY_UNAVAILABLE"},
		{name: "timeout unknown", err: blobstore.ErrTimeoutUnknown, code: codes.Unavailable, msg: "TIMEOUT_UNKNOWN"},
		{name: "writer fence", err: blobstore.ErrWriterEpochMismatch, code: codes.FailedPrecondition, msg: "WRITER_EPOCH_MISMATCH"},
		{name: "authority unavailable", err: blobstore.ErrUnavailable, code: codes.Unavailable, msg: "AUTHORITY_UNAVAILABLE"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := normalizeFarmParticipantError(test.err)
			if status.Code(err) != test.code || !bytes.Contains([]byte(err.Error()), []byte(test.msg)) {
				t.Fatalf("normalized=%v code=%s, want code=%s message=%q", err, status.Code(err), test.code, test.msg)
			}
		})
	}
}

func TestFarmParticipantServerDependencyRejectsBeforeMailbox(t *testing.T) {
	rig := newFarmParticipantTestRig(t, 1302, authoritystore.Limits{})
	options := passingFarmParticipantOptions(rig.registry, func() time.Time { return time.UnixMilli(ingressTestNowMs) })
	var dependencyCalls atomic.Int32
	options.CommitPlantDependency = CommitPlantDependencyVerifierFunc(func(context.Context, *contracts.ParticipantCall) error {
		dependencyCalls.Add(1)
		return status.Error(codes.FailedPrecondition, "RESERVE_SEED_RESULT_HASH_MISMATCH")
	})
	server, err := NewServerChecked(options)
	if err != nil {
		t.Fatal(err)
	}
	payload, workflowID := ingressTestPayload(t, 1302, "farm-ingress-proof-reject", 1)
	_, err = server.ExecuteStep(context.Background(), ingressTestRequest(t, rig.route, 1302, "farm-ingress-proof-reject", payload, workflowID))
	if status.Code(err) != codes.FailedPrecondition || dependencyCalls.Load() != 1 {
		t.Fatalf("dependency err=%v calls=%d", err, dependencyCalls.Load())
	}
	record, _, err := rig.registry.ReadOutsideMailbox(context.Background(), 1302)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.GetRecentOperationReceipts()) != 0 || len(record.GetCore().GetCropCycles()) != 0 {
		t.Fatalf("pre-mailbox rejection wrote Farm: receipts=%d cycles=%d", len(record.GetRecentOperationReceipts()), len(record.GetCore().GetCropCycles()))
	}
}

func TestFarmParticipantServerDeadlineAfterVerifierNeverEntersMailbox(t *testing.T) {
	rig := newFarmParticipantTestRig(t, 1303, authoritystore.Limits{})
	start := time.UnixMilli(ingressTestNowMs)
	exhausted := false
	options := passingFarmParticipantOptions(rig.registry, func() time.Time {
		if exhausted {
			return start.Add(5 * time.Second)
		}
		return start
	})
	options.WorkVersionVerifier = WorkVersionVerifierFunc(func(context.Context, *contracts.ParticipantCall) error {
		exhausted = true
		return nil
	})
	server, err := NewServerChecked(options)
	if err != nil {
		t.Fatal(err)
	}
	payload, workflowID := ingressTestPayload(t, 1303, "farm-ingress-deadline", 1)
	_, err = server.ExecuteStep(context.Background(), ingressTestRequest(t, rig.route, 1303, "farm-ingress-deadline", payload, workflowID))
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("deadline err=%v", err)
	}
	record, _, err := rig.registry.ReadOutsideMailbox(context.Background(), 1303)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.GetRecentOperationReceipts()) != 0 {
		t.Fatal("deadline-after-verifier entered mailbox and wrote a receipt")
	}
}

func TestFarmParticipantServerCapacityIsStableResourceExhausted(t *testing.T) {
	limits := authoritystore.DefaultLimits()
	// The 24-plot genesis model plus opportunity_header/opportunity_facts must
	// fit so this test reaches the Participant receipt boundary it exercises.
	limits.MaxModelBytes = 16 * 1024
	limits.MaxReceiptBytes = 128
	limits.MaxSingleReceiptBytes = 128
	limits.MaxCommitBytes = 16 * 1024
	rig := newFarmParticipantTestRig(t, 1304, limits)
	server, err := NewServerChecked(passingFarmParticipantOptions(rig.registry, func() time.Time { return time.UnixMilli(ingressTestNowMs) }))
	if err != nil {
		t.Fatal(err)
	}
	payload, workflowID := ingressTestPayload(t, 1304, "farm-ingress-capacity", 1)
	_, err = server.ExecuteStep(context.Background(), ingressTestRequest(t, rig.route, 1304, "farm-ingress-capacity", payload, workflowID))
	if status.Code(err) != codes.ResourceExhausted || !bytes.Contains([]byte(err.Error()), []byte("ERR_RATE_LIMITED")) {
		t.Fatalf("capacity err=%v", err)
	}
	record, _, err := rig.registry.ReadOutsideMailbox(context.Background(), 1304)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.GetRecentOperationReceipts()) != 0 {
		t.Fatal("capacity rejection wrote a receipt")
	}
}

func TestFarmParticipantServerCommitHarvestPublishesOnceAndReplaysDecision(t *testing.T) {
	const farmID = uint64(1320)
	rig := newFarmParticipantTestRig(t, farmID, authoritystore.Limits{})
	downlink := &resultCaptureDownlink{}
	service := NewService(rig.registry, nil)
	service.SetDownlinkEnqueuer(downlink)
	nowMs := int64(ingressTestNowMs)
	options := passingFarmParticipantOptions(rig.registry, func() time.Time { return time.UnixMilli(nowMs) })
	options.NowMs = func() int64 { return nowMs }
	var observed atomic.Int32
	options.CommitHarvestObserver = func(farmID uint64, action *farmactor.ActionResult) {
		observed.Add(1)
		service.EmitChange(farmID, action)
	}
	server, err := NewServerChecked(options)
	if err != nil {
		t.Fatal(err)
	}
	plantPayload, plantWorkflow := ingressTestPayload(t, farmID, "harvest-ingress-plant", 1)
	plantResponse, err := server.ExecuteStep(context.Background(), ingressTestRequest(t, rig.route, farmID, "harvest-ingress-plant", plantPayload, plantWorkflow))
	if err != nil || plantResponse.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		t.Fatalf("plant response=%v err=%v", plantResponse, err)
	}
	var plantResult contractspb.CommitPlantResult
	if err := proto.Unmarshal(plantResponse.GetTypedResult(), &plantResult); err != nil {
		t.Fatal(err)
	}
	activateFarmParticipantSubscription(t, rig.registry, farmID, 1901, "sub-harvest-participant")
	nowMs += 60_000
	harvestPayload, harvestWorkflow := ingressHarvestPayload(t, farmID, "harvest-ingress-commit", plantResult.GetCropCycleId(), 1, plantResult.GetCommittedPlotVersion())
	harvestRequest := ingressHarvestRequest(t, rig.route, farmID, "harvest-ingress-commit", harvestPayload, harvestWorkflow)
	first, err := server.ExecuteStep(context.Background(), harvestRequest)
	if err != nil || first.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		t.Fatalf("harvest first=%v err=%v", first, err)
	}
	var harvestResult contractspb.CommitHarvestResult
	if err := proto.Unmarshal(first.GetTypedResult(), &harvestResult); err != nil {
		t.Fatal(err)
	}
	if harvestResult.GetHarvestDecision().GetRewardQuantity() != 250 || len(harvestResult.GetHarvestDecision().GetHarvestDecisionHash()) != 32 {
		t.Fatalf("harvest result=%v", harvestResult.String())
	}
	replay, err := server.ExecuteStep(context.Background(), harvestRequest)
	if err != nil || !replay.GetReplayedFromReceipt() || !bytes.Equal(first.GetTypedResult(), replay.GetTypedResult()) ||
		!bytes.Equal(first.GetParticipantResultHash(), replay.GetParticipantResultHash()) {
		t.Fatalf("harvest replay=%v err=%v", replay, err)
	}
	if observed.Load() != 1 || len(participantFarmChangeItems(t, downlink)) != 1 {
		t.Fatalf("harvest publication observer=%d changes=%d", observed.Load(), len(participantFarmChangeItems(t, downlink)))
	}
	record, _, err := rig.registry.ReadOutsideMailbox(context.Background(), farmID)
	if err != nil {
		t.Fatal(err)
	}
	if record.GetCore().GetFarmVersion() != 2 || len(record.GetCore().GetCropCycles()) != 0 || len(record.GetRecentOperationReceipts()) != 2 {
		t.Fatalf("harvest record=%v", record)
	}
}

func TestFarmParticipantServerCommitHarvestFailsClosedWithoutDependencyProof(t *testing.T) {
	const farmID = uint64(1321)
	rig := newFarmParticipantTestRig(t, farmID, authoritystore.Limits{})
	options := passingFarmParticipantOptions(rig.registry, func() time.Time { return time.UnixMilli(ingressTestNowMs) })
	options.CommitHarvestDependency = nil
	server, err := NewServerChecked(options)
	if err != nil {
		t.Fatal(err)
	}
	payload, workflowID := ingressHarvestPayload(t, farmID, "harvest-ingress-no-proof", "cycle-not-proven", 1, 1)
	_, err = server.ExecuteStep(context.Background(), ingressHarvestRequest(t, rig.route, farmID, "harvest-ingress-no-proof", payload, workflowID))
	if status.Code(err) != codes.FailedPrecondition || !bytes.Contains([]byte(err.Error()), []byte("dependency verifier is not configured")) {
		t.Fatalf("missing dependency err=%v", err)
	}
	record, _, err := rig.registry.ReadOutsideMailbox(context.Background(), farmID)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.GetRecentOperationReceipts()) != 0 || record.GetCore().GetFarmVersion() != 0 {
		t.Fatalf("missing dependency entered Farm mailbox: %v", record)
	}
}

func TestFarmParticipantServerCommitHarvestResponseLossReplaysWithoutObserver(t *testing.T) {
	const farmID = uint64(1322)
	rig := newFarmParticipantTestRig(t, farmID, authoritystore.Limits{})
	nowMs := int64(ingressTestNowMs)
	options := passingFarmParticipantOptions(rig.registry, func() time.Time { return time.UnixMilli(nowMs) })
	options.NowMs = func() int64 { return nowMs }
	var observed atomic.Int32
	options.CommitHarvestObserver = func(uint64, *farmactor.ActionResult) { observed.Add(1) }
	server, err := NewServerChecked(options)
	if err != nil {
		t.Fatal(err)
	}
	plantPayload, plantWorkflow := ingressTestPayload(t, farmID, "harvest-response-loss-plant", 1)
	plantResponse, err := server.ExecuteStep(context.Background(), ingressTestRequest(t, rig.route, farmID, "harvest-response-loss-plant", plantPayload, plantWorkflow))
	if err != nil {
		t.Fatal(err)
	}
	var plantResult contractspb.CommitPlantResult
	if err := proto.Unmarshal(plantResponse.GetTypedResult(), &plantResult); err != nil {
		t.Fatal(err)
	}
	nowMs += 60_000
	payload, workflowID := ingressHarvestPayload(t, farmID, "harvest-response-loss", plantResult.GetCropCycleId(), 1, plantResult.GetCommittedPlotVersion())
	request := ingressHarvestRequest(t, rig.route, farmID, "harvest-response-loss", payload, workflowID)
	rig.fault.Inject(authoritystore.MethodCommit, authoritystore.FaultTimeoutUnknownAfterApply)
	unknown, err := server.ExecuteStep(context.Background(), request)
	if err != nil || unknown.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_TIMEOUT_UNKNOWN {
		t.Fatalf("unknown=%v err=%v", unknown, err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for rig.registry.Activation().StateOf(farmID) != activation.Active && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if rig.registry.Activation().StateOf(farmID) != activation.Active {
		t.Fatal("Harvest UNKNOWN did not resolve")
	}
	replay, err := server.ExecuteStep(context.Background(), request)
	if err != nil || !replay.GetReplayedFromReceipt() || replay.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		t.Fatalf("replay=%v err=%v", replay, err)
	}
	if observed.Load() != 0 {
		t.Fatalf("response-loss retry invoked observer=%d", observed.Load())
	}
}

func TestFarmParticipantRegistrationRegistersOnlySharedParticipantServices(t *testing.T) {
	server := NewServer(Options{})
	if server == nil || server.handler == nil {
		t.Fatal("server was not constructed")
	}
	if _, ok := server.handler.(ReceiptResolver); !ok {
		t.Fatal("default handler lacks durable receipt resolver")
	}
	if err := server.Register(nil); err == nil {
		t.Fatal("nil registrar must fail closed")
	}
}
