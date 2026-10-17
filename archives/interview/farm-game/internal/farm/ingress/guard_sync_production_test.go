package ingress

import (
	"bytes"
	"context"
	"crypto/sha256"
	"strconv"
	"testing"
	"time"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actorrouting"
	"farmgame/internal/contracts"
	farmactor "farmgame/internal/farm/actor"
	farmdomain "farmgame/internal/farm/domain"
	"farmgame/internal/identity"
	"farmgame/internal/logic/actor/authority"
	"farmgame/internal/storage/authoritystore"
	redisauthority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workflow"
	"farmgame/internal/workflow/participantproof"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

func productionGuardSyncTheftCall(t *testing.T, farmID, visitorID uint64, cycleID string, nowMs int64) *contracts.ParticipantCall {
	t.Helper()
	operationID := "guard-sync-production-theft"
	pairID, err := identity.CanonicalFriendRelationID(farmID, visitorID)
	if err != nil {
		t.Fatal(err)
	}
	workflowID, err := identity.WorkflowID(identity.EffectiveIdempotencyKey{
		SubjectPlayerID: visitorID, StableCommandTypeName: "THEFT", OperationID: operationID,
		Root: identity.OperationRootRef{Kind: identity.RootKindFriendPair, CanonicalID: pairID},
	})
	if err != nil {
		t.Fatal(err)
	}
	attemptID, err := identity.TheftAttemptID(farmID, cycleID, visitorID)
	if err != nil {
		t.Fatal(err)
	}
	settlementID, err := identity.TheftSettlementID(workflowID)
	if err != nil {
		t.Fatal(err)
	}
	permit := &contractspb.ActionPermitSnapshot{
		PermitId: "permit-production-guard-sync", VisitorPlayerId: visitorID, OwnerPlayerId: farmID,
		TargetFarmId: farmID, OperationId: operationID,
		ActionScope: contractspb.FriendActionScope_FRIEND_ACTION_SCOPE_THEFT, FriendRelationEpoch: 1,
		CanonicalPayloadHash: bytes.Repeat([]byte{0x51}, 32), TargetBindingHash: bytes.Repeat([]byte{0x52}, 32),
		IssuedAtAuthorityMs: nowMs - 10, ExpireAtAuthorityMs: nowMs + 10_000,
		ExecutionAcceptedAtAuthorityMs: nowMs,
	}
	permitHash, err := contracts.CanonicalHash(permit)
	if err != nil {
		t.Fatal(err)
	}
	payload := &contractspb.DecideTheftPayload{
		VisitorPlayerId: visitorID, OwnerPlayerId: farmID, FarmId: farmID, PlotId: 1,
		CropCycleId: cycleID, TheftAttemptId: attemptID, SettlementId: settlementID,
		ActionPermitSnapshot: permit, ActionPermitSnapshotHash: permitHash[:],
	}
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT)
	if err != nil {
		t.Fatal(err)
	}
	stepHash, err := contracts.CanonicalParticipantHash(spec, payload)
	if err != nil {
		t.Fatal(err)
	}
	invocationID, err := identity.ParticipantInvocationID(identity.WorkTypeTheft, workflowID, identity.StepDecideTheft, 1)
	if err != nil {
		t.Fatal(err)
	}
	return &contracts.ParticipantCall{
		Spec: spec, RouteHeader: &logicrpcpb.ActorRouteHeader{RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM, ActorId: farmID},
		Work: &contractspb.WorkAddress{WorkId: workflowID, WorkType: contractspb.WorkType_WORK_TYPE_THEFT,
			PlacementKind: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_INDEPENDENT_WORK_BUCKET, BucketVersion: 1},
		CallerKind: contracts.ParticipantCallerCoordinator, ParticipantStep: string(spec.Step), InvocationID: invocationID,
		OperationID: operationID, SubjectPlayerID: visitorID, ExpectedWorkVersion: 1, RemainingBudgetMs: 5_000,
		Payload: payload, CanonicalStepPayloadHash: stepHash,
	}
}

func TestGuardSyncRedisProductionCompositionHotActorThenTheft(t *testing.T) {
	const (
		farmID    = uint64(1330)
		visitorID = uint64(2330)
	)
	ctx := context.Background()
	now := time.UnixMilli(ingressTestNowMs)
	server := miniredis.RunT(t)
	server.SetTime(now)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	namespace, err := rediskey.NewNamespace("test", "guard-sync-production")
	if err != nil {
		t.Fatal(err)
	}
	profile := rediskey.PhysicalProfile{
		ProfileVersion: "p1", Namespace: namespace, AuthorityLayoutVersion: "v1",
		WorkflowLayoutVersion: "v1", WorkflowHashVersion: 1, WorkflowBucketCount: 64,
	}
	calculator, err := actorrouting.NewCalculator("guard-sync-production",
		actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608050001}, 64, 64)
	if err != nil {
		t.Fatal(err)
	}
	store, err := redisauthority.NewStore(client, nil, redisauthority.Options{Profile: profile, Resolver: calculator})
	if err != nil {
		t.Fatal(err)
	}
	claimAuthority := func(domain actorrouting.RouteDomain, actorID uint64, holder string) authoritystore.ShardWriteFence {
		shard, shardErr := calculator.ShardFor(domain, actorID)
		if shardErr != nil {
			t.Fatal(shardErr)
		}
		claimed, claimErr := store.ClaimShardAuthority(ctx, authoritystore.ClaimShardAuthorityRequest{
			Caller: authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerRouteManager,
				InstanceID: "guard-sync-route-manager", BootID: "guard-sync-route-manager-boot"},
			TransitionID: "claim-" + string(domain), AuthorityLayoutVersion: "v1", Domain: domain,
			VirtualShard: shard, NewHolderID: holder, NewHolderBootID: holder + "-boot", RequestedTTLms: 60_000,
		})
		if claimErr != nil || claimed.Status != authoritystore.ShardTransitionApplied {
			t.Fatalf("claim %s authority=%+v err=%v", domain, claimed, claimErr)
		}
		return authoritystore.ShardWriteFence{
			ExpectedHolderID: claimed.Authority.HolderID, ExpectedHolderBootID: claimed.Authority.HolderBootID,
			ExpectedAssignmentRevision: claimed.Authority.AssignmentRevision,
			ExpectedWriterEpoch:        claimed.Authority.WriterEpoch, ExpectedLeaseEpoch: claimed.Authority.LeaseEpoch,
		}
	}
	farmFence := claimAuthority(actorrouting.DomainFarm, farmID, "farm-holder")
	playerFence := claimAuthority(actorrouting.DomainPlayer, farmID, "player-holder")
	farmProvider, err := authority.NewProvider(authority.ProviderOptions{
		Store: store, Fence: authority.StaticFenceSource{Fence: farmFence},
		AuthorityLayoutVersion: "v1", BootID: farmFence.ExpectedHolderBootID,
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := farmactor.NewAuthorityRegistry(farmProvider, farmactor.RegistryOptions{
		MailboxSize: 16, TheftPRF: func(string, farmdomain.GuardSnapshot) (uint32, error) { return 0, nil },
	})
	t.Cleanup(registry.Stop)
	farmShard, err := calculator.ShardFor(actorrouting.DomainFarm, farmID)
	if err != nil {
		t.Fatal(err)
	}
	farmRoute := &logicrpcpb.ActorRouteHeader{
		RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM, ActorId: farmID, VirtualShard: farmShard,
		ShardRevision: farmFence.ExpectedAssignmentRevision, ExpectedHolderId: farmFence.ExpectedHolderID,
		ExpectedHolderBootId: farmFence.ExpectedHolderBootID, ExpectedWriterEpoch: farmFence.ExpectedWriterEpoch,
		ExpectedLeaseEpoch: farmFence.ExpectedLeaseEpoch, ExpectedLeaseUntilMs: ingressTestLeaseDeadline(),
	}
	plantPayload, plantWorkflowID := ingressTestPayload(t, farmID, "guard-sync-production-plant", 1)
	plantCall, err := contracts.ValidateAndDecode(ingressTestRequest(t, farmRoute, farmID,
		"guard-sync-production-plant", plantPayload, plantWorkflowID), contracts.ParticipantCallerCoordinator)
	if err != nil {
		t.Fatal(err)
	}
	plantValue, err := registry.SubmitParticipant(ctx, farmID, func(ctx context.Context, actor *farmactor.FarmActor, guard farmactor.ParticipantCapacityGuard) (any, error) {
		return actor.CommitPlant(ctx, plantCall, guard, func() int64 { return ingressTestNowMs })
	})
	if err != nil {
		t.Fatal(err)
	}
	plantOutcome := plantValue.(*contracts.StepOutcome)
	if plantOutcome.Outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		t.Fatalf("plant outcome=%+v", plantOutcome)
	}
	cycleID := plantOutcome.Result.(*contractspb.CommitPlantResult).GetCropCycleId()

	request := ingressGuardSyncRequest(t, farmRoute, farmID, 1, 7, "guard-sync-source-operation")
	playerBucket, err := profile.AuthorityBucketForActor(calculator,
		authoritystore.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: farmID})
	if err != nil {
		t.Fatal(err)
	}
	request.GetCoordinator().GetWork().BucketId = playerBucket.VirtualShard()
	playerLoad, err := store.LoadAndClaim(ctx, authoritystore.LoadAndClaimRequest{
		ActorAddress:           authoritystore.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: farmID},
		AuthorityLayoutVersion: "v1", ShardWriteFence: playerFence,
		ClaimOperationID: "guard-sync-player-claim", NewWriterClaimID: "guard-sync-player-writer",
		InitialModelFields: authoritystore.ModelState{"pet": []byte("guard-v1")},
	})
	if err != nil || playerLoad.Status != authoritystore.LoadAndClaimClaimed {
		t.Fatalf("player load=%+v err=%v", playerLoad, err)
	}
	rootHash := sha256.Sum256([]byte("guard-sync-source-operation"))
	key := authoritystore.IdempotencyKey{
		SubjectPlayerID: farmID, StableCommandTypeName: "PET_SWITCH", OperationID: "guard-sync-source-operation",
		Root: authoritystore.OperationRootRef{Kind: authoritystore.RootKindPlayerActor, CanonicalID: strconv.FormatUint(farmID, 10)},
	}
	outcome := authoritystore.StoredOperationOutcome{Status: authoritystore.TerminalSucceeded, Payload: []byte("guard-v1")}
	committed, err := store.Commit(ctx, authoritystore.AuthorityCommitRequest{
		CommitRequestID: "commit-guard-sync-source", ActorAddress: authoritystore.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: farmID},
		AuthorityLayoutVersion: "v1", ShardWriteFence: playerFence, ExpectedWriterClaimID: "guard-sync-player-writer",
		ExpectedRecordVersion: playerLoad.Snapshot.RecordVersion, IdempotencyKey: key,
		CanonicalPayloadHash: rootHash, CandidateState: playerLoad.Snapshot.ModelFields,
		Receipt: authoritystore.AuthorityReceipt{ReceiptID: "receipt-guard-sync-source", IdempotencyKey: key,
			CanonicalPayloadHash: rootHash, Outcome: outcome},
		OperationResult: outcome, AttachedOrders: []authoritystore.AttachedOrder{{
			OrderID: request.GetCoordinator().GetWork().GetWorkId(), OrderType: "GUARD_SYNC_ORDER",
			Payload: append([]byte(nil), request.GetTypedPayload()...), DueAtMs: ingressTestNowMs,
		}},
	})
	if err != nil || committed.Status != authoritystore.CommitCommitted {
		t.Fatalf("source GuardSyncOrder commit=%+v err=%v", committed, err)
	}

	placement := rediskey.PlacementResolver{Profile: profile, Shards: calculator}
	proof, err := participantproof.New(workflow.NewMemoryWorkRepository(func() time.Time { return now }),
		participantproof.WithBucketResolver(placement), participantproof.WithSourceWorkReader(store),
		participantproof.WithRouteFenceVerifier(participantproof.RouteFenceVerifierFunc(func(_ context.Context, call *contracts.ParticipantCall) error {
			header := call.RouteHeader
			if header.GetActorId() != farmID || header.GetVirtualShard() != request.GetRouteHeader().GetVirtualShard() ||
				header.GetExpectedHolderId() != farmFence.ExpectedHolderID ||
				header.GetExpectedHolderBootId() != farmFence.ExpectedHolderBootID ||
				header.GetShardRevision() != farmFence.ExpectedAssignmentRevision ||
				header.GetExpectedWriterEpoch() != farmFence.ExpectedWriterEpoch {
				return authoritystore.ErrStaleLeaseGrant
			}
			return nil
		})), participantproof.WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	participant, err := NewServerChecked(Options{
		Registry: registry, WorkVersionVerifier: proof, WorkLeaseVerifier: proof,
		RouteFenceVerifier: proof, WorkAddressVerifier: proof, IdentityBindingVerifier: proof,
		CommitPlantDependency: proof, CommitHarvestDependency: proof, DecideTheftDependency: proof,
		NowMs: func() int64 { return ingressTestNowMs + 1 }, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	guardResponse, err := participant.ExecuteStep(ctx, request)
	if err != nil || guardResponse.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		t.Fatalf("production GuardSync response=%+v err=%v", guardResponse, err)
	}
	theftCall := productionGuardSyncTheftCall(t, farmID, visitorID, cycleID, ingressTestNowMs+60_000)
	value, err := registry.SubmitParticipant(ctx, farmID, func(ctx context.Context, actor *farmactor.FarmActor, guard farmactor.ParticipantCapacityGuard) (any, error) {
		return actor.DecideTheft(ctx, theftCall, guard, func() int64 { return ingressTestNowMs + 60_000 })
	})
	if err != nil {
		t.Fatal(err)
	}
	theft := value.(*contracts.StepOutcome)
	decision := theft.Result.(*contractspb.DecideTheftResult).GetTheftTargetDecision()
	if theft.Outcome != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED ||
		decision.GetTheftOutcome() != contractspb.TheftOutcome_THEFT_OUTCOME_BLOCKED_BY_GUARD ||
		decision.GetGuardDecisionSnapshot().GetGuardVersion() != 1 || decision.GetNetReceivedQuantity() != 0 {
		t.Fatalf("next-turn Theft did not observe production GuardSync: outcome=%s decision=%+v", theft.Outcome, decision)
	}
	var applied contractspb.ApplyGuardSnapshotResult
	if err := proto.Unmarshal(guardResponse.GetTypedResult(), &applied); err != nil || applied.GetGuardVersion() != 1 {
		t.Fatalf("GuardSync typed result=%+v err=%v", &applied, err)
	}

	// The same production endpoint also accepts the existing worker-recovery
	// context after the source order has acquired its Redis lease. A newer
	// disabled snapshot must be visible to the very next actor turn.
	v2 := ingressGuardSyncRequest(t, farmRoute, farmID, 2, 0, "guard-sync-source-operation-v2")
	v2Work := proto.Clone(v2.GetCoordinator().GetWork()).(*contractspb.WorkAddress)
	v2Work.BucketId = playerBucket.VirtualShard()
	v2.GetCoordinator().Work = v2Work
	v2Hash := sha256.Sum256([]byte("guard-sync-source-operation-v2"))
	v2Key := authoritystore.IdempotencyKey{
		SubjectPlayerID: farmID, StableCommandTypeName: "PET_SWITCH", OperationID: "guard-sync-source-operation-v2",
		Root: authoritystore.OperationRootRef{Kind: authoritystore.RootKindPlayerActor, CanonicalID: strconv.FormatUint(farmID, 10)},
	}
	v2Outcome := authoritystore.StoredOperationOutcome{Status: authoritystore.TerminalSucceeded, Payload: []byte("guard-v2-disabled")}
	v2Commit, err := store.Commit(ctx, authoritystore.AuthorityCommitRequest{
		CommitRequestID: "commit-guard-sync-source-v2", ActorAddress: authoritystore.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: farmID},
		AuthorityLayoutVersion: "v1", ShardWriteFence: playerFence, ExpectedWriterClaimID: "guard-sync-player-writer",
		ExpectedRecordVersion: committed.Snapshot.RecordVersion, IdempotencyKey: v2Key,
		CanonicalPayloadHash: v2Hash, CandidateState: committed.Snapshot.ModelFields,
		Receipt: authoritystore.AuthorityReceipt{ReceiptID: "receipt-guard-sync-source-v2", IdempotencyKey: v2Key,
			CanonicalPayloadHash: v2Hash, Outcome: v2Outcome},
		OperationResult: v2Outcome, AttachedOrders: []authoritystore.AttachedOrder{{
			OrderID: v2Work.GetWorkId(), OrderType: "GUARD_SYNC_ORDER",
			Payload: append([]byte(nil), v2.GetTypedPayload()...), DueAtMs: ingressTestNowMs,
		}},
	})
	if err != nil || v2Commit.Status != authoritystore.CommitCommitted {
		t.Fatalf("source GuardSyncOrder v2 commit=%+v err=%v", v2Commit, err)
	}
	claim, err := store.WorkLease().ClaimWorkWithBoot(ctx, playerBucket, "GUARD_SYNC_ORDER", v2Work.GetWorkId(),
		"guard-sync-worker", "guard-sync-worker-boot", "guard-sync-worker-token", 30_000)
	if err != nil || claim.Status != redisauthority.WorkClaimClaimed {
		t.Fatalf("claim GuardSyncOrder v2=%+v err=%v", claim, err)
	}
	v2Invocation := v2.GetCoordinator().GetParticipantInvocationId()
	v2.CallerContext = &contractspb.ParticipantRequest_Recovery{Recovery: &contractspb.InternalCallerContext{
		CallerType:       contractspb.InternalCallerType_INTERNAL_CALLER_TYPE_WORKER_RECOVERY,
		CallerInstanceId: "guard-sync-worker", CallerBootId: "guard-sync-worker-boot",
		Work: v2Work, ExpectedWorkVersion: claim.WorkVersion,
		ParticipantStep:         contractspb.ParticipantStep_PARTICIPANT_STEP_APPLY_GUARD_SNAPSHOT,
		ParticipantInvocationId: v2Invocation, LeaseOwnerToken: "guard-sync-worker-token",
		LeaseEpoch: claim.LeaseEpoch, RemainingBudgetMs: 5_000,
	}}
	v2Response, err := participant.ResumeStep(ctx, v2)
	if err != nil || v2Response.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		t.Fatalf("worker GuardSync v2 response=%+v err=%v", v2Response, err)
	}
	theftAfterV2 := productionGuardSyncTheftCall(t, farmID, visitorID+1, cycleID, ingressTestNowMs+60_001)
	value, err = registry.SubmitParticipant(ctx, farmID, func(ctx context.Context, actor *farmactor.FarmActor, guard farmactor.ParticipantCapacityGuard) (any, error) {
		return actor.DecideTheft(ctx, theftAfterV2, guard, func() int64 { return ingressTestNowMs + 60_001 })
	})
	if err != nil {
		t.Fatal(err)
	}
	afterV2 := value.(*contracts.StepOutcome).Result.(*contractspb.DecideTheftResult).GetTheftTargetDecision()
	if afterV2.GetTheftOutcome() != contractspb.TheftOutcome_THEFT_OUTCOME_STOLEN ||
		afterV2.GetGuardDecisionSnapshot().GetGuardVersion() != 2 || afterV2.GetNetReceivedQuantity() == 0 {
		t.Fatalf("next-turn Theft did not observe worker-applied disabled v2: %+v", afterV2)
	}
}
