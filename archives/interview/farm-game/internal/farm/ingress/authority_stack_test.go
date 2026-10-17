package ingress

import (
	"context"
	"testing"
	"time"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actormodel/fieldv2"
	"farmgame/internal/actorrouting"
	"farmgame/internal/contracts"
	farmactor "farmgame/internal/farm/actor"
	"farmgame/internal/friend"
	"farmgame/internal/identity"
	"farmgame/internal/logic/actor/authority"
	playeractor "farmgame/internal/player/actor"
	playeringress "farmgame/internal/player/ingress"
	playerparticipant "farmgame/internal/player/participant"
	playertask "farmgame/internal/player/task"
	"farmgame/internal/storage/authoritystore"
	redisauthority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/worker/carerecovery"
	"farmgame/internal/worker/sourceorder"
	"farmgame/internal/workflow"
	"farmgame/internal/workidentity"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

func newSourceOrderTestStack(t *testing.T, current *time.Time, actorIDs ...uint64) testStack {
	t.Helper()
	if current == nil {
		t.Fatal("test clock is required")
	}
	if len(actorIDs) == 0 {
		actorIDs = []uint64{9, 10}
	}
	server := miniredis.RunT(t)
	clock := func() time.Time {
		now := *current
		server.SetTime(now)
		return now
	}
	server.SetTime(*current)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	namespace, err := rediskey.NewNamespace("test", "ingress-stack")
	if err != nil {
		t.Fatal(err)
	}
	profile := rediskey.PhysicalProfile{
		ProfileVersion: "p1", Namespace: namespace, AuthorityLayoutVersion: "v1",
		WorkflowLayoutVersion: "v1", WorkflowHashVersion: 1, WorkflowBucketCount: 64,
	}
	calculator, err := actorrouting.NewCalculator("ingress-stack",
		actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608130001}, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	store, err := redisauthority.NewStore(client, nil, redisauthority.Options{
		Profile: profile, Resolver: calculator,
		LeaseLimits: authoritystore.LeaseLimits{MinTTL: 100 * time.Millisecond, MaxTTL: time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	claim := func(domain actorrouting.RouteDomain, holder string) authoritystore.ShardWriteFence {
		t.Helper()
		claimed, claimErr := store.ClaimShardAuthority(ctx, authoritystore.ClaimShardAuthorityRequest{
			Caller: authoritystore.TrustedShardAuthorityCaller{
				Kind:       authoritystore.ShardAuthorityCallerRouteManager,
				InstanceID: "ingress-stack-rm", BootID: "ingress-stack-rm-boot",
			},
			TransitionID: "claim-" + string(domain) + "-" + holder, AuthorityLayoutVersion: "v1",
			Domain: domain, VirtualShard: 0, NewHolderID: holder, NewHolderBootID: holder + "-boot",
			RequestedTTLms: 3_600_000,
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
	farmFence := claim(actorrouting.DomainFarm, "farm-holder")
	playerFence := claim(actorrouting.DomainPlayer, "player-holder")
	farmProvider, err := authority.NewProvider(authority.ProviderOptions{
		Store: store, Fence: authority.StaticFenceSource{Fence: farmFence},
		AuthorityLayoutVersion: "v1", BootID: farmFence.ExpectedHolderBootID,
	})
	if err != nil {
		t.Fatal(err)
	}
	playerProvider, err := authority.NewProvider(authority.ProviderOptions{
		Store: store, Fence: authority.StaticFenceSource{Fence: playerFence},
		AuthorityLayoutVersion: "v1", BootID: playerFence.ExpectedHolderBootID,
	})
	if err != nil {
		t.Fatal(err)
	}
	farmRegistry := farmactor.NewAuthorityRegistry(farmProvider, farmactor.RegistryOptions{
		MailboxSize: 16, ModelFieldV2Conversion: fieldv2.FullConversionPolicy(),
	})
	playerRegistry := playeractor.NewAuthorityRegistry(playerProvider, playeractor.RegistryOptions{
		MailboxSize: 16, ModelFieldV2Conversion: fieldv2.FullConversionPolicy(),
	})
	t.Cleanup(farmRegistry.Stop)
	t.Cleanup(playerRegistry.Stop)

	playerRoutes := make(map[uint64]*logicrpcpb.ActorRouteHeader, len(actorIDs))
	farmRoutes := make(map[uint64]*logicrpcpb.ActorRouteHeader, len(actorIDs))
	for _, actorID := range actorIDs {
		if _, err := farmRegistry.Submit(ctx, actorID, func(context.Context, *farmactor.FarmActor) (any, error) {
			return nil, nil
		}); err != nil {
			t.Fatal(err)
		}
		farmRoutes[actorID] = &logicrpcpb.ActorRouteHeader{
			RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM, ActorId: actorID, VirtualShard: 0,
			ShardRevision: farmFence.ExpectedAssignmentRevision, ExpectedHolderId: farmFence.ExpectedHolderID,
			ExpectedHolderBootId: farmFence.ExpectedHolderBootID, ExpectedWriterEpoch: farmFence.ExpectedWriterEpoch,
			ExpectedLeaseEpoch: farmFence.ExpectedLeaseEpoch, ExpectedLeaseUntilMs: ingressTestLeaseDeadline(),
		}
		playerRoutes[actorID] = &logicrpcpb.ActorRouteHeader{
			RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER, ActorId: actorID, VirtualShard: 0,
			ShardRevision: playerFence.ExpectedAssignmentRevision, ExpectedHolderId: playerFence.ExpectedHolderID,
			ExpectedHolderBootId: playerFence.ExpectedHolderBootID, ExpectedWriterEpoch: playerFence.ExpectedWriterEpoch,
			ExpectedLeaseEpoch: playerFence.ExpectedLeaseEpoch, ExpectedLeaseUntilMs: ingressTestLeaseDeadline(),
		}
	}

	playerService := playeringress.NewService(playerRegistry)
	for _, playerID := range actorIDs {
		if _, err := playerService.EnsureOwnerInitialized(ctx, &logicrpcpb.EnsureOwnerInitializedRequest{
			PlayerId: playerID, BootstrapId: "bootstrap",
		}); err != nil {
			t.Fatal(err)
		}
	}
	farmService := NewService(farmRegistry, playerService)
	farmService.SetLegacyCareEnabled(true)
	workflows := workflow.NewMemoryStore()
	farmService.SetWorkflowStore(workflows)
	pairs := activeIngressPairReader(t, actorIDs[0], actorIDs[len(actorIDs)-1])
	visitAuthorizer := &testVisitAuthorizer{epoch: 1}
	farmService.SetFriendVisitAuthorizer(visitAuthorizer)
	navigation := &testNavigationVisited{marked: make(chan friend.VisitFence, 4)}
	farmService.SetFriendNavigationVisitedRepository(navigation)
	friendPermits, err := friend.NewPermitService(pairs, []byte("0123456789abcdef0123456789abcdef"), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	farmService.SetFriendPermitAuthority(friendPermits)
	farmService.SetFriendActionAdmission(friendActionAdmissionStub{})
	farmService.now = clock
	farmService.SetMatureAfter(10 * time.Second)

	bucketForFarm := func(farmID uint64) (rediskey.AuthorityBucketRef, error) {
		return profile.AuthorityBucketForActor(calculator, actorrouting.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: farmID})
	}
	bucketForPlayer := func(playerID uint64) (rediskey.AuthorityBucketRef, error) {
		return profile.AuthorityBucketForActor(calculator, actorrouting.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: playerID})
	}
	playerSource, err := NewRedisPlayerSourceVerifier(store, bucketForPlayer)
	if err != nil {
		t.Fatal(err)
	}
	harvestSource, err := playerparticipant.NewRedisHarvestSourceVerifier(store, bucketForFarm)
	if err != nil {
		t.Fatal(err)
	}
	careDependency, err := playerparticipant.NewRedisCareDependencyVerifier(store, bucketForFarm)
	if err != nil {
		t.Fatal(err)
	}
	farmOpts := passingFarmParticipantOptions(farmRegistry, time.Now)
	farmOpts.NowMs = func() int64 { return current.UnixMilli() }
	farmOpts.PlayerSourceVerifier = playerSource
	farmOpts.CommitPlantObserver = farmService.EmitChange
	farmServer, err := NewServerChecked(farmOpts)
	if err != nil {
		t.Fatal(err)
	}
	playerServer := playerparticipant.NewServer(playerparticipant.Options{
		Registry: playerRegistry,
		WorkVersionVerifier: playerparticipant.WorkVersionVerifierFunc(func(context.Context, *contracts.ParticipantCall) error {
			return nil
		}),
		WorkLeaseVerifier: playerparticipant.WorkLeaseVerifierFunc(func(context.Context, *contracts.ParticipantCall) error {
			return nil
		}),
		RouteFenceVerifier:      contracts.ParticipantRouteFenceVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
		WorkAddressVerifier:     contracts.ParticipantWorkAddressVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
		IdentityBindingVerifier: contracts.ParticipantIdentityBindingVerifierFunc(func(context.Context, *contracts.ParticipantCall) error { return nil }),
		HarvestSourceVerifier:   harvestSource,
		CareDependencyVerifier:  careDependency,
		TaskConfigRepository:    playertask.MustNewP0Repository(),
		TaskTimezone:            playertask.DefaultGameTimezone,
		NowMs:                   func() int64 { return current.UnixMilli() },
		Now:                     time.Now,
	})
	routeFor := func(routes map[uint64]*logicrpcpb.ActorRouteHeader, id uint64, kind string) (*logicrpcpb.ActorRouteHeader, error) {
		header, ok := routes[id]
		if !ok {
			t.Fatalf("unexpected %s route id=%d", kind, id)
		}
		return proto.Clone(header).(*logicrpcpb.ActorRouteHeader), nil
	}
	direct, err := carerecovery.NewDirectDispatcher(carerecovery.DirectConfig{
		WorkLease: store.WorkLease(), SourceOrders: store.WorkLease(),
		Route: func(_ context.Context, id uint64) (*logicrpcpb.ActorRouteHeader, error) {
			return routeFor(playerRoutes, id, "player")
		},
		Player: coordinatorStepClient(playerServer.ExecuteStep),
		Farm:   coordinatorStepClient(farmServer.ExecuteStep),
		FarmRoute: func(_ context.Context, id uint64) (*logicrpcpb.ActorRouteHeader, error) {
			return routeFor(farmRoutes, id, "farm")
		},
		Owner: "ingress-stack-direct", Token: "ingress-stack-token", BootID: "ingress-stack-direct-boot",
		BucketForFarm: bucketForFarm, BucketForPlayer: bucketForPlayer,
		LeaseTTL: 10 * time.Second, ItemDeadline: 5 * time.Second, Now: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	farmService.SetPlayerCoordinator(func(ctx context.Context, req *contractspb.ParticipantRequest) (*contractspb.ParticipantResponse, error) {
		return playerServer.ExecuteStep(ctx, req)
	}, func(_ context.Context, id uint64) (*logicrpcpb.ActorRouteHeader, error) {
		return routeFor(playerRoutes, id, "player")
	})
	farmService.SetPlayerBucketResolver(bucketForPlayer)
	farmService.SetPlantOrderDispatcher(func(ctx context.Context, playerID uint64, orderID string) error {
		bucket, bucketErr := bucketForPlayer(playerID)
		if bucketErr != nil {
			return bucketErr
		}
		address, addrErr := sourceorder.WorkAddress(bucket, string(identity.WorkTypePlant), orderID, workidentity.RouteDomainPlayer, playerID)
		if addrErr != nil {
			return addrErr
		}
		return direct.DispatchOnce(ctx, address)
	})
	farmService.SetUnlockOrderDispatcher(func(ctx context.Context, playerID uint64, orderID string) error {
		bucket, bucketErr := bucketForPlayer(playerID)
		if bucketErr != nil {
			return bucketErr
		}
		address, addrErr := sourceorder.WorkAddress(bucket, string(identity.WorkTypeUnlockPlot), orderID, workidentity.RouteDomainPlayer, playerID)
		if addrErr != nil {
			return addrErr
		}
		return direct.DispatchOnce(ctx, address)
	})
	farmService.SetHarvestOrderEnqueuer(func(ctx context.Context, farmID uint64, order authoritystore.AttachedOrder, evidence *authoritystore.ExactCommitEvidence) error {
		bucket, bucketErr := bucketForFarm(farmID)
		if bucketErr != nil {
			return bucketErr
		}
		address, addrErr := sourceorder.WorkAddress(bucket, string(identity.WorkTypeHarvest), order.OrderID, workidentity.RouteDomainFarm, farmID)
		if addrErr != nil {
			return addrErr
		}
		return direct.DispatchCommittedOnceWithEvidence(ctx, address, order, evidence)
	})
	farmService.SetCareOrderEnqueuer(func(ctx context.Context, farmID uint64, order authoritystore.AttachedOrder, evidence *authoritystore.ExactCommitEvidence) error {
		bucket, err := bucketForFarm(farmID)
		if err != nil {
			return err
		}
		address, err := sourceorder.WorkAddress(bucket, string(identity.WorkTypeCareReward), order.OrderID, workidentity.RouteDomainFarm, farmID)
		if err != nil {
			return err
		}
		return direct.DispatchCommittedOnceWithEvidence(ctx, address, order, evidence)
	})
	farmService.SetPlantParticipantObserverConfigured()
	return testStack{
		farm: farmService, player: playerService, visit: visitAuthorizer,
		navigation: navigation, workflows: workflows, now: current,
	}
}
