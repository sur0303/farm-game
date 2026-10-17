package actor_test

// 经真实 ingress ExecuteCommand 接缝的 Buy/Sell 集成测试:PlayerActor 的
// AuthorityStore 主线(AR1)对既有业务行为完全透明,且崩溃冷加载后以原
// operation_id + 原语义 payload 重放仍命中持久 Receipt(canonical hash)。

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	commonpb "farmgame/api/gen/common"
	logicrpcpb "farmgame/api/gen/logicrpc"
	playerpb "farmgame/api/gen/player"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/actorrouting"
	"farmgame/internal/logic/actor/authority"
	playeractor "farmgame/internal/player/actor"
	playeringress "farmgame/internal/player/ingress"
	playertask "farmgame/internal/player/task"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/blobstore"
	redisauthority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
)

var ar1IngressNow = time.UnixMilli(1_770_400_000_000)

func ar1IngressProvider(t *testing.T, backend string, playerID uint64) (*authority.Provider, authoritystore.ActorAuthorityStore) {
	t.Helper()
	calculator, err := actorrouting.NewCalculator(
		"test-realm",
		actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608040031},
		64, 64,
	)
	if err != nil {
		t.Fatal(err)
	}
	var store authoritystore.ActorAuthorityStore
	var fence authoritystore.ShardWriteFence
	if backend == "redis" {
		server := miniredis.RunT(t)
		server.SetTime(ar1IngressNow)
		client := redis.NewClient(&redis.Options{Addr: server.Addr()})
		t.Cleanup(func() { _ = client.Close() })
		namespace, err := rediskey.NewNamespace("test", "z1")
		if err != nil {
			t.Fatal(err)
		}
		profile := rediskey.PhysicalProfile{
			ProfileVersion: "p1", Namespace: namespace,
			AuthorityLayoutVersion: "v1", WorkflowLayoutVersion: "v1",
			WorkflowHashVersion: 1, WorkflowBucketCount: 64,
		}
		if err := profile.Validate(); err != nil {
			t.Fatal(err)
		}
		redisStore, err := redisauthority.NewStore(client, nil, redisauthority.Options{Profile: profile, Resolver: calculator})
		if err != nil {
			t.Fatal(err)
		}
		shard, err := calculator.ShardFor(actorrouting.DomainPlayer, playerID)
		if err != nil {
			t.Fatal(err)
		}
		claim, err := redisStore.ClaimShardAuthority(context.Background(), authoritystore.ClaimShardAuthorityRequest{
			Caller: authoritystore.TrustedShardAuthorityCaller{
				Kind: authoritystore.ShardAuthorityCallerRouteManager, InstanceID: "route-manager-1", BootID: "rm-boot-1",
			},
			TransitionID: "ar1-ingress-1", AuthorityLayoutVersion: "v1",
			Domain: actorrouting.DomainPlayer, VirtualShard: shard,
			NewHolderID: "player-holder-a", NewHolderBootID: "player-holder-a-boot", RequestedTTLms: 60_000,
		})
		if err != nil {
			t.Fatal(err)
		}
		if claim.Status != authoritystore.ShardTransitionApplied {
			t.Fatalf("claim status=%s", claim.Status)
		}
		fence = authoritystore.ShardWriteFence{
			ExpectedHolderID:           claim.Authority.HolderID,
			ExpectedHolderBootID:       claim.Authority.HolderBootID,
			ExpectedAssignmentRevision: claim.Authority.AssignmentRevision,
			ExpectedWriterEpoch:        claim.Authority.WriterEpoch,
			ExpectedLeaseEpoch:         claim.Authority.LeaseEpoch,
		}
		store = redisStore
	} else {
		memoryStore, err := authoritystore.NewMemoryStore(authoritystore.MemoryOptions{
			Resolver: calculator, AuthorityLayoutVersion: "v1",
			Now: func() time.Time { return ar1IngressNow },
		})
		if err != nil {
			t.Fatal(err)
		}
		shard, err := calculator.ShardFor(actorrouting.DomainPlayer, playerID)
		if err != nil {
			t.Fatal(err)
		}
		authority := authoritystore.ShardWriteAuthority{
			AuthorityLayoutVersion: "v1", Domain: actorrouting.DomainPlayer, VirtualShard: shard,
			HolderID: "player-holder-a", HolderBootID: "player-holder-a-boot",
			AssignmentRevision: 1, WriterEpoch: 1, LeaseEpoch: 1,
			State: authoritystore.ShardAuthorityActive, LeaseUntilMs: ar1IngressNow.Add(time.Minute).UnixMilli(),
		}
		if err := memoryStore.SetShardWriteAuthority(authority); err != nil {
			t.Fatal(err)
		}
		fence = authoritystore.ShardWriteFence{
			ExpectedHolderID:           authority.HolderID,
			ExpectedHolderBootID:       authority.HolderBootID,
			ExpectedAssignmentRevision: authority.AssignmentRevision,
			ExpectedWriterEpoch:        authority.WriterEpoch,
			ExpectedLeaseEpoch:         authority.LeaseEpoch,
		}
		store = memoryStore
	}
	provider, err := authority.NewProvider(authority.ProviderOptions{
		Store:                  store,
		Fence:                  authority.StaticFenceSource{Fence: fence},
		AuthorityLayoutVersion: "v1",
		BootID:                 "player-holder-a-boot",
	})
	if err != nil {
		t.Fatal(err)
	}
	return provider, store
}

func TestAuthorityIngressBuySellAndReplayAfterRestart(t *testing.T) {
	for _, backend := range []string{"memory", "redis"} {
		t.Run(backend, func(t *testing.T) {
			provider, store := ar1IngressProvider(t, backend, 9)
			_ = store
			registry := playeractor.NewAuthorityRegistry(provider, playeractor.RegistryOptions{MailboxSize: 8})
			t.Cleanup(registry.Stop)
			service := playeringress.NewService(registry)
			ctx := context.Background()

			if _, err := service.EnsureOwnerInitialized(ctx, &logicrpcpb.EnsureOwnerInitializedRequest{
				PlayerId: 9, BootstrapId: "bootstrap-9",
			}); err != nil {
				t.Fatal(err)
			}
			execute := func(svc *playeringress.Service, command *commonpb.ClientCommandEnvelope) *logicrpcpb.BusinessCommandOutcome {
				t.Helper()
				response, err := svc.ExecuteCommand(ctx, &logicrpcpb.ExecuteCommandRequest{
					Command: command,
					Trusted: &logicrpcpb.TrustedCommandContext{
						Session:                     &logicrpcpb.SessionPermitSnapshot{PlayerId: 9, ConnectionId: "c"},
						RecoveryCompleteAtAdmission: true,
					},
					Dispatch: &logicrpcpb.DispatchContext{
						RouteMode:  logicrpcpb.RouteMode_ROUTE_MODE_ACTOR_AFFINITY,
						ActorRoute: &logicrpcpb.ActorRouteHeader{RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER, ActorId: 9},
					},
				})
				if err != nil {
					t.Fatal(err)
				}
				return response.GetBusiness()
			}

			buyPayload, _ := proto.Marshal(&publicpb.BuyRequest{OfferId: playeractor.OfferCarrotSeed, Count: 2, ExpectedOfferVersion: 1})
			buy := &commonpb.ClientCommandEnvelope{
				OperationId: "buy-1", RequestId: "r-buy", CommandType: commonpb.CommandType_BUY,
				Payload: buyPayload,
			}
			first := execute(service, buy)
			second := execute(service, buy)
			if first.Status != commonpb.CommandStatus_SUCCEEDED || second.Status != commonpb.CommandStatus_SUCCEEDED {
				t.Fatalf("buy statuses: %s %s", first.Status, second.Status)
			}
			var bought playerpb.EconomyCommandResult
			if err := proto.Unmarshal(second.ResultPayload, &bought); err != nil {
				t.Fatal(err)
			}
			if bought.PrivateState.Coin != 80 {
				t.Fatalf("duplicate buy changed coin: %d", bought.PrivateState.Coin)
			}
			conflictPayload, _ := proto.Marshal(&publicpb.BuyRequest{OfferId: playeractor.OfferCarrotSeed, Count: 3, ExpectedOfferVersion: 1})
			conflict := execute(service, &commonpb.ClientCommandEnvelope{
				OperationId: "buy-1", RequestId: "r-buy-conflict", CommandType: commonpb.CommandType_BUY,
				Payload: conflictPayload,
			})
			if conflict.Status != commonpb.CommandStatus_REJECTED || conflict.Error == nil ||
				conflict.Error.ErrorCode != commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT {
				t.Fatalf("conflicting buy=%v", conflict)
			}

			// 发放 3 个胡萝卜并卖出 1 个(Sell 接缝)。
			if _, err := service.GrantItem(ctx, &logicrpcpb.GrantItemRequest{
				PlayerId: 9, OperationId: "grant-1", ItemId: playeractor.ItemCarrot, Count: 3, PayloadHash: []byte("grant-1"),
			}); err != nil {
				t.Fatal(err)
			}
			sellPayload, _ := proto.Marshal(&publicpb.SellRequest{ItemId: playeractor.ItemCarrot, Count: 1})
			sold := execute(service, &commonpb.ClientCommandEnvelope{
				OperationId: "sell-1", RequestId: "r-sell", CommandType: commonpb.CommandType_SELL,
				Payload: sellPayload,
			})
			if sold.Status != commonpb.CommandStatus_SUCCEEDED {
				t.Fatalf("sell=%v", sold)
			}

			// 模拟进程崩溃:同一 Store 上重建 Registry + Service;以原
			// operation_id + 原语义 payload 重放必须命中崩溃前的持久
			// Receipt(权威 canonical hash 一致),不重复扣款。
			restarted := playeractor.NewAuthorityRegistry(provider, playeractor.RegistryOptions{MailboxSize: 8})
			t.Cleanup(restarted.Stop)
			service2 := playeringress.NewService(restarted)
			polled := execute(service2, buy)
			if polled.Status != commonpb.CommandStatus_SUCCEEDED {
				t.Fatalf("replay after restart=%v", polled)
			}
			var replayed playerpb.EconomyCommandResult
			if err := proto.Unmarshal(polled.ResultPayload, &replayed); err != nil {
				t.Fatal(err)
			}
			if replayed.PrivateState.Coin != 80 {
				t.Fatalf("replayed outcome coin=%d", replayed.PrivateState.Coin)
			}
		})
	}
}

// TestAuthorityAndLegacyBootstrapPathVersionConsistent 钉死 28 §3.7 的跨 Registry
// 路径一致性:同一任务序列(EnsureOwnerInitialized → 只读 Bootstrap → 同日安全重查)
// 在 legacy blobstore 与 AuthorityStore 两条 PlayerActor 主线下必须产生完全一致的
// player_state_revision,且同日重查都保持稳定。
func TestAuthorityAndLegacyBootstrapPathVersionConsistent(t *testing.T) {
	ctx := context.Background()
	runPath := func(t *testing.T, registry *playeractor.Registry) []uint64 {
		t.Helper()
		service := playeringress.NewService(registry)
		if err := service.SetBootstrapTaskDomain(playertask.MustNewP0Repository(), playertask.DefaultGameTimezone,
			func() int64 { return ar1IngressNow.UnixMilli() }); err != nil {
			t.Fatal(err)
		}
		if _, err := service.EnsureOwnerInitialized(ctx, &logicrpcpb.EnsureOwnerInitializedRequest{
			PlayerId: 9, BootstrapId: "bootstrap-9",
		}); err != nil {
			t.Fatal(err)
		}
		bootstrap := func(operationID string) *logicrpcpb.BusinessCommandOutcome {
			t.Helper()
			payload, _ := proto.Marshal(&publicpb.GetPlayerBootstrapRequest{})
			response, err := service.ExecuteCommand(ctx, &logicrpcpb.ExecuteCommandRequest{
				Command: &commonpb.ClientCommandEnvelope{
					OperationId: operationID, RequestId: "r-" + operationID,
					CommandType: commonpb.CommandType_GET_PLAYER_BOOTSTRAP,
					Payload:    payload,
				},
				Trusted: &logicrpcpb.TrustedCommandContext{
					Session:                     &logicrpcpb.SessionPermitSnapshot{PlayerId: 9, ConnectionId: "c"},
					RecoveryCompleteAtAdmission: true,
				},
				Dispatch: &logicrpcpb.DispatchContext{
					RouteMode:  logicrpcpb.RouteMode_ROUTE_MODE_ACTOR_AFFINITY,
					ActorRoute: &logicrpcpb.ActorRouteHeader{RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER, ActorId: 9},
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			return response.GetBusiness()
		}
		revisionOf := func(outcome *logicrpcpb.BusinessCommandOutcome) uint64 {
			t.Helper()
			if outcome.Status != commonpb.CommandStatus_SUCCEEDED {
				t.Fatalf("bootstrap outcome=%v", outcome)
			}
			var result publicpb.GetPlayerBootstrapResult
			if err := proto.Unmarshal(outcome.ResultPayload, &result); err != nil {
				t.Fatal(err)
			}
			return result.PlayerStateRevision
		}
		first := revisionOf(bootstrap("bootstrap-first"))
		replay := revisionOf(bootstrap("bootstrap-replay"))
		return []uint64{first, replay}
	}

	legacyRegistry := playeractor.NewRegistry(blobstore.NewMemoryStore(), 8)
	t.Cleanup(legacyRegistry.Stop)
	legacyRevisions := runPath(t, legacyRegistry)

	provider, _ := ar1IngressProvider(t, "memory", 9)
	authorityRegistry := playeractor.NewAuthorityRegistry(provider, playeractor.RegistryOptions{MailboxSize: 8})
	t.Cleanup(authorityRegistry.Stop)
	authorityRevisions := runPath(t, authorityRegistry)

	if legacyRevisions[0] != legacyRevisions[1] || authorityRevisions[0] != authorityRevisions[1] {
		t.Errorf("same-day replay must be stable in both paths: legacy=%v authority=%v", legacyRevisions, authorityRevisions)
	}
	if authorityRevisions[0] != legacyRevisions[0] {
		t.Errorf("read-only bootstrap revisions must match across stores: legacy=%v authority=%v",
			legacyRevisions, authorityRevisions)
	}
}
