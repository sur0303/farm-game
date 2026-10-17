package main

import (
	"context"
	"fmt"
	"time"

	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actorrouting"
	"farmgame/internal/gateway/session"
	"farmgame/internal/storage/authoritystore"

	"google.golang.org/protobuf/proto"
)

func newActiveRoute(ctx context.Context, e *benchmarkEnvironment) (session.ActiveRoute, error) {
	playerID := uint64(130_000_000) + nextID()
	candidate, err := e.sessions.Reserve(ctx, playerID, fmt.Sprintf("session-%d", nextID()))
	if err != nil {
		return session.ActiveRoute{}, err
	}
	result, err := e.sessions.Activate(ctx, session.ActivationRequest{PlayerID: playerID,
		SessionID: candidate.SessionID, AuthorityEpoch: candidate.AuthorityEpoch,
		ReservedSessionEpoch: candidate.ReservedSessionEpoch,
		Route: session.RouteFence{GatewayID: "cap-gate", GatewayBootID: "cap-gate-boot",
			ConnectionID: fmt.Sprintf("connection-%d", nextID()), ConnectionGeneration: 1},
		LeaseTTL: 2 * time.Hour})
	if err != nil {
		return session.ActiveRoute{}, err
	}
	return result.Route, nil
}

func prepareSessionRenew(batchSize int, variant string) func(context.Context, *benchmarkEnvironment, string, int, int) (*preparedScenario, error) {
	return func(ctx context.Context, e *benchmarkEnvironment, distribution string, concurrency, total int) (*preparedScenario, error) {
		resourceCount := concurrency
		if distribution == "hot" {
			resourceCount = 1
		}
		resources := make([][]session.ActiveRoute, resourceCount)
		for i := range resources {
			routes := make([]session.ActiveRoute, batchSize)
			for j := range routes {
				route, err := newActiveRoute(ctx, e)
				if err != nil {
					return nil, err
				}
				routes[j] = route
			}
			if variant == "stale" {
				routes[0].GatewayBootID = "stale-boot"
			}
			if variant == "partial" {
				routes[1].ConnectionGeneration++
				if cleared, err := e.sessions.CompareAndClearRoute(ctx, routes[3]); err != nil || !cleared {
					return nil, fmt.Errorf("clear route setup cleared=%v err=%v", cleared, err)
				}
			}
			resources[i] = routes
		}
		operations := splitOperations(total, concurrency, func(worker, ordinal int) measuredOperation {
			routes := resources[worker%len(resources)]
			return func(ctx context.Context, _ int) (sampleResult, error) {
				start := time.Now()
				logicalRequest := len(routes) * 96
				serialization := time.Since(start)
				results, err := e.sessions.RenewRoutes(ctx, routes, 30*time.Minute)
				if err != nil {
					return sampleResult{}, err
				}
				renewed, stale := 0, 0
				for _, result := range results {
					if result.Renewed {
						renewed++
					} else {
						stale++
					}
					if result.Transient {
						return sampleResult{}, fmt.Errorf("unexpected transient route result")
					}
				}
				wantRenewed := batchSize
				if variant == "stale" {
					wantRenewed = 0
				}
				if variant == "partial" {
					wantRenewed = batchSize - 2
				}
				if renewed != wantRenewed {
					return sampleResult{}, fmt.Errorf("route renewed=%d stale=%d want=%d", renewed, stale, wantRenewed)
				}
				return sampleResult{Outcome: fmt.Sprintf("RENEWED_%d_STALE_%d", renewed, stale),
					LogicalRequestBytes: logicalRequest, LogicalResponseBytes: len(results) * 48,
					PayloadBytes: len(routes), SerializationTime: serialization}, nil
			}
		})
		label := "uniform_session_route_keys"
		if distribution == "hot" {
			label = fmt.Sprintf("hot_%d_session_route_keys", batchSize)
		}
		return &preparedScenario{operations: operations, keyDistribution: label}, nil
	}
}

func prepareSubscriptionProtocol(batchSize int) func(context.Context, *benchmarkEnvironment, string, int, int) (*preparedScenario, error) {
	return func(ctx context.Context, e *benchmarkEnvironment, distribution string, concurrency, total int) (*preparedScenario, error) {
		operations := make([]measuredOperation, total)
		for index := range operations {
			request := &logicrpcpb.BatchRenewSubscriptionsRequest{SourceGatewayId: "cap-gate", SourceGatewayBootId: "cap-boot"}
			for item := 0; item < batchSize; item++ {
				request.Entries = append(request.Entries, &logicrpcpb.SubscriptionLeaseEntry{
					TargetFarmId: uint64(140_000_000 + index*batchSize + item), SubscriptionId: fmt.Sprintf("subscription-%d-%d", index, item),
					JoinAttemptId: fmt.Sprintf("join-%d-%d", index, item), RequestedTtlMs: 30_000,
					Target: &logicrpcpb.DownlinkTargetFence{TargetPlayerId: uint64(150_000_000 + index),
						GatewayId: "cap-gate", GatewayBootId: "cap-boot", ConnectionId: fmt.Sprintf("conn-%d", index),
						ConnectionGeneration: 1, SessionEpoch: 1, SessionRouteRevision: 1},
				})
			}
			operations[index] = func(context.Context, int) (sampleResult, error) {
				start := time.Now()
				data, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
				serialization := time.Since(start)
				if err != nil {
					return sampleResult{}, err
				}
				start = time.Now()
				var decoded logicrpcpb.BatchRenewSubscriptionsRequest
				err = proto.Unmarshal(data, &decoded)
				deserialization := time.Since(start)
				if err != nil || len(decoded.Entries) != batchSize {
					return sampleResult{}, fmt.Errorf("subscription decode entries=%d err=%v", len(decoded.Entries), err)
				}
				return sampleResult{Outcome: "SERIALIZED", LogicalRequestBytes: len(data), LogicalResponseBytes: 0,
					PayloadBytes: batchSize, SerializationTime: serialization, DeserializationTime: deserialization}, nil
			}
		}
		return &preparedScenario{operations: operations, keyDistribution: "no_redis_key_protocol_serialization",
			notes: []string{"Subscription renewal is FarmActor-resident in R1; this L1 cell measures exact protobuf encoding only and correctly reports zero Redis commands. Actor turn capacity belongs to CAP-SINGLE-POD-L2-R1."}}, nil
	}
}

func prepareShardRenew(ctx context.Context, e *benchmarkEnvironment, distribution string, concurrency, total int) (*preparedScenario, error) {
	resourceCount := concurrency
	if distribution == "hot" {
		resourceCount = 1
	}
	type resource struct {
		domain actorrouting.RouteDomain
		shard  uint32
		fence  authoritystore.ShardWriteFence
	}
	resources := make([]resource, resourceCount)
	for i := range resources {
		domain := actorrouting.DomainPlayer
		if i%2 == 1 {
			domain = actorrouting.DomainFarm
		}
		actorID, err := actorIDFor(e, domain, "uniform", 0)
		if err != nil {
			return nil, err
		}
		address := authoritystore.ActorAddress{Domain: domain, ActorID: actorID}
		fence, bucket, err := e.fenceFor(ctx, address)
		if err != nil {
			return nil, err
		}
		resources[i] = resource{domain: domain, shard: bucket.VirtualShard(), fence: fence}
	}
	operations := splitOperations(total, concurrency, func(worker, ordinal int) measuredOperation {
		item := resources[worker%len(resources)]
		return func(ctx context.Context, _ int) (sampleResult, error) {
			start := time.Now()
			logical := 128
			serialization := time.Since(start)
			result, err := e.authority.RenewShardAuthority(ctx, authoritystore.RenewShardAuthorityRequest{
				Caller: authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerHolder,
					InstanceID: item.fence.ExpectedHolderID, BootID: item.fence.ExpectedHolderBootID},
				AuthorityLayoutVersion: "v1", Domain: item.domain, VirtualShard: item.shard,
				ExpectedFence: item.fence, RequestedTTLms: int64((4 * time.Hour).Milliseconds())})
			if err != nil {
				return sampleResult{}, err
			}
			if result.Status != authoritystore.ShardTransitionApplied {
				return sampleResult{}, fmt.Errorf("shard renew=%s", result.Status)
			}
			return sampleResult{Outcome: result.Status.String(), LogicalRequestBytes: logical,
				LogicalResponseBytes: 96, PayloadBytes: 1, SerializationTime: serialization}, nil
		}
	})
	label := "uniform_shard_lease_keys"
	if distribution == "hot" {
		label = "hot_shard_lease_key"
	}
	return &preparedScenario{operations: operations, keyDistribution: label}, nil
}

func runtimeScenarios() []scenarioDefinition {
	return []scenarioDefinition{
		{name: "session_route_renew_r1", group: "runtime_lease", prepare: prepareSessionRenew(1, "valid")},
		{name: "session_route_renew_r4", group: "runtime_lease", prepare: prepareSessionRenew(4, "valid")},
		{name: "session_route_renew_stale", group: "runtime_lease", prepare: prepareSessionRenew(1, "stale")},
		{name: "session_route_renew_partial_r4", group: "runtime_lease", prepare: prepareSessionRenew(4, "partial")},
		{name: "subscription_renew_r1_serialization", group: "runtime_lease", prepare: prepareSubscriptionProtocol(1)},
		{name: "subscription_renew_r4_serialization", group: "runtime_lease", prepare: prepareSubscriptionProtocol(4)},
		{name: "shard_authority_renew", group: "runtime_lease", prepare: prepareShardRenew},
	}
}
