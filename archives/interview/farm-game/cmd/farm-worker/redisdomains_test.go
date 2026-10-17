package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/friend"
	"farmgame/internal/gateway/bootlease"
	"farmgame/internal/gateway/session"
	"farmgame/internal/redisdomains"
	"farmgame/internal/storage/authoritystore"
	redisauthority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workidentity"

	"github.com/alicebob/miniredis/v2"
)

const domainTestHashSeed uint64 = 202608120001

var domainTestNow = time.UnixMilli(1_770_000_000_000)

func domainRedisURL(server *miniredis.Miniredis) string {
	return "redis://" + server.Addr() + "/0"
}

func domainNamespace(t *testing.T) rediskey.Namespace {
	t.Helper()
	namespace, err := rediskey.NewNamespace("test", "z1")
	if err != nil {
		t.Fatal(err)
	}
	return namespace
}

func domainCalculator(t *testing.T) *actorrouting.Calculator {
	t.Helper()
	calculator, err := actorrouting.NewCalculator("test-realm",
		actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: domainTestHashSeed}, 64, 64)
	if err != nil {
		t.Fatal(err)
	}
	return calculator
}

func domainProfile(t *testing.T) rediskey.PhysicalProfile {
	t.Helper()
	return rediskey.PhysicalProfile{
		ProfileVersion:         "p1",
		Namespace:              domainNamespace(t),
		AuthorityLayoutVersion: "v1",
		WorkflowLayoutVersion:  "v1",
		WorkflowHashVersion:    1,
		WorkflowBucketCount:    64,
	}
}

// openDomainClients opens three explicitly isolated miniredis servers and
// returns the domain clients plus the servers for cross-domain assertions.
func openDomainClients(t *testing.T) (*redisdomains.Clients, *miniredis.Miniredis, *miniredis.Miniredis, *miniredis.Miniredis) {
	t.Helper()
	authorityServer := miniredis.RunT(t)
	authorityServer.SetTime(domainTestNow)
	runtimeServer := miniredis.RunT(t)
	runtimeServer.SetTime(domainTestNow)
	projectionServer := miniredis.RunT(t)
	clients, err := redisdomains.OpenFromEnv(context.Background(), func(key string) string {
		switch key {
		case redisdomains.AuthorityURLEnv:
			return domainRedisURL(authorityServer)
		case redisdomains.RuntimeURLEnv:
			return domainRedisURL(runtimeServer)
		case redisdomains.ProjectionURLEnv:
			return domainRedisURL(projectionServer)
		default:
			return ""
		}
	}, redisdomains.SelectAll)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = clients.Close() })
	return clients, authorityServer, runtimeServer, projectionServer
}

// domainSchedule builds a contract-valid attached FRIEND_OPPORTUNITY schedule.
func domainSchedule(t *testing.T, operationID string, dueAtMs int64) authoritystore.AttachedSchedule {
	t.Helper()
	scheduleID, err := workidentity.StableOpaqueID("schedule", "FRIEND_OPPORTUNITY", operationID)
	if err != nil {
		t.Fatal(err)
	}
	return authoritystore.AttachedSchedule{
		ScheduleID:   scheduleID,
		ScheduleType: "FRIEND_OPPORTUNITY",
		Generation:   1,
		DueAtMs:      dueAtMs,
		Payload:      []byte("wake:" + operationID),
	}
}

func TestRedisDomainIsolationAuthorityScheduleState(t *testing.T) {
	clients, authorityServer, runtimeServer, projectionServer := openDomainClients(t)
	ctx := context.Background()

	calculator := domainCalculator(t)
	profile := domainProfile(t)
	store, err := redisauthority.NewStore(clients.Authority, nil, redisauthority.Options{
		Profile: profile, Resolver: calculator,
		ScheduleDueIndexMode:           redisauthority.ScheduleDueIndexDualShadow,
		ScheduleDueIndexDedicatedReady: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	address := authoritystore.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: 424242}
	bucket, err := profile.AuthorityBucketForActor(calculator, address)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimShardAuthority(ctx, authoritystore.ClaimShardAuthorityRequest{
		Caller: authoritystore.TrustedShardAuthorityCaller{
			Kind: authoritystore.ShardAuthorityCallerRouteManager, InstanceID: "route-manager-1", BootID: "rm-boot-1",
		},
		TransitionID:           "transition-1",
		AuthorityLayoutVersion: "v1",
		Domain:                 address.Domain,
		VirtualShard:           bucket.VirtualShard(),
		NewHolderID:            "farm-pod-1",
		NewHolderBootID:        "farm-boot-1",
		RequestedTTLms:         60_000,
	})
	if err != nil || claim.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("shard claim=%+v err=%v", claim, err)
	}
	fence := authoritystore.ShardWriteFence{
		ExpectedHolderID: claim.Authority.HolderID, ExpectedHolderBootID: claim.Authority.HolderBootID,
		ExpectedAssignmentRevision: claim.Authority.AssignmentRevision,
		ExpectedWriterEpoch:        claim.Authority.WriterEpoch, ExpectedLeaseEpoch: claim.Authority.LeaseEpoch,
	}
	loaded, err := store.LoadAndClaim(ctx, authoritystore.LoadAndClaimRequest{
		ActorAddress: address, AuthorityLayoutVersion: "v1", ShardWriteFence: fence,
		ClaimOperationID: "domain-schedule-claim", NewWriterClaimID: "domain-schedule-writer",
		InitialModelFields: authoritystore.ModelState{"initial": []byte("state")},
	})
	if err != nil || loaded.Status != authoritystore.LoadAndClaimClaimed {
		t.Fatalf("load=%+v err=%v", loaded, err)
	}
	root := authoritystore.OperationRootRef{Kind: authoritystore.RootKindPlayerActor, CanonicalID: "10001"}
	key := authoritystore.IdempotencyKey{SubjectPlayerID: 10001, StableCommandTypeName: "FRIEND_OPPORTUNITY_WAKE",
		OperationID: "domain-schedule-commit", Root: root}
	request := authoritystore.AuthorityCommitRequest{
		CommitRequestID: "domain-schedule-commit", ActorAddress: address, AuthorityLayoutVersion: "v1",
		ShardWriteFence: fence, ExpectedWriterClaimID: "domain-schedule-writer",
		ExpectedRecordVersion: loaded.Snapshot.RecordVersion, IdempotencyKey: key,
		CanonicalPayloadHash: authoritystore.PayloadHash{1, 2, 3},
		CandidateState:       authoritystore.ModelState{"state": []byte("v1")},
		Receipt: authoritystore.AuthorityReceipt{ReceiptID: "receipt-domain-schedule", IdempotencyKey: key,
			CanonicalPayloadHash: authoritystore.PayloadHash{1, 2, 3}, Outcome: authoritystore.StoredOperationOutcome{
				Status: authoritystore.TerminalSucceeded, Payload: []byte("ok")}},
		OperationResult: authoritystore.StoredOperationOutcome{Status: authoritystore.TerminalSucceeded, Payload: []byte("ok")},
	}
	schedule := domainSchedule(t, "domain-schedule", domainTestNow.Add(-time.Second).UnixMilli())
	request.AttachedSchedules = []authoritystore.AttachedSchedule{schedule}
	if result, err := store.Commit(ctx, request); err != nil || result.Status != authoritystore.CommitCommitted {
		t.Fatalf("commit=%+v err=%v", result, err)
	}

	keys, err := rediskey.NewAuthorityKeys(profile, calculator)
	if err != nil {
		t.Fatal(err)
	}
	dedicated, err := keys.SourceScheduleDueIndex(bucket)
	if err != nil {
		t.Fatal(err)
	}
	scheduleRef, err := workidentity.EncodeScheduleRefV1(schedule.ScheduleType, schedule.ScheduleID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := clients.Authority.ZScore(ctx, dedicated.String(), scheduleRef).Result(); err != nil {
		t.Fatalf("schedule member missing from Authority due index: %v", err)
	}
	if !authorityServer.Exists(dedicated.String()) {
		t.Fatal("Authority server does not own the schedule due index")
	}
	if runtimeServer.Exists(dedicated.String()) {
		t.Fatal("schedule due index leaked to Runtime server")
	}
	if projectionServer.Exists(dedicated.String()) {
		t.Fatal("schedule due index leaked to Projection server")
	}

	lease := store.WorkLease()
	due, err := lease.ListDueScheduledWork(ctx, bucket, domainTestNow.UnixMilli()+1, 8)
	if err != nil || len(due) != 1 {
		t.Fatalf("due=%+v err=%v", due, err)
	}
	claimWork, err := lease.ClaimScheduledWorkWithBoot(ctx, bucket, schedule.ScheduleType, schedule.ScheduleID,
		"worker", "boot", "token", 30_000)
	if err != nil || claimWork.Status != redisauthority.WorkClaimClaimed {
		t.Fatalf("claim=%+v err=%v", claimWork, err)
	}
	if len(runtimeServer.Keys()) != 0 {
		t.Fatalf("Runtime server holds Authority schedule state: %v", runtimeServer.Keys())
	}
	if len(projectionServer.Keys()) != 0 {
		t.Fatalf("Projection server holds Authority schedule state: %v", projectionServer.Keys())
	}
}

func TestRedisDomainIsolationRuntimeRouteAndBootLease(t *testing.T) {
	clients, authorityServer, runtimeServer, projectionServer := openDomainClients(t)
	ctx := context.Background()

	authority := session.NewRedis(clients.Runtime, session.Config{Secret: []byte("test-secret"), AuthorityEpoch: 1})
	candidate, err := authority.Reserve(ctx, 20001, "session-1")
	if err != nil {
		t.Fatal(err)
	}
	activated, err := authority.Activate(ctx, session.ActivationRequest{
		PlayerID: 20001, SessionID: "session-1", AuthorityEpoch: candidate.AuthorityEpoch,
		ReservedSessionEpoch: candidate.ReservedSessionEpoch,
		Route: session.RouteFence{GatewayID: "gw-1", GatewayBootID: "gw-boot-1",
			ConnectionID: "conn-1", ConnectionGeneration: 7},
		LeaseTTL: time.Minute,
	})
	if err != nil || activated.Route.PlayerID != 20001 {
		t.Fatalf("activate=%+v err=%v", activated, err)
	}
	sessionKey := rediskey.Session(20001)
	if !runtimeServer.Exists(sessionKey) {
		t.Fatal("SessionRoute key missing from Runtime server")
	}
	if authorityServer.Exists(sessionKey) {
		t.Fatal("SessionRoute leaked to Authority server")
	}
	if projectionServer.Exists(sessionKey) {
		t.Fatal("SessionRoute leaked to Projection server")
	}

	leaseStore, err := bootlease.NewRedisStore(clients.Runtime, "farm:v03:test:gate-boot-lease")
	if err != nil {
		t.Fatal(err)
	}
	record, err := leaseStore.Renew(ctx, bootlease.Fence{GatewayID: "gateway-a", BootID: "boot-1"}, time.Minute)
	if err != nil || !record.Fence.Valid() {
		t.Fatalf("renew=%+v err=%v", record, err)
	}
	foundBoot := false
	for _, key := range runtimeServer.Keys() {
		if strings.HasPrefix(key, "farm:v03:test:gate-boot-lease:") {
			foundBoot = true
		}
	}
	if !foundBoot {
		t.Fatalf("GateBoot lease key missing from Runtime server: %v", runtimeServer.Keys())
	}
	for _, other := range []*miniredis.Miniredis{authorityServer, projectionServer} {
		if len(other.Keys()) != 0 {
			t.Fatalf("GateBoot lease leaked to wrong domain: %v", other.Keys())
		}
	}
}

func TestRedisDomainIsolationProjectionHintSidecar(t *testing.T) {
	clients, authorityServer, runtimeServer, projectionServer := openDomainClients(t)
	ctx := context.Background()
	namespace := domainNamespace(t)

	hintBus, err := friend.NewRedisOpportunityHintBus(clients.Projection, namespace)
	if err != nil {
		t.Fatal(err)
	}
	subscriber := projectionServer.NewSubscriber()
	subscriber.Subscribe("classic-farm:test:z1:friend-opportunity-hint-v1")
	defer subscriber.Close()
	// miniredis subscriber channels are unbuffered: the reader must be
	// draining before the publish executes.
	received := make(chan miniredis.PubsubMessage, 1)
	go func() {
		select {
		case message := <-subscriber.Messages():
			received <- message
		case <-time.After(2 * time.Second):
		}
	}()
	if err := hintBus.PublishFriendOpportunityHint(ctx, 30001, 5, 2); err != nil {
		t.Fatal(err)
	}
	select {
	case message := <-received:
		if message.Channel != "classic-farm:test:z1:friend-opportunity-hint-v1" || message.Message == "" {
			t.Fatalf("unexpected hint message=%+v", message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Hint message did not arrive on Projection pubsub")
	}

	index, err := friend.NewRedisFriendIndexRepository(clients.Projection, friend.DefaultFriendRedisProfile(namespace))
	if err != nil {
		t.Fatal(err)
	}
	applied, err := index.Apply(ctx, 30001, friend.FriendIndexEntry{
		FriendPlayerID: 40001, RelationID: "relation-1", RelationEpoch: 1,
		ActivatedAtMs: domainTestNow.UnixMilli(),
	})
	if err != nil || !applied {
		t.Fatalf("index apply=%v err=%v", applied, err)
	}
	builder, err := friend.NewFriendshipKeyBuilder(friend.DefaultFriendRedisProfile(namespace))
	if err != nil {
		t.Fatal(err)
	}
	relationKey, _, _, _, _, _, err := builder.FriendIndexKeys(30001)
	if err != nil {
		t.Fatal(err)
	}
	if !projectionServer.Exists(relationKey.String()) {
		t.Fatal("FriendIndex relation key missing from Projection server")
	}
	if authorityServer.Exists(relationKey.String()) {
		t.Fatal("FriendIndex leaked to Authority server")
	}
	if runtimeServer.Exists(relationKey.String()) {
		t.Fatal("FriendIndex leaked to Runtime server")
	}
}
