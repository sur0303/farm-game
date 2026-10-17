package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strconv"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workidentity"
)

type actorSeed struct {
	address  authoritystore.ActorAddress
	bucket   rediskey.AuthorityBucketRef
	fence    authoritystore.ShardWriteFence
	claimID  string
	snapshot authoritystore.AuthoritySnapshot
	key      authoritystore.IdempotencyKey
	hash     authoritystore.PayloadHash
	outcome  authoritystore.StoredOperationOutcome
}

func actorIDFor(e *benchmarkEnvironment, domain actorrouting.RouteDomain, distribution string, targetShard uint32) (uint64, error) {
	for attempts := 0; attempts < 100000; attempts++ {
		id := uint64(10_000_000) + nextID()
		if distribution != "hot" {
			return id, nil
		}
		shard, err := e.calculator.ShardFor(domain, id)
		if err != nil {
			return 0, err
		}
		if shard == targetShard {
			return id, nil
		}
	}
	return 0, fmt.Errorf("cannot allocate actor in shard %d", targetShard)
}

func loadActor(ctx context.Context, e *benchmarkEnvironment, domain actorrouting.RouteDomain, distribution string, payload []byte) (*actorSeed, error) {
	actorID, err := actorIDFor(e, domain, distribution, 0)
	if err != nil {
		return nil, err
	}
	address := authoritystore.ActorAddress{Domain: domain, ActorID: actorID}
	fence, bucket, err := e.fenceFor(ctx, address)
	if err != nil {
		return nil, err
	}
	claimID := fmt.Sprintf("cap-claim-%d", nextID())
	loaded, err := e.authority.LoadAndClaim(ctx, authoritystore.LoadAndClaimRequest{
		ActorAddress: address, AuthorityLayoutVersion: "v1", ShardWriteFence: fence,
		ClaimOperationID: "load-" + claimID, NewWriterClaimID: claimID,
		InitialModelFields: authoritystore.ModelState{"record": append([]byte(nil), payload...)},
	})
	if err != nil {
		return nil, err
	}
	if loaded.Status != authoritystore.LoadAndClaimClaimed {
		return nil, fmt.Errorf("actor setup load status=%s", loaded.Status)
	}
	return &actorSeed{address: address, bucket: bucket, fence: fence, claimID: claimID, snapshot: loaded.Snapshot}, nil
}

func commitRequest(seed *actorSeed, commandType string, payload []byte, withOrder bool) authoritystore.AuthorityCommitRequest {
	opID := fmt.Sprintf("cap-operation-%d", nextID())
	rootKind := authoritystore.RootKindPlayerActor
	if seed.address.Domain == actorrouting.DomainFarm {
		rootKind = authoritystore.RootKindFarmActor
	}
	key := authoritystore.IdempotencyKey{SubjectPlayerID: seed.address.ActorID, StableCommandTypeName: commandType,
		OperationID: opID, Root: authoritystore.OperationRootRef{Kind: rootKind, CanonicalID: strconv.FormatUint(seed.address.ActorID, 10)}}
	hash := sha256.Sum256(payload)
	outcome := authoritystore.StoredOperationOutcome{Status: authoritystore.TerminalSucceeded, Payload: []byte("ok:" + opID)}
	req := authoritystore.AuthorityCommitRequest{
		CommitRequestID: "commit-" + opID, ActorAddress: seed.address, AuthorityLayoutVersion: "v1",
		ShardWriteFence: seed.fence, ExpectedWriterClaimID: seed.claimID,
		ExpectedRecordVersion: seed.snapshot.RecordVersion, IdempotencyKey: key,
		CanonicalPayloadHash: hash, CandidateState: authoritystore.ModelState{"record": append([]byte(nil), payload...)},
		Receipt: authoritystore.AuthorityReceipt{ReceiptID: "receipt-" + opID, IdempotencyKey: key,
			CanonicalPayloadHash: hash, Outcome: outcome}, OperationResult: outcome,
	}
	if withOrder {
		orderID, _ := workidentity.StableOpaqueID("capredisorder", commandType, opID)
		orderBody := payload
		if len(orderBody) > 8*1024 {
			orderBody = orderBody[:8*1024]
		}
		orderPayload := append([]byte("order:"), orderBody...)
		req.AttachedOrders = []authoritystore.AttachedOrder{{OrderID: orderID, OrderType: "CAP_ORDER",
			Payload: orderPayload, PayloadHash: sha256.Sum256(orderPayload), DueAtMs: time.Now().UnixMilli()}}
	}
	seed.key, seed.hash, seed.outcome = key, hash, outcome
	return req
}

func prepareActorLoad(domain actorrouting.RouteDomain) func(context.Context, *benchmarkEnvironment, string, int, int) (*preparedScenario, error) {
	return func(ctx context.Context, e *benchmarkEnvironment, distribution string, concurrency, total int) (*preparedScenario, error) {
		operations := make([]measuredOperation, total)
		for i := range operations {
			payload := payloadFor(i)
			actorID, err := actorIDFor(e, domain, distribution, 0)
			if err != nil {
				return nil, err
			}
			address := authoritystore.ActorAddress{Domain: domain, ActorID: actorID}
			fence, _, err := e.fenceFor(ctx, address)
			if err != nil {
				return nil, err
			}
			claimID := fmt.Sprintf("measured-load-%d", nextID())
			operations[i] = func(ctx context.Context, _ int) (sampleResult, error) {
				serializeStart := time.Now()
				state := authoritystore.ModelState{"record": payload}
				logicalRequest := authoritystore.ModelStateSerializedBytes(state) + len(claimID) + 64
				serialization := time.Since(serializeStart)
				result, err := e.authority.LoadAndClaim(ctx, authoritystore.LoadAndClaimRequest{ActorAddress: address,
					AuthorityLayoutVersion: "v1", ShardWriteFence: fence, ClaimOperationID: "op-" + claimID,
					NewWriterClaimID: claimID, InitialModelFields: state})
				if err != nil {
					return sampleResult{}, err
				}
				if result.Status != authoritystore.LoadAndClaimClaimed {
					return sampleResult{}, fmt.Errorf("load status=%s", result.Status)
				}
				deserializeStart := time.Now()
				logicalResponse := authoritystore.ModelStateSerializedBytes(result.Snapshot.ModelFields) + len(result.Snapshot.WriterClaimID) + 96
				deserialization := time.Since(deserializeStart)
				return sampleResult{Outcome: result.Status.String(), LogicalRequestBytes: logicalRequest,
					LogicalResponseBytes: logicalResponse, PayloadBytes: len(payload), SerializationTime: serialization,
					DeserializationTime: deserialization}, nil
			}
		}
		keyLabel := "uniform_key"
		if distribution == "hot" {
			keyLabel = "hot_slot_distinct_actor_keys"
		}
		return &preparedScenario{operations: operations, keyDistribution: keyLabel}, nil
	}
}

func prepareActorCommit(domain actorrouting.RouteDomain, withOrder bool) func(context.Context, *benchmarkEnvironment, string, int, int) (*preparedScenario, error) {
	return func(ctx context.Context, e *benchmarkEnvironment, distribution string, concurrency, total int) (*preparedScenario, error) {
		operations := make([]measuredOperation, total)
		type dueRef struct {
			bucket           rediskey.AuthorityBucketRef
			workType, workID string
		}
		var dueRefs []dueRef
		for i := range operations {
			payload := payloadFor(i)
			seed, err := loadActor(ctx, e, domain, distribution, []byte("initial"))
			if err != nil {
				return nil, err
			}
			command := "BUY"
			if domain == actorrouting.DomainFarm {
				command = "PLANT"
			}
			req := commitRequest(seed, command, payload, withOrder)
			if withOrder {
				dueRefs = append(dueRefs, dueRef{bucket: seed.bucket, workType: req.AttachedOrders[0].OrderType, workID: req.AttachedOrders[0].OrderID})
			}
			operations[i] = func(ctx context.Context, _ int) (sampleResult, error) {
				serializeStart := time.Now()
				logicalRequest := authoritystore.AuthorityCommitSerializedBytes(req)
				serialization := time.Since(serializeStart)
				result, err := e.authority.Commit(ctx, req)
				if err != nil {
					return sampleResult{}, err
				}
				if result.Status != authoritystore.CommitCommitted {
					return sampleResult{}, fmt.Errorf("commit status=%s", result.Status)
				}
				deserializeStart := time.Now()
				logicalResponse := authoritystore.AuthorityCommitResponseSerializedBytes(result)
				deserialization := time.Since(deserializeStart)
				return sampleResult{Outcome: result.Status.String(), LogicalRequestBytes: logicalRequest,
					LogicalResponseBytes: logicalResponse, PayloadBytes: len(payload), SerializationTime: serialization,
					DeserializationTime: deserialization}, nil
			}
		}
		keyLabel := "uniform_key"
		if distribution == "hot" {
			keyLabel = "hot_slot_distinct_actor_keys"
		}
		notes := []string(nil)
		var observeDue func(context.Context) (int64, int64, error)
		if withOrder {
			notes = append(notes, "Commit includes one durable AuthorityReceipt, SourceOrder, and DueIndex member atomically.",
				"The nonterminal SourceOrder fixture intentionally owns one Due member per operation; expected and observed fixture Due are reported before teardown.")
			observeDue = func(ctx context.Context) (int64, int64, error) {
				var observed int64
				for _, ref := range dueRefs {
					_, found, err := e.authority.WorkLease().DueIndexScore(ctx, ref.bucket, ref.workType, ref.workID)
					if err != nil {
						return int64(len(dueRefs)), observed, err
					}
					if found {
						observed++
					}
				}
				return int64(len(dueRefs)), observed, nil
			}
		}
		return &preparedScenario{operations: operations, keyDistribution: keyLabel, notes: notes, observeDue: observeDue}, nil
	}
}

func prepareFenceResolution(ctx context.Context, e *benchmarkEnvironment, distribution string, concurrency, total int) (*preparedScenario, error) {
	operations := make([]measuredOperation, total)
	for i := range operations {
		payload := payloadFor(i)
		seed, err := loadActor(ctx, e, actorrouting.DomainPlayer, distribution, []byte("before-timeout"))
		if err != nil {
			return nil, err
		}
		req := commitRequest(seed, "BUY", payload, false)
		committed, err := e.authority.Commit(ctx, req)
		if err != nil || committed.Status != authoritystore.CommitCommitted {
			return nil, fmt.Errorf("resolution setup commit=%s err=%v", committed.Status, err)
		}
		resolveID := fmt.Sprintf("resolve-%d", nextID())
		fenceReq := authoritystore.FenceAndClaimRequest{ResolveOperationID: resolveID, ActorAddress: seed.address,
			AuthorityLayoutVersion: "v1", ShardWriteFence: seed.fence, OldWriterClaimID: seed.claimID,
			NewWriterClaimID: "new-" + resolveID, OriginalIdempotencyKey: seed.key,
			CanonicalPayloadHash: seed.hash, BeforeRecordVersion: seed.snapshot.RecordVersion,
			BeforeBusinessStateDigest: seed.snapshot.BusinessStateDigest}
		operations[i] = func(ctx context.Context, _ int) (sampleResult, error) {
			start := time.Now()
			logicalRequest := len(resolveID) + len(seed.claimID) + len(fenceReq.NewWriterClaimID) + len(seed.key.OperationID) + 192
			serialization := time.Since(start)
			result, err := e.authority.FenceAndClaim(ctx, fenceReq)
			if err != nil {
				return sampleResult{}, err
			}
			if result.Status != authoritystore.FenceResolutionCommitted {
				return sampleResult{}, fmt.Errorf("resolution status=%s", result.Status)
			}
			desStart := time.Now()
			logicalResponse := authoritystore.ModelStateSerializedBytes(result.Snapshot.ModelFields) + len(result.Outcome.Payload) + 192
			deserialization := time.Since(desStart)
			return sampleResult{Outcome: result.Status.String(), LogicalRequestBytes: logicalRequest,
				LogicalResponseBytes: logicalResponse, PayloadBytes: len(payload), SerializationTime: serialization,
				DeserializationTime: deserialization}, nil
		}
	}
	keyLabel := "uniform_key"
	if distribution == "hot" {
		keyLabel = "hot_slot_distinct_actor_keys"
	}
	return &preparedScenario{operations: operations, keyDistribution: keyLabel,
		notes: []string{"Models TIMEOUT_UNKNOWN resolution: original Commit is durable before the measured FenceAndClaim receipt lookup/cut."}}, nil
}

func actorScenarios() []scenarioDefinition {
	return []scenarioDefinition{
		{name: "player_actor_load_and_claim", group: "actor_authority", prepare: prepareActorLoad(actorrouting.DomainPlayer)},
		{name: "farm_actor_load_and_claim", group: "actor_authority", prepare: prepareActorLoad(actorrouting.DomainFarm)},
		{name: "player_actor_commit_receipt", group: "actor_authority", prepare: prepareActorCommit(actorrouting.DomainPlayer, false)},
		{name: "farm_actor_commit_receipt", group: "actor_authority", prepare: prepareActorCommit(actorrouting.DomainFarm, false)},
		{name: "farm_actor_commit_order_due", group: "reliable_execution", prepare: prepareActorCommit(actorrouting.DomainFarm, true)},
		{name: "actor_timeout_unknown_fence_resolution", group: "actor_authority", prepare: prepareFenceResolution},
	}
}
