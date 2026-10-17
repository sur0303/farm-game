package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	redisauthority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/workflow"
	"farmgame/internal/workidentity"
)

func newWorkRecord(index int, payload []byte) (*workflow.WorkRecord, error) {
	opID := fmt.Sprintf("cap-work-operation-%d-%d", nextID(), index)
	workID, err := workidentity.StableOpaqueID("capredisworkflow", "CAP_WORK", opID)
	if err != nil {
		return nil, err
	}
	address := workidentity.WorkAddress{WorkID: workID, WorkType: "CAP_WORK", PlacementKind: workidentity.IndependentWorkBucket,
		BucketVersion: 1, BucketID: uint32(index % 64)}
	hash := sha256.Sum256(payload)
	now := time.Now()
	return workflow.NewWorkRecord(address, uint64(20_000_000+index), opID, payload, hash,
		[]string{"PARTICIPANT", "RESULT"}, now, now.Add(time.Hour))
}

func workflowRepo(e *benchmarkEnvironment, suffix string) (*workflow.RedisWorkRepository, error) {
	return workflow.NewRedisWorkRepository(e.client, fmt.Sprintf("%s:%s:%d", e.workflowBase, suffix, nextID()))
}

func jsonMetrics(record *workflow.WorkRecord) (requestBytes int, serialization time.Duration, deserialization time.Duration, err error) {
	start := time.Now()
	data, err := json.Marshal(record)
	serialization = time.Since(start)
	if err != nil {
		return 0, serialization, 0, err
	}
	start = time.Now()
	var decoded workflow.WorkRecord
	err = json.Unmarshal(data, &decoded)
	deserialization = time.Since(start)
	if err == nil {
		err = decoded.Validate()
	}
	return len(data), serialization, deserialization, err
}

func prepareWorkflowCreate(ctx context.Context, e *benchmarkEnvironment, distribution string, concurrency, total int) (*preparedScenario, error) {
	repo, err := workflowRepo(e, "create")
	if err != nil {
		return nil, err
	}
	operations := make([]measuredOperation, total)
	for i := range operations {
		payload := payloadFor(i)
		record, err := newWorkRecord(i, payload)
		if err != nil {
			return nil, err
		}
		operations[i] = func(ctx context.Context, _ int) (sampleResult, error) {
			logical, ser, des, err := jsonMetrics(record)
			if err != nil {
				return sampleResult{}, err
			}
			created, inserted, err := repo.CreateIfAbsent(ctx, record)
			if err != nil {
				return sampleResult{}, err
			}
			if !inserted || created.WorkVersion != 1 {
				return sampleResult{}, fmt.Errorf("workflow create inserted=%v version=%d", inserted, created.WorkVersion)
			}
			return sampleResult{Outcome: "CREATED", LogicalRequestBytes: logical, LogicalResponseBytes: logical,
				PayloadBytes: len(payload), SerializationTime: ser, DeserializationTime: des}, nil
		}
	}
	return &preparedScenario{operations: operations, keyDistribution: workflowDistribution(distribution),
		observeDue: func(ctx context.Context) (int64, int64, error) {
			observed, err := repo.PendingDue(ctx)
			return int64(total), observed, err
		},
		notes: []string{"Uniform uses distinct record keys; hot uses distinct records sharing the single generic Workflow DueIndex slot.", "The nonterminal Create fixture intentionally owns one Due member per operation; expected and observed fixture Due are reported before teardown."}}, nil
}

func prepareWorkflowAdvance(ctx context.Context, e *benchmarkEnvironment, distribution string, concurrency, total int) (*preparedScenario, error) {
	repo, err := workflowRepo(e, "advance")
	if err != nil {
		return nil, err
	}
	operations := make([]measuredOperation, total)
	for i := range operations {
		payload := payloadFor(i)
		record, err := newWorkRecord(i, payload)
		if err != nil {
			return nil, err
		}
		created, inserted, err := repo.CreateIfAbsent(ctx, record)
		if err != nil || !inserted {
			return nil, fmt.Errorf("advance setup inserted=%v err=%v", inserted, err)
		}
		next := *created
		next.State = workflow.WorkStateRunning
		operations[i] = func(ctx context.Context, _ int) (sampleResult, error) {
			logical, ser, des, err := jsonMetrics(&next)
			if err != nil {
				return sampleResult{}, err
			}
			updated, err := repo.Advance(ctx, &next, workflow.AdvanceOptions{ExpectedWorkVersion: created.WorkVersion})
			if err != nil {
				return sampleResult{}, err
			}
			if updated.WorkVersion != created.WorkVersion+1 {
				return sampleResult{}, fmt.Errorf("advance version=%d", updated.WorkVersion)
			}
			return sampleResult{Outcome: "ADVANCED", LogicalRequestBytes: logical, LogicalResponseBytes: logical,
				PayloadBytes: len(payload), SerializationTime: ser, DeserializationTime: des}, nil
		}
	}
	return &preparedScenario{operations: operations, keyDistribution: workflowDistribution(distribution),
		observeDue: func(ctx context.Context) (int64, int64, error) {
			observed, err := repo.PendingDue(ctx)
			return int64(total), observed, err
		}}, nil
}

func prepareWorkflowClaim(ctx context.Context, e *benchmarkEnvironment, distribution string, concurrency, total int) (*preparedScenario, error) {
	repo, err := workflowRepo(e, "claim")
	if err != nil {
		return nil, err
	}
	operations := make([]measuredOperation, total)
	for i := range operations {
		payload := payloadFor(i)
		record, err := newWorkRecord(i, payload)
		if err != nil {
			return nil, err
		}
		if _, inserted, err := repo.CreateIfAbsent(ctx, record); err != nil || !inserted {
			return nil, fmt.Errorf("claim setup inserted=%v err=%v", inserted, err)
		}
		owner, token := fmt.Sprintf("worker-%d", i), fmt.Sprintf("token-%d", i)
		operations[i] = func(ctx context.Context, _ int) (sampleResult, error) {
			start := time.Now()
			logicalRequest := len(record.Address.WorkID) + len(owner) + len(token) + 32
			serialization := time.Since(start)
			claimed, err := repo.ClaimWork(ctx, record.Address.WorkType, record.Address.WorkID, owner, token, 30*time.Second)
			if err != nil {
				return sampleResult{}, err
			}
			start = time.Now()
			logicalResponse, _, deserialization, err := jsonMetrics(claimed.Record)
			_ = start
			if err != nil {
				return sampleResult{}, err
			}
			return sampleResult{Outcome: "CLAIMED", LogicalRequestBytes: logicalRequest, LogicalResponseBytes: logicalResponse,
				PayloadBytes: len(payload), SerializationTime: serialization, DeserializationTime: deserialization}, nil
		}
	}
	return &preparedScenario{operations: operations, keyDistribution: workflowDistribution(distribution),
		observeDue: func(ctx context.Context) (int64, int64, error) {
			observed, err := repo.PendingDue(ctx)
			return 0, observed, err
		}}, nil
}

func prepareWorkflowRenew(ctx context.Context, e *benchmarkEnvironment, distribution string, concurrency, total int) (*preparedScenario, error) {
	repo, err := workflowRepo(e, "renew")
	if err != nil {
		return nil, err
	}
	operations := make([]measuredOperation, total)
	for i := range operations {
		payload := payloadFor(i)
		record, err := newWorkRecord(i, payload)
		if err != nil {
			return nil, err
		}
		if _, inserted, err := repo.CreateIfAbsent(ctx, record); err != nil || !inserted {
			return nil, fmt.Errorf("renew setup inserted=%v err=%v", inserted, err)
		}
		owner, token := fmt.Sprintf("worker-%d", i), fmt.Sprintf("token-%d", i)
		claimed, err := repo.ClaimWork(ctx, record.Address.WorkType, record.Address.WorkID, owner, token, 30*time.Second)
		if err != nil {
			return nil, err
		}
		operations[i] = func(ctx context.Context, _ int) (sampleResult, error) {
			start := time.Now()
			logicalRequest := len(record.Address.WorkID) + len(owner) + len(token) + 40
			serialization := time.Since(start)
			renewed, err := repo.RenewLease(ctx, record.Address.WorkType, record.Address.WorkID, owner, token, claimed.Record.LeaseEpoch, 30*time.Second)
			if err != nil {
				return sampleResult{}, err
			}
			logicalResponse, _, deserialization, err := jsonMetrics(renewed)
			if err != nil {
				return sampleResult{}, err
			}
			return sampleResult{Outcome: "RENEWED", LogicalRequestBytes: logicalRequest, LogicalResponseBytes: logicalResponse,
				PayloadBytes: len(payload), SerializationTime: serialization, DeserializationTime: deserialization}, nil
		}
	}
	return &preparedScenario{operations: operations, keyDistribution: workflowDistribution(distribution),
		observeDue: func(ctx context.Context) (int64, int64, error) {
			observed, err := repo.PendingDue(ctx)
			return 0, observed, err
		}}, nil
}

func workflowDistribution(distribution string) string {
	if distribution == "hot" {
		return "hot_due_index_distinct_record_keys"
	}
	return "uniform_record_keys_shared_due_index"
}

func prepareResultDirectoryPublish(ctx context.Context, e *benchmarkEnvironment, distribution string, concurrency, total int) (*preparedScenario, error) {
	operations := make([]measuredOperation, total)
	hotPlayer := uint64(30_000_001)
	for i := range operations {
		payload := payloadFor(i)
		if len(payload) > 1024 {
			payload = payload[:1024]
		}
		playerID := uint64(30_000_000) + nextID()
		if distribution == "hot" {
			playerID = hotPlayer
		}
		digest := fmt.Sprintf("%064x", sha256.Sum256([]byte(fmt.Sprintf("result-%d", nextID()))))
		entry := redisauthority.ResultEntry{CommandType: "THEFT", OperationDigest: digest,
			RootKind: "PLAYER_ACTOR", RootID: strconv.FormatUint(playerID, 10), SourceRef: "cap-source",
			FinalStatusClass: "SUCCEEDED", MinimalOutcome: payload}
		operations[i] = func(ctx context.Context, _ int) (sampleResult, error) {
			start := time.Now()
			logicalRequest := len(entry.CommandType) + len(entry.OperationDigest) + len(entry.RootKind) + len(entry.RootID) + len(entry.SourceRef) + len(entry.FinalStatusClass) + len(entry.MinimalOutcome)
			serialization := time.Since(start)
			status, err := e.resultDir.PublishResult(ctx, playerID, entry)
			if err != nil {
				return sampleResult{}, err
			}
			if status != redisauthority.ResultPublishPublished {
				return sampleResult{}, fmt.Errorf("result publish=%s", status)
			}
			return sampleResult{Outcome: status.String(), LogicalRequestBytes: logicalRequest, LogicalResponseBytes: 16,
				PayloadBytes: len(payload), SerializationTime: serialization}, nil
		}
	}
	keyLabel := "uniform_player_directory_keys"
	if distribution == "hot" {
		keyLabel = "hot_player_directory_key_distinct_fields"
	}
	return &preparedScenario{operations: operations, keyDistribution: keyLabel}, nil
}

func workflowScenarios() []scenarioDefinition {
	return []scenarioDefinition{
		{name: "workflow_create_if_absent", group: "reliable_execution", prepare: prepareWorkflowCreate},
		{name: "workflow_advance", group: "reliable_execution", prepare: prepareWorkflowAdvance},
		{name: "workflow_claim", group: "reliable_execution", prepare: prepareWorkflowClaim},
		{name: "workflow_renew", group: "reliable_execution", prepare: prepareWorkflowRenew},
		{name: "result_directory_publish", group: "reliable_execution", prepare: prepareResultDirectoryPublish},
	}
}
