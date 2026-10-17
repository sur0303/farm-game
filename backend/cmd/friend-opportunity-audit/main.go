// Command friend-opportunity-audit inspects one FARM_OPPORTUNITY_WAKEUP
// without scanning the shared Redis namespace. It is intended for isolated
// end-to-end diagnostics and never prints Redis credentials.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/farm/opportunitywakeup"
	"farmgame/internal/redisdomains"
	"farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/worker/opportunityrecovery"
)

type report struct {
	FarmID              uint64     `json:"farm_id"`
	Namespace           string     `json:"namespace"`
	BucketShard         uint32     `json:"bucket_shard"`
	TrustedNowMS        int64      `json:"trusted_now_ms"`
	DueIndexEntries     []dueEntry `json:"due_index_entries"`
	ScheduleFound       bool       `json:"schedule_found"`
	ScheduleError       string     `json:"schedule_error,omitempty"`
	ScheduleType        string     `json:"schedule_type,omitempty"`
	ScheduleID          string     `json:"schedule_id,omitempty"`
	ScheduleGeneration  uint64     `json:"schedule_generation,omitempty"`
	ScheduleDueAtMS     int64      `json:"schedule_due_at_ms,omitempty"`
	ScheduleState       string     `json:"schedule_state,omitempty"`
	SchedulePayloadFarm uint64     `json:"schedule_payload_farm_id,omitempty"`
	FarmFound           bool       `json:"farm_found"`
	FarmError           string     `json:"farm_error,omitempty"`
	FarmVersion         uint64     `json:"farm_version,omitempty"`
	HeaderNextAtMS      int64      `json:"header_next_opportunity_at_ms,omitempty"`
	FactsCycles         int        `json:"facts_cycle_count,omitempty"`
}

type dueEntry struct {
	Member string  `json:"member"`
	Score  float64 `json:"score"`
}

func main() {
	var farmID uint64
	var routePath, namespaceEnv, namespaceZone string
	flag.Uint64Var(&farmID, "farm-id", 0, "farm id to inspect")
	flag.StringVar(&routePath, "route", "", "route snapshot path")
	flag.StringVar(&namespaceEnv, "namespace", "dev", "Redis namespace environment")
	flag.StringVar(&namespaceZone, "zone", "z1", "Redis namespace zone")
	flag.Parse()
	if farmID == 0 || strings.TrimSpace(routePath) == "" {
		log.Fatal("--farm-id and --route are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rawURL := strings.TrimSpace(os.Getenv(redisdomains.LegacyURLEnv))
	clusterURLs := strings.TrimSpace(os.Getenv(redisdomains.LegacyClusterURLEnv))
	if rawURL == "" && clusterURLs == "" {
		log.Fatal("FARM_REDIS_URL or FARM_REDIS_CLUSTER_URLS is required")
	}
	client, err := redisdomains.OpenEndpoint(ctx, rawURL, clusterURLs)
	if err != nil {
		log.Fatalf("connect Redis: %v", err)
	}
	defer client.Close()

	resolver, err := actorrouting.LoadStaticResolver(routePath)
	if err != nil {
		log.Fatalf("load route snapshot: %v", err)
	}
	snapshot := resolver.Snapshot()
	calculator, err := actorrouting.NewCalculator(snapshot.Realm,
		actorrouting.HashProfile{Version: snapshot.HashVersion, Seed: snapshot.HashSeed},
		snapshot.Player.ShardCount, snapshot.Farm.ShardCount)
	if err != nil {
		log.Fatalf("build route calculator: %v", err)
	}
	namespace, err := rediskey.NewNamespace(namespaceEnv, namespaceZone)
	if err != nil {
		log.Fatalf("build namespace: %v", err)
	}
	profile := rediskey.PhysicalProfile{
		ProfileVersion: "redis-physical-v1", Namespace: namespace,
		AuthorityLayoutVersion: "v1", WorkflowLayoutVersion: "v1",
		WorkflowHashVersion: 1, WorkflowBucketCount: 256,
	}
	store, err := authority.NewStore(client, nil, authority.Options{Profile: profile, Resolver: calculator})
	if err != nil {
		log.Fatalf("build authority store: %v", err)
	}
	bucket, err := profile.AuthorityBucketForActor(calculator, actorrouting.ActorAddress{
		Domain: actorrouting.DomainFarm, ActorID: farmID,
	})
	if err != nil {
		log.Fatalf("resolve FARM bucket: %v", err)
	}
	keys, err := rediskey.NewAuthorityKeys(profile, calculator)
	if err != nil {
		log.Fatalf("build authority keys: %v", err)
	}
	dueKey, err := keys.SourceDueIndex(bucket)
	if err != nil {
		log.Fatalf("build DueIndex key: %v", err)
	}

	nowMS, err := store.WorkLease().TrustedNowMs(ctx)
	if err != nil {
		log.Fatalf("read trusted time: %v", err)
	}
	rows, err := client.ZRangeWithScores(ctx, dueKey.String(), 0, -1).Result()
	if err != nil {
		log.Fatalf("read DueIndex: %v", err)
	}
	result := report{FarmID: farmID, Namespace: namespaceEnv + "/" + namespaceZone,
		BucketShard: bucket.VirtualShard(), TrustedNowMS: nowMS}
	for _, row := range rows {
		result.DueIndexEntries = append(result.DueIndexEntries, dueEntry{Member: fmt.Sprint(row.Member), Score: row.Score})
	}

	scheduleID, err := opportunitywakeup.ScheduleIDForFarm(farmID)
	if err != nil {
		log.Fatalf("build schedule id: %v", err)
	}
	schedule, scheduleErr := store.WorkLease().GetScheduledWork(ctx, bucket, opportunitywakeup.WorkType, scheduleID)
	if scheduleErr != nil {
		result.ScheduleError = scheduleErr.Error()
	} else {
		result.ScheduleFound = true
		result.ScheduleType = schedule.ScheduleType
		result.ScheduleID = schedule.ScheduleID
		result.ScheduleGeneration = schedule.Generation
		result.ScheduleDueAtMS = schedule.NextDueAtMs
		result.ScheduleState = schedule.State
		if payload, decodeErr := opportunitywakeup.Unmarshal(schedule.Payload); decodeErr == nil {
			result.SchedulePayloadFarm = payload.FarmID
		}
	}

	reader, err := opportunityrecovery.NewFarmOpportunityReader(store)
	if err != nil {
		log.Fatalf("build farm reader: %v", err)
	}
	farm, farmErr := reader.ReadFarmOpportunity(ctx, farmID)
	if farmErr != nil {
		result.FarmError = farmErr.Error()
	} else {
		result.FarmFound = true
		result.FarmVersion = farm.Facts.SourceFarmVersion
		result.HeaderNextAtMS = farm.Header.NextOpportunityAtMs
		result.FactsCycles = len(farm.Facts.Cycles)
	}

	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(encoded))
}
