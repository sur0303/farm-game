package main

import (
	"context"
	"fmt"
	"sort"

	"farmgame/internal/worker/carerecovery"
	"farmgame/internal/worker/opportunityrecovery"
)

// mappedSourceOrderShardAssignment reuses the independently controlled
// FARM_WORKER domain for every Authority SourceOrder family. FARM source
// shards map one-to-one when shard counts match; PLAYER source shards use a
// stable modulo mapping. RouteControl therefore moves recovery ownership
// without introducing a second PLAYER_WORKER control domain. WorkLease remains
// the correctness fence during the bounded overlap of a rebalance.
type mappedSourceOrderShardAssignment struct {
	provider         opportunityrecovery.WorkerShardAssignmentProvider
	admission        opportunityrecovery.WorkerShardClaimAdmission
	configured       []uint32
	workerShardCount uint32
}

func newMappedSourceOrderShardAssignment(
	provider opportunityrecovery.WorkerShardAssignmentProvider,
	admission opportunityrecovery.WorkerShardClaimAdmission,
	configured []uint32,
	workerShardCount uint32,
) (*mappedSourceOrderShardAssignment, error) {
	if provider == nil || admission == nil || len(configured) == 0 || workerShardCount == 0 {
		return nil, fmt.Errorf("source-order shard assignment requires provider, admission, configured shards and worker shard count")
	}
	seen := make(map[uint32]struct{}, len(configured))
	shards := make([]uint32, 0, len(configured))
	for _, shard := range configured {
		if _, duplicate := seen[shard]; duplicate {
			return nil, fmt.Errorf("duplicate source-order shard %d", shard)
		}
		seen[shard] = struct{}{}
		shards = append(shards, shard)
	}
	sort.Slice(shards, func(i, j int) bool { return shards[i] < shards[j] })
	return &mappedSourceOrderShardAssignment{
		provider: provider, admission: admission, configured: shards, workerShardCount: workerShardCount,
	}, nil
}

func (a *mappedSourceOrderShardAssignment) Assignment(ctx context.Context) (carerecovery.ShardAssignment, error) {
	base, err := a.provider.Assignment(ctx)
	if err != nil {
		return carerecovery.ShardAssignment{}, err
	}
	ownedWorkerShards := make(map[uint32]struct{}, len(base.OwnedShards))
	for _, shard := range base.OwnedShards {
		if shard >= a.workerShardCount {
			return carerecovery.ShardAssignment{}, fmt.Errorf("FARM_WORKER shard %d outside count %d", shard, a.workerShardCount)
		}
		ownedWorkerShards[shard] = struct{}{}
	}
	owned := make([]uint32, 0, len(a.configured))
	for _, sourceShard := range a.configured {
		if _, ok := ownedWorkerShards[sourceShard%a.workerShardCount]; ok {
			owned = append(owned, sourceShard)
		}
	}
	return carerecovery.ShardAssignment{Revision: base.Revision, OwnedShards: owned}, nil
}

func (a *mappedSourceOrderShardAssignment) CanClaimShard(ctx context.Context, sourceShard uint32, revision uint64) error {
	return a.admission.CanClaimShard(ctx, sourceShard%a.workerShardCount, revision)
}

var _ carerecovery.ShardAssignmentProvider = (*mappedSourceOrderShardAssignment)(nil)
var _ carerecovery.ShardClaimAdmission = (*mappedSourceOrderShardAssignment)(nil)
