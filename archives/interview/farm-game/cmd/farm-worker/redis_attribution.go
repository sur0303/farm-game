package main

import (
	"context"

	"farmgame/internal/observability/redisworkload"
	"farmgame/internal/worker/theftrecovery"
)

// attributedPairDispatchRepository keeps Pair reads on their existing
// repository while marking only bounded DueIndex discovery as due_scan.
type attributedPairDispatchRepository struct {
	*theftrecovery.FriendPairDispatchRepository
}

func (r *attributedPairDispatchRepository) Discover(ctx context.Context, request theftrecovery.DiscoveryRequest) (theftrecovery.DiscoveryPage, error) {
	return r.FriendPairDispatchRepository.Discover(redisworkload.WithComponent(ctx, redisworkload.ComponentDueScan), request)
}

func (r *attributedPairDispatchRepository) ObserveDue(ctx context.Context, limit int) (theftrecovery.PairDueObservation, error) {
	return r.FriendPairDispatchRepository.ObserveDue(redisworkload.WithComponent(ctx, redisworkload.ComponentDueScan), limit)
}

// attributedPairWorkLease preserves the Pair lease implementation and exact
// fences. Only terminal completion receives diagnostic context attribution.
type attributedPairWorkLease struct {
	*theftrecovery.RedisWorkLeaseRepository
}

func (r *attributedPairWorkLease) Complete(ctx context.Context, request theftrecovery.CompleteRequest) (theftrecovery.LeaseCompleteResult, error) {
	return r.RedisWorkLeaseRepository.Complete(redisworkload.WithComponent(ctx, redisworkload.ComponentWorkComplete), request)
}
