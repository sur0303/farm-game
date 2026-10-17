package redisworkload

import (
	"context"

	authority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
)

// AttributedWorkLease is a composition-only adapter around the existing
// authority WorkLeaseStore. Embedding preserves every fence/lease operation
// unchanged; only due discovery and terminal completion override the context
// supplied to Redis workload diagnostics.
type AttributedWorkLease struct {
	*authority.WorkLeaseStore
}

func NewAttributedWorkLease(store *authority.WorkLeaseStore) *AttributedWorkLease {
	if store == nil {
		return nil
	}
	return &AttributedWorkLease{WorkLeaseStore: store}
}

func (s *AttributedWorkLease) ListDueWork(ctx context.Context, bucket rediskey.AuthorityBucketRef, limit int) ([]authority.DueWorkRef, error) {
	return s.WorkLeaseStore.ListDueWork(WithComponent(ctx, ComponentDueScan), bucket, limit)
}

func (s *AttributedWorkLease) ListDueWorkAt(ctx context.Context, bucket rediskey.AuthorityBucketRef, nowMs int64, limit int) ([]authority.DueWorkRef, error) {
	return s.WorkLeaseStore.ListDueWorkAt(WithComponent(ctx, ComponentDueScan), bucket, nowMs, limit)
}

func (s *AttributedWorkLease) ListDueWorkPage(ctx context.Context, bucket rediskey.AuthorityBucketRef, family string, cursor authority.DueWorkCursor, limit int) (authority.DueWorkPage, error) {
	return s.WorkLeaseStore.ListDueWorkPage(WithComponent(ctx, ComponentDueScan), bucket, family, cursor, limit)
}

func (s *AttributedWorkLease) ListDueWorkPageAt(ctx context.Context, bucket rediskey.AuthorityBucketRef, nowMs int64, family string, cursor authority.DueWorkCursor, limit int) (authority.DueWorkPage, error) {
	return s.WorkLeaseStore.ListDueWorkPageAt(WithComponent(ctx, ComponentDueScan), bucket, nowMs, family, cursor, limit)
}

func (s *AttributedWorkLease) ListDueCareWork(ctx context.Context, bucket rediskey.AuthorityBucketRef, limit int) ([]authority.DueWorkRef, error) {
	return s.WorkLeaseStore.ListDueCareWork(WithComponent(ctx, ComponentDueScan), bucket, limit)
}

func (s *AttributedWorkLease) ListDueScheduledWork(ctx context.Context, bucket rediskey.AuthorityBucketRef, nowMs int64, limit int) ([]authority.ScheduledDueRef, error) {
	return s.WorkLeaseStore.ListDueScheduledWork(WithComponent(ctx, ComponentDueScan), bucket, nowMs, limit)
}

func (s *AttributedWorkLease) ClaimBucketScanLease(ctx context.Context, bucket rediskey.AuthorityBucketRef, holderID string, requestedTTLms int64) (authority.ScanLeaseResult, error) {
	return s.WorkLeaseStore.ClaimBucketScanLease(WithComponent(ctx, ComponentDueScan), bucket, holderID, requestedTTLms)
}

func (s *AttributedWorkLease) CompleteWorkWithProof(ctx context.Context, bucket rediskey.AuthorityBucketRef, workType, workID, holderID, bootID string, leaseEpoch, workVersion uint64, leaseToken string, proof authority.WorkCompletionProof) (authority.WorkCompleteResult, error) {
	return s.WorkLeaseStore.CompleteWorkWithProof(WithComponent(ctx, ComponentWorkComplete), bucket, workType, workID, holderID, bootID, leaseEpoch, workVersion, leaseToken, proof)
}

func (s *AttributedWorkLease) ClaimAndCompleteWithProof(ctx context.Context, bucket rediskey.AuthorityBucketRef, workType, workID, holderID, bootID, leaseToken string, requestedTTLms int64, proof authority.WorkCompletionProof) (authority.WorkCompleteResult, error) {
	return s.WorkLeaseStore.ClaimAndCompleteWithProof(WithComponent(ctx, ComponentWorkComplete), bucket, workType, workID, holderID, bootID, leaseToken, requestedTTLms, proof)
}

func (s *AttributedWorkLease) ClaimAndCompleteWithProofBatch(ctx context.Context, requests []authority.WorkClaimAndCompleteRequest) []authority.WorkClaimAndCompleteBatchResult {
	return s.WorkLeaseStore.ClaimAndCompleteWithProofBatch(WithComponent(ctx, ComponentWorkComplete), requests)
}

func (s *AttributedWorkLease) CompleteScheduledWork(ctx context.Context, bucket rediskey.AuthorityBucketRef, scheduleType, scheduleID, holderID, bootID string, leaseEpoch, workVersion, generation uint64, leaseToken string) (authority.WorkCompleteResult, error) {
	return s.WorkLeaseStore.CompleteScheduledWork(WithComponent(ctx, ComponentWorkComplete), bucket, scheduleType, scheduleID, holderID, bootID, leaseEpoch, workVersion, generation, leaseToken)
}
