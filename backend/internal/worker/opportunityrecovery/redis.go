package opportunityrecovery

import (
	"context"
	"fmt"

	authority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
)

// RedisScheduleStore is the production adapter for the existing mutable
// schedule API. It creates no Redis keys and has no lifecycle of its own.
type RedisScheduleStore struct {
	lease *authority.WorkLeaseStore
}

var _ ScheduleStore = (*RedisScheduleStore)(nil)

func NewRedisScheduleStore(lease *authority.WorkLeaseStore) (*RedisScheduleStore, error) {
	if lease == nil {
		return nil, fmt.Errorf("%w: WorkLeaseStore is required", ErrInvalidConfig)
	}
	return &RedisScheduleStore{lease: lease}, nil
}

// NewWorkLeaseStoreAdapter is an explicit name for composition roots that
// prefer to describe the wrapped authority dependency rather than Redis.
func NewWorkLeaseStoreAdapter(lease *authority.WorkLeaseStore) (*RedisScheduleStore, error) {
	return NewRedisScheduleStore(lease)
}

func (s *RedisScheduleStore) ListDueScheduledWork(ctx context.Context, bucket rediskey.AuthorityBucketRef, nowMs int64, limit int) ([]authority.ScheduledDueRef, error) {
	if s == nil || s.lease == nil {
		return nil, fmt.Errorf("%w: nil Redis schedule adapter", ErrInvalidConfig)
	}
	return s.lease.ListDueScheduledWork(ctx, bucket, nowMs, limit)
}

func (s *RedisScheduleStore) ClaimScheduledWorkWithBoot(ctx context.Context, bucket rediskey.AuthorityBucketRef, scheduleType, scheduleID, holderID, bootID, leaseToken string, requestedTTLms int64) (authority.WorkClaim, error) {
	if s == nil || s.lease == nil {
		return authority.WorkClaim{}, fmt.Errorf("%w: nil Redis schedule adapter", ErrInvalidConfig)
	}
	return s.lease.ClaimScheduledWorkWithBoot(ctx, bucket, scheduleType, scheduleID, holderID, bootID, leaseToken, requestedTTLms)
}

func (s *RedisScheduleStore) GetScheduledWork(ctx context.Context, bucket rediskey.AuthorityBucketRef, scheduleType, scheduleID string) (authority.ScheduledWorkSnapshot, error) {
	if s == nil || s.lease == nil {
		return authority.ScheduledWorkSnapshot{}, fmt.Errorf("%w: nil Redis schedule adapter", ErrInvalidConfig)
	}
	return s.lease.GetScheduledWork(ctx, bucket, scheduleType, scheduleID)
}

func (s *RedisScheduleStore) AdvanceScheduledWork(ctx context.Context, bucket rediskey.AuthorityBucketRef, scheduleType, scheduleID, holderID, bootID string, leaseEpoch, workVersion, generation uint64, leaseToken string, nextDueAtMs int64, payload []byte, payloadHash string) (authority.ScheduledWorkAdvanceResult, error) {
	if s == nil || s.lease == nil {
		return authority.ScheduledWorkAdvanceResult{}, fmt.Errorf("%w: nil Redis schedule adapter", ErrInvalidConfig)
	}
	return s.lease.AdvanceScheduledWork(ctx, bucket, scheduleType, scheduleID, holderID, bootID, leaseEpoch, workVersion, generation, leaseToken, nextDueAtMs, payload, payloadHash)
}

func (s *RedisScheduleStore) CompleteScheduledWork(ctx context.Context, bucket rediskey.AuthorityBucketRef, scheduleType, scheduleID, holderID, bootID string, leaseEpoch, workVersion, generation uint64, leaseToken string) (authority.WorkCompleteResult, error) {
	if s == nil || s.lease == nil {
		return authority.WorkCompleteResult{}, fmt.Errorf("%w: nil Redis schedule adapter", ErrInvalidConfig)
	}
	return s.lease.CompleteScheduledWork(ctx, bucket, scheduleType, scheduleID, holderID, bootID, leaseEpoch, workVersion, generation, leaseToken)
}
