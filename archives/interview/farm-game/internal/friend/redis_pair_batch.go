package friend

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var _ PairBatchReader = (*RedisPairRepository)(nil)

var pairCanonicalManifest = []string{
	"identity:pair_id", "identity:low_player_id", "identity:high_player_id", "relation:id", "relation:status", "relation:epoch",
	"relation:activated_at_ms", "relation:deleted_at_ms", "relation:activation_token_id", "relation:activation_token_generation",
	"relation:termination_reason", "relation:terminated_by_player_id", "relation:last_change_operation_id",
	"block:low_to_high:status", "block:low_to_high:epoch", "block:low_to_high:changed_at_ms", "block:low_to_high:removed_at_ms",
	"block:high_to_low:status", "block:high_to_low:epoch", "block:high_to_low:changed_at_ms", "block:high_to_low:removed_at_ms",
	"reactivation:state", "reactivation:terminated_relation_epoch", "reactivation:barrier_revision",
	"reactivation:low_generation_floor", "reactivation:high_generation_floor", "termination:order_id", "termination:captured_epoch",
	"termination:barrier_revision", "termination:low_floor_captured", "termination:high_floor_captured",
	"termination:quota_released_low", "termination:quota_released_high", "termination:completed", "_meta:schema_version",
	"_meta:authority_layout_version", "_meta:record_version", "_meta:updated_at_ms", "_meta:logical_bytes", "_meta:business_state_digest",
}

type pairBatchBucketRead struct {
	group      friendRedisGroup
	recordKeys []string
	keys       []string
	args       []interface{}
	command    *redis.Cmd
	snapshot   friendAuthoritySnapshot
}

type pairBatchRecordRead struct {
	pairID      string
	low, high   uint64
	bucket      *pairBatchBucketRead
	bucketIndex int
}

// BatchGetPairs groups requested Pair records by physical authority bucket.
// Exactly one CompleteSnapshotV2 script runs per unique bucket; every bucket
// envelope and every requested complete Pair manifest share one Redis
// linearization point. The outer pipeline is only a bounded network batch.
func (r *RedisPairRepository) BatchGetPairs(ctx context.Context, viewer uint64, peers []uint64) ([]PairSnapshot, error) {
	if r == nil || r.authority == nil || viewer == 0 || len(peers) == 0 || len(peers) > MaxPageSize*refillBudgetPages {
		return nil, fmt.Errorf("%w: pair batch shape", ErrInvalidCommand)
	}
	records := make([]pairBatchRecordRead, len(peers))
	buckets := make(map[string]*pairBatchBucketRead)
	bucketOrder := make([]*pairBatchBucketRead, 0, len(peers))
	seen := make(map[uint64]struct{}, len(peers))
	for index, peer := range peers {
		if peer == 0 || peer == viewer {
			return nil, fmt.Errorf("%w: pair batch identity", ErrInvalidCommand)
		}
		if _, duplicate := seen[peer]; duplicate {
			return nil, fmt.Errorf("%w: pair batch duplicate", ErrInvalidCommand)
		}
		seen[peer] = struct{}{}
		pairID, key, low, high, err := r.recordKey(viewer, peer)
		if err != nil {
			return nil, err
		}
		group, err := r.authority.group(FriendPairAuthority, pairID, [32]byte{})
		if err != nil {
			return nil, err
		}
		bucket := buckets[group.meta]
		if bucket == nil {
			bucket = &pairBatchBucketRead{group: group}
			buckets[group.meta] = bucket
			bucketOrder = append(bucketOrder, bucket)
		}
		records[index] = pairBatchRecordRead{pairID: pairID, low: low, high: high,
			bucket: bucket, bucketIndex: len(bucket.recordKeys)}
		bucket.recordKeys = append(bucket.recordKeys, key)
	}
	for _, bucket := range bucketOrder {
		var err error
		bucket.keys, bucket.args, err = r.authority.authoritySnapshotArgs(bucket.group, bucket.recordKeys, pairCanonicalManifest)
		if err != nil {
			return nil, err
		}
	}

	started := time.Now()
	_, pipelineErr := r.authority.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, bucket := range bucketOrder {
			bucket.command = queueFriendAuthorityCompleteSnapshot(ctx, pipe, bucket.keys, bucket.args)
		}
		return nil
	})
	perReadLatency := time.Since(started) / time.Duration(len(bucketOrder))
	var firstErr error
	for _, bucket := range bucketOrder {
		r.authority.envelopeReads.atomicReadTotal.Add(1)
		r.authority.envelopeReads.atomicReadLatencyNanos.Add(uint64(perReadLatency))
		raw, snapshotErr := bucket.command.Result()
		if snapshotErr == nil {
			bucket.snapshot, snapshotErr = decodeFriendAuthoritySnapshot(raw, len(bucket.recordKeys), pairCanonicalManifest)
		}
		if snapshotErr == nil {
			snapshotErr = validateFriendAuthoritySnapshot(bucket.snapshot, bucket.group, FriendPairAuthority)
		}
		present := false
		if snapshotErr == nil {
			for _, record := range bucket.snapshot.records {
				present = present || record.keyType == "hash"
			}
		}
		switch {
		case errors.Is(snapshotErr, ErrFriendReconcile):
			r.authority.envelopeReads.reconcileTotal.Add(1)
		case snapshotErr != nil:
			r.authority.envelopeReads.technicalFailureTotal.Add(1)
		case present:
			r.authority.envelopeReads.presentTotal.Add(1)
		default:
			r.authority.envelopeReads.absentTotal.Add(1)
		}
		if snapshotErr != nil && firstErr == nil {
			firstErr = snapshotErr
		}
	}
	if firstErr == nil && pipelineErr != nil {
		// go-redis normally exposes the same failure on a queued command. Keep a
		// conservative fallback for client implementations that only return the
		// aggregate pipeline error.
		firstErr = pipelineErr
	}
	if firstErr != nil {
		return nil, firstErr
	}

	results := make([]PairSnapshot, len(records))
	for index, record := range records {
		canonical := record.bucket.snapshot.records[record.bucketIndex]
		if canonical.keyType != "hash" {
			results[index] = PairSnapshot{PairID: record.pairID, LowPlayerID: record.low, HighPlayerID: record.high,
				Relation: PairRelation{RelationID: record.pairID}}
			continue
		}
		snapshot, err := pairSnapshotFromRedis(canonical.fields, record.pairID, record.low, record.high,
			r.authority.keys.Profile().LayoutVersion, canonical.fieldCount)
		if err != nil {
			return nil, err
		}
		results[index] = snapshot
	}
	return results, nil
}
