package authority

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"

	"github.com/redis/go-redis/v9"

	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workidentity"
)

// MaxPendingWorkReconcileEntries is the hard per-invocation bound for the
// explicit pending-work accounting repair. A bucket above this bound is left
// untouched and must be drained below the bound before reconciliation.
const MaxPendingWorkReconcileEntries = 4096

type PendingWorkReconcileStatus uint8

const (
	PendingWorkReconcileUnknown PendingWorkReconcileStatus = iota
	PendingWorkReconciled
	PendingWorkReconcileStaleSnapshot
	PendingWorkReconcileUnavailableNotSent
	PendingWorkReconcileTimeoutUnknown
)

func (s PendingWorkReconcileStatus) String() string {
	switch s {
	case PendingWorkReconciled:
		return "RECONCILED"
	case PendingWorkReconcileStaleSnapshot:
		return "STALE_SNAPSHOT"
	case PendingWorkReconcileUnavailableNotSent:
		return "UNAVAILABLE_NOT_SENT"
	case PendingWorkReconcileTimeoutUnknown:
		return "TIMEOUT_UNKNOWN"
	default:
		return "UNKNOWN"
	}
}

type PendingWorkReconcileResult struct {
	Status       PendingWorkReconcileStatus
	PendingCount int64
	PendingBytes int64
	Changed      bool
}

type pendingWorkReconcileEntry struct {
	kind           string
	member         string
	workType       string
	workID         string
	key            rediskey.RedisKey
	sharedScore    *int64
	dedicatedScore *int64
}

// ReconcilePendingWorkAccounting is the bounded rolling/historical repair for
// bucket pending_work_count and pending_work_bytes. It reads the complete
// bounded union of the shared and dedicated DueIndexes, derives exact record
// keys with the frozen key builder, and lets one Lua invocation revalidate all
// cardinalities, members, scores, record identities, states, and payload bytes
// before mutation. SourceOrder/SourceSchedule hashes are authoritative for
// whether a member is counted; stale terminal index members are removed.
//
// Every supported writer atomically indexes each nonterminal record in at
// least one of these indexes. Consequently, predecessor-completed records do
// not need to be enumerated: once their terminal script removed the member,
// this repair installs counters computed only from current nonterminal record
// hashes. The operation is safe to repeat during a rolling deployment and
// must be run once more after all predecessor workers have drained. A stale
// concurrent snapshot is reported without mutation and can be retried.
func (w *WorkLeaseStore) ReconcilePendingWorkAccounting(ctx context.Context, bucket rediskey.AuthorityBucketRef, limit int) (PendingWorkReconcileResult, error) {
	if w == nil || w.client == nil {
		return PendingWorkReconcileResult{}, fmt.Errorf("%w: nil work lease store", authoritystore.ErrInvalidRequest)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return PendingWorkReconcileResult{Status: PendingWorkReconcileUnavailableNotSent}, err
	}
	if err := bucket.Validate(); err != nil {
		return PendingWorkReconcileResult{Status: PendingWorkReconcileUnavailableNotSent}, err
	}
	if limit <= 0 || limit > MaxPendingWorkReconcileEntries || limit > w.client.opts.MaxBucketPendingWork {
		return PendingWorkReconcileResult{Status: PendingWorkReconcileUnavailableNotSent},
			fmt.Errorf("%w: pending-work reconcile limit", authoritystore.ErrInvalidRequest)
	}

	tag, err := w.client.keys.HashTag(bucket)
	if err != nil {
		return PendingWorkReconcileResult{Status: PendingWorkReconcileUnavailableNotSent}, err
	}
	metaKey, err := w.client.keys.BucketMeta(bucket)
	if err != nil {
		return PendingWorkReconcileResult{Status: PendingWorkReconcileUnavailableNotSent}, err
	}
	sharedKey, err := w.client.keys.SourceDueIndex(bucket)
	if err != nil {
		return PendingWorkReconcileResult{Status: PendingWorkReconcileUnavailableNotSent}, err
	}
	dedicatedKey, err := w.client.keys.SourceScheduleDueIndex(bucket)
	if err != nil {
		return PendingWorkReconcileResult{Status: PendingWorkReconcileUnavailableNotSent}, err
	}

	pipe := w.client.Client().Pipeline()
	sharedCardCmd := pipe.ZCard(ctx, sharedKey.String())
	sharedRowsCmd := pipe.ZRangeWithScores(ctx, sharedKey.String(), 0, int64(limit))
	dedicatedCardCmd := pipe.ZCard(ctx, dedicatedKey.String())
	dedicatedRowsCmd := pipe.ZRangeWithScores(ctx, dedicatedKey.String(), 0, int64(limit))
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return PendingWorkReconcileResult{}, err
	}
	sharedCard, err := sharedCardCmd.Result()
	if err != nil {
		return PendingWorkReconcileResult{}, err
	}
	dedicatedCard, err := dedicatedCardCmd.Result()
	if err != nil {
		return PendingWorkReconcileResult{}, err
	}
	sharedRows, err := sharedRowsCmd.Result()
	if err != nil {
		return PendingWorkReconcileResult{}, err
	}
	dedicatedRows, err := dedicatedRowsCmd.Result()
	if err != nil {
		return PendingWorkReconcileResult{}, err
	}
	if sharedCard > int64(limit) || dedicatedCard > int64(limit) ||
		len(sharedRows) > limit || len(dedicatedRows) > limit {
		return PendingWorkReconcileResult{}, authoritystore.ErrCapacityExceeded
	}
	// ZCARD/ZRANGE are bounded discovery reads rather than a transaction. Lua
	// performs the real atomic fence, but an already-observed mismatch can
	// avoid an unnecessary write attempt.
	if sharedCard != int64(len(sharedRows)) || dedicatedCard != int64(len(dedicatedRows)) {
		return PendingWorkReconcileResult{Status: PendingWorkReconcileStaleSnapshot}, nil
	}

	entries := make(map[string]*pendingWorkReconcileEntry, len(sharedRows)+len(dedicatedRows))
	add := func(row redis.Z, dedicated bool) error {
		member, ok := row.Member.(string)
		if !ok || member == "" || math.IsNaN(row.Score) || math.IsInf(row.Score, 0) ||
			row.Score < 0 || row.Score > (1<<53)-1 || math.Trunc(row.Score) != row.Score {
			return fmt.Errorf("%w: pending-work due member/score", errMalformedBlob)
		}
		score := int64(row.Score)
		entry := entries[member]
		if entry == nil {
			entry = &pendingWorkReconcileEntry{member: member}
			switch {
			case len(member) >= 4 && member[:4] == "wr1_":
				ref, decodeErr := workidentity.DecodeWorkRefV1(member)
				if decodeErr != nil {
					return fmt.Errorf("%w: pending-work order ref", errMalformedBlob)
				}
				key, keyErr := w.client.keys.SourceOrder(bucket, ref.WorkType, ref.WorkID)
				if keyErr != nil {
					return fmt.Errorf("%w: pending-work order key", errMalformedBlob)
				}
				entry.kind, entry.workType, entry.workID, entry.key = "O", ref.WorkType, ref.WorkID, key
			case len(member) >= 4 && member[:4] == "sr1_":
				ref, decodeErr := workidentity.DecodeScheduleRefV1(member)
				if decodeErr != nil {
					return fmt.Errorf("%w: pending-work schedule ref", errMalformedBlob)
				}
				key, keyErr := w.client.keys.SourceSchedule(bucket, ref.ScheduleType, ref.ScheduleID)
				if keyErr != nil {
					return fmt.Errorf("%w: pending-work schedule key", errMalformedBlob)
				}
				entry.kind, entry.workType, entry.workID, entry.key = "S", ref.ScheduleType, ref.ScheduleID, key
			default:
				return fmt.Errorf("%w: pending-work reference prefix", errMalformedBlob)
			}
			entries[member] = entry
		}
		if dedicated {
			if entry.kind != "S" || entry.dedicatedScore != nil {
				return fmt.Errorf("%w: dedicated pending-work member", errMalformedBlob)
			}
			entry.dedicatedScore = &score
		} else {
			if entry.sharedScore != nil {
				return fmt.Errorf("%w: duplicate shared pending-work member", errMalformedBlob)
			}
			entry.sharedScore = &score
		}
		return nil
	}
	for _, row := range sharedRows {
		if err := add(row, false); err != nil {
			return PendingWorkReconcileResult{}, err
		}
	}
	for _, row := range dedicatedRows {
		if err := add(row, true); err != nil {
			return PendingWorkReconcileResult{}, err
		}
	}
	if len(entries) > limit {
		return PendingWorkReconcileResult{}, authoritystore.ErrCapacityExceeded
	}

	members := make([]string, 0, len(entries))
	for member := range entries {
		members = append(members, member)
	}
	sort.Strings(members)
	redisKeys := []rediskey.RedisKey{metaKey, sharedKey, dedicatedKey}
	keys := []string{metaKey.String(), sharedKey.String(), dedicatedKey.String()}
	args := []interface{}{
		tag, string(bucket.Domain()), bucket.LayoutVersion(), strconv.FormatUint(uint64(bucket.VirtualShard()), 10),
		strconv.Itoa(w.client.opts.MaxBucketPendingWork), strconv.Itoa(w.client.opts.MaxBucketPendingWorkBytes),
		strconv.FormatInt(sharedCard, 10), strconv.FormatInt(dedicatedCard, 10),
	}
	for _, member := range members {
		entry := entries[member]
		redisKeys = append(redisKeys, entry.key)
		keys = append(keys, entry.key.String())
		sharedScore, dedicatedScore := "", ""
		if entry.sharedScore != nil {
			sharedScore = strconv.FormatInt(*entry.sharedScore, 10)
		}
		if entry.dedicatedScore != nil {
			dedicatedScore = strconv.FormatInt(*entry.dedicatedScore, 10)
		}
		args = append(args, entry.kind, entry.member, sharedScore, dedicatedScore, entry.workType, entry.workID)
	}
	if err := rediskey.AssertSameSlot(redisKeys...); err != nil {
		return PendingWorkReconcileResult{Status: PendingWorkReconcileUnavailableNotSent}, err
	}

	reply, err := w.client.registry.RunNoRetry(ctx, ScriptReconcilePendingWork, keys, args...)
	if err != nil {
		status := PendingWorkReconcileTimeoutUnknown
		if classifyWriteError(err) == fateNotSent {
			status = PendingWorkReconcileUnavailableNotSent
		}
		return PendingWorkReconcileResult{Status: status}, transportErrIfContract(err)
	}
	if len(reply) == 0 {
		return PendingWorkReconcileResult{}, fmt.Errorf("%w: empty pending-work reconcile reply", errScriptContract)
	}
	switch reply[0] {
	case "RECONCILED":
		if len(reply) != 5 {
			return PendingWorkReconcileResult{}, fmt.Errorf("%w: short pending-work reconcile reply", errScriptContract)
		}
		count, countErr := strconv.ParseInt(reply[3], 10, 64)
		bytes, bytesErr := strconv.ParseInt(reply[4], 10, 64)
		if countErr != nil || bytesErr != nil || count < 0 || bytes < 0 {
			return PendingWorkReconcileResult{}, fmt.Errorf("%w: malformed pending-work reconcile counters", errScriptContract)
		}
		return PendingWorkReconcileResult{
			Status: PendingWorkReconciled, PendingCount: count, PendingBytes: bytes,
			Changed: reply[1] != reply[3] || reply[2] != reply[4],
		}, nil
	case "STALE_SNAPSHOT":
		return PendingWorkReconcileResult{Status: PendingWorkReconcileStaleSnapshot}, nil
	case "CAPACITY_EXCEEDED":
		return PendingWorkReconcileResult{}, authoritystore.ErrCapacityExceeded
	case "MALFORMED_RECORD":
		return PendingWorkReconcileResult{}, fmt.Errorf("%w: pending-work reconciliation", errMalformedBlob)
	case "CONTRACT_ERROR":
		return PendingWorkReconcileResult{}, fmt.Errorf("%w: pending-work reconciliation", errScriptContract)
	default:
		return PendingWorkReconcileResult{}, fmt.Errorf("%w: pending-work reconcile status %q", errScriptContract, reply[0])
	}
}
