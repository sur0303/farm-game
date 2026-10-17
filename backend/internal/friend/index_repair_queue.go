package friend

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const defaultFriendIndexRepairRounds = 3

type FriendIndexRepairLease struct {
	ViewerPlayerID uint64
	LeaseDueAtMs   int64
}

type FriendIndexRepairQueue interface {
	Enqueue(context.Context, uint64, int64) error
	Claim(context.Context, int64, int64, int) ([]FriendIndexRepairLease, error)
	Complete(context.Context, FriendIndexRepairLease, int64, int64) error
	Retry(context.Context, FriendIndexRepairLease, int64) error
}

var enqueueFriendIndexRepairScript = redis.NewScript(`
local current = tonumber(redis.call('ZSCORE', KEYS[1], ARGV[1]) or '0')
local due = tonumber(ARGV[2])
if current == 0 or due < current then redis.call('ZADD', KEYS[1], due, ARGV[1]) end
redis.call('HSET', KEYS[2], ARGV[1], ARGV[3])
return 1
`)

var claimFriendIndexRepairScript = redis.NewScript(`
local viewers = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[3])
local lease_due = tonumber(ARGV[1]) + tonumber(ARGV[2])
for _, viewer in ipairs(viewers) do redis.call('ZADD', KEYS[1], lease_due, viewer) end
local out = {}
for _, viewer in ipairs(viewers) do table.insert(out, viewer); table.insert(out, tostring(lease_due)) end
return out
`)

var completeFriendIndexRepairScript = redis.NewScript(`
local score = tonumber(redis.call('ZSCORE', KEYS[1], ARGV[1]) or '0')
if score ~= tonumber(ARGV[2]) then return 0 end
local remaining = tonumber(redis.call('HGET', KEYS[2], ARGV[1]) or '1') - 1
if remaining > 0 then
  redis.call('HSET', KEYS[2], ARGV[1], remaining)
  redis.call('ZADD', KEYS[1], tonumber(ARGV[3]) + tonumber(ARGV[4]), ARGV[1])
else
  redis.call('ZREM', KEYS[1], ARGV[1])
  redis.call('HDEL', KEYS[2], ARGV[1])
end
return 1
`)

var retryFriendIndexRepairScript = redis.NewScript(`
local score = tonumber(redis.call('ZSCORE', KEYS[1], ARGV[1]) or '0')
if score ~= tonumber(ARGV[2]) then return 0 end
redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1])
return 1
`)

type RedisFriendIndexRepairQueue struct {
	client    redis.UniversalClient
	due       string
	remaining string
}

func NewRedisFriendIndexRepairQueue(client redis.UniversalClient, profile FriendRedisProfile) (*RedisFriendIndexRepairQueue, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: friend index repair redis client", ErrInvalidCommand)
	}
	keys, err := NewFriendshipKeyBuilder(profile)
	if err != nil {
		return nil, err
	}
	due, remaining, err := keys.FriendIndexRepairKeys()
	if err != nil {
		return nil, err
	}
	return &RedisFriendIndexRepairQueue{client: client, due: due.String(), remaining: remaining.String()}, nil
}

func (q *RedisFriendIndexRepairQueue) Enqueue(ctx context.Context, viewer uint64, dueAtMs int64) error {
	if q == nil || viewer == 0 || dueAtMs <= 0 {
		return fmt.Errorf("%w: friend index repair enqueue", ErrInvalidCommand)
	}
	return enqueueFriendIndexRepairScript.Run(ctx, q.client, []string{q.due, q.remaining},
		strconv.FormatUint(viewer, 10), strconv.FormatInt(dueAtMs, 10), defaultFriendIndexRepairRounds).Err()
}

func (q *RedisFriendIndexRepairQueue) Claim(ctx context.Context, nowMs, leaseMs int64, limit int) ([]FriendIndexRepairLease, error) {
	if q == nil || nowMs <= 0 || leaseMs <= 0 || limit <= 0 || limit > 100 {
		return nil, fmt.Errorf("%w: friend index repair claim", ErrInvalidCommand)
	}
	values, err := claimFriendIndexRepairScript.Run(ctx, q.client, []string{q.due, q.remaining}, nowMs, leaseMs, limit).StringSlice()
	if err != nil {
		return nil, err
	}
	if len(values)%2 != 0 {
		return nil, errors.New("friend: malformed repair claim")
	}
	leases := make([]FriendIndexRepairLease, 0, len(values)/2)
	for i := 0; i < len(values); i += 2 {
		viewer, viewerErr := strconv.ParseUint(values[i], 10, 64)
		leaseDue, leaseErr := strconv.ParseInt(values[i+1], 10, 64)
		if viewerErr != nil || leaseErr != nil || viewer == 0 || leaseDue <= nowMs {
			return nil, errors.New("friend: malformed repair lease")
		}
		leases = append(leases, FriendIndexRepairLease{ViewerPlayerID: viewer, LeaseDueAtMs: leaseDue})
	}
	return leases, nil
}

func (q *RedisFriendIndexRepairQueue) Complete(ctx context.Context, lease FriendIndexRepairLease, nowMs, repeatDelayMs int64) error {
	if q == nil || lease.ViewerPlayerID == 0 || lease.LeaseDueAtMs <= 0 || nowMs <= 0 || repeatDelayMs <= 0 {
		return fmt.Errorf("%w: friend index repair complete", ErrInvalidCommand)
	}
	return completeFriendIndexRepairScript.Run(ctx, q.client, []string{q.due, q.remaining},
		lease.ViewerPlayerID, lease.LeaseDueAtMs, nowMs, repeatDelayMs).Err()
}

func (q *RedisFriendIndexRepairQueue) Retry(ctx context.Context, lease FriendIndexRepairLease, dueAtMs int64) error {
	if q == nil || lease.ViewerPlayerID == 0 || lease.LeaseDueAtMs <= 0 || dueAtMs <= 0 {
		return fmt.Errorf("%w: friend index repair retry", ErrInvalidCommand)
	}
	return retryFriendIndexRepairScript.Run(ctx, q.client, []string{q.due, q.remaining},
		lease.ViewerPlayerID, lease.LeaseDueAtMs, dueAtMs).Err()
}

func RunFriendIndexRepairWorker(ctx context.Context, queue FriendIndexRepairQueue, reconciler *FriendIndexReconciler,
	poll, lease, repeatDelay, retryDelay time.Duration, onError func(error), onReconciled func(FriendIndexReconcileReport)) {
	if queue == nil || reconciler == nil {
		return
	}
	if poll <= 0 {
		poll = 500 * time.Millisecond
	}
	if lease <= 0 {
		lease = 10 * time.Second
	}
	if repeatDelay <= 0 {
		repeatDelay = 2 * time.Second
	}
	if retryDelay <= 0 {
		retryDelay = time.Second
	}
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			leases, err := queue.Claim(ctx, now.UnixMilli(), lease.Milliseconds(), 16)
			if err != nil {
				if onError != nil {
					onError(err)
				}
				continue
			}
			for _, item := range leases {
				report, reconcileErr := reconciler.ReconcileViewer(ctx, item.ViewerPlayerID)
				nowMs := time.Now().UnixMilli()
				if reconcileErr != nil {
					err = queue.Retry(ctx, item, nowMs+retryDelay.Milliseconds())
				} else {
					if onReconciled != nil {
						onReconciled(report)
					}
					err = queue.Complete(ctx, item, nowMs, repeatDelay.Milliseconds())
				}
				if err != nil && onError != nil {
					onError(err)
				}
			}
		}
	}
}
