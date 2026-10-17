package capaudit

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workidentity"

	"github.com/redis/go-redis/v9"
)

// DueResidualGate is the contract name this check answers for.
const DueResidualGate = "due_residual_unexpected"

// DefaultDueResidualAge is how far into the past a due entry may sit before it
// is unexpected.
//
// Claiming an item moves its due score forward to the lease expiry, and the
// authority clamps a lease to at most one minute. So nothing that is legitimately
// being worked on can appear more than that far overdue. Past this line an entry
// is either abandoned, or the workers are far enough behind that the capacity
// answer is already no — both of which a run needs to hear about.
const DefaultDueResidualAge = time.Minute

const (
	orderRefPrefix          = "wr1_"
	scheduleRefPrefix       = "sr1_"
	dueResidualDefaultLimit = 65536
)

// DueResidualCheck counts scheduled work left behind in the authority due
// indexes after the work that owned it should have drained.
//
// Enumeration follows the route snapshot: one index per virtual shard per
// domain. That is the same partitioning the services use, so the scan covers
// exactly the shards that exist rather than a count copied into a config that
// could silently fall behind a resharding.
type DueResidualCheck struct {
	client   *redis.Client
	keys     rediskey.AuthorityKeys
	profile  rediskey.PhysicalProfile
	shards   rediskey.AuthorityShardResolver
	schedule bool
	age      time.Duration
	limit    int64
	now      func() time.Time
}

type DueResidualConfig struct {
	Client  *redis.Client
	Profile rediskey.PhysicalProfile
	Shards  rediskey.AuthorityShardResolver
	// DedicatedScheduleIndex mirrors FARM_SCHEDULE_DUE_INDEX_MODE. When the
	// deployment keeps schedules in their own index, that index has to be
	// walked too or its leftovers would never be seen.
	DedicatedScheduleIndex bool
	Age                    time.Duration
	Limit                  int64
	Now                    func() time.Time
}

func NewDueResidualCheck(config DueResidualConfig) (*DueResidualCheck, error) {
	if config.Client == nil {
		return nil, errors.New("capaudit: redis client is required")
	}
	keys, err := rediskey.NewAuthorityKeys(config.Profile, config.Shards)
	if err != nil {
		return nil, err
	}
	age := config.Age
	if age <= 0 {
		age = DefaultDueResidualAge
	}
	limit := config.Limit
	if limit <= 0 {
		limit = dueResidualDefaultLimit
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	return &DueResidualCheck{
		client: config.Client, keys: keys, profile: config.Profile, shards: config.Shards,
		schedule: config.DedicatedScheduleIndex, age: age, limit: limit, now: now,
	}, nil
}

func (c *DueResidualCheck) Gate() string { return DueResidualGate }

func (c *DueResidualCheck) Observe(ctx context.Context) (int64, error) {
	cutoff := c.now().Add(-c.age).UnixMilli()
	var residual, budget int64
	budget = c.limit
	for _, domain := range []actorrouting.RouteDomain{actorrouting.DomainPlayer, actorrouting.DomainFarm} {
		count, err := c.shards.ShardCount(domain)
		if err != nil {
			return 0, fmt.Errorf("shard count for %s: %w", domain, err)
		}
		for shard := uint32(0); shard < count; shard++ {
			found, spent, err := c.residualInShard(ctx, domain, shard, cutoff, budget)
			if err != nil {
				return 0, err
			}
			residual += found
			budget -= spent
			if budget <= 0 {
				return 0, fmt.Errorf("due indexes hold more than the %d entries this audit reads in one pass", c.limit)
			}
		}
	}
	return residual, nil
}

func (c *DueResidualCheck) residualInShard(ctx context.Context, domain actorrouting.RouteDomain, shard uint32, cutoffMS, budget int64) (int64, int64, error) {
	ref, err := c.profile.AuthorityBucketForShard(c.shards, domain, shard)
	if err != nil {
		return 0, 0, fmt.Errorf("bucket for %s shard %d: %w", domain, shard, err)
	}
	indexes, err := c.indexKeys(ref)
	if err != nil {
		return 0, 0, err
	}
	var residual, spent int64
	for _, index := range indexes {
		members, err := c.client.ZRangeByScore(ctx, index, &redis.ZRangeBy{
			Min: "-inf", Max: strconv.FormatInt(cutoffMS, 10), Count: budget + 1,
		}).Result()
		if err != nil {
			return 0, 0, fmt.Errorf("read %s: %w", index, err)
		}
		spent += int64(len(members))
		for _, member := range members {
			// Being this far overdue is the whole finding. The record is not
			// read, because every case it could reveal counts: gone means the
			// index outlived its work, terminal means completion failed to
			// clean up, and still-live means nothing is coming for it. What
			// the member must do is parse, so an entry of an unknown shape
			// fails the audit instead of passing through it uncounted.
			if err := validMember(member); err != nil {
				return 0, 0, err
			}
			residual++
		}
	}
	return residual, spent, nil
}

func (c *DueResidualCheck) indexKeys(ref rediskey.AuthorityBucketRef) ([]string, error) {
	due, err := c.keys.SourceDueIndex(ref)
	if err != nil {
		return nil, err
	}
	keys := []string{due.String()}
	if c.schedule {
		scheduleDue, err := c.keys.SourceScheduleDueIndex(ref)
		if err != nil {
			return nil, err
		}
		keys = append(keys, scheduleDue.String())
	}
	return keys, nil
}

// validMember accepts the two reference shapes the authority puts in a due
// index: an order and a schedule.
func validMember(member string) error {
	switch {
	case strings.HasPrefix(member, orderRefPrefix):
		if _, err := workidentity.DecodeWorkRefV1(member); err != nil {
			return fmt.Errorf("due member %q: %w", member, err)
		}
		return nil
	case strings.HasPrefix(member, scheduleRefPrefix):
		if _, err := workidentity.DecodeScheduleRefV1(member); err != nil {
			return fmt.Errorf("due member %q: %w", member, err)
		}
		return nil
	default:
		return fmt.Errorf("due member %q has a shape this audit does not know", member)
	}
}
