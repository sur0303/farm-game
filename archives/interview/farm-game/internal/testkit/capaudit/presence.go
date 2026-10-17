package capaudit

import (
	"context"
	"errors"
	"fmt"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/redis/rediskey"

	"github.com/redis/go-redis/v9"
)

// Precondition is a claim that has to hold before any answer means anything.
//
// Every check here reports how many violations it found. Pointed at a keyspace
// that holds nothing, all of them find nothing, and the run reads as clean. The
// exporter already refuses to invent a zero when storage is unreachable, but
// reachable-and-empty produces the same clean zeros for a completely different
// reason. A precondition is how those two are told apart.
type Precondition interface {
	// Describe names what is being asserted, for the failure message.
	Describe() string
	Verify(context.Context) error
}

// ShardPresence asserts the audit is reading the keyspace this deployment
// actually writes, by requiring the authority row of every shard the route
// snapshot declares. A wrong namespace, zone, realm or hash profile puts every
// key somewhere else, and the first thing that goes missing is these rows.
type ShardPresence struct {
	client  redis.UniversalClient
	profile rediskey.PhysicalProfile
	keys    rediskey.AuthorityKeys
	shards  rediskey.AuthorityShardResolver
	domains []actorrouting.RouteDomain
}

type ShardPresenceConfig struct {
	Client  redis.UniversalClient
	Profile rediskey.PhysicalProfile
	Shards  rediskey.AuthorityShardResolver
	// Domains defaults to the two the audit reads actors from.
	Domains []actorrouting.RouteDomain
}

func NewShardPresence(config ShardPresenceConfig) (*ShardPresence, error) {
	if config.Client == nil {
		return nil, errors.New("capaudit: shard presence needs a redis client")
	}
	keys, err := rediskey.NewAuthorityKeys(config.Profile, config.Shards)
	if err != nil {
		return nil, err
	}
	domains := config.Domains
	if len(domains) == 0 {
		domains = []actorrouting.RouteDomain{actorrouting.DomainPlayer, actorrouting.DomainFarm}
	}
	return &ShardPresence{
		client: config.Client, profile: config.Profile, keys: keys,
		shards: config.Shards, domains: domains,
	}, nil
}

func (p *ShardPresence) Describe() string {
	return "the authority row of every declared shard is present"
}

func (p *ShardPresence) Verify(ctx context.Context) error {
	for _, domain := range p.domains {
		count, err := p.shards.ShardCount(domain)
		if err != nil {
			return fmt.Errorf("shard count for %s: %w", domain, err)
		}
		for shard := uint32(0); shard < count; shard++ {
			ref, err := p.profile.AuthorityBucketForShard(p.shards, domain, shard)
			if err != nil {
				return fmt.Errorf("bucket for %s shard %d: %w", domain, shard, err)
			}
			key, err := p.keys.ShardAuthority(ref)
			if err != nil {
				return err
			}
			found, err := p.client.Exists(ctx, key.String()).Result()
			if err != nil {
				return fmt.Errorf("read %s: %w", key, err)
			}
			if found == 0 {
				return fmt.Errorf("%s holds no authority row, so this is not the keyspace under test "+
					"or %s shard %d was never claimed", key, domain, shard)
			}
		}
	}
	return nil
}
