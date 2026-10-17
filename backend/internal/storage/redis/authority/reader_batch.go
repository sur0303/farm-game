package authority

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"

	"github.com/redis/go-redis/v9"
)

const maxShardAuthorityBatchRead = 4096

// ReadShardWriteAuthorityBatch performs the same exact Lua validation as the
// single-key reader, but queues every key into one Redis Cluster pipeline.
// go-redis splits the pipeline by hash slot/node. The result remains ordered
// and all-or-nothing to Manager: one transport/corruption error rejects the
// complete deadline projection wave.
func (r *AuthorityReader) ReadShardWriteAuthorityBatch(ctx context.Context, domains []actorrouting.RouteDomain, shards []uint32) ([]authoritystore.ShardWriteAuthority, []bool, error) {
	if r == nil || r.client == nil || ctx == nil || len(domains) == 0 || len(domains) != len(shards) || len(domains) > maxShardAuthorityBatchRead {
		return nil, nil, ErrAuthorityReaderInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	type item struct {
		domain actorrouting.RouteDomain
		shard  uint32
		key    string
	}
	items := make([]item, len(domains))
	for index, domain := range domains {
		if domain != actorrouting.DomainPlayer && domain != actorrouting.DomainFarm && domain != actorrouting.DomainFarmWorker {
			return nil, nil, fmt.Errorf("%w: domain", ErrAuthorityReaderInvalid)
		}
		bucket, err := r.opts.Profile.AuthorityBucketForShard(r.opts.Resolver, domain, shards[index])
		if err != nil {
			return nil, nil, fmt.Errorf("%w: bucket: %v", ErrAuthorityReaderInvalid, err)
		}
		key, err := r.keys.ShardAuthority(bucket)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: key: %v", ErrAuthorityReaderInvalid, err)
		}
		items[index] = item{domain: domain, shard: shards[index], key: key.String()}
	}
	args := make([]any, 0, len(shardAuthorityFields)+1)
	args = append(args, strconv.Itoa(r.opts.MaxFieldBytes))
	for _, field := range shardAuthorityFields {
		args = append(args, field)
	}
	bounded, cancel := context.WithTimeout(ctx, r.opts.OperationTimeout)
	defer cancel()
	var commands []*redis.Cmd
	for attempt := 0; attempt < 3; attempt++ {
		commands = make([]*redis.Cmd, len(items))
		_, err := r.client.Pipelined(bounded, func(pipe redis.Pipeliner) error {
			for index, current := range items {
				commands[index] = pipe.EvalSha(bounded, r.scriptSHA, []string{current.key}, args...)
			}
			return nil
		})
		if err == nil {
			break
		}
		if !strings.Contains(err.Error(), "NOSCRIPT") {
			return nil, nil, err
		}
		sha, loadErr := r.client.ScriptLoad(bounded, readShardAuthorityLua).Result()
		if loadErr != nil {
			return nil, nil, loadErr
		}
		if sha != r.scriptSHA {
			return nil, nil, fmt.Errorf("%w: script sha", ErrAuthorityReaderCorrupt)
		}
		if attempt == 2 {
			return nil, nil, err
		}
	}
	authorities := make([]authoritystore.ShardWriteAuthority, len(items))
	found := make([]bool, len(items))
	for index, command := range commands {
		if command == nil {
			return nil, nil, fmt.Errorf("%w: missing batch command", ErrAuthorityReaderCorrupt)
		}
		reply, err := command.Result()
		if err != nil {
			return nil, nil, err
		}
		values, err := decodeAuthorityReadReply(reply)
		if err != nil {
			return nil, nil, err
		}
		if len(values) == 1 && values[0] == "NOT_FOUND" {
			continue
		}
		if len(values) > 0 && values[0] == "CORRUPT" {
			return nil, nil, fmt.Errorf("%w: atomic read %s", ErrAuthorityReaderCorrupt, strings.Join(values[1:], ","))
		}
		if len(values) != len(shardAuthorityFields)+1 || values[0] != "FOUND" {
			return nil, nil, fmt.Errorf("%w: value count", ErrAuthorityReaderCorrupt)
		}
		observation, exists, err := decodeAuthorityObservation(r.opts, items[index].domain, items[index].shard, values[1:])
		if err != nil {
			return nil, nil, err
		}
		authorities[index], found[index] = observation.Authority, exists
	}
	return authorities, found, nil
}

func decodeAuthorityReadReply(reply any) ([]string, error) {
	items, ok := reply.([]any)
	if !ok {
		return nil, fmt.Errorf("%w: script reply", ErrAuthorityReaderCorrupt)
	}
	result := make([]string, len(items))
	for index, item := range items {
		value, ok := redisText(item)
		if !ok {
			return nil, fmt.Errorf("%w: script value", ErrAuthorityReaderCorrupt)
		}
		result[index] = value
	}
	return result, nil
}
